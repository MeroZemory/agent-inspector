"""Antigravity CLI (`agy`) conversations: ~/.gemini/antigravity-cli/conversations/<uuid>.db.

Each conversation is a SQLite DB (table `steps`, one protobuf `gemini_coder.Step` per row in `step_payload`).
There is no published schema; field numbers below were mapped from the files themselves (agy 1.2.x):

  Step: 1 step_type, 4 status, 5 metadata, plus one payload field chosen by step_type
  metadata: 1 created {1 sec, 2 nanos}; 4 tool call (on tool-result steps); 9 usage
            {1 model enum, 2 uncached input, 3 output, 5 cached input}; 11 model enum
  step_type 14 user input       -> 19 {2 text}
            15 planner response -> 20 {1 visible text (8 = same text, older builds), 3 thinking,
                                       7 tool call {1 id, 2 name, 3 args JSON}}
            17 client note      -> 24 {3 {2 text}}   API errors, quota, "stream was interrupted" retries
            23 context summary  -> 30 {5 summary text}
           101 queued message   -> 114 {1 text, 3 'task_notification'|'agent_message'}
           132 tool result      -> 140 {2 {1 output text}}; status 6/7 = failed, 2 = still running
  gen_metadata.data: 1 {3 model enum, 19 model name}
  trajectory_metadata_blob.data: 1 {1 workspace uri, 2 git root uri, 4 branch}, 4 {1 agent name},
            5 parent conversation id (subagents), 7 cwd uri

Home files (read-only; credentials such as oauth_creds.json / antigravity-oauth-token are never opened):
  conversation_summaries.db  title, workspace, agent_name, parent_conversation_id per conversation
  history.jsonl              typed prompts {display, timestamp(ms), workspace, conversationId}

Human vs injected: a user-input step whose text is in history.jsonl for that conversation is human (people
sometimes resume a Teamwork subagent conversation and type into it). Otherwise, user-input steps of a subagent
conversation are agent_prompt. In a main conversation they
are human when the conversation has typed-prompt history, or when it started before history.jsonl did; a main
conversation in a Temp/.tmp workspace or without any history entry after history began is a scripted
`agy -p` run (sdk_prompt). Leading '/word' is human_command.

The DB changes in place, so every change is a full re-parse (no byte-offset append). Messages store the step
index in `offset`; `agent-inspector raw` decodes that step. A conversation still running keeps recent steps in the -wal
file (read from a private copy); the .db size/mtime may lag until agy checkpoints it, so re-ingest a live
conversation with `agent-inspector ingest --sid <sid> --reparse`. Reads never create -wal/-shm files in agy's directory.
"""
from __future__ import annotations

import collections
import json
import os
import re
import shutil
import sqlite3
import tempfile
from urllib.parse import unquote

from ..util import jdump, ms_to_iso, norm_path
from .base import Candidate, ParseOut

PROVIDER = "agy"

T_USER, T_PLAN, T_NOTE, T_SUMMARY, T_QUEUED, T_TOOL = 14, 15, 17, 23, 101, 132
FAILED_TOOL_STATUS = {6, 7}
HISTORY_START_FALLBACK = "2026-09-02T00:00:00Z"
_CMD = re.compile(r"^/[A-Za-z][\w-]*(\s|$)")


# ---------------------------------------------------------------- protobuf wire format (stdlib)

def _varint(b, i):
    r = s = 0
    while True:
        c = b[i]
        i += 1
        r |= (c & 0x7F) << s
        s += 7
        if c < 0x80:
            return r, i


def pb(b) -> dict:
    """Decode one message level: {field: [values]} (bytes for length-delimited, int for varint)."""
    out: dict = {}
    if not b:
        return out
    i, n = 0, len(b)
    while i < n:
        k, i = _varint(b, i)
        f, w = k >> 3, k & 7
        if w == 0:
            v, i = _varint(b, i)
        elif w == 2:
            ln, i = _varint(b, i)
            v = b[i:i + ln]
            i += ln
        elif w == 1:
            v = b[i:i + 8]
            i += 8
        elif w == 5:
            v = b[i:i + 4]
            i += 4
        else:
            raise ValueError(f"wire type {w}")
        out.setdefault(f, []).append(v)
    return out


def _one(d, f, default=None):
    v = d.get(f)
    return v[0] if v else default


def _sub(d, f) -> dict:
    v = _one(d, f)
    if not isinstance(v, (bytes, bytearray)):
        return {}
    try:
        return pb(v)
    except (ValueError, IndexError):
        return {}


def _str(d, f) -> str | None:
    v = _one(d, f)
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "replace")
    return None


def _ts(d) -> str | None:
    sec = _one(d, 1)
    if not isinstance(sec, int):
        return None
    ns = _one(d, 2, 0)
    return ms_to_iso(sec * 1000 + (ns if isinstance(ns, int) else 0) // 1_000_000)


def pb_to_json(b, depth=0):
    """Best-effort generic decode for `sx raw`: nested messages as dicts, text as str, other bytes as hex."""
    try:
        d = pb(b)
    except (ValueError, IndexError):
        return None
    out = {}
    for f, vals in d.items():
        conv = []
        for v in vals:
            if isinstance(v, (bytes, bytearray)):
                sub = pb_to_json(v, depth + 1) if depth < 12 and v else None
                try:
                    s = v.decode("utf-8")
                    printable = all(c.isprintable() or c in "\n\r\t" for c in s)
                except UnicodeDecodeError:
                    s, printable = None, False
                if s is not None and printable and (sub is None or len(s) > 0 and not s.startswith("\n")):
                    conv.append(s)
                elif sub is not None:
                    conv.append(sub)
                else:
                    conv.append({"hex": bytes(v[:64]).hex(), "len": len(v)})
            else:
                conv.append(v)
        out[str(f)] = conv[0] if len(conv) == 1 else conv
    return out


# ---------------------------------------------------------------- files

class _Ro:
    """Read-only view of an agy SQLite DB that never creates files next to it.

    A plain mode=ro open of a WAL database creates empty -wal/-shm files in agy's directory, so a DB without a
    live WAL opens immutable (no locks, no side files); a DB whose -wal holds data (agy running) is copied with
    its WAL into a private temp dir first and the copy is opened."""

    def __init__(self, path):
        self.tmp = None
        wal = path + "-wal"
        if os.path.exists(wal) and os.path.getsize(wal) > 0:
            self.tmp = tempfile.mkdtemp(prefix="sx-agy-")
            dst = os.path.join(self.tmp, "c.db")
            shutil.copyfile(path, dst)
            shutil.copyfile(wal, dst + "-wal")
            self.con = sqlite3.connect(dst, timeout=5)
        else:
            self.con = sqlite3.connect("file:" + path.replace("\\", "/") + "?mode=ro&immutable=1", uri=True, timeout=5)
        self.con.text_factory = bytes

    def execute(self, *a):
        return self.con.execute(*a)

    def close(self):
        self.con.close()
        if self.tmp:
            shutil.rmtree(self.tmp, ignore_errors=True)


def _connect(path):
    return _Ro(path)


def _uri_path(u) -> str | None:
    if not u:
        return None
    u = u.decode("utf-8", "replace") if isinstance(u, (bytes, bytearray)) else u
    if u.startswith("file:///"):
        u = unquote(u[8:])
    return u.replace("/", "\\") if re.match(r"^[A-Za-z]:", u) else u


def _is_temp(p: str | None) -> bool:
    p = (p or "").lower().replace("\\", "/")
    return "/temp/" in p or "/tmp/" in p or "/.tmp/" in p


def _summaries(home):
    out = {}
    p = os.path.join(home, "conversation_summaries.db")
    if not os.path.exists(p):
        return out
    try:
        con = _connect(p)
        con.con.text_factory = str
        for r in con.execute("SELECT conversation_id,title,workspace_uris,agent_name,parent_conversation_id,"
                             "status,last_modified_time,step_count,nesting_depth FROM conversation_summaries"):
            out[r[0]] = {"title": r[1], "ws": r[2], "agent_name": r[3], "parent": r[4], "status": r[5],
                         "mtime": r[6], "steps": r[7], "depth": r[8]}
        con.close()
    except sqlite3.Error:
        pass
    return out


def _history(home):
    """[(conversationId, iso ts, text, workspace)] from history.jsonl (typed prompts)."""
    out = []
    p = os.path.join(home, "history.jsonl")
    if not os.path.exists(p):
        return out
    with open(p, "rb") as fh:
        for raw in fh:
            try:
                r = json.loads(raw)
            except ValueError:
                continue
            if isinstance(r, dict) and r.get("display") is not None:
                out.append((r.get("conversationId"), ms_to_iso(r.get("timestamp")), r.get("display"),
                            r.get("workspace")))
    return out


def discover(root):
    home = root.path
    conv = os.path.join(home, "conversations")
    if not os.path.isdir(conv):
        return
    summ = _summaries(home)
    hist = _history(home)
    hkeys = collections.defaultdict(set)
    for h in hist:
        if h[0] and _hkey(h[2]):
            hkeys[h[0]].add(_hkey(h[2]))
    hist_start = min((h[1] for h in hist if h[1]), default=None) or HISTORY_START_FALLBACK
    for ent in os.scandir(conv):
        if not (ent.is_file() and ent.name.endswith(".db")):
            continue
        cid = ent.name[:-3]
        s = summ.get(cid) or {}
        ctx = {"has_history": cid in hkeys, "history_start": hist_start, "hist_keys": sorted(hkeys.get(cid, ()))}
        if s.get("parent"):
            ctx["parent_pkey"] = s["parent"]
        if s.get("agent_name"):
            ctx["agent_name"] = s["agent_name"]
        yield Candidate(ent.path, cid, "subagent" if s.get("parent") else "main", ctx)


def probe_last_ts(path):
    try:
        con = _connect(path)
        lo = con.execute("SELECT step_payload FROM steps ORDER BY idx LIMIT 1").fetchone()
        hi = con.execute("SELECT step_payload FROM steps ORDER BY idx DESC LIMIT 1").fetchone()
        con.close()
    except sqlite3.Error:
        return None, None
    f = _ts(_sub(_sub(pb(lo[0]), 5), 1)) if lo and lo[0] else None
    last = _ts(_sub(_sub(pb(hi[0]), 5), 1)) if hi and hi[0] else None
    return f, last


def head(path):
    """(cwd, first timestamp) for ingest selectors (--project/--until)."""
    try:
        con = _connect(path)
        try:
            tm = _traj_meta(con)
        finally:
            con.close()
    except sqlite3.Error:
        return None, None
    return tm.get("cwd"), probe_last_ts(path)[0] or tm.get("created")


def _traj_meta(con) -> dict:
    meta = {}
    r = con.execute("SELECT data FROM trajectory_metadata_blob LIMIT 1").fetchone()
    if not r or not r[0]:
        return meta
    d = pb(r[0])
    ws = _sub(d, 1)
    meta["workspace"] = _uri_path(_one(ws, 1))
    meta["git_root"] = _uri_path(_one(ws, 2))
    br = _str(ws, 4)
    if br:
        meta["git_branch"] = br
    repo = _sub(ws, 3)
    if _str(repo, 2):
        meta["git_origin"] = _str(repo, 2)
    meta["cwd"] = _uri_path(_one(d, 7)) or meta.get("workspace")
    an = _str(_sub(d, 4), 1)
    if an:
        meta["agent_name"] = an
    parent = _str(d, 5)
    if parent:
        meta["parent"] = parent
    created = _ts(_sub(d, 2))
    if created:
        meta["created"] = created
    return meta


def _model_names(con) -> dict:
    names = {}
    for (data,) in con.execute("SELECT data FROM gen_metadata"):
        try:
            g = _sub(pb(data), 1)
        except (ValueError, IndexError):
            continue
        e, n = _one(g, 3), _str(g, 19)
        if isinstance(e, int) and n:
            names[e] = n
    return names


def _mode(ctx, tm, first_ts):
    """(is_sub, noninteractive) for this conversation."""
    if tm.get("parent") or ctx.get("parent_pkey"):
        return True, False
    if ctx.get("has_history"):
        return False, False
    if _is_temp(tm.get("cwd")) or _is_temp(tm.get("workspace")):
        return False, True
    return False, bool(first_ts and first_ts >= ctx.get("history_start", HISTORY_START_FALLBACK))


def _hkey(t) -> str:
    return " ".join((t or "").split())[:40].lower()


def _user_kind(t: str, is_sub: bool, nonint: bool, hist_keys=()) -> str:
    k = _hkey(t)
    typed = bool(k) and any(k == h or (len(h) >= 8 and (k.startswith(h) or h.startswith(k))) for h in hist_keys)
    if typed:  # in history.jsonl: typed by a person, also when they resumed a subagent conversation directly
        return "human_command" if _CMD.match(t.lstrip()) else "human"
    if is_sub:
        return "agent_prompt"
    if nonint:
        return "sdk_prompt"
    if _CMD.match(t.lstrip()):
        return "human_command"
    return "human"


def _note_event(out, ts, text):
    t = text or ""
    if "RESOURCE_EXHAUSTED" in t or "code 429" in t or "quota" in t.lower():
        out.ev(ts, "rate_limit", t[:300])
    elif t.startswith("The stream was interrupted"):
        out.ev(ts, "stream_retry", t[:300])
    elif t.startswith("API error") or "(code " in t:
        out.ev(ts, "api_error", t[:300])


def parse(job) -> dict:
    path = job["path"]
    ctx = job.get("ctx") or {}
    st = {"seq": 0, "turn": 0}
    out = ParseOut(job["caps"], st)
    con = _connect(path)
    try:
        tm = _traj_meta(con)
        names = _model_names(con)
        models = collections.Counter()
        first_ts = last_ts = None
        is_sub = nonint = None
        n_bad = 0
        max_idx = -1
        last_model = None
        for idx, stype, status, payload, err in con.execute(
                "SELECT idx, step_type, status, step_payload, error_details FROM steps ORDER BY idx"):
            max_idx = idx
            if not payload:
                n_bad += 1
                continue
            try:
                d = pb(payload)
            except (ValueError, IndexError):
                n_bad += 1
                continue
            out.rec_counts[f"step_{stype}"] += 1
            md = _sub(d, 5)
            ts = _ts(_sub(md, 1))
            if ts:
                first_ts = first_ts or ts
                last_ts = ts
            if is_sub is None:
                is_sub, nonint = _mode(ctx, tm, first_ts or tm.get("created"))
            kw = {"off": idx, "line": idx}
            if stype == T_USER:
                u = _sub(d, 19)
                t = _str(u, 2) or _str(_sub(u, 3), 1) or ""
                out.msg(ts, "user", _user_kind(t, is_sub, nonint, ctx.get("hist_keys") or ()), t, origin="user_input", **kw)
            elif stype == T_PLAN:
                p = _sub(d, 20)
                usage = _sub(md, 9)
                enum = _one(md, 11) or _one(usage, 1)
                model = names.get(enum) or (f"model_enum_{enum}" if enum else None) or last_model
                last_model = model
                ctx_tok = None
                if usage:
                    ctx_tok = (_one(usage, 2, 0) or 0) + (_one(usage, 5, 0) or 0) or None
                think = _str(p, 3)
                if think:
                    out.msg(ts, "assistant", "reasoning", think, model=model, origin="thinking", **kw)
                text = _str(p, 1) or _str(p, 8)
                if text:
                    models[model] += 1
                    out.msg(ts, "assistant", "assistant", text, model=model, origin="planner",
                            ctx_tokens=ctx_tok, **kw)
                for tc in p.get(7, []):
                    c = pb(tc)
                    out.msg(ts, "assistant", "tool_call", _str(c, 3) or "", tool_name=_str(c, 2),
                            tool_id=_str(c, 1), model=model, origin="tool_call", **kw)
                if not text and p.get(7):
                    models[model] += 1
            elif stype == T_TOOL:
                r = _sub(d, 140)
                tc = _sub(md, 4)
                txt = _str(_sub(r, 2), 1)
                if txt is None and err:
                    e = pb(err)
                    txt = _str(e, 2) or _str(e, 3)
                failed = status in FAILED_TOOL_STATUS
                out.msg(ts, "tool", "tool_output", txt or "", tool_name=_str(tc, 2), tool_id=_str(tc, 1),
                        is_error=int(failed), origin=f"status_{status}", **kw)
                if failed:
                    out.ev(ts, "tool_error", f"{_str(tc, 2)}: {(txt or '')[:200]}")
            elif stype == T_NOTE:
                t = _str(_sub(_sub(d, 24), 3), 2) or ""
                kind = "auto_continuation" if t.startswith("The stream was interrupted") else "system"
                out.msg(ts, "system", kind, t, origin="client_note", **kw)
                _note_event(out, ts, t)
            elif stype == T_SUMMARY:
                t = _str(_sub(d, 30), 5) or ""
                out.msg(ts, "user", "compaction_summary", t, origin="context_summary", **kw)
                out.ev(ts, "compaction", "auto")
            elif stype == T_QUEUED:
                q = _sub(d, 114)
                typ = _str(q, 3) or "message"
                t = _str(q, 1) or ""
                if typ == "agent_message":
                    out.msg(ts, "agent", "agent_message", t, origin=typ, **kw)
                else:
                    out.msg(ts, "user", "task_notification", t, origin=typ, **kw)
            else:
                out.rec_counts[f"unmapped_{stype}"] += 1
        if is_sub is None:
            is_sub, nonint = _mode(ctx, tm, tm.get("created"))
        parent = tm.get("parent") or ctx.get("parent_pkey")
        if parent:
            out.link("subagent_of", PROVIDER, parent, "trajectory_metadata.parent")
        agent_name = tm.get("agent_name") or ctx.get("agent_name")
        brain = os.path.join(os.path.dirname(os.path.dirname(path)), "brain", job.get("pkey") or "")
        if job.get("pkey") and os.path.isdir(brain):
            out.fact("artifacts_dir", brain, "agy_brain")
        out.meta.update({
            "first_ts": first_ts or tm.get("created"), "last_ts": last_ts, "models": dict(models),
            "client": "agy", "source": "agy-sub" if is_sub else ("agy-print" if nonint else "agy-cli"),
            "cwd": tm.get("cwd"), "workspace": tm.get("workspace"), "git_branch": tm.get("git_branch"),
            "git_origin": tm.get("git_origin"), "is_sub": is_sub, "nonint": nonint,
            "agent_name": agent_name, "parent": parent, "n_steps": max_idx + 1,
            "live_wal": bool(os.path.exists(path + "-wal") and os.path.getsize(path + "-wal")),
        })
    finally:
        con.close()
    size = os.path.getsize(path)
    return out.result(end_offset=size, n_lines=max_idx + 1, n_bad=n_bad, partial_tail=0)


def catalog(job) -> dict:
    """Metadata, first/last step timestamps and the first prompt; never the whole step table."""
    path = job["path"]
    ctx = job.get("ctx") or {}
    con = _connect(path)
    try:
        tm = _traj_meta(con)
        f, last = probe_last_ts(path)
        r = con.execute("SELECT step_payload FROM steps WHERE step_type=? ORDER BY idx LIMIT 1", (T_USER,)).fetchone()
        n = con.execute("SELECT COUNT(*) FROM steps").fetchone()[0]
        names = _model_names(con)
    finally:
        con.close()
    is_sub, nonint = _mode(ctx, tm, f or tm.get("created"))
    first = None
    if r and r[0]:
        u = _sub(pb(r[0]), 19)
        t = (_str(u, 2) or "").strip()
        if t:
            first = [_user_kind(t, is_sub, nonint, ctx.get("hist_keys") or ()), t[:300]]
    meta = {"first_ts": f or tm.get("created"), "last_ts": last, "models": {m: 1 for m in names.values()},
            "client": "agy", "source": "agy-sub" if is_sub else ("agy-print" if nonint else "agy-cli"),
            "cwd": tm.get("cwd"), "workspace": tm.get("workspace"), "git_branch": tm.get("git_branch"),
            "is_sub": is_sub, "nonint": nonint, "agent_name": tm.get("agent_name") or ctx.get("agent_name"),
            "parent": tm.get("parent") or ctx.get("parent_pkey"), "n_steps": n}
    links = [["subagent_of", PROVIDER, meta["parent"], "trajectory_metadata.parent"]] if meta["parent"] else []
    return {"meta": meta, "aliases": [], "links": links, "first_prompt": first, "size": os.path.getsize(path)}


def home_files(root):
    return [os.path.join(root.path, n) for n in ("conversation_summaries.db", "history.jsonl")]


def home_metadata(root):
    """Titles, subagent links and typed-prompt history from the agy home (read-only)."""
    home = root.path
    res = {"prompt_history": [], "links": [], "aliases": [], "facts": [], "events": [], "errors": []}
    for cid, s in _summaries(home).items():
        if s.get("title"):
            res["aliases"].append([PROVIDER, cid, s["title"], "agy_title", 0, s.get("mtime")])
        if s.get("agent_name"):
            res["aliases"].append([PROVIDER, cid, s["agent_name"], "agy_agent_name", 0, s.get("mtime")])
        if s.get("parent"):
            res["links"].append([PROVIDER, cid, "subagent_of", PROVIDER, s["parent"], "conversation_summaries"])
        if s.get("status"):
            res["facts"].append([PROVIDER, cid, "agy.status", s["status"], "conversation_summaries", s.get("mtime")])
        if not os.path.exists(os.path.join(home, "conversations", cid + ".db")):
            res["facts"].append([PROVIDER, cid, "agy.missing_conversation_db",
                                 f"steps={s.get('steps')} ws={s.get('ws')}", "conversation_summaries", s.get("mtime")])
    for cid, ts, text, ws in _history(home):
        res["prompt_history"].append([PROVIDER, cid, ts, text, norm_path(ws) if ws else None])
    return res


def raw_record(path, idx) -> str:
    """JSON of one decoded step (for `sx raw`)."""
    con = _connect(path)
    try:
        r = con.execute("SELECT step_type, status, step_payload FROM steps WHERE idx=?", (idx,)).fetchone()
    finally:
        con.close()
    if not r:
        return jdump({"error": f"step {idx} not found"})
    return json.dumps({"idx": idx, "step_type": r[0], "status": r[1], "step": pb_to_json(r[2])}, ensure_ascii=False)
