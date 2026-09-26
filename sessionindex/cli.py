"""sx: query CLI for agents. JSON Lines by default; --format table for humans."""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys

from . import guide as guide_mod
from .classify import HUMAN_KINDS
from .config import ANN_PATH, DB_PATH
from .schema import connect_ro, connect_rw, init_ann
from .util import now_iso

SORTS = {
    "last": "s.last_activity_at", "started": "s.started_at", "human_chars": "s.human_chars",
    "human": "s.human_msgs", "length": "s.duration_s", "active": "s.active_s", "msgs": "s.msgs_n",
    "errors": "s.errors", "tool_errors": "s.tool_errors", "interrupts": "s.interrupts",
    "compactions": "s.compactions", "ctx": "s.max_ctx_tokens",
}
SESSION_FIELDS = ["sid", "provider", "kind", "origin_class", "started_at", "last_activity_at", "project", "title",
                  "human_msgs", "human_chars", "assistant_msgs", "tool_calls", "tool_errors", "errors", "interrupts",
                  "compactions", "active_s"]


# ------------------------------------------------------------------ output helpers

class Out:
    def __init__(self, fmt: str):
        self.fmt = fmt
        self.rows: list[dict] = []

    def emit(self, row: dict):
        if self.fmt == "jsonl":
            sys.stdout.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        else:
            self.rows.append(row)

    def text(self, s: str):
        sys.stdout.write(s + "\n")

    def flush(self, width: int = 60):
        if self.fmt != "table" or not self.rows:
            return
        cols = []
        for r in self.rows:
            for k in r:
                if k not in cols:
                    cols.append(k)

        def cell(v):
            s = "" if v is None else str(v)
            s = s.replace("\n", " ").replace("\r", " ")
            return s if len(s) <= width else s[:width - 1] + "~"
        w = {c: min(width, max(len(c), *(len(cell(r.get(c))) for r in self.rows))) for c in cols}
        sys.stdout.write("  ".join(c.ljust(w[c]) for c in cols) + "\n")
        for r in self.rows:
            sys.stdout.write("  ".join(cell(r.get(c)).ljust(w[c]) for c in cols) + "\n")


def clip(s, n):
    if s is None:
        return None
    s = str(s)
    return s if len(s) <= n else s[:n] + f"…[+{len(s) - n}]"


def die(msg: str, code: int = 2):
    sys.stdout.write(json.dumps({"error": msg}, ensure_ascii=False) + "\n")
    sys.exit(code)


def ro():
    if not os.path.exists(DB_PATH):
        die(f"index not found at {DB_PATH}; run: sx ingest")
    return connect_ro(DB_PATH, ANN_PATH)


def resolve(con, ident: str) -> str:
    ident = ident.strip()
    r = con.execute("SELECT sid FROM sessions WHERE sid=?", (ident,)).fetchone()
    if r:
        return r[0]
    rows = con.execute("SELECT sid FROM sessions WHERE sid LIKE ? LIMIT 6", (ident + "%",)).fetchall()
    if len(rows) == 1:
        return rows[0][0]
    if not rows and len(ident) >= 6:
        rows = con.execute("SELECT sid FROM sessions WHERE pid LIKE ? LIMIT 6", (ident + "%",)).fetchall()
        if len(rows) == 1:
            return rows[0][0]
    if not rows:
        die(f"no session matches '{ident}'")
    die(f"ambiguous id '{ident}': " + ", ".join(r[0] for r in rows))


# ------------------------------------------------------------------ session filters (shared)

def add_filters(p):
    p.add_argument("--provider", "--source", dest="provider", help="claude | codex | opencode")
    p.add_argument("--project", help="substring of derived project name or project_root")
    p.add_argument("--cwd", help="substring of cwd")
    p.add_argument("--alias", help="substring of any alias/title")
    p.add_argument("--since", help="last_activity_at >= (YYYY-MM[-DD])")
    p.add_argument("--until", help="started_at < (YYYY-MM[-DD])")
    p.add_argument("--min-human", type=int, help="min human messages")
    p.add_argument("--min-human-chars", type=int, help="min human chars")
    p.add_argument("--has-event", help="event type(s), comma-separated; session must have any")
    p.add_argument("--origin-class", help="interactive|noninteractive|subagent|imported|unknown (comma list)")
    p.add_argument("--client", help="substring of client (entrypoint/originator)")
    p.add_argument("--account", help="substring of root label (codex-home, orca-acct-...)")
    p.add_argument("--subagents", choices=["exclude", "include", "only"], default="exclude")
    p.add_argument("--include-test", action="store_true", help="include test roots (probes, agentryx test homes)")
    p.add_argument("--sid", help="restrict to one session id/prefix")


def filter_sql(a, con, alias="s"):
    w, args = [], []
    if a.provider:
        w.append(f"{alias}.provider=?")
        args.append(a.provider)
    if a.project:
        w.append(f"({alias}.project LIKE ? OR {alias}.project_root LIKE ?)")
        args += [f"%{a.project}%"] * 2
    if a.cwd:
        w.append(f"{alias}.cwd LIKE ?")
        args.append(f"%{a.cwd}%")
    if a.alias:
        w.append(f"{alias}.sid IN (SELECT sid FROM aliases WHERE alias LIKE ?)")
        args.append(f"%{a.alias}%")
    if a.since:
        w.append(f"{alias}.last_activity_at >= ?")
        args.append(a.since)
    if a.until:
        w.append(f"{alias}.started_at < ?")
        args.append(a.until)
    if a.min_human is not None:
        w.append(f"{alias}.human_msgs >= ?")
        args.append(a.min_human)
    if a.min_human_chars is not None:
        w.append(f"{alias}.human_chars >= ?")
        args.append(a.min_human_chars)
    if a.has_event:
        ts = [t.strip() for t in a.has_event.split(",") if t.strip()]
        w.append(f"{alias}.sid IN (SELECT sid FROM events WHERE type IN ({','.join('?' * len(ts))}))")
        args += ts
    if a.origin_class:
        oc = [x.strip() for x in a.origin_class.split(",")]
        w.append(f"{alias}.origin_class IN ({','.join('?' * len(oc))})")
        args += oc
    if a.client:
        w.append(f"{alias}.client LIKE ?")
        args.append(f"%{a.client}%")
    if a.account:
        w.append(f"{alias}.account LIKE ?")
        args.append(f"%{a.account}%")
    if a.subagents == "exclude":
        w.append(f"{alias}.is_subagent=0")
    elif a.subagents == "only":
        w.append(f"{alias}.is_subagent=1")
    if not a.include_test:
        w.append(f"COALESCE({alias}.is_test,0)=0")
    if getattr(a, "sid", None):
        w.append(f"{alias}.sid=?")
        args.append(resolve(con, a.sid))
    return (" AND ".join(w) if w else "1=1"), args


# ------------------------------------------------------------------ commands

def cmd_guide(a):
    live = {"db": DB_PATH}
    if os.path.exists(DB_PATH):
        con = ro()
        for k, q in (("sessions", "SELECT COUNT(*) FROM sessions"), ("messages", "SELECT COUNT(*) FROM messages"),
                     ("events", "SELECT COUNT(*) FROM events")):
            live[k] = con.execute(q).fetchone()[0]
        r = con.execute("SELECT value FROM meta WHERE key='last_ingest'").fetchone()
        live["last_ingest"] = r[0] if r else None
    secs = a.section.split(",") if a.section else ["overview", "schema", "kinds", "events", "aliases", "examples",
                                                     "caveats"]
    sys.stdout.write(guide_mod.build(secs, live) + "\n")


def cmd_stats(a):
    con = ro()
    o = Out(a.format)
    by = a.by
    base_w, args = filter_sql(a, con)
    if by in ("provider", "account", "project", "month", "origin_class", "client", "kind"):
        key = {"provider": "s.provider", "account": "s.provider||':'||s.account", "project": "s.project",
               "month": "substr(s.started_at,1,7)", "origin_class": "s.origin_class", "client": "s.client",
               "kind": "s.kind"}[by]
        q = (f"SELECT {key} k, COUNT(*) sessions, SUM(s.human_msgs) human_msgs, SUM(s.human_chars) human_chars,"
             f" SUM(s.assistant_msgs) assistant_msgs, SUM(s.tool_calls) tool_calls, SUM(s.errors) errors,"
             f" SUM(s.interrupts) interrupts, SUM(s.compactions) compactions, ROUND(SUM(s.active_s)/3600.0,1) active_h"
             f" FROM sessions s WHERE {base_w} GROUP BY k ORDER BY {'k' if by == 'month' else 'sessions DESC'}"
             f" LIMIT ?")
        for r in con.execute(q, args + [a.limit]):
            o.emit({by: r[0], "sessions": r[1], "human_msgs": r[2], "human_chars": r[3], "assistant_msgs": r[4],
                    "tool_calls": r[5], "errors": r[6], "interrupts": r[7], "compactions": r[8], "active_h": r[9]})
    elif by == "files":
        for r in con.execute("SELECT root_id, role, COUNT(*), ROUND(SUM(size)/1e9,2), SUM(n_bad), SUM(partial_tail)"
                             " FROM files GROUP BY root_id, role ORDER BY root_id, role"):
            o.emit({"root": r[0], "role": r[1], "files": r[2], "gb": r[3], "bad_lines": r[4], "partial_tails": r[5]})
    elif by == "msg_kind":
        for r in con.execute(f"SELECT m.kind, COUNT(*), SUM(m.text_len) FROM messages m JOIN sessions s ON s.sid=m.sid"
                             f" WHERE {base_w} GROUP BY m.kind ORDER BY 2 DESC", args):
            o.emit({"kind": r[0], "messages": r[1], "chars": r[2]})
    elif by == "event":
        for r in con.execute(f"SELECT e.type, e.source LIKE 'home:%', COUNT(*), COUNT(DISTINCT e.sid) FROM events e"
                             f" JOIN sessions s ON s.sid=e.sid WHERE {base_w} GROUP BY 1,2 ORDER BY 3 DESC", args):
            o.emit({"type": r[0], "from_home_db": bool(r[1]), "events": r[2], "sessions": r[3]})
    elif by == "summary":
        tot = con.execute(f"SELECT COUNT(*), SUM(human_msgs), SUM(msgs_n), MIN(started_at), MAX(last_activity_at)"
                          f" FROM sessions s WHERE {base_w}", args).fetchone()
        o.emit({"sessions": tot[0], "human_msgs": tot[1], "messages": tot[2], "first": tot[3], "last": tot[4],
                "db_mb": round(os.path.getsize(DB_PATH) / 1e6, 1)})
        for r in con.execute("SELECT stats, started_at, finished_at FROM ingest_runs WHERE finished_at IS NOT NULL "
                             "ORDER BY run_id DESC LIMIT 1"):
            o.emit({"last_ingest": r[2], "stats": json.loads(r[0] or "{}")})
    o.flush()


def cmd_sessions(a):
    con = ro()
    o = Out(a.format)
    w, args = filter_sql(a, con)
    order = SORTS.get(a.sort, "s.last_activity_at")
    fields = a.fields.split(",") if a.fields else SESSION_FIELDS
    cols = ", ".join(f"s.{f}" for f in fields)
    q = (f"SELECT {cols} FROM sessions s WHERE {w} ORDER BY {order} {'ASC' if a.asc else 'DESC'} NULLS LAST "
         f"LIMIT ? OFFSET ?")
    try:
        cur = con.execute(q, args + [a.limit, a.offset])
    except sqlite3.OperationalError as e:
        die(str(e))
    for r in cur:
        d = dict(zip(fields, r))
        if "title" in d:
            d["title"] = clip(d["title"], a.width)
        for k in ("started_at", "last_activity_at"):
            if d.get(k) and not a.full_ts:
                d[k] = d[k][:16]
        if d.get("active_s") is not None:
            d["active_min"] = round(d.pop("active_s") / 60, 1)
        o.emit(d)
    o.flush()


def cmd_show(a):
    con = ro()
    sid = resolve(con, a.id)
    o = Out(a.format)
    cur = con.execute("SELECT * FROM sessions WHERE sid=?", (sid,))
    cols = [c[0] for c in cur.description]
    s = dict(zip(cols, cur.fetchone()))
    meta = {k: s[k] for k in ("sid", "provider", "pid", "kind", "origin_class", "client", "source", "account",
                              "cwd", "project", "project_method", "git_branch", "model", "cli_version", "started_at",
                              "last_activity_at", "duration_s", "active_s", "human_msgs", "human_chars",
                              "assistant_msgs", "tool_calls", "tool_errors", "errors", "interrupts", "compactions",
                              "subagent_spawns", "denials", "max_ctx_tokens", "ctx_window", "title", "title_origin",
                              "parent_sid", "agent_type", "agent_name", "n_files", "n_copies", "inherited_msgs")
            if s.get(k) not in (None, "", 0) or k in ("human_msgs", "sid")}
    meta["aliases"] = [{"alias": clip(r[0], 80), "origin": r[1], "manual": r[2]} for r in con.execute(
        "SELECT alias, origin, manual FROM aliases WHERE sid=? ORDER BY origin", (sid,))][:12]
    meta["events"] = {r[0]: r[1] for r in con.execute(
        "SELECT type, COUNT(*) FROM events WHERE sid=? GROUP BY type ORDER BY 2 DESC", (sid,))}
    meta["links"] = [{"rel": r[0], "dst": r[1], "dst_pid": r[2], "exists": bool(r[3])} for r in con.execute(
        "SELECT l.rel, l.dst_sid, l.dst_pid, (SELECT 1 FROM sessions x WHERE x.sid=l.dst_sid) FROM links l "
        "WHERE l.src_sid=? LIMIT 20", (sid,))]
    kids = con.execute("SELECT COUNT(*) FROM sessions WHERE parent_sid=?", (sid,)).fetchone()[0]
    meta["children"] = kids
    if kids:
        meta["children_sample"] = [{"sid": r[0], "agent_type": r[1], "name": clip(r[2], 60), "msgs": r[3]}
                                   for r in con.execute("SELECT sid, agent_type, COALESCE(agent_name,title), msgs_n "
                                                        "FROM sessions WHERE parent_sid=? ORDER BY started_at LIMIT ?",
                                                        (sid, a.children))]
    try:
        if "ann" in [r[1] for r in con.execute("PRAGMA database_list")]:
            meta["notes"] = con.execute("SELECT COUNT(*) FROM ann.annotations WHERE sid=?", (sid,)).fetchone()[0]
    except sqlite3.Error:
        pass
    meta["files"] = [{"path": r[0], "role": r[1], "relation": r[2]} for r in con.execute(
        "SELECT path, role, relation FROM files WHERE sid=? ORDER BY role LIMIT ?", (sid, a.files))]
    if a.format == "jsonl":
        o.emit({"session": meta})
    else:
        for k, v in meta.items():
            o.text(f"{k}: {v if not isinstance(v, (list, dict)) else json.dumps(v, ensure_ascii=False)}")
        o.text("-- outline (one line per prompt turn)")
    kinds = list(HUMAN_KINDS)
    if s["is_subagent"] or not s["human_msgs"]:
        kinds += ["agent_prompt", "sdk_prompt"]
    q = (f"SELECT seq, ts, kind, text, text_len, turn, inherited, imported FROM messages WHERE sid=? AND kind IN "
         f"({','.join('?' * len(kinds))}) ORDER BY seq LIMIT ?")
    ev_by_turn = {}
    for t, typ, n in con.execute("SELECT m.turn, e.type, COUNT(*) FROM events e JOIN messages m ON m.sid=e.sid AND "
                                 "m.seq=e.seq WHERE e.sid=? AND e.type IN ('interrupt','tool_error','tool_denied',"
                                 "'api_error','rate_limit','compaction','subagent_spawn') GROUP BY 1,2", (sid,)):
        ev_by_turn.setdefault(t, {})[typ] = n
    for r in con.execute(q, [sid] + kinds + [a.max_turns]):
        row = {"seq": r[0], "ts": (r[1] or "")[:16], "turn": r[5], "kind": r[2], "len": r[4],
               "text": clip((r[3] or "").replace("\n", " "), a.width)}
        if r[6] or r[7]:
            row["copied"] = "inherited" if r[6] else "imported"
        if ev_by_turn.get(r[5]):
            row["ev"] = ev_by_turn[r[5]]
        o.emit(row)
    o.flush(a.width)


def parse_range(s):
    if s is None:
        return None, None
    if "-" in s:
        x, y = s.split("-", 1)
        return (int(x) if x else None), (int(y) if y else None)
    return int(s), int(s)


def cmd_read(a):
    con = ro()
    sid = resolve(con, a.id)
    o = Out(a.format)
    w, args = ["sid=?"], [sid]
    lo, hi = parse_range(a.seq)
    if lo is not None:
        w.append("seq>=?")
        args.append(lo)
    if hi is not None:
        w.append("seq<=?")
        args.append(hi)
    tlo, thi = parse_range(a.turn)
    if tlo is not None:
        w.append("turn>=?")
        args.append(tlo)
    if thi is not None:
        w.append("turn<=?")
        args.append(thi)
    if a.kinds:
        ks = [k.strip() for k in a.kinds.split(",")]
        if "human*" in ks:
            ks = [k for k in ks if k != "human*"] + list(HUMAN_KINDS)
        w.append(f"kind IN ({','.join('?' * len(ks))})")
        args += ks
    if a.exclude_kinds:
        ks = [k.strip() for k in a.exclude_kinds.split(",")]
        w.append(f"kind NOT IN ({','.join('?' * len(ks))})")
        args += ks
    if a.roles:
        rs = [k.strip() for k in a.roles.split(",")]
        w.append(f"role IN ({','.join('?' * len(rs))})")
        args += rs
    if a.no_tools:
        w.append("kind NOT IN ('tool_call','tool_output')")
    if not a.with_injected:
        w.append("kind NOT IN ('developer','context_injection','system_reminder','meta','command_expansion')")
    if not a.with_copied:
        w.append("inherited=0")
    q = (f"SELECT seq, ts, role, kind, text, text_len, truncated, tool_name, is_error, turn, model, inherited, imported"
         f" FROM messages WHERE {' AND '.join(w)} ORDER BY seq LIMIT ? OFFSET ?")
    for r in con.execute(q, args + [a.limit, a.offset]):
        text = r[4] or ""
        if not a.full:
            n = a.tool_width if r[3] in ("tool_call", "tool_output") else a.width
            text = clip(text, n)
        row = {"seq": r[0], "ts": (r[1] or "")[:19], "turn": r[9], "kind": r[3]}
        if r[7]:
            row["tool"] = r[7]
        if r[8]:
            row["err"] = 1
        row["len"] = r[5]
        if r[6]:
            row["capped"] = 1
        if r[12]:
            row["imported"] = 1
        row["text"] = text
        o.emit(row)
    o.flush(a.width)


def cmd_raw(a):
    con = ro()
    sid = resolve(con, a.id)
    if a.event is not None:
        r = con.execute("SELECT e.file_id, e.offset, f.path FROM events e JOIN files f ON f.file_id=e.file_id "
                        "WHERE e.sid=? AND e.eid=?", (sid, a.event)).fetchone()
    else:
        r = con.execute("SELECT m.file_id, m.offset, f.path FROM messages m JOIN files f ON f.file_id=m.file_id "
                        "WHERE m.sid=? AND m.seq=?", (sid, a.seq)).fetchone()
    if not r:
        die("record not found")
    with open(r[2], "rb") as fh:  # read-only
        fh.seek(r[1])
        line = fh.readline()
    txt = line.decode("utf-8", "replace").rstrip("\n")
    info = {"path": r[2], "offset": r[1], "bytes": len(line)}
    if not a.full and len(txt) > a.max_chars:
        info["truncated_to"] = a.max_chars
        txt = txt[:a.max_chars]
    sys.stdout.write(json.dumps(info, ensure_ascii=False) + "\n")
    sys.stdout.write(txt + "\n")


def _fts_query(q: str, phrase: bool) -> str:
    if phrase:
        return '"' + q.replace('"', '""') + '"'
    return q


def cmd_search(a):
    con = ro()
    o = Out(a.format)
    w, args = filter_sql(a, con)
    kinds = None
    if a.human:
        kinds = list(HUMAN_KINDS)
    elif a.kinds:
        kinds = [k.strip() for k in a.kinds.split(",")]
    kw, kargs = "", []
    if kinds:
        kw = f" AND m.kind IN ({','.join('?' * len(kinds))})"
        kargs = kinds
    if not a.with_copied:
        kw += " AND m.inherited=0"
    terms = re.findall(r'"([^"]+)"|(\S+)', a.query)
    toks = [x or y for x, y in terms]
    short = any(len(t.strip('"*()')) < 3 for t in toks if t.upper() not in ("AND", "OR", "NOT"))
    use_like = a.like or short
    order = "m.ts DESC" if a.sort == "recent" else ("m.ts ASC" if a.sort == "oldest" else "rank")
    if use_like:
        if short and not a.like:
            sys.stderr.write("[sx] term shorter than 3 chars: trigram FTS cannot match; using LIKE scan (slower)\n")
        like = a.query.strip('"')
        q = (f"SELECT m.sid, m.seq, m.ts, m.kind, s.project, m.text, m.text_len FROM messages m JOIN sessions s "
             f"ON s.sid=m.sid WHERE m.text LIKE ? AND {w}{kw} ORDER BY {order if order != 'rank' else 'm.ts DESC'} "
             f"LIMIT ? OFFSET ?")
        cur = con.execute(q, [f"%{like}%"] + args + kargs + [a.limit, a.offset])
        for r in cur:
            t = r[5] or ""
            i = t.lower().find(like.lower())
            snip = t[max(0, i - a.context): i + len(like) + a.context] if i >= 0 else t[:2 * a.context]
            o.emit({"sid": r[0], "seq": r[1], "ts": (r[2] or "")[:16], "kind": r[3], "project": r[4], "len": r[6],
                    "snippet": snip.replace("\n", " ")})
    else:
        fq = _fts_query(a.query, a.phrase)
        q = (f"SELECT m.sid, m.seq, m.ts, m.kind, s.project, snippet(messages_fts,0,'[',']','…',?), m.text_len "
             f"FROM messages_fts JOIN messages m ON m.mid=messages_fts.rowid JOIN sessions s ON s.sid=m.sid "
             f"WHERE messages_fts MATCH ? AND {w}{kw} ORDER BY {order} LIMIT ? OFFSET ?")
        try:
            cur = con.execute(q, [a.tokens, fq] + args + kargs + [a.limit, a.offset])
            for r in cur:
                o.emit({"sid": r[0], "seq": r[1], "ts": (r[2] or "")[:16], "kind": r[3], "project": r[4],
                        "len": r[6], "snippet": (r[5] or "").replace("\n", " ")})
        except sqlite3.OperationalError as e:
            die(f"fts query error: {e} (quote phrases with double quotes, or use --phrase)")
    o.flush(100)


READONLY_RE = re.compile(r"^\s*(WITH|SELECT|EXPLAIN|PRAGMA\s+table_info|VALUES)\b", re.I)


def cmd_sql(a):
    con = ro()
    o = Out(a.format)
    q = a.query.strip().rstrip(";")
    if ";" in q:
        die("one statement only")
    if not READONLY_RE.match(q):
        die("read-only: only SELECT/WITH/EXPLAIN/VALUES/PRAGMA table_info statements are allowed")
    try:
        cur = con.execute(q)
    except sqlite3.Error as e:
        die(f"sql error: {e}")
    cols = [c[0] for c in cur.description] if cur.description else []
    n = 0
    for r in cur:
        if not a.all and n >= a.limit:
            sys.stderr.write(f"[sx] stopped at --limit {a.limit} rows (use --all)\n")
            break
        d = {c: (clip(v, a.width) if (isinstance(v, str) and not a.full) else v) for c, v in zip(cols, r)}
        o.emit(d)
        n += 1
    o.flush(a.width)


def cmd_note(a):
    con = ro()
    sid = resolve(con, a.id)
    anchor = None
    if a.seq is not None:
        r = con.execute("SELECT m.file_id, m.offset, s.pid FROM messages m JOIN sessions s ON s.sid=m.sid "
                        "WHERE m.sid=? AND m.seq=?", (sid, a.seq)).fetchone()
        if not r:
            die(f"seq {a.seq} not found in {sid}")
        anchor = json.dumps({"pid": r[2], "file_id": r[0], "offset": r[1]})
    con.close()
    acon = connect_rw(ANN_PATH)
    init_ann(acon)
    cur = acon.execute("INSERT INTO annotations(sid,seq,tag,note,author,created_at,anchor) VALUES(?,?,?,?,?,?,?)",
                       (sid, a.seq, a.tag, a.note, a.author or os.environ.get("SX_AUTHOR") or "agent", now_iso(),
                        anchor))
    acon.commit()
    sys.stdout.write(json.dumps({"ok": True, "id": cur.lastrowid, "sid": sid}) + "\n")


def cmd_notes(a):
    if not os.path.exists(ANN_PATH):
        return
    con = sqlite3.connect("file:" + ANN_PATH.replace("\\", "/") + "?mode=ro", uri=True)
    o = Out(a.format)
    w, args = [], []
    if a.id:
        w.append("sid LIKE ?")
        args.append(a.id + "%")
    if a.tag:
        w.append("tag=?")
        args.append(a.tag)
    if a.author:
        w.append("author=?")
        args.append(a.author)
    q = "SELECT id,sid,seq,tag,note,author,created_at FROM annotations" + (" WHERE " + " AND ".join(w) if w else "") + \
        " ORDER BY id DESC LIMIT ?"
    for r in con.execute(q, args + [a.limit]):
        o.emit({"id": r[0], "sid": r[1], "seq": r[2], "tag": r[3], "note": r[4], "author": r[5], "at": r[6]})
    o.flush(80)


def cmd_ingest(a):
    from .ingest import run_ingest
    stats = run_ingest(source=a.source.split(",") if a.source else None, rebuild=a.rebuild, workers=a.workers,
                       max_jobs=a.max_jobs)
    sys.stdout.write(json.dumps(stats, ensure_ascii=False) + "\n")


# ------------------------------------------------------------------ parser

def build_parser():
    p = argparse.ArgumentParser(prog="sx", description="Query the local AI-session index. Start with: sx guide")
    p.add_argument("--format", choices=["jsonl", "table"], default="jsonl")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--format", choices=["jsonl", "table"], default=argparse.SUPPRESS,
                        help="jsonl (default) or table")
    sub = p.add_subparsers(dest="cmd", required=True)
    _orig_add = sub.add_parser

    def _add(name, **kw):
        kw.setdefault("parents", [common])
        return _orig_add(name, **kw)
    sub.add_parser = _add

    g = sub.add_parser("guide", help="schema, kinds, events, examples, caveats")
    g.add_argument("--section", help="comma list: overview,schema,kinds,events,aliases,examples,caveats")
    g.set_defaults(fn=cmd_guide)

    s = sub.add_parser("stats", help="counts by provider/account/project/month/origin_class/client/kind/files/msg_kind/event/summary")
    s.add_argument("--by", default="summary",
                   choices=["summary", "provider", "account", "project", "month", "origin_class", "client", "kind",
                            "files", "msg_kind", "event"])
    s.add_argument("--limit", type=int, default=50)
    add_filters(s)
    s.set_defaults(fn=cmd_stats)

    s = sub.add_parser("sessions", help="list sessions with filters and sorts")
    add_filters(s)
    s.add_argument("--sort", default="last", choices=list(SORTS))
    s.add_argument("--asc", action="store_true")
    s.add_argument("--limit", type=int, default=20)
    s.add_argument("--offset", type=int, default=0)
    s.add_argument("--fields", help="comma list of sessions columns (default compact set)")
    s.add_argument("--width", type=int, default=60, help="title clip width")
    s.add_argument("--full-ts", action="store_true")
    s.set_defaults(fn=cmd_sessions)

    s = sub.add_parser("show", help="session metadata + one line per prompt turn")
    s.add_argument("id")
    s.add_argument("--max-turns", type=int, default=80)
    s.add_argument("--width", type=int, default=120)
    s.add_argument("--children", type=int, default=10)
    s.add_argument("--files", type=int, default=5)
    s.set_defaults(fn=cmd_show)

    s = sub.add_parser("read", help="messages of a session (range, kind/role filters)")
    s.add_argument("id")
    s.add_argument("--seq", help="seq range N, N-M, N-, -M")
    s.add_argument("--turn", help="turn range N, N-M")
    s.add_argument("--kinds", help="comma list (use human* for all human kinds)")
    s.add_argument("--exclude-kinds")
    s.add_argument("--roles")
    s.add_argument("--no-tools", action="store_true")
    s.add_argument("--with-injected", action="store_true", help="include developer/context/system_reminder/meta")
    s.add_argument("--with-copied", action="store_true", help="include inherited fork-prefix messages")
    s.add_argument("--full", action="store_true", help="no truncation")
    s.add_argument("--width", type=int, default=800)
    s.add_argument("--tool-width", type=int, default=240)
    s.add_argument("--limit", type=int, default=60)
    s.add_argument("--offset", type=int, default=0)
    s.set_defaults(fn=cmd_read)

    s = sub.add_parser("raw", help="original JSON line from the source file")
    s.add_argument("id")
    s.add_argument("--seq", type=int)
    s.add_argument("--event", type=int, help="events.eid instead of --seq")
    s.add_argument("--full", action="store_true")
    s.add_argument("--max-chars", type=int, default=20000)
    s.set_defaults(fn=cmd_raw)

    s = sub.add_parser("search", help="FTS5 trigram search over message text")
    s.add_argument("query")
    add_filters(s)
    s.add_argument("--human", action="store_true", help="only human kinds")
    s.add_argument("--kinds")
    s.add_argument("--phrase", action="store_true", help="treat the query as one literal phrase")
    s.add_argument("--like", action="store_true", help="force substring LIKE scan")
    s.add_argument("--sort", choices=["rank", "recent", "oldest"], default="rank")
    s.add_argument("--with-copied", action="store_true")
    s.add_argument("--limit", type=int, default=20)
    s.add_argument("--offset", type=int, default=0)
    s.add_argument("--tokens", type=int, default=16, help="snippet size in tokens")
    s.add_argument("--context", type=int, default=80, help="LIKE snippet context chars")
    s.set_defaults(fn=cmd_search)

    s = sub.add_parser("sql", help="read-only SQL (index opened mode=ro; annotations attached as ann)")
    s.add_argument("query")
    s.add_argument("--limit", type=int, default=200)
    s.add_argument("--all", action="store_true")
    s.add_argument("--full", action="store_true", help="do not clip long text cells")
    s.add_argument("--width", type=int, default=300)
    s.set_defaults(fn=cmd_sql)

    s = sub.add_parser("note", help="write an annotation (tag/note) on a session or message")
    s.add_argument("id")
    s.add_argument("--seq", type=int)
    s.add_argument("--tag")
    s.add_argument("--note", required=True)
    s.add_argument("--author")
    s.set_defaults(fn=cmd_note)

    s = sub.add_parser("notes", help="read annotations")
    s.add_argument("id", nargs="?")
    s.add_argument("--tag")
    s.add_argument("--author")
    s.add_argument("--limit", type=int, default=50)
    s.set_defaults(fn=cmd_notes)

    s = sub.add_parser("ingest", help="incremental ingest (all sources by default)")
    s.add_argument("--source", help="comma list: claude,codex,cc_transcripts or root ids/labels")
    s.add_argument("--rebuild", action="store_true", help="drop index.sqlite and re-parse (annotations kept)")
    s.add_argument("--workers", type=int)
    s.add_argument("--max-jobs", type=int, help="parse at most N sessions (evenly sampled) - for trials")
    s.set_defaults(fn=cmd_ingest)
    return p


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except AttributeError:
        pass
    p = build_parser()
    a = p.parse_args(argv)
    try:
        a.fn(a)
    except BrokenPipeError:
        pass
