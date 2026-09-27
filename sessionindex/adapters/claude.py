"""Claude Code adapter: <config>/projects/**/<session>.jsonl (+ subagents/**/agent-*.jsonl).

Record formats were derived from samples of Claude Code 2.1.x transcripts (see README).
"""
from __future__ import annotations

import collections
import json
import os
import re

from ..classify import claude_user_text_kind, classify_split, slash_text, bash_text
from ..util import iso_to_epoch, norm_ts, jdump, ms_to_iso
from .base import Candidate, ParseOut, read_lines

PROVIDER = "claude"
TOKENS_LEFT_RE = re.compile(r"<total_tokens>\s*(\d+)")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
AGENT_RE = re.compile(r"^agent-(.+)\.jsonl$")
IMPORT_VERSIONS = ("codex-import", "pass-the-thread")
DENIAL_PATTERNS = ("The user doesn't want to proceed with this tool use",
                   "The user doesn't want to take this action right now",
                   "User rejected", "has been denied", "Permission denied by user")
SDK_ENTRYPOINTS = ("sdk-cli", "sdk-ts", "sdk-py", "sdk")


def discover(root):
    for sub in ("projects", "projects-archived"):
        base = os.path.join(root.path, sub)
        if not os.path.isdir(base):
            continue
        archived = sub != "projects"
        for proj in os.scandir(base):
            if not proj.is_dir():
                continue
            for ent in os.scandir(proj.path):
                name = ent.name
                if ent.is_file() and name.endswith(".jsonl"):
                    yield Candidate(ent.path, name[:-6], "main",
                                    {"slug": proj.name, "archived": archived})
                elif ent.is_dir() and UUID_RE.match(name):
                    sa = os.path.join(ent.path, "subagents")
                    if os.path.isdir(sa):
                        yield from _walk_subagents(sa, name, proj.name, archived)


def _walk_subagents(sa_dir, parent_sid, slug, archived):
    stack = [sa_dir]
    while stack:
        d = stack.pop()
        for ent in os.scandir(d):
            if ent.is_dir():
                stack.append(ent.path)
                continue
            m = AGENT_RE.match(ent.name)
            if not m or ent.name.endswith(".meta.json"):
                continue
            agent_id = m.group(1)
            rel = os.path.relpath(d, sa_dir)
            wf = None
            if rel != "." and rel.replace("\\", "/").startswith("workflows/"):
                wf = rel.replace("\\", "/").split("/")[1] if "/" in rel.replace("\\", "/") else None
            yield Candidate(ent.path, f"{parent_sid}:agent-{agent_id}",
                            "workflow_agent" if wf else "subagent",
                            {"slug": slug, "archived": archived, "parent_pkey": parent_sid,
                             "agent_id": agent_id, "workflow": wf,
                             "meta_path": ent.path[:-6] + ".meta.json"})


def probe_last_ts(path):
    return _probe(path)


def owner_pkey(path, pkeys):
    """Owner among logical keys that reach the same physical file: the sessionId recorded inside it."""
    try:
        with open(path, "rb") as fh:
            m = re.search(rb'"sessionId":"([^"]+)"', fh.read(65536))
    except OSError:
        return None
    if m:
        sid = m.group(1).decode("utf-8", "replace")
        for k in pkeys:
            if k.split(":", 1)[0] == sid:
                return k
    return None


def _probe(path):
    first = last = None
    try:
        with open(path, "rb") as fh:
            head = fh.read(65536)
            for m in re.finditer(rb'"timestamp":"([^"]+)"', head):
                first = m.group(1).decode()
                break
            sz = os.fstat(fh.fileno()).st_size
            fh.seek(max(0, sz - 262144))
            tail = fh.read()
            ms = re.findall(rb'"timestamp":"([^"]+)"', tail)
            if ms:
                last = ms[-1].decode()
    except OSError:
        pass
    return first, last


def _blocks_text(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts = []
    if isinstance(content, list):
        for b in content:
            if not isinstance(b, dict):
                parts.append(str(b))
                continue
            t = b.get("type")
            if t == "text":
                parts.append(b.get("text") or "")
            elif t == "image":
                parts.append("[image]")
            elif t == "document":
                parts.append("[document]")
            elif t == "tool_reference":
                parts.append(f"[tool_reference {b.get('tool_name', '')}]")
            else:
                parts.append(json.dumps(b, ensure_ascii=False)[:2000])
    else:
        parts.append(str(content))
    return "\n".join(parts)


def parse(job) -> dict:
    caps = job["caps"]
    ctx = job["ctx"]
    st = job.get("state") or {"seq": 0, "turn": 0, "lines": 0, "model": None, "perm": None,
                              "tools": {}, "out_by_id": {}}
    out = ParseOut(caps, st)
    is_sub_file = job["subkind"] in ("subagent", "workflow_agent")
    meta = out.meta
    tools = st.setdefault("tools", {})
    out_by_id = st.setdefault("out_by_id", {})
    models = collections.Counter()
    first_ts = last_ts = None
    cwd = git_branch = version = entrypoint = user_type = None
    imported_versions = collections.Counter()
    n_bad = 0
    line_no = st.get("lines", 0)
    end_off = job["start"]
    partial = 0
    queued_rows = []
    out_delta = 0

    if job["start"] == 0 and is_sub_file:
        mp = ctx.get("meta_path")
        if mp and os.path.exists(mp):
            try:
                with open(mp, encoding="utf-8") as fh:
                    am = json.load(fh)
                meta["agent_meta"] = {k: am.get(k) for k in ("agentType", "description", "toolUseId", "spawnDepth",
                                                            "model", "parentAgentId", "stoppedByUser",
                                                            "requestShape", "requestNonInteractive")}
            except (OSError, ValueError):
                meta["agent_meta_error"] = 1

    for off, raw, complete in read_lines(job["path"], job["start"]):
        try:
            r = json.loads(raw)
        except ValueError:
            if not complete:
                partial = 1
                break
            n_bad += 1
            line_no += 1
            end_off = off + len(raw)
            continue
        this_line = line_no
        line_no += 1
        end_off = off + len(raw)
        if not isinstance(r, dict):
            n_bad += 1
            continue
        typ = r.get("type")
        ts = norm_ts(r.get("timestamp"))
        if ts:
            if first_ts is None:
                first_ts = ts
            last_ts = ts
        if typ in ("user", "assistant", "system", "attachment"):
            cwd = cwd or r.get("cwd")
            gb = r.get("gitBranch")
            if gb:
                git_branch = gb
            version = r.get("version") or version
            if not st.get("entrypoint") and r.get("entrypoint"):
                st["entrypoint"] = r.get("entrypoint")
            entrypoint = entrypoint or r.get("entrypoint")
            user_type = user_type or r.get("userType")
        ver = r.get("version") or ""
        imported = 1 if any(v in ver for v in IMPORT_VERSIONS) else 0
        if imported:
            imported_versions[ver] += 1
        side = 1 if (r.get("isSidechain") and not is_sub_file) else 0
        is_sub = is_sub_file or bool(r.get("isSidechain"))
        sdk_client = (st.get("entrypoint") or "") in SDK_ENTRYPOINTS

        if typ == "user":
            out.rec_counts["user"] += 1
            msg = r.get("message") or {}
            content = msg.get("content")
            origin = r.get("origin") if isinstance(r.get("origin"), dict) else {}
            okind = origin.get("kind")
            psrc = r.get("promptSource")
            orig_tag = f"{okind or ''}/{psrc or ''}" if (okind or psrc) else None
            is_meta = bool(r.get("isMeta"))
            is_compact = bool(r.get("isCompactSummary"))
            blocks = [{"type": "text", "text": content}] if isinstance(content, str) else (content or [])
            pending_text = []  # (kind, text)
            tool_interrupt = None
            marker_seen = False
            for b in blocks:
                if not isinstance(b, dict):
                    continue
                bt = b.get("type")
                if bt == "tool_result":
                    txt = _blocks_text(b.get("content"))
                    tid = b.get("tool_use_id")
                    tname = tools.get(tid)
                    is_err = 1 if b.get("is_error") else 0
                    out.msg(ts, "tool", "tool_output", txt, off=off, line=this_line, tool_name=tname,
                            tool_id=tid, is_error=is_err, imported=imported, sidechain=side)
                    denial_kind = r.get("toolDenialKind")
                    if denial_kind or (is_err and txt and any(p in txt[:300] for p in DENIAL_PATTERNS)):
                        out.ev(ts, "tool_denied", f"{tname}: {denial_kind or txt[:200]}", off=off)
                    elif txt.startswith("[Request interrupted by user"):
                        tool_interrupt = tname or "?"
                    elif is_err:
                        out.ev(ts, "tool_error", f"{tname}: {txt[:200]}", off=off)
                elif bt == "text":
                    t = b.get("text") or ""

                    def _k(x):
                        return claude_user_text_kind(x, is_meta=is_meta, is_compact=is_compact, okind=okind,
                                                     psrc=psrc, is_sub=is_sub, sdk_client=sdk_client)
                    for k, chunk in classify_split(t, _k):
                        if k == "human_command":
                            chunk = slash_text(chunk)
                        elif k == "human_shell":
                            chunk = bash_text(chunk)
                        pending_text.append((k, chunk))
                elif bt == "image":
                    pending_text.append(("image", "[image]"))
                else:
                    pending_text.append(("meta", json.dumps(b, ensure_ascii=False)[:2000]))
            # attach image placeholders to adjacent prompt text; merge consecutive same kinds
            merged = []
            for k, t in pending_text:
                if k == "image" and merged and merged[-1][0] in ("human", "sdk_prompt", "agent_prompt"):
                    merged[-1] = (merged[-1][0], merged[-1][1] + "\n[image]")
                    continue
                if merged and merged[-1][0] == k:
                    merged[-1] = (k, merged[-1][1] + "\n" + t)
                else:
                    merged.append((k, t))
            for idx, (k, t) in enumerate(merged):
                if k == "image":
                    # image-only prompt from a person counts as a human turn
                    nk = "agent_prompt" if is_sub else ("human" if (okind in (None, "human") and not is_meta) else "meta")
                    k = nk
                row = out.msg(ts, "user", k, t, off=off, line=this_line, origin=orig_tag,
                              imported=imported, sidechain=side)
                if k == "interrupt_marker":
                    marker_seen = True
                    out.ev(ts, "interrupt", "tool_use" if "tool use" in t[:60] else "response", off=off)
            if tool_interrupt and not marker_seen:
                out.ev(ts, "interrupt", f"tool_use {tool_interrupt}", off=off)
            continue

        if typ == "assistant":
            out.rec_counts["assistant"] += 1
            msg = r.get("message") or {}
            model = msg.get("model")
            if model and model != "<synthetic>":
                models[model] += 1
                if st.get("model") and st["model"] != model:
                    out.ev(ts, "model_switch", f"{st['model']} -> {model}", off=off, seq=st["seq"])
                st["model"] = model
            usage = msg.get("usage") or {}
            ctxt = None
            if usage:
                ctxt = (usage.get("input_tokens") or 0) + (usage.get("cache_creation_input_tokens") or 0) + \
                       (usage.get("cache_read_input_tokens") or 0)
                mid = msg.get("id")
                if mid:
                    newv = usage.get("output_tokens") or 0
                    out_delta += newv - out_by_id.get(mid, 0)
                    out_by_id[mid] = newv
            api_err = bool(r.get("isApiErrorMessage"))
            if api_err or r.get("error"):
                err = r.get("error")
                status = r.get("apiErrorStatus")
                txt = _blocks_text(msg.get("content"))
                et = "rate_limit" if (err == "rate_limit" or status == 429) else "api_error"
                out.ev(ts, et, f"{err} {status}: {txt[:200]}", off=off, seq=st["seq"])
            for b in msg.get("content") or []:
                if not isinstance(b, dict):
                    continue
                bt = b.get("type")
                if bt == "text":
                    out.msg(ts, "assistant", "api_error" if api_err else "assistant", b.get("text") or "",
                            off=off, line=this_line, model=model, imported=imported, sidechain=side,
                            ctx_tokens=ctxt or None)
                elif bt == "thinking":
                    th = b.get("thinking") or ""
                    if th.strip():
                        out.msg(ts, "assistant", "reasoning", th, off=off, line=this_line, model=model,
                                imported=imported, sidechain=side)
                elif bt in ("tool_use", "server_tool_use"):
                    name = b.get("name")
                    tid = b.get("id")
                    tools[tid] = name
                    inp = b.get("input")
                    args = jdump(inp) if not isinstance(inp, str) else inp
                    out.msg(ts, "assistant", "tool_call", args, off=off, line=this_line, tool_name=name,
                            tool_id=tid, model=model, imported=imported, sidechain=side, ctx_tokens=ctxt or None)
                    if name in ("Task", "Agent") and isinstance(inp, dict):
                        out.ev(ts, "subagent_spawn", f"{inp.get('subagent_type', '')}: {inp.get('description', '')}",
                               off=off)
                elif bt in ("web_search_tool_result", "advisor_tool_result", "server_tool_result"):
                    out.msg(ts, "tool", "tool_output", _blocks_text(b.get("content")), off=off, line=this_line,
                            tool_name=bt, tool_id=b.get("tool_use_id"), imported=imported, sidechain=side)
                else:
                    out.rec_counts[f"assistant_block:{bt}"] += 1
            if r.get("isAbortedMidStream"):
                out.ev(ts, "interrupt", "aborted_mid_stream", off=off)
            continue

        if typ == "system":
            sub = r.get("subtype")
            out.rec_counts[f"system:{sub}"] += 1
            if sub == "compact_boundary":
                cm = r.get("compactMetadata") or {}
                out.ev(ts, "compaction", cm.get("trigger"), cm.get("preTokens"), off=off)
            elif sub == "api_error":
                e = r.get("error")
                out.ev(ts, "api_retry", f"attempt={r.get('retryAttempt')} {str(e)[:200]}", off=off)
            elif sub == "turn_duration":
                out.ev(ts, "turn_duration", None, r.get("durationMs"), off=off)
            elif sub == "stop_hook_summary":
                errs = r.get("hookErrors") or []
                if errs:
                    out.ev(ts, "hook_error", jdump(errs)[:300], off=off)
                if r.get("preventedContinuation"):
                    out.ev(ts, "hook_blocked", str(r.get("stopReason") or "")[:300], off=off)
            elif sub == "model_refusal_fallback":
                out.ev(ts, "model_fallback", f"{r.get('originalModel')} -> {r.get('fallbackModel')} "
                                              f"({r.get('apiRefusalCategory')})", off=off)
            elif sub == "agents_killed":
                out.ev(ts, "agents_killed", str(r.get("content") or "")[:200], off=off)
            elif sub == "local_command":
                c = r.get("content")
                if c:
                    out.msg(ts, "system", "command_output", str(c), off=off, line=this_line, count_turn=False)
            else:
                c = r.get("content")
                if c:
                    out.msg(ts, "system", "system", f"[{sub}] {c}", off=off, line=this_line, count_turn=False)
            continue

        if typ == "attachment":
            a = r.get("attachment") or {}
            at = a.get("type")
            out.rec_counts[f"attachment:{at}"] += 1
            if at == "queued_command":
                ao = a.get("origin") if isinstance(a.get("origin"), dict) else {}
                okind = ao.get("kind")
                prompt = a.get("prompt")
                if isinstance(prompt, list):
                    prompt = _blocks_text(prompt)
                if prompt:
                    if okind in (None, "human"):
                        # queued text still goes through the content rules (queued <task-notification>s etc.)
                        k = "human_shell" if a.get("commandMode") == "bash" else claude_user_text_kind(
                            str(prompt), is_meta=False, is_compact=False, okind=okind, psrc=None, is_sub=is_sub,
                            sdk_client=sdk_client)
                    elif okind == "task-notification":
                        k = "task_notification"
                    elif okind == "peer":
                        k = "peer_message"
                    else:
                        k = "meta"
                    row = out.msg(norm_ts(a.get("timestamp")) or ts, "user", k, str(prompt), off=off,
                                  line=this_line, origin=f"queued_command/{okind or ''}", imported=imported,
                                  sidechain=side)
                    if k == "human":
                        queued_rows.append(row)
            elif at == "hook_blocking_error":
                out.ev(ts, "hook_blocking_error", f"{a.get('hookName')}: {str(a.get('blockingError'))[:200]}", off=off)
            elif at == "hook_cancelled":
                out.ev(ts, "hook_timeout" if a.get("timedOut") else "hook_cancelled", a.get("hookName"),
                       a.get("durationMs"), off=off)
            elif at == "hook_non_blocking_error":
                out.ev(ts, "hook_error", f"{a.get('hookName')} exit={a.get('exitCode')}: "
                                         f"{str(a.get('stderr') or a.get('stdout') or '')[:200]}", off=off)
            elif at == "max_turns_reached":
                out.ev(ts, "max_turns", f"{a.get('turnCount')}/{a.get('maxTurns')}", a.get("maxTurns"), off=off)
            elif at == "total_tokens_reminder":
                mt = TOKENS_LEFT_RE.search(str(a.get("text") or ""))
                if mt:
                    st["tokens_left"] = int(mt.group(1))
            elif at == "goal_status":
                out.ev(ts, "goal_status", f"met={a.get('met')}: {str(a.get('condition') or '')[:300]}", off=off)
            elif at == "edited_text_file":
                out.ev(ts, "external_file_edit", a.get("filename"), off=off)
            if at in ("hook_success", "hook_additional_context", "hook_system_message"):
                c = a.get("content")
                c = _blocks_text(c) if isinstance(c, list) else (c if isinstance(c, str) else "")
                if c.strip():
                    out.msg(ts, "system", "hook_output", c, off=off, line=this_line, count_turn=False,
                            origin=f"{at}:{a.get('hookName') or a.get('hookEvent') or ''}", imported=imported,
                            sidechain=side)
            if a.get("hookEvent") == "SessionStart" and a.get("hookName"):
                src_kind = str(a.get("hookName")).split(":", 1)[-1]
                prev_ss = st.get("last_ss") or [None, None]
                t_now = iso_to_epoch(ts)
                # several hooks fire per start: emit one event per (source, start)
                if prev_ss[0] != src_kind or t_now is None or prev_ss[1] is None or abs(t_now - prev_ss[1]) > 10:
                    typ_ev = {"resume": "resume", "clear": "context_clear", "startup": "session_start"}.get(src_kind)
                    if typ_ev:
                        out.ev(ts, typ_ev, f"SessionStart:{src_kind}", off=off)
                    st["last_ss"] = [src_kind, t_now]
            continue

        out.rec_counts[str(typ)] += 1
        if typ == "ai-title":
            out.alias(r.get("aiTitle"), "claude_ai_title", 0, last_ts)
        elif typ == "custom-title":
            out.alias(r.get("customTitle"), "claude_custom_title", 1, last_ts)
        elif typ == "agent-name":
            out.alias(r.get("agentName"), "claude_agent_name", 0, last_ts)
        elif typ == "summary":
            out.alias(r.get("summary"), "claude_summary", 0, last_ts)
        elif typ == "continued-in":
            out.link("continued_in", "claude", r.get("continuedInSessionId"), "continued-in record")
            out.ev(ts, "continued_in", r.get("continuedInSessionId"), off=off)
        elif typ == "pr-link":
            out.fact("pr_url", r.get("prUrl"), "claude_pr_link", ts)
        elif typ == "permission-mode":
            pm = r.get("permissionMode")
            if pm != st.get("perm"):
                out.ev(ts or last_ts, "permission_mode", pm, off=off)
                st["perm"] = pm
        elif typ == "cost-state":
            out.fact("cost_usd", r.get("totalCostUSD"), "claude_cost_state", last_ts)
        elif typ == "relocated":
            out.ev(ts or last_ts, "relocated", r.get("relocatedCwd"), off=off)
        elif typ == "fork-context-ref":
            out.ev(ts or last_ts, "fork", f"context from {r.get('parentSessionId')} "
                                          f"(contextLength={r.get('contextLength')})", off=off)

    # drop queued_command copies whose identical text was also delivered as a user record
    if queued_rows:
        drop = {id(rw) for rw in queued_rows if rw[18] in _other_human_hashes(out.messages, queued_rows)}
        if drop:
            out.messages = [rw for rw in out.messages if id(rw) not in drop]

    # bound resume state
    if len(tools) > 400:
        st["tools"] = dict(list(tools.items())[-400:])
    if len(out_by_id) > 20:
        st["out_by_id"] = dict(list(out_by_id.items())[-20:])
    meta["out_tokens_add"] = out_delta
    st["lines"] = line_no
    meta.update({
        "cwd": cwd, "git_branch": git_branch, "cli_version": version, "client": entrypoint,
        "user_type": user_type, "models": dict(models), "first_ts": first_ts, "last_ts": last_ts,
        "imported_versions": dict(imported_versions), "slug": ctx.get("slug"), "archived": ctx.get("archived"),
        "tokens_left_last": st.get("tokens_left"),
    })
    return out.result(end_offset=end_off, n_lines=line_no, n_bad=n_bad, partial_tail=partial)


def _other_human_hashes(messages, queued_rows):
    qids = {id(r) for r in queued_rows}
    return {r[18] for r in messages if r[3] == "human" and id(r) not in qids}


# ---------------------------------------------------------------- per-root metadata

def home_files(root):
    return [os.path.join(root.path, f) for f in ("history.jsonl", "codex-import-history.json")]


def home_metadata(root) -> dict:
    """~/.claude/history.jsonl (typed prompts) and codex-import-history.json (import links)."""
    res = {"prompt_history": [], "links": [], "aliases": [], "facts": [], "events": []}
    hp = os.path.join(root.path, "history.jsonl")
    if os.path.exists(hp):
        with open(hp, "rb") as fh:
            for raw in fh:
                try:
                    r = json.loads(raw)
                except ValueError:
                    continue
                sid = r.get("sessionId")
                disp = r.get("display")
                if disp:
                    res["prompt_history"].append(["claude", sid, ms_to_iso(r.get("timestamp")), disp, r.get("project")])
    ip = os.path.join(root.path, "codex-import-history.json")
    if os.path.exists(ip):
        try:
            with open(ip, encoding="utf-8") as fh:
                d = json.load(fh)
            for rec in d.get("records") or []:
                src = rec.get("sourceRolloutPath") or ""
                m = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$", src)
                if rec.get("importedSessionId") and m:
                    res["links"].append(["claude", rec["importedSessionId"], "imported_from", "codex", m.group(1),
                                         "codex-import-history.json"])
        except (OSError, ValueError):
            pass
    return res
