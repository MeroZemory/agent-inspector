"""Codex CLI / Desktop adapter: <CODEX_HOME>/sessions/**/rollout-*.jsonl and archived_sessions/*.jsonl.

Line format (codex-rs/protocol RolloutLine): {"timestamp", "ordinal"?, "type", "payload"}.
Types: session_meta, response_item, event_msg, turn_context, compacted, world_state,
token_usage_record, inter_agent_communication_metadata. Heavy/duplicate records are skipped
by a header regex without JSON parsing (counted in rec_counts).
"""
from __future__ import annotations

import collections
import json
import os
import re
import sqlite3

from ..classify import codex_user_text_kind
from ..util import iso_to_epoch, norm_ts, jdump, text_hash, ms_to_iso
from .base import Candidate, ParseOut, read_lines

PROVIDER = "codex"
ROLLOUT_RE = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$")
HDR = re.compile(rb'^\{"timestamp":"([^"]*)",(?:"ordinal":(\d+),)?"type":"([a-z_]+)"(?:,"payload":\{"type":"([a-z_]+)")?')
ITEM_TYPE = re.compile(rb'"item":\{"type":"([A-Za-z]+)"')
FAIL_STATUS = re.compile(rb'"status":"(failed|declined)"')

# originators that drive Codex programmatically (their user-role prompts are not typed by a person)
PROGRAMMATIC_ORIGINATORS = ("codex_exec", "codex_sdk_ts", "agentryx-runner", "agentryx-probe")
SKIP_TYPES = {"world_state", "token_usage_record", "inter_agent_communication_metadata"}
SKIP_EVENTS = {
    "exec_command_output_delta", "agent_message_content_delta", "agent_message_delta", "agent_reasoning_delta",
    "reasoning_content_delta", "reasoning_raw_content_delta", "agent_reasoning_raw_content",
    "agent_reasoning_raw_content_delta", "agent_reasoning_section_break", "plan_delta", "raw_response_item",
    "raw_response_completed", "item_started", "sub_agent_activity", "mcp_startup_update", "mcp_startup_complete",
    "exec_command_begin", "exec_command_end", "patch_apply_begin", "patch_apply_updated", "mcp_tool_call_begin",
    "web_search_begin", "terminal_interaction", "turn_diff", "session_configured", "thread_settings_applied",
    "environment_connected", "environment_disconnected", "image_generation_begin", "view_image_tool_call",
    "background_event", "mcp_list_tools_response", "list_custom_prompts_response", "get_history_entry_response",
    "hook_started", "collab_agent_spawn_begin", "collab_agent_interaction_begin", "collab_waiting_begin",
    "turn_moderation_metadata", "safety_buffering", "dynamic_tool_call_request", "dynamic_tool_call_response",
}
UNION_ITEMS = {"AgentMessage", "UserMessage", "Reasoning"}
TOOL_ERR_RE = re.compile(r"^(?:Exit code: (-?\d+)|Script failed|collab spawn failed|Process exited with code (-?\d+))")
ABORT_RE = re.compile(r"^aborted by user")
IMAGE_MARKUP_RE = re.compile(r"<image\b[^>]*>|</image>|\[image\]")
LIMIT_RE = re.compile(r"usage limit|rate limit|rate_limit|429|quota|too many requests", re.I)


def discover(root):
    sess = os.path.join(root.path, "sessions")
    if os.path.isdir(sess):
        stack = [sess]
        while stack:
            d = stack.pop()
            try:
                it = os.scandir(d)
            except OSError:
                continue
            with it:
                for ent in it:
                    if ent.is_dir():
                        stack.append(ent.path)
                    elif ent.name.endswith(".jsonl"):
                        m = ROLLOUT_RE.search(ent.name)
                        if m:
                            yield Candidate(ent.path, m.group(1), "main", {"archived": False})
    arch = os.path.join(root.path, "archived_sessions")
    if os.path.isdir(arch):
        for ent in os.scandir(arch):
            if ent.is_file() and ent.name.endswith(".jsonl"):
                m = ROLLOUT_RE.search(ent.name)
                if m:
                    yield Candidate(ent.path, m.group(1), "main", {"archived": True})


def probe_last_ts(path):
    """Cheap head/tail probe: first line timestamp and last complete line timestamp."""
    first = last = None
    try:
        with open(path, "rb") as fh:
            m = HDR.match(fh.readline()[:300])
            if m:
                first = m.group(1).decode()
            sz = os.fstat(fh.fileno()).st_size
            fh.seek(max(0, sz - 262144))
            tail = fh.read()
            for line in reversed(tail.split(b"\n")):
                m = HDR.match(line[:300])
                if m:
                    last = m.group(1).decode()
                    break
    except OSError:
        pass
    return first, last


def _content_text(content) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    parts = []
    if isinstance(content, list):
        for b in content:
            if isinstance(b, dict):
                # response_item blocks use snake_case types; item_completed blocks use "Text"/"Image"
                t = str(b.get("type") or "").lower()
                if t in ("input_text", "output_text", "text", "summary_text", "reasoning_text"):
                    parts.append(b.get("text") or "")
                elif "image" in t:
                    parts.append("[image]")
                elif isinstance(b.get("text"), str):
                    parts.append(b["text"])
                else:
                    parts.append(json.dumps(b, ensure_ascii=False)[:1000])
            else:
                parts.append(str(b))
    elif isinstance(content, dict):
        return json.dumps(content, ensure_ascii=False)
    else:
        return str(content)
    return "\n".join(parts)


def _src_str(src) -> str:
    if isinstance(src, str):
        return src
    if isinstance(src, dict) and src:
        k = next(iter(src))
        v = src[k]
        if k == "subagent":
            if isinstance(v, dict) and v:
                return "subagent:" + next(iter(v))
            return f"subagent:{v}"
        return f"{k}:{v if isinstance(v, str) else ''}"
    return str(src) if src is not None else ""


def _user_key(text):
    """Pairing hash for user prompts: response_item wraps images as '<image name=..>' + '</image>' blocks while
    event/item copies append '[image]'; ignore image markup so the copies pair."""
    return text_hash(IMAGE_MARKUP_RE.sub(" ", text or ""))


class _Pairer:
    """Dedupes one message recorded in up to three representations (response_item / event / item).

    Per content hash: emitted = copies kept so far; cnt[source] = copies seen from that source.
    A copy is kept only when its source has now seen more copies than were kept, so a triple
    (response_item + event + item) is stored once while a genuinely repeated text is stored again.
    """

    MAX = 400

    def __init__(self, saved=None):
        self.d = collections.OrderedDict()
        for ent in saved or []:
            if isinstance(ent, list) and len(ent) == 3 and isinstance(ent[2], dict):
                self.d[ent[0]] = [ent[1], dict(ent[2])]

    def seen_elsewhere(self, h, source) -> bool:
        if h is None:
            return False
        ent = self.d.get(h)
        if ent is None:
            ent = self.d[h] = [0, {}]
        else:
            self.d.move_to_end(h)
        c = ent[1].get(source, 0) + 1
        ent[1][source] = c
        if c <= ent[0]:
            return True
        ent[0] += 1
        while len(self.d) > self.MAX:
            self.d.popitem(last=False)
        return False

    def dump(self):
        return [[h, e[0], e[1]] for h, e in list(self.d.items())[-200:]]


def parse(job) -> dict:
    caps = job["caps"]
    st = job.get("state") or {"seq": 0, "turn": 0, "lines": 0}
    out = ParseOut(caps, st)
    meta = out.meta
    pair = _Pairer(st.get("pair"))
    calls = st.setdefault("calls", {})
    models = collections.Counter()
    first_ts = last_ts = None
    n_bad = 0
    partial = 0
    line_no = st.get("lines", 0)
    end_off = job["start"]
    last_asst_row = None
    tid = st.get("tid")
    is_sub = st.get("is_sub", False)
    nonint = st.get("nonint", False)
    cmp_t = st.get("cmp_t")  # epoch of the last `compacted` record
    rate_max = st.get("rate_max")

    def dup_parts(parts, source) -> bool:
        """Reasoning summaries arrive as one event per part but as a part list in response_item/item:
        pair part by part; the joined text is a duplicate only if every part was already stored."""
        parts = [p for p in parts if p and p.strip()]
        if not parts:
            return True
        return all([pair.seen_elsewhere(text_hash(p), source) for p in parts])

    def emit_user(ts, text, source, off, ln, origin=None):
        k = codex_user_text_kind(text, is_sub=is_sub, noninteractive=nonint)
        h = _user_key(text)
        if source != "response_item" and k in ("context_injection", "system_reminder"):
            return
        if pair.seen_elsewhere(h, source):
            return
        out.msg(ts, "user", k, text, off=off, line=ln, origin=origin or source)
        if k == "interrupt_marker":
            pass  # counted from turn_aborted events

    for off, raw, complete in read_lines(job["path"], job["start"]):
        this_line = line_no
        m = HDR.match(raw[:300])
        if m:
            ts = norm_ts(m.group(1).decode())
            typ = m.group(3).decode()
            sub = m.group(4).decode() if m.group(4) else None
            if not complete:
                # validate the tail record before consuming it
                try:
                    json.loads(raw)
                except ValueError:
                    partial = 1
                    break
            line_no += 1
            end_off = off + len(raw)
            if ts:
                if first_ts is None:
                    first_ts = ts
                last_ts = ts
            key = f"{typ}:{sub}" if sub else typ
            if typ in SKIP_TYPES or (typ == "event_msg" and sub in SKIP_EVENTS):
                out.rec_counts[key] += 1
                continue
            if typ == "event_msg" and sub == "item_completed":
                im = ITEM_TYPE.search(raw[:600])
                itype = im.group(1).decode() if im else None
                out.rec_counts[f"item_completed:{itype}"] += 1
                if itype not in UNION_ITEMS and not (itype in ("CommandExecution", "McpToolCall", "FileChange")
                                                     and FAIL_STATUS.search(raw)):
                    continue
            else:
                out.rec_counts[key] += 1
        else:
            line_no += 1
            end_off = off + len(raw)
            typ = sub = None
        try:
            r = json.loads(raw)
        except ValueError:
            if not complete:
                partial = 1
                line_no -= 1
                end_off = off
                break
            n_bad += 1
            continue
        if not isinstance(r, dict):
            n_bad += 1
            continue
        if typ is None:
            typ = r.get("type")
            ts = norm_ts(r.get("timestamp"))
            pl0 = r.get("payload")
            sub = pl0.get("type") if isinstance(pl0, dict) else None
            out.rec_counts[f"{typ}:{sub}" if sub else str(typ)] += 1
            if ts:
                first_ts = first_ts or ts
                last_ts = ts
        pl = r.get("payload")
        if not isinstance(pl, dict):
            pl = {}

        # ------------------------------------------------------------ session_meta
        if typ == "session_meta":
            pid = pl.get("id")
            if tid is None and this_line == 0:
                tid = pid
                st["tid"] = tid
                s_str, is_sub, nonint = _flags(pl)
                thread_src = pl.get("thread_source")
                st["is_sub"] = is_sub
                st["nonint"] = nonint
                git = pl.get("git") or {}
                meta.update({
                    "pid": pid, "cwd": pl.get("cwd"), "client": pl.get("originator"),
                    "cli_version": pl.get("cli_version"), "source": s_str, "thread_source": thread_src,
                    "agent_name": pl.get("agent_nickname"), "agent_type": pl.get("agent_role"),
                    "agent_path": pl.get("agent_path"), "model_provider": pl.get("model_provider"),
                    "forked_from": pl.get("forked_from_id"), "parent_thread": pl.get("parent_thread_id"),
                    "git_branch": git.get("branch"), "git_origin": git.get("repository_url"),
                    "git_sha": git.get("commit_hash"), "history_mode": pl.get("history_mode"),
                    "is_sub": is_sub, "nonint": nonint, "meta_ts": norm_ts(pl.get("timestamp")),
                })
                if pl.get("forked_from_id"):
                    out.link("forked_from", "codex", pl.get("forked_from_id"), "session_meta.forked_from_id")
                    out.ev(ts, "fork", pl.get("forked_from_id"), off=off)
                if pl.get("parent_thread_id"):
                    out.link("subagent_of" if is_sub else "parent_thread", "codex", pl.get("parent_thread_id"),
                             "session_meta.parent_thread_id")
                if pl.get("agent_nickname"):
                    out.alias(pl.get("agent_nickname"), "codex_agent_nickname", 0, ts)
            elif pid and pid == tid:
                out.ev(ts, "resume", "session_meta repeated", off=off)
            continue

        if typ == "turn_context":
            mdl = pl.get("model")
            if mdl:
                models[mdl] += 1
                if st.get("model") and st["model"] != mdl:
                    out.ev(ts, "model_switch", f"{st['model']} -> {mdl}", off=off, seq=st["seq"])
                st["model"] = mdl
            continue

        if typ == "compacted":
            msgtxt = pl.get("message") or ""
            if msgtxt:
                out.msg(ts, "system", "compaction_summary", msgtxt, off=off, line=this_line, count_turn=False)
            out.ev(ts, "compaction", f"window={pl.get('window_number')}", off=off)
            cmp_t = iso_to_epoch(ts)
            continue

        # ------------------------------------------------------------ response_item
        if typ == "response_item":
            it = pl.get("type")
            if it == "message":
                role = pl.get("role")
                content = pl.get("content")
                if role == "assistant":
                    txt = _content_text(content)
                    if not pair.seen_elsewhere(text_hash(txt), "response_item"):
                        last_asst_row = out.msg(ts, "assistant", "assistant", txt, off=off, line=this_line,
                                                model=st.get("model"), origin=pl.get("phase") or "response_item")
                elif role == "user":
                    blocks = content if isinstance(content, list) else [{"type": "input_text", "text": str(content)}]
                    groups = []
                    for b in blocks:
                        if not isinstance(b, dict):
                            continue
                        bt = b.get("type")
                        if bt in ("input_image", "image"):
                            if groups:
                                groups[-1][1] += "\n[image]"
                            else:
                                groups.append([None, "[image]"])
                            continue
                        t = b.get("text") or ""
                        k = codex_user_text_kind(t, is_sub=is_sub, noninteractive=nonint)
                        if groups and groups[-1][0] in (k, None):
                            groups[-1][0] = k
                            groups[-1][1] = (groups[-1][1] + "\n" + t) if groups[-1][1] else t
                        else:
                            groups.append([k, t])
                    for k, t in groups:
                        if k is None:
                            k = "agent_prompt" if is_sub else ("sdk_prompt" if nonint else "human")
                        h = _user_key(t)
                        if k not in ("context_injection", "system_reminder") and pair.seen_elsewhere(h, "response_item"):
                            continue
                        out.msg(ts, "user", k, t, off=off, line=this_line, origin="response_item")
                elif role == "developer":
                    t = _content_text(content)
                    k = "interrupt_marker" if t.lstrip().startswith("<turn_aborted>") else "developer"
                    out.msg(ts, "developer", k, t, off=off, line=this_line, count_turn=False)
                else:
                    out.msg(ts, role or "system", "system", _content_text(content), off=off, line=this_line,
                            count_turn=False)
            elif it == "reasoning":
                sm = pl.get("summary")
                parts = [b.get("text") or "" for b in sm if isinstance(b, dict)] if isinstance(sm, list) else []
                if not any(p.strip() for p in parts):
                    cont = pl.get("content")
                    parts = [_content_text(cont)] if isinstance(cont, list) else []
                if not dup_parts(parts, "response_item"):
                    out.msg(ts, "assistant", "reasoning", "\n".join(p for p in parts if p.strip()), off=off,
                            line=this_line, model=st.get("model"))
            elif it in ("function_call", "custom_tool_call", "local_shell_call", "web_search_call",
                        "image_generation_call", "tool_search_call"):
                name = pl.get("name") or {"local_shell_call": "local_shell", "web_search_call": "web_search",
                                          "image_generation_call": "image_generation",
                                          "tool_search_call": "tool_search"}.get(it, it)
                if pl.get("namespace") and name and not name.startswith("mcp__"):
                    name = f"{pl.get('namespace')}.{name}"
                cid = pl.get("call_id") or pl.get("id")
                if it == "function_call":
                    args = pl.get("arguments")
                elif it == "custom_tool_call":
                    args = pl.get("input")
                else:
                    args = jdump(pl.get("action") or pl.get("arguments") or {})
                if not isinstance(args, str):
                    args = jdump(args)
                calls[cid] = name
                last_asst_row = out.msg(ts, "assistant", "tool_call", args, off=off, line=this_line,
                                        tool_name=name, tool_id=cid, model=st.get("model"))
                if name in ("spawn_agent", "spawn_agents"):
                    try:
                        a = json.loads(args)
                    except ValueError:
                        a = {}
                    out.ev(ts, "subagent_spawn",
                           f"{a.get('agent_type') or a.get('role') or ''}: {str(a.get('message') or a.get('task') or '')[:150]}",
                           off=off)
            elif it in ("function_call_output", "custom_tool_call_output", "local_shell_call_output",
                        "tool_search_output"):
                cid = pl.get("call_id")
                name = calls.get(cid)
                o = pl.get("output")
                if isinstance(o, dict):
                    txt = o.get("content") if isinstance(o.get("content"), str) else jdump(o)
                else:
                    txt = _content_text(o) if isinstance(o, list) else (o if isinstance(o, str) else jdump(o))
                txt = txt or ""
                is_err = 0
                mm = TOOL_ERR_RE.match(txt[:200])
                if mm:
                    code = mm.group(1) or mm.group(2)
                    if code is None or code != "0":
                        is_err = 1
                        out.ev(ts, "tool_error", f"{name}: {txt[:200]}", off=off, seq=st["seq"])
                elif ABORT_RE.match(txt[:40]):
                    is_err = 1
                    out.ev(ts, "tool_aborted", f"{name}: {txt[:120]}", off=off, seq=st["seq"])
                elif "rejected by user" in txt[:300] or "declined by user" in txt[:300]:
                    is_err = 1
                    out.ev(ts, "tool_denied", f"{name}: {txt[:200]}", off=off, seq=st["seq"])
                out.msg(ts, "tool", "tool_output", txt, off=off, line=this_line, tool_name=name, tool_id=cid,
                        is_error=is_err)
            elif it == "agent_message":
                txt = _content_text(pl.get("content"))
                out.msg(ts, "agent", "agent_message", f"[{pl.get('author')} -> {pl.get('recipient')}] {txt}",
                        off=off, line=this_line, count_turn=False)
            else:
                out.rec_counts[f"response_item_other:{it}"] += 1
            continue

        # ------------------------------------------------------------ event_msg
        if typ == "event_msg":
            et = pl.get("type")
            if et == "token_count":
                info = pl.get("info") or {}
                last = info.get("last_token_usage") or {}
                tot = last.get("total_tokens")
                if info.get("model_context_window"):
                    st["ctx_window"] = info.get("model_context_window")
                if tot and last_asst_row is not None:
                    last_asst_row[19] = tot
                rl = pl.get("rate_limits") or {}
                prim = (rl.get("primary") or {}).get("used_percent") if isinstance(rl, dict) else None
                if prim is not None:
                    rate_max = max(rate_max or 0, prim)
            elif et == "task_started":
                st["turn_id"] = pl.get("turn_id")
                if pl.get("model_context_window"):
                    st["ctx_window"] = pl.get("model_context_window")
            elif et == "task_complete":
                if pl.get("duration_ms") is not None:
                    out.ev(ts, "turn_duration", None, pl.get("duration_ms"), off=off)
                if pl.get("error"):
                    out.ev(ts, "turn_error", jdump(pl.get("error"))[:300], off=off)
            elif et == "turn_aborted":
                reason = pl.get("reason")
                out.ev(ts, "interrupt" if reason == "interrupted" else "turn_aborted", reason, pl.get("duration_ms"),
                       off=off)
            elif et == "error":
                msg = str(pl.get("message") or "")
                out.ev(ts, "rate_limit" if LIMIT_RE.search(msg) else "api_error",
                       f"{pl.get('codex_error_info') or ''} {msg[:250]}".strip(), off=off)
            elif et == "stream_error":
                out.ev(ts, "stream_retry", str(pl.get("message") or "")[:250], off=off)
            elif et in ("warning", "guardian_warning"):
                out.ev(ts, "warning", str(pl.get("message") or "")[:250], off=off)
            elif et in ("exec_approval_request", "apply_patch_approval_request", "request_permissions"):
                cmd = pl.get("command") or pl.get("reason") or pl.get("changes")
                out.ev(ts, "approval_request", f"{et}: {jdump(cmd)[:200] if cmd is not None else ''}", off=off)
            elif et in ("request_user_input", "elicitation_request"):
                out.ev(ts, "user_input_request", jdump(pl)[:250], off=off)
            elif et == "guardian_assessment":
                out.ev(ts, "guardian_assessment", jdump({k: pl.get(k) for k in ("decision", "risk", "status", "reason")
                                                         if pl.get(k) is not None})[:250], off=off)
            elif et == "model_reroute":
                out.ev(ts, "model_switch", f"reroute {pl.get('from_model') or ''} -> {pl.get('to_model') or ''} "
                                           f"{pl.get('reason') or ''}", off=off)
            elif et == "thread_rolled_back":
                out.ev(ts, "rollback", None, pl.get("num_turns"), off=off)
            elif et == "context_compacted":
                t_now = iso_to_epoch(ts)
                # newer Codex writes `compacted` then `context_compacted` for the same compaction
                if cmp_t is None or t_now is None or abs(t_now - cmp_t) > 60:
                    out.ev(ts, "compaction", "context_compacted", off=off)
            elif et == "thread_goal_updated":
                g = pl.get("goal") or {}
                out.ev(ts, "goal_status", f"{g.get('status', '')}: {str(g.get('objective') or '')[:300]}", off=off)
            elif et == "thread_name_updated":
                out.alias(pl.get("thread_name") or pl.get("name"), "codex_thread_name_event", None, ts)
            elif et == "patch_apply_end":
                if pl.get("success") is False:
                    out.ev(ts, "tool_error", f"apply_patch: {str(pl.get('stderr') or '')[:200]}", off=off)
            elif et == "user_message":
                txt = pl.get("message") or ""
                imgs = pl.get("images") or pl.get("local_images")
                if imgs:
                    txt = (txt + "\n[image]").strip()
                if txt:
                    emit_user(ts, txt, "event", off, this_line)
            elif et == "agent_message":
                txt = pl.get("message") or ""
                if txt and not pair.seen_elsewhere(text_hash(txt), "event"):
                    last_asst_row = out.msg(ts, "assistant", "assistant", txt, off=off, line=this_line,
                                            model=st.get("model"), origin=f"event:{pl.get('phase') or ''}")
            elif et == "agent_reasoning":
                txt = pl.get("text") or ""
                if txt.strip() and not pair.seen_elsewhere(text_hash(txt), "event"):
                    out.msg(ts, "assistant", "reasoning", txt, off=off, line=this_line, model=st.get("model"),
                            origin="event")
            elif et == "item_completed":
                item = pl.get("item") or {}
                itype = item.get("type")
                if itype == "AgentMessage":
                    txt = _content_text(item.get("content"))
                    if txt and not pair.seen_elsewhere(text_hash(txt), "item"):
                        last_asst_row = out.msg(ts, "assistant", "assistant", txt, off=off, line=this_line,
                                                model=st.get("model"), origin=f"item:{item.get('phase') or ''}")
                elif itype == "UserMessage":
                    txt = _content_text(item.get("content"))
                    if txt:
                        emit_user(ts, txt, "item", off, this_line)
                elif itype == "Reasoning":
                    s = item.get("summary_text")
                    parts = [str(x) for x in s] if isinstance(s, list) else [s or ""]
                    if not dup_parts(parts, "item"):
                        out.msg(ts, "assistant", "reasoning", "\n".join(p for p in parts if p.strip()), off=off,
                                line=this_line, model=st.get("model"), origin="item")
                elif itype in ("CommandExecution", "McpToolCall", "FileChange"):
                    status = item.get("status")
                    what = item.get("command") or item.get("tool") or item.get("changes")
                    if isinstance(what, list):
                        what = " ".join(str(x) for x in what)[-200:]
                    if status == "declined":
                        out.ev(ts, "tool_denied", f"{itype}: {str(what)[:200]}", off=off)
                    elif status == "failed":
                        out.ev(ts, "tool_error", f"{itype} exit={item.get('exit_code')}: {str(what)[:200]}", off=off)
            continue

    st["pair"] = pair.dump()
    if len(calls) > 400:
        st["calls"] = dict(list(calls.items())[-400:])
    st["lines"] = line_no
    st["cmp_t"] = cmp_t
    st["rate_max"] = rate_max
    meta.update({"models": dict(models), "first_ts": first_ts, "last_ts": last_ts,
                 "ctx_window": st.get("ctx_window"), "rate_max_primary_pct": rate_max})
    return out.result(end_offset=end_off, n_lines=line_no, n_bad=n_bad, partial_tail=partial)


# ---------------------------------------------------------------- per-home metadata

def _ro(path):
    return sqlite3.connect("file:" + path.replace("\\", "/") + "?mode=ro", uri=True, timeout=10)


HOME_FILES = ("session_index.jsonl", "history.jsonl", "state_5.sqlite", "state_5.sqlite-wal",
              "thread_history_1.sqlite", "thread_history_1.sqlite-wal", "goals_1.sqlite", "goals_1.sqlite-wal",
              "external_agent_session_imports.json")


def home_files(root):
    return [os.path.join(root.path, f) for f in HOME_FILES]


def home_metadata(root) -> dict:
    """Aliases, links, facts, events and typed-prompt history from a CODEX_HOME's own files (read-only)."""
    res = {"prompt_history": [], "links": [], "aliases": [], "facts": [], "events": [], "errors": []}
    h = root.path
    si = os.path.join(h, "session_index.jsonl")
    if os.path.exists(si):
        last = {}
        with open(si, "rb") as fh:
            for raw in fh:
                try:
                    r = json.loads(raw)
                except ValueError:
                    continue
                if r.get("id") and r.get("thread_name"):
                    last[r["id"]] = (r["thread_name"], norm_ts(r.get("updated_at")))
        for pid, (name, ts) in last.items():
            res["aliases"].append(["codex", pid, name, "codex_session_index", 0, ts])
    hp = os.path.join(h, "history.jsonl")
    if os.path.exists(hp):
        with open(hp, "rb") as fh:
            for raw in fh:
                try:
                    r = json.loads(raw)
                except ValueError:
                    continue
                if r.get("text"):
                    res["prompt_history"].append(["codex", r.get("session_id"), ms_to_iso(r.get("ts")), r["text"], None])
    sp = os.path.join(h, "state_5.sqlite")
    if os.path.exists(sp):
        try:
            con = _ro(sp)
            cols = {r[1] for r in con.execute("PRAGMA table_info(threads)")}
            want = [c for c in ("id", "title", "name", "archived", "git_branch", "git_origin_url", "git_sha",
                                "tokens_used", "model", "reasoning_effort", "source", "thread_source",
                                "agent_nickname", "agent_role", "approval_mode", "sandbox_policy", "originator",
                                "updated_at_ms", "cwd") if c in cols]
            for row in con.execute(f"SELECT {','.join(want)} FROM threads"):
                d = dict(zip(want, row))
                pid = d.get("id")
                ts = ms_to_iso(d.get("updated_at_ms")) if d.get("updated_at_ms") else None
                if d.get("name"):
                    res["aliases"].append(["codex", pid, d["name"], "codex_state_name", 1, ts])
                if d.get("title"):
                    res["aliases"].append(["codex", pid, d["title"], "codex_state_title", 0, ts])
                for k in ("archived", "git_branch", "git_origin_url", "git_sha", "tokens_used", "model",
                          "reasoning_effort", "approval_mode", "sandbox_policy", "originator", "thread_source"):
                    if d.get(k) not in (None, ""):
                        res["facts"].append(["codex", pid, f"state.{k}", str(d[k])[:500], "codex_state_threads", ts])
            if "thread_spawn_edges" in {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}:
                for p, c, s in con.execute("SELECT parent_thread_id, child_thread_id, status FROM thread_spawn_edges"):
                    res["links"].append(["codex", c, "subagent_of", "codex", p, f"state_5.thread_spawn_edges status={s}"])
            con.close()
        except sqlite3.Error as e:
            res["errors"].append(f"state_5: {e}")
    tp = os.path.join(h, "thread_history_1.sqlite")
    if os.path.exists(tp):
        try:
            con = _ro(tp)
            for tid, turn, status, err, started in con.execute(
                    "SELECT thread_id, turn_id, status, error_json, started_at FROM thread_turns "
                    "WHERE status IN ('failed','interrupted')"):
                res["events"].append(["codex", tid, norm_ts(started) if started else None,
                                      f"state_turn_{status}", f"[turn {turn}] {(err or '')[:300]}", None])
            con.close()
        except sqlite3.Error as e:
            res["errors"].append(f"thread_history_1: {e}")
    gp = os.path.join(h, "goals_1.sqlite")
    if os.path.exists(gp):
        try:
            con = _ro(gp)
            for tid, obj, status, upd in con.execute("SELECT thread_id, objective, status, updated_at_ms FROM thread_goals"):
                res["facts"].append(["codex", tid, "goal", f"[{status}] {obj}"[:2000], "codex_goals",
                                     ms_to_iso(upd) if upd else None])
            con.close()
        except sqlite3.Error as e:
            res["errors"].append(f"goals_1: {e}")
    ip = os.path.join(h, "external_agent_session_imports.json")
    if os.path.exists(ip):
        try:
            with open(ip, encoding="utf-8") as fh:
                d = json.load(fh)
            for rec in d.get("records") or []:
                src = rec.get("source_path") or ""
                stem = os.path.basename(src.replace("\\", "/"))
                if stem.endswith(".jsonl") and rec.get("imported_thread_id"):
                    res["links"].append(["codex", rec["imported_thread_id"], "imported_from", "claude", stem[:-6],
                                         "external_agent_session_imports.json"])
        except (OSError, ValueError) as e:
            res["errors"].append(f"imports: {e}")
    return res


# ---------------------------------------------------------------- light catalog (head + tail only)

def catalog(job) -> dict:
    """Session metadata from the first line (session_meta) and the last 64 KB only."""
    path = job["path"]
    with open(path, "rb") as fh:
        first = fh.readline(16 << 20)
        size = os.fstat(fh.fileno()).st_size
        start = max(len(first), size - 65536)
        fh.seek(start)
        tail = fh.read()
    if start > len(first):
        tail = tail[tail.find(b"\n") + 1:]
    meta = {"models": {}}
    aliases, links = [], []
    try:
        r = json.loads(first)
    except ValueError:
        r = {}
    pl = r.get("payload") if isinstance(r, dict) and r.get("type") == "session_meta" else None
    if isinstance(pl, dict):
        s_str, is_sub, nonint = _flags(pl)
        git = pl.get("git") or {}
        ts = norm_ts(r.get("timestamp"))
        meta.update({
            "pid": pl.get("id"), "cwd": pl.get("cwd"), "client": pl.get("originator"),
            "cli_version": pl.get("cli_version"), "source": s_str, "thread_source": pl.get("thread_source"),
            "agent_name": pl.get("agent_nickname"), "agent_type": pl.get("agent_role"),
            "forked_from": pl.get("forked_from_id"), "parent_thread": pl.get("parent_thread_id"),
            "git_branch": git.get("branch"), "git_origin": git.get("repository_url"), "git_sha": git.get("commit_hash"),
            "is_sub": is_sub, "nonint": nonint, "first_ts": ts,
        })
        if pl.get("forked_from_id"):
            links.append(["forked_from", "codex", pl.get("forked_from_id"), "session_meta.forked_from_id"])
        if pl.get("parent_thread_id"):
            links.append(["subagent_of" if is_sub else "parent_thread", "codex", pl.get("parent_thread_id"),
                          "session_meta.parent_thread_id"])
        if pl.get("agent_nickname"):
            aliases.append([str(pl.get("agent_nickname"))[:500], "codex_agent_nickname", 0, ts])
    last_ts = meta.get("first_ts")
    for line in tail.split(b"\n"):
        m = HDR.match(line[:300])
        if not m:
            continue
        last_ts = norm_ts(m.group(1).decode()) or last_ts
        typ, sub = m.group(3), m.group(4)
        if typ == b"turn_context" or (typ == b"event_msg" and sub == b"thread_name_updated"):
            try:
                p2 = json.loads(line).get("payload") or {}
            except ValueError:
                continue
            if typ == b"turn_context" and p2.get("model"):
                meta["models"] = {p2["model"]: 1}
            elif p2.get("thread_name") or p2.get("name"):
                aliases.append([str(p2.get("thread_name") or p2.get("name"))[:500], "codex_thread_name_event", None,
                                last_ts])
    meta["last_ts"] = last_ts
    return {"meta": meta, "aliases": aliases, "links": links, "first_prompt": None, "size": size}


def _flags(pl):
    """(source string, is_subagent, non-interactive) from a session_meta payload."""
    s_str = _src_str(pl.get("source"))
    is_sub = s_str.startswith("subagent") or pl.get("thread_source") == "subagent" or bool(pl.get("agent_role")) \
        or bool(pl.get("agent_nickname"))
    orig = str(pl.get("originator") or "")
    nonint = (not is_sub) and (s_str in ("exec", "mcp") or s_str.startswith(("custom", "internal"))
                               or orig in PROGRAMMATIC_ORIGINATORS or orig.startswith("probe"))
    return s_str, is_sub, nonint
