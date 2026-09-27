"""~/.claude/transcripts/ses_*.jsonl: Claude-Code-compatible transcripts written by another client.

The `ses_` id prefix matches OpenCode session ids (inference; the writer is not recorded in the files).
Records: {type: user|tool_use|tool_result, timestamp, content | tool_name, tool_input, tool_output}.
No assistant text is recorded in this format.
"""
from __future__ import annotations

import collections
import json
import os

from ..classify import claude_user_text_kind, classify_split
from ..util import norm_ts, jdump
from .base import Candidate, ParseOut, read_lines

PROVIDER = "opencode"


# delegation prompts written by an orchestrator agent (oh-my-opencode style task handoffs); heuristic
DELEGATION_PREFIXES = ("[CONTEXT]", "## TASK", "# TASK", "1. TASK")


def _kind(t: str) -> str:
    k = claude_user_text_kind(t, is_meta=False, is_compact=False, okind=None, psrc=None, is_sub=False,
                              sdk_client=False)
    if k == "human" and t.lstrip().startswith(DELEGATION_PREFIXES):
        return "agent_prompt"
    return k


def discover(root):
    for ent in os.scandir(root.path):
        if ent.is_file() and ent.name.endswith(".jsonl"):
            yield Candidate(ent.path, ent.name[:-6], "main", {})


def probe_last_ts(path):
    return None, None


def parse(job) -> dict:
    st = job.get("state") or {"seq": 0, "turn": 0, "lines": 0}
    out = ParseOut(job["caps"], st)
    first_ts = last_ts = None
    n_bad = 0
    partial = 0
    line_no = st.get("lines", 0)
    end_off = job["start"]
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
        t = r.get("type")
        out.rec_counts[str(t)] += 1
        ts = norm_ts(r.get("timestamp"))
        if ts:
            first_ts = first_ts or ts
            last_ts = ts
        if t == "user":
            c = r.get("content")
            txt = c if isinstance(c, str) else jdump(c)
            for k, chunk in classify_split(txt, _kind):
                out.msg(ts, "user", k, chunk, off=off, line=this_line, origin="cc_transcript")
        elif t == "tool_use":
            inp = r.get("tool_input")
            out.msg(ts, "assistant", "tool_call", inp if isinstance(inp, str) else jdump(inp), off=off,
                    line=this_line, tool_name=r.get("tool_name"))
        elif t == "tool_result":
            o = r.get("tool_output")
            txt = o if isinstance(o, str) else jdump(o)
            out.msg(ts, "tool", "tool_output", txt, off=off, line=this_line, tool_name=r.get("tool_name"))
    st["lines"] = line_no
    out.meta.update({"first_ts": first_ts, "last_ts": last_ts, "models": {}, "client": "cc_transcripts"})
    return out.result(end_offset=end_off, n_lines=line_no, n_bad=n_bad, partial_tail=partial)


def home_metadata(root):
    return None
