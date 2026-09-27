"""Adapter contract and the ParseOut accumulator shared by all adapters.

An adapter provides:
  PROVIDER: str                                   provider name stored in sessions.provider
  discover(root) -> iterable[Candidate]           cheap path walk (runs in main process)
  parse(job) -> dict                              parses one physical file from job['start'] (runs in worker)
  home_metadata(root) -> dict | None              optional per-root metadata (aliases/links/facts/events)
  probe_last_ts(path) -> (first_ts, last_ts)      optional cheap head/tail probe for canonical selection
"""
from __future__ import annotations

import collections
import os
from dataclasses import dataclass, field

from ..classify import PROMPT_KINDS
from ..util import text_hash, trunc

FULL_KINDS = {"human", "human_command", "human_shell", "sdk_prompt", "agent_prompt", "assistant",
              "reasoning", "compaction_summary", "peer_message", "agent_message"}


@dataclass
class Candidate:
    path: str
    pkey: str               # provider session id (logical key)
    subkind: str            # main | subagent | workflow_agent | transcript
    ctx: dict = field(default_factory=dict)


class ParseOut:
    """Collects rows while parsing one file chunk. Rows are plain lists (picklable)."""

    def __init__(self, caps: dict, state: dict):
        self.caps = caps
        self.st = state
        self.messages: list[list] = []
        self.events: list[list] = []
        self.aliases: list[list] = []
        self.links: list[list] = []
        self.facts: list[list] = []
        self.meta: dict = {}
        self.rec_counts: collections.Counter = collections.Counter()

    def cap_for(self, kind: str) -> int | None:
        if kind in FULL_KINDS:
            return None
        if kind == "tool_output":
            return self.caps["tool_output_chars"]
        if kind == "tool_call":
            return self.caps["tool_args_chars"]
        return self.caps["injected_chars"]

    def msg(self, ts, role, kind, text, *, off, line, tool_name=None, tool_id=None, is_error=0,
            model=None, origin=None, imported=0, sidechain=0, ctx_tokens=None, count_turn=True):
        st = self.st
        if count_turn and kind in PROMPT_KINDS and not sidechain:
            st["turn"] = st.get("turn", 0) + 1
        cap = self.cap_for(kind)
        text = text if text is None or isinstance(text, str) else str(text)
        h = text_hash(text if cap is None else (text[:4096] if text else text))
        if cap is None:
            t, L, tr = text, (len(text) if text else 0), 0
        elif kind == "tool_output" and text and len(text) > cap:
            # keep head and tail: errors usually appear at the end of long outputs
            head = cap * 5 // 8
            tail = cap - head
            mark = f"\n…[{len(text) - cap} chars omitted]…\n"
            t, L, tr = text[:head] + mark + text[-tail:], len(text), 1
        else:
            t, L, tr = trunc(text, cap)
        seq = st["seq"]
        st["seq"] = seq + 1
        row = [seq, ts, role, kind, t, L, tr, tool_name, tool_id, is_error, model, origin,
               st.get("turn", 0), off, line, imported, 0, sidechain, h, ctx_tokens]
        self.messages.append(row)
        return row

    def ev(self, ts, typ, detail=None, value=None, *, off=None, seq=None):
        if detail is not None and not isinstance(detail, str):
            detail = str(detail)
        if detail and len(detail) > 500:
            detail = detail[:500]
        self.events.append([self.st["seq"] - 1 if seq is None else seq, ts, typ, detail, value, off])

    def alias(self, alias, origin, manual, ts):
        if alias and str(alias).strip():
            self.aliases.append([str(alias).strip()[:500], origin, manual, ts])

    def link(self, rel, dst_provider, dst_pid, provenance):
        if dst_pid:
            self.links.append([rel, dst_provider, str(dst_pid), provenance])

    def fact(self, key, value, origin, ts=None):
        if value is not None and value != "":
            self.facts.append([key, str(value)[:2000], origin, ts])

    def result(self, **kw) -> dict:
        d = {
            "messages": self.messages, "events": self.events, "aliases": self.aliases,
            "links": self.links, "facts": self.facts, "meta": self.meta,
            "rec_counts": dict(self.rec_counts), "state": self.st,
        }
        d.update(kw)
        return d


def read_lines(path: str, start: int):
    """Yield (offset, raw_line_bytes, complete) from byte offset `start` (binary, streaming)."""
    with open(path, "rb", buffering=1 << 20) as fh:
        fh.seek(start)
        off = start
        for raw in fh:
            yield off, raw, raw.endswith(b"\n")
            off += len(raw)


# ---------------------------------------------------------------- light catalog helper (no body parse)

CAT_BYTES = 65536


def read_head_tail(path, n=CAT_BYTES):
    """(head_lines, tail_lines, size): complete lines of the first and last n bytes (read-only)."""
    with open(path, "rb") as fh:
        head = fh.read(n)
        size = os.fstat(fh.fileno()).st_size
        tail = b""
        if size > n:
            start = max(n, size - n)
            fh.seek(start)
            tail = fh.read()
            if start > 0:
                tail = tail[tail.find(b"\n") + 1:]
    hl = head.split(b"\n")
    if size > n:
        hl = hl[:-1]  # last head line is partial
    return [x for x in hl if x.strip()], [x for x in tail.split(b"\n") if x.strip()], size
