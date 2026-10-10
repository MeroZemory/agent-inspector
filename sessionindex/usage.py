"""Token usage per model response, recorded at ingest as `usage` events (source='transcript').

detail is compact JSON; value = total tokens (input + output) of the row.
  m  model            e  reasoning effort     t  service tier
  i  input tokens, INCLUDING cached reads and cache writes
  c  cached input (cache reads)               w  cache-write input (Claude cache_creation; Codex
                                                 cache_write_input_tokens, 0 in every rollout seen so far)
  o  output tokens, INCLUDING reasoning       r  reasoning part of o (Codex reasoning_output_tokens,
                                                 Claude output_tokens_details.thinking_tokens; 0 when absent)
  n  responses counted by this row (1; 0 for a later Claude record of an already counted response that only
     adds changed counts)
  id response id (Codex response_id, Claude message.id)   tu Codex turn id   src 'token_count' (Codex fallback)
  side 1 = Claude sidechain record inside a main transcript

Codex: one row per `token_usage_record` (usage = that response; thread_id must be the rollout's own thread, so
records copied into a fork are not counted twice). Model / effort / tier = the latest turn_context or
thread_settings_applied seen before the record (effort: turn_context.effort or collaboration_mode settings;
tier: thread_settings_applied.service_tier, `default` or `priority` (= Fast); null before the first one).
Rollouts written before token_usage_record existed fall back to event_msg token_count.last_token_usage, one row
per change of the running total (src='token_count').
Claude: one row per assistant message.id (a response is written as one record per content block, each carrying
the same usage). i = input_tokens + cache_creation_input_tokens + cache_read_input_tokens. Effort = the record's
effort/perTurnEffort; tier = usage.service_tier, or 'fast' when usage.speed is 'fast'. <synthetic> messages and
imported records (codex-import, pass-the-thread) are skipped.
"""
from __future__ import annotations

import bisect
import json

from .timing import intersect, merge
from .util import iso_to_epoch

TOKEN_COLS = ("input", "cached", "cache_write", "output", "reasoning")
_FIELDS = (("i", "input"), ("c", "cached"), ("w", "cache_write"), ("o", "output"), ("r", "reasoning"))
DAY = 86400.0


def parse_detail(detail):
    try:
        d = json.loads(detail or "{}")
    except ValueError:
        return None
    return d if isinstance(d, dict) else None


def turn_intervals(rows, last_ts):
    """rows: (ts, type, detail) of turn_start/turn_end ordered by ts -> [(start, end)] epoch seconds, same rules as
    timing.session_breakdown (a new turn_start closes an open turn; an open last turn ends at the last record)."""
    out, cur = [], None
    for ts, typ, _ in rows:
        t = iso_to_epoch(ts)
        if t is None:
            continue
        if typ == "turn_start":
            if cur is not None:
                out.append((cur, t))
            cur = t
        elif typ == "turn_end" and cur is not None:
            out.append((cur, t))
            cur = None
    if cur is not None:
        lt = iso_to_epoch(last_ts)
        if lt is not None and lt > cur:
            out.append((cur, lt))
    return merge(out)


def day_split(iv):
    """Split intervals at UTC midnights -> [(start, end, 'YYYY-MM-DD')]."""
    import datetime as dt
    res = []
    for s, e in iv:
        while s < e:
            nxt = min(e, (s // DAY + 1) * DAY)
            res.append((s, nxt, dt.datetime.fromtimestamp(s, dt.timezone.utc).strftime("%Y-%m-%d")))
            s = nxt
    return res


def in_turn_by_group(turns, w0, w1, resp, key_fn, by):
    """Attribute in-turn seconds (turn intervals clipped to the window) to groups.
    resp: sorted list of (epoch, group_key, output_tokens) for one session.
    session/project: whole in-turn time to the session's group; day: split at UTC midnight;
    model/effort/tier: each turn segment split in proportion to the output tokens of its responses (by response
    count when it has no output); segments without responses go to group None (unattributed)."""
    segs = intersect(turns, [(w0, w1)])
    res = {}
    if by in ("session", "project"):
        k = key_fn(None)
        res[k] = sum(e - s for s, e in segs)
        return res
    if by == "day":
        for s, e, d in day_split(segs):
            res[d] = res.get(d, 0.0) + (e - s)
        return res
    ts = [r[0] for r in resp]
    for s, e in segs:
        a, b = bisect.bisect_left(ts, s), bisect.bisect_right(ts, e)
        share = {}
        for _, k, o in resp[a:b]:
            share[k] = share.get(k, 0.0) + (o or 0)
        tot = sum(share.values())
        if not share:
            res[None] = res.get(None, 0.0) + (e - s)
            continue
        if tot <= 0:
            share = {}
            for _, k, _o in resp[a:b]:
                share[k] = share.get(k, 0.0) + 1
            tot = sum(share.values())
        for k, v in share.items():
            res[k] = res.get(k, 0.0) + (e - s) * v / tot
    return res


def new_bucket():
    return {"sessions": set(), "providers": set(), "responses": 0, **{c: 0 for c in TOKEN_COLS}, "in_turn_s": 0.0,
            "timed_output": 0}  # output of responses in sessions with turn timing (Codex): out_per_turn_h numerator


def add(b, d):
    b["responses"] += int(d.get("n", 1) or 0)
    for k, col in _FIELDS:
        b[col] += int(d.get(k) or 0)
