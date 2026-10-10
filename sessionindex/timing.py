"""In-turn wall-time breakdown (model / tool / compaction / outage) from timing events recorded at ingest.

Codex only. The adapter emits (source='transcript'):
  turn_start       ts = task_started                                detail = turn id
  turn_end         ts = task_complete | turn_aborted | next start   value = duration_ms
                   detail = 'complete|aborted:<reason>|superseded out=<model outputs> err=<0|1>'
  tool_span        ts = tool call; value = seconds until its output detail = '<kind> <call_id>'
  compaction_span  ts = ContextCompaction started_at_ms; value = seconds
  model_step       ts = model input (turn start or tool output); value = seconds until the next tool call or
                   assistant message (or the turn end: trailing step, only when no tool call of the turn is open)
                   detail = '<tool_call|assistant|turn_end|turn_end_error> err=<error/stream_error events inside>'

`breakdown` partitions the in-turn time of each session (turn intervals clipped to the window) with precedence
tool > compaction > outage > model, so the parts add up to the in-turn time:
  tool       = union of tool spans
  compaction = union of compaction spans not under a tool span
  outage     = (whole turns with no model output) + (model steps longer than the threshold, containing an
               error/stream_error event, or ending in a task_complete error), minus tool and compaction
  model      = the rest: in-turn time with no tool outstanding (generation, network, proxy)
Outside the in-turn total (not in any percentage): error_idle = gap from the end of a turn that ended with a
task_complete error or without model output to the next task_started; lost = outage + error_idle.
Tool kinds overlap each other (parallel calls), so per-kind shares can sum to more than the tool share.
"""
from __future__ import annotations

import re
import statistics

from .util import iso_to_epoch, ms_to_iso

TIMING_EVENTS = ("turn_start", "turn_end", "tool_span", "compaction_span", "model_step")
TOOL_KINDS = ("build", "shell", "wait", "subagent", "web", "edit", "other")
BUILD_RE = re.compile(r"\b(npm|npx|pnpm|yarn|vite|vitest|playwright|pytest|unittest|cargo|dotnet|msbuild|gradle|ffmpeg|"
                      r"blender|render|tsc|eslint|node\s+\S+\.m?js|python\S*\s+\S+\.py|py\s+-m)\b", re.I)


def tool_kind(name: str | None, args: str | None) -> str:
    """wait | subagent | web | build | shell | edit | other, from the raw call name and its arguments/input
    (Codex code-mode calls carry the inner tool names, e.g. multi_agent_v1__wait, inside the input)."""
    name = name or ""
    x = args or ""
    if name in ("wait", "sleep") or "wait_agent" in x or "multi_agent_v1__wait" in x:
        return "wait"
    if "multi_agent_v1__" in x:
        return "subagent"
    if "web__run" in x:
        return "web"
    if "exec_command" in x or "write_stdin" in x or name in ("shell", "exec_command", "local_shell"):
        return "build" if BUILD_RE.search(x) else "shell"
    if "apply_patch" in x or name == "apply_patch":
        return "edit"
    return "other"


# ---------------------------------------------------------------- interval helpers (lists of (start, end))

def merge(iv):
    out = []
    for s, e in sorted(iv):
        if e <= s:
            continue
        if out and s <= out[-1][1]:
            if e > out[-1][1]:
                out[-1][1] = e
        else:
            out.append([s, e])
    return [(s, e) for s, e in out]


def intersect(a, b):
    """a, b merged -> merged intersection."""
    out, i, j = [], 0, 0
    while i < len(a) and j < len(b):
        s, e = max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
        if s < e:
            out.append((s, e))
        if a[i][1] < b[j][1]:
            i += 1
        else:
            j += 1
    return out


def subtract(a, b):
    """a, b merged -> a minus b."""
    out, j = [], 0
    for s, e in a:
        cur = s
        while j < len(b) and b[j][1] <= cur:
            j += 1
        k = j
        while k < len(b) and b[k][0] < e:
            if b[k][0] > cur:
                out.append((cur, b[k][0]))
            cur = max(cur, b[k][1])
            k += 1
        if cur < e:
            out.append((cur, e))
    return out


def total(iv):
    return sum(e - s for s, e in iv)


def _kv(detail):
    parts = (detail or "").split()
    return (parts[0] if parts else ""), dict(p.split("=", 1) for p in parts[1:] if "=" in p)


# ---------------------------------------------------------------- per-session breakdown

def session_breakdown(rows, last_ts, w0, w1, outage_s):
    """rows: (ts, type, detail, value) ordered by (ts, seq) for one session. Times in epoch seconds.
    Returns a dict of seconds + raw step list + outage intervals, or None if the session has no turns."""
    turns = []
    cur = None
    spans = {k: [] for k in TOOL_KINDS}
    comp = []
    steps = []
    for ts, typ, detail, value in rows:
        t = iso_to_epoch(ts)
        if t is None:
            continue
        if typ == "turn_start":
            if cur is not None:
                turns.append((cur, t, 1, False))
            cur = t
        elif typ == "turn_end":
            if cur is not None:
                _, kv = _kv(detail)
                turns.append((cur, t, int(kv.get("out", 1)), kv.get("err") == "1"))
                cur = None
        elif typ == "tool_span" and value is not None:
            k = _kv(detail)[0]
            spans[k if k in spans else "other"].append((t, t + float(value)))
        elif typ == "compaction_span" and value is not None:
            comp.append((t, t + float(value)))
        elif typ == "model_step" and value is not None:
            end, kv = _kv(detail)
            steps.append((t, t + float(value), end, int(kv.get("err", 0))))
    if cur is not None:  # still open at the last record (live session or crash): end it at the last record
        lt = iso_to_epoch(last_ts)
        if lt is not None and lt > cur:
            turns.append((cur, lt, 1, False))
    if not turns:
        return None
    win = [(w0, w1)]
    in_turn = intersect(merge([(s, e) for s, e, _, _ in turns]), win)
    tool_by = {k: intersect(merge(v), in_turn) for k, v in spans.items()}
    tool = intersect(merge([x for v in spans.values() for x in v]), in_turn)
    compaction = subtract(intersect(merge(comp), in_turn), tool)
    dead_turns = [(s, e) for s, e, out, _ in turns if out == 0]
    dead = merge(dead_turns)
    out_iv = list(dead_turns)
    good_steps = []
    for s, e, end, err in steps:
        if intersect([(s, e)], dead):
            continue  # part of a turn without model output (already outage in full)
        if e - s > outage_s or err > 0 or end == "turn_end_error":
            out_iv.append((s, e))
        elif w0 <= s < w1:
            good_steps.append(e - s)
    outage = subtract(subtract(intersect(merge(out_iv), in_turn), tool), compaction)
    # error idle: after a turn that ended with a task_complete error or without model output, the gap to the next
    # task_started (goal sessions keep retrying the stream there without writing to the rollout); not in-turn
    err_gap = []
    for i, (s, e, out, err) in enumerate(turns[:-1]):
        if err or out == 0:
            err_gap.append((e, turns[i + 1][0]))
    error_idle = total(intersect(merge(err_gap), win))
    n_turns = sum(1 for s, e, _, _ in turns if s < w1 and e > w0)
    n_dead = sum(1 for s, e in dead_turns if s < w1 and e > w0)
    T = total(in_turn)
    return {"in_turn": T, "tool": total(tool), "compaction": total(compaction), "outage": total(outage),
            "model": max(0.0, T - total(tool) - total(compaction) - total(outage)),
            "kinds": {k: total(v) for k, v in tool_by.items()}, "steps": good_steps, "turns": n_turns,
            "outage_turns": n_dead, "outage_iv": outage, "error_idle": error_idle}


def windows(iv, gap_s):
    """Outage intervals merged across gaps shorter than gap_s (for cross-session comparison)."""
    out = []
    for s, e in sorted(iv):
        if out and s - out[-1][1] < gap_s:
            out[-1][1] = max(out[-1][1], e)
            out[-1][2] += e - s
        else:
            out.append([s, e, e - s])
    return [{"first": ms_to_iso(s * 1000), "last": ms_to_iso(e * 1000), "outage_h": round(d / 3600, 2)}
            for s, e, d in out]


def summarize(parts: list[dict]) -> dict:
    """Sum session breakdowns (a project = sum of its sessions) into report fields."""
    agg = {"in_turn": 0.0, "tool": 0.0, "compaction": 0.0, "outage": 0.0, "model": 0.0, "turns": 0,
           "outage_turns": 0, "error_idle": 0.0}
    kinds = {k: 0.0 for k in TOOL_KINDS}
    steps = []
    for p in parts:
        for k in agg:
            agg[k] += p[k]
        for k in kinds:
            kinds[k] += p["kinds"][k]
        steps += p["steps"]
    T = agg["in_turn"] or 1e-9

    def pct(x):
        return round(100 * x / T, 1)
    st = sorted(steps)
    d = {"in_turn_h": round(agg["in_turn"] / 3600, 2), "model_pct": pct(agg["model"]), "tool_pct": pct(agg["tool"])}
    for k in TOOL_KINDS:
        d[f"{k}_pct"] = pct(kinds[k])
    d.update({"compaction_pct": pct(agg["compaction"]), "outage_h": round(agg["outage"] / 3600, 2),
              "outage_pct": pct(agg["outage"]), "error_idle_h": round(agg["error_idle"] / 3600, 2),
              "lost_h": round((agg["outage"] + agg["error_idle"]) / 3600, 2), "model_steps": len(st),
              "step_median_s": round(statistics.median(st), 1) if st else None,
              "step_p90_s": round(st[int(len(st) * 0.9)], 1) if st else None,
              "turns": agg["turns"], "outage_turns": agg["outage_turns"]})
    return d
