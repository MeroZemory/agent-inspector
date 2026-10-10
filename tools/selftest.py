"""Adapter sanity + incremental-resume equivalence on a small deterministic subset (no index needed).

For each adapter: parse a few real files and print one compact line of counts (never transcript text).
Resume check: copy a file to a temp dir, cut the copy at ~50% on a line boundary, parse it, then
restore the full copy and parse from the stored byte offset with the saved state; the concatenated
rows must equal a single full parse. Sources are only read (copies are made with shutil.copyfile).
Also runs synthetic classification cases.

Usage: python tools/selftest.py [--per-adapter 4]
"""
import argparse
import collections
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sessionindex.adapters import ADAPTERS  # noqa: E402
from sessionindex.adapters.agy import _user_kind as agy_kind  # noqa: E402
from sessionindex.classify import claude_user_text_kind, codex_user_text_kind  # noqa: E402
from sessionindex.config import expand_roots, load_config  # noqa: E402
from sessionindex import timing  # noqa: E402


def job_for(root, c, path=None, start=0, state=None):
    return {"path": path or c.path, "start": start, "state": state, "subkind": c.subkind, "ctx": c.ctx,
            "pkey": c.pkey, "caps": load_config()["caps"]}


def summarize(res):
    kinds = collections.Counter(m[3] for m in res["messages"])
    evs = collections.Counter(e[2] for e in res["events"])
    return (f"msgs={len(res['messages'])} kinds={dict(kinds.most_common(6))} events={dict(evs.most_common(5))} "
            f"aliases={len(res['aliases'])} links={len(res['links'])} bad={res['n_bad']} partial={res['partial_tail']}")


def rowkey(m):
    # seq, ts, role, kind, text, text_len, truncated, tool_name, tool_id, is_error, model, origin, turn, offset
    return tuple(m[:14])


def resume_check(ad, root, c, tmp):
    if c.path.endswith(".db"):  # agy SQLite conversation: always fully re-parsed, no byte-offset resume
        return "resume=n/a(sqlite)"
    size = os.path.getsize(c.path)
    dst = os.path.join(tmp, os.path.basename(c.path))
    shutil.copyfile(c.path, dst)
    full = ad.parse(job_for(root, c, dst))
    with open(dst, "rb") as fh:
        data = fh.read()
    cut = data.rfind(b"\n", 0, size // 2) + 1
    if cut <= 0:
        return "skip(no cut)"
    with open(dst, "wb") as fh:
        fh.write(data[:cut])
    a = ad.parse(job_for(root, c, dst))
    with open(dst, "wb") as fh:
        fh.write(data)
    b = ad.parse(job_for(root, c, dst, start=a["end_offset"], state=a["state"]))
    os.remove(dst)
    ra = [rowkey(m) for m in a["messages"] + b["messages"]]
    rf = [rowkey(m) for m in full["messages"]]
    ea = [tuple(e[1:4]) for e in a["events"] + b["events"]]
    ef = [tuple(e[1:4]) for e in full["events"]]
    ok = ra == rf and ea == ef and b["end_offset"] == full["end_offset"]
    if ok:
        return f"resume=PASS ({len(rf)} msgs, {len(ef)} events, cut@{cut})"
    diff = next((i for i, (x, y) in enumerate(zip(ra, rf)) if x != y), min(len(ra), len(rf)))
    return (f"resume=FAIL msgs {len(ra)} vs {len(rf)}, events {len(ea)} vs {len(ef)}, first diff at row {diff}"
            f" kinds {ra[diff][3] if diff < len(ra) else None}/{rf[diff][3] if diff < len(rf) else None}")


def pick(cands, n, lo=20_000, hi=6_000_000):
    sized = [(os.path.getsize(c.path), c) for c in cands]
    sized = sorted([x for x in sized if lo <= x[0] <= hi], key=lambda x: x[1].path)
    if not sized:
        return []
    step = max(1, len(sized) // n)
    return [c for _, c in sized[::step][:n]]


def synthetic():
    cases = [
        (claude_user_text_kind("fix the bug", is_meta=False, is_compact=False, okind="human", psrc="typed",
                               is_sub=False, sdk_client=False), "human"),
        (claude_user_text_kind("Another Claude session sent a message: x", is_meta=True, is_compact=False,
                               okind="peer", psrc="system", is_sub=False, sdk_client=False), "peer_message"),
        (claude_user_text_kind("The coordinator sent a message", is_meta=True, is_compact=False,
                               okind="coordinator", psrc=None, is_sub=True, sdk_client=False), "peer_message"),
        (claude_user_text_kind("[Cross-session idle notice] x", is_meta=False, is_compact=False, okind=None,
                               psrc="system", is_sub=False, sdk_client=False), "meta"),
        (claude_user_text_kind("<command-name>/clear</command-name>", is_meta=False, is_compact=False, okind=None,
                               psrc=None, is_sub=False, sdk_client=False), "human_command"),
        (claude_user_text_kind("[Request interrupted by user]", is_meta=False, is_compact=False, okind=None,
                               psrc=None, is_sub=False, sdk_client=False), "interrupt_marker"),
        (claude_user_text_kind("<task-notification> x", is_meta=False, is_compact=False, okind="task-notification",
                               psrc="system", is_sub=False, sdk_client=False), "task_notification"),
        (claude_user_text_kind("do it", is_meta=False, is_compact=False, okind=None, psrc="sdk", is_sub=False,
                               sdk_client=True), "sdk_prompt"),
        (claude_user_text_kind("Stop hook feedback: [x]", is_meta=True, is_compact=False, okind=None,
                               psrc=None, is_sub=False, sdk_client=False), "hook_output"),
        (claude_user_text_kind("<task-notification> queued", is_meta=False, is_compact=False, okind=None,
                               psrc=None, is_sub=False, sdk_client=False), "task_notification"),
        (codex_user_text_kind("<environment_context>x", is_sub=False, noninteractive=False), "context_injection"),
        (codex_user_text_kind("# AGENTS.md instructions for C:/x", is_sub=False, noninteractive=False),
         "context_injection"),
        (codex_user_text_kind("<turn_aborted>x", is_sub=False, noninteractive=False), "interrupt_marker"),
        (codex_user_text_kind("다시 해줘", is_sub=False, noninteractive=False), "human"),
        (codex_user_text_kind("task text", is_sub=True, noninteractive=False), "agent_prompt"),
        (codex_user_text_kind("task text", is_sub=False, noninteractive=True), "sdk_prompt"),
        (agy_kind("어쩌라고 개발 완료했다고?", False, False), "human"),
        (agy_kind("/goal GOAL.md 를 읽고", False, False), "human_command"),
        (agy_kind("You are challenger_1", True, False), "agent_prompt"),
        (agy_kind("resume", True, False, ["resume"]), "human"),
        (agy_kind("prompt.md 파일의 지시를 그대로 수행하라.", False, True), "sdk_prompt"),
    ]
    bad = [(got, want) for got, want in cases if got != want]
    return f"synthetic classification: {len(cases) - len(bad)}/{len(cases)} ok" + (f" FAIL {bad}" if bad else "")


def bad_lines(tmp):
    """Malformed middle line is counted and skipped; a truncated last line is not consumed."""
    import json
    good = {"type": "user", "timestamp": "2026-01-01T00:00:00.000Z", "sessionId": "s", "cwd": "C:/x",
            "origin": {"kind": "human"}, "message": {"role": "user", "content": "hello there"}}
    p = os.path.join(tmp, "bad.jsonl")
    with open(p, "wb") as fh:
        fh.write(json.dumps(good).encode() + b"\n" + b"{not json\n" + json.dumps(good).encode() + b"\n"
                 + json.dumps(good).encode()[:40])
    c = type("C", (), {"subkind": "main", "ctx": {}, "pkey": "s", "path": p})()
    res = ADAPTERS["claude"].parse(job_for(None, c, p))
    ok = (len(res["messages"]) == 2 and res["n_bad"] == 1 and res["partial_tail"] == 1
          and res["end_offset"] < os.path.getsize(p))
    return f"bad-line tolerance: {'PASS' if ok else 'FAIL'} (msgs={len(res['messages'])} bad={res['n_bad']} " \
           f"partial={res['partial_tail']})"


def timing_rollout(tmp):
    """Synthetic Codex rollout -> timing events -> breakdown: parallel tool overlap, compaction, threshold outage,
    error-marked step, error turn without output, idle time between turns, window clipping, append resume."""
    import json
    t0 = 1_800_000_000

    def ts(s):
        return timing.ms_to_iso((t0 + s) * 1000)

    def rec(s, typ, pl):
        return json.dumps({"timestamp": ts(s), "type": typ, "payload": pl}, separators=(",", ":"))

    def call(s, cid, name, args):
        return rec(s, "response_item", {"type": "function_call", "name": name, "arguments": json.dumps(args),
                                        "call_id": cid})

    def output(s, cid):
        return rec(s, "response_item", {"type": "function_call_output", "call_id": cid, "output": "ok"})

    def ev(s, typ, **kw):
        return rec(s, "event_msg", dict({"type": typ}, **kw))

    def asst(s):
        return rec(s, "response_item", {"type": "message", "role": "assistant",
                                        "content": [{"type": "output_text", "text": f"done {s}"}]})
    tid = "01a00000-0000-7000-8000-000000000001"
    lines = [
        rec(0, "session_meta", {"id": tid, "cwd": "C:/x", "originator": "codex_cli_rs", "source": "cli"}),
        ev(0, "task_started", turn_id="t1"),
        call(10, "a", "exec_command", {"cmd": "npm test"}),      # build 10-60
        call(12, "b", "exec_command", {"cmd": "ls"}),            # shell 12-40 (overlaps a)
        output(40, "b"), output(60, "a"),
        asst(70),
        ev(80, "item_completed", item={"type": "ContextCompaction", "id": "c"},
           started_at_ms=(t0 + 72) * 1000, completed_at_ms=(t0 + 80) * 1000),
        ev(90, "task_complete", turn_id="t1", duration_ms=90000),
        # idle 90..1000 (waiting for the user) is not in-turn
        ev(1000, "task_started", turn_id="t2"),
        call(1010, "w", "wait", {"timeout_ms": 20000}), output(1030, "w"),
        asst(2530),                                              # 1500 s model step > 20 min -> outage
        ev(2540, "task_complete", turn_id="t2"),
        ev(3000, "task_started", turn_id="t3"),
        ev(3300, "task_complete", turn_id="t3", error={"message": "429"}),   # no model output -> outage in full
        ev(4000, "task_started", turn_id="t4"),
        ev(4005, "stream_error", message="Reconnecting... 1/5"),
        asst(4100),                                              # step with a stream_error -> outage
        ev(4110, "task_complete", turn_id="t4"),
    ]
    d = os.path.join(tmp, "timing")
    os.makedirs(d, exist_ok=True)
    p = os.path.join(d, f"rollout-2026-01-01T00-00-00-{tid}.jsonl")
    with open(p, "wb") as fh:
        fh.write(("\n".join(lines) + "\n").encode("utf-8"))
    c = type("C", (), {"subkind": "main", "ctx": {}, "pkey": tid, "path": p})()
    ad = ADAPTERS["codex"]
    res = ad.parse(job_for(None, c, p))
    rows = [(e[1], e[2], e[3], e[4]) for e in sorted(res["events"], key=lambda e: (e[1], e[0]))
            if e[2] in timing.TIMING_EVENTS]
    last = ts(4110)
    full = timing.session_breakdown(rows, last, float("-inf"), float("inf"), 1200)
    win = timing.session_breakdown(rows, last, t0 + 2000, float("inf"), 1200)
    want = {"in_turn": 2040, "tool": 70, "compaction": 8, "outage": 1900, "model": 62, "turns": 4,
            "outage_turns": 1}
    got = {k: round(full[k], 3) for k in want}
    kinds = {k: round(v, 3) for k, v in full["kinds"].items() if v}
    ok = (got == want and kinds == {"build": 50, "shell": 28, "wait": 20} and sorted(full["steps"]) == [10, 10, 10]
          and round(full["error_idle"], 3) == 700 and round(win["error_idle"], 3) == 700
          and round(timing.session_breakdown(rows, last, t0 + 3500, float("inf"), 1200)["error_idle"], 3) == 500
          and round(win["in_turn"], 3) == 540 + 300 + 110 and len(timing.windows(full["outage_iv"], 1200)) == 1)
    rc = resume_check(ad, None, c, tmp)
    ok = ok and "PASS" in rc
    return (f"timing breakdown (synthetic rollout): {'PASS' if ok else 'FAIL'} got={got} kinds={kinds} "
            f"steps={sorted(full['steps'])} error_idle={full['error_idle']} window_in_turn={round(win['in_turn'], 1)} {rc}")


def usage_cases(tmp):
    """Synthetic Codex and Claude transcripts -> usage events: settings tracking (model/effort/tier), fork-copied
    records skipped, token_count fallback with repeated totals, Claude split records counted once, streaming
    output deltas across an append resume, synthetic/imported records skipped; plus in-turn attribution."""
    import json
    from sessionindex import usage
    t0 = 1_800_000_000

    def ts(s):
        return timing.ms_to_iso((t0 + s) * 1000)

    def rec(s, typ, pl):
        return json.dumps({"timestamp": ts(s), "type": typ, "payload": pl}, separators=(",", ":"))

    def u(i, c, o, r):
        return {"input_tokens": i, "cached_input_tokens": c, "cache_write_input_tokens": 0, "output_tokens": o,
                "reasoning_output_tokens": r, "total_tokens": i + o}

    def tur(s, thread, rid, *a):
        return rec(s, "token_usage_record", {"thread_id": thread, "turn_id": "t1", "response_id": rid, "usage": u(*a)})

    def tc(s, tot, last):
        return rec(s, "event_msg", {"type": "token_count", "info": {"total_token_usage": {"total_tokens": tot},
                                                                    "last_token_usage": last}})

    def tsa(s, eff, tier):
        return rec(s, "event_msg", {"type": "thread_settings_applied", "thread_settings": {
            "model": "gpt-x", "reasoning_effort": eff, "service_tier": tier}})

    def rows(res):
        return [(e[1], json.loads(e[3])) for e in res["events"] if e[2] == "usage"]

    d_ = os.path.join(tmp, "usage")
    os.makedirs(d_, exist_ok=True)

    def parse(adapter, name, lines, subkind="main", start=0, state=None):
        p = os.path.join(d_, name)
        with open(p, "wb") as fh:
            fh.write(("\n".join(lines) + "\n").encode("utf-8"))
        c = type("C", (), {"subkind": subkind, "ctx": {}, "pkey": "x", "path": p})()
        return ADAPTERS[adapter].parse(job_for(None, c, p, start=start, state=state)), c
    tid = "01a00000-0000-7000-8000-000000000002"
    meta = rec(0, "session_meta", {"id": tid, "cwd": "C:/x", "originator": "codex_cli_rs", "source": "cli"})
    lines = [meta, tsa(1, "high", "default"), rec(2, "event_msg", {"type": "task_started", "turn_id": "t1"}),
             rec(3, "turn_context", {"turn_id": "t1", "model": "gpt-x", "effort": "xhigh"}),
             tur(10, tid, "r1", 1000, 800, 50, 20), tc(11, 1050, u(1000, 800, 50, 20)),
             tur(20, "01a00000-0000-7000-8000-00000000ffff", "r0", 9999, 0, 9999, 0),   # fork copy: skipped
             tsa(25, "xhigh", "priority"),
             tur(30, tid, "r2", 2000, 1500, 100, 0), tc(31, 3150, u(2000, 1500, 100, 0))]
    res, c = parse("codex", f"rollout-2026-01-01T00-00-00-{tid}.jsonl", lines)
    cx = [(d["m"], d["e"], d["t"], d["i"], d["c"], d["o"], d["r"], d["id"]) for _, d in rows(res)]
    ok_cx = (cx == [("gpt-x", "xhigh", "default", 1000, 800, 50, 20, "r1"),
                    ("gpt-x", "xhigh", "priority", 2000, 1500, 100, 0, "r2")]
             and res["rec_counts"].get("token_usage_record:foreign_thread") == 1)
    rc_cx = resume_check(ADAPTERS["codex"], None, c, tmp)
    # rollout without token_usage_record: token_count fallback, a repeated total is not a new response
    old = [meta, tc(5, 1050, u(1000, 800, 50, 20)), tc(6, 1050, u(1000, 800, 50, 20)), tc(9, 3150, u(2000, 0, 100, 0))]
    res2, _ = parse("codex", f"rollout-2026-01-01T00-00-01-{tid}.jsonl", old)
    fb = [(d["i"], d["o"], d.get("src")) for _, d in rows(res2)]
    ok_fb = fb == [(1000, 50, "token_count"), (2000, 100, "token_count")]

    def asst(s, mid, usage_, model="claude-x", version="2.1.0", effort="high"):
        return json.dumps({"type": "assistant", "timestamp": ts(s), "sessionId": "s", "cwd": "C:/x",
                           "version": version, "effort": effort,
                           "message": {"id": mid, "model": model, "role": "assistant", "usage": usage_,
                                       "content": [{"type": "text", "text": f"m {s}"}]}})

    def cu(i, cw, cr, o, think=None, speed=None):
        d = {"input_tokens": i, "cache_creation_input_tokens": cw, "cache_read_input_tokens": cr, "output_tokens": o,
             "service_tier": "standard"}
        if think is not None:
            d["output_tokens_details"] = {"thinking_tokens": think}
        if speed:
            d["speed"] = speed
        return d
    cl = [asst(1, "A", cu(2, 100, 900, 40, 10)), asst(2, "A", cu(2, 100, 900, 40, 10)),   # split records: once
          asst(3, "B", cu(5, 0, 1000, 5)), asst(4, "B", cu(5, 0, 1000, 50)),               # streaming: +45 out
          asst(5, "C", cu(1, 0, 0, 7, speed="fast"), effort="xhigh"),
          asst(6, "D", cu(9, 9, 9, 9), model="<synthetic>"), asst(7, "E", cu(9, 9, 9, 9), version="codex-import")]
    res3, _ = parse("claude", "claude-usage.jsonl", cl)
    got = [(d["n"], d["i"], d["c"], d["w"], d["o"], d["r"], d["t"], d["e"]) for _, d in rows(res3)]
    want = [(1, 1002, 900, 100, 40, 10, "standard", "high"), (1, 1005, 1000, 0, 5, 0, "standard", "high"),
            (0, 0, 0, 0, 45, 0, "standard", "high"), (1, 1, 0, 0, 7, 0, "fast", "xhigh")]
    # append resume between B's two records keeps the per-message state
    a, _ = parse("claude", "claude-usage-a.jsonl", cl[:3])
    p = os.path.join(d_, "claude-usage-a.jsonl")
    with open(p, "wb") as fh:
        fh.write(("\n".join(cl) + "\n").encode("utf-8"))
    cc = type("C", (), {"subkind": "main", "ctx": {}, "pkey": "x", "path": p})()
    b = ADAPTERS["claude"].parse(job_for(None, cc, p, start=a["end_offset"], state=a["state"]))
    ok_cl = got == want and [d for _, d in rows(a) + rows(b)] == [d for _, d in rows(res3)]
    # in-turn attribution: proportional to output within a turn, empty segment unattributed, UTC day split
    att = usage.in_turn_by_group([(0, 100), (200, 260)], 0, 1e9, [(10, "hi", 30), (50, "lo", 10)],
                                 lambda _d: None, "effort")
    days = usage.day_split([(86400 * 3 - 60, 86400 * 3 + 30)])
    ok_att = (att == {"hi": 75.0, "lo": 25.0, None: 60.0}
              and [(round(e - s), d) for s, e, d in days] == [(60, "1970-01-03"), (30, "1970-01-04")])
    ok = ok_cx and ok_fb and ok_cl and ok_att and "PASS" in rc_cx
    return (f"usage (synthetic codex+claude): {'PASS' if ok else 'FAIL'} codex={ok_cx} fallback={ok_fb} "
            f"claude={ok_cl} attribution={ok_att} codex {rc_cx}" + ("" if ok else f" cx={cx} fb={fb} cl={got} att={att}"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-adapter", type=int, default=4)
    a = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print(synthetic())
    t0 = tempfile.mkdtemp(prefix="sx-selftest-")
    try:
        print(bad_lines(t0))
        tr = timing_rollout(t0)
        print(tr)
        ur = usage_cases(t0)
        print(ur)
        tr += ur
    finally:
        shutil.rmtree(t0, ignore_errors=True)
    roots = expand_roots(load_config())
    tmp = tempfile.mkdtemp(prefix="sx-selftest-")
    fails = 0
    try:
        for adapter in ("claude", "codex", "cc_transcripts", "agy"):
            ad = ADAPTERS[adapter]
            cands = [(r, c) for r in roots if r.adapter == adapter and not r.is_test for c in ad.discover(r)]
            by_sub = collections.defaultdict(list)
            for r, c in cands:
                by_sub[c.subkind].append(c)
            rootmap = {c.path: r for r, c in cands}
            chosen = []
            for sub, cs in sorted(by_sub.items()):
                chosen += pick(cs, max(1, a.per_adapter // len(by_sub)) if len(by_sub) > 1 else a.per_adapter)
            for c in chosen:
                r = rootmap[c.path]
                res = ad.parse(job_for(r, c))
                rc = resume_check(ad, r, c, tmp)
                fails += "FAIL" in rc
                print(f"{adapter}/{c.subkind} {os.path.getsize(c.path) // 1024}KB {summarize(res)} {rc}")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    fails += "FAIL" in tr
    print("RESULT", "FAIL" if fails else "PASS", f"({fails} resume/timing failures)")


if __name__ == "__main__":
    main()
