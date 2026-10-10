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
