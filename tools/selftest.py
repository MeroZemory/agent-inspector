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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-adapter", type=int, default=4)
    a = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print(synthetic())
    t0 = tempfile.mkdtemp(prefix="sx-selftest-")
    try:
        print(bad_lines(t0))
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
    print("RESULT", "FAIL" if fails else "PASS", f"({fails} resume failures)")


if __name__ == "__main__":
    main()
