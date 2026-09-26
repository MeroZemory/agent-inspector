"""Cross-check human-vs-injected classification against typed-prompt logs (prompt_history).

prompt_history comes from ~/.claude/history.jsonl and CODEX_HOME/history.jsonl: what the CLI/TUI
recorded as typed input. Desktop-app prompts and queued prompts may be missing there, so
'precision' below is a lower bound.

Prints aggregate numbers and a few short examples only.
Usage: python tools/validate_human.py [--examples N]
"""
import argparse
import collections
import os
import re
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sessionindex.config import DB_PATH  # noqa: E402

HUMAN = ("human", "human_command", "human_shell")
PASTE = re.compile(r"\[Pasted text #\d+[^\]]*\]|\[Image #\d+\]")


def norm(s, n=40):
    s = PASTE.split(s or "")[0]
    s = " ".join(s.split())
    return s[:n].lower()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--examples", type=int, default=6)
    a = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    con = sqlite3.connect("file:" + DB_PATH.replace("\\", "/") + "?mode=ro", uri=True)
    hist = collections.defaultdict(list)
    for sid, prov, text in con.execute("SELECT p.sid, p.provider, p.text FROM prompt_history p "
                                       "JOIN sessions s ON s.sid=p.sid"):
        k = norm(text)
        if len(k) >= 4:
            hist[sid].append((prov, k, text))
    res = {}
    for prov in ("claude", "codex"):
        sids = [s for s, v in hist.items() if v and v[0][0] == prov]
        tot = matched = 0
        wrong = collections.Counter()
        miss_ex, fp_ex = [], []
        hm_tot = hm_match = 0
        for sid in sids:
            msgs = con.execute("SELECT kind, text FROM messages WHERE sid=? AND inherited=0 AND role IN ('user','system')",
                               (sid,)).fetchall()
            by_kind = [(k, norm(t)) for k, t in msgs]
            hkeys = {nk for k, nk in by_kind if k in HUMAN}
            allkeys = collections.defaultdict(set)
            for k, nk in by_kind:
                allkeys[nk].add(k)
            hset = {h[1] for h in hist[sid]}
            for _, k, text in hist[sid]:
                tot += 1
                if k in hkeys or any(x.startswith(k) or k.startswith(x) for x in hkeys if len(x) >= 8):
                    matched += 1
                else:
                    kinds = allkeys.get(k)
                    wrong["|".join(sorted(kinds)) if kinds else "not_found"] += 1
                    if len(miss_ex) < a.examples and kinds:
                        miss_ex.append((sid, sorted(kinds), text[:80]))
            for k, nk in by_kind:
                if k in HUMAN and len(nk) >= 4:
                    hm_tot += 1
                    if nk in hset or any(x.startswith(nk) or nk.startswith(x) for x in hset if len(x) >= 8):
                        hm_match += 1
                    elif len(fp_ex) < a.examples:
                        fp_ex.append((sid, nk[:80]))
        res[prov] = {"sessions_with_history": len(sids), "history_prompts": tot, "recall_as_human": round(matched / tot, 4) if tot else None,
                     "history_prompts_not_human_by_kind": dict(wrong.most_common(8)),
                     "human_msgs": hm_tot, "human_msgs_found_in_history": round(hm_match / hm_tot, 4) if hm_tot else None}
        print(prov, res[prov])
        for ex in miss_ex:
            print("  missed:", ex)
        for ex in fp_ex:
            print("  human-not-in-history:", ex)


if __name__ == "__main__":
    main()
