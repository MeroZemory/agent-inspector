"""Spot-check indexed sessions against their raw source files with an independent, simpler reader.

Selects (unless --sid given) Claude and Codex sessions covering: subagents, compactions, interrupts,
plain interactive sessions. For each, recomputes counts directly from the canonical raw file and
prints one compact line: raw vs db, plus MISMATCH markers. Never prints transcript content except
<=60-char excerpts for mismatched human messages.

Usage: python tools/spotcheck.py [--n 5] [--sid SID ...]
"""
import argparse
import collections
import json
import re
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent_inspector.config import DB_PATH  # noqa: E402

HUMAN = ("human", "human_command", "human_shell")
IMG = re.compile(r"<image\b[^>]*>|</image>|\[image\]")  # image markup differs between Codex representations


def db():
    return sqlite3.connect("file:" + DB_PATH.replace("\\", "/") + "?mode=ro", uri=True)


def pick(con, n):
    q = {
        "claude_interactive": "SELECT sid FROM sessions WHERE provider='claude' AND origin_class='interactive' AND human_msgs BETWEEN 3 AND 60 ORDER BY random() LIMIT 1",
        "claude_subagent": "SELECT sid FROM sessions WHERE provider='claude' AND is_subagent=1 AND msgs_n>10 ORDER BY random() LIMIT 1",
        "claude_compaction": "SELECT sid FROM sessions WHERE provider='claude' AND compactions>0 AND is_subagent=0 ORDER BY random() LIMIT 1",
        "claude_interrupt": "SELECT sid FROM sessions WHERE provider='claude' AND interrupts>0 AND is_subagent=0 ORDER BY random() LIMIT 1",
        "claude_parent_with_kids": "SELECT parent_sid FROM sessions WHERE provider='claude' AND is_subagent=1 AND parent_sid IN (SELECT sid FROM sessions) ORDER BY random() LIMIT 1",
        "codex_interactive": "SELECT sid FROM sessions WHERE provider='codex' AND origin_class='interactive' AND human_msgs BETWEEN 3 AND 60 ORDER BY random() LIMIT 1",
        "codex_subagent": "SELECT sid FROM sessions WHERE provider='codex' AND is_subagent=1 AND msgs_n>10 ORDER BY random() LIMIT 1",
        "codex_compaction": "SELECT sid FROM sessions WHERE provider='codex' AND compactions>0 AND is_subagent=0 ORDER BY random() LIMIT 1",
        "codex_interrupt": "SELECT sid FROM sessions WHERE provider='codex' AND interrupts>0 AND is_subagent=0 ORDER BY random() LIMIT 1",
        "codex_exec": "SELECT sid FROM sessions WHERE provider='codex' AND origin_class='noninteractive' AND msgs_n>5 ORDER BY random() LIMIT 1",
    }
    out = []
    for label, sql in q.items():
        for _ in range(max(1, n // 5)):
            r = con.execute(sql).fetchone()
            if r and r[0] not in [x[1] for x in out]:
                out.append((label, r[0]))
    return out


def raw_claude(path, is_sub):
    c = collections.Counter()
    human = []
    titles = collections.defaultdict(set)
    first = last = None
    for raw in open(path, "rb"):
        try:
            r = json.loads(raw)
        except ValueError:
            c["bad"] += 1
            continue
        t = r.get("type")
        ts = r.get("timestamp")
        if ts:
            first = first or ts
            last = ts
        if t == "ai-title":
            titles["claude_ai_title"].add(r.get("aiTitle"))
        if t == "custom-title":
            titles["claude_custom_title"].add(r.get("customTitle"))
        if t == "system" and r.get("subtype") == "compact_boundary":
            c["compact"] += 1
        if t == "assistant":
            for b in (r.get("message") or {}).get("content") or []:
                if b.get("type") == "tool_use":
                    c["tool_use"] += 1
                elif b.get("type") == "text":
                    c["asst_text"] += 1
        if t == "attachment" and (r.get("attachment") or {}).get("type") == "queued_command":
            a = r["attachment"]
            if (a.get("origin") or {}).get("kind") in (None, "human") and a.get("prompt") and not is_sub \
                    and not str(a.get("prompt")).lstrip().startswith("<"):  # queued notifications are not typed
                human.append(("queued", str(a.get("prompt"))))
        if t != "user":
            continue
        content = (r.get("message") or {}).get("content")
        blocks = [{"type": "text", "text": content}] if isinstance(content, str) else (content or [])
        okr = (r.get("origin") or {}).get("kind")
        if (not is_sub and not r.get("isSidechain") and not r.get("isMeta") and okr in (None, "human")
                and r.get("promptSource") not in ("sdk", "system")
                and any(isinstance(b, dict) and b.get("type") == "image" for b in blocks)
                and not any(isinstance(b, dict) and b.get("type") in ("text", "tool_result") for b in blocks)):
            human.append(("image", "[image]"))
        n0 = len(human)  # consecutive human text blocks of one record form one message (as in the index)
        for b in blocks:
            if b.get("type") == "tool_result":
                c["tool_result"] += 1
            if b.get("type") != "text":
                continue
            s = (b.get("text") or "").lstrip()
            if s.startswith("[Request interrupted by user"):
                c["interrupt_marker"] += 1
                continue
            if is_sub or r.get("isSidechain") or r.get("isMeta") or r.get("isCompactSummary"):
                continue
            ok = (r.get("origin") or {}).get("kind")
            if ok and ok != "human":
                continue
            if not ok and (r.get("promptSource") in ("sdk", "system") or (r.get("entrypoint") or "").startswith("sdk")):
                continue
            if s.startswith("<command-name>") or s.startswith("<command-message>"):
                human.append(("cmd", s))
            elif s.startswith("<bash-input>"):
                human.append(("bash", s))
            elif s.startswith("<") or s.startswith("Caveat:") or s.startswith("This session is being continued"):
                continue
            elif len(human) > n0 and human[-1][0] == "text":
                human[-1] = ("text", human[-1][1] + "\n" + s)
            else:
                human.append(("text", s))
    return c, human, titles, first, last


def raw_codex(path):
    c = collections.Counter()
    ev_user = []
    first = last = None
    meta = None
    for raw in open(path, "rb"):
        try:
            r = json.loads(raw)
        except ValueError:
            c["bad"] += 1
            continue
        ts = r.get("timestamp")
        if ts:
            first = first or ts
            last = ts
        pl = r.get("payload") or {}
        t = r.get("type")
        if t == "session_meta" and meta is None:
            meta = pl
        if t == "compacted":
            c["compacted"] += 1
        if t == "response_item":
            it = pl.get("type")
            if it in ("function_call", "custom_tool_call", "local_shell_call", "web_search_call"):
                c["tool_call"] += 1
            if it == "message" and pl.get("role") == "assistant":
                c["ri_assistant"] += 1
            if it == "message" and pl.get("role") == "user":
                c["ri_user"] += 1
        if t == "event_msg":
            et = pl.get("type")
            if et == "turn_aborted" and pl.get("reason") == "interrupted":
                c["interrupted"] += 1
            if et == "user_message" and (pl.get("message") or "").strip():
                ev_user.append(("event", pl.get("message") or ""))
            if et == "item_completed" and (pl.get("item") or {}).get("type") == "UserMessage":
                it = pl["item"]
                txt = "\n".join(x.get("text", "") for x in it.get("content") or [] if isinstance(x, dict))
                if txt.strip():
                    ev_user.append(("item", txt))
    # multiset of typed prompts: per text, max(count in event form, count in item form)
    per = collections.defaultdict(collections.Counter)
    for src, u in ev_user:
        per[" ".join(IMG.sub(" ", u).split())[:200]][src] += 1
    typed = collections.Counter({k: max(v.values()) for k, v in per.items()})
    return c, typed, meta or {}, first, last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--sid", nargs="*")
    a = ap.parse_args()
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    con = db()
    targets = [("given", s) for s in a.sid] if a.sid else pick(con, a.n)
    for label, sid in targets:
        s = con.execute("SELECT provider, is_subagent, origin_class, human_msgs, tool_calls, interrupts, compactions, "
                        "started_at, last_activity_at, canonical_file_id, title, title_origin FROM sessions WHERE sid=?",
                        (sid,)).fetchone()
        if not s:
            print(label, sid, "missing")
            continue
        prov, is_sub, oc, hm, tc, intr, comp, st, en, fid, title, torig = s
        path = con.execute("SELECT path FROM files WHERE file_id=?", (fid,)).fetchone()[0]
        kinds = dict(con.execute("SELECT kind, COUNT(*) FROM messages WHERE sid=? AND inherited=0 GROUP BY kind", (sid,)))
        dbhum = [t for (t,) in con.execute(f"SELECT text FROM messages WHERE sid=? AND inherited=0 AND imported=0 AND "
                                           f"sidechain=0 AND kind IN {HUMAN} ORDER BY seq", (sid,))]
        line = {"label": label, "sid": sid, "oc": oc}
        mism = []
        if prov == "claude":
            c, human, titles, first, last = raw_claude(path, bool(is_sub))
            # queued copies of delivered prompts are dropped by the indexer; mirror that
            texts = [h[1] for h in human if h[0] != "queued"]
            q = [h for h in human if h[0] == "queued" and " ".join(h[1].split()) not in {" ".join(x.split()) for x in texts}]
            exp_h = len(texts) + len(q)
            line.update({"raw_human": exp_h, "db_human": hm, "raw_tool_use": c["tool_use"], "db_tool_calls": tc,
                         "raw_intr_markers": c["interrupt_marker"], "db_interrupts": intr,
                         "raw_compact": c["compact"], "db_compactions": comp,
                         "raw_tool_results": c["tool_result"], "db_tool_outputs": kinds.get("tool_output", 0)})
            if exp_h != hm:
                mism.append("human")
            if c["tool_use"] != tc:
                mism.append("tool_calls")
            if c["compact"] != comp:
                mism.append("compaction")
            if c["tool_result"] != kinds.get("tool_output", 0) - 0:
                mism.append("tool_outputs")
            if c["interrupt_marker"] > intr:
                mism.append("interrupts")
            dbt = set(r[0] for r in con.execute("SELECT alias FROM aliases WHERE sid=? AND origin IN "
                                               "('claude_ai_title','claude_custom_title')", (sid,)))
            rawt = {x.strip()[:500] for v in titles.values() for x in v if x}
            if rawt != dbt:
                mism.append("titles")
            line["titles_raw/db"] = f"{len(rawt)}/{len(dbt)}"
        else:
            c, typed, meta, first, last = raw_codex(path)
            dbuser = collections.defaultdict(collections.Counter)  # normalized text -> kind counts (incl. inherited)
            for k, t in con.execute("SELECT kind, text FROM messages WHERE sid=? AND role='user'", (sid,)):
                dbuser[" ".join(IMG.sub(" ", t or "").split())[:200]][k] += 1
            n_typed = sum(typed.values())
            found = sum(min(n, sum(dbuser.get(k, {}).values())) for k, n in typed.items())
            typed_kinds = collections.Counter()
            for k, n in typed.items():
                for kk, m in dbuser.get(k, {}).items():
                    typed_kinds[kk] += min(n, m)
            dbh = collections.Counter()
            for k, kc in dbuser.items():
                for kk, m in kc.items():
                    if kk in HUMAN:
                        dbh[k] += m
            h_in_typed = sum(min(m, typed.get(k, 0)) for k, m in dbh.items())
            inh = dict(con.execute("SELECT kind, COUNT(*) FROM messages WHERE sid=? AND inherited=1 GROUP BY kind",
                                   (sid,)))
            line.update({"raw_typed": n_typed, "typed_found_in_db": found,
                         "typed_as_kinds": dict(typed_kinds.most_common(4)),
                         "db_human_all": sum(dbh.values()), "db_human_in_typed": h_in_typed, "db_human_counted": hm,
                         "raw_tool_call": c["tool_call"], "db_tool_calls": tc, "db_inherited_tool_calls": inh.get("tool_call", 0),
                         "raw_interrupted": c["interrupted"], "db_interrupts": intr, "raw_compacted": c["compacted"],
                         "db_compactions": comp, "raw_ri_asst": c["ri_assistant"], "db_asst": kinds.get("assistant", 0)})
            if found != n_typed:
                mism.append("typed_missing")
            if oc == "interactive" and n_typed and sum(typed_kinds.get(k, 0) for k in HUMAN) < n_typed:
                mism.append("typed_not_human")
            if n_typed and h_in_typed < sum(dbh.values()):
                mism.append("human_not_typed")
            if c["tool_call"] != tc + inh.get("tool_call", 0):
                mism.append("tool_calls")
            if c["interrupted"] != intr:
                mism.append("interrupts")
            if c["compacted"] > comp:
                mism.append("compaction")
            if kinds.get("assistant", 0) + inh.get("assistant", 0) < c["ri_assistant"]:
                mism.append("assistant")
            line["src"] = json.dumps(meta.get("source"))[:40]
        if (first or "")[:19] != (st or "")[:19] or (last or "")[:19] != (en or "")[:19]:
            mism.append("timestamps")
            line["ts_raw"] = f"{(first or '')[:19]}..{(last or '')[:19]}"
            line["ts_db"] = f"{(st or '')[:19]}..{(en or '')[:19]}"
        line["title_origin"] = torig
        line["MISMATCH"] = ",".join(mism) or "-"
        print(json.dumps(line, ensure_ascii=False))
        if "human" in mism and prov == "claude":
            rawset = [" ".join(t.split())[:60] for t in texts] + [" ".join(h[1].split())[:60] for h in q]
            dbset = [" ".join(t.split())[:60] for t in dbhum]
            only_raw = [x for x in rawset if x not in dbset][:3]
            only_db = [x for x in dbset if x not in rawset][:3]
            print("   only_raw:", only_raw, " only_db:", only_db)


if __name__ == "__main__":
    main()
