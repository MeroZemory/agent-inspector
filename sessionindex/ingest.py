"""Incremental, idempotent ingestion.

Pipeline: discover -> stat -> group physical files by logical session (provider, pkey) ->
collapse hardlinks by (dev, ino) -> pick one canonical content unit per session ->
plan (skip | append from byte offset | full reparse) -> parse in worker processes ->
single-writer SQLite -> per-root home metadata -> finalize aggregates/titles/inherited -> FTS.
"""
from __future__ import annotations

import collections
import hashlib
import json
import os
import sys
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, ThreadPoolExecutor, wait

from . import projects
from .adapters import ADAPTERS
from .classify import HUMAN_KINDS
from .config import ANN_PATH, DB_PATH, expand_roots, load_config
from .schema import connect_rw, init_ann, init_index
from .util import iso_to_epoch, jdump, now_iso, short_sid

ACTIVE_GAP_S = 1800


def log(msg: str) -> None:
    sys.stderr.write(f"[ingest {time.strftime('%H:%M:%S')}] {msg}\n")
    sys.stderr.flush()


def head_sha(path: str) -> str | None:
    try:
        with open(path, "rb") as fh:
            return hashlib.sha1(fh.read(4096)).hexdigest()
    except OSError:
        return None


def run_job(job: dict) -> dict:
    """Worker entry point (must be importable for multiprocessing spawn)."""
    t0 = time.time()
    try:
        ad = ADAPTERS[job["adapter"]]
        res = ad.parse(job)
        res["head_sha"] = head_sha(job["path"])
    except Exception:  # never crash the run; record the error on the file row
        res = {"error": traceback.format_exc()[-2000:]}
    res["job"] = {k: job[k] for k in ("sid", "file_id", "mode", "size", "mtime_ns", "path", "adapter", "start")}
    res["secs"] = time.time() - t0
    return res


# --------------------------------------------------------------------------- discovery

def _stat_all(cands):
    out = []
    for root, c in cands:
        try:
            s = os.stat(c.path)
        except OSError:
            continue
        out.append((root, c, s.st_size, s.st_mtime_ns, s.st_dev, s.st_ino, s.st_nlink))
    return out


def discover(roots, cfg):
    skip = [s.lower() for s in cfg.get("skip_path_substrings") or []]
    cands = []
    for root in roots:
        ad = ADAPTERS[root.adapter]
        n = 0
        for c in ad.discover(root):
            if skip and any(s in c.path.lower() for s in skip):
                continue
            cands.append((root, c))
            n += 1
        log(f"discovered {n} files in {root.root_id}")
    # stat in threads (network/AV latency bound)
    chunks = [cands[i:i + 2000] for i in range(0, len(cands), 2000)]
    stats = []
    with ThreadPoolExecutor(16) as ex:
        for part in ex.map(_stat_all, chunks):
            stats.extend(part)
    return stats


# --------------------------------------------------------------------------- main

def run_ingest(source: list[str] | None = None, rebuild: bool = False, workers: int | None = None,
               max_jobs: int | None = None, quiet: bool = False) -> dict:
    t_start = time.time()
    cfg = load_config()
    caps = cfg["caps"]
    if rebuild:
        for suf in ("", "-wal", "-shm"):
            p = DB_PATH + suf
            if os.path.exists(p):
                os.remove(p)
        log("rebuild: removed index.sqlite (annotations.sqlite untouched)")
    con = connect_rw(DB_PATH)
    init_index(con)
    acon = connect_rw(ANN_PATH)
    init_ann(acon)
    acon.close()

    run_id = con.execute("INSERT INTO ingest_runs(started_at,args) VALUES(?,?)",
                         (now_iso(), jdump({"source": source, "rebuild": rebuild, "max_jobs": max_jobs}))).lastrowid
    con.commit()

    roots = expand_roots(cfg)
    if source:
        want = [s.lower() for s in source]
        roots = [r for r in roots if any(w in (r.adapter.lower(), r.root_id.lower(), r.label.lower(),
                                               ADAPTERS[r.adapter].PROVIDER) for w in want)]
    root_by_id = {r.root_id: r for r in roots}
    for r in roots:
        con.execute("INSERT OR REPLACE INTO roots(root_id,adapter,provider,path,label,rank,is_test,last_scan) "
                    "VALUES(?,?,?,?,?,?,?,?)", (r.root_id, r.adapter, ADAPTERS[r.adapter].PROVIDER, r.path, r.label,
                                                r.rank, int(r.is_test), now_iso()))
    con.commit()

    # ---- discovery + stat
    t0 = time.time()
    stats = discover(roots, cfg)
    log(f"stat {len(stats)} files in {time.time() - t0:.1f}s")

    existing = {row[0]: row for row in con.execute(
        "SELECT path,file_id,size,mtime_ns,probe_size,probe_mtime_ns,first_ts,last_ts,parsed_offset,parsed_size,"
        "parsed_mtime_ns,head_sha,state,n_lines,n_bad,sid FROM files")}
    frows = {}
    ins, upd = [], []
    for root, c, size, mtime, dev, ino, nlink in stats:
        prov = ADAPTERS[root.adapter].PROVIDER
        e = existing.get(c.path)
        if e is None:
            ins.append((c.path, root.root_id, prov, c.pkey, c.subkind, size, mtime, str(dev), str(ino), nlink, run_id))
        else:
            upd.append((root.root_id, prov, c.pkey, c.subkind, size, mtime, str(dev), str(ino), nlink, run_id, c.path))
    con.executemany("INSERT INTO files(path,root_id,provider,pkey,subkind,size,mtime_ns,dev,ino,nlink,seen_run,missing) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,0)", ins)
    con.executemany("UPDATE files SET root_id=?,provider=?,pkey=?,subkind=?,size=?,mtime_ns=?,dev=?,ino=?,nlink=?,"
                    "seen_run=?,missing=0 WHERE path=?", upd)
    if roots:
        qs = ",".join("?" * len(roots))
        con.execute(f"UPDATE files SET missing=1, role='missing' WHERE root_id IN ({qs}) AND seen_run<>?",
                    [r.root_id for r in roots] + [run_id])
    con.commit()
    fid = {row[0]: row for row in con.execute(
        "SELECT path,file_id,probe_size,probe_mtime_ns,first_ts,last_ts,parsed_offset,parsed_size,parsed_mtime_ns,"
        "head_sha,state,n_lines,n_bad FROM files WHERE seen_run=?", (run_id,))}

    # ---- group by logical session and collapse hardlinks
    groups = collections.defaultdict(list)
    for root, c, size, mtime, dev, ino, nlink in stats:
        prov = ADAPTERS[root.adapter].PROVIDER
        groups[(prov, c.pkey)].append({"root": root, "c": c, "size": size, "mtime": mtime, "unit": (dev, ino),
                                       "nlink": nlink, "row": fid[c.path]})
    log(f"{len(stats)} files -> {len(groups)} logical sessions")

    sess_prev = {row[0]: row for row in con.execute("SELECT sid, canonical_file_id FROM sessions")}

    # probe multi-unit groups (head/tail timestamps) to select the canonical copy
    to_probe = []
    for key, ents in groups.items():
        units = {e["unit"] for e in ents}
        if len(units) > 1:
            for e in _unit_reps(ents, sess_prev.get(short_sid(*key))):
                r = e["row"]
                if r[2] == e["size"] and r[3] == e["mtime"] and r[5]:
                    e["probe"] = (r[4], r[5])
                else:
                    to_probe.append(e)
    if to_probe:
        t0 = time.time()

        def _p(e):
            return e, ADAPTERS[e["root"].adapter].probe_last_ts(e["c"].path)
        with ThreadPoolExecutor(16) as ex:
            for e, pr in ex.map(_p, to_probe):
                e["probe"] = pr
        con.executemany("UPDATE files SET first_ts=?,last_ts=?,probe_size=?,probe_mtime_ns=? WHERE file_id=?",
                        [(e["probe"][0], e["probe"][1], e["size"], e["mtime"], e["row"][1]) for e in to_probe])
        con.commit()
        log(f"probed {len(to_probe)} copies in {time.time() - t0:.1f}s")

    # ---- plan
    jobs = []
    group_info = {}
    role_updates = []
    for key, ents in groups.items():
        prov, pkey = key
        sid = short_sid(prov, pkey)
        prev = sess_prev.get(sid)
        reps = _unit_reps(ents, prev)
        if len(reps) == 1:
            canon = reps[0]
        else:
            canon = max(reps, key=lambda e: ((e.get("probe") or (None, None))[1] or "", e["size"], -e["root"].rank))
        cl = (canon.get("probe") or (None, None))[1]
        for e in ents:
            if e is canon:
                continue
            if e["unit"] == canon["unit"]:
                role_updates.append(("hardlink", "hardlink_of_canonical", sid, e["row"][1]))
            else:
                pl = (e.get("probe") or (None, None))[1]
                rel = "unknown_extent" if not (pl and cl) else (
                    "same_extent" if pl == cl else ("older_extent" if pl < cl else "newer_extent"))
                role_updates.append(("copy", rel, sid, e["row"][1]))
        role_updates.append(("canonical", None, sid, canon["row"][1]))
        group_info[sid] = {"key": key, "canon": canon, "n_files": len(ents), "n_units": len(reps),
                           "is_test": all(e["root"].is_test for e in ents)}
        r = canon["row"]
        file_id = r[1]
        mode = None
        if rebuild or prev is None or prev[1] != file_id:
            mode = "full"
        elif r[7] == canon["size"] and r[8] == canon["mtime"]:
            mode = None
        elif (r[7] or 0) < canon["size"] and r[6] and r[9] and head_sha(canon["c"].path) == r[9] and r[10]:
            mode = "append"
        else:
            mode = "full"
        if mode:
            st = json.loads(r[10]) if (mode == "append" and r[10]) else None
            jobs.append({"sid": sid, "file_id": file_id, "mode": mode, "size": canon["size"],
                         "mtime_ns": canon["mtime"], "path": canon["c"].path, "adapter": canon["root"].adapter,
                         "start": r[6] if mode == "append" else 0, "state": st, "subkind": canon["c"].subkind,
                         "ctx": canon["c"].ctx, "pkey": pkey, "caps": caps,
                         "prev_lines": r[11] or 0, "prev_bad": r[12] or 0})
    con.executemany("UPDATE files SET role=?, relation=?, sid=? WHERE file_id=?", role_updates)
    con.commit()
    if max_jobs is not None and len(jobs) > max_jobs:
        jobs.sort(key=lambda j: j["path"])
        step = len(jobs) / max_jobs
        jobs = [jobs[int(i * step)] for i in range(max_jobs)]
        log(f"max_jobs: sampled {len(jobs)} jobs evenly by path")
    total_bytes = sum(j["size"] - j["start"] for j in jobs)
    log(f"plan: {len(jobs)} jobs ({sum(1 for j in jobs if j['mode'] == 'full')} full, "
        f"{sum(1 for j in jobs if j['mode'] == 'append')} append), {total_bytes / 1e9:.2f} GB to read; "
        f"{len(groups) - len(jobs)} sessions unchanged")

    # ---- execute
    fts_mark = int(con.execute("SELECT COALESCE((SELECT value FROM meta WHERE key='fts_max_mid'),0)").fetchone()[0])
    jobs.sort(key=lambda j: -(j["size"] - j["start"]))
    stats_run = collections.Counter()
    touched = set()
    workers = workers or max(2, min(16, (os.cpu_count() or 4) - 2))
    done_bytes = 0
    t0 = time.time()
    last_log = 0.0
    last_commit = time.time()
    if jobs:
        with ProcessPoolExecutor(workers) as ex:
            it = iter(jobs)
            pending = {}
            exhausted = False
            while pending or not exhausted:
                while not exhausted and len(pending) < workers * 3:
                    j = next(it, None)
                    if j is None:
                        exhausted = True
                        break
                    pending[ex.submit(run_job, j)] = j
                if not pending:
                    break
                done, _ = wait(list(pending), return_when=FIRST_COMPLETED)
                for f in done:
                    j = pending.pop(f)
                    try:
                        res = f.result()
                    except Exception as e:  # worker crash
                        res = {"error": repr(e)}
                    _write_result(con, res, j, group_info[j["sid"]], fts_mark, stats_run)
                    touched.add(j["sid"])
                    done_bytes += j["size"] - j["start"]
                    del res
                now = time.time()
                if now - last_commit > 3:
                    con.commit()
                    last_commit = now
                if not quiet and now - last_log > 15:
                    el = now - t0
                    rate = done_bytes / el if el else 0
                    eta = (total_bytes - done_bytes) / rate if rate else 0
                    log(f"{stats_run['jobs']}/{len(jobs)} jobs, {done_bytes / 1e9:.1f}/{total_bytes / 1e9:.1f} GB, "
                        f"{stats_run['messages']} msgs, {stats_run['errors']} errors, "
                        f"{rate / 1e6:.0f} MB/s, eta {eta / 60:.1f} min")
                    last_log = now
    con.commit()
    log(f"parsed {len(jobs)} jobs in {time.time() - t0:.1f}s")

    # sessions whose group changed shape but content did not still need their file-count columns refreshed
    for sid, gi in group_info.items():
        if sid not in touched:
            con.execute("UPDATE sessions SET n_files=?, n_copies=? WHERE sid=?", (gi["n_files"], gi["n_units"] - 1, sid))
    con.commit()

    # ---- per-root home metadata
    t0 = time.time()
    for r in roots:
        ad = ADAPTERS[r.adapter]
        try:
            hm = ad.home_metadata(r)
        except Exception:
            log(f"home metadata failed for {r.root_id}: {traceback.format_exc()[-300:]}")
            continue
        if hm:
            _write_home(con, r, hm, stats_run)
    con.commit()
    log(f"home metadata in {time.time() - t0:.1f}s")

    # ---- finalize
    t0 = time.time()
    _finalize(con, touched, group_info, root_by_id)
    log(f"finalize {len(touched)} sessions in {time.time() - t0:.1f}s")

    t0 = time.time()
    new_mark = con.execute("SELECT COALESCE(MAX(mid),0) FROM messages").fetchone()[0]
    con.execute("INSERT INTO messages_fts(rowid,text) SELECT mid,text FROM fts_src WHERE mid>? AND text IS NOT NULL",
                (fts_mark,))
    con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('fts_max_mid',?)", (str(new_mark),))
    con.commit()
    log(f"fts indexed mids {fts_mark}..{new_mark} in {time.time() - t0:.1f}s")

    stats_run["wall_s"] = round(time.time() - t_start, 1)
    stats_run["sessions_total"] = con.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    con.execute("UPDATE ingest_runs SET finished_at=?, stats=? WHERE run_id=?", (now_iso(), jdump(stats_run), run_id))
    con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('last_ingest',?)", (now_iso(),))
    con.commit()
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    return dict(stats_run)


def _unit_reps(ents, prev):
    """One representative entry per content unit; prefer the previous canonical file, then lowest rank."""
    by = collections.defaultdict(list)
    for e in ents:
        by[e["unit"]].append(e)
    reps = []
    prev_fid = prev[1] if prev else None
    for unit, es in by.items():
        pick = next((e for e in es if e["row"][1] == prev_fid), None)
        if pick is None:
            pick = min(es, key=lambda e: (e["root"].rank, e["c"].path))
        reps.append(pick)
    return reps


def _delete_session_content(con, sid, fts_mark):
    con.execute("INSERT INTO messages_fts(messages_fts,rowid,text) SELECT 'delete',mid,text FROM fts_src "
                "WHERE mid IN (SELECT mid FROM messages WHERE sid=? AND mid<=?) AND text IS NOT NULL", (sid, fts_mark))
    con.execute("DELETE FROM messages WHERE sid=?", (sid,))
    con.execute("DELETE FROM events WHERE sid=? AND source='transcript'", (sid,))
    con.execute("DELETE FROM aliases WHERE sid=? AND source='transcript'", (sid,))
    con.execute("DELETE FROM links WHERE src_sid=? AND source='transcript'", (sid,))
    con.execute("DELETE FROM facts WHERE sid=? AND source='transcript'", (sid,))


def _clean(v):
    if isinstance(v, str):
        return v.encode("utf-8", "replace").decode("utf-8")
    return v


def _exec_rows(con, sql, rows):
    """executemany; on lone-surrogate text (invalid UTF-8 from JSON escapes) sanitize and retry."""
    try:
        con.executemany(sql, rows)
    except UnicodeEncodeError:
        con.executemany(sql, [tuple(_clean(v) for v in r) for r in rows])


def _write_result(con, res, job, gi, fts_mark, stats_run):
    sid = job["sid"]
    file_id = job["file_id"]
    stats_run["jobs"] += 1
    if "error" in res:
        stats_run["errors"] += 1
        con.execute("UPDATE files SET error=? WHERE file_id=?", (res["error"][-1000:], file_id))
        log(f"ERROR parsing {job['path']}: {res['error'].strip().splitlines()[-1][:200]}")
        return
    if job["mode"] == "full":
        _delete_session_content(con, sid, fts_mark)
    msgs = res["messages"]
    _exec_rows(con,
        "INSERT INTO messages(sid,seq,ts,role,kind,text,text_len,truncated,tool_name,tool_id,is_error,model,origin,"
        "turn,file_id,offset,line,imported,inherited,sidechain,h,ctx_tokens) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [(sid, m[0], m[1], m[2], m[3], m[4], m[5], m[6], m[7], m[8], m[9], m[10], m[11], m[12], file_id, m[13], m[14],
          m[15], m[16], m[17], m[18], m[19]) for m in msgs])
    _exec_rows(con, "INSERT INTO events(sid,seq,ts,type,detail,value,file_id,offset,source) "
                    "VALUES(?,?,?,?,?,?,?,?,'transcript')",
                    [(sid, e[0], e[1], e[2], e[3], e[4], file_id, e[5]) for e in res["events"]])
    _exec_rows(con, "INSERT INTO aliases(sid,alias,origin,manual,ts,source) VALUES(?,?,?,?,?,'transcript') "
                    "ON CONFLICT(sid,origin,alias) DO UPDATE SET ts=COALESCE(excluded.ts,ts)",
                    [(sid, a[0], a[1], a[2], a[3]) for a in res["aliases"]])
    _exec_rows(con, "INSERT OR IGNORE INTO links(src_sid,rel,dst_sid,dst_provider,dst_pid,provenance,source) "
                    "VALUES(?,?,?,?,?,?,'transcript')",
                    [(sid, l[0], short_sid(l[1], l[2]), l[1], l[2], l[3]) for l in res["links"]])
    _exec_rows(con, "INSERT OR IGNORE INTO facts(sid,key,value,origin,ts,source) VALUES(?,?,?,?,?,'transcript')",
                    [(sid, f[0], f[1], f[2], f[3]) for f in res["facts"]])
    stats_run["messages"] += len(msgs)
    stats_run["events"] += len(res["events"])
    stats_run["bad_lines"] += res.get("n_bad", 0)
    stats_run["partial_tails"] += res.get("partial_tail", 0)
    stats_run["bytes"] += job["size"] - job["start"]
    stats_run[f"mode_{job['mode']}"] += 1

    n_bad = res.get("n_bad", 0) + (job.get("prev_bad", 0) if job["mode"] == "append" else 0)
    con.execute("UPDATE files SET parsed_offset=?, parsed_size=?, parsed_mtime_ns=?, head_sha=?, n_lines=?, n_bad=?, "
                "partial_tail=?, state=?, error=NULL WHERE file_id=?",
                (res["end_offset"], job["size"], job["mtime_ns"], res.get("head_sha"), res.get("n_lines"), n_bad,
                 res.get("partial_tail", 0), jdump(res["state"]), file_id))
    _upsert_session(con, sid, res, job, gi)


def _merge_counts(a: dict | None, b: dict | None) -> dict:
    out = dict(a or {})
    for k, v in (b or {}).items():
        out[k] = out.get(k, 0) + v
    return out


def _upsert_session(con, sid, res, job, gi):
    prov, pkey = gi["key"]
    canon = gi["canon"]
    root = canon["root"]
    m = res["meta"]
    row = con.execute("SELECT meta, rec_counts, parse_bad, out_tokens FROM sessions WHERE sid=?", (sid,)).fetchone()
    if row and job["mode"] == "append":
        old = json.loads(row[0] or "{}")
        merged = dict(old)
        for k, v in m.items():
            if k in ("models", "imported_versions"):
                merged[k] = _merge_counts(old.get(k), v)
            elif k == "first_ts":
                merged[k] = old.get(k) or v
            elif v not in (None, "", {}, []):
                merged[k] = v
        rec = _merge_counts(json.loads(row[1] or "{}"), res["rec_counts"])
        bad = (row[2] or 0) + res.get("n_bad", 0)
        outtok = (row[3] or 0) + (m.get("out_tokens_add") or 0)
    else:
        merged = dict(m)
        rec = res["rec_counts"]
        bad = res.get("n_bad", 0)
        outtok = m.get("out_tokens_add") or 0
    ctx = job["ctx"] or {}
    merged["ctx"] = {k: v for k, v in ctx.items() if k != "meta_path"}

    subkind = job["subkind"]
    agent_type = agent_name = parent_pid = None
    if prov == "claude":
        is_sub = subkind in ("subagent", "workflow_agent")
        kind = subkind
        if is_sub:
            am = merged.get("agent_meta") or {}
            agent_type = am.get("agentType")
            agent_name = am.get("description")
            parent_pid = ctx.get("parent_pkey")
            if am.get("parentAgentId"):
                parent_pid = f"{ctx.get('parent_pkey')}:agent-{am.get('parentAgentId')}"
        client = merged.get("client")
        source = client
        if is_sub:
            origin_class = "subagent"
        elif (client or "").startswith("sdk"):
            origin_class = "noninteractive"
        else:
            origin_class = "interactive"
    elif prov == "codex":
        is_sub = bool(merged.get("is_sub"))
        kind = "subagent" if is_sub else "main"
        agent_type = merged.get("agent_type")
        agent_name = merged.get("agent_name")
        parent_pid = merged.get("parent_thread") if is_sub else None
        client = merged.get("client")
        source = merged.get("source")
        origin_class = "subagent" if is_sub else ("noninteractive" if merged.get("nonint") else "interactive")
    else:
        is_sub = False
        kind = subkind
        client = merged.get("client")
        source = client
        origin_class = "unknown"
    models = merged.get("models") or {}
    model = max(models, key=models.get) if models else None
    cwd = merged.get("cwd")
    proj, proj_root, method = projects.derive(cwd, fallback=ctx.get("slug"))
    archived = 1 if ctx.get("archived") else 0
    vals = {
        "provider": prov, "pid": pkey, "kind": kind, "is_subagent": int(is_sub),
        "parent_sid": short_sid(prov, parent_pid) if parent_pid else None, "parent_pid": parent_pid,
        "agent_type": agent_type, "agent_name": agent_name, "client": client, "source": source,
        "origin_class": origin_class, "root_id": root.root_id, "account": root.label,
        "canonical_file_id": job["file_id"], "n_files": gi["n_files"], "n_copies": gi["n_units"] - 1,
        "cwd": cwd, "project": proj, "project_root": proj_root, "project_method": method,
        "git_branch": merged.get("git_branch"), "git_origin": merged.get("git_origin"), "git_sha": merged.get("git_sha"),
        "cli_version": merged.get("cli_version"), "models": jdump(models), "model": model,
        "ctx_window": merged.get("ctx_window"), "out_tokens": outtok or None,
        "is_test": int(gi["is_test"]), "archived": archived, "rec_counts": jdump(rec), "parse_bad": bad,
        "meta": jdump(merged), "updated_at": now_iso(),
    }
    cols = list(vals)
    con.execute(f"INSERT INTO sessions(sid,{','.join(cols)}) VALUES(?,{','.join('?' * len(cols))}) "
                f"ON CONFLICT(sid) DO UPDATE SET {','.join(f'{c}=excluded.{c}' for c in cols)}",
                [sid] + [vals[c] for c in cols])
    if prov == "claude" and agent_name:
        con.execute("INSERT OR IGNORE INTO aliases(sid,alias,origin,manual,ts,source) VALUES(?,?,?,?,?,'transcript')",
                    (sid, agent_name[:500], "claude_subagent_description", 0, merged.get("first_ts")))
    if parent_pid:
        con.execute("INSERT OR IGNORE INTO links(src_sid,rel,dst_sid,dst_provider,dst_pid,provenance,source) "
                    "VALUES(?,?,?,?,?,?,'transcript')",
                    (sid, "subagent_of", short_sid(prov, parent_pid), prov, parent_pid,
                     "subagent file location/meta" if prov == "claude" else "session_meta.parent_thread_id"))
    if prov == "codex" and merged.get("pid") and merged.get("pid") != pkey:
        con.execute("INSERT OR IGNORE INTO facts(sid,key,value,origin,ts,source) VALUES(?,?,?,?,?,'transcript')",
                    (sid, "session_meta_id_mismatch", merged.get("pid"), "codex_session_meta", None))


def _write_home(con, root, hm, stats_run):
    src = f"home:{root.root_id}"
    for t, col in (("aliases", "source"), ("links", "source"), ("facts", "source"), ("events", "source"),
                   ("prompt_history", "source")):
        con.execute(f"DELETE FROM {t} WHERE {col}=?", (src,))
    _exec_rows(con, "INSERT INTO aliases(sid,alias,origin,manual,ts,source) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(sid,origin,alias) DO UPDATE SET ts=COALESCE(excluded.ts,ts), source=excluded.source",
                    [(short_sid(a[0], a[1]), str(a[2])[:500], a[3], a[4], a[5], src) for a in hm.get("aliases", [])
                     if a[1] and a[2]])
    _exec_rows(con, "INSERT OR IGNORE INTO links(src_sid,rel,dst_sid,dst_provider,dst_pid,provenance,source) "
                    "VALUES(?,?,?,?,?,?,?)",
                    [(short_sid(l[0], l[1]), l[2], short_sid(l[3], l[4]), l[3], l[4], l[5], src)
                     for l in hm.get("links", []) if l[1] and l[4]])
    _exec_rows(con, "INSERT OR IGNORE INTO facts(sid,key,value,origin,ts,source) VALUES(?,?,?,?,?,?)",
                    [(short_sid(f[0], f[1]), f[2], f[3], f[4], f[5], src) for f in hm.get("facts", []) if f[1]])
    _exec_rows(con, "INSERT INTO events(sid,seq,ts,type,detail,value,file_id,offset,source) "
                    "VALUES(?,NULL,?,?,?,?,NULL,NULL,?)",
                    [(short_sid(e[0], e[1]), e[2], e[3], e[4], e[5], src) for e in hm.get("events", []) if e[1]])
    _exec_rows(con, "INSERT INTO prompt_history(sid,provider,pid,ts,text,project,source) VALUES(?,?,?,?,?,?,?)",
                    [(short_sid(p[0], p[1]) if p[1] else None, p[0], p[1], p[2], p[3], p[4], src)
                     for p in hm.get("prompt_history", [])])
    for e in hm.get("errors", []):
        log(f"home metadata {root.root_id}: {e}")
    stats_run["home_aliases"] += len(hm.get("aliases", []))
    stats_run["home_links"] += len(hm.get("links", []))
    stats_run["home_events"] += len(hm.get("events", []))


TITLE_PRIORITY = [
    ("claude_custom_title", 0), ("codex_state_name", 0), ("codex_thread_name_event", 1),
    ("claude_ai_title", 2), ("codex_session_index", 2), ("codex_state_title", 3), ("claude_summary", 3),
    ("claude_agent_name", 4), ("claude_subagent_description", 4), ("codex_agent_nickname", 5),
    ("first_human_prompt", 9),
]
_TP = dict(TITLE_PRIORITY)


def _finalize(con, touched, group_info, root_by_id):
    hk = ",".join("'%s'" % k for k in HUMAN_KINDS)
    con.execute("CREATE TEMP TABLE IF NOT EXISTS touched(sid TEXT PRIMARY KEY)")
    con.execute("DELETE FROM touched")
    con.executemany("INSERT OR IGNORE INTO touched(sid) VALUES(?)", [(s,) for s in touched])

    # inherited prefix marking for forks/imports (content identical to the link target's messages)
    con.execute("UPDATE messages SET inherited=0 WHERE sid IN (SELECT sid FROM touched) AND inherited<>0")
    pairs = con.execute("SELECT l.src_sid, l.dst_sid FROM links l JOIN touched t ON t.sid=l.src_sid "
                        "WHERE l.rel IN ('forked_from','imported_from')").fetchall()
    n_inh = 0
    for child, parent in pairs:
        ph = {r[0] for r in con.execute("SELECT h FROM messages WHERE sid=? AND h IS NOT NULL", (parent,))}
        if not ph:
            continue
        mark = []
        for mid, h, kind in con.execute("SELECT mid,h,kind FROM messages WHERE sid=? ORDER BY seq", (child,)):
            if h is not None and h in ph:
                mark.append((mid,))
            elif kind in ("human", "human_command", "human_shell", "assistant", "sdk_prompt", "agent_prompt",
                          "tool_call"):
                break
        con.executemany("UPDATE messages SET inherited=1 WHERE mid=?", mark)
        n_inh += len(mark)
    if pairs:
        log(f"inherited-prefix marking: {len(pairs)} fork/import links, {n_inh} messages marked")

    agg = {}
    for row in con.execute(
            f"SELECT sid, SUM(kind IN ({hk}) AND imported=0 AND inherited=0 AND sidechain=0),"
            f" SUM(CASE WHEN kind IN ({hk}) AND imported=0 AND inherited=0 AND sidechain=0 THEN text_len ELSE 0 END),"
            f" SUM(kind='assistant' AND imported=0 AND inherited=0 AND sidechain=0),"
            f" SUM(CASE WHEN kind='assistant' AND imported=0 AND inherited=0 AND sidechain=0 THEN text_len ELSE 0 END),"
            f" SUM(kind='tool_call' AND imported=0 AND inherited=0),"
            f" COUNT(*), SUM(imported=1 OR inherited=1), MAX(ctx_tokens), MIN(ts), MAX(ts)"
            f" FROM messages WHERE sid IN (SELECT sid FROM touched) GROUP BY sid"):
        agg[row[0]] = list(row[1:])
    ev = {}
    for row in con.execute(
            "SELECT sid, SUM(type IN ('tool_error','tool_aborted')), SUM(type IN ('api_error','rate_limit','turn_error')),"
            " SUM(type='interrupt'), SUM(type='compaction'), SUM(type='subagent_spawn'), SUM(type='tool_denied'),"
            " COUNT(*) FROM events WHERE source='transcript' AND sid IN (SELECT sid FROM touched) GROUP BY sid"):
        ev[row[0]] = list(row[1:])
    # active time: sum of inter-message gaps <= 30 min
    active = collections.defaultdict(float)
    prev_sid = prev_t = None
    for sid, ts in con.execute("SELECT m.sid, m.ts FROM messages m JOIN touched t ON t.sid=m.sid "
                               "WHERE m.ts IS NOT NULL ORDER BY m.sid, m.seq"):
        t = iso_to_epoch(ts)
        if sid == prev_sid and t is not None and prev_t is not None:
            g = t - prev_t
            if 0 < g <= ACTIVE_GAP_S:
                active[sid] += g
        prev_sid, prev_t = sid, (t if t is not None else prev_t)
    first_prompt = {}
    for sid, text in con.execute(
            f"SELECT m.sid, m.text FROM messages m JOIN (SELECT sid, MIN(seq) s FROM messages WHERE sid IN "
            f"(SELECT sid FROM touched) AND kind IN ({hk},'sdk_prompt','agent_prompt') AND imported=0 AND inherited=0 "
            f"GROUP BY sid) x ON x.sid=m.sid AND x.s=m.seq"):
        first_prompt[sid] = (text or "")[:300]
    ups = []
    for sid in touched:
        a = agg.get(sid) or [0, 0, 0, 0, 0, 0, 0, None, None, None]
        e = ev.get(sid) or [0, 0, 0, 0, 0, 0, 0]
        meta = con.execute("SELECT meta FROM sessions WHERE sid=?", (sid,)).fetchone()
        m = json.loads(meta[0]) if meta and meta[0] else {}
        starts = [x for x in (m.get("first_ts"), a[8]) if x]
        ends = [x for x in (m.get("last_ts"), a[9]) if x]
        st = min(starts) if starts else None
        en = max(ends) if ends else None
        dur = (iso_to_epoch(en) - iso_to_epoch(st)) if (st and en and iso_to_epoch(en) and iso_to_epoch(st)) else None
        fp = first_prompt.get(sid)
        ups.append((a[0] or 0, a[1] or 0, a[2] or 0, a[3] or 0, a[4] or 0, e[0] or 0, e[1] or 0, e[2] or 0, e[3] or 0,
                    e[4] or 0, e[5] or 0, e[6] or 0, a[5] or 0, a[6] or 0, a[7], st, en, dur,
                    round(active.get(sid, 0.0), 1), fp, sid))
        if fp:
            con.execute("DELETE FROM aliases WHERE sid=? AND origin='first_human_prompt'", (sid,))
            con.execute("INSERT OR IGNORE INTO aliases(sid,alias,origin,manual,ts,source) "
                        "VALUES(?,?,?,?,?,'transcript')", (sid, fp[:200], "first_human_prompt", 0, st))
    con.executemany(
        "UPDATE sessions SET human_msgs=?, human_chars=?, assistant_msgs=?, assistant_chars=?, tool_calls=?,"
        " tool_errors=?, errors=?, interrupts=?, compactions=?, subagent_spawns=?, denials=?, events_n=?, msgs_n=?,"
        " inherited_msgs=?, max_ctx_tokens=?, started_at=?, last_activity_at=?, duration_s=?, active_s=?,"
        " first_prompt=? WHERE sid=?", ups)
    # imported-only sessions
    con.execute("UPDATE sessions SET origin_class='imported' WHERE sid IN (SELECT sid FROM touched) "
                "AND msgs_n>0 AND inherited_msgs>=msgs_n")
    con.execute("UPDATE sessions SET origin_class='imported' WHERE sid IN (SELECT sid FROM touched) AND sid IN "
                "(SELECT sid FROM messages WHERE imported=1 GROUP BY sid) AND NOT EXISTS "
                "(SELECT 1 FROM messages m WHERE m.sid=sessions.sid AND m.imported=0 AND m.kind IN "
                "('human','human_command','human_shell','assistant'))")
    # archived via codex state facts
    con.execute("UPDATE sessions SET archived=1 WHERE sid IN (SELECT sid FROM facts WHERE key='state.archived' "
                "AND value IN ('1','true'))")
    # titles for every session (aliases can change via home metadata without a reparse)
    best = {}
    for sid, alias, origin, ts in con.execute("SELECT sid, alias, origin, ts FROM aliases"):
        p = _TP.get(origin, 6)
        cur = best.get(sid)
        k = (p, "" if ts is None else ts)
        if cur is None or p < cur[0][0] or (p == cur[0][0] and (ts or "") > (cur[0][1] or "")):
            best[sid] = (k, alias, origin)
    con.executemany("UPDATE sessions SET title=?, title_origin=? WHERE sid=?",
                    [(v[1], v[2], sid) for sid, v in best.items()])
    con.commit()
