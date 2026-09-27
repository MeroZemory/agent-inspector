"""Orca metadata adapter (metadata only, read-only). Orca stores no transcripts of its own.

Links Orca's names and signals onto catalog sessions (table `orca`, plus aliases with origin orca_*):
  tab        profiles/*/orca-data.json workspaceSession tabs: customTitle/customLabel/quickCommandLabel/
             generatedTitle/defaultTitle; aiVaultTitle{agent,sessionId,title} is a direct tab->session join
  pane       sleepingAgentSessionsByPaneKey and agent-hooks/last-status.json (providerSession.id, state, model)
  worktree   orca-claude-usage.json / orca-codex-usage.json: sessionId -> primaryWorktreeId, lastCwd, branch;
             names from orca-data.json worktreeMeta.displayName / folderWorkspaces[].name
  pty        terminal-history/<ptyId>/meta.json (cwd, startedAt, endedAt, exitCode); output.log is never read
  orch_*     orchestration.db (mode=ro): dispatch_contexts / worker_dispatches / worker_terminal_resources
             (status, failures, last error) joined to sessions through their pane key
Join: provider session id when present (method=sessionId); otherwise cwd + time overlap between the Orca
pty/pane and main sessions with the same normalized cwd (method=cwd_time; may link several sessions).
Never opened: orca-runtime.json, claude-accounts/**, recovery-backups/**, recovery-tools/**, ai-vault/**.
"""
from __future__ import annotations

import collections
import glob
import json
import os
import sqlite3
import time
import urllib.parse

from .util import iso_to_epoch, jdump, norm_path, norm_ts

BASE = os.path.expanduser(os.environ.get("SX_ORCA_DIR") or "~/AppData/Roaming/orca")
_FORBIDDEN = ("orca-runtime.json", "claude-accounts", "recovery-backups", "recovery-tools", "ai-vault")
_DETAIL_COLS = ("status", "state", "stage", "failure_count", "last_failure", "last_error", "termination_reason",
                "recovery_attempt_count", "display_name", "run_id", "task_id")


def _safe(path: str) -> bool:
    n = path.replace("\\", "/").lower()
    return not any(f in n for f in _FORBIDDEN)


def _load(path: str):
    if not _safe(path) or not os.path.isfile(path):
        return None
    try:
        with open(path, "rb") as fh:  # read-only
            return json.loads(fh.read().decode("utf-8-sig"))
    except (OSError, ValueError):
        return None


def _walk(o, pred, key=None, depth=0, parent=None):
    """Yield (key, dict, parent_key) for dicts satisfying pred (not descending into matches)."""
    if depth > 6:
        return
    if isinstance(o, dict):
        if pred(o):
            yield key, o, parent
            return
        for k, v in o.items():
            if isinstance(v, (dict, list)):
                yield from _walk(v, pred, k, depth + 1, key)
    elif isinstance(o, list):
        for v in o:
            if isinstance(v, (dict, list)):
                yield from _walk(v, pred, key, depth + 1, parent)


def _is_wid(x) -> bool:
    """worktreeId = '<repoId>::<path>' | 'folder:<uuid>'"""
    return isinstance(x, str) and ("::" in x or x.startswith("folder:"))


def _ep(v):
    t = norm_ts(v) if v not in (None, "") else None
    return iso_to_epoch(t) if t else None


class _Sessions:
    def __init__(self, con):
        self.by_pid = {}
        self.by_cwd = collections.defaultdict(list)
        for sid, pid, cwd_key, st, en, sub in con.execute(
                "SELECT sid, pid, cwd_key, started_at, last_activity_at, is_subagent FROM sessions"):
            if pid:
                self.by_pid[str(pid).lower()] = sid
            if cwd_key and not sub and st:
                self.by_cwd[cwd_key].append((iso_to_epoch(st) or 0, iso_to_epoch(en) or 0, sid))

    def by_id(self, pid):
        return self.by_pid.get(str(pid).lower()) if pid else None

    def by_cwd_time(self, cwd, t0, t1):
        """Main sessions in the same cwd that started inside [t0, t1] (t1 open-ended -> now)."""
        key = (norm_path(cwd) or "").lower()
        if not key or t0 is None:
            return []
        hi = (t1 or time.time()) + 120
        return [sid for st, en, sid in self.by_cwd.get(key, ()) if t0 - 120 <= st <= hi][:20]


def collect(con) -> tuple[list, list, collections.Counter]:
    """Return (orca rows, alias rows, stats). Rows: (sid, kind, key, name, detail, ts, method)."""
    S = _Sessions(con)
    rows, aliases = [], []
    stats = collections.Counter()
    wt_name = {}
    tabs = {}          # tabId -> (name, worktreeId)
    tab_ptys = {}      # tabId -> {leafId: ptyId}
    pane_sid = {}      # paneKey -> sid (direct)
    pty_meta = {}      # ptyId -> (cwd, t0, t1, exitCode, startedAt)

    # --- PTY meta (cwd/time) first: fallback joins need it
    for mp in glob.glob(os.path.join(BASE, "terminal-history", "*", "meta.json")):
        d = _load(mp)
        if not isinstance(d, dict):
            continue
        pty = urllib.parse.unquote(os.path.basename(os.path.dirname(mp)))
        pty_meta[pty] = (d.get("cwd"), _ep(d.get("startedAt")), _ep(d.get("endedAt")), d.get("exitCode"),
                         d.get("startedAt"))

    def link(kind, key, name, detail, ts, sid=None, cwd=None, t0=None, t1=None):
        if sid:
            rows.append((sid, kind, key, name, detail, ts, "sessionId"))
            return [sid]
        sids = S.by_cwd_time(cwd, t0, t1) if cwd else []
        for s in sids:
            rows.append((s, kind, key, name, detail, ts, "cwd_time"))
        if not sids:
            stats[f"unlinked_{kind}"] += 1
        return sids

    def via_pty(kind, key, name, detail, ts, pty_ids):
        out = []
        for pty in pty_ids:
            pm = pty_meta.get(pty)
            if pm:
                out += link(kind, key, name, detail, ts, cwd=pm[0], t0=pm[1], t1=pm[2])
        if not out and not pty_ids:
            stats[f"unlinked_{kind}"] += 1
        return out

    # --- orca-data.json (every profile)
    for od_path in glob.glob(os.path.join(BASE, "profiles", "*", "orca-data.json")):
        od = _load(od_path)
        if not isinstance(od, dict):
            continue
        for wid, meta in (od.get("worktreeMeta") or {}).items() if isinstance(od.get("worktreeMeta"), dict) else ():
            if isinstance(meta, dict) and meta.get("displayName"):
                wt_name[wid] = str(meta["displayName"])
        for fw in od.get("folderWorkspaces") or []:
            if isinstance(fw, dict) and fw.get("name") and fw.get("id"):
                fid = str(fw["id"])
                wt_name[fid if fid.startswith("folder:") else f"folder:{fid}"] = str(fw["name"])
        ws = od.get("workspaceSession") or {}
        for tid, lay in (ws.get("terminalLayoutsByTabId") or {}).items():
            if isinstance(lay, dict) and isinstance(lay.get("ptyIdsByLeafId"), dict):
                tab_ptys[tid] = {k: v for k, v in lay["ptyIdsByLeafId"].items() if isinstance(v, str)}
        is_tab = (lambda d: any(k in d for k in ("customTitle", "generatedTitle", "defaultTitle", "aiVaultTitle",
                                                  "customLabel", "quickCommandLabel")))
        for src in ("tabsByWorktree", "unifiedTabs"):
            for key, d, parent in _walk(ws.get(src), is_tab, key=None):
                tid = str(d.get("id") or d.get("tabId") or (key if not _is_wid(key) else "") or "")
                wid = d.get("worktreeId") or (key if _is_wid(key) else (parent if _is_wid(parent) else None))
                av = d.get("aiVaultTitle") if isinstance(d.get("aiVaultTitle"), dict) else {}
                name = next((str(d[k]) for k in ("customTitle", "customLabel", "quickCommandLabel", "generatedTitle")
                             if d.get(k)), None) or av.get("title") or d.get("defaultTitle")
                if not tid or tid in tabs:
                    continue
                tabs[tid] = (name, wid)
                wname = wt_name.get(wid) if wid else None
                detail = f"worktree={wname or wid or ''}"
                sid = S.by_id(av.get("sessionId"))
                sids = link("tab", tid, name, detail, None, sid=sid) if sid else \
                    via_pty("tab", tid, name, detail, None, list(tab_ptys.get(tid, {}).values()))
                for s in sids:
                    if name:
                        aliases.append((s, str(name)[:500], "orca_tab_title", None, None))
                    if av.get("title") and sid:
                        aliases.append((s, str(av["title"])[:500], "orca_ai_vault_title", 0, None))
                    if wname:
                        aliases.append((s, wname[:500], "orca_worktree_name", None, None))
        for pk, d in (ws.get("sleepingAgentSessionsByPaneKey") or {}).items():
            if not isinstance(d, dict):
                continue
            ps = d.get("providerSession") if isinstance(d.get("providerSession"), dict) else {}
            sid = S.by_id(ps.get("id"))
            ts = norm_ts(d.get("capturedAt")) if d.get("capturedAt") else None
            if sid:
                pane_sid[pk] = sid
                link("pane", pk, (tabs.get(pk.split(":")[0]) or (None,))[0], f"sleeping state={d.get('state')}", ts,
                     sid=sid)
            else:
                stats["unlinked_pane"] += 1

    # --- agent-hooks/last-status.json (pane snapshot)
    ls = _load(os.path.join(BASE, "agent-hooks", "last-status.json"))
    for pk, d, _ in _walk(ls, lambda d: isinstance(d.get("providerSession"), dict)):
        pk = str(d.get("paneKey") or pk or "")
        ps = d["providerSession"]
        sid = S.by_id(ps.get("id"))
        pl = d.get("payload") if isinstance(d.get("payload"), dict) else {}
        detail = f"{d.get('source') or ''} {d.get('hookEventName') or ''} state={pl.get('state') or d.get('state')}" \
                 f" model={d.get('model') or ''}".strip()
        ts = norm_ts(d.get("receivedAt")) if d.get("receivedAt") else None
        if sid:
            pane_sid.setdefault(pk, sid)
            link("pane", pk, (tabs.get(pk.split(":")[0]) or (None,))[0], detail[:300], ts, sid=sid)
            tname = (tabs.get(pk.split(":")[0]) or (None,))[0]
            if tname:
                aliases.append((sid, str(tname)[:500], "orca_tab_title", None, ts))
        else:
            stats["unlinked_pane"] += 1

    # --- usage caches: sessionId -> worktree
    for fn in ("orca-claude-usage.json", "orca-codex-usage.json"):
        data = _load(os.path.join(BASE, fn))
        for key, d, _ in _walk(data, lambda d: "primaryWorktreeId" in d or "lastCwd" in d):
            sid = S.by_id(d.get("sessionId") or d.get("id") or key)
            if not sid:
                stats["unlinked_worktree"] += 1
                continue
            wid = d.get("primaryWorktreeId")
            wname = wt_name.get(wid) if wid else None
            name = wname or (os.path.basename(norm_path(d.get("lastCwd")) or "") or None)
            link("worktree", wid, name, f"branch={d.get('lastGitBranch') or ''} repo={d.get('primaryRepoId') or ''}",
                 norm_ts(d.get("lastTimestamp")) if d.get("lastTimestamp") else None, sid=sid)
            if wname:
                aliases.append((sid, wname[:500], "orca_worktree_name", None, None))

    # --- PTY rows (cwd + time)
    for pty, (cwd, t0, t1, code, started) in pty_meta.items():
        link("pty", pty, wt_name.get(pty.split("@@")[0]), f"exit={code}", norm_ts(started) if started else None,
             cwd=cwd, t0=t0, t1=t1)

    # --- orchestration.db
    op = os.path.join(BASE, "orchestration.db")
    if os.path.isfile(op):
        try:
            oc = sqlite3.connect("file:" + op.replace("\\", "/") + "?mode=ro", uri=True, timeout=10)
            oc.row_factory = sqlite3.Row
            tables = {r[0] for r in oc.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            task_name = {}
            if "tasks" in tables:
                tcols = {r[1] for r in oc.execute("PRAGMA table_info(tasks)")}
                if "id" in tcols:
                    nm = "display_name" if "display_name" in tcols else None
                    for r in oc.execute(f"SELECT id{', ' + nm if nm else ''}{', status' if 'status' in tcols else ''} "
                                        f"FROM tasks"):
                        task_name[r["id"]] = (r[nm] if nm else None, r["status"] if "status" in tcols else None)
            for t in ("dispatch_contexts", "worker_dispatches", "worker_terminal_resources"):
                if t not in tables:
                    continue
                cols = [r[1] for r in oc.execute(f"PRAGMA table_info({t})")]
                pcol = next((c for c in ("assignee_pane_key", "pane_key") if c in cols), None)
                if not pcol:
                    stats[f"orch_{t}_no_pane_col"] += 1
                    continue
                for r in oc.execute(f"SELECT * FROM {t}"):
                    d = dict(r)
                    pk = d.get(pcol)
                    tn = task_name.get(d.get("task_id")) if d.get("task_id") is not None else None
                    det = {k: d[k] for k in _DETAIL_COLS if k in d and d[k] not in (None, "")}
                    if tn and tn[1]:
                        det["task_status"] = tn[1]
                    ts = next((d[k] for k in ("updated_at", "created_at", "started_at") if d.get(k)), None)
                    key = str(d.get("id") or pk or "")
                    name = tn[0] if tn else None
                    sid = pane_sid.get(pk) if pk else None
                    if sid:
                        link(f"orch_{t}", key, name, jdump(det)[:400], norm_ts(ts) if ts else None, sid=sid)
                    elif pk:
                        tid, _, leaf = str(pk).partition(":")
                        pty = (tab_ptys.get(tid) or {}).get(leaf)
                        via_pty(f"orch_{t}", key, name, jdump(det)[:400], norm_ts(ts) if ts else None,
                                [pty] if pty else [])
            oc.close()
        except sqlite3.Error as e:
            stats["orchestration_error"] += 1
            stats["orchestration_error_msg:" + str(e)[:60]] += 1
    return rows, aliases, stats


def collect_and_write(con) -> dict:
    rows, aliases, stats = collect(con)
    con.execute("DELETE FROM orca")
    con.execute("DELETE FROM aliases WHERE source='orca'")
    con.executemany("INSERT INTO orca(sid,kind,key,name,detail,ts,method) VALUES(?,?,?,?,?,?,?)",
                    [(s, k, key, (str(n)[:300] if n else None), d, ts, m) for s, k, key, n, d, ts, m in rows])
    con.executemany("INSERT INTO aliases(sid,alias,origin,manual,ts,source) VALUES(?,?,?,?,?,'orca') "
                    "ON CONFLICT(sid,origin,alias) DO UPDATE SET ts=COALESCE(excluded.ts,ts)",
                    [a for a in aliases if a[1] and a[1].strip()])
    con.commit()
    by = collections.Counter(f"{r[1]}/{r[6]}" for r in rows)
    out = {"rows": len(rows), "sessions_linked": len({r[0] for r in rows}), "aliases": len(aliases)}
    out.update({f"rows_{k}": v for k, v in sorted(by.items())})
    out.update({k: v for k, v in stats.items()})
    return out
