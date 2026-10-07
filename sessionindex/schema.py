"""SQLite schema. index.sqlite is fully rebuildable; annotations.sqlite is never touched by ingest."""
from __future__ import annotations

import os
import re
import sqlite3

SCHEMA_VERSION = "3"

INDEX_DDL = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE IF NOT EXISTS roots(
  root_id TEXT PRIMARY KEY, adapter TEXT, provider TEXT, path TEXT, label TEXT,
  rank INTEGER, is_test INTEGER, n_files INTEGER, last_scan TEXT);

CREATE TABLE IF NOT EXISTS files(
  file_id INTEGER PRIMARY KEY,
  path TEXT UNIQUE NOT NULL,
  root_id TEXT, provider TEXT, pkey TEXT, sid TEXT, subkind TEXT,
  size INTEGER, mtime_ns INTEGER, dev TEXT, ino TEXT, nlink INTEGER,
  role TEXT, relation TEXT,
  first_ts TEXT, last_ts TEXT, probe_size INTEGER, probe_mtime_ns INTEGER, probe_lines INTEGER,
  parsed_offset INTEGER, parsed_size INTEGER, parsed_mtime_ns INTEGER, head_sha TEXT,
  n_lines INTEGER, n_bad INTEGER, partial_tail INTEGER, state TEXT,
  seen_run INTEGER, missing INTEGER DEFAULT 0, error TEXT);
CREATE INDEX IF NOT EXISTS files_pkey ON files(provider, pkey);
CREATE INDEX IF NOT EXISTS files_sid ON files(sid);

CREATE TABLE IF NOT EXISTS sessions(
  sid TEXT PRIMARY KEY, provider TEXT, pid TEXT, kind TEXT, is_subagent INTEGER,
  parent_sid TEXT, parent_pid TEXT, agent_type TEXT, agent_name TEXT,
  client TEXT, source TEXT, origin_class TEXT,
  root_id TEXT, account TEXT, canonical_file_id INTEGER, n_files INTEGER, n_copies INTEGER,
  cwd TEXT, cwd_key TEXT, project TEXT, project_root TEXT, project_method TEXT,
  git_branch TEXT, git_origin TEXT, git_sha TEXT,
  cli_version TEXT, models TEXT, model TEXT,
  started_at TEXT, last_activity_at TEXT, duration_s REAL, active_s REAL,
  human_msgs INTEGER, human_chars INTEGER, assistant_msgs INTEGER, assistant_chars INTEGER,
  tool_calls INTEGER, tool_errors INTEGER, errors INTEGER, interrupts INTEGER, compactions INTEGER,
  subagent_spawns INTEGER, denials INTEGER, events_n INTEGER, msgs_n INTEGER, inherited_msgs INTEGER,
  max_ctx_tokens INTEGER, ctx_window INTEGER, out_tokens INTEGER,
  title TEXT, title_origin TEXT, first_prompt TEXT,
  is_test INTEGER, archived INTEGER, rec_counts TEXT, parse_bad INTEGER, meta TEXT, updated_at TEXT,
  body INTEGER DEFAULT 0, cat_sig TEXT, size_bytes INTEGER, hist_prompts INTEGER);
CREATE INDEX IF NOT EXISTS sessions_last ON sessions(last_activity_at);
CREATE INDEX IF NOT EXISTS sessions_project ON sessions(project);
CREATE INDEX IF NOT EXISTS sessions_pid ON sessions(pid);
CREATE INDEX IF NOT EXISTS sessions_parent ON sessions(parent_sid);
CREATE INDEX IF NOT EXISTS sessions_cwd_key ON sessions(cwd_key);

CREATE TABLE IF NOT EXISTS messages(
  mid INTEGER PRIMARY KEY,
  sid TEXT NOT NULL, seq INTEGER NOT NULL, ts TEXT,
  role TEXT, kind TEXT, text TEXT, text_len INTEGER, truncated INTEGER,
  tool_name TEXT, tool_id TEXT, is_error INTEGER, model TEXT, origin TEXT,
  turn INTEGER, file_id INTEGER, offset INTEGER, line INTEGER,
  imported INTEGER DEFAULT 0, inherited INTEGER DEFAULT 0, sidechain INTEGER DEFAULT 0,
  h INTEGER, ctx_tokens INTEGER);
CREATE INDEX IF NOT EXISTS messages_sid_seq ON messages(sid, seq);
CREATE INDEX IF NOT EXISTS messages_kind ON messages(kind);

CREATE TABLE IF NOT EXISTS events(
  eid INTEGER PRIMARY KEY, sid TEXT NOT NULL, seq INTEGER, ts TEXT, type TEXT,
  detail TEXT, value REAL, file_id INTEGER, offset INTEGER, source TEXT);
CREATE INDEX IF NOT EXISTS events_sid ON events(sid, seq);
CREATE INDEX IF NOT EXISTS events_type ON events(type);

CREATE TABLE IF NOT EXISTS aliases(
  sid TEXT NOT NULL, alias TEXT, origin TEXT, manual INTEGER, ts TEXT, source TEXT, current INTEGER,
  UNIQUE(sid, origin, alias));
CREATE INDEX IF NOT EXISTS aliases_sid ON aliases(sid);

CREATE TABLE IF NOT EXISTS links(
  src_sid TEXT NOT NULL, rel TEXT NOT NULL, dst_sid TEXT, dst_provider TEXT, dst_pid TEXT,
  provenance TEXT, source TEXT,
  UNIQUE(src_sid, rel, dst_sid));
CREATE INDEX IF NOT EXISTS links_dst ON links(dst_sid);

CREATE TABLE IF NOT EXISTS facts(
  sid TEXT NOT NULL, key TEXT, value TEXT, origin TEXT, ts TEXT, source TEXT,
  UNIQUE(sid, key, origin, value));
CREATE INDEX IF NOT EXISTS facts_sid ON facts(sid);

CREATE TABLE IF NOT EXISTS prompt_history(
  sid TEXT, provider TEXT, pid TEXT, ts TEXT, text TEXT, project TEXT, source TEXT);
CREATE INDEX IF NOT EXISTS prompt_history_sid ON prompt_history(sid);
CREATE INDEX IF NOT EXISTS prompt_history_pid_ts ON prompt_history(pid, ts);
CREATE INDEX IF NOT EXISTS events_sid_ts ON events(sid, ts);
-- (source, sid) composites: fast per-root deletes AND per-session deletes (a bare source index would be
-- picked for `sid=? AND source='transcript'` and scan every transcript row)
DROP INDEX IF EXISTS events_source;
DROP INDEX IF EXISTS aliases_source;
DROP INDEX IF EXISTS links_source;
DROP INDEX IF EXISTS facts_source;
CREATE INDEX IF NOT EXISTS prompt_history_source ON prompt_history(source);
CREATE INDEX IF NOT EXISTS events_source_sid ON events(source, sid);
CREATE INDEX IF NOT EXISTS aliases_source_sid ON aliases(source, sid);
CREATE INDEX IF NOT EXISTS links_source_sid ON links(source, src_sid);
CREATE INDEX IF NOT EXISTS facts_source_sid ON facts(source, sid);

CREATE VIEW IF NOT EXISTS fts_src AS SELECT mid, CASE
  WHEN kind IN ('developer','context_injection','system_reminder','meta','command_expansion','hook_output') THEN NULL
  WHEN kind='tool_output' AND is_error=1 THEN text
  WHEN kind='tool_output' AND length(text)>768 THEN substr(text,1,256)||' … '||substr(text,-512)
  WHEN kind='tool_call' AND length(text)>512 THEN substr(text,1,512)
  ELSE text END AS text FROM messages;
CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  text, content='fts_src', content_rowid='mid', tokenize='trigram');

CREATE TABLE IF NOT EXISTS orca(
  sid TEXT, kind TEXT, key TEXT, name TEXT, detail TEXT, ts TEXT, method TEXT);
CREATE INDEX IF NOT EXISTS orca_sid ON orca(sid);

CREATE TABLE IF NOT EXISTS ingest_runs(
  run_id INTEGER PRIMARY KEY, started_at TEXT, finished_at TEXT, args TEXT, stats TEXT);
"""

ANN_DDL = """
CREATE TABLE IF NOT EXISTS annotations(
  id INTEGER PRIMARY KEY,
  sid TEXT NOT NULL,
  seq INTEGER,
  tag TEXT,
  note TEXT,
  author TEXT,
  created_at TEXT,
  anchor TEXT);
CREATE INDEX IF NOT EXISTS annotations_sid ON annotations(sid);
CREATE INDEX IF NOT EXISTS annotations_tag ON annotations(tag);
"""


def connect_rw(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path, timeout=60)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")
    con.execute("PRAGMA cache_size=-262144")
    return con


class SchemaMismatch(RuntimeError):
    pass


def init_index(con: sqlite3.Connection) -> None:
    try:
        row = con.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    except sqlite3.OperationalError:
        row = None
    if row and row[0] != SCHEMA_VERSION:
        raise SchemaMismatch(f"index.sqlite has schema {row[0]}, code expects {SCHEMA_VERSION}: "
                             f"run `sx ingest --rebuild` (annotations are kept)")
    con.executescript(INDEX_DDL)
    con.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version',?)", (SCHEMA_VERSION,))
    con.commit()


def init_ann(con: sqlite3.Connection) -> None:
    con.executescript(ANN_DDL)
    con.commit()


_RE_CACHE: dict = {}


def _regexp(pat, s):
    if s is None or pat is None:
        return 0
    r = _RE_CACHE.get(pat)
    if r is None:
        r = _RE_CACHE[pat] = re.compile(pat, re.I | re.S)
    return 1 if r.search(s) else 0


def connect_ro(index_path: str, ann_path: str | None = None) -> sqlite3.Connection:
    """Read-only connection (mode=ro + query_only); annotations DB attached as `ann`."""
    uri = "file:" + index_path.replace("\\", "/") + "?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=30)
    con.execute("PRAGMA query_only=1")
    con.create_function("regexp", 2, _regexp, deterministic=True)  # enables `text REGEXP 'pat'` (case-insensitive)
    if ann_path and os.path.exists(ann_path):
        con.execute("ATTACH DATABASE ? AS ann", ("file:" + ann_path.replace("\\", "/") + "?mode=ro",))
    return con
