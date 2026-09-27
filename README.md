# session-index (`sx`)

Local, agent-queryable catalog of every AI coding-agent session transcript on this PC
(Claude Code, Codex CLI/Desktop, Orca-run Codex homes, and Claude-compatible `ses_*` transcripts).
It is a **catalog + query surface**, not an analysis: agents (Claude subagents, Codex CLI agents with
only a shell) orient with one command and decide what to read.

```
python sx.py guide            # schema, kinds, event types, examples, caveats (start here)
sx.cmd stats --by month       # Windows wrapper;  ./sx on POSIX shells
```

Stdlib-only Python 3.11 + SQLite FTS5 (`tokenize='trigram'`, so Korean and English substrings both match).

## Layout

```
sx.py / sx.cmd / sx          entry point + wrappers
sources.json                 source roots (adapter, path/glob, label, rank, is_test) and text caps
sessionindex/
  config.py                  root expansion, data paths (env overrides: SX_DATA_DIR, SX_DB, SX_ANN_DB, SX_SOURCES)
  schema.py                  DDL (index.sqlite) and annotations DDL (annotations.sqlite)
  classify.py                human-vs-injected rules (message kinds)
  adapters/base.py           adapter contract + ParseOut row accumulator
  adapters/claude.py         Claude Code transcripts + ~/.claude/history.jsonl + codex-import-history.json
  adapters/codex.py          Codex rollouts + CODEX_HOME metadata (session_index, state_5, thread_history_1, goals_1, history, imports)
  adapters/cc_transcripts.py ~/.claude/transcripts/ses_*.jsonl (OpenCode-style ids; writer not recorded)
  ingest.py                  discovery, dedupe, canonical selection, incremental plan, worker pool, finalize, FTS
  projects.py                derived project from cwd (git root / git-worktree -> main repo / patterns)
  cli.py, guide.py           query CLI and the self-describing guide
tools/selftest.py            adapter sanity + append-resume equivalence on a small subset (no index needed)
tools/spotcheck.py           raw-vs-index spot checks (independent reader)
tools/validate_human.py      classification check against typed-prompt logs
data/                        (gitignored) index.sqlite (rebuildable) + annotations.sqlite (never touched by ingest)
```

## Catalog (the full list) -> ingest (selected bodies) -> read

```
python sx.py catalog                   # every session on the PC, light: ~4-5 min first run, incremental after
python sx.py sessions --body no --sort hist --limit 20   # pick sessions from the catalog
python sx.py ingest --sid <sid>,<sid>  # parse only those bodies, then: sx show / read / raw / search
```

`sx catalog` never parses transcript bodies. Per logical session (same inode/canonical-copy dedupe as ingest,
except that size stands in for the line count among copies tied on last timestamp) it reads the file stat, the
first 64 KB (Codex: the session_meta line) and the last 64 KB, plus provider stores (Codex session_index.jsonl,
state_5 threads/thread_spawn_edges, thread_history_1, goals_1; history.jsonl per-session typed-prompt counts;
Claude subagent .meta.json) and Orca metadata. Rows get `body=0`; `sx ingest` sets `body=1`; a catalog run
that sees a changed file marks `body=2` (stale). `read`/`raw` refuse catalog-only rows with the exact ingest
command; `search` warns how many matching sessions are catalog-only. `sx ingest` requires a selector
(`--sid/--file/--project/--since/--until/--source`) or `--all`.
Measured 2026-09-27: first full catalog 260 s wall, 18,555 sessions, DB 187 MB; incremental re-run 44 s.

**Orca** (`sessionindex/orca.py`, metadata only, read-only; never opens orca-runtime.json, claude-accounts/**,
recovery-*/**, ai-vault/**): tab/pane/worktree names, usage-cache session->worktree mappings, pane hook status,
PTY meta (cwd/start/end/exit) and orchestration dispatches go to table `orca` (+ aliases `orca_tab_title`,
`orca_ai_vault_title`, `orca_worktree_name`), joined by provider session id (`method=sessionId`) or by same cwd
+ start time within the PTY lifetime (`method=cwd_time`, may link several sessions).

## Ingest

Index on demand (only what you need); selectors combine, and only matching sessions are probed/parsed:

```
python sx.py ingest --sid x2ezyvekqm,01a06ef9-7ac1      # one or more sessions (short sid or provider id prefix)
python sx.py ingest --file C:/Users/me/.codex/sessions/2026/09/05/rollout-...jsonl   # the session owning a file
python sx.py ingest --project Agentryx --since 2026-09-20            # cwd/project substring + changed since (UTC)
python sx.py ingest --source orca-acct-b4012820 --until 2026-09-01   # one root; sessions started before a date
python sx.py ingest                    # everything (all roots in sources.json), incremental
python sx.py ingest --reparse [--sid ...|--source ...]   # re-parse in place after classifier changes
python sx.py ingest --reparse-sql "SELECT sid FROM sessions WHERE provider='claude'"
python sx.py ingest --rebuild          # delete index.sqlite and re-parse (annotations kept)
```

Re-running the same selection is a no-op for unchanged files. Each run also refreshes per-root home metadata
(aliases/links/typed-prompt logs; ~25-50 s when a CODEX_HOME changed). Measured full build of this PC
(2026-09-27, 18.5k sessions, 81 GB canonical): parse ~35 min, finalize ~7 min, FTS ~70 min, DB ~12 GB.

* **Sources are opened read-only.** Transcripts are read with `open(..., 'rb')`; provider SQLite files with
  `mode=ro` URIs. Nothing under a source root is written, moved or deleted. Credential files are never
  opened (adapters only open `*.jsonl`, `*.meta.json`, the named metadata files, and `state_5`/`thread_history_1`/`goals_1` SQLite DBs).
* **Physical vs logical.** Every discovered file is a row in `files` (path, root, size, mtime, dev/ino, role).
  Files are grouped by logical session key (Claude `sessionId` or `<sessionId>:agent-<id>` for subagents;
  Codex thread id = the last UUID in the rollout file name). Measured 2026-09-27: the file-name id equals
  `session_meta.payload.id` in 18,157 of 18,158 parseable unique Codex files and the Claude file name equals
  `sessionId` in all main/subagent files. The one Codex exception is a repair artifact
  (`rollout-...-<idA>_<idB>.jsonl`, content = thread B, header rewritten to A); it stays grouped with B and gets
  fact `session_meta_id_mismatch`. Hardlinks (same `(st_dev, st_ino)`) collapse to one content unit and only
  one representative per unit is ever opened. When a session has several distinct units (Codex homes copied
  across Orca accounts, archived vs active, migrated/rewritten copies), the **canonical** copy is: latest
  last-record timestamp (head/tail probe), then **more lines** (counted only for copies tied on that
  timestamp; cached in `files.probe_lines` per size+mtime), then lower root rank (`~/.codex` = 0). Only the
  canonical copy is parsed; the others stay in `files` with `role=hardlink|copy` and
  `relation=same_extent|older_extent|newer_extent` (by last timestamp).
* **Incremental.** Each canonical file stores `parsed_offset`, size, mtime, a SHA-1 of its first 4 KB and a
  small parser state. Unchanged -> skipped. Grown with the same head -> parsed from the stored byte offset
  (append-only JSONL). Rewritten/shrunk/canonical changed -> full re-parse of that session. A truncated last
  line is not consumed (`partial_tail=1`) and is picked up when the file grows. Malformed lines are counted
  (`files.n_bad`, `sessions.parse_bad`) and skipped. `tools/selftest.py` checks that a file parsed in two
  appends yields exactly the rows of one full parse.
* **Resumable.** Parsed sessions are committed every few seconds; a session's aggregates are recomputed
  whenever `sessions.msgs_n IS NULL` (left by an interrupted run), and FTS is built in committed `mid`
  batches (`meta.fts_max_mid`). Re-running `sx ingest` after an interruption continues where it stopped.
* **Parallel.** Parsing runs in a process pool (default `cpu-2`, max 16), largest files first; one writer
  process owns SQLite. Codex lines are pre-classified with a header regex so heavy duplicate records
  (`world_state`, `token_usage_record`, deltas, `item_started`...) are counted without JSON parsing.
* **Stored text.** Human, prompt, assistant, reasoning-summary and compaction texts are stored in full.
  Tool calls store name + arguments (capped at 256 KB). Tool outputs keep the first 2.5 KB + last 1.5 KB
  (cap 4 KB) and injected boilerplate is capped at 4 KB; `text_len` always holds the original length and
  `file_id + offset` locate the exact source line (`sx raw`). Encrypted reasoning blobs are never stored.
* **Idempotent metadata.** Per-root metadata (aliases, links, facts, typed-prompt history, Codex state-DB turn
  failures) is rebuilt per adapter as one unit over all of that adapter's roots (`source='home:<root_id>'`),
  and skipped when no metadata file of those roots changed size/mtime. Transcript-derived rows use
  `source='transcript'` and are replaced when their session is re-parsed; `source='derived'` events
  (token_pressure) are recomputed at finalize. Alias `current=1` marks the latest-timestamped name per
  (session, origin), e.g. the last `session_index.jsonl` line across all CODEX_HOMEs.

## Schema (summary; `sx guide --section schema` is authoritative)

| table | purpose |
|---|---|
| `sessions` | one row per logical session: provider, pid, kind, origin_class, client/source, account, cwd, derived project, git, models, times, counts, title |
| `messages` | ordered messages with `kind`, `role`, text (+caps), tool linkage, model, origin hint, turn, raw locator, imported/inherited/sidechain flags, ctx_tokens |
| `events` | structured signals (interrupt, tool_error, tool_denied, api_error, rate_limit, compaction, model_switch, subagent_spawn, fork, rollback, hooks, turn_duration, ...) |
| `aliases` | every name with origin (manual/auto) |
| `links` | subagent_of, forked_from, parent_thread, continued_in, imported_from (cross-provider) |
| `facts` | provider metadata key/values (Codex state threads, goals, PR links, cost) |
| `files` | every physical file incl. copies/hardlinks, parse offsets, errors |
| `prompt_history` | typed-prompt logs from `~/.claude/history.jsonl` and each `CODEX_HOME/history.jsonl` |
| `messages_fts` | FTS5 trigram index over view `fts_src` (prompts/assistant/reasoning full; tool_call first 512 chars; tool_output full if is_error else first 256 + last 512; injected kinds not indexed) |
| `ann.annotations` | separate `annotations.sqlite`: agent-written tags/notes; ingest never touches it |

`sid` = provider letter (`c` Claude, `x` Codex, `o` ses_* transcripts) + 9 base32 chars of
`sha1(provider:pid)`: short, stable across rebuilds, prefix-matchable. Provider ids (≥6 chars) also resolve.

## Classification: human vs injected (`messages.kind`)

The user role carries much more than what a person typed. Rules are ordered (first match wins); see
`sessionindex/classify.py`.

Claude Code (`type:"user"` records, text blocks; attachments):
0. Text blocks that contain `<system-reminder>` and would classify as a prompt or reminder are split first;
   each remaining chunk is classified on its own (so a reminder prepended to a typed prompt does not hide it).
1. `isCompactSummary` or "This session is being continued..." -> `compaction_summary`
2. `[Request interrupted by user...]` -> `interrupt_marker` (+ `interrupt` event)
3. `<command-name>`/`<command-message>` -> `human_command` (`/name args`) unless `isMeta` (-> `command_expansion`)
4. `<local-command-stdout|stderr|caveat>`, `<bash-stdout|stderr>`, local-command caveat -> `command_output`
5. `<bash-input>` -> `human_shell`
6. `<task-notification>`/`<subagent_notification>` or `origin.kind=task-notification` -> `task_notification`
7. `<system-reminder>` -> `system_reminder` (reminders embedded after human text are split off)
8. `<codex_internal_context source="goal"`, `[SYSTEM DIRECTIVE:` -> `auto_continuation`;
   `<agentryx_inbox_injection>`, `[AGENTRYX TEAM CHANNEL MESSAGE BATCH]` -> `relayed_prompt`;
   `[search-mode]`, `[analyze-mode]`, other `<codex_internal_context` -> `command_expansion`
9. `origin.kind=peer|coordinator` -> `peer_message`; `auto-continuation` -> `auto_continuation` (checked before
   isMeta: these records are also isMeta)
10. `isMeta` -> `command_expansion` (skill bodies) or `meta`
11. Known injection tags (`<environment_context>`, AGENTS.md, `<user_instructions>`, plugin/app/skill lists...) -> `context_injection`
12. Subagent/sidechain transcript -> `agent_prompt`
13. `origin.kind=human` -> `human`; other unknown origin kinds -> `meta`
14. `promptSource=system` without a more specific marker -> `meta`; `promptSource=sdk` without human origin,
    or `sdk-*` entrypoint -> `sdk_prompt`
15. otherwise -> `human`
Attachments `queued_command` with human origin (typed while the model was busy) -> `human`, dropped
when the identical text was also delivered as a user record. `tool_result` blocks -> `tool_output`.
Hook attachments with content (SessionStart hook stdout, `hook_additional_context`, `hook_system_message`)
-> `hook_output`.

Codex (`response_item` message role=user, plus `event_msg:user_message` / `item_completed:UserMessage`):
`<turn_aborted>` -> `interrupt_marker`; `<user_shell_command>` -> `human_shell`; notifications -> `task_notification`;
`<skill>` -> `command_expansion`; goal/relay/mode prefixes as above; `<environment_context>`, `# AGENTS.md instructions`,
`<user_instructions>`, `<recommended_plugins>`, `<codex_delegation>`, `<app-context>`... -> `context_injection`;
subagent thread (`session_meta.source.subagent`, `thread_source=subagent`, agent role/nickname) -> `agent_prompt`;
non-interactive source (`exec`, `mcp`, custom/internal) -> `sdk_prompt`; otherwise `human`.
Developer-role messages -> `developer`. The same message recorded as `response_item`, `event_msg` and
`item_completed` is stored once: per content hash, a copy is kept only when its source has seen more copies
than were already kept (so a genuinely repeated prompt is kept twice); reasoning summaries are paired part by
part (events carry one part each). The `origin` column says which copy was kept.

Session `origin_class`: `interactive` (Claude cli/desktop/vscode, Codex cli/vscode/Desktop), `noninteractive`
(Claude `sdk-*` entrypoints, Codex exec/mcp), `subagent`, `imported` (only copied content), `unknown`.

Known limits:
* Prompts sent through non-interactive clients are `sdk_prompt`: they may be human-written or program-generated.
* Pasted text is part of the human message (Claude history shows it as `[Pasted text #n]`).
* Old Claude records without `origin`/`promptSource` rely on content prefixes only.
* An unknown new injection tag in the user role is classified `human` until a rule is added; check
  `sx sql "SELECT substr(text,1,40), COUNT(*) FROM messages WHERE kind='human' GROUP BY 1 ORDER BY 2 DESC LIMIT 30"`.
* Fork prefixes: messages of a fork/import child identical (content hash) to messages of its parent, up to the
  first non-matching prompt/assistant/tool message, are marked `inherited=1` and excluded from counts. If the
  parent was never ingested, the copied prefix is counted.

Measured accuracy: see "Verification" below.

## Events (problem signals)

Only signals present in the logs are emitted; `sx guide --section events` defines each type and its source.
Counters on `sessions` use transcript events only; Codex state-DB turn statuses are kept as
`state_turn_failed|interrupted` events with `source='home:<root>'`.

## Query CLI

JSON Lines by default (`--format table` for humans, allowed before or after the command). Defaults are
token-lean (compact fields, clipped text with explicit lengths, small limits); opt in with `--full`,
`--fields`, `--limit`.

| command | purpose |
|---|---|
| `guide [--section ...]` | orientation: schema, kinds, events, alias origins, examples, caveats |
| `stats --by summary|provider|account|project|month|origin_class|client|kind|files|msg_kind|event` | counts |
| `sessions [filters] [--sort last|started|human_chars|human|length|active|msgs|errors|tool_errors|interrupts|compactions|ctx]` | list |
| `show <id>` | metadata, aliases, links, children, event counts + one line per prompt turn |
| `read <id> [--seq a-b] [--turn a-b] [--kinds ...] [--no-tools] [--full]` | messages |
| `raw <id> --seq N` / `--event EID` | original JSON line from the source file |
| `search "<fts>" [filters] [--human] [--kinds] [--phrase] [--like]` | trigram FTS with snippets |
| `sql "<SELECT ...>"` | read-only SQL (mode=ro + query_only; one statement; SELECT/WITH/EXPLAIN/VALUES only) |
| `note <id> [--seq N] --tag T --note "..."` / `notes [id] [--tag]` | annotations |
| `ingest [--source X] [--rebuild] [--max-jobs N]` | build/update the index |

Filters (sessions/search/stats): `--provider --project --cwd --alias --since --until --min-human
--min-human-chars --has-event --origin-class --client --account --subagents exclude|include|only
--include-test --sid`.

## Adding a source (e.g. Orca metadata)

Add a module in `sessionindex/adapters/` exposing `PROVIDER`, `discover(root)`, `parse(job)`,
`probe_last_ts(path)`, `home_metadata(root)` (optional `home_files(root)` for change detection); register it
in `adapters/__init__.py`; add a root entry to `sources.json`. Metadata-only adapters can return nothing from
`discover` and write `aliases`/`links`/`facts`/`events` keyed by `(provider, provider session id)`.
Join keys available for extra per-session link tables: `sessions.pid` (Claude sessionId, Codex thread id),
`sessions.cwd_key` (lower-cased normalized cwd; indexed), `sessions.project_root` (derived main repo for
git worktrees) and `files.path`. New tables can be added with `CREATE TABLE IF NOT EXISTS` in `schema.py`
without touching existing ones; changing an existing table needs a `SCHEMA_VERSION` bump, after which
ingest refuses to run until `sx ingest --rebuild` (annotations live in a separate DB and survive).

## Verification (2026-09-27)

* `python tools/selftest.py`: synthetic classification cases, bad-line/partial-tail tolerance, and on real
  files per adapter a two-step append parse that must equal one full parse (all PASS).
* `python tools/spotcheck.py --sid ...`: independent raw reader vs index on 20 sessions (10 Claude: interactive,
  subagent, compaction, interrupt, parent-with-children; 10 Codex: interactive, subagent, compaction,
  interrupt, exec): 0 mismatches after fixes (Claude human 554/554, Codex typed prompts 94/94, tool calls,
  interrupts, compactions, titles, first/last timestamps).
* `python tools/validate_human.py` on the full parse (before the queued-notification fix): typed-prompt
  recall as human 96.4% (Claude, 4,093 logged prompts) and 91.2% (Codex, 7,366); unmatched typed prompts are
  local commands or entries absent from every copy of the transcript (403-sample: 0 found only in a
  non-canonical copy). Share of human messages found in typed logs: Codex TUI 97.7%; Claude CLI 66% before /
  ~94% expected after removing 1,634 queued `<task-notification>` attachments that had been counted as human.
  Desktop apps do not write typed-prompt logs, so their share is not a precision measure.
