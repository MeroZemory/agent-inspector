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
tools/spotcheck.py           raw-vs-index spot checks (independent reader)
tools/validate_human.py      classification check against typed-prompt logs
data/                        (gitignored) index.sqlite (rebuildable) + annotations.sqlite (never touched by ingest)
```

## Ingest

```
python sx.py ingest                    # incremental; all roots in sources.json
python sx.py ingest --source codex     # adapter/provider/root id/label filter (comma list)
python sx.py ingest --rebuild          # delete index.sqlite and re-parse (annotations kept)
python sx.py ingest --max-jobs 300     # trial: parse an evenly sampled subset of sessions
```

* **Sources are opened read-only.** Transcripts are read with `open(..., 'rb')`; provider SQLite files with
  `mode=ro` URIs. Nothing under a source root is written, moved or deleted. Credential files are never
  opened (adapters only open `*.jsonl`, `*.meta.json`, the named metadata files, and `state_5`/`thread_history_1`/`goals_1` SQLite DBs).
* **Physical vs logical.** Every discovered file is a row in `files` (path, root, size, mtime, dev/ino, role).
  Files are grouped by logical session key (Claude `sessionId` or `<sessionId>:agent-<id>` for subagents;
  Codex thread id from the rollout file name). Hardlinks (same `(st_dev, st_ino)`) collapse to one content
  unit. When a session has several distinct content units (Codex homes copied across Orca accounts,
  archived vs active, migrated/rewritten copies), a cheap head/tail probe picks the **canonical** copy:
  latest last-record timestamp, then larger size, then lower root rank (`~/.codex` = 0). Only the canonical
  copy is parsed; the others stay in `files` with `role=hardlink|copy` and `relation=same_extent|older_extent`.
* **Incremental.** Each canonical file stores `parsed_offset`, size, mtime, a SHA-1 of its first 4 KB and a
  small parser state. Unchanged -> skipped. Grown with the same head -> parsed from the stored byte offset
  (append-only JSONL). Rewritten/shrunk/canonical changed -> full re-parse of that session. A truncated last
  line is not consumed (`partial_tail=1`) and is picked up when the file grows. Malformed lines are counted
  (`files.n_bad`, `sessions.parse_bad`) and skipped.
* **Parallel.** Parsing runs in a process pool (default `cpu-2`, max 16), largest files first; one writer
  process owns SQLite. Codex lines are pre-classified with a header regex so heavy duplicate records
  (`world_state`, `token_usage_record`, deltas, `item_started`...) are counted without JSON parsing.
* **Stored text.** Human, prompt, assistant, reasoning-summary and compaction texts are stored in full.
  Tool calls store name + arguments (capped at 16 KB). Tool outputs keep the first 2.5 KB + last 1.5 KB
  (cap 4 KB) and injected boilerplate is capped at 4 KB; `text_len` always holds the original length and
  `file_id + offset` locate the exact source line (`sx raw`). Encrypted reasoning blobs are never stored.
* **Idempotent metadata.** Per-root metadata (aliases, links, facts, typed-prompt history, Codex state-DB turn
  failures) is replaced wholesale per root on every run (`source='home:<root_id>'`); transcript-derived rows
  use `source='transcript'` and are replaced when their session is re-parsed.

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
| `messages_fts` | FTS5 trigram index over view `fts_src` |
| `ann.annotations` | separate `annotations.sqlite`: agent-written tags/notes; ingest never touches it |

`sid` = provider letter (`c` Claude, `x` Codex, `o` ses_* transcripts) + 9 base32 chars of
`sha1(provider:pid)`: short, stable across rebuilds, prefix-matchable. Provider ids (≥6 chars) also resolve.

## Classification: human vs injected (`messages.kind`)

The user role carries much more than what a person typed. Rules are ordered (first match wins); see
`sessionindex/classify.py`.

Claude Code (`type:"user"` records, text blocks; attachments):
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
9. `isMeta` -> `command_expansion` (skill bodies) or `meta`
10. `origin.kind=peer` -> `peer_message`; `auto-continuation` -> `auto_continuation`
11. Known injection tags (`<environment_context>`, AGENTS.md, `<user_instructions>`, plugin/app/skill lists...) -> `context_injection`
12. Subagent/sidechain transcript -> `agent_prompt`
13. `origin.kind=human` -> `human`; other unknown origin kinds -> `meta`
14. `promptSource=sdk` without human origin, or `sdk-*` entrypoint -> `sdk_prompt`
15. otherwise -> `human`
Attachments `queued_command` with human origin (typed while the model was busy) -> `human`, dropped
when the identical text was also delivered as a user record. `tool_result` blocks -> `tool_output`.

Codex (`response_item` message role=user, plus `event_msg:user_message` / `item_completed:UserMessage`):
`<turn_aborted>` -> `interrupt_marker`; `<user_shell_command>` -> `human_shell`; notifications -> `task_notification`;
`<skill>` -> `command_expansion`; goal/relay/mode prefixes as above; `<environment_context>`, `# AGENTS.md instructions`,
`<user_instructions>`, `<recommended_plugins>`, `<codex_delegation>`, `<app-context>`... -> `context_injection`;
subagent thread (`session_meta.source.subagent`, `thread_source=subagent`, agent role/nickname) -> `agent_prompt`;
non-interactive source (`exec`, `mcp`, custom/internal) -> `sdk_prompt`; otherwise `human`.
Developer-role messages -> `developer`. The same message recorded as `response_item`, `event_msg` and
`item_completed` is paired by content hash and stored once (the `origin` column says which copy was kept).

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
`probe_last_ts(path)`, `home_metadata(root)`; register it in `adapters/__init__.py`; add a root entry to
`sources.json`. Metadata-only adapters can return `None` from `discover` and write `aliases`/`links`/`facts`
keyed by provider session id (join keys: Claude sessionId, Codex thread id, cwd/worktree path). No schema
change is needed: `facts` is a generic key/value table.
