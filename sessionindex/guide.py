"""Text for `sx guide`: the single place an agent orients itself. Kept compact on purpose."""
from __future__ import annotations

from .classify import KIND_DOCS

EVENT_DOCS = {
    "interrupt": "User interrupted the model (Claude '[Request interrupted by user...]' marker or aborted stream; Codex turn_aborted reason=interrupted). detail: response|tool_use|reason.",
    "turn_aborted": "Codex turn aborted for a reason other than a user interrupt (e.g. replaced, review_ended).",
    "tool_error": "Tool result reported failure (Claude tool_result is_error; Codex 'Exit code: N!=0'/'Script failed'/failed item/patch failure).",
    "tool_aborted": "Codex tool output 'aborted by user after Ns'.",
    "tool_denied": "Tool use rejected/denied (Claude permission rejection text or toolDenialKind; Codex declined command/'rejected by user').",
    "api_error": "API error (Claude isApiErrorMessage/error; Codex event error without limit wording).",
    "rate_limit": "Rate/usage limit hit (Claude error=rate_limit or HTTP 429; Codex error text mentions usage/rate limit/429/quota).",
    "api_retry": "Claude system api_error record (client retrying the API call).",
    "stream_retry": "Codex stream_error (reconnect/retry).",
    "turn_error": "Codex task_complete carried an error.",
    "compaction": "Context compaction (Claude compact_boundary: detail=trigger auto|manual, value=preTokens; Codex compacted/context_compacted, deduped per turn).",
    "model_switch": "Model changed within the session (Claude assistant message.model change; Codex turn_context.model change or model_reroute).",
    "model_fallback": "Claude model_refusal_fallback (refusal led to a fallback model).",
    "subagent_spawn": "Agent spawned a subagent (Claude Task/Agent tool_use; Codex spawn_agent call). detail: type: description.",
    "fork": "Codex session_meta.forked_from_id present (this thread is a fork; most Codex subagents fork their parent); Claude fork-context-ref record (subagent got parent context).",
    "resume": "Session resumed: Codex session_meta of the same thread repeated later in the file; Claude SessionStart:resume hook record (only present when a SessionStart hook is configured).",
    "session_start": "Claude SessionStart:startup hook record (process start; only with a SessionStart hook configured).",
    "context_clear": "Claude SessionStart:clear hook record (/clear).",
    "relocated": "Claude relocated record: the session's cwd was moved. detail=new cwd.",
    "max_turns": "Claude max_turns_reached attachment. detail=turnCount/maxTurns.",
    "token_pressure": "(source derived) first message of a turn whose context reached >=90% of the reported window (Codex token_count.model_context_window). value=fraction. Claude transcripts do not record the window: use compaction (detail auto) and sessions.max_ctx_tokens.",
    "continued_in": "Claude continued-in record (session continued in another session id).",
    "rollback": "Codex thread_rolled_back (user undid N turns). value=num_turns.",
    "approval_request": "Codex asked for approval (exec/apply_patch/request_permissions).",
    "user_input_request": "Codex requested user input / elicitation.",
    "guardian_assessment": "Codex guardian (auto-approval reviewer) assessment.",
    "permission_mode": "Claude permission mode set/changed (default, plan, acceptEdits, bypassPermissions, auto).",
    "hook_error": "Claude hook failed: stop-hook summary hookErrors, or hook_non_blocking_error (exit code + stderr head).",
    "hook_blocked": "Claude hook prevented continuation.",
    "hook_blocking_error": "Claude hook returned a blocking error (attachment).",
    "hook_timeout": "Claude hook cancelled after timing out. value=durationMs.",
    "hook_cancelled": "Claude hook cancelled (not a timeout).",
    "goal_status": "Goal condition/status recorded (Claude goal_status attachment; Codex thread_goal_updated).",
    "external_file_edit": "Claude noticed a file edited outside the model (by the user or a tool). detail=filename.",
    "turn_duration": "Duration of a model turn in ms (Claude system turn_duration; Codex task_complete.duration_ms). value=ms.",
    "warning": "Codex warning event.",
    "turn_start": "(timing, Codex) task_started. detail=turn id. Used by `sx timing`; excluded from sessions.events_n.",
    "turn_end": "(timing, Codex) task_complete | turn_aborted | next task_started. value=duration_ms; detail='complete|aborted:<reason>|superseded out=<model outputs in the turn> err=<task_complete error 0|1>'.",
    "tool_span": "(timing, Codex) one tool call: ts=call, value=seconds until its output; detail='<kind> <call_id>', kind build|shell|wait|subagent|web|edit|other (see `sx guide --section timing`).",
    "compaction_span": "(timing, Codex) ContextCompaction item: ts=started_at_ms, value=seconds.",
    "model_step": "(timing, Codex) model input (turn start / tool output) to the next tool call or assistant message, or to the turn end when no tool call of the turn is open. ts=input; value=seconds; detail='<tool_call|assistant|turn_end|turn_end_error> err=<error+stream_error events inside>'.",
    "agents_killed": "Claude background agents killed.",
    "state_turn_failed": "(source home:*) Codex state DB thread_turns status=failed; detail='[turn <turn_id>] <error_json>' (turn id keeps distinct turns distinct across home copies). Not counted in sessions.errors.",
    "state_turn_interrupted": "(source home:*) Codex state DB thread_turns status=interrupted; detail='[turn <turn_id>]'. Overlaps transcript 'interrupt'; not counted in sessions.interrupts.",
}

ALIAS_ORIGINS = {
    "(all)": "aliases.current=1 marks the latest-timestamped name per (session, origin): e.g. the last session_index.jsonl line across all CODEX_HOMEs; older names stay with current=0",
    "claude_custom_title": "manual (/rename or custom title) - wins",
    "codex_state_name": "manual (Codex thread rename, state_5.sqlite threads.name) - wins",
    "codex_thread_name_event": "Codex thread_name_updated event (manual/auto unknown)",
    "claude_ai_title": "auto (Claude ai-title)",
    "codex_session_index": "auto (CODEX_HOME/session_index.jsonl thread_name; last entry wins)",
    "codex_state_title": "auto (state_5.sqlite threads.title)",
    "claude_summary": "auto (legacy Claude summary record)",
    "claude_agent_name": "Claude agent-name record (fallback)",
    "claude_subagent_description": "Subagent task description from .meta.json",
    "codex_agent_nickname": "Codex subagent nickname",
    "orca_tab_title": "Orca tab title (customTitle/label/generated; manual vs automatic not recorded)",
    "orca_ai_vault_title": "Orca aiVaultTitle for the tab's session (auto)",
    "orca_worktree_name": "Orca worktree/folder display name",
    "first_human_prompt": "auto (first prompt text, 200 chars)",
}

SCHEMA_DOCS = {
    "sessions": [
        ("sid", "short stable id (c=claude, x=codex, o=opencode, g=agy) = hash(provider:pid); prefix match allowed"),
        ("provider/pid", "provider and provider session id (Claude sessionId or '<parent>:agent-<id>' for subagents; Codex thread id; agy conversation uuid)"),
        ("kind", "main | subagent | workflow_agent (Claude workflow subagent)"),
        ("origin_class", "interactive | noninteractive (claude sdk-*/codex exec,mcp) | subagent | imported | unknown"),
        ("client/source", "Claude entrypoint (cli, claude-desktop, sdk-cli...) / Codex originator + SessionSource"),
        ("parent_sid", "parent session for subagents (may not exist in index)"),
        ("account/root_id", "source root label of the canonical file (e.g. codex-home, orca-acct-xxxx)"),
        ("cwd/project/project_root/project_method", "project is DERIVED from cwd (git root, git-worktree->main repo, pattern, cwd)"),
        ("cwd_key", "lower-cased normalized cwd (forward slashes): join key for external per-cwd/worktree metadata"),
        ("started_at/last_activity_at", "UTC ISO; min/max record timestamps"),
        ("duration_s/active_s", "wall span / sum of inter-message gaps <= 30 min (derived)"),
        ("human_msgs/human_chars", "kinds human+human_command+human_shell, excluding imported/inherited/sidechain"),
        ("assistant_msgs/assistant_chars/tool_calls", "counts excluding imported/inherited"),
        ("tool_errors/errors/interrupts/compactions/subagent_spawns/denials", "counts of transcript events (see events)"),
        ("max_ctx_tokens/ctx_window", "max context tokens seen (Claude input+cache tokens; Codex last_token_usage.total_tokens)"),
        ("title/title_origin", "best alias: manual > auto title > Orca tab/worktree name > first prompt"),
        ("body", "0 = catalog row only (run sx ingest --sid), 1 = body ingested, 2 = body stale (file changed)"),
        ("hist_prompts/size_bytes", "typed prompts logged in history.jsonl for this session / canonical file size (catalog)"),
        ("first_prompt", "first prompt text (300 chars; catalog reads it from the file head)"),
        ("n_files/n_copies", "physical files for this session / distinct non-canonical content copies"),
        ("inherited_msgs", "messages copied from a fork/import parent or marked imported"),
        ("rec_counts", "JSON counts of raw record types (discover unmapped signals here)"),
        ("meta", "JSON of raw adapter metadata (agent_meta, history_mode, rate_max_primary_pct, ...)"),
    ],
    "messages": [
        ("mid", "global row id (FTS rowid)"), ("sid, seq", "session + monotonic order (gaps allowed)"),
        ("ts", "UTC ISO"), ("role", "user|assistant|tool|developer|system|agent"),
        ("kind", "classification, see KINDS"), ("text", "full for prompts/assistant/reasoning; capped for tool/injected"),
        ("text_len/truncated", "original length in chars / 1 if text was capped"),
        ("tool_name/tool_id/is_error", "tool call/result linkage"), ("model", "model for assistant rows"),
        ("origin", "raw origin hint (Claude origin.kind/promptSource, Codex response_item|event|item, phase)"),
        ("turn", "count of prompt-kind messages so far (human/sdk/agent prompt)"),
        ("file_id/offset/line", "raw locator: files.path + byte offset of the source JSON line"),
        ("imported/inherited/sidechain", "1 = copied from another provider / copied from fork parent / inline sidechain"),
        ("ctx_tokens", "context tokens at this assistant message (see sessions.max_ctx_tokens)"),
    ],
    "events": [("sid,seq,ts,type,detail,value,file_id,offset", "problem/process signals; seq = nearest message; source=transcript | home:<root> (provider DBs) | derived (computed at ingest)")],
    "aliases": [("sid,alias,origin,manual,ts,source,current", "all names; manual=1 user-set, 0 auto, NULL unknown; current=1 latest per origin")],
    "links": [("src_sid,rel,dst_sid,dst_provider,dst_pid,provenance", "rel: subagent_of | forked_from | parent_thread | continued_in | imported_from")],
    "facts": [("sid,key,value,origin", "provider metadata (pr_url, cost_usd, state.*, goal, ...)")],
    "files": [("path,root_id,pkey,sid,role,relation,size,mtime_ns,dev,ino,last_ts,probe_lines,parsed_offset,n_lines,n_bad,partial_tail",
               "every physical file; role canonical|hardlink|copy|missing; relation for copies same_extent|older_extent|newer_extent")],
    "prompt_history": [("sid,provider,pid,ts,text,project", "typed-prompt logs (~/.claude/history.jsonl, CODEX_HOME/history.jsonl, ~/.gemini/antigravity-cli/history.jsonl): ground truth for human typing")],
    "orca": [("sid,kind,key,name,detail,ts,method",
              "Orca links: kind tab|pane|worktree|pty|orch_dispatch_contexts|orch_worker_dispatches|orch_worker_terminal_resources; "
              "method sessionId (direct) or cwd_time (same cwd + time overlap; may be ambiguous)")],
    "ann.annotations": [("id,sid,seq,tag,note,author,created_at", "agent-written notes (separate DB, never touched by ingest)")],
    "messages_fts": [("text", "FTS5 trigram index over view fts_src: full text for prompts/assistant/reasoning; tool_call first 512 chars; tool_output: stored text if is_error=1, else first 256 + last 512 chars; developer/context_injection/system_reminder/meta/command_expansion/hook_output not indexed (use --like or sql LIKE)")],
}

TIMING = """\
`sx timing [filters] [--by session|project] [--outage-min 20]` splits the IN-TURN wall time of Codex sessions
(task_started -> task_complete/turn_aborted; idle time waiting for the user is excluded) into parts that add up:
  tool       union of tool-call intervals (call -> output; parallel calls overlap, so the union is used)
  compaction ContextCompaction item durations not under a tool call
  outage     whole turns with no model output (no tool call, assistant message or reasoning: e.g. 429 loops whose
             turns end in an error) + model steps longer than --outage-min, containing a Codex `error`/`stream_error`
             event, or ending the turn with a task_complete error; minus tool and compaction
  model      the rest: in-turn time with no tool call outstanding = generation + network + proxy
Not in-turn and not in any percentage: error_idle_h = for each turn that ended with a task_complete error or without
model output, the gap to the next task_started of the session (goal sessions keep retrying the stream there without
writing to the rollout, e.g. 429 limit loops); lost_h = outage_h + error_idle_h. error_idle may include deliberate
user stops right after an errored turn; a session's last errored turn has no next start and adds nothing.
Columns: in_turn_h, model_pct, tool_pct, per tool kind *_pct (build = shell running npm/vite/vitest/playwright/
pytest/unittest/cargo/dotnet/ffmpeg/blender/render/tsc/python script...; shell = other exec_command/write_stdin;
wait = wait/sleep/multi-agent wait, i.e. background cells and subagents; subagent = other multi_agent_v1__* control;
web = web__run; edit = apply_patch; other), compaction_pct, outage_h/pct, model_steps + step_median_s/p90_s
(non-outage model steps: input -> first model output), turns, outage_turns, outage_windows (outage intervals merged
across gaps < --outage-min; table shows first/last). --since/--until select sessions (as elsewhere) AND clip the
window (accepts 2026-10-07T15:00Z). Defaults: --provider codex, subagents excluded (--subagents include).
Caveats: model time includes network/proxy latency and client-side streaming; outages are inferred (threshold +
error events + output-less turns), so a genuinely slow >threshold model step counts as outage; background waits
count as tool time (kind wait), not model; per-kind shares overlap; a turn still open at the last record ends there.
Codex only: Claude transcripts have tool_use/tool_result timestamps and turn_duration, but interrupted
turns write no turn_duration and compaction has no duration, so turn boundaries would be guessed; not implemented.
Sessions parsed before timing support are re-parsed by a plain `sx ingest --sid ...` (adapter PARSER_VERSION).
"""

EXAMPLES = """\
sx catalog                                       # refresh the full session list (light; incremental)
sx sessions --body no --sort hist --limit 10     # busiest catalog-only sessions by typed prompts
sx ingest --sid xabc123,cdef456                  # parse only the bodies you picked
sx stats --by month                              # corpus shape
sx ingest --project Agentryx --since 2026-09-20   # index only what you need (also --sid/--file/--source/--until)
sx sessions --provider codex --min-human 5 --sort human_chars --limit 10
sx sessions --project Agentryx --since 2026-09-01 --has-event interrupt
sx show <sid>                                    # metadata + one line per human turn
sx read <sid> --kinds human,assistant --limit 40 # conversation without tool noise
sx read <sid> --turn 3-5 --full                  # everything in turns 3..5, untruncated
sx raw <sid> --seq 12                            # original JSON line from the source file
sx search '"다시 해"' --human --limit 20         # Korean phrase in human messages (>=3 chars)
sx search 'rate limit' --kinds tool_output,api_error
sx search 'ㅋㅋ|헐|이게 뭐' --regex --human --project maplestory   # regex (case-insensitive) over full text, human only
sx sessions --provider agy --project maplestory --subagents include   # Antigravity CLI (agy) incl. Teamwork subagents
sx timing --since 2026-10-07T15:00Z --by session --format table   # Codex in-turn time: model / tool kinds / outage
sx timing --project QAAI --by project --outage-min 10
sx sql "SELECT type, COUNT(*) n FROM events GROUP BY type ORDER BY n DESC"
sx sql "SELECT s.project, AVG(s.interrupts) FROM sessions s WHERE s.origin_class='interactive' GROUP BY 1"
sx sessions --has-event rate_limit,token_pressure --sort last   # problem-signal filter
sx sql "WITH t AS (SELECT sid, seq, kind, text, LAG(kind) OVER (PARTITION BY sid ORDER BY seq) pk FROM messages WHERE kind IN ('human','interrupt_marker')) SELECT sid, seq, substr(text,1,120) t FROM t WHERE kind='human' AND pk='interrupt_marker' LIMIT 20"   # what people typed right after interrupting
sx note <sid> --seq 12 --tag correction --note "user re-asks same fix"   # write
sx notes --tag correction                                                # read
"""

CAVEATS = """\
- Timestamps are UTC (user is in KST = UTC+9).
- Human-vs-injected is heuristic (README 'Classification'); prefer origin_class='interactive' and kinds human*.
  Codex exec / claude -p prompts are kind sdk_prompt (could be human or program-written).
- Codex sessions exist as many physical copies (hardlinks and rewritten copies across CODEX_HOMEs);
  only the canonical copy (latest last timestamp) is parsed; others are listed in `files`.
- Rewritten Codex copies can have collapsed timestamps (all records = thread start); canonical selection avoids them
  when a better copy exists.
- Tool outputs are capped (default 4 KB), tool args 16 KB, injected text 4 KB; text_len keeps the original length;
  `sx raw` returns the full source record.
- Trigram FTS needs >= 3 characters per term; shorter queries fall back to LIKE (slow). Quote phrases: '"..."'.
  `--regex` (or `text REGEXP 'pat'` in sx sql) scans full stored text with a Python regex.
- agy (Antigravity CLI): files are SQLite DBs; messages.offset = step index and `sx raw` prints the decoded step.
  A running agy conversation grows its -wal, not the .db: re-ingest it with `sx ingest --sid <sid> --reparse`.
  agy human-vs-scripted (`agy -p`) is inferred from history.jsonl + Temp workspaces (README).
- Counts exclude messages marked imported/inherited (fork prefix copies, Codex->Claude imports).
- events from source home:* (Codex state DBs) duplicate some transcript signals and are excluded from session counters.
- Canonical copy = latest last-record timestamp, then more lines, then lowest root rank (codex-home=0).
- Tool call arguments are stored up to 256 KB; `sx raw` has the rest.
- Orca metadata adapter is not implemented yet (Orca-run sessions are indexed via their Codex/Claude homes);
  external per-session metadata can join on sessions.pid (Claude sessionId / Codex thread id) or sessions.cwd_key.
"""


def build(sections, live: dict) -> str:
    out = []
    if "overview" in sections:
        out.append("# sx - session index guide")
        out.append(f"db={live.get('db')} last_ingest={live.get('last_ingest')} sessions={live.get('sessions')} "
                   f"messages={live.get('messages')} events={live.get('events')}")
        out.append("Output is JSON Lines by default (--format table for humans). IDs: sid prefix or provider id prefix.")
        out.append("Commands: guide stats sessions show read raw search sql timing note notes ingest  (sx <cmd> -h for flags)")
        out.append("Workflow: 1) `sx catalog` (if the catalog is missing/old; lists EVERY session from file heads/tails + "
                   "provider/Orca metadata, no bodies)  2) `sx sessions [filters]` / `sx stats` / `sx show <sid>` on the "
                   "catalog (body=0 rows have titles, cwd, times, sizes, typed-prompt counts)  3) `sx ingest --sid a,b` "
                   "(or --project P --since D) for the sessions you selected  4) `sx show <sid>` (one line per human "
                   "turn) -> `sx read <sid> --turn a-b` -> `sx raw <sid> --seq N` -> `sx search` (ingested bodies only). "
                   "Record findings with `sx note`.")
    if "schema" in sections:
        out.append("\n## schema (tables: columns - meaning)")
        for t, cols in SCHEMA_DOCS.items():
            out.append(f"[{t}]")
            for c, d in cols:
                out.append(f"  {c}: {d}")
    if "kinds" in sections:
        out.append("\n## message kinds (human = what a person typed)")
        for k, d in KIND_DOCS.items():
            out.append(f"  {k}: {d}")
    if "events" in sections:
        out.append("\n## event types")
        for k, d in EVENT_DOCS.items():
            out.append(f"  {k}: {d}")
    if "aliases" in sections:
        out.append("\n## alias origins")
        for k, d in ALIAS_ORIGINS.items():
            out.append(f"  {k}: {d}")
    if "timing" in sections:
        out.append("\n## timing (sx timing)\n" + TIMING)
    if "examples" in sections:
        out.append("\n## examples\n" + EXAMPLES)
    if "caveats" in sections:
        out.append("## caveats\n" + CAVEATS)
    return "\n".join(out)
