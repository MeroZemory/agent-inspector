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
    "fork": "Codex session_meta.forked_from_id present (this thread is a fork).",
    "resume": "Codex session_meta for the same thread repeated later in the file.",
    "continued_in": "Claude continued-in record (session continued in another session id).",
    "rollback": "Codex thread_rolled_back (user undid N turns). value=num_turns.",
    "approval_request": "Codex asked for approval (exec/apply_patch/request_permissions).",
    "user_input_request": "Codex requested user input / elicitation.",
    "guardian_assessment": "Codex guardian (auto-approval reviewer) assessment.",
    "permission_mode": "Claude permission mode set/changed (default, plan, acceptEdits, bypassPermissions, auto).",
    "hook_error": "Claude stop-hook summary with hook errors.",
    "hook_blocked": "Claude hook prevented continuation.",
    "hook_blocking_error": "Claude hook returned a blocking error (attachment).",
    "hook_timeout": "Claude hook cancelled after timing out. value=durationMs.",
    "hook_cancelled": "Claude hook cancelled (not a timeout).",
    "goal_status": "Goal condition/status recorded (Claude goal_status attachment; Codex thread_goal_updated).",
    "external_file_edit": "Claude noticed a file edited outside the model (by the user or a tool). detail=filename.",
    "turn_duration": "Duration of a model turn in ms (Claude system turn_duration; Codex task_complete.duration_ms). value=ms.",
    "warning": "Codex warning event.",
    "agents_killed": "Claude background agents killed.",
    "state_turn_failed": "(source home:*) Codex state DB thread_turns status=failed; detail=error_json. Not counted in sessions.errors.",
    "state_turn_interrupted": "(source home:*) Codex state DB thread_turns status=interrupted. Overlaps transcript 'interrupt'; not counted in sessions.interrupts.",
}

ALIAS_ORIGINS = {
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
    "first_human_prompt": "auto (first prompt text, 200 chars)",
}

SCHEMA_DOCS = {
    "sessions": [
        ("sid", "short stable id (c=claude, x=codex, o=opencode) = hash(provider:pid); prefix match allowed"),
        ("provider/pid", "provider and provider session id (Claude sessionId or '<parent>:agent-<id>' for subagents; Codex thread id)"),
        ("kind", "main | subagent | workflow_agent (Claude workflow subagent)"),
        ("origin_class", "interactive | noninteractive (claude sdk-*/codex exec,mcp) | subagent | imported | unknown"),
        ("client/source", "Claude entrypoint (cli, claude-desktop, sdk-cli...) / Codex originator + SessionSource"),
        ("parent_sid", "parent session for subagents (may not exist in index)"),
        ("account/root_id", "source root label of the canonical file (e.g. codex-home, orca-acct-xxxx)"),
        ("cwd/project/project_root/project_method", "project is DERIVED from cwd (git root, git-worktree->main repo, pattern, cwd)"),
        ("started_at/last_activity_at", "UTC ISO; min/max record timestamps"),
        ("duration_s/active_s", "wall span / sum of inter-message gaps <= 30 min (derived)"),
        ("human_msgs/human_chars", "kinds human+human_command+human_shell, excluding imported/inherited/sidechain"),
        ("assistant_msgs/assistant_chars/tool_calls", "counts excluding imported/inherited"),
        ("tool_errors/errors/interrupts/compactions/subagent_spawns/denials", "counts of transcript events (see events)"),
        ("max_ctx_tokens/ctx_window", "max context tokens seen (Claude input+cache tokens; Codex last_token_usage.total_tokens)"),
        ("title/title_origin", "best alias: manual > auto title > first prompt"),
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
    "events": [("sid,seq,ts,type,detail,value", "problem/process signals; seq = nearest message; source=transcript or home:<root>")],
    "aliases": [("sid,alias,origin,manual,ts,source", "all names; manual=1 user-set, 0 auto, NULL unknown")],
    "links": [("src_sid,rel,dst_sid,dst_provider,dst_pid,provenance", "rel: subagent_of | forked_from | parent_thread | continued_in | imported_from")],
    "facts": [("sid,key,value,origin", "provider metadata (pr_url, cost_usd, state.*, goal, ...)")],
    "files": [("path,root_id,pkey,sid,role,relation,size,mtime_ns,dev,ino,parsed_offset,n_lines,n_bad,partial_tail",
               "every physical file; role canonical|hardlink|copy|missing; relation for copies same_extent|older_extent")],
    "prompt_history": [("sid,provider,pid,ts,text,project", "typed-prompt logs (~/.claude/history.jsonl, CODEX_HOME/history.jsonl): ground truth for human typing")],
    "ann.annotations": [("id,sid,seq,tag,note,author,created_at", "agent-written notes (separate DB, never touched by ingest)")],
    "messages_fts": [("text", "FTS5 trigram index over view fts_src: full text for prompts/assistant/reasoning; tool_call first 1024 chars; tool_output first 512 + last 1024 chars; developer/context_injection/system_reminder/meta/command_expansion not indexed (use --like or sql LIKE)")],
}

EXAMPLES = """\
sx stats --by month                              # corpus shape
sx sessions --provider codex --min-human 5 --sort human_chars --limit 10
sx sessions --project Agentryx --since 2026-09-01 --has-event interrupt
sx show <sid>                                    # metadata + one line per human turn
sx read <sid> --kinds human,assistant --limit 40 # conversation without tool noise
sx read <sid> --turn 3-5 --full                  # everything in turns 3..5, untruncated
sx raw <sid> --seq 12                            # original JSON line from the source file
sx search '"다시 해"' --human --limit 20         # Korean phrase in human messages (>=3 chars)
sx search 'rate limit' --kinds tool_output,api_error
sx sql "SELECT type, COUNT(*) n FROM events GROUP BY type ORDER BY n DESC"
sx sql "SELECT s.project, AVG(s.interrupts) FROM sessions s WHERE s.origin_class='interactive' GROUP BY 1"
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
- Counts exclude messages marked imported/inherited (fork prefix copies, Codex->Claude imports).
- events from source home:* (Codex state DBs) duplicate some transcript signals and are excluded from session counters.
- Orca metadata adapter is not implemented yet (Orca-run sessions are indexed via their Codex/Claude homes).
"""


def build(sections, live: dict) -> str:
    out = []
    if "overview" in sections:
        out.append("# sx - session index guide")
        out.append(f"db={live.get('db')} last_ingest={live.get('last_ingest')} sessions={live.get('sessions')} "
                   f"messages={live.get('messages')} events={live.get('events')}")
        out.append("Output is JSON Lines by default (--format table for humans). IDs: sid prefix or provider id prefix.")
        out.append("Commands: guide stats sessions show read raw search sql note notes ingest  (sx <cmd> -h for flags)")
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
    if "examples" in sections:
        out.append("\n## examples\n" + EXAMPLES)
    if "caveats" in sections:
        out.append("## caveats\n" + CAVEATS)
    return "\n".join(out)
