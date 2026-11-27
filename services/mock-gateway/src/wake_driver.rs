//! AO-3 wake driver: pending agent events → unattended loop Runs.
//!
//! The driver rides the sweeper 10 s tick (`drive_wake_events`,
//! called from `spawn_assignment_sweeper` right after
//! `drive_due_timers`, so a just-fired timer event is claimed
//! in the same pass — no second scheduler, D1 red line).
//! One pass: reset stale claims → list pending (oldest first)
//! → stop at the daily cap → atomically claim each event
//! (reusing `claim_agent_event`, whose per-session gate is
//! the single-Run rule: one Run per session at a time, the
//! rest queue for a later tick) → spawn one background task
//! per claimed event. The tick never blocks: the task waits
//! for its loop, then writes back.
//!
//! A wake Run is an existing loop session: a new background
//! loop (`start_background_with_memory`) with the event's
//! `session_id`, so AO-4 memory loads the session's
//! instruction / notes / progress layers (AO3-J4 — the
//! driver never resumes a user's live loop object: loop
//! objects are ephemeral, restart turns them Interrupted
//! with no LiveLoop to resume, and a per-loop background
//! flag would leak into a later foreground resume).
//! The Run ends with a memory writeback (project notes
//! layer) + a `local_notification` Outbox row + the event
//! marked done, all in ONE transaction — or the event
//! marked failed with no Outbox row and no retry (C4).
//!
//! Budgets (Q-AO-12): daily wake cap 48 (counted from
//! `claimed_ms`, which is never cleared — the cap counts
//! starts, not outcomes); per-run token cap 100k, enforced
//! on the AO-4 estimator in the loop step (see
//! `loop_engine`, AO3-J5 — the loop never sees API usage
//! data). Over the daily cap the events stay pending for
//! tomorrow and one Inbox `failed` row per day is written
//! (`wake-budget-YYYY-MM-DD`); a per-run token breach
//! fails the event and writes its own Inbox row
//! (`wake-token-{event_id}`).
//!
//! First end-to-end source: AO-2 timers only (AO3-J6).
//! `file.changed` needs NS-1, which is unapproved and
//! unimplemented, so this batch proves only the timer
//! chain: event → wake → Run → memory writeback → Outbox
//! row → Dispatcher delivery.

use std::time::Duration;

use crate::loop_engine::{ErrorCode, LoopStatus, StartLoop};
use crate::memory_store::{MemoryRow, MemorySource};
use crate::server::AppState;
use crate::sqlite_store::{self, AgentEventRow};

/// Q-AO-12: at most 48 wake Runs start per UTC day.
pub const WAKE_DAILY_CAP: usize = 48;

/// Q-AO-12: at most 100k estimated tokens per wake Run
/// (local definition — the loop never sees API usage
/// data, so the cap is enforced on the AO-4 estimator;
/// see `loop_engine`).
pub const WAKE_RUN_TOKEN_CAP: usize = 100_000;

/// A claimed event older than this never had a live Run:
/// the longest possible Run is the loop 120 s budget +
/// one final step, so 10 min cannot strand a live Run.
pub const WAKE_STALE_CLAIM_MS: i64 = 600_000;

/// The waiter gives a Run 5 min to reach a terminal
/// state (2.5x the 120 s loop budget) before failing
/// the event; the loop itself keeps running under its
/// own driver and its Execution still closes.
pub const WAKE_WAIT_TIMEOUT_MS: u64 = 300_000;

/// Poll interval for the waiter.
const WAKE_WAIT_POLL_MS: u64 = 250;

/// The Run's final text is truncated to 500 chars in
/// the memory writeback (the 4k memory budget loads
/// notes as index-only anyway; the note stays readable).
const WAKE_FINAL_TEXT_TRUNC_CHARS: usize = 500;

/// The notification summary carries 200 chars of the
/// Run's result.
const WAKE_NOTIFICATION_TEXT_TRUNC_CHARS: usize = 200;

/// Q-AO-09: the unattended whitelist — read + search
/// only. The three denied tools are exactly the three
/// that require approval (`loop_tool_requires_approval`),
/// so a background loop can never park on
/// AwaitingApproval. The gate itself lives in
/// `loop_engine` (research-gate pattern); this is the
/// membership list it keys on.
pub fn background_tool_allowed(tool: &str) -> bool {
    matches!(
        tool,
        "fs.list"
            | "fs.read"
            | "reminders.list"
            | "email.threads"
            | "email.message"
            | "imessage.threads"
            | "contacts.search"
            | "location.current"
            | "code.search"
            | "git.read"
            | "worker.wait"
    )
}

/// UTC calendar date (YYYY-MM-DD) for a unix-ms
/// timestamp. No chrono in this crate: days-since-epoch
/// → civil date (Howard Hinnant's algorithm).
/// Unit-tested against Python-computed dates below.
fn utc_date_string(now_ms: i64) -> String {
    let days = now_ms.div_euclid(86_400_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// UTC midnight (ms) starting the day `now_ms` falls in.
/// The Dispatcher counts its daily cap on the same
/// boundary (E2 test arithmetic).
fn utc_day_start_ms(now_ms: i64) -> i64 {
    now_ms - now_ms.rem_euclid(86_400_000)
}

/// The wake prompt, built deterministically from the
/// event summary only — never raw content (red line 7;
/// the summary is already summary-only by construction
/// at ingest, and every audit payload this Run writes
/// passes the `redact_event_payload` choke point inside
/// `append_event`). The "read-only" line guides the
/// model; the code gate in `loop_engine` enforces it.
fn wake_prompt(event: &AgentEventRow) -> String {
    let summary: serde_json::Value =
        serde_json::from_str(&event.payload_summary_json).unwrap_or(serde_json::Value::Null);
    let timer_id = summary
        .get("timer_id")
        .and_then(|v| v.as_str())
        .unwrap_or("?");
    let kind = summary
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or(event.source_kind.as_str());
    format!(
        "[定时器唤醒] 定时器 {timer_id}（{kind}）触发，事件 {}。摘要：{}。请按摘要完成跟进：只读、只查，不写文件、不改提醒、不执行命令；完成后用一句话总结结果。",
        event.event_id, event.payload_summary_json
    )
}

/// The memory writeback content: a deterministic note
/// in the project layer (AO3-J3 — a wake outcome is a
/// note, so future loops see only its index line under
/// AO-4's notes rule).
fn wake_memory_content(event: &AgentEventRow, final_text: &str) -> String {
    let result: String = final_text
        .chars()
        .take(WAKE_FINAL_TEXT_TRUNC_CHARS)
        .collect();
    let result = if result.trim().is_empty() {
        "(无结果)".to_string()
    } else {
        result
    };
    format!(
        "唤醒记录：事件 {}（{}，{}）已完成。结果：{}",
        event.event_id, event.source_kind, event.source_event_id, result
    )
}

/// The sweeper entry: claim pending events and spawn
/// their Runs. Synchronous and non-blocking — the Runs
/// live in spawned tasks. Returns the Runs spawned.
pub fn drive_wake_events(state: &AppState) -> usize {
    use crate::sqlite_store::StateBackend;
    if state.sqlite_mode() != StateBackend::Sqlite || state.sqlite_is_broken() {
        return 0;
    }
    let Some(sqlite) = &state.sqlite else {
        return 0;
    };
    let now = sqlite_store::now_ms() as i64;
    // Stale-claim recovery first: a claimed event whose
    // Run never finished (gateway died mid-Run) returns
    // to pending so this same pass can drive it.
    {
        let mut conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::reset_stale_claimed_agent_events(
            &mut conn,
            now - WAKE_STALE_CLAIM_MS,
        ) {
            Ok(reset) => {
                if reset > 0 {
                    eprintln!("AO-3 wake: reset {reset} stale claimed event(s) to pending");
                }
            }
            Err(detail) => {
                eprintln!("warning: AO-3 wake stale-claim reset failed: {detail}");
                return 0;
            }
        }
    }
    let pending = {
        let conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::list_pending_agent_events(&conn, WAKE_DAILY_CAP as i64) {
            Ok(rows) => rows,
            Err(detail) => {
                eprintln!("warning: AO-3 wake pending list failed: {detail}");
                return 0;
            }
        }
    };
    if pending.is_empty() {
        return 0;
    }
    // Daily cap (Q-AO-12): wakes started today.
    let day_start = utc_day_start_ms(now);
    let used = {
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::count_claimed_agent_events_since(&conn, day_start).unwrap_or(0)
    };
    let mut remaining = WAKE_DAILY_CAP as i64 - used;
    if remaining <= 0 {
        // Over the cap: stop, leave every pending event
        // pending for tomorrow, and write the one Inbox
        // row for the day (deduped — one row per day).
        let mut conn = sqlite.lock().expect("sqlite lock");
        if let Err(detail) =
            sqlite_store::insert_inbox_failed(&mut conn, &format!("wake-budget-{}", utc_date_string(now)))
        {
            eprintln!("warning: AO-3 wake daily-cap inbox row failed: {detail}");
        }
        return 0;
    }
    let mut spawned = 0usize;
    for event in pending {
        if remaining <= 0 {
            break;
        }
        let claimed = {
            let mut conn = sqlite.lock().expect("sqlite lock");
            sqlite_store::claim_agent_event(&mut conn, &event.event_id, now)
        };
        let claimed = match claimed {
            Ok(claimed) => claimed,
            Err(detail) => {
                eprintln!(
                    "warning: AO-3 wake claim failed for {}: {detail}",
                    event.event_id
                );
                continue;
            }
        };
        if !claimed {
            // Session busy (single-Run gate) or another
            // driver took it — stays pending for a later tick.
            continue;
        }
        remaining -= 1;
        spawned += 1;
        let app = state.clone();
        tokio::spawn(async move {
            run_wake_task(app, event).await;
        });
    }
    spawned
}

enum WakeOutcome {
    Completed { final_text: String },
    AwaitingApproval,
    Failed {
        error: Option<ErrorCode>,
        estimated_tokens: usize,
    },
    WaitTimeout,
    UnknownLoop,
}

/// Wait for a wake loop to reach a terminal state.
/// AwaitingApproval is reported, not waited on: a
/// background loop cannot park there by construction,
/// and waiting would hang the task on a user decision.
async fn wait_for_loop(state: &AppState, loop_id: &str) -> WakeOutcome {
    let deadline = std::time::Instant::now() + Duration::from_millis(WAKE_WAIT_TIMEOUT_MS);
    loop {
        match state.loops.get(loop_id) {
            Ok(view) => {
                let status = &view.record.status;
                if status.terminal() {
                    return match status {
                        LoopStatus::Completed => WakeOutcome::Completed {
                            final_text: view.final_text.clone().unwrap_or_default(),
                        },
                        _ => WakeOutcome::Failed {
                            error: view.record.error.clone(),
                            estimated_tokens: view.record.estimated_tokens,
                        },
                    };
                }
                if *status == LoopStatus::AwaitingApproval {
                    return WakeOutcome::AwaitingApproval;
                }
            }
            Err(_) => return WakeOutcome::UnknownLoop,
        }
        if std::time::Instant::now() >= deadline {
            return WakeOutcome::WaitTimeout;
        }
        tokio::time::sleep(Duration::from_millis(WAKE_WAIT_POLL_MS)).await;
    }
}

/// One claimed event's Run: start the background loop,
/// wait for it, then write back or fail the event.
async fn run_wake_task(state: AppState, event: AgentEventRow) {
    let session_id = event.session_id.clone();
    // AO-4: memory is advisory context, loaded before
    // the loop starts. Any load failure means "no
    // memory", never a failed start.
    let memory = crate::memory_context::load_loop_memory(&state, session_id.as_deref());
    let request = StartLoop {
        prompt: wake_prompt(&event),
        max_calls: None,
        budget_ms: None,
        worktree_id: None,
        session_id: session_id.clone(),
        goal_id: None,
    };
    let view = match state.loops.start_background_with_memory(request, memory) {
        Ok(view) => view,
        Err(error) => {
            // No loop started (e.g. model not configured
            // → 503): the event fails, no Outbox row, no
            // retry (C4).
            eprintln!(
                "AO-3 wake: loop start failed for {}: {} {}",
                event.event_id, error.0, error.1
            );
            fail_wake_event(&state, &event.event_id);
            return;
        }
    };
    let loop_id = view.record.loop_id.clone();
    // P0 phase 1: the loop attempt gets its Execution
    // shell record, like `loop_start` — the driver
    // closes it when the loop terminates, and the
    // Outbox row names it.
    let execution = crate::server::create_execution_with_binding(
        &state,
        None,
        Some(loop_id.clone()),
        None,
        session_id,
        None,
    );
    crate::routes_loop::spawn_driver(&state, &loop_id);
    match wait_for_loop(&state, &loop_id).await {
        WakeOutcome::Completed { final_text } => {
            match finish_wake_success(&state, &event, &execution.execution_id, &final_text) {
                Ok(()) => {}
                Err(detail) => {
                    // The writeback transaction failed, so
                    // nothing landed: leave the event claimed
                    // rather than failing it — the
                    // stale-claim recovery re-drives it and
                    // the outcome stays exactly-once.
                    eprintln!(
                        "AO-3 wake: writeback failed for {}, leaving claimed for recovery: {detail}",
                        event.event_id
                    );
                }
            }
        }
        WakeOutcome::AwaitingApproval => {
            // Unreachable by construction (the background
            // gate refuses every approval-requiring tool),
            // but if a loop ever parks here the Run cannot
            // finish unattended — fail the event rather
            // than hang on a user decision.
            eprintln!(
                "AO-3 wake: loop {loop_id} parked on approval for {}",
                event.event_id
            );
            fail_wake_event(&state, &event.event_id);
        }
        WakeOutcome::Failed {
            error,
            estimated_tokens,
        } => {
            eprintln!(
                "AO-3 wake: loop {loop_id} failed for {}: {error:?}",
                event.event_id
            );
            if error == Some(ErrorCode::BudgetExceeded)
                && estimated_tokens > WAKE_RUN_TOKEN_CAP
            {
                // Per-run token cap breach: its own Inbox
                // row, so the user sees the missed wake.
                if let Some(sqlite) = &state.sqlite {
                    let mut conn = sqlite.lock().expect("sqlite lock");
                    let _ = sqlite_store::insert_inbox_failed(
                        &mut conn,
                        &format!("wake-token-{}", event.event_id),
                    );
                }
            }
            fail_wake_event(&state, &event.event_id);
        }
        WakeOutcome::WaitTimeout => {
            eprintln!(
                "AO-3 wake: loop {loop_id} did not finish in time for {}",
                event.event_id
            );
            fail_wake_event(&state, &event.event_id);
        }
        WakeOutcome::UnknownLoop => {
            eprintln!("AO-3 wake: loop {loop_id} vanished for {}", event.event_id);
            fail_wake_event(&state, &event.event_id);
        }
    }
}

/// Mark a claimed event failed: no Outbox row, no
/// memory writeback, no auto-retry (honest rules).
fn fail_wake_event(state: &AppState, event_id: &str) {
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) =
        sqlite_store::fail_agent_event(&mut conn, event_id, &state.origin_device_id)
    {
        eprintln!("warning: AO-3 wake fail event {event_id} failed: {detail}");
    }
}

/// The success path, in ONE transaction: memory
/// writeback + Outbox row + event done. The Outbox
/// row names the wake Execution (so `execution_id` is
/// never a placeholder) and carries a per-event topic
/// (`wake:{event_id}`), so two wake notifications never
/// merge into one and swallow each other's content —
/// the merge key is per event by construction.
fn finish_wake_success(
    state: &AppState,
    event: &AgentEventRow,
    execution_id: &str,
    final_text: &str,
) -> Result<(), String> {
    let Some(sqlite) = &state.sqlite else {
        return Err("no sqlite configured".to_string());
    };
    let now = sqlite_store::now_ms() as i64;
    // The memory id mints from the same in-memory
    // counter the save handler uses (one minting
    // authority in production; the counter re-syncs
    // from the DB at startup import).
    let memory_id = {
        let mut next = state.next_memory_id.lock().expect("memory id lock");
        let id = format!("memory-{}", *next);
        *next += 1;
        id
    };
    let source = MemorySource {
        session_id: event.session_id.clone(),
        execution_id: Some(execution_id.to_string()),
        source_id: Some(event.event_id.clone()),
        author: "wake".to_string(),
        created_ms: now as u64,
    };
    let row = MemoryRow {
        memory_id,
        layer: "project".to_string(),
        scope_json: "{}".to_string(),
        content: wake_memory_content(event, final_text),
        source_json: source.canonical(),
        created_ms: now as u64,
        updated_ms: now as u64,
        deleted: false,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    };
    let result: String = final_text
        .chars()
        .take(WAKE_NOTIFICATION_TEXT_TRUNC_CHARS)
        .collect();
    let summary = serde_json::json!({
        "session_id": event.session_id,
        "topic": format!("wake:{}", event.event_id),
        "kind": "wake",
        "event_id": event.event_id,
        "result": result,
    });
    let summary_json = serde_json::to_string(&summary).map_err(|e| e.to_string())?;
    let mut conn = sqlite.lock().expect("sqlite lock");
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    crate::memory_store::insert_memory_tx(&tx, &row).map_err(|e| e.to_string())?;
    let (_outbox, fresh) = sqlite_store::enqueue_outbox_kind_tx(
        &tx,
        execution_id,
        &summary_json,
        "local_notification",
        &format!("{}:local_notification", event.event_id),
        None,
    )?;
    if !fresh {
        // Unreachable in practice (one claim → one task →
        // one transaction), but a duplicate dedup key must
        // not fail the writeback: the row already exists.
        eprintln!(
            "AO-3 wake: outbox row already existed for {}",
            event.event_id
        );
    }
    sqlite_store::complete_agent_event_tx(&tx, &event.event_id)?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Expected dates computed independently in Python
    /// (datetime.fromtimestamp, UTC) before this test
    /// was written — not copied from the function's
    /// output.
    #[test]
    fn utc_date_string_matches_python() {
        assert_eq!(utc_date_string(0), "1970-01-01");
        assert_eq!(utc_date_string(1_000), "1970-01-01");
        assert_eq!(utc_date_string(86_400_000), "1970-01-02");
        assert_eq!(utc_date_string(-1), "1969-12-31");
        assert_eq!(utc_date_string(1_800_000_000_000), "2027-01-15");
        assert_eq!(utc_date_string(1_794_704_400_000), "2026-11-15");
        assert_eq!(utc_date_string(1_764_000_000_000), "2025-11-24");
    }

    #[test]
    fn utc_day_start_is_utc_midnight() {
        // 1_800_000_000_000 % 86_400_000 = 28_800_000
        // (08:00 UTC), computed in Python.
        assert_eq!(utc_day_start_ms(1_800_000_000_000), 1_799_971_200_000);
        assert_eq!(utc_day_start_ms(86_400_000), 86_400_000);
        assert_eq!(utc_day_start_ms(86_400_001), 86_400_000);
    }

    #[test]
    fn background_whitelist_is_read_and_search_only() {
        for tool in [
            "fs.list",
            "fs.read",
            "reminders.list",
            "email.threads",
            "email.message",
            "imessage.threads",
            "contacts.search",
            "location.current",
            "code.search",
            "git.read",
            "worker.wait",
        ] {
            assert!(background_tool_allowed(tool), "{tool} must be allowed");
        }
        for tool in [
            "fs.write",
            "reminders.set_completed",
            "worker.run",
            "invalid_tool",
            "shell.exec",
        ] {
            assert!(!background_tool_allowed(tool), "{tool} must be denied");
        }
    }

    #[test]
    fn wake_prompt_names_timer_and_summary() {
        let event = AgentEventRow {
            event_id: "agent-event-1".to_string(),
            seq: 1,
            source_kind: "timers".to_string(),
            source_event_id: "timer:timer-1:1000".to_string(),
            payload_summary_json:
                r#"{"kind":"follow_up","note":"remind","timer_id":"timer-1"}"#.to_string(),
            session_id: Some("session-1".to_string()),
            status: "claimed".to_string(),
            created_ms: 1000,
            claimed_ms: Some(1000),
            delivered_ms: None,
        };
        let prompt = wake_prompt(&event);
        assert!(prompt.contains("timer-1"), "{prompt}");
        assert!(prompt.contains("follow_up"), "{prompt}");
        assert!(prompt.contains("agent-event-1"), "{prompt}");
        assert!(prompt.contains("remind"), "{prompt}");
    }
}
