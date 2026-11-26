//! ORC-5 (Q-R5-01/02): the Swarm object + message
//! board.
//!
//! One `swarms` row per fan-out: the task + plan it
//! belongs to, its status word, and the
//! steps-to-runs mapping as canonical JSON
//! (`steps_json`). `swarm_messages` is the message
//! board (Q-ORC-17): `body_hash` only — the board
//! text is NEVER stored (red line 7), and board rows
//! never enter the events table (the board is
//! coordination state, not an audit log). Neither
//! table has a JSONL export (the `plans` precedent —
//! SQLite-only, no `*.jsonl` file).
//!
//! Swarm statuses (LOCAL DEFINITION, plan §6.3):
//! `created` (idle, fanout not yet called) →
//! `fanning_out` (copy add-runs running) → `running`
//! (step preparations + approvals + executions) →
//! `verifying` (all step executions terminal,
//! cross-check running) → `merging` (diffs applied
//! to the main worktree) → `completed` / `failed` /
//! `conflict` (terminal). The task stays `draft_ready`
//! through the whole swarm — there is NO
//! `draft_ready → running` edge in the task state
//! machine (D1 red line 1), so the swarm's own status
//! carries progress.
//!
//! Local ids are `swarm-N` / `swarm-msg-N`, assigned
//! from max-suffix + 1 inside the write transaction
//! (the `plan-N` / `sent-N` precedent).

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::sqlite_store::{append_event, canonical_json, id_suffix, now_ms};

fn err(detail: impl std::fmt::Display) -> String {
    format!("sqlite: {detail}")
}

const SWARM_COLS: &str =
    "swarm_id, task_id, plan_id, status, steps_json, created_ms, updated_ms, origin_device_id";
const SWARM_MESSAGE_COLS: &str =
    "message_id, swarm_id, sender_worker_id, kind, body_hash, created_ms, origin_device_id";

/// One fanned-out step's lifecycle (LOCAL
/// DEFINITION, plan §6.3). `run_status` is one of
/// `pending` (copy not ready) / `preparing` (approval
/// being built) / `waiting_approval` (approval
/// pending) / `running` (run queued or executing) /
/// `succeeded` / `failed`. `approval_kind` is
/// `worker.run` (command step) or `fs.write`
/// (file-write step). `content_base64` /
/// `content_sha256` exist only for file-write steps:
/// the Executor-model-generated content, stored in
/// SQLite only — never in events or Evidence rows
/// (only the sha256 + length leave the swarm).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmStep {
    pub step_no: i64,
    pub worker_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_kind: Option<String>,
    pub run_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect_id: Option<String>,
    /// The step's terminal failure, when it failed
    /// (prepare / execute / run). Persisted so the
    /// aggregating tick can hand it to `fail_step`
    /// (Q-R5-10: the swarm aggregates all run terminals
    /// before one `fail_step` / one hand-back).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
}

impl SwarmStep {
    pub fn new(step_no: i64, worker_id: String) -> Self {
        Self {
            step_no,
            worker_id,
            worktree_id: None,
            add_run_id: None,
            run_id: None,
            approval_id: None,
            approval_kind: None,
            run_status: "pending".to_string(),
            effect_id: None,
            failure: None,
            content_base64: None,
            content_sha256: None,
        }
    }
}

/// One swarm row (LOCAL DEFINITION, plan §6.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmRow {
    pub swarm_id: String,
    pub task_id: String,
    pub plan_id: String,
    pub status: String,
    pub steps_json: String,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub origin_device_id: String,
}

impl SwarmRow {
    pub fn steps(&self) -> Result<Vec<SwarmStep>, String> {
        serde_json::from_str(&self.steps_json).map_err(|e| format!("swarm steps decode: {e}"))
    }
}

/// One message-board row (LOCAL DEFINITION, plan
/// §6.2). `kind` is one of `status` / `result` /
/// `conflict` (the draft's `question` kind is cut —
/// Q-R5-08). `body_hash` is the sha256 of the board
/// text; the text itself is never persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmMessageRow {
    pub message_id: String,
    pub swarm_id: String,
    pub sender_worker_id: String,
    pub kind: String,
    pub body_hash: String,
    pub created_ms: i64,
    pub origin_device_id: String,
}

/// One step in the swarm view (plan §6.3): the
/// `SwarmStep` minus its SQLite-only fields
/// (`content_base64` / `content_sha256` never leave
/// the gateway) plus the approval's live status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmStepView {
    pub step_no: i64,
    pub worker_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worktree_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    pub run_status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_status: Option<String>,
}

/// One board row in the swarm view (plan §6.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmMessageView {
    pub message_id: String,
    pub sender_worker_id: String,
    pub kind: String,
    pub body_hash: String,
    pub created_ms: i64,
}

/// The swarm view (plan §6.3): the swarm row +
/// step views + the latest board rows +
/// worktree records still present after the swarm
/// ended (merge cleanup leftovers).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwarmView {
    pub swarm_id: String,
    pub task_id: String,
    pub plan_id: String,
    pub status: String,
    pub steps: Vec<SwarmStepView>,
    pub messages: Vec<SwarmMessageView>,
    pub leftover_worktrees: Vec<String>,
    pub created_ms: i64,
    pub updated_ms: i64,
}

/// Swarm status transitions (LOCAL DEFINITION). The
/// drive walks created → fanning_out → running →
/// verifying → merging → completed, with `failed` /
/// `conflict` as terminal exits from any in-flight
/// state. Terminal states have no outgoing edge.
pub fn valid_swarm_status_transition(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        ("created", "fanning_out")
            | ("created", "failed")
            | ("fanning_out", "running")
            | ("fanning_out", "failed")
            | ("fanning_out", "conflict")
            | ("running", "verifying")
            | ("running", "failed")
            | ("running", "conflict")
            | ("verifying", "merging")
            | ("verifying", "failed")
            | ("verifying", "conflict")
            | ("merging", "completed")
            | ("merging", "failed")
            | ("merging", "conflict")
    )
}

fn swarm_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SwarmRow> {
    Ok(SwarmRow {
        swarm_id: row.get(0)?,
        task_id: row.get(1)?,
        plan_id: row.get(2)?,
        status: row.get(3)?,
        steps_json: row.get(4)?,
        created_ms: row.get(5)?,
        updated_ms: row.get(6)?,
        origin_device_id: row.get(7)?,
    })
}

fn swarm_message_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SwarmMessageRow> {
    Ok(SwarmMessageRow {
        message_id: row.get(0)?,
        swarm_id: row.get(1)?,
        sender_worker_id: row.get(2)?,
        kind: row.get(3)?,
        body_hash: row.get(4)?,
        created_ms: row.get(5)?,
        origin_device_id: row.get(6)?,
    })
}

fn next_swarm_number_tx(tx: &Transaction) -> Result<u64, String> {
    let mut stmt = tx.prepare("SELECT swarm_id FROM swarms").map_err(err)?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    let mut next: u64 = 1;
    for id in ids {
        let suffix = id_suffix(&id);
        if suffix >= 0 {
            next = next.max((suffix as u64) + 1);
        }
    }
    Ok(next)
}

fn next_swarm_message_number_tx(tx: &Transaction) -> Result<u64, String> {
    let mut stmt = tx
        .prepare("SELECT message_id FROM swarm_messages")
        .map_err(err)?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    let mut next: u64 = 1;
    for id in ids {
        let suffix = id_suffix(&id);
        if suffix >= 0 {
            next = next.max((suffix as u64) + 1);
        }
    }
    Ok(next)
}

/// The swarm event payload: ids + counts only, never
/// step text or board text (red line 7 — the payload
/// passes through `redact_event_payload` anyway, but
/// it must not need it).
fn swarm_event_payload(row: &SwarmRow, step_count: usize) -> String {
    canonical_json(&serde_json::json!({
        "swarm_id": row.swarm_id,
        "task_id": row.task_id,
        "plan_id": row.plan_id,
        "status": row.status,
        "step_count": step_count,
    }))
}

pub fn create_swarm(
    conn: &mut Connection,
    task_id: &str,
    plan_id: &str,
    steps: &[SwarmStep],
    origin_device_id: &str,
) -> Result<SwarmRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let next = next_swarm_number_tx(&tx)?;
    let now = now_ms() as i64;
    let steps_json = canonical_json(
        &serde_json::to_value(steps).map_err(|e| format!("swarm steps encode: {e}"))?,
    );
    let row = SwarmRow {
        swarm_id: format!("swarm-{next}"),
        task_id: task_id.to_string(),
        plan_id: plan_id.to_string(),
        status: "created".to_string(),
        steps_json,
        created_ms: now,
        updated_ms: now,
        origin_device_id: origin_device_id.to_string(),
    };
    tx.execute(
        &format!(
            "INSERT INTO swarms ({SWARM_COLS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"
        ),
        params![
            row.swarm_id,
            row.task_id,
            row.plan_id,
            row.status,
            row.steps_json,
            row.created_ms,
            row.updated_ms,
            row.origin_device_id,
        ],
    )
    .map_err(err)?;
    append_event(
        &tx,
        "swarm",
        &row.swarm_id,
        "swarm.created",
        &swarm_event_payload(&row, steps.len()),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(row)
}

pub fn get_swarm(conn: &Connection, swarm_id: &str) -> Result<Option<SwarmRow>, String> {
    conn.query_row(
        &format!("SELECT {SWARM_COLS} FROM swarms WHERE swarm_id = ?1"),
        params![swarm_id],
        swarm_from_row,
    )
    .optional()
    .map_err(err)
}

/// The task's active swarm, if any: the newest row
/// whose status is not terminal. A task has at most
/// one active swarm — `POST /swarm/create` and
/// `tasks_execute` both refuse while one exists.
pub fn active_swarm_for_task(
    conn: &Connection,
    task_id: &str,
) -> Result<Option<SwarmRow>, String> {
    conn.query_row(
        &format!(
            "SELECT {SWARM_COLS} FROM swarms WHERE task_id = ?1 AND status NOT IN ('completed', 'failed', 'conflict') ORDER BY created_ms DESC, swarm_id DESC LIMIT 1"
        ),
        params![task_id],
        swarm_from_row,
    )
    .optional()
    .map_err(err)
}

/// All swarms, newest first. The drive scans these
/// once per tick (the table is tiny: one row per
/// fan-out, and completed rows stay as audit).
pub fn list_swarms(conn: &Connection) -> Result<Vec<SwarmRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {SWARM_COLS} FROM swarms ORDER BY created_ms DESC, swarm_id DESC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map([], swarm_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// Rewrite a swarm's steps + status in one
/// transaction. A terminal status also appends the
/// matching `swarm.<status>` event (ids + counts
/// only). Intermediate statuses emit no event — they
/// are progress, not audit.
pub fn update_swarm(
    conn: &mut Connection,
    swarm_id: &str,
    steps: &[SwarmStep],
    status: &str,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    let current = get_swarm(&tx, swarm_id)?.ok_or_else(|| "unknown swarm".to_string())?;
    if current.status != status && !valid_swarm_status_transition(&current.status, status) {
        return Err(format!(
            "swarm status transition invalid: {} -> {}",
            current.status, status
        ));
    }
    let now = now_ms() as i64;
    let steps_json = canonical_json(
        &serde_json::to_value(steps).map_err(|e| format!("swarm steps encode: {e}"))?,
    );
    tx.execute(
        "UPDATE swarms SET steps_json = ?1, status = ?2, updated_ms = ?3 WHERE swarm_id = ?4",
        params![steps_json, status, now, swarm_id],
    )
    .map_err(err)?;
    if matches!(status, "completed" | "failed" | "conflict") {
        let row = SwarmRow {
            status: status.to_string(),
            steps_json,
            updated_ms: now,
            ..current
        };
        append_event(
            &tx,
            "swarm",
            swarm_id,
            &format!("swarm.{status}"),
            &swarm_event_payload(&row, steps.len()),
            &row.origin_device_id,
            now,
            1,
        )?;
    }
    tx.commit().map_err(err)?;
    Ok(())
}

/// Append one board row. The body text is NEVER
/// passed in — only its sha256 (`body_hash`). No
/// event is written (red line 7: board rows never
/// enter the events table).
pub fn insert_swarm_message(
    conn: &mut Connection,
    swarm_id: &str,
    sender_worker_id: &str,
    kind: &str,
    body_hash: &str,
    origin_device_id: &str,
) -> Result<SwarmMessageRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let next = next_swarm_message_number_tx(&tx)?;
    let now = now_ms() as i64;
    let row = SwarmMessageRow {
        message_id: format!("swarm-msg-{next}"),
        swarm_id: swarm_id.to_string(),
        sender_worker_id: sender_worker_id.to_string(),
        kind: kind.to_string(),
        body_hash: body_hash.to_string(),
        created_ms: now,
        origin_device_id: origin_device_id.to_string(),
    };
    tx.execute(
        &format!(
            "INSERT INTO swarm_messages ({SWARM_MESSAGE_COLS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
        ),
        params![
            row.message_id,
            row.swarm_id,
            row.sender_worker_id,
            row.kind,
            row.body_hash,
            row.created_ms,
            row.origin_device_id,
        ],
    )
    .map_err(err)?;
    tx.commit().map_err(err)?;
    Ok(row)
}

/// The latest board rows, chronological. `limit` is
/// capped by the route (default 50, max 200 —
/// Q-R5-08, no pagination yet).
pub fn list_swarm_messages(
    conn: &Connection,
    swarm_id: &str,
    limit: i64,
) -> Result<Vec<SwarmMessageRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {SWARM_MESSAGE_COLS} FROM swarm_messages WHERE swarm_id = ?1 ORDER BY created_ms DESC, message_id DESC LIMIT ?2"
        ))
        .map_err(err)?;
    let mut rows = stmt
        .query_map(params![swarm_id, limit], swarm_message_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    rows.reverse();
    Ok(rows)
}
