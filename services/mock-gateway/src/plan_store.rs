//! ORC-1 (Q-ORC-03): the Plan first-class object.
//!
//! One `plans` row per plan version. The task's current
//! plan is the highest `version` row; supersede flips
//! the old row's `status`, never deletes it (red line
//! 9). `steps_json` / `replan_if_json` are canonical
//! JSON arrays (see `canonical_json`): the blueprint
//! body text lives only here — `plan.created` /
//! `plan.superseded` events carry counts + digest only
//! (Q-ORC-04). `execution_id` is NULL at draft time
//! (no Execution exists yet); `planner_model` is the
//! model string of the planning round, a fact record.
//! There is NO approval flow in ORC-1: the digest
//! column + `plan_digest` helper exist so a later batch
//! can bind a plan approval to them (plan §3); this
//! batch invents no approve route and no approval tool.
//!
//! Local ids are `plan-N`, assigned from max-suffix +
//! 1 inside the write transaction (the `sent-N`
//! precedent in `mail_send.rs`).

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::sqlite_store::{append_event, canonical_json, id_suffix, now_ms};

fn err(detail: impl std::fmt::Display) -> String {
    format!("sqlite: {detail}")
}

const PLAN_COLS: &str = "plan_id, task_id, execution_id, version, status, steps_json, replan_if_json, planner_model, digest, created_ms, updated_ms, origin_device_id";

/// One blueprint step (LOCAL DEFINITION, plan §3).
/// The four-piece set (`path` / `symbol` / `change` /
/// `verify_cmd` / `expected`) is required (Q-ORC-04);
/// the route rejects a step missing any of them with
/// 400. `kind` is one of `file_write` / `command` /
/// `verify` / `review`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    pub step_no: i64,
    pub kind: String,
    pub path: String,
    pub symbol: String,
    pub change: String,
    pub verify_cmd: String,
    pub expected: String,
}

/// One Re-plan-if condition (LOCAL DEFINITION, plan
/// §3, Q-ORC-05). `signal` is free text; `action` has
/// exactly one value, `stop_and_hand_back` — the
/// execution model never improvises.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplanIf {
    pub signal: String,
    pub action: String,
}

/// One plan version row (LOCAL DEFINITION, plan §3).
/// `status` is one of `draft` / `approved` /
/// `superseded` / `rejected` (the plan's proposed
/// words); ORC-1 only ever writes `draft` and
/// `superseded`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRow {
    pub plan_id: String,
    pub task_id: String,
    pub execution_id: Option<String>,
    pub version: i64,
    pub status: String,
    pub steps_json: String,
    pub replan_if_json: String,
    pub planner_model: String,
    pub digest: String,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub origin_device_id: String,
}

impl PlanRow {
    /// The stored steps, parsed. Storage is canonical
    /// JSON, so a stored row always parses.
    pub fn steps(&self) -> Result<Vec<PlanStep>, String> {
        serde_json::from_str(&self.steps_json).map_err(|e| format!("plan steps parse: {e}"))
    }

    /// The stored Re-plan-if conditions, parsed.
    pub fn replan_if(&self) -> Result<Vec<ReplanIf>, String> {
        serde_json::from_str(&self.replan_if_json).map_err(|e| format!("plan replan_if parse: {e}"))
    }

    /// The row as the routes return it: `steps` and
    /// `replan_if` as arrays, not JSON strings.
    pub fn to_record(&self) -> Result<PlanRecord, String> {
        Ok(PlanRecord {
            plan_id: self.plan_id.clone(),
            task_id: self.task_id.clone(),
            execution_id: self.execution_id.clone(),
            version: self.version,
            status: self.status.clone(),
            steps: self.steps()?,
            replan_if: self.replan_if()?,
            planner_model: self.planner_model.clone(),
            digest: self.digest.clone(),
            created_ms: self.created_ms,
            updated_ms: self.updated_ms,
            origin_device_id: self.origin_device_id.clone(),
        })
    }
}

/// The plan as the routes return it (the `PlanRow`
/// shape with `steps` / `replan_if` decoded).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRecord {
    pub plan_id: String,
    pub task_id: String,
    pub execution_id: Option<String>,
    pub version: i64,
    pub status: String,
    pub steps: Vec<PlanStep>,
    pub replan_if: Vec<ReplanIf>,
    pub planner_model: String,
    pub digest: String,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub origin_device_id: String,
}

fn plan_from_row(row: &rusqlite::Row) -> rusqlite::Result<PlanRow> {
    Ok(PlanRow {
        plan_id: row.get(0)?,
        task_id: row.get(1)?,
        execution_id: row.get(2)?,
        version: row.get(3)?,
        status: row.get(4)?,
        steps_json: row.get(5)?,
        replan_if_json: row.get(6)?,
        planner_model: row.get(7)?,
        digest: row.get(8)?,
        created_ms: row.get(9)?,
        updated_ms: row.get(10)?,
        origin_device_id: row.get(11)?,
    })
}

fn next_plan_number_tx(tx: &Transaction) -> Result<u64, String> {
    let mut stmt = tx.prepare("SELECT plan_id FROM plans").map_err(err)?;
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

/// The task's current plan: the highest `version` row.
/// A newer draft always supersedes the old row in the
/// same transaction, so the max-version row is never a
/// superseded one.
pub fn current_plan_for_task(conn: &Connection, task_id: &str) -> Result<Option<PlanRow>, String> {
    conn.query_row(
        &format!(
            "SELECT {PLAN_COLS} FROM plans WHERE task_id = ?1 ORDER BY version DESC LIMIT 1"
        ),
        params![task_id],
        plan_from_row,
    )
    .optional()
    .map_err(err)
}

/// One plan version by local `plan-N` id.
pub fn get_plan(conn: &Connection, plan_id: &str) -> Result<Option<PlanRow>, String> {
    conn.query_row(
        &format!("SELECT {PLAN_COLS} FROM plans WHERE plan_id = ?1"),
        params![plan_id],
        plan_from_row,
    )
    .optional()
    .map_err(err)
}

/// Plans, newest first. With a `task_id` the rows are
/// that task's versions, highest version first;
/// without one they are all plans, newest created
/// first. `limit` is clamped by the caller (default
/// 50, max 200, the MCL list precedent).
pub fn list_plans(
    conn: &Connection,
    task_id: Option<&str>,
    limit: usize,
    offset: usize,
) -> Result<Vec<PlanRow>, String> {
    let (sql, params): (String, Vec<String>) = match task_id {
        Some(task_id) => (
            format!(
                "SELECT {PLAN_COLS} FROM plans WHERE task_id = ?1 ORDER BY version DESC LIMIT ?2 OFFSET ?3"
            ),
            vec![task_id.to_string()],
        ),
        None => (
            format!(
                "SELECT {PLAN_COLS} FROM plans ORDER BY created_ms DESC, rowid DESC LIMIT ?1 OFFSET ?2"
            ),
            vec![],
        ),
    };
    let mut stmt = conn.prepare(&sql).map_err(err)?;
    let rows = match task_id {
        Some(_) => stmt
            .query_map(params![params[0], limit as i64, offset as i64], plan_from_row)
            .map_err(err)?,
        None => stmt
            .query_map(params![limit as i64, offset as i64], plan_from_row)
            .map_err(err)?,
    };
    rows.collect::<Result<Vec<_>, _>>().map_err(err)
}

fn plan_event_payload(row: &PlanRow, step_count: usize) -> String {
    serde_json::json!({
        "plan_id": row.plan_id,
        "task_id": row.task_id,
        "version": row.version,
        "step_count": step_count,
        "digest": row.digest,
    })
    .to_string()
}

/// Draft a new plan version in a single transaction:
/// the new `plan-N` row + one `plan.created` event,
/// and — when the task already has a current plan —
/// the old row flipped to `superseded` + one
/// `plan.superseded` event in the SAME transaction
/// (Q-ORC-12). The steps / replan_if are stored as
/// canonical JSON, so the stored text is exactly what
/// the digest commits to.
pub fn create_plan(
    conn: &mut Connection,
    task_id: &str,
    steps: &[PlanStep],
    replan_if: &[ReplanIf],
    planner_model: &str,
    digest: &str,
    origin_device_id: &str,
) -> Result<PlanRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let next = next_plan_number_tx(&tx)?;
    let current = current_plan_for_task(&tx, task_id)?;
    let version = current.as_ref().map(|row| row.version + 1).unwrap_or(1);
    let now = now_ms() as i64;
    let steps_json = canonical_json(
        &serde_json::to_value(steps).map_err(|e| format!("plan steps encode: {e}"))?,
    );
    let replan_if_json = canonical_json(
        &serde_json::to_value(replan_if).map_err(|e| format!("plan replan_if encode: {e}"))?,
    );
    let row = PlanRow {
        plan_id: format!("plan-{next}"),
        task_id: task_id.to_string(),
        execution_id: None,
        version,
        status: "draft".to_string(),
        steps_json,
        replan_if_json,
        planner_model: planner_model.to_string(),
        digest: digest.to_string(),
        created_ms: now,
        updated_ms: now,
        origin_device_id: origin_device_id.to_string(),
    };
    if let Some(old) = &current {
        tx.execute(
            "UPDATE plans SET status = 'superseded', updated_ms = ?1 WHERE plan_id = ?2",
            params![now, old.plan_id],
        )
        .map_err(err)?;
        append_event(
            &tx,
            "plan",
            &old.plan_id,
            "plan.superseded",
            &plan_event_payload(old, old.steps().map(|s| s.len()).unwrap_or(0)),
            origin_device_id,
            now,
            1,
        )?;
    }
    tx.execute(
        &format!(
            "INSERT INTO plans ({PLAN_COLS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
        ),
        params![
            row.plan_id,
            row.task_id,
            row.execution_id,
            row.version,
            row.status,
            row.steps_json,
            row.replan_if_json,
            row.planner_model,
            row.digest,
            row.created_ms,
            row.updated_ms,
            row.origin_device_id,
        ],
    )
    .map_err(err)?;
    append_event(
        &tx,
        "plan",
        &row.plan_id,
        "plan.created",
        &plan_event_payload(&row, steps.len()),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(row)
}
