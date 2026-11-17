//! Ledger routes: /executions (3) + /worker/runs (4) +
//! /assignments (3) + /effects (5) + /evidence (3) +
//! /inbox (3) + /outbox (4) = 25.
//!
//! The handlers moved here from `server.rs` (FU-5 S-9g, the last
//! ledger batch). No `//!` directory lines move with this batch:
//! `server.rs` has zero `//! - ` lines (27 prose `//!` lines stay
//! there); the directory-line total stays 32 (misc 1 + files 13 +
//! chat 9 + loop 1 + approvals 8). The "59" tracked in S-0..S-9e
//! was a mistake (it mixed server.rs's total `//!` count, prose
//! included, with the modules' moved directory lines).
//!
//! The two transition routes stay in their own domains:
//! `executions_transition` with executions, `effects_transition`
//! with effects -- do not swap them.
//!
//! Staying in `server.rs` (frozen-pattern rule: called by staying
//! code): the whole worker helper cluster -- `WorkerRunCreateDto`
//! (constructed by staying execute_loop_tool), `WorkerCreateError`,
//! `worker_create_error`, `worker_binding_code`,
//! `worker_binary_for_command`, `check_worktree_add_preconditions`,
//! `worker_run_params_from_args`, `worker_command_digest` (called
//! by staying loop_gate_digest + loop_engine.rs),
//! `validate_worker_approval` (called only by staying
//! `create_worker_run_internal`), `LedgerOutcome` / `LedgerEntry` /
//! `LedgerEntryDecision` + `ledger_entry` / `ledger_entry_finish`
//! (called from already-moved routes_notes / routes_devices /
//! routes_data via `crate::server::` + staying code), and
//! `create_worker_run_internal` (the shared HTTP + loop
//! `worker.run` path). The moved `worker_runs_create` calls it
//! qualified and constructs `crate::server::WorkerRunCreateDto`.
//! `refresh_executions_export`, `effects_snapshot`,
//! `effect_number`, `latest_evidence_for_effect` became pub(crate)
//! in this batch (staying callers + the moving handlers); the
//! AppState field `sqlite_broken` became pub(crate) too (the
//! moving takeover/transition handlers write it).
//! `executions_path` stays private: only the staying
//! `refresh_executions_export` reads it.
//!
//! `p0e_tests` moves with this batch (the only remaining
//! #[cfg(test)] module in server.rs). Its glob import of the
//! parent module is rewritten to explicit imports: in this
//! module `super` is the crate root, not `server.rs`.
//!
//! The 13 /api/email routes stay in server.rs (S-9h, after S-9g);
//! after this batch, only those 13 remain there.

use std::collections::HashMap;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::effect_store::{EffectFileRecord, Reconciliation};
use crate::execution_store::ExecutionFileRecord;
use crate::routes_common::{bad_request, sqlite_error_response, sqlite_not_configured};
use crate::server::{AppState, WorkerRunCreateDto};
use crate::sqlite_store::{self, StateBackend};


// --- Execution shell (P0 phase 1) ---
//
// Read-only observability for the phase-1 shell records. Creation
// happens only as a side effect of task-runner / loop attempts
// (see create_execution); there is deliberately no create route
// yet, and no status transitions until phase 4.

#[derive(Debug, Deserialize)]
pub(crate) struct ExecutionGetQuery {
    execution_id: String,
}

/// P0-E read view: the stored record plus the two derived
/// fields (plan §2.2). Additive only: old clients reading the
/// record fields see no change. Both routes build views
/// through this one function, which is the single supplier of
/// the recoverability judgement — there is no second copy of
/// the algorithm in the get path.
#[derive(Debug, Serialize)]
pub(crate) struct ExecutionView {
    #[serde(flatten)]
    record: ExecutionFileRecord,
    recoverable: bool,
    active_assignment_id: Option<String>,
}

/// Attach derived fields to records. In SQLite mode the
/// driving-assignment map comes from one SQLite query; a
/// failed map read fails the whole response closed (a view
/// computed as "no drivers" from a broken read would lie
/// about recoverability). Outside SQLite mode there is no
/// assignment table, so the map is empty by construction.
pub(crate) fn execution_views(
    state: &AppState,
    records: Vec<ExecutionFileRecord>,
) -> Option<Vec<ExecutionView>> {
    let active_map: HashMap<String, String> = if state.sqlite_mode() == StateBackend::Sqlite {
        let sqlite = state.sqlite.as_ref()?;
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::active_assignments_map(&conn).ok()?
    } else {
        HashMap::new()
    };
    Some(
        records
            .into_iter()
            .map(|record| {
                let active = active_map.get(&record.execution_id).cloned();
                let recoverable = crate::execution_store::execution_recoverable(
                    &record.status,
                    active.is_some(),
                );
                ExecutionView { record, recoverable, active_assignment_id: active }
            })
            .collect(),
    )
}

pub(crate) async fn executions_list(State(state): State<AppState>) -> impl IntoResponse {
    // P0-D: SQLite mode reads SQLite; a broken database fails
    // closed (5xx) instead of serving a stale map.
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = crate::server::executions_snapshot(&state) else {
            return sqlite_error_response();
        };
        let Some(mut views) = execution_views(&state, records) else {
            return sqlite_error_response();
        };
        views.sort_by_key(|v| crate::server::execution_number(&v.record.execution_id));
        return Json(views).into_response();
    }
    let mut records: Vec<ExecutionFileRecord> =
        state.executions.lock().expect("execution lock").values().cloned().collect();
    records.sort_by_key(|r| crate::server::execution_number(&r.execution_id));
    let views = execution_views(&state, records).expect("non-SQLite views cannot fail");
    Json(views).into_response()
}

pub(crate) async fn executions_get(
    State(state): State<AppState>,
    Query(query): Query<ExecutionGetQuery>,
) -> impl IntoResponse {
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = crate::server::executions_snapshot(&state) else {
            return sqlite_error_response();
        };
        let Some(record) = records.into_iter().find(|r| r.execution_id == query.execution_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown execution" })))
                .into_response();
        };
        let Some(mut views) = execution_views(&state, vec![record]) else {
            return sqlite_error_response();
        };
        return Json(views.remove(0)).into_response();
    }
    match state.executions.lock().expect("execution lock").get(&query.execution_id) {
        Some(record) => {
            let views =
                execution_views(&state, vec![record.clone()]).expect("non-SQLite views cannot fail");
            Json(views.into_iter().next().expect("one view")).into_response()
        }
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown execution" })))
            .into_response(),
    }
}

// --- P0-E Assignment / Execution-transition routes (gateway
// only, no client) ---
//
// Takeover and transition are SQLite-plane operations: they
// require SQLite mode (not shadow, not JSONL). Shadow mode is
// a transient pre-switch state in production (startup
// auto-switches), and its JSONL authority would silently
// diverge from a takeover's SQLite-only assignment rows, so
// refusing there is the fail-closed choice, not an omission.
// Assignment reads, by contrast, touch SQLite-only tables
// with no JSONL counterpart, so they mirror the Inbox routes
// and serve shadow mode too.

#[derive(Debug, Deserialize)]
pub(crate) struct TakeoverDto {
    execution_id: String,
    worker_session_id: Option<String>,
}

pub(crate) async fn assignments_takeover(
    State(state): State<AppState>,
    Json(req): Json<TakeoverDto>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let result = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::takeover_assignment(
            &mut conn,
            &req.execution_id,
            req.worker_session_id.as_deref(),
            &state.origin_device_id,
        )
    };
    match result {
        Ok(outcome) => {
            // The connection guard is dropped before the map
            // lock (persist sites take them in the opposite
            // order); holding both at once would risk deadlock.
            state
                .executions
                .lock()
                .expect("execution lock")
                .insert(outcome.execution.execution_id.clone(), outcome.execution.clone());
            // JUDGEMENT CALL (plan is silent): after a
            // takeover, this process drives AS the takeover
            // worker, so it becomes the current worker: later
            // executions stamp it, and a clean shutdown ends
            // it (with its assignment) instead of only the
            // scan worker. Without this, a takeover worker's
            // assignment would survive a clean exit until the
            // next scan, contradicting §2.1 convergence.
            *state.current_worker_session.lock().expect("current worker lock") =
                outcome.assignment.worker_session_id.clone();
            crate::server::refresh_executions_export(&state);
            Json(serde_json::json!({
                "assignment": outcome.assignment,
                "execution": outcome.execution,
                "checkpoint": outcome.checkpoint,
                "superseded": outcome.superseded,
            }))
            .into_response()
        }
        Err(sqlite_store::TakeoverError::Refused(code)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": code })),
        )
            .into_response(),
        Err(sqlite_store::TakeoverError::Internal(detail)) => {
            *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
            eprintln!("warning: P0-E takeover failed: {detail}");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn assignments_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_assignments(&conn) {
        Ok(rows) => Json(rows).into_response(),
        Err(detail) => {
            eprintln!("warning: P0-E assignments list failed: {detail}");
            sqlite_error_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AssignmentGetQuery {
    assignment_id: String,
}

pub(crate) async fn assignments_get(
    State(state): State<AppState>,
    Query(query): Query<AssignmentGetQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_assignment(&conn, &query.assignment_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown assignment" })))
            .into_response(),
        Err(detail) => {
            eprintln!("warning: P0-E assignments get failed: {detail}");
            sqlite_error_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ExecutionTransitionDto {
    execution_id: String,
    status: String,
}

pub(crate) async fn executions_transition(
    State(state): State<AppState>,
    Json(req): Json<ExecutionTransitionDto>,
) -> impl IntoResponse {
    if !crate::execution_store::valid_execution_status(&req.status) {
        return bad_request("unknown execution status");
    }
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let result = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::transition_execution(&mut conn, &req.execution_id, &req.status)
    };
    match result {
        Ok(record) => {
            state
                .executions
                .lock()
                .expect("execution lock")
                .insert(record.execution_id.clone(), record.clone());
            crate::server::refresh_executions_export(&state);
            Json(record).into_response()
        }
        Err(sqlite_store::ExecutionTransitionError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown execution" })),
        )
            .into_response(),
        Err(sqlite_store::ExecutionTransitionError::Illegal) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "execution_transition_refused" })),
        )
            .into_response(),
        Err(sqlite_store::ExecutionTransitionError::Internal(detail)) => {
            *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
            eprintln!("warning: P0-E execution transition failed: {detail}");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn worker_runs_create(
    State(state): State<AppState>,
    Json(req): Json<WorkerRunCreateDto>,
) -> impl IntoResponse {
    match crate::server::create_worker_run_internal(&state, req).await {
        Ok(row) => (StatusCode::CREATED, Json(row)).into_response(),
        Err(err) => (
            err.status,
            Json(serde_json::json!({ "error": err.code })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct WorkerRunsListQuery {
    #[serde(default)]
    execution_id: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

pub(crate) async fn worker_runs_list(
    State(state): State<AppState>,
    Query(query): Query<WorkerRunsListQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_worker_runs(
        &conn,
        query.execution_id.as_deref(),
        query.status.as_deref(),
    ) {
        Ok(rows) => Json(rows).into_response(),
        Err(detail) => {
            eprintln!("warning: E2 worker runs list failed: {detail}");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn worker_run_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_worker_run(&conn, &id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown_worker_run" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: E2 worker run get failed: {detail}");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn worker_run_cancel(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let row = {
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::get_worker_run(&conn, &id)
    };
    let row = match row {
        Ok(Some(row)) => row,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "unknown_worker_run" })),
            )
                .into_response();
        }
        Err(detail) => {
            eprintln!("warning: E2 worker run cancel read failed: {detail}");
            return sqlite_error_response();
        }
    };
    if crate::worker::is_terminal_run_status(&row.status) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "worker_run_terminal" })),
        )
            .into_response();
    }
    // Claim first, kill second: the driver finalizes the
    // run as `cancelled` when it sees the claim (or when
    // the kill wakes its wait). A queued run with no pid
    // yet is claimed the same way and never spawns.
    state.mark_worker_cancelled(&id);
    if let Some(pid) = row.pid {
        crate::worker::kill_process_group(pid, "TERM");
    }
    let row = {
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::get_worker_run(&conn, &id)
    };
    match row {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown_worker_run" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: E2 worker run cancel re-read failed: {detail}");
            sqlite_error_response()
        }
    }
}

// Phase 2 read-only Effect routes, mirroring /executions: the old
// read paths keep serving the old records, and these routes expose
// the converged view alongside. No existing response changes.
#[derive(Debug, Deserialize)]
pub(crate) struct EffectGetQuery {
    effect_id: String,
}

pub(crate) async fn effects_list(State(state): State<AppState>) -> impl IntoResponse {
    // P0-D: SQLite mode reads SQLite; a broken database fails
    // closed (5xx) instead of serving a stale map.
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = crate::server::effects_snapshot(&state) else {
            return sqlite_error_response();
        };
        let mut records = records;
        records.sort_by_key(|r| crate::server::effect_number(&r.effect_id));
        return Json(records).into_response();
    }
    let mut records: Vec<EffectFileRecord> =
        state.effects.lock().expect("effect lock").values().cloned().collect();
    records.sort_by_key(|r| crate::server::effect_number(&r.effect_id));
    Json(records).into_response()
}

pub(crate) async fn effects_get(
    State(state): State<AppState>,
    Query(query): Query<EffectGetQuery>,
) -> impl IntoResponse {
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = crate::server::effects_snapshot(&state) else {
            return sqlite_error_response();
        };
        return match records.into_iter().find(|r| r.effect_id == query.effect_id) {
            Some(record) => Json(record).into_response(),
            None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
                .into_response(),
        };
    }
    match state.effects.lock().expect("effect lock").get(&query.effect_id) {
        Some(record) => Json(record.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
            .into_response(),
    }
}

// --- P0-C Effect lifecycle routes (gateway only, no client) ---

#[derive(Debug, Deserialize)]
pub(crate) struct EffectTransitionDto {
    effect_id: String,
    status: String,
    reconciliation_result: Option<String>,
    reconciliation_detail: Option<String>,
}

/// P0-C: drive one Effect along its lifecycle. Only contract
/// transitions are accepted (400 for a word outside the
/// vocabulary, 409 for an illegal edge or a legacy row, 404
/// for an unknown id). Driving to `reconciled` requires a
/// reconciliation result: a reconciled row without a conclusion
/// is refused.
pub(crate) async fn effects_transition(
    State(state): State<AppState>,
    Json(req): Json<EffectTransitionDto>,
) -> impl IntoResponse {
    if !crate::effect_store::is_new_effect_status(&req.status) {
        return bad_request("unknown effect status");
    }
    if req.status == "reconciled" && req.reconciliation_result.is_none() {
        return bad_request("reconciled requires reconciliation_result");
    }
    let reconciliation = req.reconciliation_result.map(|result| Reconciliation {
        result,
        checked_ms: crate::effect_store::now_ms(),
        detail: req.reconciliation_detail.clone(),
    });
    match crate::server::transition_effect(&state, &req.effect_id, &req.status, reconciliation, None) {
        Ok(record) => Json(record).into_response(),
        Err(404) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
            .into_response(),
        Err(400) => bad_request("unknown effect status"),
        // P0-D: broken SQLite fails closed (5xx), never 409.
        Err(500) => sqlite_error_response(),
        Err(_) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "effect_transition_refused" })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct EffectReconcileDto {
    effect_id: String,
}

/// P0-R: re-check an `unknown` or `started` Effect. Device
/// Effects keep their action-based re-check; every other class
/// goes through the observation dispatch: a landed observation
/// reconciles with `completed`, a device-reminder-missing one
/// with `failed`, an inconclusive one returns the row to
/// `unknown`. Rows with no observation method answer 409
/// `effect_no_readback`; rows without the information an
/// observation needs answer 409 `evidence_missing`. A
/// conclusive re-check lands on `reconciled` with its
/// conclusion; an inconclusive one returns the Effect to
/// `unknown`. The backing action record is updated to match,
/// when one exists.
pub(crate) async fn effects_reconcile(
    State(state): State<AppState>,
    Json(req): Json<EffectReconcileDto>,
) -> impl IntoResponse {
    let Some(effect) = crate::server::effect_by_id(&state, &req.effect_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
            .into_response();
    };
    match crate::server::reconcile_effect(&state, &effect) {
        Ok(record) => {
            // Mirror the verdict onto the action record, as
            // actions_refresh does (device class only; other
            // classes have no backing action to update).
            let mut actions = state.actions.lock().expect("action lock");
            if let Some(current) = actions.values_mut().find(|a| {
                a.effect_id.as_deref() == Some(&record.effect_id)
                    || (record.execution_id.is_some() && a.execution_id == record.execution_id)
            }) {
                match record.reconciliation.as_ref().map(|r| r.result.as_str()) {
                    Some("completed") => {
                        current.status = "completed".to_string();
                        current.detail = None;
                    }
                    Some("failed") => {
                        current.status = "failed".to_string();
                        current.detail = record
                            .reconciliation
                            .as_ref()
                            .and_then(|r| r.detail.clone());
                    }
                    _ => {}
                }
                let updated = current.clone();
                drop(actions);
                crate::server::persist_action(&state, &updated);
            }
            Json(record).into_response()
        }
        Err(error) => {
            // Preserve the P0-C shape for the status gate:
            // `effect_not_unknown` carries the row's status.
            let mut body = serde_json::json!({ "error": error });
            if error == "effect_not_unknown" {
                body["status"] = serde_json::json!(effect.status);
            }
            (StatusCode::CONFLICT, Json(body)).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct EffectVerifyDto {
    effect_id: String,
}

/// P0-R: verify a `succeeded` Effect against the world, without
/// consuming any approval, writing any idempotency key, or
/// changing the Effect row. The response is the new Evidence
/// row. Non-`succeeded` rows answer 409 `effect_not_verifiable`
/// (with the row's status); rows without the information an
/// observation needs answer 409 `evidence_missing`; rows with
/// no observation method answer 409 `effect_no_readback`.
/// A `mismatch` verdict opens a `verification_mismatch` Inbox
/// row (deduped); a later `verified` verdict auto-closes it.
/// A mismatch never enqueues Outbox. Evidence lives only in
/// SQLite: without a configured database this answers 500.
pub(crate) async fn effects_verify(
    State(state): State<AppState>,
    Json(req): Json<EffectVerifyDto>,
) -> impl IntoResponse {
    let Some(effect) = crate::server::effect_by_id(&state, &req.effect_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
            .into_response();
    };
    if effect.status != "succeeded" {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "effect_not_verifiable", "status": effect.status })),
        )
            .into_response();
    }
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    // The backing action carries the target flag the Effect
    // row does not — when one exists. A device row without
    // one (loop writes have none) falls through to the
    // generic path below (E2-C unification), where the
    // target is recovered from the row's own digest; it must
    // not 409 here, or unification would be a regression
    // against the loop rows' old generic verify.
    let action_backed = if matches!(effect.capability.as_str(), "device" | "devices") {
        let actions = state.actions.lock().expect("action lock");
        actions
            .values()
            .find(|a| {
                a.effect_id.as_deref() == Some(&effect.effect_id)
                    || (effect.execution_id.is_some()
                        && a.execution_id == effect.execution_id)
            })
            .cloned()
    } else {
        None
    };
    let outcome = match action_backed {
        Some(action) => crate::evidence::device_verify_outcome(
            &state,
            action.target_completed,
            &action.reminder_id,
        ),
        None => {
            let write_evidence = crate::server::latest_evidence_for_effect(&state, &effect);
            match crate::evidence::verify_outcome(&state, &effect, write_evidence.as_ref()) {
                Ok(outcome) => outcome,
                Err(crate::evidence::ObserveError::NoMethod) => {
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({ "error": "effect_no_readback" })),
                    )
                        .into_response();
                }
                Err(crate::evidence::ObserveError::MissingEvidence) => {
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({ "error": "evidence_missing" })),
                    )
                        .into_response();
                }
            }
        }
    };
    let (verdict, reason, observed) = match outcome {
        crate::evidence::VerifyOutcome::Verified { observed, reason } => {
            ("verified", reason, observed)
        }
        crate::evidence::VerifyOutcome::Mismatch { observed, reason } => {
            ("mismatch", reason, observed)
        }
        crate::evidence::VerifyOutcome::Inconclusive { observed, reason } => {
            ("inconclusive", reason, observed)
        }
    };
    let draft = crate::evidence::OwnedEvidence {
        effect_id: effect.effect_id.clone(),
        execution_id: effect.execution_id.clone(),
        source: "verify".to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        observed_json: serde_json::to_string(&observed).unwrap_or_else(|_| "{}".to_string()),
        checked_ms: crate::effect_store::now_ms() as i64,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::record_verification(&mut conn, &draft.as_draft()) {
        Ok(row) => {
            // E2-C §4.5: a verify writes Evidence too — the
            // export regenerates here as well.
            crate::server::export_evidence_best_effort(&state, &conn);
            Json(row).into_response()
        }
        Err(_) => sqlite_error_response(),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct EvidenceIdQuery {
    evidence_id: String,
}

/// P0-R: list Evidence rows (bare JSON array, seq order, like
/// `/effects/list`). Same SQLite gate as the Inbox routes.
pub(crate) async fn evidence_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::list_evidence(&conn) {
        Ok(rows) => Json(rows).into_response(),
        Err(_) => sqlite_error_response(),
    }
}

/// P0-R: get one Evidence row by id, or 404 `unknown evidence`.
pub(crate) async fn evidence_get(
    State(state): State<AppState>,
    Query(query): Query<EvidenceIdQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::get_evidence(&conn, &query.evidence_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown evidence" })),
        )
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

/// E2-C §4.5: return the Evidence audit export as JSONL — one
/// line per Evidence row, the same rows `evidence_list`
/// returns, generated from SQLite on every call (the single
/// source of truth; the audit FILE written at Evidence write
/// points holds the same content). Same guards as the list/get
/// reads: 500 without a configured SQLite database, 500 on a
/// broken one. Generating live — rather than serving the file —
/// keeps the export consistent with list/get even where the
/// audit file is not wired (the packaged app now sets
/// MOCK_EVIDENCE_PATH via the shell, tails follow-up).
pub(crate) async fn evidence_export(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::list_evidence(&conn) {
        Ok(rows) => {
            let body = rows
                .iter()
                .filter_map(|r| serde_json::to_string(r).ok())
                .collect::<Vec<_>>()
                .join("\n");
            let body = if body.is_empty() {
                body
            } else {
                format!("{body}\n")
            };
            (
                StatusCode::OK,
                [("content-type", "application/jsonl")],
                body,
            )
                .into_response()
        }
        Err(_) => sqlite_error_response(),
    }
}

// --- P0-D Inbox / Outbox routes (gateway only, no client) ---
//
// Inbox first batch: approval_required, review_required,
// failed, unknown_requires_reconciliation. Rows are derived
// in the same SQLite transaction as the triggering transition
// (see sqlite_store); failed rows close only by user ack,
// the others close automatically on decision / reconciliation
// or by ack. Outbox first batch: local_notification only,
// pending -> sent | failed, no automatic retry, requeue is an
// internal function with no route. Without a configured
// SQLite database these routes answer 500 (the Inbox /
// Outbox live in SQLite); a broken database also answers 5xx.

#[derive(Debug, Deserialize)]
pub(crate) struct InboxListQuery {
    kind: Option<String>,
    open_only: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InboxIdQuery {
    inbox_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct InboxAckDto {
    inbox_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OutboxIdQuery {
    outbox_id: String,
}

pub(crate) async fn inbox_list(
    State(state): State<AppState>,
    Query(query): Query<InboxListQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let open_only = query.open_only.as_deref() == Some("true") || query.open_only.as_deref() == Some("1");
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_inbox(&conn, query.kind.as_deref(), open_only) {
        Ok(rows) => Json(rows).into_response(),
        Err(_) => sqlite_error_response(),
    }
}

pub(crate) async fn inbox_get(
    State(state): State<AppState>,
    Query(query): Query<InboxIdQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_inbox(&conn, &query.inbox_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown inbox item" })))
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

pub(crate) async fn inbox_ack(
    State(state): State<AppState>,
    Json(req): Json<InboxAckDto>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::ack_inbox(&mut conn, &req.inbox_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown inbox item" })))
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

pub(crate) async fn outbox_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_outbox(&conn) {
        Ok(rows) => Json(rows).into_response(),
        Err(_) => sqlite_error_response(),
    }
}

pub(crate) async fn outbox_get(
    State(state): State<AppState>,
    Query(query): Query<OutboxIdQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_outbox(&conn, &query.outbox_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown outbox item" })))
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

/// D1-3 (plan §4): manual top-up delivery of one Outbox
/// row through the log sink. A row that is already `sent`
/// (or `failed`) is returned unchanged — delivery is
/// idempotent, a repeat call never re-delivers. Unknown id
/// is 404, same shape as `outbox_get`.
pub(crate) async fn outbox_deliver(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::deliver_outbox_via_log_sink(&mut conn, &id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown outbox item" })))
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

/// D1-3 (plan §4): manual requeue of one Outbox row. This
/// opens the P0-D "No route exposes this" gap: a `failed`
/// row returns to `pending` so it can be delivered again —
/// by the tick consumer on its next pass (the `outbox.requeued`
/// event is a delivery trigger) or by a manual
/// `POST /outbox/{id}/deliver`, whichever runs first. A row
/// that is not `failed` is returned unchanged.
pub(crate) async fn outbox_requeue(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::requeue_outbox(&mut conn, &id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown outbox item" })))
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

#[cfg(test)]
mod p0e_tests {
    use std::path::PathBuf;

    use crate::server::{create_execution, AppState};
    use crate::sqlite_store;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mock-server-p0e-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    /// P0-E creation stamping, unit-level: the worker comes
    /// from the scan, the snapshot id tracks the execution
    /// number, and the instruction digest follows the driver
    /// (task prefix / loop system / adapter empty string).
    /// Pure JSONL mode (no SQLite) keeps both fields None.
    #[test]
    fn p0e_create_execution_stamps_worker_and_snapshot_per_driver() {
        let dir = temp_dir("create");
        let state = AppState::with_files_root_and_approvals(dir.join("files"), None)
            .with_sqlite_path(dir.join("gateway.sqlite3"));
        state.sqlite_import_and_switch().expect("switch");
        assert_eq!(state.current_worker_session().as_deref(), Some("worker-1"));

        let task_exec = create_execution(&state, Some("task-1".to_string()), None);
        let loop_exec = create_execution(&state, None, Some("loop-1".to_string()));
        let adapter_exec = create_execution(&state, None, None);
        assert_eq!(task_exec.worker_session_id.as_deref(), Some("worker-1"));
        assert_eq!(loop_exec.worker_session_id.as_deref(), Some("worker-1"));
        assert_eq!(adapter_exec.worker_session_id.as_deref(), Some("worker-1"));
        assert_eq!(
            task_exec.workspace_snapshot_id.as_deref(),
            Some("snapshot-1")
        );
        assert_eq!(
            loop_exec.workspace_snapshot_id.as_deref(),
            Some("snapshot-2")
        );
        assert_eq!(
            adapter_exec.workspace_snapshot_id.as_deref(),
            Some("snapshot-3")
        );
        let conn = state.sqlite.as_ref().unwrap().lock().expect("sqlite lock");
        let task_snap = sqlite_store::get_snapshot(&conn, "snapshot-1")
            .expect("snapshot-1")
            .expect("snapshot-1 row");
        assert_eq!(
            task_snap.instruction_digest,
            sqlite_store::sha256_hex(crate::task_runner::PROMPT_PREFIX.as_bytes())
        );
        let loop_snap = sqlite_store::get_snapshot(&conn, "snapshot-2")
            .expect("snapshot-2")
            .expect("snapshot-2 row");
        assert_eq!(
            loop_snap.instruction_digest,
            sqlite_store::sha256_hex(crate::deepseek_loop::SYSTEM.as_bytes())
        );
        let adapter_snap = sqlite_store::get_snapshot(&conn, "snapshot-3")
            .expect("snapshot-3")
            .expect("snapshot-3 row");
        assert_eq!(
            adapter_snap.instruction_digest,
            sqlite_store::sha256_hex(b"")
        );
        assert_ne!(
            task_snap.instruction_digest,
            loop_snap.instruction_digest
        );
        drop(conn);
        let _ = std::fs::remove_dir_all(&dir);

        // Pure JSONL mode: no worker, no snapshot, unchanged.
        let plain = AppState::with_files_root_and_approvals(dir.join("files"), None);
        let record = create_execution(&plain, Some("task-9".to_string()), None);
        assert_eq!(record.worker_session_id, None);
        assert_eq!(record.workspace_snapshot_id, None);
    }
}
