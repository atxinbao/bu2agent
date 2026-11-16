//! Devices routes: /devices reminders list/set, actions
//! create/list/get/refresh/mark_rejected, authorization read/request.
//!
//! The handlers moved here from `server.rs` (FU-5 S-2). The rest of the
//! route directory lives in `server.rs`. There is no `//!` directory line
//! for devices in the `server.rs` header, so no directory line moved with
//! this batch. The `// --- Device loop ---` section comment stays in
//! `server.rs` with the DEVICE_* consts (shared with the loop-tool path
//! and `device_effect_digest`) and the WORKER_WAIT consts (worker.wait,
//! not devices); `persist_action` also stays there — `effects_reconcile`
//! calls it — and is imported from `crate::server`, as are the ledger
//! helpers, which stay in `server.rs` for the S-9g ledger batch.

use std::sync::Arc;
use std::time::Duration;

use agent_desktop::approval::ApprovalStatus;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::device_backend::{
    execute_set, resolve_unknown, AuthorizationStatus, ExecuteOutcome, Reminder,
    ResolveOutcome,
};
use crate::device_store::DeviceActionRecord;
use crate::effect_store::NewEffect;
use crate::routes_common::bad_request;
use crate::server::{
    check_device_approval, check_idempotency, consume_device_approval_for_effect,
    device_effect_digest, effect_by_id, idempotency_key_missing, ledger_entry,
    ledger_entry_finish, persist_action, reconcile_device_effect_with_outcome,
    record_effect, stamp_effect_approval, transition_effect, AppState, DEVICE_ACTION,
    DEVICE_ID, DEVICE_TIMEOUT_SECS, IdempotencyDecision, LedgerEntryDecision,
    LedgerOutcome,
};

#[derive(Debug, Serialize)]
pub(crate) struct ReminderDto {
    id: String,
    title: String,
    completed: bool,
}

impl From<&Reminder> for ReminderDto {
    fn from(r: &Reminder) -> Self {
        Self { id: r.id.clone(), title: r.title.clone(), completed: r.completed }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ActionCreateDto {
    reminder_id: String,
    target_completed: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeviceSetDto {
    action_id: String,
    tool_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ActionRejectDto {
    action_id: String,
    tool_call_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ActionRefreshDto {
    action_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ActionGetQuery {
    action_id: String,
}

fn action_number(action_id: &str) -> u64 {
    action_id.strip_prefix("action-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

/// Allowlist pin: when a test reminder id is pinned, any other id
/// is refused. Returns Some(error response) on refusal, None when ok.
fn device_pin_error(state: &AppState, reminder_id: &str) -> Option<axum::response::Response> {
    if let Some(pin) = &state.device_reminder_pin {
        if pin != reminder_id {
            return Some(
                (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "reminder_not_allowed" })))
                    .into_response(),
            );
        }
    }
    None
}

pub(crate) async fn reminders_list(State(state): State<AppState>) -> impl IntoResponse {
    match state.device_backend.list() {
        Ok(reminders) => {
            Json(reminders.iter().map(ReminderDto::from).collect::<Vec<_>>()).into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub(crate) async fn actions_create(
    State(state): State<AppState>,
    Json(req): Json<ActionCreateDto>,
) -> impl IntoResponse {
    if let Some(response) = device_pin_error(&state, &req.reminder_id) {
        return response;
    }
    let mut next = state.next_action_id.lock().expect("action id lock");
    let id = format!("action-{}", *next);
    *next += 1;
    // P0-B adapter: the attempt starts at creation, so the
    // Execution is created here and carried on the action record;
    // device_set attaches its Effect to it. The pin check above
    // already passed, so a rejected creation leaves no orphan.
    // (E2-C §4.2: the Execution is built inside `ledger_entry`
    // below, not here.)
    // P0-C: the action is planned here. Its Effect is created
    // as `planned` (contract triple, Q5 digest) and carried on
    // the action record; device_set drives it forward instead
    // of recording a fresh row. A never-executed action leaves
    // its planned row planned: recorded, never authorized.
    // E2-C §4.2: the plan step goes through the shared ledger
    // entry (the gate cannot fire on a fresh Execution, but
    // the decision is handled, not ignored).
    let digest = device_effect_digest(&req.reminder_id, req.target_completed);
    let (execution_id, planned) = match ledger_entry(
        &state,
        None,
        "device",
        "reminders.set_completed",
        &req.reminder_id,
        Some(digest),
        None,
    ) {
        LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
        LedgerEntryDecision::Replay(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_replay", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
        LedgerEntryDecision::Blocked(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
    };
    let record = DeviceActionRecord {
        action_id: id,
        device: DEVICE_ID.to_string(),
        action: DEVICE_ACTION.to_string(),
        reminder_id: req.reminder_id,
        target_completed: req.target_completed,
        status: "requested".to_string(),
        detail: None,
        execution_id: Some(execution_id),
        effect_id: Some(planned.effect_id),
    };
    state.actions.lock().expect("action lock").insert(record.action_id.clone(), record.clone());
    persist_action(&state, &record);
    Json(record).into_response()
}

pub(crate) async fn device_set(
    State(state): State<AppState>,
    Json(req): Json<DeviceSetDto>,
) -> impl IntoResponse {
    // Phase 2 (Q4): peek first (approval errors keep their old
    // precedence over action errors, nothing burned), then bind
    // and consume only once the action is known executable — so a
    // failed set never burns the approval, and a lost consume race
    // still leaves the action requested for a fresh approval.
    if let Err(response) = check_device_approval(&state, &req.tool_call_id, &req.action_id) {
        return *response;
    }
    let (reminder_id, target, action_execution, action_effect) = {
        let actions = state.actions.lock().expect("action lock");
        let Some(record) = actions.get(&req.action_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
                .into_response();
        };
        if record.status != "requested" {
            return bad_request("action is not requested");
        }
        if let Some(response) = device_pin_error(&state, &record.reminder_id) {
            return response;
        }
        (
            record.reminder_id.clone(),
            record.target_completed,
            record.execution_id.clone(),
            record.effect_id.clone(),
        )
    };
    // P0-C: the action's own Effect gates the set. A terminal
    // Effect with a still-requested action is an inconsistent
    // state (e.g. operator-driven): fail closed, never execute.
    // An unsettled Effect refuses likewise. A different Effect
    // with the same idempotency key replays/blocks through the
    // same rule. Legacy actions without a planned Effect use
    // the key check alone. A planned Effect that is missing
    // entirely (the Effects store was not persisted across a
    // restart while actions were) falls back to the legacy
    // gate, like the file gate: the action binding and
    // single-use consumption still apply, and a fresh terminal
    // row is recorded below.
    let digest = device_effect_digest(&reminder_id, target);
    let mut action_effect_for_gate = action_effect.clone();
    if let Some(effect_id) = &action_effect {
        match effect_by_id(&state, effect_id).map(|e| e.status) {
            Some(status) if crate::effect_store::is_terminal_effect_status(&status) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_terminal", "effect_id": effect_id, "status": status })),
                )
                    .into_response();
            }
            Some(status) if status != "planned" && status != "authorized" => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_not_ready", "effect_id": effect_id, "status": status })),
                )
                    .into_response();
            }
            None => {
                action_effect_for_gate = None;
            }
            _ => {}
        }
    }
    // E2-A §2.4: the idempotency gate applies only when the
    // action carries an Execution. A pre-P0-B action has
    // none BY DESIGN (P0-B never fabricates a historical
    // association for it; locked by
    // `old_rows_read_unchanged_and_unattributed` in
    // p0b_adapters): its set proceeds and records a new row
    // with a null execution, as before — the gate is skipped,
    // not failed. The 409 `effect_idempotency_unavailable`
    // below is for the current shape, where the action DOES
    // carry an Execution but the digest is missing (a key
    // that is owed and absent, not one that never existed).
    if action_execution.is_some() {
        if idempotency_key_missing(action_execution.as_deref(), Some(&digest)) {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_unavailable" })),
            )
                .into_response();
        }
        match check_idempotency(
            &state,
            action_execution.as_deref(),
            "device",
            "reminders.set_completed",
            &reminder_id,
            Some(&digest),
            action_effect_for_gate.as_deref(),
        ) {
            IdempotencyDecision::Proceed => {}
            IdempotencyDecision::Replay(other) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_idempotency_replay", "effect_id": other.effect_id, "status": other.status })),
                )
                    .into_response();
            }
            IdempotencyDecision::Blocked(other) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
                )
                    .into_response();
            }
            // P0-D: broken migrated-domain store fails closed.
            IdempotencyDecision::Unavailable => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "sqlite_unavailable" })),
                )
                    .into_response();
            }
        }
    }
    if let Err(response) = consume_device_approval_for_effect(
        &state,
        &req.tool_call_id,
        &req.action_id,
        action_effect_for_gate.as_deref(),
        Some(&digest),
    ) {
        return *response;
    }
    // The planned device Effect was created before its approval
    // existed; stamp the authorizing approval onto it now.
    if let (Some(effect_id), Some(approval_id)) = (&action_effect_for_gate, &req.tool_call_id) {
        stamp_effect_approval(&state, effect_id, approval_id);
    }
    {
        let mut actions = state.actions.lock().expect("action lock");
        let Some(record) = actions.get_mut(&req.action_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
                .into_response();
        };
        record.status = "approved".to_string();
        let approved = record.clone();
        persist_action(&state, &approved);
    }
    let backend = Arc::clone(&state.device_backend);
    let outcome = tokio::task::spawn_blocking(move || {
        execute_set(&backend, &reminder_id, target, Duration::from_secs(DEVICE_TIMEOUT_SECS))
    })
    .await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "device execution failed" })),
            )
                .into_response();
        }
    };
    let mut actions = state.actions.lock().expect("action lock");
    let Some(record) = actions.get_mut(&req.action_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response();
    };
    match outcome {
        ExecuteOutcome::Completed(_) => {
            record.status = "completed".to_string();
            record.detail = None;
        }
        ExecuteOutcome::Failed(e) => {
            record.status = "failed".to_string();
            record.detail = Some(e.to_string());
        }
        ExecuteOutcome::Unknown => {
            record.status = "unknown".to_string();
            record.detail =
                Some("write submitted but target state not confirmed by readback".to_string());
        }
        ExecuteOutcome::AwaitingAuthorization => {
            record.status = "awaiting_authorization".to_string();
            record.detail = Some("waiting for authorization".to_string());
        }
    }
    let updated = record.clone();
    persist_action(&state, &updated);
    // P0-C: drive the action's planned Effect to its terminal
    // state (contract §4 mapping: completed -> succeeded,
    // failed -> failed, unknown -> unknown). awaiting_
    // authorization is terminal for the action but means
    // nothing was attempted: the Effect stays `authorized`
    // (approval passed, execution never started) rather than
    // taking a backwards transition to `planned` — the §4
    // `awaiting_authorization -> planned` row describes
    // single-row conversion, not a lifecycle transition, and
    // the action record itself carries the waiting detail.
    // Legacy actions without a planned Effect record one
    // terminal row, in the new word.
    let terminal = match updated.status.as_str() {
        "completed" => Some("succeeded"),
        "failed" => Some("failed"),
        "unknown" => Some("unknown"),
        _ => None,
    };
    if let Some(terminal) = terminal {
        let outcome = match terminal {
            "succeeded" => LedgerOutcome::Succeeded,
            "failed" => LedgerOutcome::Failed,
            _ => LedgerOutcome::Unknown,
        };
        // Drive the planned Effect when it exists in this run;
        // a fallback run (planned row lost across restart, or a
        // legacy action with no planned row) records a fresh
        // terminal row instead.
        let drive = updated
            .effect_id
            .as_deref()
            .filter(|id| effect_by_id(&state, id).is_some());
        match drive {
            Some(effect_id) => {
                // E2-C §4.2: the terminal transition goes through
                // the shared ledger finish step. The chain is
                // built from the row's current status, so the
                // authorized row takes started → terminal exactly
                // as before.
                let _ = ledger_entry_finish(&state, effect_id, outcome);
            }
            None => match &updated.execution_id {
                Some(execution_id) => {
                    // E2-C §4.2: the fallback row also goes
                    // through the shared ledger entry (plan +
                    // finish) on the action's own Execution. The
                    // gate cannot fire here — the planned row is
                    // gone and the Execution belongs to this
                    // action alone — and a Replay/Blocked answer
                    // would mean an identical row already covers
                    // the audit, so it is skipped, not recorded
                    // a second time.
                    if let LedgerEntryDecision::Entered(entry) = ledger_entry(
                        &state,
                        Some(execution_id.clone()),
                        "device",
                        "reminders.set_completed",
                        &updated.reminder_id,
                        Some(digest.clone()),
                        req.tool_call_id.clone(),
                    ) {
                        let _ =
                            ledger_entry_finish(&state, &entry.effect.effect_id, outcome);
                    }
                }
                None => {
                    // Legacy action with no Execution: the direct
                    // terminal record stays, as before (E2-A:
                    // legacy device_set actions are exempt from
                    // the gate).
                    record_effect(
                        &state,
                        NewEffect {
                            execution_id: updated.execution_id.clone(),
                            capability: "device".to_string(),
                            operation: "reminders.set_completed".to_string(),
                            scope: updated.reminder_id.clone(),
                            input_digest: Some(digest.clone()),
                            approval_id: req.tool_call_id.clone(),
                            status: terminal.to_string(),
                        },
                    );
                }
            },
        }
    }
    Json(updated).into_response()
}

pub(crate) async fn actions_mark_rejected(
    State(state): State<AppState>,
    Json(req): Json<ActionRejectDto>,
) -> impl IntoResponse {
    // The approval itself must be rejected: no device call happens here.
    let approval_status = {
        let approvals = state.approvals.lock().expect("approval lock");
        approvals.get(&req.tool_call_id).map(|r| r.status)
    };
    let Some(status) = approval_status else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown tool_call_id" })))
            .into_response();
    };
    if status != ApprovalStatus::Rejected {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "approval_not_rejected" })),
        )
            .into_response();
    }
    let mut actions = state.actions.lock().expect("action lock");
    let Some(record) = actions.get_mut(&req.action_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response();
    };
    if record.status != "requested" {
        return bad_request("action is not requested");
    }
    if let Some(response) = device_pin_error(&state, &record.reminder_id) {
        return response;
    }
    record.status = "rejected".to_string();
    let updated = record.clone();
    persist_action(&state, &updated);
    Json(updated).into_response()
}

pub(crate) async fn actions_refresh(
    State(state): State<AppState>,
    Json(req): Json<ActionRefreshDto>,
) -> impl IntoResponse {
    let current = {
        let actions = state.actions.lock().expect("action lock");
        let Some(record) = actions.get(&req.action_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
                .into_response();
        };
        record.clone()
    };
    if current.status != "unknown" {
        return Json(current).into_response();
    }
    // P0-C: when the action has an `unknown` Effect, the same
    // single re-check observation drives both the action and
    // its Effect (unknown -> reconciling -> reconciled, or back
    // to unknown on an inconclusive re-check). Legacy actions
    // without an Effect keep the old action-only behaviour.
    let effect = current
        .effect_id
        .as_deref()
        .and_then(|id| effect_by_id(&state, id))
        .filter(|e| e.status == "unknown");
    if let Some(effect) = &effect {
        let _ = transition_effect(&state, &effect.effect_id, "reconciling", None, None);
    }
    let outcome =
        resolve_unknown(&state.device_backend, &current.reminder_id, current.target_completed);
    let mut actions = state.actions.lock().expect("action lock");
    let Some(record) = actions.get_mut(&req.action_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response();
    };
    match &outcome {
        ResolveOutcome::Completed(_) => {
            record.status = "completed".to_string();
            record.detail = None;
        }
        ResolveOutcome::Failed(e) => {
            record.status = "failed".to_string();
            record.detail = Some(e.to_string());
        }
        ResolveOutcome::StillUnknown(maybe_err) => {
            record.detail = Some(match maybe_err {
                Some(e) => e.to_string(),
                None => "state unchanged; write may still be in flight".to_string(),
            });
        }
    }
    let updated = record.clone();
    persist_action(&state, &updated);
    drop(actions);
    if let Some(effect) = &effect {
        // The action above already reflects this outcome; the
        // Effect transition must agree with it, and a failed
        // re-check never writes unknown as failed (see the
        // helper's hard constraint).
        let _ = reconcile_device_effect_with_outcome(&state, effect, outcome);
    }
    Json(updated).into_response()
}

pub(crate) async fn actions_list(State(state): State<AppState>) -> Json<Vec<DeviceActionRecord>> {
    let mut records: Vec<DeviceActionRecord> =
        state.actions.lock().expect("action lock").values().cloned().collect();
    records.sort_by_key(|r| action_number(&r.action_id));
    Json(records)
}

pub(crate) async fn actions_get(
    State(state): State<AppState>,
    Query(query): Query<ActionGetQuery>,
) -> impl IntoResponse {
    match state.actions.lock().expect("action lock").get(&query.action_id) {
        Some(record) => Json(record.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response(),
    }
}

fn authorization_status_name(status: AuthorizationStatus) -> &'static str {
    match status {
        AuthorizationStatus::Authorized => "authorized",
        AuthorizationStatus::Denied => "denied",
        AuthorizationStatus::NotDetermined => "not_determined",
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct AuthorizationStatusDto {
    status: String,
}

/// Read-only: report the backend's current reminders authorization.
/// No approval needed: this reads permission state, it writes nothing.
pub(crate) async fn devices_authorization(State(state): State<AppState>) -> Json<AuthorizationStatusDto> {
    Json(AuthorizationStatusDto {
        status: authorization_status_name(state.device_backend.authorization_status()).to_string(),
    })
}

/// Ask the system for reminders access and report the resulting
/// status. This is the one endpoint that may show the OS prompt
/// (only when the status is NotDetermined). It writes no reminder:
/// it only requests the permission precondition. The request runs
/// on a blocking thread because the synchronous EventKit request
/// waits up to 30 seconds for an answer; a JoinError fails closed
/// to Denied (a panicked request proves nothing, never Authorized).
pub(crate) async fn devices_authorization_request(
    State(state): State<AppState>,
) -> Json<AuthorizationStatusDto> {
    let backend = Arc::clone(&state.device_backend);
    let status = tokio::task::spawn_blocking(move || backend.request_authorization())
        .await
        .unwrap_or(AuthorizationStatus::Denied);
    Json(AuthorizationStatusDto {
        status: authorization_status_name(status).to_string(),
    })
}
