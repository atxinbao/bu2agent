//! iMessage routes, MCL-1 (threads-only) + MCL-4 (send).
//!
//! Six routes, the mail read-batch 1 pattern
//! (`routes_mail.rs` is the model) for the first three,
//! the 6b-1 send pattern for the last three:
//!
//! - `POST /api/imessage/sync` — manual trigger, fact
//!   recording only (no Effect, no approval, no Outbox,
//!   like `POST /api/email/sync`). Reads the live source
//!   and writes the `imessage_threads` table + the
//!   `imessage.synced` events in one transaction. The
//!   fetch runs outside the SQLite lock (the source read
//!   is synchronous, like `fetch_window`).
//! - `GET /api/imessage/threads` — list synced threads
//!   from SQLite (default 50, max 200), a bare array
//!   like `GET /api/email/synced`; tombstones are
//!   visible (6b-3 Q-14).
//! - `GET /api/imessage/threads/{id}` — one synced
//!   thread by local `thread-N` id, a bare row like
//!   `GET /api/email/synced/{id}`; tombstones are
//!   returned, unknown ids are 404.
//! - `POST /api/imessage/send` — MCL-4: send one
//!   plain-text message to one already-synced thread
//!   (Q-MCL-20/21/23), the 6b-1 send order — 400,
//!   disconnect 409, idempotency replay before
//!   planning, approval peek, reuse-or-plan, consume,
//!   execute, finish. "Sent" means Messages.app
//!   accepted the send, never that the other side
//!   received it (Q-MCL-21). D1-3 C-2: an optional
//!   `deliver_at_ms` schedules the send instead — the
//!   approval is consumed at schedule time, the Effect
//!   is built at delivery time (Q-C-13), and the body
//!   waits in `scheduled_imessages_sends` (Q-C2-01).
//! - `GET /api/imessage/sent` — list sent rows, a bare
//!   array like `GET /api/email/sent`.
//! - `GET /api/imessage/sent/{id}` — one sent row by
//!   local `sent-imsg-N` id, a bare row like
//!   `GET /api/email/sent/{id}`; unknown ids are 404.

use std::sync::Arc;

use agent_desktop::approval::ApprovalStatus;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::imessage_source::{clamp_limit, IMessageError};
use crate::imessage_sync::{
    get_imessage_thread, get_source_cursor, list_imessage_threads, record_sync,
    IMESSAGE_SOURCE_ID,
};
use crate::routes_common::{
    bad_request, now_ms, sqlite_error_response, sqlite_not_configured,
};
use crate::server::{
    consume_bound_approval_for_effect, create_execution_with_binding, create_write_mandate,
    current_policy_version_id, effect_for_approval, ledger_entry, ledger_entry_finish,
    start_bound_effect, AppState, LedgerEntryDecision, LedgerOutcome,
};
use crate::sqlite_store::{self, StateBackend};

#[derive(Deserialize, Debug)]
pub struct ListParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Map a source failure onto the shared read-path
/// codes: unavailable -> 501, denied -> 403, not
/// running / upstream / timeout -> 502 (the mail
/// `mail_error_response` precedent, with imessage
/// wording — never a mail error string).
pub(crate) fn imessage_error_response(error: &IMessageError, what: &str) -> Response {
    let (status, detail) = match error {
        IMessageError::NotFound(detail) => (StatusCode::NOT_FOUND, detail.to_string()),
        IMessageError::Unavailable(detail) => (StatusCode::NOT_IMPLEMENTED, detail.clone()),
        IMessageError::Denied => (
            StatusCode::FORBIDDEN,
            "imessage automation denied (check Messages automation permission)".to_string(),
        ),
        IMessageError::Upstream(detail) => (StatusCode::BAD_GATEWAY, detail.clone()),
        IMessageError::NotRunning => (
            StatusCode::BAD_GATEWAY,
            "Messages is not running".to_string(),
        ),
        IMessageError::Timeout => (
            StatusCode::BAD_GATEWAY,
            "imessage read timed out".to_string(),
        ),
    };
    (
        status,
        Json(serde_json::json!({
            "error": format!("imessage {what} failed: {detail}"),
            "code": "imessage_source_error",
        })),
    )
        .into_response()
}

pub(crate) async fn imessage_sync(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    // The fetch runs outside the SQLite lock (osascript
    // calls must not hold it); the write is one
    // transaction (rows + events + cursor).
    let threads = match state.imessage_source.threads(usize::MAX, 0) {
        Ok(threads) => threads,
        Err(error) => return imessage_error_response(&error, "sync"),
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match record_sync(
        &mut conn,
        &threads,
        IMESSAGE_SOURCE_ID,
        &state.origin_device_id,
    ) {
        Ok(result) => {
            Json(serde_json::to_value(result).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: imessage sync failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn imessage_threads_list(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
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
    let limit = clamp_limit(params.limit);
    let offset = params.offset.unwrap_or(0);
    let conn = sqlite.lock().expect("sqlite lock");
    match list_imessage_threads(&conn, IMESSAGE_SOURCE_ID, limit, offset) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: imessage threads list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn imessage_thread_get(
    State(state): State<AppState>,
    Path(thread_id): Path<String>,
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
    let conn = sqlite.lock().expect("sqlite lock");
    match get_imessage_thread(&conn, &thread_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown imessage thread" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: imessage thread get failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// The stored source cursor (used by tests asserting
/// the cursor advances across syncs).
pub fn get_imessage_cursor(conn: &rusqlite::Connection) -> Result<String, String> {
    get_source_cursor(conn, IMESSAGE_SOURCE_ID)
}

/// MCL-4: a disconnected imessage-source rejects new
/// writes with 409 `source_disconnected` (the 6b-1
/// `mail_source_disconnected` precedent). Reads are
/// unaffected, and a missing Registry row (seed never
/// ran) is not a disconnect — the write proceeds.
pub(crate) fn imessage_source_disconnected(state: &AppState) -> bool {
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let Ok(conn) = sqlite.lock() else {
        return false;
    };
    matches!(
        sqlite_store::get_source(&conn, IMESSAGE_SOURCE_ID),
        Ok(Some(row)) if row.status == "disconnected"
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct IMessageSendDto {
    // All four validated fields are Option so a missing
    // field is the route's own 400 (not the extractor's
    // 422): "no key → 400" must hold however the client
    // forgets the key. LOCAL DEFINITION (plan §6, the
    // 6b-1 `EmailSendDto` precedent). `deliver_at_ms` is
    // the D1-3 C-2 schedule flag: absent → send now (the
    // MCL-4 behavior, byte-identical); present → schedule
    // the send for that epoch-ms time instead.
    thread_id: Option<String>,
    body: Option<String>,
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
    deliver_at_ms: Option<i64>,
}

pub(crate) fn sent_row_response(
    row: &crate::imessage_send::SentIMessageRow,
    replayed: bool,
) -> axum::response::Response {
    let mut value = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = value.as_object_mut() {
        obj.insert("replayed".to_string(), serde_json::Value::Bool(replayed));
    }
    Json(value).into_response()
}

pub(crate) async fn imessage_send(
    State(state): State<AppState>,
    Json(req): Json<IMessageSendDto>,
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
    // 1. Validate the DTO: empty thread_id / body /
    // idempotency_key → 400. The thread must be an
    // already-synced row (Q-MCL-20): an unknown id — or
    // a tombstoned one — is 404, never a new thread.
    // This is input validation, so it runs before the
    // disconnect gate; the 6b-1 order below it is
    // untouched.
    let Some(thread_id) = req.thread_id.filter(|s| !s.is_empty()) else {
        return bad_request("thread_id is required");
    };
    let Some(body) = req.body.filter(|s| !s.is_empty()) else {
        return bad_request("body must not be empty");
    };
    let Some(idempotency_key) = req.idempotency_key.filter(|k| !k.trim().is_empty()) else {
        return bad_request("idempotency_key is required");
    };
    let thread = {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::imessage_sync::get_imessage_thread(&conn, &thread_id) {
            Ok(Some(row)) if !row.deleted => row,
            Ok(_) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown imessage thread" })),
                )
                    .into_response();
            }
            Err(detail) => {
                eprintln!("warning: imessage send thread lookup failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    let body_hash = crate::imessage_send::imessage_body_hash(&body);
    let digest = crate::operation_binding::imessage_send_digest(&thread_id, &body_hash);
    // 2. Disconnected source rejects new writes (409).
    if imessage_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 3. Idempotency replay BEFORE planning a new Effect:
    // same key + same content → 200 original row, no
    // second send, no second Effect; same key +
    // different content → 409.
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::imessage_send::find_by_idempotency_key(&conn, &idempotency_key) {
            Ok(Some(existing)) => {
                if existing.thread_id == thread_id && existing.body_hash == body_hash {
                    return sent_row_response(&existing, true);
                }
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "idempotency_key_conflict" })),
                )
                    .into_response();
            }
            Ok(None) => {}
            Err(detail) => {
                eprintln!("warning: imessage send idempotency lookup failed ({detail})");
                return sqlite_error_response();
            }
        }
    }
    // 4. Approval peek (read-only, nothing burned): the
    // digest is checked BEFORE planning, so a mismatch
    // leaves no orphaned planned row and the approval
    // stays unconsumed for a correct retry (SD3).
    let Some(approval_id) = req.tool_call_id.clone() else {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "approval_required" })),
        )
            .into_response();
    };
    {
        let approvals = state.approvals.lock().expect("approval lock");
        let Some(record) = approvals.get(&approval_id) else {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({ "error": "approval_required" })),
            )
                .into_response();
        };
        if record.tool != "imessage.send" {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({ "error": "approval_required" })),
            )
                .into_response();
        }
        if record.status == ApprovalStatus::Rejected {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({ "error": "approval_rejected" })),
            )
                .into_response();
        }
        if record.status != ApprovalStatus::Approved {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({ "error": "approval_required" })),
            )
                .into_response();
        }
        if record.consumed {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "approval_already_consumed" })),
            )
                .into_response();
        }
        match record.operation_digest.as_deref() {
            // A tool-only approval can never authorize a
            // digest-bound write.
            None => {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({ "error": "approval_binding_mismatch" })),
                )
                    .into_response();
            }
            Some(bound) if bound != digest => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "approval_digest_mismatch" })),
                )
                    .into_response();
            }
            _ => {}
        }
    }
    // D1-3 C-2: a present `deliver_at_ms` schedules the
    // send instead of executing it now — steps 1–4 above
    // (validate, thread, disconnect, replay, approval
    // peek) are identical; the schedule path below
    // consumes the approval and enqueues the Outbox row,
    // and builds NO Effect (Q-C-13: the Effect is built
    // at delivery time). A past value is already due —
    // the next row-scan pass delivers it (Q-C-01).
    if let Some(deliver_at_ms) = req.deliver_at_ms {
        return schedule_imessage_send(
            &state,
            &thread_id,
            &body,
            &body_hash,
            &idempotency_key,
            &digest,
            &approval_id,
            deliver_at_ms,
        );
    }
    // 5. Reuse-or-plan (E2-A V1): a planned row with the
    // same approval_id — a previous attempt that planned
    // then failed to consume — is reused, never
    // duplicated. Otherwise build the write Execution +
    // single-tool Mandate (Q-D1-6b-23) and plan through
    // the shared ledger entry. The adapter fallback is
    // NOT used for 6b writes.
    let (execution_id, effect) = match effect_for_approval(&state, &approval_id, Some(&digest)) {
        Some(effect) => match effect.status.as_str() {
            "planned" | "authorized" => {
                let Some(execution_id) = effect.execution_id.clone() else {
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({ "error": "effect_idempotency_unavailable" })),
                    )
                        .into_response();
                };
                (execution_id, effect)
            }
            "succeeded" => {
                // Terminal with an unconsumed approval
                // should not happen (consume precedes
                // terminal), but a succeeded replay is
                // still the original row, never a resend.
                let conn = sqlite.lock().expect("sqlite lock");
                match crate::imessage_send::find_by_idempotency_key(&conn, &idempotency_key) {
                    Ok(Some(existing)) => return sent_row_response(&existing, true),
                    _ => {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({ "error": "effect_terminal", "effect_id": effect.effect_id, "status": effect.status })),
                        )
                            .into_response();
                    }
                }
            }
            _ => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_not_ready", "effect_id": effect.effect_id, "status": effect.status })),
                )
                    .into_response();
            }
        },
        None => {
            let execution = create_execution_with_binding(&state, None, None, None, None, None);
            create_write_mandate(&state, &execution.execution_id, "imessage.send");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "imessage",
                "send",
                &idempotency_key,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::imessage_send::find_by_idempotency_key(&conn, &idempotency_key) {
                        Ok(Some(existing)) => return sent_row_response(&existing, true),
                        _ => {
                            return (
                                StatusCode::CONFLICT,
                                Json(serde_json::json!({ "error": "effect_idempotency_replay", "effect_id": other.effect_id, "status": other.status })),
                            )
                                .into_response();
                        }
                    }
                }
                LedgerEntryDecision::Blocked(other) => {
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
                    )
                        .into_response();
                }
            }
        }
    };
    // 6. Consume the bound approval (binding + push to
    // authorized), then drive the Effect to started.
    if let Err(response) = consume_bound_approval_for_effect(
        &state,
        "imessage.send",
        &req.tool_call_id,
        &digest,
        Some(&effect.effect_id),
    ) {
        return *response;
    }
    if let Err(response) = start_bound_effect(&state, &effect) {
        return *response;
    }
    // 7. Execute outside the SQLite lock: the AppleScript
    // call can take up to 30 s. Same spawn_blocking shape
    // as device_set. The send inputs are cloned for the
    // blocking task; the originals are still needed below
    // for the fact row.
    let source = Arc::clone(&state.imessage_source);
    let send_native_id = thread.native_id.clone();
    let send_body = body.clone();
    let send_result = tokio::task::spawn_blocking(move || {
        source.send(&send_native_id, &send_body)
    })
    .await;
    let outcome = match send_result {
        Ok(result) => result,
        Err(_) => Err(IMessageError::Timeout),
    };
    let policy_version = current_policy_version_id(&state).unwrap_or_default();
    match outcome {
        Ok(sent) => {
            // Record the fact row + imessage.sent event
            // in one transaction FIRST, then finish the
            // Effect: a succeeded Effect must never exist
            // without its fact row.
            let sent_ms = now_ms() as i64;
            let mut conn = sqlite.lock().expect("sqlite lock");
            match crate::imessage_send::record_sent(
                &mut conn,
                sent.native_id.as_deref(),
                &idempotency_key,
                &thread,
                &body_hash,
                sent_ms,
                &effect.effect_id,
                &execution_id,
                &policy_version,
                &state.origin_device_id,
            ) {
                Ok(row) => {
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => sent_row_response(&row, false),
                        Err(_) => {
                            eprintln!(
                                "warning: imessage send effect finish failed for {}",
                                effect.effect_id
                            );
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(serde_json::json!({ "error": "effect_transition_failed" })),
                            )
                                .into_response()
                        }
                    }
                }
                Err(detail) => {
                    // A UNIQUE-key race (a concurrent send
                    // with the same key won the row): the
                    // message content was sent, so the
                    // existing row is the replay, never a
                    // second row.
                    drop(conn);
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::imessage_send::find_by_idempotency_key(&conn, &idempotency_key) {
                        Ok(Some(existing))
                            if existing.thread_id == thread_id
                                && existing.body_hash == body_hash =>
                        {
                            drop(conn);
                            let _ = ledger_entry_finish(
                                &state,
                                &effect.effect_id,
                                LedgerOutcome::Succeeded,
                            );
                            sent_row_response(&existing, true)
                        }
                        _ => {
                            eprintln!("warning: imessage send record failed ({detail})");
                            let _ = ledger_entry_finish(
                                &state,
                                &effect.effect_id,
                                LedgerOutcome::Failed,
                            );
                            (
                                StatusCode::CONFLICT,
                                Json(serde_json::json!({ "error": "idempotency_key_conflict" })),
                            )
                                .into_response()
                        }
                    }
                }
            }
        }
        Err(err) => {
            // Only a killed/ambiguous script run is
            // Unknown (P0-R): Messages.app may or may
            // not have sent the message, so an
            // unknown_requires_reconciliation Inbox row
            // is opened by the transition itself. Every
            // other gateway-side failure is Failed.
            let (status, error, outcome) = match err {
                IMessageError::Timeout => (
                    StatusCode::BAD_GATEWAY,
                    "imessage send timed out",
                    LedgerOutcome::Unknown,
                ),
                IMessageError::NotRunning => (
                    StatusCode::BAD_GATEWAY,
                    "Messages is not running",
                    LedgerOutcome::Failed,
                ),
                IMessageError::Denied => (
                    StatusCode::FORBIDDEN,
                    "imessage automation denied",
                    LedgerOutcome::Failed,
                ),
                IMessageError::Unavailable(reason) => {
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    return (
                        StatusCode::NOT_IMPLEMENTED,
                        Json(serde_json::json!({ "error": reason, "effect_id": effect.effect_id })),
                    )
                        .into_response();
                }
                _ => (
                    StatusCode::BAD_GATEWAY,
                    "imessage source error",
                    LedgerOutcome::Failed,
                ),
            };
            let _ = ledger_entry_finish(&state, &effect.effect_id, outcome);
            (
                status,
                Json(serde_json::json!({ "error": error, "effect_id": effect.effect_id })),
            )
                .into_response()
        }
    }
}

/// The `body_hash` inside a scheduled-send Outbox
/// row's `payload_summary_json`, if the payload parses
/// and carries one. The schedule path compares it
/// against the fresh request's hash for the
/// same-key-different-content 409.
fn scheduled_payload_body_hash(payload_summary_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(payload_summary_json)
        .ok()?
        .get("body_hash")?
        .as_str()
        .map(str::to_string)
}

/// D1-3 C-2: schedule one iMessage send for later
/// instead of executing it now. The caller
/// (`imessage_send`) already ran steps 1–4 (validate,
/// thread, disconnect, replay, approval peek), so the
/// approval here is approved, unconsumed, and
/// digest-bound for exactly this content.
///
/// Order: same-key check (replay / conflict, approval
/// untouched) → consume the approval (Q-C-13: consumed
/// at schedule time, with no Effect yet, so
/// `effect_id` is None) → write Execution + single-tool
/// Mandate (the step-5 None-branch shape, minus the
/// Effect) → enqueue the Outbox row + body row in one
/// transaction. The response is the Outbox row only —
/// no Effect row (Q-C-13 clarification). A past
/// `deliver_at_ms` is already due (Q-C-01).
#[allow(clippy::too_many_arguments)]
fn schedule_imessage_send(
    state: &AppState,
    thread_id: &str,
    body: &str,
    body_hash: &str,
    idempotency_key: &str,
    digest: &str,
    approval_id: &str,
    deliver_at_ms: i64,
) -> Response {
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::get_outbox_by_dedup_key(&conn, idempotency_key) {
            Ok(Some(existing)) => {
                let same = scheduled_payload_body_hash(&existing.payload_summary_json)
                    .is_some_and(|hash| hash == body_hash);
                if same {
                    return Json(existing).into_response();
                }
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "idempotency_key_conflict" })),
                )
                    .into_response();
            }
            Ok(None) => {}
            Err(detail) => {
                eprintln!("warning: imessage schedule dedup lookup failed ({detail})");
                return sqlite_error_response();
            }
        }
    }
    // Consume before creating anything: a failed
    // consume leaves no orphaned Execution, Mandate,
    // or Outbox row behind.
    if let Err(response) = consume_bound_approval_for_effect(
        state,
        "imessage.send",
        &Some(approval_id.to_string()),
        digest,
        None,
    ) {
        return *response;
    }
    let execution = create_execution_with_binding(state, None, None, None, None, None);
    create_write_mandate(state, &execution.execution_id, "imessage.send");
    // The payload carries ids + hashes only, never the
    // body (red line 7); the body lives only in the
    // `scheduled_imessage_sends` row (Q-C2-01).
    let payload = sqlite_store::canonical_json(&serde_json::json!({
        "thread_id": thread_id,
        "body_hash": body_hash,
        "idempotency_key": idempotency_key,
        "approval_id": approval_id,
    }));
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::enqueue_scheduled_imessage_send(
        &mut conn,
        &execution.execution_id,
        &payload,
        deliver_at_ms,
        thread_id,
        body,
        body_hash,
        idempotency_key,
        approval_id,
    ) {
        Ok((row, true)) => Json(row).into_response(),
        Ok((row, false)) => {
            // A concurrent schedule with the same key
            // won the row between the check above and
            // this transaction: same content replays,
            // different content is a conflict. The
            // approval above was already consumed in
            // this narrow race — recorded limitation
            // (see the C-2 commit message).
            let same = scheduled_payload_body_hash(&row.payload_summary_json)
                .is_some_and(|hash| hash == body_hash);
            if same {
                Json(row).into_response()
            } else {
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "idempotency_key_conflict" })),
                )
                    .into_response()
            }
        }
        Err(detail) => {
            eprintln!("warning: imessage schedule enqueue failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// Mark one scheduled-send Outbox row sent or failed
/// through the pending-only guard (the anti-double-
/// delivery guard: a row that already left `pending`
/// is returned unchanged and no second event is
/// written). Returns true only when the row now reads
/// `sent`.
/// AO-5 (Q-AO-12): a failed attempt does NOT mark the
/// row failed outright — it consumes retry backoff
/// (attempts + 1, rescheduled to now + 1/5/15 min);
/// only the 4th failed attempt marks it failed.
fn mark_scheduled_row(state: &AppState, row: &sqlite_store::OutboxRow, success: bool) -> bool {
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let Ok(mut conn) = sqlite.lock() else {
        return false;
    };
    if !success {
        return match sqlite_store::fail_outbox_with_backoff(
            &mut conn,
            &row.outbox_id,
            sqlite_store::now_ms() as i64,
        ) {
            Ok(Some(updated)) => updated.status == "sent",
            _ => false,
        };
    }
    match sqlite_store::deliver_outbox(&mut conn, &row.outbox_id, true) {
        Ok(Some(updated)) => updated.status == "sent",
        _ => false,
    }
}

/// Mark one scheduled-send Outbox row sent and delete
/// its body row (delete-on-sent). A delete failure only
/// warns — the row is already `sent`, so the orphaned
/// body is never read again (recorded residual).
fn finish_scheduled_row_sent(state: &AppState, row: &sqlite_store::OutboxRow) -> bool {
    let sent = mark_scheduled_row(state, row, true);
    if sent {
        if let Some(sqlite) = &state.sqlite {
            if let Ok(mut conn) = sqlite.lock() {
                if let Err(detail) =
                    sqlite_store::delete_scheduled_imessage_send(&mut conn, &row.outbox_id)
                {
                    eprintln!(
                        "warning: scheduled body delete failed for {}: {detail}",
                        row.outbox_id
                    );
                }
            }
        }
    }
    sent
}

/// What the delivery Effect plan found.
enum ScheduledPlan {
    /// Execute now under this Execution + Effect.
    /// Boxed: the record is 352 bytes and the other
    /// variant carries nothing (clippy large-enum-variant).
    Execute(String, Box<crate::effect_store::EffectFileRecord>),
    /// The send already happened (a succeeded Effect);
    /// mark the row sent, never resend.
    AlreadySent,
}

/// Plan (or reuse) the delivery Effect. A planned /
/// authorized row for this approval is reused (E2-A
/// V1). A succeeded row means the send already
/// happened (the replay check above returns first, so
/// this arm is defensive). Any other state — failed,
/// unknown, or started from a previous delivery
/// attempt — plans a FRESH Effect under a FRESH
/// Execution + Mandate: the old Effect's idempotency
/// key (`exec:<old execution>:...`) blocks a replan
/// under the row's Execution, and the operator's
/// explicit requeue is the decision to resend despite
/// the ambiguity (judgment #4 — the unknown case may
/// have sent already; the Inbox row is the checkpoint).
fn plan_scheduled_effect(
    state: &AppState,
    row: &sqlite_store::OutboxRow,
    approval_id: &str,
    digest: &str,
    idempotency_key: &str,
) -> Option<ScheduledPlan> {
    match effect_for_approval(state, approval_id, Some(digest)) {
        Some(effect) => match effect.status.as_str() {
            "planned" | "authorized" => {
                let execution_id = effect
                    .execution_id
                    .clone()
                    .unwrap_or_else(|| row.execution_id.clone());
                Some(ScheduledPlan::Execute(execution_id, Box::new(effect)))
            }
            "succeeded" => {
                let Some(sqlite) = &state.sqlite else {
                    return None;
                };
                let conn = sqlite.lock().expect("sqlite lock");
                match crate::imessage_send::find_by_idempotency_key(&conn, idempotency_key) {
                    Ok(Some(_)) => Some(ScheduledPlan::AlreadySent),
                    _ => None,
                }
            }
            _ => plan_fresh_scheduled_effect(state, approval_id, digest, idempotency_key),
        },
        None => match ledger_entry(
            state,
            Some(row.execution_id.clone()),
            "imessage",
            "send",
            idempotency_key,
            Some(digest.to_string()),
            Some(approval_id.to_string()),
        ) {
            LedgerEntryDecision::Entered(entry) => {
                Some(ScheduledPlan::Execute(entry.execution_id, Box::new(entry.effect)))
            }
            // A terminal / in-flight Effect under the
            // row's Execution that the approval lookup
            // missed: fall through to a fresh Effect.
            LedgerEntryDecision::Replay(_) | LedgerEntryDecision::Blocked(_) => {
                plan_fresh_scheduled_effect(state, approval_id, digest, idempotency_key)
            }
        },
    }
}

fn plan_fresh_scheduled_effect(
    state: &AppState,
    approval_id: &str,
    digest: &str,
    idempotency_key: &str,
) -> Option<ScheduledPlan> {
    let execution = create_execution_with_binding(state, None, None, None, None, None);
    create_write_mandate(state, &execution.execution_id, "imessage.send");
    match ledger_entry(
        state,
        Some(execution.execution_id.clone()),
        "imessage",
        "send",
        idempotency_key,
        Some(digest.to_string()),
        Some(approval_id.to_string()),
    ) {
        LedgerEntryDecision::Entered(entry) => {
            Some(ScheduledPlan::Execute(entry.execution_id, Box::new(entry.effect)))
        }
        LedgerEntryDecision::Replay(other) | LedgerEntryDecision::Blocked(other) => {
            eprintln!(
                "warning: fresh scheduled effect blocked by {} ({})",
                other.effect_id, other.status
            );
            None
        }
    }
}

/// D1-3 C-2: deliver one due `imessage_send` Outbox
/// row (called by the row scan and by manual deliver).
/// Returns true only when the row now reads `sent`
/// (including the idempotency replay). The approval is
/// NOT consumed here — it was consumed at schedule time
/// (Q-C-13); the Effect is planned here, at delivery
/// time. Outcome mapping is Q-C-06: timeout → the
/// Effect goes unknown (+ Inbox via the transition)
/// AND the row goes failed (judgment #1: a pending row
/// would be auto-retried on the next tick, and
/// auto-retry is not in this plan — the Effect's
/// unknown state preserves the honest distinction);
/// every other failure → Effect failed + row failed.
/// The body row is deleted only on `sent` and retained
/// on failure, so a manual requeue can redeliver
/// (judgment #2). A single row's failure never aborts
/// the caller's pass (the C-1 precedent). A transient
/// store error returns false WITHOUT marking the row —
/// the next tick retries it.
pub(crate) async fn deliver_scheduled_imessage_send(
    state: &AppState,
    row: &sqlite_store::OutboxRow,
) -> bool {
    if state.sqlite_is_broken() {
        return false;
    }
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return false;
    }
    // 1. The payload: thread_id + body_hash +
    // idempotency_key + approval_id, all required.
    let payload: serde_json::Value = match serde_json::from_str(&row.payload_summary_json) {
        Ok(value) => value,
        Err(_) => {
            eprintln!(
                "warning: scheduled send {} has a malformed payload",
                row.outbox_id
            );
            return mark_scheduled_row(state, row, false);
        }
    };
    let Some(thread_id) = payload.get("thread_id").and_then(|v| v.as_str()) else {
        eprintln!(
            "warning: scheduled send {} has no thread_id in its payload",
            row.outbox_id
        );
        return mark_scheduled_row(state, row, false);
    };
    let Some(body_hash) = payload.get("body_hash").and_then(|v| v.as_str()) else {
        eprintln!(
            "warning: scheduled send {} has no body_hash in its payload",
            row.outbox_id
        );
        return mark_scheduled_row(state, row, false);
    };
    let Some(idempotency_key) = payload.get("idempotency_key").and_then(|v| v.as_str()) else {
        eprintln!(
            "warning: scheduled send {} has no idempotency_key in its payload",
            row.outbox_id
        );
        return mark_scheduled_row(state, row, false);
    };
    let Some(approval_id) = payload.get("approval_id").and_then(|v| v.as_str()) else {
        eprintln!(
            "warning: scheduled send {} has no approval_id in its payload",
            row.outbox_id
        );
        return mark_scheduled_row(state, row, false);
    };
    let (thread_id, body_hash, idempotency_key, approval_id) = (
        thread_id.to_string(),
        body_hash.to_string(),
        idempotency_key.to_string(),
        approval_id.to_string(),
    );
    // 2. Idempotency replay FIRST (crash recovery: a
    // previous attempt may have recorded the sent row
    // but died before marking this row sent).
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::imessage_send::find_by_idempotency_key(&conn, &idempotency_key) {
            Ok(Some(existing))
                if existing.thread_id == thread_id && existing.body_hash == body_hash =>
            {
                drop(conn);
                return finish_scheduled_row_sent(state, row);
            }
            Ok(Some(_)) => {
                eprintln!(
                    "warning: scheduled send {} conflicts on idempotency key {}",
                    row.outbox_id, idempotency_key
                );
                drop(conn);
                return mark_scheduled_row(state, row, false);
            }
            Ok(None) => {}
            Err(detail) => {
                eprintln!(
                    "warning: scheduled send idempotency lookup failed ({detail})"
                );
                return false;
            }
        }
    }
    // 3. The body (Q-C2-01: the only durable copy).
    let body_row = {
        let conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::get_scheduled_imessage_send(&conn, &row.outbox_id) {
            Ok(Some(body_row)) => body_row,
            Ok(None) => {
                eprintln!(
                    "warning: scheduled send {} has no stored body",
                    row.outbox_id
                );
                drop(conn);
                return mark_scheduled_row(state, row, false);
            }
            Err(detail) => {
                eprintln!("warning: scheduled body lookup failed ({detail})");
                return false;
            }
        }
    };
    // 4. Integrity: the stored row must agree with the
    // payload on every field, and the body's own hash
    // must match — a mismatch is a corrupt schedule,
    // never a send.
    if body_row.body_hash != body_hash
        || crate::imessage_send::imessage_body_hash(&body_row.body) != body_hash
        || body_row.thread_id != thread_id
        || body_row.idempotency_key != idempotency_key
        || body_row.approval_id != approval_id
    {
        eprintln!(
            "warning: scheduled send {} body does not match its payload",
            row.outbox_id
        );
        return mark_scheduled_row(state, row, false);
    }
    let body = body_row.body.clone();
    // 5. The thread must still be a synced,
    // non-tombstoned row (Q-MCL-20, the send order).
    let thread = {
        let conn = sqlite.lock().expect("sqlite lock");
        match get_imessage_thread(&conn, &thread_id) {
            Ok(Some(thread)) if !thread.deleted => thread,
            Ok(_) => {
                eprintln!(
                    "warning: scheduled send {} names an unknown or deleted thread",
                    row.outbox_id
                );
                drop(conn);
                return mark_scheduled_row(state, row, false);
            }
            Err(detail) => {
                eprintln!("warning: scheduled send thread lookup failed ({detail})");
                return false;
            }
        }
    };
    // 6. Disconnect gate (judgment #3): a disconnected
    // source rejects the delivery; the operator requeues
    // after reconnect.
    if imessage_source_disconnected(state) {
        eprintln!(
            "warning: scheduled send {} refused: source_disconnected",
            row.outbox_id
        );
        return mark_scheduled_row(state, row, false);
    }
    // 7. Plan the Effect (Q-C-13: built at delivery).
    let digest = crate::operation_binding::imessage_send_digest(&thread_id, &body_hash);
    let (execution_id, effect) = match plan_scheduled_effect(
        state,
        row,
        &approval_id,
        &digest,
        &idempotency_key,
    ) {
        Some(ScheduledPlan::Execute(execution_id, effect)) => (execution_id, effect),
        Some(ScheduledPlan::AlreadySent) => return finish_scheduled_row_sent(state, row),
        None => {
            eprintln!(
                "warning: scheduled send {} could not plan its Effect",
                row.outbox_id
            );
            return mark_scheduled_row(state, row, false);
        }
    };
    if let Err(response) = start_bound_effect(state, &effect) {
        eprintln!(
            "warning: scheduled send {} could not start its Effect",
            row.outbox_id
        );
        drop(response);
        return mark_scheduled_row(state, row, false);
    }
    // 8. Execute outside the SQLite lock (the send
    // handler's step-7 shape, verbatim).
    let source = Arc::clone(&state.imessage_source);
    let send_native_id = thread.native_id.clone();
    let send_body = body.clone();
    let send_result =
        tokio::task::spawn_blocking(move || source.send(&send_native_id, &send_body)).await;
    let outcome = match send_result {
        Ok(result) => result,
        Err(_) => Err(IMessageError::Timeout),
    };
    let policy_version = current_policy_version_id(state).unwrap_or_default();
    match outcome {
        Ok(sent) => {
            // Record the fact row + imessage.sent event
            // in one transaction FIRST, then finish the
            // Effect: a succeeded Effect must never exist
            // without its fact row.
            let sent_ms = now_ms() as i64;
            let mut conn = sqlite.lock().expect("sqlite lock");
            match crate::imessage_send::record_sent(
                &mut conn,
                sent.native_id.as_deref(),
                &idempotency_key,
                &thread,
                &body_hash,
                sent_ms,
                &effect.effect_id,
                &execution_id,
                &policy_version,
                &state.origin_device_id,
            ) {
                Ok(_) => {
                    drop(conn);
                    match ledger_entry_finish(state, &effect.effect_id, LedgerOutcome::Succeeded)
                    {
                        Ok(_) => finish_scheduled_row_sent(state, row),
                        Err(_) => {
                            eprintln!(
                                "warning: imessage scheduled send effect finish failed for {}",
                                effect.effect_id
                            );
                            // The sent row exists; the
                            // replay check on a later pass
                            // marks the Outbox row sent.
                            // Leave it pending (no failed
                            // mark).
                            false
                        }
                    }
                }
                Err(detail) => {
                    // A UNIQUE-key race (a concurrent send
                    // with the same key won the row): the
                    // message content was sent, so the
                    // existing row is the replay, never a
                    // second row.
                    drop(conn);
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::imessage_send::find_by_idempotency_key(&conn, &idempotency_key)
                    {
                        Ok(Some(existing))
                            if existing.thread_id == thread_id
                                && existing.body_hash == body_hash =>
                        {
                            drop(conn);
                            let _ = ledger_entry_finish(
                                state,
                                &effect.effect_id,
                                LedgerOutcome::Succeeded,
                            );
                            finish_scheduled_row_sent(state, row)
                        }
                        _ => {
                            eprintln!(
                                "warning: imessage scheduled send record failed ({detail})"
                            );
                            drop(conn);
                            let _ = ledger_entry_finish(
                                state,
                                &effect.effect_id,
                                LedgerOutcome::Failed,
                            );
                            mark_scheduled_row(state, row, false)
                        }
                    }
                }
            }
        }
        Err(err) => {
            // Q-C-06: only a killed/ambiguous script run
            // is Unknown (P0-R); every other gateway-side
            // failure is Failed. Either way the Outbox
            // row is failed (judgment #1) and the body
            // is retained for a manual requeue
            // (judgment #2).
            let (outcome, detail) = match err {
                IMessageError::Timeout => (
                    LedgerOutcome::Unknown,
                    "imessage send timed out".to_string(),
                ),
                IMessageError::NotRunning => (
                    LedgerOutcome::Failed,
                    "Messages is not running".to_string(),
                ),
                IMessageError::Denied => (
                    LedgerOutcome::Failed,
                    "imessage automation denied".to_string(),
                ),
                IMessageError::Unavailable(reason) => (LedgerOutcome::Failed, reason),
                _ => (
                    LedgerOutcome::Failed,
                    "imessage source error".to_string(),
                ),
            };
            eprintln!(
                "warning: imessage scheduled send failed for {}: {detail}",
                row.outbox_id
            );
            let _ = ledger_entry_finish(state, &effect.effect_id, outcome);
            mark_scheduled_row(state, row, false)
        }
    }
}

pub(crate) async fn imessage_sent_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::imessage_send::list_sent_imessages(&conn) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: imessage sent list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn imessage_sent_get(
    State(state): State<AppState>,
    Path(sent_id): Path<String>,
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
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::imessage_send::get_sent_imessage(&conn, &sent_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown sent imessage" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: imessage sent get failed ({detail})");
            sqlite_error_response()
        }
    }
}
