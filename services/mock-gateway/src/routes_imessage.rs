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
//!   received it (Q-MCL-21).
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
    // 6b-1 `EmailSendDto` precedent).
    thread_id: Option<String>,
    body: Option<String>,
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
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
