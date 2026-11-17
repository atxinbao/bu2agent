//! Mail routes: /api/email (13).
//!
//! The handlers moved here from `server.rs` (FU-5 S-9h, the last
//! batch of the server.rs split). After this batch `server.rs`
//! contains ZERO route registrations: the 13 mail routes were the
//! last handlers left there (the other 234 routes already point
//! at `crate::routes_*` modules; the single `Router::new()` chain
//! stays in `server.rs`).
//!
//! No `//!` directory lines move with this batch: `server.rs` has
//! zero `//! - ` lines (27 prose `//!` lines stay there); the
//! directory-line total stays 32 (misc 1 + files 13 + chat 9 +
//! loop 1 + approvals 8). The "59" tracked in S-0..S-9e was a
//! mistake (it mixed server.rs's total `//!` count, prose
//! included, with the modules' moved directory lines).
//!
//! The "Email read surface" section comment stays in `server.rs`:
//! it also covers the staying loop helpers `email_rendered` and
//! `email_error_text` (called by staying loop code at ~1343/1387
//! and ~1348/1392).
//!
//! Staying in `server.rs` (frozen-pattern rule: called by staying
//! code or by already-moved modules): `effect_for_approval`,
//! `create_execution_with_binding`, `create_write_mandate`,
//! `ledger_entry`, `ledger_entry_finish`,
//! `consume_bound_approval_for_effect`, `start_bound_effect`,
//! `current_policy_version_id` + `LedgerOutcome` /
//! `LedgerEntryDecision` (all already `pub(crate)` -- no
//! visibility change was needed in this batch). The moved code
//! imports them BY NAME from `crate::server`, the
//! routes_notes.rs / routes_data.rs pattern for the identical
//! 6b write flow, so the moved handlers stay byte-identical
//! modulo `pub(crate)`. `create_write_mandate` /
//! `start_bound_effect` have no staying callers in `server.rs`
//! itself, but already-moved routes_notes / routes_data /
//! routes_tasks / routes_files / routes_engineering /
//! routes_loop call them via `crate::server::`.
//!
//! Every `AppState` field the moved code touches (`approvals`,
//! `mail_source`, `origin_device_id`, `sqlite`) was already
//! `pub(crate)`; `sqlite_is_broken` is `pub fn`, `sqlite_mode`
//! is `pub(crate) fn`.
//!
//! Import trims in `server.rs` (each provably unused after the
//! move): `extract::State`, `clamp_limit`, `DeleteQuery`,
//! `sqlite_error_response`, `sqlite_not_configured`.
//! `bad_request` / `now_ms` / `MailError` / `MailSource` /
//! `UnavailableMailSource` / `Json` / `StatusCode` /
//! `IntoResponse` stay (staying uses counted by hand).

use std::sync::Arc;

use agent_desktop::approval::ApprovalStatus;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::mail_source::{clamp_limit, MailError};
use crate::routes_common::{
    bad_request, now_ms, sqlite_error_response, sqlite_not_configured, DeleteQuery,
};
use crate::server::{
    consume_bound_approval_for_effect, create_execution_with_binding, create_write_mandate,
    current_policy_version_id, effect_for_approval, ledger_entry, ledger_entry_finish,
    start_bound_effect, AppState, LedgerEntryDecision, LedgerOutcome,
};
use crate::sqlite_store::{self, StateBackend};


pub(crate) fn mail_error_response(err: MailError, not_found_what: &'static str) -> axum::response::Response {
    match err {
        MailError::NotFound(_) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("unknown {not_found_what}") })),
        )
            .into_response(),
        MailError::Unavailable(reason) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({ "error": reason })),
        )
            .into_response(),
        MailError::Denied => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "mail automation denied" })),
        )
            .into_response(),
        MailError::Upstream(_) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "mail source error" })),
        )
            .into_response(),
        // D1-6b-1: same 502 as any other upstream
        // failure on the read paths; the send route
        // matches on the variant itself, not the status.
        MailError::NotRunning | MailError::Timeout => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "mail source error" })),
        )
            .into_response(),
    }
}

pub(crate) async fn email_mailbox(State(state): State<AppState>) -> impl IntoResponse {
    match state.mail_source.mailbox() {
        Ok(info) => Json(serde_json::to_value(info).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "mailbox"),
    }
}

pub(crate) async fn email_mailbox_setup(State(state): State<AppState>) -> impl IntoResponse {
    match state.mail_source.setup() {
        Ok(info) => Json(serde_json::to_value(info).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "mailbox"),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct EmailThreadsQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

pub(crate) async fn email_threads_list(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<EmailThreadsQuery>,
) -> impl IntoResponse {
    let limit = clamp_limit(query.limit);
    let offset = query.offset.unwrap_or(0);
    match state.mail_source.threads(limit, offset) {
        Ok(threads) => Json(serde_json::to_value(threads).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "threads"),
    }
}

pub(crate) async fn email_thread_get(
    State(state): State<AppState>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.mail_source.thread(&thread_id) {
        Ok(thread) => Json(serde_json::to_value(thread).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "thread"),
    }
}

pub(crate) async fn email_message_get(
    State(state): State<AppState>,
    axum::extract::Path(message_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.mail_source.message(&message_id) {
        Ok(message) => Json(serde_json::to_value(message).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "message"),
    }
}

pub(crate) async fn email_attachment_get(
    State(state): State<AppState>,
    axum::extract::Path((message_id, index)): axum::extract::Path<(String, usize)>,
) -> impl IntoResponse {
    match state.mail_source.attachment(&message_id, index) {
        Ok(data) => {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_str(&data.content_type)
                    .unwrap_or_else(|_| axum::http::HeaderValue::from_static("application/octet-stream")),
            );
            headers.insert(
                axum::http::header::CONTENT_DISPOSITION,
                axum::http::HeaderValue::from_str(&format!("attachment; filename=\"{}\"", data.filename))
                    .unwrap_or_else(|_| axum::http::HeaderValue::from_static("attachment")),
            );
            (headers, axum::body::Bytes::from(data.bytes)).into_response()
        }
        Err(err) => mail_error_response(err, "attachment"),
    }
}

// --- D1-6a: mail incremental sync + calendar read-only ---
//
// Mail sync is a manual trigger only: POST
// /api/email/sync reads the inbox window from the
// MailSource (AppleScript on macOS, Mock in tests,
// Unavailable → 501 on non-macOS) and records new
// messages into SQLite (v10 mail_messages). No Effect,
// no approval, no Outbox — fact recording (red line
// 3). The fetch runs outside the SQLite lock
// (osascript calls must not hold it); the write is one
// transaction (rows + events + cursor).
pub(crate) async fn email_sync(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let messages = match crate::mail_sync::fetch_window(state.mail_source.as_ref()) {
        Ok(messages) => messages,
        Err(err) => return mail_error_response(err, "sync"),
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match crate::mail_sync::record_sync(
        &mut conn,
        &messages,
        crate::mail_sync::MAIL_SOURCE_ID,
        &state.origin_device_id,
    ) {
        Ok(result) => Json(serde_json::to_value(result).unwrap_or(serde_json::Value::Null))
            .into_response(),
        Err(detail) => {
            eprintln!("warning: mail sync failed ({detail})");
            sqlite_error_response()
        }
    }
}

// --- D1-6b-1: real mail send + sent readers ---
//
// POST /api/email/send is the one real write in 6b:
// a digest-bound, Mandate-bound, Effect-recorded send
// through Mail.app. "Sent" means Mail.app accepted the
// message, never that it was delivered (Q-D1-6b-06).
// No loop write tools (Q-D1-6b-25); the Mock
// /connectors/mail/send route is untouched.
//
// Route order (the E2-A V1 lesson): source-connected
// check first, then the idempotency replay check
// BEFORE planning a new Effect, then plan (reusing a
// planned row with the same approval_id when one
// exists), consume the bound approval, execute, and
// finish. The digest is checked BEFORE planning, so a
// mismatch leaves no orphaned planned row.
//
// Evidence: there is no readback in 6b-1 — Sent
// mailbox re-check needs a native_id the script may
// not return (Q-D1-6b-08), so `write_time_observation`
// has no ("mail", "send") arm and no Evidence row is
// written; verify stays 409 `effect_no_readback`. The
// sent-N row + `mail.sent` event are the audit trail;
// a readback is a later batch's work, never faked here.

/// D1-6b-1 (Q-D1-6b-24): a disconnected mail-source
/// rejects new writes. Read paths are unaffected. A
/// missing Registry row (seed never ran) is not a
/// disconnect — the write proceeds.
pub(crate) fn mail_source_disconnected(state: &AppState) -> bool {
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let Ok(conn) = sqlite.lock() else {
        return false;
    };
    matches!(
        sqlite_store::get_source(&conn, crate::mail_sync::MAIL_SOURCE_ID),
        Ok(Some(row)) if row.status == "disconnected"
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct EmailSendDto {
    // All four validated fields are Option so a missing
    // field is the route's own 400 (not the extractor's
    // 422): "no key → 400" must hold however the client
    // forgets the key. LOCAL DEFINITION (plan §2).
    to: Option<Vec<String>>,
    subject: Option<String>,
    body: Option<String>,
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
}

pub(crate) fn sent_row_response(
    row: &crate::mail_send::SentMessageRow,
    replayed: bool,
) -> axum::response::Response {
    let mut value = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = value.as_object_mut() {
        obj.insert("replayed".to_string(), serde_json::Value::Bool(replayed));
    }
    Json(value).into_response()
}

pub(crate) async fn email_send(
    State(state): State<AppState>,
    Json(req): Json<EmailSendDto>,
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
    // 1. Validate the DTO: empty to / subject / body /
    // idempotency_key → 400. Plain-text send, no
    // attachments.
    let Some(to_raw) = req.to else {
        return bad_request("to is required");
    };
    let Some(subject) = req.subject.filter(|s| !s.is_empty()) else {
        return bad_request("subject must not be empty");
    };
    let Some(body) = req.body.filter(|s| !s.is_empty()) else {
        return bad_request("body must not be empty");
    };
    let Some(idempotency_key) = req.idempotency_key.filter(|k| !k.trim().is_empty()) else {
        return bad_request("idempotency_key is required");
    };
    let recipients = crate::mail_send::normalize_recipients(&to_raw);
    if recipients.is_empty() {
        return bad_request("to must contain at least one recipient");
    }
    let body_hash = crate::mail_send::mail_body_hash(&body);
    let digest = crate::operation_binding::mail_send_digest(&recipients, &subject, &body_hash);
    // 2. Disconnected source rejects new writes (409).
    if mail_source_disconnected(&state) {
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
        match crate::mail_send::find_by_idempotency_key(&conn, &idempotency_key) {
            Ok(Some(existing)) => {
                if existing.to_json == crate::mail_send::recipients_json(&recipients)
                    && existing.subject == subject
                    && existing.body_hash == body_hash
                {
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
                eprintln!("warning: mail send idempotency lookup failed ({detail})");
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
        if record.tool != "mail.send" {
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
                match crate::mail_send::find_by_idempotency_key(&conn, &idempotency_key) {
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
            create_write_mandate(&state, &execution.execution_id, "mail.send");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "mail",
                "send",
                &idempotency_key,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::mail_send::find_by_idempotency_key(&conn, &idempotency_key) {
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
        "mail.send",
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
    let source = Arc::clone(&state.mail_source);
    let send_recipients = recipients.clone();
    let send_subject = subject.clone();
    let send_body = body.clone();
    let send_result = tokio::task::spawn_blocking(move || {
        source.send(&send_recipients, &send_subject, &send_body)
    })
    .await;
    let outcome = match send_result {
        Ok(result) => result,
        Err(_) => Err(MailError::Timeout),
    };
    let policy_version = current_policy_version_id(&state).unwrap_or_default();
    match outcome {
        Ok(sent) => {
            // Record the fact row + mail.sent event in one
            // transaction FIRST, then finish the Effect:
            // a succeeded Effect must never exist without
            // its fact row.
            let sent_ms = now_ms() as i64;
            let mut conn = sqlite.lock().expect("sqlite lock");
            match crate::mail_send::record_sent(
                &mut conn,
                sent.native_id.as_deref(),
                &idempotency_key,
                &recipients,
                &subject,
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
                                "warning: mail send effect finish failed for {}",
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
                    // mail content was sent, so the existing
                    // row is the replay, never a second row.
                    drop(conn);
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::mail_send::find_by_idempotency_key(&conn, &idempotency_key) {
                        Ok(Some(existing))
                            if existing.to_json
                                == crate::mail_send::recipients_json(&recipients)
                                && existing.subject == subject
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
                            eprintln!("warning: mail send record failed ({detail})");
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
            // Unknown (P0-R): Mail.app may or may not
            // have sent the message, so an
            // unknown_requires_reconciliation Inbox row
            // is opened by the transition itself. Every
            // other gateway-side failure is Failed.
            let (status, error, outcome) = match err {
                MailError::Timeout => (
                    StatusCode::BAD_GATEWAY,
                    "mail source error",
                    LedgerOutcome::Unknown,
                ),
                MailError::NotRunning => (
                    StatusCode::BAD_GATEWAY,
                    "Mail is not running",
                    LedgerOutcome::Failed,
                ),
                MailError::Denied => (
                    StatusCode::FORBIDDEN,
                    "mail automation denied",
                    LedgerOutcome::Failed,
                ),
                MailError::Unavailable(reason) => {
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    return (
                        StatusCode::NOT_IMPLEMENTED,
                        Json(serde_json::json!({ "error": reason, "effect_id": effect.effect_id })),
                    )
                        .into_response();
                }
                _ => (
                    StatusCode::BAD_GATEWAY,
                    "mail source error",
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

pub(crate) async fn email_sent_list(State(state): State<AppState>) -> impl IntoResponse {
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
    match crate::mail_send::list_sent_messages(&conn) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: sent list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn email_sent_get(
    State(state): State<AppState>,
    axum::extract::Path(sent_id): axum::extract::Path<String>,
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
    match crate::mail_send::get_sent_message(&conn, &sent_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown sent message" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: sent get failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// D1-6b-3 (Q-D1-6b-12/13/15/22/23/24): delete one
/// message — Trash only, never purge. `{message_id}` is
/// a local `mail-N` or a native id; the digest binds
/// the resolved native id. Tombstone only: the row is
/// never hard-deleted (a hard delete would resurrect
/// the mail on next sync — the `(source_id, native_id)`
/// UNIQUE dedupe). A re-delete of a tombstoned row
/// returns 200 with the current row and no second event,
/// no second Effect. Deleting never-synced mail is
/// allowed (Q-D1-6b-15): the real-mailbox delete
/// executes, no empty fact row is created, and the
/// response is the minimal delete record below.
/// D1-6b-3: one delete outcome (LOCAL DEFINITION).
/// `message_id` is the local `mail-N` when the message
/// was synced, `None` when the deleted mail was never
/// synced (Q-D1-6b-15: no empty fact row). `effect_id`
/// is `None` only on a tombstone replay whose original
/// Effect could not be found (the row carries no
/// effect id).
#[derive(Debug, Serialize)]
pub(crate) struct MailDeleteResponse {
    message_id: Option<String>,
    source_id: String,
    native_id: String,
    deleted: bool,
    effect_id: Option<String>,
    replayed: bool,
}

pub(crate) fn delete_response(
    row: Option<&crate::mail_sync::MailMessageRow>,
    native_id: &str,
    effect_id: Option<String>,
    replayed: bool,
) -> axum::response::Response {
    Json(MailDeleteResponse {
        message_id: row.map(|r| r.message_id.clone()),
        source_id: crate::mail_sync::MAIL_SOURCE_ID.to_string(),
        native_id: native_id.to_string(),
        deleted: true,
        effect_id,
        replayed,
    })
    .into_response()
}

/// The succeeded delete Effect for one native id, when
/// one exists (the replay branch's effect id — the
/// tombstoned row itself carries no effect id).
pub(crate) fn delete_effect_id_for(conn: &rusqlite::Connection, native_id: &str) -> Option<String> {
    sqlite_store::list_effects(conn).ok().and_then(|effects| {
        effects
            .into_iter()
            .find(|e| {
                e.capability == "mail"
                    && e.operation == "delete"
                    && e.scope == native_id
                    && e.status == "succeeded"
            })
            .map(|e| e.effect_id)
    })
}

pub(crate) async fn email_message_delete(
    State(state): State<AppState>,
    axum::extract::Path(message_id): axum::extract::Path<String>,
    axum::extract::Query(query): axum::extract::Query<DeleteQuery>,
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
    // 1. Resolve the id: local `mail-N` first, then a row
    // by native id, then a bare numeric native id (a
    // never-synced message). Anything else is unknown.
    let (row, native_id) = {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::mail_sync::resolve_delete_target(&conn, &message_id) {
            Ok(Some(resolved)) => resolved,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown message" })),
                )
                    .into_response();
            }
            Err(detail) => {
                eprintln!("warning: mail delete resolve failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    let digest = crate::operation_binding::mail_delete_digest(&native_id);
    // 2. Disconnected source rejects new writes (409).
    if mail_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 3. Tombstone check BEFORE planning a new Effect: an
    // already-tombstoned row is the replay — 200 current
    // row, no second event, no second Effect.
    if let Some(row) = &row {
        if row.deleted {
            let conn = sqlite.lock().expect("sqlite lock");
            let effect_id = delete_effect_id_for(&conn, &native_id);
            return delete_response(Some(row), &native_id, effect_id, true);
        }
    }
    // 4. Approval peek (read-only, nothing burned): the
    // digest is checked BEFORE planning, so a mismatch
    // leaves no orphaned planned row and the approval
    // stays unconsumed for a correct retry (the 6b-1
    // SD3 shape).
    let Some(approval_id) = query.tool_call_id.clone() else {
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
        if record.tool != "mail.delete" {
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
                // still the original row, never a re-delete.
                if let Some(row) = &row {
                    return delete_response(
                        Some(row),
                        &native_id,
                        Some(effect.effect_id.clone()),
                        true,
                    );
                }
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_terminal", "effect_id": effect.effect_id, "status": effect.status })),
                )
                    .into_response();
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
            create_write_mandate(&state, &execution.execution_id, "mail.delete");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "mail",
                "delete",
                &native_id,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    if let Some(row) = &row {
                        return delete_response(
                            Some(row),
                            &native_id,
                            Some(other.effect_id.clone()),
                            true,
                        );
                    }
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
            }
        }
    };
    // 6. Consume the bound approval (binding + push to
    // authorized), then drive the Effect to started.
    if let Err(response) = consume_bound_approval_for_effect(
        &state,
        "mail.delete",
        &query.tool_call_id,
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
    // as device_set and the 6b-1 send. The native id is
    // cloned for the blocking task; the resolved row is
    // still needed below for the tombstone.
    let source = Arc::clone(&state.mail_source);
    let delete_id = native_id.clone();
    let delete_result =
        tokio::task::spawn_blocking(move || source.delete(&delete_id)).await;
    let outcome = match delete_result {
        Ok(result) => result,
        Err(_) => Err(MailError::Timeout),
    };
    match outcome {
        Ok(()) => {
            // Record the tombstone + mail.deleted event in
            // one transaction FIRST, then finish the
            // Effect: a succeeded Effect must never exist
            // without its fact record. A never-synced
            // delete records the event alone (no empty
            // fact row, Q-D1-6b-15).
            let mut conn = sqlite.lock().expect("sqlite lock");
            let recorded = match &row {
                Some(row) => crate::mail_sync::tombstone_mail_message(
                    &mut conn,
                    &row.message_id,
                    &execution_id,
                    &state.origin_device_id,
                )
                .map(|_| ()),
                None => crate::mail_sync::record_deleted_no_row(
                    &mut conn,
                    &native_id,
                    &execution_id,
                    &state.origin_device_id,
                ),
            };
            match recorded {
                Ok(()) => {
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => delete_response(
                            row.as_ref(),
                            &native_id,
                            Some(effect.effect_id.clone()),
                            false,
                        ),
                        Err(_) => {
                            eprintln!(
                                "warning: mail delete effect finish failed for {}",
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
                    eprintln!("warning: mail delete record failed ({detail})");
                    drop(conn);
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    sqlite_error_response()
                }
            }
        }
        Err(err) => {
            // Only a killed/ambiguous script run is
            // Unknown (P0-R): Mail.app may or may not
            // have moved the message, so an
            // unknown_requires_reconciliation Inbox row
            // is opened by the transition itself. A
            // lookup miss is Failed: the message is not
            // in any live mailbox, so there was nothing
            // to delete. Every other gateway-side failure
            // is Failed.
            let (status, error, outcome) = match err {
                MailError::Timeout => (
                    StatusCode::BAD_GATEWAY,
                    "mail source error",
                    LedgerOutcome::Unknown,
                ),
                MailError::NotRunning => (
                    StatusCode::BAD_GATEWAY,
                    "Mail is not running",
                    LedgerOutcome::Failed,
                ),
                MailError::NotFound(_) => (
                    StatusCode::NOT_FOUND,
                    "unknown message",
                    LedgerOutcome::Failed,
                ),
                MailError::Denied => (
                    StatusCode::FORBIDDEN,
                    "mail automation denied",
                    LedgerOutcome::Failed,
                ),
                MailError::Unavailable(reason) => {
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    return (
                        StatusCode::NOT_IMPLEMENTED,
                        Json(serde_json::json!({ "error": reason, "effect_id": effect.effect_id })),
                    )
                        .into_response();
                }
                _ => (
                    StatusCode::BAD_GATEWAY,
                    "mail source error",
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

pub(crate) async fn email_synced_list(State(state): State<AppState>) -> impl IntoResponse {
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
    match crate::mail_sync::list_mail_messages(&conn, crate::mail_sync::MAIL_SOURCE_ID) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: synced list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn email_synced_get(
    State(state): State<AppState>,
    axum::extract::Path(message_id): axum::extract::Path<String>,
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
    match crate::mail_sync::get_mail_message(&conn, &message_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown synced message" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: synced get failed ({detail})");
            sqlite_error_response()
        }
    }
}
