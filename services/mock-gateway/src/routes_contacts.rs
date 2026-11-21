//! Contacts routes, MCL-2 + MCL-5.
//!
//! Seven routes, the mail read-batch 1 pattern
//! (`routes_mail.rs` is the model, `routes_imessage.rs`
//! is the nearest copy) for the first four, the 6b-1
//! send pattern for the last three:
//!
//! - `POST /api/contacts/sync` — manual trigger, fact
//!   recording only (no Effect, no approval, no Outbox,
//!   like `POST /api/email/sync` and
//!   `POST /api/imessage/sync`). Reads the live source
//!   and writes the `contacts` table + the
//!   `contact.synced` events in one transaction. The
//!   fetch runs outside the SQLite lock (the source read
//!   is synchronous, like `fetch_window`).
//! - `GET /api/contacts` — list synced contacts from
//!   SQLite (default 50, max 200), a bare array like
//!   `GET /api/email/synced`; tombstones are visible
//!   (6b-3 Q-14).
//! - `GET /api/contacts/{id}` — one synced contact by
//!   local `contact-N` id, a bare row like
//!   `GET /api/email/synced/{id}`; tombstones are
//!   returned, unknown ids are 404.
//! - `GET /api/contacts/search?q=` — search synced
//!   contacts (SQLite list + in-memory filter over
//!   display_name, phones, and emails —
//!   case-insensitive substring; see
//!   `contacts_sync::search_contacts`). A missing or
//!   empty `q` behaves like the list route (same limit /
//!   offset). Address / birthday values are never
//!   stored, so they cannot be searched (Q-MCL-11).
//! - `POST /api/contacts/create` — MCL-5: create one
//!   card in Contacts.app (Q-MCL-22), the 6b-1 send
//!   order — 400, disconnect 409, idempotency replay
//!   before planning, approval peek, reuse-or-plan,
//!   consume, execute, finish. The fact row +
//!   `contact.created` event land in one transaction.
//! - `POST /api/contacts/{id}/update` — MCL-5: full
//!   replace of one card's writable fields (Q-MCL-22),
//!   the 6b-4 append order — the live conflict re-read
//!   runs after the approval peek and BEFORE planning,
//!   so a changed base is 409 `contact_changed` with
//!   the approval NOT burned.
//! - `POST /api/contacts/{id}/delete` — MCL-5: delete
//!   one card (Q-MCL-13), the 6b-3 delete order — the
//!   tombstone check runs before the approval peek, so
//!   a re-delete is 200 with the current row, no second
//!   event, no second Effect. Tombstone only: the row
//!   is never hard-deleted, so a later sync cannot
//!   resurrect it.

use std::sync::Arc;

use agent_desktop::approval::ApprovalStatus;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::contacts_source::{clamp_limit, ContactContent, ContactsError};
use crate::contacts_sync::{
    find_contact_by_idempotency_key, get_contact, get_source_cursor, list_contacts,
    record_contact_created, record_contact_deleted, record_contact_updated, record_sync,
    search_contacts, CONTACTS_SOURCE_ID,
};
use crate::operation_binding::{
    contact_content_hash, contact_create_digest, contact_delete_digest, contact_update_digest,
};
use crate::routes_common::{bad_request, sqlite_error_response, sqlite_not_configured};
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

#[derive(Deserialize, Debug)]
pub struct SearchParams {
    pub q: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Map a source failure onto the shared read-path
/// codes: unavailable -> 501, denied -> 403, not
/// running / upstream / timeout -> 502 (the mail
/// `mail_error_response` precedent, with contacts
/// wording — never a mail error string).
pub(crate) fn contacts_error_response(error: &ContactsError, what: &str) -> Response {
    let (status, detail) = match error {
        ContactsError::NotFound(detail) => (StatusCode::NOT_FOUND, detail.to_string()),
        ContactsError::Unavailable(detail) => (StatusCode::NOT_IMPLEMENTED, detail.clone()),
        ContactsError::Denied => (
            StatusCode::FORBIDDEN,
            "contacts automation denied (check Contacts automation permission)".to_string(),
        ),
        ContactsError::Upstream(detail) => (StatusCode::BAD_GATEWAY, detail.clone()),
        ContactsError::NotRunning => (
            StatusCode::BAD_GATEWAY,
            "Contacts is not running".to_string(),
        ),
        ContactsError::Timeout => (
            StatusCode::BAD_GATEWAY,
            "contacts read timed out".to_string(),
        ),
    };
    (
        status,
        Json(serde_json::json!({
            "error": format!("contacts {what} failed: {detail}"),
            "code": "contacts_source_error",
        })),
    )
        .into_response()
}

pub(crate) async fn contacts_sync(State(state): State<AppState>) -> impl IntoResponse {
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
    let contacts = match state.contacts_source.contacts(usize::MAX, 0) {
        Ok(contacts) => contacts,
        Err(error) => return contacts_error_response(&error, "sync"),
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match record_sync(
        &mut conn,
        &contacts,
        CONTACTS_SOURCE_ID,
        &state.origin_device_id,
    ) {
        Ok(result) => {
            Json(serde_json::to_value(result).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: contacts sync failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn contacts_list(
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
    match list_contacts(&conn, CONTACTS_SOURCE_ID, limit, offset) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: contacts list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn contacts_get(
    State(state): State<AppState>,
    Path(contact_id): Path<String>,
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
    match get_contact(&conn, &contact_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown contact" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: contact get failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn contacts_search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
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
    let query = params.q.as_deref().unwrap_or("");
    let conn = sqlite.lock().expect("sqlite lock");
    match search_contacts(&conn, CONTACTS_SOURCE_ID, query, limit, offset) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: contacts search failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// The stored source cursor (used by tests asserting
/// the cursor advances across syncs).
pub fn get_contacts_cursor(conn: &rusqlite::Connection) -> Result<String, String> {
    get_source_cursor(conn, CONTACTS_SOURCE_ID)
}

/// MCL-5: a disconnected contacts-source rejects new
/// writes with 409 `source_disconnected` (the 6b-1
/// `mail_source_disconnected` precedent). Reads are
/// unaffected, and a missing Registry row (seed never
/// ran) is not a disconnect — the write proceeds.
pub(crate) fn contacts_source_disconnected(state: &AppState) -> bool {
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let Ok(conn) = sqlite.lock() else {
        return false;
    };
    matches!(
        sqlite_store::get_source(&conn, CONTACTS_SOURCE_ID),
        Ok(Some(row)) if row.status == "disconnected"
    )
}

/// Serialize one contact row plus the `replayed` flag
/// (the `sent_row_response` / `note_row_response`
/// shape): a replay returns the original row with
/// `replayed: true`, a fresh write with `false`.
pub(crate) fn contact_row_response(
    row: &crate::contacts_sync::ContactRow,
    replayed: bool,
) -> axum::response::Response {
    let mut value = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = value.as_object_mut() {
        obj.insert("replayed".to_string(), serde_json::Value::Bool(replayed));
    }
    Json(value).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct ContactCreateDto {
    // All validated fields are Option so a missing
    // field is the route's own 400 (not the extractor's
    // 422): "no key → 400" must hold however the client
    // forgets the key. LOCAL DEFINITION (plan §6, the
    // 6b-1 `EmailSendDto` precedent). `display_name` is
    // the only required content field (a card with just
    // a name is valid); organization / job_title default
    // to "", phones / emails to [].
    display_name: Option<String>,
    organization: Option<String>,
    job_title: Option<String>,
    phones: Option<Vec<String>>,
    emails: Option<Vec<String>>,
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ContactUpdateDto {
    // Same Option rule as the create DTO. The update is
    // a full replace: display_name is required (a card
    // with an empty name is not a card), the rest
    // default like the create. `base_content_hash` is
    // required: the conflict check compares the live
    // card against the version the approver saw.
    display_name: Option<String>,
    organization: Option<String>,
    job_title: Option<String>,
    phones: Option<Vec<String>>,
    emails: Option<Vec<String>>,
    base_content_hash: Option<String>,
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ContactDeleteDto {
    // Same Option rule as the create DTO. The delete
    // takes a JSON body (the MCL-4 send precedent), not
    // a query parameter: all three MCL-5 writes carry
    // their idempotency key + approval id the same way.
    // (The 6b-3 mail delete uses a query parameter; the
    // MCL-5 routes follow the MCL-4 body shape so the
    // three writes share one client call shape.)
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
}

/// Map a write-execution source failure onto the
/// response + ledger outcome (the MCL-4 send mapping):
/// only a killed / ambiguous script run is Unknown
/// (P0-R) — Contacts.app may or may not have applied
/// the write, so an unknown_requires_reconciliation
/// Inbox row is opened by the transition itself. Every
/// other gateway-side failure is Failed. The caller
/// finishes the Effect with the returned outcome and
/// renders the returned response.
fn write_execution_error(
    err: ContactsError,
    what: &str,
    effect_id: &str,
) -> (Response, LedgerOutcome) {
    let (status, error, outcome) = match err {
        ContactsError::Timeout => (
            StatusCode::BAD_GATEWAY,
            format!("contacts {what} timed out"),
            LedgerOutcome::Unknown,
        ),
        ContactsError::NotRunning => (
            StatusCode::BAD_GATEWAY,
            "Contacts is not running".to_string(),
            LedgerOutcome::Failed,
        ),
        ContactsError::Denied => (
            StatusCode::FORBIDDEN,
            "contacts automation denied".to_string(),
            LedgerOutcome::Failed,
        ),
        ContactsError::NotFound(_) => (
            StatusCode::NOT_FOUND,
            "unknown contact".to_string(),
            LedgerOutcome::Failed,
        ),
        ContactsError::Upstream(_) => (
            StatusCode::BAD_GATEWAY,
            "contacts source error".to_string(),
            LedgerOutcome::Failed,
        ),
        ContactsError::Unavailable(reason) => {
            return (
                (
                    StatusCode::NOT_IMPLEMENTED,
                    Json(serde_json::json!({ "error": reason, "effect_id": effect_id })),
                )
                    .into_response(),
                LedgerOutcome::Failed,
            );
        }
    };
    (
        (
            status,
            Json(serde_json::json!({ "error": error, "effect_id": effect_id })),
        )
            .into_response(),
        outcome,
    )
}

pub(crate) async fn contacts_create(
    State(state): State<AppState>,
    Json(req): Json<ContactCreateDto>,
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
    // 1. Validate the DTO: an empty display_name /
    // idempotency_key → 400. The content is normalized
    // once here (trim / sort / dedupe) and the
    // normalized form is what the digest, the script,
    // and the stored row all see.
    let Some(display_name) = req.display_name.filter(|s| !s.trim().is_empty()) else {
        return bad_request("display_name is required");
    };
    let Some(idempotency_key) = req.idempotency_key.filter(|k| !k.trim().is_empty()) else {
        return bad_request("idempotency_key is required");
    };
    let content = ContactContent::normalized(
        &display_name,
        req.organization.as_deref().unwrap_or(""),
        req.job_title.as_deref().unwrap_or(""),
        req.phones.as_deref().unwrap_or(&[]),
        req.emails.as_deref().unwrap_or(&[]),
    );
    let digest = contact_create_digest(
        &content.display_name,
        &content.organization,
        &content.job_title,
        &content.phones,
        &content.emails,
    );
    // 2. Disconnected source rejects new writes (409).
    if contacts_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 3. Idempotency replay BEFORE planning a new Effect:
    // same key + same content → 200 original row, no
    // second card, no second Effect; same key +
    // different content → 409.
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match find_contact_by_idempotency_key(&conn, &idempotency_key, "create", &digest, None) {
            Err(detail) if detail == "idempotency_key_conflict" => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                )
                    .into_response()
            }
            Ok(Some(existing)) => {
                return contact_row_response(&existing, true);
            }
            Ok(None) => {}
            Err(detail) => {
                eprintln!("warning: contacts create idempotency lookup failed ({detail})");
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
        if record.tool != "contacts.create" {
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
    // NOT used for 6b writes. The scope is the
    // idempotency key: the contact has no id yet.
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
                // 返回当前联系人行，不执行第二次
                // card.
                let conn = sqlite.lock().expect("sqlite lock");
                match find_contact_by_idempotency_key(
                    &conn,
                    &idempotency_key,
                    "create",
                    &digest,
                    None,
                ) {
                    Err(detail) if detail == "idempotency_key_conflict" => {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                        )
                            .into_response()
                    }
                    Ok(Some(existing)) => return contact_row_response(&existing, true),
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
            create_write_mandate(&state, &execution.execution_id, "contacts.create");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "contacts",
                "create",
                &idempotency_key,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    match find_contact_by_idempotency_key(
                        &conn,
                        &idempotency_key,
                        "create",
                        &digest,
                        None,
                    ) {
                        Err(detail) if detail == "idempotency_key_conflict" => {
                            return (
                                StatusCode::CONFLICT,
                                Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                            )
                                .into_response()
                        }
                        Ok(Some(existing)) => return contact_row_response(&existing, true),
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
        "contacts.create",
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
    // call can take up to 120 s. Same spawn_blocking
    // shape as device_set. The content is cloned for the
    // blocking task.
    let source = Arc::clone(&state.contacts_source);
    let create_content = content.clone();
    let create_result =
        tokio::task::spawn_blocking(move || source.create_contact(&create_content)).await;
    let outcome = match create_result {
        Ok(result) => result,
        Err(_) => Err(ContactsError::Timeout),
    };
    let policy_version = current_policy_version_id(&state).unwrap_or_default();
    match outcome {
        Ok(native_id) => {
            // Record the fact row + contact.created
            // event in one transaction FIRST, then
            // finish the Effect: a succeeded Effect must
            // never exist without its fact row. A native
            // id that already has a row (a concurrent
            // create, or a lost response retried with a
            // new key) is the replay, never a second row.
            let mut conn = sqlite.lock().expect("sqlite lock");
            match record_contact_created(
                &mut conn,
                &native_id,
                &content,
                &idempotency_key,
                &effect.effect_id,
                &execution_id,
                &policy_version,
                &state.origin_device_id,
            ) {
                Ok((row, inserted)) => {
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => contact_row_response(&row, !inserted),
                        Err(_) => {
                            eprintln!(
                                "warning: contacts create effect finish failed for {}",
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
                    eprintln!("warning: contacts create record failed ({detail})");
                    drop(conn);
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    sqlite_error_response()
                }
            }
        }
        Err(err) => {
            let (response, outcome) = write_execution_error(err, "create", &effect.effect_id);
            let _ = ledger_entry_finish(&state, &effect.effect_id, outcome);
            response
        }
    }
}

pub(crate) async fn contacts_update(
    State(state): State<AppState>,
    Path(contact_id): Path<String>,
    Json(req): Json<ContactUpdateDto>,
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
    // 1. Validate the DTO: an empty display_name /
    // base_content_hash / idempotency_key → 400. The
    // content is normalized once here and the normalized
    // form is what the digest, the script, and the
    // stored row all see.
    let Some(display_name) = req.display_name.filter(|s| !s.trim().is_empty()) else {
        return bad_request("display_name is required");
    };
    let Some(base_content_hash) = req.base_content_hash.filter(|s| !s.is_empty()) else {
        return bad_request("base_content_hash is required");
    };
    let Some(idempotency_key) = req.idempotency_key.filter(|k| !k.trim().is_empty()) else {
        return bad_request("idempotency_key is required");
    };
    // 先解析联系人；墓碑在历史键重放后再拒绝新更新。
    let row = {
        let conn = sqlite.lock().expect("sqlite lock");
        match get_contact(&conn, &contact_id) {
            Ok(Some(row)) => row,
            Ok(_) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown contact" })),
                )
                    .into_response();
            }
            Err(detail) => {
                eprintln!("warning: contacts update resolve failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    let content = ContactContent::normalized(
        &display_name,
        req.organization.as_deref().unwrap_or(""),
        req.job_title.as_deref().unwrap_or(""),
        req.phones.as_deref().unwrap_or(&[]),
        req.emails.as_deref().unwrap_or(&[]),
    );
    let digest = contact_update_digest(
        &contact_id,
        &content.display_name,
        &content.organization,
        &content.job_title,
        &content.phones,
        &content.emails,
    );
    // 3. Disconnected source rejects new writes (409).
    if contacts_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 4. Idempotency replay BEFORE planning a new Effect:
    // same key + same content on the same contact → 200
    // 返回当前行（含墓碑），不再写入、不创建第二个 Effect。
    // same key on another contact, or with different
    // content → 409.
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match find_contact_by_idempotency_key(
            &conn,
            &idempotency_key,
            "update",
            &digest,
            Some(&contact_id),
        ) {
            Err(detail) if detail == "idempotency_key_conflict" => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                )
                    .into_response()
            }
            Ok(Some(existing)) => {
                return contact_row_response(&existing, true);
            }
            Ok(None) => {}
            Err(detail) => {
                eprintln!("warning: contacts update idempotency lookup failed ({detail})");
                return sqlite_error_response();
            }
        }
    }
    // 仅已完成写入的历史键可返回墓碑；未命中幂等记录的新更新仍拒绝。
    if row.deleted {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "unknown contact"})),
        )
            .into_response();
    }
    // 5. Approval peek (read-only, nothing burned).
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
        if record.tool != "contacts.update" {
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
    // 6. Conflict re-read BEFORE planning (Q-D1-6b-20,
    // the 6b-4 append step 6): re-read the LIVE card
    // and compare its content hash with the approved
    // base. Changed → 409 `contact_changed` with the
    // fresh base hash, no write, approval NOT burned —
    // the client re-reads the base from this response
    // and retries. Gone → 404. Any other backend error
    // returns the mapped error before planning: the
    // live card is load-bearing for the write, so a
    // failed re-read cannot fall through to a replace
    // against an unknown base. The check runs outside
    // the SQLite lock. A residual TOCTOU window remains
    // between this read and the set (planning +
    // consume, milliseconds, no lock held) — the plan's
    // "not burned" rule forces the check before consume,
    // so the window is inherent, not a defect.
    {
        let source = Arc::clone(&state.contacts_source);
        let check_native = row.native_id.clone();
        let read_result =
            tokio::task::spawn_blocking(move || source.contact_content(&check_native)).await;
        match read_result {
            Ok(Ok(Some(live))) => {
                let live_hash = contact_content_hash(
                    &live.display_name,
                    &live.organization,
                    &live.job_title,
                    &live.phones,
                    &live.emails,
                );
                if live_hash != base_content_hash {
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({
                            "error": "contact_changed",
                            "current_hash": live_hash,
                        })),
                    )
                        .into_response();
                }
            }
            Ok(Ok(None)) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown contact" })),
                )
                    .into_response();
            }
            Ok(Err(err)) => return contacts_error_response(&err, "update"),
            Err(_) => return contacts_error_response(&ContactsError::Timeout, "update"),
        }
    }
    // 7. Reuse-or-plan (E2-A V1). The scope is the
    // contact id (the notes.append precedent).
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
                // 返回当前联系人行，不执行第二次
                // write.
                let conn = sqlite.lock().expect("sqlite lock");
                match find_contact_by_idempotency_key(
                    &conn,
                    &idempotency_key,
                    "update",
                    &digest,
                    Some(&contact_id),
                ) {
                    Err(detail) if detail == "idempotency_key_conflict" => {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                        )
                            .into_response()
                    }
                    Ok(Some(existing)) => return contact_row_response(&existing, true),
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
            create_write_mandate(&state, &execution.execution_id, "contacts.update");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "contacts",
                "update",
                &contact_id,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    match find_contact_by_idempotency_key(
                        &conn,
                        &idempotency_key,
                        "update",
                        &digest,
                        Some(&contact_id),
                    ) {
                        Err(detail) if detail == "idempotency_key_conflict" => {
                            return (
                                StatusCode::CONFLICT,
                                Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                            )
                                .into_response()
                        }
                        Ok(Some(existing)) => return contact_row_response(&existing, true),
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
    // 8. Consume the bound approval (binding + push to
    // authorized), then drive the Effect to started.
    if let Err(response) = consume_bound_approval_for_effect(
        &state,
        "contacts.update",
        &req.tool_call_id,
        &digest,
        Some(&effect.effect_id),
    ) {
        return *response;
    }
    if let Err(response) = start_bound_effect(&state, &effect) {
        return *response;
    }
    // 9. Execute outside the SQLite lock: the AppleScript
    // call can take up to 120 s. Same spawn_blocking
    // shape as device_set. The native id + content are
    // cloned for the blocking task; the row is still
    // needed below for the fact record.
    let source = Arc::clone(&state.contacts_source);
    let update_native = row.native_id.clone();
    let update_content = content.clone();
    let update_result =
        tokio::task::spawn_blocking(move || source.update_contact(&update_native, &update_content))
            .await;
    let outcome = match update_result {
        Ok(result) => result,
        Err(_) => Err(ContactsError::Timeout),
    };
    let policy_version = current_policy_version_id(&state).unwrap_or_default();
    match outcome {
        Ok(()) => {
            // Record the updated row + contact.updated
            // event in one transaction FIRST, then
            // finish the Effect: a succeeded Effect must
            // never exist without its fact record.
            let mut conn = sqlite.lock().expect("sqlite lock");
            match record_contact_updated(
                &mut conn,
                &row,
                &content,
                &idempotency_key,
                &effect.effect_id,
                &execution_id,
                &policy_version,
                &state.origin_device_id,
            ) {
                Ok(updated) => {
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => contact_row_response(&updated, false),
                        Err(_) => {
                            eprintln!(
                                "warning: contacts update effect finish failed for {}",
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
                    eprintln!("warning: contacts update record failed ({detail})");
                    drop(conn);
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    sqlite_error_response()
                }
            }
        }
        Err(err) => {
            let (response, outcome) = write_execution_error(err, "update", &effect.effect_id);
            let _ = ledger_entry_finish(&state, &effect.effect_id, outcome);
            response
        }
    }
}

pub(crate) async fn contacts_delete(
    State(state): State<AppState>,
    Path(contact_id): Path<String>,
    Json(req): Json<ContactDeleteDto>,
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
    // 1. Validate the DTO: an empty idempotency_key →
    // 400.
    let Some(idempotency_key) = req.idempotency_key.filter(|k| !k.trim().is_empty()) else {
        return bad_request("idempotency_key is required");
    };
    // 2. Resolve the row: unknown id is 404. A
    // tombstoned row DOES resolve here (unlike the
    // update): the delete replays it below. This is
    // input validation, so it runs before the
    // disconnect gate.
    let row = {
        let conn = sqlite.lock().expect("sqlite lock");
        match get_contact(&conn, &contact_id) {
            Ok(Some(row)) => row,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown contact" })),
                )
                    .into_response();
            }
            Err(detail) => {
                eprintln!("warning: contacts delete resolve failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    let digest = contact_delete_digest(&contact_id);
    // 3. Disconnected source rejects new writes (409).
    if contacts_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 4. Idempotency replay BEFORE planning a new Effect:
    // same key on the same contact → 200 original row,
    // no second delete, no second Effect; same key on
    // another contact → 409.
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match find_contact_by_idempotency_key(
            &conn,
            &idempotency_key,
            "delete",
            &digest,
            Some(&contact_id),
        ) {
            Err(detail) if detail == "idempotency_key_conflict" => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                )
                    .into_response()
            }
            Ok(Some(existing)) => {
                return contact_row_response(&existing, true);
            }
            Ok(None) => {}
            Err(detail) => {
                eprintln!("warning: contacts delete idempotency lookup failed ({detail})");
                return sqlite_error_response();
            }
        }
    }
    // 5. Tombstone check BEFORE planning a new Effect:
    // an already-tombstoned row is the replay — 200
    // current row, no second event, no second Effect
    // (the 6b-3 mail delete step 3). No approval is
    // needed for a re-delete.
    if row.deleted {
        return contact_row_response(&row, true);
    }
    // 6. Approval peek (read-only, nothing burned).
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
        if record.tool != "contacts.delete" {
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
    // 7. Reuse-or-plan (E2-A V1). The scope is the
    // contact id (the notes.delete precedent).
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
                // 返回当前联系人行，不执行第二次
                // delete.
                let conn = sqlite.lock().expect("sqlite lock");
                match find_contact_by_idempotency_key(
                    &conn,
                    &idempotency_key,
                    "delete",
                    &digest,
                    Some(&contact_id),
                ) {
                    Err(detail) if detail == "idempotency_key_conflict" => {
                        return (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                        )
                            .into_response()
                    }
                    Ok(Some(existing)) => return contact_row_response(&existing, true),
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
            create_write_mandate(&state, &execution.execution_id, "contacts.delete");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "contacts",
                "delete",
                &contact_id,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    match find_contact_by_idempotency_key(
                        &conn,
                        &idempotency_key,
                        "delete",
                        &digest,
                        Some(&contact_id),
                    ) {
                        Err(detail) if detail == "idempotency_key_conflict" => {
                            return (
                                StatusCode::CONFLICT,
                                Json(serde_json::json!({"error": "idempotency_key_conflict"})),
                            )
                                .into_response()
                        }
                        Ok(Some(existing)) => return contact_row_response(&existing, true),
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
    // 8. Consume the bound approval (binding + push to
    // authorized), then drive the Effect to started.
    if let Err(response) = consume_bound_approval_for_effect(
        &state,
        "contacts.delete",
        &req.tool_call_id,
        &digest,
        Some(&effect.effect_id),
    ) {
        return *response;
    }
    if let Err(response) = start_bound_effect(&state, &effect) {
        return *response;
    }
    // 9. Execute outside the SQLite lock: the AppleScript
    // call can take up to 120 s. Same spawn_blocking
    // shape as device_set. The native id is cloned for
    // the blocking task; the row is still needed below
    // for the tombstone.
    let source = Arc::clone(&state.contacts_source);
    let delete_native = row.native_id.clone();
    let delete_result =
        tokio::task::spawn_blocking(move || source.delete_contact(&delete_native)).await;
    let outcome = match delete_result {
        Ok(result) => result,
        Err(_) => Err(ContactsError::Timeout),
    };
    let policy_version = current_policy_version_id(&state).unwrap_or_default();
    match outcome {
        Ok(()) => {
            // Record the tombstone + contact.deleted
            // event in one transaction FIRST, then
            // finish the Effect: a succeeded Effect must
            // never exist without its fact record.
            let mut conn = sqlite.lock().expect("sqlite lock");
            match record_contact_deleted(
                &mut conn,
                &row,
                &idempotency_key,
                &effect.effect_id,
                &execution_id,
                &policy_version,
                &state.origin_device_id,
            ) {
                Ok(tombstoned) => {
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => contact_row_response(&tombstoned, false),
                        Err(_) => {
                            eprintln!(
                                "warning: contacts delete effect finish failed for {}",
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
                    eprintln!("warning: contacts delete record failed ({detail})");
                    drop(conn);
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    sqlite_error_response()
                }
            }
        }
        Err(err) => {
            let (response, outcome) = write_execution_error(err, "delete", &effect.effect_id);
            let _ = ledger_entry_finish(&state, &effect.effect_id, outcome);
            response
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contacts_error_response_maps_each_variant() {
        // NotRunning -> 502 with contacts wording;
        // Denied -> 403; Unavailable -> 501.
        let running = contacts_error_response(&ContactsError::NotRunning, "sync");
        assert_eq!(running.status(), StatusCode::BAD_GATEWAY);
        let denied = contacts_error_response(&ContactsError::Denied, "sync");
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let unavailable =
            contacts_error_response(&ContactsError::Unavailable("no source".to_string()), "sync");
        assert_eq!(unavailable.status(), StatusCode::NOT_IMPLEMENTED);
        let upstream =
            contacts_error_response(&ContactsError::Upstream("boom".to_string()), "sync");
        assert_eq!(upstream.status(), StatusCode::BAD_GATEWAY);
    }
}
