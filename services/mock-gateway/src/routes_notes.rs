//! Notes routes (D1-6b-4): /api/notes sync, list, get, create, append, delete.
//!
//! The handlers moved here from `server.rs` (FU-5 S-1). The rest of the
//! route directory lives in `server.rs`. There is no `//!` directory line
//! for notes in the `server.rs` header, so no directory line moved with
//! this batch; the section's own `///` doc comment moved with its code
//! below. The shared `DeleteQuery` DTO and the generic sqlite response
//! helpers live in `routes_common.rs`; the ledger helpers
//! (`ledger_entry`, `effect_for_approval`, ...) stay in `server.rs`
//! for the S-9g ledger batch and are imported from `crate::server`.

use std::sync::Arc;

use agent_desktop::approval::ApprovalStatus;
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::notes_source::NotesError;
use crate::routes_common::{
    bad_request, sqlite_error_response, sqlite_not_configured, DeleteQuery,
};
use crate::server::{
    consume_bound_approval_for_effect, create_execution_with_binding, create_write_mandate,
    current_policy_version_id, effect_for_approval, ledger_entry, ledger_entry_finish,
    start_bound_effect, AppState, LedgerEntryDecision, LedgerOutcome,
};
use crate::sqlite_store::{self, StateBackend};

/// D1-6b-4: Notes read + write (SQLite v13).
///
/// POST /api/notes/sync is the manual-only read (no
/// auto-trigger before A1); GET /api/notes[/{id}] are
/// the readers; POST /api/notes/create|{id}/append|
/// {id}/delete are the digest-bound, Mandate-bound,
/// Effect-recorded writes (Q-D1-6b-22/23). No loop
/// write tools (Q-D1-6b-25); the Mock notes routes are
/// untouched.
///
/// Route order (the E2-A V1 lesson, the 6b-1/6b-2/6b-3
/// shape): source-connected check first, then the
/// idempotency / tombstone replay check BEFORE planning
/// a new Effect, then plan (reusing a planned row with
/// the same approval_id when one exists), consume the
/// bound approval, execute, and finish. The digest is
/// checked BEFORE planning, so a mismatch leaves no
/// orphaned planned row and the approval stays
/// unconsumed for a correct retry (the 6b-1 SD3 shape).
///
/// Evidence: create / append verify by re-reading the
/// note's `plaintext` hash against the fact row;
/// delete verifies by absence. Without a working
/// readback, verify stays 409 `effect_no_readback` —
/// never faked (see `evidence.rs`).
///
/// D1-6b-4 (Q-D1-6b-24): a disconnected notes-source
/// rejects new writes. Read paths are unaffected. A
/// missing Registry row (seed never ran) is not a
/// disconnect — the write proceeds.
fn notes_source_disconnected(state: &AppState) -> bool {
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let Ok(conn) = sqlite.lock() else {
        return false;
    };
    matches!(
        sqlite_store::get_source(&conn, crate::notes_source::NOTES_SOURCE_ID),
        Ok(Some(row)) if row.status == "disconnected"
    )
}

/// Map a source failure to the read-path response (the
/// `mail_error_response` shape): `NotFound` -> 404,
/// `Unavailable` -> 501, `Denied` -> 403, everything
/// else -> 502. Error strings stay generic: automation
/// stderr can contain local paths.
fn notes_error_response(err: NotesError, not_found_what: &'static str) -> axum::response::Response {
    match err {
        NotesError::Unavailable(reason) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({ "error": reason })),
        )
            .into_response(),
        NotesError::Denied => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "notes access denied" })),
        )
            .into_response(),
        NotesError::NotFound => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": not_found_what })),
        )
            .into_response(),
        NotesError::NotRunning => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "Notes is not running" })),
        )
            .into_response(),
        NotesError::Timeout | NotesError::Upstream(_) | NotesError::Unverified => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "notes source error" })),
        )
            .into_response(),
    }
}

fn note_row_response(
    row: &crate::notes_sync::NoteRow,
    replayed: bool,
) -> axum::response::Response {
    let mut value = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = value.as_object_mut() {
        obj.insert("replayed".to_string(), serde_json::Value::Bool(replayed));
    }
    Json(value).into_response()
}

/// D1-6b-4: one delete outcome (LOCAL DEFINITION, the
/// `MailDeleteResponse` shape). `effect_id` is `None`
/// only on a tombstone replay whose original Effect
/// could not be found (the row carries the create
/// effect id, not the delete one).
#[derive(Debug, Serialize)]
struct NoteDeleteResponse {
    note_id: String,
    source_id: String,
    native_id: String,
    folder: String,
    deleted: bool,
    effect_id: Option<String>,
    replayed: bool,
}

fn note_delete_response(
    row: &crate::notes_sync::NoteRow,
    effect_id: Option<String>,
    replayed: bool,
) -> axum::response::Response {
    Json(NoteDeleteResponse {
        note_id: row.note_id.clone(),
        source_id: row.source_id.clone(),
        native_id: row.native_id.clone(),
        folder: row.folder.clone(),
        deleted: true,
        effect_id,
        replayed,
    })
    .into_response()
}

/// The succeeded delete Effect for one note id, when
/// one exists (the replay branch's effect id — the
/// tombstoned row itself carries the create effect id,
/// not the delete one).
fn note_delete_effect_id_for(conn: &rusqlite::Connection, note_id: &str) -> Option<String> {
    sqlite_store::list_effects(conn).ok().and_then(|effects| {
        effects
            .into_iter()
            .find(|e| {
                e.capability == "notes"
                    && e.operation == "delete"
                    && e.scope == note_id
                    && e.status == "succeeded"
            })
            .map(|e| e.effect_id)
    })
}

#[derive(Debug, Deserialize)]
pub(crate) struct NotesSyncDto {
    folder: Option<String>,
}

pub(crate) async fn notes_sync(
    State(state): State<AppState>,
    Json(req): Json<NotesSyncDto>,
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
    // 1. Validate: a folder is required (Q-D1-6b-19:
    // user-named folder only, never the whole library).
    let Some(folder) = req.folder.filter(|s| !s.is_empty()) else {
        return bad_request("folder is required");
    };
    // 2. The folder must pre-exist in Notes.app (404).
    // 3. Read the folder outside the SQLite lock: the
    // AppleScript dump can take seconds on a big folder.
    let source = Arc::clone(&state.notes_source);
    let sync_folder = folder.clone();
    let notes_result = tokio::task::spawn_blocking(move || {
        let exists = source.folder_exists(&sync_folder)?;
        if !exists {
            return Err(NotesError::NotFound);
        }
        source.list_notes(&sync_folder)
    })
    .await;
    let notes = match notes_result {
        Ok(Ok(notes)) => notes,
        Ok(Err(err)) => return notes_error_response(err, "unknown folder"),
        Err(_) => return notes_error_response(NotesError::Timeout, "unknown folder"),
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match crate::notes_sync::record_sync(
        &mut conn,
        &notes,
        &folder,
        &state.origin_device_id,
    ) {
        Ok(result) => Json(serde_json::to_value(result).unwrap_or(serde_json::Value::Null))
            .into_response(),
        Err(detail) => {
            eprintln!("warning: notes sync failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn notes_api_list(State(state): State<AppState>) -> impl IntoResponse {
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
    match crate::notes_sync::list_notes(&conn, crate::notes_source::NOTES_SOURCE_ID) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: notes list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn notes_api_get(
    State(state): State<AppState>,
    axum::extract::Path(note_id): axum::extract::Path<String>,
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
    match crate::notes_sync::get_note_row(&conn, &note_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown note" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: notes get failed ({detail})");
            sqlite_error_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct NotesCreateDto {
    // All validated fields are Option so a missing
    // field is the route's own 400 (not the extractor's
    // 422): "no key → 400" must hold however the client
    // forgets the key. LOCAL DEFINITION (plan §5).
    folder: Option<String>,
    name: Option<String>,
    body: Option<String>,
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
}

pub(crate) async fn notes_api_create(
    State(state): State<AppState>,
    Json(req): Json<NotesCreateDto>,
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
    // 1. Validate the DTO: empty folder / name / body /
    // idempotency_key → 400. Plain-text body; the
    // gateway converts it to minimal HTML for Notes.
    let Some(folder) = req.folder.filter(|s| !s.is_empty()) else {
        return bad_request("folder is required");
    };
    let Some(name) = req.name.filter(|s| !s.is_empty()) else {
        return bad_request("name must not be empty");
    };
    let Some(body) = req.body.filter(|s| !s.is_empty()) else {
        return bad_request("body must not be empty");
    };
    let Some(idempotency_key) = req.idempotency_key.filter(|k| !k.trim().is_empty()) else {
        return bad_request("idempotency_key is required");
    };
    let folder_hash = crate::operation_binding::sha256_hex(folder.as_bytes());
    let name_hash = crate::operation_binding::sha256_hex(name.as_bytes());
    let body_hash = crate::operation_binding::sha256_hex(body.as_bytes());
    let digest = crate::operation_binding::note_create_digest(&folder_hash, &name_hash, &body_hash);
    // 2. Disconnected source rejects new writes (409).
    if notes_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 3. Idempotency replay BEFORE planning a new Effect:
    // same key + same content → 200 original row, no
    // second create, no second Effect; same key +
    // different content → 409. "Same content" is decided
    // on the create Effect's input digest, not the row's
    // body_hash: the row records the hash of the re-read
    // stored plaintext, which Notes may normalize away
    // from the request hash.
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::notes_sync::find_by_idempotency_key(&conn, &idempotency_key) {
            Ok(Some(existing)) => {
                let same = sqlite_store::list_effects(&conn)
                    .ok()
                    .and_then(|effects| {
                        effects
                            .iter()
                            .find(|e| e.effect_id == existing.effect_id)
                            .map(|e| e.input_digest.as_deref() == Some(digest.as_str()))
                    })
                    .unwrap_or(false);
                if same {
                    return note_row_response(&existing, true);
                }
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "idempotency_key_conflict" })),
                )
                    .into_response();
            }
            Ok(None) => {}
            Err(detail) => {
                eprintln!("warning: notes create idempotency lookup failed ({detail})");
                return sqlite_error_response();
            }
        }
    }
    // 4. Approval peek (read-only, nothing burned): the
    // digest is checked BEFORE planning, so a mismatch
    // leaves no orphaned planned row and the approval
    // stays unconsumed for a correct retry (the 6b-1
    // SD3 shape).
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
        if record.tool != "notes.create" {
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
    // 5. The folder must exist BEFORE planning: a create
    // into a missing folder is a 404 with nothing
    // burned (no Effect, approval unconsumed). The
    // check runs outside the SQLite lock. Any OTHER
    // backend error (Notes not running, denied,
    // timeout) falls through to the execute step, where
    // the Effect lifecycle records it as failed /
    // unknown — the pre-check must not swallow a
    // backend-down failure into a plan-less error.
    {
        let source = Arc::clone(&state.notes_source);
        let check_folder = folder.clone();
        let exists_result =
            tokio::task::spawn_blocking(move || source.folder_exists(&check_folder)).await;
        match exists_result {
            Ok(Ok(true)) => {}
            Ok(Ok(false)) | Ok(Err(NotesError::NotFound)) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown folder" })),
                )
                    .into_response();
            }
            Ok(Err(other)) => {
                eprintln!(
                    "warning: notes folder pre-check failed ({other:?}); deferring to execute"
                );
            }
            Err(_) => {
                eprintln!("warning: notes folder pre-check task failed; deferring to execute");
            }
        }
    }
    // 6. Reuse-or-plan (E2-A V1): a planned row with the
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
                // still the original row, never a re-create.
                let conn = sqlite.lock().expect("sqlite lock");
                match crate::notes_sync::find_by_idempotency_key(&conn, &idempotency_key) {
                    Ok(Some(existing)) => return note_row_response(&existing, true),
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
            create_write_mandate(&state, &execution.execution_id, "notes.create");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "notes",
                "create",
                &idempotency_key,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::notes_sync::find_by_idempotency_key(&conn, &idempotency_key) {
                        Ok(Some(existing)) => return note_row_response(&existing, true),
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
    // 7. Consume the bound approval (binding + push to
    // authorized), then drive the Effect to started.
    if let Err(response) = consume_bound_approval_for_effect(
        &state,
        "notes.create",
        &req.tool_call_id,
        &digest,
        Some(&effect.effect_id),
    ) {
        return *response;
    }
    if let Err(response) = start_bound_effect(&state, &effect) {
        return *response;
    }
    // 8. Execute outside the SQLite lock: the AppleScript
    // create can take 30+ s. Same spawn_blocking shape
    // as device_set / email_send. The create inputs are
    // cloned for the blocking task.
    let source = Arc::clone(&state.notes_source);
    let create_folder = folder.clone();
    let create_name = name.clone();
    let create_body = body.clone();
    let create_result = tokio::task::spawn_blocking(move || {
        source.create_note(&create_folder, &create_name, &create_body)
    })
    .await;
    let outcome = match create_result {
        Ok(result) => result,
        Err(_) => {
            // The blocking task was cancelled or panicked:
            // the create may or may not have run, so the
            // outcome is Unknown (P0-R), never Failed.
            let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Unknown);
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "notes source error", "effect_id": effect.effect_id })),
            )
                .into_response();
        }
    };
    let policy_version = current_policy_version_id(&state).unwrap_or_default();
    match outcome {
        Ok(created) => {
            // Record the fact row + note.created event in
            // one transaction FIRST, then finish the
            // Effect: a succeeded Effect must never exist
            // without its fact row. The row's body_hash
            // is the hash of the RE-READ plaintext the
            // backend returned, not of the request body.
            let stored_body_hash =
                crate::operation_binding::sha256_hex(created.plaintext.as_bytes());
            let mut conn = sqlite.lock().expect("sqlite lock");
            match crate::notes_sync::record_created(
                &mut conn,
                &created,
                &folder,
                &stored_body_hash,
                &idempotency_key,
                &effect.effect_id,
                &execution_id,
                &policy_version,
                &state.origin_device_id,
            ) {
                Ok(row) => {
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => note_row_response(&row, false),
                        Err(_) => {
                            eprintln!(
                                "warning: notes create effect finish failed for {}",
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
                    // A UNIQUE-key race (a concurrent create
                    // with the same key won the row): the
                    // note was created, so the existing row
                    // is the replay, never a second row.
                    drop(conn);
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::notes_sync::find_by_idempotency_key(&conn, &idempotency_key) {
                        Ok(Some(existing)) => {
                            let same = sqlite_store::list_effects(&conn)
                                .ok()
                                .and_then(|effects| {
                                    effects
                                        .iter()
                                        .find(|e| e.effect_id == existing.effect_id)
                                        .map(|e| {
                                            e.input_digest.as_deref() == Some(digest.as_str())
                                        })
                                })
                                .unwrap_or(false);
                            drop(conn);
                            if same {
                                let _ = ledger_entry_finish(
                                    &state,
                                    &effect.effect_id,
                                    LedgerOutcome::Succeeded,
                                );
                                note_row_response(&existing, true)
                            } else {
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
                        _ => {
                            eprintln!("warning: notes create record failed ({detail})");
                            let _ = ledger_entry_finish(
                                &state,
                                &effect.effect_id,
                                LedgerOutcome::Failed,
                            );
                            sqlite_error_response()
                        }
                    }
                }
            }
        }
        Err(err) => {
            // Only an ambiguous create is Unknown (P0-R):
            // the note may or may not have been created,
            // so an unknown_requires_reconciliation Inbox
            // row is opened by the transition itself.
            // Every other gateway-side failure is Failed.
            let (status, error, outcome) = match err {
                NotesError::Unverified => (
                    StatusCode::BAD_GATEWAY,
                    "notes source error",
                    LedgerOutcome::Unknown,
                ),
                NotesError::Timeout => (
                    StatusCode::BAD_GATEWAY,
                    "notes source error",
                    LedgerOutcome::Unknown,
                ),
                NotesError::NotFound => (
                    StatusCode::NOT_FOUND,
                    "unknown folder",
                    LedgerOutcome::Failed,
                ),
                NotesError::Denied => (
                    StatusCode::FORBIDDEN,
                    "notes access denied",
                    LedgerOutcome::Failed,
                ),
                NotesError::NotRunning => (
                    StatusCode::BAD_GATEWAY,
                    "Notes is not running",
                    LedgerOutcome::Failed,
                ),
                NotesError::Unavailable(reason) => {
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    return (
                        StatusCode::NOT_IMPLEMENTED,
                        Json(serde_json::json!({ "error": reason, "effect_id": effect.effect_id })),
                    )
                        .into_response();
                }
                NotesError::Upstream(_) => (
                    StatusCode::BAD_GATEWAY,
                    "notes source error",
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

#[derive(Debug, Deserialize)]
pub(crate) struct NotesAppendDto {
    // All validated fields are Option so a missing
    // field is the route's own 400 (not the extractor's
    // 422). LOCAL DEFINITION (plan §5).
    text: Option<String>,
    base_body_hash: Option<String>,
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
}

pub(crate) async fn notes_api_append(
    State(state): State<AppState>,
    axum::extract::Path(note_id): axum::extract::Path<String>,
    Json(req): Json<NotesAppendDto>,
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
    // 1. Validate the DTO: empty text / base_body_hash /
    // idempotency_key → 400. The appended text is joined
    // to the live plaintext with no added separator (the
    // caller owns the separator — LOCAL DEFINITION).
    let Some(text) = req.text.filter(|s| !s.is_empty()) else {
        return bad_request("text must not be empty");
    };
    let Some(base_body_hash) = req.base_body_hash.filter(|s| !s.is_empty()) else {
        return bad_request("base_body_hash is required");
    };
    let Some(idempotency_key) = req.idempotency_key.filter(|k| !k.trim().is_empty()) else {
        return bad_request("idempotency_key is required");
    };
    // 2. Resolve the row: unknown note → 404.
    let row = {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::notes_sync::get_note_row(&conn, &note_id) {
            Ok(Some(row)) => row,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown note" })),
                )
                    .into_response();
            }
            Err(detail) => {
                eprintln!("warning: notes append resolve failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    let text_hash = crate::operation_binding::sha256_hex(text.as_bytes());
    let digest = crate::operation_binding::note_append_digest(&note_id, &base_body_hash, &text_hash);
    // 3. Disconnected source rejects new writes (409).
    if notes_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 4. Append replay BEFORE planning a new Effect:
    // same key + same (note_id, base, new text) → 200
    // current row, no second write, no second Effect;
    // same key + different content → 409. The replay
    // state lives on the row (the D-1 last-append
    // triple); a note never appended to has no triple.
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::notes_sync::last_append_for(&conn, &note_id) {
            Ok(state)
                if state.key.as_deref() == Some(idempotency_key.as_str())
                    && state.base_body_hash.as_deref() == Some(base_body_hash.as_str())
                    && state.text_hash.as_deref() == Some(text_hash.as_str()) =>
            {
                return note_row_response(&row, true);
            }
            Ok(state) if state.key.as_deref() == Some(idempotency_key.as_str()) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "idempotency_key_conflict" })),
                )
                    .into_response();
            }
            Ok(_) => {}
            Err(detail) => {
                eprintln!("warning: notes append replay lookup failed ({detail})");
                return sqlite_error_response();
            }
        }
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
        if record.tool != "notes.append" {
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
    // 6. Conflict re-read BEFORE planning (Q-D1-6b-20):
    // re-read the live plaintext and compare its hash
    // with the approved base. Changed → 409
    // `note_changed`, no write, approval NOT burned.
    // Locked → 403 `note_locked` (a locked note's body
    // cannot be read, so it cannot be appended to).
    // Gone → 404. Any other backend error returns the
    // mapped error before planning: the live text is
    // load-bearing for the write, so a failed re-read
    // cannot fall through to an append against an
    // unknown base. The check runs outside the SQLite
    // lock; the live text it returns is the base for
    // the write. A residual TOCTOU window remains
    // between this read and the set (planning +
    // consume, milliseconds, no lock held) — the plan's
    // "not burned" rule forces the check before consume,
    // so the window is inherent, not a defect.
    let live_plaintext = {
        let source = Arc::clone(&state.notes_source);
        let check_folder = row.folder.clone();
        let check_native = row.native_id.clone();
        let read_result =
            tokio::task::spawn_blocking(move || source.get_note(&check_folder, &check_native))
                .await;
        match read_result {
            Ok(Ok(Some(live))) => {
                if live.password_protected {
                    return (
                        StatusCode::FORBIDDEN,
                        Json(serde_json::json!({ "error": "note_locked" })),
                    )
                        .into_response();
                }
                let live_hash = crate::operation_binding::sha256_hex(live.plaintext.as_bytes());
                if live_hash != base_body_hash {
                    // The 409 carries the fresh base hash
                    // (a hash only, never plaintext): the
                    // row's stored hash is stale after an
                    // external edit, and sync never updates
                    // it (the mail precedent), so the
                    // client re-reads the base from this
                    // response and retries (plan §5: "用户
                    // 重读后重试"). The approval is not
                    // burned.
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({
                            "error": "note_changed",
                            "current_body_hash": live_hash,
                        })),
                    )
                        .into_response();
                }
                live.plaintext
            }
            Ok(Ok(None)) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown note" })),
                )
                    .into_response();
            }
            Ok(Err(err)) => return notes_error_response(err, "unknown note"),
            Err(_) => return notes_error_response(NotesError::Timeout, "unknown note"),
        }
    };
    // 7. Reuse-or-plan (E2-A V1). Effect triple
    // ("notes", "append", note_id) — the scope is the
    // note, so the Effect layer alone cannot tell two
    // appends apart; the row's last-append triple (step
    // 4) is the replay record.
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
                // still the current row, never a re-append.
                let conn = sqlite.lock().expect("sqlite lock");
                match crate::notes_sync::get_note_row(&conn, &note_id) {
                    Ok(Some(current)) => return note_row_response(&current, true),
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
            create_write_mandate(&state, &execution.execution_id, "notes.append");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "notes",
                "append",
                &note_id,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::notes_sync::get_note_row(&conn, &note_id) {
                        Ok(Some(current)) => return note_row_response(&current, true),
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
    // 8. Consume the bound approval, then drive the
    // Effect to started.
    if let Err(response) = consume_bound_approval_for_effect(
        &state,
        "notes.append",
        &req.tool_call_id,
        &digest,
        Some(&effect.effect_id),
    ) {
        return *response;
    }
    if let Err(response) = start_bound_effect(&state, &effect) {
        return *response;
    }
    // 9. Execute outside the SQLite lock: the new body
    // is the live text from step 6 plus the appended
    // text; `set body` replaces the whole note. The
    // backend returns the post-write re-read plaintext.
    let source = Arc::clone(&state.notes_source);
    let append_folder = row.folder.clone();
    let append_native = row.native_id.clone();
    let new_body = format!("{live_plaintext}{text}");
    let append_result =
        tokio::task::spawn_blocking(move || source.set_body(&append_folder, &append_native, &new_body))
            .await;
    let outcome = match append_result {
        Ok(result) => result,
        Err(_) => {
            let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Unknown);
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "notes source error", "effect_id": effect.effect_id })),
            )
                .into_response();
        }
    };
    match outcome {
        Ok(plaintext) => {
            // Record the row update + note.updated event
            // in one transaction FIRST, then finish the
            // Effect. The new body_hash is the hash of
            // the post-write re-read, not of the
            // concatenated request text.
            let new_body_hash = crate::operation_binding::sha256_hex(plaintext.as_bytes());
            let mut conn = sqlite.lock().expect("sqlite lock");
            match crate::notes_sync::record_appended(
                &mut conn,
                &note_id,
                &new_body_hash,
                &idempotency_key,
                &base_body_hash,
                &text_hash,
                &execution_id,
                &state.origin_device_id,
            ) {
                Ok(Some(updated)) => {
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => note_row_response(&updated, false),
                        Err(_) => {
                            eprintln!(
                                "warning: notes append effect finish failed for {}",
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
                Ok(None) => {
                    drop(conn);
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({ "error": "unknown note" })),
                    )
                        .into_response()
                }
                Err(detail) => {
                    eprintln!("warning: notes append record failed ({detail})");
                    drop(conn);
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    sqlite_error_response()
                }
            }
        }
        Err(err) => {
            // Only an ambiguous write is Unknown (P0-R):
            // the set may or may not have landed. Every
            // other gateway-side failure is Failed.
            let (status, error, outcome) = match err {
                NotesError::Unverified => (
                    StatusCode::BAD_GATEWAY,
                    "notes source error",
                    LedgerOutcome::Unknown,
                ),
                NotesError::Timeout => (
                    StatusCode::BAD_GATEWAY,
                    "notes source error",
                    LedgerOutcome::Unknown,
                ),
                NotesError::NotFound => (
                    StatusCode::NOT_FOUND,
                    "unknown note",
                    LedgerOutcome::Failed,
                ),
                NotesError::Denied => (
                    StatusCode::FORBIDDEN,
                    "notes access denied",
                    LedgerOutcome::Failed,
                ),
                NotesError::NotRunning => (
                    StatusCode::BAD_GATEWAY,
                    "Notes is not running",
                    LedgerOutcome::Failed,
                ),
                NotesError::Unavailable(reason) => {
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    return (
                        StatusCode::NOT_IMPLEMENTED,
                        Json(serde_json::json!({ "error": reason, "effect_id": effect.effect_id })),
                    )
                        .into_response();
                }
                NotesError::Upstream(_) => (
                    StatusCode::BAD_GATEWAY,
                    "notes source error",
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

pub(crate) async fn notes_api_delete(
    State(state): State<AppState>,
    axum::extract::Path(note_id): axum::extract::Path<String>,
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
    // 1. Resolve the row: unknown note → 404. Notes
    // delete is addressed by the local `note-N` only
    // (no never-synced path — the mail Q-15 shape has
    // no notes analogue in the plan).
    let row = {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::notes_sync::get_note_row(&conn, &note_id) {
            Ok(Some(row)) => row,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown note" })),
                )
                    .into_response();
            }
            Err(detail) => {
                eprintln!("warning: notes delete resolve failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    let digest = crate::operation_binding::note_delete_digest(&note_id);
    // 2. Disconnected source rejects new writes (409).
    if notes_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 3. Tombstone check BEFORE planning a new Effect: an
    // already-tombstoned row is the replay — 200 current
    // row, no second event, no second Effect.
    if row.deleted {
        let conn = sqlite.lock().expect("sqlite lock");
        let effect_id = note_delete_effect_id_for(&conn, &note_id);
        return note_delete_response(&row, effect_id, true);
    }
    // 4. Approval peek (read-only, nothing burned). The
    // `tool_call_id` travels as a query parameter: the
    // desktop `delete_json` sends no body (the 6b-3 D-5
    // precedent).
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
        if record.tool != "notes.delete" {
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
    // 5. Reuse-or-plan (E2-A V1). Effect triple
    // ("notes", "delete", note_id).
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
                // still the tombstoned row, never a
                // re-delete.
                return note_delete_response(
                    &row,
                    Some(effect.effect_id.clone()),
                    true,
                );
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
            create_write_mandate(&state, &execution.execution_id, "notes.delete");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "notes",
                "delete",
                &note_id,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    return note_delete_response(
                        &row,
                        Some(other.effect_id.clone()),
                        true,
                    );
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
        "notes.delete",
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
    // as the mail delete.
    let source = Arc::clone(&state.notes_source);
    let delete_folder = row.folder.clone();
    let delete_native = row.native_id.clone();
    let delete_result =
        tokio::task::spawn_blocking(move || source.delete_note(&delete_folder, &delete_native))
            .await;
    let outcome = match delete_result {
        Ok(result) => result,
        Err(_) => Err(NotesError::Timeout),
    };
    match outcome {
        Ok(()) => {
            // Record the tombstone + note.deleted event in
            // one transaction FIRST, then finish the
            // Effect: a succeeded Effect must never exist
            // without its fact record.
            let mut conn = sqlite.lock().expect("sqlite lock");
            match crate::notes_sync::tombstone_note(
                &mut conn,
                &note_id,
                &execution_id,
                &state.origin_device_id,
            ) {
                Ok(Some(tombstoned)) => {
                    let tombstoned_row = match tombstoned {
                        crate::notes_sync::TombstoneOutcome::Tombstoned(row)
                        | crate::notes_sync::TombstoneOutcome::AlreadyTombstoned(row) => row,
                    };
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => note_delete_response(
                            &tombstoned_row,
                            Some(effect.effect_id.clone()),
                            false,
                        ),
                        Err(_) => {
                            eprintln!(
                                "warning: notes delete effect finish failed for {}",
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
                Ok(None) => {
                    drop(conn);
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({ "error": "unknown note" })),
                    )
                        .into_response()
                }
                Err(detail) => {
                    eprintln!("warning: notes delete record failed ({detail})");
                    drop(conn);
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    sqlite_error_response()
                }
            }
        }
        Err(err) => {
            // Only a killed/ambiguous script run is
            // Unknown (P0-R): Notes.app may or may not
            // have deleted the note, so an
            // unknown_requires_reconciliation Inbox row
            // is opened by the transition itself. A
            // lookup miss is Failed: the note is not in
            // the folder, so there was nothing to delete.
            // Every other gateway-side failure is Failed.
            let (status, error, outcome) = match err {
                NotesError::Timeout => (
                    StatusCode::BAD_GATEWAY,
                    "notes source error",
                    LedgerOutcome::Unknown,
                ),
                NotesError::NotRunning => (
                    StatusCode::BAD_GATEWAY,
                    "Notes is not running",
                    LedgerOutcome::Failed,
                ),
                NotesError::NotFound => (
                    StatusCode::NOT_FOUND,
                    "unknown note",
                    LedgerOutcome::Failed,
                ),
                NotesError::Denied => (
                    StatusCode::FORBIDDEN,
                    "notes access denied",
                    LedgerOutcome::Failed,
                ),
                NotesError::Unavailable(reason) => {
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    return (
                        StatusCode::NOT_IMPLEMENTED,
                        Json(serde_json::json!({ "error": reason, "effect_id": effect.effect_id })),
                    )
                        .into_response();
                }
                _ => (
                    StatusCode::BAD_GATEWAY,
                    "notes source error",
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
