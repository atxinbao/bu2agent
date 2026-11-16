//! Data routes (D1 data plane): /memories, /goals, /sources, /api/calendar.
//!
//! The handlers moved here from `server.rs` (FU-5 S-5). The rest of the
//! route directory lives in `server.rs`. There is no `//!` directory line
//! for memories, goals, sources or calendar in the `server.rs` header,
//! so no directory line moved with this batch. The stores
//! (`memory_store.rs`, goal/source rows in `sqlite_store.rs`,
//! `calendar_store.rs`) already hold the fact rows and persistence;
//! only the handlers move.
//!
//! Count note: the plan's S-5 row says sources 3, but the code
//! registers a fourth sources route — POST /sources/{id}/reconnect,
//! added by the tails batch as the mirror of disconnect (the
//! registration comment reads "sources (3 routes) + tails reconnect
//! (1 route)"). It lives in the same section block as disconnect and
//! no later batch owns it, so it moves with this batch: 15 routes
//! total (memories 4 + goals 4 + sources 4 + calendar 3).

use std::sync::Arc;

use agent_desktop::approval::ApprovalStatus;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::calendar_backend::CalendarError;
use crate::routes_common::{bad_request, now_ms, sqlite_error_response, sqlite_not_configured};
use crate::server::{
    consume_bound_approval_for_effect, create_execution_with_binding, create_write_mandate,
    current_policy_version_id, effect_for_approval, ledger_entry, ledger_entry_finish,
    start_bound_effect, unknown_record, AppState, LedgerEntryDecision, LedgerOutcome,
};
use crate::sqlite_store::{self, StateBackend};

// --- Goals (D1-1) ---
//
// A Goal is a first-class object a Task can link to
// (goal_id), managed directly like Project: no Effect, no
// approval loop (direct management actions, same rationale
// as the server.rs:7334-7341 comment on the four JSONL
// stores). SQLite only, no JSONL mirror. Archive is a
// status flip, never a delete; list hides archived rows,
// detail still reads them. Writes require SQLite mode
// (same fail-closed rule as the assignment takeover);
// reads serve whenever SQLite is attached.

const GOAL_ACTIVE: &str = "active";

#[derive(Debug, Deserialize)]
pub(crate) struct GoalCreateDto {
    title: Option<String>,
    description: Option<String>,
}

pub(crate) async fn goals_create(
    State(state): State<AppState>,
    Json(req): Json<GoalCreateDto>,
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
    let Some(title) = req.title.filter(|t| !t.trim().is_empty()) else {
        return bad_request("title is required");
    };
    let mut next = state.next_goal_id.lock().expect("goal id lock");
    let id = format!("goal-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = sqlite_store::GoalRow {
        goal_id: id,
        title,
        description: req.description.unwrap_or_default(),
        status: GOAL_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = sqlite_store::insert_goal(&mut conn, &record) {
        eprintln!("warning: goal insert failed ({detail})");
        return sqlite_error_response();
    }
    Json(record).into_response()
}

pub(crate) async fn goals_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_goals(&conn) {
        Ok(records) => Json(records).into_response(),
        Err(detail) => {
            eprintln!("warning: goal list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn goals_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_goal(&conn, &id) {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => unknown_record("goal"),
        Err(detail) => {
            eprintln!("warning: goal get failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn goals_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    // A goal is pure metadata: archive is always allowed.
    // Existing references keep reading back with status
    // archived; nothing is deleted or unlinked.
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::archive_goal(&mut conn, &id) {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => unknown_record("goal"),
        Err(detail) => {
            eprintln!("warning: goal archive failed ({detail})");
            sqlite_error_response()
        }
    }
}

// D1-5 (Q-D1-51–Q-D1-54): memories are direct-managed
// metadata like Goals (no Effect, no approval loop,
// SQLite only, no JSONL mirror). Save is user-explicit
// only — nothing extracts memories automatically.
// Writes require SQLite mode (same fail-closed rule as
// the assignment takeover); reads serve whenever SQLite
// is attached. Delete is a tombstone, never a physical
// delete; list hides tombstones, detail still reads them.
//
// Save DTO (LOCAL DEFINITION — the plan names the
// scope triple and the provenance fields but leaves
// the request shape unspecified):
//   {
//     "layer": "project" | "user",
//     "content": "...",
//     "scope": {"workspace_id"?, "project_id"?, "session_id"?},
//     "current_scope": {"workspace_id"?, "project_id"?, "session_id"?},
//     "source": {"session_id"?, "execution_id"?, "source_id"?,
//                "author": "..."}
//   }
// `current_scope` is the caller's declared current
// scope (there is no server-side "current scope").
// A save whose scope differs from current_scope is
// rejected with 409 `memory_scope_mismatch` (the code
// Q-D1-52 leaves open; §6 M2 names 409). The user
// layer uses the all-empty (global) scope only.

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryScopeDto {
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemorySourceDto {
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    execution_id: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    author: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemorySaveDto {
    layer: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    scope: Option<MemoryScopeDto>,
    #[serde(default)]
    current_scope: Option<MemoryScopeDto>,
    #[serde(default)]
    source: Option<MemorySourceDto>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MemoryListQuery {
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    layer: Option<String>,
}

/// Blank or whitespace-only strings are absent, same
/// normalization as the location/project create DTOs.
pub(crate) fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.trim().is_empty())
}

pub(crate) fn memory_scope_from(dto: Option<MemoryScopeDto>) -> crate::memory_store::MemoryScope {
    let dto = dto.unwrap_or(MemoryScopeDto {
        workspace_id: None,
        project_id: None,
        session_id: None,
    });
    crate::memory_store::MemoryScope {
        workspace_id: non_empty(dto.workspace_id),
        project_id: non_empty(dto.project_id),
        session_id: non_empty(dto.session_id),
    }
}

pub(crate) async fn memories_save(
    State(state): State<AppState>,
    Json(req): Json<MemorySaveDto>,
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
    let Some(layer) = non_empty(req.layer) else {
        return bad_request("layer is required");
    };
    if layer != "project" && layer != "user" {
        return bad_request("layer must be 'project' or 'user'");
    }
    let Some(content) = non_empty(req.content) else {
        return bad_request("content is required");
    };
    let Some(source) = req.source else {
        return bad_request("source is required");
    };
    let Some(author) = non_empty(source.author) else {
        return bad_request("source.author is required");
    };
    let scope = memory_scope_from(req.scope);
    let current_scope = memory_scope_from(req.current_scope);
    // Q-D1-52: the user layer is global — it carries no
    // scope triple of its own.
    if layer == "user" && !scope.is_empty() {
        return bad_request("user memories use the empty (global) scope");
    }
    // Q-D1-52: write to a scope other than the caller's
    // current scope is rejected (409 per §6 M2).
    if scope.canonical() != current_scope.canonical() {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "memory_scope_mismatch"})),
        )
            .into_response();
    }
    let mut next = state.next_memory_id.lock().expect("memory id lock");
    let id = format!("memory-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let provenance = crate::memory_store::MemorySource {
        session_id: non_empty(source.session_id),
        execution_id: non_empty(source.execution_id),
        source_id: non_empty(source.source_id),
        author,
        created_ms: now,
    };
    let record = crate::memory_store::MemoryRow {
        memory_id: id,
        layer,
        scope_json: scope.canonical(),
        content,
        source_json: provenance.canonical(),
        created_ms: now,
        updated_ms: now,
        deleted: false,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = crate::memory_store::insert_memory(&mut conn, &record) {
        eprintln!("warning: memory insert failed ({detail})");
        return sqlite_error_response();
    }
    Json(record).into_response()
}

pub(crate) async fn memories_list(
    State(state): State<AppState>,
    Query(query): Query<MemoryListQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if let Some(layer) = &query.layer {
        if layer != "project" && layer != "user" {
            return bad_request("layer must be 'project' or 'user'");
        }
    }
    let scope = crate::memory_store::MemoryScope {
        workspace_id: non_empty(query.workspace_id),
        project_id: non_empty(query.project_id),
        session_id: non_empty(query.session_id),
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::memory_store::list_memories(&conn, query.layer.as_deref(), &scope) {
        Ok(records) => Json(records).into_response(),
        Err(detail) => {
            eprintln!("warning: memory list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn memories_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::memory_store::get_memory(&conn, &id) {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => unknown_record("memory"),
        Err(detail) => {
            eprintln!("warning: memory get failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn memories_delete(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    // Tombstone only: the text stays on disk. A re-delete
    // returns the current row (no second event).
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let mut conn = sqlite.lock().expect("sqlite lock");
    match crate::memory_store::delete_memory(&mut conn, &id, &state.origin_device_id) {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => unknown_record("memory"),
        Err(detail) => {
            eprintln!("warning: memory delete failed ({detail})");
            sqlite_error_response()
        }
    }
}

// --- D1-4: Source Registry (plan §5) ---
//
// Reads go to SQLite when configured (the table is the
// fact source) and fall back to the in-memory read cache
// otherwise. Disconnect is a write and requires SQLite
// mode, same as the goal archive. Disconnect flips only
// the row's `status` flag — the source's history
// Effect/Inbox rows are untouched, nothing is deleted
// (Q-D1-42); re-disconnect is a 200 no-op returning the
// current row, same as the goal re-archive.

pub(crate) async fn sources_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    if let Some(sqlite) = &state.sqlite {
        let conn = sqlite.lock().expect("sqlite lock");
        return match sqlite_store::list_sources(&conn) {
            Ok(records) => Json(records).into_response(),
            Err(detail) => {
                eprintln!("warning: source list failed ({detail})");
                sqlite_error_response()
            }
        };
    }
    Json(state.source_registry.list()).into_response()
}

pub(crate) async fn sources_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    if let Some(sqlite) = &state.sqlite {
        let conn = sqlite.lock().expect("sqlite lock");
        return match sqlite_store::get_source(&conn, &id) {
            Ok(Some(record)) => Json(record).into_response(),
            Ok(None) => unknown_record("source"),
            Err(detail) => {
                eprintln!("warning: source get failed ({detail})");
                sqlite_error_response()
            }
        };
    }
    match state.source_registry.get(&id) {
        Some(record) => Json(record).into_response(),
        None => unknown_record("source"),
    }
}

pub(crate) async fn sources_disconnect(
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
    let record = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::disconnect_source(&mut conn, &id, &state.origin_device_id) {
            Ok(Some(record)) => record,
            Ok(None) => return unknown_record("source"),
            Err(detail) => {
                eprintln!("warning: source disconnect failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    // Refresh the read cache after the write (the lock
    // above is released, so no lock-order inversion).
    if let Ok(conn) = sqlite.lock() {
        let _ = state.source_registry.load(&conn);
    }
    Json(record).into_response()
}

/// Reconnect a source (tails follow-up to D1-4): the
/// mirror of `sources_disconnect` — the store-level
/// `reconnect_source` flips `status` back to `active`
/// and records a `source.registered` event in the same
/// transaction. Re-connecting an already-active source
/// is a 200 no-op (no rewrite, no second event), and an
/// unknown id 404s — both are the store function's own
/// rules, not a new rule invented here. Memories the
/// disconnect cascade tombstoned stay tombstoned:
/// reconnect restores the source, it does not resurrect
/// history (red line 9).
pub(crate) async fn sources_reconnect(
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
    let record = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::reconnect_source(&mut conn, &id, &state.origin_device_id) {
            Ok(Some(record)) => record,
            Ok(None) => return unknown_record("source"),
            Err(detail) => {
                eprintln!("warning: source reconnect failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    // Refresh the read cache after the write (the lock
    // above is released, so no lock-order inversion).
    if let Ok(conn) = sqlite.lock() {
        let _ = state.source_registry.load(&conn);
    }
    Json(record).into_response()
}

/// D1-6b-2 (Q-D1-6b-24): a disconnected calendar-source
/// rejects new writes. Read paths are unaffected. A
/// missing Registry row (seed never ran) is not a
/// disconnect — the write proceeds.
pub(crate) fn calendar_source_disconnected(state: &AppState) -> bool {
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let Ok(conn) = sqlite.lock() else {
        return false;
    };
    matches!(
        sqlite_store::get_source(&conn, crate::calendar_store::CALENDAR_SOURCE_ID),
        Ok(Some(row)) if row.status == "disconnected"
    )
}

#[derive(Debug, Deserialize)]
pub(crate) struct CalendarCreateDto {
    // All validated fields are Option so a missing field
    // is the route's own 400 (not the extractor's 422):
    // the 6b-1 EmailSendDto precedent. LOCAL DEFINITION
    // (plan §3). `location` / `notes` are optional; an
    // empty string counts as absent in both the approval
    // path and this route, so the two digests can never
    // disagree about an empty field.
    calendar_id: Option<String>,
    title: Option<String>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    all_day: Option<bool>,
    location: Option<String>,
    notes: Option<String>,
    idempotency_key: Option<String>,
    tool_call_id: Option<String>,
}

pub(crate) fn created_row_response(
    row: &crate::calendar_store::CalendarEventRow,
    replayed: bool,
) -> axum::response::Response {
    let mut value = serde_json::to_value(row).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = value.as_object_mut() {
        obj.insert("replayed".to_string(), serde_json::Value::Bool(replayed));
    }
    Json(value).into_response()
}

pub(crate) async fn calendar_events_create(
    State(state): State<AppState>,
    Json(req): Json<CalendarCreateDto>,
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
    // 1. Validate the DTO: empty calendar_id / title /
    // idempotency_key → 400; missing start / end /
    // all_day → 400; end at or before start → 400.
    // Minimal surface only (Q-D1-6b-10): no recurrence,
    // attendees, or alarms exist to validate.
    let Some(calendar_id) = req.calendar_id.filter(|s| !s.is_empty()) else {
        return bad_request("calendar_id is required");
    };
    let Some(title) = req.title.filter(|s| !s.is_empty()) else {
        return bad_request("title must not be empty");
    };
    let Some(start_ms) = req.start_ms else {
        return bad_request("start_ms is required");
    };
    let Some(end_ms) = req.end_ms else {
        return bad_request("end_ms is required");
    };
    let Some(all_day) = req.all_day else {
        return bad_request("all_day is required");
    };
    if end_ms <= start_ms {
        return bad_request("end_ms must be after start_ms");
    }
    let Some(idempotency_key) = req.idempotency_key.filter(|k| !k.trim().is_empty()) else {
        return bad_request("idempotency_key is required");
    };
    let location = req.location.filter(|s| !s.is_empty());
    let notes = req.notes.filter(|s| !s.is_empty());
    let title_hash = crate::operation_binding::sha256_hex(title.as_bytes());
    let location_hash =
        crate::operation_binding::sha256_hex(location.as_deref().unwrap_or("").as_bytes());
    let notes_hash = crate::calendar_store::calendar_notes_hash(notes.as_deref());
    let digest = crate::operation_binding::calendar_create_digest(
        &calendar_id,
        &title_hash,
        start_ms,
        end_ms,
        all_day,
        &location_hash,
        &notes_hash,
    );
    // 2. Disconnected source rejects new writes (409).
    if calendar_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // 3. Idempotency replay BEFORE planning a new Effect:
    // same key + same content → 200 original row, no
    // second create, no second Effect; same key +
    // different content → 409.
    {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::calendar_store::find_by_idempotency_key(&conn, &idempotency_key) {
            Ok(Some(existing)) => {
                if existing.calendar_id == calendar_id
                    && existing.title == title
                    && existing.start_ms == start_ms
                    && existing.end_ms == end_ms
                    && existing.all_day == all_day
                    && existing.location == location
                    && existing.notes_hash == notes_hash
                {
                    return created_row_response(&existing, true);
                }
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "idempotency_key_conflict" })),
                )
                    .into_response();
            }
            Ok(None) => {}
            Err(detail) => {
                eprintln!("warning: calendar create idempotency lookup failed ({detail})");
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
        if record.tool != "calendar.create" {
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
                // still the original row, never a re-create.
                let conn = sqlite.lock().expect("sqlite lock");
                match crate::calendar_store::find_by_idempotency_key(&conn, &idempotency_key) {
                    Ok(Some(existing)) => return created_row_response(&existing, true),
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
            create_write_mandate(&state, &execution.execution_id, "calendar.create");
            match ledger_entry(
                &state,
                Some(execution.execution_id.clone()),
                "calendar",
                "create",
                &idempotency_key,
                Some(digest.clone()),
                Some(approval_id.clone()),
            ) {
                LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
                LedgerEntryDecision::Replay(other) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::calendar_store::find_by_idempotency_key(&conn, &idempotency_key) {
                        Ok(Some(existing)) => return created_row_response(&existing, true),
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
        "calendar.create",
        &req.tool_call_id,
        &digest,
        Some(&effect.effect_id),
    ) {
        return *response;
    }
    if let Err(response) = start_bound_effect(&state, &effect) {
        return *response;
    }
    // 7. Execute outside the SQLite lock: the EventKit
    // save can block on the store. Same spawn_blocking
    // shape as device_set / email_send. The create
    // inputs are cloned for the blocking task; the
    // originals are still needed below for the fact row.
    let backend = Arc::clone(&state.calendar_backend);
    let create_calendar_id = calendar_id.clone();
    let create_title = title.clone();
    let create_location = location.clone();
    let create_notes = notes.clone();
    let create_result = tokio::task::spawn_blocking(move || {
        backend.create_event(
            &create_calendar_id,
            &create_title,
            start_ms,
            end_ms,
            all_day,
            create_location.as_deref(),
            create_notes.as_deref(),
        )
    })
    .await;
    let outcome = match create_result {
        Ok(result) => result,
        Err(_) => {
            // The blocking task was cancelled or panicked:
            // the save may or may not have run, so the
            // outcome is Unknown (P0-R), never Failed.
            let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Unknown);
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "calendar source error", "effect_id": effect.effect_id })),
            )
                .into_response();
        }
    };
    let policy_version = current_policy_version_id(&state).unwrap_or_default();
    match outcome {
        Ok(created) => {
            // Record the fact row + calendar.created event
            // in one transaction FIRST, then finish the
            // Effect: a succeeded Effect must never exist
            // without its fact row.
            let mut conn = sqlite.lock().expect("sqlite lock");
            match crate::calendar_store::record_calendar_event(
                &mut conn,
                &created,
                &calendar_id,
                &title,
                location.as_deref(),
                &notes_hash,
                &idempotency_key,
                &effect.effect_id,
                &execution_id,
                &policy_version,
                &state.origin_device_id,
            ) {
                Ok(row) => {
                    drop(conn);
                    match ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Succeeded) {
                        Ok(_) => created_row_response(&row, false),
                        Err(_) => {
                            eprintln!(
                                "warning: calendar create effect finish failed for {}",
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
                    // event was created, so the existing row
                    // is the replay, never a second row.
                    drop(conn);
                    let conn = sqlite.lock().expect("sqlite lock");
                    match crate::calendar_store::find_by_idempotency_key(&conn, &idempotency_key) {
                        Ok(Some(existing))
                            if existing.calendar_id == calendar_id
                                && existing.title == title
                                && existing.start_ms == start_ms
                                && existing.end_ms == end_ms
                                && existing.all_day == all_day
                                && existing.location == location
                                && existing.notes_hash == notes_hash =>
                        {
                            drop(conn);
                            let _ = ledger_entry_finish(
                                &state,
                                &effect.effect_id,
                                LedgerOutcome::Succeeded,
                            );
                            created_row_response(&existing, true)
                        }
                        _ => {
                            eprintln!("warning: calendar create record failed ({detail})");
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
            // Only an ambiguous save is Unknown (P0-R):
            // the save may or may not have landed, so an
            // unknown_requires_reconciliation Inbox row is
            // opened by the transition itself. Every other
            // gateway-side failure is Failed.
            let (status, error, outcome) = match err {
                CalendarError::Unverified => (
                    StatusCode::BAD_GATEWAY,
                    "calendar source error",
                    LedgerOutcome::Unknown,
                ),
                CalendarError::NotFound => (
                    StatusCode::NOT_FOUND,
                    "unknown calendar",
                    LedgerOutcome::Failed,
                ),
                CalendarError::Denied => (
                    StatusCode::FORBIDDEN,
                    "calendar access denied",
                    LedgerOutcome::Failed,
                ),
                CalendarError::Unavailable(reason) => {
                    let _ = ledger_entry_finish(&state, &effect.effect_id, LedgerOutcome::Failed);
                    return (
                        StatusCode::NOT_IMPLEMENTED,
                        Json(serde_json::json!({ "error": reason, "effect_id": effect.effect_id })),
                    )
                        .into_response();
                }
                CalendarError::Upstream(_) => (
                    StatusCode::BAD_GATEWAY,
                    "calendar source error",
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

pub(crate) fn calendar_error_response(
    err: crate::calendar_backend::CalendarError,
) -> axum::response::Response {
    match err {
        crate::calendar_backend::CalendarError::Unavailable(reason) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({ "error": reason })),
        )
            .into_response(),
        crate::calendar_backend::CalendarError::Denied => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "calendar access denied" })),
        )
            .into_response(),
        crate::calendar_backend::CalendarError::Upstream(_) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "calendar source error" })),
        )
            .into_response(),
        // D1-6b-2: unknown calendar id (Q-D1-6b-09) and
        // unrecoverable save identifier (the Effect stays
        // Unknown, never guessed — the create route maps
        // this one itself; the read paths never produce
        // it, so it is unreachable here).
        crate::calendar_backend::CalendarError::NotFound => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown calendar" })),
        )
            .into_response(),
        crate::calendar_backend::CalendarError::Unverified => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "calendar source error" })),
        )
            .into_response(),
    }
}

pub(crate) async fn calendar_events_list(State(state): State<AppState>) -> impl IntoResponse {
    match state.calendar_backend.list_events() {
        Ok(events) => Json(serde_json::to_value(events).unwrap_or(serde_json::Value::Null))
            .into_response(),
        Err(err) => calendar_error_response(err),
    }
}

pub(crate) async fn calendar_event_get(
    State(state): State<AppState>,
    axum::extract::Path(event_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.calendar_backend.get_event(&event_id) {
        Ok(Some(event)) => Json(serde_json::to_value(event).unwrap_or(serde_json::Value::Null))
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown event" })),
        )
            .into_response(),
        Err(err) => calendar_error_response(err),
    }
}
