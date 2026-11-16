//! Engineering routes: projects, bindings, locations, worktrees, patches (20 routes).
//!
//! The handlers moved here from `server.rs` (FU-5 S-8). The rest of the
//! route directory lives in `server.rs`; this batch moved no `//!`
//! directory lines (the header has none for the engineering groups).
//! `unknown_record` and `create_worktree_record` stay in `server.rs`
//! (already `pub(crate)`): `routes_data.rs` calls the former and the
//! P3-C driver (`worker.rs`) calls the latter via `crate::server::`.
//! The persist helpers (`persist_binding`, `persist_project`,
//! `persist_location`, `persist_worktree`), `export_evidence_best_effort`,
//! `check_patch_write`, `sync_patch_file_status` and `PatchCheck` also
//! stay in `server.rs` as `pub(crate)` and are imported from
//! `crate::server` — the loop patch path still calls the last three,
//! and `create_worktree_record` still calls `persist_worktree`.
//! `location_canonical_root` stays PRIVATE in `server.rs`: only staying
//! code calls it (`create_worktree_record` and the snapshot helper).
//! `write_time_observation` and `PATCH_STATUSES` are called from the
//! moved code by their fully-qualified paths, so they are not imported.
//! `ApprovalState.effect_binding` becomes `pub(crate)` in `server.rs`:
//! `apply_patch_file_locked` reads it, and it was only visible inside
//! `server.rs` before the move (same visibility-only class as the
//! AppState fields).
//! `apply_patch_file_locked` moved with `patches_apply`: the plan says
//! the patch-apply approval-lock logic moves with the handler.

use std::path::Path;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::binding_store::{WorkspaceBinding, BINDING_ACTIVE, BINDING_ARCHIVED};
use crate::effect_store::EffectFileRecord;
use crate::location_store::{Location, LOCATION_ACTIVE, LOCATION_ARCHIVED};
use crate::project_store::{Project, PROJECT_ACTIVE, PROJECT_ARCHIVED};
use crate::routes_common::{bad_request, now_ms, sqlite_error_response, sqlite_not_configured};
use crate::server::{
    check_idempotency, check_patch_write, compute_policy_digest, create_worktree_record,
    effect_by_id, export_evidence_best_effort, idempotency_key_missing,
    location_root_sha256, persist_approval, persist_binding,
    persist_location, persist_project, persist_worktree, start_bound_effect,
    sync_patch_file_status, transition_effect, unknown_record, AppState, IdempotencyDecision,
    PatchCheck,
};
use crate::sqlite_store::{self, StateBackend};
use crate::worktree_store::{Worktree, WORKTREE_ACTIVE, WORKTREE_ARCHIVED};
use agent_desktop::approval::ApprovalStatus;


// --- Engineering record surface (P3-A, record subset) ---
//
// WorkspaceBinding + Project + Location + Worktree: four
// JSONL record stores, no Effect, no approval loop (direct
// management actions, like the Task 9 connector surface).
// List hides archived rows; get returns archived rows with
// their status. Archive is a status flip, never a delete:
// there is no hard-delete route and directories are never
// removed. Creation validation failures are all 400.

pub(crate) fn workspace_number(id: &str) -> u64 {
    id.strip_prefix("workspace-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

pub(crate) fn project_number(id: &str) -> u64 {
    id.strip_prefix("project-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

pub(crate) fn location_number(id: &str) -> u64 {
    id.strip_prefix("location-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

pub(crate) fn worktree_number(id: &str) -> u64 {
    id.strip_prefix("worktree-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// P3-B archive check (plan §3.8 "non-terminal Patch"): a
/// Worktree is blocked while any of its PatchFiles is pending
/// or failed — the work the Patch still owes. A stale Patch
/// with no pending/failed file does not block: its files are
/// terminally refused and the Patch is an audit row. Without
/// SQLite there is no Patch store, so nothing blocks.
pub(crate) fn worktree_archive_blocked(state: &AppState, worktree_id: &str) -> Option<&'static str> {
    if state.sqlite_mode() != StateBackend::Sqlite || state.sqlite_is_broken() {
        return None;
    }
    let Some(sqlite) = &state.sqlite else {
        return None;
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::worktree_has_actionable_patch(&conn, worktree_id) {
        Ok(true) => Some("worktree_has_active_patch"),
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct BindingCreateDto {
    display_name: Option<String>,
    path: Option<String>,
}

pub(crate) async fn bindings_create(
    State(state): State<AppState>,
    Json(req): Json<BindingCreateDto>,
) -> impl IntoResponse {
    let Some(display_name) = req.display_name.filter(|n| !n.trim().is_empty()) else {
        return bad_request("display_name is required");
    };
    let Some(raw_path) = req.path.filter(|p| !p.trim().is_empty()) else {
        return bad_request("path is required");
    };
    // The picked directory is canonicalized at creation (a
    // symlinked pick binds its real target); a missing path
    // or a non-directory is a 400.
    let Ok(canonical) = Path::new(&raw_path).canonicalize() else {
        return bad_request("directory does not exist");
    };
    if !canonical.is_dir() {
        return bad_request("path is not a directory");
    }
    let canonical_root = canonical.to_string_lossy().into_owned();
    let mut next = state.next_workspace_id.lock().expect("workspace id lock");
    let id = format!("workspace-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = WorkspaceBinding {
        workspace_id: id.clone(),
        display_name,
        allowed_roots: vec![canonical_root],
        policy_digest: compute_policy_digest(&state),
        status: BINDING_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
    };
    state
        .bindings
        .lock()
        .expect("binding lock")
        .insert(record.workspace_id.clone(), record.clone());
    persist_binding(&state, &record);
    Json(record).into_response()
}

pub(crate) async fn bindings_list(State(state): State<AppState>) -> Json<Vec<WorkspaceBinding>> {
    let bindings = state.bindings.lock().expect("binding lock");
    let mut records: Vec<WorkspaceBinding> = bindings
        .values()
        .filter(|b| b.status == BINDING_ACTIVE)
        .cloned()
        .collect();
    records.sort_by_key(|b| workspace_number(&b.workspace_id));
    Json(records)
}

pub(crate) async fn bindings_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let bindings = state.bindings.lock().expect("binding lock");
    match bindings.get(&id) {
        Some(record) => Json(record.clone()).into_response(),
        None => unknown_record("binding"),
    }
}

pub(crate) async fn bindings_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut bindings = state.bindings.lock().expect("binding lock");
    let Some(record) = bindings.get_mut(&id) else {
        return unknown_record("binding");
    };
    if record.status == BINDING_ARCHIVED {
        return Json(record.clone()).into_response();
    }
    // Archive guard (plan §3.8): an active Location blocks.
    let blocked = state
        .locations
        .lock()
        .expect("location lock")
        .values()
        .any(|l| l.workspace_id == id && l.status == LOCATION_ACTIVE);
    if blocked {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "binding_has_active_locations" })),
        )
            .into_response();
    }
    record.status = BINDING_ARCHIVED.to_string();
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_binding(&state, &updated);
    Json(updated).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct ProjectCreateDto {
    name: Option<String>,
    description: Option<String>,
}

pub(crate) async fn projects_create(
    State(state): State<AppState>,
    Json(req): Json<ProjectCreateDto>,
) -> impl IntoResponse {
    let Some(name) = req.name.filter(|n| !n.trim().is_empty()) else {
        return bad_request("name is required");
    };
    let Some(description) = req.description.filter(|d| !d.trim().is_empty()) else {
        return bad_request("description is required");
    };
    let mut next = state.next_project_id.lock().expect("project id lock");
    let id = format!("project-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = Project {
        project_id: id.clone(),
        name,
        description,
        status: PROJECT_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
    };
    state
        .projects
        .lock()
        .expect("project lock")
        .insert(record.project_id.clone(), record.clone());
    persist_project(&state, &record);
    Json(record).into_response()
}

pub(crate) async fn projects_list(State(state): State<AppState>) -> Json<Vec<Project>> {
    let projects = state.projects.lock().expect("project lock");
    let mut records: Vec<Project> = projects
        .values()
        .filter(|p| p.status == PROJECT_ACTIVE)
        .cloned()
        .collect();
    records.sort_by_key(|p| project_number(&p.project_id));
    Json(records)
}

pub(crate) async fn projects_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let projects = state.projects.lock().expect("project lock");
    match projects.get(&id) {
        Some(record) => Json(record.clone()).into_response(),
        None => unknown_record("project"),
    }
}

pub(crate) async fn projects_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    // A project is pure metadata: archive is always allowed.
    // Existing references keep reading back with status
    // archived; nothing is deleted or unlinked.
    let mut projects = state.projects.lock().expect("project lock");
    let Some(record) = projects.get_mut(&id) else {
        return unknown_record("project");
    };
    if record.status == PROJECT_ARCHIVED {
        return Json(record.clone()).into_response();
    }
    record.status = PROJECT_ARCHIVED.to_string();
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_project(&state, &updated);
    Json(updated).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct LocationCreateDto {
    workspace_id: Option<String>,
    name: Option<String>,
    path: Option<String>,
}

pub(crate) async fn locations_create(
    State(state): State<AppState>,
    Json(req): Json<LocationCreateDto>,
) -> impl IntoResponse {
    let Some(workspace_id) = req.workspace_id.filter(|w| !w.trim().is_empty()) else {
        return bad_request("workspace_id is required");
    };
    let Some(name) = req.name.filter(|n| !n.trim().is_empty()) else {
        return bad_request("name is required");
    };
    let Some(raw_path) = req.path.filter(|p| !p.trim().is_empty()) else {
        return bad_request("path is required");
    };
    // The binding must exist and be active.
    let allowed_roots = {
        let bindings = state.bindings.lock().expect("binding lock");
        let Some(binding) = bindings.get(&workspace_id) else {
            return bad_request("unknown workspace");
        };
        if binding.status != BINDING_ACTIVE {
            return bad_request("workspace is archived");
        }
        binding.allowed_roots.clone()
    };
    // The candidate must exist and be a directory, and after
    // canonicalizing BOTH sides it must sit inside one of the
    // binding's allowed roots (settings_store's
    // validate_working_dir pattern: a symlinked escape
    // canonicalizes outside the root and fails here).
    let Ok(candidate) = Path::new(&raw_path).canonicalize() else {
        return bad_request("directory does not exist");
    };
    if !candidate.is_dir() {
        return bad_request("path is not a directory");
    }
    let inside = allowed_roots.iter().any(|root| {
        Path::new(root)
            .canonicalize()
            .is_ok_and(|canonical_root| candidate.strip_prefix(&canonical_root).is_ok())
    });
    if !inside {
        return bad_request("directory is outside the workspace binding");
    }
    let canonical_root = candidate.to_string_lossy().into_owned();
    let root_sha256 = location_root_sha256(&workspace_id, &canonical_root);
    let mut next = state.next_location_id.lock().expect("location id lock");
    let id = format!("location-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = Location {
        location_id: id.clone(),
        workspace_id,
        name,
        canonical_root,
        root_sha256,
        status: LOCATION_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
    };
    state
        .locations
        .lock()
        .expect("location lock")
        .insert(record.location_id.clone(), record.clone());
    persist_location(&state, &record);
    Json(record).into_response()
}

pub(crate) async fn locations_list(State(state): State<AppState>) -> Json<Vec<Location>> {
    let locations = state.locations.lock().expect("location lock");
    let mut records: Vec<Location> = locations
        .values()
        .filter(|l| l.status == LOCATION_ACTIVE)
        .cloned()
        .collect();
    records.sort_by_key(|l| location_number(&l.location_id));
    Json(records)
}

pub(crate) async fn locations_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let locations = state.locations.lock().expect("location lock");
    match locations.get(&id) {
        Some(record) => Json(record.clone()).into_response(),
        None => unknown_record("location"),
    }
}

pub(crate) async fn locations_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut locations = state.locations.lock().expect("location lock");
    let Some(record) = locations.get_mut(&id) else {
        return unknown_record("location");
    };
    if record.status == LOCATION_ARCHIVED {
        return Json(record.clone()).into_response();
    }
    // Archive guard (plan §3.8): an active Worktree blocks.
    let blocked = state
        .worktrees
        .lock()
        .expect("worktree lock")
        .values()
        .any(|w| w.location_id == id && w.status == WORKTREE_ACTIVE);
    if blocked {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "location_has_active_worktrees" })),
        )
            .into_response();
    }
    record.status = LOCATION_ARCHIVED.to_string();
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_location(&state, &updated);
    Json(updated).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct WorktreeCreateDto {
    location_id: Option<String>,
    directory_relative: Option<String>,
    branch_label: Option<String>,
    project_id: Option<String>,
}

pub(crate) async fn worktrees_create(
    State(state): State<AppState>,
    Json(req): Json<WorktreeCreateDto>,
) -> impl IntoResponse {
    let Some(location_id) = req.location_id.filter(|l| !l.trim().is_empty()) else {
        return bad_request("location_id is required");
    };
    match create_worktree_record(
        &state,
        location_id,
        req.directory_relative.unwrap_or_default(),
        req.branch_label.unwrap_or_default(),
        req.project_id,
    ) {
        Ok(record) => Json(record).into_response(),
        Err(code) => bad_request(code),
    }
}

pub(crate) async fn worktrees_list(State(state): State<AppState>) -> Json<Vec<Worktree>> {
    let worktrees = state.worktrees.lock().expect("worktree lock");
    let mut records: Vec<Worktree> = worktrees
        .values()
        .filter(|w| w.status == WORKTREE_ACTIVE)
        .cloned()
        .collect();
    records.sort_by_key(|w| worktree_number(&w.worktree_id));
    Json(records)
}

pub(crate) async fn worktrees_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let worktrees = state.worktrees.lock().expect("worktree lock");
    match worktrees.get(&id) {
        Some(record) => Json(record.clone()).into_response(),
        None => unknown_record("worktree"),
    }
}

pub(crate) async fn worktrees_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut worktrees = state.worktrees.lock().expect("worktree lock");
    let Some(record) = worktrees.get_mut(&id) else {
        return unknown_record("worktree");
    };
    if record.status == WORKTREE_ARCHIVED {
        return Json(record.clone()).into_response();
    }
    // Archive guard (plan §3.8): a non-terminal Patch blocks.
    // P3-A has no Patch store; the check point lives in
    // worktree_archive_blocked for P3-B to wire.
    if let Some(code) = worktree_archive_blocked(&state, &id) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": code })),
        )
            .into_response();
    }
    record.status = WORKTREE_ARCHIVED.to_string();
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_worktree(&state, &updated);
    Json(updated).into_response()
}

// --- P3-B patches (plan §4.7) ---
//
// Patches exist only in SQLite (plan §4.6): the routes
// answer 500 `sqlite_not_configured` without a configured
// SQLite database, and 500 `sqlite_unavailable` on a broken
// one — the same fail-closed gate as the Evidence routes.
// List/get read; apply/reject mutate and are SQLite-mode
// only (in Shadow mode there is no Patch store to mutate).

#[derive(Debug, Deserialize)]
pub(crate) struct PatchListQuery {
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PatchApplyRequest {
    #[serde(default)]
    expected_old_sha256: Option<String>,
    approval_id: String,
}

pub(crate) async fn patches_list(
    State(state): State<AppState>,
    Query(query): Query<PatchListQuery>,
) -> impl IntoResponse {
    if let Some(status) = &query.status {
        if !crate::patch_store::PATCH_STATUSES.contains(&status.as_str()) {
            return bad_request("unknown patch status");
        }
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
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_patches(&conn) {
        Ok(rows) => {
            let rows: Vec<_> = match &query.status {
                Some(status) => rows
                    .into_iter()
                    .filter(|r| &r.status == status)
                    .collect(),
                None => rows,
            };
            Json(rows).into_response()
        }
        Err(_) => sqlite_error_response(),
    }
}

pub(crate) async fn patches_get(
    State(state): State<AppState>,
    axum::extract::Path(patch_id): axum::extract::Path<String>,
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
    let patch = match sqlite_store::get_patch(&conn, &patch_id) {
        Ok(Some(patch)) => patch,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown patch" })))
                .into_response()
        }
        Err(_) => return sqlite_error_response(),
    };
    let files = match sqlite_store::list_patch_files(&conn, &patch_id) {
        Ok(files) => files,
        Err(_) => return sqlite_error_response(),
    };
    Json(serde_json::json!({ "patch": patch, "files": files })).into_response()
}

pub(crate) async fn patches_apply(
    State(state): State<AppState>,
    axum::extract::Path((patch_id, patch_file_id)): axum::extract::Path<(String, String)>,
    Json(req): Json<PatchApplyRequest>,
) -> impl IntoResponse {
    if req.approval_id.trim().is_empty() {
        return bad_request("approval_id is required");
    }
    match apply_patch_file_locked(
        &state,
        &patch_id,
        &patch_file_id,
        req.expected_old_sha256.as_deref(),
        &req.approval_id,
    ) {
        Ok((patch_file, effect, replayed)) => {
            // E2-C §4.5: a fresh apply writes Evidence in-tx
            // (the export regenerates from SQLite, so on a
            // replay this is a no-op rewrite of the same rows).
            if let Some(sqlite) = &state.sqlite {
                if let Ok(conn) = sqlite.lock() {
                    export_evidence_best_effort(&state, &conn);
                }
            }
            Json(serde_json::json!({
                "patch_file": patch_file,
                "effect": effect,
                "replayed": replayed,
            }))
            .into_response()
        }
        Err(response) => *response,
    }
}

pub(crate) async fn patches_reject(
    State(state): State<AppState>,
    axum::extract::Path(patch_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let patch = {
        let conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::get_patch(&conn, &patch_id) {
            Ok(Some(patch)) => patch,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown patch" })),
                )
                    .into_response()
            }
            Err(_) => return sqlite_error_response(),
        }
    };
    // Already rejected -> 200 idempotent (the get shape).
    if patch.status == "rejected" {
        let conn = sqlite.lock().expect("sqlite lock");
        let files = sqlite_store::list_patch_files(&conn, &patch_id).unwrap_or_default();
        return Json(serde_json::json!({ "patch": patch, "files": files })).into_response();
    }
    let mut conn = sqlite.lock().expect("sqlite lock");
    let tx = match conn.transaction() {
        Ok(tx) => tx,
        Err(_) => return sqlite_error_response(),
    };
    let patch = match sqlite_store::reject_patch_tx(&tx, &patch_id, now_ms() as i64) {
        Ok(patch) => patch,
        Err(code) if code == "patch_has_applied_files" => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "patch_has_applied_files" })),
            )
                .into_response()
        }
        Err(code) if code == "patch_not_found" => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "unknown patch" })),
            )
                .into_response()
        }
        Err(_) => return sqlite_error_response(),
    };
    if tx.commit().is_err() {
        return sqlite_error_response();
    }
    let files = sqlite_store::list_patch_files(&conn, &patch_id).unwrap_or_default();
    Json(serde_json::json!({ "patch": patch, "files": files })).into_response()
}

/// Apply one PatchFile under ONE approvals lock hold (plan
/// §4.4). The three-way check, the approval burn and the
/// Effect authorize/start all serialize here, so a
/// concurrent apply of the same file cannot double-write.
/// `consume_loop_approval` and
/// `consume_bound_approval_for_effect` cannot be called
/// from inside this hold (a std Mutex is not reentrant — it
/// would deadlock), so the loop-approval validation below
/// mirrors their checks; keep in sync: Approved only (a
/// bound approval can never be Edited), unconsumed,
/// `effect_binding` is this file's Effect, the Effect
/// exists with the same digest at planned/authorized, and
/// the approval's `tool` is fs.write. Error codes reuse the
/// existing bound-gate replies (`approval_required`,
/// `approval_rejected`, `approval_binding_mismatch`,
/// `approval_audit_failed`, `effect_transition_failed`).
pub(crate) fn apply_patch_file_locked(
    state: &AppState,
    patch_id: &str,
    patch_file_id: &str,
    expected_old_sha256: Option<&str>,
    approval_id: &str,
) -> Result<(sqlite_store::PatchFileRecord, EffectFileRecord, bool), Box<Response>> {
    let not_found = |error: &str| -> Box<Response> {
        Box::new((StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": error }))).into_response())
    };
    let forbidden = |error: &str| -> Box<Response> {
        Box::new((StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": error }))).into_response())
    };
    let conflict = |error: &str| -> Box<Response> {
        Box::new((StatusCode::CONFLICT, Json(serde_json::json!({ "error": error }))).into_response())
    };
    let bad_request = |error: &str| -> Box<Response> {
        Box::new((StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": error }))).into_response())
    };
    let internal = |error: &str| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error })),
        )
            .into_response()
    };
    // Patches are SQLite-only (plan §4.6).
    if state.sqlite_mode() != StateBackend::Sqlite {
        return Err(Box::new(sqlite_not_configured()));
    }
    if state.sqlite_is_broken() {
        return Err(Box::new(sqlite_error_response()));
    }
    let Some(sqlite) = &state.sqlite else {
        return Err(Box::new(sqlite_not_configured()));
    };
    let mut approvals = state.approvals.lock().expect("approval lock");
    let (patch, file) = {
        let conn = sqlite.lock().expect("sqlite lock");
        let patch = match sqlite_store::get_patch(&conn, patch_id) {
            Ok(Some(patch)) => patch,
            Ok(None) => return Err(not_found("unknown patch")),
            Err(_) => return Err(Box::new(internal("sqlite_unavailable"))),
        };
        let file = match sqlite_store::get_patch_file(&conn, patch_file_id) {
            Ok(Some(file)) => file,
            Ok(None) => return Err(not_found("unknown patch file")),
            Err(_) => return Err(Box::new(internal("sqlite_unavailable"))),
        };
        if file.patch_id != patch.patch_id {
            return Err(not_found("unknown patch file"));
        }
        (patch, file)
    };
    // Idempotent replay (plan §4.4): an already-applied file
    // returns 200 + replayed:true with no re-check/re-write.
    if file.status == "applied" {
        let effect = effect_by_id(state, &file.effect_id)
            .ok_or_else(|| internal("unknown effect"))?;
        return Ok((file, effect, true));
    }
    if patch.status == "rejected" {
        return Err(conflict("patch_rejected"));
    }
    if file.status == "rejected" {
        return Err(conflict("patch_file_rejected"));
    }
    if file.status == "stale" {
        return Err(conflict("patch_stale"));
    }
    if file.status != "pending" && file.status != "failed" {
        return Err(conflict("patch_file_not_appliable"));
    }
    if expected_old_sha256 != file.old_sha256.as_deref() {
        return Err(bad_request("expected_old_sha256_mismatch"));
    }
    // Loop-approval validation (mirror of
    // consume_loop_approval + consume_bound_approval_for_effect).
    let Some(record) = approvals.get_mut(approval_id) else {
        return Err(forbidden("approval_required"));
    };
    if record.tool != "fs.write" {
        return Err(forbidden("approval_required"));
    }
    if record.status == ApprovalStatus::Rejected {
        return Err(forbidden("approval_rejected"));
    }
    if record.status != ApprovalStatus::Approved {
        return Err(forbidden("approval_required"));
    }
    if record.consumed {
        return Err(forbidden("approval_binding_mismatch"));
    }
    if record.effect_binding.as_deref() != Some(file.effect_id.as_str()) {
        return Err(forbidden("approval_binding_mismatch"));
    }
    let digest = record.operation_digest.clone();
    let effect = match effect_by_id(state, &file.effect_id) {
        Some(effect)
            if effect.input_digest.as_deref() == digest.as_deref()
                && (effect.status == "planned" || effect.status == "authorized") => effect,
        _ => return Err(forbidden("approval_binding_mismatch")),
    };
    // E2-A §2.4: idempotency is the second door behind the
    // stale check — a replayed apply (same approval, same
    // input_digest) must not rewrite the file. The bound
    // Effect itself is excluded: it is the current attempt
    // (planned/authorized), not a prior write. A *different*
    // terminal Effect with the same key (same execution,
    // fs.write, scope, digest) proves the write already
    // landed, so the apply returns replayed without touching
    // the file. A bound Effect without an execution id or
    // digest has no key to check: 409, not a silent Proceed.
    if idempotency_key_missing(effect.execution_id.as_deref(), effect.input_digest.as_deref()) {
        return Err(Box::new(
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_unavailable" })),
            )
                .into_response(),
        ));
    }
    match check_idempotency(
        state,
        effect.execution_id.as_deref(),
        "fs",
        "write",
        &file.path_relative,
        effect.input_digest.as_deref(),
        Some(&effect.effect_id),
    ) {
        IdempotencyDecision::Proceed => {}
        IdempotencyDecision::Replay(other) => {
            return Ok((file, other, true));
        }
        IdempotencyDecision::Blocked(other) => {
            return Err(Box::new(
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
                )
                    .into_response(),
            ));
        }
        // Store broken: fail closed, never guess.
        IdempotencyDecision::Unavailable => {
            return Err(Box::new(internal("sqlite_unavailable")));
        }
    }
    // Three-way check, before the approval is burned: a
    // drifted proposal never consumes its approval.
    let (check, target) = check_patch_write(state, &patch, &file);
    if check == PatchCheck::Stale {
        if let Err(detail) = sync_patch_file_status(state, &file.patch_file_id, "stale") {
            eprintln!("warning: patch file stale-marking failed: {detail}");
        }
        return Err(conflict("patch_stale"));
    }
    if check == PatchCheck::Unavailable {
        return Err(Box::new(internal("patch_target_unavailable")));
    }
    // Burn the approval (persist failure -> 500 with the
    // in-memory consumed flag kept, the P0-C fail-closed
    // rule), then drive planned -> authorized -> started.
    record.consumed = true;
    if let Err(detail) = persist_approval(state, approval_id, record) {
        eprintln!("warning: bound approval consumption persistence failed: {detail}");
        return Err(Box::new(internal("approval_audit_failed")));
    }
    if effect.status == "planned"
        && transition_effect(state, &effect.effect_id, "authorized", None, None).is_err()
    {
        return Err(Box::new(internal("effect_transition_failed")));
    }
    start_bound_effect(state, &effect)?;
    // Write (or skip it on the AlreadyNew branch), then one
    // SQLite transaction for the records.
    let replayed = check == PatchCheck::AlreadyNew;
    if !replayed {
        let target = target.ok_or_else(|| internal("patch_target_unavailable"))?;
        if let Some(parent) = target.parent() {
            if std::fs::create_dir_all(parent).is_err() {
                return Err(Box::new(internal("patch_write_failed")));
            }
        }
        if std::fs::write(&target, &file.new_text).is_err() {
            // The approval is spent and the Effect is started;
            // leave an honest failed state (best-effort tx) so
            // the file is retryable through a fresh proposal.
            let mut failed = effect.clone();
            failed.status = "failed".to_string();
            let now = now_ms() as i64;
            let mut conn = sqlite.lock().expect("sqlite lock");
            if let Ok(tx) = conn.transaction() {
                let _ = sqlite_store::fail_patch_records_tx(
                    &tx,
                    &failed,
                    &file.patch_file_id,
                    now,
                );
                let _ = tx.commit();
            }
            state
                .effects
                .lock()
                .expect("effect lock")
                .insert(failed.effect_id.clone(), failed.clone());
            return Err(Box::new(internal("patch_write_failed")));
        }
    }
    let mut succeeded = effect.clone();
    succeeded.status = "succeeded".to_string();
    let owned_evidence = crate::evidence::write_time_observation(state, &succeeded);
    let evidence_draft = owned_evidence.as_ref().map(|o| o.as_draft());
    let now = now_ms() as i64;
    let (file, _patch) = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(_) => return Err(Box::new(internal("sqlite_unavailable"))),
        };
        let out = match sqlite_store::apply_patch_records_tx(
            &tx,
            &succeeded,
            evidence_draft.as_ref(),
            &file.patch_file_id,
            now,
        ) {
            Ok(out) => out,
            Err(detail) => {
                eprintln!("warning: patch apply records failed: {detail}");
                drop(tx);
                // The file is written (or was already new) but
                // the records did not land; mark the file failed
                // (best-effort) so a fresh proposal can recover
                // it instead of leaving it pending forever.
                let mut failed = effect.clone();
                failed.status = "failed".to_string();
                let mut conn = sqlite.lock().expect("sqlite lock");
                if let Ok(tx) = conn.transaction() {
                    let _ = sqlite_store::fail_patch_records_tx(
                        &tx,
                        &failed,
                        &file.patch_file_id,
                        now,
                    );
                    let _ = tx.commit();
                }
                state
                    .effects
                    .lock()
                    .expect("effect lock")
                    .insert(failed.effect_id.clone(), failed.clone());
                return Err(Box::new(internal("sqlite_unavailable")));
            }
        };
        if tx.commit().is_err() {
            return Err(Box::new(internal("sqlite_unavailable")));
        }
        out
    };
    state
        .effects
        .lock()
        .expect("effect lock")
        .insert(succeeded.effect_id.clone(), succeeded.clone());
    Ok((file, succeeded, replayed))
}
