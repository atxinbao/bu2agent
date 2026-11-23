//! Task routes: /tasks (13 routes).
//!
//! The handlers moved here from `server.rs` (FU-5 S-6). The rest of the
//! route directory lives in `server.rs`. There is no `//!` directory line
//! for tasks in the `server.rs` header, so no directory line moved with
//! this batch. Task records stay a JSONL domain (`task_store.rs`);
//! only the handlers move, plus their DTOs and the helpers no staying
//! code calls (the two Mandate hooks, `valid_task_status`,
//! `task_number`, `execution_for_task`, `close_execution_for_task`,
//! `supersede_old_task_executions`).
//!
//! Shared helpers stay in `server.rs` as `pub(crate)`: the ledger
//! helpers (`record_effect`, `transition_effect`, `check_idempotency`,
//! `start_bound_effect`, `consume_bound_approval_for_effect`),
//! `create_execution`, `persist_task`, `resolve_file`,
//! `resolve_worktree_binding`, `ensure_root`, plus `create_mandate`,
//! `executions_snapshot` and `execution_number`, which staying code
//! (bind_execution_mandate, execution_for_loop, reconcile, execution
//! shell views) still calls — and `execution_for_task` /
//! `close_execution_for_task`, which `task_runner.rs` still calls
//! (qualified `crate::server::` call sites; the S-5 `unknown_record`
//! precedent: a helper called from staying code stays in server.rs).

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::effect_store::NewEffect;
use crate::operation_binding::plan_digest;
use crate::plan_store::{PlanStep, ReplanIf};
use crate::routes_common::{bad_request, sqlite_error_response, sqlite_not_configured};
use crate::server::{
    check_idempotency, close_execution, close_execution_for_task,
    consume_bound_approval_for_effect, create_execution, create_mandate, ensure_root,
    execution_for_task, executions_snapshot, idempotency_key_missing, persist_task,
    record_effect, resolve_file, resolve_worktree_binding, start_bound_effect,
    transition_effect, AppState, IdempotencyDecision,
};
use crate::sqlite_store::{self, StateBackend};
use crate::task_store::{TaskExecuteBinding, TaskFileRecord};

/// The Mandate hook for Task creation: one task-level row
/// (execution_id NULL).
pub(crate) fn create_task_mandate(state: &AppState, record: &TaskFileRecord) {
    create_mandate(
        state,
        Some(record.task_id.clone()),
        None,
        record.goal_id.clone(),
        "task",
    );
}

/// The Mandate hook for a Task goal change: supersede the
/// old task-level rows and create a fresh one binding the
/// current policy version.
pub(crate) fn supersede_task_mandates_and_create(state: &AppState, record: &TaskFileRecord) {
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return;
    }
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = sqlite_store::supersede_mandates_for_task(&mut conn, &record.task_id) {
        eprintln!("warning: D1-2 mandate supersede failed: {detail}");
        return;
    }
    drop(conn);
    create_mandate(
        state,
        Some(record.task_id.clone()),
        None,
        record.goal_id.clone(),
        "task",
    );
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskCreateDto {
    directory: String,
    /// D1-1: structural links to the scene the task serves.
    /// Optional; absent stays None (unlinked).
    #[serde(default)]
    goal_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskGetQuery {
    task_id: String,
    /// E2 slice: optional bound worktree for the execute
    /// step. Validated, and frozen on first use (scheme A,
    /// P3-C plan §2.3): a later execute naming a different
    /// worktree is 409 `task_binding_frozen_mismatch`.
    #[serde(default)]
    worktree_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TaskUpdateDto {
    task_id: String,
    status: String,
    artifact_path: Option<String>,
    /// D1-1: link fields, same shape as the create DTO.
    /// Present values replace the link; absent leaves it.
    /// (No clearing through this route: absent and null
    /// are indistinguishable in the DTO.)
    #[serde(default)]
    goal_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
}

pub(crate) fn valid_task_status(status: &str) -> bool {
    matches!(status, "created" | "draft_ready" | "completed" | "rejected" | "failed")
}

pub(crate) fn task_number(task_id: &str) -> u64 {
    task_id.strip_prefix("task-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

/// E2-A §2.3: re-run supersede. When a Task run/resume
/// creates a fresh Execution, cancel every OTHER still-
/// `"running"` Execution of that Task (a stale attempt left
/// running by a crash/restart, or by a manual status reset).
/// The new Execution is the current attempt; the
/// `execution_for_task` latest-wins read semantics are
/// unchanged.
pub(crate) fn supersede_old_task_executions(
    state: &AppState,
    task_id: &str,
    keep_execution_id: &str,
) {
    let Some(records) = executions_snapshot(state) else {
        return;
    };
    for record in records {
        if record.task_id.as_deref() != Some(task_id) {
            continue;
        }
        if record.execution_id == keep_execution_id {
            continue;
        }
        if record.status != "running" {
            continue;
        }
        close_execution(state, &record.execution_id, "cancelled");
    }
}

pub(crate) async fn tasks_create(
    State(state): State<AppState>,
    Json(req): Json<TaskCreateDto>,
) -> impl IntoResponse {
    if ensure_root(&state.files_root).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create file root" }))).into_response();
    }
    let resolved = match resolve_file(&state.files_root, &req.directory) {
        Ok(full) => full,
        Err(_) => return bad_request("path escapes file root"),
    };
    if !resolved.is_dir() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "directory not found" }))).into_response();
    }
    let mut next = state.next_task_id.lock().expect("task id lock");
    let id = format!("task-{}", *next);
    *next += 1;
    let record = TaskFileRecord {
        task_id: id,
        directory: req.directory,
        artifact_path: None,
        status: "created".to_string(),
        error: None,
        approval_id: None,
        draft_sha256: None,
        draft_bytes: None,
        execute_binding: None,
        goal_id: req.goal_id,
        project_id: req.project_id,
        workspace_id: req.workspace_id,
        plan_id: None,
    };
    state.tasks.lock().expect("task lock").insert(record.task_id.clone(), record.clone());
    persist_task(&state, &record);
    // D1-2 (Q-D1-22): one task-level Mandate row.
    create_task_mandate(&state, &record);
    Json(record).into_response()
}

/// D1-1: list filters. Each is an exact match on the
/// task's link field; absent means no filter on that
/// dimension. No new route: the filters ride the existing
/// list query.
#[derive(Debug, Deserialize)]
pub(crate) struct TaskListQuery {
    #[serde(default)]
    goal_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
}

pub(crate) async fn tasks_list(
    State(state): State<AppState>,
    Query(query): Query<TaskListQuery>,
) -> Json<Vec<TaskFileRecord>> {
    let mut records: Vec<TaskFileRecord> =
        state.tasks.lock().expect("task lock").values().cloned().collect();
    if let Some(goal_id) = &query.goal_id {
        records.retain(|r| r.goal_id.as_deref() == Some(goal_id.as_str()));
    }
    if let Some(project_id) = &query.project_id {
        records.retain(|r| r.project_id.as_deref() == Some(project_id.as_str()));
    }
    if let Some(workspace_id) = &query.workspace_id {
        records.retain(|r| r.workspace_id.as_deref() == Some(workspace_id.as_str()));
    }
    records.sort_by_key(|r| task_number(&r.task_id));
    Json(records)
}

pub(crate) async fn tasks_get(
    State(state): State<AppState>,
    Query(query): Query<TaskGetQuery>,
) -> impl IntoResponse {
    match state.tasks.lock().expect("task lock").get(&query.task_id) {
        Some(record) => Json(record.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response(),
    }
}

pub(crate) async fn tasks_update(
    State(state): State<AppState>,
    Json(req): Json<TaskUpdateDto>,
) -> impl IntoResponse {
    if !valid_task_status(&req.status) {
        return bad_request("unknown task status");
    }
    if let Some(path) = &req.artifact_path {
        if resolve_file(&state.files_root, path).is_err() {
            return bad_request("path escapes file root");
        }
    }
    let mut tasks = state.tasks.lock().expect("task lock");
    let Some(record) = tasks.get_mut(&req.task_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response();
    };
    // D1-2 (Q-D1-22): a goal change supersedes the Task's
    // Mandates (computed before the move below).
    let goal_changed = req
        .goal_id
        .as_ref()
        .is_some_and(|goal_id| Some(goal_id) != record.goal_id.as_ref());
    record.status = req.status;
    if let Some(path) = req.artifact_path {
        record.artifact_path = Some(path);
    }
    // D1-1: changing a link does not freeze anything —
    // freezing is still `execute_binding`'s job.
    if let Some(goal_id) = req.goal_id {
        record.goal_id = Some(goal_id);
    }
    if let Some(project_id) = req.project_id {
        record.project_id = Some(project_id);
    }
    if let Some(workspace_id) = req.workspace_id {
        record.workspace_id = Some(workspace_id);
    }
    let updated = record.clone();
    persist_task(&state, &updated);
    drop(tasks);
    // D1-2 (Q-D1-22): a goal change supersedes the old
    // task-level Mandates and creates a fresh one.
    if goal_changed {
        supersede_task_mandates_and_create(&state, &updated);
    }
    Json(updated).into_response()
}

// --- Task runner endpoints (Round 3, 3b) ---
//
// run/resume only validate and spawn; the runner owns transitions.
// Execute consumes the bound approval and never self-approves;
// the window decides approved first, then calls execute. Reject
// decides rejected itself (rejection only removes power). Cancel
// stops a running task between steps, or rejects a draft_ready
// one whose approval is still pending; an already-decided
// approval cannot be cancelled or rejected, only executed or
// left alone.

#[derive(Debug, Serialize)]
pub(crate) struct TaskDraftDto {
    task_id: String,
    draft: String,
    draft_sha256: Option<String>,
    draft_bytes: Option<u64>,
}

pub(crate) async fn tasks_run(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    // No model, no run: refused outright, never an echo draft.
    if state.deepseek.config().api_key.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "model_not_configured"})),
        )
            .into_response();
    }
    let mut tasks = state.tasks.lock().expect("task lock");
    let Some(record) = tasks.get_mut(&req.task_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    if !crate::task_store::valid_task_status_transition(&record.status, "running") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
    record.status = "running".to_string();
    record.error = None;
    let updated = record.clone();
    persist_task(&state, &updated);
    drop(tasks);
    // P0 phase 1: the runner attempt gets its Execution shell record.
    // E2-A §2.3: a fresh attempt supersedes any still-running
    // Execution a crashed earlier attempt left behind.
    let execution = create_execution(&state, Some(req.task_id.clone()), None);
    supersede_old_task_executions(&state, &req.task_id, &execution.execution_id);
    tokio::spawn(crate::task_runner::run_task(state.clone(), req.task_id));
    Json(updated).into_response()
}

pub(crate) async fn tasks_draft(
    State(state): State<AppState>,
    Query(query): Query<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&query.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    let Some(draft) = crate::task_runner::load_draft(&state, &query.task_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "no draft" })))
            .into_response();
    };
    Json(TaskDraftDto {
        task_id: record.task_id,
        draft_sha256: record
            .draft_sha256
            .or_else(|| Some(crate::operation_binding::sha256_hex(draft.as_bytes()))),
        draft_bytes: record.draft_bytes.or(Some(draft.len() as u64)),
        draft,
    })
    .into_response()
}

pub(crate) async fn tasks_execute(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(mut record) = state
        .tasks
        .lock()
        .expect("task lock")
        .get(&req.task_id)
        .cloned()
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown task" })),
        )
            .into_response();
    };
    if !crate::task_store::valid_task_status_transition(&record.status, "completed") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
    // E2 slice, scheme A (P3-C plan §2.3): a stated worktree
    // must exist and be active, and the first execute that
    // names one freezes the binding into the task record. A
    // later execute naming a different worktree is 409
    // `task_binding_frozen_mismatch` — the Worker runs the
    // frozen binding, never a re-resolved one.
    if let Some(worktree_id) = &req.worktree_id {
        let binding = match resolve_worktree_binding(&state, worktree_id) {
            Ok(binding) => binding,
            Err(detail) => return bad_request(detail),
        };
        let frozen = TaskExecuteBinding {
            location_id: binding.location_id.clone(),
            worktree_id: binding.worktree_id.clone(),
            root_digest: binding.root_digest.clone(),
        };
        match &record.execute_binding {
            Some(existing) if *existing != frozen => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "task_binding_frozen_mismatch" })),
                )
                    .into_response();
            }
            Some(_) => {}
            None => {
                record.execute_binding = Some(frozen);
                state
                    .tasks
                    .lock()
                    .expect("task lock")
                    .insert(record.task_id.clone(), record.clone());
                persist_task(&state, &record);
            }
        }
    }
    let Some(approval_id) = record.approval_id.clone() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "task approval missing" })),
        )
            .into_response();
    };
    let Some(artifact_path) = record.artifact_path.clone() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "task artifact missing" })),
        )
            .into_response();
    };
    let Some(draft) = crate::task_runner::load_draft(&state, &req.task_id) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "draft unavailable" })),
        )
            .into_response();
    };
    // The digest is recomputed server-side from the stored draft,
    // never trusted from the caller; consume is atomic and
    // single-use, exactly like files_write.
    let Some(digest) =
        crate::operation_binding::write_digest(&artifact_path, draft.as_bytes(), false)
    else {
        return bad_request("invalid path");
    };
    // P0-C: the task write is planned here (the execute request
    // is its first execution step; the task approval itself was
    // created by the runner, not by /approval/request, so no
    // planned row exists yet). A terminal Effect for the same
    // key replays the task record without writing; an unsettled
    // one refuses — except the execute's own planned row from
    // an earlier attempt (E2-A fix F-1, below): that row resumes
    // instead of refusing. Otherwise the approval is consumed
    // bound to the planned Effect and the Effect is driven
    // forward.
    let task_execution = execution_for_task(&state, &req.task_id);
    // E2-A §2.4: a task with no Execution has no key to check
    // — refuse with 409, not the store-broken 500 below.
    if idempotency_key_missing(task_execution.as_deref(), Some(&digest)) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "effect_idempotency_unavailable" })),
        )
            .into_response();
    }
    // E2-A fix F-1: an execute retried after its own first
    // attempt (approval_required before the approval was
    // decided) finds the planned Effect that first attempt left
    // behind. When the blocking row is that very row — status
    // `planned` and bound to this execute's approval — resume
    // it: reuse the existing row as `task_effect` and drive it
    // through the same tail below (consume → start → write →
    // completed). No second Effect row is created (a second
    // insert would collide on the UNIQUE idempotency key).
    // Any other unsettled row (started, authorized, or another
    // approval's) still refuses with 409.
    let task_effect = match check_idempotency(
        &state,
        task_execution.as_deref(),
        "tasks",
        "execute",
        &req.task_id,
        Some(&digest),
        None,
    ) {
        IdempotencyDecision::Replay(_) => {
            return Json(record).into_response();
        }
        IdempotencyDecision::Blocked(other) => {
            if other.status == "planned"
                && other.approval_id.as_deref() == Some(approval_id.as_str())
            {
                other
            } else {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
                )
                    .into_response();
            }
        }
        // P0-D: broken migrated-domain store fails closed.
        IdempotencyDecision::Unavailable => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "sqlite_unavailable" })),
            )
                .into_response();
        }
        IdempotencyDecision::Proceed => record_effect(
            &state,
            NewEffect {
                execution_id: task_execution,
                capability: "tasks".to_string(),
                operation: "execute".to_string(),
                scope: req.task_id.clone(),
                input_digest: Some(digest.clone()),
                approval_id: Some(approval_id.clone()),
                status: "planned".to_string(),
            },
        ),
    };
    if let Err(response) = consume_bound_approval_for_effect(
        &state,
        "fs.write",
        &Some(approval_id.clone()),
        &digest,
        Some(&task_effect.effect_id),
    ) {
        return *response;
    }
    if let Err(response) = start_bound_effect(&state, &task_effect) {
        return *response;
    }
    // Same tail as files_write (overwrite): normalized path,
    // parent dirs, direct write. The approval is already consumed,
    // so a write failure fails the task instead of leaving it
    // executable; the draft is kept, and the manual Tasks flow
    // can still write it with a fresh approval.
    let Some(normalized) = crate::operation_binding::normalize_path(&artifact_path) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "invalid path" })),
        )
            .into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return bad_request("path escapes file root");
    };
    if let Some(parent) = full.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            let _ = transition_effect(&state, &task_effect.effect_id, "failed", None, None);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "cannot create parent directory" })),
            )
                .into_response();
        }
    }
    if std::fs::write(&full, draft.as_bytes()).is_err() {
        crate::task_runner::fail(&state, &req.task_id, "产物写入失败");
        // P0-C: the artifact write was attempted and failed,
        // and the task now persists as failed — the planned
        // Effect drives to `failed`.
        let _ = transition_effect(&state, &task_effect.effect_id, "failed", None, None);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "cannot write file" })),
        )
            .into_response();
    }
    // P0-R: the tasks guard must be released before the Effect
    // transition below: its write-time Evidence hook re-locks
    // the tasks map to read the completed record.
    let updated = {
        let mut tasks = state.tasks.lock().expect("task lock");
        let Some(current) = tasks.get_mut(&req.task_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response();
        };
        current.status = "completed".to_string();
        current.error = None;
        current.clone()
    };
    persist_task(&state, &updated);
    // E2-A §2.3: the terminal Task closes its Execution.
    close_execution_for_task(&state, &req.task_id, "completed");
    // P0-C: a successful task write drives the planned
    // Effect to `succeeded`. The digest is the artifact
    // write digest the approval gate consumed; the
    // operation is the task execution, not the underlying
    // file write.
    let _ = transition_effect(&state, &task_effect.effect_id, "succeeded", None, None);
    Json(updated).into_response()
}

pub(crate) async fn tasks_reject(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&req.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    if !crate::task_store::valid_task_status_transition(&record.status, "rejected") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
    let Some(approval_id) = record.approval_id.clone() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "task approval missing" })),
        )
            .into_response();
    };
    if let Err(error) = crate::task_runner::decide_rejected(&state, &approval_id) {
        if error == "approval already decided" {
            return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": error })))
                .into_response();
        }
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": error })))
            .into_response();
    }
    let mut tasks = state.tasks.lock().expect("task lock");
    if let Some(current) = tasks.get_mut(&req.task_id) {
        if current.status != "draft_ready" {
            return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not rejectable" })))
                .into_response();
        }
        current.status = "rejected".to_string();
        let updated = current.clone();
        persist_task(&state, &updated);
        drop(tasks);
        // E2-A §2.3: the terminal Task closes its Execution
        // (as `failed`: Execution has no `rejected` word).
        close_execution_for_task(&state, &req.task_id, "rejected");
        return Json(updated).into_response();
    }
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response()
}

pub(crate) async fn tasks_resume(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&req.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    if !crate::task_store::valid_task_status_transition(&record.status, "running") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
    // Contract: resume is accepted, not completed. The returned
    // record is the pre-run snapshot (still interrupted); the
    // runner flips it to running or draft_ready promptly, and
    // callers poll until the status leaves interrupted. A
    // fabricated running record would lie about the runner's
    // state, and blocking here would stall on a model call.
    // P0 phase 1: a resume spawns a fresh runner attempt, so it
    // gets a fresh Execution shell record.
    // E2-A §2.3: a fresh attempt supersedes any still-running
    // Execution a crashed earlier attempt left behind.
    let execution = create_execution(&state, Some(req.task_id.clone()), None);
    supersede_old_task_executions(&state, &req.task_id, &execution.execution_id);
    tokio::spawn(crate::task_runner::run_task(state.clone(), req.task_id));
    Json(record).into_response()
}

pub(crate) async fn tasks_cancel(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&req.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    // E2-A §2.2: the lifecycle entries enforce the edge table;
    // anything else is 409 `task_status_transition_invalid`.
    // (The manual /tasks/update override keeps its own
    // word-whitelist contract and is not governed by it.)
    if !crate::task_store::valid_task_status_transition(&record.status, "cancelled") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
    match record.status.as_str() {
        "running" => {
            // The runner observes the flip at its next checkpoint
            // and discards the model result; no approval exists
            // yet, so nothing is left behind.
            let mut tasks = state.tasks.lock().expect("task lock");
            if let Some(current) = tasks.get_mut(&req.task_id) {
                if current.status != "running" {
                    return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not cancellable" })))
                        .into_response();
                }
                current.status = "cancelled".to_string();
                let updated = current.clone();
                persist_task(&state, &updated);
                drop(tasks);
                // E2-A §2.3: the terminal Task closes its Execution.
                close_execution_for_task(&state, &req.task_id, "cancelled");
                return Json(updated).into_response();
            }
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
                .into_response()
        }
        "draft_ready" => {
            let Some(approval_id) = record.approval_id.clone() else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "task approval missing" })),
                )
                    .into_response();
            };
            if let Err(error) = crate::task_runner::decide_rejected(&state, &approval_id) {
                if error == "approval already decided" {
                    return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": error })))
                        .into_response();
                }
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": error })))
                    .into_response();
            }
            let mut tasks = state.tasks.lock().expect("task lock");
            if let Some(current) = tasks.get_mut(&req.task_id) {
                if current.status != "draft_ready" {
                    return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not cancellable" })))
                        .into_response();
                }
                current.status = "cancelled".to_string();
                let updated = current.clone();
                persist_task(&state, &updated);
                drop(tasks);
                // E2-A §2.3: the terminal Task closes its Execution.
                close_execution_for_task(&state, &req.task_id, "cancelled");
                return Json(updated).into_response();
            }
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
                .into_response()
        }
        _ => (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not cancellable" })))
            .into_response(),
    }
}

/// ORC-1 plan DTOs (plan §3, Q-ORC-03/04/05).
/// The DTO carries no `digest` field and no
/// `execution_id`: the digest is recomputed
/// server-side from the validated arrays, and a draft
/// has no Execution yet (execution_id is NULL).
#[derive(Debug, Deserialize)]
pub(crate) struct PlanStepDto {
    step_no: i64,
    kind: String,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    change: Option<String>,
    #[serde(default)]
    verify_cmd: Option<String>,
    #[serde(default)]
    expected: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PlanReplanIfDto {
    signal: String,
    action: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PlanDraftDto {
    task_id: String,
    steps: Vec<PlanStepDto>,
    #[serde(default)]
    replan_if: Vec<PlanReplanIfDto>,
    #[serde(default)]
    planner_model: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PlanListQuery {
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    offset: Option<usize>,
}

/// Validate one draft step (Q-ORC-04): `kind` must
/// be one of the four blueprint kinds; the
/// four-piece set — `path` / `symbol` / `change` /
/// `verify_cmd` / `expected` — must be present and
/// non-empty. An empty string counts as missing: a
/// step that names nothing cannot be executed
/// "按图索骥".
fn non_empty_field(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|text| !text.is_empty())
}

fn validate_plan_step(step: &PlanStepDto) -> Result<PlanStep, String> {
    if !matches!(
        step.kind.as_str(),
        "file_write" | "command" | "verify" | "review"
    ) {
        return Err(format!("invalid plan step kind: {}", step.kind));
    }
    let path = non_empty_field(&step.path).ok_or("plan step missing path")?;
    let symbol = non_empty_field(&step.symbol).ok_or("plan step missing symbol")?;
    let change = non_empty_field(&step.change).ok_or("plan step missing change")?;
    let verify_cmd = non_empty_field(&step.verify_cmd).ok_or("plan step missing verify_cmd")?;
    let expected = non_empty_field(&step.expected).ok_or("plan step missing expected")?;
    Ok(PlanStep {
        step_no: step.step_no,
        kind: step.kind.clone(),
        path: path.to_string(),
        symbol: symbol.to_string(),
        change: change.to_string(),
        verify_cmd: verify_cmd.to_string(),
        expected: expected.to_string(),
    })
}

/// ORC-1 (plan §3): draft a plan version for a task.
/// The plan belongs to the task, so the task must
/// exist — unknown task is 404, the `tasks_*`
/// unknown-task wording. A second draft for the same
/// task supersedes the current version in the same
/// transaction (Q-ORC-12). ORC-1 has NO approval
/// flow: this route creates the plan row + digest
/// only; no approve route, no approval tool, no
/// approval arm is added here.
pub(crate) async fn tasks_plan_draft(
    State(state): State<AppState>,
    Json(req): Json<PlanDraftDto>,
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
    // The tasks lock is held across the whole draft:
    // existence, the plan write, and the pointer
    // update serialize, so two concurrent drafts for
    // one task cannot leave `plan_id` pointing at the
    // superseded version.
    let mut tasks = state.tasks.lock().expect("task lock");
    let Some(record) = tasks.get_mut(&req.task_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    if req.steps.is_empty() {
        return bad_request("plan steps must be a non-empty array");
    }
    // step_no must be 1-up consecutive and unique
    // (LOCAL DEFINITION — the plan leaves the step_no
    // rule to the implementer; a gap or a duplicate
    // would make the Executor's order ambiguous).
    let mut steps: Vec<PlanStep> = Vec::with_capacity(req.steps.len());
    for (index, step) in req.steps.iter().enumerate() {
        if step.step_no != index as i64 + 1 {
            return bad_request("plan step_no must be consecutive starting at 1");
        }
        match validate_plan_step(step) {
            Ok(valid) => steps.push(valid),
            Err(message) => return bad_request(&message),
        }
    }
    // Re-plan-if (Q-ORC-05): `signal` is free text, but
    // an empty signal can never trigger, so it is
    // refused; `action` has exactly one value.
    let mut replan_if: Vec<ReplanIf> = Vec::with_capacity(req.replan_if.len());
    for condition in &req.replan_if {
        if condition.signal.trim().is_empty() {
            return bad_request("plan replan_if signal must not be empty");
        }
        if condition.action != "stop_and_hand_back" {
            return bad_request("plan replan_if action must be stop_and_hand_back");
        }
        replan_if.push(ReplanIf {
            signal: condition.signal.clone(),
            action: condition.action.clone(),
        });
    }
    // planner_model defaults to the gateway's current
    // model string (LOCAL DEFINITION — the plan leaves
    // the default to the implementer): the staged
    // model when one is staged, else the running
    // DeepSeek config's model — the same precedence
    // `model_view` uses in routes_misc.rs.
    let planner_model = req.planner_model.clone().unwrap_or_else(|| {
        state
            .model_staged
            .lock()
            .expect("model staged lock")
            .as_ref()
            .and_then(|staged| staged.model.clone())
            .unwrap_or_else(|| state.deepseek.config().model.clone())
    });
    let steps_value = serde_json::to_value(&steps).unwrap_or(serde_json::Value::Null);
    let replan_value = serde_json::to_value(&replan_if).unwrap_or(serde_json::Value::Null);
    let digest = plan_digest(
        &req.task_id,
        &crate::sqlite_store::canonical_json(&steps_value),
        &crate::sqlite_store::canonical_json(&replan_value),
    );
    let mut conn = sqlite.lock().expect("sqlite lock");
    let row = match crate::plan_store::create_plan(
        &mut conn,
        &req.task_id,
        &steps,
        &replan_if,
        &planner_model,
        &digest,
        &state.origin_device_id,
    ) {
        Ok(row) => row,
        Err(detail) => {
            eprintln!("warning: plan draft failed ({detail})");
            return sqlite_error_response();
        }
    };
    // Point the task at its new current plan (a
    // pointer only — the `plans` table is the fact
    // source).
    record.plan_id = Some(row.plan_id.clone());
    let updated = record.clone();
    persist_task(&state, &updated);
    drop(tasks);
    match row.to_record() {
        Ok(record) => Json(record).into_response(),
        Err(detail) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": detail })),
        )
            .into_response(),
    }
}

/// ORC-1 (plan §3): read plans. With `?task_id=`
/// the task's current plan object (unknown task or
/// no plan yet: 404, the `tasks_draft` wording);
/// without it a bare array of all plans, newest
/// first, default 50, max 200 (the MCL list
/// precedent).
pub(crate) async fn tasks_plan_list(
    State(state): State<AppState>,
    Query(query): Query<PlanListQuery>,
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
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let offset = query.offset.unwrap_or(0);
    if let Some(task_id) = &query.task_id {
        if !state.tasks.lock().expect("task lock").contains_key(task_id) {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
                .into_response();
        }
        return match crate::plan_store::current_plan_for_task(&conn, task_id) {
            Ok(Some(row)) => match row.to_record() {
                Ok(record) => Json(record).into_response(),
                Err(detail) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": detail })),
                )
                    .into_response(),
            },
            Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "no plan" })))
                .into_response(),
            Err(detail) => {
                eprintln!("warning: plan list failed ({detail})");
                sqlite_error_response()
            }
        };
    }
    match crate::plan_store::list_plans(&conn, None, limit, offset) {
        Ok(rows) => {
            let mut records = Vec::with_capacity(rows.len());
            for row in rows {
                match row.to_record() {
                    Ok(record) => records.push(record),
                    Err(detail) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({ "error": detail })),
                        )
                            .into_response();
                    }
                }
            }
            Json(records).into_response()
        }
        Err(detail) => {
            eprintln!("warning: plan list failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// ORC-1 (plan §3): one plan version by local
/// `plan-N` id. Unknown id is 404.
pub(crate) async fn tasks_plan_get(
    State(state): State<AppState>,
    Path(plan_id): Path<String>,
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
    match crate::plan_store::get_plan(&conn, &plan_id) {
        Ok(Some(row)) => match row.to_record() {
            Ok(record) => Json(record).into_response(),
            Err(detail) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": detail })),
            )
                .into_response(),
        },
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown plan" })))
            .into_response(),
        Err(detail) => {
            eprintln!("warning: plan get failed ({detail})");
            sqlite_error_response()
        }
    }
}
