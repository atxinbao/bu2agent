//! Swarm routes: /swarm (3 routes).
//!
//! - `POST /swarm/create` — create a swarm (Q-R5-02).
//! - `POST /swarm/{id}/fanout` — validate the steps and queue the copy add-runs (Q-R5-03).
//! - `GET /swarm/{id}` — the swarm view (Q-R5-08, §6.3).
//!
//! A swarm fans a task's plan steps out to parallel
//! worker copies. The fanout route validates (steps
//! belong to the swarm's pinned plan, all
//! `file_write` / `command`, no two file-write steps
//! share a path, at most 4 steps) and queues one
//! `git worktree add` run per step in the parent
//! worktree — the scaffolding is authorized by the
//! fanout call itself, so the add-runs carry no
//! approval (the step approvals are built later, by
//! the sweeper drive, once the copies exist — the
//! command digest needs the copy's cwd, so an
//! approval cannot exist at fanout time).
//!
//! The task stays `draft_ready` through the whole
//! swarm (D1 red line 1: no `draft_ready → running`
//! edge); the swarm's own status carries progress.
//! The swarm id is `swarm-N`, the board message id
//! `swarm-msg-N`, both server-generated.
//!
//! ORC-5 branch naming (R5-J4): the draft's
//! `swarm/{swarm_id}/{worker_id}` branch contains
//! `/`, which `validate_branch_name` rejects (the
//! worker template only allows `[A-Za-z0-9._-]`),
//! so the branch is `swarm-{swarm_id}-{worker_id}`
//! — still unique per swarm + worker, still
//! namespaced. The copy directory is the same
//! shape, location-root-relative.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::plan_store::{current_plan_for_task, get_plan};
use crate::routes_common::{sqlite_error_response, sqlite_not_configured};
use crate::server::{
    approval_status, approval_status_name, create_worker_run_internal, execution_for_task,
    AppState, WorkerCreateError, WorkerRunCreateDto,
};
use crate::sqlite_store::{self, StateBackend};
use crate::swarm_store::{
    active_swarm_for_task, create_swarm, get_swarm, insert_swarm_message, list_swarm_messages,
    update_swarm, SwarmMessageView, SwarmRow, SwarmStep, SwarmStepView, SwarmView,
};

fn err_json(status: StatusCode, code: &str) -> Response {
    (status, Json(serde_json::json!({ "error": code }))).into_response()
}

fn sqlite_guard(state: &AppState) -> Option<Response> {
    if state.sqlite_is_broken() {
        return Some(sqlite_error_response());
    }
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return Some(sqlite_not_configured());
    }
    None
}

/// Assemble the swarm view (plan §6.3): the row's
/// steps (SQLite-only content fields dropped) with
/// each approval's live status, the latest board rows
/// (hashes only), and the worktree records still
/// present for this swarm (merge-cleanup leftovers).
pub(crate) fn swarm_view(state: &AppState, row: &SwarmRow, limit: i64) -> Response {
    let sqlite = match &state.sqlite {
        Some(sqlite) => sqlite,
        None => return sqlite_not_configured(),
    };
    let steps = match row.steps() {
        Ok(steps) => steps
            .into_iter()
            .map(|step| SwarmStepView {
                approval_status: step.approval_id.as_ref().and_then(|id| {
                    approval_status(state, id).map(|status| {
                        approval_status_name(status).to_string()
                    })
                }),
                step_no: step.step_no,
                worker_id: step.worker_id,
                worktree_id: step.worktree_id,
                run_id: step.run_id,
                run_status: step.run_status,
                approval_id: step.approval_id,
            })
            .collect(),
        Err(detail) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": detail })),
            )
                .into_response();
        }
    };
    let conn = sqlite.lock().expect("sqlite lock");
    let messages = match list_swarm_messages(&conn, &row.swarm_id, limit) {
        Ok(rows) => rows
            .into_iter()
            .map(|row| SwarmMessageView {
                message_id: row.message_id,
                sender_worker_id: row.sender_worker_id,
                kind: row.kind,
                body_hash: row.body_hash,
                created_ms: row.created_ms,
            })
            .collect(),
        Err(detail) => {
            eprintln!("warning: swarm messages read failed ({detail})");
            return sqlite_error_response();
        }
    };
    let prefix = format!("swarm-{}-", row.swarm_id);
    let mut leftover_worktrees: Vec<String> = state
        .worktrees
        .lock()
        .expect("worktree lock")
        .values()
        .filter(|w| {
            // Archived copies are gone (merge cleanup
            // archives the record); only active records
            // are leftovers.
            w.status == crate::worktree_store::WORKTREE_ACTIVE
                && (w.branch_label.starts_with(&prefix)
                    || w.directory_relative.starts_with(&prefix))
        })
        .map(|w| w.worktree_id.clone())
        .collect();
    leftover_worktrees.sort_by_key(|id| sqlite_store::id_suffix(id));
    Json(SwarmView {
        swarm_id: row.swarm_id.clone(),
        task_id: row.task_id.clone(),
        plan_id: row.plan_id.clone(),
        status: row.status.clone(),
        steps,
        messages,
        leftover_worktrees,
        created_ms: row.created_ms,
        updated_ms: row.updated_ms,
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct SwarmCreateDto {
    pub task_id: String,
}

/// ORC-5 (Q-R5-02): create the swarm row, pinning
/// the task's current plan. The task must be
/// `draft_ready` (a swarm is a parallel execution of
/// a planned task, not a new plan) and must have no
/// active swarm already.
pub(crate) async fn swarm_create(
    State(state): State<AppState>,
    Json(req): Json<SwarmCreateDto>,
) -> impl IntoResponse {
    if let Some(resp) = sqlite_guard(&state) {
        return resp;
    }
    let sqlite = state.sqlite.as_ref().expect("sqlite checked");
    let task = {
        let tasks = state.tasks.lock().expect("task lock");
        tasks.get(&req.task_id).cloned()
    };
    let Some(task) = task else {
        return err_json(StatusCode::NOT_FOUND, "unknown task");
    };
    if task.status != "draft_ready" {
        return err_json(StatusCode::CONFLICT, "task_not_draft_ready");
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match active_swarm_for_task(&conn, &req.task_id) {
        Ok(Some(_)) => return err_json(StatusCode::CONFLICT, "swarm_active"),
        Ok(None) => {}
        Err(detail) => {
            eprintln!("warning: active swarm check failed ({detail})");
            return sqlite_error_response();
        }
    }
    let plan = match current_plan_for_task(&conn, &req.task_id) {
        Ok(Some(plan)) => plan,
        Ok(None) => return err_json(StatusCode::NOT_FOUND, "unknown plan"),
        Err(detail) => {
            eprintln!("warning: current plan read failed ({detail})");
            return sqlite_error_response();
        }
    };
    drop(conn);
    let mut conn = sqlite.lock().expect("sqlite lock");
    let row = match create_swarm(
        &mut conn,
        &req.task_id,
        &plan.plan_id,
        &[],
        &state.origin_device_id,
    ) {
        Ok(row) => row,
        Err(detail) => {
            eprintln!("warning: swarm create failed ({detail})");
            return sqlite_error_response();
        }
    };
    // Drop the sqlite lock before building the view:
    // `swarm_view` locks it again for the board rows,
    // and the Mutex is not re-entrant.
    drop(conn);
    swarm_view(&state, &row, 50)
}

#[derive(Debug, Deserialize)]
pub(crate) struct SwarmFanoutDto {
    pub step_nos: Vec<i64>,
    #[serde(default)]
    pub priority: Option<i64>,
}

/// ORC-5 (Q-R5-03): validate the fan-out and queue
/// one `git worktree add` run per step. The route
/// validates everything before creating any run; a
/// mid-creation failure (a race on the path/branch)
/// leaves the swarm `created` with the steps
/// recorded so far, so a retry re-fans out and skips
/// the steps that already have an add-run.
pub(crate) async fn swarm_fanout(
    State(state): State<AppState>,
    Path(swarm_id): Path<String>,
    Json(req): Json<SwarmFanoutDto>,
) -> impl IntoResponse {
    if let Some(resp) = sqlite_guard(&state) {
        return resp;
    }
    let sqlite = state.sqlite.as_ref().expect("sqlite checked");
    if req.step_nos.is_empty() || req.step_nos.len() > 4 {
        return err_json(
            if req.step_nos.is_empty() {
                StatusCode::BAD_REQUEST
            } else {
                StatusCode::CONFLICT
            },
            if req.step_nos.is_empty() {
                "invalid_step_nos"
            } else {
                "swarm_fanout_cap_exceeded"
            },
        );
    }
    let mut sorted = req.step_nos.clone();
    sorted.sort_unstable();
    if sorted.windows(2).any(|w| w[0] == w[1]) {
        return err_json(StatusCode::BAD_REQUEST, "invalid_step_nos");
    }
    let swarm = {
        let conn = sqlite.lock().expect("sqlite lock");
        match get_swarm(&conn, &swarm_id) {
            Ok(Some(row)) => row,
            Ok(None) => return err_json(StatusCode::NOT_FOUND, "unknown swarm"),
            Err(detail) => {
                eprintln!("warning: swarm read failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    if swarm.status != "created" {
        return err_json(StatusCode::CONFLICT, "swarm_status_invalid");
    }
    let task = {
        let tasks = state.tasks.lock().expect("task lock");
        tasks.get(&swarm.task_id).cloned()
    };
    let Some(task) = task else {
        return err_json(StatusCode::NOT_FOUND, "unknown task");
    };
    if task.status != "draft_ready" {
        return err_json(StatusCode::CONFLICT, "task_not_draft_ready");
    }
    let plan = {
        let conn = sqlite.lock().expect("sqlite lock");
        match get_plan(&conn, &swarm.plan_id) {
            Ok(Some(plan)) => plan,
            Ok(None) => return err_json(StatusCode::NOT_FOUND, "unknown plan"),
            Err(detail) => {
                eprintln!("warning: swarm plan read failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    let current = {
        let conn = sqlite.lock().expect("sqlite lock");
        match current_plan_for_task(&conn, &swarm.task_id) {
            Ok(Some(plan)) => plan,
            Ok(None) => return err_json(StatusCode::NOT_FOUND, "unknown plan"),
            Err(detail) => {
                eprintln!("warning: current plan read failed ({detail})");
                return sqlite_error_response();
            }
        }
    };
    if current.plan_id != swarm.plan_id {
        return err_json(StatusCode::CONFLICT, "plan_superseded");
    }
    let Some(execution_id) = execution_for_task(&state, &swarm.task_id) else {
        return err_json(StatusCode::CONFLICT, "task_execution_missing");
    };
    let Some(binding) = &task.execute_binding else {
        return err_json(StatusCode::CONFLICT, "task_binding_missing");
    };
    let plan_steps = match plan.steps() {
        Ok(steps) => steps,
        Err(detail) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": detail })),
            )
                .into_response();
        }
    };
    let mut steps: Vec<SwarmStep> = Vec::with_capacity(req.step_nos.len());
    let mut seen_paths: Vec<String> = Vec::new();
    for step_no in &req.step_nos {
        let Some(plan_step) = plan_steps.iter().find(|s| s.step_no == *step_no) else {
            return err_json(StatusCode::BAD_REQUEST, "unknown_plan_step");
        };
        if !matches!(plan_step.kind.as_str(), "file_write" | "command") {
            return err_json(StatusCode::CONFLICT, "swarm_step_kind_unsupported");
        }
        if plan_step.kind == "file_write" {
            if seen_paths.contains(&plan_step.path) {
                return err_json(StatusCode::CONFLICT, "swarm_path_overlap");
            }
            seen_paths.push(plan_step.path.clone());
        }
        steps.push(SwarmStep::new(*step_no, format!("w{step_no}")));
    }
    // Queue the copy add-runs in the parent worktree
    // (the task's frozen binding). The add-run's
    // `swarm_id` bypasses the one-run-per-Execution
    // guard — the N copies are created in parallel by
    // design.
    let mut first_error: Option<WorkerCreateError> = None;
    for step in steps.iter_mut() {
        let worker_id = step.worker_id.clone();
        let path = format!("swarm-{}-{worker_id}", swarm.swarm_id);
        let branch = format!("swarm-{}-{worker_id}", swarm.swarm_id);
        let dto = WorkerRunCreateDto::internal(
            binding.worktree_id.clone(),
            "git-worktree-add".to_string(),
            Some(serde_json::json!({ "path": path, "branch": branch })),
            Some(execution_id.clone()),
            None,
        )
        .with_swarm_id(swarm.swarm_id.clone())
        .with_priority(req.priority);
        match create_worker_run_internal(&state, dto).await {
            Ok(row) => {
                step.add_run_id = Some(row.run_id);
                step.run_status = "pending".to_string();
            }
            Err(error) => {
                first_error = Some(error);
                break;
            }
        }
    }
    let status = if first_error.is_none() {
        "fanning_out"
    } else {
        "created"
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = update_swarm(&mut conn, &swarm.swarm_id, &steps, status) {
        eprintln!("warning: swarm fanout persist failed ({detail})");
        return sqlite_error_response();
    }
    if let Some(error) = first_error {
        return (error.status, Json(serde_json::json!({ "error": error.code }))).into_response();
    }
    let row = match get_swarm(&conn, &swarm.swarm_id) {
        Ok(Some(row)) => row,
        _ => return sqlite_error_response(),
    };
    drop(conn);
    swarm_view(&state, &row, 50)
}

#[derive(Debug, Deserialize)]
pub(crate) struct SwarmGetQuery {
    #[serde(default)]
    pub limit: Option<i64>,
}

/// ORC-5 (Q-R5-08): the swarm view. `limit`
/// defaults to 50 board rows, capped at 200 — no
/// pagination yet.
pub(crate) async fn swarm_get(
    State(state): State<AppState>,
    Path(swarm_id): Path<String>,
    Query(query): Query<SwarmGetQuery>,
) -> impl IntoResponse {
    if let Some(resp) = sqlite_guard(&state) {
        return resp;
    }
    let sqlite = state.sqlite.as_ref().expect("sqlite checked");
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let conn = sqlite.lock().expect("sqlite lock");
    match get_swarm(&conn, &swarm_id) {
        Ok(Some(row)) => {
            drop(conn);
            swarm_view(&state, &row, limit)
        }
        Ok(None) => err_json(StatusCode::NOT_FOUND, "unknown swarm"),
        Err(detail) => {
            eprintln!("warning: swarm get failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// Board write helper for the sweeper drive: hash
/// the body text here, at the gateway, and persist
/// only the hash (Q-ORC-17 / red line 7).
pub(crate) fn swarm_board_write(
    state: &AppState,
    swarm_id: &str,
    sender_worker_id: &str,
    kind: &str,
    body: &str,
) {
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let body_hash = crate::operation_binding::sha256_hex(body.as_bytes());
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = insert_swarm_message(
        &mut conn,
        swarm_id,
        sender_worker_id,
        kind,
        &body_hash,
        &state.origin_device_id,
    ) {
        eprintln!("warning: swarm board write failed ({detail})");
    }
}
