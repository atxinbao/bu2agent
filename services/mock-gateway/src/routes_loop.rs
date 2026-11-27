//! - POST /agent/tool/call       -> execute a tool, server re-checks its own approval record
//! - POST /agent/loop/research/start -> start a Planner research loop (planning phase only)
//! - GET /agent/loop/research/{id}  -> read one research loop (kind must be "research")
//!
//! Loop routes: /agent (6) — agent_tool_call + loop
//! start/get/resume/cancel/list.
//!
//! The handlers moved here from `server.rs` (FU-5 S-9d). One
//! directory line moved with this batch: the header's
//! `/agent/tool/call` line (line 18); the `/agent/loop/*` routes
//! have no directory lines. The directory total stays 59
//! (35 server.rs + 13 files + 1 misc + 9 chat + 1 loop).
//! `WriteGate` / `bound_write_gate` stay in `server.rs` (already
//! `pub(crate)`): the S-7 routes_files.rs module calls them at 5
//! sites and imports them from `crate::server` (its header
//! documents them as staying); moving them would force a
//! routes_files -> routes_loop import, which the
//! no-inter-module-import rule forbids. The moved agent_tool_call
//! calls `crate::server::bound_write_gate` instead.
//! `dispatch_agent_tool` and `ToolCallDto` stay in `server.rs` as
//! `pub(crate)`: the staying `execute_loop_tool` AppState method
//! calls dispatch_agent_tool and constructs ToolCallDto literals
//! (S-6 execution_for_task / S-9c registry-core staying-helper
//! precedent).
//! `approval_status_name`, `close_execution_for_loop`,
//! `resolve_worktree_binding`, `create_execution_with_binding`
//! stay for the same reason (staying callers; loop_engine.rs
//! also calls close_execution_for_loop via crate::server::).
//! The 13 /api/email routes stay in server.rs (S-9h, after S-9g).

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::effect_store::NewEffect;
use crate::loop_engine::{LoopError, LoopStatus, LoopView, StartLoop};
use crate::routes_common::bad_request;
use crate::server::{
    adapter_execution, approval_status_name, bound_write_gate, check_approval,
    close_execution_for_loop, consume_bound_approval, consume_bound_approval_for_effect,
    create_execution_with_binding, dispatch_agent_tool, record_effect,
    resolve_worktree_binding, start_bound_effect, transition_effect, AppState, ToolCallDto,
    WriteGate,
};
use crate::tool_requires_approval;


pub(crate) async fn agent_tool_call(
    State(state): State<AppState>,
    Json(req): Json<ToolCallDto>,
) -> impl IntoResponse {
    let Some(requires) = tool_requires_approval(&req.tool) else {
        return bad_request("unknown tool");
    };
    if requires {
        // File tools go through the bound gate: the digest is computed
        // from the very args about to be dispatched, so an approval
        // for one write can never authorize a different write. Other
        // approval tools keep the status-only gate (their endpoints
        // are not file execution paths).
        let bound = match req.tool.as_str() {
            "fs.write" => {
                let (Some(path), Some(text)) = (req.args.get("path"), req.args.get("text")) else {
                    return bad_request("fs.write requires path and text args");
                };
                crate::operation_binding::write_digest(path, text.as_bytes(), false)
            }
            "fs.delete" => {
                let Some(path) = req.args.get("path") else {
                    return bad_request("fs.delete requires a path arg");
                };
                crate::operation_binding::delete_digest(path)
            }
            _ => None,
        };
        if let Some(digest) = bound {
            // P0-C: resolve the bound Effect before consuming.
            // A terminal Effect replays without dispatching; an
            // unsettled one refuses; a planned/authorized one is
            // consumed bound to it and driven forward. Legacy
            // approvals with no planned row keep the old gate
            // and terminal-row recording.
            let gate = match bound_write_gate(&state, &req.tool_call_id, &digest) {
                Ok(gate) => gate,
                Err(response) => return *response,
            };
            let bound_effect = match gate {
                WriteGate::Replay(effect) => {
                    return Json(serde_json::json!({
                        "result": "replayed",
                        "effect_id": effect.effect_id,
                        "replayed": true,
                    }))
                    .into_response();
                }
                WriteGate::Proceed(effect) => effect,
            };
            if let Some(effect) = &bound_effect {
                if let Err(response) = consume_bound_approval_for_effect(
                    &state,
                    &req.tool,
                    &req.tool_call_id,
                    &digest,
                    Some(&effect.effect_id),
                ) {
                    return *response;
                }
                if let Err(response) = start_bound_effect(&state, effect) {
                    return *response;
                }
            } else if let Err(response) =
                consume_bound_approval(&state, &req.tool, &req.tool_call_id, &digest)
            {
                return *response;
            }
            // Dispatch the normalized path so the executed path is
            // exactly the path the digest bound.
            let mut req = req;
            if let Some(path) = req.args.get("path").cloned() {
                if let Some(normalized) = crate::operation_binding::normalize_path(&path) {
                    req.args.insert("path".to_string(), normalized);
                }
            }
            let scope = req.args.get("path").cloned().unwrap_or_default();
            let operation = match req.tool.as_str() {
                "fs.write" => "write",
                _ => "delete",
            };
            let approval_id = req.tool_call_id.clone();
            let response = dispatch_agent_tool(&state, req);
            let succeeded = response.status().is_success();
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(
                    &state,
                    &effect.effect_id,
                    if succeeded { "succeeded" } else { "failed" },
                    None,
                    None,
                );
            } else if succeeded {
                // Legacy fallback: one terminal row, new word.
                record_effect(
                    &state,
                    NewEffect {
                        execution_id: Some(adapter_execution(&state)),
                        capability: "fs".to_string(),
                        operation: operation.to_string(),
                        scope,
                        input_digest: Some(digest),
                        approval_id,
                        status: "succeeded".to_string(),
                    },
                );
            }
            return response;
        }
        if let Err(response) = check_approval(&state, &req.tool, &req.tool_call_id) {
            return *response;
        }
    }
    dispatch_agent_tool(&state, req)
}


#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LoopIdDto {
    loop_id: String,
}

pub(crate) fn loop_response(
    state: &AppState,
    result: Result<crate::loop_engine::LoopView, LoopError>,
) -> axum::response::Response {
    match result {
        Ok(mut view) => {
            if view.pending_write.is_some() || view.pending_reminder.is_some() {
                if let Some(call) = view.record.calls.get(view.record.queue_position) {
                    if let (Some(id), Some(binding)) = (&call.approval_id, &call.binding) {
                        view.approval_status = state.loop_approval_status(id, binding).ok()
                            .map(|status| approval_status_name(status).to_string());
                    }
                }
            }
            fill_pending_reminder_title(state, &mut view);
            Json(view).into_response()
        },
        Err(LoopError(status, code)) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(serde_json::json!({"error":code})),
        )
            .into_response(),
    }
}

pub(crate) fn fill_pending_reminder_title(state: &AppState, view: &mut LoopView) {
    if let Some(pending) = view.pending_reminder.as_mut() {
        if let Ok(Some(reminder)) = state.device_backend.get(&pending.reminder_id) {
            pending.title = reminder.title;
        }
    }
}

/// Start an auto-run driver unless auto-drive is off (loop_smoke)
/// or the loop already stopped. The driver re-checks the flag and
/// the loop state itself, so a redundant spawn exits at once.
pub(crate) fn spawn_driver(state: &AppState, loop_id: &str) {
    if !state.loops.auto_drive() {
        return;
    }
    let engine = Arc::clone(&state.loops);
    let app = state.clone();
    let loop_id = loop_id.to_string();
    tokio::spawn(async move {
        engine.drive_until_stopped(app, loop_id).await;
    });
}

pub(crate) async fn loop_start(
    State(state): State<AppState>,
    Json(req): Json<crate::loop_engine::StartLoop>,
) -> axum::response::Response {
    // P3-A: an explicitly bound worktree must exist and be
    // active; its binding freezes into the Execution snapshot.
    // A bad id is a caller error, before the loop starts.
    let binding = match &req.worktree_id {
        None => None,
        Some(worktree_id) => match resolve_worktree_binding(&state, worktree_id) {
            Ok(binding) => Some(binding),
            Err(detail) => return bad_request(detail),
        },
    };
    let session_id = req.session_id.clone();
    let goal_id = req.goal_id.clone();
    // AO-4 (J4/J6): memory is advisory context, loaded
    // here — the handler owns AppState/SQLite — before
    // the loop starts. Any load failure means "no
    // memory", never a failed start.
    let memory = crate::memory_context::load_loop_memory(&state, session_id.as_deref());
    let result = state.loops.start_with_memory(req, memory);
    if let Ok(view) = &result {
        // P0 phase 1: the loop attempt gets its Execution shell record.
        create_execution_with_binding(
            &state,
            None,
            Some(view.record.loop_id.clone()),
            binding,
            session_id,
            goal_id,
        );
        spawn_driver(&state, &view.record.loop_id);
    }
    loop_response(&state, result)
}
pub(crate) async fn loop_get(
    State(state): State<AppState>,
    Query(req): Query<LoopIdDto>,
) -> axum::response::Response {
    loop_response(&state, state.loops.get(&req.loop_id))
}
pub(crate) async fn loop_resume(
    State(state): State<AppState>,
    Json(req): Json<crate::loop_engine::ResumeLoop>,
) -> axum::response::Response {
    let engine = Arc::clone(&state.loops);
    let result = engine.resume(state.clone(), req).await;
    // A manual resume runs one step, then hands over to the
    // driver; a stopping view needs no driver. Frontend copy
    // says "run to next stopping point", not "step once".
    if let Ok(view) = &result {
        if view.record.status.terminal() {
            // E2-A §2.3: the manual resume ended the loop;
            // close its Execution (the driver never runs for
            // a stopping view, so this route owns the close).
            close_execution_for_loop(&state, &view.record.loop_id, &view.record.status);
        } else if view.record.status != LoopStatus::AwaitingApproval {
            spawn_driver(&state, &view.record.loop_id);
        }
    }
    loop_response(&state, result)
}
pub(crate) async fn loop_cancel(
    State(state): State<AppState>,
    Json(req): Json<LoopIdDto>,
) -> axum::response::Response {
    let result = state.loops.cancel(&req.loop_id);
    // E2-A §2.3: a cancelled loop closes its Execution.
    if let Ok(view) = &result {
        if view.record.status.terminal() {
            close_execution_for_loop(&state, &view.record.loop_id, &view.record.status);
        }
    }
    loop_response(&state, result)
}
pub(crate) async fn loops_list(State(state): State<AppState>) -> Json<Vec<LoopView>> {
    // ORC-4 (Q-R4-12): the list shows main loops
    // only; a research loop is visible through
    // research_get alone.
    let mut views = state.loops.list();
    views.retain(|view| view.record.kind != "research");
    for view in &mut views {
        fill_pending_reminder_title(&state, view);
    }
    Json(views)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResearchStartLoop {
    task_id: String,
    worktree_id: Option<String>,
    max_calls: Option<usize>,
    budget_ms: Option<u64>,
}

/// ORC-4 (Q-R4-01): the fixed research prompt
/// prefix. The Planner's question and the task file
/// context follow; the bound worktree (when one
/// exists) is named so the model passes it to
/// code.search / git.read / fs_read (R4-J5, R4-F2).
pub(crate) const RESEARCH_PROMPT_PREFIX: &str = "你是任务规划的只读调研员。只用 code_search、fs_read、git_read 调研，不写文件、不执行命令。先调研，再给出最终调研报告：先写结论，再列支撑结论的关键证据（文件路径、行号、命中行）。\n\n两个命名空间不要混：任务文件在文件根下，用不传 worktree_id 的 fs_read 读；有工作区时，仓库文件用传 worktree_id 的 fs_read 读整文件，搜内容用 code.search（path 必须是目录前缀，传文件路径会失败），读 git 状态或差异用 git_read。\n\n";

pub(crate) fn research_prompt(
    file_context: &str,
    worktree_id: Option<&str>,
    question: Option<&str>,
) -> String {
    let mut prompt = String::from(RESEARCH_PROMPT_PREFIX);
    if let Some(worktree_id) = worktree_id {
        prompt.push_str(&format!(
            "本次调研的工作区：{worktree_id}。code.search / git.read / fs_read 调用里把 worktree_id 传为该值；不传时用文件根。\n\n"
        ));
    }
    if let Some(question) = question {
        prompt.push_str(&format!("规划者的问题：{question}\n\n"));
    }
    if !file_context.trim().is_empty() {
        prompt.push_str("任务文件如下：\n");
        prompt.push_str(file_context);
    }
    prompt
}

/// ORC-4 (Q-R4-08 / R4-J1): planning phase = the
/// task is `running` and no Executor step has been
/// prepared yet. `executor_step_no` is set to
/// Some(step_no) by prepare_file_write_step /
/// prepare_worker_step the moment a step is prepared
/// (task_runner.rs), and by the manual re-plan route
/// (Some(1)); a failed/cancelled/handed-back task
/// fails the `running` check first.
pub(crate) fn check_research_phase(state: &AppState, task_id: &str) -> Result<(), LoopError> {
    let tasks = state.tasks.lock().expect("task lock");
    let Some(record) = tasks.get(task_id) else {
        return Err(LoopError(404, "unknown_task"));
    };
    if record.status != "running" || record.executor_step_no.is_some() {
        return Err(LoopError(409, "research_planning_only"));
    }
    Ok(())
}

/// ORC-4 (S-10): the worktree a research loop
/// searches. An explicit id wins; otherwise the
/// task's frozen execute_binding supplies it; no
/// binding means the files root (the existing
/// dispatch fallback). Existence is validated in
/// start_research, not here.
pub(crate) fn research_worktree_for_task(
    state: &AppState,
    task_id: &str,
    explicit: Option<String>,
) -> Option<String> {
    if let Some(id) = explicit {
        return Some(id);
    }
    let tasks = state.tasks.lock().expect("task lock");
    tasks
        .get(task_id)
        .and_then(|r| r.execute_binding.as_ref().map(|b| b.worktree_id.clone()))
}

/// ORC-4: the single research start path (R4-J2).
/// The HTTP route and the Planner's internal call
/// both come through here, so both get the same
/// phase check, worktree validation and model
/// override. No Execution shell is created (Q-R4-07);
/// the driver is spawned exactly like loop_start's.
pub(crate) fn start_research(
    state: &AppState,
    task_id: &str,
    prompt: String,
    worktree_id: Option<String>,
    max_calls: Option<usize>,
    budget_ms: Option<u64>,
) -> Result<LoopView, LoopError> {
    check_research_phase(state, task_id)?;
    if let Some(id) = &worktree_id {
        resolve_worktree_binding(state, id).map_err(|_| LoopError(400, "invalid_worktree"))?;
    }
    // Q-R4-06: the research model is the planner_model
    // slot. Building it is best-effort (R4-J4): no
    // API key (e.g. scripted-model tests) falls back
    // to the engine's own model, and research-ness
    // still comes from the kind; a loop with no model
    // at all fails in start_with_kind with 503.
    let model_override = {
        let config = state.deepseek.config();
        let planner_config = crate::deepseek::DeepSeekConfig {
            base_url: config.base_url.clone(),
            model: crate::task_runner::planner_model_for(state),
            api_key: config.api_key.clone(),
        };
        match crate::deepseek_loop::DeepSeekLoopModel::new(planner_config) {
            Ok(Some(model)) => Some(Arc::new(
                model.with_network_rules(state.policy_rules.clone()),
            ) as Arc<dyn crate::loop_engine::LoopModel>),
            _ => None,
        }
    };
    let view = state.loops.start_with_kind(
        StartLoop {
            prompt,
            max_calls,
            budget_ms,
            worktree_id,
            session_id: None,
            goal_id: None,
        },
        "research",
        model_override,
        // AO-4 (J4): research loops never carry memory.
        None,
    )?;
    spawn_driver(state, &view.record.loop_id);
    Ok(view)
}

pub(crate) async fn research_start(
    State(state): State<AppState>,
    Json(req): Json<ResearchStartLoop>,
) -> axum::response::Response {
    let worktree_id = research_worktree_for_task(&state, &req.task_id, req.worktree_id.clone());
    // The route's prompt carries the task file
    // context (best-effort: an unreadable directory
    // still starts a research loop on the task id
    // alone); the Planner's internal call passes its
    // own context + question instead.
    let prompt = {
        let directory = {
            let tasks = state.tasks.lock().expect("task lock");
            tasks.get(&req.task_id).map(|r| r.directory.clone())
        };
        let file_context = directory
            .and_then(|dir| crate::task_runner::read_task_files(&state, &dir).ok())
            .map(|parts| parts.join("\n\n"))
            .unwrap_or_default();
        research_prompt(&file_context, worktree_id.as_deref(), None)
    };
    loop_response(
        &state,
        start_research(
            &state,
            &req.task_id,
            prompt,
            worktree_id,
            req.max_calls,
            req.budget_ms,
        ),
    )
}

pub(crate) async fn research_get(
    State(state): State<AppState>,
    Path(loop_id): Path<String>,
) -> axum::response::Response {
    match state.loops.get(&loop_id) {
        Ok(view) if view.record.kind == "research" => loop_response(&state, Ok(view)),
        _ => loop_response(&state, Err(LoopError(404, "unknown_loop"))),
    }
}
