//! - POST /agent/tool/call       -> execute a tool, server re-checks its own approval record
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
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::effect_store::NewEffect;
use crate::loop_engine::{LoopError, LoopStatus, LoopView};
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
    let result = state.loops.start(req);
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
    let mut views = state.loops.list();
    for view in &mut views {
        fill_pending_reminder_title(&state, view);
    }
    Json(views)
}
