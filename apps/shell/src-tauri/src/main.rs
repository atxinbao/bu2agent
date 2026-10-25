// Tauri 2 shell for the bu2agent client.
//
// The frontend (apps/web, React) talks to the Mock Gateway
// through invoke commands backed by the Rust client
// (apps/desktop): gateway_commands.rs for the chat line,
// tool_commands.rs for the file, agent, connector, task, and device views.
// In a plain browser preview the frontend falls back to HTTP;
// inside this shell, invoke is the transport. The two commands
// below exist to prove the bridge works end to end: the
// frontend shows their result in its status line.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod gateway_commands;
mod tool_commands;

#[cfg(test)]
mod shell_tests;

use serde::Serialize;

#[derive(Serialize)]
struct AppInfo {
    name: &'static str,
    version: &'static str,
}

#[tauri::command]
fn ping() -> &'static str {
    "pong"
}

#[tauri::command]
fn app_info() -> AppInfo {
    AppInfo {
        name: "bu2agent",
        version: env!("CARGO_PKG_VERSION"),
    }
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![
            ping,
            app_info,
            gateway_commands::gateway_health,
            gateway_commands::gateway_history,
            gateway_commands::gateway_stream_start,
            gateway_commands::gateway_stream_apply,
            gateway_commands::gateway_stream_cancel,
            gateway_commands::gateway_approval_check,
            tool_commands::file_list,
            tool_commands::file_read_text,
            tool_commands::file_write,
            tool_commands::file_delete,
            tool_commands::agent_request_approval,
            tool_commands::agent_decide_approval,
            tool_commands::agent_call_tool,
            tool_commands::agent_run_tool,
            tool_commands::connector_notes_list,
            tool_commands::connector_mail_list,
            tool_commands::connector_notes_create,
            tool_commands::connector_mail_send,
            tool_commands::task_create,
            tool_commands::task_list,
            tool_commands::task_get,
            tool_commands::task_update,
            tool_commands::device_reminders_list,
            tool_commands::device_actions_create,
            tool_commands::device_reminder_set,
            tool_commands::device_action_mark_rejected,
            tool_commands::device_action_refresh,
            tool_commands::device_actions_list,
            tool_commands::device_action_get
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
