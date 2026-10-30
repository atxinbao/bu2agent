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
mod session_commands;
mod gateway_endpoint;
mod gateway_home;
mod gateway_lifecycle;
mod settings_commands;
mod settings_store;
mod tool_commands;
mod loop_commands;

#[cfg(test)]
mod shell_tests;

use serde::Serialize;
use tauri::Manager;

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
    // Plugin registration is skipped in test builds: mock_builder
    // builds several apps in one test process, and plugins do OS-level
    // registration. The lifecycle setup is skipped there for the same
    // reason; shell tests use the script-started gateway instead.
    #[cfg(not(test))]
    let builder = tauri::Builder::default()
        // 单实例插件必须先注册，避免第二实例初始化其它插件。
        .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            // A second launch hands off to this instance and exits
            // without reaching setup, so it can never spawn a second
            // gateway. Just bring the existing window forward.
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.unminimize();
                let _ = window.set_focus();
            }
        }))
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init());
    #[cfg(test)]
    let builder = tauri::Builder::default();
    let app = builder
        .manage(gateway_lifecycle::GatewayLifecycle::default())
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
            tool_commands::file_stat,
            tool_commands::file_read_text,
            tool_commands::file_write,
            tool_commands::file_delete,
            tool_commands::file_rename,
            tool_commands::file_mkdir,
            tool_commands::file_reads,
            tool_commands::file_stats,
            tool_commands::file_search,
            tool_commands::file_subscribe,
            tool_commands::file_thumbnail,
            tool_commands::file_export,
            tool_commands::file_library,
            tool_commands::agent_request_approval,
            tool_commands::agent_decide_approval,
            tool_commands::agent_call_tool,
            tool_commands::agent_run_tool,
            tool_commands::connector_notes_list,
            tool_commands::connector_mail_list,
            tool_commands::connector_notes_create,
            tool_commands::connector_mail_send,
            tool_commands::connector_list,
            tool_commands::connector_get,
            tool_commands::connector_accounts,
            tool_commands::connector_account_link,
            tool_commands::connector_account_unlink,
            tool_commands::connector_connect_info,
            tool_commands::connector_connect,
            tool_commands::connector_disconnect,
            tool_commands::vault_credentials_list,
            tool_commands::vault_credential_register,
            tool_commands::vault_credential_delete,
            tool_commands::vault_credential_capture,
            tool_commands::vault_capture_request,
            tool_commands::vault_capture_fulfill,
            tool_commands::vault_catalog_list,
            tool_commands::vault_catalog_details,
            tool_commands::vault_catalog_update,
            tool_commands::vault_catalog_delete,
            tool_commands::vault_custom_register,
            tool_commands::vault_custom_list,
            tool_commands::vault_custom_delete,
            tool_commands::task_create,
            tool_commands::task_list,
            tool_commands::task_get,
            tool_commands::task_update,
            tool_commands::task_run,
            tool_commands::task_draft,
            tool_commands::task_execute,
            tool_commands::task_reject,
            tool_commands::task_resume,
            tool_commands::task_cancel,
            tool_commands::device_reminders_list,
            tool_commands::device_actions_create,
            tool_commands::device_reminder_set,
            tool_commands::device_action_mark_rejected,
            tool_commands::device_action_refresh,
            tool_commands::device_actions_list,
            tool_commands::device_action_get,
            loop_commands::loop_start,
            loop_commands::loop_get,
            loop_commands::loop_resume,
            loop_commands::loop_cancel,
            loop_commands::loop_list,
            session_commands::session_create,
            session_commands::session_list,
            session_commands::session_get,
            session_commands::session_send,
            session_commands::session_delete,
            session_commands::session_list_including_archived,
            session_commands::session_pin,
            session_commands::session_unpin,
            session_commands::session_archive,
            session_commands::session_unarchive,
            session_commands::session_rename,
            gateway_lifecycle::gateway_status,
            gateway_lifecycle::gateway_restart,
            settings_commands::settings_get,
            settings_commands::settings_pick_directory,
            settings_commands::settings_save_working_dir,
            settings_commands::settings_clear_working_dir,
            settings_commands::settings_key_status,
            settings_commands::settings_key_save,
            settings_commands::settings_key_remove,
            settings_commands::device_authorization
        ])
        .setup(|_app| {
            #[cfg(not(test))]
            gateway_lifecycle::setup(_app.handle());
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while running tauri application");
    app.run(|handle, event| {
        if let tauri::RunEvent::Exit = event {
            if let Some(lifecycle) = handle.try_state::<gateway_lifecycle::GatewayLifecycle>() {
                lifecycle.stop_owned();
            }
        }
    });
}
