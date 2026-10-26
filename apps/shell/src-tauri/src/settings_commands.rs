//! Tauri commands for the Round 2 settings page.
//!
//! Three areas, kept separate on purpose:
//! - Default working directory: stored in settings.json as a
//!   root-relative path (see settings_store.rs). The file root
//!   never moves; the directory is only the default location
//!   for new files and tasks, and an explicitly entered path
//!   always wins. While attached to a foreign gateway the
//!   working-dir section is read-only: the foreign root is
//!   unknown to the shell, so nothing can be validated
//!   against it. Clearing back to the root stays allowed.
//! - Model credential: the existing Keychain store, service
//!   `bu2agent`, handle `deepseek` (the same slot the gateway
//!   reads at startup). The page never receives a secret back:
//!   status returns only configured + a ****last-4 redaction.
//!   Save and effective state are separate: after a save the
//!   page says a restart is needed, with a restart button for
//!   an owned gateway and a manual-restart note for an
//!   attached one. The gateway reads credentials at startup,
//!   so a save alone never changes a running gateway.
//! - Status lines: gateway (the existing gateway_status
//!   command), model (Keychain presence only -- this checks
//!   configuration, not a live model call), reminders (the
//!   gateway's authorization endpoint, new device command
//!   below). No secret, key, or response body ever reaches
//!   the frontend beyond the redacted tail above.
//!
//! 目录选择回调通过容量为一的异步通道返回，避免阻塞处理系统对话框的主线程。
//! It never creates directories:
//! only existing directories inside the file root are
//! accepted, and the save step re-validates (the directory
//! could move between pick and save).

use std::path::Path;

use serde::Serialize;
use tauri::{AppHandle, State};
use tauri_plugin_dialog::DialogExt;

use agent_desktop::credential_store::{CredentialStore, KeychainCredentialStore};
use agent_desktop::device_client::DeviceClient;
use agent_desktop::UnifiedError;

use crate::gateway_lifecycle::GatewayLifecycle;
use crate::settings_store;

fn err_string(error: &UnifiedError) -> String {
    format!("{:?}: {}", error.code, error.redacted_detail)
}

#[derive(Debug, Serialize)]
pub struct SettingsOut {
    pub default_working_dir: Option<String>,
    pub working_dir_display: String,
    pub working_dir_valid: bool,
    pub file_root: String,
    pub working_dir_editable: bool,
}

#[derive(Debug, Serialize)]
pub struct KeyStatusOut {
    pub configured: bool,
    pub redacted: Option<String>,
}

fn current_settings(lifecycle: &GatewayLifecycle) -> SettingsOut {
    let root = settings_store::file_root();
    let stored = settings_store::load_settings();
    let resolved = settings_store::resolve_stored(&root, &stored.default_working_dir);
    SettingsOut {
        default_working_dir: if resolved.relative.is_empty() {
            None
        } else {
            Some(resolved.relative)
        },
        working_dir_display: resolved.absolute_display,
        working_dir_valid: resolved.valid,
        file_root: root.to_string_lossy().into_owned(),
        working_dir_editable: !lifecycle.is_attached(),
    }
}

fn require_editable(lifecycle: &GatewayLifecycle) -> Result<(), String> {
    if lifecycle.is_attached() {
        return Err("附着网关的文件根未知，默认工作目录暂不可设置".to_string());
    }
    Ok(())
}

#[tauri::command]
pub fn settings_get(lifecycle: State<'_, GatewayLifecycle>) -> SettingsOut {
    current_settings(&lifecycle)
}

#[tauri::command]
pub async fn settings_pick_directory(
    app: AppHandle,
    lifecycle: State<'_, GatewayLifecycle>,
) -> Result<Option<String>, String> {
    require_editable(&lifecycle)?;
    let root = settings_store::file_root();
    let stored = settings_store::load_settings();
    let resolved = settings_store::resolve_stored(&root, &stored.default_working_dir);
    let start = if resolved.valid {
        Path::new(&resolved.absolute_display).to_path_buf()
    } else {
        root.clone()
    };
    let (tx, mut rx) = tauri::async_runtime::channel(1);
    app.dialog()
        .file()
        .set_title("选择默认工作目录")
        .set_directory(&start)
        .set_can_create_directories(false)
        .pick_folder(move |picked| {
            let _ = tx.try_send(picked.and_then(|p| p.into_path().ok()));
        });
    let picked = rx
        .recv()
        .await
        .ok_or_else(|| "directory picker closed unexpectedly".to_string())?;
    let Some(path) = picked else {
        return Ok(None);
    };
    // Validate now so the page shows an out-of-root error
    // immediately; the save step re-validates.
    settings_store::validate_working_dir(&root, &path)?;
    Ok(Some(path.to_string_lossy().into_owned()))
}

#[tauri::command(rename_all = "snake_case")]
pub fn settings_save_working_dir(
    path: String,
    lifecycle: State<'_, GatewayLifecycle>,
) -> Result<SettingsOut, String> {
    require_editable(&lifecycle)?;
    let root = settings_store::file_root();
    let relative = settings_store::validate_working_dir(&root, Path::new(&path))?;
    let stored = if relative.is_empty() {
        None
    } else {
        Some(relative)
    };
    settings_store::save_settings(&settings_store::SettingsFile {
        default_working_dir: stored,
    })?;
    Ok(current_settings(&lifecycle))
}

#[tauri::command]
pub fn settings_clear_working_dir(
    lifecycle: State<'_, GatewayLifecycle>,
) -> Result<SettingsOut, String> {
    // Clearing back to the root is safe even while attached:
    // it removes a preference, it never validates a new path.
    settings_store::save_settings(&settings_store::SettingsFile {
        default_working_dir: None,
    })?;
    Ok(current_settings(&lifecycle))
}

fn key_store() -> KeychainCredentialStore {
    KeychainCredentialStore::new()
}

fn key_status() -> Result<KeyStatusOut, String> {
    match key_store().redacted("deepseek") {
        Ok(redacted) => Ok(KeyStatusOut {
            configured: true,
            redacted: Some(redacted),
        }),
        Err(error) if error.redacted_detail == "credential handle not found" => Ok(KeyStatusOut {
            configured: false,
            redacted: None,
        }),
        Err(error) => Err(err_string(&error)),
    }
}

#[tauri::command]
pub fn settings_key_status() -> Result<KeyStatusOut, String> {
    key_status()
}

#[tauri::command]
pub fn settings_key_save(secret: String) -> Result<KeyStatusOut, String> {
    if secret.is_empty() {
        return Err("secret must not be empty".to_string());
    }
    key_store()
        .store("deepseek", &secret)
        .map_err(|e| err_string(&e))?;
    key_status()
}

#[tauri::command]
pub fn settings_key_remove() -> Result<KeyStatusOut, String> {
    match key_store().remove("deepseek") {
        Ok(()) => Ok(KeyStatusOut {
            configured: false,
            redacted: None,
        }),
        Err(error) if error.redacted_detail == "credential handle not found" => Ok(KeyStatusOut {
            configured: false,
            redacted: None,
        }),
        Err(error) => Err(err_string(&error)),
    }
}

/// Reminders authorization for the settings status line.
/// Read-only: requesting authorization stays in the Devices view.
#[tauri::command]
pub fn device_authorization() -> Result<String, String> {
    let client = DeviceClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))?;
    client.authorization().map_err(|e| err_string(&e))
}
