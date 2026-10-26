//! Tauri commands for workbench sessions (Round 3, 3a).
//!
//! Thin forwarding to the desktop SessionClient over the shared
//! gateway endpoint: no state machine lives here, and sending
//! never auto-approves or resumes anything else. Invoke argument
//! keys are snake_case, matching the frontend clients.

use agent_desktop::session_client::{SessionClient, SessionRecord, SessionSummary};
use agent_desktop::UnifiedError;

use crate::gateway_endpoint;

fn err_string(error: &UnifiedError) -> String {
    format!("{:?}: {}", error.code, error.redacted_detail)
}

fn client() -> Result<SessionClient, String> {
    SessionClient::new(&gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn session_create(title: Option<String>) -> Result<SessionRecord, String> {
    client()?.create(title.as_deref()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn session_list() -> Result<Vec<SessionSummary>, String> {
    client()?.list().map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn session_get(session_id: String) -> Result<SessionRecord, String> {
    client()?.get(&session_id).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn session_send(session_id: String, text: String) -> Result<SessionRecord, String> {
    client()?.send(&session_id, &text).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn session_delete(session_id: String) -> Result<(), String> {
    client()?.delete(&session_id).map_err(|e| err_string(&e))
}
