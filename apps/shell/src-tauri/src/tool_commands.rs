//! Tauri commands for the file, agent, and connector views.
//!
//! Same pattern as gateway_commands.rs: each command builds its
//! Rust client per call against the localhost Mock Gateway, and
//! errors report only the UnifiedError code plus its redacted
//! detail, never a response body. DTOs are `pub` on purpose:
//! the command macro generates wrappers that reference them from
//! outside this module, so a private DTO fails the build (that
//! exact error was caught by the first Mac build of the chat
//! commands and is not repeated here).
//!
//! Approval errors from the agent loop use one marker string,
//! `approval_pending:<tool_call_id>`, so the TS invoke clients
//! can turn it back into ApprovalPendingError. The views
//! confirm before they call, so they normally pass "approve"
//! and never see this marker; it exists for parity with the
//! HTTP clients, which throw ApprovalPendingError on "ask".

use std::collections::HashMap;

use serde::Serialize;

use agent_desktop::agent_client::{AgentClient, AgentError, ToolDecision, ToolResult};
use agent_desktop::connector_client::{ConnectorClient, MockMessage, MockNote};
use agent_desktop::file_gateway_client::{FileGatewayClient, GatewayFileEntry};
use agent_desktop::{ErrorCode, UnifiedError, UserAction};

const MOCK_BASE_URL: &str = "http://127.0.0.1:3001";

fn err_string(error: &UnifiedError) -> String {
    format!("{:?}: {}", error.code, error.redacted_detail)
}

fn agent_err_string(error: AgentError) -> String {
    match error {
        AgentError::Unified(inner) => err_string(&inner),
        AgentError::ApprovalPending { tool_call_id } => {
            format!("approval_pending:{tool_call_id}")
        }
    }
}

fn parse_decision(decision: &str) -> Result<ToolDecision, String> {
    match decision {
        "ask" => Ok(ToolDecision::Ask),
        "approve" => Ok(ToolDecision::Approve),
        "reject" => Ok(ToolDecision::Reject),
        other => Err(format!("unknown tool decision: {other}")),
    }
}

fn denied(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::PermissionDenied,
        capability: Some("tools".to_string()),
        retryable: false,
        user_action: UserAction::RequestApproval,
        redacted_detail: detail.to_string(),
    }
}

#[derive(Debug, Serialize)]
pub struct ApprovalRequestOut {
    pub tool_call_id: Option<String>,
    pub requires_approval: bool,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct ToolResultOut {
    pub tool: String,
    pub result: serde_json::Value,
}

impl From<ToolResult> for ToolResultOut {
    fn from(value: ToolResult) -> Self {
        ToolResultOut { tool: value.tool, result: value.result }
    }
}

fn file_client() -> Result<FileGatewayClient, String> {
    FileGatewayClient::new(MOCK_BASE_URL).map_err(|e| err_string(&e))
}

fn agent_client() -> Result<AgentClient, String> {
    AgentClient::new(MOCK_BASE_URL).map_err(|e| err_string(&e))
}

fn connector_client() -> Result<ConnectorClient, String> {
    ConnectorClient::new(MOCK_BASE_URL).map_err(|e| err_string(&e))
}

// --- Files (Phase 2 file gateway) ---

#[tauri::command]
pub fn file_list(path: String) -> Result<Vec<GatewayFileEntry>, String> {
    file_client()?.list(&path).map_err(|e| err_string(&e))
}

#[tauri::command]
pub fn file_read_text(
    path: String,
    chunk_len: u64,
    max_total: u64,
) -> Result<String, String> {
    let bytes = file_client()?
        .read_all_chunked(&path, chunk_len, max_total)
        .map_err(|e| err_string(&e))?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[tauri::command]
pub fn file_write(
    path: String,
    text: String,
    approved: bool,
    append: bool,
) -> Result<u64, String> {
    if !approved {
        return Err(err_string(&denied("file write requires approval")));
    }
    file_client()?
        .write(&path, text.as_bytes(), true, append)
        .map_err(|e| err_string(&e))
}

#[tauri::command]
pub fn file_delete(path: String, approved: bool) -> Result<(), String> {
    if !approved {
        return Err(err_string(&denied("file delete requires approval")));
    }
    file_client()?.delete(&path, true).map_err(|e| err_string(&e))
}

// --- Agent tools (Phase 3 approval loop) ---

#[tauri::command]
pub fn agent_request_approval(tool: String) -> Result<ApprovalRequestOut, String> {
    let request = agent_client()?
        .request_approval(&tool)
        .map_err(agent_err_string)?;
    Ok(ApprovalRequestOut {
        tool_call_id: request.tool_call_id,
        requires_approval: request.requires_approval,
        status: request.status,
    })
}

#[tauri::command]
pub fn agent_decide_approval(
    tool_call_id: String,
    decision: String,
) -> Result<(), String> {
    let decision = parse_decision(&decision)?;
    if decision == ToolDecision::Ask {
        return Err("ask is not a decision".to_string());
    }
    agent_client()?
        .decide_approval(&tool_call_id, decision)
        .map_err(agent_err_string)
}

#[tauri::command]
pub fn agent_call_tool(
    tool: String,
    args: HashMap<String, String>,
    tool_call_id: Option<String>,
) -> Result<ToolResultOut, String> {
    let result = agent_client()?
        .call_tool(&tool, &args, tool_call_id.as_deref())
        .map_err(agent_err_string)?;
    Ok(result.into())
}

#[tauri::command]
pub fn agent_run_tool(
    tool: String,
    args: HashMap<String, String>,
    decision: String,
) -> Result<ToolResultOut, String> {
    let decision = parse_decision(&decision)?;
    let result = agent_client()?
        .run_tool(&tool, &args, decision)
        .map_err(agent_err_string)?;
    Ok(result.into())
}

// --- Connectors (Phase 4 Mock connectors) ---

#[tauri::command]
pub fn connector_notes_list() -> Result<Vec<MockNote>, String> {
    connector_client()?.notes_list().map_err(agent_err_string)
}

#[tauri::command]
pub fn connector_mail_list() -> Result<Vec<MockMessage>, String> {
    connector_client()?.mail_list().map_err(agent_err_string)
}

#[tauri::command]
pub fn connector_notes_create(
    title: String,
    body: String,
    decision: String,
) -> Result<MockNote, String> {
    let decision = parse_decision(&decision)?;
    connector_client()?
        .notes_create(&title, &body, decision)
        .map_err(agent_err_string)
}

#[tauri::command]
pub fn connector_mail_send(
    to: String,
    subject: String,
    body: String,
    decision: String,
) -> Result<MockMessage, String> {
    let decision = parse_decision(&decision)?;
    connector_client()?
        .mail_send(&to, &subject, &body, decision)
        .map_err(agent_err_string)
}
