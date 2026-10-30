//! Tauri commands for the file, agent, connector, task, and device views.
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
use agent_desktop::connector_client::{
    ConnectorAccount, ConnectorClient, ConnectorConnectInfo, ConnectorInfo, MockMessage, MockNote,
};
use agent_desktop::credential_vault_client::{
    CaptureTicketView, CatalogEntryView, CredentialSlotView, CredentialVaultClient, CustomConnectorView,
};
use agent_desktop::device_client::{DeviceActionRecord, DeviceClient, DeviceReminder};
use agent_desktop::task_client::{TaskClient, TaskRecord};
use agent_desktop::file_gateway_client::{FileGatewayClient, GatewayFileEntry, GatewayFileStat, GatewayBatchReadItem, GatewayBatchReadResult, GatewayBatchStatResult, GatewaySearchResult, GatewayThumbnail, GatewayLibraryResult};
use agent_desktop::{ErrorCode, UnifiedError, UserAction};

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
    FileGatewayClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

fn agent_client() -> Result<AgentClient, String> {
    AgentClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

fn connector_client() -> Result<ConnectorClient, String> {
    ConnectorClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

fn vault_client() -> Result<CredentialVaultClient, String> {
    CredentialVaultClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

// --- Files (Phase 2 file gateway) ---

#[tauri::command]
pub fn file_list(path: String) -> Result<Vec<GatewayFileEntry>, String> {
    file_client()?.list(&path).map_err(|e| err_string(&e))
}

#[tauri::command]
pub fn file_stat(path: String) -> Result<GatewayFileStat, String> {
    file_client()?.stat(&path).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
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

#[tauri::command(rename_all = "snake_case")]
pub fn file_rename(from_path: String, to_path: String, approved: bool) -> Result<(), String> {
    if !approved {
        return Err(err_string(&denied("file rename requires approval")));
    }
    file_client()?.rename(&from_path, &to_path, true).map_err(|e| err_string(&e))
}

#[tauri::command]
pub fn file_mkdir(path: String, approved: bool) -> Result<(), String> {
    if !approved {
        return Err(err_string(&denied("file mkdir requires approval")));
    }
    file_client()?.mkdir(&path, true).map_err(|e| err_string(&e))
}

#[tauri::command]
pub fn file_reads(items: Vec<GatewayBatchReadItem>) -> Result<Vec<GatewayBatchReadResult>, String> { file_client()?.reads(&items).map_err(|e| err_string(&e)) }

#[tauri::command]
pub fn file_stats(paths: Vec<String>) -> Result<Vec<GatewayBatchStatResult>, String> { file_client()?.stats(&paths).map_err(|e| err_string(&e)) }

#[tauri::command(rename_all = "snake_case")]
pub fn file_search(query: String, start_path: String) -> Result<GatewaySearchResult, String> { file_client()?.search(&query, &start_path).map_err(|e| err_string(&e)) }

#[tauri::command(rename_all = "snake_case")]
pub async fn file_subscribe(path: String, cursor: Option<String>, wait_ms: u64) -> Result<serde_json::Value, String> {
    // 长轮询必须放到阻塞线程，不能占住 Tauri 异步运行时；否则切换 Files/工作台时其它 invoke 会排队。
    tauri::async_runtime::spawn_blocking(move || {
        file_client()?.subscribe(&path, cursor.as_deref(), wait_ms).map_err(|e| err_string(&e))
    }).await.map_err(|e| format!("file subscribe task failed: {e}"))?
}

#[tauri::command(rename_all = "snake_case")]
pub fn file_thumbnail(path: String, width: u32) -> Result<GatewayThumbnail, String> { file_client()?.thumbnail(&path, width).map_err(|e| err_string(&e)) }

#[tauri::command(rename_all = "snake_case")]
pub fn file_export(from_path: String, to_path: String, approved: bool) -> Result<u64, String> { file_client()?.export(&from_path, &to_path, approved).map_err(|e| err_string(&e)) }

#[tauri::command]
pub fn file_library(kinds: Option<Vec<String>>, limit: Option<usize>) -> Result<GatewayLibraryResult, String> { file_client()?.library(&kinds.unwrap_or_default(), limit.unwrap_or(200)).map_err(|e| err_string(&e)) }

// --- Agent tools (Phase 3 approval loop) ---

/// `args` carries the exact operation for file tools (fs.write:
/// path + text, fs.delete: path) so the Rust client can request
/// a bound approval; other tools ignore it. Absent args for a
/// file tool is a client-side error, not a tool-only request.
#[tauri::command(rename_all = "snake_case")]
pub fn agent_request_approval(
    tool: String,
    args: Option<HashMap<String, String>>,
) -> Result<ApprovalRequestOut, String> {
    let request = agent_client()?
        .request_approval_for(&tool, args.as_ref())
        .map_err(agent_err_string)?;
    Ok(ApprovalRequestOut {
        tool_call_id: request.tool_call_id,
        requires_approval: request.requires_approval,
        status: request.status,
    })
}

#[tauri::command(rename_all = "snake_case")]
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

#[tauri::command(rename_all = "snake_case")]
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

// --- Connector registry (Task 9) ---

#[tauri::command]
pub fn connector_list() -> Result<Vec<ConnectorInfo>, String> {
    connector_client()?.registry_list().map_err(agent_err_string)
}

#[tauri::command]
pub fn connector_get(id: String) -> Result<ConnectorInfo, String> {
    connector_client()?.registry_get(&id).map_err(agent_err_string)
}

#[tauri::command]
pub fn connector_accounts(id: String) -> Result<Vec<ConnectorAccount>, String> {
    connector_client()?.accounts(&id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn connector_account_link(id: String, display_name: Option<String>) -> Result<ConnectorAccount, String> {
    connector_client()?.link_account(&id, display_name.as_deref()).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn connector_account_unlink(id: String, account_id: String) -> Result<ConnectorAccount, String> {
    connector_client()?.unlink_account(&id, &account_id).map_err(agent_err_string)
}

#[tauri::command]
pub fn connector_connect_info(id: String) -> Result<ConnectorConnectInfo, String> {
    connector_client()?.connect_info(&id).map_err(agent_err_string)
}

#[tauri::command]
pub fn connector_connect(id: String) -> Result<ConnectorInfo, String> {
    connector_client()?.connect(&id).map_err(agent_err_string)
}

#[tauri::command]
pub fn connector_disconnect(id: String) -> Result<ConnectorInfo, String> {
    connector_client()?.disconnect(&id).map_err(agent_err_string)
}

// --- Vault: credentials, catalog, custom connectors (Task 10) ---
//
// Every command carries rename_all = "snake_case": the Task 9
// link/unlink failure was exactly these two attributes missing
// while the frontend sent snake_case keys.

#[tauri::command(rename_all = "snake_case")]
pub fn vault_credentials_list() -> Result<Vec<CredentialSlotView>, String> {
    vault_client()?.list_credentials().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_credential_register(
    handle: String,
    secret: String,
    provider: Option<String>,
) -> Result<CredentialSlotView, String> {
    vault_client()?.register_credential(&handle, &secret, provider.as_deref()).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_credential_delete(handle: String) -> Result<CredentialSlotView, String> {
    vault_client()?.delete_credential(&handle).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_credential_capture(handle: String, secret: String) -> Result<CredentialSlotView, String> {
    vault_client()?.capture(&handle, &secret).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_capture_request(provider: String, handle: String) -> Result<CaptureTicketView, String> {
    vault_client()?.capture_request(&provider, &handle).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_capture_fulfill(ticket_id: String, secret: String) -> Result<CredentialSlotView, String> {
    vault_client()?.capture_fulfill(&ticket_id, &secret).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_catalog_list() -> Result<Vec<CatalogEntryView>, String> {
    vault_client()?.catalog_list().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_catalog_details(id: String) -> Result<CatalogEntryView, String> {
    vault_client()?.catalog_details(&id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_catalog_update(
    id: String,
    display_name: Option<String>,
    description: Option<String>,
) -> Result<CatalogEntryView, String> {
    vault_client()?.catalog_update(&id, display_name.as_deref(), description.as_deref()).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_catalog_delete(id: String) -> Result<CatalogEntryView, String> {
    vault_client()?.catalog_delete(&id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_custom_register(
    id: String,
    display_name: String,
    description: Option<String>,
) -> Result<CustomConnectorView, String> {
    vault_client()?.custom_register(&id, &display_name, description.as_deref()).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_custom_list() -> Result<Vec<CustomConnectorView>, String> {
    vault_client()?.custom_list().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn vault_custom_delete(id: String) -> Result<CustomConnectorView, String> {
    vault_client()?.custom_delete(&id).map_err(agent_err_string)
}

// --- Tasks (deterministic task loop) ---

#[derive(Debug, Serialize)]
pub struct TaskOut {
    pub task_id: String,
    pub directory: String,
    pub artifact_path: Option<String>,
    pub status: String,
    pub error: Option<String>,
    pub approval_id: Option<String>,
    pub draft_sha256: Option<String>,
    pub draft_bytes: Option<u64>,
}

impl From<TaskRecord> for TaskOut {
    fn from(value: TaskRecord) -> Self {
        TaskOut {
            task_id: value.task_id,
            directory: value.directory,
            artifact_path: value.artifact_path,
            status: value.status,
            error: value.error,
            approval_id: value.approval_id,
            draft_sha256: value.draft_sha256,
            draft_bytes: value.draft_bytes,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct TaskDraftOut {
    pub task_id: String,
    pub draft: String,
    pub draft_sha256: Option<String>,
    pub draft_bytes: Option<u64>,
}

fn task_client() -> Result<TaskClient, String> {
    TaskClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command]
pub fn task_create(directory: String) -> Result<TaskOut, String> {
    task_client()?.create(&directory).map_err(|e| err_string(&e)).map(TaskOut::from)
}

#[tauri::command]
pub fn task_list() -> Result<Vec<TaskOut>, String> {
    let records = task_client()?.list().map_err(|e| err_string(&e))?;
    Ok(records.into_iter().map(TaskOut::from).collect())
}

#[tauri::command(rename_all = "snake_case")]
pub fn task_get(task_id: String) -> Result<TaskOut, String> {
    task_client()?.get(&task_id).map_err(|e| err_string(&e)).map(TaskOut::from)
}

#[tauri::command(rename_all = "snake_case")]
pub fn task_update(
    task_id: String,
    status: String,
    artifact_path: Option<String>,
) -> Result<TaskOut, String> {
    task_client()?
        .update(&task_id, &status, artifact_path.as_deref())
        .map_err(|e| err_string(&e))
        .map(TaskOut::from)
}

// --- Task runner (Round 3b): one command per gateway endpoint.
// All take only the task id; execute never self-approves, the
// window decides approved first, then calls task_execute.

#[tauri::command(rename_all = "snake_case")]
pub fn task_run(task_id: String) -> Result<TaskOut, String> {
    task_client()?.run(&task_id).map_err(|e| err_string(&e)).map(TaskOut::from)
}

#[tauri::command(rename_all = "snake_case")]
pub fn task_draft(task_id: String) -> Result<TaskDraftOut, String> {
    let draft = task_client()?.draft(&task_id).map_err(|e| err_string(&e))?;
    Ok(TaskDraftOut {
        task_id: draft.task_id,
        draft: draft.draft,
        draft_sha256: draft.draft_sha256,
        draft_bytes: draft.draft_bytes,
    })
}

#[tauri::command(rename_all = "snake_case")]
pub fn task_execute(task_id: String) -> Result<TaskOut, String> {
    task_client()?.execute(&task_id).map_err(|e| err_string(&e)).map(TaskOut::from)
}

#[tauri::command(rename_all = "snake_case")]
pub fn task_reject(task_id: String) -> Result<TaskOut, String> {
    task_client()?.reject(&task_id).map_err(|e| err_string(&e)).map(TaskOut::from)
}

#[tauri::command(rename_all = "snake_case")]
pub fn task_resume(task_id: String) -> Result<TaskOut, String> {
    task_client()?.resume(&task_id).map_err(|e| err_string(&e)).map(TaskOut::from)
}

#[tauri::command(rename_all = "snake_case")]
pub fn task_cancel(task_id: String) -> Result<TaskOut, String> {
    task_client()?.cancel(&task_id).map_err(|e| err_string(&e)).map(TaskOut::from)
}

// --- Devices (B-layer single-device validation) ---

#[derive(Debug, Serialize)]
pub struct DeviceReminderOut {
    pub id: String,
    pub title: String,
    pub completed: bool,
}

#[derive(Debug, Serialize)]
pub struct DeviceActionOut {
    pub action_id: String,
    pub device: String,
    pub action: String,
    pub reminder_id: String,
    pub target_completed: bool,
    pub status: String,
    pub detail: Option<String>,
}

impl From<DeviceReminder> for DeviceReminderOut {
    fn from(value: DeviceReminder) -> Self {
        DeviceReminderOut { id: value.id, title: value.title, completed: value.completed }
    }
}

impl From<DeviceActionRecord> for DeviceActionOut {
    fn from(value: DeviceActionRecord) -> Self {
        DeviceActionOut {
            action_id: value.action_id,
            device: value.device,
            action: value.action,
            reminder_id: value.reminder_id,
            target_completed: value.target_completed,
            status: value.status,
            detail: value.detail,
        }
    }
}

fn device_client() -> Result<DeviceClient, String> {
    DeviceClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command]
pub fn device_reminders_list() -> Result<Vec<DeviceReminderOut>, String> {
    let reminders = device_client()?.list_reminders().map_err(|e| err_string(&e))?;
    Ok(reminders.into_iter().map(DeviceReminderOut::from).collect())
}

#[tauri::command(rename_all = "snake_case")]
pub fn device_actions_create(
    reminder_id: String,
    target_completed: bool,
) -> Result<DeviceActionOut, String> {
    device_client()?
        .create_action(&reminder_id, target_completed)
        .map_err(|e| err_string(&e))
        .map(DeviceActionOut::from)
}

#[tauri::command(rename_all = "snake_case")]
pub fn device_reminder_set(
    action_id: String,
    tool_call_id: String,
) -> Result<DeviceActionOut, String> {
    device_client()?
        .set_reminder(&action_id, &tool_call_id)
        .map_err(|e| err_string(&e))
        .map(DeviceActionOut::from)
}

#[tauri::command(rename_all = "snake_case")]
pub fn device_action_mark_rejected(
    action_id: String,
    tool_call_id: String,
) -> Result<DeviceActionOut, String> {
    device_client()?
        .mark_rejected(&action_id, &tool_call_id)
        .map_err(|e| err_string(&e))
        .map(DeviceActionOut::from)
}

#[tauri::command(rename_all = "snake_case")]
pub fn device_action_refresh(action_id: String) -> Result<DeviceActionOut, String> {
    device_client()?
        .refresh_action(&action_id)
        .map_err(|e| err_string(&e))
        .map(DeviceActionOut::from)
}

#[tauri::command]
pub fn device_actions_list() -> Result<Vec<DeviceActionOut>, String> {
    let records = device_client()?.list_actions().map_err(|e| err_string(&e))?;
    Ok(records.into_iter().map(DeviceActionOut::from).collect())
}

#[tauri::command(rename_all = "snake_case")]
pub fn device_action_get(action_id: String) -> Result<DeviceActionOut, String> {
    device_client()?
        .get_action(&action_id)
        .map_err(|e| err_string(&e))
        .map(DeviceActionOut::from)
}
