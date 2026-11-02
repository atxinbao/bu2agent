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
use agent_desktop::permission_policy_client::{
    ConnectorPolicyView, EffectivePolicyView, HitlView, NetworkRuleView, PermissionPolicyClient,
    PolicyDefaultsView, PolicyInventoryView, PolicyView, RevokeView, ScopeView, SwitchView,
};
use agent_desktop::model_client::{ModelClient, ModelView};
use agent_desktop::config_client::{ConfigClient, ConfigView};
use agent_desktop::peer_client::{Peer, PeerClient, PeerInboxMessage, PeerInvite, PeerRequest, PeerStatus};
use agent_desktop::space_client::{Space, SpaceClient, SpaceItem};
use agent_desktop::feed_client::{FeedClient, FeedSeenResult, FeedStatus, FeedUnit};
use agent_desktop::chat_records_client::{
    ChatRecordsClient, ChatThemes, ChatTurnView, HistoryWindow, SearchMatch, SeenResult,
    UnreadCount,
};
use agent_desktop::node_client::{Node, NodeClient};
use agent_desktop::stream_client::{
    StreamCancelResult, StreamClient, StreamEvents, StreamOpen, StreamPoke, StreamSubscription,
    SxsSelection,
};
use agent_desktop::voice_client::{
    ActiveVoice, CatalogFilters, CatalogVoice, CustomVoice, OnboardingReset, PipelineProfile,
    PipelineSelection, VoiceClient,
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

// --- Permissions policy (Task 11) ---
//
// Every command carries rename_all = "snake_case": the Task 9
// link/unlink failure was exactly these attributes missing
// while the frontend sent snake_case keys. Policy calls carry
// no approval loop (same decision as the Task 9/10 registry
// and vault calls); the gateway stores policy data only.

fn policy_client() -> Result<PermissionPolicyClient, String> {
    PermissionPolicyClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_get() -> Result<PolicyView, String> {
    policy_client()?.get_policy().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_active_get() -> Result<EffectivePolicyView, String> {
    policy_client()?.get_active().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_active_update(
    hitl_enabled: Option<bool>,
    mitm_enabled: Option<bool>,
    tproxy_enabled: Option<bool>,
    sni_rejection_enabled: Option<bool>,
    hitl_snoozed_until_ms: Option<Option<u64>>,
) -> Result<EffectivePolicyView, String> {
    policy_client()?
        .update_active(
            hitl_enabled,
            mitm_enabled,
            tproxy_enabled,
            sni_rejection_enabled,
            hitl_snoozed_until_ms,
        )
        .map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_connector_update(
    connector: String,
    allowed: bool,
) -> Result<ConnectorPolicyView, String> {
    policy_client()?.update_connector(&connector, allowed).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_connector_reset(connector: String) -> Result<ConnectorPolicyView, String> {
    policy_client()?.reset_connector(&connector).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_hitl_update(
    enabled: Option<bool>,
    snoozed_until_ms: Option<Option<u64>>,
) -> Result<HitlView, String> {
    policy_client()?.update_hitl(enabled, snoozed_until_ms).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_hitl_snooze_delete() -> Result<HitlView, String> {
    policy_client()?.delete_hitl_snooze().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_mitm_update(enabled: bool) -> Result<SwitchView, String> {
    policy_client()?.update_mitm(enabled).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_tproxy_update(enabled: bool) -> Result<SwitchView, String> {
    policy_client()?.update_tproxy(enabled).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_sni_rejection_update(enabled: bool) -> Result<SwitchView, String> {
    policy_client()?.update_sni_rejection(enabled).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_network_rules_update(rules: Vec<NetworkRuleView>) -> Result<Vec<NetworkRuleView>, String> {
    policy_client()?.update_network_rules(rules).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_reset() -> Result<PolicyView, String> {
    policy_client()?.reset_policy().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_revoke(
    scope_type: Option<String>,
    scope_id: Option<String>,
) -> Result<RevokeView, String> {
    policy_client()?.revoke(scope_type.as_deref(), scope_id.as_deref()).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_scope_get(scope_type: String, scope_id: String) -> Result<ScopeView, String> {
    policy_client()?.get_scope(&scope_type, &scope_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_scope_update(
    scope_type: String,
    scope_id: String,
    granted: Option<bool>,
) -> Result<ScopeView, String> {
    policy_client()?.update_scope(&scope_type, &scope_id, granted).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_inventory() -> Result<PolicyInventoryView, String> {
    policy_client()?.get_inventory().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_settings_get() -> Result<PolicyDefaultsView, String> {
    policy_client()?.get_settings().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_settings_update(
    hitl_enabled: Option<bool>,
    mitm_enabled: Option<bool>,
    tproxy_enabled: Option<bool>,
    sni_rejection_enabled: Option<bool>,
) -> Result<PolicyDefaultsView, String> {
    policy_client()?
        .update_settings(hitl_enabled, mitm_enabled, tproxy_enabled, sni_rejection_enabled)
        .map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn policy_connector_permissions(connector: String) -> Result<ConnectorPolicyView, String> {
    policy_client()?.connector_permissions(&connector).map_err(agent_err_string)
}

// --- Model surface (Task 12) ---
//
// Every command carries rename_all = "snake_case" (Task 9
// lesson). Model calls carry no approval loop (same decision
// as the Task 9/10/11 metadata calls). POST stages a
// selection that takes effect only after a gateway restart;
// the running client is never rebuilt here, and no key is
// ever accepted or returned on this surface.

fn model_client() -> Result<ModelClient, String> {
    ModelClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn model_get() -> Result<ModelView, String> {
    model_client()?.get_model().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn model_set(
    model: Option<String>,
    base_url: Option<String>,
) -> Result<ModelView, String> {
    model_client()?.set_model(model.as_deref(), base_url.as_deref()).map_err(agent_err_string)
}

// --- Config surface (Task 14, GET only) ---
//
// No config_set command exists on purpose: the gateway
// has no runtime-mutable self-owned config to write.

fn config_client() -> Result<ConfigClient, String> {
    ConfigClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn config_get() -> Result<ConfigView, String> {
    config_client()?.get_config().map_err(agent_err_string)
}

// --- Peers surface (Task 15, record subset) ---
//
// All commands carry rename_all = "snake_case" (Task 9
// lesson): the invoke client sends snake_case params.

fn peer_client() -> Result<PeerClient, String> {
    PeerClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_list() -> Result<Vec<Peer>, String> {
    peer_client()?.list_peers().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_get(muse_id: String) -> Result<Peer, String> {
    peer_client()?.get_peer(&muse_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_update(
    muse_id: String,
    display_name: Option<String>,
    status: Option<String>,
) -> Result<Peer, String> {
    peer_client()?.update_peer(&muse_id, display_name.as_deref(), status.as_deref()).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_disconnect(muse_id: String) -> Result<Peer, String> {
    peer_client()?.disconnect_peer(&muse_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_invites_list() -> Result<Vec<PeerInvite>, String> {
    peer_client()?.list_invites().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_invite_create(muse_id: String, display_name: String) -> Result<PeerInvite, String> {
    peer_client()?.create_invite(&muse_id, &display_name).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_invite_accept(invite_id: String) -> Result<PeerInvite, String> {
    peer_client()?.accept_invite(&invite_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_invite_open(invite_id: String) -> Result<PeerInvite, String> {
    peer_client()?.open_invite(&invite_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_invite_revoke(invite_id: String) -> Result<PeerInvite, String> {
    peer_client()?.revoke_invite(&invite_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_inbox_list() -> Result<Vec<PeerInboxMessage>, String> {
    peer_client()?.list_inbox().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_inbox_deliver(from_muse_id: String, text: String) -> Result<PeerInboxMessage, String> {
    peer_client()?.deliver_inbox(&from_muse_id, &text).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_inbox_decline(message_id: String) -> Result<PeerInboxMessage, String> {
    peer_client()?.decline_inbox(&message_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_requests_list() -> Result<Vec<PeerRequest>, String> {
    peer_client()?.list_requests().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn peer_status() -> Result<PeerStatus, String> {
    peer_client()?.get_status().map_err(agent_err_string)
}

// --- Spaces surface (Task 16, v2 record subset) ---
//
// All commands carry rename_all = "snake_case" (Task 9
// lesson): the invoke client sends snake_case params.

fn space_client() -> Result<SpaceClient, String> {
    SpaceClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn space_list() -> Result<Vec<Space>, String> {
    space_client()?.list_spaces().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn space_save(
    slug: String,
    title: Option<String>,
    items: Vec<SpaceItem>,
) -> Result<Space, String> {
    space_client()?.save_space(&slug, title.as_deref(), &items).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn space_delete(slug: String) -> Result<Space, String> {
    space_client()?.delete_space(&slug).map_err(agent_err_string)
}

// --- Feed surface (Task 17, unit subset) ---
//
// All commands carry rename_all = "snake_case" (Task 9
// lesson): the invoke client sends snake_case params.

fn feed_client() -> Result<FeedClient, String> {
    FeedClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn feed_list() -> Result<Vec<FeedUnit>, String> {
    feed_client()?.list_units().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn feed_get(unit_id: String) -> Result<FeedUnit, String> {
    feed_client()?.get_unit(&unit_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn feed_delete(unit_id: String) -> Result<FeedUnit, String> {
    feed_client()?.delete_unit(&unit_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn feed_react(unit_id: String, reaction: String) -> Result<FeedUnit, String> {
    feed_client()?.react_unit(&unit_id, &reaction).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn feed_tap(unit_id: String) -> Result<FeedUnit, String> {
    feed_client()?.tap_unit(&unit_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn feed_seen(unit_ids: Vec<String>) -> Result<FeedSeenResult, String> {
    feed_client()?.seen_units(&unit_ids).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn feed_status() -> Result<FeedStatus, String> {
    feed_client()?.get_status().map_err(agent_err_string)
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

// --- Chat records surface (Task 18, record subset) ---
//
// All commands carry rename_all = "snake_case" (Task 9
// lesson): the invoke client sends snake_case params.

fn chat_records_client() -> Result<ChatRecordsClient, String> {
    ChatRecordsClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_themes_get() -> Result<ChatThemes, String> {
    chat_records_client()?.themes().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_themes_set(theme: String) -> Result<ChatThemes, String> {
    chat_records_client()?.set_theme(&theme).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_unread_count() -> Result<UnreadCount, String> {
    chat_records_client()?.unread_count().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_mark_seen(thread_id: String, index: Option<usize>) -> Result<SeenResult, String> {
    chat_records_client()?.mark_seen(&thread_id, index).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_delete_turn(thread_id: String, index: usize) -> Result<ChatTurnView, String> {
    chat_records_client()?.delete_turn(&thread_id, index).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_message_get(thread_id: String, index: usize) -> Result<ChatTurnView, String> {
    chat_records_client()?.message(&thread_id, index).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_history_window(
    thread_id: String,
    offset: usize,
    limit: usize,
) -> Result<HistoryWindow, String> {
    chat_records_client()?.history_window(&thread_id, offset, limit).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_search(query: String) -> Result<Vec<SearchMatch>, String> {
    chat_records_client()?.search(&query).map_err(agent_err_string)
}

// --- Node registry surface (Task 19, record subset) ---
//
// All commands carry rename_all = "snake_case" (Task 9
// lesson): the invoke client sends snake_case params.

fn node_client() -> Result<NodeClient, String> {
    NodeClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn node_register(name: Option<String>) -> Result<Node, String> {
    node_client()?.register_node(name.as_deref()).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn node_list(status: Option<String>) -> Result<Vec<Node>, String> {
    node_client()?.list_nodes(status.as_deref()).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn node_heartbeat(node_id: String) -> Result<Node, String> {
    node_client()?.heartbeat_node(&node_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn node_disconnect(node_id: String) -> Result<Node, String> {
    node_client()?.disconnect_node(&node_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn node_unpair(node_id: String) -> Result<Node, String> {
    node_client()?.unpair_node(&node_id).map_err(agent_err_string)
}

// --- Chat stream surface (Task 20, stream form subset) ---
//
// All commands carry rename_all = "snake_case" (Task 9
// lesson): the invoke client sends snake_case params.

fn stream_client() -> Result<StreamClient, String> {
    StreamClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_subscribe(thread_id: String) -> Result<StreamSubscription, String> {
    stream_client()?.subscribe(&thread_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_stream_events(
    subscription_id: String,
    cursor: Option<String>,
    wait_ms: Option<u64>,
) -> Result<StreamEvents, String> {
    stream_client()?
        .stream_events(&subscription_id, cursor.as_deref(), wait_ms)
        .map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_cancel(thread_id: String) -> Result<StreamCancelResult, String> {
    stream_client()?.cancel(&thread_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_stream_open(thread_id: String) -> Result<StreamOpen, String> {
    stream_client()?.open_stream(&thread_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_poke(thread_id: String) -> Result<StreamPoke, String> {
    stream_client()?.poke(&thread_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn chat_sxs_select(thread_id: String, index: usize) -> Result<SxsSelection, String> {
    stream_client()?.sxs_select(&thread_id, index).map_err(agent_err_string)
}

// --- Voice record surface (Task 21, record subset, D2) ---
//
// All commands carry rename_all = "snake_case" (Task 9
// lesson): the invoke client sends snake_case params.

fn voice_client() -> Result<VoiceClient, String> {
    VoiceClient::new(&crate::gateway_endpoint::base_url()).map_err(|e| err_string(&e))
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_catalog() -> Result<Vec<CatalogVoice>, String> {
    voice_client()?.catalog().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_catalog_filters() -> Result<CatalogFilters, String> {
    voice_client()?.catalog_filters().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_available_voices() -> Result<Vec<CatalogVoice>, String> {
    voice_client()?.available_voices().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_pipeline_profiles() -> Result<Vec<PipelineProfile>, String> {
    voice_client()?.pipeline_profiles().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_pipeline_profile_select(profile_id: String) -> Result<PipelineSelection, String> {
    voice_client()?.select_pipeline_profile(&profile_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_custom_voices_list() -> Result<Vec<CustomVoice>, String> {
    voice_client()?.custom_voices().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_custom_voice_rename(
    saved_voice_id: String,
    name: String,
) -> Result<CustomVoice, String> {
    voice_client()?.rename_custom_voice(&saved_voice_id, &name).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_custom_voice_delete(saved_voice_id: String) -> Result<CustomVoice, String> {
    voice_client()?.delete_custom_voice(&saved_voice_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_active_voice_get() -> Result<ActiveVoice, String> {
    voice_client()?.active_voice().map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_active_voice_save(voice_id: String) -> Result<ActiveVoice, String> {
    voice_client()?.save_active_voice(&voice_id).map_err(agent_err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub fn voice_onboarding_reset() -> Result<OnboardingReset, String> {
    voice_client()?.onboarding_reset().map_err(agent_err_string)
}
