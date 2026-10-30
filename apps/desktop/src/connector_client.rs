//! Mock connector client: Rust shell -> Mock Gateway connectors.
//!
//! Phase 4 connectors are Mock only, and the data is fake:
//! - `mock-notes`: seeded fake notes, plus notes created in
//!   this server run. Never real notes, never real accounts.
//! - `mock-mail`: seeded fake messages, plus messages sent in
//!   this server run. Never real mail, never real delivery.
//!
//! Every step is gated, in order:
//! 1. Egress: the base URL must be the localhost Mock, and a
//!    mail recipient must be inside `mock.local`.
//! 2. Registry: unknown connectors are rejected before any
//!    request (notes.list/notes.create, mail.list/mail.send).
//! 3. Approval (Phase 3 loop, reused): notes.create and
//!    mail.send need a decided approval record; the server
//!    re-checks its own record before executing, so a direct
//!    create/send with no record is refused server-side.
//! 4. Payload: secret markers are refused before sending.

use serde::{Deserialize, Serialize};

use crate::agent_client::{AgentClient, AgentError, ToolDecision};
use crate::egress::EgressPolicy;
use crate::http_client::{gateway_error, transport_error, HttpClient};
use crate::{ErrorCode, Result, UnifiedError, UserAction};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockNote {
    pub id: String,
    pub title: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MockMessage {
    pub id: String,
    pub from: String,
    pub to: String,
    pub subject: String,
    pub body: String,
}

/// One entry of the static connector registry (Task 9): exactly
/// the Mock connectors the gateway implements, with their live
/// connection flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorInfo {
    pub id: String,
    pub name: String,
    pub capabilities: Vec<String>,
    pub connected: bool,
}

/// A local Mock account record. `status` is "linked" or
/// "unlinked"; there is no real account behind it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorAccount {
    pub account_id: String,
    pub connector_id: String,
    pub display_name: String,
    pub status: String,
    pub linked_ms: u64,
}

/// The local Mock connect flow description: no OAuth, no
/// external redirect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorConnectInfo {
    pub connector_id: String,
    pub flow: String,
    pub requires_account: bool,
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorAction {
    NotesList,
    NotesCreate,
    MailList,
    MailSend,
}

impl ConnectorAction {
    pub fn tool_name(&self) -> &'static str {
        match self {
            ConnectorAction::NotesList => "notes.list",
            ConnectorAction::NotesCreate => "notes.create",
            ConnectorAction::MailList => "mail.list",
            ConnectorAction::MailSend => "mail.send",
        }
    }

    pub fn requires_approval(&self) -> bool {
        matches!(self, ConnectorAction::NotesCreate | ConnectorAction::MailSend)
    }
}

fn denied(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::PermissionDenied,
        capability: Some("connectors".to_string()),
        retryable: false,
        user_action: UserAction::RequestApproval,
        redacted_detail: detail.to_string(),
    }
}

#[derive(Debug, Serialize)]
struct NoteCreateDto {
    tool_call_id: Option<String>,
    title: String,
    body: String,
}

#[derive(Debug, Serialize)]
struct MailSendDto {
    tool_call_id: Option<String>,
    to: String,
    subject: String,
    body: String,
}

#[derive(Debug, Serialize)]
struct AccountLinkDto {
    display_name: Option<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct ConnectorClient {
    http: HttpClient,
    agent: AgentClient,
    policy: EgressPolicy,
}

impl ConnectorClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            agent: AgentClient::new(base_url)?,
            policy,
        })
    }

    pub fn notes_list(&self) -> std::result::Result<Vec<MockNote>, AgentError> {
        let notes: Vec<MockNote> = self.http.get_json("/connectors/notes/list")?;
        Ok(notes)
    }

    pub fn mail_list(&self) -> std::result::Result<Vec<MockMessage>, AgentError> {
        let messages: Vec<MockMessage> = self.http.get_json("/connectors/mail/list")?;
        Ok(messages)
    }

    pub fn notes_create(
        &self,
        title: &str,
        body: &str,
        decision: ToolDecision,
    ) -> std::result::Result<MockNote, AgentError> {
        self.policy.check_payload(body)?;
        self.policy.check_payload(title)?;
        let tool_call_id = self.approve(ConnectorAction::NotesCreate, decision)?;
        let request = serde_json::to_string(&NoteCreateDto {
            tool_call_id,
            title: title.to_string(),
            body: body.to_string(),
        })
        .map_err(|_| transport_error("cannot encode note create request"))?;
        let (status, resp_body) = self.http.request("POST", "/connectors/notes/create", Some(&request))?;
        if status == 403 {
            return Err(denied("connector call refused: approval required or rejected").into());
        }
        if !(200..300).contains(&status) {
            return Err(gateway_error(status).into());
        }
        let note: MockNote = serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))?;
        Ok(note)
    }

    pub fn mail_send(
        &self,
        to: &str,
        subject: &str,
        body: &str,
        decision: ToolDecision,
    ) -> std::result::Result<MockMessage, AgentError> {
        self.policy.check_recipient(to)?;
        self.policy.check_payload(body)?;
        self.policy.check_payload(subject)?;
        let tool_call_id = self.approve(ConnectorAction::MailSend, decision)?;
        let request = serde_json::to_string(&MailSendDto {
            tool_call_id,
            to: to.to_string(),
            subject: subject.to_string(),
            body: body.to_string(),
        })
        .map_err(|_| transport_error("cannot encode mail send request"))?;
        let (status, resp_body) = self.http.request("POST", "/connectors/mail/send", Some(&request))?;
        if status == 403 {
            return Err(denied("connector call refused: approval required or rejected").into());
        }
        if !(200..300).contains(&status) {
            return Err(gateway_error(status).into());
        }
        let message: MockMessage = serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))?;
        Ok(message)
    }

    /// Registry + account lifecycle (Task 9). These are local
    /// metadata calls, not connector writes, so they carry no
    /// approval loop: the server changes only Mock state.
    pub fn registry_list(&self) -> std::result::Result<Vec<ConnectorInfo>, AgentError> {
        let infos: Vec<ConnectorInfo> = self.http.get_json("/connectors")?;
        Ok(infos)
    }

    pub fn registry_get(&self, id: &str) -> std::result::Result<ConnectorInfo, AgentError> {
        let info: ConnectorInfo = self.http.get_json(&format!("/connectors/{id}"))?;
        Ok(info)
    }

    pub fn accounts(&self, id: &str) -> std::result::Result<Vec<ConnectorAccount>, AgentError> {
        let accounts: Vec<ConnectorAccount> = self.http.get_json(&format!("/connectors/{id}/accounts"))?;
        Ok(accounts)
    }

    pub fn link_account(
        &self,
        id: &str,
        display_name: Option<&str>,
    ) -> std::result::Result<ConnectorAccount, AgentError> {
        let body = serde_json::to_string(&AccountLinkDto { display_name: display_name.map(|s| s.to_string()) })
            .map_err(|_| transport_error("cannot encode account link request"))?;
        let account: ConnectorAccount = self.http.post_json(&format!("/connectors/{id}/accounts/link"), &body)?;
        Ok(account)
    }

    pub fn unlink_account(
        &self,
        id: &str,
        account_id: &str,
    ) -> std::result::Result<ConnectorAccount, AgentError> {
        let account: ConnectorAccount = self
            .http
            .post_json(&format!("/connectors/{id}/accounts/{account_id}/unlink"), "{}")?;
        Ok(account)
    }

    pub fn connect_info(&self, id: &str) -> std::result::Result<ConnectorConnectInfo, AgentError> {
        let info: ConnectorConnectInfo = self.http.get_json(&format!("/connectors/{id}/connect/info"))?;
        Ok(info)
    }

    pub fn connect(&self, id: &str) -> std::result::Result<ConnectorInfo, AgentError> {
        let info: ConnectorInfo = self.http.post_json(&format!("/connectors/{id}/connect"), "{}")?;
        Ok(info)
    }

    pub fn disconnect(&self, id: &str) -> std::result::Result<ConnectorInfo, AgentError> {
        let info: ConnectorInfo = self.http.post_json(&format!("/connectors/{id}/disconnect"), "{}")?;
        Ok(info)
    }

    /// Run the Phase 3 approval loop for a connector write.
    /// Returns the approved tool_call_id, or ApprovalPending
    /// when the decision is Ask (nothing executed).
    fn approve(
        &self,
        action: ConnectorAction,
        decision: ToolDecision,
    ) -> std::result::Result<Option<String>, AgentError> {
        if !action.requires_approval() {
            return Ok(None);
        }
        let request = self.agent.request_approval(action.tool_name())?;
        let Some(tool_call_id) = request.tool_call_id else {
            return Err(transport_error("approval tool returned no tool_call_id").into());
        };
        match decision {
            ToolDecision::Ask => Err(AgentError::ApprovalPending { tool_call_id }),
            ToolDecision::Approve | ToolDecision::Reject => {
                self.agent.decide_approval(&tool_call_id, decision)?;
                Ok(Some(tool_call_id))
            }
        }
    }
}
