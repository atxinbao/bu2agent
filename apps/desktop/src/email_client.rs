//! Email client: Rust shell -> Mock Gateway email read
//! surface (Task 22, batch 1, option A).
//!
//! Read-only: mailbox, mailbox/setup, threads list/get,
//! message get, attachment download. No approval loop
//! (reads). On non-macOS the gateway answers 501 and the
//! error surfaces here unchanged; the packaged Mac app
//! reads real Mail.app data via AppleScript.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxInfo {
    pub configured: bool,
    pub address: Option<String>,
    pub name: Option<String>,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailboxSetup {
    pub configured: bool,
    pub source: String,
    pub steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadView {
    pub thread_id: String,
    pub subject: String,
    pub message_ids: Vec<String>,
    pub message_count: usize,
    pub snippet: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentMeta {
    pub index: usize,
    pub name: String,
    pub content_type: String,
    pub size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageView {
    pub message_id: String,
    pub from: String,
    pub to: Vec<String>,
    pub date: String,
    pub subject: String,
    pub body_text: String,
    pub attachments: Vec<AttachmentMeta>,
}

/// D1-6a: one mail sync pass (manual trigger only).
/// Counts + the mail-source cursor; the gateway records
/// one row + one `mail.synced` event per new message in
/// the same transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailSyncResult {
    pub synced_count: usize,
    pub skipped_count: usize,
    pub total_count: usize,
    pub cursor: String,
}

/// D1-6b-1: one accepted send (the `sent_messages` fact
/// row). The body is never in this struct — only its
/// hash. "Sent" means Mail.app accepted the message,
/// never that it was delivered (Q-D1-6b-06).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentMessageRow {
    pub sent_id: String,
    pub source_id: String,
    pub native_id: Option<String>,
    pub idempotency_key: String,
    pub to_json: String,
    pub subject: String,
    pub body_hash: String,
    pub sent_ms: i64,
    pub effect_id: String,
    pub policy_version: String,
    pub created_ms: i64,
    pub origin_device_id: String,
}

/// D1-6b-3: one synced mail row (the gateway
/// `MailMessageRow` shape). The list includes
/// tombstoned rows with `deleted: true` (approved plan
/// §3 / Q-D1-6b-14); `synced_get` likewise returns a
/// tombstoned row with `deleted: true`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailMessageRow {
    pub message_id: String,
    pub source_id: String,
    pub native_id: String,
    pub thread_id: String,
    pub subject: String,
    pub sender: String,
    pub date_ms: i64,
    pub body_hash: String,
    pub created_ms: i64,
    pub origin_device_id: String,
    pub deleted: bool,
    pub updated_ms: i64,
}

/// D1-6b-3: one delete outcome (the gateway
/// `MailDeleteResponse` shape). `message_id` is `None`
/// when the deleted mail was never synced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailDeleteResponse {
    pub message_id: Option<String>,
    pub source_id: String,
    pub native_id: String,
    pub deleted: bool,
    pub effect_id: Option<String>,
    pub replayed: bool,
}

/// D1-6b-1: a send request. The idempotency key is
/// client-supplied and required (Q-D1-6b-05); the
/// approval id is the decided digest-bound approval for
/// this exact message (Q-D1-6b-22).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendMailInput {
    pub to: Vec<String>,
    pub subject: String,
    pub body: String,
    pub idempotency_key: String,
    pub tool_call_id: Option<String>,
}

/// D1-6a fix F-1: mail sync runs AppleScript against a real
/// mailbox and takes ~50 s for a 50-message window, far past
/// the shared 10 s read timeout. Sync alone gets a 120 s
/// bound (2x+ headroom over the observed ~50 s); every other
/// email call keeps the 10 s default so a hung gateway still
/// fails fast instead of hanging the CLI.
///
/// D1-6b-1: send shares the long-timeout client — a send
/// is one `osascript` call capped at 30 s gateway-side,
/// and the 10 s default would time out before the
/// gateway finishes (the same F-1 failure shape).
const MAIL_SYNC_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct EmailClient {
    http: HttpClient,
    /// Long-timeout client (F-1): sync and send share
    /// it — both run AppleScript gateway-side and can
    /// take up to ~50 s / 30 s. All other calls share
    /// `http` and its 10 s default.
    sync_http: HttpClient,
}

impl EmailClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            sync_http: HttpClient::with_timeout(base_url, MAIL_SYNC_TIMEOUT_MS)?,
        })
    }

    pub fn mailbox(&self) -> std::result::Result<MailboxInfo, AgentError> {
        Ok(self.http.get_json("/api/email/mailbox")?)
    }

    pub fn mailbox_setup(&self) -> std::result::Result<MailboxSetup, AgentError> {
        Ok(self.http.get_json("/api/email/mailbox/setup")?)
    }

    pub fn threads_list(
        &self,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> std::result::Result<Vec<ThreadView>, AgentError> {
        let mut path = "/api/email/threads".to_string();
        let mut query = Vec::new();
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if let Some(offset) = offset {
            query.push(format!("offset={offset}"));
        }
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query.join("&"));
        }
        Ok(self.http.get_json(&path)?)
    }

    pub fn thread_get(&self, thread_id: &str) -> std::result::Result<ThreadView, AgentError> {
        Ok(self.http.get_json(&format!("/api/email/threads/{thread_id}"))?)
    }

    pub fn message_get(&self, message_id: &str) -> std::result::Result<MessageView, AgentError> {
        Ok(self.http.get_json(&format!("/api/email/messages/{message_id}"))?)
    }

    pub fn attachment_download(
        &self,
        message_id: &str,
        index: usize,
    ) -> std::result::Result<Vec<u8>, AgentError> {
        Ok(self.http.get_bytes(&format!("/api/email/messages/{message_id}/attachments/{index}"))?)
    }

    pub fn sync_mail(&self) -> std::result::Result<MailSyncResult, AgentError> {
        // F-1: sync alone uses the long-timeout client; a
        // real mailbox takes ~50 s and the shared 10 s
        // client times out first with "cannot read mock
        // gateway response" while the gateway still finishes.
        Ok(self.sync_http.post_json("/api/email/sync", "{}")?)
    }

    /// D1-6b-1: send one plain-text message through the
    /// digest-bound approval flow (approval_request →
    /// decide → send). Uses the long-timeout client
    /// (same F-1 reason as sync: the gateway-side send
    /// can take up to 30 s). The gateway's extra
    /// `replayed` flag is ignored on decode; the row is
    /// the fact record.
    pub fn send_mail(
        &self,
        input: &SendMailInput,
    ) -> std::result::Result<SentMessageRow, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode mail send"))?;
        Ok(self.sync_http.post_json("/api/email/send", &body)?)
    }

    pub fn list_sent(&self) -> std::result::Result<Vec<SentMessageRow>, AgentError> {
        Ok(self.http.get_json("/api/email/sent")?)
    }

    pub fn sent_get(&self, sent_id: &str) -> std::result::Result<SentMessageRow, AgentError> {
        Ok(self.http.get_json(&format!("/api/email/sent/{sent_id}"))?)
    }

    /// D1-6b-3: delete one message — Trash only, never
    /// purge (Q-D1-6b-12). Uses the long-timeout client
    /// (same F-1 reason as sync/send: the gateway-side
    /// delete can take up to 30 s). The `tool_call_id`
    /// travels as a query parameter: `delete_json` sends
    /// no body (the Task 11 DELETE precedent).
    pub fn delete_message(
        &self,
        message_id: &str,
        tool_call_id: &str,
    ) -> std::result::Result<MailDeleteResponse, AgentError> {
        Ok(self.sync_http.delete_json(&format!(
            "/api/email/messages/{message_id}?tool_call_id={tool_call_id}"
        ))?)
    }

    /// D1-6b-3: list synced mail. The list includes
    /// tombstoned rows with `deleted: true` (approved
    /// plan §3 / Q-D1-6b-14).
    pub fn list_synced(&self) -> std::result::Result<Vec<MailMessageRow>, AgentError> {
        Ok(self.http.get_json("/api/email/synced")?)
    }

    /// D1-6b-3: one synced row by local id; a tombstoned
    /// row is still returned, with `deleted: true` (the
    /// goals-detail precedent).
    pub fn synced_get(&self, message_id: &str) -> std::result::Result<MailMessageRow, AgentError> {
        Ok(self.http.get_json(&format!("/api/email/synced/{message_id}"))?)
    }
}
