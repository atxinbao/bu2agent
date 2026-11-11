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

/// D1-6a fix F-1: mail sync runs AppleScript against a real
/// mailbox and takes ~50 s for a 50-message window, far past
/// the shared 10 s read timeout. Sync alone gets a 120 s
/// bound (2x+ headroom over the observed ~50 s); every other
/// email call keeps the 10 s default so a hung gateway still
/// fails fast instead of hanging the CLI.
const MAIL_SYNC_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct EmailClient {
    http: HttpClient,
    /// Sync-only client with the long timeout (F-1). All
    /// other calls share `http` and its 10 s default.
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
}
