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

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct EmailClient {
    http: HttpClient,
}

impl EmailClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
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
}
