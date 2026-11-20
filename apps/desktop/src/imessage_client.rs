//! iMessage client: Rust shell -> Mock Gateway iMessage
//! threads + send surface (MCL-1 threads-only read,
//! MCL-4 send).
//!
//! Reads are sync, threads list/get (no approval loop).
//! The send goes through the digest-bound approval flow
//! (approval_request -> decide -> send, the 6b-1 mail
//! shape). On non-macOS the gateway answers 501 and the
//! error surfaces here unchanged; the packaged Mac app
//! sends real Messages.app messages via AppleScript.
//!
//! No message content anywhere in this file: MCL-1 is
//! threads-only (message history is not readable — the
//! MCL-0 probe proved the AppleScript dictionary has no
//! message element). `last_message_ms` / `unread_count`
//! are always `None` under AppleScript sync (never
//! fabricated). The send input carries the body to the
//! gateway only; the stored fact row carries its hash.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// One `imessage_threads` row (the gateway
/// `IMessageThreadRow` shape). The list includes
/// tombstoned rows with `deleted: true` (6b-3 Q-14);
/// `thread_get` likewise returns a tombstoned row with
/// `deleted: true`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IMessageThreadRow {
    pub thread_id: String,
    pub source_id: String,
    pub native_id: String,
    pub service: String,
    pub is_group: bool,
    pub participant_count: i64,
    pub participant_hash: String,
    /// No message data source under AppleScript sync:
    /// always `None` (never fabricated).
    pub last_message_ms: Option<i64>,
    /// No message data source under AppleScript sync:
    /// always `None` (never fabricated).
    pub unread_count: Option<i64>,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub deleted: bool,
    pub origin_device_id: String,
}

/// MCL-4: one accepted send (the gateway
/// `SentIMessageRow` shape). The body is never in this
/// struct — only its hash. "Sent" means Messages.app
/// accepted the message, never that the other side
/// received it (Q-MCL-21).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentIMessageRow {
    pub sent_id: String,
    pub source_id: String,
    pub thread_id: String,
    pub native_id: Option<String>,
    pub idempotency_key: String,
    pub body_hash: String,
    pub sent_ms: i64,
    pub effect_id: String,
    pub policy_version: String,
    pub created_ms: i64,
    pub origin_device_id: String,
}

/// MCL-4: a send request. The idempotency key is
/// client-supplied and required (Q-MCL-23, the 6b-1
/// shape); the approval id is the decided digest-bound
/// approval for this exact message in this exact thread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendIMessageInput {
    pub thread_id: String,
    pub body: String,
    pub idempotency_key: String,
    pub tool_call_id: Option<String>,
}

/// MCL-1: one iMessage sync pass (manual trigger only).
/// Counts + the imessage-source cursor; the gateway
/// records one row + one `imessage.synced` event per new
/// thread in the same transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IMessageSyncResult {
    pub synced_count: usize,
    pub skipped_count: usize,
    pub total_count: usize,
    pub cursor: String,
}

/// MCL-1: sync runs AppleScript against Messages.app
/// gateway-side and can take up to the 30 s osascript
/// cap, past the shared 10 s read timeout — the same
/// F-1 failure shape as mail sync (D1-6a). Sync alone
/// gets a 120 s bound; every other iMessage call keeps
/// the 10 s default so a hung gateway still fails fast
/// instead of hanging the CLI.
const IMESSAGE_SYNC_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct IMessageClient {
    http: HttpClient,
    /// Long-timeout client (F-1): sync and send share
    /// it — the gateway-side AppleScript enumeration can
    /// take up to 30 s, and the send call the same. All
    /// other calls share `http` and its 10 s default so a
    /// hung gateway still fails fast instead of hanging
    /// the CLI.
    sync_http: HttpClient,
}

impl IMessageClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            sync_http: HttpClient::with_timeout(base_url, IMESSAGE_SYNC_TIMEOUT_MS)?,
        })
    }

    pub fn sync_imessage(&self) -> std::result::Result<IMessageSyncResult, AgentError> {
        // F-1: sync alone uses the long-timeout client;
        // the gateway-side AppleScript enumeration can
        // take up to 30 s and the shared 10 s client
        // times out first while the gateway still finishes.
        Ok(self.sync_http.post_json("/api/imessage/sync", "{}")?)
    }

    /// List synced threads (default 50, max 200
    /// gateway-side). Tombstones included, with
    /// `deleted: true`.
    pub fn threads_list(
        &self,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> std::result::Result<Vec<IMessageThreadRow>, AgentError> {
        let mut path = "/api/imessage/threads".to_string();
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

    /// MCL-4: send one plain-text message to one
    /// already-synced thread, through the digest-bound
    /// approval flow (approval_request → decide → send).
    /// Uses the long-timeout client (the mail `send_mail`
    /// precedent: the gateway-side send can take up to
    /// 30 s, and the 10 s default would time out before
    /// the gateway finishes). The gateway's extra
    /// `replayed` flag is ignored on decode; the row is
    /// the fact record.
    pub fn send_imessage(
        &self,
        input: &SendIMessageInput,
    ) -> std::result::Result<SentIMessageRow, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode imessage send"))?;
        Ok(self.sync_http.post_json("/api/imessage/send", &body)?)
    }

    pub fn list_sent(&self) -> std::result::Result<Vec<SentIMessageRow>, AgentError> {
        Ok(self.http.get_json("/api/imessage/sent")?)
    }

    pub fn sent_get(&self, sent_id: &str) -> std::result::Result<SentIMessageRow, AgentError> {
        Ok(self.http.get_json(&format!("/api/imessage/sent/{sent_id}"))?)
    }

    /// One synced thread by local `thread-N` id; a
    /// tombstoned row is still returned, with
    /// `deleted: true`.
    pub fn thread_get(&self, thread_id: &str) -> std::result::Result<IMessageThreadRow, AgentError> {
        Ok(self.http.get_json(&format!("/api/imessage/threads/{thread_id}"))?)
    }
}
