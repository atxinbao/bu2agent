//! Chat stream client: Rust shell -> Mock Gateway chat
//! stream surface (Task 20, stream form subset).
//!
//! Mirrors the chat records client: no approval loop (local
//! record/subscription operations, like the other metadata
//! surfaces), and the base URL is allowlisted to the
//! localhost Mock. Events are a record projection, not a
//! live token stream; see stream_store.rs on the gateway.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSubscription {
    pub subscription_id: String,
    pub thread_id: String,
    pub cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEvent {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub turn_index: Option<usize>,
    pub text: Option<String>,
    pub created_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEvents {
    pub subscription_id: String,
    pub cursor: String,
    pub events: Vec<StreamEvent>,
    pub cancelled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCancelResult {
    pub thread_id: String,
    pub cancelled_subscriptions: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamOpen {
    pub stream_id: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPoke {
    pub thread_id: String,
    pub event_id: String,
    pub created_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SxsSelection {
    pub thread_id: String,
    pub index: usize,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct StreamClient {
    http: HttpClient,
}

impl StreamClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn subscribe(&self, thread_id: &str) -> std::result::Result<StreamSubscription, AgentError> {
        let body = serde_json::json!({ "thread_id": thread_id }).to_string();
        Ok(self.http.post_json("/chat/subscribe", &body)?)
    }

    pub fn stream_events(
        &self,
        subscription_id: &str,
        cursor: Option<&str>,
        wait_ms: Option<u64>,
    ) -> std::result::Result<StreamEvents, AgentError> {
        let body = serde_json::json!({
            "subscription_id": subscription_id,
            "cursor": cursor,
            "wait_ms": wait_ms,
        })
        .to_string();
        Ok(self.http.post_json("/chat/stream-events", &body)?)
    }

    pub fn cancel(&self, thread_id: &str) -> std::result::Result<StreamCancelResult, AgentError> {
        let body = serde_json::json!({ "thread_id": thread_id }).to_string();
        Ok(self.http.post_json("/chat/cancel", &body)?)
    }

    pub fn open_stream(&self, thread_id: &str) -> std::result::Result<StreamOpen, AgentError> {
        let body = serde_json::json!({ "thread_id": thread_id }).to_string();
        Ok(self.http.post_json("/chat/stream", &body)?)
    }

    pub fn poke(&self, thread_id: &str) -> std::result::Result<StreamPoke, AgentError> {
        let body = serde_json::json!({ "thread_id": thread_id }).to_string();
        Ok(self.http.post_json("/chat/poke", &body)?)
    }

    pub fn sxs_select(
        &self,
        thread_id: &str,
        index: usize,
    ) -> std::result::Result<SxsSelection, AgentError> {
        let body = serde_json::json!({ "thread_id": thread_id, "index": index }).to_string();
        Ok(self.http.post_json("/api/chat/sxs/select", &body)?)
    }
}
