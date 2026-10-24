//! Chat mainline client: Rust shell -> Mock Gateway over HTTP.
//!
//! This is the Phase 1 chat mainline, not a UI: history, stream
//! start, stream apply (with the sequence dedup rule) and cancel,
//! all against the Axum transport in `services/mock-gateway`.
//!
//! Transport notes, stated plainly:
//! - HTTP only, localhost only (the Mock binds 127.0.0.1:3001).
//!   No TLS here; the real gateway adapter must add TLS before
//!   any non-Mock target is used.
//! - HTTP plumbing lives in `http_client.rs`, shared with the
//!   file gateway client. This module only owns chat semantics.
//! - The server speaks snake_case JSON; this client maps it to
//!   the Rust `SubscriptionRecord` shape. The TS client does the
//!   same mapping to camelCase (see `apps/web/src/domain/`).
//! - Error bodies are never copied into `UnifiedError`: they may
//!   contain user text. Only the HTTP status is reported.

use serde::{Deserialize, Serialize};

use crate::gateway::SubscriptionRecord;
use crate::http_client::{gateway_error, transport_error, HttpClient};
use crate::node::events;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryMessage {
    pub turn_id: String,
    pub role: String,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StreamRecordDto {
    stream_id: String,
    sequence: u64,
    event_type: String,
    payload: String,
    resume_cursor: Option<String>,
}

impl From<SubscriptionRecord> for StreamRecordDto {
    fn from(r: SubscriptionRecord) -> Self {
        Self {
            stream_id: r.stream_id,
            sequence: r.sequence,
            event_type: r.event_type,
            payload: r.payload,
            resume_cursor: r.resume_cursor,
        }
    }
}

impl From<StreamRecordDto> for SubscriptionRecord {
    fn from(d: StreamRecordDto) -> Self {
        Self {
            stream_id: d.stream_id,
            sequence: d.sequence,
            event_type: d.event_type,
            payload: d.payload,
            resume_cursor: d.resume_cursor,
        }
    }
}

#[derive(Debug, Serialize)]
struct StartRequest {
    stream_id: String,
    turn_id: String,
    text: String,
}

#[derive(Debug, Serialize)]
struct ApplyRequest {
    stream_id: String,
    records: Vec<StreamRecordDto>,
}

#[derive(Debug)]
pub struct ApplyResponse {
    pub kept: Vec<SubscriptionRecord>,
    pub last_applied_sequence: u64,
}

#[derive(Debug, Deserialize)]
struct ApplyResponseDto {
    kept: Vec<StreamRecordDto>,
    last_applied_sequence: u64,
}

#[derive(Debug, Serialize)]
struct CancelRequest {
    stream_id: String,
}

/// Minimal chat client over the shared Mock HTTP transport.
/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct ChatClient {
    http: HttpClient,
}

impl ChatClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn health(&self) -> Result<bool> {
        let (status, _) = self.http.request("GET", "/health", None)?;
        Ok((200..300).contains(&status))
    }

    pub fn history(&self) -> Result<Vec<HistoryMessage>> {
        self.http.get_json("/chat/history")
    }

    pub fn start_stream(
        &self,
        stream_id: &str,
        turn_id: &str,
        text: &str,
    ) -> Result<Vec<SubscriptionRecord>> {
        let body = serde_json::to_string(&StartRequest {
            stream_id: stream_id.to_string(),
            turn_id: turn_id.to_string(),
            text: text.to_string(),
        })
        .map_err(|_| transport_error("cannot encode stream start request"))?;
        let dtos: Vec<StreamRecordDto> = self.http.post_json("/chat/stream/start", &body)?;
        Ok(dtos.into_iter().map(SubscriptionRecord::from).collect())
    }

    pub fn apply_stream(
        &self,
        stream_id: &str,
        records: &[SubscriptionRecord],
    ) -> Result<ApplyResponse> {
        let body = serde_json::to_string(&ApplyRequest {
            stream_id: stream_id.to_string(),
            records: records.iter().cloned().map(StreamRecordDto::from).collect(),
        })
        .map_err(|_| transport_error("cannot encode stream apply request"))?;
        let dto: ApplyResponseDto = self.http.post_json("/chat/stream/apply", &body)?;
        Ok(ApplyResponse {
            kept: dto.kept.into_iter().map(SubscriptionRecord::from).collect(),
            last_applied_sequence: dto.last_applied_sequence,
        })
    }

    pub fn cancel_stream(&self, stream_id: &str) -> Result<bool> {
        let body = serde_json::to_string(&CancelRequest { stream_id: stream_id.to_string() })
            .map_err(|_| transport_error("cannot encode stream cancel request"))?;
        let (status, _) = self.http.request("POST", "/chat/stream/cancel", Some(&body))?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        Ok(true)
    }
}

/// Assemble the assistant text from applied records: only
/// `delta.text_append` records contribute text, and each payload
/// is parsed as JSON with a `text` field. Other events advance
/// the stream but add no text.
pub fn assemble_text(records: &[SubscriptionRecord]) -> String {
    let mut out = String::new();
    for record in records {
        if record.event_type != events::TEXT_APPEND {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&record.payload) {
            if let Some(text) = value.get("text").and_then(|t| t.as_str()) {
                out.push_str(text);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::SubscriptionRecord;

    fn record(sequence: u64, event_type: &str, payload: &str) -> SubscriptionRecord {
        SubscriptionRecord {
            stream_id: "s".to_string(),
            sequence,
            event_type: event_type.to_string(),
            payload: payload.to_string(),
            resume_cursor: None,
        }
    }

    #[test]
    fn assemble_text_uses_only_text_append_payloads() {
        let records = vec![
            record(1, events::TURN_START, r#"{"turnId":"t1"}"#),
            record(2, events::TEXT_APPEND, r#"{"turnId":"t1","text":"hello "}"#),
            record(3, events::AGENT_STATUS, r#"{"turnId":"t1","status":"completed"}"#),
            record(4, events::TEXT_APPEND, r#"{"turnId":"t1","text":"world"}"#),
            record(5, events::MESSAGE_DONE, r#"{"turnId":"t1"}"#),
        ];
        assert_eq!(assemble_text(&records), "hello world");
    }

    #[test]
    fn chat_client_rejects_non_http_base_url() {
        assert!(ChatClient::new("https://127.0.0.1:3001").is_err());
        assert!(ChatClient::new("127.0.0.1:3001").is_err());
        assert!(ChatClient::new("http://127.0.0.1:3001").is_ok());
    }
}
