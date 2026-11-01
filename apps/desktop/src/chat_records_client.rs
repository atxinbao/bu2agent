//! Chat records client: Rust shell -> Mock Gateway chat
//! record surface (Task 18, record subset).
//!
//! Mirrors the space client: no approval loop (local record
//! operations, like the other metadata surfaces), and the
//! base URL is allowlisted to the localhost Mock.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatTurnView {
    pub thread_id: String,
    pub index: usize,
    pub role: String,
    pub text: String,
    pub created_ms: u64,
    pub model: Option<String>,
    pub finish_reason: Option<String>,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatThemes {
    pub theme: String,
    pub available: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadUnread {
    pub thread_id: String,
    pub unread: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnreadCount {
    pub unread_count: usize,
    pub threads: Vec<ThreadUnread>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeenResult {
    pub thread_id: String,
    pub marker: usize,
    pub unread: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryWindow {
    pub thread_id: String,
    pub total_visible: usize,
    pub entries: Vec<ChatTurnView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchMatch {
    pub thread_id: String,
    pub index: usize,
    pub role: String,
    pub text: String,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct ChatRecordsClient {
    http: HttpClient,
}

impl ChatRecordsClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn themes(&self) -> std::result::Result<ChatThemes, AgentError> {
        Ok(self.http.get_json("/api/chat/themes")?)
    }

    pub fn set_theme(&self, theme: &str) -> std::result::Result<ChatThemes, AgentError> {
        let body = serde_json::json!({ "theme": theme }).to_string();
        Ok(self.http.post_json("/api/chat/themes/set", &body)?)
    }

    pub fn unread_count(&self) -> std::result::Result<UnreadCount, AgentError> {
        Ok(self.http.get_json("/chat/unread-count")?)
    }

    pub fn mark_seen(
        &self,
        thread_id: &str,
        index: Option<usize>,
    ) -> std::result::Result<SeenResult, AgentError> {
        let body = serde_json::json!({ "index": index }).to_string();
        Ok(self.http.post_json(&format!("/chat/{thread_id}/seen"), &body)?)
    }

    pub fn delete_turn(
        &self,
        thread_id: &str,
        index: usize,
    ) -> std::result::Result<ChatTurnView, AgentError> {
        let body = serde_json::json!({ "thread_id": thread_id, "index": index }).to_string();
        Ok(self.http.post_json("/api/chat/delete-turn", &body)?)
    }

    pub fn message(
        &self,
        thread_id: &str,
        index: usize,
    ) -> std::result::Result<ChatTurnView, AgentError> {
        Ok(self.http.get_json(&format!("/chat/message?thread_id={thread_id}&index={index}"))?)
    }

    pub fn history_window(
        &self,
        thread_id: &str,
        offset: usize,
        limit: usize,
    ) -> std::result::Result<HistoryWindow, AgentError> {
        Ok(self.http.get_json(&format!(
            "/chat/history-window?thread_id={thread_id}&offset={offset}&limit={limit}"
        ))?)
    }

    pub fn search(&self, query: &str) -> std::result::Result<Vec<SearchMatch>, AgentError> {
        Ok(self.http.get_json(&format!("/chat/search?q={}", encode_query(query)))?)
    }
}

/// Minimal query percent-encoding (no urlencoding crate in
/// this workspace): unreserved ASCII passes through, every
/// other byte is %XX. The gateway trims and lowercases the
/// decoded query before matching.
fn encode_query(query: &str) -> String {
    let mut out = String::new();
    for byte in query.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_encoding_covers_spaces_and_unicode() {
        assert_eq!(encode_query("hello world"), "hello%20world");
        assert_eq!(encode_query("a+b"), "a%2Bb");
        assert_eq!(encode_query("你好"), "%E4%BD%A0%E5%A5%BD");
    }
}
