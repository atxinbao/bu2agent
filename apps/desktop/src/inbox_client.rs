//! Inbox client: Rust shell -> Mock Gateway inbox endpoints.
//!
//! Inbox rows only: list, get, ack. Rows live in SQLite only
//! (P0-D); without a configured SQLite database the routes
//! answer 5xx fail-closed. Localhost only, no TLS, Mock only
//! -- same transport rules as the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::http_client::{transport_error, HttpClient};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxRow {
    pub inbox_id: String,
    pub kind: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub effect_id: Option<String>,
    #[serde(default)]
    pub approval_id: Option<String>,
    pub source_event_id: String,
    pub created_ms: i64,
    #[serde(default)]
    pub resolved_ms: Option<i64>,
    #[serde(default)]
    pub resolved_reason: Option<String>,
}

#[derive(Debug, Serialize)]
struct InboxIdRequest {
    inbox_id: String,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct InboxClient {
    http: HttpClient,
}

impl InboxClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list(&self, kind: Option<&str>, open_only: bool) -> Result<Vec<InboxRow>> {
        let mut path = "/inbox/list".to_string();
        let mut first = true;
        if let Some(kind) = kind {
            path.push_str(&format!("?kind={kind}"));
            first = false;
        }
        if !first {
            path.push('&');
        } else {
            path.push('?');
        }
        path.push_str(&format!("open_only={open_only}"));
        self.http.get_json(&path)
    }

    pub fn get(&self, inbox_id: &str) -> Result<InboxRow> {
        self.http.get_json(&format!("/inbox/get?inbox_id={inbox_id}"))
    }

    pub fn ack(&self, inbox_id: &str) -> Result<InboxRow> {
        let body = serde_json::to_string(&InboxIdRequest { inbox_id: inbox_id.to_string() })
            .map_err(|_| transport_error("cannot encode inbox id request"))?;
        self.http.post_json("/inbox/ack", &body)
    }
}
