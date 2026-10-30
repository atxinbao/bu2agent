//! Session record client: Rust shell -> Mock Gateway session endpoints.
//!
//! Sessions only: create, list summaries, get one session, send a
//! message (appending the user turn and the model or echo reply),
//! delete as a tombstone. The gateway stores bodies in its own
//! sessions JSONL, never in the audit files (see server.rs).
//! Localhost only, no TLS, Mock only -- same transport rules as
//! the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::http_client::{transport_error, HttpClient};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionTurn {
    pub role: String,
    pub text: String,
    pub created_ms: u64,
    pub model: Option<String>,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub title: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub deleted: bool,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub archived: bool,
    pub turns: Vec<SessionTurn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub title: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub turn_count: usize,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub archived: bool,
}

#[derive(Debug, Serialize)]
struct SessionCreateRequest {
    title: Option<String>,
}

#[derive(Debug, Serialize)]
struct SessionSendRequest {
    session_id: String,
    text: String,
}

#[derive(Debug, Serialize)]
struct SessionDeleteRequest {
    session_id: String,
}

#[derive(Debug, Serialize)]
struct SessionIdRequest {
    session_id: String,
}

#[derive(Debug, Serialize)]
struct SessionRenameRequest {
    session_id: String,
    title: String,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct SessionClient {
    http: HttpClient,
}

impl SessionClient {
    pub fn new(base_url: &str) -> Result<Self> {
        // Like the loop client, the 70 s wait covers the gateway's
        // 30 s model step on send.
        Ok(Self { http: HttpClient::with_timeout(base_url, 70_000)? })
    }

    pub fn create(&self, title: Option<&str>) -> Result<SessionRecord> {
        let body = serde_json::to_string(&SessionCreateRequest { title: title.map(|s| s.to_string()) })
            .map_err(|_| transport_error("cannot encode session create request"))?;
        self.http.post_json("/sessions/create", &body)
    }

    pub fn list(&self) -> Result<Vec<SessionSummary>> {
        self.http.get_json("/sessions/list")
    }

    pub fn list_including_archived(&self) -> Result<Vec<SessionSummary>> {
        self.http.get_json("/sessions/list?include_archived=true")
    }

    pub fn get(&self, session_id: &str) -> Result<SessionRecord> {
        self.http.get_json(&format!("/sessions/get?session_id={session_id}"))
    }

    pub fn send(&self, session_id: &str, text: &str) -> Result<SessionRecord> {
        let body = serde_json::to_string(&SessionSendRequest {
            session_id: session_id.to_string(),
            text: text.to_string(),
        })
        .map_err(|_| transport_error("cannot encode session send request"))?;
        self.http.post_json("/sessions/send", &body)
    }

    pub fn delete(&self, session_id: &str) -> Result<()> {
        let body = serde_json::to_string(&SessionDeleteRequest { session_id: session_id.to_string() })
            .map_err(|_| transport_error("cannot encode session delete request"))?;
        let _: serde_json::Value = self.http.post_json("/sessions/delete", &body)?;
        Ok(())
    }

    fn flag(&self, path: &str, session_id: &str) -> Result<SessionSummary> {
        let body = serde_json::to_string(&SessionIdRequest { session_id: session_id.to_string() })
            .map_err(|_| transport_error("cannot encode session flag request"))?;
        self.http.post_json(path, &body)
    }

    pub fn pin(&self, session_id: &str) -> Result<SessionSummary> {
        self.flag("/session/pin", session_id)
    }

    pub fn unpin(&self, session_id: &str) -> Result<SessionSummary> {
        self.flag("/session/unpin", session_id)
    }

    pub fn archive(&self, session_id: &str) -> Result<SessionSummary> {
        self.flag("/session/archive", session_id)
    }

    pub fn unarchive(&self, session_id: &str) -> Result<SessionSummary> {
        self.flag("/session/unarchive", session_id)
    }

    pub fn rename(&self, session_id: &str, title: &str) -> Result<SessionSummary> {
        let body = serde_json::to_string(&SessionRenameRequest {
            session_id: session_id.to_string(),
            title: title.to_string(),
        })
        .map_err(|_| transport_error("cannot encode session rename request"))?;
        self.http.post_json("/session/rename", &body)
    }
}
