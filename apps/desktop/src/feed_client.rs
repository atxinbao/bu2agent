//! Feed client: Rust shell -> Mock Gateway feed unit
//! surface (Task 17, record subset).
//!
//! Mirrors the space client: no approval loop (local
//! feed records, like the other metadata surfaces), and
//! the base URL is allowlisted to the localhost Mock.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedUnit {
    pub unit_id: String,
    pub title: String,
    pub body: String,
    pub source: String,
    pub seen: bool,
    pub reaction: Option<String>,
    pub tap_count: u64,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedStatus {
    pub units: usize,
    pub unseen: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedSeenResult {
    pub seen: Vec<String>,
    pub unknown: Vec<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct FeedClient {
    http: HttpClient,
}

impl FeedClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list_units(&self) -> std::result::Result<Vec<FeedUnit>, AgentError> {
        Ok(self.http.get_json("/api/feed")?)
    }

    pub fn get_unit(&self, unit_id: &str) -> std::result::Result<FeedUnit, AgentError> {
        Ok(self.http.get_json(&format!("/api/feed/units/{unit_id}"))?)
    }

    pub fn delete_unit(&self, unit_id: &str) -> std::result::Result<FeedUnit, AgentError> {
        Ok(self.http.delete_json(&format!("/api/feed/units/{unit_id}"))?)
    }

    pub fn react_unit(&self, unit_id: &str, reaction: &str) -> std::result::Result<FeedUnit, AgentError> {
        let body = serde_json::json!({ "reaction": reaction }).to_string();
        Ok(self.http.post_json(&format!("/api/feed/units/{unit_id}/reaction"), &body)?)
    }

    pub fn tap_unit(&self, unit_id: &str) -> std::result::Result<FeedUnit, AgentError> {
        Ok(self.http.post_json(&format!("/api/feed/units/{unit_id}/tap"), "")?)
    }

    pub fn seen_units(&self, unit_ids: &[String]) -> std::result::Result<FeedSeenResult, AgentError> {
        let body = serde_json::json!({ "unit_ids": unit_ids }).to_string();
        Ok(self.http.post_json("/api/feed/units/seen", &body)?)
    }

    pub fn get_status(&self) -> std::result::Result<FeedStatus, AgentError> {
        Ok(self.http.get_json("/api/feed/status")?)
    }
}
