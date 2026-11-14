//! Source client: Rust shell -> Mock Gateway sources surface
//! (D1-4, minimal list/detail/disconnect; tails adds
//! reconnect, the mirror of disconnect).
//!
//! Mirrors the goal client: no approval loop (local
//! metadata records, like the other management surfaces),
//! and the base URL is allowlisted to the localhost Mock.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Source {
    pub source_id: String,
    pub kind: String,
    pub display_name: String,
    pub config_json: String,
    pub scope: String,
    pub health: String,
    pub cursor: String,
    pub dedup_key: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub origin_device_id: String,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct SourceClient {
    http: HttpClient,
}

impl SourceClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
        })
    }

    pub fn list_sources(&self) -> std::result::Result<Vec<Source>, AgentError> {
        Ok(self.http.get_json("/sources/list")?)
    }

    pub fn get_source(&self, source_id: &str) -> std::result::Result<Source, AgentError> {
        Ok(self.http.get_json(&format!("/sources/{source_id}"))?)
    }

    pub fn disconnect_source(
        &self,
        source_id: &str,
    ) -> std::result::Result<Source, AgentError> {
        Ok(self
            .http
            .post_json(&format!("/sources/{source_id}/disconnect"), "{}")?)
    }

    pub fn reconnect_source(
        &self,
        source_id: &str,
    ) -> std::result::Result<Source, AgentError> {
        Ok(self
            .http
            .post_json(&format!("/sources/{source_id}/reconnect"), "{}")?)
    }
}
