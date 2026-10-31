//! Space client: Rust shell -> Mock Gateway spaces v2
//! surface (Task 16, record subset).
//!
//! Mirrors the peer client: no approval loop (local
//! saved-space records, like the other metadata surfaces),
//! and the base URL is allowlisted to the localhost Mock.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpaceItem {
    pub kind: String,
    #[serde(rename = "ref")]
    pub reference: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Space {
    pub slug: String,
    pub title: String,
    pub items: Vec<SpaceItem>,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct SpaceClient {
    http: HttpClient,
}

impl SpaceClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list_spaces(&self) -> std::result::Result<Vec<Space>, AgentError> {
        Ok(self.http.get_json("/spaces/v2")?)
    }

    pub fn save_space(
        &self,
        slug: &str,
        title: Option<&str>,
        items: &[SpaceItem],
    ) -> std::result::Result<Space, AgentError> {
        let body = serde_json::json!({ "title": title, "items": items }).to_string();
        Ok(self.http.post_json(&format!("/spaces/v2/{slug}/save"), &body)?)
    }

    pub fn delete_space(&self, slug: &str) -> std::result::Result<Space, AgentError> {
        Ok(self.http.delete_json(&format!("/spaces/v2/{slug}"))?)
    }
}
