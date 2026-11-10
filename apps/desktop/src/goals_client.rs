//! Goal client: Rust shell -> Mock Gateway goals surface
//! (D1-1, minimal list/detail).
//!
//! Mirrors the space client: no approval loop (local
//! metadata records, like the other management surfaces),
//! and the base URL is allowlisted to the localhost Mock.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Goal {
    pub goal_id: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub origin_device_id: String,
    pub schema_version: i64,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct GoalClient {
    http: HttpClient,
}

impl GoalClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
        })
    }

    pub fn list_goals(&self) -> std::result::Result<Vec<Goal>, AgentError> {
        Ok(self.http.get_json("/goals/list")?)
    }

    pub fn get_goal(&self, goal_id: &str) -> std::result::Result<Goal, AgentError> {
        Ok(self.http.get_json(&format!("/goals/{goal_id}"))?)
    }

    pub fn create_goal(
        &self,
        title: &str,
        description: Option<&str>,
    ) -> std::result::Result<Goal, AgentError> {
        let body = serde_json::json!({ "title": title, "description": description }).to_string();
        Ok(self.http.post_json("/goals/create", &body)?)
    }

    pub fn archive_goal(&self, goal_id: &str) -> std::result::Result<Goal, AgentError> {
        Ok(self
            .http
            .post_json(&format!("/goals/{goal_id}/archive"), "{}")?)
    }
}
