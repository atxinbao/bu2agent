//! Node client: Rust shell -> Mock Gateway node registry
//! surface (Task 19, record subset).
//!
//! Mirrors the space client: no approval loop (local node
//! records, like the other metadata surfaces), and the base
//! URL is allowlisted to the localhost Mock.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub node_id: String,
    pub name: String,
    pub status: String,
    pub last_heartbeat_ms: u64,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub unpaired: bool,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct NodeClient {
    http: HttpClient,
}

impl NodeClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn register_node(&self, name: Option<&str>) -> std::result::Result<Node, AgentError> {
        let body = serde_json::json!({ "name": name }).to_string();
        Ok(self.http.post_json("/api/nodes/register", &body)?)
    }

    pub fn list_nodes(&self, status: Option<&str>) -> std::result::Result<Vec<Node>, AgentError> {
        let body = serde_json::json!({ "status": status }).to_string();
        Ok(self.http.post_json("/api/nodes/list", &body)?)
    }

    pub fn heartbeat_node(&self, node_id: &str) -> std::result::Result<Node, AgentError> {
        let body = serde_json::json!({ "node_id": node_id }).to_string();
        Ok(self.http.post_json("/api/nodes/heartbeat", &body)?)
    }

    pub fn disconnect_node(&self, node_id: &str) -> std::result::Result<Node, AgentError> {
        let body = serde_json::json!({ "node_id": node_id }).to_string();
        Ok(self.http.post_json("/api/nodes/disconnect", &body)?)
    }

    pub fn unpair_node(&self, node_id: &str) -> std::result::Result<Node, AgentError> {
        let body = serde_json::json!({ "node_id": node_id }).to_string();
        Ok(self.http.post_json("/api/nodes/unpair", &body)?)
    }
}
