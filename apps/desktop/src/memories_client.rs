//! Memory client: Rust shell -> Mock Gateway memories surface
//! (D1-5, minimal save/list/detail/delete).
//!
//! Mirrors the goal client: no approval loop (local
//! metadata records, like the other management surfaces),
//! and the base URL is allowlisted to the localhost Mock.
//! The scope triple and the provenance record are plain
//! snake_case structs, so the same shape serves both as
//! the Tauri command argument (deserialized from the
//! invoke payload) and as the gateway request body.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// Scope triple (workspace / project / session); a
/// missing dimension means "broader". The all-empty
/// triple is the global (user-layer) scope.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryScope {
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Provenance record: where the memory came from
/// (session / execution / source) plus its author.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemorySource {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    pub author: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Memory {
    pub memory_id: String,
    pub layer: String,
    pub scope_json: String,
    pub content: String,
    pub source_json: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub deleted: bool,
    pub origin_device_id: String,
    pub schema_version: i64,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct MemoryClient {
    http: HttpClient,
}

impl MemoryClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
        })
    }

    pub fn save_memory(
        &self,
        layer: &str,
        content: &str,
        scope: &MemoryScope,
        current_scope: &MemoryScope,
        source: &MemorySource,
    ) -> std::result::Result<Memory, AgentError> {
        let body = serde_json::json!({
            "layer": layer,
            "content": content,
            "scope": scope,
            "current_scope": current_scope,
            "source": source,
        })
        .to_string();
        Ok(self.http.post_json("/memories/save", &body)?)
    }

    pub fn list_memories(
        &self,
        workspace_id: Option<&str>,
        project_id: Option<&str>,
        session_id: Option<&str>,
        layer: Option<&str>,
    ) -> std::result::Result<Vec<Memory>, AgentError> {
        let mut path = "/memories/list".to_string();
        let mut first = true;
        for (key, value) in [
            ("workspace_id", workspace_id),
            ("project_id", project_id),
            ("session_id", session_id),
            ("layer", layer),
        ] {
            if let Some(value) = value {
                path.push(if first { '?' } else { '&' });
                path.push_str(&format!("{key}={value}"));
                first = false;
            }
        }
        Ok(self.http.get_json(&path)?)
    }

    pub fn get_memory(&self, memory_id: &str) -> std::result::Result<Memory, AgentError> {
        Ok(self.http.get_json(&format!("/memories/{memory_id}"))?)
    }

    pub fn delete_memory(
        &self,
        memory_id: &str,
    ) -> std::result::Result<Memory, AgentError> {
        Ok(self
            .http
            .post_json(&format!("/memories/{memory_id}/delete"), "{}")?)
    }
}
