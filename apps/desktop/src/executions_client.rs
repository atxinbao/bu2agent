//! Execution record client: Rust shell -> Mock Gateway execution endpoints.
//!
//! Execution records only: list, get, transition. The list/get
//! routes return the P0-E read view: the stored record plus the
//! derived `recoverable` / `active_assignment_id` fields. The
//! transition route returns the bare stored record, so the
//! derived fields default (false / None) on that path.
//! Localhost only, no TLS, Mock only -- same transport rules
//! as the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::http_client::{transport_error, HttpClient};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub execution_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub loop_id: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub worker_session_id: Option<String>,
    #[serde(default)]
    pub workspace_snapshot_id: Option<String>,
    pub status: String,
    #[serde(default = "unknown_origin")]
    pub origin_device_id: String,
    #[serde(default)]
    pub schema_version: u64,
    /// P0-E derived field (list/get only): non-terminal status
    /// with no active assignment driving it.
    #[serde(default)]
    pub recoverable: bool,
    /// P0-E derived field (list/get only): the driving
    /// assignment id, when one exists.
    #[serde(default)]
    pub active_assignment_id: Option<String>,
}

fn unknown_origin() -> String {
    "unknown".to_string()
}

#[derive(Debug, Serialize)]
struct ExecutionTransitionRequest {
    execution_id: String,
    status: String,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct ExecutionsClient {
    http: HttpClient,
}

impl ExecutionsClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list(&self) -> Result<Vec<ExecutionRecord>> {
        self.http.get_json("/executions/list")
    }

    pub fn get(&self, execution_id: &str) -> Result<ExecutionRecord> {
        self.http.get_json(&format!("/executions/get?execution_id={execution_id}"))
    }

    /// Explicit Execution transition (P0-E). The client does not
    /// pre-judge the edge table: illegal edges are refused by
    /// the server with 409. The returned record carries no
    /// derived fields (see the module docs).
    pub fn transition(&self, execution_id: &str, status: &str) -> Result<ExecutionRecord> {
        let body = serde_json::to_string(&ExecutionTransitionRequest {
            execution_id: execution_id.to_string(),
            status: status.to_string(),
        })
        .map_err(|_| transport_error("cannot encode execution transition request"))?;
        self.http.post_json("/executions/transition", &body)
    }
}
