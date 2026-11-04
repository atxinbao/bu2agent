//! Assignment client: Rust shell -> Mock Gateway assignment endpoints.
//!
//! Assignment rows only: list, get, takeover. Rows live in
//! SQLite only (P0-E); reads serve SQLite and shadow modes,
//! takeover requires SQLite mode. Localhost only, no TLS, Mock
//! only -- same transport rules as the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::executions_client::ExecutionRecord;
use crate::http_client::{transport_error, HttpClient};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentRow {
    pub assignment_id: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub worker_session_id: Option<String>,
    pub origin_device_id: String,
    pub status: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub lease_expires_ms: Option<i64>,
    #[serde(default)]
    pub heartbeat_ms: Option<i64>,
    pub schema_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRow {
    pub checkpoint_id: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    pub execution_seq: i64,
    pub kind: String,
    pub payload_json: String,
    pub origin_device_id: String,
    pub schema_version: i64,
}

/// Successful takeover outcome, verbatim from the gateway:
/// the new driving assignment, the execution row after its
/// worker update, the plan-position checkpoint written at
/// takeover, and any old driving assignments superseded
/// (normally empty: validation refuses when one exists).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TakeoverOutcome {
    pub assignment: AssignmentRow,
    pub execution: ExecutionRecord,
    pub checkpoint: CheckpointRow,
    pub superseded: Vec<AssignmentRow>,
}

#[derive(Debug, Serialize)]
struct TakeoverRequest {
    execution_id: String,
    worker_session_id: Option<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct AssignmentsClient {
    http: HttpClient,
}

impl AssignmentsClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list(&self) -> Result<Vec<AssignmentRow>> {
        self.http.get_json("/assignments/list")
    }

    pub fn get(&self, assignment_id: &str) -> Result<AssignmentRow> {
        self.http.get_json(&format!("/assignments/get?assignment_id={assignment_id}"))
    }

    /// Take over a recoverable execution as the current worker.
    /// No worker session is named: the gateway creates one for
    /// this process. Refused with 409 while any driving
    /// assignment exists (no double-drive).
    pub fn takeover(&self, execution_id: &str) -> Result<TakeoverOutcome> {
        let body = serde_json::to_string(&TakeoverRequest {
            execution_id: execution_id.to_string(),
            worker_session_id: None,
        })
        .map_err(|_| transport_error("cannot encode takeover request"))?;
        self.http.post_json("/assignments/takeover", &body)
    }
}
