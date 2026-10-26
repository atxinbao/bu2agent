//! Task record client: Rust shell -> Mock Gateway task endpoints.
//!
//! Task records only: create, list, get, update. The deterministic task
//! loop drives these from the frontend; the gateway stores the records
//! (see server.rs). Localhost only, no TLS, Mock only -- same transport
//! rules as the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::http_client::{transport_error, HttpClient};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRecord {
    pub task_id: String,
    pub directory: String,
    pub artifact_path: Option<String>,
    pub status: String,
    /// Runner-only fields (Round 3b); old records carry None.
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub approval_id: Option<String>,
    #[serde(default)]
    pub draft_sha256: Option<String>,
    #[serde(default)]
    pub draft_bytes: Option<u64>,
}

/// A gateway-run task's draft, fetched separately: the record
/// never carries the body, only its digest and byte count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskDraft {
    pub task_id: String,
    pub draft: String,
    pub draft_sha256: Option<String>,
    pub draft_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TaskCreateRequest {
    directory: String,
}

#[derive(Debug, Serialize)]
struct TaskIdRequest {
    task_id: String,
}

#[derive(Debug, Serialize)]
struct TaskUpdateRequest {
    task_id: String,
    status: String,
    artifact_path: Option<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct TaskClient {
    http: HttpClient,
}

impl TaskClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn create(&self, directory: &str) -> Result<TaskRecord> {
        let body = serde_json::to_string(&TaskCreateRequest { directory: directory.to_string() })
            .map_err(|_| transport_error("cannot encode task create request"))?;
        self.http.post_json("/tasks/create", &body)
    }

    pub fn list(&self) -> Result<Vec<TaskRecord>> {
        self.http.get_json("/tasks/list")
    }

    pub fn get(&self, task_id: &str) -> Result<TaskRecord> {
        self.http.get_json(&format!("/tasks/get?task_id={task_id}"))
    }

    pub fn update(
        &self,
        task_id: &str,
        status: &str,
        artifact_path: Option<&str>,
    ) -> Result<TaskRecord> {
        let body = serde_json::to_string(&TaskUpdateRequest {
            task_id: task_id.to_string(),
            status: status.to_string(),
            artifact_path: artifact_path.map(|s| s.to_string()),
        })
        .map_err(|_| transport_error("cannot encode task update request"))?;
        self.http.post_json("/tasks/update", &body)
    }

    /// Round 3b runner endpoints. `run` only starts the runner
    /// (the gateway polls nothing here); `draft` fetches the
    /// sidecar body; `execute` consumes the bound approval and
    /// writes; `reject` decides the approval rejected; `resume`
    /// restarts an interrupted task; `cancel` stops a running or
    /// draft_ready task. All take only the task id.
    pub fn run(&self, task_id: &str) -> Result<TaskRecord> {
        self.post_task_id("/tasks/run", task_id)
    }

    pub fn draft(&self, task_id: &str) -> Result<TaskDraft> {
        self.http.get_json(&format!("/tasks/draft?task_id={task_id}"))
    }

    pub fn execute(&self, task_id: &str) -> Result<TaskRecord> {
        self.post_task_id("/tasks/execute", task_id)
    }

    pub fn reject(&self, task_id: &str) -> Result<TaskRecord> {
        self.post_task_id("/tasks/reject", task_id)
    }

    pub fn resume(&self, task_id: &str) -> Result<TaskRecord> {
        self.post_task_id("/tasks/resume", task_id)
    }

    pub fn cancel(&self, task_id: &str) -> Result<TaskRecord> {
        self.post_task_id("/tasks/cancel", task_id)
    }

    fn post_task_id(&self, path: &str, task_id: &str) -> Result<TaskRecord> {
        let body = serde_json::to_string(&TaskIdRequest { task_id: task_id.to_string() })
            .map_err(|_| transport_error("cannot encode task id request"))?;
        self.http.post_json(path, &body)
    }
}
