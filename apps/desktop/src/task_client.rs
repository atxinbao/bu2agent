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
pub struct TaskExecuteBinding {
    pub location_id: String,
    pub worktree_id: String,
    pub root_digest: String,
}

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
    /// D1-1 link fields (FU-2, Q-R0-03): the gateway has carried these
    /// since D1-1, but the desktop record silently dropped them
    /// (serde tolerates unknown fields). Manual tasks carry None.
    #[serde(default)]
    pub execute_binding: Option<TaskExecuteBinding>,
    #[serde(default)]
    pub goal_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    // FU-2 (Q-R0-03): the gateway has emitted all 12 fields since D1-1;
    // the desktop record used to drop the last four silently.
    const FULL_JSON: &str = r#"{
        "task_id": "task-1", "directory": "/tmp/x", "artifact_path": null,
        "status": "created", "error": null, "approval_id": null,
        "draft_sha256": null, "draft_bytes": null,
        "execute_binding": {"location_id": "loc-1", "worktree_id": "wt-a", "root_digest": "abc"},
        "goal_id": "goal-1", "project_id": "proj-1", "workspace_id": "ws-1"
    }"#;

    const LEGACY_JSON: &str = r#"{
        "task_id": "task-1", "directory": "/tmp/x", "artifact_path": null,
        "status": "created", "error": null, "approval_id": null,
        "draft_sha256": null, "draft_bytes": null
    }"#;

    #[test]
    fn task_record_decodes_all_twelve_fields() {
        let record: TaskRecord = serde_json::from_str(FULL_JSON).expect("full record");
        let binding = record.execute_binding.as_ref().expect("binding present");
        assert_eq!(binding.location_id, "loc-1");
        assert_eq!(binding.worktree_id, "wt-a");
        assert_eq!(binding.root_digest, "abc");
        assert_eq!(record.goal_id.as_deref(), Some("goal-1"));
        assert_eq!(record.project_id.as_deref(), Some("proj-1"));
        assert_eq!(record.workspace_id.as_deref(), Some("ws-1"));
    }

    #[test]
    fn task_record_legacy_eight_fields_default_to_none() {
        let record: TaskRecord = serde_json::from_str(LEGACY_JSON).expect("legacy record");
        assert_eq!(record.execute_binding, None);
        assert_eq!(record.goal_id, None);
        assert_eq!(record.project_id, None);
        assert_eq!(record.workspace_id, None);
    }
}
