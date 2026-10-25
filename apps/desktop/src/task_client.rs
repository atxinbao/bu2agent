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
}

#[derive(Debug, Serialize)]
struct TaskCreateRequest {
    directory: String,
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
}
