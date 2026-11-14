//! Mandates client: Rust shell -> Mock Gateway mandates endpoints.
//!
//! Mandate rows only: list, get.
//! FU-3C (Q-R0-01): the `/mandates` family had no production
//! client; this wires it for the "授权范围" (authorization
//! scope) sections in ExecutionDetail and TasksView. Rows live
//! in SQLite only (P0-D); without a configured SQLite
//! database the routes answer 5xx fail-closed. Localhost only,
//! no TLS, Mock only -- same transport rules as the other
//! desktop clients.
//!
//! `allowed_json` is a JSON string, not an object; callers
//! display it as text (full in the expanded row detail).
//! A row binds a task (`task_id`, `execution_id` NULL) or one
//! execution to a goal text, an allowed tool set and the policy
//! version live at its creation; superseded rows are kept.

use serde::{Deserialize, Serialize};

use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MandateRow {
    pub mandate_id: String,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub execution_id: Option<String>,
    pub goal: String,
    pub allowed_json: String,
    pub policy_version_id: String,
    pub status: String,
    pub created_ms: i64,
    pub origin_device_id: String,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct MandatesClient {
    http: HttpClient,
}

impl MandatesClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list(
        &self,
        task_id: Option<&str>,
        execution_id: Option<&str>,
    ) -> Result<Vec<MandateRow>> {
        let mut path = "/mandates".to_string();
        let mut first = true;
        if let Some(task_id) = task_id {
            path.push_str(&format!("?task_id={task_id}"));
            first = false;
        }
        if let Some(execution_id) = execution_id {
            if !first {
                path.push('&');
            } else {
                path.push('?');
            }
            path.push_str(&format!("execution_id={execution_id}"));
        }
        self.http.get_json(&path)
    }

    pub fn get(&self, mandate_id: &str) -> Result<MandateRow> {
        self.http.get_json(&format!("/mandates/{mandate_id}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // FU-3C (Q-R0-01): the gateway emits the full MandateRow;
    // the desktop record must decode every field, including
    // the JSON-string allowed set. A task-level row carries
    // `execution_id` NULL; the minimal form omits both link
    // keys entirely, which must decode to None (this locks
    // in the `#[serde(default)]` on both Option fields).
    const FULL_JSON: &str = r#"{
        "mandate_id": "mandate-1", "task_id": "task-8",
        "execution_id": "execution-88",
        "goal": "send the weekly report",
        "allowed_json": "{\"tools\":[\"mail.send\"]}",
        "policy_version_id": "policy-v2", "status": "active",
        "created_ms": 1794611497606, "origin_device_id": "dev-1"
    }"#;

    const MINIMAL_JSON: &str = r#"{
        "mandate_id": "mandate-2",
        "goal": "draft only",
        "allowed_json": "{}",
        "policy_version_id": "policy-v1", "status": "superseded",
        "created_ms": 1794611497700, "origin_device_id": "dev-1"
    }"#;

    #[test]
    fn mandate_row_decodes_all_fields() {
        let row: MandateRow = serde_json::from_str(FULL_JSON).expect("full row");
        assert_eq!(row.mandate_id, "mandate-1");
        assert_eq!(row.task_id.as_deref(), Some("task-8"));
        assert_eq!(row.execution_id.as_deref(), Some("execution-88"));
        assert_eq!(row.goal, "send the weekly report");
        assert_eq!(row.allowed_json, "{\"tools\":[\"mail.send\"]}");
        assert_eq!(row.policy_version_id, "policy-v2");
        assert_eq!(row.status, "active");
        assert_eq!(row.created_ms, 1794611497606);
        assert_eq!(row.origin_device_id, "dev-1");
    }

    #[test]
    fn mandate_row_minimal_json_defaults_links_to_none() {
        let row: MandateRow = serde_json::from_str(MINIMAL_JSON).expect("minimal row");
        assert_eq!(row.mandate_id, "mandate-2");
        assert_eq!(row.task_id, None);
        assert_eq!(row.execution_id, None);
        assert_eq!(row.goal, "draft only");
        assert_eq!(row.allowed_json, "{}");
        assert_eq!(row.policy_version_id, "policy-v1");
        assert_eq!(row.status, "superseded");
        assert_eq!(row.created_ms, 1794611497700);
        assert_eq!(row.origin_device_id, "dev-1");
    }
}
