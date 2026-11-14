//! Approvals client: Rust shell -> Mock Gateway approvals endpoints.
//!
//! Approvals only: list, get, editable-message, edit-message.
//! FU-3A (Q-R0-01): the `/approvals` family had no production
//! client; this wires it for the WorkbenchView approvals section.
//! Localhost only, no TLS, Mock only -- same transport rules as
//! the other desktop clients.
//!
//! Edit semantics (verified in SQLite mode, Q-FU-02): only a
//! pending, unconsumed, unbound approval is editable; a
//! successful edit moves it to `edited` and bumps `version`.
//! The edit response itself carries null versions (the map-built
//! view has no version chain), so callers must re-read with
//! `get()` after an edit instead of trusting the response.

use serde::{Deserialize, Serialize};

use crate::http_client::{transport_error, HttpClient};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalBinding {
    pub loop_id: String,
    pub call_id: usize,
    pub tool: String,
    pub args_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalView {
    pub approval_id: String,
    pub tool_call_id: String,
    pub tool: String,
    pub status: String,
    pub consumed: bool,
    pub bound: bool,
    #[serde(default)]
    pub binding: Option<ApprovalBinding>,
    #[serde(default)]
    pub operation_digest: Option<String>,
    #[serde(default)]
    pub effect_binding: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub consumed_by_worker_session_id: Option<String>,
    /// D1-2 version chain; null in JSONL/shadow views, where the
    /// in-memory map carries no version.
    #[serde(default)]
    pub version: Option<u64>,
    #[serde(default)]
    pub supersedes_version: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditableMessage {
    pub approval_id: String,
    #[serde(default)]
    pub message: Option<String>,
    pub editable: bool,
}

#[derive(Debug, Serialize)]
struct EditMessageRequest {
    message: String,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct ApprovalsClient {
    http: HttpClient,
}

impl ApprovalsClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list(&self) -> Result<Vec<ApprovalView>> {
        self.http.get_json("/approvals")
    }

    pub fn get(&self, approval_id: &str) -> Result<ApprovalView> {
        self.http.get_json(&format!("/approvals/{approval_id}"))
    }

    pub fn editable_message(&self, approval_id: &str) -> Result<EditableMessage> {
        self.http.get_json(&format!("/approvals/{approval_id}/editable-message"))
    }

    pub fn edit_message(&self, approval_id: &str, message: &str) -> Result<ApprovalView> {
        let body = serde_json::to_string(&EditMessageRequest { message: message.to_string() })
            .map_err(|_| transport_error("cannot encode approval edit message request"))?;
        self.http.post_json(&format!("/approvals/{approval_id}/edit-message"), &body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // FU-3A (Q-R0-01): the gateway emits the full ApprovalView;
    // the desktop record must decode every field, including the
    // D1-2 version chain and the nested binding.
    const FULL_JSON: &str = r#"{
        "approval_id": "approval-1", "tool_call_id": "approval-1",
        "tool": "fs.write", "status": "edited", "consumed": false, "bound": true,
        "binding": {"loop_id": "loop-8", "call_id": 1, "tool": "fs.write", "args_sha256": "abc"},
        "operation_digest": "op-digest", "effect_binding": "effect-3",
        "message": "revised draft", "consumed_by_worker_session_id": null,
        "version": 2, "supersedes_version": 1
    }"#;

    const MINIMAL_JSON: &str = r#"{
        "approval_id": "approval-2", "tool_call_id": "approval-2",
        "tool": "mail.send", "status": "pending", "consumed": false, "bound": false
    }"#;

    #[test]
    fn approval_view_decodes_all_fields() {
        let view: ApprovalView = serde_json::from_str(FULL_JSON).expect("full view");
        assert_eq!(view.approval_id, "approval-1");
        assert_eq!(view.tool_call_id, "approval-1");
        assert_eq!(view.tool, "fs.write");
        assert_eq!(view.status, "edited");
        assert!(!view.consumed);
        assert!(view.bound);
        let binding = view.binding.as_ref().expect("binding present");
        assert_eq!(binding.loop_id, "loop-8");
        assert_eq!(binding.call_id, 1);
        assert_eq!(binding.tool, "fs.write");
        assert_eq!(binding.args_sha256, "abc");
        assert_eq!(view.operation_digest.as_deref(), Some("op-digest"));
        assert_eq!(view.effect_binding.as_deref(), Some("effect-3"));
        assert_eq!(view.message.as_deref(), Some("revised draft"));
        assert_eq!(view.consumed_by_worker_session_id, None);
        assert_eq!(view.version, Some(2));
        assert_eq!(view.supersedes_version, Some(1));
    }

    #[test]
    fn approval_view_minimal_json_defaults_to_none() {
        let view: ApprovalView = serde_json::from_str(MINIMAL_JSON).expect("minimal view");
        assert_eq!(view.binding, None);
        assert_eq!(view.operation_digest, None);
        assert_eq!(view.effect_binding, None);
        assert_eq!(view.message, None);
        assert_eq!(view.consumed_by_worker_session_id, None);
        assert_eq!(view.version, None);
        assert_eq!(view.supersedes_version, None);
    }
}
