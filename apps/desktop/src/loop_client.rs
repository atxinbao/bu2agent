//! 循环客户端只转发四端点；不重试、不自行迁移状态或申请审批。

use crate::http_client::{transport_error, HttpClient};
use crate::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentDigest {
    pub sha256: String,
    pub bytes: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovalBinding {
    pub loop_id: String,
    pub call_id: usize,
    pub tool: String,
    pub args_sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSummary {
    pub status: String,
    pub error: Option<String>,
    pub body_digest: ContentDigest,
    pub truncated: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LateResult {
    pub arrived_at_ms: u64,
    pub result: ResultSummary,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopCall {
    pub call_id: usize,
    pub tool: String,
    pub path: Option<String>,
    pub args_sha256: String,
    pub content_digest: Option<ContentDigest>,
    pub status: String,
    pub error: Option<String>,
    pub approval_id: Option<String>,
    pub binding: Option<ApprovalBinding>,
    pub submitted: bool,
    pub duplicate_of: Option<usize>,
    pub result: Option<ResultSummary>,
    pub late: Option<LateResult>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingWrite {
    pub call_id: usize,
    pub path: String,
    pub text: String,
    pub args_sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingReminder {
    pub call_id: usize,
    pub reminder_id: String,
    pub title: String,
    pub target_completed: bool,
    pub args_sha256: String,
}
/// ORC-4 R4-F1 (R4-J9): a LoopSnapshot without a kind is a main loop.
/// Mirrors the gateway's `default_loop_kind` — the plain `#[serde(default)]`
/// decoded to `""`, which broke the shell round-trip test.
fn default_loop_kind() -> String {
    "main".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopSnapshot {
    pub loop_id: String,
    /// ORC-4 (Q-R4-03/Q-R4-12): "research" or "main".
    #[serde(default = "default_loop_kind")]
    pub kind: String,
    pub prompt_digest: ContentDigest,
    pub model: String,
    pub status: String,
    pub error: Option<String>,
    pub max_calls: usize,
    pub proposal_count: usize,
    pub model_attempts: usize,
    pub budget_ms: u64,
    pub used_ms: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub queue_position: usize,
    pub calls: Vec<LoopCall>,
    pub unknown_writes: Vec<usize>,
    pub unknown_reads: Vec<usize>,
    pub late_model_arrivals_ms: Vec<u64>,
    pub final_digest: Option<ContentDigest>,
    pub in_flight: bool,
    pub remaining_ms: u64,
    pub final_text: Option<String>,
    pub pending_write: Option<PendingWrite>,
    #[serde(default)]
    pub pending_reminder: Option<PendingReminder>,
    pub approval_status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct LoopClient {
    http: HttpClient,
}
impl LoopClient {
    pub fn new(base_url: &str) -> Result<Self> {
        // 与聊天一致，桌面 70 秒等待覆盖网关最多 30 秒的模型步骤。
        Ok(Self {
            http: HttpClient::with_timeout(base_url, 70_000)?,
        })
    }
    fn post(&self, path: &str, body: serde_json::Value) -> Result<LoopSnapshot> {
        self.http.post_json(path, &body.to_string())
    }
    pub fn start(
        &self,
        prompt: &str,
        max_calls: Option<usize>,
        budget_ms: Option<u64>,
    ) -> Result<LoopSnapshot> {
        self.post(
            "/agent/loop/start",
            serde_json::json!({"prompt":prompt,"max_calls":max_calls,"budget_ms":budget_ms}),
        )
    }
    pub fn get(&self, loop_id: &str) -> Result<LoopSnapshot> {
        // ID 输入不能改变请求的查询结构。
        let number = loop_id.strip_prefix("loop-").unwrap_or("");
        if number.is_empty() || !number.bytes().all(|c| c.is_ascii_digit()) {
            return Err(transport_error("invalid loop id"));
        }
        self.http
            .get_json(&format!("/agent/loop/get?loop_id={loop_id}"))
    }
    pub fn list(&self) -> Result<Vec<LoopSnapshot>> {
        self.http.get_json("/agent/loop/list")
    }
    pub fn resume(
        &self,
        loop_id: &str,
        approval_id: Option<&str>,
        binding: Option<ApprovalBinding>,
    ) -> Result<LoopSnapshot> {
        self.post(
            "/agent/loop/resume",
            serde_json::json!({"loop_id":loop_id,"approval_id":approval_id,"binding":binding}),
        )
    }
    pub fn cancel(&self, loop_id: &str) -> Result<LoopSnapshot> {
        self.post("/agent/loop/cancel", serde_json::json!({"loop_id":loop_id}))
    }
    /// ORC-4: start a Planner research loop (planning
    /// phase only; the gateway enforces it) and read
    /// one back. Like `start`, this returns the initial
    /// view — completion is polled via `research_get`.
    pub fn research_start(
        &self,
        task_id: &str,
        worktree_id: Option<&str>,
        max_calls: Option<usize>,
        budget_ms: Option<u64>,
    ) -> Result<LoopSnapshot> {
        self.post(
            "/agent/loop/research/start",
            serde_json::json!({"task_id":task_id,"worktree_id":worktree_id,"max_calls":max_calls,"budget_ms":budget_ms}),
        )
    }
    pub fn research_get(&self, loop_id: &str) -> Result<LoopSnapshot> {
        // ID 输入不能改变请求的查询结构。
        let number = loop_id.strip_prefix("loop-").unwrap_or("");
        if number.is_empty() || !number.bytes().all(|c| c.is_ascii_digit()) {
            return Err(transport_error("invalid loop id"));
        }
        self.http
            .get_json(&format!("/agent/loop/research/{loop_id}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ORC-4 R4-F1 (R4-J9): a LoopSnapshot JSON without a kind decodes
    // as a main loop — the plain `#[serde(default)]` decoded to `""`,
    // which broke `shell_tests::loop_workflow_invoke_roundtrip` (CI #163).
    const WITHOUT_KIND_JSON: &str = r#"{
        "loop_id": "loop-8",
        "prompt_digest": {"sha256": "prompt-hash", "bytes": 6},
        "model": "fixture", "status": "created", "error": null,
        "max_calls": 3, "proposal_count": 0, "model_attempts": 0,
        "budget_ms": 45000, "used_ms": 0, "created_at_ms": 1, "updated_at_ms": 1,
        "queue_position": 0, "calls": [], "unknown_writes": [], "unknown_reads": [],
        "late_model_arrivals_ms": [], "final_digest": null,
        "in_flight": false, "remaining_ms": 45000, "final_text": null,
        "pending_write": null, "pending_reminder": null, "approval_status": null
    }"#;

    #[test]
    fn loop_snapshot_without_kind_defaults_to_main() {
        let snapshot: LoopSnapshot =
            serde_json::from_str(WITHOUT_KIND_JSON).expect("snapshot without kind");
        assert_eq!(snapshot.kind, "main");
        let round_tripped: LoopSnapshot =
            serde_json::from_value(serde_json::to_value(&snapshot).expect("serialize"))
                .expect("re-deserialize");
        assert_eq!(round_tripped.kind, "main");
    }
}
