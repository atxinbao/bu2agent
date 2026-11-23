//! Plan client: Rust shell -> Mock Gateway ORC-1 plan
//! surface (draft + read).
//!
//! ORC-1 has no approval flow: drafting a plan is a
//! plain SQLite write (plan row + `plan.created`
//! event), not a write through the approval machinery,
//! so this client never requests or decides an
//! approval. The digest is computed gateway-side; the
//! draft input carries no digest field and no
//! execution_id (a draft has no Execution yet).
//!
//! Draft uses the 120 s client (the
//! `EmailClient::sync_mail` form): the draft holds the
//! tasks lock across the plan write, so a contended
//! gateway could take longer than the shared 10 s read
//! timeout. Reads keep the 10 s default.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// One blueprint step (the gateway `PlanStep`
/// shape). The four-piece set — `path` / `symbol` /
/// `change` / `verify_cmd` / `expected` — is required
/// (Q-ORC-04); `kind` is one of `file_write` /
/// `command` / `verify` / `review`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStep {
    pub step_no: i64,
    pub kind: String,
    pub path: String,
    pub symbol: String,
    pub change: String,
    pub verify_cmd: String,
    pub expected: String,
}

/// One Re-plan-if condition (the gateway `ReplanIf`
/// shape). `signal` is free text; `action` has exactly
/// one value, `stop_and_hand_back` (Q-ORC-05).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanReplanIf {
    pub signal: String,
    pub action: String,
}

/// One plan version (the gateway `PlanRecord` shape).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanRecord {
    pub plan_id: String,
    pub task_id: String,
    pub execution_id: Option<String>,
    pub version: i64,
    pub status: String,
    pub steps: Vec<PlanStep>,
    pub replan_if: Vec<PlanReplanIf>,
    pub planner_model: String,
    pub digest: String,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub origin_device_id: String,
}

/// A draft step, as the caller passes it. Same fields
/// as `PlanStep`; kept separate so a future
/// client-side default never leaks into the stored
/// row by accident.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanStepInput {
    pub step_no: i64,
    pub kind: String,
    pub path: String,
    pub symbol: String,
    pub change: String,
    pub verify_cmd: String,
    pub expected: String,
}

/// A draft Re-plan-if condition, as the caller passes
/// it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanReplanIfInput {
    pub signal: String,
    pub action: String,
}

/// A plan draft request. No digest field, no
/// execution_id: the gateway computes the digest
/// server-side and a draft has no Execution yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDraftInput {
    pub task_id: String,
    pub steps: Vec<PlanStepInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replan_if: Vec<PlanReplanIfInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner_model: Option<String>,
}

/// The 120 s bound (the `EmailClient::sync_mail`
/// form): the draft holds the tasks lock across the
/// plan write, so a contended gateway could take
/// longer than the shared 10 s read timeout.
const PLAN_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct PlanClient {
    http: HttpClient,
    /// Long-timeout client: draft only. Reads keep the
    /// 10 s default so a hung gateway still fails fast
    /// instead of hanging the CLI.
    sync_http: HttpClient,
}

impl PlanClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            sync_http: HttpClient::with_timeout(base_url, PLAN_TIMEOUT_MS)?,
        })
    }

    pub fn draft_plan(
        &self,
        input: &PlanDraftInput,
    ) -> std::result::Result<PlanRecord, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode plan draft"))?;
        Ok(self.sync_http.post_json("/tasks/plan/draft", &body)?)
    }

    pub fn current_plan(&self, task_id: &str) -> std::result::Result<PlanRecord, AgentError> {
        Ok(self.http.get_json(&format!("/tasks/plan?task_id={task_id}"))?)
    }

    pub fn plan_get(&self, plan_id: &str) -> std::result::Result<PlanRecord, AgentError> {
        Ok(self.http.get_json(&format!("/tasks/plan/{plan_id}"))?)
    }

    pub fn plans_list(
        &self,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> std::result::Result<Vec<PlanRecord>, AgentError> {
        let mut path = "/tasks/plan".to_string();
        let mut query = Vec::new();
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if let Some(offset) = offset {
            query.push(format!("offset={offset}"));
        }
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query.join("&"));
        }
        Ok(self.http.get_json(&path)?)
    }
}
