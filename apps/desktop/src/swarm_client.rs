//! Swarm client: Rust shell -> Mock Gateway ORC-5
//! swarm surface (create + fanout + read).
//!
//! A swarm fans a task's plan steps out to parallel
//! worker copies. The client never requests or decides
//! an approval itself: the step approvals are built
//! gateway-side by the sweeper drive, and the user
//! decides them through the existing approval surface
//! (`POST /approval/decide`). The fanout body carries
//! `step_nos` (1..=4) and an optional `priority` —
//! the priority is recorded on the copy add-runs
//! (immediate runs are not ordered by it, E2-D).
//!
//! Create + fanout use the 120 s client (the
//! `EmailClient::sync_mail` form): fanout holds the
//! tasks lock and creates worker runs, so a contended
//! gateway could take longer than the shared 10 s read
//! timeout. Reads keep the 10 s default.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// One step in the swarm view (the gateway
/// `SwarmStepView` shape, plan §6.3): the step's
/// live run status plus its approval's live status.
/// The SQLite-only content fields never leave the
/// gateway, so they are not here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmStepView {
    pub step_no: i64,
    pub worker_id: String,
    #[serde(default)]
    pub worktree_id: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    pub run_status: String,
    #[serde(default)]
    pub approval_id: Option<String>,
    #[serde(default)]
    pub approval_status: Option<String>,
}

/// One board row in the swarm view (the gateway
/// `SwarmMessageView` shape, plan §6.3): the
/// board text is never stored — only its sha256.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmMessageView {
    pub message_id: String,
    pub sender_worker_id: String,
    pub kind: String,
    pub body_hash: String,
    pub created_ms: i64,
}

/// The swarm view (the gateway `SwarmView` shape,
/// plan §6.3): the swarm row + step views + the
/// latest board rows + worktree records still
/// present after the swarm ended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmView {
    pub swarm_id: String,
    pub task_id: String,
    pub plan_id: String,
    pub status: String,
    pub steps: Vec<SwarmStepView>,
    pub messages: Vec<SwarmMessageView>,
    pub leftover_worktrees: Vec<String>,
    pub created_ms: i64,
    pub updated_ms: i64,
}

/// A swarm create request: the task whose current
/// plan the swarm pins.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmCreateInput {
    pub task_id: String,
}

/// A swarm fan-out request: the plan steps to fan
/// out (1..=4, each `file_write` / `command`) and
/// an optional priority for the copy add-runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwarmFanoutInput {
    pub step_nos: Vec<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i64>,
}

/// The 120 s bound (the `EmailClient::sync_mail`
/// form): fanout holds the tasks lock across the
/// run creation, so a contended gateway could take
/// longer than the shared 10 s read timeout.
const SWARM_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct SwarmClient {
    http: HttpClient,
    /// Long-timeout client: create + fanout only.
    /// Reads keep the 10 s default so a hung gateway
    /// still fails fast instead of hanging the CLI.
    sync_http: HttpClient,
}

impl SwarmClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            sync_http: HttpClient::with_timeout(base_url, SWARM_TIMEOUT_MS)?,
        })
    }

    pub fn swarm_create(
        &self,
        input: &SwarmCreateInput,
    ) -> std::result::Result<SwarmView, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode swarm create"))?;
        Ok(self.sync_http.post_json("/swarm/create", &body)?)
    }

    pub fn swarm_fanout(
        &self,
        swarm_id: &str,
        input: &SwarmFanoutInput,
    ) -> std::result::Result<SwarmView, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode swarm fanout"))?;
        Ok(self
            .sync_http
            .post_json(&format!("/swarm/{swarm_id}/fanout"), &body)?)
    }

    pub fn swarm_get(
        &self,
        swarm_id: &str,
    ) -> std::result::Result<SwarmView, AgentError> {
        Ok(self.http.get_json(&format!("/swarm/{swarm_id}"))?)
    }
}
