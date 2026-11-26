//! Timers client: Rust shell -> Mock Gateway AO-2
//! timer surface (create + list + revoke).
//!
//! A timer is a fact row, not an Agent run: the
//! client only records when it should fire. The
//! sweeper drive fires due rows by writing an event
//! through AO-1's ingest path — a timer NEVER starts
//! an Agent directly.
//!
//! All three calls are single-row local ops (no lock
//! holds, no AppleScript), so the shared 10 s read
//! timeout is enough — unlike swarm fanout, there is
//! no long-timeout client here.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// One timer row (the gateway `AgentTimerView`
/// shape, plan §4): the timer's kind, its fire
/// time, and its live status. The payload is a
/// summary object, never raw text (red line 7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerView {
    pub timer_id: String,
    pub kind: String,
    pub payload_summary_json: serde_json::Value,
    #[serde(default)]
    pub session_id: Option<String>,
    pub status: String,
    pub next_fire_ms: i64,
    #[serde(default)]
    pub recurrence_json: Option<serde_json::Value>,
    pub created_ms: i64,
}

/// A timer create request: `kind` is `follow_up`
/// (one-shot) or `schedule` (periodic). A schedule
/// needs `recurrence_json` = `{"interval_ms": N}`
/// with N > 0; a follow-up must not carry one
/// (AO2-J2). `next_fire_ms` may be in the past —
/// past means due on the first tick (B4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerCreateInput {
    pub kind: String,
    pub next_fire_ms: i64,
    #[serde(default)]
    pub recurrence_json: Option<serde_json::Value>,
    #[serde(default)]
    pub payload_summary_json: Option<serde_json::Value>,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct TimerClient {
    http: HttpClient,
}

impl TimerClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
        })
    }

    pub fn timer_create(
        &self,
        input: &TimerCreateInput,
    ) -> std::result::Result<TimerView, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode timer create"))?;
        Ok(self.http.post_json("/agent/timers", &body)?)
    }

    pub fn timer_list(&self, limit: Option<i64>) -> std::result::Result<Vec<TimerView>, AgentError> {
        let path = match limit {
            Some(limit) => format!("/agent/timers?limit={limit}"),
            None => "/agent/timers".to_string(),
        };
        Ok(self.http.get_json(&path)?)
    }

    pub fn timer_delete(
        &self,
        timer_id: &str,
    ) -> std::result::Result<TimerView, AgentError> {
        Ok(self.http.delete_json(&format!("/agent/timers/{timer_id}"))?)
    }
}
