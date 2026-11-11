//! Calendar client: Rust shell -> Mock Gateway calendar
//! read-only surface (D1-6a, EventKit on macOS, 501
//! elsewhere).
//!
//! Read-only: events list/get. No approval loop
//! (reads). The gateway answers 501 on non-macOS and
//! the error surfaces here unchanged; the packaged Mac
//! app reads real Calendar data via EventKit (first use
//! requests full access in the gateway).

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalendarEvent {
    pub event_id: String,
    pub title: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub all_day: bool,
    pub calendar_id: Option<String>,
    pub calendar_title: Option<String>,
    pub location: Option<String>,
    pub notes: Option<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct CalendarClient {
    http: HttpClient,
}

impl CalendarClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
        })
    }

    pub fn list_events(&self) -> std::result::Result<Vec<CalendarEvent>, AgentError> {
        Ok(self.http.get_json("/api/calendar/events")?)
    }

    pub fn get_event(&self, event_id: &str) -> std::result::Result<CalendarEvent, AgentError> {
        Ok(self.http.get_json(&format!("/api/calendar/events/{event_id}"))?)
    }
}
