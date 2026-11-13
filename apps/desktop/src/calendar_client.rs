//! Calendar client: Rust shell -> Mock Gateway calendar
//! surface (D1-6a read-only, D1-6b-2 create).
//!
//! Reads: events list/get. No approval loop (reads).
//! The gateway answers 501 on non-macOS and the error
//! surfaces here unchanged; the packaged Mac app reads
//! real Calendar data via EventKit (first use requests
//! full access in the gateway).
//!
//! Create: one digest-bound approved event per call
//! (approval_request -> decide -> create). Uses the
//! long-timeout client (the D1-6a F-1 lesson: the
//! gateway-side EventKit save can block past the shared
//! 10 s read timeout).

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

/// D1-6b-2: one accepted create (the `calendar_events`
/// fact row). The notes body is never in this struct —
/// only its hash. The gateway's extra `replayed` flag
/// is ignored on decode; the row is the fact record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatedCalendarEventRow {
    pub event_id: String,
    pub source_id: String,
    pub calendar_id: String,
    pub title: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub all_day: bool,
    pub location: Option<String>,
    pub notes_hash: String,
    pub idempotency_key: String,
    pub effect_id: String,
    pub policy_version: String,
    pub created_ms: i64,
    pub origin_device_id: String,
}

/// D1-6b-2: a create request. The idempotency key is
/// client-supplied and required (Q-D1-6b-05); the
/// approval id is the decided digest-bound approval for
/// this exact event (Q-D1-6b-22).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCalendarEventInput {
    pub calendar_id: String,
    pub title: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub all_day: bool,
    pub location: Option<String>,
    pub notes: Option<String>,
    pub idempotency_key: String,
    pub tool_call_id: Option<String>,
}

/// D1-6b-2: create shares the 120 s bound (the F-1
/// lesson, same as mail sync/send): the gateway-side
/// EventKit save can block, and the 10 s default would
/// time out before the gateway finishes. All other
/// calendar calls keep the 10 s default.
const CALENDAR_CREATE_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct CalendarClient {
    http: HttpClient,
    /// Long-timeout client (F-1): create only. Reads
    /// share `http` and its 10 s default.
    create_http: HttpClient,
}

impl CalendarClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            create_http: HttpClient::with_timeout(base_url, CALENDAR_CREATE_TIMEOUT_MS)?,
        })
    }

    pub fn list_events(&self) -> std::result::Result<Vec<CalendarEvent>, AgentError> {
        Ok(self.http.get_json("/api/calendar/events")?)
    }

    pub fn get_event(&self, event_id: &str) -> std::result::Result<CalendarEvent, AgentError> {
        Ok(self.http.get_json(&format!("/api/calendar/events/{event_id}"))?)
    }

    /// D1-6b-2: create one event through the digest-bound
    /// approval flow (approval_request → decide →
    /// create). Uses the long-timeout client (same F-1
    /// reason as mail send: the gateway-side write can
    /// take past 10 s).
    pub fn create_event(
        &self,
        input: &CreateCalendarEventInput,
    ) -> std::result::Result<CreatedCalendarEventRow, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode calendar create"))?;
        Ok(self.create_http.post_json("/api/calendar/events", &body)?)
    }
}
