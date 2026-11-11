//! D1-6a (Q-D1-64): calendar read-only via EventKit.
//!
//! The same framework as reminders
//! (`eventkit_backend.rs`, `EKEntityType::Reminder`),
//! the other entity type (`EKEntityType::Event`).
//! Live query only — NO sync table for calendar (the
//! plan defers that to 6b or later). No Effect rows, no
//! approval: reading the user's own calendar is fact
//! retrieval, not side-effect execution (red line 3).
//!
//! macOS only, cfg-gated; the `eventkit` crate itself
//! does not build on Linux, so the macOS body is
//! compiled ONLY by CI's macOS job — the same standing
//! as the shell commands in D1-1/D1-4/D1-5. The
//! non-macOS stub returns the same never-fake-data 501
//! wording as `UnavailableMailSource`.
//!
//! Authorization: 6a defines no separate authorization
//! route, so the first list/get call requests full
//! access when the status is NotDetermined (the
//! synchronous EventKit API waits up to 30 s, then the
//! prompt may still be showing — the status is re-read
//! either way and that is the authoritative answer,
//! the `eventkit_backend.rs` pattern). Denied /
//! Restricted / WriteOnly fail closed with 403.
//!
//! Privacy: `notes` is the user's own text, returned
//! to the user only, never logged. Attendees and
//! organizer are excluded from the DTO (privacy-minimal
//! 6a surface), as are alarms and recurrence rules.

use serde::{Deserialize, Serialize};

/// One calendar event, as the routes return it.
/// LOCAL DEFINITION: the 6a surface is title / time /
/// calendar / location / notes only — no attendees,
/// organizer, alarms, or recurrence rules. `start_ms`
/// / `end_ms` are epoch milliseconds parsed from
/// EventKit's ISO8601 strings (0 when unparseable —
/// EventKit always emits fractional ISO8601, so 0 is a
/// fallback only).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug)]
pub enum CalendarError {
    Unavailable(String),
    Denied,
    Upstream(String),
}

#[cfg(target_os = "macos")]
use eventkit::error::{EKAuthorizationStatus, EventKitError};
#[cfg(target_os = "macos")]
use eventkit::event::EKEvent;
#[cfg(target_os = "macos")]
use eventkit::event_store::{EKEntityType, EKEventStore};
#[cfg(target_os = "macos")]
use crate::operation_binding::{iso8601_to_ms, ms_to_iso8601_utc};
#[cfg(target_os = "macos")]
use crate::sqlite_store::now_ms;

/// The list window (LOCAL DEFINITION): 7 days back to
/// 90 days ahead, all calendars, start ascending.
/// EventKit expands recurring occurrences inside the
/// range; the 4-year EventKit range limit is far above
/// this window.
#[cfg(target_os = "macos")]
const LIST_BACK_DAYS: i64 = 7;
#[cfg(target_os = "macos")]
const LIST_AHEAD_DAYS: i64 = 90;

#[cfg(target_os = "macos")]
fn map_error(error: EventKitError) -> CalendarError {
    CalendarError::Upstream(error.to_string())
}

#[cfg(target_os = "macos")]
fn open_store() -> Result<EKEventStore, CalendarError> {
    EKEventStore::new().map_err(map_error)
}

#[cfg(target_os = "macos")]
fn ensure_readable() -> Result<(), CalendarError> {
    match EKEventStore::authorization_status(EKEntityType::Event) {
        EKAuthorizationStatus::FullAccess => Ok(()),
        EKAuthorizationStatus::NotDetermined => {
            let store = open_store()?;
            let _ = store.request_full_access_to_events();
            match EKEventStore::authorization_status(EKEntityType::Event) {
                EKAuthorizationStatus::FullAccess => Ok(()),
                _ => Err(CalendarError::Denied),
            }
        }
        _ => Err(CalendarError::Denied),
    }
}

#[cfg(target_os = "macos")]
fn to_event(item: &EKEvent) -> Option<CalendarEvent> {
    Some(CalendarEvent {
        event_id: item.identifier.clone()?,
        title: item.title.clone(),
        start_ms: iso8601_to_ms(&item.start_date),
        end_ms: iso8601_to_ms(&item.end_date),
        all_day: item.all_day,
        calendar_id: item.calendar_identifier.clone(),
        calendar_title: item.calendar.as_ref().map(|c| c.title.clone()),
        location: item.location.clone(),
        notes: item.notes.clone(),
    })
}

#[cfg(target_os = "macos")]
pub fn list_events() -> Result<Vec<CalendarEvent>, CalendarError> {
    ensure_readable()?;
    let store = open_store()?;
    let now = now_ms() as i64;
    let start = ms_to_iso8601_utc(now - LIST_BACK_DAYS * 86_400_000);
    let end = ms_to_iso8601_utc(now + LIST_AHEAD_DAYS * 86_400_000);
    let predicate = store.predicate_for_events(start, end, None);
    let found = store.events_matching(&predicate).map_err(map_error)?;
    let mut events: Vec<CalendarEvent> = found.iter().filter_map(to_event).collect();
    events.sort_by_key(|e| (e.start_ms, e.event_id.clone()));
    Ok(events)
}

#[cfg(target_os = "macos")]
pub fn get_event(event_id: &str) -> Result<Option<CalendarEvent>, CalendarError> {
    ensure_readable()?;
    let store = open_store()?;
    let found = store.event_with_identifier(event_id).map_err(map_error)?;
    Ok(found.as_ref().and_then(to_event))
}

#[cfg(not(target_os = "macos"))]
pub fn list_events() -> Result<Vec<CalendarEvent>, CalendarError> {
    Err(CalendarError::Unavailable(
        "calendar unavailable on this platform".to_string(),
    ))
}

#[cfg(not(target_os = "macos"))]
pub fn get_event(_event_id: &str) -> Result<Option<CalendarEvent>, CalendarError> {
    Err(CalendarError::Unavailable(
        "calendar unavailable on this platform".to_string(),
    ))
}
