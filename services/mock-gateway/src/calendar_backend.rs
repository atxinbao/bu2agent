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

use std::sync::Arc;

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
    /// D1-6b-2 (Q-D1-6b-09): the caller-named calendar
    /// does not exist. A write never falls back to a
    /// default calendar — the route answers 404.
    NotFound,
    /// D1-6b-2: the save call succeeded but the saved
    /// event could not be re-read by identifier, so the
    /// fact row cannot be keyed. The outcome must stay
    /// Unknown (P0-R honesty) — never guessed from the
    /// requested fields.
    Unverified,
}

/// D1-6b-2: what a create returns once the saved event
/// is re-read by identifier. The gateway needs the
/// identifier to key the `calendar_events` fact row,
/// and `save_event` itself returns `()` (verified
/// against the eventkit 0.4.0 source:
/// `event_store.rs:711`), so the identifier comes from
/// the re-read, never from the save call.
#[derive(Debug, Clone, PartialEq)]
pub struct CreatedCalendarEvent {
    pub event_id: String,
    pub title: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub all_day: bool,
    pub calendar_id: Option<String>,
    pub location: Option<String>,
    pub notes: Option<String>,
}

/// D1-6b-2: create-capable calendar backend. Reads are
/// the D1-6a free functions; writes go through this
/// trait so tests can substitute a fake (the EventKit
/// create path is macOS-only and untestable in the VM).
/// Production behaviour is identical: the EventKit impl
/// delegates reads to the existing free functions.
pub trait CalendarBackend: Send + Sync {
    fn list_events(&self) -> Result<Vec<CalendarEvent>, CalendarError>;
    fn get_event(&self, event_id: &str) -> Result<Option<CalendarEvent>, CalendarError>;
    fn calendar_exists(&self, calendar_id: &str) -> Result<bool, CalendarError>;
    #[allow(clippy::too_many_arguments)]
    fn create_event(
        &self,
        calendar_id: &str,
        title: &str,
        start_ms: i64,
        end_ms: i64,
        all_day: bool,
        location: Option<&str>,
        notes: Option<&str>,
    ) -> Result<CreatedCalendarEvent, CalendarError>;
}

#[cfg(target_os = "macos")]
use eventkit::error::{EKAuthorizationStatus, EventKitError};
#[cfg(target_os = "macos")]
use eventkit::event::EKEvent;
#[cfg(target_os = "macos")]
use eventkit::event_store::{EKEntityType, EKEventStore, EKSpan};
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

/// D1-6b-2: the EventKit backend. Reads delegate to the
/// D1-6a free functions, so the read path is unchanged;
/// `create_event` is the new write path.
#[cfg(target_os = "macos")]
pub struct EventKitCalendarBackend;

#[cfg(target_os = "macos")]
impl CalendarBackend for EventKitCalendarBackend {
    fn list_events(&self) -> Result<Vec<CalendarEvent>, CalendarError> {
        list_events()
    }

    fn get_event(&self, event_id: &str) -> Result<Option<CalendarEvent>, CalendarError> {
        get_event(event_id)
    }

    fn calendar_exists(&self, calendar_id: &str) -> Result<bool, CalendarError> {
        ensure_readable()?;
        let store = open_store()?;
        let found = store
            .calendar_with_identifier(calendar_id)
            .map_err(map_error)?;
        Ok(found.is_some())
    }

    fn create_event(
        &self,
        calendar_id: &str,
        title: &str,
        start_ms: i64,
        end_ms: i64,
        all_day: bool,
        location: Option<&str>,
        notes: Option<&str>,
    ) -> Result<CreatedCalendarEvent, CalendarError> {
        // Full access is requested on first use
        // (ensure_readable); per framework semantics that
        // should cover writes too, but no write call
        // exists in-tree to prove it — window validation
        // is the proof (the D1-6a investigation's
        // standing caveat, carried into 6b-2).
        ensure_readable()?;
        let store = open_store()?;
        // Validate the calendar first: an unknown id is a
        // 404, never a silent fall into the default
        // calendar (Q-D1-6b-09).
        let found = store
            .calendar_with_identifier(calendar_id)
            .map_err(map_error)?;
        if found.is_none() {
            return Err(CalendarError::NotFound);
        }
        let mut event = EKEvent::new(
            title,
            ms_to_iso8601_utc(start_ms),
            ms_to_iso8601_utc(end_ms),
        )
        .with_calendar_identifier(calendar_id)
        .with_all_day(all_day);
        event.location = location.map(str::to_string);
        event.notes = notes.map(str::to_string);
        store
            .save_event(&event, EKSpan::ThisEvent, true)
            .map_err(map_error)?;
        // `save_event` returns `()` — it does not hand
        // back the saved event or its identifier
        // (eventkit 0.4.0 `event_store.rs:711`). Recover
        // the identifier by re-listing a tight window
        // around the event (±1 s slack, so the event
        // overlaps the predicate under either matching
        // semantics) and matching the exact fields the
        // caller asked for. Exactly one match keys the
        // fact row; zero or several means the save cannot
        // be keyed, and the outcome must stay Unknown —
        // never guessed (P0-R honesty).
        let start = ms_to_iso8601_utc(start_ms - 1_000);
        let end = ms_to_iso8601_utc(end_ms + 1_000);
        let predicate = store.predicate_for_events(start, end, None);
        // The save already landed at this point: a
        // re-list failure leaves the outcome ambiguous
        // (Unverified → Unknown), never Failed.
        let found = store
            .events_matching(&predicate)
            .map_err(|_| CalendarError::Unverified)?;
        let mut matches: Vec<CalendarEvent> = found
            .iter()
            .filter_map(to_event)
            .filter(|e| {
                e.calendar_id.as_deref() == Some(calendar_id)
                    && e.title == title
                    && e.start_ms == start_ms
                    && e.end_ms == end_ms
            })
            .collect();
        if matches.len() != 1 {
            return Err(CalendarError::Unverified);
        }
        let event = matches.remove(0);
        Ok(CreatedCalendarEvent {
            event_id: event.event_id,
            title: event.title,
            start_ms: event.start_ms,
            end_ms: event.end_ms,
            all_day: event.all_day,
            calendar_id: event.calendar_id,
            location: event.location,
            notes: event.notes,
        })
    }
}

/// D1-6b-2: the non-macOS backend. Reads delegate to
/// the D1-6a free functions (which answer the same
/// never-fake-data 501); writes have no platform
/// implementation and answer 501 the same way.
#[cfg(not(target_os = "macos"))]
pub struct UnavailableCalendarBackend;

#[cfg(not(target_os = "macos"))]
impl CalendarBackend for UnavailableCalendarBackend {
    fn list_events(&self) -> Result<Vec<CalendarEvent>, CalendarError> {
        list_events()
    }

    fn get_event(&self, event_id: &str) -> Result<Option<CalendarEvent>, CalendarError> {
        get_event(event_id)
    }

    fn calendar_exists(&self, _calendar_id: &str) -> Result<bool, CalendarError> {
        Err(CalendarError::Unavailable(
            "calendar unavailable on this platform".to_string(),
        ))
    }

    fn create_event(
        &self,
        _calendar_id: &str,
        _title: &str,
        _start_ms: i64,
        _end_ms: i64,
        _all_day: bool,
        _location: Option<&str>,
        _notes: Option<&str>,
    ) -> Result<CreatedCalendarEvent, CalendarError> {
        Err(CalendarError::Unavailable(
            "calendar unavailable on this platform".to_string(),
        ))
    }
}

/// D1-6b-2: the production backend for `AppState` —
/// EventKit on macOS, the 501 stub everywhere else.
/// Tests override via `with_calendar_backend` (the
/// `with_mail_source` precedent).
pub fn default_calendar_backend() -> Arc<dyn CalendarBackend> {
    #[cfg(target_os = "macos")]
    {
        Arc::new(EventKitCalendarBackend)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Arc::new(UnavailableCalendarBackend)
    }
}
