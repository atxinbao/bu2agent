//! D1-6b-2 (Q-D1-6b-09/10/11): the calendar create fact
//! record.
//!
//! One `calendar_events` row per accepted create, one
//! `calendar.created` event in the SAME transaction (the
//! `mail_send.rs` `record_sent` pattern: row + event in
//! one tx). The row is the fact source; the event is a
//! redacted audit summary, never the row serialization,
//! so calendar has NO fold arm (the D1-6a `mail.synced`
//! precedent).
//!
//! Privacy (red line 7): the table stores `title` /
//! `location` (user-local fact record, same posture as
//! `sent_messages` storing `subject`) but NEVER the
//! notes body — only `notes_hash`. The `calendar.created`
//! event payload carries ids, hashes, lengths, and
//! counts only (no title/location/notes plaintext);
//! `append_event` whitelists it again.
//!
//! Idempotency (Q-D1-6b-05, the mail precedent):
//! `idempotency_key` is UNIQUE and client-supplied (no
//! key → 400 at the route). Same key + same content
//! (calendar/title/start/end/all_day/location/
//! notes_hash all equal) replays the original row; same
//! key + different content is a 409. The plan's §3.3
//! column list omits the key, but the §3 replay rule
//! needs it stored — the column is added here per the
//! 6b-1 precedent, not silently (see Deviation D-1 in
//! the 6b-2 report).
//!
//! `event_id` is the EventKit identifier itself — the
//! stable system id, so no local `event-N` number is
//! assigned (plan §3.3). A create whose identifier
//! cannot be recovered never reaches this store (the
//! backend answers `Unverified` first; the Effect stays
//! Unknown, never a guessed row).

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::calendar_backend::CreatedCalendarEvent;
use crate::operation_binding::sha256_hex;
use crate::sqlite_store::{append_event, canonical_json, now_ms};

/// The calendar source's Registry id (the `mail-source`
/// precedent in `mail_sync.rs`).
pub const CALENDAR_SOURCE_ID: &str = "calendar-source";

fn err(detail: impl std::fmt::Display) -> String {
    format!("sqlite: {detail}")
}

/// One created calendar event (LOCAL DEFINITION, plan
/// §3.3 + the idempotency-key column the §3 replay rule
/// needs). `event_id` is the EventKit identifier.
/// `location` is stored; `notes` never is — only
/// `notes_hash` (sha256 of the notes text, or of the
/// empty string when no notes were given).
/// `policy_version` is the current Policy version at
/// write time, for the Mandate audit trail (Q-D1-6b-23).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CalendarEventRow {
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

fn calendar_event_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CalendarEventRow> {
    Ok(CalendarEventRow {
        event_id: row.get(0)?,
        source_id: row.get(1)?,
        calendar_id: row.get(2)?,
        title: row.get(3)?,
        start_ms: row.get(4)?,
        end_ms: row.get(5)?,
        all_day: row.get(6)?,
        location: row.get(7)?,
        notes_hash: row.get(8)?,
        idempotency_key: row.get(9)?,
        effect_id: row.get(10)?,
        policy_version: row.get(11)?,
        created_ms: row.get(12)?,
        origin_device_id: row.get(13)?,
    })
}

const CALENDAR_COLS: &str = "event_id, source_id, calendar_id, title, start_ms, end_ms, all_day, location, notes_hash, idempotency_key, effect_id, policy_version, created_ms, origin_device_id";

/// The notes hash: sha256 of the notes text. The notes
/// body itself is never stored and never enters an
/// event; absent notes hash as the empty string, so the
/// column stays NOT NULL and the digest stays stable.
pub fn calendar_notes_hash(notes: Option<&str>) -> String {
    sha256_hex(notes.unwrap_or("").as_bytes())
}

/// The `calendar.created` event payload: ids, hashes,
/// lengths, and flags only. `title_len` is a character
/// count; no title, location, or notes plaintext — the
/// payload is built redacted and `append_event`
/// whitelists it again.
fn created_payload(row: &CalendarEventRow, execution_id: &str) -> String {
    canonical_json(&serde_json::json!({
        "event_id": row.event_id,
        "calendar_id": row.calendar_id,
        "source_id": row.source_id,
        "title_len": row.title.chars().count(),
        "start_ms": row.start_ms,
        "end_ms": row.end_ms,
        "all_day": row.all_day,
        "created_ms": row.created_ms,
        "execution_id": execution_id,
    }))
}

/// One created event by idempotency key (the replay
/// lookup). The key is UNIQUE, so at most one row ever
/// matches.
pub fn find_by_idempotency_key(
    conn: &Connection,
    idempotency_key: &str,
) -> Result<Option<CalendarEventRow>, String> {
    conn.query_row(
        &format!("SELECT {CALENDAR_COLS} FROM calendar_events WHERE idempotency_key = ?1"),
        params![idempotency_key],
        calendar_event_from_row,
    )
    .optional()
    .map_err(err)
}

/// One created event by Effect id (the Evidence
/// readback lookup: the Effect row names the fact row,
/// the fact row names the EventKit identifier).
pub fn find_by_effect_id(
    conn: &Connection,
    effect_id: &str,
) -> Result<Option<CalendarEventRow>, String> {
    conn.query_row(
        &format!("SELECT {CALENDAR_COLS} FROM calendar_events WHERE effect_id = ?1"),
        params![effect_id],
        calendar_event_from_row,
    )
    .optional()
    .map_err(err)
}

/// Record one accepted create in a single transaction:
/// the fact row + one `calendar.created` event. The
/// caller passes the Effect id the row belongs to; the
/// Effect itself is finished by the caller after this
/// returns (row first, then terminal transition — a
/// succeeded Effect must never exist without its fact
/// row).
#[allow(clippy::too_many_arguments)]
pub fn record_calendar_event(
    conn: &mut Connection,
    created: &CreatedCalendarEvent,
    calendar_id: &str,
    title: &str,
    location: Option<&str>,
    notes_hash: &str,
    idempotency_key: &str,
    effect_id: &str,
    execution_id: &str,
    policy_version: &str,
    origin_device_id: &str,
) -> Result<CalendarEventRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let now = now_ms() as i64;
    let row = CalendarEventRow {
        event_id: created.event_id.clone(),
        source_id: CALENDAR_SOURCE_ID.to_string(),
        calendar_id: calendar_id.to_string(),
        title: title.to_string(),
        start_ms: created.start_ms,
        end_ms: created.end_ms,
        all_day: created.all_day,
        location: location.map(str::to_string),
        notes_hash: notes_hash.to_string(),
        idempotency_key: idempotency_key.to_string(),
        effect_id: effect_id.to_string(),
        policy_version: policy_version.to_string(),
        created_ms: now,
        origin_device_id: origin_device_id.to_string(),
    };
    tx.execute(
        &format!(
            "INSERT INTO calendar_events ({CALENDAR_COLS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)"
        ),
        params![
            row.event_id,
            row.source_id,
            row.calendar_id,
            row.title,
            row.start_ms,
            row.end_ms,
            row.all_day,
            row.location,
            row.notes_hash,
            row.idempotency_key,
            row.effect_id,
            row.policy_version,
            row.created_ms,
            row.origin_device_id,
        ],
    )
    .map_err(err)?;
    append_event(
        &tx,
        "calendar",
        &row.event_id,
        "calendar.created",
        &created_payload(&row, execution_id),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(row)
}

/// All created rows for the calendar source, oldest first.
pub fn list_calendar_events(conn: &Connection) -> Result<Vec<CalendarEventRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {CALENDAR_COLS} FROM calendar_events WHERE source_id = ?1 ORDER BY rowid ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![CALENDAR_SOURCE_ID], calendar_event_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// One created row by EventKit identifier.
pub fn get_calendar_event(
    conn: &Connection,
    event_id: &str,
) -> Result<Option<CalendarEventRow>, String> {
    conn.query_row(
        &format!("SELECT {CALENDAR_COLS} FROM calendar_events WHERE event_id = ?1"),
        params![event_id],
        calendar_event_from_row,
    )
    .optional()
    .map_err(err)
}
