//! D1-6a (Q-D1-64): mail incremental sync into SQLite.
//!
//! Batch 1 (Task 22) is live query only — no cache, no
//! cursor, no fact record. This module adds the fact
//! record: one `mail_messages` row per message, one
//! `mail.synced` event per new row, and the mail-source
//! Registry cursor advancing in the SAME transaction
//! (D1-3 cursor discipline).
//!
//! The sync is split in two phases, and the split is
//! load-bearing:
//!
//! - `fetch_window` reads the source (threads, then one
//!   `message()` per id) with NO SQLite lock held — the
//!   AppleScript source makes one `osascript` call per
//!   message, and holding the database lock across that
//!   would stall every other SQLite-mode request.
//! - `record_sync` does all writes in ONE transaction:
//!   row + event per new message, then the cursor.
//!
//! The 50-message window is the source's contract
//! (batch-1 `INBOX_WINDOW`): the source's list IS the
//! first 50 of the inbox, and the sync trusts it —
//! `fetch_window` asks for all threads the source
//! reports (limit 200, above any window) and syncs
//! exactly those. Thread attribution reuses batch-1
//! `thread_id_for` / `normalize_subject`.
//!
//! Idempotency: `(source_id, native_id)` is UNIQUE —
//! a second sync skips every row already present
//! (counted, not re-inserted), and a newly arrived
//! message adds only its row. Local ids are
//! `mail-N`, assigned in first-seen scan order from
//! max-suffix + 1 inside the write transaction. There
//! is exactly one writer (the sync route) and the write
//! phase is serialized by the caller's SQLite lock, so
//! no counter lives in `AppState` (unlike memories,
//! whose saves come from many handlers). A rebuild
//! reassigns the same local ids because the scan order
//! is deterministic and the counter restarts at 1 on
//! an empty table — the `(source_id, native_id)` key
//! is the remap key the plan names.
//!
//! Privacy (red line 7): the `mail_messages` table
//! stores `subject`/`sender` (user-local fact record,
//! plan §7 item 5) but NEVER the body — only
//! `body_hash`. The `mail.synced` event payload carries
//! ids, hashes, lengths, and counts only (no subject,
//! no sender, no body); `append_event` whitelists it
//! again. There is NO fold arm for mail: the event
//! payload is a redacted summary, not the row
//! serialization, so mail events are audit-only (the
//! `file.changed` precedent).
//!
//! No Effect rows, no approval, no Outbox rows: sync
//! is fact recording (red line 3), not side-effect
//! execution. No auto-trigger: the route is manual
//! only; automatic sync waits for A1 timers.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::mail_source::{normalize_subject, thread_id_for, MailError, MailSource, MessageView};
use crate::operation_binding::{iso8601_to_ms, sha256_hex};
use crate::sqlite_store::{
    append_event, canonical_json, id_suffix, now_ms, update_source_cursor_tx,
};

/// The Registry row this sync writes against (D1-4
/// local definition: the mail source registers as
/// `mail-source`).
pub const MAIL_SOURCE_ID: &str = "mail-source";

fn err(detail: impl std::fmt::Display) -> String {
    format!("sqlite: {detail}")
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MailMessageRow {
    pub message_id: String,
    pub source_id: String,
    pub native_id: String,
    pub thread_id: String,
    pub subject: String,
    pub sender: String,
    pub date_ms: i64,
    pub body_hash: String,
    pub created_ms: u64,
    pub origin_device_id: String,
    /// D1-6b-3 tombstone: `true` once the message was
    /// moved to Trash. The text stays on the row (red
    /// line 9: deletion is tombstone-only). Rows
    /// backfilled before 6b-3 read NULL, which maps to
    /// `false` here.
    pub deleted: bool,
    /// D1-6b-3: advances on tombstone (the sync time on
    /// insert). Backfilled rows read NULL, which maps
    /// to 0 here.
    pub updated_ms: i64,
}

fn mail_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MailMessageRow> {
    Ok(MailMessageRow {
        message_id: row.get(0)?,
        source_id: row.get(1)?,
        native_id: row.get(2)?,
        thread_id: row.get(3)?,
        subject: row.get(4)?,
        sender: row.get(5)?,
        date_ms: row.get(6)?,
        body_hash: row.get(7)?,
        created_ms: row.get(8)?,
        origin_device_id: row.get(9)?,
        deleted: row.get::<_, Option<i64>>(10)?.unwrap_or(0) != 0,
        updated_ms: row.get::<_, Option<i64>>(11)?.unwrap_or(0),
    })
}

const MAIL_COLS: &str =
    "message_id, source_id, native_id, thread_id, subject, sender, date_ms, body_hash, created_ms, origin_device_id, deleted, updated_ms";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MailSyncResult {
    pub synced_count: usize,
    pub skipped_count: usize,
    pub total_count: usize,
    pub cursor: String,
}

pub enum MailSyncError {
    Source(MailError),
    Store(String),
}

/// Fetch the inbox window via the existing read
/// surface: `threads()` enumerates the window, then one
/// `message()` per id fetches the body (needed for the
/// `body_hash`). A message that leaves the window
/// between the two calls is a benign skip (`NotFound`);
/// any other source failure aborts the sync — a partial
/// fetch must not be recorded as a successful sync.
pub fn fetch_window(source: &dyn MailSource) -> Result<Vec<MessageView>, MailError> {
    let threads = source.threads(200, 0)?;
    let mut messages = Vec::new();
    for thread in threads {
        for native_id in thread.message_ids {
            match source.message(&native_id) {
                Ok(message) => messages.push(message),
                Err(MailError::NotFound(_)) => continue,
                Err(other) => return Err(other),
            }
        }
    }
    Ok(messages)
}

/// The `mail.synced` event payload: ids, hashes,
/// lengths, and counts only. `subject_len` is a
/// character count; `sender_hash` is the sha256 of the
/// sender address. No subject, no sender, no body —
/// the payload is built redacted and `append_event`
/// whitelists it again.
fn synced_payload(row: &MailMessageRow) -> String {
    canonical_json(&serde_json::json!({
        "message_id": row.message_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "thread_id": row.thread_id,
        "body_hash": row.body_hash,
        "subject_len": row.subject.chars().count(),
        "sender_hash": sha256_hex(row.sender.as_bytes()),
        "date_ms": row.date_ms,
        "created_ms": row.created_ms,
    }))
}

/// The mail cursor: the content hash of the synced set
/// for this source — canonical JSON of the sorted
/// native ids, sha256 hex (the D1-4 file-cursor shape).
/// `""` means "never synced"; after any sync the
/// cursor is a 64-hex sha256, and it changes exactly
/// when the synced set changes.
fn mail_cursor_tx(tx: &Transaction, source_id: &str) -> Result<String, String> {
    let mut stmt = tx
        .prepare("SELECT native_id FROM mail_messages WHERE source_id = ?1 ORDER BY native_id ASC")
        .map_err(err)?;
    let mut ids = stmt
        .query_map(params![source_id], |row| row.get::<_, String>(0))
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    ids.sort();
    let json = serde_json::to_string(&ids).map_err(|e| e.to_string())?;
    Ok(sha256_hex(json.as_bytes()))
}

fn next_mail_number_tx(tx: &Transaction) -> Result<u64, String> {
    let mut stmt = tx
        .prepare("SELECT message_id FROM mail_messages")
        .map_err(err)?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    let mut next: u64 = 1;
    for id in ids {
        let suffix = id_suffix(&id);
        if suffix >= 0 {
            next = next.max((suffix as u64) + 1);
        }
    }
    Ok(next)
}

/// Record one sync pass in a single transaction: one
/// row + one `mail.synced` event per new message, then
/// the mail-source cursor advances. Rows already
/// present (same `(source_id, native_id)`) are skipped
/// and counted, never re-inserted.
pub fn record_sync(
    conn: &mut Connection,
    messages: &[MessageView],
    source_id: &str,
    origin_device_id: &str,
) -> Result<MailSyncResult, String> {
    let tx = conn.transaction().map_err(err)?;
    let mut next = next_mail_number_tx(&tx)?;
    let mut synced = 0usize;
    let mut skipped = 0usize;
    let now = now_ms();
    for message in messages {
        // D1-6b-3: this exists check is the anti-resurrect
        // defense — a tombstoned row is still in the
        // table, so a re-sync skips it instead of
        // re-inserting it. Adding a `deleted = 0`
        // condition here would do the opposite: the
        // tombstoned row would look absent and the
        // INSERT would hit the (source_id, native_id)
        // UNIQUE key. The tombstone lives in the table
        // precisely so the sync can see it and skip.
        let exists: bool = tx
            .query_row(
                "SELECT 1 FROM mail_messages WHERE source_id = ?1 AND native_id = ?2",
                params![source_id, message.message_id],
                |_| Ok(true),
            )
            .optional()
            .map_err(err)?
            .is_some();
        if exists {
            skipped += 1;
            continue;
        }
        let id = format!("mail-{next}");
        next += 1;
        let row = MailMessageRow {
            message_id: id.clone(),
            source_id: source_id.to_string(),
            native_id: message.message_id.clone(),
            thread_id: thread_id_for(&normalize_subject(&message.subject)),
            subject: message.subject.clone(),
            sender: message.from.clone(),
            // Mail display dates are locale strings, not
            // timestamps: RFC3339 parses, anything else
            // lands at 0 ("unknown date", never 1970).
            date_ms: iso8601_to_ms(&message.date),
            body_hash: sha256_hex(message.body_text.as_bytes()),
            created_ms: now,
            origin_device_id: origin_device_id.to_string(),
            deleted: false,
            updated_ms: now as i64,
        };
        tx.execute(
            &format!(
                "INSERT INTO mail_messages ({MAIL_COLS})
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
            ),
            params![
                row.message_id,
                row.source_id,
                row.native_id,
                row.thread_id,
                row.subject,
                row.sender,
                row.date_ms,
                row.body_hash,
                row.created_ms as i64,
                row.origin_device_id,
                0i64,
                row.updated_ms,
            ],
        )
        .map_err(err)?;
        append_event(
            &tx,
            "mail",
            &id,
            "mail.synced",
            &synced_payload(&row),
            origin_device_id,
            now as i64,
            1,
        )?;
        synced += 1;
    }
    let cursor = mail_cursor_tx(&tx, source_id)?;
    update_source_cursor_tx(&tx, source_id, &cursor)?;
    let total: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM mail_messages WHERE source_id = ?1",
            params![source_id],
            |row| row.get(0),
        )
        .map_err(err)?;
    tx.commit().map_err(err)?;
    Ok(MailSyncResult {
        synced_count: synced,
        skipped_count: skipped,
        total_count: total as usize,
        cursor,
    })
}

/// All synced rows for one source, oldest first.
/// D1-6b-3 (approved plan §3 / Q-D1-6b-14): the list
/// INCLUDES tombstoned rows, with `deleted = true`
/// visible — the tombstone state must be verifiable
/// over HTTP (red line 5: no reader without a write
/// side). `mail_from_row` maps a NULL `deleted`
/// (pre-6b-3 rows, backfilled NULL) to `false`, so
/// legacy rows stay visible as live rows.
pub fn list_mail_messages(
    conn: &Connection,
    source_id: &str,
) -> Result<Vec<MailMessageRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {MAIL_COLS} FROM mail_messages WHERE source_id = ?1 ORDER BY rowid ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![source_id], mail_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// One synced row by local id, any source. Tombstoned
/// rows are still returned (the goals-detail precedent):
/// the delete route and the reader both need to see the
/// tombstone state.
pub fn get_mail_message(
    conn: &Connection,
    message_id: &str,
) -> Result<Option<MailMessageRow>, String> {
    conn.query_row(
        &format!("SELECT {MAIL_COLS} FROM mail_messages WHERE message_id = ?1"),
        params![message_id],
        mail_from_row,
    )
    .optional()
    .map_err(err)
}

/// One synced row by native id, for one source.
pub fn find_by_native_id(
    conn: &Connection,
    source_id: &str,
    native_id: &str,
) -> Result<Option<MailMessageRow>, String> {
    conn.query_row(
        &format!(
            "SELECT {MAIL_COLS} FROM mail_messages WHERE source_id = ?1 AND native_id = ?2"
        ),
        params![source_id, native_id],
        mail_from_row,
    )
    .optional()
    .map_err(err)
}

/// Resolve a delete-route id to its (row, native id).
/// Local `mail-N` first, then a row by native id, then
/// a bare numeric native id with no row (a never-synced
/// message, Q-D1-6b-15). Anything else is unknown
/// (`None` — the caller answers 404, never a guess).
pub fn resolve_delete_target(
    conn: &Connection,
    message_id: &str,
) -> Result<Option<(Option<MailMessageRow>, String)>, String> {
    if let Some(row) = get_mail_message(conn, message_id)? {
        let native_id = row.native_id.clone();
        return Ok(Some((Some(row), native_id)));
    }
    if let Some(row) = find_by_native_id(conn, MAIL_SOURCE_ID, message_id)? {
        let native_id = row.native_id.clone();
        return Ok(Some((Some(row), native_id)));
    }
    if message_id.parse::<i64>().is_ok() {
        return Ok(Some((None, message_id.to_string())));
    }
    Ok(None)
}

/// The `mail.deleted` event payload: ids and timestamps
/// only. No subject, no sender, no body — the payload
/// is built redacted and `append_event` whitelists it
/// again. Every field here is already on the redact
/// allowlist, so no addition was needed (plan §4.3).
fn deleted_payload(row: &MailMessageRow, execution_id: &str, deleted_ms: i64) -> String {
    canonical_json(&serde_json::json!({
        "message_id": row.message_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "created_ms": row.created_ms,
        "updated_ms": deleted_ms,
        "execution_id": execution_id,
    }))
}

/// The `mail.deleted` event payload for a never-synced
/// delete (no row, Q-D1-6b-15): the aggregate is the
/// native id itself, and there is no local `message_id`
/// to name — no empty fact row is created for it.
fn deleted_payload_no_row(
    native_id: &str,
    execution_id: &str,
    deleted_ms: i64,
) -> String {
    // No fact row exists (Q-D1-6b-15), so there is no
    // local `mail-N` and no `updated_ms`; the native id
    // doubles as the aggregate / message id, matching
    // the `aggregate_id` passed to `append_event`.
    canonical_json(&serde_json::json!({
        "message_id": native_id,
        "source_id": MAIL_SOURCE_ID,
        "native_id": native_id,
        "created_ms": deleted_ms,
        "execution_id": execution_id,
    }))
}

/// Outcome of a tombstone write.
pub enum TombstoneOutcome {
    /// Newly tombstoned (the returned row is post-state).
    Tombstoned(MailMessageRow),
    /// Already tombstoned (the returned row is current
    /// state; no second event was written).
    AlreadyTombstoned(MailMessageRow),
}

/// Tombstone one synced row in a single transaction:
/// `deleted = 1`, `updated_ms` advances, text stays on
/// the row, plus one `mail.deleted` event. A re-delete
/// is a no-op returning the current row (no second
/// event), like re-archive / re-disconnect and the D1-5
/// memory delete. Unknown id returns `Ok(None)`.
pub fn tombstone_mail_message(
    conn: &mut Connection,
    message_id: &str,
    execution_id: &str,
    origin_device_id: &str,
) -> Result<Option<TombstoneOutcome>, String> {
    let tx = conn.transaction().map_err(err)?;
    let Some(row) = get_mail_message(&tx, message_id)? else {
        return Ok(None);
    };
    if row.deleted {
        tx.commit().map_err(err)?;
        return Ok(Some(TombstoneOutcome::AlreadyTombstoned(row)));
    }
    let now = now_ms() as i64;
    tx.execute(
        "UPDATE mail_messages SET deleted = 1, updated_ms = ?1 WHERE message_id = ?2",
        params![now, message_id],
    )
    .map_err(err)?;
    let mut row = row;
    row.deleted = true;
    row.updated_ms = now;
    append_event(
        &tx,
        "mail",
        &row.message_id,
        "mail.deleted",
        &deleted_payload(&row, execution_id, now),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(Some(TombstoneOutcome::Tombstoned(row)))
}

/// Record a never-synced delete (no row, Q-D1-6b-15):
/// the `mail.deleted` event alone, aggregate id = the
/// native id. The Effect + this event are the
/// credential; no empty fact row is created.
pub fn record_deleted_no_row(
    conn: &mut Connection,
    native_id: &str,
    execution_id: &str,
    origin_device_id: &str,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    let now = now_ms() as i64;
    append_event(
        &tx,
        "mail",
        native_id,
        "mail.deleted",
        &deleted_payload_no_row(native_id, execution_id, now),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(())
}

/// Next free mail number (max suffix + 1, or 1). The
/// sync computes this inside its own transaction; this
/// is the read-only seam for tests.
pub fn next_mail_number(conn: &Connection) -> Result<u64, String> {
    let mut stmt = conn
        .prepare("SELECT message_id FROM mail_messages")
        .map_err(err)?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    let mut next: u64 = 1;
    for id in ids {
        let suffix = id_suffix(&id);
        if suffix >= 0 {
            next = next.max((suffix as u64) + 1);
        }
    }
    Ok(next)
}
