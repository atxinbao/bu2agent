//! iMessage sync cache: record `imessage.synced` rows from
//! a live source read into SQLite (`imessage_threads`),
//! mirroring `mail_sync.rs` (Q-MCL-04: threads-only; the
//! mail read-batch 1 patterns are the model).
//!
//! What a row records is the thread identity only —
//! native id, service, group flag, participant count +
//! hash, sync timestamps. `last_message_ms` and
//! `unread_count` have no message data source under
//! AppleScript sync and stay NULL (never fabricated).
//! The payload recorded per row is the redacted event
//! payload (ids / hashes / counts / timestamps only);
//! handles and names never reach the event log.
//!
//! A sync records each new thread in the same
//! transaction as its `imessage.synced` event and the
//! source cursor, so a restart neither replays consumed
//! threads nor loses the cursor. A row that was deleted
//! (tombstone) is never resurrected by a later sync:
//! the exists check selects by `(source_id, native_id)`
//! with NO `deleted = 0` filter (the mail_sync.rs
//! anti-resurrect precedent). Disconnect is a flag flip
//! only and does not affect reads (6b Q-24).

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::imessage_source::IMessageThreadView;
use crate::operation_binding::sha256_hex;
use crate::sqlite_store::{append_event, update_source_cursor_tx};

pub const IMESSAGE_SOURCE_ID: &str = "imessage-source";

/// One `imessage_threads` row (serde used by the list/
/// get routes and the desktop/TS clients).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IMessageThreadRow {
    pub thread_id: String,
    pub source_id: String,
    pub native_id: String,
    pub service: String,
    pub is_group: bool,
    pub participant_count: i64,
    pub participant_hash: String,
    /// No message data source under AppleScript sync:
    /// always NULL (never fabricated).
    pub last_message_ms: Option<i64>,
    /// No message data source under AppleScript sync:
    /// always NULL (never fabricated).
    pub unread_count: Option<i64>,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub deleted: bool,
    pub origin_device_id: String,
}

pub const IMESSAGE_THREAD_COLS: &str = "thread_id, source_id, native_id, service, is_group, \
    participant_count, participant_hash, last_message_ms, unread_count, created_ms, \
    updated_ms, deleted, origin_device_id";

fn imessage_thread_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<IMessageThreadRow> {
    Ok(IMessageThreadRow {
        thread_id: row.get(0)?,
        source_id: row.get(1)?,
        native_id: row.get(2)?,
        service: row.get(3)?,
        is_group: row.get(4)?,
        participant_count: row.get(5)?,
        participant_hash: row.get(6)?,
        last_message_ms: row.get(7)?,
        unread_count: row.get(8)?,
        created_ms: row.get(9)?,
        updated_ms: row.get(10)?,
        deleted: row.get(11)?,
        origin_device_id: row.get(12)?,
    })
}

/// List synced threads (tombstones included: 6b-3 Q-14
/// — tombstones are visible, never hidden by default).
/// `limit` is clamped by the caller (the routes pass
/// `clamp_limit`); `offset` is a plain row offset.
pub fn list_imessage_threads(
    conn: &Connection,
    source_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<IMessageThreadRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {IMESSAGE_THREAD_COLS} FROM imessage_threads \
             WHERE source_id = ?1 ORDER BY rowid ASC LIMIT ?2 OFFSET ?3"
        ))
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![source_id, limit as i64, offset as i64],
            imessage_thread_from_row,
        )
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

/// Get one synced thread by its local `thread-N` id.
/// Tombstones are returned (a deleted thread is a fact,
/// not an unknown id).
pub fn get_imessage_thread(
    conn: &Connection,
    thread_id: &str,
) -> Result<Option<IMessageThreadRow>, String> {
    conn.query_row(
        &format!("SELECT {IMESSAGE_THREAD_COLS} FROM imessage_threads WHERE thread_id = ?1"),
        params![thread_id],
        imessage_thread_from_row,
    )
    .optional()
    .map_err(|e| e.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IMessageSyncResult {
    pub synced_count: usize,
    pub skipped_count: usize,
    pub total_count: usize,
    pub cursor: String,
}

/// Read the source's stored cursor (empty before the
/// first sync). Used by the sync route's response and
/// by tests asserting the cursor advances.
pub fn get_source_cursor(conn: &Connection, source_id: &str) -> Result<String, String> {
    conn.query_row(
        "SELECT cursor FROM sources WHERE source_id = ?1",
        params![source_id],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(|e| e.to_string())
    .map(|value| value.unwrap_or_default())
}

/// The redacted event payload for one synced thread:
/// ids / hashes / counts / timestamps only. No handle,
/// no name, no message field exists (the allowlist in
/// `redact_event_payload` must carry `service`,
/// `is_group`, `participant_count`, `participant_hash`;
/// anything else the payload names is dropped there).
fn synced_payload(row: &IMessageThreadRow) -> String {
    let payload = serde_json::json!({
        "thread_id": row.thread_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "service": row.service,
        "is_group": row.is_group,
        "participant_count": row.participant_count,
        "participant_hash": row.participant_hash,
        "created_ms": row.created_ms,
        "updated_ms": row.updated_ms,
    });
    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
}

/// Insert one new synced thread row + its
/// `imessage.synced` event in the caller's transaction.
/// The caller holds the same transaction for the cursor
/// update and the total count, so one sync is atomic.
/// Returns `true` when the row was inserted, `false`
/// when the thread already exists (including a
/// tombstone: the exists check has NO `deleted = 0`
/// filter, so a deleted thread is never resurrected by
/// a later sync — the mail_sync.rs precedent).
fn insert_thread_if_absent(
    tx: &Transaction<'_>,
    view: &IMessageThreadView,
    source_id: &str,
    origin_device_id: &str,
    now_ms: i64,
) -> Result<bool, String> {
    let exists = tx
        .query_row(
            "SELECT thread_id FROM imessage_threads WHERE source_id = ?1 AND native_id = ?2",
            params![source_id, view.native_id],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    if exists.is_some() {
        return Ok(false);
    }
    let next: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM imessage_threads WHERE source_id = ?1",
            params![source_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())?;
    let thread_id = format!("thread-{}", next + 1);
    let row = IMessageThreadRow {
        thread_id,
        source_id: source_id.to_string(),
        native_id: view.native_id.clone(),
        service: view.service.clone(),
        is_group: view.is_group,
        participant_count: view.participant_count as i64,
        participant_hash: view.participant_hash.clone(),
        last_message_ms: None,
        unread_count: None,
        created_ms: now_ms,
        updated_ms: now_ms,
        deleted: false,
        origin_device_id: origin_device_id.to_string(),
    };
    tx.execute(
        "INSERT INTO imessage_threads (thread_id, source_id, native_id, service, is_group, \
         participant_count, participant_hash, last_message_ms, unread_count, created_ms, \
         updated_ms, deleted, origin_device_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, ?8, 0, ?9)",
        params![
            row.thread_id,
            row.source_id,
            row.native_id,
            row.service,
            row.is_group,
            row.participant_count,
            row.participant_hash,
            now_ms,
            row.origin_device_id,
        ],
    )
    .map_err(|e| e.to_string())?;
    append_event(
        tx,
        "imessage",
        &row.thread_id,
        "imessage.synced",
        &synced_payload(&row),
        origin_device_id,
        now_ms,
        crate::sqlite_store::DB_SCHEMA_VERSION,
    )?;
    Ok(true)
}

/// Record one sync into the `imessage_threads` table
/// from the live source's enumeration, in a single
/// transaction: new rows + their `imessage.synced`
/// events + the cursor advance. The cursor is the sha256
/// of the sorted native ids (the mail_sync.rs pattern),
/// so it advances exactly when the thread set changes.
/// Sync is idempotent: threads already in the table are
/// skipped, never re-inserted, and tombstones are never
/// resurrected.
pub fn record_sync(
    conn: &mut Connection,
    threads: &[IMessageThreadView],
    source_id: &str,
    origin_device_id: &str,
) -> Result<IMessageSyncResult, String> {
    let now_ms = crate::sqlite_store::now_ms() as i64;
    let mut native_ids: Vec<&str> = threads.iter().map(|t| t.native_id.as_str()).collect();
    native_ids.sort();
    let cursor = sha256_hex(native_ids.join(",").as_bytes());
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let mut synced_count = 0usize;
    let mut skipped_count = 0usize;
    for thread in threads {
        if insert_thread_if_absent(&tx, thread, source_id, origin_device_id, now_ms)? {
            synced_count += 1;
        } else {
            skipped_count += 1;
        }
    }
    let cursor_changed = update_source_cursor_tx(&tx, source_id, &cursor)?;
    let total_count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM imessage_threads WHERE source_id = ?1",
            params![source_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())?;
    let _ = cursor_changed;
    Ok(IMessageSyncResult {
        synced_count,
        skipped_count,
        total_count: total_count as usize,
        cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synced_payload_names_no_handle_and_no_message_field() {
        // The event payload must be ids / hashes / counts /
        // timestamps only. No handle, no name, and no
        // message field can exist (MCL-1 has no message
        // rows at all).
        let row = IMessageThreadRow {
            thread_id: "thread-1".to_string(),
            source_id: "imessage-source".to_string(),
            native_id: "chat-guid".to_string(),
            service: "iMessage".to_string(),
            is_group: false,
            participant_count: 1,
            participant_hash: "aa".repeat(32),
            last_message_ms: None,
            unread_count: None,
            created_ms: 1_000,
            updated_ms: 1_000,
            deleted: false,
            origin_device_id: "dev-1".to_string(),
        };
        let payload = synced_payload(&row);
        assert!(payload.contains("\"thread_id\":\"thread-1\""));
        assert!(payload.contains("\"participant_hash\""));
        assert!(payload.contains("\"service\":\"iMessage\""));
        assert!(!payload.contains("message_id"), "no message field");
        assert!(!payload.contains("body_hash"), "no message field");
        assert!(!payload.contains("sender_hash"), "no message field");
        assert!(!payload.contains("attachment_"), "no message field");
        assert!(
            !payload.contains("last_message_ms"),
            "NULL fields are omitted from the payload"
        );
        assert!(
            !payload.contains("unread_count"),
            "NULL fields are omitted from the payload"
        );
    }
}
