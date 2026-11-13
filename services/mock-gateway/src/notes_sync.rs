//! D1-6b-4 (Q-D1-6b-16–21): Notes sync into SQLite +
//! the Notes fact record.
//!
//! The sync is split in two phases, and the split is
//! load-bearing (the `mail_sync.rs` precedent):
//!
//! - The route reads the source (one `osascript` call
//!   for the whole folder dump) with NO SQLite lock
//!   held.
//! - `record_sync` does all writes in ONE transaction:
//!   row + event per new note, then the notes-source
//!   cursor.
//!
//! Idempotency: `(source_id, native_id)` is UNIQUE — a
//! second sync skips every row already present
//! (counted, not re-inserted). Local ids are `note-N`,
//! assigned in first-seen scan order from max-suffix +
//! 1 inside the write transaction. There is exactly one
//! writer (the sync route) and the write phase is
//! serialized by the caller's SQLite lock, so no counter
//! lives in `AppState` (the mail precedent).
//!
//! Tombstones (Q-D1-6b-21, the mail delete precedent):
//! a tombstoned row stays in the table so a re-sync
//! skips it instead of resurrecting it — the exists
//! check has NO `deleted = 0` filter, for the same
//! reason as mail's (adding one would make the
//! tombstoned row look absent and the INSERT would hit
//! the `(source_id, native_id)` UNIQUE key).
//!
//! Privacy (red line 7): the `notes` table stores
//! `folder` / `name` (user-local fact record, the
//! `sent_messages` storing `subject` precedent) but
//! NEVER the body — only `body_hash` (sha256 of the
//! re-read `plaintext`, Q-D1-6b-18). The `note.*` event
//! payloads carry ids, hashes, lengths, and counts
//! only (no folder/name/body plaintext — `folder_hash`
//! and `name_len` instead); `append_event` whitelists
//! them again. There is NO fold arm for notes: the
//! event payloads are redacted summaries, not row
//! serializations, so note events are audit-only (the
//! D1-6a `mail.synced` precedent).
//!
//! No Effect rows, no approval, no Outbox rows for
//! sync: sync is fact recording (red line 3), not
//! side-effect execution. No auto-trigger: the route is
//! manual only; automatic sync waits for A1 timers.
//!
//! Deviation D-1 (the 6b-2 `calendar_events` precedent):
//! the plan's §3 column list omits the columns the §5
//! replay rules need stored, so the table adds them
//! here, labeled, not silently:
//!
//! - `idempotency_key TEXT UNIQUE` — the create replay
//!   backstop (same key + same content replays the
//!   original row; same key + different content is a
//!   409). The replay compares Effect digests, not row
//!   fields: the row's `body_hash` is the re-read
//!   stored hash, which Notes may normalize away from
//!   the request hash.
//! - `effect_id` / `policy_version` — the create
//!   Effect and the Policy version at write time, the
//!   6b-1/6b-2 fact-row audit trail.
//! - `last_append_key` / `last_append_base_hash` /
//!   `last_append_text_hash` — the append replay state
//!   (same key + same (note_id, base, new text)
//!   replays 200). Internal replay state only: it is
//!   NOT part of the serialized `NoteRow` (read via
//!   `last_append_for`), so no client ever sees it.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::notes_source::{CreatedNote, NoteView, NOTES_SOURCE_ID};
use crate::operation_binding::sha256_hex;
use crate::sqlite_store::{
    append_event, canonical_json, id_suffix, now_ms, update_source_cursor_tx,
};

fn err(detail: impl std::fmt::Display) -> String {
    format!("sqlite: {detail}")
}

/// One synced / created note (LOCAL DEFINITION, plan
/// §3 + the D-1 columns above). `body_hash` is
/// sha256 of the re-read `plaintext` (Q-D1-6b-18);
/// the body itself is never stored. `deleted` is the
/// tombstone (Q-D1-6b-21); `updated_ms` advances on
/// append and on tombstone.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoteRow {
    pub note_id: String,
    pub source_id: String,
    pub native_id: String,
    pub folder: String,
    pub name: String,
    pub body_hash: String,
    pub idempotency_key: String,
    pub effect_id: String,
    pub policy_version: String,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub deleted: bool,
    pub origin_device_id: String,
}

fn note_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<NoteRow> {
    Ok(NoteRow {
        note_id: row.get(0)?,
        source_id: row.get(1)?,
        native_id: row.get(2)?,
        folder: row.get(3)?,
        name: row.get(4)?,
        body_hash: row.get(5)?,
        // Sync rows carry a NULL key (SQLite UNIQUE
        // treats NULLs as distinct, so the empty-key
        // rows never collide); created rows always
        // carry a real key.
        idempotency_key: row.get::<_, Option<String>>(6)?.unwrap_or_default(),
        effect_id: row.get(7)?,
        policy_version: row.get(8)?,
        created_ms: row.get(9)?,
        updated_ms: row.get(10)?,
        deleted: row.get::<_, i64>(11)? != 0,
        origin_device_id: row.get(12)?,
    })
}

const NOTES_COLS: &str = "note_id, source_id, native_id, folder, name, body_hash, idempotency_key, effect_id, policy_version, created_ms, updated_ms, deleted, origin_device_id";

/// One notes sync pass (manual trigger only): counts
/// plus the notes-source cursor. The gateway records
/// one row and one `note.synced` event per new note in
/// the same transaction. Locked notes are counted in
/// the separate locked count, not in the skipped count,
/// because they are excluded from sync (Q-D1-6b-17),
/// not merely already-present.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoteSyncResult {
    pub synced_count: usize,
    pub skipped_count: usize,
    pub locked_count: usize,
    pub total_count: usize,
    pub cursor: String,
}

/// The `note.*` event payloads: ids, hashes, lengths,
/// and timestamps only. `folder_hash` is sha256 of the
/// folder name, `name_len` the title's character count
/// — no folder, name, or body plaintext. The payloads
/// are built redacted and `append_event` whitelists
/// them again.
fn synced_payload(row: &NoteRow) -> String {
    canonical_json(&serde_json::json!({
        "note_id": row.note_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "folder_hash": sha256_hex(row.folder.as_bytes()),
        "name_len": row.name.chars().count(),
        "body_hash": row.body_hash,
        "created_ms": row.created_ms,
    }))
}

fn created_payload(row: &NoteRow, execution_id: &str) -> String {
    canonical_json(&serde_json::json!({
        "note_id": row.note_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "folder_hash": sha256_hex(row.folder.as_bytes()),
        "name_len": row.name.chars().count(),
        "body_hash": row.body_hash,
        "created_ms": row.created_ms,
        "execution_id": execution_id,
    }))
}

fn updated_payload(row: &NoteRow, execution_id: &str) -> String {
    canonical_json(&serde_json::json!({
        "note_id": row.note_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "folder_hash": sha256_hex(row.folder.as_bytes()),
        "name_len": row.name.chars().count(),
        "body_hash": row.body_hash,
        "created_ms": row.created_ms,
        "updated_ms": row.updated_ms,
        "execution_id": execution_id,
    }))
}

fn deleted_payload(row: &NoteRow, execution_id: &str) -> String {
    canonical_json(&serde_json::json!({
        "note_id": row.note_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "folder_hash": sha256_hex(row.folder.as_bytes()),
        "name_len": row.name.chars().count(),
        "body_hash": row.body_hash,
        "created_ms": row.created_ms,
        "updated_ms": row.updated_ms,
        "execution_id": execution_id,
    }))
}

/// The notes cursor: the content hash of the synced set
/// for this source — canonical JSON of the sorted
/// native ids, sha256 hex (the D1-4 file-cursor shape).
/// `""` means "never synced"; after any sync the
/// cursor is a 64-hex sha256, and it changes exactly
/// when the synced set changes. Tombstoned rows still
/// count: they are in the table precisely so sync can
/// see them and skip.
fn notes_cursor_tx(tx: &Transaction, source_id: &str) -> Result<String, String> {
    let mut stmt = tx
        .prepare("SELECT native_id FROM notes WHERE source_id = ?1 ORDER BY native_id ASC")
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

fn next_note_number_tx(tx: &Transaction) -> Result<u64, String> {
    let mut stmt = tx
        .prepare("SELECT note_id FROM notes")
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
/// row + one `note.synced` event per new note, then the
/// notes-source cursor advances. Rows already present
/// (same `(source_id, native_id)`) are skipped and
/// counted, never re-inserted — including tombstoned
/// rows (the anti-resurrect rule, see the module docs).
/// Locked notes are excluded before the exists check
/// (Q-D1-6b-17) and counted separately. A note whose
/// body changed in Notes.app since its sync is NOT
/// updated here (the mail precedent: sync only adds);
/// the append route's base check surfaces the drift.
pub fn record_sync(
    conn: &mut Connection,
    notes: &[NoteView],
    folder: &str,
    origin_device_id: &str,
) -> Result<NoteSyncResult, String> {
    let tx = conn.transaction().map_err(err)?;
    let mut next = next_note_number_tx(&tx)?;
    let mut synced = 0usize;
    let mut skipped = 0usize;
    let mut locked = 0usize;
    let now = now_ms() as i64;
    for note in notes {
        if note.password_protected {
            locked += 1;
            continue;
        }
        let exists: bool = tx
            .query_row(
                "SELECT 1 FROM notes WHERE source_id = ?1 AND native_id = ?2",
                params![NOTES_SOURCE_ID, note.native_id],
                |_| Ok(true),
            )
            .optional()
            .map_err(err)?
            .is_some();
        if exists {
            skipped += 1;
            continue;
        }
        let id = format!("note-{next}");
        next += 1;
        let row = NoteRow {
            note_id: id.clone(),
            source_id: NOTES_SOURCE_ID.to_string(),
            native_id: note.native_id.clone(),
            folder: folder.to_string(),
            name: note.name.clone(),
            body_hash: sha256_hex(note.plaintext.as_bytes()),
            // Sync rows are not write Effects: no
            // idempotency key, no Effect, no Policy
            // version — the key column is NULL (SQLite
            // UNIQUE treats NULLs as distinct, so the
            // keyless sync rows never collide) and the
            // other two read as empty strings (the
            // `effect_id` of a sync row is meaningless,
            // and no route ever looks it up).
            idempotency_key: String::new(),
            effect_id: String::new(),
            policy_version: String::new(),
            created_ms: now,
            updated_ms: now,
            deleted: false,
            origin_device_id: origin_device_id.to_string(),
        };
        tx.execute(
            &format!(
                "INSERT INTO notes ({NOTES_COLS})
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
            ),
            params![
                row.note_id,
                row.source_id,
                row.native_id,
                row.folder,
                row.name,
                row.body_hash,
                // NULL key for sync rows (see the
                // struct comment above): an empty
                // string would collide on the UNIQUE
                // column.
                Option::<String>::None,
                row.effect_id,
                row.policy_version,
                row.created_ms,
                row.updated_ms,
                0i64,
                row.origin_device_id,
            ],
        )
        .map_err(err)?;
        append_event(
            &tx,
            "note",
            &id,
            "note.synced",
            &synced_payload(&row),
            origin_device_id,
            now,
            1,
        )?;
        synced += 1;
    }
    let cursor = notes_cursor_tx(&tx, NOTES_SOURCE_ID)?;
    update_source_cursor_tx(&tx, NOTES_SOURCE_ID, &cursor)?;
    let total: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM notes WHERE source_id = ?1",
            params![NOTES_SOURCE_ID],
            |row| row.get(0),
        )
        .map_err(err)?;
    tx.commit().map_err(err)?;
    Ok(NoteSyncResult {
        synced_count: synced,
        skipped_count: skipped,
        locked_count: locked,
        total_count: total as usize,
        cursor,
    })
}

/// All synced rows for one source, oldest first.
/// The list INCLUDES tombstoned rows, with
/// `deleted = true` visible — the 6b-3 D-1 lesson
/// (approved plan §3 / Q-D1-6b-14, applied here to
/// notes): the tombstone state must be verifiable over
/// HTTP (red line 5: no reader without a write side).
pub fn list_notes(conn: &Connection, source_id: &str) -> Result<Vec<NoteRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {NOTES_COLS} FROM notes WHERE source_id = ?1 ORDER BY rowid ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![source_id], note_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// One row by local id, any source. Tombstoned rows
/// are still returned (the goals-detail precedent):
/// the delete route and the reader both need to see
/// the tombstone state.
pub fn get_note_row(conn: &Connection, note_id: &str) -> Result<Option<NoteRow>, String> {
    conn.query_row(
        &format!("SELECT {NOTES_COLS} FROM notes WHERE note_id = ?1"),
        params![note_id],
        note_from_row,
    )
    .optional()
    .map_err(err)
}

/// One row by native id, for one source.
pub fn find_by_native_id(
    conn: &Connection,
    source_id: &str,
    native_id: &str,
) -> Result<Option<NoteRow>, String> {
    conn.query_row(
        &format!("SELECT {NOTES_COLS} FROM notes WHERE source_id = ?1 AND native_id = ?2"),
        params![source_id, native_id],
        note_from_row,
    )
    .optional()
    .map_err(err)
}

/// One created row by idempotency key (the create
/// replay lookup). The key is UNIQUE on created rows,
/// so at most one row ever matches. Sync rows carry an
/// empty key and never match a real key.
pub fn find_by_idempotency_key(
    conn: &Connection,
    idempotency_key: &str,
) -> Result<Option<NoteRow>, String> {
    conn.query_row(
        &format!("SELECT {NOTES_COLS} FROM notes WHERE idempotency_key = ?1"),
        params![idempotency_key],
        note_from_row,
    )
    .optional()
    .map_err(err)
}

/// One created row by Effect id (the Evidence readback
/// lookup for creates: the Effect row names the fact
/// row, the fact row names the Notes native id).
pub fn find_by_effect_id(
    conn: &Connection,
    effect_id: &str,
) -> Result<Option<NoteRow>, String> {
    conn.query_row(
        &format!("SELECT {NOTES_COLS} FROM notes WHERE effect_id = ?1"),
        params![effect_id],
        note_from_row,
    )
    .optional()
    .map_err(err)
}

/// The last append replay state for one row (D-1:
/// internal replay state, not part of the serialized
/// row). All three `None` when the note was never
/// appended to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LastAppendState {
    pub key: Option<String>,
    pub base_body_hash: Option<String>,
    pub text_hash: Option<String>,
}

/// The last append replay state for one row: the
/// client key, the base hash, and the appended-text
/// hash of the most recent successful append.
pub fn last_append_for(conn: &Connection, note_id: &str) -> Result<LastAppendState, String> {
    conn.query_row(
        "SELECT last_append_key, last_append_base_hash, last_append_text_hash FROM notes WHERE note_id = ?1",
        params![note_id],
        |row| {
            Ok(LastAppendState {
                key: row.get::<_, Option<String>>(0)?,
                base_body_hash: row.get::<_, Option<String>>(1)?,
                text_hash: row.get::<_, Option<String>>(2)?,
            })
        },
    )
    .optional()
    .map_err(err)
    .map(|found| found.unwrap_or_default())
}

/// Record one accepted create in a single transaction:
/// the fact row + one `note.created` event. The
/// caller passes the Effect id the row belongs to; the
/// Effect itself is finished by the caller after this
/// returns (row first, then terminal transition — a
/// succeeded Effect must never exist without its fact
/// row). `body_hash` is the hash of the RE-READ
/// plaintext the backend returned, not of the request
/// text (see `notes_source.rs`).
#[allow(clippy::too_many_arguments)]
pub fn record_created(
    conn: &mut Connection,
    created: &CreatedNote,
    folder: &str,
    body_hash: &str,
    idempotency_key: &str,
    effect_id: &str,
    execution_id: &str,
    policy_version: &str,
    origin_device_id: &str,
) -> Result<NoteRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let now = now_ms() as i64;
    // The local id comes from the same max-suffix + 1
    // counter as sync (one writer at a time, serialized
    // by the caller's SQLite lock).
    let next = next_note_number_tx(&tx)?;
    let id = format!("note-{next}");
    let row = NoteRow {
        note_id: id.clone(),
        source_id: NOTES_SOURCE_ID.to_string(),
        native_id: created.native_id.clone(),
        folder: folder.to_string(),
        name: created.name.clone(),
        body_hash: body_hash.to_string(),
        idempotency_key: idempotency_key.to_string(),
        effect_id: effect_id.to_string(),
        policy_version: policy_version.to_string(),
        created_ms: now,
        updated_ms: now,
        deleted: false,
        origin_device_id: origin_device_id.to_string(),
    };
    tx.execute(
        &format!(
            "INSERT INTO notes ({NOTES_COLS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
        ),
        params![
            row.note_id,
            row.source_id,
            row.native_id,
            row.folder,
            row.name,
            row.body_hash,
            row.idempotency_key,
            row.effect_id,
            row.policy_version,
            row.created_ms,
            row.updated_ms,
            0i64,
            row.origin_device_id,
        ],
    )
    .map_err(err)?;
    append_event(
        &tx,
        "note",
        &id,
        "note.created",
        &created_payload(&row, execution_id),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(row)
}

/// Record one accepted append in a single transaction:
/// the row's `body_hash` + `updated_ms` advance, the
/// last-append replay triple is stored, plus one
/// `note.updated` event. The caller passes the hash of
/// the post-write re-read plaintext (the backend's
/// return), so the row records what Notes actually
/// stored. Returns the updated row, or `Ok(None)` when
/// the row is unknown.
#[allow(clippy::too_many_arguments)]
pub fn record_appended(
    conn: &mut Connection,
    note_id: &str,
    new_body_hash: &str,
    idempotency_key: &str,
    base_body_hash: &str,
    text_hash: &str,
    execution_id: &str,
    origin_device_id: &str,
) -> Result<Option<NoteRow>, String> {
    let tx = conn.transaction().map_err(err)?;
    let Some(mut row) = get_note_row(&tx, note_id)? else {
        return Ok(None);
    };
    let now = now_ms() as i64;
    tx.execute(
        "UPDATE notes SET body_hash = ?1, updated_ms = ?2, last_append_key = ?3, last_append_base_hash = ?4, last_append_text_hash = ?5 WHERE note_id = ?6",
        params![new_body_hash, now, idempotency_key, base_body_hash, text_hash, note_id],
    )
    .map_err(err)?;
    row.body_hash = new_body_hash.to_string();
    row.updated_ms = now;
    append_event(
        &tx,
        "note",
        &row.note_id,
        "note.updated",
        &updated_payload(&row, execution_id),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(Some(row))
}

/// Outcome of a tombstone write.
pub enum TombstoneOutcome {
    /// Newly tombstoned (the returned row is post-state).
    Tombstoned(NoteRow),
    /// Already tombstoned (the returned row is current
    /// state; no second event was written).
    AlreadyTombstoned(NoteRow),
}

/// Tombstone one row in a single transaction:
/// `deleted = 1`, `updated_ms` advances, text stays on
/// the row, plus one `note.deleted` event. A re-delete
/// is a no-op returning the current row (no second
/// event), like re-archive / re-disconnect and the D1-5
/// memory delete. Unknown id returns `Ok(None)`.
pub fn tombstone_note(
    conn: &mut Connection,
    note_id: &str,
    execution_id: &str,
    origin_device_id: &str,
) -> Result<Option<TombstoneOutcome>, String> {
    let tx = conn.transaction().map_err(err)?;
    let Some(row) = get_note_row(&tx, note_id)? else {
        return Ok(None);
    };
    if row.deleted {
        tx.commit().map_err(err)?;
        return Ok(Some(TombstoneOutcome::AlreadyTombstoned(row)));
    }
    let now = now_ms() as i64;
    tx.execute(
        "UPDATE notes SET deleted = 1, updated_ms = ?1 WHERE note_id = ?2",
        params![now, note_id],
    )
    .map_err(err)?;
    let mut row = row;
    row.deleted = true;
    row.updated_ms = now;
    append_event(
        &tx,
        "note",
        &row.note_id,
        "note.deleted",
        &deleted_payload(&row, execution_id),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(Some(TombstoneOutcome::Tombstoned(row)))
}

/// Next free note number (max suffix + 1, or 1). The
/// sync / create paths compute this inside their own
/// transactions; this is the read-only seam for tests.
pub fn next_note_number(conn: &Connection) -> Result<u64, String> {
    let mut stmt = conn.prepare("SELECT note_id FROM notes").map_err(err)?;
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
