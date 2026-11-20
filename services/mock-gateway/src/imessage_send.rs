//! MCL-4 (Q-MCL-20/21/23): the iMessage send fact
//! record.
//!
//! One `sent_imessages` row per accepted send, one
//! `imessage.sent` event in the SAME transaction (the
//! `mail_send.rs` `record_sent` pattern: row + event in
//! one tx). The row is the fact source; the event is a
//! redacted audit summary, never the row serialization,
//! so imessage has NO fold arm (the D1-6a `mail.synced`
//! precedent).
//!
//! Privacy (red line 7): the table stores NO body —
//! only `body_hash`. The `imessage.sent` event payload
//! carries ids, hashes, lengths, and counts only (the
//! thread's participant count + hash, never a handle or
//! a name; no body); `append_event` whitelists it again
//! — every payload field below (`sent_id` /
//! `thread_id` / `recipient_count` / `recipient_hash` /
//! `body_hash` / `date_ms` / `created_ms` /
//! `execution_id`, plus `source_id` / `native_id`) is
//! already in `REDACTED_PAYLOAD_FIELDS` (the whitelist
//! is by field name globally, so the mail `sent_id`
//! needs no rename: mail and imessage rows are told
//! apart by `event_type` / `aggregate_kind`, never by
//! field name). "Sent" means Messages.app accepted the
//! message, never that it was delivered (Q-MCL-21).
//!
//! Idempotency (Q-MCL-23, the 6b-1 shape):
//! `idempotency_key` is UNIQUE and client-supplied (no
//! key → 400 at the route). Same key + same content
//! (thread_id / body_hash both equal) replays the
//! original row; same key + different content is a 409.
//! Local ids are `sent-imsg-N`, assigned from
//! max-suffix + 1 inside the write transaction (the
//! `sent-N` precedent in `mail_send.rs`).

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::imessage_sync::{IMessageThreadRow, IMESSAGE_SOURCE_ID};
use crate::operation_binding::sha256_hex;
use crate::sqlite_store::{append_event, canonical_json, id_suffix, now_ms};

fn err(detail: impl std::fmt::Display) -> String {
    format!("sqlite: {detail}")
}

/// One sent iMessage (LOCAL DEFINITION, plan §6).
/// `sent_id` is `sent-imsg-N` (a distinct shape from
/// `sent-N`, so the mail and imessage id spaces can
/// never collide). `native_id` is nullable: the send
/// script has no read-back (reading the sent message
/// would need message history — falsified by the MCL-0
/// probe), so NULL is the normal case, never
/// fabricated. `thread_id` is the local `thread-N` the
/// message was sent to. The body never is stored —
/// only `body_hash`. `policy_version` is the current
/// Policy version at write time, for the Mandate audit
/// trail (Q-D1-6b-23).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SentIMessageRow {
    pub sent_id: String,
    pub source_id: String,
    pub thread_id: String,
    pub native_id: Option<String>,
    pub idempotency_key: String,
    pub body_hash: String,
    pub sent_ms: i64,
    pub effect_id: String,
    pub policy_version: String,
    pub created_ms: i64,
    pub origin_device_id: String,
}

fn sent_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SentIMessageRow> {
    Ok(SentIMessageRow {
        sent_id: row.get(0)?,
        source_id: row.get(1)?,
        thread_id: row.get(2)?,
        native_id: row.get(3)?,
        idempotency_key: row.get(4)?,
        body_hash: row.get(5)?,
        sent_ms: row.get(6)?,
        effect_id: row.get(7)?,
        policy_version: row.get(8)?,
        created_ms: row.get(9)?,
        origin_device_id: row.get(10)?,
    })
}

const SENT_COLS: &str = "sent_id, source_id, thread_id, native_id, idempotency_key, body_hash, sent_ms, effect_id, policy_version, created_ms, origin_device_id";

/// The body hash: sha256 of the body text. The body
/// itself is never stored and never enters an event.
pub fn imessage_body_hash(body: &str) -> String {
    sha256_hex(body.as_bytes())
}

/// The `imessage.sent` event payload: ids, hashes, and
/// counts only. `recipient_count` / `recipient_hash`
/// are the thread's participant count + hash (the
/// `mail.sent` precedent: the set is auditable without
/// naming anyone); `date_ms` is the sent timestamp
/// (`sent_ms` — the field keeps the plan's `date_ms`
/// name). No body, no handle, no name — the payload is
/// built redacted and `append_event` whitelists it
/// again.
fn sent_payload(row: &SentIMessageRow, thread: &IMessageThreadRow, execution_id: &str) -> String {
    canonical_json(&serde_json::json!({
        "sent_id": row.sent_id,
        "source_id": row.source_id,
        "thread_id": row.thread_id,
        "native_id": row.native_id,
        "recipient_count": thread.participant_count,
        "recipient_hash": thread.participant_hash,
        "body_hash": row.body_hash,
        "date_ms": row.sent_ms,
        "created_ms": row.created_ms,
        "execution_id": execution_id,
    }))
}

fn next_sent_number_tx(tx: &Transaction) -> Result<u64, String> {
    let mut stmt = tx
        .prepare("SELECT sent_id FROM sent_imessages")
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

/// One sent row by idempotency key (the replay lookup).
/// The key is UNIQUE, so at most one row ever matches.
pub fn find_by_idempotency_key(
    conn: &Connection,
    idempotency_key: &str,
) -> Result<Option<SentIMessageRow>, String> {
    conn.query_row(
        &format!("SELECT {SENT_COLS} FROM sent_imessages WHERE idempotency_key = ?1"),
        params![idempotency_key],
        sent_from_row,
    )
    .optional()
    .map_err(err)
}

/// Record one accepted send in a single transaction:
/// the `sent-imsg-N` row + one `imessage.sent` event.
/// The caller passes the Effect id the row belongs to;
/// the Effect itself is finished by the caller after
/// this returns (row first, then terminal transition —
/// a succeeded Effect must never exist without its fact
/// row).
#[allow(clippy::too_many_arguments)]
pub fn record_sent(
    conn: &mut Connection,
    native_id: Option<&str>,
    idempotency_key: &str,
    thread: &IMessageThreadRow,
    body_hash: &str,
    sent_ms: i64,
    effect_id: &str,
    execution_id: &str,
    policy_version: &str,
    origin_device_id: &str,
) -> Result<SentIMessageRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let next = next_sent_number_tx(&tx)?;
    let now = now_ms() as i64;
    let row = SentIMessageRow {
        sent_id: format!("sent-imsg-{next}"),
        source_id: IMESSAGE_SOURCE_ID.to_string(),
        thread_id: thread.thread_id.clone(),
        native_id: native_id.map(str::to_string),
        idempotency_key: idempotency_key.to_string(),
        body_hash: body_hash.to_string(),
        sent_ms,
        effect_id: effect_id.to_string(),
        policy_version: policy_version.to_string(),
        created_ms: now,
        origin_device_id: origin_device_id.to_string(),
    };
    tx.execute(
        &format!(
            "INSERT INTO sent_imessages ({SENT_COLS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
        ),
        params![
            row.sent_id,
            row.source_id,
            row.thread_id,
            row.native_id,
            row.idempotency_key,
            row.body_hash,
            row.sent_ms,
            row.effect_id,
            row.policy_version,
            row.created_ms,
            row.origin_device_id,
        ],
    )
    .map_err(err)?;
    append_event(
        &tx,
        "imessage",
        &row.sent_id,
        "imessage.sent",
        &sent_payload(&row, thread, execution_id),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(row)
}

/// All sent rows for the imessage source, oldest first.
pub fn list_sent_imessages(conn: &Connection) -> Result<Vec<SentIMessageRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {SENT_COLS} FROM sent_imessages WHERE source_id = ?1 ORDER BY rowid ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![IMESSAGE_SOURCE_ID], sent_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// One sent row by local id.
pub fn get_sent_imessage(
    conn: &Connection,
    sent_id: &str,
) -> Result<Option<SentIMessageRow>, String> {
    conn.query_row(
        &format!("SELECT {SENT_COLS} FROM sent_imessages WHERE sent_id = ?1"),
        params![sent_id],
        sent_from_row,
    )
    .optional()
    .map_err(err)
}
