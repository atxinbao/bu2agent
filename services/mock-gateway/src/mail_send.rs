//! D1-6b-1 (Q-D1-6b-05/06/07/08): the mail send fact
//! record.
//!
//! One `sent_messages` row per accepted send, one
//! `mail.sent` event in the SAME transaction (the
//! `mail_sync.rs` `record_sync` pattern: row + event in
//! one tx). The row is the fact source; the event is a
//! redacted audit summary, never the row serialization,
//! so mail has NO fold arm (the D1-6a `mail.synced`
//! precedent).
//!
//! Privacy (red line 7): the table stores `to_json` /
//! `subject` (user-local fact record, same posture as
//! `mail_messages`) but NEVER the body — only
//! `body_hash`. The `mail.sent` event payload carries
//! ids, hashes, lengths, and counts only (no recipient
//! plaintext, no subject, no body); `append_event`
//! whitelists it again. "Sent" means Mail.app accepted
//! the message, never that it was delivered (Q-D1-6b-06).
//!
//! Idempotency (Q-D1-6b-05): `idempotency_key` is
//! UNIQUE and client-supplied (no key → 400 at the
//! route). Same key + same content (to/subject/
//! body_hash all equal) replays the original row;
//! same key + different content is a 409. Local ids
//! are `sent-N`, assigned from max-suffix + 1 inside
//! the write transaction (the `mail-N` precedent in
//! `mail_sync.rs`).

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::mail_sync::MAIL_SOURCE_ID;
use crate::operation_binding::sha256_hex;
use crate::sqlite_store::{append_event, canonical_json, id_suffix, now_ms};

fn err(detail: impl std::fmt::Display) -> String {
    format!("sqlite: {detail}")
}

/// One sent message (LOCAL DEFINITION, plan §2.3).
/// `sent_id` is `sent-N` (a distinct shape from
/// `mail-N`, so the two id spaces can never collide).
/// `native_id` is nullable: Mail.app may accept a send
/// whose id the script cannot read back (Q-D1-6b-08).
/// `to_json` is the canonical JSON of the deduplicated,
/// sorted recipient list; `subject` is stored (the body
/// never is — only `body_hash`). `policy_version` is
/// the current Policy version at write time, for the
/// Mandate audit trail (Q-D1-6b-23).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SentMessageRow {
    pub sent_id: String,
    pub source_id: String,
    pub native_id: Option<String>,
    pub idempotency_key: String,
    pub to_json: String,
    pub subject: String,
    pub body_hash: String,
    pub sent_ms: i64,
    pub effect_id: String,
    pub policy_version: String,
    pub created_ms: i64,
    pub origin_device_id: String,
}

fn sent_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SentMessageRow> {
    Ok(SentMessageRow {
        sent_id: row.get(0)?,
        source_id: row.get(1)?,
        native_id: row.get(2)?,
        idempotency_key: row.get(3)?,
        to_json: row.get(4)?,
        subject: row.get(5)?,
        body_hash: row.get(6)?,
        sent_ms: row.get(7)?,
        effect_id: row.get(8)?,
        policy_version: row.get(9)?,
        created_ms: row.get(10)?,
        origin_device_id: row.get(11)?,
    })
}

const SENT_COLS: &str = "sent_id, source_id, native_id, idempotency_key, to_json, subject, body_hash, sent_ms, effect_id, policy_version, created_ms, origin_device_id";

/// Normalize the recipient list for storage and digest:
/// trim, drop empties, deduplicate, sort. The digest
/// and the stored `to_json` both use this order, so
/// approval time and send time can never disagree about
/// the recipient set.
pub fn normalize_recipients(to: &[String]) -> Vec<String> {
    let mut recipients: Vec<String> = to
        .iter()
        .map(|addr| addr.trim().to_string())
        .filter(|addr| !addr.is_empty())
        .collect();
    recipients.sort();
    recipients.dedup();
    recipients
}

/// The body hash: sha256 of the body text. The body
/// itself is never stored and never enters an event.
pub fn mail_body_hash(body: &str) -> String {
    sha256_hex(body.as_bytes())
}

/// Canonical JSON of the normalized recipient list, as
/// stored in `to_json`.
pub fn recipients_json(to_sorted: &[String]) -> String {
    serde_json::to_string(to_sorted).unwrap_or_else(|_| "[]".to_string())
}

/// The `mail.sent` event payload: ids, hashes, lengths,
/// and counts only. `subject_len` is a character count;
/// `recipient_hash` is the sha256 of the canonical
/// recipient JSON (the `sender_hash` precedent);
/// `date_ms` is the sent timestamp (`sent_ms` — the
/// field keeps the plan's `date_ms` name). No recipient
/// plaintext, no subject, no body — the payload is
/// built redacted and `append_event` whitelists it
/// again.
fn sent_payload(row: &SentMessageRow, execution_id: &str) -> String {
    let recipients: Vec<String> = serde_json::from_str(&row.to_json).unwrap_or_default();
    canonical_json(&serde_json::json!({
        "sent_id": row.sent_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "body_hash": row.body_hash,
        "subject_len": row.subject.chars().count(),
        "recipient_count": recipients.len(),
        "recipient_hash": sha256_hex(row.to_json.as_bytes()),
        "date_ms": row.sent_ms,
        "created_ms": row.created_ms,
        "execution_id": execution_id,
    }))
}

fn next_sent_number_tx(tx: &Transaction) -> Result<u64, String> {
    let mut stmt = tx
        .prepare("SELECT sent_id FROM sent_messages")
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
) -> Result<Option<SentMessageRow>, String> {
    conn.query_row(
        &format!("SELECT {SENT_COLS} FROM sent_messages WHERE idempotency_key = ?1"),
        params![idempotency_key],
        sent_from_row,
    )
    .optional()
    .map_err(err)
}

/// Record one accepted send in a single transaction:
/// the `sent-N` row + one `mail.sent` event. The caller
/// passes the Effect id the row belongs to; the Effect
/// itself is finished by the caller after this returns
/// (row first, then terminal transition — a succeeded
/// Effect must never exist without its fact row).
#[allow(clippy::too_many_arguments)]
pub fn record_sent(
    conn: &mut Connection,
    native_id: Option<&str>,
    idempotency_key: &str,
    to_sorted: &[String],
    subject: &str,
    body_hash: &str,
    sent_ms: i64,
    effect_id: &str,
    execution_id: &str,
    policy_version: &str,
    origin_device_id: &str,
) -> Result<SentMessageRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let next = next_sent_number_tx(&tx)?;
    let now = now_ms() as i64;
    let row = SentMessageRow {
        sent_id: format!("sent-{next}"),
        source_id: MAIL_SOURCE_ID.to_string(),
        native_id: native_id.map(str::to_string),
        idempotency_key: idempotency_key.to_string(),
        to_json: recipients_json(to_sorted),
        subject: subject.to_string(),
        body_hash: body_hash.to_string(),
        sent_ms,
        effect_id: effect_id.to_string(),
        policy_version: policy_version.to_string(),
        created_ms: now,
        origin_device_id: origin_device_id.to_string(),
    };
    tx.execute(
        &format!(
            "INSERT INTO sent_messages ({SENT_COLS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)"
        ),
        params![
            row.sent_id,
            row.source_id,
            row.native_id,
            row.idempotency_key,
            row.to_json,
            row.subject,
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
        "mail",
        &row.sent_id,
        "mail.sent",
        &sent_payload(&row, execution_id),
        origin_device_id,
        now,
        1,
    )?;
    tx.commit().map_err(err)?;
    Ok(row)
}

/// All sent rows for the mail source, oldest first.
pub fn list_sent_messages(conn: &Connection) -> Result<Vec<SentMessageRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {SENT_COLS} FROM sent_messages WHERE source_id = ?1 ORDER BY rowid ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![MAIL_SOURCE_ID], sent_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// One sent row by local id.
pub fn get_sent_message(
    conn: &Connection,
    sent_id: &str,
) -> Result<Option<SentMessageRow>, String> {
    conn.query_row(
        &format!("SELECT {SENT_COLS} FROM sent_messages WHERE sent_id = ?1"),
        params![sent_id],
        sent_from_row,
    )
    .optional()
    .map_err(err)
}
