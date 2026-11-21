//! Contacts sync: `contacts` fact table + `contact.synced`
//! events + source cursor. MCL-2 (SQLite v15).
//!
//! Shape (plan §4, Q-MCL-11 / Q-MCL-12):
//!
//! - `record_sync` in ONE transaction: new rows + their
//!   `contact.synced` events + the cursor advance.
//! - The row key is `(source_id, native_id)` (UNIQUE):
//!   NO merging — a person with several cards has
//!   several rows (Q-MCL-12).
//! - The sync is insert-if-absent, exactly the mail /
//!   imessage precedent: rows already present are
//!   skipped, never re-inserted, and a tombstone is
//!   never resurrected (the exists check has NO
//!   `deleted = 0` filter — see mail_sync.rs:233-241).
//!   Changed fields in the live source are NOT
//!   reflected by a re-sync in MCL-2 (update semantics
//!   belong to the MCL-5 write batch, which defines the
//!   409 + current-hash line); a re-sync that sees a
//!   known native_id skips the row.
//! - The cursor is the sha256 of the sorted live
//!   native ids, so it advances exactly when the
//!   contact set changes.
//! - The event payload carries ids / hashes / counts /
//!   timestamps ONLY (the 13-field allowlist in
//!   `redact_event_payload` must carry the new field
//!   names; anything else the payload names is dropped
//!   there). List / get / search responses DO carry
//!   plaintext display_name / phones / emails —
//!   Q-MCL-11 says display and dial need them. The
//!   redact test asserts on EVENT payloads only.
//! - Phones / emails ride as JSON arrays on the wire;
//!   the table stores the canonical JSON text
//!   (`phones_json` / `emails_json` columns). Address /
//!   birthday values are never stored at all — only
//!   `addresses_hash` / `birthday_hash` (NULL = none).
//!
//! MCL-5 adds the write record functions
//! (`record_contact_created` / `record_contact_updated`
//! / `record_contact_deleted`): one transaction per
//! write — the row (with its write audit trail:
//! `idempotency_key` / `effect_id` / `policy_version`)
//! and the `contact.created` / `contact.updated` /
//! `contact.deleted` event. The sync stays
//! insert-if-absent and never touches those columns.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::{
    contacts_source::{
        addresses_hash_for, birthday_hash_for, string_list_hash, ContactContent, ContactView,
    },
    operation_binding::{
        contact_content_hash, contact_create_digest, contact_delete_digest, contact_update_digest,
        sha256_hex,
    },
    sqlite_store::{append_event, update_source_cursor_tx, DB_SCHEMA_VERSION},
};

/// The seeded contacts source id (the registry seed in
/// `source_registry.rs` writes this row).
pub const CONTACTS_SOURCE_ID: &str = "contacts-source";

/// A contacts row, ready to serialize. `phones` /
/// `emails` are JSON arrays on the wire (Q-MCL-11
/// plaintext); `addresses_hash` / `birthday_hash` are
/// hashes or NULL (NULL is the existence flag). The
/// `deleted` column is visible in list responses
/// (6b-3 Q-14: tombstones visible) so the UI can show a
/// deleted card as deleted, never as a live contact.
///
/// MCL-5 adds the write audit trail + the conflict
/// base: `idempotency_key` / `effect_id` /
/// `policy_version` are the last write's key, Effect,
/// and Policy version (NULL on rows that only ever
/// synced — the backfill columns land NULL on old
/// rows); `content_hash` is computed on the fly from
/// the stored fields (never stored in its own column),
/// so the UI can present it as the update's
/// `base_content_hash` and the route can compare it
/// against a live re-read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactRow {
    pub contact_id: String,
    pub source_id: String,
    pub native_id: String,
    pub display_name: String,
    pub organization: String,
    pub job_title: String,
    pub phones: Vec<String>,
    pub emails: Vec<String>,
    pub addresses_hash: Option<String>,
    pub birthday_hash: Option<String>,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub deleted: bool,
    pub origin_device_id: String,
    pub idempotency_key: Option<String>,
    pub effect_id: Option<String>,
    pub policy_version: Option<String>,
    pub content_hash: String,
}

/// One sync's outcome: rows inserted, rows skipped
/// (already present, including tombstones), the total
/// row count for the source (tombstones included — the
/// mail / imessage precedent), and the new cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactsSyncResult {
    pub synced_count: usize,
    pub skipped_count: usize,
    pub total_count: usize,
    pub cursor: String,
}

/// The redacted event payload for one synced contact:
/// exactly the 13 fields the plan allows — contact_id,
/// source_id, native_id, display_name_hash,
/// organization_hash, phone_count, phone_hash,
/// email_count, email_hash, has_address, has_birthday,
/// created_ms, updated_ms. No display name, no phone or
/// email value, no address or birthday value (or
/// hash-input), no job title. `has_address` /
/// `has_birthday` are the existence flags (the hash
/// column being non-NULL says the same thing, but the
/// payload names them explicitly for the event log).
fn synced_payload(row: &ContactRow) -> String {
    let payload = serde_json::json!({
        "contact_id": row.contact_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "display_name_hash": sha256_hex(row.display_name.as_bytes()),
        "organization_hash": sha256_hex(row.organization.as_bytes()),
        "phone_count": row.phones.len(),
        "phone_hash": string_list_hash(&row.phones),
        "email_count": row.emails.len(),
        "email_hash": string_list_hash(&row.emails),
        "has_address": row.addresses_hash.is_some(),
        "has_birthday": row.birthday_hash.is_some(),
        "created_ms": row.created_ms,
        "updated_ms": row.updated_ms,
    });
    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
}

fn row_from_sqlite(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContactRow> {
    let phones_json: String = row.get("phones_json")?;
    let emails_json: String = row.get("emails_json")?;
    let phones: Vec<String> = serde_json::from_str(&phones_json).unwrap_or_default();
    let emails: Vec<String> = serde_json::from_str(&emails_json).unwrap_or_default();
    let display_name: String = row.get("display_name")?;
    let organization: String = row.get("organization")?;
    let job_title: String = row.get("job_title")?;
    // The conflict base is computed on the fly, never
    // stored: the hash normalizes (trims / sorts /
    // dedupes), so a stored row, a presented write, and
    // a live re-read hash the same content the same way.
    let content_hash =
        contact_content_hash(&display_name, &organization, &job_title, &phones, &emails);
    Ok(ContactRow {
        contact_id: row.get("contact_id")?,
        source_id: row.get("source_id")?,
        native_id: row.get("native_id")?,
        display_name,
        organization,
        job_title,
        phones,
        emails,
        addresses_hash: row.get("addresses_hash")?,
        birthday_hash: row.get("birthday_hash")?,
        created_ms: row.get("created_ms")?,
        updated_ms: row.get("updated_ms")?,
        deleted: row.get::<_, i64>("deleted")? != 0,
        origin_device_id: row.get("origin_device_id")?,
        // The backfill columns are NULL on rows that
        // only ever synced (and on pre-MCL-5 rows); a
        // missing column (a DB that predates the
        // backfill step) reads as NULL too — the column
        // is optional by name, so an older fixture
        // without it still reads.
        idempotency_key: row
            .get::<_, Option<String>>("idempotency_key")
            .unwrap_or(None),
        effect_id: row.get::<_, Option<String>>("effect_id").unwrap_or(None),
        policy_version: row
            .get::<_, Option<String>>("policy_version")
            .unwrap_or(None),
        content_hash,
    })
}

/// Insert one new synced contact row + its
/// `contact.synced` event in the caller's transaction.
/// Returns `true` when the row was inserted, `false`
/// when the contact already exists (including a
/// tombstone: the exists check has NO `deleted = 0`
/// filter, so a deleted contact is never resurrected
/// by a later sync — the mail_sync.rs precedent).
fn insert_contact_if_absent(
    tx: &Transaction<'_>,
    view: &ContactView,
    source_id: &str,
    origin_device_id: &str,
    now_ms: i64,
) -> Result<bool, String> {
    let exists = tx
        .query_row(
            "SELECT contact_id FROM contacts WHERE source_id = ?1 AND native_id = ?2",
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
            "SELECT COUNT(*) FROM contacts WHERE source_id = ?1",
            params![source_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())?;
    let contact_id = format!("contact-{}", next + 1);
    let phones_json = serde_json::to_string(&view.phones).unwrap_or_else(|_| "[]".to_string());
    let emails_json = serde_json::to_string(&view.emails).unwrap_or_else(|_| "[]".to_string());
    let row = ContactRow {
        contact_id,
        source_id: source_id.to_string(),
        native_id: view.native_id.clone(),
        display_name: view.display_name.clone(),
        organization: view.organization.clone(),
        job_title: view.job_title.clone(),
        phones: view.phones.clone(),
        emails: view.emails.clone(),
        addresses_hash: addresses_hash_for(&view.addresses),
        birthday_hash: view.birthday.as_deref().map(birthday_hash_for),
        created_ms: now_ms,
        updated_ms: now_ms,
        deleted: false,
        origin_device_id: origin_device_id.to_string(),
        // A synced row has no write behind it: the
        // write audit trail stays NULL until an MCL-5
        // write records one. The content hash is
        // computed from the synced fields, like every
        // row read.
        idempotency_key: None,
        effect_id: None,
        policy_version: None,
        content_hash: contact_content_hash(
            &view.display_name,
            &view.organization,
            &view.job_title,
            &view.phones,
            &view.emails,
        ),
    };
    tx.execute(
        "INSERT INTO contacts (contact_id, source_id, native_id, display_name, organization, \
         job_title, phones_json, emails_json, addresses_hash, birthday_hash, created_ms, \
         updated_ms, deleted, origin_device_id) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11, 0, ?12)",
        params![
            row.contact_id,
            row.source_id,
            row.native_id,
            row.display_name,
            row.organization,
            row.job_title,
            phones_json,
            emails_json,
            row.addresses_hash,
            row.birthday_hash,
            now_ms,
            row.origin_device_id,
        ],
    )
    .map_err(|e| e.to_string())?;
    append_event(
        tx,
        "contacts",
        &row.contact_id,
        "contact.synced",
        &synced_payload(&row),
        origin_device_id,
        now_ms,
        DB_SCHEMA_VERSION,
    )?;
    Ok(true)
}

/// Record one sync into the `contacts` table from the
/// live source's enumeration, in a single transaction:
/// new rows + their `contact.synced` events + the
/// cursor advance. The cursor is the sha256 of the
/// sorted native ids (the mail / imessage pattern), so
/// it advances exactly when the contact set changes.
/// Sync is idempotent: contacts already in the table
/// are skipped, never re-inserted, and tombstones are
/// never resurrected.
pub fn record_sync(
    conn: &mut Connection,
    contacts: &[ContactView],
    source_id: &str,
    origin_device_id: &str,
) -> Result<ContactsSyncResult, String> {
    let now_ms = crate::sqlite_store::now_ms() as i64;
    let mut native_ids: Vec<&str> = contacts.iter().map(|c| c.native_id.as_str()).collect();
    native_ids.sort();
    let cursor = sha256_hex(native_ids.join(",").as_bytes());
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let mut synced_count = 0usize;
    let mut skipped_count = 0usize;
    for contact in contacts {
        if insert_contact_if_absent(&tx, contact, source_id, origin_device_id, now_ms)? {
            synced_count += 1;
        } else {
            skipped_count += 1;
        }
    }
    let cursor_changed = update_source_cursor_tx(&tx, source_id, &cursor)?;
    let total_count: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM contacts WHERE source_id = ?1",
            params![source_id],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())?;
    let _ = cursor_changed;
    Ok(ContactsSyncResult {
        synced_count,
        skipped_count,
        total_count: total_count as usize,
        cursor,
    })
}

/// The stored cursor for one source (`""` before the
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

/// The full contacts column list (MCL-5: the three
/// backfill columns ride along so `row_from_sqlite`
/// sees the write audit trail).
const CONTACT_COLS: &str = "contact_id, source_id, native_id, display_name, organization, \
    job_title, phones_json, emails_json, addresses_hash, birthday_hash, created_ms, \
    updated_ms, deleted, origin_device_id, idempotency_key, effect_id, policy_version";

/// 按不可覆盖的写入索引验证摘要，再返回联系人当前行（含墓碑）。
/// 同键跨操作、跨联系人或不同内容均冲突，不能用当前行重算历史摘要。
pub fn find_contact_by_idempotency_key(
    conn: &Connection,
    idempotency_key: &str,
    operation: &str,
    content_digest: &str,
    contact_id: Option<&str>,
) -> Result<Option<ContactRow>, String> {
    let entry = conn.query_row(
        "SELECT contact_id, operation, content_digest FROM contact_idempotency WHERE idempotency_key = ?1",
        params![idempotency_key],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
    ).optional().map_err(|e| e.to_string())?;
    let Some((stored_id, stored_operation, stored_digest)) = entry else {
        return Ok(None);
    };
    if stored_operation != operation
        || stored_digest != content_digest
        || contact_id.is_some_and(|id| id != stored_id)
    {
        return Err("idempotency_key_conflict".to_string());
    }
    get_contact(conn, &stored_id)?
        .map(Some)
        .ok_or_else(|| "contact idempotency references missing row".to_string())
}

/// 索引与联系人、事件在同一事务中写入；冲突必须回滚，不能覆盖原记录。
#[allow(clippy::too_many_arguments)]
fn insert_contact_idempotency(
    tx: &Transaction<'_>,
    key: &str,
    contact_id: &str,
    operation: &str,
    digest: &str,
    effect_id: &str,
    created_ms: i64,
    origin_device_id: &str,
) -> Result<(), String> {
    tx.execute(
        "INSERT INTO contact_idempotency (idempotency_key, contact_id, operation, content_digest, effect_id, created_ms, origin_device_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![key, contact_id, operation, digest, effect_id, created_ms, origin_device_id],
    ).map_err(|e| e.to_string())?;
    Ok(())
}

/// One contact by its live native id (MCL-5 create
/// collision guard). `None` when the card is unknown.
pub fn find_contact_by_native_id(
    conn: &Connection,
    source_id: &str,
    native_id: &str,
) -> Result<Option<ContactRow>, String> {
    conn.query_row(
        &format!("SELECT {CONTACT_COLS} FROM contacts WHERE source_id = ?1 AND native_id = ?2"),
        params![source_id, native_id],
        row_from_sqlite,
    )
    .optional()
    .map_err(|e| e.to_string())
}

/// The redacted event payload for one written contact
/// (MCL-5): the synced payload's 13 fields, plus the
/// content hash of the written fields and the write
/// Execution id. No display name, no phone or email
/// value, no job title, no address or birthday value —
/// the same allowlist rule as `synced_payload`.
fn write_payload(row: &ContactRow, execution_id: &str) -> String {
    let payload = serde_json::json!({
        "contact_id": row.contact_id,
        "source_id": row.source_id,
        "native_id": row.native_id,
        "display_name_hash": sha256_hex(row.display_name.as_bytes()),
        "organization_hash": sha256_hex(row.organization.as_bytes()),
        "phone_count": row.phones.len(),
        "phone_hash": string_list_hash(&row.phones),
        "email_count": row.emails.len(),
        "email_hash": string_list_hash(&row.emails),
        "has_address": row.addresses_hash.is_some(),
        "has_birthday": row.birthday_hash.is_some(),
        "created_ms": row.created_ms,
        "updated_ms": row.updated_ms,
        "content_hash": row.content_hash,
        "execution_id": execution_id,
    });
    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
}

/// Record one created contact (MCL-5) in a single
/// transaction: the row + its `contact.created` event.
/// Returns the row and whether it was inserted: when a
/// row already exists for the native id (a concurrent
/// create, or a lost response retried with a new key),
/// the existing row is the replay — no second row, no
/// second event, never a duplicate (the MCL-4 send
/// UNIQUE-race shape). A created card has no addresses
/// / birthday (the create script never sets them), so
/// both hashes stay NULL.
#[allow(clippy::too_many_arguments)]
pub fn record_contact_created(
    conn: &mut Connection,
    native_id: &str,
    content: &ContactContent,
    idempotency_key: &str,
    effect_id: &str,
    execution_id: &str,
    policy_version: &str,
    origin_device_id: &str,
) -> Result<(ContactRow, bool), String> {
    let now_ms = crate::sqlite_store::now_ms() as i64;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let digest = contact_create_digest(
        &content.display_name,
        &content.organization,
        &content.job_title,
        &content.phones,
        &content.emails,
    );
    if let Some(existing) =
        find_contact_by_idempotency_key(&tx, idempotency_key, "create", &digest, None)?
    {
        tx.commit().map_err(|e| e.to_string())?;
        return Ok((existing, false));
    }
    if let Some(existing) = find_contact_by_native_id(&tx, CONTACTS_SOURCE_ID, native_id)? {
        insert_contact_idempotency(
            &tx,
            idempotency_key,
            &existing.contact_id,
            "create",
            &digest,
            effect_id,
            now_ms,
            origin_device_id,
        )?;
        tx.commit().map_err(|e| e.to_string())?;
        return Ok((existing, false));
    }
    let next: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM contacts WHERE source_id = ?1",
            params![CONTACTS_SOURCE_ID],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())?;
    let contact_id = format!("contact-{}", next + 1);
    let phones_json = serde_json::to_string(&content.phones).unwrap_or_else(|_| "[]".to_string());
    let emails_json = serde_json::to_string(&content.emails).unwrap_or_else(|_| "[]".to_string());
    let row = ContactRow {
        contact_id,
        source_id: CONTACTS_SOURCE_ID.to_string(),
        native_id: native_id.to_string(),
        display_name: content.display_name.clone(),
        organization: content.organization.clone(),
        job_title: content.job_title.clone(),
        phones: content.phones.clone(),
        emails: content.emails.clone(),
        addresses_hash: None,
        birthday_hash: None,
        created_ms: now_ms,
        updated_ms: now_ms,
        deleted: false,
        origin_device_id: origin_device_id.to_string(),
        idempotency_key: Some(idempotency_key.to_string()),
        effect_id: Some(effect_id.to_string()),
        policy_version: Some(policy_version.to_string()),
        content_hash: contact_content_hash(
            &content.display_name,
            &content.organization,
            &content.job_title,
            &content.phones,
            &content.emails,
        ),
    };
    tx.execute(
        "INSERT INTO contacts (contact_id, source_id, native_id, display_name, organization, \
         job_title, phones_json, emails_json, addresses_hash, birthday_hash, created_ms, \
         updated_ms, deleted, origin_device_id, idempotency_key, effect_id, policy_version) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, NULL, ?9, ?9, 0, ?10, ?11, ?12, ?13)",
        params![
            row.contact_id,
            row.source_id,
            row.native_id,
            row.display_name,
            row.organization,
            row.job_title,
            phones_json,
            emails_json,
            now_ms,
            row.origin_device_id,
            idempotency_key,
            effect_id,
            policy_version,
        ],
    )
    .map_err(|e| e.to_string())?;
    insert_contact_idempotency(
        &tx,
        idempotency_key,
        &row.contact_id,
        "create",
        &digest,
        effect_id,
        now_ms,
        origin_device_id,
    )?;
    append_event(
        &tx,
        "contacts",
        &row.contact_id,
        "contact.created",
        &write_payload(&row, execution_id),
        origin_device_id,
        now_ms,
        DB_SCHEMA_VERSION,
    )?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok((row, true))
}

/// Record one updated contact (MCL-5) in a single
/// transaction: the full-replace field write + its
/// `contact.updated` event. The caller already ran the
/// live conflict re-read and the AppleScript replace;
/// this records the stored truth. Addresses / birthday
/// hashes are untouched (address / birthday writes are
/// deferred). The write audit trail advances to this
/// write's key / Effect / Policy version.
#[allow(clippy::too_many_arguments)]
pub fn record_contact_updated(
    conn: &mut Connection,
    row: &ContactRow,
    content: &ContactContent,
    idempotency_key: &str,
    effect_id: &str,
    execution_id: &str,
    policy_version: &str,
    origin_device_id: &str,
) -> Result<ContactRow, String> {
    let now_ms = crate::sqlite_store::now_ms() as i64;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let digest = contact_update_digest(
        &row.contact_id,
        &content.display_name,
        &content.organization,
        &content.job_title,
        &content.phones,
        &content.emails,
    );
    if let Some(existing) = find_contact_by_idempotency_key(
        &tx,
        idempotency_key,
        "update",
        &digest,
        Some(&row.contact_id),
    )? {
        tx.commit().map_err(|e| e.to_string())?;
        return Ok(existing);
    }
    let phones_json = serde_json::to_string(&content.phones).unwrap_or_else(|_| "[]".to_string());
    let emails_json = serde_json::to_string(&content.emails).unwrap_or_else(|_| "[]".to_string());
    tx.execute(
        "UPDATE contacts SET display_name = ?1, organization = ?2, job_title = ?3, \
         phones_json = ?4, emails_json = ?5, updated_ms = ?6, idempotency_key = ?7, \
         effect_id = ?8, policy_version = ?9 WHERE contact_id = ?10",
        params![
            content.display_name,
            content.organization,
            content.job_title,
            phones_json,
            emails_json,
            now_ms,
            idempotency_key,
            effect_id,
            policy_version,
            row.contact_id,
        ],
    )
    .map_err(|e| e.to_string())?;
    let updated = get_contact(&tx, &row.contact_id)?
        .ok_or_else(|| format!("contact {} vanished during its own update", row.contact_id))?;
    insert_contact_idempotency(
        &tx,
        idempotency_key,
        &updated.contact_id,
        "update",
        &digest,
        effect_id,
        now_ms,
        origin_device_id,
    )?;
    append_event(
        &tx,
        "contacts",
        &updated.contact_id,
        "contact.updated",
        &write_payload(&updated, execution_id),
        origin_device_id,
        now_ms,
        DB_SCHEMA_VERSION,
    )?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok(updated)
}

/// Record one deleted contact (MCL-5) in a single
/// transaction: the tombstone (`deleted = 1`,
/// `updated_ms` advances, text stays) + its
/// `contact.deleted` event. An already-tombstoned row
/// is returned unchanged — no second event (the mail
/// `tombstone_mail_message` precedent). The write audit
/// trail advances to this write's key / Effect / Policy
/// version, so a later replay can name the delete's
/// Effect.
#[allow(clippy::too_many_arguments)]
pub fn record_contact_deleted(
    conn: &mut Connection,
    row: &ContactRow,
    idempotency_key: &str,
    effect_id: &str,
    execution_id: &str,
    policy_version: &str,
    origin_device_id: &str,
) -> Result<ContactRow, String> {
    let now_ms = crate::sqlite_store::now_ms() as i64;
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let digest = contact_delete_digest(&row.contact_id);
    if let Some(existing) = find_contact_by_idempotency_key(
        &tx,
        idempotency_key,
        "delete",
        &digest,
        Some(&row.contact_id),
    )? {
        tx.commit().map_err(|e| e.to_string())?;
        return Ok(existing);
    }
    if row.deleted {
        tx.commit().map_err(|e| e.to_string())?;
        return Ok(row.clone());
    }
    tx.execute(
        "UPDATE contacts SET deleted = 1, updated_ms = ?1, idempotency_key = ?2, \
         effect_id = ?3, policy_version = ?4 WHERE contact_id = ?5",
        params![
            now_ms,
            idempotency_key,
            effect_id,
            policy_version,
            row.contact_id
        ],
    )
    .map_err(|e| e.to_string())?;
    let tombstoned = get_contact(&tx, &row.contact_id)?
        .ok_or_else(|| format!("contact {} vanished during its own delete", row.contact_id))?;
    insert_contact_idempotency(
        &tx,
        idempotency_key,
        &tombstoned.contact_id,
        "delete",
        &digest,
        effect_id,
        now_ms,
        origin_device_id,
    )?;
    append_event(
        &tx,
        "contacts",
        &tombstoned.contact_id,
        "contact.deleted",
        &write_payload(&tombstoned, execution_id),
        origin_device_id,
        now_ms,
        DB_SCHEMA_VERSION,
    )?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok(tombstoned)
}

/// List contacts for one source, tombstones visible
/// (6b-3 Q-14), insertion order. `limit` is clamped
/// to 50/200 by the caller.
pub fn list_contacts(
    conn: &Connection,
    source_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<ContactRow>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT contact_id, source_id, native_id, display_name, organization, job_title, \
             phones_json, emails_json, addresses_hash, birthday_hash, created_ms, updated_ms, \
             deleted, origin_device_id, idempotency_key, effect_id, policy_version FROM contacts WHERE source_id = ?1 \
             ORDER BY rowid ASC LIMIT ?2 OFFSET ?3",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![source_id, limit as i64, offset as i64],
            row_from_sqlite,
        )
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

/// One contact by id. `None` when the id is unknown.
pub fn get_contact(conn: &Connection, contact_id: &str) -> Result<Option<ContactRow>, String> {
    conn.query_row(
        "SELECT contact_id, source_id, native_id, display_name, organization, job_title, \
         phones_json, emails_json, addresses_hash, birthday_hash, created_ms, updated_ms, \
         deleted, origin_device_id, idempotency_key, effect_id, policy_version FROM contacts WHERE contact_id = ?1",
        params![contact_id],
        row_from_sqlite,
    )
    .optional()
    .map_err(|e| e.to_string())
}

/// Search contacts: SQLite list + in-memory filter over
/// display_name, phones, and emails (case-insensitive
/// substring, Q-MCL-11 plaintext fields only — address
/// and birthday values are never stored, so they cannot
/// be searched). The query is trimmed and lowercased
/// before matching. An empty query behaves like the
/// list route (same limit / offset). The filter runs
/// over the full table for the source — cards are
/// metadata-only and number in the hundreds, so no FTS
/// index is warranted in MCL-2 (plan §4: FTS later).
pub fn search_contacts(
    conn: &Connection,
    source_id: &str,
    query: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<ContactRow>, String> {
    let needle = query.trim().to_lowercase();
    if needle.is_empty() {
        return list_contacts(conn, source_id, limit, offset);
    }
    let mut stmt = conn
        .prepare(
            "SELECT contact_id, source_id, native_id, display_name, organization, job_title, \
             phones_json, emails_json, addresses_hash, birthday_hash, created_ms, updated_ms, \
             deleted, origin_device_id, idempotency_key, effect_id, policy_version FROM contacts WHERE source_id = ?1 ORDER BY rowid ASC",
        )
        .map_err(|e| e.to_string())?;
    let all = stmt
        .query_map(params![source_id], row_from_sqlite)
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    let hits: Vec<ContactRow> = all
        .into_iter()
        .filter(|row| {
            row.display_name.to_lowercase().contains(&needle)
                || row
                    .phones
                    .iter()
                    .any(|p| p.to_lowercase().contains(&needle))
                || row
                    .emails
                    .iter()
                    .any(|e| e.to_lowercase().contains(&needle))
        })
        .collect();
    Ok(hits.into_iter().skip(offset).take(limit).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contacts_source::ContactAddress;

    #[test]
    fn synced_payload_carries_hashes_and_counts_only() {
        // The event payload must be ids / hashes /
        // counts / timestamps only. No display name, no
        // phone or email value, no job title — and no
        // address or birthday value (they are never
        // stored at all).
        let row = ContactRow {
            contact_id: "contact-1".to_string(),
            source_id: "contacts-source".to_string(),
            native_id: "uuid:ABPerson".to_string(),
            display_name: "Ada Lovelace".to_string(),
            organization: "Analytical Engines".to_string(),
            job_title: "Mathematician".to_string(),
            phones: vec!["+15550101001".to_string()],
            emails: vec!["ada@mock.invalid".to_string()],
            addresses_hash: Some("aa".repeat(32)),
            birthday_hash: Some("bb".repeat(32)),
            created_ms: 1_000,
            updated_ms: 1_000,
            deleted: false,
            origin_device_id: "dev-1".to_string(),
            idempotency_key: None,
            effect_id: None,
            policy_version: None,
            content_hash: contact_content_hash(
                "Ada Lovelace",
                "Analytical Engines",
                "Mathematician",
                &["+15550101001".to_string()],
                &["ada@mock.invalid".to_string()],
            ),
        };
        let payload = synced_payload(&row);
        assert!(payload.contains("\"contact_id\":\"contact-1\""));
        assert!(payload.contains("\"native_id\":\"uuid:ABPerson\""));
        assert!(payload.contains("\"display_name_hash\""));
        assert!(payload.contains("\"organization_hash\""));
        assert!(payload.contains("\"phone_count\":1"));
        assert!(payload.contains("\"phone_hash\""));
        assert!(payload.contains("\"email_count\":1"));
        assert!(payload.contains("\"email_hash\""));
        assert!(payload.contains("\"has_address\":true"));
        assert!(payload.contains("\"has_birthday\":true"));
        assert!(!payload.contains("Ada Lovelace"), "no display name");
        assert!(!payload.contains("Analytical Engines"), "no organization");
        assert!(!payload.contains("Mathematician"), "no job title");
        assert!(!payload.contains("+15550101001"), "no phone value");
        assert!(!payload.contains("ada@mock.invalid"), "no email value");
        assert!(!payload.contains("addresses_hash"), "no address hash value");
        assert!(!payload.contains("birthday_hash"), "no birthday hash value");
    }

    #[test]
    fn synced_payload_flags_absent_address_and_birthday() {
        let row = ContactRow {
            contact_id: "contact-2".to_string(),
            source_id: "contacts-source".to_string(),
            native_id: "uuid2:ABPerson".to_string(),
            display_name: "Bob".to_string(),
            organization: String::new(),
            job_title: String::new(),
            phones: vec![],
            emails: vec![],
            addresses_hash: None,
            birthday_hash: None,
            created_ms: 1_000,
            updated_ms: 1_000,
            deleted: false,
            origin_device_id: "dev-1".to_string(),
            idempotency_key: None,
            effect_id: None,
            policy_version: None,
            content_hash: contact_content_hash("Bob", "", "", &[], &[]),
        };
        let payload = synced_payload(&row);
        assert!(payload.contains("\"has_address\":false"));
        assert!(payload.contains("\"has_birthday\":false"));
        assert!(payload.contains("\"phone_count\":0"));
        assert!(payload.contains("\"email_count\":0"));
    }

    #[test]
    fn address_hash_helper_matches_the_stored_hash() {
        let addr = ContactAddress {
            street: "1 Mock St".to_string(),
            city: "Springfield".to_string(),
            state: "IL".to_string(),
            postal_code: "62701".to_string(),
            country: "USA".to_string(),
        };
        assert_eq!(
            addresses_hash_for(&[addr]),
            addresses_hash_for(&[ContactAddress {
                street: "1 Mock St".to_string(),
                city: "Springfield".to_string(),
                state: "IL".to_string(),
                postal_code: "62701".to_string(),
                country: "USA".to_string(),
            }])
        );
    }
}
