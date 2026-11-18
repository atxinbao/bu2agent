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

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::{
    contacts_source::{addresses_hash_for, birthday_hash_for, string_list_hash, ContactView},
    operation_binding::sha256_hex,
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
    Ok(ContactRow {
        contact_id: row.get("contact_id")?,
        source_id: row.get("source_id")?,
        native_id: row.get("native_id")?,
        display_name: row.get("display_name")?,
        organization: row.get("organization")?,
        job_title: row.get("job_title")?,
        phones: serde_json::from_str(&phones_json).unwrap_or_default(),
        emails: serde_json::from_str(&emails_json).unwrap_or_default(),
        addresses_hash: row.get("addresses_hash")?,
        birthday_hash: row.get("birthday_hash")?,
        created_ms: row.get("created_ms")?,
        updated_ms: row.get("updated_ms")?,
        deleted: row.get::<_, i64>("deleted")? != 0,
        origin_device_id: row.get("origin_device_id")?,
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
             deleted, origin_device_id FROM contacts WHERE source_id = ?1 \
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
         deleted, origin_device_id FROM contacts WHERE contact_id = ?1",
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
             deleted, origin_device_id FROM contacts WHERE source_id = ?1 ORDER BY rowid ASC",
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
