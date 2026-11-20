//! Contacts client: Rust shell -> Mock Gateway contacts
//! surface (MCL-2 read, MCL-5 write).
//!
//! Reads: sync, contacts list/get/search. No approval
//! loop (reads). Writes (MCL-5): create / update /
//! delete — each is a digest-bound approved write
//! (approval id in `tool_call_id`, client-supplied
//! idempotency key), executed against Contacts.app
//! gateway-side via AppleScript. On non-macOS the
//! gateway answers 501 and the error surfaces here
//! unchanged; the packaged Mac app writes real
//! Contacts.app data via AppleScript.
//!
//! PII shape (Q-MCL-11): display_name / organization /
//! job_title and phones / emails are PLAINTEXT here
//! (display, dial, and send need them); addresses /
//! birthday are hash / existence-flag only —
//! `addresses_hash` / `birthday_hash` are sha256 hexes
//! (NULL when the card has none), and no address /
//! birthday VALUE exists anywhere in this file.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// One `contacts` row (the gateway `ContactRow`
/// shape). The list includes tombstoned rows with
/// `deleted: true` (6b-3 Q-14); `contact_get`
/// likewise returns a tombstoned row with
/// `deleted: true`. `phones` / `emails` are JSON
/// arrays on the wire; `addresses_hash` /
/// `birthday_hash` are sha256 hexes or NULL (NULL is
/// the existence flag). `idempotency_key` /
/// `effect_id` / `policy_version` are NULL on synced
/// rows (a sync writes no Effect) and set on rows
/// written by MCL-5; `content_hash` is the sha256 of
/// the normalized writable content and is computed on
/// every row, synced or written. The write responses
/// also carry a `replayed` flag — serde drops it here
/// (the imessage `SentIMessageRow` precedent); the row
/// itself is the same either way.
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
    pub idempotency_key: Option<String>,
    pub effect_id: Option<String>,
    pub policy_version: Option<String>,
    pub content_hash: String,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub deleted: bool,
    pub origin_device_id: String,
}

/// MCL-5: a create request. The idempotency key is
/// client-supplied and required (Q-MCL-23, the 6b-1
/// shape); the approval id is the decided digest-bound
/// approval for this exact content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateContactInput {
    pub display_name: String,
    pub organization: String,
    pub job_title: String,
    pub phones: Vec<String>,
    pub emails: Vec<String>,
    pub idempotency_key: String,
    pub tool_call_id: Option<String>,
}

/// MCL-5: an update request — full replace of the
/// writable fields. `base_content_hash` is the hash of
/// the card the caller last read; if the live card
/// moved since, the gateway answers 409
/// `contact_changed` with the fresh hash and writes
/// nothing (the 6b-4 U6 shape).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateContactInput {
    pub display_name: String,
    pub organization: String,
    pub job_title: String,
    pub phones: Vec<String>,
    pub emails: Vec<String>,
    pub base_content_hash: String,
    pub idempotency_key: String,
    pub tool_call_id: Option<String>,
}

/// MCL-5: a delete request. The gateway tombstones the
/// row (`deleted: true`, still listed) — it never
/// hard-deletes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteContactInput {
    pub idempotency_key: String,
    pub tool_call_id: Option<String>,
}

/// MCL-2: one contacts sync pass (manual trigger only).
/// Counts + the contacts-source cursor; the gateway
/// records one row + one `contact.synced` event per new
/// contact in the same transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactsSyncResult {
    pub synced_count: usize,
    pub skipped_count: usize,
    pub total_count: usize,
    pub cursor: String,
}

/// MCL-2: sync runs AppleScript against Contacts.app
/// gateway-side and can take up to the 30 s osascript
/// cap, past the shared 10 s read timeout — the same
/// F-1 failure shape as mail sync (D1-6a). MCL-5: the
/// three writes run AppleScript too (a single-card
/// write takes ~0.25 s on the probe, but the gateway
/// cap is 120 s), so they share the long-timeout
/// client. Sync + writes alone use it; every other
/// contacts call keeps the 10 s default so a hung
/// gateway still fails fast instead of hanging the CLI.
const CONTACTS_SYNC_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct ContactsClient {
    http: HttpClient,
    /// Long-timeout client (F-1): sync and the three
    /// MCL-5 writes use it — the gateway-side
    /// AppleScript enumeration / write can take up to
    /// 30 s / 120 s. All other calls share `http` and
    /// its 10 s default.
    sync_http: HttpClient,
}

impl ContactsClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            sync_http: HttpClient::with_timeout(base_url, CONTACTS_SYNC_TIMEOUT_MS)?,
        })
    }

    pub fn sync_contacts(&self) -> std::result::Result<ContactsSyncResult, AgentError> {
        // F-1: sync alone uses the long-timeout client;
        // the gateway-side AppleScript enumeration can
        // take up to 30 s and the shared 10 s client
        // times out first while the gateway still finishes.
        Ok(self.sync_http.post_json("/api/contacts/sync", "{}")?)
    }

    /// List synced contacts (default 50, max 200
    /// gateway-side). Tombstones included, with
    /// `deleted: true`.
    pub fn contacts_list(
        &self,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> std::result::Result<Vec<ContactRow>, AgentError> {
        let mut path = "/api/contacts".to_string();
        let mut query = Vec::new();
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if let Some(offset) = offset {
            query.push(format!("offset={offset}"));
        }
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query.join("&"));
        }
        Ok(self.http.get_json(&path)?)
    }

    /// One synced contact by local `contact-N` id; a
    /// tombstoned row is still returned, with
    /// `deleted: true`.
    pub fn contact_get(&self, contact_id: &str) -> std::result::Result<ContactRow, AgentError> {
        Ok(self.http.get_json(&format!("/api/contacts/{contact_id}"))?)
    }

    /// Search synced contacts (display_name / phones /
    /// emails, case-insensitive substring — the gateway
    /// `/api/contacts/search` route). A missing or empty
    /// `q` behaves like the list route. Address /
    /// birthday values are never stored, so they cannot
    /// be searched.
    pub fn contacts_search(
        &self,
        query: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> std::result::Result<Vec<ContactRow>, AgentError> {
        let mut path = format!("/api/contacts/search?q={}", encode_query(query));
        let mut extra = Vec::new();
        if let Some(limit) = limit {
            extra.push(format!("limit={limit}"));
        }
        if let Some(offset) = offset {
            extra.push(format!("offset={offset}"));
        }
        if !extra.is_empty() {
            path.push('&');
            path.push_str(&extra.join("&"));
        }
        Ok(self.http.get_json(&path)?)
    }

    /// MCL-5: create a contact in Contacts.app
    /// (gateway-side AppleScript). Uses the 120 s
    /// client (the mail-send precedent): the gateway
    /// cap is 120 s. The approval id is the decided
    /// digest-bound `contacts.create` approval for this
    /// exact content; the idempotency key is
    /// client-supplied and required. A replay of the
    /// same key + same content returns the original row
    /// (the gateway marks it `replayed: true`; serde
    /// drops the flag here).
    pub fn create_contact(
        &self,
        input: &CreateContactInput,
    ) -> std::result::Result<ContactRow, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode contact create"))?;
        Ok(self.sync_http.post_json("/api/contacts/create", &body)?)
    }

    /// MCL-5: update a contact (full replace of the
    /// writable fields). `base_content_hash` is the
    /// hash the caller last read; a 409
    /// `contact_changed` (with `current_hash`) means
    /// the live card moved — re-read and retry, the
    /// approval is not burned.
    pub fn update_contact(
        &self,
        contact_id: &str,
        input: &UpdateContactInput,
    ) -> std::result::Result<ContactRow, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode contact update"))?;
        Ok(self
            .sync_http
            .post_json(&format!("/api/contacts/{contact_id}/update"), &body)?)
    }

    /// MCL-5: delete a contact. The gateway tombstones
    /// the row (`deleted: true`, still listed) — it
    /// never hard-deletes, and a re-delete replays the
    /// tombstone.
    pub fn delete_contact(
        &self,
        contact_id: &str,
        input: &DeleteContactInput,
    ) -> std::result::Result<ContactRow, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode contact delete"))?;
        Ok(self
            .sync_http
            .post_json(&format!("/api/contacts/{contact_id}/delete"), &body)?)
    }
}

/// Minimal query percent-encoding (no urlencoding crate in
/// this workspace): unreserved ASCII passes through, every
/// other byte is %XX (the chat_records_client.rs
/// precedent). The gateway trims and lowercases the
/// decoded query before matching.
fn encode_query(query: &str) -> String {
    let mut out = String::new();
    for byte in query.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_encoding_covers_spaces_and_unicode() {
        assert_eq!(encode_query("hello world"), "hello%20world");
        assert_eq!(encode_query("ada&bob"), "ada%26bob");
    }
}
