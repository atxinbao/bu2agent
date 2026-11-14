//! Outbox client: Rust shell -> Mock Gateway outbox endpoints.
//!
//! Outbox rows only: list, get, deliver, requeue.
//! FU-3B (Q-R0-01): the `/outbox` family had no production
//! client; this wires it for the WorkbenchView outbox section.
//! Rows live in SQLite only (P0-D); without a configured
//! SQLite database the routes answer 5xx fail-closed.
//! Localhost only, no TLS, Mock only -- same transport rules as
//! the other desktop clients.
//!
//! Delivery semantics (D1-3, verified against the gateway
//! handlers): `deliver` does NOT revive a `failed` row -- a
//! `sent`/`failed` row is returned unchanged (idempotent).
//! Only `requeue` revives a `failed` row, back to `pending`,
//! after which the sweeper tick or a manual deliver sends it.
//! Neither operation creates an Effect (#11/#12 are Outbox
//! operations, not Effects); the list is read-only otherwise.
//!
//! `payload_summary_json` is a JSON string, not an object;
//! callers display it as text (truncated in lists, full in
//! detail). Both POSTs take no body: the handlers take only
//! the Path id, so the empty-object body follows the existing
//! no-body POST precedent (e.g. sources reconnect, which
//! posts "{}" the same way).

use serde::{Deserialize, Serialize};

use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxRow {
    pub outbox_id: String,
    pub execution_id: String,
    pub payload_summary_json: String,
    pub destination_kind: String,
    pub status: String,
    pub attempts: i64,
    pub dedup_key: String,
    pub created_ms: i64,
    pub updated_at_ms: i64,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct OutboxClient {
    http: HttpClient,
}

impl OutboxClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list(&self) -> Result<Vec<OutboxRow>> {
        self.http.get_json("/outbox/list")
    }

    pub fn get(&self, outbox_id: &str) -> Result<OutboxRow> {
        self.http.get_json(&format!("/outbox/get?outbox_id={outbox_id}"))
    }

    pub fn deliver(&self, outbox_id: &str) -> Result<OutboxRow> {
        self.http.post_json(&format!("/outbox/{outbox_id}/deliver"), "{}")
    }

    pub fn requeue(&self, outbox_id: &str) -> Result<OutboxRow> {
        self.http.post_json(&format!("/outbox/{outbox_id}/requeue"), "{}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // FU-3B (Q-R0-01): the gateway emits the full OutboxRow;
    // the desktop record must decode every field, including
    // the i64 counters and timestamps and the JSON-string
    // payload summary.
    const FULL_JSON: &str = r#"{
        "outbox_id": "outbox-1", "execution_id": "execution-88",
        "payload_summary_json": "{\"kind\":\"mail.send\"}",
        "destination_kind": "mail", "status": "failed", "attempts": 2,
        "dedup_key": "dedup-1", "created_ms": 1794611497606,
        "updated_at_ms": 1794611497630
    }"#;

    const MINIMAL_JSON: &str = r#"{
        "outbox_id": "outbox-2", "execution_id": "execution-89",
        "payload_summary_json": "{}", "destination_kind": "calendar",
        "status": "pending", "attempts": 0, "dedup_key": "dedup-2",
        "created_ms": 1794611497700, "updated_at_ms": 1794611497700
    }"#;

    #[test]
    fn outbox_row_decodes_all_fields() {
        let row: OutboxRow = serde_json::from_str(FULL_JSON).expect("full row");
        assert_eq!(row.outbox_id, "outbox-1");
        assert_eq!(row.execution_id, "execution-88");
        assert_eq!(row.payload_summary_json, "{\"kind\":\"mail.send\"}");
        assert_eq!(row.destination_kind, "mail");
        assert_eq!(row.status, "failed");
        assert_eq!(row.attempts, 2);
        assert_eq!(row.dedup_key, "dedup-1");
        assert_eq!(row.created_ms, 1794611497606);
        assert_eq!(row.updated_at_ms, 1794611497630);
    }

    #[test]
    fn outbox_row_minimal_json_decodes() {
        let row: OutboxRow = serde_json::from_str(MINIMAL_JSON).expect("minimal row");
        assert_eq!(row.outbox_id, "outbox-2");
        assert_eq!(row.execution_id, "execution-89");
        assert_eq!(row.payload_summary_json, "{}");
        assert_eq!(row.destination_kind, "calendar");
        assert_eq!(row.status, "pending");
        assert_eq!(row.attempts, 0);
        assert_eq!(row.dedup_key, "dedup-2");
        assert_eq!(row.created_ms, 1794611497700);
        assert_eq!(row.updated_at_ms, 1794611497700);
    }
}
