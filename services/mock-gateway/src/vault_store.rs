//! Vault state: credential slots, catalog entries, custom-connector
//! definitions, and external-provider capture tickets.
//!
//! This file holds METADATA ONLY. Secrets never live here, never
//! appear in a response, a log line, or an error: they live in the
//! OS keychain through `agent_desktop::credential_store` (production)
//! or the memory store (tests). A credential delete removes the
//! secret from the secret backend for real and appends a metadata
//! tombstone; catalog and custom-connector deletes are metadata
//! tombstones only, mirroring the connector store.
//!
//! Persistence mirrors connector_store.rs: tagged JSONL snapshots
//! in one file, loaded with last-write-wins. Capture tickets carry
//! no secret either, only the handle they will fill.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// A credential slot: one handle in the secret backend, plus the
/// metadata the list surface may show. `redacted` is computed at
/// response time from the backend, never stored here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialSlot {
    pub handle: String,
    pub provider: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

/// A catalog entry: provider/definition metadata, no secret.
/// Entries are created implicitly when a credential registers with
/// a new provider (display name defaults to the provider id) and
/// can then be renamed or deleted through the catalog routes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub id: String,
    pub display_name: String,
    pub description: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

/// A user-registered custom-connector definition. Metadata only;
/// custom connectors are NOT merged into the /connectors registry,
/// which still lists only the implemented Mock connectors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomConnectorDef {
    pub id: String,
    pub display_name: String,
    pub description: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TicketStatus {
    Pending,
    Fulfilled,
    Expired,
}

/// A pending external-provider capture: a local ticket, not a call
/// to any real provider. Fulfill stores the secret into the named
/// slot; the ticket itself never carries a secret.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureTicket {
    pub ticket_id: String,
    pub provider: String,
    pub handle: String,
    pub status: TicketStatus,
    pub created_ms: u64,
    pub expires_ms: u64,
}

/// Local capture-ticket lifetime: 15 minutes, a local choice.
pub const TICKET_TTL_MS: u64 = 15 * 60 * 1000;

pub fn ticket_expired(ticket: &CaptureTicket, now_ms: u64) -> bool {
    ticket.status == TicketStatus::Pending && now_ms >= ticket.expires_ms
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum VaultStateLine {
    #[serde(rename = "credential")]
    Credential(CredentialSlot),
    #[serde(rename = "catalog")]
    Catalog(CatalogEntry),
    #[serde(rename = "custom_connector")]
    CustomConnector(CustomConnectorDef),
    #[serde(rename = "ticket")]
    Ticket(CaptureTicket),
}

/// Load vault state; malformed lines are skipped with a warning,
/// mirroring the other stores. Callers fold the vectors into maps
/// with last-write-wins.
pub fn load_vault_state(
    path: &Path,
) -> (
    Vec<CredentialSlot>,
    Vec<CatalogEntry>,
    Vec<CustomConnectorDef>,
    Vec<CaptureTicket>,
) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    };
    let mut slots = Vec::new();
    let mut catalog = Vec::new();
    let mut custom = Vec::new();
    let mut tickets = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<VaultStateLine>(line) {
            Ok(VaultStateLine::Credential(record)) => slots.push(record),
            Ok(VaultStateLine::Catalog(record)) => catalog.push(record),
            Ok(VaultStateLine::CustomConnector(record)) => custom.push(record),
            Ok(VaultStateLine::Ticket(record)) => tickets.push(record),
            Err(_) => eprintln!("warning: skipping malformed vault state line in {}", path.display()),
        }
    }
    (slots, catalog, custom, tickets)
}

/// Append one state snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_vault_state(path: &Path, line: &VaultStateLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append vault state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append vault state to {}: {err}", path.display());
    }
}

pub fn next_ticket_number(tickets: &[CaptureTicket]) -> u64 {
    tickets
        .iter()
        .map(|t| t.ticket_id.strip_prefix("ticket-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0))
        .max()
        .unwrap_or(0)
        + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_lines_roundtrip() {
        let slot = CredentialSlot {
            handle: "验收-凭据A".to_string(),
            provider: Some("mock-provider".to_string()),
            created_ms: 7,
            updated_ms: 7,
            deleted: false,
        };
        let line = serde_json::to_string(&VaultStateLine::Credential(slot)).unwrap();
        assert!(line.contains("\"kind\":\"credential\""));
        let parsed: VaultStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, VaultStateLine::Credential(_)));
    }

    #[test]
    fn ticket_expiry_is_pending_only() {
        let base = CaptureTicket {
            ticket_id: "ticket-1".to_string(),
            provider: "mock-provider".to_string(),
            handle: "验收-凭据A".to_string(),
            status: TicketStatus::Pending,
            created_ms: 1000,
            expires_ms: 2000,
        };
        assert!(!ticket_expired(&base, 1999));
        assert!(ticket_expired(&base, 2000));
        let fulfilled = CaptureTicket { status: TicketStatus::Fulfilled, ..base.clone() };
        assert!(!ticket_expired(&fulfilled, 9999));
    }
}
