//! Connector registry state: Mock accounts and connection flags.
//!
//! The registry itself is static: the two Mock connectors the
//! gateway actually implements (notes and mail). Accounts and
//! connection flags persist as tagged JSONL snapshots in one
//! file, mirroring the session store: each mutation appends a
//! fresh snapshot, and loading folds lines into maps with
//! last-write-wins. There are no real accounts, no OAuth, and
//! no external state here; a "linked" account is a local record.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorAccount {
    pub account_id: String,
    pub connector_id: String,
    pub display_name: String,
    pub linked_ms: u64,
    #[serde(default)]
    pub unlinked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConnection {
    pub connector_id: String,
    pub connected: bool,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ConnectorStateLine {
    #[serde(rename = "account")]
    Account(ConnectorAccount),
    #[serde(rename = "connection")]
    Connection(ConnectorConnection),
}

/// Load connector state; malformed lines are skipped with a
/// warning, mirroring the other stores.
pub fn load_connector_state(path: &Path) -> (Vec<ConnectorAccount>, Vec<ConnectorConnection>) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (Vec::new(), Vec::new());
    };
    let mut accounts = Vec::new();
    let mut connections = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<ConnectorStateLine>(line) {
            Ok(ConnectorStateLine::Account(record)) => accounts.push(record),
            Ok(ConnectorStateLine::Connection(record)) => connections.push(record),
            Err(_) => eprintln!("warning: skipping malformed connector state line in {}", path.display()),
        }
    }
    (accounts, connections)
}

/// Append one state snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_connector_state(path: &Path, line: &ConnectorStateLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append connector state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append connector state to {}: {err}", path.display());
    }
}

pub fn next_account_number(accounts: &[ConnectorAccount]) -> u64 {
    accounts
        .iter()
        .map(|a| a.account_id.strip_prefix("account-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0))
        .max()
        .unwrap_or(0)
        + 1
}

pub fn account_number(account_id: &str) -> u64 {
    account_id.strip_prefix("account-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_lines_roundtrip() {
        let account = ConnectorAccount {
            account_id: "account-1".to_string(),
            connector_id: "notes".to_string(),
            display_name: "Mock account".to_string(),
            linked_ms: 7,
            unlinked: false,
        };
        let line = serde_json::to_string(&ConnectorStateLine::Account(account)).unwrap();
        assert!(line.contains("\"kind\":\"account\""));
        let parsed: ConnectorStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, ConnectorStateLine::Account(_)));
    }
}
