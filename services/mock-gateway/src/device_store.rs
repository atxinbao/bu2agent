//! Device action records persisted as JSONL, mirroring the task store.
//!
//! One line per mutation; the last line for an action id wins on reload
//! (the server folds lines into a map). A missing file means no actions;
//! malformed lines are skipped with a stderr warning. Append failures are
//! stderr warnings, never fatal: the in-memory map stays authoritative.
//!
//! Statuses: requested -> approved -> completed | rejected | failed |
//! unknown | awaiting_authorization. `unknown` means a write was submitted
//! but its result was not confirmed within the timeout; only a successful
//! re-query may resolve it to completed or failed. A failed re-query never
//! converts unknown into failed: the status stays unknown.

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceActionRecord {
    pub action_id: String,
    pub device: String,
    pub action: String,
    pub reminder_id: String,
    pub target_completed: bool,
    pub status: String,
    pub detail: Option<String>,
}

pub fn valid_action_status(status: &str) -> bool {
    matches!(
        status,
        "requested"
            | "approved"
            | "completed"
            | "rejected"
            | "failed"
            | "unknown"
            | "awaiting_authorization"
    )
}

pub fn load_actions(path: &Path) -> Vec<DeviceActionRecord> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<DeviceActionRecord>(line) {
            Ok(record) => out.push(record),
            Err(_) => {
                eprintln!(
                    "warning: skipping malformed action record at line {} in {}",
                    index + 1,
                    path.display()
                );
            }
        }
    }
    out
}

pub fn append_action(path: &Path, record: &DeviceActionRecord) {
    let line = match serde_json::to_string(record) {
        Ok(line) => line,
        Err(_) => {
            eprintln!("warning: cannot serialize action record {}", record.action_id);
            return;
        }
    };
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!(
                "warning: cannot append action record to {}: {e}",
                path.display()
            );
            return;
        }
    };
    if let Err(e) = writeln!(file, "{line}") {
        eprintln!(
            "warning: cannot write action record to {}: {e}",
            path.display()
        );
    }
}

/// Next action number: one past the largest `action-N` suffix seen, minimum 1.
pub fn next_action_number(records: &[DeviceActionRecord]) -> u64 {
    records
        .iter()
        .filter_map(|r| r.action_id.strip_prefix("action-")?.parse::<u64>().ok())
        .max()
        .map(|n| n + 1)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("mock-actions-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn record(id: &str, status: &str) -> DeviceActionRecord {
        DeviceActionRecord {
            action_id: id.to_string(),
            device: "mac-reminders".to_string(),
            action: "reminders.set_completed".to_string(),
            reminder_id: "reminder-1".to_string(),
            target_completed: true,
            status: status.to_string(),
            detail: None,
        }
    }

    #[test]
    fn load_missing_file_is_empty() {
        let dir = temp_dir("missing");
        let path = dir.join("actions.jsonl");
        assert!(load_actions(&path).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_and_reload_roundtrip() {
        let dir = temp_dir("roundtrip");
        let path = dir.join("actions.jsonl");
        append_action(&path, &record("action-1", "requested"));
        append_action(&path, &record("action-1", "completed"));
        let records = load_actions(&path);
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].status, "completed");
        assert_eq!(next_action_number(&records), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn next_number_skips_non_action_ids() {
        let records = vec![record("action-3", "completed"), record("other", "failed")];
        assert_eq!(next_action_number(&records), 4);
        assert_eq!(next_action_number(&[]), 1);
    }

    #[test]
    fn unknown_and_rejected_roundtrip() {
        let dir = temp_dir("lifecycle");
        let path = dir.join("actions.jsonl");
        append_action(&path, &record("action-1", "unknown"));
        append_action(&path, &record("action-2", "rejected"));
        let records = load_actions(&path);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].status, "unknown");
        assert_eq!(records[1].status, "rejected");
        assert!(valid_action_status("awaiting_authorization"));
        assert!(!valid_action_status("bogus"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
