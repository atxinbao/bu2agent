//! Approval record persistence for the Mock Gateway (stage 2).
//!
//! Format: one JSON object per line (JSONL), appended on every
//! approval request and decision:
//! `{"tool_call_id":"approval-1","tool":"fs.write","status":"pending"}`
//! A record never contains secrets: tool names and statuses only.
//! Loading replays lines in order, so the last line for an id
//! wins (a decision overwrites the pending line that opened it).
//! Malformed lines are skipped with a stderr warning, never fatal:
//! a corrupt tail must not take the Mock down, and the in-memory
//! map stays authoritative for the running server. Append failures
//! are likewise best-effort warnings; the server keeps serving.
//!
//! The store is deliberately dumb: it knows files, not approval
//! semantics. The server maps records to its own state.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One line of the approvals file. Field names are snake_case to
/// match the HTTP DTOs elsewhere in this crate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalFileRecord {
    pub tool_call_id: String,
    pub tool: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<crate::loop_engine::ApprovalBinding>,
    #[serde(default)]
    pub consumed: bool,
}

/// Read every well-formed record from `path`, in file order.
/// A missing file loads as empty (first run). Malformed lines are
/// skipped with a warning on stderr.
 pub fn load_records(path: &Path) -> Vec<ApprovalFileRecord> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            eprintln!("warning: cannot read approvals file {}: {e}", path.display());
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<ApprovalFileRecord>(line) {
            Ok(record) => out.push(record),
            Err(_) => {
                eprintln!(
                    "warning: skipping malformed approvals line {} in {}",
                    index + 1,
                    path.display()
                );
            }
        }
    }
    out
}

/// Append one record as a single JSON line. Creates the file and
/// its parent directory on first use. Returns an error string on
/// failure; callers log it and keep serving.
pub fn append_record(path: &Path, record: &ApprovalFileRecord) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| format!("cannot create approvals dir: {e}"))?;
        }
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("cannot open approvals file: {e}"))?;
    let line =
        serde_json::to_string(record).map_err(|e| format!("cannot encode approval record: {e}"))?;
    file.write_all(line.as_bytes())
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|e| format!("cannot append approval record: {e}"))?;
    Ok(())
}

/// Next approval id number: one past the largest `approval-N`
/// suffix seen, or 1 when no usable id exists. Ids that do not
/// parse are ignored, so a corrupt line can never rewind the counter.
pub fn next_id_number(records: &[ApprovalFileRecord]) -> u64 {
    records
        .iter()
        .filter_map(|r| r.tool_call_id.strip_prefix("approval-")?.parse::<u64>().ok())
        .max()
        .map_or(1, |max| max + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, tool: &str, status: &str) -> ApprovalFileRecord {
        ApprovalFileRecord {
            tool_call_id: id.to_string(),
            tool: tool.to_string(),
            status: status.to_string(),
            binding: None,
            consumed: false,
        }
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("mock-approvals-{name}-{}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    #[test]
    fn append_then_load_roundtrips_in_order() {
        let path = temp_path("roundtrip");
        append_record(&path, &record("approval-1", "fs.write", "pending")).expect("append 1");
        append_record(&path, &record("approval-1", "fs.write", "approved")).expect("append 2");
        append_record(&path, &record("approval-2", "mail.send", "pending")).expect("append 3");
        let loaded = load_records(&path);
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].status, "pending");
        assert_eq!(loaded[1].status, "approved");
        assert_eq!(loaded[2].tool_call_id, "approval-2");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_file_loads_empty_and_bad_lines_are_skipped() {
        let path = temp_path("bad-lines");
        assert!(load_records(&path).is_empty());
        std::fs::write(
            &path,
            "{\"tool_call_id\":\"approval-1\",\"tool\":\"fs.write\",\"status\":\"pending\"}\nnot json\n\n{\"tool_call_id\":\"approval-2\",\"tool\":\"echo\",\"status\":\"pending\"}\n",
        )
        .expect("write fixture");
        let loaded = load_records(&path);
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[1].tool_call_id, "approval-2");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn next_id_number_ignores_unparsable_ids() {
        let records = vec![
            record("approval-1", "fs.write", "pending"),
            record("approval-7", "fs.write", "approved"),
            record("weird", "fs.write", "pending"),
        ];
        assert_eq!(next_id_number(&records), 8);
        assert_eq!(next_id_number(&[]), 1);
    }
}
