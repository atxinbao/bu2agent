//! Execution records persisted as JSONL, mirroring the task store.
//!
//! Phase 1 (PA P0) shell: one line per creation; the last line for
//! an execution id wins on reload. A missing file means no
//! executions; malformed lines are skipped with a stderr warning.
//! Append failures are stderr warnings, never fatal: the in-memory
//! map stays authoritative for the run.
//!
//! Phase 1 creates records only: Task Runner and Loop Engine each
//! create one Execution when they start an attempt, keeping the old
//! task-N / loop-N id as a reference. No status transitions happen
//! in phase 1 (state convergence is phase 4); a record keeps the
//! status it was created with, including across restarts. There is
//! no `kind` field yet (draft §3 leaves it undecided) and no
//! workspace snapshot id (phase 3): exactly one of task_id /
//! loop_id is set, identifying the source attempt.

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionFileRecord {
    pub execution_id: String,
    /// Source task, if this execution is a task-runner attempt.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Source loop, if this execution is a loop attempt.
    #[serde(default)]
    pub loop_id: Option<String>,
    /// One of: running, awaiting_approval, needs_review,
    /// completed, failed, cancelled. Phase 1 only ever writes
    /// "running" at creation; transitions arrive with phase 4.
    pub status: String,
}

/// The four Q2 status groups. Terminal outcomes share one group so
/// a shell record can keep its outcome without phase 1 inventing a
/// converged state machine. Unknown strings map to None: the group
/// function must not launder an invalid status into a group.
pub fn execution_status_group(status: &str) -> Option<&'static str> {
    match status {
        "running" => Some("running"),
        "awaiting_approval" => Some("awaiting_approval"),
        "needs_review" => Some("needs_review"),
        "completed" | "failed" | "cancelled" => Some("terminal"),
        _ => None,
    }
}

pub fn valid_execution_status(status: &str) -> bool {
    execution_status_group(status).is_some()
}

pub fn load_executions(path: &Path) -> Vec<ExecutionFileRecord> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<ExecutionFileRecord>(line) {
            Ok(record) => out.push(record),
            Err(_) => {
                eprintln!(
                    "warning: skipping malformed execution record at line {} in {}",
                    index + 1,
                    path.display()
                );
            }
        }
    }
    out
}

pub fn append_execution(path: &Path, record: &ExecutionFileRecord) {
    let line = match serde_json::to_string(record) {
        Ok(line) => line,
        Err(_) => {
            eprintln!("warning: cannot serialize execution record {}", record.execution_id);
            return;
        }
    };
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("warning: cannot append execution record to {}: {e}", path.display());
            return;
        }
    };
    if let Err(e) = writeln!(file, "{line}") {
        eprintln!("warning: cannot write execution record to {}: {e}", path.display());
    }
}

/// Next execution number: one past the largest `execution-N` suffix seen, minimum 1.
pub fn next_execution_number(records: &[ExecutionFileRecord]) -> u64 {
    records
        .iter()
        .filter_map(|r| r.execution_id.strip_prefix("execution-")?.parse::<u64>().ok())
        .max()
        .map(|n| n + 1)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("mock-executions-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn load_missing_file_is_empty() {
        let dir = temp_dir("missing");
        let path = dir.join("executions.jsonl");
        assert!(load_executions(&path).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_and_reload_roundtrip() {
        let dir = temp_dir("roundtrip");
        let path = dir.join("executions.jsonl");
        append_execution(
            &path,
            &ExecutionFileRecord {
                execution_id: "execution-1".to_string(),
                task_id: Some("task-1".to_string()),
                loop_id: None,
                status: "running".to_string(),
            },
        );
        append_execution(
            &path,
            &ExecutionFileRecord {
                execution_id: "execution-1".to_string(),
                task_id: Some("task-1".to_string()),
                loop_id: None,
                status: "completed".to_string(),
            },
        );
        let records = load_executions(&path);
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].status, "completed");
        assert_eq!(next_execution_number(&records), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn malformed_lines_skipped_and_numbers_ignored() {
        let dir = temp_dir("malformed");
        let path = dir.join("executions.jsonl");
        std::fs::write(
            &path,
            "not json\n{\"execution_id\":\"execution-3\",\"task_id\":null,\"loop_id\":\"loop-2\",\"status\":\"running\"}\n",
        )
        .unwrap();
        let records = load_executions(&path);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].loop_id.as_deref(), Some("loop-2"));
        assert_eq!(next_execution_number(&records), 4);
        assert_eq!(next_execution_number(&[]), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn status_groups_cover_all_valid_values() {
        for (status, group) in [
            ("running", "running"),
            ("awaiting_approval", "awaiting_approval"),
            ("needs_review", "needs_review"),
            ("completed", "terminal"),
            ("failed", "terminal"),
            ("cancelled", "terminal"),
        ] {
            assert_eq!(execution_status_group(status), Some(group));
            assert!(valid_execution_status(status));
        }
        assert_eq!(execution_status_group("bogus"), None);
        assert!(!valid_execution_status("bogus"));
    }
}
