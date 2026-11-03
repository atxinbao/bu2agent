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
//!
//! P0-A (PA contract §2.1) only adds fields: the originating
//! conversation session, the current worker session and the locked
//! workspace snapshot (all still None in P0-A: sessions,
//! workers and snapshots land in later phases), plus the record
//! provenance every new record carries: `origin_device_id` and
//! `schema_version`. No status value, no gate and no creation
//! path changes here. Old lines without the new fields still
//! parse: they read as schema version 0 with origin "unknown",
//! never backfilled.

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// Origin reported for records written before P0-A added the
/// field, and for any line that omits it. Deliberately not a
/// device id: "unknown" means "no provenance recorded".
fn unknown_origin() -> String {
    "unknown".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionFileRecord {
    pub execution_id: String,
    /// Source task, if this execution is a task-runner attempt.
    #[serde(default)]
    pub task_id: Option<String>,
    /// Source loop, if this execution is a loop attempt.
    #[serde(default)]
    pub loop_id: Option<String>,
    /// Originating conversation session, when one exists. P0-A
    /// only reserves the field; no creation path sets it yet.
    #[serde(default)]
    pub session_id: Option<String>,
    /// Worker session currently driving this execution. P0-A
    /// only reserves the field; WorkerSession storage is later.
    #[serde(default)]
    pub worker_session_id: Option<String>,
    /// Locked workspace snapshot. P0-A only reserves the field;
    /// snapshots land in a later phase.
    #[serde(default)]
    pub workspace_snapshot_id: Option<String>,
    /// One of: running, awaiting_approval, needs_review,
    /// completed, failed, cancelled. Phase 1 only ever writes
    /// "running" at creation; transitions arrive with phase 4.
    pub status: String,
    /// Device that created this execution, resolved at gateway
    /// startup (see `resolve_origin_device_id`); "unknown" for
    /// pre-P0-A lines.
    #[serde(default = "unknown_origin")]
    pub origin_device_id: String,
    /// Contract schema version: 1 for rows written by P0-A or
    /// later, 0 for pre-P0-A rows read without the field.
    #[serde(default)]
    pub schema_version: u64,
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
                session_id: None,
                worker_session_id: None,
                workspace_snapshot_id: None,
                status: "running".to_string(),
                origin_device_id: "mac-local".to_string(),
                schema_version: 1,
            },
        );
        append_execution(
            &path,
            &ExecutionFileRecord {
                execution_id: "execution-1".to_string(),
                task_id: Some("task-1".to_string()),
                loop_id: None,
                session_id: None,
                worker_session_id: None,
                workspace_snapshot_id: None,
                status: "completed".to_string(),
                origin_device_id: "mac-local".to_string(),
                schema_version: 1,
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
    fn p0a_fields_roundtrip_and_old_lines_read_as_schema_zero() {
        let dir = temp_dir("p0a");
        let path = dir.join("executions.jsonl");
        append_execution(
            &path,
            &ExecutionFileRecord {
                execution_id: "execution-1".to_string(),
                task_id: Some("task-1".to_string()),
                loop_id: None,
                session_id: Some("session-1".to_string()),
                worker_session_id: None,
                workspace_snapshot_id: None,
                status: "running".to_string(),
                origin_device_id: "worker-9".to_string(),
                schema_version: 1,
            },
        );
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .expect("open for append");
            writeln!(
                f,
                "{{\"execution_id\":\"execution-2\",\"task_id\":null,\"loop_id\":\"loop-3\",\"status\":\"running\"}}"
            )
            .expect("append old line");
        }
        let records = load_executions(&path);
        assert_eq!(records.len(), 2);
        let new_row = &records[0];
        assert_eq!(new_row.session_id.as_deref(), Some("session-1"));
        assert_eq!(new_row.worker_session_id, None);
        assert_eq!(new_row.workspace_snapshot_id, None);
        assert_eq!(new_row.origin_device_id, "worker-9");
        assert_eq!(new_row.schema_version, 1);
        let old_row = &records[1];
        assert_eq!(old_row.session_id, None);
        assert_eq!(old_row.worker_session_id, None);
        assert_eq!(old_row.workspace_snapshot_id, None);
        assert_eq!(old_row.origin_device_id, "unknown");
        assert_eq!(old_row.schema_version, 0);
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
