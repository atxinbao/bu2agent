//! Task records persisted as JSONL, mirroring the approval store.
//!
//! One line per mutation; the last line for a task id wins on reload.
//! A missing file means no tasks; malformed lines are skipped with a
//! stderr warning. Append failures are stderr warnings, never fatal:
//! the in-memory map stays authoritative for the run.

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// E2 slice, scheme A (P3-C plan §2.3): the frozen
/// Workspace/Worktree binding of a task's execute step.
/// Same three fields as the Execution snapshot binding
/// (`SnapshotBinding` in server.rs); the type lives here so
/// the task store does not import the server module.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskExecuteBinding {
    pub location_id: String,
    pub worktree_id: String,
    pub root_digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskFileRecord {
    pub task_id: String,
    pub directory: String,
    pub artifact_path: Option<String>,
    pub status: String,
    /// Runner-only failure reason, shown in the workbench. Manual
    /// tasks never set it; old JSONL lines load as None.
    #[serde(default)]
    pub error: Option<String>,
    /// Bound file-write approval for a gateway-run task, set at
    /// draft_ready. Manual tasks keep None and use the client-side
    /// approve path; the approval itself is a file-write
    /// authorization, not a task-state lock.
    #[serde(default)]
    pub approval_id: Option<String>,
    /// Draft identity: SHA-256 of the draft bytes and its length.
    /// The record never carries the draft body; the body lives in
    /// the task_drafts sidecar (see task_runner), keyed by task id.
    #[serde(default)]
    pub draft_sha256: Option<String>,
    #[serde(default)]
    pub draft_bytes: Option<u64>,
    /// E2 slice: frozen execute binding, stored on the first
    /// execute that names a worktree. A later execute naming
    /// a different worktree is 409
    /// `task_binding_frozen_mismatch`; old JSONL lines load
    /// as None (unfrozen).
    #[serde(default)]
    pub execute_binding: Option<TaskExecuteBinding>,
    /// D1-1: structural links to the scene the task serves.
    /// Optional, set at create or via update; old JSONL
    /// lines load as None (unlinked). Changing a link does
    /// not freeze anything — freezing is still
    /// `execute_binding`'s job.
    #[serde(default)]
    pub goal_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
}

pub fn load_tasks(path: &Path) -> Vec<TaskFileRecord> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<TaskFileRecord>(line) {
            Ok(record) => out.push(record),
            Err(_) => {
                eprintln!(
                    "warning: skipping malformed task record at line {} in {}",
                    index + 1,
                    path.display()
                );
            }
        }
    }
    out
}

pub fn append_task(path: &Path, record: &TaskFileRecord) {
    let line = match serde_json::to_string(record) {
        Ok(line) => line,
        Err(_) => {
            eprintln!("warning: cannot serialize task record {}", record.task_id);
            return;
        }
    };
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("warning: cannot append task record to {}: {e}", path.display());
            return;
        }
    };
    if let Err(e) = writeln!(file, "{line}") {
        eprintln!("warning: cannot write task record to {}: {e}", path.display());
    }
}

/// Next task number: one past the largest `task-N` suffix seen, minimum 1.
/// E2-A §2.2: the Task lifecycle edge table, mirroring
/// `valid_effect_transition` (effect_store.rs). The table is
/// the full runtime enumeration (full-repo grep, 2026-11-06):
/// 8 status words, 15 edges (13 at E2-A, +2 at the E2-A fix:
/// terminal re-run is legal — `completed→running` and
/// `failed→running`). Every runtime Task status write
/// goes through one of these edges; the manual
/// `POST /tasks/update` override is NOT governed by it (it is
/// a word-whitelist free setter, locked by task_smoke /
/// task_persistence / p0_snapshot, e.g. created→completed and
/// completed→rejected are legal there but are not runtime
/// edges). `fail()` contributes two edges the call sites hide
/// (interrupted→failed from restore_or_regenerate,
/// draft_ready→failed from the tasks_execute write-failure
/// path); the remaining eleven are the direct writes in
/// server.rs tasks_run/tasks_cancel/tasks_execute/tasks_reject,
/// task_runner run_task/restore_or_regenerate/generate_body,
/// and the with_tasks_path restart fold (running→interrupted).
pub fn valid_task_status_transition(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        ("created", "running")
            | ("running", "draft_ready")
            | ("running", "failed")
            | ("running", "cancelled")
            | ("running", "interrupted")
            | ("interrupted", "running")
            | ("interrupted", "draft_ready")
            | ("interrupted", "rejected")
            | ("interrupted", "failed")
            | ("draft_ready", "completed")
            | ("draft_ready", "rejected")
            | ("draft_ready", "cancelled")
            | ("draft_ready", "failed")
            | ("completed", "running")
            | ("failed", "running")
    )
}

pub fn next_task_number(records: &[TaskFileRecord]) -> u64 {
    records
        .iter()
        .filter_map(|r| r.task_id.strip_prefix("task-")?.parse::<u64>().ok())
        .max()
        .map(|n| n + 1)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("mock-tasks-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn load_missing_file_is_empty() {
        let dir = temp_dir("missing");
        let path = dir.join("tasks.jsonl");
        assert!(load_tasks(&path).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn append_and_reload_roundtrip() {
        let dir = temp_dir("roundtrip");
        let path = dir.join("tasks.jsonl");
        append_task(
            &path,
            &TaskFileRecord {
                task_id: "task-1".to_string(),
                directory: "notes".to_string(),
                artifact_path: None,
                status: "created".to_string(),
                error: None,
                approval_id: None,
                draft_sha256: None,
                draft_bytes: None,
                execute_binding: None,
                goal_id: None,
                project_id: None,
                workspace_id: None,
            },
        );
        append_task(
            &path,
            &TaskFileRecord {
                task_id: "task-1".to_string(),
                directory: "notes".to_string(),
                artifact_path: Some("notes/report.md".to_string()),
                status: "completed".to_string(),
                error: None,
                approval_id: None,
                draft_sha256: None,
                draft_bytes: None,
                execute_binding: None,
                goal_id: None,
                project_id: None,
                workspace_id: None,
            },
        );
        let records = load_tasks(&path);
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].status, "completed");
        assert_eq!(next_task_number(&records), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn next_number_skips_non_task_ids() {
        let records = vec![
            TaskFileRecord {
                task_id: "task-3".to_string(),
                directory: "a".to_string(),
                artifact_path: None,
                status: "created".to_string(),
                error: None,
                approval_id: None,
                draft_sha256: None,
                draft_bytes: None,
                execute_binding: None,
                goal_id: None,
                project_id: None,
                workspace_id: None,
            },
            TaskFileRecord {
                task_id: "other".to_string(),
                directory: "b".to_string(),
                artifact_path: None,
                status: "created".to_string(),
                error: None,
                approval_id: None,
                draft_sha256: None,
                draft_bytes: None,
                execute_binding: None,
                goal_id: None,
                project_id: None,
                workspace_id: None,
            },
        ];
        assert_eq!(next_task_number(&records), 4);
        assert_eq!(next_task_number(&[]), 1);
    }

    #[test]
    fn task_status_transition_table() {
        // All fifteen runtime edges accept; everything else
        // refuses, including the manual-override jumps the
        // /tasks/update route allows (created→completed,
        // completed→rejected) and identity transitions.
        for (from, to) in [
            ("created", "running"),
            ("running", "draft_ready"),
            ("running", "failed"),
            ("running", "cancelled"),
            ("running", "interrupted"),
            ("interrupted", "running"),
            ("interrupted", "draft_ready"),
            ("interrupted", "rejected"),
            ("interrupted", "failed"),
            ("draft_ready", "completed"),
            ("draft_ready", "rejected"),
            ("draft_ready", "cancelled"),
            ("draft_ready", "failed"),
            // E2-A fix: terminal re-run is legal.
            ("completed", "running"),
            ("failed", "running"),
        ] {
            assert!(valid_task_status_transition(from, to), "{from}→{to}");
        }
        for (from, to) in [
            ("created", "completed"),
            ("created", "draft_ready"),
            ("completed", "rejected"),
            ("failed", "created"),
            ("rejected", "failed"),
            ("cancelled", "created"),
            ("running", "running"),
            ("created", "created"),
            ("bogus", "running"),
            ("running", "bogus"),
        ] {
            assert!(!valid_task_status_transition(from, to), "{from}→{to}");
        }
    }
}
