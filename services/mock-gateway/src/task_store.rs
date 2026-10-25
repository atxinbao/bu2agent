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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskFileRecord {
    pub task_id: String,
    pub directory: String,
    pub artifact_path: Option<String>,
    pub status: String,
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
            },
        );
        append_task(
            &path,
            &TaskFileRecord {
                task_id: "task-1".to_string(),
                directory: "notes".to_string(),
                artifact_path: Some("notes/report.md".to_string()),
                status: "completed".to_string(),
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
            },
            TaskFileRecord {
                task_id: "other".to_string(),
                directory: "b".to_string(),
                artifact_path: None,
                status: "created".to_string(),
            },
        ];
        assert_eq!(next_task_number(&records), 4);
        assert_eq!(next_task_number(&[]), 1);
    }
}
