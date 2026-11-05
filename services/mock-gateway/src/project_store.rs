//! Project records (P3-A, record subset).
//!
//! A project is lightweight metadata: the stable work theme
//! ("about what") a Task or Patch may reference. It carries
//! no directory binding of its own. Archive is a status
//! field, not a tombstone: archived projects stay readable
//! via get, and existing references keep reading back with
//! `status: archived`.
//!
//! Persistence mirrors the node store: one JSON object per
//! line, every mutation appends a full snapshot, and the last
//! line per id wins on reload.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const PROJECT_ACTIVE: &str = "active";
pub const PROJECT_ARCHIVED: &str = "archived";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub project_id: String,
    pub name: String,
    pub description: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

fn numeric_suffix(id: &str, prefix: &str) -> u64 {
    id.strip_prefix(prefix).and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Next project number after the loaded records, mirroring
/// the node store's numbering helper.
pub fn next_project_number(projects: &[Project]) -> u64 {
    projects
        .iter()
        .map(|p| numeric_suffix(&p.project_id, "project-"))
        .max()
        .unwrap_or(0)
        + 1
}

/// Load project snapshots; malformed lines are skipped with a
/// warning, mirroring the other stores. Last line per id wins.
pub fn load_projects(path: &Path) -> Vec<Project> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut projects: Vec<Project> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Project>(line) {
            Ok(record) => {
                if let Some(existing) = projects
                    .iter_mut()
                    .find(|p| p.project_id == record.project_id)
                {
                    *existing = record;
                } else {
                    projects.push(record);
                }
            }
            Err(_) => eprintln!(
                "warning: skipping malformed project state line in {}",
                path.display()
            ),
        }
    }
    projects
}

/// Append one project snapshot; failures are stderr warnings
/// only, mirroring the other stores.
pub fn append_project(path: &Path, record: &Project) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append project state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(record).unwrap_or_default()) {
        eprintln!("warning: cannot append project state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(id: &str) -> Project {
        Project {
            project_id: id.to_string(),
            name: id.to_string(),
            description: "test project".to_string(),
            status: PROJECT_ACTIVE.to_string(),
            created_ms: 0,
            updated_ms: 0,
        }
    }

    #[test]
    fn next_project_number_follows_max_suffix() {
        assert_eq!(next_project_number(&[]), 1);
        assert_eq!(
            next_project_number(&[project("project-2"), project("project-7")]),
            8
        );
    }
}
