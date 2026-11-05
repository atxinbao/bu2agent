//! Worktree records (P3-A, record-only, no git execution).
//!
//! A worktree is the concrete working directory a code-class
//! Execution uses: a Location plus a relative directory
//! inside it (`""` means the Location root itself).
//! `branch_label` is a display string only in P3-A — it
//! claims no git semantics — and `project_id` is an optional
//! reference to a Project. Archive is a status field, not a
//! tombstone: archived worktrees stay readable via get.
//!
//! Persistence mirrors the node store: one JSON object per
//! line, every mutation appends a full snapshot, and the last
//! line per id wins on reload.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const WORKTREE_ACTIVE: &str = "active";
pub const WORKTREE_ARCHIVED: &str = "archived";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Worktree {
    pub worktree_id: String,
    pub location_id: String,
    pub directory_relative: String,
    pub branch_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

fn numeric_suffix(id: &str, prefix: &str) -> u64 {
    id.strip_prefix(prefix).and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Next worktree number after the loaded records, mirroring
/// the node store's numbering helper.
pub fn next_worktree_number(worktrees: &[Worktree]) -> u64 {
    worktrees
        .iter()
        .map(|w| numeric_suffix(&w.worktree_id, "worktree-"))
        .max()
        .unwrap_or(0)
        + 1
}

/// Load worktree snapshots; malformed lines are skipped with
/// a warning, mirroring the other stores. Last line per id wins.
pub fn load_worktrees(path: &Path) -> Vec<Worktree> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut worktrees: Vec<Worktree> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Worktree>(line) {
            Ok(record) => {
                if let Some(existing) = worktrees
                    .iter_mut()
                    .find(|w| w.worktree_id == record.worktree_id)
                {
                    *existing = record;
                } else {
                    worktrees.push(record);
                }
            }
            Err(_) => eprintln!(
                "warning: skipping malformed worktree state line in {}",
                path.display()
            ),
        }
    }
    worktrees
}

/// Append one worktree snapshot; failures are stderr warnings
/// only, mirroring the other stores.
pub fn append_worktree(path: &Path, record: &Worktree) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append worktree state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(record).unwrap_or_default()) {
        eprintln!("warning: cannot append worktree state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worktree(id: &str) -> Worktree {
        Worktree {
            worktree_id: id.to_string(),
            location_id: "location-1".to_string(),
            directory_relative: String::new(),
            branch_label: String::new(),
            project_id: None,
            status: WORKTREE_ACTIVE.to_string(),
            created_ms: 0,
            updated_ms: 0,
        }
    }

    #[test]
    fn next_worktree_number_follows_max_suffix() {
        assert_eq!(next_worktree_number(&[]), 1);
        assert_eq!(
            next_worktree_number(&[worktree("worktree-2"), worktree("worktree-7")]),
            8
        );
    }
}
