//! Workspace binding records (P3-A, record subset).
//!
//! A binding is a user-explicitly-chosen directory: the
//! gateway canonicalizes it at creation and stores the
//! canonical root in `allowed_roots` (one root in P3-A; the
//! field stays an array for the P3-C multi-root extension).
//! `policy_digest` freezes the live policy at creation time
//! (see `server::compute_policy_digest`).
//!
//! Persistence mirrors the node store: one JSON object per
//! line, every mutation appends a full snapshot, and the last
//! line per id wins on reload. Archive is a status field, not
//! a tombstone: archived bindings stay readable via get and
//! keep their records, but no new Location may be created
//! under them.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const BINDING_ACTIVE: &str = "active";
pub const BINDING_ARCHIVED: &str = "archived";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub workspace_id: String,
    pub display_name: String,
    pub allowed_roots: Vec<String>,
    pub policy_digest: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

fn numeric_suffix(id: &str, prefix: &str) -> u64 {
    id.strip_prefix(prefix).and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Next workspace number after the loaded records, mirroring
/// the node store's numbering helper.
pub fn next_workspace_number(bindings: &[WorkspaceBinding]) -> u64 {
    bindings
        .iter()
        .map(|b| numeric_suffix(&b.workspace_id, "workspace-"))
        .max()
        .unwrap_or(0)
        + 1
}

/// Load binding snapshots; malformed lines are skipped with a
/// warning, mirroring the other stores. Last line per id wins.
pub fn load_bindings(path: &Path) -> Vec<WorkspaceBinding> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut bindings: Vec<WorkspaceBinding> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<WorkspaceBinding>(line) {
            Ok(record) => {
                if let Some(existing) = bindings
                    .iter_mut()
                    .find(|b| b.workspace_id == record.workspace_id)
                {
                    *existing = record;
                } else {
                    bindings.push(record);
                }
            }
            Err(_) => eprintln!(
                "warning: skipping malformed binding state line in {}",
                path.display()
            ),
        }
    }
    bindings
}

/// Append one binding snapshot; failures are stderr warnings
/// only, mirroring the other stores.
pub fn append_binding(path: &Path, record: &WorkspaceBinding) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append binding state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(record).unwrap_or_default()) {
        eprintln!("warning: cannot append binding state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(id: &str) -> WorkspaceBinding {
        WorkspaceBinding {
            workspace_id: id.to_string(),
            display_name: id.to_string(),
            allowed_roots: vec!["/tmp/root".to_string()],
            policy_digest: "abc".to_string(),
            status: BINDING_ACTIVE.to_string(),
            created_ms: 0,
            updated_ms: 0,
        }
    }

    #[test]
    fn next_workspace_number_follows_max_suffix() {
        assert_eq!(next_workspace_number(&[]), 1);
        assert_eq!(
            next_workspace_number(&[binding("workspace-2"), binding("workspace-7")]),
            8
        );
    }
}
