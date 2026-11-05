//! Location records (P3-A, record subset).
//!
//! A location is a directory anchor under one workspace
//! binding: the canonical root the binding was created with
//! plus a fingerprint. `root_sha256` is the binding
//! fingerprint `sha256(canonical_json({workspace_id,
//! canonical_root}))` — it detects "the root was rebound to
//! a different directory", not content drift. Archive is a
//! status field, not a tombstone: archived locations stay
//! readable via get, but no new Worktree may be created
//! under them.
//!
//! Persistence mirrors the node store: one JSON object per
//! line, every mutation appends a full snapshot, and the last
//! line per id wins on reload.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

pub const LOCATION_ACTIVE: &str = "active";
pub const LOCATION_ARCHIVED: &str = "archived";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Location {
    pub location_id: String,
    pub workspace_id: String,
    pub name: String,
    pub canonical_root: String,
    pub root_sha256: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

fn numeric_suffix(id: &str, prefix: &str) -> u64 {
    id.strip_prefix(prefix).and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Next location number after the loaded records, mirroring
/// the node store's numbering helper.
pub fn next_location_number(locations: &[Location]) -> u64 {
    locations
        .iter()
        .map(|l| numeric_suffix(&l.location_id, "location-"))
        .max()
        .unwrap_or(0)
        + 1
}

/// Load location snapshots; malformed lines are skipped with
/// a warning, mirroring the other stores. Last line per id wins.
pub fn load_locations(path: &Path) -> Vec<Location> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut locations: Vec<Location> = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Location>(line) {
            Ok(record) => {
                if let Some(existing) = locations
                    .iter_mut()
                    .find(|l| l.location_id == record.location_id)
                {
                    *existing = record;
                } else {
                    locations.push(record);
                }
            }
            Err(_) => eprintln!(
                "warning: skipping malformed location state line in {}",
                path.display()
            ),
        }
    }
    locations
}

/// Append one location snapshot; failures are stderr warnings
/// only, mirroring the other stores.
pub fn append_location(path: &Path, record: &Location) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append location state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(record).unwrap_or_default()) {
        eprintln!("warning: cannot append location state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(id: &str) -> Location {
        Location {
            location_id: id.to_string(),
            workspace_id: "workspace-1".to_string(),
            name: id.to_string(),
            canonical_root: "/tmp/root".to_string(),
            root_sha256: "abc".to_string(),
            status: LOCATION_ACTIVE.to_string(),
            created_ms: 0,
            updated_ms: 0,
        }
    }

    #[test]
    fn next_location_number_follows_max_suffix() {
        assert_eq!(next_location_number(&[]), 1);
        assert_eq!(
            next_location_number(&[location("location-2"), location("location-7")]),
            8
        );
    }
}
