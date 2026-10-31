//! Saved-space state: local v2 space records (Task 16,
//! spaces v2 subset).
//!
//! Only the v2 record routes are implemented: list, save,
//! delete (including the `?source=saved_space` variant).
//! Share/unshare/review/settings/fix, debug invocations/
//! log/summary, export_html, CVM render/get/post, and
//! subscribe are excluded: sharing backend, hosted app
//! runtime, and debug/export machinery have no local
//! counterpart (see the coverage table).
//!
//! Local semantics (no runtime evidence; not claimed as
//! original-product behavior):
//! - A space is a named collection; content is session/
//!   file references only, no rendering.
//! - Save upserts: creating a new slug or replacing the
//!   title/items of an existing one (including
//!   resurrecting a deleted slug as a fresh record).
//! - Delete is a tombstone; list hides deleted spaces.
//!   There is no GET-single route in the spec, so delete
//!   returns the tombstoned record.
//! - The `?source=saved_space` delete variant is accepted
//!   and has the same record effect as a plain delete.
//! - Persistence mirrors the peer store: tagged JSONL
//!   snapshots in one file, last-write-wins.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpaceItem {
    pub kind: String,
    #[serde(rename = "ref")]
    pub reference: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Space {
    pub slug: String,
    pub title: String,
    pub items: Vec<SpaceItem>,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SpaceStateLine {
    #[serde(rename = "space")]
    Space(Space),
}

/// Load space state; malformed lines are skipped with a
/// warning, mirroring the other stores.
pub fn load_space_state(path: &Path) -> Vec<Space> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut spaces = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<SpaceStateLine>(line) {
            Ok(SpaceStateLine::Space(record)) => spaces.push(record),
            Err(_) => eprintln!("warning: skipping malformed space state line in {}", path.display()),
        }
    }
    spaces
}

/// Append one state snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_space_state(path: &Path, line: &SpaceStateLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append space state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append space state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_line_roundtrip() {
        let space = Space {
            slug: "demo".to_string(),
            title: "Demo".to_string(),
            items: vec![SpaceItem { kind: "file".to_string(), reference: "notes/a.md".to_string() }],
            created_ms: 1,
            updated_ms: 2,
            deleted: false,
        };
        let line = serde_json::to_string(&SpaceStateLine::Space(space)).unwrap();
        assert!(line.contains("\"kind\":\"space\""));
        assert!(line.contains("\"ref\":\"notes/a.md\""));
        let parsed: SpaceStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, SpaceStateLine::Space(_)));
    }
}
