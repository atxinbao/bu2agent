//! Feed unit state: local feed records (Task 17, feed
//! unit subset).
//!
//! Only the unit record routes are implemented: list,
//! infinite (alias of list), status, unit get, unit
//! delete, unit reaction, unit tap, units seen. Prompt
//! get/update, prompts delete, regenerate, reorder,
//! search, social-source-icon, and unit media are
//! excluded: they imply a ranking/generation engine or
//! real sources (see the coverage table).
//!
//! Local semantics (no runtime evidence; not claimed as
//! original-product behavior):
//! - Units are clearly-labeled Mock seeds (Task 9
//!   notes/mail pattern); there is no creation route in
//!   the implemented subset, so the feed starts with
//!   three seeds and mutations act on them.
//! - Seeds always exist in memory; stored JSONL lines
//!   overlay them by unit_id (last-wins), so a seed's
//!   reacted/tapped/seen/deleted state survives restart
//!   when a state path is configured.
//! - Delete is a tombstone; list hides deleted units,
//!   get still returns the tombstoned record (peers
//!   precedent).
//! - Tap is an event: each tap increments tap_count.
//! - Seen is bulk best-effort: the response reports
//!   both marked ids and unknown ids (deleted units
//!   count as unknown).
//! - Persistence mirrors the space store: tagged JSONL
//!   snapshots in one file, last-write-wins.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedUnit {
    pub unit_id: String,
    pub title: String,
    pub body: String,
    pub source: String,
    pub seen: bool,
    pub reaction: Option<String>,
    pub tap_count: u64,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum FeedStateLine {
    #[serde(rename = "unit")]
    Unit(FeedUnit),
}

/// Clearly-labeled Mock seeds. created_ms values are
/// fixed so list order is deterministic.
pub fn seed_feed_units() -> Vec<FeedUnit> {
    vec![
        FeedUnit {
            unit_id: "feed-seed-1".to_string(),
            title: "[Mock] Welcome to your feed".to_string(),
            body: "This is a local Mock seed unit. Real feed ranking and sources are out of scope.".to_string(),
            source: "mock".to_string(),
            seen: false,
            reaction: None,
            tap_count: 0,
            created_ms: 1,
            updated_ms: 1,
            deleted: false,
        },
        FeedUnit {
            unit_id: "feed-seed-2".to_string(),
            title: "[Mock] Peers are now local records".to_string(),
            body: "Peer invites, inbox, and requests live in the gateway as tagged JSONL.".to_string(),
            source: "mock".to_string(),
            seen: false,
            reaction: None,
            tap_count: 0,
            created_ms: 2,
            updated_ms: 2,
            deleted: false,
        },
        FeedUnit {
            unit_id: "feed-seed-3".to_string(),
            title: "[Mock] Spaces are named collections".to_string(),
            body: "A space holds session/file references only, no rendering.".to_string(),
            source: "mock".to_string(),
            seen: false,
            reaction: None,
            tap_count: 0,
            created_ms: 3,
            updated_ms: 3,
            deleted: false,
        },
    ]
}

/// Load feed overlays; malformed lines are skipped with
/// a warning, mirroring the other stores.
pub fn load_feed_state(path: &Path) -> Vec<FeedUnit> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut units = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<FeedStateLine>(line) {
            Ok(FeedStateLine::Unit(record)) => units.push(record),
            Err(_) => eprintln!("warning: skipping malformed feed state line in {}", path.display()),
        }
    }
    units
}

/// Append one state snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_feed_state(path: &Path, line: &FeedStateLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append feed state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append feed state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeds_are_labeled_mock() {
        let seeds = seed_feed_units();
        assert_eq!(seeds.len(), 3);
        assert!(seeds.iter().all(|u| u.title.starts_with("[Mock]")));
        assert!(seeds.iter().all(|u| u.source == "mock"));
    }

    #[test]
    fn state_line_roundtrip() {
        let unit = seed_feed_units().into_iter().next().unwrap();
        let line = serde_json::to_string(&FeedStateLine::Unit(unit)).unwrap();
        assert!(line.contains("\"kind\":\"unit\""));
        let parsed: FeedStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, FeedStateLine::Unit(_)));
    }
}
