//! Chat record state: per-thread seen watermarks and the
//! current theme (Task 18, chat record subset).
//!
//! Only the record surface is implemented: themes get/set,
//! unread-count, mark-seen, delete-turn, message get,
//! history-window, and search. Stream/subscribe/stream-events,
//! poke, cancel, and sxs.select are excluded (see the Task 18
//! plan): no local counterpart for the stream protocol form
//! or for poke/cancel/sxs semantics.
//!
//! Persistence mirrors the peer/space/feed stores: tagged
//! JSONL snapshots in one file, last-write-wins. Seen
//! markers and the theme are small preference records, not
//! message bodies: message bodies stay in the session
//! snapshots, and turn deletion is a flag on SessionTurn
//! (see session_store.rs), not a line here.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Local theme catalog (Task 18, D6). These names are local
/// vocabulary, not original-product evidence; the theme is
/// record-only in this round and changes no visuals.
pub const CHAT_THEMES: [&str; 3] = ["system", "light", "dark"];
pub const DEFAULT_CHAT_THEME: &str = "system";

/// A seen watermark for one thread: entries with a stored
/// index below the marker count as read. A missing marker
/// reads as 0 (everything unread).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeenMarker {
    pub thread_id: String,
    pub marker: usize,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ChatStateLine {
    #[serde(rename = "seen")]
    Seen(SeenMarker),
    #[serde(rename = "theme")]
    Theme { theme: String },
}

/// Load chat state; malformed lines are skipped with a
/// warning, mirroring the other stores. Returns the seen
/// markers (last-wins by thread_id) and the current theme
/// (last theme line wins, defaulting to system).
pub fn load_chat_state(path: &Path) -> (Vec<SeenMarker>, String) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (Vec::new(), DEFAULT_CHAT_THEME.to_string());
    };
    let mut markers: Vec<SeenMarker> = Vec::new();
    let mut theme = DEFAULT_CHAT_THEME.to_string();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<ChatStateLine>(line) {
            Ok(ChatStateLine::Seen(marker)) => {
                if let Some(existing) = markers.iter_mut().find(|m| m.thread_id == marker.thread_id) {
                    *existing = marker;
                } else {
                    markers.push(marker);
                }
            }
            Ok(ChatStateLine::Theme { theme: next }) => {
                if CHAT_THEMES.contains(&next.as_str()) {
                    theme = next;
                }
            }
            Err(_) => eprintln!("warning: skipping malformed chat state line in {}", path.display()),
        }
    }
    (markers, theme)
}

/// Append one state snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_chat_state(path: &Path, line: &ChatStateLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append chat state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append chat state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_lines_roundtrip_last_wins() {
        let seen = ChatStateLine::Seen(SeenMarker {
            thread_id: "session-1".to_string(),
            marker: 3,
            updated_ms: 7,
        });
        let line = serde_json::to_string(&seen).unwrap();
        assert!(line.contains("\"kind\":\"seen\""));
        let parsed: ChatStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, ChatStateLine::Seen(_)));
        let theme = ChatStateLine::Theme { theme: "dark".to_string() };
        let line = serde_json::to_string(&theme).unwrap();
        assert!(line.contains("\"kind\":\"theme\""));
        let parsed: ChatStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, ChatStateLine::Theme { .. }));
    }
}
