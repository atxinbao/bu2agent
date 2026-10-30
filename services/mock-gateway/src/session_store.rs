//! Chat session records for Round 3 (3a: persistence + unified list).
//!
//! Sessions are deliberately separate from the audit JSONL files
//! (approvals/tasks/actions/loops): those promise never to hold
//! message bodies, while this store exists to hold them. A session
//! record is one JSON object per line; every mutation appends a
//! full snapshot, and the last line per id wins on reload,
//! mirroring the task store.
//!
//! Deletion is a tombstone, not erasure: a deleted session is
//! hidden from list/get/send and never enters model context, but
//! its earlier snapshot lines stay in the file until the file
//! itself is removed. The UI delete confirmation must say so;
//! never describe a UI delete as wiping the disk text.
//!
//! Context selection (user-confirmed defaults): the last 10
//! complete turns (a user message plus its following assistant
//! reply), at most 32 KiB total, trimmed oldest-first by whole
//! turns. A trailing user message without a reply is not a
//! complete turn and never enters context; the current input is
//! validated separately and never silently truncated.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Complete turns kept for model context.
pub const MAX_TURNS: usize = 10;
/// Total context bytes (UTF-8 bytes of both messages per turn).
pub const MAX_CONTEXT_BYTES: usize = 32 * 1024;
/// Single-message limit for a send; over it is a 400, never a
/// silent truncation.
pub const MAX_INPUT_BYTES: usize = 64 * 1024;
/// Title length in characters, from the first user message.
pub const TITLE_CHARS: usize = 40;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionTurn {
    /// "user" | "assistant".
    pub role: String,
    pub text: String,
    pub created_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub title: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub turns: Vec<SessionTurn>,
}

/// Load session snapshots; malformed lines are skipped with a
/// warning, mirroring the other stores.
pub fn load_sessions(path: &Path) -> Vec<SessionRecord> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut records = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<SessionRecord>(line) {
            Ok(record) => records.push(record),
            Err(_) => eprintln!("warning: skipping malformed session line in {}", path.display()),
        }
    }
    records
}

/// Append one full snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_session(path: &Path, record: &SessionRecord) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append session record to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(record).unwrap_or_default()) {
        eprintln!("warning: cannot append session record to {}: {err}", path.display());
    }
}

pub fn next_session_number(records: &[SessionRecord]) -> u64 {
    records
        .iter()
        .map(|r| r.session_id.strip_prefix("session-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0))
        .max()
        .unwrap_or(0)
        + 1
}

pub fn session_number(session_id: &str) -> u64 {
    session_id.strip_prefix("session-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

/// Title from the first user message, truncated by characters.
pub fn title_from(text: &str) -> String {
    text.chars().take(TITLE_CHARS).collect()
}

/// Complete turns (user + following assistant) for model context:
/// newest-first selection under both caps, returned oldest-first.
/// A pair that alone exceeds the byte cap is excluded, never cut.
pub fn select_context(turns: &[SessionTurn]) -> Vec<(String, String)> {
    let mut pairs: Vec<(usize, usize)> = Vec::new();
    let mut index = 0;
    while index < turns.len() {
        if turns[index].role == "user"
            && index + 1 < turns.len()
            && turns[index + 1].role == "assistant"
        {
            pairs.push((turns[index].text.len() + turns[index + 1].text.len(), index));
            index += 2;
        } else {
            index += 1;
        }
    }
    let mut kept: Vec<usize> = Vec::new();
    let mut bytes = 0;
    for (pair_bytes, pair_index) in pairs.iter().rev() {
        if kept.len() >= MAX_TURNS || bytes + pair_bytes > MAX_CONTEXT_BYTES {
            break;
        }
        bytes += pair_bytes;
        kept.push(*pair_index);
    }
    kept.reverse();
    let mut out = Vec::new();
    for pair_index in kept {
        out.push(("user".to_string(), turns[pair_index].text.clone()));
        out.push(("assistant".to_string(), turns[pair_index + 1].text.clone()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(role: &str, text: &str) -> SessionTurn {
        SessionTurn { role: role.to_string(), text: text.to_string(), created_ms: 0, model: None, finish_reason: None }
    }

    fn pair_turns(count: usize) -> Vec<SessionTurn> {
        let mut turns = Vec::new();
        for n in 0..count {
            turns.push(turn("user", &format!("q{n}")));
            turns.push(turn("assistant", &format!("a{n}")));
        }
        turns
    }

    #[test]
    fn context_keeps_last_ten_complete_turns() {
        let turns = pair_turns(12);
        let context = select_context(&turns);
        assert_eq!(context.len(), 20);
        assert_eq!(context[0].1, "q2");
        assert_eq!(context[19].1, "a11");
    }

    #[test]
    fn context_trims_by_bytes_oldest_first() {
        let mut turns = Vec::new();
        for n in 0..4 {
            turns.push(turn("user", &format!("q{n}")));
            turns.push(turn("assistant", &"x".repeat(10 * 1024)));
        }
        let context = select_context(&turns);
        assert_eq!(context.len(), 6);
        assert_eq!(context[0].1, "q1");
        assert_eq!(context[5].1, "x".repeat(10 * 1024));
    }

    #[test]
    fn context_skips_incomplete_trailing_user() {
        let mut turns = pair_turns(2);
        turns.push(turn("user", "pending"));
        let context = select_context(&turns);
        assert_eq!(context.len(), 4);
        assert!(!context.iter().any(|(_, text)| text == "pending"));
    }

    #[test]
    fn context_excludes_oversized_single_pair() {
        let turns = vec![turn("user", "q"), turn("assistant", &"x".repeat(40 * 1024))];
        assert!(select_context(&turns).is_empty());
    }

    #[test]
    fn title_truncates_by_characters() {
        assert_eq!(title_from(&"a".repeat(50)), "a".repeat(40));
        assert_eq!(title_from("你好世界"), "你好世界");
    }

    #[test]
    fn tombstone_defaults_to_not_deleted() {
        let record: SessionRecord =
            serde_json::from_str(r#"{"session_id":"session-1","title":"","created_ms":0,"updated_ms":0,"turns":[]}"#)
                .expect("parse");
        assert!(!record.deleted);
    }
}
