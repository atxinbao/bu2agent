//! Mock Gateway, chat state machine first.
//!
//! This is deliberately pure logic, not an HTTP server yet:
//! history, stream records, dedup, cancel and the approval gate
//! can be tested without a network. The Axum transport that
//! serves this state machine is the next step, and it must not
//! change any rule defined here.
//!
//! Rules pinned here (see ARCHITECTURE.md):
//! - stream sequences are strictly increasing, starting at 1
//! - a record with sequence <= last applied is a duplicate
//! - cancel stops the stream: later records are not applied
//! - unapproved sensitive calls are refused, even if the UI is bypassed

pub mod approval_store;
pub mod loop_engine;
pub mod loop_store;
pub mod deepseek;
pub mod deepseek_loop;
pub mod server;
pub mod task_store;
pub mod device_store;
pub mod device_backend;

// EventKit backend: macOS only. Other platforms compile without
// it; the gateway keeps the Fake backend there.
#[cfg(target_os = "macos")]
pub mod eventkit_backend;

use agent_desktop::approval::{self, ApprovalStatus};
use agent_desktop::gateway::SubscriptionRecord;
use agent_desktop::node::events;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatMessage {
    pub turn_id: String,
    pub role: String, // "user" | "assistant"
    pub text: String,
}

/// Seed history, mirroring services/mock-gateway/fixtures/chat-history.example.json.
pub fn seed_history() -> Vec<ChatMessage> {
    vec![
        ChatMessage {
            turn_id: "turn-seed-001".to_string(),
            role: "user".to_string(),
            text: "你好，我想先跑通聊天主线。".to_string(),
        },
        ChatMessage {
            turn_id: "turn-seed-001".to_string(),
            role: "assistant".to_string(),
            text: "好的，主线是：Rust 客户端壳 + TS 前端 + Mock Gateway。".to_string(),
        },
    ]
}

fn record(stream_id: &str, sequence: u64, event_type: &str, payload: &str, cursor: Option<&str>) -> SubscriptionRecord {
    SubscriptionRecord {
        stream_id: stream_id.to_string(),
        sequence,
        event_type: event_type.to_string(),
        payload: payload.to_string(),
        resume_cursor: cursor.map(|c| c.to_string()),
    }
}

/// Build one complete stream for a turn: turn_start, text_append,
/// agent_status(completed), message_done. Sequences 1..=4.
pub fn start_stream(stream_id: &str, turn_id: &str, text: &str) -> Vec<SubscriptionRecord> {
    vec![
        record(stream_id, 1, events::TURN_START, &serde_json::json!({"turnId": turn_id}).to_string(), Some("cursor-1")),
        record(stream_id, 2, events::TEXT_APPEND, &serde_json::json!({"turnId": turn_id, "text": text}).to_string(), Some("cursor-2")),
        record(stream_id, 3, events::AGENT_STATUS, &serde_json::json!({"turnId": turn_id, "status": "completed"}).to_string(), Some("cursor-3")),
        record(stream_id, 4, events::MESSAGE_DONE, &serde_json::json!({"turnId": turn_id}).to_string(), None),
    ]
}

/// Phase 3 tool approval flags, mirroring the desktop
/// `ToolRegistry::phase1` plus the Mock-only `echo` demo tool
/// (never a real tool). `None` means the tool is unknown.
pub fn tool_requires_approval(name: &str) -> Option<bool> {
    match name {
        "fs.list" | "fs.read" | "echo" | "notes.list" | "mail.list" => Some(false),
        "fs.write" | "fs.delete" | "notes.create" | "mail.send" | "reminders.set_completed" => Some(true),
        _ => None,
    }
}

/// Dedup: keep only records newer than `last_applied_sequence`.
/// Returns the kept records and the new last-applied sequence.
pub fn apply_records(
    records: &[SubscriptionRecord],
    last_applied_sequence: u64,
) -> (Vec<SubscriptionRecord>, u64) {
    let mut kept = Vec::new();
    let mut last = last_applied_sequence;
    for r in records {
        if r.sequence > last {
            last = r.sequence;
            kept.push(r.clone());
        }
    }
    (kept, last)
}

/// A cancellable stream. Once cancelled, no later record may be applied.
pub struct CancellableStream {
    pub cancelled: bool,
}

impl CancellableStream {
    pub fn new() -> Self {
        Self { cancelled: false }
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// False when the stream was cancelled: the caller must not
    /// write this record into the Store.
    pub fn may_apply(&self, _record: &SubscriptionRecord) -> bool {
        !self.cancelled
    }
}

impl Default for CancellableStream {
    fn default() -> Self {
        Self::new()
    }
}

/// Approval gate for a tool call. This is the Mock Gateway side
/// of the check: it must agree with the Rust client gate, and
/// it runs even when the frontend is bypassed.
pub fn may_execute_tool(status: ApprovalStatus, sensitive: bool) -> bool {
    approval::may_execute(status, sensitive)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_sequences_are_strictly_increasing_from_one() {
        let records = start_stream("stream-1", "turn-1", "hi");
        assert_eq!(records.len(), 4);
        let seqs: Vec<u64> = records.iter().map(|r| r.sequence).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
        assert_eq!(records[0].event_type, events::TURN_START);
        assert_eq!(records[1].event_type, events::TEXT_APPEND);
        assert_eq!(records[2].event_type, events::AGENT_STATUS);
        assert_eq!(records[3].event_type, events::MESSAGE_DONE);
        assert_eq!(records[3].resume_cursor, None);
    }

    #[test]
    fn dedup_drops_replayed_records() {
        let records = start_stream("stream-1", "turn-1", "hi");
        // Replay the whole stream after sequence 2 was applied.
        let (kept, last) = apply_records(&records, 2);
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].sequence, 3);
        assert_eq!(last, 4);
        // Replaying again applies nothing.
        let (kept2, last2) = apply_records(&records, last);
        assert!(kept2.is_empty());
        assert_eq!(last2, 4);
    }

    #[test]
    fn cancel_blocks_later_records() {
        let records = start_stream("stream-1", "turn-1", "hi");
        let mut stream = CancellableStream::new();
        assert!(stream.may_apply(&records[0]));
        stream.cancel();
        assert!(!stream.may_apply(&records[1]));
        assert!(!stream.may_apply(&records[3]));
    }

    #[test]
    fn mock_approval_gate_refuses_pending_sensitive_calls() {
        assert!(!may_execute_tool(ApprovalStatus::Pending, true));
        assert!(may_execute_tool(ApprovalStatus::Approved, true));
        assert!(may_execute_tool(ApprovalStatus::Pending, false));
    }

    /// Phase 3 tool approval flags, mirroring the desktop
    /// `ToolRegistry::phase1` plus the Mock-only `echo` demo tool
    /// (never a real tool). The server is the source of truth for
    /// execution; the client gate is defense in depth only.
    #[test]
    fn tool_approval_flags_match_phase1_registry() {
        assert_eq!(tool_requires_approval("fs.list"), Some(false));
        assert_eq!(tool_requires_approval("fs.read"), Some(false));
        assert_eq!(tool_requires_approval("fs.write"), Some(true));
        assert_eq!(tool_requires_approval("fs.delete"), Some(true));
        assert_eq!(tool_requires_approval("echo"), Some(false));
        assert_eq!(tool_requires_approval("notes.list"), Some(false));
        assert_eq!(tool_requires_approval("mail.list"), Some(false));
        assert_eq!(tool_requires_approval("notes.create"), Some(true));
        assert_eq!(tool_requires_approval("mail.send"), Some(true));
        assert_eq!(tool_requires_approval("reminders.set_completed"), Some(true));
        assert_eq!(tool_requires_approval("no.such.tool"), None);
    }

    #[test]
    fn seed_history_has_user_and_assistant_turn() {
        let history = seed_history();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].role, "user");
        assert_eq!(history[1].role, "assistant");
        assert_eq!(history[0].turn_id, history[1].turn_id);
    }
}
