//! Chat stream records (Task 20, stream form subset).
//!
//! The six remaining chat routes are covered as a local record
//! surface, not as the original product's live stream protocol:
//!
//! - `POST /chat/subscribe` creates a subscription for a thread
//!   (thread_id == session_id, as in Task 18) and returns a
//!   cursor caught up to the thread's current events.
//! - `POST /chat/stream-events` pulls events after a cursor,
//!   long-polling in the fs.subscribe shape (wait_ms, capped,
//!   100ms re-checks). Events are a *projection* of the
//!   thread's visible turns plus injected pokes, not a live
//!   token stream: the mock loop answers synchronously with a
//!   whole reply, so `delta.text_append` carries full text.
//! - `POST /chat/cancel` cancels a thread's active
//!   subscriptions (delivery only, never an in-flight model
//!   call, which the synchronous mock loop does not have).
//! - `POST /chat/stream` opens a stream identity for a thread
//!   (subscription-style open; events still come from
//!   stream-events). It differs from the older
//!   `/chat/stream/start`, which mints records at once.
//! - `POST /chat/poke` injects a `delta.agent_status` poke
//!   event into a thread's event stream. The spec prose never
//!   mentions poke; this is a local definition.
//! - `POST /api/chat/sxs/select` records a selected turn per
//!   thread (last-wins, record-only: the spec has no read-back
//!   route, and `sxs` itself is unexplained in the spec).
//!
//! Persistence mirrors the other stores: one tagged JSON
//! object per line in stream.jsonl. Subscriptions are
//! snapshots (cancel flips the flag, last-wins); pokes and
//! stream opens are append-only audit lines; selections are
//! last-wins per thread. Events themselves are never stored:
//! they are derived from session turns plus poke lines, so
//! the event stream cannot drift from the turn list.
//!
//! Cursor stability: event ids are stable across reloads
//! and tombstones. Turn events use the turn's stored index
//! (stable per Task 18: turns are never removed, only
//! tombstoned), pokes use their file order. A cursor taken
//! before a turn delete still resumes correctly: deleted
//! turns keep their positions as anchors, their events are
//! just skipped on delivery.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::session_store::SessionTurn;

/// A subscription to one thread's event stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSubscription {
    pub subscription_id: String,
    pub thread_id: String,
    pub created_ms: u64,
    #[serde(default)]
    pub cancelled: bool,
}

/// A stream identity opened for a thread (audit record only;
/// events are still pulled through a subscription).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamOpen {
    pub stream_id: String,
    pub thread_id: String,
    pub created_ms: u64,
}

/// An injected poke event. `order` is the global file order
/// (1-based), which is what makes the event id stable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamPoke {
    pub thread_id: String,
    pub created_ms: u64,
    pub order: u64,
}

/// A thread's selected turn (last-wins per thread).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SxsSelection {
    pub thread_id: String,
    pub index: usize,
    pub updated_ms: u64,
}

/// One tagged line in stream.jsonl.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum StreamLine {
    #[serde(rename = "subscription")]
    Subscription(StreamSubscription),
    #[serde(rename = "stream-open")]
    StreamOpen(StreamOpen),
    #[serde(rename = "poke")]
    Poke(StreamPoke),
    #[serde(rename = "sxs-select")]
    SxsSelect(SxsSelection),
}

/// Everything loaded from stream.jsonl.
#[derive(Debug, Default)]
pub struct StreamStateData {
    pub subscriptions: Vec<StreamSubscription>,
    pub opens: Vec<StreamOpen>,
    pub pokes: Vec<StreamPoke>,
    pub selections: Vec<SxsSelection>,
}

fn numeric_suffix(id: &str, prefix: &str) -> u64 {
    id.strip_prefix(prefix).and_then(|s| s.parse().ok()).unwrap_or(0)
}

/// Next subscription number after the loaded records.
pub fn next_subscription_number(subscriptions: &[StreamSubscription]) -> u64 {
    subscriptions
        .iter()
        .map(|s| numeric_suffix(&s.subscription_id, "sub-"))
        .max()
        .unwrap_or(0)
        + 1
}

/// Next stream number after the loaded records.
pub fn next_stream_number(opens: &[StreamOpen]) -> u64 {
    opens
        .iter()
        .map(|o| numeric_suffix(&o.stream_id, "stream-"))
        .max()
        .unwrap_or(0)
        + 1
}

/// Next poke order after the loaded records.
pub fn next_poke_order(pokes: &[StreamPoke]) -> u64 {
    pokes.iter().map(|p| p.order).max().unwrap_or(0) + 1
}

/// Load stream lines; malformed lines are skipped with a
/// warning, mirroring the other stores. Last line per id
/// wins for subscriptions and per thread for selections;
/// pokes and opens keep file order.
pub fn load_stream_state(path: &Path) -> StreamStateData {
    let Ok(text) = std::fs::read_to_string(path) else {
        return StreamStateData::default();
    };
    let mut data = StreamStateData::default();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<StreamLine>(line) {
            Ok(StreamLine::Subscription(record)) => {
                if let Some(existing) = data
                    .subscriptions
                    .iter_mut()
                    .find(|s| s.subscription_id == record.subscription_id)
                {
                    *existing = record;
                } else {
                    data.subscriptions.push(record);
                }
            }
            Ok(StreamLine::StreamOpen(record)) => data.opens.push(record),
            Ok(StreamLine::Poke(record)) => data.pokes.push(record),
            Ok(StreamLine::SxsSelect(record)) => {
                if let Some(existing) = data
                    .selections
                    .iter_mut()
                    .find(|s| s.thread_id == record.thread_id)
                {
                    *existing = record;
                } else {
                    data.selections.push(record);
                }
            }
            Err(_) => eprintln!(
                "warning: skipping malformed stream state line in {}",
                path.display()
            ),
        }
    }
    data
}

/// Append one stream line; failures are stderr warnings
/// only, mirroring the other stores.
pub fn append_stream_line(path: &Path, line: &StreamLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append stream state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append stream state to {}: {err}", path.display());
    }
}

/// One projected event in a thread's stream, in delivery
/// order. `visible` is false for events of tombstoned turns:
/// they keep their positions as cursor anchors but are
/// never delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedEvent {
    pub id: String,
    pub event_type: String,
    pub turn_index: Option<usize>,
    pub text: Option<String>,
    pub created_ms: u64,
    pub visible: bool,
}

/// Project a thread's full event list: three events per
/// assistant turn (`delta.turn_start`, `delta.text_append`
/// with full text, `delta.message_done`) merged with the
/// thread's pokes (`delta.agent_status`). Order is by
/// creation time; turns sort before pokes at equal time,
/// then by stored turn index / phase, or poke order.
pub fn project_thread_events(
    turns: &[SessionTurn],
    pokes: &[StreamPoke],
    thread_id: &str,
) -> Vec<ProjectedEvent> {
    // (created_ms, kind_rank, sub_index, phase, event)
    let mut keyed: Vec<(u64, u8, usize, u8, ProjectedEvent)> = Vec::new();
    for (index, turn) in turns.iter().enumerate() {
        if turn.role != "assistant" {
            continue;
        }
        let visible = !turn.deleted;
        for (phase, event_type, text) in [
            (0u8, "delta.turn_start", None),
            (1u8, "delta.text_append", Some(turn.text.clone())),
            (2u8, "delta.message_done", None),
        ] {
            let phase_name = match phase {
                0 => "start",
                1 => "append",
                _ => "done",
            };
            keyed.push((
                turn.created_ms,
                0,
                index,
                phase,
                ProjectedEvent {
                    id: format!("turn:{index}:{phase_name}"),
                    event_type: event_type.to_string(),
                    turn_index: Some(index),
                    text,
                    created_ms: turn.created_ms,
                    visible,
                },
            ));
        }
    }
    for poke in pokes.iter().filter(|p| p.thread_id == thread_id) {
        keyed.push((
            poke.created_ms,
            1,
            poke.order as usize,
            0,
            ProjectedEvent {
                id: format!("poke:{}", poke.order),
                event_type: "delta.agent_status".to_string(),
                turn_index: None,
                text: Some("poke".to_string()),
                created_ms: poke.created_ms,
                visible: true,
            },
        ));
    }
    keyed.sort_by_key(|k| (k.0, k.1, k.2, k.3));
    keyed.into_iter().map(|(_, _, _, _, event)| event).collect()
}

/// Id of the last event in a projected list, or "" when
/// there are no events (the cursor for a fresh subscription
/// on an empty thread).
pub fn last_event_id(events: &[ProjectedEvent]) -> String {
    events.last().map(|e| e.id.clone()).unwrap_or_default()
}

/// Events deliverable after `cursor` ("" = from the start),
/// skipping events of deleted turns. Returns None when a
/// non-empty cursor names no event in the list.
pub fn events_after(events: &[ProjectedEvent], cursor: &str) -> Option<Vec<ProjectedEvent>> {
    let start = if cursor.is_empty() {
        0
    } else {
        let pos = events.iter().position(|e| e.id == cursor)?;
        pos + 1
    };
    Some(
        events[start..]
            .iter()
            .filter(|e| e.visible)
            .cloned()
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(role: &str, text: &str, created_ms: u64) -> SessionTurn {
        SessionTurn {
            role: role.to_string(),
            text: text.to_string(),
            created_ms,
            model: None,
            finish_reason: None,
            deleted: false,
        }
    }

    #[test]
    fn projection_maps_assistant_turns_and_merges_pokes() {
        let turns = vec![
            turn("user", "q0", 10),
            turn("assistant", "a0", 11),
            turn("assistant", "a1", 12),
        ];
        let pokes = vec![StreamPoke { thread_id: "t".to_string(), created_ms: 11, order: 1 }];
        let events = project_thread_events(&turns, &pokes, "t");
        // 2 assistant turns x 3 events + 1 poke; user turn yields none.
        assert_eq!(events.len(), 7);
        assert_eq!(events[0].id, "turn:1:start");
        assert_eq!(events[1].event_type, "delta.text_append");
        assert_eq!(events[1].text.as_deref(), Some("a0"));
        // Poke at ms 11 sorts after the turn at ms 11 (turns first).
        assert_eq!(events[3].id, "poke:1");
        assert_eq!(events[4].id, "turn:2:start");
        assert_eq!(last_event_id(&events), "turn:2:done");
    }

    #[test]
    fn cursor_survives_tombstone_without_duplicates() {
        let mut turns = vec![turn("assistant", "a0", 10), turn("assistant", "a1", 11)];
        let events = project_thread_events(&turns, &[], "t");
        let delivered = events_after(&events, "turn:0:done").expect("cursor known");
        assert_eq!(delivered.len(), 3);
        // Tombstone the first turn after delivery; the old cursor
        // still anchors, and no delivered event repeats.
        turns[0].deleted = true;
        let events = project_thread_events(&turns, &[], "t");
        let resumed = events_after(&events, "turn:0:done").expect("cursor still anchors");
        assert_eq!(resumed.len(), 3);
        assert!(resumed.iter().all(|e| e.turn_index == Some(1)));
        // Unknown cursor is rejected, not silently replayed.
        assert!(events_after(&events, "turn:9:start").is_none());
    }

    #[test]
    fn numbering_follows_max_suffix() {
        assert_eq!(next_subscription_number(&[]), 1);
        assert_eq!(
            next_subscription_number(&[StreamSubscription {
                subscription_id: "sub-7".to_string(),
                thread_id: "t".to_string(),
                created_ms: 0,
                cancelled: false,
            }]),
            8
        );
        assert_eq!(next_poke_order(&[]), 1);
    }
}
