//! SyncEnvelope structure only. Real sync (calendar, contacts,
//! mail, notes, reminders) is deferred (baseline 4.6, 12).
//! Defining the envelope now avoids a later rework of the
//! baseline/cursor/ack shape.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncKind {
    Snapshot,
    Delta,
    Backfill,
    Search,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncEnvelope {
    pub source: String, // calendar | contacts | mail | notes | reminders
    pub schema: String, // e.g. "calendar|v1"
    pub scope: String,  // JSON text
    pub baseline_id: String,
    pub cursor: Option<String>,
    pub sequence: u64,
    pub kind: SyncKind,
    pub payload: String, // JSON text
    pub checksum: String,
}
