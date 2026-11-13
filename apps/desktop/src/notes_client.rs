//! Notes client: Rust shell -> Mock Gateway notes
//! surface (D1-6b-4 read + write).
//!
//! Reads: notes list/get (rows carry `body_hash` only
//! — the body never enters SQLite, Q-D1-6b-16). Sync
//! is manual only (no auto-trigger before A1). Writes
//! (create / append / delete) go through the
//! digest-bound approval flow (approval_request →
//! decide → write) and use the long-timeout client
//! (the F-1 lesson: the gateway-side AppleScript write
//! can take 30+ s, and the notes runner cap is 60 s).

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// One notes fact row. The body is never in this
/// struct — only its hash (Q-D1-6b-16). The gateway's
/// extra `replayed` flag on create / append responses
/// is ignored on decode; the row is the fact record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteRow {
    pub note_id: String,
    pub source_id: String,
    pub native_id: String,
    pub folder: String,
    pub name: String,
    pub body_hash: String,
    pub idempotency_key: String,
    pub effect_id: String,
    pub policy_version: String,
    pub created_ms: i64,
    pub updated_ms: i64,
    pub deleted: bool,
    pub origin_device_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteSyncResult {
    pub synced_count: usize,
    pub skipped_count: usize,
    pub locked_count: usize,
    pub total_count: usize,
    pub cursor: String,
}

/// D1-6b-4: a create request. The idempotency key is
/// client-supplied and required (Q-D1-6b-05); the
/// approval id is the decided digest-bound approval
/// for this exact note (Q-D1-6b-22).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateNoteInput {
    pub folder: String,
    pub name: String,
    pub body: String,
    pub idempotency_key: String,
    pub tool_call_id: Option<String>,
}

/// D1-6b-4: an append request. The base hash is the
/// version the approver saw; the gateway re-reads and
/// 409s `note_changed` when the live text moved
/// (Q-D1-6b-20). The appended text is joined to the
/// live plaintext with no added separator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendNoteInput {
    pub text: String,
    pub base_body_hash: String,
    pub idempotency_key: String,
    pub tool_call_id: Option<String>,
}

/// D1-6b-4: one delete outcome (the gateway's
/// `NoteDeleteResponse` shape). `effect_id` is `None`
/// only on a tombstone replay whose original Effect
/// could not be found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteDeleteResponse {
    pub note_id: String,
    pub source_id: String,
    pub native_id: String,
    pub folder: String,
    pub deleted: bool,
    pub effect_id: Option<String>,
    pub replayed: bool,
}

/// D1-6b-4: sync / create / append / delete share the
/// 120 s bound (the F-1 lesson, same as mail
/// sync/send): the gateway-side AppleScript call can
/// block past the shared 10 s read timeout. List/get
/// keep the 10 s default.
const NOTES_WRITE_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct NotesClient {
    http: HttpClient,
    /// Long-timeout client (F-1): sync / create /
    /// append / delete. Reads share `http` and its 10 s
    /// default.
    write_http: HttpClient,
}

impl NotesClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            write_http: HttpClient::with_timeout(base_url, NOTES_WRITE_TIMEOUT_MS)?,
        })
    }

    pub fn sync_notes(&self, folder: &str) -> std::result::Result<NoteSyncResult, AgentError> {
        let body = serde_json::to_string(&serde_json::json!({"folder": folder}))
            .map_err(|_| crate::http_client::transport_error("cannot encode notes sync"))?;
        Ok(self.write_http.post_json("/api/notes/sync", &body)?)
    }

    /// The list includes tombstoned rows with
    /// `deleted: true` (the 6b-3 D-1 lesson: the
    /// approved plan decides list visibility).
    pub fn list_notes(&self) -> std::result::Result<Vec<NoteRow>, AgentError> {
        Ok(self.http.get_json("/api/notes")?)
    }

    /// One row by local id; a tombstoned row is still
    /// returned, with `deleted: true` (the goals-detail
    /// precedent).
    pub fn get_note(&self, note_id: &str) -> std::result::Result<NoteRow, AgentError> {
        Ok(self.http.get_json(&format!("/api/notes/{note_id}"))?)
    }

    pub fn create_note(
        &self,
        input: &CreateNoteInput,
    ) -> std::result::Result<NoteRow, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode note create"))?;
        Ok(self.write_http.post_json("/api/notes/create", &body)?)
    }

    pub fn append_note(
        &self,
        note_id: &str,
        input: &AppendNoteInput,
    ) -> std::result::Result<NoteRow, AgentError> {
        let body = serde_json::to_string(input)
            .map_err(|_| crate::http_client::transport_error("cannot encode note append"))?;
        Ok(self
            .write_http
            .post_json(&format!("/api/notes/{note_id}/append"), &body)?)
    }

    /// Delete one note — tombstone only, never purge
    /// (Q-D1-6b-21). The route is POST (plan §3), not
    /// DELETE; the `tool_call_id` travels as a query
    /// parameter because the desktop `delete_json`
    /// sends no body (the 6b-3 D-5 precedent).
    pub fn delete_note(
        &self,
        note_id: &str,
        tool_call_id: &str,
    ) -> std::result::Result<NoteDeleteResponse, AgentError> {
        Ok(self.write_http.post_json(
            &format!("/api/notes/{note_id}/delete?tool_call_id={tool_call_id}"),
            "{}",
        )?)
    }
}
