//! Axum HTTP transport for the Mock Gateway.
//!
//! This module is additive only: it exposes the pure-logic rules
//! already defined in `lib.rs` (seed history, stream sequences
//! 1..=4, dedup, cancel, approval gate) over HTTP. It must not
//! change any of those rules. If a rule needs to change, change
//! `lib.rs` and its unit tests first, then re-check this module.
//!
//! Endpoints (all local, Mock only, no real auth):
//! - GET  /health                -> {"status":"ok"}
//! - GET  /chat/history          -> seed history
//! - POST /chat/stream/start    -> stream records 1..=4 for a turn
//! - POST /chat/stream/apply    -> deduped records + last applied
//! - POST /chat/stream/cancel   -> cancel a stream
//! - POST /approval/check       -> approval gate result
//! - GET  /files/list            -> list a directory in the file root
//! - GET  /files/read            -> chunked read (offset/len, base64, EOF)
//! - POST /files/write           -> write/append a file in the root
//! - POST /files/delete          -> delete a file in the root
//! - POST /approval/request      -> open a pending approval record (approval tools)
//! - POST /approval/decide       -> decide an approval record (approved/rejected/edited)
//! - POST /agent/tool/call       -> execute a tool, server re-checks its own approval record
//!
//! File scope: every file path is relative to one root (default
//! `./mock-files`, overridable with MOCK_FILES_ROOT). Absolute
//! paths and `..` are rejected, so a request can never escape
//! the root by path construction. Symlink escape is NOT checked
//! in this Mock -- the real client must check canonical paths.
//!
//! The server binds to 127.0.0.1:3001 by default (see `main.rs`,
//! overridable with MOCK_GATEWAY_ADDR). It is a Mock, never a
//! production gateway, and must not be exposed beyond localhost.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use agent_desktop::approval::ApprovalStatus;
use agent_desktop::gateway::SubscriptionRecord;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use tower_http::cors::{AllowOrigin, CorsLayer};
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::{apply_records, may_execute_tool, seed_history, start_stream, tool_requires_approval, ChatMessage, CancellableStream};

#[derive(Debug, Clone, Serialize)]
pub struct HistoryMessage {
    pub turn_id: String,
    pub role: String,
    pub text: String,
}

impl From<ChatMessage> for HistoryMessage {
    fn from(m: ChatMessage) -> Self {
        Self { turn_id: m.turn_id, role: m.role, text: m.text }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecordDto {
    pub stream_id: String,
    pub sequence: u64,
    pub event_type: String,
    pub payload: String,
    pub resume_cursor: Option<String>,
}

impl From<SubscriptionRecord> for StreamRecordDto {
    fn from(r: SubscriptionRecord) -> Self {
        Self {
            stream_id: r.stream_id,
            sequence: r.sequence,
            event_type: r.event_type,
            payload: r.payload,
            resume_cursor: r.resume_cursor,
        }
    }
}

impl From<StreamRecordDto> for SubscriptionRecord {
    fn from(d: StreamRecordDto) -> Self {
        Self {
            stream_id: d.stream_id,
            sequence: d.sequence,
            event_type: d.event_type,
            payload: d.payload,
            resume_cursor: d.resume_cursor,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StartRequest {
    pub stream_id: String,
    pub turn_id: String,
    pub text: String,
}

#[derive(Debug, Deserialize)]
pub struct ApplyRequest {
    pub stream_id: String,
    pub records: Vec<StreamRecordDto>,
}

#[derive(Debug, Serialize)]
pub struct ApplyResponse {
    pub kept: Vec<StreamRecordDto>,
    pub last_applied_sequence: u64,
}

#[derive(Debug, Deserialize)]
pub struct CancelRequest {
    pub stream_id: String,
}

#[derive(Debug, Serialize)]
pub struct CancelResponse {
    pub stream_id: String,
    pub cancelled: bool,
}

#[derive(Debug, Deserialize)]
pub struct ApprovalRequest {
    pub status: String,
    pub sensitive: bool,
}

#[derive(Debug, Serialize)]
pub struct ApprovalResponse {
    pub may_execute: bool,
}

struct StreamState {
    stream: CancellableStream,
    last_applied: u64,
}

struct ApprovalState {
    tool: String,
    status: ApprovalStatus,
}

#[derive(Debug, Clone, Serialize)]
struct MockNote {
    id: String,
    title: String,
    body: String,
}

#[derive(Debug, Clone, Serialize)]
struct MockMessage {
    id: String,
    from: String,
    to: String,
    subject: String,
    body: String,
}

fn seed_notes() -> Vec<MockNote> {
    vec![
        MockNote { id: "note-1".to_string(), title: "Mock note 1".to_string(), body: "Fake seed data for the Mock connector demo.".to_string() },
        MockNote { id: "note-2".to_string(), title: "Mock note 2".to_string(), body: "Created by the server, never read from a real account.".to_string() },
    ]
}

fn seed_messages() -> Vec<MockMessage> {
    vec![
        MockMessage { id: "mail-1".to_string(), from: "alice@mock.local".to_string(), to: "me@mock.local".to_string(), subject: "Mock welcome".to_string(), body: "Fake seed message.".to_string() },
        MockMessage { id: "mail-2".to_string(), from: "bob@mock.local".to_string(), to: "me@mock.local".to_string(), subject: "Mock reminder".to_string(), body: "Fake seed message.".to_string() },
    ]
}

#[derive(Clone)]
pub struct AppState {
    streams: Arc<Mutex<HashMap<String, StreamState>>>,
    files_root: PathBuf,
    approvals: Arc<Mutex<HashMap<String, ApprovalState>>>,
    next_approval_id: Arc<Mutex<u64>>,
    notes: Arc<Mutex<Vec<MockNote>>>,
    messages: Arc<Mutex<Vec<MockMessage>>>,
    next_note_id: Arc<Mutex<u64>>,
    next_message_id: Arc<Mutex<u64>>,
}

impl Default for AppState {
    fn default() -> Self {
        let root = std::env::var("MOCK_FILES_ROOT").unwrap_or_else(|_| "mock-files".to_string());
        Self {
            streams: Arc::new(Mutex::new(HashMap::new())),
            files_root: PathBuf::from(root),
            approvals: Arc::new(Mutex::new(HashMap::new())),
            next_approval_id: Arc::new(Mutex::new(1)),
            notes: Arc::new(Mutex::new(seed_notes())),
            messages: Arc::new(Mutex::new(seed_messages())),
            next_note_id: Arc::new(Mutex::new(3)),
            next_message_id: Arc::new(Mutex::new(3)),
        }
    }
}

pub fn new_state() -> AppState {
    AppState::default()
}

pub fn new_state_with_files_root(root: PathBuf) -> AppState {
    AppState {
        streams: Arc::new(Mutex::new(HashMap::new())),
        files_root: root,
        approvals: Arc::new(Mutex::new(HashMap::new())),
        next_approval_id: Arc::new(Mutex::new(1)),
        notes: Arc::new(Mutex::new(seed_notes())),
        messages: Arc::new(Mutex::new(seed_messages())),
        next_note_id: Arc::new(Mutex::new(3)),
        next_message_id: Arc::new(Mutex::new(3)),
    }
}

// --- File endpoints (Phase 2) ---

#[derive(Debug, Serialize)]
struct FileEntryDto {
    name: String,
    path: String,
    is_dir: bool,
    size_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadQuery {
    path: String,
    offset: Option<u64>,
    len: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ReadResponseDto {
    path: String,
    offset: u64,
    content_base64: String,
    eof: bool,
    total_size: u64,
    bytes_read: u64,
}

#[derive(Debug, Deserialize)]
struct WriteRequestDto {
    path: String,
    content_base64: String,
    append: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DeleteRequestDto {
    path: String,
}

fn resolve_file(root: &Path, rel: &str) -> Result<PathBuf, StatusCode> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if rel_path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(root.join(rel_path))
}

fn ensure_root(root: &Path) -> Result<(), StatusCode> {
    std::fs::create_dir_all(root).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn files_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let rel = query.path.unwrap_or_default();
    let Ok(dir) = resolve_file(&state.files_root, &rel) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if ensure_root(&state.files_root).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create file root" }))).into_response();
    }
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "directory not found" }))).into_response();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else { continue };
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = if rel.is_empty() { name.clone() } else { format!("{rel}/{name}") };
        out.push(FileEntryDto {
            name,
            path,
            is_dir: metadata.is_dir(),
            size_bytes: if metadata.is_dir() { 0 } else { metadata.len() },
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Json(out).into_response()
}

async fn files_read(
    State(state): State<AppState>,
    Query(query): Query<ReadQuery>,
) -> impl IntoResponse {
    let Ok(full) = resolve_file(&state.files_root, &query.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(data) = std::fs::read(&full) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response();
    };
    let offset = query.offset.unwrap_or(0);
    let len = query.len.unwrap_or(4096).min(1_048_576);
    let total = data.len() as u64;
    let start = (offset as usize).min(data.len());
    let end = start.saturating_add(len as usize).min(data.len());
    let chunk = &data[start..end];
    Json(ReadResponseDto {
        path: query.path,
        offset,
        content_base64: base64::engine::general_purpose::STANDARD.encode(chunk),
        eof: (end as u64) >= total,
        total_size: total,
        bytes_read: chunk.len() as u64,
    })
    .into_response()
}

async fn files_write(
    State(state): State<AppState>,
    Json(req): Json<WriteRequestDto>,
) -> impl IntoResponse {
    let Ok(full) = resolve_file(&state.files_root, &req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(data) = base64::engine::general_purpose::STANDARD.decode(&req.content_base64) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid base64 content" }))).into_response();
    };
    if let Some(parent) = full.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create parent directory" }))).into_response();
        }
    }
    let written: Result<u64, ()> = if req.append.unwrap_or(false) {
        (|| {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&full)
                .map_err(|_| ())?;
            f.write_all(&data).map_err(|_| ())?;
            Ok(std::fs::metadata(&full).map(|m| m.len()).unwrap_or(data.len() as u64))
        })()
    } else {
        std::fs::write(&full, &data).map(|_| data.len() as u64).map_err(|_| ())
    };
    match written {
        Ok(size) => Json(serde_json::json!({ "path": req.path, "size_bytes": size })).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot write file" }))).into_response(),
    }
}

async fn files_delete(
    State(state): State<AppState>,
    Json(req): Json<DeleteRequestDto>,
) -> impl IntoResponse {
    let Ok(full) = resolve_file(&state.files_root, &req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if full.is_dir() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "delete refuses directories" }))).into_response();
    }
    match std::fs::remove_file(&full) {
        Ok(()) => Json(serde_json::json!({ "deleted": true })).into_response(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot delete file" }))).into_response(),
    }
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn history() -> Json<Vec<HistoryMessage>> {
    Json(seed_history().into_iter().map(HistoryMessage::from).collect())
}

async fn stream_start(
    State(state): State<AppState>,
    Json(req): Json<StartRequest>,
) -> Json<Vec<StreamRecordDto>> {
    let records = start_stream(&req.stream_id, &req.turn_id, &req.text);
    let mut streams = state.streams.lock().expect("stream state lock");
    streams.insert(
        req.stream_id.clone(),
        StreamState { stream: CancellableStream::new(), last_applied: 0 },
    );
    Json(records.into_iter().map(StreamRecordDto::from).collect())
}

async fn stream_apply(
    State(state): State<AppState>,
    Json(req): Json<ApplyRequest>,
) -> impl IntoResponse {
    let mut streams = state.streams.lock().expect("stream state lock");
    let Some(stream_state) = streams.get_mut(&req.stream_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown stream" })),
        )
            .into_response();
    };
    if stream_state.stream.cancelled {
        // Cancel rule from lib.rs: no later record may be applied.
        return Json(serde_json::json!({
            "kept": [],
            "last_applied_sequence": stream_state.last_applied,
        }))
        .into_response();
    }
    let records: Vec<SubscriptionRecord> =
        req.records.into_iter().map(SubscriptionRecord::from).collect();
    let (kept, last) = apply_records(&records, stream_state.last_applied);
    stream_state.last_applied = last;
    Json(ApplyResponse {
        kept: kept.into_iter().map(StreamRecordDto::from).collect(),
        last_applied_sequence: last,
    })
    .into_response()
}

async fn stream_cancel(
    State(state): State<AppState>,
    Json(req): Json<CancelRequest>,
) -> impl IntoResponse {
    let mut streams = state.streams.lock().expect("stream state lock");
    let Some(stream_state) = streams.get_mut(&req.stream_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown stream" })),
        )
            .into_response();
    };
    stream_state.stream.cancel();
    Json(CancelResponse { stream_id: req.stream_id, cancelled: true }).into_response()
}

fn parse_approval_status(s: &str) -> Option<ApprovalStatus> {
    match s {
        "not_required" => Some(ApprovalStatus::NotRequired),
        "pending" => Some(ApprovalStatus::Pending),
        "approved" => Some(ApprovalStatus::Approved),
        "rejected" => Some(ApprovalStatus::Rejected),
        "edited" => Some(ApprovalStatus::Edited),
        _ => None,
    }
}

async fn approval_check(Json(req): Json<ApprovalRequest>) -> impl IntoResponse {
    let Some(status) = parse_approval_status(&req.status) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "unknown approval status" })),
        )
            .into_response();
    };
    Json(ApprovalResponse { may_execute: may_execute_tool(status, req.sensitive) }).into_response()
}

// --- Agent invocation + approval loop (Phase 3) ---

#[derive(Debug, Deserialize)]
struct ApprovalRequestDto {
    tool: String,
}

#[derive(Debug, Serialize)]
struct ApprovalRequestResponse {
    tool_call_id: Option<String>,
    requires_approval: bool,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ApprovalDecideDto {
    tool_call_id: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ToolCallDto {
    tool: String,
    args: HashMap<String, String>,
    tool_call_id: Option<String>,
}

fn bad_request(message: &str) -> axum::response::Response {
    (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": message }))).into_response()
}

async fn approval_request(
    State(state): State<AppState>,
    Json(req): Json<ApprovalRequestDto>,
) -> impl IntoResponse {
    let Some(requires) = tool_requires_approval(&req.tool) else {
        return bad_request("unknown tool");
    };
    if !requires {
        return Json(ApprovalRequestResponse {
            tool_call_id: None,
            requires_approval: false,
            status: "not_required".to_string(),
        })
        .into_response();
    }
    let mut next = state.next_approval_id.lock().expect("approval id lock");
    let id = format!("approval-{}", *next);
    *next += 1;
    state.approvals.lock().expect("approval lock").insert(
        id.clone(),
        ApprovalState { tool: req.tool, status: ApprovalStatus::Pending },
    );
    Json(ApprovalRequestResponse {
        tool_call_id: Some(id),
        requires_approval: true,
        status: "pending".to_string(),
    })
    .into_response()
}

async fn approval_decide(
    State(state): State<AppState>,
    Json(req): Json<ApprovalDecideDto>,
) -> impl IntoResponse {
    let Some(status) = parse_approval_status(&req.status) else {
        return bad_request("unknown approval status");
    };
    if !matches!(status, ApprovalStatus::Approved | ApprovalStatus::Rejected | ApprovalStatus::Edited) {
        return bad_request("decision must be approved, rejected or edited");
    }
    let mut approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get_mut(&req.tool_call_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown tool_call_id" }))).into_response();
    };
    record.status = status;
    Json(serde_json::json!({ "tool_call_id": req.tool_call_id, "status": req.status })).into_response()
}

/// Shared approval gate for agent tools and connector writes:
/// the server trusts only its own record, never the client's
/// claim. Returns Err(403) for no record, pending, rejected,
/// or a record for a different tool.
/// The error is boxed: an axum Response is large, and a
/// bare Result<_, Response> trips clippy's large-error lint.
fn check_approval(
    state: &AppState,
    tool: &str,
    tool_call_id: &Option<String>,
) -> Result<(), Box<axum::response::Response>> {
    let Some(id) = tool_call_id else {
        return Err(Box::new((StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "approval_required" }))).into_response()));
    };
    let approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get(id) else {
        return Err(Box::new((StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "approval_required" }))).into_response()));
    };
    if record.tool != tool || !may_execute_tool(record.status, true) {
        let error = if record.status == ApprovalStatus::Rejected {
            "approval_rejected"
        } else {
            "approval_required"
        };
        return Err(Box::new((StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": error }))).into_response()));
    }
    Ok(())
}

async fn agent_tool_call(
    State(state): State<AppState>,
    Json(req): Json<ToolCallDto>,
) -> impl IntoResponse {
    let Some(requires) = tool_requires_approval(&req.tool) else {
        return bad_request("unknown tool");
    };
    if requires {
        if let Err(response) = check_approval(&state, &req.tool, &req.tool_call_id) {
            return *response;
        }
    }
    let arg = |name: &str| req.args.get(name).cloned();
    let result: Result<serde_json::Value, axum::response::Response> = match req.tool.as_str() {
        "echo" => Ok(serde_json::json!(arg("text").unwrap_or_default())),
        "fs.list" => {
            let rel = arg("path").unwrap_or_default();
            match resolve_file(&state.files_root, &rel) {
                Err(_) => Err(bad_request("path escapes file root")),
                Ok(dir) => {
                    if ensure_root(&state.files_root).is_err() {
                        Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create file root" }))).into_response())
                    } else {
                        match std::fs::read_dir(&dir) {
                            Err(_) => Err((StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "directory not found" }))).into_response()),
                            Ok(entries) => {
                                let mut out = Vec::new();
                                for entry in entries.flatten() {
                                    let Ok(metadata) = entry.metadata() else { continue };
                                    let name = entry.file_name().to_string_lossy().into_owned();
                                    let path = if rel.is_empty() { name.clone() } else { format!("{rel}/{name}") };
                                    out.push(serde_json::json!({
                                        "name": name,
                                        "path": path,
                                        "is_dir": metadata.is_dir(),
                                        "size_bytes": if metadata.is_dir() { 0 } else { metadata.len() },
                                    }));
                                }
                                out.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
                                Ok(serde_json::Value::Array(out))
                            }
                        }
                    }
                }
            }
        }
        "fs.read" => {
            let Some(path) = arg("path") else { return bad_request("fs.read requires a path arg") };
            match resolve_file(&state.files_root, &path) {
                Err(_) => Err(bad_request("path escapes file root")),
                Ok(full) => match std::fs::read_to_string(&full) {
                    Ok(text) => Ok(serde_json::json!(text)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        Err((StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response())
                    }
                    Err(_) => Err(bad_request("file is not valid UTF-8 text")),
                },
            }
        }
        "fs.write" => {
            let (Some(path), Some(text)) = (arg("path"), arg("text")) else {
                return bad_request("fs.write requires path and text args");
            };
            match resolve_file(&state.files_root, &path) {
                Err(_) => Err(bad_request("path escapes file root")),
                Ok(full) => {
                    if let Some(parent) = full.parent() {
                        if std::fs::create_dir_all(parent).is_err() {
                            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create parent directory" }))).into_response();
                        }
                    }
                    match std::fs::write(&full, text.as_bytes()) {
                        Ok(()) => Ok(serde_json::json!({ "path": path, "size_bytes": text.len() })),
                        Err(_) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot write file" }))).into_response()),
                    }
                }
            }
        }
        "fs.delete" => {
            let Some(path) = arg("path") else { return bad_request("fs.delete requires a path arg") };
            match resolve_file(&state.files_root, &path) {
                Err(_) => Err(bad_request("path escapes file root")),
                Ok(full) => {
                    if full.is_dir() {
                        Err(bad_request("delete refuses directories"))
                    } else {
                        match std::fs::remove_file(&full) {
                            Ok(()) => Ok(serde_json::json!({ "deleted": true })),
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                Err((StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response())
                            }
                            Err(_) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot delete file" }))).into_response()),
                        }
                    }
                }
            }
        }
        _ => Err(bad_request("unknown tool")),
    };
    match result {
        Ok(value) => Json(serde_json::json!({ "tool": req.tool, "result": value, "executed": true })).into_response(),
        Err(response) => response,
    }
}

// --- Mock connectors (Phase 4) ---
//
// Fake data only: seeded notes/messages plus items created in
// this server run. No real accounts, no real delivery, no
// OAuth. Connector writes reuse the Phase 3 approval gate;
// mail sends additionally re-check the egress domain here.

#[derive(Debug, Deserialize)]
struct NoteCreateDto {
    tool_call_id: Option<String>,
    title: String,
    body: String,
}

#[derive(Debug, Deserialize)]
struct MailSendDto {
    tool_call_id: Option<String>,
    to: String,
    subject: String,
    body: String,
}

async fn notes_list(State(state): State<AppState>) -> Json<Vec<MockNote>> {
    Json(state.notes.lock().expect("notes lock").clone())
}

async fn notes_create(
    State(state): State<AppState>,
    Json(req): Json<NoteCreateDto>,
) -> impl IntoResponse {
    if let Err(response) = check_approval(&state, "notes.create", &req.tool_call_id) {
        return *response;
    }
    if req.title.is_empty() {
        return bad_request("note title must not be empty");
    }
    let mut next = state.next_note_id.lock().expect("note id lock");
    let id = format!("note-{}", *next);
    *next += 1;
    let note = MockNote { id, title: req.title, body: req.body };
    state.notes.lock().expect("notes lock").push(note.clone());
    Json(note).into_response()
}

async fn mail_list(State(state): State<AppState>) -> Json<Vec<MockMessage>> {
    Json(state.messages.lock().expect("messages lock").clone())
}

async fn mail_send(
    State(state): State<AppState>,
    Json(req): Json<MailSendDto>,
) -> impl IntoResponse {
    if let Err(response) = check_approval(&state, "mail.send", &req.tool_call_id) {
        return *response;
    }
    let domain = req.to.rsplit_once('@').map(|(_, d)| d).unwrap_or("");
    if domain != "mock.local" {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "egress_denied" }))).into_response();
    }
    if req.subject.is_empty() {
        return bad_request("mail subject must not be empty");
    }
    let mut next = state.next_message_id.lock().expect("message id lock");
    let id = format!("mail-{}", *next);
    *next += 1;
    let message = MockMessage {
        id,
        from: "me@mock.local".to_string(),
        to: req.to,
        subject: req.subject,
        body: req.body,
    };
    state.messages.lock().expect("messages lock").push(message.clone());
    Json(message).into_response()
}

/// CORS for browser/WebView clients. The Rust CLIs and curl
/// are not subject to CORS, which is why every CLI and smoke
/// test passed while the Tauri window (page origin
/// http://localhost:5173) saw the gateway as unreachable: the
/// browser blocks cross-origin reads without these headers.
/// The allowlist is the Vite dev-server origin only, never a
/// wildcard: the Mock binds loopback only, and no other
/// website may read its responses.
fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::list([
            HeaderValue::from_static("http://localhost:5173"),
            HeaderValue::from_static("http://127.0.0.1:5173"),
        ]))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::CONTENT_TYPE])
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/chat/history", get(history))
        .route("/chat/stream/start", post(stream_start))
        .route("/chat/stream/apply", post(stream_apply))
        .route("/chat/stream/cancel", post(stream_cancel))
        .route("/approval/check", post(approval_check))
        .route("/files/list", get(files_list))
        .route("/files/read", get(files_read))
        .route("/files/write", post(files_write))
        .route("/files/delete", post(files_delete))
        .route("/approval/request", post(approval_request))
        .route("/approval/decide", post(approval_decide))
        .route("/agent/tool/call", post(agent_tool_call))
        .route("/connectors/notes/list", get(notes_list))
        .route("/connectors/notes/create", post(notes_create))
        .route("/connectors/mail/list", get(mail_list))
        .route("/connectors/mail/send", post(mail_send))
        .layer(cors_layer())
        .with_state(state)
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    state: AppState,
) -> std::io::Result<()> {
    // Phase 5: Ctrl-C stops the server gracefully instead of
    // leaving in-flight requests cut off mid-response.
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        println!("mock-gateway shutting down");
    };
    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown)
        .await
}
