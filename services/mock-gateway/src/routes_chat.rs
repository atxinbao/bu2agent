//! - GET  /chat/history          -> seed history
//! - POST /chat/stream/start    -> stream records 1..=4 for a turn
//! - POST /chat/stream/apply    -> deduped records + last applied
//! - POST /chat/stream/cancel   -> cancel a stream
//! - POST /sessions/create      -> new chat session
//! - GET  /sessions/list        -> session summaries (deleted hidden)
//! - GET  /sessions/get         -> full session record
//! - POST /sessions/send        -> append turn + model reply
//! - POST /sessions/delete      -> tombstone (hidden, not erased)
//!
//! Chat routes: /chat (14) + /api/chat (4) + /sessions (5) + /session (5) = 28 routes.
//!
//! The handlers moved here from `server.rs` (FU-5 S-9a). The 9
//! directory lines above moved with their handlers (S-7 precedent);
//! the rest of the route directory lives in `server.rs`.
//! `StreamState` moved with `stream_start`: it backs the AppState
//! `streams` field, and `server.rs` imports it back from here
//! (S-7 `FileEntryDto` precedent). `ApprovalRequest` /
//! `ApprovalResponse` stay in `server.rs`: `approval_check` stays
//! for S-9f. `next_poke_order` the `stream_store` fn stays imported
//! in `server.rs` (startup calls it at 2960); the moved code only
//! touches the AppState field of the same name.

use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use agent_desktop::gateway::SubscriptionRecord;
use crate::chat_store::{append_chat_state, ChatStateLine, SeenMarker, CHAT_THEMES};
use crate::deepseek::DeepseekOutcome;
use crate::routes_common::{bad_request, now_ms};
use crate::server::AppState;
use crate::session_store::{
    append_session, select_context, session_number, title_from, SessionRecord, SessionTurn,
    MAX_INPUT_BYTES,
};
use crate::stream_store::{
    append_stream_line, events_after, last_event_id, project_thread_events, StreamLine,
    StreamOpen, StreamPoke, StreamSubscription, SxsSelection,
};
use crate::{
    apply_records, seed_history, start_stream, ChatMessage, CancellableStream,
};


#[derive(Debug, Clone, Serialize)]
pub(crate) struct HistoryMessage {
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
pub(crate) struct StreamRecordDto {
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
pub(crate) struct StartRequest {
    pub stream_id: String,
    pub turn_id: String,
    pub text: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ApplyRequest {
    pub stream_id: String,
    pub records: Vec<StreamRecordDto>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ApplyResponse {
    pub kept: Vec<StreamRecordDto>,
    pub last_applied_sequence: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CancelRequest {
    pub stream_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct CancelResponse {
    pub stream_id: String,
    pub cancelled: bool,
}

pub(crate) struct StreamState {
    stream: CancellableStream,
    last_applied: u64,
}

pub(crate) async fn history() -> Json<Vec<HistoryMessage>> {
    Json(seed_history().into_iter().map(HistoryMessage::from).collect())
}

pub(crate) async fn stream_start(
    State(state): State<AppState>,
    Json(req): Json<StartRequest>,
) -> impl IntoResponse {
    // DeepSeek: key presence enables the model reply; no key
    // keeps the Mock echo, and a failed model call returns
    // 502, never a silent echo.
    let reply = match state.deepseek.reply(&req.text).await {
        DeepseekOutcome::Disabled => req.text.clone(),
        DeepseekOutcome::Replied(text) => text,
        DeepseekOutcome::Denied(pattern) => {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "network rule denied model call",
                    "rule": pattern,
                })),
            )
                .into_response();
        }
        DeepseekOutcome::Failed(status) => {
            eprintln!("warning: deepseek request failed: status {status:?}");
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": "deepseek request failed",
                    "status": status,
                })),
            )
                .into_response();
        }
    };
    let records = start_stream(&req.stream_id, &req.turn_id, &reply);
    let mut streams = state.streams.lock().expect("stream state lock");
    streams.insert(
        req.stream_id.clone(),
        StreamState { stream: CancellableStream::new(), last_applied: 0 },
    );
    Json(records.into_iter().map(StreamRecordDto::from).collect::<Vec<_>>()).into_response()
}

pub(crate) async fn stream_apply(
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

pub(crate) async fn stream_cancel(
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

// --- Chat record surface (Task 18, record subset) ---
//
// Threads are sessions (thread_id == session_id); the static
// /chat/history seed is untouched. Message indices are stored
// positions (oldest-first), stable across tombstones; views
// hide deleted turns but report stored indices. The stream
// form (subscribe, stream-events, cancel, stream, poke,
// sxs.select) is covered separately in Task 20; see
// stream_store.rs.

pub(crate) fn chat_turn_view(thread_id: &str, index: usize, turn: &SessionTurn) -> serde_json::Value {
    serde_json::json!({
        "thread_id": thread_id,
        "index": index,
        "role": turn.role,
        "text": turn.text,
        "created_ms": turn.created_ms,
        "model": turn.model,
        "finish_reason": turn.finish_reason,
        "deleted": turn.deleted,
    })
}

pub(crate) fn persist_chat_line(state: &AppState, line: &ChatStateLine) {
    let Some(path) = &state.chat_state_path else { return };
    append_chat_state(path, line);
}

/// Unread entries for one record: visible turns at or after
/// the thread's seen watermark.
pub(crate) fn thread_unread(record: &SessionRecord, marker: usize) -> usize {
    record
        .turns
        .iter()
        .enumerate()
        .filter(|(index, turn)| !turn.deleted && *index >= marker)
        .count()
}

pub(crate) async fn chat_themes(State(state): State<AppState>) -> Json<serde_json::Value> {
    let theme = state.chat_theme.lock().expect("chat theme lock").clone();
    Json(serde_json::json!({ "theme": theme, "available": CHAT_THEMES }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ChatThemeSetDto {
    theme: String,
}

pub(crate) async fn chat_themes_set(
    State(state): State<AppState>,
    Json(req): Json<ChatThemeSetDto>,
) -> impl IntoResponse {
    if !CHAT_THEMES.contains(&req.theme.as_str()) {
        return bad_request("unknown theme").into_response();
    }
    *state.chat_theme.lock().expect("chat theme lock") = req.theme.clone();
    persist_chat_line(&state, &ChatStateLine::Theme { theme: req.theme.clone() });
    Json(serde_json::json!({ "theme": req.theme, "available": CHAT_THEMES })).into_response()
}

pub(crate) async fn chat_unread_count(State(state): State<AppState>) -> Json<serde_json::Value> {
    let seen = state.chat_seen.lock().expect("chat seen lock");
    let sessions = state.sessions.lock().expect("session lock");
    let mut records: Vec<&SessionRecord> = sessions.values().filter(|r| !r.deleted).collect();
    records.sort_by_key(|r| (!r.pinned, session_number(&r.session_id)));
    let mut threads = Vec::new();
    let mut total = 0;
    for record in records {
        let marker = seen.get(&record.session_id).copied().unwrap_or(0);
        let unread = thread_unread(record, marker);
        total += unread;
        threads.push(serde_json::json!({ "thread_id": record.session_id, "unread": unread }));
    }
    Json(serde_json::json!({ "unread_count": total, "threads": threads }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ChatSeenDto {
    index: Option<usize>,
}

pub(crate) async fn chat_mark_seen(
    State(state): State<AppState>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
    Json(req): Json<ChatSeenDto>,
) -> impl IntoResponse {
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(&thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let len = record.turns.len();
    let marker = match req.index {
        Some(index) if index >= len => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
                .into_response();
        }
        Some(index) => index + 1,
        None => len,
    };
    let unread = thread_unread(record, marker);
    state.chat_seen.lock().expect("chat seen lock").insert(thread_id.clone(), marker);
    persist_chat_line(
        &state,
        &ChatStateLine::Seen(SeenMarker { thread_id: thread_id.clone(), marker, updated_ms: now_ms() }),
    );
    Json(serde_json::json!({ "thread_id": thread_id, "marker": marker, "unread": unread }))
        .into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct ChatDeleteTurnDto {
    thread_id: String,
    index: usize,
}

pub(crate) async fn chat_delete_turn(
    State(state): State<AppState>,
    Json(req): Json<ChatDeleteTurnDto>,
) -> impl IntoResponse {
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(&req.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    if req.index >= record.turns.len() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
            .into_response();
    }
    if !record.turns[req.index].deleted {
        record.turns[req.index].deleted = true;
        record.updated_ms = now_ms();
        let updated = record.clone();
        persist_session(&state, &updated);
    }
    let view = chat_turn_view(&req.thread_id, req.index, &record.turns[req.index]);
    Json(view).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct ChatMessageQuery {
    thread_id: String,
    index: usize,
}

pub(crate) async fn chat_message(
    State(state): State<AppState>,
    Query(query): Query<ChatMessageQuery>,
) -> impl IntoResponse {
    let sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get(&query.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let Some(turn) = record.turns.get(query.index) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
            .into_response();
    };
    Json(chat_turn_view(&query.thread_id, query.index, turn)).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct ChatHistoryWindowQuery {
    thread_id: String,
    offset: Option<usize>,
    limit: Option<usize>,
}

pub(crate) async fn chat_history_window(
    State(state): State<AppState>,
    Query(query): Query<ChatHistoryWindowQuery>,
) -> impl IntoResponse {
    let sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get(&query.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let visible: Vec<(usize, &SessionTurn)> =
        record.turns.iter().enumerate().filter(|(_, turn)| !turn.deleted).collect();
    let total_visible = visible.len();
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(50).min(200);
    let entries: Vec<serde_json::Value> = visible
        .iter()
        .skip(offset)
        .take(limit)
        .map(|(index, turn)| chat_turn_view(&query.thread_id, *index, turn))
        .collect();
    Json(serde_json::json!({
        "thread_id": query.thread_id,
        "total_visible": total_visible,
        "entries": entries,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct ChatSearchQuery {
    q: String,
}

pub(crate) async fn chat_search(
    State(state): State<AppState>,
    Query(query): Query<ChatSearchQuery>,
) -> impl IntoResponse {
    let needle = query.q.trim().to_lowercase();
    if needle.is_empty() {
        return bad_request("search query must not be empty").into_response();
    }
    let sessions = state.sessions.lock().expect("session lock");
    let mut records: Vec<&SessionRecord> = sessions.values().filter(|r| !r.deleted).collect();
    records.sort_by_key(|r| (!r.pinned, session_number(&r.session_id)));
    let mut matches = Vec::new();
    for record in records {
        for (index, turn) in record.turns.iter().enumerate() {
            if turn.deleted {
                continue;
            }
            if turn.text.to_lowercase().contains(&needle) {
                matches.push(serde_json::json!({
                    "thread_id": record.session_id,
                    "index": index,
                    "role": turn.role,
                    "text": turn.text,
                }));
                if matches.len() >= 50 {
                    break;
                }
            }
        }
        if matches.len() >= 50 {
            break;
        }
    }
    Json(matches).into_response()
}

// --- Chat stream surface (Task 20, stream form subset) ---
//
// Local semantics only (see stream_store.rs and the Task 20
// plan): subscribe + stream-events as a long-poll record
// channel over a thread's visible turns, thread-level
// cancel, subscription-style stream open, poke event
// injection, and record-only sxs.select. Events are a
// projection, not a live token stream; cancel stops
// delivery, never an in-flight model call.

pub(crate) fn stream_thread_exists(state: &AppState, thread_id: &str) -> bool {
    let sessions = state.sessions.lock().expect("session lock");
    sessions.get(thread_id).is_some_and(|r| !r.deleted)
}

pub(crate) fn stream_thread_turns(state: &AppState, thread_id: &str) -> Option<Vec<SessionTurn>> {
    let sessions = state.sessions.lock().expect("session lock");
    let record = sessions.get(thread_id)?;
    if record.deleted {
        return None;
    }
    Some(record.turns.clone())
}

pub(crate) fn stream_thread_pokes(state: &AppState, thread_id: &str) -> Vec<StreamPoke> {
    state
        .stream_pokes
        .lock()
        .expect("stream pokes lock")
        .iter()
        .filter(|p| p.thread_id == thread_id)
        .cloned()
        .collect()
}

pub(crate) fn persist_stream_line(state: &AppState, line: &StreamLine) {
    let Some(path) = &state.stream_state_path else {
        return;
    };
    append_stream_line(path, line);
}

pub(crate) fn stream_event_view(event: &crate::stream_store::ProjectedEvent) -> serde_json::Value {
    serde_json::json!({
        "id": event.id,
        "type": event.event_type,
        "turn_index": event.turn_index,
        "text": event.text,
        "created_ms": event.created_ms,
    })
}

#[derive(Debug, Deserialize)]
pub(crate) struct StreamThreadDto {
    thread_id: String,
}

pub(crate) async fn chat_subscribe(
    State(state): State<AppState>,
    Json(req): Json<StreamThreadDto>,
) -> impl IntoResponse {
    let Some(turns) = stream_thread_turns(&state, &req.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    let pokes = stream_thread_pokes(&state, &req.thread_id);
    let events = project_thread_events(&turns, &pokes, &req.thread_id);
    let cursor = last_event_id(&events);
    let mut id_guard = state.next_subscription_id.lock().expect("subscription id lock");
    let number = *id_guard;
    *id_guard += 1;
    drop(id_guard);
    let record = StreamSubscription {
        subscription_id: format!("sub-{number}"),
        thread_id: req.thread_id.clone(),
        created_ms: now_ms(),
        cancelled: false,
    };
    state
        .stream_subscriptions
        .lock()
        .expect("stream subscriptions lock")
        .insert(record.subscription_id.clone(), record.clone());
    persist_stream_line(&state, &StreamLine::Subscription(record.clone()));
    Json(serde_json::json!({
        "subscription_id": record.subscription_id,
        "thread_id": record.thread_id,
        "cursor": cursor,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct StreamEventsDto {
    subscription_id: String,
    cursor: Option<String>,
    wait_ms: Option<u64>,
}

pub(crate) async fn chat_stream_events(
    State(state): State<AppState>,
    Json(req): Json<StreamEventsDto>,
) -> impl IntoResponse {
    let cursor = req.cursor.clone().unwrap_or_default();
    let wait = req.wait_ms.unwrap_or(0).min(25_000);
    let Some(subscription) = state
        .stream_subscriptions
        .lock()
        .expect("stream subscriptions lock")
        .get(&req.subscription_id)
        .cloned()
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown subscription" })),
        )
            .into_response();
    };
    if subscription.cancelled {
        return Json(serde_json::json!({
            "subscription_id": subscription.subscription_id,
            "cursor": cursor,
            "events": [],
            "cancelled": true,
        }))
        .into_response();
    }
    // Long-poll mirroring files_subscribe: re-check each
    // round so a cancel or a new turn lands promptly.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait);
    loop {
        let cancelled = state
            .stream_subscriptions
            .lock()
            .expect("stream subscriptions lock")
            .get(&req.subscription_id)
            .is_some_and(|s| s.cancelled);
        if cancelled {
            return Json(serde_json::json!({
                "subscription_id": subscription.subscription_id,
                "cursor": cursor,
                "events": [],
                "cancelled": true,
            }))
            .into_response();
        }
        let Some(turns) = stream_thread_turns(&state, &subscription.thread_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "unknown thread" })),
            )
                .into_response();
        };
        let pokes = stream_thread_pokes(&state, &subscription.thread_id);
        let events = project_thread_events(&turns, &pokes, &subscription.thread_id);
        match events_after(&events, &cursor) {
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "unknown cursor" })),
                )
                    .into_response();
            }
            Some(delivered) if !delivered.is_empty() || wait == 0 => {
                let next_cursor = delivered
                    .last()
                    .map(|e| e.id.clone())
                    .unwrap_or(cursor.clone());
                return Json(serde_json::json!({
                    "subscription_id": subscription.subscription_id,
                    "cursor": next_cursor,
                    "events": delivered.iter().map(stream_event_view).collect::<Vec<_>>(),
                    "cancelled": false,
                }))
                .into_response();
            }
            Some(_) => {
                if tokio::time::Instant::now() >= deadline {
                    return Json(serde_json::json!({
                        "subscription_id": subscription.subscription_id,
                        "cursor": cursor,
                        "events": [],
                        "cancelled": false,
                    }))
                    .into_response();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

pub(crate) async fn chat_cancel(
    State(state): State<AppState>,
    Json(req): Json<StreamThreadDto>,
) -> impl IntoResponse {
    if !stream_thread_exists(&state, &req.thread_id) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let mut cancelled_lines = Vec::new();
    let mut count = 0;
    {
        let mut subscriptions = state.stream_subscriptions.lock().expect("stream subscriptions lock");
        for subscription in subscriptions
            .values_mut()
            .filter(|s| s.thread_id == req.thread_id && !s.cancelled)
        {
            subscription.cancelled = true;
            count += 1;
            cancelled_lines.push(StreamLine::Subscription(subscription.clone()));
        }
    }
    for line in &cancelled_lines {
        persist_stream_line(&state, line);
    }
    Json(serde_json::json!({
        "thread_id": req.thread_id,
        "cancelled_subscriptions": count,
    }))
    .into_response()
}

pub(crate) async fn chat_stream_open(
    State(state): State<AppState>,
    Json(req): Json<StreamThreadDto>,
) -> impl IntoResponse {
    if !stream_thread_exists(&state, &req.thread_id) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let mut id_guard = state.next_stream_id.lock().expect("stream id lock");
    let number = *id_guard;
    *id_guard += 1;
    drop(id_guard);
    let record = StreamOpen {
        stream_id: format!("stream-{number}"),
        thread_id: req.thread_id.clone(),
        created_ms: now_ms(),
    };
    persist_stream_line(&state, &StreamLine::StreamOpen(record.clone()));
    Json(serde_json::json!({
        "stream_id": record.stream_id,
        "thread_id": record.thread_id,
    }))
    .into_response()
}

pub(crate) async fn chat_poke(
    State(state): State<AppState>,
    Json(req): Json<StreamThreadDto>,
) -> impl IntoResponse {
    if !stream_thread_exists(&state, &req.thread_id) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let mut order_guard = state.next_poke_order.lock().expect("poke order lock");
    let order = *order_guard;
    *order_guard += 1;
    drop(order_guard);
    let record = StreamPoke { thread_id: req.thread_id.clone(), created_ms: now_ms(), order };
    state.stream_pokes.lock().expect("stream pokes lock").push(record.clone());
    persist_stream_line(&state, &StreamLine::Poke(record.clone()));
    Json(serde_json::json!({
        "thread_id": record.thread_id,
        "event_id": format!("poke:{order}"),
        "created_ms": record.created_ms,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct SxsSelectDto {
    thread_id: String,
    index: usize,
}

pub(crate) async fn chat_sxs_select(
    State(state): State<AppState>,
    Json(req): Json<SxsSelectDto>,
) -> impl IntoResponse {
    let Some(turns) = stream_thread_turns(&state, &req.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    let Some(turn) = turns.get(req.index) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
            .into_response();
    };
    if turn.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
            .into_response();
    }
    let record = SxsSelection {
        thread_id: req.thread_id.clone(),
        index: req.index,
        updated_ms: now_ms(),
    };
    state
        .stream_selections
        .lock()
        .expect("stream selections lock")
        .insert(req.thread_id.clone(), record.clone());
    persist_stream_line(&state, &StreamLine::SxsSelect(record.clone()));
    Json(serde_json::json!({
        "thread_id": req.thread_id,
        "index": req.index,
    }))
    .into_response()
}

// --- Chat sessions (Round 3, 3a) ---
//
// Session bodies live here, never in the audit JSONL files.
// Deletion is a tombstone: hidden from list/get/send and never
// entering model context, but earlier snapshot lines stay in
// the file until it is removed. The UI delete confirmation must
// say exactly that.

#[derive(Debug, Deserialize)]
pub(crate) struct SessionCreateDto {
    title: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionGetQuery {
    session_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionSendDto {
    session_id: String,
    text: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionDeleteDto {
    session_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct SessionSummary {
    session_id: String,
    title: String,
    created_ms: u64,
    updated_ms: u64,
    turn_count: usize,
    pinned: bool,
    archived: bool,
}

/// Append one session snapshot to the persistence file, when
/// configured. Best-effort, mirroring persist_task.
pub(crate) fn persist_session(state: &AppState, record: &SessionRecord) {
    let Some(path) = &state.sessions_path else { return };
    append_session(path, record);
}

pub(crate) async fn sessions_create(
    State(state): State<AppState>,
    Json(req): Json<SessionCreateDto>,
) -> impl IntoResponse {
    let mut next = state.next_session_id.lock().expect("session id lock");
    let id = format!("session-{}", *next);
    *next += 1;
    let now = now_ms();
    let record = SessionRecord {
        session_id: id,
        title: req.title.unwrap_or_default(),
        created_ms: now,
        updated_ms: now,
        deleted: false,
        pinned: false,
        archived: false,
        turns: Vec::new(),
    };
    state.sessions.lock().expect("session lock").insert(record.session_id.clone(), record.clone());
    persist_session(&state, &record);
    Json(record).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionsListQuery {
    include_archived: Option<bool>,
}

pub(crate) fn session_summary(record: &SessionRecord) -> SessionSummary {
    SessionSummary {
        session_id: record.session_id.clone(),
        title: record.title.clone(),
        created_ms: record.created_ms,
        updated_ms: record.updated_ms,
        // Task 18: tombstoned turns are hidden everywhere, so
        // the count reports visible turns only.
        turn_count: record.turns.iter().filter(|turn| !turn.deleted).count(),
        pinned: record.pinned,
        archived: record.archived,
    }
}

pub(crate) async fn sessions_list(
    State(state): State<AppState>,
    Query(query): Query<SessionsListQuery>,
) -> Json<Vec<SessionSummary>> {
    let include_archived = query.include_archived.unwrap_or(false);
    let mut records: Vec<SessionSummary> = state
        .sessions
        .lock()
        .expect("session lock")
        .values()
        .filter(|r| !r.deleted && (include_archived || !r.archived))
        .map(session_summary)
        .collect();
    records.sort_by_key(|r| (!r.pinned, session_number(&r.session_id)));
    Json(records)
}

pub(crate) async fn sessions_get(
    State(state): State<AppState>,
    Query(query): Query<SessionGetQuery>,
) -> impl IntoResponse {
    match state.sessions.lock().expect("session lock").get(&query.session_id) {
        Some(record) if !record.deleted => Json(record.clone()).into_response(),
        _ => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" }))).into_response(),
    }
}

pub(crate) async fn sessions_send(
    State(state): State<AppState>,
    Json(req): Json<SessionSendDto>,
) -> impl IntoResponse {
    let text = req.text.trim().to_string();
    if text.is_empty() {
        return bad_request("empty message");
    }
    if text.len() > MAX_INPUT_BYTES {
        return bad_request("message too large");
    }
    // Record the user turn first, then release the lock for the
    // model call: holding a std mutex across a 30 s await would
    // stall every other session request.
    let context = {
        let mut sessions = state.sessions.lock().expect("session lock");
        let Some(record) = sessions.get_mut(&req.session_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
                .into_response();
        };
        if record.deleted {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
                .into_response();
        }
        let context = select_context(&record.turns);
        let first_message = record.turns.is_empty();
        let now = now_ms();
        record.turns.push(SessionTurn {
            role: "user".to_string(),
            text: text.clone(),
            created_ms: now,
            model: None,
            finish_reason: None,
            deleted: false,
        });
        if first_message && record.title.is_empty() {
            record.title = title_from(&text);
        }
        record.updated_ms = now;
        let snapshot = record.clone();
        persist_session(&state, &snapshot);
        context
    };
    let outcome = state.deepseek.reply_with_history(&context, &text).await;
    let (reply, model, finish_reason) = match outcome {
        DeepseekOutcome::Disabled => (text.clone(), None, None),
        DeepseekOutcome::Replied(reply) => {
            (reply, Some(state.deepseek.config().model.clone()), Some("stop".to_string()))
        }
        DeepseekOutcome::Denied(pattern) => {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "network rule denied model call",
                    "rule": pattern,
                })),
            )
                .into_response();
        }
        DeepseekOutcome::Failed(status) => {
            // The user turn is already persisted; a failed model
            // call adds no assistant turn and never echoes.
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "deepseek request failed", "status": status })),
            )
                .into_response();
        }
    };
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(&req.session_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    };
    if record.deleted {
        // Deleted while the model was in flight: the delete wins,
        // the late reply is dropped, never resurrecting the session.
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    }
    record.turns.push(SessionTurn {
        role: "assistant".to_string(),
        text: reply,
        created_ms: now_ms(),
        model,
        finish_reason,
        deleted: false,
    });
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_session(&state, &updated);
    Json(updated).into_response()
}

pub(crate) async fn sessions_delete(
    State(state): State<AppState>,
    Json(req): Json<SessionDeleteDto>,
) -> impl IntoResponse {
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(&req.session_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    };
    record.deleted = true;
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_session(&state, &updated);
    Json(serde_json::json!({ "session_id": updated.session_id, "deleted": true })).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionIdDto {
    session_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionRenameDto {
    session_id: String,
    title: String,
}

/// Shared mutation path for pin/unpin/archive/unarchive/rename:
/// unknown ids and tombstoned sessions both read as 404, matching
/// `sessions_get`/`sessions_delete`; the mutation bumps `updated_ms`
/// and appends a fresh snapshot, like deletion does.
pub(crate) fn mutate_session_flag(
    state: &AppState,
    session_id: &str,
    mutate: impl FnOnce(&mut SessionRecord),
) -> Response {
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(session_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    }
    mutate(record);
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_session(state, &updated);
    Json(session_summary(&updated)).into_response()
}

pub(crate) async fn session_pin(State(state): State<AppState>, Json(req): Json<SessionIdDto>) -> impl IntoResponse {
    mutate_session_flag(&state, &req.session_id, |r| r.pinned = true)
}

pub(crate) async fn session_unpin(State(state): State<AppState>, Json(req): Json<SessionIdDto>) -> impl IntoResponse {
    mutate_session_flag(&state, &req.session_id, |r| r.pinned = false)
}

pub(crate) async fn session_archive(State(state): State<AppState>, Json(req): Json<SessionIdDto>) -> impl IntoResponse {
    mutate_session_flag(&state, &req.session_id, |r| r.archived = true)
}

pub(crate) async fn session_unarchive(State(state): State<AppState>, Json(req): Json<SessionIdDto>) -> impl IntoResponse {
    mutate_session_flag(&state, &req.session_id, |r| r.archived = false)
}

pub(crate) async fn session_rename(State(state): State<AppState>, Json(req): Json<SessionRenameDto>) -> impl IntoResponse {
    if req.title.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "title must not be empty" })))
            .into_response();
    }
    mutate_session_flag(&state, &req.session_id, |r| r.title = req.title.clone())
}
