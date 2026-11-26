//! Agent-event routes: /agent/events (1 route).
//!
//! - `POST /agent/events/ingest` — ingest one event into the inbox (AO-1, Q-AO-04).
//!
//! The inbox is a queue, not an audit log and not an
//! attention row: ingest writes the fact record only
//! (plus `agent_event.created` in the events audit),
//! no Effect, no approval, no Inbox row, no Outbox
//! row (the D1-6a mail-sync posture). Only internal
//! sources call it (file.changed, timers, mail sync
//! hooks — Q-AO-05 names file.changed + timers
//! first; the wiring of those sources is AO-2 /
//! NS-1, not this batch). External webhooks are NOT
//! accepted (no signature semantics in this batch).
//!
//! Payload discipline (red line 7): the summary is a
//! JSON object of summaries + pointers only, never
//! raw text. `path` never enters the row, the event,
//! or the response.
//!
//! The response echoes the stored row plus
//! `deduped`: `true` when the `source_event_id`
//! was already ingested (the existing row is
//! returned, nothing is rewritten — AO1-J2).

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::routes_common::{sqlite_error_response, sqlite_not_configured};
use crate::server::AppState;
use crate::sqlite_store::{self, StateBackend};

fn err_json(status: StatusCode, code: &str) -> Response {
    (status, Json(serde_json::json!({ "error": code }))).into_response()
}

fn sqlite_guard(state: &AppState) -> Option<Response> {
    if state.sqlite_is_broken() {
        return Some(sqlite_error_response());
    }
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return Some(sqlite_not_configured());
    }
    None
}

#[derive(Debug, Deserialize)]
pub struct AgentEventIngestDto {
    pub source_kind: String,
    pub source_event_id: String,
    #[serde(default)]
    pub payload_summary_json: Option<serde_json::Value>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AgentEventView {
    pub event_id: String,
    pub seq: i64,
    pub source_kind: String,
    pub source_event_id: String,
    pub payload_summary_json: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub status: String,
    pub created_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claimed_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_ms: Option<i64>,
    pub deduped: bool,
}

/// Ingest one event (AO-1, Q-AO-04). Fast
/// response, writes the row only, no deep semantic
/// validation (the handler does not check whether
/// the source exists or the session is live — the
/// queue is the fact record, the driver judges
/// later). Empty `source_kind` / `source_event_id`
/// and a non-object `payload_summary_json` are
/// 400 `invalid_agent_event`.
pub(crate) async fn agent_events_ingest(
    State(state): State<AppState>,
    Json(req): Json<AgentEventIngestDto>,
) -> impl IntoResponse {
    if let Some(resp) = sqlite_guard(&state) {
        return resp;
    }
    if req.source_kind.trim().is_empty() || req.source_event_id.trim().is_empty() {
        return err_json(StatusCode::BAD_REQUEST, "invalid_agent_event");
    }
    let summary = match &req.payload_summary_json {
        None => serde_json::Value::Object(serde_json::Map::new()),
        Some(value) if value.is_object() => value.clone(),
        Some(_) => return err_json(StatusCode::BAD_REQUEST, "invalid_agent_event"),
    };
    let summary_json = sqlite_store::canonical_json(&summary);
    let session_id = req
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let sqlite = state.sqlite.as_ref().expect("sqlite checked");
    let mut conn = sqlite.lock().expect("sqlite lock");
    let (row, deduped) = match sqlite_store::ingest_agent_event(
        &mut conn,
        req.source_kind.trim(),
        req.source_event_id.trim(),
        &summary_json,
        session_id,
        &state.origin_device_id,
    ) {
        Ok(pair) => pair,
        Err(detail) => {
            eprintln!("warning: agent event ingest failed ({detail})");
            return sqlite_error_response();
        }
    };
    let view = AgentEventView {
        event_id: row.event_id,
        seq: row.seq,
        source_kind: row.source_kind,
        source_event_id: row.source_event_id,
        payload_summary_json: serde_json::from_str(&row.payload_summary_json)
            .unwrap_or(serde_json::Value::Null),
        session_id: row.session_id,
        status: row.status,
        created_ms: row.created_ms,
        claimed_ms: row.claimed_ms,
        delivered_ms: row.delivered_ms,
        deduped,
    };
    (
        StatusCode::OK,
        Json(serde_json::to_value(view).unwrap_or(serde_json::Value::Null)),
    )
        .into_response()
}
