//! Agent-timer routes: /agent/timers (3 routes).
//!
//! - `POST /agent/timers` — create a timer (AO-2, Q-AO-07).
//! - `GET /agent/timers` — list timers, soonest first (AO-2, Q-AO-07).
//! - `DELETE /agent/timers/{id}` — revoke a timer (AO-2, Q-AO-07).
//!
//! A timer is a fact row, not an Agent run: creating
//! one only records when it should fire. The sweeper
//! drive (server.rs `drive_due_timers`) fires due
//! rows by writing an event through AO-1's ingest
//! path — a timer NEVER starts an Agent directly.
//!
//! Payload discipline (red line 7): the summary is a
//! JSON object of summaries + pointers only, never
//! raw text. `path` never enters the row or the
//! response.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::routes_common::{sqlite_error_response, sqlite_not_configured};
use crate::server::AppState;
use crate::sqlite_store::{self, AgentTimerRow, StateBackend};

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
pub struct AgentTimerCreateDto {
    pub kind: String,
    pub next_fire_ms: i64,
    #[serde(default)]
    pub recurrence_json: Option<serde_json::Value>,
    #[serde(default)]
    pub payload_summary_json: Option<serde_json::Value>,
    #[serde(default)]
    pub session_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AgentTimerView {
    pub timer_id: String,
    pub kind: String,
    pub payload_summary_json: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub status: String,
    pub next_fire_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recurrence_json: Option<serde_json::Value>,
    pub created_ms: i64,
}

fn timer_view(row: &AgentTimerRow) -> AgentTimerView {
    AgentTimerView {
        timer_id: row.timer_id.clone(),
        kind: row.kind.clone(),
        payload_summary_json: serde_json::from_str(&row.payload_summary_json)
            .unwrap_or(serde_json::Value::Null),
        session_id: row.session_id.clone(),
        status: row.status.clone(),
        next_fire_ms: row.next_fire_ms,
        recurrence_json: row
            .recurrence_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
        created_ms: row.created_ms,
    }
}

/// Create one timer (AO-2, Q-AO-07). The timer is
/// live from the start (`active` — a past
/// `next_fire_ms` is due on the first tick, the
/// D1-3 C-1 "past = due" rule). Validation is
/// 400 `invalid_agent_timer`: an unknown `kind`,
/// a `schedule` without a positive
/// `recurrence_json.interval_ms`, a `follow_up`
/// carrying a recurrence, or a non-object
/// `payload_summary_json` (the AO-1
/// `invalid_agent_event` precedent).
pub(crate) async fn agent_timers_create(
    State(state): State<AppState>,
    Json(req): Json<AgentTimerCreateDto>,
) -> impl IntoResponse {
    if let Some(resp) = sqlite_guard(&state) {
        return resp;
    }
    let kind = req.kind.trim();
    if kind != "follow_up" && kind != "schedule" {
        return err_json(StatusCode::BAD_REQUEST, "invalid_agent_timer");
    }
    let summary = match &req.payload_summary_json {
        None => serde_json::Value::Object(serde_json::Map::new()),
        Some(value) if value.is_object() => value.clone(),
        Some(_) => return err_json(StatusCode::BAD_REQUEST, "invalid_agent_timer"),
    };
    let summary_json = sqlite_store::canonical_json(&summary);
    let session_id = req
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    // AO2-J2: a schedule needs exactly one number,
    // `{"interval_ms": N}` with N > 0; a follow-up
    // must not carry a recurrence at all.
    let recurrence_json = match (kind, &req.recurrence_json) {
        ("schedule", Some(value)) => {
            let canonical = sqlite_store::canonical_json(value);
            if sqlite_store::recurrence_interval_ms(&canonical).is_none() {
                return err_json(StatusCode::BAD_REQUEST, "invalid_agent_timer");
            }
            Some(canonical)
        }
        ("schedule", None) => {
            return err_json(StatusCode::BAD_REQUEST, "invalid_agent_timer");
        }
        (_, Some(_)) => {
            return err_json(StatusCode::BAD_REQUEST, "invalid_agent_timer");
        }
        (_, None) => None,
    };
    let sqlite = state.sqlite.as_ref().expect("sqlite checked");
    let mut conn = sqlite.lock().expect("sqlite lock");
    let row = match sqlite_store::create_agent_timer(
        &mut conn,
        kind,
        &summary_json,
        session_id,
        req.next_fire_ms,
        recurrence_json.as_deref(),
    ) {
        Ok(row) => row,
        Err(detail) => {
            eprintln!("warning: agent timer create failed ({detail})");
            return sqlite_error_response();
        }
    };
    (
        StatusCode::OK,
        Json(serde_json::to_value(timer_view(&row)).unwrap_or(serde_json::Value::Null)),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct AgentTimersListQuery {
    pub limit: Option<i64>,
}

/// List timers, soonest first (AO2-J7). `limit`
/// defaults to 50 and clamps to 1..=200 (the swarm
/// GET precedent). The response is a bare array
/// (the outbox list precedent).
pub(crate) async fn agent_timers_list(
    State(state): State<AppState>,
    Query(query): Query<AgentTimersListQuery>,
) -> impl IntoResponse {
    if let Some(resp) = sqlite_guard(&state) {
        return resp;
    }
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let sqlite = state.sqlite.as_ref().expect("sqlite checked");
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_agent_timers(&conn, limit) {
        Ok(rows) => {
            let views: Vec<AgentTimerView> = rows.iter().map(timer_view).collect();
            Json(serde_json::to_value(views).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: agent timer list failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// Revoke one timer (AO2-J6): an `active` row moves
/// to `cancelled`; an already-terminal row comes
/// back unchanged (idempotent); an unknown id is
/// 404 `unknown_timer`. The row is never deleted
/// (red line 9).
pub(crate) async fn agent_timers_delete(
    State(state): State<AppState>,
    Path(timer_id): Path<String>,
) -> impl IntoResponse {
    if let Some(resp) = sqlite_guard(&state) {
        return resp;
    }
    let sqlite = state.sqlite.as_ref().expect("sqlite checked");
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::revoke_agent_timer(&mut conn, &timer_id) {
        Ok(Some(row)) => (
            StatusCode::OK,
            Json(serde_json::to_value(timer_view(&row)).unwrap_or(serde_json::Value::Null)),
        )
            .into_response(),
        Ok(None) => err_json(StatusCode::NOT_FOUND, "unknown_timer"),
        Err(detail) => {
            eprintln!("warning: agent timer revoke failed ({detail})");
            sqlite_error_response()
        }
    }
}
