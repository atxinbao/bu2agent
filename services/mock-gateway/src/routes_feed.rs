//! Feed routes: /api/feed list, infinite scroll, status, unit get/delete,
//! reaction, tap, seen.
//!
//! The handlers moved here from `server.rs` (FU-5 S-4). The rest of the
//! route directory lives in `server.rs`. There is no `//!` directory line
//! for feed in the `server.rs` header, so no directory line moved with
//! this batch. The feed store (`feed_store.rs`) already holds the
//! seed units, record types, and persistence; only the handlers move.

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::feed_store::{append_feed_state, FeedStateLine, FeedUnit};
use crate::routes_common::{bad_request, now_ms};
use crate::server::AppState;

// --- Feed surface (Task 17, unit subset) ---
//
// Local semantics only (see feed_store.rs): Mock seed
// units with list/infinite/status/get/delete/reaction/
// tap/seen. Prompt, regenerate, reorder, search,
// social-source-icon, and media are excluded.

pub(crate) fn persist_feed_line(state: &AppState, line: &FeedStateLine) {
    let Some(path) = &state.feed_state_path else { return };
    append_feed_state(path, line);
}

pub(crate) fn feed_visible_units(state: &AppState) -> Vec<FeedUnit> {
    let mut units: Vec<FeedUnit> = state
        .feed_units
        .lock()
        .expect("feed lock")
        .values()
        .filter(|u| !u.deleted)
        .cloned()
        .collect();
    units.sort_by_key(|u| (u.created_ms, u.unit_id.clone()));
    units
}

pub(crate) async fn feed_list(State(state): State<AppState>) -> Json<Vec<FeedUnit>> {
    Json(feed_visible_units(&state))
}

// Infinite scroll returns the same visible units: without
// a known pagination schema, inventing cursor params
// would be a fake semantic, so this is a documented alias.
pub(crate) async fn feed_infinite(State(state): State<AppState>) -> Json<Vec<FeedUnit>> {
    Json(feed_visible_units(&state))
}

pub(crate) async fn feed_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let units = state.feed_units.lock().expect("feed lock");
    let visible: Vec<&FeedUnit> = units.values().filter(|u| !u.deleted).collect();
    Json(serde_json::json!({
        "units": visible.len(),
        "unseen": visible.iter().filter(|u| !u.seen).count(),
    }))
}

pub(crate) async fn feed_unit_get(
    State(state): State<AppState>,
    axum::extract::Path(unit_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.feed_units.lock().expect("feed lock").get(&unit_id) {
        Some(unit) => Json(unit.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response(),
    }
}

pub(crate) async fn feed_unit_delete(
    State(state): State<AppState>,
    axum::extract::Path(unit_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut units = state.feed_units.lock().expect("feed lock");
    let Some(unit) = units.get_mut(&unit_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    };
    // Idempotent: deleting an already-deleted unit
    // succeeds without a new record.
    if !unit.deleted {
        unit.deleted = true;
        unit.updated_ms = now_ms();
        let updated = unit.clone();
        persist_feed_line(&state, &FeedStateLine::Unit(updated.clone()));
        return Json(updated).into_response();
    }
    Json(unit.clone()).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct FeedReactionDto {
    reaction: String,
}

pub(crate) async fn feed_unit_reaction(
    State(state): State<AppState>,
    axum::extract::Path(unit_id): axum::extract::Path<String>,
    Json(req): Json<FeedReactionDto>,
) -> impl IntoResponse {
    let reaction = req.reaction.trim().to_string();
    if reaction.is_empty() {
        return bad_request("reaction must not be empty").into_response();
    }
    let mut units = state.feed_units.lock().expect("feed lock");
    let Some(unit) = units.get_mut(&unit_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    };
    if unit.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    }
    unit.reaction = Some(reaction);
    unit.updated_ms = now_ms();
    let updated = unit.clone();
    persist_feed_line(&state, &FeedStateLine::Unit(updated.clone()));
    Json(updated).into_response()
}

pub(crate) async fn feed_unit_tap(
    State(state): State<AppState>,
    axum::extract::Path(unit_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut units = state.feed_units.lock().expect("feed lock");
    let Some(unit) = units.get_mut(&unit_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    };
    if unit.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    }
    unit.tap_count += 1;
    unit.updated_ms = now_ms();
    let updated = unit.clone();
    persist_feed_line(&state, &FeedStateLine::Unit(updated.clone()));
    Json(updated).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct FeedSeenDto {
    unit_ids: Vec<String>,
}

pub(crate) async fn feed_units_seen(
    State(state): State<AppState>,
    Json(req): Json<FeedSeenDto>,
) -> Json<serde_json::Value> {
    let mut units = state.feed_units.lock().expect("feed lock");
    let mut seen = Vec::new();
    let mut unknown = Vec::new();
    for unit_id in req.unit_ids {
        match units.get_mut(&unit_id) {
            Some(unit) if !unit.deleted => {
                if !unit.seen {
                    unit.seen = true;
                    unit.updated_ms = now_ms();
                    let updated = unit.clone();
                    persist_feed_line(&state, &FeedStateLine::Unit(updated.clone()));
                }
                seen.push(unit_id);
            }
            _ => unknown.push(unit_id),
        }
    }
    Json(serde_json::json!({ "seen": seen, "unknown": unknown }))
}
