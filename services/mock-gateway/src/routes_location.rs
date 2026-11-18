//! Location routes, MCL-3.
//!
//! Three routes, the mail read-batch 1 pattern
//! (`routes_mail.rs` is the model, `routes_imessage.rs`
//! is the nearest copy):
//!
//! - `POST /api/location/record` — manual trigger, one
//!   current sample (no Effect, no approval, no Outbox:
//!   fact recording only, like `POST /api/email/sync`).
//!   Reads the live source (outside the SQLite lock)
//!   and writes the `device_locations` row + the
//!   `location.recorded` event + the 30-day prune in one
//!   transaction. A disconnected `location-source`
//!   rejects the new record with 409
//!   `source_disconnected` (6b Q-24: reads unaffected).
//!   The response is the recorded row, bare (like
//!   `GET /api/location/samples/{id}`).
//! - `GET /api/location/samples` — list recorded samples
//!   from SQLite (default 50, max 200), a bare array
//!   like `GET /api/email/synced`; newest first
//!   (`recorded_ms` DESC). No tombstones exist in this
//!   table (rolling delete, Q-MCL-16).
//! - `GET /api/location/samples/{id}` — one recorded
//!   sample by local `loc-sample-N` id, a bare row;
//!   unknown ids are 404.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::location_record::{
    get_device_location, list_device_locations, record_location, LOCATION_SOURCE_ID,
};
use crate::location_source::{clamp_limit, LocationError};
use crate::routes_common::{sqlite_error_response, sqlite_not_configured};
use crate::server::AppState;
use crate::sqlite_store::{self, StateBackend};

#[derive(Deserialize, Debug)]
pub struct ListParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Map a source failure onto the shared read-path
/// codes: unavailable -> 501, denied -> 403, not
/// running / upstream / timeout -> 502 (the mail
/// `mail_error_response` precedent, with location
/// wording — never a mail error string).
pub(crate) fn location_error_response(error: &LocationError, what: &str) -> Response {
    let (status, detail) = match error {
        LocationError::NotFound(detail) => (StatusCode::NOT_FOUND, detail.to_string()),
        LocationError::Unavailable(detail) => (StatusCode::NOT_IMPLEMENTED, detail.clone()),
        LocationError::Denied => (
            StatusCode::FORBIDDEN,
            "location access denied (check Location Services permission)".to_string(),
        ),
        LocationError::Upstream(detail) => (StatusCode::BAD_GATEWAY, detail.clone()),
        LocationError::NotRunning => (
            StatusCode::BAD_GATEWAY,
            "location source is not running".to_string(),
        ),
        LocationError::Timeout => (
            StatusCode::BAD_GATEWAY,
            "location read timed out".to_string(),
        ),
    };
    (
        status,
        Json(serde_json::json!({
            "error": format!("location {what} failed: {detail}"),
            "code": "location_source_error",
        })),
    )
        .into_response()
}

/// MCL-3 (Q-MCL-15 + 6b Q-24): a disconnected
/// location-source rejects new records. Read paths are
/// unaffected. A missing Registry row (seed never ran)
/// is not a disconnect — the record proceeds.
fn location_source_disconnected(state: &AppState) -> bool {
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let Ok(conn) = sqlite.lock() else {
        return false;
    };
    matches!(
        sqlite_store::get_source(&conn, LOCATION_SOURCE_ID),
        Ok(Some(row)) if row.status == "disconnected"
    )
}

pub(crate) async fn location_record(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    if location_source_disconnected(&state) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "source_disconnected" })),
        )
            .into_response();
    }
    // The fetch runs outside the SQLite lock (the Core
    // Location call can block up to its 60 s cap while
    // the user answers the authorization prompt); the
    // write is one transaction (prune + row + event).
    let view = match state.location_source.current() {
        Ok(view) => view,
        Err(error) => return location_error_response(&error, "record"),
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match record_location(
        &mut conn,
        &view,
        LOCATION_SOURCE_ID,
        &state.origin_device_id,
    ) {
        Ok((row, _pruned)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: location record failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn location_samples_list(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let limit = clamp_limit(params.limit);
    let offset = params.offset.unwrap_or(0);
    let conn = sqlite.lock().expect("sqlite lock");
    match list_device_locations(&conn, LOCATION_SOURCE_ID, limit, offset) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: location samples list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn location_sample_get(
    State(state): State<AppState>,
    Path(sample_id): Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match get_device_location(&conn, &sample_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown location sample" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: location sample get failed ({detail})");
            sqlite_error_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_error_response_maps_each_variant() {
        // Denied -> 403; Unavailable -> 501; Timeout ->
        // 502 (the error-mapping contract the L-tests
        // exercise end to end through the record route).
        let denied = location_error_response(&LocationError::Denied, "record");
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let unavailable = location_error_response(
            &LocationError::Unavailable("no source".to_string()),
            "record",
        );
        assert_eq!(unavailable.status(), StatusCode::NOT_IMPLEMENTED);
        let timeout = location_error_response(&LocationError::Timeout, "record");
        assert_eq!(timeout.status(), StatusCode::BAD_GATEWAY);
        let upstream =
            location_error_response(&LocationError::Upstream("boom".to_string()), "record");
        assert_eq!(upstream.status(), StatusCode::BAD_GATEWAY);
    }
}
