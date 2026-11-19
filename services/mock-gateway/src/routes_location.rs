//! Location routes, MCL-3 (shell route, S-1).
//!
//! Three routes, the mail read-batch 1 pattern
//! (`routes_mail.rs` is the model, `routes_imessage.rs`
//! is the nearest copy):
//!
//! - `POST /api/location/record-from-shell` — manual
//!   trigger, one current sample (no Effect, no
//!   approval, no Outbox: fact recording only, like
//!   `POST /api/email/sync`). The fix arrives in the
//!   request body — raw latitude / longitude / accuracy
//!   — but ONLY from the packaged app's shell process,
//!   attested by the startup token (`X-Shell-Token`,
//!   Q-S-02): a fix without the token, or with the
//!   wrong token, is 403 and never reaches SQLite
//!   (the old sidecar-fetch route
//!   `POST /api/location/record` was deleted with the
//!   sidecar Core Location body, Q-S-03/Q-S-08).
//!   Writes the `device_locations` row + the
//!   `location.recorded` event + the 30-day prune in
//!   one transaction. A disconnected `location-source`
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
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::location_record::{
    get_device_location, list_device_locations, record_location, LOCATION_SOURCE_ID,
};
use crate::location_source::{clamp_limit, DeviceLocationView};
use crate::routes_common::{sqlite_error_response, sqlite_not_configured};
use crate::server::AppState;
use crate::sqlite_store::{self, StateBackend};

#[derive(Deserialize, Debug)]
pub struct ListParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// The shell-attested fix: raw latitude / longitude /
/// accuracy only. No `country_code` — Core Location
/// alone has none (the column stays NULL); no grid or
/// locality hash — the gateway computes those from the
/// fix (Q-S-10: the shell sends raw coordinates only).
#[derive(Deserialize, Debug)]
pub struct ShellFixDto {
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_meters: f64,
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

/// MCL-3 shell route (S-1, Q-S-02): the shell
/// acquired the fix itself and attests it with the
/// startup token. The token check runs FIRST, before
/// any SQLite/state touch: without the token (or with
/// the wrong one) no fix is ever recorded, whatever
/// else the gateway's state is.
///
/// Failure mapping (Q-S-02, coordinator's call from
/// the gateway's own precedent): both failures are
/// 403, never 401 — no 401 precedent exists anywhere
/// in the gateway (a grep for 401/UNAUTHORIZED finds
/// only a retry comment), while 403 is the established
/// code for "caller not allowed"
/// (`approval_binding_mismatch` 403, location Denied
/// 403). Missing and wrong differ only in the error
/// string (`shell_token_required` vs
/// `shell_token_invalid`) so the shell can tell "no
/// token configured" from "token mismatch" without
/// the gateway leaking which side failed.
pub(crate) async fn location_record_from_shell(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(fix): Json<ShellFixDto>,
) -> impl IntoResponse {
    let presented = headers.get("X-Shell-Token");
    let token_ok = match (&state.location_shell_token, presented) {
        (Some(expected), Some(got)) => got.as_bytes() == expected.as_bytes(),
        _ => false,
    };
    if !token_ok {
        let error = if presented.is_some() {
            "shell_token_invalid"
        } else {
            "shell_token_required"
        };
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": error })),
        )
            .into_response();
    }
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
    // The fix is already in hand (the shell did the
    // 60 s Core Location wait); the write is one
    // transaction (prune + row + event) via the same
    // `record_location` the old route used. The shell
    // sends no country: Core Location alone has none.
    let view = DeviceLocationView {
        latitude: fix.latitude,
        longitude: fix.longitude,
        accuracy_meters: fix.accuracy_meters,
        country_code: None,
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
