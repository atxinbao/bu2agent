//! iMessage routes, MCL-1 (threads-only).
//!
//! Three routes, the mail read-batch 1 pattern
//! (`routes_mail.rs` is the model):
//!
//! - `POST /api/imessage/sync` — manual trigger, fact
//!   recording only (no Effect, no approval, no Outbox,
//!   like `POST /api/email/sync`). Reads the live source
//!   and writes the `imessage_threads` table + the
//!   `imessage.synced` events in one transaction. The
//!   fetch runs outside the SQLite lock (the source read
//!   is synchronous, like `fetch_window`).
//! - `GET /api/imessage/threads` — list synced threads
//!   from SQLite (default 50, max 200), a bare array
//!   like `GET /api/email/synced`; tombstones are
//!   visible (6b-3 Q-14).
//! - `GET /api/imessage/threads/{id}` — one synced
//!   thread by local `thread-N` id, a bare row like
//!   `GET /api/email/synced/{id}`; tombstones are
//!   returned, unknown ids are 404.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::imessage_source::{clamp_limit, IMessageError};
use crate::imessage_sync::{
    get_imessage_thread, get_source_cursor, list_imessage_threads, record_sync,
    IMESSAGE_SOURCE_ID,
};
use crate::routes_common::{sqlite_error_response, sqlite_not_configured};
use crate::server::AppState;
use crate::sqlite_store::StateBackend;

#[derive(Deserialize, Debug)]
pub struct ListParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Map a source failure onto the shared read-path
/// codes: unavailable -> 501, denied -> 403, not
/// running / upstream / timeout -> 502 (the mail
/// `mail_error_response` precedent, with imessage
/// wording — never a mail error string).
pub(crate) fn imessage_error_response(error: &IMessageError, what: &str) -> Response {
    let (status, detail) = match error {
        IMessageError::NotFound(detail) => (StatusCode::NOT_FOUND, detail.to_string()),
        IMessageError::Unavailable(detail) => (StatusCode::NOT_IMPLEMENTED, detail.clone()),
        IMessageError::Denied => (
            StatusCode::FORBIDDEN,
            "imessage automation denied (check Messages automation permission)".to_string(),
        ),
        IMessageError::Upstream(detail) => (StatusCode::BAD_GATEWAY, detail.clone()),
        IMessageError::NotRunning => (
            StatusCode::BAD_GATEWAY,
            "Messages is not running".to_string(),
        ),
        IMessageError::Timeout => (
            StatusCode::BAD_GATEWAY,
            "imessage read timed out".to_string(),
        ),
    };
    (
        status,
        Json(serde_json::json!({
            "error": format!("imessage {what} failed: {detail}"),
            "code": "imessage_source_error",
        })),
    )
        .into_response()
}

pub(crate) async fn imessage_sync(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    // The fetch runs outside the SQLite lock (osascript
    // calls must not hold it); the write is one
    // transaction (rows + events + cursor).
    let threads = match state.imessage_source.threads(usize::MAX, 0) {
        Ok(threads) => threads,
        Err(error) => return imessage_error_response(&error, "sync"),
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match record_sync(
        &mut conn,
        &threads,
        IMESSAGE_SOURCE_ID,
        &state.origin_device_id,
    ) {
        Ok(result) => {
            Json(serde_json::to_value(result).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: imessage sync failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn imessage_threads_list(
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
    match list_imessage_threads(&conn, IMESSAGE_SOURCE_ID, limit, offset) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: imessage threads list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn imessage_thread_get(
    State(state): State<AppState>,
    Path(thread_id): Path<String>,
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
    match get_imessage_thread(&conn, &thread_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown imessage thread" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: imessage thread get failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// The stored source cursor (used by tests asserting
/// the cursor advances across syncs).
pub fn get_imessage_cursor(conn: &rusqlite::Connection) -> Result<String, String> {
    get_source_cursor(conn, IMESSAGE_SOURCE_ID)
}
