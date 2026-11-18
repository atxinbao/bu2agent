//! Contacts routes, MCL-2.
//!
//! Four routes, the mail read-batch 1 pattern
//! (`routes_mail.rs` is the model, `routes_imessage.rs`
//! is the nearest copy):
//!
//! - `POST /api/contacts/sync` — manual trigger, fact
//!   recording only (no Effect, no approval, no Outbox,
//!   like `POST /api/email/sync` and
//!   `POST /api/imessage/sync`). Reads the live source
//!   and writes the `contacts` table + the
//!   `contact.synced` events in one transaction. The
//!   fetch runs outside the SQLite lock (the source read
//!   is synchronous, like `fetch_window`).
//! - `GET /api/contacts` — list synced contacts from
//!   SQLite (default 50, max 200), a bare array like
//!   `GET /api/email/synced`; tombstones are visible
//!   (6b-3 Q-14).
//! - `GET /api/contacts/{id}` — one synced contact by
//!   local `contact-N` id, a bare row like
//!   `GET /api/email/synced/{id}`; tombstones are
//!   returned, unknown ids are 404.
//! - `GET /api/contacts/search?q=` — search synced
//!   contacts (SQLite list + in-memory filter over
//!   display_name, phones, and emails —
//!   case-insensitive substring; see
//!   `contacts_sync::search_contacts`). A missing or
//!   empty `q` behaves like the list route (same limit /
//!   offset). Address / birthday values are never
//!   stored, so they cannot be searched (Q-MCL-11).

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::contacts_source::{clamp_limit, ContactsError};
use crate::contacts_sync::{
    get_contact, get_source_cursor, list_contacts, record_sync, search_contacts, CONTACTS_SOURCE_ID,
};
use crate::routes_common::{sqlite_error_response, sqlite_not_configured};
use crate::server::AppState;
use crate::sqlite_store::StateBackend;

#[derive(Deserialize, Debug)]
pub struct ListParams {
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Deserialize, Debug)]
pub struct SearchParams {
    pub q: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Map a source failure onto the shared read-path
/// codes: unavailable -> 501, denied -> 403, not
/// running / upstream / timeout -> 502 (the mail
/// `mail_error_response` precedent, with contacts
/// wording — never a mail error string).
pub(crate) fn contacts_error_response(error: &ContactsError, what: &str) -> Response {
    let (status, detail) = match error {
        ContactsError::NotFound(detail) => (StatusCode::NOT_FOUND, detail.to_string()),
        ContactsError::Unavailable(detail) => (StatusCode::NOT_IMPLEMENTED, detail.clone()),
        ContactsError::Denied => (
            StatusCode::FORBIDDEN,
            "contacts automation denied (check Contacts automation permission)".to_string(),
        ),
        ContactsError::Upstream(detail) => (StatusCode::BAD_GATEWAY, detail.clone()),
        ContactsError::NotRunning => (
            StatusCode::BAD_GATEWAY,
            "Contacts is not running".to_string(),
        ),
        ContactsError::Timeout => (
            StatusCode::BAD_GATEWAY,
            "contacts read timed out".to_string(),
        ),
    };
    (
        status,
        Json(serde_json::json!({
            "error": format!("contacts {what} failed: {detail}"),
            "code": "contacts_source_error",
        })),
    )
        .into_response()
}

pub(crate) async fn contacts_sync(State(state): State<AppState>) -> impl IntoResponse {
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
    let contacts = match state.contacts_source.contacts(usize::MAX, 0) {
        Ok(contacts) => contacts,
        Err(error) => return contacts_error_response(&error, "sync"),
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match record_sync(
        &mut conn,
        &contacts,
        CONTACTS_SOURCE_ID,
        &state.origin_device_id,
    ) {
        Ok(result) => {
            Json(serde_json::to_value(result).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: contacts sync failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn contacts_list(
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
    match list_contacts(&conn, CONTACTS_SOURCE_ID, limit, offset) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: contacts list failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn contacts_get(
    State(state): State<AppState>,
    Path(contact_id): Path<String>,
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
    match get_contact(&conn, &contact_id) {
        Ok(Some(row)) => {
            Json(serde_json::to_value(row).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown contact" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: contact get failed ({detail})");
            sqlite_error_response()
        }
    }
}

pub(crate) async fn contacts_search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
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
    let query = params.q.as_deref().unwrap_or("");
    let conn = sqlite.lock().expect("sqlite lock");
    match search_contacts(&conn, CONTACTS_SOURCE_ID, query, limit, offset) {
        Ok(rows) => {
            Json(serde_json::to_value(rows).unwrap_or(serde_json::Value::Null)).into_response()
        }
        Err(detail) => {
            eprintln!("warning: contacts search failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// The stored source cursor (used by tests asserting
/// the cursor advances across syncs).
pub fn get_contacts_cursor(conn: &rusqlite::Connection) -> Result<String, String> {
    get_source_cursor(conn, CONTACTS_SOURCE_ID)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contacts_error_response_maps_each_variant() {
        // NotRunning -> 502 with contacts wording;
        // Denied -> 403; Unavailable -> 501.
        let running = contacts_error_response(&ContactsError::NotRunning, "sync");
        assert_eq!(running.status(), StatusCode::BAD_GATEWAY);
        let denied = contacts_error_response(&ContactsError::Denied, "sync");
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let unavailable =
            contacts_error_response(&ContactsError::Unavailable("no source".to_string()), "sync");
        assert_eq!(unavailable.status(), StatusCode::NOT_IMPLEMENTED);
        let upstream =
            contacts_error_response(&ContactsError::Upstream("boom".to_string()), "sync");
        assert_eq!(upstream.status(), StatusCode::BAD_GATEWAY);
    }
}
