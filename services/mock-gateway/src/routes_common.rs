//! Shared helpers and DTOs for the gateway route modules (`routes_*`).
//!
//! Rules (FU-5 S-0, frozen for later batches): a helper or DTO lives
//! here only when two or more route modules use it, or when it is a
//! domain-free generic helper every module needs (like `now_ms`); a
//! helper used by a single domain moves with that domain's module.
//! New route modules may depend on this module and on the existing
//! store modules, never on each other. `server.rs` keeps AppState,
//! route assembly, and the helpers coupled to server startup or
//! state construction (for example `merge_staged_config`, which the
//! startup path and the public smoke-test API both use).

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

/// 400 with a `{"error": message}` JSON body. Moved verbatim from
/// `server.rs` (FU-5 S-0); the call sites in `server.rs` keep
/// working through `use crate::routes_common::bad_request;`.
pub(crate) fn bad_request(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": message }))).into_response()
}

/// Current wall-clock time in milliseconds since the Unix epoch
/// (0 on clock error). Moved verbatim from `server.rs` (FU-5 S-0);
/// used across every route domain, so it lives here rather than
/// being re-imported from `server.rs` by each new module.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
