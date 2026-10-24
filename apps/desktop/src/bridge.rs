//! endoHatch bridge, cross-platform equivalent.
//!
//! Frontend -> Rust: typed invoke, whitelisted actions only.
//! Rust -> frontend: typed events. Bridge version mismatch
//! fails loudly at startup; it never silently degrades.

/// Current bridge contract version. Bump only with a contract change.
pub const BRIDGE_VERSION: &str = "1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeCommand {
    pub action: String,
    pub payload: String, // JSON text, schema-checked at the boundary
    pub request_id: String,
    pub bridge_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeEvent {
    pub event: String,
    pub payload: String, // JSON text, errors already mapped to UnifiedError
    pub bridge_version: String,
    pub request_id: Option<String>,
    pub stream_id: Option<String>,
}

/// Fatal report: cross-platform equivalent of embedFatal.
/// `message` must already be redacted before it is constructed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeFatalReport {
    pub telemetry_allowed: bool,
    pub error_kind: String,
    pub error_type: String,
    pub message: String,
}

/// Whitelist: the only actions the frontend may invoke.
/// Anything else is rejected on the Rust side, even if the UI is bypassed.
/// Phase 1 starts with gateway + file + approval actions only.
pub fn is_allowed(action: &str) -> bool {
    matches!(
        action,
        "gateway.request"
            | "gateway.subscribe"
            | "gateway.cancel"
            | "fs.list"
            | "fs.read"
            | "fs.write"
            | "fs.delete"
            | "approval.decide"
            | "approval.history"
            | "session.switch_target"
            | "bridge.ready"
    )
}

pub fn check_version(version: &str) -> bool {
    version == BRIDGE_VERSION
}
