//! Rust client shell skeleton.
//!
//! Mapping to the Muse baseline (see ARCHITECTURE.md):
//! - gateway.rs     -> HatchClient
//! - transport.rs   -> HCHGatewayTransport (trait; Mock first, Noise later)
//! - bridge.rs      -> endoHatch bridge, cross-platform equivalent
//! - file_client.rs -> HatchFileClient
//! - node.rs        -> HatchNodeCore / HatchNodeProtocol
//! - session.rs     -> EndoAuth / VM target abstraction
//! - permission.rs  -> PermissionSnapshot / precheck / degrade
//! - approval.rs    -> egress approval / HITL state machine
//! - sync.rs        -> SyncEnvelope structure only (real sync deferred)
//! - tools.rs       -> HatchTools registry skeleton
//! - observability.rs -> redacted logging / fatal reporting / bug report
//!
//! This is a skeleton: types, traits and state machines. `todo!()` /
//! `unimplemented!()` marks are honest not-implemented points for Phase 1.

pub mod agent_client;
pub mod approval;
pub mod connector_client;
pub mod credential_store;
pub mod egress;
pub mod retry;
pub mod bridge;
pub mod chat_client;
pub mod file_gateway_client;
pub mod http_client;
pub mod file_client;
pub mod gateway;
pub mod node;
pub mod observability;
pub mod permission;
pub mod session;
pub mod sync;
pub mod tools;
pub mod transport;
pub mod task_client;
pub mod device_client;
pub mod loop_client;
pub mod session_client;

/// Unified error shape. Mirrors the JSON Schema `UnifiedError`.
/// Raw Rust errors, stack traces and system paths must never be
/// converted into this type without redaction (see observability).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnifiedError {
    pub code: ErrorCode,
    pub capability: Option<String>,
    pub retryable: bool,
    pub user_action: UserAction,
    pub redacted_detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    PermissionDenied,
    NotRunning,
    NotSupported,
    Timeout,
    Transport,
    Gateway,
    InvalidInput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserAction {
    OpenSystemSettings,
    OpenApp,
    Retry,
    RequestApproval,
    None,
}

pub type Result<T> = std::result::Result<T, UnifiedError>;
