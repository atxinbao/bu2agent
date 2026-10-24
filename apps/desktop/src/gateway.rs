//! HatchClient equivalent: connection, health, reconnect, streaming
//! events and auth state. Transport itself lives in transport.rs;
//! this module owns the connection state machine only.

use crate::{Result, UnifiedError};
use serde::{Deserialize, Serialize};

/// Which gateway target a request goes to. Generation changes
/// invalidate all in-flight results for the old target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTargetKey {
    pub vm_name: String,
    pub gateway_url: String,
    pub vm_id: Option<String>,
    pub generation: u64,
}

impl GatewayTargetKey {
    /// True when `other` is stale relative to this (current) key.
    pub fn is_stale(&self, other: &GatewayTargetKey) -> bool {
        other.generation != self.generation
            || other.gateway_url != self.gateway_url
            || other.vm_name != self.vm_name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRequest {
    pub request_id: String,
    pub method: String,
    pub path: String,
    pub service: GatewayService,
    pub noise_only: bool,
    pub timeout_ms: u64,
    pub target_key: GatewayTargetKey,
    pub body: Option<String>, // JSON text; schema-checked at the boundary
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayService {
    Gateway,
    Daemon,
    Authd,
    Sentinel,
    Vault,
    NoiseOnly,
    Media,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayResponse {
    pub status: u16,
    pub body: String, // JSON text; already redacted at the transport layer
    pub request_id: String,
    pub target_key: GatewayTargetKey,
}

/// Connection state machine (baseline 4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Idle,
    Dialing,
    TransportReady,
    ServerConfirmed,
    Active,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionFailure {
    TransportFailed,
    ClosedBeforeConfirm,
    KeepaliveTimeout,
    PrivateAuthUnverified,
    BackendError,
    IntentionalDisconnect,
}

/// Streaming subscription record. Sequence is the dedup key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRecord {
    pub stream_id: String,
    pub sequence: u64,
    pub event_type: String,
    pub payload: String, // JSON text
    pub resume_cursor: Option<String>,
}

/// Minimal HatchClient shell. Phase 1 implements this over
/// MockTransport; the state transitions themselves are the contract.
pub struct HatchClient {
    pub state: ConnectionState,
    pub target_key: GatewayTargetKey,
}

impl HatchClient {
    pub fn new(target_key: GatewayTargetKey) -> Self {
        Self { state: ConnectionState::Idle, target_key }
    }

    /// Switching target cancels old work: callers must drop any
    /// stream/invocation tied to the previous generation.
    pub fn switch_target(&mut self, next: GatewayTargetKey) {
        self.target_key = next;
        self.state = ConnectionState::Idle;
    }

    pub fn request(&self, _req: GatewayRequest) -> Result<GatewayResponse> {
        // TODO(Phase 1): route through GatewayTransport (Mock first).
        Err(UnifiedError {
            code: crate::ErrorCode::NotSupported,
            capability: Some("gateway".to_string()),
            retryable: false,
            user_action: crate::UserAction::None,
            redacted_detail: "gateway request not yet implemented".to_string(),
        })
    }
}
