//! EndoAuth / VM target abstraction, cross-platform.
//!
//! Real OAuth, Keychain recovery secrets, VM lease responses and
//! checkpoint / waitlist flows are unknown or deferred (baseline 7, 12).
//! Phase 1 implements only the abstraction + Mock shape below.

use crate::gateway::GatewayTargetKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Launch,
    RestoringCredential,
    SessionBootstrap,
    LoginOptions,
    Authenticating,
    TokenExchange,
    VmAdmission,
    GatewayBootstrap,
    Active,
    Failed,
}

/// VM lease lifecycle, structure only in Phase 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VmLeaseState {
    Requested,
    Assigned,
    Waking,
    Ready,
    Released,
}

/// Session manager: owns the current target key. Any generation
/// bump must cancel/ignore work tied to the previous generation.
pub struct SessionManager {
    pub state: SessionState,
    pub target_key: Option<GatewayTargetKey>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self { state: SessionState::Launch, target_key: None }
    }

    pub fn activate(&mut self, target_key: GatewayTargetKey) {
        self.target_key = Some(target_key);
        self.state = SessionState::Active;
    }

    /// Phase 2 target switch: adopting a new target invalidates
    /// all in-flight work for the old target. Callers must drop
    /// any stream/invocation tied to the previous generation;
    /// `is_current_target` is the check they use before applying
    /// a late result. The state goes back to bootstrap, not
    /// straight to Active: the new target must be confirmed first.
    pub fn switch_target(&mut self, next: GatewayTargetKey) {
        self.target_key = Some(next);
        self.state = SessionState::SessionBootstrap;
    }

    /// True only when `key` is exactly the current target.
    /// A late result for any other generation, URL or VM is stale.
    pub fn is_current_target(&self, key: &GatewayTargetKey) -> bool {
        self.target_key.as_ref() == Some(key)
    }

    /// Logout / account switch: drop the target and any cached
    /// per-account state. Callers must clear caches, not just this struct.
    pub fn logout(&mut self) {
        self.target_key = None;
        self.state = SessionState::Launch;
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}
