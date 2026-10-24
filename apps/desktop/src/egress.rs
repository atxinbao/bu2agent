//! Egress policy, Mock level only.
//!
//! Every outbound step is checked here before it happens:
//! - Destination: only the localhost Mock Gateway is allowed
//!   (http://127.0.0.1 and http://localhost, any port; the
//!   Mock binds 3001 by default, tests use ephemeral ports).
//!   Any other host, and any non-http scheme, is refused.
//! - Recipient: Mock mail may only be sent inside the fake
//!   `mock.local` domain. A real-looking address is refused
//!   client-side, before any request is sent.
//! - Secret markers: a payload containing a token marker
//!   (`token:`, `Bearer `, `api_key=`) is refused, so a secret
//!   can never be pasted into an outbound body by accident.
//!
//! The server re-checks the recipient domain for mail sends;
//! the client check is defense in depth, as in Phase 3.

use crate::{ErrorCode, Result, UnifiedError, UserAction};

fn denied(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::PermissionDenied,
        capability: Some("egress".to_string()),
        retryable: false,
        user_action: UserAction::None,
        redacted_detail: detail.to_string(),
    }
}

#[derive(Debug, Clone)]
pub struct EgressPolicy {
    pub allowed_hosts: Vec<String>,
    pub allowed_recipient_domains: Vec<String>,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self {
            allowed_hosts: vec!["127.0.0.1".to_string(), "localhost".to_string()],
            allowed_recipient_domains: vec!["mock.local".to_string()],
        }
    }
}

impl EgressPolicy {
    pub fn mock() -> Self {
        Self::default()
    }

    /// Check a base URL against the destination allowlist.
    pub fn check_url(&self, base_url: &str) -> Result<()> {
        let stripped = base_url.strip_prefix("http://").ok_or_else(|| {
            denied("egress refused: only http:// localhost Mock targets are allowed")
        })?;
        let host = stripped.trim_end_matches('/').split(':').next().unwrap_or("");
        if self.allowed_hosts.iter().any(|h| h == host) {
            Ok(())
        } else {
            Err(denied("egress refused: destination is not on the Mock allowlist"))
        }
    }

    /// Check a mail recipient against the domain allowlist.
    pub fn check_recipient(&self, address: &str) -> Result<()> {
        let Some(domain) = address.rsplit_once('@').map(|(_, d)| d) else {
            return Err(denied("egress refused: recipient address has no domain"));
        };
        if self.allowed_recipient_domains.iter().any(|d| d == domain) {
            Ok(())
        } else {
            Err(denied("egress refused: recipient domain is not on the Mock allowlist"))
        }
    }

    /// Refuse payloads that look like they carry a secret.
    pub fn check_payload(&self, payload: &str) -> Result<()> {
        let lower = payload.to_lowercase();
        if lower.contains("token:") || lower.contains("bearer ") || lower.contains("api_key=") {
            return Err(denied("egress refused: payload looks like it carries a secret"));
        }
        Ok(())
    }
}
