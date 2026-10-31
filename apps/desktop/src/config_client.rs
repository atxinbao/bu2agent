//! Config client: Rust shell -> Mock Gateway config surface
//! (Task 14, GET only).
//!
//! GET /config reports gateway facts, read-only. There is
//! no PATCH client because the gateway has no
//! runtime-mutable self-owned config to write (files_root
//! is a startup jail root, model belongs to /model, the
//! key belongs to the Keychain). Like the other metadata
//! clients, this call carries no approval loop, and the
//! base URL is allowlisted to the localhost Mock.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// Gateway facts, read-only. `files_root` is the
/// gateway's own jail root, which may differ from the
/// shell's file root when attached to a foreign gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigView {
    pub files_root: String,
    pub gateway_addr: String,
    pub provider: String,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct ConfigClient {
    http: HttpClient,
}

impl ConfigClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn get_config(&self) -> std::result::Result<ConfigView, AgentError> {
        let view: ConfigView = self.http.get_json("/config")?;
        Ok(view)
    }
}
