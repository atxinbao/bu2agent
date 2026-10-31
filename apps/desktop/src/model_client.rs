//! Model client: Rust shell -> Mock Gateway model surface
//! (Task 12).
//!
//! These are local metadata calls, so they carry no approval
//! loop (same decision as the Task 9/10/11 registry, vault,
//! and policy calls). The base URL is still allowlisted to
//! the localhost Mock. The key is never accepted here and
//! never appears in a response: only configured + the same
//! ****last-4 redaction the Settings key status uses.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::{transport_error, HttpClient};
use crate::Result;

/// The model surface: running config merged with any staged
/// selection, plus key status (never the key itself).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelView {
    pub provider: String,
    pub model: String,
    pub base_url: String,
    pub key_configured: bool,
    pub key_redacted: Option<String>,
    pub staged_model: Option<String>,
    pub staged_base_url: Option<String>,
    pub restart_needed: bool,
}

#[derive(Debug, Serialize)]
struct ModelSetDto {
    model: Option<String>,
    base_url: Option<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct ModelClient {
    http: HttpClient,
}

impl ModelClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn get_model(&self) -> std::result::Result<ModelView, AgentError> {
        let view: ModelView = self.http.get_json("/model")?;
        Ok(view)
    }

    pub fn set_model(
        &self,
        model: Option<&str>,
        base_url: Option<&str>,
    ) -> std::result::Result<ModelView, AgentError> {
        let body = serde_json::to_string(&ModelSetDto {
            model: model.map(|s| s.to_string()),
            base_url: base_url.map(|s| s.to_string()),
        })
        .map_err(|_| transport_error("cannot encode model set request"))?;
        let view: ModelView = self.http.post_json("/model", &body)?;
        Ok(view)
    }
}
