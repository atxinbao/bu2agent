//! Effect record client: Rust shell -> Mock Gateway effect endpoints.
//!
//! Effect records only: list, get, reconcile, verify. The gateway
//! stores the records (see server.rs); reconcile drives an
//! `unknown` Effect through its re-check, verify runs a
//! post-hoc drift check on a `succeeded` Effect and returns the
//! Evidence row it wrote. Neither consumes an approval nor
//! moves the Effect lifecycle beyond what the gateway itself
//! does. Localhost only, no TLS, Mock only -- same transport
//! rules as the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::evidence_client::EvidenceRow;
use crate::http_client::{transport_error, HttpClient};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reconciliation {
    pub result: String,
    pub checked_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRecord {
    pub effect_id: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    pub capability: String,
    pub operation: String,
    pub scope: String,
    #[serde(default)]
    pub input_digest: Option<String>,
    #[serde(default)]
    pub approval_id: Option<String>,
    pub status: String,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "unknown_origin")]
    pub origin_device_id: String,
    #[serde(default)]
    pub schema_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation: Option<Reconciliation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

fn unknown_origin() -> String {
    "unknown".to_string()
}

#[derive(Debug, Serialize)]
struct EffectIdRequest {
    effect_id: String,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct EffectsClient {
    http: HttpClient,
}

impl EffectsClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list(&self) -> Result<Vec<EffectRecord>> {
        self.http.get_json("/effects/list")
    }

    pub fn get(&self, effect_id: &str) -> Result<EffectRecord> {
        self.http.get_json(&format!("/effects/get?effect_id={effect_id}"))
    }

    pub fn reconcile(&self, effect_id: &str) -> Result<EffectRecord> {
        self.post_effect_id("/effects/reconcile", effect_id)
    }

    /// Post-hoc drift check on a `succeeded` Effect. Returns the
    /// Evidence row this verification wrote; the Effect row
    /// itself does not move (terminal states have no outgoing
    /// edge). 409 `effect_not_verifiable` for non-succeeded
    /// rows, 409 `evidence_missing` for legacy rows that cannot
    /// be verified without write-time evidence.
    pub fn verify(&self, effect_id: &str) -> Result<EvidenceRow> {
        self.post_effect_id("/effects/verify", effect_id)
    }

    fn post_effect_id<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        effect_id: &str,
    ) -> Result<T> {
        let body = serde_json::to_string(&EffectIdRequest { effect_id: effect_id.to_string() })
            .map_err(|_| transport_error("cannot encode effect id request"))?;
        self.http.post_json(path, &body)
    }
}
