//! Location client: Rust shell -> Mock Gateway location
//! surface (MCL-3, manual record + read).
//!
//! Manual trigger only: `record_from_shell` records one
//! sample the shell acquired itself; there is no
//! auto-collection and no background track. The gateway
//! never fetches a fix any more (S-1 deleted the
//! sidecar Core Location body): the shell does the
//! Core Location wait and POSTs the raw fix with the
//! startup token.
//!
//! Q-S-05 (S-1 decision): the 120 s client is GONE.
//! It existed only because the gateway-side Core
//! Location fetch could block up to 60 s (the D1-6a
//! F-1 shape). The new route is a fast insert — one
//! SQLite transaction, no live fetch — so the shared
//! 10 s client is the right bound: a hung gateway
//! still fails fast instead of hanging the command.
//!
//! PII shape (Q-MCL-17): SQLite stores the RAW
//! coordinates and the UI shows exact values only
//! locally in the packaged app, never logged or
//! exported. `locality_hash` is a coarse grid hash;
//! `country_code` is NULL when the gateway could not
//! determine one.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

/// One `device_locations` row (the gateway
/// `DeviceLocationRow` shape).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceLocationRow {
    pub sample_id: String,
    pub source_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_meters: f64,
    pub locality_hash: String,
    pub country_code: Option<String>,
    pub recorded_ms: i64,
    pub created_ms: i64,
    pub origin_device_id: String,
}

/// The shell-attested fix the shell POSTs to
/// `POST /api/location/record-from-shell`: raw
/// coordinates + accuracy only. No `country_code`
/// (Core Location alone has none) and no hashes (the
/// gateway computes those). Serialized snake_case to
/// match the gateway's `ShellFixDto`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ShellFixInput {
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_meters: f64,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct LocationClient {
    http: HttpClient,
}

impl LocationClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
        })
    }

    /// Record one shell-acquired fix. The fix is
    /// already in hand (the shell did the 60 s wait
    /// before this call), so this POST is a fast insert
    /// on the default 10 s client (Q-S-05). The token
    /// is the startup token the shell passed the
    /// gateway at launch; without it the gateway 403s
    /// and nothing is recorded. The token itself is
    /// never logged — only the HTTP status reaches the
    /// error (the `HttpClient` redaction policy).
    pub fn record_from_shell(
        &self,
        fix: &ShellFixInput,
        shell_token: &str,
    ) -> std::result::Result<DeviceLocationRow, AgentError> {
        let body = serde_json::to_string(fix)
            .map_err(|_| crate::http_client::transport_error("cannot encode location fix"))?;
        Ok(self.http.post_json_with_headers(
            "/api/location/record-from-shell",
            &body,
            &[("X-Shell-Token", shell_token)],
        )?)
    }

    /// List recorded samples, newest first
    /// (`recorded_ms` DESC gateway-side; default 50,
    /// max 200).
    pub fn samples_list(
        &self,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> std::result::Result<Vec<DeviceLocationRow>, AgentError> {
        let mut path = "/api/location/samples".to_string();
        let mut query = Vec::new();
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if let Some(offset) = offset {
            query.push(format!("offset={offset}"));
        }
        if !query.is_empty() {
            path.push('?');
            path.push_str(&query.join("&"));
        }
        Ok(self.http.get_json(&path)?)
    }

    /// One recorded sample by local `sample-N` id.
    pub fn sample_get(&self, sample_id: &str) -> std::result::Result<DeviceLocationRow, AgentError> {
        Ok(self.http.get_json(&format!("/api/location/samples/{sample_id}"))?)
    }
}
