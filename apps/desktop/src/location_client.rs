//! Location client: Rust shell -> Mock Gateway location
//! surface (MCL-3, manual record + read).
//!
//! Manual trigger only: `record_location` records one
//! sample; there is no auto-collection and no
//! background track. On non-macOS the gateway answers
//! 501 and the error surfaces here unchanged; the
//! packaged Mac app reads real system location via
//! Core Location.
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

/// MCL-3: record runs Core Location gateway-side and
/// can take up to 60 s, past the shared 10 s read
/// timeout — the same F-1 failure shape as mail sync
/// (D1-6a) and contacts sync (MCL-2). Record alone
/// gets a 120 s bound; every other location call keeps
/// the 10 s default so a hung gateway still fails fast
/// instead of hanging the CLI.
const LOCATION_RECORD_TIMEOUT_MS: u64 = 120_000;

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct LocationClient {
    http: HttpClient,
    /// Long-timeout client (F-1): record alone uses it —
    /// the gateway-side Core Location fetch can take up
    /// to 60 s. All other calls share `http` and its
    /// 10 s default.
    sync_http: HttpClient,
}

impl LocationClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self {
            http: HttpClient::new(base_url)?,
            sync_http: HttpClient::with_timeout(base_url, LOCATION_RECORD_TIMEOUT_MS)?,
        })
    }

    pub fn record_location(&self) -> std::result::Result<DeviceLocationRow, AgentError> {
        // F-1: record alone uses the long-timeout client;
        // the gateway-side Core Location fetch can take
        // up to 60 s and the shared 10 s client times
        // out first while the gateway still finishes.
        Ok(self.sync_http.post_json("/api/location/record", "{}")?)
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
