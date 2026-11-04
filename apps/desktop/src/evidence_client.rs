//! Evidence client: Rust shell -> Mock Gateway evidence endpoints.
//!
//! Evidence rows only: list, get. Evidence is SQLite-only
//! (P0-R plan §2.4, following the event-table precedent): it
//! is never mirrored to JSONL. Each row is one observation of
//! a write's post-state -- a write-time observation, a
//! verification, or a reconciliation mirror -- and never
//! carries file bytes, credential values, or session text.
//! Localhost only, no TLS, Mock only -- same transport rules
//! as the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::http_client::HttpClient;
use crate::Result;

/// One Evidence row, snake_case as served. `observed_json`
/// is the observed post-write snapshot (file kind + hash +
/// length, absent/dir markers, record flags, or artifact
/// hash + task status) -- never file bytes or credentials.
/// `checked_ms` / `created_ms` follow the SQLite row
/// convention (i64 wall-clock milliseconds).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceRow {
    pub evidence_id: String,
    pub effect_id: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    /// `write` | `verify` | `reconcile`.
    pub source: String,
    /// `verified` | `mismatch` | `inconclusive`.
    pub verdict: String,
    /// Machine-readable reason code, e.g. `hash_match`,
    /// `hash_mismatch`, `target_missing`, `io_error`.
    pub reason: String,
    pub observed_json: serde_json::Value,
    pub checked_ms: i64,
    pub origin_device_id: String,
    pub schema_version: i64,
    pub created_ms: i64,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct EvidenceClient {
    http: HttpClient,
}

impl EvidenceClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list(&self) -> Result<Vec<EvidenceRow>> {
        self.http.get_json("/evidence/list")
    }

    pub fn get(&self, evidence_id: &str) -> Result<EvidenceRow> {
        self.http.get_json(&format!("/evidence/get?evidence_id={evidence_id}"))
    }
}
