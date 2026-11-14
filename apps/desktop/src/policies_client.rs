//! Policies client: Rust shell -> Mock Gateway policy versions endpoint.
//!
//! Policy versions only: list.
//! FU-3C (Q-R0-01): the `/policies/versions` route had no
//! production client; this wires it for the SettingsView
//! policy section. Rows live in SQLite only (P0-D); without
//! a configured SQLite database the route answers 5xx
//! fail-closed. Localhost only, no TLS, Mock only -- same
//! transport rules as the other desktop clients.
//!
//! `document_json` is a JSON string, not an object; callers
//! display it as text (full in the expanded row detail).

use serde::{Deserialize, Serialize};

use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyVersionRow {
    pub version_id: String,
    pub version: i64,
    pub document_json: String,
    pub digest: String,
    #[serde(default)]
    pub supersedes: Option<String>,
    pub created_ms: i64,
    pub origin_device_id: String,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct PoliciesClient {
    http: HttpClient,
}

impl PoliciesClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list_versions(&self) -> Result<Vec<PolicyVersionRow>> {
        self.http.get_json("/policies/versions")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // FU-3C (Q-R0-01): the gateway emits the full
    // PolicyVersionRow; the desktop record must decode every
    // field, including the supersedes link when present and
    // its null form (versions are insert-only; the first
    // version supersedes nothing).
    const FULL_JSON: &str = r#"{
        "version_id": "policy-v2", "version": 2,
        "document_json": "{\"hitl\":true}",
        "digest": "sha256:abc", "supersedes": "policy-v1",
        "created_ms": 1794611497606, "origin_device_id": "dev-1"
    }"#;

    const NULL_SUPERSEDES_JSON: &str = r#"{
        "version_id": "policy-v1", "version": 1,
        "document_json": "{\"hitl\":false}",
        "digest": "sha256:def", "supersedes": null,
        "created_ms": 1794611497500, "origin_device_id": "dev-1"
    }"#;

    #[test]
    fn policy_version_row_decodes_all_fields() {
        let row: PolicyVersionRow = serde_json::from_str(FULL_JSON).expect("full row");
        assert_eq!(row.version_id, "policy-v2");
        assert_eq!(row.version, 2);
        assert_eq!(row.document_json, "{\"hitl\":true}");
        assert_eq!(row.digest, "sha256:abc");
        assert_eq!(row.supersedes.as_deref(), Some("policy-v1"));
        assert_eq!(row.created_ms, 1794611497606);
        assert_eq!(row.origin_device_id, "dev-1");
    }

    #[test]
    fn policy_version_row_null_supersedes_decodes_to_none() {
        let row: PolicyVersionRow =
            serde_json::from_str(NULL_SUPERSEDES_JSON).expect("null supersedes row");
        assert_eq!(row.version_id, "policy-v1");
        assert_eq!(row.version, 1);
        assert_eq!(row.document_json, "{\"hitl\":false}");
        assert_eq!(row.digest, "sha256:def");
        assert_eq!(row.supersedes, None);
        assert_eq!(row.created_ms, 1794611497500);
        assert_eq!(row.origin_device_id, "dev-1");
    }
}
