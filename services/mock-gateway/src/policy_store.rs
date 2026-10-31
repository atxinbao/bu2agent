//! Permission policy state: the egress permissions system
//! (Task 11) as local data.
//!
//! This file holds POLICY DATA ONLY. It does not drive any
//! enforcement: the approval gates (`may_execute_tool`,
//! `tool_requires_approval`) and the `EgressPolicy` allowlist
//! keep their existing behaviour, and the switches stored here
//! (hitl/mitm/tproxy/sni-rejection) do not intercept real
//! traffic — the Mock has no network machinery for that. The
//! UI says so plainly. A later round may wire one real effect
//! (network-rules into the Mock egress check) once this data
//! surface is validated.
//!
//! Persistence mirrors connector_store.rs and vault_store.rs:
//! tagged JSONL snapshots in one file, loaded with
//! last-write-wins. Single-record sections (defaults,
//! overrides, network-rules) keep only their latest snapshot;
//! per-connector policies and scopes are keyed maps, with
//! `deleted` tombstones for reset/revoke so a restart does not
//! resurrect them.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Built-in defaults: everything off. The policy is data-only
/// this round, so these defaults describe the stored document,
/// not the approval gates (which keep their own behaviour).
/// Derived Default gives exactly the builtin values (all
/// switches off, timestamp zero).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyDefaults {
    pub hitl_enabled: bool,
    pub mitm_enabled: bool,
    pub tproxy_enabled: bool,
    pub sni_rejection_enabled: bool,
    pub updated_ms: u64,
}



/// Partial overrides written through PATCH /permissions/active
/// (and the single-switch PATCH routes, which land here too).
/// `None` means "no override; fall back to defaults".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyOverrides {
    pub hitl_enabled: Option<bool>,
    pub mitm_enabled: Option<bool>,
    pub tproxy_enabled: Option<bool>,
    pub sni_rejection_enabled: Option<bool>,
    pub hitl_snoozed_until_ms: Option<u64>,
    pub updated_ms: u64,
}

/// The effective policy: defaults merged with overrides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectivePolicy {
    pub hitl_enabled: bool,
    pub mitm_enabled: bool,
    pub tproxy_enabled: bool,
    pub sni_rejection_enabled: bool,
    pub hitl_snoozed_until_ms: Option<u64>,
    pub updated_ms: u64,
}

pub fn effective_policy(defaults: &PolicyDefaults, overrides: &PolicyOverrides) -> EffectivePolicy {
    EffectivePolicy {
        hitl_enabled: overrides.hitl_enabled.unwrap_or(defaults.hitl_enabled),
        mitm_enabled: overrides.mitm_enabled.unwrap_or(defaults.mitm_enabled),
        tproxy_enabled: overrides.tproxy_enabled.unwrap_or(defaults.tproxy_enabled),
        sni_rejection_enabled: overrides
            .sni_rejection_enabled
            .unwrap_or(defaults.sni_rejection_enabled),
        hitl_snoozed_until_ms: overrides.hitl_snoozed_until_ms,
        updated_ms: overrides.updated_ms.max(defaults.updated_ms),
    }
}

/// Per-connector policy. `allowed` defaults to true: a known
/// connector with no stored policy reads as allowed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorPolicy {
    pub connector_id: String,
    pub allowed: bool,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

/// One network rule. PATCH /permissions/network-rules replaces
/// the whole list: the spec has no single-rule route.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkRule {
    pub pattern: String,
    pub action: String,
}

impl NetworkRule {
    pub fn valid(&self) -> bool {
        !self.pattern.trim().is_empty() && (self.action == "allow" || self.action == "deny")
    }
}

/// One granted scope, keyed by (scope_type, scope_id). PATCH is
/// an upsert (the spec has no scope create route); revoke is a
/// tombstone, so GET on a revoked scope 404s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeRecord {
    pub scope_type: String,
    pub scope_id: String,
    pub granted: bool,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

impl ScopeRecord {
    pub fn key(&self) -> String {
        format!("{}/{}", self.scope_type, self.scope_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PolicyStateLine {
    #[serde(rename = "defaults")]
    Defaults(PolicyDefaults),
    #[serde(rename = "overrides")]
    Overrides(PolicyOverrides),
    #[serde(rename = "connector_policy")]
    ConnectorPolicy(ConnectorPolicy),
    // Struct variant, not a Vec newtype: internally-tagged enums
    // cannot serialize a bare Vec payload (the write degrades
    // to a blank line and the rules are lost on reload).
    #[serde(rename = "network_rules")]
    NetworkRules { rules: Vec<NetworkRule> },
    #[serde(rename = "scope")]
    Scope(ScopeRecord),
}

/// Loaded policy state: defaults, overrides, connector
/// policies, network rules, and scopes, in that order.
pub type PolicyStateData = (
    Option<PolicyDefaults>,
    Option<PolicyOverrides>,
    Vec<ConnectorPolicy>,
    Option<Vec<NetworkRule>>,
    Vec<ScopeRecord>,
);

/// Load policy state; malformed lines are skipped with a
/// warning, mirroring the other stores. Single-record sections
/// keep their last snapshot; maps fold with last-write-wins.
pub fn load_policy_state(path: &Path) -> PolicyStateData {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (None, None, Vec::new(), None, Vec::new());
    };
    let mut defaults = None;
    let mut overrides = None;
    let mut connectors = Vec::new();
    let mut rules = None;
    let mut scopes = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<PolicyStateLine>(line) {
            Ok(PolicyStateLine::Defaults(record)) => defaults = Some(record),
            Ok(PolicyStateLine::Overrides(record)) => overrides = Some(record),
            Ok(PolicyStateLine::ConnectorPolicy(record)) => connectors.push(record),
            Ok(PolicyStateLine::NetworkRules { rules: record }) => rules = Some(record),
            Ok(PolicyStateLine::Scope(record)) => scopes.push(record),
            Err(_) => eprintln!("warning: skipping malformed policy state line in {}", path.display()),
        }
    }
    (defaults, overrides, connectors, rules, scopes)
}

/// Append one state snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_policy_state(path: &Path, line: &PolicyStateLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append policy state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append policy state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_lines_roundtrip() {
        let record = ConnectorPolicy {
            connector_id: "notes".to_string(),
            allowed: false,
            updated_ms: 7,
            deleted: false,
        };
        let line = serde_json::to_string(&PolicyStateLine::ConnectorPolicy(record)).unwrap();
        assert!(line.contains("\"kind\":\"connector_policy\""));
        let parsed: PolicyStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, PolicyStateLine::ConnectorPolicy(_)));
    }

    #[test]
    fn effective_policy_prefers_overrides() {
        let defaults = PolicyDefaults::default();
        let overrides = PolicyOverrides {
            mitm_enabled: Some(true),
            hitl_snoozed_until_ms: Some(4242),
            updated_ms: 9,
            ..Default::default()
        };
        let effective = effective_policy(&defaults, &overrides);
        assert!(effective.mitm_enabled);
        assert!(!effective.hitl_enabled);
        assert_eq!(effective.hitl_snoozed_until_ms, Some(4242));
    }

    #[test]
    fn network_rule_validation() {
        assert!(NetworkRule { pattern: "example.com".to_string(), action: "deny".to_string() }.valid());
        assert!(!NetworkRule { pattern: "  ".to_string(), action: "allow".to_string() }.valid());
        assert!(!NetworkRule { pattern: "example.com".to_string(), action: "block".to_string() }.valid());
    }
}
