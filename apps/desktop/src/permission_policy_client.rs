//! Permission policy client: Rust shell -> Mock Gateway
//! permissions surface (Task 11).
//!
//! These are local metadata calls, so they carry no approval
//! loop (same decision as the Task 9/10 registry and vault
//! calls). The base URL is still allowlisted to the localhost
//! Mock. Nothing here enforces a policy: the gateway stores
//! policy data only, and the switches do not intercept real
//! traffic — the Settings UI says so plainly.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::{transport_error, HttpClient};
use crate::Result;

/// Stored defaults (the settings layer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyDefaultsView {
    pub hitl_enabled: bool,
    pub mitm_enabled: bool,
    pub tproxy_enabled: bool,
    pub sni_rejection_enabled: bool,
    pub updated_ms: u64,
}

/// Effective policy: defaults merged with overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectivePolicyView {
    pub hitl_enabled: bool,
    pub mitm_enabled: bool,
    pub tproxy_enabled: bool,
    pub sni_rejection_enabled: bool,
    pub hitl_snoozed_until_ms: Option<u64>,
    pub updated_ms: u64,
}

/// Per-connector policy. A known connector with no stored
/// policy reads as allowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorPolicyView {
    pub connector_id: String,
    pub allowed: bool,
    pub updated_ms: u64,
}

/// One network rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkRuleView {
    pub pattern: String,
    pub action: String,
}

/// One granted scope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeView {
    pub scope_type: String,
    pub scope_id: String,
    pub granted: bool,
    pub updated_ms: u64,
}

/// Full policy document: defaults, effective policy, overrides,
/// per-connector policies, scopes, and network rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyView {
    pub defaults: PolicyDefaultsView,
    pub active: EffectivePolicyView,
    pub overrides: PolicyOverridesView,
    pub connectors: Vec<ConnectorPolicyView>,
    pub scopes: Vec<ScopeView>,
    pub network_rules: Vec<NetworkRuleView>,
}

/// Stored overrides; `None` means "no override".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyOverridesView {
    pub hitl_enabled: Option<bool>,
    pub mitm_enabled: Option<bool>,
    pub tproxy_enabled: Option<bool>,
    pub sni_rejection_enabled: Option<bool>,
    pub hitl_snoozed_until_ms: Option<u64>,
    pub updated_ms: u64,
}

/// The hitl section: effective switch plus snooze deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HitlView {
    pub enabled: bool,
    pub snoozed_until_ms: Option<u64>,
}

/// A single on/off switch, as the gateway reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SwitchView {
    pub enabled: bool,
}

/// Computed inventory: section list plus record counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyInventoryView {
    pub sections: Vec<String>,
    pub connector_policies: usize,
    pub scopes: usize,
    pub network_rules: usize,
    pub defaults_customized: bool,
}

/// Revoke result: the scope keys that were tombstoned.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevokeView {
    pub revoked: Vec<String>,
}

#[derive(Debug, Serialize)]
struct PolicyActiveDto {
    hitl_enabled: Option<bool>,
    mitm_enabled: Option<bool>,
    tproxy_enabled: Option<bool>,
    sni_rejection_enabled: Option<bool>,
    // None must serialize as an omitted key, not null: null
    // means "clear" on the server side.
    #[serde(skip_serializing_if = "Option::is_none")]
    hitl_snoozed_until_ms: Option<Option<u64>>,
}

#[derive(Debug, Serialize)]
struct ConnectorPolicyDto {
    allowed: bool,
}

#[derive(Debug, Serialize)]
struct HitlDto {
    enabled: Option<bool>,
    // None must serialize as an omitted key, not null: null
    // means "clear" on the server side.
    #[serde(skip_serializing_if = "Option::is_none")]
    snoozed_until_ms: Option<Option<u64>>,
}

#[derive(Debug, Serialize)]
struct SwitchDto {
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct NetworkRulesDto {
    rules: Vec<NetworkRuleView>,
}

#[derive(Debug, Serialize)]
struct ScopeDto {
    granted: Option<bool>,
}

#[derive(Debug, Serialize)]
struct RevokeDto {
    scope_type: Option<String>,
    scope_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct PolicySettingsDto {
    hitl_enabled: Option<bool>,
    mitm_enabled: Option<bool>,
    tproxy_enabled: Option<bool>,
    sni_rejection_enabled: Option<bool>,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct PermissionPolicyClient {
    http: HttpClient,
}

impl PermissionPolicyClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn get_policy(&self) -> std::result::Result<PolicyView, AgentError> {
        let policy: PolicyView = self.http.get_json("/permissions")?;
        Ok(policy)
    }

    pub fn get_active(&self) -> std::result::Result<EffectivePolicyView, AgentError> {
        let active: EffectivePolicyView = self.http.get_json("/permissions/active")?;
        Ok(active)
    }

    pub fn update_active(
        &self,
        hitl_enabled: Option<bool>,
        mitm_enabled: Option<bool>,
        tproxy_enabled: Option<bool>,
        sni_rejection_enabled: Option<bool>,
        hitl_snoozed_until_ms: Option<Option<u64>>,
    ) -> std::result::Result<EffectivePolicyView, AgentError> {
        let body = serde_json::to_string(&PolicyActiveDto {
            hitl_enabled,
            mitm_enabled,
            tproxy_enabled,
            sni_rejection_enabled,
            hitl_snoozed_until_ms,
        })
        .map_err(|_| transport_error("cannot encode active policy update"))?;
        let active: EffectivePolicyView = self.http.patch_json("/permissions/active", &body)?;
        Ok(active)
    }

    pub fn update_connector(
        &self,
        connector: &str,
        allowed: bool,
    ) -> std::result::Result<ConnectorPolicyView, AgentError> {
        let body = serde_json::to_string(&ConnectorPolicyDto { allowed })
            .map_err(|_| transport_error("cannot encode connector policy update"))?;
        let view: ConnectorPolicyView =
            self.http.patch_json(&format!("/permissions/connectors/{connector}"), &body)?;
        Ok(view)
    }

    pub fn reset_connector(
        &self,
        connector: &str,
    ) -> std::result::Result<ConnectorPolicyView, AgentError> {
        let view: ConnectorPolicyView =
            self.http.delete_json(&format!("/permissions/connectors/{connector}"))?;
        Ok(view)
    }

    pub fn update_hitl(
        &self,
        enabled: Option<bool>,
        snoozed_until_ms: Option<Option<u64>>,
    ) -> std::result::Result<HitlView, AgentError> {
        let body = serde_json::to_string(&HitlDto { enabled, snoozed_until_ms })
            .map_err(|_| transport_error("cannot encode hitl update"))?;
        let view: HitlView = self.http.patch_json("/permissions/hitl", &body)?;
        Ok(view)
    }

    pub fn delete_hitl_snooze(&self) -> std::result::Result<HitlView, AgentError> {
        let view: HitlView = self.http.delete_json("/permissions/hitl/snooze")?;
        Ok(view)
    }

    pub fn update_mitm(&self, enabled: bool) -> std::result::Result<SwitchView, AgentError> {
        let body = serde_json::to_string(&SwitchDto { enabled })
            .map_err(|_| transport_error("cannot encode mitm update"))?;
        let view: SwitchView = self.http.patch_json("/permissions/mitm", &body)?;
        Ok(view)
    }

    pub fn update_tproxy(&self, enabled: bool) -> std::result::Result<SwitchView, AgentError> {
        let body = serde_json::to_string(&SwitchDto { enabled })
            .map_err(|_| transport_error("cannot encode tproxy update"))?;
        let view: SwitchView = self.http.patch_json("/permissions/tproxy", &body)?;
        Ok(view)
    }

    pub fn update_sni_rejection(&self, enabled: bool) -> std::result::Result<SwitchView, AgentError> {
        let body = serde_json::to_string(&SwitchDto { enabled })
            .map_err(|_| transport_error("cannot encode sni-rejection update"))?;
        let view: SwitchView = self.http.patch_json("/permissions/sni-rejection", &body)?;
        Ok(view)
    }

    pub fn update_network_rules(
        &self,
        rules: Vec<NetworkRuleView>,
    ) -> std::result::Result<Vec<NetworkRuleView>, AgentError> {
        let body = serde_json::to_string(&NetworkRulesDto { rules })
            .map_err(|_| transport_error("cannot encode network rules update"))?;
        let rules: Vec<NetworkRuleView> = self.http.patch_json("/permissions/network-rules", &body)?;
        Ok(rules)
    }

    pub fn reset_policy(&self) -> std::result::Result<PolicyView, AgentError> {
        let policy: PolicyView = self.http.post_json("/permissions/reset", "{}")?;
        Ok(policy)
    }

    pub fn revoke(
        &self,
        scope_type: Option<&str>,
        scope_id: Option<&str>,
    ) -> std::result::Result<RevokeView, AgentError> {
        let body = serde_json::to_string(&RevokeDto {
            scope_type: scope_type.map(|s| s.to_string()),
            scope_id: scope_id.map(|s| s.to_string()),
        })
        .map_err(|_| transport_error("cannot encode revoke request"))?;
        let view: RevokeView = self.http.post_json("/permissions/revoke", &body)?;
        Ok(view)
    }

    pub fn get_scope(
        &self,
        scope_type: &str,
        scope_id: &str,
    ) -> std::result::Result<ScopeView, AgentError> {
        let view: ScopeView =
            self.http.get_json(&format!("/permissions/scopes/{scope_type}/{scope_id}"))?;
        Ok(view)
    }

    pub fn update_scope(
        &self,
        scope_type: &str,
        scope_id: &str,
        granted: Option<bool>,
    ) -> std::result::Result<ScopeView, AgentError> {
        let body = serde_json::to_string(&ScopeDto { granted })
            .map_err(|_| transport_error("cannot encode scope update"))?;
        let view: ScopeView =
            self.http.patch_json(&format!("/permissions/scopes/{scope_type}/{scope_id}"), &body)?;
        Ok(view)
    }

    pub fn get_inventory(&self) -> std::result::Result<PolicyInventoryView, AgentError> {
        let inventory: PolicyInventoryView = self.http.get_json("/permissions/inventory")?;
        Ok(inventory)
    }

    pub fn get_settings(&self) -> std::result::Result<PolicyDefaultsView, AgentError> {
        let defaults: PolicyDefaultsView = self.http.get_json("/permissions/settings")?;
        Ok(defaults)
    }

    pub fn update_settings(
        &self,
        hitl_enabled: Option<bool>,
        mitm_enabled: Option<bool>,
        tproxy_enabled: Option<bool>,
        sni_rejection_enabled: Option<bool>,
    ) -> std::result::Result<PolicyDefaultsView, AgentError> {
        let body = serde_json::to_string(&PolicySettingsDto {
            hitl_enabled,
            mitm_enabled,
            tproxy_enabled,
            sni_rejection_enabled,
        })
        .map_err(|_| transport_error("cannot encode settings update"))?;
        let defaults: PolicyDefaultsView = self.http.patch_json("/permissions/settings", &body)?;
        Ok(defaults)
    }

    pub fn connector_permissions(
        &self,
        connector: &str,
    ) -> std::result::Result<ConnectorPolicyView, AgentError> {
        let view: ConnectorPolicyView =
            self.http.get_json(&format!("/connectors/{connector}/permissions"))?;
        Ok(view)
    }
}
