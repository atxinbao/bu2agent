//! Permissions routes: /permissions (18) — policy_get,
//! policy_active_get/update, policy_connector_update/reset,
//! policy_hitl_update/snooze_delete, policy_mitm_update,
//! policy_network_rules_update, policy_reset, policy_revoke,
//! policy_scope_get/update, policy_sni_rejection_update,
//! policy_tproxy_update, policy_inventory, policy_settings_get/update.
//!
//! The handlers moved here from `server.rs` (FU-5 S-9e). No `//!`
//! directory lines moved with this batch: the header has no
//! /permissions lines, so server.rs stays at 35 and the directory
//! total stays 59 (35 server.rs + 13 files + 1 misc + 9 chat + 1 loop).
//! The `// --- Permissions policy (Task 11) ---` section comment stays
//! in `server.rs`: it still heads the staying policy-state code.
//! Per the plan's S-9e row ("Policy 状态机随 handler 走") the write
//! machinery moves with the handlers: persist_policy_line,
//! store_policy_overrides/defaults, record_policy_version.
//! Staying in `server.rs` as `pub(crate)` (staying callers):
//! policy_defaults / policy_overrides / policy_effective (called by
//! staying apply_policy_document and policy_digests),
//! current_policy_document (called by the staying sqlite-import
//! paths), retain_policy_rules_version (called by staying
//! with_policy_state_path / apply_policy_versions). PolicyDocument
//! itself is pub(crate) (its fields stay private): the moved
//! record_policy_version binds the value current_policy_document
//! returns, and a pub(crate) fn may not return a private type.
//! All calls to staying helpers are qualified `crate::server::...`,
//! so the module imports only the TYPES AppState +
//! ConnectorPolicyView from `crate::server`, not the fns.
//! connector_policy_view + ConnectorPolicyView (already pub(crate),
//! S-9c: called by the ALREADY-MOVED routes_connectors.rs as well as
//! these handlers; moving them would force a routes_connectors ->
//! routes_permissions import), registry_entry / unknown_connector
//! (already pub(crate), S-9c). The moved policy_view imports
//! ConnectorPolicyView back from `crate::server` (S-9c
//! MockNote/MockMessage import-back precedent), and the struct's
//! three fields are pub(crate) because policy_view sorts by
//! connector_id (S-9c ConnectorEntry-fields precedent).
//! The 13 /api/email routes stay in server.rs (S-9h, after S-9g);
//! the 3 policies/mandates + 8 approval routes stay (S-9f); the
//! ledger routes stay (S-9g).

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::policy_store::{
    append_policy_state, ConnectorPolicy, EffectivePolicy, NetworkRule, PolicyDefaults,
    PolicyOverrides, PolicyStateLine, ScopeRecord,
};
use crate::routes_common::now_ms;
use crate::server::{AppState, ConnectorPolicyView};
use crate::sqlite_store::{self, StateBackend};


pub(crate) fn persist_policy_line(state: &AppState, line: &PolicyStateLine) {
    // D1-2 (Q-D1-21): in SQLite mode the policy_versions
    // table is the fact source; the JSONL file is a
    // read-only archive after the one-time v6 import and
    // is never appended again. JSONL/shadow mode keeps
    // appending (no versioning exists there).
    if state.sqlite_mode() == StateBackend::Sqlite {
        return;
    }
    let Some(path) = &state.policy_state_path else { return };
    append_policy_state(path, line);
}

/// Record the current Policy document as a new version
/// row (SQLite mode only). Called at the end of every
/// /permissions/* write handler, after the in-memory maps
/// hold the post-write document. Best-effort: a
/// versioning failure must not fail a policy write that
/// already succeeded in memory (warn only). The document
/// is built before the SQLite lock is taken (map locks
/// first, never under the SQLite lock).
pub(crate) fn record_policy_version(state: &AppState) {
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return;
    }
    let document = crate::server::current_policy_document(state);
    let Ok(document_value) = serde_json::to_value(&document) else {
        eprintln!("warning: D1-2 policy version skipped (cannot encode document)");
        return;
    };
    let document_json = sqlite_store::canonical_json(&document_value);
    let digest = sqlite_store::sha256_hex(document_json.as_bytes());
    let Some(sqlite) = &state.sqlite else { return };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = sqlite_store::insert_policy_version(
        &mut conn,
        &document_json,
        &digest,
        sqlite_store::now_ms(),
        &state.origin_device_id,
    ) {
        eprintln!("warning: D1-2 policy version insert failed: {detail}");
    }
}

pub(crate) fn store_policy_overrides(state: &AppState, overrides: PolicyOverrides) {
    *state.policy_overrides.lock().expect("policy overrides lock") = Some(overrides.clone());
    persist_policy_line(state, &PolicyStateLine::Overrides(overrides));
}

pub(crate) fn store_policy_defaults(state: &AppState, defaults: PolicyDefaults) {
    *state.policy_defaults.lock().expect("policy defaults lock") = Some(defaults.clone());
    persist_policy_line(state, &PolicyStateLine::Defaults(defaults));
}

#[derive(Debug, Serialize)]
pub(crate) struct ScopeView {
    scope_type: String,
    scope_id: String,
    granted: bool,
    updated_ms: u64,
}

pub(crate) fn scope_view(record: &ScopeRecord) -> ScopeView {
    ScopeView {
        scope_type: record.scope_type.clone(),
        scope_id: record.scope_id.clone(),
        granted: record.granted,
        updated_ms: record.updated_ms,
    }
}

pub(crate) fn unknown_scope() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown scope" }))).into_response()
}

#[derive(Debug, Serialize)]
pub(crate) struct PolicyView {
    defaults: PolicyDefaults,
    active: EffectivePolicy,
    overrides: PolicyOverrides,
    connectors: Vec<ConnectorPolicyView>,
    scopes: Vec<ScopeView>,
    network_rules: Vec<NetworkRule>,
}

pub(crate) fn policy_view(state: &AppState) -> PolicyView {
    let mut connectors: Vec<ConnectorPolicyView> = state
        .policy_connectors
        .lock()
        .expect("policy connectors lock")
        .values()
        .filter(|r| !r.deleted)
        .map(|r| ConnectorPolicyView {
            connector_id: r.connector_id.clone(),
            allowed: r.allowed,
            updated_ms: r.updated_ms,
        })
        .collect();
    connectors.sort_by(|a, b| a.connector_id.cmp(&b.connector_id));
    let mut scopes: Vec<ScopeView> = state
        .policy_scopes
        .lock()
        .expect("policy scopes lock")
        .values()
        .filter(|r| !r.deleted)
        .map(scope_view)
        .collect();
    scopes.sort_by(|a, b| (&a.scope_type, &a.scope_id).cmp(&(&b.scope_type, &b.scope_id)));
    let rules = state
        .policy_rules
        .lock()
        .expect("policy rules lock")
        .clone()
        .unwrap_or_default();
    PolicyView {
        defaults: crate::server::policy_defaults(state),
        active: crate::server::policy_effective(state),
        overrides: crate::server::policy_overrides(state),
        connectors,
        scopes,
        network_rules: rules,
    }
}

pub(crate) async fn policy_get(State(state): State<AppState>) -> Json<PolicyView> {
    Json(policy_view(&state))
}

pub(crate) async fn policy_active_get(State(state): State<AppState>) -> Json<EffectivePolicy> {
    Json(crate::server::policy_effective(&state))
}

/// Three-state snooze field: absent means "no change", explicit
/// null means "clear", a number means "set". Plain
/// `Option<Option<u64>>` collapses absent and null into one
/// value under serde_json, so this helper keeps them apart.
pub(crate) fn deserialize_snooze<'de, D>(deserializer: D) -> Result<Option<Option<u64>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let inner = Option::<u64>::deserialize(deserializer)?;
    Ok(Some(inner))
}

#[derive(Debug, Deserialize)]
pub(crate) struct PolicyActiveDto {
    hitl_enabled: Option<bool>,
    mitm_enabled: Option<bool>,
    tproxy_enabled: Option<bool>,
    sni_rejection_enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_snooze")]
    hitl_snoozed_until_ms: Option<Option<u64>>,
}

pub(crate) async fn policy_active_update(
    State(state): State<AppState>,
    Json(req): Json<PolicyActiveDto>,
) -> Json<EffectivePolicy> {
    let mut overrides = crate::server::policy_overrides(&state);
    if let Some(v) = req.hitl_enabled {
        overrides.hitl_enabled = Some(v);
    }
    if let Some(v) = req.mitm_enabled {
        overrides.mitm_enabled = Some(v);
    }
    if let Some(v) = req.tproxy_enabled {
        overrides.tproxy_enabled = Some(v);
    }
    if let Some(v) = req.sni_rejection_enabled {
        overrides.sni_rejection_enabled = Some(v);
    }
    if let Some(v) = req.hitl_snoozed_until_ms {
        overrides.hitl_snoozed_until_ms = v;
    }
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(crate::server::policy_effective(&state))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ConnectorPolicyDto {
    allowed: bool,
}

pub(crate) async fn policy_connector_update(
    State(state): State<AppState>,
    axum::extract::Path(connector): axum::extract::Path<String>,
    Json(req): Json<ConnectorPolicyDto>,
) -> impl IntoResponse {
    if crate::server::registry_entry(&connector).is_none() {
        return crate::server::unknown_connector();
    }
    let record = ConnectorPolicy {
        connector_id: connector.clone(),
        allowed: req.allowed,
        updated_ms: now_ms(),
        deleted: false,
    };
    state
        .policy_connectors
        .lock()
        .expect("policy connectors lock")
        .insert(connector.clone(), record.clone());
    persist_policy_line(&state, &PolicyStateLine::ConnectorPolicy(record));
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(crate::server::connector_policy_view(&state, &connector)).into_response()
}

pub(crate) async fn policy_connector_reset(
    State(state): State<AppState>,
    axum::extract::Path(connector): axum::extract::Path<String>,
) -> impl IntoResponse {
    if crate::server::registry_entry(&connector).is_none() {
        return crate::server::unknown_connector();
    }
    let mut map = state.policy_connectors.lock().expect("policy connectors lock");
    if let Some(record) = map.get_mut(&connector).filter(|r| !r.deleted) {
        record.deleted = true;
        record.updated_ms = now_ms();
        let updated = record.clone();
        persist_policy_line(&state, &PolicyStateLine::ConnectorPolicy(updated));
    }
    drop(map);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(crate::server::connector_policy_view(&state, &connector)).into_response()
}

#[derive(Debug, Serialize)]
pub(crate) struct HitlView {
    enabled: bool,
    snoozed_until_ms: Option<u64>,
}

pub(crate) fn hitl_view(state: &AppState) -> HitlView {
    let effective = crate::server::policy_effective(state);
    HitlView { enabled: effective.hitl_enabled, snoozed_until_ms: effective.hitl_snoozed_until_ms }
}

#[derive(Debug, Deserialize)]
pub(crate) struct HitlDto {
    enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_snooze")]
    snoozed_until_ms: Option<Option<u64>>,
}

pub(crate) async fn policy_hitl_update(
    State(state): State<AppState>,
    Json(req): Json<HitlDto>,
) -> Json<HitlView> {
    let mut overrides = crate::server::policy_overrides(&state);
    if let Some(v) = req.enabled {
        overrides.hitl_enabled = Some(v);
    }
    if let Some(v) = req.snoozed_until_ms {
        overrides.hitl_snoozed_until_ms = v;
    }
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(hitl_view(&state))
}

pub(crate) async fn policy_hitl_snooze_delete(State(state): State<AppState>) -> Json<HitlView> {
    let mut overrides = crate::server::policy_overrides(&state);
    if overrides.hitl_snoozed_until_ms.is_some() {
        overrides.hitl_snoozed_until_ms = None;
        overrides.updated_ms = now_ms();
        store_policy_overrides(&state, overrides);
    }
    // D1-2 (Q-D1-21): one version row per policy write
    // (even a no-op delete: the write was issued).
    record_policy_version(&state);
    Json(hitl_view(&state))
}

#[derive(Debug, Deserialize)]
pub(crate) struct SwitchDto {
    enabled: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct SwitchView {
    enabled: bool,
}

pub(crate) async fn policy_mitm_update(
    State(state): State<AppState>,
    Json(req): Json<SwitchDto>,
) -> Json<SwitchView> {
    let mut overrides = crate::server::policy_overrides(&state);
    overrides.mitm_enabled = Some(req.enabled);
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(SwitchView { enabled: crate::server::policy_effective(&state).mitm_enabled })
}

pub(crate) async fn policy_tproxy_update(
    State(state): State<AppState>,
    Json(req): Json<SwitchDto>,
) -> Json<SwitchView> {
    let mut overrides = crate::server::policy_overrides(&state);
    overrides.tproxy_enabled = Some(req.enabled);
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(SwitchView { enabled: crate::server::policy_effective(&state).tproxy_enabled })
}

pub(crate) async fn policy_sni_rejection_update(
    State(state): State<AppState>,
    Json(req): Json<SwitchDto>,
) -> Json<SwitchView> {
    let mut overrides = crate::server::policy_overrides(&state);
    overrides.sni_rejection_enabled = Some(req.enabled);
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(SwitchView { enabled: crate::server::policy_effective(&state).sni_rejection_enabled })
}

#[derive(Debug, Deserialize)]
pub(crate) struct NetworkRulesDto {
    rules: Vec<NetworkRule>,
}

pub(crate) async fn policy_network_rules_update(
    State(state): State<AppState>,
    Json(req): Json<NetworkRulesDto>,
) -> impl IntoResponse {
    if req.rules.iter().any(|r| !r.valid()) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid network rule" })))
            .into_response();
    }
    *state.policy_rules.lock().expect("policy rules lock") = Some(req.rules.clone());
    crate::server::retain_policy_rules_version(&state, &req.rules);
    persist_policy_line(&state, &PolicyStateLine::NetworkRules { rules: req.rules.clone() });
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(req.rules).into_response()
}

pub(crate) async fn policy_reset(State(state): State<AppState>) -> Json<PolicyView> {
    let now = now_ms();
    store_policy_defaults(&state, PolicyDefaults { updated_ms: now, ..Default::default() });
    store_policy_overrides(&state, PolicyOverrides { updated_ms: now, ..Default::default() });
    *state.policy_rules.lock().expect("policy rules lock") = Some(Vec::new());
    crate::server::retain_policy_rules_version(&state, &[]);
    persist_policy_line(&state, &PolicyStateLine::NetworkRules { rules: Vec::new() });
    let mut connectors = state.policy_connectors.lock().expect("policy connectors lock");
    for record in connectors.values_mut().filter(|r| !r.deleted) {
        record.deleted = true;
        record.updated_ms = now;
        let updated = record.clone();
        persist_policy_line(&state, &PolicyStateLine::ConnectorPolicy(updated));
    }
    connectors.retain(|_, r| !r.deleted);
    drop(connectors);
    let mut scopes = state.policy_scopes.lock().expect("policy scopes lock");
    for record in scopes.values_mut().filter(|r| !r.deleted) {
        record.deleted = true;
        record.updated_ms = now;
        let updated = record.clone();
        persist_policy_line(&state, &PolicyStateLine::Scope(updated));
    }
    scopes.retain(|_, r| !r.deleted);
    drop(scopes);
    // D1-2 (Q-D1-21): one version row per policy write —
    // the reset's five section mutations are one write.
    record_policy_version(&state);
    Json(policy_view(&state))
}

#[derive(Debug, Deserialize)]
pub(crate) struct RevokeDto {
    scope_type: Option<String>,
    scope_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RevokeView {
    revoked: Vec<String>,
}

pub(crate) async fn policy_revoke(
    State(state): State<AppState>,
    Json(req): Json<RevokeDto>,
) -> Json<RevokeView> {
    let now = now_ms();
    let mut revoked = Vec::new();
    let mut scopes = state.policy_scopes.lock().expect("policy scopes lock");
    let target = match (&req.scope_type, &req.scope_id) {
        (Some(t), Some(i)) => Some(format!("{t}/{i}")),
        _ => None,
    };
    for record in scopes.values_mut().filter(|r| !r.deleted) {
        if target.as_ref().is_some_and(|t| t != &record.key()) {
            continue;
        }
        record.deleted = true;
        record.updated_ms = now;
        let updated = record.clone();
        persist_policy_line(&state, &PolicyStateLine::Scope(updated));
        revoked.push(record.key());
    }
    scopes.retain(|_, r| !r.deleted);
    revoked.sort();
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(RevokeView { revoked })
}

pub(crate) async fn policy_scope_get(
    State(state): State<AppState>,
    axum::extract::Path((scope_type, scope_id)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    let key = format!("{scope_type}/{scope_id}");
    let scopes = state.policy_scopes.lock().expect("policy scopes lock");
    match scopes.get(&key).filter(|r| !r.deleted) {
        Some(record) => Json(scope_view(record)).into_response(),
        None => unknown_scope(),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ScopeDto {
    granted: Option<bool>,
}

pub(crate) async fn policy_scope_update(
    State(state): State<AppState>,
    axum::extract::Path((scope_type, scope_id)): axum::extract::Path<(String, String)>,
    Json(req): Json<ScopeDto>,
) -> Json<ScopeView> {
    let record = ScopeRecord {
        scope_type: scope_type.clone(),
        scope_id: scope_id.clone(),
        granted: req.granted.unwrap_or(true),
        updated_ms: now_ms(),
        deleted: false,
    };
    state
        .policy_scopes
        .lock()
        .expect("policy scopes lock")
        .insert(record.key(), record.clone());
    persist_policy_line(&state, &PolicyStateLine::Scope(record.clone()));
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(scope_view(&record))
}

#[derive(Debug, Serialize)]
pub(crate) struct PolicyInventoryView {
    sections: Vec<&'static str>,
    connector_policies: usize,
    scopes: usize,
    network_rules: usize,
    defaults_customized: bool,
}

pub(crate) async fn policy_inventory(State(state): State<AppState>) -> Json<PolicyInventoryView> {
    let connector_policies = state
        .policy_connectors
        .lock()
        .expect("policy connectors lock")
        .values()
        .filter(|r| !r.deleted)
        .count();
    let scopes = state
        .policy_scopes
        .lock()
        .expect("policy scopes lock")
        .values()
        .filter(|r| !r.deleted)
        .count();
    let network_rules = state
        .policy_rules
        .lock()
        .expect("policy rules lock")
        .clone()
        .unwrap_or_default()
        .len();
    // "Customized" means the stored defaults differ from the
    // built-in ones: a reset stores a builtin-valued snapshot,
    // which must not report as customized.
    let defaults_customized = state
        .policy_defaults
        .lock()
        .expect("policy defaults lock")
        .as_ref()
        .is_some_and(|d| {
            d.hitl_enabled || d.mitm_enabled || d.tproxy_enabled || d.sni_rejection_enabled
        });
    Json(PolicyInventoryView {
        sections: vec![
            "hitl",
            "mitm",
            "tproxy",
            "sni_rejection",
            "network_rules",
            "connectors",
            "scopes",
        ],
        connector_policies,
        scopes,
        network_rules,
        defaults_customized,
    })
}

pub(crate) async fn policy_settings_get(State(state): State<AppState>) -> Json<PolicyDefaults> {
    Json(crate::server::policy_defaults(&state))
}

#[derive(Debug, Deserialize)]
pub(crate) struct PolicySettingsDto {
    hitl_enabled: Option<bool>,
    mitm_enabled: Option<bool>,
    tproxy_enabled: Option<bool>,
    sni_rejection_enabled: Option<bool>,
}

pub(crate) async fn policy_settings_update(
    State(state): State<AppState>,
    Json(req): Json<PolicySettingsDto>,
) -> Json<PolicyDefaults> {
    let mut defaults = crate::server::policy_defaults(&state);
    if let Some(v) = req.hitl_enabled {
        defaults.hitl_enabled = v;
    }
    if let Some(v) = req.mitm_enabled {
        defaults.mitm_enabled = v;
    }
    if let Some(v) = req.tproxy_enabled {
        defaults.tproxy_enabled = v;
    }
    if let Some(v) = req.sni_rejection_enabled {
        defaults.sni_rejection_enabled = v;
    }
    defaults.updated_ms = now_ms();
    store_policy_defaults(&state, defaults);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(crate::server::policy_defaults(&state))
}
