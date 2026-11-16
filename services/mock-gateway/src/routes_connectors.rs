//! Connectors routes: /connectors (13) + /v1 (13) = 26 routes.
//!
//! The handlers moved here from `server.rs` (FU-5 S-9c). No
//! directory lines moved with this batch: the `//!` header has
//! no connectors/vault entries (S-8/S-9b precedent); the directory
//! total stays 59 (36 server.rs + 13 files + 1 misc + 9 chat).
//! `MockNote` / `MockMessage` move with the Phase 4 mock block;
//! their fields are `pub(crate)` because the staying seed fns
//! (`seed_notes` / `seed_messages`) construct literals in
//! `server.rs`, which imports the types back (S-7 FileEntryDto /
//! S-9a StreamState import-back precedent).
//! The registry core stays in `server.rs` as `pub(crate)`:
//! `ConnectorEntry` / `CONNECTOR_REGISTRY` / `registry_entry` /
//! `unknown_connector` are called by the staying permissions
//! handlers (which move only in S-9e); moving them now would force
//! a routes_permissions -> routes_connectors import, which the
//! no-inter-module-import rule forbids (S-5/S-6/S-8 staying-helper
//! precedent). `check_approval` and `connector_policy_view` stay
//! for the same reason (staying callers). `cors_layer()` stays:
//! it is app() plumbing, not a handler.

use std::collections::HashMap;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::connector_store::{
    account_number, append_connector_state, ConnectorAccount, ConnectorConnection,
    ConnectorStateLine,
};
use crate::routes_common::{bad_request, now_ms};
use crate::server::{
    check_approval, check_connector_write, connector_policy_view, record_connector_effect,
    registry_entry, unknown_connector, AppState, ConnectorEntry, IdempotencyDecision,
    CONNECTOR_REGISTRY,
};
use crate::vault_store::{
    append_vault_state, ticket_expired, CaptureTicket, CatalogEntry, CredentialSlot,
    CustomConnectorDef, TicketStatus, VaultStateLine, TICKET_TTL_MS,
};


#[derive(Debug, Clone, Serialize)]
pub(crate) struct MockNote {
    pub(crate) id: String,
    pub(crate) title: String,
    pub(crate) body: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct MockMessage {
    pub(crate) id: String,
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) subject: String,
    pub(crate) body: String,
}


// --- Mock connectors (Phase 4) ---
//
// Fake data only: seeded notes/messages plus items created in
// this server run. No real accounts, no real delivery, no
// OAuth. Connector writes reuse the Phase 3 approval gate;
// mail sends additionally re-check the egress domain here.

#[derive(Debug, Deserialize)]
pub(crate) struct NoteCreateDto {
    tool_call_id: Option<String>,
    title: String,
    body: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct MailSendDto {
    tool_call_id: Option<String>,
    to: String,
    subject: String,
    body: String,
}

pub(crate) async fn notes_list(State(state): State<AppState>) -> Json<Vec<MockNote>> {
    Json(state.notes.lock().expect("notes lock").clone())
}

pub(crate) async fn notes_create(
    State(state): State<AppState>,
    Json(req): Json<NoteCreateDto>,
) -> impl IntoResponse {
    if let Err(response) = check_approval(&state, "notes.create", &req.tool_call_id) {
        return *response;
    }
    if req.title.is_empty() {
        return bad_request("note title must not be empty");
    }
    let mut next = state.next_note_id.lock().expect("note id lock");
    let id = format!("note-{}", *next);
    *next += 1;
    let note = MockNote { id, title: req.title, body: req.body };
    state.notes.lock().expect("notes lock").push(note.clone());
    Json(note).into_response()
}

pub(crate) async fn mail_list(State(state): State<AppState>) -> Json<Vec<MockMessage>> {
    Json(state.messages.lock().expect("messages lock").clone())
}

pub(crate) async fn mail_send(
    State(state): State<AppState>,
    Json(req): Json<MailSendDto>,
) -> impl IntoResponse {
    if let Err(response) = check_approval(&state, "mail.send", &req.tool_call_id) {
        return *response;
    }
    let domain = req.to.rsplit_once('@').map(|(_, d)| d).unwrap_or("");
    if domain != "mock.local" {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "egress_denied" }))).into_response();
    }
    if req.subject.is_empty() {
        return bad_request("mail subject must not be empty");
    }
    let mut next = state.next_message_id.lock().expect("message id lock");
    let id = format!("mail-{}", *next);
    *next += 1;
    let message = MockMessage {
        id,
        from: "me@mock.local".to_string(),
        to: req.to,
        subject: req.subject,
        body: req.body,
    };
    state.messages.lock().expect("messages lock").push(message.clone());
    Json(message).into_response()
}


#[derive(Debug, Serialize)]
pub(crate) struct ConnectorView {
    id: String,
    name: String,
    capabilities: Vec<String>,
    connected: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ConnectorAccountView {
    account_id: String,
    connector_id: String,
    display_name: String,
    status: String,
    linked_ms: u64,
}

pub(crate) fn connector_view(state: &AppState, entry: &'static ConnectorEntry) -> ConnectorView {
    let connected = state
        .connector_connections
        .lock()
        .expect("connector connections lock")
        .get(entry.id)
        .copied()
        .unwrap_or(false);
    ConnectorView {
        id: entry.id.to_string(),
        name: entry.name.to_string(),
        capabilities: entry.capabilities.iter().map(|c| c.to_string()).collect(),
        connected,
    }
}

pub(crate) fn account_view(account: &ConnectorAccount) -> ConnectorAccountView {
    ConnectorAccountView {
        account_id: account.account_id.clone(),
        connector_id: account.connector_id.clone(),
        display_name: account.display_name.clone(),
        status: if account.unlinked { "unlinked".to_string() } else { "linked".to_string() },
        linked_ms: account.linked_ms,
    }
}

pub(crate) fn persist_connector_line(state: &AppState, line: &ConnectorStateLine) {
    let Some(path) = &state.connector_state_path else { return };
    append_connector_state(path, line);
}


pub(crate) async fn connectors_list(State(state): State<AppState>) -> Json<Vec<ConnectorView>> {
    Json(CONNECTOR_REGISTRY.iter().map(|e| connector_view(&state, e)).collect())
}

pub(crate) async fn connectors_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match registry_entry(&id) {
        Some(entry) => Json(connector_view(&state, entry)).into_response(),
        None => unknown_connector(),
    }
}

pub(crate) async fn connectors_accounts(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let Some(_) = registry_entry(&id) else {
        return unknown_connector();
    };
    let mut accounts: Vec<ConnectorAccountView> = state
        .connector_accounts
        .lock()
        .expect("connector accounts lock")
        .values()
        .filter(|a| a.connector_id == id && !a.unlinked)
        .map(account_view)
        .collect();
    accounts.sort_by_key(|a| account_number(&a.account_id));
    Json(accounts).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct AccountLinkDto {
    display_name: Option<String>,
}

pub(crate) async fn connectors_accounts_link(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<AccountLinkDto>,
) -> impl IntoResponse {
    let Some(_) = registry_entry(&id) else {
        return unknown_connector();
    };
    let display_name = req
        .display_name
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Mock account".to_string());
    let mut next = state.next_account_id.lock().expect("account id lock");
    let account = ConnectorAccount {
        account_id: format!("account-{}", *next),
        connector_id: id,
        display_name,
        linked_ms: now_ms(),
        unlinked: false,
    };
    *next += 1;
    state
        .connector_accounts
        .lock()
        .expect("connector accounts lock")
        .insert(account.account_id.clone(), account.clone());
    persist_connector_line(&state, &ConnectorStateLine::Account(account.clone()));
    // Phase 2: a successful link mirrors into an Effect.
    record_connector_effect(&state, None, "connectors", "link", &account.account_id);
    Json(account_view(&account)).into_response()
}

pub(crate) async fn connectors_accounts_unlink(
    State(state): State<AppState>,
    axum::extract::Path((id, account_id)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    let Some(_) = registry_entry(&id) else {
        return unknown_connector();
    };
    // Validate first (404s keep their exact shape), then gate,
    // then mutate — the gate reads the Effects store with no
    // locks held, matching the P0-R discipline below.
    let account = {
        let accounts = state.connector_accounts.lock().expect("connector accounts lock");
        let Some(account) = accounts.get(&account_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown account" }))).into_response();
        };
        if account.connector_id != id {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown account" }))).into_response();
        }
        account.clone()
    };
    // E2-A §2.4: a repeat unlink with no intervening write is a
    // replay — return the current view, append no second
    // tombstone, record no second Effect row. (The old comment
    // below about "even when idempotent" described the
    // pre-gate behaviour: the first unlink still appends its
    // tombstone; only the replay skips it.)
    match check_connector_write(&state, "connectors", "unlink", &account_id) {
        IdempotencyDecision::Proceed => {}
        IdempotencyDecision::Replay(_) => {
            return Json(account_view(&account)).into_response();
        }
        IdempotencyDecision::Blocked(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
        IdempotencyDecision::Unavailable => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "sqlite_unavailable" })),
            )
                .into_response();
        }
    }
    let mut accounts = state.connector_accounts.lock().expect("connector accounts lock");
    let Some(account) = accounts.get_mut(&account_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown account" }))).into_response();
    };
    // Idempotent: unlinking an already-unlinked account succeeds
    // with its current state, mirroring session unpin/unarchive.
    account.unlinked = true;
    let updated = account.clone();
    persist_connector_line(&state, &ConnectorStateLine::Account(updated.clone()));
    // P0-R: the write-time Evidence hook inside the Effect
    // mirror re-locks the accounts map, so the guard must be
    // released first (the borrow ends at the clone above).
    drop(accounts);
    // Phase 2: a successful unlink mirrors into an Effect, even
    // when idempotent (the tombstone line was still appended).
    record_connector_effect(&state, None, "connectors", "unlink", &updated.account_id);
    Json(account_view(&updated)).into_response()
}

pub(crate) async fn connectors_connect_info(
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let Some(entry) = registry_entry(&id) else {
        return unknown_connector();
    };
    Json(serde_json::json!({
        "connector_id": entry.id,
        "flow": "mock",
        "requires_account": true,
        "description": "Local Mock flow: link a Mock account, then connect. No OAuth, no real account, no external redirect.",
    }))
    .into_response()
}

pub(crate) async fn connectors_connect(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let Some(entry) = registry_entry(&id) else {
        return unknown_connector();
    };
    let has_linked = state
        .connector_accounts
        .lock()
        .expect("connector accounts lock")
        .values()
        .any(|a| a.connector_id == id && !a.unlinked);
    if !has_linked {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "connector has no linked account" })))
            .into_response();
    }
    // E2-A §2.4: a repeat connect with no intervening write is
    // a replay — return the current view, write nothing, record
    // no second Effect row. (A connect after a disconnect is a
    // new write: the disconnect intervened, so the gate
    // Proceeds.)
    match check_connector_write(&state, "connectors", "connect", &id) {
        IdempotencyDecision::Proceed => {}
        IdempotencyDecision::Replay(_) => {
            return Json(connector_view(&state, entry)).into_response();
        }
        IdempotencyDecision::Blocked(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
        IdempotencyDecision::Unavailable => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "sqlite_unavailable" })),
            )
                .into_response();
        }
    }
    state
        .connector_connections
        .lock()
        .expect("connector connections lock")
        .insert(id.to_string(), true);
    persist_connector_line(
        &state,
        &ConnectorStateLine::Connection(ConnectorConnection {
            connector_id: id.to_string(),
            connected: true,
            updated_ms: now_ms(),
        }),
    );
    // Phase 2: a successful connect mirrors into an Effect.
    record_connector_effect(&state, None, "connectors", "connect", &id);
    Json(connector_view(&state, entry)).into_response()
}

pub(crate) async fn connectors_disconnect(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let Some(entry) = registry_entry(&id) else {
        return unknown_connector();
    };
    // E2-A §2.4: a repeat disconnect with no intervening write
    // is a replay — return the current view, write nothing,
    // record no second Effect row.
    match check_connector_write(&state, "connectors", "disconnect", &id) {
        IdempotencyDecision::Proceed => {}
        IdempotencyDecision::Replay(_) => {
            return Json(connector_view(&state, entry)).into_response();
        }
        IdempotencyDecision::Blocked(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
        IdempotencyDecision::Unavailable => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "sqlite_unavailable" })),
            )
                .into_response();
        }
    }
    // Disconnect flips only the connection flag; linked accounts
    // stay linked, so reconnecting needs no fresh link.
    state
        .connector_connections
        .lock()
        .expect("connector connections lock")
        .insert(id.to_string(), false);
    persist_connector_line(
        &state,
        &ConnectorStateLine::Connection(ConnectorConnection {
            connector_id: id.to_string(),
            connected: false,
            updated_ms: now_ms(),
        }),
    );
    // Phase 2: a successful disconnect mirrors into an Effect.
    record_connector_effect(&state, None, "connectors", "disconnect", &id);
    Json(connector_view(&state, entry)).into_response()
}


// --- Vault: credentials, catalog, custom connectors (Task 10) ---
//
// Metadata lives in vault_store.rs (tagged JSONL); secrets live
// only in the secret backend (OS keychain in production, memory
// in tests). No handler ever returns, logs, or echoes a secret:
// responses carry the handle, provider, timestamps, and the
// backend's redacted last-4 display only. Slot deletes remove
// the secret from the backend for real; catalog and
// custom-connector deletes are metadata tombstones only.

pub(crate) fn persist_vault_line(state: &AppState, line: &VaultStateLine) {
    let Some(path) = &state.vault_state_path else { return };
    append_vault_state(path, line);
}

pub(crate) fn unknown_credential() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown credential" }))).into_response()
}

pub(crate) fn unknown_catalog_entry() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown catalog entry" }))).into_response()
}

pub(crate) fn unknown_custom_connector() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown custom connector" }))).into_response()
}

pub(crate) fn unknown_ticket() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown capture ticket" }))).into_response()
}

pub(crate) fn store_failed() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "credential store failed" }))).into_response()
}

#[derive(Debug, Serialize)]
pub(crate) struct CredentialSlotView {
    handle: String,
    provider: Option<String>,
    redacted: Option<String>,
    created_ms: u64,
    updated_ms: u64,
}

pub(crate) fn credential_view(state: &AppState, slot: &CredentialSlot) -> CredentialSlotView {
    CredentialSlotView {
        handle: slot.handle.clone(),
        provider: slot.provider.clone(),
        redacted: state.vault_secrets.redacted(&slot.handle).ok(),
        created_ms: slot.created_ms,
        updated_ms: slot.updated_ms,
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct CatalogView {
    id: String,
    display_name: String,
    description: Option<String>,
    created_ms: u64,
    updated_ms: u64,
}

pub(crate) fn catalog_view(entry: &CatalogEntry) -> CatalogView {
    CatalogView {
        id: entry.id.clone(),
        display_name: entry.display_name.clone(),
        description: entry.description.clone(),
        created_ms: entry.created_ms,
        updated_ms: entry.updated_ms,
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct CustomConnectorView {
    id: String,
    display_name: String,
    description: Option<String>,
    created_ms: u64,
    updated_ms: u64,
}

pub(crate) fn custom_connector_view(def: &CustomConnectorDef) -> CustomConnectorView {
    CustomConnectorView {
        id: def.id.clone(),
        display_name: def.display_name.clone(),
        description: def.description.clone(),
        created_ms: def.created_ms,
        updated_ms: def.updated_ms,
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct TicketView {
    ticket_id: String,
    provider: String,
    handle: String,
    status: TicketStatus,
    created_ms: u64,
    expires_ms: u64,
}

pub(crate) fn ticket_view(ticket: &CaptureTicket) -> TicketView {
    TicketView {
        ticket_id: ticket.ticket_id.clone(),
        provider: ticket.provider.clone(),
        handle: ticket.handle.clone(),
        status: ticket.status,
        created_ms: ticket.created_ms,
        expires_ms: ticket.expires_ms,
    }
}

/// A provider id appearing on a credential implies a catalog
/// entry; create it (display name defaults to the id) when absent.
/// There is no catalog create route in the spec, so this is how
/// entries come to exist for catalog/update/delete to manage.
pub(crate) fn ensure_catalog_entry(state: &AppState, provider: &str, now: u64) {
    let mut catalog = state.vault_catalog.lock().expect("vault catalog lock");
    if catalog.get(provider).is_some_and(|e| !e.deleted) {
        return;
    }
    let entry = CatalogEntry {
        id: provider.to_string(),
        display_name: provider.to_string(),
        description: None,
        created_ms: now,
        updated_ms: now,
        deleted: false,
    };
    catalog.insert(provider.to_string(), entry.clone());
    persist_vault_line(state, &VaultStateLine::Catalog(entry));
}

pub(crate) fn live_slot(slots: &HashMap<String, CredentialSlot>, handle: &str) -> Option<CredentialSlot> {
    slots.get(handle).filter(|s| !s.deleted).cloned()
}

#[derive(Debug, Deserialize)]
pub(crate) struct CustomRegisterDto {
    id: String,
    display_name: String,
    description: Option<String>,
}

pub(crate) async fn vault_custom_register(
    State(state): State<AppState>,
    Json(req): Json<CustomRegisterDto>,
) -> impl IntoResponse {
    let id = req.id.trim().to_string();
    let display_name = req.display_name.trim().to_string();
    if id.is_empty() || display_name.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "id and display_name must not be empty" }))).into_response();
    }
    let mut custom = state.vault_custom.lock().expect("vault custom lock");
    if custom.get(&id).is_some_and(|d| !d.deleted) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "custom connector already exists" }))).into_response();
    }
    let now = now_ms();
    let def = CustomConnectorDef {
        id: id.clone(),
        display_name,
        description: req.description.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
        created_ms: now,
        updated_ms: now,
        deleted: false,
    };
    custom.insert(id, def.clone());
    persist_vault_line(&state, &VaultStateLine::CustomConnector(def.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the custom map).
    drop(custom);
    // Phase 2: a successful custom-connector registration mirrors
    // into an Effect.
    record_connector_effect(&state, None, "custom-connectors", "register", &def.id);
    Json(custom_connector_view(&def)).into_response()
}

pub(crate) async fn vault_custom_list(State(state): State<AppState>) -> Json<Vec<CustomConnectorView>> {
    let mut defs: Vec<CustomConnectorView> = state
        .vault_custom
        .lock()
        .expect("vault custom lock")
        .values()
        .filter(|d| !d.deleted)
        .map(custom_connector_view)
        .collect();
    defs.sort_by(|a, b| a.id.cmp(&b.id));
    Json(defs)
}

#[derive(Debug, Deserialize)]
pub(crate) struct CustomDeleteDto {
    id: String,
}

pub(crate) async fn vault_custom_delete(
    State(state): State<AppState>,
    Json(req): Json<CustomDeleteDto>,
) -> impl IntoResponse {
    let mut custom = state.vault_custom.lock().expect("vault custom lock");
    let Some(def) = custom.get_mut(&req.id) else {
        return unknown_custom_connector();
    };
    if def.deleted {
        return unknown_custom_connector();
    }
    def.deleted = true;
    let updated = def.clone();
    persist_vault_line(&state, &VaultStateLine::CustomConnector(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the custom map).
    drop(custom);
    // Phase 2: a successful custom-connector delete mirrors into
    // an Effect.
    record_connector_effect(&state, None, "custom-connectors", "delete", &req.id);
    Json(custom_connector_view(&updated)).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct CredentialRegisterDto {
    handle: String,
    secret: String,
    provider: Option<String>,
}

pub(crate) async fn vault_credentials_register(
    State(state): State<AppState>,
    Json(req): Json<CredentialRegisterDto>,
) -> impl IntoResponse {
    let handle = req.handle.trim().to_string();
    if handle.is_empty() || req.secret.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "handle and secret must not be empty" }))).into_response();
    }
    let provider = req.provider.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let mut slots = state.vault_slots.lock().expect("vault slots lock");
    if slots.get(&handle).is_some_and(|s| !s.deleted) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "credential already exists" }))).into_response();
    }
    if state.vault_secrets.store(&handle, &req.secret).is_err() {
        return store_failed();
    }
    let now = now_ms();
    let slot = CredentialSlot {
        handle: handle.clone(),
        provider: provider.clone(),
        created_ms: now,
        updated_ms: now,
        deleted: false,
    };
    slots.insert(handle, slot.clone());
    persist_vault_line(&state, &VaultStateLine::Credential(slot.clone()));
    drop(slots);
    if let Some(provider) = &provider {
        ensure_catalog_entry(&state, provider, now);
    }
    // Phase 2: a successful credential registration mirrors into
    // an Effect. The incidental catalog ensure above is not a
    // second Effect: one call, one Effect. No secret material ever
    // enters an Effect — only the handle.
    record_connector_effect(&state, None, "credentials", "register", &slot.handle);
    Json(credential_view(&state, &slot)).into_response()
}

pub(crate) async fn vault_credentials_list(State(state): State<AppState>) -> Json<Vec<CredentialSlotView>> {
    let slots = state.vault_slots.lock().expect("vault slots lock");
    let mut views: Vec<CredentialSlotView> = slots
        .values()
        .filter(|s| !s.deleted)
        .map(|s| credential_view(&state, s))
        .collect();
    views.sort_by(|a, b| a.handle.cmp(&b.handle));
    Json(views)
}

#[derive(Debug, Deserialize)]
pub(crate) struct CredentialDeleteDto {
    handle: String,
}

pub(crate) async fn vault_credentials_delete(
    State(state): State<AppState>,
    Json(req): Json<CredentialDeleteDto>,
) -> impl IntoResponse {
    let mut slots = state.vault_slots.lock().expect("vault slots lock");
    let Some(slot) = slots.get_mut(&req.handle) else {
        return unknown_credential();
    };
    if slot.deleted {
        return unknown_credential();
    }
    // Real erasure: a missing backend entry is already-gone, any
    // other backend failure aborts before the tombstone.
    match state.vault_secrets.remove(&req.handle) {
        Ok(()) => {}
        Err(err) if err.code == agent_desktop::ErrorCode::InvalidInput => {}
        Err(_) => return store_failed(),
    }
    slot.deleted = true;
    let updated = slot.clone();
    persist_vault_line(&state, &VaultStateLine::Credential(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the slots map).
    drop(slots);
    // Phase 2: a successful credential delete (real backend
    // erasure + tombstone) mirrors into an Effect.
    record_connector_effect(&state, None, "credentials", "delete", &req.handle);
    Json(credential_view(&state, &updated)).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct CredentialCaptureDto {
    handle: String,
    secret: String,
}

pub(crate) async fn vault_credentials_capture(
    State(state): State<AppState>,
    Json(req): Json<CredentialCaptureDto>,
) -> impl IntoResponse {
    if req.secret.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "secret must not be empty" }))).into_response();
    }
    let mut slots = state.vault_slots.lock().expect("vault slots lock");
    let Some(slot) = slots.get_mut(&req.handle) else {
        return unknown_credential();
    };
    if slot.deleted {
        return unknown_credential();
    }
    if state.vault_secrets.store(&req.handle, &req.secret).is_err() {
        return store_failed();
    }
    slot.updated_ms = now_ms();
    let updated = slot.clone();
    persist_vault_line(&state, &VaultStateLine::Credential(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the slots map).
    drop(slots);
    // Phase 2: a successful credential capture (secret rotation)
    // mirrors into an Effect; the secret itself never does.
    record_connector_effect(&state, None, "credentials", "capture", &req.handle);
    Json(credential_view(&state, &updated)).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct CaptureRequestDto {
    provider: String,
    handle: String,
}

pub(crate) async fn vault_capture_request(
    State(state): State<AppState>,
    Json(req): Json<CaptureRequestDto>,
) -> impl IntoResponse {
    let provider = req.provider.trim().to_string();
    if provider.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "provider must not be empty" }))).into_response();
    }
    {
        let slots = state.vault_slots.lock().expect("vault slots lock");
        if live_slot(&slots, &req.handle).is_none() {
            return unknown_credential();
        }
    }
    let mut next = state.next_ticket_id.lock().expect("ticket id lock");
    let now = now_ms();
    let ticket = CaptureTicket {
        ticket_id: format!("ticket-{}", *next),
        provider,
        handle: req.handle,
        status: TicketStatus::Pending,
        created_ms: now,
        expires_ms: now + TICKET_TTL_MS,
    };
    *next += 1;
    state
        .vault_tickets
        .lock()
        .expect("vault tickets lock")
        .insert(ticket.ticket_id.clone(), ticket.clone());
    persist_vault_line(&state, &VaultStateLine::Ticket(ticket.clone()));
    Json(ticket_view(&ticket)).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct CaptureFulfillDto {
    ticket_id: String,
    secret: String,
}

pub(crate) async fn vault_capture_fulfill(
    State(state): State<AppState>,
    Json(req): Json<CaptureFulfillDto>,
) -> impl IntoResponse {
    let ticket = {
        let mut tickets = state.vault_tickets.lock().expect("vault tickets lock");
        let Some(ticket) = tickets.get_mut(&req.ticket_id) else {
            return unknown_ticket();
        };
        if ticket.status != TicketStatus::Pending {
            return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "ticket already fulfilled" }))).into_response();
        }
        if ticket_expired(ticket, now_ms()) {
            ticket.status = TicketStatus::Expired;
            let updated = ticket.clone();
            persist_vault_line(&state, &VaultStateLine::Ticket(updated));
            return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "ticket expired" }))).into_response();
        }
        ticket.clone()
    };
    if req.secret.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "secret must not be empty" }))).into_response();
    }
    let updated = {
        let mut slots = state.vault_slots.lock().expect("vault slots lock");
        let Some(slot) = slots.get_mut(&ticket.handle) else {
            return unknown_credential();
        };
        if slot.deleted {
            return unknown_credential();
        }
        if state.vault_secrets.store(&ticket.handle, &req.secret).is_err() {
            return store_failed();
        }
        slot.updated_ms = now_ms();
        let updated = slot.clone();
        persist_vault_line(&state, &VaultStateLine::Credential(updated.clone()));
        updated
    };
    let mut tickets = state.vault_tickets.lock().expect("vault tickets lock");
    let Some(ticket) = tickets.get_mut(&req.ticket_id) else {
        return unknown_ticket();
    };
    if ticket.status != TicketStatus::Pending {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "ticket already fulfilled" }))).into_response();
    }
    ticket.status = TicketStatus::Fulfilled;
    let updated_ticket = ticket.clone();
    persist_vault_line(&state, &VaultStateLine::Ticket(updated_ticket));
    // Phase 2: a successful capture fulfill mirrors into an
    // Effect for the materialized credential. The ticket request
    // itself is not an Effect: it mints an ephemeral exchange
    // token, and its durable outcome is this fulfill.
    record_connector_effect(&state, None, "credentials", "fulfill", &ticket.handle);
    Json(credential_view(&state, &updated)).into_response()
}

pub(crate) async fn vault_catalog_list(State(state): State<AppState>) -> Json<Vec<CatalogView>> {
    let mut entries: Vec<CatalogView> = state
        .vault_catalog
        .lock()
        .expect("vault catalog lock")
        .values()
        .filter(|e| !e.deleted)
        .map(catalog_view)
        .collect();
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    Json(entries)
}

#[derive(Debug, Deserialize)]
pub(crate) struct CatalogDetailsQuery {
    id: String,
}

pub(crate) async fn vault_catalog_details(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<CatalogDetailsQuery>,
) -> impl IntoResponse {
    let catalog = state.vault_catalog.lock().expect("vault catalog lock");
    match catalog.get(&query.id).filter(|e| !e.deleted) {
        Some(entry) => Json(catalog_view(entry)).into_response(),
        None => unknown_catalog_entry(),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CatalogUpdateDto {
    id: String,
    display_name: Option<String>,
    description: Option<String>,
}

pub(crate) async fn vault_catalog_update(
    State(state): State<AppState>,
    Json(req): Json<CatalogUpdateDto>,
) -> impl IntoResponse {
    let display_name_provided = req.display_name.is_some();
    let display_name = req.display_name.as_ref().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    if display_name_provided && display_name.is_none() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "display_name must not be empty" }))).into_response();
    }
    let mut catalog = state.vault_catalog.lock().expect("vault catalog lock");
    let Some(entry) = catalog.get_mut(&req.id) else {
        return unknown_catalog_entry();
    };
    if entry.deleted {
        return unknown_catalog_entry();
    }
    if let Some(display_name) = display_name {
        entry.display_name = display_name;
    }
    if let Some(description) = req.description {
        let description = description.trim().to_string();
        entry.description = if description.is_empty() { None } else { Some(description) };
    }
    entry.updated_ms = now_ms();
    let updated = entry.clone();
    persist_vault_line(&state, &VaultStateLine::Catalog(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the catalog map).
    drop(catalog);
    // Phase 2: a successful catalog update mirrors into an Effect.
    record_connector_effect(&state, None, "catalog", "update", &req.id);
    Json(catalog_view(&updated)).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct CatalogDeleteDto {
    id: String,
}

pub(crate) async fn vault_catalog_delete(
    State(state): State<AppState>,
    Json(req): Json<CatalogDeleteDto>,
) -> impl IntoResponse {
    let mut catalog = state.vault_catalog.lock().expect("vault catalog lock");
    let Some(entry) = catalog.get_mut(&req.id) else {
        return unknown_catalog_entry();
    };
    if entry.deleted {
        return unknown_catalog_entry();
    }
    entry.deleted = true;
    let updated = entry.clone();
    persist_vault_line(&state, &VaultStateLine::Catalog(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the catalog map).
    drop(catalog);
    // Phase 2: a successful catalog delete mirrors into an Effect.
    record_connector_effect(&state, None, "catalog", "delete", &req.id);
    Json(catalog_view(&updated)).into_response()
}


pub(crate) async fn connectors_permissions(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if registry_entry(&id).is_none() {
        return unknown_connector();
    }
    Json(connector_policy_view(&state, &id)).into_response()
}
