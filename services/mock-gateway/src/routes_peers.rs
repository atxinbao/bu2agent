//! Peers routes: /peers (14) + /api/nodes (5) = 19 routes.
//!
//! The handlers moved here from `server.rs` (FU-5 S-9b). No
//! directory lines moved with this batch: the `//!` header has
//! no peers/nodes entries (S-8 precedent); the directory total
//! stays 59 (36 server.rs + 13 files + 1 misc + 9 chat).
//! `persist_binding` / `persist_project` / `persist_location` /
//! `persist_worktree` stay in `server.rs` (S-8 staying helpers):
//! they sit between the two node ranges and are untouched.
//! `conflict` (one arg) moves with the peers block; the two-arg
//! `conflict(...)` calls in `bound_write_gate` are a separate
//! local closure, not this fn. No type needs importing back:
//! `server.rs` keeps Peer/Node imported from the stores for
//! the AppState field types.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::node_store::{append_node, Node, NODE_DISCONNECTED, NODE_ONLINE};
use crate::peer_store::{
    append_peer_state, next_invite_number, next_message_number, Peer, PeerInboxMessage,
    PeerInvite, PeerRequest, PeerStateLine,
};
use crate::routes_common::{bad_request, now_ms};
use crate::server::AppState;


// --- Peers surface (Task 15, record subset) ---
//
// Local semantics only (see peer_store.rs): peers form by
// accepting an invite; an invite doubles as its request
// record; inbox deliver creates a pending item, decline
// resolves it. SAS, avatar/photo, and the fs/write
// discretion route are excluded.

pub(crate) fn persist_peer_line(state: &AppState, line: &PeerStateLine) {
    let Some(path) = &state.peer_state_path else { return };
    append_peer_state(path, line);
}

pub(crate) fn peer_not_found() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown peer" })))
}

pub(crate) fn invite_not_found() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown invite" })))
}

pub(crate) fn message_not_found() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown inbox message" })))
}

pub(crate) fn conflict(message: &str) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::CONFLICT, Json(serde_json::json!({ "error": message })))
}

pub(crate) async fn peers_list(State(state): State<AppState>) -> Json<Vec<Peer>> {
    let mut peers: Vec<Peer> = state
        .peer_peers
        .lock()
        .expect("peer lock")
        .values()
        .filter(|p| !p.disconnected)
        .cloned()
        .collect();
    peers.sort_by(|a, b| a.muse_id.cmp(&b.muse_id));
    Json(peers)
}

pub(crate) async fn peers_get(
    State(state): State<AppState>,
    axum::extract::Path(muse_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.peer_peers.lock().expect("peer lock").get(&muse_id) {
        Some(peer) => Json(peer.clone()).into_response(),
        None => peer_not_found().into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct PeerUpdateDto {
    display_name: Option<String>,
    status: Option<String>,
}

pub(crate) async fn peers_update(
    State(state): State<AppState>,
    axum::extract::Path(muse_id): axum::extract::Path<String>,
    Json(req): Json<PeerUpdateDto>,
) -> impl IntoResponse {
    let display_name = match req.display_name {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                return bad_request("display_name must not be empty").into_response();
            }
            Some(trimmed)
        }
        None => None,
    };
    let status = match req.status {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                return bad_request("status must not be empty").into_response();
            }
            Some(trimmed)
        }
        None => None,
    };
    let mut peers = state.peer_peers.lock().expect("peer lock");
    let Some(peer) = peers.get_mut(&muse_id) else {
        return peer_not_found().into_response();
    };
    if peer.disconnected {
        return peer_not_found().into_response();
    }
    if let Some(name) = display_name {
        peer.display_name = name;
    }
    if let Some(status) = status {
        peer.status = status;
    }
    peer.updated_ms = now_ms();
    let updated = peer.clone();
    persist_peer_line(&state, &PeerStateLine::Peer(updated.clone()));
    Json(updated).into_response()
}

pub(crate) async fn peers_disconnect(
    State(state): State<AppState>,
    axum::extract::Path(muse_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut peers = state.peer_peers.lock().expect("peer lock");
    let Some(peer) = peers.get_mut(&muse_id) else {
        return peer_not_found().into_response();
    };
    // Idempotent: disconnecting an already-disconnected
    // peer succeeds without a new record.
    if !peer.disconnected {
        peer.disconnected = true;
        peer.updated_ms = now_ms();
        let updated = peer.clone();
        persist_peer_line(&state, &PeerStateLine::Peer(updated.clone()));
        return Json(updated).into_response();
    }
    Json(peer.clone()).into_response()
}

pub(crate) async fn peers_invites_list(State(state): State<AppState>) -> Json<Vec<PeerInvite>> {
    let mut invites: Vec<PeerInvite> = state
        .peer_invites
        .lock()
        .expect("peer lock")
        .values()
        .filter(|i| !i.revoked)
        .cloned()
        .collect();
    invites.sort_by_key(|i| (i.created_ms, i.invite_id.clone()));
    Json(invites)
}

#[derive(Debug, Deserialize)]
pub(crate) struct PeerInviteCreateDto {
    muse_id: String,
    display_name: String,
}

pub(crate) async fn peers_invites_create(
    State(state): State<AppState>,
    Json(req): Json<PeerInviteCreateDto>,
) -> impl IntoResponse {
    let muse_id = req.muse_id.trim().to_string();
    let display_name = req.display_name.trim().to_string();
    if muse_id.is_empty() || display_name.is_empty() {
        return bad_request("muse_id and display_name must not be empty").into_response();
    }
    let now = now_ms();
    let invite_id = {
        let invites = state.peer_invites.lock().expect("peer lock");
        let records: Vec<PeerInvite> = invites.values().cloned().collect();
        format!("invite-{}", next_invite_number(&records))
    };
    let invite = PeerInvite {
        invite_id: invite_id.clone(),
        muse_id: muse_id.clone(),
        display_name: display_name.clone(),
        status: "pending".to_string(),
        created_ms: now,
        updated_ms: now,
        revoked: false,
    };
    let request = PeerRequest {
        relationship_generation_id: invite_id.clone(),
        muse_id,
        display_name,
        status: "pending".to_string(),
        created_ms: now,
    };
    state.peer_invites.lock().expect("peer lock").insert(invite_id, invite.clone());
    state.peer_requests.lock().expect("peer lock").insert(request.relationship_generation_id.clone(), request.clone());
    persist_peer_line(&state, &PeerStateLine::Invite(invite.clone()));
    persist_peer_line(&state, &PeerStateLine::Request(request));
    Json(invite).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct PeerInviteIdDto {
    invite_id: String,
}

pub(crate) async fn peers_invites_accept(
    State(state): State<AppState>,
    Json(req): Json<PeerInviteIdDto>,
) -> impl IntoResponse {
    let mut invites = state.peer_invites.lock().expect("peer lock");
    let Some(invite) = invites.get_mut(&req.invite_id) else {
        return invite_not_found().into_response();
    };
    if invite.revoked {
        return conflict("invite revoked").into_response();
    }
    // Idempotent: accepting an already-accepted invite
    // succeeds without duplicating the peer.
    if invite.status != "accepted" {
        invite.status = "accepted".to_string();
        invite.updated_ms = now_ms();
        let updated = invite.clone();
        let mut peers = state.peer_peers.lock().expect("peer lock");
        let peer = Peer {
            muse_id: updated.muse_id.clone(),
            display_name: updated.display_name.clone(),
            status: "connected".to_string(),
            updated_ms: now_ms(),
            disconnected: false,
        };
        peers.insert(peer.muse_id.clone(), peer.clone());
        let mut requests = state.peer_requests.lock().expect("peer lock");
        if let Some(request) = requests.get_mut(&req.invite_id) {
            request.status = "accepted".to_string();
            let request = request.clone();
            persist_peer_line(&state, &PeerStateLine::Request(request));
        }
        persist_peer_line(&state, &PeerStateLine::Invite(updated.clone()));
        persist_peer_line(&state, &PeerStateLine::Peer(peer));
        return Json(updated).into_response();
    }
    Json(invite.clone()).into_response()
}

pub(crate) async fn peers_invites_open(
    State(state): State<AppState>,
    Json(req): Json<PeerInviteIdDto>,
) -> impl IntoResponse {
    let mut invites = state.peer_invites.lock().expect("peer lock");
    let Some(invite) = invites.get_mut(&req.invite_id) else {
        return invite_not_found().into_response();
    };
    if invite.revoked {
        return conflict("invite revoked").into_response();
    }
    if invite.status == "accepted" {
        return conflict("invite already accepted").into_response();
    }
    if invite.status != "opened" {
        invite.status = "opened".to_string();
        invite.updated_ms = now_ms();
        let updated = invite.clone();
        persist_peer_line(&state, &PeerStateLine::Invite(updated.clone()));
        return Json(updated).into_response();
    }
    Json(invite.clone()).into_response()
}

pub(crate) async fn peers_invites_revoke(
    State(state): State<AppState>,
    axum::extract::Path(invite_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut invites = state.peer_invites.lock().expect("peer lock");
    let Some(invite) = invites.get_mut(&invite_id) else {
        return invite_not_found().into_response();
    };
    // Idempotent: revoking an already-revoked invite succeeds.
    if !invite.revoked {
        invite.revoked = true;
        invite.updated_ms = now_ms();
        let updated = invite.clone();
        let mut requests = state.peer_requests.lock().expect("peer lock");
        if let Some(request) = requests.get_mut(&invite_id) {
            request.status = "revoked".to_string();
            let request = request.clone();
            persist_peer_line(&state, &PeerStateLine::Request(request));
        }
        persist_peer_line(&state, &PeerStateLine::Invite(updated.clone()));
        return Json(updated).into_response();
    }
    Json(invite.clone()).into_response()
}

pub(crate) async fn peers_inbox_list(State(state): State<AppState>) -> Json<Vec<PeerInboxMessage>> {
    let mut messages: Vec<PeerInboxMessage> =
        state.peer_inbox.lock().expect("peer lock").values().cloned().collect();
    messages.sort_by_key(|m| (m.created_ms, m.message_id.clone()));
    Json(messages)
}

#[derive(Debug, Deserialize)]
pub(crate) struct PeerInboxDeliverDto {
    from_muse_id: String,
    text: String,
}

pub(crate) async fn peers_inbox_deliver(
    State(state): State<AppState>,
    Json(req): Json<PeerInboxDeliverDto>,
) -> impl IntoResponse {
    let from_muse_id = req.from_muse_id.trim().to_string();
    let text = req.text.trim().to_string();
    if from_muse_id.is_empty() || text.is_empty() {
        return bad_request("from_muse_id and text must not be empty").into_response();
    }
    let message_id = {
        let inbox = state.peer_inbox.lock().expect("peer lock");
        let records: Vec<PeerInboxMessage> = inbox.values().cloned().collect();
        format!("inbox-{}", next_message_number(&records))
    };
    let message = PeerInboxMessage {
        message_id: message_id.clone(),
        from_muse_id,
        text,
        status: "pending".to_string(),
        created_ms: now_ms(),
    };
    state.peer_inbox.lock().expect("peer lock").insert(message_id, message.clone());
    persist_peer_line(&state, &PeerStateLine::Inbox(message.clone()));
    Json(message).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct PeerInboxDeclineDto {
    message_id: String,
}

pub(crate) async fn peers_inbox_decline(
    State(state): State<AppState>,
    Json(req): Json<PeerInboxDeclineDto>,
) -> impl IntoResponse {
    let mut inbox = state.peer_inbox.lock().expect("peer lock");
    let Some(message) = inbox.get_mut(&req.message_id) else {
        return message_not_found().into_response();
    };
    // Idempotent: declining an already-declined message succeeds.
    if message.status != "declined" {
        message.status = "declined".to_string();
        let updated = message.clone();
        persist_peer_line(&state, &PeerStateLine::Inbox(updated.clone()));
        return Json(updated).into_response();
    }
    Json(message.clone()).into_response()
}

pub(crate) async fn peers_requests_list(State(state): State<AppState>) -> Json<Vec<PeerRequest>> {
    let mut requests: Vec<PeerRequest> =
        state.peer_requests.lock().expect("peer lock").values().cloned().collect();
requests.sort_by_key(|r| (r.created_ms, r.relationship_generation_id.clone()));
    Json(requests)
}

pub(crate) async fn peers_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let peers = state.peer_peers.lock().expect("peer lock").values().filter(|p| !p.disconnected).count();
    let pending_invites = state
        .peer_invites
        .lock()
        .expect("peer lock")
        .values()
        .filter(|i| !i.revoked && (i.status == "pending" || i.status == "opened"))
        .count();
    let pending_inbox = state
        .peer_inbox
        .lock()
        .expect("peer lock")
        .values()
        .filter(|m| m.status == "pending")
        .count();
    let pending_requests = state
        .peer_requests
        .lock()
        .expect("peer lock")
        .values()
        .filter(|r| r.status == "pending")
        .count();
    Json(serde_json::json!({
        "peers": peers,
        "pending_invites": pending_invites,
        "pending_inbox": pending_inbox,
        "pending_requests": pending_requests,
    }))
}


// --- Node registry surface (Task 19, record subset) ---
//
// Invoke-ack, invoke-result, and sync are excluded (see
// the Task 19 plan): receipt/sync protocol with a real
// node, and there is no local counterpart.

pub(crate) fn persist_node(state: &AppState, record: &Node) {
    let Some(path) = &state.node_state_path else { return };
    append_node(path, record);
}


pub(crate) fn node_number(node_id: &str) -> u64 {
    node_id.strip_prefix("node-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

#[derive(Debug, Deserialize)]
pub(crate) struct NodeRegisterDto {
    name: Option<String>,
}

pub(crate) async fn nodes_register(
    State(state): State<AppState>,
    Json(req): Json<NodeRegisterDto>,
) -> Json<Node> {
    let mut next = state.next_node_id.lock().expect("node id lock");
    let id = format!("node-{}", *next);
    *next += 1;
    let now = now_ms();
    let record = Node {
        node_id: id.clone(),
        name: req.name.filter(|n| !n.trim().is_empty()).unwrap_or(id),
        status: NODE_ONLINE.to_string(),
        last_heartbeat_ms: now,
        created_ms: now,
        updated_ms: now,
        unpaired: false,
    };
    state.nodes.lock().expect("node lock").insert(record.node_id.clone(), record.clone());
    persist_node(&state, &record);
    Json(record)
}

#[derive(Debug, Deserialize)]
pub(crate) struct NodeListDto {
    status: Option<String>,
}

pub(crate) async fn nodes_list(
    State(state): State<AppState>,
    Json(req): Json<NodeListDto>,
) -> impl IntoResponse {
    if let Some(status) = &req.status {
        if status != NODE_ONLINE && status != NODE_DISCONNECTED {
            return bad_request("unknown node status").into_response();
        }
    }
    let nodes = state.nodes.lock().expect("node lock");
    let mut records: Vec<Node> = nodes
        .values()
        .filter(|n| !n.unpaired && req.status.as_ref().is_none_or(|s| s == &n.status))
        .cloned()
        .collect();
    records.sort_by_key(|n| node_number(&n.node_id));
    Json(records).into_response()
}

#[derive(Debug, Deserialize)]
pub(crate) struct NodeIdDto {
    node_id: String,
}

/// Shared mutation path for heartbeat/disconnect: unknown
/// ids and tombstoned nodes both read as 404; the mutation
/// bumps `updated_ms` and appends a fresh snapshot.
pub(crate) fn mutate_node(state: &AppState, node_id: &str, mutate: impl FnOnce(&mut Node)) -> Response {
    let mut nodes = state.nodes.lock().expect("node lock");
    let Some(record) = nodes.get_mut(node_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown node" })))
            .into_response();
    };
    if record.unpaired {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown node" })))
            .into_response();
    }
    mutate(record);
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_node(state, &updated);
    Json(updated).into_response()
}

pub(crate) async fn nodes_heartbeat(
    State(state): State<AppState>,
    Json(req): Json<NodeIdDto>,
) -> impl IntoResponse {
    mutate_node(&state, &req.node_id, |n| {
        n.status = NODE_ONLINE.to_string();
        n.last_heartbeat_ms = now_ms();
    })
}

pub(crate) async fn nodes_disconnect(
    State(state): State<AppState>,
    Json(req): Json<NodeIdDto>,
) -> impl IntoResponse {
    mutate_node(&state, &req.node_id, |n| {
        n.status = NODE_DISCONNECTED.to_string();
    })
}

pub(crate) async fn nodes_unpair(
    State(state): State<AppState>,
    Json(req): Json<NodeIdDto>,
) -> impl IntoResponse {
    let mut nodes = state.nodes.lock().expect("node lock");
    let Some(record) = nodes.get_mut(&req.node_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown node" })))
            .into_response();
    };
    if !record.unpaired {
        record.unpaired = true;
        record.updated_ms = now_ms();
        let updated = record.clone();
        persist_node(&state, &updated);
    }
    // Re-unpair is idempotent success, mirroring the
    // delete-turn tombstone: no new snapshot, same record.
    Json(record.clone()).into_response()
}
