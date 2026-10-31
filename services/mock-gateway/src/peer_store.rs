//! Peer relationship state: local peer records, invites,
//! inbox items, and relationship requests (Task 15, peers
//! record subset).
//!
//! Only the record subset of the spec's peers surface is
//! implemented. SAS confirm, avatar/photo by sha256, and
//! the `POST /fs/write` discretion route are excluded:
//! SAS is a real crypto protocol, avatar/photo are content
//! addressing, and the fs/write path is a coincidence, not
//! a peer semantic (see the coverage table).
//!
//! Local semantics (no runtime evidence; not claimed as
//! original-product behavior):
//! - Peers come into existence only by accepting an
//!   invite; there is no POST /peers route in the spec.
//! - An invite IS a relationship attempt: creating one
//!   also creates a request record, and the invite_id
//!   doubles as the relationship_generation_id, so the
//!   invites list and the requests list are two views of
//!   the same attempt.
//! - Inbox deliver creates a pending item; decline
//!   resolves it. There is no separate inbox-accept route.
//! - Disconnect, invite revoke are tombstones (list hides
//!   them); accept/open/revoke on a finished invite follow
//!   the idempotent-or-409 rules in server.rs.
//! - Persistence mirrors the connector store: tagged
//!   JSONL snapshots in one file, last-write-wins.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    pub muse_id: String,
    pub display_name: String,
    pub status: String,
    pub updated_ms: u64,
    #[serde(default)]
    pub disconnected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInvite {
    pub invite_id: String,
    pub muse_id: String,
    pub display_name: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInboxMessage {
    pub message_id: String,
    pub from_muse_id: String,
    pub text: String,
    pub status: String,
    pub created_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRequest {
    pub relationship_generation_id: String,
    pub muse_id: String,
    pub display_name: String,
    pub status: String,
    pub created_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum PeerStateLine {
    #[serde(rename = "peer")]
    Peer(Peer),
    #[serde(rename = "invite")]
    Invite(PeerInvite),
    #[serde(rename = "inbox")]
    Inbox(PeerInboxMessage),
    #[serde(rename = "request")]
    Request(PeerRequest),
}

/// Load peer state; malformed lines are skipped with a
/// warning, mirroring the other stores.
pub fn load_peer_state(
    path: &Path,
) -> (Vec<Peer>, Vec<PeerInvite>, Vec<PeerInboxMessage>, Vec<PeerRequest>) {
    let Ok(text) = std::fs::read_to_string(path) else {
        return (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    };
    let mut peers = Vec::new();
    let mut invites = Vec::new();
    let mut inbox = Vec::new();
    let mut requests = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<PeerStateLine>(line) {
            Ok(PeerStateLine::Peer(record)) => peers.push(record),
            Ok(PeerStateLine::Invite(record)) => invites.push(record),
            Ok(PeerStateLine::Inbox(record)) => inbox.push(record),
            Ok(PeerStateLine::Request(record)) => requests.push(record),
            Err(_) => eprintln!("warning: skipping malformed peer state line in {}", path.display()),
        }
    }
    (peers, invites, inbox, requests)
}

/// Append one state snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_peer_state(path: &Path, line: &PeerStateLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append peer state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append peer state to {}: {err}", path.display());
    }
}

fn numeric_suffix(id: &str, prefix: &str) -> u64 {
    id.strip_prefix(prefix).and_then(|s| s.parse().ok()).unwrap_or(0)
}

pub fn next_invite_number(invites: &[PeerInvite]) -> u64 {
    invites.iter().map(|i| numeric_suffix(&i.invite_id, "invite-")).max().unwrap_or(0) + 1
}

pub fn next_message_number(messages: &[PeerInboxMessage]) -> u64 {
    messages.iter().map(|m| numeric_suffix(&m.message_id, "inbox-")).max().unwrap_or(0) + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_line_roundtrip() {
        let peer = Peer {
            muse_id: "muse-1".to_string(),
            display_name: "Ada".to_string(),
            status: "connected".to_string(),
            updated_ms: 7,
            disconnected: false,
        };
        let line = serde_json::to_string(&PeerStateLine::Peer(peer)).unwrap();
        assert!(line.contains("\"kind\":\"peer\""));
        let parsed: PeerStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, PeerStateLine::Peer(_)));
    }

    #[test]
    fn next_numbers_skip_gaps() {
        let invites = vec![PeerInvite {
            invite_id: "invite-4".to_string(),
            muse_id: "m".to_string(),
            display_name: "M".to_string(),
            status: "pending".to_string(),
            created_ms: 1,
            updated_ms: 1,
            revoked: false,
        }];
        assert_eq!(next_invite_number(&invites), 5);
        assert_eq!(next_message_number(&[]), 1);
    }
}
