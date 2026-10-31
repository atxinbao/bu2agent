//! Peer client: Rust shell -> Mock Gateway peers surface
//! (Task 15, record subset).
//!
//! Mirrors the connector client: no approval loop (local
//! relationship records, like the other metadata surfaces),
//! and the base URL is allowlisted to the localhost Mock.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub muse_id: String,
    pub display_name: String,
    pub status: String,
    pub updated_ms: u64,
    #[serde(default)]
    pub disconnected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerInboxMessage {
    pub message_id: String,
    pub from_muse_id: String,
    pub text: String,
    pub status: String,
    pub created_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerRequest {
    pub relationship_generation_id: String,
    pub muse_id: String,
    pub display_name: String,
    pub status: String,
    pub created_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerStatus {
    pub peers: usize,
    pub pending_invites: usize,
    pub pending_inbox: usize,
    pub pending_requests: usize,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct PeerClient {
    http: HttpClient,
}

impl PeerClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list_peers(&self) -> std::result::Result<Vec<Peer>, AgentError> {
        Ok(self.http.get_json("/peers")?)
    }

    pub fn get_peer(&self, muse_id: &str) -> std::result::Result<Peer, AgentError> {
        Ok(self.http.get_json(&format!("/peers/{muse_id}"))?)
    }

    pub fn update_peer(
        &self,
        muse_id: &str,
        display_name: Option<&str>,
        status: Option<&str>,
    ) -> std::result::Result<Peer, AgentError> {
        let body = serde_json::json!({ "display_name": display_name, "status": status }).to_string();
        Ok(self.http.patch_json(&format!("/peers/{muse_id}"), &body)?)
    }

    pub fn disconnect_peer(&self, muse_id: &str) -> std::result::Result<Peer, AgentError> {
        Ok(self.http.delete_json(&format!("/peers/{muse_id}"))?)
    }

    pub fn list_invites(&self) -> std::result::Result<Vec<PeerInvite>, AgentError> {
        Ok(self.http.get_json("/peers/invites")?)
    }

    pub fn create_invite(
        &self,
        muse_id: &str,
        display_name: &str,
    ) -> std::result::Result<PeerInvite, AgentError> {
        let body = serde_json::json!({ "muse_id": muse_id, "display_name": display_name }).to_string();
        Ok(self.http.post_json("/peers/invites", &body)?)
    }

    pub fn accept_invite(&self, invite_id: &str) -> std::result::Result<PeerInvite, AgentError> {
        let body = serde_json::json!({ "invite_id": invite_id }).to_string();
        Ok(self.http.post_json("/peers/invites/accept", &body)?)
    }

    pub fn open_invite(&self, invite_id: &str) -> std::result::Result<PeerInvite, AgentError> {
        let body = serde_json::json!({ "invite_id": invite_id }).to_string();
        Ok(self.http.post_json("/peers/invites/open", &body)?)
    }

    pub fn revoke_invite(&self, invite_id: &str) -> std::result::Result<PeerInvite, AgentError> {
        Ok(self.http.delete_json(&format!("/peers/invites/{invite_id}"))?)
    }

    pub fn list_inbox(&self) -> std::result::Result<Vec<PeerInboxMessage>, AgentError> {
        Ok(self.http.get_json("/peers/inbox")?)
    }

    pub fn deliver_inbox(
        &self,
        from_muse_id: &str,
        text: &str,
    ) -> std::result::Result<PeerInboxMessage, AgentError> {
        let body = serde_json::json!({ "from_muse_id": from_muse_id, "text": text }).to_string();
        Ok(self.http.post_json("/peers/inbox/deliver", &body)?)
    }

    pub fn decline_inbox(&self, message_id: &str) -> std::result::Result<PeerInboxMessage, AgentError> {
        let body = serde_json::json!({ "message_id": message_id }).to_string();
        Ok(self.http.post_json("/peers/inbox/decline", &body)?)
    }

    pub fn list_requests(&self) -> std::result::Result<Vec<PeerRequest>, AgentError> {
        Ok(self.http.get_json("/peers/requests")?)
    }

    pub fn get_status(&self) -> std::result::Result<PeerStatus, AgentError> {
        Ok(self.http.get_json("/peers/status")?)
    }
}
