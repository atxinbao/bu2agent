import { invoke } from "@tauri-apps/api/core";
import type {
  Peer,
  PeerGateway,
  PeerInboxMessage,
  PeerInvite,
  PeerRequest,
  PeerStatus,
} from "./peer_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

interface ServerPeer {
  muse_id: string;
  display_name: string;
  status: string;
  updated_ms: number;
  disconnected?: boolean;
}

interface ServerPeerInvite {
  invite_id: string;
  muse_id: string;
  display_name: string;
  status: string;
  created_ms: number;
  updated_ms: number;
  revoked?: boolean;
}

interface ServerPeerInboxMessage {
  message_id: string;
  from_muse_id: string;
  text: string;
  status: string;
  created_ms: number;
}

interface ServerPeerRequest {
  relationship_generation_id: string;
  muse_id: string;
  display_name: string;
  status: string;
  created_ms: number;
}

interface ServerPeerStatus {
  peers: number;
  pending_invites: number;
  pending_inbox: number;
  pending_requests: number;
}

function toPeer(server: ServerPeer): Peer {
  return {
    museId: server.muse_id,
    displayName: server.display_name,
    status: server.status,
    updatedMs: server.updated_ms,
    disconnected: server.disconnected ?? false,
  };
}

function toPeerInvite(server: ServerPeerInvite): PeerInvite {
  return {
    inviteId: server.invite_id,
    museId: server.muse_id,
    displayName: server.display_name,
    status: server.status,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    revoked: server.revoked ?? false,
  };
}

function toPeerInboxMessage(server: ServerPeerInboxMessage): PeerInboxMessage {
  return {
    messageId: server.message_id,
    fromMuseId: server.from_muse_id,
    text: server.text,
    status: server.status,
    createdMs: server.created_ms,
  };
}

function toPeerRequest(server: ServerPeerRequest): PeerRequest {
  return {
    relationshipGenerationId: server.relationship_generation_id,
    museId: server.muse_id,
    displayName: server.display_name,
    status: server.status,
    createdMs: server.created_ms,
  };
}

function toPeerStatus(server: ServerPeerStatus): PeerStatus {
  return {
    peers: server.peers,
    pendingInvites: server.pending_invites,
    pendingInbox: server.pending_inbox,
    pendingRequests: server.pending_requests,
  };
}

export class InvokePeerClient implements PeerGateway {
  async listPeers(): Promise<Peer[]> {
    return (await invoke<ServerPeer[]>("peer_list")).map(toPeer);
  }

  async getPeer(museId: string): Promise<Peer> {
    return toPeer(await invoke<ServerPeer>("peer_get", { muse_id: museId }));
  }

  async updatePeer(museId: string, displayName?: string, status?: string): Promise<Peer> {
    return toPeer(
      await invoke<ServerPeer>("peer_update", {
        muse_id: museId,
        display_name: displayName,
        status,
      }),
    );
  }

  async disconnectPeer(museId: string): Promise<Peer> {
    return toPeer(await invoke<ServerPeer>("peer_disconnect", { muse_id: museId }));
  }

  async listInvites(): Promise<PeerInvite[]> {
    return (await invoke<ServerPeerInvite[]>("peer_invites_list")).map(toPeerInvite);
  }

  async createInvite(museId: string, displayName: string): Promise<PeerInvite> {
    return toPeerInvite(
      await invoke<ServerPeerInvite>("peer_invite_create", {
        muse_id: museId,
        display_name: displayName,
      }),
    );
  }

  async acceptInvite(inviteId: string): Promise<PeerInvite> {
    return toPeerInvite(
      await invoke<ServerPeerInvite>("peer_invite_accept", { invite_id: inviteId }),
    );
  }

  async openInvite(inviteId: string): Promise<PeerInvite> {
    return toPeerInvite(
      await invoke<ServerPeerInvite>("peer_invite_open", { invite_id: inviteId }),
    );
  }

  async revokeInvite(inviteId: string): Promise<PeerInvite> {
    return toPeerInvite(
      await invoke<ServerPeerInvite>("peer_invite_revoke", { invite_id: inviteId }),
    );
  }

  async listInbox(): Promise<PeerInboxMessage[]> {
    return (await invoke<ServerPeerInboxMessage[]>("peer_inbox_list")).map(toPeerInboxMessage);
  }

  async deliverInbox(fromMuseId: string, text: string): Promise<PeerInboxMessage> {
    return toPeerInboxMessage(
      await invoke<ServerPeerInboxMessage>("peer_inbox_deliver", {
        from_muse_id: fromMuseId,
        text,
      }),
    );
  }

  async declineInbox(messageId: string): Promise<PeerInboxMessage> {
    return toPeerInboxMessage(
      await invoke<ServerPeerInboxMessage>("peer_inbox_decline", { message_id: messageId }),
    );
  }

  async listRequests(): Promise<PeerRequest[]> {
    return (await invoke<ServerPeerRequest[]>("peer_requests_list")).map(toPeerRequest);
  }

  async getStatus(): Promise<PeerStatus> {
    return toPeerStatus(await invoke<ServerPeerStatus>("peer_status"));
  }
}
