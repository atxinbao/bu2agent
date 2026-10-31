// Peer client for the TS Domain Store (Task 15, record
// subset): registry, invites, inbox, requests, status.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view.

export interface Peer {
  museId: string;
  displayName: string;
  status: string;
  updatedMs: number;
  disconnected: boolean;
}

export interface PeerInvite {
  inviteId: string;
  museId: string;
  displayName: string;
  status: string;
  createdMs: number;
  updatedMs: number;
  revoked: boolean;
}

export interface PeerInboxMessage {
  messageId: string;
  fromMuseId: string;
  text: string;
  status: string;
  createdMs: number;
}

export interface PeerRequest {
  relationshipGenerationId: string;
  museId: string;
  displayName: string;
  status: string;
  createdMs: number;
}

export interface PeerStatus {
  peers: number;
  pendingInvites: number;
  pendingInbox: number;
  pendingRequests: number;
}

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

// Transport-neutral surface used by the Peers view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface PeerGateway {
  listPeers(): Promise<Peer[]>;
  getPeer(museId: string): Promise<Peer>;
  updatePeer(museId: string, displayName?: string, status?: string): Promise<Peer>;
  disconnectPeer(museId: string): Promise<Peer>;
  listInvites(): Promise<PeerInvite[]>;
  createInvite(museId: string, displayName: string): Promise<PeerInvite>;
  acceptInvite(inviteId: string): Promise<PeerInvite>;
  openInvite(inviteId: string): Promise<PeerInvite>;
  revokeInvite(inviteId: string): Promise<PeerInvite>;
  listInbox(): Promise<PeerInboxMessage[]>;
  deliverInbox(fromMuseId: string, text: string): Promise<PeerInboxMessage>;
  declineInbox(messageId: string): Promise<PeerInboxMessage>;
  listRequests(): Promise<PeerRequest[]>;
  getStatus(): Promise<PeerStatus>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class PeerClient implements PeerGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async listPeers(): Promise<Peer[]> {
    return (await this.request<ServerPeer[]>("GET", "/peers")).map(toPeer);
  }

  async getPeer(museId: string): Promise<Peer> {
    return toPeer(await this.request<ServerPeer>("GET", `/peers/${museId}`));
  }

  async updatePeer(museId: string, displayName?: string, status?: string): Promise<Peer> {
    return toPeer(
      await this.request<ServerPeer>("PATCH", `/peers/${museId}`, {
        display_name: displayName,
        status,
      }),
    );
  }

  async disconnectPeer(museId: string): Promise<Peer> {
    return toPeer(await this.request<ServerPeer>("DELETE", `/peers/${museId}`));
  }

  async listInvites(): Promise<PeerInvite[]> {
    return (await this.request<ServerPeerInvite[]>("GET", "/peers/invites")).map(toPeerInvite);
  }

  async createInvite(museId: string, displayName: string): Promise<PeerInvite> {
    return toPeerInvite(
      await this.request<ServerPeerInvite>("POST", "/peers/invites", {
        muse_id: museId,
        display_name: displayName,
      }),
    );
  }

  async acceptInvite(inviteId: string): Promise<PeerInvite> {
    return toPeerInvite(
      await this.request<ServerPeerInvite>("POST", "/peers/invites/accept", {
        invite_id: inviteId,
      }),
    );
  }

  async openInvite(inviteId: string): Promise<PeerInvite> {
    return toPeerInvite(
      await this.request<ServerPeerInvite>("POST", "/peers/invites/open", {
        invite_id: inviteId,
      }),
    );
  }

  async revokeInvite(inviteId: string): Promise<PeerInvite> {
    return toPeerInvite(
      await this.request<ServerPeerInvite>("DELETE", `/peers/invites/${inviteId}`),
    );
  }

  async listInbox(): Promise<PeerInboxMessage[]> {
    return (await this.request<ServerPeerInboxMessage[]>("GET", "/peers/inbox")).map(
      toPeerInboxMessage,
    );
  }

  async deliverInbox(fromMuseId: string, text: string): Promise<PeerInboxMessage> {
    return toPeerInboxMessage(
      await this.request<ServerPeerInboxMessage>("POST", "/peers/inbox/deliver", {
        from_muse_id: fromMuseId,
        text,
      }),
    );
  }

  async declineInbox(messageId: string): Promise<PeerInboxMessage> {
    return toPeerInboxMessage(
      await this.request<ServerPeerInboxMessage>("POST", "/peers/inbox/decline", {
        message_id: messageId,
      }),
    );
  }

  async listRequests(): Promise<PeerRequest[]> {
    return (await this.request<ServerPeerRequest[]>("GET", "/peers/requests")).map(toPeerRequest);
  }

  async getStatus(): Promise<PeerStatus> {
    return toPeerStatus(await this.request<ServerPeerStatus>("GET", "/peers/status"));
  }
}
