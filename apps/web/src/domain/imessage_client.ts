// iMessage client: browser/dev -> Mock Gateway iMessage
// threads + send surface (MCL-1 threads-only read,
// MCL-4 send).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Reads are
// sync / threads list/get (no approval loop); the send
// goes through the digest-bound approval flow
// (approval_request -> decide -> send, the 6b-1 mail
// shape). The packaged Mac app reads real Messages.app
// data via AppleScript; other platforms answer 501.
//
// No message content anywhere in this file: MCL-1 is
// threads-only (message history is not readable — the
// MCL-0 probe proved the AppleScript dictionary has no
// message element). `lastMessageMs` / `unreadCount`
// are always null under AppleScript sync (never
// fabricated).

// One synced iMessage thread (the gateway
// `IMessageThreadRow` shape, camel-cased). The list
// includes tombstoned rows with `deleted: true`;
// `thread` may return a tombstoned row with
// `deleted: true`.
export interface IMessageThread {
  threadId: string;
  sourceId: string;
  nativeId: string;
  service: string;
  isGroup: boolean;
  participantCount: number;
  participantHash: string;
  lastMessageMs: number | null;
  unreadCount: number | null;
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
  originDeviceId: string;
}

// MCL-1: one iMessage sync pass (manual trigger only).
// Counts + the imessage-source cursor.
export interface IMessageSyncResult {
  syncedCount: number;
  skippedCount: number;
  totalCount: number;
  cursor: string;
}

// MCL-4: one accepted send (the gateway
// `SentIMessageRow` shape, camel-cased). The body is
// never in this shape — only its hash. "Sent" means
// Messages.app accepted the message, never that the
// other side received it (Q-MCL-21).
export interface SentIMessage {
  sentId: string;
  sourceId: string;
  threadId: string;
  nativeId: string | null;
  idempotencyKey: string;
  bodyHash: string;
  sentMs: number;
  effectId: string;
  policyVersion: string;
  createdMs: number;
  originDeviceId: string;
  replayed?: boolean;
}

// MCL-4: a send request. The idempotency key is
// client-supplied and required (Q-MCL-23, the 6b-1
// shape); the approval id is the decided digest-bound
// approval for this exact message in this exact thread.
export interface SendIMessageInput {
  threadId: string;
  body: string;
  idempotencyKey: string;
  toolCallId: string | null;
}

// Transport-neutral surface used by the Messages view.
// The HTTP client below and the invoke client both
// implement it, so the view never knows which transport
// it runs on.
export interface IMessageGateway {
  syncImessage(): Promise<IMessageSyncResult>;
  threads(limit: number | null, offset: number | null): Promise<IMessageThread[]>;
  thread(threadId: string): Promise<IMessageThread>;
  sendImessage(input: SendIMessageInput): Promise<SentIMessage>;
  sent(): Promise<SentIMessage[]>;
  sentImessage(sentId: string): Promise<SentIMessage>;
}

interface ServerIMessageSyncResult {
  synced_count: number;
  skipped_count: number;
  total_count: number;
  cursor: string;
}

interface ServerIMessageThread {
  thread_id: string;
  source_id: string;
  native_id: string;
  service: string;
  is_group: boolean;
  participant_count: number;
  participant_hash: string;
  last_message_ms: number | null;
  unread_count: number | null;
  created_ms: number;
  updated_ms: number;
  deleted: boolean;
  origin_device_id: string;
}

interface ServerSentIMessage {
  sent_id: string;
  source_id: string;
  thread_id: string;
  native_id: string | null;
  idempotency_key: string;
  body_hash: string;
  sent_ms: number;
  effect_id: string;
  policy_version: string;
  created_ms: number;
  origin_device_id: string;
  replayed?: boolean;
}

function toIMessageThread(server: ServerIMessageThread): IMessageThread {
  return {
    threadId: server.thread_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    service: server.service,
    isGroup: server.is_group,
    participantCount: server.participant_count,
    participantHash: server.participant_hash,
    lastMessageMs: server.last_message_ms,
    unreadCount: server.unread_count,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted,
    originDeviceId: server.origin_device_id,
  };
}

function toSentIMessage(server: ServerSentIMessage): SentIMessage {
  return {
    sentId: server.sent_id,
    sourceId: server.source_id,
    threadId: server.thread_id,
    nativeId: server.native_id,
    idempotencyKey: server.idempotency_key,
    bodyHash: server.body_hash,
    sentMs: server.sent_ms,
    effectId: server.effect_id,
    policyVersion: server.policy_version,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
    replayed: server.replayed,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("imessage gateway URL must be a localhost Mock");
  }
}

export class IMessageClient implements IMessageGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`imessage gateway ${method} ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`imessage gateway POST ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async syncImessage(): Promise<IMessageSyncResult> {
    const result = await this.requestJson<ServerIMessageSyncResult>(
      "POST",
      "/api/imessage/sync",
    );
    return {
      syncedCount: result.synced_count,
      skippedCount: result.skipped_count,
      totalCount: result.total_count,
      cursor: result.cursor,
    };
  }

  async threads(limit: number | null, offset: number | null): Promise<IMessageThread[]> {
    const query = new URLSearchParams();
    if (limit !== null) query.set("limit", String(limit));
    if (offset !== null) query.set("offset", String(offset));
    const suffix = query.toString() === "" ? "" : `?${query.toString()}`;
    const threads = await this.requestJson<ServerIMessageThread[]>("GET", `/api/imessage/threads${suffix}`);
    return threads.map(toIMessageThread);
  }

  async thread(threadId: string): Promise<IMessageThread> {
    const thread = await this.requestJson<ServerIMessageThread>(
      "GET",
      `/api/imessage/threads/${threadId}`,
    );
    return toIMessageThread(thread);
  }

  // MCL-4: send one plain-text message to one
  // already-synced thread, through the digest-bound
  // approval flow (approval_request -> decide -> send,
  // the 6b-1 mail shape). The gateway's extra `replayed`
  // flag rides on the row; the view shows it.
  async sendImessage(input: SendIMessageInput): Promise<SentIMessage> {
    const sent = await this.postJson<ServerSentIMessage>("/api/imessage/send", {
      thread_id: input.threadId,
      body: input.body,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId,
    });
    return toSentIMessage(sent);
  }

  async sent(): Promise<SentIMessage[]> {
    const rows = await this.requestJson<ServerSentIMessage[]>("GET", "/api/imessage/sent");
    return rows.map(toSentIMessage);
  }

  async sentImessage(sentId: string): Promise<SentIMessage> {
    const row = await this.requestJson<ServerSentIMessage>(
      "GET",
      `/api/imessage/sent/${sentId}`,
    );
    return toSentIMessage(row);
  }
}
