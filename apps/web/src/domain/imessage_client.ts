// iMessage client: browser/dev -> Mock Gateway iMessage
// threads surface (MCL-1, threads-only).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Reads only;
// no approval loop. The packaged Mac app reads real
// Messages.app data via AppleScript; other platforms
// answer 501.
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

// Transport-neutral surface used by the Messages view.
// The HTTP client below and the invoke client both
// implement it, so the view never knows which transport
// it runs on.
export interface IMessageGateway {
  syncImessage(): Promise<IMessageSyncResult>;
  threads(limit: number | null, offset: number | null): Promise<IMessageThread[]>;
  thread(threadId: string): Promise<IMessageThread>;
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
}
