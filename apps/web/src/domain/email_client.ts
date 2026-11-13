// Email client: browser/dev -> Mock Gateway email read
// surface (Task 22, batch 1, option A).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Reads only;
// no approval loop. The packaged Mac app reads real
// Mail.app data via AppleScript; other platforms answer
// 501.

export interface MailboxInfo {
  configured: boolean;
  address: string | null;
  name: string | null;
  source: string;
}

export interface MailboxSetup {
  configured: boolean;
  source: string;
  steps: string[];
}

export interface ThreadView {
  threadId: string;
  subject: string;
  messageIds: string[];
  messageCount: number;
  snippet: string;
}

export interface AttachmentMeta {
  index: number;
  name: string;
  contentType: string;
  size: number;
}

export interface MessageView {
  messageId: string;
  from: string;
  to: string[];
  date: string;
  subject: string;
  bodyText: string;
  attachments: AttachmentMeta[];
}

// D1-6a: one mail sync pass (manual trigger only).
// Counts + the mail-source cursor.
export interface MailSyncResult {
  syncedCount: number;
  skippedCount: number;
  totalCount: number;
  cursor: string;
}

// D1-6b-1: one accepted send (the `sent_messages` fact
// row). The body is never in this shape — only its
// hash. "Sent" means Mail.app accepted the message,
// never that it was delivered (Q-D1-6b-06).
export interface SentMessage {
  sentId: string;
  sourceId: string;
  nativeId: string | null;
  idempotencyKey: string;
  toJson: string;
  subject: string;
  bodyHash: string;
  sentMs: number;
  effectId: string;
  policyVersion: string;
  createdMs: number;
  originDeviceId: string;
}

// D1-6b-1: a send request. The idempotency key is
// client-supplied and required (Q-D1-6b-05); the
// toolCallId is the decided digest-bound approval for
// this exact message (Q-D1-6b-22).
export interface SendMailInput {
  to: string[];
  subject: string;
  body: string;
  idempotencyKey: string;
  toolCallId?: string;
}

// D1-6b-3: one synced mail row (the gateway
// `MailMessageRow` shape, camel-cased). The list
// includes tombstoned rows with `deleted: true`;
// `synced` may return a tombstoned row with
// `deleted: true`.
export interface SyncedMessage {
  messageId: string;
  sourceId: string;
  nativeId: string;
  threadId: string;
  subject: string;
  sender: string;
  dateMs: number;
  bodyHash: string;
  createdMs: number;
  originDeviceId: string;
  deleted: boolean;
  updatedMs: number;
}

// D1-6b-3: one delete outcome (the gateway
// `MailDeleteResponse` shape). `messageId` is null when
// the deleted mail was never synced.
export interface MailDeleteResult {
  messageId: string | null;
  sourceId: string;
  nativeId: string;
  deleted: boolean;
  effectId: string | null;
  replayed: boolean;
}

// Transport-neutral surface used by the Mail view. The
// HTTP client below and the invoke client both implement
// it, so the view never knows which transport it runs on.
export interface EmailGateway {
  mailbox(): Promise<MailboxInfo>;
  mailboxSetup(): Promise<MailboxSetup>;
  threads(limit: number | null, offset: number | null): Promise<ThreadView[]>;
  thread(threadId: string): Promise<ThreadView>;
  message(messageId: string): Promise<MessageView>;
  attachment(messageId: string, index: number): Promise<number[]>;
  syncMail(): Promise<MailSyncResult>;
  sendMail(input: SendMailInput): Promise<SentMessage>;
  listSent(): Promise<SentMessage[]>;
  sent(sentId: string): Promise<SentMessage>;
  deleteMessage(messageId: string, toolCallId: string): Promise<MailDeleteResult>;
  listSynced(): Promise<SyncedMessage[]>;
  synced(messageId: string): Promise<SyncedMessage>;
}

interface ServerMailSyncResult {
  synced_count: number;
  skipped_count: number;
  total_count: number;
  cursor: string;
}

interface ServerThreadView {
  thread_id: string;
  subject: string;
  message_ids: string[];
  message_count: number;
  snippet: string;
}

interface ServerAttachmentMeta {
  index: number;
  name: string;
  content_type: string;
  size: number;
}

interface ServerMessageView {
  message_id: string;
  from: string;
  to: string[];
  date: string;
  subject: string;
  body_text: string;
  attachments: ServerAttachmentMeta[];
}

function toThreadView(server: ServerThreadView): ThreadView {
  return {
    threadId: server.thread_id,
    subject: server.subject,
    messageIds: server.message_ids,
    messageCount: server.message_count,
    snippet: server.snippet,
  };
}

function toMessageView(server: ServerMessageView): MessageView {
  return {
    messageId: server.message_id,
    from: server.from,
    to: server.to,
    date: server.date,
    subject: server.subject,
    bodyText: server.body_text,
    attachments: server.attachments.map((a) => ({
      index: a.index,
      name: a.name,
      contentType: a.content_type,
      size: a.size,
    })),
  };
}

interface ServerSentMessage {
  sent_id: string;
  source_id: string;
  native_id: string | null;
  idempotency_key: string;
  to_json: string;
  subject: string;
  body_hash: string;
  sent_ms: number;
  effect_id: string;
  policy_version: string;
  created_ms: number;
  origin_device_id: string;
}

function toSentMessage(server: ServerSentMessage): SentMessage {
  return {
    sentId: server.sent_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    idempotencyKey: server.idempotency_key,
    toJson: server.to_json,
    subject: server.subject,
    bodyHash: server.body_hash,
    sentMs: server.sent_ms,
    effectId: server.effect_id,
    policyVersion: server.policy_version,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
  };
}

interface ServerSyncedMessage {
  message_id: string;
  source_id: string;
  native_id: string;
  thread_id: string;
  subject: string;
  sender: string;
  date_ms: number;
  body_hash: string;
  created_ms: number;
  origin_device_id: string;
  deleted: boolean;
  updated_ms: number;
}

function toSyncedMessage(server: ServerSyncedMessage): SyncedMessage {
  return {
    messageId: server.message_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    threadId: server.thread_id,
    subject: server.subject,
    sender: server.sender,
    dateMs: server.date_ms,
    bodyHash: server.body_hash,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
    deleted: server.deleted,
    updatedMs: server.updated_ms,
  };
}

interface ServerMailDeleteResponse {
  message_id: string | null;
  source_id: string;
  native_id: string;
  deleted: boolean;
  effect_id: string | null;
  replayed: boolean;
}

function toMailDeleteResult(server: ServerMailDeleteResponse): MailDeleteResult {
  return {
    messageId: server.message_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    deleted: server.deleted,
    effectId: server.effect_id,
    replayed: server.replayed,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("email gateway URL must be a localhost Mock");
  }
}

export class EmailClient implements EmailGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`email gateway ${method} ${path} failed: ${response.status} ${text}`);
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
      throw new Error(`email gateway POST ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  mailbox(): Promise<MailboxInfo> {
    return this.requestJson<MailboxInfo>("GET", "/api/email/mailbox");
  }

  async syncMail(): Promise<MailSyncResult> {
    const result = await this.requestJson<ServerMailSyncResult>(
      "POST",
      "/api/email/sync",
    );
    return {
      syncedCount: result.synced_count,
      skippedCount: result.skipped_count,
      totalCount: result.total_count,
      cursor: result.cursor,
    };
  }

  mailboxSetup(): Promise<MailboxSetup> {
    return this.requestJson<MailboxSetup>("GET", "/api/email/mailbox/setup");
  }

  async threads(limit: number | null, offset: number | null): Promise<ThreadView[]> {
    const query = new URLSearchParams();
    if (limit !== null) query.set("limit", String(limit));
    if (offset !== null) query.set("offset", String(offset));
    const suffix = query.toString() === "" ? "" : `?${query.toString()}`;
    const threads = await this.requestJson<ServerThreadView[]>("GET", `/api/email/threads${suffix}`);
    return threads.map(toThreadView);
  }

  async thread(threadId: string): Promise<ThreadView> {
    const thread = await this.requestJson<ServerThreadView>(
      "GET",
      `/api/email/threads/${threadId}`,
    );
    return toThreadView(thread);
  }

  async message(messageId: string): Promise<MessageView> {
    const message = await this.requestJson<ServerMessageView>(
      "GET",
      `/api/email/messages/${messageId}`,
    );
    return toMessageView(message);
  }

  async attachment(messageId: string, index: number): Promise<number[]> {
    const response = await fetch(
      `${this.baseUrl}/api/email/messages/${messageId}/attachments/${index}`,
    );
    if (!response.ok) {
      const text = await response.text();
      throw new Error(
        `email gateway GET attachment failed: ${response.status} ${text}`,
      );
    }
    const buffer = await response.arrayBuffer();
    return Array.from(new Uint8Array(buffer));
  }

  async sendMail(input: SendMailInput): Promise<SentMessage> {
    const sent = await this.postJson<ServerSentMessage>("/api/email/send", {
      to: input.to,
      subject: input.subject,
      body: input.body,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId ?? null,
    });
    return toSentMessage(sent);
  }

  async listSent(): Promise<SentMessage[]> {
    const rows = await this.requestJson<ServerSentMessage[]>("GET", "/api/email/sent");
    return rows.map(toSentMessage);
  }

  async sent(sentId: string): Promise<SentMessage> {
    const row = await this.requestJson<ServerSentMessage>("GET", `/api/email/sent/${sentId}`);
    return toSentMessage(row);
  }

  async deleteMessage(messageId: string, toolCallId: string): Promise<MailDeleteResult> {
    // D1-6b-3: Trash only, never purge (Q-D1-6b-12).
    // The tool_call_id travels as a query parameter —
    // the DELETE carries no body (the desktop
    // delete_json sends none).
    const result = await this.requestJson<ServerMailDeleteResponse>(
      "DELETE",
      `/api/email/messages/${messageId}?tool_call_id=${toolCallId}`,
    );
    return toMailDeleteResult(result);
  }

  async listSynced(): Promise<SyncedMessage[]> {
    const rows = await this.requestJson<ServerSyncedMessage[]>("GET", "/api/email/synced");
    return rows.map(toSyncedMessage);
  }

  async synced(messageId: string): Promise<SyncedMessage> {
    const row = await this.requestJson<ServerSyncedMessage>(
      "GET",
      `/api/email/synced/${messageId}`,
    );
    return toSyncedMessage(row);
  }
}
