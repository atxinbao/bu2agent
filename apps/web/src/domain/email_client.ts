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

  mailbox(): Promise<MailboxInfo> {
    return this.requestJson<MailboxInfo>("GET", "/api/email/mailbox");
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
}
