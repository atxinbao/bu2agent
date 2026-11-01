// Chat records client for the TS Domain Store (Task 18,
// record subset): themes, unread-count, mark-seen,
// delete-turn, message get, history-window, search.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view.

export interface ChatTurnView {
  threadId: string;
  index: number;
  role: string;
  text: string;
  createdMs: number;
  model: string | null;
  finishReason: string | null;
  deleted: boolean;
}

export interface ChatThemes {
  theme: string;
  available: string[];
}

export interface ThreadUnread {
  threadId: string;
  unread: number;
}

export interface UnreadCount {
  unreadCount: number;
  threads: ThreadUnread[];
}

export interface SeenResult {
  threadId: string;
  marker: number;
  unread: number;
}

export interface HistoryWindow {
  threadId: string;
  totalVisible: number;
  entries: ChatTurnView[];
}

export interface SearchMatch {
  threadId: string;
  index: number;
  role: string;
  text: string;
}

export interface ChatRecordsGateway {
  getThemes(): Promise<ChatThemes>;
  setTheme(theme: string): Promise<ChatThemes>;
  getUnreadCount(): Promise<UnreadCount>;
  markSeen(threadId: string, index: number | null): Promise<SeenResult>;
  deleteTurn(threadId: string, index: number): Promise<ChatTurnView>;
  getMessage(threadId: string, index: number): Promise<ChatTurnView>;
  getHistoryWindow(threadId: string, offset: number, limit: number): Promise<HistoryWindow>;
  search(query: string): Promise<SearchMatch[]>;
}

interface ServerChatTurnView {
  thread_id: string;
  index: number;
  role: string;
  text: string;
  created_ms: number;
  model: string | null;
  finish_reason: string | null;
  deleted?: boolean;
}

interface ServerChatThemes {
  theme: string;
  available: string[];
}

interface ServerThreadUnread {
  thread_id: string;
  unread: number;
}

interface ServerUnreadCount {
  unread_count: number;
  threads: ServerThreadUnread[];
}

interface ServerSeenResult {
  thread_id: string;
  marker: number;
  unread: number;
}

interface ServerHistoryWindow {
  thread_id: string;
  total_visible: number;
  entries: ServerChatTurnView[];
}

interface ServerSearchMatch {
  thread_id: string;
  index: number;
  role: string;
  text: string;
}

function toChatTurnView(server: ServerChatTurnView): ChatTurnView {
  return {
    threadId: server.thread_id,
    index: server.index,
    role: server.role,
    text: server.text,
    createdMs: server.created_ms,
    model: server.model,
    finishReason: server.finish_reason,
    deleted: server.deleted ?? false,
  };
}

function toUnreadCount(server: ServerUnreadCount): UnreadCount {
  return {
    unreadCount: server.unread_count,
    threads: server.threads.map((t) => ({ threadId: t.thread_id, unread: t.unread })),
  };
}

function toSeenResult(server: ServerSeenResult): SeenResult {
  return {
    threadId: server.thread_id,
    marker: server.marker,
    unread: server.unread,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class ChatRecordsClient implements ChatRecordsGateway {
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

  async getThemes(): Promise<ChatThemes> {
    return this.request<ServerChatThemes>("GET", "/api/chat/themes");
  }

  async setTheme(theme: string): Promise<ChatThemes> {
    return this.request<ServerChatThemes>("POST", "/api/chat/themes/set", { theme });
  }

  async getUnreadCount(): Promise<UnreadCount> {
    return toUnreadCount(await this.request<ServerUnreadCount>("GET", "/chat/unread-count"));
  }

  async markSeen(threadId: string, index: number | null): Promise<SeenResult> {
    return toSeenResult(
      await this.request<ServerSeenResult>("POST", `/chat/${threadId}/seen`, { index }),
    );
  }

  async deleteTurn(threadId: string, index: number): Promise<ChatTurnView> {
    return toChatTurnView(
      await this.request<ServerChatTurnView>("POST", "/api/chat/delete-turn", {
        thread_id: threadId,
        index,
      }),
    );
  }

  async getMessage(threadId: string, index: number): Promise<ChatTurnView> {
    return toChatTurnView(
      await this.request<ServerChatTurnView>(
        "GET",
        `/chat/message?thread_id=${threadId}&index=${index}`,
      ),
    );
  }

  async getHistoryWindow(
    threadId: string,
    offset: number,
    limit: number,
  ): Promise<HistoryWindow> {
    const server = await this.request<ServerHistoryWindow>(
      "GET",
      `/chat/history-window?thread_id=${threadId}&offset=${offset}&limit=${limit}`,
    );
    return {
      threadId: server.thread_id,
      totalVisible: server.total_visible,
      entries: server.entries.map(toChatTurnView),
    };
  }

  async search(query: string): Promise<SearchMatch[]> {
    const server = await this.request<ServerSearchMatch[]>(
      "GET",
      `/chat/search?q=${encodeURIComponent(query)}`,
    );
    return server.map((m) => ({
      threadId: m.thread_id,
      index: m.index,
      role: m.role,
      text: m.text,
    }));
  }
}
