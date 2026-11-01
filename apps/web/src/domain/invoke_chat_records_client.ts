import { invoke } from "@tauri-apps/api/core";
import type {
  ChatRecordsGateway,
  ChatThemes,
  ChatTurnView,
  HistoryWindow,
  SearchMatch,
  SeenResult,
  UnreadCount,
} from "./chat_records_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

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

export class InvokeChatRecordsClient implements ChatRecordsGateway {
  async getThemes(): Promise<ChatThemes> {
    return invoke<ServerChatThemes>("chat_themes_get");
  }

  async setTheme(theme: string): Promise<ChatThemes> {
    return invoke<ServerChatThemes>("chat_themes_set", { theme });
  }

  async getUnreadCount(): Promise<UnreadCount> {
    return toUnreadCount(await invoke<ServerUnreadCount>("chat_unread_count"));
  }

  async markSeen(threadId: string, index: number | null): Promise<SeenResult> {
    return toSeenResult(
      await invoke<ServerSeenResult>("chat_mark_seen", { thread_id: threadId, index }),
    );
  }

  async deleteTurn(threadId: string, index: number): Promise<ChatTurnView> {
    return toChatTurnView(
      await invoke<ServerChatTurnView>("chat_delete_turn", {
        thread_id: threadId,
        index,
      }),
    );
  }

  async getMessage(threadId: string, index: number): Promise<ChatTurnView> {
    return toChatTurnView(
      await invoke<ServerChatTurnView>("chat_message_get", {
        thread_id: threadId,
        index,
      }),
    );
  }

  async getHistoryWindow(
    threadId: string,
    offset: number,
    limit: number,
  ): Promise<HistoryWindow> {
    const server = await invoke<ServerHistoryWindow>("chat_history_window", {
      thread_id: threadId,
      offset,
      limit,
    });
    return {
      threadId: server.thread_id,
      totalVisible: server.total_visible,
      entries: server.entries.map(toChatTurnView),
    };
  }

  async search(query: string): Promise<SearchMatch[]> {
    const server = await invoke<ServerSearchMatch[]>("chat_search", { query });
    return server.map((m) => ({
      threadId: m.thread_id,
      index: m.index,
      role: m.role,
      text: m.text,
    }));
  }
}
