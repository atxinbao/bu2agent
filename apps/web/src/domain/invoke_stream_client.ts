import { invoke } from "@tauri-apps/api/core";
import type {
  SxsSelection,
  StreamCancelResult,
  StreamEvents,
  StreamGateway,
  StreamOpen,
  StreamPoke,
  StreamSubscription,
} from "./stream_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

interface ServerStreamEvent {
  id: string;
  type: string;
  turn_index: number | null;
  text: string | null;
  created_ms: number;
}

function toStreamEvent(server: ServerStreamEvent) {
  return {
    id: server.id,
    type: server.type,
    turnIndex: server.turn_index,
    text: server.text,
    createdMs: server.created_ms,
  };
}

export class InvokeStreamClient implements StreamGateway {
  async subscribe(threadId: string): Promise<StreamSubscription> {
    const server = await invoke<{
      subscription_id: string;
      thread_id: string;
      cursor: string;
    }>("chat_subscribe", { thread_id: threadId });
    return {
      subscriptionId: server.subscription_id,
      threadId: server.thread_id,
      cursor: server.cursor,
    };
  }

  async streamEvents(
    subscriptionId: string,
    cursor: string | null,
    waitMs: number | null,
  ): Promise<StreamEvents> {
    const server = await invoke<{
      subscription_id: string;
      cursor: string;
      events: ServerStreamEvent[];
      cancelled: boolean;
    }>("chat_stream_events", {
      subscription_id: subscriptionId,
      cursor,
      wait_ms: waitMs,
    });
    return {
      subscriptionId: server.subscription_id,
      cursor: server.cursor,
      events: server.events.map(toStreamEvent),
      cancelled: server.cancelled,
    };
  }

  async cancel(threadId: string): Promise<StreamCancelResult> {
    const server = await invoke<{ thread_id: string; cancelled_subscriptions: number }>(
      "chat_cancel",
      { thread_id: threadId },
    );
    return {
      threadId: server.thread_id,
      cancelledSubscriptions: server.cancelled_subscriptions,
    };
  }

  async openStream(threadId: string): Promise<StreamOpen> {
    const server = await invoke<{ stream_id: string; thread_id: string }>("chat_stream_open", {
      thread_id: threadId,
    });
    return { streamId: server.stream_id, threadId: server.thread_id };
  }

  async poke(threadId: string): Promise<StreamPoke> {
    const server = await invoke<{ thread_id: string; event_id: string; created_ms: number }>(
      "chat_poke",
      { thread_id: threadId },
    );
    return {
      threadId: server.thread_id,
      eventId: server.event_id,
      createdMs: server.created_ms,
    };
  }

  async sxsSelect(threadId: string, index: number): Promise<SxsSelection> {
    const server = await invoke<{ thread_id: string; index: number }>("chat_sxs_select", {
      thread_id: threadId,
      index,
    });
    return { threadId: server.thread_id, index: server.index };
  }
}
