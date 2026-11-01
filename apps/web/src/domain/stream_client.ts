// Stream client for the TS Domain Store (Task 20, stream
// form subset): subscribe, stream-events, cancel, stream
// open, poke, sxs.select.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Events are a
// record projection, not a live token stream.

export interface StreamSubscription {
  subscriptionId: string;
  threadId: string;
  cursor: string;
}

export interface StreamEvent {
  id: string;
  type: string;
  turnIndex: number | null;
  text: string | null;
  createdMs: number;
}

export interface StreamEvents {
  subscriptionId: string;
  cursor: string;
  events: StreamEvent[];
  cancelled: boolean;
}

export interface StreamCancelResult {
  threadId: string;
  cancelledSubscriptions: number;
}

export interface StreamOpen {
  streamId: string;
  threadId: string;
}

export interface StreamPoke {
  threadId: string;
  eventId: string;
  createdMs: number;
}

export interface SxsSelection {
  threadId: string;
  index: number;
}

export interface StreamGateway {
  subscribe(threadId: string): Promise<StreamSubscription>;
  streamEvents(
    subscriptionId: string,
    cursor: string | null,
    waitMs: number | null,
  ): Promise<StreamEvents>;
  cancel(threadId: string): Promise<StreamCancelResult>;
  openStream(threadId: string): Promise<StreamOpen>;
  poke(threadId: string): Promise<StreamPoke>;
  sxsSelect(threadId: string, index: number): Promise<SxsSelection>;
}

interface ServerStreamEvent {
  id: string;
  type: string;
  turn_index: number | null;
  text: string | null;
  created_ms: number;
}

function toStreamEvent(server: ServerStreamEvent): StreamEvent {
  return {
    id: server.id,
    type: server.type,
    turnIndex: server.turn_index,
    text: server.text,
    createdMs: server.created_ms,
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

export class StreamClient implements StreamGateway {
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

  async subscribe(threadId: string): Promise<StreamSubscription> {
    const server = await this.request<{
      subscription_id: string;
      thread_id: string;
      cursor: string;
    }>("POST", "/chat/subscribe", { thread_id: threadId });
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
    const server = await this.request<{
      subscription_id: string;
      cursor: string;
      events: ServerStreamEvent[];
      cancelled: boolean;
    }>("POST", "/chat/stream-events", {
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
    const server = await this.request<{
      thread_id: string;
      cancelled_subscriptions: number;
    }>("POST", "/chat/cancel", { thread_id: threadId });
    return {
      threadId: server.thread_id,
      cancelledSubscriptions: server.cancelled_subscriptions,
    };
  }

  async openStream(threadId: string): Promise<StreamOpen> {
    const server = await this.request<{ stream_id: string; thread_id: string }>(
      "POST",
      "/chat/stream",
      { thread_id: threadId },
    );
    return { streamId: server.stream_id, threadId: server.thread_id };
  }

  async poke(threadId: string): Promise<StreamPoke> {
    const server = await this.request<{
      thread_id: string;
      event_id: string;
      created_ms: number;
    }>("POST", "/chat/poke", { thread_id: threadId });
    return {
      threadId: server.thread_id,
      eventId: server.event_id,
      createdMs: server.created_ms,
    };
  }

  async sxsSelect(threadId: string, index: number): Promise<SxsSelection> {
    const server = await this.request<{ thread_id: string; index: number }>(
      "POST",
      "/api/chat/sxs/select",
      { thread_id: threadId, index },
    );
    return { threadId: server.thread_id, index: server.index };
  }
}
