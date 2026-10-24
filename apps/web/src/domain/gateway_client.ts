// Gateway chat client for the TS Domain Store (Phase 1 chat mainline).
//
// This is the TS half of the same mainline the Rust `ChatClient`
// implements: history, stream start, stream apply, stream cancel,
// approval check -- all against the Mock Gateway Axum transport.
//
// Mapping, stated plainly because it is the easiest place to break:
// the server speaks snake_case JSON; this client maps it to the
// camelCase contract types in ./types.ts. On the way out, records
// are mapped back to snake_case for the server.
//
// Notes:
// - No UI code here, no direct system access: the Store calls this
// client, components only render Store state (see chat.ts).
// - Error bodies are never surfaced: they may contain user text.
// Only the HTTP status is reported.
// - The server payload field is itself a JSON string (for example
// '{"turnId":"turn-1","text":"hi"}'); it is parsed once here so
// the Store works with objects, not double-encoded strings.

import type { ApprovalStatus, SubscriptionRecord } from "./types";

export interface HistoryMessage {
  turnId: string;
  role: "user" | "assistant";
  text: string;
}

interface ServerStreamRecord {
  stream_id: string;
  sequence: number;
  event_type: string;
  payload: string;
  resume_cursor: string | null;
}

interface ServerHistoryMessage {
  turn_id: string;
  role: "user" | "assistant";
  text: string;
}

export interface ApplyResult {
  kept: SubscriptionRecord[];
  lastAppliedSequence: number;
}

/**
 * The chat transport interface `runTurn` depends on. Both the
 * HTTP client (browser preview) and the Tauri invoke client
 * (Rust client behind the shell) implement it, so the Store
 * never learns which transport is underneath.
 */
export interface ChatGateway {
  health(): Promise<boolean>;
  history(): Promise<HistoryMessage[]>;
  startStream(
    streamId: string,
    turnId: string,
    text: string,
  ): Promise<SubscriptionRecord[]>;
  applyStream(
    streamId: string,
    records: SubscriptionRecord[],
  ): Promise<ApplyResult>;
  cancelStream(streamId: string): Promise<boolean>;
  checkApproval(
    status: ApprovalStatus,
    sensitive: boolean,
  ): Promise<boolean>;
}

function parsePayload(payload: string): unknown {
  try {
    return JSON.parse(payload) as unknown;
  } catch {
    return payload;
  }
}

function toRecord(server: ServerStreamRecord): SubscriptionRecord {
  return {
    streamId: server.stream_id,
    sequence: server.sequence,
    eventType: server.event_type,
    payload: parsePayload(server.payload),
    resumeCursor: server.resume_cursor,
  };
}

function toServerRecord(record: SubscriptionRecord): ServerStreamRecord {
  return {
    stream_id: record.streamId,
    sequence: record.sequence,
    event_type: record.eventType,
    payload:
      typeof record.payload === "string"
        ? record.payload
        : JSON.stringify(record.payload),
    resume_cursor: record.resumeCursor,
  };
}

export class GatewayChatClient implements ChatGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, init);
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async health(): Promise<boolean> {
    try {
      const response = await fetch(`${this.baseUrl}/health`);
      return response.ok;
    } catch {
      return false;
    }
  }

  async history(): Promise<HistoryMessage[]> {
    const messages = await this.request<ServerHistoryMessage[]>("/chat/history");
    return messages.map((m) => ({
      turnId: m.turn_id,
      role: m.role,
      text: m.text,
    }));
  }

  async startStream(
    streamId: string,
    turnId: string,
    text: string,
  ): Promise<SubscriptionRecord[]> {
    const records = await this.request<ServerStreamRecord[]>(
      "/chat/stream/start",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stream_id: streamId, turn_id: turnId, text }),
      },
    );
    return records.map(toRecord);
  }

  async applyStream(
    streamId: string,
    records: SubscriptionRecord[],
  ): Promise<ApplyResult> {
    const result = await this.request<{
      kept: ServerStreamRecord[];
      last_applied_sequence: number;
    }>("/chat/stream/apply", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        stream_id: streamId,
        records: records.map(toServerRecord),
      }),
    });
    return {
      kept: result.kept.map(toRecord),
      lastAppliedSequence: result.last_applied_sequence,
    };
  }

  async cancelStream(streamId: string): Promise<boolean> {
    const response = await fetch(`${this.baseUrl}/chat/stream/cancel`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ stream_id: streamId }),
    });
    return response.ok;
  }

  async checkApproval(
    status: ApprovalStatus,
    sensitive: boolean,
  ): Promise<boolean> {
    const result = await this.request<{ may_execute: boolean }>(
      "/approval/check",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status, sensitive }),
      },
    );
    return result.may_execute;
  }
}
