// Chat domain: the Store is the frontend's single source of truth;
// components only render Store state and dispatch actions.
// No direct system access from here: gateway calls go through
// GatewayChatClient (see gateway_client.ts), which the Store
// owns. The Rust bridge is the next wiring step for a packaged
// client; the HTTP client here is what the Phase 1 web mainline
// uses against the Mock Gateway.

import type { SubscriptionRecord } from "./types";
import type { ChatGateway } from "./gateway_client";

export type TurnState =
  | "created"
  | "submitted"
  | "thinking"
  | "tool_call_pending"
  | "awaiting_approval"
  | "tool_running"
  | "streaming"
  | "completed"
  | "cancelled"
  | "failed";

export interface ChatMessage {
  turnId: string;
  role: "user" | "assistant";
  text: string;
}

export interface ChatState {
  messages: ChatMessage[];
  activeTurnId: string | null;
  activeTurnState: TurnState | null;
  /** Highest sequence already applied, per stream. Dedup key. */
  lastSequenceByStream: Record<string, number>;
  /** Streams cancelled by the user: their later records are dropped. */
  cancelledStreamIds: string[];
}

export const initialChatState: ChatState = {
  messages: [],
  activeTurnId: null,
  activeTurnState: null,
  lastSequenceByStream: {},
  cancelledStreamIds: [],
};

function payloadText(payload: unknown): string | null {
  if (typeof payload === "object" && payload !== null && "text" in payload) {
    const text = (payload as { text?: unknown }).text;
    return typeof text === "string" ? text : null;
  }
  return null;
}

function payloadTurnId(payload: unknown): string | null {
  if (typeof payload === "object" && payload !== null && "turnId" in payload) {
    const turnId = (payload as { turnId?: unknown }).turnId;
    return typeof turnId === "string" ? turnId : null;
  }
  return null;
}

/**
 * Apply one subscription record. Returns false when the record is
 * a duplicate (sequence <= last applied) or belongs to a cancelled
 * stream -- in both cases nothing is appended. Otherwise the
 * sequence cursor advances and the event is folded into state:
 * turn_start opens the turn, text_append appends to the
 * assistant message for that turn, message_done completes it.
 * This rule is part of the contract, not a rendering detail.
 */
export function applyStreamRecord(
  state: ChatState,
  record: SubscriptionRecord,
): { state: ChatState; applied: boolean } {
  if (state.cancelledStreamIds.includes(record.streamId)) {
    return { state, applied: false };
  }
  const last = state.lastSequenceByStream[record.streamId] ?? 0;
  if (record.sequence <= last) {
    return { state, applied: false };
  }
  let next: ChatState = {
    ...state,
    lastSequenceByStream: {
      ...state.lastSequenceByStream,
      [record.streamId]: record.sequence,
    },
  };
  if (record.eventType === "delta.turn_start") {
    const turnId = payloadTurnId(record.payload) ?? state.activeTurnId;
    next = { ...next, activeTurnId: turnId, activeTurnState: "streaming" };
  } else if (record.eventType === "delta.text_append") {
    const text = payloadText(record.payload);
    const turnId = payloadTurnId(record.payload) ?? state.activeTurnId;
    if (text !== null && turnId !== null) {
      const existing = next.messages.find(
        (m) => m.turnId === turnId && m.role === "assistant",
      );
      if (existing) {
        next = {
          ...next,
          messages: next.messages.map((m) =>
            m === existing ? { ...m, text: m.text + text } : m,
          ),
        };
      } else {
        next = {
          ...next,
          messages: [...next.messages, { turnId, role: "assistant", text }],
        };
      }
      next = { ...next, activeTurnId: turnId, activeTurnState: "streaming" };
    }
  } else if (record.eventType === "delta.message_done") {
    next = { ...next, activeTurnState: "completed" };
  }
  return { state: next, applied: true };
}

/** Mark a stream cancelled: its later records will be dropped. */
export function cancelTurn(state: ChatState, streamId: string): ChatState {
  if (state.cancelledStreamIds.includes(streamId)) {
    return state;
  }
  return {
    ...state,
    cancelledStreamIds: [...state.cancelledStreamIds, streamId],
    activeTurnState:
      state.activeTurnState === "streaming" ? "cancelled" : state.activeTurnState,
  };
}

/**
 * Run one turn through the gateway: start, apply, fold into the
 * Store. Returns the new state and the assembled assistant text
 * for the turn. Server-side dedup (applyStream) and Store-side
 * dedup (applyStreamRecord) both run; a record dropped by either
 * never reaches the messages list.
 */
export async function runTurn(
  client: ChatGateway,
  state: ChatState,
  input: { streamId: string; turnId: string; text: string },
): Promise<{ state: ChatState; assistantText: string }> {
  const userMessage: ChatMessage = {
    turnId: input.turnId,
    role: "user",
    text: input.text,
  };
  let next: ChatState = {
    ...state,
    messages: [...state.messages, userMessage],
    activeTurnId: input.turnId,
    activeTurnState: "submitted",
  };
  const records = await client.startStream(input.streamId, input.turnId, input.text);
  const applied = await client.applyStream(input.streamId, records);
  for (const record of applied.kept) {
    const result = applyStreamRecord(next, record);
    next = result.state;
  }
  const assistant = next.messages.find(
    (m) => m.turnId === input.turnId && m.role === "assistant",
  );
  return { state: next, assistantText: assistant?.text ?? "" };
}
