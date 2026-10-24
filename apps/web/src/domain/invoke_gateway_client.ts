// Tauri invoke client for the chat line, backed by the Rust
// `ChatClient` through the shell's gateway commands (see
// apps/shell/src-tauri/src/gateway_commands.rs).
//
// Same `ChatGateway` interface as the HTTP client, same DTO
// shapes (snake_case, payload as a JSON string), so the Store
// and its mapping code cannot tell the transports apart.
//
// Invoke argument keys are snake_case on purpose: they match
// the Rust command parameters exactly, regardless of any
// camelCase conversion the bridge may or may not do.

import { invoke } from "@tauri-apps/api/core";
import type { ApprovalStatus, SubscriptionRecord } from "./types";
import type {
  ApplyResult,
  ChatGateway,
  HistoryMessage,
} from "./gateway_client";

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

export class InvokeGatewayClient implements ChatGateway {
  async health(): Promise<boolean> {
    try {
      return await invoke<boolean>("gateway_health");
    } catch {
      return false;
    }
  }

  async history(): Promise<HistoryMessage[]> {
    const messages = await invoke<ServerHistoryMessage[]>("gateway_history");
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
    const records = await invoke<ServerStreamRecord[]>("gateway_stream_start", {
      stream_id: streamId,
      turn_id: turnId,
      text,
    });
    return records.map(toRecord);
  }

  async applyStream(
    streamId: string,
    records: SubscriptionRecord[],
  ): Promise<ApplyResult> {
    const result = await invoke<{
      kept: ServerStreamRecord[];
      last_applied_sequence: number;
    }>("gateway_stream_apply", {
      stream_id: streamId,
      records: records.map(toServerRecord),
    });
    return {
      kept: result.kept.map(toRecord),
      lastAppliedSequence: result.last_applied_sequence,
    };
  }

  async cancelStream(streamId: string): Promise<boolean> {
    try {
      return await invoke<boolean>("gateway_stream_cancel", {
        stream_id: streamId,
      });
    } catch {
      return false;
    }
  }

  async checkApproval(
    status: ApprovalStatus,
    sensitive: boolean,
  ): Promise<boolean> {
    return invoke<boolean>("gateway_approval_check", { status, sensitive });
  }
}
