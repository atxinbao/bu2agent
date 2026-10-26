import { invoke } from "@tauri-apps/api/core";
import type {
  SessionGateway,
  SessionRecord,
  SessionSummary,
  SessionTurn,
} from "./session_client";

// The Rust session record serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
interface ServerSessionTurn {
  role: string;
  text: string;
  created_ms: number;
  model: string | null;
  finish_reason: string | null;
}

interface ServerSessionRecord {
  session_id: string;
  title: string;
  created_ms: number;
  updated_ms: number;
  deleted: boolean;
  turns: ServerSessionTurn[];
}

interface ServerSessionSummary {
  session_id: string;
  title: string;
  created_ms: number;
  updated_ms: number;
  turn_count: number;
}

function toTurn(server: ServerSessionTurn): SessionTurn {
  return {
    role: server.role,
    text: server.text,
    createdMs: server.created_ms,
    model: server.model,
    finishReason: server.finish_reason,
  };
}

function toRecord(server: ServerSessionRecord): SessionRecord {
  return {
    sessionId: server.session_id,
    title: server.title,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted,
    turns: server.turns.map(toTurn),
  };
}

function toSummary(server: ServerSessionSummary): SessionSummary {
  return {
    sessionId: server.session_id,
    title: server.title,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    turnCount: server.turn_count,
  };
}

// Session line over Tauri invoke, backed by the Rust SessionClient.
// Invoke argument keys are snake_case to match the Rust
// command parameters exactly.
export class InvokeSessionClient implements SessionGateway {
  async create(title?: string | null): Promise<SessionRecord> {
    const record = await invoke<ServerSessionRecord>("session_create", {
      title: title ?? null,
    });
    return toRecord(record);
  }

  async list(): Promise<SessionSummary[]> {
    const records = await invoke<ServerSessionSummary[]>("session_list");
    return records.map(toSummary);
  }

  async get(sessionId: string): Promise<SessionRecord> {
    const record = await invoke<ServerSessionRecord>("session_get", {
      session_id: sessionId,
    });
    return toRecord(record);
  }

  async send(sessionId: string, text: string): Promise<SessionRecord> {
    const record = await invoke<ServerSessionRecord>("session_send", {
      session_id: sessionId,
      text,
    });
    return toRecord(record);
  }

  async delete(sessionId: string): Promise<void> {
    await invoke("session_delete", { session_id: sessionId });
  }
}
