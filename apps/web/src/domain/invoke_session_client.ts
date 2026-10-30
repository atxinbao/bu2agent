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
  pinned: boolean;
  archived: boolean;
  turns: ServerSessionTurn[];
}

interface ServerSessionSummary {
  session_id: string;
  title: string;
  created_ms: number;
  updated_ms: number;
  turn_count: number;
  pinned: boolean;
  archived: boolean;
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
    pinned: server.pinned ?? false,
    archived: server.archived ?? false,
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
    pinned: server.pinned ?? false,
    archived: server.archived ?? false,
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

  async list(includeArchived?: boolean): Promise<SessionSummary[]> {
    const command = includeArchived ? "session_list_including_archived" : "session_list";
    const records = await invoke<ServerSessionSummary[]>(command);
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

  private async flag(command: string, sessionId: string): Promise<SessionSummary> {
    const summary = await invoke<ServerSessionSummary>(command, { session_id: sessionId });
    return toSummary(summary);
  }

  async pin(sessionId: string): Promise<SessionSummary> {
    return this.flag("session_pin", sessionId);
  }

  async unpin(sessionId: string): Promise<SessionSummary> {
    return this.flag("session_unpin", sessionId);
  }

  async archive(sessionId: string): Promise<SessionSummary> {
    return this.flag("session_archive", sessionId);
  }

  async unarchive(sessionId: string): Promise<SessionSummary> {
    return this.flag("session_unarchive", sessionId);
  }

  async rename(sessionId: string, title: string): Promise<SessionSummary> {
    const summary = await invoke<ServerSessionSummary>("session_rename", {
      session_id: sessionId,
      title,
    });
    return toSummary(summary);
  }
}
