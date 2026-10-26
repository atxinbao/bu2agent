// Session record client for the TS Domain Store (Round 3, 3a).
//
// Same shape as the gateway session endpoints, in the same order:
// create a session, list summaries (deleted hidden), get one
// session with its turns, send a message (the gateway appends
// the user turn and the model or echo reply), delete as a
// tombstone. The client never invents turns: what the gateway
// returns is what the view shows.
//
// Deletion is a tombstone in the gateway file, not erasure: the
// delete confirmation in the view must say the local record
// file keeps its earlier lines until it is removed by hand.

export interface SessionTurn {
  role: string;
  text: string;
  createdMs: number;
  model: string | null;
  finishReason: string | null;
}

export interface SessionRecord {
  sessionId: string;
  title: string;
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
  turns: SessionTurn[];
}

export interface SessionSummary {
  sessionId: string;
  title: string;
  createdMs: number;
  updatedMs: number;
  turnCount: number;
}

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

// Transport-neutral surface used by the workbench view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface SessionGateway {
  create(title?: string | null): Promise<SessionRecord>;
  list(): Promise<SessionSummary[]>;
  get(sessionId: string): Promise<SessionRecord>;
  send(sessionId: string, text: string): Promise<SessionRecord>;
  delete(sessionId: string): Promise<void>;
}

export class SessionClient implements SessionGateway {
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

  async create(title?: string | null): Promise<SessionRecord> {
    const record = await this.request<ServerSessionRecord>("/sessions/create", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title: title ?? null }),
    });
    return toRecord(record);
  }

  async list(): Promise<SessionSummary[]> {
    const records = await this.request<ServerSessionSummary[]>("/sessions/list");
    return records.map(toSummary);
  }

  async get(sessionId: string): Promise<SessionRecord> {
    const record = await this.request<ServerSessionRecord>(
      `/sessions/get?session_id=${sessionId}`,
    );
    return toRecord(record);
  }

  async send(sessionId: string, text: string): Promise<SessionRecord> {
    const record = await this.request<ServerSessionRecord>("/sessions/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId, text }),
    });
    return toRecord(record);
  }

  async delete(sessionId: string): Promise<void> {
    await this.request<unknown>("/sessions/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: sessionId }),
    });
  }
}
