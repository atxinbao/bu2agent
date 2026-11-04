// Inbox client for the TS Domain Store.
//
// Inbox rows only: list, get, ack. Rows live in SQLite only
// (P0-D); without a configured SQLite database the routes
// answer 5xx fail-closed.

export interface InboxRow {
  inboxId: string;
  kind: string;
  executionId: string | null;
  effectId: string | null;
  approvalId: string | null;
  sourceEventId: string;
  createdMs: number;
  resolvedMs: number | null;
  resolvedReason: string | null;
}

interface ServerInboxRow {
  inbox_id: string;
  kind: string;
  execution_id: string | null;
  effect_id: string | null;
  approval_id: string | null;
  source_event_id: string;
  created_ms: number;
  resolved_ms: number | null;
  resolved_reason: string | null;
}

function toRow(server: ServerInboxRow): InboxRow {
  return {
    inboxId: server.inbox_id,
    kind: server.kind,
    executionId: server.execution_id,
    effectId: server.effect_id,
    approvalId: server.approval_id,
    sourceEventId: server.source_event_id,
    createdMs: server.created_ms,
    resolvedMs: server.resolved_ms,
    resolvedReason: server.resolved_reason,
  };
}

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface InboxGateway {
  list(kind?: string | null, openOnly?: boolean): Promise<InboxRow[]>;
  get(inboxId: string): Promise<InboxRow>;
  ack(inboxId: string): Promise<InboxRow>;
}

export class InboxClient implements InboxGateway {
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

  async list(
    kind?: string | null,
    openOnly?: boolean,
  ): Promise<InboxRow[]> {
    const params = new URLSearchParams();
    if (kind) {
      params.set("kind", kind);
    }
    params.set("open_only", openOnly ? "true" : "false");
    const rows = await this.request<ServerInboxRow[]>(
      `/inbox/list?${params.toString()}`,
    );
    return rows.map(toRow);
  }

  async get(inboxId: string): Promise<InboxRow> {
    const row = await this.request<ServerInboxRow>(
      `/inbox/get?inbox_id=${inboxId}`,
    );
    return toRow(row);
  }

  async ack(inboxId: string): Promise<InboxRow> {
    const row = await this.request<ServerInboxRow>("/inbox/ack", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inbox_id: inboxId }),
    });
    return toRow(row);
  }
}
