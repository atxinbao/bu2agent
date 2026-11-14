// Outbox client for the TS Domain Store.
//
// Outbox rows only: list, get, deliver, requeue.
// FU-3B (Q-R0-01): the `/outbox` family had no production
// client; this wires it for the WorkbenchView outbox section.
// Rows live in SQLite only (P0-D); without a configured
// SQLite database the routes answer 5xx fail-closed.
//
// Delivery semantics (D1-3): `deliver` does NOT revive a
// `failed` row -- a `sent`/`failed` row is returned unchanged
// (idempotent). Only `requeue` revives a `failed` row, back
// to `pending`, after which the sweeper tick or a manual
// deliver sends it. Neither operation creates an Effect
// (#11/#12 are Outbox operations, not Effects); the list is
// read-only otherwise.
//
// `payloadSummaryJson` is a JSON string, not an object; the
// view displays it as text (truncated in lists, full in
// detail). Both POSTs take no body: the handlers take only
// the Path id.

export interface OutboxRow {
  outboxId: string;
  executionId: string;
  payloadSummaryJson: string;
  destinationKind: string;
  status: string;
  attempts: number;
  dedupKey: string;
  createdMs: number;
  updatedAtMs: number;
}

interface ServerOutboxRow {
  outbox_id: string;
  execution_id: string;
  payload_summary_json: string;
  destination_kind: string;
  status: string;
  attempts: number;
  dedup_key: string;
  created_ms: number;
  updated_at_ms: number;
}

function toRow(server: ServerOutboxRow): OutboxRow {
  return {
    outboxId: server.outbox_id,
    executionId: server.execution_id,
    payloadSummaryJson: server.payload_summary_json,
    destinationKind: server.destination_kind,
    status: server.status,
    attempts: server.attempts,
    dedupKey: server.dedup_key,
    createdMs: server.created_ms,
    updatedAtMs: server.updated_at_ms,
  };
}

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface OutboxGateway {
  list(): Promise<OutboxRow[]>;
  get(outboxId: string): Promise<OutboxRow>;
  deliver(outboxId: string): Promise<OutboxRow>;
  requeue(outboxId: string): Promise<OutboxRow>;
}

export class OutboxClient implements OutboxGateway {
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

  async list(): Promise<OutboxRow[]> {
    const rows = await this.request<ServerOutboxRow[]>("/outbox/list");
    return rows.map(toRow);
  }

  async get(outboxId: string): Promise<OutboxRow> {
    const row = await this.request<ServerOutboxRow>(
      `/outbox/get?outbox_id=${encodeURIComponent(outboxId)}`,
    );
    return toRow(row);
  }

  async deliver(outboxId: string): Promise<OutboxRow> {
    const row = await this.request<ServerOutboxRow>(
      `/outbox/${encodeURIComponent(outboxId)}/deliver`,
      { method: "POST" },
    );
    return toRow(row);
  }

  async requeue(outboxId: string): Promise<OutboxRow> {
    const row = await this.request<ServerOutboxRow>(
      `/outbox/${encodeURIComponent(outboxId)}/requeue`,
      { method: "POST" },
    );
    return toRow(row);
  }
}
