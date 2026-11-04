// Evidence client for the TS Domain Store.
//
// Evidence rows only: list, get. Evidence is SQLite-only
// (P0-R plan §2.4, following the event-table precedent): it
// is never mirrored to JSONL. Each row is one observation of
// a write's post-state -- a write-time observation, a
// verification, or a reconciliation mirror -- and never
// carries file bytes, credential values, or session text.

export interface EvidenceRow {
  evidenceId: string;
  effectId: string;
  executionId: string | null;
  // "write" | "verify" | "reconcile".
  source: string;
  // "verified" | "mismatch" | "inconclusive".
  verdict: string;
  // Machine-readable reason code, e.g. hash_match,
  // hash_mismatch, target_missing, io_error.
  reason: string;
  // Observed post-write snapshot (file kind + hash + length,
  // absent/dir markers, record flags, or artifact hash + task
  // status) -- never file bytes or credentials.
  observedJson: unknown;
  checkedMs: number;
  originDeviceId: string;
  schemaVersion: number;
  createdMs: number;
}

export interface ServerEvidenceRow {
  evidence_id: string;
  effect_id: string;
  execution_id: string | null;
  source: string;
  verdict: string;
  reason: string;
  observed_json: unknown;
  checked_ms: number;
  origin_device_id: string;
  schema_version: number;
  created_ms: number;
}

export function toEvidenceRow(server: ServerEvidenceRow): EvidenceRow {
  return {
    evidenceId: server.evidence_id,
    effectId: server.effect_id,
    executionId: server.execution_id,
    source: server.source,
    verdict: server.verdict,
    reason: server.reason,
    observedJson: server.observed_json,
    checkedMs: server.checked_ms,
    originDeviceId: server.origin_device_id,
    schemaVersion: server.schema_version,
    createdMs: server.created_ms,
  };
}

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface EvidenceGateway {
  list(): Promise<EvidenceRow[]>;
  get(evidenceId: string): Promise<EvidenceRow>;
}

export class EvidenceClient implements EvidenceGateway {
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

  async list(): Promise<EvidenceRow[]> {
    const rows = await this.request<ServerEvidenceRow[]>("/evidence/list");
    return rows.map(toEvidenceRow);
  }

  async get(evidenceId: string): Promise<EvidenceRow> {
    const row = await this.request<ServerEvidenceRow>(
      `/evidence/get?evidence_id=${evidenceId}`,
    );
    return toEvidenceRow(row);
  }
}
