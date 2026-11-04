// Effect record client for the TS Domain Store.
//
// Same shape as the gateway effect endpoints, in the same
// order: list effects, get one effect, reconcile an unknown
// effect, verify a succeeded effect. Reconcile drives the
// re-check; verify runs a post-hoc drift check and returns
// the Evidence row it wrote -- the Effect row itself does
// not move. Neither consumes an approval.

import type { EvidenceRow, ServerEvidenceRow } from "./evidence_client";
import { toEvidenceRow } from "./evidence_client";

export interface Reconciliation {
  result: string;
  checkedMs: number;
  detail: string | null;
}

export interface EffectRecord {
  effectId: string;
  executionId: string | null;
  capability: string;
  operation: string;
  scope: string;
  inputDigest: string | null;
  approvalId: string | null;
  status: string;
  idempotencyKey: string | null;
  originDeviceId: string;
  schemaVersion: number;
  reconciliation: Reconciliation | null;
  detail: string | null;
}

interface ServerReconciliation {
  result: string;
  checked_ms: number;
  detail?: string | null;
}

interface ServerEffectRecord {
  effect_id: string;
  execution_id: string | null;
  capability: string;
  operation: string;
  scope: string;
  input_digest: string | null;
  approval_id: string | null;
  status: string;
  idempotency_key: string | null;
  origin_device_id: string;
  schema_version: number;
  reconciliation?: ServerReconciliation | null;
  detail?: string | null;
}

function toRecord(server: ServerEffectRecord): EffectRecord {
  return {
    effectId: server.effect_id,
    executionId: server.execution_id,
    capability: server.capability,
    operation: server.operation,
    scope: server.scope,
    inputDigest: server.input_digest,
    approvalId: server.approval_id,
    status: server.status,
    idempotencyKey: server.idempotency_key,
    originDeviceId: server.origin_device_id,
    schemaVersion: server.schema_version,
    reconciliation: server.reconciliation
      ? {
          result: server.reconciliation.result,
          checkedMs: server.reconciliation.checked_ms,
          detail: server.reconciliation.detail ?? null,
        }
      : null,
    detail: server.detail ?? null,
  };
}

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface EffectsGateway {
  list(): Promise<EffectRecord[]>;
  get(effectId: string): Promise<EffectRecord>;
  reconcile(effectId: string): Promise<EffectRecord>;
  verify(effectId: string): Promise<EvidenceRow>;
}

export class EffectsClient implements EffectsGateway {
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

  async list(): Promise<EffectRecord[]> {
    const records = await this.request<ServerEffectRecord[]>("/effects/list");
    return records.map(toRecord);
  }

  async get(effectId: string): Promise<EffectRecord> {
    const record = await this.request<ServerEffectRecord>(
      `/effects/get?effect_id=${effectId}`,
    );
    return toRecord(record);
  }

  private async postEffectId<T>(path: string, effectId: string): Promise<T> {
    return this.request<T>(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ effect_id: effectId }),
    });
  }

  async reconcile(effectId: string): Promise<EffectRecord> {
    const record = await this.postEffectId<ServerEffectRecord>(
      "/effects/reconcile",
      effectId,
    );
    return toRecord(record);
  }

  async verify(effectId: string): Promise<EvidenceRow> {
    // The verify response is an Evidence row; its mapping
    // lives in the evidence domain, not duplicated here.
    const row = await this.postEffectId<ServerEvidenceRow>(
      "/effects/verify",
      effectId,
    );
    return toEvidenceRow(row);
  }
}
