import { invoke } from "@tauri-apps/api/core";
import type {
  EffectRecord,
  EffectsGateway,
} from "./effects_client";
import type { EvidenceRow, ServerEvidenceRow } from "./evidence_client";
import { toEvidenceRow } from "./evidence_client";

// The Rust effect record serializes snake_case; the domain
// shape is camelCase, so the mapping lives here, not in
// the view.
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

// Effect line over Tauri invoke, backed by the Rust
// EffectsClient. Invoke argument keys are snake_case to
// match the Rust command parameters exactly.
export class InvokeEffectsClient implements EffectsGateway {
  async list(): Promise<EffectRecord[]> {
    const records = await invoke<ServerEffectRecord[]>("effect_list");
    return records.map(toRecord);
  }

  async get(effectId: string): Promise<EffectRecord> {
    const record = await invoke<ServerEffectRecord>("effect_get", {
      effect_id: effectId,
    });
    return toRecord(record);
  }

  private async postEffectId(
    command: string,
    effectId: string,
  ): Promise<ServerEffectRecord> {
    return invoke<ServerEffectRecord>(command, {
      effect_id: effectId,
    });
  }

  async reconcile(effectId: string): Promise<EffectRecord> {
    const record = await this.postEffectId("effect_reconcile", effectId);
    return toRecord(record);
  }

  async verify(effectId: string): Promise<EvidenceRow> {
    const row = await invoke<ServerEvidenceRow>("effect_verify", {
      effect_id: effectId,
    });
    return toEvidenceRow(row);
  }
}
