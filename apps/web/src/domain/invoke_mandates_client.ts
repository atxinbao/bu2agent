import { invoke } from "@tauri-apps/api/core";
import type {
  MandateFilters,
  MandateRow,
  MandatesGateway,
} from "./mandates_client";

// The Rust mandate row serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
interface ServerMandateRow {
  mandate_id: string;
  task_id: string | null;
  execution_id: string | null;
  goal: string;
  allowed_json: string;
  policy_version_id: string;
  status: string;
  created_ms: number;
  origin_device_id: string;
}

function toRow(server: ServerMandateRow): MandateRow {
  return {
    mandateId: server.mandate_id,
    taskId: server.task_id,
    executionId: server.execution_id,
    goal: server.goal,
    allowedJson: server.allowed_json,
    policyVersionId: server.policy_version_id,
    status: server.status,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
  };
}

// Mandates line over Tauri invoke, backed by the Rust
// MandatesClient. Invoke argument keys are snake_case to match
// the Rust command parameters exactly; a missing filter is
// passed as null, which the Rust Option<String> reads as None.
export class InvokeMandatesClient implements MandatesGateway {
  async list(filters?: MandateFilters): Promise<MandateRow[]> {
    const rows = await invoke<ServerMandateRow[]>("mandates_list", {
      task_id: filters?.taskId ?? null,
      execution_id: filters?.executionId ?? null,
    });
    return rows.map(toRow);
  }

  async get(mandateId: string): Promise<MandateRow> {
    const row = await invoke<ServerMandateRow>("mandates_get", {
      mandate_id: mandateId,
    });
    return toRow(row);
  }
}
