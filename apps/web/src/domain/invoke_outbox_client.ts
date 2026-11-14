import { invoke } from "@tauri-apps/api/core";
import type { OutboxGateway, OutboxRow } from "./outbox_client";

// The Rust outbox row serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
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

// Outbox line over Tauri invoke, backed by the Rust
// OutboxClient. Invoke argument keys are snake_case to match
// the Rust command parameters exactly.
export class InvokeOutboxClient implements OutboxGateway {
  async list(): Promise<OutboxRow[]> {
    const rows = await invoke<ServerOutboxRow[]>("outbox_list", {});
    return rows.map(toRow);
  }

  async get(outboxId: string): Promise<OutboxRow> {
    const row = await invoke<ServerOutboxRow>("outbox_get", {
      outbox_id: outboxId,
    });
    return toRow(row);
  }

  async deliver(outboxId: string): Promise<OutboxRow> {
    const row = await invoke<ServerOutboxRow>("outbox_deliver", {
      outbox_id: outboxId,
    });
    return toRow(row);
  }

  async requeue(outboxId: string): Promise<OutboxRow> {
    const row = await invoke<ServerOutboxRow>("outbox_requeue", {
      outbox_id: outboxId,
    });
    return toRow(row);
  }
}
