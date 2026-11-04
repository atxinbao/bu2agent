import { invoke } from "@tauri-apps/api/core";
import type { InboxGateway, InboxRow } from "./inbox_client";

// The Rust inbox row serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
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

// Inbox line over Tauri invoke, backed by the Rust
// InboxClient. Invoke argument keys are snake_case to match
// the Rust command parameters exactly.
export class InvokeInboxClient implements InboxGateway {
  async list(
    kind?: string | null,
    openOnly?: boolean,
  ): Promise<InboxRow[]> {
    const rows = await invoke<ServerInboxRow[]>("inbox_list", {
      kind: kind ?? null,
      open_only: openOnly ?? false,
    });
    return rows.map(toRow);
  }

  async get(inboxId: string): Promise<InboxRow> {
    const row = await invoke<ServerInboxRow>("inbox_get", {
      inbox_id: inboxId,
    });
    return toRow(row);
  }

  async ack(inboxId: string): Promise<InboxRow> {
    const row = await invoke<ServerInboxRow>("inbox_ack", {
      inbox_id: inboxId,
    });
    return toRow(row);
  }
}
