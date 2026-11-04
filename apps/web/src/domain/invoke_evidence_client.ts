import { invoke } from "@tauri-apps/api/core";
import type { EvidenceGateway } from "./evidence_client";
import type { EvidenceRow, ServerEvidenceRow } from "./evidence_client";
import { toEvidenceRow } from "./evidence_client";

// Evidence line over Tauri invoke, backed by the Rust
// EvidenceClient. Invoke argument keys are snake_case to
// match the Rust command parameters exactly. The mapping
// lives in the evidence domain, not duplicated here.
export class InvokeEvidenceClient implements EvidenceGateway {
  async list(): Promise<EvidenceRow[]> {
    const rows = await invoke<ServerEvidenceRow[]>("evidence_list");
    return rows.map(toEvidenceRow);
  }

  async get(evidenceId: string): Promise<EvidenceRow> {
    const row = await invoke<ServerEvidenceRow>("evidence_get", {
      evidence_id: evidenceId,
    });
    return toEvidenceRow(row);
  }
}
