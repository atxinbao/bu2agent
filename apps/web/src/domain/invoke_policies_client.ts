import { invoke } from "@tauri-apps/api/core";
import type { PoliciesGateway, PolicyVersionRow } from "./policies_client";

// The Rust policy version row serializes snake_case; the
// domain shape is camelCase, so the mapping lives here, not
// in the view.
interface ServerPolicyVersionRow {
  version_id: string;
  version: number;
  document_json: string;
  digest: string;
  supersedes: string | null;
  created_ms: number;
  origin_device_id: string;
}

function toRow(server: ServerPolicyVersionRow): PolicyVersionRow {
  return {
    versionId: server.version_id,
    version: server.version,
    documentJson: server.document_json,
    digest: server.digest,
    supersedes: server.supersedes,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
  };
}

// Policies line over Tauri invoke, backed by the Rust
// PoliciesClient. Invoke argument keys are snake_case to match
// the Rust command parameters exactly.
export class InvokePoliciesClient implements PoliciesGateway {
  async listVersions(): Promise<PolicyVersionRow[]> {
    const rows = await invoke<ServerPolicyVersionRow[]>("policies_versions_list", {});
    return rows.map(toRow);
  }
}
