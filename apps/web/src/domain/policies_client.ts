// Policies client for the TS Domain Store.
//
// Policy versions only: list.
// FU-3C (Q-R0-01): the `/policies/versions` route had no
// production client; this wires it for the SettingsView
// policy section. Rows live in SQLite only (P0-D); without
// a configured SQLite database the route answers 5xx
// fail-closed.
//
// `documentJson` is a JSON string, not an object; the view
// displays it as text (full in the expanded row detail).

export interface PolicyVersionRow {
  versionId: string;
  version: number;
  documentJson: string;
  digest: string;
  supersedes: string | null;
  createdMs: number;
  originDeviceId: string;
}

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

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface PoliciesGateway {
  listVersions(): Promise<PolicyVersionRow[]>;
}

export class PoliciesClient implements PoliciesGateway {
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

  async listVersions(): Promise<PolicyVersionRow[]> {
    const rows = await this.request<ServerPolicyVersionRow[]>("/policies/versions");
    return rows.map(toRow);
  }
}
