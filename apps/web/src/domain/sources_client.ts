// Source client for the TS Domain Store (D1-4, minimal
// list/detail/disconnect/reconnect): list, get, disconnect, reconnect.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view.

export interface Source {
  sourceId: string;
  kind: string;
  displayName: string;
  configJson: string;
  scope: string;
  health: string;
  cursor: string;
  dedupKey: string;
  status: string;
  createdMs: number;
  updatedMs: number;
  originDeviceId: string;
}

interface ServerSource {
  source_id: string;
  kind: string;
  display_name: string;
  config_json: string;
  scope: string;
  health: string;
  cursor: string;
  dedup_key: string;
  status: string;
  created_ms: number;
  updated_ms: number;
  origin_device_id: string;
}

function toSource(server: ServerSource): Source {
  return {
    sourceId: server.source_id,
    kind: server.kind,
    displayName: server.display_name,
    configJson: server.config_json,
    scope: server.scope,
    health: server.health,
    cursor: server.cursor,
    dedupKey: server.dedup_key,
    status: server.status,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    originDeviceId: server.origin_device_id,
  };
}

// Transport-neutral surface used by the Sources view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface SourceGateway {
  listSources(): Promise<Source[]>;
  getSource(sourceId: string): Promise<Source>;
  disconnectSource(sourceId: string): Promise<Source>;
  reconnectSource(sourceId: string): Promise<Source>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class SourceClient implements SourceGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async listSources(): Promise<Source[]> {
    return (await this.request<ServerSource[]>("GET", "/sources/list")).map(toSource);
  }

  async getSource(sourceId: string): Promise<Source> {
    return toSource(await this.request<ServerSource>("GET", `/sources/${sourceId}`));
  }

  async disconnectSource(sourceId: string): Promise<Source> {
    return toSource(
      await this.request<ServerSource>("POST", `/sources/${sourceId}/disconnect`, {}),
    );
  }

  async reconnectSource(sourceId: string): Promise<Source> {
    return toSource(
      await this.request<ServerSource>("POST", `/sources/${sourceId}/reconnect`, {}),
    );
  }
}
