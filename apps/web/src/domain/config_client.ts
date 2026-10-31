// Config client for the TS Domain Store (Task 14, GET only):
// gateway facts, read-only.
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. There is no
// set/patch client on purpose: the gateway has no
// runtime-mutable self-owned config to write.

export interface ConfigState {
  filesRoot: string;
  gatewayAddr: string;
  provider: string;
}

interface ServerConfigState {
  files_root: string;
  gateway_addr: string;
  provider: string;
}

function toConfigState(server: ServerConfigState): ConfigState {
  return {
    filesRoot: server.files_root,
    gatewayAddr: server.gateway_addr,
    provider: server.provider,
  };
}

// Transport-neutral surface used by the Settings view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface ConfigGateway {
  getConfig(): Promise<ConfigState>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class ConfigClient implements ConfigGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
    });
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async getConfig(): Promise<ConfigState> {
    return toConfigState(await this.request<ServerConfigState>("GET", "/config"));
  }
}
