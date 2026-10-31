// Model client for the TS Domain Store (Task 12): the running
// model config plus any staged selection.
//
// Same shape as the policy client: the gateway returns
// snake_case; the domain shape is camelCase, so the mapping
// lives here, not in the view. These calls carry no approval
// loop. The key is never accepted here and never appears in
// a response: only configured + the ****last-4 redaction.
// POST stages a selection that takes effect only after a
// gateway restart; the running client is never rebuilt.

export interface ModelState {
  provider: string;
  model: string;
  baseUrl: string;
  keyConfigured: boolean;
  keyRedacted: string | null;
  stagedModel: string | null;
  stagedBaseUrl: string | null;
  restartNeeded: boolean;
}

interface ServerModelState {
  provider: string;
  model: string;
  base_url: string;
  key_configured: boolean;
  key_redacted: string | null;
  staged_model: string | null;
  staged_base_url: string | null;
  restart_needed: boolean;
}

function toModelState(server: ServerModelState): ModelState {
  return {
    provider: server.provider,
    model: server.model,
    baseUrl: server.base_url,
    keyConfigured: server.key_configured,
    keyRedacted: server.key_redacted,
    stagedModel: server.staged_model,
    stagedBaseUrl: server.staged_base_url,
    restartNeeded: server.restart_needed,
  };
}

export interface ModelSetPatch {
  model?: string | null;
  baseUrl?: string | null;
}

// Transport-neutral surface used by the Settings view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface ModelGateway {
  getModel(): Promise<ModelState>;
  setModel(patch: ModelSetPatch): Promise<ModelState>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class ModelClient implements ModelGateway {
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

  async getModel(): Promise<ModelState> {
    return toModelState(await this.request<ServerModelState>("GET", "/model"));
  }

  async setModel(patch: ModelSetPatch): Promise<ModelState> {
    const state = await this.request<ServerModelState>("POST", "/model", {
      model: patch.model ?? null,
      base_url: patch.baseUrl ?? null,
    });
    return toModelState(state);
  }
}
