// Memory client for the TS Domain Store (D1-5, minimal
// save/list/delete): save, list, get, delete (tombstone).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. The save
// request carries the scope triple, the caller's
// declared current scope, and the provenance record
// (server field names in the comment on each DTO
// member).

export interface MemoryScope {
  workspaceId?: string;
  projectId?: string;
  sessionId?: string;
}

export interface MemorySource {
  sessionId?: string;
  executionId?: string;
  sourceId?: string;
  author: string;
}

export interface Memory {
  memoryId: string;
  layer: string;
  scopeJson: string;
  content: string;
  sourceJson: string;
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
  originDeviceId: string;
}

interface ServerMemory {
  memory_id: string;
  layer: string;
  scope_json: string;
  content: string;
  source_json: string;
  created_ms: number;
  updated_ms: number;
  deleted: boolean;
  origin_device_id: string;
}

function toMemory(server: ServerMemory): Memory {
  return {
    memoryId: server.memory_id,
    layer: server.layer,
    scopeJson: server.scope_json,
    content: server.content,
    sourceJson: server.source_json,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted,
    originDeviceId: server.origin_device_id,
  };
}

export interface MemorySaveInput {
  layer: "project" | "user";
  content: string;
  scope: MemoryScope;
  currentScope: MemoryScope;
  source: MemorySource;
}

export interface MemoryListFilter {
  workspaceId?: string;
  projectId?: string;
  sessionId?: string;
  layer?: string;
}

// Transport-neutral surface used by the Memories view.
// Only the HTTP client implements it in this batch:
// there is no InvokeMemoryClient and no shell memory
// command (the plan names neither for D1-5), so the
// invoke branch leaves the field unset — see App.tsx.
export interface MemoryGateway {
  saveMemory(input: MemorySaveInput): Promise<Memory>;
  listMemories(filter?: MemoryListFilter): Promise<Memory[]>;
  getMemory(memoryId: string): Promise<Memory>;
  deleteMemory(memoryId: string): Promise<Memory>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

function toServerScope(scope: MemoryScope): Record<string, string> {
  const out: Record<string, string> = {};
  if (scope.workspaceId !== undefined) out.workspace_id = scope.workspaceId;
  if (scope.projectId !== undefined) out.project_id = scope.projectId;
  if (scope.sessionId !== undefined) out.session_id = scope.sessionId;
  return out;
}

function toServerSource(source: MemorySource): Record<string, string> {
  const out: Record<string, string> = { author: source.author };
  if (source.sessionId !== undefined) out.session_id = source.sessionId;
  if (source.executionId !== undefined) out.execution_id = source.executionId;
  if (source.sourceId !== undefined) out.source_id = source.sourceId;
  return out;
}

export class MemoryClient implements MemoryGateway {
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

  async saveMemory(input: MemorySaveInput): Promise<Memory> {
    return toMemory(
      await this.request<ServerMemory>("POST", "/memories/save", {
        layer: input.layer,
        content: input.content,
        scope: toServerScope(input.scope),
        current_scope: toServerScope(input.currentScope),
        source: toServerSource(input.source),
      }),
    );
  }

  async listMemories(filter?: MemoryListFilter): Promise<Memory[]> {
    const params = new URLSearchParams();
    if (filter?.workspaceId !== undefined) params.set("workspace_id", filter.workspaceId);
    if (filter?.projectId !== undefined) params.set("project_id", filter.projectId);
    if (filter?.sessionId !== undefined) params.set("session_id", filter.sessionId);
    if (filter?.layer !== undefined) params.set("layer", filter.layer);
    const query = params.toString();
    return (await this.request<ServerMemory[]>("GET", `/memories/list${query === "" ? "" : `?${query}`}`)).map(
      toMemory,
    );
  }

  async getMemory(memoryId: string): Promise<Memory> {
    return toMemory(await this.request<ServerMemory>("GET", `/memories/${memoryId}`));
  }

  async deleteMemory(memoryId: string): Promise<Memory> {
    return toMemory(
      await this.request<ServerMemory>("POST", `/memories/${memoryId}/delete`, {}),
    );
  }
}
