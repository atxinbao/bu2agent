import { invoke } from "@tauri-apps/api/core";
import type {
  Memory,
  MemoryGateway,
  MemoryListFilter,
  MemorySaveInput,
  MemoryScope,
  MemorySource,
} from "./memories_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.
// Same converters as the HTTP client: the invoke payload
// carries the same snake_case keys the gateway expects.

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

export class InvokeMemoryClient implements MemoryGateway {
  async saveMemory(input: MemorySaveInput): Promise<Memory> {
    return toMemory(
      await invoke<ServerMemory>("memory_save", {
        layer: input.layer,
        content: input.content,
        scope: toServerScope(input.scope),
        current_scope: toServerScope(input.currentScope),
        source: toServerSource(input.source),
      }),
    );
  }

  async listMemories(filter?: MemoryListFilter): Promise<Memory[]> {
    return (
      await invoke<ServerMemory[]>("memory_list", {
        workspace_id: filter?.workspaceId,
        project_id: filter?.projectId,
        session_id: filter?.sessionId,
        layer: filter?.layer,
      })
    ).map(toMemory);
  }

  async getMemory(memoryId: string): Promise<Memory> {
    return toMemory(
      await invoke<ServerMemory>("memory_get", { memory_id: memoryId }),
    );
  }

  async deleteMemory(memoryId: string): Promise<Memory> {
    return toMemory(
      await invoke<ServerMemory>("memory_delete", { memory_id: memoryId }),
    );
  }
}
