import { invoke } from "@tauri-apps/api/core";
import type { Source, SourceGateway } from "./sources_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

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

export class InvokeSourceClient implements SourceGateway {
  async listSources(): Promise<Source[]> {
    return (await invoke<ServerSource[]>("source_list")).map(toSource);
  }

  async getSource(sourceId: string): Promise<Source> {
    return toSource(await invoke<ServerSource>("source_get", { source_id: sourceId }));
  }

  async disconnectSource(sourceId: string): Promise<Source> {
    return toSource(
      await invoke<ServerSource>("source_disconnect", { source_id: sourceId }),
    );
  }
}
