import { invoke } from "@tauri-apps/api/core";
import type {
  ModelGateway,
  ModelSetPatch,
  ModelState,
} from "./model_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

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

export class InvokeModelClient implements ModelGateway {
  async getModel(): Promise<ModelState> {
    return toModelState(await invoke<ServerModelState>("model_get"));
  }

  async setModel(patch: ModelSetPatch): Promise<ModelState> {
    const state = await invoke<ServerModelState>("model_set", {
      model: patch.model ?? null,
      base_url: patch.baseUrl ?? null,
    });
    return toModelState(state);
  }
}
