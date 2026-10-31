import { invoke } from "@tauri-apps/api/core";
import type {
  ConfigGateway,
  ConfigState,
} from "./config_client";

// The Rust struct serializes snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.

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

export class InvokeConfigClient implements ConfigGateway {
  async getConfig(): Promise<ConfigState> {
    return toConfigState(await invoke<ServerConfigState>("config_get"));
  }
}
