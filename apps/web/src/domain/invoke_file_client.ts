import { invoke } from "@tauri-apps/api/core";
import type { FileGateway, GatewayFileEntry } from "./file_client";

// The Rust file entry serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
interface ServerFileEntry {
  name: string;
  path: string;
  is_dir: boolean;
  size_bytes: number;
}

function toEntry(entry: ServerFileEntry): GatewayFileEntry {
  return {
    name: entry.name,
    path: entry.path,
    isDir: entry.is_dir,
    sizeBytes: entry.size_bytes,
  };
}

// File line over Tauri invoke, backed by the Rust
// FileGatewayClient. Invoke argument keys are snake_case to
// match the Rust command parameters exactly.
export class InvokeFileClient implements FileGateway {
  async list(path = ""): Promise<GatewayFileEntry[]> {
    const entries = await invoke<ServerFileEntry[]>("file_list", { path });
    return entries.map(toEntry);
  }

  async readText(path: string, chunkLen: number, maxTotal: number): Promise<string> {
    return invoke<string>("file_read_text", {
      path,
      chunk_len: chunkLen,
      max_total: maxTotal,
    });
  }

  async write(
    path: string,
    text: string,
    approved: boolean,
    append = false,
  ): Promise<number> {
    return invoke<number>("file_write", { path, text, approved, append });
  }

  async delete(path: string, approved: boolean): Promise<void> {
    await invoke("file_delete", { path, approved });
  }
}
