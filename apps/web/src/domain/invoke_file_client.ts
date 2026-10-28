import { invoke } from "@tauri-apps/api/core";
import type { FileGateway, GatewayFileEntry, GatewayFileStat, GatewayBatchReadItem, GatewayBatchReadResult, GatewayBatchStatResult, GatewaySearchResult, GatewayThumbnail, GatewayLibraryResult } from "./file_client";

// The Rust file entry serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
interface ServerFileEntry {
  name: string;
  path: string;
  is_dir: boolean;
  size_bytes: number;
}

interface ServerFileStat {
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

  async stat(path: string): Promise<GatewayFileStat> {
    const result = await invoke<ServerFileStat>("file_stat", { path });
    return { path: result.path, isDir: result.is_dir, sizeBytes: result.size_bytes };
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

  async rename(from: string, to: string, approved: boolean): Promise<void> {
    await invoke("file_rename", { from_path: from, to_path: to, approved });
  }

  async mkdir(path: string, approved: boolean): Promise<void> {
    await invoke("file_mkdir", { path, approved });
  }

  async reads(items: GatewayBatchReadItem[]): Promise<GatewayBatchReadResult[]> {
    return invoke<GatewayBatchReadResult[]>("file_reads", { items });
  }
  async stats(paths: string[]): Promise<GatewayBatchStatResult[]> {
    const results = await invoke<Array<{ path: string; ok: boolean; stat?: ServerFileStat; error?: string }>>("file_stats", { paths });
    return results.map((item) => ({ path: item.path, ok: item.ok, error: item.error, stat: item.stat ? { path: item.stat.path, isDir: item.stat.is_dir, sizeBytes: item.stat.size_bytes } : undefined }));
  }
  async search(query: string, startPath = ""): Promise<GatewaySearchResult> {
    const result = await invoke<{ entries: ServerFileEntry[]; truncated: boolean }>("file_search", { query, start_path: startPath });
    return { truncated: result.truncated, entries: result.entries.map(toEntry) };
  }
  async subscribe(path: string, cursor?: string, waitMs = 25000): Promise<{ changed: boolean; cursor: string; entries?: GatewayFileEntry[] }> {
    const result = await invoke<{ changed: boolean; cursor: string; entries?: ServerFileEntry[] }>("file_subscribe", { path, cursor, wait_ms: waitMs });
    return { changed: result.changed, cursor: result.cursor, entries: result.entries?.map(toEntry) };
  }
  async thumbnail(path: string, width = 256): Promise<GatewayThumbnail> {
    const result = await invoke<{ path: string; content_type: string; content_base64: string }>("file_thumbnail", { path, width });
    return { path: result.path, contentType: result.content_type, contentBase64: result.content_base64 };
  }
  async export(from: string, to: string, approved: boolean): Promise<number> {
    return invoke<number>("file_export", { fromPath: from, toPath: to, approved });
  }
  async library(kinds?: string[], limit?: number): Promise<GatewayLibraryResult> {
    const result = await invoke<{ items: Array<{ path: string; kind: string; is_dir: boolean; size_bytes: number; modified_ms: number }>; truncated: boolean }>("file_library", { kinds, limit });
    return { truncated: result.truncated, items: result.items.map((item) => ({ path: item.path, kind: item.kind, isDir: item.is_dir, sizeBytes: item.size_bytes, modifiedMs: item.modified_ms })) };
  }
}
