// File gateway client for the TS Domain Store (Phase 2).
//
// Same rules as the Rust file gateway client, in the same order:
// - Paths are relative to the server's scoped root. Absolute
//   paths and `..` are rejected client-side, before any request.
// - Reads (list / read) may retry at most once on 401, after
//   calling the caller's refresh callback once.
// - Writes and deletes are NEVER retried on 401: a retry could
//   duplicate a write. They also require approval: without it,
//   the client refuses before sending anything.
// - Chunked reads loop until EOF and refuse files larger than
//   maxTotal. Base64 is the wire form; text is decoded with
//   TextDecoder, never lossily.

export interface GatewayFileEntry {
  name: string;
  path: string;
  isDir: boolean;
  sizeBytes: number;
}

export interface FileChunk {
  data: Uint8Array;
  eof: boolean;
  totalSize: number;
  bytesRead: number;
}

export interface GatewayFileStat {
  path: string;
  isDir: boolean;
  sizeBytes: number;
}

export interface GatewayBatchReadItem { path: string; offset?: number; len?: number }
export interface GatewayBatchReadResult { path: string; ok: boolean; result?: { content_base64: string; eof: boolean; total_size: number; bytes_read: number }; error?: string }
export interface GatewayBatchStatResult { path: string; ok: boolean; stat?: GatewayFileStat; error?: string }
export interface GatewaySearchResult { entries: GatewayFileEntry[]; truncated: boolean }
export interface GatewayThumbnail { path: string; contentType: string; contentBase64: string }
export interface GatewayLibraryItem { path: string; kind: string; isDir: boolean; sizeBytes: number; modifiedMs: number }
export interface GatewayLibraryResult { items: GatewayLibraryItem[]; truncated: boolean }

interface ServerFileEntry {
  name: string;
  path: string;
  is_dir: boolean;
  size_bytes: number;
}

interface ServerReadResponse {
  path: string;
  offset: number;
  content_base64: string;
  eof: boolean;
  total_size: number;
  bytes_read: number;
}

interface ServerFileStat {
  path: string;
  is_dir: boolean;
  size_bytes: number;
}

function checkRelative(path: string): void {
  if (path.startsWith("/") || path.split("/").includes("..")) {
    throw new Error("file path must not escape the root");
  }
}

function decodeBase64(text: string): Uint8Array {
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function encodeBase64(data: Uint8Array): string {
  let binary = "";
  for (const byte of data) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

// Transport-neutral surface used by the Files view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface FileGateway {
  list(path?: string): Promise<GatewayFileEntry[]>;
  stat(path: string): Promise<GatewayFileStat>;
  readText(path: string, chunkLen: number, maxTotal: number): Promise<string>;
  write(path: string, text: string, approved: boolean, append?: boolean): Promise<number>;
  delete(path: string, approved: boolean): Promise<void>;
  rename(from: string, to: string, approved: boolean): Promise<void>;
  mkdir(path: string, approved: boolean): Promise<void>;
  reads(items: GatewayBatchReadItem[]): Promise<GatewayBatchReadResult[]>;
  stats(paths: string[]): Promise<GatewayBatchStatResult[]>;
  search(query: string, startPath?: string): Promise<GatewaySearchResult>;
  subscribe(path: string, cursor?: string, waitMs?: number, signal?: AbortSignal): Promise<{ changed: boolean; cursor: string; entries?: GatewayFileEntry[] }>;
  thumbnail(path: string, width?: number): Promise<GatewayThumbnail>;
  export(from: string, to: string, approved: boolean): Promise<number>;
  library(kinds?: string[], limit?: number): Promise<GatewayLibraryResult>;
}

export class FileGatewayClient implements FileGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestWithRetry<T>(
    path: string,
    init: RequestInit | undefined,
    refresh: (() => Promise<void>) | undefined,
    allowRetry: boolean,
  ): Promise<T> {
    let retried = false;
    for (;;) {
      const response = await fetch(`${this.baseUrl}${path}`, init);
      if (response.status === 401 && allowRetry && !retried && refresh) {
        retried = true;
        await refresh();
        continue;
      }
      if (!response.ok) {
        throw new Error(`mock gateway returned HTTP ${response.status}`);
      }
      return (await response.json()) as T;
    }
  }

  async list(
    path = "",
    refresh?: () => Promise<void>,
  ): Promise<GatewayFileEntry[]> {
    if (path !== "") {
      checkRelative(path);
    }
    const query = path === "" ? "/files/list" : `/files/list?path=${path}`;
    const entries = await this.requestWithRetry<ServerFileEntry[]>(
      query,
      undefined,
      refresh,
      true,
    );
    return entries.map((e) => ({
      name: e.name,
      path: e.path,
      isDir: e.is_dir,
      sizeBytes: e.size_bytes,
    }));
  }

  async stat(path: string): Promise<GatewayFileStat> {
    if (path !== "") {
      checkRelative(path);
    }
    const result = await this.requestWithRetry<ServerFileStat>(
      `/files/stat?path=${path}`,
      undefined,
      undefined,
      true,
    );
    return { path: result.path, isDir: result.is_dir, sizeBytes: result.size_bytes };
  }

  async readChunk(
    path: string,
    offset: number,
    len: number,
    refresh?: () => Promise<void>,
  ): Promise<FileChunk> {
    checkRelative(path);
    const result = await this.requestWithRetry<ServerReadResponse>(
      `/files/read?path=${path}&offset=${offset}&len=${len}`,
      undefined,
      refresh,
      true,
    );
    return {
      data: decodeBase64(result.content_base64),
      eof: result.eof,
      totalSize: result.total_size,
      bytesRead: result.bytes_read,
    };
  }

  async readAll(
    path: string,
    chunkLen: number,
    maxTotal: number,
    refresh?: () => Promise<void>,
  ): Promise<Uint8Array> {
    checkRelative(path);
    const parts: Uint8Array[] = [];
    let total = 0;
    let offset = 0;
    for (;;) {
      const chunk = await this.readChunk(path, offset, chunkLen, refresh);
      if (total + chunk.bytesRead > maxTotal) {
        throw new Error("file exceeds maxTotal");
      }
      total += chunk.bytesRead;
      offset += chunk.bytesRead;
      parts.push(chunk.data);
      if (chunk.eof || chunk.bytesRead === 0) {
        break;
      }
    }
    const out = new Uint8Array(total);
    let at = 0;
    for (const part of parts) {
      out.set(part, at);
      at += part.length;
    }
    return out;
  }

  async readText(
    path: string,
    chunkLen: number,
    maxTotal: number,
    refresh?: () => Promise<void>,
  ): Promise<string> {
    const bytes = await this.readAll(path, chunkLen, maxTotal, refresh);
    return new TextDecoder().decode(bytes);
  }

  // Plain POST that reports the status instead of throwing a
  // generic error, so the approval flow can word a 403 exactly.
  // Never retried: like the old write path, these are writes.
  private async postJson(
    path: string,
    body: unknown,
  ): Promise<{ status: number; json: Record<string, unknown> }> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const json = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    return { status: response.status, json };
  }

  // Request a bound approval for one file operation, then
  // decide it approved. `approved` means the window already
  // confirmed; a 403 on execute after approval can only mean
  // the operation no longer matches the record (changed,
  // already used, or expired) -- never a user rejection.
  private async boundApprovalId(
    tool: string,
    path: string,
    toPath?: string,
    contentBase64?: string,
    append?: boolean,
  ): Promise<string> {
    const requestBody: Record<string, unknown> = { tool, path };
    if (toPath !== undefined) {
      requestBody.to_path = toPath;
    }
    if (contentBase64 !== undefined) {
      requestBody.content_base64 = contentBase64;
    }
    if (append !== undefined) {
      requestBody.append = append;
    }
    const request = await this.postJson("/approval/request", requestBody);
    if (request.status !== 200) {
      throw new Error(`mock gateway returned HTTP ${request.status}`);
    }
    const toolCallId = request.json.tool_call_id;
    if (typeof toolCallId !== "string" || toolCallId === "") {
      throw new Error("approval tool returned no tool_call_id");
    }
    const decision = await this.postJson("/approval/decide", {
      tool_call_id: toolCallId,
      status: "approved",
    });
    if (decision.status !== 200) {
      throw new Error(`mock gateway returned HTTP ${decision.status}`);
    }
    return toolCallId;
  }

  async write(
    path: string,
    text: string,
    approved: boolean,
    append = false,
  ): Promise<number> {
    if (!approved) {
      throw new Error("file write requires approval");
    }
    checkRelative(path);
    const data = new TextEncoder().encode(text);
    const contentBase64 = encodeBase64(data);
    const approvalId = await this.boundApprovalId("fs.write", path, undefined, contentBase64, append);
    const result = await this.postJson("/files/write", {
      path,
      content_base64: contentBase64,
      append,
      approval_id: approvalId,
    });
    if (result.status === 403) {
      throw new Error(
        "approved file operation does not match this execution (changed, already used, or expired); nothing was written",
      );
    }
    if (result.status !== 200) {
      throw new Error(`mock gateway returned HTTP ${result.status}`);
    }
    const size = result.json.size_bytes;
    return typeof size === "number" ? size : data.length;
  }

  async delete(path: string, approved: boolean): Promise<void> {
    if (!approved) {
      throw new Error("file delete requires approval");
    }
    checkRelative(path);
    const approvalId = await this.boundApprovalId("fs.delete", path);
    const result = await this.postJson("/files/delete", {
      path,
      approval_id: approvalId,
    });
    if (result.status === 403) {
      throw new Error(
        "approved file operation does not match this execution (changed, already used, or expired); nothing was deleted",
      );
    }
    if (result.status !== 200) {
      throw new Error(`mock gateway returned HTTP ${result.status}`);
    }
  }

  async rename(from: string, to: string, approved: boolean): Promise<void> {
    if (!approved) {
      throw new Error("file rename requires approval");
    }
    checkRelative(from);
    checkRelative(to);
    const approvalId = await this.boundApprovalId("fs.rename", from, to);
    const result = await this.postJson("/files/rename", { from, to, approval_id: approvalId });
    if (result.status === 403) {
      throw new Error("approved file operation does not match this execution (changed, already used, or expired); nothing was renamed");
    }
    if (result.status === 409) {
      throw new Error("rename target already exists; nothing was renamed");
    }
    if (result.status !== 200) {
      throw new Error(`mock gateway returned HTTP ${result.status}`);
    }
  }

  async mkdir(path: string, approved: boolean): Promise<void> {
    if (!approved) {
      throw new Error("file mkdir requires approval");
    }
    checkRelative(path);
    const approvalId = await this.boundApprovalId("fs.mkdir", path);
    const result = await this.postJson("/files/mkdir", { path, approval_id: approvalId });
    if (result.status === 403) {
      throw new Error("approved file operation does not match this execution (changed, already used, or expired); directory was not created");
    }
    if (result.status !== 200) {
      throw new Error(`mock gateway returned HTTP ${result.status}`);
    }
  }

  async reads(items: GatewayBatchReadItem[]): Promise<GatewayBatchReadResult[]> {
    const result = await this.requestWithRetry<{ items: GatewayBatchReadResult[] }>("/files/reads", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ items }) }, undefined, false);
    return result.items;
  }
  async stats(paths: string[]): Promise<GatewayBatchStatResult[]> {
    const result = await this.requestWithRetry<{ items: GatewayBatchStatResult[] }>("/files/stats", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ paths }) }, undefined, false);
    return result.items;
  }
  async search(query: string, startPath = ""): Promise<GatewaySearchResult> {
    return this.requestWithRetry<GatewaySearchResult>("/files/search", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ query, start_path: startPath }) }, undefined, false);
  }
  async subscribe(path: string, cursor?: string, waitMs = 25000, signal?: AbortSignal): Promise<{ changed: boolean; cursor: string; entries?: GatewayFileEntry[] }> {
    const result = await this.requestWithRetry<{ changed: boolean; cursor: string; entries?: ServerFileEntry[] }>("/files/subscribe", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ path, cursor, wait_ms: waitMs }), signal }, undefined, false);
    return { changed: result.changed, cursor: result.cursor, entries: result.entries?.map((e) => ({ name: e.name, path: e.path, isDir: e.is_dir, sizeBytes: e.size_bytes })) };
  }
  async thumbnail(path: string, width = 256): Promise<GatewayThumbnail> {
    const result = await this.requestWithRetry<{ path: string; content_type: string; content_base64: string }>(`/files/thumbnail?path=${encodeURIComponent(path)}&w=${width}&format=json`, undefined, undefined, false);
    return { path: result.path, contentType: result.content_type, contentBase64: result.content_base64 };
  }
  async export(from: string, to: string, approved: boolean): Promise<number> {
    if (!approved) throw new Error("file export requires approval");
    checkRelative(from);
    if (!to.startsWith("/")) throw new Error("export target must be an absolute path outside the file root");
    const approvalId = await this.boundApprovalId("fs.export", from, to);
    const result = await this.postJson("/files/export", { from, to, approval_id: approvalId });
    if (result.status === 403) throw new Error("approved file operation does not match this execution (changed, already used, or expired); nothing was exported");
    if (result.status === 409) throw new Error("export target already exists; nothing was exported");
    if (result.status !== 200) throw new Error(`mock gateway returned HTTP ${result.status}`);
    return typeof result.json.size_bytes === "number" ? result.json.size_bytes : 0;
  }
  async library(kinds?: string[], limit?: number): Promise<GatewayLibraryResult> {
    const result = await this.requestWithRetry<{ items: Array<{ path: string; kind: string; is_dir: boolean; size_bytes: number; modified_ms: number }>; truncated: boolean }>("/files/library", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ kinds, limit }) }, undefined, false);
    return { truncated: result.truncated, items: result.items.map((e) => ({ path: e.path, kind: e.kind, isDir: e.is_dir, sizeBytes: e.size_bytes, modifiedMs: e.modified_ms })) };
  }
}
