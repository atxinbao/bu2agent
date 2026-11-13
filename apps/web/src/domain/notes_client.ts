// Notes client: browser/dev -> Mock Gateway notes
// surface (D1-6b-4 read + write).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Reads only
// need the gateway; create / append / delete go through
// the digest-bound approval flow (approval_request ->
// decide -> write) and carry a client-supplied
// idempotency key. The body is never in any row — only
// its hash (Q-D1-6b-16).

export interface Note {
  noteId: string;
  sourceId: string;
  nativeId: string;
  folder: string;
  name: string;
  bodyHash: string;
  idempotencyKey: string;
  effectId: string;
  policyVersion: string;
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
  originDeviceId: string;
}

export interface NoteSyncResult {
  syncedCount: number;
  skippedCount: number;
  lockedCount: number;
  totalCount: number;
  cursor: string;
}

// D1-6b-4: a create request. The idempotency key is
// client-supplied and required (Q-D1-6b-05); the
// approval id is the decided digest-bound approval for
// this exact note (Q-D1-6b-22).
export interface CreateNoteInput {
  folder: string;
  name: string;
  body: string;
  idempotencyKey: string;
  toolCallId?: string | null;
}

// D1-6b-4: an append request. The base hash is the
// version the approver saw; the gateway re-reads and
// 409s `note_changed` when the live text moved
// (Q-D1-6b-20). The appended text is joined to the live
// plaintext with no added separator.
export interface AppendNoteInput {
  text: string;
  baseBodyHash: string;
  idempotencyKey: string;
  toolCallId?: string | null;
}

// D1-6b-4: one delete outcome. `effectId` is null only
// on a tombstone replay whose original Effect could not
// be found.
export interface NoteDeleteResult {
  noteId: string;
  sourceId: string;
  nativeId: string;
  folder: string;
  deleted: boolean;
  effectId: string | null;
  replayed: boolean;
}

// Transport-neutral surface used by the Notes view. The
// HTTP client below and the invoke client both
// implement it, so the view never knows which transport
// it runs on.
export interface NotesGateway {
  syncNotes(folder: string): Promise<NoteSyncResult>;
  listNotes(): Promise<Note[]>;
  getNote(noteId: string): Promise<Note>;
  createNote(input: CreateNoteInput): Promise<Note>;
  appendNote(noteId: string, input: AppendNoteInput): Promise<Note>;
  deleteNote(noteId: string, toolCallId: string): Promise<NoteDeleteResult>;
}

interface ServerNote {
  note_id: string;
  source_id: string;
  native_id: string;
  folder: string;
  name: string;
  body_hash: string;
  idempotency_key: string;
  effect_id: string;
  policy_version: string;
  created_ms: number;
  updated_ms: number;
  deleted: boolean;
  origin_device_id: string;
}

function toNote(server: ServerNote): Note {
  return {
    noteId: server.note_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    folder: server.folder,
    name: server.name,
    bodyHash: server.body_hash,
    idempotencyKey: server.idempotency_key,
    effectId: server.effect_id,
    policyVersion: server.policy_version,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted,
    originDeviceId: server.origin_device_id,
  };
}

interface ServerNoteSyncResult {
  synced_count: number;
  skipped_count: number;
  locked_count: number;
  total_count: number;
  cursor: string;
}

function toNoteSyncResult(server: ServerNoteSyncResult): NoteSyncResult {
  return {
    syncedCount: server.synced_count,
    skippedCount: server.skipped_count,
    lockedCount: server.locked_count,
    totalCount: server.total_count,
    cursor: server.cursor,
  };
}

interface ServerNoteDeleteResult {
  note_id: string;
  source_id: string;
  native_id: string;
  folder: string;
  deleted: boolean;
  effect_id: string | null;
  replayed: boolean;
}

function toNoteDeleteResult(server: ServerNoteDeleteResult): NoteDeleteResult {
  return {
    noteId: server.note_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    folder: server.folder,
    deleted: server.deleted,
    effectId: server.effect_id,
    replayed: server.replayed,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("notes gateway URL must be a localhost Mock");
  }
}

export class NotesClient implements NotesGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`notes gateway ${method} ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`notes gateway POST ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async syncNotes(folder: string): Promise<NoteSyncResult> {
    const result = await this.postJson<ServerNoteSyncResult>("/api/notes/sync", { folder });
    return toNoteSyncResult(result);
  }

  async listNotes(): Promise<Note[]> {
    const notes = await this.requestJson<ServerNote[]>("GET", "/api/notes");
    return notes.map(toNote);
  }

  async getNote(noteId: string): Promise<Note> {
    const note = await this.requestJson<ServerNote>("GET", `/api/notes/${noteId}`);
    return toNote(note);
  }

  async createNote(input: CreateNoteInput): Promise<Note> {
    const row = await this.postJson<ServerNote>("/api/notes/create", {
      folder: input.folder,
      name: input.name,
      body: input.body,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId ?? null,
    });
    return toNote(row);
  }

  async appendNote(noteId: string, input: AppendNoteInput): Promise<Note> {
    const row = await this.postJson<ServerNote>(`/api/notes/${noteId}/append`, {
      text: input.text,
      base_body_hash: input.baseBodyHash,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId ?? null,
    });
    return toNote(row);
  }

  async deleteNote(noteId: string, toolCallId: string): Promise<NoteDeleteResult> {
    // The route is POST (plan §3); the `tool_call_id`
    // travels as a query parameter (the 6b-3 D-5
    // precedent).
    const result = await this.postJson<ServerNoteDeleteResult>(
      `/api/notes/${noteId}/delete?tool_call_id=${encodeURIComponent(toolCallId)}`,
      {},
    );
    return toNoteDeleteResult(result);
  }
}
