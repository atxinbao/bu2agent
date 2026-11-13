// Invoke notes client: packaged app -> Tauri shell
// notes_* commands (D1-6b-4, read + write). Param
// names are snake_case to match the shell's
// rename_all; the invoke transport returns the Rust
// structs' snake_case fields, so the mapping lives
// here, not in the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  AppendNoteInput,
  CreateNoteInput,
  Note,
  NoteDeleteResult,
  NoteSyncResult,
  NotesGateway,
} from "./notes_client";

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

export class InvokeNotesClient implements NotesGateway {
  async syncNotes(folder: string): Promise<NoteSyncResult> {
    const result = await invoke<ServerNoteSyncResult>("notes_sync", { folder });
    return toNoteSyncResult(result);
  }

  async listNotes(): Promise<Note[]> {
    const notes = await invoke<ServerNote[]>("notes_list");
    return notes.map(toNote);
  }

  async getNote(noteId: string): Promise<Note> {
    const note = await invoke<ServerNote>("notes_get", { note_id: noteId });
    return toNote(note);
  }

  async createNote(input: CreateNoteInput): Promise<Note> {
    const row = await invoke<ServerNote>("notes_create", {
      folder: input.folder,
      name: input.name,
      body: input.body,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId ?? null,
    });
    return toNote(row);
  }

  async appendNote(noteId: string, input: AppendNoteInput): Promise<Note> {
    const row = await invoke<ServerNote>("notes_append", {
      note_id: noteId,
      text: input.text,
      base_body_hash: input.baseBodyHash,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId ?? null,
    });
    return toNote(row);
  }

  async deleteNote(noteId: string, toolCallId: string): Promise<NoteDeleteResult> {
    const result = await invoke<ServerNoteDeleteResult>("notes_delete", {
      note_id: noteId,
      tool_call_id: toolCallId,
    });
    return toNoteDeleteResult(result);
  }
}
