// Invoke iMessage client: packaged app -> Tauri shell
// imessage_* commands (MCL-1 threads-only read, MCL-4
// send). Param names are snake_case to match the
// shell's rename_all; the invoke transport returns the
// Rust structs' snake_case fields, so the mapping lives
// here, not in the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  IMessageGateway,
  IMessageSyncResult,
  IMessageThread,
  SendIMessageInput,
  SentIMessage,
} from "./imessage_client";

interface ServerIMessageSyncResult {
  synced_count: number;
  skipped_count: number;
  total_count: number;
  cursor: string;
}

interface ServerIMessageThread {
  thread_id: string;
  source_id: string;
  native_id: string;
  service: string;
  is_group: boolean;
  participant_count: number;
  participant_hash: string;
  last_message_ms: number | null;
  unread_count: number | null;
  created_ms: number;
  updated_ms: number;
  deleted: boolean;
  origin_device_id: string;
}

interface ServerSentIMessage {
  sent_id: string;
  source_id: string;
  thread_id: string;
  native_id: string | null;
  idempotency_key: string;
  body_hash: string;
  sent_ms: number;
  effect_id: string;
  policy_version: string;
  created_ms: number;
  origin_device_id: string;
  replayed?: boolean;
}

function toIMessageThread(server: ServerIMessageThread): IMessageThread {
  return {
    threadId: server.thread_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    service: server.service,
    isGroup: server.is_group,
    participantCount: server.participant_count,
    participantHash: server.participant_hash,
    lastMessageMs: server.last_message_ms,
    unreadCount: server.unread_count,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted,
    originDeviceId: server.origin_device_id,
  };
}

function toSentIMessage(server: ServerSentIMessage): SentIMessage {
  return {
    sentId: server.sent_id,
    sourceId: server.source_id,
    threadId: server.thread_id,
    nativeId: server.native_id,
    idempotencyKey: server.idempotency_key,
    bodyHash: server.body_hash,
    sentMs: server.sent_ms,
    effectId: server.effect_id,
    policyVersion: server.policy_version,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
    replayed: server.replayed,
  };
}

export class InvokeIMessageClient implements IMessageGateway {
  async syncImessage(): Promise<IMessageSyncResult> {
    const result = await invoke<ServerIMessageSyncResult>("imessage_sync");
    return {
      syncedCount: result.synced_count,
      skippedCount: result.skipped_count,
      totalCount: result.total_count,
      cursor: result.cursor,
    };
  }

  async threads(limit: number | null, offset: number | null): Promise<IMessageThread[]> {
    const threads = await invoke<ServerIMessageThread[]>("imessage_list", {
      limit,
      offset,
    });
    return threads.map(toIMessageThread);
  }

  async thread(threadId: string): Promise<IMessageThread> {
    const thread = await invoke<ServerIMessageThread>("imessage_get", {
      thread_id: threadId,
    });
    return toIMessageThread(thread);
  }

  // MCL-4: send one plain-text message to one
  // already-synced thread, through the digest-bound
  // approval flow (approval_request -> decide -> send).
  async sendImessage(input: SendIMessageInput): Promise<SentIMessage> {
    const sent = await invoke<ServerSentIMessage>("imessage_send", {
      thread_id: input.threadId,
      body: input.body,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId,
    });
    return toSentIMessage(sent);
  }

  async sent(): Promise<SentIMessage[]> {
    const rows = await invoke<ServerSentIMessage[]>("imessage_sent_list");
    return rows.map(toSentIMessage);
  }

  async sentImessage(sentId: string): Promise<SentIMessage> {
    const row = await invoke<ServerSentIMessage>("imessage_sent_get", {
      sent_id: sentId,
    });
    return toSentIMessage(row);
  }
}
