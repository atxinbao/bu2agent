// Invoke email client: packaged app -> Tauri shell email_*
// commands (Task 22, batch 1, option A). Param names are
// snake_case to match the shell's rename_all; the invoke
// transport returns the Rust structs' snake_case fields, so
// the mapping lives here, not in the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  EmailGateway,
  MailDeleteResult,
  MailSyncResult,
  MailboxInfo,
  MailboxSetup,
  MessageView,
  SendMailInput,
  SentMessage,
  SyncedMessage,
  ThreadView,
} from "./email_client";

interface ServerMailSyncResult {
  synced_count: number;
  skipped_count: number;
  total_count: number;
  cursor: string;
}

interface ServerThreadView {
  thread_id: string;
  subject: string;
  message_ids: string[];
  message_count: number;
  snippet: string;
}

interface ServerAttachmentMeta {
  index: number;
  name: string;
  content_type: string;
  size: number;
}

interface ServerMessageView {
  message_id: string;
  from: string;
  to: string[];
  date: string;
  subject: string;
  body_text: string;
  attachments: ServerAttachmentMeta[];
}

function toThreadView(server: ServerThreadView): ThreadView {
  return {
    threadId: server.thread_id,
    subject: server.subject,
    messageIds: server.message_ids,
    messageCount: server.message_count,
    snippet: server.snippet,
  };
}

function toMessageView(server: ServerMessageView): MessageView {
  return {
    messageId: server.message_id,
    from: server.from,
    to: server.to,
    date: server.date,
    subject: server.subject,
    bodyText: server.body_text,
    attachments: server.attachments.map((a) => ({
      index: a.index,
      name: a.name,
      contentType: a.content_type,
      size: a.size,
    })),
  };
}

interface ServerSentMessage {
  sent_id: string;
  source_id: string;
  native_id: string | null;
  idempotency_key: string;
  to_json: string;
  subject: string;
  body_hash: string;
  sent_ms: number;
  effect_id: string;
  policy_version: string;
  created_ms: number;
  origin_device_id: string;
}

function toSentMessage(server: ServerSentMessage): SentMessage {
  return {
    sentId: server.sent_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    idempotencyKey: server.idempotency_key,
    toJson: server.to_json,
    subject: server.subject,
    bodyHash: server.body_hash,
    sentMs: server.sent_ms,
    effectId: server.effect_id,
    policyVersion: server.policy_version,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
  };
}

interface ServerSyncedMessage {
  message_id: string;
  source_id: string;
  native_id: string;
  thread_id: string;
  subject: string;
  sender: string;
  date_ms: number;
  body_hash: string;
  created_ms: number;
  origin_device_id: string;
  deleted: boolean;
  updated_ms: number;
}

function toSyncedMessage(server: ServerSyncedMessage): SyncedMessage {
  return {
    messageId: server.message_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    threadId: server.thread_id,
    subject: server.subject,
    sender: server.sender,
    dateMs: server.date_ms,
    bodyHash: server.body_hash,
    createdMs: server.created_ms,
    originDeviceId: server.origin_device_id,
    deleted: server.deleted,
    updatedMs: server.updated_ms,
  };
}

interface ServerMailDeleteResponse {
  message_id: string | null;
  source_id: string;
  native_id: string;
  deleted: boolean;
  effect_id: string | null;
  replayed: boolean;
}

function toMailDeleteResult(server: ServerMailDeleteResponse): MailDeleteResult {
  return {
    messageId: server.message_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    deleted: server.deleted,
    effectId: server.effect_id,
    replayed: server.replayed,
  };
}

export class InvokeEmailClient implements EmailGateway {
  mailbox(): Promise<MailboxInfo> {
    return invoke("email_mailbox");
  }

  async syncMail(): Promise<MailSyncResult> {
    const result = await invoke<ServerMailSyncResult>("mail_sync");
    return {
      syncedCount: result.synced_count,
      skippedCount: result.skipped_count,
      totalCount: result.total_count,
      cursor: result.cursor,
    };
  }

  mailboxSetup(): Promise<MailboxSetup> {
    return invoke("email_mailbox_setup");
  }

  async threads(limit: number | null, offset: number | null): Promise<ThreadView[]> {
    const threads = await invoke<ServerThreadView[]>("email_threads_list", {
      limit,
      offset,
    });
    return threads.map(toThreadView);
  }

  async thread(threadId: string): Promise<ThreadView> {
    const thread = await invoke<ServerThreadView>("email_thread_get", {
      thread_id: threadId,
    });
    return toThreadView(thread);
  }

  async message(messageId: string): Promise<MessageView> {
    const message = await invoke<ServerMessageView>("email_message_get", {
      message_id: messageId,
    });
    return toMessageView(message);
  }

  attachment(messageId: string, index: number): Promise<number[]> {
    return invoke("email_message_attachment", { message_id: messageId, index });
  }

  async sendMail(input: SendMailInput): Promise<SentMessage> {
    const sent = await invoke<ServerSentMessage>("mail_send", {
      to: input.to,
      subject: input.subject,
      body: input.body,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId ?? null,
    });
    return toSentMessage(sent);
  }

  async listSent(): Promise<SentMessage[]> {
    const rows = await invoke<ServerSentMessage[]>("mail_sent_list");
    return rows.map(toSentMessage);
  }

  async sent(sentId: string): Promise<SentMessage> {
    const row = await invoke<ServerSentMessage>("mail_sent_get", { sent_id: sentId });
    return toSentMessage(row);
  }

  async deleteMessage(messageId: string, toolCallId: string): Promise<MailDeleteResult> {
    // D1-6b-3: Trash only, never purge (Q-D1-6b-12).
    const result = await invoke<ServerMailDeleteResponse>("mail_delete", {
      message_id: messageId,
      tool_call_id: toolCallId,
    });
    return toMailDeleteResult(result);
  }

  async listSynced(): Promise<SyncedMessage[]> {
    const rows = await invoke<ServerSyncedMessage[]>("mail_synced_list");
    return rows.map(toSyncedMessage);
  }

  async synced(messageId: string): Promise<SyncedMessage> {
    const row = await invoke<ServerSyncedMessage>("mail_synced_get", {
      message_id: messageId,
    });
    return toSyncedMessage(row);
  }
}
