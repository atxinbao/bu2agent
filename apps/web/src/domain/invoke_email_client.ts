// Invoke email client: packaged app -> Tauri shell email_*
// commands (Task 22, batch 1, option A). Param names are
// snake_case to match the shell's rename_all; the invoke
// transport returns the Rust structs' snake_case fields, so
// the mapping lives here, not in the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  EmailGateway,
  MailSyncResult,
  MailboxInfo,
  MailboxSetup,
  MessageView,
  SendMailInput,
  SentMessage,
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
}
