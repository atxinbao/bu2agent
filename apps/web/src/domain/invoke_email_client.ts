// Invoke email client: packaged app -> Tauri shell email_*
// commands (Task 22, batch 1, option A). Param names are
// snake_case to match the shell's rename_all; the invoke
// transport returns the Rust structs' snake_case fields, so
// the mapping lives here, not in the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  EmailGateway,
  MailboxInfo,
  MailboxSetup,
  MessageView,
  ThreadView,
} from "./email_client";

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

export class InvokeEmailClient implements EmailGateway {
  mailbox(): Promise<MailboxInfo> {
    return invoke("email_mailbox");
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
}
