// Mail view: email read surface, batch 1 (Task 22,
// option A). Threads list -> thread messages -> message
// reading -> attachment download. Reads only: no send,
// no delete, no sync cache.
//
// Honest copy (shown in the view): on the packaged Mac
// app this reads real Mail.app data via AppleScript;
// other platforms answer 501. Threading groups messages
// by normalized subject (Mail's AppleScript dictionary
// exposes no thread id), so thread ids are a local
// definition, not the original product's thread ids.

import { useEffect, useState } from "react";
import type {
  EmailGateway,
  MailboxInfo,
  MailboxSetup,
  MessageView,
  ThreadView,
} from "../domain/email_client";

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

export function MailView({ email }: { email: EmailGateway }) {
  const [mailbox, setMailbox] = useState<MailboxInfo | null>(null);
  const [setup, setSetup] = useState<MailboxSetup | null>(null);
  const [threads, setThreads] = useState<ThreadView[] | null>(null);
  const [thread, setThread] = useState<ThreadView | null>(null);
  const [message, setMessage] = useState<MessageView | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      setMailbox(await email.mailbox());
      setSetup(await email.mailboxSetup());
      setThreads(await email.threads(50, null));
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function openThread(threadId: string) {
    setBusy(true);
    setError(null);
    try {
      setThread(await email.thread(threadId));
      setMessage(null);
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function openMessage(messageId: string) {
    setBusy(true);
    setError(null);
    try {
      setMessage(await email.message(messageId));
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function downloadAttachment(messageId: string, index: number, name: string, contentType: string) {
    setBusy(true);
    setError(null);
    try {
      const bytes = await email.attachment(messageId, index);
      const blob = new Blob([new Uint8Array(bytes)], { type: contentType });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = name;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="view">
      <h2>Mail</h2>
      <p className="note">
        只读：线程列表、消息阅读、附件下载；不发送、不删除、不做增量同步缓存。
        打包的 Mac 应用读真实 Mail.app 数据（AppleScript 问 Mail.app）；其他平台返回 501。
        线程按规范化主题分组（Mail 的 AppleScript 没有 thread id），线程 id 是本地定义。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {mailbox === null ? (
        <p className="empty">邮箱状态未知（状态查询失败）。</p>
      ) : (
        <div>
          <p>
            邮箱：
            {mailbox.configured
              ? `${mailbox.name ?? ""} ${mailbox.address ?? ""}（${mailbox.source}）`
              : `未配置（${mailbox.source}）`}
          </p>
          {setup !== null && !setup.configured && (
            <ul>
              {setup.steps.map((step) => (
                <li key={step}>{step}</li>
              ))}
            </ul>
          )}
        </div>
      )}
      <div className="row">
        <button type="button" onClick={() => void refresh()} disabled={busy}>
          刷新
        </button>
      </div>
      <h3>线程</h3>
      {(threads ?? []).length === 0 ? (
        <p className="empty">无线程。</p>
      ) : (
        <ul>
          {(threads ?? []).map((t) => (
            <li key={t.threadId}>
              <button type="button" onClick={() => void openThread(t.threadId)} disabled={busy}>
                {t.subject}（{t.messageCount} 封）
              </button>
              <p>{t.snippet}</p>
            </li>
          ))}
        </ul>
      )}
      {thread !== null && (
        <div>
          <h3>线程消息</h3>
          <p>
            {thread.subject} · {thread.messageCount} 封
          </p>
          <ul>
            {thread.messageIds.map((id) => (
              <li key={id}>
                <button type="button" onClick={() => void openMessage(id)} disabled={busy}>
                  {id}
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      {message !== null && (
        <div>
          <h3>{message.subject}</h3>
          <p>
            发件人：{message.from} · 收件人：{message.to.join("、")} · {message.date}
          </p>
          <pre>{message.bodyText}</pre>
          {message.attachments.length > 0 && (
            <div>
              <p>附件：</p>
              <ul>
                {message.attachments.map((a) => (
                  <li key={a.index}>
                    {a.name}（{a.contentType}）
                    <button
                      type="button"
                      onClick={() =>
                        void downloadAttachment(message.messageId, a.index, a.name, a.contentType)
                      }
                      disabled={busy}
                    >
                      下载
                    </button>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
