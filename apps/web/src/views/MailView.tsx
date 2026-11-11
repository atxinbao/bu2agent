// Mail view: email read surface, batch 1 (Task 22,
// option A) + D1-6a manual incremental sync. Threads
// list -> thread messages -> message reading ->
// attachment download. Reads only: no send, no delete.
// The sync button is a manual trigger: it records the
// inbox first 50 into local SQLite (body hash only,
// never body text); there is no auto-trigger — that
// waits for A1 timers.
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
  MailSyncResult,
  MailboxInfo,
  MailboxSetup,
  MessageView,
  ThreadView,
} from "../domain/email_client";
import type { LoopGateway } from "../domain/loop_client";

// P2 摘要为草稿入口：直接调 loop，不走 Workbench 对话。循环
// 内读邮件（email.threads / email.message，只读工具）后用
// fs.write 写草稿；写草稿的审批在 Loops 页的等待审批 UI 里
// 批，与现有循环流程一致。MailView 本身仍只读：不加发送、
// 不加删除。
const LOOP_MAX_CALLS = 5;
const LOOP_BUDGET_MS = 120000;

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

export function MailView({
  email,
  loops,
  onNavigate,
}: {
  email: EmailGateway;
  loops: LoopGateway;
  onNavigate: (target: "loops") => void;
}) {
  const [mailbox, setMailbox] = useState<MailboxInfo | null>(null);
  const [setup, setSetup] = useState<MailboxSetup | null>(null);
  const [threads, setThreads] = useState<ThreadView[] | null>(null);
  const [thread, setThread] = useState<ThreadView | null>(null);
  const [message, setMessage] = useState<MessageView | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [syncResult, setSyncResult] = useState<MailSyncResult | null>(null);

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

  async function syncMail() {
    setBusy(true);
    setError(null);
    try {
      setSyncResult(await email.syncMail());
      await refresh();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

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

  async function summarizeToDraft() {
    if (message === null || thread === null) return;
    setBusy(true);
    setError(null);
    try {
      const draftPath = `drafts/draft-${Date.now()}.md`;
      const prompt =
        `请把下面这封邮件摘要为一篇草稿笔记。\n` +
        `\n` +
        `邮件 message id：${message.messageId}\n` +
        `线程 thread id：${thread.threadId}\n` +
        `目标草稿路径：${draftPath}\n` +
        `\n` +
        `步骤：\n` +
        `1. 用 email.message 工具读取 message id 为 ${message.messageId} 的邮件（必要时用 email.threads 工具确认它所在的线程）；只读，不做任何发送或删除。\n` +
        `2. 用中文写一份摘要，保留原文要点。\n` +
        `3. 用 fs.write 把摘要写入目标路径 ${draftPath}（覆盖写入该文件）。\n` +
        `4. 草稿开头以纯文本写明引用：message:${message.messageId}、thread:${thread.threadId}、标题：${message.subject}、发件人：${message.from}、日期：${message.date}；引用只写工具结果实际返回过的内容。\n` +
        `5. 若邮件正文在工具结果中被截断，摘要中必须注明“基于已读部分”，不得冒充读过全文。\n`;
      const snapshot = await loops.start(prompt, LOOP_MAX_CALLS, LOOP_BUDGET_MS);
      // Loops 页挂载时按 localStorage 里的上次循环 ID 恢复；
      // 把新循环 ID 存下，跳转后直接跟进该循环的审批。
      try {
        localStorage.setItem("bu2agent.lastLoopId", snapshot.loop_id);
      } catch {
        // 存储不可用不影响跳转，Loops 页可按 ID 手动查询。
      }
      onNavigate("loops");
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
        只读：线程列表、消息阅读、附件下载；不发送、不删除。
        手动同步把收件箱前 50 封写入本地 SQLite（只存正文哈希，不存正文）；自动同步等 A1 定时器。
        打包的 Mac 应用读真实 Mail.app 数据（AppleScript 问 Mail.app）；其他平台返回 501。
        线程按规范化主题分组（Mail 的 AppleScript 没有 thread id），线程 id 是本地定义。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {syncResult !== null && (
        <p className="note">
          已同步：新增 {syncResult.syncedCount} 封，跳过 {syncResult.skippedCount} 封，
          本地共 {syncResult.totalCount} 封。
        </p>
      )}
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
        </button>{" "}
        <button type="button" onClick={() => void syncMail()} disabled={busy}>
          同步邮件
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
          {thread !== null && (
            <div className="row">
              <button type="button" onClick={() => void summarizeToDraft()} disabled={busy}>
                摘要为草稿
              </button>
            </div>
          )}
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
