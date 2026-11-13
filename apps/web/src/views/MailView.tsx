// Mail view: email read surface, batch 1 (Task 22,
// option A) + D1-6a manual incremental sync + D1-6b-1
// real send + D1-6b-3 delete. Threads list -> thread
// messages -> message reading -> attachment download.
// Send goes through the digest-bound approval flow
// (approval_request -> decide -> send); delete goes
// through the same flow (approval_request -> decide ->
// delete) and is Trash only — never permanently
// deleted.
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
// "已发送" means Mail.app accepted the message; it
// never promises the recipient received it.

import { useEffect, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
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
} from "../domain/email_client";
import type { LoopGateway } from "../domain/loop_client";

// P2 摘要为草稿入口：直接调 loop，不走 Workbench 对话。循环
// 内读邮件（email.threads / email.message，只读工具）后用
// fs.write 写草稿；写草稿的审批在 Loops 页的等待审批 UI 里
// 批，与现有循环流程一致。MailView 的发送与删除走下面的审批流。
const LOOP_MAX_CALLS = 5;
const LOOP_BUDGET_MS = 120000;

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

function newIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `key-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
}

interface PendingSend {
  to: string[];
  subject: string;
  body: string;
  idempotencyKey: string;
  toolCallId: string;}

interface PendingDelete {
  messageId: string;
  nativeId: string;
  subject: string | null;
  toolCallId: string;
}

export function MailView({
  email,
  loops,
  agent,
  onNavigate,
}: {
  email: EmailGateway;
  loops: LoopGateway;
  agent: AgentGateway;
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
  const [sent, setSent] = useState<SentMessage[] | null>(null);
  const [sendResult, setSendResult] = useState<SentMessage | null>(null);
  const [synced, setSynced] = useState<SyncedMessage[] | null>(null);
  const [deleteResult, setDeleteResult] = useState<MailDeleteResult | null>(null);
  const [pendingDelete, setPendingDelete] = useState<PendingDelete | null>(null);
  const [toInput, setToInput] = useState("");
  const [subjectInput, setSubjectInput] = useState("");
  const [bodyInput, setBodyInput] = useState("");
  const [pendingSend, setPendingSend] = useState<PendingSend | null>(null);

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      setMailbox(await email.mailbox());
      setSetup(await email.mailboxSetup());
      setThreads(await email.threads(50, null));
      setSent(await email.listSent());
      setSynced(await email.listSynced());
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

  // D1-6b-1 send flow: request a digest-bound approval
  // for this exact message, then decide in this view,
  // then send with the decided approval id. The pending
  // copy is frozen at request time: editing the form
  // afterwards cannot change what the approval bound.
  async function requestSendApproval() {
    const to = toInput.split(",").map((s) => s.trim()).filter((s) => s !== "");
    if (to.length === 0 || subjectInput === "" || bodyInput === "") {
      setError("收件人、主题、正文都不能为空。");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const request = await agent.requestApproval("mail.send", {
        to: JSON.stringify(to),
        subject: subjectInput,
        body: bodyInput,
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingSend({
        to,
        subject: subjectInput,
        body: bodyInput,
        idempotencyKey: newIdempotencyKey(),
        toolCallId: request.tool_call_id,
      });
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function approveAndSend() {
    if (pendingSend === null) return;
    setBusy(true);
    setError(null);
    try {
      await agent.decideApproval(pendingSend.toolCallId, "approve");
      const input: SendMailInput = {
        to: pendingSend.to,
        subject: pendingSend.subject,
        body: pendingSend.body,
        idempotencyKey: pendingSend.idempotencyKey,
        toolCallId: pendingSend.toolCallId,
      };
      const row = await email.sendMail(input);
      setSendResult(row);
      setPendingSend(null);
      setToInput("");
      setSubjectInput("");
      setBodyInput("");
      setSent(await email.listSent());
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function rejectSend() {
    if (pendingSend === null) return;
    setBusy(true);
    setError(null);
    try {
      await agent.decideApproval(pendingSend.toolCallId, "reject");
      setPendingSend(null);
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  // D1-6b-3 delete flow: same approval shape as send —
  // request a digest-bound approval for this native id,
  // decide in this view, then delete with the decided
  // approval id. Trash only, never permanently deleted
  // (Q-D1-6b-12); the approval binds the exact message.
  async function requestDeleteApproval(row: SyncedMessage) {
    setBusy(true);
    setError(null);
    try {
      const request = await agent.requestApproval("mail.delete", {
        native_id: row.nativeId,
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingDelete({
        messageId: row.messageId,
        nativeId: row.nativeId,
        subject: row.subject,
        toolCallId: request.tool_call_id,
      });
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function approveAndDelete() {
    if (pendingDelete === null) return;
    setBusy(true);
    setError(null);
    try {
      await agent.decideApproval(pendingDelete.toolCallId, "approve");
      const result = await email.deleteMessage(pendingDelete.messageId, pendingDelete.toolCallId);
      setDeleteResult(result);
      setPendingDelete(null);
      setSynced(await email.listSynced());
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function rejectDelete() {
    if (pendingDelete === null) return;
    setBusy(true);
    setError(null);
    try {
      await agent.decideApproval(pendingDelete.toolCallId, "reject");
      setPendingDelete(null);
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
        线程列表、消息阅读、附件下载；发送与删除走审批流（先请求审批、通过后再执行）。
        删除只移入废纸篓，永不彻底删除。手动同步把收件箱前 50 封写入本地 SQLite（只存正文哈希，不存正文）；自动同步等 A1 定时器。
        打包的 Mac 应用读真实 Mail.app 数据（AppleScript 问 Mail.app）；其他平台返回 501。
        线程按规范化主题分组（Mail 的 AppleScript 没有 thread id），线程 id 是本地定义。
        “已发送”指 Mail.app 已接受这封邮件，不承诺对方收到。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {syncResult !== null && (
        <p className="note">
          已同步：新增 {syncResult.syncedCount} 封，跳过 {syncResult.skippedCount} 封，
          本地共 {syncResult.totalCount} 封。
        </p>
      )}
      {sendResult !== null && (
        <p className="note">
          已发送：{sendResult.sentId}（{sendResult.subject}）——Mail.app 已接受，不承诺对方收到。
        </p>
      )}
      {deleteResult !== null && (
        <p className="note">
          已移入废纸篓：{deleteResult.messageId ?? deleteResult.nativeId}
          （{deleteResult.replayed ? "已删除过，未重复执行" : "本次删除"}）——只移入废纸篓，未彻底删除。
        </p>
      )}
      <h3>发送邮件</h3>
      <div>
        <label>
          收件人（多个用逗号分隔）：
          <input
            type="text"
            value={toInput}
            onChange={(e) => setToInput(e.target.value)}
            disabled={busy || pendingSend !== null}
          />
        </label>{" "}
        <label>
          主题：
          <input
            type="text"
            value={subjectInput}
            onChange={(e) => setSubjectInput(e.target.value)}
            disabled={busy || pendingSend !== null}
          />
        </label>
        <div>
          <label>
            正文（纯文本，不含附件）：
            <textarea
              value={bodyInput}
              onChange={(e) => setBodyInput(e.target.value)}
              disabled={busy || pendingSend !== null}
            />
          </label>
        </div>
        <div className="row">
          <button type="button" onClick={() => void requestSendApproval()} disabled={busy || pendingSend !== null}>
            请求审批
          </button>
        </div>
        {pendingSend !== null && (
          <div>
            <p>
              待审批：{pendingSend.to.join("、")} · {pendingSend.subject}（
              {pendingSend.toolCallId}）
            </p>
            <div className="row">
              <button type="button" onClick={() => void approveAndSend()} disabled={busy}>
                通过并发送
              </button>{" "}
              <button type="button" onClick={() => void rejectSend()} disabled={busy}>
                拒绝
              </button>
            </div>
          </div>
        )}
      </div>
      <h3>已发送</h3>
      {(sent ?? []).length === 0 ? (
        <p className="empty">无已发送记录。</p>
      ) : (
        <ul>
          {(sent ?? []).map((s) => (
            <li key={s.sentId}>
              {s.sentId} · {s.subject} · {s.toJson} ·{" "}
              {new Date(s.sentMs).toLocaleString()}
            </li>
          ))}
        </ul>
      )}
      <h3>已同步（可删除）</h3>
      {(synced ?? []).length === 0 ? (
        <p className="empty">无已同步邮件。</p>
      ) : (
        <ul>
          {(synced ?? []).map((row) => (
            <li key={row.messageId}>
              {row.messageId} · {row.subject ?? "（无主题）"} · {row.sender ?? ""} ·{" "}
              {new Date(row.dateMs).toLocaleString()}{" "}
              <button
                type="button"
                onClick={() => void requestDeleteApproval(row)}
                disabled={busy || pendingDelete !== null}
              >
                移入废纸篓
              </button>
            </li>
          ))}
        </ul>
      )}
      {pendingDelete !== null && (
        <div>
          <p>
            待审批删除：{pendingDelete.messageId} · {pendingDelete.subject ?? "（无主题）"}（
            {pendingDelete.toolCallId}）——只移入废纸篓，永不彻底删除。
          </p>
          <div className="row">
            <button type="button" onClick={() => void approveAndDelete()} disabled={busy}>
              通过并移入废纸篓
            </button>{" "}
            <button type="button" onClick={() => void rejectDelete()} disabled={busy}>
              拒绝
            </button>
          </div>
        </div>
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
