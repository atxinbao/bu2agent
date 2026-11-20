// Messages view: iMessage threads surface (MCL-1,
// threads-only) + send (MCL-4). Threads list -> thread
// detail (row fields only). No message detail view —
// message history is not readable: the MCL-0 probe
// proved the AppleScript dictionary has no message
// element (a real-session read errors -1700), and
// chat.db is never a fact source. The sync button is a
// manual trigger: it records the Messages.app thread
// list into local SQLite (ids / hashes / counts only,
// never handles or names); there is no auto-trigger.
//
// Send goes through the digest-bound approval flow
// (approval_request -> decide -> send, the 6b-1 mail
// shape) to one already-synced thread only — no new
// threads (Q-MCL-20). "已发送" means Messages.app
// accepted the message; it never promises the other
// side received it (Q-MCL-21).
//
// Honest copy (shown in the view): on the packaged Mac
// app this reads real Messages.app data via AppleScript;
// other platforms answer 501. Thread ids are local
// (`thread-N`), not the original product's thread ids;
// the participant hash identifies the participant set
// without naming anyone.

import { useEffect, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type {
  IMessageGateway,
  IMessageSyncResult,
  IMessageThread,
  SendIMessageInput,
  SentIMessage,
} from "../domain/imessage_client";

interface MessagesViewProps {
  imessage: IMessageGateway;
  agent: AgentGateway;
}

interface PendingSend {
  threadId: string;
  body: string;
  idempotencyKey: string;
  toolCallId: string;
}

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

function newIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `key-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
}

function fmtTime(ms: number): string {
  return new Date(ms).toLocaleString();
}

export default function MessagesView({ imessage, agent }: MessagesViewProps) {
  const [threads, setThreads] = useState<IMessageThread[]>([]);
  const [selected, setSelected] = useState<IMessageThread | null>(null);
  const [syncResult, setSyncResult] = useState<IMessageSyncResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sent, setSent] = useState<SentIMessage[]>([]);
  const [sendResult, setSendResult] = useState<SentIMessage | null>(null);
  const [bodyInput, setBodyInput] = useState("");
  const [pendingSend, setPendingSend] = useState<PendingSend | null>(null);

  async function loadThreads() {
    try {
      setThreads(await imessage.threads(50, 0));
    } catch (e) {
      setError(errText(e));
    }
  }

  async function loadSent() {
    try {
      setSent(await imessage.sent());
    } catch (e) {
      setError(errText(e));
    }
  }

  useEffect(() => {
    void loadThreads();
    void loadSent();
    // The gateway is stable for the view's lifetime.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function syncImessage() {
    setBusy(true);
    setError(null);
    try {
      const result = await imessage.syncImessage();
      setSyncResult(result);
      await loadThreads();
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  // MCL-4 send flow: request a digest-bound approval
  // for this exact message in this exact thread, then
  // decide in this view, then send with the decided
  // approval id. The pending copy is frozen at request
  // time: editing the form afterwards cannot change
  // what the approval bound.
  async function requestSendApproval() {
    if (selected === null || bodyInput === "") {
      setError("请先选一个会话，并输入正文。");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const request = await agent.requestApproval("imessage.send", {
        thread_id: selected.threadId,
        body: bodyInput,
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingSend({
        threadId: selected.threadId,
        body: bodyInput,
        idempotencyKey: newIdempotencyKey(),
        toolCallId: request.tool_call_id,
      });
    } catch (e) {
      setError(errText(e));
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
      const input: SendIMessageInput = {
        threadId: pendingSend.threadId,
        body: pendingSend.body,
        idempotencyKey: pendingSend.idempotencyKey,
        toolCallId: pendingSend.toolCallId,
      };
      const row = await imessage.sendImessage(input);
      setSendResult(row);
      setPendingSend(null);
      setBodyInput("");
      await loadSent();
    } catch (e) {
      setError(errText(e));
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
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="view">
      <h2>信息</h2>
      <p className="note">
        只读会话列表：会话标识、服务类型与参与者数量/哈希，不含消息内容。打包的 Mac
        应用读真实 Messages.app 数据，其他平台返回 501。消息历史不可读。
      </p>
      <div className="row">
        <button type="button" onClick={() => void syncImessage()} disabled={busy}>
          同步会话
        </button>
        <button type="button" onClick={() => void loadThreads()} disabled={busy}>
          刷新
        </button>
      </div>
      {syncResult !== null && (
        <p className="note">
          已同步 {syncResult.syncedCount} 条，跳过 {syncResult.skippedCount} 条，共{" "}
          {syncResult.totalCount} 条。
        </p>
      )}
      {error !== null && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {threads.length === 0 && <p className="empty">暂无已同步的会话。</p>}
      <ul className="entries">
        {threads.map((thread) => (
          <li key={thread.threadId}>
            <button type="button" onClick={() => setSelected(thread)}>
              {thread.threadId} · {thread.service} · {thread.participantCount} 位参与者
              {thread.isGroup ? " · 群组" : ""}
              {thread.deleted ? " · 已删除" : ""}
            </button>
          </li>
        ))}
      </ul>
      {selected !== null && (
        <div>
          <h3>会话 {selected.threadId}</h3>
          <p className="note">
            会话详情只有行字段，没有消息内容——消息历史不可读。
          </p>
          <pre className="result">
            {[
              `thread_id: ${selected.threadId}`,
              `native_id: ${selected.nativeId}`,
              `service: ${selected.service}`,
              `is_group: ${selected.isGroup ? "是" : "否"}`,
              `participant_count: ${selected.participantCount}`,
              `participant_hash: ${selected.participantHash}`,
              `created: ${fmtTime(selected.createdMs)}`,
              `updated: ${fmtTime(selected.updatedMs)}`,
              `deleted: ${selected.deleted ? "是" : "否"}`,
            ].join("\n")}
          </pre>
          <h3>发送消息</h3>
          <p className="note">
            只发给这个已同步的会话，不新建会话。发送前要审批：审批绑定这条正文与这个会话。
            「已发送」表示 Messages.app 已接受，不代表对方收到。
          </p>
          <textarea
            value={bodyInput}
            onChange={(e) => setBodyInput(e.target.value)}
            placeholder="输入要发送的正文"
            rows={3}
            disabled={busy || pendingSend !== null}
          />
          <div className="row">
            <button
              type="button"
              onClick={() => void requestSendApproval()}
              disabled={busy || pendingSend !== null || bodyInput === ""}
            >
              请求发送审批
            </button>
          </div>
          {pendingSend !== null && (
            <div>
              <p className="note">
                待审批发送到 {pendingSend.threadId}：
              </p>
              <pre className="result">{pendingSend.body}</pre>
              <div className="row">
                <button type="button" onClick={() => void approveAndSend()} disabled={busy}>
                  批准并发送
                </button>
                <button type="button" onClick={() => void rejectSend()} disabled={busy}>
                  拒绝
                </button>
              </div>
            </div>
          )}
          {sendResult !== null && (
            <p className="note">
              已发送：{sendResult.sentId}（{fmtTime(sendResult.sentMs)}
              {sendResult.replayed ? "，重放" : ""}）。Messages.app 已接受，不代表对方收到。
            </p>
          )}
        </div>
      )}
      {sent.length > 0 && (
        <div>
          <h3>已发送</h3>
          <ul className="entries">
            {sent.map((row) => (
              <li key={row.sentId}>
                {row.sentId} · {row.threadId} · {fmtTime(row.sentMs)}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
