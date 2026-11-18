// Messages view: iMessage threads surface (MCL-1,
// threads-only). Threads list -> thread detail (row
// fields only). No message detail view — message
// history is not readable: the MCL-0 probe proved the
// AppleScript dictionary has no message element (a
// real-session read errors -1700), and chat.db is never
// a fact source. The sync button is a manual trigger:
// it records the Messages.app thread list into local
// SQLite (ids / hashes / counts only, never handles or
// names); there is no auto-trigger.
//
// Honest copy (shown in the view): on the packaged Mac
// app this reads real Messages.app data via AppleScript;
// other platforms answer 501. Thread ids are local
// (`thread-N`), not the original product's thread ids;
// the participant hash identifies the participant set
// without naming anyone.

import { useEffect, useState } from "react";
import type {
  IMessageGateway,
  IMessageSyncResult,
  IMessageThread,
} from "../domain/imessage_client";

interface MessagesViewProps {
  imessage: IMessageGateway;
}

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

function fmtTime(ms: number): string {
  return new Date(ms).toLocaleString();
}

export default function MessagesView({ imessage }: MessagesViewProps) {
  const [threads, setThreads] = useState<IMessageThread[]>([]);
  const [selected, setSelected] = useState<IMessageThread | null>(null);
  const [syncResult, setSyncResult] = useState<IMessageSyncResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadThreads() {
    try {
      setThreads(await imessage.threads(50, 0));
    } catch (e) {
      setError(errText(e));
    }
  }

  useEffect(() => {
    void loadThreads();
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
        </div>
      )}
    </div>
  );
}
