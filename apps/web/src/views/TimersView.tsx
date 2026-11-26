import { useEffect, useState } from "react";
import type { Timer, TimersGateway } from "../domain/timers_client";

// AO-2 定时器（plan §4）：创建、列表、撤销三件套。
// 定时器是事实行，不是 Agent 运行：创建只记录何时
// 到期；到期后 sweeper 经 AO-1 的 ingest 路径写事件，
// 定时器本身永不直接启动 Agent。本视图不新增任何语义。

interface TimersViewProps {
  timers: TimersGateway | null;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function fmtMs(ms: number): string {
  return new Date(ms).toLocaleString();
}

export function TimersView({ timers }: TimersViewProps) {
  const [rows, setRows] = useState<Timer[]>([]);
  const [kind, setKind] = useState("follow_up");
  const [fireAt, setFireAt] = useState("");
  const [intervalMs, setIntervalMs] = useState("60000");
  const [sessionId, setSessionId] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function refresh() {
    if (!timers) return;
    try {
      setRows(await timers.listTimers());
    } catch (e) {
      setError(`定时器列表读取失败：${errText(e)}`);
    }
  }

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function createTimer() {
    if (!timers) return;
    const nextFireMs = fireAt ? new Date(fireAt).getTime() : NaN;
    if (!Number.isFinite(nextFireMs)) {
      setError("请先选择到期时间");
      return;
    }
    const interval = Number(intervalMs);
    if (kind === "schedule" && (!Number.isInteger(interval) || interval <= 0)) {
      setError("周期定时器需要正整数间隔毫秒数");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const created = await timers.createTimer({
        kind,
        nextFireMs,
        recurrenceJson: kind === "schedule" ? { intervalMs: interval } : null,
        payloadSummaryJson: null,
        sessionId: sessionId.trim() || null,
      });
      setMessage(`已创建 ${created.timerId}（${created.status}）`);
      setFireAt("");
      setSessionId("");
      await refresh();
    } catch (e) {
      setError(`创建失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function revokeTimer(timerId: string) {
    if (!timers) return;
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const revoked = await timers.deleteTimer(timerId);
      setMessage(`${revoked.timerId} 已撤销（${revoked.status}）`);
      await refresh();
    } catch (e) {
      setError(`撤销失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h2>定时器</h2>
      <p>
        到期后写一个事件进收件箱（AO-1），由唤醒驱动接手；定时器本身不启动 Agent。
        过去的时间视为已到期，下一个 tick 补发一次。
      </p>
      <div>
        <label>
          类型
          <select value={kind} onChange={(e) => setKind(e.target.value)}>
            <option value="follow_up">follow_up（一次性）</option>
            <option value="schedule">schedule（周期）</option>
          </select>
        </label>
        <label>
          到期时间
          <input
            type="datetime-local"
            value={fireAt}
            onChange={(e) => setFireAt(e.target.value)}
          />
        </label>
        {kind === "schedule" && (
          <label>
            间隔（毫秒）
            <input
              type="number"
              value={intervalMs}
              onChange={(e) => setIntervalMs(e.target.value)}
            />
          </label>
        )}
        <label>
          session（可选）
          <input value={sessionId} onChange={(e) => setSessionId(e.target.value)} />
        </label>
        <button type="button" disabled={busy || !timers} onClick={createTimer}>
          创建定时器
        </button>
        <button type="button" disabled={busy || !timers} onClick={refresh}>
          刷新
        </button>
      </div>
      {message && <p>{message}</p>}
      {error && <p>{error}</p>}
      {!timers && <p>定时器客户端不可用。</p>}
      <table>
        <thead>
          <tr>
            <th>定时器</th>
            <th>类型</th>
            <th>状态</th>
            <th>下次到期</th>
            <th>创建时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.timerId}>
              <td>{row.timerId}</td>
              <td>{row.kind}</td>
              <td>{row.status}</td>
              <td>{fmtMs(row.nextFireMs)}</td>
              <td>{fmtMs(row.createdMs)}</td>
              <td>
                {row.status === "active" && (
                  <button
                    type="button"
                    disabled={busy}
                    onClick={() => revokeTimer(row.timerId)}
                  >
                    撤销
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length === 0 && <p>暂无定时器。</p>}
    </div>
  );
}
