import { useEffect, useState } from "react";
import type { MandateRow, MandatesGateway } from "../domain/mandates_client";

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

interface MandatesSectionProps {
  mandates: MandatesGateway | null;
  taskId?: string | null;
  executionId?: string | null;
}

// FU-3C (Q-R0-01): the `/mandates` family had no production
// client; this section renders the mandate list for one task
// or one execution (the gateway list route filters by either),
// read-only. Expanding a row re-reads it via get() and shows
// the full allowed_json. Shared by ExecutionDetail (per
// execution) and TasksView (per task): a mandate can bind a
// task with no execution, so the two filters are not the same
// list.
export function MandatesSection({
  mandates,
  taskId = null,
  executionId = null,
}: MandatesSectionProps) {
  const [rows, setRows] = useState<MandateRow[]>([]);
  const [error, setError] = useState("");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [detail, setDetail] = useState<MandateRow | null>(null);

  async function refresh() {
    if (!mandates) return;
    setError("");
    try {
      setRows(await mandates.list({ taskId, executionId }));
    } catch (e) {
      setError(errText(e));
    }
  }

  useEffect(() => {
    if (!mandates) return;
    setRows([]);
    setDetail(null);
    setExpandedId(null);
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mandates, taskId, executionId]);

  async function expand(row: MandateRow) {
    if (!mandates) return;
    if (expandedId === row.mandateId) {
      setExpandedId(null);
      setDetail(null);
      return;
    }
    setExpandedId(row.mandateId);
    setError("");
    try {
      setDetail(await mandates.get(row.mandateId));
    } catch (e) {
      setError(errText(e));
    }
  }

  return (
    <div>
      <h4>授权范围</h4>
      {!mandates && (
        <p className="note">
          授权范围客户端尚未接入：待 App 接线传入客户端后，这里显示该任务/执行的 Mandate 记录。
        </p>
      )}
      {mandates && (
        <div>
          <button type="button" onClick={() => void refresh()}>
            刷新授权范围
          </button>
          {error !== "" && <p className="error">{error}</p>}
          {rows.length === 0 && <p className="empty">暂无 Mandate 记录。</p>}
          <ul>
            {rows.map((row) => {
              const expanded = expandedId === row.mandateId;
              return (
                <li key={row.mandateId}>
                  <button type="button" onClick={() => void expand(row)}>
                    {row.mandateId} · {row.goal} · {row.status}
                    {` · 策略 ${row.policyVersionId}`}
                    {row.taskId ? ` · 任务 ${row.taskId}` : ""}
                    {row.executionId ? ` · 执行 ${row.executionId}` : ""}
                  </button>
                  {expanded && detail && detail.mandateId === row.mandateId && (
                    <div>
                      <p>目标：{detail.goal}</p>
                      <p>状态：{detail.status}</p>
                      <p>策略版本：{detail.policyVersionId}</p>
                      <p>任务：{detail.taskId ?? "—"}</p>
                      <p>执行：{detail.executionId ?? "—"}</p>
                      <p>
                        创建时间：
                        {new Date(detail.createdMs).toLocaleString()}
                      </p>
                      <p>来源设备：{detail.originDeviceId}</p>
                      <p>允许范围：</p>
                      <pre className="result">{detail.allowedJson}</pre>
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        </div>
      )}
    </div>
  );
}
