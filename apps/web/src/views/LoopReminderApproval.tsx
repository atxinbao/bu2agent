import type { LoopCall, LoopSnapshot } from "../domain/loop_client";

interface LoopReminderApprovalProps {
  snapshot: LoopSnapshot;
  call: LoopCall | undefined;
  busy: boolean;
  onDecide: (decision: "approve" | "reject") => void;
}

// 提醒审批与文件审批共用同一绑定校验和决定接口；这里只负责展示提醒语义。
export function LoopReminderApproval({ snapshot, call, busy, onDecide }: LoopReminderApprovalProps) {
  const pending = snapshot.pending_reminder;
  if (!pending) return null;
  const previewMatches = !!(
    call?.binding &&
    pending.call_id === call.call_id &&
    pending.args_sha256 === call.binding.args_sha256
  );
  const target = pending.target_completed ? "已完成" : "未完成";
  const label = pending.title.trim()
    ? `把《${pending.title}》设为${target}`
    : `把提醒 ${pending.reminder_id} 设为${target}（标题暂不可用）`;
  return (
    <div className="loop-approval">
      <h3>待处理提醒：{label}</h3>
      <p className="note">调用 #{pending.call_id} · 审批 {call?.approval_id ?? "未同步"}</p>
      <details>
        <summary>核对绑定摘要</summary>
        <pre className="result">{pending.args_sha256}</pre>
      </details>
      {!previewMatches && <p className="error">预览与绑定不一致，请查询状态。</p>}
      {snapshot.approval_status === "pending" && (
        <div className="row">
          <button type="button" disabled={busy || !previewMatches} onClick={() => onDecide("approve")}>
            批准本次提醒操作并执行
          </button>
          <button type="button" disabled={busy} onClick={() => onDecide("reject")}>
            拒绝本次提醒操作
          </button>
        </div>
      )}
    </div>
  );
}
