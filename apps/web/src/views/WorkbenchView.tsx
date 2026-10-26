import { useEffect, useRef, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type { FileGateway } from "../domain/file_client";
import type { LoopGateway, LoopSnapshot } from "../domain/loop_client";
import { isTerminal } from "../domain/loop_client";
import type {
  SessionGateway,
  SessionRecord,
} from "../domain/session_client";
import type { TaskGateway, TaskRecord } from "../domain/task_client";

// Workbench (Round 3, 3a): one list on the left, a timeline in
// the middle, an on-demand file preview on the right. Navigation
// only: selecting an item never approves or resumes anything,
// and each kind keeps its own state machine (sessions, the
// deterministic task flow, the model loop). The old tabs stay;
// this view is additive navigation, not a replacement.
//
// Depth, stated honestly: sessions are full here (history +
// input + delete). Tasks show their record; drafts and approval
// writes still happen on the Tasks tab. Loops show progress,
// calls, and their pending approval, which can be decided here.

type ItemKind = "session" | "task" | "loop";

interface WorkItem {
  key: string;
  kind: ItemKind;
  id: string;
  title: string;
  status: string;
  // Pending is derived from real states, never rewritten: a loop
  // awaiting approval and a loop needing review are different
  // states, so they get different badges even though both pass
  // the pending filter.
  pending: boolean;
  pendingLabel: string | null;
}

interface Preview {
  path: string;
  text: string;
}

const taskLabels: Record<string, string> = {
  created: "已创建",
  draft_ready: "草稿就绪",
  completed: "已完成",
  rejected: "已拒绝",
  failed: "失败",
};

const loopLabels: Record<string, string> = {
  created: "已创建",
  running: "进行中",
  awaiting_approval: "等待审批",
  completed: "已完成",
  needs_review: "待复核",
  failed: "失败",
  cancelled: "已取消",
  interrupted: "已中断",
};

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

export interface WorkbenchViewProps {
  sessions: SessionGateway;
  tasks: TaskGateway;
  loops: LoopGateway;
  agent: AgentGateway;
  files: FileGateway;
  onNavigate: (tab: "tasks" | "loops" | "files") => void;
}

export function WorkbenchView(props: WorkbenchViewProps) {
  const { sessions, tasks, loops, agent, files, onNavigate } = props;
  const [items, setItems] = useState<WorkItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [pendingOnly, setPendingOnly] = useState(false);
  const [selection, setSelection] = useState<WorkItem | null>(null);
  const [sessionDetail, setSessionDetail] = useState<SessionRecord | null>(null);
  const [taskDetail, setTaskDetail] = useState<TaskRecord | null>(null);
  const [loopDetail, setLoopDetail] = useState<LoopSnapshot | null>(null);
  const [detailError, setDetailError] = useState("");
  const [composer, setComposer] = useState("");
  const [sending, setSending] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [loopBusy, setLoopBusy] = useState(false);
  const selectionToken = useRef(0);

  // 详情查询与显式操作共用同一快照更新列表，避免已执行项仍被标为待审批。
  function updateLoop(snapshot: LoopSnapshot) {
    setLoopDetail(snapshot);
    setItems((previous) => previous.map((item) => item.key === `loop:${snapshot.loop_id}`
      ? {
          ...item,
          status: loopLabels[snapshot.status] ?? snapshot.status,
          pending: snapshot.status === "awaiting_approval" || snapshot.status === "needs_review",
          pendingLabel: snapshot.status === "awaiting_approval" ? "待审批"
            : snapshot.status === "needs_review" ? "待复核" : null,
        }
      : item));
  }

  async function refreshList() {
    setLoading(true);
    setError("");
    try {
      const [sessionList, taskList, loopList] = await Promise.all([
        sessions.list(),
        tasks.list(),
        loops.list(),
      ]);
      const next: WorkItem[] = [];
      for (const s of sessionList) {
        next.push({
          key: `session:${s.sessionId}`,
          kind: "session",
          id: s.sessionId,
          title: s.title || "新会话",
          status: `${s.turnCount} 条消息`,
          pending: false,
          pendingLabel: null,
        });
      }
      for (const t of taskList) {
        next.push({
          key: `task:${t.taskId}`,
          kind: "task",
          id: t.taskId,
          title: t.artifactPath ?? (t.directory || t.taskId),
          status: taskLabels[t.status] ?? t.status,
          pending: false,
          pendingLabel: null,
        });
      }
      for (const l of loopList) {
        next.push({
          key: `loop:${l.loop_id}`,
          kind: "loop",
          id: l.loop_id,
          title: l.loop_id,
          status: loopLabels[l.status] ?? l.status,
          pending: l.status === "awaiting_approval" || l.status === "needs_review",
          pendingLabel:
            l.status === "awaiting_approval"
              ? "待审批"
              : l.status === "needs_review"
                ? "待复核"
                : null,
        });
      }
      setItems(next);
    } catch (e) {
      setError(errText(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void refreshList();
    // Mount only: refreshes come from explicit actions below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function select(item: WorkItem) {
    selectionToken.current += 1;
    setSelection(item);
    setSessionDetail(null);
    setTaskDetail(null);
    setLoopDetail(null);
    setDetailError("");
    setConfirmDelete(false);
    setPreview(null);
    setPreviewError("");
    try {
      if (item.kind === "session") setSessionDetail(await sessions.get(item.id));
      else if (item.kind === "task") setTaskDetail(await tasks.get(item.id));
      else updateLoop(await loops.get(item.id));
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  async function createSession() {
    setError("");
    try {
      const record = await sessions.create(null);
      await refreshList();
      await select({
        key: `session:${record.sessionId}`,
        kind: "session",
        id: record.sessionId,
        title: "新会话",
        status: "0 条消息",
        pending: false,
        pendingLabel: null,
      });
    } catch (e) {
      setError(errText(e));
    }
  }

  async function sendSession() {
    if (!sessionDetail || sending || !composer.trim()) return;
    setSending(true);
    setDetailError("");
    try {
      const updated = await sessions.send(sessionDetail.sessionId, composer);
      setSessionDetail(updated);
      setComposer("");
      await refreshList();
    } catch (e) {
      setDetailError(errText(e));
    } finally {
      setSending(false);
    }
  }

  async function deleteSession() {
    if (!sessionDetail) return;
    setDetailError("");
    try {
      await sessions.delete(sessionDetail.sessionId);
      setSelection(null);
      setSessionDetail(null);
      setConfirmDelete(false);
      await refreshList();
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  async function previewFile(path: string) {
    setPreview(null);
    setPreviewError("");
    try {
      const text = await files.readText(path, 4096, 16384);
      setPreview({ path, text });
    } catch (e) {
      // A path in a record is not proof the file exists: a
      // missing or unreadable file must say so, with its path.
      setPreviewError(`无法预览 ${path}：${errText(e)}`);
    }
  }

  // Loop polling: status reads only, never a step or a decision.
  useEffect(() => {
    if (!loopDetail || isTerminal(loopDetail.status)) return;
    const token = selectionToken.current;
    const timer = setInterval(async () => {
      try {
        const next = await loops.get(loopDetail.loop_id);
        if (selectionToken.current === token) updateLoop(next);
      } catch {
        // A failed poll leaves the last snapshot; the user can
        // refresh explicitly. Never invent a state.
      }
    }, 1500);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loopDetail?.loop_id, loopDetail?.status]);

  async function decideLoop(decision: "approve" | "reject") {
    const view = loopDetail;
    if (!view) return;
    const call = view.calls[view.queue_position];
    if (!call?.approval_id || !call.binding || view.approval_status !== "pending") return;
    setLoopBusy(true);
    setDetailError("");
    try {
      // The loop's own bound approval, never a fresh tool-only one.
      await agent.decideApproval(call.approval_id, decision);
      updateLoop(await loops.resume(view.loop_id, call.approval_id, call.binding));
    } catch (e) {
      setDetailError(errText(e));
    } finally {
      setLoopBusy(false);
    }
  }

  async function resumeLoopDecided() {
    const view = loopDetail;
    if (!view) return;
    const call = view.calls[view.queue_position];
    if (!call?.approval_id || !call.binding) return;
    setLoopBusy(true);
    setDetailError("");
    try {
      updateLoop(await loops.resume(view.loop_id, call.approval_id, call.binding));
    } catch (e) {
      setDetailError(errText(e));
    } finally {
      setLoopBusy(false);
    }
  }

  async function refreshLoop() {
    if (!loopDetail) return;
    setDetailError("");
    try {
      updateLoop(await loops.get(loopDetail.loop_id));
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  async function cancelLoop() {
    if (!loopDetail) return;
    setDetailError("");
    try {
      updateLoop(await loops.cancel(loopDetail.loop_id));
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  function renderGroup(kind: ItemKind, label: string) {
    const group = items.filter((item) => item.kind === kind && (!pendingOnly || item.pending));
    if (group.length === 0) return null;
    return (
      <div key={kind}>
        <h3>{label}</h3>
        <ul className="entries">
          {group.map((item) => (
            <li key={item.key}>
              <button
                type="button"
                className={selection?.key === item.key ? "selected" : ""}
                onClick={() => void select(item)}
              >
                {item.title} · {item.status}
                {item.pendingLabel && <span className="pending-badge"> · {item.pendingLabel}</span>}
              </button>
            </li>
          ))}
        </ul>
      </div>
    );
  }

  return (
    <section className="view workbench-view">
      <h2>工作台</h2>
      <p className="note">
        左侧是会话与任务列表，中间是交流与执行时间线，右侧按需预览文件或产物。切换项目不批准、不续跑任何内容。
      </p>
      <div className="workbench">
        <div className="workbench-list">
          <div className="row">
            <button type="button" onClick={() => void createSession()} disabled={loading}>
              新建会话
            </button>
            <button type="button" onClick={() => void refreshList()} disabled={loading}>
              刷新列表
            </button>
          </div>
          <label>
            <input
              type="checkbox"
              checked={pendingOnly}
              onChange={(e) => setPendingOnly(e.target.checked)}
            />
            仅看待处理
          </label>
          {loading && <p className="empty">加载中…</p>}
          {error && <p className="error" role="alert">{error}</p>}
          {!loading && items.length === 0 && <p className="empty">暂无会话或任务。</p>}
          {renderGroup("session", "会话")}
          {renderGroup("task", "确定性任务")}
          {renderGroup("loop", "模型循环任务")}
        </div>
        <div className="workbench-center">
          {detailError && <p className="error" role="alert">{detailError}</p>}
          {!selection && <p className="empty">从左侧选择一项，或新建一个会话。</p>}
          {selection?.kind === "session" && sessionDetail && (
            <>
              <h3>{sessionDetail.title || "新会话"}</h3>
              <p className="note">整段回复：当前回复一次显示全部内容，不按 token 流式呈现。</p>
              {sessionDetail.turns.length === 0 && <p className="empty">还没有消息。</p>}
              <div className="messages">
                {sessionDetail.turns.map((turn, index) => (
                  <div key={`${turn.createdMs}-${index}`} className={`message ${turn.role}`}>
                    <div className="role">
                      {turn.role === "user" ? "你" : "助手"}
                      {turn.model ? ` · ${turn.model}` : ""}
                    </div>
                    <div className="text">{turn.text}</div>
                  </div>
                ))}
              </div>
              <div className="row">
                <input
                  aria-label="会话输入"
                  type="text"
                  value={composer}
                  onChange={(e) => setComposer(e.target.value)}
                  placeholder="输入消息…"
                  disabled={sending}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") void sendSession();
                  }}
                />
                <button type="button" onClick={() => void sendSession()} disabled={sending || !composer.trim()}>
                  {sending ? "发送中…" : "发送"}
                </button>
              </div>
              {!confirmDelete && (
                <button type="button" onClick={() => setConfirmDelete(true)}>
                  删除会话
                </button>
              )}
              {confirmDelete && (
                <div className="confirm">
                  <span>
                    删除后不再显示，也不再进入模型上下文；但本地记录文件在手动删除前仍保留此前的正文，界面删除不等于磁盘正文已清除。确认删除？
                  </span>
                  <button type="button" onClick={() => void deleteSession()}>
                    确认删除
                  </button>
                  <button type="button" onClick={() => setConfirmDelete(false)}>
                    取消
                  </button>
                </div>
              )}
            </>
          )}
          {selection?.kind === "task" && taskDetail && (
            <>
              <h3>{taskDetail.taskId}</h3>
              <p>
                状态：{taskLabels[taskDetail.status] ?? taskDetail.status} · 目录：
                {taskDetail.directory || "文件根"}
              </p>
              {taskDetail.artifactPath ? (
                <p>
                  产物：{taskDetail.artifactPath}{" "}
                  <button type="button" onClick={() => void previewFile(taskDetail.artifactPath!)}>
                    右侧预览
                  </button>{" "}
                  <button type="button" onClick={() => onNavigate("files")}>
                    在文件页查看
                  </button>
                </p>
              ) : (
                <p className="empty">暂无产物。</p>
              )}
              <p className="note">草稿与审批写入仍在任务页进行；这里只展示任务记录。</p>
              <button type="button" onClick={() => onNavigate("tasks")}>
                在任务页继续
              </button>
            </>
          )}
          {selection?.kind === "loop" && loopDetail && (
            <LoopCenter
              snapshot={loopDetail}
              busy={loopBusy}
              onDecide={(decision) => void decideLoop(decision)}
              onResumeDecided={() => void resumeLoopDecided()}
              onRefresh={() => void refreshLoop()}
              onCancel={() => void cancelLoop()}
              onPreview={(path) => void previewFile(path)}
            />
          )}
        </div>
        {preview && (
          <div className="workbench-preview">
            <h3>预览：{preview.path}</h3>
            <p className="note">只读预览，仅显示前 16 KiB。</p>
            <pre className="result">{preview.text}</pre>
            <button type="button" onClick={() => setPreview(null)}>
              关闭预览
            </button>
          </div>
        )}
        {previewError && <p className="error" role="alert">{previewError}</p>}
      </div>
    </section>
  );
}

interface LoopCenterProps {
  snapshot: LoopSnapshot;
  busy: boolean;
  onDecide: (decision: "approve" | "reject") => void;
  onResumeDecided: () => void;
  onRefresh: () => void;
  onCancel: () => void;
  onPreview: (path: string) => void;
}

// Loop detail inside the workbench: progress, calls, and the
// pending approval, using the loop's own bound approval flow
// (decide, then resume with the call's binding). Starting a
// loop still happens on the Loops tab; this center never
// starts one on selection.
function LoopCenter(props: LoopCenterProps) {
  const { snapshot: current, busy, onDecide, onResumeDecided, onRefresh, onCancel, onPreview } = props;
  const terminal = isTerminal(current.status);
  const waiting = current.status === "awaiting_approval";
  const preview = current.pending_write;
  const call = current.calls[current.queue_position];
  const previewMatches = !!(
    preview &&
    call?.binding &&
    preview.call_id === call.call_id &&
    preview.args_sha256 === call.binding.args_sha256
  );
  const decided = current.approval_status === "approved" || current.approval_status === "rejected";
  return (
    <>
      <h3>{current.loop_id} · {loopLabels[current.status] ?? current.status}</h3>
      <p aria-live="polite">
        状态：{current.status}
        {current.in_flight ? " · 步骤在途" : ""} · 模型：{current.model}
        <br />
        工具提议 {current.proposal_count}/{current.max_calls} · 模型尝试 {current.model_attempts} · 已用
        {(current.used_ms / 1000).toFixed(1)} 秒 · 剩余 {(current.remaining_ms / 1000).toFixed(1)} 秒
      </p>
      {waiting && <p>等待审批不消耗执行预算。审批状态：{current.approval_status ?? "尚未同步，请查询"}。</p>}
      <div className="row">
        {waiting && decided && (
          <button type="button" disabled={busy || current.in_flight} onClick={onResumeDecided}>
            继续已决定的步骤
          </button>
        )}
        <button type="button" onClick={onRefresh}>
          查询状态
        </button>
        {!terminal && (
          <button type="button" onClick={onCancel}>
            取消循环
          </button>
        )}
      </div>
      {waiting && preview && (
        <div className="loop-approval">
          <h3>待写入文件：{preview.path}</h3>
          <p>操作：覆盖整个文件 · {call?.content_digest?.bytes ?? 0} 字节</p>
          <details>
            <summary>查看写入全文</summary>
            <pre className="result">{preview.text}</pre>
          </details>
          <p className="note">调用 #{preview.call_id} · 审批 {call?.approval_id}</p>
          <details>
            <summary>核对绑定摘要</summary>
            <pre className="result">{preview.args_sha256}</pre>
          </details>
          {!previewMatches && <p className="error">预览与绑定不一致，请查询状态。</p>}
          {current.approval_status === "pending" && (
            <div className="row">
              <button type="button" disabled={busy || !previewMatches} onClick={() => onDecide("approve")}>
                批准本次写入并执行
              </button>
              <button type="button" disabled={busy} onClick={() => onDecide("reject")}>
                拒绝本次写入
              </button>
            </div>
          )}
        </div>
      )}
      {!!current.unknown_writes.length && (
        <p className="error">
          未确认写入：{current.unknown_writes.map((i) => `#${i}`).join("、")}。不要重试相同写入；读取只提供观测，不消除未知状态。
        </p>
      )}
      {!!current.unknown_reads.length && <p>未确认读取：{current.unknown_reads.map((i) => `#${i}`).join("、")}。</p>}
      {current.status === "interrupted" && <p>网关重启已中断该循环。只能查看记录，请新建任务；旧审批不能复用。</p>}
      {current.status === "cancelled" && <p>已取消后续推进；已在途的操作可能仍有结果，请查询迟到记录。取消不撤回已经提交的写入。</p>}
      {current.error && <p className="error">结束原因：{current.error}</p>}
      {current.final_text && <pre className="result" aria-label="循环最终说明">{current.final_text}</pre>}
      {terminal && !current.final_text && <p>重启后仅保留审计摘要，最终回答正文不落盘。</p>}
      <h3>调用记录</h3>
      {current.calls.length === 0 && <p>暂无工具提议。</p>}
      <ol className="loop-calls">
        {current.calls.map((item) => (
          <li key={item.call_id}>
            <strong>
              #{item.call_id} {item.tool}
            </strong>{" "}· {item.path ?? "无有效路径"} · {item.status}
            {item.error && <span> · {item.error}</span>}
            {item.duplicate_of !== null && <span> · 复用 #{item.duplicate_of}（未再次执行）</span>}
            {item.path && (
              <button type="button" onClick={() => onPreview(item.path!)}>
                预览文件
              </button>
            )}
          </li>
        ))}
      </ol>
    </>
  );
}
