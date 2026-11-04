import { useEffect, useRef, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type { FileGateway } from "../domain/file_client";
import type { LoopGateway, LoopSnapshot } from "../domain/loop_client";
import { isTerminal } from "../domain/loop_client";
import type {
  SessionGateway,
  SessionRecord,
} from "../domain/session_client";
import type { TaskDraft, TaskGateway, TaskRecord } from "../domain/task_client";
import type {
  ChatRecordsGateway,
  SearchMatch,
} from "../domain/chat_records_client";
import type { StreamEvent, StreamGateway } from "../domain/stream_client";
import { LoopReminderApproval } from "./LoopReminderApproval";
// P0-R R5: Inbox / Execution clients are optional props
// (App.tsx still needs to pass them — outside this task's file
// scope); the view takes them as props like every other client,
// so App.tsx's transport choice applies unchanged.
import type { AssignmentsGateway } from "../domain/assignments_client";
import type { EffectRecord, EffectsGateway } from "../domain/effects_client";
import type { EvidenceGateway, EvidenceRow } from "../domain/evidence_client";
import type {
  ExecutionRecord,
  ExecutionsGateway,
} from "../domain/executions_client";
import type { InboxGateway, InboxRow } from "../domain/inbox_client";
import { ExecutionDetail } from "./ExecutionDetail";

// Workbench (Round 3, 3a+3b): one list on the left, a timeline in
// the middle, an on-demand file preview on the right. Navigation
// only: selecting an item never approves or resumes anything,
// and each kind keeps its own state machine (sessions, the
// deterministic task flow, the model loop).
//
// Since the R1 decision (2026-10-28) this view is the product's
// only chat entry and the default landing view: sessions here
// are the main chat (multi-turn context, local JSONL
// persistence, recovery after restart). The legacy Chat tab
// was removed from the visible navigation; its code and the
// /chat/* interface remain as a compatibility layer only.
//
// Depth, stated honestly: sessions are full here (history +
// input + delete). Tasks show their record; gateway-run drafts
// can be approved or rejected here, manual drafts stay on the
// Tasks tab. Loops show progress,
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
  // Session-only flags; tasks and loops always carry false.
  pinned: boolean;
  archived: boolean;
}

interface Preview {
  path: string;
  text: string;
}

const taskLabels: Record<string, string> = {
  created: "已创建",
  running: "运行中",
  draft_ready: "草稿就绪",
  completed: "已完成",
  rejected: "已拒绝",
  failed: "失败",
  cancelled: "已取消",
  interrupted: "已中断",
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
  chatRecords: ChatRecordsGateway;
  streams: StreamGateway;
  onNavigate: (tab: "tasks" | "loops" | "files") => void;
  inbox?: InboxGateway | null;
  executions?: ExecutionsGateway | null;
  effects?: EffectsGateway | null;
  evidence?: EvidenceGateway | null;
  assignments?: AssignmentsGateway | null;
}

export function WorkbenchView(props: WorkbenchViewProps) {
  const { sessions, tasks, loops, agent, files, chatRecords, streams, onNavigate } = props;
  const inboxClient = props.inbox ?? null;
  const executionsClient = props.executions ?? null;
  const effectsClient = props.effects ?? null;
  const evidenceClient = props.evidence ?? null;
  const assignmentsClient = props.assignments ?? null;
  const [items, setItems] = useState<WorkItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [pendingOnly, setPendingOnly] = useState(false);
  const [showArchived, setShowArchived] = useState(false);
  const [selection, setSelection] = useState<WorkItem | null>(null);
  const [sessionDetail, setSessionDetail] = useState<SessionRecord | null>(null);
  const [taskDetail, setTaskDetail] = useState<TaskRecord | null>(null);
  const [taskDraft, setTaskDraft] = useState<TaskDraft | null>(null);
  const [taskBusy, setTaskBusy] = useState(false);
  // Resume is accepted, not completed: the record it returns is
  // still interrupted. While this holds the task's id, polling
  // continues until the runner leaves interrupted.
  const [resumingTaskId, setResumingTaskId] = useState<string | null>(null);
  const [loopDetail, setLoopDetail] = useState<LoopSnapshot | null>(null);
  const [detailError, setDetailError] = useState("");
  const [composer, setComposer] = useState("");
  const [sending, setSending] = useState(false);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const [renaming, setRenaming] = useState(false);
  const [renameTitle, setRenameTitle] = useState("");
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewError, setPreviewError] = useState("");
  const [loopBusy, setLoopBusy] = useState(false);
  // Chat records (Task 18): per-thread unread counts from
  // /chat/unread-count, plus a local message search over the
  // gateway's /chat/search. Stored indices are the array
  // positions of sessionDetail.turns (the gateway returns
  // all turns, tombstones included, in stored order).
  const [unreadByThread, setUnreadByThread] = useState<Record<string, number>>({});
  const [chatNotice, setChatNotice] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<SearchMatch[] | null>(null);
  const [searching, setSearching] = useState(false);
  const [searchError, setSearchError] = useState("");
  // Chat stream (Task 20): subscription + event replay for the
  // current session. Events are a record projection, not a
  // live token stream; the copy below says so.
  const [streamSubscriptionId, setStreamSubscriptionId] = useState<string | null>(null);
  const [streamCursor, setStreamCursor] = useState<string | null>(null);
  const [streamEvents, setStreamEvents] = useState<StreamEvent[]>([]);
  const [streamCancelled, setStreamCancelled] = useState(false);
  const [streamNotice, setStreamNotice] = useState("");
  const [streamBusy, setStreamBusy] = useState(false);
  const selectionToken = useRef(0);
  // P0-R R5 Inbox section: open rows only, from the gateway;
  // no push, no reminder mechanism beyond the open count.
  // Clicking a row expands the same ExecutionDetail the Tasks
  // view uses, inline — never a new page.
  const [inboxRows, setInboxRows] = useState<InboxRow[]>([]);
  const [inboxError, setInboxError] = useState("");
  const [inboxNotice, setInboxNotice] = useState("");
  const [expandedExecutionId, setExpandedExecutionId] = useState<string | null>(null);
  const [inboxExecutions, setInboxExecutions] = useState<ExecutionRecord[]>([]);
  const [inboxEffects, setInboxEffects] = useState<EffectRecord[]>([]);
  const [inboxEvidence, setInboxEvidence] = useState<EvidenceRow[]>([]);

  async function refreshInbox() {
    if (!inboxClient) return;
    setInboxError("");
    try {
      setInboxRows(await inboxClient.list(undefined, true));
    } catch (e) {
      setInboxError(errText(e));
    }
  }

  async function expandInboxRow(row: InboxRow) {
    if (!row.executionId) {
      setExpandedExecutionId(null);
      setInboxNotice("该待办未关联执行记录，无法展开详情。");
      return;
    }
    setInboxNotice("");
    const id = row.executionId;
    if (expandedExecutionId === id) {
      setExpandedExecutionId(null);
      return;
    }
    setExpandedExecutionId(id);
    try {
      const [allExecutions, allEffects, allEvidence] = await Promise.all([
        executionsClient ? executionsClient.list() : Promise.resolve([] as ExecutionRecord[]),
        effectsClient ? effectsClient.list() : Promise.resolve([] as EffectRecord[]),
        evidenceClient ? evidenceClient.list() : Promise.resolve([] as EvidenceRow[]),
      ]);
      setInboxExecutions(allExecutions);
      setInboxEffects(allEffects);
      setInboxEvidence(allEvidence);
    } catch (e) {
      setInboxError(errText(e));
    }
  }

  async function ackInboxRow(row: InboxRow) {
    if (!inboxClient) return;
    setInboxError("");
    try {
      await inboxClient.ack(row.inboxId);
      setInboxNotice(`已确认待办 ${row.inboxId}。`);
      await refreshInbox();
    } catch (e) {
      setInboxError(errText(e));
    }
  }

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

  // Task detail shares the same list-sync rule. A gateway-run
  // task at draft_ready genuinely awaits a decision here, so it
  // carries the pending badge; a manual draft_ready task has no
  // gateway approval and can only be decided on the Tasks tab,
  // so it never shows as pending here.
  function updateTask(record: TaskRecord) {
    setTaskDetail(record);
    const pending = record.status === "draft_ready" && !!record.approvalId;
    setItems((previous) => previous.map((item) => item.key === `task:${record.taskId}`
      ? {
          ...item,
          status: taskLabels[record.status] ?? record.status,
          pending,
          pendingLabel: pending ? "待审批" : null,
        }
      : item));
  }

  // After any task action or poll, sync the detail and the list,
  // and fetch the draft exactly when it is gateway-run and ready.
  // A missing draft is not a state: the detail stays, the draft
  // block explains itself.
  async function syncTask(record: TaskRecord) {
    updateTask(record);
    if (record.status !== "interrupted") setResumingTaskId(null);
    // A gateway-run task's draft stays inspectable after the
    // decision: rejected, cancelled, failed and completed all
    // keep the sidecar. Only running and interrupted have no
    // draft yet, so no fetch there.
    const viewable = record.status === "draft_ready" || record.status === "rejected"
      || record.status === "cancelled" || record.status === "failed"
      || record.status === "completed";
    if (viewable && record.approvalId) {
      try {
        setTaskDraft(await tasks.draft(record.taskId));
      } catch (e) {
        // On draft_ready the draft must exist, so a fetch failure
        // is an error. On a decided task it only means no draft
        // block; the detail itself stays.
        if (record.status === "draft_ready") setDetailError(errText(e));
        else setTaskDraft(null);
      }
    } else {
      setTaskDraft(null);
    }
  }

  // A 409 on a loop resume means the driver is already running
  // the step: show running, not failure.
  function loopRunText(e: unknown): string {
    const text = errText(e);
    if (text.includes("409") || text.includes("loop_step_in_flight")) return "自动运行中，稍后查询即可。";
    return text;
  }

  async function refreshList(includeArchived = showArchived) {
    setLoading(true);
    setError("");
    try {
      const [sessionList, taskList, loopList] = await Promise.all([
        sessions.list(includeArchived),
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
          status: s.pinned
            ? `置顶 · ${s.turnCount} 条消息`
            : s.archived
              ? `归档 · ${s.turnCount} 条消息`
              : `${s.turnCount} 条消息`,
          pending: false,
          pendingLabel: null,
          pinned: s.pinned,
          archived: s.archived,
        });
      }
      for (const t of taskList) {
        next.push({
          key: `task:${t.taskId}`,
          kind: "task",
          id: t.taskId,
          title: t.artifactPath ?? (t.directory || t.taskId),
          status: taskLabels[t.status] ?? t.status,
          pending: t.status === "draft_ready" && !!t.approvalId,
          pendingLabel: t.status === "draft_ready" && !!t.approvalId ? "待审批" : null,
          pinned: false,
          archived: false,
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
          pinned: false,
          archived: false,
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
    void refreshUnread();
    void refreshInbox();
    // Mount only: refreshes come from explicit actions below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function select(item: WorkItem) {
    selectionToken.current += 1;
    setSelection(item);
    setSessionDetail(null);
    setTaskDetail(null);
    setTaskDraft(null);
    setResumingTaskId(null);
    setLoopDetail(null);
    setDetailError("");
    setConfirmDelete(false);
    setRenaming(false);
    setPreview(null);
    setPreviewError("");
    setSearchResults(null);
    setSearchError("");
    try {
      if (item.kind === "session") setSessionDetail(await sessions.get(item.id));
      else if (item.kind === "task") await syncTask(await tasks.get(item.id));
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
        pinned: false,
        archived: false,
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
      await refreshUnread();
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

  async function refreshUnread() {
    try {
      const counts = await chatRecords.getUnreadCount();
      const next: Record<string, number> = {};
      for (const thread of counts.threads) next[thread.threadId] = thread.unread;
      setUnreadByThread(next);
    } catch {
      // Unread counts are decorative; a failed refresh must
      // never block the message list.
    }
  }

  async function markSeenCurrent() {
    if (!sessionDetail) return;
    setChatNotice("");
    try {
      await chatRecords.markSeen(sessionDetail.sessionId, null);
      setSessionDetail(await sessions.get(sessionDetail.sessionId));
      await refreshUnread();
      setChatNotice("已标为已读。");
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  async function deleteTurn(index: number) {
    if (!sessionDetail) return;
    setChatNotice("");
    try {
      await chatRecords.deleteTurn(sessionDetail.sessionId, index);
      setSessionDetail(await sessions.get(sessionDetail.sessionId));
      await refreshList();
      await refreshUnread();
      setChatNotice("已删除该条消息。删除是 tombstone：记录文件旧行仍在，不会从磁盘擦除。");
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  async function runSearch() {
    if (!searchQuery.trim()) {
      setSearchResults(null);
      setSearchError("");
      return;
    }
    setSearching(true);
    setSearchError("");
    try {
      setSearchResults(await chatRecords.search(searchQuery.trim()));
    } catch (e) {
      setSearchError(errText(e));
      setSearchResults(null);
    } finally {
      setSearching(false);
    }
  }

  // Switching sessions drops the stream view: subscriptions
  // belong to one thread, and the event list must not leak
  // across threads.
  useEffect(() => {
    setStreamSubscriptionId(null);
    setStreamCursor(null);
    setStreamEvents([]);
    setStreamCancelled(false);
    setStreamNotice("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionDetail?.sessionId]);

  async function subscribeStream() {
    if (!sessionDetail) return;
    setStreamNotice("");
    setStreamBusy(true);
    try {
      const sub = await streams.subscribe(sessionDetail.sessionId);
      setStreamSubscriptionId(sub.subscriptionId);
      setStreamCancelled(false);
      // Replay from the start so the user sees the projection;
      // later pulls use the cursor and only fetch new events.
      const replay = await streams.streamEvents(sub.subscriptionId, null, null);
      setStreamEvents(replay.events);
      setStreamCursor(replay.cursor);
      setStreamNotice(`已订阅，订阅号 ${sub.subscriptionId}。`);
    } catch (e) {
      setDetailError(errText(e));
    } finally {
      setStreamBusy(false);
    }
  }

  async function pullStreamEvents() {
    if (!streamSubscriptionId) return;
    setStreamBusy(true);
    try {
      const page = await streams.streamEvents(streamSubscriptionId, streamCursor, null);
      setStreamEvents((previous) => [...previous, ...page.events]);
      setStreamCursor(page.cursor);
      setStreamCancelled(page.cancelled);
      if (page.cancelled) setStreamNotice("该订阅已取消，不再投递事件。");
    } catch (e) {
      setDetailError(errText(e));
    } finally {
      setStreamBusy(false);
    }
  }

  async function cancelStream() {
    if (!sessionDetail) return;
    setStreamNotice("");
    try {
      const result = await streams.cancel(sessionDetail.sessionId);
      setStreamCancelled(true);
      setStreamNotice(
        `已取消 ${result.cancelledSubscriptions} 个订阅。取消只停事件投递，不中断模型调用。`,
      );
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  async function openSearchResult(match: SearchMatch) {
    const item = items.find((entry) => entry.kind === "session" && entry.id === match.threadId);
    if (item) {
      await select(item);
    } else {
      setChatNotice("该会话不在当前列表中（可能已归档或被隐藏）。");
    }
  }

  async function togglePin() {
    if (!sessionDetail) return;
    setDetailError("");
    try {
      const summary = sessionDetail.pinned
        ? await sessions.unpin(sessionDetail.sessionId)
        : await sessions.pin(sessionDetail.sessionId);
      setSessionDetail({ ...sessionDetail, pinned: summary.pinned });
      await refreshList();
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  async function toggleArchive() {
    if (!sessionDetail) return;
    setDetailError("");
    try {
      const summary = sessionDetail.archived
        ? await sessions.unarchive(sessionDetail.sessionId)
        : await sessions.archive(sessionDetail.sessionId);
      setSessionDetail({ ...sessionDetail, archived: summary.archived });
      await refreshList();
    } catch (e) {
      setDetailError(errText(e));
    }
  }

  async function confirmRename() {
    if (!sessionDetail || !renameTitle.trim()) return;
    setDetailError("");
    try {
      const summary = await sessions.rename(sessionDetail.sessionId, renameTitle.trim());
      setSessionDetail({ ...sessionDetail, title: summary.title });
      setRenaming(false);
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

  // Task polling: status reads only, while the gateway runner
  // is working. Landing on draft_ready also fetches the draft.
  // A resumed task polls too: resume returns the pre-run
  // interrupted snapshot, so polling must continue until the
  // runner leaves interrupted, not just while running.
  useEffect(() => {
    if (!taskDetail) return;
    const polling = taskDetail.status === "running"
      || (taskDetail.status === "interrupted" && resumingTaskId === taskDetail.taskId);
    if (!polling) return;
    const token = selectionToken.current;
    const timer = setInterval(async () => {
      try {
        const next = await tasks.get(taskDetail.taskId);
        if (selectionToken.current === token) await syncTask(next);
      } catch {
        // A failed poll leaves the last snapshot; the user can
        // refresh explicitly. Never invent a state.
      }
    }, 2000);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [taskDetail?.taskId, taskDetail?.status, resumingTaskId]);

  async function resumeTask() {
    if (!taskDetail || taskBusy) return;
    setTaskBusy(true);
    setDetailError("");
    try {
      const record = await tasks.resume(taskDetail.taskId);
      await syncTask(record);
      // Resume is accepted, not completed: the record above is
      // still interrupted. Poll until the runner leaves it.
      if (record.status === "interrupted") setResumingTaskId(taskDetail.taskId);
    } catch (e) {
      setDetailError(errText(e));
    } finally {
      setTaskBusy(false);
    }
  }

  async function runTaskAction(action: () => Promise<TaskRecord>) {
    if (!taskDetail || taskBusy) return;
    setTaskBusy(true);
    setDetailError("");
    try {
      await syncTask(await action());
    } catch (e) {
      setDetailError(errText(e));
    } finally {
      setTaskBusy(false);
    }
  }

  async function approveTask() {
    const record = taskDetail;
    if (!record?.approvalId) return;
    setTaskBusy(true);
    setDetailError("");
    try {
      // The gateway-run approval, decided here, then executed:
      // execute never approves itself.
      await agent.decideApproval(record.approvalId, "approve");
      await syncTask(await tasks.execute(record.taskId));
    } catch (e) {
      setDetailError(errText(e));
    } finally {
      setTaskBusy(false);
    }
  }

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
      setDetailError(loopRunText(e));
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
      setDetailError(loopRunText(e));
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
                {item.kind === "session" && (unreadByThread[item.id] ?? 0) > 0 && (
                  <span className="pending-badge"> · 未读 {unreadByThread[item.id]} 条</span>
                )}
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
        这里是主聊天入口：会话支持多轮上下文，记录保存在本地、重启后恢复。左侧是会话与任务列表，中间是交流与执行时间线，右侧按需预览文件或产物。切换项目不批准、不续跑任何内容。
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
          <label>
            <input
              type="checkbox"
              checked={showArchived}
              onChange={(e) => {
                setShowArchived(e.target.checked);
                void refreshList(e.target.checked);
              }}
            />
            显示归档
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
              <div className="row">
                <span>未读 {unreadByThread[sessionDetail.sessionId] ?? 0} 条</span>
                <button type="button" onClick={() => void markSeenCurrent()}>标为已读</button>
              </div>
              {chatNotice && <p className="note">{chatNotice}</p>}
              <div className="row">
                <input
                  aria-label="聊天记录搜索"
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="搜索消息…"
                  onKeyDown={(e) => {
                    if (e.key === "Enter") void runSearch();
                  }}
                />
                <button type="button" onClick={() => void runSearch()} disabled={searching}>
                  {searching ? "搜索中…" : "搜索"}
                </button>
              </div>
              {searchError && <p className="error" role="alert">{searchError}</p>}
              {searchResults !== null && (
                <div className="messages">
                  {searchResults.length === 0 && <p className="empty">没有匹配的消息。</p>}
                  {searchResults.map((match) => (
                    <div key={`${match.threadId}-${match.index}`} className={`message ${match.role}`}>
                      <div className="role">
                        {match.role === "user" ? "你" : "助手"} · {match.threadId}
                      </div>
                      <div className="text">{match.text}</div>
                      <button type="button" onClick={() => void openSearchResult(match)}>打开会话</button>
                    </div>
                  ))}
                </div>
              )}
              <p className="note">流事件是记录重放，不是实时 token 流。</p>
              <div className="row">
                <button type="button" onClick={() => void subscribeStream()} disabled={streamBusy}>
                  订阅流事件
                </button>
                <button
                  type="button"
                  onClick={() => void pullStreamEvents()}
                  disabled={streamBusy || !streamSubscriptionId}
                >
                  拉取新事件
                </button>
                <button type="button" onClick={() => void cancelStream()}>
                  取消订阅
                </button>
              </div>
              {streamNotice && <p className="note">{streamNotice}</p>}
              {streamSubscriptionId && (
                <div className="messages">
                  {streamEvents.length === 0 && <p className="empty">暂无流事件。</p>}
                  {streamEvents.map((event) => (
                    <div key={event.id} className="message">
                      <div className="role">
                        {event.type}
                        {event.turnIndex !== null ? ` · turn ${event.turnIndex}` : ""}
                      </div>
                      {event.text && <div className="text">{event.text}</div>}
                    </div>
                  ))}
                  {streamCancelled && <p className="note">该订阅已取消。</p>}
                </div>
              )}
              {sessionDetail.turns.filter((turn) => !turn.deleted).length === 0 && (
                <p className="empty">还没有消息。</p>
              )}
              <div className="messages">
                {sessionDetail.turns.map((turn, index) =>
                  turn.deleted ? null : (
                    <div key={`${turn.createdMs}-${index}`} className={`message ${turn.role}`}>
                      <div className="role">
                        {turn.role === "user" ? "你" : "助手"}
                        {turn.model ? ` · ${turn.model}` : ""}
                      </div>
                      <div className="text">{turn.text}</div>
                      <button type="button" onClick={() => void deleteTurn(index)}>删除</button>
                    </div>
                  ),
                )}
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
              <div className="row">
                <button type="button" onClick={() => void togglePin()}>
                  {sessionDetail.pinned ? "取消置顶" : "置顶"}
                </button>
                <button type="button" onClick={() => void toggleArchive()}>
                  {sessionDetail.archived ? "恢复归档" : "归档"}
                </button>
                {!renaming && (
                  <button
                    type="button"
                    onClick={() => {
                      setRenameTitle(sessionDetail.title);
                      setRenaming(true);
                    }}
                  >
                    重命名
                  </button>
                )}
              </div>
              {renaming && (
                <div className="row">
                  <input
                    aria-label="会话新标题"
                    type="text"
                    value={renameTitle}
                    onChange={(e) => setRenameTitle(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") void confirmRename();
                    }}
                  />
                  <button type="button" onClick={() => void confirmRename()} disabled={!renameTitle.trim()}>
                    确认
                  </button>
                  <button type="button" onClick={() => setRenaming(false)}>
                    取消
                  </button>
                </div>
              )}
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
              {taskDetail.status === "failed" && taskDetail.error && (
                <p className="error">失败原因：{taskDetail.error}</p>
              )}
              {taskDetail.status === "created" && (
                <div className="row">
                  <button type="button" disabled={taskBusy} onClick={() => void runTaskAction(() => tasks.run(taskDetail.taskId))}>
                    自动运行到审批点
                  </button>
                </div>
              )}
              {taskDetail.status === "running" && (
                <>
                  <p>网关正在运行，关窗不丢进度。</p>
                  <div className="row">
                    <button type="button" disabled={taskBusy} onClick={() => void runTaskAction(() => tasks.cancel(taskDetail.taskId))}>
                      取消
                    </button>
                  </div>
                </>
              )}
              {taskDetail.status === "interrupted" && (
                <div className="row">
                  <button type="button" disabled={taskBusy} onClick={() => void resumeTask()}>
                    恢复运行
                  </button>
                </div>
              )}
              {taskDetail.status === "draft_ready" && taskDetail.approvalId && taskDraft && (
                <div>
                  <h3>草稿 · 产物：{taskDetail.artifactPath ?? "report.md"}</h3>
                  <p className="note">
                    草稿摘要：{taskDraft.draftSha256} · {taskDraft.draftBytes} 字节。批准即按此草稿写入，路径不可改。
                  </p>
                  <pre className="result">{taskDraft.draft}</pre>
                  <div className="row">
                    <button type="button" disabled={taskBusy} onClick={() => void approveTask()}>
                      批准并写入
                    </button>
                    <button type="button" disabled={taskBusy} onClick={() => void runTaskAction(() => tasks.reject(taskDetail.taskId))}>
                      拒绝
                    </button>
                    <button type="button" disabled={taskBusy} onClick={() => void runTaskAction(() => tasks.cancel(taskDetail.taskId))}>
                      取消
                    </button>
                  </div>
                </div>
              )}
              {taskDetail.status === "draft_ready" && taskDetail.approvalId && !taskDraft && (
                <p>草稿加载中…若一直未出现，点刷新列表后重选任务。</p>
              )}
              {taskDetail.status === "draft_ready" && !taskDetail.approvalId && (
                <p className="note">手动任务的草稿只在任务页，审批写入也在任务页进行。</p>
              )}
              {taskDraft && taskDetail.status !== "draft_ready" && (
                <div>
                  <h3>草稿（只读）</h3>
                  <p className="note">只供查看原草稿，不产生新的写入或审批。</p>
                  <pre className="result">{taskDraft.draft}</pre>
                </div>
              )}
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
      <div>
        <h3>Inbox（待办）</h3>
        {!inboxClient && (
          <p className="note">待办客户端尚未接入：待 App 接线传入客户端后，这里显示未关闭的待办（含 verification_mismatch 类）。</p>
        )}
        {inboxClient && (
          <div>
            <p>未关闭待办 {inboxRows.length} 条。点一行在行内展开执行详情，不跳新页面。</p>
            <button type="button" onClick={() => void refreshInbox()}>
              刷新待办
            </button>
            {inboxError && <p className="error" role="alert">{inboxError}</p>}
            {inboxNotice && <p className="note">{inboxNotice}</p>}
            {inboxRows.length === 0 && <p className="empty">暂无未关闭待办。</p>}
            <ul>
              {inboxRows.map((row) => {
                const expanded = expandedExecutionId !== null
                  && row.executionId !== null
                  && expandedExecutionId === row.executionId;
                const execution = inboxExecutions.find((e) => e.executionId === row.executionId);
                return (
                  <li key={row.inboxId}>
                    <button type="button" onClick={() => void expandInboxRow(row)}>
                      {row.inboxId} · {row.kind}
                      {row.executionId ? ` · 执行 ${row.executionId}` : ""}
                      {row.effectId ? ` · Effect ${row.effectId}` : ""}
                    </button>{" "}
                    <button type="button" onClick={() => void ackInboxRow(row)}>
                      确认
                    </button>
                    {expanded && execution && (
                      <ExecutionDetail
                        execution={execution}
                        effects={inboxEffects.filter((e) => e.executionId === execution.executionId)}
                        evidence={inboxEvidence}
                        effectsClient={effectsClient}
                        executionsClient={executionsClient}
                        assignmentsClient={assignmentsClient}
                        onRefresh={() => void refreshInbox()}
                        onError={(message) => setInboxError(message)}
                      />
                    )}
                    {expanded && !execution && (
                      <p className="note">该执行记录尚未载入：点“刷新待办”后重试，或到任务页查看。</p>
                    )}
                  </li>
                );
              })}
            </ul>
          </div>
        )}
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
            运行到下一停止点
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
      {waiting && current.pending_reminder && (
        <LoopReminderApproval
          snapshot={current}
          call={call}
          busy={busy}
          onDecide={onDecide}
        />
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
