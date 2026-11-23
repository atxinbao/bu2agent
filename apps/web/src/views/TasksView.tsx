import { useEffect, useState } from "react";
import type { ChatGateway } from "../domain/gateway_client";
import { initialChatState, runTurn } from "../domain/chat";
import type { ChatState } from "../domain/chat";
import type { FileGateway, GatewayFileEntry } from "../domain/file_client";
import type { AgentGateway } from "../domain/agent_client";
import type { TaskGateway, TaskRecord } from "../domain/task_client";
// P0-R R5: Execution / Effect / Evidence / Assignment clients
// are optional props (App.tsx still needs to pass them —
// outside this task's file scope); the view takes them as
// props like every other client, so App.tsx's transport choice
// applies unchanged. When absent the Execution section below
// says so and the rest of the view works as before.
import type { AssignmentsGateway } from "../domain/assignments_client";
import type { EffectRecord, EffectsGateway } from "../domain/effects_client";
import type { EvidenceGateway, EvidenceRow } from "../domain/evidence_client";
import type {
  ExecutionRecord,
  ExecutionsGateway,
} from "../domain/executions_client";
import type { MandatesGateway } from "../domain/mandates_client";
import type { Plan, PlanGateway } from "../domain/plan_client";
import type { ModelGateway, ModelState } from "../domain/model_client";
import type {
  EngineeringGateway,
  Worktree,
} from "../domain/engineering_client";
import { ExecutionDetail } from "./ExecutionDetail";
import { MandatesSection } from "./MandatesSection";

// Tasks view: the deterministic task loop, driven step by step
// from the frontend (see the task-loop design doc). No model
// tool loop: the model only generates the report text, and
// every other step reuses a verified primitive.
//
// Flow: pick a directory -> create task -> read its text files
// -> generate a draft via the chat line -> show the draft and
// the target artifact path -> approve, then write -> task
// completed with its artifact path -> view the artifact.
// Rejection records the task as rejected and writes nothing.
// Any step failure marks the task failed.
//
// Round 3b: a task can also run on the gateway (create -> run
// -> draft_ready -> approve -> execute). Gateway-run tasks keep
// their draft in a server sidecar, so closing the window loses
// nothing; their approval binds the record's artifact path, so
// the path is shown read-only and the manual write path is not
// used for them. The frontend-driven flow above stays for
// manual tasks.
//
// Honest boundary, same as the chat line: without a model key
// the generate step returns the echo, so the loop runs
// mechanically but a real report needs a configured key.

interface TasksViewProps {
  chat: ChatGateway;
  files: FileGateway;
  agent: AgentGateway;
  tasks: TaskGateway;
  executions?: ExecutionsGateway | null;
  effects?: EffectsGateway | null;
  evidence?: EvidenceGateway | null;
  assignments?: AssignmentsGateway | null;
  // FU-3C (Q-R0-01): mandates client for the task-level
  // "授权范围" section below and for ExecutionDetail. Optional,
  // same as the other clients: until App.tsx passes it the
  // sections below say so.
  mandates?: MandatesGateway | null;
  // ORC-1 (Q-ORC-03): plan client for the task-level
  // "蓝图" section below. Optional, same as the other
  // clients: until App.tsx passes it the section below
  // says so.
  plans?: PlanGateway | null;
  // ORC-2: model client for the read-only role-slot
  // display ("模型" section below). Optional, same
  // pattern: until App.tsx passes it the section says
  // so. Read-only — slot setting lives in Settings.
  models?: ModelGateway | null;
  // W4-1 (Q-FW-02): engineering client for the
  // task-level worktree picker below. Optional, same
  // pattern as the other clients: until App.tsx passes
  // it the section says so. The pick is explicit per
  // task — no global default, no remembered pick — and
  // an unpicked run passes no worktree id at all.
  engineering?: EngineeringGateway | null;
  // Settings page's default working directory (root-relative,
  // "" = file root). Only the initial directory for a new
  // task and its default artifact path; an explicitly typed
  // directory always wins, and existing tasks are untouched.
  // When defaultDirValid is false the stored default is
  // ignored entirely: creating a task with no typed directory
  // is refused instead of falling back to the root, so no
  // write can land in a location the user never chose.
  defaultDir?: string;
  defaultDirValid?: boolean;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

const CHUNK_LEN = 4096;
const MAX_FILE_BYTES = 262144;
const MAX_ARTIFACT_BYTES = 1048576;

export function TasksView({
  chat,
  files,
  agent,
  tasks,
  executions: executionsClient = null,
  effects: effectsClient = null,
  evidence: evidenceClient = null,
  assignments: assignmentsClient = null,
  mandates: mandatesClient = null,
  plans: plansClient = null,
  models: modelsClient = null,
  engineering: engineeringClient = null,
  defaultDir = "",
  defaultDirValid = true,
}: TasksViewProps) {
  const [directory, setDirectory] = useState(defaultDirValid ? defaultDir : "");
  const [entries, setEntries] = useState<GatewayFileEntry[]>([]);
  const [taskList, setTaskList] = useState<TaskRecord[]>([]);
  const [current, setCurrent] = useState<TaskRecord | null>(null);
  const [draft, setDraft] = useState("");
  const [artifactPath, setArtifactPath] = useState("report.md");
  const [artifactContent, setArtifactContent] = useState("");
  const [chatState, setChatState] = useState<ChatState>(initialChatState);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  // Resume is accepted, not completed: the record it returns is
  // still interrupted. While this is true, polling continues
  // until the runner leaves interrupted.
  const [resuming, setResuming] = useState(false);
  // P0-R R5 Execution section: gateway rows loaded once per
  // selected task and filtered client-side (the list routes
  // take no filter parameters). These are the same rows the
  // Workbench Inbox and Loops views read; nothing is cached
  // or derived here beyond the id filters.
  const [taskExecutions, setTaskExecutions] = useState<ExecutionRecord[]>([]);
  const [taskEffects, setTaskEffects] = useState<EffectRecord[]>([]);
  const [taskEvidence, setTaskEvidence] = useState<EvidenceRow[]>([]);
  const [execError, setExecError] = useState("");
  // ORC-1: the selected task's current plan, loaded
  // once per selected task. A 404 means the task has
  // no plan yet ("暂无蓝图"); any other failure is a
  // read error, not an empty plan.
  const [currentPlan, setCurrentPlan] = useState<Plan | null>(null);
  const [planError, setPlanError] = useState("");
  // ORC-2: the effective Planner/Executor models
  // (read-only display). Loaded once when the view
  // mounts; the slots are global, not per-task.
  const [modelState, setModelState] = useState<ModelState | null>(null);
  const [modelError, setModelError] = useState("");

  async function refreshModels() {
    if (!modelsClient) return;
    setModelError("");
    try {
      setModelState(await modelsClient.getModel());
    } catch (e) {
      setModelState(null);
      setModelError(`模型读取失败：${errText(e)}`);
    }
  }

  // W4-1 (Q-FW-02): the worktree picker options.
  // Loaded once when the view mounts; only active
  // rows are offered. The pick itself lives in
  // selectedWorktreeId and resets per selected task.
  const [worktrees, setWorktrees] = useState<Worktree[]>([]);
  const [worktreeError, setWorktreeError] = useState("");
  const [selectedWorktreeId, setSelectedWorktreeId] = useState("");

  async function refreshWorktrees() {
    if (!engineeringClient) return;
    setWorktreeError("");
    try {
      const all = await engineeringClient.worktreeList();
      setWorktrees(all.filter((w) => w.status === "active"));
    } catch (e) {
      setWorktrees([]);
      setWorktreeError(`工作区读取失败：${errText(e)}`);
    }
  }

  async function refreshPlan(taskId: string) {
    if (!plansClient) return;
    setPlanError("");
    try {
      setCurrentPlan(await plansClient.currentPlan(taskId));
    } catch (e) {
      if (errText(e).includes("404")) {
        setCurrentPlan(null);
      } else {
        setCurrentPlan(null);
        setPlanError(`蓝图读取失败：${errText(e)}`);
      }
    }
  }

  async function refreshExecutionData(taskId: string) {
    if (!executionsClient) return;
    setExecError("");
    try {
      const [allExecutions, allEffects, allEvidence] = await Promise.all([
        executionsClient.list(),
        effectsClient ? effectsClient.list() : Promise.resolve([] as EffectRecord[]),
        evidenceClient ? evidenceClient.list() : Promise.resolve([] as EvidenceRow[]),
      ]);
      const mine = allExecutions.filter((e) => e.taskId === taskId);
      const ids = new Set(mine.map((e) => e.executionId));
      setTaskExecutions(mine);
      setTaskEffects(allEffects.filter((e) => e.executionId !== null && ids.has(e.executionId)));
      setTaskEvidence(allEvidence.filter((e) => e.executionId !== null && ids.has(e.executionId)));
    } catch (e) {
      setExecError(`执行记录读取失败：${errText(e)}`);
    }
  }

  useEffect(() => {
    if (current) {
      void refreshExecutionData(current.taskId);
      void refreshPlan(current.taskId);
    } else {
      setTaskExecutions([]);
      setTaskEffects([]);
      setTaskEvidence([]);
      setCurrentPlan(null);
      setPlanError("");
    }
    // W4-1: the worktree pick is explicit per task.
    setSelectedWorktreeId("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [current?.taskId]);

  async function refreshTasks() {
    try {
      setTaskList(await tasks.list());
    } catch (e) {
      setError(`任务列表读取失败：${errText(e)}`);
    }
  }

  useEffect(() => {
    void refreshTasks();
    void refreshModels();
    void refreshWorktrees();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // While the gateway runner is working, poll the current task
  // only. Landing on draft_ready fetches the server draft, so
  // the approval block appears without re-selecting the task.
  // A resumed task polls too: resume returns the pre-run
  // interrupted snapshot, so polling must continue until the
  // runner leaves interrupted, not just while running.
  useEffect(() => {
    if (!current) return;
    const polling = current.status === "running"
      || (current.status === "interrupted" && resuming);
    if (!polling) return;
    const id = current.taskId;
    const timer = setInterval(async () => {
      try {
        const next = await tasks.get(id);
        setCurrent(next);
        if (next.status !== "interrupted") setResuming(false);
        if (next.status === "draft_ready" && next.approvalId) {
          try {
            setDraft((await tasks.draft(id)).draft);
          } catch (e) {
            setError(`网关草稿读取失败：${errText(e)}`);
          }
        }
      } catch {
        // A failed poll leaves the last snapshot; the user can
        // refresh the list explicitly.
      }
    }, 2000);
    return () => clearInterval(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [current?.taskId, current?.status, resuming]);

  async function failCurrent(task: TaskRecord, reason: string) {
    try {
      const updated = await tasks.update(task.taskId, "failed");
      setCurrent(updated);
    } catch {
      // Best effort: the in-memory record keeps its old status,
      // the error below is what the user acts on.
    }
    setError(reason);
  }

  async function handleListDirectory() {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      setEntries(await files.list(directory));
    } catch (e) {
      setError(`目录列出失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleCreate() {
    setError("");
    setMessage("");
    if (!defaultDirValid && directory === "") {
      // No usable default and no explicit directory: refuse
      // instead of creating the task at the root. The user
      // either re-selects a default in settings or types a
      // directory here; either is an explicit choice.
      setError("默认工作目录已失效，请先到设置页重新选择，或在此填写一个目录。");
      return;
    }
    setBusy(true);
    try {
      const task = await tasks.create(directory);
      setCurrent(task);
      setDraft("");
      setArtifactContent("");
      setArtifactPath(directory === "" ? "report.md" : `${directory}/report.md`);
      setMessage(`任务已创建：${task.taskId}（${task.status}）`);
      await refreshTasks();
    } catch (e) {
      setError(`任务创建失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleRun() {
    if (!current) return;
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const updated = await tasks.run(
        current.taskId,
        selectedWorktreeId === "" ? undefined : selectedWorktreeId,
      );
      setCurrent(updated);
      setMessage("网关正在运行，关窗不丢进度；到审批点后在此批准。");
      await refreshTasks();
    } catch (e) {
      setError(`运行失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  // ORC-2 (Q-ORC-07): force-run a task whose Planner
  // round already answered "no file change" (the task
  // completed with a direct answer and no artifact).
  // The button only renders for such tasks, so this
  // never overrides a judgment the user has not seen.
  async function handleRunForce() {
    if (!current) return;
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const updated = await tasks.runForce(
        current.taskId,
        selectedWorktreeId === "" ? undefined : selectedWorktreeId,
      );
      setCurrent(updated);
      setMessage("已强制执行：网关正在按蓝图运行，关窗不丢进度；到审批点后在此批准。");
      await refreshTasks();
    } catch (e) {
      setError(`强制执行失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleResume() {
    if (!current) return;
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const updated = await tasks.resume(current.taskId);
      setCurrent(updated);
      if (updated.status === "draft_ready" && updated.approvalId) {
        try {
          setDraft((await tasks.draft(updated.taskId)).draft);
        } catch (e) {
          setError(`网关草稿读取失败：${errText(e)}`);
        }
      } else if (updated.status === "interrupted") {
        // Accepted, not completed: keep polling until the runner
        // leaves interrupted.
        setResuming(true);
        setMessage("恢复已接受，正在跟进运行结果。");
      } else {
        setMessage("已恢复运行。");
      }
      await refreshTasks();
    } catch (e) {
      setError(`恢复失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleCancel() {
    if (!current) return;
    const wasDraftReady = current.status === "draft_ready";
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const updated = await tasks.cancel(current.taskId);
      setCurrent(updated);
      setMessage(wasDraftReady
        ? "已取消；该任务的待审批已同时标记拒绝，无法再执行写入。"
        : "已取消。");
      await refreshTasks();
    } catch (e) {
      setError(`取消失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleGenerate() {
    if (!current) {
      setError("请先创建任务。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const listed = await files.list(current.directory);
      const textFiles = listed.filter((e) => !e.isDir);
      if (textFiles.length === 0) {
        await failCurrent(current, "目录下没有可读文件，任务已标记失败。");
        return;
      }
      const parts: string[] = [];
      for (const entry of textFiles) {
        const text = await files.readText(entry.path, CHUNK_LEN, MAX_FILE_BYTES);
        parts.push(`## ${entry.path}\n${text}`);
      }
      const prompt = `请把以下目录文本整理成一份结构清晰的报告（标题、要点、结论）：\n\n${parts.join("\n\n")}`;
      const turnId = `task-${current.taskId}-gen-${Date.now()}`;
      const result = await runTurn(chat, chatState, {
        streamId: turnId,
        turnId,
        text: prompt,
      });
      setChatState(result.state);
      if (result.assistantText === "") {
        await failCurrent(current, "模型没有返回报告草稿，任务已标记失败。");
        return;
      }
      setDraft(result.assistantText);
      const updated = await tasks.update(current.taskId, "draft_ready");
      setCurrent(updated);
      setMessage("草稿已生成，请核对后批准写入。");
      await refreshTasks();
    } catch (e) {
      await failCurrent(current, `生成失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleApproveWrite() {
    if (!current) {
      setError("请先创建任务。");
      return;
    }
    if (draft === "") {
      setError("还没有草稿，请先生成。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      // Gateway-run tasks (approvalId set) never use the manual
      // file write: the window decides first, then the gateway
      // executes the bound approval. Execute never approves.
      if (current.approvalId) {
        await agent.decideApproval(current.approvalId, "approve");
        const updated = await tasks.execute(
          current.taskId,
          selectedWorktreeId === "" ? undefined : selectedWorktreeId,
        );
        setCurrent(updated);
        setMessage(`已批准并写入 ${artifactPath}，任务完成。`);
        await refreshTasks();
        return;
      }
      // Round 2: the file client requests a bound approval for
      // this exact draft, decides it from this click, and
      // executes under it; a separate tool-only request would
      // leave an orphan record and no longer passes the server.
      await files.write(artifactPath, draft, true);
      const updated = await tasks.update(current.taskId, "completed", artifactPath);
      setCurrent(updated);
      setMessage(`已批准并写入 ${artifactPath}，任务完成。`);
      await refreshTasks();
    } catch (e) {
      setError(`写入失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleReject() {
    if (!current) {
      setError("请先创建任务。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      // Gateway-run tasks reject through their own endpoint,
      // which decides the stored approval rejected itself.
      if (current.approvalId) {
        const updated = await tasks.reject(current.taskId);
        setCurrent(updated);
        setMessage("已拒绝写入，任务标记为 rejected，未写任何文件。");
        await refreshTasks();
        return;
      }
      // The rejection is recorded against the exact draft, so
      // the audit shows what was refused. With no draft there
      // is nothing to refuse; the task rejection alone stands.
      if (draft !== "") {
        const request = await agent.requestApproval("fs.write", {
          path: artifactPath,
          text: draft,
        });
        if (request.requires_approval && request.tool_call_id) {
          await agent.decideApproval(request.tool_call_id, "reject");
        }
      }
      const updated = await tasks.update(current.taskId, "rejected");
      setCurrent(updated);
      setMessage("已拒绝写入，任务标记为 rejected，未写任何文件。");
      await refreshTasks();
    } catch (e) {
      setError(`拒绝失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleViewArtifact(task: TaskRecord) {
    if (!task.artifactPath) {
      setError("该任务还没有产物路径。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const content = await files.readText(
        task.artifactPath,
        CHUNK_LEN,
        MAX_ARTIFACT_BYTES,
      );
      setArtifactContent(content);
      setCurrent(task);
      setMessage(`产物：${task.artifactPath}`);
    } catch (e) {
      setError(`产物读取失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleSelect(task: TaskRecord) {
    setError("");
    setMessage("");
    setResuming(false);
    setBusy(true);
    try {
      const fresh = await tasks.get(task.taskId);
      setCurrent(fresh);
      setDraft("");
      setArtifactContent("");
      if (fresh.artifactPath) {
        setArtifactPath(fresh.artifactPath);
      }
      // Gateway-run drafts live on the server, not in page
      // state: fetch them on selection, or the draft block
      // would stay empty for a task that is ready to decide.
      // Decided tasks keep their draft inspectable too; only
      // running and interrupted have no draft yet.
      const viewable = fresh.status === "draft_ready" || fresh.status === "rejected"
        || fresh.status === "cancelled" || fresh.status === "failed"
        || fresh.status === "completed";
      if (viewable && fresh.approvalId) {
        try {
          const saved = await tasks.draft(fresh.taskId);
          setDraft(saved.draft);
        } catch (e) {
          if (fresh.status === "draft_ready") {
            setError(`网关草稿读取失败：${errText(e)}`);
          }
        }
      }
    } catch (e) {
      setError(`任务读取失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h2>Tasks</h2>
      <p>
        确定性任务闭环：选目录 → 生成报告草稿 → 审批后写入 → 查看产物。
        无模型 key 时生成步骤返回回显（与聊天线一致），真实报告需要已配置的 key。
        已创建的任务也可以点“自动运行到审批点”，由网关端运行，关窗不丢进度。
      </p>
      {!defaultDirValid && (
        <p className="error">
          默认工作目录已失效，请到设置页重新选择；失效的默认目录不会用于任何写入，
          新建任务需先重新选择或在此填写目录。
        </p>
      )}
      <div>
        <input
          value={directory}
          onChange={(e) => setDirectory(e.target.value)}
          placeholder="目录（空 = 文件根）"
        />
        <button onClick={() => void handleListDirectory()} disabled={busy}>
          列出目录
        </button>
        <button onClick={() => void handleCreate()} disabled={busy}>
          创建任务
        </button>
        <button onClick={() => void refreshTasks()} disabled={busy}>
          刷新任务列表
        </button>
      </div>
      {entries.length > 0 && (
        <ul>
          {entries.map((e) => (
            <li key={e.path}>
              {e.isDir ? "📁" : "📄"} {e.path} ({e.sizeBytes} bytes)
            </li>
          ))}
        </ul>
      )}
      {current && (
        <div>
          <p>
            当前任务：{current.taskId} · 目录 {current.directory || "（根）"} ·
            状态 {current.status}
            {current.artifactPath ? ` · 产物 ${current.artifactPath}` : ""}
            {current.goalId ? ` · 目标 ${current.goalId}` : ""}
            {current.projectId ? ` · 项目 ${current.projectId}` : ""}
            {current.executeBinding
              ? ` · 冻结绑定 ${current.executeBinding.worktreeId}`
              : ""}
          </p>
          {current.status === "failed" && current.error && (
            <p>失败原因：{current.error}</p>
          )}
          <div>
            <h3>工作区</h3>
            {!engineeringClient && (
              <p className="note">工作区客户端尚未接入：待 App 接线传入客户端后，这里可以选择运行任务的工作区。</p>
            )}
            {engineeringClient && current.executeBinding && (
              <p>
                已冻结绑定 {current.executeBinding.worktreeId}，运行中不可改。
              </p>
            )}
            {engineeringClient && !current.executeBinding && (
              <div>
                <select
                  value={selectedWorktreeId}
                  onChange={(e) => setSelectedWorktreeId(e.target.value)}
                  disabled={busy}
                >
                  <option value="">未选择工作区</option>
                  {worktrees.map((w) => (
                    <option key={w.worktreeId} value={w.worktreeId}>
                      {w.worktreeId}
                      {w.directoryRelative ? ` · ${w.directoryRelative}` : ""}
                      {w.branchLabel ? ` · ${w.branchLabel}` : ""}
                    </option>
                  ))}
                </select>
                <button onClick={() => void refreshWorktrees()} disabled={busy}>
                  刷新工作区
                </button>
                {worktreeError !== "" && <p>错误：{worktreeError}</p>}
                <p className="note">
                  worker 步骤（验证/命令）要在工作区里跑；不选则不传，遇到这类步骤会按“未绑定工作区”失败。
                </p>
              </div>
            )}
          </div>
          <div>
            {!current.approvalId && (
              <button onClick={() => void handleGenerate()} disabled={busy}>
                读取并生成
              </button>
            )}
            {current.status === "created" && (
              <button onClick={() => void handleRun()} disabled={busy}>
                自动运行到审批点
              </button>
            )}
            {current.status === "completed" && !current.artifactPath && (
              <button onClick={() => void handleRunForce()} disabled={busy}>
                仍要执行
              </button>
            )}
            {current.status === "interrupted" && (
              <button onClick={() => void handleResume()} disabled={busy}>
                恢复运行
              </button>
            )}
            {(current.status === "running" ||
              (current.status === "draft_ready" && current.approvalId)) && (
              <button onClick={() => void handleCancel()} disabled={busy}>
                取消
              </button>
            )}
          </div>
          {draft !== "" && (
            <div>
              <h3>草稿</h3>
              <pre>{draft}</pre>
              {current.status === "draft_ready" ? (
                <div>
                  {current.approvalId ? (
                    <p>产物路径：{artifactPath}（审批绑定的路径，不可改）</p>
                  ) : (
                    <input
                      value={artifactPath}
                      onChange={(e) => setArtifactPath(e.target.value)}
                      placeholder="产物路径，如 report.md"
                    />
                  )}
                  <button onClick={() => void handleApproveWrite()} disabled={busy}>
                    批准并写入
                  </button>
                  <button onClick={() => void handleReject()} disabled={busy}>
                    拒绝
                  </button>
                </div>
              ) : (
                <p>只读草稿：查看原草稿，不产生新的写入或审批。</p>
              )}
            </div>
          )}
          {current.artifactPath && (
            <div>
              <button onClick={() => void handleViewArtifact(current)} disabled={busy}>
                查看产物
              </button>
            </div>
          )}
          <h3>模型</h3>
          {!modelsClient && (
            <p className="note">模型客户端尚未接入：待 App 接线传入客户端后，这里显示当前的规划/执行模型。</p>
          )}
          {modelsClient && (
            <div>
              <button onClick={() => void refreshModels()} disabled={busy}>
                刷新模型
              </button>
              {modelError !== "" && <p>错误：{modelError}</p>}
              {modelState !== null && (
                <p>
                  规划模型 {modelState.plannerModel} · 执行模型 {modelState.executorModel}
                  {modelState.restartNeeded ? "（有暂存改动，重启网关后生效）" : ""}
                </p>
              )}
              {modelState !== null &&
                (modelState.stagedPlannerModel !== null ||
                  modelState.stagedExecutorModel !== null) && (
                  <p className="note">
                    暂存：规划 {modelState.stagedPlannerModel ?? "—"} · 执行{" "}
                    {modelState.stagedExecutorModel ?? "—"}
                  </p>
                )}
              <p className="note">只读：槽位在设置页修改，重启网关后生效。</p>
            </div>
          )}
          <h3>蓝图</h3>
          {!plansClient && (
            <p className="note">蓝图客户端尚未接入：待 App 接线传入客户端后，这里显示该任务的当前蓝图。</p>
          )}
          {plansClient && (
            <div>
              <button onClick={() => void refreshPlan(current.taskId)} disabled={busy}>
                刷新蓝图
              </button>
              {planError !== "" && <p>错误：{planError}</p>}
              {currentPlan === null && planError === "" && <p className="empty">暂无蓝图。</p>}
              {currentPlan !== null && (
                <div>
                  <p>
                    {currentPlan.planId} · 版本 {currentPlan.version} · {currentPlan.status} ·
                    规划模型 {currentPlan.plannerModel} · 摘要 {currentPlan.digest.slice(0, 16)}…
                  </p>
                  <ol>
                    {currentPlan.steps.map((step) => (
                      <li key={step.stepNo}>
                        <p>
                          步骤 {step.stepNo} · {step.kind}
                        </p>
                        <p>路径：{step.path}</p>
                        <p>符号：{step.symbol}</p>
                        <p>改动：{step.change}</p>
                        <p>验证：{step.verifyCmd}</p>
                        <p>预期：{step.expected}</p>
                      </li>
                    ))}
                  </ol>
                  {currentPlan.replanIf.length > 0 && (
                    <div>
                      <h4>Re-plan if</h4>
                      <ul>
                        {currentPlan.replanIf.map((condition, index) => (
                          <li key={index}>
                            {condition.signal} → {condition.action}
                          </li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
          <h3>Execution</h3>
          {!executionsClient && (
            <p className="note">执行记录客户端尚未接入：待 App 接线传入客户端后，这里显示该任务的执行与 Effect 记录。</p>
          )}
          {executionsClient && (
            <div>
              <button onClick={() => void refreshExecutionData(current.taskId)} disabled={busy}>
                刷新执行记录
              </button>
              {execError !== "" && <p>错误：{execError}</p>}
              {taskExecutions.length === 0 && <p className="empty">该任务暂无执行记录。</p>}
              {taskExecutions.map((execution) => (
                <ExecutionDetail
                  key={execution.executionId}
                  execution={execution}
                  effects={taskEffects.filter((e) => e.executionId === execution.executionId)}
                  evidence={taskEvidence}
                  effectsClient={effectsClient}
                  executionsClient={executionsClient}
                  assignmentsClient={assignmentsClient}
                  mandatesClient={mandatesClient}
                  onRefresh={() => void refreshExecutionData(current.taskId)}
                  onError={(message) => setExecError(message)}
                />
              ))}
            </div>
          )}
          <MandatesSection mandates={mandatesClient} taskId={current.taskId} />
        </div>
      )}
      {artifactContent !== "" && (
        <div>
          <h3>产物内容</h3>
          <pre>{artifactContent}</pre>
        </div>
      )}
      {message !== "" && <p>{message}</p>}
      {error !== "" && <p>错误：{error}</p>}
      <h3>任务列表</h3>
      <ul>
        {taskList.map((t) => (
          <li key={t.taskId}>
            <button onClick={() => void handleSelect(t)} disabled={busy}>
              {t.taskId}
            </button>{" "}
            {t.directory || "（根）"} · {t.status}
            {t.artifactPath ? ` · ${t.artifactPath}` : ""}
          </li>
        ))}
      </ul>
    </div>
  );
}
