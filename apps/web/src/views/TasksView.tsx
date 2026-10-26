import { useEffect, useState } from "react";
import type { ChatGateway } from "../domain/gateway_client";
import { initialChatState, runTurn } from "../domain/chat";
import type { ChatState } from "../domain/chat";
import type { FileGateway, GatewayFileEntry } from "../domain/file_client";
import type { AgentGateway } from "../domain/agent_client";
import type { TaskGateway, TaskRecord } from "../domain/task_client";

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
// Honest boundary, same as the chat line: without a model key
// the generate step returns the echo, so the loop runs
// mechanically but a real report needs a configured key.

interface TasksViewProps {
  chat: ChatGateway;
  files: FileGateway;
  agent: AgentGateway;
  tasks: TaskGateway;
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

  async function refreshTasks() {
    try {
      setTaskList(await tasks.list());
    } catch (e) {
      setError(`任务列表读取失败：${errText(e)}`);
    }
  }

  useEffect(() => {
    void refreshTasks();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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
    setBusy(true);
    try {
      const fresh = await tasks.get(task.taskId);
      setCurrent(fresh);
      setDraft("");
      setArtifactContent("");
      if (fresh.artifactPath) {
        setArtifactPath(fresh.artifactPath);
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
          </p>
          <div>
            <button onClick={() => void handleGenerate()} disabled={busy}>
              读取并生成
            </button>
          </div>
          {draft !== "" && (
            <div>
              <h3>草稿</h3>
              <pre>{draft}</pre>
              <div>
                <input
                  value={artifactPath}
                  onChange={(e) => setArtifactPath(e.target.value)}
                  placeholder="产物路径，如 report.md"
                />
                <button onClick={() => void handleApproveWrite()} disabled={busy}>
                  批准并写入
                </button>
                <button onClick={() => void handleReject()} disabled={busy}>
                  拒绝
                </button>
              </div>
            </div>
          )}
          {current.artifactPath && (
            <div>
              <button onClick={() => void handleViewArtifact(current)} disabled={busy}>
                查看产物
              </button>
            </div>
          )}
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
