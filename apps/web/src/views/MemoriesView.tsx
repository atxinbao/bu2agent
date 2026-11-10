import { useEffect, useState } from "react";
import type {
  Memory,
  MemoryGateway,
  MemoryScope,
} from "../domain/memories_client";

// Memories view over the MemoryGateway interface (D1-5,
// minimal save/list/delete). A memory is written only
// by an explicit save — nothing extracts memories
// automatically. Every memory carries its scope triple
// and its provenance (author + session/execution/source
// ids); the user layer is global (no scope). Delete is
// a tombstone: the text stays on disk, so the view never
// claims "deleted from disk".
export function MemoriesView({ client }: { client: MemoryGateway }) {
  const [memories, setMemories] = useState<Memory[]>([]);
  const [selected, setSelected] = useState<Memory | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [layer, setLayer] = useState<"project" | "user">("project");
  const [content, setContent] = useState("");
  const [workspaceId, setWorkspaceId] = useState("");
  const [projectId, setProjectId] = useState("");
  const [sessionId, setSessionId] = useState("");
  const [author, setAuthor] = useState("");
  const [sourceSessionId, setSourceSessionId] = useState("");
  const [sourceExecutionId, setSourceExecutionId] = useState("");
  const [sourceSourceId, setSourceSourceId] = useState("");
  const [filterLayer, setFilterLayer] = useState("");
  const [filterWorkspaceId, setFilterWorkspaceId] = useState("");

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      setMemories(
        await client.listMemories({
          layer: filterLayer === "" ? undefined : filterLayer,
          workspaceId: filterWorkspaceId === "" ? undefined : filterWorkspaceId,
        }),
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "list failed");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function run(action: () => Promise<unknown>, done: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await action();
      setNotice(done);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "action failed");
    } finally {
      setBusy(false);
    }
  }

  async function select(memoryId: string) {
    setBusy(true);
    setError(null);
    try {
      setSelected(await client.getMemory(memoryId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "get failed");
    } finally {
      setBusy(false);
    }
  }

  function scopeFromForm(): MemoryScope {
    const scope: MemoryScope = {};
    if (workspaceId.trim() !== "") scope.workspaceId = workspaceId.trim();
    if (projectId.trim() !== "") scope.projectId = projectId.trim();
    if (sessionId.trim() !== "") scope.sessionId = sessionId.trim();
    return scope;
  }

  async function save() {
    if (content.trim() === "") {
      setError("content is required");
      return;
    }
    if (author.trim() === "") {
      setError("source.author is required");
      return;
    }
    const scope = scopeFromForm();
    await run(async () => {
      const saved = await client.saveMemory({
        layer,
        content,
        scope,
        // The caller declares its current scope; the
        // gateway rejects a save scoped elsewhere
        // (409 memory_scope_mismatch).
        currentScope: scope,
        source: {
          author: author.trim(),
          sessionId: sourceSessionId.trim() === "" ? undefined : sourceSessionId.trim(),
          executionId:
            sourceExecutionId.trim() === "" ? undefined : sourceExecutionId.trim(),
          sourceId: sourceSourceId.trim() === "" ? undefined : sourceSourceId.trim(),
        },
      });
      setSelected(saved);
      setContent("");
    }, "Memory saved.");
  }

  return (
    <div>
      <h2>Memories</h2>
      <p className="note">
        记忆只由显式保存写入，不自动提取。每条记忆带作用域三元组（workspace / project / session）与来源（作者 +
        session / execution / source）。user 层是全局的，不带作用域。删除是墓碑：文本保留在磁盘上，不称已从磁盘清除。断开来源会把该来源的记忆一并置墓碑。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p className="note">{notice}</p>}
      <div>
        <label>
          Layer{" "}
          <select
            value={layer}
            disabled={busy}
            onChange={(event) =>
              setLayer(event.target.value as "project" | "user")
            }
          >
            <option value="project">project</option>
            <option value="user">user</option>
          </select>
        </label>{" "}
        <label>
          Content{" "}
          <input
            type="text"
            value={content}
            disabled={busy}
            onChange={(event) => setContent(event.target.value)}
          />
        </label>
      </div>
      <div>
        <label>
          workspace_id{" "}
          <input
            type="text"
            value={workspaceId}
            disabled={busy}
            onChange={(event) => setWorkspaceId(event.target.value)}
          />
        </label>{" "}
        <label>
          project_id{" "}
          <input
            type="text"
            value={projectId}
            disabled={busy}
            onChange={(event) => setProjectId(event.target.value)}
          />
        </label>{" "}
        <label>
          session_id{" "}
          <input
            type="text"
            value={sessionId}
            disabled={busy}
            onChange={(event) => setSessionId(event.target.value)}
          />
        </label>
      </div>
      <div>
        <label>
          author{" "}
          <input
            type="text"
            value={author}
            disabled={busy}
            onChange={(event) => setAuthor(event.target.value)}
          />
        </label>{" "}
        <label>
          source session_id{" "}
          <input
            type="text"
            value={sourceSessionId}
            disabled={busy}
            onChange={(event) => setSourceSessionId(event.target.value)}
          />
        </label>{" "}
        <label>
          source execution_id{" "}
          <input
            type="text"
            value={sourceExecutionId}
            disabled={busy}
            onChange={(event) => setSourceExecutionId(event.target.value)}
          />
        </label>{" "}
        <label>
          source source_id{" "}
          <input
            type="text"
            value={sourceSourceId}
            disabled={busy}
            onChange={(event) => setSourceSourceId(event.target.value)}
          />
        </label>
      </div>
      <div>
        <button type="button" disabled={busy} onClick={() => void save()}>
          Save memory
        </button>
      </div>
      <div>
        <label>
          Filter layer{" "}
          <select
            value={filterLayer}
            disabled={busy}
            onChange={(event) => setFilterLayer(event.target.value)}
          >
            <option value="">all</option>
            <option value="project">project</option>
            <option value="user">user</option>
          </select>
        </label>{" "}
        <label>
          Filter workspace_id{" "}
          <input
            type="text"
            value={filterWorkspaceId}
            disabled={busy}
            onChange={(event) => setFilterWorkspaceId(event.target.value)}
          />
        </label>{" "}
        <button type="button" disabled={busy} onClick={() => void refresh()}>
          Refresh
        </button>
      </div>
      {memories.length === 0 ? (
        <p className="empty">No memories saved yet.</p>
      ) : (
        <ul>
          {memories.map((memory) => (
            <li key={memory.memoryId}>
              <button
                type="button"
                disabled={busy}
                onClick={() => void select(memory.memoryId)}
              >
                {memory.content} ({memory.memoryId}, {memory.layer}
                {memory.deleted ? ", tombstoned" : ""})
              </button>
            </li>
          ))}
        </ul>
      )}
      {selected !== null && (
        <div>
          <h3>{selected.content}</h3>
          <p>
            {selected.memoryId} · {selected.layer}
            {selected.deleted ? " · tombstoned" : ""}
          </p>
          <p>scope {selected.scopeJson}</p>
          <p>source {selected.sourceJson}</p>
          <p className="note">
            created {new Date(selected.createdMs).toLocaleString()} · origin{" "}
            {selected.originDeviceId}
          </p>
          {!selected.deleted && (
            <button
              type="button"
              disabled={busy}
              onClick={() =>
                void run(
                  async () => {
                    setSelected(await client.deleteMemory(selected.memoryId));
                  },
                  "Memory tombstoned (text stays on disk).",
                )
              }
            >
              Delete (tombstone)
            </button>
          )}
        </div>
      )}
    </div>
  );
}
