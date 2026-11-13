import { useEffect, useState } from "react";
import type { FileGateway, GatewayFileEntry } from "../domain/file_client";
import type { EffectRecord, EffectsGateway } from "../domain/effects_client";
import type { EvidenceGateway, EvidenceRow } from "../domain/evidence_client";
import type { AgentGateway } from "../domain/agent_client";
import type {
  AppendNoteInput,
  CreateNoteInput,
  Note,
  NotesGateway,
  NoteSyncResult,
} from "../domain/notes_client";
import { EffectBadge, effectNumber, latestEvidenceFor } from "./ExecutionDetail";

// P2 邮件/笔记（基线 P2）：草稿 Artifact 的 Web 视图，只做 UI。
// 草稿就是文件根目录 drafts/ 下的 markdown 文件；列表、打开、
// 编辑、保存全部走现有 FileGateway，不新增客户端方法。保存是
// UI 直接路（files.write，approved=true）：用户点保存本身就是
// 授权，与任何直接文件写相同，不另弹审批；写出的 Effect 就是
// fs.write 的 Effect（方案 §3.3），行内展示复用 P0-R 的共享
// EffectBadge（含验证按钮）。引用只以纯文本展示，不做引用
// 点击跳转（方案 §10-7，延后）。
//
// 注意区分：这里的草稿是真实文件；mock 的 notes.create /
// mail.send 与此无关——它们只往内存列表里追加假数据，不是
// 真实发送、也不是真实笔记（方案 §7 风险要求，不得让用户
// 把 mock 当成真实发送）。

const DRAFTS_DIR = "drafts";
const CHUNK_LEN = 4096;
const MAX_TOTAL = 1048576;

interface NotesViewProps {
  files: FileGateway;
  effects?: EffectsGateway | null;
  evidence?: EvidenceGateway | null;
  notes?: NotesGateway | null;
  agent?: AgentGateway | null;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function newIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `key-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
}

interface PendingCreate {
  folder: string;
  name: string;
  body: string;
  idempotencyKey: string;
  toolCallId: string;
}

interface PendingAppend {
  noteId: string;
  text: string;
  baseBodyHash: string;
  idempotencyKey: string;
  toolCallId: string;
}

interface PendingDelete {
  noteId: string;
  name: string;
  toolCallId: string;
}

function draftPath(ms: number): string {
  return `${DRAFTS_DIR}/draft-${ms}.md`;
}

function firstLine(text: string): string {
  const line = text.split("\n")[0]?.trim() ?? "";
  return line === "" ? "（空草稿）" : line;
}

export function NotesView({
  files,
  effects: effectsClient = null,
  evidence: evidenceClient = null,
  notes: notesClient = null,
  agent: agentClient = null,
}: NotesViewProps) {
  const [entries, setEntries] = useState<GatewayFileEntry[]>([]);
  const [titles, setTitles] = useState<Record<string, string>>({});
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [selectedText, setSelectedText] = useState("");
  const [editText, setEditText] = useState("");
  const [editing, setEditing] = useState(false);
  const [effectRows, setEffectRows] = useState<EffectRecord[]>([]);
  const [effectEvidence, setEffectEvidence] = useState<EvidenceRow[]>([]);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  // D1-6b-4: real Notes.app notes (fact rows, body hash
  // only — the body never leaves Notes.app in this view).
  const [realNotes, setRealNotes] = useState<Note[]>([]);
  const [selectedNote, setSelectedNote] = useState<Note | null>(null);
  const [folderInput, setFolderInput] = useState("");
  const [syncResult, setSyncResult] = useState<NoteSyncResult | null>(null);
  const [createNameInput, setCreateNameInput] = useState("");
  const [createBodyInput, setCreateBodyInput] = useState("");
  const [appendTextInput, setAppendTextInput] = useState("");
  const [pendingCreate, setPendingCreate] = useState<PendingCreate | null>(null);
  const [pendingAppend, setPendingAppend] = useState<PendingAppend | null>(null);
  const [pendingDelete, setPendingDelete] = useState<PendingDelete | null>(null);

  async function refreshDrafts(): Promise<GatewayFileEntry[]> {
    try {
      const listed = (await files.list(DRAFTS_DIR)).filter((e) => !e.isDir);
      listed.sort((a, b) => (a.name < b.name ? 1 : a.name > b.name ? -1 : 0));
      setEntries(listed);
      // 每行首行标题：逐篇读首块取首行，单篇读失败只影响该行标题。
      const nextTitles: Record<string, string> = {};
      await Promise.all(
        listed.map(async (entry) => {
          try {
            nextTitles[entry.path] = firstLine(
              await files.readText(entry.path, CHUNK_LEN, MAX_TOTAL),
            );
          } catch {
            nextTitles[entry.path] = "（标题读取失败）";
          }
        }),
      );
      setTitles(nextTitles);
      return listed;
    } catch (e) {
      // drafts/ 目录不存在时视为空列表，不报错刷屏；其他
      // 失败（如网关不可达）仍如实报错。
      const text = errText(e);
      if (/404|not found|NotFound/i.test(text)) {
        setEntries([]);
        setTitles({});
        return [];
      }
      setError(`草稿列表读取失败：${text}`);
      return [];
    }
  }

  async function refreshEffectRows() {
    if (!effectsClient || !evidenceClient) return;
    try {
      const [allEffects, allEvidence] = await Promise.all([
        effectsClient.list(),
        evidenceClient.list(),
      ]);
      setEffectRows(allEffects);
      setEffectEvidence(allEvidence);
    } catch (e) {
      setError(`Effect 记录读取失败：${errText(e)}`);
    }
  }

  async function refreshAll() {
    await refreshDrafts();
    await refreshEffectRows();
  }

  useEffect(() => {
    void refreshAll();
    void refreshRealNotes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 该草稿的最新 Effect：按 scope（草稿路径）匹配最新一条，
  // 与 FilesView 的按 scope 找 Effect 同一规则。
  function effectForDraft(path: string): EffectRecord | null {
    const matches = effectRows.filter((e) => e.scope === path);
    matches.sort((a, b) => effectNumber(b.effectId) - effectNumber(a.effectId));
    return matches.length === 0 ? null : matches[0];
  }

  async function openDraft(entry: GatewayFileEntry) {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const text = await files.readText(entry.path, CHUNK_LEN, MAX_TOTAL);
      setSelectedPath(entry.path);
      setSelectedText(text);
      setEditText(text);
      setEditing(false);
    } catch (e) {
      setError(`打开草稿失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleNewDraft() {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const listed = await refreshDrafts();
      const existing = new Set(listed.map((e) => e.path));
      let ms = Date.now();
      let path = draftPath(ms);
      if (existing.has(path)) {
        // 毫秒时间戳防碰撞；若该名已存在，毫秒+1 重试一次。
        ms += 1;
        path = draftPath(ms);
        if (existing.has(path)) {
          throw new Error("草稿文件名已存在，请稍后再试");
        }
      }
      const initial = "# 草稿\n";
      await files.write(path, initial, true);
      await refreshDrafts();
      await refreshEffectRows();
      setSelectedPath(path);
      setSelectedText(initial);
      setEditText(initial);
      setEditing(true);
      setMessage(`已新建草稿 ${path}。`);
    } catch (e) {
      setError(`新建草稿失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleSave() {
    if (selectedPath === null) return;
    setError("");
    setMessage("");
    setBusy(true);
    try {
      // UI 直接路：用户点保存本身就是授权，不另弹审批。
      await files.write(selectedPath, editText, true);
      setSelectedText(editText);
      setEditing(false);
      await refreshDrafts();
      await refreshEffectRows();
      setMessage(`已保存 ${selectedPath}。`);
    } catch (e) {
      setError(`保存失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  const selectedEffect =
    selectedPath === null ? null : effectForDraft(selectedPath);

  // ---- D1-6b-4: real Notes.app notes ----
  //
  // Sync is manual only; the folder must pre-exist in
  // Notes.app (no whole-library default). Create /
  // append / delete go through the digest-bound approval
  // flow (approval_request -> decide -> write), the
  // CalendarView pending pattern: the pending copy is
  // frozen at request time, so editing the form
  // afterwards cannot change what the approval bound.
  // Append 409s `note_changed` when the note was edited
  // in Notes.app since the base was read — refresh the
  // list (fresh base hash) and retry.
  async function refreshRealNotes() {
    if (!notesClient) return;
    try {
      setRealNotes(await notesClient.listNotes());
    } catch (e) {
      setError(`真实笔记列表读取失败：${errText(e)}`);
    }
  }

  async function handleSyncNotes() {
    if (!notesClient) return;
    if (folderInput === "") {
      setError("同步需要一个已存在的文件夹名（Notes.app 里先建好）。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const result = await notesClient.syncNotes(folderInput);
      setSyncResult(result);
      await refreshRealNotes();
      setMessage(
        `已同步 ${folderInput}：新增 ${result.syncedCount}，跳过 ${result.skippedCount}，锁定的 ${result.lockedCount} 未同步。`,
      );
    } catch (e) {
      setError(`同步失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function selectRealNote(noteId: string) {
    if (!notesClient) return;
    setError("");
    setMessage("");
    setBusy(true);
    try {
      setSelectedNote(await notesClient.getNote(noteId));
      setAppendTextInput("");
    } catch (e) {
      setError(`打开笔记失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function requestCreateApproval() {
    if (!agentClient) return;
    if (folderInput === "" || createNameInput === "" || createBodyInput === "") {
      setError("新建笔记需要文件夹、标题、正文。");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const request = await agentClient.requestApproval("notes.create", {
        folder: folderInput,
        name: createNameInput,
        body: createBodyInput,
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingCreate({
        folder: folderInput,
        name: createNameInput,
        body: createBodyInput,
        idempotencyKey: newIdempotencyKey(),
        toolCallId: request.tool_call_id,
      });
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function approveAndCreate() {
    if (!notesClient || !agentClient || pendingCreate === null) return;
    setBusy(true);
    setError("");
    try {
      await agentClient.decideApproval(pendingCreate.toolCallId, "approve");
      const input: CreateNoteInput = {
        folder: pendingCreate.folder,
        name: pendingCreate.name,
        body: pendingCreate.body,
        idempotencyKey: pendingCreate.idempotencyKey,
        toolCallId: pendingCreate.toolCallId,
      };
      const row = await notesClient.createNote(input);
      setPendingCreate(null);
      setCreateNameInput("");
      setCreateBodyInput("");
      await refreshRealNotes();
      await refreshEffectRows();
      setMessage(`已新建笔记 ${row.noteId}（${row.name}）。`);
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function rejectCreate() {
    if (!agentClient || pendingCreate === null) return;
    try {
      await agentClient.decideApproval(pendingCreate.toolCallId, "reject");
    } catch (e) {
      setError(errText(e));
    } finally {
      setPendingCreate(null);
    }
  }

  async function requestAppendApproval() {
    if (!agentClient || selectedNote === null) return;
    if (appendTextInput === "") {
      setError("追加内容不能为空。");
      return;
    }
    setBusy(true);
    setError("");
    try {
      const request = await agentClient.requestApproval("notes.append", {
        note_id: selectedNote.noteId,
        base_body_hash: selectedNote.bodyHash,
        body: appendTextInput,
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingAppend({
        noteId: selectedNote.noteId,
        text: appendTextInput,
        baseBodyHash: selectedNote.bodyHash,
        idempotencyKey: newIdempotencyKey(),
        toolCallId: request.tool_call_id,
      });
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function approveAndAppend() {
    if (!notesClient || !agentClient || pendingAppend === null) return;
    setBusy(true);
    setError("");
    try {
      await agentClient.decideApproval(pendingAppend.toolCallId, "approve");
      const input: AppendNoteInput = {
        text: pendingAppend.text,
        baseBodyHash: pendingAppend.baseBodyHash,
        idempotencyKey: pendingAppend.idempotencyKey,
        toolCallId: pendingAppend.toolCallId,
      };
      const row = await notesClient.appendNote(pendingAppend.noteId, input);
      setPendingAppend(null);
      setAppendTextInput("");
      await refreshRealNotes();
      await refreshEffectRows();
      setSelectedNote(row);
      setMessage(`已追加到 ${row.noteId}。`);
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function rejectAppend() {
    if (!agentClient || pendingAppend === null) return;
    try {
      await agentClient.decideApproval(pendingAppend.toolCallId, "reject");
    } catch (e) {
      setError(errText(e));
    } finally {
      setPendingAppend(null);
    }
  }

  async function requestDeleteApproval() {
    if (!agentClient || selectedNote === null) return;
    setBusy(true);
    setError("");
    try {
      const request = await agentClient.requestApproval("notes.delete", {
        note_id: selectedNote.noteId,
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingDelete({
        noteId: selectedNote.noteId,
        name: selectedNote.name,
        toolCallId: request.tool_call_id,
      });
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function approveAndDelete() {
    if (!notesClient || !agentClient || pendingDelete === null) return;
    setBusy(true);
    setError("");
    try {
      await agentClient.decideApproval(pendingDelete.toolCallId, "approve");
      await notesClient.deleteNote(pendingDelete.noteId, pendingDelete.toolCallId);
      setPendingDelete(null);
      setSelectedNote(null);
      await refreshRealNotes();
      await refreshEffectRows();
      setMessage(`已删除 ${pendingDelete.noteId}（移到“最近删除”，事实行保留删除标记）。`);
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function rejectDelete() {
    if (!agentClient || pendingDelete === null) return;
    try {
      await agentClient.decideApproval(pendingDelete.toolCallId, "reject");
    } catch (e) {
      setError(errText(e));
    } finally {
      setPendingDelete(null);
    }
  }

  // The selected real note's latest Effect: append /
  // delete scope is the note id, create scope is the
  // idempotency key.
  function effectForRealNote(note: Note): EffectRecord | null {
    const matches = effectRows.filter(
      (e) => e.scope === note.noteId || e.scope === note.idempotencyKey,
    );
    matches.sort((a, b) => effectNumber(b.effectId) - effectNumber(a.effectId));
    return matches.length === 0 ? null : matches[0];
  }

  const selectedRealEffect =
    selectedNote === null ? null : effectForRealNote(selectedNote);

  return (
    <section>
      <h2>Notes</h2>
      <p className="note">
        草稿是真实文件，存在文件根目录的 drafts/ 下，可在 Files
        页签看到同一文件。行内 Effect 与工作台 Inbox 里的同一
        effect id 是同一条记录，不另开页面。
      </p>
      <p className="note">
        注意：mock 的 notes.create / mail.send 与这里的草稿无关——
        它们只往内存里追加假数据，不是真实发送，也不是真实笔记。
        本页不发送任何邮件，引用只以纯文本展示，不做引用点击跳转。
      </p>
      <div className="row">
        <button type="button" onClick={() => void refreshAll()} disabled={busy}>
          刷新草稿
        </button>
        <button type="button" onClick={() => void handleNewDraft()} disabled={busy}>
          新建草稿
        </button>
      </div>
      {error !== "" && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {message !== "" && <p>{message}</p>}
      {!effectsClient && (
        <p className="note">Effect 客户端尚未接入：待 App 接线传入客户端后，这里显示每篇草稿的最新 Effect 记录。</p>
      )}
      {entries.length === 0 && <p className="empty">暂无草稿。</p>}
      <ul>
        {entries.map((entry) => (
          <li key={entry.path}>
            <p>
              {entry.name} · {titles[entry.path] ?? ""}
            </p>
            <button
              type="button"
              onClick={() => void openDraft(entry)}
              disabled={busy}
            >
              打开
            </button>
          </li>
        ))}
      </ul>
      {selectedPath !== null && (
        <div>
          <h3>{selectedPath}</h3>
          {editing ? (
            <div>
              <textarea
                value={editText}
                onChange={(event) => setEditText(event.target.value)}
              />
              <div className="row">
                <button type="button" onClick={() => void handleSave()} disabled={busy}>
                  保存
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setEditText(selectedText);
                    setEditing(false);
                  }}
                  disabled={busy}
                >
                  取消
                </button>
              </div>
            </div>
          ) : (
            <div>
              <pre className="result">{selectedText}</pre>
              <button
                type="button"
                onClick={() => setEditing(true)}
                disabled={busy}
              >
                编辑
              </button>
            </div>
          )}
          {effectsClient && (
            <p>
              Effect：
              {selectedEffect ? (
                <EffectBadge
                  effect={selectedEffect}
                  latestEvidence={latestEvidenceFor(selectedEffect.effectId, effectEvidence)}
                  effectsClient={effectsClient}
                  onRefresh={() => void refreshEffectRows()}
                  onError={(message) => setError(message)}
                />
              ) : (
                "暂无对应 Effect 记录。"
              )}
            </p>
          )}
        </div>
      )}
      <div>
        <h3>真实笔记（Notes.app）</h3>
        <p className="note">
          这里是 Notes.app 里的真实笔记，不是上面的草稿文件，也不是
          mock 数据。列表只显示标题与正文哈希，不显示正文内容；
          同步只记录新增笔记（正文哈希），不回填、不更新已同步的
          笔记。删除只移到“最近删除”，事实行保留删除标记，不会
          真正抹掉。新建、追加、删除都要先审批（审批绑定 exact
          内容），未审批不执行。
        </p>
        {!notesClient && (
          <p className="note">真实笔记客户端尚未接入：待 App 接线传入客户端后，这里显示 Notes.app 的笔记。</p>
        )}
        {notesClient && (
          <div>
            <div className="row">
              <label>
                文件夹（Notes.app 里已存在）
                <input
                  type="text"
                  value={folderInput}
                  onChange={(event) => setFolderInput(event.target.value)}
                  placeholder="例如 bu2agent-test"
                />
              </label>
              <button type="button" onClick={() => void handleSyncNotes()} disabled={busy}>
                同步该文件夹
              </button>
              <button type="button" onClick={() => void refreshRealNotes()} disabled={busy}>
                刷新列表
              </button>
            </div>
            {syncResult !== null && (
              <p>
                上次同步：新增 {syncResult.syncedCount}，跳过 {syncResult.skippedCount}，
                锁定的 {syncResult.lockedCount} 未同步，共 {syncResult.totalCount}。
              </p>
            )}
            <div className="row">
              <label>
                新建标题
                <input
                  type="text"
                  value={createNameInput}
                  onChange={(event) => setCreateNameInput(event.target.value)}
                />
              </label>
              <label>
                新建正文
                <textarea
                  value={createBodyInput}
                  onChange={(event) => setCreateBodyInput(event.target.value)}
                />
              </label>
              <button type="button" onClick={() => void requestCreateApproval()} disabled={busy}>
                申请新建审批
              </button>
            </div>
            {pendingCreate !== null && (
              <div>
                <p>
                  待审批新建：{pendingCreate.name}（文件夹 {pendingCreate.folder}）
                </p>
                <div className="row">
                  <button type="button" onClick={() => void approveAndCreate()} disabled={busy}>
                    批准并新建
                  </button>
                  <button type="button" onClick={() => void rejectCreate()} disabled={busy}>
                    拒绝
                  </button>
                </div>
              </div>
            )}
            {realNotes.length === 0 && <p className="empty">暂无已同步的真实笔记。</p>}
            <ul>
              {realNotes.map((note) => (
                <li key={note.noteId}>
                  <p>
                    {note.noteId} · {note.name}
                    {note.deleted ? "（已删除）" : ""}
                  </p>
                  <p className="note">
                    文件夹 {note.folder} · 正文哈希 {note.bodyHash.slice(0, 12)}…
                  </p>
                  <button
                    type="button"
                    onClick={() => void selectRealNote(note.noteId)}
                    disabled={busy}
                  >
                    打开
                  </button>
                </li>
              ))}
            </ul>
            {selectedNote !== null && (
              <div>
                <h3>
                  {selectedNote.noteId} · {selectedNote.name}
                  {selectedNote.deleted ? "（已删除）" : ""}
                </h3>
                <p className="note">
                  文件夹 {selectedNote.folder} · 正文哈希 {selectedNote.bodyHash.slice(0, 12)}…
                  · 更新于 {new Date(selectedNote.updatedMs).toLocaleString()}
                </p>
                {effectsClient && (
                  <p>
                    Effect：
                    {selectedRealEffect ? (
                      <EffectBadge
                        effect={selectedRealEffect}
                        latestEvidence={latestEvidenceFor(selectedRealEffect.effectId, effectEvidence)}
                        effectsClient={effectsClient}
                        onRefresh={() => void refreshEffectRows()}
                        onError={(message) => setError(message)}
                      />
                    ) : (
                      "暂无对应 Effect 记录。"
                    )}
                  </p>
                )}
                {!selectedNote.deleted && (
                  <div>
                    <div className="row">
                      <label>
                        追加内容（直接拼在现有正文后，不加分隔符）
                        <textarea
                          value={appendTextInput}
                          onChange={(event) => setAppendTextInput(event.target.value)}
                        />
                      </label>
                      <button type="button" onClick={() => void requestAppendApproval()} disabled={busy}>
                        申请追加审批
                      </button>
                    </div>
                    <p className="note">
                      若提示 note_changed，说明这篇笔记在 Notes.app 里被改过：
                      刷新列表拿到新的正文哈希后，用新 base 重试追加。
                    </p>
                    {pendingAppend !== null && (
                      <div>
                        <p>待审批追加到 {pendingAppend.noteId}。</p>
                        <div className="row">
                          <button type="button" onClick={() => void approveAndAppend()} disabled={busy}>
                            批准并追加
                          </button>
                          <button type="button" onClick={() => void rejectAppend()} disabled={busy}>
                            拒绝
                          </button>
                        </div>
                      </div>
                    )}
                    <div className="row">
                      <button type="button" onClick={() => void requestDeleteApproval()} disabled={busy}>
                        申请删除审批
                      </button>
                    </div>
                    {pendingDelete !== null && (
                      <div>
                        <p>待审批删除：{pendingDelete.name}（{pendingDelete.noteId}）。</p>
                        <div className="row">
                          <button type="button" onClick={() => void approveAndDelete()} disabled={busy}>
                            批准并删除
                          </button>
                          <button type="button" onClick={() => void rejectDelete()} disabled={busy}>
                            拒绝
                          </button>
                        </div>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
          </div>
        )}
      </div>
    </section>
  );
}
