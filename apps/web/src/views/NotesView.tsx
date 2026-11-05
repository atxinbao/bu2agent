import { useEffect, useState } from "react";
import type { FileGateway, GatewayFileEntry } from "../domain/file_client";
import type { EffectRecord, EffectsGateway } from "../domain/effects_client";
import type { EvidenceGateway, EvidenceRow } from "../domain/evidence_client";
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
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
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
    </section>
  );
}
