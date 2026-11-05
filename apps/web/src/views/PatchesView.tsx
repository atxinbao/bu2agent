import { useEffect, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type {
  EffectRecord,
  EffectsGateway,
} from "../domain/effects_client";
import type {
  EngineeringGateway,
  Patch,
  PatchDetail,
  PatchFile,
} from "../domain/engineering_client";
import type { EvidenceGateway, EvidenceRow } from "../domain/evidence_client";
import type { InboxGateway, InboxRow } from "../domain/inbox_client";
import { EffectBadge, effectNumber, latestEvidenceFor } from "./ExecutionDetail";

// P3-B 补丁审阅（方案 §4.7/§4.8）：一个 Patch 是同一轮
// 循环在同一个工作区的改动集合；一个 PatchFile 是一次提
// 议的 fs.write。diff 与新旧哈希由网关在提议时算好，
// new_text 不出网关，本页只展示 diff，不展示新文本。
// 审批复用现有 /approval/decide（逐文件批准/拒绝），
// 应用走 POST /patches/{id}/files/{file}/apply（只消费
// 已批准的审批；待批准时网关 403）。没有 Patch 级审批、
// 没有 Patch 级 Effect、没有新 Inbox 类；验证不通过的
// Effect 行内展示工作台 Inbox 的 verification_mismatch
// 行，与 RemindersView/NotesView 同一规则。

interface PatchesViewProps {
  engineering: EngineeringGateway;
  agent: AgentGateway;
  effects?: EffectsGateway | null;
  evidence?: EvidenceGateway | null;
  inbox?: InboxGateway | null;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

const PATCH_STATUSES = [
  "proposed",
  "partially_applied",
  "applied",
  "rejected",
  "stale",
] as const;

type Filter = "all" | (typeof PATCH_STATUSES)[number];

function patchStatusCopy(status: string): string {
  switch (status) {
    case "applied":
      return "全部文件已应用。";
    case "partially_applied":
      return "部分文件已应用，其余待处理或已拒绝——一个 Patch 是同一轮循环在同一个工作区的改动集合。";
    case "stale":
      return "至少一个文件在提议后被改动过，且本 Patch 尚未应用任何文件；被改动的文件需重新提议（新一轮循环），不能直接应用。";
    case "rejected":
      return "已拒绝：文件不再可应用。";
    default:
      return "待处理：文件尚未应用。";
  }
}

function fileStatusCopy(file: PatchFile): string {
  switch (file.status) {
    case "applied":
      return "已应用";
    case "failed":
      return "应用失败，可重试";
    case "rejected":
      return "已拒绝";
    case "stale":
      return "已过期：文件在提议后被改动过，需重新提议";
    default:
      return "待处理";
  }
}

export function PatchesView({
  engineering,
  agent,
  effects: effectsClient = null,
  evidence: evidenceClient = null,
  inbox: inboxClient = null,
}: PatchesViewProps) {
  const [patches, setPatches] = useState<Patch[]>([]);
  const [filter, setFilter] = useState<Filter>("all");
  const [selected, setSelected] = useState<PatchDetail | null>(null);
  const [effectRows, setEffectRows] = useState<EffectRecord[]>([]);
  const [effectEvidence, setEffectEvidence] = useState<EvidenceRow[]>([]);
  const [mismatchRows, setMismatchRows] = useState<InboxRow[]>([]);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function refreshPatches() {
    try {
      const latest = await engineering.patchList(
        filter === "all" ? null : filter,
      );
      setPatches(latest);
      return latest;
    } catch (e) {
      setError(`补丁列表读取失败：${errText(e)}`);
      return null;
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

  async function refreshMismatchRows() {
    if (!inboxClient) return;
    try {
      setMismatchRows(
        await inboxClient.list("verification_mismatch", true),
      );
    } catch (e) {
      setError(`Inbox 读取失败：${errText(e)}`);
    }
  }

  async function refreshAll() {
    await refreshPatches();
    await refreshEffectRows();
    await refreshMismatchRows();
  }

  useEffect(() => {
    void refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filter]);

  async function openPatch(patch: Patch) {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      setSelected(await engineering.patchGet(patch.patchId));
    } catch (e) {
      setError(`补丁读取失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  function effectForFile(file: PatchFile): EffectRecord | null {
    return effectRows.find((e) => e.effectId === file.effectId) ?? null;
  }

  function mismatchForFile(file: PatchFile): InboxRow | null {
    return (
      mismatchRows.find((r) => r.effectId === file.effectId) ?? null
    );
  }

  // 逐文件批准并应用：先走现有 decide（已批准时网关报
  // "approval already decided"，属预期内，直接继续），
  // 再走 apply 命令——apply 只消费已批准的审批，待批准
  // 时网关 403，真正的门在网关不在本页。
  async function handleApproveAndApply(file: PatchFile) {
    if (!file.approvalId) {
      setError(`文件 ${file.pathRelative} 没有审批记录，无法应用。`);
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      try {
        await agent.decideApproval(file.approvalId, "approve");
      } catch (e) {
        if (!errText(e).includes("already decided")) throw e;
      }
      const result = await engineering.patchApply(
        file.patchId,
        file.patchFileId,
        file.oldSha256,
        file.approvalId,
      );
      await refreshAll();
      if (selected) {
        setSelected(await engineering.patchGet(file.patchId));
      }
      setMessage(
        result.replayed
          ? `文件 ${file.pathRelative} 已是应用过的（重放，未重复写盘）。`
          : `文件 ${file.pathRelative} 已应用（Effect ${result.effect.effectId}）。`,
      );
    } catch (e) {
      setError(`应用失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleRejectFile(file: PatchFile) {
    if (!file.approvalId) {
      setError(`文件 ${file.pathRelative} 没有审批记录，无法拒绝。`);
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      await agent.decideApproval(file.approvalId, "reject");
      await refreshAll();
      if (selected) {
        setSelected(await engineering.patchGet(file.patchId));
      }
      setMessage(`文件 ${file.pathRelative} 已拒绝。`);
    } catch (e) {
      setError(`拒绝失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleRejectPatch(patch: Patch) {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const detail = await engineering.patchReject(patch.patchId);
      setSelected(detail);
      await refreshPatches();
      setMessage(`Patch ${patch.patchId} 已拒绝。`);
    } catch (e) {
      setError(`拒绝失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <section>
      <h2>补丁</h2>
      <p className="note">
        审阅循环提议的文件改动，按文件批准并应用。一个 Patch
        是同一轮循环在同一个工作区的改动集合；不相关的改动请
        另起一轮循环，不要混进同一个 Patch。
      </p>
      <p className="note">
        状态说明：proposed 待处理；partially_applied 部分文件已
        应用；applied 全部已应用；stale 有文件在提议后被改动过
        且本 Patch 未应用任何文件（需重新提议，不能直接应用）；
        rejected 已拒绝，不再可应用。已应用的文件再次应用是
        重放，不重复写盘。
      </p>
      <div className="row">
        <button type="button" onClick={() => void refreshAll()} disabled={busy}>
          刷新补丁
        </button>
        <button type="button" onClick={() => setFilter("all")} disabled={filter === "all"}>
          全部
        </button>
        {PATCH_STATUSES.map((status) => (
          <button
            key={status}
            type="button"
            onClick={() => setFilter(status)}
            disabled={filter === status}
          >
            {status}
          </button>
        ))}
      </div>
      {error !== "" && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {message !== "" && <p>{message}</p>}
      {patches.length === 0 && <p className="empty">该筛选下暂无补丁。</p>}
      <ul>
        {patches.map((patch) => (
          <li key={patch.patchId}>
            <p>
              {patch.patchId} · {patch.status} · 执行 {patch.executionId} · 工作区{" "}
              {patch.worktreeId}
              {patch.projectId ? ` · 项目 ${patch.projectId}` : ""}
            </p>
            <p className="note">{patchStatusCopy(patch.status)}</p>
            <button
              type="button"
              onClick={() => void openPatch(patch)}
              disabled={busy}
            >
              {selected?.patch.patchId === patch.patchId ? "收起" : "查看文件"}
            </button>
            {patch.status !== "applied" && patch.status !== "rejected" && (
              <button
                type="button"
                onClick={() => void handleRejectPatch(patch)}
                disabled={busy}
              >
                拒绝整个 Patch
              </button>
            )}
            {selected?.patch.patchId === patch.patchId && (
              <ul>
                {selected.files.map((file) => {
                  const effect = effectForFile(file);
                  const mismatch = mismatchForFile(file);
                  return (
                    <li key={file.patchFileId}>
                      <p>
                        {file.pathRelative} · {fileStatusCopy(file)} ·{" "}
                        {file.oldSha256 === null
                          ? "新建文件"
                          : `旧 ${file.oldSha256.slice(0, 12)}…`}
                        {" → "}
                        新 {file.newSha256.slice(0, 12)}…
                      </p>
                      <pre>{file.diff}</pre>
                      {file.status === "pending" && (
                        <span>
                          <button
                            type="button"
                            onClick={() => void handleApproveAndApply(file)}
                            disabled={busy}
                          >
                            批准并应用
                          </button>{" "}
                          <button
                            type="button"
                            onClick={() => void handleRejectFile(file)}
                            disabled={busy}
                          >
                            拒绝
                          </button>
                        </span>
                      )}
                      {file.status === "failed" && (
                        <button
                          type="button"
                          onClick={() => void handleApproveAndApply(file)}
                          disabled={busy}
                        >
                          重试应用
                        </button>
                      )}
                      {effectsClient && (
                        <p>
                          Effect：
                          {effect ? (
                            <EffectBadge
                              effect={effect}
                              latestEvidence={latestEvidenceFor(
                                effect.effectId,
                                effectEvidence,
                              )}
                              effectsClient={effectsClient}
                              onRefresh={() => void refreshEffectRows()}
                              onError={(message) => setError(message)}
                            />
                          ) : (
                            "暂无对应 Effect 记录。"
                          )}
                        </p>
                      )}
                      {mismatch && (
                        <p className="note">
                          验证不通过（Inbox verification_mismatch，
                          {mismatch.inboxId}）：该 Effect 的最新验证与
                          预期不一致，请在工作台 Inbox 里处理，不要直接
                          重试应用。
                        </p>
                      )}
                    </li>
                  );
                })}
              </ul>
            )}
          </li>
        ))}
      </ul>
    </section>
  );
}
