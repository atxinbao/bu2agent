import { useState } from "react";

// Execution detail, shared by the Workbench Inbox rows and the
// Tasks Execution section (P0-R R5, minimal binding, no new
// page). It renders gateway rows only: the parent loads the
// lists once and filters them client-side by id, so every view
// shows the same Execution / Effect / Evidence rows.
//
// Wiring note: the views take the domain clients as optional
// props (App.tsx still needs to pass them — it was outside
// this task's file scope, so until it does the sections below
// say the client is not wired yet and the rest of each view
// works as before). The views never construct an HTTP or
// Invoke client themselves, so they follow App.tsx's existing
// transport choice (invoke in the Tauri shell, HTTP in browser
// preview) instead of inventing a third fetch path.
// Presentation words come from domain/effect_presentation.ts
// (§3.2 table, presentation layer only); no mapping is copied
// into this file.
import {
  nextStepForReason,
  presentEffectStatus,
} from "../domain/effect_presentation";
import type {
  EffectRecord,
  EffectsGateway,
} from "../domain/effects_client";
import type {
  ExecutionRecord,
  ExecutionsGateway,
} from "../domain/executions_client";
import type { EvidenceRow } from "../domain/evidence_client";
import type { AssignmentsGateway } from "../domain/assignments_client";

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// Effect ids are `effect-N`; numeric order is the list order
// the gateway itself serves. Used only to pick the latest row
// among client-side filtered rows, never to derive a status.
export function effectNumber(effectId: string): number {
  const n = Number(effectId.replace(/^effect-/, ""));
  return Number.isFinite(n) ? n : 0;
}

export function latestEvidenceFor(
  effectId: string,
  evidence: EvidenceRow[],
): EvidenceRow | null {
  const rows = evidence.filter((row) => row.effectId === effectId);
  if (rows.length === 0) return null;
  // Evidence is append-only; the latest row is the current
  // verification state. createdMs is wall-clock display time,
  // so order by the id sequence, not by the clock.
  rows.sort((a, b) => {
    const an = Number(a.evidenceId.replace(/^evidence-/, ""));
    const bn = Number(b.evidenceId.replace(/^evidence-/, ""));
    const av = Number.isFinite(an) ? an : 0;
    const bv = Number.isFinite(bn) ? bn : 0;
    return bv - av;
  });
  return rows[0];
}

interface EffectBadgeProps {
  effect: EffectRecord;
  latestEvidence: EvidenceRow | null;
  effectsClient?: EffectsGateway | null;
  showActions?: boolean;
  onRefresh?: () => void;
  onError?: (message: string) => void;
}

// One Effect line: status word (via the presentation call
// site), latest Evidence verdict badge, and the actions the
// R5 scope allows on this row — verify on succeeded rows,
// reconcile on unknown rows. Server 409 text is passed
// through verbatim; the client never pre-judges an edge.
export function EffectBadge(props: EffectBadgeProps) {
  const { effect, latestEvidence, effectsClient, showActions = true, onRefresh, onError } = props;
  const [busy, setBusy] = useState(false);
  const label = presentEffectStatus(
    effect.status,
    latestEvidence?.verdict ?? null,
    effect.reconciliation?.result ?? null,
  );
  const nextStep = latestEvidence ? nextStepForReason(latestEvidence.reason) : "";

  async function run(action: () => Promise<unknown>, failurePrefix: string) {
    if (!effectsClient) return;
    setBusy(true);
    try {
      await action();
      onRefresh?.();
    } catch (e) {
      onError?.(`${failurePrefix}：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  return (
    <span>
      {effect.effectId} · {effect.capability}.{effect.operation} · {effect.scope} · {label}
      {latestEvidence && (
        <span> · 最新验证：{latestEvidence.verdict}（{latestEvidence.reason}）</span>
      )}
      {nextStep !== "" && <span> · 下一步：{nextStep}</span>}
      {showActions && effectsClient && effect.status === "succeeded" && (
        <button
          type="button"
          disabled={busy}
          onClick={() => void run(() => effectsClient.verify(effect.effectId), "验证失败")}
        >
          验证
        </button>
      )}
      {showActions && effectsClient && effect.status === "unknown" && (
        <button
          type="button"
          disabled={busy}
          onClick={() => void run(() => effectsClient.reconcile(effect.effectId), "核查失败")}
        >
          核查
        </button>
      )}
    </span>
  );
}

export interface ExecutionDetailProps {
  execution: ExecutionRecord;
  effects: EffectRecord[];
  evidence: EvidenceRow[];
  effectsClient?: EffectsGateway | null;
  executionsClient?: ExecutionsGateway | null;
  assignmentsClient?: AssignmentsGateway | null;
  showActions?: boolean;
  onRefresh?: () => void;
  onError?: (message: string) => void;
}

// Execution detail (not a page): header + its Effect list.
// Takeover is shown on recoverable rows and disabled while an
// assignment is active; transition is offered only on
// needs_review rows (resume to running, or mark a terminal
// state). Illegal edges are left to the server's 409, whose
// text is passed through by the caller.
export function ExecutionDetail(props: ExecutionDetailProps) {
  const {
    execution,
    effects,
    evidence,
    effectsClient,
    executionsClient,
    assignmentsClient,
    showActions = true,
    onRefresh,
    onError,
  } = props;
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("");

  async function run(action: () => Promise<unknown>, done: string, failurePrefix: string) {
    setBusy(true);
    setNotice("");
    try {
      await action();
      setNotice(done);
      onRefresh?.();
    } catch (e) {
      onError?.(`${failurePrefix}：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  const showTakeover = execution.recoverable || execution.activeAssignmentId !== null;

  return (
    <div>
      <p>
        执行：{execution.executionId} · 状态 {execution.status}
        {execution.taskId ? ` · 任务 ${execution.taskId}` : ""}
        {execution.loopId ? ` · 循环 ${execution.loopId}` : ""}
        {execution.workerSessionId ? ` · Worker ${execution.workerSessionId}` : ""}
        {execution.activeAssignmentId ? ` · 活动分配 ${execution.activeAssignmentId}` : ""}
        {execution.recoverable ? " · 可接管" : ""}
      </p>
      {showActions && assignmentsClient && showTakeover && (
        <div className="row">
          <button
            type="button"
            disabled={busy || execution.activeAssignmentId !== null}
            onClick={() =>
              void run(
                () => assignmentsClient.takeover(execution.executionId),
                "已接管：新 Worker 会话已接手该执行。",
                "接管失败",
              )
            }
          >
            接管
          </button>
        </div>
      )}
      {showActions && executionsClient && execution.status === "needs_review" && (
        <div className="row">
          <button
            type="button"
            disabled={busy}
            onClick={() =>
              void run(
                () => executionsClient.transition(execution.executionId, "running"),
                "已恢复运行。",
                "恢复运行失败",
              )
            }
          >
            恢复运行
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() =>
              void run(
                () => executionsClient.transition(execution.executionId, "completed"),
                "已标记完成。",
                "标记完成失败",
              )
            }
          >
            标记完成
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() =>
              void run(
                () => executionsClient.transition(execution.executionId, "failed"),
                "已标记失败。",
                "标记失败失败",
              )
            }
          >
            标记失败
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() =>
              void run(
                () => executionsClient.transition(execution.executionId, "cancelled"),
                "已标记取消。",
                "标记取消失败",
              )
            }
          >
            标记取消
          </button>
        </div>
      )}
      {notice !== "" && <p className="note">{notice}</p>}
      <h4>Effect 清单</h4>
      {effects.length === 0 && <p className="empty">该执行暂无 Effect 记录。</p>}
      <ul>
        {effects.map((effect) => (
          <li key={effect.effectId}>
            <EffectBadge
              effect={effect}
              latestEvidence={latestEvidenceFor(effect.effectId, evidence)}
              effectsClient={effectsClient}
              showActions={showActions}
              onRefresh={onRefresh}
              onError={onError}
            />
          </li>
        ))}
      </ul>
    </div>
  );
}
