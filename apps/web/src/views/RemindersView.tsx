import { useEffect, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type {
  DeviceActionRecord,
  DeviceGateway,
  DeviceReminder,
} from "../domain/device_client";
import type { EffectRecord, EffectsGateway } from "../domain/effects_client";
import type { EvidenceGateway, EvidenceRow } from "../domain/evidence_client";
import { EffectBadge, effectNumber, latestEvidenceFor } from "./ExecutionDetail";

// P1 提醒（基线 P1）：完成态管理产品化，只做 UI。
// 本视图不新增任何语义：列表、完成动作、Effect/Evidence 行内
// 展示全部复用现有客户端与网关流——提醒读取、建动作、审批、
// 执行、重读核查、unknown 核查、Inbox 联动均已在 P0-B/C/D/R
// 落地。没有创建、删除、编辑、到期日、复发、推迟、到期触发
//（见方案 §8 排除项）；词汇映射沿用 effect_presentation，
// 不引入新词。行内 Effect 与工作台 Inbox 里的同一 effect id
// 是同一条记录，不另开页面。

interface RemindersViewProps {
  agent: AgentGateway;
  devices: DeviceGateway;
  effects?: EffectsGateway | null;
  evidence?: EvidenceGateway | null;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

type Filter = "all" | "completed" | "uncompleted";

export function RemindersView({
  agent,
  devices,
  effects: effectsClient = null,
  evidence: evidenceClient = null,
}: RemindersViewProps) {
  const [reminders, setReminders] = useState<DeviceReminder[]>([]);
  const [filter, setFilter] = useState<Filter>("all");
  const [actionList, setActionList] = useState<DeviceActionRecord[]>([]);
  const [effectRows, setEffectRows] = useState<EffectRecord[]>([]);
  const [effectEvidence, setEffectEvidence] = useState<EvidenceRow[]>([]);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function refreshReminders(): Promise<DeviceReminder[] | null> {
    try {
      const latest = await devices.listReminders();
      setReminders(latest);
      return latest;
    } catch (e) {
      setError(`提醒列表读取失败：${errText(e)}`);
      return null;
    }
  }

  async function refreshActions() {
    try {
      setActionList(await devices.listActions());
    } catch (e) {
      setError(`动作列表读取失败：${errText(e)}`);
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
    await refreshReminders();
    await refreshActions();
    await refreshEffectRows();
  }

  useEffect(() => {
    void refreshAll();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 该提醒的最新动作：动作列表按创建顺序返回，取同提醒的
  // 最后一个；其 Effect 优先按 effect_id 直绑（P0-C 起动作行
  // 自带 planned Effect id），否则按 scope（提醒 id）匹配
  // 最新一条——与 DevicesView 角标同一规则。
  function effectForReminder(reminder: DeviceReminder): EffectRecord | null {
    const mine = actionList.filter((a) => a.reminderId === reminder.id);
    const latest = mine.length === 0 ? null : mine[mine.length - 1];
    if (latest && latest.effectId) {
      const direct = effectRows.find((e) => e.effectId === latest.effectId);
      if (direct) return direct;
    }
    const matches = effectRows.filter((e) => e.scope === reminder.id);
    matches.sort((a, b) => effectNumber(b.effectId) - effectNumber(a.effectId));
    return matches.length === 0 ? null : matches[0];
  }

  async function handleComplete(reminder: DeviceReminder) {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const action = await devices.createAction(reminder.id, true);
      const request = await agent.requestApproval("reminders.set_completed");
      if (!request.requires_approval || !request.tool_call_id) {
        throw new Error("reminders.set_completed 审批记录未建立，与预期不符");
      }
      await agent.decideApproval(request.tool_call_id, "approve");
      const updated = await devices.setReminder(
        action.actionId,
        request.tool_call_id,
      );
      const latest = await refreshReminders();
      await refreshActions();
      await refreshEffectRows();
      if (updated.status === "completed") {
        const item = latest === null ? undefined : latest.find((r) => r.id === reminder.id);
        if (latest === null) {
          setMessage(`已批准（${request.tool_call_id}）并执行，但当前提醒查询失败，状态尚未核对。`);
        } else if (!item || item.completed !== true) {
          setMessage(`已批准并执行，但当前提醒状态与目标不一致；请核查，不要直接重试。`);
        } else {
          setMessage(`已批准（${request.tool_call_id}）并执行，提醒已完成。`);
        }
      } else if (updated.status === "unknown") {
        setMessage(`已批准并执行，但结果未知（${request.tool_call_id}）；请用行内“核查”确认，不要直接重试。`);
      } else {
        setMessage(`已批准并执行，动作状态：${updated.status}。`);
      }
    } catch (e) {
      setError(`执行失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleRefreshAction(actionId: string) {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      await devices.refreshAction(actionId);
      await refreshAll();
    } catch (e) {
      setError(`再次查询失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  const visible =
    filter === "all"
      ? reminders
      : reminders.filter((r) => r.completed === (filter === "completed"));

  return (
    <section>
      <h2>提醒</h2>
      <p className="note">
        查看与完成提醒。行内 Effect 与工作台 Inbox 里的同一 effect id
        是同一条记录，不另开页面。本页不创建、不删除、不编辑提醒，
        也不做到期、复发、推迟与到期触发。
      </p>
      <p className="note">
        权限说明：提醒权限未决定时，请在系统设置中授予提醒权限后点刷新；
        权限被拒时，列表不可用，需在系统设置中手动开启；执行超时会记为
        unknown，请用行内“核查”确认，不要直接重试。
      </p>
      <div className="row">
        <button type="button" onClick={() => void refreshAll()} disabled={busy}>
          刷新提醒
        </button>
        <button type="button" onClick={() => setFilter("all")} disabled={filter === "all"}>
          全部
        </button>
        <button type="button" onClick={() => setFilter("uncompleted")} disabled={filter === "uncompleted"}>
          未完成
        </button>
        <button type="button" onClick={() => setFilter("completed")} disabled={filter === "completed"}>
          已完成
        </button>
      </div>
      {error !== "" && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {message !== "" && <p>{message}</p>}
      {!effectsClient && (
        <p className="note">Effect 客户端尚未接入：待 App 接线传入客户端后，这里显示每条提醒的最新 Effect 记录。</p>
      )}
      {reminders.length === 0 && <p className="empty">暂无提醒。</p>}
      {reminders.length > 0 && visible.length === 0 && (
        <p className="empty">该筛选下暂无提醒。</p>
      )}
      <ul>
        {visible.map((reminder) => {
          const effect = effectForReminder(reminder);
          const latestAction =
            actionList.filter((a) => a.reminderId === reminder.id).pop() ?? null;
          return (
            <li key={reminder.id}>
              <p>
                {reminder.title} · {reminder.id} · {reminder.completed ? "已完成" : "未完成"}
              </p>
              {!reminder.completed && (
                <button
                  type="button"
                  onClick={() => void handleComplete(reminder)}
                  disabled={busy}
                >
                  完成
                </button>
              )}
              {latestAction && latestAction.status === "unknown" && (
                <button
                  type="button"
                  onClick={() => void handleRefreshAction(latestAction.actionId)}
                  disabled={busy}
                >
                  再次查询
                </button>
              )}
              {effectsClient && (
                <p>
                  Effect：
                  {effect ? (
                    <EffectBadge
                      effect={effect}
                      latestEvidence={latestEvidenceFor(effect.effectId, effectEvidence)}
                      effectsClient={effectsClient}
                      onRefresh={() => void refreshEffectRows()}
                      onError={(message) => setError(message)}
                    />
                  ) : (
                    "暂无对应 Effect 记录。"
                  )}
                </p>
              )}
            </li>
          );
        })}
      </ul>
    </section>
  );
}
