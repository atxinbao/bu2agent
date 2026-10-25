import { useEffect, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type {
  DeviceActionRecord,
  DeviceGateway,
  DeviceReminder,
} from "../domain/device_client";

// Devices view: the B-layer single-device validation loop,
// driven step by step from the frontend (see the device-loop
// design doc). The gateway owns the Fake backend for now;
// the view never touches a real reminder in this round.
//
// Flow: list reminders -> pick one -> create an action ->
// approve, then execute -> re-query to confirm the state.
// Rejection records the action as rejected and never calls
// the device. An unknown action is re-queried, never retried
// blindly.
//
// Honest boundary: the default backend is a Fake seeded with
// one demo reminder (reminder-demo). Real reminder writes
// wait for EventKit verification, which is a later phase.

interface DevicesViewProps {
  agent: AgentGateway;
  devices: DeviceGateway;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function DevicesView({ agent, devices }: DevicesViewProps) {
  const [reminders, setReminders] = useState<DeviceReminder[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [targetCompleted, setTargetCompleted] = useState(true);
  const [actionList, setActionList] = useState<DeviceActionRecord[]>([]);
  const [current, setCurrent] = useState<DeviceActionRecord | null>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  async function refreshReminders() {
    try {
      const latest = await devices.listReminders();
      setReminders(latest);
      return latest;
    } catch (e) {
      setError(`提醒列表读取失败：${errText(e)}`);
      return null;
    }
  }

  function confirmedMessage(action: DeviceActionRecord, latest: DeviceReminder[] | null) {
    // 动作记录与此刻查询分开表述，避免把旧记录或查询失败说成最新状态确认。
    if (latest === null) return "动作记录为完成，但当前提醒查询失败，状态尚未核对。";
    const item = latest.find((r) => r.id === action.reminderId);
    if (!item || item.completed !== action.targetCompleted) {
      return "动作记录为完成，但当前提醒状态与目标不一致；请核查，不要直接重试。";
    }
    return "动作已完成；本次重查确认提醒状态与目标一致。";
  }

  async function refreshActions() {
    try {
      setActionList(await devices.listActions());
    } catch (e) {
      setError(`动作列表读取失败：${errText(e)}`);
    }
  }

  useEffect(() => {
    void refreshReminders();
    void refreshActions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function handleCreate() {
    if (selectedId === "") {
      setError("请先选择一条提醒。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const action = await devices.createAction(selectedId, targetCompleted);
      setCurrent(action);
      setMessage(`动作已创建：${action.actionId}（${action.status}），请审批后执行。`);
      await refreshActions();
    } catch (e) {
      setError(`动作创建失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleApproveSet() {
    if (!current) {
      setError("请先创建动作。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const request = await agent.requestApproval("reminders.set_completed");
      if (!request.requires_approval || !request.tool_call_id) {
        throw new Error("reminders.set_completed 审批记录未建立，与预期不符");
      }
      await agent.decideApproval(request.tool_call_id, "approve");
      const updated = await devices.setReminder(
        current.actionId,
        request.tool_call_id,
      );
      setCurrent(updated);
      const latest = await refreshReminders();
      await refreshActions();
      if (updated.status === "completed") {
        setMessage(`已批准（${request.tool_call_id}）并执行。${confirmedMessage(updated, latest)}`);
      } else if (updated.status === "unknown") {
        setMessage(`已批准并执行，但结果未知（${request.tool_call_id}）；请用“再次查询”确认，不要直接重试。`);
      } else {
        setMessage(`已批准并执行，动作状态：${updated.status}。`);
      }
    } catch (e) {
      setError(`执行失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleReject() {
    if (!current) {
      setError("请先创建动作。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const request = await agent.requestApproval("reminders.set_completed");
      if (!request.requires_approval || !request.tool_call_id) {
        throw new Error("reminders.set_completed 审批记录未建立，与预期不符");
      }
      await agent.decideApproval(request.tool_call_id, "reject");
      const updated = await devices.markRejected(
        current.actionId,
        request.tool_call_id,
      );
      setCurrent(updated);
      setMessage("已拒绝执行，动作标记为 rejected，未调用设备写接口。");
      await refreshActions();
    } catch (e) {
      setError(`拒绝失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleRefresh() {
    if (!current) {
      setError("请先创建动作。");
      return;
    }
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const updated = await devices.refreshAction(current.actionId);
      setCurrent(updated);
      const latest = await refreshReminders();
      await refreshActions();
      if (updated.status === "completed") {
        setMessage(confirmedMessage(updated, latest));
      } else if (updated.status === "unknown") {
        setMessage("再次查询：结果仍未知，写可能仍在途中，请稍后再查，不要直接重试。");
      } else {
        setMessage(`再次查询：动作状态 ${updated.status}。`);
      }
    } catch (e) {
      setError(`再次查询失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  async function handleSelect(action: DeviceActionRecord) {
    setError("");
    setMessage("");
    setBusy(true);
    try {
      const fresh = await devices.getAction(action.actionId);
      setCurrent(fresh);
    } catch (e) {
      setError(`动作读取失败：${errText(e)}`);
    } finally {
      setBusy(false);
    }
  }

  const selected = reminders.find((r) => r.id === selectedId) ?? null;

  return (
    <div>
      <h2>Devices</h2>
      <p>
        单设备验证：选提醒 → 创建动作 → 审批后执行 → 重查确认。
        默认后端是 Fake，含一条演示提醒 reminder-demo；真实提醒写入待 EventKit 验证，本视图不创建真实提醒。
      </p>
      <div>
        <button onClick={() => void refreshReminders()} disabled={busy}>
          列出提醒
        </button>
        <button onClick={() => void refreshActions()} disabled={busy}>
          刷新动作列表
        </button>
      </div>
      {reminders.length > 0 && (
        <ul>
          {reminders.map((r) => (
            <li key={r.id}>
              <button onClick={() => setSelectedId(r.id)} disabled={busy}>
                {r.id}
              </button>{" "}
              {r.title} · {r.completed ? "已完成" : "未完成"}
            </li>
          ))}
        </ul>
      )}
      {selected && (
        <div>
          <p>
            当前提醒：{selected.id} · {selected.title} ·{" "}
            {selected.completed ? "已完成" : "未完成"}
          </p>
          <label>
            <input
              type="checkbox"
              checked={targetCompleted}
              onChange={(e) => setTargetCompleted(e.target.checked)}
            />
            目标状态：已完成（不勾选 = 设为未完成）
          </label>
          <div>
            <button onClick={() => void handleCreate()} disabled={busy}>
              创建动作
            </button>
          </div>
        </div>
      )}
      {current && (
        <div>
          <p>
            当前动作：{current.actionId} · 提醒 {current.reminderId} ·
            目标 {current.targetCompleted ? "已完成" : "未完成"} · 状态 {current.status}
            {current.detail ? ` · ${current.detail}` : ""}
          </p>
          <div>
            {current.status === "requested" && (
              <>
                <button onClick={() => void handleApproveSet()} disabled={busy}>
                  批准并执行
                </button>
                <button onClick={() => void handleReject()} disabled={busy}>
                  拒绝
                </button>
              </>
            )}
            {current.status === "unknown" && (
              <button onClick={() => void handleRefresh()} disabled={busy}>
                再次查询
              </button>
            )}
          </div>
        </div>
      )}
      {message !== "" && <p>{message}</p>}
      {error !== "" && <p>错误：{error}</p>}
      <h3>动作列表</h3>
      <ul>
        {actionList.map((a) => (
          <li key={a.actionId}>
            <button onClick={() => void handleSelect(a)} disabled={busy}>
              {a.actionId}
            </button>{" "}
            {a.reminderId} · 目标 {a.targetCompleted ? "已完成" : "未完成"} · {a.status}
          </li>
        ))}
      </ul>
    </div>
  );
}
