import { useEffect, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type {
  DeviceActionRecord,
  DeviceGateway,
  DeviceReminder,
} from "../domain/device_client";
import type { Node, NodeGateway } from "../domain/node_client";
// P0-R R5: optional props (App.tsx still needs to pass them —
// outside this task's file scope); taken as props so App.tsx's
// transport choice applies unchanged.
import type { EffectRecord, EffectsGateway } from "../domain/effects_client";
import type { EvidenceGateway, EvidenceRow } from "../domain/evidence_client";
import { EffectBadge, effectNumber, latestEvidenceFor } from "./ExecutionDetail";

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
  nodes: NodeGateway;
  effects?: EffectsGateway | null;
  evidence?: EvidenceGateway | null;
}

function errText(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function DevicesView({
  agent,
  devices,
  nodes,
  effects: effectsClient = null,
  evidence: evidenceClient = null,
}: DevicesViewProps) {
  const [reminders, setReminders] = useState<DeviceReminder[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [targetCompleted, setTargetCompleted] = useState(true);
  const [actionList, setActionList] = useState<DeviceActionRecord[]>([]);
  const [current, setCurrent] = useState<DeviceActionRecord | null>(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  // Nodes section (Task 19): the node registry lives on
  // the Devices tab (concept-adjacent, no new tab). Unpair
  // is a tombstone: the record stays in nodes.jsonl but
  // leaves the list and reads as unknown afterwards.
  const [nodeList, setNodeList] = useState<Node[]>([]);
  const [nodeName, setNodeName] = useState("");
  const [nodeMessage, setNodeMessage] = useState("");
  const [nodeError, setNodeError] = useState("");
  // P0-R R5 Effect badges: gateway rows only, filtered
  // client-side. The badge prefers the action's own
  // effect_id (P0-C direct binding); scope matching —
  // device/reminders rows scope the reminder id, legacy
  // device rows scope the action id — is only the
  // pre-P0-C fallback, and takes the latest row. Device
  // action management itself is untouched.
  const [effectRows, setEffectRows] = useState<EffectRecord[]>([]);
  const [effectEvidence, setEffectEvidence] = useState<EvidenceRow[]>([]);
  const [effectError, setEffectError] = useState("");

  function effectForAction(action: DeviceActionRecord): EffectRecord | null {
    // Direct binding first: the action's own planned Effect
    // (P0-C). Scope matching is only the pre-P0-C fallback,
    // where no effect_id exists on the row.
    if (action.effectId) {
      const direct = effectRows.find((e) => e.effectId === action.effectId);
      if (direct) return direct;
    }
    const matches = effectRows.filter(
      (e) =>
        (e.capability === "device" || e.capability === "devices")
        && (e.scope === action.reminderId || e.scope === action.actionId),
    );
    if (matches.length === 0) return null;
    matches.sort((a, b) => effectNumber(b.effectId) - effectNumber(a.effectId));
    return matches[0];
  }

  // 该提醒的最新 Effect（P1 RemindersView 的 effectForReminder
  // 同一规则，本页加 capability 过滤）：最新动作的 effect_id
  // 直绑优先，否则按 scope（提醒 id）匹配最新一条
  // device/devices 行。loop 写没有动作行时走 scope 匹配，
  // 这正是角标缺口的修复点。
  function effectForReminder(reminder: DeviceReminder): EffectRecord | null {
    const mine = actionList.filter((a) => a.reminderId === reminder.id);
    const latest = mine.length === 0 ? null : mine[mine.length - 1];
    if (latest && latest.effectId) {
      const direct = effectRows.find((e) => e.effectId === latest.effectId);
      if (direct) return direct;
    }
    const matches = effectRows.filter(
      (e) =>
        (e.capability === "device" || e.capability === "devices") &&
        e.scope === reminder.id,
    );
    if (matches.length === 0) return null;
    matches.sort((a, b) => effectNumber(b.effectId) - effectNumber(a.effectId));
    return matches[0];
  }

  async function refreshEffectRows() {
    if (!effectsClient) return;
    setEffectError("");
    try {
      const [allEffects, allEvidence] = await Promise.all([
        effectsClient.list(),
        evidenceClient ? evidenceClient.list() : Promise.resolve([] as EvidenceRow[]),
      ]);
      setEffectRows(allEffects);
      setEffectEvidence(allEvidence);
    } catch (e) {
      setEffectError(`Effect 记录读取失败：${errText(e)}`);
    }
  }

  async function refreshNodes() {
    try {
      setNodeList(await nodes.listNodes(null));
    } catch (e) {
      setNodeError(errText(e));
    }
  }

  useEffect(() => {
    void refreshNodes();
    // Mount only: refreshes come from explicit actions below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function registerNode() {
    setNodeError("");
    setNodeMessage("");
    try {
      const record = await nodes.registerNode(nodeName.trim() === "" ? null : nodeName.trim());
      setNodeName("");
      await refreshNodes();
      setNodeMessage(`已登记节点 ${record.nodeId}。`);
    } catch (e) {
      setNodeError(errText(e));
    }
  }

  async function actOnNode(nodeId: string, action: "heartbeat" | "disconnect" | "unpair") {
    setNodeError("");
    setNodeMessage("");
    try {
      if (action === "heartbeat") await nodes.heartbeatNode(nodeId);
      else if (action === "disconnect") await nodes.disconnectNode(nodeId);
      else await nodes.unpairNode(nodeId);
      await refreshNodes();
      setNodeMessage(
        action === "heartbeat"
          ? `节点 ${nodeId} 心跳已更新。`
          : action === "disconnect"
            ? `节点 ${nodeId} 已下线（心跳可重新上线）。`
            : `节点 ${nodeId} 已解配（tombstone：记录文件旧行仍在）。`,
      );
    } catch (e) {
      setNodeError(errText(e));
    }
  }

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
    void refreshEffectRows();
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
  const currentEffect = current ? effectForAction(current) : null;

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
          {reminders.map((r) => {
            const eff = effectsClient ? effectForReminder(r) : null;
            return (
              <li key={r.id}>
                <button onClick={() => setSelectedId(r.id)} disabled={busy}>
                  {r.id}
                </button>{" "}
                {r.title} · {r.completed ? "已完成" : "未完成"}
                {effectsClient && (
                  <span>
                    {" "}· Effect：
                    {eff ? (
                      <EffectBadge
                        effect={eff}
                        latestEvidence={latestEvidenceFor(eff.effectId, effectEvidence)}
                        effectsClient={effectsClient}
                        onRefresh={() => void refreshEffectRows()}
                        onError={(message) => setEffectError(message)}
                      />
                    ) : (
                      "暂无对应 Effect 记录。"
                    )}
                  </span>
                )}
              </li>
            );
          })}
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
          {effectsClient && (
            <p>
              Effect：{currentEffect ? (
                <EffectBadge
                  effect={currentEffect}
                  latestEvidence={latestEvidenceFor(currentEffect.effectId, effectEvidence)}
                  effectsClient={effectsClient}
                  onRefresh={() => void refreshEffectRows()}
                  onError={(message) => setEffectError(message)}
                />
              ) : (
                "暂无对应 Effect 记录（按 scope 匹配，未找到）。"
              )}
            </p>
          )}
          {effectError !== "" && <p>错误：{effectError}</p>}
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
      <h3>节点（Nodes）</h3>
      <p className="note">
        节点注册表：登记、心跳、下线、解配。解配是 tombstone，不从磁盘擦除旧记录；
        下线可逆，心跳可重新上线。调用回执与同步（invoke-ack、invoke-result、sync）不在本轮范围。
      </p>
      <div className="row">
        <input
          aria-label="节点名字"
          type="text"
          value={nodeName}
          onChange={(e) => setNodeName(e.target.value)}
          placeholder="节点名字（可选，缺省用编号）"
          onKeyDown={(e) => {
            if (e.key === "Enter") void registerNode();
          }}
        />
        <button type="button" onClick={() => void registerNode()}>
          登记节点
        </button>
        <button type="button" onClick={() => void refreshNodes()}>
          刷新
        </button>
      </div>
      {nodeMessage !== "" && <p>{nodeMessage}</p>}
      {nodeError !== "" && <p>错误：{nodeError}</p>}
      <ul>
        {nodeList.map((node) => (
          <li key={node.nodeId}>
            {node.nodeId} · {node.name} · {node.status}
            {" "}
            <button type="button" onClick={() => void actOnNode(node.nodeId, "heartbeat")}>
              心跳
            </button>
            {" "}
            <button type="button" onClick={() => void actOnNode(node.nodeId, "disconnect")}>
              下线
            </button>
            {" "}
            <button type="button" onClick={() => void actOnNode(node.nodeId, "unpair")}>
              解配
            </button>
          </li>
        ))}
      </ul>
      <h3>动作列表</h3>
      {effectsClient && (
        <button type="button" onClick={() => void refreshEffectRows()} disabled={busy}>
          刷新 Effect 角标
        </button>
      )}
      <ul>
        {actionList.map((a) => {
          const eff = effectsClient ? effectForAction(a) : null;
          return (
            <li key={a.actionId}>
              <button onClick={() => void handleSelect(a)} disabled={busy}>
                {a.actionId}
              </button>{" "}
              {a.reminderId} · 目标 {a.targetCompleted ? "已完成" : "未完成"} · {a.status}
              {effectsClient && eff && (
                <span>
                  {" "}· Effect：<EffectBadge
                    effect={eff}
                    latestEvidence={latestEvidenceFor(eff.effectId, effectEvidence)}
                    effectsClient={effectsClient}
                    onRefresh={() => void refreshEffectRows()}
                    onError={(message) => setEffectError(message)}
                  />
                </span>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
}
