import { useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import type {
  InvokeSettingsClient,
  KeyStatus,
  SettingsData,
} from "../domain/settings_client";
import type {
  CatalogEntry,
  CredentialSlot,
  CustomConnector,
} from "../domain/credential_vault_client";
import type { InvokeCredentialVaultClient } from "../domain/invoke_credential_vault_client";
import type {
  ConnectorPolicy,
  Policy,
} from "../domain/permission_policy_client";
import type { InvokePermissionPolicyClient } from "../domain/invoke_permission_policy_client";

// Settings page (invoke-only). Save and effective state are
// separate on purpose: a credential or directory save lands
// immediately, but a running gateway only reads credentials
// at startup, so the page says a restart is needed after a
// save -- with a restart button for an owned gateway and a
// manual-restart note for an attached one. The secret itself
// never comes back: key status carries only configured + a
// ****last-4 redaction, and the input is cleared after save.
interface GatewayStatusView {
  endpoint: string;
  port: number;
  ownership: string;
  state: string;
  foreign_port: boolean;
  error: string | null;
}

interface SettingsViewProps {
  settings: InvokeSettingsClient;
  vault: InvokeCredentialVaultClient;
  policy: InvokePermissionPolicyClient;
  isInvoke: boolean;
  gatewayStatus: GatewayStatusView | null;
  onSettingsChanged: () => void;
}

// Tauri invoke rejects with the Rust error string itself, not
// an Error object, so `err instanceof Error` misses it and
// every catch used to show a generic fallback (the out-of-root
// reason surfaced only as "pick failed"). Pass strings through.
function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

function remindersText(status: string | null): string {
  switch (status) {
    case "authorized":
      return "已授权";
    case "denied":
      return "已拒绝（请到系统设置中开启）";
    case "not_determined":
      return "未决定（请到提醒事项页请求授权）";
    case null:
      return "未知（状态查询失败）";
    default:
      return status;
  }
}

export function SettingsView({
  settings,
  vault,
  policy,
  isInvoke,
  gatewayStatus,
  onSettingsChanged,
}: SettingsViewProps) {
  const [data, setData] = useState<SettingsData | null>(null);
  const [key, setKey] = useState<KeyStatus | null>(null);
  const [reminders, setReminders] = useState<string | null>(null);
  const [picked, setPicked] = useState<string | null>(null);
  const [secret, setSecret] = useState("");
  const [pendingRemove, setPendingRemove] = useState(false);
  const [needsRestart, setNeedsRestart] = useState<"saved" | "removed" | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  // Vault section (Task 10). Secrets are never kept after save:
  // inputs clear on success, lists carry only redacted displays.
  const [slots, setSlots] = useState<CredentialSlot[] | null>(null);
  const [catalog, setCatalog] = useState<CatalogEntry[] | null>(null);
  const [custom, setCustom] = useState<CustomConnector[] | null>(null);
  const [vaultHandle, setVaultHandle] = useState("");
  const [vaultSecret, setVaultSecret] = useState("");
  const [vaultProvider, setVaultProvider] = useState("");
  const [captureHandle, setCaptureHandle] = useState("");
  const [captureSecret, setCaptureSecret] = useState("");
  const [ticketProvider, setTicketProvider] = useState("");
  const [ticketHandle, setTicketHandle] = useState("");
  const [ticketId, setTicketId] = useState("");
  const [ticketSecret, setTicketSecret] = useState("");
  const [ticketNote, setTicketNote] = useState<string | null>(null);
  const [customId, setCustomId] = useState("");
  const [customName, setCustomName] = useState("");
  const [renameValues, setRenameValues] = useState<Record<string, string>>({});
  const [pendingSlotDelete, setPendingSlotDelete] = useState<string | null>(null);
  // Policy section (Task 11). Data only: the switches and
  // rules below are stored, never enforced — they do not
  // change the approval gates and do not intercept traffic.
  const [policyDoc, setPolicyDoc] = useState<Policy | null>(null);
  const [rulePattern, setRulePattern] = useState("");
  const [ruleAction, setRuleAction] = useState("deny");
  const [scopeType, setScopeType] = useState("");
  const [scopeId, setScopeId] = useState("");
  const [snoozeMs, setSnoozeMs] = useState("");
  const [pendingPolicyReset, setPendingPolicyReset] = useState(false);
  const [pendingRevokeAll, setPendingRevokeAll] = useState(false);

  async function refreshVault() {
    try {
      setSlots(await vault.listCredentials());
    } catch {
      setSlots(null);
    }
    try {
      setCatalog(await vault.catalogList());
    } catch {
      setCatalog(null);
    }
    try {
      setCustom(await vault.customList());
    } catch {
      setCustom(null);
    }
  }

  async function refreshPolicy() {
    try {
      setPolicyDoc(await policy.getPolicy());
    } catch {
      setPolicyDoc(null);
    }
  }

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      setData(await settings.get());
      try {
        setKey(await settings.keyStatus());
      } catch {
        setKey(null);
      }
      try {
        setReminders(await settings.deviceAuthorization());
      } catch {
        setReminders(null);
      }
      await refreshVault();
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    if (isInvoke) {
      void refresh();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  if (!isInvoke) {
    return (
      <div className="view">
        <h2>设置</h2>
        <p className="note">设置页仅在应用内可用（浏览器预览无壳命令）。</p>
      </div>
    );
  }

  async function pick() {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const path = await settings.pickDirectory();
      if (path !== null) {
        setPicked(path);
      }
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function savePicked() {
    if (picked === null) {
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      setData(await settings.saveWorkingDir(picked));
      setPicked(null);
      setNotice("默认工作目录已保存，新建操作起生效。");
      onSettingsChanged();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function clear() {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      setData(await settings.clearWorkingDir());
      setPicked(null);
      setNotice("默认工作目录已恢复为文件根。");
      onSettingsChanged();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function saveKey() {
    if (secret.trim() === "") {
      setError("凭据不能为空");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      setKey(await settings.keySave(secret));
      setSecret("");
      setNeedsRestart("saved");
      setNotice("凭据已保存到 Keychain。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function removeKey() {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      setKey(await settings.keyRemove());
      setPendingRemove(false);
      setNeedsRestart("removed");
      setNotice("凭据已从 Keychain 删除。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function restartForKey() {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const status = await invoke<{ state: string }>("gateway_restart");
      if (status.state === "ready") {
        setNeedsRestart(null);
        setNotice("网关已重启，凭据已生效。");
      } else {
        setError("网关重启后仍未就绪，请查看页头状态。");
      }
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function registerSlot() {
    if (vaultHandle.trim() === "" || vaultSecret.trim() === "") {
      setError("凭据名与密钥不能为空");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await vault.registerCredential(
        vaultHandle.trim(),
        vaultSecret,
        vaultProvider.trim() === "" ? null : vaultProvider.trim(),
      );
      setVaultHandle("");
      setVaultSecret("");
      setVaultProvider("");
      await refreshVault();
      setNotice("凭据已保存到 Keychain（仅显示尾号）。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function captureSlot() {
    if (captureHandle.trim() === "" || captureSecret.trim() === "") {
      setError("凭据名与密钥不能为空");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await vault.captureCredential(captureHandle.trim(), captureSecret);
      setCaptureHandle("");
      setCaptureSecret("");
      await refreshVault();
      setNotice("凭据密钥已更新。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function deleteSlot() {
    if (pendingSlotDelete === null) {
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await vault.deleteCredential(pendingSlotDelete);
      setPendingSlotDelete(null);
      await refreshVault();
      setNotice("凭据已从 Keychain 删除。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function requestTicket() {
    if (ticketProvider.trim() === "" || ticketHandle.trim() === "") {
      setError("票据的 provider 与凭据名不能为空");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const ticket = await vault.requestCapture(ticketProvider.trim(), ticketHandle.trim());
      setTicketId(ticket.ticketId);
      setTicketNote(`票据 ${ticket.ticketId} 已创建，15 分钟内有效。`);
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function fulfillTicket() {
    if (ticketId.trim() === "" || ticketSecret.trim() === "") {
      setError("票据号与密钥不能为空");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await vault.fulfillCapture(ticketId.trim(), ticketSecret);
      setTicketId("");
      setTicketSecret("");
      setTicketNote(null);
      await refreshVault();
      setNotice("票据已完成，凭据密钥已更新。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function registerCustom() {
    if (customId.trim() === "" || customName.trim() === "") {
      setError("自定义连接器 id 与名称不能为空");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await vault.customRegister(customId.trim(), customName.trim(), null);
      setCustomId("");
      setCustomName("");
      await refreshVault();
      setNotice("自定义连接器已注册。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function deleteCustom(id: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await vault.customDelete(id);
      await refreshVault();
      setNotice("自定义连接器已删除。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function renameCatalog(id: string) {
    const displayName = (renameValues[id] ?? "").trim();
    if (displayName === "") {
      setError("新名称不能为空");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await vault.catalogUpdate(id, displayName, null);
      setRenameValues((prev) => ({ ...prev, [id]: "" }));
      await refreshVault();
      setNotice("目录条目已改名。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function deleteCatalog(id: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await vault.catalogDelete(id);
      await refreshVault();
      setNotice("目录条目已删除（已存凭据不受影响）。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  const owned = gatewayStatus?.ownership === "owned";
  const attached = gatewayStatus?.ownership === "attached";

  function connectorPolicyRow(connector: string): ConnectorPolicy {
    const found = policyDoc?.connectors.find((c) => c.connectorId === connector);
    return found ?? { connectorId: connector, allowed: true, updatedMs: 0 };
  }

  async function toggleEffective(key: "hitl" | "mitm" | "tproxy" | "sni") {
    if (policyDoc === null) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const active = policyDoc.active;
      if (key === "hitl") {
        await policy.updateHitl(!active.hitlEnabled);
      } else if (key === "mitm") {
        await policy.updateMitm(!active.mitmEnabled);
      } else if (key === "tproxy") {
        await policy.updateTproxy(!active.tproxyEnabled);
      } else {
        await policy.updateSniRejection(!active.sniRejectionEnabled);
      }
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function toggleDefault(key: "hitl" | "mitm" | "tproxy" | "sni") {
    if (policyDoc === null) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const defaults = policyDoc.defaults;
      const patch =
        key === "hitl"
          ? { hitlEnabled: !defaults.hitlEnabled }
          : key === "mitm"
            ? { mitmEnabled: !defaults.mitmEnabled }
            : key === "tproxy"
              ? { tproxyEnabled: !defaults.tproxyEnabled }
              : { sniRejectionEnabled: !defaults.sniRejectionEnabled };
      await policy.updateSettings(patch);
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function setSnooze() {
    const ms = Number(snoozeMs);
    if (snoozeMs.trim() === "" || !Number.isFinite(ms) || ms < 0) {
      setError("snooze 到期时间须为非负毫秒数。");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.updateHitl(undefined, ms);
      setSnoozeMs("");
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function clearSnooze() {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.deleteHitlSnooze();
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function addRule() {
    if (policyDoc === null || rulePattern.trim() === "") {
      setError("规则 pattern 不能为空。");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.updateNetworkRules([
        ...policyDoc.networkRules,
        { pattern: rulePattern.trim(), action: ruleAction },
      ]);
      setRulePattern("");
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function removeRule(index: number) {
    if (policyDoc === null) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.updateNetworkRules(policyDoc.networkRules.filter((_, i) => i !== index));
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function toggleConnector(connector: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const current = connectorPolicyRow(connector);
      await policy.updateConnector(connector, !current.allowed);
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function resetConnector(connector: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.resetConnector(connector);
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function grantScope() {
    if (scopeType.trim() === "" || scopeId.trim() === "") {
      setError("scope 类型与 id 均不能为空。");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.updateScope(scopeType.trim(), scopeId.trim(), true);
      setScopeType("");
      setScopeId("");
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function revokeScope(scopeType: string, scopeId: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.revoke(scopeType, scopeId);
      await refreshPolicy();
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function resetPolicy() {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.resetPolicy();
      setPendingPolicyReset(false);
      await refreshPolicy();
      setNotice("策略已恢复全部默认（覆盖、连接器策略、scope 与规则已清空）。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  async function revokeAllScopes() {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await policy.revoke();
      setPendingRevokeAll(false);
      await refreshPolicy();
      setNotice("全部 scope 授权已撤销。");
    } catch (err) {
      setError(errText(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="view">
      <h2>设置</h2>
      <p className="note">
        保存与生效分开：凭据与目录保存后立即落盘；运行中的网关只在启动时读取凭据，
        所以保存后需重启才生效（自有网关可手动重启，附着网关只提示）。
      </p>
      <button type="button" onClick={() => void refresh()} disabled={busy}>
        刷新状态
      </button>

      <h3>默认工作目录</h3>
      <p className="note">
        用于新建文件和任务，不改变可访问范围。已打开的编辑、已有任务和产物路径不变；
        显式填写的路径优先。只接受文件根内的现有目录（符号链接解析后仍须在根内），
        不创建新目录。
      </p>
      {data === null ? (
        <p className="empty">正在读取设置…</p>
      ) : (
        <div>
          <p>文件根：{data.file_root}</p>
          <p>
            当前默认：
            {data.default_working_dir === null
              ? "文件根（默认）"
              : data.working_dir_display}
          </p>
          {!data.working_dir_valid && (
            <p className="error">
              默认工作目录已失效（已删除、移出文件根或不可访问），请重新选择；
              失效的默认目录不会用于任何写入，新建任务需先重新选择或填写目录。
            </p>
          )}
          {!data.working_dir_editable && (
            <p className="note">附着网关的文件根未知，默认工作目录暂不可设置。</p>
          )}
          <div className="row">
            <button type="button" onClick={() => void pick()} disabled={busy || !data.working_dir_editable}>
              选择目录
            </button>
            <button type="button" onClick={() => void clear()} disabled={busy}>
              恢复文件根
            </button>
          </div>
          {picked !== null && (
            <div className="confirm">
              <span>已选：{picked}</span>
              <button type="button" onClick={() => void savePicked()} disabled={busy}>
                保存
              </button>
              <button type="button" onClick={() => setPicked(null)} disabled={busy}>
                取消
              </button>
            </div>
          )}
        </div>
      )}

      <h3>模型凭据</h3>
      <p className="note">
        保存到系统 Keychain（服务 bu2agent），页面不回读密钥，只显示是否已配置与尾号。
      </p>
      <p>
        状态：
        {key === null
          ? "未知（状态查询失败）"
          : key.configured
            ? `已配置 ${key.redacted ?? ""}`
            : "未配置"}
      </p>
      <div className="row">
        <input
          type="password"
          value={secret}
          placeholder="DeepSeek API key"
          onChange={(event) => setSecret(event.target.value)}
        />
        <button type="button" onClick={() => void saveKey()} disabled={busy || secret.trim() === ""}>
          保存凭据
        </button>
        {key?.configured && !pendingRemove && (
          <button type="button" onClick={() => setPendingRemove(true)} disabled={busy}>
            删除凭据
          </button>
        )}
      </div>
      {pendingRemove && (
        <div className="confirm">
          <span>删除 Keychain 中的模型凭据？运行中的网关重启后才失去凭据。</span>
          <button type="button" onClick={() => void removeKey()} disabled={busy}>
            确认删除
          </button>
          <button type="button" onClick={() => setPendingRemove(false)} disabled={busy}>
            取消
          </button>
        </div>
      )}
      {needsRestart !== null && (
        <div className="confirm">
          <span>
            {needsRestart === "saved" ? "凭据已保存，" : "凭据已删除，"}
            {attached
              ? "附着网关不自动重启，请手动重启该网关。"
              : owned
                ? "重启网关后生效。"
                : "网关状态未知，确认网关归属后再重启。"}
          </span>
          {owned && (
            <button type="button" onClick={() => void restartForKey()} disabled={busy}>
              重启网关
            </button>
          )}
        </div>
      )}

      <h3>凭据库（Vault）</h3>
      <p className="note">
        凭据密钥只进系统 Keychain，页面不回读密钥，只显示尾号；模型凭据（deepseek）仍在上方单独管理，不在此列出。
        目录条目由凭据注册时的 provider 自动创建；删除目录条目不删除已存凭据。
      </p>
      <p>
        已存凭据：
        {slots === null
          ? "未知（状态查询失败）"
          : slots.length === 0
            ? "无"
            : slots.map((s) => `${s.handle}${s.redacted ? ` ${s.redacted}` : ""}`).join(" · ")}
      </p>
      <div className="row">
        <input
          type="text"
          value={vaultHandle}
          placeholder="凭据名（如验收-凭据A）"
          onChange={(event) => setVaultHandle(event.target.value)}
        />
        <input
          type="password"
          value={vaultSecret}
          placeholder="密钥（测试请用 mock-token-…）"
          onChange={(event) => setVaultSecret(event.target.value)}
        />
        <input
          type="text"
          value={vaultProvider}
          placeholder="provider（可选）"
          onChange={(event) => setVaultProvider(event.target.value)}
        />
        <button type="button" onClick={() => void registerSlot()} disabled={busy}>
          注册凭据
        </button>
      </div>
      <div className="row">
        <input
          type="text"
          value={captureHandle}
          placeholder="已注册凭据名"
          onChange={(event) => setCaptureHandle(event.target.value)}
        />
        <input
          type="password"
          value={captureSecret}
          placeholder="新密钥"
          onChange={(event) => setCaptureSecret(event.target.value)}
        />
        <button type="button" onClick={() => void captureSlot()} disabled={busy}>
          填入密钥（capture）
        </button>
      </div>
      {slots !== null &&
        slots.map((slot) => (
          <div className="row" key={slot.handle}>
            <span>
              {slot.handle}
              {slot.provider ? ` · ${slot.provider}` : ""}
              {slot.redacted ? ` · ${slot.redacted}` : ""}
            </span>
            {pendingSlotDelete !== slot.handle && (
              <button type="button" onClick={() => setPendingSlotDelete(slot.handle)} disabled={busy}>
                删除凭据
              </button>
            )}
          </div>
        ))}
      {pendingSlotDelete !== null && (
        <div className="confirm">
          <span>删除 Keychain 中的凭据 {pendingSlotDelete}？此操作不可撤销。</span>
          <button type="button" onClick={() => void deleteSlot()} disabled={busy}>
            确认删除
          </button>
          <button type="button" onClick={() => setPendingSlotDelete(null)} disabled={busy}>
            取消
          </button>
        </div>
      )}

      <h3>外部票据（capture ticket）</h3>
      <p className="note">本地票据流程：先为已注册凭据申请票据，再凭票据号填入密钥；票据 15 分钟过期，不调用任何真实外部 provider。</p>
      <div className="row">
        <input
          type="text"
          value={ticketProvider}
          placeholder="provider"
          onChange={(event) => setTicketProvider(event.target.value)}
        />
        <input
          type="text"
          value={ticketHandle}
          placeholder="已注册凭据名"
          onChange={(event) => setTicketHandle(event.target.value)}
        />
        <button type="button" onClick={() => void requestTicket()} disabled={busy}>
          申请票据
        </button>
      </div>
      {ticketNote !== null && <p className="notice">{ticketNote}</p>}
      <div className="row">
        <input
          type="text"
          value={ticketId}
          placeholder="票据号"
          onChange={(event) => setTicketId(event.target.value)}
        />
        <input
          type="password"
          value={ticketSecret}
          placeholder="密钥（测试请用 mock-token-…）"
          onChange={(event) => setTicketSecret(event.target.value)}
        />
        <button type="button" onClick={() => void fulfillTicket()} disabled={busy}>
          凭票据填入
        </button>
      </div>

      <h3>目录（catalog）与自定义连接器</h3>
      {catalog === null ? (
        <p className="empty">目录读取失败。</p>
      ) : catalog.length === 0 ? (
        <p className="empty">暂无目录条目（注册带 provider 的凭据后自动创建）。</p>
      ) : (
        catalog.map((entry) => (
          <div className="row" key={entry.id}>
            <span>
              {entry.id} · {entry.displayName}
              {entry.description ? ` · ${entry.description}` : ""}
            </span>
            <input
              type="text"
              value={renameValues[entry.id] ?? ""}
              placeholder="新名称"
              onChange={(event) =>
                setRenameValues((prev) => ({ ...prev, [entry.id]: event.target.value }))
              }
            />
            <button type="button" onClick={() => void renameCatalog(entry.id)} disabled={busy}>
              改名
            </button>
            <button type="button" onClick={() => void deleteCatalog(entry.id)} disabled={busy}>
              删除条目
            </button>
          </div>
        ))
      )}
      <div className="row">
        <input
          type="text"
          value={customId}
          placeholder="自定义连接器 id"
          onChange={(event) => setCustomId(event.target.value)}
        />
        <input
          type="text"
          value={customName}
          placeholder="显示名称"
          onChange={(event) => setCustomName(event.target.value)}
        />
        <button type="button" onClick={() => void registerCustom()} disabled={busy}>
          注册自定义连接器
        </button>
      </div>
      {custom !== null &&
        custom.map((def) => (
          <div className="row" key={def.id}>
            <span>
              {def.id} · {def.displayName}
            </span>
            <button type="button" onClick={() => void deleteCustom(def.id)} disabled={busy}>
              删除
            </button>
          </div>
        ))}

      <h3>状态检查</h3>
      <p className="note">三项分开展示：网关状态、模型凭据配置、提醒事项授权。此行只反映凭据配置与网关可达，不做真实模型调用。</p>
      <h3>权限策略（Permissions）</h3>
      <p className="note">
        本地策略记录，不拦截真实流量，也不改变审批门：这里的开关与规则只存取、不执行。
        生效值 = 基准 + 覆盖；基准改动影响未覆盖项，覆盖改动只影响对应项。
      </p>
      {policyDoc === null ? (
        <p className="empty">策略未知（状态查询失败）。</p>
      ) : (
        <div>
          <p>
            生效开关：
            hitl {policyDoc.active.hitlEnabled ? "开" : "关"} · mitm{" "}
            {policyDoc.active.mitmEnabled ? "开" : "关"} · tproxy{" "}
            {policyDoc.active.tproxyEnabled ? "开" : "关"} · sni-rejection{" "}
            {policyDoc.active.sniRejectionEnabled ? "开" : "关"}
            {policyDoc.active.hitlSnoozedUntilMs !== null &&
              ` · hitl snooze 至 ${policyDoc.active.hitlSnoozedUntilMs}`}
          </p>
          <div className="row">
            <button type="button" onClick={() => void toggleEffective("hitl")} disabled={busy}>
              生效 hitl {policyDoc.active.hitlEnabled ? "关" : "开"}
            </button>
            <button type="button" onClick={() => void toggleEffective("mitm")} disabled={busy}>
              生效 mitm {policyDoc.active.mitmEnabled ? "关" : "开"}
            </button>
            <button type="button" onClick={() => void toggleEffective("tproxy")} disabled={busy}>
              生效 tproxy {policyDoc.active.tproxyEnabled ? "关" : "开"}
            </button>
            <button type="button" onClick={() => void toggleEffective("sni")} disabled={busy}>
              生效 sni-rejection {policyDoc.active.sniRejectionEnabled ? "关" : "开"}
            </button>
          </div>
          <p>
            基准开关：
            hitl {policyDoc.defaults.hitlEnabled ? "开" : "关"} · mitm{" "}
            {policyDoc.defaults.mitmEnabled ? "开" : "关"} · tproxy{" "}
            {policyDoc.defaults.tproxyEnabled ? "开" : "关"} · sni-rejection{" "}
            {policyDoc.defaults.sniRejectionEnabled ? "开" : "关"}
          </p>
          <div className="row">
            <button type="button" onClick={() => void toggleDefault("hitl")} disabled={busy}>
              基准 hitl {policyDoc.defaults.hitlEnabled ? "关" : "开"}
            </button>
            <button type="button" onClick={() => void toggleDefault("mitm")} disabled={busy}>
              基准 mitm {policyDoc.defaults.mitmEnabled ? "关" : "开"}
            </button>
            <button type="button" onClick={() => void toggleDefault("tproxy")} disabled={busy}>
              基准 tproxy {policyDoc.defaults.tproxyEnabled ? "关" : "开"}
            </button>
            <button type="button" onClick={() => void toggleDefault("sni")} disabled={busy}>
              基准 sni-rejection {policyDoc.defaults.sniRejectionEnabled ? "关" : "开"}
            </button>
          </div>
          <div className="row">
            <input
              type="text"
              value={snoozeMs}
              placeholder="hitl snooze 到期毫秒（如 4242）"
              onChange={(event) => setSnoozeMs(event.target.value)}
            />
            <button type="button" onClick={() => void setSnooze()} disabled={busy}>
              设 snooze
            </button>
            <button type="button" onClick={() => void clearSnooze()} disabled={busy}>
              清 snooze
            </button>
          </div>
          <p>
            网络规则：
            {policyDoc.networkRules.length === 0
              ? "无"
              : policyDoc.networkRules.map((r) => `${r.pattern} ${r.action}`).join(" · ")}
          </p>
          <div className="row">
            <input
              type="text"
              value={rulePattern}
              placeholder="规则 pattern（如 example.com）"
              onChange={(event) => setRulePattern(event.target.value)}
            />
            <select value={ruleAction} onChange={(event) => setRuleAction(event.target.value)}>
              <option value="deny">deny</option>
              <option value="allow">allow</option>
            </select>
            <button type="button" onClick={() => void addRule()} disabled={busy}>
              添加规则
            </button>
          </div>
          {policyDoc.networkRules.map((rule, index) => (
            <div className="row" key={`${rule.pattern}-${rule.action}-${index}`}>
              <span>
                {rule.pattern} {rule.action}
              </span>
              <button type="button" onClick={() => void removeRule(index)} disabled={busy}>
                删除
              </button>
            </div>
          ))}
          <p>连接器策略：</p>
          {["notes", "mail"].map((connector) => {
            const row = connectorPolicyRow(connector);
            return (
              <div className="row" key={connector}>
                <span>
                  {connector}：{row.allowed ? "允许" : "禁止"}
                </span>
                <button type="button" onClick={() => void toggleConnector(connector)} disabled={busy}>
                  {row.allowed ? "禁" : "允许"}
                </button>
                <button type="button" onClick={() => void resetConnector(connector)} disabled={busy}>
                  恢复基准
                </button>
              </div>
            );
          })}
          <p>
            scope：
            {policyDoc.scopes.length === 0
              ? "无"
              : policyDoc.scopes.map((s) => `${s.scopeType}/${s.scopeId}`).join(" · ")}
          </p>
          <div className="row">
            <input
              type="text"
              value={scopeType}
              placeholder="scope 类型"
              onChange={(event) => setScopeType(event.target.value)}
            />
            <input
              type="text"
              value={scopeId}
              placeholder="scope id"
              onChange={(event) => setScopeId(event.target.value)}
            />
            <button type="button" onClick={() => void grantScope()} disabled={busy}>
              授权
            </button>
          </div>
          {policyDoc.scopes.map((scope) => (
            <div className="row" key={`${scope.scopeType}/${scope.scopeId}`}>
              <span>
                {scope.scopeType}/{scope.scopeId}
              </span>
              <button
                type="button"
                onClick={() => void revokeScope(scope.scopeType, scope.scopeId)}
                disabled={busy}
              >
                撤销
              </button>
            </div>
          ))}
          <div className="row">
            {pendingPolicyReset ? (
              <>
                <span>恢复全部默认将清空覆盖、连接器策略、scope 与规则，不可撤销。确认？</span>
                <button type="button" onClick={() => void resetPolicy()} disabled={busy}>
                  确认恢复
                </button>
                <button type="button" onClick={() => setPendingPolicyReset(false)} disabled={busy}>
                  取消
                </button>
              </>
            ) : (
              <button type="button" onClick={() => setPendingPolicyReset(true)} disabled={busy}>
                恢复全部默认
              </button>
            )}
            {pendingRevokeAll ? (
              <>
                <span>撤销全部 scope 授权，不可撤销。确认？</span>
                <button type="button" onClick={() => void revokeAllScopes()} disabled={busy}>
                  确认撤销全部
                </button>
                <button type="button" onClick={() => setPendingRevokeAll(false)} disabled={busy}>
                  取消
                </button>
              </>
            ) : (
              <button type="button" onClick={() => setPendingRevokeAll(true)} disabled={busy}>
                撤销全部 scope
              </button>
            )}
          </div>
        </div>
      )}

      <p>
        网关：
        {gatewayStatus === null
          ? "未知（等待状态轮询）"
          : `${gatewayStatus.ownership === "owned" ? "自有" : "附着"} · ${gatewayStatus.state === "ready" ? "就绪" : "未响应"} · 端口 ${gatewayStatus.port}`}
      </p>
      <p>
        模型：
        {key === null
          ? "未知"
          : key.configured
            ? `凭据已配置 ${key.redacted ?? ""}`
            : "凭据未配置"}
      </p>
      <p>提醒事项：{remindersText(reminders)}</p>
      <p className="note">提醒事项授权请求在提醒事项页完成，这里只显示状态。</p>

      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p className="notice">{notice}</p>}
    </div>
  );
}
