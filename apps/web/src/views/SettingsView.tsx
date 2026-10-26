import { useEffect, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import type {
  InvokeSettingsClient,
  KeyStatus,
  SettingsData,
} from "../domain/settings_client";

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

  const owned = gatewayStatus?.ownership === "owned";
  const attached = gatewayStatus?.ownership === "attached";

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

      <h3>状态检查</h3>
      <p className="note">三项分开展示：网关状态、模型凭据配置、提醒事项授权。此行只反映凭据配置与网关可达，不做真实模型调用。</p>
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
