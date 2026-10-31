import { invoke } from "@tauri-apps/api/core";
import { useEffect, useRef, useState } from "react";
import {
  initialChatState,
  runTurn,
  type ChatState,
} from "./domain/chat";
import {
  GatewayChatClient,
  type ChatGateway,
} from "./domain/gateway_client";
import { InvokeGatewayClient } from "./domain/invoke_gateway_client";
import {
  FileGatewayClient,
  type FileGateway,
} from "./domain/file_client";
import { InvokeFileClient } from "./domain/invoke_file_client";
import {
  AgentClient,
  type AgentGateway,
} from "./domain/agent_client";
import { InvokeAgentClient } from "./domain/invoke_agent_client";
import {
  ConnectorClient,
  type ConnectorGateway,
} from "./domain/connector_client";
import { InvokeConnectorClient } from "./domain/invoke_connector_client";
import { InvokeCredentialVaultClient } from "./domain/invoke_credential_vault_client";
import { InvokePermissionPolicyClient } from "./domain/invoke_permission_policy_client";
import { InvokeModelClient } from "./domain/invoke_model_client";
import {
  TaskClient,
  type TaskGateway,
} from "./domain/task_client";
import { InvokeTaskClient } from "./domain/invoke_task_client";
import {
  DeviceClient,
  type DeviceGateway,
} from "./domain/device_client";
import { InvokeDeviceClient } from "./domain/invoke_device_client";
import { FilesView } from "./views/FilesView";
import { AgentView } from "./views/AgentView";
import { ConnectorsView } from "./views/ConnectorsView";
import { TasksView } from "./views/TasksView";
import { DevicesView } from "./views/DevicesView";
import { LoopClient, type LoopGateway } from "./domain/loop_client";
import { InvokeLoopClient } from "./domain/invoke_loop_client";
import { LoopsView } from "./views/LoopsView";
import { SessionClient, type SessionGateway } from "./domain/session_client";
import { InvokeSessionClient } from "./domain/invoke_session_client";
import { WorkbenchView } from "./views/WorkbenchView";
import {
  InvokeSettingsClient,
  type SettingsData,
} from "./domain/settings_client";
import { SettingsView } from "./views/SettingsView";

// Browser-preview fallback: plain HTTP to the Mock Gateway.
// In a Tauri window the invoke clients below replace them, so
// the Rust client owns the transport and the page never sees
// the gateway URL.
const MOCK_BASE_URL = "http://127.0.0.1:3001";

const httpClient = new GatewayChatClient(MOCK_BASE_URL);

interface ShellInfo {
  name: string;
  version: string;
}

// 壳内网关状态（gateway_status 命令的返回形状）。HTTP 预览没有
// 这个命令，状态只在 invoke 传输下存在。
interface GatewayStatus {
  endpoint: string;
  port: number;
  ownership: "owned" | "attached";
  state: "ready" | "down";
  foreign_port: boolean;
  error: string | null;
}

// One client per mainline, chosen together with the transport:
// either all invoke (Tauri window) or all HTTP (browser
// preview). Views take these interfaces, never a concrete
// client, so they work identically on both transports.
interface Clients {
  chat: ChatGateway;
  files: FileGateway;
  agent: AgentGateway;
  connectors: ConnectorGateway;
  tasks: TaskGateway;
  devices: DeviceGateway;
  loops: LoopGateway;
  sessions: SessionGateway;
}

type Tab = "workbench" | "chat" | "files" | "agent" | "connectors" | "tasks" | "devices" | "loops" | "settings";

// Visible navigation: the workbench (sessions) is the only
// chat entry and the default view (R1, 2026-10-28). The legacy
// Chat tab is not listed here; its code and the /chat/* gateway
// interface stay as a compatibility layer only.
const TABS: { id: Tab; label: string }[] = [
  { id: "workbench", label: "工作台" },
  { id: "files", label: "Files" },
  { id: "agent", label: "Agent" },
  { id: "connectors", label: "Connectors" },
  { id: "tasks", label: "Tasks" },
  { id: "devices", label: "Devices" },
  { id: "loops", label: "Loops" },
  { id: "settings", label: "设置" },
];

export function App() {
  const [tab, setTab] = useState<Tab>("workbench");
  const [chat, setChat] = useState<ChatState>(initialChatState);
  const [gatewayOk, setGatewayOk] = useState<boolean | null>(null);
  const [gatewayStatus, setGatewayStatus] = useState<GatewayStatus | null>(null);
  const [isInvoke, setIsInvoke] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const [restartError, setRestartError] = useState<string | null>(null);
  const [shell, setShell] = useState<string>("checking shell bridge…");
  const [transport, setTransport] = useState<string | null>(null);
  // Default working directory (root-relative, "" = file root) from
  // the settings page. Files/Tasks tabs mount fresh on every
  // switch, so they pick up the latest value as their initial
  // directory; existing tasks and artifacts are untouched.
  // When the stored default is invalid, the views get valid=false
  // and must NOT fall back to the root for writes: an invalid
  // default is never a usable default, it only asks for a
  // re-selection (or an explicitly typed path).
  const [defaultDir, setDefaultDir] = useState<string | null>(null);
  const [defaultDirValid, setDefaultDirValid] = useState(true);
  const [clients, setClients] = useState<Clients | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const chatRef = useRef(chat);
  chatRef.current = chat;
  const clientsRef = useRef<Clients | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const turnCounter = useRef(0);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      // Transport selection: a working ping means this page
      // runs inside the Tauri shell, so every line goes
      // through the Rust client via invoke. Otherwise (plain
      // browser preview) it falls back to HTTP. The choice is
      // shown in the status line, never silent.
      let selected: Clients;
      let transportName: string;
      let invokeTransport = false;
      try {
        const pong = await invoke<string>("ping");
        const info = await invoke<ShellInfo>("app_info");
        invokeTransport = true;
        if (!cancelled) {
          setShell(`shell bridge ok: ping=${pong}, ${info.name} ${info.version}`);
          setIsInvoke(true);
        }
        selected = {
          chat: new InvokeGatewayClient(),
          files: new InvokeFileClient(),
          agent: new InvokeAgentClient(),
          connectors: new InvokeConnectorClient(),
          tasks: new InvokeTaskClient(),
          devices: new InvokeDeviceClient(),
          loops: new InvokeLoopClient(),
          sessions: new InvokeSessionClient(),
        };
        transportName = "invoke (Rust client)";
      } catch {
        if (!cancelled) {
          setShell("shell bridge unavailable (browser preview, not a Tauri window)");
          setIsInvoke(false);
        }
        selected = {
          chat: httpClient,
          files: new FileGatewayClient(MOCK_BASE_URL),
          agent: new AgentClient(MOCK_BASE_URL),
          connectors: new ConnectorClient(MOCK_BASE_URL),
          tasks: new TaskClient(MOCK_BASE_URL),
          devices: new DeviceClient(MOCK_BASE_URL),
          loops: new LoopClient(MOCK_BASE_URL),
          sessions: new SessionClient(MOCK_BASE_URL),
        };
        transportName = "http (browser preview)";
      }
      if (cancelled) {
        return;
      }
      clientsRef.current = selected;
      setClients(selected);
      setTransport(transportName);
      if (invokeTransport) {
        try {
          const s = await invoke<SettingsData>("settings_get");
          if (!cancelled) {
            setDefaultDir(s.default_working_dir);
            setDefaultDirValid(s.working_dir_valid);
          }
        } catch {
          // Settings stay at the root default; the settings
          // page itself reports the failure when opened.
        }
      }
      const ok = await selected.chat.health();
      if (!cancelled) {
        setGatewayOk(ok);
      }
      try {
        const messages = await selected.chat.history();
        if (!cancelled) {
          setChat((prev) => ({ ...prev, messages }));
        }
      } catch {
        // History is best-effort at startup; the error line
        // below the composer reports gateway failures on send.
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // HTTP 预览没有 status 命令，沿用 health 复查：启动检查失败
  // 可能只是网关尚未就绪，不能让 unreachable 一直保留。仅失败
  // 时每两秒复查；成功或卸载后停止，网关未启动时仍显示不可达。
  // invoke 传输走下面那条 status 轮询，不并存两套定时器。
  useEffect(() => {
    if (gatewayOk !== false || isInvoke) {
      return;
    }
    let cancelled = false;
    let inFlight = false;
    const timer = setInterval(() => {
      if (inFlight) {
        return;
      }
      inFlight = true;
      (async () => {
        try {
          const ok = await clientsRef.current?.chat.health();
          if (!cancelled && ok) {
            setGatewayOk(true);
          }
        } catch {
          // 两种传输失败时均返回 false；这里兜底处理意外抛错。
        } finally {
          inFlight = false;
        }
      })();
    }, 2000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [gatewayOk, isInvoke]);

  // invoke 传输下持续轮询壳内网关状态：启动未就绪与运行中崩溃
  // 都要翻转显示。状态来自壳命令的新鲜 health 检查，不是缓存。
  useEffect(() => {
    if (!isInvoke) {
      return;
    }
    let cancelled = false;
    let inFlight = false;
    const poll = async () => {
      if (inFlight) {
        return;
      }
      inFlight = true;
      try {
        const status = await invoke<GatewayStatus>("gateway_status");
        if (!cancelled) {
          setGatewayStatus(status);
          setGatewayOk(status.state === "ready");
        }
      } catch {
        // 壳命令失败时不改写状态，下一轮再试。
      } finally {
        inFlight = false;
      }
    };
    void poll();
    const timer = setInterval(() => void poll(), 5000);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [isInvoke]);

  // 仅自有网关可重启：附着网关会被后端拒绝，故不给按钮。
  async function restartGateway() {
    setRestarting(true);
    setRestartError(null);
    try {
      const status = await invoke<GatewayStatus>("gateway_restart");
      setGatewayStatus(status);
      setGatewayOk(status.state === "ready");
    } catch (err) {
      setRestartError(err instanceof Error ? err.message : typeof err === "string" ? err : "restart failed");
    } finally {
      setRestarting(false);
    }
  }

  // 保存设置或进入文件/任务页前重新校验，捕获 Finder 改名等外部变化。
  async function refreshSettings() {
    if (!isInvoke) {
      return;
    }
    try {
      const s = await invoke<SettingsData>("settings_get");
      setDefaultDir(s.default_working_dir);
      setDefaultDirValid(s.working_dir_valid);
    } catch {
      // 无法校验时不把缓存目录当作有效默认值，显式路径仍可使用。
      setDefaultDirValid(false);
    }
  }

  async function send() {
    const input = inputRef.current;
    const gateway = clientsRef.current?.chat ?? null;
    if (!input || busy || gateway === null) {
      return;
    }
    const text = input.value.trim();
    if (text === "") {
      return;
    }
    input.value = "";
    setBusy(true);
    setError(null);
    turnCounter.current += 1;
    const n = turnCounter.current;
    try {
      // The input is deliberately uncontrolled: a controlled
      // React input can fight IME composition (see the
      // framework decision notes). The value is read once here.
      const result = await runTurn(gateway, chatRef.current, {
        streamId: `stream-${Date.now()}-${n}`,
        turnId: `turn-${Date.now()}-${n}`,
        text,
      });
      chatRef.current = result.state;
      setChat(result.state);
    } catch (err) {
      // Domain clients surface only the HTTP status, never the
      // response body, so this message is safe to display.
      setError(err instanceof Error ? err.message : typeof err === "string" ? err : "send failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="app">
      <header className="app-header">
        <h1>bu2agent</h1>
        <div className="status-line">
          <span>
            gateway:{" "}
            {isInvoke && gatewayStatus !== null
              ? gatewayStatus.state === "ready"
                ? `ok (端口 ${gatewayStatus.port}${gatewayStatus.foreign_port ? "，外来占用" : ""})`
                : `down${gatewayStatus.error ? `（${gatewayStatus.error}）` : ""}`
              : gatewayOk === null
                ? "…"
                : gatewayOk
                  ? "ok"
                  : "unreachable (is mock-gateway running?)"}
          </span>
          {isInvoke && gatewayStatus !== null && gatewayStatus.state === "down" && gatewayStatus.ownership === "owned" && (
            <button type="button" onClick={() => void restartGateway()} disabled={restarting}>
              {restarting ? "重启中…" : "重启网关"}
            </button>
          )}
          {isInvoke && gatewayStatus !== null && gatewayStatus.state === "down" && gatewayStatus.ownership === "attached" && (
            <span>附着的网关未响应（本应用不管理它）</span>
          )}
          {restartError !== null && <span className="error">{restartError}</span>}
          <span>{shell}</span>
          <span>transport: {transport ?? "…"}</span>
        </div>
      </header>
      <nav className="tabs">
        {TABS.map((entry) => (
          <button
            key={entry.id}
            type="button"
            className={tab === entry.id ? "active" : ""}
            onClick={async () => {
              if (entry.id === "files" || entry.id === "tasks") {
                await refreshSettings();
              }
              setTab(entry.id);
            }}
          >
            {entry.label}
          </button>
        ))}
      </nav>
      {tab === "chat" && (
        <>
          <main className="messages">
            {chat.messages.length === 0 && (
              <p className="empty">No messages yet. Send one below.</p>
            )}
            {chat.messages.map((message) => (
              <div key={message.turnId + message.role} className={`message ${message.role}`}>
                <div className="role">{message.role === "user" ? "You" : "Assistant"}</div>
                <div className="text">{message.text}</div>
              </div>
            ))}
            {chat.activeTurnState === "streaming" && <p className="empty">Streaming…</p>}
          </main>
          <footer className="composer">
            <input
              ref={inputRef}
              type="text"
              placeholder="Type a message…"
              disabled={busy || transport === null}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  void send();
                }
              }}
            />
            <button type="button" onClick={() => void send()} disabled={busy || transport === null}>
              {busy ? "Sending…" : "Send"}
            </button>
          </footer>
          {error !== null && <p className="error">{error}</p>}
          <p className="note">
            当前聊天在回复完成后一次显示全部内容。每次提问独立处理，不携带前文。
          </p>
        </>
      )}
      {tab === "workbench" &&
        (clients === null ? (
          <p className="empty">Choosing transport…</p>
        ) : (
          <WorkbenchView
            sessions={clients.sessions}
            tasks={clients.tasks}
            loops={clients.loops}
            agent={clients.agent}
            files={clients.files}
            onNavigate={(target) => setTab(target)}
          />
        ))}
      {tab === "files" &&
        (clients === null ? (
          <p className="empty">Choosing transport…</p>
        ) : (
          <FilesView
            client={clients.files}
            defaultDir={defaultDir ?? ""}
            defaultDirValid={defaultDirValid}
          />
        ))}
      {tab === "agent" &&
        (clients === null ? (
          <p className="empty">Choosing transport…</p>
        ) : (
          <AgentView client={clients.agent} />
        ))}
      {tab === "connectors" &&
        (clients === null ? (
          <p className="empty">Choosing transport…</p>
        ) : (
          <ConnectorsView client={clients.connectors} />
        ))}
      {tab === "tasks" &&
        (clients === null ? (
          <p className="empty">Choosing transport…</p>
        ) : (
          <TasksView
            chat={clients.chat}
            files={clients.files}
            agent={clients.agent}
            tasks={clients.tasks}
            defaultDir={defaultDir ?? ""}
            defaultDirValid={defaultDirValid}
          />
        ))}
      {tab === "devices" &&
        (clients === null ? (
          <p className="empty">Choosing transport…</p>
        ) : (
          <DevicesView agent={clients.agent} devices={clients.devices} />
        ))}
      {tab === "loops" && clients && <LoopsView loops={clients.loops} agent={clients.agent} />}
      {tab === "settings" && (
        <SettingsView
          settings={new InvokeSettingsClient()}
          vault={new InvokeCredentialVaultClient()}
          policy={new InvokePermissionPolicyClient()}
          model={new InvokeModelClient()}
          isInvoke={isInvoke}
          gatewayStatus={gatewayStatus}
          onSettingsChanged={() => void refreshSettings()}
        />
      )}
    </div>
  );
}
