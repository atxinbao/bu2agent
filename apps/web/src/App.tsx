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
}

type Tab = "chat" | "files" | "agent" | "connectors" | "tasks" | "devices";

const TABS: { id: Tab; label: string }[] = [
  { id: "chat", label: "Chat" },
  { id: "files", label: "Files" },
  { id: "agent", label: "Agent" },
  { id: "connectors", label: "Connectors" },
  { id: "tasks", label: "Tasks" },
  { id: "devices", label: "Devices" },
];

export function App() {
  const [tab, setTab] = useState<Tab>("chat");
  const [chat, setChat] = useState<ChatState>(initialChatState);
  const [gatewayOk, setGatewayOk] = useState<boolean | null>(null);
  const [shell, setShell] = useState<string>("checking shell bridge…");
  const [transport, setTransport] = useState<string | null>(null);
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
      try {
        const pong = await invoke<string>("ping");
        const info = await invoke<ShellInfo>("app_info");
        if (!cancelled) {
          setShell(`shell bridge ok: ping=${pong}, ${info.name} ${info.version}`);
        }
        selected = {
          chat: new InvokeGatewayClient(),
          files: new InvokeFileClient(),
          agent: new InvokeAgentClient(),
          connectors: new InvokeConnectorClient(),
          tasks: new InvokeTaskClient(),
          devices: new InvokeDeviceClient(),
        };
        transportName = "invoke (Rust client)";
      } catch {
        if (!cancelled) {
          setShell("shell bridge unavailable (browser preview, not a Tauri window)");
        }
        selected = {
          chat: httpClient,
          files: new FileGatewayClient(MOCK_BASE_URL),
          agent: new AgentClient(MOCK_BASE_URL),
          connectors: new ConnectorClient(MOCK_BASE_URL),
          tasks: new TaskClient(MOCK_BASE_URL),
          devices: new DeviceClient(MOCK_BASE_URL),
        };
        transportName = "http (browser preview)";
      }
      if (cancelled) {
        return;
      }
      clientsRef.current = selected;
      setClients(selected);
      setTransport(transportName);
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
            {gatewayOk === null ? "…" : gatewayOk ? "ok" : "unreachable (is mock-gateway running?)"}
          </span>
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
            onClick={() => setTab(entry.id)}
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
      {tab === "files" &&
        (clients === null ? (
          <p className="empty">Choosing transport…</p>
        ) : (
          <FilesView client={clients.files} />
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
          />
        ))}
      {tab === "devices" &&
        (clients === null ? (
          <p className="empty">Choosing transport…</p>
        ) : (
          <DevicesView agent={clients.agent} devices={clients.devices} />
        ))}
    </div>
  );
}
