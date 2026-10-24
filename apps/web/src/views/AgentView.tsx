import { useState } from "react";
import {
  toolRequiresApproval,
  type AgentGateway,
  type ToolResult,
} from "../domain/agent_client";

const TOOLS = ["fs.list", "fs.read", "echo", "fs.write", "fs.delete"] as const;

// Agent view over the AgentGateway interface. Approval tools
// show a confirm step first and only then run with "approve";
// rejecting just stops, it never sends a rejected record to
// be refused server-side. Non-approval tools run directly.
// Argument shapes come from the Mock server: echo takes text,
// fs.list takes an optional path, fs.read/fs.delete take path,
// fs.write takes path and text.
export function AgentView({ client }: { client: AgentGateway }) {
  const [tool, setTool] = useState<(typeof TOOLS)[number]>("echo");
  const [path, setPath] = useState("");
  const [text, setText] = useState("");
  const [pending, setPending] = useState<Record<string, string> | null>(null);
  const [result, setResult] = useState<ToolResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  function buildArgs(): Record<string, string> {
    switch (tool) {
      case "fs.list":
        return path === "" ? {} : { path };
      case "fs.read":
      case "fs.delete":
        return { path };
      case "echo":
        return { text };
      case "fs.write":
        return { path, text };
    }
  }

  async function execute(args: Record<string, string>) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      setResult(await client.runTool(tool, args, "approve"));
    } catch (err) {
      setError(err instanceof Error ? err.message : "tool call failed");
    } finally {
      setBusy(false);
    }
  }

  function run() {
    const args = buildArgs();
    if (toolRequiresApproval(tool) === true) {
      setPending(args);
      return;
    }
    void execute(args);
  }

  function showResult(value: unknown): string {
    return typeof value === "string" ? value : JSON.stringify(value, null, 2);
  }

  return (
    <div className="view">
      <h2>Agent tools</h2>
      <p className="note">
        fs.list and fs.read need no approval; echo is a Mock-only demo tool. fs.write and
        fs.delete ask for approval first, and the server re-checks its own record before
        executing.
      </p>
      <div className="row">
        <select value={tool} onChange={(event) => setTool(event.target.value as typeof tool)}>
          {TOOLS.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
        <input
          type="text"
          value={path}
          placeholder="Path (fs tools; empty = root for fs.list)"
          onChange={(event) => setPath(event.target.value)}
        />
      </div>
      {(tool === "echo" || tool === "fs.write") && (
        <textarea
          value={text}
          placeholder={tool === "echo" ? "Text to echo" : "Text to write"}
          onChange={(event) => setText(event.target.value)}
        />
      )}
      <button type="button" onClick={run} disabled={busy}>
        Run tool
      </button>
      {pending !== null && (
        <div className="confirm">
          <span>
            {tool} needs approval. Args: {JSON.stringify(pending)}
          </span>
          <button
            type="button"
            onClick={() => {
              const args = pending;
              setPending(null);
              void execute(args);
            }}
            disabled={busy}
          >
            Approve and run
          </button>
          <button
            type="button"
            onClick={() => {
              setPending(null);
              setNotice("rejected: nothing was executed");
            }}
            disabled={busy}
          >
            Reject
          </button>
        </div>
      )}
      {result !== null && (
        <div>
          <h3>Result from {result.tool}</h3>
          <pre className="result">{showResult(result.result)}</pre>
        </div>
      )}
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p className="notice">{notice}</p>}
    </div>
  );
}
