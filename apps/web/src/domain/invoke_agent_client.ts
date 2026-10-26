import { invoke } from "@tauri-apps/api/core";
import {
  ApprovalPendingError,
  type AgentGateway,
  type ApprovalRequestResponse,
  type ToolDecision,
  type ToolResult,
} from "./agent_client";

// The Rust side reports a pending approval as the marker
// string `approval_pending:<tool_call_id>`; the HTTP client
// throws ApprovalPendingError for the same case, so this
// mapping keeps both transports indistinguishable to views.
function pendingId(err: unknown): string | null {
  const message = typeof err === "string" ? err : err instanceof Error ? err.message : "";
  const prefix = "approval_pending:";
  return message.startsWith(prefix) ? message.slice(prefix.length) : null;
}

// Agent line over Tauri invoke, backed by the Rust AgentClient.
// Invoke argument keys are snake_case to match the Rust
// command parameters exactly.
export class InvokeAgentClient implements AgentGateway {
  async requestApproval(
    tool: string,
    args?: Record<string, string>,
  ): Promise<ApprovalRequestResponse> {
    return invoke<ApprovalRequestResponse>("agent_request_approval", {
      tool,
      args: args ?? null,
    });
  }

  async decideApproval(toolCallId: string, decision: ToolDecision): Promise<void> {
    await invoke("agent_decide_approval", { tool_call_id: toolCallId, decision });
  }

  async callTool(
    tool: string,
    args: Record<string, string>,
    toolCallId?: string,
  ): Promise<ToolResult> {
    return invoke<ToolResult>("agent_call_tool", {
      tool,
      args,
      tool_call_id: toolCallId ?? null,
    });
  }

  async runTool(
    tool: string,
    args: Record<string, string>,
    decision: ToolDecision,
  ): Promise<ToolResult> {
    try {
      return await invoke<ToolResult>("agent_run_tool", { tool, args, decision });
    } catch (err) {
      const id = pendingId(err);
      if (id !== null) {
        throw new ApprovalPendingError(id);
      }
      throw err;
    }
  }
}
