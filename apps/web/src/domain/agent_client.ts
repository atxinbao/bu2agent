// Agent invocation client for the TS Domain Store (Phase 3).
//
// Same loop as the Rust agent client, in the same order:
// 1. Client-side registry gate: unknown tools are rejected
//    before any request; approval tools are not executed
//    without a decided approval record.
// 2. POST /approval/request opens a pending record for
//    approval tools (non-approval tools need no record).
//    Round 2: file tools present the exact operation in the
//    request, so the server binds the record to it.
// 3. POST /approval/decide records the user's decision.
// 4. POST /agent/tool/call executes, and the server re-checks
//    its own record: no record, pending, or rejected records
//    are refused server-side, even if the client claimed
//    approval. The client gate is defense in depth; the
//    server gate is the enforcement point.
//
// `echo` is a Mock-only demo tool (no approval), not a real
// tool; the registry tools are fs.list / fs.read (no
// approval) and fs.write / fs.delete (approval).

export type ToolDecision = "ask" | "approve" | "reject";

export interface ToolResult {
  tool: string;
  result: unknown;
}

export class ApprovalPendingError extends Error {
  readonly toolCallId: string;

  constructor(toolCallId: string) {
    super(`approval required: tool_call_id=${toolCallId}`);
    this.toolCallId = toolCallId;
  }
}

export interface ApprovalRequestResponse {
  tool_call_id: string | null;
  requires_approval: boolean;
  status: string;
}

// A 403 from the server approval gate. Deliberately neutral:
// a direct call carries no decision context, so it must not
// claim a user rejection. `runTool` rewords it by the
// decision it made (approved -> mismatch, rejected -> rejected).
export class ApprovalGateError extends Error {
  constructor() {
    super("tool call refused by the server approval gate");
  }
}

interface ToolCallResponse {
  tool: string;
  result: unknown;
}

function encodeBase64(text: string): string {
  const data = new TextEncoder().encode(text);
  let binary = "";
  for (const byte of data) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary);
}

function requiresApproval(tool: string): boolean | undefined {
  switch (tool) {
    case "fs.list":
    case "fs.read":
    case "echo":
      return false;
    case "fs.write":
    case "fs.delete":
      return true;
    default:
      return undefined;
  }
}

// Transport-neutral surface used by the Agent view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface AgentGateway {
  requestApproval(tool: string, args?: Record<string, string>): Promise<ApprovalRequestResponse>;
  decideApproval(toolCallId: string, decision: ToolDecision): Promise<void>;
  callTool(tool: string, args: Record<string, string>, toolCallId?: string): Promise<ToolResult>;
  runTool(tool: string, args: Record<string, string>, decision: ToolDecision): Promise<ToolResult>;
}

// The Agent view needs to know whether a tool needs approval
// before it runs, so it can show the confirm step first. This
// mirrors the client registry gate, without exposing it.
export function toolRequiresApproval(tool: string): boolean | undefined {
  return requiresApproval(tool);
}

export class AgentClient implements AgentGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (response.status === 403) {
      throw new ApprovalGateError();
    }
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async requestApproval(
    tool: string,
    args?: Record<string, string>,
  ): Promise<ApprovalRequestResponse> {
    const requires = requiresApproval(tool);
    if (requires === undefined) {
      throw new Error("unknown tool");
    }
    // Round 2: file tools present the exact operation, so the
    // server binds the record to it. Agent writes are always
    // overwrite (the tool call has no append mode).
    const body: Record<string, unknown> = { tool };
    if (tool === "fs.write") {
      if (args?.path === undefined || args?.text === undefined) {
        throw new Error("fs.write approval requires path and text");
      }
      body.path = args.path;
      body.content_base64 = encodeBase64(args.text);
      body.append = false;
    } else if (tool === "fs.delete") {
      if (args?.path === undefined) {
        throw new Error("fs.delete approval requires path");
      }
      body.path = args.path;
    }
    const response = await this.post<ApprovalRequestResponse>("/approval/request", body);
    if (response.requires_approval !== requires) {
      throw new Error("mock gateway disagrees on tool approval flag");
    }
    return response;
  }

  async decideApproval(toolCallId: string, decision: ToolDecision): Promise<void> {
    if (decision === "ask") {
      throw new Error("ask is not a decision");
    }
    const status = decision === "approve" ? "approved" : "rejected";
    await this.post("/approval/decide", { tool_call_id: toolCallId, status });
  }

  async callTool(
    tool: string,
    args: Record<string, string>,
    toolCallId?: string,
  ): Promise<ToolResult> {
    if (requiresApproval(tool) === undefined) {
      throw new Error("unknown tool");
    }
    const response = await this.post<ToolCallResponse>("/agent/tool/call", {
      tool,
      args,
      tool_call_id: toolCallId ?? null,
    });
    return { tool: response.tool, result: response.result };
  }

  async runTool(
    tool: string,
    args: Record<string, string>,
    decision: ToolDecision,
  ): Promise<ToolResult> {
    const requires = requiresApproval(tool);
    if (requires === undefined) {
      throw new Error("unknown tool");
    }
    if (!requires) {
      return this.callTool(tool, args);
    }
    const request = await this.requestApproval(tool, args);
    if (!request.tool_call_id) {
      throw new Error("approval tool returned no tool_call_id");
    }
    if (decision === "ask") {
      throw new ApprovalPendingError(request.tool_call_id);
    }
    await this.decideApproval(request.tool_call_id, decision);
    try {
      return await this.callTool(tool, args, request.tool_call_id);
    } catch (err) {
      // The decision is known here, so the 403 wording can be
      // exact: an approved call refused after approval is a
      // mismatch (changed, already used, or expired), never a
      // user rejection.
      if (err instanceof ApprovalGateError) {
        if (decision === "approve") {
          throw new Error(
            "approved operation does not match this execution (changed, already used, or expired); nothing was executed",
          );
        }
        throw new Error("tool call refused: approval was rejected");
      }
      throw err;
    }
  }
}
