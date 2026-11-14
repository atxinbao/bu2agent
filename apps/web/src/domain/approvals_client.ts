// Approvals client for the TS Domain Store.
//
// Approvals only: list, get, editable-message, edit-message.
// FU-3A (Q-R0-01): the `/approvals` family had no production
// client; this wires it for the WorkbenchView approvals section.
//
// Edit semantics (verified in SQLite mode, Q-FU-02): only a
// pending, unconsumed, unbound approval is editable; a
// successful edit moves it to `edited` and bumps `version`.
// The edit response itself carries null versions (the map-built
// view has no version chain), so callers must re-read with
// `get()` after an edit instead of trusting the response.

export interface ApprovalBinding {
  loopId: string;
  callId: number;
  tool: string;
  argsSha256: string;
}

export interface ApprovalView {
  approvalId: string;
  toolCallId: string;
  tool: string;
  status: string;
  consumed: boolean;
  bound: boolean;
  binding: ApprovalBinding | null;
  operationDigest: string | null;
  effectBinding: string | null;
  message: string | null;
  consumedByWorkerSessionId: string | null;
  // D1-2 version chain; null in JSONL/shadow views, where the
  // in-memory map carries no version.
  version: number | null;
  supersedesVersion: number | null;
}

export interface EditableMessage {
  approvalId: string;
  message: string | null;
  editable: boolean;
}

interface ServerApprovalBinding {
  loop_id: string;
  call_id: number;
  tool: string;
  args_sha256: string;
}

interface ServerApprovalView {
  approval_id: string;
  tool_call_id: string;
  tool: string;
  status: string;
  consumed: boolean;
  bound: boolean;
  binding: ServerApprovalBinding | null;
  operation_digest: string | null;
  effect_binding: string | null;
  message: string | null;
  consumed_by_worker_session_id: string | null;
  version: number | null;
  supersedes_version: number | null;
}

interface ServerEditableMessage {
  approval_id: string;
  message: string | null;
  editable: boolean;
}

function toBinding(server: ServerApprovalBinding | null): ApprovalBinding | null {
  if (!server) return null;
  return {
    loopId: server.loop_id,
    callId: server.call_id,
    tool: server.tool,
    argsSha256: server.args_sha256,
  };
}

function toRow(server: ServerApprovalView): ApprovalView {
  return {
    approvalId: server.approval_id,
    toolCallId: server.tool_call_id,
    tool: server.tool,
    status: server.status,
    consumed: server.consumed,
    bound: server.bound,
    binding: toBinding(server.binding),
    operationDigest: server.operation_digest,
    effectBinding: server.effect_binding,
    message: server.message,
    consumedByWorkerSessionId: server.consumed_by_worker_session_id,
    version: server.version,
    supersedesVersion: server.supersedes_version,
  };
}

function toEditable(server: ServerEditableMessage): EditableMessage {
  return {
    approvalId: server.approval_id,
    message: server.message,
    editable: server.editable,
  };
}

// Transport-neutral surface used by the views. The HTTP client
// below and the invoke client both implement it, so the view
// never knows which transport it runs on.
export interface ApprovalsGateway {
  list(): Promise<ApprovalView[]>;
  get(approvalId: string): Promise<ApprovalView>;
  editableMessage(approvalId: string): Promise<EditableMessage>;
  editMessage(approvalId: string, message: string): Promise<ApprovalView>;
}

export class ApprovalsClient implements ApprovalsGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, init);
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async list(): Promise<ApprovalView[]> {
    const rows = await this.request<ServerApprovalView[]>("/approvals");
    return rows.map(toRow);
  }

  async get(approvalId: string): Promise<ApprovalView> {
    const row = await this.request<ServerApprovalView>(`/approvals/${approvalId}`);
    return toRow(row);
  }

  async editableMessage(approvalId: string): Promise<EditableMessage> {
    const row = await this.request<ServerEditableMessage>(
      `/approvals/${approvalId}/editable-message`,
    );
    return toEditable(row);
  }

  async editMessage(approvalId: string, message: string): Promise<ApprovalView> {
    const row = await this.request<ServerApprovalView>(
      `/approvals/${approvalId}/edit-message`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message }),
      },
    );
    return toRow(row);
  }
}
