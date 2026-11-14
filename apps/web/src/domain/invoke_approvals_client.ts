import { invoke } from "@tauri-apps/api/core";
import type {
  ApprovalView,
  ApprovalsGateway,
  EditableMessage,
} from "./approvals_client";

// The Rust approval view serializes snake_case; the domain shape
// is camelCase, so the mapping lives here, not in the view.
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

function toBinding(
  server: ServerApprovalBinding | null,
): ApprovalView["binding"] {
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

// Approvals line over Tauri invoke, backed by the Rust
// ApprovalsClient. Invoke argument keys are snake_case to match
// the Rust command parameters exactly.
export class InvokeApprovalsClient implements ApprovalsGateway {
  async list(): Promise<ApprovalView[]> {
    const rows = await invoke<ServerApprovalView[]>("approvals_list", {});
    return rows.map(toRow);
  }

  async get(approvalId: string): Promise<ApprovalView> {
    const row = await invoke<ServerApprovalView>("approvals_get", {
      approval_id: approvalId,
    });
    return toRow(row);
  }

  async editableMessage(approvalId: string): Promise<EditableMessage> {
    const row = await invoke<ServerEditableMessage>("approvals_editable_message", {
      approval_id: approvalId,
    });
    return toEditable(row);
  }

  async editMessage(approvalId: string, message: string): Promise<ApprovalView> {
    const row = await invoke<ServerApprovalView>("approvals_edit_message", {
      approval_id: approvalId,
      message,
    });
    return toRow(row);
  }
}
