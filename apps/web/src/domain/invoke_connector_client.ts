import { invoke } from "@tauri-apps/api/core";
import { ApprovalPendingError } from "./agent_client";
import type {
  ConnectorDecision,
  ConnectorGateway,
  MockMessage,
  MockNote,
} from "./connector_client";

// Same pending-approval marker as the agent invoke client:
// the Rust side reports `approval_pending:<tool_call_id>`.
function pendingId(err: unknown): string | null {
  const message = typeof err === "string" ? err : err instanceof Error ? err.message : "";
  const prefix = "approval_pending:";
  return message.startsWith(prefix) ? message.slice(prefix.length) : null;
}

// Connector line over Tauri invoke, backed by the Rust
// ConnectorClient. Invoke argument keys are snake_case to
// match the Rust command parameters exactly.
export class InvokeConnectorClient implements ConnectorGateway {
  async notesList(): Promise<MockNote[]> {
    return invoke<MockNote[]>("connector_notes_list");
  }

  async mailList(): Promise<MockMessage[]> {
    return invoke<MockMessage[]>("connector_mail_list");
  }

  async notesCreate(
    title: string,
    body: string,
    decision: ConnectorDecision,
  ): Promise<MockNote> {
    try {
      return await invoke<MockNote>("connector_notes_create", { title, body, decision });
    } catch (err) {
      const id = pendingId(err);
      if (id !== null) {
        throw new ApprovalPendingError(id);
      }
      throw err;
    }
  }

  async mailSend(
    to: string,
    subject: string,
    body: string,
    decision: ConnectorDecision,
  ): Promise<MockMessage> {
    try {
      return await invoke<MockMessage>("connector_mail_send", { to, subject, body, decision });
    } catch (err) {
      const id = pendingId(err);
      if (id !== null) {
        throw new ApprovalPendingError(id);
      }
      throw err;
    }
  }
}
