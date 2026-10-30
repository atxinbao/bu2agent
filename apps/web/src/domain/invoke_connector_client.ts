import { invoke } from "@tauri-apps/api/core";
import { ApprovalPendingError } from "./agent_client";
import type {
  ConnectorAccount,
  ConnectorConnectInfo,
  ConnectorDecision,
  ConnectorGateway,
  ConnectorInfo,
  MockMessage,
  MockNote,
} from "./connector_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.
interface ServerConnectorInfo {
  id: string;
  name: string;
  capabilities: string[];
  connected: boolean;
}

interface ServerConnectorAccount {
  account_id: string;
  connector_id: string;
  display_name: string;
  status: string;
  linked_ms: number;
}

interface ServerConnectorConnectInfo {
  connector_id: string;
  flow: string;
  requires_account: boolean;
  description: string;
}

function toConnectorInfo(server: ServerConnectorInfo): ConnectorInfo {
  return {
    id: server.id,
    name: server.name,
    capabilities: server.capabilities,
    connected: server.connected,
  };
}

function toConnectorAccount(server: ServerConnectorAccount): ConnectorAccount {
  return {
    accountId: server.account_id,
    connectorId: server.connector_id,
    displayName: server.display_name,
    status: server.status,
    linkedMs: server.linked_ms,
  };
}

function toConnectorConnectInfo(server: ServerConnectorConnectInfo): ConnectorConnectInfo {
  return {
    connectorId: server.connector_id,
    flow: server.flow,
    requiresAccount: server.requires_account,
    description: server.description,
  };
}

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

  async registryList(): Promise<ConnectorInfo[]> {
    const infos = await invoke<ServerConnectorInfo[]>("connector_list");
    return infos.map(toConnectorInfo);
  }

  async registryGet(id: string): Promise<ConnectorInfo> {
    const info = await invoke<ServerConnectorInfo>("connector_get", { id });
    return toConnectorInfo(info);
  }

  async accounts(id: string): Promise<ConnectorAccount[]> {
    const accounts = await invoke<ServerConnectorAccount[]>("connector_accounts", { id });
    return accounts.map(toConnectorAccount);
  }

  async linkAccount(id: string, displayName?: string | null): Promise<ConnectorAccount> {
    const account = await invoke<ServerConnectorAccount>("connector_account_link", {
      id,
      display_name: displayName ?? null,
    });
    return toConnectorAccount(account);
  }

  async unlinkAccount(id: string, accountId: string): Promise<ConnectorAccount> {
    const account = await invoke<ServerConnectorAccount>("connector_account_unlink", {
      id,
      account_id: accountId,
    });
    return toConnectorAccount(account);
  }

  async connectInfo(id: string): Promise<ConnectorConnectInfo> {
    const info = await invoke<ServerConnectorConnectInfo>("connector_connect_info", { id });
    return toConnectorConnectInfo(info);
  }

  async connect(id: string): Promise<ConnectorInfo> {
    const info = await invoke<ServerConnectorInfo>("connector_connect", { id });
    return toConnectorInfo(info);
  }

  async disconnect(id: string): Promise<ConnectorInfo> {
    const info = await invoke<ServerConnectorInfo>("connector_disconnect", { id });
    return toConnectorInfo(info);
  }
}
