import { invoke } from "@tauri-apps/api/core";
import type {
  CaptureTicket,
  CatalogEntry,
  CredentialSlot,
  CustomConnector,
  VaultGateway,
} from "./credential_vault_client";

// The Rust structs serialize snake_case; the domain shape is
// camelCase, so the mapping lives here, not in the view.
interface ServerCredentialSlot {
  handle: string;
  provider: string | null;
  redacted: string | null;
  created_ms: number;
  updated_ms: number;
}

interface ServerCatalogEntry {
  id: string;
  display_name: string;
  description: string | null;
  created_ms: number;
  updated_ms: number;
}

interface ServerCustomConnector {
  id: string;
  display_name: string;
  description: string | null;
  created_ms: number;
  updated_ms: number;
}

interface ServerCaptureTicket {
  ticket_id: string;
  provider: string;
  handle: string;
  status: string;
  created_ms: number;
  expires_ms: number;
}

function toCredentialSlot(server: ServerCredentialSlot): CredentialSlot {
  return {
    handle: server.handle,
    provider: server.provider,
    redacted: server.redacted,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
  };
}

function toCatalogEntry(server: ServerCatalogEntry): CatalogEntry {
  return {
    id: server.id,
    displayName: server.display_name,
    description: server.description,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
  };
}

function toCustomConnector(server: ServerCustomConnector): CustomConnector {
  return {
    id: server.id,
    displayName: server.display_name,
    description: server.description,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
  };
}

function toCaptureTicket(server: ServerCaptureTicket): CaptureTicket {
  return {
    ticketId: server.ticket_id,
    provider: server.provider,
    handle: server.handle,
    status: server.status,
    createdMs: server.created_ms,
    expiresMs: server.expires_ms,
  };
}

// Vault line over Tauri invoke, backed by the Rust
// CredentialVaultClient. Invoke argument keys are snake_case to
// match the Rust command parameters exactly: the Task 9
// link/unlink failure was these keys arriving in the wrong case.
export class InvokeCredentialVaultClient implements VaultGateway {
  async listCredentials(): Promise<CredentialSlot[]> {
    const slots = await invoke<ServerCredentialSlot[]>("vault_credentials_list");
    return slots.map(toCredentialSlot);
  }

  async registerCredential(handle: string, secret: string, provider?: string | null): Promise<CredentialSlot> {
    const slot = await invoke<ServerCredentialSlot>("vault_credential_register", {
      handle,
      secret,
      provider: provider ?? null,
    });
    return toCredentialSlot(slot);
  }

  async deleteCredential(handle: string): Promise<CredentialSlot> {
    const slot = await invoke<ServerCredentialSlot>("vault_credential_delete", { handle });
    return toCredentialSlot(slot);
  }

  async captureCredential(handle: string, secret: string): Promise<CredentialSlot> {
    const slot = await invoke<ServerCredentialSlot>("vault_credential_capture", { handle, secret });
    return toCredentialSlot(slot);
  }

  async requestCapture(provider: string, handle: string): Promise<CaptureTicket> {
    const ticket = await invoke<ServerCaptureTicket>("vault_capture_request", { provider, handle });
    return toCaptureTicket(ticket);
  }

  async fulfillCapture(ticketId: string, secret: string): Promise<CredentialSlot> {
    const slot = await invoke<ServerCredentialSlot>("vault_capture_fulfill", {
      ticket_id: ticketId,
      secret,
    });
    return toCredentialSlot(slot);
  }

  async catalogList(): Promise<CatalogEntry[]> {
    const entries = await invoke<ServerCatalogEntry[]>("vault_catalog_list");
    return entries.map(toCatalogEntry);
  }

  async catalogDetails(id: string): Promise<CatalogEntry> {
    const entry = await invoke<ServerCatalogEntry>("vault_catalog_details", { id });
    return toCatalogEntry(entry);
  }

  async catalogUpdate(
    id: string,
    displayName?: string | null,
    description?: string | null,
  ): Promise<CatalogEntry> {
    const entry = await invoke<ServerCatalogEntry>("vault_catalog_update", {
      id,
      display_name: displayName ?? null,
      description: description ?? null,
    });
    return toCatalogEntry(entry);
  }

  async catalogDelete(id: string): Promise<CatalogEntry> {
    const entry = await invoke<ServerCatalogEntry>("vault_catalog_delete", { id });
    return toCatalogEntry(entry);
  }

  async customRegister(
    id: string,
    displayName: string,
    description?: string | null,
  ): Promise<CustomConnector> {
    const def = await invoke<ServerCustomConnector>("vault_custom_register", {
      id,
      display_name: displayName,
      description: description ?? null,
    });
    return toCustomConnector(def);
  }

  async customList(): Promise<CustomConnector[]> {
    const defs = await invoke<ServerCustomConnector[]>("vault_custom_list");
    return defs.map(toCustomConnector);
  }

  async customDelete(id: string): Promise<CustomConnector> {
    const def = await invoke<ServerCustomConnector>("vault_custom_delete", { id });
    return toCustomConnector(def);
  }
}
