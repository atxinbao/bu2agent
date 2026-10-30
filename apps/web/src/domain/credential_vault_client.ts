// Vault client for the TS Domain Store (Task 10): credentials,
// catalog, custom connectors, and capture tickets.
//
// Same shape as the connector clients: the gateway returns
// snake_case; the domain shape is camelCase, so the mapping
// lives here, not in the view. Secrets are never returned by
// the gateway: a slot carries only the redacted last-4 display.
// These calls carry no approval loop (Task 9 registry decision),
// and no payload check runs on secrets: the secret is the
// payload by design, destined for the localhost Mock only.

export interface CredentialSlot {
  handle: string;
  provider: string | null;
  redacted: string | null;
  createdMs: number;
  updatedMs: number;
}

export interface CatalogEntry {
  id: string;
  displayName: string;
  description: string | null;
  createdMs: number;
  updatedMs: number;
}

export interface CustomConnector {
  id: string;
  displayName: string;
  description: string | null;
  createdMs: number;
  updatedMs: number;
}

export interface CaptureTicket {
  ticketId: string;
  provider: string;
  handle: string;
  status: string;
  createdMs: number;
  expiresMs: number;
}

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

// Transport-neutral surface used by the Settings view. The HTTP
// client below and the invoke client both implement it, so the
// view never knows which transport it runs on.
export interface VaultGateway {
  listCredentials(): Promise<CredentialSlot[]>;
  registerCredential(handle: string, secret: string, provider?: string | null): Promise<CredentialSlot>;
  deleteCredential(handle: string): Promise<CredentialSlot>;
  captureCredential(handle: string, secret: string): Promise<CredentialSlot>;
  requestCapture(provider: string, handle: string): Promise<CaptureTicket>;
  fulfillCapture(ticketId: string, secret: string): Promise<CredentialSlot>;
  catalogList(): Promise<CatalogEntry[]>;
  catalogDetails(id: string): Promise<CatalogEntry>;
  catalogUpdate(id: string, displayName?: string | null, description?: string | null): Promise<CatalogEntry>;
  catalogDelete(id: string): Promise<CatalogEntry>;
  customRegister(id: string, displayName: string, description?: string | null): Promise<CustomConnector>;
  customList(): Promise<CustomConnector[]>;
  customDelete(id: string): Promise<CustomConnector>;
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("egress refused: destination is not on the Mock allowlist");
  }
}

export class CredentialVaultClient implements VaultGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async get<T>(path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`);
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  private async post<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(`mock gateway returned HTTP ${response.status}`);
    }
    return (await response.json()) as T;
  }

  async listCredentials(): Promise<CredentialSlot[]> {
    const slots = await this.get<ServerCredentialSlot[]>("/v1/credentials/list");
    return slots.map(toCredentialSlot);
  }

  async registerCredential(handle: string, secret: string, provider?: string | null): Promise<CredentialSlot> {
    const slot = await this.post<ServerCredentialSlot>("/v1/credentials/register", {
      handle,
      secret,
      provider: provider ?? null,
    });
    return toCredentialSlot(slot);
  }

  async deleteCredential(handle: string): Promise<CredentialSlot> {
    const slot = await this.post<ServerCredentialSlot>("/v1/credentials/delete", { handle });
    return toCredentialSlot(slot);
  }

  async captureCredential(handle: string, secret: string): Promise<CredentialSlot> {
    const slot = await this.post<ServerCredentialSlot>("/v1/credentials/capture", { handle, secret });
    return toCredentialSlot(slot);
  }

  async requestCapture(provider: string, handle: string): Promise<CaptureTicket> {
    const ticket = await this.post<ServerCaptureTicket>(
      "/v1/credentials/capture/external-provider/request",
      { provider, handle },
    );
    return toCaptureTicket(ticket);
  }

  async fulfillCapture(ticketId: string, secret: string): Promise<CredentialSlot> {
    const slot = await this.post<ServerCredentialSlot>(
      "/v1/credentials/capture/external-provider/fulfill",
      { ticket_id: ticketId, secret },
    );
    return toCredentialSlot(slot);
  }

  async catalogList(): Promise<CatalogEntry[]> {
    const entries = await this.get<ServerCatalogEntry[]>("/v1/credentials/catalog");
    return entries.map(toCatalogEntry);
  }

  async catalogDetails(id: string): Promise<CatalogEntry> {
    const entry = await this.get<ServerCatalogEntry>(
      `/v1/credentials/catalog/details?id=${encodeURIComponent(id)}`,
    );
    return toCatalogEntry(entry);
  }

  async catalogUpdate(
    id: string,
    displayName?: string | null,
    description?: string | null,
  ): Promise<CatalogEntry> {
    const entry = await this.post<ServerCatalogEntry>("/v1/credentials/catalog/update", {
      id,
      display_name: displayName ?? null,
      description: description ?? null,
    });
    return toCatalogEntry(entry);
  }

  async catalogDelete(id: string): Promise<CatalogEntry> {
    const entry = await this.post<ServerCatalogEntry>("/v1/credentials/catalog/delete", { id });
    return toCatalogEntry(entry);
  }

  async customRegister(
    id: string,
    displayName: string,
    description?: string | null,
  ): Promise<CustomConnector> {
    const def = await this.post<ServerCustomConnector>("/v1/custom-connectors/register", {
      id,
      display_name: displayName,
      description: description ?? null,
    });
    return toCustomConnector(def);
  }

  async customList(): Promise<CustomConnector[]> {
    const defs = await this.post<ServerCustomConnector[]>("/v1/custom-connectors/list", {});
    return defs.map(toCustomConnector);
  }

  async customDelete(id: string): Promise<CustomConnector> {
    const def = await this.post<ServerCustomConnector>("/v1/custom-connectors/delete", { id });
    return toCustomConnector(def);
  }
}
