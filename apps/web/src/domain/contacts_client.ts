// Contacts client: browser/dev -> Mock Gateway contacts
// surface (MCL-2 read, MCL-5 write).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Reads need
// no approval; the three writes go through the
// digest-bound approval flow (approval_request ->
// decide -> write, the 6b-1 mail shape). The packaged
// Mac app reads / writes real Contacts.app data via
// AppleScript; other platforms answer 501.
//
// PII shape (Q-MCL-11): displayName / organization /
// jobTitle and phones / emails are PLAINTEXT here
// (display, dial, and send need them); addresses /
// birthday are hash / existence-flag only —
// `addressesHash` / `birthdayHash` are sha256 hexes
// (null when the card has none), and no address /
// birthday VALUE exists anywhere in this file.

// One synced contact (the gateway `ContactRow` shape,
// camel-cased). The list includes tombstoned rows with
// `deleted: true`; `contact` may return a tombstoned
// row with `deleted: true`. `idempotencyKey` /
// `effectId` / `policyVersion` are null on synced rows
// (a sync writes no Effect) and set on rows written by
// MCL-5; `contentHash` is the sha256 of the normalized
// writable content and is present on every row. The
// write responses also carry a `replayed` flag (the
// imessage `SentIMessage` precedent); the view shows it.
export interface Contact {
  contactId: string;
  sourceId: string;
  nativeId: string;
  displayName: string;
  organization: string;
  jobTitle: string;
  phones: string[];
  emails: string[];
  addressesHash: string | null;
  birthdayHash: string | null;
  idempotencyKey: string | null;
  effectId: string | null;
  policyVersion: string | null;
  contentHash: string;
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
  originDeviceId: string;
  replayed?: boolean;
}

// MCL-2: one contacts sync pass (manual trigger only).
// Counts + the contacts-source cursor.
export interface ContactsSyncResult {
  syncedCount: number;
  skippedCount: number;
  totalCount: number;
  cursor: string;
}

// MCL-5: a create request. The idempotency key is
// client-supplied and required (Q-MCL-23, the 6b-1
// shape); the approval id is the decided digest-bound
// approval for this exact content.
export interface CreateContactInput {
  displayName: string;
  organization: string;
  jobTitle: string;
  phones: string[];
  emails: string[];
  idempotencyKey: string;
  toolCallId: string | null;
}

// MCL-5: an update request — full replace of the
// writable fields. `baseContentHash` is the hash of
// the card the caller last read; if the live card
// moved since, the gateway answers 409
// `contact_changed` with the fresh hash and writes
// nothing (the 6b-4 U6 shape).
export interface UpdateContactInput {
  displayName: string;
  organization: string;
  jobTitle: string;
  phones: string[];
  emails: string[];
  baseContentHash: string;
  idempotencyKey: string;
  toolCallId: string | null;
}

// MCL-5: a delete request. The gateway tombstones the
// row (`deleted: true`, still listed) — it never
// hard-deletes.
export interface DeleteContactInput {
  idempotencyKey: string;
  toolCallId: string | null;
}

// Transport-neutral surface used by the Contacts view.
// The HTTP client below and the invoke client both
// implement it, so the view never knows which transport
// it runs on.
export interface ContactsGateway {
  syncContacts(): Promise<ContactsSyncResult>;
  contacts(limit: number | null, offset: number | null): Promise<Contact[]>;
  contact(contactId: string): Promise<Contact>;
  searchContacts(query: string, limit: number | null, offset: number | null): Promise<Contact[]>;
  createContact(input: CreateContactInput): Promise<Contact>;
  updateContact(contactId: string, input: UpdateContactInput): Promise<Contact>;
  deleteContact(contactId: string, input: DeleteContactInput): Promise<Contact>;
}

interface ServerContactsSyncResult {
  synced_count: number;
  skipped_count: number;
  total_count: number;
  cursor: string;
}

interface ServerContact {
  contact_id: string;
  source_id: string;
  native_id: string;
  display_name: string;
  organization: string;
  job_title: string;
  phones: string[];
  emails: string[];
  addresses_hash: string | null;
  birthday_hash: string | null;
  idempotency_key: string | null;
  effect_id: string | null;
  policy_version: string | null;
  content_hash: string;
  created_ms: number;
  updated_ms: number;
  deleted: boolean;
  origin_device_id: string;
  replayed?: boolean;
}

function toContact(server: ServerContact): Contact {
  return {
    contactId: server.contact_id,
    sourceId: server.source_id,
    nativeId: server.native_id,
    displayName: server.display_name,
    organization: server.organization,
    jobTitle: server.job_title,
    phones: server.phones,
    emails: server.emails,
    addressesHash: server.addresses_hash,
    birthdayHash: server.birthday_hash,
    idempotencyKey: server.idempotency_key,
    effectId: server.effect_id,
    policyVersion: server.policy_version,
    contentHash: server.content_hash,
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted,
    originDeviceId: server.origin_device_id,
    replayed: server.replayed,
  };
}

const ALLOWED_HOSTS = ["127.0.0.1", "localhost"];

function checkUrl(baseUrl: string): void {
  const stripped = baseUrl.replace(/^http:\/\//, "").replace(/\/$/, "");
  const host = stripped.split(":")[0];
  if (!baseUrl.startsWith("http://") || !ALLOWED_HOSTS.includes(host)) {
    throw new Error("contacts gateway URL must be a localhost Mock");
  }
}

export class ContactsClient implements ContactsGateway {
  private readonly baseUrl: string;

  constructor(baseUrl: string) {
    checkUrl(baseUrl);
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  private async requestJson<T>(method: string, path: string): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, { method });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`contacts gateway ${method} ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  private async postJson<T>(path: string, body: unknown): Promise<T> {
    const response = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!response.ok) {
      const text = await response.text();
      throw new Error(`contacts gateway POST ${path} failed: ${response.status} ${text}`);
    }
    return (await response.json()) as T;
  }

  async syncContacts(): Promise<ContactsSyncResult> {
    const result = await this.requestJson<ServerContactsSyncResult>(
      "POST",
      "/api/contacts/sync",
    );
    return {
      syncedCount: result.synced_count,
      skippedCount: result.skipped_count,
      totalCount: result.total_count,
      cursor: result.cursor,
    };
  }

  async contacts(limit: number | null, offset: number | null): Promise<Contact[]> {
    const query = new URLSearchParams();
    if (limit !== null) query.set("limit", String(limit));
    if (offset !== null) query.set("offset", String(offset));
    const suffix = query.toString() === "" ? "" : `?${query.toString()}`;
    const contacts = await this.requestJson<ServerContact[]>("GET", `/api/contacts${suffix}`);
    return contacts.map(toContact);
  }

  async contact(contactId: string): Promise<Contact> {
    const contact = await this.requestJson<ServerContact>(
      "GET",
      `/api/contacts/${contactId}`,
    );
    return toContact(contact);
  }

  async searchContacts(query: string, limit: number | null, offset: number | null): Promise<Contact[]> {
    const params = new URLSearchParams();
    params.set("q", query);
    if (limit !== null) params.set("limit", String(limit));
    if (offset !== null) params.set("offset", String(offset));
    const contacts = await this.requestJson<ServerContact[]>(
      "GET",
      `/api/contacts/search?${params.toString()}`,
    );
    return contacts.map(toContact);
  }

  // MCL-5: create one contact, through the digest-bound
  // approval flow (approval_request -> decide -> write,
  // the 6b-1 mail shape). The gateway's extra `replayed`
  // flag rides on the row; the view shows it.
  async createContact(input: CreateContactInput): Promise<Contact> {
    const created = await this.postJson<ServerContact>("/api/contacts/create", {
      display_name: input.displayName,
      organization: input.organization,
      job_title: input.jobTitle,
      phones: input.phones,
      emails: input.emails,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId,
    });
    return toContact(created);
  }

  // MCL-5: update one contact (full replace of the
  // writable fields). A 409 `contact_changed` means the
  // live card moved since `baseContentHash` — the view
  // re-reads and offers a retry; the approval is not
  // burned.
  async updateContact(contactId: string, input: UpdateContactInput): Promise<Contact> {
    const updated = await this.postJson<ServerContact>(`/api/contacts/${contactId}/update`, {
      display_name: input.displayName,
      organization: input.organization,
      job_title: input.jobTitle,
      phones: input.phones,
      emails: input.emails,
      base_content_hash: input.baseContentHash,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId,
    });
    return toContact(updated);
  }

  // MCL-5: delete one contact. The gateway tombstones
  // the row (deleted: true, still listed) — it never
  // hard-deletes, and a re-delete replays the tombstone.
  async deleteContact(contactId: string, input: DeleteContactInput): Promise<Contact> {
    const deleted = await this.postJson<ServerContact>(`/api/contacts/${contactId}/delete`, {
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId,
    });
    return toContact(deleted);
  }
}
