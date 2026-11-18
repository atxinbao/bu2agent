// Contacts client: browser/dev -> Mock Gateway contacts
// surface (MCL-2, read-only).
//
// Same shape as the other domain clients: the gateway
// returns snake_case; the domain shape is camelCase, so
// the mapping lives here, not in the view. Reads only;
// no approval loop. The packaged Mac app reads real
// Contacts.app data via AppleScript; other platforms
// answer 501.
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
// row with `deleted: true`.
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
  createdMs: number;
  updatedMs: number;
  deleted: boolean;
  originDeviceId: string;
}

// MCL-2: one contacts sync pass (manual trigger only).
// Counts + the contacts-source cursor.
export interface ContactsSyncResult {
  syncedCount: number;
  skippedCount: number;
  totalCount: number;
  cursor: string;
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
  created_ms: number;
  updated_ms: number;
  deleted: boolean;
  origin_device_id: string;
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
    createdMs: server.created_ms,
    updatedMs: server.updated_ms,
    deleted: server.deleted,
    originDeviceId: server.origin_device_id,
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
}
