// Invoke contacts client: packaged app -> Tauri shell
// contacts_* commands (MCL-2 read, MCL-5 write). Param
// names are snake_case to match the shell's rename_all;
// the invoke transport returns the Rust structs'
// snake_case fields, so the mapping lives here, not in
// the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  Contact,
  ContactsGateway,
  ContactsSyncResult,
  CreateContactInput,
  DeleteContactInput,
  UpdateContactInput,
} from "./contacts_client";

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

export class InvokeContactsClient implements ContactsGateway {
  async syncContacts(): Promise<ContactsSyncResult> {
    const result = await invoke<ServerContactsSyncResult>("contacts_sync");
    return {
      syncedCount: result.synced_count,
      skippedCount: result.skipped_count,
      totalCount: result.total_count,
      cursor: result.cursor,
    };
  }

  async contacts(limit: number | null, offset: number | null): Promise<Contact[]> {
    const contacts = await invoke<ServerContact[]>("contacts_list", {
      limit,
      offset,
    });
    return contacts.map(toContact);
  }

  async contact(contactId: string): Promise<Contact> {
    const contact = await invoke<ServerContact>("contacts_get", {
      contact_id: contactId,
    });
    return toContact(contact);
  }

  async searchContacts(query: string, limit: number | null, offset: number | null): Promise<Contact[]> {
    const contacts = await invoke<ServerContact[]>("contacts_search", {
      query,
      limit,
      offset,
    });
    return contacts.map(toContact);
  }

  // MCL-5: create one contact, through the digest-bound
  // approval flow (the imessage `sendImessage` shape).
  async createContact(input: CreateContactInput): Promise<Contact> {
    const created = await invoke<ServerContact>("contacts_create", {
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
    const updated = await invoke<ServerContact>("contacts_update", {
      contact_id: contactId,
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
    const deleted = await invoke<ServerContact>("contacts_delete", {
      contact_id: contactId,
      idempotency_key: input.idempotencyKey,
      tool_call_id: input.toolCallId,
    });
    return toContact(deleted);
  }
}
