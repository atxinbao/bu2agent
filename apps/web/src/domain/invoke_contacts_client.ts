// Invoke contacts client: packaged app -> Tauri shell
// contacts_* commands (MCL-2, read-only). Param
// names are snake_case to match the shell's rename_all;
// the invoke transport returns the Rust structs'
// snake_case fields, so the mapping lives here, not in
// the view.

import { invoke } from "@tauri-apps/api/core";
import type {
  Contact,
  ContactsGateway,
  ContactsSyncResult,
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
}
