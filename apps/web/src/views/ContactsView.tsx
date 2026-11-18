// Contacts view: contacts read surface (MCL-2).
// Contacts list -> contact detail (row fields only)
// -> search (display name / phones / emails). The
// sync button is a manual trigger: it records the
// Contacts.app card list into local SQLite
// (display_name / organization / job_title and phones /
// emails in plaintext, addresses / birthday as hashes
// only — Q-MCL-11); there is no auto-trigger.
//
// Honest copy (shown in the view): on the packaged Mac
// app this reads real Contacts.app data via AppleScript;
// other platforms answer 501. Contact ids are local
// (`contact-N`), not the original product's contact
// ids; address / birthday values are never stored —
// only their hashes (NULL = none).

import { useEffect, useState } from "react";
import type {
  Contact,
  ContactsGateway,
  ContactsSyncResult,
} from "../domain/contacts_client";

interface ContactsViewProps {
  contacts: ContactsGateway;
}

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

function fmtTime(ms: number): string {
  return new Date(ms).toLocaleString();
}

export default function ContactsView({ contacts }: ContactsViewProps) {
  const [rows, setRows] = useState<Contact[]>([]);
  const [selected, setSelected] = useState<Contact | null>(null);
  const [query, setQuery] = useState("");
  const [syncResult, setSyncResult] = useState<ContactsSyncResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function loadContacts() {
    try {
      setRows(await contacts.contacts(50, 0));
    } catch (e) {
      setError(errText(e));
    }
  }

  useEffect(() => {
    void loadContacts();
    // The gateway is stable for the view's lifetime.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function syncContacts() {
    setBusy(true);
    setError(null);
    try {
      const result = await contacts.syncContacts();
      setSyncResult(result);
      await loadContacts();
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function searchContacts() {
    setBusy(true);
    setError(null);
    try {
      setRows(await contacts.searchContacts(query, 50, 0));
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="view">
      <h2>通讯录</h2>
      <p className="note">
        只读联系人列表：姓名、单位、电话与邮箱明文显示；地址与生日只存哈希，不显示取值。打包的
        Mac 应用读真实 Contacts.app 数据，其他平台返回 501。
      </p>
      <div className="row">
        <button type="button" onClick={() => void syncContacts()} disabled={busy}>
          同步联系人
        </button>
        <button type="button" onClick={() => void loadContacts()} disabled={busy}>
          刷新
        </button>
      </div>
      <div className="row">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="按姓名、电话或邮箱搜索"
          disabled={busy}
        />
        <button type="button" onClick={() => void searchContacts()} disabled={busy}>
          搜索
        </button>
      </div>
      {syncResult !== null && (
        <p className="note">
          已同步 {syncResult.syncedCount} 条，跳过 {syncResult.skippedCount} 条，共{" "}
          {syncResult.totalCount} 条。
        </p>
      )}
      {error !== null && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {rows.length === 0 && <p className="empty">暂无已同步的联系人。</p>}
      <ul className="entries">
        {rows.map((row) => (
          <li key={row.contactId}>
            <button type="button" onClick={() => setSelected(row)}>
              {row.displayName || "(无姓名)"} · {row.phones.length} 个电话 · {row.emails.length}{" "}
              个邮箱
              {row.deleted ? " · 已删除" : ""}
            </button>
          </li>
        ))}
      </ul>
      {selected !== null && (
        <div>
          <h3>联系人 {selected.displayName || selected.contactId}</h3>
          <p className="note">
            联系人详情只有行字段；地址与生日只存哈希，不显示取值。
          </p>
          <pre className="result">
            {[
              `contact_id: ${selected.contactId}`,
              `native_id: ${selected.nativeId}`,
              `display_name: ${selected.displayName}`,
              `organization: ${selected.organization}`,
              `job_title: ${selected.jobTitle}`,
              `phones: ${selected.phones.join(", ")}`,
              `emails: ${selected.emails.join(", ")}`,
              `has_address: ${selected.addressesHash !== null ? "是" : "否"}`,
              `has_birthday: ${selected.birthdayHash !== null ? "是" : "否"}`,
              `created: ${fmtTime(selected.createdMs)}`,
              `updated: ${fmtTime(selected.updatedMs)}`,
              `deleted: ${selected.deleted ? "是" : "否"}`,
            ].join("\n")}
          </pre>
        </div>
      )}
    </div>
  );
}
