// Contacts view: contacts surface (MCL-2 read, MCL-5
// write). Contacts list -> contact detail (row fields
// only) -> search (display name / phones / emails).
// The sync button is a manual trigger: it records the
// Contacts.app card list into local SQLite
// (display_name / organization / job_title and phones /
// emails in plaintext, addresses / birthday as hashes
// only — Q-MCL-11); there is no auto-trigger.
//
// Writes go through the digest-bound approval flow
// (approval_request -> decide -> write, the 6b-1 mail
// shape): create a contact, update one (full replace
// of the writable fields), or delete one (tombstone:
// deleted: true, still listed — never a hard delete).
// An update carries the hash of the card the caller
// last read; if the live card moved since, the gateway
// answers 409 `contact_changed` with the fresh hash
// and writes nothing — the view re-reads and offers a
// retry, and the approval is not burned (the 6b-4 U6
// shape). "已创建 / 已更新" means Contacts.app
// accepted the write; it never promises more.
//
// Honest copy (shown in the view): on the packaged Mac
// app this reads / writes real Contacts.app data via
// AppleScript; other platforms answer 501. Contact ids
// are local (`contact-N`), not the original product's
// contact ids; address / birthday values are never
// stored — only their hashes (NULL = none).

import { useEffect, useState } from "react";
import type { AgentGateway } from "../domain/agent_client";
import type {
  Contact,
  ContactsGateway,
  ContactsSyncResult,
  CreateContactInput,
  DeleteContactInput,
  UpdateContactInput,
} from "../domain/contacts_client";

interface ContactsViewProps {
  contacts: ContactsGateway;
  agent: AgentGateway;
}

type PendingKind = "create" | "update" | "delete";

interface PendingWrite {
  kind: PendingKind;
  contactId: string | null;
  displayName: string;
  organization: string;
  jobTitle: string;
  phones: string[];
  emails: string[];
  baseContentHash: string | null;
  idempotencyKey: string;
  toolCallId: string;
}

function errText(e: unknown): string {
  return typeof e === "string" ? e : e instanceof Error ? e.message : String(e);
}

function newIdempotencyKey(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `key-${Date.now()}-${Math.floor(Math.random() * 1e9)}`;
}

function fmtTime(ms: number): string {
  return new Date(ms).toLocaleString();
}

/// Split a comma-separated form field into a list
/// (commas, Chinese commas, and newlines all separate;
/// blanks drop out). The gateway normalizes again
/// server-side (trim / sort / dedupe), so the exact
/// split here never changes the digest.
function splitList(text: string): string[] {
  return text
    .split(/[,，\n]+/)
    .map((part) => part.trim())
    .filter((part) => part !== "");
}

export default function ContactsView({ contacts, agent }: ContactsViewProps) {
  const [rows, setRows] = useState<Contact[]>([]);
  const [selected, setSelected] = useState<Contact | null>(null);
  const [query, setQuery] = useState("");
  const [syncResult, setSyncResult] = useState<ContactsSyncResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [writeResult, setWriteResult] = useState<Contact | null>(null);
  const [pendingWrite, setPendingWrite] = useState<PendingWrite | null>(null);

  // Create form.
  const [newDisplayName, setNewDisplayName] = useState("");
  const [newOrganization, setNewOrganization] = useState("");
  const [newJobTitle, setNewJobTitle] = useState("");
  const [newPhones, setNewPhones] = useState("");
  const [newEmails, setNewEmails] = useState("");

  // Update form, prefilled from the selected contact
  // when the user starts an edit.
  const [editDisplayName, setEditDisplayName] = useState("");
  const [editOrganization, setEditOrganization] = useState("");
  const [editJobTitle, setEditJobTitle] = useState("");
  const [editPhones, setEditPhones] = useState("");
  const [editEmails, setEditEmails] = useState("");
  const [editBaseHash, setEditBaseHash] = useState<string | null>(null);

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

  function startEdit(contact: Contact) {
    setEditDisplayName(contact.displayName);
    setEditOrganization(contact.organization);
    setEditJobTitle(contact.jobTitle);
    setEditPhones(contact.phones.join(", "));
    setEditEmails(contact.emails.join(", "));
    setEditBaseHash(contact.contentHash);
    setWriteResult(null);
    setError(null);
  }

  // The three write flows share one shape: request a
  // digest-bound approval for the exact frozen content,
  // then decide in this view, then write with the
  // decided approval id. The pending copy is frozen at
  // request time: editing the form afterwards cannot
  // change what the approval bound.
  async function requestCreateApproval() {
    if (newDisplayName.trim() === "") {
      setError("新建联系人需要姓名。");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const request = await agent.requestApproval("contacts.create", {
        display_name: newDisplayName.trim(),
        organization: newOrganization.trim(),
        job_title: newJobTitle.trim(),
        phones: JSON.stringify(splitList(newPhones)),
        emails: JSON.stringify(splitList(newEmails)),
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingWrite({
        kind: "create",
        contactId: null,
        displayName: newDisplayName.trim(),
        organization: newOrganization.trim(),
        jobTitle: newJobTitle.trim(),
        phones: splitList(newPhones),
        emails: splitList(newEmails),
        baseContentHash: null,
        idempotencyKey: newIdempotencyKey(),
        toolCallId: request.tool_call_id,
      });
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function requestUpdateApproval() {
    if (selected === null || editBaseHash === null) {
      setError("请先选一个联系人，并点「开始编辑」。");
      return;
    }
    if (editDisplayName.trim() === "") {
      setError("联系人姓名不能为空。");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const request = await agent.requestApproval("contacts.update", {
        contact_id: selected.contactId,
        display_name: editDisplayName.trim(),
        organization: editOrganization.trim(),
        job_title: editJobTitle.trim(),
        phones: JSON.stringify(splitList(editPhones)),
        emails: JSON.stringify(splitList(editEmails)),
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingWrite({
        kind: "update",
        contactId: selected.contactId,
        displayName: editDisplayName.trim(),
        organization: editOrganization.trim(),
        jobTitle: editJobTitle.trim(),
        phones: splitList(editPhones),
        emails: splitList(editEmails),
        baseContentHash: editBaseHash,
        idempotencyKey: newIdempotencyKey(),
        toolCallId: request.tool_call_id,
      });
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function requestDeleteApproval() {
    if (selected === null) {
      setError("请先选一个联系人。");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const request = await agent.requestApproval("contacts.delete", {
        contact_id: selected.contactId,
      });
      if (!request.tool_call_id) {
        setError("审批请求未返回 tool_call_id。");
        return;
      }
      setPendingWrite({
        kind: "delete",
        contactId: selected.contactId,
        displayName: selected.displayName,
        organization: selected.organization,
        jobTitle: selected.jobTitle,
        phones: selected.phones,
        emails: selected.emails,
        baseContentHash: null,
        idempotencyKey: newIdempotencyKey(),
        toolCallId: request.tool_call_id,
      });
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function approveAndWrite() {
    if (pendingWrite === null) return;
    setBusy(true);
    setError(null);
    try {
      await agent.decideApproval(pendingWrite.toolCallId, "approve");
      let row: Contact;
      if (pendingWrite.kind === "create") {
        const input: CreateContactInput = {
          displayName: pendingWrite.displayName,
          organization: pendingWrite.organization,
          jobTitle: pendingWrite.jobTitle,
          phones: pendingWrite.phones,
          emails: pendingWrite.emails,
          idempotencyKey: pendingWrite.idempotencyKey,
          toolCallId: pendingWrite.toolCallId,
        };
        row = await contacts.createContact(input);
        setNewDisplayName("");
        setNewOrganization("");
        setNewJobTitle("");
        setNewPhones("");
        setNewEmails("");
      } else if (pendingWrite.kind === "update") {
        if (pendingWrite.contactId === null || pendingWrite.baseContentHash === null) {
          setError("更新缺少联系人或基线哈希。");
          return;
        }
        const input: UpdateContactInput = {
          displayName: pendingWrite.displayName,
          organization: pendingWrite.organization,
          jobTitle: pendingWrite.jobTitle,
          phones: pendingWrite.phones,
          emails: pendingWrite.emails,
          baseContentHash: pendingWrite.baseContentHash,
          idempotencyKey: pendingWrite.idempotencyKey,
          toolCallId: pendingWrite.toolCallId,
        };
        row = await contacts.updateContact(pendingWrite.contactId, input);
      } else {
        if (pendingWrite.contactId === null) {
          setError("删除缺少联系人。");
          return;
        }
        const input: DeleteContactInput = {
          idempotencyKey: pendingWrite.idempotencyKey,
          toolCallId: pendingWrite.toolCallId,
        };
        row = await contacts.deleteContact(pendingWrite.contactId, input);
      }
      setWriteResult(row);
      setPendingWrite(null);
      setSelected(row);
      await loadContacts();
    } catch (e) {
      const text = errText(e);
      // 409 contact_changed: the live card moved since
      // the frozen base. Re-read it for the fresh base
      // hash and keep the pending approval — it was not
      // burned, so the same approval retries with the
      // fresh base.
      if (pendingWrite.kind === "update" && text.includes("contact_changed")) {
        // The fresh base lives in the 409 itself: the
        // gateway returns `current_hash` (the live card's
        // hash) and the transport surfaces it — either as
        // `current_hash=<64hex>` (invoke path) or as
        // `"current_hash":"<64hex>"` (browser path, whose
        // client carries the error body). Do NOT re-read
        // via GET: GET only reads the DB row, whose hash
        // is the stale base, so a GET-based retry would
        // 409 again (MCL-5 W2).
        const match = /"current_hash"\s*:\s*"([0-9a-f]{64})"|current_hash=([0-9a-f]{64})/.exec(text);
        const hash = match?.[1] ?? match?.[2] ?? null;
        if (hash !== null) {
          setPendingWrite({ ...pendingWrite, baseContentHash: hash });
          setEditBaseHash(hash);
          setError("联系人已被外部修改，已读取最新版本，可点「批准并写入」重试。审批未消耗。");
        } else {
          setError("联系人已被外部修改，已读取最新版本，可点「批准并写入」重试。审批未消耗。（未读到新基线，可再点一次「批准并写入」重试）");
        }
      } else {
        setError(text);
      }
    } finally {
      setBusy(false);
    }
  }

  async function rejectWrite() {
    if (pendingWrite === null) return;
    setBusy(true);
    setError(null);
    try {
      await agent.decideApproval(pendingWrite.toolCallId, "reject");
      setPendingWrite(null);
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  const pendingLabel =
    pendingWrite === null
      ? ""
      : pendingWrite.kind === "create"
        ? `新建联系人「${pendingWrite.displayName}」`
        : pendingWrite.kind === "update"
          ? `更新联系人 ${pendingWrite.contactId ?? ""}「${pendingWrite.displayName}」`
          : `删除联系人 ${pendingWrite.contactId ?? ""}「${pendingWrite.displayName}」`;

  return (
    <div className="view">
      <h2>通讯录</h2>
      <p className="note">
        联系人列表：姓名、单位、电话与邮箱明文显示；地址与生日只存哈希，不显示取值。新建、更新与删除都要先审批：审批绑定要写的全部内容，删除只写墓碑（deleted:
        true，仍在列表中），不真正删卡。打包的 Mac
        应用读写真实 Contacts.app 数据，其他平台返回 501。
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
      <div>
        <h3>新建联系人</h3>
        <p className="note">电话与邮箱用逗号分隔多个。新建前要审批，审批绑定下面填的全部内容。</p>
        <div className="row">
          <input
            type="text"
            value={newDisplayName}
            onChange={(e) => setNewDisplayName(e.target.value)}
            placeholder="姓名（必填）"
            disabled={busy || pendingWrite !== null}
          />
          <input
            type="text"
            value={newOrganization}
            onChange={(e) => setNewOrganization(e.target.value)}
            placeholder="单位"
            disabled={busy || pendingWrite !== null}
          />
          <input
            type="text"
            value={newJobTitle}
            onChange={(e) => setNewJobTitle(e.target.value)}
            placeholder="职位"
            disabled={busy || pendingWrite !== null}
          />
        </div>
        <div className="row">
          <input
            type="text"
            value={newPhones}
            onChange={(e) => setNewPhones(e.target.value)}
            placeholder="电话，逗号分隔"
            disabled={busy || pendingWrite !== null}
          />
          <input
            type="text"
            value={newEmails}
            onChange={(e) => setNewEmails(e.target.value)}
            placeholder="邮箱，逗号分隔"
            disabled={busy || pendingWrite !== null}
          />
        </div>
        <div className="row">
          <button
            type="button"
            onClick={() => void requestCreateApproval()}
            disabled={busy || pendingWrite !== null || newDisplayName.trim() === ""}
          >
            请求新建审批
          </button>
        </div>
      </div>
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
          {!selected.deleted && (
            <div>
              <h3>编辑联系人</h3>
              <p className="note">
                整卡替换：电话与邮箱会先清空再写新值。点「开始编辑」读入当前内容，更新前要审批；如果卡片在别处被改过，写入会报
                409，页面会重读最新版本再重试，审批不消耗。
              </p>
              <div className="row">
                <button type="button" onClick={() => startEdit(selected)} disabled={busy || pendingWrite !== null}>
                  开始编辑
                </button>
              </div>
              {editBaseHash !== null && (
                <div>
                  <div className="row">
                    <input
                      type="text"
                      value={editDisplayName}
                      onChange={(e) => setEditDisplayName(e.target.value)}
                      placeholder="姓名（必填）"
                      disabled={busy || pendingWrite !== null}
                    />
                    <input
                      type="text"
                      value={editOrganization}
                      onChange={(e) => setEditOrganization(e.target.value)}
                      placeholder="单位"
                      disabled={busy || pendingWrite !== null}
                    />
                    <input
                      type="text"
                      value={editJobTitle}
                      onChange={(e) => setEditJobTitle(e.target.value)}
                      placeholder="职位"
                      disabled={busy || pendingWrite !== null}
                    />
                  </div>
                  <div className="row">
                    <input
                      type="text"
                      value={editPhones}
                      onChange={(e) => setEditPhones(e.target.value)}
                      placeholder="电话，逗号分隔"
                      disabled={busy || pendingWrite !== null}
                    />
                    <input
                      type="text"
                      value={editEmails}
                      onChange={(e) => setEditEmails(e.target.value)}
                      placeholder="邮箱，逗号分隔"
                      disabled={busy || pendingWrite !== null}
                    />
                  </div>
                  <div className="row">
                    <button
                      type="button"
                      onClick={() => void requestUpdateApproval()}
                      disabled={busy || pendingWrite !== null || editDisplayName.trim() === ""}
                    >
                      请求更新审批
                    </button>
                  </div>
                </div>
              )}
              <h3>删除联系人</h3>
              <p className="note">
                删除只写墓碑：行仍在列表中、标「已删除」，同步不会复活它，卡片从 Contacts.app
                中删掉。删除前要审批。
              </p>
              <div className="row">
                <button type="button" onClick={() => void requestDeleteApproval()} disabled={busy || pendingWrite !== null}>
                  请求删除审批
                </button>
              </div>
            </div>
          )}
        </div>
      )}
      {pendingWrite !== null && (
        <div>
          <p className="note">待审批：{pendingLabel}：</p>
          <pre className="result">
            {[
              `display_name: ${pendingWrite.displayName}`,
              `organization: ${pendingWrite.organization}`,
              `job_title: ${pendingWrite.jobTitle}`,
              `phones: ${pendingWrite.phones.join(", ")}`,
              `emails: ${pendingWrite.emails.join(", ")}`,
            ].join("\n")}
          </pre>
          <div className="row">
            <button type="button" onClick={() => void approveAndWrite()} disabled={busy}>
              批准并写入
            </button>
            <button type="button" onClick={() => void rejectWrite()} disabled={busy}>
              拒绝
            </button>
          </div>
        </div>
      )}
      {writeResult !== null && (
        <p className="note">
          已写入：{writeResult.contactId}
          {writeResult.deleted ? "（已删除，墓碑）" : ""}
          {writeResult.replayed ? "，重放" : ""}。Contacts.app 已接受。
        </p>
      )}
    </div>
  );
}
