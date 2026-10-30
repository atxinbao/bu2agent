import { useEffect, useState } from "react";
import type {
  ConnectorAccount,
  ConnectorConnectInfo,
  ConnectorGateway,
  ConnectorInfo,
  MockMessage,
  MockNote,
} from "../domain/connector_client";

// Connectors view over the ConnectorGateway interface. Lists
// need no approval; creates and sends show a confirm step
// first and only then run with "approve". Everything here is
// Mock data: seeded fake notes and mail plus items created
// in this server run. Mail recipients must be inside
// mock.local, and payloads carrying secret markers are
// refused by the egress rules before anything is sent.
export function ConnectorsView({ client }: { client: ConnectorGateway }) {
  const [notes, setNotes] = useState<MockNote[]>([]);
  const [messages, setMessages] = useState<MockMessage[]>([]);
  const [noteTitle, setNoteTitle] = useState("");
  const [noteBody, setNoteBody] = useState("");
  const [mailTo, setMailTo] = useState("");
  const [mailSubject, setMailSubject] = useState("");
  const [mailBody, setMailBody] = useState("");
  const [pending, setPending] = useState<"note" | "mail" | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [registry, setRegistry] = useState<ConnectorInfo[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [accounts, setAccounts] = useState<ConnectorAccount[]>([]);
  const [connectInfo, setConnectInfo] = useState<ConnectorConnectInfo | null>(null);
  const [linkName, setLinkName] = useState("");

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      const [listedNotes, listedMail, listedRegistry] = await Promise.all([
        client.notesList(),
        client.mailList(),
        client.registryList(),
      ]);
      setNotes(listedNotes);
      setMessages(listedMail);
      setRegistry(listedRegistry);
    } catch (err) {
      setError(err instanceof Error ? err.message : "list failed");
    } finally {
      setBusy(false);
    }
  }

  async function refreshRegistry() {
    setError(null);
    try {
      setRegistry(await client.registryList());
      if (selectedId !== null) {
        setAccounts(await client.accounts(selectedId));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "registry refresh failed");
    }
  }

  async function selectConnector(id: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const [listedAccounts, info] = await Promise.all([
        client.accounts(id),
        client.connectInfo(id),
      ]);
      setSelectedId(id);
      setAccounts(listedAccounts);
      setConnectInfo(info);
    } catch (err) {
      setError(err instanceof Error ? err.message : "connector load failed");
    } finally {
      setBusy(false);
    }
  }

  async function linkSelectedAccount() {
    if (selectedId === null) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const account = await client.linkAccount(selectedId, linkName.trim() || null);
      setNotice(`linked account ${account.accountId} (${account.displayName})`);
      setLinkName("");
      await refreshRegistry();
    } catch (err) {
      setError(err instanceof Error ? err.message : "link failed");
    } finally {
      setBusy(false);
    }
  }

  async function unlinkSelectedAccount(accountId: string) {
    if (selectedId === null) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await client.unlinkAccount(selectedId, accountId);
      setNotice(`unlinked account ${accountId}`);
      await refreshRegistry();
    } catch (err) {
      setError(err instanceof Error ? err.message : "unlink failed");
    } finally {
      setBusy(false);
    }
  }

  async function toggleSelectedConnection(connected: boolean) {
    if (selectedId === null) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const info = connected
        ? await client.disconnect(selectedId)
        : await client.connect(selectedId);
      setNotice(info.connected ? `connected ${info.id}` : `disconnected ${info.id}`);
      await refreshRegistry();
    } catch (err) {
      setError(err instanceof Error ? err.message : "connection change failed");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function confirmPending() {
    if (pending === null) {
      return;
    }
    const kind = pending;
    setPending(null);
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      if (kind === "note") {
        const note = await client.notesCreate(noteTitle, noteBody, "approve");
        setNotice(`created note ${note.id}`);
      } else {
        const message = await client.mailSend(mailTo, mailSubject, mailBody, "approve");
        setNotice(`sent mail ${message.id} to ${message.to}`);
      }
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "connector call failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="view">
      <h2>Connectors (Mock)</h2>
      <p className="note">
        Fake data only, never real accounts or delivery. Creates and sends need approval;
        mail recipients must be inside mock.local.
      </p>
      <button type="button" onClick={() => void refresh()} disabled={busy}>
        Refresh lists
      </button>
      <h3>Registry</h3>
      <p className="note">
        The registry lists only the Mock connectors this gateway implements. Accounts
        are local Mock records: no OAuth, no real account, no external redirect.
      </p>
      {registry.length === 0 && <p className="empty">No connectors.</p>}
      <ul className="entries">
        {registry.map((info) => (
          <li key={info.id}>
            <button type="button" onClick={() => void selectConnector(info.id)} disabled={busy}>
              <strong>{info.name}</strong> — {info.capabilities.join(", ")} ·{" "}
              {info.connected ? "connected" : "not connected"}
            </button>
          </li>
        ))}
      </ul>
      {selectedId !== null && (
        <>
          <h3>Connector: {selectedId}</h3>
          {connectInfo !== null && <p className="note">{connectInfo.description}</p>}
          <div className="row">
            <button
              type="button"
              onClick={() =>
                void toggleSelectedConnection(
                  registry.find((info) => info.id === selectedId)?.connected ?? false,
                )
              }
              disabled={busy}
            >
              {registry.find((info) => info.id === selectedId)?.connected
                ? "Disconnect"
                : "Connect"}
            </button>
          </div>
          <h4>Accounts</h4>
          {accounts.length === 0 && <p className="empty">No linked accounts.</p>}
          <ul className="entries">
            {accounts.map((account) => (
              <li key={account.accountId}>
                <strong>{account.displayName}</strong> — {account.accountId} · {account.status}
                <button
                  type="button"
                  onClick={() => void unlinkSelectedAccount(account.accountId)}
                  disabled={busy}
                >
                  Unlink
                </button>
              </li>
            ))}
          </ul>
          <div className="row">
            <input
              type="text"
              value={linkName}
              placeholder="Display name (optional)"
              onChange={(event) => setLinkName(event.target.value)}
            />
            <button type="button" onClick={() => void linkSelectedAccount()} disabled={busy}>
              Link account
            </button>
          </div>
        </>
      )}
      <h3>Notes</h3>
      {notes.length === 0 && <p className="empty">No notes.</p>}
      <ul className="entries">
        {notes.map((note) => (
          <li key={note.id}>
            <strong>{note.title}</strong> — {note.body}
          </li>
        ))}
      </ul>
      <div className="row">
        <input
          type="text"
          value={noteTitle}
          placeholder="Note title"
          onChange={(event) => setNoteTitle(event.target.value)}
        />
      </div>
      <textarea
        value={noteBody}
        placeholder="Note body"
        onChange={(event) => setNoteBody(event.target.value)}
      />
      <button type="button" onClick={() => setPending("note")} disabled={busy}>
        Create note
      </button>
      <h3>Mail</h3>
      {messages.length === 0 && <p className="empty">No messages.</p>}
      <ul className="entries">
        {messages.map((message) => (
          <li key={message.id}>
            <strong>{message.subject}</strong> — from {message.from} to {message.to}:{" "}
            {message.body}
          </li>
        ))}
      </ul>
      <div className="row">
        <input
          type="text"
          value={mailTo}
          placeholder="To, e.g. someone@mock.local"
          onChange={(event) => setMailTo(event.target.value)}
        />
        <input
          type="text"
          value={mailSubject}
          placeholder="Subject"
          onChange={(event) => setMailSubject(event.target.value)}
        />
      </div>
      <textarea
        value={mailBody}
        placeholder="Mail body"
        onChange={(event) => setMailBody(event.target.value)}
      />
      <button type="button" onClick={() => setPending("mail")} disabled={busy}>
        Send mail
      </button>
      {pending !== null && (
        <div className="confirm">
          <span>
            {pending === "note" ? "Create this note?" : "Send this mail?"} It needs
            approval before it runs.
          </span>
          <button type="button" onClick={() => void confirmPending()} disabled={busy}>
            Approve
          </button>
          <button
            type="button"
            onClick={() => {
              setPending(null);
              setNotice("rejected: nothing was executed");
            }}
            disabled={busy}
          >
            Reject
          </button>
        </div>
      )}
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p className="notice">{notice}</p>}
    </div>
  );
}
