import { useEffect, useState } from "react";
import type { FileGateway, GatewayFileEntry } from "../domain/file_client";

// Files view over the FileGateway interface: the view never
// knows whether it runs on invoke or on HTTP. Writes and
// deletes first pass the client-side approval gate (the
// Phase 2 with_writes_allowed rule); the Mock file endpoints
// themselves do not re-check approval records, unlike the
// agent and connector lines. Deletes additionally ask once
// more, because they are destructive.
export function FilesView({ client }: { client: FileGateway }) {
  const [dir, setDir] = useState("");
  const [entries, setEntries] = useState<GatewayFileEntry[]>([]);
  const [selectedPath, setSelectedPath] = useState<string | null>(null);
  const [selectedText, setSelectedText] = useState("");
  const [writePath, setWritePath] = useState("");
  const [writeText, setWriteText] = useState("");
  const [writesAllowed, setWritesAllowed] = useState(false);
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  async function refresh(path: string) {
    setBusy(true);
    setError(null);
    try {
      setEntries(await client.list(path));
    } catch (err) {
      setError(err instanceof Error ? err.message : "list failed");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh("");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function openEntry(entry: GatewayFileEntry) {
    setError(null);
    setNotice(null);
    if (entry.isDir) {
      setDir(entry.path);
      setSelectedPath(null);
      await refresh(entry.path);
      return;
    }
    setBusy(true);
    try {
      const text = await client.readText(entry.path, 4096, 1048576);
      setSelectedPath(entry.path);
      setSelectedText(text);
    } catch (err) {
      setError(err instanceof Error ? err.message : "read failed");
    } finally {
      setBusy(false);
    }
  }

  async function write() {
    if (writePath.trim() === "") {
      setError("write needs a path");
      return;
    }
    if (!writesAllowed) {
      setError("turn on the write switch first: writes need explicit approval");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      const size = await client.write(writePath, writeText, true);
      setNotice(`wrote ${size} bytes to ${writePath}`);
      await refresh(dir);
    } catch (err) {
      setError(err instanceof Error ? err.message : "write failed");
    } finally {
      setBusy(false);
    }
  }

  async function confirmDelete() {
    if (pendingDelete === null) {
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await client.delete(pendingDelete, true);
      setNotice(`deleted ${pendingDelete}`);
      if (selectedPath === pendingDelete) {
        setSelectedPath(null);
        setSelectedText("");
      }
      setPendingDelete(null);
      await refresh(dir);
    } catch (err) {
      setError(err instanceof Error ? err.message : "delete failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="view">
      <h2>Files</h2>
      <p className="note">
        Scoped to the Mock file root. Paths are relative; absolute paths and `..` are
        refused client-side. Writes and deletes need the switch below, and deletes ask
        once more before they run.
      </p>
      <div className="row">
        <input
          type="text"
          value={dir}
          placeholder="Directory (empty = root)"
          onChange={(event) => setDir(event.target.value)}
        />
        <button type="button" onClick={() => void refresh(dir)} disabled={busy}>
          List
        </button>
      </div>
      {entries.length === 0 && <p className="empty">No entries in this directory.</p>}
      <ul className="entries">
        {entries.map((entry) => (
          <li key={entry.path}>
            <button type="button" onClick={() => void openEntry(entry)} disabled={busy}>
              {entry.isDir ? "📁" : "📄"} {entry.path}
              {!entry.isDir && ` (${entry.sizeBytes} bytes)`}
            </button>
            {!entry.isDir && pendingDelete !== entry.path && (
              <button type="button" onClick={() => setPendingDelete(entry.path)} disabled={busy}>
                Delete
              </button>
            )}
          </li>
        ))}
      </ul>
      {pendingDelete !== null && (
        <div className="confirm">
          <span>Delete {pendingDelete}?</span>
          <button type="button" onClick={() => void confirmDelete()} disabled={busy}>
            Confirm delete
          </button>
          <button type="button" onClick={() => setPendingDelete(null)} disabled={busy}>
            Cancel
          </button>
        </div>
      )}
      {selectedPath !== null && (
        <div>
          <h3>{selectedPath}</h3>
          <pre className="result">{selectedText}</pre>
        </div>
      )}
      <h3>Write a file</h3>
      <div className="row">
        <input
          type="text"
          value={writePath}
          placeholder="Path, e.g. hello.txt"
          onChange={(event) => setWritePath(event.target.value)}
        />
        <label>
          <input
            type="checkbox"
            checked={writesAllowed}
            onChange={(event) => setWritesAllowed(event.target.checked)}
          />
          Allow writes
        </label>
      </div>
      <textarea
        value={writeText}
        placeholder="File content"
        onChange={(event) => setWriteText(event.target.value)}
      />
      <button type="button" onClick={() => void write()} disabled={busy}>
        Write
      </button>
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p className="notice">{notice}</p>}
    </div>
  );
}
