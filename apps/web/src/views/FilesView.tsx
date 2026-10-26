import { useEffect, useState } from "react";
import type { FileGateway, GatewayFileEntry } from "../domain/file_client";

// Files view over the FileGateway interface: the view never
// knows whether it runs on invoke or on HTTP. Writes and
// deletes first pass the client-side approval gate (the
// Phase 2 with_writes_allowed rule); since round 2 they also
// execute under a server-side bound approval, same as the
// agent line: the client requests an approval for the exact
// operation, decides it from the window confirmation, and the
// server consumes it once on execute. Deletes additionally
// ask once more, because they are destructive.
// defaultDir is the settings page's default working directory
// (root-relative, "" = file root). It only sets the initial
// listing directory; write paths stay root-relative and an
// explicitly typed path always wins. When defaultDirValid is
// false the stored default is ignored entirely -- no fallback
// location is substituted for it -- and the banner below asks
// for a re-selection. Listing starts at the root either way;
// listing is a read, and writes here always need an explicit
// path, so no write can land in a fallback location.
export function FilesView({
  client,
  defaultDir = "",
  defaultDirValid = true,
}: {
  client: FileGateway;
  defaultDir?: string;
  defaultDirValid?: boolean;
}) {
  const [dir, setDir] = useState(defaultDirValid ? defaultDir : "");
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
    void refresh(dir);
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
      {!defaultDirValid && (
        <p className="error">
          默认工作目录已失效，请到设置页重新选择；失效的默认目录不会用于任何写入。
        </p>
      )}
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
