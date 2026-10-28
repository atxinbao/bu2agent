import { useEffect, useState } from "react";
import type { FileGateway, GatewayFileEntry, GatewayLibraryItem } from "../domain/file_client";

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
  const [selectedStat, setSelectedStat] = useState<Awaited<ReturnType<FileGateway["stat"]>> | null>(null);
  const [writePath, setWritePath] = useState("");
  const [writeText, setWriteText] = useState("");
  const [writesAllowed, setWritesAllowed] = useState(false);
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);
  const [renameFrom, setRenameFrom] = useState<string | null>(null);
  const [renameTo, setRenameTo] = useState("");
  const [pendingRename, setPendingRename] = useState<{ from: string; to: string } | null>(null);
  const [mkdirPath, setMkdirPath] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<GatewayFileEntry[]>([]);
  const [libraryItems, setLibraryItems] = useState<GatewayLibraryItem[]>([]);
  const [showLibrary, setShowLibrary] = useState(false);
  const [exportTarget, setExportTarget] = useState("");
  const [thumbnail, setThumbnail] = useState<string | null>(null);

  async function refresh(path: string) {
    setBusy(true);
    setError(null);
    try {
      setEntries(await client.list(path));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh(dir);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    let stopped = false;
    let cursor: string | undefined;
    async function watch() {
      try {
        const result = await client.subscribe(dir, cursor, 25000);
        if (stopped) return;
        cursor = result.cursor;
        if (result.changed) await refresh(dir);
      } catch {
        // 订阅是辅助刷新；目录列表本身仍可手动刷新。
      }
      if (!stopped) void watch();
    }
    void watch();
    return () => { stopped = true; };
  }, [client, dir]);

  async function openEntry(entry: GatewayFileEntry) {
    setError(null);
    setNotice(null);
    try {
      setSelectedStat(await client.stat(entry.path));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      return;
    }
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
      const ext = entry.path.toLowerCase().split(".").pop() ?? "";
      if (["png", "jpg", "jpeg", "gif", "webp"].includes(ext)) {
        try {
          const image = await client.thumbnail(entry.path, 256);
          setThumbnail(`data:${image.contentType};base64,${image.contentBase64}`);
        } catch { setThumbnail(null); }
      } else {
        setThumbnail(null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function search() {
    setBusy(true); setError(null);
    try { const result = await client.search(searchQuery, dir); setSearchResults(result.entries); setNotice(result.truncated ? "search results truncated" : `found ${result.entries.length} entries`); }
    catch (err) { setError(err instanceof Error ? err.message : String(err)); }
    finally { setBusy(false); }
  }

  async function loadLibrary() {
    setBusy(true); setError(null);
    try { const result = await client.library(); setLibraryItems(result.items); setShowLibrary(true); }
    catch (err) { setError(err instanceof Error ? err.message : String(err)); }
    finally { setBusy(false); }
  }

  async function exportSelected() {
    if (!selectedPath || !writesAllowed || exportTarget.trim() === "") { setError("export needs a selected file, an absolute target, and approval"); return; }
    setBusy(true); setError(null);
    try { const size = await client.export(selectedPath, exportTarget.trim(), true); setNotice(`exported ${size} bytes to ${exportTarget.trim()}`); }
    catch (err) { setError(err instanceof Error ? err.message : String(err)); }
    finally { setBusy(false); }
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
      setError(err instanceof Error ? err.message : String(err));
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
        setSelectedStat(null);
      }
      setPendingDelete(null);
      await refresh(dir);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  function requestRename(from: string) {
    setRenameFrom(from);
    setRenameTo(from);
    setError(null);
    setNotice(null);
  }

  function confirmRenameRequest() {
    if (renameFrom === null || renameTo.trim() === "") {
      setError("rename needs a target path");
      return;
    }
    if (!writesAllowed) {
      setError("turn on the write switch first: renames need explicit approval");
      return;
    }
    setPendingRename({ from: renameFrom, to: renameTo.trim() });
    setRenameFrom(null);
  }

  async function confirmRename() {
    if (pendingRename === null) return;
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await client.rename(pendingRename.from, pendingRename.to, true);
      setNotice(`renamed ${pendingRename.from} to ${pendingRename.to}`);
      setPendingRename(null);
      await refresh(dir);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }

  async function mkdir() {
    if (mkdirPath.trim() === "") {
      setError("mkdir needs a path");
      return;
    }
    if (!writesAllowed) {
      setError("turn on the write switch first: directories need explicit approval");
      return;
    }
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await client.mkdir(mkdirPath.trim(), true);
      setNotice(`created directory ${mkdirPath.trim()}`);
      setMkdirPath("");
      await refresh(dir);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
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
      <div className="row">
        <input value={searchQuery} placeholder="Search file name or path" onChange={(event) => setSearchQuery(event.target.value)} />
        <button type="button" onClick={() => void search()} disabled={busy || searchQuery.trim() === ""}>Search</button>
        <button type="button" onClick={() => void loadLibrary()} disabled={busy}>Library</button>
      </div>
      {searchResults.length > 0 && <div className="note"><strong>Search results</strong><ul>{searchResults.map((entry) => <li key={`search-${entry.path}`}>{entry.path}</li>)}</ul></div>}
      {showLibrary && <div className="note"><strong>Library (single-root local interpretation)</strong><ul>{libraryItems.map((item) => <li key={`library-${item.path}`}>{item.kind}: {item.path} ({item.sizeBytes} bytes)</li>)}</ul></div>}
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
            {renameFrom === entry.path ? (
              <span className="row">
                <input value={renameTo} onChange={(event) => setRenameTo(event.target.value)} />
                <button type="button" onClick={confirmRenameRequest} disabled={busy}>Rename</button>
                <button type="button" onClick={() => setRenameFrom(null)} disabled={busy}>Cancel</button>
              </span>
            ) : (
              <button type="button" onClick={() => requestRename(entry.path)} disabled={busy}>Rename</button>
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
      {pendingRename !== null && (
        <div className="confirm">
          <span>Rename {pendingRename.from} to {pendingRename.to}?</span>
          <button type="button" onClick={() => void confirmRename()} disabled={busy}>Confirm rename</button>
          <button type="button" onClick={() => setPendingRename(null)} disabled={busy}>Cancel</button>
        </div>
      )}
      {selectedPath !== null && (
        <div>
          <h3>{selectedPath}</h3>
          <pre className="result">{selectedText}</pre>
          {thumbnail !== null && <img src={thumbnail} alt={`Thumbnail for ${selectedPath}`} style={{ maxWidth: 256, maxHeight: 256 }} />}
          <div className="row">
            <input value={exportTarget} placeholder="Absolute export path" onChange={(event) => setExportTarget(event.target.value)} />
            <button type="button" onClick={() => void exportSelected()} disabled={busy || !writesAllowed}>Export</button>
          </div>
        </div>
      )}
      {selectedStat !== null && (
        <div className="note">
          <div>Path: {selectedStat.path}</div>
          <div>Type: {selectedStat.isDir ? "directory" : "file"}</div>
          <div>Size: {selectedStat.sizeBytes} bytes</div>
        </div>
      )}
      <h3>Create directory</h3>
      <div className="row">
        <input type="text" value={mkdirPath} placeholder="Directory path" onChange={(event) => setMkdirPath(event.target.value)} />
        <button type="button" onClick={() => void mkdir()} disabled={busy}>Create directory</button>
      </div>
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
