import { useEffect, useState } from "react";
import type { Source, SourceGateway } from "../domain/sources_client";

// Sources view over the SourceGateway interface (D1-4,
// minimal list/detail/disconnect; tails adds reconnect).
// A source is one concrete instance — a connector account, a device
// backend, a mail source, a file root — never a
// capability group. Disconnect flips only the row's
// status flag: the source's history Effect/Inbox rows
// are untouched, nothing is deleted, and reconnect is
// the reverse flip (no delete-registration route exists
// in this batch).
export function SourcesView({ client }: { client: SourceGateway }) {
  const [sources, setSources] = useState<Source[]>([]);
  const [selected, setSelected] = useState<Source | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      setSources(await client.listSources());
    } catch (err) {
      setError(err instanceof Error ? err.message : "list failed");
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    void refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function run(action: () => Promise<unknown>, done: string) {
    setBusy(true);
    setError(null);
    setNotice(null);
    try {
      await action();
      setNotice(done);
      await refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "action failed");
    } finally {
      setBusy(false);
    }
  }

  async function select(sourceId: string) {
    setBusy(true);
    setError(null);
    try {
      setSelected(await client.getSource(sourceId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "get failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h2>Sources</h2>
      <p className="note">
        来源是一等注册：一行是一个具体实例（连接器账号、设备后端、邮件来源、文件根目录），不是能力组。断开只翻状态旗标：历史
        Effect/Inbox 不动，什么都不删；重连是反向翻转。删除注册与清除缓存是另批的操作，本批没有路由。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p className="note">{notice}</p>}
      <div>
        <button type="button" disabled={busy} onClick={() => void refresh()}>
          Refresh
        </button>
      </div>
      {sources.length === 0 ? (
        <p className="empty">No sources registered yet.</p>
      ) : (
        <ul>
          {sources.map((source) => (
            <li key={source.sourceId}>
              <button type="button" disabled={busy} onClick={() => void select(source.sourceId)}>
                {source.displayName} ({source.sourceId}, {source.kind}, {source.status})
              </button>
            </li>
          ))}
        </ul>
      )}
      {selected !== null && (
        <div>
          <h3>{selected.displayName}</h3>
          <p>
            {selected.sourceId} · {selected.kind} · {selected.status}
          </p>
          <p>
            scope {selected.scope} · health {selected.health}
          </p>
          {selected.cursor !== "" && <p>cursor {selected.cursor}</p>}
          <p className="note">
            created {new Date(selected.createdMs).toLocaleString()} · origin{" "}
            {selected.originDeviceId}
          </p>
          {selected.status === "active" && (
            <button
              type="button"
              disabled={busy}
              onClick={() =>
                void run(
                  async () => {
                    setSelected(await client.disconnectSource(selected.sourceId));
                  },
                  "Source disconnected.",
                )
              }
            >
              Disconnect
            </button>
          )}
          {selected.status === "disconnected" && (
            <button
              type="button"
              disabled={busy}
              onClick={() =>
                void run(
                  async () => {
                    setSelected(await client.reconnectSource(selected.sourceId));
                  },
                  "Source reconnected.",
                )
              }
            >
              Reconnect
            </button>
          )}
        </div>
      )}
    </div>
  );
}
