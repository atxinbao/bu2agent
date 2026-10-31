import { useEffect, useState } from "react";
import type { FeedGateway, FeedStatus, FeedUnit } from "../domain/feed_client";

// Feed view over the FeedGateway interface (Task 17,
// unit subset). Units are clearly-labeled Mock seeds;
// actions are seen, reaction, tap, and delete. Prompt,
// regenerate, reorder, search, social-source-icon, and
// media are not part of this surface.
export function FeedView({ client }: { client: FeedGateway }) {
  const [units, setUnits] = useState<FeedUnit[]>([]);
  const [status, setStatus] = useState<FeedStatus | null>(null);
  const [reaction, setReaction] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      const [listed, listedStatus] = await Promise.all([client.listUnits(), client.getStatus()]);
      setUnits(listed);
      setStatus(listedStatus);
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

  return (
    <div>
      <h2>Feed</h2>
      <p className="note">
        本地 Mock 种子单元（标题带 [Mock] 标记），没有排序/生成引擎与真实来源。操作：已读、反应、点击、删除。提示词、重新生成、重排、搜索、来源图标、媒体不在本面。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p>{notice}</p>}
      {status !== null && (
        <p>
          单元 {status.units} · 未读 {status.unseen}
        </p>
      )}
      <button
        type="button"
        disabled={busy || units.length === 0}
        onClick={() =>
          void run(
            () => client.seenUnits(units.map((unit) => unit.unitId)),
            "已全部标记已读。",
          )
        }
      >
        全部标为已读
      </button>
      {units.length === 0 ? (
        <p className="empty">Feed 为空。</p>
      ) : (
        <ul>
          {units.map((unit) => (
            <li key={unit.unitId}>
              <strong>{unit.title}</strong>（{unit.source}，{unit.seen ? "已读" : "未读"}，点击{" "}
              {unit.tapCount}
              {unit.reaction !== null ? `，反应：${unit.reaction}` : ""}）
              <p>{unit.body}</p>
              <input
                type="text"
                placeholder="反应（如 like）"
                value={reaction[unit.unitId] ?? ""}
                onChange={(event) =>
                  setReaction({ ...reaction, [unit.unitId]: event.target.value })
                }
              />
              <button
                type="button"
                disabled={busy || (reaction[unit.unitId] ?? "").trim() === ""}
                onClick={() =>
                  void run(
                    () => client.reactUnit(unit.unitId, reaction[unit.unitId].trim()),
                    "反应已保存。",
                  )
                }
              >
                反应
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => void run(() => client.tapUnit(unit.unitId), "已点击。")}
              >
                点击
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => void run(() => client.deleteUnit(unit.unitId), "已删除。")}
              >
                删除
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
