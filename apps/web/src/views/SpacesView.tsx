import { useEffect, useState } from "react";
import type { Space, SpaceGateway, SpaceItem } from "../domain/space_client";

// Spaces view over the SpaceGateway interface (Task 16,
// v2 record subset). A space is a named collection of
// session/file references only, no rendering. Save
// upserts; delete is a tombstone. Share, debug,
// export_html, CVM, and subscribe are not part of this
// surface.
export function SpacesView({ client }: { client: SpaceGateway }) {
  const [spaces, setSpaces] = useState<Space[]>([]);
  const [slug, setSlug] = useState("");
  const [title, setTitle] = useState("");
  const [itemKind, setItemKind] = useState("file");
  const [itemRef, setItemRef] = useState("");
  const [items, setItems] = useState<SpaceItem[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      setSpaces(await client.listSpaces());
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
      <h2>Spaces</h2>
      <p className="note">
        具名收藏夹：内容只含 session/file 引用，不渲染。保存是 upsert（同名覆盖），删除是
        tombstone。分享、debug、export_html、CVM、订阅不在本面。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p>{notice}</p>}

      <h3>保存空间</h3>
      <div>
        <input
          type="text"
          placeholder="slug"
          value={slug}
          onChange={(event) => setSlug(event.target.value)}
        />
        <input
          type="text"
          placeholder="标题（空则用 slug）"
          value={title}
          onChange={(event) => setTitle(event.target.value)}
        />
        <select value={itemKind} onChange={(event) => setItemKind(event.target.value)}>
          <option value="file">file</option>
          <option value="session">session</option>
        </select>
        <input
          type="text"
          placeholder="引用（如 notes/a.md）"
          value={itemRef}
          onChange={(event) => setItemRef(event.target.value)}
        />
        <button
          type="button"
          disabled={itemRef.trim() === ""}
          onClick={() => {
            setItems([...items, { kind: itemKind, ref: itemRef.trim() }]);
            setItemRef("");
          }}
        >
          加引用
        </button>
        <button
          type="button"
          disabled={busy || slug.trim() === ""}
          onClick={() =>
            void run(
              () => client.saveSpace(slug.trim(), title.trim() || undefined, items),
              "已保存。",
            )
          }
        >
          保存
        </button>
      </div>
      {items.length > 0 && (
        <ul>
          {items.map((item, index) => (
            <li key={`${item.kind}:${item.ref}:${index}`}>
              {item.kind}：{item.ref}
              <button type="button" onClick={() => setItems(items.filter((_, i) => i !== index))}>
                移除
              </button>
            </li>
          ))}
        </ul>
      )}

      <h3>已保存</h3>
      {spaces.length === 0 ? (
        <p className="empty">暂无已保存空间。</p>
      ) : (
        <ul>
          {spaces.map((space) => (
            <li key={space.slug}>
              {space.title}（{space.slug}，{space.items.length} 项）
              {space.items.map((item, index) => (
                <span key={`${item.kind}:${item.ref}:${index}`}>
                  {" "}
                  [{item.kind}：{item.ref}]
                </span>
              ))}
              <button
                type="button"
                disabled={busy}
                onClick={() => {
                  setSlug(space.slug);
                  setTitle(space.title);
                  setItems(space.items);
                }}
              >
                载入编辑
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => void run(() => client.deleteSpace(space.slug), "已删除。")}
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
