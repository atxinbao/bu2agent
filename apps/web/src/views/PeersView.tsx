import { useEffect, useState } from "react";
import type {
  Peer,
  PeerGateway,
  PeerInboxMessage,
  PeerInvite,
  PeerRequest,
  PeerStatus,
} from "../domain/peer_client";

// Peers view over the PeerGateway interface (Task 15,
// record subset). Local records only: peers form by
// accepting an invite, invites double as their request
// records, inbox deliver creates a pending item and
// decline resolves it. SAS, avatar/photo, and the fs/write
// discretion route are not part of this surface.
export function PeersView({ client }: { client: PeerGateway }) {
  const [peers, setPeers] = useState<Peer[]>([]);
  const [invites, setInvites] = useState<PeerInvite[]>([]);
  const [inbox, setInbox] = useState<PeerInboxMessage[]>([]);
  const [requests, setRequests] = useState<PeerRequest[]>([]);
  const [status, setStatus] = useState<PeerStatus | null>(null);
  const [inviteMuseId, setInviteMuseId] = useState("");
  const [inviteName, setInviteName] = useState("");
  const [inboxFrom, setInboxFrom] = useState("");
  const [inboxText, setInboxText] = useState("");
  const [renameName, setRenameName] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  async function refresh() {
    setBusy(true);
    setError(null);
    try {
      const [listedPeers, listedInvites, listedInbox, listedRequests, listedStatus] =
        await Promise.all([
          client.listPeers(),
          client.listInvites(),
          client.listInbox(),
          client.listRequests(),
          client.getStatus(),
        ]);
      setPeers(listedPeers);
      setInvites(listedInvites);
      setInbox(listedInbox);
      setRequests(listedRequests);
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
      <h2>Peers</h2>
      <p className="note">
        本地关系记录：peer 经邀请接受产生；邀请与请求是同一关系尝试的两个视图；收件箱 deliver
        创建待处理项，decline 解决它。SAS、头像/照片、fs/write 自由裁量路由不在本面。
      </p>
      {error !== null && <p className="error">{error}</p>}
      {notice !== null && <p>{notice}</p>}
      {status !== null && (
        <p>
          已连接 {status.peers} · 待处理邀请 {status.pendingInvites} · 待处理收件{" "}
          {status.pendingInbox} · 待处理请求 {status.pendingRequests}
        </p>
      )}

      <h3>已连接</h3>
      {peers.length === 0 ? (
        <p className="empty">暂无已连接 peer（接受邀请后出现）。</p>
      ) : (
        <ul>
          {peers.map((peer) => (
            <li key={peer.museId}>
              {peer.displayName}（{peer.museId}，{peer.status}）
              <input
                type="text"
                placeholder="新名字"
                value={renameName[peer.museId] ?? ""}
                onChange={(event) =>
                  setRenameName({ ...renameName, [peer.museId]: event.target.value })
                }
              />
              <button
                type="button"
                disabled={busy || (renameName[peer.museId] ?? "").trim() === ""}
                onClick={() =>
                  void run(
                    () => client.updatePeer(peer.museId, renameName[peer.museId].trim()),
                    "已改名。",
                  )
                }
              >
                改名
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => void run(() => client.disconnectPeer(peer.museId), "已断开。")}
              >
                断开
              </button>
            </li>
          ))}
        </ul>
      )}

      <h3>邀请</h3>
      <div>
        <input
          type="text"
          placeholder="muse_id"
          value={inviteMuseId}
          onChange={(event) => setInviteMuseId(event.target.value)}
        />
        <input
          type="text"
          placeholder="显示名"
          value={inviteName}
          onChange={(event) => setInviteName(event.target.value)}
        />
        <button
          type="button"
          disabled={busy || inviteMuseId.trim() === "" || inviteName.trim() === ""}
          onClick={() =>
            void run(
              () => client.createInvite(inviteMuseId.trim(), inviteName.trim()),
              "邀请已创建。",
            )
          }
        >
          创建邀请
        </button>
      </div>
      {invites.length === 0 ? (
        <p className="empty">暂无邀请。</p>
      ) : (
        <ul>
          {invites.map((invite) => (
            <li key={invite.inviteId}>
              {invite.displayName}（{invite.museId}，{invite.status}）
              <button
                type="button"
                disabled={busy}
                onClick={() => void run(() => client.openInvite(invite.inviteId), "已打开。")}
              >
                打开
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => void run(() => client.acceptInvite(invite.inviteId), "已接受。")}
              >
                接受
              </button>
              <button
                type="button"
                disabled={busy}
                onClick={() => void run(() => client.revokeInvite(invite.inviteId), "已撤销。")}
              >
                撤销
              </button>
            </li>
          ))}
        </ul>
      )}

      <h3>收件箱</h3>
      <div>
        <input
          type="text"
          placeholder="from_muse_id"
          value={inboxFrom}
          onChange={(event) => setInboxFrom(event.target.value)}
        />
        <input
          type="text"
          placeholder="内容"
          value={inboxText}
          onChange={(event) => setInboxText(event.target.value)}
        />
        <button
          type="button"
          disabled={busy || inboxFrom.trim() === "" || inboxText.trim() === ""}
          onClick={() =>
            void run(() => client.deliverInbox(inboxFrom.trim(), inboxText.trim()), "已投递。")
          }
        >
          投递
        </button>
      </div>
      {inbox.length === 0 ? (
        <p className="empty">收件箱为空。</p>
      ) : (
        <ul>
          {inbox.map((message) => (
            <li key={message.messageId}>
              {message.fromMuseId}：{message.text}（{message.status}）
              <button
                type="button"
                disabled={busy || message.status === "declined"}
                onClick={() => void run(() => client.declineInbox(message.messageId), "已拒绝。")}
              >
                拒绝
              </button>
            </li>
          ))}
        </ul>
      )}

      <h3>关系请求</h3>
      {requests.length === 0 ? (
        <p className="empty">暂无关系请求。</p>
      ) : (
        <ul>
          {requests.map((request) => (
            <li key={request.relationshipGenerationId}>
              {request.displayName}（{request.museId}，{request.status}）
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
