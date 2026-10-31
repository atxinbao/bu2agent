//! Smoke tests for Task 15 peers record subset: peer
//! registry via invite accept, invite open/accept/revoke,
//! inbox deliver/decline, requests as the second view of
//! invites, status counts, disconnect tombstones, and
//! restart recovery through tagged JSONL.
//!
//! Covers the 14 implemented routes: GET /peers,
//! GET/PATCH/DELETE /peers/{muse_id}, GET/POST
//! /peers/invites, POST /peers/invites/accept|open,
//! DELETE /peers/invites/{invite_id}, GET /peers/inbox,
//! POST /peers/inbox/decline|deliver, GET /peers/requests,
//! GET /peers/status. SAS, avatar/photo, and the fs/write
//! discretion route are excluded by design.

use mock_gateway::server::{app, AppState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start_server(state: AppState) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
    let addr = listener.local_addr().expect("test listener addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("serve test server");
    });
    (addr, handle)
}

async fn raw(addr: std::net::SocketAddr, request: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect test server");
    stream.write_all(request.as_bytes()).await.expect("write test request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read test response");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    (head.lines().next().unwrap_or("").to_string(), body.to_string())
}

async fn call(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> (u16, serde_json::Value) {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let (head, resp_body) = raw(addr, &request).await;
    let status = head.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    let value: serde_json::Value = serde_json::from_str(&resp_body).unwrap_or(serde_json::Value::Null);
    (status, value)
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-peers-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn peers_invite_accept_inbox_disconnect_and_restart() {
    let path = temp_root("smoke").join("peers.jsonl");
    let state = AppState::default().with_peer_state_path(path.clone());
    let (addr, handle) = start_server(state).await;

    let (status, peers) = call(addr, "GET", "/peers", "").await;
    assert_eq!(status, 200);
    assert_eq!(peers.as_array().unwrap().len(), 0);

    // Invite creates both an invite and a request record.
    let (status, invite) = call(
        addr,
        "POST",
        "/peers/invites",
        r#"{"muse_id":"muse-ada","display_name":"Ada"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(invite["invite_id"], "invite-1");
    assert_eq!(invite["status"], "pending");
    let (status, requests) = call(addr, "GET", "/peers/requests", "").await;
    assert_eq!(status, 200);
    assert_eq!(requests.as_array().unwrap().len(), 1);
    assert_eq!(requests[0]["relationship_generation_id"], "invite-1");
    assert_eq!(requests[0]["status"], "pending");

    // Open is idempotent; accept creates the peer.
    let (status, invite) = call(addr, "POST", "/peers/invites/open", r#"{"invite_id":"invite-1"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(invite["status"], "opened");
    let (status, invite) = call(addr, "POST", "/peers/invites/open", r#"{"invite_id":"invite-1"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(invite["status"], "opened");
    let (status, invite) = call(addr, "POST", "/peers/invites/accept", r#"{"invite_id":"invite-1"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(invite["status"], "accepted");
    let (status, peers) = call(addr, "GET", "/peers", "").await;
    assert_eq!(status, 200);
    assert_eq!(peers.as_array().unwrap().len(), 1);
    assert_eq!(peers[0]["muse_id"], "muse-ada");
    assert_eq!(peers[0]["status"], "connected");
    // Accept again: idempotent, no duplicate peer.
    let (status, _) = call(addr, "POST", "/peers/invites/accept", r#"{"invite_id":"invite-1"}"#).await;
    assert_eq!(status, 200);
    let (status, peers) = call(addr, "GET", "/peers", "").await;
    assert_eq!(status, 200);
    assert_eq!(peers.as_array().unwrap().len(), 1);

    // Update, then inbox deliver/decline.
    let (status, peer) = call(addr, "PATCH", "/peers/muse-ada", r#"{"display_name":"Ada L"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(peer["display_name"], "Ada L");
    let (status, message) = call(
        addr,
        "POST",
        "/peers/inbox/deliver",
        r#"{"from_muse_id":"muse-ada","text":"hi"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(message["message_id"], "inbox-1");
    assert_eq!(message["status"], "pending");
    let (status, message) = call(addr, "POST", "/peers/inbox/decline", r#"{"message_id":"inbox-1"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(message["status"], "declined");

    // Status counts reflect the finished flows.
    let (status, summary) = call(addr, "GET", "/peers/status", "").await;
    assert_eq!(status, 200);
    assert_eq!(summary["peers"], 1);
    assert_eq!(summary["pending_invites"], 0);
    assert_eq!(summary["pending_inbox"], 0);
    assert_eq!(summary["pending_requests"], 0);

    // Revoked invite cannot be accepted (409); unknown
    // ids are 404.
    let (status, invite2) = call(
        addr,
        "POST",
        "/peers/invites",
        r#"{"muse_id":"muse-bob","display_name":"Bob"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(invite2["invite_id"], "invite-2");
    let (status, _) = call(addr, "DELETE", "/peers/invites/invite-2", "").await;
    assert_eq!(status, 200);
    let (status, _) = call(addr, "POST", "/peers/invites/accept", r#"{"invite_id":"invite-2"}"#).await;
    assert_eq!(status, 409);
    let (status, _) = call(addr, "GET", "/peers/muse-nope", "").await;
    assert_eq!(status, 404);
    let (status, _) = call(addr, "POST", "/peers/invites/accept", r#"{"invite_id":"invite-9"}"#).await;
    assert_eq!(status, 404);

    // Disconnect hides from the list but get still shows
    // the tombstone; patch on it is 404; disconnect again
    // is idempotent success.
    let (status, peer) = call(addr, "DELETE", "/peers/muse-ada", "").await;
    assert_eq!(status, 200);
    assert_eq!(peer["disconnected"], true);
    let (status, peers) = call(addr, "GET", "/peers", "").await;
    assert_eq!(status, 200);
    assert_eq!(peers.as_array().unwrap().len(), 0);
    let (status, peer) = call(addr, "GET", "/peers/muse-ada", "").await;
    assert_eq!(status, 200);
    assert_eq!(peer["disconnected"], true);
    let (status, _) = call(addr, "PATCH", "/peers/muse-ada", r#"{"display_name":"X"}"#).await;
    assert_eq!(status, 404);
    let (status, _) = call(addr, "DELETE", "/peers/muse-ada", "").await;
    assert_eq!(status, 200);

    // Restart recovery: tombstones stay hidden, resolved
    // records stay visible.
    handle.abort();
    let state = AppState::default().with_peer_state_path(path);
    let (addr, handle) = start_server(state).await;
    let (status, peers) = call(addr, "GET", "/peers", "").await;
    assert_eq!(status, 200);
    assert_eq!(peers.as_array().unwrap().len(), 0);
    let (status, invites) = call(addr, "GET", "/peers/invites", "").await;
    assert_eq!(status, 200);
    assert_eq!(invites.as_array().unwrap().len(), 1);
    assert_eq!(invites[0]["invite_id"], "invite-1");
    let (status, inbox) = call(addr, "GET", "/peers/inbox", "").await;
    assert_eq!(status, 200);
    assert_eq!(inbox.as_array().unwrap().len(), 1);
    assert_eq!(inbox[0]["status"], "declined");
    handle.abort();
}
