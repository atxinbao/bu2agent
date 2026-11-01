//! Smoke tests for Task 18 chat record subset: themes
//! get/set, unread-count, mark-seen, delete-turn (tombstone),
//! message get, history-window, and search, plus restart
//! recovery through the session snapshots and chat.jsonl.
//!
//! Stream/subscribe/stream-events, poke, cancel, and
//! sxs.select are excluded by design (see the Task 18 plan).

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
    let dir = std::env::temp_dir().join(format!("mock-chat-{name}-{ }", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn chat_records_flow_and_restart() {
    let dir = temp_root("smoke");
    let sessions_path = dir.join("sessions.jsonl");
    let chat_path = dir.join("chat.jsonl");
    let state = AppState::default()
        .with_sessions_path(sessions_path.clone())
        .with_chat_state_path(chat_path.clone());
    let (addr, handle) = start_server(state).await;

    // Themes: default system, set dark, bogus rejected.
    let (status, themes) = call(addr, "GET", "/api/chat/themes", "").await;
    assert_eq!(status, 200);
    assert_eq!(themes["theme"], "system");
    assert_eq!(themes["available"], serde_json::json!(["system", "light", "dark"]));
    let (status, themes) = call(addr, "POST", "/api/chat/themes/set", r#"{"theme":"dark"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(themes["theme"], "dark");
    let (status, _) = call(addr, "POST", "/api/chat/themes/set", r#"{"theme":"neon"}"#).await;
    assert_eq!(status, 400);
    let (status, themes) = call(addr, "GET", "/api/chat/themes", "").await;
    assert_eq!(status, 200);
    assert_eq!(themes["theme"], "dark");

    // One session, two exchanges (disabled DeepSeek echoes).
    let (status, created) = call(addr, "POST", "/sessions/create", r#"{"title":"chat test"}"#).await;
    assert_eq!(status, 200);
    let thread = created["session_id"].as_str().expect("session id").to_string();
    for text in ["hello", "world"] {
        let body = serde_json::json!({ "session_id": thread, "text": text }).to_string();
        let (status, _) = call(addr, "POST", "/sessions/send", &body).await;
        assert_eq!(status, 200);
    }

    // Unread: 4 entries, all unread.
    let (status, unread) = call(addr, "GET", "/chat/unread-count", "").await;
    assert_eq!(status, 200);
    assert_eq!(unread["unread_count"], 4);
    assert_eq!(unread["threads"][0]["thread_id"], thread);
    assert_eq!(unread["threads"][0]["unread"], 4);

    // Delete-turn: index 1 (the "hello" echo) becomes a
    // tombstone; re-delete is idempotent success.
    let body = serde_json::json!({ "thread_id": thread, "index": 1 }).to_string();
    let (status, deleted) = call(addr, "POST", "/api/chat/delete-turn", &body).await;
    assert_eq!(status, 200);
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["index"], 1);
    let (status, deleted) = call(addr, "POST", "/api/chat/delete-turn", &body).await;
    assert_eq!(status, 200);
    assert_eq!(deleted["deleted"], true);

    // Message get still returns the tombstoned turn.
    let (status, msg) =
        call(addr, "GET", &format!("/chat/message?thread_id={thread}&index=1"), "").await;
    assert_eq!(status, 200);
    assert_eq!(msg["deleted"], true);
    assert_eq!(msg["text"], "hello");
    let (status, _) =
        call(addr, "GET", &format!("/chat/message?thread_id={thread}&index=99"), "").await;
    assert_eq!(status, 404);
    let (status, _) = call(addr, "GET", "/chat/message?thread_id=nope&index=0", "").await;
    assert_eq!(status, 404);

    // History window hides the tombstone but keeps stored
    // indices; paging over the visible entries works.
    let (status, window) =
        call(addr, "GET", &format!("/chat/history-window?thread_id={thread}"), "").await;
    assert_eq!(status, 200);
    assert_eq!(window["total_visible"], 3);
    assert_eq!(window["entries"][0]["index"], 0);
    assert_eq!(window["entries"][1]["index"], 2);
    assert_eq!(window["entries"][2]["index"], 3);
    let (status, window) = call(
        addr,
        "GET",
        &format!("/chat/history-window?thread_id={thread}&offset=1&limit=1"),
        "",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(window["entries"].as_array().unwrap().len(), 1);
    assert_eq!(window["entries"][0]["index"], 2);

    // Search skips the tombstone; blank query is 400.
    let (status, matches) = call(addr, "GET", "/chat/search?q=hello", "").await;
    assert_eq!(status, 200);
    assert_eq!(matches.as_array().unwrap().len(), 1);
    assert_eq!(matches[0]["index"], 0);
    let (status, matches) = call(addr, "GET", "/chat/search?q=world", "").await;
    assert_eq!(status, 200);
    assert_eq!(matches.as_array().unwrap().len(), 2);
    let (status, _) = call(addr, "GET", "/chat/search?q=%20%20", "").await;
    assert_eq!(status, 400);

    // Seen: watermark to index 1 leaves stored indices >= 2
    // unread (2 entries); mark-all clears it.
    let (status, seen) =
        call(addr, "POST", &format!("/chat/{thread}/seen"), r#"{"index":1}"#).await;
    assert_eq!(status, 200);
    assert_eq!(seen["marker"], 2);
    assert_eq!(seen["unread"], 2);
    let (status, seen) = call(addr, "POST", &format!("/chat/{thread}/seen"), r#"{}"#).await;
    assert_eq!(status, 200);
    assert_eq!(seen["marker"], 4);
    assert_eq!(seen["unread"], 0);
    let (status, unread) = call(addr, "GET", "/chat/unread-count", "").await;
    assert_eq!(status, 200);
    assert_eq!(unread["unread_count"], 0);
    let (status, _) = call(addr, "POST", "/chat/nope/seen", r#"{}"#).await;
    assert_eq!(status, 404);

    handle.abort();

    // Restart: theme, watermark, and tombstone all reload.
    let state = AppState::default()
        .with_sessions_path(sessions_path)
        .with_chat_state_path(chat_path);
    let (addr, handle) = start_server(state).await;
    let (status, themes) = call(addr, "GET", "/api/chat/themes", "").await;
    assert_eq!(status, 200);
    assert_eq!(themes["theme"], "dark");
    let (status, unread) = call(addr, "GET", "/chat/unread-count", "").await;
    assert_eq!(status, 200);
    assert_eq!(unread["unread_count"], 0);
    let (status, window) =
        call(addr, "GET", &format!("/chat/history-window?thread_id={thread}"), "").await;
    assert_eq!(status, 200);
    assert_eq!(window["total_visible"], 3);
    handle.abort();
}
