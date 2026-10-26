//! Session record tests for the Mock Gateway (Round 3, 3a).
//!
//! Sessions persist as JSONL (sibling of the file root) and hold
//! message bodies by design, unlike the audit files. Create, list
//! summaries, get one session, send (echo mode: no key, reply is
//! the input), delete as a tombstone (hidden from list/get/send,
//! idempotent), validation (empty/oversized/unknown), isolation
//! between sessions, and restart recovery including tombstones.

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

async fn post(addr: std::net::SocketAddr, path: &str, json: &str) -> (String, String) {
    raw(
        addr,
        &format!(
            "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            json.len(),
            json
        ),
    )
    .await
}

async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    raw(addr, &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")).await
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-session-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn value(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("session json")
}

#[tokio::test]
async fn session_create_list_get_send_delete_roundtrip() {
    let files_root = temp_root("roundtrip");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/sessions/create", r#"{}"#).await;
    assert!(status.contains("200"), "create status: {status} body: {body}");
    let created = value(&body);
    assert_eq!(created["session_id"], "session-1");
    assert_eq!(created["title"], "");
    assert_eq!(created["deleted"], false);
    assert_eq!(created["turns"].as_array().unwrap().len(), 0);

    let (status, body) = post(addr, "/sessions/send", r#"{"session_id":"session-1","text":"hello session"}"#).await;
    assert!(status.contains("200"), "send status: {status} body: {body}");
    let sent = value(&body);
    assert_eq!(sent["title"], "hello session");
    let turns = sent["turns"].as_array().unwrap();
    assert_eq!(turns.len(), 2);
    assert_eq!(turns[0]["role"], "user");
    assert_eq!(turns[0]["text"], "hello session");
    assert_eq!(turns[1]["role"], "assistant");
    assert_eq!(turns[1]["text"], "hello session");
    assert!(turns[1]["model"].is_null());

    let (status, body) = get(addr, "/sessions/list").await;
    assert!(status.contains("200"), "list status: {status} body: {body}");
    let listed = value(&body);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["session_id"], "session-1");
    assert_eq!(listed[0]["turn_count"], 2);

    let (status, body) = post(addr, "/sessions/delete", r#"{"session_id":"session-1"}"#).await;
    assert!(status.contains("200"), "delete status: {status} body: {body}");
    assert_eq!(value(&body)["deleted"], true);

    let (status, _) = get(addr, "/sessions/get?session_id=session-1").await;
    assert!(status.contains("404"), "deleted get status: {status}");
    let (status, _) = post(addr, "/sessions/send", r#"{"session_id":"session-1","text":"again"}"#).await;
    assert!(status.contains("404"), "deleted send status: {status}");
    let (status, body) = get(addr, "/sessions/list").await;
    assert!(status.contains("200"), "list after delete status: {status}");
    assert_eq!(value(&body).as_array().unwrap().len(), 0);
    let (status, _) = post(addr, "/sessions/delete", r#"{"session_id":"session-1"}"#).await;
    assert!(status.contains("200"), "repeat delete status: {status}");
    server.abort();
}

#[tokio::test]
async fn session_send_validation_and_isolation() {
    let files_root = temp_root("validation");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, _) = post(addr, "/sessions/send", r#"{"session_id":"session-9","text":"hi"}"#).await;
    assert!(status.contains("404"), "unknown send status: {status}");
    let (status, _) = get(addr, "/sessions/get?session_id=session-9").await;
    assert!(status.contains("404"), "unknown get status: {status}");
    let (status, _) = post(addr, "/sessions/delete", r#"{"session_id":"session-9"}"#).await;
    assert!(status.contains("404"), "unknown delete status: {status}");

    let (_, body) = post(addr, "/sessions/create", r#"{}"#).await;
    assert_eq!(value(&body)["session_id"], "session-1");
    let (status, _) = post(addr, "/sessions/send", r#"{"session_id":"session-1","text":"   "}"#).await;
    assert!(status.contains("400"), "empty send status: {status}");
    let big = "x".repeat(65 * 1024);
    let (status, _) =
        post(addr, "/sessions/send", &format!("{{\"session_id\":\"session-1\",\"text\":\"{big}\"}}")).await;
    assert!(status.contains("400"), "oversized send status: {status}");
    let (status, body) = get(addr, "/sessions/get?session_id=session-1").await;
    assert!(status.contains("200"), "get after rejected sends status: {status}");
    assert_eq!(value(&body)["turns"].as_array().unwrap().len(), 0);

    let (_, body) = post(addr, "/sessions/create", r#"{}"#).await;
    assert_eq!(value(&body)["session_id"], "session-2");
    let (_, _) = post(addr, "/sessions/send", r#"{"session_id":"session-1","text":"first only"}"#).await;
    let (status, body) = get(addr, "/sessions/get?session_id=session-2").await;
    assert!(status.contains("200"), "second get status: {status}");
    assert_eq!(value(&body)["turns"].as_array().unwrap().len(), 0);
    let (status, body) = get(addr, "/sessions/get?session_id=session-1").await;
    assert!(status.contains("200"), "first get status: {status}");
    assert_eq!(value(&body)["turns"].as_array().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn session_persistence_across_restart() {
    let files_root = temp_root("persistence");
    let sessions_path = files_root.join("sessions.jsonl");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_sessions_path(sessions_path.clone());
    let (addr, server) = start_server(state).await;
    let (_, _) = post(addr, "/sessions/create", r#"{}"#).await;
    let (_, _) = post(addr, "/sessions/send", r#"{"session_id":"session-1","text":"keep me"}"#).await;
    let (_, _) = post(addr, "/sessions/create", r#"{}"#).await;
    let (_, _) = post(addr, "/sessions/delete", r#"{"session_id":"session-2"}"#).await;
    server.abort();

    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_sessions_path(sessions_path.clone());
    let (addr, server) = start_server(state).await;
    let (status, body) = get(addr, "/sessions/get?session_id=session-1").await;
    assert!(status.contains("200"), "restarted get status: {status}");
    let kept = value(&body);
    assert_eq!(kept["turns"].as_array().unwrap().len(), 2);
    assert_eq!(kept["turns"][1]["text"], "keep me");
    let (status, body) = get(addr, "/sessions/list").await;
    assert!(status.contains("200"), "restarted list status: {status}");
    assert_eq!(value(&body).as_array().unwrap().len(), 1);
    let (status, _) = post(addr, "/sessions/create", r#"{}"#).await;
    assert!(status.contains("200"), "restarted create status: {status}");
    let (status, body) = get(addr, "/sessions/list").await;
    assert!(status.contains("200"), "list after create status: {status}");
    assert_eq!(value(&body)[1]["session_id"], "session-3");
    server.abort();
}
