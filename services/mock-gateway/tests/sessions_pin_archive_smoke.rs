//! Smoke tests for Task 8 session pin / archive / rename.
//!
//! Covers the five `POST /session/*` routes, the list semantics
//! (pinned first, archived hidden by default, `include_archived`
//! opt-in), get-on-archived, blank-title rename rejection,
//! unknown/deleted-session 404s, idempotent unpin/unarchive,
//! and restart recovery of the new flags through the JSONL store.

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
    let dir = std::env::temp_dir().join(format!("mock-session-pin-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn value(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("session json")
}

fn ids(body: &str) -> Vec<String> {
    value(body)
        .as_array()
        .expect("list array")
        .iter()
        .map(|v| v["session_id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn pin_archive_rename_roundtrip() {
    let files_root = temp_root("roundtrip");
    let sessions_path = files_root.join("sessions.jsonl");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_sessions_path(sessions_path.clone());
    let (addr, server) = start_server(state).await;

    for _ in 0..3 {
        let (status, _) = post(addr, "/sessions/create", r#"{}"#).await;
        assert!(status.contains("200"), "create status: {status}");
    }

    // Pin session-2: it jumps to the front of the default list.
    let (status, body) = post(addr, "/session/pin", r#"{"session_id":"session-2"}"#).await;
    assert!(status.contains("200"), "pin status: {status} body: {body}");
    assert_eq!(value(&body)["pinned"], true);
    let (status, body) = get(addr, "/sessions/list").await;
    assert!(status.contains("200"), "list status: {status}");
    assert_eq!(ids(&body), vec!["session-2", "session-1", "session-3"]);

    // Archive session-3: hidden by default, visible with the opt-in,
    // still retrievable with get.
    let (status, body) = post(addr, "/session/archive", r#"{"session_id":"session-3"}"#).await;
    assert!(status.contains("200"), "archive status: {status} body: {body}");
    assert_eq!(value(&body)["archived"], true);
    let (_, body) = get(addr, "/sessions/list").await;
    assert_eq!(ids(&body), vec!["session-2", "session-1"]);
    let (_, body) = get(addr, "/sessions/list?include_archived=true").await;
    assert_eq!(ids(&body), vec!["session-2", "session-1", "session-3"]);
    assert_eq!(value(&body).as_array().unwrap()[2]["archived"], true);
    let (status, body) = get(addr, "/sessions/get?session_id=session-3").await;
    assert!(status.contains("200"), "get archived status: {status} body: {body}");
    assert_eq!(value(&body)["archived"], true);

    // Rename session-1; blank titles and unknown ids are rejected.
    let (status, body) = post(addr, "/session/rename", r#"{"session_id":"session-1","title":"renamed"}"#).await;
    assert!(status.contains("200"), "rename status: {status} body: {body}");
    assert_eq!(value(&body)["title"], "renamed");
    let (status, _) = post(addr, "/session/rename", r#"{"session_id":"session-1","title":"   "}"#).await;
    assert!(status.contains("400"), "blank rename status: {status}");
    let (status, _) = post(addr, "/session/rename", r#"{"session_id":"session-9","title":"x"}"#).await;
    assert!(status.contains("404"), "unknown rename status: {status}");

    // Idempotent no-ops succeed with the current state.
    let (status, body) = post(addr, "/session/unpin", r#"{"session_id":"session-1"}"#).await;
    assert!(status.contains("200"), "unpin no-op status: {status}");
    assert_eq!(value(&body)["pinned"], false);
    let (status, body) = post(addr, "/session/unarchive", r#"{"session_id":"session-1"}"#).await;
    assert!(status.contains("200"), "unarchive no-op status: {status}");
    assert_eq!(value(&body)["archived"], false);

    // Operations on a tombstoned session read as 404.
    let (status, _) = post(addr, "/sessions/delete", r#"{"session_id":"session-2"}"#).await;
    assert!(status.contains("200"), "delete status: {status}");
    let (status, _) = post(addr, "/session/pin", r#"{"session_id":"session-2"}"#).await;
    assert!(status.contains("404"), "pin deleted status: {status}");

    // Unarchive session-3, then restart: flags survive the JSONL reload.
    let (status, _) = post(addr, "/session/unarchive", r#"{"session_id":"session-3"}"#).await;
    assert!(status.contains("200"), "unarchive status: {status}");
    server.abort();
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_sessions_path(sessions_path.clone());
    let (addr, server2) = start_server(state).await;
    let (status, body) = get(addr, "/sessions/list").await;
    assert!(status.contains("200"), "relist status: {status}");
    let list = value(&body);
    let by_id: std::collections::HashMap<&str, &serde_json::Value> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|v| (v["session_id"].as_str().unwrap(), v))
        .collect();
    assert_eq!(by_id["session-1"]["title"], "renamed");
    assert_eq!(by_id["session-3"]["archived"], false);
    assert!(!by_id.contains_key("session-2"), "deleted session stays hidden after restart");
    server2.abort();
}
