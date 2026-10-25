//! Approval persistence tests for the Mock Gateway (stage 2).
//!
//! Proves the JSONL roundtrip end to end: request an approval,
//! approve it, execute a write with it, then restart the server
//! on the same approvals file and execute another write with the
//! SAME id. If reload failed, the second call would 403.
//! The server is started on ephemeral ports and spoken to with
//! raw HTTP over a TcpStream, like the other smoke tests.

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

fn temp_paths(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("mock-approvals-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let file = dir.join("approvals.jsonl");
    (dir, file)
}

#[tokio::test]
async fn approval_survives_server_restart() {
    let (files_root, approvals_path) = temp_paths("restart");
    let state1 =
        AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals_path.clone()));
    let (addr1, server1) = start_server(state1).await;

    let (status, body) = post(addr1, "/approval/request", "{\"tool\":\"fs.write\"}").await;
    assert!(status.contains("200"), "request status: {status} body: {body}");
    let id: String = serde_json::from_str::<serde_json::Value>(&body)
        .expect("request json")["tool_call_id"]
        .as_str()
        .expect("tool_call_id string")
        .to_string();
    assert_eq!(id, "approval-1");

    let (status, _) = post(
        addr1,
        "/approval/decide",
        &format!("{{\"tool_call_id\":\"{id}\",\"status\":\"approved\"}}"),
    )
    .await;
    assert!(status.contains("200"), "decide status: {status}");

    let (status, _) = post(
        addr1,
        "/agent/tool/call",
        &format!(
            "{{\"tool\":\"fs.write\",\"args\":{{\"path\":\"persisted.txt\",\"text\":\"before restart\"}},\"tool_call_id\":\"{id}\"}}"
        ),
    )
    .await;
    assert!(status.contains("200"), "first write status: {status}");

    server1.abort();

    // Restart on the same approvals file: the same id must still
    // be approved, and the next request must continue the counter.
    let state2 =
        AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals_path.clone()));
    let (addr2, server2) = start_server(state2).await;

    let (status, _) = post(
        addr2,
        "/agent/tool/call",
        &format!(
            "{{\"tool\":\"fs.write\",\"args\":{{\"path\":\"persisted2.txt\",\"text\":\"after restart\"}},\"tool_call_id\":\"{id}\"}}"
        ),
    )
    .await;
    assert!(status.contains("200"), "reloaded write status: {status}");

    let (status, body) = post(addr2, "/approval/request", "{\"tool\":\"fs.write\"}").await;
    assert!(status.contains("200"), "second request status: {status}");
    let id2: String = serde_json::from_str::<serde_json::Value>(&body)
        .expect("request json")["tool_call_id"]
        .as_str()
        .expect("tool_call_id string")
        .to_string();
    assert_eq!(id2, "approval-2");

    server2.abort();
    let text = std::fs::read_to_string(&approvals_path).expect("read approvals file");
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3, "approvals file lines: {text}");
    assert!(lines[2].contains("approval-2"), "last line: {}", lines[2]);
    let _ = std::fs::remove_dir_all(&files_root);
}
