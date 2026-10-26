//! File endpoint smoke tests for the Mock Gateway Axum transport.
//!
//! Covers the Phase 2 file rules on the server side: scoped root,
//! chunked read with offset/len/base64/EOF, write/append, delete,
//! and rejection of paths that escape the root. Writes and deletes
//! require a bound approval (round 2 unified execution): the
//! approval request carries the exact operation, and execution
//! consumes it once.

use mock_gateway::server::{app, new_state_with_files_root};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start_server(root: std::path::PathBuf) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
    let addr = listener.local_addr().expect("test listener addr");
    tokio::spawn(async move {
        axum::serve(listener, app(new_state_with_files_root(root)))
            .await
            .expect("serve test server");
    });
    addr
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

async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    raw(
        addr,
        &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"),
    )
    .await
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

fn temp_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("mock-files-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

/// Request a bound approval for one file operation and approve it.
async fn approve(addr: std::net::SocketAddr, tool: &str, params: &str) -> String {
    let (status, body) = post(addr, "/approval/request", &format!("{{\"tool\":\"{tool}\",{params}}}")).await;
    assert!(status.contains("200"), "request status: {status} body: {body}");
    let id = serde_json::from_str::<serde_json::Value>(&body).expect("request json")["tool_call_id"]
        .as_str()
        .expect("approval id")
        .to_string();
    let (status, body) = post(addr, "/approval/decide", &format!("{{\"tool_call_id\":\"{id}\",\"status\":\"approved\"}}")).await;
    assert!(status.contains("200"), "decide status: {status} body: {body}");
    id
}

#[tokio::test]
async fn file_write_read_list_delete_and_escape() {
    let root = temp_root("smoke");
    let addr = start_server(root.clone()).await;

    // Write "hello world" (base64: aGVsbG8gd29ybGQ=).
    let id = approve(addr, "fs.write", r#""path":"demo.txt","content_base64":"aGVsbG8gd29ybGQ=","append":false"#).await;
    let (status, body) = post(
        addr,
        "/files/write",
        &format!("{{\"path\":\"demo.txt\",\"content_base64\":\"aGVsbG8gd29ybGQ=\",\"append\":false,\"approval_id\":\"{id}\"}}"),
    )
    .await;
    assert!(status.contains("200"), "write status: {status} body: {body}");
    assert!(body.contains("\"size_bytes\":11"), "write body: {body}");

    // Chunked read: first 5 bytes, not EOF; then the rest, EOF.
    let (status, body) = get(addr, "/files/read?path=demo.txt&offset=0&len=5").await;
    assert!(status.contains("200"), "read status: {status}");
    let chunk: serde_json::Value = serde_json::from_str(&body).expect("read json");
    assert_eq!(chunk["content_base64"].as_str(), Some("aGVsbG8="));
    assert_eq!(chunk["eof"].as_bool(), Some(false));
    assert_eq!(chunk["total_size"].as_u64(), Some(11));
    let (status, body) = get(addr, "/files/read?path=demo.txt&offset=5&len=100").await;
    assert!(status.contains("200"), "read2 status: {status}");
    let chunk: serde_json::Value = serde_json::from_str(&body).expect("read2 json");
    assert_eq!(chunk["content_base64"].as_str(), Some("IHdvcmxk"));
    assert_eq!(chunk["eof"].as_bool(), Some(true));

    // Append "!" and list.
    let id = approve(addr, "fs.write", r#""path":"demo.txt","content_base64":"IQ==","append":true"#).await;
    let (status, _) = post(
        addr,
        "/files/write",
        &format!("{{\"path\":\"demo.txt\",\"content_base64\":\"IQ==\",\"append\":true,\"approval_id\":\"{id}\"}}"),
    )
    .await;
    assert!(status.contains("200"), "append status: {status}");
    let (status, body) = get(addr, "/files/list").await;
    assert!(status.contains("200"), "list status: {status}");
    let list: serde_json::Value = serde_json::from_str(&body).expect("list json");
    assert_eq!(list.as_array().expect("list array").len(), 1);
    assert_eq!(list[0]["name"].as_str(), Some("demo.txt"));
    assert_eq!(list[0]["size_bytes"].as_u64(), Some(12));

    // Escape attempts are rejected, not served.
    let (status, _) = get(addr, "/files/read?path=../secret.txt").await;
    assert!(status.contains("400"), "escape read status: {status}");
    let (status, _) = post(
        addr,
        "/files/write",
        r#"{"path":"../evil.txt","content_base64":"eA==","append":false}"#,
    )
    .await;
    assert!(status.contains("400"), "escape write status: {status}");

    // Delete, then list is empty and read is 404.
    let id = approve(addr, "fs.delete", r#""path":"demo.txt""#).await;
    let (status, body) = post(addr, "/files/delete", &format!("{{\"path\":\"demo.txt\",\"approval_id\":\"{id}\"}}")).await;
    assert!(status.contains("200"), "delete status: {status} body: {body}");
    let (status, body) = get(addr, "/files/list").await;
    assert!(status.contains("200"), "list2 status: {status}");
    assert_eq!(body, "[]", "list2 body: {body}");
    let (status, _) = get(addr, "/files/read?path=demo.txt").await;
    assert!(status.contains("404"), "read deleted status: {status}");

    let _ = std::fs::remove_dir_all(&root);
}
