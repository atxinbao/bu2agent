//! Unified file execution tests for the Mock Gateway (round 2).
//!
//! File writes and deletes no longer execute on a bare call: the
//! client requests an approval bound to the exact operation (path,
//! content hash, write mode), the user approves it, and execution
//! consumes that approval exactly once. A changed parameter, a
//! legacy unbound approval, a second execution, or a failed audit
//! write must all refuse execution without touching the file.

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
    raw(
        addr,
        &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"),
    )
    .await
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-operation-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn b64(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn value(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("response json")
}

/// Request a bound approval for one operation and approve it.
/// `params` is the JSON object body after the tool field.
async fn approve(addr: std::net::SocketAddr, tool: &str, params: &str) -> String {
    let (status, body) = post(addr, "/approval/request", &format!("{{\"tool\":\"{tool}\",{params}}}")).await;
    assert!(status.contains("200"), "request status: {status} body: {body}");
    let id = value(&body)["tool_call_id"].as_str().expect("approval id").to_string();
    let (status, body) = post(addr, "/approval/decide", &format!("{{\"tool_call_id\":\"{id}\",\"status\":\"approved\"}}")).await;
    assert!(status.contains("200"), "decide status: {status} body: {body}");
    id
}

async fn approve_write(addr: std::net::SocketAddr, path: &str, content: &[u8], append: bool) -> String {
    approve(addr, "fs.write", &format!("\"path\":\"{path}\",\"content_base64\":\"{}\",\"append\":{append}", b64(content))).await
}

async fn approve_delete(addr: std::net::SocketAddr, path: &str) -> String {
    approve(addr, "fs.delete", &format!("\"path\":\"{path}\"")).await
}

#[tokio::test]
async fn bound_write_roundtrip() {
    let files_root = temp_root("roundtrip");
    let approvals = files_root.join("approvals.jsonl");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals));
    let (addr, server) = start_server(state).await;

    let id = approve_write(addr, "report.md", b"hello", false).await;
    let (status, body) = post(addr, "/files/write", &format!("{{\"path\":\"report.md\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}", b64(b"hello"))).await;
    assert!(status.contains("200"), "write status: {status} body: {body}");

    let (status, body) = get(addr, "/files/read?path=report.md").await;
    assert!(status.contains("200"), "read status: {status} body: {body}");
    assert!(body.contains(&b64(b"hello")), "read body: {body}");

    // The same approval is spent: a second execution is a 403.
    let (status, _body) = post(addr, "/files/write", &format!("{{\"path\":\"report.md\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}", b64(b"hello"))).await;
    assert!(status.contains("403"), "second write must be 403: {status}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn bare_write_and_delete_are_forbidden() {
    let files_root = temp_root("bare");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, _body) = post(addr, "/files/write", &format!("{{\"path\":\"x.md\",\"content_base64\":\"{}\"}}", b64(b"x"))).await;
    assert!(status.contains("403"), "bare write must be 403: {status}");
    assert!(!files_root.join("x.md").exists(), "bare write must not create the file");

    let (status, _body) = post(addr, "/files/delete", r#"{"path":"x.md"}"#).await;
    assert!(status.contains("403"), "bare delete must be 403: {status}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn digest_mismatch_rejects_changed_parameters_without_burning() {
    let files_root = temp_root("mismatch");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let id = approve_write(addr, "a.txt", b"hello", false).await;
    // Changed content.
    let (status, _body) = post(addr, "/files/write", &format!("{{\"path\":\"a.txt\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}", b64(b"helloo"))).await;
    assert!(status.contains("403"), "changed content must be 403: {status}");
    // Changed path.
    let (status, _body) = post(addr, "/files/write", &format!("{{\"path\":\"b.txt\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}", b64(b"hello"))).await;
    assert!(status.contains("403"), "changed path must be 403: {status}");
    // Changed mode: approved overwrite, attempted append.
    let (status, _body) = post(addr, "/files/write", &format!("{{\"path\":\"a.txt\",\"content_base64\":\"{}\",\"append\":true,\"approval_id\":\"{id}\"}}", b64(b"hello"))).await;
    assert!(status.contains("403"), "changed mode must be 403: {status}");
    assert!(!files_root.join("a.txt").exists(), "mismatched writes must not create the file");
    assert!(!files_root.join("b.txt").exists(), "mismatched writes must not create the file");
    // Mismatches do not consume: the exact approved write still works.
    let (status, body) = post(addr, "/files/write", &format!("{{\"path\":\"a.txt\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}", b64(b"hello"))).await;
    assert!(status.contains("200"), "exact write must be 200: {status} body: {body}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn legacy_unbound_approval_cannot_execute() {
    let files_root = temp_root("legacy");
    let approvals = files_root.join("approvals.jsonl");
    std::fs::write(&approvals, "{\"tool_call_id\":\"approval-9\",\"tool\":\"fs.write\",\"status\":\"approved\"}\n").expect("seed approvals");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals));
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/files/write", &format!("{{\"path\":\"old.md\",\"content_base64\":\"{}\",\"approval_id\":\"approval-9\"}}", b64(b"old"))).await;
    assert!(status.contains("403"), "legacy approval must be 403: {status} body: {body}");
    assert!(!files_root.join("old.md").exists(), "legacy approval must not write");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn consume_persist_failure_blocks_execution() {
    let files_root = temp_root("persistfail");
    let approvals = files_root.join("approvals.jsonl");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals.clone()));
    let (addr, server) = start_server(state).await;

    let id = approve_write(addr, "p.md", b"data", false).await;
    // Break the audit file: replace it with a directory so the
    // consumption append fails.
    std::fs::remove_file(&approvals).expect("remove approvals file");
    std::fs::create_dir(&approvals).expect("approvals path becomes a directory");
    let (status, body) = post(addr, "/files/write", &format!("{{\"path\":\"p.md\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}", b64(b"data"))).await;
    assert!(status.contains("500"), "persist failure must be 500: {status} body: {body}");
    assert!(body.contains("approval_audit_failed"), "body: {body}");
    assert!(!files_root.join("p.md").exists(), "failed consumption must not write");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn bound_decide_persist_failure_keeps_pending() {
    let files_root = temp_root("decidefail");
    let approvals = files_root.join("approvals.jsonl");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals.clone()));
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/approval/request", &format!("{{\"tool\":\"fs.write\",\"path\":\"d.md\",\"content_base64\":\"{}\"}}", b64(b"d"))).await;
    assert!(status.contains("200"), "request status: {status} body: {body}");
    let id = value(&body)["tool_call_id"].as_str().expect("approval id").to_string();
    std::fs::remove_file(&approvals).expect("remove approvals file");
    std::fs::create_dir(&approvals).expect("approvals path becomes a directory");
    let (status, body) = post(addr, "/approval/decide", &format!("{{\"tool_call_id\":\"{id}\",\"status\":\"approved\"}}")).await;
    assert!(status.contains("500"), "decide persist failure must be 500: {status} body: {body}");
    assert!(body.contains("approval_audit_failed"), "body: {body}");
    // The decision did not take effect: execution stays forbidden
    // (status is still pending, checked before any disk write).
    let (status, _body) = post(addr, "/files/write", &format!("{{\"path\":\"d.md\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}", b64(b"d"))).await;
    assert!(status.contains("403"), "undecided approval must be 403: {status}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn concurrent_consume_allows_exactly_one() {
    let files_root = temp_root("race");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let id = approve_write(addr, "race.md", b"race", false).await;
    let body = format!("{{\"path\":\"race.md\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}", b64(b"race"));
    let (first, second) = tokio::join!(
        post(addr, "/files/write", &body),
        post(addr, "/files/write", &body),
    );
    // Exactly one execution succeeds; the order between the two
    // concurrent requests is not specified, so assert the pair
    // without sorting (status lines sort "200" before "403").
    let statuses = [first.0, second.0];
    assert!(
        statuses.iter().any(|s| s.contains("200")) && statuses.iter().any(|s| s.contains("403")),
        "one execution must be 200 and the other 403: {statuses:?}"
    );

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn agent_bound_write_and_delete() {
    let files_root = temp_root("agent");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let id = approve_write(addr, "agent.md", b"agent body", false).await;
    let (status, body) = post(addr, "/agent/tool/call", &format!("{{\"tool\":\"fs.write\",\"args\":{{\"path\":\"agent.md\",\"text\":\"agent body\"}},\"tool_call_id\":\"{id}\"}}")).await;
    assert!(status.contains("200"), "agent write status: {status} body: {body}");
    assert!(value(&body)["executed"].as_bool().unwrap_or(false), "body: {body}");

    // Bare agent call without an approval id is forbidden.
    let (status, _body) = post(addr, "/agent/tool/call", r#"{"tool":"fs.write","args":{"path":"agent.md","text":"nope"}}"#).await;
    assert!(status.contains("403"), "bare agent write must be 403: {status}");

    // Mismatched text under a fresh approval is forbidden.
    let id2 = approve_write(addr, "agent.md", b"agent body", false).await;
    let (status, _body) = post(addr, "/agent/tool/call", &format!("{{\"tool\":\"fs.write\",\"args\":{{\"path\":\"agent.md\",\"text\":\"different\"}},\"tool_call_id\":\"{id2}\"}}")).await;
    assert!(status.contains("403"), "mismatched agent write must be 403: {status}");

    // Bound agent delete works through the same gate.
    let id3 = approve_delete(addr, "agent.md").await;
    let (status, body) = post(addr, "/agent/tool/call", &format!("{{\"tool\":\"fs.delete\",\"args\":{{\"path\":\"agent.md\"}},\"tool_call_id\":\"{id3}\"}}")).await;
    assert!(status.contains("200"), "agent delete status: {status} body: {body}");
    assert!(!files_root.join("agent.md").exists(), "agent delete must remove the file");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn tool_only_file_approval_request_is_rejected() {
    let files_root = temp_root("toolonly");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, _body) = post(addr, "/approval/request", r#"{"tool":"fs.write"}"#).await;
    assert!(status.contains("400"), "tool-only write approval must be 400: {status}");
    let (status, _body) = post(addr, "/approval/request", r#"{"tool":"fs.delete"}"#).await;
    assert!(status.contains("400"), "tool-only delete approval must be 400: {status}");
    // Connector tools keep tool-only requests.
    let (status, body) = post(addr, "/approval/request", r#"{"tool":"notes.create"}"#).await;
    assert!(status.contains("200"), "connector approval status: {status} body: {body}");
    assert!(value(&body)["requires_approval"].as_bool().unwrap_or(false), "body: {body}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}
