//! DeepSeek chat tests for the Mock Gateway (first cut).
//!
//! A fake DeepSeek server (one-shot, tokio) stands in for the
//! real API: the gateway must send the model reply through the
//! normal stream records, keep echoing with no key, return 502
//! (never a silent echo) when a configured key fails, and
//! refuse truncated or empty completions as failures. The
//! fake key is `test-key`; no real credential is used anywhere.

use std::sync::{Arc, Mutex};

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
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

/// One-shot fake DeepSeek server: records the request it saw,
/// then answers with the given status line and JSON body.
async fn start_fake_deepseek(
    status_line: &str,
    body: &str,
) -> (std::net::SocketAddr, Arc<Mutex<String>>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake deepseek");
    let addr = listener.local_addr().expect("fake deepseek addr");
    let seen = Arc::new(Mutex::new(String::new()));
    let seen_clone = seen.clone();
    let response = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let handle = tokio::spawn(async move {
        let Ok((mut socket, _)) = listener.accept().await else { return };
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        // Read headers, then the body per Content-Length.
        let mut header_end = None;
        let mut content_length = 0usize;
        while header_end.is_none() {
            let Ok(n) = socket.read(&mut chunk).await else { return };
            if n == 0 {
                return;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                header_end = Some(pos + 4);
                let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
                for line in head.lines().skip(1) {
                    if let Some((name, value)) = line.split_once(':') {
                        if name.trim().eq_ignore_ascii_case("content-length") {
                            content_length = value.trim().parse().unwrap_or(0);
                        }
                    }
                }
            }
        }
        let end = header_end.expect("header end") + content_length;
        while buf.len() < end {
            let Ok(n) = socket.read(&mut chunk).await else { return };
            if n == 0 {
                return;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        *seen_clone.lock().expect("seen lock") = String::from_utf8_lossy(&buf).into_owned();
        let _ = socket.write_all(response.as_bytes()).await;
    });
    (addr, seen, handle)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-deepseek-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn stream_text(body: &str) -> String {
    let records: serde_json::Value = serde_json::from_str(body).expect("records json");
    let record = records
        .as_array()
        .expect("records array")
        .iter()
        .find(|r| r["event_type"] == agent_desktop::node::events::TEXT_APPEND)
        .expect("text_append record");
    let payload: serde_json::Value =
        serde_json::from_str(record["payload"].as_str().expect("payload string")).expect("payload json");
    payload["text"].as_str().expect("payload text").to_string()
}

const FAKE_REPLY: &str = "模型回复：\"测试通过\"。\n路径\\示例 😀";

fn fake_completion_body() -> String {
    serde_json::json!({
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "model": "deepseek-flash",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": FAKE_REPLY }, "finish_reason": "stop" }],
    })
    .to_string()
}

fn configured_state(files_root: std::path::PathBuf, fake_addr: std::net::SocketAddr) -> AppState {
    let config = DeepSeekConfig {
        base_url: format!("http://{fake_addr}"),
        model: "deepseek-flash".to_string(),
        api_key: Some("test-key".to_string()),
    };
    AppState::with_files_root_and_approvals(files_root, None)
        .with_deepseek(DeepSeekClient::new(config))
}

#[tokio::test]
async fn stream_start_returns_model_reply_when_configured() {
    let (fake_addr, seen, fake) = start_fake_deepseek("200 OK", &fake_completion_body()).await;
    let files_root = temp_root("reply");
    let state = configured_state(files_root.clone(), fake_addr);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(
        addr,
        "/chat/stream/start",
        r#"{"stream_id":"s1","turn_id":"t1","text":"你好"}"#,
    )
    .await;
    assert!(status.contains("200"), "start status: {status} body: {body}");
    assert_eq!(stream_text(&body), FAKE_REPLY, "reply must come from the model, not the echo");

    let request = seen.lock().expect("seen lock").clone();
    assert!(request.starts_with("POST /chat/completions "), "deepseek path must be /chat/completions");
    // HTTP 头名不区分大小写，校验实际头值而非序列化时的大小写。
    assert!(request.split("\r\n\r\n").next().unwrap().lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("authorization") && value.trim() == "Bearer test-key"
        })
    }), "deepseek request must carry auth");
    assert!(request.contains("deepseek-flash"), "deepseek request must name the model");

    server.abort();
    fake.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn stream_start_echoes_without_key() {
    let files_root = temp_root("echo");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_deepseek(DeepSeekClient::new(DeepSeekConfig::disabled()));
    let (addr, server) = start_server(state).await;

    let (status, body) = post(
        addr,
        "/chat/stream/start",
        r#"{"stream_id":"s1","turn_id":"t1","text":"你好"}"#,
    )
    .await;
    assert!(status.contains("200"), "start status: {status} body: {body}");
    assert_eq!(stream_text(&body), "你好", "no key must keep the Mock echo");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn stream_start_returns_502_when_model_call_fails() {
    let (fake_addr, _seen, fake) = start_fake_deepseek(
        "401 Unauthorized",
        r#"{"error":{"message":"Incorrect API key","type":"authentication_error"}}"#,
    )
    .await;
    let files_root = temp_root("fail");
    let state = configured_state(files_root.clone(), fake_addr);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(
        addr,
        "/chat/stream/start",
        r#"{"stream_id":"s1","turn_id":"t1","text":"你好"}"#,
    )
    .await;
    assert!(status.contains("502"), "start status: {status} body: {body}");
    assert!(body.contains("deepseek request failed"), "error body: {body}");
    assert!(!body.contains("你好"), "a failed model call must never fall back to the echo");

    server.abort();
    fake.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn stream_start_returns_502_when_completion_truncated() {
    let (fake_addr, _seen, fake) = start_fake_deepseek(
        "200 OK",
        r#"{"choices":[{"message":{"role":"assistant","content":"partial"},"finish_reason":"length"}]}"#,
    )
    .await;
    let files_root = temp_root("truncated");
    let state = configured_state(files_root.clone(), fake_addr);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(
        addr,
        "/chat/stream/start",
        r#"{"stream_id":"s1","turn_id":"t1","text":"你好"}"#,
    )
    .await;
    assert!(status.contains("502"), "start status: {status} body: {body}");
    assert!(!body.contains("partial"), "a truncated completion must not pose as a complete answer");

    server.abort();
    fake.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn stream_start_returns_502_when_completion_empty() {
    let (fake_addr, _seen, fake) = start_fake_deepseek(
        "200 OK",
        r#"{"choices":[{"message":{"role":"assistant","content":"   "},"finish_reason":"stop"}]}"#,
    )
    .await;
    let files_root = temp_root("empty");
    let state = configured_state(files_root.clone(), fake_addr);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(
        addr,
        "/chat/stream/start",
        r#"{"stream_id":"s1","turn_id":"t1","text":"你好"}"#,
    )
    .await;
    assert!(status.contains("502"), "start status: {status} body: {body}");
    assert!(!body.contains("你好"), "an empty completion must not fall back to the echo");

    server.abort();
    fake.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}
