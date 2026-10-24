//! HTTP smoke tests for the Mock Gateway Axum transport.
//!
//! These tests do not re-implement the state machine: they check
//! that the HTTP layer faithfully exposes the rules in `lib.rs`
//! (history, stream sequences 1..=4, dedup, cancel, approval gate)
//! and that unknown streams / statuses are rejected, not invented.
//!
//! The server is started on an ephemeral localhost port and spoken
//! to with raw HTTP over a TcpStream, so no extra HTTP client
//! dependency is needed.

use mock_gateway::server::{app, new_state};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn start_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
    let addr = listener.local_addr().expect("test listener addr");
    tokio::spawn(async move {
        axum::serve(listener, app(new_state())).await.expect("serve test server");
    });
    addr
}

/// Send one raw HTTP request with `Connection: close` and return
/// (status line, body). The server closes the connection after
/// the response, so reading to EOF is safe here.
async fn raw(addr: std::net::SocketAddr, request: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect test server");
    stream.write_all(request.as_bytes()).await.expect("write test request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read test response");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head.lines().next().unwrap_or("").to_string();
    (status, body.to_string())
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

#[tokio::test]
async fn health_and_history_over_http() {
    let addr = start_server().await;
    let (status, body) = get(addr, "/health").await;
    assert!(status.contains("200"), "health status: {status}");
    assert!(body.contains("\"status\":\"ok\""), "health body: {body}");

    let (status, body) = get(addr, "/chat/history").await;
    assert!(status.contains("200"), "history status: {status}");
    let history: serde_json::Value = serde_json::from_str(&body).expect("history json");
    let history = history.as_array().expect("history array");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0]["role"], "user");
    assert_eq!(history[1]["role"], "assistant");
    assert_eq!(history[0]["turn_id"], history[1]["turn_id"]);
}

#[tokio::test]
async fn stream_start_apply_cancel_and_approval_over_http() {
    let addr = start_server().await;

    // Start: exactly 4 records, sequences 1..=4.
    let (status, body) = post(
        addr,
        "/chat/stream/start",
        r#"{"stream_id":"stream-1","turn_id":"turn-1","text":"hi"}"#,
    )
    .await;
    assert!(status.contains("200"), "start status: {status}");
    let records: serde_json::Value = serde_json::from_str(&body).expect("start json");
    let records = records.as_array().expect("start array");
    assert_eq!(records.len(), 4);
    let seqs: Vec<u64> = records
        .iter()
        .map(|r| r["sequence"].as_u64().expect("sequence"))
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4]);

    // Apply once: all kept, last = 4. Apply again: nothing kept.
    let apply_body = serde_json::json!({ "stream_id": "stream-1", "records": records }).to_string();
    let (status, body) = post(addr, "/chat/stream/apply", &apply_body).await;
    assert!(status.contains("200"), "apply status: {status}");
    let applied: serde_json::Value = serde_json::from_str(&body).expect("apply json");
    assert_eq!(applied["kept"].as_array().expect("kept").len(), 4);
    assert_eq!(applied["last_applied_sequence"], 4);
    let (status, body) = post(addr, "/chat/stream/apply", &apply_body).await;
    assert!(status.contains("200"), "re-apply status: {status}");
    let applied: serde_json::Value = serde_json::from_str(&body).expect("re-apply json");
    assert!(applied["kept"].as_array().expect("kept").is_empty());
    assert_eq!(applied["last_applied_sequence"], 4);

    // Cancel: later records are not applied.
    let (_, _) = post(
        addr,
        "/chat/stream/start",
        r#"{"stream_id":"stream-2","turn_id":"turn-2","text":"hi"}"#,
    )
    .await;
    let (status, body) = post(addr, "/chat/stream/cancel", r#"{"stream_id":"stream-2"}"#).await;
    assert!(status.contains("200"), "cancel status: {status}");
    assert!(body.contains("\"cancelled\":true"), "cancel body: {body}");
    let apply_body2 = serde_json::json!({ "stream_id": "stream-2", "records": records }).to_string();
    let (_, body) = post(addr, "/chat/stream/apply", &apply_body2).await;
    let applied: serde_json::Value = serde_json::from_str(&body).expect("cancelled apply json");
    assert!(applied["kept"].as_array().expect("kept").is_empty());
    assert_eq!(applied["last_applied_sequence"], 0);

    // Unknown stream and unknown status are rejected, not invented.
    let (status, _) = post(
        addr,
        "/chat/stream/apply",
        r#"{"stream_id":"no-such-stream","records":[]}"#,
    )
    .await;
    assert!(status.contains("404"), "unknown stream status: {status}");
    let (status, _) = post(
        addr,
        "/approval/check",
        r#"{"status":"whatever","sensitive":true}"#,
    )
    .await;
    assert!(status.contains("400"), "unknown status status: {status}");

    // Approval gate over HTTP agrees with the pure-logic gate.
    let (_, body) = post(addr, "/approval/check", r#"{"status":"pending","sensitive":true}"#).await;
    assert!(body.contains("\"may_execute\":false"), "pending body: {body}");
    let (_, body) = post(addr, "/approval/check", r#"{"status":"approved","sensitive":true}"#).await;
    assert!(body.contains("\"may_execute\":true"), "approved body: {body}");
}
