//! Smoke tests for Task 22 email read surface, batch 1
//! (option A): mailbox, mailbox/setup, threads list/get,
//! message get, attachment download. The mock source
//! stands in for Mail.app on this Linux VM; the packaged
//! Mac app uses the AppleScript source instead.

use mock_gateway::mail_source::{MockMailSource, UnavailableMailSource};
use mock_gateway::server::{app, AppState};
use std::sync::Arc;
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

async fn raw(addr: std::net::SocketAddr, request: &str) -> (String, Vec<u8>) {
    let mut stream = TcpStream::connect(addr).await.expect("connect test server");
    stream.write_all(request.as_bytes()).await.expect("write test request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read test response");
    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| pos + 4)
        .unwrap_or(buf.len());
    let (head, body) = buf.split_at(split);
    (String::from_utf8_lossy(head).to_string(), body.to_vec())
}

async fn call(addr: std::net::SocketAddr, method: &str, path: &str) -> (u16, serde_json::Value) {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n"
    );
    let (head, body) = raw(addr, &request).await;
    let status: u16 = head
        .lines()
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status code parses");
    (status, serde_json::from_slice(&body).expect("response body parses"))
}

async fn call_bytes(addr: std::net::SocketAddr, method: &str, path: &str) -> (u16, String, Vec<u8>) {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n"
    );
    let (head, body) = raw(addr, &request).await;
    let status: u16 = head
        .lines()
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status code parses");
    (status, head, body)
}

#[tokio::test]
async fn email_smoke() {
    let state = AppState::default().with_mail_source(Arc::new(MockMailSource));
    let (addr, handle) = start_server(state).await;

    let (status, mailbox) = call(addr, "GET", "/api/email/mailbox").await;
    assert_eq!(status, 200);
    assert_eq!(mailbox["configured"], true);
    assert_eq!(mailbox["source"], "mock");

    let (status, setup) = call(addr, "GET", "/api/email/mailbox/setup").await;
    assert_eq!(status, 200);
    assert_eq!(setup["configured"], true);

    let (status, threads) = call(addr, "GET", "/api/email/threads").await;
    assert_eq!(status, 200);
    let threads = threads.as_array().expect("threads array");
    assert_eq!(threads.len(), 2);
    assert_eq!(threads[0]["message_count"], 2);
    let thread_id = threads[0]["thread_id"].as_str().expect("thread id").to_string();

    let (status, page) = call(addr, "GET", "/api/email/threads?limit=1&offset=1").await;
    assert_eq!(status, 200);
    assert_eq!(page.as_array().expect("page array").len(), 1);

    let (status, thread) = call(addr, "GET", &format!("/api/email/threads/{thread_id}")).await;
    assert_eq!(status, 200);
    assert_eq!(thread["message_ids"].as_array().expect("message ids").len(), 2);
    let (status, _) = call(addr, "GET", "/api/email/threads/no-such-thread").await;
    assert_eq!(status, 404);

    let (status, message) = call(addr, "GET", "/api/email/messages/msg-seed-2").await;
    assert_eq!(status, 200);
    assert_eq!(message["subject"], "Re: Mock Thread Alpha");
    assert_eq!(message["attachments"].as_array().expect("attachments").len(), 1);
    let (status, _) = call(addr, "GET", "/api/email/messages/no-such-message").await;
    assert_eq!(status, 404);

    let (status, head, body) =
        call_bytes(addr, "GET", "/api/email/messages/msg-seed-2/attachments/0").await;
    assert_eq!(status, 200);
    assert!(head.contains("text/plain"));
    assert!(head.contains("notes.txt"));
    assert_eq!(body, b"hello attachment\n");
    let (status, _) = call(addr, "GET", "/api/email/messages/msg-seed-2/attachments/5").await;
    assert_eq!(status, 404);
    let (status, _) = call(addr, "GET", "/api/email/messages/msg-seed-1/attachments/0").await;
    assert_eq!(status, 404);

    handle.abort();
}

#[tokio::test]
async fn email_unavailable_smoke() {
    // The unavailable source is injected explicitly:
    // AppState::default() goes through from_deepseek_config,
    // which installs the AppleScript source on macOS (and
    // that source answers 502 when Mail is not running),
    // so a bare Default would assert 501 on Linux/Windows
    // but 502 on macOS. Explicit injection keeps this test
    // platform-independent: every email route answers 501,
    // never fake data.
    let state = AppState::default().with_mail_source(Arc::new(UnavailableMailSource));
    let (addr, handle) = start_server(state).await;
    let (status, body) = call(addr, "GET", "/api/email/mailbox").await;
    assert_eq!(status, 501);
    assert!(body["error"].as_str().unwrap_or("").contains("unavailable"));
    let (status, _) = call(addr, "GET", "/api/email/threads").await;
    assert_eq!(status, 501);
    handle.abort();
}
