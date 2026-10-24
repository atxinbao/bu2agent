//! ChatClient end-to-end test against a canned fake HTTP server.
//!
//! This test does not depend on the mock-gateway crate: a small
//! std TcpListener answers the exact endpoints the real Axum
//! transport serves, with the same snake_case shapes. It proves
//! the client side of the mainline (history, start, apply, cancel)
//! without pretending the fake server is the Mock Gateway itself.
//! The real server is covered by mock-gateway's http_smoke tests
//! and by the chat-cli demo run.

use agent_desktop::chat_client::{assemble_text, ChatClient};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn canned_response(path: &str, _body: &str) -> (u16, &'static str) {
    match path {
        "/health" => (200, r#"{"status":"ok"}"#),
        "/chat/history" => (
            200,
            r#"[{"turn_id":"turn-seed-001","role":"user","text":"hi"},{"turn_id":"turn-seed-001","role":"assistant","text":"hello"}]"#,
        ),
        "/chat/stream/start" => (
            200,
            r#"[{"stream_id":"stream-1","sequence":1,"event_type":"delta.turn_start","payload":"{\"turnId\":\"turn-1\"}","resume_cursor":"cursor-1"},{"stream_id":"stream-1","sequence":2,"event_type":"delta.text_append","payload":"{\"turnId\":\"turn-1\",\"text\":\"hi\"}","resume_cursor":"cursor-2"},{"stream_id":"stream-1","sequence":3,"event_type":"delta.agent_status","payload":"{\"turnId\":\"turn-1\",\"status\":\"completed\"}","resume_cursor":"cursor-3"},{"stream_id":"stream-1","sequence":4,"event_type":"delta.message_done","payload":"{\"turnId\":\"turn-1\"}","resume_cursor":null}]"#,
        ),
        "/chat/stream/apply" => (
            200,
            r#"{"kept":[{"stream_id":"stream-1","sequence":2,"event_type":"delta.text_append","payload":"{\"turnId\":\"turn-1\",\"text\":\"hi\"}","resume_cursor":"cursor-2"}],"last_applied_sequence":2}"#,
        ),
        "/chat/stream/cancel" => (200, r#"{"stream_id":"stream-1","cancelled":true}"#),
        _ => (404, r#"{"error":"unknown"}"#),
    }
}

fn start_fake_server(requests: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake server");
    let addr = listener.local_addr().expect("fake server addr");
    let served = Arc::new(AtomicUsize::new(0));
    let served_clone = Arc::clone(&served);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let mut buf = [0u8; 8192];
            let n = stream.read(&mut buf).unwrap_or(0);
            let text = String::from_utf8_lossy(&buf[..n]).into_owned();
            let path = text
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_string();
            let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
            let (status, resp_body) = canned_response(&path, &body);
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                resp_body.len(),
                resp_body
            );
            let _ = stream.write_all(response.as_bytes());
            if served_clone.fetch_add(1, Ordering::SeqCst) + 1 >= requests {
                break;
            }
        }
    });
    format!("http://{addr}")
}

#[test]
fn chat_client_history_start_apply_cancel() {
    // health + history + start + apply + cancel = 5 requests.
    let base_url = start_fake_server(5);
    let client = ChatClient::new(&base_url).expect("client");
    assert!(client.health().expect("health"));
    let history = client.history().expect("history");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].role, "user");
    let records = client
        .start_stream("stream-1", "turn-1", "hi")
        .expect("start");
    assert_eq!(records.len(), 4);
    assert_eq!(records[1].sequence, 2);
    let applied = client.apply_stream("stream-1", &records).expect("apply");
    assert_eq!(applied.last_applied_sequence, 2);
    assert_eq!(assemble_text(&applied.kept), "hi");
    assert!(client.cancel_stream("stream-1").expect("cancel"));
}
