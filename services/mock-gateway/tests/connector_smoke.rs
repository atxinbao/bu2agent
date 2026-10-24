//! Mock connector smoke tests for the Mock Gateway Axum transport.
//!
//! Covers Phase 4 on the server side: seeded fake notes/mail,
//! creates/sends refused without an approved record, approved
//! creates/sends landing and showing up in later lists, and
//! mail outside mock.local refused with egress_denied.

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
    raw(addr, &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")).await
}

async fn post(addr: std::net::SocketAddr, path: &str, json: &str) -> (String, String) {
    raw(addr, &format!("POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", json.len(), json)).await
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("mock-connector-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

#[tokio::test]
async fn mock_connectors_over_http() {
    let root = temp_root("smoke");
    let addr = start_server(root.clone()).await;
    let (status, body) = get(addr, "/connectors/notes/list").await;
    assert!(status.contains("200"), "notes list status: {status}");
    let notes: serde_json::Value = serde_json::from_str(&body).expect("notes json");
    assert_eq!(notes.as_array().expect("notes array").len(), 2);
    let (status, body) = get(addr, "/connectors/mail/list").await;
    assert!(status.contains("200"), "mail list status: {status}");
    let mail: serde_json::Value = serde_json::from_str(&body).expect("mail json");
    assert_eq!(mail.as_array().expect("mail array").len(), 2);
    let (status, _) = post(addr, "/connectors/notes/create", "{\"tool_call_id\":null,\"title\":\"T\",\"body\":\"B\"}").await;
    assert!(status.contains("403"), "no-record create status: {status}");
    let (status, body) = post(addr, "/approval/request", "{\"tool\":\"notes.create\"}").await;
    assert!(status.contains("200"), "request status: {status}");
    let request: serde_json::Value = serde_json::from_str(&body).expect("request json");
    let id = request["tool_call_id"].as_str().expect("tool_call_id").to_string();
    let decide = format!("{{\"tool_call_id\":\"{id}\",\"status\":\"approved\"}}");
    let (status, _) = post(addr, "/approval/decide", &decide).await;
    assert!(status.contains("200"), "decide status: {status}");
    let create = format!("{{\"tool_call_id\":\"{id}\",\"title\":\"Mock created\",\"body\":\"created in this run\"}}");
    let (status, body) = post(addr, "/connectors/notes/create", &create).await;
    assert!(status.contains("200"), "approved create status: {status} body: {body}");
    assert!(body.contains("\"id\":\"note-3\""), "created note body: {body}");
    let (status, body) = get(addr, "/connectors/notes/list").await;
    assert!(status.contains("200"), "notes list2 status: {status}");
    let notes: serde_json::Value = serde_json::from_str(&body).expect("notes2 json");
    assert_eq!(notes.as_array().expect("notes2 array").len(), 3);
    let (status, body) = post(addr, "/approval/request", "{\"tool\":\"mail.send\"}").await;
    assert!(status.contains("200"), "mail request status: {status}");
    let request: serde_json::Value = serde_json::from_str(&body).expect("mail request json");
    let mail_id = request["tool_call_id"].as_str().expect("mail tool_call_id").to_string();
    let decide = format!("{{\"tool_call_id\":\"{mail_id}\",\"status\":\"approved\"}}");
    let (status, _) = post(addr, "/approval/decide", &decide).await;
    assert!(status.contains("200"), "mail decide status: {status}");
    let send = format!("{{\"tool_call_id\":\"{mail_id}\",\"to\":\"a@example.com\",\"subject\":\"S\",\"body\":\"B\"}}");
    let (status, body) = post(addr, "/connectors/mail/send", &send).await;
    assert!(status.contains("403"), "egress send status: {status}");
    assert!(body.contains("egress_denied"), "egress body: {body}");
    let send = format!("{{\"tool_call_id\":\"{mail_id}\",\"to\":\"a@mock.local\",\"subject\":\"Mock hello\",\"body\":\"sent in this run\"}}");
    let (status, body) = post(addr, "/connectors/mail/send", &send).await;
    assert!(status.contains("200"), "approved send status: {status} body: {body}");
    assert!(body.contains("\"id\":\"mail-3\""), "sent mail body: {body}");
    let (status, body) = get(addr, "/connectors/mail/list").await;
    assert!(status.contains("200"), "mail list2 status: {status}");
    let mail: serde_json::Value = serde_json::from_str(&body).expect("mail2 json");
    assert_eq!(mail.as_array().expect("mail2 array").len(), 3);
    let _ = std::fs::remove_dir_all(&root);
}
