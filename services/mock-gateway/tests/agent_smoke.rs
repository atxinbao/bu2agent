//! Agent invocation smoke tests for the Mock Gateway Axum transport.
//!
//! Covers the Phase 3 approval loop on the server side: echo
//! needs no record, approval tools need an approved record,
//! pending/rejected/no-record calls are refused with 403,
//! unknown tools are refused with 400, and an approved write
//! actually lands in the file root and reads back.

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

async fn post(addr: std::net::SocketAddr, path: &str, json: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect test server");
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        json.len(),
        json
    );
    stream.write_all(request.as_bytes()).await.expect("write test request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read test response");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    (head.lines().next().unwrap_or("").to_string(), body.to_string())
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!("mock-agent-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create temp root");
    root
}

#[tokio::test]
async fn agent_approval_loop_over_http() {
    let root = temp_root("smoke");
    let addr = start_server(root.clone()).await;
    let (status, body) = post(addr, "/agent/tool/call", "{\"tool\":\"echo\",\"args\":{\"text\":\"hi\"},\"tool_call_id\":null}").await;
    assert!(status.contains("200"), "echo status: {status} body: {body}");
    assert!(body.contains("\"result\":\"hi\""), "echo body: {body}");
    let (status, _) = post(
        addr,
        "/agent/tool/call",
        "{\"tool\":\"fs.write\",\"args\":{\"path\":\"agent.txt\",\"text\":\"from agent\"},\"tool_call_id\":null}",
    )
    .await;
    assert!(status.contains("403"), "no-record write status: {status}");
    let (status, body) = post(addr, "/approval/request", "{\"tool\":\"fs.write\"}").await;
    assert!(status.contains("200"), "request status: {status}");
    let request: serde_json::Value = serde_json::from_str(&body).expect("request json");
    let id = request["tool_call_id"].as_str().expect("tool_call_id").to_string();
    assert_eq!(request["requires_approval"].as_bool(), Some(true));
    let call = format!("{{\"tool\":\"fs.write\",\"args\":{{\"path\":\"agent.txt\",\"text\":\"from agent\"}},\"tool_call_id\":\"{id}\"}}");
    let (status, _) = post(addr, "/agent/tool/call", &call).await;
    assert!(status.contains("403"), "pending write status: {status}");
    let decide = format!("{{\"tool_call_id\":\"{id}\",\"status\":\"rejected\"}}");
    let (status, _) = post(addr, "/approval/decide", &decide).await;
    assert!(status.contains("200"), "decide rejected status: {status}");
    let (status, body) = post(addr, "/agent/tool/call", &call).await;
    assert!(status.contains("403"), "rejected write status: {status}");
    assert!(body.contains("approval_rejected"), "rejected body: {body}");
    let (status, body) = post(addr, "/approval/request", "{\"tool\":\"fs.write\"}").await;
    assert!(status.contains("200"), "request2 status: {status}");
    let request: serde_json::Value = serde_json::from_str(&body).expect("request2 json");
    let id2 = request["tool_call_id"].as_str().expect("tool_call_id2").to_string();
    let decide = format!("{{\"tool_call_id\":\"{id2}\",\"status\":\"approved\"}}");
    let (status, _) = post(addr, "/approval/decide", &decide).await;
    assert!(status.contains("200"), "decide approved status: {status}");
    let call = format!("{{\"tool\":\"fs.write\",\"args\":{{\"path\":\"agent.txt\",\"text\":\"from agent\"}},\"tool_call_id\":\"{id2}\"}}");
    let (status, body) = post(addr, "/agent/tool/call", &call).await;
    assert!(status.contains("200"), "approved write status: {status} body: {body}");
    assert!(body.contains("\"size_bytes\":10"), "approved write body: {body}");
    let (status, body) = post(addr, "/agent/tool/call", "{\"tool\":\"fs.read\",\"args\":{\"path\":\"agent.txt\"},\"tool_call_id\":null}").await;
    assert!(status.contains("200"), "read status: {status}");
    assert!(body.contains("\"result\":\"from agent\""), "read body: {body}");
    let (status, _) = post(addr, "/approval/request", "{\"tool\":\"no.such.tool\"}").await;
    assert!(status.contains("400"), "unknown request status: {status}");
    let (status, _) = post(addr, "/agent/tool/call", "{\"tool\":\"no.such.tool\",\"args\":{},\"tool_call_id\":null}").await;
    assert!(status.contains("400"), "unknown call status: {status}");
    let _ = std::fs::remove_dir_all(&root);
}
