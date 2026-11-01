//! Smoke tests for Task 19 node record subset: register,
//! list (with status filter), heartbeat (including
//! reactivation after disconnect), disconnect, and unpair
//! (tombstone), plus restart recovery through nodes.jsonl.
//!
//! Invoke-ack, invoke-result, and sync are excluded by
//! design (see the Task 19 plan).

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

async fn call(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> (u16, serde_json::Value) {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let (head, resp_body) = raw(addr, &request).await;
    let status = head.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    let value: serde_json::Value = serde_json::from_str(&resp_body).unwrap_or(serde_json::Value::Null);
    (status, value)
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-node-{name}-{ }", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn node_registry_flow_and_restart() {
    let dir = temp_root("smoke");
    let node_path = dir.join("nodes.jsonl");
    let state = AppState::default().with_node_state_path(node_path.clone());
    let (addr, handle) = start_server(state).await;

    // Register: named and unnamed (name defaults to id).
    let (status, first) = call(addr, "POST", "/api/nodes/register", r#"{"name":"alpha"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(first["node_id"], "node-1");
    assert_eq!(first["name"], "alpha");
    assert_eq!(first["status"], "online");
    let (status, second) = call(addr, "POST", "/api/nodes/register", r#"{}"#).await;
    assert_eq!(status, 200);
    assert_eq!(second["node_id"], "node-2");
    assert_eq!(second["name"], "node-2");

    // List: both nodes, in number order; status filter works.
    let (status, listed) = call(addr, "POST", "/api/nodes/list", r#"{}"#).await;
    assert_eq!(status, 200);
    assert_eq!(listed.as_array().unwrap().len(), 2);
    assert_eq!(listed[0]["node_id"], "node-1");
    let (status, listed) =
        call(addr, "POST", "/api/nodes/list", r#"{"status":"disconnected"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(listed.as_array().unwrap().len(), 0);
    let (status, _) = call(addr, "POST", "/api/nodes/list", r#"{"status":"bogus"}"#).await;
    assert_eq!(status, 400);

    // Disconnect then heartbeat reactivates to online.
    let (status, gone) =
        call(addr, "POST", "/api/nodes/disconnect", r#"{"node_id":"node-1"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(gone["status"], "disconnected");
    let (status, listed) =
        call(addr, "POST", "/api/nodes/list", r#"{"status":"disconnected"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    let (status, back) =
        call(addr, "POST", "/api/nodes/heartbeat", r#"{"node_id":"node-1"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(back["status"], "online");
    assert!(back["last_heartbeat_ms"].as_u64().unwrap() > 0);

    // Unknown node reads as 404 on every mutation.
    for path in ["/api/nodes/heartbeat", "/api/nodes/disconnect", "/api/nodes/unpair"] {
        let (status, _) = call(addr, "POST", path, r#"{"node_id":"node-99"}"#).await;
        assert_eq!(status, 404);
    }

    // Unpair: tombstone hidden from list, mutations 404,
    // re-unpair idempotent success.
    let (status, unpaired) =
        call(addr, "POST", "/api/nodes/unpair", r#"{"node_id":"node-2"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(unpaired["unpaired"], true);
    let (status, listed) = call(addr, "POST", "/api/nodes/list", r#"{}"#).await;
    assert_eq!(status, 200);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["node_id"], "node-1");
    let (status, _) = call(addr, "POST", "/api/nodes/heartbeat", r#"{"node_id":"node-2"}"#).await;
    assert_eq!(status, 404);
    let (status, again) = call(addr, "POST", "/api/nodes/unpair", r#"{"node_id":"node-2"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(again["unpaired"], true);

    handle.abort();

    // Restart: records and numbering reload; node-3 is next.
    let state = AppState::default().with_node_state_path(node_path);
    let (addr, handle) = start_server(state).await;
    let (status, listed) = call(addr, "POST", "/api/nodes/list", r#"{}"#).await;
    assert_eq!(status, 200);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["node_id"], "node-1");
    let (status, third) = call(addr, "POST", "/api/nodes/register", r#"{"name":"gamma"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(third["node_id"], "node-3");
    handle.abort();
}
