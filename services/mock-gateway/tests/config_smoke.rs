//! Smoke tests for Task 14 config surface (GET only):
//! GET /config reports the gateway's own files_root,
//! listen address, and provider. PATCH is deliberately
//! unimplemented (the gateway has no runtime-mutable
//! self-owned config to write); Axum answers it with 405,
//! which this test asserts so the exclusion stays visible.

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

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-config-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn config_get_and_patch_405() {
    let root = temp_root("smoke");
    let state = AppState::with_files_root_and_approvals(root.clone(), None);
    let (addr, handle) = start_server(state).await;

    let (head, body) = raw(addr, "GET /config HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await;
    assert!(head.contains("200"), "{head}");
    let view: serde_json::Value = serde_json::from_str(&body).expect("GET /config body is JSON");
    assert_eq!(view["files_root"], root.display().to_string());
    assert_eq!(view["provider"], "deepseek");
    // gateway_addr follows MOCK_GATEWAY_ADDR or the
    // 127.0.0.1:3001 default; only non-emptiness is
    // asserted so the test does not depend on test env.
    assert!(!view["gateway_addr"].as_str().unwrap_or("").is_empty());

    // PATCH is out of scope by design: 405, not a silent
    // no-op and not an invented write.
    let (head, _) = raw(
        addr,
        "PATCH /config HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
    )
    .await;
    assert!(head.contains("405"), "{head}");
    handle.abort();
}
