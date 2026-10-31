//! Smoke tests for Task 16 spaces v2 record subset:
//! list, save (upsert), delete (plain and
//! ?source=saved_space), tombstones, and restart
//! recovery through tagged JSONL.
//!
//! Covers the implemented v2 routes: GET /spaces/v2,
//! POST /spaces/v2/{slug}/save, DELETE /spaces/v2/{slug}.
//! Share, debug, export_html, CVM, and subscribe are
//! excluded by design.

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
    let dir = std::env::temp_dir().join(format!("mock-spaces-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn spaces_save_list_delete_and_restart() {
    let path = temp_root("smoke").join("spaces.jsonl");
    let state = AppState::default().with_space_state_path(path.clone());
    let (addr, handle) = start_server(state).await;

    let (status, spaces) = call(addr, "GET", "/spaces/v2", "").await;
    assert_eq!(status, 200);
    assert_eq!(spaces.as_array().unwrap().len(), 0);

    // Save creates; a second save upserts (title and
    // items replaced, same slug).
    let (status, space) = call(
        addr,
        "POST",
        "/spaces/v2/demo/save",
        r#"{"title":"Demo","items":[{"kind":"file","ref":"notes/a.md"},{"kind":"session","ref":"sess-1"}]}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(space["slug"], "demo");
    assert_eq!(space["title"], "Demo");
    assert_eq!(space["items"].as_array().unwrap().len(), 2);
    let (status, space) = call(
        addr,
        "POST",
        "/spaces/v2/demo/save",
        r#"{"title":"Demo 2","items":[{"kind":"file","ref":"notes/b.md"}]}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(space["title"], "Demo 2");
    assert_eq!(space["items"].as_array().unwrap().len(), 1);
    assert_eq!(space["items"][0]["ref"], "notes/b.md");

    // Invalid items are rejected without touching the record.
    let (status, _) = call(
        addr,
        "POST",
        "/spaces/v2/demo/save",
        r#"{"items":[{"kind":"peer","ref":"x"}]}"#,
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = call(
        addr,
        "POST",
        "/spaces/v2/demo/save",
        r#"{"items":[{"kind":"file","ref":"  "}]}"#,
    )
    .await;
    assert_eq!(status, 400);
    let (status, spaces) = call(addr, "GET", "/spaces/v2", "").await;
    assert_eq!(status, 200);
    assert_eq!(spaces.as_array().unwrap().len(), 1);
    assert_eq!(spaces[0]["title"], "Demo 2");

    // The ?source=saved_space delete variant has the same
    // record effect; delete is idempotent; unknown is 404.
    let (status, space) = call(addr, "DELETE", "/spaces/v2/demo?source=saved_space", "").await;
    assert_eq!(status, 200);
    assert_eq!(space["deleted"], true);
    let (status, spaces) = call(addr, "GET", "/spaces/v2", "").await;
    assert_eq!(status, 200);
    assert_eq!(spaces.as_array().unwrap().len(), 0);
    let (status, _) = call(addr, "DELETE", "/spaces/v2/demo", "").await;
    assert_eq!(status, 200);
    let (status, _) = call(addr, "DELETE", "/spaces/v2/nope", "").await;
    assert_eq!(status, 404);

    // Saving a deleted slug resurrects it as a fresh record.
    let (status, space) = call(addr, "POST", "/spaces/v2/demo/save", r#"{"title":"Back"}"#).await;
    assert_eq!(status, 200);
    assert_eq!(space["deleted"], false);
    assert_eq!(space["title"], "Back");

    // Restart recovery keeps the latest record.
    handle.abort();
    let state = AppState::default().with_space_state_path(path);
    let (addr, handle) = start_server(state).await;
    let (status, spaces) = call(addr, "GET", "/spaces/v2", "").await;
    assert_eq!(status, 200);
    assert_eq!(spaces.as_array().unwrap().len(), 1);
    assert_eq!(spaces[0]["slug"], "demo");
    assert_eq!(spaces[0]["title"], "Back");
    handle.abort();
}
