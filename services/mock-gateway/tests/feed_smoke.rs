//! Smoke tests for Task 17 feed unit subset: seed
//! list, get, reaction, tap counting, bulk seen with
//! unknown reporting, delete tombstone, status counts,
//! and restart recovery through tagged JSONL overlays.
//!
//! Covers the implemented unit routes: GET /api/feed,
//! GET /api/feed/infinite, GET /api/feed/status,
//! GET/DELETE /api/feed/units/{unit_id},
//! POST /api/feed/units/{unit_id}/reaction|tap,
//! POST /api/feed/units/seen. Prompt, regenerate,
//! reorder, search, social-source-icon, and media are
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
    let dir = std::env::temp_dir().join(format!("mock-feed-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn feed_seeds_react_tap_seen_delete_and_restart() {
    let path = temp_root("smoke").join("feed.jsonl");
    let state = AppState::default().with_feed_state_path(path.clone());
    let (addr, handle) = start_server(state).await;

    // Three clearly-labeled Mock seeds, same via infinite.
    let (status, units) = call(addr, "GET", "/api/feed", "").await;
    assert_eq!(status, 200);
    assert_eq!(units.as_array().unwrap().len(), 3);
    assert!(units.as_array().unwrap().iter().all(|u| u["title"].as_str().unwrap().starts_with("[Mock]")));
    let (status, infinite) = call(addr, "GET", "/api/feed/infinite", "").await;
    assert_eq!(status, 200);
    assert_eq!(infinite, units);

    // Get, react, tap twice (event counting).
    let (status, unit) = call(addr, "GET", "/api/feed/units/feed-seed-1", "").await;
    assert_eq!(status, 200);
    assert_eq!(unit["seen"], false);
    let (status, unit) = call(
        addr,
        "POST",
        "/api/feed/units/feed-seed-1/reaction",
        r#"{"reaction":"like"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(unit["reaction"], "like");
    let (status, _) = call(addr, "POST", "/api/feed/units/feed-seed-1/reaction", r#"{"reaction":"  "}"#).await;
    assert_eq!(status, 400);
    let (status, unit) = call(addr, "POST", "/api/feed/units/feed-seed-1/tap", "").await;
    assert_eq!(status, 200);
    assert_eq!(unit["tap_count"], 1);
    let (status, unit) = call(addr, "POST", "/api/feed/units/feed-seed-1/tap", "").await;
    assert_eq!(status, 200);
    assert_eq!(unit["tap_count"], 2);

    // Bulk seen reports both marked and unknown ids.
    let (status, seen) = call(
        addr,
        "POST",
        "/api/feed/units/seen",
        r#"{"unit_ids":["feed-seed-1","feed-seed-2","feed-nope"]}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(seen["seen"].as_array().unwrap().len(), 2);
    assert_eq!(seen["unknown"].as_array().unwrap().len(), 1);
    assert_eq!(seen["unknown"][0], "feed-nope");

    // Status counts reflect the mutations.
    let (status, summary) = call(addr, "GET", "/api/feed/status", "").await;
    assert_eq!(status, 200);
    assert_eq!(summary["units"], 3);
    assert_eq!(summary["unseen"], 1);

    // Delete hides from the list; get still shows the
    // tombstone; unknown is 404.
    let (status, unit) = call(addr, "DELETE", "/api/feed/units/feed-seed-3", "").await;
    assert_eq!(status, 200);
    assert_eq!(unit["deleted"], true);
    let (status, units) = call(addr, "GET", "/api/feed", "").await;
    assert_eq!(status, 200);
    assert_eq!(units.as_array().unwrap().len(), 2);
    let (status, unit) = call(addr, "GET", "/api/feed/units/feed-seed-3", "").await;
    assert_eq!(status, 200);
    assert_eq!(unit["deleted"], true);
    let (status, _) = call(addr, "GET", "/api/feed/units/feed-nope", "").await;
    assert_eq!(status, 404);

    // Restart recovery: overlays re-apply over the seeds.
    handle.abort();
    let state = AppState::default().with_feed_state_path(path);
    let (addr, handle) = start_server(state).await;
    let (status, units) = call(addr, "GET", "/api/feed", "").await;
    assert_eq!(status, 200);
    assert_eq!(units.as_array().unwrap().len(), 2);
    let (status, unit) = call(addr, "GET", "/api/feed/units/feed-seed-1", "").await;
    assert_eq!(status, 200);
    assert_eq!(unit["reaction"], "like");
    assert_eq!(unit["tap_count"], 2);
    assert_eq!(unit["seen"], true);
    handle.abort();
}
