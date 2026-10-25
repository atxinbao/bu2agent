//! Task persistence test for the Mock Gateway (task loop).
//!
//! Mirrors the approval persistence test: with a JSONL path
//! wired, a task record created before a restart must still be
//! listed after it, and the id counter must continue past the
//! largest persisted id (next create is task-2, not task-1
//! again). The JSONL file lives next to the file root, never
//! inside it, so the shell file test's empty-root assertion
//! stays valid.

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

async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    raw(
        addr,
        &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"),
    )
    .await
}

#[tokio::test]
async fn task_survives_server_restart() {
    let base = std::env::temp_dir().join(format!("mock-task-persist-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let files_root = base.join("files");
    std::fs::create_dir_all(&files_root).expect("create files root");
    let tasks_path = base.join("tasks.jsonl");

    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_tasks_path(tasks_path.clone());
    let (addr, server) = start_server(state).await;
    let (status, body) = post(addr, "/tasks/create", r#"{"directory":""}"#).await;
    assert!(status.contains("200"), "create status: {status} body: {body}");
    let (status, body) = post(
        addr,
        "/tasks/update",
        r#"{"task_id":"task-1","status":"completed","artifact_path":"report.md"}"#,
    )
    .await;
    assert!(status.contains("200"), "update status: {status} body: {body}");
    server.abort();

    // Restart with the same paths: the record must reload and
    // the next id must continue, not restart at task-1.
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_tasks_path(tasks_path.clone());
    let (addr, server) = start_server(state).await;
    let (status, body) = get(addr, "/tasks/list").await;
    assert!(status.contains("200"), "list status: {status} body: {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("list json");
    let items = listed.as_array().expect("list array");
    assert_eq!(items.len(), 1, "restart must reload exactly one task");
    assert_eq!(items[0]["task_id"], "task-1");
    assert_eq!(items[0]["status"], "completed");
    assert_eq!(items[0]["artifact_path"], "report.md");

    let (status, body) = post(addr, "/tasks/create", r#"{"directory":""}"#).await;
    assert!(status.contains("200"), "second create status: {status} body: {body}");
    let created: serde_json::Value = serde_json::from_str(&body).expect("create json");
    assert_eq!(created["task_id"], "task-2", "id counter must continue past task-1");

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}
