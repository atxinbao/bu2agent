//! Task record tests for the Mock Gateway (task loop).
//!
//! The gateway gains only task records as new backend surface:
//! create a task for an existing directory, list tasks, get one
//! task, update its status (and, on completion, its artifact
//! path). Directory escapes are refused, unknown ids and
//! unknown statuses are refused, and ids continue across a
//! restart when the JSONL path is wired (see task_persistence).

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

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-task-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn task(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("task json")
}

#[tokio::test]
async fn task_create_list_get_update_roundtrip() {
    let files_root = temp_root("roundtrip");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/tasks/create", r#"{"directory":""}"#).await;
    assert!(status.contains("200"), "create status: {status} body: {body}");
    let created = task(&body);
    assert_eq!(created["task_id"], "task-1");
    assert_eq!(created["directory"], "");
    assert_eq!(created["status"], "created");
    assert!(created["artifact_path"].is_null());

    let (status, body) = get(addr, "/tasks/list").await;
    assert!(status.contains("200"), "list status: {status} body: {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("list json");
    assert_eq!(listed.as_array().expect("list array").len(), 1);
    assert_eq!(listed[0]["task_id"], "task-1");

    let (status, body) = get(addr, "/tasks/get?task_id=task-1").await;
    assert!(status.contains("200"), "get status: {status} body: {body}");
    assert_eq!(task(&body)["status"], "created");

    let (status, body) = post(
        addr,
        "/tasks/update",
        r#"{"task_id":"task-1","status":"draft_ready"}"#,
    )
    .await;
    assert!(status.contains("200"), "update status: {status} body: {body}");
    assert_eq!(task(&body)["status"], "draft_ready");

    let (status, body) = post(
        addr,
        "/tasks/update",
        r#"{"task_id":"task-1","status":"completed","artifact_path":"report.md"}"#,
    )
    .await;
    assert!(status.contains("200"), "complete status: {status} body: {body}");
    let done = task(&body);
    assert_eq!(done["status"], "completed");
    assert_eq!(done["artifact_path"], "report.md");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn task_create_rejects_missing_directory() {
    let files_root = temp_root("missing");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/tasks/create", r#"{"directory":"no-such-dir"}"#).await;
    assert!(status.contains("404"), "missing dir status: {status} body: {body}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn task_create_rejects_escape() {
    let files_root = temp_root("escape");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/tasks/create", r#"{"directory":"../evil"}"#).await;
    assert!(status.contains("400"), "escape status: {status} body: {body}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn task_update_rejects_unknown_status_and_task() {
    let files_root = temp_root("badupdate");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/tasks/create", r#"{"directory":""}"#).await;
    assert!(status.contains("200"), "create status: {status} body: {body}");

    let (status, _body) = post(
        addr,
        "/tasks/update",
        r#"{"task_id":"task-1","status":"bogus"}"#,
    )
    .await;
    assert!(status.contains("400"), "unknown status must be 400: {status}");

    let (status, _body) = post(
        addr,
        "/tasks/update",
        r#"{"task_id":"task-999","status":"completed"}"#,
    )
    .await;
    assert!(status.contains("404"), "unknown task must be 404: {status}");

    let (status, _body) = get(addr, "/tasks/get?task_id=task-999").await;
    assert!(status.contains("404"), "unknown get must be 404: {status}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}
