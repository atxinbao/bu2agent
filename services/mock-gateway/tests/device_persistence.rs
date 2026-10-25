//! Device action persistence test for the Mock Gateway (B-layer wiring).
//!
//! Mirrors the task persistence test: with a JSONL path wired, an
//! action record completed before a restart must still be listed
//! after it, and the id counter must continue past the largest
//! persisted id (next create is action-2, not action-1 again).
//! The JSONL file lives next to the file root, never inside it,
//! so the shell file test's empty-root assertion stays valid.

use mock_gateway::device_backend::{FakeReminderBackend, Reminder};
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

fn seeded_fake() -> Arc<FakeReminderBackend> {
    let fake = FakeReminderBackend::new();
    fake.add_reminder(Reminder {
        id: "reminder-1".to_string(),
        title: "Test reminder 1".to_string(),
        completed: false,
    });
    Arc::new(fake)
}

#[tokio::test]
async fn device_action_survives_server_restart() {
    let base = std::env::temp_dir().join(format!("mock-device-persist-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let files_root = base.join("files");
    std::fs::create_dir_all(&files_root).expect("create files root");
    let actions_path = base.join("actions.jsonl");

    let fake = seeded_fake();
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_actions_path(actions_path.clone())
        .with_device_backend(fake.clone());
    let (addr, server) = start_server(state).await;
    let (status, body) = post(
        addr,
        "/devices/actions/create",
        r#"{"reminder_id":"reminder-1","target_completed":true}"#,
    )
    .await;
    assert!(status.contains("200"), "create status: {status} body: {body}");
    let (status, body) =
        post(addr, "/approval/request", r#"{"tool":"reminders.set_completed"}"#).await;
    assert!(status.contains("200"), "request: {status} {body}");
    let approval =
        serde_json::from_str::<serde_json::Value>(&body).expect("approval json")["tool_call_id"]
            .as_str()
            .expect("tool_call_id")
            .to_string();
    let (status, body) = post(
        addr,
        "/approval/decide",
        &format!(r#"{{"tool_call_id":"{approval}","status":"approved"}}"#),
    )
    .await;
    assert!(status.contains("200"), "decide: {status} {body}");
    let (status, body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{approval}"}}"#),
    )
    .await;
    assert!(status.contains("200"), "set status: {status} body: {body}");
    server.abort();

    let persisted =
        std::fs::read_to_string(&actions_path).expect("actions jsonl must exist after restart");
    assert!(persisted.contains("action-1"), "jsonl must hold action-1");
    assert!(persisted.contains("completed"), "jsonl must hold the completed outcome");

    // Restart with the same paths: the record must reload and
    // the next id must continue, not restart at action-1.
    let fake = seeded_fake();
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_actions_path(actions_path.clone())
        .with_device_backend(fake.clone());
    let (addr, server) = start_server(state).await;
    let (status, body) = get(addr, "/devices/actions/list").await;
    assert!(status.contains("200"), "list status: {status} body: {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("list json");
    let items = listed.as_array().expect("list array");
    assert_eq!(items.len(), 1, "restart must reload exactly one action");
    assert_eq!(items[0]["action_id"], "action-1");
    assert_eq!(items[0]["status"], "completed");
    assert_eq!(items[0]["reminder_id"], "reminder-1");

    let (status, body) = post(
        addr,
        "/devices/actions/create",
        r#"{"reminder_id":"reminder-1","target_completed":false}"#,
    )
    .await;
    assert!(status.contains("200"), "second create status: {status} body: {body}");
    let created: serde_json::Value = serde_json::from_str(&body).expect("create json");
    assert_eq!(created["action_id"], "action-2", "id counter must continue past action-1");

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}
