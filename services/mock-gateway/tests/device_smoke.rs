//! Device record tests for the Mock Gateway (B-layer wiring).
//!
//! The gateway gains device action records as new backend surface:
//! create an action for a reminder, approve it through the existing
//! approval loop, execute the set, and re-query unknown actions.
//! Approval gates are server-enforced (no record, pending, or
//! rejected all refuse execution), rejections record without any
//! device call, and the allowlist pin refuses other reminders.
//! Restart persistence lives in device_persistence.

use mock_gateway::device_backend::{FakeFault, FakeReminderBackend, Reminder};
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

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-device-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn action(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("action json")
}

fn seeded_fake() -> Arc<FakeReminderBackend> {
    let fake = FakeReminderBackend::new();
    fake.add_reminder(Reminder {
        id: "reminder-1".to_string(),
        title: "Test reminder 1".to_string(),
        completed: false,
    });
    fake.add_reminder(Reminder {
        id: "reminder-2".to_string(),
        title: "Test reminder 2".to_string(),
        completed: true,
    });
    Arc::new(fake)
}

fn state_with(root: std::path::PathBuf, fake: &Arc<FakeReminderBackend>) -> AppState {
    AppState::with_files_root_and_approvals(root, None).with_device_backend(fake.clone())
}

/// Request an approval for reminders.set_completed and decide it,
/// returning the tool_call_id.
async fn approve(addr: std::net::SocketAddr, decision: &str) -> String {
    let (status, body) =
        post(addr, "/approval/request", r#"{"tool":"reminders.set_completed"}"#).await;
    assert!(status.contains("200"), "approval request: {status} {body}");
    let id = action(&body)["tool_call_id"].as_str().expect("tool_call_id").to_string();
    let (status, body) = post(
        addr,
        "/approval/decide",
        &format!(r#"{{"tool_call_id":"{id}","status":"{decision}"}}"#),
    )
    .await;
    assert!(status.contains("200"), "approval decide: {status} {body}");
    id
}

async fn create_action(addr: std::net::SocketAddr, reminder_id: &str, target: bool) -> String {
    let (status, body) = post(
        addr,
        "/devices/actions/create",
        &format!(r#"{{"reminder_id":"{reminder_id}","target_completed":{target}}}"#),
    )
    .await;
    assert!(status.contains("200"), "create status: {status} body: {body}");
    action(&body)["action_id"].as_str().expect("action_id").to_string()
}

#[tokio::test]
async fn device_create_set_get_roundtrip() {
    let files_root = temp_root("roundtrip");
    let fake = seeded_fake();
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let (status, body) = get(addr, "/devices/reminders/list").await;
    assert!(status.contains("200"), "list status: {status} body: {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("list json");
    let items = listed.as_array().expect("list array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["id"], "reminder-1");
    assert_eq!(items[0]["completed"], false);

    let action_id = create_action(addr, "reminder-1", true).await;
    assert_eq!(action_id, "action-1");
    let (status, body) = get(addr, "/devices/actions/get?action_id=action-1").await;
    assert!(status.contains("200"), "get status: {status} body: {body}");
    assert_eq!(action(&body)["status"], "requested");

    let approval = approve(addr, "approved").await;
    let (status, body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{approval}"}}"#),
    )
    .await;
    assert!(status.contains("200"), "set status: {status} body: {body}");
    let done = action(&body);
    assert_eq!(done["status"], "completed");
    assert!(done["detail"].is_null());

    let (status, body) = get(addr, "/devices/reminders/list").await;
    assert!(status.contains("200"), "relist status: {status} body: {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("relist json");
    assert_eq!(listed[0]["completed"], true);

    // A second set on the same action is refused: each action
    // executes at most once.
    let (status, _body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{approval}"}}"#),
    )
    .await;
    assert!(status.contains("400"), "second set must be 400: {status}");

    let _ = create_action(addr, "reminder-2", false).await;
    let (status, body) = get(addr, "/devices/actions/list").await;
    assert!(status.contains("200"), "actions list: {status} body: {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("actions json");
    let items = listed.as_array().expect("actions array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["action_id"], "action-1");
    assert_eq!(items[1]["action_id"], "action-2");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_set_requires_approved_approval() {
    let files_root = temp_root("gates");
    let fake = seeded_fake();
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let _ = create_action(addr, "reminder-1", true).await;

    // No approval at all.
    let (status, body) = post(
        addr,
        "/devices/reminders/set",
        r#"{"action_id":"action-1"}"#,
    )
    .await;
    assert!(status.contains("403"), "no approval must be 403: {status} {body}");
    assert!(body.contains("approval_required"));

    // Pending approval (requested, not decided).
    let (status, body) =
        post(addr, "/approval/request", r#"{"tool":"reminders.set_completed"}"#).await;
    assert!(status.contains("200"), "request: {status} {body}");
    let pending = action(&body)["tool_call_id"].as_str().expect("id").to_string();
    let (status, body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{pending}"}}"#),
    )
    .await;
    assert!(status.contains("403"), "pending must be 403: {status} {body}");
    assert!(body.contains("approval_required"));

    let (status, body) = get(addr, "/devices/actions/get?action_id=action-1").await;
    assert!(status.contains("200"), "get: {status} {body}");
    assert_eq!(action(&body)["status"], "requested");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_set_rejected_approval_forbidden() {
    let files_root = temp_root("rejected-set");
    let fake = seeded_fake();
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let _ = create_action(addr, "reminder-1", true).await;
    let approval = approve(addr, "rejected").await;
    let (status, body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{approval}"}}"#),
    )
    .await;
    assert!(status.contains("403"), "rejected must be 403: {status} {body}");
    assert!(body.contains("approval_rejected"));

    let (status, body) = get(addr, "/devices/reminders/list").await;
    assert!(status.contains("200"), "list: {status} {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("list json");
    assert_eq!(listed[0]["completed"], false, "rejected set must not write");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_mark_rejected_records_without_device_call() {
    let files_root = temp_root("mark-rejected");
    let fake = seeded_fake();
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let _ = create_action(addr, "reminder-1", true).await;
    let approval = approve(addr, "rejected").await;
    let (status, body) = post(
        addr,
        "/devices/actions/mark_rejected",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{approval}"}}"#),
    )
    .await;
    assert!(status.contains("200"), "mark_rejected: {status} {body}");
    assert_eq!(action(&body)["status"], "rejected");

    let (status, body) = get(addr, "/devices/reminders/list").await;
    assert!(status.contains("200"), "list: {status} {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("list json");
    assert_eq!(listed[0]["completed"], false, "rejection must not touch the device");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_mark_rejected_requires_rejected_approval() {
    let files_root = temp_root("mark-gates");
    let fake = seeded_fake();
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let _ = create_action(addr, "reminder-1", true).await;
    let approved = approve(addr, "approved").await;
    let (status, body) = post(
        addr,
        "/devices/actions/mark_rejected",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{approved}"}}"#),
    )
    .await;
    assert!(status.contains("403"), "approved approval must be 403: {status} {body}");
    assert!(body.contains("approval_not_rejected"));

    let rejected = approve(addr, "rejected").await;
    let (status, _body) = post(
        addr,
        "/devices/actions/mark_rejected",
        &format!(r#"{{"action_id":"action-999","tool_call_id":"{rejected}"}}"#),
    )
    .await;
    assert!(status.contains("404"), "unknown action must be 404: {status}");

    let (status, _body) = post(
        addr,
        "/devices/actions/mark_rejected",
        r#"{"action_id":"action-1","tool_call_id":"approval-999"}"#,
    )
    .await;
    assert!(status.contains("404"), "unknown approval must be 404: {status}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_pin_refuses_other_reminder() {
    let files_root = temp_root("pin");
    let fake = seeded_fake();
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_device_backend(fake.clone())
        .with_device_reminder_pin("reminder-1".to_string());
    let (addr, server) = start_server(state).await;

    let (status, body) = post(
        addr,
        "/devices/actions/create",
        r#"{"reminder_id":"reminder-2","target_completed":true}"#,
    )
    .await;
    assert!(status.contains("403"), "pinned-out id must be 403: {status} {body}");
    assert!(body.contains("reminder_not_allowed"));

    let (status, body) = post(
        addr,
        "/devices/actions/create",
        r#"{"reminder_id":"reminder-1","target_completed":true}"#,
    )
    .await;
    assert!(status.contains("200"), "pinned id must pass: {status} {body}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_refresh_keeps_unknown_then_resolves() {
    let files_root = temp_root("refresh");
    let fake = seeded_fake();
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let _ = create_action(addr, "reminder-1", true).await;
    let approval = approve(addr, "approved").await;
    fake.set_fault(Some(FakeFault::Timeout));
    let (status, body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{approval}"}}"#),
    )
    .await;
    assert!(status.contains("200"), "timeout set: {status} {body}");
    assert_eq!(action(&body)["status"], "unknown");

    // Fault cleared, but the state is still old: refresh must keep
    // Unknown, not judge the write failed.
    fake.set_fault(None);
    let (status, body) = post(addr, "/devices/actions/refresh", r#"{"action_id":"action-1"}"#).await;
    assert!(status.contains("200"), "refresh: {status} {body}");
    let refreshed = action(&body);
    assert_eq!(refreshed["status"], "unknown");
    assert!(refreshed["detail"].as_str().unwrap_or("").contains("still be in flight"));

    // A later successful write lands the state; refreshing the old
    // Unknown action now resolves it to Completed.
    let _ = create_action(addr, "reminder-1", true).await;
    let approval2 = approve(addr, "approved").await;
    let (status, body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-2","tool_call_id":"{approval2}"}}"#),
    )
    .await;
    assert!(status.contains("200"), "second set: {status} {body}");
    assert_eq!(action(&body)["status"], "completed");

    let (status, body) = post(addr, "/devices/actions/refresh", r#"{"action_id":"action-1"}"#).await;
    assert!(status.contains("200"), "second refresh: {status} {body}");
    assert_eq!(action(&body)["status"], "completed");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_refresh_non_unknown_and_unknown_ids() {
    let files_root = temp_root("refresh-ids");
    let fake = seeded_fake();
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let _ = create_action(addr, "reminder-1", true).await;
    let approval = approve(addr, "approved").await;
    let (status, _body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-1","tool_call_id":"{approval}"}}"#),
    )
    .await;
    assert!(status.contains("200"), "set: {status}");

    let (status, body) = post(addr, "/devices/actions/refresh", r#"{"action_id":"action-1"}"#).await;
    assert!(status.contains("200"), "refresh completed: {status} {body}");
    assert_eq!(action(&body)["status"], "completed");

    let (status, _body) =
        post(addr, "/devices/actions/refresh", r#"{"action_id":"action-999"}"#).await;
    assert!(status.contains("404"), "unknown refresh must be 404: {status}");
    let (status, _body) = get(addr, "/devices/actions/get?action_id=action-999").await;
    assert!(status.contains("404"), "unknown get must be 404: {status}");

    let approval2 = approve(addr, "approved").await;
    let (status, _body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"action-999","tool_call_id":"{approval2}"}}"#),
    )
    .await;
    assert!(status.contains("404"), "unknown set must be 404: {status}");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_default_backend_lists_demo_reminder() {
    let files_root = temp_root("demo");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, body) = get(addr, "/devices/reminders/list").await;
    assert!(status.contains("200"), "list: {status} {body}");
    let listed: serde_json::Value = serde_json::from_str(&body).expect("list json");
    let items = listed.as_array().expect("list array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], "reminder-demo");
    assert_eq!(items[0]["completed"], false);

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_authorization_reports_fake_status() {
    // The Fake default auth is Authorized; the request endpoint is
    // a no-op for the Fake (no system permission to request).
    let files_root = temp_root("auth");
    let fake = seeded_fake();
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let (status, body) = get(addr, "/devices/authorization").await;
    assert!(status.contains("200"), "authorization: {status} {body}");
    assert_eq!(action(&body)["status"], "authorized");

    let (status, body) = post(addr, "/devices/authorization/request", "{}").await;
    assert!(status.contains("200"), "request: {status} {body}");
    assert_eq!(action(&body)["status"], "authorized");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn device_authorization_reports_denied() {
    use mock_gateway::device_backend::AuthorizationStatus;

    let files_root = temp_root("auth-denied");
    let fake = seeded_fake();
    fake.set_auth(AuthorizationStatus::Denied);
    let state = state_with(files_root.clone(), &fake);
    let (addr, server) = start_server(state).await;

    let (status, body) = get(addr, "/devices/authorization").await;
    assert!(status.contains("200"), "authorization: {status} {body}");
    assert_eq!(action(&body)["status"], "denied");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}
