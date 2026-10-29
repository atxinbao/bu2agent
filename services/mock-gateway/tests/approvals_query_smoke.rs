//! Egress approval query surface smoke tests (Task 7).
//!
//! Covers the five plural routes end to end over raw HTTP:
//! list, history, detail, editable-message and edit-message,
//! including the local edit semantics (pending + unconsumed +
//! unbound only), the 404/400/409 paths, history-vs-current
//! divergence, and restart persistence of the edited state.
//! Every JSON request body is built with serde_json::json! so
//! no hand-assembled JSON can break on any platform.

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

async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    raw(addr, &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")).await
}

async fn post(addr: std::net::SocketAddr, path: &str, json: &serde_json::Value) -> (String, String) {
    let body = json.to_string();
    raw(
        addr,
        &format!(
            "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        ),
    )
    .await
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("response is json")
}

fn temp_paths(name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("mock-approvals-query-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let file = dir.join("approvals.jsonl");
    (dir, file)
}

async fn request_approval(
    addr: std::net::SocketAddr,
    body: serde_json::Value,
) -> String {
    let (status, body) = post(addr, "/approval/request", &body).await;
    assert!(status.contains("200"), "request status: {status} body: {body}");
    json(&body)["tool_call_id"].as_str().expect("tool_call_id string").to_string()
}

#[tokio::test]
async fn approvals_query_edit_and_history_flow() {
    let (files_root, approvals_path) = temp_paths("flow");
    let state =
        AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals_path.clone()));
    let (addr, server) = start_server(state).await;

    // An unbound approval (connector tool) carrying a message.
    let id = request_approval(
        addr,
        serde_json::json!({"tool": "mail.send", "message": "first draft"}),
    )
    .await;
    assert_eq!(id, "approval-1");

    // List: the record is there, with the view shape.
    let (status, body) = get(addr, "/approvals").await;
    assert!(status.contains("200"), "list status: {status}");
    let list = json(&body);
    assert_eq!(list.as_array().expect("list array").len(), 1);
    let view = &list[0];
    assert_eq!(view["approval_id"], "approval-1");
    assert_eq!(view["tool_call_id"], "approval-1");
    assert_eq!(view["tool"], "mail.send");
    assert_eq!(view["status"], "pending");
    assert_eq!(view["consumed"], false);
    assert_eq!(view["bound"], false);
    assert_eq!(view["binding"], serde_json::Value::Null);
    assert_eq!(view["operation_digest"], serde_json::Value::Null);
    assert_eq!(view["message"], "first draft");

    // Detail matches; unknown id is a 404.
    let (status, body) = get(addr, "/approvals/approval-1").await;
    assert!(status.contains("200"), "detail status: {status}");
    assert_eq!(json(&body)["status"], "pending");
    let (status, body) = get(addr, "/approvals/approval-999").await;
    assert!(status.contains("404"), "unknown detail status: {status}");
    assert_eq!(json(&body)["error"], "unknown approval_id");

    // Editable-message: the message is there and edits are allowed.
    let (status, body) = get(addr, "/approvals/approval-1/editable-message").await;
    assert!(status.contains("200"), "editable status: {status}");
    let editable = json(&body);
    assert_eq!(editable["approval_id"], "approval-1");
    assert_eq!(editable["message"], "first draft");
    assert_eq!(editable["editable"], true);

    // Edit succeeds: status becomes edited, message is replaced.
    let (status, body) = post(
        addr,
        "/approvals/approval-1/edit-message",
        &serde_json::json!({"message": "revised draft"}),
    )
    .await;
    assert!(status.contains("200"), "edit status: {status} body: {body}");
    let view = json(&body);
    assert_eq!(view["status"], "edited");
    assert_eq!(view["message"], "revised draft");

    // A second edit is a conflict: the record is no longer pending.
    let (status, body) = post(
        addr,
        "/approvals/approval-1/edit-message",
        &serde_json::json!({"message": "third draft"}),
    )
    .await;
    assert!(status.contains("409"), "second edit status: {status}");
    assert_eq!(json(&body)["error"], "approval_not_editable");

    // Empty message on a fresh pending approval is a 400 and
    // leaves the record untouched.
    let id2 = request_approval(addr, serde_json::json!({"tool": "notes.create"})).await;
    assert_eq!(id2, "approval-2");
    let (status, body) = get(addr, "/approvals/approval-2/editable-message").await;
    assert!(status.contains("200"), "editable 2 status: {status}");
    let editable = json(&body);
    assert_eq!(editable["message"], serde_json::Value::Null);
    assert_eq!(editable["editable"], true);
    let (status, _) = post(
        addr,
        "/approvals/approval-2/edit-message",
        &serde_json::json!({"message": "   "}),
    )
    .await;
    assert!(status.contains("400"), "empty edit status: {status}");
    let (status, body) = get(addr, "/approvals/approval-2").await;
    assert!(status.contains("200"), "detail 2 status: {status}");
    assert_eq!(json(&body)["status"], "pending");

    // A bound approval (fs.write) cannot be edited.
    let id3 = request_approval(
        addr,
        serde_json::json!({"tool": "fs.write", "path": "bound.txt", "content_base64": "aGVsbG8="}),
    )
    .await;
    assert_eq!(id3, "approval-3");
    let (status, body) = get(addr, "/approvals/approval-3").await;
    assert!(status.contains("200"), "bound detail status: {status}");
    let view = json(&body);
    assert_eq!(view["bound"], true);
    assert!(view["operation_digest"].is_string(), "bound digest: {view}");
    let (status, body) = get(addr, "/approvals/approval-3/editable-message").await;
    assert!(status.contains("200"), "bound editable status: {status}");
    assert_eq!(json(&body)["editable"], false);
    let (status, body) = post(
        addr,
        "/approvals/approval-3/edit-message",
        &serde_json::json!({"message": "sneaky rewrite"}),
    )
    .await;
    assert!(status.contains("409"), "bound edit status: {status}");
    assert_eq!(json(&body)["error"], "approval_not_editable");

    // History: every persisted event in file order. This also
    // proves /approvals/history is not captured as an approval id
    // (that would 404 on the unknown id "history").
    let (status, body) = get(addr, "/approvals/history").await;
    assert!(status.contains("200"), "history status: {status} body: {body}");
    let history = json(&body);
    let events = history.as_array().expect("history array");
    assert_eq!(events.len(), 4, "history events: {history}");
    assert_eq!(events[0]["approval_id"], "approval-1");
    assert_eq!(events[0]["status"], "pending");
    assert_eq!(events[0]["message"], "first draft");
    assert_eq!(events[1]["approval_id"], "approval-1");
    assert_eq!(events[1]["status"], "edited");
    assert_eq!(events[1]["message"], "revised draft");
    assert_eq!(events[2]["approval_id"], "approval-2");
    assert_eq!(events[2]["status"], "pending");
    assert_eq!(events[3]["approval_id"], "approval-3");
    assert_eq!(events[3]["status"], "pending");
    assert_eq!(events[3]["bound"], true);

    // The current list shows only the latest state per id.
    let (status, body) = get(addr, "/approvals").await;
    assert!(status.contains("200"), "list after edit status: {status}");
    let list = json(&body);
    let current = list.as_array().expect("list array");
    assert_eq!(current.len(), 3);
    assert_eq!(current[0]["approval_id"], "approval-1");
    assert_eq!(current[0]["status"], "edited");
    assert_eq!(current[1]["approval_id"], "approval-2");
    assert_eq!(current[2]["approval_id"], "approval-3");

    server.abort();

    // Restart on the same approvals file: the edited status and
    // message survive, and so does the full history.
    let state =
        AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals_path.clone()));
    let (addr, server) = start_server(state).await;

    let (status, body) = get(addr, "/approvals/approval-1").await;
    assert!(status.contains("200"), "reloaded detail status: {status}");
    let view = json(&body);
    assert_eq!(view["status"], "edited");
    assert_eq!(view["message"], "revised draft");

    let (status, body) = get(addr, "/approvals/approval-1/editable-message").await;
    assert!(status.contains("200"), "reloaded editable status: {status}");
    assert_eq!(json(&body)["editable"], false);

    let (status, body) = get(addr, "/approvals/history").await;
    assert!(status.contains("200"), "reloaded history status: {status}");
    assert_eq!(json(&body).as_array().expect("history array").len(), 4);

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}

#[tokio::test]
async fn history_without_persistence_falls_back_to_current_state() {
    let (files_root, _) = temp_paths("memory-only");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let id = request_approval(
        addr,
        serde_json::json!({"tool": "mail.send", "message": "only in memory"}),
    )
    .await;
    assert_eq!(id, "approval-1");

    let (status, body) = get(addr, "/approvals/history").await;
    assert!(status.contains("200"), "fallback history status: {status}");
    let history = json(&body);
    let events = history.as_array().expect("history array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["approval_id"], "approval-1");
    assert_eq!(events[0]["status"], "pending");
    assert_eq!(events[0]["message"], "only in memory");

    server.abort();
    let _ = std::fs::remove_dir_all(&files_root);
}
