//! Smoke tests for Task 9 connector registry + account lifecycle.
//!
//! Covers the nine `GET|POST /connectors/*` routes: the static
//! registry (exactly the two Mock connectors the gateway
//! implements), unknown-connector 404s, account link/unlink
//! (default display name, tombstone, idempotent re-unlink,
//! cross-connector 404), connect requiring a linked account
//! (409 otherwise), disconnect keeping accounts linked, and
//! restart recovery of accounts and connection flags through
//! the tagged JSONL store. The notes/mail data routes are
//! checked still intact at the end.

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
    raw(addr, &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")).await
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-connector-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn value(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("connector json")
}

#[tokio::test]
async fn registry_account_lifecycle_roundtrip() {
    let files_root = temp_root("roundtrip");
    let state_path = files_root.join("connectors.jsonl");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_connector_state_path(state_path.clone());
    let (addr, server) = start_server(state).await;

    // Registry: exactly notes + mail, both disconnected.
    let (status, body) = get(addr, "/connectors").await;
    assert!(status.contains("200"), "list status: {status}");
    let list = value(&body);
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert_eq!(list[0]["id"], "notes");
    assert_eq!(list[0]["connected"], false);
    assert_eq!(list[1]["id"], "mail");
    assert_eq!(list[1]["connected"], false);

    let (status, body) = get(addr, "/connectors/notes").await;
    assert!(status.contains("200"), "get notes status: {status}");
    assert_eq!(value(&body)["capabilities"], serde_json::json!(["notes.list", "notes.create"]));
    let (status, _) = get(addr, "/connectors/slack").await;
    assert!(status.contains("404"), "unknown connector status: {status}");

    // Connect info describes the local Mock flow; no OAuth anywhere.
    let (status, body) = get(addr, "/connectors/notes/connect/info").await;
    assert!(status.contains("200"), "connect info status: {status}");
    assert_eq!(value(&body)["flow"], "mock");
    assert_eq!(value(&body)["requires_account"], true);

    // Connect without a linked account is a conflict.
    let (status, _) = post(addr, "/connectors/notes/connect", r#"{}"#).await;
    assert!(status.contains("409"), "connect without account status: {status}");

    // Link accounts: default display name, then an explicit one.
    let (status, body) = post(addr, "/connectors/notes/accounts/link", r#"{}"#).await;
    assert!(status.contains("200"), "link default status: {status} body: {body}");
    assert_eq!(value(&body)["display_name"], "Mock account");
    assert_eq!(value(&body)["status"], "linked");
    let notes_account = value(&body)["account_id"].as_str().unwrap().to_string();
    let (status, body) = post(addr, "/connectors/notes/accounts/link", r#"{"display_name":"Work notes"}"#).await;
    assert!(status.contains("200"), "link named status: {status}");
    assert_eq!(value(&body)["display_name"], "Work notes");
    let (status, body) = post(addr, "/connectors/mail/accounts/link", r#"{"display_name":"Inbox"}"#).await;
    assert!(status.contains("200"), "link mail status: {status}");
    let mail_account = value(&body)["account_id"].as_str().unwrap().to_string();
    let (status, _) = post(addr, "/connectors/slack/accounts/link", r#"{}"#).await;
    assert!(status.contains("404"), "link unknown connector status: {status}");

    let (_, body) = get(addr, "/connectors/notes/accounts").await;
    assert_eq!(value(&body).as_array().unwrap().len(), 2);
    let (_, body) = get(addr, "/connectors/mail/accounts").await;
    assert_eq!(value(&body).as_array().unwrap().len(), 1);

    // Connect, disconnect (accounts stay), reconnect.
    let (status, body) = post(addr, "/connectors/notes/connect", r#"{}"#).await;
    assert!(status.contains("200"), "connect status: {status}");
    assert_eq!(value(&body)["connected"], true);
    let (status, body) = post(addr, "/connectors/notes/disconnect", r#"{}"#).await;
    assert!(status.contains("200"), "disconnect status: {status}");
    assert_eq!(value(&body)["connected"], false);
    let (_, body) = get(addr, "/connectors/notes/accounts").await;
    assert_eq!(value(&body).as_array().unwrap().len(), 2, "disconnect keeps accounts linked");
    let (status, _) = post(addr, "/connectors/notes/connect", r#"{}"#).await;
    assert!(status.contains("200"), "reconnect status: {status}");

    // Restart: accounts and the connection flag survive the reload.
    server.abort();
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_connector_state_path(state_path.clone());
    let (addr, server2) = start_server(state).await;
    let (status, body) = get(addr, "/connectors/notes").await;
    assert!(status.contains("200"), "relist status: {status}");
    assert_eq!(value(&body)["connected"], true);
    let (_, body) = get(addr, "/connectors/notes/accounts").await;
    assert_eq!(value(&body).as_array().unwrap().len(), 2);

    // Unlink: tombstone, excluded from lists, idempotent re-unlink.
    let (status, body) = post(addr, &format!("/connectors/notes/accounts/{notes_account}/unlink"), r#"{}"#).await;
    assert!(status.contains("200"), "unlink status: {status} body: {body}");
    assert_eq!(value(&body)["status"], "unlinked");
    let (_, body) = get(addr, "/connectors/notes/accounts").await;
    assert_eq!(value(&body).as_array().unwrap().len(), 1);
    let (status, _) = post(addr, &format!("/connectors/notes/accounts/{notes_account}/unlink"), r#"{}"#).await;
    assert!(status.contains("200"), "re-unlink status: {status}");
    let (status, _) = post(addr, "/connectors/notes/accounts/account-999/unlink", r#"{}"#).await;
    assert!(status.contains("404"), "unlink unknown status: {status}");
    let (status, _) = post(addr, &format!("/connectors/notes/accounts/{mail_account}/unlink"), r#"{}"#).await;
    assert!(status.contains("404"), "cross-connector unlink status: {status}");

    // The notes/mail data routes are untouched by the registry.
    let (status, body) = get(addr, "/connectors/notes/list").await;
    assert!(status.contains("200"), "notes list status: {status}");
    assert!(!value(&body).as_array().unwrap().is_empty());
    server2.abort();
}
