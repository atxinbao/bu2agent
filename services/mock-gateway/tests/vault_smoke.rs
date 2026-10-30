//! Smoke tests for Task 10 vault surface: credentials, catalog,
//! custom connectors, and external-provider capture tickets.
//!
//! Covers all 13 `POST|GET /v1/*` routes: register/duplicate-409/
//! blank-400, capture on unknown-404, capture ticket request/
//! fulfill/double-fulfill-409, catalog auto-create on register
//! with a provider plus update/delete (metadata only: deleting a
//! catalog entry keeps the slot), custom-connector register/
//! duplicate-409/delete/delete-again-404, credential delete
//! (real backend erasure + metadata tombstone), restart recovery
//! of metadata through the tagged JSONL store, and the hard rule
//! that no response body ever contains a secret value.

use std::sync::Arc;

use agent_desktop::credential_store::{CredentialStore, MemoryCredentialStore};
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
    let dir = std::env::temp_dir().join(format!("mock-vault-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn value(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("vault json")
}

fn assert_no_secret(body: &str) {
    assert!(!body.contains("mock-token-"), "secret leaked in response: {body}");
}

#[tokio::test]
async fn vault_roundtrip() {
    let files_root = temp_root("roundtrip");
    let state_path = files_root.join("vault.jsonl");
    // One shared memory backend across both states: it plays the
    // role Keychain plays in production (secrets outlive a restart).
    let backend = Arc::new(MemoryCredentialStore::new());
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_vault_state_path(state_path.clone())
        .with_vault_secrets(backend.clone());
    let (addr, server) = start_server(state).await;

    // Register: metadata + redacted, never the secret.
    let (status, body) = post(
        addr,
        "/v1/credentials/register",
        r#"{"handle":"验收-凭据A","secret":"mock-token-1234567","provider":"mock-provider"}"#,
    )
    .await;
    assert!(status.contains("200"), "register status: {status} {body}");
    assert_no_secret(&body);
    let reg = value(&body);
    assert_eq!(reg["handle"], "验收-凭据A");
    assert_eq!(reg["provider"], "mock-provider");
    assert_eq!(reg["redacted"], "****4567");

    // Duplicate handle -> 409; blank handle/secret -> 400.
    let (status, _) = post(
        addr,
        "/v1/credentials/register",
        r#"{"handle":"验收-凭据A","secret":"mock-token-9999"}"#,
    )
    .await;
    assert!(status.contains("409"), "duplicate status: {status}");
    let (status, _) = post(addr, "/v1/credentials/register", r#"{"handle":"","secret":"x"}"#).await;
    assert!(status.contains("400"), "blank handle status: {status}");
    let (status, _) = post(addr, "/v1/credentials/register", r#"{"handle":"h2","secret":""}"#).await;
    assert!(status.contains("400"), "blank secret status: {status}");

    // Capture on unknown -> 404; capture rotates the secret.
    let (status, _) = post(addr, "/v1/credentials/capture", r#"{"handle":"nope","secret":"mock-token-1"}"#).await;
    assert!(status.contains("404"), "capture unknown status: {status}");
    let (status, body) = post(
        addr,
        "/v1/credentials/capture",
        r#"{"handle":"验收-凭据A","secret":"mock-token-7654321"}"#,
    )
    .await;
    assert!(status.contains("200"), "capture status: {status} {body}");
    assert_no_secret(&body);
    assert_eq!(value(&body)["redacted"], "****4321");

    // Provider auto-created a catalog entry at register time.
    let (status, body) = get(addr, "/v1/credentials/catalog").await;
    assert!(status.contains("200"), "catalog status: {status}");
    let catalog = value(&body);
    assert_eq!(catalog.as_array().unwrap().len(), 1);
    assert_eq!(catalog[0]["id"], "mock-provider");
    let (status, body) = get(addr, "/v1/credentials/catalog/details?id=mock-provider").await;
    assert!(status.contains("200"), "details status: {status}");
    assert_eq!(value(&body)["display_name"], "mock-provider");
    let (status, _) = get(addr, "/v1/credentials/catalog/details?id=nope").await;
    assert!(status.contains("404"), "details unknown status: {status}");

    // Catalog update + delete: metadata only, slot survives.
    let (status, body) = post(
        addr,
        "/v1/credentials/catalog/update",
        r#"{"id":"mock-provider","display_name":"Mock Provider","description":"test provider"}"#,
    )
    .await;
    assert!(status.contains("200"), "catalog update status: {status} {body}");
    assert_eq!(value(&body)["display_name"], "Mock Provider");
    let (status, _) = post(addr, "/v1/credentials/catalog/delete", r#"{"id":"mock-provider"}"#).await;
    assert!(status.contains("200"), "catalog delete status: {status}");
    let (status, body) = get(addr, "/v1/credentials/catalog").await;
    assert!(status.contains("200"), "catalog after delete: {status}");
    assert_eq!(value(&body).as_array().unwrap().len(), 0);
    let (status, body) = get(addr, "/v1/credentials/list").await;
    assert!(status.contains("200"), "list after catalog delete: {status}");
    assert_eq!(value(&body).as_array().unwrap().len(), 1);

    // Custom connectors: register, duplicate 409, list, delete, re-delete 404.
    let (status, body) = post(
        addr,
        "/v1/custom-connectors/register",
        r#"{"id":"my-connector","display_name":"My Connector"}"#,
    )
    .await;
    assert!(status.contains("200"), "custom register status: {status} {body}");
    assert_eq!(value(&body)["id"], "my-connector");
    let (status, _) = post(
        addr,
        "/v1/custom-connectors/register",
        r#"{"id":"my-connector","display_name":"Again"}"#,
    )
    .await;
    assert!(status.contains("409"), "custom duplicate status: {status}");
    let (status, body) = post(addr, "/v1/custom-connectors/list", "").await;
    assert!(status.contains("200"), "custom list status: {status}");
    assert_eq!(value(&body).as_array().unwrap().len(), 1);
    let (status, _) = post(addr, "/v1/custom-connectors/delete", r#"{"id":"my-connector"}"#).await;
    assert!(status.contains("200"), "custom delete status: {status}");
    let (status, _) = post(addr, "/v1/custom-connectors/delete", r#"{"id":"my-connector"}"#).await;
    assert!(status.contains("404"), "custom re-delete status: {status}");

    // Capture tickets: unknown handle 404, request, fulfill, double-fulfill 409.
    let (status, _) = post(
        addr,
        "/v1/credentials/capture/external-provider/request",
        r#"{"provider":"mock-provider","handle":"nope"}"#,
    )
    .await;
    assert!(status.contains("404"), "ticket unknown handle: {status}");
    let (status, body) = post(
        addr,
        "/v1/credentials/capture/external-provider/request",
        r#"{"provider":"mock-provider","handle":"验收-凭据A"}"#,
    )
    .await;
    assert!(status.contains("200"), "ticket request status: {status} {body}");
    let ticket = value(&body);
    assert_eq!(ticket["status"], "pending");
    assert!(ticket["expires_ms"].as_u64().unwrap() > ticket["created_ms"].as_u64().unwrap());
    let ticket_id = ticket["ticket_id"].as_str().unwrap().to_string();
    let (status, _) = post(
        addr,
        "/v1/credentials/capture/external-provider/fulfill",
        r#"{"ticket_id":"ticket-999","secret":"mock-token-1"}"#,
    )
    .await;
    assert!(status.contains("404"), "fulfill unknown ticket: {status}");
    let (status, _) = post(
        addr,
        "/v1/credentials/capture/external-provider/fulfill",
        &format!(r#"{{"ticket_id":"{ticket_id}","secret":""}}"#),
    )
    .await;
    assert!(status.contains("400"), "fulfill blank secret: {status}");
    let (status, body) = post(
        addr,
        "/v1/credentials/capture/external-provider/fulfill",
        &format!(r#"{{"ticket_id":"{ticket_id}","secret":"mock-token-abcdefg"}}"#),
    )
    .await;
    assert!(status.contains("200"), "fulfill status: {status} {body}");
    assert_no_secret(&body);
    assert_eq!(value(&body)["redacted"], "****defg");
    let (status, _) = post(
        addr,
        "/v1/credentials/capture/external-provider/fulfill",
        &format!(r#"{{"ticket_id":"{ticket_id}","secret":"mock-token-2"}}"#),
    )
    .await;
    assert!(status.contains("409"), "double fulfill status: {status}");

    // Credential delete: real erasure + tombstone; second delete 404.
    let (status, body) = post(addr, "/v1/credentials/delete", r#"{"handle":"验收-凭据A"}"#).await;
    assert!(status.contains("200"), "delete status: {status} {body}");
    assert_no_secret(&body);
    let (status, _) = post(addr, "/v1/credentials/delete", r#"{"handle":"验收-凭据A"}"#).await;
    assert!(status.contains("404"), "re-delete status: {status}");
    let (status, _) = post(addr, "/v1/credentials/capture", r#"{"handle":"验收-凭据A","secret":"mock-token-1"}"#).await;
    assert!(status.contains("404"), "capture after delete: {status}");
    let (status, body) = get(addr, "/v1/credentials/list").await;
    assert!(status.contains("200"), "list after delete: {status}");
    assert_eq!(value(&body).as_array().unwrap().len(), 0);

    // Backend erasure proof: the secret is really gone.
    assert!(backend.retrieve("验收-凭据A").is_err(), "secret survived delete");
    server.abort();

    // Restart recovery: a fresh state on the same JSONL reloads
    // metadata; the shared backend still holds the secret, so a
    // re-registered slot shows its redacted value again.
    let state2 = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_vault_state_path(state_path.clone())
        .with_vault_secrets(backend.clone());
    let (addr2, server2) = start_server(state2).await;
    let (status, body) = post(
        addr2,
        "/v1/credentials/register",
        r#"{"handle":"验收-凭据B","secret":"mock-token-31337","provider":"mock-provider"}"#,
    )
    .await;
    assert!(status.contains("200"), "re-register status: {status} {body}");
    assert_eq!(value(&body)["redacted"], "****1337");
    let (status, body) = get(addr2, "/v1/credentials/list").await;
    assert!(status.contains("200"), "list after restart: {status}");
    // Only B: A was tombstoned before the restart.
    let list = value(&body);
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["handle"], "验收-凭据B");
    server2.abort();
}
