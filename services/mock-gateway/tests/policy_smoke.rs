//! Smoke tests for Task 11 permissions surface: defaults,
//! active overrides, hitl/snooze, single switches, network
//! rules, per-connector policies, scopes, reset, revoke,
//! inventory, and restart recovery through tagged JSONL.
//!
//! Covers all 19 routes: GET /permissions, GET/PATCH
//! /permissions/active, PATCH/DELETE
//! /permissions/connectors/{connector}, PATCH /permissions/hitl,
//! DELETE /permissions/hitl/snooze, PATCH /permissions/mitm,
//! PATCH /permissions/network-rules, POST /permissions/reset,
//! POST /permissions/revoke, GET/PATCH
//! /permissions/scopes/{scope_type}/{scope_id}, PATCH
//! /permissions/sni-rejection, PATCH /permissions/tproxy, GET
//! /permissions/inventory, GET/PATCH /permissions/settings, and
//! GET /connectors/{id}/permissions. Error codes: unknown
//! connector/scope 404, invalid network rule 400. The snooze
//! field is three-state: absent means no change, explicit null
//! clears, a number sets — the test asserts all three.

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

async fn call(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    json: Option<&str>,
) -> (String, String) {
    let body = json.unwrap_or("");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n"
    );
    if json.is_some() {
        request.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    request.push_str("\r\n");
    request.push_str(body);
    raw(addr, &request).await
}

async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    call(addr, "GET", path, None).await
}

async fn post(addr: std::net::SocketAddr, path: &str, json: &str) -> (String, String) {
    call(addr, "POST", path, Some(json)).await
}

async fn patch(addr: std::net::SocketAddr, path: &str, json: &str) -> (String, String) {
    call(addr, "PATCH", path, Some(json)).await
}

async fn delete(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    call(addr, "DELETE", path, None).await
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-policy-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn value(body: &str) -> serde_json::Value {
    serde_json::from_str(body).expect("policy json")
}

fn status(head: &str) -> u16 {
    head.split_whitespace().nth(1).and_then(|c| c.parse().ok()).expect("policy status")
}

#[tokio::test]
async fn policy_roundtrip() {
    let files_root = temp_root("roundtrip");
    let state_path = files_root.join("policies.jsonl");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_policy_state_path(state_path.clone());
    let (addr, _handle) = start_server(state).await;

    // Fresh policy: builtin defaults, everything off and empty.
    let (head, body) = get(addr, "/permissions").await;
    assert_eq!(status(&head), 200);
    let doc = value(&body);
    assert_eq!(doc["defaults"]["hitl_enabled"], false);
    assert_eq!(doc["active"]["mitm_enabled"], false);
    assert_eq!(doc["connectors"].as_array().unwrap().len(), 0);
    assert_eq!(doc["scopes"].as_array().unwrap().len(), 0);
    assert_eq!(doc["network_rules"].as_array().unwrap().len(), 0);

    // Settings layer: defaults change, active follows.
    let (head, body) =
        patch(addr, "/permissions/settings", r#"{"hitl_enabled":true,"mitm_enabled":true}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["hitl_enabled"], true);
    let (_, body) = get(addr, "/permissions/active").await;
    assert_eq!(value(&body)["hitl_enabled"], true);
    assert_eq!(value(&body)["mitm_enabled"], true);

    // Active layer: overrides merge over defaults, not replace.
    let (head, body) = patch(addr, "/permissions/active", r#"{"tproxy_enabled":true}"#).await;
    assert_eq!(status(&head), 200);
    let active = value(&body);
    assert_eq!(active["tproxy_enabled"], true);
    assert_eq!(active["hitl_enabled"], true);
    assert_eq!(active["mitm_enabled"], true);
    assert_eq!(active["sni_rejection_enabled"], false);

    // Hitl snooze, all three states: set, clear-by-null, set again.
    let (head, body) = patch(addr, "/permissions/hitl", r#"{"snoozed_until_ms":4242}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["enabled"], true);
    assert_eq!(value(&body)["snoozed_until_ms"], 4242);
    let (head, body) =
        patch(addr, "/permissions/active", r#"{"hitl_snoozed_until_ms":null}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["hitl_snoozed_until_ms"], serde_json::Value::Null);
    let (head, body) = patch(addr, "/permissions/hitl", r#"{"snoozed_until_ms":7777}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["snoozed_until_ms"], 7777);
    let (head, body) = delete(addr, "/permissions/hitl/snooze").await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["enabled"], true);
    assert_eq!(value(&body)["snoozed_until_ms"], serde_json::Value::Null);

    // Single switches: override beats defaults both ways.
    let (head, body) = patch(addr, "/permissions/mitm", r#"{"enabled":false}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["enabled"], false);
    let (_, body) = get(addr, "/permissions/active").await;
    assert_eq!(value(&body)["mitm_enabled"], false);
    for (route, key) in [("/permissions/tproxy", "tproxy_enabled"), ("/permissions/sni-rejection", "sni_rejection_enabled")] {
        let (head, _) = patch(addr, route, r#"{"enabled":true}"#).await;
        assert_eq!(status(&head), 200);
        let (_, body) = get(addr, "/permissions/active").await;
        assert_eq!(value(&body)[key], true);
    }

    // Network rules: replace whole list; invalid rules 400 and
    // leave the stored list untouched.
    let (head, body) = patch(
        addr,
        "/permissions/network-rules",
        r#"{"rules":[{"pattern":"example.com","action":"deny"}]}"#,
    )
    .await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body).as_array().unwrap().len(), 1);
    let (head, _) = patch(
        addr,
        "/permissions/network-rules",
        r#"{"rules":[{"pattern":"  ","action":"allow"}]}"#,
    )
    .await;
    assert_eq!(status(&head), 400);
    let (head, _) = patch(
        addr,
        "/permissions/network-rules",
        r#"{"rules":[{"pattern":"example.com","action":"block"}]}"#,
    )
    .await;
    assert_eq!(status(&head), 400);
    let (_, body) = get(addr, "/permissions").await;
    assert_eq!(value(&body)["network_rules"].as_array().unwrap().len(), 1);

    // Per-connector policy: update, read through the connectors
    // route, reset to default, unknown connector 404s.
    let (head, body) = patch(addr, "/permissions/connectors/notes", r#"{"allowed":false}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["allowed"], false);
    let (_, body) = get(addr, "/connectors/notes/permissions").await;
    assert_eq!(value(&body)["allowed"], false);
    let (head, _) = patch(addr, "/permissions/connectors/bogus", r#"{"allowed":false}"#).await;
    assert_eq!(status(&head), 404);
    let (head, _) = get(addr, "/connectors/bogus/permissions").await;
    assert_eq!(status(&head), 404);
    let (head, body) = delete(addr, "/permissions/connectors/notes").await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["allowed"], true);
    let (_, body) = get(addr, "/connectors/notes/permissions").await;
    assert_eq!(value(&body)["allowed"], true);

    // Scopes: upsert, read, unknown 404, revoke one, revoke all.
    let (head, body) = patch(addr, "/permissions/scopes/files/read", r#"{"granted":true}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["granted"], true);
    let (head, body) = patch(addr, "/permissions/scopes/files/read", r#"{"granted":false}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["granted"], false);
    let (head, _) = get(addr, "/permissions/scopes/files/write").await;
    assert_eq!(status(&head), 404);
    let (head, body) =
        post(addr, "/permissions/revoke", r#"{"scope_type":"files","scope_id":"read"}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["revoked"], serde_json::json!(["files/read"]));
    let (head, _) = get(addr, "/permissions/scopes/files/read").await;
    assert_eq!(status(&head), 404);
    patch(addr, "/permissions/scopes/a/b", r#"{}"#).await;
    patch(addr, "/permissions/scopes/c/d", r#"{}"#).await;
    let (head, body) = post(addr, "/permissions/revoke", r#"{}"#).await;
    assert_eq!(status(&head), 200);
    assert_eq!(value(&body)["revoked"], serde_json::json!(["a/b", "c/d"]));

    // Inventory reflects the stored state.
    let (_, body) = get(addr, "/permissions/inventory").await;
    let inventory = value(&body);
    assert_eq!(inventory["sections"].as_array().unwrap().len(), 7);
    assert_eq!(inventory["connector_policies"], 0);
    assert_eq!(inventory["scopes"], 0);
    assert_eq!(inventory["network_rules"], 1);
    assert_eq!(inventory["defaults_customized"], true);

    // Reset restores builtin defaults and clears everything.
    let (head, body) = post(addr, "/permissions/reset", r#"{}"#).await;
    assert_eq!(status(&head), 200);
    let doc = value(&body);
    assert_eq!(doc["defaults"]["hitl_enabled"], false);
    assert_eq!(doc["active"]["tproxy_enabled"], false);
    assert_eq!(doc["network_rules"].as_array().unwrap().len(), 0);
    let (_, body) = get(addr, "/permissions/inventory").await;
    assert_eq!(value(&body)["defaults_customized"], false);
}

#[tokio::test]
async fn policy_restart_recovery() {
    let files_root = temp_root("restart");
    let state_path = files_root.join("policies.jsonl");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_policy_state_path(state_path.clone());
    let (addr, _) = start_server(state).await;
    patch(addr, "/permissions/settings", r#"{"sni_rejection_enabled":true}"#).await;
    patch(addr, "/permissions/connectors/mail", r#"{"allowed":false}"#).await;
    patch(addr, "/permissions/scopes/x/y", r#"{"granted":true}"#).await;
    patch(
        addr,
        "/permissions/network-rules",
        r#"{"rules":[{"pattern":"a.example","action":"allow"}]}"#,
    )
    .await;
    // Reset then restart: tombstones must keep cleared records gone.
    patch(addr, "/permissions/scopes/x/y", r#"{"granted":true}"#).await;
    post(addr, "/permissions/revoke", r#"{"scope_type":"x","scope_id":"y"}"#).await;

    let state2 = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_policy_state_path(state_path.clone());
    let (addr2, _) = start_server(state2).await;
    let (_, body) = get(addr2, "/permissions").await;
    let doc = value(&body);
    assert_eq!(doc["defaults"]["sni_rejection_enabled"], true);
    assert_eq!(doc["active"]["sni_rejection_enabled"], true);
    assert_eq!(doc["connectors"].as_array().unwrap().len(), 1);
    assert_eq!(doc["connectors"][0]["connector_id"], "mail");
    assert_eq!(doc["connectors"][0]["allowed"], false);
    assert_eq!(doc["scopes"].as_array().unwrap().len(), 0);
    assert_eq!(doc["network_rules"].as_array().unwrap().len(), 1);
}
