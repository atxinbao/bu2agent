//! Phase 4 tests: credential store, egress policy, and the
//! Mock connector client against a fake gateway.
//!
//! Credentials: memory-only, redacted display never contains
//! the full secret, a fresh store is empty.
//! Egress: only the localhost Mock host, only mock.local
//! recipients, secret markers refused.
//! Connectors: lists run directly; creates/sends stop at
//! pending without approval, run after approval, and are
//! refused server-side with no record.

use agent_desktop::agent_client::{AgentError, ToolDecision};
use agent_desktop::connector_client::ConnectorClient;
use agent_desktop::credential_store::{CredentialStore, MemoryCredentialStore};
use agent_desktop::egress::EgressPolicy;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

#[test]
fn credential_store_is_memory_only_and_redacted() {
    let store = MemoryCredentialStore::new();
    assert!(store.retrieve("mock").is_err());
    store.store("mock", "mock-token-1234").expect("store");
    assert_eq!(store.retrieve("mock").expect("retrieve"), "mock-token-1234");
    let display = store.redacted("mock").expect("redacted");
    assert_eq!(display, "****1234");
    assert!(!display.contains("mock-token"));
    store.remove("mock").expect("remove");
    assert!(store.retrieve("mock").is_err());
    let fresh = MemoryCredentialStore::new();
    assert!(fresh.retrieve("mock").is_err());
}

#[test]
fn egress_policy_allows_only_mock_destinations() {
    let policy = EgressPolicy::mock();
    assert!(policy.check_url("http://127.0.0.1:3001").is_ok());
    assert!(policy.check_url("http://localhost:3001").is_ok());
    // Loopback is the boundary, not the port: ephemeral test
    // ports are allowed, any non-loopback host is not.
    assert!(policy.check_url("http://127.0.0.1:9999").is_ok());
    assert!(policy.check_url("https://example.com").is_err());
    assert!(policy.check_url("http://example.com").is_err());
    assert!(policy.check_recipient("a@mock.local").is_ok());
    assert!(policy.check_recipient("a@example.com").is_err());
    assert!(policy.check_recipient("not-an-address").is_err());
    assert!(policy.check_payload("hello world").is_ok());
    assert!(policy.check_payload("token: abc123").is_err());
    assert!(policy.check_payload("Authorization: Bearer xyz").is_err());
    assert!(policy.check_payload("api_key=secret").is_err());
}

fn start_fake_gateway() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake gateway");
    let addr = listener.local_addr().expect("fake gateway addr");
    let creates = Arc::new(AtomicUsize::new(0));
    let sends = Arc::new(AtomicUsize::new(0));
    let records: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let creates_clone = Arc::clone(&creates);
    let sends_clone = Arc::clone(&sends);
    let records_clone = Arc::clone(&records);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let mut buf = [0u8; 8192];
            let n = stream.read(&mut buf).unwrap_or(0);
            let text = String::from_utf8_lossy(&buf[..n]).into_owned();
            let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
            let path = head.lines().next().and_then(|line| line.split_whitespace().nth(1)).unwrap_or("/").to_string();
            let json: serde_json::Value = serde_json::from_str(body).unwrap_or(serde_json::json!({}));
            let tool = json.get("tool").and_then(|t| t.as_str()).unwrap_or("").to_string();
            let (status, resp) = if path == "/approval/request" {
                if tool == "notes.create" || tool == "mail.send" {
                    records_clone.lock().expect("record lock").insert("approval-1".to_string(), "pending".to_string());
                    (200, r#"{"tool_call_id":"approval-1","requires_approval":true,"status":"pending"}"#.to_string())
                } else {
                    (400, r#"{"error":"unknown tool"}"#.to_string())
                }
            } else if path == "/approval/decide" {
                let id = json.get("tool_call_id").and_then(|t| t.as_str()).unwrap_or("").to_string();
                let decision = json.get("status").and_then(|t| t.as_str()).unwrap_or("").to_string();
                records_clone.lock().expect("record lock").insert(id, decision);
                (200, r#"{"decided":true}"#.to_string())
            } else if path == "/connectors/notes/list" {
                (200, r#"[{"id":"note-1","title":"Mock note 1","body":"seed"},{"id":"note-2","title":"Mock note 2","body":"seed"}]"#.to_string())
            } else if path == "/connectors/mail/list" {
                (200, r#"[{"id":"mail-1","from":"alice@mock.local","to":"me@mock.local","subject":"Mock welcome","body":"seed"}]"#.to_string())
            } else if path == "/connectors/notes/create" {
                creates_clone.fetch_add(1, Ordering::SeqCst);
                let id = json.get("tool_call_id").and_then(|t| t.as_str()).unwrap_or("");
                let status = records_clone.lock().expect("record lock").get(id).cloned().unwrap_or_default();
                if status == "approved" {
                    (200, r#"{"id":"note-3","title":"T","body":"B"}"#.to_string())
                } else {
                    (403, r#"{"error":"approval_required"}"#.to_string())
                }
            } else if path == "/connectors/mail/send" {
                sends_clone.fetch_add(1, Ordering::SeqCst);
                let id = json.get("tool_call_id").and_then(|t| t.as_str()).unwrap_or("");
                let status = records_clone.lock().expect("record lock").get(id).cloned().unwrap_or_default();
                if status == "approved" {
                    (200, r#"{"id":"mail-3","from":"me@mock.local","to":"a@mock.local","subject":"S","body":"B"}"#.to_string())
                } else {
                    (403, r#"{"error":"approval_required"}"#.to_string())
                }
            } else {
                (404, r#"{"error":"unknown"}"#.to_string())
            };
            let response = format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}", resp.len());
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (format!("http://{addr}"), creates, sends)
}

#[test]
fn connector_client_approval_and_egress() {
    let (base_url, creates, sends) = start_fake_gateway();
    let client = ConnectorClient::new(&base_url).expect("client");
    assert!(ConnectorClient::new("https://example.com").is_err());
    let notes = client.notes_list().expect("notes list");
    assert_eq!(notes.len(), 2);
    let messages = client.mail_list().expect("mail list");
    assert_eq!(messages.len(), 1);
    // Create + Ask stops at pending; the server never sees a create.
    let result = client.notes_create("T", "B", ToolDecision::Ask);
    match result {
        Err(AgentError::ApprovalPending { tool_call_id }) => assert_eq!(tool_call_id, "approval-1"),
        other => panic!("expected ApprovalPending, got {other:?}"),
    }
    assert_eq!(creates.load(Ordering::SeqCst), 0);
    // Create + Approve runs.
    let note = client.notes_create("T", "B", ToolDecision::Approve).expect("approved create");
    assert_eq!(note.id, "note-3");
    assert_eq!(creates.load(Ordering::SeqCst), 1);
    // Mail outside mock.local is refused client-side, no request.
    let before = sends.load(Ordering::SeqCst);
    let result = client.mail_send("a@example.com", "S", "B", ToolDecision::Approve);
    assert!(result.is_err());
    assert_eq!(sends.load(Ordering::SeqCst), before);
    // Mail inside mock.local with approval runs.
    let message = client.mail_send("a@mock.local", "S", "B", ToolDecision::Approve).expect("approved send");
    assert_eq!(message.id, "mail-3");
    assert_eq!(sends.load(Ordering::SeqCst), before + 1);
}
