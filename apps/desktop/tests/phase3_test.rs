//! Phase 3 tests: agent invocation with the approval loop.
//!
//! Against a fake gateway that keeps its own approval records:
//! - Non-approval tools run directly, no record needed.
//! - Approval tools + Ask stop at a pending record, nothing runs.
//! - Approval tools + Approve run after the decision.
//! - Approval tools + Reject are refused by the server gate.
//! - A direct call with no record is refused server-side.
//! - Unknown tools are rejected before any request.

use agent_desktop::agent_client::{AgentClient, AgentError, ToolDecision};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn start_fake_gateway() -> (String, Arc<AtomicUsize>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake gateway");
    let addr = listener.local_addr().expect("fake gateway addr");
    let requests = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let decides = Arc::new(AtomicUsize::new(0));
    let records: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    let requests_clone = Arc::clone(&requests);
    let calls_clone = Arc::clone(&calls);
    let decides_clone = Arc::clone(&decides);
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
            let path = head
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_string();
            let json: serde_json::Value =
                serde_json::from_str(body).unwrap_or(serde_json::json!({}));
            let tool = json.get("tool").and_then(|t| t.as_str()).unwrap_or("").to_string();
            let requires = matches!(tool.as_str(), "fs.write" | "fs.delete");
            let known = matches!(
                tool.as_str(),
                "fs.list" | "fs.read" | "fs.write" | "fs.delete" | "echo"
            );
            let (status, resp) = if path == "/approval/request" {
                requests_clone.fetch_add(1, Ordering::SeqCst);
                if !known {
                    (400, r#"{"error":"unknown tool"}"#.to_string())
                } else if !requires {
                    (200, r#"{"tool_call_id":null,"requires_approval":false,"status":"not_required"}"#.to_string())
                } else {
                    let id = "approval-1".to_string();
                    records_clone
                        .lock()
                        .expect("record lock")
                        .insert(id.clone(), "pending".to_string());
                    (200, format!("{{\"tool_call_id\":\"{id}\",\"requires_approval\":true,\"status\":\"pending\"}}"))
                }
            } else if path == "/approval/decide" {
                decides_clone.fetch_add(1, Ordering::SeqCst);
                let id = json.get("tool_call_id").and_then(|t| t.as_str()).unwrap_or("").to_string();
                let decision = json.get("status").and_then(|t| t.as_str()).unwrap_or("").to_string();
                records_clone.lock().expect("record lock").insert(id, decision);
                (200, r#"{"decided":true}"#.to_string())
            } else if path == "/agent/tool/call" {
                calls_clone.fetch_add(1, Ordering::SeqCst);
                if !known {
                    (400, r#"{"error":"unknown tool"}"#.to_string())
                } else if requires {
                    let id = json.get("tool_call_id").and_then(|t| t.as_str()).unwrap_or("");
                    let status = records_clone
                        .lock()
                        .expect("record lock")
                        .get(id)
                        .cloned()
                        .unwrap_or_default();
                    if status == "approved" || status == "edited" {
                        (200, format!("{{\"tool\":\"{tool}\",\"result\":{{\"written\":true}},\"executed\":true}}"))
                    } else {
                        (403, r#"{"error":"approval_required"}"#.to_string())
                    }
                } else {
                    (200, format!("{{\"tool\":\"{tool}\",\"result\":\"ok\",\"executed\":true}}"))
                }
            } else {
                (404, r#"{"error":"unknown"}"#.to_string())
            };
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{resp}",
                resp.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    (format!("http://{addr}"), requests, calls, decides)
}

fn args(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn agent_approval_loop_end_to_end() {
    let (base_url, requests, calls, decides) = start_fake_gateway();
    let client = AgentClient::new(&base_url).expect("client");
    // echo needs no approval and runs directly.
    let result = client.run_tool("echo", &args(&[("text", "hi")]), ToolDecision::Ask).expect("echo");
    assert_eq!(result.tool, "echo");
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // fs.write + Ask stops at a pending record; nothing executes.
    let result = client.run_tool("fs.write", &args(&[("path", "a.txt"), ("text", "x")]), ToolDecision::Ask);
    match result {
        Err(AgentError::ApprovalPending { tool_call_id }) => {
            assert_eq!(tool_call_id, "approval-1");
        }
        other => panic!("expected ApprovalPending, got {other:?}"),
    }
    assert_eq!(requests.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // fs.write + Approve runs after the decision.
    let result = client
        .run_tool("fs.write", &args(&[("path", "a.txt"), ("text", "x")]), ToolDecision::Approve)
        .expect("approved write");
    assert_eq!(result.tool, "fs.write");
    assert_eq!(decides.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // fs.write + Reject is refused by the server gate.
    let result = client.run_tool("fs.write", &args(&[("path", "a.txt"), ("text", "x")]), ToolDecision::Reject);
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    // A direct call with no record is refused server-side.
    let result = client.call_tool("fs.write", &args(&[("path", "a.txt"), ("text", "x")]), None);
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    // Unknown tools are rejected before any request.
    let before_requests = requests.load(Ordering::SeqCst);
    let before_calls = calls.load(Ordering::SeqCst);
    let result = client.run_tool("no.such.tool", &args(&[]), ToolDecision::Ask);
    assert!(result.is_err());
    assert_eq!(requests.load(Ordering::SeqCst), before_requests);
    assert_eq!(calls.load(Ordering::SeqCst), before_calls);
}
