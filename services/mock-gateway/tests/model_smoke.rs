//! Smoke tests for Task 12 model surface: GET reports the
//! running config (deepseek-flash defaults, no key in the
//! disabled test state), POST stages a selection that does
//! NOT change the running client (restart_needed), invalid
//! model/base_url are 400, all-absent body is a no-op, and
//! the staged selection survives a restart through tagged
//! JSONL. A state with a fake key proves the key is never
//! leaked: only configured + ****last-4 appear.

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
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

async fn get(addr: std::net::SocketAddr) -> (String, serde_json::Value) {
    let (head, body) = raw(addr, "GET /model HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await;
    (head, serde_json::from_str(&body).expect("GET /model body is JSON"))
}

async fn post(addr: std::net::SocketAddr, body: &str) -> (String, serde_json::Value) {
    let request = format!(
        "POST /model HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let (head, resp_body) = raw(addr, &request).await;
    let value: serde_json::Value = serde_json::from_str(&resp_body).unwrap_or(serde_json::Value::Null);
    (head, value)
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-model-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn model_get_post_and_restart() {
    let path = temp_root("smoke").join("model.jsonl");
    let state = AppState::default().with_model_state_path(path.clone());
    let (addr, handle) = start_server(state).await;

    // Defaults from the disabled test state: deepseek-flash,
    // official base URL, no key, nothing staged.
    let (head, view) = get(addr).await;
    assert!(head.contains("200"), "{head}");
    assert_eq!(view["provider"], "deepseek");
    assert_eq!(view["model"], "deepseek-flash");
    assert_eq!(view["base_url"], "https://api.deepseek.com");
    assert_eq!(view["key_configured"], false);
    assert_eq!(view["key_redacted"], serde_json::Value::Null);
    assert_eq!(view["staged_model"], serde_json::Value::Null);
    assert_eq!(view["restart_needed"], false);

    // Stage a model: the effective view shows it, the run
    // is flagged restart_needed, and the staged record persists.
    let (head, view) = post(addr, r#"{"model": "deepseek-chat"}"#).await;
    assert!(head.contains("200"), "{head}");
    assert_eq!(view["model"], "deepseek-chat");
    assert_eq!(view["staged_model"], "deepseek-chat");
    assert_eq!(view["restart_needed"], true);

    // Validation: blank model and non-http(s) base_url are 400.
    let (head, _) = post(addr, r#"{"model": "  "}"#).await;
    assert!(head.contains("400"), "{head}");
    let (head, _) = post(addr, r#"{"base_url": "ftp://example.com"}"#).await;
    assert!(head.contains("400"), "{head}");

    // All-absent body is a no-op success: staged value kept.
    let (head, view) = post(addr, r#"{}"#).await;
    assert!(head.contains("200"), "{head}");
    assert_eq!(view["staged_model"], "deepseek-chat");

    // Restart recovery: a fresh state on the same file keeps
    // the staged selection (tombstone-free, single record).
    handle.abort();
    let state = AppState::default().with_model_state_path(path);
    let (addr, handle) = start_server(state).await;
    let (head, view) = get(addr).await;
    assert!(head.contains("200"), "{head}");
    assert_eq!(view["model"], "deepseek-chat");
    assert_eq!(view["staged_model"], "deepseek-chat");
    assert_eq!(view["restart_needed"], true);
    handle.abort();
}

#[tokio::test]
async fn model_key_never_leaks() {
    let config = DeepSeekConfig {
        base_url: "https://api.deepseek.com".to_string(),
        model: "deepseek-flash".to_string(),
        api_key: Some("sk-test-fake-key-1234".to_string()),
    };
    let state = AppState::default().with_deepseek(DeepSeekClient::new(config));
    let (addr, handle) = start_server(state).await;
    let (head, body) = raw(addr, "GET /model HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").await;
    assert!(head.contains("200"), "{head}");
    assert!(!body.contains("sk-test-fake-key-1234"), "key leaked in response");
    let view: serde_json::Value = serde_json::from_str(&body).expect("GET /model body is JSON");
    assert_eq!(view["key_configured"], true);
    assert_eq!(view["key_redacted"], "****1234");
    handle.abort();
}
