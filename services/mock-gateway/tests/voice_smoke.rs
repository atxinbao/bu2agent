//! Smoke tests for Task 21 voice record subset (D2):
//! static [Mock] catalog and pipeline seeds, custom voice
//! rename/delete over seeds, record-only active voice and
//! pipeline selections, and an onboarding reset that
//! clears just those two selections. No audio plays here.

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
    let text = String::from_utf8(buf).expect("response is utf-8");
    let (head, body) = text.split_once("\r\n\r\n").expect("response head/body split");
    (head.to_string(), body.to_string())
}

async fn call(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> (u16, serde_json::Value) {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nhost: 127.0.0.1\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let (head, body) = raw(addr, &request).await;
    let status: u16 = head
        .lines()
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status code parses");
    (status, serde_json::from_str(&body).expect("response body parses"))
}

#[tokio::test]
async fn voice_smoke() {
    let state = AppState::default().with_voice_state_path(
        std::env::temp_dir().join(format!("bu2agent-voice-smoke-{}.jsonl", std::process::id())),
    );
    let (addr, handle) = start_server(state).await;

    let (status, catalog) = call(addr, "GET", "/api/voice/catalog", "").await;
    assert_eq!(status, 200);
    let catalog = catalog.as_array().expect("catalog array");
    assert_eq!(catalog.len(), 3);
    assert!(catalog.iter().all(|v| v["available"].as_bool() == Some(true)));

    let (status, filters) = call(addr, "GET", "/api/voice/catalog/filters", "").await;
    assert_eq!(status, 200);
    assert_eq!(filters["locales"], serde_json::json!(["en-US", "es-ES", "zh-CN"]));

    let (status, available) = call(addr, "GET", "/api/voice/available-voices", "").await;
    assert_eq!(status, 200);
    assert_eq!(available.as_array().expect("available array").len(), 3);

    let (status, profiles) = call(addr, "GET", "/api/voice/pipeline-profiles", "").await;
    assert_eq!(status, 200);
    assert_eq!(profiles.as_array().expect("profiles array").len(), 2);

    let (status, selection) = call(
        addr,
        "POST",
        "/api/voice/pipeline-profile",
        r#"{"profile_id":"pipeline-seed-low-latency"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(selection["profile_id"], "pipeline-seed-low-latency");
    let (status, _) = call(
        addr,
        "POST",
        "/api/voice/pipeline-profile",
        r#"{"profile_id":"no-such-profile"}"#,
    )
    .await;
    assert_eq!(status, 404);

    let (status, customs) = call(addr, "GET", "/api/voice/custom-voices", "").await;
    assert_eq!(status, 200);
    let customs = customs.as_array().expect("custom array");
    assert_eq!(customs.len(), 2);
    assert_eq!(customs[0]["saved_voice_id"], "saved-voice-1");

    let (status, renamed) = call(
        addr,
        "PATCH",
        "/api/voice/custom-voices/saved-voice-1",
        r#"{"name":"Renamed One"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(renamed["name"], "Renamed One");
    let (status, _) = call(
        addr,
        "PATCH",
        "/api/voice/custom-voices/saved-voice-1",
        r#"{"name":"   "}"#,
    )
    .await;
    assert_eq!(status, 400);
    let (status, _) = call(
        addr,
        "PATCH",
        "/api/voice/custom-voices/no-such-voice",
        r#"{"name":"X"}"#,
    )
    .await;
    assert_eq!(status, 404);

    let (status, deleted) = call(addr, "DELETE", "/api/voice/custom-voices/saved-voice-2", "").await;
    assert_eq!(status, 200);
    assert_eq!(deleted["deleted"], true);
    let (status, _) = call(addr, "DELETE", "/api/voice/custom-voices/saved-voice-2", "").await;
    assert_eq!(status, 200);
    let (status, _) = call(
        addr,
        "PATCH",
        "/api/voice/custom-voices/saved-voice-2",
        r#"{"name":"X"}"#,
    )
    .await;
    assert_eq!(status, 404);
    let (status, customs) = call(addr, "GET", "/api/voice/custom-voices", "").await;
    assert_eq!(status, 200);
    let customs = customs.as_array().expect("custom array 2");
    assert_eq!(customs.len(), 1);
    assert_eq!(customs[0]["saved_voice_id"], "saved-voice-1");

    let (status, active) = call(addr, "GET", "/api/voice/active-voice", "").await;
    assert_eq!(status, 200);
    assert_eq!(active["voice_id"], serde_json::Value::Null);
    let (status, _) = call(
        addr,
        "POST",
        "/api/voice/active-voice",
        r#"{"voice_id":"saved-voice-1"}"#,
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = call(
        addr,
        "POST",
        "/api/voice/active-voice",
        r#"{"voice_id":"no-such-voice"}"#,
    )
    .await;
    assert_eq!(status, 404);
    let (status, active) = call(addr, "GET", "/api/voice/active-voice", "").await;
    assert_eq!(status, 200);
    assert_eq!(active["voice_id"], "saved-voice-1");

    let (status, reset) = call(addr, "POST", "/api/voice/onboarding/reset", "{}").await;
    assert_eq!(status, 200);
    assert_eq!(reset["voice_id"], serde_json::Value::Null);
    assert_eq!(reset["profile_id"], serde_json::Value::Null);
    let (status, active) = call(addr, "GET", "/api/voice/active-voice", "").await;
    assert_eq!(status, 200);
    assert_eq!(active["voice_id"], serde_json::Value::Null);
    // Reset touches only the two selections: the renamed
    // custom voice is still there.
    let (status, customs) = call(addr, "GET", "/api/voice/custom-voices", "").await;
    assert_eq!(status, 200);
    let customs = customs.as_array().expect("custom array 3");
    assert_eq!(customs.len(), 1);
    assert_eq!(customs[0]["name"], "Renamed One");

    // Restart recovery: reload the same state file and
    // confirm the rename, the tombstone, and the cleared
    // selections all survived.
    let path = std::env::temp_dir().join(format!("bu2agent-voice-smoke-{}.jsonl", std::process::id()));
    let state = AppState::default().with_voice_state_path(path);
    let (addr, handle2) = start_server(state).await;
    let (status, customs) = call(addr, "GET", "/api/voice/custom-voices", "").await;
    assert_eq!(status, 200);
    let customs = customs.as_array().expect("custom array restart");
    assert_eq!(customs.len(), 1);
    assert_eq!(customs[0]["name"], "Renamed One");
    let (status, active) = call(addr, "GET", "/api/voice/active-voice", "").await;
    assert_eq!(status, 200);
    assert_eq!(active["voice_id"], serde_json::Value::Null);

    handle.abort();
    handle2.abort();
}
