//! Smoke tests for Task 20 chat stream form subset:
//! subscribe, stream-events (replay, cursor resume, long
//! poll), cancel, stream open, poke, and sxs.select, plus
//! restart recovery through stream.jsonl.
//!
//! Events are a projection of visible turns plus pokes
//! (see stream_store.rs), not a live token stream. The
//! spec's stream/stream-events division, poke, and
//! sxs.select have no runtime evidence; their local
//! semantics come from the Task 20 plan amendment.

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

async fn call(addr: std::net::SocketAddr, method: &str, path: &str, body: &str) -> (u16, serde_json::Value) {
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let (head, resp_body) = raw(addr, &request).await;
    let status = head.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0);
    let value: serde_json::Value = serde_json::from_str(&resp_body).unwrap_or(serde_json::Value::Null);
    (status, value)
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-stream-{name}-{ }", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

#[tokio::test]
async fn stream_form_flow_and_restart() {
    let dir = temp_root("smoke");
    let sessions_path = dir.join("sessions.jsonl");
    let stream_path = dir.join("stream.jsonl");
    let state = AppState::default()
        .with_sessions_path(sessions_path.clone())
        .with_stream_state_path(stream_path.clone());
    let (addr, handle) = start_server(state).await;

    // One session, two exchanges (disabled DeepSeek echoes):
    // turns 0..=3, assistant turns at 1 and 3.
    let (status, created) = call(addr, "POST", "/sessions/create", r#"{"title":"stream test"}"#).await;
    assert_eq!(status, 200);
    let thread = created["session_id"].as_str().expect("session id").to_string();
    for text in ["hello", "world"] {
        let body = serde_json::json!({ "session_id": thread, "text": text }).to_string();
        let (status, _) = call(addr, "POST", "/sessions/send", &body).await;
        assert_eq!(status, 200);
    }

    // Subscribe: unknown thread 404; known thread gets sub-1
    // with a cursor caught up to the last event.
    let (status, _) = call(addr, "POST", "/chat/subscribe", r#"{"thread_id":"nope"}"#).await;
    assert_eq!(status, 404);
    let (status, sub) =
        call(addr, "POST", "/chat/subscribe", &format!(r#"{{"thread_id":"{thread}"}}"#)).await;
    assert_eq!(status, 200);
    assert_eq!(sub["subscription_id"], "sub-1");
    assert_eq!(sub["cursor"], "turn:3:done");

    // Replay from the start: 2 assistant turns x 3 events.
    let (status, page) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-1"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(page["cancelled"], false);
    let events = page["events"].as_array().expect("events array");
    assert_eq!(events.len(), 6);
    assert_eq!(events[0]["type"], "delta.turn_start");
    assert_eq!(events[1]["type"], "delta.text_append");
    assert_eq!(events[1]["text"], "hello");
    assert_eq!(events[1]["turn_index"], 1);
    assert_eq!(events[4]["text"], "world");
    assert_eq!(page["cursor"], "turn:3:done");

    // Caught-up cursor yields nothing; unknown subscription
    // 404s; unknown cursor 400s (never silently replayed).
    let (status, page) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-1","cursor":"turn:3:done"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(page["events"].as_array().unwrap().len(), 0);
    let (status, _) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-9"}"#,
    )
    .await;
    assert_eq!(status, 404);
    let (status, _) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-1","cursor":"turn:9:start"}"#,
    )
    .await;
    assert_eq!(status, 400);

    // Stream open mints stream-1; poke injects an agent_status
    // event that a resume from the old cursor picks up once.
    let (status, opened) =
        call(addr, "POST", "/chat/stream", &format!(r#"{{"thread_id":"{thread}"}}"#)).await;
    assert_eq!(status, 200);
    assert_eq!(opened["stream_id"], "stream-1");
    let (status, poked) =
        call(addr, "POST", "/chat/poke", &format!(r#"{{"thread_id":"{thread}"}}"#)).await;
    assert_eq!(status, 200);
    assert_eq!(poked["event_id"], "poke:1");
    let (status, page) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-1","cursor":"turn:3:done"}"#,
    )
    .await;
    assert_eq!(status, 200);
    let events = page["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["type"], "delta.agent_status");
    assert_eq!(events[0]["id"], "poke:1");
    assert_eq!(page["cursor"], "poke:1");

    // Long-poll with a caught-up cursor returns empty rather
    // than hanging.
    let (status, page) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-1","cursor":"poke:1","wait_ms":200}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(page["events"].as_array().unwrap().len(), 0);
    assert_eq!(page["cancelled"], false);

    // sxs.select: visible turn ok, unknown index 404.
    let (status, selected) = call(
        addr,
        "POST",
        "/api/chat/sxs/select",
        &format!(r#"{{"thread_id":"{thread}","index":1}}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(selected["index"], 1);
    let (status, _) = call(
        addr,
        "POST",
        "/api/chat/sxs/select",
        &format!(r#"{{"thread_id":"{thread}","index":99}}"#),
    )
    .await;
    assert_eq!(status, 404);

    // Tombstone turn 1: a fresh replay skips its events, and
    // selecting it now 404s.
    let (status, _) = call(
        addr,
        "POST",
        "/api/chat/delete-turn",
        &format!(r#"{{"thread_id":"{thread}","index":1}}"#),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = call(
        addr,
        "POST",
        "/api/chat/sxs/select",
        &format!(r#"{{"thread_id":"{thread}","index":1}}"#),
    )
    .await;
    assert_eq!(status, 404);
    let (status, page) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-1"}"#,
    )
    .await;
    assert_eq!(status, 200);
    let events = page["events"].as_array().expect("events array");
    assert_eq!(events.len(), 4);
    assert!(events.iter().all(|e| e["turn_index"] != 1));

    // Cancel is thread-level and idempotent; a cancelled
    // subscription reports cancelled with no events.
    let (status, cancelled) =
        call(addr, "POST", "/chat/cancel", &format!(r#"{{"thread_id":"{thread}"}}"#)).await;
    assert_eq!(status, 200);
    assert_eq!(cancelled["cancelled_subscriptions"], 1);
    let (status, cancelled) =
        call(addr, "POST", "/chat/cancel", &format!(r#"{{"thread_id":"{thread}"}}"#)).await;
    assert_eq!(status, 200);
    assert_eq!(cancelled["cancelled_subscriptions"], 0);
    let (status, page) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-1"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(page["cancelled"], true);
    assert_eq!(page["events"].as_array().unwrap().len(), 0);

    // Restart: numbering continues (sub-2, stream-2),
    // cancellation survives, tombstone stays hidden.
    handle.abort();
    let state = AppState::default()
        .with_sessions_path(sessions_path.clone())
        .with_stream_state_path(stream_path.clone());
    let (addr, handle) = start_server(state).await;
    let (status, page) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-1"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(page["cancelled"], true);
    let (status, sub) =
        call(addr, "POST", "/chat/subscribe", &format!(r#"{{"thread_id":"{thread}"}}"#)).await;
    assert_eq!(status, 200);
    assert_eq!(sub["subscription_id"], "sub-2");
    let (status, opened) =
        call(addr, "POST", "/chat/stream", &format!(r#"{{"thread_id":"{thread}"}}"#)).await;
    assert_eq!(status, 200);
    assert_eq!(opened["stream_id"], "stream-2");
    let (status, page) = call(
        addr,
        "POST",
        "/chat/stream-events",
        r#"{"subscription_id":"sub-2"}"#,
    )
    .await;
    assert_eq!(status, 200);
    let events = page["events"].as_array().expect("events array");
    assert_eq!(events.len(), 4);
    assert!(events.iter().all(|e| e["turn_index"] != 1));
    handle.abort();
}
