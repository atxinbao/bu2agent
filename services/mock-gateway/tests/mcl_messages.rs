//! MCL-1 tests (plan §3, M1–M6): iMessage threads-only
//! sync — add / skip / tombstone-no-resurrect, cursor
//! advance, restart consistency, disconnect read
//! unaffected, redact no plaintext, loop tool lists
//! threads. M0 is the registration unit test (the six
//! loop-tool points, without a server).
//!
//! Tests use a mutable fake source only; never the real
//! Messages.app. Cross-platform: temp dirs, ports,
//! SQLite only. No kill command, no POSIX signals, no
//! Unix-only path spellings. JSON bodies are built with
//! `json!` (the test-hygiene skill); time budgets are
//! generous (60 s client timeout, no ms-class bounds).

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::imessage_source::{
    participant_hash_for, IMessageError, IMessageSource, IMessageThreadView,
};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use mock_gateway::tool_requires_approval;
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-mcl1-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct ScriptModel {
    steps: Mutex<VecDeque<ModelReply>>,
    contexts: Mutex<Vec<Vec<Message>>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(replies.into()),
            contexts: Mutex::new(vec![]),
        }
    }
}

impl LoopModel for ScriptModel {
    fn name(&self) -> &str {
        "scripted-test"
    }

    fn complete(
        &self,
        messages: Vec<Message>,
        _timeout: Duration,
    ) -> BackendFuture<Result<ModelReply, ModelError>> {
        self.contexts.lock().unwrap().push(messages);
        let reply = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move { Ok(reply) })
    }
}

fn proposal(tool: &str, args: Value) -> Proposal {
    Proposal {
        tool: tool.into(),
        args,
        wire: None,
    }
}

fn calls(items: Vec<Proposal>) -> ModelReply {
    ModelReply::Calls {
        proposals: items,
        content: None,
    }
}

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

/// Mutable fake iMessage source: the test controls the
/// thread set (add a thread between syncs for M2).
/// Handles are fictional (555-01xx numbers, .invalid
/// domains) and never leave this file — only their
/// hashes reach the gateway.
struct MutableFakeIMessageSource {
    threads: Mutex<Vec<IMessageThreadView>>,
}

impl MutableFakeIMessageSource {
    fn seeded() -> Arc<Self> {
        Arc::new(Self {
            threads: Mutex::new(vec![
                thread_view("chat-a", "iMessage", &["+15550101001"]),
                thread_view("chat-b", "SMS", &["+15550101002", "friend@mock.invalid"]),
            ]),
        })
    }

    fn add(&self, view: IMessageThreadView) {
        self.threads.lock().unwrap().push(view);
    }
}

impl IMessageSource for MutableFakeIMessageSource {
    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<IMessageThreadView>, IMessageError> {
        let views = self.threads.lock().unwrap().clone();
        Ok(views.into_iter().skip(offset).take(limit).collect())
    }
}

fn thread_view(native_id: &str, service: &str, handles: &[&str]) -> IMessageThreadView {
    let handles: Vec<String> = handles.iter().map(|h| h.to_string()).collect();
    let participant_count = handles.len();
    IMessageThreadView {
        native_id: native_id.to_string(),
        service: service.to_string(),
        participant_hash: participant_hash_for(&handles),
        participant_count,
        is_group: participant_count > 1,
    }
}

fn build_state(dir: &Path, model: Arc<ScriptModel>, source: Arc<MutableFakeIMessageSource>) -> AppState {
    let state = AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_tasks_path(dir.join("tasks.jsonl"))
    .with_task_drafts_dir(dir.join("drafts"))
    .with_loop_backends(
        Some(dir.join("loops.jsonl")),
        Some(model),
        None,
        Timeouts::default(),
    )
    .unwrap()
    .with_deepseek(mock_gateway::deepseek::DeepSeekClient::new(
        mock_gateway::deepseek::DeepSeekConfig {
            base_url: "http://127.0.0.1:1".to_string(),
            model: "fixture".to_string(),
            api_key: Some("fake-mcl1-key".to_string()),
        },
    ))
    .with_auto_drive(false)
    .with_executions_path(dir.join("executions.jsonl"))
    .with_effects_path(dir.join("effects.jsonl"))
    .with_evidence_path(dir.join("evidence.jsonl"))
    .with_binding_state_path(dir.join("bindings.jsonl"))
    .with_project_state_path(dir.join("projects.jsonl"))
    .with_location_state_path(dir.join("locations.jsonl"))
    .with_worktree_state_path(dir.join("worktrees.jsonl"))
    .with_imessage_source(source)
    .with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    state
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: Option<tokio::task::JoinHandle<()>>,
    model: Arc<ScriptModel>,
    source: Arc<MutableFakeIMessageSource>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let source = MutableFakeIMessageSource::seeded();
        let state = build_state(&dir, model.clone(), source.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            server: Some(server),
            model,
            source,
        }
    }

    /// Restart the gateway on the same directory (same
    /// SQLite file, same JSONL files, same fake source):
    /// the production restart path, not a fresh database.
    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let model = Arc::new(ScriptModel::new(vec![]));
        self.model = model.clone();
        let state = build_state(&self.dir, model, self.source.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server = Some(
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() }),
        );
    }

    fn db(&self) -> rusqlite::Connection {
        let conn = sqlite_store::open_db(&self.dir.join("gateway.sqlite3")).expect("open db");
        conn.busy_timeout(Duration::from_secs(5))
            .expect("busy timeout");
        conn
    }

    async fn post(&self, path: &str, value: Value) -> (u16, Value) {
        let response = self
            .client
            .post(format!("{}{}", self.url, path))
            .json(&value)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let response = self
            .client
            .get(format!("{}{}", self.url, path))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{path}: {body}");
        body
    }

    async fn ok_get(&self, path: &str) -> Value {
        let (status, body) = self.get(path).await;
        assert_eq!(status, 200, "{path}: {body}");
        body
    }

    async fn sync(&self) -> Value {
        self.ok("/api/imessage/sync", json!({})).await
    }

    async fn start(&self) -> String {
        self.ok("/agent/loop/start", json!({"prompt": "list the message threads"}))
            .await["loop_id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id": id})).await
    }

    /// Step until the loop reaches a terminal status (bounded).
    async fn drive(&self, id: &str) -> Value {
        let mut last = Value::Null;
        for _ in 0..16 {
            last = self.step(id).await;
            let status = last["status"].as_str().unwrap_or("");
            if matches!(status, "completed" | "failed" | "cancelled") {
                break;
            }
        }
        last
    }

    /// Tool results the scripted model has seen, deduped by call
    /// id (message history repeats them in later contexts).
    fn tool_messages(&self) -> Vec<(String, bool)> {
        let mut out: Vec<(usize, String, bool)> = Vec::new();
        for messages in self.model.contexts.lock().unwrap().iter() {
            for m in messages {
                if let Message::Tool {
                    call_id,
                    result,
                    ..
                } = m
                {
                    if !out.iter().any(|(id, _, _)| id == call_id) {
                        out.push((*call_id, result.body.clone(), result.summary.truncated));
                    }
                }
            }
        }
        out.into_iter()
            .map(|(_, body, truncated)| (body, truncated))
            .collect()
    }

    async fn effects(&self) -> Vec<Value> {
        self.ok_get("/effects/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }
}

fn imessage_event_payloads(h: &Harness) -> Vec<String> {
    let conn = h.db();
    let mut stmt = conn
        .prepare(
            "SELECT payload_json FROM events WHERE aggregate_kind = 'imessage' \
             AND event_type = 'imessage.synced' ORDER BY seq",
        )
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn source_cursor(h: &Harness) -> String {
    let conn = h.db();
    conn.query_row(
        "SELECT cursor FROM sources WHERE source_id = 'imessage-source'",
        [],
        |row| row.get::<_, String>(0),
    )
    .unwrap()
}

// ---------------------------------------------------------------- M0

#[test]
fn m0_loop_tool_registration_points() {
    // lib.rs: a read, no approval.
    assert_eq!(tool_requires_approval("imessage.threads"), Some(false));
    // loop_engine.rs: the loop's own gate agrees.
    assert!(!loop_tool_requires_approval("imessage.threads"));
    // loop_engine.rs normalize: defaults, clamp, reject.
    let norm = normalize("imessage.threads", &json!({})).unwrap();
    assert_eq!(norm["limit"], "50");
    assert_eq!(norm["offset"], "0");
    let norm = normalize("imessage.threads", &json!({"limit": 500})).unwrap();
    assert_eq!(norm["limit"], "200", "clamps like the list route");
    assert!(normalize("imessage.threads", &json!({"bogus": 1})).is_err());
}

// ---------------------------------------------------------------- M1

#[tokio::test]
async fn m1_sync_add_skip_tombstone_no_resurrect() {
    let h = Harness::serve(vec![]).await;
    // First sync adds both seeded threads.
    let first = h.sync().await;
    assert_eq!(first["synced_count"], 2);
    assert_eq!(first["skipped_count"], 0);
    assert_eq!(first["total_count"], 2);
    let cursor = first["cursor"].as_str().unwrap().to_string();
    assert!(!cursor.is_empty());
    assert_eq!(source_cursor(&h), cursor);

    // The list carries the thread identity only; the
    // message fields stay NULL (never fabricated).
    let threads = h.ok_get("/api/imessage/threads").await;
    let rows = threads.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["thread_id"], "thread-1");
    assert_eq!(rows[0]["native_id"], "chat-a");
    assert_eq!(rows[0]["service"], "iMessage");
    assert_eq!(rows[0]["is_group"], false);
    assert_eq!(rows[0]["participant_count"], 1);
    assert_eq!(
        rows[0]["participant_hash"],
        participant_hash_for(&["+15550101001".to_string()])
    );
    assert_eq!(rows[0]["last_message_ms"], Value::Null);
    assert_eq!(rows[0]["unread_count"], Value::Null);
    assert_eq!(rows[1]["thread_id"], "thread-2");
    assert_eq!(rows[1]["service"], "SMS");
    assert_eq!(rows[1]["is_group"], true);
    assert_eq!(rows[1]["participant_count"], 2);

    // Get one row; unknown id 404s.
    let one = h.ok_get("/api/imessage/threads/thread-1").await;
    assert_eq!(one["native_id"], "chat-a");
    let (status, _) = h.get("/api/imessage/threads/thread-99").await;
    assert_eq!(status, 404);

    // Second sync skips both; the cursor does not move.
    let second = h.sync().await;
    assert_eq!(second["synced_count"], 0);
    assert_eq!(second["skipped_count"], 2);
    assert_eq!(second["total_count"], 2);
    assert_eq!(second["cursor"].as_str().unwrap(), cursor);

    // Exactly two `imessage.synced` events — one per
    // thread, none for the skipped re-syncs.
    assert_eq!(imessage_event_payloads(&h).len(), 2);

    // Tombstone thread-1 by direct SQL (there is no
    // delete route in MCL-1); the next sync must NOT
    // resurrect it.
    h.db()
        .execute(
            "UPDATE imessage_threads SET deleted = 1 WHERE thread_id = 'thread-1'",
            [],
        )
        .unwrap();
    let third = h.sync().await;
    assert_eq!(third["synced_count"], 0, "tombstone must not resurrect");
    assert_eq!(third["skipped_count"], 2);
    assert_eq!(third["total_count"], 2);
    assert_eq!(third["cursor"].as_str().unwrap(), cursor);
    let rows = h.ok_get("/api/imessage/threads").await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2, "tombstones stay visible");
    assert_eq!(rows[0]["deleted"], true);
    let one = h.ok_get("/api/imessage/threads/thread-1").await;
    assert_eq!(one["deleted"], true, "get returns the tombstone");
    assert_eq!(imessage_event_payloads(&h).len(), 2, "no new events");
}

// ---------------------------------------------------------------- M2

#[tokio::test]
async fn m2_cursor_advances_when_thread_set_changes() {
    let h = Harness::serve(vec![]).await;
    let first = h.sync().await;
    let cursor1 = first["cursor"].as_str().unwrap().to_string();
    assert_eq!(source_cursor(&h), cursor1);

    // A new thread appears in the live source.
    h.source
        .add(thread_view("chat-c", "iMessage", &["+15550101003"]));
    let second = h.sync().await;
    assert_eq!(second["synced_count"], 1);
    assert_eq!(second["skipped_count"], 2);
    assert_eq!(second["total_count"], 3);
    let cursor2 = second["cursor"].as_str().unwrap().to_string();
    assert_ne!(cursor2, cursor1, "cursor must advance on a new thread");
    assert_eq!(source_cursor(&h), cursor2);

    let rows = h.ok_get("/api/imessage/threads").await;
    assert_eq!(rows.as_array().unwrap().len(), 3);
    assert_eq!(rows[2]["thread_id"], "thread-3");
    assert_eq!(rows[2]["native_id"], "chat-c");
}

// ---------------------------------------------------------------- M3

#[tokio::test]
async fn m3_restart_keeps_rows_and_cursor() {
    let mut h = Harness::serve(vec![]).await;
    let first = h.sync().await;
    let cursor = first["cursor"].as_str().unwrap().to_string();
    let before = h.ok_get("/api/imessage/threads").await;

    h.restart().await;

    let after = h.ok_get("/api/imessage/threads").await;
    assert_eq!(after, before, "rows must survive a restart");
    assert_eq!(source_cursor(&h), cursor, "cursor must survive a restart");

    // A post-restart sync is idempotent, not a replay.
    let again = h.sync().await;
    assert_eq!(again["synced_count"], 0);
    assert_eq!(again["skipped_count"], 2);
    assert_eq!(again["cursor"].as_str().unwrap(), cursor);
    assert_eq!(imessage_event_payloads(&h).len(), 2);
}

// ---------------------------------------------------------------- M4

#[tokio::test]
async fn m4_disconnect_leaves_reads_unaffected() {
    let h = Harness::serve(vec![]).await;
    h.sync().await;

    let disconnected = h
        .ok("/sources/imessage-source/disconnect", json!({}))
        .await;
    assert_eq!(disconnected["status"], "disconnected");

    // List + get still read (6b Q-24: disconnect is a
    // flag flip only; reads unaffected).
    let rows = h.ok_get("/api/imessage/threads").await;
    assert_eq!(rows.as_array().unwrap().len(), 2);
    let one = h.ok_get("/api/imessage/threads/thread-1").await;
    assert_eq!(one["native_id"], "chat-a");

    // Sync is fact recording, like mail sync: it still
    // works after disconnect (only writes are gated).
    let again = h.sync().await;
    assert_eq!(again["synced_count"], 0);
    assert_eq!(again["skipped_count"], 2);
}

// ---------------------------------------------------------------- M5

#[tokio::test]
async fn m5_redact_keeps_no_handle_or_message_field() {
    let h = Harness::serve(vec![]).await;
    h.sync().await;

    // No event payload names a handle, a name, or any
    // message field (MCL-1 has no message rows at all).
    for payload in imessage_event_payloads(&h) {
        assert!(!payload.contains("+15550101001"), "{payload}");
        assert!(!payload.contains("+15550101002"), "{payload}");
        assert!(!payload.contains("friend@mock.invalid"), "{payload}");
        assert!(!payload.contains("message_id"), "{payload}");
        assert!(!payload.contains("body_hash"), "{payload}");
        assert!(!payload.contains("sender_hash"), "{payload}");
        assert!(!payload.contains("attachment_"), "{payload}");
        assert!(payload.contains("participant_hash"), "{payload}");
        assert!(payload.contains("\"service\""), "{payload}");
    }

    // The list response likewise carries no handle.
    let list = h.ok_get("/api/imessage/threads").await;
    let serialized = serde_json::to_string(&list).unwrap();
    assert!(!serialized.contains("+15550101001"), "{serialized}");
    assert!(!serialized.contains("friend@mock.invalid"), "{serialized}");
}

// ---------------------------------------------------------------- M6

#[tokio::test]
async fn m6_loop_tool_lists_threads() {
    let h = Harness::serve(vec![
        calls(vec![proposal("imessage.threads", json!({}))]),
        done(),
    ])
    .await;
    let loop_id = h.start().await;
    let final_view = h.drive(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    let seen = h.tool_messages();
    assert_eq!(seen.len(), 1, "{seen:?}");
    let (body, truncated) = &seen[0];
    assert!(!truncated);
    assert!(body.contains("thread: chat-a"), "{body}");
    assert!(body.contains("service: iMessage"), "{body}");
    assert!(body.contains("participants: 1"), "{body}");
    assert!(body.contains("group: false"), "{body}");
    assert!(body.contains("thread: chat-b"), "{body}");
    assert!(body.contains("service: SMS"), "{body}");
    assert!(body.contains("participants: 2"), "{body}");
    assert!(body.contains("group: true"), "{body}");
    // The participant hash is the citation anchor; the
    // raw handle never reaches the model.
    assert!(
        body.contains(&participant_hash_for(&["+15550101001".to_string()])),
        "{body}"
    );
    assert!(!body.contains("+15550101001"), "{body}");
    assert!(!body.contains("friend@mock.invalid"), "{body}");
    // Reads produce no Effect.
    assert!(h.effects().await.is_empty());
}
