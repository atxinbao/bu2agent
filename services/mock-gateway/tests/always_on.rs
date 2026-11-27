//! AO-1 tests (plan §3, Q-AO-04/05/06/17): the event
//! inbox — ingest + dedupe (A1), claim atomicity
//! (A2), the single-session gate (A3), and failure
//! handling (A4).
//!
//! AO-2 tests (plan §4, Q-AO-07/17): the general
//! timers — follow-up fires once (B1), schedule
//! advances and refires (B2), revoke never fires
//! (B3), and restart keeps unexpired timers while
//! the first post-restart drive fires expired ones
//! once (B4).
//!
//! AO-5 tests (plan §5, Q-AO-11/12/13): the Outbox
//! Dispatcher — same-topic merge within 10 minutes
//! (E1), the 20-per-day cap holding the 21st row
//! (E2), quiet-hours hold + the next-morning digest
//! (E3), and the 1/5/15-minute retry backoff ending
//! in failed after the 4th attempt (E4). All four
//! drive the deliverers with fixed clocks, so the
//! hour the suite runs in cannot change the outcome.
//!
//! The harness serves with no sweeper (the
//! `e2d_long_tasks` precedent for
//! `start_due_worker_runs`): no drive call happens
//! except the test's own, so each phase transition
//! is deterministic. The store-level claim /
//! complete / fail calls go through fresh
//! connections to the same DB file, like the
//! `mcl_contact_write` precedent. The fake DeepSeek
//! listener accepts and drops connections — the
//! ingest route never calls the model, so it is
//! never hit. The fake key is `test-key`; no real
//! credential is used.
//!
//! AO-4 tests (plan §6, Q-AO-10): per-round memory
//! loading + dreaming — loop_start loads the user
//! instruction layer (D1), over-budget rows degrade
//! to index lines (D2), and the dreaming pass writes
//! a summary row while tombstoning the older exact
//! duplicate (D3). D1/D2 need to see what the
//! gateway actually sends the model, so they serve
//! a recording fake DeepSeek (it stores each
//! `/chat/completions` body and answers one final
//! text reply); the system prompt is not observable
//! through `GET /agent/loop/get`, which carries no
//! messages — the recorded model request is the
//! honest observation point.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{extract::State, routing::post, Json, Router};
use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::deepseek_loop::DeepSeekLoopModel;
use mock_gateway::loop_engine::Timeouts;
use mock_gateway::memory_store::{self, DreamSession, MemoryRow, MemorySource};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use reqwest::StatusCode;
use rusqlite::OptionalExtension;
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-ao1-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start_fake_deepseek() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            drop(socket);
        }
    });
    format!("http://{addr}")
}

/// AO-4: a recording fake DeepSeek — it stores each
/// `/chat/completions` body (so D1/D2 can assert what
/// the gateway actually sent the model) and answers
/// one final-text reply with no tool calls, so the
/// loop completes.
async fn recording_completion(
    State(requests): State<Arc<Mutex<Vec<Value>>>>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    requests.lock().unwrap().push(body);
    (
        StatusCode::OK,
        Json(json!({"choices":[{"message":{"role":"assistant","content":"done","tool_calls":null},"finish_reason":"stop"}]})),
    )
}

async fn start_recording_deepseek() -> (String, Arc<Mutex<Vec<Value>>>) {
    let requests: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route("/chat/completions", post(recording_completion))
        .with_state(requests.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), requests)
}

/// AO-3: a scripted fake DeepSeek — it serves the
/// given replies in order (one per model request,
/// falling back to the recording fake's final-text
/// reply once the script runs out) and records every
/// request body, so C3/C6/C7 can script the model's
/// exact replies (a refused write proposal, a giant
/// final text).
#[derive(Clone)]
struct ScriptedFake {
    replies: Arc<Vec<Value>>,
    next: Arc<Mutex<usize>>,
    requests: Arc<Mutex<Vec<Value>>>,
}

async fn scripted_completion(
    State(shared): State<ScriptedFake>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    shared.requests.lock().unwrap().push(body);
    let mut index = shared.next.lock().unwrap();
    let reply = shared.replies.get(*index).cloned().unwrap_or_else(|| {
        json!({"choices":[{"message":{"role":"assistant","content":"done","tool_calls":null},"finish_reason":"stop"}]})
    });
    *index += 1;
    (StatusCode::OK, Json(reply))
}

async fn start_scripted_deepseek(replies: Vec<Value>) -> (String, Arc<Mutex<Vec<Value>>>) {
    let fake = ScriptedFake {
        replies: Arc::new(replies),
        next: Arc::new(Mutex::new(0)),
        requests: Arc::new(Mutex::new(Vec::new())),
    };
    let requests = fake.requests.clone();
    let app = Router::new()
        .route("/chat/completions", post(scripted_completion))
        .with_state(fake);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), requests)
}

fn build_state(dir: &Path, deepseek_base_url: &str, auto_drive: bool) -> AppState {
    let config = DeepSeekConfig {
        base_url: deepseek_base_url.to_string(),
        model: "fixture".to_string(),
        api_key: Some("test-key".to_string()),
    };
    // AO-4: the loop engine needs a model backend, or
    // loop_start 503s with loop_model_not_configured.
    // The model points at the (fake) DeepSeek URL, so
    // D1/D2 reach the recording fake; A/B/E tests never
    // start a loop, so this changes nothing for them.
    let model = DeepSeekLoopModel::new(config.clone())
        .expect("loop model builds")
        .expect("test key configures a loop model");
    let state = AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_tasks_path(dir.join("tasks.jsonl"))
    .with_task_drafts_dir(dir.join("drafts"))
    .with_deepseek(DeepSeekClient::new(config))
    .with_loop_backends(
        Some(dir.join("loops.jsonl")),
        Some(std::sync::Arc::new(model)),
        None,
        Timeouts::default(),
    )
    .expect("loop backends")
    // AO-3: C-tests drive wake Runs through the real
    // driver path (spawn_driver + waiter), so the
    // driven harness leaves auto-drive on; A/B/D/E
    // tests keep it off for determinism.
    .with_auto_drive(auto_drive)
    .with_executions_path(dir.join("executions.jsonl"))
    .with_effects_path(dir.join("effects.jsonl"))
    .with_evidence_path(dir.join("evidence.jsonl"))
    .with_binding_state_path(dir.join("bindings.jsonl"))
    .with_project_state_path(dir.join("projects.jsonl"))
    .with_location_state_path(dir.join("locations.jsonl"))
    .with_worktree_state_path(dir.join("worktrees.jsonl"))
    .with_model_state_path(dir.join("model.jsonl"));
    let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    state
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    deepseek_base_url: String,
    #[allow(dead_code)]
    server: Option<tokio::task::JoinHandle<()>>,
    #[allow(dead_code)]
    state: AppState,
    /// AO-4: bodies the recording fake DeepSeek saw
    /// (empty for `serve`, which drops connections).
    model_requests: Arc<Mutex<Vec<Value>>>,
    /// AO-3: whether this harness leaves the loop
    /// auto-driver on (driven harnesses run wake Runs
    /// through the real spawn_driver + waiter path).
    auto_drive: bool,
}

impl Harness {
    /// Shared serve body: build state on `dir` with the
    /// given model, serve the app, keep the state for
    /// direct drive calls (`drive_due_timers`,
    /// `drive_wake_events`).
    async fn serve_from(
        dir: PathBuf,
        deepseek_base_url: String,
        model_requests: Arc<Mutex<Vec<Value>>>,
        auto_drive: bool,
    ) -> Self {
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let state = build_state(&dir, &deepseek_base_url, auto_drive);
        let harness_state = state.clone();
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
            deepseek_base_url,
            server: Some(server),
            state: harness_state,
            model_requests,
            auto_drive,
        }
    }

    async fn serve() -> Self {
        let dir = temp_dir("ao1");
        let deepseek_base_url = start_fake_deepseek().await;
        Self::serve_from(
            dir,
            deepseek_base_url,
            Arc::new(Mutex::new(Vec::new())),
            false,
        )
        .await
    }

    /// AO-3: like `serve`, but the loop auto-driver
    /// stays on, so a wake Run's spawn_driver + waiter
    /// take the real production path.
    async fn serve_driven() -> Self {
        let dir = temp_dir("ao3");
        let deepseek_base_url = start_fake_deepseek().await;
        Self::serve_from(
            dir,
            deepseek_base_url,
            Arc::new(Mutex::new(Vec::new())),
            true,
        )
        .await
    }

    /// AO-4: like `serve`, but the fake DeepSeek
    /// records every model request and answers one
    /// final-text reply, so D1/D2 can assert what the
    /// gateway sent the model.
    async fn serve_recording() -> Self {
        let dir = temp_dir("ao4");
        let (deepseek_base_url, model_requests) = start_recording_deepseek().await;
        Self::serve_from(dir, deepseek_base_url, model_requests, false).await
    }

    /// AO-3: the recording fake with the auto-driver
    /// on — the C1/C2/C5 harness.
    async fn serve_recording_driven() -> Self {
        let dir = temp_dir("ao3");
        let (deepseek_base_url, model_requests) = start_recording_deepseek().await;
        Self::serve_from(dir, deepseek_base_url, model_requests, true).await
    }

    /// AO-3: the scripted fake (replies served in
    /// order) with the auto-driver off — the C7
    /// foreground harness, driven by `loop_resume`.
    async fn serve_scripted(replies: Vec<Value>) -> Self {
        let dir = temp_dir("ao3");
        let (deepseek_base_url, model_requests) = start_scripted_deepseek(replies).await;
        Self::serve_from(dir, deepseek_base_url, model_requests, false).await
    }

    /// AO-3: the scripted fake with the auto-driver
    /// on — the C3/C6 harness.
    async fn serve_scripted_driven(replies: Vec<Value>) -> Self {
        let dir = temp_dir("ao3");
        let (deepseek_base_url, model_requests) = start_scripted_deepseek(replies).await;
        Self::serve_from(dir, deepseek_base_url, model_requests, true).await
    }

    fn model_requests(&self) -> Vec<Value> {
        self.model_requests.lock().unwrap().clone()
    }

    /// AO-3: one event row, by id.
    fn event_row(&self, event_id: &str) -> sqlite_store::AgentEventRow {
        let conn = self.db();
        sqlite_store::get_agent_event(&conn, event_id)
            .unwrap()
            .expect("event row")
    }

    /// AO-3: poll an event's status until it reaches
    /// `want` (up to 30 s — a wake Run is bounded by
    /// the 120 s loop budget, but the recording fake
    /// answers at once, so the happy path lands in
    /// well under a second; the long window is for the
    /// failing-model case only). Returns the last status
    /// seen, so a timeout reads as the wrong status in
    /// the caller's assert instead of hanging the test.
    async fn wait_event_status(&self, event_id: &str, want: &str) -> String {
        for _ in 0..300 {
            let status = self.event_row(event_id).status;
            if status == want {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.event_row(event_id).status
    }

    /// AO-3: memory rows across both layers.
    fn count_memories(&self) -> i64 {
        let conn = self.db();
        conn.query_row(
            "SELECT (SELECT COUNT(*) FROM project_memories) + (SELECT COUNT(*) FROM user_memories)",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// AO-3: (memory_id, layer, content) across both
    /// layers, creation order.
    fn memory_rows(&self) -> Vec<(String, String, String)> {
        let conn = self.db();
        let mut rows = Vec::new();
        for table in ["project_memories", "user_memories"] {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT memory_id, layer, content FROM {table} ORDER BY created_ms"
                ))
                .unwrap();
            let mut found = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0).unwrap(),
                        row.get::<_, String>(1).unwrap(),
                        row.get::<_, String>(2).unwrap(),
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            rows.append(&mut found);
        }
        rows.sort_by_key(|(id, _, _)| {
            id.strip_prefix("memory-")
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0)
        });
        rows
    }

    /// AO-3: outbox row count.
    fn count_outbox(&self) -> i64 {
        let conn = self.db();
        conn.query_row("SELECT COUNT(*) FROM outbox", [], |row| row.get(0))
            .unwrap()
    }

    /// AO-3: all outbox rows, creation order.
    fn outbox_rows(&self) -> Vec<sqlite_store::OutboxRow> {
        let conn = self.db();
        let mut stmt = conn
            .prepare(
                "SELECT outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms, deliver_at_ms FROM outbox ORDER BY created_ms",
            )
            .unwrap();
        stmt.query_map([], |row| {
            Ok(sqlite_store::OutboxRow {
                outbox_id: row.get(0)?,
                execution_id: row.get(1)?,
                payload_summary_json: row.get(2)?,
                destination_kind: row.get(3)?,
                status: row.get(4)?,
                attempts: row.get(5)?,
                dedup_key: row.get(6)?,
                created_ms: row.get(7)?,
                updated_at_ms: row.get(8)?,
                deliver_at_ms: row.get(9)?,
            })
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }

    /// AO-3: inbox row count.
    fn count_inbox(&self) -> i64 {
        let conn = self.db();
        conn.query_row("SELECT COUNT(*) FROM inbox", [], |row| row.get(0))
            .unwrap()
    }

    /// AO-3: inbox `source_event_id`s, creation order.
    fn inbox_source_ids(&self) -> Vec<String> {
        let conn = self.db();
        let mut stmt = conn
            .prepare("SELECT source_event_id FROM inbox ORDER BY seq")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    /// AO-3: the workbench loop list (main loops only;
    /// a wake loop is kind "main", so it shows up).
    async fn loop_list(&self) -> (StatusCode, Value) {
        let resp = self
            .client
            .get(format!("{}/agent/loop/list", self.url))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap())
    }

    /// AO-3: one loop view, by id.
    async fn loop_get(&self, loop_id: &str) -> (StatusCode, Value) {
        let resp = self
            .client
            .get(format!("{}/agent/loop/get?loop_id={loop_id}", self.url))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap())
    }

    /// AO-4: seed one memory row through the store
    /// (fresh connection, the `mcl_contact_write`
    /// precedent), with an explicit id and timestamp
    /// so the test controls ordering.
    fn seed_memory(&self, memory_id: &str, layer: &str, content: &str, created_ms: u64) {
        let mut conn = self.db();
        let source = MemorySource {
            session_id: None,
            execution_id: None,
            source_id: None,
            author: "user".to_string(),
            created_ms,
        };
        let row = MemoryRow {
            memory_id: memory_id.to_string(),
            layer: layer.to_string(),
            scope_json: "{}".to_string(),
            content: content.to_string(),
            source_json: source.canonical(),
            created_ms,
            updated_ms: created_ms,
            deleted: false,
            origin_device_id: "test".to_string(),
            schema_version: 1,
        };
        memory_store::insert_memory(&mut conn, &row).unwrap();
    }

    async fn loop_start(&self, body: Value) -> (StatusCode, Value) {
        let resp = self
            .client
            .post(format!("{}/agent/loop/start", self.url))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap())
    }

    async fn loop_resume(&self, loop_id: &str) -> (StatusCode, Value) {
        let resp = self
            .client
            .post(format!("{}/agent/loop/resume", self.url))
            .json(&json!({"loop_id": loop_id}))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap())
    }

    /// Restart the gateway on the same directory (same
    /// SQLite file, same JSONL files): the production
    /// restart path, not a fresh database (the
    /// `orc_plan` precedent).
    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let state = build_state(&self.dir, &self.deepseek_base_url, self.auto_drive);
        self.state = state.clone();
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

    fn schema_version(&self) -> i64 {
        let conn = self.db();
        let raw: String = conn
            .query_row(
                "SELECT value FROM db_meta WHERE key = 'db_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        raw.parse().unwrap()
    }

    async fn ingest(&self, body: Value) -> (StatusCode, Value) {
        let resp = self
            .client
            .post(format!("{}/agent/events/ingest", self.url))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let value: Value = resp.json().await.unwrap();
        (status, value)
    }

    fn count_agent_events(&self) -> i64 {
        let conn = self.db();
        conn.query_row("SELECT COUNT(*) FROM agent_events", [], |row| row.get(0))
            .unwrap()
    }

    fn agent_event_status(&self, event_id: &str) -> String {
        let conn = self.db();
        conn.query_row(
            "SELECT status FROM agent_events WHERE event_id = ?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn audit_event_types(&self, event_id: &str) -> Vec<String> {
        let conn = self.db();
        let mut stmt = conn
            .prepare(
                "SELECT event_type FROM events WHERE aggregate_kind = 'agent_event' AND aggregate_id = ?1 ORDER BY seq",
            )
            .unwrap();
        stmt.query_map([event_id], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn audit_payload(&self, event_id: &str, event_type: &str) -> Value {
        let conn = self.db();
        let raw: String = conn
            .query_row(
                "SELECT payload_json FROM events WHERE aggregate_kind = 'agent_event' AND aggregate_id = ?1 AND event_type = ?2",
                rusqlite::params![event_id, event_type],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    async fn create_timer(&self, body: Value) -> (StatusCode, Value) {
        let resp = self
            .client
            .post(format!("{}/agent/timers", self.url))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let value: Value = resp.json().await.unwrap();
        (status, value)
    }

    async fn list_timers(&self) -> (StatusCode, Value) {
        let resp = self
            .client
            .get(format!("{}/agent/timers", self.url))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let value: Value = resp.json().await.unwrap();
        (status, value)
    }

    async fn delete_timer(&self, timer_id: &str) -> (StatusCode, Value) {
        let resp = self
            .client
            .delete(format!("{}/agent/timers/{timer_id}", self.url))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        let value: Value = resp.json().await.unwrap();
        (status, value)
    }

    fn count_agent_timers(&self) -> i64 {
        let conn = self.db();
        conn.query_row("SELECT COUNT(*) FROM agent_timers", [], |row| row.get(0))
            .unwrap()
    }

    fn timer_status(&self, timer_id: &str) -> String {
        let conn = self.db();
        conn.query_row(
            "SELECT status FROM agent_timers WHERE timer_id = ?1",
            [timer_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn timer_next_fire(&self, timer_id: &str) -> i64 {
        let conn = self.db();
        conn.query_row(
            "SELECT next_fire_ms FROM agent_timers WHERE timer_id = ?1",
            [timer_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn count_timer_events(&self) -> i64 {
        let conn = self.db();
        conn.query_row(
            "SELECT COUNT(*) FROM agent_events WHERE source_kind = 'timers'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn timer_event_payload(&self) -> Value {
        let conn = self.db();
        let raw: String = conn
            .query_row(
                "SELECT payload_summary_json FROM agent_events WHERE source_kind = 'timers' ORDER BY seq LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str(&raw).unwrap()
    }

    fn timer_event_source_ids(&self) -> Vec<String> {
        let conn = self.db();
        let mut stmt = conn
            .prepare(
                "SELECT source_event_id FROM agent_events WHERE source_kind = 'timers' ORDER BY seq",
            )
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    // --- AO-5 Dispatcher helpers ---

    fn enqueue_notification(&self, execution_id: &str, topic: &str) -> sqlite_store::OutboxRow {
        let mut conn = self.db();
        let summary = sqlite_store::canonical_json(&json!({
            "kind": "notification",
            "topic": topic,
        }));
        sqlite_store::enqueue_outbox(&mut conn, execution_id, &summary)
            .expect("enqueue notification")
    }

    fn outbox_row(&self, outbox_id: &str) -> sqlite_store::OutboxRow {
        let conn = self.db();
        conn.query_row(
            "SELECT outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms, deliver_at_ms FROM outbox WHERE outbox_id = ?1",
            [outbox_id],
            |row| {
                Ok(sqlite_store::OutboxRow {
                    outbox_id: row.get(0)?,
                    execution_id: row.get(1)?,
                    payload_summary_json: row.get(2)?,
                    destination_kind: row.get(3)?,
                    status: row.get(4)?,
                    attempts: row.get(5)?,
                    dedup_key: row.get(6)?,
                    created_ms: row.get(7)?,
                    updated_at_ms: row.get(8)?,
                    deliver_at_ms: row.get(9)?,
                })
            },
        )
        .unwrap()
    }

    fn count_outbox_events(&self, event_type: &str) -> i64 {
        let conn = self.db();
        conn.query_row(
            "SELECT COUNT(*) FROM events WHERE aggregate_kind = 'outbox' AND event_type = ?1",
            [event_type],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn outbox_event_aggregate_ids(&self, event_type: &str) -> Vec<String> {
        let conn = self.db();
        let mut stmt = conn
            .prepare(
                "SELECT aggregate_id FROM events WHERE aggregate_kind = 'outbox' AND event_type = ?1 ORDER BY seq",
            )
            .unwrap();
        stmt.query_map([event_type], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn merged_into_of(&self, outbox_id: &str) -> String {
        let conn = self.db();
        let raw: String = conn
            .query_row(
                "SELECT payload_json FROM events WHERE aggregate_kind = 'outbox' AND aggregate_id = ?1 AND event_type = 'outbox.merged'",
                [outbox_id],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str::<Value>(&raw).unwrap()["merged_into"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn find_digest(&self, date: &str) -> Option<sqlite_store::OutboxRow> {
        let conn = self.db();
        conn.query_row(
            "SELECT outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms, deliver_at_ms FROM outbox WHERE dedup_key = ?1",
            [format!("digest:{date}")],
            |row| {
                Ok(sqlite_store::OutboxRow {
                    outbox_id: row.get(0)?,
                    execution_id: row.get(1)?,
                    payload_summary_json: row.get(2)?,
                    destination_kind: row.get(3)?,
                    status: row.get(4)?,
                    attempts: row.get(5)?,
                    dedup_key: row.get(6)?,
                    created_ms: row.get(7)?,
                    updated_at_ms: row.get(8)?,
                    deliver_at_ms: row.get(9)?,
                })
            },
        )
        .optional()
        .unwrap()
    }

    /// One scheduled `imessage_send` row whose body row
    /// is deleted right after enqueue: the deterministic
    /// failure for E4 (step 3, "no stored body") — no
    /// AppleScript, no network, no real thread.
    fn enqueue_scheduled_send_no_body(&self, deliver_at_ms: i64) -> sqlite_store::OutboxRow {
        let body = "e4 test body";
        let body_hash = mock_gateway::imessage_send::imessage_body_hash(body);
        let payload = sqlite_store::canonical_json(&json!({
            "thread_id": "thread-1",
            "body_hash": body_hash,
            "idempotency_key": "e4-key",
            "approval_id": "approval-e4",
        }));
        let mut conn = self.db();
        let (row, created) = sqlite_store::enqueue_scheduled_imessage_send(
            &mut conn,
            "exec-e4",
            &payload,
            deliver_at_ms,
            "thread-1",
            body,
            &body_hash,
            "e4-key",
            "approval-e4",
        )
        .expect("enqueue scheduled send");
        assert!(created, "e4 row must be newly created");
        sqlite_store::delete_scheduled_imessage_send(&mut conn, &row.outbox_id)
            .expect("delete body row");
        row
    }

    /// Re-due a pending row to the past through the
    /// manual schedule route (ungated, AO5-J7): the
    /// test's way to make a backoff row due again
    /// without waiting out the tier.
    fn redue_to_past(&self, outbox_id: &str) {
        let mut conn = self.db();
        let past = sqlite_store::now_ms() as i64 - 60_000;
        sqlite_store::schedule_outbox(&mut conn, outbox_id, past).expect("redue to past");
    }
}

/// AO-5: a fixed 10:00 daytime clock, so the
/// Dispatcher gates (quiet hours 23:00-08:00) never
/// hold rows no matter what hour CI runs in.
fn daytime_clock() -> sqlite_store::DispatcherClock {
    sqlite_store::DispatcherClock::fixed_for_test(10, 0, sqlite_store::now_ms() as i64)
}

#[tokio::test]
async fn schema_version_is_23() {
    assert_eq!(DB_SCHEMA_VERSION, 23, "AO-2 bumps the schema to v23 (AO-1 added v22)");
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), 23);
}

/// A1: ingest writes one row; the same
/// `source_event_id` twice writes nothing the
/// second time (the existing row comes back with
/// `deduped: true`).
#[tokio::test]
async fn a1_ingest_writes_row_and_dedupes() {
    let h = Harness::serve().await;
    let body = json!({
        "source_kind": "timers",
        "source_event_id": "timer-1",
        "payload_summary_json": {"timer_id": "timer-1", "kind": "follow_up"},
        "session_id": "session-1",
    });
    let (status, first) = h.ingest(body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["event_id"], "agent-event-1");
    assert_eq!(first["status"], "pending");
    assert_eq!(first["deduped"], false);
    assert_eq!(first["source_kind"], "timers");
    assert_eq!(first["source_event_id"], "timer-1");
    assert_eq!(first["payload_summary_json"]["timer_id"], "timer-1");
    assert_eq!(h.count_agent_events(), 1);

    // A re-delivered trigger with a different
    // summary still maps to the one row.
    let mut again = body.clone();
    again["payload_summary_json"] = json!({"timer_id": "timer-1", "kind": "follow_up", "retry": 1});
    let (status, second) = h.ingest(again).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["event_id"], "agent-event-1");
    assert_eq!(second["deduped"], true);
    assert_eq!(h.count_agent_events(), 1);

    // Ingest appends agent_event.created to the
    // events audit (AO1-J4); the deduped re-ingest
    // appends nothing.
    assert_eq!(
        h.audit_event_types("agent-event-1"),
        vec!["agent_event.created".to_string()]
    );
    let payload = h.audit_payload("agent-event-1", "agent_event.created");
    assert_eq!(payload["event_id"], "agent-event-1");
    assert_eq!(payload["source_kind"], "timers");
    assert_eq!(payload["source_event_id"], "timer-1");
    assert_eq!(payload["status"], "pending");
}

/// A1 (validation): empty ids and a non-object
/// summary are 400 `invalid_agent_event`.
#[tokio::test]
async fn a1_ingest_rejects_invalid_body() {
    let h = Harness::serve().await;
    let (status, value) = h
        .ingest(json!({"source_kind": "", "source_event_id": "x"}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "invalid_agent_event");
    let (status, value) = h
        .ingest(json!({"source_kind": "timers", "source_event_id": "x", "payload_summary_json": "nope"}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "invalid_agent_event");
    assert_eq!(h.count_agent_events(), 0);
}

/// A2: claim atomicity — two claims of the same
/// pending event, sequential and racing, yield
/// exactly one winner.
#[tokio::test]
async fn a2_claim_is_atomic() {
    let h = Harness::serve().await;
    let (_, first) = h
        .ingest(json!({
            "source_kind": "timers",
            "source_event_id": "timer-a2",
            "session_id": "session-a2",
        }))
        .await;
    let event_id = first["event_id"].as_str().unwrap().to_string();

    // Sequential: the first claim wins, the second
    // sees a non-pending row.
    let mut conn = h.db();
    assert!(sqlite_store::claim_agent_event(&mut conn, &event_id, 1000).unwrap());
    assert!(!sqlite_store::claim_agent_event(&mut conn, &event_id, 1001).unwrap());
    assert_eq!(h.agent_event_status(&event_id), "claimed");

    // Racing: two threads on separate connections
    // claim a fresh pending event; exactly one wins.
    let (_, second) = h
        .ingest(json!({
            "source_kind": "timers",
            "source_event_id": "timer-a2-race",
        }))
        .await;
    let race_id = second["event_id"].as_str().unwrap().to_string();
    let db_path = h.dir.join("gateway.sqlite3");
    // The two connections are opened BEFORE the
    // threads spawn (AO1-J6): `open_db` runs the
    // migrations under a write lock, so two
    // simultaneous opens of the same file can
    // collide with "database is locked" — the race
    // under test is the claim UPDATE, not the open.
    let mut conns: Vec<rusqlite::Connection> = (0..2)
        .map(|_| {
            let conn = sqlite_store::open_db(&db_path).expect("open db");
            conn.busy_timeout(Duration::from_secs(5))
                .expect("busy timeout");
            conn
        })
        .collect();
    let results: Vec<bool> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for mut conn in conns.drain(..) {
            let id = race_id.clone();
            handles.push(
                scope.spawn(move || {
                    sqlite_store::claim_agent_event(&mut conn, &id, 2000).unwrap()
                }),
            );
        }
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect()
    });
    assert_eq!(results.iter().filter(|won| **won).count(), 1);
    assert_eq!(h.agent_event_status(&race_id), "claimed");
}

/// A3: the single-session gate — a second event
/// for a busy session stays pending until the
/// first leaves `claimed`; a NULL-session event
/// claims independently (AO1-J1).
#[tokio::test]
async fn a3_single_session_gate_queues() {
    let h = Harness::serve().await;
    let (_, first) = h
        .ingest(json!({
            "source_kind": "file.changed",
            "source_event_id": "file-1",
            "session_id": "session-a3",
        }))
        .await;
    let (_, second) = h
        .ingest(json!({
            "source_kind": "file.changed",
            "source_event_id": "file-2",
            "session_id": "session-a3",
        }))
        .await;
    let first_id = first["event_id"].as_str().unwrap().to_string();
    let second_id = second["event_id"].as_str().unwrap().to_string();

    let mut conn = h.db();
    assert!(sqlite_store::claim_agent_event(&mut conn, &first_id, 1000).unwrap());
    // Same session, still claimed: the second event
    // cannot claim — it queues for a later tick.
    assert!(!sqlite_store::claim_agent_event(&mut conn, &second_id, 1001).unwrap());
    assert_eq!(h.agent_event_status(&second_id), "pending");

    // A session-less event is not gated by the busy
    // session (AO1-J1).
    let (_, third) = h
        .ingest(json!({
            "source_kind": "timers",
            "source_event_id": "timer-a3",
        }))
        .await;
    let third_id = third["event_id"].as_str().unwrap().to_string();
    assert!(sqlite_store::claim_agent_event(&mut conn, &third_id, 1002).unwrap());

    // Once the first event completes, the second
    // event of that session can claim.
    assert!(sqlite_store::complete_agent_event(&mut conn, &first_id).unwrap());
    assert_eq!(h.agent_event_status(&first_id), "done");
    assert!(sqlite_store::claim_agent_event(&mut conn, &second_id, 1003).unwrap());
}

/// A4: a claimed event that fails is recorded
/// `failed`, never auto-retried, and the failure
/// enters the events audit; terminal rows do not
/// move again.
#[tokio::test]
async fn a4_failure_is_failed_without_retry_and_audited() {
    let h = Harness::serve().await;
    let (_, first) = h
        .ingest(json!({
            "source_kind": "timers",
            "source_event_id": "timer-a4",
            "session_id": "session-a4",
        }))
        .await;
    let event_id = first["event_id"].as_str().unwrap().to_string();

    let mut conn = h.db();
    assert!(sqlite_store::claim_agent_event(&mut conn, &event_id, 1000).unwrap());
    assert!(sqlite_store::fail_agent_event(&mut conn, &event_id, "test-device").unwrap());
    assert_eq!(h.agent_event_status(&event_id), "failed");

    // The failure enters the events audit (the
    // created row plus the failed row, nothing else).
    assert_eq!(
        h.audit_event_types(&event_id),
        vec![
            "agent_event.created".to_string(),
            "agent_event.failed".to_string()
        ]
    );
    let payload = h.audit_payload(&event_id, "agent_event.failed");
    assert_eq!(payload["event_id"], event_id);
    assert_eq!(payload["status"], "failed");

    // Terminal: no second failure, no claim, no
    // completion — and no retry ever picks it up
    // (there is no driver in this batch; the row
    // simply stays failed).
    assert!(!sqlite_store::fail_agent_event(&mut conn, &event_id, "test-device").unwrap());
    assert!(!sqlite_store::claim_agent_event(&mut conn, &event_id, 1001).unwrap());
    assert!(!sqlite_store::complete_agent_event(&mut conn, &event_id).unwrap());
    assert_eq!(h.agent_event_status(&event_id), "failed");
    assert_eq!(
        h.audit_event_types(&event_id),
        vec![
            "agent_event.created".to_string(),
            "agent_event.failed".to_string()
        ]
    );
}

/// B1: a due follow-up fires exactly once — one
/// event through AO-1 ingest, the timer moves to
/// `done`, and a second drive does not re-fire.
#[tokio::test]
async fn b1_follow_up_fires_once_then_done() {
    let h = Harness::serve().await;
    let now = sqlite_store::now_ms() as i64;
    let (status, timer) = h
        .create_timer(json!({
            "kind": "follow_up",
            "next_fire_ms": now - 1000,
            "payload_summary_json": {"note": "remind"},
            "session_id": "session-b1",
        }))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(timer["timer_id"], "timer-1");
    assert_eq!(timer["status"], "active");
    assert_eq!(timer["session_id"], "session-b1");
    assert_eq!(h.count_agent_timers(), 1);

    assert_eq!(h.state.drive_due_timers(), 1);
    assert_eq!(h.timer_status("timer-1"), "done");
    assert_eq!(h.count_timer_events(), 1);
    assert_eq!(h.state.drive_due_timers(), 0);
    assert_eq!(h.count_timer_events(), 1);

    // The fired event carries the timer identity
    // (AO2-J3) plus the timer's own summary, and the
    // timer's session so the AO-3 driver knows it.
    let payload = h.timer_event_payload();
    assert_eq!(payload["timer_id"], "timer-1");
    assert_eq!(payload["kind"], "follow_up");
    assert_eq!(payload["note"], "remind");
    assert_eq!(
        h.timer_event_source_ids(),
        vec![format!("timer:timer-1:{}", now - 1000)]
    );
}

/// B1 (validation): an unknown kind, a schedule
/// without a positive interval, a follow-up
/// carrying a recurrence, and a non-object summary
/// are all 400 `invalid_agent_timer`.
#[tokio::test]
async fn b1_create_rejects_invalid_body() {
    let h = Harness::serve().await;
    let now = sqlite_store::now_ms() as i64;
    let (status, value) = h
        .create_timer(json!({"kind": "snooze", "next_fire_ms": now}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "invalid_agent_timer");
    let (status, value) = h
        .create_timer(json!({"kind": "schedule", "next_fire_ms": now}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "invalid_agent_timer");
    let (status, value) = h
        .create_timer(json!({"kind": "schedule", "next_fire_ms": now, "recurrence_json": {"interval_ms": 0}}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "invalid_agent_timer");
    let (status, value) = h
        .create_timer(json!({"kind": "follow_up", "next_fire_ms": now, "recurrence_json": {"interval_ms": 1000}}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "invalid_agent_timer");
    let (status, value) = h
        .create_timer(json!({"kind": "follow_up", "next_fire_ms": now, "payload_summary_json": "nope"}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(value["error"], "invalid_agent_timer");
    assert_eq!(h.count_agent_timers(), 0);

    // The list is a bare array (the outbox list
    // precedent), soonest first.
    let (status, list) = h.list_timers().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list, json!([]));
}

/// B2: a schedule fires, advances `next_fire_ms`
/// by the interval, stays `active`, and fires the
/// next cycle with a distinct event id.
#[tokio::test]
async fn b2_schedule_advances_and_refires() {
    let h = Harness::serve().await;
    let now = sqlite_store::now_ms() as i64;
    let fire_at = now - 60_000;
    let (status, timer) = h
        .create_timer(json!({
            "kind": "schedule",
            "next_fire_ms": fire_at,
            "recurrence_json": {"interval_ms": 1000},
            "session_id": "session-b2",
        }))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(timer["timer_id"], "timer-1");

    assert_eq!(h.state.drive_due_timers(), 1);
    assert_eq!(h.timer_status("timer-1"), "active");
    assert_eq!(h.timer_next_fire("timer-1"), fire_at + 1000);
    assert_eq!(h.count_timer_events(), 1);

    // The advanced fire time is still in the past,
    // so the next drive fires the second cycle.
    assert_eq!(h.state.drive_due_timers(), 1);
    assert_eq!(h.timer_status("timer-1"), "active");
    assert_eq!(h.timer_next_fire("timer-1"), fire_at + 2000);
    assert_eq!(h.count_timer_events(), 2);
    assert_eq!(
        h.timer_event_source_ids(),
        vec![
            format!("timer:timer-1:{fire_at}"),
            format!("timer:timer-1:{}", fire_at + 1000),
        ]
    );
}

/// B3: a revoked timer never fires — due or
/// future. Revoke is idempotent (AO2-J6) and an
/// unknown id is 404 `unknown_timer`.
#[tokio::test]
async fn b3_revoke_never_fires() {
    let h = Harness::serve().await;
    let now = sqlite_store::now_ms() as i64;
    let (_, timer) = h
        .create_timer(json!({
            "kind": "follow_up",
            "next_fire_ms": now - 1000,
        }))
        .await;
    let timer_id = timer["timer_id"].as_str().unwrap().to_string();
    let (status, revoked) = h.delete_timer(&timer_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(revoked["status"], "cancelled");
    assert_eq!(h.state.drive_due_timers(), 0);
    assert_eq!(h.count_timer_events(), 0);

    let (status, again) = h.delete_timer(&timer_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["status"], "cancelled");
    assert_eq!(again["timer_id"], timer_id);

    let (status, value) = h.delete_timer("timer-404").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(value["error"], "unknown_timer");

    // A future timer revoked before its time also
    // never fires.
    let (_, future) = h
        .create_timer(json!({
            "kind": "follow_up",
            "next_fire_ms": now + 3_600_000,
        }))
        .await;
    let future_id = future["timer_id"].as_str().unwrap().to_string();
    let (status, _) = h.delete_timer(&future_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.state.drive_due_timers(), 0);
    assert_eq!(h.count_timer_events(), 0);
    assert_eq!(h.count_agent_timers(), 2);
}

/// B4: restart keeps unexpired timers and fires
/// expired ones exactly once on the first drive
/// after restart (past = due, the D1-3 C-1 rule).
#[tokio::test]
async fn b4_restart_keeps_unexpired_and_fires_expired() {
    let mut h = Harness::serve().await;
    let now = sqlite_store::now_ms() as i64;
    let (_, due) = h
        .create_timer(json!({
            "kind": "follow_up",
            "next_fire_ms": now - 1000,
        }))
        .await;
    let due_id = due["timer_id"].as_str().unwrap().to_string();
    let (_, future) = h
        .create_timer(json!({
            "kind": "follow_up",
            "next_fire_ms": now + 3_600_000,
        }))
        .await;
    let future_id = future["timer_id"].as_str().unwrap().to_string();

    h.restart().await;
    assert_eq!(h.schema_version(), 23, "schema stays v23 across restart");
    assert_eq!(h.count_agent_timers(), 2);
    assert_eq!(h.timer_status(&due_id), "active");
    assert_eq!(h.timer_status(&future_id), "active");

    assert_eq!(h.state.drive_due_timers(), 1);
    assert_eq!(h.timer_status(&due_id), "done");
    assert_eq!(h.timer_status(&future_id), "active");
    assert_eq!(h.count_timer_events(), 1);
    assert_eq!(h.state.drive_due_timers(), 0);
    assert_eq!(h.count_timer_events(), 1);
}

/// E1 (Q-AO-11): three `local_notification` rows with
/// the same topic, enqueued within seconds of each
/// other, merge into ONE delivery — one survivor
/// delivered, the other two folded into it (one
/// `outbox.sent`, two `outbox.merged` naming the
/// survivor). No row is held: all three stay
/// immediate (`deliver_at_ms` untouched).
#[tokio::test]
async fn e1_same_topic_merges_within_window() {
    let h = Harness::serve().await;
    let rows: Vec<_> = (1..=3)
        .map(|i| h.enqueue_notification(&format!("exec-e1-{i}"), "digest-topic"))
        .collect();
    assert_eq!(
        h.state.deliver_pending_outbox_with_clock(Some(daytime_clock())),
        1,
        "one survivor delivered"
    );
    assert_eq!(h.count_outbox_events("outbox.sent"), 1);
    assert_eq!(h.count_outbox_events("outbox.merged"), 2);
    let sent_ids = h.outbox_event_aggregate_ids("outbox.sent");
    assert_eq!(sent_ids.len(), 1);
    let survivor = &sent_ids[0];
    for row in &rows {
        let after = h.outbox_row(&row.outbox_id);
        assert_eq!(after.status, "sent", "row {}", row.outbox_id);
        assert_eq!(after.attempts, 1, "row {}", row.outbox_id);
        assert_eq!(after.deliver_at_ms, None, "row {}", row.outbox_id);
        if row.outbox_id != *survivor {
            assert_eq!(h.merged_into_of(&row.outbox_id), *survivor);
        }
    }
}

/// E2 (Q-AO-11): 21 distinct-topic rows against a
/// 20-per-day cap — the first 20 deliver, the 21st
/// stays pending, deferred to tomorrow 08:00
/// (day_start + 115_200_000 ms = 32 h: 24 h + 8 h).
/// A second pass delivers nothing more.
#[tokio::test]
async fn e2_daily_cap_holds_21st_row() {
    let h = Harness::serve().await;
    let now = sqlite_store::now_ms() as i64;
    let clock = sqlite_store::DispatcherClock::fixed_for_test(10, 0, now);
    let rows: Vec<_> = (0..21)
        .map(|i| h.enqueue_notification(&format!("exec-e2-{i}"), &format!("topic-{i}")))
        .collect();
    assert_eq!(
        h.state.deliver_pending_outbox_with_clock(Some(clock)),
        20,
        "20 of 21 deliver"
    );
    assert_eq!(h.count_outbox_events("outbox.sent"), 20);
    assert_eq!(h.count_outbox_events("outbox.merged"), 0);
    for row in &rows[..20] {
        assert_eq!(h.outbox_row(&row.outbox_id).status, "sent");
    }
    let held = h.outbox_row(&rows[20].outbox_id);
    assert_eq!(held.status, "pending");
    // 115_200_000 ms = 32 h = (24 + 8) h — tomorrow 08:00.
    let day_start = now - now.rem_euclid(86_400_000);
    assert_eq!(held.deliver_at_ms, Some(day_start + 115_200_000));
    assert_eq!(
        h.state.deliver_pending_outbox_with_clock(Some(daytime_clock())),
        0,
        "the held row is timed now, not in the event channel"
    );
}

/// E3 (Q-AO-11): two rows enqueued at 23:30 are held
/// (nothing delivered, both deferred to tomorrow
/// 08:00); at 08:05 the next day the due pass folds
/// them into ONE morning digest row (count 2, ids
/// listed), and the digest itself delivers on the
/// next pending-channel tick — exactly one
/// `outbox.sent` for the whole morning.
#[tokio::test]
async fn e3_quiet_hours_hold_then_morning_digest() {
    let h = Harness::serve().await;
    let now = sqlite_store::now_ms() as i64;
    let day_start = now - now.rem_euclid(86_400_000);
    let quiet = sqlite_store::DispatcherClock::fixed_for_test(23, 30, now);
    let rows: Vec<_> = (1..=2)
        .map(|i| h.enqueue_notification(&format!("exec-e3-{i}"), &format!("topic-{i}")))
        .collect();
    assert_eq!(
        h.state.deliver_pending_outbox_with_clock(Some(quiet)),
        0,
        "quiet hours deliver nothing"
    );
    assert_eq!(h.count_outbox_events("outbox.sent"), 0);
    for row in &rows {
        let held = h.outbox_row(&row.outbox_id);
        assert_eq!(held.status, "pending");
        assert_eq!(held.deliver_at_ms, Some(day_start + 115_200_000));
    }
    // Next day 08:05 — 300_000 ms past tomorrow 08:00.
    let morning = sqlite_store::DispatcherClock::fixed_for_test(
        8,
        5,
        day_start + 115_200_000 + 300_000,
    );
    assert_eq!(
        h.state
            .deliver_due_outbox_with_clock(Some(morning.clone()))
            .await,
        0,
        "the due pass folds, it does not deliver the digest itself"
    );
    let digest = h
        .find_digest(&morning.local_date)
        .expect("one digest row for the morning");
    assert_eq!(digest.status, "pending");
    let summary: Value = serde_json::from_str(&digest.payload_summary_json).unwrap();
    assert_eq!(summary["kind"], "digest");
    assert_eq!(summary["date"], morning.local_date);
    assert_eq!(summary["count"], 2);
    let ids: Vec<String> = serde_json::from_value(summary["outbox_ids"].clone()).unwrap();
    assert_eq!(
        ids,
        rows.iter().map(|r| r.outbox_id.clone()).collect::<Vec<_>>()
    );
    assert_eq!(h.count_outbox_events("outbox.merged"), 2);
    for row in &rows {
        assert_eq!(h.outbox_row(&row.outbox_id).status, "sent");
        assert_eq!(h.merged_into_of(&row.outbox_id), digest.outbox_id);
    }
    assert_eq!(
        h.state.deliver_pending_outbox_with_clock(Some(morning)),
        1,
        "the digest delivers on the next tick"
    );
    assert_eq!(h.outbox_row(&digest.outbox_id).status, "sent");
    assert_eq!(h.count_outbox_events("outbox.sent"), 1);
}

/// E4 (Q-AO-12): a scheduled `imessage_send` row with
/// no stored body fails deterministically at step 3.
/// Attempts 1-3 reschedule it (backoff 1/5/15 min —
/// each delay lands in [tier, tier + 60_000] ms of
/// the attempt's start); the 4th failed attempt marks
/// it failed, and it stays failed (no 5th attempt,
/// no `outbox.sent`).
#[tokio::test]
async fn e4_failure_backoff_then_failed() {
    let h = Harness::serve().await;
    let now = sqlite_store::now_ms() as i64;
    let row = h.enqueue_scheduled_send_no_body(now - 60_000);
    let tiers: [i64; 3] = [60_000, 300_000, 900_000];
    for (attempt, tier) in tiers.iter().enumerate() {
        let before = sqlite_store::now_ms() as i64;
        assert_eq!(
            h.state
                .deliver_due_outbox_with_clock(Some(daytime_clock()))
                .await,
            0,
            "attempt {} delivers nothing",
            attempt + 1
        );
        let after = h.outbox_row(&row.outbox_id);
        assert_eq!(after.status, "pending", "attempt {}", attempt + 1);
        assert_eq!(after.attempts, (attempt + 1) as i64, "attempt {}", attempt + 1);
        let due = after.deliver_at_ms.expect("backoff sets a due time");
        let delay = due - before;
        assert!(
            (*tier..=*tier + 60_000).contains(&delay),
            "attempt {} delay {delay} outside [{tier}, {}]",
            attempt + 1,
            tier + 60_000
        );
        // `outbox.rescheduled` counts BOTH sources: one backoff
        // reschedule per failed attempt (n so far) plus one
        // manual redue per completed cycle (n - 1 so far) —
        // the manual `schedule_outbox` route appends the
        // same event type. Total after attempt n: 2n - 1.
        let n = (attempt + 1) as i64;
        assert_eq!(h.count_outbox_events("outbox.rescheduled"), 2 * n - 1);
        assert_eq!(h.count_outbox_events("outbox.failed"), 0);
        h.redue_to_past(&row.outbox_id);
    }
    // 4th failed attempt: failed, attempts 4.
    assert_eq!(
        h.state
            .deliver_due_outbox_with_clock(Some(daytime_clock()))
            .await,
        0,
        "the 4th attempt delivers nothing"
    );
    let after = h.outbox_row(&row.outbox_id);
    assert_eq!(after.status, "failed");
    assert_eq!(after.attempts, 4);
    // 3 backoff reschedules + 3 manual redues = 6.
    assert_eq!(h.count_outbox_events("outbox.rescheduled"), 6);
    assert_eq!(h.count_outbox_events("outbox.failed"), 1);
    assert_eq!(h.count_outbox_events("outbox.sent"), 0);
    // It stays failed: no 5th attempt, no resurrection.
    assert_eq!(
        h.state
            .deliver_due_outbox_with_clock(Some(daytime_clock()))
            .await,
        0
    );
    let still = h.outbox_row(&row.outbox_id);
    assert_eq!(still.status, "failed");
    assert_eq!(still.attempts, 4);
}

/// D1 (Q-AO-10): loop_start loads the user
/// instruction layer — the recorded model request
/// carries the instruction verbatim as the first
/// user message, ahead of the prompt, and the loop
/// completes on the fake's final reply.
#[tokio::test]
async fn ao4_d1_loop_start_loads_instruction_layer() {
    let h = Harness::serve_recording().await;
    let instruction = "每天早上九点提醒我喝水。";
    h.seed_memory("memory-1", "user", instruction, 1_000);
    let (status, started) = h
        .loop_start(json!({"prompt": "帮我看看今天要做什么"}))
        .await;
    assert_eq!(status, StatusCode::OK, "{started}");
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let (status, view) = h.loop_resume(&loop_id).await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["status"], "completed");
    assert_eq!(view["final_text"], "done");
    let requests = h.model_requests();
    assert_eq!(requests.len(), 1);
    let messages = requests[0]["messages"].as_array().unwrap();
    assert_eq!(messages[0]["role"], "system");
    assert_eq!(messages[1]["role"], "user");
    assert!(
        messages[1]["content"]
            .as_str()
            .unwrap()
            .contains(instruction),
        "{}",
        messages[1]
    );
    assert_eq!(messages[2]["content"], "帮我看看今天要做什么");
}

/// D2 (Q-AO-10): four 1000-CJK-char instruction rows
/// against the 4k budget — header (5) + three full
/// rows (1004 each) = 3017; a fourth full row would
/// reach 4021 > 4000, so it degrades to its index
/// line (47), and the notes index follows (header 5
/// + 48 = 3117 total). The over-budget row's full
/// text never enters the context; its index line
/// does. All token figures were computed
/// independently in Python before this test was
/// written, not copied from the loader's output.
#[tokio::test]
async fn ao4_d2_over_budget_rows_load_as_index_only() {
    let h = Harness::serve_recording().await;
    h.seed_memory("memory-1", "user", &"甲".repeat(1000), 1_000);
    h.seed_memory("memory-2", "user", &"乙".repeat(1000), 2_000);
    h.seed_memory("memory-3", "user", &"丙".repeat(1000), 3_000);
    h.seed_memory("memory-4", "user", &"丁".repeat(1000), 4_000);
    h.seed_memory("memory-5", "project", &"戊".repeat(100), 5_000);
    let (status, started) = h.loop_start(json!({"prompt": "开始"})).await;
    assert_eq!(status, StatusCode::OK, "{started}");
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let (status, view) = h.loop_resume(&loop_id).await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["status"], "completed");
    let requests = h.model_requests();
    assert_eq!(requests.len(), 1);
    let messages = requests[0]["messages"].as_array().unwrap();
    let context = messages[1]["content"].as_str().unwrap().to_string();
    assert!(context.contains("[记忆·指令]"));
    assert!(context.contains("[记忆·笔记]"));
    assert!(!context.contains("[记忆·任务进度]"));
    assert!(context.contains(&"甲".repeat(1000)));
    assert!(context.contains(&"乙".repeat(1000)));
    assert!(context.contains(&"丙".repeat(1000)));
    assert!(!context.contains(&"丁".repeat(1000)));
    assert!(context.contains("memory-4"));
    assert!(context.contains("索引"));
    assert!(context.contains(&"丁".repeat(40)));
    assert!(context.contains("memory-5"));
    assert!(!context.contains(&"戊".repeat(100)));
    assert!(context.contains(&"戊".repeat(40)));
}

/// D3 (Q-AO-10): the dreaming pass writes a weekly
/// summary row, tombstones the older of two
/// exact-duplicate rows, and loses no content — the
/// tombstoned row stays readable with its text
/// intact, and the surviving rows are byte-identical.
/// The expected summary string was computed
/// independently in Python before this test was
/// written.
#[tokio::test]
async fn ao4_d3_dreaming_writes_back_and_tombstones() {
    let h = Harness::serve().await;
    h.seed_memory("memory-1", "user", "喜欢红茶", 1_000);
    h.seed_memory("memory-2", "user", "喜欢红茶", 2_000);
    h.seed_memory("memory-3", "user", "每周五跑步", 3_000);
    let now_ms = 1_800_000_000_000u64;
    let sessions = vec![
        DreamSession {
            session_id: "loop-1".to_string(),
            created_ms: now_ms - 1_000,
            kind: "main".to_string(),
            status: "completed".to_string(),
            tools: vec!["fs.read".to_string()],
        },
        DreamSession {
            session_id: "loop-2".to_string(),
            created_ms: now_ms - 2_000,
            kind: "research".to_string(),
            status: "failed".to_string(),
            tools: vec!["code.search".to_string(), "fs.read".to_string()],
        },
    ];
    let mut conn = h.db();
    let outcome =
        memory_store::run_dreaming_pass(&mut conn, &sessions, now_ms, "test").unwrap();
    assert!(outcome.summary_written);
    assert_eq!(outcome.summary_id.as_deref(), Some("memory-4"));
    assert_eq!(outcome.tombstoned, vec!["memory-1".to_string()]);
    assert_eq!(outcome.sessions_seen, 2);
    drop(conn);
    let conn = h.db();
    let summary = memory_store::get_memory(&conn, "memory-4")
        .unwrap()
        .expect("summary row");
    assert_eq!(
        summary.content,
        "近 7 天会话整理：共 2 个会话（main 1 个，research 1 个）；完成 1 个，失败 1 个，其他 0 个；用过工具：code.search、fs.read。"
    );
    assert_eq!(summary.layer, "user");
    assert!(!summary.deleted);
    assert!(summary.source_json.contains("dreaming"));
    let old = memory_store::get_memory(&conn, "memory-1")
        .unwrap()
        .expect("tombstoned row stays readable");
    assert!(old.deleted);
    assert_eq!(old.content, "喜欢红茶");
    let kept = memory_store::get_memory(&conn, "memory-2")
        .unwrap()
        .expect("surviving duplicate");
    assert!(!kept.deleted);
    assert_eq!(kept.content, "喜欢红茶");
    let other = memory_store::get_memory(&conn, "memory-3")
        .unwrap()
        .expect("untouched row");
    assert!(!other.deleted);
    assert_eq!(other.content, "每周五跑步");
}

/// C1 (AO-3): a due follow-up timer fires, the wake
/// driver claims its event, the Run completes, the
/// event is done, one project memory row carries the
/// writeback, the wake Outbox row carries the
/// notification (plus the D1-3 execution row, AO3-J7),
/// and the Dispatcher delivers both. The wake prompt
/// the model saw names the timer and its summary.
#[tokio::test]
async fn ao3_c1_timer_event_wakes_run_to_done() {
    let h = Harness::serve_recording_driven().await;
    let now = sqlite_store::now_ms() as i64;
    let (status, timer) = h
        .create_timer(json!({
            "kind": "follow_up",
            "next_fire_ms": now - 1000,
            "payload_summary_json": {"note": "remind"},
            "session_id": "session-c1",
        }))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(timer["timer_id"], "timer-1");

    assert_eq!(h.state.drive_due_timers(), 1);
    assert_eq!(h.count_timer_events(), 1);
    assert_eq!(h.state.drive_wake_events(), 1, "one Run spawned");
    assert_eq!(
        h.wait_event_status("agent-event-1", "done").await,
        "done"
    );

    // Exactly one model request, and the wake prompt in
    // it names the timer and its summary.
    let requests = h.model_requests();
    assert_eq!(requests.len(), 1);
    let messages = requests[0]["messages"].as_array().unwrap();
    let prompt = messages
        .iter()
        .filter(|m| m["role"] == "user")
        .map(|m| m["content"].as_str().unwrap_or(""))
        .find(|c| c.contains("定时器唤醒"))
        .expect("wake prompt in the request");
    assert!(prompt.contains("timer-1"), "{prompt}");
    assert!(prompt.contains("follow_up"), "{prompt}");
    assert!(prompt.contains("remind"), "{prompt}");
    assert!(prompt.contains("agent-event-1"), "{prompt}");

    // The Run completed: one background loop, with a
    // positive token estimate under the 100k cap.
    let (status, list) = h.loop_list().await;
    assert_eq!(status, StatusCode::OK);
    let loops = list.as_array().unwrap();
    assert_eq!(loops.len(), 1);
    assert_eq!(loops[0]["status"], "completed");
    assert_eq!(loops[0]["background"], true);
    let estimated = loops[0]["estimated_tokens"].as_u64().unwrap();
    assert!(estimated > 0, "the estimator ran");
    assert!(estimated < 100_000, "under the per-run cap");

    // Memory writeback: one project row, naming the
    // event and the Run's final text.
    assert_eq!(h.count_memories(), 1);
    let memories = h.memory_rows();
    assert_eq!(memories[0].1, "project");
    assert!(memories[0].2.contains("agent-event-1"), "{}", memories[0].2);
    assert!(memories[0].2.contains("done"), "{}", memories[0].2);

    // Outbox: TWO rows. The D1-3 producer writes one
    // local_notification per terminal Execution (dedup
    // `{execution_id}:local_notification`, summary ids +
    // status only — pre-existing behaviour for every
    // loop, foreground included), and the wake driver
    // writes its own row carrying the Run's result
    // (dedup `{event_id}:local_notification`, per-event
    // topic so it never merges). AO3-J7: C1's "Outbox
    // 行" is the wake row; the D1-3 row is not the
    // wake driver's, and suppressing it is out of
    // AO-3's scope.
    assert_eq!(h.count_outbox(), 2);
    let rows = h.outbox_rows();
    let wake = rows
        .iter()
        .find(|r| r.dedup_key == "agent-event-1:local_notification")
        .expect("wake notification row");
    assert_eq!(wake.destination_kind, "local_notification");
    assert_eq!(wake.status, "pending");
    let summary: Value = serde_json::from_str(&wake.payload_summary_json).unwrap();
    assert_eq!(summary["topic"], "wake:agent-event-1");
    assert_eq!(summary["event_id"], "agent-event-1");
    assert_eq!(summary["result"], "done");
    assert!(wake.execution_id.starts_with("execution-"));
    let exec_row = rows
        .iter()
        .find(|r| r.dedup_key == format!("{}:local_notification", wake.execution_id))
        .expect("D1-3 execution notification row");
    let exec_summary: Value =
        serde_json::from_str(&exec_row.payload_summary_json).unwrap();
    assert_eq!(exec_summary["status"], "completed");
    assert_eq!(exec_row.execution_id, wake.execution_id);

    // The Dispatcher delivers both on the next pass
    // (daytime, under the daily cap, distinct merge
    // keys — the wake topic is per event, the D1-3
    // row merges by status only).
    assert_eq!(
        h.state.deliver_pending_outbox_with_clock(Some(daytime_clock())),
        2,
        "wake + execution notifications delivered"
    );
    assert_eq!(h.count_outbox_events("outbox.sent"), 2);
    assert_eq!(h.outbox_row(&wake.outbox_id).status, "sent");
    assert_eq!(h.outbox_row(&wake.outbox_id).attempts, 1);

    // The event carries its delivery timestamp.
    assert!(h.event_row("agent-event-1").delivered_ms.is_some());
}

/// C2 (Q-AO-06): two events of one session — the
/// first claims and runs, the second queues (no
/// concurrency); once the first is done, the next
/// drive claims the second. The second claim lands
/// after the first delivery, never during it.
#[tokio::test]
async fn ao3_c2_same_session_queues_without_concurrency() {
    let h = Harness::serve_recording_driven().await;
    for (id, note) in [("timer:c2:1", "first"), ("timer:c2:2", "second")] {
        let (status, ingested) = h
            .ingest(json!({
                "source_kind": "timers",
                "source_event_id": id,
                "payload_summary_json": {"note": note},
                "session_id": "session-c2",
            }))
            .await;
        assert_eq!(status, StatusCode::OK, "{ingested}");
    }
    assert_eq!(h.state.drive_wake_events(), 1, "only the first claims");
    assert_eq!(h.event_row("agent-event-1").status, "claimed");
    assert_eq!(h.event_row("agent-event-2").status, "pending");
    assert_eq!(
        h.wait_event_status("agent-event-1", "done").await,
        "done"
    );
    let first_delivered = h.event_row("agent-event-1").delivered_ms.unwrap();

    assert_eq!(h.state.drive_wake_events(), 1, "the second claims now");
    let second_claimed = h.event_row("agent-event-2").claimed_ms.unwrap();
    assert!(
        second_claimed >= first_delivered,
        "second claim ({second_claimed}) must not precede first delivery ({first_delivered})"
    );
    assert_eq!(
        h.wait_event_status("agent-event-2", "done").await,
        "done"
    );
    assert_eq!(h.count_memories(), 2);
    // Two wake rows (one per event) plus the two D1-3
    // execution rows (AO3-J7 — see C1).
    assert_eq!(h.count_outbox(), 4);
    let wake_keys: Vec<_> = h
        .outbox_rows()
        .iter()
        .filter(|r| r.dedup_key.starts_with("agent-event-"))
        .map(|r| r.dedup_key.clone())
        .collect();
    assert_eq!(
        wake_keys,
        vec![
            "agent-event-1:local_notification".to_string(),
            "agent-event-2:local_notification".to_string(),
        ]
    );
    let (status, list) = h.loop_list().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert!(list
        .as_array()
        .unwrap()
        .iter()
        .all(|l| l["status"] == "completed"));
}

/// C3 (Q-AO-09): the model proposes `fs.write` in a
/// wake Run — the loop-layer gate refuses it with
/// InvalidTool, the call is recorded Failed, the file
/// is never written, no approval is ever parked, and
/// the Run still completes (the model answers with a
/// final text on the next round).
#[tokio::test]
async fn ao3_c3_write_tool_refused_before_dispatch() {
    let write_reply = json!({"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"fs_write","arguments":"{\"path\":\"wake-should-not-exist.txt\",\"text\":\"x\"}"}}]},"finish_reason":"tool_calls"}]});
    let h = Harness::serve_scripted_driven(vec![write_reply]).await;
    let (status, ingested) = h
        .ingest(json!({
            "source_kind": "timers",
            "source_event_id": "timer:c3:1",
            "payload_summary_json": {"note": "write-please"},
            "session_id": "session-c3",
        }))
        .await;
    assert_eq!(status, StatusCode::OK, "{ingested}");
    assert_eq!(h.state.drive_wake_events(), 1);
    assert_eq!(
        h.wait_event_status("agent-event-1", "done").await,
        "done"
    );

    // Two model rounds: the refused proposal, then the
    // final text the scripted fake falls back to.
    assert_eq!(h.model_requests().len(), 2);
    let (status, list) = h.loop_list().await;
    assert_eq!(status, StatusCode::OK);
    let loops = list.as_array().unwrap();
    assert_eq!(loops.len(), 1);
    assert_eq!(loops[0]["status"], "completed");
    let (status, view) = h.loop_get("loop-1").await;
    assert_eq!(status, StatusCode::OK, "{view}");
    let calls = view["calls"].as_array().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["tool"], "fs.write");
    assert_eq!(calls[0]["status"], "failed");
    assert_eq!(calls[0]["error"], "invalid_tool");
    assert_eq!(calls[0]["approval_id"], Value::Null);
    assert_eq!(calls[0]["submitted"], false);

    // The write never landed anywhere.
    assert!(!h.dir.join("files").join("wake-should-not-exist.txt").exists());
    assert_eq!(h.count_memories(), 1);
    // One wake row + the D1-3 execution row (AO3-J7).
    assert_eq!(h.count_outbox(), 2);
    assert!(h
        .outbox_rows()
        .iter()
        .any(|r| r.dedup_key == "agent-event-1:local_notification"));
}

/// C4: the model is unreachable (the dropping fake) —
/// the Run fails, the event is failed, the wake driver
/// writes no Outbox row and no memory writeback, and a
/// later drive does not retry it. (AO3-J7: "Outbox
/// 无行" means no WAKE row — the D1-3 producer still
/// writes its one execution-failed row when the wake
/// Execution closes, exactly as for any failed
/// foreground loop; that row is the Execution layer's,
/// not the wake driver's.)
#[tokio::test]
async fn ao3_c4_run_failure_is_failed_without_retry() {
    let h = Harness::serve_driven().await;
    let (status, ingested) = h
        .ingest(json!({
            "source_kind": "timers",
            "source_event_id": "timer:c4:1",
            "payload_summary_json": {"note": "will-fail"},
            "session_id": "session-c4",
        }))
        .await;
    assert_eq!(status, StatusCode::OK, "{ingested}");
    assert_eq!(h.state.drive_wake_events(), 1);
    assert_eq!(
        h.wait_event_status("agent-event-1", "failed").await,
        "failed"
    );
    assert!(!h
        .outbox_rows()
        .iter()
        .any(|r| r.dedup_key == "agent-event-1:local_notification"),
        "no wake notification row on a failed Run");
    assert_eq!(h.count_outbox(), 1, "only the D1-3 execution row");
    assert_eq!(h.count_memories(), 0, "no writeback on a failed Run");
    // One Inbox row: the execution-failed row
    // `upsert_execution_tx` opens for every failed
    // Execution close (P0-D, pre-existing — not a wake
    // row, and no wake-budget/token row exists here).
    assert_eq!(h.count_inbox(), 1);
    assert!(h
        .inbox_source_ids()
        .iter()
        .all(|id| !id.starts_with("wake-")));
    let (status, list) = h.loop_list().await;
    assert_eq!(status, StatusCode::OK);
    let loops = list.as_array().unwrap();
    assert_eq!(loops.len(), 1);
    assert_eq!(loops[0]["status"], "failed");

    // No auto-retry: a later drive claims nothing and
    // the event stays failed.
    assert_eq!(h.state.drive_wake_events(), 0);
    assert_eq!(h.event_row("agent-event-1").status, "failed");
    assert_eq!(h.count_outbox(), 1);
}

/// C5: a claimed event whose Run never finished
/// (stale claim) returns to pending and runs on the
/// next drive; a fresh claim is left alone.
#[tokio::test]
async fn ao3_c5_stale_claim_recovers_fresh_claim_untouched() {
    let h = Harness::serve_recording_driven().await;
    let now = sqlite_store::now_ms() as i64;
    for (id, session) in [("timer:c5:1", "session-c5a"), ("timer:c5:2", "session-c5b")] {
        let (status, ingested) = h
            .ingest(json!({
                "source_kind": "timers",
                "source_event_id": id,
                "payload_summary_json": {"note": "stale"},
                "session_id": session,
            }))
            .await;
        assert_eq!(status, StatusCode::OK, "{ingested}");
    }
    // Simulate a gateway that died mid-Run for event 2
    // (claimed 700_000 ms ago — past the 600_000 ms
    // stale threshold) and a live Run for event 1
    // (claimed just now).
    {
        let mut conn = h.db();
        assert!(sqlite_store::claim_agent_event(&mut conn, "agent-event-1", now).unwrap());
        assert!(sqlite_store::claim_agent_event(&mut conn, "agent-event-2", now - 700_000).unwrap());
    }
    assert_eq!(h.state.drive_wake_events(), 1, "only the stale one re-drives");
    assert_eq!(h.event_row("agent-event-1").status, "claimed");
    assert_eq!(
        h.wait_event_status("agent-event-2", "done").await,
        "done"
    );
    assert_eq!(h.event_row("agent-event-1").status, "claimed");

    // Age the first claim past the threshold too, and
    // it recovers the same way.
    {
        let conn = h.db();
        conn.execute(
            "UPDATE agent_events SET claimed_ms = ?1 WHERE event_id = 'agent-event-1'",
            [now - 700_000],
        )
        .unwrap();
    }
    assert_eq!(h.state.drive_wake_events(), 1);
    assert_eq!(
        h.wait_event_status("agent-event-1", "done").await,
        "done"
    );
    assert_eq!(h.count_memories(), 2);
}

/// C6 (Q-AO-12): a wake Run whose estimated tokens
/// pass 100k fails with BudgetExceeded, the event is
/// failed, and a wake-token Inbox row names the breach
/// (alongside the pre-existing execution-failed row,
/// AO3-J7 — see C4). The giant reply alone estimates
/// 100_001 tokens (each CJK char counts 1 under the
/// AO-4 estimator), so the total is over the cap
/// regardless of the input prompt's size.
#[tokio::test]
async fn ao3_c6_token_cap_fails_run_and_writes_inbox_row() {
    let giant = "甲".repeat(100_001);
    let giant_reply =
        json!({"choices":[{"message":{"role":"assistant","content":giant,"tool_calls":null},"finish_reason":"stop"}]});
    let h = Harness::serve_scripted_driven(vec![giant_reply]).await;
    let (status, ingested) = h
        .ingest(json!({
            "source_kind": "timers",
            "source_event_id": "timer:c6:1",
            "payload_summary_json": {"note": "giant"},
            "session_id": "session-c6",
        }))
        .await;
    assert_eq!(status, StatusCode::OK, "{ingested}");
    assert_eq!(h.state.drive_wake_events(), 1);
    assert_eq!(
        h.wait_event_status("agent-event-1", "failed").await,
        "failed"
    );
    let (status, list) = h.loop_list().await;
    assert_eq!(status, StatusCode::OK);
    let loops = list.as_array().unwrap();
    assert_eq!(loops.len(), 1);
    assert_eq!(loops[0]["status"], "failed");
    assert_eq!(loops[0]["error"], "budget_exceeded");
    assert!(
        loops[0]["estimated_tokens"].as_u64().unwrap() > 100_000,
        "estimate must clear the cap: {}",
        loops[0]["estimated_tokens"]
    );
    assert_eq!(h.count_memories(), 0);
    // Two Inbox rows: the execution-failed row
    // `upsert_execution_tx` opens for every failed
    // Execution close (P0-D, pre-existing) plus the
    // wake-token row for the cap breach.
    assert_eq!(h.count_inbox(), 2);
    let inbox_ids = h.inbox_source_ids();
    assert!(inbox_ids.contains(&"wake-token-agent-event-1".to_string()));
    assert!(
        inbox_ids.iter().any(|id| id.starts_with("event-")),
        "execution-failed row present: {inbox_ids:?}"
    );
    // No wake notification row; the one outbox row is
    // the D1-3 execution-failed row (AO3-J7 — see C4).
    assert!(!h
        .outbox_rows()
        .iter()
        .any(|r| r.dedup_key == "agent-event-1:local_notification"));
    assert_eq!(h.count_outbox(), 1);
}

/// C7 (Q-AO-12): the same giant reply in a
/// FOREGROUND loop completes — the token cap fails
/// background Runs only; a foreground loop only
/// accumulates the estimate.
#[tokio::test]
async fn ao3_c7_foreground_loop_not_capped_by_token_estimate() {
    let giant = "甲".repeat(100_001);
    let giant_reply =
        json!({"choices":[{"message":{"role":"assistant","content":giant,"tool_calls":null},"finish_reason":"stop"}]});
    let h = Harness::serve_scripted(vec![giant_reply]).await;
    let (status, started) = h.loop_start(json!({"prompt": "开始"})).await;
    assert_eq!(status, StatusCode::OK, "{started}");
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let (status, view) = h.loop_resume(&loop_id).await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["status"], "completed");
    assert_eq!(view["final_text"], "甲".repeat(100_001));
    assert!(view["estimated_tokens"].as_u64().unwrap() > 100_000);
    assert_eq!(view["background"], false);
}

/// C8 (Q-AO-12): 48 wakes already started today — the
/// 49th event is not claimed, stays pending, and one
/// Inbox row per day names the breach; a second drive
/// writes no second row.
#[tokio::test]
async fn ao3_c8_daily_cap_holds_49th_event() {
    let h = Harness::serve_recording_driven().await;
    let now = sqlite_store::now_ms() as i64;
    for i in 0..49 {
        let (status, ingested) = h
            .ingest(json!({
                "source_kind": "timers",
                "source_event_id": format!("timer:c8:{i}"),
                "payload_summary_json": {"note": format!("event-{i}")},
                "session_id": format!("session-c8-{i}"),
            }))
            .await;
        assert_eq!(status, StatusCode::OK, "{ingested}");
    }
    // 48 wakes already started today (distinct sessions,
    // so the per-session gate never blocks the claims).
    {
        let mut conn = h.db();
        for i in 1..=48 {
            assert!(
                sqlite_store::claim_agent_event(&mut conn, &format!("agent-event-{i}"), now)
                    .unwrap(),
                "claim event {i}"
            );
        }
    }
    assert_eq!(h.state.drive_wake_events(), 0, "cap exhausted");
    assert_eq!(h.event_row("agent-event-49").status, "pending");
    assert_eq!(h.count_inbox(), 1);
    let ids = h.inbox_source_ids();
    assert_eq!(ids.len(), 1);
    assert!(
        ids[0].starts_with("wake-budget-20"),
        "daily-cap row names the day: {}",
        ids[0]
    );
    assert_eq!(h.state.drive_wake_events(), 0, "still capped");
    assert_eq!(h.count_inbox(), 1, "one row per day, deduped");
    assert_eq!(h.event_row("agent-event-49").status, "pending");
}
