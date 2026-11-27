//! D1-3 C-2 tests (plan §4 + Q-C-11/13, Q-C2-01): timed
//! iMessage sends — schedule via POST /api/imessage/send
//! with `deliver_at_ms`, kind dispatch in the row scan,
//! the delivery execute, failure mapping (Q-C-06),
//! manual deliver/requeue, restart continuity (Q-C-12),
//! and the body-privacy rule (the body lives only in
//! `scheduled_imessage_sends` — never in any event
//! payload, never in the Outbox row).
//!
//! HTTP routes only, plus direct store reads where HTTP
//! cannot reach the state (body rows, approval consumed
//! flags, events, inbox). The imessage source is the same
//! test-only fake as tests/mcl_message_send.rs (no
//! AppleScript, no real Messages.app). The due pass is
//! driven directly through `AppState::deliver_due_outbox`
//! (the 10 s sweeper tick is too slow to wait for).
//! "Sent" means Messages.app accepted the message,
//! never that it was delivered (Q-MCL-21).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mock_gateway::imessage_send::{
    imessage_body_hash, list_sent_imessages, SentIMessageRow,
};
use mock_gateway::imessage_source::{
    participant_hash_for, IMessageError, IMessageSource, IMessageThreadView, SentIMessage,
};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{
    self, InboxRow, OutboxRow, ScheduledIMessageSendRow, DB_SCHEMA_VERSION,
};
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-d1c2-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FakeMode {
    Ok,
    NotRunning,
    Timeout,
}

/// Test-only fake imessage source: the seeded threads
/// plus a recordable send with the failure modes on
/// demand. Handles are fictional (555-01xx numbers,
/// .invalid domains) and never leave this file — only
/// their hashes reach the gateway. Copied from
/// tests/mcl_message_send.rs (same fake, same modes).
struct MutableFakeIMessageSource {
    threads: Mutex<Vec<IMessageThreadView>>,
    mode: Mutex<FakeMode>,
    send_count: AtomicUsize,
    /// Every send call, whatever the mode — the
    /// `send_count` above only counts successes, so a
    /// failed attempt is invisible to it.
    attempt_count: AtomicUsize,
    sent: Mutex<Vec<(String, String)>>,
}

impl MutableFakeIMessageSource {
    fn seeded() -> Arc<Self> {
        Arc::new(Self {
            threads: Mutex::new(vec![
                thread_view("chat-a", "iMessage", &["+15550101001"]),
                thread_view("chat-b", "SMS", &["+15550101002", "friend@mock.invalid"]),
            ]),
            mode: Mutex::new(FakeMode::Ok),
            send_count: AtomicUsize::new(0),
            attempt_count: AtomicUsize::new(0),
            sent: Mutex::new(Vec::new()),
        })
    }

    fn set_mode(&self, mode: FakeMode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn send_count(&self) -> usize {
        self.send_count.load(Ordering::SeqCst)
    }

    fn attempt_count(&self) -> usize {
        self.attempt_count.load(Ordering::SeqCst)
    }

    fn sent(&self) -> Vec<(String, String)> {
        self.sent.lock().unwrap().clone()
    }
}

impl IMessageSource for MutableFakeIMessageSource {
    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<IMessageThreadView>, IMessageError> {
        let views = self.threads.lock().unwrap().clone();
        Ok(views.into_iter().skip(offset).take(limit).collect())
    }

    fn send(&self, native_id: &str, body: &str) -> Result<SentIMessage, IMessageError> {
        self.attempt_count.fetch_add(1, Ordering::SeqCst);
        match *self.mode.lock().unwrap() {
            FakeMode::Ok => {
                self.send_count.fetch_add(1, Ordering::SeqCst);
                self.sent
                    .lock()
                    .unwrap()
                    .push((native_id.to_string(), body.to_string()));
                // The real script has no read-back: the
                // sent id stays None, never fabricated.
                Ok(SentIMessage { native_id: None })
            }
            FakeMode::NotRunning => Err(IMessageError::NotRunning),
            FakeMode::Timeout => Err(IMessageError::Timeout),
        }
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

fn build_state(dir: &Path, source: Arc<dyn IMessageSource>) -> AppState {
    let state = AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_tasks_path(dir.join("tasks.jsonl"))
    .with_task_drafts_dir(dir.join("drafts"))
    .with_deepseek(mock_gateway::deepseek::DeepSeekClient::new(
        mock_gateway::deepseek::DeepSeekConfig {
            base_url: "http://127.0.0.1:1".to_string(),
            model: "fixture".to_string(),
            api_key: Some("fake-mcl4-key".to_string()),
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
    .with_imessage_source(source);
    let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    state
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    // Kept alive for the test's lifetime (dropping the
    // handle detaches the server task; the field exists
    // so the task is tracked, not read).
    #[allow(dead_code)]
    server: Option<tokio::task::JoinHandle<()>>,
    // A clone of the served state, so tests can drive
    // the sweeper's due pass directly (the 10 s tick is
    // too slow to wait for).
    state: AppState,
    source: Arc<MutableFakeIMessageSource>,
}

impl Harness {
    async fn serve() -> Self {
        let dir = temp_dir("imessage-timed-send");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let source = MutableFakeIMessageSource::seeded();
        let state = build_state(&dir, source.clone());
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
            server: Some(server),
            state: harness_state,
            source,
        }
    }

    /// Restart the gateway on the same directory (same
    /// SQLite file, same JSONL files): the production
    /// restart path, not a fresh database.
    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let source = MutableFakeIMessageSource::seeded();
        let state = build_state(&self.dir, source.clone());
        self.state = state.clone();
        self.source = source;
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

    fn sent_rows(&self) -> Vec<SentIMessageRow> {
        let conn = self.db();
        list_sent_imessages(&conn).expect("list sent")
    }

    fn imessage_effects(&self) -> Vec<mock_gateway::effect_store::EffectFileRecord> {
        let conn = self.db();
        sqlite_store::list_effects(&conn)
            .expect("effects")
            .into_iter()
            .filter(|e| e.capability == "imessage")
            .collect()
    }

    fn imessage_events(&self) -> Vec<sqlite_store::EventRow> {
        let conn = self.db();
        sqlite_store::list_events(&conn)
            .expect("events")
            .into_iter()
            .filter(|e| e.event_type == "imessage.sent")
            .collect()
    }

    fn outbox_rows(&self) -> Vec<OutboxRow> {
        let conn = self.db();
        sqlite_store::list_outbox(&conn).expect("list outbox")
    }

    fn outbox_row(&self, outbox_id: &str) -> OutboxRow {
        let conn = self.db();
        sqlite_store::get_outbox(&conn, outbox_id)
            .expect("get outbox")
            .expect("outbox row exists")
    }

    fn body_row(&self, outbox_id: &str) -> Option<ScheduledIMessageSendRow> {
        let conn = self.db();
        sqlite_store::get_scheduled_imessage_send(&conn, outbox_id).expect("body row lookup")
    }

    fn approval_consumed(&self, approval_id: &str) -> bool {
        let conn = self.db();
        conn.query_row(
            "SELECT consumed FROM approvals WHERE approval_id = ?1",
            rusqlite::params![approval_id],
            |row| row.get::<_, i64>(0),
        )
        .expect("approval row exists")
            == 1
    }

    fn open_inbox(&self) -> Vec<InboxRow> {
        let conn = self.db();
        sqlite_store::list_inbox(&conn, None, true).expect("list inbox")
    }

    fn all_event_payloads(&self) -> Vec<String> {
        let conn = self.db();
        sqlite_store::list_events(&conn)
            .expect("events")
            .into_iter()
            .map(|e| e.payload_json)
            .collect()
    }

    /// The sweeper's due pass, driven directly.
    /// AO-5: a fixed 10:00 clock so the Dispatcher gates
    /// (quiet hours 23:00-08:00) never hold rows no
    /// matter what hour CI runs in.
    async fn deliver_due(&self) -> usize {
        let clock = sqlite_store::DispatcherClock::fixed_for_test(
            10,
            0,
            sqlite_store::now_ms() as i64,
        );
        self.state.deliver_due_outbox_with_clock(Some(clock)).await
    }

    /// Re-due a pending row to the past through the
    /// manual schedule route (ungated, AO5-J7): the
    /// test's way to make a backoff row due again
    /// without waiting out the tier.
    async fn redue_to_past(&self, outbox_id: &str) {
        let (status, body) = self
            .post(
                &format!("/outbox/{outbox_id}/schedule"),
                json!({ "deliver_at_ms": now_ms() - 1_000 }),
            )
            .await;
        assert_eq!(status, 200, "redue to past: {body}");
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
        let response = self.client.get(format!("{}{}", self.url, path)).send().await.unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    /// Sync the seeded threads so the send has a real
    /// `thread-N` to name. Returns the first thread id.
    async fn sync_threads(&self) -> String {
        let (status, synced) = self.post("/api/imessage/sync", json!({})).await;
        assert_eq!(status, 200, "sync: {synced}");
        assert_eq!(synced["synced_count"], 2);
        let (status, threads) = self.get("/api/imessage/threads").await;
        assert_eq!(status, 200, "threads: {threads}");
        threads[0]["thread_id"].as_str().unwrap().to_string()
    }

    /// Request an `imessage.send` approval and decide it
    /// approved. Returns the tool_call_id.
    async fn approved_imessage_send(&self, thread_id: &str, body: &str) -> String {
        let (status, requested) = self
            .post(
                "/approval/request",
                json!({"tool": "imessage.send", "thread_id": thread_id, "body": body}),
            )
            .await;
        assert_eq!(status, 200, "approval request: {requested}");
        assert_eq!(requested["requires_approval"], true);
        let id = requested["tool_call_id"].as_str().unwrap().to_string();
        let (status, decided) = self
            .post(
                "/approval/decide",
                json!({"tool_call_id": id, "status": "approved"}),
            )
            .await;
        assert_eq!(status, 200, "approval decide: {decided}");
        id
    }

    /// Schedule one send: POST /api/imessage/send with
    /// `deliver_at_ms`. Returns the raw status + body.
    async fn schedule(
        &self,
        thread_id: &str,
        body: &str,
        idempotency_key: &str,
        approval_id: &str,
        deliver_at_ms: i64,
    ) -> (u16, Value) {
        self.post(
            "/api/imessage/send",
            json!({
                "thread_id": thread_id,
                "body": body,
                "idempotency_key": idempotency_key,
                "tool_call_id": approval_id,
                "deliver_at_ms": deliver_at_ms,
            }),
        )
        .await
    }
}

/// C-2 schedule: a pending `imessage_send` Outbox row
/// with its due time, the approval consumed at schedule
/// time, NO Effect row yet, the body row present — and
/// neither the response nor any event carries the body.
#[tokio::test]
async fn c2_schedule_creates_pending_row_and_consumes_approval() {
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    assert_eq!(h.schema_version(), 23);
    let thread_id = h.sync_threads().await;
    let body = "c2-schedule-body-pending";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let due = now_ms() + 3_600_000;
    let (status, scheduled) = h
        .schedule(&thread_id, body, "key-c2-schedule", &approval_id, due)
        .await;
    assert_eq!(status, 200, "schedule: {scheduled}");
    assert_eq!(scheduled["status"], "pending");
    assert_eq!(scheduled["destination_kind"], "imessage_send");
    assert_eq!(scheduled["deliver_at_ms"], due);
    assert!(scheduled.get("effect_id").is_none(), "no Effect row: {scheduled}");
    assert!(
        !scheduled.to_string().contains(body),
        "response carries no body: {scheduled}"
    );
    let outbox_id = scheduled["outbox_id"].as_str().unwrap().to_string();
    assert!(h.approval_consumed(&approval_id), "approval consumed at schedule");
    assert!(h.imessage_effects().is_empty(), "no Effect built at schedule");
    assert!(h.sent_rows().is_empty(), "nothing sent at schedule");
    assert_eq!(h.source.send_count(), 0);
    let stored = h.body_row(&outbox_id).expect("body row present");
    assert_eq!(stored.thread_id, thread_id);
    assert_eq!(stored.body, body);
    assert_eq!(stored.body_hash, imessage_body_hash(body));
    assert_eq!(stored.idempotency_key, "key-c2-schedule");
    assert_eq!(stored.approval_id, approval_id);
    assert!(h.imessage_events().is_empty(), "no sent event at schedule");
}

/// Past-due schedule → the next due pass delivers it:
/// sent row + `imessage.sent` event (exactly the 10
/// keys, no body plaintext anywhere) + succeeded Effect
/// + Outbox row `sent` + body row deleted.
#[tokio::test]
async fn c2_past_due_delivered_on_next_pass() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let body = "c2-past-due-body-delivers";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let (status, scheduled) = h
        .schedule(&thread_id, body, "key-c2-past-due", &approval_id, now_ms() - 1_000)
        .await;
    assert_eq!(status, 200, "schedule: {scheduled}");
    let outbox_id = scheduled["outbox_id"].as_str().unwrap().to_string();
    assert_eq!(h.deliver_due().await, 1, "one due row delivered");
    assert_eq!(h.source.send_count(), 1);
    assert_eq!(h.source.sent(), vec![("chat-a".to_string(), body.to_string())]);
    let sent = h.sent_rows();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].thread_id, thread_id);
    assert_eq!(sent[0].body_hash, imessage_body_hash(body));
    assert_eq!(sent[0].idempotency_key, "key-c2-past-due");
    let row = h.outbox_row(&outbox_id);
    assert_eq!(row.status, "sent");
    assert_eq!(row.attempts, 1);
    assert!(h.body_row(&outbox_id).is_none(), "body row deleted on sent");
    let effects = h.imessage_effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].status, "succeeded");
    assert_eq!(effects[0].effect_id, sent[0].effect_id);
    let events = h.imessage_events();
    assert_eq!(events.len(), 1);
    let payload: Value = serde_json::from_str(&events[0].payload_json).unwrap();
    let keys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        vec![
            "body_hash",
            "created_ms",
            "date_ms",
            "execution_id",
            "native_id",
            "recipient_count",
            "recipient_hash",
            "sent_id",
            "source_id",
            "thread_id",
        ],
        "exactly the 10 imessage.sent keys"
    );
    assert!(
        !events[0].payload_json.contains(body),
        "sent event carries no body"
    );
    for payload in h.all_event_payloads() {
        assert!(!payload.contains(body), "no event carries the body: {payload}");
    }
}

/// A future-due row is not delivered by the pass and
/// stays pending with its body row retained.
#[tokio::test]
async fn c2_future_due_stays_pending() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let body = "c2-future-body-waits";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let (status, scheduled) = h
        .schedule(&thread_id, body, "key-c2-future", &approval_id, now_ms() + 3_600_000)
        .await;
    assert_eq!(status, 200, "schedule: {scheduled}");
    let outbox_id = scheduled["outbox_id"].as_str().unwrap().to_string();
    assert_eq!(h.deliver_due().await, 0, "nothing due yet");
    assert!(h.sent_rows().is_empty());
    assert_eq!(h.source.send_count(), 0);
    assert_eq!(h.outbox_row(&outbox_id).status, "pending");
    assert!(h.body_row(&outbox_id).is_some(), "body retained while pending");
    assert!(h.imessage_effects().is_empty(), "no Effect before delivery");
}

/// Schedule replay: once the send has happened, a
/// second schedule with the same key + content returns
/// the sent row (`replayed: true`) and creates nothing
/// — no second Outbox row, no second send, no second
/// Effect. The replay check precedes the approval peek,
/// so the already-consumed approval is never re-checked.
#[tokio::test]
async fn c2_schedule_replay_after_send() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let body = "c2-replay-body";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let (status, scheduled) = h
        .schedule(&thread_id, body, "key-c2-replay", &approval_id, now_ms() - 1_000)
        .await;
    assert_eq!(status, 200, "schedule: {scheduled}");
    assert_eq!(h.deliver_due().await, 1);
    let sent_id = h.sent_rows()[0].sent_id.clone();
    let (status, replayed) = h
        .schedule(&thread_id, body, "key-c2-replay", &approval_id, now_ms() - 1_000)
        .await;
    assert_eq!(status, 200, "replay schedule: {replayed}");
    assert_eq!(replayed["replayed"], true);
    assert_eq!(replayed["sent_id"], sent_id);
    assert_eq!(h.outbox_rows().len(), 1, "no second Outbox row");
    assert_eq!(h.source.send_count(), 1, "no second send");
    assert_eq!(h.imessage_effects().len(), 1, "no second Effect");
}

/// Same key + different content → 409, and the second
/// approval stays unconsumed (the conflict is decided
/// before anything is burned).
#[tokio::test]
async fn c2_schedule_same_key_different_content_conflicts() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let approval_a = h.approved_imessage_send(&thread_id, "c2-conflict-body-a").await;
    let (status, scheduled) = h
        .schedule(&thread_id, "c2-conflict-body-a", "key-c2-conflict", &approval_a, now_ms() - 1_000)
        .await;
    assert_eq!(status, 200, "first schedule: {scheduled}");
    let approval_b = h.approved_imessage_send(&thread_id, "c2-conflict-body-b").await;
    let (status, conflicted) = h
        .schedule(&thread_id, "c2-conflict-body-b", "key-c2-conflict", &approval_b, now_ms() - 1_000)
        .await;
    assert_eq!(status, 409, "conflict: {conflicted}");
    assert_eq!(conflicted["error"], "idempotency_key_conflict");
    assert_eq!(h.outbox_rows().len(), 1, "no second row");
    assert!(!h.approval_consumed(&approval_b), "second approval not burned");
    assert_eq!(h.source.send_count(), 0);
}

/// An unknown thread is 404 at schedule time, and the
/// approval stays unconsumed.
#[tokio::test]
async fn c2_schedule_unknown_thread_404() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let _ = thread_id;
    let approval_id = h.approved_imessage_send("thread-999", "c2-unknown-thread").await;
    let (status, body) = h
        .schedule("thread-999", "c2-unknown-thread", "key-c2-unknown", &approval_id, now_ms() - 1_000)
        .await;
    assert_eq!(status, 404, "unknown thread: {body}");
    assert_eq!(body["error"], "unknown imessage thread");
    assert!(h.outbox_rows().is_empty(), "no row for an unknown thread");
    assert!(!h.approval_consumed(&approval_id), "approval not burned");
}

/// A disconnected source rejects the schedule with 409
/// and creates nothing.
#[tokio::test]
async fn c2_disconnect_409_at_schedule() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let (status, disconnected) = h.post("/sources/imessage-source/disconnect", json!({})).await;
    assert_eq!(status, 200, "disconnect: {disconnected}");
    let approval_id = h.approved_imessage_send(&thread_id, "c2-disconnect-body").await;
    let (status, body) = h
        .schedule(&thread_id, "c2-disconnect-body", "key-c2-disconnect", &approval_id, now_ms() - 1_000)
        .await;
    assert_eq!(status, 409, "disconnected schedule: {body}");
    assert_eq!(body["error"], "source_disconnected");
    assert!(h.outbox_rows().is_empty(), "no row while disconnected");
    assert!(!h.approval_consumed(&approval_id), "approval not burned");
}

/// Timeout (Q-C-06, as amended by AO-5/Q-AO-12): the
/// Effect goes unknown + an Inbox row opens, the body
/// is retained — and the Outbox row goes pending with
/// a 1-minute backoff (AO-5 replaces C-2's immediate
/// `failed` with bounded retry: attempts 1-3 reschedule
/// at 1/5/15 min, the 4th failure marks failed). A
/// second due pass right after does NOT resend — the
/// row is not due until the backoff elapses, so there
/// is still no immediate auto-retry.
#[tokio::test]
async fn c2_timeout_backoff_no_immediate_retry() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let body = "c2-timeout-body-kept";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let (status, scheduled) = h
        .schedule(&thread_id, body, "key-c2-timeout", &approval_id, now_ms() - 1_000)
        .await;
    assert_eq!(status, 200, "schedule: {scheduled}");
    let outbox_id = scheduled["outbox_id"].as_str().unwrap().to_string();
    h.source.set_mode(FakeMode::Timeout);
    let before = now_ms();
    assert_eq!(h.deliver_due().await, 0, "a timeout delivers nothing");
    assert!(h.sent_rows().is_empty(), "no sent row on timeout");
    assert_eq!(h.source.attempt_count(), 1, "one AppleScript attempt");
    assert_eq!(h.source.send_count(), 0, "no successful send on timeout");
    let effects = h.imessage_effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].status, "unknown");
    let inbox = h.open_inbox();
    assert!(
        inbox.iter().any(|r| r.kind == "unknown_requires_reconciliation"
            && r.effect_id.as_deref() == Some(effects[0].effect_id.as_str())),
        "unknown Inbox row opened for the timed-out Effect: {inbox:?}"
    );
    let row = h.outbox_row(&outbox_id);
    assert_eq!(row.status, "pending", "AO-5: first failure backs off, not failed");
    assert_eq!(row.attempts, 1);
    let due = row.deliver_at_ms.expect("backoff sets a due time");
    assert!(
        (60_000..=120_000).contains(&(due - before)),
        "first backoff tier is 1 min: delay {}",
        due - before
    );
    assert!(h.body_row(&outbox_id).is_some(), "body retained on timeout");
    assert_eq!(h.deliver_due().await, 0, "second pass does not retry before the backoff elapses");
    assert_eq!(h.source.attempt_count(), 1, "no second AppleScript attempt");
    assert_eq!(h.source.send_count(), 0, "still no successful send");
    assert_eq!(h.imessage_effects().len(), 1, "no second Effect");
    for payload in h.all_event_payloads() {
        assert!(!payload.contains(body), "no event carries the body: {payload}");
    }
}

/// Rejection (Q-C-06, as amended by AO-5/Q-AO-12):
/// the Effect fails on every attempt; attempts 1-3
/// back off (pending, 1/5/15 min), the 4th marks the
/// row failed and the body is retained — then a manual
/// requeue redelivers it successfully under a fresh
/// Effect.
#[tokio::test]
async fn c2_rejection_requeue_redelivers() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let body = "c2-requeue-body-redelivers";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let (status, scheduled) = h
        .schedule(&thread_id, body, "key-c2-requeue", &approval_id, now_ms() - 1_000)
        .await;
    assert_eq!(status, 200, "schedule: {scheduled}");
    let outbox_id = scheduled["outbox_id"].as_str().unwrap().to_string();
    h.source.set_mode(FakeMode::NotRunning);
    // AO-5: four failed attempts to reach `failed`
    // (attempts 1-3 back off; re-due through the manual
    // schedule route between attempts).
    for attempt in 1..=4 {
        assert_eq!(h.deliver_due().await, 0, "a rejection delivers nothing");
        let row = h.outbox_row(&outbox_id);
        assert_eq!(row.attempts, attempt, "attempt {attempt}");
        if attempt < 4 {
            assert_eq!(row.status, "pending", "attempt {attempt} backs off");
            h.redue_to_past(&outbox_id).await;
        }
    }
    assert!(h.sent_rows().is_empty());
    let effects = h.imessage_effects();
    assert_eq!(effects.len(), 4, "one failed Effect per attempt");
    assert!(effects.iter().all(|e| e.status == "failed"));
    let row = h.outbox_row(&outbox_id);
    assert_eq!(row.status, "failed", "4th failure marks failed");
    assert_eq!(row.attempts, 4);
    assert!(h.body_row(&outbox_id).is_some(), "body retained on rejection");
    let (status, requeued) = h.post(&format!("/outbox/{outbox_id}/requeue"), json!({})).await;
    assert_eq!(status, 200, "requeue: {requeued}");
    assert_eq!(requeued["status"], "pending");
    h.source.set_mode(FakeMode::Ok);
    assert_eq!(h.deliver_due().await, 1, "requeued row redelivers");
    assert_eq!(h.sent_rows().len(), 1);
    assert_eq!(h.source.attempt_count(), 5, "four failed attempts + redelivery");
    assert_eq!(h.source.send_count(), 1, "one real send");
    assert_eq!(h.outbox_row(&outbox_id).status, "sent");
    assert!(h.body_row(&outbox_id).is_none(), "body deleted on sent");
    let effects = h.imessage_effects();
    assert_eq!(effects.len(), 5, "four failed attempts + redelivery");
    assert!(effects.iter().any(|e| e.status == "failed"));
    assert!(effects.iter().any(|e| e.status == "succeeded"));
}

/// Restart continuity (Q-C-12): a due row survives a
/// store reopen and is delivered by the next pass.
#[tokio::test]
async fn c2_restart_backfill() {
    let mut h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let body = "c2-restart-body-survives";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let (status, scheduled) = h
        .schedule(&thread_id, body, "key-c2-restart", &approval_id, now_ms() - 1_000)
        .await;
    assert_eq!(status, 200, "schedule: {scheduled}");
    let outbox_id = scheduled["outbox_id"].as_str().unwrap().to_string();
    h.restart().await;
    assert_eq!(h.schema_version(), 23);
    assert_eq!(h.outbox_row(&outbox_id).status, "pending");
    assert!(h.body_row(&outbox_id).is_some(), "body survives restart");
    // No re-sync needed: the threads are already in the
    // store (a re-sync would report 0 new rows). Just
    // confirm the thread id is still thread-1.
    let (status, threads) = h.get("/api/imessage/threads").await;
    assert_eq!(status, 200, "threads after restart: {threads}");
    assert_eq!(threads[0]["thread_id"], "thread-1");
    assert_eq!(h.deliver_due().await, 1, "due row delivered after restart");
    assert_eq!(h.sent_rows().len(), 1);
    assert_eq!(h.outbox_row(&outbox_id).status, "sent");
    assert!(h.body_row(&outbox_id).is_none(), "body deleted on sent");
}

/// Manual deliver (Q-C-04) is an immediate override:
/// a future-due row sends NOW, and a repeat call is a
/// no-op (the row is already `sent`).
#[tokio::test]
async fn c2_manual_deliver_sends_now() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let body = "c2-manual-body-now";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let (status, scheduled) = h
        .schedule(&thread_id, body, "key-c2-manual", &approval_id, now_ms() + 3_600_000)
        .await;
    assert_eq!(status, 200, "schedule: {scheduled}");
    let outbox_id = scheduled["outbox_id"].as_str().unwrap().to_string();
    let (status, delivered) = h.post(&format!("/outbox/{outbox_id}/deliver"), json!({})).await;
    assert_eq!(status, 200, "manual deliver: {delivered}");
    assert_eq!(delivered["status"], "sent");
    assert_eq!(delivered["outbox_id"], outbox_id);
    assert_eq!(h.sent_rows().len(), 1);
    assert_eq!(h.source.send_count(), 1);
    assert!(h.body_row(&outbox_id).is_none(), "body deleted on sent");
    let (status, again) = h.post(&format!("/outbox/{outbox_id}/deliver"), json!({})).await;
    assert_eq!(status, 200, "repeat deliver: {again}");
    assert_eq!(again["status"], "sent");
    assert_eq!(h.source.send_count(), 1, "no second send on repeat deliver");
    assert_eq!(h.sent_rows().len(), 1);
}

/// A schedule without `deliver_at_ms` is the old
/// immediate send, byte-identical: it sends at once and
/// creates no Outbox row.
#[tokio::test]
async fn c2_no_due_flag_is_still_immediate_send() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let body = "c2-immediate-body-unchanged";
    let approval_id = h.approved_imessage_send(&thread_id, body).await;
    let (status, sent) = h
        .post(
            "/api/imessage/send",
            json!({
                "thread_id": thread_id,
                "body": body,
                "idempotency_key": "key-c2-immediate",
                "tool_call_id": approval_id,
            }),
        )
        .await;
    assert_eq!(status, 200, "immediate send: {sent}");
    assert_eq!(sent["replayed"], false);
    assert_eq!(h.sent_rows().len(), 1);
    assert_eq!(h.source.send_count(), 1);
    assert!(h.outbox_rows().is_empty(), "no Outbox row for an immediate send");
    assert!(h.body_row("outbox-1").is_none(), "no body row for an immediate send");
}
