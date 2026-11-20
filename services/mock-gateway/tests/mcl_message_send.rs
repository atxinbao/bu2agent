//! MCL-4 tests (plan §6, S1–S5 + the 6b-1 U-form
//! extras): iMessage send end to end — digest-bound
//! approval + write Execution + single-tool Mandate +
//! `sent_imessages` fact row + `imessage.sent` event +
//! Effect, idempotency, approval gates, redaction,
//! error mapping, and disconnect.
//!
//! HTTP routes only, plus `sqlite_store` reads (sent
//! rows / effects / events / inbox) where HTTP cannot
//! reach the state. The imessage source is a test-only
//! fake (no AppleScript, no real Messages.app): it
//! records sends and can be switched into the
//! not-running / denied / timeout / unavailable failure
//! modes. "Sent" means Messages.app accepted the
//! message, never that it was delivered (Q-MCL-21) — no
//! test here asserts delivery. Every test syncs first:
//! a send names an already-synced thread (Q-MCL-20),
//! never a new one.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::imessage_send::{
    get_sent_imessage, imessage_body_hash, list_sent_imessages, SentIMessageRow,
};
use mock_gateway::imessage_source::{
    participant_hash_for, IMessageError, IMessageSource, IMessageThreadView, SentIMessage,
};
use mock_gateway::operation_binding::{imessage_send_digest, sha256_hex};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-mcl4-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FakeMode {
    Ok,
    NotRunning,
    Denied,
    Timeout,
    Unavailable,
}

/// Test-only fake imessage source: the seeded threads
/// plus a recordable send with the failure modes on
/// demand. Handles are fictional (555-01xx numbers,
/// .invalid domains) and never leave this file — only
/// their hashes reach the gateway. The production
/// AppleScript source is never exercised here (no
/// Messages.app in the VM); the script form itself is
/// guarded by the `send_script` unit test in
/// `imessage_source.rs`, and window validation on a real
/// Mac is the proof of the real send.
struct MutableFakeIMessageSource {
    threads: Mutex<Vec<IMessageThreadView>>,
    mode: Mutex<FakeMode>,
    send_count: AtomicUsize,
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
            sent: Mutex::new(Vec::new()),
        })
    }

    fn set_mode(&self, mode: FakeMode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn send_count(&self) -> usize {
        self.send_count.load(Ordering::SeqCst)
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
            FakeMode::Denied => Err(IMessageError::Denied),
            FakeMode::Timeout => Err(IMessageError::Timeout),
            FakeMode::Unavailable => Err(IMessageError::Unavailable(
                "imessage source unavailable in tests".to_string(),
            )),
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
    source: Arc<MutableFakeIMessageSource>,
}

impl Harness {
    async fn serve() -> Self {
        let dir = temp_dir("imessage-send");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let source = MutableFakeIMessageSource::seeded();
        let state = build_state(&dir, source.clone());
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
            source,
        }
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

    /// Request an `imessage.send` approval and decide it
    /// rejected. Returns the tool_call_id.
    async fn rejected_imessage_send(&self, thread_id: &str, body: &str) -> String {
        let (status, requested) = self
            .post(
                "/approval/request",
                json!({"tool": "imessage.send", "thread_id": thread_id, "body": body}),
            )
            .await;
        assert_eq!(status, 200, "approval request: {requested}");
        let id = requested["tool_call_id"].as_str().unwrap().to_string();
        let (status, decided) = self
            .post(
                "/approval/decide",
                json!({"tool_call_id": id, "status": "rejected"}),
            )
            .await;
        assert_eq!(status, 200, "approval decide: {decided}");
        id
    }

    async fn send(&self, value: Value) -> (u16, Value) {
        self.post("/api/imessage/send", value).await
    }

    fn send_body(
        thread_id: &str,
        body: &str,
        key: &str,
        tool_call_id: Option<&str>,
    ) -> Value {
        let mut value = json!({
            "thread_id": thread_id,
            "body": body,
            "idempotency_key": key,
        });
        if let Some(id) = tool_call_id {
            value["tool_call_id"] = json!(id);
        }
        value
    }
}

const BODY: &str = "Hello S1, this is a test message.";

/// S1: send success end to end — fake send + decided
/// digest-bound approval → 200, `sent-imsg-N` row +
/// `imessage.sent` event + Effect succeeded; `body_hash`
/// == sha256(body); the body is in neither the table
/// nor the event; the digest the route recomputes
/// matches the approval's.
#[tokio::test]
async fn s1_send_end_to_end() {
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    let thread_id = h.sync_threads().await;
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, sent) = h
        .send(Harness::send_body(&thread_id, BODY, "key-s1", Some(&approval)))
        .await;
    assert_eq!(status, 200, "send: {sent}");
    assert_eq!(sent["sent_id"], "sent-imsg-1");
    assert_eq!(sent["thread_id"], thread_id);
    assert_eq!(sent["body_hash"], sha256_hex(BODY.as_bytes()));
    assert_eq!(sent["native_id"], Value::Null);
    assert_eq!(sent["replayed"], false);
    assert_eq!(h.source.send_count(), 1);
    assert_eq!(h.source.sent(), vec![("chat-a".to_string(), BODY.to_string())]);
    let rows = h.sent_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].sent_id, "sent-imsg-1");
    assert_eq!(rows[0].body_hash, imessage_body_hash(BODY));
    let events = h.imessage_events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].aggregate_kind, "imessage");
    let payload: Value = serde_json::from_str(&events[0].payload_json).unwrap();
    assert_eq!(payload["sent_id"], "sent-imsg-1");
    assert_eq!(payload["thread_id"], thread_id);
    assert_eq!(payload["recipient_count"], 1);
    assert_eq!(
        payload["recipient_hash"],
        participant_hash_for(&["+15550101001".to_string()])
    );
    assert_eq!(payload["body_hash"], sha256_hex(BODY.as_bytes()));
    assert_eq!(payload["date_ms"], sent["sent_ms"]);
    let effects = h.imessage_effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].status, "succeeded");
    assert_eq!(effects[0].operation, "send");
    assert_eq!(effects[0].effect_id, sent["effect_id"].as_str().unwrap());
    // The route's digest and the approval's digest are
    // the same function over the same inputs.
    assert_eq!(
        imessage_send_digest(&thread_id, &sha256_hex(BODY.as_bytes())),
        effects[0].input_digest.as_deref().unwrap()
    );
}

/// S2: idempotency — same key + same content replays
/// the original row (no second send, no second Effect,
/// no second row); same key + different content is a
/// 409; no key is a 400.
#[tokio::test]
async fn s2_idempotency() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, first) = h
        .send(Harness::send_body(&thread_id, BODY, "key-s2", Some(&approval)))
        .await;
    assert_eq!(status, 200, "first send: {first}");
    assert_eq!(h.source.send_count(), 1);

    // Replay: same key + same content → 200 original,
    // no second send, no second Effect, no second row.
    // A replay needs no approval (the row is the proof).
    let (status, replay) = h
        .send(Harness::send_body(&thread_id, BODY, "key-s2", None))
        .await;
    assert_eq!(status, 200, "replay: {replay}");
    assert_eq!(replay["sent_id"], "sent-imsg-1");
    assert_eq!(replay["replayed"], true);
    assert_eq!(h.source.send_count(), 1, "replay must not send");
    assert_eq!(h.imessage_effects().len(), 1, "replay builds no new Effect");
    assert_eq!(h.sent_rows().len(), 1, "replay builds no new row");
    assert_eq!(h.imessage_events().len(), 1, "replay emits no new event");

    // Same key + different content → 409.
    let (status, conflict) = h
        .send(Harness::send_body(&thread_id, "A different body", "key-s2", None))
        .await;
    assert_eq!(status, 409, "conflict: {conflict}");
    assert_eq!(conflict["error"], "idempotency_key_conflict");
    assert_eq!(h.source.send_count(), 1, "conflict must not send");

    // Same key + different thread → 409 as well.
    let (status, threads) = h.get("/api/imessage/threads").await;
    assert_eq!(status, 200);
    let thread_2 = threads[1]["thread_id"].as_str().unwrap().to_string();
    let (status, conflict) = h
        .send(Harness::send_body(&thread_2, BODY, "key-s2", None))
        .await;
    assert_eq!(status, 409, "thread conflict: {conflict}");
    assert_eq!(conflict["error"], "idempotency_key_conflict");

    // No key → 400.
    let approval2 = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, missing) = h
        .send(json!({"thread_id": thread_id, "body": BODY, "tool_call_id": approval2}))
        .await;
    assert_eq!(status, 400, "missing key: {missing}");
    assert_eq!(missing["error"], "idempotency_key is required");
}

/// S3: approval gates — no approval, unknown id,
/// rejected, tool-only, digest mismatch (approval
/// unconsumed, no orphaned planned row), consumed
/// replay; unknown thread 404; missing fields 400.
#[tokio::test]
async fn s3_approval_gates() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;

    // No approval at all.
    let (status, body) = h
        .send(Harness::send_body(&thread_id, BODY, "key-s3-none", None))
        .await;
    assert_eq!(status, 403, "no approval: {body}");
    assert_eq!(body["error"], "approval_required");

    // Unknown tool_call_id.
    let (status, body) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s3-unknown",
            Some("approval-999999"),
        ))
        .await;
    assert_eq!(status, 403, "unknown approval: {body}");
    assert_eq!(body["error"], "approval_required");

    // Rejected approval.
    let rejected = h.rejected_imessage_send(&thread_id, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s3-rejected",
            Some(&rejected),
        ))
        .await;
    assert_eq!(status, 403, "rejected approval: {body}");
    assert_eq!(body["error"], "approval_rejected");

    // Tool-only approval (bare request, no thread/body)
    // can never authorize the digest-bound send.
    let (status, requested) = h
        .post("/approval/request", json!({"tool": "imessage.send"}))
        .await;
    assert_eq!(status, 200, "bare request: {requested}");
    assert_eq!(requested["requires_approval"], true);
    let tool_only = requested["tool_call_id"].as_str().unwrap().to_string();
    let (status, decided) = h
        .post(
            "/approval/decide",
            json!({"tool_call_id": tool_only, "status": "approved"}),
        )
        .await;
    assert_eq!(status, 200, "bare decide: {decided}");
    let (status, body) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s3-tool-only",
            Some(&tool_only),
        ))
        .await;
    assert_eq!(status, 403, "tool-only approval: {body}");
    assert_eq!(body["error"], "approval_binding_mismatch");

    // Digest mismatch: approval for content A, send
    // content B. 409, approval unconsumed, and no
    // orphaned planned row is left behind.
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, mismatch) = h
        .send(Harness::send_body(
            &thread_id,
            "A different body",
            "key-s3-mismatch",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "digest mismatch: {mismatch}");
    assert_eq!(mismatch["error"], "approval_digest_mismatch");
    assert_eq!(h.source.send_count(), 0, "mismatch must not send");
    let planned: Vec<_> = h
        .imessage_effects()
        .into_iter()
        .filter(|e| e.approval_id.as_deref() == Some(approval.as_str()) && e.status == "planned")
        .collect();
    assert!(
        planned.is_empty(),
        "mismatch left an orphaned planned row: {planned:?}"
    );
    // The approval is unconsumed: the correct content
    // still sends with the same approval id.
    let (status, sent) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s3-correct",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "retry with the same approval: {sent}");
    assert_eq!(h.source.send_count(), 1);

    // Consumed-approval replay: the same approval id
    // with different content (and a new key) is a 409,
    // and no second Effect is planned for it.
    let (status, replay) = h
        .send(Harness::send_body(
            &thread_id,
            "Yet another body",
            "key-s3-consumed",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "consumed replay: {replay}");
    assert_eq!(replay["error"], "approval_already_consumed");
    assert_eq!(h.imessage_effects().len(), 1, "no second Effect");
    assert_eq!(h.source.send_count(), 1);

    // Unknown thread → 404 (Q-MCL-20: no new threads).
    let approval2 = h.approved_imessage_send("thread-999999", BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            "thread-999999",
            BODY,
            "key-s3-unknown-thread",
            Some(&approval2),
        ))
        .await;
    assert_eq!(status, 404, "unknown thread: {body}");
    assert_eq!(body["error"], "unknown imessage thread");
    assert_eq!(h.source.send_count(), 1, "unknown thread must not send");

    // Missing thread_id / body → 400.
    let (status, body) = h
        .send(json!({"body": BODY, "idempotency_key": "key-s3-no-thread", "tool_call_id": approval2}))
        .await;
    assert_eq!(status, 400, "missing thread: {body}");
    let (status, body) = h
        .send(json!({"thread_id": thread_id, "idempotency_key": "key-s3-no-body", "tool_call_id": approval2}))
        .await;
    assert_eq!(status, 400, "missing body: {body}");
}

/// S4: error mapping — Messages not running 502
/// (Effect failed), Automation denied 403 (Effect
/// failed), script timeout 502 (Effect unknown + Inbox
/// row), non-macOS/unavailable 501 (Effect failed).
#[tokio::test]
async fn s4_error_mapping() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;

    // Messages not running → 502, Effect failed, no row.
    h.source.set_mode(FakeMode::NotRunning);
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s4-not-running",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 502, "not running: {body}");
    assert_eq!(body["error"], "Messages is not running");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.source.send_count(), 0);
    assert!(h.sent_rows().is_empty());
    let effect = h
        .imessage_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Automation denied → 403, Effect failed.
    h.source.set_mode(FakeMode::Denied);
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s4-denied",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 403, "denied: {body}");
    assert_eq!(body["error"], "imessage automation denied");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .imessage_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Script timeout → 502, Effect unknown + an
    // `unknown_requires_reconciliation` Inbox row. The
    // send may or may not have happened: no row, no
    // guess.
    h.source.set_mode(FakeMode::Timeout);
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s4-timeout",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 502, "timeout: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .imessage_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "unknown");
    assert!(h.sent_rows().is_empty(), "timeout must not record a row");
    let conn = h.db();
    let inbox = sqlite_store::list_inbox(&conn, Some("unknown_requires_reconciliation"), true)
        .expect("inbox");
    assert!(
        inbox
            .iter()
            .any(|row| row.effect_id.as_deref() == Some(effect_id.as_str())),
        "no unknown_requires_reconciliation row for {effect_id}: {inbox:?}"
    );
    drop(conn);

    // Unavailable (the non-macOS production source) →
    // 501, Effect failed.
    h.source.set_mode(FakeMode::Unavailable);
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s4-unavailable",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 501, "unavailable: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .imessage_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");
}

/// S5: a disconnected imessage-source rejects new
/// writes with 409 `source_disconnected`; reads are
/// unaffected.
#[tokio::test]
async fn s5_disconnected_source_rejects_writes() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let (status, disconnected) = h.post("/sources/imessage-source/disconnect", json!({})).await;
    assert_eq!(status, 200, "disconnect: {disconnected}");
    assert_eq!(disconnected["status"], "disconnected");
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &thread_id,
            BODY,
            "key-s5-disconnected",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "disconnected write: {body}");
    assert_eq!(body["error"], "source_disconnected");
    assert_eq!(h.source.send_count(), 0, "disconnected must not send");
    assert!(h.sent_rows().is_empty());
    // Reads are unaffected by the disconnect.
    let (status, threads) = h.get("/api/imessage/threads").await;
    assert_eq!(status, 200, "disconnected read: {threads}");
    assert_eq!(threads.as_array().unwrap().len(), 2);
}

/// S6: redaction — the `imessage.sent` payload carries
/// only whitelisted fields; no body, handle, or name
/// anywhere in the event log; the fold holds.
#[tokio::test]
async fn s6_redaction() {
    let secret_body = "The launch code is hunter2 and the key is sk-live-TEST123.";
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let approval = h.approved_imessage_send(&thread_id, secret_body).await;
    let (status, sent) = h
        .send(Harness::send_body(
            &thread_id,
            secret_body,
            "key-s6",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "send: {sent}");

    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "imessage.sent")
            .count(),
        1
    );
    for event in &events {
        for forbidden in ["hunter2", "sk-live-TEST123", "The launch code is", "+15550101001"] {
            assert!(
                !event.payload_json.contains(forbidden),
                "event {} leaks {forbidden}: {}",
                event.event_type,
                event.payload_json
            );
        }
    }
    let sent_event = events.iter().find(|e| e.event_type == "imessage.sent").unwrap();
    let payload: Value = serde_json::from_str(&sent_event.payload_json).unwrap();
    let keys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert_eq!(keys.len(), 10, "no extra fields: {payload}");
    assert_eq!(payload["body_hash"], sha256_hex(secret_body.as_bytes()));
    assert_eq!(payload["recipient_count"], 1);
    // The body is in no row either (only its hash).
    let row = get_sent_imessage(&conn, "sent-imsg-1")
        .expect("get")
        .expect("row");
    let row_json = serde_json::to_string(&row).unwrap();
    assert!(!row_json.contains("hunter2"), "row leaks body: {row_json}");
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// S7: sent list / get — the list returns the row, get
/// returns it by `sent-imsg-N`, unknown ids are 404.
#[tokio::test]
async fn s7_sent_list_get() {
    let h = Harness::serve().await;
    let thread_id = h.sync_threads().await;
    let approval = h.approved_imessage_send(&thread_id, BODY).await;
    let (status, sent) = h
        .send(Harness::send_body(&thread_id, BODY, "key-s7", Some(&approval)))
        .await;
    assert_eq!(status, 200, "send: {sent}");
    let (status, list) = h.get("/api/imessage/sent").await;
    assert_eq!(status, 200, "list: {list}");
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["sent_id"], "sent-imsg-1");
    assert!(!list[0].to_string().contains(BODY), "list leaks body");
    let (status, one) = h.get("/api/imessage/sent/sent-imsg-1").await;
    assert_eq!(status, 200, "get: {one}");
    assert_eq!(one["sent_id"], "sent-imsg-1");
    let (status, missing) = h.get("/api/imessage/sent/sent-imsg-999999").await;
    assert_eq!(status, 404, "unknown sent: {missing}");
    assert_eq!(missing["error"], "unknown sent imessage");
}
