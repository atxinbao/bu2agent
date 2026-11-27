//! D1-3 C-1 tests (plan §3, T1–T7): timed Outbox
//! delivery — the `deliver_at_ms` column, the sweeper row
//! scan (`due_outbox_rows`), the dual-channel split
//! (event channel skips timed rows), the
//! `POST /outbox/{id}/schedule` route, reschedule +
//! `outbox.rescheduled`, restart continuity, and the
//! legacy TEXT storage regression.
//!
//! HTTP routes only, plus direct-SQL time simulation for
//! T1/T2/T3/T6 (plan §3, the E2-D D1/D2 precedent: move
//! `deliver_at_ms` into the past through `Harness::db()`;
//! the deliveries themselves are real) and direct store
//! calls for the two consumers (the 10 s sweeper tick is
//! too slow to wait for). Loops run with auto-drive off,
//! so a started loop's Execution stays `running` until
//! the test transitions it — no race with the driver
//! completing the loop on its own.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use mock_gateway::sqlite_store::OutboxRow;
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-d1c1-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct ScriptModel {
    steps: Mutex<VecDeque<ModelReply>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(replies.into()),
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
        let _ = messages;
        let reply = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move { Ok(reply) })
    }
}

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

fn build_state(dir: &Path, model: Arc<ScriptModel>) -> AppState {
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
            api_key: Some("fake-d1c1-key".to_string()),
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
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let state = build_state(&dir, model.clone());
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
        let model = Arc::new(ScriptModel::new(vec![]));
        self.model = model.clone();
        let state = build_state(&self.dir, model);
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

    async fn execution_for_loop(&self, loop_id: &str) -> Value {
        let list = self.ok_get("/executions/list").await;
        list.as_array()
            .unwrap()
            .iter()
            .find(|e| e["loop_id"] == loop_id)
            .cloned()
            .unwrap_or_else(|| panic!("no execution for loop {loop_id}: {list}"))
    }

    /// Start a loop (auto-drive off, so the Execution
    /// stays `running`) and transition it to `completed`
    /// through the route. Returns (loop_id, execution_id).
    async fn terminal_execution(&self) -> (String, String) {
        let started = self
            .ok("/agent/loop/start", json!({"prompt": "d1c1"}))
            .await;
        let loop_id = started["loop_id"].as_str().unwrap().to_string();
        let exec = self.execution_for_loop(&loop_id).await;
        let execution_id = exec["execution_id"].as_str().unwrap().to_string();
        assert_eq!(exec["status"], "running");
        let moved = self
            .ok(
                "/executions/transition",
                json!({ "execution_id": execution_id, "status": "completed" }),
            )
            .await;
        assert_eq!(moved["status"], "completed");
        (loop_id, execution_id)
    }

    /// The pending Outbox row for one execution, via the
    /// list route (exactly one row is expected after a
    /// single terminal transition).
    async fn outbox_for(&self, execution_id: &str) -> Value {
        let list = self.ok_get("/outbox/list").await;
        let rows: Vec<&Value> = list
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["execution_id"] == execution_id)
            .collect();
        assert_eq!(rows.len(), 1, "outbox for {execution_id}: {list}");
        rows[0].clone()
    }
}

/// Drive the tick's event channel directly (the Harness
/// serves with `axum::serve`, so the 10 s sweeper does
/// not run in tests; the production tick calls this same
/// function through `AppState::deliver_pending_outbox`).
fn consume(h: &Harness) -> Vec<OutboxRow> {
    let mut conn = h.db();
    // AO-5: drive the channel with a fixed 10:00 clock so
    // the Dispatcher gates (quiet hours 23:00-08:00) never
    // hold rows no matter what hour CI runs in.
    let clock = sqlite_store::DispatcherClock::fixed_for_test(
        10,
        0,
        sqlite_store::now_ms() as i64,
    );
    sqlite_store::consume_outbox_events_with_clock(&mut conn, "outbox-deliverer", "test", Some(&clock))
        .expect("consume")
}

/// Drive the tick's row scan directly: fetch due timed
/// rows and deliver each through the log sink — the same
/// loop `AppState::deliver_due_outbox` runs on the
/// sweeper tick.
fn deliver_due(h: &Harness) -> Vec<OutboxRow> {
    let due = {
        let conn = h.db();
        sqlite_store::due_outbox_rows(&conn, sqlite_store::now_ms() as i64).expect("due rows")
    };
    let mut delivered = Vec::new();
    for row in due {
        let mut conn = h.db();
        if let Some(sent) =
            sqlite_store::deliver_outbox_via_log_sink(&mut conn, &row.outbox_id).expect("deliver")
        {
            delivered.push(sent);
        }
    }
    delivered
}

/// One full sweeper pass, both channels in production
/// order: event channel first, row scan second.
fn tick(h: &Harness) -> (Vec<OutboxRow>, Vec<OutboxRow>) {
    (consume(h), deliver_due(h))
}

fn count_events(conn: &rusqlite::Connection, event_type: &str, aggregate_id: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM events WHERE event_type = ?1 AND aggregate_id = ?2",
        rusqlite::params![event_type, aggregate_id],
        |row| row.get(0),
    )
    .expect("count events")
}

/// Direct-SQL time simulation (the E2-D D1/D2
/// precedent): move a row's due time into the past.
fn set_due(h: &Harness, outbox_id: &str, deliver_at_ms: i64) {
    let conn = h.db();
    conn.execute(
        "UPDATE outbox SET deliver_at_ms = ?1 WHERE outbox_id = ?2",
        rusqlite::params![deliver_at_ms, outbox_id],
    )
    .expect("set due");
}

async fn schedule(h: &Harness, outbox_id: &str, deliver_at_ms: i64) -> Value {
    h.ok(
        &format!("/outbox/{outbox_id}/schedule"),
        json!({ "deliver_at_ms": deliver_at_ms }),
    )
    .await
}

// T1: a due row is not delivered before its due time —
// schedule 60 s out, three ticks later still pending,
// attempts unchanged, no `outbox.sent` event.
#[tokio::test]
async fn t1_timed_row_not_delivered_before_due() {
    let h = Harness::serve(vec![done()]).await;
    let (_, execution_id) = h.terminal_execution().await;
    let row = h.outbox_for(&execution_id).await;
    let outbox_id = row["outbox_id"].as_str().unwrap().to_string();
    assert_eq!(row["status"], "pending");
    assert!(row["deliver_at_ms"].is_null(), "fresh row has no due time");

    let future = sqlite_store::now_ms() as i64 + 60_000;
    let scheduled = schedule(&h, &outbox_id, future).await;
    assert_eq!(scheduled["status"], "pending");
    assert_eq!(scheduled["deliver_at_ms"], future);

    for _ in 0..3 {
        let (via_events, via_scan) = tick(&h);
        assert_eq!(via_events.len(), 0, "event channel must not deliver a timed row");
        assert_eq!(via_scan.len(), 0, "row scan must not deliver before due");
    }
    let row = h.outbox_for(&execution_id).await;
    assert_eq!(row["status"], "pending");
    assert_eq!(row["attempts"], 0, "attempts unchanged");
    assert_eq!(row["deliver_at_ms"], future);
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.sent", &outbox_id), 0);
}

// T2: a due row is delivered exactly once after its due
// time — move the due time into the past, one tick sends
// it (attempts=1); three more ticks change nothing and
// write no second event.
#[tokio::test]
async fn t2_timed_row_delivered_once_when_due() {
    let h = Harness::serve(vec![done()]).await;
    let (_, execution_id) = h.terminal_execution().await;
    let row = h.outbox_for(&execution_id).await;
    let outbox_id = row["outbox_id"].as_str().unwrap().to_string();

    let future = sqlite_store::now_ms() as i64 + 60_000;
    schedule(&h, &outbox_id, future).await;
    set_due(&h, &outbox_id, sqlite_store::now_ms() as i64 - 1_000);

    let (via_events, via_scan) = tick(&h);
    assert_eq!(via_events.len(), 0, "timed row never goes through the event channel");
    assert_eq!(via_scan.len(), 1, "row scan delivers the due row");
    assert_eq!(via_scan[0].outbox_id, outbox_id);
    assert_eq!(via_scan[0].status, "sent");
    let row = h.outbox_for(&execution_id).await;
    assert_eq!(row["status"], "sent");
    assert_eq!(row["attempts"], 1);
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.sent", &outbox_id), 1);

    for _ in 0..3 {
        let (via_events, via_scan) = tick(&h);
        assert_eq!(via_events.len(), 0);
        assert_eq!(via_scan.len(), 0, "no re-delivery");
    }
    let row = h.outbox_for(&execution_id).await;
    assert_eq!(row["status"], "sent");
    assert_eq!(row["attempts"], 1, "attempts unchanged");
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.sent", &outbox_id), 1);
}

// T3: the dual channels neither double-deliver nor drop
// — an immediate row still goes through the event
// channel (the D1-3 E2 assertion, kept verbatim), and a
// timed row whose enqueued event the cursor has already
// passed is still delivered by the row scan (the cursor
// blind-spot regression).
#[tokio::test]
async fn t3_dual_channels_no_double_no_drop() {
    let h = Harness::serve(vec![done(), done()]).await;
    let (_, exec_a) = h.terminal_execution().await;
    let row_a = h.outbox_for(&exec_a).await;
    let id_a = row_a["outbox_id"].as_str().unwrap().to_string();
    let (_, exec_b) = h.terminal_execution().await;
    let row_b = h.outbox_for(&exec_b).await;
    let id_b = row_b["outbox_id"].as_str().unwrap().to_string();

    let future = sqlite_store::now_ms() as i64 + 60_000;
    schedule(&h, &id_b, future).await;

    // One event-channel run: the immediate row is
    // delivered, the timed row is skipped — but the
    // cursor advances past the timed row's enqueued event.
    let delivered = consume(&h);
    assert_eq!(delivered.len(), 1, "consumer delivers the immediate row");
    assert_eq!(delivered[0].outbox_id, id_a);
    let row_a = h.outbox_for(&exec_a).await;
    assert_eq!(row_a["status"], "sent");
    assert_eq!(row_a["attempts"], 1);
    let row_b = h.outbox_for(&exec_b).await;
    assert_eq!(row_b["status"], "pending");
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.sent", &id_b), 0);
    let enqueued_id = sqlite_store::list_events(&conn)
        .expect("events")
        .iter()
        .filter(|e| e.event_type == "outbox.enqueued" && e.aggregate_id == id_b)
        .map(|e| e.id)
        .max()
        .expect("enqueued event for the timed row");
    assert!(
        sqlite_store::get_event_cursor(&conn, "outbox-deliverer").expect("cursor") >= enqueued_id,
        "cursor has passed the timed row's enqueued event"
    );

    // The timed row is now past due; the row scan — not
    // the event channel, which will never see it again —
    // delivers it.
    set_due(&h, &id_b, sqlite_store::now_ms() as i64 - 1_000);
    let (via_events, via_scan) = tick(&h);
    assert_eq!(via_events.len(), 0);
    assert_eq!(via_scan.len(), 1);
    assert_eq!(via_scan[0].outbox_id, id_b);
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.sent", &id_a), 1);
    assert_eq!(count_events(&conn, "outbox.sent", &id_b), 1);
}

// T4: manual deliver of a not-yet-due row sends it
// immediately (Q-C-04: the operator override).
#[tokio::test]
async fn t4_manual_deliver_before_due_sends_immediately() {
    let h = Harness::serve(vec![done()]).await;
    let (_, execution_id) = h.terminal_execution().await;
    let row = h.outbox_for(&execution_id).await;
    let outbox_id = row["outbox_id"].as_str().unwrap().to_string();

    let future = sqlite_store::now_ms() as i64 + 60_000;
    schedule(&h, &outbox_id, future).await;
    let delivered = h
        .ok(&format!("/outbox/{outbox_id}/deliver"), json!({}))
        .await;
    assert_eq!(delivered["status"], "sent");
    assert_eq!(delivered["attempts"], 1);
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.sent", &outbox_id), 1);
}

// T5: reschedule — a second schedule call moves the due
// time and appends an `outbox.rescheduled` event; the
// old due time no longer delivers (checked at the store
// level: the due query at the old time is empty, at the
// new time it finds the row). A sent row is returned
// unchanged with no reschedule event, and a missing /
// non-integer body is a 400, an unknown id a 404.
#[tokio::test]
async fn t5_reschedule_moves_due_and_logs_event() {
    let h = Harness::serve(vec![done()]).await;
    let (_, execution_id) = h.terminal_execution().await;
    let row = h.outbox_for(&execution_id).await;
    let outbox_id = row["outbox_id"].as_str().unwrap().to_string();

    let now = sqlite_store::now_ms() as i64;
    let first = now + 60_000;
    let second = now + 3_600_000;
    schedule(&h, &outbox_id, first).await;
    let rescheduled = schedule(&h, &outbox_id, second).await;
    assert_eq!(rescheduled["status"], "pending");
    assert_eq!(rescheduled["deliver_at_ms"], second);
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.rescheduled", &outbox_id), 2);
    // The old due time does not deliver: the due query
    // at `first` is empty, at `second` it finds the row.
    let at_old = sqlite_store::due_outbox_rows(&conn, first).expect("due at old");
    assert!(at_old.is_empty(), "old due time must not deliver");
    let at_new = sqlite_store::due_outbox_rows(&conn, second).expect("due at new");
    assert_eq!(at_new.len(), 1);
    assert_eq!(at_new[0].outbox_id, outbox_id);

    // Bad bodies are 400, unknown ids are 404.
    let (status, _) = h.post(&format!("/outbox/{outbox_id}/schedule"), json!({})).await;
    assert_eq!(status, 400, "missing deliver_at_ms");
    let (status, _) = h
        .post(
            &format!("/outbox/{outbox_id}/schedule"),
            json!({ "deliver_at_ms": "soon" }),
        )
        .await;
    assert_eq!(status, 400, "non-integer deliver_at_ms");
    let (status, _) = h
        .post("/outbox/outbox-999/schedule", json!({ "deliver_at_ms": second }))
        .await;
    assert_eq!(status, 404, "unknown outbox item");

    // A sent row is returned unchanged: no write, no
    // third rescheduled event.
    h.ok(&format!("/outbox/{outbox_id}/deliver"), json!({})).await;
    let (status, body) = h
        .post(
            &format!("/outbox/{outbox_id}/schedule"),
            json!({ "deliver_at_ms": second + 60_000 }),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "sent");
    assert_eq!(body["deliver_at_ms"], second, "sent row keeps its due time");
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.rescheduled", &outbox_id), 2);
}

// T6: restart continuity — an undue row survives a
// restart still pending (due time intact) and delivers
// when due; a sent row is never re-delivered after a
// second restart.
#[tokio::test]
async fn t6_restart_keeps_undue_pending_and_sent_sent() {
    let mut h = Harness::serve(vec![done()]).await;
    let (_, execution_id) = h.terminal_execution().await;
    let row = h.outbox_for(&execution_id).await;
    let outbox_id = row["outbox_id"].as_str().unwrap().to_string();

    let future = sqlite_store::now_ms() as i64 + 60_000;
    schedule(&h, &outbox_id, future).await;

    h.restart().await;
    let row = h.outbox_for(&execution_id).await;
    assert_eq!(row["status"], "pending");
    assert_eq!(row["deliver_at_ms"], future, "due time survives restart");

    set_due(&h, &outbox_id, sqlite_store::now_ms() as i64 - 1_000);
    let (_, via_scan) = tick(&h);
    assert_eq!(via_scan.len(), 1);
    assert_eq!(via_scan[0].status, "sent");

    h.restart().await;
    let (_, via_scan) = tick(&h);
    assert_eq!(via_scan.len(), 0, "no re-delivery after restart");
    let row = h.outbox_for(&execution_id).await;
    assert_eq!(row["status"], "sent");
    assert_eq!(row["attempts"], 1);
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.sent", &outbox_id), 1);
}

// T7: TEXT storage regression — a hand-built outbox
// table with a TEXT `deliver_at_ms` column (the legacy
// shape the generic backfiller used to create) must
// order due rows numerically, not lexicographically:
// '999999999999' (12 digits) is due before
// '1794611497000' (13 digits) even though it sorts after
// it as TEXT. A TEXT due time in the future stays out.
#[test]
fn t7_legacy_text_due_times_order_numerically() {
    let conn = rusqlite::Connection::open_in_memory().expect("open sqlite");
    conn.execute_batch(
        "CREATE TABLE outbox (
            seq INTEGER NOT NULL,
            outbox_id TEXT PRIMARY KEY,
            execution_id TEXT NOT NULL,
            payload_summary_json TEXT NOT NULL,
            destination_kind TEXT NOT NULL,
            status TEXT NOT NULL,
            attempts INTEGER NOT NULL,
            dedup_key TEXT NOT NULL UNIQUE,
            created_ms INTEGER NOT NULL,
            updated_at_ms INTEGER,
            deliver_at_ms TEXT
        );",
    )
    .expect("create legacy outbox");
    // Numerically smallest due time first, created so
    // lexicographic order disagrees with numeric order.
    for (seq, outbox_id, due) in [
        (1_i64, "outbox-9", "999999999999"),
        (2_i64, "outbox-17", "1794611497000"),
    ] {
        conn.execute(
            "INSERT INTO outbox (seq, outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms, deliver_at_ms) VALUES (?1, ?2, 'execution-1', '{}', 'local_notification', 'pending', 0, ?3, 1794611497606, 1794611497606, ?4)",
            rusqlite::params![seq, outbox_id, format!("{outbox_id}:local_notification"), due],
        )
        .expect("insert legacy row");
    }
    // A TEXT due time in the future must stay out.
    conn.execute(
        "INSERT INTO outbox (seq, outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms, deliver_at_ms) VALUES (3, 'outbox-future', 'execution-1', '{}', 'local_notification', 'pending', 0, 'outbox-future:local_notification', 1794611497606, 1794611497606, '1794611499000')",
        [],
    )
    .expect("insert future row");
    // Sanity: the values really are stored as TEXT.
    let stored: String = conn
        .query_row(
            "SELECT typeof(deliver_at_ms) FROM outbox WHERE outbox_id = 'outbox-9'",
            [],
            |row| row.get(0),
        )
        .expect("typeof");
    assert_eq!(stored, "text");

    let due = sqlite_store::due_outbox_rows(&conn, 1_794_611_498_000).expect("due");
    let ids: Vec<&str> = due.iter().map(|row| row.outbox_id.as_str()).collect();
    assert_eq!(ids, vec!["outbox-9", "outbox-17"]);
    assert_eq!(
        due.iter().map(|row| row.deliver_at_ms).collect::<Vec<_>>(),
        vec![Some(999_999_999_999), Some(1_794_611_497_000)]
    );
}
