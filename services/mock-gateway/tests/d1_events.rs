//! D1-3 tests (plan §4, E1–E5): event-log consumption
//! (cursor + Outbox deliverer), the Outbox producer /
//! manual deliver / requeue routes, the two Inbox dedupe
//! guards, and the fold extension (policy_versions,
//! mandates, event_cursors).
//!
//! HTTP routes only, plus `sqlite_store` reads and the
//! pub test seams (`upsert_effect`, `insert_assignment`,
//! `sweep_expired_assignment`, `consume_outbox_events`,
//! `deliver_outbox`) where HTTP cannot reach the state
//! (a failed Effect cannot be re-upserted through HTTP —
//! failed→failed is an illegal transition — and the 10 s
//! sweeper tick is too slow to wait for). Loops run with
//! auto-drive off, so a started loop's Execution stays
//! `running` until the test transitions it — no race with
//! the driver completing the loop on its own.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::effect_store::EffectFileRecord;
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use mock_gateway::sqlite_store::{AssignmentRow, OutboxRow};
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-d1-{tag}-{}-{}",
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
            api_key: Some("fake-d1-key".to_string()),
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

    async fn patch(&self, path: &str, value: Value) -> (u16, Value) {
        let response = self
            .client
            .patch(format!("{}{}", self.url, path))
            .json(&value)
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

    async fn ok_patch(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.patch(path, value).await;
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
            .ok("/agent/loop/start", json!({"prompt": "d1-3"}))
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

/// Drive the tick consumer directly (the Harness serves
/// with `axum::serve`, so the 10 s sweeper does not run
/// in tests; the production tick calls this same function
/// through `AppState::deliver_pending_outbox`).
fn consume(h: &Harness) -> Vec<OutboxRow> {
    let mut conn = h.db();
    sqlite_store::consume_outbox_events(&mut conn, "outbox-deliverer", "test").expect("consume")
}

fn count_open_inbox(conn: &rusqlite::Connection, kind: &str, col: &str, value: &str) -> i64 {
    let sql = format!(
        "SELECT COUNT(*) FROM inbox WHERE kind = ?1 AND {col} = ?2 AND resolved_ms IS NULL"
    );
    conn.query_row(&sql, rusqlite::params![kind, value], |row| row.get(0))
        .expect("count inbox")
}

fn count_events(conn: &rusqlite::Connection, event_type: &str, aggregate_id: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM events WHERE event_type = ?1 AND aggregate_id = ?2",
        rusqlite::params![event_type, aggregate_id],
        |row| row.get(0),
    )
    .expect("count events")
}

// E1: cursor + side effect in one transaction, restart
// safety. With the same-tx design the
// "delivered-but-cursor-not-advanced" window does not
// exist, so the test locks the observable invariant:
// after one consumer run the row is sent and the cursor
// is at the max event id read (the `outbox.sent` event
// the consumer appended in the same transaction lands one
// id higher and is only "seen" on the next run); after a
// restart and a second run nothing is re-delivered, the
// cursor has advanced past the sent event and does not
// move again, and there is exactly one `outbox.sent` event.
#[tokio::test]
async fn e1_consumer_cursor_same_tx_and_restart() {
    let mut h = Harness::serve(vec![done()]).await;
    let (_, execution_id) = h.terminal_execution().await;
    let row = h.outbox_for(&execution_id).await;
    let outbox_id = row["outbox_id"].as_str().unwrap().to_string();
    assert_eq!(row["status"], "pending");
    // The high-water mark before the consumer runs: the
    // `outbox.enqueued` event is the log's last event.
    let high_water_before = {
        let conn = h.db();
        sqlite_store::list_events(&conn)
            .expect("events")
            .last()
            .map(|e| e.id)
            .unwrap_or(0)
    };

    let delivered = consume(&h);
    assert_eq!(delivered.len(), 1, "one row delivered");
    assert_eq!(delivered[0].outbox_id, outbox_id);
    assert_eq!(delivered[0].status, "sent");

    let conn = h.db();
    // The cursor advances to the max event id READ (the
    // enqueued event). The `outbox.sent` event the consumer
    // appended in the same transaction lands one id higher
    // and is only "seen" on the next run — the cursor
    // trailing the log by that one self-written event is
    // correct, not lag: a crash before commit would have
    // replayed from the old cursor.
    assert_eq!(
        sqlite_store::get_event_cursor(&conn, "outbox-deliverer").expect("cursor"),
        high_water_before,
        "cursor at the max event id read"
    );
    assert_eq!(count_events(&conn, "outbox.sent", &outbox_id), 1);

    h.restart().await;
    let delivered_again = consume(&h);
    assert_eq!(delivered_again.len(), 0, "no re-delivery after restart");
    let conn = h.db();
    let cursor_after = sqlite_store::get_event_cursor(&conn, "outbox-deliverer").expect("cursor");
    assert_eq!(
        cursor_after,
        sqlite_store::list_events(&conn)
            .expect("events")
            .last()
            .map(|e| e.id)
            .unwrap_or(0),
        "second run advanced the cursor past the sent event it read"
    );
    assert_eq!(count_events(&conn, "outbox.sent", &outbox_id), 1);
    let row_after = h.outbox_for(&execution_id).await;
    assert_eq!(row_after["status"], "sent");
    assert_eq!(row_after["attempts"], 1, "attempts unchanged");
    assert!(
        sqlite_store::fold_matches_tables(&conn).expect("fold"),
        "fold holds after consume + restart"
    );
}

// E2: Outbox end-to-end. Terminal → exactly 1 pending
// row → manual deliver → sent → repeat deliver does not
// re-deliver → failed row manual requeue → re-deliverable
// (both by manual deliver and by the tick consumer on the
// `outbox.requeued` event).
#[tokio::test]
async fn e2_outbox_end_to_end() {
    let h = Harness::serve(vec![done(), done(), done()]).await;

    // Leg 1: manual deliver + repeat deliver is a no-op.
    let (_, exec_a) = h.terminal_execution().await;
    let row_a = h.outbox_for(&exec_a).await;
    let id_a = row_a["outbox_id"].as_str().unwrap().to_string();
    assert_eq!(row_a["status"], "pending");
    assert_eq!(row_a["destination_kind"], "local_notification");
    let delivered = h.ok(&format!("/outbox/{id_a}/deliver"), json!({})).await;
    assert_eq!(delivered["status"], "sent");
    assert_eq!(delivered["attempts"], 1);
    let again = h.ok(&format!("/outbox/{id_a}/deliver"), json!({})).await;
    assert_eq!(again["status"], "sent");
    assert_eq!(again["attempts"], 1, "repeat deliver does not re-deliver");
    let conn = h.db();
    assert_eq!(count_events(&conn, "outbox.sent", &id_a), 1);

    // Leg 2: failed row → manual requeue → manual re-deliver.
    // The log sink never fails, so the failed state is
    // forced at the store level (the only way to reach it
    // until a real sink exists).
    let (_, exec_b) = h.terminal_execution().await;
    let row_b = h.outbox_for(&exec_b).await;
    let id_b = row_b["outbox_id"].as_str().unwrap().to_string();
    {
        let mut conn = h.db();
        let failed = sqlite_store::deliver_outbox(&mut conn, &id_b, false)
            .expect("fail row")
            .expect("row exists");
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.attempts, 1);
    }
    let requeued = h.ok(&format!("/outbox/{id_b}/requeue"), json!({})).await;
    assert_eq!(requeued["status"], "pending");
    assert_eq!(requeued["attempts"], 1, "requeue keeps the attempt count");
    let redelivered = h.ok(&format!("/outbox/{id_b}/deliver"), json!({})).await;
    assert_eq!(redelivered["status"], "sent");
    assert_eq!(redelivered["attempts"], 2);

    // Leg 3: a requeued row is also delivered by the tick
    // consumer (the `outbox.requeued` event is a trigger).
    let (_, exec_c) = h.terminal_execution().await;
    let row_c = h.outbox_for(&exec_c).await;
    let id_c = row_c["outbox_id"].as_str().unwrap().to_string();
    {
        let mut conn = h.db();
        sqlite_store::deliver_outbox(&mut conn, &id_c, false).expect("fail row");
    }
    h.ok(&format!("/outbox/{id_c}/requeue"), json!({})).await;
    let delivered = consume(&h);
    assert_eq!(delivered.len(), 1, "consumer delivers the requeued row");
    assert_eq!(delivered[0].outbox_id, id_c);
    let row_c_after = h.outbox_for(&exec_c).await;
    assert_eq!(row_c_after["status"], "sent");
    assert_eq!(row_c_after["attempts"], 2);

    // Unknown ids are 404 on both new routes.
    let (status, _) = h.post("/outbox/outbox-999/deliver", json!({})).await;
    assert_eq!(status, 404);
    let (status, _) = h.post("/outbox/outbox-999/requeue", json!({})).await;
    assert_eq!(status, 404);
}

// E3: the failed-branch dedupe guard. A repeated upsert
// of the same still-failed Effect opens exactly one open
// `failed` Inbox row; the P3-C §5.4 exception is
// unchanged — (`test`, `run`) Effects never open one.
#[tokio::test]
async fn e3_failed_branch_dedupe() {
    let h = Harness::serve(vec![done()]).await;
    let started = h.ok("/agent/loop/start", json!({"prompt": "d1-3"})).await;
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let exec = h.execution_for_loop(&loop_id).await;
    let execution_id = exec["execution_id"].as_str().unwrap().to_string();

    let failed = EffectFileRecord {
        effect_id: "effect-1".to_string(),
        execution_id: Some(execution_id.clone()),
        capability: "fs".to_string(),
        operation: "write".to_string(),
        scope: "x".to_string(),
        input_digest: Some("digest-1".to_string()),
        approval_id: None,
        status: "failed".to_string(),
        idempotency_key: None,
        origin_device_id: "test".to_string(),
        schema_version: 1,
        reconciliation: None,
        detail: None,
    };
    {
        let mut conn = h.db();
        sqlite_store::upsert_effect(&mut conn, &failed).expect("upsert 1");
        sqlite_store::upsert_effect(&mut conn, &failed).expect("upsert 2");
    }
    let conn = h.db();
    assert_eq!(
        count_open_inbox(&conn, "failed", "effect_id", "effect-1"),
        1,
        "one open failed row after two upserts"
    );

    let test_failed = EffectFileRecord {
        effect_id: "effect-2".to_string(),
        capability: "test".to_string(),
        operation: "run".to_string(),
        ..failed.clone()
    };
    {
        let mut conn = h.db();
        sqlite_store::upsert_effect(&mut conn, &test_failed).expect("upsert test 1");
        sqlite_store::upsert_effect(&mut conn, &test_failed).expect("upsert test 2");
    }
    let conn = h.db();
    assert_eq!(
        count_open_inbox(&conn, "failed", "effect_id", "effect-2"),
        0,
        "(test, run) still never opens a failed row"
    );
}

// E4: the lease-sweep dedupe guard. Two lost assignments
// for the same still-unreviewed Execution open exactly
// one open `unknown_requires_reconciliation` row: the
// second sweep transitions the Execution back to
// `needs_review` but the guard blocks the second row.
#[tokio::test]
async fn e4_sweep_path_dedupe() {
    let h = Harness::serve(vec![done()]).await;
    let started = h.ok("/agent/loop/start", json!({"prompt": "d1-3"})).await;
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let exec = h.execution_for_loop(&loop_id).await;
    let execution_id = exec["execution_id"].as_str().unwrap().to_string();
    assert_eq!(exec["status"], "running");

    let assignment = |id: &str| AssignmentRow {
        assignment_id: id.to_string(),
        execution_id: Some(execution_id.clone()),
        worker_session_id: Some(format!("worker-session-{id}")),
        origin_device_id: "test".to_string(),
        status: "active".to_string(),
        detail: None,
        lease_expires_ms: Some(1),
        heartbeat_ms: Some(1),
        schema_version: 1,
    };
    {
        let mut conn = h.db();
        sqlite_store::insert_assignment(&mut conn, &assignment("assignment-1"))
            .expect("insert assignment-1");
        sqlite_store::insert_assignment(&mut conn, &assignment("assignment-2"))
            .expect("insert assignment-2");
    }
    let outcome = {
        let mut conn = h.db();
        sqlite_store::sweep_expired_assignment(&mut conn, "assignment-1").expect("sweep 1")
    };
    assert!(outcome.assignment_lost && outcome.execution_transitioned);
    let conn = h.db();
    assert_eq!(
        count_open_inbox(&conn, "unknown_requires_reconciliation", "execution_id", &execution_id),
        1,
        "one open unknown row after the first sweep"
    );
    // Back to running so the second sweep can transition
    // the Execution to needs_review again.
    let moved = h
        .ok(
            "/executions/transition",
            json!({ "execution_id": execution_id, "status": "running" }),
        )
        .await;
    assert_eq!(moved["status"], "running");
    let outcome = {
        let mut conn = h.db();
        sqlite_store::sweep_expired_assignment(&mut conn, "assignment-2").expect("sweep 2")
    };
    assert!(outcome.assignment_lost && outcome.execution_transitioned);
    let conn = h.db();
    assert_eq!(
        count_open_inbox(&conn, "unknown_requires_reconciliation", "execution_id", &execution_id),
        1,
        "still one open unknown row after the second sweep"
    );
    assert!(
        sqlite_store::fold_matches_tables(&conn).expect("fold"),
        "fold holds after the sweeps"
    );
}

// E5: fold extension. Policy writes, Mandate create /
// supersede, and the Outbox producer + consumer all land
// on the event log with post-state row payloads, so the
// fold reproduces the tables exactly; the cursor row
// itself is covered by the reference invariant (its
// last_event_id is an existing event id). The new event
// payloads carry no secret-shaped strings (plan §11 red
// line 7, test lock — the `redact_event_payload` helper
// itself is not built in this batch; see the deviation
// note in the implementation report).
#[tokio::test]
async fn e5_fold_covers_policy_mandates_cursors() {
    let h = Harness::serve(vec![done()]).await;
    let goal1 = h.ok("/goals/create", json!({"title": "Ship D1"})).await;
    assert_eq!(goal1["goal_id"], "goal-1");
    let goal2 = h.ok("/goals/create", json!({"title": "Second"})).await;
    assert_eq!(goal2["goal_id"], "goal-2");

    h.ok_patch("/permissions/hitl", json!({"enabled": true})).await;
    let versions = h.ok_get("/policies/versions").await;
    assert!(
        !versions.as_array().unwrap().is_empty(),
        "policy version recorded: {versions}"
    );

    // The task directory must exist under files_root
    // (same setup as d1_policy P2).
    std::fs::create_dir_all(h.dir.join("files").join("proj")).unwrap();
    let task = h
        .ok("/tasks/create", json!({"directory": "proj", "goal_id": "goal-1"}))
        .await;
    let task_id = task["task_id"].as_str().unwrap().to_string();
    let mandates = h.ok_get(&format!("/mandates?task_id={task_id}")).await;
    assert_eq!(mandates.as_array().unwrap().len(), 1, "{mandates}");

    // Goal change supersedes the task-level Mandate and
    // creates a new one bound to the current version.
    h.ok(
        "/tasks/update",
        json!({"task_id": task_id, "status": "created", "goal_id": "goal-2"}),
    )
    .await;
    let mandates = h.ok_get(&format!("/mandates?task_id={task_id}")).await;
    assert_eq!(mandates.as_array().unwrap().len(), 2, "{mandates}");
    assert!(
        mandates
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["status"] == "superseded"),
        "old mandate superseded: {mandates}"
    );

    let (_, execution_id) = h.terminal_execution().await;
    let row = h.outbox_for(&execution_id).await;
    assert_eq!(row["status"], "pending");
    assert_eq!(consume(&h).len(), 1, "consumer delivers the row");

    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    assert!(
        events.iter().any(|e| e.event_type == "policy_version.created"),
        "policy_version.created on the log"
    );
    assert!(
        events.iter().any(|e| e.event_type == "mandate.created"),
        "mandate.created on the log"
    );
    assert!(
        events.iter().any(|e| e.event_type == "mandate.superseded"),
        "mandate.superseded on the log"
    );
    for event in events.iter().filter(|e| {
        e.aggregate_kind == "policy_version" || e.aggregate_kind == "mandate"
    }) {
        let lower = event.payload_json.to_lowercase();
        for secret_shape in ["secret", "api_key", "password", "token"] {
            assert!(
                !lower.contains(secret_shape),
                "no secret-shaped string in {} payload: {}",
                event.event_type,
                event.payload_json
            );
        }
    }
    let cursors = sqlite_store::list_event_cursors(&conn).expect("cursors");
    assert_eq!(cursors.len(), 1, "{cursors:?}");
    assert_eq!(cursors[0].consumer_name, "outbox-deliverer");
    assert!(
        events.iter().any(|e| e.id == cursors[0].last_event_id),
        "cursor points at an existing event"
    );
    assert!(
        sqlite_store::fold_matches_tables(&conn).expect("fold"),
        "fold reproduces policy_versions, mandates, and the cursor invariant"
    );
}
