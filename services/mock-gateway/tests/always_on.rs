//! AO-1 tests (plan §3, Q-AO-04/05/06/17): the event
//! inbox — ingest + dedupe (A1), claim atomicity
//! (A2), the single-session gate (A3), and failure
//! handling (A4).
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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use reqwest::StatusCode;
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

fn build_state(dir: &Path, deepseek_base_url: &str) -> AppState {
    let state = AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_tasks_path(dir.join("tasks.jsonl"))
    .with_task_drafts_dir(dir.join("drafts"))
    .with_deepseek(DeepSeekClient::new(DeepSeekConfig {
        base_url: deepseek_base_url.to_string(),
        model: "fixture".to_string(),
        api_key: Some("test-key".to_string()),
    }))
    .with_auto_drive(false)
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
    #[allow(dead_code)]
    server: Option<tokio::task::JoinHandle<()>>,
    #[allow(dead_code)]
    state: AppState,
}

impl Harness {
    async fn serve() -> Self {
        let dir = temp_dir("ao1");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let deepseek_base_url = start_fake_deepseek().await;
        let state = build_state(&dir, &deepseek_base_url);
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
}

#[tokio::test]
async fn schema_version_is_22() {
    assert_eq!(DB_SCHEMA_VERSION, 22, "AO-1 bumps the schema to v22");
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), 22);
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
