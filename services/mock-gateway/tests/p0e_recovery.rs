//! P0-E tests: local recovery and reassignment acceptance
//! (pa-p0e-plan-draft.md §5, matrix T1–T14).
//!
//! Gateway kernel only: no client, no UI, no driver rewrites.
//! Trigger means, per the plan: a real gateway binary for the
//! stop/restart cases (SIGINT for clean shutdown, SIGKILL for
//! crash), HTTP routes for takeover / transition / approval
//! consumption, and sqlite_store seams only to READ the
//! database after the fact (never to fake a production step).
//! "New worker" always means a WorkerSession created by a
//! restart's recovery scan or by a takeover, never a forged
//! second process.
//!
//! Matrix map:
//! - T1 clean shutdown: binary test below (SIGINT).
//! - T2 kill -9 + restart: binary test below (SIGKILL) plus
//!   an in-process restart scan test (same assertions, faster).
//! - T3 restart equivalence: reused from P0-D (p0d_sqlite
//!   import/switch/restart test, still green in T14).
//! - T4 same-worker double consume: reused from P0-C
//!   (fs_digest_tamper_replay_and_unknown_block, still green).
//! - T5 cross-worker double consume + T8 cross-context
//!   consume: p0e_cross_worker_consume_and_replay.
//! - T6 unknown核查串联: p0e_unknown_takeover_then_reconciled
//!   (takeover inserted between unknown and reconcile; the
//!   device-readback reconcile mechanics themselves are P0-C's
//!   and reused, not rewritten).
//! - T7 reassignment: p0e_takeover_flow.
//! - T9 terminal replay: p0e_terminal_effect_replays_after_takeover.
//! - T10 unknown blocked: p0e_unknown_takeover_then_reconciled.
//! - T11 Inbox贯穿: p0e_inbox_flow_with_review_transition.
//! - T12 snapshot immutability + live gate:
//!   p0e_snapshot_immutable_and_gate_live.
//! - T13 checkpoint recovery: p0e_checkpoint_recovery
//!   (plus the multi-effect payload unit test in sqlite_store).
//! - T14 full regression + clippy: the whole suite + the
//!   clippy run (see the final report, not a test here).
//! - Gate regression (plan §2.4/§3.4): p0e_gate_reads_no_origin.

use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p0e-{}-{}-{}",
        tag,
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Harness {
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    state: AppState,
}

impl Harness {
    async fn serve(state: AppState, dir: PathBuf) -> Self {
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        let kept = state.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server =
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
            state: kept,
        }
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

    async fn ok_post(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn ok_get(&self, path: &str) -> Value {
        let (status, body) = self.get(path).await;
        assert_eq!(status, 200, "{body}");
        body
    }
}

fn base_state(dir: &std::path::Path) -> AppState {
    AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_executions_path(dir.join("executions.jsonl"))
    .with_effects_path(dir.join("effects.jsonl"))
}

/// Fresh SQLite-mode state: shadow open + import/switch, which
/// runs the first recovery scan, so the current worker is
/// worker-1 before any test step runs.
fn sqlite_state(dir: &std::path::Path) -> AppState {
    let state = base_state(dir).with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    assert_eq!(state.sqlite_backend_name(), "sqlite");
    assert_eq!(state.current_worker_session().as_deref(), Some("worker-1"));
    state
}

/// "Restart": a new AppState on the same database. import_done
/// is set, so with_sqlite_path goes straight to SQLite mode
/// and runs the recovery scan (ending the previous workers).
fn restarted_state(dir: &std::path::Path) -> AppState {
    let state = base_state(dir).with_sqlite_path(dir.join("gateway.sqlite3"));
    assert_eq!(state.sqlite_backend_name(), "sqlite");
    state
}

fn db(dir: &std::path::Path) -> rusqlite::Connection {
    sqlite_store::open_db(&dir.join("gateway.sqlite3")).expect("open db")
}

fn approval_request_body(path: &str, content: &str) -> Value {
    // "hello" in base64; the content is fixed so digests and
    // replay assertions are deterministic.
    let _ = content;
    json!({
        "tool": "fs.write",
        "path": path,
        "content_base64": "aGVsbG8=",
        "message": "please write",
    })
}

async fn request_approval(h: &Harness, path: &str) -> (String, String, String) {
    let body = h.ok_post("/approval/request", approval_request_body(path, "hello")).await;
    let approval_id = body["tool_call_id"].as_str().unwrap().to_string();
    let effects = h.ok_get("/effects/list").await;
    let effect = effects
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["approval_id"] == approval_id)
        .expect("planned effect for approval");
    (
        approval_id,
        effect["effect_id"].as_str().unwrap().to_string(),
        effect["execution_id"].as_str().unwrap().to_string(),
    )
}

async fn effect_status(h: &Harness, effect_id: &str) -> String {
    let effect = h.ok_get(&format!("/effects/get?effect_id={effect_id}")).await;
    effect["status"].as_str().unwrap().to_string()
}

async fn transition_effect(h: &Harness, effect_id: &str, status: &str) {
    let mut req = json!({ "effect_id": effect_id, "status": status });
    if status == "reconciled" {
        req["reconciliation_result"] = json!("completed");
    }
    h.ok_post("/effects/transition", req).await;
}

async fn execution_view(h: &Harness, execution_id: &str) -> Value {
    h.ok_get(&format!("/executions/get?execution_id={execution_id}")).await
}

async fn open_inbox(h: &Harness, kind: &str) -> Vec<Value> {
    let rows = h
        .ok_get(&format!("/inbox/list?kind={kind}&open_only=true"))
        .await;
    rows.as_array().unwrap().clone()
}

// --- T2 (in-process half): restart scan ---

#[tokio::test]
async fn p0e_restart_scan_ends_worker_and_keeps_approval_and_effect() {
    let dir = temp_dir("scan");
    let h = Harness::serve(sqlite_state(&dir), dir.clone()).await;
    let (approval, effect_id, execution_id) = request_approval(&h, "scan.txt").await;
    h.ok_post(
        "/approval/decide",
        json!({ "tool_call_id": approval, "status": "approved" }),
    )
    .await;
    // Effect authorized but unconcluded at "crash" time.
    transition_effect(&h, &effect_id, "authorized").await;
    let view_before = execution_view(&h, &execution_id).await;
    assert_eq!(view_before["worker_session_id"], "worker-1");
    h.server.abort();
    drop(h);

    let h2 = Harness::serve(restarted_state(&dir), dir.clone()).await;
    assert_eq!(h2.state.current_worker_session().as_deref(), Some("worker-2"));
    // Scan assertions on the database itself.
    let conn = db(&dir);
    let workers = sqlite_store::list_worker_sessions(&conn).expect("workers");
    let w1 = workers.iter().find(|w| w.worker_session_id == "worker-1").unwrap();
    assert_eq!(w1.status, "ended");
    assert_eq!(w1.detail.as_deref(), Some("process ended"));
    let w2 = workers.iter().find(|w| w.worker_session_id == "worker-2").unwrap();
    assert_eq!(w2.status, "active");
    // Committed approval and effect survive; nothing is lost.
    let approvals = sqlite_store::list_approvals(&conn).expect("approvals");
    let kept = approvals.iter().find(|a| a.tool_call_id == approval).unwrap();
    assert_eq!(kept.status, "approved");
    assert!(!kept.consumed);
    assert_eq!(effect_status(&h2, &effect_id).await, "authorized");
    // The execution is recoverable: non-terminal, no driver.
    let view = execution_view(&h2, &execution_id).await;
    assert_eq!(view["status"], "running");
    assert_eq!(view["recoverable"], true);
    assert_eq!(view["active_assignment_id"], Value::Null);
    // The execution row itself is untouched by the scan (its
    // worker still names the ended worker until a takeover).
    assert_eq!(view["worker_session_id"], "worker-1");
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
    assert_eq!(sqlite_store::integrity_check(&conn).expect("integrity"), "ok");
    h2.server.abort();
}

// --- T7: takeover flow ---

#[tokio::test]
async fn p0e_takeover_flow() {
    let dir = temp_dir("takeover");
    let h = Harness::serve(sqlite_state(&dir), dir.clone()).await;
    let (_approval, effect_id, execution_id) = request_approval(&h, "take.txt").await;
    h.server.abort();
    drop(h);
    // Restart so worker-1 is ended and worker-2 is current.
    let h = Harness::serve(restarted_state(&dir), dir.clone()).await;
    assert_eq!(h.state.current_worker_session().as_deref(), Some("worker-2"));
    // Ended worker named explicitly: 409, nothing written.
    let (status, body) = h
        .post(
            "/assignments/takeover",
            json!({ "execution_id": execution_id, "worker_session_id": "worker-1" }),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "worker_session_not_active");
    // Unknown execution: 409 with its own code.
    let (status, body) = h
        .post("/assignments/takeover", json!({ "execution_id": "execution-99" }))
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "execution_not_found");
    // Takeover: new worker, new active assignment, execution
    // re-pointed, checkpoint snapshotting the Effect list.
    let (status, body) = h
        .post("/assignments/takeover", json!({ "execution_id": execution_id }))
        .await;
    assert_eq!(status, 200, "{body}");
    let assignment_id = body["assignment"]["assignment_id"].as_str().unwrap().to_string();
    assert_eq!(body["assignment"]["status"], "active");
    assert_eq!(body["assignment"]["worker_session_id"], "worker-3");
    assert_eq!(body["execution"]["worker_session_id"], "worker-3");
    // Takeover makes the new worker the process's current one,
    // so later executions stamp it (see the judgement call in
    // the takeover route).
    assert_eq!(h.state.current_worker_session().as_deref(), Some("worker-3"));
    let (_a2, _e2, execution2) = request_approval(&h, "later.txt").await;
    assert_eq!(
        execution_view(&h, &execution2).await["worker_session_id"],
        "worker-3"
    );
    // The checkpoint: the planned effect is next, nothing completed.
    let conn = db(&dir);
    let checkpoint = sqlite_store::latest_plan_position(&conn, &execution_id)
        .expect("checkpoint")
        .expect("plan_position written at takeover");
    assert_eq!(checkpoint.kind, "execution.plan_position");
    let payload: Value =
        serde_json::from_str(&checkpoint.payload_json).expect("checkpoint json");
    assert_eq!(payload["completed_keys"], json!([]));
    let effects = h.ok_get("/effects/list").await;
    let planned = effects
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["effect_id"] == effect_id)
        .unwrap();
    assert_eq!(payload["next_effect_key"], planned["idempotency_key"]);
    // Events: reassigned + created are on the log.
    let events = sqlite_store::list_events(&conn).expect("events");
    assert!(events.iter().any(|e| e.event_type == "execution.reassigned"
        && e.aggregate_id == execution_id));
    assert!(events.iter().any(|e| e.event_type == "assignment.created"
        && e.aggregate_id == assignment_id));
    // Read routes serve the assignment.
    let got = h
        .ok_get(&format!("/assignments/get?assignment_id={assignment_id}"))
        .await;
    assert_eq!(got["status"], "active");
    let (status, _) = h.get("/assignments/get?assignment_id=assignment-99").await;
    assert_eq!(status, 404);
    // Second takeover: a driver already exists -> 409, and the
    // refused attempt wrote no new worker row (rollback).
    let workers_before =
        sqlite_store::list_worker_sessions(&db(&dir)).expect("workers").len();
    let (status, body) = h
        .post("/assignments/takeover", json!({ "execution_id": execution_id }))
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "assignment_active");
    assert_eq!(
        sqlite_store::list_worker_sessions(&db(&dir)).expect("workers").len(),
        workers_before
    );
    assert_eq!(execution_view(&h, &execution_id).await["recoverable"], false);
    assert_eq!(
        execution_view(&h, &execution_id).await["active_assignment_id"],
        assignment_id
    );
    // Terminal landing ends the assignment as done (not lost),
    // and a terminal execution refuses takeover outright.
    let moved = h
        .ok_post(
            "/executions/transition",
            json!({ "execution_id": execution_id, "status": "completed" }),
        )
        .await;
    assert_eq!(moved["status"], "completed");
    let got = h
        .ok_get(&format!("/assignments/get?assignment_id={assignment_id}"))
        .await;
    assert_eq!(got["status"], "done");
    let (status, body) = h
        .post("/assignments/takeover", json!({ "execution_id": execution_id }))
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "execution_terminal");
    // Transition vocabulary: unknown word 400, unknown id 404,
    // illegal edge (terminal -> running) 409, state untouched.
    let (status, _) = h
        .post(
            "/executions/transition",
            json!({ "execution_id": execution_id, "status": "bogus" }),
        )
        .await;
    assert_eq!(status, 400);
    let (status, _) = h
        .post(
            "/executions/transition",
            json!({ "execution_id": "execution-99", "status": "running" }),
        )
        .await;
    assert_eq!(status, 404);
    let (status, body) = h
        .post(
            "/executions/transition",
            json!({ "execution_id": execution_id, "status": "running" }),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(execution_view(&h, &execution_id).await["status"], "completed");
    assert!(sqlite_store::fold_matches_tables(&db(&dir)).expect("fold"));
    h.server.abort();
}

// --- T5 + T8: cross-worker consume, then re-presentation ---

#[tokio::test]
async fn p0e_cross_worker_consume_and_replay() {
    let dir = temp_dir("crossworker");
    let h = Harness::serve(sqlite_state(&dir), dir.clone()).await;
    let (approval, effect_id, execution_id) = request_approval(&h, "cross.txt").await;
    // Granted before the stop, never consumed.
    h.ok_post(
        "/approval/decide",
        json!({ "tool_call_id": approval, "status": "approved" }),
    )
    .await;
    let session_before = execution_view(&h, &execution_id).await["session_id"].clone();
    h.server.abort();
    drop(h);
    // Restart (new worker) + takeover: the new worker's
    // execution presents the old approval.
    let h = Harness::serve(restarted_state(&dir), dir.clone()).await;
    h.ok_post("/assignments/takeover", json!({ "execution_id": execution_id })).await;
    let (status, body) = h
        .post(
            "/files/write",
            json!({ "path": "cross.txt", "content_base64": "aGVsbG8=", "approval_id": approval }),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    // The write executes through to succeeded; the authorize
    // step itself is proven by the event chain below (the
    // consume moves planned -> authorized before the driver
    // runs the write to succeeded).
    assert_eq!(effect_status(&h, &effect_id).await, "succeeded");
    assert_eq!(
        std::fs::read_to_string(dir.join("files").join("cross.txt")).unwrap(),
        "hello"
    );
    // Conversation session untouched by the takeover.
    assert_eq!(
        execution_view(&h, &execution_id).await["session_id"],
        session_before
    );
    // Audit chain: approval.consumed + the effect's
    // authorized transition, both naming the same effect.
    let conn = db(&dir);
    let events = sqlite_store::list_events(&conn).expect("events");
    assert!(events.iter().any(|e| e.aggregate_kind == "approval"
        && e.event_type == "approval.consumed"
        && e.aggregate_id == approval));
    assert!(events.iter().any(|e| {
        e.aggregate_kind == "effect"
            && e.event_type == "effect.transitioned"
            && e.aggregate_id == effect_id
            && serde_json::from_str::<Value>(&e.payload_json)
                .map(|p| p["status"] == "authorized")
                .unwrap_or(false)
    }));
    let approvals = sqlite_store::list_approvals(&conn).expect("approvals");
    assert!(approvals.iter().find(|a| a.tool_call_id == approval).unwrap().consumed);
    // T5: the consumed approval presented for a NEW operation
    // is refused (403): consumed state survived the worker
    // change. (Same parameters would replay the terminal
    // effect instead — that is T9, asserted separately.)
    let (status, _) = h
        .post(
            "/files/write",
            json!({ "path": "other.txt", "content_base64": "aGVsbG8=", "approval_id": approval }),
        )
        .await;
    assert_eq!(status, 403);
    assert!(!dir.join("files").join("other.txt").exists());
    h.server.abort();
}

// --- T9: terminal effects replay after takeover ---

#[tokio::test]
async fn p0e_terminal_effect_replays_after_takeover() {
    let dir = temp_dir("replay");
    let h = Harness::serve(sqlite_state(&dir), dir.clone()).await;
    let (approval, effect_id, execution_id) = request_approval(&h, "done.txt").await;
    h.ok_post(
        "/approval/decide",
        json!({ "tool_call_id": approval, "status": "approved" }),
    )
    .await;
    let (status, _) = h
        .post(
            "/files/write",
            json!({ "path": "done.txt", "content_base64": "aGVsbG8=", "approval_id": approval }),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(effect_status(&h, &effect_id).await, "succeeded");
    h.server.abort();
    drop(h);
    let h = Harness::serve(restarted_state(&dir), dir.clone()).await;
    h.ok_post("/assignments/takeover", json!({ "execution_id": execution_id })).await;
    // Same parameters after takeover: the terminal effect is
    // returned, not re-executed (200 + replayed, content kept).
    let (status, body) = h
        .post(
            "/files/write",
            json!({ "path": "done.txt", "content_base64": "aGVsbG8=", "approval_id": approval }),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["replayed"], true);
    assert_eq!(body["effect_id"], effect_id);
    assert_eq!(
        std::fs::read_to_string(dir.join("files").join("done.txt")).unwrap(),
        "hello"
    );
    h.server.abort();
}

// --- T6 + T10: unknown blocked across takeover, then reconciled ---

#[tokio::test]
async fn p0e_unknown_takeover_then_reconciled() {
    let dir = temp_dir("unknown");
    let h = Harness::serve(sqlite_state(&dir), dir.clone()).await;
    let (approval, effect_id, execution_id) = request_approval(&h, "unk.txt").await;
    h.ok_post(
        "/approval/decide",
        json!({ "tool_call_id": approval, "status": "approved" }),
    )
    .await;
    for to in ["authorized", "started", "unknown"] {
        transition_effect(&h, &effect_id, to).await;
    }
    assert_eq!(open_inbox(&h, "unknown_requires_reconciliation").await.len(), 1);
    // Takeover with an unknown effect in the list: the
    // checkpoint names it as next (nothing completed yet).
    h.ok_post("/assignments/takeover", json!({ "execution_id": execution_id })).await;
    let conn = db(&dir);
    let checkpoint = sqlite_store::latest_plan_position(&conn, &execution_id)
        .expect("checkpoint")
        .expect("plan_position");
    let payload: Value =
        serde_json::from_str(&checkpoint.payload_json).expect("checkpoint json");
    let effects = h.ok_get("/effects/list").await;
    let unknown = effects
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["effect_id"] == effect_id)
        .unwrap();
    assert_eq!(payload["next_effect_key"], unknown["idempotency_key"]);
    // T10: before any reconciliation, re-execution refuses
    // (409) and writes nothing, without consuming the approval.
    let (status, _) = h
        .post(
            "/files/write",
            json!({ "path": "unk.txt", "content_base64": "aGVsbG8=", "approval_id": approval }),
        )
        .await;
    assert_eq!(status, 409);
    assert!(!dir.join("files").join("unk.txt").exists());
    let approvals = sqlite_store::list_approvals(&db(&dir)).expect("approvals");
    assert!(!approvals.iter().find(|a| a.tool_call_id == approval).unwrap().consumed);
    // T6: reconcile after the takeover (transition-route
    // conclusion; the device-readback mechanics are P0-C's and
    // covered by its own tests). The unknown inbox row closes.
    transition_effect(&h, &effect_id, "reconciling").await;
    transition_effect(&h, &effect_id, "reconciled").await;
    assert_eq!(effect_status(&h, &effect_id).await, "reconciled");
    assert!(open_inbox(&h, "unknown_requires_reconciliation").await.is_empty());
    // Reconciled is terminal: re-presenting replays, and a
    // reconciled-failed effect in the same execution would
    // still return its failure rather than re-executing
    // (P0-C rule, proven by the replay path here).
    let (status, body) = h
        .post(
            "/files/write",
            json!({ "path": "unk.txt", "content_base64": "aGVsbG8=", "approval_id": approval }),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["replayed"], true);
    h.server.abort();
}

// --- T11: Inbox贯穿, including needs_review via the transition route ---

#[tokio::test]
async fn p0e_inbox_flow_with_review_transition() {
    let dir = temp_dir("inbox");
    let h = Harness::serve(sqlite_state(&dir), dir.clone()).await;
    // approval_required opens on the bound pending approval
    // and closes automatically on decision.
    let (approval, effect_id, execution_id) = request_approval(&h, "inb.txt").await;
    let rows = open_inbox(&h, "approval_required").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["approval_id"], approval);
    h.ok_post(
        "/approval/decide",
        json!({ "tool_call_id": approval, "status": "approved" }),
    )
    .await;
    assert!(open_inbox(&h, "approval_required").await.is_empty());
    // review_required opens only via the transition route
    // (its only production path) and closes on leaving review.
    h.ok_post(
        "/executions/transition",
        json!({ "execution_id": execution_id, "status": "needs_review" }),
    )
    .await;
    let rows = open_inbox(&h, "review_required").await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["execution_id"], execution_id);
    h.ok_post(
        "/executions/transition",
        json!({ "execution_id": execution_id, "status": "running" }),
    )
    .await;
    assert!(open_inbox(&h, "review_required").await.is_empty());
    // failed rows stay open across takeover and close only by ack.
    for to in ["authorized", "started", "failed"] {
        transition_effect(&h, &effect_id, to).await;
    }
    let rows = open_inbox(&h, "failed").await;
    assert_eq!(rows.len(), 1);
    let failed_id = rows[0]["inbox_id"].as_str().unwrap().to_string();
    h.ok_post("/assignments/takeover", json!({ "execution_id": execution_id })).await;
    assert_eq!(open_inbox(&h, "failed").await.len(), 1);
    let acked = h.ok_post("/inbox/ack", json!({ "inbox_id": failed_id })).await;
    assert_eq!(acked["resolved_reason"], "acked");
    assert!(open_inbox(&h, "failed").await.is_empty());
    // Outbox unchanged by the whole flow: no production row.
    let outbox = h.ok_get("/outbox/list").await;
    assert!(outbox.as_array().unwrap().is_empty());
    h.server.abort();
}

// --- T12: snapshot immutability + live gate ---

#[tokio::test]
async fn p0e_snapshot_immutable_and_gate_live() {
    let dir = temp_dir("snapshot");
    let h = Harness::serve(sqlite_state(&dir), dir.clone()).await;
    let (_approval, _effect, execution_id) = request_approval(&h, "snap.txt").await;
    let view = execution_view(&h, &execution_id).await;
    let snapshot_id = view["workspace_snapshot_id"].as_str().unwrap().to_string();
    let conn = db(&dir);
    let before = sqlite_store::get_snapshot(&conn, &snapshot_id)
        .expect("snapshot")
        .expect("snapshot row");
    // Adapter execution: empty-template digest (the well-known
    // SHA-256 of ""), placeholder memory scope, gateway home
    // as the files_root approximation.
    assert_eq!(
        before.instruction_digest,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(before.memory_scope, "default");
    let home = dir.canonicalize().unwrap().to_string_lossy().into_owned();
    assert_eq!(before.files_root, home);
    assert_eq!(before.connector_ids_json, "[]");
    assert_eq!(before.device_ids_json, "[]");
    assert_eq!(
        before.content_sha256,
        sqlite_store::snapshot_content_sha256(&before)
    );
    // Gate before the edit: no deny rule for the host.
    let perms_before = h.ok_get("/permissions").await;
    let rules_before: Vec<mock_gateway::policy_store::NetworkRule> =
        serde_json::from_value(perms_before["network_rules"].clone()).unwrap();
    assert_eq!(
        mock_gateway::policy_store::denied_rule(&rules_before, "https://example.com/x"),
        None
    );
    // Policy edit (the "Workspace change" the plan names).
    let patched = h
        .client
        .patch(format!("{}/permissions/network-rules", h.url))
        .json(&json!({ "rules": [{ "pattern": "example.com", "action": "deny" }] }))
        .send()
        .await
        .unwrap();
    assert_eq!(patched.status().as_u16(), 200);
    // Old snapshot row and execution pointer: byte-identical.
    let conn = db(&dir);
    let after = sqlite_store::get_snapshot(&conn, &snapshot_id)
        .expect("snapshot")
        .expect("snapshot row");
    assert_eq!(before, after);
    assert_eq!(
        execution_view(&h, &execution_id).await["workspace_snapshot_id"],
        snapshot_id
    );
    // Gate after the edit: the same gate function, fed the
    // server's current rules, now denies the host. (A full
    // model call would need a key; the gate reads the shared
    // rules handle on every call, so this plus the new
    // snapshot's digest below is the live-read proof.)
    let perms_after = h.ok_get("/permissions").await;
    let rules_after: Vec<mock_gateway::policy_store::NetworkRule> =
        serde_json::from_value(perms_after["network_rules"].clone()).unwrap();
    assert_eq!(
        mock_gateway::policy_store::denied_rule(&rules_after, "https://example.com/x"),
        Some("example.com".to_string())
    );
    // A new execution reads live policy: only its network
    // digest moved; the permissions digest did not.
    let (_a2, _e2, execution2) = request_approval(&h, "snap2.txt").await;
    let snapshot2_id = execution_view(&h, &execution2)
        .await["workspace_snapshot_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(snapshot2_id, snapshot_id);
    let second = sqlite_store::get_snapshot(&db(&dir), &snapshot2_id)
        .expect("snapshot2")
        .expect("snapshot2 row");
    let bindings_before: Vec<String> =
        serde_json::from_str(&before.policy_binding_ids_json).unwrap();
    let bindings_after: Vec<String> =
        serde_json::from_str(&second.policy_binding_ids_json).unwrap();
    assert_ne!(bindings_before[0], bindings_after[0]);
    assert_eq!(bindings_before[1], bindings_after[1]);
    h.server.abort();
}

// --- T13: checkpoint recovery positions ---

#[tokio::test]
async fn p0e_checkpoint_recovery() {
    let dir = temp_dir("checkpoint");
    let h = Harness::serve(sqlite_state(&dir), dir.clone()).await;
    let (approval, effect_id, execution_id) = request_approval(&h, "ckpt.txt").await;
    h.ok_post(
        "/approval/decide",
        json!({ "tool_call_id": approval, "status": "approved" }),
    )
    .await;
    // Drive the effect to terminal before takeover, so the
    // checkpoint must list it as completed with no next key.
    let (status, _) = h
        .post(
            "/files/write",
            json!({ "path": "ckpt.txt", "content_base64": "aGVsbG8=", "approval_id": approval }),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(effect_status(&h, &effect_id).await, "succeeded");
    h.ok_post("/assignments/takeover", json!({ "execution_id": execution_id })).await;
    let conn = db(&dir);
    let checkpoint = sqlite_store::latest_plan_position(&conn, &execution_id)
        .expect("checkpoint")
        .expect("plan_position");
    let payload: Value =
        serde_json::from_str(&checkpoint.payload_json).expect("checkpoint json");
    let effects = h.ok_get("/effects/list").await;
    let done = effects
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["effect_id"] == effect_id)
        .unwrap();
    assert_eq!(payload["completed_keys"], json!([done["idempotency_key"]]));
    assert_eq!(payload["next_effect_key"], Value::Null);
    // Continuation agrees with the checkpoint: the completed
    // key replays, and the checkpoint row is append-only (one
    // row from the one takeover; a later takeover would append
    // a second row rather than rewriting this one).
    let (status, body) = h
        .post(
            "/files/write",
            json!({ "path": "ckpt.txt", "content_base64": "aGVsbG8=", "approval_id": approval }),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["replayed"], true);
    let checkpoints =
        sqlite_store::list_checkpoints_for_execution(&db(&dir), &execution_id)
            .expect("checkpoints");
    assert_eq!(checkpoints.len(), 1);
    h.server.abort();
}

// --- Gate regression: no origin read on the gate path ---

#[test]
fn p0e_gate_reads_no_origin() {
    // Grep-level regression (plan §2.4): the approval gates
    // never read origin_device_id, so cross-context consumption
    // cannot depend on which device or worker created the row.
    let candidates = ["src/server.rs", "services/mock-gateway/src/server.rs"];
    let path = candidates.iter().find(|p| std::path::Path::new(p).exists())
        .expect("server.rs next to the test package");
    let src = std::fs::read_to_string(path).expect("read server.rs");
    for name in [
        "check_approval",
        "consume_bound_approval_for_effect",
        "device_approval_error",
        "check_device_approval",
    ] {
        let needle = format!("fn {name}(");
        let start = src.find(&needle).unwrap_or_else(|| {
            panic!("gate function {name} not found in server.rs")
        });
        // Function start is the beginning of its signature line.
        let line_start = src[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
        let rest = &src[line_start..];
        // Next top-level function after this one.
        let mut end = rest.len();
        for marker in ["\nfn ", "\nasync fn ", "\npub(crate) fn ", "\npub fn "] {
            if let Some(i) = rest[marker.len()..].find(marker).map(|i| i + marker.len()) {
                end = end.min(i);
            }
        }
        let span = &rest[..end];
        assert!(
            !span.contains("origin"),
            "gate {name} reads origin: {span:.200}"
        );
    }
}

// --- T1/T2 binary tests: real processes, real signals ---

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_health(url: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    for _ in 0..60 {
        if let Ok(response) = client.get(format!("{url}/health")).send().await {
            if response.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("gateway did not become healthy at {url}");
}

fn spawn_gateway(bin: &str, dir: &std::path::Path, port: u16) -> std::process::Child {
    std::process::Command::new(bin)
        .arg("--mock-chat")
        .env("MOCK_GATEWAY_ADDR", format!("127.0.0.1:{port}"))
        .env("MOCK_FILES_ROOT", dir.join("files"))
        .env("MOCK_APPROVALS_PATH", dir.join("approvals.jsonl"))
        .env("MOCK_EXECUTIONS_PATH", dir.join("executions.jsonl"))
        .env("MOCK_EFFECTS_PATH", dir.join("effects.jsonl"))
        .env("MOCK_SQLITE_PATH", dir.join("gateway.sqlite3"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn gateway")
}

fn wait_child(mut child: std::process::Child, secs: u64) -> std::process::ExitStatus {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let status = child.wait().expect("wait child");
        let _ = tx.send(status);
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(status) => status,
        Err(_) => panic!("gateway did not exit within {secs}s of the signal"),
    }
}

#[tokio::test]
async fn p0e_binary_clean_shutdown_ends_workers_and_loses_assignments() {
    let bin = std::env::var("CARGO_BIN_EXE_mock-gateway").expect("mock-gateway test binary");
    let dir = temp_dir("clean-shutdown");
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}");
    let child = spawn_gateway(&bin, &dir, port);
    wait_health(&url).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let response = client
        .post(format!("{url}/approval/request"))
        .json(&approval_request_body("clean.txt", "hello"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body: Value = response.json().await.unwrap();
    let approval_id = body["tool_call_id"].as_str().unwrap().to_string();
    let effects: Value = client
        .get(format!("{url}/effects/list"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let effect = effects
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["approval_id"] == approval_id)
        .unwrap();
    let execution_id = effect["execution_id"].as_str().unwrap().to_string();
    // Takeover gives the shutdown something to lose: the
    // assignment belongs to the takeover worker, which the
    // shutdown must end (it is the process's current worker).
    let response = client
        .post(format!("{url}/assignments/takeover"))
        .json(&json!({ "execution_id": execution_id }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    // Clean shutdown: SIGINT, the graceful path in serve().
    let pid = child.id();
    let sigint = std::process::Command::new("kill")
        .arg("-INT")
        .arg(pid.to_string())
        .status()
        .expect("send SIGINT");
    assert!(sigint.success());
    let status = wait_child(child, 15);
    assert!(status.success(), "clean shutdown exits 0, got {status}");
    // Before any restart: every worker ended cleanly, the
    // assignment lost — the shutdown converged on its own.
    let conn = db(&dir);
    let workers = sqlite_store::list_worker_sessions(&conn).expect("workers");
    assert!(!workers.is_empty());
    assert!(workers.iter().all(|w| w.status == "ended"), "{workers:?}");
    assert!(workers
        .iter()
        .all(|w| w.detail.as_deref() == Some("clean shutdown")));
    let assignments = sqlite_store::list_assignments(&conn).expect("assignments");
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].status, "lost");
    assert_eq!(assignments[0].detail.as_deref(), Some("worker stopped"));
    // Restart: the execution is recoverable again.
    let mut child = spawn_gateway(&bin, &dir, port);
    wait_health(&url).await;
    let executions: Value = client
        .get(format!("{url}/executions/list"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let view = executions
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["execution_id"] == execution_id)
        .expect("execution after restart");
    assert_eq!(view["recoverable"], true);
    assert_eq!(view["active_assignment_id"], Value::Null);
    child.kill().expect("kill gateway");
    let _ = child.wait();
}

#[tokio::test]
async fn p0e_binary_kill9_keeps_state_and_scan_closes_workers() {
    let bin = std::env::var("CARGO_BIN_EXE_mock-gateway").expect("mock-gateway test binary");
    let dir = temp_dir("kill9-scan");
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}");
    let mut child = spawn_gateway(&bin, &dir, port);
    wait_health(&url).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let response = client
        .post(format!("{url}/approval/request"))
        .json(&approval_request_body("durable.txt", "hello"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body: Value = response.json().await.unwrap();
    let approval_id = body["tool_call_id"].as_str().unwrap().to_string();
    client
        .post(format!("{url}/approval/decide"))
        .json(&json!({ "tool_call_id": approval_id, "status": "approved" }))
        .send()
        .await
        .unwrap();
    let effects: Value = client
        .get(format!("{url}/effects/list"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let effect = effects
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["approval_id"] == approval_id)
        .unwrap();
    let effect_id = effect["effect_id"].as_str().unwrap().to_string();
    let execution_id = effect["execution_id"].as_str().unwrap().to_string();
    // Authorized but unconcluded at kill time.
    let response = client
        .post(format!("{url}/effects/transition"))
        .json(&json!({ "effect_id": effect_id, "status": "authorized" }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    // SIGKILL: no graceful shutdown, no cleanup runs.
    child.kill().expect("kill gateway");
    let _ = child.wait();
    // Restart: the scan closes the dead worker, state survives.
    let mut child = spawn_gateway(&bin, &dir, port);
    wait_health(&url).await;
    let approvals: Value = client
        .get(format!("{url}/approvals"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let kept = approvals
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["approval_id"] == approval_id)
        .expect("approval survives kill -9");
    assert_eq!(kept["status"], "approved");
    let effect: Value = client
        .get(format!("{url}/effects/get?effect_id={effect_id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(effect["status"], "authorized");
    let executions: Value = client
        .get(format!("{url}/executions/list"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let view = executions
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["execution_id"] == execution_id)
        .expect("execution after restart");
    assert_eq!(view["recoverable"], true);
    child.kill().expect("kill gateway again");
    let _ = child.wait();
    let conn = db(&dir);
    let workers = sqlite_store::list_worker_sessions(&conn).expect("workers");
    // worker-1 died by kill -9, so the restart scan ended it
    // with "process ended". (worker-2, created by that scan,
    // is still active after the final kill: no scan runs after
    // it, and that is exactly what the next scan would close.)
    let w1 = workers.iter().find(|w| w.worker_session_id == "worker-1").unwrap();
    assert_eq!(w1.status, "ended");
    assert_eq!(w1.detail.as_deref(), Some("process ended"));
    assert_eq!(sqlite_store::integrity_check(&conn).expect("integrity"), "ok");
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}
