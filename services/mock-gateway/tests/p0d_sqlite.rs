//! P0-D tests: SQLite persistence for the unified plane.
//! Gateway kernel only: no client, no UI.
//!
//! Covered here (pa-p0d-plan-draft.md §8, server surface):
//! - D2 shadow dual-write: HTTP writes land in JSONL and in
//!   SQLite, and the shadow comparison is green;
//! - D3/D4 import + switch: folded JSONL state (including an
//!   old schema-0 row) appears in SQLite, counters take the max
//!   of both sources, JSONL appends stop after the switch and
//!   the JSONL files become generated audit exports;
//! - restart equivalence: a fresh state on the same database
//!   serves the same migrated-domain rows;
//! - Inbox over HTTP: approval_required from a bound pending
//!   approval, failed (ack-only), unknown auto-closed by
//!   reconciliation, ack idempotence via the list surface;
//! - Outbox over HTTP: a seeded local_notification row is
//!   listed / fetched (no redelivery route exists in P0-D);
//! - fail-closed: a database that cannot be opened answers
//!   5xx on the migrated-domain surface while chat still works;
//! - durability: a kill -9 (Child::kill, SIGKILL on Unix) of
//!   a running gateway loses no committed approval / effect,
//!   and the reopened database passes integrity_check.

use mock_gateway::approval_store::{append_record, ApprovalFileRecord};
use mock_gateway::effect_store::{append_effect, EffectFileRecord};
use mock_gateway::execution_store::{append_execution, ExecutionFileRecord};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p0d-{}-{}-{}",
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

fn approval_request_body(path: &str, content: &str) -> Value {
    use base64::Engine;
    json!({
        "tool": "fs.write",
        "path": path,
        "content_base64": base64::engine::general_purpose::STANDARD.encode(content),
        "message": "please write",
    })
}

#[tokio::test]
async fn p0d_shadow_dual_write_comparison_is_green() {
    let dir = temp_dir("shadow");
    let state = base_state(&dir).with_sqlite_path(dir.join("gateway.sqlite3"));
    assert_eq!(state.sqlite_backend_name(), "shadow");
    let h = Harness::serve(state, dir.clone()).await;
    let body = h
        .ok_post("/approval/request", approval_request_body("a.txt", "hello"))
        .await;
    let approval_id = body["tool_call_id"].as_str().unwrap().to_string();
    assert_eq!(h.state.sqlite_shadow_failures(), 0);
    // JSONL still appended in shadow mode.
    let jsonl_approvals =
        mock_gateway::approval_store::load_records(&dir.join("approvals.jsonl"));
    assert!(jsonl_approvals.iter().any(|r| r.tool_call_id == approval_id));
    // SQLite shadow holds the same folded rows (test seam:
    // open the same file through sqlite_store and compare).
    let conn = sqlite_store::open_db(&dir.join("gateway.sqlite3")).expect("open db");
    let report = sqlite_store::shadow_compare(
        &conn,
        &mock_gateway::execution_store::load_executions(&dir.join("executions.jsonl")),
        &mock_gateway::effect_store::load_effects(&dir.join("effects.jsonl")),
        &jsonl_approvals,
    )
    .expect("compare");
    assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
    h.server.abort();
}

#[tokio::test]
async fn p0d_import_switch_restart_and_export_equivalence() {
    let dir = temp_dir("switch");
    // Pre-existing JSONL history: an old schema-0 execution,
    // an old effect, an old approval.
    append_execution(
        &dir.join("executions.jsonl"),
        &ExecutionFileRecord {
            execution_id: "execution-5".to_string(),
            task_id: Some("task-9".to_string()),
            loop_id: None,
            session_id: None,
            goal_id: None,
            worker_session_id: None,
            workspace_snapshot_id: None,
            status: "running".to_string(),
            origin_device_id: "unknown".to_string(),
            schema_version: 0,
        },
    );
    append_effect(
        &dir.join("effects.jsonl"),
        &EffectFileRecord {
            effect_id: "effect-3".to_string(),
            execution_id: Some("execution-5".to_string()),
            capability: "fs".to_string(),
            operation: "write".to_string(),
            scope: "old.txt".to_string(),
            input_digest: Some("old-digest".to_string()),
            approval_id: None,
            status: "completed".to_string(),
            idempotency_key: None,
            origin_device_id: "unknown".to_string(),
            schema_version: 0,
            reconciliation: None,
            detail: None,
        },
    );
    append_record(
        &dir.join("approvals.jsonl"),
        &ApprovalFileRecord {
            tool_call_id: "approval-2".to_string(),
            tool: "fs.write".to_string(),
            status: "pending".to_string(),
            binding: None,
            consumed: false,
            operation_digest: None,
            action_binding: None,
            effect_binding: None,
            message: None,
            // P3-C field; this fixture models a pre-P3-C row,
            // which carries no Worker command spec.
            command: None,
            // E2-A field; this fixture models a pre-E2-A row.
            consumed_by_worker_session_id: None,
        },
    )
    .expect("append approval");
    let state = base_state(&dir).with_sqlite_path(dir.join("gateway.sqlite3"));
    assert_eq!(state.sqlite_backend_name(), "shadow");
    let report = state.sqlite_import_and_switch().expect("switch");
    assert_eq!(report.executions, 1);
    assert_eq!(report.effects, 1);
    assert_eq!(report.approvals, 1);
    assert!(!report.already_done);
    assert_eq!(state.sqlite_backend_name(), "sqlite");
    // Re-running the switch is a no-op.
    assert!(state.sqlite_import_and_switch().expect("reswitch").already_done);
    let h = Harness::serve(state, dir.clone()).await;
    let executions = h.ok_get("/executions/list").await;
    let exec_ids: Vec<&str> = executions
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["execution_id"].as_str().unwrap())
        .collect();
    assert!(exec_ids.contains(&"execution-5"));
    let effects = h.ok_get("/effects/list").await;
    let effect_rows = effects.as_array().unwrap();
    let old = effect_rows
        .iter()
        .find(|v| v["effect_id"] == "effect-3")
        .expect("imported effect");
    // Old status words are never translated on read.
    assert_eq!(old["status"], "completed");
    assert_eq!(old["schema_version"], 0);
    // A new bound approval: counters resume past both sources
    // (approval-2 imported -> next is approval-3; the planned
    // effect's adapter execution follows execution-5 -> 6).
    let body = h
        .ok_post("/approval/request", approval_request_body("b.txt", "world"))
        .await;
    assert_eq!(body["tool_call_id"], "approval-3");
    let executions = h.ok_get("/executions/list").await;
    let exec_ids: Vec<&str> = executions
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["execution_id"].as_str().unwrap())
        .collect();
    assert!(exec_ids.contains(&"execution-6"));
    // After the switch the JSONL files are generated audit
    // exports: one snapshot line per current row, no history
    // appends. The executions export holds exactly the two rows.
    let exported =
        mock_gateway::execution_store::load_executions(&dir.join("executions.jsonl"));
    assert_eq!(exported.len(), 2);
    let exported_ids: Vec<&str> = exported
        .iter()
        .map(|r| r.execution_id.as_str())
        .collect();
    assert!(exported_ids.contains(&"execution-5"));
    assert!(exported_ids.contains(&"execution-6"));
    // Restart equivalence: a fresh state on the same database
    // (import_done already set) serves the same rows from SQLite.
    let restarted = base_state(&dir).with_sqlite_path(dir.join("gateway.sqlite3"));
    assert_eq!(restarted.sqlite_backend_name(), "sqlite");
    let h2 = Harness::serve(restarted, dir.clone()).await;
    let executions2 = h2.ok_get("/executions/list").await;
    assert_eq!(executions, executions2);
    h.server.abort();
    h2.server.abort();
}

#[tokio::test]
async fn p0d_inbox_http_lifecycle() {
    let dir = temp_dir("inbox");
    let state = base_state(&dir).with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    let h = Harness::serve(state, dir.clone()).await;
    let body = h
        .ok_post("/approval/request", approval_request_body("c.txt", "data"))
        .await;
    let approval_id = body["tool_call_id"].as_str().unwrap().to_string();
    // The bound pending approval produced approval_required.
    let inbox = h.ok_get("/inbox/list?open_only=true").await;
    let approval_rows: Vec<&Value> = inbox
        .as_array()
        .unwrap()
        .iter()
        .filter(|v| v["kind"] == "approval_required")
        .collect();
    assert_eq!(approval_rows.len(), 1);
    assert_eq!(approval_rows[0]["approval_id"], approval_id);
    let inbox_id = approval_rows[0]["inbox_id"].as_str().unwrap().to_string();
    let fetched = h
        .ok_get(&format!("/inbox/get?inbox_id={inbox_id}"))
        .await;
    assert_eq!(fetched["inbox_id"], inbox_id);
    // Deciding the approval auto-closes its approval_required row.
    h.ok_post(
        "/approval/decide",
        json!({ "tool_call_id": approval_id, "status": "approved" }),
    )
    .await;
    let open = h.ok_get("/inbox/list?open_only=true").await;
    assert!(!open
        .as_array()
        .unwrap()
        .iter()
        .any(|v| v["inbox_id"] == inbox_id));
    // Drive the planned effect to failed: failed closes by ack only.
    let effects = h.ok_get("/effects/list").await;
    let effect_id = effects.as_array().unwrap()[0]["effect_id"]
        .as_str()
        .unwrap()
        .to_string();
    for status in ["authorized", "started", "failed"] {
        h.ok_post(
            "/effects/transition",
            json!({ "effect_id": effect_id, "status": status }),
        )
        .await;
    }
    let failed = h.ok_get("/inbox/list?kind=failed&open_only=true").await;
    let failed_rows: Vec<&Value> = failed.as_array().unwrap().iter().collect();
    assert_eq!(failed_rows.len(), 1);
    let failed_id = failed_rows[0]["inbox_id"].as_str().unwrap().to_string();
    let acked = h
        .ok_post("/inbox/ack", json!({ "inbox_id": failed_id }))
        .await;
    assert_eq!(acked["resolved_reason"], "acked");
    let open = h.ok_get("/inbox/list?kind=failed&open_only=true").await;
    assert!(open.as_array().unwrap().is_empty());
    // Unknown -> reconciled auto-closes unknown_requires_reconciliation.
    let body = h
        .ok_post("/approval/request", approval_request_body("d.txt", "more"))
        .await;
    let _ = body;
    let effects = h.ok_get("/effects/list").await;
    let effect2 = effects.as_array().unwrap().last().unwrap()["effect_id"]
        .as_str()
        .unwrap()
        .to_string();
    for status in ["authorized", "started", "unknown"] {
        h.ok_post(
            "/effects/transition",
            json!({ "effect_id": effect2, "status": status }),
        )
        .await;
    }
    let unknown = h
        .ok_get("/inbox/list?kind=unknown_requires_reconciliation&open_only=true")
        .await;
    assert_eq!(unknown.as_array().unwrap().len(), 1);
    h.ok_post(
        "/effects/transition",
        json!({ "effect_id": effect2, "status": "reconciling" }),
    )
    .await;
    h.ok_post(
        "/effects/transition",
        json!({ "effect_id": effect2, "status": "reconciled", "reconciliation_result": "completed" }),
    )
    .await;
    let unknown = h
        .ok_get("/inbox/list?kind=unknown_requires_reconciliation&open_only=true")
        .await;
    assert!(unknown.as_array().unwrap().is_empty());
    h.server.abort();
}

#[tokio::test]
async fn p0d_outbox_http_lists_seeded_row() {
    let dir = temp_dir("outbox");
    let state = base_state(&dir).with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    // Test seam: enqueue through sqlite_store directly (no
    // production route enqueues in P0-D; the sink is log-only).
    {
        let mut conn = sqlite_store::open_db(&dir.join("gateway.sqlite3")).expect("open db");
        let row = sqlite_store::enqueue_outbox(&mut conn, "execution-1", "{\"result\":\"done\"}")
            .expect("enqueue");
        assert_eq!(row.destination_kind, "local_notification");
    }
    let h = Harness::serve(state, dir.clone()).await;
    let list = h.ok_get("/outbox/list").await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    let outbox_id = list[0]["outbox_id"].as_str().unwrap().to_string();
    let fetched = h
        .ok_get(&format!("/outbox/get?outbox_id={outbox_id}"))
        .await;
    assert_eq!(fetched["execution_id"], "execution-1");
    assert_eq!(fetched["status"], "pending");
    h.server.abort();
}

#[tokio::test]
async fn p0d_broken_sqlite_fails_closed_and_chat_survives() {
    let dir = temp_dir("broken");
    // A directory is not an openable database file.
    let db_dir = dir.join("gateway.sqlite3");
    std::fs::create_dir_all(&db_dir).unwrap();
    let state = base_state(&dir).with_sqlite_path(db_dir);
    assert_eq!(state.sqlite_backend_name(), "sqlite");
    assert!(state.sqlite_is_broken());
    let h = Harness::serve(state, dir.clone()).await;
    let (status, body) = h.get("/executions/list").await;
    assert_eq!(status, 500);
    assert_eq!(body["error"], "sqlite_unavailable");
    let (status, _) = h.get("/health").await;
    assert_eq!(status, 200);
    let (status, _) = h.get("/chat/history").await;
    assert_eq!(status, 200);
    // A bound approval request fails closed (5xx), not issued.
    let (status, _) = h
        .post("/approval/request", approval_request_body("e.txt", "x"))
        .await;
    assert_eq!(status, 500);
    h.server.abort();
}

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

#[tokio::test]
async fn p0d_kill9_keeps_committed_state() {
    let bin = std::env::var("CARGO_BIN_EXE_mock-gateway")
        .expect("mock-gateway test binary");
    let dir = temp_dir("kill9");
    let files_root = dir.join("files");
    std::fs::create_dir_all(&files_root).unwrap();
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}");
    let spawn = || {
        std::process::Command::new(&bin)
            .arg("--mock-chat")
            .env("MOCK_GATEWAY_ADDR", format!("127.0.0.1:{port}"))
            .env("MOCK_FILES_ROOT", &files_root)
            .env("MOCK_APPROVALS_PATH", dir.join("approvals.jsonl"))
            .env("MOCK_EXECUTIONS_PATH", dir.join("executions.jsonl"))
            .env("MOCK_EFFECTS_PATH", dir.join("effects.jsonl"))
            .env("MOCK_SQLITE_PATH", dir.join("gateway.sqlite3"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn gateway")
    };
    let mut child = spawn();
    wait_health(&url).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let response = client
        .post(format!("{url}/approval/request"))
        .json(&approval_request_body("kill.txt", "durable"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let body: Value = response.json().await.unwrap();
    let approval_id = body["tool_call_id"].as_str().unwrap().to_string();
    // SIGKILL mid-run: no graceful shutdown, no checkpoint flush
    // beyond what SQLite committed (WAL + FULL).
    child.kill().expect("kill gateway");
    let _ = child.wait();
    let mut child = spawn();
    wait_health(&url).await;
    let approvals: Value = client
        .get(format!("{url}/approvals"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        approvals
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["approval_id"] == approval_id),
        "{approvals}"
    );
    let effects: Value = client
        .get(format!("{url}/effects/list"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        effects
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["approval_id"] == approval_id),
        "{effects}"
    );
    child.kill().expect("kill gateway again");
    let _ = child.wait();
    let conn = sqlite_store::open_db(&dir.join("gateway.sqlite3")).expect("reopen db");
    assert_eq!(sqlite_store::integrity_check(&conn).expect("integrity"), "ok");
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}
