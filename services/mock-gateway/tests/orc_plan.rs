//! ORC-1 tests (plan §3 + Q-ORC-03/04/05/12): the
//! Plan first-class object — draft a blueprint for a
//! task, read it back (both routes), step-structure
//! validation (missing required item → 400), Re-plan-if
//! storage, restart consistency, and a second draft
//! superseding the first (version +1, old row
//! superseded, `plan.superseded` event).
//!
//! HTTP routes only, plus direct store reads where
//! HTTP cannot reach the state (the plans rows, the
//! task's `plan_id` pointer, the plan events). No
//! approval flow exists in ORC-1: nothing here asks
//! for an approval, and no approve route is asserted.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mock_gateway::plan_store::{get_plan, PlanRow};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-orc1-{tag}-{}-{}",
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

fn build_state(dir: &Path) -> AppState {
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
            api_key: Some("fixture-key".to_string()),
        },
    ))
    .with_auto_drive(false)
    .with_executions_path(dir.join("executions.jsonl"))
    .with_effects_path(dir.join("effects.jsonl"))
    .with_evidence_path(dir.join("evidence.jsonl"))
    .with_binding_state_path(dir.join("bindings.jsonl"))
    .with_project_state_path(dir.join("projects.jsonl"))
    .with_location_state_path(dir.join("locations.jsonl"))
    .with_worktree_state_path(dir.join("worktrees.jsonl"));
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
    state: AppState,
}

impl Harness {
    async fn serve() -> Self {
        let dir = temp_dir("plan");
        std::fs::create_dir_all(dir.join("files").join("proj")).unwrap();
        let state = build_state(&dir);
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

    /// Restart the gateway on the same directory (same
    /// SQLite file, same JSONL files): the production
    /// restart path, not a fresh database.
    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let state = build_state(&self.dir);
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

    fn plan_row(&self, plan_id: &str) -> PlanRow {
        let conn = self.db();
        get_plan(&conn, plan_id)
            .expect("get plan")
            .expect("plan row exists")
    }

    fn plan_events(&self, event_type: &str) -> Vec<sqlite_store::EventRow> {
        let conn = self.db();
        sqlite_store::list_events(&conn)
            .expect("events")
            .into_iter()
            .filter(|e| e.event_type == event_type)
            .collect()
    }

    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        let response = self
            .client
            .post(format!("{}{}", self.url, path))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let value: Value = response.json().await.unwrap_or(Value::Null);
        (status, value)
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let response = self
            .client
            .get(format!("{}{}", self.url, path))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let value: Value = response.json().await.unwrap_or(Value::Null);
        (status, value)
    }

    async fn make_task(&self) -> String {
        let (status, value) = self
            .post("/tasks/create", json!({"directory": "proj"}))
            .await;
        assert_eq!(status, StatusCode::OK, "create task: {value}");
        value["task_id"].as_str().unwrap().to_string()
    }

    async fn task_record(&self, task_id: &str) -> Value {
        let (status, value) = self.get(&format!("/tasks/get?task_id={task_id}")).await;
        assert_eq!(status, StatusCode::OK, "get task: {value}");
        value
    }
}

fn step(step_no: i64, kind: &str) -> Value {
    json!({
        "step_no": step_no,
        "kind": kind,
        "path": "src/example.rs",
        "symbol": "example_fn",
        "change": "rename example_fn to fixture_fn",
        "verify_cmd": "cargo test -p mock-gateway example_fn",
        "expected": "test passes",
    })
}

fn draft_body(task_id: &str) -> Value {
    json!({
        "task_id": task_id,
        "steps": [step(1, "file_write"), step(2, "verify")],
        "replan_if": [{"signal": "the file does not exist", "action": "stop_and_hand_back"}],
    })
}

fn is_hex64(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

#[tokio::test]
async fn p1_draft_blueprint() {
    assert_eq!(DB_SCHEMA_VERSION, 21, "ORC-5 bumps the schema to v21");
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), 21);
    let task_id = h.make_task().await;
    let before = now_ms();
    let (status, plan) = h.post("/tasks/plan/draft", draft_body(&task_id)).await;
    assert_eq!(status, StatusCode::OK, "draft: {plan}");
    assert_eq!(plan["plan_id"], "plan-1");
    assert_eq!(plan["task_id"], task_id);
    assert_eq!(plan["execution_id"], Value::Null, "draft has no Execution yet");
    assert_eq!(plan["version"], 1);
    assert_eq!(plan["status"], "draft");
    assert_eq!(plan["planner_model"], "fixture", "default is the gateway model");
    let digest = plan["digest"].as_str().unwrap();
    assert!(is_hex64(digest), "digest is a sha256 hex: {digest}");
    assert_eq!(plan["steps"].as_array().unwrap().len(), 2);
    assert_eq!(plan["steps"][0]["step_no"], 1);
    assert_eq!(plan["steps"][0]["kind"], "file_write");
    assert_eq!(plan["steps"][0]["change"], "rename example_fn to fixture_fn");
    assert_eq!(plan["steps"][1]["step_no"], 2);
    assert_eq!(plan["replan_if"].as_array().unwrap().len(), 1);
    assert_eq!(plan["replan_if"][0]["signal"], "the file does not exist");
    assert_eq!(plan["replan_if"][0]["action"], "stop_and_hand_back");
    assert!(plan["created_ms"].as_i64().unwrap() >= before);
    // The stored row matches the response, and the task
    // points at its current plan.
    let row = h.plan_row("plan-1");
    assert_eq!(row.task_id, task_id);
    assert_eq!(row.version, 1);
    assert_eq!(row.status, "draft");
    assert_eq!(row.digest, digest);
    assert_eq!(row.planner_model, "fixture");
    assert_eq!(h.task_record(&task_id).await["plan_id"], "plan-1");
    // The created event carries counts + digest only —
    // the blueprint body text never enters an event.
    let events = h.plan_events("plan.created");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].aggregate_kind, "plan");
    assert_eq!(events[0].aggregate_id, "plan-1");
    let payload: Value = serde_json::from_str(&events[0].payload_json).unwrap();
    assert_eq!(payload["plan_id"], "plan-1");
    assert_eq!(payload["task_id"], task_id);
    assert_eq!(payload["version"], 1);
    assert_eq!(payload["step_count"], 2);
    assert_eq!(payload["digest"], digest);
    assert!(
        !events[0].payload_json.contains("rename example_fn"),
        "blueprint body text must not enter events: {}",
        events[0].payload_json
    );
}

#[tokio::test]
async fn p2_read_blueprint_both_routes() {
    let h = Harness::serve().await;
    let task_id = h.make_task().await;
    let (_, plan) = h.post("/tasks/plan/draft", draft_body(&task_id)).await;
    let plan_id = plan["plan_id"].as_str().unwrap().to_string();
    // Current plan by task.
    let (status, current) = h.get(&format!("/tasks/plan?task_id={task_id}")).await;
    assert_eq!(status, StatusCode::OK, "current plan: {current}");
    assert_eq!(current, plan, "same plan object by task");
    // One row by id.
    let (status, by_id) = h.get(&format!("/tasks/plan/{plan_id}")).await;
    assert_eq!(status, StatusCode::OK, "plan by id: {by_id}");
    assert_eq!(by_id, plan, "same plan object by id");
    // Bare list without task_id.
    let (status, list) = h.get("/tasks/plan").await;
    assert_eq!(status, StatusCode::OK, "plan list: {list}");
    let rows = list.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], plan);
    // Unknown task / unknown plan / task with no plan.
    let (status, value) = h.get("/tasks/plan?task_id=task-99").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unknown task: {value}");
    assert_eq!(value["error"], "unknown task");
    let (status, value) = h.get("/tasks/plan/plan-99").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unknown plan: {value}");
    assert_eq!(value["error"], "unknown plan");
    let other = h.make_task().await;
    let (status, value) = h.get(&format!("/tasks/plan?task_id={other}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "no plan yet: {value}");
    assert_eq!(value["error"], "no plan");
}

#[tokio::test]
async fn p3_missing_required_item_is_400() {
    let h = Harness::serve().await;
    let task_id = h.make_task().await;
    // Unknown task → 404 before any validation.
    let (status, value) = h.post("/tasks/plan/draft", draft_body("task-99")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unknown task: {value}");
    // Empty steps.
    let (status, value) = h
        .post("/tasks/plan/draft", json!({"task_id": task_id, "steps": []}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty steps: {value}");
    // One step missing `change` (the four-piece set is
    // required, Q-ORC-04).
    let mut missing = step(1, "file_write");
    missing.as_object_mut().unwrap().remove("change");
    let (status, value) = h
        .post(
            "/tasks/plan/draft",
            json!({"task_id": task_id, "steps": [missing]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "missing change: {value}");
    assert!(value["error"].as_str().unwrap().contains("change"), "{value}");
    // Unknown kind.
    let (status, value) = h
        .post(
            "/tasks/plan/draft",
            json!({"task_id": task_id, "steps": [step(1, "improvise")]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "bad kind: {value}");
    // step_no gap: 1 then 3 is not consecutive.
    let (status, value) = h
        .post(
            "/tasks/plan/draft",
            json!({"task_id": task_id, "steps": [step(1, "file_write"), step(3, "verify")]}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "step_no gap: {value}");
    // Re-plan-if action other than stop_and_hand_back.
    let (status, value) = h
        .post(
            "/tasks/plan/draft",
            json!({
                "task_id": task_id,
                "steps": [step(1, "file_write")],
                "replan_if": [{"signal": "anything", "action": "retry"}],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "bad replan_if action: {value}");
    // No plan row was created by any of the failures.
    let (status, list) = h.get("/tasks/plan").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn p4_replan_if_store_and_read() {
    let h = Harness::serve().await;
    let task_id = h.make_task().await;
    // Two conditions, plus an empty replan_if on a
    // second task.
    let (status, plan) = h
        .post(
            "/tasks/plan/draft",
            json!({
                "task_id": task_id,
                "steps": [step(1, "command")],
                "replan_if": [
                    {"signal": "the command fails", "action": "stop_and_hand_back"},
                    {"signal": "the file is locked", "action": "stop_and_hand_back"},
                ],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "draft: {plan}");
    assert_eq!(plan["replan_if"].as_array().unwrap().len(), 2);
    assert_eq!(plan["replan_if"][1]["signal"], "the file is locked");
    let (status, by_id) = h.get("/tasks/plan/plan-1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(by_id["replan_if"], plan["replan_if"]);
    let other = h.make_task().await;
    let (status, plan) = h
        .post(
            "/tasks/plan/draft",
            json!({"task_id": other, "steps": [step(1, "review")], "replan_if": []}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "empty replan_if: {plan}");
    assert_eq!(plan["replan_if"], json!([]));
    let row = h.plan_row("plan-2");
    assert_eq!(row.replan_if_json, "[]");
}

#[tokio::test]
async fn p5_restart_consistency() {
    let mut h = Harness::serve().await;
    let task_id = h.make_task().await;
    let (_, plan) = h.post("/tasks/plan/draft", draft_body(&task_id)).await;
    h.restart().await;
    assert_eq!(h.schema_version(), 21, "schema stays v21 across restart");
    let (status, current) = h.get(&format!("/tasks/plan?task_id={task_id}")).await;
    assert_eq!(status, StatusCode::OK, "current plan after restart: {current}");
    assert_eq!(current, plan, "same plan object after restart");
    assert_eq!(h.task_record(&task_id).await["plan_id"], "plan-1");
    // The stored row is field-identical.
    let row = h.plan_row("plan-1");
    assert_eq!(row.digest, plan["digest"].as_str().unwrap());
    assert_eq!(row.status, "draft");
}

#[tokio::test]
async fn p6_second_draft_supersedes_first() {
    let h = Harness::serve().await;
    let task_id = h.make_task().await;
    let (_, v1) = h.post("/tasks/plan/draft", draft_body(&task_id)).await;
    let v1_digest = v1["digest"].as_str().unwrap().to_string();
    // A different blueprint: one more step, so the
    // digest must differ too.
    let mut v2_body = draft_body(&task_id);
    v2_body["steps"]
        .as_array_mut()
        .unwrap()
        .push(step(3, "review"));
    let (status, v2) = h.post("/tasks/plan/draft", v2_body).await;
    assert_eq!(status, StatusCode::OK, "second draft: {v2}");
    assert_eq!(v2["plan_id"], "plan-2");
    assert_eq!(v2["version"], 2);
    assert_eq!(v2["status"], "draft");
    assert_ne!(v2["digest"].as_str().unwrap(), v1_digest);
    assert_eq!(v2["steps"].as_array().unwrap().len(), 3);
    // The old row is superseded, not deleted.
    let (status, old) = h.get("/tasks/plan/plan-1").await;
    assert_eq!(status, StatusCode::OK, "old plan still readable: {old}");
    assert_eq!(old["status"], "superseded");
    assert_eq!(old["version"], 1);
    assert_eq!(old["digest"], v1_digest);
    assert_eq!(h.plan_row("plan-1").status, "superseded");
    // The task's current plan is the new version.
    let (status, current) = h.get(&format!("/tasks/plan?task_id={task_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(current, v2);
    assert_eq!(h.task_record(&task_id).await["plan_id"], "plan-2");
    // Exactly one superseded event, for the old row,
    // counts + digest only.
    let superseded = h.plan_events("plan.superseded");
    assert_eq!(superseded.len(), 1);
    assert_eq!(superseded[0].aggregate_id, "plan-1");
    let payload: Value = serde_json::from_str(&superseded[0].payload_json).unwrap();
    assert_eq!(payload["plan_id"], "plan-1");
    assert_eq!(payload["version"], 1);
    assert_eq!(payload["step_count"], 2);
    assert_eq!(payload["digest"], v1_digest);
    assert!(
        !superseded[0].payload_json.contains("rename example_fn"),
        "blueprint body text must not enter events"
    );
    // Two created events, one per version.
    assert_eq!(h.plan_events("plan.created").len(), 2);
}
