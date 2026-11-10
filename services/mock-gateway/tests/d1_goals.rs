//! D1-1 tests (plan §2, G1–G4): Goal as a first-class object,
//! Task link fields, Execution session/goal binding, and
//! snapshot content-drift detection.
//!
//! HTTP routes only (plus `sqlite_store` reads after the
//! fact, never to fake a production step). Loops run on the
//! scripted model; the task run uses a fake DeepSeek key
//! against an unreachable base URL, so the runner fails
//! fast in the background — the Execution row is created
//! synchronously by the run route, before the runner spawns.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
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
    .with_deepseek(DeepSeekClient::new(DeepSeekConfig {
        base_url: "http://127.0.0.1:1".to_string(),
        model: "fixture".to_string(),
        api_key: Some("fake-d1-key".to_string()),
    }))
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
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        Self::serve_with_tasks_seed(replies, None).await
    }

    async fn serve_with_tasks_seed(replies: Vec<ModelReply>, tasks_seed: Option<&str>) -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        if let Some(seed) = tasks_seed {
            std::fs::write(dir.join("tasks.jsonl"), seed).unwrap();
        }
        let model = Arc::new(ScriptModel::new(replies));
        let state = build_state(&dir, model);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            // A loop step can block up to its 10 s budget;
            // 60 s leaves ample headroom on slow CI.
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            server: Some(server),
        }
    }

    fn db(&self) -> rusqlite::Connection {
        let conn = sqlite_store::open_db(&self.dir.join("gateway.sqlite3")).expect("open db");
        conn.busy_timeout(Duration::from_secs(5))
            .expect("busy timeout");
        conn
    }

    /// Restart the gateway on the same directory (same
    /// SQLite file, same JSONL files): the production
    /// restart path, not a fresh database. No new loops
    /// start after the restart, so the fresh scripted model
    /// needs no replies.
    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let model = Arc::new(ScriptModel::new(vec![]));
        let state = build_state(&self.dir, model);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server = Some(
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() }),
        );
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
}

/// G1: Goal create/list/detail/archive round-trip.
/// Archive hides the row from the list, detail still
/// reads it, re-archive is idempotent.
#[tokio::test]
async fn g1_goal_create_list_detail_archive() {
    let h = Harness::serve(vec![]).await;
    let (status, _) = h.post("/goals/create", json!({})).await;
    assert_eq!(status, 400);
    let (status, _) = h.post("/goals/create", json!({"title": "   "})).await;
    assert_eq!(status, 400);
    let g1 = h.ok("/goals/create", json!({"title": "Ship D1"})).await;
    assert_eq!(g1["goal_id"], "goal-1");
    assert_eq!(g1["status"], "active");
    assert_eq!(g1["title"], "Ship D1");
    assert_eq!(g1["description"], "");
    let g2 = h
        .ok(
            "/goals/create",
            json!({"title": "Second", "description": "desc"}),
        )
        .await;
    assert_eq!(g2["goal_id"], "goal-2");
    assert_eq!(g2["description"], "desc");
    let list = h.ok_get("/goals/list").await;
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert_eq!(list[0]["goal_id"], "goal-1");
    let detail = h.ok_get("/goals/goal-1").await;
    assert_eq!(detail["goal_id"], "goal-1");
    let (status, body) = h.get("/goals/goal-999").await;
    assert_eq!(status, 404);
    assert_eq!(body["error"], "unknown goal");
    let archived = h.ok("/goals/goal-1/archive", json!({})).await;
    assert_eq!(archived["status"], "archived");
    let list = h.ok_get("/goals/list").await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["goal_id"], "goal-2");
    let detail = h.ok_get("/goals/goal-1").await;
    assert_eq!(detail["status"], "archived");
    let again = h.ok("/goals/goal-1/archive", json!({})).await;
    assert_eq!(again["status"], "archived");
    let (status, _) = h.post("/goals/goal-999/archive", json!({})).await;
    assert_eq!(status, 404);
}

/// G2: Task link fields round-trip + list filters; an old
/// JSONL row without the three fields reads back as None
/// and is never migrated (its line stays verbatim).
#[tokio::test]
async fn g2_task_link_fields_roundtrip_and_old_rows() {
    let seed = "{\"task_id\":\"task-0\",\"directory\":\"proj\",\"artifact_path\":null,\"status\":\"created\",\"error\":null,\"approval_id\":null,\"draft_sha256\":null,\"draft_bytes\":null,\"execute_binding\":null}\n";
    let h = Harness::serve_with_tasks_seed(vec![], Some(seed)).await;
    std::fs::create_dir_all(h.dir.join("files").join("proj")).unwrap();
    let list = h.ok_get("/tasks/list").await;
    let old = list
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["task_id"] == "task-0")
        .expect("seeded old task");
    assert_eq!(old["goal_id"], Value::Null);
    assert_eq!(old["project_id"], Value::Null);
    assert_eq!(old["workspace_id"], Value::Null);
    let filtered = h.ok_get("/tasks/list?goal_id=goal-1").await;
    assert!(
        filtered
            .as_array()
            .unwrap()
            .iter()
            .all(|t| t["task_id"] != "task-0"),
        "old row excluded by link filter: {filtered}"
    );

    let goal1 = h.ok("/goals/create", json!({"title": "G1"})).await;
    assert_eq!(goal1["goal_id"], "goal-1");
    let goal2 = h.ok("/goals/create", json!({"title": "G2"})).await;
    assert_eq!(goal2["goal_id"], "goal-2");
    let task = h
        .ok(
            "/tasks/create",
            json!({"directory": "proj", "goal_id": "goal-1", "project_id": "project-1", "workspace_id": "workspace-1"}),
        )
        .await;
    assert_eq!(task["task_id"], "task-1");
    assert_eq!(task["goal_id"], "goal-1");
    assert_eq!(task["project_id"], "project-1");
    assert_eq!(task["workspace_id"], "workspace-1");
    let by_goal = h.ok_get("/tasks/list?goal_id=goal-1").await;
    assert_eq!(by_goal.as_array().unwrap().len(), 1);
    assert_eq!(by_goal[0]["task_id"], "task-1");
    let by_project = h.ok_get("/tasks/list?project_id=project-1").await;
    assert_eq!(by_project.as_array().unwrap().len(), 1);
    let by_workspace = h.ok_get("/tasks/list?workspace_id=workspace-1").await;
    assert_eq!(by_workspace.as_array().unwrap().len(), 1);
    let by_missing = h.ok_get("/tasks/list?goal_id=goal-999").await;
    assert_eq!(by_missing.as_array().unwrap().len(), 0);
    // Update moves the link; the old goal's list empties.
    // Changing a link does not freeze anything.
    let updated = h
        .ok(
            "/tasks/update",
            json!({"task_id": "task-1", "status": "created", "goal_id": "goal-2"}),
        )
        .await;
    assert_eq!(updated["goal_id"], "goal-2");
    assert_eq!(updated["project_id"], "project-1");
    let by_goal1 = h.ok_get("/tasks/list?goal_id=goal-1").await;
    assert_eq!(by_goal1.as_array().unwrap().len(), 0);
    let by_goal2 = h.ok_get("/tasks/list?goal_id=goal-2").await;
    assert_eq!(by_goal2.as_array().unwrap().len(), 1);
    let content = std::fs::read_to_string(h.dir.join("tasks.jsonl")).unwrap();
    assert!(
        content.lines().any(|line| line.trim() == seed.trim()),
        "old line preserved verbatim:\n{content}"
    );
}

/// G3: Execution binds session + goal. A loop execution
/// carries the loop caller's session_id and goal_id; a
/// task execution resolves goal_id from the Task record
/// and keeps session_id None (no session caller).
#[tokio::test]
async fn g3_execution_binds_session_and_goal() {
    let h = Harness::serve(vec![done()]).await;
    let goal = h.ok("/goals/create", json!({"title": "G1"})).await;
    assert_eq!(goal["goal_id"], "goal-1");
    let started = h
        .ok(
            "/agent/loop/start",
            json!({"prompt": "hello", "session_id": "session-9", "goal_id": "goal-1"}),
        )
        .await;
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let exec = h.execution_for_loop(&loop_id).await;
    assert_eq!(exec["session_id"], "session-9");
    assert_eq!(exec["goal_id"], "goal-1");
    std::fs::create_dir_all(h.dir.join("files").join("proj")).unwrap();
    let task = h
        .ok(
            "/tasks/create",
            json!({"directory": "proj", "goal_id": "goal-1"}),
        )
        .await;
    let task_id = task["task_id"].as_str().unwrap().to_string();
    let (status, _) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, 200);
    let list = h.ok_get("/executions/list").await;
    let exec = list
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["task_id"] == task_id)
        .unwrap_or_else(|| panic!("no execution for task {task_id}: {list}"));
    assert_eq!(exec["goal_id"], "goal-1");
    assert_eq!(exec["session_id"], Value::Null);
}

/// G4: snapshot content drift is detectable. After a
/// connector's connection state changes, the new
/// Execution's snapshot digest changes, while the old
/// snapshot row stays byte-identical — including after a
/// gateway restart. `memory_scope` is still the "default"
/// placeholder this batch (D1-5 fills the real value).
#[tokio::test]
async fn g4_snapshot_content_drift_detectable() {
    let mut h = Harness::serve(vec![done(), done()]).await;
    let started = h.ok("/agent/loop/start", json!({"prompt": "first"})).await;
    let loop1 = started["loop_id"].as_str().unwrap().to_string();
    let exec1 = h.execution_for_loop(&loop1).await;
    let snap1_id = exec1["workspace_snapshot_id"]
        .as_str()
        .unwrap()
        .to_string();
    let snap1 = {
        let conn = h.db();
        sqlite_store::get_snapshot(&conn, &snap1_id)
            .expect("read snapshot-1")
            .expect("snapshot-1 exists")
    };
    let snap1_json = serde_json::to_string(&snap1).unwrap();
    assert_eq!(snap1.memory_scope, "default");
    assert_eq!(snap1.connector_ids_json, "[]");
    // Change one connector's connection state: link an
    // account, then connect.
    h.ok(
        "/connectors/notes/accounts/link",
        json!({"display_name": "Test"}),
    )
    .await;
    h.ok("/connectors/notes/connect", json!({})).await;
    let started = h.ok("/agent/loop/start", json!({"prompt": "second"})).await;
    let loop2 = started["loop_id"].as_str().unwrap().to_string();
    let exec2 = h.execution_for_loop(&loop2).await;
    let snap2_id = exec2["workspace_snapshot_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(snap2_id, snap1_id);
    let (snap1_again, snap2) = {
        let conn = h.db();
        (
            sqlite_store::get_snapshot(&conn, &snap1_id)
                .expect("re-read snapshot-1")
                .expect("snapshot-1 exists"),
            sqlite_store::get_snapshot(&conn, &snap2_id)
                .expect("read snapshot-2")
                .expect("snapshot-2 exists"),
        )
    };
    assert_ne!(snap2.content_sha256, snap1.content_sha256);
    assert!(
        snap2.connector_ids_json.contains("notes:sha256:"),
        "connector entry carries a content digest: {}",
        snap2.connector_ids_json
    );
    assert_eq!(snap1_again, snap1);
    assert_eq!(serde_json::to_string(&snap1_again).unwrap(), snap1_json);
    assert_eq!(snap1_again.memory_scope, "default");
    h.restart().await;
    let snap1_after_restart = {
        let conn = h.db();
        sqlite_store::get_snapshot(&conn, &snap1_id)
            .expect("read snapshot-1 after restart")
            .expect("snapshot-1 exists")
    };
    assert_eq!(snap1_after_restart, snap1);
    assert_eq!(
        serde_json::to_string(&snap1_after_restart).unwrap(),
        snap1_json
    );
    let exec1_after = h
        .ok_get(&format!(
            "/executions/get?execution_id={}",
            exec1["execution_id"].as_str().unwrap()
        ))
        .await;
    assert_eq!(exec1_after["workspace_snapshot_id"], snap1_id);
}
