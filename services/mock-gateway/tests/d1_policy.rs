//! D1-2 tests (plan §3, P1–P5): Policy versioning,
//! Mandate binding, Approval versioning, the bound-event
//! backfill, and restart recovery from SQLite.
//!
//! HTTP routes only (plus `sqlite_store` reads after the
//! fact, never to fake a production step). Loops run on
//! the scripted model; the worker run in P4 uses real git
//! (`git-status`, a read-only command) against a fixture
//! repo, same as the E2-A A11 test.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use mock_gateway::worker::is_terminal_run_status;
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

/// One Defaults line: the seed policy for the import
/// tests (v1). `updated_ms` is fixed so the document is
/// deterministic across restarts.
const POLICY_SEED: &str = "{\"kind\":\"defaults\",\"hitl_enabled\":false,\"mitm_enabled\":false,\"tproxy_enabled\":false,\"sni_rejection_enabled\":false,\"updated_ms\":1000}\n";

fn real_git() -> PathBuf {
    mock_gateway::worker::resolve_git_path().expect("git on PATH")
}

fn real_cargo() -> PathBuf {
    mock_gateway::worker::resolve_cargo_path().expect("cargo on PATH")
}

fn git(args: &[&str], cwd: &Path) {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("run git");
    assert!(status.success(), "git {args:?} in {}", cwd.display());
}

fn init_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&["init", "-b", "main"], &repo);
    git(&["config", "user.email", "test@example.invalid"], &repo);
    git(&["config", "user.name", "d1-test"], &repo);
    std::fs::write(repo.join("a.txt"), "hello\n").unwrap();
    git(&["add", "-A"], &repo);
    git(&["commit", "-m", "init"], &repo);
    repo
}

fn build_state(
    dir: &Path,
    model: Arc<ScriptModel>,
    policy_seed: Option<&str>,
    policy_path: Option<PathBuf>,
    git_path: Option<PathBuf>,
    cargo_path: Option<PathBuf>,
) -> AppState {
    if let Some(seed) = policy_seed {
        std::fs::write(dir.join("policy.jsonl"), seed).unwrap();
    }
    let mut state = AppState::with_files_root_and_approvals(
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
    .with_worktree_state_path(dir.join("worktrees.jsonl"));
    if let Some(path) = policy_path {
        state = state.with_policy_state_path(path);
    }
    let state = state
        .with_git_path(git_path)
        .with_cargo_path(cargo_path)
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
    policy_path: Option<PathBuf>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        Self::serve_full(replies, None, None, None).await
    }

    async fn serve_with_policy(replies: Vec<ModelReply>, seed: &str) -> Self {
        Self::serve_full(replies, Some(seed.to_string()), None, None).await
    }

    async fn serve_with_worker(
        replies: Vec<ModelReply>,
        git_path: Option<PathBuf>,
        cargo_path: Option<PathBuf>,
    ) -> Self {
        Self::serve_full(replies, None, git_path, cargo_path).await
    }

    async fn serve_full(
        replies: Vec<ModelReply>,
        policy_seed: Option<String>,
        git_path: Option<PathBuf>,
        cargo_path: Option<PathBuf>,
    ) -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let policy_path = policy_seed.as_ref().map(|_| dir.join("policy.jsonl"));
        let state = build_state(
            &dir,
            model.clone(),
            policy_seed.as_deref(),
            policy_path.clone(),
            git_path,
            cargo_path,
        );
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
            model,
            policy_path,
        }
    }

    /// Restart the gateway on the same directory (same
    /// SQLite file, same JSONL files): the production
    /// restart path, not a fresh database. The policy file
    /// is attached when it was configured, but never
    /// re-seeded — a test that deleted it (P5) restarts
    /// without it.
    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let model = Arc::new(ScriptModel::new(vec![]));
        self.model = model.clone();
        let state = build_state(
            &self.dir,
            model,
            None,
            self.policy_path.clone(),
            None,
            None,
        );
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

    async fn ok_patch(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.patch(path, value).await;
        assert_eq!(status, 200, "{path}: {body}");
        body
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

    async fn approval(&self, id: &str) -> Value {
        self.ok_get(&format!("/approvals/{id}")).await
    }

    /// Binding + Location + Worktree for `repo`; returns the
    /// worktree id (P4 only).
    async fn setup_worktree(&self, repo: &Path) -> String {
        let repo_str = repo.to_string_lossy().into_owned();
        let (status, binding) = self
            .post(
                "/bindings/create",
                json!({"display_name": "repo", "path": repo_str}),
            )
            .await;
        assert_eq!(status, 200, "{binding}");
        let workspace_id = binding["workspace_id"].as_str().unwrap().to_string();
        let (status, location) = self
            .post(
                "/locations/create",
                json!({"workspace_id": workspace_id, "name": "root", "path": repo_str}),
            )
            .await;
        assert_eq!(status, 200, "{location}");
        let location_id = location["location_id"].as_str().unwrap().to_string();
        let (status, worktree) = self
            .post(
                "/worktrees/create",
                json!({"location_id": location_id, "directory_relative": "", "branch_label": "main", "project_id": null}),
            )
            .await;
        assert_eq!(status, 200, "{worktree}");
        worktree["worktree_id"].as_str().unwrap().to_string()
    }

    /// Request a worker.run approval and approve it; returns
    /// the approval id. `timeout_ms` must match the timeout
    /// the run is created with — the digest commits it, so a
    /// mismatch refuses the run (approval_binding_mismatch).
    async fn approve_worker_run(
        &self,
        worktree_id: &str,
        command: &str,
        params: Value,
        timeout_ms: Option<u64>,
    ) -> String {
        let (status, body) = self
            .post(
                "/approval/request",
                json!({
                    "tool": "worker.run",
                    "worktree_id": worktree_id,
                    "command_template": command,
                    "params": params,
                    "timeout_ms": timeout_ms,
                }),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["requires_approval"], true);
        let id = body["tool_call_id"].as_str().unwrap().to_string();
        let (status, _) = self
            .post(
                "/approval/decide",
                json!({"tool_call_id": id, "status": "approved"}),
            )
            .await;
        assert_eq!(status, 200, "decide {id}");
        id
    }

    async fn create_run(&self, body: Value) -> (u16, Value) {
        self.post("/worker/runs", body).await
    }

    async fn run(&self, run_id: &str) -> Value {
        self.ok_get(&format!("/worker/runs/{run_id}")).await
    }

    async fn wait_run(&self, run_id: &str, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let row = self.run(run_id).await;
            let status = row["status"].as_str().unwrap_or("");
            if is_terminal_run_status(status) {
                return row;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "run {run_id} stuck at {status}: {row}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// P1: one Policy write bumps the version by one; the old
/// version stays retrievable (v1 + v2, documents and
/// digests differ); the JSONL import is one-time (a second
/// startup does not re-import).
#[tokio::test]
async fn p1_policy_write_versions_and_onetime_import() {
    let mut h = Harness::serve_with_policy(vec![], POLICY_SEED).await;
    let versions = h.ok_get("/policies/versions").await;
    let rows = versions.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{versions}");
    assert_eq!(rows[0]["version"], 1);
    assert_eq!(rows[0]["version_id"], "policy-v1");
    assert_eq!(rows[0]["supersedes"], Value::Null);
    let v1_digest = rows[0]["digest"].as_str().unwrap().to_string();
    let v1_doc: Value =
        serde_json::from_str(rows[0]["document_json"].as_str().unwrap()).unwrap();
    assert_eq!(v1_doc["defaults"]["hitl_enabled"], false);

    h.ok_patch("/permissions/hitl", json!({"enabled": true})).await;
    let versions = h.ok_get("/policies/versions").await;
    let rows = versions.as_array().unwrap();
    assert_eq!(rows.len(), 2, "{versions}");
    assert_eq!(rows[1]["version"], 2);
    assert_eq!(rows[1]["version_id"], "policy-v2");
    assert_eq!(rows[1]["supersedes"], "policy-v1");
    assert_ne!(rows[1]["digest"], v1_digest);
    let v2_doc: Value =
        serde_json::from_str(rows[1]["document_json"].as_str().unwrap()).unwrap();
    assert_eq!(v2_doc["overrides"]["hitl_enabled"], true);
    assert_ne!(v2_doc, v1_doc);

    // Second startup: no re-import (still 2 rows, same digests).
    h.restart().await;
    let versions = h.ok_get("/policies/versions").await;
    let rows = versions.as_array().unwrap();
    assert_eq!(rows.len(), 2, "{versions}");
    assert_eq!(rows[0]["digest"], v1_digest);
    assert_eq!(rows[1]["supersedes"], "policy-v1");
    // The live policy is still the v2 document.
    let active = h.ok_get("/permissions/active").await;
    assert_eq!(active["hitl_enabled"], true);
}

/// P2: a Mandate pins its creation policy version — after
/// a policy change the old Execution's Mandate still names
/// the old version and a new Execution binds the new one;
/// the same Task changing goal supersedes its old Mandate
/// and creates a fresh active one.
#[tokio::test]
async fn p2_mandate_pins_policy_version() {
    let h = Harness::serve_with_policy(vec![done(), done()], POLICY_SEED).await;
    let versions = h.ok_get("/policies/versions").await;
    assert_eq!(versions.as_array().unwrap().len(), 1, "{versions}");

    let goal = h.ok("/goals/create", json!({"title": "Ship D1"})).await;
    assert_eq!(goal["goal_id"], "goal-1");
    let started = h
        .ok(
            "/agent/loop/start",
            json!({"prompt": "first", "goal_id": "goal-1"}),
        )
        .await;
    let loop1 = started["loop_id"].as_str().unwrap().to_string();
    let exec1 = h.execution_for_loop(&loop1).await;
    let exec1_id = exec1["execution_id"].as_str().unwrap().to_string();
    let mandates = h
        .ok_get(&format!("/mandates?execution_id={exec1_id}"))
        .await;
    let rows = mandates.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{mandates}");
    assert_eq!(rows[0]["policy_version_id"], "policy-v1");
    assert_eq!(rows[0]["status"], "active");
    assert_eq!(rows[0]["goal"], "Ship D1");
    let allowed: Value =
        serde_json::from_str(rows[0]["allowed_json"].as_str().unwrap()).unwrap();
    assert_eq!(allowed["entry"], "loop");
    assert!(allowed["tools"].as_array().unwrap().len() >= 11);

    // Policy change -> v2.
    h.ok_patch("/permissions/hitl", json!({"enabled": true})).await;
    let versions = h.ok_get("/policies/versions").await;
    assert_eq!(versions.as_array().unwrap().len(), 2, "{versions}");

    // New Execution binds v2; the old Mandate still names v1.
    let started = h
        .ok(
            "/agent/loop/start",
            json!({"prompt": "second", "goal_id": "goal-1"}),
        )
        .await;
    let loop2 = started["loop_id"].as_str().unwrap().to_string();
    let exec2 = h.execution_for_loop(&loop2).await;
    let exec2_id = exec2["execution_id"].as_str().unwrap().to_string();
    let mandates = h
        .ok_get(&format!("/mandates?execution_id={exec2_id}"))
        .await;
    let rows = mandates.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{mandates}");
    assert_eq!(rows[0]["policy_version_id"], "policy-v2");
    let old = h
        .ok_get(&format!("/mandates?execution_id={exec1_id}"))
        .await;
    assert_eq!(old.as_array().unwrap().len(), 1, "{old}");
    assert_eq!(old[0]["policy_version_id"], "policy-v1");

    // Same Task changing goal: old Mandate superseded, new one active.
    std::fs::create_dir_all(h.dir.join("files").join("proj")).unwrap();
    let task = h
        .ok(
            "/tasks/create",
            json!({"directory": "proj", "goal_id": "goal-1"}),
        )
        .await;
    let task_id = task["task_id"].as_str().unwrap().to_string();
    let goal2 = h.ok("/goals/create", json!({"title": "Second"})).await;
    assert_eq!(goal2["goal_id"], "goal-2");
    let mandates = h.ok_get(&format!("/mandates?task_id={task_id}")).await;
    let rows = mandates.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{mandates}");
    assert_eq!(rows[0]["policy_version_id"], "policy-v2");
    assert_eq!(rows[0]["status"], "active");
    let mandate_id = rows[0]["mandate_id"].as_str().unwrap().to_string();
    h.ok(
        "/tasks/update",
        json!({"task_id": task_id, "status": "created", "goal_id": "goal-2"}),
    )
    .await;
    let mandates = h.ok_get(&format!("/mandates?task_id={task_id}")).await;
    let rows = mandates.as_array().unwrap();
    assert_eq!(rows.len(), 2, "{mandates}");
    let old = rows
        .iter()
        .find(|m| m["mandate_id"] == mandate_id)
        .expect("old mandate kept");
    assert_eq!(old["status"], "superseded");
    let new = rows
        .iter()
        .find(|m| m["mandate_id"] != mandate_id)
        .expect("new mandate created");
    assert_eq!(new["status"], "active");
    assert_eq!(new["goal"], "Second");
    assert_eq!(new["policy_version_id"], "policy-v2");
}

/// P3: Approval version column — decide bumps the version
/// by one and the event payload carries `supersedes_version`;
/// an unbound approval's second decide 409s, same as bound
/// (re-approve stays the idempotent 200 no-op).
#[tokio::test]
async fn p3_approval_version_and_single_decision() {
    let h = Harness::serve(vec![]).await;
    // `reminders.set_completed` requires approval but takes
    // no digest: the request stays unbound.
    let (status, body) = h
        .post(
            "/approval/request",
            json!({"tool": "reminders.set_completed"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["requires_approval"], true);
    let id = body["tool_call_id"].as_str().unwrap().to_string();
    let view = h.approval(&id).await;
    assert_eq!(view["version"], 1);
    assert_eq!(view["status"], "pending");

    let (status, _) = h
        .post(
            "/approval/decide",
            json!({"tool_call_id": id, "status": "approved"}),
        )
        .await;
    assert_eq!(status, 200);
    let view = h.approval(&id).await;
    assert_eq!(view["version"], 2);
    assert_eq!(view["status"], "approved");

    // The decided event carries the supersede chain.
    let events = {
        let conn = h.db();
        sqlite_store::list_events(&conn).expect("list events")
    };
    let decided = events
        .iter()
        .find(|e| e.event_type == "approval.decided" && e.aggregate_id == id)
        .unwrap_or_else(|| panic!("no approval.decided event for {id}"));
    let payload: Value = serde_json::from_str(&decided.payload_json).unwrap();
    assert_eq!(payload["version"], 2);
    assert_eq!(payload["supersedes_version"], 1);

    // P3-http: the HTTP views carry the supersede chain
    // too, not just the stored event payload.
    let history = h.ok_get("/approvals/history").await;
    let decided_row = history
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["approval_id"] == id && r["status"] == "approved")
        .unwrap_or_else(|| panic!("no decided history row for {id}: {history}"));
    assert_eq!(decided_row["version"], 2);
    assert_eq!(decided_row["supersedes_version"], 1);
    let view = h.approval(&id).await;
    assert_eq!(view["version"], 2);
    assert_eq!(view["supersedes_version"], 1);

    // Re-approve is the idempotent 200 no-op (same as bound);
    // a different decision afterwards 409s.
    let (status, _) = h
        .post(
            "/approval/decide",
            json!({"tool_call_id": id, "status": "approved"}),
        )
        .await;
    assert_eq!(status, 200);
    let view = h.approval(&id).await;
    assert_eq!(view["version"], 2, "no-op decide writes nothing");
    let (status, body) = h
        .post(
            "/approval/decide",
            json!({"tool_call_id": id, "status": "rejected"}),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "loop_approval_already_decided_or_invalid");
}

/// P6 (FU-1, Q-R0-02): in SQLite mode `GET /approvals/history`
/// reads the `approvals` table — one row per approval, latest
/// state — identical to `GET /approvals`. The same approval is
/// driven through request → edit → decide → consume (device
/// set), which writes several event-log rows for it; the old
/// history read would have returned one row per event. The
/// event log itself keeps the superseded payloads, and
/// `fold_matches_tables` still holds.
#[tokio::test]
async fn p6_approvals_history_reads_table() {
    let h = Harness::serve(vec![]).await;
    // A device action on the seeded Fake reminder.
    let action = h
        .ok(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-demo", "target_completed": true}),
        )
        .await;
    let action_id = action["action_id"].as_str().unwrap().to_string();
    // `reminders.set_completed` requires approval but takes no
    // digest: the request stays unbound (and therefore editable).
    let (status, body) = h
        .post(
            "/approval/request",
            json!({"tool": "reminders.set_completed"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let id = body["tool_call_id"].as_str().unwrap().to_string();
    let view = h.approval(&id).await;
    assert_eq!(view["version"], 1);
    assert_eq!(view["status"], "pending");

    // Edit: version 1 -> 2, status Edited. (The edit response
    // itself is built from the in-memory record, whose version
    // fields are null by design — read the stored row instead.)
    let (status, _) = h
        .post(
            &format!("/approvals/{id}/edit-message"),
            json!({"message": "revised draft"}),
        )
        .await;
    assert_eq!(status, 200);
    let edited = h.approval(&id).await;
    assert_eq!(edited["status"], "edited");
    assert_eq!(edited["message"], "revised draft");
    assert_eq!(edited["version"], 2);
    assert_eq!(edited["supersedes_version"], 1);

    // Decide approve: version 2 -> 3, supersedes 2.
    let (status, _) = h
        .post(
            "/approval/decide",
            json!({"tool_call_id": id, "status": "approved"}),
        )
        .await;
    assert_eq!(status, 200);
    let view = h.approval(&id).await;
    assert_eq!(view["version"], 3);
    assert_eq!(view["supersedes_version"], 2);

    // Consume via the device set.
    h.ok(
        "/devices/reminders/set",
        json!({"action_id": action_id, "tool_call_id": id}),
    )
    .await;

    // History is now the table, not the event log: exactly the
    // same rows as the list, latest state only.
    let history = h.ok_get("/approvals/history").await;
    let list = h.ok_get("/approvals").await;
    assert_eq!(history, list, "history must equal list");
    let rows = history.as_array().unwrap();
    assert_eq!(rows.len(), 1, "{history}");
    let row = &rows[0];
    assert_eq!(row["approval_id"], id);
    assert_eq!(row["status"], "approved");
    assert_eq!(row["message"], "revised draft");
    assert_eq!(row["version"], 3);
    assert_eq!(row["supersedes_version"], 2);

    // The event log still holds the superseded payloads: at
    // least the v1 requested snapshot survives under the v3 row.
    let events = {
        let conn = h.db();
        sqlite_store::list_events(&conn).expect("list events")
    };
    let mine: Vec<_> = events
        .iter()
        .filter(|e| e.aggregate_kind == "approval" && e.aggregate_id == id)
        .collect();
    assert!(mine.len() >= 3, "expected several events for {id}, got {}", mine.len());
    let has_v1 = mine.iter().any(|e| {
        serde_json::from_str::<Value>(&e.payload_json)
            .map(|p| p["version"] == 1)
            .unwrap_or(false)
    });
    assert!(has_v1, "superseded v1 payload must remain in the event log");
    let conn = h.db();
    assert!(
        sqlite_store::fold_matches_tables(&conn).expect("fold"),
        "fold must still hold after the history read change"
    );
}

/// P4: the `consumed_by_worker_session_id` backfill emits
/// an `approval.bound` event (findable in the event
/// stream) whose payload names the consuming session.
#[tokio::test]
async fn p4_consumed_by_backfill_emits_bound_event() {
    let dir = temp_dir("p4");
    let repo = init_repo(&dir);
    let h =
        Harness::serve_with_worker(vec![], Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    let approval = h
        .approve_worker_run(&worktree, "git-status", json!({}), None)
        .await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-status",
            "params": {},
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    let run_id = run["run_id"].as_str().unwrap().to_string();
    let row = h.wait_run(&run_id, 30).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    let worker_session_id = row["worker_session_id"].as_str().unwrap().to_string();
    assert!(!worker_session_id.is_empty());

    let events = {
        let conn = h.db();
        sqlite_store::list_events(&conn).expect("list events")
    };
    let bound = events
        .iter()
        .find(|e| e.event_type == "approval.bound" && e.aggregate_id == approval)
        .unwrap_or_else(|| {
            panic!(
                "no approval.bound event for {approval}: {:?}",
                events
                    .iter()
                    .map(|e| (e.event_type.clone(), e.aggregate_id.clone()))
                    .collect::<Vec<_>>()
            )
        });
    let payload: Value = serde_json::from_str(&bound.payload_json).unwrap();
    assert_eq!(payload["tool_call_id"], approval);
    assert_eq!(
        payload["consumed_by_worker_session_id"], worker_session_id,
        "{payload}"
    );
    let view = h.approval(&approval).await;
    assert_eq!(view["consumed_by_worker_session_id"], worker_session_id);
}

/// P5: restart recovery — after restart (with the JSONL
/// policy file deleted, so nothing can be re-read from
/// it) the current Policy version, all historical versions
/// and the Mandates recover from SQLite, value-identical
/// to before the restart.
#[tokio::test]
async fn p5_restart_recovery_from_sqlite() {
    let mut h = Harness::serve_with_policy(vec![done()], POLICY_SEED).await;
    h.ok_patch("/permissions/hitl", json!({"enabled": true})).await;
    let versions_before = h.ok_get("/policies/versions").await;
    assert_eq!(versions_before.as_array().unwrap().len(), 2, "{versions_before}");

    let goal = h.ok("/goals/create", json!({"title": "Ship D1"})).await;
    assert_eq!(goal["goal_id"], "goal-1");
    let started = h
        .ok(
            "/agent/loop/start",
            json!({"prompt": "p5", "goal_id": "goal-1"}),
        )
        .await;
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let exec = h.execution_for_loop(&loop_id).await;
    let exec_id = exec["execution_id"].as_str().unwrap().to_string();
    let mandates_before = h.ok_get("/mandates").await;
    assert_eq!(mandates_before.as_array().unwrap().len(), 1, "{mandates_before}");
    let active_before = h.ok_get("/permissions/active").await;
    assert_eq!(active_before["hitl_enabled"], true);

    // Delete the JSONL file: recovery must not read it.
    std::fs::remove_file(h.dir.join("policy.jsonl")).unwrap();
    h.restart().await;

    let versions_after = h.ok_get("/policies/versions").await;
    assert_eq!(versions_after, versions_before, "versions recover value-identical");
    let mandates_after = h.ok_get("/mandates").await;
    assert_eq!(mandates_after, mandates_before, "mandates recover value-identical");
    let active_after = h.ok_get("/permissions/active").await;
    assert_eq!(active_after, active_before, "live policy recovers value-identical");
    let m = h
        .ok_get(&format!("/mandates?execution_id={exec_id}"))
        .await;
    assert_eq!(m.as_array().unwrap().len(), 1, "{m}");
    assert_eq!(m[0]["policy_version_id"], "policy-v2");
}

/// P5b: restart recovery with the JSONL policy file KEPT
/// (stale default doc, hitl false) — the live policy must
/// still recover from SQLite, value-identical, with hitl
/// left true (differs from the JSONL default) so a JSONL
/// overwrite on restart would be caught. P5 deletes the
/// file; this is the discriminating case for the
/// production startup order (JSONL before SQLite).
#[tokio::test]
async fn p5b_restart_recovery_with_jsonl_present() {
    let mut h = Harness::serve_with_policy(vec![done()], POLICY_SEED).await;
    h.ok_patch("/permissions/hitl", json!({"enabled": true})).await;
    let versions_before = h.ok_get("/policies/versions").await;
    assert_eq!(versions_before.as_array().unwrap().len(), 2, "{versions_before}");

    let goal = h.ok("/goals/create", json!({"title": "Ship D1"})).await;
    assert_eq!(goal["goal_id"], "goal-1");
    let started = h
        .ok(
            "/agent/loop/start",
            json!({"prompt": "p5b", "goal_id": "goal-1"}),
        )
        .await;
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let exec = h.execution_for_loop(&loop_id).await;
    let exec_id = exec["execution_id"].as_str().unwrap().to_string();
    let mandates_before = h.ok_get("/mandates").await;
    assert_eq!(mandates_before.as_array().unwrap().len(), 1, "{mandates_before}");
    let active_before = h.ok_get("/permissions/active").await;
    assert_eq!(active_before["hitl_enabled"], true);

    // Do NOT delete the JSONL file: the stale default doc
    // (hitl false) must not overwrite the SQLite recovery.
    assert!(h.dir.join("policy.jsonl").exists());
    h.restart().await;

    let versions_after = h.ok_get("/policies/versions").await;
    assert_eq!(versions_after, versions_before, "versions recover value-identical");
    let mandates_after = h.ok_get("/mandates").await;
    assert_eq!(mandates_after, mandates_before, "mandates recover value-identical");
    let active_after = h.ok_get("/permissions/active").await;
    assert_eq!(active_after, active_before, "live policy recovers value-identical");
    assert_eq!(
        active_after["hitl_enabled"], true,
        "JSONL default must not win on restart"
    );
    let m = h
        .ok_get(&format!("/mandates?execution_id={exec_id}"))
        .await;
    assert_eq!(m.as_array().unwrap().len(), 1, "{m}");
    assert_eq!(m[0]["policy_version_id"], "policy-v2");
}
