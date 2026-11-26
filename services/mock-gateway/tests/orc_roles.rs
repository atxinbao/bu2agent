//! ORC-2 tests (plan §4 + Q-ORC-07/08/09/10/11):
//! the Planner/Executor dual-model flow — model dual
//! slots (stage/fallback/restart-effective), the
//! Planner round + fast triage (no Execution, Evidence
//! `no_file_change`), forced execution bypassing the
//! triage judgment, the Executor's single file-write
//! step (one digest-bound Effect), and a single step
//! failure stopping the whole batch.
//!
//! A multi-shot fake DeepSeek server stands in for the
//! real API: each queued reply is one model call's
//! completion, and every request's `model` field is
//! recorded so the tests can assert the Planner round
//! used the planner slot and the Executor round used
//! the executor slot. The fake key is `test-key`; no
//! real credential is used anywhere.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::plan_store::get_plan;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-orc2-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Multi-shot fake DeepSeek: serves the queued
/// completion contents in order (one per request) and
/// records every request body it saw. An empty queue
/// answers 500 — a test that under-queues fails loudly
/// instead of hanging on a model call that never
/// returns.
struct FakeDeepseek {
    #[allow(dead_code)]
    handle: tokio::task::JoinHandle<()>,
    seen: Arc<Mutex<Vec<Value>>>,
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

async fn start_fake_deepseek(replies: Vec<String>) -> (String, FakeDeepseek) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let queue = Arc::new(Mutex::new(VecDeque::from(replies)));
    let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let queue = queue.clone();
            let seen = seen_clone.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let mut header_end = None;
                let mut content_length = 0usize;
                while header_end.is_none() {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                        header_end = Some(pos + 4);
                        let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
                        for line in head.lines().skip(1) {
                            if let Some((name, value)) = line.split_once(':') {
                                if name.trim().eq_ignore_ascii_case("content-length") {
                                    content_length = value.trim().parse().unwrap_or(0);
                                }
                            }
                        }
                    }
                }
                let end = header_end.expect("header end") + content_length;
                while buf.len() < end {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let body_text =
                    String::from_utf8_lossy(&buf[header_end.expect("header end")..end])
                        .into_owned();
                if let Ok(body) = serde_json::from_str::<Value>(&body_text) {
                    seen.lock().expect("seen lock").push(body);
                }
                let next = queue.lock().expect("queue lock").pop_front();
                let (status_line, body) = match next {
                    Some(content) => (
                        "200 OK",
                        serde_json::to_string(&json!({
                            "id": "chatcmpl-test",
                            "object": "chat.completion",
                            "created": 1,
                            "model": "fake",
                            "choices": [{
                                "index": 0,
                                "message": {"role": "assistant", "content": content},
                                "finish_reason": "stop",
                            }],
                        }))
                        .unwrap(),
                    ),
                    None => (
                        "500 Internal Server Error",
                        "{\"error\": \"fake deepseek queue exhausted\"}".to_string(),
                    ),
                };
                let response = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    (
        format!("http://{addr}"),
        FakeDeepseek { handle, seen },
    )
}

impl FakeDeepseek {
    /// Wait until the fake has seen `count` requests
    /// (the runner is async — a run returns before the
    /// Planner round fires).
    async fn wait_seen(&self, count: usize, secs: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            if self.seen.lock().expect("seen lock").len() >= count {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "fake saw fewer than {count} requests"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The `model` field of every request, in order.
    fn seen_models(&self) -> Vec<String> {
        self.seen
            .lock()
            .expect("seen lock")
            .iter()
            .filter_map(|b| b["model"].as_str().map(str::to_string))
            .collect()
    }

    /// The full prompt text of the Nth request (the last
    /// user message's content).
    fn prompt_of(&self, index: usize) -> String {
        let seen = self.seen.lock().expect("seen lock");
        let body = &seen[index];
        body["messages"]
            .as_array()
            .and_then(|m| m.last())
            .and_then(|m| m["content"].as_str())
            .unwrap_or("")
            .to_string()
    }
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
    deepseek_base_url: String,
}

impl Harness {
    async fn serve(deepseek_base_url: &str) -> Self {
        let dir = temp_dir("roles");
        std::fs::create_dir_all(dir.join("files").join("proj")).unwrap();
        std::fs::write(dir.join("files").join("proj").join("notes.txt"), "hello\n").unwrap();
        let state = build_state(&dir, deepseek_base_url);
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
            deepseek_base_url: deepseek_base_url.to_string(),
        }
    }

    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let state = build_state(&self.dir, &self.deepseek_base_url);
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

    fn executions_for_task(&self, task_id: &str) -> Vec<String> {
        let conn = self.db();
        let mut stmt = conn
            .prepare("SELECT execution_id FROM executions WHERE task_id = ?1")
            .unwrap();
        stmt.query_map([task_id], |row| row.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    }

    fn plan_count(&self, task_id: &str) -> i64 {
        let conn = self.db();
        conn.query_row(
            "SELECT COUNT(*) FROM plans WHERE task_id = ?1",
            [task_id],
            |row| row.get(0),
        )
        .unwrap()
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
        let response = self.client.get(format!("{}{}", self.url, path)).send().await.unwrap();
        let status = response.status();
        let value: Value = response.json().await.unwrap_or(Value::Null);
        (status, value)
    }

    async fn make_task(&self) -> String {
        let (status, value) = self.post("/tasks/create", json!({"directory": "proj"})).await;
        assert_eq!(status, StatusCode::OK, "create task: {value}");
        value["task_id"].as_str().unwrap().to_string()
    }

    async fn task_record(&self, task_id: &str) -> Value {
        let (status, value) = self.get(&format!("/tasks/get?task_id={task_id}")).await;
        assert_eq!(status, StatusCode::OK, "get task: {value}");
        value
    }

    /// Poll until the task reaches `want` (or the
    /// deadline passes, failing with the last record).
    async fn wait_task_status(&self, task_id: &str, want: &str, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let record = self.task_record(task_id).await;
            if record["status"] == want {
                return record;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "task {task_id} stuck at {} (want {want}): {record}",
                record["status"]
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn approve_current(&self, task_id: &str) -> String {
        let record = self.task_record(task_id).await;
        let approval_id = record["approval_id"]
            .as_str()
            .expect("task has an approval to decide")
            .to_string();
        let (status, value) = self
            .post(
                "/approval/decide",
                json!({"tool_call_id": approval_id, "status": "approved"}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "decide {approval_id}: {value}");
        approval_id
    }

    async fn effects(&self) -> Vec<Value> {
        let (status, value) = self.get("/effects/list").await;
        assert_eq!(status, StatusCode::OK, "effects list: {value}");
        value.as_array().unwrap().clone()
    }

    async fn evidence(&self) -> Vec<Value> {
        let (status, value) = self.get("/evidence/list").await;
        assert_eq!(status, StatusCode::OK, "evidence list: {value}");
        value.as_array().unwrap().clone()
    }

    async fn open_inbox(&self, kind: &str) -> Vec<Value> {
        let (status, value) = self
            .get(&format!("/inbox/list?kind={kind}&open_only=true"))
            .await;
        assert_eq!(status, StatusCode::OK, "inbox list: {value}");
        value.as_array().unwrap().clone()
    }

    /// Binding + Location + Worktree for `repo`; returns
    /// the worktree id (p3c_worker pattern).
    async fn setup_worktree(&self, repo: &Path) -> String {
        let repo_str = repo.to_string_lossy().into_owned();
        let (status, binding) = self
            .post(
                "/bindings/create",
                json!({"display_name": "repo", "path": repo_str}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{binding}");
        let workspace_id = binding["workspace_id"].as_str().unwrap().to_string();
        let (status, location) = self
            .post(
                "/locations/create",
                json!({"workspace_id": workspace_id, "name": "root", "path": repo_str}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{location}");
        let location_id = location["location_id"].as_str().unwrap().to_string();
        let (status, worktree) = self
            .post(
                "/worktrees/create",
                json!({"location_id": location_id, "directory_relative": "", "branch_label": "main", "project_id": null}),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{worktree}");
        worktree["worktree_id"].as_str().unwrap().to_string()
    }
}

fn git(args: &[&str], cwd: &Path) {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {} failed in {}: {}",
        args.join(" "),
        cwd.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&["init", "-b", "main"], &repo);
    git(&["config", "user.email", "test@example.invalid"], &repo);
    git(&["config", "user.name", "orc2-test"], &repo);
    std::fs::write(repo.join("README.md"), "# test repo\n").unwrap();
    git(&["add", "."], &repo);
    git(&["commit", "-m", "init"], &repo);
    repo
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_millis() as i64
}

/// A real git repo that is also a Cargo workspace with one
/// tiny package whose single test sleeps: the cargo-test
/// run stays `running` for seconds, so the test can
/// intervene on its row while it runs.
fn init_repo_with_slow_cargo_package(dir: &Path) -> PathBuf {
    let repo = init_repo(dir);
    std::fs::write(
        repo.join("Cargo.toml"),
        "[workspace]\nresolver = \"2\"\nmembers = [\"testpkg-pass\"]\n",
    )
    .unwrap();
    let pkg = repo.join("testpkg-pass");
    std::fs::create_dir_all(pkg.join("src")).unwrap();
    std::fs::write(
        pkg.join("Cargo.toml"),
        "[package]\nname = \"testpkg-pass\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[lib]\npath = \"src/lib.rs\"\n",
    )
    .unwrap();
    std::fs::write(
        pkg.join("src").join("lib.rs"),
        "#[cfg(test)]\nmod tests {\n    #[test]\n    fn passes() {\n        std::thread::sleep(std::time::Duration::from_secs(5));\n        assert_eq!(1 + 1, 2);\n    }\n}\n",
    )
    .unwrap();
    git(&["add", "-A"], &repo);
    git(&["commit", "-m", "add cargo package"], &repo);
    repo
}

fn planner_triage_reply(answer: &str) -> String {
    serde_json::to_string(&json!({
        "needs_file_change": false,
        "answer": answer,
        "steps": [],
        "replan_if": [],
    }))
    .unwrap()
}

fn planner_steps_reply(steps: Vec<Value>) -> String {
    serde_json::to_string(&json!({
        "needs_file_change": true,
        "steps": steps,
        "replan_if": [],
    }))
    .unwrap()
}

fn file_write_step(step_no: i64, path: &str, verify_cmd: &str) -> Value {
    json!({
        "step_no": step_no,
        "kind": "file_write",
        "path": path,
        "symbol": "report",
        "change": "write the report",
        "verify_cmd": verify_cmd,
        "expected": "file exists",
    })
}

fn verify_step(step_no: i64, verify_cmd: &str) -> Value {
    json!({
        "step_no": step_no,
        "kind": "verify",
        "path": "proj/notes.txt",
        "symbol": "notes",
        "change": "verify the repo",
        "verify_cmd": verify_cmd,
        "expected": "command succeeds",
    })
}

fn observed_of(row: &Value) -> Value {
    match &row["observed_json"] {
        Value::String(s) => serde_json::from_str(s).unwrap(),
        other => other.clone(),
    }
}

/// R1 (Q-ORC-08/09): dual slots stage, fall back to
/// the running model, and survive a restart. No model
/// allowlist, no effort/temperature — the POST surface
/// takes only the slot names.
#[tokio::test]
async fn r1_dual_slots_stage_fallback_restart() {
    assert_eq!(DB_SCHEMA_VERSION, 21, "ORC-5 bumps the schema to v21 (ORC-2 adds no migration)");
    let mut h = Harness::serve("http://127.0.0.1:1").await;
    assert_eq!(h.schema_version(), 21);
    // Defaults: both slots effective = the running
    // model; nothing staged.
    let (status, view) = h.get("/model").await;
    assert_eq!(status, StatusCode::OK, "get model: {view}");
    assert_eq!(view["model"], "fixture");
    assert_eq!(view["planner_model"], "fixture");
    assert_eq!(view["executor_model"], "fixture");
    assert_eq!(view["staged_planner_model"], Value::Null);
    assert_eq!(view["staged_executor_model"], Value::Null);
    assert_eq!(view["restart_needed"], false);
    // Stage only the planner slot: the executor keeps
    // falling back to the running model, and no
    // restart is needed for a slot (base-model-only).
    let (status, view) = h
        .post("/model", json!({"planner_model": "planner-fixture"}))
        .await;
    assert_eq!(status, StatusCode::OK, "stage planner: {view}");
    assert_eq!(view["staged_planner_model"], "planner-fixture");
    assert_eq!(view["planner_model"], "planner-fixture");
    assert_eq!(view["executor_model"], "fixture");
    assert_eq!(view["restart_needed"], false);
    // Stage the executor slot too.
    let (status, view) = h
        .post("/model", json!({"executor_model": "executor-fixture"}))
        .await;
    assert_eq!(status, StatusCode::OK, "stage executor: {view}");
    assert_eq!(view["staged_executor_model"], "executor-fixture");
    assert_eq!(view["executor_model"], "executor-fixture");
    assert_eq!(view["planner_model"], "planner-fixture");
    // An empty slot name is a 400, like the base model.
    let (status, _) = h.post("/model", json!({"planner_model": ""})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = h.post("/model", json!({"executor_model": ""})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Restart: the staged slots persist (restart-
    // effective) and the effective values are unchanged.
    h.restart().await;
    let (status, view) = h.get("/model").await;
    assert_eq!(status, StatusCode::OK, "get model after restart: {view}");
    assert_eq!(view["staged_planner_model"], "planner-fixture");
    assert_eq!(view["staged_executor_model"], "executor-fixture");
    assert_eq!(view["planner_model"], "planner-fixture");
    assert_eq!(view["executor_model"], "executor-fixture");
    assert_eq!(view["restart_needed"], false);
}

/// R2 (Q-ORC-07): the Planner judges "no file
/// change" → direct answer, no Execution, no plan
/// row, one Evidence row (`no_file_change`), and the
/// task completes on the two legal edges.
#[tokio::test]
async fn r2_triage_no_change_no_execution() {
    let (fake_url, fake) = start_fake_deepseek(vec![planner_triage_reply(
        "The answer is 42.",
    )])
    .await;
    let h = Harness::serve(&fake_url).await;
    let task_id = h.make_task().await;
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    let record = h.wait_task_status(&task_id, "completed", 30).await;
    assert_eq!(record["status"], "completed");
    assert_eq!(record["artifact_path"], Value::Null, "no file was written");
    assert_eq!(record["approval_id"], Value::Null, "no write approval exists");
    // The answer is the draft.
    let (status, draft) = h.get(&format!("/tasks/draft?task_id={task_id}")).await;
    assert_eq!(status, StatusCode::OK, "draft: {draft}");
    assert_eq!(draft["draft"], "The answer is 42.");
    // No Execution exists for this task (a triage run
    // never reaches begin_execution).
    assert!(
        h.executions_for_task(&task_id).is_empty(),
        "triage creates no Execution"
    );
    // No plan row was created (J13).
    assert_eq!(h.plan_count(&task_id), 0);
    assert_eq!(h.task_record(&task_id).await["plan_id"], Value::Null);
    // One Evidence row records the judgment — answer
    // hash + length only, never the answer text.
    let evidence = h.evidence().await;
    let triage: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "no_file_change")
        .collect();
    assert_eq!(triage.len(), 1, "evidence: {evidence:?}");
    assert_eq!(triage[0]["effect_id"], format!("triage:{task_id}"));
    assert_eq!(triage[0]["source"], "planner");
    assert_eq!(triage[0]["verdict"], "verified");
    let observed = observed_of(triage[0]);
    assert_eq!(observed["task_id"], task_id);
    assert_eq!(observed["answer_len"], 17);
    assert!(
        observed.get("answer").is_none(),
        "the answer text never enters Evidence: {observed}"
    );
    // The Planner round ran on the planner slot.
    assert_eq!(fake.seen_models(), vec!["fixture"]);
    // No failed Inbox row.
    assert!(h.open_inbox("failed").await.is_empty());
}

/// R3 (Q-ORC-07, J14): `force_execute: true`
/// bypasses the triage judgment — the Planner still
/// runs (and is told to produce steps), the Executor
/// runs the plan, and no triage Evidence is recorded.
#[tokio::test]
async fn r3_force_execute_bypasses_triage() {
    let (fake_url, fake) = start_fake_deepseek(vec![
        // The Planner judges no change AND returns a
        // step (the force note asks for one): the run
        // proceeds with the step instead of triaging.
        serde_json::to_string(&json!({
            "needs_file_change": false,
            "answer": "No change needed.",
            "steps": [file_write_step(1, "proj/report.md", "git status")],
            "replan_if": [],
        }))
        .unwrap(),
        // The Executor's file content for step 1.
        "FORCED REPORT\n".to_string(),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    // Stage distinct role models so the routing is
    // observable in the fake's request log.
    let (status, _) = h
        .post(
            "/model",
            json!({"planner_model": "planner-fixture", "executor_model": "executor-fixture"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let task_id = h.make_task().await;
    let (status, value) = h
        .post("/tasks/run", json!({"task_id": task_id, "force_execute": true}))
        .await;
    assert_eq!(status, StatusCode::OK, "forced run: {value}");
    // The forced Planner prompt carries the force note.
    fake.wait_seen(1, 30).await;
    assert!(
        fake.prompt_of(0).contains("强制执行"),
        "the Planner prompt must tell the model the run is forced"
    );
    // The Planner ran on the planner slot (the first
    // call; the Executor call for step 1 follows
    // immediately in prepare, so only the first
    // element is asserted here — the full order is
    // asserted at the end).
    assert_eq!(
        fake.seen_models().first().map(String::as_str),
        Some("planner-fixture")
    );
    // Step 1 prepares: draft_ready with a bound approval.
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    assert!(record["approval_id"].is_string(), "step 1 has an approval");
    // A plan row and an Execution exist (the forced run
    // is an execution run, not a triage).
    assert_eq!(h.plan_count(&task_id), 1);
    assert_eq!(h.executions_for_task(&task_id).len(), 1);
    // No triage Evidence (J14: the judgment was
    // overridden, the plan + step Effect are the trail).
    let evidence = h.evidence().await;
    assert!(
        evidence.iter().all(|e| e["reason"] != "no_file_change"),
        "a forced run records no triage Evidence: {evidence:?}"
    );
    // Approve + execute step 1 (the only step): the
    // task completes and the file holds the Executor's
    // content.
    h.approve_current(&task_id).await;
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute: {value}");
    let record = h.wait_task_status(&task_id, "completed", 30).await;
    assert_eq!(record["status"], "completed");
    let written = std::fs::read_to_string(h.dir.join("files").join("proj").join("report.md"))
        .expect("step file exists");
    assert_eq!(written, "FORCED REPORT\n");
    assert_eq!(fake.seen_models(), vec!["planner-fixture", "executor-fixture"]);
}

/// R4 (Q-ORC-10): the Executor's single file-write
/// step produces exactly one digest-bound Effect, and
/// the task completes with the file written.
#[tokio::test]
async fn r4_executor_single_step_effect() {
    let (fake_url, fake) = start_fake_deepseek(vec![
        planner_steps_reply(vec![file_write_step(1, "proj/report.md", "git status")]),
        "EXECUTED REPORT\n".to_string(),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let (status, _) = h
        .post(
            "/model",
            json!({"planner_model": "planner-fixture", "executor_model": "executor-fixture"}),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let task_id = h.make_task().await;
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    assert_eq!(record["artifact_path"], "proj/report.md");
    let approval_id = record["approval_id"].as_str().unwrap().to_string();
    // The plan row exists, written by the Planner round
    // (not by an HTTP draft call).
    let plan_id = h.task_record(&task_id).await["plan_id"]
        .as_str()
        .expect("task points at its plan")
        .to_string();
    let conn = h.db();
    let row = get_plan(&conn, &plan_id).expect("get plan").expect("plan row");
    assert_eq!(row.task_id, task_id);
    assert_eq!(row.planner_model, "planner-fixture");
    // Approve + execute: the file is written, the
    // task completes.
    h.approve_current(&task_id).await;
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute: {value}");
    h.wait_task_status(&task_id, "completed", 30).await;
    let written = std::fs::read_to_string(h.dir.join("files").join("proj").join("report.md"))
        .expect("step file exists");
    assert_eq!(written, "EXECUTED REPORT\n");
    // Exactly one Effect for this task's execution:
    // the tasks.execute write, succeeded, bound to the
    // approval the step prepared.
    let effects = h.effects().await;
    let mine: Vec<&Value> = effects
        .iter()
        .filter(|e| e["scope"] == task_id && e["operation"] == "execute")
        .collect();
    assert_eq!(mine.len(), 1, "effects: {effects:?}");
    assert_eq!(mine[0]["capability"], "tasks");
    assert_eq!(mine[0]["status"], "succeeded");
    assert_eq!(mine[0]["approval_id"], approval_id);
    // The write's write-time Evidence row (reason
    // `written`, source `write`) is the step's audit
    // trail — the hash_match reason belongs to the
    // separate /effects/verify path, not this hook.
    let evidence = h.evidence().await;
    let verify: Vec<&Value> = evidence
        .iter()
        .filter(|e| {
            e["effect_id"] == mine[0]["effect_id"]
                && e["reason"] == "written"
                && e["source"] == "write"
        })
        .collect();
    assert_eq!(verify.len(), 1, "evidence: {evidence:?}");
    assert_eq!(fake.seen_models(), vec!["planner-fixture", "executor-fixture"]);
}

/// R5 (Q-ORC-11): a single step failure stops the
/// whole batch — step 1's file stays written, step
/// 2's worker run fails, the task fails, one
/// `step_failed` Evidence row is recorded, no Inbox
/// row opens, and nothing is re-planned.
#[tokio::test]
async fn r5_single_step_failure_stops_batch() {
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(vec![
            file_write_step(1, "proj/report.md", "git status"),
            verify_step(2, "cargo test -p does-not-exist"),
        ]),
        "STEP ONE REPORT\n".to_string(),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let task_id = h.make_task().await;
    let (status, value) = h
        .post("/tasks/run", json!({"task_id": task_id, "worktree_id": worktree_id}))
        .await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    // Step 1: approve + execute the file write.
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    h.approve_current(&task_id).await;
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute step 1: {value}");
    // Step 2 prepares (a bound worker.run approval for
    // the cargo-test step).
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 2);
    assert!(record["approval_id"].is_string(), "step 2 has an approval");
    // Approve + execute step 2: the worker run fails
    // (no such package) and the batch stops.
    h.approve_current(&task_id).await;
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute step 2 accepted: {value}");
    let record = h.wait_task_status(&task_id, "failed", 90).await;
    assert_eq!(record["status"], "failed");
    assert!(
        record["error"].as_str().unwrap_or("").contains("步骤 2"),
        "the failure names the failed step: {record}"
    );
    // Step 1's file stays written (the batch stopped
    // after it, it was not rolled back).
    let written = std::fs::read_to_string(h.dir.join("files").join("proj").join("report.md"))
        .expect("step file exists");
    assert_eq!(written, "STEP ONE REPORT\n");
    // One `step_failed` Evidence row, with the run's
    // terminal status.
    let evidence = h.evidence().await;
    let failed: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "step_failed")
        .collect();
    assert_eq!(failed.len(), 1, "evidence: {evidence:?}");
    assert_eq!(failed[0]["source"], "executor");
    let observed = observed_of(failed[0]);
    assert_eq!(observed["step_no"], 2);
    assert_eq!(observed["failure"], "worker_run_failed");
    assert_eq!(observed["run_status"], "failed");
    assert!(observed["run_id"].is_string(), "the run is named: {observed}");
    // No Inbox row (Q-ORC-11: no skip, no guess, no
    // Inbox row, no re-planning) and no new plan.
    assert!(h.open_inbox("failed").await.is_empty());
    assert_eq!(h.plan_count(&task_id), 1);
}

/// R6 (W4-1, Q-FW-09): a worker step with no frozen
/// binding fails honestly at the prepare gate — the
/// task fails, the error tells the user to pick a
/// worktree in the task detail, exactly one
/// `step_failed` Evidence row is recorded, and no
/// Inbox row opens. (This is the ORC-2 window W4
/// failure, which no test covered: r5 always passed
/// a `worktree_id`.)
#[tokio::test]
async fn r6_worker_step_without_binding_fails_honestly() {
    let (fake_url, _fake) = start_fake_deepseek(vec![planner_steps_reply(vec![
        verify_step(1, "git status"),
    ])])
    .await;
    let h = Harness::serve(&fake_url).await;
    let task_id = h.make_task().await;
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    let record = h.wait_task_status(&task_id, "failed", 60).await;
    let error = record["error"].as_str().unwrap_or("");
    assert!(
        error.contains("未绑定工作区"),
        "the failure names the missing binding: {record}"
    );
    assert!(
        error.contains("选择工作区"),
        "the failure tells the user how to fix it: {record}"
    );
    assert_eq!(h.plan_count(&task_id), 1);
    // Exactly one `step_failed` Evidence row, from the
    // prepare gate (no worker run was ever created).
    let evidence = h.evidence().await;
    let failed: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "step_failed")
        .collect();
    assert_eq!(failed.len(), 1, "evidence: {evidence:?}");
    assert_eq!(failed[0]["source"], "executor");
    let observed = observed_of(failed[0]);
    assert_eq!(observed["step_no"], 1);
    assert_eq!(observed["step_kind"], "verify");
    assert!(
        observed["failure"].as_str().unwrap_or("").contains("未绑定工作区"),
        "observed failure: {observed}"
    );
    assert_eq!(observed["run_id"], Value::Null);
    // The failed Execution close opens one `failed`
    // Inbox row for review (E2-A §2.3) — unlike r5,
    // no worker run ever existed, so the §5.4 test-run
    // carve-out (`close_execution_no_inbox`) does not
    // apply. The row names the Execution, no Effect.
    let inbox = h.open_inbox("failed").await;
    assert_eq!(inbox.len(), 1, "inbox: {inbox:?}");
    assert_eq!(inbox[0]["execution_id"], "execution-1");
    assert_eq!(inbox[0]["effect_id"], Value::Null);
}

/// R7 (W4-1, Q-FW-01): the same worker step with a
/// frozen binding runs — the read-only verify step
/// (`git status`, no approval) executes inside the
/// worktree and the task completes.
#[tokio::test]
async fn r7_worker_step_with_binding_succeeds() {
    let (fake_url, _fake) = start_fake_deepseek(vec![planner_steps_reply(vec![
        verify_step(1, "git status"),
    ])])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let task_id = h.make_task().await;
    let (status, value) = h
        .post("/tasks/run", json!({"task_id": task_id, "worktree_id": worktree_id}))
        .await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    // The read-only verify step stops at draft_ready
    // with no approval; `tasks_execute` runs it.
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    assert_eq!(record["approval_id"], Value::Null);
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute: {value}");
    let record = h.wait_task_status(&task_id, "completed", 90).await;
    assert_eq!(record["status"], "completed");
    assert_eq!(h.plan_count(&task_id), 1);
    let evidence = h.evidence().await;
    assert!(
        evidence.iter().all(|e| e["reason"] != "step_failed"),
        "no step failure: {evidence:?}"
    );
}

/// R5-F2 (Q-R5F2-03): when a worker step's run fails
/// with a `budget_exceeded` tail, the task error carries
/// the cause verbatim and the `step_failed` Evidence row
/// carries `failure_tail`. The refusal itself cannot be
/// produced through the task path after the fix (a new
/// run's creation re-anchors the deadline), so the test
/// fails the run row the way the driver would on a
/// refusal — while cargo is still running, so the
/// driver's own finish cannot land first.
#[tokio::test]
async fn r5f2_step_failure_budget_exceeded_tail_surfaces() {
    let (fake_url, _fake) = start_fake_deepseek(vec![planner_steps_reply(vec![
        verify_step(1, "cargo test -p testpkg-pass"),
    ])])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo_with_slow_cargo_package(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let task_id = h.make_task().await;
    let (status, value) = h
        .post("/tasks/run", json!({"task_id": task_id, "worktree_id": worktree_id}))
        .await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    assert!(record["approval_id"].is_string(), "step has an approval");
    h.approve_current(&task_id).await;
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute: {value}");
    // Fail the step's run the way the driver would on a
    // deadline refusal, while cargo is still running.
    let execution_id = h
        .executions_for_task(&task_id)
        .into_iter()
        .next()
        .expect("task execution");
    let run_id = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let found = {
                let conn = h.db();
                conn.query_row(
                    "SELECT run_id, status FROM worker_runs WHERE execution_id = ?1 ORDER BY seq DESC LIMIT 1",
                    [&execution_id],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .ok()
            };
            if let Some((run_id, status)) = found {
                if status == "queued" || status == "running" {
                    break run_id;
                }
                panic!("step run went terminal before the flip: {run_id} {status}");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "step run never appeared"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    {
        let conn = h.db();
        let changed = conn
            .execute(
                "UPDATE worker_runs SET status = 'failed', tail = ?1, exit_code = NULL, finished_ms = ?2 WHERE run_id = ?3 AND status IN ('queued', 'running')",
                rusqlite::params![
                    "budget_exceeded: execution deadline passed before start",
                    now_ms(),
                    run_id
                ],
            )
            .unwrap();
        assert_eq!(changed, 1, "flip landed while the run was still active");
    }
    let record = h.wait_task_status(&task_id, "failed", 90).await;
    let error = record["error"].as_str().unwrap_or("");
    assert_eq!(
        error,
        "步骤 1 的 worker run 失败：failed（budget_exceeded: execution deadline passed before start）",
        "task error carries the refusal cause: {record}"
    );
    let evidence = h.evidence().await;
    let failed: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "step_failed")
        .collect();
    assert_eq!(failed.len(), 1, "evidence: {evidence:?}");
    let observed = observed_of(failed[0]);
    assert_eq!(observed["step_no"], 1);
    assert_eq!(observed["failure"], "worker_run_failed");
    assert_eq!(observed["run_status"], "failed");
    assert_eq!(observed["run_id"], run_id);
    assert_eq!(
        observed["failure_tail"],
        "budget_exceeded: execution deadline passed before start"
    );
    // Cargo-test steps close with no Inbox row (R3-J18b).
    assert!(h.open_inbox("failed").await.is_empty());
}

/// R5-F2 (Q-R5F2-03): any other tail stays out of the
/// task error and out of the Evidence row. The run here
/// fails naturally (no such package) with a cargo
/// stderr tail.
#[tokio::test]
async fn r5f2_step_failure_other_tail_stays_out() {
    let (fake_url, _fake) = start_fake_deepseek(vec![planner_steps_reply(vec![
        verify_step(1, "cargo test -p does-not-exist"),
    ])])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let task_id = h.make_task().await;
    let (status, value) = h
        .post("/tasks/run", json!({"task_id": task_id, "worktree_id": worktree_id}))
        .await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    h.approve_current(&task_id).await;
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute: {value}");
    let record = h.wait_task_status(&task_id, "failed", 90).await;
    let error = record["error"].as_str().unwrap_or("");
    assert_eq!(error, "步骤 1 的 worker run 失败：failed", "task error: {record}");
    let evidence = h.evidence().await;
    let failed: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "step_failed")
        .collect();
    assert_eq!(failed.len(), 1, "evidence: {evidence:?}");
    let observed = observed_of(failed[0]);
    assert_eq!(observed["step_no"], 1);
    assert_eq!(observed["failure"], "worker_run_failed");
    assert_eq!(observed["run_status"], "failed");
    assert!(observed["run_id"].is_string(), "the run is named: {observed}");
    assert_eq!(
        observed.get("failure_tail"),
        None,
        "no failure_tail for a non-budget tail: {observed}"
    );
    // Sanity: the run really failed with a non-empty,
    // non-budget tail, so the gate was exercised.
    let run_id = observed["run_id"].as_str().unwrap().to_string();
    let tail = {
        let conn = h.db();
        conn.query_row(
            "SELECT tail FROM worker_runs WHERE run_id = ?1",
            [&run_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap()
    };
    assert!(!tail.is_empty(), "run tail present: {tail}");
    assert!(
        !tail.starts_with("budget_exceeded"),
        "run tail is a cargo error, not a refusal: {tail}"
    );
    assert!(h.open_inbox("failed").await.is_empty());
}
