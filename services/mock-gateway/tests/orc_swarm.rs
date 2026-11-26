//! ORC-5 tests (plan §9, Q-R5-01–15): the Swarm
//! fan-out — create + fanout validation (W1), the
//! message board (W2, hashes only), cross-check
//! failure hand-back (W3), merge conflict stop +
//! successful merge (W4), and the restart freeze
//! (swarm rows + `worker_runs.swarm_id` survive a
//! restart and the drive resumes).
//!
//! The multi-shot fake DeepSeek from orc_replan.rs
//! is reused: each queued reply is one model call's
//! completion (Planner round, serial Executor
//! content, swarm Executor contents, in that order).
//! The fake key is `test-key`; no real credential
//! is used.
//!
//! The drive is called directly
//! (`state.drive_ready_swarms().await`) — the
//! harness serves with no sweeper, the
//! `e2d_long_tasks` precedent for
//! `start_due_worker_runs`. No drive call happens
//! except the test's own, so each phase transition
//! is deterministic: the test drives, asserts the
//! state, then drives again.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-orc5-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

struct FakeDeepseek {
    #[allow(dead_code)]
    handle: tokio::task::JoinHandle<()>,
}

async fn start_fake_deepseek(replies: Vec<String>) -> (String, FakeDeepseek) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let queue = Arc::new(Mutex::new(VecDeque::from(replies)));
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let queue = queue.clone();
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
                let _ = body_text;
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
        FakeDeepseek { handle },
    )
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
    state: AppState,
    deepseek_base_url: String,
}

impl Harness {
    async fn serve(deepseek_base_url: &str) -> Self {
        let dir = temp_dir("swarm");
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

    fn evidence_by_reason(&self, reason: &str) -> Vec<Value> {
        let conn = self.db();
        let mut stmt = conn
            .prepare(
                "SELECT verdict, observed_json FROM evidence WHERE reason = ?1 ORDER BY seq ASC",
            )
            .unwrap();
        let rows = stmt
            .query_map([reason], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        rows.into_iter()
            .map(|(verdict, observed)| {
                let mut value: Value = serde_json::from_str(&observed).unwrap();
                value["verdict"] = Value::String(verdict);
                value
            })
            .collect()
    }

    async fn open_inbox(&self, kind: &str) -> Vec<Value> {
        let (status, value) = self
            .get(&format!("/inbox/list?kind={kind}&open_only=true"))
            .await;
        assert_eq!(status, StatusCode::OK, "inbox list: {value}");
        value.as_array().unwrap().clone()
    }

    async fn wait_open_inbox(&self, kind: &str, want: usize, secs: u64) -> Vec<Value> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let rows = self.open_inbox(kind).await;
            if rows.len() == want {
                return rows;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "inbox {kind} stuck at {} rows (want {want})",
                rows.len()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn executions(&self) -> Vec<Value> {
        let (status, value) = self.get("/executions/list").await;
        assert_eq!(status, StatusCode::OK, "executions list: {value}");
        value.as_array().unwrap().clone()
    }

    async fn execution_for_task(&self, task_id: &str) -> Value {
        let executions = self.executions().await;
        executions
            .into_iter()
            .find(|e| e["task_id"] == task_id)
            .unwrap_or_else(|| panic!("no Execution for task {task_id}"))
    }

    async fn wait_execution_status(&self, task_id: &str, want: &str, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let execution = self.execution_for_task(task_id).await;
            if execution["status"] == want {
                return execution;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "execution for {task_id} stuck at {} (want {want}): {execution}",
                execution["status"]
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

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

    async fn swarm_create(&self, task_id: &str) -> (StatusCode, Value) {
        self.post("/swarm/create", json!({"task_id": task_id})).await
    }

    async fn swarm_fanout(&self, swarm_id: &str, step_nos: Vec<i64>) -> (StatusCode, Value) {
        self.post(
            &format!("/swarm/{swarm_id}/fanout"),
            json!({"step_nos": step_nos}),
        )
        .await
    }

    async fn swarm_get(&self, swarm_id: &str) -> (StatusCode, Value) {
        self.get(&format!("/swarm/{swarm_id}")).await
    }

    /// Drive the swarm pass, then poll the swarm view
    /// until it reaches `want` (or the deadline).
    /// Each loop iteration drives once — the drive
    /// advances exactly one phase per call, and the
    /// add-runs / step runs it spawns finish
    /// asynchronously between calls.
    async fn drive_until_swarm_status(&self, swarm_id: &str, want: &str, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            self.state.drive_ready_swarms().await;
            let (status, view) = self.swarm_get(swarm_id).await;
            assert_eq!(status, StatusCode::OK, "get swarm: {view}");
            if view["status"] == want {
                return view;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "swarm {swarm_id} stuck at {} (want {want}): {view}",
                view["status"]
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Approve one swarm step's approval, by step
    /// number, from the live swarm view. Returns the
    /// approval id.
    /// Wait until the task is `draft_ready` at a
    /// given executor step. `wait_task_status` alone
    /// is racy after an execute: the task is already
    /// `draft_ready` (at the old step) while the
    /// spawned runner is still starting.
    async fn wait_task_step(&self, task_id: &str, want_step: i64, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let record = self.task_record(task_id).await;
            if record["status"] == "draft_ready" && record["executor_step_no"] == want_step {
                return record;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "task {task_id} stuck at step {} (want {want_step}): {record}",
                record["executor_step_no"]
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    async fn approve_swarm_step(&self, swarm_id: &str, step_no: i64) -> String {
        let (status, view) = self.swarm_get(swarm_id).await;
        assert_eq!(status, StatusCode::OK, "get swarm: {view}");
        let step = view["steps"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["step_no"] == step_no)
            .unwrap_or_else(|| panic!("swarm {swarm_id} has no step {step_no}: {view}"));
        let approval_id = step["approval_id"]
            .as_str()
            .unwrap_or_else(|| panic!("step {step_no} has no approval: {view}"))
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

    fn swarm_row(&self, swarm_id: &str) -> Value {
        let conn = self.db();
        conn.query_row(
            "SELECT swarm_id, task_id, plan_id, status, steps_json, created_ms, updated_ms, origin_device_id FROM swarms WHERE swarm_id = ?1",
            [swarm_id],
            |row| {
                Ok(json!({
                    "swarm_id": row.get::<_, String>(0)?,
                    "task_id": row.get::<_, String>(1)?,
                    "plan_id": row.get::<_, String>(2)?,
                    "status": row.get::<_, String>(3)?,
                    "steps_json": row.get::<_, String>(4)?,
                    "created_ms": row.get::<_, i64>(5)?,
                    "updated_ms": row.get::<_, i64>(6)?,
                    "origin_device_id": row.get::<_, String>(7)?,
                }))
            },
        )
        .unwrap()
    }

    fn swarm_messages(&self, swarm_id: &str) -> Vec<Value> {
        let conn = self.db();
        let mut stmt = conn
            .prepare("SELECT message_id, sender_worker_id, kind, body_hash, created_ms FROM swarm_messages WHERE swarm_id = ?1 ORDER BY created_ms ASC, message_id ASC")
            .unwrap();
        stmt.query_map([swarm_id], |row| {
            Ok(json!({
                "message_id": row.get::<_, String>(0)?,
                "sender_worker_id": row.get::<_, String>(1)?,
                "kind": row.get::<_, String>(2)?,
                "body_hash": row.get::<_, String>(3)?,
                "created_ms": row.get::<_, i64>(4)?,
            }))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }

    fn worker_runs_for_swarm(&self, swarm_id: &str) -> Vec<Value> {
        let conn = self.db();
        let mut stmt = conn
            .prepare("SELECT run_id, command, status, swarm_id FROM worker_runs WHERE swarm_id = ?1 ORDER BY seq ASC")
            .unwrap();
        stmt.query_map([swarm_id], |row| {
            Ok(json!({
                "run_id": row.get::<_, String>(0)?,
                "command": row.get::<_, String>(1)?,
                "status": row.get::<_, String>(2)?,
                "swarm_id": row.get::<_, Option<String>>(3)?,
            }))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }

    async fn worktrees_for_swarm(&self, swarm_id: &str) -> Vec<Value> {
        let (status, value) = self.get("/worktrees/list").await;
        assert_eq!(status, StatusCode::OK, "worktrees list: {value}");
        value
            .as_array()
            .unwrap()
            .iter()
            .filter(|w| {
                w["branch_label"]
                    .as_str()
                    .unwrap_or("")
                    .starts_with(&format!("swarm-{swarm_id}-"))
            })
            .cloned()
            .collect()
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
    git(&["config", "user.name", "orc5-test"], &repo);
    std::fs::write(repo.join("README.md"), "# test repo\n").unwrap();
    std::fs::create_dir_all(repo.join("proj")).unwrap();
    std::fs::write(repo.join("proj").join("a.md"), "line1\nline2\nline3\n").unwrap();
    std::fs::write(repo.join("proj").join("b.md"), "b1\nb2\nb3\n").unwrap();
    git(&["add", "."], &repo);
    git(&["commit", "-m", "init"], &repo);
    repo
}

fn planner_steps_reply(steps: Vec<Value>, signals: Vec<Value>) -> String {
    serde_json::to_string(&json!({
        "needs_file_change": true,
        "steps": steps,
        "replan_if": signals,
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

fn command_step(step_no: i64, verify_cmd: &str) -> Value {
    json!({
        "step_no": step_no,
        "kind": "command",
        "path": "proj/notes.txt",
        "symbol": "notes",
        "change": "run the check",
        "verify_cmd": verify_cmd,
        "expected": "command succeeds",
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

/// Drive a task to `draft_ready` at step 1 (the
/// serial file-write step). The fake queue must hold,
/// in order: the Planner steps reply, then the
/// serial Executor content for step 1.
async fn drive_to_draft_ready(h: &Harness, worktree_id: &str) -> (String, String) {
    let task_id = h.make_task().await;
    let (status, value) = h
        .post(
            "/tasks/run",
            json!({"task_id": task_id, "worktree_id": worktree_id}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    let plan_id = h.task_record(&task_id).await["plan_id"]
        .as_str()
        .expect("task points at its plan")
        .to_string();
    (task_id, plan_id)
}

/// W1 (Q-R5-03/Q-R5-04): fanout — 4 file-write
/// steps → 4 Worktree records + 4 `git worktree
/// add` runs + 4 Pending approvals once the drive
/// prepares them. Guards: cap 5 → 409, two steps
/// on the same path → 409, a verify-kind step →
/// 409, duplicates / empty / unknown steps → 400.
/// Also the F2 order-constraint regression: a
/// step run whose worktree directory was never
/// materialized → 400 `worktree_directory_missing`.
#[tokio::test]
async fn w1_fanout_four_steps_and_guards() {
    let steps = vec![
        file_write_step(1, "proj/a.md", "git status"),
        file_write_step(2, "proj/b.md", "git status"),
        file_write_step(3, "proj/c.md", "git status"),
        file_write_step(4, "proj/d.md", "git status"),
        file_write_step(5, "proj/a.md", "git status"),
        verify_step(6, "git status"),
    ];
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(steps, vec![]),
        "serial a\n".to_string(),
        "swarm a\n".to_string(),
        "swarm b\n".to_string(),
        "swarm c\n".to_string(),
        "swarm d\n".to_string(),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, plan_id) = drive_to_draft_ready(&h, &worktree_id).await;

    let (status, view) = h.swarm_create(&task_id).await;
    assert_eq!(status, StatusCode::OK, "create swarm: {view}");
    let swarm_id = view["swarm_id"].as_str().unwrap().to_string();
    assert_eq!(view["task_id"], task_id);
    assert_eq!(view["plan_id"], plan_id);
    assert_eq!(view["status"], "created");
    assert_eq!(view["steps"].as_array().unwrap().len(), 0);
    assert_eq!(view["messages"].as_array().unwrap().len(), 0);
    assert_eq!(view["leftover_worktrees"].as_array().unwrap().len(), 0);

    // Guards, all on the still-created swarm.
    for (step_nos, want_status) in [
        (vec![1, 2, 3, 4, 5], StatusCode::CONFLICT),
        (vec![1, 5], StatusCode::CONFLICT),
        (vec![6], StatusCode::CONFLICT),
        (vec![], StatusCode::BAD_REQUEST),
        (vec![1, 1], StatusCode::BAD_REQUEST),
        (vec![99], StatusCode::BAD_REQUEST),
    ] {
        let (status, value) = h.swarm_fanout(&swarm_id, step_nos.clone()).await;
        assert_eq!(
            status, want_status,
            "fanout {step_nos:?}: {value} (want {want_status})"
        );
    }
    let (_, view) = h.swarm_get(&swarm_id).await;
    assert_eq!(view["status"], "created", "guards leave the swarm created");

    // Happy-path fanout.
    let (status, view) = h.swarm_fanout(&swarm_id, vec![1, 2, 3, 4]).await;
    assert_eq!(status, StatusCode::OK, "fanout: {view}");
    assert_eq!(view["status"], "fanning_out");
    let view_steps = view["steps"].as_array().unwrap();
    assert_eq!(view_steps.len(), 4);
    for (i, step) in view_steps.iter().enumerate() {
        assert_eq!(step["step_no"], i as i64 + 1);
        assert_eq!(step["worker_id"], format!("w{}", i + 1));
        // §6.3 carries no add_run_id in the view —
        // the add-runs are verified below from the
        // worker_runs rows, not the view.
        assert_eq!(step["run_status"], "pending");
    }

    // The drive links the copies once the add-runs
    // succeed, then the swarm is running.
    let _view = h.drive_until_swarm_status(&swarm_id, "running", 30).await;
    let copies = h.worktrees_for_swarm(&swarm_id).await;
    assert_eq!(copies.len(), 4, "4 copy Worktree records: {copies:?}");
    for copy in &copies {
        assert_eq!(copy["status"], "active");
        assert!(
            copy["directory_relative"]
                .as_str()
                .unwrap()
                .starts_with(&format!("swarm-{swarm_id}-")),
            "copy dir: {copy}"
        );
    }
    let add_runs = h.worker_runs_for_swarm(&swarm_id);
    assert_eq!(add_runs.len(), 4, "4 add-runs: {add_runs:?}");
    for run in &add_runs {
        assert_eq!(run["command"], "git-worktree-add");
        assert_eq!(run["status"], "succeeded");
        assert_eq!(run["swarm_id"], swarm_id);
    }
    let fanout_evidence = h.evidence_by_reason("swarm_fanout");
    assert_eq!(fanout_evidence.len(), 4, "one fanout row per step");
    for row in &fanout_evidence {
        assert_eq!(row["verdict"], "verified");
    }
    let row = h.swarm_row(&swarm_id);
    let steps_json: Value = serde_json::from_str(row["steps_json"].as_str().unwrap()).unwrap();
    for step in steps_json.as_array().unwrap() {
        assert!(
            step["worktree_id"].as_str().is_some(),
            "step linked to its copy: {step}"
        );
    }

    // Prepare: the Executor model writes one content
    // per step, each bound by an fs.write approval.
    h.state.drive_ready_swarms().await;
    let (_, view) = h.swarm_get(&swarm_id).await;
    assert_eq!(view["status"], "running");
    let pending: Vec<&Value> = view["steps"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|s| s["approval_status"] == "pending")
        .collect();
    assert_eq!(pending.len(), 4, "4 Pending approvals: {view}");
    for step in pending {
        assert!(step["approval_id"].as_str().is_some());
    }

    // F2 regression: a step run whose worktree
    // directory was never materialized is a 400,
    // not a panic or a silent skip.
    let execution = h.execution_for_task(&task_id).await;
    let execution_id = execution["execution_id"].as_str().unwrap().to_string();
    std::fs::create_dir_all(repo.join("proj2")).unwrap();
    let (status, worktree) = h
        .post(
            "/worktrees/create",
            json!({"location_id": h.task_record(&task_id).await["execute_binding"]["location_id"], "directory_relative": "proj2", "branch_label": "proj2", "project_id": null}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "create proj2 worktree: {worktree}");
    let proj2_id = worktree["worktree_id"].as_str().unwrap().to_string();
    std::fs::remove_dir_all(repo.join("proj2")).unwrap();
    let (status, value) = h
        .post(
            "/worker/runs",
            json!({"execution_id": execution_id, "worktree_id": proj2_id, "command": "git-status"}),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "run in a missing dir: {value}");
    assert_eq!(value["error"], "worktree_directory_missing");
}

/// W2 (Q-ORC-17/Q-R5-08): the message board holds
/// hashes only — the `copies_ready` status row's
/// `body_hash` is the sha256 of the text, the
/// plaintext appears in no column, and no board row
/// ever enters the events table.
#[tokio::test]
async fn w2_board_hashes_only() {
    let steps = vec![
        file_write_step(1, "proj/a.md", "git status"),
        file_write_step(2, "proj/b.md", "git status"),
    ];
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(steps, vec![]),
        "serial a\n".to_string(),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, _plan_id) = drive_to_draft_ready(&h, &worktree_id).await;
    let (_, view) = h.swarm_create(&task_id).await;
    let swarm_id = view["swarm_id"].as_str().unwrap().to_string();
    let (status, _) = h.swarm_fanout(&swarm_id, vec![1, 2]).await;
    assert_eq!(status, StatusCode::OK);
    let view = h.drive_until_swarm_status(&swarm_id, "running", 30).await;

    let messages = view["messages"].as_array().unwrap();
    assert!(!messages.is_empty(), "board has rows: {view}");
    let status_row = messages
        .iter()
        .find(|m| m["kind"] == "status")
        .expect("a status board row");
    let hash = status_row["body_hash"].as_str().unwrap();
    assert_eq!(hash.len(), 64, "sha256 hex: {hash}");
    assert!(
        hash.chars().all(|c| c.is_ascii_hexdigit()),
        "hex only: {hash}"
    );
    assert_eq!(
        hash,
        mock_gateway::operation_binding::sha256_hex(b"copies_ready"),
        "the hash is the sha256 of the board text"
    );

    // Plaintext in no column, on any board row.
    let rows = h.swarm_messages(&swarm_id);
    assert!(!rows.is_empty());
    for row in &rows {
        for (key, value) in row.as_object().unwrap() {
            let text = value.to_string();
            assert!(
                !text.contains("copies_ready"),
                "plaintext leaked into {key}: {row}"
            );
        }
    }
    // Board rows never enter the events table
    // (swarm_messages is not a serialization kind).
    let conn = h.db();
    let board_events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM events WHERE aggregate_kind = 'swarm_message'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(board_events, 0, "board rows in the events table");
}

/// W3 (Q-R5-06/Q-R5-10/Q-R5-12): a cross-check
/// mismatch hands back — the swarm goes `conflict`,
/// the task stays `draft_ready`, the Execution is
/// `needs_review`, one `review_required` Inbox row
/// opens, a `swarm_conflict` Evidence row is
/// written, and the board carries a `conflict` row.
#[tokio::test]
async fn w3_cross_check_failure_hands_back() {
    let steps = vec![
        file_write_step(1, "proj/a.md", "git status"),
        file_write_step(2, "proj/b.md", "git status"),
    ];
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(steps, vec![]),
        "serial a\n".to_string(),
        "swarm a\n".to_string(),
        "swarm b\n".to_string(),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, _plan_id) = drive_to_draft_ready(&h, &worktree_id).await;
    let (_, view) = h.swarm_create(&task_id).await;
    let swarm_id = view["swarm_id"].as_str().unwrap().to_string();
    let (status, _) = h.swarm_fanout(&swarm_id, vec![1, 2]).await;
    assert_eq!(status, StatusCode::OK);
    h.drive_until_swarm_status(&swarm_id, "running", 30).await;
    h.state.drive_ready_swarms().await;
    h.approve_swarm_step(&swarm_id, 1).await;
    h.approve_swarm_step(&swarm_id, 2).await;
    // Both approved steps apply, then the drive
    // moves the swarm to `verifying` — but the
    // cross-check itself has not run yet, so the
    // test can still tamper with a copy.
    h.drive_until_swarm_status(&swarm_id, "verifying", 30).await;

    let copy = repo.join(format!("swarm-{swarm_id}-w1"));
    let a_path = copy.join("proj").join("a.md");
    assert_eq!(std::fs::read_to_string(&a_path).unwrap(), "swarm a\n");
    std::fs::write(&a_path, "tampered after approval\n").unwrap();

    let view = h.drive_until_swarm_status(&swarm_id, "conflict", 10).await;
    let conflict_rows: Vec<&Value> = view["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["kind"] == "conflict")
        .collect();
    assert_eq!(conflict_rows.len(), 1, "one board conflict row: {view}");

    let record = h.task_record(&task_id).await;
    assert_eq!(record["status"], "draft_ready", "task handed back, not failed");
    let execution = h.wait_execution_status(&task_id, "needs_review", 10).await;
    assert_eq!(execution["status"], "needs_review");
    let inbox = h.wait_open_inbox("review_required", 1, 10).await;
    assert_eq!(inbox.len(), 1);
    let conflict_evidence = h.evidence_by_reason("swarm_conflict");
    assert_eq!(conflict_evidence.len(), 1, "one conflict Evidence row");
    assert_eq!(conflict_evidence[0]["verdict"], "mismatch");
    let cross_evidence = h.evidence_by_reason("swarm_cross_check");
    assert_eq!(cross_evidence.len(), 2, "one cross-check row per step");
    assert!(
        cross_evidence
            .iter()
            .any(|row| row["verdict"] == "mismatch"),
        "the tampered step is a mismatch: {cross_evidence:?}"
    );
}

/// W4a (Q-R5-07/Q-ORC-18): a merge conflict stops
/// the merge — the swarm goes `conflict`, the task
/// is handed back, the copies are left listed
/// (not removed), and the main worktree is
/// untouched by the swarm.
#[tokio::test]
async fn w4a_merge_conflict_stops_and_hands_back() {
    let steps = vec![file_write_step(1, "proj/a.md", "git status")];
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(steps, vec![]),
        "serial a\n".to_string(),
        "line1\nSWARM\nline3\n".to_string(),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, _plan_id) = drive_to_draft_ready(&h, &worktree_id).await;
    let (_, view) = h.swarm_create(&task_id).await;
    let swarm_id = view["swarm_id"].as_str().unwrap().to_string();
    let (status, _) = h.swarm_fanout(&swarm_id, vec![1]).await;
    assert_eq!(status, StatusCode::OK);
    h.drive_until_swarm_status(&swarm_id, "running", 30).await;
    h.state.drive_ready_swarms().await;
    h.approve_swarm_step(&swarm_id, 1).await;
    h.drive_until_swarm_status(&swarm_id, "merging", 30).await;

    // Main moves first: the same lines change on
    // main, so the copy's patch no longer applies.
    std::fs::write(repo.join("proj").join("a.md"), "line1\nMAIN\nline3\n").unwrap();
    git(&["add", "."], &repo);
    git(&["commit", "-m", "main moves"], &repo);

    let view = h.drive_until_swarm_status(&swarm_id, "conflict", 10).await;
    assert_eq!(view["status"], "conflict");
    let leftover = view["leftover_worktrees"].as_array().unwrap();
    assert_eq!(leftover.len(), 1, "the copy is left listed: {view}");
    assert_eq!(
        leftover[0].as_str().unwrap(),
        view["steps"][0]["worktree_id"].as_str().unwrap(),
        "leftover is the copy's worktree id: {leftover:?}"
    );
    assert_eq!(
        std::fs::read_to_string(repo.join("proj").join("a.md")).unwrap(),
        "line1\nMAIN\nline3\n",
        "main is untouched by the swarm"
    );
    let record = h.task_record(&task_id).await;
    assert_eq!(record["status"], "draft_ready", "task handed back");
    let inbox = h.wait_open_inbox("review_required", 1, 10).await;
    assert_eq!(inbox.len(), 1);
    let conflict_evidence = h.evidence_by_reason("swarm_conflict");
    assert_eq!(conflict_evidence.len(), 1);
    assert_eq!(conflict_evidence[0]["verdict"], "mismatch");
}

/// W4b (Q-R5-05/Q-R5-07): a clean merge lands the
/// sum of the copies on main, removes the copies,
/// writes `swarm_merged` Evidence, and the task
/// continues with its unfanned steps (read steps
/// clear the stale approval) to `completed`.
#[tokio::test]
async fn w4b_successful_merge_then_task_continues() {
    let steps = vec![
        file_write_step(1, "proj/a.md", "git status"),
        file_write_step(2, "proj/b.md", "git status"),
        command_step(3, "git status"),
        command_step(4, "git status"),
    ];
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(steps, vec![]),
        "serial a\n".to_string(),
        "line1\nSWARM-A\nline3\n".to_string(),
        "b1\nSWARM-B\nb3\n".to_string(),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, _plan_id) = drive_to_draft_ready(&h, &worktree_id).await;
    let (_, view) = h.swarm_create(&task_id).await;
    let swarm_id = view["swarm_id"].as_str().unwrap().to_string();
    let (status, _) = h.swarm_fanout(&swarm_id, vec![1, 2]).await;
    assert_eq!(status, StatusCode::OK);
    h.drive_until_swarm_status(&swarm_id, "running", 30).await;
    h.state.drive_ready_swarms().await;
    h.approve_swarm_step(&swarm_id, 1).await;
    h.approve_swarm_step(&swarm_id, 2).await;
    let view = h.drive_until_swarm_status(&swarm_id, "completed", 30).await;
    assert_eq!(view["status"], "completed");
    assert_eq!(
        view["leftover_worktrees"].as_array().unwrap().len(),
        0,
        "no copies left: {view}"
    );
    assert_eq!(
        std::fs::read_to_string(repo.join("proj").join("a.md")).unwrap(),
        "line1\nSWARM-A\nline3\n",
        "main carries copy w1's change"
    );
    assert_eq!(
        std::fs::read_to_string(repo.join("proj").join("b.md")).unwrap(),
        "b1\nSWARM-B\nb3\n",
        "main carries copy w2's change"
    );
    assert!(
        !repo.join(format!("swarm-{swarm_id}-w1")).exists(),
        "copy w1 removed from disk"
    );
    assert!(
        !repo.join(format!("swarm-{swarm_id}-w2")).exists(),
        "copy w2 removed from disk"
    );
    let copies = h.worktrees_for_swarm(&swarm_id).await;
    assert_eq!(copies.len(), 0, "no active copy records left: {copies:?}");
    let merged = h.evidence_by_reason("swarm_merged");
    assert_eq!(merged.len(), 1, "one merged Evidence row");
    assert_eq!(merged[0]["verdict"], "verified");
    let board = view["messages"].as_array().unwrap();
    assert!(
        board.iter().any(|m| m["kind"] == "result"),
        "a merged board row: {board:?}"
    );

    // The task continues with its unfanned steps:
    // step 3 is a read, so it clears the stale
    // approval and runs without a fresh approval.
    let record = h.wait_task_step(&task_id, 3, 10).await;
    assert!(record["approval_id"].is_null(), "read step clears approval");
    let (status, _) = h
        .post("/tasks/execute", json!({"task_id": task_id}))
        .await;
    assert_eq!(status, StatusCode::OK);
    h.wait_task_step(&task_id, 4, 30).await;
    let (status, _) = h
        .post("/tasks/execute", json!({"task_id": task_id}))
        .await;
    assert_eq!(status, StatusCode::OK);
    h.wait_task_status(&task_id, "completed", 30).await;
    h.wait_execution_status(&task_id, "completed", 10).await;
}

/// Restart freeze (plan §13 G2): swarm rows and
/// `worker_runs.swarm_id` are identical across a
/// restart, and the drive resumes without losing a
/// step.
#[tokio::test]
async fn restart_freezes_swarm_and_drive_resumes() {
    let steps = vec![
        file_write_step(1, "proj/a.md", "git status"),
        file_write_step(2, "proj/b.md", "git status"),
    ];
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(steps, vec![]),
        "serial a\n".to_string(),
        "swarm a\n".to_string(),
        "swarm b\n".to_string(),
    ])
    .await;
    let mut h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, _plan_id) = drive_to_draft_ready(&h, &worktree_id).await;
    let (_, view) = h.swarm_create(&task_id).await;
    let swarm_id = view["swarm_id"].as_str().unwrap().to_string();
    let (status, _) = h.swarm_fanout(&swarm_id, vec![1, 2]).await;
    assert_eq!(status, StatusCode::OK);
    h.drive_until_swarm_status(&swarm_id, "running", 30).await;
    let before_row = h.swarm_row(&swarm_id);
    let before_runs = h.worker_runs_for_swarm(&swarm_id);
    assert_eq!(before_runs.len(), 2);

    h.restart().await;

    let after_row = h.swarm_row(&swarm_id);
    assert_eq!(after_row, before_row, "swarm row identical across restart");
    let after_runs = h.worker_runs_for_swarm(&swarm_id);
    assert_eq!(after_runs, before_runs, "run rows identical across restart");
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);

    // The drive resumes: prepare + approve + merge.
    h.state.drive_ready_swarms().await;
    h.approve_swarm_step(&swarm_id, 1).await;
    h.approve_swarm_step(&swarm_id, 2).await;
    h.drive_until_swarm_status(&swarm_id, "completed", 30).await;
    assert_eq!(
        std::fs::read_to_string(repo.join("proj").join("a.md")).unwrap(),
        "swarm a\n"
    );
}

/// The v21 pin: a fresh DB opens at schema 21.
#[test]
fn schema_version_is_21() {
    assert_eq!(DB_SCHEMA_VERSION, 21);
    let dir = temp_dir("schema");
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let _state = build_state(&dir, "http://127.0.0.1:1");
    let conn = sqlite_store::open_db(&dir.join("gateway.sqlite3")).expect("open db");
    assert_eq!(sqlite_store::schema_version(&conn).unwrap(), 21);
}
