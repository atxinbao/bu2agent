//! ORC-3 tests (plan §4/§8, Q-R3-01–12):
//! the Re-plan-if trigger evaluation + hand-back
//! loop — a step failure that matches a `replan_if`
//! signal hands the task back (draft_ready +
//! Execution needs_review + one `review_required`
//! Inbox row + one `replan_triggered` Evidence row,
//! no new plan version) instead of failing it; a
//! miss keeps the ORC-2 failure path; the user gate
//! (no re-plan before the user acts); the 3-re-plan
//! limit (over-limit → failed + `failed` Inbox row);
//! manual re-plan (new version supersedes, old
//! unconsumed approval superseded, batch re-runs
//! from step 1); and the draft/triage regression
//! (a manual draft is not a re-plan).
//!
//! The multi-shot fake DeepSeek from orc_roles.rs is
//! reused: each queued reply is one model call's
//! completion (Planner round, Executor content,
//! re-plan judge, re-plan Planner round). The fake
//! key is `test-key`; no real credential is used.

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
        "bu2agent-orc3-{tag}-{}-{}",
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
    seen: Arc<Mutex<Vec<Value>>>,
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
    fn seen_count(&self) -> usize {
        self.seen.lock().expect("seen lock").len()
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
}

impl Harness {
    async fn serve(deepseek_base_url: &str) -> Self {
        let dir = temp_dir("replan");
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

    fn plan_count(&self, task_id: &str) -> i64 {
        let conn = self.db();
        conn.query_row(
            "SELECT COUNT(*) FROM plans WHERE task_id = ?1",
            [task_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn events_of_type(&self, event_type: &str) -> i64 {
        let conn = self.db();
        conn.query_row(
            "SELECT COUNT(*) FROM events WHERE event_type = ?1",
            [event_type],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// (version, supersedes_version, consumed) of one
    /// approval row, read from SQLite.
    fn approval_row(&self, approval_id: &str) -> (Option<i64>, Option<i64>, bool) {
        let conn = self.db();
        conn.query_row(
            "SELECT version, supersedes_version, consumed FROM approvals WHERE approval_id = ?1",
            [approval_id],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, i64>(2)? != 0,
                ))
            },
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

    /// Poll until exactly `want` open Inbox rows of
    /// `kind` exist (the hand-back lands async).
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

    async fn plan_by_id(&self, plan_id: &str) -> Value {
        let (status, value) = self.get(&format!("/tasks/plan/{plan_id}")).await;
        assert_eq!(status, StatusCode::OK, "get plan {plan_id}: {value}");
        value
    }

    async fn current_plan(&self, task_id: &str) -> Value {
        let (status, value) = self.get(&format!("/tasks/plan?task_id={task_id}")).await;
        assert_eq!(status, StatusCode::OK, "current plan: {value}");
        value
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
    git(&["config", "user.name", "orc3-test"], &repo);
    std::fs::write(repo.join("README.md"), "# test repo\n").unwrap();
    git(&["add", "."], &repo);
    git(&["commit", "-m", "init"], &repo);
    repo
}

const SIGNAL: &str = "测试失败，请重新规划";

fn planner_steps_reply(steps: Vec<Value>, signals: Vec<Value>) -> String {
    serde_json::to_string(&json!({
        "needs_file_change": true,
        "steps": steps,
        "replan_if": signals,
    }))
    .unwrap()
}

fn signal_condition() -> Value {
    json!({"signal": SIGNAL, "action": "stop_and_hand_back"})
}

fn judge_reply(triggered: bool) -> String {
    serde_json::to_string(&json!({
        "triggered": triggered,
        "signal": if triggered { Value::String(SIGNAL.to_string()) } else { Value::Null },
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

/// Drive a two-step task (file write, then a failing
/// cargo-test verify step) to the step-2 worker run,
/// which fails. Returns the task id and the plan id.
/// The fake queue must hold, in order: the Planner
/// steps reply, the step-1 Executor content, and the
/// judge reply.
async fn drive_to_step2_failure(h: &Harness, worktree_id: &str) -> (String, String) {
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
    h.approve_current(&task_id).await;
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute step 1: {value}");
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 2);
    assert!(record["approval_id"].is_string(), "step 2 has an approval");
    h.approve_current(&task_id).await;
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute step 2 accepted: {value}");
    let plan_id = h.task_record(&task_id).await["plan_id"]
        .as_str()
        .expect("task points at its plan")
        .to_string();
    (task_id, plan_id)
}

/// H1 (Q-R3-01/02/03/04): a step failure that
/// matches a `replan_if` signal hands the task back
/// instead of failing it — task stays `draft_ready`,
/// Execution goes `needs_review`, exactly one
/// `review_required` Inbox row opens, one
/// `replan_triggered` Evidence row is written, no
/// `step_failed` row, no new plan version.
#[tokio::test]
async fn h1_trigger_match_hands_back() {
    let (fake_url, fake) = start_fake_deepseek(vec![
        planner_steps_reply(
            vec![
                file_write_step(1, "proj/report.md", "git status"),
                verify_step(2, "cargo test -p does-not-exist"),
            ],
            vec![signal_condition()],
        ),
        "STEP ONE REPORT\n".to_string(),
        judge_reply(true),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, plan_id) = drive_to_step2_failure(&h, &worktree_id).await;
    // The hand-back lands async: poll the Inbox row.
    let rows = h.wait_open_inbox("review_required", 1, 90).await;
    let execution_id = h.execution_for_task(&task_id).await["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(rows[0]["execution_id"], execution_id);
    assert!(
        rows[0]["effect_id"].is_string(),
        "the review row names the failed step's Effect: {rows:?}"
    );
    // The task never failed: still draft_ready at the
    // stopped step, no error recorded.
    let record = h.task_record(&task_id).await;
    assert_eq!(record["status"], "draft_ready");
    assert_eq!(record["executor_step_no"], 2);
    assert_eq!(record["error"], Value::Null);
    // The Execution is needs_review (the transition
    // that opened the row).
    let execution = h.wait_execution_status(&task_id, "needs_review", 10).await;
    assert_eq!(execution["execution_id"], execution_id);
    // Exactly one `replan_triggered` row, from the
    // executor, naming the step and the signal — and
    // no `step_failed` row for this failure.
    let evidence = h.evidence().await;
    let triggered: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "replan_triggered")
        .collect();
    assert_eq!(triggered.len(), 1, "evidence: {evidence:?}");
    assert_eq!(triggered[0]["source"], "executor");
    assert_eq!(triggered[0]["effect_id"], rows[0]["effect_id"]);
    let observed = observed_of(triggered[0]);
    assert_eq!(observed["task_id"], task_id);
    assert_eq!(observed["plan_id"], plan_id);
    assert_eq!(observed["step_no"], 2);
    assert_eq!(observed["signal"], SIGNAL);
    assert!(
        evidence.iter().all(|e| e["reason"] != "step_failed"),
        "a matched failure writes no step_failed row: {evidence:?}"
    );
    // No new plan version was created by the
    // hand-back, and no failed Inbox row opened.
    assert_eq!(h.plan_count(&task_id), 1);
    assert_eq!(h.current_plan(&task_id).await["version"], 1);
    assert_eq!(h.current_plan(&task_id).await["replan_count"], 0);
    assert!(h.open_inbox("failed").await.is_empty());
    // The judge ran on the planner slot (third model
    // call) and saw the signal list.
    assert_eq!(fake.seen_count(), 3);
    assert!(
        fake.prompt_of(2).contains(SIGNAL),
        "the judge prompt carries the signal list"
    );
}

/// H2 (Q-R3-01/02): a step failure that matches no
/// signal keeps the ORC-2 failure path exactly —
/// `step_failed` Evidence + task `failed` + Execution
/// `failed`, one `replan_not_triggered` row, and no
/// `review_required` row.
#[tokio::test]
async fn h2_trigger_miss_keeps_failure_path() {
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(
            vec![
                file_write_step(1, "proj/report.md", "git status"),
                verify_step(2, "cargo test -p does-not-exist"),
            ],
            vec![signal_condition()],
        ),
        "STEP ONE REPORT\n".to_string(),
        judge_reply(false),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, plan_id) = drive_to_step2_failure(&h, &worktree_id).await;
    let record = h.wait_task_status(&task_id, "failed", 90).await;
    assert_eq!(record["status"], "failed");
    assert!(
        record["error"].as_str().unwrap_or("").contains("步骤 2"),
        "the failure names the failed step: {record}"
    );
    let execution = h.execution_for_task(&task_id).await;
    assert_eq!(execution["status"], "failed");
    let evidence = h.evidence().await;
    let failed: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "step_failed")
        .collect();
    assert_eq!(failed.len(), 1, "evidence: {evidence:?}");
    assert_eq!(failed[0]["source"], "executor");
    let not_triggered: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "replan_not_triggered")
        .collect();
    assert_eq!(not_triggered.len(), 1, "evidence: {evidence:?}");
    let observed = observed_of(not_triggered[0]);
    assert_eq!(observed["task_id"], task_id);
    assert_eq!(observed["plan_id"], plan_id);
    assert_eq!(observed["step_no"], 2);
    assert_eq!(observed["signal"], Value::Null);
    assert!(
        evidence.iter().all(|e| e["reason"] != "replan_triggered"),
        "a miss writes no triggered row: {evidence:?}"
    );
    assert!(h.open_inbox("review_required").await.is_empty());
    assert_eq!(h.plan_count(&task_id), 1);
}

/// H3 (Q-R3-05/08): the user gate — after a
/// hand-back, no re-plan happens until the user
/// acts; one replan call writes version+1, supersedes
/// the old version, and leaves the task at step 1 of
/// the new plan (a manual draft would not count).
#[tokio::test]
async fn h3_user_gate_then_one_replan() {
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(
            vec![
                file_write_step(1, "proj/report.md", "git status"),
                verify_step(2, "cargo test -p does-not-exist"),
            ],
            vec![signal_condition()],
        ),
        "STEP ONE REPORT\n".to_string(),
        judge_reply(true),
        // The replan Planner round: a single worker
        // step, so its prepare needs no model call.
        planner_steps_reply(vec![verify_step(1, "git status")], vec![]),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, plan_id) = drive_to_step2_failure(&h, &worktree_id).await;
    h.wait_open_inbox("review_required", 1, 90).await;
    // Before the user acts: no new version, counts
    // frozen, exactly one plan.created event.
    assert_eq!(h.plan_count(&task_id), 1);
    assert_eq!(h.current_plan(&task_id).await["version"], 1);
    assert_eq!(h.events_of_type("plan.created"), 1);
    assert_eq!(h.events_of_type("plan.superseded"), 0);
    // One replan call: 200, version 2, count 1.
    let (status, next) = h
        .post(&format!("/tasks/plan/{plan_id}/replan"), json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "replan: {next}");
    assert_eq!(next["version"], 2);
    assert_eq!(next["replan_count"], 1);
    let next_id = next["plan_id"].as_str().unwrap().to_string();
    assert_ne!(next_id, plan_id);
    // The old version is superseded, the task points
    // at the new plan at step 1, still draft_ready.
    assert_eq!(h.plan_by_id(&plan_id).await["status"], "superseded");
    assert_eq!(h.plan_count(&task_id), 2);
    assert_eq!(h.events_of_type("plan.superseded"), 1);
    let record = h.task_record(&task_id).await;
    assert_eq!(record["status"], "draft_ready");
    assert_eq!(record["plan_id"], next_id);
    assert_eq!(record["executor_step_no"], 1);
    assert_eq!(record["approval_id"], Value::Null);
    // The review row is still open: the Execution
    // stays needs_review until the batch actually
    // resumes or ends (R3-J17).
    assert_eq!(h.open_inbox("review_required").await.len(), 1);
}

/// H4 (Q-R3-09/10): the 3-re-plan limit — three
/// replans succeed (count 1/2/3); the fourth is 409
/// `replan_limit_exceeded`, the task fails with the
/// limit message, the review row auto-closes, one
/// `failed` Inbox row opens, and one
/// `replan_exhausted` Evidence row names the count.
#[tokio::test]
async fn h4_limit_three_then_terminal() {
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(
            vec![
                file_write_step(1, "proj/report.md", "git status"),
                verify_step(2, "cargo test -p does-not-exist"),
            ],
            vec![signal_condition()],
        ),
        "STEP ONE REPORT\n".to_string(),
        judge_reply(true),
        planner_steps_reply(vec![verify_step(1, "git status")], vec![]),
        planner_steps_reply(vec![verify_step(1, "git status")], vec![]),
        planner_steps_reply(vec![verify_step(1, "git status")], vec![]),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let (task_id, mut plan_id) = drive_to_step2_failure(&h, &worktree_id).await;
    h.wait_open_inbox("review_required", 1, 90).await;
    for want_count in 1..=3 {
        let (status, next) = h
            .post(&format!("/tasks/plan/{plan_id}/replan"), json!({}))
            .await;
        assert_eq!(status, StatusCode::OK, "replan {want_count}: {next}");
        assert_eq!(next["replan_count"], want_count);
        plan_id = next["plan_id"].as_str().unwrap().to_string();
    }
    assert_eq!(h.plan_count(&task_id), 4);
    assert_eq!(h.current_plan(&task_id).await["replan_count"], 3);
    // The fourth replan is refused and fails the task.
    let (status, value) = h
        .post(&format!("/tasks/plan/{plan_id}/replan"), json!({}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "fourth replan: {value}");
    assert_eq!(value["error"], "replan_limit_exceeded");
    let record = h.wait_task_status(&task_id, "failed", 30).await;
    assert!(
        record["error"]
            .as_str()
            .unwrap_or("")
            .contains("重规划次数已达上限"),
        "the failure names the limit: {record}"
    );
    let execution = h.execution_for_task(&task_id).await;
    assert_eq!(execution["status"], "failed");
    // R3-J20 (user decision 2026-11-24): the over-limit
    // failure closes the hand-back's review row — a failed
    // close auto-closes review rows for the same execution,
    // so only the failed row stays open (ack-only).
    assert_eq!(h.open_inbox("review_required").await.len(), 0);
    let failed = h.wait_open_inbox("failed", 1, 10).await;
    assert_eq!(failed[0]["execution_id"], execution["execution_id"]);
    let evidence = h.evidence().await;
    let exhausted: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "replan_exhausted")
        .collect();
    assert_eq!(exhausted.len(), 1, "evidence: {evidence:?}");
    let observed = observed_of(exhausted[0]);
    assert_eq!(observed["task_id"], task_id);
    assert_eq!(observed["replan_count"], 3);
}

/// H5 (Q-R3-05/06/07): manual re-plan — no trigger,
/// straight to the route: 200, new version supersedes
/// the old one, the old version's unconsumed approval
/// is superseded (version chain + event, its
/// `approval_required` row auto-closed), and the
/// batch re-runs from step 1 and completes.
#[tokio::test]
async fn h5_manual_replan_supersedes_approval_and_reruns() {
    let (fake_url, _fake) = start_fake_deepseek(vec![
        planner_steps_reply(vec![file_write_step(1, "proj/report.md", "git status")], vec![]),
        // Step-1 prepare: the Executor content for the
        // file step (a model call, before the replan).
        "STEP ONE REPORT\n".to_string(),
        // The replan Planner round: a worker step, so
        // the resumed batch needs no file approval.
        planner_steps_reply(vec![verify_step(1, "git status")], vec![]),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let repo = init_repo(&h.dir);
    let worktree_id = h.setup_worktree(&repo).await;
    let task_id = h.make_task().await;
    let (status, value) = h
        .post(
            "/tasks/run",
            json!({"task_id": task_id, "worktree_id": worktree_id}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    // Step 1 is prepared: an unconsumed write
    // approval is pending, with an open
    // `approval_required` row.
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    let approval_id = record["approval_id"]
        .as_str()
        .expect("step 1 has an approval")
        .to_string();
    assert_eq!(h.approval_row(&approval_id), (Some(1), None, false));
    // NOTE (R3-J19): no `approval_required` row opens
    // at prepare — the write approval carries no
    // effect_binding until execute (ORC-2 behavior),
    // so there is no row to assert here; the supersede
    // below is proven by the version chain + event.
    assert!(h.open_inbox("approval_required").await.is_empty());
    let plan_id = record["plan_id"].as_str().unwrap().to_string();
    // Manual replan, no trigger involved.
    let (status, next) = h
        .post(&format!("/tasks/plan/{plan_id}/replan"), json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "replan: {next}");
    assert_eq!(next["version"], 2);
    assert_eq!(next["replan_count"], 1);
    let next_id = next["plan_id"].as_str().unwrap().to_string();
    assert_eq!(h.plan_by_id(&plan_id).await["status"], "superseded");
    // The old approval is superseded, not consumed:
    // version chain + event, row auto-closed.
    assert_eq!(h.approval_row(&approval_id), (Some(2), Some(1), false));
    assert_eq!(h.events_of_type("approval.superseded"), 1);
    // The task is at step 1 of the new plan, prepared
    // (R3-J16: the replan route prepares step 1).
    let record = h.task_record(&task_id).await;
    assert_eq!(record["status"], "draft_ready");
    assert_eq!(record["plan_id"], next_id);
    assert_eq!(record["executor_step_no"], 1);
    assert_eq!(record["approval_id"], Value::Null);
    // The re-run is the read step: execute runs it in
    // the frozen worktree and the task completes. The
    // abandoned file was never written.
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute: {value}");
    h.wait_task_status(&task_id, "completed", 90).await;
    assert!(
        !h.dir.join("files").join("proj").join("report.md").exists(),
        "the abandoned step's file was never written"
    );
    assert_eq!(h.plan_count(&task_id), 2);
}

/// H6 (Q-R3-08, regression): a manual draft is not a
/// re-plan (version+1, old superseded, count stays
/// 0), and triage is untouched by trigger evaluation
/// (no `replan_*` Evidence anywhere — triage has no
/// failure branch to hook).
#[tokio::test]
async fn h6_manual_draft_is_not_replan_and_triage_untouched() {
    assert_eq!(DB_SCHEMA_VERSION, 22, "AO-1 bumps the schema to v22 (ORC-5 added v21; ORC-3 adds no migration)");
    let (fake_url, _fake) = start_fake_deepseek(vec![serde_json::to_string(&json!({
        "needs_file_change": false,
        "answer": "No change needed.",
        "steps": [],
        "replan_if": [],
    }))
    .unwrap()])
    .await;
    let h = Harness::serve(&fake_url).await;
    assert_eq!(h.schema_version(), 22);
    let task_id = h.make_task().await;
    // Two manual drafts: v1 then v2, count stays 0.
    let draft = |steps: Vec<Value>| {
        json!({
            "task_id": task_id,
            "steps": steps,
            "replan_if": [],
        })
    };
    let (status, v1) = h.post("/tasks/plan/draft", draft(vec![file_write_step(1, "proj/a.md", "git status")])).await;
    assert_eq!(status, StatusCode::OK, "draft v1: {v1}");
    assert_eq!(v1["version"], 1);
    assert_eq!(v1["replan_count"], 0);
    let plan_id = v1["plan_id"].as_str().unwrap().to_string();
    let (status, v2) = h.post("/tasks/plan/draft", draft(vec![file_write_step(1, "proj/b.md", "git status")])).await;
    assert_eq!(status, StatusCode::OK, "draft v2: {v2}");
    assert_eq!(v2["version"], 2);
    assert_eq!(v2["replan_count"], 0);
    assert_eq!(h.plan_by_id(&plan_id).await["status"], "superseded");
    // Triage runs on a FRESH task with no plan: the
    // Planner round answers no-change, so the run
    // completes with no Execution and no replan_*
    // Evidence (a task that already has a plan would
    // reuse it and execute, never triage).
    let triage_id = h.make_task().await;
    let (status, value) = h.post("/tasks/run", json!({"task_id": triage_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    h.wait_task_status(&triage_id, "completed", 30).await;
    assert!(h.executions_for_task(&triage_id).is_empty());
    let evidence = h.evidence().await;
    assert!(
        evidence
            .iter()
            .all(|e| !e["reason"].as_str().unwrap_or("").starts_with("replan_")),
        "triage records no replan Evidence: {evidence:?}"
    );
    assert!(
        evidence.iter().any(|e| e["reason"] == "no_file_change"),
        "the triage row is still there: {evidence:?}"
    );
}

// Harness helper used by H6 (executions of one task).
impl Harness {
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
}
