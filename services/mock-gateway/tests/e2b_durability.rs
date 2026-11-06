//! E2-B durability tests (plan §6, B1–B7): Worker lease /
//! heartbeat + sweeper + checkpoint readers + snapshot gate +
//! Windows T1 portable equivalent.
//!
//! HTTP routes only (plus `sqlite_store` reads after the fact,
//! never to fake a production step), temp dirs, real git where
//! the Worker path is exercised, a native fake `git` executable
//! (test fixture) where a run must outlive the test's patience.
//! Paths are compared canonicalized against canonicalized;
//! timing budgets are generous (cross-platform hygiene skill).
//!
//! Two deliberate exceptions, both time simulation, not
//! production-step fakes: B1/B2 write a stale heartbeat / lease
//! straight to the database to simulate 30+ s of driver
//! silence (waiting out a real 30 s lease per test would cost
//! a minute of CI per case; the lease arithmetic itself is
//! locked by the heartbeat observation in B1). The sweep
//! decision and its effects are exercised for real.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::deepseek_loop::DeepSeekLoopModel;
use mock_gateway::deepseek::DeepSeekConfig;
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-e2b-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_millis() as i64
}

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(
                replies
                    .into_iter()
                    .map(|r| (Duration::ZERO, Ok(r)))
                    .collect(),
            ),
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
        let (delay, reply) = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            reply
        })
    }
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    /// The AppState the server runs on (shared Arcs), kept so
    /// tests can call the in-process seams B1/B7 need (the
    /// sweep pass, the shutdown cleanup) without a restart.
    state: AppState,
    /// Real loop-model base URL when B5/B6 use it (None →
    /// ScriptModel). Rebuilt on restart.
    real_loop_model_url: Option<String>,
    git: Option<PathBuf>,
    cargo: Option<PathBuf>,
}

impl Harness {
    async fn serve() -> Self {
        Self::serve_in(temp_dir("harness"), None, None, None).await
    }

    /// Worker tests: real git/cargo or the native fake binaries.
    /// No loop replies needed.
    async fn serve_with_worker(git: Option<PathBuf>, cargo: Option<PathBuf>) -> Self {
        Self::serve_in(temp_dir("harness"), None, git, cargo).await
    }

    /// B5/B6: the REAL DeepSeekLoopModel pointed at a fake
    /// loop-model server, so the snapshot gate's deny check
    /// runs for real (a ScriptModel would never deny).
    async fn serve_with_real_loop_model(model_url: String) -> Self {
        Self::serve_in(temp_dir("harness"), Some(model_url), None, None).await
    }

    async fn serve_in(
        dir: PathBuf,
        real_loop_model_url: Option<String>,
        git: Option<PathBuf>,
        cargo: Option<PathBuf>,
    ) -> Self {
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let state = Self::build_state(
            &dir,
            real_loop_model_url.clone(),
            git.clone(),
            cargo.clone(),
            true,
        );
        let kept = state.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
            state: kept,
            real_loop_model_url,
            git,
            cargo,
        }
    }

    /// Shared state builder (e2a pattern). First build imports
    /// the (empty) JSONL files into SQLite and switches; a
    /// restart build (`fresh_import = false`) goes straight to
    /// SQLite mode via `with_sqlite_path`, which runs the
    /// recovery scan.
    fn build_state(
        dir: &Path,
        real_loop_model_url: Option<String>,
        git: Option<PathBuf>,
        cargo: Option<PathBuf>,
        fresh_import: bool,
    ) -> AppState {
        let root = dir.join("files");
        let model: Arc<dyn LoopModel> = match &real_loop_model_url {
            Some(base) => {
                let model = DeepSeekLoopModel::new(DeepSeekConfig {
                    base_url: base.clone(),
                    model: "fixture-loop".to_string(),
                    api_key: Some("fake-loop-key".to_string()),
                })
                .expect("loop model config")
                .expect("loop model with key");
                // No with_network_rules handle: enforcement for
                // loop calls comes from the per-call frozen
                // rules the engine resolves (E2-B §3.4), never
                // from a handle held here.
                Arc::new(model)
            }
            None => Arc::new(ScriptModel::new(Vec::new())),
        };
        let state = AppState::with_files_root_and_approvals(
            root.clone(),
            Some(dir.join("approvals.jsonl")),
        )
        .with_loop_backends(
            Some(dir.join("loops.jsonl")),
            Some(model),
            None,
            Timeouts::default(),
        )
        .unwrap()
        .with_auto_drive(false)
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_binding_state_path(dir.join("bindings.jsonl"))
        .with_project_state_path(dir.join("projects.jsonl"))
        .with_location_state_path(dir.join("locations.jsonl"))
        .with_worktree_state_path(dir.join("worktrees.jsonl"))
        .with_tasks_path(dir.join("tasks.jsonl"))
        .with_task_drafts_dir(dir.join("drafts"))
        .with_connector_state_path(dir.join("connectors.jsonl"))
        .with_git_path(git)
        .with_cargo_path(cargo);
        let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
        if fresh_import {
            state.sqlite_import_and_switch().expect("switch");
        }
        state
    }

    /// "Restart": a new AppState on the same database (scan +
    /// reconciliation run at construction).
    async fn restart(&mut self) {
        self.server.abort();
        let state = Self::build_state(
            &self.dir,
            self.real_loop_model_url.clone(),
            self.git.clone(),
            self.cargo.clone(),
            false,
        );
        self.state = state.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server =
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
    }

    fn db(&self) -> rusqlite::Connection {
        let conn = sqlite_store::open_db(&self.dir.join("gateway.sqlite3")).expect("open db");
        conn.busy_timeout(Duration::from_secs(5)).expect("busy timeout");
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

    async fn get_ok(&self, path: &str) -> Value {
        let (status, body) = self.get(path).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn start(&self, worktree_id: Option<&str>) -> String {
        let mut req = json!({"prompt": "edit the code"});
        if let Some(w) = worktree_id {
            req["worktree_id"] = json!(w);
        }
        self.ok("/agent/loop/start", req).await["loop_id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id": id})).await
    }

    /// Step until the loop reaches a terminal status (bounded).
    async fn drive(&self, id: &str) -> Value {
        let mut last = Value::Null;
        for _ in 0..16 {
            last = self.step(id).await;
            let status = last["status"].as_str().unwrap_or("");
            if matches!(status, "completed" | "failed" | "cancelled") {
                break;
            }
        }
        last
    }

    async fn effects(&self) -> Vec<Value> {
        self.get_ok("/effects/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn executions(&self) -> Vec<Value> {
        self.get_ok("/executions/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn execution(&self, id: &str) -> Value {
        self.get_ok(&format!("/executions/get?execution_id={id}"))
            .await
    }

    async fn execution_for_loop(&self, loop_id: &str) -> Value {
        self.executions()
            .await
            .into_iter()
            .find(|e| e["loop_id"] == loop_id)
            .unwrap_or_else(|| panic!("no execution for loop {loop_id}"))
    }

    async fn open_inbox(&self, kind: &str) -> Vec<Value> {
        self.get_ok(&format!("/inbox/list?kind={kind}&open_only=true"))
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn assignments(&self) -> Vec<Value> {
        self.get_ok("/assignments/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn assignment_for_execution(&self, execution_id: &str) -> Value {
        self.assignments()
            .await
            .into_iter()
            .find(|a| a["execution_id"] == execution_id)
            .unwrap_or_else(|| panic!("no assignment for execution {execution_id}"))
    }

    /// Takeover for an execution; returns the assignment row.
    async fn takeover(&self, execution_id: &str) -> Value {
        self.ok(
            "/assignments/takeover",
            json!({ "execution_id": execution_id }),
        )
        .await["assignment"]
            .clone()
    }

    /// Time simulation (see file header): pretend the driver
    /// holding this assignment went silent `silent_ms` ago.
    /// Writes the stale heartbeat / lease straight to the
    /// database; the sweep decision itself is exercised for
    /// real.
    fn simulate_silence(&self, assignment_id: &str, silent_ms: i64) {
        let now = now_ms();
        let conn = self.db();
        let changed = conn
            .execute(
                "UPDATE assignments SET heartbeat_ms = ?1, lease_expires_ms = ?2 WHERE assignment_id = ?3",
                rusqlite::params![now - silent_ms, now - 1_000, assignment_id],
            )
            .expect("stale heartbeat write");
        assert_eq!(changed, 1, "assignment {assignment_id} not found");
    }

    // --- Worker runs (B1/B3/B4) ---

    /// Binding + Location + Worktree for `repo`; returns the
    /// worktree id (p3c pattern).
    async fn setup_worktree(&self, repo: &Path) -> String {
        let repo_str = repo.to_string_lossy().into_owned();
        let binding = self
            .ok("/bindings/create", json!({"display_name": "repo", "path": repo_str}))
            .await;
        let workspace_id = binding["workspace_id"].as_str().unwrap().to_string();
        let location = self
            .ok(
                "/locations/create",
                json!({"workspace_id": workspace_id, "name": "root", "path": repo_str}),
            )
            .await;
        let location_id = location["location_id"].as_str().unwrap().to_string();
        let worktree = self
            .ok(
                "/worktrees/create",
                json!({"location_id": location_id, "directory_relative": "", "branch_label": "main", "project_id": null}),
            )
            .await;
        worktree["worktree_id"].as_str().unwrap().to_string()
    }

    /// Request a worker.run approval and approve it; returns
    /// the approval id. `timeout_ms` must match the timeout
    /// the run is created with — the digest commits it.
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
        self.get_ok(&format!("/worker/runs/{run_id}")).await
    }

    async fn wait_run(&self, run_id: &str, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let row = self.run(run_id).await;
            let status = row["status"].as_str().unwrap_or("");
            if mock_gateway::worker::is_terminal_run_status(status) {
                return row;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "run {run_id} stuck at {status}: {row}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll until the run leaves `queued` (the child spawned).
    async fn wait_run_started(&self, run_id: &str, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let row = self.run(run_id).await;
            if row["status"].as_str() != Some("queued") {
                return row;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "run {run_id} never started: {row}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// --- Fake loop model (B5/B6) ---
//
// A local HTTP service answering the loop model's
// completions call, never the real API and never a real
// credential. Unlike the e2a task fake, the message carries
// `role: "assistant"`: `parse_response` requires it, and a
// `finish_reason` of `stop` with non-empty content parses to
// `ModelReply::Final`, which completes the loop in one step.

async fn start_fake_loop_model(
    text: &str,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = serde_json::to_string(
        &json!({"choices": [{"message": {"role": "assistant", "content": text}, "finish_reason": "stop"}]}),
    )
    .unwrap();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                continue;
            };
            let body = body.clone();
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
                        for line in head.lines() {
                            if let Some(v) = line.strip_prefix("Content-Length:") {
                                content_length = v.trim().parse().unwrap_or(0);
                            }
                        }
                    }
                }
                let header_end = header_end.unwrap();
                while buf.len() < header_end + content_length {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    (addr, handle)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
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
    git(&["config", "user.name", "e2b-test"], &repo);
    std::fs::write(repo.join("a.txt"), "hello\n").unwrap();
    git(&["add", "-A"], &repo);
    git(&["commit", "-m", "init"], &repo);
    repo
}

/// Native fake `git`/`cargo` executable (test fixture, never
/// a production execution path). The fixture sleeps `secs`
/// seconds, optionally spawning a grandchild when `tree` is
/// true. Each test copies the same compiled product.
fn fake_binary(dir: &Path, stem: &str, secs: u64, tree: bool) -> PathBuf {
    static BINARY: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    let binary = BINARY.get_or_init(|| {
        let build = temp_dir("native-fixture");
        let output = build.join(format!("worker-fixture{}", std::env::consts::EXE_SUFFIX));
        let source =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/worker_process.rs");
        let result = std::process::Command::new(
            std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()),
        )
        .arg(source)
        .arg("-o")
        .arg(&output)
        .output()
        .expect("compile native worker fixture");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        output
    });
    let executable = dir.join(format!("{stem}{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(binary, &executable).unwrap();
    std::fs::write(executable.with_extension("config"), format!("{secs} {tree}")).unwrap();
    executable
}

fn fake_git_sleeping(dir: &Path) -> PathBuf {
    fake_binary(dir, "fake-git", 60, true)
}

fn real_cargo() -> PathBuf {
    mock_gateway::worker::resolve_cargo_path().expect("cargo on PATH")
}

/// Poll the assignments list until one assignment for the
/// execution reaches `status` (generous: the sweeper ticks
/// every 10 s).
async fn poll_assignment_status(h: &Harness, execution_id: &str, status: &str, secs: u64) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let row = h.assignment_for_execution(execution_id).await;
        if row["status"].as_str() == Some(status) {
            return row;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "assignment for {execution_id} stuck at {}: {row}",
            row["status"]
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// --- B1: heartbeat writer + expiry marks assignment lost ---
//
// A run driving a takeover-held assignment writes a heartbeat
// at start and every 5 s (lease = heartbeat + 30 s); once the
// driver goes silent past the lease, the sweep pass marks the
// assignment lost, its Execution `needs_review`, and opens
// the recovery-scan Inbox shape — all without a restart.

#[tokio::test]
async fn e2b_heartbeat_expiry_marks_assignment_lost() {
    let dir = temp_dir("b1");
    let repo = init_repo(&dir);
    let git = fake_git_sleeping(&dir);
    let h = Harness::serve_with_worker(Some(git), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;

    // The run needs a driving assignment: a loop's Execution
    // taken over before the run starts.
    let loop_id = h.start(None).await;
    let execution_id = h.execution_for_loop(&loop_id).await["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    let assignment = h.takeover(&execution_id).await;
    let assignment_id = assignment["assignment_id"].as_str().unwrap().to_string();

    let approval = h
        .approve_worker_run(&worktree, "git-status", json!({}), Some(15_000))
        .await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-status",
            "params": {},
            "timeout_ms": 15_000,
            "approval_id": approval,
            "execution_id": execution_id,
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    let run_id = run["run_id"].as_str().unwrap().to_string();
    let started = h.wait_run_started(&run_id, 10).await;
    assert_eq!(started["status"], "running", "{started}");

    // Heartbeat 1: the driver writes at start (poll until the
    // write lands — the claim precedes it by microseconds).
    let h1 = poll_heartbeat(&h, &execution_id, 10).await;
    // Heartbeat 2: the 5 s write advances it.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let row = h.assignment_for_execution(&execution_id).await;
    let h2 = row["heartbeat_ms"].as_i64().expect("heartbeat 2");
    let lease = row["lease_expires_ms"].as_i64().expect("lease 2");
    assert!(h2 > h1 + 3_000, "heartbeat did not advance: {h1} -> {h2}");
    assert_eq!(lease, h2 + 30_000, "lease is heartbeat + 30 s: {row}");

    // Silence past the lease (time simulation), then one sweep
    // pass in-process: no restart.
    h.simulate_silence(&assignment_id, 61_000);
    let swept = h.state.sweep_expired_assignments();
    assert_eq!(swept, 1, "one expired assignment swept");
    let row = h.assignment_for_execution(&execution_id).await;
    assert_eq!(row["status"], "lost", "{row}");
    assert_eq!(row["detail"], "heartbeat expired", "{row}");
    assert_eq!(h.execution(&execution_id).await["status"], "needs_review");
    let unknown = h.open_inbox("unknown_requires_reconciliation").await;
    assert!(
        unknown.iter().any(|r| r["execution_id"] == execution_id),
        "{unknown:?}"
    );
    let review = h.open_inbox("review_required").await;
    assert!(
        review.iter().any(|r| r["execution_id"] == execution_id),
        "{review:?}"
    );
}

/// Poll until the execution's assignment carries a heartbeat.
async fn poll_heartbeat(h: &Harness, execution_id: &str, secs: u64) -> i64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let row = h.assignment_for_execution(execution_id).await;
        if let Some(ms) = row["heartbeat_ms"].as_i64() {
            return ms;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no heartbeat written: {row}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// --- B2: the background sweeper marks expired without restart ---
//
// Same lost state as B1, but driven by the spawned background
// loop (10 s tick) in the live process — the point is that no
// restart is needed for expiry to converge.

#[tokio::test]
async fn e2b_sweeper_marks_expired_without_restart() {
    let h = Harness::serve().await;
    let loop_id = h.start(None).await;
    let execution_id = h.execution_for_loop(&loop_id).await["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    let assignment = h.takeover(&execution_id).await;
    let assignment_id = assignment["assignment_id"].as_str().unwrap().to_string();
    // A takeover-held assignment never heartbeats (no driver),
    // so simulate a driver that held this lease and went silent.
    h.simulate_silence(&assignment_id, 61_000);

    let sweeper = mock_gateway::server::spawn_assignment_sweeper(h.state.clone());
    let row = poll_assignment_status(&h, &execution_id, "lost", 30).await;
    sweeper.abort();
    assert_eq!(row["detail"], "heartbeat expired", "{row}");
    assert_eq!(h.execution(&execution_id).await["status"], "needs_review");
    let unknown = h.open_inbox("unknown_requires_reconciliation").await;
    assert!(
        unknown.iter().any(|r| r["execution_id"] == execution_id),
        "{unknown:?}"
    );
    let review = h.open_inbox("review_required").await;
    assert!(
        review.iter().any(|r| r["execution_id"] == execution_id),
        "{review:?}"
    );
    // The sweep leaves the ledger foldable (events match
    // tables exactly, as the recovery scan's sweep does).
    let conn = h.db();
    assert!(
        sqlite_store::fold_matches_tables(&conn).expect("fold"),
        "fold broke after sweep"
    );
}

// --- B3: takeover reads the plan position ---
//
// A timed-out run writes a completion checkpoint for its
// Execution; the takeover that follows reads
// `latest_plan_position` and carries the resume point in the
// new assignment's detail (the `assignment.created` event
// payload), while writing its own fresh checkpoint.

#[tokio::test]
async fn e2b_takeover_reads_plan_position() {
    let dir = temp_dir("b3");
    let repo = init_repo(&dir);
    let git = fake_git_sleeping(&dir);
    let h = Harness::serve_with_worker(Some(git), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;

    let approval = h
        .approve_worker_run(&worktree, "git-status", json!({}), Some(2000))
        .await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-status",
            "params": {},
            "timeout_ms": 2000,
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    let run_id = run["run_id"].as_str().unwrap().to_string();
    let execution_id = run["execution_id"].as_str().unwrap().to_string();
    let row = h.wait_run(&run_id, 30).await;
    assert_eq!(row["status"], "timeout", "{row}");
    assert_eq!(h.execution(&execution_id).await["status"], "needs_review");

    let conn = h.db();
    let checkpoint = sqlite_store::latest_plan_position(&conn, &execution_id)
        .expect("latest checkpoint")
        .expect("timeout run wrote a checkpoint");
    drop(conn);

    let assignment = h.takeover(&execution_id).await;
    assert_eq!(
        assignment["detail"].as_str(),
        Some(checkpoint.payload_json.as_str()),
        "takeover carries the previous plan position: {assignment}"
    );
    // The takeover wrote its own fresh checkpoint on top.
    let conn = h.db();
    let fresh = sqlite_store::latest_plan_position(&conn, &execution_id)
        .expect("fresh checkpoint")
        .expect("takeover wrote a checkpoint");
    assert_ne!(fresh.checkpoint_id, checkpoint.checkpoint_id);
    assert_eq!(fresh.kind, "execution.plan_position");
}

// --- B4: the recovery scan reads the plan position ---
//
// A checkpoint exists (from a takeover) before a run goes
// stale; after a restart, the scan's unknown Effect carries
// the resume hint in its detail.

#[tokio::test]
async fn e2b_recovery_scan_reads_plan_position() {
    let dir = temp_dir("b4");
    let repo = init_repo(&dir);
    let git = fake_git_sleeping(&dir);
    let mut h = Harness::serve_with_worker(Some(git), Some(real_cargo())).await;
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
    let execution_id = run["execution_id"].as_str().unwrap().to_string();
    let started = h.wait_run_started(&run_id, 10).await;
    assert_eq!(started["status"], "running", "{started}");

    // A checkpoint for this execution, written by a takeover
    // while the run is still in flight (no Effects yet, so
    // the position is "end, 0 completed, none pending").
    h.takeover(&execution_id).await;

    h.restart().await;
    let row = h.run(&run_id).await;
    assert_eq!(row["status"], "orphaned", "{row}");
    assert_eq!(h.execution(&execution_id).await["status"], "needs_review");
    let effect_id = row["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .effects()
        .await
        .into_iter()
        .find(|e| e["effect_id"] == effect_id)
        .expect("scan effect");
    assert_eq!(effect["status"], "unknown");
    let detail = effect["detail"].as_str().unwrap_or("");
    assert!(
        detail.contains("resume at end (0 completed, none pending)"),
        "scan effect carries the resume hint: {detail}"
    );
}

// --- B5: snapshot gate — old Execution runs per snapshot ---
//
// A loop started under empty rules keeps running them after a
// deny rule for the model host is added: the model call
// succeeds and the loop completes.

#[tokio::test]
async fn e2b_snapshot_gate_old_execution_uses_snapshot() {
    let (addr, _model) = start_fake_loop_model("done, all good").await;
    let model_url = format!("http://{addr}");
    let h = Harness::serve_with_real_loop_model(model_url).await;
    let loop_id = h.start(None).await;

    // Policy change AFTER the Execution (and its snapshot)
    // was created: deny the model host.
    let (status, _) = h
        .patch(
            "/permissions/network-rules",
            json!({"rules": [{"pattern": "127.0.0.1", "action": "deny"}]}),
        )
        .await;
    assert_eq!(status, 200);

    let view = h.drive(&loop_id).await;
    assert_eq!(view["status"], "completed", "{view}");
}

// --- B6: snapshot gate — new Execution runs per live policy ---
//
// The same deny rule, set BEFORE the loop starts, is frozen
// into the new Execution's snapshot: its model call is denied
// and the loop fails.

#[tokio::test]
async fn e2b_snapshot_gate_new_execution_uses_live_policy() {
    let (addr, _model) = start_fake_loop_model("done, all good").await;
    let model_url = format!("http://{addr}");
    let h = Harness::serve_with_real_loop_model(model_url).await;

    let (status, _) = h
        .patch(
            "/permissions/network-rules",
            json!({"rules": [{"pattern": "127.0.0.1", "action": "deny"}]}),
        )
        .await;
    assert_eq!(status, 200);

    let loop_id = h.start(None).await;
    let view = h.drive(&loop_id).await;
    assert_eq!(view["status"], "failed", "{view}");
}

// --- B7: Windows T1 portable equivalent ---
//
// The same shutdown-cleanup logic T1 covers via a unix SIGINT,
// exercised in-process on every platform (Windows included):
// arrange a takeover-held assignment over HTTP, call
// `sqlite_shutdown_cleanup` directly, and assert the same
// post-shutdown state T1 asserts — workers ended with
// "clean shutdown", the assignment lost with "worker stopped".

#[tokio::test]
async fn e2b_windows_t1() {
    let h = Harness::serve().await;
    let loop_id = h.start(None).await;
    let execution_id = h.execution_for_loop(&loop_id).await["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    let assignment = h.takeover(&execution_id).await;
    let assignment_id = assignment["assignment_id"].as_str().unwrap().to_string();

    h.state.sqlite_shutdown_cleanup();

    let conn = h.db();
    let workers = sqlite_store::list_worker_sessions(&conn).expect("workers");
    assert!(!workers.is_empty());
    assert!(
        workers.iter().all(|w| w.status == "ended"),
        "{workers:?}"
    );
    assert!(
        workers.iter().all(|w| w.detail.as_deref() == Some("clean shutdown")),
        "{workers:?}"
    );
    let assignments = sqlite_store::list_assignments(&conn).expect("assignments");
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].assignment_id, assignment_id);
    assert_eq!(assignments[0].status, "lost");
    assert_eq!(assignments[0].detail.as_deref(), Some("worker stopped"));
}
