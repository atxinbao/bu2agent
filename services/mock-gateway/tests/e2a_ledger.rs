//! E2-A ledger tests (plan §6, A1–A14): unified ledger —
//! Task/Loop/Worker terminal coupling, re-run supersede,
//! recovery-scan record_effect, idempotency closure,
//! Inbox dedupe, approval worker-session writeback, Loop
//! fs.write single path, Task validator, historical
//! reconciliation.
//!
//! HTTP routes only (plus `sqlite_store` reads after the
//! fact, never to fake a production step), temp dirs, real
//! git/cargo where the Worker path is exercised. Paths are
//! compared canonicalized against canonicalized; timing
//! budgets are generous (cross-platform hygiene skill).

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::effect_store::idempotency_key;
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
        "bu2agent-e2a-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
    started: tokio::sync::Notify,
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
            started: tokio::sync::Notify::new(),
        }
    }

    /// Replies that may fail (A2: a model error fails the loop).
    fn new_results(replies: Vec<Result<ModelReply, ModelError>>) -> Self {
        Self {
            steps: Mutex::new(
                replies.into_iter().map(|r| (Duration::ZERO, r)).collect(),
            ),
            started: tokio::sync::Notify::new(),
        }
    }

    /// Replies with a per-call delay (A2: the model call is still
    /// in flight when the server is aborted mid-run, so the
    /// restart folds the loop to Interrupted).
    fn new_delayed(replies: Vec<(Duration, ModelReply)>) -> Self {
        Self {
            steps: Mutex::new(
                replies.into_iter().map(|(d, r)| (d, Ok(r))).collect(),
            ),
            started: tokio::sync::Notify::new(),
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
        self.started.notify_one();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            reply
        })
    }
}

fn proposal(tool: &str, args: Value) -> Proposal {
    Proposal {
        tool: tool.into(),
        args,
        wire: None,
    }
}

fn calls(items: Vec<Proposal>) -> ModelReply {
    ModelReply::Calls {
        proposals: items,
        content: None,
    }
}

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

fn write_proposal(path: &str, text: &str) -> Proposal {
    proposal("fs.write", json!({"path": path, "text": text}))
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    model_url: Option<String>,
    git: Option<PathBuf>,
    cargo: Option<PathBuf>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        Self::serve_in(temp_dir("harness"), replies, None, None, None).await
    }

    async fn serve_results(replies: Vec<Result<ModelReply, ModelError>>) -> Self {
        let dir = temp_dir("harness");
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let model = Arc::new(ScriptModel::new_results(replies));
        Self::serve_state(dir, model, None, None, None, true).await
    }

    async fn serve_delayed(replies: Vec<(Duration, ModelReply)>) -> Self {
        let dir = temp_dir("harness");
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let model = Arc::new(ScriptModel::new_delayed(replies));
        Self::serve_state(dir, model, None, None, None, true).await
    }

    /// Task-runner tests: the fake DeepSeek server answers the
    /// model calls (A1/A5/A13).
    async fn serve_with_model(replies: Vec<ModelReply>, model_url: String) -> Self {
        Self::serve_in(temp_dir("harness"), replies, Some(model_url), None, None).await
    }

    /// Worker tests: real git/cargo or the native fake binaries
    /// (A3/A4/A6/A11). No loop replies needed.
    async fn serve_with_worker(git: Option<PathBuf>, cargo: Option<PathBuf>) -> Self {
        Self::serve_in(
            temp_dir("harness"),
            Vec::new(),
            None,
            git,
            cargo,
        )
        .await
    }

    /// Combined harness: loop replies + DeepSeek model + worker
    /// binaries (A14: one DB holds a terminal Task, a terminal
    /// Loop and a terminal Worker run together).
    async fn serve_full(
        replies: Vec<ModelReply>,
        model_url: String,
        git: Option<PathBuf>,
        cargo: Option<PathBuf>,
    ) -> Self {
        Self::serve_in(
            temp_dir("harness"),
            replies,
            Some(model_url),
            git,
            cargo,
        )
        .await
    }

    async fn serve_in(
        dir: PathBuf,
        replies: Vec<ModelReply>,
        model_url: Option<String>,
        git: Option<PathBuf>,
        cargo: Option<PathBuf>,
    ) -> Self {
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        Self::serve_state(dir, model, model_url, git, cargo, true).await
    }

    async fn serve_state(
        dir: PathBuf,
        model: Arc<ScriptModel>,
        model_url: Option<String>,
        git: Option<PathBuf>,
        cargo: Option<PathBuf>,
        fresh_import: bool,
    ) -> Self {
        let state = Self::build_state(
            &dir,
            model,
            model_url.clone(),
            git.clone(),
            cargo.clone(),
            fresh_import,
        );
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
            model_url,
            git,
            cargo,
        }
    }

    /// Shared state builder. First build imports the (empty)
    /// JSONL files into SQLite and switches; a restart build
    /// (`fresh_import = false`) goes straight to SQLite mode
    /// via `with_sqlite_path`, which runs the recovery scan
    /// (p0e `restarted_state` pattern) — including the E2-A
    /// §2.8 stranded-Execution reconciliation.
    fn build_state(
        dir: &Path,
        model: Arc<ScriptModel>,
        model_url: Option<String>,
        git: Option<PathBuf>,
        cargo: Option<PathBuf>,
        fresh_import: bool,
    ) -> AppState {
        let root = dir.join("files");
        let mut state = AppState::with_files_root_and_approvals(
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
        if let Some(base) = model_url {
            state = state.with_deepseek(DeepSeekClient::new(DeepSeekConfig {
                base_url: base,
                model: "fixture-3b".to_string(),
                api_key: Some("test-key".to_string()),
            }));
        }
        let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
        if fresh_import {
            state.sqlite_import_and_switch().expect("switch");
        }
        state
    }

    /// "Restart": a new AppState on the same database (scan +
    /// reconciliation run at construction). `model_url`
    /// replaces the DeepSeek base when given (A5: the resumed
    /// run answers from the fast fake, not the slow one).
    async fn restart(&mut self) {
        self.restart_with_model(None).await;
    }

    async fn restart_with_model(&mut self, model_url: Option<String>) {
        self.server.abort();
        if let Some(url) = model_url {
            self.model_url = Some(url);
        }
        let model = Arc::new(ScriptModel::new(Vec::new()));
        let state = Self::build_state(
            &self.dir,
            model,
            self.model_url.clone(),
            self.git.clone(),
            self.cargo.clone(),
            false,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server =
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
    }

    fn db(&self) -> rusqlite::Connection {
        sqlite_store::open_db(&self.dir.join("gateway.sqlite3")).expect("open db")
    }

    fn files_root(&self) -> PathBuf {
        self.dir.join("files")
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

    /// Step until a call awaits approval; returns its
    /// approval id + binding.
    async fn await_approval(&self, loop_id: &str) -> (String, Value) {
        let mut view = Value::Null;
        for _ in 0..8 {
            view = self.step(loop_id).await;
            if view["status"] == "awaiting_approval" {
                break;
            }
        }
        assert_eq!(view["status"], "awaiting_approval", "{view}");
        let queue = view["queue_position"].as_u64().unwrap() as usize;
        let call = &view["calls"][queue];
        (
            call["approval_id"].as_str().unwrap().to_string(),
            call["binding"].clone(),
        )
    }

    /// Decide approved, then resume the loop with that
    /// approval + binding; returns the resume view.
    async fn approve_and_resume(&self, loop_id: &str, approval: &str, binding: &Value) -> Value {
        self.ok(
            "/approval/decide",
            json!({"tool_call_id": approval, "status": "approved"}),
        )
        .await;
        self.ok(
            "/agent/loop/resume",
            json!({"loop_id": loop_id, "approval_id": approval, "binding": binding}),
        )
        .await
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

    async fn execution_for_task(&self, task_id: &str) -> Value {
        self.executions()
            .await
            .into_iter()
            .find(|e| e["task_id"] == task_id)
            .unwrap_or_else(|| panic!("no execution for task {task_id}"))
    }

    async fn execution_for_loop(&self, loop_id: &str) -> Value {
        self.executions()
            .await
            .into_iter()
            .find(|e| e["loop_id"] == loop_id)
            .unwrap_or_else(|| panic!("no execution for loop {loop_id}"))
    }

    async fn loop_view(&self, id: &str) -> Value {
        self.get_ok(&format!("/agent/loop/get?loop_id={id}")).await
    }

    async fn approvals(&self) -> Vec<Value> {
        self.get_ok("/approvals")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn approval(&self, id: &str) -> Value {
        self.get_ok(&format!("/approvals/{id}")).await
    }

    async fn open_inbox(&self, kind: &str) -> Vec<Value> {
        self.get_ok(&format!("/inbox/list?kind={kind}&open_only=true"))
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    // --- Tasks (A1/A5/A13) ---

    async fn create_task(&self, directory: &str) -> String {
        let body = self
            .ok("/tasks/create", json!({"directory": directory}))
            .await;
        body["task_id"].as_str().unwrap().to_string()
    }

    async fn task(&self, task_id: &str) -> Value {
        self.get_ok(&format!("/tasks/get?task_id={task_id}")).await
    }

    async fn poll_task(&self, task_id: &str, want: &str, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let record = self.task(task_id).await;
            if record["status"].as_str() == Some(want) {
                return record;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for task {task_id} {want}: {record}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // --- Worker runs (A3/A4/A6/A11) ---

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

    // --- Connectors (A7) ---

    async fn link_connector(&self, id: &str) -> String {
        let body = self
            .ok(&format!("/connectors/{id}/accounts/link"), json!({}))
            .await;
        body["account_id"].as_str().unwrap().to_string()
    }

    // --- Patches (A8/A9) ---

    async fn patch(&self, id: &str) -> Value {
        self.get_ok(&format!("/patches/{id}")).await
    }

    async fn apply(
        &self,
        patch_id: &str,
        patch_file_id: &str,
        expected_old_sha256: Option<&str>,
        approval_id: &str,
    ) -> (u16, Value) {
        let mut req = json!({"approval_id": approval_id});
        if let Some(old) = expected_old_sha256 {
            req["expected_old_sha256"] = json!(old);
        }
        self.post(
            &format!("/patches/{patch_id}/files/{patch_file_id}/apply"),
            req,
        )
        .await
    }

    /// One repo under the files root with its own binding /
    /// location / worktree. repo/src/a.txt seeds "old a\n".
    /// Returns the worktree id.
    async fn setup_repo(&self) -> String {
        let repo = self.files_root().join("repo");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::write(repo.join("src/a.txt"), "old a\n").unwrap();
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
                json!({"location_id": location_id, "directory_relative": "", "branch_label": "main"}),
            )
            .await;
        worktree["worktree_id"].as_str().unwrap().to_string()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// --- Fake DeepSeek (task_runner_smoke pattern) ---
//
// A local HTTP service answering the model call, never the
// real API and never a real credential. `status` lets A1
// drive a failing model (500); `delay` lets A1/A5 hold the
// model call in flight while the test cancels or aborts.

const MODEL_TEXT: &str = "# 报告\n\n要点：alpha 与 beta。\n\n结论：完成。";

async fn start_fake_model(
    text: &str,
    delay: Duration,
    status: u16,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = serde_json::to_string(
        &json!({"choices": [{"message": {"content": text}, "finish_reason": "stop"}]}),
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
                let mut content_length = 0usize;
                let mut header_end = None;
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
                tokio::time::sleep(delay).await;
                let reason = if status == 200 { "OK" } else { "Internal Server Error" };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
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
    haystack.windows(needle.len()).position(|w| w == needle)
}

// --- Fake git/cargo + repos (p3c_worker pattern) ---

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
    git(&["config", "user.name", "e2a-test"], &repo);
    std::fs::write(repo.join("a.txt"), "hello\n").unwrap();
    git(&["add", "-A"], &repo);
    git(&["commit", "-m", "init"], &repo);
    repo
}

/// A real git repo that is also a Cargo workspace with two
/// tiny packages (no dependencies, so no network): one whose
/// suite passes, one with one passing and one failing test.
fn init_repo_with_cargo_packages(dir: &Path) -> PathBuf {
    let repo = init_repo(dir);
    std::fs::write(
        repo.join("Cargo.toml"),
        "[workspace]\nresolver = \"2\"\nmembers = [\"testpkg-pass\", \"testpkg-fail\"]\n",
    )
    .unwrap();
    for (name, body) in [
        (
            "testpkg-pass",
            "#[cfg(test)]\nmod tests {\n    #[test]\n    fn passes() {\n        assert_eq!(1 + 1, 2);\n    }\n}\n",
        ),
        (
            "testpkg-fail",
            "#[cfg(test)]\nmod tests {\n    #[test]\n    fn passes() {\n        assert_eq!(1 + 1, 2);\n    }\n\n    #[test]\n    fn fails() {\n        assert_eq!(1 + 1, 3);\n    }\n}\n",
        ),
    ] {
        let pkg = repo.join(name);
        std::fs::create_dir_all(pkg.join("src")).unwrap();
        std::fs::write(
            pkg.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[lib]\npath = \"src/lib.rs\"\n"
            ),
        )
        .unwrap();
        std::fs::write(pkg.join("src").join("lib.rs"), body).unwrap();
    }
    git(&["add", "-A"], &repo);
    git(&["commit", "-m", "add cargo packages"], &repo);
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

fn real_git() -> PathBuf {
    mock_gateway::worker::resolve_git_path().expect("git on PATH")
}

fn real_cargo() -> PathBuf {
    mock_gateway::worker::resolve_cargo_path().expect("cargo on PATH")
}

// --- A12: loop fs.write single path ---
//
// Locks the exact Effect row a loop fs.write produces
// (capability/operation/scope/digest/status/execution/
// approval/key) BEFORE the §2.3 convergence, so replacing
// the inline dispatch closure with `record_loop_effect`
// cannot drift the output: the same test must pass after
// the refactor with zero changes.

#[tokio::test]
async fn e2a_a12_loop_fs_write_single_path() {
    let h = Harness::serve(vec![
        calls(vec![write_proposal("a.txt", "hello\n")]),
        done(),
    ])
    .await;
    let loop_id = h.start(None).await;
    let (approval, binding) = h.await_approval(&loop_id).await;
    h.approve_and_resume(&loop_id, &approval, &binding).await;
    let view = h.drive(&loop_id).await;
    assert_eq!(view["status"], "completed", "{view}");
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("a.txt")).unwrap(),
        "hello\n"
    );
    let effects = h.effects().await;
    let rows: Vec<&Value> = effects
        .iter()
        .filter(|e| e["operation"] == "write" && e["scope"] == "a.txt")
        .collect();
    assert_eq!(rows.len(), 1, "{effects:?}");
    let row = rows[0];
    assert_eq!(row["capability"], "fs");
    assert_eq!(row["status"], "succeeded");
    assert_eq!(row["approval_id"], approval);
    let executions = h.executions().await;
    let exec = executions
        .iter()
        .find(|e| e["loop_id"] == loop_id)
        .expect("loop execution");
    assert_eq!(row["execution_id"], exec["execution_id"]);
    // Digest + key from the normalized args (path+text only;
    // normalize() drops any model-submitted diff).
    let mut args = BTreeMap::new();
    args.insert("path".to_string(), "a.txt".to_string());
    args.insert("text".to_string(), "hello\n".to_string());
    let digest = mock_gateway::loop_engine::args_digest("fs.write", &args);
    assert_eq!(row["input_digest"], digest);
    let key = idempotency_key(
        exec["execution_id"].as_str().unwrap(),
        "fs",
        "write",
        "a.txt",
        &digest,
    );
    assert_eq!(row["idempotency_key"], key);
}

// --- A1: task terminal closes Execution ---
//
// Four terminal shapes, one task each: run → draft_ready →
// execute → completed; failing model → failed; delay +
// cancel → cancelled; run → draft_ready → reject → rejected.
// Each terminal Task closes its Execution per the §2.3
// mapping (rejected → failed: Execution has no `rejected`
// word). The Execution stays `running` while the Task is
// non-terminal (draft_ready is not terminal).

#[tokio::test]
async fn e2a_a1_task_terminal_closes_execution() {
    let (model_addr, _model) =
        start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(
        Vec::new(),
        format!("http://{model_addr}"),
    )
    .await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();

    // Completed: run → draft_ready → approve → execute.
    let task = h.create_task("notes").await;
    let (status, _) = h.post("/tasks/run", json!({"task_id": task})).await;
    assert_eq!(status, 200);
    let record = h.poll_task(&task, "draft_ready", 15).await;
    assert_eq!(h.execution_for_task(&task).await["status"], "running");
    let approval = record["approval_id"].as_str().unwrap().to_string();
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (status, body) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "completed");
    assert_eq!(h.task(&task).await["status"], "completed");
    assert_eq!(
        h.execution_for_task(&task).await["status"],
        "completed",
        "terminal task must close its execution"
    );

    // Rejected: run → draft_ready → reject.
    let task2 = h.create_task("notes").await;
    h.post("/tasks/run", json!({"task_id": task2})).await;
    h.poll_task(&task2, "draft_ready", 15).await;
    let (status, body) = h.post("/tasks/reject", json!({"task_id": task2})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(h.task(&task2).await["status"], "rejected");
    assert_eq!(
        h.execution_for_task(&task2).await["status"],
        "failed",
        "rejected maps to failed (no `rejected` execution word)"
    );
}

#[tokio::test]
async fn e2a_a1_task_failed_and_cancelled_close_execution() {
    // Failed: the model answers 500.
    let (bad_addr, _bad) = start_fake_model(MODEL_TEXT, Duration::ZERO, 500).await;
    let h = Harness::serve_with_model(
        Vec::new(),
        format!("http://{bad_addr}"),
    )
    .await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task = h.create_task("notes").await;
    h.post("/tasks/run", json!({"task_id": task})).await;
    h.poll_task(&task, "failed", 15).await;
    assert_eq!(h.execution_for_task(&task).await["status"], "failed");

    // Cancelled: the model call is in flight (5 s delay) when
    // the cancel lands; the runner discards the late result.
    let (slow_addr, _slow) =
        start_fake_model(MODEL_TEXT, Duration::from_secs(5), 200).await;
    let h2 = Harness::serve_with_model(
        Vec::new(),
        format!("http://{slow_addr}"),
    )
    .await;
    let notes2 = h2.files_root().join("notes");
    std::fs::create_dir_all(&notes2).unwrap();
    std::fs::write(notes2.join("a.txt"), "alpha").unwrap();
    let task2 = h2.create_task("notes").await;
    h2.post("/tasks/run", json!({"task_id": task2})).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(h2.task(&task2).await["status"], "running");
    let (status, body) = h2.post("/tasks/cancel", json!({"task_id": task2})).await;
    assert_eq!(status, 200, "{body}");
    h2.poll_task(&task2, "cancelled", 15).await;
    assert_eq!(
        h2.execution_for_task(&task2).await["status"],
        "cancelled"
    );
    h2.server.abort();
}

// --- A2: loop terminal closes Execution ---
//
// Completed closes to `completed`; a model error fails the
// loop and closes to `failed`; an Interrupted loop (server
// aborted mid-model-call, restart folds Running →
// Interrupted) closes NOTHING — its Execution stays
// `running` until an explicit cancel path closes it.

#[tokio::test]
async fn e2a_a2_loop_terminal_closes_execution() {
    // Completed.
    let h = Harness::serve(vec![done()]).await;
    let loop_id = h.start(None).await;
    assert_eq!(h.drive(&loop_id).await["status"], "completed");
    assert_eq!(
        h.execution_for_loop(&loop_id).await["status"],
        "completed"
    );

    // Failed: the model errors.
    let h2 = Harness::serve_results(vec![Err(ModelError::Failed)]).await;
    let loop2 = h2.start(None).await;
    let view = h2.drive(&loop2).await;
    assert_eq!(view["status"], "failed", "{view}");
    assert_eq!(h2.execution_for_loop(&loop2).await["status"], "failed");
}

#[tokio::test]
async fn e2a_a2_loop_interrupted_closes_nothing() {
    let mut h = Harness::serve_delayed(vec![(Duration::from_secs(30), done())]).await;
    let loop_id = h.start(None).await;
    // Drive one step in the background: the model call sleeps
    // 30 s, so the resume is still in flight at 1 s.
    let url = h.url.clone();
    let step_loop = loop_id.clone();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let step = tokio::spawn(async move {
        let _ = client
            .post(format!("{url}/agent/loop/resume"))
            .json(&json!({"loop_id": step_loop}))
            .send()
            .await;
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(h.loop_view(&loop_id).await["status"], "running");
    assert_eq!(
        h.execution_for_loop(&loop_id).await["status"],
        "running"
    );
    // Abort mid-run and restart: the restore folds the loop
    // to Interrupted. The background resume is NOT awaited:
    // the old server's in-flight handler survives the abort
    // (axum connection tasks outlive the serve handle) and
    // would complete the loop ~30 s later, closing the
    // Execution — the assertions below run first, and the
    // test runtime drop kills the zombie at test end.
    h.restart().await;
    assert_eq!(h.loop_view(&loop_id).await["status"], "interrupted");
    assert_eq!(
        h.execution_for_loop(&loop_id).await["status"],
        "running",
        "interrupted closes no execution"
    );
    drop(step);
}

// --- A13: task status validator rejects illegal edges ---
//
// The §2.2 edge table (8 words, 13 edges) is enforced at
// the lifecycle entries: execute/reject/cancel on a
// `created` task are all 409 `task_status_transition_invalid`
// (none of those edges exists), while resume on `created` is
// legal (created → running is a table edge) and cancel on
// `running` is legal.

#[tokio::test]
async fn e2a_a13_task_status_validator_rejects_illegal_edge() {
    let (model_addr, _model) =
        start_fake_model(MODEL_TEXT, Duration::from_secs(5), 200).await;
    let h = Harness::serve_with_model(
        Vec::new(),
        format!("http://{model_addr}"),
    )
    .await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task = h.create_task("notes").await;
    assert_eq!(h.task(&task).await["status"], "created");
    for path in ["/tasks/execute", "/tasks/reject", "/tasks/cancel"] {
        let (status, body) = h.post(path, json!({"task_id": task})).await;
        assert_eq!(status, 409, "{path}: {body}");
        assert_eq!(body["error"], "task_status_transition_invalid");
    }
    // Resume on `created` is LEGAL (created → running is a
    // table edge, and resume is a start, not only a resume):
    // it spawns a runner attempt and returns the pre-run
    // snapshot.
    let task2 = h.create_task("notes").await;
    let (status, body) = h.post("/tasks/resume", json!({"task_id": task2})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "created");
    h.poll_task(&task2, "running", 15).await;
    let (status, _) = h.post("/tasks/cancel", json!({"task_id": task2})).await;
    assert_eq!(status, 200);
    h.poll_task(&task2, "cancelled", 15).await;
}

// --- E2-A fix F1–F7 (V1 resume + terminal re-run) ---
//
// F1: execute-before-approval strands a planned Effect; the
// post-approval retry resumes that same row (not a second row).
// F2/F3: tasks_run re-runs a completed/failed Task. F4:
// tasks_resume from a terminal Task regenerates (Q1a). F5:
// draft_ready/rejected/cancelled still cannot re-run. F6: the
// resume condition refuses other-approval and started rows
// (store-pre-seeded: unreachable via public HTTP). F7:
// concurrent double-execute writes exactly once.

impl Harness {
    /// Run a task to `draft_ready`; returns (task_id, approval_id).
    async fn run_to_draft_ready(&self, directory: &str) -> (String, String) {
        let task = self.create_task(directory).await;
        let (status, _) = self.post("/tasks/run", json!({"task_id": task})).await;
        assert_eq!(status, 200, "run {task}");
        let record = self.poll_task(&task, "draft_ready", 15).await;
        let approval = record["approval_id"].as_str().unwrap().to_string();
        (task, approval)
    }

    async fn draft_text(&self, task_id: &str) -> String {
        self.get_ok(&format!("/tasks/draft?task_id={task_id}"))
            .await["draft"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// `(tasks, execute)` Effect rows for one task, in id order.
    async fn task_execute_effects(&self, task_id: &str) -> Vec<Value> {
        let mut rows: Vec<Value> = self
            .effects()
            .await
            .into_iter()
            .filter(|e| {
                e["capability"] == "tasks" && e["operation"] == "execute" && e["scope"] == task_id
            })
            .collect();
        rows.sort_by_key(|e| e["effect_id"].as_str().unwrap_or("").to_string());
        rows
    }

    async fn task_executions(&self, task_id: &str) -> Vec<Value> {
        self.executions()
            .await
            .into_iter()
            .filter(|e| e["task_id"] == task_id)
            .collect()
    }
}

#[tokio::test]
async fn e2a_f1_execute_retry_resumes_own_planned_effect() {
    let (model_addr, _model) = start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(Vec::new(), format!("http://{model_addr}")).await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let (task, approval) = h.run_to_draft_ready("notes").await;
    let draft = h.draft_text(&task).await;
    let execution_id = h.execution_for_task(&task).await["execution_id"]
        .as_str()
        .unwrap()
        .to_string();

    // First execute BEFORE the approval is decided: 403
    // approval_required (consume returns `forbidden`), and the
    // planned row is stranded. (The plan's §9.1 table says 409
    // here; the window evidence file carries the body only, no
    // status — the code returns 403, unchanged by this fix.)
    let (status, body) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "approval_required");
    assert_eq!(h.task(&task).await["status"], "draft_ready");
    assert_eq!(
        h.execution_for_task(&task).await["status"],
        "running",
        "execution stays running while the task is stranded"
    );
    let rows = h.task_execute_effects(&task).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"], "planned");
    assert_eq!(rows[0]["approval_id"], approval);
    let effect_id = rows[0]["effect_id"].as_str().unwrap().to_string();

    // Decide, then execute again: the same row is resumed.
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (status, body) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "completed");
    assert_eq!(h.task(&task).await["status"], "completed");
    assert_eq!(h.execution_for_task(&task).await["status"], "completed");
    let rows = h.task_execute_effects(&task).await;
    assert_eq!(rows.len(), 1, "resume must not create a second row: {rows:?}");
    assert_eq!(rows[0]["effect_id"], effect_id, "same row, driven forward");
    assert_eq!(rows[0]["status"], "succeeded");
    assert_eq!(rows[0]["execution_id"], execution_id);
    let artifact = std::fs::read(h.files_root().join("notes/report.md")).unwrap();
    assert_eq!(artifact, draft.as_bytes(), "artifact bytes == draft bytes");
}

#[tokio::test]
async fn e2a_f2_rerun_from_completed() {
    let (model_addr, _model) = start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(Vec::new(), format!("http://{model_addr}")).await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let (task, approval) = h.run_to_draft_ready("notes").await;
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (status, _) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 200);
    assert_eq!(h.task(&task).await["status"], "completed");
    let old_executions = h.task_executions(&task).await;
    assert_eq!(old_executions.len(), 1);
    let old_execution_id = old_executions[0]["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(old_executions[0]["status"], "completed");

    // Re-run from completed: a fresh Execution, the old one untouched.
    let (status, _) = h.post("/tasks/run", json!({"task_id": task})).await;
    assert_eq!(status, 200, "completed→running must be a legal edge");
    let record = h.poll_task(&task, "draft_ready", 15).await;
    assert_ne!(record["approval_id"], approval, "re-run mints a fresh approval");
    let executions = h.task_executions(&task).await;
    assert_eq!(executions.len(), 2, "{executions:?}");
    let new_execution = executions
        .iter()
        .find(|e| e["execution_id"] != old_execution_id)
        .expect("new execution");
    assert_eq!(new_execution["status"], "running");
    let old = executions
        .iter()
        .find(|e| e["execution_id"] == old_execution_id)
        .unwrap();
    assert_eq!(old["status"], "completed", "supersede must not touch a terminal execution");
    let new_approval = record["approval_id"].as_str().unwrap().to_string();
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": new_approval, "status": "approved"}),
    )
    .await;
    let (status, body) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(h.task(&task).await["status"], "completed");
    let rows = h.task_execute_effects(&task).await;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().all(|r| r["status"] == "succeeded"));
    assert_ne!(rows[0]["effect_id"], rows[1]["effect_id"]);
    assert_ne!(rows[0]["execution_id"], rows[1]["execution_id"]);
}

#[tokio::test]
async fn e2a_f3_rerun_from_failed() {
    let (model_addr, _model) = start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(Vec::new(), format!("http://{model_addr}")).await;
    let empty = h.files_root().join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let task = h.create_task("empty").await;
    let (status, _) = h.post("/tasks/run", json!({"task_id": task})).await;
    assert_eq!(status, 200);
    let record = h.poll_task(&task, "failed", 15).await;
    assert_eq!(record["error"], "目录下没有可读文件");
    let old_executions = h.task_executions(&task).await;
    assert_eq!(old_executions.len(), 1);
    let old_execution_id = old_executions[0]["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(old_executions[0]["status"], "failed");

    // Fix the directory, then re-run from failed.
    std::fs::write(empty.join("a.txt"), "alpha").unwrap();
    let (status, _) = h.post("/tasks/run", json!({"task_id": task})).await;
    assert_eq!(status, 200, "failed→running must be a legal edge");
    let record = h.poll_task(&task, "draft_ready", 15).await;
    assert!(record["approval_id"].as_str().is_some());
    let executions = h.task_executions(&task).await;
    assert_eq!(executions.len(), 2, "{executions:?}");
    let old = executions
        .iter()
        .find(|e| e["execution_id"] == old_execution_id)
        .unwrap();
    assert_eq!(old["status"], "failed", "supersede must not touch a terminal execution");
    let approval = record["approval_id"].as_str().unwrap().to_string();
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (status, body) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(h.task(&task).await["status"], "completed");
}

#[tokio::test]
async fn e2a_f4_resume_from_completed_regenerates() {
    let (model_addr, _model) = start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(Vec::new(), format!("http://{model_addr}")).await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let (task, approval) = h.run_to_draft_ready("notes").await;
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (status, _) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 200);
    assert_eq!(h.task(&task).await["status"], "completed");
    let old_execution_id = h.task_executions(&task).await[0]["execution_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Resume from completed (Q1a): a fresh attempt that
    // regenerates the draft and a new approval — not a silent
    // no-op, and no stranded `running` Execution.
    let (status, body) = h.post("/tasks/resume", json!({"task_id": task})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "completed", "resume returns the pre-run snapshot");
    let record = h.poll_task(&task, "draft_ready", 15).await;
    assert_ne!(record["approval_id"], approval, "resume mints a fresh approval");
    let executions = h.task_executions(&task).await;
    assert_eq!(executions.len(), 2, "{executions:?}");
    let new_execution = executions
        .iter()
        .find(|e| e["execution_id"] != old_execution_id)
        .expect("new execution");
    assert_eq!(new_execution["status"], "running");
    let old = executions
        .iter()
        .find(|e| e["execution_id"] == old_execution_id)
        .unwrap();
    assert_eq!(old["status"], "completed");
}

#[tokio::test]
async fn e2a_f5_terminal_states_cannot_rerun() {
    let (model_addr, _model) = start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(Vec::new(), format!("http://{model_addr}")).await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();

    // draft_ready cannot re-run: it must be executed first.
    let (task, _) = h.run_to_draft_ready("notes").await;
    for path in ["/tasks/run", "/tasks/resume"] {
        let (status, body) = h.post(path, json!({"task_id": task})).await;
        assert_eq!(status, 409, "{path}: {body}");
        assert_eq!(body["error"], "task_status_transition_invalid");
    }
    assert_eq!(h.task(&task).await["status"], "draft_ready");
    assert_eq!(h.task_executions(&task).await.len(), 1);

    // rejected cannot re-run.
    let (task2, _) = h.run_to_draft_ready("notes").await;
    let (status, _) = h.post("/tasks/reject", json!({"task_id": task2})).await;
    assert_eq!(status, 200);
    assert_eq!(h.task(&task2).await["status"], "rejected");
    for path in ["/tasks/run", "/tasks/resume"] {
        let (status, body) = h.post(path, json!({"task_id": task2})).await;
        assert_eq!(status, 409, "{path}: {body}");
        assert_eq!(body["error"], "task_status_transition_invalid");
    }
    assert_eq!(h.task_executions(&task2).await.len(), 1);

    // cancelled cannot re-run (approval still pending at draft_ready).
    let (task3, _) = h.run_to_draft_ready("notes").await;
    let (status, body) = h.post("/tasks/cancel", json!({"task_id": task3})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(h.task(&task3).await["status"], "cancelled");
    for path in ["/tasks/run", "/tasks/resume"] {
        let (status, body) = h.post(path, json!({"task_id": task3})).await;
        assert_eq!(status, 409, "{path}: {body}");
        assert_eq!(body["error"], "task_status_transition_invalid");
    }
    assert_eq!(h.task_executions(&task3).await.len(), 1);
}

/// F6 setup: run to draft_ready, execute once before the
/// approval is decided (409 approval_required, planned row
/// stranded), then return (task, approval, planned effect id).
async fn f6_stranded(h: &Harness) -> (String, String, String) {
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let (task, approval) = h.run_to_draft_ready("notes").await;
    let (status, body) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "approval_required");
    let rows = h.task_execute_effects(&task).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"], "planned");
    let effect_id = rows[0]["effect_id"].as_str().unwrap().to_string();
    (task, approval, effect_id)
}

#[tokio::test]
async fn e2a_f6_resume_refuses_other_approval_planned_row() {
    let (model_addr, _model) = start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(Vec::new(), format!("http://{model_addr}")).await;
    let (task, approval, effect_id) = f6_stranded(&h).await;
    // The planned row belongs to another approval now: the
    // retry must refuse, not drive a foreign row forward.
    let db = h.db();
    let changed = db
        .execute(
            "UPDATE effects SET approval_id = ?1 WHERE effect_id = ?2",
            rusqlite::params!["approval-9999", effect_id],
        )
        .unwrap();
    assert_eq!(changed, 1);
    drop(db);
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (status, body) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_idempotency_blocked");
    assert_eq!(body["effect_id"], effect_id);
    assert_eq!(h.task(&task).await["status"], "draft_ready");
    assert!(!h.files_root().join("notes/report.md").exists(), "no write on refusal");
    let rows = h.task_execute_effects(&task).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"], "planned");
}

#[tokio::test]
async fn e2a_f6_resume_refuses_started_row() {
    let (model_addr, _model) = start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(Vec::new(), format!("http://{model_addr}")).await;
    let (task, approval, effect_id) = f6_stranded(&h).await;
    // The row already started (interrupted mid-write): the
    // retry must refuse, not re-drive it.
    let db = h.db();
    let changed = db
        .execute(
            "UPDATE effects SET status = ?1 WHERE effect_id = ?2",
            rusqlite::params!["started", effect_id],
        )
        .unwrap();
    assert_eq!(changed, 1);
    drop(db);
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (status, body) = h.post("/tasks/execute", json!({"task_id": task})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_idempotency_blocked");
    assert_eq!(body["effect_id"], effect_id);
    assert_eq!(h.task(&task).await["status"], "draft_ready");
    assert!(!h.files_root().join("notes/report.md").exists(), "no write on refusal");
    let rows = h.task_execute_effects(&task).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"], "started");
}

#[tokio::test]
async fn e2a_f7_concurrent_double_execute_writes_once() {
    let (model_addr, _model) = start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let h = Harness::serve_with_model(Vec::new(), format!("http://{model_addr}")).await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let (task, approval) = h.run_to_draft_ready("notes").await;
    let draft = h.draft_text(&task).await;
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (a, b) = tokio::join!(
        h.post("/tasks/execute", json!({"task_id": task})),
        h.post("/tasks/execute", json!({"task_id": task})),
    );
    let mut statuses = vec![a.0, b.0];
    statuses.sort_unstable();
    assert_eq!(statuses, vec![200, 409], "one winner, one refusal: {a:?} {b:?}");
    // The loser's 409 is timing-dependent: if it enters after
    // the winner completed the task, the entry validator refuses
    // (task_status_transition_invalid); if it enters during the
    // winner's execute, the consume (approval_binding_mismatch)
    // or the idempotency gate (effect_idempotency_blocked)
    // refuses. All three are legitimate refusals of the second
    // write; the locked property is exactly one 200 + exactly
    // one succeeded row + artifact == draft.
    let loser = if a.0 == 409 { &a.1 } else { &b.1 };
    assert!(
        matches!(
            loser["error"].as_str(),
            Some("task_status_transition_invalid")
                | Some("approval_binding_mismatch")
                | Some("effect_idempotency_blocked")
        ),
        "unexpected loser error: {loser}"
    );
    assert_eq!(h.task(&task).await["status"], "completed");
    let rows = h.task_execute_effects(&task).await;
    assert_eq!(rows.len(), 1, "exactly one row: {rows:?}");
    assert_eq!(rows[0]["status"], "succeeded");
    let artifact = std::fs::read(h.files_root().join("notes/report.md")).unwrap();
    assert_eq!(artifact, draft.as_bytes());
}

// --- A3: worker terminal closes Execution ---
//
// git-status (read) succeeds → Execution completed, no
// Effect/Evidence/Inbox; cargo-test on the failing package
// fails → Execution failed with a (test, run) failed Effect.

#[tokio::test]
async fn e2a_a3_worker_terminal_closes_execution() {
    let dir = temp_dir("a3");
    let repo = init_repo_with_cargo_packages(&dir);
    let h = Harness::serve_with_worker(Some(real_git()), Some(real_cargo())).await;
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
    assert_eq!(row["effect_id"], Value::Null, "read runs record no effect");
    let execution_id = row["execution_id"].as_str().unwrap().to_string();
    assert_eq!(h.execution(&execution_id).await["status"], "completed");
    assert!(h.effects().await.is_empty(), "read runs record no effect");

    let approval = h
        .approve_worker_run(
            &worktree,
            "cargo-test",
            json!({"package": "testpkg-fail"}),
            None,
        )
        .await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "cargo-test",
            "params": {"package": "testpkg-fail"},
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    let run_id = run["run_id"].as_str().unwrap().to_string();
    let row = h.wait_run(&run_id, 120).await;
    assert_eq!(row["status"], "failed", "{row}");
    let execution_id = row["execution_id"].as_str().unwrap().to_string();
    assert_eq!(h.execution(&execution_id).await["status"], "failed");
    let effect_id = row["effect_id"].as_str().unwrap().to_string();
    let effects = h.effects().await;
    let effect = effects
        .iter()
        .find(|e| e["effect_id"] == effect_id)
        .expect("test effect");
    assert_eq!(effect["capability"], "test");
    assert_eq!(effect["operation"], "run");
    assert_eq!(effect["scope"], "cargo-test");
    assert_eq!(effect["status"], "failed");
    // P3-C §5.4 survives the §2.3 coupling: the Execution
    // closes to `failed`, but a test result is a result, not
    // something to review — the close opens no failed Inbox
    // row (neither the Effect's nor the Execution's).
    assert!(h.open_inbox("failed").await.is_empty());
}

// --- A4: timeout maps to needs_review ---
//
// A sleeping fake git with a 2 s timeout: the run times out,
// its Execution goes to `needs_review` (not a fabricated
// terminal word), an unknown Effect + Inbox row are recorded.
// The same test locks the Worker dedupe gates: a second
// concurrent run on the same Execution is 409
// `execution_has_running_worker`, and reusing the consumed
// digest-bound approval is 403 `approval_binding_mismatch`.

#[tokio::test]
async fn e2a_a4_timeout_maps_to_needs_review() {
    let dir = temp_dir("a4");
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

    // Dedupe: a second run on the same Execution while the
    // first is still queued/running is refused.
    let (status, body) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-status",
            "params": {},
            "execution_id": execution_id,
        }))
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "execution_has_running_worker");

    let row = h.wait_run(&run_id, 30).await;
    assert_eq!(row["status"], "timeout", "{row}");
    assert_eq!(h.execution(&execution_id).await["status"], "needs_review");
    let effect_id = row["effect_id"].as_str().unwrap().to_string();
    let effects = h.effects().await;
    let effect = effects
        .iter()
        .find(|e| e["effect_id"] == effect_id)
        .expect("timeout effect");
    assert_eq!(effect["status"], "unknown");
    let inbox = h.open_inbox("unknown_requires_reconciliation").await;
    assert!(
        inbox.iter().any(|r| r["effect_id"] == effect_id),
        "{inbox:?}"
    );

    // Dedupe: the digest-bound approval is single-use.
    let (status, body) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-status",
            "params": {},
            "timeout_ms": 2000,
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "approval_binding_mismatch");
}

// --- A6: recovery scan uses record_effect ---
//
// A sleeping fake git run is still alive at restart: the scan
// orphans it (group kill, no auto-retry) and records its
// unknown Effect through `record_effect` — the row carries a
// real idempotency key matching the central formula, and it
// appears in the effects.jsonl audit export. The run's
// Execution goes to `needs_review`.

#[tokio::test]
async fn e2a_a6_recovery_scan_uses_record_effect() {
    let dir = temp_dir("a6");
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

    h.restart().await;
    let row = h.run(&run_id).await;
    assert_eq!(row["status"], "orphaned", "{row}");
    assert_eq!(h.execution(&execution_id).await["status"], "needs_review");
    let effect_id = row["effect_id"].as_str().unwrap().to_string();
    let effects = h.effects().await;
    let effect = effects
        .iter()
        .find(|e| e["effect_id"] == effect_id)
        .expect("scan effect");
    assert_eq!(effect["status"], "unknown");
    // A real key, not null and not a placeholder: it matches
    // the central formula over the row's own fields.
    let key = effect["idempotency_key"].as_str().expect("key present");
    assert!(!key.is_empty());
    let expected = idempotency_key(
        effect["execution_id"].as_str().unwrap(),
        effect["capability"].as_str().unwrap(),
        effect["operation"].as_str().unwrap(),
        effect["scope"].as_str().unwrap(),
        effect["input_digest"].as_str().unwrap(),
    );
    assert_eq!(key, expected);
    // The audit export (effects.jsonl, rewritten from SQLite
    // on every persist) carries the scan row.
    let export = std::fs::read_to_string(h.dir.join("effects.jsonl")).unwrap();
    assert!(
        export.lines().any(|l| l.contains(&effect_id)),
        "scan effect missing from effects.jsonl export"
    );
    let inbox = h.open_inbox("unknown_requires_reconciliation").await;
    assert!(
        inbox.iter().any(|r| r["effect_id"] == effect_id),
        "{inbox:?}"
    );
}

// --- A11: approval consumed_by_worker_session_id ---
//
// A digest-bound approval consumed by a run carries the
// consuming worker session on the approval (HTTP list +
// detail); an unconsumed approval carries null.

#[tokio::test]
async fn e2a_a11_approval_consumed_by_worker_session_id() {
    let dir = temp_dir("a11");
    let repo = init_repo(&dir);
    let h = Harness::serve_with_worker(Some(real_git()), Some(real_cargo())).await;
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

    let views = h.approvals().await;
    let view = views
        .iter()
        .find(|v| v["approval_id"] == approval)
        .expect("approval in list");
    assert_eq!(
        view["consumed_by_worker_session_id"], worker_session_id,
        "{view}"
    );
    let detail = h.approval(&approval).await;
    assert_eq!(detail["consumed_by_worker_session_id"], worker_session_id);

    // Negative control: an approval never used to create a run
    // carries null.
    let unused = h
        .approve_worker_run(&worktree, "git-status", json!({}), None)
        .await;
    let view = h
        .approvals()
        .await
        .into_iter()
        .find(|v| v["approval_id"] == unused)
        .expect("unused approval in list");
    assert_eq!(view["consumed_by_worker_session_id"], Value::Null);
}

// --- A5: re-run supersedes old Execution ---
//
// A task run is in flight against the slow fake when the
// server is aborted; after restart the task is interrupted
// and its Execution is still `running`. Resume creates a
// NEW Execution (the resumed run answers from the fast
// fake) and cancels the old still-`running` Execution.

#[tokio::test]
async fn e2a_a5_rerun_supersedes_old_execution() {
    let (slow_addr, _slow) =
        start_fake_model(MODEL_TEXT, Duration::from_secs(10), 200).await;
    let (fast_addr, _fast) =
        start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let mut h = Harness::serve_with_model(
        Vec::new(),
        format!("http://{slow_addr}"),
    )
    .await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task = h.create_task("notes").await;
    h.post("/tasks/run", json!({"task_id": task})).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(h.task(&task).await["status"], "running");
    let old_execution = h.execution_for_task(&task).await;
    let old_id = old_execution["execution_id"].as_str().unwrap().to_string();
    assert_eq!(old_execution["status"], "running");

    h.restart_with_model(Some(format!("http://{fast_addr}")))
        .await;
    assert_eq!(h.task(&task).await["status"], "interrupted");
    assert_eq!(h.execution(&old_id).await["status"], "running");

    let (status, _) = h.post("/tasks/resume", json!({"task_id": task})).await;
    assert_eq!(status, 200);
    // The supersede lands synchronously in the resume
    // handler, before the new runner attempt spawns.
    assert_eq!(h.execution(&old_id).await["status"], "cancelled");
    let task_executions: Vec<Value> = h
        .executions()
        .await
        .into_iter()
        .filter(|e| e["task_id"] == task)
        .collect();
    assert_eq!(task_executions.len(), 2, "{task_executions:?}");
    let new_execution = task_executions
        .iter()
        .find(|e| e["execution_id"] != old_id)
        .expect("new execution");
    let new_id = new_execution["execution_id"].as_str().unwrap().to_string();
    assert_ne!(new_id, old_id);
    assert_eq!(new_execution["status"], "running");
    // The new attempt actually runs (fast fake answers).
    h.poll_task(&task, "draft_ready", 15).await;
}

// --- A7: connector write idempotency replay blocked ---
//
// connect → connect is a replay (200, current view, no
// second Effect row); connect after a disconnect is a new
// write; unlink → unlink is a replay (200, no second row).
// The latest-effect rule: only a repeat of the latest write
// with no intervening write is a replay.

#[tokio::test]
async fn e2a_a7_connector_write_idempotency_replay_blocked() {
    let h = Harness::serve(Vec::new()).await;
    let account = h.link_connector("notes").await;

    let count = |effects: &[Value], op: &str, scope: &str| {
        effects
            .iter()
            .filter(|e| {
                e["capability"] == "connectors"
                    && e["operation"] == op
                    && e["scope"] == scope
            })
            .count()
    };

    let (status, view) = h.post("/connectors/notes/connect", json!({})).await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(view["connected"], true);
    assert_eq!(count(&h.effects().await, "connect", "notes"), 1);

    // Replay: same view, no second row.
    let (status, view) = h.post("/connectors/notes/connect", json!({})).await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(view["connected"], true);
    assert_eq!(count(&h.effects().await, "connect", "notes"), 1);

    // The disconnect intervenes, so the next connect is new.
    let (status, view) = h.post("/connectors/notes/disconnect", json!({})).await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(view["connected"], false);
    assert_eq!(count(&h.effects().await, "disconnect", "notes"), 1);
    let (status, _) = h.post("/connectors/notes/connect", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(count(&h.effects().await, "connect", "notes"), 2);

    // Unlink, then unlink again: the second is a replay.
    let (status, view) = h
        .post(
            &format!("/connectors/notes/accounts/{account}/unlink"),
            json!({}),
        )
        .await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(count(&h.effects().await, "unlink", &account), 1);
    let (status, view) = h
        .post(
            &format!("/connectors/notes/accounts/{account}/unlink"),
            json!({}),
        )
        .await;
    assert_eq!(status, 200, "{view}");
    assert_eq!(count(&h.effects().await, "unlink", &account), 1);
}

// --- A8: patch apply idempotency replay blocked ---
//
// Two locks, because the plan's literal store-level setup
// (a second Effect row with the same idempotency key) is
// impossible: `idempotency_key` is UNIQUE at the DB level,
// so a duplicate row cannot exist in SQLite — the
// patch-apply gate's Replay/Blocked arms are defense in
// depth for a state the database cannot hold, and the UNIQUE
// constraint is the real enforcement. (a) proves the
// constraint refuses the duplicate row; (b) proves the
// observable requirement — a replayed apply returns 200 +
// replayed:true and does not rewrite the file, leaving
// exactly one Effect row.

#[tokio::test]
async fn e2a_a8_patch_apply_idempotency_replay_blocked() {
    let h = Harness::serve(vec![
        calls(vec![write_proposal("repo/src/new.txt", "new file\n")]),
        done(),
    ])
    .await;
    let worktree = h.setup_repo().await;
    let loop_id = h.start(Some(&worktree)).await;
    let (approval, _binding) = h.await_approval(&loop_id).await;
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let (status, applied) = h.apply("patch-1", "patchfile-1", None, &approval).await;
    assert_eq!(status, 200, "{applied}");
    assert_eq!(applied["replayed"], false);
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/new.txt")).unwrap(),
        "new file\n"
    );

    // (a) The duplicate-key row is refused by the DB itself.
    let effects = h.effects().await;
    let effect = effects
        .iter()
        .find(|e| e["operation"] == "write" && e["scope"] == "repo/src/new.txt")
        .expect("applied effect");
    let key = effect["idempotency_key"].as_str().unwrap().to_string();
    let db = h.db();
    let duplicate = db.execute(
        "INSERT INTO effects (seq, effect_id, execution_id, capability, operation, scope, input_digest, approval_id, idempotency_key, status, origin_device_id, schema_version, created_ms, updated_at_ms) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        rusqlite::params![
            9999i64,
            "effect-9999",
            effect["execution_id"].as_str().unwrap(),
            "fs",
            "write",
            "repo/src/new.txt",
            effect["input_digest"].as_str().unwrap(),
            approval,
            key,
            "succeeded",
            "unknown",
            1i64,
            0i64,
            0i64,
        ],
    );
    assert!(duplicate.is_err(), "duplicate key must be refused");
    assert!(
        format!("{:?}", duplicate.unwrap_err()).contains("UNIQUE"),
        "refusal must be the UNIQUE constraint"
    );

    // (b) Repeat apply: 200 + replayed:true, file untouched,
    // still exactly one Effect row for the write.
    let (status, replayed) = h.apply("patch-1", "patchfile-1", None, &approval).await;
    assert_eq!(status, 200, "{replayed}");
    assert_eq!(replayed["replayed"], true);
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/new.txt")).unwrap(),
        "new file\n"
    );
    assert_eq!(
        h.effects()
            .await
            .iter()
            .filter(|e| e["operation"] == "write" && e["scope"] == "repo/src/new.txt")
            .count(),
        1
    );
}

// --- A9: empty key rejected ---
//
// A bound Effect with no execution id has no idempotency key
// to check: the apply refuses with 409
// `effect_idempotency_unavailable` BEFORE burning the
// approval, and the file is not written.
// (The digest half of the empty key is unreachable at this
// gate: a NULL input_digest fails the approval digest check
// first, 403 `approval_binding_mismatch` — so the reachable
// empty-key shape is the missing execution id, a state
// pre-P0-B rows legitimately hold.)

#[tokio::test]
async fn e2a_a9_empty_key_rejected() {
    let h = Harness::serve(vec![
        calls(vec![write_proposal("repo/src/new.txt", "new file\n")]),
        done(),
    ])
    .await;
    let worktree = h.setup_repo().await;
    let loop_id = h.start(Some(&worktree)).await;
    let (approval, _binding) = h.await_approval(&loop_id).await;
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let patch = h.patch("patch-1").await;
    let effect_id = patch["files"][0]["effect_id"]
        .as_str()
        .unwrap()
        .to_string();
    // Store-level setup of a legacy unattributed row: the
    // bound Effect loses its execution id.
    let db = h.db();
    let changed = db
        .execute(
            "UPDATE effects SET execution_id = NULL WHERE effect_id = ?1",
            rusqlite::params![effect_id],
        )
        .unwrap();
    assert_eq!(changed, 1);
    drop(db);

    let (status, body) = h.apply("patch-1", "patchfile-1", None, &approval).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_idempotency_unavailable");
    assert!(
        !h.files_root().join("repo/src/new.txt").exists(),
        "refused apply must not write the file"
    );
    assert_eq!(h.approval(&approval).await["consumed"], false);
}

// --- A10: second upsert, no duplicate unknown Inbox ---
//
// Store-level: upserting the same still-unknown Effect twice
// opens exactly one `unknown_requires_reconciliation` row
// (the §2.6 open-row guard). A second upsert after the row
// transitions to `reconciling` is also covered by the guard
// shape: the guard counts open rows for the effect, so a
// re-persist of the same unknown row never opens a second.

#[tokio::test]
async fn e2a_a10_second_upsert_no_duplicate_unknown_inbox() {
    use mock_gateway::effect_store::EffectFileRecord;
    let dir = temp_dir("a10");
    let path = dir.join("t.sqlite3");
    let mut conn = sqlite_store::open_db(&path).expect("open db");
    sqlite_store::apply_migrations(&mut conn).expect("migrate");
    let record = EffectFileRecord {
        effect_id: "effect-1".to_string(),
        execution_id: Some("execution-1".to_string()),
        capability: "git".to_string(),
        operation: "run".to_string(),
        scope: "git-status".to_string(),
        input_digest: Some("digest-1".to_string()),
        approval_id: None,
        status: "unknown".to_string(),
        idempotency_key: Some("exec:execution-1:git.run:git-status:digest-1".to_string()),
        origin_device_id: "unknown".to_string(),
        schema_version: 1,
        reconciliation: None,
        detail: None,
    };
    sqlite_store::upsert_effect(&mut conn, &record).expect("first upsert");
    sqlite_store::upsert_effect(&mut conn, &record).expect("second upsert");
    // A third upsert with a fresh detail (the re-persist shape
    // behind the original hole) still opens no second row.
    let mut again = record.clone();
    again.detail = Some("worker run never started".to_string());
    sqlite_store::upsert_effect(&mut conn, &again).expect("third upsert");
    let open: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM inbox WHERE kind = 'unknown_requires_reconciliation' AND effect_id = ?1 AND resolved_ms IS NULL",
            rusqlite::params!["effect-1"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(open, 1, "one open unknown row for one unknown effect");
}

// --- A14: historical reconciliation ---
//
// Three stranded rows, each produced by a real production
// flow and then reverted to `running` at the store level to
// simulate a pre-E2-A database: a completed Task, a
// completed Loop, and a succeeded Worker run. A restart runs
// the §2.8 reconciliation: all three Executions close per
// the §2.3 mapping, each with an `execution.reconciled`
// audit event — 3 rows reconciled.

#[tokio::test]
async fn e2a_a14_historical_reconciliation() {
    let (model_addr, _model) =
        start_fake_model(MODEL_TEXT, Duration::ZERO, 200).await;
    let dir = temp_dir("a14");
    let repo = init_repo(&dir);
    let mut h = Harness::serve_full(
        vec![done()],
        format!("http://{model_addr}"),
        Some(real_git()),
        Some(real_cargo()),
    )
    .await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();

    // Terminal Task + its Execution.
    let task = h.create_task("notes").await;
    h.post("/tasks/run", json!({"task_id": task})).await;
    let record = h.poll_task(&task, "draft_ready", 15).await;
    let approval = record["approval_id"].as_str().unwrap().to_string();
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    h.post("/tasks/execute", json!({"task_id": task})).await;
    h.poll_task(&task, "completed", 15).await;
    let exec_task = h.execution_for_task(&task).await;
    assert_eq!(exec_task["status"], "completed");
    let exec_task_id = exec_task["execution_id"].as_str().unwrap().to_string();

    // Terminal Loop + its Execution.
    let loop_id = h.start(None).await;
    assert_eq!(h.drive(&loop_id).await["status"], "completed");
    let exec_loop = h.execution_for_loop(&loop_id).await;
    assert_eq!(exec_loop["status"], "completed");
    let exec_loop_id = exec_loop["execution_id"].as_str().unwrap().to_string();

    // Terminal Worker run + its Execution.
    let worktree = h.setup_worktree(&repo).await;
    let run_approval = h
        .approve_worker_run(&worktree, "git-status", json!({}), None)
        .await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-status",
            "params": {},
            "approval_id": run_approval,
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    let run_id = run["run_id"].as_str().unwrap().to_string();
    let row = h.wait_run(&run_id, 30).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    let exec_run_id = row["execution_id"].as_str().unwrap().to_string();
    assert_eq!(h.execution(&exec_run_id).await["status"], "completed");

    // Revert all three Executions to `running` at the store
    // level: the pre-E2-A gap the reconciliation closes.
    // (No production step does this — it simulates a database
    // written before the §2.3 coupling existed.)
    let db = h.db();
    for id in [&exec_task_id, &exec_loop_id, &exec_run_id] {
        let changed = db
            .execute(
                "UPDATE executions SET status = 'running' WHERE execution_id = ?1",
                rusqlite::params![id],
            )
            .unwrap();
        assert_eq!(changed, 1, "{id}");
    }
    drop(db);
    assert_eq!(h.execution(&exec_task_id).await["status"], "running");
    assert_eq!(h.execution(&exec_loop_id).await["status"], "running");
    assert_eq!(h.execution(&exec_run_id).await["status"], "running");

    // Restart: the scan reconciles all three.
    h.restart().await;
    assert_eq!(h.execution(&exec_task_id).await["status"], "completed");
    assert_eq!(h.execution(&exec_loop_id).await["status"], "completed");
    assert_eq!(h.execution(&exec_run_id).await["status"], "completed");
    let db = h.db();
    let reconciled: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM events WHERE event_type = 'execution.reconciled'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reconciled, 3, "one audit event per stranded execution");
}
