//! E2-D batch-1 tests (plan §7, D3–D6): the >1 h strategy
//! lock (D3), in-flight progress (D4), and the loop
//! `worker.wait` tool (D5–D6). D1/D2 (deferred start,
//! priority) are batch 2 — gated on a named use case —
//! and are NOT in this file.
//!
//! HTTP routes only (plus `sqlite_store` reads after the
//! fact, never to fake a production step). Real git and real
//! cargo for the execution path; the native fake `cargo`
//! executable (test fixture, never a production execution
//! path) for the long-running runs D4/D6 need — no template
//! sleeps, so a run that must survive past the driver's
//! first 5 s progress write is a fake cargo sleeping 60 s
//! with a short run timeout. The production 5 s write
//! cadence is never changed for the tests.
//!
//! The loop tool result body is not exposed by the loop
//! view — only `result.body_digest` (sha256 + bytes) is —
//! so D5/D6 assert the wait's returned body through its
//! digest: D5's terminal body is fully deterministic
//! (effect_id read back from the run row), D6's
//! still-running body carries a non-deterministic
//! `elapsed_ms`, so it is asserted by length (4- or
//! 5-digit elapsed) plus the second wait's exact terminal
//! digest. Timing budgets are generous (cross-platform
//! hygiene skill).

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::loop_engine::*;
use mock_gateway::operation_binding::sha256_hex;
use mock_gateway::server::{app, AppState};
use mock_gateway::worker::is_terminal_run_status;
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-e2d-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

/// A real git repo with one committed file, for the read path.
fn init_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&["init", "-b", "main"], &repo);
    git(&["config", "user.email", "test@example.invalid"], &repo);
    git(&["config", "user.name", "e2d-test"], &repo);
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

/// 使用本机可执行夹具，避免 Windows 将 .sh 当作 Win32 程序启动。
/// 每个测试复制同一份编译产物并使用自己的配置和 PID 标记。
/// `stem` is the binary name (fake-git / fake-cargo); the
/// fixture sleeps `secs` seconds, optionally spawning a
/// grandchild when `tree` is true.
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

fn fake_cargo_sleeping(dir: &Path) -> PathBuf {
    fake_binary(dir, "fake-cargo", 60, false)
}

/// `git` / `cargo` are always explicit overrides: `Some(path)`
/// pins the binary (real or fake), `None` forces the
/// unavailable path. Never inherits the ambient PATH
/// resolution, so tests are hermetic.
fn real_git() -> PathBuf {
    mock_gateway::worker::resolve_git_path().expect("git on PATH")
}

fn real_cargo() -> PathBuf {
    mock_gateway::worker::resolve_cargo_path().expect("cargo on PATH")
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

fn worker_run_proposal(
    worktree_id: &str,
    command: &str,
    params: Value,
    timeout_ms: Option<u64>,
) -> ModelReply {
    let mut args = params.as_object().unwrap().clone();
    args.insert(
        "command".to_string(),
        Value::String(command.to_string()),
    );
    args.insert(
        "worktree_id".to_string(),
        Value::String(worktree_id.to_string()),
    );
    if let Some(timeout) = timeout_ms {
        args.insert("timeout_ms".to_string(), json!(timeout));
    }
    ModelReply::Calls {
        proposals: vec![Proposal {
            tool: "worker.run".into(),
            args: Value::Object(args),
            wire: None,
        }],
        content: None,
    }
}

fn worker_wait_proposal(run_id: &str, timeout_ms: Option<u64>) -> ModelReply {
    let mut args = serde_json::Map::new();
    args.insert("run_id".to_string(), Value::String(run_id.to_string()));
    if let Some(timeout) = timeout_ms {
        args.insert("timeout_ms".to_string(), json!(timeout));
    }
    ModelReply::Calls {
        proposals: vec![Proposal {
            tool: "worker.wait".into(),
            args: Value::Object(args),
            wire: None,
        }],
        content: None,
    }
}

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    model: Arc<ScriptModel>,
}

impl Harness {
    async fn serve(
        replies: Vec<ModelReply>,
        git: Option<PathBuf>,
        cargo: Option<PathBuf>,
    ) -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let state = AppState::with_files_root_and_approvals(
            dir.join("files"),
            Some(dir.join("approvals.jsonl")),
        )
        .with_loop_backends(
            Some(dir.join("loops.jsonl")),
            Some(model.clone()),
            None,
            Timeouts::default(),
        )
        .unwrap()
        .with_auto_drive(false)
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_evidence_path(dir.join("evidence.jsonl"))
        .with_binding_state_path(dir.join("bindings.jsonl"))
        .with_project_state_path(dir.join("projects.jsonl"))
        .with_location_state_path(dir.join("locations.jsonl"))
        .with_worktree_state_path(dir.join("worktrees.jsonl"))
        .with_git_path(git)
        .with_cargo_path(cargo)
        .with_sqlite_path(dir.join("gateway.sqlite3"));
        state.sqlite_import_and_switch().expect("switch");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server_state = state.clone();
        let server =
            tokio::spawn(async move { axum::serve(listener, app(server_state)).await.unwrap() });
        Self {
            dir,
            url,
            // A wait step can block up to its 10 s budget;
            // 60 s leaves ample headroom on slow CI.
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            server,
            model,
        }
    }

    /// Replace the scripted model's remaining replies. The
    /// worker.run proposal needs the worktree id, which only
    /// exists after `setup_worktree`, so loop tests serve
    /// with an empty script and set the replies here, before
    /// starting the loop.
    fn set_replies(&self, replies: Vec<ModelReply>) {
        *self.model.steps.lock().unwrap() = replies.into();
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

    /// Binding + Location + Worktree for `repo`; returns the
    /// worktree id.
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

    async fn effects(&self) -> Vec<Value> {
        self.ok_get("/effects/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn evidence(&self) -> Vec<Value> {
        self.ok_get("/evidence/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn open_inbox(&self, kind: &str) -> Vec<Value> {
        self.ok_get(&format!("/inbox/list?kind={kind}&open_only=true"))
            .await
            .as_array()
            .unwrap()
            .clone()
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

    /// Start a loop bound to a worktree (P3-A: the loop's
    /// Execution snapshot freezes the worktree).
    async fn start_with_worktree(&self, worktree_id: &str) -> String {
        self.ok(
            "/agent/loop/start",
            json!({"prompt": "e2d loop", "worktree_id": worktree_id}),
        )
        .await["loop_id"]
        .as_str()
        .unwrap()
        .into()
    }

    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id": id})).await
    }

    /// Step until the loop awaits approval; returns its
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

    /// Step until the loop completes (each resume runs one
    /// model round + its tool calls; a wait step can take
    /// its whole budget).
    async fn drive_to_completed(&self, loop_id: &str) -> Value {
        let mut view = Value::Null;
        for _ in 0..12 {
            view = self.step(loop_id).await;
            if view["status"] == "completed" || view["status"] == "failed" {
                break;
            }
        }
        assert_eq!(view["status"], "completed", "{view}");
        view
    }

    /// The loop-view calls for `tool`, in order.
    fn calls_for<'a>(&self, view: &'a Value, tool: &str) -> Vec<&'a Value> {
        view["calls"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|c| c["tool"] == tool)
            .collect()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// D3 (§4): the >1 h strategy is locked, no code change —
/// creation above 1 h is still refused with
/// `invalid_timeout`, and a long task split into several
/// runs gives each run its own Effect + Evidence.
#[tokio::test]
async fn e2d_over_1h_creation_rejected() {
    let dir = temp_dir("d3");
    let repo = init_repo_with_cargo_packages(&dir);
    let h = Harness::serve(vec![], Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    // Over 1 h: resolve_command refuses before the approval
    // gate, so no approval is needed to see the refusal.
    let (status, body) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "cargo-test",
            "params": {"package": "testpkg-pass"},
            "timeout_ms": 3_600_001,
        }))
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"], "invalid_timeout");
    // Split into two runs: each has its own Effect + Evidence.
    for expected_run in ["run-1", "run-2"] {
        let approval = h
            .approve_worker_run(&worktree, "cargo-test", json!({"package": "testpkg-pass"}), None)
            .await;
        let (status, run) = h
            .create_run(json!({
                "worktree_id": worktree,
                "command": "cargo-test",
                "params": {"package": "testpkg-pass"},
                "approval_id": approval,
            }))
            .await;
        assert_eq!(status, 201, "{run}");
        assert_eq!(run["run_id"], expected_run);
        let row = h.wait_run(expected_run, 120).await;
        assert_eq!(row["status"], "succeeded", "{row}");
        assert_eq!(row["exit_code"], 0);
    }
    let effects = h.effects().await;
    assert_eq!(effects.len(), 2, "{effects:?}");
    for effect in &effects {
        assert_eq!(effect["capability"], "test");
        assert_eq!(effect["operation"], "run");
        assert_eq!(effect["scope"], "cargo-test");
        assert_eq!(effect["status"], "succeeded");
    }
    assert_ne!(effects[0]["effect_id"], effects[1]["effect_id"]);
    let evidence = h.evidence().await;
    assert_eq!(evidence.len(), 2, "{evidence:?}");
    for row in &evidence {
        assert_eq!(row["source"], "test");
        assert_eq!(row["reason"], "test_passed");
    }
    let effect_ids: Vec<_> = effects
        .iter()
        .map(|e| e["effect_id"].as_str().unwrap())
        .collect();
    for row in &evidence {
        assert!(
            effect_ids.contains(&row["effect_id"].as_str().unwrap()),
            "{row}"
        );
    }
}

/// D4 (§5): while a run is alive, polling
/// `GET /worker/runs/{id}` shows `progress_json` — the
/// three numeric fields, written at most every 5 s by the
/// driver. The fake cargo sleeps 60 s and the run times
/// out at 8 s, so the first 5 s write lands while the run
/// is still running.
#[tokio::test]
async fn e2d_progress_visible_while_running() {
    let dir = temp_dir("d4");
    let repo = init_repo(&dir);
    let h = Harness::serve(vec![], Some(real_git()), Some(fake_cargo_sleeping(&dir))).await;
    let worktree = h.setup_worktree(&repo).await;
    let approval = h
        .approve_worker_run(
            &worktree,
            "cargo-test",
            json!({"package": "testpkg-pass"}),
            Some(8000),
        )
        .await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "cargo-test",
            "params": {"package": "testpkg-pass"},
            "timeout_ms": 8000,
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    // Poll until a row is seen running WITH progress: the
    // write lands at ~5 s, the run times out at 8 s.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let progress: Value = loop {
        let row = h.run("run-1").await;
        if row["status"] == "running" {
            if let Some(text) = row["progress_json"].as_str() {
                let progress: Value = serde_json::from_str(text).unwrap();
                if progress["elapsed_ms"].is_u64()
                    && progress["output_bytes"].is_u64()
                    && progress["tail_lines"].is_u64()
                {
                    break progress;
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no progress seen while running: {row}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // First write lands at ~5 s; the run ends at 8 s.
    assert!(
        progress["elapsed_ms"].as_u64().unwrap() >= 5000,
        "{progress}"
    );
    assert!(
        progress["elapsed_ms"].as_u64().unwrap() < 10_000,
        "{progress}"
    );
    // The run ends on its own timeout; the progress value
    // survives on the terminal row (the conditional write
    // never overwrites terminal fields).
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "timeout", "{row}");
    assert!(row["progress_json"].as_str().is_some(), "{row}");
}

/// D5 (§6): `worker.wait` waits for the run's terminal
/// state and returns its status + effect_id. The loop
/// enqueues a git-worktree-add run, then waits for it; the
/// wait's returned body is asserted through the loop view's
/// `body_digest` (the view exposes no body).
#[tokio::test]
async fn e2d_loop_waits_for_run_terminal() {
    let dir = temp_dir("d5");
    let repo = init_repo(&dir);
    // The worktree-add precondition requires the target's
    // parent to exist (P3-C C16).
    std::fs::create_dir_all(repo.join("scratch")).unwrap();
    let h = Harness::serve(vec![], Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    h.set_replies(vec![
        worker_run_proposal(
            &worktree,
            "git-worktree-add",
            json!({"path": "scratch/w1", "branch": "d5-branch"}),
            None,
        ),
        worker_wait_proposal("run-1", None),
        done(),
    ]);
    let loop_id = h.start_with_worktree(&worktree).await;
    let (approval, binding) = h.await_approval(&loop_id).await;
    let _ = h.approve_and_resume(&loop_id, &approval, &binding).await;
    let view = h.drive_to_completed(&loop_id).await;
    let row = h.run("run-1").await;
    assert_eq!(row["status"], "succeeded", "{row}");
    let effect_id = row["effect_id"].as_str().unwrap().to_string();
    // The wait call succeeded and its body is exactly the
    // terminal shape: status + effect_id + run_id.
    let waits = h.calls_for(&view, "worker.wait");
    assert_eq!(waits.len(), 1, "{view}");
    assert_eq!(waits[0]["status"], "succeeded", "{waits:?}");
    let expected = serde_json::to_string(&json!({
        "effect_id": effect_id,
        "run_id": "run-1",
        "status": "succeeded",
    }))
    .unwrap();
    assert_eq!(
        waits[0]["result"]["body_digest"]["sha256"],
        sha256_hex(expected.as_bytes())
    );
    assert_eq!(
        waits[0]["result"]["body_digest"]["bytes"],
        expected.len()
    );
    // The run's Effect is the one the wait returned.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["effect_id"], effect_id);
    assert_eq!(effects[0]["capability"], "git");
    assert_eq!(effects[0]["operation"], "worktree-add");
    assert_eq!(effects[0]["scope"], "scratch/w1");
    assert_eq!(effects[0]["status"], "succeeded");
    // The worktree is really on disk.
    assert!(repo.join("scratch").join("w1").join(".git").exists());
}

/// D6 (§6): when the budget runs out before the run is
/// terminal, `worker.wait` returns "still running + current
/// progress" and the loop can wait again next round. The
/// run sleeps (fake cargo) with an 8 s timeout; the first
/// wait has a 6 s budget (progress written at 5 s is inside
/// it), the second wait sees the 8 s timeout land.
#[tokio::test]
async fn e2d_loop_wait_budget_returns_still_running() {
    let dir = temp_dir("d6");
    let repo = init_repo(&dir);
    let h = Harness::serve(vec![], Some(real_git()), Some(fake_cargo_sleeping(&dir))).await;
    let worktree = h.setup_worktree(&repo).await;
    h.set_replies(vec![
        worker_run_proposal(
            &worktree,
            "cargo-test",
            json!({"package": "testpkg-pass"}),
            Some(8000),
        ),
        worker_wait_proposal("run-1", Some(6000)),
        worker_wait_proposal("run-1", None),
        done(),
    ]);
    let loop_id = h.start_with_worktree(&worktree).await;
    let (approval, binding) = h.await_approval(&loop_id).await;
    let _ = h.approve_and_resume(&loop_id, &approval, &binding).await;
    let view = h.drive_to_completed(&loop_id).await;
    let waits = h.calls_for(&view, "worker.wait");
    assert_eq!(waits.len(), 2, "{view}");
    assert_eq!(waits[0]["status"], "succeeded", "{waits:?}");
    assert_eq!(waits[1]["status"], "succeeded", "{waits:?}");
    // First wait: still running + current progress. The body
    // carries a non-deterministic elapsed_ms (~6 s), so the
    // shape is asserted by length: the still-running body
    // with a 4- or 5-digit elapsed (the wait returns by
    // ~10.5 s at the latest — the engine's 10 s tool timeout
    // bounds it), zero output bytes, zero tail lines (the
    // fake cargo prints nothing while sleeping).
    let len_for = |elapsed_ms: u64| {
        serde_json::to_string(&json!({
            "progress": {"elapsed_ms": elapsed_ms, "output_bytes": 0, "tail_lines": 0},
            "run_id": "run-1",
            "status": "running",
        }))
        .unwrap()
        .len()
    };
    let first_bytes = waits[0]["result"]["body_digest"]["bytes"]
        .as_u64()
        .unwrap() as usize;
    assert!(
        first_bytes == len_for(5000) || first_bytes == len_for(50_000),
        "first wait body length {first_bytes} is not the still-running shape"
    );
    // Second wait: the 8 s timeout landed, so it returns the
    // terminal state — status timeout + the unknown Effect's
    // id, asserted by exact digest.
    let row = h.run("run-1").await;
    assert_eq!(row["status"], "timeout", "{row}");
    let effect_id = row["effect_id"].as_str().unwrap().to_string();
    let expected = serde_json::to_string(&json!({
        "effect_id": effect_id,
        "run_id": "run-1",
        "status": "timeout",
    }))
    .unwrap();
    assert_eq!(
        waits[1]["result"]["body_digest"]["sha256"],
        sha256_hex(expected.as_bytes())
    );
    assert_eq!(
        waits[1]["result"]["body_digest"]["bytes"],
        expected.len()
    );
    // The timeout left the honest unknown Effect + Inbox row.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["effect_id"], effect_id);
    assert_eq!(effects[0]["status"], "unknown");
    assert_eq!(h.open_inbox("unknown_requires_reconciliation").await.len(), 1);
}
