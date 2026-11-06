//! E2 prerequisite slice tests (P3-C plan §8.1):
//! C12 lifecycle, C13 allowlist, C14 containment, C15 git
//! read-only, C19 unknown + takeover/superseded, C20
//! restart, plus the task-binding freeze (plan §2.3,
//! scheme A).
//!
//! HTTP routes only, plus `sqlite_store` reads after the
//! fact (never to fake a production step). Real git for the
//! read path; a native fake `git` executable (test fixture,
//! never a production execution path) for slow/timeout/cancel
//! paths, selected through the plan's own `MOCK_GIT_PATH`
//! mechanism via `AppState::with_git_path`. "Restart" is a
//! new AppState on the same database (p0e T2 in-process
//! precedent): the recovery scan code path is identical to
//! a real kill -9 restart; the binary SIGKILL variant is
//! window/U4 work, not repeated here.

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use mock_gateway::worker::process_alive;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-e2slice-{}-{}-{}",
        tag,
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
    git(&["config", "user.name", "slice-test"], &repo);
    std::fs::write(repo.join("a.txt"), "hello\n").unwrap();
    git(&["add", "-A"], &repo);
    git(&["commit", "-m", "init"], &repo);
    repo
}

/// 使用本机可执行夹具，避免 Windows 将 .sh 当作 Win32 程序启动。
/// 每个测试复制同一份编译产物并使用自己的配置和 PID 标记。
fn fake_git(dir: &Path, secs: u64, tree: bool) -> PathBuf {
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
    let executable = dir.join(format!("fake-git{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(binary, &executable).unwrap();
    std::fs::write(executable.with_extension("config"), format!("{secs} {tree}")).unwrap();
    executable
}

fn fake_git_sleeping(dir: &Path) -> PathBuf {
    fake_git(dir, 60, true)
}

fn fake_git_slow(dir: &Path, secs: u64) -> PathBuf {
    fake_git(dir, secs, false)
}

/// `git` is always an explicit override: `Some(path)` pins
/// the binary (real git or a fake script), `None` forces
/// the git-unavailable path. Never inherits the ambient
/// PATH resolution, so tests are hermetic.
fn build_state(dir: &Path, git: Option<PathBuf>) -> AppState {
    let state = AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_executions_path(dir.join("executions.jsonl"))
    .with_effects_path(dir.join("effects.jsonl"))
    .with_binding_state_path(dir.join("bindings.jsonl"))
    .with_project_state_path(dir.join("projects.jsonl"))
    .with_location_state_path(dir.join("locations.jsonl"))
    .with_worktree_state_path(dir.join("worktrees.jsonl"))
    .with_git_path(git);
    let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    assert_eq!(state.sqlite_backend_name(), "sqlite");
    state
}

fn real_git() -> PathBuf {
    mock_gateway::worker::resolve_git_path().expect("git on PATH")
}

struct Harness {
    dir: PathBuf,
    git: Option<PathBuf>,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    state: AppState,
}

impl Harness {
    async fn serve(dir: &Path, git: Option<PathBuf>) -> Self {
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let state = build_state(dir, git.clone());
        let kept = state.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir: dir.to_path_buf(),
            git,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
            state: kept,
        }
    }

    /// New AppState on the same database: runs the recovery
    /// scan, like a gateway restart after kill -9.
    async fn restart(&mut self) {
        self.server.abort();
        let state = build_state(&self.dir, self.git.clone());
        self.state = state.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
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

    async fn wait_run_status(&self, run_id: &str, want: &str, secs: u64) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let row = self.run(run_id).await;
            if row["status"].as_str() == Some(want) {
                return row;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "run {run_id} never reached {want}: {row}"
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

    async fn open_inbox(&self, kind: &str) -> Vec<Value> {
        self.ok_get(&format!("/inbox/list?kind={kind}&open_only=true"))
            .await
            .as_array()
            .unwrap()
            .clone()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn db(dir: &Path) -> rusqlite::Connection {
    sqlite_store::open_db(&dir.join("gateway.sqlite3")).expect("open db")
}

// --- C12: lifecycle ---

#[tokio::test]
async fn e2slice_c12_status_run_succeeds_with_exit_code() {
    let dir = temp_dir("c12-status");
    let repo = init_repo(&dir);
    let h = Harness::serve(&dir, Some(real_git())).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status"}))
        .await;
    assert_eq!(status, 201, "{run}");
    assert_eq!(run["status"], "queued");
    assert_eq!(run["run_id"], "run-1");
    let row = h.wait_run("run-1", 15).await;
    assert_eq!(row["status"], "succeeded");
    assert_eq!(row["exit_code"], 0);
    assert!(row["pid"].is_number());
    assert!(row["started_ms"].is_number());
    assert!(row["finished_ms"].is_number());
    assert!(!row["truncated"].as_bool().unwrap());
    assert!(row["tail"].as_str().unwrap().contains("## main"));
    // No Effect, no Evidence, no checkpoint kind beyond the
    // one plan_position row the driver writes.
    assert!(h.effects().await.is_empty());
    assert!(h
        .ok_get("/evidence/list")
        .await
        .as_array()
        .unwrap()
        .is_empty());
    let checkpoints =
        sqlite_store::list_checkpoints_for_execution(&db(&dir), "execution-1").expect("cps");
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].kind, "execution.plan_position");
}

#[tokio::test]
async fn e2slice_c12_timeout_kills_process_group_including_grandchild() {
    let dir = temp_dir("c12-timeout");
    let repo = init_repo(&dir);
    let fake = fake_git_sleeping(&dir);
    let h = Harness::serve(&dir, Some(fake)).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status", "timeout_ms": 1000}))
        .await;
    assert_eq!(status, 201, "{run}");
    let row = h.wait_run_status("run-1", "running", 15).await;
    let pid = row["pid"].as_i64().unwrap();
    assert!(process_alive(pid));
    // The grandchild records its own pid; the group kill
    // must take it too.
    let marker = dir.join("grandchild.pid");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !marker.exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let grandchild: i64 = std::fs::read_to_string(&marker)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(process_alive(grandchild));
    let row = h.wait_run("run-1", 20).await;
    assert_eq!(row["status"], "timeout", "{row}");
    assert!(!process_alive(pid), "child survived group kill");
    assert!(!process_alive(grandchild), "grandchild survived group kill");
    // Unknown Effect + Inbox row (honesty rule: timeout
    // never becomes `failed`).
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["status"], "unknown");
    assert_eq!(effects[0]["capability"], "git");
    assert_eq!(row["effect_id"], effects[0]["effect_id"]);
    let inbox = h.open_inbox("unknown_requires_reconciliation").await;
    assert_eq!(inbox.len(), 1, "{inbox:?}");
    assert_eq!(inbox[0]["effect_id"], effects[0]["effect_id"]);
}

#[tokio::test]
async fn e2slice_c12_cancel_kills_group_and_cancels_execution() {
    let dir = temp_dir("c12-cancel");
    let repo = init_repo(&dir);
    let fake = fake_git_sleeping(&dir);
    let h = Harness::serve(&dir, Some(fake)).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status", "timeout_ms": 60000}))
        .await;
    assert_eq!(status, 201, "{run}");
    let execution_id = run["execution_id"].as_str().unwrap().to_string();
    let row = h.wait_run_status("run-1", "running", 15).await;
    let pid = row["pid"].as_i64().unwrap();
    let (status, cancelled) = h.post("/worker/runs/run-1/cancel", json!({})).await;
    assert_eq!(status, 200, "{cancelled}");
    let row = h.wait_run("run-1", 20).await;
    assert_eq!(row["status"], "cancelled", "{row}");
    assert!(!process_alive(pid), "child survived cancel");
    let execution = h
        .ok_get(&format!("/executions/get?execution_id={execution_id}"))
        .await;
    assert_eq!(execution["status"], "cancelled");
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["status"], "unknown");
    assert_eq!(
        h.open_inbox("unknown_requires_reconciliation").await.len(),
        1
    );
    // Cancelling a terminal run is a 409.
    let (status, body) = h.post("/worker/runs/run-1/cancel", json!({})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "worker_run_terminal");
}

#[tokio::test]
async fn e2slice_c12_second_run_while_running_is_409() {
    let dir = temp_dir("c12-busy");
    let repo = init_repo(&dir);
    let fake = fake_git_sleeping(&dir);
    let h = Harness::serve(&dir, Some(fake)).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status", "timeout_ms": 60000}))
        .await;
    assert_eq!(status, 201, "{run}");
    let execution_id = run["execution_id"].as_str().unwrap().to_string();
    h.wait_run_status("run-1", "running", 15).await;
    let (status, body) = h
        .create_run(
            json!({"worktree_id": worktree, "command": "git-status", "execution_id": execution_id}),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "execution_has_running_worker");
    // Cleanup: cancel the long run so no sleep survives.
    h.post("/worker/runs/run-1/cancel", json!({})).await;
    h.wait_run("run-1", 20).await;
}

// --- C13: allowlist ---

#[tokio::test]
async fn e2slice_c13_allowlist_rejects() {
    let dir = temp_dir("c13");
    let repo = init_repo(&dir);
    let h = Harness::serve(&dir, Some(real_git())).await;
    let worktree = h.setup_worktree(&repo).await;
    for (command, params) in [
        ("cargo test", Value::Null),
        ("git", Value::Null),
        ("git worktree add", Value::Null),
        ("git commit", Value::Null),
        ("sh", Value::Null),
        ("git-status", json!({"path": "a.txt"})),
        ("git-diff", json!({"path": "../escape"})),
        ("git-diff", json!({"path": "/abs/path"})),
        ("git-diff", json!({"path": "a;b"})),
        ("git-diff", json!({"path": "a|b"})),
        ("git-diff", json!({"path": "$(x)"})),
        ("git-diff", json!({"path": "`x`"})),
        ("git-diff", json!({"path": ""})),
        ("git-diff", json!({"other": "x"})),
        ("git-diff", json!({"path": 1})),
    ] {
        let mut body = json!({"worktree_id": worktree, "command": command});
        if !params.is_null() {
            body["params"] = params.clone();
        }
        let (status, out) = h.create_run(body).await;
        assert_eq!(status, 400, "{command} {params}: {out}");
    }
    // Timeout bounds.
    for timeout in [0u64, 3_600_001] {
        let (status, out) = h
            .create_run(
                json!({"worktree_id": worktree, "command": "git-status", "timeout_ms": timeout}),
            )
            .await;
        assert_eq!(status, 400, "timeout {timeout}: {out}");
    }
    // Unknown worktree.
    let (status, out) = h
        .create_run(json!({"worktree_id": "worktree-99", "command": "git-status"}))
        .await;
    assert_eq!(status, 400, "{out}");
    assert_eq!(out["error"], "unknown_worktree");
    // Unknown run / unknown execution.
    let (status, out) = h.get("/worker/runs/run-99").await;
    assert_eq!(status, 404, "{out}");
    let (status, out) = h
        .create_run(
            json!({"worktree_id": worktree, "command": "git-status", "execution_id": "execution-99"}),
        )
        .await;
    assert_eq!(status, 404, "{out}");
    assert_eq!(out["error"], "unknown_execution");
}

// --- C14: containment ---

#[tokio::test]
async fn e2slice_c14_diff_path_containment() {
    let dir = temp_dir("c14");
    let repo = init_repo(&dir);
    let outside = dir.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("secret.txt"), "secret\n").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, repo.join("escape")).unwrap();
    let h = Harness::serve(&dir, Some(real_git())).await;
    let worktree = h.setup_worktree(&repo).await;
    // Symlink escape: rejected even though the link itself
    // lives inside the worktree.
    #[cfg(unix)]
    {
        let (status, out) = h
            .create_run(
                json!({"worktree_id": worktree, "command": "git-diff", "params": {"path": "escape"}}),
            )
            .await;
        assert_eq!(status, 400, "{out}");
        assert_eq!(out["error"], "invalid_path");
    }
    // A plain relative path inside the worktree is fine.
    let (status, run) = h
        .create_run(
            json!({"worktree_id": worktree, "command": "git-diff", "params": {"path": "a.txt"}}),
        )
        .await;
    assert_eq!(status, 201, "{run}");
    let row = h.wait_run("run-1", 15).await;
    assert_eq!(row["status"], "succeeded");
    // The stored cwd is the canonical worktree directory.
    assert_eq!(
        row["cwd"].as_str().unwrap(),
        repo.canonicalize().unwrap().to_string_lossy()
    );
}

// --- C15: git read-only ---

#[tokio::test]
async fn e2slice_c15_status_leaves_repo_clean_and_truncates_diff() {
    let dir = temp_dir("c15-status");
    let repo = init_repo(&dir);
    let h = Harness::serve(&dir, Some(real_git())).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status"}))
        .await;
    assert_eq!(status, 201, "{run}");
    let row = h.wait_run("run-1", 15).await;
    assert_eq!(row["status"], "succeeded");
    assert_eq!(row["tail"], "## main\n");
    // The repo is untouched afterwards.
    let out = std::process::Command::new("git")
        .args(["status", "--porcelain=v1"])
        .current_dir(&repo)
        .output()
        .unwrap();
    assert!(out.stdout.is_empty(), "repo dirtied by read");
    assert!(h.effects().await.is_empty());
    assert!(h
        .ok_get("/evidence/list")
        .await
        .as_array()
        .unwrap()
        .is_empty());

    // Truncation: a diff larger than 4096 bytes keeps only
    // the tail, with the flag set.
    let dir2 = temp_dir("c15-diff");
    let repo2 = init_repo(&dir2);
    std::fs::write(repo2.join("a.txt"), "x".repeat(6000)).unwrap();
    let h2 = Harness::serve(&dir2, Some(real_git())).await;
    let worktree2 = h2.setup_worktree(&repo2).await;
    let (status, run) = h2
        .create_run(json!({"worktree_id": worktree2, "command": "git-diff"}))
        .await;
    assert_eq!(status, 201, "{run}");
    let row = h2.wait_run("run-1", 15).await;
    assert_eq!(row["status"], "succeeded");
    assert!(row["truncated"].as_bool().unwrap());
    assert!(row["tail"].as_str().unwrap().len() <= 4096);
    assert!(h2.effects().await.is_empty());
}

// --- C19: unknown + superseded ---

#[tokio::test]
async fn e2slice_c19_takeover_bound_run_superseded_after_assignment_ends() {
    let dir = temp_dir("c19-superseded");
    let repo = init_repo(&dir);
    let slow = fake_git_slow(&dir, 5);
    let h = Harness::serve(&dir, Some(slow)).await;
    let worktree = h.setup_worktree(&repo).await;
    // E2-A §2.3: a run that succeeds closes its Execution, so
    // the old setup (run-1 succeeds, then takeover + run-2 on
    // the same Execution) now 409s `execution_terminal` at the
    // takeover. The supersede under test needs a NON-terminal
    // Execution with no completed run on it, so the Execution
    // is a store-level fixture (there is no bare-Execution
    // creation route); the takeover → bind → supersede flow
    // under test is unchanged, and the final assertions
    // (superseded run wrote no Effect, opened no Inbox row)
    // are unchanged.
    let execution_id = "execution-9000".to_string();
    {
        let conn = db(&dir);
        conn.execute(
            "INSERT INTO executions (seq, execution_id, task_id, loop_id, session_id, worker_session_id, workspace_snapshot_id, status, origin_device_id, schema_version, created_ms, updated_at_ms) VALUES (?1, ?2, NULL, NULL, NULL, NULL, NULL, 'running', 'unknown', 1, ?3, ?3)",
            rusqlite::params![9000i64, execution_id, sqlite_store::now_ms() as i64],
        )
        .expect("insert fixture execution");
    }
    // Takeover creates the assignment the run binds.
    let takeover = h
        .ok_post(
            "/assignments/takeover",
            json!({"execution_id": execution_id}),
        )
        .await;
    let assignment_id = takeover["assignment"]["assignment_id"]
        .as_str()
        .unwrap()
        .to_string();
    // Run 1 binds the takeover's assignment.
    let (status, run) = h
        .create_run(
            json!({"worktree_id": worktree, "command": "git-status", "timeout_ms": 30000, "execution_id": execution_id}),
        )
        .await;
    assert_eq!(status, 201, "{run}");
    assert_eq!(run["assignment_id"], assignment_id);
    h.wait_run_status("run-1", "running", 15).await;
    // The assignment ends (terminal transition drives it to
    // `done`; a takeover cannot supersede an active
    // assignment by design — it 409s — so the terminal
    // transition is the assignment-loss path exercised
    // here). The still-running worker must not write when
    // it completes.
    let transitioned = h
        .ok_post(
            "/executions/transition",
            json!({"execution_id": execution_id, "status": "failed"}),
        )
        .await;
    assert_eq!(transitioned["status"], "failed");
    let row = h.wait_run("run-1", 20).await;
    assert_eq!(row["status"], "superseded", "{row}");
    assert!(row["effect_id"].is_null());
    assert!(
        h.effects().await.is_empty(),
        "superseded run wrote an Effect"
    );
    assert!(
        h.open_inbox("unknown_requires_reconciliation")
            .await
            .is_empty(),
        "superseded run opened an Inbox row"
    );
}

// --- C20: restart ---

#[tokio::test]
async fn e2slice_c20_restart_orphans_running_run_and_keeps_counter_and_deadline() {
    let dir = temp_dir("c20-orphan");
    let repo = init_repo(&dir);
    let fake = fake_git_sleeping(&dir);
    let mut h = Harness::serve(&dir, Some(fake)).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status", "timeout_ms": 60000}))
        .await;
    assert_eq!(status, 201, "{run}");
    let execution_id = run["execution_id"].as_str().unwrap().to_string();
    let row = h.wait_run_status("run-1", "running", 15).await;
    let pid = row["pid"].as_i64().unwrap();
    let deadline_before = sqlite_store::get_execution_deadline(&db(&dir), &execution_id)
        .expect("deadline read")
        .expect("deadline set");
    let marker = dir.join("grandchild.pid");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !marker.exists() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let grandchild: i64 = std::fs::read_to_string(&marker)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    // Restart: the scan must orphan the live run, kill its
    // group, and leave an unknown Effect + Inbox row.
    h.restart().await;
    let row = h.wait_run("run-1", 15).await;
    assert_eq!(row["status"], "orphaned", "{row}");
    assert!(!process_alive(pid), "orphaned child survived scan kill");
    assert!(
        !process_alive(grandchild),
        "orphaned grandchild survived scan kill"
    );
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["status"], "unknown");
    assert_eq!(row["effect_id"], effects[0]["effect_id"]);
    assert_eq!(
        h.open_inbox("unknown_requires_reconciliation").await.len(),
        1
    );
    // Counter not reused, deadline survives the restart.
    let deadline_after = sqlite_store::get_execution_deadline(&db(&dir), &execution_id)
        .expect("deadline reread")
        .expect("deadline kept");
    assert_eq!(deadline_before, deadline_after);
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status"}))
        .await;
    assert_eq!(status, 201, "{run}");
    assert_eq!(run["run_id"], "run-2");
    // Run-2 executes the fake git (sleep 60); cancel it so
    // no sleeper survives the test.
    h.post("/worker/runs/run-2/cancel", json!({})).await;
    let row = h.wait_run("run-2", 20).await;
    assert_eq!(row["status"], "cancelled", "{row}");
}

#[tokio::test]
async fn e2slice_c20_expired_deadline_refuses_new_run() {
    let dir = temp_dir("c20-deadline");
    let repo = init_repo(&dir);
    // E2-A §2.3: a run that succeeds closes its Execution, so
    // the old setup (run-1 succeeds, then a second run on the
    // same Execution) now 409s `execution_terminal` at create
    // time. The deadline-refusal path instead needs a
    // NON-terminal Execution: run-1 times out, which leaves
    // its Execution at `needs_review` (recoverable, per the
    // §2.3 timeout mapping) — the exact recovery shape the
    // second run exercises.
    let fake = fake_git_sleeping(&dir);
    let h = Harness::serve(&dir, Some(fake)).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status", "timeout_ms": 2000}))
        .await;
    assert_eq!(status, 201, "{run}");
    let execution_id = run["execution_id"].as_str().unwrap().to_string();
    let row = h.wait_run("run-1", 15).await;
    assert_eq!(row["status"], "timeout", "{row}");
    assert_eq!(
        h.ok_get(&format!("/executions/get?execution_id={execution_id}"))
            .await["status"],
        "needs_review"
    );
    // Move the deadline into the past (test seam: direct
    // SQLite write; production sets it only at run
    // creation). A new run must refuse to start and fail
    // the Execution with budget_exceeded.
    {
        let mut conn = db(&dir);
        let past = sqlite_store::now_ms() as i64 - 1000;
        sqlite_store::set_execution_deadline(&mut conn, &execution_id, past).expect("set deadline");
    }
    let (status, run) = h
        .create_run(
            json!({"worktree_id": worktree, "command": "git-status", "execution_id": execution_id}),
        )
        .await;
    assert_eq!(status, 201, "{run}");
    let row = h.wait_run("run-2", 15).await;
    assert_eq!(row["status"], "failed", "{row}");
    assert!(row["tail"].as_str().unwrap().contains("budget_exceeded"));
    let execution = h
        .ok_get(&format!("/executions/get?execution_id={execution_id}"))
        .await;
    assert_eq!(execution["status"], "failed");
}

// --- git unavailable ---

#[tokio::test]
async fn e2slice_git_unavailable_fails_closed() {
    let dir = temp_dir("git-missing");
    let repo = init_repo(&dir);
    let h = Harness::serve(&dir, None).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, out) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status"}))
        .await;
    assert_eq!(status, 503, "{out}");
    assert_eq!(out["error"], "git_unavailable");
    assert!(h
        .ok_get("/worker/runs/list")
        .await
        .as_array()
        .unwrap()
        .is_empty());
}

// --- task binding freeze (plan §2.3, scheme A) ---

const MODEL_TEXT: &str = "# 报告\n\n要点：alpha 与 beta。\n\n结论：完成。";

async fn raw(addr: std::net::SocketAddr, request: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect test server");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write test request");
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .expect("read test response");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    (
        head.lines().next().unwrap_or("").to_string(),
        body.to_string(),
    )
}

async fn start_fake_model(text: &str) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
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
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
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

struct TaskHarness {
    dir: PathBuf,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

impl TaskHarness {
    async fn new(model_url: String) -> Self {
        let dir = temp_dir("freeze");
        let files_root = dir.join("files");
        std::fs::create_dir_all(files_root.clone()).unwrap();
        let state =
            AppState::with_files_root_and_approvals(files_root, Some(dir.join("approvals.jsonl")))
                .with_tasks_path(dir.join("tasks.jsonl"))
                .with_task_drafts_dir(dir.join("drafts"))
                .with_binding_state_path(dir.join("bindings.jsonl"))
                .with_project_state_path(dir.join("projects.jsonl"))
                .with_location_state_path(dir.join("locations.jsonl"))
                .with_worktree_state_path(dir.join("worktrees.jsonl"))
                .with_deepseek(DeepSeekClient::new(DeepSeekConfig {
                    base_url: model_url,
                    model: "fixture-3b".to_string(),
                    api_key: Some("test-key".to_string()),
                }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self { dir, addr, server }
    }

    async fn post(&self, path: &str, json: &str) -> (String, Value) {
        let (head, body) = raw(
            self.addr,
            &format!(
                "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                json.len(),
                json
            ),
        )
        .await;
        (head, serde_json::from_str(&body).unwrap_or(Value::Null))
    }

    async fn get(&self, path: &str) -> (String, Value) {
        let (head, body) = raw(
            self.addr,
            &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"),
        )
        .await;
        (head, serde_json::from_str(&body).unwrap_or(Value::Null))
    }

    async fn task(&self, task_id: &str) -> Value {
        let (head, body) = self.get(&format!("/tasks/get?task_id={task_id}")).await;
        assert!(head.contains("200"), "get task: {head} {body}");
        body
    }

    async fn poll_status(&self, task_id: &str, want: &str) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let record = self.task(task_id).await;
            if record["status"].as_str() == Some(want) {
                return record;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {want}: {record}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for TaskHarness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn e2slice_task_binding_freeze_mismatch_is_409() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT).await;
    let h = TaskHarness::new(format!("http://{model_addr}")).await;
    let notes = h.dir.join("files").join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    // Two worktrees in two repos, so the bindings differ.
    let repo_a = init_repo(&h.dir.join("a"));
    let repo_b = init_repo(&h.dir.join("b"));
    let repo_a_str = repo_a.to_string_lossy().into_owned();
    let repo_b_str = repo_b.to_string_lossy().into_owned();
    let (_, binding_a) = {
        let (head, body) = h
            .post(
                "/bindings/create",
                &json!({"display_name": "a", "path": repo_a_str}).to_string(),
            )
            .await;
        assert!(head.contains("200"), "{body}");
        (head, body)
    };
    let ws_a = binding_a["workspace_id"].as_str().unwrap();
    let (_, loc_a) = {
        let (head, body) = h
            .post(
                "/locations/create",
                &json!({"workspace_id": ws_a, "name": "root", "path": repo_a_str}).to_string(),
            )
            .await;
        assert!(head.contains("200"), "{body}");
        (head, body)
    };
    let loc_a_id = loc_a["location_id"].as_str().unwrap();
    let (_, wt_a) = {
        let (head, body) = h
            .post(
                "/worktrees/create",
                &json!({"location_id": loc_a_id, "directory_relative": "", "branch_label": "main", "project_id": null}).to_string(),
            )
            .await;
        assert!(head.contains("200"), "{body}");
        (head, body)
    };
    let wt_a_id = wt_a["worktree_id"].as_str().unwrap().to_string();
    let (_, binding_b) = {
        let (head, body) = h
            .post(
                "/bindings/create",
                &json!({"display_name": "b", "path": repo_b_str}).to_string(),
            )
            .await;
        assert!(head.contains("200"), "{body}");
        (head, body)
    };
    let ws_b = binding_b["workspace_id"].as_str().unwrap();
    let (_, loc_b) = {
        let (head, body) = h
            .post(
                "/locations/create",
                &json!({"workspace_id": ws_b, "name": "root", "path": repo_b_str}).to_string(),
            )
            .await;
        assert!(head.contains("200"), "{body}");
        (head, body)
    };
    let loc_b_id = loc_b["location_id"].as_str().unwrap();
    let (_, wt_b) = {
        let (head, body) = h
            .post(
                "/worktrees/create",
                &json!({"location_id": loc_b_id, "directory_relative": "", "branch_label": "main", "project_id": null}).to_string(),
            )
            .await;
        assert!(head.contains("200"), "{body}");
        (head, body)
    };
    let wt_b_id = wt_b["worktree_id"].as_str().unwrap().to_string();

    let (head, body) = h
        .post("/tasks/create", &json!({"directory": "notes"}).to_string())
        .await;
    assert!(head.contains("200"), "{body}");
    let task_id = body["task_id"].as_str().unwrap().to_string();
    let (head, _) = h
        .post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#))
        .await;
    assert!(head.contains("200"), "{head}");
    h.poll_status(&task_id, "draft_ready").await;
    // First execute names worktree A but the approval is
    // still pending: the execute fails at the approval gate
    // (403) AFTER the freeze, so the task stays draft_ready
    // with the binding frozen.
    let (head, body) = h
        .post(
            "/tasks/execute",
            &format!(r#"{{"task_id":"{task_id}","worktree_id":"{wt_a_id}"}}"#),
        )
        .await;
    assert!(head.contains("403"), "first execute: {head} {body}");
    let record = h.task(&task_id).await;
    assert_eq!(record["status"], "draft_ready");
    assert_eq!(record["execute_binding"]["worktree_id"], wt_a_id);
    // Same worktree again: passes the freeze, then hits
    // the idempotency gate — the first execute already
    // recorded a planned Effect for the same digest, so the
    // second is refused as a replay-block, NOT as a
    // binding mismatch. That is the proof the freeze passed.
    let (head, body) = h
        .post(
            "/tasks/execute",
            &format!(r#"{{"task_id":"{task_id}","worktree_id":"{wt_a_id}"}}"#),
        )
        .await;
    assert!(head.contains("409"), "same-worktree execute: {head} {body}");
    assert_eq!(body["error"], "effect_idempotency_blocked");
    // Different worktree: 409 task_binding_frozen_mismatch.
    let (head, body) = h
        .post(
            "/tasks/execute",
            &format!(r#"{{"task_id":"{task_id}","worktree_id":"{wt_b_id}"}}"#),
        )
        .await;
    assert!(head.contains("409"), "mismatch execute: {head} {body}");
    assert_eq!(body["error"], "task_binding_frozen_mismatch");
    model.abort();
}
