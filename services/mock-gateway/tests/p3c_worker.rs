//! P3-C tests (plan §8.1): C16 worktree-add, C17 commit,
//! C18 cargo test (Test Evidence), C21 transport (the 4
//! worker routes over HTTP; the Invoke track is Tauri,
//! compile-only via the CI macOS job), plus the loop-tool
//! registration (normalize + args_digest + requires_approval)
//! and the approval digest gate.
//!
//! HTTP routes only, plus `sqlite_store` reads after the
//! fact (never to fake a production step). Real git and real
//! cargo for the execution path; a native fake `git`/`cargo`
//! executable (test fixture, never a production execution
//! path) for the timeout/unknown path, selected through the
//! plan's own `MOCK_GIT_PATH` / `MOCK_CARGO_PATH`
//! mechanisms via `AppState::with_git_path` /
//! `with_cargo_path`.

use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p3c-{}-{}-{}",
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
    git(&["config", "user.name", "p3c-test"], &repo);
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

fn fake_git_sleeping(dir: &Path) -> PathBuf {
    fake_binary(dir, "fake-git", 60, true)
}

fn fake_cargo_sleeping(dir: &Path) -> PathBuf {
    fake_binary(dir, "fake-cargo", 60, false)
}

/// Canned `cargo test` output (see
/// tests/fixtures/fake_cargo_output.rs): two suites' summaries
/// on stdout, then >4096 bytes of Cargo progress on stderr,
/// exit 0. The summaries survive only in the full combined
/// output, not in the stored tail.
fn fake_cargo_output(dir: &Path) -> PathBuf {
    static BINARY: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    let binary = BINARY.get_or_init(|| {
        let build = temp_dir("native-fixture-output");
        let output = build.join(format!("fake-cargo-output{}", std::env::consts::EXE_SUFFIX));
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/fake_cargo_output.rs");
        let result = std::process::Command::new(
            std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()),
        )
        .arg(source)
        .arg("-o")
        .arg(&output)
        .output()
        .expect("compile fake cargo output fixture");
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        output
    });
    let executable = dir.join(format!("fake-cargo-output{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(binary, &executable).unwrap();
    executable
}

/// `git` / `cargo` are always explicit overrides: `Some(path)`
/// pins the binary (real or fake), `None` forces the
/// unavailable path. Never inherits the ambient PATH
/// resolution, so tests are hermetic.
fn build_state(dir: &Path, git: Option<PathBuf>, cargo: Option<PathBuf>) -> AppState {
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
    .with_git_path(git)
    .with_cargo_path(cargo);
    let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    assert_eq!(state.sqlite_backend_name(), "sqlite");
    state
}

fn real_git() -> PathBuf {
    mock_gateway::worker::resolve_git_path().expect("git on PATH")
}

fn real_cargo() -> PathBuf {
    mock_gateway::worker::resolve_cargo_path().expect("cargo on PATH")
}

struct Harness {
    dir: PathBuf,
    git: Option<PathBuf>,
    cargo: Option<PathBuf>,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    state: AppState,
}

impl Harness {
    async fn serve(dir: &Path, git: Option<PathBuf>, cargo: Option<PathBuf>) -> Self {
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let state = build_state(dir, git.clone(), cargo.clone());
        let kept = state.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir: dir.to_path_buf(),
            git,
            cargo,
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
        let state = build_state(&self.dir, self.git.clone(), self.cargo.clone());
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

    async fn verify(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/verify", json!({"effect_id": effect_id})).await
    }

    async fn reconcile(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/reconcile", json!({"effect_id": effect_id})).await
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// --- C16: worktree-add ---

#[tokio::test]
async fn p3c_c16_worktree_add_approval_effect_evidence_record() {
    let dir = temp_dir("c16-add");
    let repo = init_repo(&dir);
    // The worktree-add precondition requires the target's
    // parent to exist (plan §4.2).
    std::fs::create_dir_all(repo.join("scratch")).unwrap();
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    let approval = h
        .approve_worker_run(
            &worktree,
            "git-worktree-add",
            json!({"path": "scratch/w1", "branch": "scratch-w1"}),
            None,
        )
        .await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-worktree-add",
            "params": {"path": "scratch/w1", "branch": "scratch-w1"},
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    assert_eq!(run["status"], "queued");
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    assert_eq!(row["exit_code"], 0);
    // The approval is consumed at creation (digest-bound).
    let approval_row = h.ok_get(&format!("/approvals/{approval}")).await;
    assert_eq!(approval_row["status"], "approved");
    assert_eq!(approval_row["consumed"], true);
    // One Effect, (git, worktree-add, scratch/w1), succeeded.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["capability"], "git");
    assert_eq!(effects[0]["operation"], "worktree-add");
    assert_eq!(effects[0]["scope"], "scratch/w1");
    assert_eq!(effects[0]["status"], "succeeded");
    assert_eq!(row["effect_id"], effects[0]["effect_id"]);
    // Write-time Evidence: kind worktree, head_sha present.
    let evidence = h.evidence().await;
    assert_eq!(evidence.len(), 1, "{evidence:?}");
    assert_eq!(evidence[0]["source"], "write");
    assert_eq!(evidence[0]["effect_id"], effects[0]["effect_id"]);
    let observed: Value =
        serde_json::from_str(evidence[0]["observed_json"].as_str().unwrap()).unwrap();
    assert_eq!(observed["kind"], "worktree");
    assert!(observed["head_sha"].as_str().is_some_and(|s| !s.is_empty()));
    // The Worktree record exists (created after success).
    let worktrees = h.ok_get("/worktrees/list").await;
    let created = worktrees
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["directory_relative"] == "scratch/w1");
    assert!(created.is_some(), "{worktrees}");
    assert_eq!(created.unwrap()["branch_label"], "scratch-w1");
    // The directory is a real git worktree on disk.
    let new_path = repo.join("scratch").join("w1");
    assert!(new_path.join(".git").exists());
    git(&["rev-parse", "HEAD"], &new_path);
    // Verify re-reads the repo: HEAD matches the baseline.
    let (status, verdict) = h.verify(effects[0]["effect_id"].as_str().unwrap()).await;
    assert_eq!(status, 200, "{verdict}");
    assert_eq!(verdict["verdict"], "verified");
    assert_eq!(verdict["reason"], "hash_match");
}

#[tokio::test]
async fn p3c_c16_worktree_add_direct_click_needs_no_approval() {
    // §7: the run route mirrors /files/write — a UI direct
    // click needs no separate approval (the click is the
    // authorization). The loop arm is the path that forces
    // an approved approval, not the route.
    let dir = temp_dir("c16-direct");
    let repo = init_repo(&dir);
    // The worktree-add precondition requires the target's
    // parent to exist (plan §4.2).
    std::fs::create_dir_all(repo.join("scratch")).unwrap();
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-worktree-add",
            "params": {"path": "scratch/direct", "branch": "scratch-direct"},
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    assert!(repo.join("scratch").join("direct").join(".git").exists());
}

#[tokio::test]
async fn p3c_c16_worktree_add_branch_and_path_exist_409() {
    let dir = temp_dir("c16-409");
    let repo = init_repo(&dir);
    // The worktree-add precondition requires the target's
    // parent to exist (plan §4.2).
    std::fs::create_dir_all(repo.join("scratch")).unwrap();
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    // Branch already exists: the approval request itself
    // refuses (the resolver runs the preconditions), and so
    // does the run route.
    git(&["branch", "taken"], &repo);
    let (status, body) = h
        .post(
            "/approval/request",
            json!({
                "tool": "worker.run",
                "worktree_id": worktree,
                "command_template": "git-worktree-add",
                "params": {"path": "scratch/taken", "branch": "taken"},
            }),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "worktree_branch_exists");
    let (status, body) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-worktree-add",
            "params": {"path": "scratch/taken", "branch": "taken"},
        }))
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "worktree_branch_exists");
    // Path already exists (a plain directory, no branch):
    // 409 worktree_path_exists.
    std::fs::create_dir_all(repo.join("scratch").join("busy")).unwrap();
    let (status, body) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-worktree-add",
            "params": {"path": "scratch/busy", "branch": "busy"},
        }))
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "worktree_path_exists");
    // No run was created for either refusal.
    let runs = h.ok_get("/worker/runs/list").await;
    assert!(runs.as_array().unwrap().is_empty(), "{runs}");
}

#[tokio::test]
async fn p3c_c16_pending_approval_and_wrong_digest_403() {
    let dir = temp_dir("c16-403");
    let repo = init_repo(&dir);
    // The worktree-add precondition requires the target's
    // parent to exist (plan §4.2).
    std::fs::create_dir_all(repo.join("scratch")).unwrap();
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    // A pending (unapproved) approval cannot authorize a run.
    let (status, body) = h
        .post(
            "/approval/request",
            json!({
                "tool": "worker.run",
                "worktree_id": worktree,
                "command_template": "git-worktree-add",
                "params": {"path": "scratch/p", "branch": "p"},
            }),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let pending = body["tool_call_id"].as_str().unwrap().to_string();
    let (status, body) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-worktree-add",
            "params": {"path": "scratch/p", "branch": "p"},
            "approval_id": pending,
        }))
        .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "approval_required");
    // An approval for a different command does not match
    // this command's digest.
    let other = h
        .approve_worker_run(&worktree, "cargo-test", json!({"package": "testpkg-pass"}), None)
        .await;
    let (status, body) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-worktree-add",
            "params": {"path": "scratch/p", "branch": "p"},
            "approval_id": other,
        }))
        .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "approval_binding_mismatch");
}

#[tokio::test]
async fn p3c_c16_approval_replay_refused() {
    // The replay guard (UNIQUE index on approval_id) is
    // reached only when the command digest matches; a
    // worktree-add replay would hit the branch-exists
    // precondition first, so the replay is exercised with
    // cargo-test (no preconditions). A digest-bound approval
    // is single-use by consumption: the second create fails
    // the consume (403 approval_binding_mismatch). The 409
    // approval_already_used fast path is the loop-bound
    // replay (the approval is already consumed by the loop,
    // so validation passes and the run-row check catches
    // the duplicate); it needs a loop-bound approval, which
    // only the loop engine creates, so it is not exercised
    // end-to-end here.
    let dir = temp_dir("c16-replay");
    let repo = init_repo_with_cargo_packages(&dir);
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    let approval = h
        .approve_worker_run(&worktree, "cargo-test", json!({"package": "testpkg-pass"}), None)
        .await;
    let (status, _) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "cargo-test",
            "params": {"package": "testpkg-pass"},
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 201);
    // The same approval cannot authorize a second run.
    let (status, body) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "cargo-test",
            "params": {"package": "testpkg-pass"},
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 403, "{body}");
    assert_eq!(body["error"], "approval_binding_mismatch");
}

// --- C17: commit ---

#[tokio::test]
async fn p3c_c17_commit_approval_effect_evidence_verify() {
    let dir = temp_dir("c17-commit");
    let repo = init_repo(&dir);
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    std::fs::write(repo.join("b.txt"), "second file\n").unwrap();
    git(&["add", "-A"], &repo);
    let approval = h
        .approve_worker_run(&worktree, "git-commit", json!({"message": "p3c commit one"}), None)
        .await;
    let (status, run) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-commit",
            "params": {"message": "p3c commit one"},
            "approval_id": approval,
        }))
        .await;
    assert_eq!(status, 201, "{run}");
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    assert_eq!(row["exit_code"], 0);
    // One Effect, (git, commit), succeeded.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["capability"], "git");
    assert_eq!(effects[0]["operation"], "commit");
    assert_eq!(effects[0]["status"], "succeeded");
    assert_eq!(row["effect_id"], effects[0]["effect_id"]);
    // Write-time Evidence: kind commit, sha + parent + tree.
    let evidence = h.evidence().await;
    assert_eq!(evidence.len(), 1, "{evidence:?}");
    assert_eq!(evidence[0]["source"], "write");
    let observed: Value =
        serde_json::from_str(evidence[0]["observed_json"].as_str().unwrap()).unwrap();
    assert_eq!(observed["kind"], "commit");
    let sha = observed["sha256"].as_str().unwrap();
    assert!(!sha.is_empty());
    assert!(observed["parent_sha256"].as_str().is_some());
    assert!(observed["tree_sha256"].as_str().is_some());
    // The commit is in the repo, with the exact message.
    let log = std::process::Command::new("git")
        .args(["log", "--format=%s", "-n", "1"])
        .current_dir(&repo)
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8_lossy(&log.stdout).trim(),
        "p3c commit one"
    );
    // Verify re-reads the repo: HEAD == baseline sha.
    let (status, verdict) = h.verify(effects[0]["effect_id"].as_str().unwrap()).await;
    assert_eq!(status, 200, "{verdict}");
    assert_eq!(verdict["verdict"], "verified");
    assert_eq!(verdict["reason"], "hash_match");
}

#[tokio::test]
async fn p3c_c17_commit_verify_ancestor_after_later_commit() {
    let dir = temp_dir("c17-ancestor");
    let repo = init_repo(&dir);
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    std::fs::write(repo.join("b.txt"), "second file\n").unwrap();
    git(&["add", "-A"], &repo);
    let first = h
        .approve_worker_run(&worktree, "git-commit", json!({"message": "p3c ancestor one"}), None)
        .await;
    let (status, _) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-commit",
            "params": {"message": "p3c ancestor one"},
            "approval_id": first,
        }))
        .await;
    assert_eq!(status, 201);
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    let first_effect = row["effect_id"].as_str().unwrap().to_string();
    // A second commit on the same branch.
    std::fs::write(repo.join("c.txt"), "third file\n").unwrap();
    git(&["add", "-A"], &repo);
    let second = h
        .approve_worker_run(&worktree, "git-commit", json!({"message": "p3c ancestor two"}), None)
        .await;
    let (status, _) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-commit",
            "params": {"message": "p3c ancestor two"},
            "approval_id": second,
        }))
        .await;
    assert_eq!(status, 201);
    let row = h.wait_run("run-2", 30).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    // The first commit is no longer HEAD, but it is an
    // ancestor of HEAD, so it still verifies.
    let (status, verdict) = h.verify(&first_effect).await;
    assert_eq!(status, 200, "{verdict}");
    assert_eq!(verdict["verdict"], "verified");
    assert_eq!(verdict["reason"], "hash_match");
}

#[tokio::test]
async fn p3c_c17_commit_unknown_reconcile_landed_via_message() {
    // The run times out under a fake git (unknown Effect),
    // then the commit is made by hand with the same message;
    // reconcile finds it by exact message + reachability.
    // The driver never re-runs the commit itself: the repo
    // ends with exactly one commit carrying that message.
    let dir = temp_dir("c17-reconcile-landed");
    let repo = init_repo(&dir);
    let fake = fake_git_sleeping(&dir);
    let mut h = Harness::serve(&dir, Some(fake), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    let message = "p3c reconcile probe landed";
    let approval = h
        .approve_worker_run(&worktree, "git-commit", json!({"message": message}), Some(1000))
        .await;
    let (status, _) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-commit",
            "params": {"message": message},
            "approval_id": approval,
            "timeout_ms": 1000,
        }))
        .await;
    assert_eq!(status, 201);
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "timeout", "{row}");
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["capability"], "git");
    assert_eq!(effects[0]["operation"], "commit");
    assert_eq!(effects[0]["status"], "unknown");
    let effect_id = effects[0]["effect_id"].as_str().unwrap().to_string();
    // Restart with the real git so the observe path reads
    // the repo (the fake git would sleep through it).
    h.git = Some(real_git());
    h.restart().await;
    // The commit, made by hand with the exact message.
    std::fs::write(repo.join("b.txt"), "reconciled file\n").unwrap();
    git(&["add", "-A"], &repo);
    git(&["commit", "-m", message], &repo);
    let (status, reconciled) = h.reconcile(&effect_id).await;
    assert_eq!(status, 200, "{reconciled}");
    assert_eq!(reconciled["status"], "reconciled");
    assert_eq!(reconciled["reconciliation"]["result"], "completed");
    // Exactly one commit carries the message: no auto-retry.
    let log = std::process::Command::new("git")
        .args(["log", "--format=%s"])
        .current_dir(&repo)
        .output()
        .unwrap();
    let matches = String::from_utf8_lossy(&log.stdout)
        .lines()
        .filter(|line| *line == message)
        .count();
    assert_eq!(matches, 1);
}

#[tokio::test]
async fn p3c_c17_commit_unknown_reconcile_stays_unknown_without_commit() {
    let dir = temp_dir("c17-reconcile-unknown");
    let repo = init_repo(&dir);
    let fake = fake_git_sleeping(&dir);
    let mut h = Harness::serve(&dir, Some(fake), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    let message = "p3c reconcile probe absent";
    let approval = h
        .approve_worker_run(&worktree, "git-commit", json!({"message": message}), Some(1000))
        .await;
    let (status, _) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-commit",
            "params": {"message": message},
            "approval_id": approval,
            "timeout_ms": 1000,
        }))
        .await;
    assert_eq!(status, 201);
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "timeout", "{row}");
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["status"], "unknown");
    let effect_id = effects[0]["effect_id"].as_str().unwrap().to_string();
    h.git = Some(real_git());
    h.restart().await;
    // No commit with that message exists: reconcile is
    // Inconclusive and the row stays unknown (honesty rule:
    // never auto-retry, never call it failed). The unknown
    // row is still open. (The restart re-imports the unknown
    // Effect and the import re-fires the Inbox insert, so
    // there may be two open rows for the same effect — a
    // pre-existing restart/import artifact, not a P3-C
    // behavior; the assertion is that the row is still open
    // and no new kind appeared.)
    let (status, reconciled) = h.reconcile(&effect_id).await;
    assert_eq!(status, 200, "{reconciled}");
    assert_eq!(reconciled["status"], "unknown");
    assert!(reconciled["reconciliation"].is_null());
    let inbox = h.open_inbox("unknown_requires_reconciliation").await;
    assert!(!inbox.is_empty(), "{inbox:?}");
    assert!(inbox.iter().all(|row| row["effect_id"] == effect_id));
}

#[tokio::test]
async fn p3c_c17_git_status_unknown_reconcile_stays_unknown() {
    // P3-C window finding (R0): a timed-out git-status run's
    // unknown Effect carries operation `git-status` (the
    // template name, via unknown_effect_identity), and the
    // observe branch must match that name — not the loop
    // tool's `status`. HEAD resolves but a read lands
    // nothing, so reconcile is Inconclusive and the row
    // stays unknown with its Inbox row still open.
    let dir = temp_dir("c17-reconcile-status");
    let repo = init_repo(&dir);
    let fake = fake_git_sleeping(&dir);
    let mut h = Harness::serve(&dir, Some(fake), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    // git-status is a read: no approval needed.
    let (status, _) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "git-status",
            "timeout_ms": 1000,
        }))
        .await;
    assert_eq!(status, 201);
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "timeout", "{row}");
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["capability"], "git");
    assert_eq!(effects[0]["operation"], "git-status");
    assert_eq!(effects[0]["status"], "unknown");
    let effect_id = effects[0]["effect_id"].as_str().unwrap().to_string();
    h.git = Some(real_git());
    h.restart().await;
    let (status, reconciled) = h.reconcile(&effect_id).await;
    assert_eq!(status, 200, "{reconciled}");
    assert_eq!(reconciled["status"], "unknown");
    assert!(reconciled["reconciliation"].is_null());
    let inbox = h.open_inbox("unknown_requires_reconciliation").await;
    assert!(!inbox.is_empty(), "{inbox:?}");
    assert!(inbox.iter().all(|row| row["effect_id"] == effect_id));
}

// --- C18: cargo test ---

#[tokio::test]
async fn p3c_c18_cargo_test_pass_effect_and_evidence() {
    let dir = temp_dir("c18-pass");
    let repo = init_repo_with_cargo_packages(&dir);
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
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
    let row = h.wait_run("run-1", 120).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    assert_eq!(row["exit_code"], 0);
    // One Effect, (test, run), succeeded.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["capability"], "test");
    assert_eq!(effects[0]["operation"], "run");
    assert_eq!(effects[0]["scope"], "cargo-test");
    assert_eq!(effects[0]["status"], "succeeded");
    assert_eq!(row["effect_id"], effects[0]["effect_id"]);
    // Test Evidence: source test, reason test_passed, the
    // closed observed set (kind test-run, counts, duration,
    // exit code, failed-name hashes — no plaintext names).
    let evidence = h.evidence().await;
    assert_eq!(evidence.len(), 1, "{evidence:?}");
    assert_eq!(evidence[0]["source"], "test");
    assert_eq!(evidence[0]["reason"], "test_passed");
    assert_eq!(evidence[0]["effect_id"], effects[0]["effect_id"]);
    let observed: Value =
        serde_json::from_str(evidence[0]["observed_json"].as_str().unwrap()).unwrap();
    assert_eq!(observed["kind"], "test-run");
    assert_eq!(observed["passed"], 1);
    assert_eq!(observed["failed"], 0);
    assert_eq!(observed["exit_code"], 0);
    assert!(observed["duration_ms"].as_u64().is_some());
    assert_eq!(observed["failed_sha256"], json!([]));
    // Verify trusts the stored Evidence only (never re-runs).
    let (status, verdict) = h.verify(effects[0]["effect_id"].as_str().unwrap()).await;
    assert_eq!(status, 200, "{verdict}");
    assert_eq!(verdict["verdict"], "verified");
    assert_eq!(verdict["reason"], "test_passed");
}

#[tokio::test]
async fn p3c_c18_cargo_test_counts_survive_trailing_stderr() {
    // P3-C window finding (U7-1): a real `cargo test` run's
    // stderr progress fills the stored tail, so parsing only
    // the tail returned null counts. The finish path now
    // parses the full combined output: the canned fake cargo
    // prints two suites' summaries on stdout, then >4096
    // bytes of progress on stderr, and the Evidence must
    // still carry the summed counts — while the stored tail
    // stays truncated.
    let dir = temp_dir("c18-counts-tail");
    let repo = init_repo(&dir);
    let h = Harness::serve(&dir, Some(real_git()), Some(fake_cargo_output(&dir))).await;
    let worktree = h.setup_worktree(&repo).await;
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
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "succeeded", "{row}");
    assert_eq!(row["exit_code"], 0);
    assert_eq!(row["truncated"], true);
    assert!(!row["tail"].as_str().unwrap().contains("test result:"));
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["status"], "succeeded");
    let evidence = h.evidence().await;
    assert_eq!(evidence.len(), 1, "{evidence:?}");
    assert_eq!(evidence[0]["source"], "test");
    assert_eq!(evidence[0]["reason"], "test_passed");
    let observed: Value =
        serde_json::from_str(evidence[0]["observed_json"].as_str().unwrap()).unwrap();
    assert_eq!(observed["kind"], "test-run");
    assert_eq!(observed["passed"], 3);
    assert_eq!(observed["failed"], 0);
    assert_eq!(observed["exit_code"], 0);
    assert_eq!(observed["failed_sha256"], json!([]));
}

#[tokio::test]
async fn p3c_c18_cargo_test_fail_effect_failed_patch_untouched() {
    let dir = temp_dir("c18-fail");
    let repo = init_repo_with_cargo_packages(&dir);
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    let approval = h
        .approve_worker_run(&worktree, "cargo-test", json!({"package": "testpkg-fail"}), None)
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
    let row = h.wait_run("run-1", 120).await;
    // A non-zero exit is a known result (failed), never unknown.
    // cargo exits 101 when tests fail.
    assert_eq!(row["status"], "failed", "{row}");
    assert_eq!(row["exit_code"], 101);
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["capability"], "test");
    assert_eq!(effects[0]["operation"], "run");
    assert_eq!(effects[0]["status"], "failed");
    // Test Evidence: failed counts + the failed name as a
    // sha256 only (never plaintext).
    let evidence = h.evidence().await;
    assert_eq!(evidence.len(), 1, "{evidence:?}");
    assert_eq!(evidence[0]["source"], "test");
    assert_eq!(evidence[0]["reason"], "test_failed");
    let observed: Value =
        serde_json::from_str(evidence[0]["observed_json"].as_str().unwrap()).unwrap();
    assert_eq!(observed["kind"], "test-run");
    assert_eq!(observed["passed"], 1);
    assert_eq!(observed["failed"], 1);
    assert_eq!(observed["exit_code"], 101);
    let expected = mock_gateway::sqlite_store::sha256_hex(b"tests::fails");
    assert_eq!(observed["failed_sha256"], json!([expected]));
    assert!(
        !evidence[0]["observed_json"]
            .as_str()
            .unwrap()
            .contains("tests::fails"),
        "failed test name leaked in plaintext"
    );
    // A failed row is not verifiable (only succeeded rows are).
    let (status, body) = h.verify(effects[0]["effect_id"].as_str().unwrap()).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_not_verifiable");
    // The failure touches no Patch: none exists, and the
    // failure opened no Inbox row (a test result is a result,
    // not something to review).
    let patches = h.ok_get("/patches/list").await;
    assert!(patches.as_array().unwrap().is_empty(), "{patches}");
    assert!(h.open_inbox("failed").await.is_empty());
}

#[tokio::test]
async fn p3c_c18_cargo_test_timeout_unknown() {
    let dir = temp_dir("c18-timeout");
    let repo = init_repo_with_cargo_packages(&dir);
    let fake = fake_cargo_sleeping(&dir);
    let h = Harness::serve(&dir, Some(real_git()), Some(fake)).await;
    let worktree = h.setup_worktree(&repo).await;
    let approval = h
        .approve_worker_run(&worktree, "cargo-test", json!({"package": "testpkg-pass"}), Some(1000))
        .await;
    let (status, _) = h
        .create_run(json!({
            "worktree_id": worktree,
            "command": "cargo-test",
            "params": {"package": "testpkg-pass"},
            "approval_id": approval,
            "timeout_ms": 1000,
        }))
        .await;
    assert_eq!(status, 201);
    let row = h.wait_run("run-1", 30).await;
    assert_eq!(row["status"], "timeout", "{row}");
    // Timeout is unknown (honesty rule), with one unknown
    // Effect and one unknown_requires_reconciliation row.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["capability"], "test");
    assert_eq!(effects[0]["operation"], "run");
    assert_eq!(effects[0]["status"], "unknown");
    let inbox = h.open_inbox("unknown_requires_reconciliation").await;
    assert_eq!(inbox.len(), 1, "{inbox:?}");
    assert_eq!(inbox[0]["effect_id"], effects[0]["effect_id"]);
}

// --- Loop-tool registration ---

#[test]
fn p3c_loop_tools_registered_in_normalize_digest_and_approval() {
    use mock_gateway::loop_engine::{args_digest, loop_tool_requires_approval, normalize};
    // git.read: read-only, no approval; shape validated.
    assert!(!loop_tool_requires_approval("git.read"));
    let args = normalize(
        "git.read",
        &json!({"operation": "status", "worktree_id": "worktree-1"}),
    )
    .expect("git.read status");
    assert_eq!(args["operation"], "status");
    assert_eq!(args["worktree_id"], "worktree-1");
    assert_eq!(args["path"], "");
    assert!(normalize("git.read", &json!({"operation": "log"})).is_err());
    assert!(normalize("git.read", &json!({"operation": "diff", "path": "../escape"})).is_err());
    // worker.run: approval-gated; per-template params.
    assert!(loop_tool_requires_approval("worker.run"));
    let args = normalize(
        "worker.run",
        &json!({
            "command": "git-commit",
            "worktree_id": "worktree-1",
            "message": "p3c loop commit",
        }),
    )
    .expect("worker.run commit");
    assert_eq!(args["command"], "git-commit");
    assert_eq!(args["message"], "p3c loop commit");
    assert!(normalize(
        "worker.run",
        &json!({"command": "git-commit", "worktree_id": "worktree-1"})
    )
    .is_err());
    assert!(normalize(
        "worker.run",
        &json!({"command": "git-commit", "worktree_id": "worktree-1", "message": "x", "package": "y"})
    )
    .is_err());
    assert!(normalize(
        "worker.run",
        &json!({"command": "rm", "worktree_id": "worktree-1"})
    )
    .is_err());
    // The proposal-time digest is deterministic and distinct
    // per command (the planning branch overwrites it with the
    // command digest, which needs AppState).
    let a = args_digest("worker.run", &args);
    let b = args_digest(
        "worker.run",
        &normalize(
            "worker.run",
            &json!({
                "command": "cargo-test",
                "worktree_id": "worktree-1",
                "package": "testpkg-pass",
            }),
        )
        .unwrap(),
    );
    assert_ne!(a, b);
    assert_eq!(a, args_digest("worker.run", &args));
    // The gateway's approval table agrees.
    assert_eq!(mock_gateway::tool_requires_approval("git.read"), Some(false));
    assert_eq!(mock_gateway::tool_requires_approval("worker.run"), Some(true));
}

// --- C21: transport (HTTP) ---

#[tokio::test]
async fn p3c_c21_worker_routes_over_http() {
    let dir = temp_dir("c21-routes");
    let repo = init_repo(&dir);
    let h = Harness::serve(&dir, Some(real_git()), Some(real_cargo())).await;
    let worktree = h.setup_worktree(&repo).await;
    // POST /worker/runs (201) + GET /worker/runs/{id} +
    // GET /worker/runs/list (filter by execution) +
    // POST /worker/runs/{id}/cancel on a queued run.
    let (status, run) = h
        .create_run(json!({"worktree_id": worktree, "command": "git-status"}))
        .await;
    assert_eq!(status, 201, "{run}");
    let run_id = run["run_id"].as_str().unwrap().to_string();
    let execution_id = run["execution_id"].as_str().unwrap().to_string();
    let got = h.run(&run_id).await;
    assert_eq!(got["run_id"], run_id);
    let listed = h
        .ok_get(&format!("/worker/runs/list?execution_id={execution_id}"))
        .await;
    assert_eq!(listed.as_array().unwrap().len(), 1, "{listed}");
    let (status, cancelled) = h
        .post(&format!("/worker/runs/{run_id}/cancel"), json!({}))
        .await;
    // The run may already have finished (git-status is
    // fast); either a 200 cancel or a 409 terminal refusal
    // is a correct answer for the route. A 200 cancel is
    // asynchronous (the driver marks it), so wait for it.
    assert!(status == 200 || status == 409, "{cancelled}");
    if status == 200 {
        let row = h.wait_run(&run_id, 15).await;
        assert_eq!(row["status"], "cancelled", "{row}");
    }
    // Unknown run id: 404.
    let (status, body) = h.get("/worker/runs/run-999").await;
    assert_eq!(status, 404, "{body}");
}
