//! P3-A record tests: bindings / projects / locations /
//! worktrees — create/list/get/archive routes, the C2
//! containment matrix (real repo, non-bound dirs, parent
//! dir, symlinks, missing dirs, 400s), the §3.8 archive
//! guards (409s), restart persistence + counters (C9), and
//! the tasks.execute worktree_id field.
//!
//! Tests use temp dirs and the HTTP routes only; no real
//! git, no subprocesses, no real commands. Paths are
//! compared canonicalized against canonicalized.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use mock_gateway::server::{app, new_state_with_files_root};
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p3a-records-{}-{}-{}",
        tag,
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
}

impl Harness {
    async fn serve(dir: &Path) -> Self {
        let state = new_state_with_files_root(dir.join("files"))
            .with_binding_state_path(dir.join("bindings.jsonl"))
            .with_project_state_path(dir.join("projects.jsonl"))
            .with_location_state_path(dir.join("locations.jsonl"))
            .with_worktree_state_path(dir.join("worktrees.jsonl"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server =
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir: dir.to_path_buf(),
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
        }
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
        let response = self.client.get(format!("{}{}", self.url, path)).send().await.unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{path}: {body}");
        body
    }

    async fn get_ok(&self, path: &str) -> Value {
        let (status, body) = self.get(path).await;
        assert_eq!(status, 200, "{path}: {body}");
        body
    }

    async fn create_binding(&self, display_name: &str, path: &str) -> (u16, Value) {
        self.post(
            "/bindings/create",
            json!({"display_name": display_name, "path": path}),
        )
        .await
    }

    async fn create_location(&self, workspace_id: &str, name: &str, path: &str) -> (u16, Value) {
        self.post(
            "/locations/create",
            json!({"workspace_id": workspace_id, "name": name, "path": path}),
        )
        .await
    }

    async fn create_worktree(
        &self,
        location_id: &str,
        directory_relative: &str,
        project_id: Option<&str>,
    ) -> (u16, Value) {
        self.post(
            "/worktrees/create",
            json!({"location_id": location_id, "directory_relative": directory_relative, "branch_label": "main", "project_id": project_id}),
        )
        .await
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn records_create_list_get_flow() {
    let dir = temp_dir("flow");
    let repo = dir.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let h = Harness::serve(&dir).await;
    let repo_str = repo.to_string_lossy().into_owned();
    let canonical_repo = repo.canonicalize().unwrap();
    let canonical_repo_str = canonical_repo.to_string_lossy().into_owned();

    // Binding: canonical root stored, policy digest frozen.
    let (status, binding) = h.create_binding("repo", &repo_str).await;
    assert_eq!(status, 200, "{binding}");
    assert_eq!(binding["workspace_id"], "workspace-1");
    assert_eq!(binding["display_name"], "repo");
    assert_eq!(binding["allowed_roots"], json!([canonical_repo_str]));
    assert_eq!(binding["status"], "active");
    let digest = binding["policy_digest"].as_str().unwrap();
    assert_eq!(digest.len(), 64);
    assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    // Same live policy, second binding, same digest.
    let (status, second) = h.create_binding("repo-2", &repo_str).await;
    assert_eq!(status, 200, "{second}");
    assert_eq!(second["policy_digest"], binding["policy_digest"]);

    // Project: minimal metadata.
    let project = h
        .ok("/projects/create", json!({"name": "bu2agent", "description": "test project"}))
        .await;
    assert_eq!(project["project_id"], "project-1");
    assert_eq!(project["status"], "active");

    // Location: root fingerprint is sha256 of the canonical
    // JSON {workspace_id, canonical_root}.
    let (status, location) = h.create_location("workspace-1", "root", &repo_str).await;
    assert_eq!(status, 200, "{location}");
    assert_eq!(location["location_id"], "location-1");
    assert_eq!(location["workspace_id"], "workspace-1");
    assert_eq!(location["canonical_root"], canonical_repo_str);
    let expected_root = mock_gateway::sqlite_store::sha256_hex(
        mock_gateway::sqlite_store::canonical_json(&json!({
            "workspace_id": "workspace-1",
            "canonical_root": canonical_repo_str,
        }))
        .as_bytes(),
    );
    assert_eq!(location["root_sha256"], expected_root);

    // Worktree: "" means the Location root; "src" is a child.
    let (status, worktree) = h.create_worktree("location-1", "", Some("project-1")).await;
    assert_eq!(status, 200, "{worktree}");
    assert_eq!(worktree["worktree_id"], "worktree-1");
    assert_eq!(worktree["directory_relative"], "");
    assert_eq!(worktree["project_id"], "project-1");
    let (status, child) = h.create_worktree("location-1", "src", None).await;
    assert_eq!(status, 200, "{child}");
    assert_eq!(child["worktree_id"], "worktree-2");
    assert_eq!(child["directory_relative"], "src");
    assert_eq!(child["project_id"], Value::Null);

    // List shows actives in number order; get returns rows.
    let listed = h.get_ok("/bindings/list").await;
    assert_eq!(listed.as_array().unwrap().len(), 2);
    assert_eq!(listed[0]["workspace_id"], "workspace-1");
    assert_eq!(h.get_ok("/bindings/workspace-1").await["display_name"], "repo");
    assert_eq!(h.get_ok("/projects/list").await.as_array().unwrap().len(), 1);
    assert_eq!(h.get_ok("/projects/project-1").await["name"], "bu2agent");
    assert_eq!(h.get_ok("/locations/list").await.as_array().unwrap().len(), 1);
    assert_eq!(h.get_ok("/locations/location-1").await["name"], "root");
    let worktrees = h.get_ok("/worktrees/list").await;
    assert_eq!(worktrees.as_array().unwrap().len(), 2);
    assert_eq!(h.get_ok("/worktrees/worktree-1").await["location_id"], "location-1");

    // Unknown ids read as 404 on get.
    for path in [
        "/bindings/workspace-99",
        "/projects/project-99",
        "/locations/location-99",
        "/worktrees/worktree-99",
    ] {
        let (status, _) = h.get(path).await;
        assert_eq!(status, 404, "{path}");
    }
}

#[tokio::test]
async fn containment_matrix_400s() {
    let dir = temp_dir("contain");
    let repo = dir.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    let outside = dir.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(dir.join("afile.txt"), b"x").unwrap();
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let h = Harness::serve(&dir).await;
    let repo_str = repo.to_string_lossy().into_owned();
    let outside_str = outside.to_string_lossy().into_owned();

    // Binding creation: missing dir, non-dir, empty name/path.
    let (status, _) = h.create_binding("x", &dir.join("nope").to_string_lossy()).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_binding("x", &dir.join("afile.txt").to_string_lossy()).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_binding("", &repo_str).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_binding("x", "").await;
    assert_eq!(status, 400);

    let binding = h.ok("/bindings/create", json!({"display_name": "repo", "path": repo_str})).await;
    assert_eq!(binding["workspace_id"], "workspace-1");

    // Location creation: parent dir, non-bound dir, missing
    // dir, unknown workspace, empty name.
    let (status, _) = h.create_location("workspace-1", "parent", &dir.to_string_lossy()).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_location("workspace-1", "out", &outside_str).await;
    assert_eq!(status, 400);
    let (status, _) = h
        .create_location("workspace-1", "gone", &repo.join("nope").to_string_lossy())
        .await;
    assert_eq!(status, 400);
    let (status, _) = h.create_location("workspace-99", "x", &repo_str).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_location("workspace-1", "", &repo_str).await;
    assert_eq!(status, 400);
    // A file (not a directory) inside the binding is a 400.
    std::fs::write(repo.join("f.txt"), b"x").unwrap();
    let (status, _) = h
        .create_location("workspace-1", "file", &repo.join("f.txt").to_string_lossy())
        .await;
    assert_eq!(status, 400);

    let (status, location) = h.create_location("workspace-1", "root", &repo_str).await;
    assert_eq!(status, 200, "{location}");

    // Project creation: empty name / description are 400s.
    let (status, _) = h.post("/projects/create", json!({"name": "", "description": "d"})).await;
    assert_eq!(status, 400);
    let (status, _) = h.post("/projects/create", json!({"name": "n", "description": ""})).await;
    assert_eq!(status, 400);

    // Worktree creation: "..", absolute path, missing dir,
    // unknown location, unknown project.
    let (status, _) = h.create_worktree("location-1", "..", None).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_worktree("location-1", "/etc", None).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_worktree("location-1", "nope", None).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_worktree("location-99", "", None).await;
    assert_eq!(status, 400);
    let (status, _) = h.create_worktree("location-1", "", Some("project-99")).await;
    assert_eq!(status, 400);

    // Archived workspace rejects new locations; archived
    // location rejects new worktrees.
    let archived = h.ok("/bindings/create", json!({"display_name": "gone", "path": outside_str})).await;
    assert_eq!(archived["workspace_id"], "workspace-2");
    let (status, body) = h.post("/bindings/workspace-2/archive", json!({})).await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = h.create_location("workspace-2", "x", &outside_str).await;
    assert_eq!(status, 400);
    let (status, body) = h.post("/locations/location-1/archive", json!({})).await;
    assert_eq!(status, 200, "{body}");
    let (status, _) = h.create_worktree("location-1", "", None).await;
    assert_eq!(status, 400);
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_escape_400s() {
    let dir = temp_dir("symlink");
    let repo = dir.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    let outside = dir.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let h = Harness::serve(&dir).await;
    let repo_str = repo.to_string_lossy().into_owned();
    h.ok("/bindings/create", json!({"display_name": "repo", "path": repo_str})).await;

    // A symlinked directory inside the binding that points
    // outside canonicalizes outside the root: 400.
    std::os::unix::fs::symlink(&outside, repo.join("escape")).unwrap();
    let (status, _) = h
        .create_location("workspace-1", "esc", &repo.join("escape").to_string_lossy())
        .await;
    assert_eq!(status, 400);

    let (status, _) = h.create_location("workspace-1", "root", &repo_str).await;
    assert_eq!(status, 200);

    // A symlinked subdirectory of the location that points
    // outside: worktree creation is a 400.
    std::os::unix::fs::symlink(&outside, repo.join("src").join("escape")).unwrap();
    let (status, _) = h.create_worktree("location-1", "src/escape", None).await;
    assert_eq!(status, 400);

    // A symlinked file inside the location that points
    // outside: also a 400 (canonical check, not lexical).
    std::fs::write(outside.join("secret.txt"), b"outside").unwrap();
    std::os::unix::fs::symlink(outside.join("secret.txt"), repo.join("src").join("link.txt"))
        .unwrap();
    let (status, _) = h.create_worktree("location-1", "src/link.txt", None).await;
    assert_eq!(status, 400);

    // A symlinked file that stays inside the binding is still
    // a file, not a directory: a worktree must be a directory,
    // so it stays a 400 (containment is not the question here,
    // the directory check is).
    std::fs::write(repo.join("src").join("real.txt"), b"inside").unwrap();
    std::os::unix::fs::symlink(repo.join("src").join("real.txt"), repo.join("src").join("ok.txt"))
        .unwrap();
    let (status, _) = h.create_worktree("location-1", "src/ok.txt", None).await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn archive_guards_409() {
    let dir = temp_dir("archive");
    let repo = dir.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let h = Harness::serve(&dir).await;
    let repo_str = repo.to_string_lossy().into_owned();
    h.ok("/bindings/create", json!({"display_name": "repo", "path": repo_str})).await;
    h.ok("/projects/create", json!({"name": "p", "description": "d"})).await;
    let (status, _) = h.create_location("workspace-1", "root", &repo_str).await;
    assert_eq!(status, 200);
    let (status, _) = h.create_worktree("location-1", "", None).await;
    assert_eq!(status, 200);

    // Binding with an active Location: 409.
    let (status, body) = h.post("/bindings/workspace-1/archive", json!({})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "binding_has_active_locations");
    // Location with an active Worktree: 409.
    let (status, body) = h.post("/locations/location-1/archive", json!({})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "location_has_active_worktrees");
    // Unknown archive targets are 404.
    for path in [
        "/bindings/workspace-99/archive",
        "/projects/project-99/archive",
        "/locations/location-99/archive",
        "/worktrees/worktree-99/archive",
    ] {
        let (status, _) = h.post(path, json!({})).await;
        assert_eq!(status, 404, "{path}");
    }

    // Worktree archives (no Patch store in P3-A, so nothing
    // blocks); then Location, then Binding; re-archive is an
    // idempotent 200 and list hides the archived rows while
    // get still returns them with status.
    let (status, archived) = h.post("/worktrees/worktree-1/archive", json!({})).await;
    assert_eq!(status, 200, "{archived}");
    assert_eq!(archived["status"], "archived");
    let (status, again) = h.post("/worktrees/worktree-1/archive", json!({})).await;
    assert_eq!(status, 200, "{again}");
    assert_eq!(again["status"], "archived");
    let (status, _) = h.post("/locations/location-1/archive", json!({})).await;
    assert_eq!(status, 200);
    let (status, _) = h.post("/bindings/workspace-1/archive", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(h.get_ok("/worktrees/list").await.as_array().unwrap().len(), 0);
    assert_eq!(h.get_ok("/locations/list").await.as_array().unwrap().len(), 0);
    assert_eq!(h.get_ok("/bindings/list").await.as_array().unwrap().len(), 0);
    assert_eq!(h.get_ok("/worktrees/worktree-1").await["status"], "archived");
    assert_eq!(h.get_ok("/locations/location-1").await["status"], "archived");
    assert_eq!(h.get_ok("/bindings/workspace-1").await["status"], "archived");

    // Project archives any time (pure metadata).
    let (status, body) = h.post("/projects/project-1/archive", json!({})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "archived");
    assert_eq!(h.get_ok("/projects/list").await.as_array().unwrap().len(), 0);
    assert_eq!(h.get_ok("/projects/project-1").await["status"], "archived");
}

#[tokio::test]
async fn tasks_execute_accepts_and_validates_worktree_id() {
    // The task flow needs a model to reach draft_ready: a
    // local fake DeepSeek HTTP service (effect_smoke pattern),
    // never the real API and never a real credential.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
                    return;
                };
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (addr, handle)
    }

    let dir = temp_dir("taskexec");
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    let notes = dir.join("files").join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let (model_addr, model) = start_fake_model("# report\n\n要点：alpha。\n\n结论：完成。").await;
    let state = new_state_with_files_root(dir.join("files"))
        .with_tasks_path(dir.join("tasks.jsonl"))
        .with_task_drafts_dir(dir.join("drafts"))
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_binding_state_path(dir.join("bindings.jsonl"))
        .with_project_state_path(dir.join("projects.jsonl"))
        .with_location_state_path(dir.join("locations.jsonl"))
        .with_worktree_state_path(dir.join("worktrees.jsonl"))
        .with_deepseek(mock_gateway::deepseek::DeepSeekClient::new(
            mock_gateway::deepseek::DeepSeekConfig {
                base_url: format!("http://{model_addr}"),
                model: "fixture-3b".to_string(),
                api_key: Some("test-only-fake-key".to_string()),
            },
        ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let post = |path: &str, value: Value| {
        let client = client.clone();
        let url = format!("{url}{path}");
        async move {
            let response = client.post(url).json(&value).send().await.unwrap();
            let status = response.status().as_u16();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            (status, body)
        }
    };
    let get = |path: &str| {
        let client = client.clone();
        let url = format!("{url}{path}");
        async move {
            let response = client.get(url).send().await.unwrap();
            let status = response.status().as_u16();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            (status, body)
        }
    };

    // A valid worktree to state, and an archived one to reject.
    let repo_str = repo.to_string_lossy().into_owned();
    let (status, _) = post(
        "/bindings/create",
        json!({"display_name": "repo", "path": repo_str}),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = post(
        "/locations/create",
        json!({"workspace_id": "workspace-1", "name": "root", "path": repo_str}),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = post(
        "/worktrees/create",
        json!({"location_id": "location-1", "directory_relative": "", "branch_label": "main"}),
    )
    .await;
    assert_eq!(status, 200);

    // Run a task to draft_ready.
    let (_, task_body) = post("/tasks/create", json!({"directory": "notes"})).await;
    let task_id = task_body["task_id"].as_str().unwrap().to_string();
    let (status, _) = post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, 200);
    let mut task = Value::Null;
    for _ in 0..200 {
        let (_, got) = get(&format!("/tasks/get?task_id={task_id}")).await;
        if got["status"] == "draft_ready" {
            task = got;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(task["status"], "draft_ready", "task never reached draft_ready: {task}");
    let task_approval = task["approval_id"].as_str().unwrap().to_string();
    let (status, _) = post(
        "/approval/decide",
        json!({"tool_call_id": task_approval, "status": "approved"}),
    )
    .await;
    assert_eq!(status, 200);

    // Unknown worktree: 400. Valid worktree: 200, completed.
    let (status, body) = post(
        "/tasks/execute",
        json!({"task_id": task_id, "worktree_id": "worktree-99"}),
    )
    .await;
    assert_eq!(status, 400, "{body}");
    let (status, executed) = post(
        "/tasks/execute",
        json!({"task_id": task_id, "worktree_id": "worktree-1"}),
    )
    .await;
    assert_eq!(status, 200, "{executed}");
    assert_eq!(executed["status"], "completed");

    server.abort();
    model.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn restart_persists_records_and_counters() {
    let dir = temp_dir("restart");
    let repo = dir.join("repo");
    std::fs::create_dir_all(repo.join("src")).unwrap();
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let h = Harness::serve(&dir).await;
    let repo_str = repo.to_string_lossy().into_owned();
    h.ok("/bindings/create", json!({"display_name": "repo", "path": repo_str})).await;
    h.ok("/projects/create", json!({"name": "p", "description": "d"})).await;
    let (status, _) = h.create_location("workspace-1", "root", &repo_str).await;
    assert_eq!(status, 200);
    let (status, _) = h.create_worktree("location-1", "src", None).await;
    assert_eq!(status, 200);
    // Abort the server WITHOUT dropping the harness: Drop
    // removes the temp dir, and the restart must read the
    // same state files back.
    let first = h;
    first.server.abort();
    std::mem::forget(first);

    // Restart: same state files, records and counters reload.
    let h = Harness::serve(&dir).await;
    assert_eq!(h.get_ok("/bindings/list").await.as_array().unwrap().len(), 1);
    assert_eq!(h.get_ok("/projects/list").await.as_array().unwrap().len(), 1);
    assert_eq!(h.get_ok("/locations/list").await.as_array().unwrap().len(), 1);
    assert_eq!(h.get_ok("/worktrees/list").await.as_array().unwrap().len(), 1);
    assert_eq!(h.get_ok("/bindings/workspace-1").await["display_name"], "repo");
    assert_eq!(h.get_ok("/worktrees/worktree-1").await["directory_relative"], "src");
    // Counters continue past the largest persisted id.
    let next = h.ok("/bindings/create", json!({"display_name": "repo-2", "path": repo_str})).await;
    assert_eq!(next["workspace_id"], "workspace-2");
    let next = h.ok("/projects/create", json!({"name": "p2", "description": "d2"})).await;
    assert_eq!(next["project_id"], "project-2");
    let (status, next) = h.create_location("workspace-2", "root", &repo_str).await;
    assert_eq!(status, 200, "{next}");
    assert_eq!(next["location_id"], "location-2");
    let (status, next) = h.create_worktree("location-2", "", None).await;
    assert_eq!(status, 200, "{next}");
    assert_eq!(next["worktree_id"], "worktree-2");
}

#[tokio::test]
async fn loop_start_freezes_binding_into_snapshot() {
    // P3-A §3.6: a loop started with worktree_id freezes the
    // binding (location, worktree, root digest) into its
    // Execution snapshot; an unbound loop leaves the three
    // columns NULL; an unknown worktree is a 400 before the
    // loop starts. SQLite mode only: JSONL mode has no
    // snapshot rows, so this test opens the database directly
    // (p0e_recovery pattern) instead of trusting a view.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
                    return;
                };
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                loop {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        (addr, handle)
    }

    let dir = temp_dir("freeze");
    let repo = dir.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let (model_addr, model) = start_fake_model("done, no tools needed.").await;
    let state = new_state_with_files_root(dir.join("files"))
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_sqlite_path(dir.join("gateway.sqlite3"))
        .with_binding_state_path(dir.join("bindings.jsonl"))
        .with_project_state_path(dir.join("projects.jsonl"))
        .with_location_state_path(dir.join("locations.jsonl"))
        .with_worktree_state_path(dir.join("worktrees.jsonl"))
        .with_deepseek_backends(
            mock_gateway::deepseek::DeepSeekConfig {
                base_url: format!("http://{model_addr}"),
                model: "fixture-3b".to_string(),
                api_key: Some("test-only-fake-key".to_string()),
            },
            None,
            None,
        )
        .expect("backends");
    state.sqlite_import_and_switch().expect("switch");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let post = |path: &str, value: Value| {
        let client = client.clone();
        let url = format!("{url}{path}");
        async move {
            let response = client.post(url).json(&value).send().await.unwrap();
            let status = response.status().as_u16();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            (status, body)
        }
    };

    let repo_str = repo.to_string_lossy().into_owned();
    let (status, _) = post(
        "/bindings/create",
        json!({"display_name": "repo", "path": repo_str}),
    )
    .await;
    assert_eq!(status, 200);
    let (status, location) = post(
        "/locations/create",
        json!({"workspace_id": "workspace-1", "name": "root", "path": repo_str}),
    )
    .await;
    assert_eq!(status, 200, "{location}");
    let root_digest = location["root_sha256"].as_str().unwrap().to_string();
    let (status, _) = post(
        "/worktrees/create",
        json!({"location_id": "location-1", "directory_relative": "", "branch_label": "main"}),
    )
    .await;
    assert_eq!(status, 200);

    // Unknown worktree: 400, before the loop starts.
    let (status, _) = post(
        "/agent/loop/start",
        json!({"prompt": "hi", "worktree_id": "worktree-99"}),
    )
    .await;
    assert_eq!(status, 400);

    // Bound loop: snapshot carries the three frozen values.
    let (status, started) = post(
        "/agent/loop/start",
        json!({"prompt": "hi", "worktree_id": "worktree-1"}),
    )
    .await;
    assert_eq!(status, 200, "{started}");
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let conn = mock_gateway::sqlite_store::open_db(&dir.join("gateway.sqlite3")).expect("open db");
    let executions = mock_gateway::sqlite_store::list_executions(&conn).expect("list executions");
    let execution = executions
        .iter()
        .find(|e| e.loop_id.as_deref() == Some(loop_id.as_str()))
        .expect("loop execution");
    let snapshot_id = execution.workspace_snapshot_id.clone().expect("snapshot id");
    let snapshot = mock_gateway::sqlite_store::get_snapshot(&conn, &snapshot_id)
        .expect("get snapshot")
        .expect("snapshot row");
    assert_eq!(snapshot.binding_location_id.as_deref(), Some("location-1"));
    assert_eq!(snapshot.binding_worktree_id.as_deref(), Some("worktree-1"));
    assert_eq!(snapshot.binding_root_digest.as_deref(), Some(root_digest.as_str()));

    // Unbound loop: the three columns stay NULL.
    let (status, started) = post("/agent/loop/start", json!({"prompt": "hi"})).await;
    assert_eq!(status, 200, "{started}");
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let executions = mock_gateway::sqlite_store::list_executions(&conn).expect("list executions");
    let execution = executions
        .iter()
        .find(|e| e.loop_id.as_deref() == Some(loop_id.as_str()))
        .expect("loop execution");
    let snapshot_id = execution.workspace_snapshot_id.clone().expect("snapshot id");
    let snapshot = mock_gateway::sqlite_store::get_snapshot(&conn, &snapshot_id)
        .expect("get snapshot")
        .expect("snapshot row");
    assert_eq!(snapshot.binding_location_id, None);
    assert_eq!(snapshot.binding_worktree_id, None);
    assert_eq!(snapshot.binding_root_digest, None);

    server.abort();
    model.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
