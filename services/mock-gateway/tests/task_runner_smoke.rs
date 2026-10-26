//! Task runner tests: gateway-run tasks reach draft_ready with no
//! window clicks, execute writes exactly the approved draft,
//! reject keeps the draft and blocks execute, no-key runs are
//! refused with no echo draft, and interrupted tasks resume.
//! The model is a local fake DeepSeek HTTP service, never the
//! real API and never a real credential.

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MODEL_TEXT: &str = "# 报告\n\n要点：alpha 与 beta。\n\n结论：完成。";

async fn raw(addr: std::net::SocketAddr, request: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect test server");
    stream.write_all(request.as_bytes()).await.expect("write test request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read test response");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    (head.lines().next().unwrap_or("").to_string(), body.to_string())
}

async fn start_fake_model(text: &str, delay: Duration) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
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
                // Read the full request: headers, then Content-Length body.
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
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    // Unique per harness: tests in one process run in parallel,
    // and a shared temp dir would cross task records and drafts.
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-taskrun-{name}-{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn build_state(
    dir: &std::path::Path,
    files_root: std::path::PathBuf,
    model_url: Option<String>,
) -> AppState {
    let mut state = AppState::with_files_root_and_approvals(files_root, Some(dir.join("approvals.jsonl")))
        .with_tasks_path(dir.join("tasks.jsonl"))
        .with_task_drafts_dir(dir.join("drafts"));
    if let Some(base) = model_url {
        state = state.with_deepseek(DeepSeekClient::new(DeepSeekConfig {
            base_url: base,
            model: "fixture-3b".to_string(),
            api_key: Some("fixture-not-real".to_string()),
        }));
    }
    state
}

struct Harness {
    dir: std::path::PathBuf,
    files_root: std::path::PathBuf,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
    model_url: Option<String>,
}

impl Harness {
    async fn new(model_url: Option<String>) -> Self {
        let dir = temp_dir("harness");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        let state = build_state(&dir, files_root.clone(), model_url.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self { dir, files_root, addr, server, model_url }
    }

    // Build state over a pre-written tasks.jsonl, so the test
    // can seed history lines (including a stale running line
    // superseded by a later terminal line for the same task).
    // The seed is terminated with a newline: the restore append
    // writes one line per record, and a missing trailing newline
    // would glue the appended record onto the last seed line.
    async fn new_with_tasks_jsonl(content: &str) -> Self {
        let dir = temp_dir("seeded");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        let mut seeded = content.to_string();
        if !seeded.ends_with('\n') {
            seeded.push('\n');
        }
        std::fs::write(dir.join("tasks.jsonl"), seeded).unwrap();
        let state = build_state(&dir, files_root.clone(), None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self { dir, files_root, addr, server, model_url: None }
    }

    async fn restart(&mut self) {
        self.server.abort();
        let state = build_state(&self.dir, self.files_root.clone(), self.model_url.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.addr = listener.local_addr().unwrap();
        self.server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
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

    async fn create(&self, directory: &str) -> String {
        let (head, body) =
            self.post("/tasks/create", &serde_json::to_string(&json!({"directory": directory})).unwrap()).await;
        assert!(head.contains("200"), "create: {head} {body}");
        body["task_id"].as_str().unwrap().to_string()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn run_to_draft_then_execute_writes_exact_draft_and_survives_restart() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT, Duration::ZERO).await;
    let mut h = Harness::new(Some(format!("http://{model_addr}"))).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    std::fs::write(notes.join("b.txt"), "beta").unwrap();
    let task_id = h.create("notes").await;
    let (head, body) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "run: {head} {body}");
    assert_eq!(body["status"], "running");
    let record = h.poll_status(&task_id, "draft_ready").await;
    assert_eq!(record["artifact_path"], "notes/report.md");
    let approval_id = record["approval_id"].as_str().unwrap().to_string();
    assert!(record["draft_sha256"].as_str().map(|s| s.len()) == Some(64));
    assert_eq!(record["draft_bytes"], MODEL_TEXT.len() as u64);
    let (head, draft) = h.get(&format!("/tasks/draft?task_id={task_id}")).await;
    assert!(head.contains("200"), "draft: {head} {draft}");
    assert_eq!(draft["draft"], MODEL_TEXT);
    // Approve, then execute: the artifact must be byte-identical
    // to the approved draft, and the task completes.
    let (head, _) = h
        .post("/approval/decide", &format!(r#"{{"tool_call_id":"{approval_id}","status":"approved"}}"#))
        .await;
    assert!(head.contains("200"), "decide: {head}");
    let (head, body) = h.post("/tasks/execute", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "execute: {head} {body}");
    assert_eq!(body["status"], "completed");
    assert_eq!(std::fs::read_to_string(notes.join("report.md")).unwrap(), MODEL_TEXT);
    // Restart: the completed record and the draft survive.
    h.restart().await;
    let record = h.task(&task_id).await;
    assert_eq!(record["status"], "completed");
    assert_eq!(record["artifact_path"], "notes/report.md");
    let (head, draft) = h.get(&format!("/tasks/draft?task_id={task_id}")).await;
    assert!(head.contains("200"), "draft after restart: {head}");
    assert_eq!(draft["draft"], MODEL_TEXT);
    model.abort();
}

#[tokio::test]
async fn reject_keeps_draft_and_blocks_execute() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT, Duration::ZERO).await;
    let h = Harness::new(Some(format!("http://{model_addr}"))).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create("notes").await;
    h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    let record = h.poll_status(&task_id, "draft_ready").await;
    assert!(record["approval_id"].is_string());
    let (head, body) = h.post("/tasks/reject", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "reject: {head} {body}");
    assert_eq!(body["status"], "rejected");
    let (head, draft) = h.get(&format!("/tasks/draft?task_id={task_id}")).await;
    assert!(head.contains("200"), "draft after reject: {head}");
    assert_eq!(draft["draft"], MODEL_TEXT);
    let (head, _) = h.post("/tasks/execute", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("409"), "execute after reject: {head}");
    assert!(!notes.join("report.md").exists());
    model.abort();
}

#[tokio::test]
async fn run_without_model_is_refused_and_leaves_created() {
    let h = Harness::new(None).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create("notes").await;
    let (head, body) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("503"), "run without model: {head} {body}");
    assert!(body.to_string().contains("model_not_configured"), "{body}");
    assert_eq!(h.task(&task_id).await["status"], "created");
}

#[tokio::test]
async fn run_on_empty_directory_fails_with_reason() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT, Duration::ZERO).await;
    let h = Harness::new(Some(format!("http://{model_addr}"))).await;
    let notes = h.files_root.join("empty");
    std::fs::create_dir_all(&notes).unwrap();
    let task_id = h.create("empty").await;
    h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    let record = h.poll_status(&task_id, "failed").await;
    assert_eq!(record["error"], "目录下没有可读文件");
    model.abort();
}

#[tokio::test]
async fn restart_interrupted_task_resumes_and_regenerates() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT, Duration::from_secs(3)).await;
    let mut h = Harness::new(Some(format!("http://{model_addr}"))).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create("notes").await;
    let (head, body) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "run: {head} {body}");
    assert_eq!(h.task(&task_id).await["status"], "running");
    // Simulate a crash mid-model-call: the new process loads the
    // task as interrupted, with no draft to restore.
    h.restart().await;
    assert_eq!(h.task(&task_id).await["status"], "interrupted");
    let (head, body) = h.post("/tasks/resume", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "resume: {head} {body}");
    let record = h.poll_status(&task_id, "draft_ready").await;
    assert!(record["approval_id"].is_string());
    let (head, draft) = h.get(&format!("/tasks/draft?task_id={task_id}")).await;
    assert!(head.contains("200"), "draft: {head}");
    assert_eq!(draft["draft"], MODEL_TEXT);
    model.abort();
}

#[tokio::test]
async fn two_restarts_keep_terminal_states_and_fields() {
    // Regression for the window finding: a stale running line
    // superseded by a later terminal line must not come back as
    // interrupted on the next load, and terminal records must
    // keep artifact_path, approval_id, digest and bytes across
    // two consecutive restarts. Only the genuinely running
    // task converts, and only once.
    let digest = "a".repeat(64);
    let line = |task_id: &str, status: &str, artifact: Value, approval: Value| {
        serde_json::to_string(&json!({
            "task_id": task_id,
            "directory": "notes",
            "artifact_path": artifact,
            "status": status,
            "error": Value::Null,
            "approval_id": approval,
            "draft_sha256": digest.as_str(),
            "draft_bytes": 162,
        }))
        .unwrap()
    };
    let seed = [
        line("task-1", "running", Value::Null, Value::Null),
        line("task-1", "completed", json!("notes/report.md"), json!("approval-4")),
        line("task-2", "rejected", json!("notes/report.md"), json!("approval-2")),
        line("task-3", "cancelled", json!("notes/report.md"), json!("approval-7")),
        line("task-4", "draft_ready", json!("notes/report.md"), json!("approval-8")),
        line("task-5", "running", Value::Null, Value::Null),
    ]
    .join("\n");
    let mut h = Harness::new_with_tasks_jsonl(&seed).await;
    let tasks_path = h.dir.join("tasks.jsonl");
    let count_lines = || {
        std::fs::read_to_string(&tasks_path).unwrap().lines().filter(|l| !l.trim().is_empty()).count()
    };
    // First load: only task-5 converts, so exactly one line is
    // appended; task-1 keeps its completed line and fields.
    assert_eq!(count_lines(), 7);
    let expect = |record: &Value, status: &str, artifact: &str, approval: &str| {
        assert_eq!(record["status"], status, "{record}");
        assert_eq!(record["artifact_path"], artifact, "{record}");
        assert_eq!(record["approval_id"], approval, "{record}");
        assert_eq!(record["draft_sha256"].as_str(), Some(digest.as_str()), "{record}");
        assert_eq!(record["draft_bytes"], 162, "{record}");
    };
    expect(&h.task("task-1").await, "completed", "notes/report.md", "approval-4");
    expect(&h.task("task-2").await, "rejected", "notes/report.md", "approval-2");
    expect(&h.task("task-3").await, "cancelled", "notes/report.md", "approval-7");
    expect(&h.task("task-4").await, "draft_ready", "notes/report.md", "approval-8");
    assert_eq!(h.task("task-5").await["status"], "interrupted");
    // Second load: nothing is running anymore, so nothing is
    // appended and every record keeps its fields.
    h.restart().await;
    assert_eq!(count_lines(), 7);
    expect(&h.task("task-1").await, "completed", "notes/report.md", "approval-4");
    expect(&h.task("task-2").await, "rejected", "notes/report.md", "approval-2");
    expect(&h.task("task-3").await, "cancelled", "notes/report.md", "approval-7");
    expect(&h.task("task-4").await, "draft_ready", "notes/report.md", "approval-8");
    assert_eq!(h.task("task-5").await["status"], "interrupted");
}
