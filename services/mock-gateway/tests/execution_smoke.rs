//! P0 phase 1 shell tests: Task Runner and Loop Engine each create
//! one Execution record when they start an attempt, keeping the old
//! task-N / loop-N id as a reference; the counter survives restarts
//! and old paths keep working. Gateway kernel only: no client, no
//! UI. The model is a local fake DeepSeek HTTP service, never the
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
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-exec-{name}-{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

struct Harness {
    dir: std::path::PathBuf,
    files_root: std::path::PathBuf,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
    model_url: Option<String>,
    loop_mode: bool,
}

impl Harness {
    fn build_state(
        dir: &std::path::Path,
        files_root: std::path::PathBuf,
        model_url: Option<String>,
        loop_mode: bool,
    ) -> AppState {
        let mut state = AppState::with_files_root_and_approvals(files_root, Some(dir.join("approvals.jsonl")))
            .with_tasks_path(dir.join("tasks.jsonl"))
            .with_task_drafts_dir(dir.join("drafts"))
            .with_executions_path(dir.join("executions.jsonl"));
        if loop_mode {
            let config = DeepSeekConfig {
                base_url: model_url.clone().unwrap(),
                model: "fixture-3b".to_string(),
                api_key: Some("fake-execution-key".into()),
            };
            state = state
                .with_deepseek_backends(config, Some(dir.join("loops.jsonl")), None)
                .expect("loop backends")
                .with_auto_drive(false);
        } else if let Some(base) = model_url {
            state = state.with_deepseek(DeepSeekClient::new(DeepSeekConfig {
                base_url: base,
                model: "fixture-3b".to_string(),
                api_key: Some("fake-execution-key".into()),
            }));
        }
        state
    }

    async fn new(model_url: Option<String>) -> Self {
        Self::new_inner(model_url, false).await
    }

    async fn new_loop(model_url: String) -> Self {
        Self::new_inner(Some(model_url), true).await
    }

    async fn new_inner(model_url: Option<String>, loop_mode: bool) -> Self {
        let dir = temp_dir("harness");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        let state = Self::build_state(&dir, files_root.clone(), model_url.clone(), loop_mode);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self { dir, files_root, addr, server, model_url, loop_mode }
    }

    async fn new_with_executions_jsonl(content: &str, model_url: Option<String>) -> Self {
        let dir = temp_dir("seeded");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        let mut seeded = content.to_string();
        if !seeded.ends_with('\n') {
            seeded.push('\n');
        }
        std::fs::write(dir.join("executions.jsonl"), seeded).unwrap();
        let state = Self::build_state(&dir, files_root.clone(), model_url.clone(), false);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self { dir, files_root, addr, server, model_url, loop_mode: false }
    }

    async fn restart(&mut self) {
        self.server.abort();
        let state = Self::build_state(&self.dir, self.files_root.clone(), self.model_url.clone(), self.loop_mode);
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

    async fn create_task(&self, dirname: &str) -> String {
        let (head, body) = self.post("/tasks/create", &format!(r#"{{"directory":"{dirname}"}}"#)).await;
        assert!(head.contains("200"), "create task: {head} {body}");
        body["task_id"].as_str().unwrap().to_string()
    }

    async fn executions(&self) -> Value {
        let (head, body) = self.get("/executions/list").await;
        assert!(head.contains("200"), "list executions: {head} {body}");
        body
    }

    async fn task_status(&self, task_id: &str) -> String {
        let (head, body) = self.get(&format!("/tasks/get?task_id={task_id}")).await;
        assert!(head.contains("200"), "get task: {head} {body}");
        body["status"].as_str().unwrap().to_string()
    }
}

#[tokio::test]
async fn task_run_creates_execution_referencing_task() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT, Duration::ZERO).await;
    let h = Harness::new(Some(format!("http://{model_addr}"))).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create_task("notes").await;
    let (head, _) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "run: {head}");

    let list = h.executions().await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    let exec = &list[0];
    assert_eq!(exec["execution_id"], "execution-1");
    assert_eq!(exec["task_id"], task_id);
    assert!(exec["loop_id"].is_null());
    assert_eq!(exec["status"], "running");

    let (head, got) = h.get("/executions/get?execution_id=execution-1").await;
    assert!(head.contains("200"), "get: {head}");
    assert_eq!(got["task_id"], task_id);
    let (head, _) = h.get("/executions/get?execution_id=execution-99").await;
    assert!(head.contains("404"), "unknown execution must 404: {head}");
    model.abort();
}

#[tokio::test]
async fn loop_start_creates_execution_referencing_loop() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT, Duration::ZERO).await;
    let h = Harness::new_loop(format!("http://{model_addr}")).await;
    // auto_drive is off in the loop harness: start creates the loop
    // record but no driver runs, so no model call is in flight.
    let (head, body) = h.post("/agent/loop/start", r#"{"prompt":"hello execution"}"#).await;
    assert!(head.contains("200"), "loop start: {head} {body}");
    let loop_id = body["loop_id"].as_str().unwrap().to_string();
    assert_eq!(loop_id, "loop-1");

    let list = h.executions().await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    let exec = &list[0];
    assert_eq!(exec["execution_id"], "execution-1");
    assert!(exec["task_id"].is_null());
    assert_eq!(exec["loop_id"], loop_id);
    assert_eq!(exec["status"], "running");
    model.abort();
}

#[tokio::test]
async fn execution_counter_resumes_across_restart() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT, Duration::ZERO).await;
    let h = Harness::new_with_executions_jsonl(
        r#"{"execution_id":"execution-1","task_id":"task-9","loop_id":null,"status":"running"}"#,
        Some(format!("http://{model_addr}")),
    )
    .await;
    // Phase 1 performs no restart conversion: the seeded record
    // reloads verbatim.
    let list = h.executions().await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["execution_id"], "execution-1");
    assert_eq!(list[0]["task_id"], "task-9");
    assert_eq!(list[0]["status"], "running");

    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create_task("notes").await;
    let (head, _) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "run: {head}");
    let list = h.executions().await;
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert_eq!(list[1]["execution_id"], "execution-2");
    assert_eq!(list[1]["task_id"], task_id);
    model.abort();
}

#[tokio::test]
async fn task_resume_creates_new_execution() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT, Duration::from_secs(3)).await;
    let mut h = Harness::new(Some(format!("http://{model_addr}"))).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create_task("notes").await;
    let (head, _) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "run: {head}");
    assert_eq!(h.task_status(&task_id).await, "running");
    assert_eq!(h.executions().await.as_array().unwrap().len(), 1);

    // Simulate a crash mid-model-call: the new process loads the
    // task as interrupted.
    h.restart().await;
    assert_eq!(h.task_status(&task_id).await, "interrupted");
    let (head, _) = h.post("/tasks/resume", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "resume: {head}");
    // A resume spawns a fresh runner attempt, so it gets a fresh
    // Execution; resume-continuation semantics are phase 4 (Q6).
    let list = h.executions().await;
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert_eq!(list[1]["execution_id"], "execution-2");
    assert_eq!(list[1]["task_id"], task_id);
    model.abort();
}
