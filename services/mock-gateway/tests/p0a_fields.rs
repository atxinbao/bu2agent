//! P0-A contract field landing tests: additive fields only, no
//! gate, state or client changes. New Execution and Effect rows
//! carry origin + schema version (+ the idempotency key for
//! effects with an execution and a digest); pre-P0-A lines read
//! back as schema 0 / origin "unknown" and are never backfilled.
//! Gateway kernel only: no client, no UI. The model is a local
//! fake DeepSeek HTTP service, never the real API.

use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::server::{app, resolve_origin_device_id, AppState};
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
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p0a-{name}-{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

struct Harness {
    files_root: std::path::PathBuf,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Harness {
    fn build_state(
        dir: &std::path::Path,
        files_root: std::path::PathBuf,
        model_url: Option<String>,
        origin: Option<&str>,
    ) -> AppState {
        let mut state = AppState::with_files_root_and_approvals(
            files_root,
            Some(dir.join("approvals.jsonl")),
        )
        .with_tasks_path(dir.join("tasks.jsonl"))
        .with_task_drafts_dir(dir.join("drafts"))
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"));
        if let Some(origin) = origin {
            state = state.with_origin_device_id(origin.to_string());
        }
        if let Some(base) = model_url {
            state = state.with_deepseek(DeepSeekClient::new(DeepSeekConfig {
                base_url: base,
                model: "fixture-3b".to_string(),
                api_key: Some("test".to_string()),
            }));
        }
        state
    }

    async fn new(model_url: Option<String>, origin: Option<&str>) -> Self {
        Self::new_seeded(model_url, origin, None).await
    }

    async fn new_seeded(
        model_url: Option<String>,
        origin: Option<&str>,
        seeded_executions: Option<&str>,
    ) -> Self {
        let dir = temp_dir("harness");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        if let Some(content) = seeded_executions {
            let mut content = content.to_string();
            if !content.ends_with('\n') {
                content.push('\n');
            }
            std::fs::write(dir.join("executions.jsonl"), content).unwrap();
        }
        let state = Self::build_state(&dir, files_root.clone(), model_url, origin);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self { files_root, addr, server }
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

    async fn executions(&self) -> Vec<Value> {
        let (head, body) = self.get("/executions/list").await;
        assert!(head.contains("200"), "list executions: {head} {body}");
        body.as_array().unwrap().clone()
    }

    async fn effects(&self) -> Vec<Value> {
        let (head, body) = self.get("/effects/list").await;
        assert!(head.contains("200"), "list effects: {head} {body}");
        body.as_array().unwrap().clone()
    }

    async fn create_task(&self, dirname: &str) -> String {
        let (head, body) = self
            .post("/tasks/create", &format!(r#"{{"directory":"{dirname}"}}"#))
            .await;
        assert!(head.contains("200"), "create task: {head} {body}");
        body["task_id"].as_str().unwrap().to_string()
    }

    async fn run_task_to_draft(&self, task_id: &str) -> String {
        let (head, _) = self
            .post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#))
            .await;
        assert!(head.contains("200"), "run: {head}");
        let mut task = Value::Null;
        for _ in 0..200 {
            let (_, got) = self.get(&format!("/tasks/get?task_id={task_id}")).await;
            if got["status"] == "draft_ready" {
                task = got;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(task["status"], "draft_ready", "task never reached draft_ready: {task}");
        task["approval_id"].as_str().unwrap().to_string()
    }

    async fn approve(&self, approval_id: &str) {
        let (head, body) = self
            .post(
                "/approval/decide",
                &format!(r#"{{"tool_call_id":"{approval_id}","status":"approved"}}"#),
            )
            .await;
        assert!(head.contains("200"), "decide: {head} {body}");
    }
}

#[test]
fn resolve_origin_device_id_defaults_to_mac_local() {
    assert_eq!(resolve_origin_device_id(Some("edge-1".to_string())), "edge-1");
    assert_eq!(resolve_origin_device_id(None), "mac-local");
    assert_eq!(resolve_origin_device_id(Some(String::new())), "mac-local");
}

#[tokio::test]
async fn task_run_execution_carries_p0a_fields() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT).await;
    let h = Harness::new(Some(format!("http://{model_addr}")), None).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create_task("notes").await;
    let (head, _) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "run: {head}");

    let list = h.executions().await;
    assert_eq!(list.len(), 1);
    let exec = &list[0];
    assert_eq!(exec["execution_id"], "execution-1");
    assert_eq!(exec["task_id"], task_id);
    assert!(exec["loop_id"].is_null());
    assert_eq!(exec["status"], "running");
    // P0-A additive fields; the reserved references stay null.
    assert!(exec["session_id"].is_null());
    assert!(exec["worker_session_id"].is_null());
    assert!(exec["workspace_snapshot_id"].is_null());
    assert_eq!(exec["origin_device_id"], "mac-local");
    assert_eq!(exec["schema_version"], 1);
    model.abort();
}

#[tokio::test]
async fn origin_override_stamps_execution_and_effect_rows() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT).await;
    let h = Harness::new(Some(format!("http://{model_addr}")), Some("worker-9")).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create_task("notes").await;
    let (head, _) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "run: {head}");
    assert_eq!(h.executions().await[0]["origin_device_id"], "worker-9");

    // P0-B: a direct write is its own attempt, so it now carries
    // a minimal Execution and a non-null key; provenance stamps
    // are unchanged.
    let (head, body) = h
        .post(
            "/approval/request",
            r#"{"tool":"fs.write","path":"a.txt","content_base64":"aGVsbG8="}"#,
        )
        .await;
    assert!(head.contains("200"), "request approval: {head} {body}");
    let approval = body["tool_call_id"].as_str().unwrap().to_string();
    h.approve(&approval).await;
    let (head, body) = h
        .post(
            "/files/write",
            &format!(r#"{{"path":"a.txt","content_base64":"aGVsbG8=","approval_id":"{approval}"}}"#),
        )
        .await;
    assert!(head.contains("200"), "write: {head} {body}");
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    let effect = &effects[0];
    assert_eq!(effect["capability"], "fs");
    assert_eq!(effect["operation"], "write");
    let execution_id = effect["execution_id"].as_str().expect("minimal execution");
    assert!(execution_id.starts_with("execution-"));
    assert!(effect["idempotency_key"].is_string());
    assert_eq!(effect["origin_device_id"], "worker-9");
    assert_eq!(effect["schema_version"], 1);
    model.abort();
}

#[tokio::test]
async fn task_execute_effect_carries_idempotency_key() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT).await;
    let h = Harness::new(Some(format!("http://{model_addr}")), None).await;
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create_task("notes").await;
    let task_approval = h.run_task_to_draft(&task_id).await;
    h.approve(&task_approval).await;
    let (head, executed) = h
        .post("/tasks/execute", &format!(r#"{{"task_id":"{task_id}"}}"#))
        .await;
    assert!(head.contains("200"), "execute: {head} {executed}");
    assert_eq!(executed["status"], "completed");

    let exec = h
        .executions()
        .await
        .into_iter()
        .find(|e| e["task_id"] == task_id)
        .expect("task execution");
    let execution_id = exec["execution_id"].as_str().unwrap().to_string();
    let effect = h
        .effects()
        .await
        .into_iter()
        .find(|e| e["capability"] == "tasks" && e["operation"] == "execute" && e["scope"] == task_id)
        .expect("task execute effect");
    assert_eq!(effect["execution_id"], execution_id);
    assert_eq!(effect["approval_id"], task_approval);
    assert_eq!(effect["status"], "completed");
    let digest = effect["input_digest"].as_str().unwrap();
    assert_eq!(
        effect["idempotency_key"],
        format!("exec:{execution_id}:tasks.execute:{task_id}:{digest}")
    );
    assert_eq!(effect["origin_device_id"], "mac-local");
    assert_eq!(effect["schema_version"], 1);
    model.abort();
}

#[tokio::test]
async fn old_execution_lines_read_as_schema_zero() {
    let (model_addr, model) = start_fake_model(MODEL_TEXT).await;
    let h = Harness::new_seeded(
        Some(format!("http://{model_addr}")),
        None,
        Some(r#"{"execution_id":"execution-1","task_id":"task-9","loop_id":null,"status":"running"}"#),
    )
    .await;
    // Pre-P0-A line reloads verbatim, with provenance defaults.
    let list = h.executions().await;
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["execution_id"], "execution-1");
    assert_eq!(list[0]["task_id"], "task-9");
    assert!(list[0]["session_id"].is_null());
    assert!(list[0]["worker_session_id"].is_null());
    assert!(list[0]["workspace_snapshot_id"].is_null());
    assert_eq!(list[0]["origin_device_id"], "unknown");
    assert_eq!(list[0]["schema_version"], 0);

    // The counter resumes past the seeded row; new rows are schema 1.
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let task_id = h.create_task("notes").await;
    let (head, _) = h.post("/tasks/run", &format!(r#"{{"task_id":"{task_id}"}}"#)).await;
    assert!(head.contains("200"), "run: {head}");
    let list = h.executions().await;
    assert_eq!(list.len(), 2);
    assert_eq!(list[1]["execution_id"], "execution-2");
    assert_eq!(list[1]["origin_device_id"], "mac-local");
    assert_eq!(list[1]["schema_version"], 1);
    model.abort();
}
