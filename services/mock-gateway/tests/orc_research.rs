//! ORC-4 tests (plan §9, Q-R4-01–12): the research
//! sub-agent — an isolated read-only loop the Planner
//! asks for in its first-round reply, whose report
//! lands in Evidence and whose summary feeds the
//! second Planner prompt.
//!
//! S1: a research proposal of `fs.write` is recorded
//! Failed + `invalid_tool`, with no approval and no
//! Effect; the three read tools succeed.
//! S2: a main loop and a research loop keep separate
//! call records and final texts; the report summary
//! reaches the second Planner prompt (not the main
//! loop's context — D8).
//! S3: the research report lands as one Evidence row
//! (reason `research_report`, full text + hash); the
//! row survives a restart while the completed loop
//! keeps its status but loses its final text, and a
//! still-running research loop becomes `Interrupted`
//! across the same restart.
//! S4: `research/start` is 200 in the planning phase
//! (running, no Executor step yet) and 409
//! `research_planning_only` once an Executor step is
//! prepared; an unknown task is 404.
//!
//! The multi-shot fake DeepSeek from orc_roles.rs is
//! reused, extended: queued items are full message
//! objects (text or tool_calls), so one queue serves
//! the Planner, the Executor and both loop models in
//! call order. The fake answers connections strictly
//! in accept order (no per-connection spawn), so a
//! gated first request cannot let a later request
//! steal its queued reply. The fake key is `test-key`;
//! no real credential is used.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::deepseek::DeepSeekConfig;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-orc4-{tag}-{}-{}",
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

/// A re-armable gate: after `arm`, the next request
/// the fake handles blocks until `release`. Later
/// requests are answered at once. The wait is async
/// (`Notify`, never a std Condvar — blocking the
/// runtime thread here deadlocks the test).
struct Gate {
    open: Mutex<bool>,
    used: Mutex<bool>,
    notify: tokio::sync::Notify,
}

impl Gate {
    fn new() -> Self {
        Self {
            open: Mutex::new(true),
            used: Mutex::new(true),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn arm(&self) {
        *self.open.lock().expect("gate lock") = false;
        *self.used.lock().expect("gate lock") = false;
    }

    fn release(&self) {
        *self.open.lock().expect("gate lock") = true;
        self.notify.notify_waiters();
    }

    async fn wait_if_first(&self) {
        {
            let mut used = self.used.lock().expect("gate lock");
            if *used {
                return;
            }
            *used = true;
        }
        loop {
            if *self.open.lock().expect("gate lock") {
                return;
            }
            self.notify.notified().await;
        }
    }
}

struct FakeDeepseek {
    #[allow(dead_code)]
    handle: tokio::task::JoinHandle<()>,
    seen: Arc<Mutex<Vec<Value>>>,
    gate: Arc<Gate>,
}

async fn start_fake_deepseek(replies: Vec<Value>) -> (String, FakeDeepseek) {
    start_fake_deepseek_canned(replies, false).await
}

async fn start_fake_deepseek_canned(
    replies: Vec<Value>,
    research_canned: bool,
) -> (String, FakeDeepseek) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let queue = Arc::new(Mutex::new(VecDeque::from(replies)));
    let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(Gate::new());
    let gate_clone = gate.clone();
    let seen_clone = seen.clone();
    let canned = Arc::new(AtomicBool::new(research_canned));
    let canned_clone = canned.clone();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            // Sequential handling (no per-connection
            // spawn): responses leave in accept order,
            // so a gated first request keeps its place
            // in the queue.
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
                String::from_utf8_lossy(&buf[header_end.expect("header end")..end]).into_owned();
            let request: Value = serde_json::from_str(&body_text).unwrap_or(Value::Null);
            seen_clone
                .lock()
                .expect("seen lock")
                .push(request.clone());
            gate_clone.wait_if_first().await;
            let is_research = request["messages"]
                .as_array()
                .and_then(|m| m.first())
                .and_then(|m| m["content"].as_str())
                .is_some_and(|system| system.contains("只读调研员"));
            let (status_line, body) = if is_research
                && canned_clone.load(Ordering::SeqCst)
            {
                (
                    "200 OK",
                    serde_json::to_string(&json!({
                        "id": "chatcmpl-test",
                        "object": "chat.completion",
                        "created": 1,
                        "model": "fake",
                        "choices": [{
                            "index": 0,
                            "message": {"role": "assistant", "content": "canned research report for phase check"},
                            "finish_reason": "stop",
                        }],
                    }))
                    .unwrap(),
                )
            } else {
                match queue.lock().expect("queue lock").pop_front() {
                    Some(message) => (
                        "200 OK",
                        serde_json::to_string(&json!({
                            "id": "chatcmpl-test",
                            "object": "chat.completion",
                            "created": 1,
                            "model": "fake",
                            "choices": [{
                                "index": 0,
                                "message": {
                                    "role": "assistant",
                                    "content": message["content"],
                                    "tool_calls": message["tool_calls"],
                                },
                                "finish_reason": message["finish_reason"],
                            }],
                        }))
                        .unwrap(),
                    ),
                    None => (
                        "500 Internal Server Error",
                        "{\"error\": \"fake deepseek queue exhausted\"}".to_string(),
                    ),
                }
            };
            let response = format!(
                "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (
        format!("http://{addr}"),
        FakeDeepseek {
            handle,
            seen,
            gate,
        },
    )
}

impl FakeDeepseek {
    fn seen_count(&self) -> usize {
        self.seen.lock().expect("seen lock").len()
    }

    fn seen_body(&self, index: usize) -> Value {
        self.seen.lock().expect("seen lock")[index].clone()
    }

    /// The tool function names of the Nth request's
    /// `tools` array (empty when the request has none).
    fn tool_names_of(&self, index: usize) -> Vec<String> {
        self.seen_body(index)["tools"]
            .as_array()
            .map(|tools| {
                tools
                    .iter()
                    .filter_map(|t| t["function"]["name"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The full prompt text of the Nth request (the
    /// last user message's content).
    fn prompt_of(&self, index: usize) -> String {
        self.seen_body(index)["messages"]
            .as_array()
            .and_then(|m| m.last())
            .and_then(|m| m["content"].as_str())
            .unwrap_or("")
            .to_string()
    }

    async fn wait_seen(&self, count: usize, secs: u64) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            if self.seen_count() >= count {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "fake saw {} requests, want {count}",
                self.seen_count()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn build_state(dir: &Path, deepseek_base_url: &str) -> AppState {
    let state = AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_tasks_path(dir.join("tasks.jsonl"))
    .with_task_drafts_dir(dir.join("drafts"))
    .with_deepseek_backends(
        DeepSeekConfig {
            base_url: deepseek_base_url.to_string(),
            model: "fixture".to_string(),
            api_key: Some("test-key".to_string()),
        },
        Some(dir.join("loops.jsonl")),
        None,
    )
    .expect("loop backends")
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
    deepseek_base_url: String,
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Harness {
    async fn serve(deepseek_base_url: &str) -> Self {
        let dir = temp_dir("research");
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
            deepseek_base_url: deepseek_base_url.to_string(),
        }
    }

    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let state = build_state(&self.dir, &self.deepseek_base_url);
        self.state = state.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server = Some(
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() }),
        );
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

    /// Poll until the task reaches `want` (or the
    /// deadline passes, failing with the last record).
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

    async fn effects(&self) -> Vec<Value> {
        let (status, value) = self.get("/effects/list").await;
        assert_eq!(status, StatusCode::OK, "effects list: {value}");
        value.as_array().unwrap().clone()
    }

    async fn evidence(&self) -> Vec<Value> {
        let (status, value) = self.get("/evidence/list").await;
        assert_eq!(status, StatusCode::OK, "evidence list: {value}");
        value.as_array().unwrap().clone()
    }

    /// Poll a loop view until it reaches a terminal
    /// status. `research` picks the research get route,
    /// otherwise the main get route.
    async fn wait_loop_terminal(&self, loop_id: &str, research: bool, secs: u64) -> Value {
        let path = if research {
            format!("/agent/loop/research/{loop_id}")
        } else {
            format!("/agent/loop/get?loop_id={loop_id}")
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
        loop {
            let (status, view) = self.get(&path).await;
            assert_eq!(status, StatusCode::OK, "get loop {loop_id}: {view}");
            let terminal = matches!(
                view["status"].as_str(),
                Some("completed" | "needs_review" | "failed" | "cancelled" | "interrupted")
            );
            if terminal {
                return view;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "loop {loop_id} stuck at {}: {view}",
                view["status"]
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    fn approvals_lines(&self) -> Vec<String> {
        match std::fs::read_to_string(self.dir.join("approvals.jsonl")) {
            Ok(text) => text
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(str::to_string)
                .collect(),
            Err(_) => Vec::new(),
        }
    }
}

fn observed_of(row: &Value) -> Value {
    match &row["observed_json"] {
        Value::String(s) => serde_json::from_str(s).unwrap(),
        other => other.clone(),
    }
}

/// One model reply: a final text answer.
fn text_msg(content: &str) -> Value {
    json!({"content": content, "finish_reason": "stop"})
}

/// One model reply: tool calls. Each call is
/// (id, function name, arguments object).
fn calls_msg(calls: Vec<(&str, &str, Value)>) -> Value {
    json!({
        "content": Value::Null,
        "tool_calls": calls.iter().map(|(id, name, args)| json!({
            "id": id,
            "type": "function",
            "function": {"name": name, "arguments": serde_json::to_string(args).unwrap()},
        })).collect::<Vec<_>>(),
        "finish_reason": "tool_calls",
    })
}

fn planner_research_msg(question: &str) -> Value {
    text_msg(
        &serde_json::to_string(&json!({
            "needs_file_change": false,
            "research": {"question": question},
            "steps": [],
            "replan_if": [],
        }))
        .unwrap(),
    )
}

fn planner_triage_msg(answer: &str) -> Value {
    text_msg(
        &serde_json::to_string(&json!({
            "needs_file_change": false,
            "answer": answer,
            "steps": [],
            "replan_if": [],
        }))
        .unwrap(),
    )
}

fn planner_steps_msg(steps: Vec<Value>) -> Value {
    text_msg(
        &serde_json::to_string(&json!({
            "needs_file_change": true,
            "steps": steps,
            "replan_if": [],
        }))
        .unwrap(),
    )
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

/// S1: a research proposal of `fs.write` is recorded
/// Failed + `invalid_tool`, with no approval and no
/// Effect; `code.search` succeeds in the same round.
#[tokio::test]
async fn s1_research_write_proposal_is_refused_at_record() {
    let (fake_url, fake) = start_fake_deepseek(vec![
        planner_research_msg("哪些文件定义了问候语？"),
        calls_msg(vec![
            (
                "w1",
                "fs_write",
                json!({"path": "proj/notes.txt", "text": "hacked\n"}),
            ),
            ("c1", "code_search", json!({"query": "hello"})),
        ]),
        text_msg("调研结论：notes.txt 定义了问候语。"),
        planner_triage_msg("问候语在 notes.txt。"),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let task_id = h.make_task().await;
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    h.wait_task_status(&task_id, "completed", 30).await;

    // The report row names the research loop.
    let evidence = h.evidence().await;
    let reports: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "research_report")
        .collect();
    assert_eq!(reports.len(), 1, "evidence: {evidence:?}");
    let observed = observed_of(reports[0]);
    let loop_id = observed["loop_id"].as_str().unwrap().to_string();
    assert_eq!(observed["task_id"], task_id);
    assert!(observed["report_text"]
        .as_str()
        .unwrap()
        .contains("调研结论"));

    // The research loop's own record: the write
    // proposal is Failed + invalid_tool, the read
    // proposal succeeded, and neither carries an
    // approval id.
    let (status, view) = h.get(&format!("/agent/loop/research/{loop_id}")).await;
    assert_eq!(status, StatusCode::OK, "research get: {view}");
    assert_eq!(view["kind"], "research");
    assert_eq!(view["status"], "completed");
    let calls = view["calls"].as_array().unwrap();
    assert_eq!(calls.len(), 2, "calls: {calls:?}");
    assert_eq!(calls[0]["tool"], "fs.write");
    assert_eq!(calls[0]["status"], "failed");
    assert_eq!(calls[0]["error"], "invalid_tool");
    assert_eq!(calls[0]["approval_id"], Value::Null);
    assert_eq!(calls[1]["tool"], "code.search");
    assert_eq!(calls[1]["status"], "succeeded");
    assert_eq!(calls[1]["approval_id"], Value::Null);

    // No approval was requested anywhere, and no
    // Effect row was produced by the research loop.
    assert_eq!(h.approvals_lines().len(), 0, "no loop approval");
    assert_eq!(h.effects().await.len(), 0, "no effect");
    // A main-loop get of the research id works too
    // (kind travels on the view), but the list hides
    // research rows (Q-R4-12).
    let (status, view) = h.get(&format!("/agent/loop/get?loop_id={loop_id}")).await;
    assert_eq!(status, StatusCode::OK, "main get: {view}");
    assert_eq!(view["kind"], "research");
    let (status, list) = h.get("/agent/loop/list").await;
    assert_eq!(status, StatusCode::OK, "list: {list}");
    assert!(
        list.as_array()
            .unwrap()
            .iter()
            .all(|v| v["kind"] != "research"),
        "list hides research rows: {list}"
    );
    // The research model call saw only the three
    // read tools, and its SYSTEM names no write tool.
    let research_request = fake.seen_body(1);
    assert_eq!(
        fake.tool_names_of(1),
        ["fs_read", "code_search", "git_read"],
        "research tools: {}",
        research_request["tools"]
    );
    let system = research_request["messages"][0]["content"]
        .as_str()
        .unwrap();
    assert!(system.contains("只读调研员"), "research SYSTEM: {system}");
    assert!(!system.contains("fs_write"), "research SYSTEM names no write tool");
}

/// S2: a main loop and a research loop keep separate
/// call records and final texts; the report summary
/// reaches the second Planner prompt, not the main
/// loop's context (D8).
#[tokio::test]
async fn s2_main_and_research_loops_are_isolated() {
    let (fake_url, fake) = start_fake_deepseek(vec![
        planner_research_msg("问候语在哪？"),
        calls_msg(vec![("c1", "code_search", json!({"query": "hello"}))]),
        text_msg("研究报告 MARKER-RESEARCH-7：notes.txt 包含 hello。"),
        planner_triage_msg("问候语在 notes.txt。"),
        calls_msg(vec![("m1", "code_search", json!({"query": "notes"}))]),
        text_msg("主循环结论 MARKER-MAIN-9。"),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let task_id = h.make_task().await;
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    h.wait_task_status(&task_id, "completed", 30).await;

    let evidence = h.evidence().await;
    let reports: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "research_report")
        .collect();
    assert_eq!(reports.len(), 1, "evidence: {evidence:?}");
    let observed = observed_of(reports[0]);
    let research_id = observed["loop_id"].as_str().unwrap().to_string();

    let (status, main) = h
        .post(
            "/agent/loop/start",
            json!({"prompt": "主循环：搜索 notes", "max_calls": 5, "budget_ms": 120000}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "main start: {main}");
    let main_id = main["loop_id"].as_str().unwrap().to_string();
    assert_ne!(main_id, research_id);
    let main = h.wait_loop_terminal(&main_id, false, 30).await;
    assert_eq!(main["kind"], "main");
    assert_eq!(main["status"], "completed");
    assert_eq!(main["final_text"], "主循环结论 MARKER-MAIN-9。");
    assert!(
        !main["final_text"].as_str().unwrap().contains("MARKER-RESEARCH-7"),
        "main final text must not carry the research report"
    );

    let (status, research) = h.get(&format!("/agent/loop/research/{research_id}")).await;
    assert_eq!(status, StatusCode::OK, "research get: {research}");
    assert_eq!(research["kind"], "research");
    assert_eq!(research["calls"].as_array().unwrap().len(), 1);
    assert_eq!(research["calls"][0]["tool"], "code.search");
    assert_eq!(main["calls"].as_array().unwrap().len(), 1);
    assert_eq!(main["calls"][0]["tool"], "code.search");

    // The summary reached the second Planner prompt
    // (request 3), while the main loop's model call
    // (request 4) still saw the full 14-tool list.
    assert!(
        fake.prompt_of(3).contains("MARKER-RESEARCH-7"),
        "second Planner prompt carries the summary: {}",
        fake.prompt_of(3)
    );
    assert_eq!(fake.tool_names_of(4).len(), 14, "main loop tools");
    assert!(fake.tool_names_of(4).contains(&"fs_write".to_string()));
}

/// S3: the research report lands as one Evidence row
/// and survives a restart; the report cannot live on
/// the LoopView alone — after restart the completed
/// loop keeps its status but its final text is gone.
/// A second, still-running research loop becomes
/// `Interrupted` across the same restart.
#[tokio::test]
async fn s3_report_persists_across_restart() {
    let (fake_url, fake) = start_fake_deepseek(vec![
        planner_research_msg("问候语在哪？"),
        calls_msg(vec![("c1", "code_search", json!({"query": "hello"}))]),
        text_msg("研究报告：notes.txt 包含 hello。"),
        planner_triage_msg("问候语在 notes.txt。"),
        planner_triage_msg("ok"),
    ])
    .await;
    let mut h = Harness::serve(&fake_url).await;
    let task_id = h.make_task().await;
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    h.wait_task_status(&task_id, "completed", 30).await;

    let evidence = h.evidence().await;
    let reports: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "research_report")
        .collect();
    assert_eq!(reports.len(), 1, "evidence: {evidence:?}");
    let row = reports[0];
    assert_eq!(row["source"], "planner");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["effect_id"], format!("research:{task_id}"));
    assert_eq!(row["execution_id"], Value::Null);
    let observed = observed_of(row);
    assert_eq!(observed["task_id"], task_id);
    let loop_id = observed["loop_id"].as_str().unwrap().to_string();
    assert!(observed["report_sha256"].as_str().is_some());
    assert!(observed["report_text"]
        .as_str()
        .unwrap()
        .contains("notes.txt 包含 hello"));
    assert_eq!(observed["report_len"], observed["report_text"].as_str().unwrap().len());
    let evidence_id = row["evidence_id"].as_str().unwrap().to_string();
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);

    // The JSONL audit row carries the kind.
    let loops_jsonl = std::fs::read_to_string(h.dir.join("loops.jsonl")).unwrap();
    assert!(
        loops_jsonl.contains("\"kind\":\"research\""),
        "loops.jsonl must carry kind: {loops_jsonl}"
    );

    h.restart().await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    let evidence = h.evidence().await;
    let reports: Vec<&Value> = evidence
        .iter()
        .filter(|e| e["reason"] == "research_report")
        .collect();
    assert_eq!(reports.len(), 1, "report survives restart: {evidence:?}");
    assert_eq!(reports[0]["evidence_id"], evidence_id);
    // Reverse assertion: the report cannot live on
    // the LoopView alone — after restart the completed
    // loop keeps its status (only non-terminal rows
    // become Interrupted) but its final text is gone.
    let (status, view) = h.get(&format!("/agent/loop/research/{loop_id}")).await;
    assert_eq!(status, StatusCode::OK, "research get after restart: {view}");
    assert_eq!(view["kind"], "research");
    assert_eq!(view["status"], "completed");
    assert_eq!(view["final_text"], Value::Null);
    let (status, list) = h.get("/agent/loop/list").await;
    assert_eq!(status, StatusCode::OK, "list: {list}");
    assert!(
        list.as_array()
            .unwrap()
            .iter()
            .all(|v| v["kind"] != "research"),
        "list hides research rows after restart: {list}"
    );

    // A still-running research loop becomes
    // Interrupted across a restart. The second
    // task's Planner call is gated; the manual
    // research loop's own model call is gated too
    // (re-armed after the Planner request lands),
    // so the loop is Running when the gateway dies.
    let task_id = h.make_task().await;
    fake.gate.arm();
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    fake.wait_seen(5, 10).await;
    fake.gate.arm();
    let (status, view) = h
        .post("/agent/loop/research/start", json!({"task_id": task_id}))
        .await;
    assert_eq!(status, StatusCode::OK, "research start: {view}");
    let running_id = view["loop_id"].as_str().unwrap().to_string();
    // The driver has resumed the loop (its model
    // call is stuck behind the gate); the last
    // saved row is non-terminal.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let (status, view) = h.get(&format!("/agent/loop/research/{running_id}")).await;
        assert_eq!(status, StatusCode::OK, "research get: {view}");
        if view["status"] == "running" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "research loop never reached running: {view}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    h.restart().await;
    let (status, view) = h.get(&format!("/agent/loop/research/{running_id}")).await;
    assert_eq!(status, StatusCode::OK, "research get after restart: {view}");
    assert_eq!(view["kind"], "research");
    assert_eq!(view["status"], "interrupted");
    assert_eq!(view["final_text"], Value::Null);
    // Release the two gated fake handlers (their
    // gateway sockets are dead; the writes are
    // dropped by the fake).
    fake.gate.release();
}

/// S4: `research/start` is 200 in the planning phase
/// and 409 once an Executor step is prepared.
#[tokio::test]
async fn s4_research_start_is_planning_phase_only() {
    // (a) Planning phase: the Planner call is gated,
    // so the task sits running with no Executor step.
    // The manually started research loop gets the
    // canned reply and cannot steal the Planner's
    // queued triage answer.
    let (fake_url, fake) = start_fake_deepseek_canned(
        vec![planner_triage_msg("ok")],
        true,
    )
    .await;
    let h = Harness::serve(&fake_url).await;
    let task_id = h.make_task().await;
    fake.gate.arm();
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    fake.wait_seen(1, 10).await;
    let record = h.task_record(&task_id).await;
    assert_eq!(record["status"], "running");
    assert_eq!(record["executor_step_no"], Value::Null);

    let (status, view) = h
        .post("/agent/loop/research/start", json!({"task_id": task_id}))
        .await;
    assert_eq!(status, StatusCode::OK, "research start in planning: {view}");
    assert_eq!(view["kind"], "research");
    let manual_id = view["loop_id"].as_str().unwrap().to_string();

    let (status, value) = h
        .post("/agent/loop/research/start", json!({"task_id": "task-9999"}))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unknown task: {value}");
    assert_eq!(value["error"], "unknown_task");

    fake.gate.release();
    let manual = h.wait_loop_terminal(&manual_id, true, 30).await;
    assert_eq!(manual["status"], "completed");
    assert!(manual["final_text"]
        .as_str()
        .unwrap()
        .contains("canned research report"));
    h.wait_task_status(&task_id, "completed", 30).await;

    // (b) Executor phase: a two-step plan, step 1
    // approved + executed; the step-2 prepare call is
    // gated, so the task sits running at step 2.
    let (fake_url, fake) = start_fake_deepseek(vec![
        planner_steps_msg(vec![
            file_write_step(1, "proj/report.md", "git status"),
            file_write_step(2, "proj/report2.md", "git status"),
        ]),
        text_msg("STEP1 CONTENT\n"),
        text_msg("STEP2 CONTENT\n"),
    ])
    .await;
    let h = Harness::serve(&fake_url).await;
    let task_id = h.make_task().await;
    let (status, value) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "run: {value}");
    let record = h.wait_task_status(&task_id, "draft_ready", 30).await;
    assert_eq!(record["executor_step_no"], 1);
    h.approve_current(&task_id).await;
    fake.gate.arm();
    let (status, value) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, StatusCode::OK, "execute: {value}");
    // The advance already ran; the step-2 prepare is
    // stuck behind the gate.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let record = loop {
        let record = h.task_record(&task_id).await;
        if record["status"] == "running" && record["executor_step_no"] == 2 {
            break record;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "task never reached running step 2: {record}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(record["executor_step_no"], 2);

    let (status, value) = h
        .post("/agent/loop/research/start", json!({"task_id": task_id}))
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "research start in execution: {value}");
    assert_eq!(value["error"], "research_planning_only");

    // A research get of a main loop id is 404.
    let (status, main) = h
        .post(
            "/agent/loop/start",
            json!({"prompt": "main loop", "max_calls": 5, "budget_ms": 120000}),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "main start: {main}");
    let main_id = main["loop_id"].as_str().unwrap().to_string();
    let (status, value) = h.get(&format!("/agent/loop/research/{main_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "research get of main loop: {value}");
    assert_eq!(value["error"], "unknown_loop");

    fake.gate.release();
    h.wait_task_status(&task_id, "draft_ready", 30).await;
}
