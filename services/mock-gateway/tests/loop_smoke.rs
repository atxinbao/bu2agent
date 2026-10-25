//! 冻结契约的 HTTP 回归：脚本模型、内存文件工具，不访问真实模型或提醒。

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};
use tokio::sync::Notify;

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
    contexts: Mutex<Vec<Vec<Message>>>,
    started: Notify,
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
            contexts: Mutex::new(vec![]),
            started: Notify::new(),
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
        self.contexts.lock().unwrap().push(messages);
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

#[derive(Clone, Default)]
struct FakeFiles {
    files: Arc<Mutex<BTreeMap<String, String>>>,
    executions: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
    started: Arc<Notify>,
    delay: Arc<Mutex<Duration>>,
    forced: Arc<Mutex<Option<CallStatus>>>,
}

impl LoopTools for FakeFiles {
    fn execute(&self, tool: String, args: BTreeMap<String, String>) -> BackendFuture<ToolOutcome> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let this = self.clone();
        let delay = *self.delay.lock().unwrap();
        let forced = self.forced.lock().unwrap().clone();
        self.started.notify_one();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            if let Some(status) = forced {
                return ToolOutcome {
                    status,
                    error: Some(ErrorCode::Unconfirmed),
                    body: String::new(),
                };
            }
            let mut files = this.files.lock().unwrap();
            match tool.as_str() {
                "fs.list" => ToolOutcome::success(
                    serde_json::to_string(&files.keys().collect::<Vec<_>>()).unwrap(),
                ),
                "fs.read" => files
                    .get(&args["path"])
                    .map(|s| ToolOutcome::success(s.clone()))
                    .unwrap_or_else(|| ToolOutcome::failed(ErrorCode::ToolFailed)),
                "fs.write" => {
                    this.writes.fetch_add(1, Ordering::SeqCst);
                    files.insert(args["path"].clone(), args["text"].clone());
                    ToolOutcome::success("saved and verified".into())
                }
                _ => panic!("invalid tool reached executor"),
            }
        })
    }
}

static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Harness {
    dir: PathBuf,
    root: PathBuf,
    url: String,
    client: reqwest::Client,
    model: Arc<ScriptModel>,
    files: FakeFiles,
    server: tokio::task::JoinHandle<()>,
}

impl Harness {
    async fn use_existing_file_dispatch(&mut self) {
        self.server.abort();
        let state = AppState::with_files_root_and_approvals(
            self.root.clone(),
            Some(self.dir.join("approvals.jsonl")),
        )
        .with_loop_backends(
            Some(self.dir.join("loops.jsonl")),
            Some(self.model.clone()),
            None,
            Timeouts::default(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
    }
    async fn new(replies: Vec<ModelReply>) -> Self {
        Self::custom(replies, Timeouts::default()).await
    }
    async fn custom(replies: Vec<ModelReply>, timeouts: Timeouts) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "bu2agent-loop-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let files = FakeFiles::default();
        let state = AppState::with_files_root_and_approvals(
            root.clone(),
            Some(dir.join("approvals.jsonl")),
        )
        .with_loop_backends(
            Some(dir.join("loops.jsonl")),
            Some(model.clone()),
            Some(Arc::new(files.clone())),
            timeouts,
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            root,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
            model,
            files,
            server,
        }
    }
    async fn post(&self, path: &str, value: Value) -> (u16, Value) {
        let response = self
            .client
            .post(format!("{}{path}", self.url))
            .json(&value)
            .send()
            .await
            .unwrap();
        (response.status().as_u16(), response.json().await.unwrap())
    }
    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{body}");
        body
    }
    async fn start(&self) -> String {
        self.ok("/agent/loop/start", json!({"prompt":"private task prompt"}))
            .await["loop_id"]
            .as_str()
            .unwrap()
            .into()
    }
    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id":id})).await
    }
    async fn get(&self, id: &str) -> Value {
        self.client
            .get(format!("{}/agent/loop/get?loop_id={id}", self.url))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    async fn approve(&self, view: &Value, status: &str) -> Value {
        let call = &view["calls"][view["queue_position"].as_u64().unwrap() as usize];
        self.ok(
            "/approval/decide",
            json!({"tool_call_id":call["approval_id"],"status":status}),
        )
        .await;
        self.ok("/agent/loop/resume", json!({"loop_id":view["loop_id"],"approval_id":call["approval_id"],"binding":call["binding"]})).await
    }
    fn resumed_state(&self, path: PathBuf) -> AppState {
        AppState::with_files_root_and_approvals(
            self.root.clone(),
            Some(self.dir.join("approvals.jsonl")),
        )
        .with_loop_backends(
            Some(path),
            Some(self.model.clone()),
            Some(Arc::new(self.files.clone())),
            Timeouts::default(),
        )
        .unwrap()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn proposal(tool: &str, args: Value) -> Proposal {
    Proposal {
        tool: tool.into(),
        args,
        wire: None,
    }
}
fn read(path: &str) -> Proposal {
    proposal("fs.read", json!({"path":path}))
}
fn write(path: &str, text: &str) -> Proposal {
    proposal("fs.write", json!({"path":path,"text":text}))
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

#[tokio::test]
async fn approval_preview_is_memory_only_and_reports_decision_after_disconnect() {
    let text = "preview-only-private-body";
    let h = Harness::new(vec![calls(vec![write("./preview.txt", text)]), done()]).await;
    let id = h.start().await;
    let proposed = h.step(&id).await;
    assert!(proposed["pending_write"].is_null());
    let waiting = h.step(&id).await;
    assert_eq!(waiting["approval_status"], "pending");
    assert_eq!(waiting["pending_write"]["path"], "preview.txt");
    assert_eq!(waiting["pending_write"]["text"], text);
    assert_eq!(
        waiting["pending_write"]["args_sha256"],
        waiting["calls"][0]["binding"]["args_sha256"]
    );
    h.ok(
        "/approval/decide",
        json!({"tool_call_id":waiting["calls"][0]["approval_id"],"status":"approved"}),
    )
    .await;
    let after = h.get(&id).await;
    assert_eq!(after["approval_status"], "approved");
    assert_eq!(after["status"], "awaiting_approval");
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 0);
    assert!(!std::fs::read_to_string(h.dir.join("loops.jsonl"))
        .unwrap()
        .contains(text));
    let completed = h.ok("/agent/loop/resume", json!({"loop_id":id,"approval_id":after["calls"][0]["approval_id"],"binding":after["calls"][0]["binding"]})).await;
    assert!(completed["pending_write"].is_null());
    assert!(completed["approval_status"].is_null());
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restart_removes_approval_preview_and_cannot_resume_old_write() {
    let h = Harness::new(vec![calls(vec![write("out", "never-persist-preview")])]).await;
    let id = h.start().await;
    h.step(&id).await;
    assert_eq!(h.step(&id).await["approval_status"], "pending");
    let restored = AppState::with_files_root_and_approvals(
        h.root.clone(),
        Some(h.dir.join("approvals.jsonl")),
    )
    .with_loop_backends(
        Some(h.dir.join("loops.jsonl")),
        Some(h.model.clone()),
        Some(Arc::new(h.files.clone())),
        Timeouts::default(),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app(restored)).await.unwrap() });
    let view: Value = h
        .client
        .get(format!("{url}/agent/loop/get?loop_id={id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(view["status"], "interrupted");
    assert!(view["pending_write"].is_null());
    assert!(view["approval_status"].is_null());
    assert_eq!(
        h.client
            .post(format!("{url}/agent/loop/resume"))
            .json(&json!({"loop_id":id}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert!(!std::fs::read_to_string(h.dir.join("loops.jsonl"))
        .unwrap()
        .contains("never-persist-preview"));
    server.abort();
}

#[tokio::test]
async fn success_pause_binding_and_read_write_read_over_http() {
    let h = Harness::new(vec![
        calls(vec![read("seed")]),
        calls(vec![write("out", "secret small body")]),
        calls(vec![read("out")]),
        done(),
    ])
    .await;
    h.files
        .files
        .lock()
        .unwrap()
        .insert("seed".into(), "seed body".into());
    let id = h.start().await;
    h.step(&id).await;
    h.step(&id).await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    let budget = waiting["used_ms"].clone();
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(h.get(&id).await["used_ms"], budget);
    let call = &waiting["calls"][1];
    let mut binding = call["binding"].clone();
    binding["args_sha256"] = json!("wrong");
    assert_eq!(
        h.post(
            "/agent/loop/resume",
            json!({"loop_id":id,"approval_id":call["approval_id"],"binding":binding})
        )
        .await
        .0,
        403
    );
    assert_eq!(h.get(&id).await["status"], "awaiting_approval");
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 0);
    h.ok(
        "/approval/decide",
        json!({"tool_call_id":call["approval_id"],"status":"approved"}),
    )
    .await;
    // 已批准的循环凭据也不能从通用工具接口换参数或绕过一次性执行。
    assert_eq!(h.post("/agent/tool/call", json!({"tool":"fs.write","args":{"path":"other","text":"evil"},"tool_call_id":call["approval_id"]})).await.0,403);
    let result = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id":id,"approval_id":call["approval_id"],"binding":call["binding"]}),
        )
        .await;
    assert_eq!(result["calls"][1]["status"], "succeeded");
    h.step(&id).await;
    h.step(&id).await;
    let final_view = h.step(&id).await;
    assert_eq!(final_view["status"], "completed");
    assert_eq!(h.files.files.lock().unwrap()["out"], "secret small body");
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 1);
    let audit = std::fs::read_to_string(h.dir.join("loops.jsonl")).unwrap();
    for private in [
        "private task prompt",
        "secret small body",
        "seed body",
        "finished",
    ] {
        assert!(!audit.contains(private));
    }
    assert_eq!(std::fs::read_dir(&h.root).unwrap().count(), 0);
}

#[tokio::test]
async fn duplicate_successful_write_uses_normalized_parameters_and_original_result() {
    let h = Harness::new(vec![
        calls(vec![write("./out", "one"), write("out", "one")]),
        done(),
    ])
    .await;
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    h.approve(&waiting, "approved").await;
    h.files
        .files
        .lock()
        .unwrap()
        .insert("out".into(), "external change".into());
    let duplicate = h.step(&id).await;
    assert_eq!(duplicate["calls"][1]["status"], "duplicate");
    assert_eq!(duplicate["calls"][1]["duplicate_of"], 1);
    assert!(duplicate["calls"][1]["approval_id"].is_null());
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 1);
    assert_eq!(h.files.files.lock().unwrap()["out"], "external change");
    assert_eq!(h.step(&id).await["status"], "completed");
}

#[tokio::test]
async fn rejected_write_is_not_reapproved_and_model_can_finish() {
    let h = Harness::new(vec![
        calls(vec![write("out", "x"), write("out", "x")]),
        done(),
    ])
    .await;
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    let rejected = h.approve(&waiting, "rejected").await;
    assert_eq!(rejected["calls"][0]["status"], "rejected");
    let duplicate = h.step(&id).await;
    assert_eq!(duplicate["calls"][1]["result"]["status"], "rejected");
    assert!(duplicate["calls"][1]["approval_id"].is_null());
    assert_eq!(h.files.executions.load(Ordering::SeqCst), 0);
    assert_eq!(h.step(&id).await["status"], "completed");
}

#[tokio::test]
async fn timeout_write_duplicate_and_read_do_not_resolve_unknown_even_after_late_success() {
    let h = Harness::custom(
        vec![
            calls(vec![write("out", "x"), write("out", "x"), read("out")]),
            done(),
        ],
        Timeouts {
            model: Duration::from_secs(1),
            tool: Duration::from_millis(10),
        },
    )
    .await;
    *h.files.delay.lock().unwrap() = Duration::from_millis(60);
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    let unknown = h.approve(&waiting, "approved").await;
    assert_eq!(unknown["calls"][0]["status"], "unknown");
    assert_eq!(h.step(&id).await["calls"][1]["result"]["status"], "unknown");
    tokio::time::sleep(Duration::from_millis(90)).await;
    *h.files.delay.lock().unwrap() = Duration::ZERO;
    let observed = h.step(&id).await;
    assert_eq!(observed["calls"][0]["status"], "unknown");
    assert_eq!(
        observed["calls"][0]["late"]["result"]["status"],
        "succeeded"
    );
    assert_eq!(observed["calls"][2]["status"], "succeeded");
    let final_view = h.step(&id).await;
    assert_eq!(final_view["status"], "needs_review");
    assert_eq!(final_view["unknown_writes"], json!([1]));
    assert_ne!(final_view["final_text"], "finished");
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn parallel_resume_returns_conflict_cancel_archives_late_tool_without_new_calls() {
    let h = Harness::new(vec![calls(vec![write("out", "x")]), done()]).await;
    *h.files.delay.lock().unwrap() = Duration::from_millis(120);
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    let call = &waiting["calls"][0];
    h.ok(
        "/approval/decide",
        json!({"tool_call_id":call["approval_id"],"status":"approved"}),
    )
    .await;
    let c = h.client.clone();
    let url = format!("{}/agent/loop/resume", h.url);
    let payload = json!({"loop_id":id,"approval_id":call["approval_id"],"binding":call["binding"]});
    let pending = tokio::spawn(async move {
        c.post(url)
            .json(&payload)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    });
    h.files.started.notified().await;
    assert_eq!(
        h.post("/agent/loop/resume", json!({"loop_id":id})).await.0,
        409
    );
    assert_eq!(
        h.ok("/agent/loop/cancel", json!({"loop_id":id})).await["status"],
        "cancelled"
    );
    let final_view = pending.await.unwrap();
    assert_eq!(final_view["status"], "cancelled");
    assert_eq!(
        final_view["calls"][0]["late"]["result"]["status"],
        "succeeded"
    );
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 1);
    assert_eq!(h.model.contexts.lock().unwrap().len(), 1);
    assert_eq!(
        h.post("/agent/loop/resume", json!({"loop_id":id})).await.0,
        409
    );
}

#[tokio::test]
async fn cancel_before_step_prevents_all_backend_calls() {
    let h = Harness::new(vec![done()]).await;
    let id = h.start().await;
    h.ok("/agent/loop/cancel", json!({"loop_id":id})).await;
    assert_eq!(
        h.post("/agent/loop/resume", json!({"loop_id":id})).await.0,
        409
    );
    assert!(h.model.contexts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_model_response_never_enters_queue_or_audit_body() {
    let h = Harness::new(vec![]).await;
    h.model.steps.lock().unwrap().push_back((
        Duration::from_millis(70),
        Ok(calls(vec![write("out", "late-secret")])),
    ));
    let id = h.start().await;
    let c = h.client.clone();
    let url = format!("{}/agent/loop/resume", h.url);
    let payload = json!({"loop_id":id});
    let pending = tokio::spawn(async move {
        c.post(url)
            .json(&payload)
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()
    });
    h.model.started.notified().await;
    h.ok("/agent/loop/cancel", json!({"loop_id":id})).await;
    let final_view = pending.await.unwrap();
    assert_eq!(final_view["status"], "cancelled");
    assert_eq!(final_view["calls"], json!([]));
    assert_eq!(
        final_view["late_model_arrivals_ms"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(!std::fs::read_to_string(h.dir.join("loops.jsonl"))
        .unwrap()
        .contains("late-secret"));
}

#[tokio::test]
async fn batch_over_cap_records_failures_but_allows_one_final_model_call() {
    let proposals = (0..7).map(|_| proposal("fs.list", json!({}))).collect();
    let h = Harness::new(vec![calls(proposals), done()]).await;
    let id = h.start().await;
    h.step(&id).await;
    for _ in 0..7 {
        h.step(&id).await;
    }
    let final_view = h.step(&id).await;
    assert_eq!(final_view["status"], "completed");
    assert_eq!(final_view["proposal_count"], 7);
    assert_eq!(h.files.executions.load(Ordering::SeqCst), 5);
    for index in [5, 6] {
        assert_eq!(final_view["calls"][index]["status"], "failed");
        assert_eq!(final_view["calls"][index]["error"], "cap_exceeded");
    }
    assert_eq!(h.model.contexts.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn closing_model_proposal_stops_without_execution() {
    let h = Harness::new(vec![calls(vec![read("a")]), calls(vec![write("out", "x")])]).await;
    let start = h
        .ok("/agent/loop/start", json!({"prompt":"test","max_calls":1}))
        .await;
    let id = start["loop_id"].as_str().unwrap();
    h.step(id).await;
    h.step(id).await;
    let final_view = h.step(id).await;
    assert_eq!(final_view["status"], "failed");
    assert_eq!(final_view["error"], "cap_exceeded");
    assert_eq!(final_view["proposal_count"], 2);
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_proposals_count_without_reaching_tool_backend() {
    let h = Harness::new(vec![
        calls(vec![
            proposal("fs.delete", json!({"path":"x"})),
            read("../x"),
            write("/absolute", "x"),
            proposal("fs.write", json!({"path":"x","text":"y","append":true})),
            proposal("fs.read", json!({"path":3})),
        ]),
        done(),
    ])
    .await;
    let id = h.start().await;
    h.step(&id).await;
    for _ in 0..5 {
        h.step(&id).await;
    }
    let result = h.step(&id).await;
    assert_eq!(result["proposal_count"], 5);
    assert_eq!(result["status"], "completed");
    assert_eq!(h.files.executions.load(Ordering::SeqCst), 0);
    for call in result["calls"].as_array().unwrap() {
        assert_eq!(call["status"], "failed");
        assert!(call["approval_id"].is_null());
    }
}

#[tokio::test]
async fn model_failure_ends_without_retry_and_charges_time() {
    let h = Harness::new(vec![]).await;
    h.model
        .steps
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(15), Err(ModelError::Failed)));
    let id = h.start().await;
    let result = h.step(&id).await;
    assert_eq!(result["error"], "model_failed");
    assert!(result["used_ms"].as_u64().unwrap() >= 10);
    assert_eq!(
        h.post("/agent/loop/resume", json!({"loop_id":id})).await.0,
        409
    );
    assert_eq!(h.model.contexts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn remaining_budget_limits_model_and_archives_late_metadata() {
    let h = Harness::new(vec![]).await;
    h.model.steps.lock().unwrap().push_back((
        Duration::from_millis(80),
        Ok(ModelReply::Final("late answer".into())),
    ));
    let id = h
        .ok("/agent/loop/start", json!({"prompt":"task","budget_ms":10}))
        .await["loop_id"]
        .as_str()
        .unwrap()
        .to_string();
    let result = h.step(&id).await;
    assert_eq!(result["status"], "failed");
    assert_eq!(result["error"], "budget_exceeded");
    assert_eq!(result["used_ms"], 10);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let result = h.get(&id).await;
    assert_eq!(
        result["late_model_arrivals_ms"].as_array().unwrap().len(),
        1
    );
    assert!(!std::fs::read_to_string(h.dir.join("loops.jsonl"))
        .unwrap()
        .contains("late answer"));
}

#[tokio::test]
async fn read_unknown_only_is_completed_with_notice_and_utf8_results_are_bounded() {
    let h = Harness::new(vec![calls(vec![read("seed"), read("seed")]), done()]).await;
    let body = "汉".repeat(2000);
    h.files.files.lock().unwrap().insert("seed".into(), body);
    let id = h.start().await;
    h.step(&id).await;
    let first = h.step(&id).await;
    assert_eq!(first["calls"][0]["result"]["body_digest"]["bytes"], 6000);
    assert_eq!(first["calls"][0]["result"]["truncated"], true);
    *h.files.forced.lock().unwrap() = Some(CallStatus::Unknown);
    h.step(&id).await;
    let final_view = h.step(&id).await;
    assert_eq!(final_view["status"], "completed");
    assert_eq!(final_view["unknown_reads"], json!([2]));
    let contexts = h.model.contexts.lock().unwrap();
    let message = contexts
        .last()
        .unwrap()
        .iter()
        .find_map(|m| match m {
            Message::Tool {
                call_id: 1, result, ..
            } => Some(result),
            _ => None,
        })
        .unwrap();
    assert_eq!(message.body.len(), 4095);
    assert!(message.summary.truncated);
}

#[tokio::test]
async fn unknown_write_does_not_override_cancelled_or_model_failed() {
    for cancel in [true, false] {
        let h = Harness::new(vec![calls(vec![write("out", "x")])]).await;
        h.model
            .steps
            .lock()
            .unwrap()
            .push_back((Duration::ZERO, Err(ModelError::Failed)));
        *h.files.forced.lock().unwrap() = Some(CallStatus::Unknown);
        let id = h.start().await;
        h.step(&id).await;
        let waiting = h.step(&id).await;
        h.approve(&waiting, "approved").await;
        let result = if cancel {
            h.ok("/agent/loop/cancel", json!({"loop_id":id})).await
        } else {
            h.step(&id).await
        };
        assert_eq!(
            result["status"],
            if cancel { "cancelled" } else { "failed" }
        );
        assert_eq!(result["unknown_writes"], json!([1]));
    }
}

#[tokio::test]
async fn restart_interrupts_pending_approval_and_old_approval_cannot_authorize_new_loop() {
    let h = Harness::new(vec![
        calls(vec![write("out", "x")]),
        calls(vec![write("out", "x")]),
    ])
    .await;
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    let old = &waiting["calls"][0];
    let state = h.resumed_state(h.dir.join("loops.jsonl"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
    let restored: Value = h
        .client
        .get(format!("{url}/agent/loop/get?loop_id={id}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(restored["status"], "interrupted");
    let next: Value = h
        .client
        .post(format!("{url}/agent/loop/start"))
        .json(&json!({"prompt":"new"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_ne!(next["loop_id"], id);
    let payload = json!({"loop_id":next["loop_id"]});
    for _ in 0..2 {
        h.client
            .post(format!("{url}/agent/loop/resume"))
            .json(&payload)
            .send()
            .await
            .unwrap();
    }
    let response=h.client.post(format!("{url}/agent/loop/resume")).json(&json!({"loop_id":next["loop_id"],"approval_id":old["approval_id"],"binding":old["binding"]})).send().await.unwrap();
    assert_eq!(response.status(), 403);
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn loop_audit_rejects_file_root_and_limits_cannot_be_relaxed() {
    let h = Harness::new(vec![done()]).await;
    let state = AppState::with_files_root_and_approvals(h.root.clone(), None);
    assert!(state
        .with_loop_backends(
            Some(h.root.join("loops.jsonl")),
            None,
            None,
            Timeouts::default()
        )
        .is_err());
    for params in [
        json!({"prompt":"task","max_calls":6}),
        json!({"prompt":"task","budget_ms":120001}),
        json!({"prompt":"task","max_calls":0}),
    ] {
        assert_eq!(h.post("/agent/loop/start", params).await.0, 400);
    }
}

#[tokio::test]
async fn restart_inflight_write_becomes_unknown_and_interrupted_without_reexecution() {
    let h = Harness::new(vec![calls(vec![write("out", "x")])]).await;
    *h.files.delay.lock().unwrap() = Duration::from_millis(100);
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    let call = &waiting["calls"][0];
    h.ok(
        "/approval/decide",
        json!({"tool_call_id":call["approval_id"],"status":"approved"}),
    )
    .await;
    let client = h.client.clone();
    let url = format!("{}/agent/loop/resume", h.url);
    let payload = json!({"loop_id":id,"approval_id":call["approval_id"],"binding":call["binding"]});
    let pending =
        tokio::spawn(async move { client.post(url).json(&payload).send().await.unwrap() });
    h.files.started.notified().await;
    let snapshot = h.dir.join("crash-snapshot.jsonl");
    std::fs::copy(h.dir.join("loops.jsonl"), &snapshot).unwrap();
    let restarted =
        LoopEngine::new(&h.root, Some(snapshot), None, None, Timeouts::default()).unwrap();
    let view = restarted.get(&id).unwrap();
    assert_eq!(view.record.status, LoopStatus::Interrupted);
    assert_eq!(view.record.calls[0].status, CallStatus::Unknown);
    assert_eq!(view.record.unknown_writes, vec![1]);
    assert_eq!(h.files.executions.load(Ordering::SeqCst), 1);
    pending.await.unwrap();
}

#[tokio::test]
async fn tool_budget_exhaustion_keeps_failed_and_unknown_after_late_success() {
    let h = Harness::new(vec![calls(vec![write("out", "x")])]).await;
    *h.files.delay.lock().unwrap() = Duration::from_millis(160);
    let start = h
        .ok("/agent/loop/start", json!({"prompt":"task","budget_ms":60}))
        .await;
    let id = start["loop_id"].as_str().unwrap();
    h.step(id).await;
    let waiting = h.step(id).await;
    let result = h.approve(&waiting, "approved").await;
    assert_eq!(result["status"], "failed");
    assert_eq!(result["error"], "budget_exceeded");
    assert_eq!(result["unknown_writes"], json!([1]));
    tokio::time::sleep(Duration::from_millis(180)).await;
    let result = h.get(id).await;
    assert_eq!(result["status"], "failed");
    assert_eq!(result["calls"][0]["status"], "unknown");
    assert_eq!(result["calls"][0]["late"]["result"]["status"], "succeeded");
}

#[tokio::test]
async fn model_timeout_has_no_retry_and_late_arrival_is_recorded_once() {
    let h = Harness::custom(
        vec![],
        Timeouts {
            model: Duration::from_millis(10),
            tool: Duration::from_secs(1),
        },
    )
    .await;
    h.model
        .steps
        .lock()
        .unwrap()
        .push_back((Duration::from_millis(60), Ok(done())));
    let id = h.start().await;
    let result = h.step(&id).await;
    assert_eq!(result["status"], "failed");
    assert_eq!(result["error"], "model_timeout");
    assert!(result["late_model_arrivals_ms"]
        .as_array()
        .unwrap()
        .is_empty());
    tokio::time::sleep(Duration::from_millis(90)).await;
    assert_eq!(
        h.get(&id).await["late_model_arrivals_ms"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(h.model.contexts.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn disconnecting_resume_client_does_not_abandon_worker() {
    let h = Harness::new(vec![done()]).await;
    *h.model.steps.lock().unwrap() = VecDeque::from([(Duration::from_millis(60), Ok(done()))]);
    let id = h.start().await;
    let client = h.client.clone();
    let url = format!("{}/agent/loop/resume", h.url);
    let payload = json!({"loop_id":id});
    let pending = tokio::spawn(async move { client.post(url).json(&payload).send().await });
    h.model.started.notified().await;
    pending.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(h.get(&id).await["status"], "completed");
}

#[tokio::test]
async fn existing_agent_dispatch_writes_only_temp_root_and_verifies_readback() {
    let mut h = Harness::new(vec![
        calls(vec![
            write("demo/out.txt", "test body"),
            read("demo/out.txt"),
        ]),
        done(),
    ])
    .await;
    h.use_existing_file_dispatch().await;
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    let written = h.approve(&waiting, "approved").await;
    assert_eq!(written["calls"][0]["status"], "succeeded");
    assert_eq!(
        std::fs::read_to_string(h.root.join("demo/out.txt")).unwrap(),
        "test body"
    );
    assert_eq!(h.step(&id).await["calls"][1]["status"], "succeeded");
    let result = h.step(&id).await;
    assert_eq!(result["status"], "completed");
    assert_eq!(result["model_attempts"], 2);
    assert_eq!(h.files.executions.load(Ordering::SeqCst), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn loop_file_dispatch_refuses_symlink_outside_root() {
    let mut h = Harness::new(vec![calls(vec![write("escape/out", "test body")]), done()]).await;
    let outside = h.dir.join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, h.root.join("escape")).unwrap();
    h.use_existing_file_dispatch().await;
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    let result = h.approve(&waiting, "approved").await;
    assert_eq!(result["calls"][0]["status"], "failed");
    assert_eq!(result["calls"][0]["error"], "invalid_arguments");
    assert!(!outside.join("out").exists());
}
