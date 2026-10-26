//! Auto-run driver tests: start/resume must run steps until a
//! stopping point with no further clicks. Script model, memory
//! file tools, no real model or reminders. Auto-drive stays on
//! here (the production default); loop_smoke turns it off so
//! single-step states stay deterministic.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
    contexts: Mutex<Vec<Vec<Message>>>,
}

impl LoopModel for ScriptModel {
    fn name(&self) -> &str {
        "scripted-driver-test"
    }
    fn complete(
        &self,
        messages: Vec<Message>,
        _timeout: Duration,
    ) -> BackendFuture<Result<ModelReply, ModelError>> {
        self.contexts.lock().unwrap().push(messages);
        let (delay, reply) =
            self.steps.lock().unwrap().pop_front().expect("unexpected extra model call");
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            reply
        })
    }
}

#[derive(Clone, Default)]
struct FakeFiles {
    files: Arc<Mutex<BTreeMap<String, String>>>,
}

impl LoopTools for FakeFiles {
    fn execute(&self, tool: String, args: BTreeMap<String, String>) -> BackendFuture<ToolOutcome> {
        let this = self.clone();
        Box::pin(async move {
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
                    files.insert(args["path"].clone(), args["text"].clone());
                    ToolOutcome::success("saved and verified".into())
                }
                _ => panic!("invalid tool reached executor"),
            }
        })
    }
}

static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    model: Arc<ScriptModel>,
    files: FakeFiles,
    server: tokio::task::JoinHandle<()>,
}

impl Harness {
    async fn new(replies: Vec<(Duration, ModelReply)>) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "bu2agent-driver-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let model = Arc::new(ScriptModel {
            steps: Mutex::new(replies.into_iter().map(|(d, r)| (d, Ok(r))).collect()),
            contexts: Mutex::new(vec![]),
        });
        let files = FakeFiles::default();
        let state = AppState::with_files_root_and_approvals(root, Some(dir.join("approvals.jsonl")))
            .with_loop_backends(
                Some(dir.join("loops.jsonl")),
                Some(model.clone()),
                Some(Arc::new(files.clone())),
                Timeouts::default(),
            )
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
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

    async fn get(&self, loop_id: &str) -> Value {
        let response = self
            .client
            .get(format!("{}/agent/loop/get?loop_id={}", self.url, loop_id))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        response.json().await.unwrap()
    }

    async fn start(&self) -> String {
        let (status, body) =
            self.post("/agent/loop/start", json!({"prompt":"driver task"})).await;
        assert_eq!(status, 200, "{body}");
        body["loop_id"].as_str().unwrap().to_string()
    }

    async fn poll_status(&self, loop_id: &str, want: &str) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let view = self.get(loop_id).await;
            if view["status"].as_str() == Some(want) {
                return view;
            }
            assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {want}: {view}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn decide_and_resume(&self, view: &Value, decision: &str) {
        let pos = view["queue_position"].as_u64().unwrap() as usize;
        let call = &view["calls"][pos];
        let approval_id = call["approval_id"].as_str().unwrap();
        let binding = call["binding"].clone();
        let (status, body) = self
            .post("/approval/decide", json!({"tool_call_id": approval_id, "status": decision}))
            .await;
        assert_eq!(status, 200, "{body}");
        let loop_id = view["loop_id"].as_str().unwrap();
        let (status, body) = self
            .post(
                "/agent/loop/resume",
                json!({"loop_id": loop_id, "approval_id": approval_id, "binding": binding}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn write(path: &str, text: &str) -> ModelReply {
    ModelReply::Calls {
        proposals: vec![Proposal { tool: "fs.write".into(), args: json!({"path": path, "text": text}), wire: None }],
        content: None,
    }
}

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

#[tokio::test]
async fn driver_runs_to_approval_then_completes_after_one_resume() {
    let h = Harness::new(vec![
        (Duration::ZERO, write("out.txt", "hello driver")),
        (Duration::ZERO, done()),
    ])
    .await;
    let loop_id = h.start().await;
    // No clicks: start alone must reach the approval point.
    let view = h.poll_status(&loop_id, "awaiting_approval").await;
    assert_eq!(view["pending_write"]["path"], "out.txt");
    assert_eq!(view["approval_status"], "pending");
    // One resume with the approval: the driver must continue to
    // the terminal state with no further clicks.
    h.decide_and_resume(&view, "approved").await;
    let view = h.poll_status(&loop_id, "completed").await;
    assert_eq!(view["model_attempts"], 2);
    assert_eq!(h.files.files.lock().unwrap().get("out.txt").map(String::as_str), Some("hello driver"));
    assert_eq!(h.model.contexts.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn driver_rejection_continues_to_final_without_writing() {
    let h = Harness::new(vec![
        (Duration::ZERO, write("no.txt", "x")),
        (Duration::ZERO, done()),
    ])
    .await;
    let loop_id = h.start().await;
    let view = h.poll_status(&loop_id, "awaiting_approval").await;
    h.decide_and_resume(&view, "rejected").await;
    let view = h.poll_status(&loop_id, "completed").await;
    assert_eq!(view["model_attempts"], 2);
    assert!(!h.files.files.lock().unwrap().contains_key("no.txt"));
}

#[tokio::test]
async fn driver_cancel_stops_and_starts_no_further_model_call() {
    let h = Harness::new(vec![(Duration::from_millis(800), done())]).await;
    let loop_id = h.start().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (status, _) = h.post("/agent/loop/cancel", json!({"loop_id": loop_id})).await;
    assert_eq!(status, 200);
    h.poll_status(&loop_id, "cancelled").await;
    // The late model result must not start a second call.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(h.model.contexts.lock().unwrap().len(), 1);
    assert_eq!(h.get(&loop_id).await["status"], "cancelled");
}
