//! 模型循环接入提醒事项的 Fake 后端冒烟测试。
//! 不使用真实 EventKit、模型或凭据；只验证查询、审批、allowlist 与未知结果映射。

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::device_backend::{
    AuthorizationStatus, FakeFault, FakeReminderBackend, Reminder, ReminderBackend,
};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};

struct ScriptModel {
    steps: Mutex<VecDeque<ModelReply>>,
    contexts: Mutex<Vec<Vec<Message>>>,
}

impl LoopModel for ScriptModel {
    fn name(&self) -> &str {
        "reminder-loop-test-model"
    }

    fn complete(
        &self,
        messages: Vec<Message>,
        _timeout: Duration,
    ) -> BackendFuture<Result<ModelReply, ModelError>> {
        self.contexts.lock().unwrap().push(messages);
        let reply = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected model call");
        Box::pin(async move { Ok(reply) })
    }
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    model: Arc<ScriptModel>,
    fake: Arc<FakeReminderBackend>,
    server: tokio::task::JoinHandle<()>,
}

impl Harness {
    async fn new(steps: Vec<ModelReply>) -> Self {
        Self::new_with_pin(steps, None).await
    }

    async fn new_with_pin(steps: Vec<ModelReply>, pin: Option<&str>) -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
        let dir = std::env::temp_dir().join(format!(
            "bu2agent-reminder-loop-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let model = Arc::new(ScriptModel {
            steps: Mutex::new(steps.into()),
            contexts: Mutex::new(vec![]),
        });
        let fake = Arc::new(FakeReminderBackend::new());
        fake.add_reminder(Reminder {
            id: "reminder-1".into(),
            title: "模型循环验收提醒".into(),
            completed: false,
        });
        fake.add_reminder(Reminder {
            id: "reminder-2".into(),
            title: "第二条提醒".into(),
            completed: false,
        });
        let mut state = AppState::with_files_root_and_approvals(dir.join("files"), None)
            .with_device_backend(fake.clone());
        if let Some(pin) = pin {
            state = state.with_device_reminder_pin(pin.to_string());
        }
        let state = state
            .with_loop_backends(
                Some(dir.join("loops.jsonl")),
                Some(model.clone()),
                None,
                Timeouts::default(),
            )
            .unwrap()
            .with_auto_drive(false);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::new(),
            model,
            fake,
            server,
        }
    }

    async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        let response = self
            .client
            .post(format!("{}{path}", self.url))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap())
    }

    async fn start(&self) -> String {
        let (status, body) = self
            .post("/agent/loop/start", json!({"prompt":"提醒循环测试"}))
            .await;
        assert_eq!(status, 200, "{body}");
        body["loop_id"].as_str().unwrap().into()
    }

    async fn step(&self, loop_id: &str) -> Value {
        let (status, body) = self
            .post("/agent/loop/resume", json!({"loop_id":loop_id}))
            .await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn approve_waiting(&self, view: &Value, decision: &str) -> Value {
        let index = view["queue_position"].as_u64().unwrap() as usize;
        let call = &view["calls"][index];
        let approval_id = call["approval_id"].as_str().unwrap();
        let (status, body) = self
            .post(
                "/approval/decide",
                json!({"tool_call_id":approval_id,"status":decision}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let (status, body) = self.post(
            "/agent/loop/resume",
            json!({"loop_id":view["loop_id"],"approval_id":approval_id,"binding":call["binding"]}),
        ).await;
        assert_eq!(status, 200, "{body}");
        body
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn list() -> ModelReply {
    ModelReply::Calls {
        proposals: vec![Proposal {
            tool: "reminders.list".into(),
            args: json!({}),
            wire: None,
        }],
        content: None,
    }
}

fn set(id: &str, completed: bool) -> ModelReply {
    ModelReply::Calls {
        proposals: vec![Proposal {
            tool: "reminders.set_completed".into(),
            args: json!({"id":id,"completed":completed}),
            wire: None,
        }],
        content: None,
    }
}

fn done() -> ModelReply {
    ModelReply::Final("完成".into())
}

async fn reach_set_approval(h: &Harness, id: &str) -> Value {
    let loop_id = h.start().await;
    h.step(&loop_id).await;
    h.step(&loop_id).await;
    h.step(&loop_id).await;
    let proposed = h.step(&loop_id).await;
    assert_eq!(proposed["status"], "awaiting_approval");
    assert_eq!(proposed["pending_reminder"]["reminder_id"], id);
    assert_eq!(proposed["pending_reminder"]["title"], "模型循环验收提醒");
    proposed
}

#[tokio::test]
async fn list_returns_reminder_data_to_model_without_approval() {
    let h = Harness::new(vec![list(), done()]).await;
    let loop_id = h.start().await;
    h.step(&loop_id).await;
    h.step(&loop_id).await;
    let executed = h.step(&loop_id).await;
    assert_eq!(executed["calls"][0]["tool"], "reminders.list");
    assert_eq!(executed["calls"][0]["status"], "succeeded");
    let contexts = h.model.contexts.lock().unwrap();
    let tool = contexts[1]
        .iter()
        .find_map(|message| match message {
            Message::Tool { result, .. } => Some(result.body.clone()),
            _ => None,
        })
        .unwrap();
    assert!(tool.contains("模型循环验收提醒"));
}

#[tokio::test]
async fn approved_set_completed_changes_fake_reminder_and_completes() {
    let h = Harness::new(vec![list(), set("reminder-1", true), done()]).await;
    let waiting = reach_set_approval(&h, "reminder-1").await;
    let after_execute = h.approve_waiting(&waiting, "approved").await;
    assert_eq!(after_execute["calls"][1]["status"], "succeeded");
    let completed = h.step(waiting["loop_id"].as_str().unwrap()).await;
    assert_eq!(completed["calls"][1]["status"], "succeeded");
    assert!(h.fake.get("reminder-1").unwrap().unwrap().completed);
}

#[tokio::test]
async fn rejected_set_completed_does_not_change_fake_reminder() {
    let h = Harness::new(vec![list(), set("reminder-1", true), done()]).await;
    let waiting = reach_set_approval(&h, "reminder-1").await;
    let rejected = h.approve_waiting(&waiting, "rejected").await;
    assert_eq!(rejected["calls"][1]["status"], "rejected");
    let completed = h.step(waiting["loop_id"].as_str().unwrap()).await;
    assert_eq!(completed["calls"][1]["status"], "rejected");
    assert!(!h.fake.get("reminder-1").unwrap().unwrap().completed);
}

#[tokio::test]
async fn unauthorized_set_returns_failure_without_requesting_authorization() {
    let h = Harness::new(vec![list(), set("reminder-1", true), done()]).await;
    h.fake.set_auth(AuthorizationStatus::NotDetermined);
    let waiting = reach_set_approval(&h, "reminder-1").await;
    let failed = h.approve_waiting(&waiting, "approved").await;
    assert_eq!(failed["calls"][1]["status"], "failed");
    let completed = h.step(waiting["loop_id"].as_str().unwrap()).await;
    assert_eq!(completed["calls"][1]["status"], "failed");
    assert_eq!(completed["calls"][1]["error"], "tool_failed");
    assert!(!h.fake.get("reminder-1").unwrap().unwrap().completed);
}

#[tokio::test]
async fn pinned_reminder_rejects_other_id_without_writing() {
    let h = Harness::new_with_pin(
        vec![list(), set("reminder-2", true), done()],
        Some("reminder-1"),
    )
    .await;
    let loop_id = h.start().await;
    h.step(&loop_id).await;
    h.step(&loop_id).await;
    h.step(&loop_id).await;
    let waiting = h.step(&loop_id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    let failed = h.approve_waiting(&waiting, "approved").await;
    assert_eq!(failed["calls"][1]["status"], "failed");
    let final_view = h.step(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    assert!(!h.fake.get("reminder-2").unwrap().unwrap().completed);
}

#[tokio::test]
async fn backend_timeout_becomes_unknown_and_needs_review() {
    let h = Harness::new(vec![list(), set("reminder-1", true), done()]).await;
    let waiting = reach_set_approval(&h, "reminder-1").await;
    h.fake.set_fault(Some(FakeFault::Timeout));
    let unknown = h.approve_waiting(&waiting, "approved").await;
    assert_eq!(unknown["calls"][1]["status"], "unknown");
    let final_view = h.step(waiting["loop_id"].as_str().unwrap()).await;
    assert_eq!(final_view["status"], "needs_review");
    assert_eq!(final_view["unknown_writes"], json!([2]));
}
