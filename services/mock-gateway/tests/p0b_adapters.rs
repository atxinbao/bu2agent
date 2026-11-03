//! P0-B adapter tests: every entry point creates an internal
//! Execution and every new Effect is attributed to it, without
//! rewriting any driver, gate, or state machine. Gateway kernel
//! only: no client, no UI. Task and loop attribution via the
//! existing lookups is already covered by effect_smoke; this file
//! covers the P0-B gaps (device, direct writes, connector writes,
//! loop-resume reuse) and the no-backfill rule for old rows.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_desktop::credential_store::MemoryCredentialStore;
use mock_gateway::device_backend::{FakeReminderBackend, Reminder};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};
use tokio::net::TcpListener;

// ---------------------------------------------------------------- harness

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p0b-{}-{}-{}",
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
    fn build_state(dir: &std::path::Path, files_root: PathBuf) -> AppState {
        let fake = Arc::new(FakeReminderBackend::new());
        fake.add_reminder(Reminder {
            id: "reminder-1".to_string(),
            title: "Test reminder 1".to_string(),
            completed: false,
        });
        fake.add_reminder(Reminder {
            id: "reminder-2".to_string(),
            title: "Test reminder 2".to_string(),
            completed: false,
        });
        AppState::with_files_root_and_approvals(
            files_root,
            Some(dir.join("approvals.jsonl")),
        )
        .with_tasks_path(dir.join("tasks.jsonl"))
        .with_task_drafts_dir(dir.join("drafts"))
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_actions_path(dir.join("actions.jsonl"))
        .with_connector_state_path(dir.join("connectors.jsonl"))
        .with_vault_state_path(dir.join("vault.jsonl"))
        .with_vault_secrets(Arc::new(MemoryCredentialStore::new()))
        .with_device_backend(fake)
    }

    async fn serve(dir: PathBuf, files_root: PathBuf) -> Self {
        let state = Self::build_state(&dir, files_root.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
        }
    }

    async fn new() -> Self {
        let dir = temp_dir("harness");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        Self::serve(dir, files_root).await
    }

    async fn new_in(dir: PathBuf) -> Self {
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        Self::serve(dir, files_root).await
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

    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{body}");
        body
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

    async fn effects(&self) -> Vec<Value> {
        let (status, body) = self.get("/effects/list").await;
        assert_eq!(status, 200, "{body}");
        body.as_array().unwrap().clone()
    }

    async fn executions(&self) -> Vec<Value> {
        let (status, body) = self.get("/executions/list").await;
        assert_eq!(status, 200, "{body}");
        body.as_array().unwrap().clone()
    }

    async fn request_approval(&self, tool: &str, extra: Value) -> String {
        let mut req = json!({"tool": tool});
        if let Value::Object(map) = extra {
            for (k, v) in map {
                req[k] = v;
            }
        }
        let body = self.ok("/approval/request", req).await;
        body["tool_call_id"].as_str().unwrap().to_string()
    }

    async fn decide(&self, id: &str, status: &str) {
        self.ok(
            "/approval/decide",
            json!({"tool_call_id": id, "status": status}),
        )
        .await;
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn find_effect<'a>(effects: &'a [Value], capability: &str, operation: &str, scope: &str) -> &'a Value {
    effects
        .iter()
        .find(|e| {
            e["capability"] == capability && e["operation"] == operation && e["scope"] == scope
        })
        .unwrap_or_else(|| panic!("no effect {capability}/{operation}/{scope} in {effects:?}"))
}

fn find_execution<'a>(executions: &'a [Value], id: &str) -> &'a Value {
    executions
        .iter()
        .find(|e| e["execution_id"] == id)
        .unwrap_or_else(|| panic!("no execution {id} in {executions:?}"))
}

// ---------------------------------------------------------------- loop harness

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(replies.into_iter().map(|r| (Duration::ZERO, Ok(r))).collect()),
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
        let _ = messages;
        let (delay, reply) = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            reply
        })
    }
}

#[derive(Clone, Default)]
struct FakeFiles {
    files: Arc<Mutex<BTreeMap<String, String>>>,
    writes: Arc<AtomicUsize>,
}

impl LoopTools for FakeFiles {
    fn execute(&self, tool: String, args: BTreeMap<String, String>) -> BackendFuture<ToolOutcome> {
        let this = self.clone();
        Box::pin(async move {
            let mut files = this.files.lock().unwrap();
            match tool.as_str() {
                "fs.write" => {
                    this.writes.fetch_add(1, Ordering::SeqCst);
                    files.insert(args["path"].clone(), args["text"].clone());
                    ToolOutcome::success("saved and verified".into())
                }
                "fs.read" => files
                    .get(&args["path"])
                    .map(|s| ToolOutcome::success(s.clone()))
                    .unwrap_or_else(|| ToolOutcome::failed(ErrorCode::ToolFailed)),
                "fs.list" => ToolOutcome::success(
                    serde_json::to_string(&files.keys().collect::<Vec<_>>()).unwrap(),
                ),
                _ => panic!("invalid tool reached executor"),
            }
        })
    }
}

fn proposal(tool: &str, args: Value) -> Proposal {
    Proposal {
        tool: tool.into(),
        args,
        wire: None,
    }
}

fn write_proposal(path: &str, text: &str) -> Proposal {
    proposal("fs.write", json!({"path": path, "text": text}))
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

struct LoopHarness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
}

impl LoopHarness {
    async fn new(replies: Vec<ModelReply>) -> Self {
        let dir = temp_dir("loop");
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
            Some(model),
            Some(Arc::new(files.clone())),
            Timeouts::default(),
        )
        .unwrap()
        .with_auto_drive(false)
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
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

    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn get(&self, path: &str) -> Value {
        let response = self
            .client
            .get(format!("{}{}", self.url, path))
            .send()
            .await
            .unwrap();
        response.json().await.unwrap()
    }

    async fn start(&self) -> String {
        self.ok("/agent/loop/start", json!({"prompt": "private task prompt"}))
            .await["loop_id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id": id})).await
    }

    async fn effects(&self) -> Vec<Value> {
        let body = self.get("/effects/list").await;
        body.as_array().unwrap().clone()
    }

    async fn executions(&self) -> Vec<Value> {
        let body = self.get("/executions/list").await;
        body.as_array().unwrap().clone()
    }
}

impl Drop for LoopHarness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ---------------------------------------------------------------- tests

#[tokio::test]
async fn direct_write_effect_carries_minimal_execution_and_key() {
    let h = Harness::new().await;
    let w = h
        .request_approval("fs.write", json!({"path": "a.txt", "content_base64": "aGVsbG8="}))
        .await;
    h.decide(&w, "approved").await;
    let (status, _) = h
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8=", "approval_id": w}),
        )
        .await;
    assert_eq!(status, 200);

    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    let effect = find_effect(&effects, "fs", "write", "a.txt");
    let execution_id = effect["execution_id"].as_str().expect("minimal execution");
    assert!(execution_id.starts_with("execution-"));
    // The key commits to the execution plus the semantic identity.
    let digest = effect["input_digest"].as_str().unwrap();
    assert_eq!(
        effect["idempotency_key"],
        format!("exec:{execution_id}:fs.write:a.txt:{digest}")
    );
    // Minimal means no task and no loop: the write IS the attempt.
    let executions = h.executions().await;
    let exec = find_execution(&executions, execution_id);
    assert!(exec["task_id"].is_null());
    assert!(exec["loop_id"].is_null());
}

#[tokio::test]
async fn agent_tool_call_bound_write_carries_minimal_execution() {
    let h = Harness::new().await;
    let approval = h
        .request_approval(
            "fs.write",
            json!({"path": "agent.txt", "content_base64": "ZnJvbSBhZ2VudA=="}),
        )
        .await;
    h.decide(&approval, "approved").await;
    let (status, _) = h
        .post(
            "/agent/tool/call",
            json!({"tool": "fs.write", "args": {"path": "agent.txt", "text": "from agent"}, "tool_call_id": approval}),
        )
        .await;
    assert_eq!(status, 200);

    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    let effect = find_effect(&effects, "fs", "write", "agent.txt");
    let execution_id = effect["execution_id"].as_str().expect("minimal execution");
    assert!(effect["idempotency_key"].is_string());
    let executions = h.executions().await;
    let exec = find_execution(&executions, execution_id);
    assert!(exec["task_id"].is_null());
    assert!(exec["loop_id"].is_null());
}

#[tokio::test]
async fn connector_write_effect_carries_minimal_execution_and_key() {
    let h = Harness::new().await;
    let (_, linked) = h
        .post(
            "/connectors/notes/accounts/link",
            json!({"display_name": "Mock account"}),
        )
        .await;
    let account_id = linked["account_id"].as_str().unwrap().to_string();

    let effects = h.effects().await;
    let effect = find_effect(&effects, "connectors", "link", &account_id);
    let execution_id = effect["execution_id"].as_str().expect("minimal execution");
    assert!(effect["idempotency_key"].is_string());
    let executions = h.executions().await;
    let exec = find_execution(&executions, execution_id);
    assert!(exec["task_id"].is_null());
    assert!(exec["loop_id"].is_null());
}

#[tokio::test]
async fn device_effect_carries_action_execution_and_contract_labels() {
    let h = Harness::new().await;
    let (_, created) = h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-1", "target_completed": true}),
        )
        .await;
    let action_execution = created["execution_id"].as_str().expect("action execution");
    assert!(action_execution.starts_with("execution-"));

    let approval = h.request_approval("reminders.set_completed", json!({})).await;
    h.decide(&approval, "approved").await;
    let (status, body) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": created["action_id"], "tool_call_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "completed");

    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    // Contract §3 triple, not the phase-2 (devices, set, action_id).
    let effect = find_effect(&effects, "device", "reminders.set_completed", "reminder-1");
    assert_eq!(effect["execution_id"], action_execution);
    let digest = effect["input_digest"].as_str().unwrap();
    assert_eq!(
        effect["idempotency_key"],
        format!("exec:{action_execution}:device.reminders.set_completed:reminder-1:{digest}")
    );
}

#[tokio::test]
async fn loop_resume_reuses_single_execution() {
    let h = LoopHarness::new(vec![
        calls(vec![write_proposal("out", "loop body")]),
        done(),
    ])
    .await;
    let loop_id = h.start().await;
    // One Execution from loop_start, before any resume runs.
    let executions = h.executions().await;
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0]["loop_id"], loop_id);
    let execution_id = executions[0]["execution_id"].as_str().unwrap().to_string();

    h.step(&loop_id).await;
    let waiting = h.step(&loop_id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    let call = &waiting["calls"][waiting["queue_position"].as_u64().unwrap() as usize];
    let binding = call["binding"].clone();
    let approval = call["approval_id"].as_str().unwrap().to_string();
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": approval, "status": "approved"}),
    )
    .await;
    let result = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop_id, "approval_id": approval, "binding": binding}),
        )
        .await;
    assert_eq!(result["calls"][0]["status"], "succeeded");
    let final_view = h.step(&loop_id).await;
    assert_eq!(final_view["status"], "completed");

    // Resumes never spawn a second Execution: resume continues
    // the same attempt, and the call Effect belongs to it.
    let executions = h.executions().await;
    assert_eq!(executions.len(), 1);
    assert_eq!(executions[0]["execution_id"], execution_id);
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["execution_id"], execution_id);
}

#[tokio::test]
async fn old_rows_read_unchanged_and_unattributed() {
    let dir = temp_dir("old-rows");
    // Phase-2 rows, written before P0-B: no execution id, no key,
    // no provenance fields, and the old device triple.
    std::fs::write(
        dir.join("effects.jsonl"),
        "{\"effect_id\":\"effect-1\",\"capability\":\"devices\",\"operation\":\"set\",\"scope\":\"action-9\",\"status\":\"completed\"}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("actions.jsonl"),
        "{\"action_id\":\"action-9\",\"device\":\"mac-reminders\",\"action\":\"reminders.set_completed\",\"reminder_id\":\"reminder-1\",\"target_completed\":true,\"status\":\"requested\",\"detail\":null}\n",
    )
    .unwrap();
    let h = Harness::new_in(dir).await;

    // Old rows read back verbatim: no backfill, no relabel.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    let old = find_effect(&effects, "devices", "set", "action-9");
    assert_eq!(old["effect_id"], "effect-1");
    assert!(old["execution_id"].is_null());
    assert!(old["idempotency_key"].is_null());
    assert_eq!(old["schema_version"], 0);
    assert_eq!(old["origin_device_id"], "unknown");

    let (_, actions) = h.get("/devices/actions/list").await;
    let old_action = actions
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["action_id"] == "action-9")
        .expect("old action");
    assert!(old_action["execution_id"].is_null());

    // Setting the old action writes a NEW row with the contract
    // triple, but its execution stays null: P0-B never fabricates
    // a historical association for a pre-P0-B action.
    let approval = h.request_approval("reminders.set_completed", json!({})).await;
    h.decide(&approval, "approved").await;
    let (status, body) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": "action-9", "tool_call_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let effects = h.effects().await;
    assert_eq!(effects.len(), 2);
    let new = find_effect(&effects, "device", "reminders.set_completed", "reminder-1");
    assert!(new["execution_id"].is_null(), "old action stays unattributed");
    // And the old row is still exactly what it was.
    let old = find_effect(&effects, "devices", "set", "action-9");
    assert_eq!(old["effect_id"], "effect-1");
    assert!(old["execution_id"].is_null());
}
