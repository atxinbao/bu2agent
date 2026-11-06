//! P0 phase 2 convergence tests: every durable side effect mirrors
//! into an Effect record, and the three approval-consumption
//! families keep their single-use guarantees under the unified
//! (tool, mode, params) digest. Gateway kernel only: no client, no
//! UI. The model is a local fake DeepSeek HTTP service for the task
//! flow, and scripted loop backends for the loop flow; never the
//! real API and never a real credential. Secrets in the vault tests
//! are throwaway test values, never real credentials.

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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ---------------------------------------------------------------- loop backends

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
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

// ---------------------------------------------------------------- fake model server

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

// ---------------------------------------------------------------- harness

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-effect-{}-{}-{}",
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
    files_root: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    model_url: Option<String>,
}

impl Harness {
    fn build_state(dir: &std::path::Path, files_root: PathBuf, model_url: Option<String>) -> AppState {
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
        let mut state = AppState::with_files_root_and_approvals(
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
        .with_device_backend(fake);
        if let Some(base) = model_url {
            state = state.with_deepseek(mock_gateway::deepseek::DeepSeekClient::new(
                mock_gateway::deepseek::DeepSeekConfig {
                    base_url: base,
                    model: "fixture-3b".to_string(),
                    api_key: Some("test-only-fake-key".to_string()),
                },
            ));
        }
        state
    }

    async fn new(model_url: Option<String>) -> Self {
        let dir = temp_dir("harness");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        let state = Self::build_state(&dir, files_root.clone(), model_url.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            files_root,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
            model_url,
        }
    }

    async fn restart(&mut self) {
        self.server.abort();
        let state = Self::build_state(&self.dir, self.files_root.clone(), self.model_url.clone());
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

    fn last_approval(&self, id: &str) -> Value {
        let text = std::fs::read_to_string(self.dir.join("approvals.jsonl")).unwrap();
        let mut found = Value::Null;
        for line in text.lines() {
            if line.contains(id) {
                found = serde_json::from_str(line).unwrap();
            }
        }
        assert!(found.is_object(), "no approval line for {id}");
        found
    }

    fn effects_file_lines(&self) -> Vec<Value> {
        let path = self.dir.join("effects.jsonl");
        if !path.exists() {
            return Vec::new();
        }
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct LoopHarness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    files: FakeFiles,
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
            files,
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
}

impl Drop for LoopHarness {
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

// ---------------------------------------------------------------- Q5 parity

#[test]
fn unified_digest_loop_matches_direct_write() {
    // Q5: a loop write and a direct write of the same normalized
    // path, text, and mode commit to the same digest. The loop used
    // to hash the raw text; the direct path hashes the content
    // SHA-256. Both now go through operation_digest.
    let loop_args = BTreeMap::from([
        ("path".to_string(), "out/report.md".to_string()),
        ("text".to_string(), "hello".to_string()),
    ]);
    assert_eq!(
        args_digest("fs.write", &loop_args),
        mock_gateway::operation_binding::write_digest("out/report.md", b"hello", false).unwrap()
    );
    assert_eq!(
        args_digest("fs.write", &loop_args),
        mock_gateway::operation_binding::write_digest("./out//report.md", b"hello", false).unwrap(),
        "normalization must not change the digest"
    );
    // Reminder digests keep their own mode label: a reminder
    // authorization can never authorize a file write.
    let reminder_args = BTreeMap::from([
        ("completed".to_string(), "true".to_string()),
        ("id".to_string(), "reminder-1".to_string()),
    ]);
    let reminder_digest = args_digest("reminders.set_completed", &reminder_args);
    assert_ne!(reminder_digest, args_digest("fs.write", &loop_args));
    assert_eq!(
        reminder_digest,
        mock_gateway::operation_binding::operation_digest(
            "reminders.set_completed",
            Some("set_completed"),
            &reminder_args
        )
    );
    // E2-C unification (Q7): the digest is value-sensitive —
    // the false write hashes differently from the true write,
    // which is what lets A2 recover the target from the row.
    let false_digest = mock_gateway::operation_binding::operation_digest(
        "reminders.set_completed",
        Some("set_completed"),
        &BTreeMap::from([
            ("id".to_string(), "reminder-1".to_string()),
            ("completed".to_string(), "false".to_string()),
        ]),
    );
    assert_ne!(false_digest, reminder_digest);
    // Path normalization is shared: the loop delegates to the same
    // function, keeping only its fs.list root allowance.
    assert_eq!(
        normalize("fs.write", &json!({"path": "./demo//report.md", "text": "t"})).unwrap()["path"],
        "demo/report.md"
    );
    assert_eq!(
        normalize("fs.list", &json!({"path": ""})).unwrap()["path"],
        ""
    );
    assert!(normalize("fs.read", &json!({"path": "../escape"})).is_err());
}

// ---------------------------------------------------------------- digest gate

#[tokio::test]
async fn digest_gate_rejects_mismatch_without_burning_then_single_use() {
    let h = Harness::new(None).await;
    // Approval bound to (a.txt, "hello").
    let approval = h
        .request_approval(
            "fs.write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8="}),
        )
        .await;
    h.decide(&approval, "approved").await;

    // Wrong content: 403, and the approval is NOT consumed.
    let (status, _) = h
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8td29ybGQ=", "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 403);
    assert_eq!(h.last_approval(&approval)["consumed"], false);
    // P0-C (contract §4): the approval request already planned
    // the write, so one `planned` Effect remains; what a
    // rejected write leaves is no *terminal* Effect.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["status"], "planned");

    // Exact content: 200, and the old response shape is unchanged.
    let (status, body) = h
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8=", "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["path"], "a.txt");
    assert_eq!(body["size_bytes"], 5);
    assert_eq!(h.last_approval(&approval)["consumed"], true);

    // Reuse: P0-C idempotency (contract §3, task item 4)
    // replaces the old bare 403 with a replay: the terminal
    // Effect is returned, nothing is written a second time,
    // and still exactly one Effect exists. (A *different*
    // write with a consumed approval is still 403, as the
    // persistence and device tests prove.)
    let (status, body) = h
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8=", "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["replayed"], true);
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    let effect = find_effect(&effects, "fs", "write", "a.txt");
    assert_eq!(effect["effect_id"], "effect-1");
    // P0-B: a direct write is its own attempt, so it now carries
    // a minimal Execution and a non-null key; the phase-2 null
    // attribution was the gap this phase closes.
    let execution_id = effect["execution_id"].as_str().expect("minimal execution");
    assert!(execution_id.starts_with("execution-"));
    assert!(effect["idempotency_key"].is_string());
    assert_eq!(effect["approval_id"], approval);
    // P0-C (contract §4): new rows use the lifecycle word
    // `succeeded`, not the old device/file word `completed`.
    assert_eq!(effect["status"], "succeeded");
    assert_eq!(
        effect["input_digest"],
        h.last_approval(&approval)["operation_digest"],
        "the Effect carries the digest the gate consumed"
    );
    // Dual-write consistency: the JSONL file holds the
    // lifecycle chain for the one Effect (planned at approval
    // request, authorized at consume, started, succeeded),
    // last line wins.
    let lines = h.effects_file_lines();
    assert_eq!(lines.len(), 4);
    assert!(lines.iter().all(|l| l["effect_id"] == "effect-1"));
    assert_eq!(lines[0]["status"], "planned");
    assert_eq!(lines[3]["status"], "succeeded");
    assert_eq!(lines[3]["scope"], "a.txt");
}

// ---------------------------------------------------------------- device gate

#[tokio::test]
async fn device_bind_consume_and_reuse_fails() {
    let h = Harness::new(None).await;
    let (_, created) = h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-1", "target_completed": true}),
        )
        .await;
    let action_id = created["action_id"].as_str().unwrap().to_string();
    assert_eq!(action_id, "action-1");

    // A plain (unbound) approval authorizes the device set.
    let approval = h.request_approval("reminders.set_completed", json!({})).await;
    h.decide(&approval, "approved").await;
    let (status, body) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": action_id, "tool_call_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "completed");

    // Q4: the approval is now consumed and bound to this action.
    let record = h.last_approval(&approval);
    assert_eq!(record["consumed"], true);
    assert_eq!(record["action_binding"], "action-1");

    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    // P0-B (contract §3): new device rows use the contract triple
    // (device, reminders.set_completed, reminder_id), not the
    // phase-2 (devices, set, action_id); and the Effect belongs
    // to the Execution created at actions_create.
    let effect = find_effect(&effects, "device", "reminders.set_completed", "reminder-1");
    assert_eq!(effect["approval_id"], approval);
    // P0-C (contract §4): new device rows use `succeeded`.
    assert_eq!(effect["status"], "succeeded");
    assert_eq!(effect["execution_id"], created["execution_id"]);
    assert!(effect["idempotency_key"].is_string(), "device effect carries a key");

    // Reuse for a second action: 403 binding mismatch, and the
    // second action stays requested (no terminal Effect, no
    // consumption; P0-C: its planned Effect from creation does
    // exist, asserted below).
    let (_, created2) = h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-2", "target_completed": true}),
        )
        .await;
    let action2 = created2["action_id"].as_str().unwrap().to_string();
    let (status, _) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": action2, "tool_call_id": approval}),
        )
        .await;
    assert_eq!(status, 403);
    // P0-C: the second action was planned at creation, so its
    // `planned` Effect exists even though the set was refused;
    // the first action's terminal Effect is still the only
    // non-planned one.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 2);
    let second = find_effect(&effects, "device", "reminders.set_completed", "reminder-2");
    assert_eq!(second["status"], "planned");

    // A loop-bound approval cannot authorize a device set either.
    // (Checked on the loop server itself: the approval must exist
    // in the same state for the binding rejection to be the cause.)
    let loop_h = LoopHarness::new(vec![
        calls(vec![write_proposal("out", "loop body")]),
        done(),
    ])
    .await;
    let loop_id = loop_h.start().await;
    loop_h.step(&loop_id).await;
    let waiting = loop_h.step(&loop_id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    let call = &waiting["calls"][waiting["queue_position"].as_u64().unwrap() as usize];
    let loop_approval = call["approval_id"].as_str().unwrap().to_string();
    let (_, created3) = loop_h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-1", "target_completed": true}),
        )
        .await;
    let action3 = created3["action_id"].as_str().unwrap().to_string();
    let (status, _) = loop_h
        .post(
            "/devices/reminders/set",
            json!({"action_id": action3, "tool_call_id": loop_approval}),
        )
        .await;
    assert_eq!(status, 403, "a loop binding must not authorize a device action");
}

// ---------------------------------------------------------------- loop call effect

#[tokio::test]
async fn loop_call_effect_and_binding_gate() {
    let h = LoopHarness::new(vec![
        calls(vec![write_proposal("out", "loop body")]),
        done(),
    ])
    .await;
    let loop_id = h.start().await;
    h.step(&loop_id).await;
    let waiting = h.step(&loop_id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    let call = &waiting["calls"][waiting["queue_position"].as_u64().unwrap() as usize];
    let binding = call["binding"].clone();
    let approval = call["approval_id"].as_str().unwrap().to_string();

    // Q5 parity, observed over HTTP: the loop binding carries the
    // same unified digest a direct write of the same text would.
    assert_eq!(
        binding["args_sha256"],
        mock_gateway::operation_binding::write_digest("out", b"loop body", false).unwrap()
    );

    // Reference mismatch: a binding for another call id is refused.
    let mut tampered = binding.clone();
    tampered["call_id"] = json!(call["call_id"].as_u64().unwrap() + 1);
    let (status, _) = h
        .post(
            "/agent/loop/resume",
            json!({"loop_id": loop_id, "approval_id": approval, "binding": tampered}),
        )
        .await;
    assert_eq!(status, 403);
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 0);

    // A loop approval cannot authorize a direct write either.
    let (status, _) = h
        .post(
            "/files/write",
            json!({"path": "out", "content_base64": "bG9vcCBib2R5", "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 403);

    // Approve and resume with the exact binding: the call succeeds.
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
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 1);
    let final_view = h.step(&loop_id).await;
    assert_eq!(final_view["status"], "completed");

    // The loop's Execution (phase 1) is the Effect's execution.
    let executions = h.get("/executions/list").await;
    let exec = executions
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["loop_id"] == loop_id)
        .expect("loop execution");
    let execution_id = exec["execution_id"].as_str().unwrap();

    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    let effect = find_effect(&effects, "fs", "write", "out");
    assert_eq!(effect["effect_id"], "effect-1");
    assert_eq!(effect["execution_id"], execution_id);
    assert_eq!(effect["approval_id"], approval);
    assert_eq!(effect["status"], "succeeded");
    assert_eq!(effect["input_digest"], binding["args_sha256"]);
}

// ---------------------------------------------------------------- files, tasks, connectors

#[tokio::test]
async fn file_task_connector_effects_and_read_paths() {
    let (model_addr, model) = start_fake_model("# report\n\n要点：alpha。\n\n结论：完成。").await;
    let h = Harness::new(Some(format!("http://{model_addr}"))).await;

    // ---- files: every one-shot write mirrors into an Effect.
    let w = h
        .request_approval("fs.write", json!({"path": "a.txt", "content_base64": "aGVsbG8="}))
        .await;
    h.decide(&w, "approved").await;
    let (status, body) = h
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8=", "approval_id": w}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["size_bytes"], 5);

    let m = h.request_approval("fs.mkdir", json!({"path": "dir"})).await;
    h.decide(&m, "approved").await;
    let (status, _) = h
        .post("/files/mkdir", json!({"path": "dir", "approval_id": m}))
        .await;
    assert_eq!(status, 200);

    let r = h
        .request_approval("fs.rename", json!({"path": "a.txt", "to_path": "b.txt"}))
        .await;
    h.decide(&r, "approved").await;
    let (status, _) = h
        .post("/files/rename", json!({"from": "a.txt", "to": "b.txt", "approval_id": r}))
        .await;
    assert_eq!(status, 200);

    let export_to = h.dir.join("exported.txt");
    let export_to_str = export_to.to_string_lossy().into_owned();
    let e = h
        .request_approval("fs.export", json!({"path": "b.txt", "to_path": export_to_str}))
        .await;
    h.decide(&e, "approved").await;
    let (status, _) = h
        .post(
            "/files/export",
            json!({"from": "b.txt", "to": export_to_str, "approval_id": e}),
        )
        .await;
    assert_eq!(status, 200);
    assert!(export_to.exists());

    let d = h.request_approval("fs.delete", json!({"path": "b.txt"})).await;
    h.decide(&d, "approved").await;
    let (status, body) = h
        .post("/files/delete", json!({"path": "b.txt", "approval_id": d}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["deleted"], true);

    let effects = h.effects().await;
    assert_eq!(find_effect(&effects, "fs", "write", "a.txt")["approval_id"], w);
    assert_eq!(find_effect(&effects, "fs", "mkdir", "dir")["approval_id"], m);
    assert_eq!(
        find_effect(&effects, "fs", "rename", "a.txt -> b.txt")["approval_id"],
        r
    );
    assert_eq!(find_effect(&effects, "fs", "export", "b.txt")["approval_id"], e);
    assert_eq!(find_effect(&effects, "fs", "delete", "b.txt")["approval_id"], d);

    // ---- tasks: run, approve, execute mirrors into an Effect.
    let notes = h.files_root.join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let (_, task_body) = h.post("/tasks/create", json!({"directory": "notes"})).await;
    let task_id = task_body["task_id"].as_str().unwrap().to_string();
    let (status, _) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, 200);
    // The runner finishes asynchronously: poll until the draft is
    // ready (or fail loudly after a generous bound).
    let mut task = Value::Null;
    for _ in 0..200 {
        let (_, got) = h.get(&format!("/tasks/get?task_id={task_id}")).await;
        if got["status"] == "draft_ready" {
            task = got;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(task["status"], "draft_ready", "task never reached draft_ready: {task}");
    let task_approval = task["approval_id"].as_str().unwrap().to_string();
    h.decide(&task_approval, "approved").await;
    let (status, executed) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, 200, "{executed}");
    assert_eq!(executed["status"], "completed");
    let artifact = executed["artifact_path"].as_str().unwrap().to_string();
    assert!(h.files_root.join(&artifact).exists(), "artifact {artifact} missing");
    let executions = h.get("/executions/list").await;
    let exec = executions
        .1
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["task_id"] == task_id)
        .expect("task execution");
    let all_effects = h.effects().await;
    let task_effect = find_effect(&all_effects, "tasks", "execute", &task_id);
    assert_eq!(task_effect["execution_id"], exec["execution_id"]);
    assert_eq!(task_effect["approval_id"], task_approval);
    // P0-C (contract §4): new rows use `succeeded`.
    assert_eq!(task_effect["status"], "succeeded");

    // ---- connectors and vault: one call, one Effect each.
    let (_, linked) = h
        .post(
            "/connectors/notes/accounts/link",
            json!({"display_name": "Mock account"}),
        )
        .await;
    let account_id = linked["account_id"].as_str().unwrap().to_string();
    let (_, custom) = h
        .post(
            "/v1/custom-connectors/register",
            json!({"id": "custom-1", "display_name": "Custom"}),
        )
        .await;
    assert_eq!(custom["id"], "custom-1");
    let before = h.effects().await.len();
    let (_, _) = h
        .post(
            "/v1/credentials/register",
            json!({"handle": "h1", "secret": "test-secret-1", "provider": "prov"}),
        )
        .await;
    // The incidental catalog ensure is not a second Effect.
    assert_eq!(h.effects().await.len(), before + 1);

    let (_, ticket) = h
        .post(
            "/v1/credentials/capture/external-provider/request",
            json!({"provider": "prov", "handle": "h1"}),
        )
        .await;
    let ticket_id = ticket["ticket_id"].as_str().unwrap().to_string();
    // A ticket request mints an ephemeral exchange token, not an
    // Effect: the count must not move until the fulfill lands.
    assert_eq!(h.effects().await.len(), before + 1);
    let (status, _) = h
        .post(
            "/v1/credentials/capture/external-provider/fulfill",
            json!({"ticket_id": ticket_id, "secret": "test-secret-2"}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(h.effects().await.len(), before + 2);

    let (status, _) = h
        .post(
            "/v1/credentials/catalog/update",
            json!({"id": "prov", "display_name": "Provider"}),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = h
        .post("/v1/credentials/catalog/delete", json!({"id": "prov"}))
        .await;
    assert_eq!(status, 200);

    let effects = h.effects().await;
    // P0-C (contract §4): new rows use `succeeded`.
    assert_eq!(
        find_effect(&effects, "connectors", "link", &account_id)["status"],
        "succeeded"
    );
    assert_eq!(
        find_effect(&effects, "custom-connectors", "register", "custom-1")["status"],
        "succeeded"
    );
    assert_eq!(
        find_effect(&effects, "credentials", "register", "h1")["status"],
        "succeeded"
    );
    assert_eq!(
        find_effect(&effects, "credentials", "fulfill", "h1")["status"],
        "succeeded"
    );
    assert_eq!(
        find_effect(&effects, "catalog", "update", "prov")["status"],
        "succeeded"
    );
    assert_eq!(
        find_effect(&effects, "catalog", "delete", "prov")["status"],
        "succeeded"
    );

    // The read-only Effect routes: get works, unknown 404s.
    let (status, got) = h.get("/effects/get?effect_id=effect-1").await;
    assert_eq!(status, 200, "{got}");
    assert_eq!(got["effect_id"], "effect-1");
    let (status, _) = h.get("/effects/get?effect_id=effect-99").await;
    assert_eq!(status, 404);
    model.abort();
}

// ---------------------------------------------------------------- restart

#[tokio::test]
async fn effects_survive_restart_with_counter() {
    let mut h = Harness::new(None).await;
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
    assert_eq!(h.effects().await.len(), 1);

    h.restart().await;
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["effect_id"], "effect-1");

    // The counter resumes past the largest suffix: the next Effect
    // is effect-2, never a reused effect-1.
    let w2 = h
        .request_approval("fs.write", json!({"path": "b.txt", "content_base64": "aGVsbG8="}))
        .await;
    h.decide(&w2, "approved").await;
    let (status, _) = h
        .post(
            "/files/write",
            json!({"path": "b.txt", "content_base64": "aGVsbG8=", "approval_id": w2}),
        )
        .await;
    assert_eq!(status, 200);
    let effects = h.effects().await;
    assert_eq!(effects.len(), 2);
    assert_eq!(effects[1]["effect_id"], "effect-2");
    // P0-C: each fs write leaves a 4-line lifecycle chain
    // (planned, authorized, started, succeeded) in the file.
    assert_eq!(h.effects_file_lines().len(), 8);
}
