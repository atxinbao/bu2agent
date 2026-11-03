//! P0-C tests: Effect lifecycle enforcement, approval binding
//! to Effect, idempotency enforcement, and unknown
//! reconciliation. Gateway kernel only: no client, no UI.
//!
//! Covered here (contract §4/§5, task P0-C items 1-5):
//! - lifecycle positive chain + illegal-transition refusals
//!   (including `planned` straight to `succeeded`, terminal
//!   re-transition, `NeedsReview` as a non-word, legacy rows
//!   that cannot be driven);
//! - approval bound to Effect for device and loop, digest
//!   tampering 403, consumed reuse, one approval unusable for
//!   another Effect, legacy approvals keeping their old gate;
//! - idempotency: terminal replay without a second side
//!   effect (file content is the witness), `unknown` blocking
//!   execution, loop duplicate writing once;
//! - reconciliation: unknown -> reconciling -> reconciled with
//!   a conclusion field, inconclusive re-checks returning to
//!   `unknown` (never `failed`), no-readback Effect types
//!   staying `unknown`;
//! - old rows read verbatim.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_desktop::credential_store::MemoryCredentialStore;
use mock_gateway::device_backend::{FakeFault, FakeReminderBackend, Reminder};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p0c-{}-{}-{}",
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
    fake: Arc<FakeReminderBackend>,
}

impl Harness {
    async fn serve(dir: PathBuf, files_root: PathBuf, fake: Arc<FakeReminderBackend>) -> Self {
        let state = AppState::with_files_root_and_approvals(
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
        .with_device_backend(Arc::clone(&fake) as Arc<dyn mock_gateway::device_backend::ReminderBackend>);
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
            fake,
        }
    }

    async fn new() -> Self {
        let dir = temp_dir("harness");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
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
        Self::serve(dir, files_root, fake).await
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
        let response = self.client.get(format!("{}{}", self.url, path)).send().await.unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn effects(&self) -> Vec<Value> {
        let (status, body) = self.get("/effects/list").await;
        assert_eq!(status, 200, "{body}");
        body.as_array().unwrap().clone()
    }

    async fn effect(&self, id: &str) -> Value {
        let (status, body) = self.get(&format!("/effects/get?effect_id={id}")).await;
        assert_eq!(status, 200, "{body}");
        body
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
        self.ok("/approval/decide", json!({"tool_call_id": id, "status": status})).await;
    }

    async fn last_approval(&self, id: &str) -> Value {
        let (status, body) = self.get(&format!("/approvals/{id}")).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    fn files_root(&self) -> PathBuf {
        self.dir.join("files")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The last persisted approvals-file line for an id. The
/// query views (list/detail/history) do not carry
/// `action_binding`; the file record does.
fn approval_file_record(dir: &std::path::Path, id: &str) -> Value {
    let text = std::fs::read_to_string(dir.join("approvals.jsonl")).expect("approvals file");
    let records: Vec<Value> = text
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter(|r| r["tool_call_id"] == id)
        .collect();
    records
        .into_iter()
        .last()
        .unwrap_or_else(|| panic!("no persisted approval {id} in {text}"))
}

fn find_effect<'a>(effects: &'a [Value], capability: &str, operation: &str, scope: &str) -> &'a Value {
    effects
        .iter()
        .find(|e| {
            e["capability"] == capability && e["operation"] == operation && e["scope"] == scope
        })
        .unwrap_or_else(|| panic!("no effect {capability}/{operation}/{scope} in {effects:?}"))
}

// ---------------------------------------------------------------- lifecycle

#[tokio::test]
async fn lifecycle_positive_chain_and_illegal_transitions_refused() {
    let h = Harness::new().await;
    // The approval request plans the write: a `planned` row
    // already exists, and the approval is bound to it.
    let approval = h
        .request_approval("fs.write", json!({"path": "a.txt", "content_base64": "aGVsbG8="}))
        .await;
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["status"], "planned");
    let effect_id = effects[0]["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.last_approval(&approval).await["effect_binding"], effect_id);

    // Illegal jump: planned straight to succeeded is refused
    // and leaves the row unchanged.
    let (status, _) = h
        .post("/effects/transition", json!({"effect_id": effect_id, "status": "succeeded"}))
        .await;
    assert_eq!(status, 409);
    assert_eq!(h.effect(&effect_id).await["status"], "planned");

    // NeedsReview never enters the Effect vocabulary.
    let (status, _) = h
        .post("/effects/transition", json!({"effect_id": effect_id, "status": "needs_review"}))
        .await;
    assert_eq!(status, 400);

    // Unknown effect id, and a word outside the vocabulary.
    let (status, _) = h
        .post("/effects/transition", json!({"effect_id": "effect-99", "status": "started"}))
        .await;
    assert_eq!(status, 404);

    // The positive chain, one edge at a time.
    for (from, to) in [
        ("planned", "authorized"),
        ("authorized", "started"),
        ("started", "succeeded"),
    ] {
        assert_eq!(h.effect(&effect_id).await["status"], from);
        let (status, body) = h
            .post("/effects/transition", json!({"effect_id": effect_id, "status": to}))
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["status"], to);
    }
    // Terminal states never move again.
    let (status, _) = h
        .post("/effects/transition", json!({"effect_id": effect_id, "status": "planned"}))
        .await;
    assert_eq!(status, 409);
    assert_eq!(h.effect(&effect_id).await["status"], "succeeded");

    // Reconciled without a conclusion is refused.
    let approval2 = h
        .request_approval("fs.write", json!({"path": "b.txt", "content_base64": "aGVsbG8="}))
        .await;
    let _ = approval2;
    let effects = h.effects().await;
    let second = find_effect(&effects, "fs", "write", "b.txt");
    let second_id = second["effect_id"].as_str().unwrap().to_string();
    for to in ["authorized", "started", "unknown", "reconciling"] {
        let (status, _) = h
            .post("/effects/transition", json!({"effect_id": second_id, "status": to}))
            .await;
        assert_eq!(status, 200, "to {to}");
    }
    let (status, _) = h
        .post("/effects/transition", json!({"effect_id": second_id, "status": "reconciled"}))
        .await;
    assert_eq!(status, 400, "reconciled needs a conclusion");
    let (status, body) = h
        .post(
            "/effects/transition",
            json!({"effect_id": second_id, "status": "reconciled", "reconciliation_result": "completed"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "reconciled");
    assert_eq!(body["reconciliation"]["result"], "completed");
    assert!(body["reconciliation"]["checked_ms"].is_number());
}

#[tokio::test]
async fn legacy_rows_read_verbatim_and_cannot_be_driven() {
    let dir = temp_dir("legacy");
    std::fs::write(
        dir.join("effects.jsonl"),
        "{\"effect_id\":\"effect-1\",\"capability\":\"devices\",\"operation\":\"set\",\"scope\":\"action-9\",\"status\":\"completed\"}\n",
    )
    .unwrap();
    let files_root = dir.join("files");
    std::fs::create_dir_all(&files_root).unwrap();
    let fake = Arc::new(FakeReminderBackend::new());
    let h = Harness::serve(dir, files_root, fake).await;
    // Old row reads verbatim: old word, no reconciliation.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["status"], "completed");
    assert!(effects[0]["reconciliation"].is_null());
    // And it cannot be driven forward: history is never
    // translated, so no lifecycle edge applies to it.
    let (status, _) = h
        .post("/effects/transition", json!({"effect_id": "effect-1", "status": "succeeded"}))
        .await;
    assert_eq!(status, 409);
    assert_eq!(h.effect("effect-1").await["status"], "completed");
}

// ---------------------------------------------------------------- approval binding

#[tokio::test]
async fn device_approval_binds_effect_and_reuse_fails() {
    let h = Harness::new().await;
    let (_, created) = h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-1", "target_completed": true}),
        )
        .await;
    let action_id = created["action_id"].as_str().unwrap().to_string();
    // Planned at creation, before any approval exists.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["status"], "planned");
    let effect_id = effects[0]["effect_id"].as_str().unwrap().to_string();
    assert_eq!(created["effect_id"], effect_id);

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

    // The approval is consumed and now bound to the Effect.
    // The action binding lives on the persisted record
    // (phase-2 readers), not on the query view.
    let record = h.last_approval(&approval).await;
    assert_eq!(record["consumed"], true);
    assert_eq!(record["effect_binding"], effect_id);
    let persisted = approval_file_record(&h.dir, &approval);
    assert_eq!(persisted["action_binding"], action_id);
    let effect = h.effect(&effect_id).await;
    assert_eq!(effect["status"], "succeeded");
    assert_eq!(effect["approval_id"], approval);

    // Reuse for a second action: 403, and the second action
    // stays requested with only its planned Effect.
    let (_, created2) = h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-2", "target_completed": true}),
        )
        .await;
    let (status, _) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": created2["action_id"], "tool_call_id": approval}),
        )
        .await;
    assert_eq!(status, 403);
    let effects = h.effects().await;
    let second = find_effect(&effects, "device", "reminders.set_completed", "reminder-2");
    assert_eq!(second["status"], "planned");
}

#[tokio::test]
async fn fs_digest_tamper_replay_and_unknown_block() {
    let h = Harness::new().await;
    let approval = h
        .request_approval("fs.write", json!({"path": "a.txt", "content_base64": "aGVsbG8="}))
        .await;
    h.decide(&approval, "approved").await;

    // Tampered parameters: 403, approval unconsumed, planned
    // Effect untouched.
    let (status, _) = h
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8td29ybGQ=", "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 403);
    assert_eq!(h.last_approval(&approval).await["consumed"], false);
    assert_eq!(h.effects().await[0]["status"], "planned");

    // One approval cannot authorize a different Effect: the
    // same approval presented for b.txt is a mismatch.
    let (status, _) = h
        .post(
            "/files/write",
            json!({"path": "b.txt", "content_base64": "aGVsbG8=", "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 403);

    // Correct write: 200, Effect succeeded, file written once.
    let (status, _) = h
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8=", "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(std::fs::read_to_string(h.files_root().join("a.txt")).unwrap(), "hello");

    // Re-presenting the same approval and parameters replays
    // the terminal Effect: 200 with replayed=true, and the
    // file is not written a second time (an append-mode second
    // write would have doubled the content; here the replay
    // path is proven by the flag plus the unchanged content).
    let (status, body) = h
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8=", "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["replayed"], true);
    assert_eq!(std::fs::read_to_string(h.files_root().join("a.txt")).unwrap(), "hello");

    // Unknown blocks execution: drive a fresh planned Effect
    // to unknown, then the write refuses with 409 and writes
    // nothing, without consuming the approval.
    let approval2 = h
        .request_approval("fs.write", json!({"path": "c.txt", "content_base64": "aGVsbG8="}))
        .await;
    h.decide(&approval2, "approved").await;
    let effects = h.effects().await;
    let planned = find_effect(&effects, "fs", "write", "c.txt");
    let planned_id = planned["effect_id"].as_str().unwrap().to_string();
    for to in ["authorized", "started", "unknown"] {
        let (status, _) = h
            .post("/effects/transition", json!({"effect_id": planned_id, "status": to}))
            .await;
        assert_eq!(status, 200, "to {to}");
    }
    let (status, body) = h
        .post(
            "/files/write",
            json!({"path": "c.txt", "content_base64": "aGVsbG8=", "approval_id": approval2}),
        )
        .await;
    assert_eq!(status, 409, "{body}");
    assert!(!h.files_root().join("c.txt").exists());
    assert_eq!(h.last_approval(&approval2).await["consumed"], false);
}

#[tokio::test]
async fn legacy_approvals_keep_their_old_gate() {
    // Device: a pre-P0-C approval (no effect_binding, no
    // digest) and a pre-P0-C action (no effect_id) still set.
    let dir = temp_dir("legacy-device");
    std::fs::write(
        dir.join("approvals.jsonl"),
        "{\"tool_call_id\":\"approval-1\",\"tool\":\"reminders.set_completed\",\"status\":\"approved\",\"consumed\":false}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("actions.jsonl"),
        "{\"action_id\":\"action-1\",\"device\":\"mac-reminders\",\"action\":\"reminders.set_completed\",\"reminder_id\":\"reminder-1\",\"target_completed\":true,\"status\":\"requested\",\"detail\":null}\n",
    )
    .unwrap();
    let files_root = dir.join("files");
    std::fs::create_dir_all(&files_root).unwrap();
    let fake = Arc::new(FakeReminderBackend::new());
    fake.add_reminder(Reminder {
        id: "reminder-1".to_string(),
        title: "Test reminder 1".to_string(),
        completed: false,
    });
    let h = Harness::serve(dir, files_root, fake).await;
    let (status, body) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": "action-1", "tool_call_id": "approval-1"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "completed");
    let record = h.last_approval("approval-1").await;
    assert_eq!(record["consumed"], true);
    let persisted = approval_file_record(&h.dir, "approval-1");
    assert_eq!(persisted["action_binding"], "action-1");
    // The new row uses the new word, even on the legacy path.
    let effects = h.effects().await;
    let effect = find_effect(&effects, "device", "reminders.set_completed", "reminder-1");
    assert_eq!(effect["status"], "succeeded");

    // Files: a pre-P0-C digest-bound approval (no
    // effect_binding, no planned row) still writes, once,
    // through the legacy gate, and its new row uses the new
    // word.
    let dir2 = temp_dir("legacy-fs");
    let digest = mock_gateway::operation_binding::write_digest("a.txt", b"hello", false).unwrap();
    std::fs::write(
        dir2.join("approvals.jsonl"),
        format!(
            "{{\"tool_call_id\":\"approval-1\",\"tool\":\"fs.write\",\"status\":\"approved\",\"consumed\":false,\"operation_digest\":\"{digest}\"}}\n"
        ),
    )
    .unwrap();
    let files_root2 = dir2.join("files");
    std::fs::create_dir_all(&files_root2).unwrap();
    let h2 = Harness::serve(dir2, files_root2, Arc::new(FakeReminderBackend::new())).await;
    let (status, body) = h2
        .post(
            "/files/write",
            json!({"path": "a.txt", "content_base64": "aGVsbG8=", "approval_id": "approval-1"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(std::fs::read_to_string(h2.files_root().join("a.txt")).unwrap(), "hello");
    let effects = h2.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["status"], "succeeded");
    assert_eq!(effects[0]["approval_id"], "approval-1");
}

// ---------------------------------------------------------------- reconciliation

#[tokio::test]
async fn unknown_reconciliation_completed_inconclusive_and_no_readback() {
    let h = Harness::new().await;

    // Force unknown: the backend times out on set.
    h.fake.set_fault(Some(FakeFault::Timeout));
    let (_, created) = h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-1", "target_completed": true}),
        )
        .await;
    let action_id = created["action_id"].as_str().unwrap().to_string();
    let effect_id = created["effect_id"].as_str().unwrap().to_string();
    let approval = h.request_approval("reminders.set_completed", json!({})).await;
    h.decide(&approval, "approved").await;
    let (status, body) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": action_id, "tool_call_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "unknown");
    assert_eq!(h.effect(&effect_id).await["status"], "unknown");

    // The write actually landed late: flip the backend state
    // (as a late write would), clear the fault, reconcile.
    h.fake.set_fault(None);
    h.fake.add_reminder(Reminder {
        id: "reminder-1".to_string(),
        title: "Test reminder 1".to_string(),
        completed: true,
    });
    let (status, body) = h
        .post("/effects/reconcile", json!({"effect_id": effect_id}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "reconciled");
    assert_eq!(body["reconciliation"]["result"], "completed");
    assert!(body["reconciliation"]["checked_ms"].is_number());
    let (_, action) = h.get(&format!("/devices/actions/get?action_id={action_id}")).await;
    assert_eq!(action["status"], "completed");

    // Inconclusive, state mismatch: a second unknown write
    // whose target never landed stays unknown after reconcile.
    h.fake.set_fault(Some(FakeFault::Timeout));
    let (_, created2) = h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-2", "target_completed": true}),
        )
        .await;
    let action2 = created2["action_id"].as_str().unwrap().to_string();
    let effect2 = created2["effect_id"].as_str().unwrap().to_string();
    let approval2 = h.request_approval("reminders.set_completed", json!({})).await;
    h.decide(&approval2, "approved").await;
    let (status, _) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": action2, "tool_call_id": approval2}),
        )
        .await;
    assert_eq!(status, 200);
    h.fake.set_fault(None);
    let (status, body) = h.post("/effects/reconcile", json!({"effect_id": effect2})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "unknown", "inconclusive re-check returns to unknown");
    assert!(body["reconciliation"].is_null(), "no verdict on an inconclusive re-check");
    let (_, action) = h.get(&format!("/devices/actions/get?action_id={action2}")).await;
    assert_eq!(action["status"], "unknown");

    // Inconclusive, query failure: the re-check itself errors,
    // and the Effect must never become failed.
    h.fake.set_fault(Some(FakeFault::Unavailable));
    let (status, body) = h.post("/effects/reconcile", json!({"effect_id": effect2})).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "unknown");
    h.fake.set_fault(None);

    // No readback: an fs Effect driven to unknown has no
    // re-check method; reconcile refuses and leaves it unknown.
    let approval3 = h
        .request_approval("fs.write", json!({"path": "n.txt", "content_base64": "aGVsbG8="}))
        .await;
    let effects = h.effects().await;
    let fs_effect = find_effect(&effects, "fs", "write", "n.txt");
    let fs_id = fs_effect["effect_id"].as_str().unwrap().to_string();
    let _ = approval3;
    for to in ["authorized", "started", "unknown"] {
        let (status, _) = h
            .post("/effects/transition", json!({"effect_id": fs_id, "status": to}))
            .await;
        assert_eq!(status, 200, "to {to}");
    }
    let (status, body) = h.post("/effects/reconcile", json!({"effect_id": fs_id})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_no_readback");
    assert_eq!(h.effect(&fs_id).await["status"], "unknown");

    // actions_refresh drives the same reconciliation for the
    // device action: still unknown, still not failed.
    let (status, body) = h
        .post("/devices/actions/refresh", json!({"action_id": action2}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "unknown");
    assert_eq!(h.effect(&effect2).await["status"], "unknown");
}

// ---------------------------------------------------------------- loop

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
        let response = self.client.get(format!("{}{}", self.url, path)).send().await.unwrap();
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
        self.get("/effects/list").await.as_array().unwrap().clone()
    }
}

impl Drop for LoopHarness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn loop_call_planned_at_proposal_and_bound_to_effect() {
    let h = LoopHarness::new(vec![calls(vec![write_proposal("out", "loop body")]), done()]).await;
    let loop_id = h.start().await;
    h.step(&loop_id).await;
    let waiting = h.step(&loop_id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    let call = &waiting["calls"][waiting["queue_position"].as_u64().unwrap() as usize];
    let binding = call["binding"].clone();
    let approval = call["approval_id"].as_str().unwrap().to_string();

    // Planned at proposal: one planned Effect, bound to the
    // approval, before anything is decided or executed.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["status"], "planned");
    assert_eq!(effects[0]["capability"], "fs");
    assert_eq!(effects[0]["operation"], "write");
    assert_eq!(effects[0]["scope"], "out");
    let effect_id = effects[0]["effect_id"].as_str().unwrap().to_string();
    let approval_view = h.get(&format!("/approvals/{approval}")).await;
    assert_eq!(approval_view["effect_binding"], effect_id);

    h.ok("/approval/decide", json!({"tool_call_id": approval, "status": "approved"})).await;
    let result = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop_id, "approval_id": approval, "binding": binding}),
        )
        .await;
    assert_eq!(result["calls"][0]["status"], "succeeded");
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["status"], "succeeded");
    assert_eq!(effects[0]["approval_id"], approval);
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn loop_duplicate_call_writes_only_once() {
    // The same write proposed twice in one loop: the second
    // proposal is a Duplicate (driver dedup, retained) and the
    // idempotency gate would refuse a second dispatch anyway.
    // The backend write counter is the witness.
    let h = LoopHarness::new(vec![
        calls(vec![write_proposal("out", "same body"), write_proposal("out", "same body")]),
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
    h.ok("/approval/decide", json!({"tool_call_id": approval, "status": "approved"})).await;
    let dispatched = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop_id, "approval_id": approval, "binding": binding}),
        )
        .await;
    assert_eq!(dispatched["calls"][0]["status"], "succeeded");
    // The duplicate proposal is delivered without a dispatch.
    let dup = h.step(&loop_id).await;
    assert_eq!(dup["calls"][1]["status"], "duplicate");
    let final_view = h.step(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    assert_eq!(h.files.writes.load(Ordering::SeqCst), 1, "one backend write only");
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "a Duplicate builds no second Effect");
    assert_eq!(effects[0]["status"], "succeeded");
}
