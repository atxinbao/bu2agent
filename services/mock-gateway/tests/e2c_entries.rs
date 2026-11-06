//! E2-C entry-convergence tests (plan §4, C1–C6): the shared
//! ledger entry — Device direct + loop writes, Connector
//! reuse / adapter fallback, Evidence export, and the §4.4
//! no-Effect positions.
//!
//! HTTP routes only, except C3, which drives
//! `record_connector_effect` directly: no task/loop path
//! performs a connector write yet, so no HTTP route can supply
//! a caller Execution today (the helper is `pub` for exactly
//! this seam; cf. `spawn_assignment_sweeper` in e2b).
//! Temp dirs, SQLite mode (the Evidence routes need it);
//! timing budgets are generous (cross-platform hygiene skill).
//! C2's 4.5s backend delay costs 9s end to end (write +
//! readback share the budget) — between the old 8s loop budget
//! and the unified 10s DEVICE_TIMEOUT_SECS on purpose.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_desktop::credential_store::MemoryCredentialStore;
use mock_gateway::device_backend::{FakeReminderBackend, Reminder, ReminderBackend};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, record_connector_effect, AppState};
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-e2c-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct ScriptModel {
    steps: Mutex<VecDeque<ModelReply>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(replies.into()),
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
        let reply = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move { Ok(reply) })
    }
}

fn proposal(tool: &str, args: Value) -> Proposal {
    Proposal {
        tool: tool.into(),
        args,
        wire: None,
    }
}

fn set_proposal(id: &str, completed: bool) -> ModelReply {
    ModelReply::Calls {
        proposals: vec![proposal(
            "reminders.set_completed",
            json!({"id": id, "completed": completed}),
        )],
        content: None,
    }
}

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    state: AppState,
    fake: Arc<FakeReminderBackend>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        Self::serve_with_delay(replies, Duration::ZERO).await
    }

    async fn serve_with_delay(replies: Vec<ModelReply>, delay: Duration) -> Self {
        let dir = temp_dir("harness");
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let fake = Arc::new(FakeReminderBackend::new());
        fake.add_reminder(Reminder {
            id: "reminder-1".into(),
            title: "E2-C reminder 1".into(),
            completed: false,
        });
        fake.add_reminder(Reminder {
            id: "reminder-2".into(),
            title: "E2-C reminder 2".into(),
            completed: false,
        });
        if !delay.is_zero() {
            fake.set_delay(delay);
        }
        let model = Arc::new(ScriptModel::new(replies));
        let state = AppState::with_files_root_and_approvals(
            root.clone(),
            Some(dir.join("approvals.jsonl")),
        )
        .with_device_backend(fake.clone())
        .with_loop_backends(
            Some(dir.join("loops.jsonl")),
            Some(model),
            None,
            Timeouts::default(),
        )
        .unwrap()
        .with_auto_drive(false)
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_evidence_path(dir.join("evidence.jsonl"))
        .with_binding_state_path(dir.join("bindings.jsonl"))
        .with_project_state_path(dir.join("projects.jsonl"))
        .with_location_state_path(dir.join("locations.jsonl"))
        .with_worktree_state_path(dir.join("worktrees.jsonl"))
        .with_tasks_path(dir.join("tasks.jsonl"))
        .with_task_drafts_dir(dir.join("drafts"))
        .with_connector_state_path(dir.join("connectors.jsonl"))
        .with_vault_state_path(dir.join("vault.jsonl"))
        .with_vault_secrets(Arc::new(MemoryCredentialStore::new()))
        .with_sqlite_path(dir.join("gateway.sqlite3"));
        state.sqlite_import_and_switch().expect("switch");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server_state = state.clone();
        let server =
            tokio::spawn(async move { axum::serve(listener, app(server_state)).await.unwrap() });
        Self {
            dir,
            url,
            // C2's 9s tool dispatch runs inside one resume
            // call; 60s leaves ample headroom on slow CI.
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            server,
            state,
            fake,
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
        assert_eq!(status, 200, "{path}: {body}");
        body
    }

    async fn patch(&self, path: &str, value: Value) -> (u16, Value) {
        let response = self
            .client
            .patch(format!("{}{}", self.url, path))
            .json(&value)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn delete(&self, path: &str) -> (u16, Value) {
        let response = self
            .client
            .delete(format!("{}{}", self.url, path))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
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

    async fn get_ok(&self, path: &str) -> Value {
        let (status, body) = self.get(path).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    /// Raw text GET (the export route returns JSONL, not JSON).
    async fn get_text(&self, path: &str) -> (u16, String) {
        let response = self
            .client
            .get(format!("{}{}", self.url, path))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        (status, body)
    }

    async fn effects(&self) -> Vec<Value> {
        self.get_ok("/effects/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn executions(&self) -> Vec<Value> {
        self.get_ok("/executions/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn execution_for_loop(&self, loop_id: &str) -> Value {
        self.executions()
            .await
            .into_iter()
            .find(|e| e["loop_id"] == loop_id)
            .unwrap_or_else(|| panic!("no execution for loop {loop_id}"))
    }

    async fn evidence(&self) -> Vec<Value> {
        self.get_ok("/evidence/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn start(&self) -> String {
        self.ok("/agent/loop/start", json!({"prompt": "e2c loop"}))
            .await["loop_id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id": id})).await
    }

    /// Step until the loop awaits approval; returns its
    /// approval id + binding.
    async fn await_approval(&self, loop_id: &str) -> (String, Value) {
        let mut view = Value::Null;
        for _ in 0..8 {
            view = self.step(loop_id).await;
            if view["status"] == "awaiting_approval" {
                break;
            }
        }
        assert_eq!(view["status"], "awaiting_approval", "{view}");
        let queue = view["queue_position"].as_u64().unwrap() as usize;
        let call = &view["calls"][queue];
        (
            call["approval_id"].as_str().unwrap().to_string(),
            call["binding"].clone(),
        )
    }

    /// Decide approved, then resume the loop with that
    /// approval + binding; returns the resume view.
    async fn approve_and_resume(&self, loop_id: &str, approval: &str, binding: &Value) -> Value {
        self.ok(
            "/approval/decide",
            json!({"tool_call_id": approval, "status": "approved"}),
        )
        .await;
        self.ok(
            "/agent/loop/resume",
            json!({"loop_id": loop_id, "approval_id": approval, "binding": binding}),
        )
        .await
    }

    /// Approve a direct-route tool (no loop binding).
    async fn approve_direct(&self, tool: &str) -> String {
        let body = self
            .ok("/approval/request", json!({"tool": tool}))
            .await;
        let id = body["tool_call_id"].as_str().unwrap().to_string();
        self.ok(
            "/approval/decide",
            json!({"tool_call_id": id, "status": "approved"}),
        )
        .await;
        id
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// C1 (§4.2): the direct Device write runs the full chain
/// through the shared entry — one planned Effect at action
/// creation, driven to succeeded by the set, on the action's
/// own Execution, with no second row.
#[tokio::test]
async fn e2c_device_write_full_chain() {
    let h = Harness::serve(Vec::new()).await;
    let action = h
        .ok(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-1", "target_completed": true}),
        )
        .await;
    let action_id = action["action_id"].as_str().unwrap();
    let execution_id = action["execution_id"].as_str().unwrap().to_string();
    let effect_id = action["effect_id"].as_str().unwrap().to_string();
    let planned = h
        .effects()
        .await
        .into_iter()
        .find(|e| e["effect_id"] == effect_id)
        .expect("planned effect");
    assert_eq!(planned["status"], "planned");
    assert_eq!(planned["execution_id"], execution_id);
    assert_eq!(planned["capability"], "device");
    assert_eq!(planned["operation"], "reminders.set_completed");
    assert_eq!(planned["scope"], "reminder-1");

    let tool_call_id = h.approve_direct("reminders.set_completed").await;
    let updated = h
        .ok(
            "/devices/reminders/set",
            json!({"action_id": action_id, "tool_call_id": tool_call_id}),
        )
        .await;
    assert_eq!(updated["status"], "completed");
    let rows: Vec<_> = h
        .effects()
        .await
        .into_iter()
        .filter(|e| {
            e["capability"] == "device"
                && e["operation"] == "reminders.set_completed"
                && e["scope"] == "reminder-1"
        })
        .collect();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["effect_id"], effect_id);
    assert_eq!(rows[0]["status"], "succeeded");
    assert_eq!(rows[0]["execution_id"], execution_id);
    assert!(h.fake.get("reminder-1").unwrap().unwrap().completed);
}

/// C2 (§4.2): the loop Device write uses the same 10s budget
/// as the direct route. `execute_set` runs the write and its
/// readback inside the one budget, so a 4.5s backend delay
/// costs 9s end to end: over the old 8s loop budget, under the
/// unified 10s DEVICE_TIMEOUT_SECS. The set must therefore
/// complete — and its Effect must land on the loop's
/// Execution as a single succeeded row.
#[tokio::test]
async fn e2c_device_loop_path_same_budget() {
    let h = Harness::serve_with_delay(
        vec![set_proposal("reminder-1", true), done()],
        Duration::from_secs_f64(4.5),
    )
    .await;
    let loop_id = h.start().await;
    let (approval, binding) = h.await_approval(&loop_id).await;
    let after = h.approve_and_resume(&loop_id, &approval, &binding).await;
    assert_eq!(after["calls"][0]["status"], "succeeded", "{after}");
    let final_view = h.step(&loop_id).await;
    assert_eq!(final_view["status"], "completed", "{final_view}");
    assert!(h.fake.get("reminder-1").unwrap().unwrap().completed);
    let execution_id = h.execution_for_loop(&loop_id).await["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    let rows: Vec<_> = h
        .effects()
        .await
        .into_iter()
        .filter(|e| {
            e["capability"] == "reminders"
                && e["operation"] == "set_completed"
                && e["scope"] == "reminder-1"
        })
        .collect();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"], "succeeded");
    assert_eq!(rows[0]["execution_id"], execution_id);
}

/// C3 (§4.3): a connector write with a caller Execution
/// reuses it — the Effect is attributed to the loop that
/// triggered the write — and a second identical write is a
/// replay under the gate, not a second row.
#[tokio::test]
async fn e2c_connector_write_reuses_caller_execution() {
    let h = Harness::serve(vec![done()]).await;
    let loop_id = h.start().await;
    let completed = h.step(&loop_id).await;
    assert_eq!(completed["status"], "completed", "{completed}");
    let execution_id = h.execution_for_loop(&loop_id).await["execution_id"]
        .as_str()
        .unwrap()
        .to_string();
    record_connector_effect(
        &h.state,
        Some(execution_id.clone()),
        "connectors",
        "connect",
        "notes",
    );
    let rows: Vec<_> = h
        .effects()
        .await
        .into_iter()
        .filter(|e| {
            e["capability"] == "connectors"
                && e["operation"] == "connect"
                && e["scope"] == "notes"
        })
        .collect();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"], "succeeded");
    assert_eq!(rows[0]["execution_id"], execution_id);
    record_connector_effect(
        &h.state,
        Some(execution_id.clone()),
        "connectors",
        "connect",
        "notes",
    );
    let rows: Vec<_> = h
        .effects()
        .await
        .into_iter()
        .filter(|e| {
            e["capability"] == "connectors"
                && e["operation"] == "connect"
                && e["scope"] == "notes"
        })
        .collect();
    assert_eq!(rows.len(), 1, "{rows:?}");
}

/// C4 (§4.3): a direct-route connector write keeps the
/// adapter Execution fallback — each write builds its own
/// fresh Execution, owned by no task or loop.
#[tokio::test]
async fn e2c_connector_write_direct_route_adapter_fallback() {
    let h = Harness::serve(Vec::new()).await;
    h.ok("/connectors/notes/accounts/link", json!({})).await;
    h.ok("/connectors/notes/connect", json!({})).await;
    h.ok("/connectors/notes/disconnect", json!({})).await;
    h.ok("/connectors/notes/connect", json!({})).await;
    let connects: Vec<_> = h
        .effects()
        .await
        .into_iter()
        .filter(|e| {
            e["capability"] == "connectors"
                && e["operation"] == "connect"
                && e["scope"] == "notes"
        })
        .collect();
    assert_eq!(connects.len(), 2, "{connects:?}");
    assert!(connects.iter().all(|e| e["status"] == "succeeded"));
    let first = connects[0]["execution_id"].as_str().unwrap();
    let second = connects[1]["execution_id"].as_str().unwrap();
    assert_ne!(
        first, second,
        "direct routes build a fresh adapter Execution per write"
    );
    let executions = h.executions().await;
    for id in [first, second] {
        let exec = executions
            .iter()
            .find(|e| e["execution_id"] == id)
            .expect("adapter execution listed");
        assert!(exec["task_id"].is_null(), "{exec}");
        assert!(exec["loop_id"].is_null(), "{exec}");
    }
}

/// C5 (§4.5): the Evidence export serves the verify's row as
/// JSONL, the audit file written at the verify holds the same
/// row, and both agree with the list.
#[tokio::test]
async fn e2c_evidence_export_jsonl() {
    let h = Harness::serve(Vec::new()).await;
    let action = h
        .ok(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-1", "target_completed": true}),
        )
        .await;
    let action_id = action["action_id"].as_str().unwrap();
    let effect_id = action["effect_id"].as_str().unwrap().to_string();
    let tool_call_id = h.approve_direct("reminders.set_completed").await;
    h.ok(
        "/devices/reminders/set",
        json!({"action_id": action_id, "tool_call_id": tool_call_id}),
    )
    .await;
    let evidence = h
        .ok("/effects/verify", json!({"effect_id": effect_id}))
        .await;
    assert_eq!(evidence["verdict"], "verified");
    let evidence_id = evidence["evidence_id"].as_str().unwrap().to_string();

    let (status, body) = h.get_text("/evidence/export").await;
    assert_eq!(status, 200, "{body}");
    let lines: Vec<Value> = body
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 1, "{body}");
    assert_eq!(lines[0]["evidence_id"], evidence_id);

    let file = std::fs::read_to_string(h.dir.join("evidence.jsonl")).expect("evidence export file");
    let file_lines: Vec<Value> = file
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(file_lines.len(), 1, "{file}");
    assert_eq!(file_lines[0]["evidence_id"], evidence_id);

    let listed = h.evidence().await;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["evidence_id"], evidence_id);
}

/// C6 (§4.4): each of the five no-Effect positions produces
/// no Effect row. Every item measures the count immediately
/// before and after its own call, so setup rows (action
/// planning, credential registration) do not leak into the
/// assertion.
#[tokio::test]
async fn e2c_no_effect_positions() {
    let h = Harness::serve(Vec::new()).await;
    let count = || async { h.effects().await.len() };

    // 1. Credentials capture request: intentionally no Effect
    // (the ticket is a request, not a write; the fulfill is
    // the write and keeps its own row).
    h.ok(
        "/v1/credentials/register",
        json!({"handle": "h-e2c", "secret": "x"}),
    )
    .await;
    let before = count().await;
    let ticket = h
        .ok(
            "/v1/credentials/capture/external-provider/request",
            json!({"provider": "p-e2c", "handle": "h-e2c"}),
        )
        .await;
    assert!(ticket["ticket_id"].as_str().is_some(), "{ticket}");
    assert_eq!(count().await, before, "capture request wrote an Effect");

    // 2. devices/actions/mark_rejected: no new Effect; the
    // planned row stays as the audit (Q16).
    let action = h
        .ok(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-2", "target_completed": true}),
        )
        .await;
    let action_id = action["action_id"].as_str().unwrap();
    let effect_id = action["effect_id"].as_str().unwrap().to_string();
    let body = h
        .ok("/approval/request", json!({"tool": "reminders.set_completed"}))
        .await;
    let tool_call_id = body["tool_call_id"].as_str().unwrap();
    h.ok(
        "/approval/decide",
        json!({"tool_call_id": tool_call_id, "status": "rejected"}),
    )
    .await;
    let before = count().await;
    let rejected = h
        .ok(
            "/devices/actions/mark_rejected",
            json!({"action_id": action_id, "tool_call_id": tool_call_id}),
        )
        .await;
    assert_eq!(rejected["status"], "rejected", "{rejected}");
    assert_eq!(count().await, before, "mark_rejected wrote an Effect");
    let planned = h
        .effects()
        .await
        .into_iter()
        .find(|e| e["effect_id"] == effect_id)
        .expect("planned effect");
    assert_eq!(planned["status"], "planned", "mark_rejected moved the planned row");

    // 3. devices/authorization/request: not a write, no Effect.
    let before = count().await;
    let (status, _) = h.post("/devices/authorization/request", json!({})).await;
    assert_eq!(status, 200);
    assert_eq!(count().await, before, "authorization request wrote an Effect");

    // 4. connectors notes.create / mail.send: deferred,
    // Mock-only — no Effect row.
    let notes_approval = h.approve_direct("notes.create").await;
    let before = count().await;
    let note = h
        .ok(
            "/connectors/notes/create",
            json!({"tool_call_id": notes_approval, "title": "t", "body": "b"}),
        )
        .await;
    assert!(note["id"].as_str().is_some(), "{note}");
    assert_eq!(count().await, before, "notes.create wrote an Effect");
    let mail_approval = h.approve_direct("mail.send").await;
    let before = count().await;
    let message = h
        .ok(
            "/connectors/mail/send",
            json!({"tool_call_id": mail_approval, "to": "a@mock.local", "subject": "s", "body": "b"}),
        )
        .await;
    assert!(message["id"].as_str().is_some(), "{message}");
    assert_eq!(count().await, before, "mail.send wrote an Effect");

    // 5. PATCH/DELETE /permissions/connectors: no Effect;
    // policy audit belongs to D1.
    let before = count().await;
    let (status, _) = h
        .patch("/permissions/connectors/notes", json!({"allowed": false}))
        .await;
    assert_eq!(status, 200);
    assert_eq!(count().await, before, "policy PATCH wrote an Effect");
    let (status, _) = h.delete("/permissions/connectors/notes").await;
    assert_eq!(status, 200);
    assert_eq!(count().await, before, "policy DELETE wrote an Effect");
}
