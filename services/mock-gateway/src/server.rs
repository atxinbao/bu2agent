//! Axum HTTP transport for the Mock Gateway.
//!
//! This module is additive only: it exposes the pure-logic rules
//! already defined in `lib.rs` (seed history, stream sequences
//! 1..=4, dedup, cancel, approval gate) over HTTP. It must not
//! change any of those rules. If a rule needs to change, change
//! `lib.rs` and its unit tests first, then re-check this module.
//!
//! 聊天：无 key 时 Mock 回显；配置 key 后（env DEEPSEEK_API_KEY 或 Keychain deepseek）调用 DeepSeek deepseek-flash，失败返回 502。
//! - GET  /health                -> {"status":"ok"}
//! - GET  /chat/history          -> seed history
//! - POST /chat/stream/start    -> stream records 1..=4 for a turn
//! - POST /chat/stream/apply    -> deduped records + last applied
//! - POST /chat/stream/cancel   -> cancel a stream
//! - POST /sessions/create      -> new chat session
//! - GET  /sessions/list        -> session summaries (deleted hidden)
//! - GET  /sessions/get         -> full session record
//! - POST /sessions/send        -> append turn + model reply
//! - POST /sessions/delete      -> tombstone (hidden, not erased)
//! - POST /approval/check       -> approval gate result
//! - GET  /files/list            -> list a directory in the file root
//! - GET  /files/read            -> chunked read (offset/len, base64, EOF)
//! - POST /files/write           -> write/append a file in the root
//! - POST /files/delete          -> delete a file in the root
//! - POST /approval/request      -> open a pending approval record (approval tools)
//! - POST /approval/decide       -> decide an approval record (approved/rejected/edited)
//! - POST /agent/tool/call       -> execute a tool, server re-checks its own approval record
//!
//! File scope: every file path is relative to one root (default
//! `./mock-files`, overridable with MOCK_FILES_ROOT). Absolute
//! paths and `..` are rejected, so a request can never escape
//! the root by path construction. Symlink escape is NOT checked
//! in this Mock -- the real client must check canonical paths.
//!
//! The server binds to 127.0.0.1:3001 by default (see `main.rs`,
//! overridable with MOCK_GATEWAY_ADDR). It is a Mock, never a
//! production gateway, and must not be exposed beyond localhost.
//!
//! Approval persistence (stage 2): when MOCK_APPROVALS_PATH is set,
//! approval records are appended as JSONL (one object per line)
//! on every request and decision, and reloaded at startup, so a
//! restart keeps approved records usable. When unset, records
//! stay memory-only: that keeps unit tests and throwaway runs
//! isolated. Append/load failures are stderr warnings, never
//! fatal; the in-memory map stays authoritative for the run.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_desktop::approval::ApprovalStatus;
use agent_desktop::gateway::SubscriptionRecord;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use tower_http::cors::{AllowOrigin, CorsLayer};
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::approval_store::{append_record, load_records, next_id_number, ApprovalFileRecord};
use crate::loop_engine::{
    loop_tool_requires_approval, ApprovalBinding, LoopEngine, LoopError, LoopModel, LoopStatus,
    LoopTools, LoopView, Timeouts, ToolOutcome, ErrorCode,
};
use crate::device_backend::{
    execute_set, resolve_unknown, AuthorizationStatus, ExecuteOutcome, FakeReminderBackend,
    Reminder, ReminderBackend, ResolveOutcome,
};
#[cfg(target_os = "macos")]
use crate::eventkit_backend::EventKitReminderBackend;
use crate::device_store::{
    append_action, load_actions, next_action_number, DeviceActionRecord,
};
use crate::task_store::{append_task, load_tasks, next_task_number, TaskFileRecord};
use crate::session_store::{
    append_session, load_sessions, next_session_number, select_context, session_number, title_from,
    SessionRecord, SessionTurn, MAX_INPUT_BYTES,
};
use crate::deepseek::{DeepSeekClient, DeepSeekConfig, DeepseekOutcome};
use crate::{apply_records, may_execute_tool, seed_history, start_stream, tool_requires_approval, ChatMessage, CancellableStream};

#[derive(Debug, Clone, Serialize)]
pub struct HistoryMessage {
    pub turn_id: String,
    pub role: String,
    pub text: String,
}

impl From<ChatMessage> for HistoryMessage {
    fn from(m: ChatMessage) -> Self {
        Self { turn_id: m.turn_id, role: m.role, text: m.text }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecordDto {
    pub stream_id: String,
    pub sequence: u64,
    pub event_type: String,
    pub payload: String,
    pub resume_cursor: Option<String>,
}

impl From<SubscriptionRecord> for StreamRecordDto {
    fn from(r: SubscriptionRecord) -> Self {
        Self {
            stream_id: r.stream_id,
            sequence: r.sequence,
            event_type: r.event_type,
            payload: r.payload,
            resume_cursor: r.resume_cursor,
        }
    }
}

impl From<StreamRecordDto> for SubscriptionRecord {
    fn from(d: StreamRecordDto) -> Self {
        Self {
            stream_id: d.stream_id,
            sequence: d.sequence,
            event_type: d.event_type,
            payload: d.payload,
            resume_cursor: d.resume_cursor,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StartRequest {
    pub stream_id: String,
    pub turn_id: String,
    pub text: String,
}

#[derive(Debug, Deserialize)]
pub struct ApplyRequest {
    pub stream_id: String,
    pub records: Vec<StreamRecordDto>,
}

#[derive(Debug, Serialize)]
pub struct ApplyResponse {
    pub kept: Vec<StreamRecordDto>,
    pub last_applied_sequence: u64,
}

#[derive(Debug, Deserialize)]
pub struct CancelRequest {
    pub stream_id: String,
}

#[derive(Debug, Serialize)]
pub struct CancelResponse {
    pub stream_id: String,
    pub cancelled: bool,
}

#[derive(Debug, Deserialize)]
pub struct ApprovalRequest {
    pub status: String,
    pub sensitive: bool,
}

#[derive(Debug, Serialize)]
pub struct ApprovalResponse {
    pub may_execute: bool,
}

struct StreamState {
    stream: CancellableStream,
    last_applied: u64,
}

#[derive(Clone)]
pub(crate) struct ApprovalState {
    tool: String,
    status: ApprovalStatus,
    binding: Option<ApprovalBinding>,
    consumed: bool,
    operation_digest: Option<String>,
}

impl ApprovalState {
    /// Cross-module constructor: fields stay private to this
    /// module, so other modules build records through here.
    /// (A struct literal with any private field fails E0451
    /// outside this module, even when every field is given.)
    pub(crate) fn new(
        tool: String,
        status: ApprovalStatus,
        binding: Option<ApprovalBinding>,
        consumed: bool,
        operation_digest: Option<String>,
    ) -> Self {
        ApprovalState { tool, status, binding, consumed, operation_digest }
    }
}

#[derive(Debug, Clone, Serialize)]
struct MockNote {
    id: String,
    title: String,
    body: String,
}

#[derive(Debug, Clone, Serialize)]
struct MockMessage {
    id: String,
    from: String,
    to: String,
    subject: String,
    body: String,
}

fn seed_notes() -> Vec<MockNote> {
    vec![
        MockNote { id: "note-1".to_string(), title: "Mock note 1".to_string(), body: "Fake seed data for the Mock connector demo.".to_string() },
        MockNote { id: "note-2".to_string(), title: "Mock note 2".to_string(), body: "Created by the server, never read from a real account.".to_string() },
    ]
}

fn seed_messages() -> Vec<MockMessage> {
    vec![
        MockMessage { id: "mail-1".to_string(), from: "alice@mock.local".to_string(), to: "me@mock.local".to_string(), subject: "Mock welcome".to_string(), body: "Fake seed message.".to_string() },
        MockMessage { id: "mail-2".to_string(), from: "bob@mock.local".to_string(), to: "me@mock.local".to_string(), subject: "Mock reminder".to_string(), body: "Fake seed message.".to_string() },
    ]
}

#[derive(Clone)]
// The type stays pub (as before Round 3b): integration tests
// and the pub constructors/serve functions expose it, and
// narrowing it to pub(crate) produced private_interfaces
// warnings on all of them. Fields stay pub(crate); other
// modules reach them only through server-side helpers.
pub struct AppState {
    pub(crate) loops: Arc<LoopEngine>,
    loop_fault: crate::loop_fault::LoopFault,
    pub(crate) deepseek: DeepSeekClient,
    streams: Arc<Mutex<HashMap<String, StreamState>>>,
    pub(crate) files_root: PathBuf,
    pub(crate) approvals: Arc<Mutex<HashMap<String, ApprovalState>>>,
    pub(crate) next_approval_id: Arc<Mutex<u64>>,
    approvals_path: Option<PathBuf>,
    pub(crate) tasks: Arc<Mutex<HashMap<String, TaskFileRecord>>>,
    next_task_id: Arc<Mutex<u64>>,
    tasks_path: Option<PathBuf>,
    pub(crate) task_drafts: Arc<Mutex<HashMap<String, String>>>,
    pub(crate) task_drafts_dir: Option<PathBuf>,
    sessions: Arc<Mutex<HashMap<String, SessionRecord>>>,
    next_session_id: Arc<Mutex<u64>>,
    sessions_path: Option<PathBuf>,
    actions: Arc<Mutex<HashMap<String, DeviceActionRecord>>>,
    next_action_id: Arc<Mutex<u64>>,
    actions_path: Option<PathBuf>,
    device_backend: Arc<dyn ReminderBackend>,
    device_reminder_pin: Option<String>,
    notes: Arc<Mutex<Vec<MockNote>>>,
    messages: Arc<Mutex<Vec<MockMessage>>>,
    next_note_id: Arc<Mutex<u64>>,
    next_message_id: Arc<Mutex<u64>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::from_deepseek_config(DeepSeekConfig::disabled())
    }
}

impl AppState {
    /// 正常启动共用一次凭据解析结果；测试构造器不读取环境凭据。
    pub fn from_deepseek_config(config: DeepSeekConfig) -> Self {
        let root = std::env::var("MOCK_FILES_ROOT").unwrap_or_else(|_| "mock-files".to_string());
        let approvals_path = std::env::var("MOCK_APPROVALS_PATH").ok().map(PathBuf::from);
        let mut state = AppState::with_files_root_and_approvals(PathBuf::from(root), approvals_path);
        state.loop_fault = crate::loop_fault::LoopFault::from_env(&state.files_root)
            .expect("invalid test-only loop fault configuration");
        let loops_path = std::env::var("MOCK_LOOPS_PATH").ok().map(PathBuf::from);
        state = state.with_deepseek_backends(config, loops_path)
            .expect("invalid loop configuration or audit");
        if let Ok(tasks_path) = std::env::var("MOCK_TASKS_PATH") {
            state = state.with_tasks_path(PathBuf::from(tasks_path));
        }
        if let Ok(drafts_dir) = std::env::var("MOCK_TASK_DRAFTS_PATH") {
            state = state.with_task_drafts_dir(PathBuf::from(drafts_dir));
        }
        if let Ok(sessions_path) = std::env::var("MOCK_SESSIONS_PATH") {
            state = state.with_sessions_path(PathBuf::from(sessions_path));
        }
        if let Ok(actions_path) = std::env::var("MOCK_DEVICE_ACTIONS_PATH") {
            state = state.with_actions_path(PathBuf::from(actions_path));
        }
        if let Ok(pin) = std::env::var("MOCK_DEVICE_REMINDER_ID") {
            state = state.with_device_reminder_pin(pin);
        }
        // EventKit backend selection: only the production Default
        // reads this env var, never the test constructors (which
        // always start from the Fake backend). On non-macOS the
        // var is ignored with a warning: the EventKit backend
        // does not exist there.
        #[cfg(target_os = "macos")]
        if std::env::var("MOCK_DEVICE_BACKEND").as_deref() == Ok("eventkit") {
            state = state.with_device_backend(Arc::new(EventKitReminderBackend));
        }
        #[cfg(not(target_os = "macos"))]
        if std::env::var("MOCK_DEVICE_BACKEND").as_deref() == Ok("eventkit") {
            eprintln!("warning: eventkit backend is macOS-only; using the Fake backend");
        }
        state
    }
}

pub fn new_state() -> AppState {
    AppState::default()
}

pub fn new_state_with_files_root(root: PathBuf) -> AppState {
    AppState::with_files_root_and_approvals(root, None)
}

impl AppState {
    /// Build state, reloading persisted approvals when a path is
    /// given. Unknown-status lines are skipped with a warning;
    /// the id counter continues past the largest persisted id.
    pub fn with_files_root_and_approvals(root: PathBuf, approvals_path: Option<PathBuf>) -> AppState {
        let mut approvals = HashMap::new();
        let mut next_approval_id: u64 = 1;
        if let Some(path) = &approvals_path {
            let records = load_records(path);
            for record in &records {
                match parse_approval_status(&record.status) {
                    Some(status) => {
                        approvals.insert(
                            record.tool_call_id.clone(),
                            ApprovalState { tool: record.tool.clone(), status, binding: record.binding.clone(), consumed: record.consumed, operation_digest: record.operation_digest.clone() },
                        );
                    }
                    None => {
                        eprintln!(
                            "warning: skipping approval record with unknown status {} in {}",
                            record.status,
                            path.display()
                        );
                    }
                }
            }
            next_approval_id = next_id_number(&records);
        }
        AppState {
            loop_fault: crate::loop_fault::LoopFault::default(),
            loops: Arc::new(LoopEngine::new(&root, None, None, None, Timeouts::default()).expect("memory loop engine")),
            deepseek: DeepSeekClient::new(DeepSeekConfig::disabled()),
            streams: Arc::new(Mutex::new(HashMap::new())),
            files_root: root,
            approvals: Arc::new(Mutex::new(approvals)),
            next_approval_id: Arc::new(Mutex::new(next_approval_id)),
            approvals_path,
            tasks: Arc::new(Mutex::new(HashMap::new())),
            next_task_id: Arc::new(Mutex::new(1)),
            tasks_path: None,
            task_drafts: Arc::new(Mutex::new(HashMap::new())),
            task_drafts_dir: None,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            next_session_id: Arc::new(Mutex::new(1)),
            sessions_path: None,
            actions: Arc::new(Mutex::new(HashMap::new())),
            next_action_id: Arc::new(Mutex::new(1)),
            actions_path: None,
            device_backend: default_device_backend(),
            device_reminder_pin: None,
            notes: Arc::new(Mutex::new(seed_notes())),
            messages: Arc::new(Mutex::new(seed_messages())),
            next_note_id: Arc::new(Mutex::new(3)),
            next_message_id: Arc::new(Mutex::new(3)),
        }
    }

    /// 注入模型客户端；基础构造器不读取凭据，正常启动在 main 中加载 env 与 Keychain。
    pub fn with_deepseek(mut self, client: DeepSeekClient) -> Self {
        self.deepseek = client;
        self
    }

    /// 聊天与循环共用配置；缺少 key 时聊天回显、循环不可用。
    pub fn with_deepseek_backends(self, config: DeepSeekConfig, path: Option<PathBuf>) -> Result<Self, String> {
        let model = crate::deepseek_loop::DeepSeekLoopModel::new(config.clone())?
            .map(|m| Arc::new(m) as Arc<dyn LoopModel>);
        let timeouts = self.loop_fault.timeouts();
        self.with_deepseek(DeepSeekClient::new(config))
            .with_loop_backends(path, model, None, timeouts)
    }

    /// 测试显式装配，不修改环境；仅影响循环的默认文件分派。
    pub fn with_loop_fault(mut self, fault: crate::loop_fault::LoopFault) -> Self {
        self.loop_fault = fault;
        self
    }

    /// 测试可注入循环模型和工具；未配置模型时 start 返回 503。
    pub fn with_loop_backends(
        mut self,
        path: Option<PathBuf>,
        model: Option<Arc<dyn LoopModel>>,
        tools: Option<Arc<dyn LoopTools>>,
        timeouts: Timeouts,
    ) -> Result<Self, String> {
        self.loops = Arc::new(LoopEngine::new(
            &self.files_root,
            path,
            model,
            tools,
            timeouts,
        )?);
        Ok(self)
    }

    pub(crate) fn request_loop_approval(
        &self,
        binding: ApprovalBinding,
    ) -> Result<String, LoopError> {
        let mut next = self.next_approval_id.lock().unwrap();
        let id = format!("approval-{}", *next);
        *next += 1;
        let record = ApprovalState {
            tool: binding.tool.clone(),
            status: ApprovalStatus::Pending,
            binding: Some(binding),
            consumed: false,
            operation_digest: None,
        };
        let mut approvals = self.approvals.lock().unwrap();
        persist_approval(self, &id, &record)
            .map_err(|_| LoopError(500, "approval_audit_failed"))?;
        approvals.insert(id.clone(), record);
        Ok(id)
    }

    pub(crate) fn loop_approval_status(
        &self,
        id: &str,
        binding: &ApprovalBinding,
    ) -> Result<ApprovalStatus, LoopError> {
        let approvals = self.approvals.lock().unwrap();
        let record = approvals
            .get(id)
            .ok_or(LoopError(403, "approval_required"))?;
        if record.binding.as_ref() != Some(binding)
            || record.tool != binding.tool
            || record.consumed
        {
            return Err(LoopError(403, "approval_binding_mismatch"));
        }
        Ok(record.status)
    }

    pub(crate) fn consume_loop_approval(
        &self,
        id: &str,
        binding: &ApprovalBinding,
    ) -> Result<(), LoopError> {
        let mut approvals = self.approvals.lock().unwrap();
        let record = approvals
            .get_mut(id)
            .ok_or(LoopError(403, "approval_required"))?;
        if record.binding.as_ref() != Some(binding)
            || record.status != ApprovalStatus::Approved
            || record.consumed
        {
            return Err(LoopError(403, "approval_binding_mismatch"));
        }
        record.consumed = true;
        persist_approval(self, id, record).map_err(|_| LoopError(500, "approval_audit_failed"))
    }

    /// 循环与 POST /agent/tool/call 共用服务端分派。绑定凭据不能从公共端点重放。
    pub(crate) async fn execute_loop_tool(
        &self,
        tool: String,
        args: std::collections::BTreeMap<String, String>,
        approval_id: Option<String>,
        binding: Option<ApprovalBinding>,
        backend: Option<Arc<dyn LoopTools>>,
    ) -> ToolOutcome {
        if loop_tool_requires_approval(&tool) {
            let valid = {
                let approvals = self.approvals.lock().unwrap();
                approval_id
                    .as_ref()
                    .and_then(|id| approvals.get(id))
                    .is_some_and(|record| {
                        record.status == ApprovalStatus::Approved
                            && record.consumed
                            && record.binding == binding
                            && binding.as_ref().is_some_and(|b| {
                                b.tool == tool
                                    && b.args_sha256
                                        == crate::loop_engine::args_digest(&tool, &args)
                            })
                    })
            };
            if !valid {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            }
        }
        if tool == "reminders.list" {
            let device_backend = Arc::clone(&self.device_backend);
            return match tokio::task::spawn_blocking(move || device_backend.list()).await {
                Ok(Ok(reminders)) => ToolOutcome::success(
                    serde_json::to_string(
                        &reminders
                            .iter()
                            .map(|reminder| {
                                serde_json::json!({
                                    "id": reminder.id.clone(),
                                    "title": reminder.title.clone(),
                                    "completed": reminder.completed,
                                })
                            })
                            .collect::<Vec<_>>(),
                    )
                    .unwrap_or_else(|_| "[]".to_string()),
                ),
                _ => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: "reminders list failed".to_string(),
                },
            };
        }
        if tool == "reminders.set_completed" {
            let Some(id) = args.get("id").cloned() else {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            };
            let Some(completed) = args.get("completed").and_then(|value| value.parse::<bool>().ok()) else {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            };
            if self.device_reminder_pin.as_ref().is_some_and(|pin| pin != &id) {
                return ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: "reminder is outside the configured allowlist".to_string(),
                };
            }
            let device_backend = Arc::clone(&self.device_backend);
            return match tokio::task::spawn_blocking(move || {
                execute_set(&device_backend, &id, completed, Duration::from_secs(8))
            })
            .await
            {
                Ok(ExecuteOutcome::Completed(reminder)) => ToolOutcome::success(
                    serde_json::to_string(&serde_json::json!({
                        "id": reminder.id,
                        "title": reminder.title,
                        "completed": reminder.completed,
                    }))
                    .unwrap_or_else(|_| "{}".to_string()),
                ),
                Ok(ExecuteOutcome::Unknown) => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Unknown,
                    error: Some(ErrorCode::Unconfirmed),
                    body: "reminder set result unknown; query the reminder to observe its state".to_string(),
                },
                Ok(ExecuteOutcome::AwaitingAuthorization) => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: "reminders authorization is not granted; authorize it in Devices first".to_string(),
                },
                Ok(ExecuteOutcome::Failed(error)) => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: format!("reminder operation failed: {}", error.kind.as_str()),
                },
                Err(_) => ToolOutcome::unknown(),
            };
        }
        if let Some(backend) = backend {
            return backend.execute(tool, args).await;
        }
        // 真实文件分派继续限制单根；拒绝路径中的符号链接，不能用链接绕开参数校验。
        let mut checked = self.files_root.clone();
        for component in Path::new(&args["path"]).components() {
            checked.push(component);
            if std::fs::symlink_metadata(&checked).is_ok_and(|m| m.file_type().is_symlink()) {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            }
        }
        let state = self.clone();
        let write_target =
            (tool == "fs.write").then(|| (args["path"].clone(), args["text"].clone()));
        let response = tokio::task::spawn_blocking(move || {
            let write_path = write_target.as_ref().map(|(path, _)| state.files_root.join(path));
            let response = state.loop_fault.dispatch(write_path.as_deref(), || dispatch_agent_tool(
                &state,
                ToolCallDto {
                    tool,
                    args: args.into_iter().collect(),
                    tool_call_id: approval_id,
                },
            ));
            if response.status().is_success() {
                if let Some((path, text)) = write_target {
                    // 写入和重读都在同一阻塞任务内，等待预算才能覆盖两者。
                    if std::fs::read_to_string(state.files_root.join(path))
                        .ok()
                        .as_deref()
                        != Some(&text)
                    {
                        return Err(ToolOutcome::unknown());
                    }
                }
            }
            Ok(response)
        })
        .await;
        let response = match response {
            Ok(Ok(response)) => response,
            Ok(Err(outcome)) => return outcome,
            Err(_) => return ToolOutcome::unknown(),
        };
        if !response.status().is_success() {
            return ToolOutcome::failed(ErrorCode::ToolFailed);
        }
        match axum::body::to_bytes(response.into_body(), usize::MAX).await {
            Ok(bytes) => {
                let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_default();
                let result = &value["result"];
                ToolOutcome::success(
                    result
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| result.to_string()),
                )
            }
            Err(_) => ToolOutcome::unknown(),
        }
    }

    /// Test seam: load task records from a JSONL file, so
    /// persistence tests can point at a temp path. Production
    /// state reads MOCK_TASKS_PATH in Default. A run that died
    /// with the process comes back interrupted, never running;
    /// its draft sidecar and approval survive, so resume can
    /// return it to the approval point.
    ///
    /// Fold first, then convert: the file holds history, one
    /// line per transition. Converting before folding would
    /// append an interrupted line for a stale running snapshot
    /// AFTER a later completed / rejected / cancelled line, and
    /// the next load would read that appended line as the latest
    /// state. Only the latest record per task may convert, and
    /// only converted records are appended.
    pub fn with_tasks_path(mut self, path: PathBuf) -> Self {
        let records = load_tasks(&path);
        let mut tasks = HashMap::new();
        for record in &records {
            tasks.insert(record.task_id.clone(), record.clone());
        }
        for record in tasks.values_mut() {
            if record.status == "running" {
                record.status = "interrupted".to_string();
                append_task(&path, record);
            }
        }
        self.next_task_id = Arc::new(Mutex::new(next_task_number(&records)));
        self.tasks = Arc::new(Mutex::new(tasks));
        self.tasks_path = Some(path);
        self
    }

    /// Test seam + production path for task drafts: load
    /// `<task_id>.md` sidecars so an interrupted task's draft
    /// survives a restart. Production reads MOCK_TASK_DRAFTS_PATH.
    pub fn with_task_drafts_dir(mut self, dir: PathBuf) -> Self {
        self.task_drafts =
            Arc::new(Mutex::new(crate::task_runner::load_draft_dir(&dir)));
        self.task_drafts_dir = Some(dir);
        self
    }

    /// Test seam for the loop auto-run driver (see LoopEngine).
    pub fn with_auto_drive(self, enabled: bool) -> Self {
        self.loops.set_auto_drive(enabled);
        self
    }

    /// Test seam: load session records from a JSONL file, mirroring
    /// with_tasks_path. Production state reads MOCK_SESSIONS_PATH.
    pub fn with_sessions_path(mut self, path: PathBuf) -> Self {
        let records = load_sessions(&path);
        let mut sessions = HashMap::new();
        for record in &records {
            sessions.insert(record.session_id.clone(), record.clone());
        }
        self.next_session_id = Arc::new(Mutex::new(next_session_number(&records)));
        self.sessions = Arc::new(Mutex::new(sessions));
        self.sessions_path = Some(path);
        self
    }

    /// Test seam: load action records from a JSONL file, mirroring
    /// with_tasks_path. Production state reads MOCK_DEVICE_ACTIONS_PATH.
    pub fn with_actions_path(mut self, path: PathBuf) -> Self {
        let records = load_actions(&path);
        let mut actions = HashMap::new();
        for record in &records {
            actions.insert(record.action_id.clone(), record.clone());
        }
        self.next_action_id = Arc::new(Mutex::new(next_action_number(&records)));
        self.actions = Arc::new(Mutex::new(actions));
        self.actions_path = Some(path);
        self
    }

    /// Test seam: replace the device backend (Fake by default).
    pub fn with_device_backend(mut self, backend: Arc<dyn ReminderBackend>) -> Self {
        self.device_backend = backend;
        self
    }

    /// Pin the single allowed test reminder id. When set, create/set
    /// for any other id are refused; when unset, any existing
    /// reminder may be used (the view only ever uses the recorded id).
    pub fn with_device_reminder_pin(mut self, pin: String) -> Self {
        self.device_reminder_pin = Some(pin);
        self
    }
}

/// Default device backend: a Fake seeded with one demo reminder, so the
/// wiring-phase window flow has something to operate on. This is Mock
/// demo data, never a real reminder; the EventKit backend replaces it
/// in the read-only verification phase.
fn default_device_backend() -> Arc<dyn ReminderBackend> {
    let backend = FakeReminderBackend::new();
    backend.add_reminder(Reminder {
        id: "reminder-demo".to_string(),
        title: "bu2agent 设备验证（演示）".to_string(),
        completed: false,
    });
    Arc::new(backend)
}

// --- File endpoints (Phase 2) ---

#[derive(Debug, Serialize)]
struct FileEntryDto {
    name: String,
    path: String,
    is_dir: bool,
    size_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReadQuery {
    path: String,
    offset: Option<u64>,
    len: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ReadResponseDto {
    path: String,
    offset: u64,
    content_base64: String,
    eof: bool,
    total_size: u64,
    bytes_read: u64,
}

#[derive(Debug, Deserialize)]
struct WriteRequestDto {
    path: String,
    content_base64: String,
    append: Option<bool>,
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeleteRequestDto {
    path: String,
    approval_id: Option<String>,
}

pub(crate) fn resolve_file(root: &Path, rel: &str) -> Result<PathBuf, StatusCode> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if rel_path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(root.join(rel_path))
}

pub(crate) fn ensure_root(root: &Path) -> Result<(), StatusCode> {
    std::fs::create_dir_all(root).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn files_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let rel = query.path.unwrap_or_default();
    let Ok(dir) = resolve_file(&state.files_root, &rel) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if ensure_root(&state.files_root).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create file root" }))).into_response();
    }
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "directory not found" }))).into_response();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else { continue };
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = if rel.is_empty() { name.clone() } else { format!("{rel}/{name}") };
        out.push(FileEntryDto {
            name,
            path,
            is_dir: metadata.is_dir(),
            size_bytes: if metadata.is_dir() { 0 } else { metadata.len() },
        });
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Json(out).into_response()
}

async fn files_read(
    State(state): State<AppState>,
    Query(query): Query<ReadQuery>,
) -> impl IntoResponse {
    let Ok(full) = resolve_file(&state.files_root, &query.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(data) = std::fs::read(&full) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response();
    };
    let offset = query.offset.unwrap_or(0);
    let len = query.len.unwrap_or(4096).min(1_048_576);
    let total = data.len() as u64;
    let start = (offset as usize).min(data.len());
    let end = start.saturating_add(len as usize).min(data.len());
    let chunk = &data[start..end];
    Json(ReadResponseDto {
        path: query.path,
        offset,
        content_base64: base64::engine::general_purpose::STANDARD.encode(chunk),
        eof: (end as u64) >= total,
        total_size: total,
        bytes_read: chunk.len() as u64,
    })
    .into_response()
}

async fn files_write(
    State(state): State<AppState>,
    Json(req): Json<WriteRequestDto>,
) -> impl IntoResponse {
    // The digest is computed from the normalized path and the decoded
    // bytes, then the bound approval is consumed before any write.
    // A bare call without an approval id is a 403, not a write.
    let Ok(data) = base64::engine::general_purpose::STANDARD.decode(&req.content_base64) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid base64 content" }))).into_response();
    };
    let Some(digest) = crate::operation_binding::write_digest(&req.path, &data, req.append.unwrap_or(false)) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if let Err(response) = consume_bound_approval(&state, "fs.write", &req.approval_id, &digest) {
        return *response;
    }
    let Some(normalized) = crate::operation_binding::normalize_path(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if let Some(parent) = full.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create parent directory" }))).into_response();
        }
    }
    let written: Result<u64, ()> = if req.append.unwrap_or(false) {
        (|| {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&full)
                .map_err(|_| ())?;
            f.write_all(&data).map_err(|_| ())?;
            Ok(std::fs::metadata(&full).map(|m| m.len()).unwrap_or(data.len() as u64))
        })()
    } else {
        std::fs::write(&full, &data).map(|_| data.len() as u64).map_err(|_| ())
    };
    match written {
        Ok(size) => Json(serde_json::json!({ "path": req.path, "size_bytes": size })).into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot write file" }))).into_response(),
    }
}

async fn files_delete(
    State(state): State<AppState>,
    Json(req): Json<DeleteRequestDto>,
) -> impl IntoResponse {
    // Same bound gate as files_write: no approval id, no delete.
    let Some(digest) = crate::operation_binding::delete_digest(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if let Err(response) = consume_bound_approval(&state, "fs.delete", &req.approval_id, &digest) {
        return *response;
    }
    let Some(normalized) = crate::operation_binding::normalize_path(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if full.is_dir() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "delete refuses directories" }))).into_response();
    }
    match std::fs::remove_file(&full) {
        Ok(()) => Json(serde_json::json!({ "deleted": true })).into_response(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response()
        }
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot delete file" }))).into_response(),
    }
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn history() -> Json<Vec<HistoryMessage>> {
    Json(seed_history().into_iter().map(HistoryMessage::from).collect())
}

async fn stream_start(
    State(state): State<AppState>,
    Json(req): Json<StartRequest>,
) -> impl IntoResponse {
    // DeepSeek: key presence enables the model reply; no key
    // keeps the Mock echo, and a failed model call returns
    // 502, never a silent echo.
    let reply = match state.deepseek.reply(&req.text).await {
        DeepseekOutcome::Disabled => req.text.clone(),
        DeepseekOutcome::Replied(text) => text,
        DeepseekOutcome::Failed(status) => {
            eprintln!("warning: deepseek request failed: status {status:?}");
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": "deepseek request failed",
                    "status": status,
                })),
            )
                .into_response();
        }
    };
    let records = start_stream(&req.stream_id, &req.turn_id, &reply);
    let mut streams = state.streams.lock().expect("stream state lock");
    streams.insert(
        req.stream_id.clone(),
        StreamState { stream: CancellableStream::new(), last_applied: 0 },
    );
    Json(records.into_iter().map(StreamRecordDto::from).collect::<Vec<_>>()).into_response()
}

async fn stream_apply(
    State(state): State<AppState>,
    Json(req): Json<ApplyRequest>,
) -> impl IntoResponse {
    let mut streams = state.streams.lock().expect("stream state lock");
    let Some(stream_state) = streams.get_mut(&req.stream_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown stream" })),
        )
            .into_response();
    };
    if stream_state.stream.cancelled {
        // Cancel rule from lib.rs: no later record may be applied.
        return Json(serde_json::json!({
            "kept": [],
            "last_applied_sequence": stream_state.last_applied,
        }))
        .into_response();
    }
    let records: Vec<SubscriptionRecord> =
        req.records.into_iter().map(SubscriptionRecord::from).collect();
    let (kept, last) = apply_records(&records, stream_state.last_applied);
    stream_state.last_applied = last;
    Json(ApplyResponse {
        kept: kept.into_iter().map(StreamRecordDto::from).collect(),
        last_applied_sequence: last,
    })
    .into_response()
}

async fn stream_cancel(
    State(state): State<AppState>,
    Json(req): Json<CancelRequest>,
) -> impl IntoResponse {
    let mut streams = state.streams.lock().expect("stream state lock");
    let Some(stream_state) = streams.get_mut(&req.stream_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown stream" })),
        )
            .into_response();
    };
    stream_state.stream.cancel();
    Json(CancelResponse { stream_id: req.stream_id, cancelled: true }).into_response()
}

fn parse_approval_status(s: &str) -> Option<ApprovalStatus> {
    match s {
        "not_required" => Some(ApprovalStatus::NotRequired),
        "pending" => Some(ApprovalStatus::Pending),
        "approved" => Some(ApprovalStatus::Approved),
        "rejected" => Some(ApprovalStatus::Rejected),
        "edited" => Some(ApprovalStatus::Edited),
        _ => None,
    }
}

async fn approval_check(Json(req): Json<ApprovalRequest>) -> impl IntoResponse {
    let Some(status) = parse_approval_status(&req.status) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "unknown approval status" })),
        )
            .into_response();
    };
    Json(ApprovalResponse { may_execute: may_execute_tool(status, req.sensitive) }).into_response()
}

// --- Agent invocation + approval loop (Phase 3) ---

#[derive(Debug, Deserialize)]
struct ApprovalRequestDto {
    tool: String,
    path: Option<String>,
    content_base64: Option<String>,
    append: Option<bool>,
}

#[derive(Debug, Serialize)]
struct ApprovalRequestResponse {
    tool_call_id: Option<String>,
    requires_approval: bool,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ApprovalDecideDto {
    tool_call_id: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct ToolCallDto {
    tool: String,
    args: HashMap<String, String>,
    tool_call_id: Option<String>,
}

fn bad_request(message: &str) -> axum::response::Response {
    (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": message }))).into_response()
}

async fn approval_request(
    State(state): State<AppState>,
    Json(req): Json<ApprovalRequestDto>,
) -> impl IntoResponse {
    let Some(requires) = tool_requires_approval(&req.tool) else {
        return bad_request("unknown tool");
    };
    if !requires {
        return Json(ApprovalRequestResponse {
            tool_call_id: None,
            requires_approval: false,
            status: "not_required".to_string(),
        })
        .into_response();
    }
    // File tools must present their full operation so the server can
    // bind the approval to it; the digest is computed here from the
    // parameters the client presented, never from a client hash.
    // Connector tools keep tool-only requests and stay unbound.
    let operation_digest = match req.tool.as_str() {
        "fs.write" => {
            let (Some(path), Some(content_base64)) = (req.path.as_deref(), req.content_base64.as_deref()) else {
                return bad_request("fs.write approval requires path and content_base64");
            };
            let Ok(data) = base64::engine::general_purpose::STANDARD.decode(content_base64) else {
                return bad_request("invalid base64 content");
            };
            match crate::operation_binding::write_digest(path, &data, req.append.unwrap_or(false)) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid path"),
            }
        }
        "fs.delete" => {
            let Some(path) = req.path.as_deref() else {
                return bad_request("fs.delete approval requires path");
            };
            match crate::operation_binding::delete_digest(path) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid path"),
            }
        }
        _ => None,
    };
    let mut next = state.next_approval_id.lock().expect("approval id lock");
    let id = format!("approval-{}", *next);
    *next += 1;
    let record = ApprovalState { tool: req.tool.clone(), status: ApprovalStatus::Pending, binding: None, consumed: false, operation_digest: operation_digest.clone() };
    let mut approvals = state.approvals.lock().expect("approval lock");
    if let Err(detail) = persist_approval(&state, &id, &record) {
        // Bound records fail closed like loop approvals: an id whose
        // pending line is not durable must not be issued.
        if operation_digest.is_some() {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"approval_audit_failed"}))).into_response();
        }
        eprintln!("warning: approval persistence failed: {detail}");
    }
    approvals.insert(id.clone(), record);
    Json(ApprovalRequestResponse {
        tool_call_id: Some(id),
        requires_approval: true,
        status: "pending".to_string(),
    })
    .into_response()
}

async fn approval_decide(
    State(state): State<AppState>,
    Json(req): Json<ApprovalDecideDto>,
) -> impl IntoResponse {
    let Some(status) = parse_approval_status(&req.status) else {
        return bad_request("unknown approval status");
    };
    if !matches!(status, ApprovalStatus::Approved | ApprovalStatus::Rejected | ApprovalStatus::Edited) {
        return bad_request("decision must be approved, rejected or edited");
    }
    let mut approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get_mut(&req.tool_call_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown tool_call_id" }))).into_response();
    };
    // Bound records (loop binding or operation digest) fail closed on
    // decide persistence, exactly like loop approvals already did:
    // a decision that cannot be audited must not take effect.
    let bound = record.binding.is_some() || record.operation_digest.is_some();
    if bound && (record.consumed || record.status != ApprovalStatus::Pending || status == ApprovalStatus::Edited) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({"error":"loop_approval_already_decided_or_invalid"}))).into_response();
    }
    record.status = status;
    if let Err(detail) = persist_approval(&state, &req.tool_call_id, record) {
        if bound { record.status = ApprovalStatus::Pending; return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"approval_audit_failed"}))).into_response(); }
        eprintln!("warning: approval persistence failed: {detail}");
    }
    Json(serde_json::json!({ "tool_call_id": req.tool_call_id, "status": req.status })).into_response()
}

/// Append one approval record to the persistence file, when
/// configured. Best-effort: a failure is logged to stderr and
/// the in-memory record (authoritative for this run) is kept.
pub(crate) fn persist_approval(state: &AppState, tool_call_id: &str, approval: &ApprovalState) -> Result<(), String> {
    let Some(path) = &state.approvals_path else { return Ok(()) };
    let record = ApprovalFileRecord {
        tool_call_id: tool_call_id.to_string(),
        tool: approval.tool.clone(),
        status: approval_status_name(approval.status).to_string(),
        binding: approval.binding.clone(),
        consumed: approval.consumed,
        operation_digest: approval.operation_digest.clone(),
    };
    append_record(path, &record)
}

/// Task-runner read of an approval's status. ApprovalState fields
/// stay private to this module; the runner must not read them
/// directly (a cross-module field access does not compile).
pub(crate) fn approval_status(state: &AppState, approval_id: &str) -> Option<ApprovalStatus> {
    state.approvals.lock().expect("approval lock").get(approval_id).map(|r| r.status)
}

/// Decide an approval rejected, failing closed like
/// approval_decide: a persist failure restores Pending and the
/// caller sees the error, never a silent rejection.
pub(crate) fn decide_approval_rejected(state: &AppState, approval_id: &str) -> Result<(), String> {
    let mut approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get_mut(approval_id) else {
        return Err("unknown approval".to_string());
    };
    if record.status != ApprovalStatus::Pending {
        return Err("approval already decided".to_string());
    }
    record.status = ApprovalStatus::Rejected;
    if let Err(detail) = persist_approval(state, approval_id, record) {
        record.status = ApprovalStatus::Pending;
        return Err(format!("approval_audit_failed: {detail}"));
    }
    Ok(())
}

/// Reverse of `parse_approval_status`: the string form stored in
/// the JSONL file. Every variant must round-trip through parse.
fn approval_status_name(status: ApprovalStatus) -> &'static str {
    match status {
        ApprovalStatus::NotRequired => "not_required",
        ApprovalStatus::Pending => "pending",
        ApprovalStatus::Approved => "approved",
        ApprovalStatus::Rejected => "rejected",
        ApprovalStatus::Edited => "edited",
    }
}

/// Shared approval gate for agent tools and connector writes:
/// the server trusts only its own record, never the client's
/// claim. Returns Err(403) for no record, pending, rejected,
/// or a record for a different tool.
/// The error is boxed: an axum Response is large, and a
/// bare Result<_, Response> trips clippy's large-error lint.
fn check_approval(
    state: &AppState,
    tool: &str,
    tool_call_id: &Option<String>,
) -> Result<(), Box<axum::response::Response>> {
    let Some(id) = tool_call_id else {
        return Err(Box::new((StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "approval_required" }))).into_response()));
    };
    let approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get(id) else {
        return Err(Box::new((StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "approval_required" }))).into_response()));
    };
    if record.binding.is_some() || record.tool != tool || !may_execute_tool(record.status, true) {
        let error = if record.status == ApprovalStatus::Rejected {
            "approval_rejected"
        } else {
            "approval_required"
        };
        return Err(Box::new((StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": error }))).into_response()));
    }
    Ok(())
}

/// Unified execution gate for file writes and deletes: the caller
/// presents the server-computed digest of the exact operation it is
/// about to run. Lookup, tool match, status, digest match and
/// consumption happen under a single lock acquisition, so concurrent
/// executions of one approval serialize and only the first consumes
/// it. A persistence failure returns 500 without executing, and the
/// in-memory consumed flag stays set (fail closed, as in the loop
/// path). Loop-bound records and legacy unbound records can never
/// pass this gate.
pub(crate) fn consume_bound_approval(
    state: &AppState,
    tool: &str,
    tool_call_id: &Option<String>,
    digest: &str,
) -> Result<(), Box<axum::response::Response>> {
    let forbidden = |error: &str| {
        Box::new(
            (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": error }))).into_response(),
        )
    };
    let Some(id) = tool_call_id else {
        return Err(forbidden("approval_required"));
    };
    let mut approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get_mut(id) else {
        return Err(forbidden("approval_required"));
    };
    if record.binding.is_some() || record.operation_digest.is_none() {
        return Err(forbidden("approval_binding_mismatch"));
    }
    if record.tool != tool {
        return Err(forbidden("approval_required"));
    }
    if record.status == ApprovalStatus::Rejected {
        return Err(forbidden("approval_rejected"));
    }
    if record.status != ApprovalStatus::Approved {
        return Err(forbidden("approval_required"));
    }
    if record.consumed || record.operation_digest.as_deref() != Some(digest) {
        return Err(forbidden("approval_binding_mismatch"));
    }
    record.consumed = true;
    if let Err(detail) = persist_approval(state, id, record) {
        eprintln!("warning: bound approval consumption persistence failed: {detail}");
        return Err(Box::new(
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "approval_audit_failed" }))).into_response(),
        ));
    }
    Ok(())
}

async fn agent_tool_call(
    State(state): State<AppState>,
    Json(req): Json<ToolCallDto>,
) -> impl IntoResponse {
    let Some(requires) = tool_requires_approval(&req.tool) else {
        return bad_request("unknown tool");
    };
    if requires {
        // File tools go through the bound gate: the digest is computed
        // from the very args about to be dispatched, so an approval
        // for one write can never authorize a different write. Other
        // approval tools keep the status-only gate (their endpoints
        // are not file execution paths).
        let bound = match req.tool.as_str() {
            "fs.write" => {
                let (Some(path), Some(text)) = (req.args.get("path"), req.args.get("text")) else {
                    return bad_request("fs.write requires path and text args");
                };
                crate::operation_binding::write_digest(path, text.as_bytes(), false)
            }
            "fs.delete" => {
                let Some(path) = req.args.get("path") else {
                    return bad_request("fs.delete requires a path arg");
                };
                crate::operation_binding::delete_digest(path)
            }
            _ => None,
        };
        if let Some(digest) = bound {
            if let Err(response) = consume_bound_approval(&state, &req.tool, &req.tool_call_id, &digest) {
                return *response;
            }
            // Dispatch the normalized path so the executed path is
            // exactly the path the digest bound.
            let mut req = req;
            if let Some(path) = req.args.get("path").cloned() {
                if let Some(normalized) = crate::operation_binding::normalize_path(&path) {
                    req.args.insert("path".to_string(), normalized);
                }
            }
            return dispatch_agent_tool(&state, req);
        }
        if let Err(response) = check_approval(&state, &req.tool, &req.tool_call_id) {
            return *response;
        }
    }
    dispatch_agent_tool(&state, req)
}

fn dispatch_agent_tool(state: &AppState, req: ToolCallDto) -> axum::response::Response {
    let arg = |name: &str| req.args.get(name).cloned();
    let result: Result<serde_json::Value, axum::response::Response> = match req.tool.as_str() {
        "echo" => Ok(serde_json::json!(arg("text").unwrap_or_default())),
        "fs.list" => {
            let rel = arg("path").unwrap_or_default();
            match resolve_file(&state.files_root, &rel) {
                Err(_) => Err(bad_request("path escapes file root")),
                Ok(dir) => {
                    if ensure_root(&state.files_root).is_err() {
                        Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create file root" }))).into_response())
                    } else {
                        match std::fs::read_dir(&dir) {
                            Err(_) => Err((StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "directory not found" }))).into_response()),
                            Ok(entries) => {
                                let mut out = Vec::new();
                                for entry in entries.flatten() {
                                    let Ok(metadata) = entry.metadata() else { continue };
                                    let name = entry.file_name().to_string_lossy().into_owned();
                                    let path = if rel.is_empty() { name.clone() } else { format!("{rel}/{name}") };
                                    out.push(serde_json::json!({
                                        "name": name,
                                        "path": path,
                                        "is_dir": metadata.is_dir(),
                                        "size_bytes": if metadata.is_dir() { 0 } else { metadata.len() },
                                    }));
                                }
                                out.sort_by(|a, b| a["path"].as_str().cmp(&b["path"].as_str()));
                                Ok(serde_json::Value::Array(out))
                            }
                        }
                    }
                }
            }
        }
        "fs.read" => {
            let Some(path) = arg("path") else { return bad_request("fs.read requires a path arg") };
            match resolve_file(&state.files_root, &path) {
                Err(_) => Err(bad_request("path escapes file root")),
                Ok(full) => match std::fs::read_to_string(&full) {
                    Ok(text) => Ok(serde_json::json!(text)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        Err((StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response())
                    }
                    Err(_) => Err(bad_request("file is not valid UTF-8 text")),
                },
            }
        }
        "fs.write" => {
            let (Some(path), Some(text)) = (arg("path"), arg("text")) else {
                return bad_request("fs.write requires path and text args");
            };
            match resolve_file(&state.files_root, &path) {
                Err(_) => Err(bad_request("path escapes file root")),
                Ok(full) => {
                    if let Some(parent) = full.parent() {
                        if std::fs::create_dir_all(parent).is_err() {
                            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create parent directory" }))).into_response();
                        }
                    }
                    match std::fs::write(&full, text.as_bytes()) {
                        Ok(()) => Ok(serde_json::json!({ "path": path, "size_bytes": text.len() })),
                        Err(_) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot write file" }))).into_response()),
                    }
                }
            }
        }
        "fs.delete" => {
            let Some(path) = arg("path") else { return bad_request("fs.delete requires a path arg") };
            match resolve_file(&state.files_root, &path) {
                Err(_) => Err(bad_request("path escapes file root")),
                Ok(full) => {
                    if full.is_dir() {
                        Err(bad_request("delete refuses directories"))
                    } else {
                        match std::fs::remove_file(&full) {
                            Ok(()) => Ok(serde_json::json!({ "deleted": true })),
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                Err((StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response())
                            }
                            Err(_) => Err((StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot delete file" }))).into_response()),
                        }
                    }
                }
            }
        }
        _ => Err(bad_request("unknown tool")),
    };
    match result {
        Ok(value) => Json(serde_json::json!({ "tool": req.tool, "result": value, "executed": true })).into_response(),
        Err(response) => response,
    }
}

// --- Mock connectors (Phase 4) ---
//
// Fake data only: seeded notes/messages plus items created in
// this server run. No real accounts, no real delivery, no
// OAuth. Connector writes reuse the Phase 3 approval gate;
// mail sends additionally re-check the egress domain here.

#[derive(Debug, Deserialize)]
struct NoteCreateDto {
    tool_call_id: Option<String>,
    title: String,
    body: String,
}

#[derive(Debug, Deserialize)]
struct MailSendDto {
    tool_call_id: Option<String>,
    to: String,
    subject: String,
    body: String,
}

async fn notes_list(State(state): State<AppState>) -> Json<Vec<MockNote>> {
    Json(state.notes.lock().expect("notes lock").clone())
}

async fn notes_create(
    State(state): State<AppState>,
    Json(req): Json<NoteCreateDto>,
) -> impl IntoResponse {
    if let Err(response) = check_approval(&state, "notes.create", &req.tool_call_id) {
        return *response;
    }
    if req.title.is_empty() {
        return bad_request("note title must not be empty");
    }
    let mut next = state.next_note_id.lock().expect("note id lock");
    let id = format!("note-{}", *next);
    *next += 1;
    let note = MockNote { id, title: req.title, body: req.body };
    state.notes.lock().expect("notes lock").push(note.clone());
    Json(note).into_response()
}

async fn mail_list(State(state): State<AppState>) -> Json<Vec<MockMessage>> {
    Json(state.messages.lock().expect("messages lock").clone())
}

async fn mail_send(
    State(state): State<AppState>,
    Json(req): Json<MailSendDto>,
) -> impl IntoResponse {
    if let Err(response) = check_approval(&state, "mail.send", &req.tool_call_id) {
        return *response;
    }
    let domain = req.to.rsplit_once('@').map(|(_, d)| d).unwrap_or("");
    if domain != "mock.local" {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "egress_denied" }))).into_response();
    }
    if req.subject.is_empty() {
        return bad_request("mail subject must not be empty");
    }
    let mut next = state.next_message_id.lock().expect("message id lock");
    let id = format!("mail-{}", *next);
    *next += 1;
    let message = MockMessage {
        id,
        from: "me@mock.local".to_string(),
        to: req.to,
        subject: req.subject,
        body: req.body,
    };
    state.messages.lock().expect("messages lock").push(message.clone());
    Json(message).into_response()
}

/// CORS for browser/WebView clients. The Rust CLIs and curl
/// are not subject to CORS, which is why every CLI and smoke
/// test passed while the Tauri window (page origin
/// http://localhost:5173) saw the gateway as unreachable: the
/// browser blocks cross-origin reads without these headers.
/// The allowlist is the Vite dev-server origin only, never a
/// wildcard: the Mock binds loopback only, and no other
/// website may read its responses.
fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::list([
            HeaderValue::from_static("http://localhost:5173"),
            HeaderValue::from_static("http://127.0.0.1:5173"),
        ]))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::CONTENT_TYPE])
}

// --- Tasks (deterministic task loop) ---
//
// A task is a directory + a report artifact. The manual flow stays
// frontend-driven (read -> generate -> approve -> write) through
// tasks_update. Round 3b adds a gateway runner (task_runner):
// run/resume spawn it, it generates to draft_ready, execute
// consumes the bound approval and writes, reject decides the
// approval rejected, cancel stops it between steps. Records
// persist as JSONL when MOCK_TASKS_PATH is set, mirroring the
// approval store; draft bodies live in the task_drafts sidecar,
// never in the record.

#[derive(Debug, Deserialize)]
struct TaskCreateDto {
    directory: String,
}

#[derive(Debug, Deserialize)]
struct TaskGetQuery {
    task_id: String,
}

#[derive(Debug, Deserialize)]
struct TaskUpdateDto {
    task_id: String,
    status: String,
    artifact_path: Option<String>,
}

fn valid_task_status(status: &str) -> bool {
    matches!(status, "created" | "draft_ready" | "completed" | "rejected" | "failed")
}

/// Append one task record to the persistence file, when configured.
/// Best-effort, mirroring persist_approval.
pub(crate) fn persist_task(state: &AppState, record: &TaskFileRecord) {
    let Some(path) = &state.tasks_path else { return };
    append_task(path, record);
}

fn task_number(task_id: &str) -> u64 {
    task_id.strip_prefix("task-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

async fn tasks_create(
    State(state): State<AppState>,
    Json(req): Json<TaskCreateDto>,
) -> impl IntoResponse {
    if ensure_root(&state.files_root).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create file root" }))).into_response();
    }
    let resolved = match resolve_file(&state.files_root, &req.directory) {
        Ok(full) => full,
        Err(_) => return bad_request("path escapes file root"),
    };
    if !resolved.is_dir() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "directory not found" }))).into_response();
    }
    let mut next = state.next_task_id.lock().expect("task id lock");
    let id = format!("task-{}", *next);
    *next += 1;
    let record = TaskFileRecord {
        task_id: id,
        directory: req.directory,
        artifact_path: None,
        status: "created".to_string(),
        error: None,
        approval_id: None,
        draft_sha256: None,
        draft_bytes: None,
    };
    state.tasks.lock().expect("task lock").insert(record.task_id.clone(), record.clone());
    persist_task(&state, &record);
    Json(record).into_response()
}

async fn tasks_list(State(state): State<AppState>) -> Json<Vec<TaskFileRecord>> {
    let mut records: Vec<TaskFileRecord> =
        state.tasks.lock().expect("task lock").values().cloned().collect();
    records.sort_by_key(|r| task_number(&r.task_id));
    Json(records)
}

async fn tasks_get(
    State(state): State<AppState>,
    Query(query): Query<TaskGetQuery>,
) -> impl IntoResponse {
    match state.tasks.lock().expect("task lock").get(&query.task_id) {
        Some(record) => Json(record.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response(),
    }
}

async fn tasks_update(
    State(state): State<AppState>,
    Json(req): Json<TaskUpdateDto>,
) -> impl IntoResponse {
    if !valid_task_status(&req.status) {
        return bad_request("unknown task status");
    }
    if let Some(path) = &req.artifact_path {
        if resolve_file(&state.files_root, path).is_err() {
            return bad_request("path escapes file root");
        }
    }
    let mut tasks = state.tasks.lock().expect("task lock");
    let Some(record) = tasks.get_mut(&req.task_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response();
    };
    record.status = req.status;
    if let Some(path) = req.artifact_path {
        record.artifact_path = Some(path);
    }
    let updated = record.clone();
    persist_task(&state, &updated);
    Json(updated).into_response()
}

// --- Task runner endpoints (Round 3, 3b) ---
//
// run/resume only validate and spawn; the runner owns transitions.
// Execute consumes the bound approval and never self-approves;
// the window decides approved first, then calls execute. Reject
// decides rejected itself (rejection only removes power). Cancel
// stops a running task between steps, or rejects a draft_ready
// one whose approval is still pending; an already-decided
// approval cannot be cancelled or rejected, only executed or
// left alone.

#[derive(Debug, Serialize)]
struct TaskDraftDto {
    task_id: String,
    draft: String,
    draft_sha256: Option<String>,
    draft_bytes: Option<u64>,
}

async fn tasks_run(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    // No model, no run: refused outright, never an echo draft.
    if state.deepseek.config().api_key.is_none() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "model_not_configured"})),
        )
            .into_response();
    }
    let mut tasks = state.tasks.lock().expect("task lock");
    let Some(record) = tasks.get_mut(&req.task_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    if record.status != "created" {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not runnable" })))
            .into_response();
    }
    record.status = "running".to_string();
    record.error = None;
    let updated = record.clone();
    persist_task(&state, &updated);
    drop(tasks);
    tokio::spawn(crate::task_runner::run_task(state.clone(), req.task_id));
    Json(updated).into_response()
}

async fn tasks_draft(
    State(state): State<AppState>,
    Query(query): Query<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&query.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    let Some(draft) = crate::task_runner::load_draft(&state, &query.task_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "no draft" })))
            .into_response();
    };
    Json(TaskDraftDto {
        task_id: record.task_id,
        draft_sha256: record
            .draft_sha256
            .or_else(|| Some(crate::operation_binding::sha256_hex(draft.as_bytes()))),
        draft_bytes: record.draft_bytes.or(Some(draft.len() as u64)),
        draft,
    })
    .into_response()
}

async fn tasks_execute(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&req.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    if record.status != "draft_ready" {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not executable" })))
            .into_response();
    }
    let Some(approval_id) = record.approval_id.clone() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "task approval missing" })),
        )
            .into_response();
    };
    let Some(artifact_path) = record.artifact_path.clone() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "task artifact missing" })),
        )
            .into_response();
    };
    let Some(draft) = crate::task_runner::load_draft(&state, &req.task_id) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "draft unavailable" })),
        )
            .into_response();
    };
    // The digest is recomputed server-side from the stored draft,
    // never trusted from the caller; consume is atomic and
    // single-use, exactly like files_write.
    let Some(digest) =
        crate::operation_binding::write_digest(&artifact_path, draft.as_bytes(), false)
    else {
        return bad_request("invalid path");
    };
    if let Err(response) =
        consume_bound_approval(&state, "fs.write", &Some(approval_id), &digest)
    {
        return *response;
    }
    // Same tail as files_write (overwrite): normalized path,
    // parent dirs, direct write. The approval is already consumed,
    // so a write failure fails the task instead of leaving it
    // executable; the draft is kept, and the manual Tasks flow
    // can still write it with a fresh approval.
    let Some(normalized) = crate::operation_binding::normalize_path(&artifact_path) else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "invalid path" })),
        )
            .into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return bad_request("path escapes file root");
    };
    if let Some(parent) = full.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "cannot create parent directory" })),
            )
                .into_response();
        }
    }
    if std::fs::write(&full, draft.as_bytes()).is_err() {
        crate::task_runner::fail(&state, &req.task_id, "产物写入失败");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "cannot write file" })),
        )
            .into_response();
    }
    let mut tasks = state.tasks.lock().expect("task lock");
    if let Some(current) = tasks.get_mut(&req.task_id) {
        current.status = "completed".to_string();
        current.error = None;
        let updated = current.clone();
        persist_task(&state, &updated);
        return Json(updated).into_response();
    }
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response()
}

async fn tasks_reject(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&req.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    if record.status != "draft_ready" {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not rejectable" })))
            .into_response();
    }
    let Some(approval_id) = record.approval_id.clone() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "task approval missing" })),
        )
            .into_response();
    };
    if let Err(error) = crate::task_runner::decide_rejected(&state, &approval_id) {
        if error == "approval already decided" {
            return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": error })))
                .into_response();
        }
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": error })))
            .into_response();
    }
    let mut tasks = state.tasks.lock().expect("task lock");
    if let Some(current) = tasks.get_mut(&req.task_id) {
        if current.status != "draft_ready" {
            return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not rejectable" })))
                .into_response();
        }
        current.status = "rejected".to_string();
        let updated = current.clone();
        persist_task(&state, &updated);
        return Json(updated).into_response();
    }
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response()
}

async fn tasks_resume(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&req.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    if record.status != "interrupted" {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not resumable" })))
            .into_response();
    }
    // Contract: resume is accepted, not completed. The returned
    // record is the pre-run snapshot (still interrupted); the
    // runner flips it to running or draft_ready promptly, and
    // callers poll until the status leaves interrupted. A
    // fabricated running record would lie about the runner's
    // state, and blocking here would stall on a model call.
    tokio::spawn(crate::task_runner::run_task(state.clone(), req.task_id));
    Json(record).into_response()
}

async fn tasks_cancel(
    State(state): State<AppState>,
    Json(req): Json<TaskGetQuery>,
) -> impl IntoResponse {
    let Some(record) =
        state.tasks.lock().expect("task lock").get(&req.task_id).cloned()
    else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
            .into_response();
    };
    match record.status.as_str() {
        "running" => {
            // The runner observes the flip at its next checkpoint
            // and discards the model result; no approval exists
            // yet, so nothing is left behind.
            let mut tasks = state.tasks.lock().expect("task lock");
            if let Some(current) = tasks.get_mut(&req.task_id) {
                if current.status != "running" {
                    return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not cancellable" })))
                        .into_response();
                }
                current.status = "cancelled".to_string();
                let updated = current.clone();
                persist_task(&state, &updated);
                return Json(updated).into_response();
            }
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
                .into_response()
        }
        "draft_ready" => {
            let Some(approval_id) = record.approval_id.clone() else {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "task approval missing" })),
                )
                    .into_response();
            };
            if let Err(error) = crate::task_runner::decide_rejected(&state, &approval_id) {
                if error == "approval already decided" {
                    return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": error })))
                        .into_response();
                }
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": error })))
                    .into_response();
            }
            let mut tasks = state.tasks.lock().expect("task lock");
            if let Some(current) = tasks.get_mut(&req.task_id) {
                if current.status != "draft_ready" {
                    return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not cancellable" })))
                        .into_response();
                }
                current.status = "cancelled".to_string();
                let updated = current.clone();
                persist_task(&state, &updated);
                return Json(updated).into_response();
            }
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
                .into_response()
        }
        _ => (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not cancellable" })))
            .into_response(),
    }
}

// --- Chat sessions (Round 3, 3a) ---
//
// Session bodies live here, never in the audit JSONL files.
// Deletion is a tombstone: hidden from list/get/send and never
// entering model context, but earlier snapshot lines stay in
// the file until it is removed. The UI delete confirmation must
// say exactly that.

#[derive(Debug, Deserialize)]
struct SessionCreateDto {
    title: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SessionGetQuery {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct SessionSendDto {
    session_id: String,
    text: String,
}

#[derive(Debug, Deserialize)]
struct SessionDeleteDto {
    session_id: String,
}

#[derive(Debug, Serialize)]
struct SessionSummary {
    session_id: String,
    title: String,
    created_ms: u64,
    updated_ms: u64,
    turn_count: usize,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Append one session snapshot to the persistence file, when
/// configured. Best-effort, mirroring persist_task.
fn persist_session(state: &AppState, record: &SessionRecord) {
    let Some(path) = &state.sessions_path else { return };
    append_session(path, record);
}

async fn sessions_create(
    State(state): State<AppState>,
    Json(req): Json<SessionCreateDto>,
) -> impl IntoResponse {
    let mut next = state.next_session_id.lock().expect("session id lock");
    let id = format!("session-{}", *next);
    *next += 1;
    let now = now_ms();
    let record = SessionRecord {
        session_id: id,
        title: req.title.unwrap_or_default(),
        created_ms: now,
        updated_ms: now,
        deleted: false,
        turns: Vec::new(),
    };
    state.sessions.lock().expect("session lock").insert(record.session_id.clone(), record.clone());
    persist_session(&state, &record);
    Json(record).into_response()
}

async fn sessions_list(State(state): State<AppState>) -> Json<Vec<SessionSummary>> {
    let mut records: Vec<SessionSummary> = state
        .sessions
        .lock()
        .expect("session lock")
        .values()
        .filter(|r| !r.deleted)
        .map(|r| SessionSummary {
            session_id: r.session_id.clone(),
            title: r.title.clone(),
            created_ms: r.created_ms,
            updated_ms: r.updated_ms,
            turn_count: r.turns.len(),
        })
        .collect();
    records.sort_by_key(|r| session_number(&r.session_id));
    Json(records)
}

async fn sessions_get(
    State(state): State<AppState>,
    Query(query): Query<SessionGetQuery>,
) -> impl IntoResponse {
    match state.sessions.lock().expect("session lock").get(&query.session_id) {
        Some(record) if !record.deleted => Json(record.clone()).into_response(),
        _ => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" }))).into_response(),
    }
}

async fn sessions_send(
    State(state): State<AppState>,
    Json(req): Json<SessionSendDto>,
) -> impl IntoResponse {
    let text = req.text.trim().to_string();
    if text.is_empty() {
        return bad_request("empty message");
    }
    if text.len() > MAX_INPUT_BYTES {
        return bad_request("message too large");
    }
    // Record the user turn first, then release the lock for the
    // model call: holding a std mutex across a 30 s await would
    // stall every other session request.
    let context = {
        let mut sessions = state.sessions.lock().expect("session lock");
        let Some(record) = sessions.get_mut(&req.session_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
                .into_response();
        };
        if record.deleted {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
                .into_response();
        }
        let context = select_context(&record.turns);
        let first_message = record.turns.is_empty();
        let now = now_ms();
        record.turns.push(SessionTurn {
            role: "user".to_string(),
            text: text.clone(),
            created_ms: now,
            model: None,
            finish_reason: None,
        });
        if first_message && record.title.is_empty() {
            record.title = title_from(&text);
        }
        record.updated_ms = now;
        let snapshot = record.clone();
        persist_session(&state, &snapshot);
        context
    };
    let outcome = state.deepseek.reply_with_history(&context, &text).await;
    let (reply, model, finish_reason) = match outcome {
        DeepseekOutcome::Disabled => (text.clone(), None, None),
        DeepseekOutcome::Replied(reply) => {
            (reply, Some(state.deepseek.config().model.clone()), Some("stop".to_string()))
        }
        DeepseekOutcome::Failed(status) => {
            // The user turn is already persisted; a failed model
            // call adds no assistant turn and never echoes.
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": "deepseek request failed", "status": status })),
            )
                .into_response();
        }
    };
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(&req.session_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    };
    if record.deleted {
        // Deleted while the model was in flight: the delete wins,
        // the late reply is dropped, never resurrecting the session.
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    }
    record.turns.push(SessionTurn {
        role: "assistant".to_string(),
        text: reply,
        created_ms: now_ms(),
        model,
        finish_reason,
    });
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_session(&state, &updated);
    Json(updated).into_response()
}

async fn sessions_delete(
    State(state): State<AppState>,
    Json(req): Json<SessionDeleteDto>,
) -> impl IntoResponse {
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(&req.session_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    };
    record.deleted = true;
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_session(&state, &updated);
    Json(serde_json::json!({ "session_id": updated.session_id, "deleted": true })).into_response()
}

// --- Device loop (B-layer single-device validation) ---
//
// The gateway owns the device backend (Fake for now, EventKit later)
// and the action records. The frontend drives the fixed steps
// (create -> approve -> set -> re-query); the gateway only stores
// action records and executes approved sets. Records persist as
// JSONL when MOCK_DEVICE_ACTIONS_PATH is set, mirroring tasks.
//
// The view drives actions sequentially; concurrent set/reject on
// one action is out of scope for this Mock.

const DEVICE_ID: &str = "mac-reminders";
const DEVICE_ACTION: &str = "reminders.set_completed";
const DEVICE_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Serialize)]
struct ReminderDto {
    id: String,
    title: String,
    completed: bool,
}

impl From<&Reminder> for ReminderDto {
    fn from(r: &Reminder) -> Self {
        Self { id: r.id.clone(), title: r.title.clone(), completed: r.completed }
    }
}

#[derive(Debug, Deserialize)]
struct ActionCreateDto {
    reminder_id: String,
    target_completed: bool,
}

#[derive(Debug, Deserialize)]
struct DeviceSetDto {
    action_id: String,
    tool_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ActionRejectDto {
    action_id: String,
    tool_call_id: String,
}

#[derive(Debug, Deserialize)]
struct ActionRefreshDto {
    action_id: String,
}

#[derive(Debug, Deserialize)]
struct ActionGetQuery {
    action_id: String,
}

/// Append one action record to the persistence file, when
/// configured. Best-effort, mirroring persist_task.
fn persist_action(state: &AppState, record: &DeviceActionRecord) {
    let Some(path) = &state.actions_path else { return };
    append_action(path, record);
}

fn action_number(action_id: &str) -> u64 {
    action_id.strip_prefix("action-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

/// Allowlist pin: when a test reminder id is pinned, any other id
/// is refused. Returns Some(error response) on refusal, None when ok.
fn device_pin_error(state: &AppState, reminder_id: &str) -> Option<axum::response::Response> {
    if let Some(pin) = &state.device_reminder_pin {
        if pin != reminder_id {
            return Some(
                (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "reminder_not_allowed" })))
                    .into_response(),
            );
        }
    }
    None
}

async fn reminders_list(State(state): State<AppState>) -> impl IntoResponse {
    match state.device_backend.list() {
        Ok(reminders) => {
            Json(reminders.iter().map(ReminderDto::from).collect::<Vec<_>>()).into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn actions_create(
    State(state): State<AppState>,
    Json(req): Json<ActionCreateDto>,
) -> impl IntoResponse {
    if let Some(response) = device_pin_error(&state, &req.reminder_id) {
        return response;
    }
    let mut next = state.next_action_id.lock().expect("action id lock");
    let id = format!("action-{}", *next);
    *next += 1;
    let record = DeviceActionRecord {
        action_id: id,
        device: DEVICE_ID.to_string(),
        action: DEVICE_ACTION.to_string(),
        reminder_id: req.reminder_id,
        target_completed: req.target_completed,
        status: "requested".to_string(),
        detail: None,
    };
    state.actions.lock().expect("action lock").insert(record.action_id.clone(), record.clone());
    persist_action(&state, &record);
    Json(record).into_response()
}

async fn device_set(
    State(state): State<AppState>,
    Json(req): Json<DeviceSetDto>,
) -> impl IntoResponse {
    if let Err(response) = check_approval(&state, DEVICE_ACTION, &req.tool_call_id) {
        return *response;
    }
    let (reminder_id, target) = {
        let mut actions = state.actions.lock().expect("action lock");
        let Some(record) = actions.get_mut(&req.action_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
                .into_response();
        };
        if record.status != "requested" {
            return bad_request("action is not requested");
        }
        if let Some(response) = device_pin_error(&state, &record.reminder_id) {
            return response;
        }
        record.status = "approved".to_string();
        let approved = record.clone();
        persist_action(&state, &approved);
        (record.reminder_id.clone(), record.target_completed)
    };
    let backend = Arc::clone(&state.device_backend);
    let outcome = tokio::task::spawn_blocking(move || {
        execute_set(&backend, &reminder_id, target, Duration::from_secs(DEVICE_TIMEOUT_SECS))
    })
    .await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "device execution failed" })),
            )
                .into_response();
        }
    };
    let mut actions = state.actions.lock().expect("action lock");
    let Some(record) = actions.get_mut(&req.action_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response();
    };
    match outcome {
        ExecuteOutcome::Completed(_) => {
            record.status = "completed".to_string();
            record.detail = None;
        }
        ExecuteOutcome::Failed(e) => {
            record.status = "failed".to_string();
            record.detail = Some(e.to_string());
        }
        ExecuteOutcome::Unknown => {
            record.status = "unknown".to_string();
            record.detail =
                Some("write submitted but target state not confirmed by readback".to_string());
        }
        ExecuteOutcome::AwaitingAuthorization => {
            record.status = "awaiting_authorization".to_string();
            record.detail = Some("waiting for authorization".to_string());
        }
    }
    let updated = record.clone();
    persist_action(&state, &updated);
    Json(updated).into_response()
}

async fn actions_mark_rejected(
    State(state): State<AppState>,
    Json(req): Json<ActionRejectDto>,
) -> impl IntoResponse {
    // The approval itself must be rejected: no device call happens here.
    let approval_status = {
        let approvals = state.approvals.lock().expect("approval lock");
        approvals.get(&req.tool_call_id).map(|r| r.status)
    };
    let Some(status) = approval_status else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown tool_call_id" })))
            .into_response();
    };
    if status != ApprovalStatus::Rejected {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "approval_not_rejected" })),
        )
            .into_response();
    }
    let mut actions = state.actions.lock().expect("action lock");
    let Some(record) = actions.get_mut(&req.action_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response();
    };
    if record.status != "requested" {
        return bad_request("action is not requested");
    }
    if let Some(response) = device_pin_error(&state, &record.reminder_id) {
        return response;
    }
    record.status = "rejected".to_string();
    let updated = record.clone();
    persist_action(&state, &updated);
    Json(updated).into_response()
}

async fn actions_refresh(
    State(state): State<AppState>,
    Json(req): Json<ActionRefreshDto>,
) -> impl IntoResponse {
    let current = {
        let actions = state.actions.lock().expect("action lock");
        let Some(record) = actions.get(&req.action_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
                .into_response();
        };
        record.clone()
    };
    if current.status != "unknown" {
        return Json(current).into_response();
    }
    let outcome =
        resolve_unknown(&state.device_backend, &current.reminder_id, current.target_completed);
    let mut actions = state.actions.lock().expect("action lock");
    let Some(record) = actions.get_mut(&req.action_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response();
    };
    match outcome {
        ResolveOutcome::Completed(_) => {
            record.status = "completed".to_string();
            record.detail = None;
        }
        ResolveOutcome::Failed(e) => {
            record.status = "failed".to_string();
            record.detail = Some(e.to_string());
        }
        ResolveOutcome::StillUnknown(maybe_err) => {
            record.detail = Some(match maybe_err {
                Some(e) => e.to_string(),
                None => "state unchanged; write may still be in flight".to_string(),
            });
        }
    }
    let updated = record.clone();
    persist_action(&state, &updated);
    Json(updated).into_response()
}

async fn actions_list(State(state): State<AppState>) -> Json<Vec<DeviceActionRecord>> {
    let mut records: Vec<DeviceActionRecord> =
        state.actions.lock().expect("action lock").values().cloned().collect();
    records.sort_by_key(|r| action_number(&r.action_id));
    Json(records)
}

async fn actions_get(
    State(state): State<AppState>,
    Query(query): Query<ActionGetQuery>,
) -> impl IntoResponse {
    match state.actions.lock().expect("action lock").get(&query.action_id) {
        Some(record) => Json(record.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response(),
    }
}

fn authorization_status_name(status: AuthorizationStatus) -> &'static str {
    match status {
        AuthorizationStatus::Authorized => "authorized",
        AuthorizationStatus::Denied => "denied",
        AuthorizationStatus::NotDetermined => "not_determined",
    }
}

#[derive(Debug, Serialize)]
struct AuthorizationStatusDto {
    status: String,
}

/// Read-only: report the backend's current reminders authorization.
/// No approval needed: this reads permission state, it writes nothing.
async fn devices_authorization(State(state): State<AppState>) -> Json<AuthorizationStatusDto> {
    Json(AuthorizationStatusDto {
        status: authorization_status_name(state.device_backend.authorization_status()).to_string(),
    })
}

/// Ask the system for reminders access and report the resulting
/// status. This is the one endpoint that may show the OS prompt
/// (only when the status is NotDetermined). It writes no reminder:
/// it only requests the permission precondition. The request runs
/// on a blocking thread because the synchronous EventKit request
/// waits up to 30 seconds for an answer; a JoinError fails closed
/// to Denied (a panicked request proves nothing, never Authorized).
async fn devices_authorization_request(
    State(state): State<AppState>,
) -> Json<AuthorizationStatusDto> {
    let backend = Arc::clone(&state.device_backend);
    let status = tokio::task::spawn_blocking(move || backend.request_authorization())
        .await
        .unwrap_or(AuthorizationStatus::Denied);
    Json(AuthorizationStatusDto {
        status: authorization_status_name(status).to_string(),
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoopIdDto {
    loop_id: String,
}

fn loop_response(
    state: &AppState,
    result: Result<crate::loop_engine::LoopView, LoopError>,
) -> axum::response::Response {
    match result {
        Ok(mut view) => {
            if view.pending_write.is_some() || view.pending_reminder.is_some() {
                if let Some(call) = view.record.calls.get(view.record.queue_position) {
                    if let (Some(id), Some(binding)) = (&call.approval_id, &call.binding) {
                        view.approval_status = state.loop_approval_status(id, binding).ok()
                            .map(|status| approval_status_name(status).to_string());
                    }
                }
            }
            fill_pending_reminder_title(state, &mut view);
            Json(view).into_response()
        },
        Err(LoopError(status, code)) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            Json(serde_json::json!({"error":code})),
        )
            .into_response(),
    }
}

fn fill_pending_reminder_title(state: &AppState, view: &mut LoopView) {
    if let Some(pending) = view.pending_reminder.as_mut() {
        if let Ok(Some(reminder)) = state.device_backend.get(&pending.reminder_id) {
            pending.title = reminder.title;
        }
    }
}

/// Start an auto-run driver unless auto-drive is off (loop_smoke)
/// or the loop already stopped. The driver re-checks the flag and
/// the loop state itself, so a redundant spawn exits at once.
fn spawn_driver(state: &AppState, loop_id: &str) {
    if !state.loops.auto_drive() {
        return;
    }
    let engine = Arc::clone(&state.loops);
    let app = state.clone();
    let loop_id = loop_id.to_string();
    tokio::spawn(async move {
        engine.drive_until_stopped(app, loop_id).await;
    });
}

async fn loop_start(
    State(state): State<AppState>,
    Json(req): Json<crate::loop_engine::StartLoop>,
) -> axum::response::Response {
    let result = state.loops.start(req);
    if let Ok(view) = &result {
        spawn_driver(&state, &view.record.loop_id);
    }
    loop_response(&state, result)
}
async fn loop_get(
    State(state): State<AppState>,
    Query(req): Query<LoopIdDto>,
) -> axum::response::Response {
    loop_response(&state, state.loops.get(&req.loop_id))
}
async fn loop_resume(
    State(state): State<AppState>,
    Json(req): Json<crate::loop_engine::ResumeLoop>,
) -> axum::response::Response {
    let engine = Arc::clone(&state.loops);
    let result = engine.resume(state.clone(), req).await;
    // A manual resume runs one step, then hands over to the
    // driver; a stopping view needs no driver. Frontend copy
    // says "run to next stopping point", not "step once".
    if let Ok(view) = &result {
        if !view.record.status.terminal() && view.record.status != LoopStatus::AwaitingApproval {
            spawn_driver(&state, &view.record.loop_id);
        }
    }
    loop_response(&state, result)
}
async fn loop_cancel(
    State(state): State<AppState>,
    Json(req): Json<LoopIdDto>,
) -> axum::response::Response {
    loop_response(&state, state.loops.cancel(&req.loop_id))
}
async fn loops_list(State(state): State<AppState>) -> Json<Vec<LoopView>> {
    let mut views = state.loops.list();
    for view in &mut views {
        fill_pending_reminder_title(&state, view);
    }
    Json(views)
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/chat/history", get(history))
        .route("/chat/stream/start", post(stream_start))
        .route("/chat/stream/apply", post(stream_apply))
        .route("/chat/stream/cancel", post(stream_cancel))
        .route("/approval/check", post(approval_check))
        .route("/files/list", get(files_list))
        .route("/files/read", get(files_read))
        .route("/files/write", post(files_write))
        .route("/files/delete", post(files_delete))
        .route("/approval/request", post(approval_request))
        .route("/approval/decide", post(approval_decide))
        .route("/agent/tool/call", post(agent_tool_call))
        .route("/agent/loop/start", post(loop_start))
        .route("/agent/loop/get", get(loop_get))
        .route("/agent/loop/resume", post(loop_resume))
        .route("/agent/loop/cancel", post(loop_cancel))
        .route("/agent/loop/list", get(loops_list))
        .route("/connectors/notes/list", get(notes_list))
        .route("/connectors/notes/create", post(notes_create))
        .route("/connectors/mail/list", get(mail_list))
        .route("/connectors/mail/send", post(mail_send))
        .route("/tasks/create", post(tasks_create))
        .route("/tasks/list", get(tasks_list))
        .route("/tasks/get", get(tasks_get))
        .route("/tasks/update", post(tasks_update))
        .route("/tasks/run", post(tasks_run))
        .route("/tasks/draft", get(tasks_draft))
        .route("/tasks/execute", post(tasks_execute))
        .route("/tasks/reject", post(tasks_reject))
        .route("/tasks/resume", post(tasks_resume))
        .route("/tasks/cancel", post(tasks_cancel))
        .route("/sessions/create", post(sessions_create))
        .route("/sessions/list", get(sessions_list))
        .route("/sessions/get", get(sessions_get))
        .route("/sessions/send", post(sessions_send))
        .route("/sessions/delete", post(sessions_delete))
        .route("/devices/reminders/list", get(reminders_list))
        .route("/devices/actions/create", post(actions_create))
        .route("/devices/reminders/set", post(device_set))
        .route("/devices/actions/mark_rejected", post(actions_mark_rejected))
        .route("/devices/actions/refresh", post(actions_refresh))
        .route("/devices/actions/list", get(actions_list))
        .route("/devices/actions/get", get(actions_get))
        .route("/devices/authorization", get(devices_authorization))
        .route("/devices/authorization/request", post(devices_authorization_request))
        .layer(cors_layer())
        .with_state(state)
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    state: AppState,
) -> std::io::Result<()> {
    // Phase 5: Ctrl-C stops the server gracefully instead of
    // leaving in-flight requests cut off mid-response.
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        println!("mock-gateway shutting down");
    };
    axum::serve(listener, app(state))
        .with_graceful_shutdown(shutdown)
        .await
}
