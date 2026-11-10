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
//! - GET  /files/stat            -> stat a file or directory
//! - POST /files/rename          -> move a file or directory
//! - POST /files/mkdir           -> create a directory
//! - POST /files/reads|stats     -> bounded batch read/stat
//! - POST /files/search          -> bounded name/path search
//! - GET  /files/raw             -> HTTP raw bytes (JSON variant for invoke)
//! - POST /files/subscribe       -> long-poll directory cursor
//! - GET  /files/thumbnail       -> bounded image thumbnail
//! - POST /files/export|library  -> bound external export and local media index
//! - POST /approval/request      -> open a pending approval record (approval tools)
//! - POST /approval/decide       -> decide an approval record (approved/rejected/edited)
//! - GET  /approvals             -> current approval records, one per id
//! - GET  /approvals/history     -> approval event history (JSONL order)
//! - GET  /approvals/{id}        -> one approval record
//! - GET  /approvals/{id}/editable-message -> message + whether it may be edited
//! - POST /approvals/{id}/edit-message     -> edit an unbound pending approval's message
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
use std::io::Cursor;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_desktop::approval::ApprovalStatus;
use agent_desktop::gateway::SubscriptionRecord;
use axum::{
    body::Body,
    Json, Router,
    extract::{Query, State},
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
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
use crate::task_store::{append_task, load_tasks, next_task_number, TaskExecuteBinding, TaskFileRecord};
use crate::execution_store::{
    append_execution, load_executions, next_execution_number, ExecutionFileRecord,
};
use crate::effect_store::{
    append_effect, load_effects, next_effect_number, EffectFileRecord, NewEffect,
    Reconciliation,
};
use crate::sqlite_store::{self, StateBackend};
use rusqlite::Connection;
use crate::session_store::{
    append_session, load_sessions, next_session_number, select_context, session_number, title_from,
    SessionRecord, SessionTurn, MAX_INPUT_BYTES,
};
use crate::connector_store::{
    account_number, append_connector_state, load_connector_state, next_account_number,
    ConnectorAccount, ConnectorConnection, ConnectorStateLine,
};
use crate::vault_store::{
    append_vault_state, load_vault_state, next_ticket_number, ticket_expired, CatalogEntry,
    CaptureTicket, CredentialSlot, CustomConnectorDef, TicketStatus, VaultStateLine, TICKET_TTL_MS,
};
use crate::policy_store::{
    append_policy_state, effective_policy, load_policy_state, ConnectorPolicy, EffectivePolicy,
    NetworkRule, PolicyDefaults, PolicyOverrides, PolicyStateLine, ScopeRecord,
};
use crate::model_store::{append_model_state, load_model_state, ModelStateLine, StagedModel};
use crate::peer_store::{
    append_peer_state, load_peer_state, next_invite_number, next_message_number, Peer,
    PeerInboxMessage, PeerInvite, PeerRequest, PeerStateLine,
};
use crate::space_store::{append_space_state, load_space_state, Space, SpaceItem, SpaceStateLine};
use crate::feed_store::{
    append_feed_state, load_feed_state, seed_feed_units, FeedStateLine, FeedUnit,
};
use crate::chat_store::{
    append_chat_state, load_chat_state, ChatStateLine, SeenMarker, CHAT_THEMES,
    DEFAULT_CHAT_THEME,
};
use crate::node_store::{
    append_node, load_nodes, next_node_number, Node, NODE_DISCONNECTED, NODE_ONLINE,
};
use crate::binding_store::{
    append_binding, load_bindings, next_workspace_number, WorkspaceBinding, BINDING_ACTIVE,
    BINDING_ARCHIVED,
};
use crate::project_store::{
    append_project, load_projects, next_project_number, Project, PROJECT_ACTIVE,
    PROJECT_ARCHIVED,
};
use crate::location_store::{
    append_location, load_locations, next_location_number, Location, LOCATION_ACTIVE,
    LOCATION_ARCHIVED,
};
use crate::worktree_store::{
    append_worktree, load_worktrees, next_worktree_number, Worktree, WORKTREE_ACTIVE,
    WORKTREE_ARCHIVED,
};
use crate::stream_store::{
    append_stream_line, events_after, last_event_id, load_stream_state, next_poke_order,
    next_stream_number, next_subscription_number, project_thread_events, StreamLine,
    StreamOpen, StreamPoke, StreamSubscription, SxsSelection,
};
use crate::mail_source::{clamp_limit, MailError, MailSource, UnavailableMailSource};
use crate::voice_store::{
    append_voice_line, catalog_locales, catalog_voices, load_voice_state,
    merged_custom_voices, pipeline_profiles, ActiveVoiceRecord, CustomVoice,
    PipelineSelectionRecord, VoiceLine,
};
use agent_desktop::credential_store::{redact, CredentialStore, KeychainCredentialStore};
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

/// P3-C (§7): the resolved Worker command an approval
/// authorizes. `argv_sha256` commits the exact argv (the
/// commit message is inside it, hashed, never plaintext);
/// `cwd` and `timeout_ms` are part of the binding too — the
/// run route recomputes all three from the resolved command
/// and rejects a presented spec that does not match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub argv_template: String,
    pub argv_sha256: String,
    pub cwd: String,
    pub timeout_ms: u64,
}

#[derive(Clone)]
pub(crate) struct ApprovalState {
    tool: String,
    status: ApprovalStatus,
    binding: Option<ApprovalBinding>,
    consumed: bool,
    operation_digest: Option<String>,
    /// Phase 2 (Q4): device action id this approval was consumed
    /// for, set at first device use. Never set on loop-bound or
    /// digest-bound records.
    action_binding: Option<String>,
    /// P0-C (contract §5): the Effect this approval authorizes,
    /// set when the Effect is planned (loop proposal, device
    /// action creation, digest-bound approval request) or at
    /// first consume for approvals planned elsewhere (task
    /// execute). The binding is `effect_id + input_digest`:
    /// the gate checks both, never the execution device and
    /// never the action record alone. None for legacy approvals
    /// issued before P0-C, which keep their old gate.
    effect_binding: Option<String>,
    message: Option<String>,
    /// P3-C (§7): the resolved Worker command this approval
    /// authorizes. None for every other tool.
    command: Option<CommandSpec>,
    /// E2-A §2.7: the worker session that consumed this
    /// approval to create a run. None for old rows and for
    /// approvals never used to create a run.
    consumed_by_worker_session_id: Option<String>,
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
        ApprovalState { tool, status, binding, consumed, operation_digest, action_binding: None, effect_binding: None, message: None, command: None, consumed_by_worker_session_id: None }
    }

    fn is_bound(&self) -> bool {
        self.binding.is_some() || self.operation_digest.is_some() || self.effect_binding.is_some()
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
    pub(crate) executions: Arc<Mutex<HashMap<String, ExecutionFileRecord>>>,
    next_execution_id: Arc<Mutex<u64>>,
    executions_path: Option<PathBuf>,
    pub(crate) effects: Arc<Mutex<HashMap<String, EffectFileRecord>>>,
    next_effect_id: Arc<Mutex<u64>>,
    effects_path: Option<PathBuf>,
    /// E2-C §4.5: the Evidence audit export path
    /// (evidence.jsonl). A derived audit copy only — Evidence
    /// lives in SQLite and has no in-memory map; the export
    /// route reads SQLite directly, and the file is regenerated
    /// at every Evidence write point.
    evidence_path: Option<PathBuf>,
    /// P0-D SQLite connection, when configured. `None` means
    /// pure JSONL behaviour. The in-memory maps above stay as a
    /// write-through cache in SQLite mode (every persist updates
    /// both); the migrated-domain list / get / history routes
    /// read SQLite, and a restart reloads the maps from SQLite,
    /// so SQLite is the authority and JSONL appends stop.
    pub(crate) sqlite: Option<Arc<Mutex<Connection>>>,
    sqlite_path: Option<PathBuf>,
    sqlite_backend: Arc<Mutex<StateBackend>>,
    sqlite_shadow_failures: Arc<Mutex<u64>>,
    sqlite_broken: Arc<Mutex<bool>>,
    /// P0-E: the WorkerSession this process created at startup
    /// (recovery scan) and stamps on new Executions. None in
    /// pure JSONL mode (no WorkerSession concept there) and
    /// before any SQLite-mode scan has run.
    pub(crate) current_worker_session: Arc<Mutex<Option<String>>>,
    /// P0-E: the recovery scan runs at most once per AppState
    /// instance, so the with_sqlite_path and import-and-switch
    /// call sites cannot scan twice (a second scan would end
    /// the worker the first scan just created).
    recovery_scanned: Arc<Mutex<bool>>,
    /// P0-E: the recovery scan failed. The migrated-domain
    /// surface is fail-closed (sqlite_broken is also set) and
    /// the production binary refuses to start (see main.rs).
    recovery_failed: Arc<Mutex<bool>>,
    /// Device this gateway instance records as the origin of new
    /// Execution and Effect rows (P0-A provenance). Resolved once
    /// at startup from MOCK_ORIGIN_DEVICE_ID, default "mac-local";
    /// never mixed into ids or idempotency keys.
    pub(crate) origin_device_id: String,
    pub(crate) task_drafts: Arc<Mutex<HashMap<String, String>>>,
    pub(crate) task_drafts_dir: Option<PathBuf>,
    sessions: Arc<Mutex<HashMap<String, SessionRecord>>>,
    next_session_id: Arc<Mutex<u64>>,
    sessions_path: Option<PathBuf>,
    actions: Arc<Mutex<HashMap<String, DeviceActionRecord>>>,
    next_action_id: Arc<Mutex<u64>>,
    actions_path: Option<PathBuf>,
    pub(crate) device_backend: Arc<dyn ReminderBackend>,
    device_reminder_pin: Option<String>,
    notes: Arc<Mutex<Vec<MockNote>>>,
    messages: Arc<Mutex<Vec<MockMessage>>>,
    next_note_id: Arc<Mutex<u64>>,
    next_message_id: Arc<Mutex<u64>>,
    pub(crate) connector_accounts: Arc<Mutex<HashMap<String, ConnectorAccount>>>,
    next_account_id: Arc<Mutex<u64>>,
    pub(crate) connector_connections: Arc<Mutex<HashMap<String, bool>>>,
    connector_state_path: Option<PathBuf>,
    pub(crate) vault_slots: Arc<Mutex<HashMap<String, CredentialSlot>>>,
    pub(crate) vault_catalog: Arc<Mutex<HashMap<String, CatalogEntry>>>,
    pub(crate) vault_custom: Arc<Mutex<HashMap<String, CustomConnectorDef>>>,
    vault_tickets: Arc<Mutex<HashMap<String, CaptureTicket>>>,
    next_ticket_id: Arc<Mutex<u64>>,
    vault_state_path: Option<PathBuf>,
    vault_secrets: Arc<dyn CredentialStore + Send + Sync>,
    policy_defaults: Arc<Mutex<Option<PolicyDefaults>>>,
    policy_overrides: Arc<Mutex<Option<PolicyOverrides>>>,
    policy_connectors: Arc<Mutex<HashMap<String, ConnectorPolicy>>>,
    policy_rules: Arc<Mutex<Option<Vec<NetworkRule>>>>,
    /// E2-B §3.4 (snapshot gate): network rules versions
    /// retained by their snapshot digest,
    /// `sha256(canonical_json(rules))`, so an Execution's
    /// frozen snapshot can resolve to the rules list that
    /// was live at its creation. Every set/load inserts the
    /// new version and keeps the old ones (policy changes
    /// are rare operator actions; the map grows by one entry
    /// per change). The initial empty list is present from
    /// construction, so an Execution created before any rule
    /// was ever set still resolves. A digest that does not
    /// resolve (a version predating this process that was
    /// never persisted) falls back to live rules at the call
    /// site — the gate degrades, it never refuses.
    policy_rule_versions: Arc<Mutex<HashMap<String, Vec<NetworkRule>>>>,
    policy_scopes: Arc<Mutex<HashMap<String, ScopeRecord>>>,
    policy_state_path: Option<PathBuf>,
    model_staged: Arc<Mutex<Option<StagedModel>>>,
    model_state_path: Option<PathBuf>,
    peer_peers: Arc<Mutex<HashMap<String, Peer>>>,
    peer_invites: Arc<Mutex<HashMap<String, PeerInvite>>>,
    peer_inbox: Arc<Mutex<HashMap<String, PeerInboxMessage>>>,
    peer_requests: Arc<Mutex<HashMap<String, PeerRequest>>>,
    peer_state_path: Option<PathBuf>,
    spaces: Arc<Mutex<HashMap<String, Space>>>,
    space_state_path: Option<PathBuf>,
    feed_units: Arc<Mutex<HashMap<String, FeedUnit>>>,
    feed_state_path: Option<PathBuf>,
    chat_seen: Arc<Mutex<HashMap<String, usize>>>,
    chat_theme: Arc<Mutex<String>>,
    chat_state_path: Option<PathBuf>,
    nodes: Arc<Mutex<HashMap<String, Node>>>,
    next_node_id: Arc<Mutex<u64>>,
    node_state_path: Option<PathBuf>,
    bindings: Arc<Mutex<HashMap<String, WorkspaceBinding>>>,
    next_workspace_id: Arc<Mutex<u64>>,
    binding_state_path: Option<PathBuf>,
    projects: Arc<Mutex<HashMap<String, Project>>>,
    next_project_id: Arc<Mutex<u64>>,
    project_state_path: Option<PathBuf>,
    // D1-1: goals live in SQLite only (no JSONL mirror, no
    // in-memory map); the counter is the id allocator,
    // seeded from the goals table on restart.
    next_goal_id: Arc<Mutex<u64>>,
    // D1-2: mandates live in SQLite only (same shape as
    // goals); the counter is the id allocator, seeded from
    // the mandates table on restart.
    next_mandate_id: Arc<Mutex<u64>>,
    locations: Arc<Mutex<HashMap<String, Location>>>,
    next_location_id: Arc<Mutex<u64>>,
    location_state_path: Option<PathBuf>,
    worktrees: Arc<Mutex<HashMap<String, Worktree>>>,
    next_worktree_id: Arc<Mutex<u64>>,
    worktree_state_path: Option<PathBuf>,
    stream_subscriptions: Arc<Mutex<HashMap<String, StreamSubscription>>>,
    next_subscription_id: Arc<Mutex<u64>>,
    next_stream_id: Arc<Mutex<u64>>,
    stream_pokes: Arc<Mutex<Vec<StreamPoke>>>,
    next_poke_order: Arc<Mutex<u64>>,
    stream_selections: Arc<Mutex<HashMap<String, SxsSelection>>>,
    stream_state_path: Option<PathBuf>,
    voice_custom: Arc<Mutex<HashMap<String, CustomVoice>>>,
    voice_active: Arc<Mutex<Option<String>>>,
    voice_pipeline: Arc<Mutex<Option<String>>>,
    voice_state_path: Option<PathBuf>,
    mail_source: Arc<dyn MailSource>,
    /// E2 slice: git binary resolved once at startup (PATH
    /// search + optional MOCK_GIT_PATH override, plan §3.4).
    /// None means Worker git commands fail closed with 503
    /// `git_unavailable`.
    pub(crate) git_path: Option<PathBuf>,
    /// P3-C: cargo binary resolved once at startup (PATH
    /// search + optional MOCK_CARGO_PATH override, §3.4).
    /// None means `cargo-test` runs fail closed with 503
    /// `cargo_unavailable`.
    pub(crate) cargo_path: Option<PathBuf>,
    /// E2 slice: run ids a cancel request has claimed. The
    /// driver checks this before spawning and after the
    /// child exits; the set is the cancel signal, the kill
    /// itself happens in the cancel route.
    worker_cancelled: Arc<Mutex<std::collections::HashSet<String>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::from_deepseek_config(DeepSeekConfig::disabled())
    }
}

/// P0-A origin device id resolution (contract §9): the gateway
/// stamps new Execution and Effect rows with the device it runs
/// on. An explicitly set, non-empty value wins; anything else
/// (unset or empty) falls back to "mac-local", the current
/// machine's standing value. The fallback is a value, not a
/// model constant: ids and idempotency keys never contain it.
pub fn resolve_origin_device_id(value: Option<String>) -> String {
    match value {
        Some(v) if !v.is_empty() => v,
        _ => "mac-local".to_string(),
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
        // Task 12 U4 fix: the staged model selection must be
        // merged into the config BEFORE the backends are
        // built, because with_deepseek_backends builds both
        // the chat client and the loop model from this one
        // config. Merging only state.deepseek afterwards
        // would leave chat and loops on different models.
        // The staged record is kept as the persisted
        // preference (not consumed), so a second restart
        // keeps the applied model; an explicit env value
        // still wins over staged on every restart.
        let mut config = config;
        let model_path = std::env::var("MOCK_MODEL_STATE_PATH").ok().map(PathBuf::from);
        if let Some(path) = &model_path {
            let staged = load_model_state(path);
            let env_model = std::env::var("DEEPSEEK_MODEL").ok().filter(|s| !s.is_empty());
            let env_base = std::env::var("DEEPSEEK_BASE_URL").ok().filter(|s| !s.is_empty());
            config = merge_staged_config(config, &staged, env_model, env_base);
        }
        let loops_path = std::env::var("MOCK_LOOPS_PATH").ok().map(PathBuf::from);
        let network_rules = state.policy_rules.clone();
        state = state.with_deepseek_backends(config, loops_path, Some(network_rules))
            .expect("invalid loop configuration or audit");
        if let Some(path) = model_path {
            state = state.with_model_state_path(path);
        }
        if let Ok(tasks_path) = std::env::var("MOCK_TASKS_PATH") {
            state = state.with_tasks_path(PathBuf::from(tasks_path));
        }
        if let Ok(drafts_dir) = std::env::var("MOCK_TASK_DRAFTS_PATH") {
            state = state.with_task_drafts_dir(PathBuf::from(drafts_dir));
        }
        if let Ok(executions_path) = std::env::var("MOCK_EXECUTIONS_PATH") {
            state = state.with_executions_path(PathBuf::from(executions_path));
        }
        if let Ok(effects_path) = std::env::var("MOCK_EFFECTS_PATH") {
            state = state.with_effects_path(PathBuf::from(effects_path));
        }
        // E2-C §4.5: the Evidence audit export path. (The
        // packaged-app shell does not wire this env yet — see
        // the with_evidence_path note — so the file is only
        // written when the variable is set; the export route
        // reads SQLite and does not depend on the file.)
        if let Ok(evidence_path) = std::env::var("MOCK_EVIDENCE_PATH") {
            state = state.with_evidence_path(PathBuf::from(evidence_path));
        }
        // P0-D: MOCK_SQLITE_PATH opens the unified-plane SQLite
        // database; when it opens in shadow mode the explicit
        // D4 switch (import + read-path switch) runs here, so a
        // production gateway with a SQLite path is SQLite-
        // authoritative. MOCK_STATE_BACKEND=jsonl is the loud
        // emergency diagnostic override: it skips SQLite
        // entirely and keeps the JSONL read path.
        if sqlite_store::backend_override_from_env(std::env::var("MOCK_STATE_BACKEND").ok().as_deref()) {
            eprintln!("P0-D WARNING: MOCK_STATE_BACKEND=jsonl override active: SQLite is disabled, JSONL read path in use for diagnosis only (non-steady-state, never a silent fallback)");
        } else if let Ok(sqlite_path) = std::env::var("MOCK_SQLITE_PATH") {
            state = state.with_sqlite_path(PathBuf::from(sqlite_path));
            if state.sqlite_backend_name() == "shadow" {
                match state.sqlite_import_and_switch() {
                    Ok(report) => eprintln!(
                        "P0-D: SQLite read path active (imported {} executions, {} effects, {} approvals)",
                        report.executions, report.effects, report.approvals
                    ),
                    Err(detail) => eprintln!(
                        "warning: P0-D SQLite import/switch failed ({detail}); staying on the JSONL read path in shadow mode"
                    ),
                }
            }
        }
        // P0-A provenance: the origin device id is a value, never
        // part of any id. Shell packaging keeps the default: no
        // gateway_home wiring is needed for the default to hold.
        state = state.with_origin_device_id(resolve_origin_device_id(
            std::env::var("MOCK_ORIGIN_DEVICE_ID").ok(),
        ));
        if let Ok(sessions_path) = std::env::var("MOCK_SESSIONS_PATH") {
            state = state.with_sessions_path(PathBuf::from(sessions_path));
        }
        if let Ok(actions_path) = std::env::var("MOCK_DEVICE_ACTIONS_PATH") {
            state = state.with_actions_path(PathBuf::from(actions_path));
        }
        if let Ok(connector_state_path) = std::env::var("MOCK_CONNECTOR_STATE_PATH") {
            state = state.with_connector_state_path(PathBuf::from(connector_state_path));
        }
        if let Ok(vault_state_path) = std::env::var("MOCK_VAULT_STATE_PATH") {
            state = state.with_vault_state_path(PathBuf::from(vault_state_path));
        }
        if let Ok(policy_state_path) = std::env::var("MOCK_POLICY_STATE_PATH") {
            state = state.with_policy_state_path(PathBuf::from(policy_state_path));
        }
        if let Ok(peer_state_path) = std::env::var("MOCK_PEER_STATE_PATH") {
            state = state.with_peer_state_path(PathBuf::from(peer_state_path));
        }
        if let Ok(space_state_path) = std::env::var("MOCK_SPACE_STATE_PATH") {
            state = state.with_space_state_path(PathBuf::from(space_state_path));
        }
        if let Ok(feed_state_path) = std::env::var("MOCK_FEED_STATE_PATH") {
            state = state.with_feed_state_path(PathBuf::from(feed_state_path));
        }
        if let Ok(chat_state_path) = std::env::var("MOCK_CHAT_STATE_PATH") {
            state = state.with_chat_state_path(PathBuf::from(chat_state_path));
        }
        if let Ok(node_state_path) = std::env::var("MOCK_NODE_STATE_PATH") {
            state = state.with_node_state_path(PathBuf::from(node_state_path));
        }
        if let Ok(stream_state_path) = std::env::var("MOCK_STREAM_STATE_PATH") {
            state = state.with_stream_state_path(PathBuf::from(stream_state_path));
        }
        if let Ok(voice_state_path) = std::env::var("MOCK_VOICE_STATE_PATH") {
            state = state.with_voice_state_path(PathBuf::from(voice_state_path));
        }
        // P3-A: engineering record stores. Production state
        // reads MOCK_*_STATE_PATH; the shell wires these from
        // the fixed record directory (gateway_home.rs).
        if let Ok(binding_state_path) = std::env::var("MOCK_BINDING_STATE_PATH") {
            state = state.with_binding_state_path(PathBuf::from(binding_state_path));
        }
        if let Ok(project_state_path) = std::env::var("MOCK_PROJECT_STATE_PATH") {
            state = state.with_project_state_path(PathBuf::from(project_state_path));
        }
        if let Ok(location_state_path) = std::env::var("MOCK_LOCATION_STATE_PATH") {
            state = state.with_location_state_path(PathBuf::from(location_state_path));
        }
        if let Ok(worktree_state_path) = std::env::var("MOCK_WORKTREE_STATE_PATH") {
            state = state.with_worktree_state_path(PathBuf::from(worktree_state_path));
        }
        // Task 22: on macOS the production mail source asks
        // Mail.app via AppleScript; every other platform
        // keeps the Unavailable source (501, never fake
        // data). Tests override via with_mail_source.
        #[cfg(target_os = "macos")]
        {
            state = state.with_mail_source(Arc::new(crate::mail_source::AppleScriptMailSource::new()));
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
                            ApprovalState { tool: record.tool.clone(), status, binding: record.binding.clone(), consumed: record.consumed, operation_digest: record.operation_digest.clone(), action_binding: record.action_binding.clone(), effect_binding: record.effect_binding.clone(), message: record.message.clone(), command: record.command.clone(), consumed_by_worker_session_id: record.consumed_by_worker_session_id.clone() },
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
            executions: Arc::new(Mutex::new(HashMap::new())),
            next_execution_id: Arc::new(Mutex::new(1)),
            executions_path: None,
            effects: Arc::new(Mutex::new(HashMap::new())),
            next_effect_id: Arc::new(Mutex::new(1)),
            effects_path: None,
            evidence_path: None,
            sqlite: None,
            sqlite_path: None,
            sqlite_backend: Arc::new(Mutex::new(StateBackend::Jsonl)),
            sqlite_shadow_failures: Arc::new(Mutex::new(0)),
            sqlite_broken: Arc::new(Mutex::new(false)),
            current_worker_session: Arc::new(Mutex::new(None)),
            recovery_scanned: Arc::new(Mutex::new(false)),
            recovery_failed: Arc::new(Mutex::new(false)),
            origin_device_id: "mac-local".to_string(),
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
            connector_accounts: Arc::new(Mutex::new(HashMap::new())),
            next_account_id: Arc::new(Mutex::new(1)),
            connector_connections: Arc::new(Mutex::new(HashMap::new())),
            connector_state_path: None,
            vault_slots: Arc::new(Mutex::new(HashMap::new())),
            vault_catalog: Arc::new(Mutex::new(HashMap::new())),
            vault_custom: Arc::new(Mutex::new(HashMap::new())),
            vault_tickets: Arc::new(Mutex::new(HashMap::new())),
            next_ticket_id: Arc::new(Mutex::new(1)),
            vault_state_path: None,
            vault_secrets: Arc::new(KeychainCredentialStore::new()),
            policy_defaults: Arc::new(Mutex::new(None)),
            policy_overrides: Arc::new(Mutex::new(None)),
            policy_connectors: Arc::new(Mutex::new(HashMap::new())),
            policy_rules: Arc::new(Mutex::new(None)),
            policy_rule_versions: Arc::new(Mutex::new(HashMap::from([(
                crate::policy_store::network_rules_digest(&[]),
                Vec::new(),
            )]))),
            policy_scopes: Arc::new(Mutex::new(HashMap::new())),
            policy_state_path: None,
            model_staged: Arc::new(Mutex::new(None)),
            model_state_path: None,
            peer_peers: Arc::new(Mutex::new(HashMap::new())),
            peer_invites: Arc::new(Mutex::new(HashMap::new())),
            peer_inbox: Arc::new(Mutex::new(HashMap::new())),
            peer_requests: Arc::new(Mutex::new(HashMap::new())),
            peer_state_path: None,
            spaces: Arc::new(Mutex::new(HashMap::new())),
            space_state_path: None,
            feed_units: Arc::new(Mutex::new(
                seed_feed_units().into_iter().map(|u| (u.unit_id.clone(), u)).collect(),
            )),
            feed_state_path: None,
            chat_seen: Arc::new(Mutex::new(HashMap::new())),
            chat_theme: Arc::new(Mutex::new(DEFAULT_CHAT_THEME.to_string())),
            chat_state_path: None,
            nodes: Arc::new(Mutex::new(HashMap::new())),
            next_node_id: Arc::new(Mutex::new(1)),
            node_state_path: None,
            bindings: Arc::new(Mutex::new(HashMap::new())),
            next_workspace_id: Arc::new(Mutex::new(1)),
            binding_state_path: None,
            projects: Arc::new(Mutex::new(HashMap::new())),
            next_project_id: Arc::new(Mutex::new(1)),
            next_goal_id: Arc::new(Mutex::new(1)),
            next_mandate_id: Arc::new(Mutex::new(1)),
            project_state_path: None,
            locations: Arc::new(Mutex::new(HashMap::new())),
            next_location_id: Arc::new(Mutex::new(1)),
            location_state_path: None,
            worktrees: Arc::new(Mutex::new(HashMap::new())),
            next_worktree_id: Arc::new(Mutex::new(1)),
            worktree_state_path: None,
            stream_subscriptions: Arc::new(Mutex::new(HashMap::new())),
            next_subscription_id: Arc::new(Mutex::new(1)),
            next_stream_id: Arc::new(Mutex::new(1)),
            stream_pokes: Arc::new(Mutex::new(Vec::new())),
            next_poke_order: Arc::new(Mutex::new(1)),
            stream_selections: Arc::new(Mutex::new(HashMap::new())),
            stream_state_path: None,
            voice_custom: Arc::new(Mutex::new(
                crate::voice_store::custom_voice_seeds()
                    .into_iter()
                    .map(|v| (v.saved_voice_id.clone(), v))
                    .collect(),
            )),
            voice_active: Arc::new(Mutex::new(None)),
            voice_pipeline: Arc::new(Mutex::new(None)),
            voice_state_path: None,
            mail_source: Arc::new(UnavailableMailSource),
            git_path: crate::worker::resolve_git_path(),
            cargo_path: crate::worker::resolve_cargo_path(),
            worker_cancelled: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// 注入模型客户端；基础构造器不读取凭据，正常启动在 main 中加载 env 与 Keychain。
    pub fn with_deepseek(mut self, client: DeepSeekClient) -> Self {
        self.deepseek = client;
        self
    }

    /// 聊天与循环共用配置；缺少 key 时聊天回显、循环不可用。
    /// Task 13: `network_rules` is the AppState policy_rules
    /// Arc (or None in tests). It is shared, not copied, so
    /// rule edits apply to in-flight clients live. Callers
    /// must pass the Arc that later loads will fill in
    /// place (see with_policy_state_path), never a copy.
    pub fn with_deepseek_backends(
        self,
        config: DeepSeekConfig,
        path: Option<PathBuf>,
        network_rules: Option<Arc<Mutex<Option<Vec<NetworkRule>>>>>,
    ) -> Result<Self, String> {
        let model = crate::deepseek_loop::DeepSeekLoopModel::new(config.clone())?
            .map(|m| {
                let m = match &network_rules {
                    Some(rules) => m.with_network_rules(rules.clone()),
                    None => m,
                };
                Arc::new(m) as Arc<dyn LoopModel>
            });
        let timeouts = self.loop_fault.timeouts();
        let deepseek = match &network_rules {
            Some(rules) => DeepSeekClient::new(config).with_network_rules(rules.clone()),
            None => DeepSeekClient::new(config),
        };
        self.with_deepseek(deepseek)
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
        digest: String,
        command: Option<CommandSpec>,
    ) -> Result<String, LoopError> {
        let mut next = self.next_approval_id.lock().unwrap();
        let id = format!("approval-{}", *next);
        *next += 1;
        let record = ApprovalState {
            tool: binding.tool.clone(),
            status: ApprovalStatus::Pending,
            binding: Some(binding),
            consumed: false,
            // Phase 2 (Q5): the loop approval carries the unified
            // digest from creation, so the gate no longer depends
            // on the binding's wire field for parameter binding.
            operation_digest: Some(digest),
            action_binding: None,
            effect_binding: None,
            message: None,
            // P3-C: a worker.run approval also carries the
            // resolved command spec (argv digest, cwd, timeout).
            command,
            // E2-A §2.7: no consumer yet at plan time.
            consumed_by_worker_session_id: None,
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
        // Phase 2 (Q5): the reference triple identifies the call;
        // the digest is checked at consume/execute time, not here.
        if record
            .binding
            .as_ref()
            .is_none_or(|b| !crate::loop_engine::binding_ref_matches(b, binding))
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
        digest: &str,
    ) -> Result<(), LoopError> {
        let mut approvals = self.approvals.lock().unwrap();
        let record = approvals
            .get_mut(id)
            .ok_or(LoopError(403, "approval_required"))?;
        // Phase 2 (Q5): reference triple + unified digest, both
        // under the same lock as consumption, so a concurrent
        // execution of one approval still serializes and only the
        // first consumes it. The wire args_sha256 is not consulted.
        if record
            .binding
            .as_ref()
            .is_none_or(|b| !crate::loop_engine::binding_ref_matches(b, binding))
            || record.status != ApprovalStatus::Approved
            || record.consumed
            || record.operation_digest.as_deref() != Some(digest)
        {
            return Err(LoopError(403, "approval_binding_mismatch"));
        }
        // P0-C (contract §5): when the approval is bound to an
        // Effect, the Effect must exist, carry the same digest,
        // and sit at `planned` (or already `authorized`, which
        // a retried consume after a crash may meet). The check
        // runs before consumption, so a mismatched Effect never
        // burns the approval; legacy approvals without an
        // effect_binding keep the old gate above, unchanged.
        let effect_id = record.effect_binding.clone();
        if let Some(effect_id) = &effect_id {
            let effect = effect_by_id(self, effect_id);
            match effect {
                Some(effect)
                    if effect.input_digest.as_deref() == Some(digest)
                        && (effect.status == "planned" || effect.status == "authorized") => {}
                _ => return Err(LoopError(403, "approval_binding_mismatch")),
            }
        }
        record.consumed = true;
        persist_approval(self, id, record).map_err(|_| LoopError(500, "approval_audit_failed"))?;
        // Authorization lands the Effect at `authorized`. The
        // persist above already failed closed on error, so this
        // transition only runs for a consumed approval; an
        // already-authorized Effect is left alone.
        if let Some(effect_id) = &effect_id {
            if effect_by_id(self, effect_id).is_some_and(|e| e.status == "planned") {
                let _ = transition_effect(self, effect_id, "authorized", None, None);
            }
        }
        Ok(())
    }

    /// P0-C: drive a bound loop Effect to its terminal state.
    /// Returns true when a bound Effect was found and driven
    /// (an `authorized` Effect takes the compressed
    /// `authorized -> started -> terminal` chain). False means
    /// the caller must fall back to recording a terminal row.
    fn transition_bound_loop_effect(&self, approval_id: &str, terminal: &str) -> bool {
        let Some(effect) = effect_for_approval(self, approval_id, None) else {
            return false;
        };
        if effect.status == "authorized" {
            let _ = transition_effect(self, &effect.effect_id, "started", None, None);
        }
        transition_effect(self, &effect.effect_id, terminal, None, None).is_ok()
    }

    /// P0-C loop-call Effect: drives the planned Effect created
    /// at proposal time through its terminal transition. Only
    /// the two approval-gated tools produce effects; reads never
    /// do, and neither do gate failures or pre-dispatch refusals
    /// (nothing was attempted). The outcome's CallStatus maps
    /// by contract §4: `succeeded`/`failed`/`unknown` are already
    /// lifecycle words; `Running` never reaches here post-
    /// dispatch. When no planned Effect exists (a legacy approval
    /// from before P0-C, or a dispatch without a proposal-time
    /// plan), the old behaviour applies: one terminal row is
    /// recorded directly, in the new word.
    fn record_loop_effect(
        &self,
        tool: &str,
        args: &std::collections::BTreeMap<String, String>,
        approval_id: &Option<String>,
        binding: &Option<ApprovalBinding>,
        outcome: &ToolOutcome,
    ) {
        let (capability, operation, scope) = match tool {
            "fs.write" => (
                "fs",
                "write",
                args.get("path").cloned().unwrap_or_default(),
            ),
            // E2-C unification: loop device writes use the
            // contract triple (device, reminders.set_completed),
            // the same identity the direct route records.
            "reminders.set_completed" => (
                "device",
                "reminders.set_completed",
                args.get("id").cloned().unwrap_or_default(),
            ),
            _ => return,
        };
        let status = serde_json::to_string(&outcome.status)
            .unwrap_or_else(|_| "\"unknown\"".to_string());
        let status = status.trim_matches('"');
        let terminal = crate::effect_store::map_legacy_status_to_new(status)
            .unwrap_or("failed");
        if let Some(approval_id) = approval_id {
            if self.transition_bound_loop_effect(approval_id, terminal) {
                sync_patch_file_after_loop_write(self, approval_id, terminal);
                return;
            }
        }
        // E2-C §4.2: the unbound fallback (no bound Effect for
        // this approval — a legacy dispatch, or a proposal whose
        // plan step recorded nothing) also goes through the
        // shared ledger entry: plan + gate, then the terminal
        // transition. A Replay/Blocked answer means an
        // identical row already covers this write — a duplicate
        // proposal of the same tool call — so nothing more is
        // recorded (a second row would collide on the UNIQUE
        // idempotency key and mark the store broken).
        let execution_id =
            binding.as_ref().and_then(|b| execution_for_loop(self, &b.loop_id));
        if let LedgerEntryDecision::Entered(entry) = ledger_entry(
            self,
            execution_id,
            capability,
            operation,
            &scope,
            Some(crate::loop_engine::args_digest(tool, args)),
            approval_id.clone(),
        ) {
            let outcome = match terminal {
                "succeeded" => LedgerOutcome::Succeeded,
                "failed" => LedgerOutcome::Failed,
                _ => LedgerOutcome::Unknown,
            };
            let _ = ledger_entry_finish(self, &entry.effect.effect_id, outcome);
        }
    }

    /// P3-C: the digest the approval gate checks for a loop
    /// tool call. worker.run's gate digest is the command
    /// digest (argv + cwd + timeout, resolved with AppState);
    /// every other tool uses the pure args digest. None when
    /// the command cannot be resolved (the gate fails closed).
    fn loop_gate_digest(
        &self,
        tool: &str,
        args: &std::collections::BTreeMap<String, String>,
    ) -> Option<String> {
        if tool == "worker.run" {
            return worker_command_digest(self, args)
                .ok()
                .map(|(digest, _)| digest);
        }
        Some(crate::loop_engine::args_digest(tool, args))
    }

    /// 循环与 POST /agent/tool/call 共用服务端分派。绑定凭据不能从公共端点重放。
    ///
    /// E2-D §6 (`worker.wait`): default wait budget 10 s
    /// (the loop tool timeout caps it — normalize rejects
    /// anything above 10 s), polled every 500 ms against
    /// the run row. A default-budget wait can be cut off by
    /// the engine's 10 s tool timeout at the boundary; the
    /// loop then sees a timed-out tool call and may wait
    /// again next round.
    pub(crate) async fn execute_loop_tool(
        &self,
        tool: String,
        args: std::collections::BTreeMap<String, String>,
        approval_id: Option<String>,
        binding: Option<ApprovalBinding>,
        backend: Option<Arc<dyn LoopTools>>,
    ) -> ToolOutcome {
        if loop_tool_requires_approval(&tool) {
            let Some(digest) = self.loop_gate_digest(&tool, &args) else {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            };
            let valid = {
                let approvals = self.approvals.lock().unwrap();
                approval_id
                    .as_ref()
                    .and_then(|id| approvals.get(id))
                    .is_some_and(|record| {
                        // Phase 2 (Q5): reference triple + unified
                        // digest, never the binding's wire field.
                        // The digest is recomputed from the exact
                        // args about to be dispatched.
                        record.status == ApprovalStatus::Approved
                            && record.consumed
                            && record
                                .binding
                                .as_ref()
                                .is_some_and(|b| binding
                                    .as_ref()
                                    .is_some_and(|given| crate::loop_engine::binding_ref_matches(b, given)))
                            && record.operation_digest.as_deref()
                                == Some(digest.as_str())
                    })
            };
            if !valid {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            }
            // P0-C: the bound Effect gates dispatch too. A
            // terminal Effect means this write already happened:
            // replay its outcome, never dispatch twice. An
            // `unknown` or `reconciling` Effect means the outcome
            // is unsettled: report unknown, never dispatch twice.
            // A `started` Effect means a dispatch is already in
            // flight: refuse. Otherwise drive `authorized ->
            // started` now; the outcome transition happens in
            // record_loop_effect after dispatch.
            let digest = crate::loop_engine::args_digest(&tool, &args);
            if let Some(approval_id) = &approval_id {
                if let Some(effect) = effect_for_approval(self, approval_id, Some(&digest)) {
                    match effect.status.as_str() {
                        "succeeded" => {
                            return ToolOutcome::success(
                                "replayed from existing effect".to_string(),
                            );
                        }
                        "failed" => {
                            return ToolOutcome::failed(ErrorCode::ToolFailed);
                        }
                        "reconciled" => {
                            return match effect
                                .reconciliation
                                .as_ref()
                                .map(|r| r.result.as_str())
                            {
                                Some("completed") => ToolOutcome::success(
                                    "replayed from reconciled effect".to_string(),
                                ),
                                Some("failed") => {
                                    ToolOutcome::failed(ErrorCode::ToolFailed)
                                }
                                _ => ToolOutcome::unknown(),
                            };
                        }
                        "unknown" | "reconciling" => return ToolOutcome::unknown(),
                        "started" => {
                            return ToolOutcome::failed(ErrorCode::InvalidArguments);
                        }
                        "authorized" => {
                            let _ = transition_effect(
                                self,
                                &effect.effect_id,
                                "started",
                                None,
                                None,
                            );
                        }
                        "planned" => {
                            let _ = transition_effect_chain(
                                self,
                                &effect.effect_id,
                                &["authorized", "started"],
                            );
                        }
                        _ => {
                            return ToolOutcome::failed(ErrorCode::InvalidArguments);
                        }
                    }
                }
            }
            // Defensive idempotency check for a *different*
            // Effect with the same key in this execution (the
            // bound Effect itself is handled above).
            if let (Some(binding), Some(approval_id)) = (&binding, &approval_id) {
                let triple = match tool.as_str() {
                    "fs.write" => Some((
                        "fs",
                        "write",
                        args.get("path").cloned().unwrap_or_default(),
                    )),
                    // E2-C unification: same contract triple as
                    // plan_loop_effect / record_loop_effect.
                    "reminders.set_completed" => Some((
                        "device",
                        "reminders.set_completed",
                        args.get("id").cloned().unwrap_or_default(),
                    )),
                    // P3-C: worker.run has no planned Effect
                    // (digest-only approval); the run's own
                    // idempotency key + the approval single-use
                    // + execution_has_running_worker are the
                    // dedup gates. Skipping the check is not a
                    // hole: a replayed approval cannot create a
                    // second run (UNIQUE index on approval_id).
                    "worker.run" => None,
                    _ => return ToolOutcome::failed(ErrorCode::InvalidArguments),
                };
                if let Some((capability, operation, scope)) = triple {
                let bound_id = effect_for_approval(self, approval_id, Some(&digest))
                    .map(|e| e.effect_id);
                match check_idempotency(
                    self,
                    execution_for_loop(self, &binding.loop_id).as_deref(),
                    capability,
                    operation,
                    &scope,
                    Some(&digest),
                    bound_id.as_deref(),
                ) {
                    IdempotencyDecision::Proceed => {}
                    IdempotencyDecision::Blocked(_) => return ToolOutcome::unknown(),
                    // P0-D: broken migrated-domain store refuses
                    // the loop write rather than guessing.
                    IdempotencyDecision::Unavailable => return ToolOutcome::unknown(),
                    IdempotencyDecision::Replay(other) => {
                        return match other.status.as_str() {
                            "succeeded" => ToolOutcome::success(
                                "replayed from existing effect".to_string(),
                            ),
                            "failed" => ToolOutcome::failed(ErrorCode::ToolFailed),
                            "reconciled" => match other
                                .reconciliation
                                .as_ref()
                                .map(|r| r.result.as_str())
                            {
                                Some("completed") => ToolOutcome::success(
                                    "replayed from reconciled effect".to_string(),
                                ),
                                Some("failed") => {
                                    ToolOutcome::failed(ErrorCode::ToolFailed)
                                }
                                _ => ToolOutcome::unknown(),
                            },
                            _ => ToolOutcome::unknown(),
                        };
                    }
                }
                }
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
            // Every arm below is post-dispatch: the backend was
            // invoked and reported a terminal outcome, so each one
            // mirrors into an Effect (including Unknown and
            // AwaitingAuthorization, which are terminal reports,
            // not refusals to attempt).
            // E2-C §4.2: one Device budget only — the loop path
            // used 8s while the direct route used
            // DEVICE_TIMEOUT_SECS (10s); both now use the
            // constant, so a slow backend reports the same
            // outcome on either path.
            let outcome = match tokio::task::spawn_blocking(move || {
                execute_set(
                    &device_backend,
                    &id,
                    completed,
                    Duration::from_secs(DEVICE_TIMEOUT_SECS),
                )
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
            self.record_loop_effect(&tool, &args, &approval_id, &binding, &outcome);
            return outcome;
        }
        // P2: read-only mail tools. They mirror reminders.list, not
        // fs.read: the source is AppState.mail_source (the same
        // MailSource the /api/email/* routes use, never the mock
        // connector list), and the call runs on a blocking thread
        // because AppleScript is synchronous. Reads produce no
        // Effect and need no approval (registered Some(false)).
        // Deliberately not added to dispatch_agent_tool: like
        // reminders.*, these are loop-only tools, and the file
        // fallthrough below indexes args["path"], which mail args
        // do not carry.
        if tool == "email.threads" {
            let limit = args
                .get("limit")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(50);
            let offset = args
                .get("offset")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            let source = Arc::clone(&self.mail_source);
            return match tokio::task::spawn_blocking(move || source.threads(limit, offset))
                .await
            {
                Ok(Ok(threads)) => {
                    let mut out = String::new();
                    for t in &threads {
                        out.push_str(&format!(
                            "thread: {} | subject: {} | messages: {} | snippet: {}\n",
                            t.thread_id,
                            t.subject,
                            t.message_ids.join(", "),
                            t.snippet,
                        ));
                    }
                    if out.is_empty() {
                        out.push_str("（收件箱暂无线程。）\n");
                    }
                    ToolOutcome::success(email_rendered(out))
                }
                Ok(Err(err)) => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: email_error_text(err),
                },
                Err(_) => ToolOutcome::unknown(),
            };
        }
        if tool == "email.message" {
            let Some(id) = args.get("id").cloned() else {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            };
            let source = Arc::clone(&self.mail_source);
            return match tokio::task::spawn_blocking(move || source.message(&id)).await {
                Ok(Ok(message)) => {
                    // MessageView carries no thread id; derive it
                    // with the same functions the routes use, so the
                    // citation anchor matches the thread listing.
                    let thread_id = crate::mail_source::thread_id_for(
                        &crate::mail_source::normalize_subject(&message.subject),
                    );
                    let attachments = if message.attachments.is_empty() {
                        "none".to_string()
                    } else {
                        message
                            .attachments
                            .iter()
                            .map(|a| format!("{} ({})", a.name, a.index))
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    let out = format!(
                        "message: {}\nthread: {}\nsubject: {}\nfrom: {}\nto: {}\ndate: {}\nattachments: {}\nbody:\n{}",
                        message.message_id,
                        thread_id,
                        message.subject,
                        message.from,
                        message.to.join(", "),
                        message.date,
                        attachments,
                        message.body_text,
                    );
                    ToolOutcome::success(email_rendered(out))
                }
                Ok(Err(err)) => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: email_error_text(err),
                },
                Err(_) => ToolOutcome::unknown(),
            };
        }
        // P3-A: read-only code search. Deliberately not added
        // to dispatch_agent_tool: like the mail tools, this is
        // a loop-only tool and the file fallthrough below
        // indexes args["path"], which code.search args do not
        // carry. Zero Effect; results go only to the model
        // context — the query and the hits are never logged.
        if tool == "code.search" {
            let query = args.get("query").cloned().unwrap_or_default();
            let worktree_id = args
                .get("worktree_id")
                .filter(|id| !id.is_empty())
                .map(String::as_str);
            let path = args.get("path").cloned().unwrap_or_default();
            let max_results = args
                .get("max_results")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(crate::code_search::CODE_SEARCH_MAX_RESULTS);
            if query.trim().is_empty() {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            }
            let root = match resolve_code_search_root(self, worktree_id, binding.as_ref()) {
                Ok(root) => root,
                Err(_) => return ToolOutcome::failed(ErrorCode::InvalidArguments),
            };
            let outcome = tokio::task::spawn_blocking(move || {
                crate::code_search::search_code(&root, &query, &path, max_results)
            })
            .await;
            return match outcome {
                Ok(Ok(found)) => ToolOutcome::success(
                    serde_json::to_string(&found).unwrap_or_else(|_| {
                        "{\"results\":[],\"truncated\":false}".to_string()
                    }),
                ),
                Ok(Err(_)) => ToolOutcome::failed(ErrorCode::InvalidArguments),
                Err(_) => ToolOutcome::unknown(),
            };
        }
        if tool == "git.read" {
            // P3-C (§6): read-only git status/diff. No Effect,
            // no approval. The root resolves like code.search
            // (explicit worktree wins, else the loop's binding,
            // else files_root); a root that is not a git
            // worktree fails the command (ToolFailed), never
            // invents output. Synchronous, 10s budget, 4096 B
            // truncation with a flag.
            let operation = args.get("operation").cloned().unwrap_or_default();
            let root = match resolve_code_search_root(
                self,
                args.get("worktree_id")
                    .filter(|id| !id.is_empty())
                    .map(String::as_str),
                binding.as_ref(),
            ) {
                Ok(root) => root,
                Err(_) => return ToolOutcome::failed(ErrorCode::InvalidArguments),
            };
            let command = if operation == "diff" {
                "git-diff"
            } else {
                "git-status"
            };
            let binary = match worker_binary_for_command(self, command) {
                Ok(path) => path,
                Err(_) => return ToolOutcome::failed(ErrorCode::ToolFailed),
            };
            let params = match args.get("path") {
                Some(p) if !p.is_empty() => Some(serde_json::json!({ "path": p })),
                _ => None,
            };
            let resolved = match crate::worker::resolve_command(
                command,
                params.as_ref(),
                &binary,
                &root,
                Some(10_000),
                None,
            ) {
                Ok(resolved) => resolved,
                Err(_) => return ToolOutcome::failed(ErrorCode::InvalidArguments),
            };
            let state = self.clone();
            let argv = resolved.argv.clone();
            let output = tokio::task::spawn_blocking(move || {
                crate::worker::run_command_sync(
                    &state,
                    &argv,
                    &root,
                    &binary,
                    std::time::Duration::from_millis(10_000),
                )
            })
            .await;
            let output = match output {
                Ok(output) => output,
                Err(_) => return ToolOutcome::unknown(),
            };
            if output.timed_out {
                return ToolOutcome::unknown();
            }
            if output.exit_code != Some(0) {
                // Not a git worktree, or git failed: the tool
                // fails, never invents output.
                return ToolOutcome::failed(ErrorCode::ToolFailed);
            }
            let mut output_text = output.stdout.clone();
            let mut truncated = false;
            if output_text.len() > 4096 {
                let mut end = 4096;
                while !output_text.is_char_boundary(end) {
                    end -= 1;
                }
                output_text.truncate(end);
                truncated = true;
            }
            return ToolOutcome::success(
                serde_json::to_string(&serde_json::json!({
                    "command": command,
                    "output": output_text,
                    "truncated": truncated,
                    "exit_code": 0,
                }))
                .unwrap_or_default(),
            );
        }
        if tool == "worker.run" {
            // P3-C (§6): enqueue-only. The approval was already
            // enforced by the gate above (loop-bound approvals
            // were consumed by the loop; the run row's UNIQUE
            // index on approval_id makes the enqueue single-use).
            // The run completes asynchronously; the Effect is
            // recorded by the driver at completion, never here.
            let dto = WorkerRunCreateDto {
                worktree_id: args.get("worktree_id").cloned().unwrap_or_default(),
                command: args.get("command").cloned().unwrap_or_default(),
                params: Some(worker_run_params_from_args(&args)),
                timeout_ms: args.get("timeout_ms").and_then(|s| s.parse::<u64>().ok()),
                execution_id: binding
                    .as_ref()
                    .and_then(|b| execution_for_loop(self, &b.loop_id)),
                approval_id: approval_id.clone(),
            };
            return match create_worker_run_internal(self, dto).await {
                Ok(run) => ToolOutcome::success(
                    serde_json::to_string(&serde_json::json!({
                        "run_id": run.run_id,
                        "status": "queued",
                        "execution_id": run.execution_id,
                    }))
                    .unwrap_or_default(),
                ),
                Err(err) => match err.status {
                    StatusCode::BAD_REQUEST => {
                        ToolOutcome::failed(ErrorCode::InvalidArguments)
                    }
                    StatusCode::NOT_FOUND
                    | StatusCode::CONFLICT
                    | StatusCode::SERVICE_UNAVAILABLE => {
                        ToolOutcome::failed(ErrorCode::ToolFailed)
                    }
                    _ => ToolOutcome::unknown(),
                },
            };
        }
        if tool == "worker.wait" {
            // E2-D (§6): poll-only wait for a run. No Effect,
            // no overwrite, no dispatch: the arm only reads the
            // run row (the same read path as `worker_run_get`)
            // until the run reaches a terminal status or the
            // budget runs out. Terminal returns the run's
            // terminal status + effect_id (null for read runs,
            // which record no Effect); budget-exhausted returns
            // "still running" + the run's current progress_json
            // (null before the driver's first 5 s write). The
            // loop-tool timeout (10 s) bounds the wait from
            // outside; the default budget is that same 10 s, so
            // a default wait can be cut off by the engine's
            // tool timeout at the boundary — the loop then sees
            // a timed-out tool call and may wait again next
            // round. Unknown run_id maps to ToolFailed: loop
            // tools carry no HTTP status, so the plan's "404"
            // is the HTTP-route analog (the same mapping the
            // worker.run arm uses for NOT_FOUND).
            let run_id = args.get("run_id").cloned().unwrap_or_default();
            let budget_ms = args
                .get("timeout_ms")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(WORKER_WAIT_DEFAULT_TIMEOUT_MS);
            let deadline = std::time::Instant::now() + Duration::from_millis(budget_ms);
            loop {
                let row = {
                    let Some(sqlite) = &self.sqlite else {
                        return ToolOutcome::unknown();
                    };
                    if self.sqlite_is_broken() {
                        return ToolOutcome::unknown();
                    }
                    let conn = sqlite.lock().expect("sqlite lock");
                    match sqlite_store::get_worker_run(&conn, &run_id) {
                        Ok(row) => row,
                        Err(_) => return ToolOutcome::unknown(),
                    }
                };
                let Some(row) = row else {
                    return ToolOutcome::failed(ErrorCode::ToolFailed);
                };
                if crate::worker::is_terminal_run_status(&row.status) {
                    return ToolOutcome::success(
                        serde_json::to_string(&serde_json::json!({
                            "effect_id": row.effect_id,
                            "run_id": row.run_id,
                            "status": row.status,
                        }))
                        .unwrap_or_default(),
                    );
                }
                if std::time::Instant::now() >= deadline {
                    let progress: serde_json::Value = row
                        .progress_json
                        .as_deref()
                        .and_then(|text| serde_json::from_str(text).ok())
                        .unwrap_or(serde_json::Value::Null);
                    return ToolOutcome::success(
                        serde_json::to_string(&serde_json::json!({
                            "progress": progress,
                            "run_id": row.run_id,
                            "status": "running",
                        }))
                        .unwrap_or_default(),
                    );
                }
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                tokio::time::sleep(remaining.min(WORKER_WAIT_POLL_INTERVAL)).await;
            }
        }
        if let Some(backend) = backend {
            // P3-B (§10-8a): the SAME three-way stale check as
            // the apply command, for a tool backend that
            // executes fs.write. The production backend is
            // None (the fallthrough below dispatches), but a
            // custom backend must not bypass the check either.
            if tool == "fs.write" {
                if let Some(outcome) = check_loop_patch_write(self, &approval_id, &|outcome| {
                    self.record_loop_effect(&tool, &args, &approval_id, &binding, outcome)
                }) {
                    return outcome;
                }
            }
            let outcome = backend.execute(tool.clone(), args.clone()).await;
            if tool == "fs.write" {
                self.record_loop_effect(&tool, &args, &approval_id, &binding, &outcome);
            }
            return outcome;
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
        // Post-dispatch Effect context for fs.write, cloned before
        // args/approval_id move into the blocking task. Reads never
        // produce an Effect, so the context exists only for writes.
        // E2-A §2.3: the single record path is record_loop_effect
        // (the old inline dispatch closure is deleted — A12 locks
        // the identical output). The digest, scope, approval and
        // binding all come from these clones, exactly as the
        // closure computed them from the same args.
        let effect_args = (tool == "fs.write").then(|| args.clone());
        let effect_approval = approval_id.clone();
        let effect_binding = binding.clone();
        let record_fs_effect = |outcome: &ToolOutcome| {
            if let Some(args) = &effect_args {
                // `effect_args` exists only for fs.write, so the
                // tool word is the literal, not the borrowed `tool`
                // (the spawn below moves it into the dispatch).
                self.record_loop_effect("fs.write", args, &effect_approval, &effect_binding, outcome);
            }
        };
        // P3-B (§10-8a): the loop entry calls the SAME
        // three-way stale check as the apply command, before
        // the fs.write dispatch. A drifted proposal fails
        // here (patch_stale), the PatchFile goes stale in the
        // same tx, and the file is untouched.
        if tool == "fs.write" {
            if let Some(outcome) = check_loop_patch_write(self, &approval_id, &record_fs_effect) {
                return outcome;
            }
        }
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
            // The write was dispatched and its verification failed:
            // an attempted execution with an Unknown report.
            Ok(Err(outcome)) => {
                record_fs_effect(&outcome);
                return outcome;
            }
            // The blocking task itself failed to join: dispatch was
            // attempted, outcome unknown.
            Err(_) => {
                let outcome = ToolOutcome::unknown();
                record_fs_effect(&outcome);
                return outcome;
            }
        };
        if !response.status().is_success() {
            let outcome = ToolOutcome::failed(ErrorCode::ToolFailed);
            record_fs_effect(&outcome);
            return outcome;
        }
        let outcome = match axum::body::to_bytes(response.into_body(), usize::MAX).await {
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
        };
        record_fs_effect(&outcome);
        outcome
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

    /// Test seam + production path for execution records: load
    /// `executions.jsonl` so a restart keeps the phase-1 shell
    /// records and the counter resumes. Production reads
    /// MOCK_EXECUTIONS_PATH. Phase 1 performs no restart
    /// conversion: records keep the status they were created
    /// with (always "running" in phase 1); restart semantics
    /// are phase 4 work, not something this seam may invent.
    pub fn with_executions_path(mut self, path: PathBuf) -> Self {
        let records = load_executions(&path);
        let mut executions = HashMap::new();
        for record in &records {
            executions.insert(record.execution_id.clone(), record.clone());
        }
        self.next_execution_id = Arc::new(Mutex::new(next_execution_number(&records)));
        self.executions = Arc::new(Mutex::new(executions));
        self.executions_path = Some(path);
        self
    }

    /// Test seam + production path for effect records: load
    /// `effects.jsonl` so a restart keeps the phase-2 convergence
    /// records and the counter resumes. Production reads
    /// MOCK_EFFECTS_PATH. Phase 2 only appends: no read path
    /// switches to effects yet, and no status conversion happens
    /// on reload (state convergence is phase 4).
    pub fn with_effects_path(mut self, path: PathBuf) -> Self {
        let records = load_effects(&path);
        let mut effects = HashMap::new();
        for record in &records {
            effects.insert(record.effect_id.clone(), record.clone());
        }
        self.next_effect_id = Arc::new(Mutex::new(next_effect_number(&records)));
        self.effects = Arc::new(Mutex::new(effects));
        self.effects_path = Some(path);
        self
    }

    /// E2-C §4.5: the Evidence audit export path
    /// (evidence.jsonl). Unlike `with_effects_path` this loads
    /// nothing: Evidence lives only in SQLite and has no
    /// in-memory map to resume; the file is a derived audit
    /// copy, regenerated at every Evidence write point, and the
    /// export route reads SQLite directly. Production reads
    /// MOCK_EVIDENCE_PATH. The packaged-app shell
    /// (`gateway_home.rs`) does not wire this variable yet — it
    /// wires only the record files the app needs for restart
    /// persistence — so until that follow-up lands, the file is
    /// written only when the variable is set explicitly; no
    /// behaviour is lost, because the route never reads the file.
    pub fn with_evidence_path(mut self, path: PathBuf) -> Self {
        self.evidence_path = Some(path);
        self
    }

    /// P0-D D1/D2 seam: open the SQLite database at `path`
    /// (schema v1, WAL + FULL). The backend starts in shadow
    /// mode unless the database already has `import_done = 1`,
    /// in which case this is a restart: the maps are reloaded
    /// from SQLite (never from JSONL) and the counters take the
    /// max of the JSONL-loaded and SQLite values, and the
    /// backend is SQLite immediately. An open failure marks
    /// the state broken (fail-closed at the migrated-domain
    /// surface) instead of silently falling back to JSONL.
    /// Call `sqlite_import_and_switch` for the explicit D4 switch.
    pub fn with_sqlite_path(mut self, path: PathBuf) -> Self {
        match sqlite_store::open_db(&path) {
            Ok(mut conn) => {
                let done = sqlite_store::import_done(&conn).unwrap_or(false);
                let mut backend = StateBackend::Shadow;
                if done {
                    backend = StateBackend::Sqlite;
                    if let Ok(rows) = sqlite_store::list_executions(&conn) {
                        let mut map = HashMap::new();
                        for row in &rows {
                            map.insert(row.execution_id.clone(), row.clone());
                        }
                        self.executions = Arc::new(Mutex::new(map));
                    }
                    if let Ok(rows) = sqlite_store::list_effects(&conn) {
                        let mut map = HashMap::new();
                        for row in &rows {
                            map.insert(row.effect_id.clone(), row.clone());
                        }
                        self.effects = Arc::new(Mutex::new(map));
                    }
                    if let Ok(rows) = sqlite_store::list_approvals(&conn) {
                        let mut map = HashMap::new();
                        for row in &rows {
                            match parse_approval_status(&row.status) {
                                Some(status) => {
                                    map.insert(
                                        row.tool_call_id.clone(),
                                        ApprovalState {
                                            tool: row.tool.clone(),
                                            status,
                                            binding: row.binding.clone(),
                                            consumed: row.consumed,
                                            operation_digest: row.operation_digest.clone(),
                                            action_binding: row.action_binding.clone(),
                                            effect_binding: row.effect_binding.clone(),
                                            message: row.message.clone(),
                                            command: row.command.clone(),
                                            consumed_by_worker_session_id: row.consumed_by_worker_session_id.clone(),
                                        },
                                    );
                                }
                                None => eprintln!(
                                    "warning: skipping SQLite approval record with unknown status {}",
                                    row.status
                                ),
                            }
                        }
                        self.approvals = Arc::new(Mutex::new(map));
                    }
                    if let Ok((next_exec, next_effect, next_appr)) =
                        sqlite_store::next_numbers(&conn)
                    {
                        let mut current = self.next_execution_id.lock().expect("execution id lock");
                        *current = (*current).max(next_exec);
                        drop(current);
                        let mut current = self.next_effect_id.lock().expect("effect id lock");
                        *current = (*current).max(next_effect);
                        drop(current);
                        let mut current = self.next_approval_id.lock().expect("approval id lock");
                        *current = (*current).max(next_appr);
                    }
                    if let Ok(next_goal) = sqlite_store::next_goal_number(&conn) {
                        let mut current = self.next_goal_id.lock().expect("goal id lock");
                        *current = (*current).max(next_goal);
                    }
                    if let Ok(next_mandate) = sqlite_store::next_mandate_number(&conn) {
                        let mut current = self.next_mandate_id.lock().expect("mandate id lock");
                        *current = (*current).max(next_mandate);
                    }
                    // D1-2 (Q-D1-21): one-time Policy import
                    // (v1 from the JSONL-loaded document when
                    // the table is empty) + reload from the
                    // versions table — the table, not the
                    // JSONL file, is the fact source after v6.
                    // The document is built from the maps
                    // before any SQLite write (map locks
                    // first); `conn` is a plain local here,
                    // not a locked guard.
                    let document = current_policy_document(&self);
                    if let Err(detail) =
                        import_policy_version_once(&self, &mut conn, &document)
                    {
                        eprintln!("warning: D1-2 policy import failed: {detail}");
                    }
                    let policy_versions =
                        sqlite_store::list_policy_versions(&conn).unwrap_or_default();
                    apply_policy_versions(&self, &policy_versions);
                }
                self.sqlite = Some(Arc::new(Mutex::new(conn)));
                self.sqlite_path = Some(path);
                self.sqlite_backend = Arc::new(Mutex::new(backend));
                // P0-E: a restart straight into SQLite mode
                // (import_done already set) scans now; a fresh
                // database in shadow mode scans after the
                // import-and-switch below makes it SQLite.
                if backend == StateBackend::Sqlite {
                    self.run_recovery_scan();
                }
            }
            Err(detail) => {
                eprintln!("warning: P0-D SQLite open failed ({detail}); migrated-domain surface is fail-closed, no JSONL fallback");
                *self.sqlite_broken.lock().expect("sqlite broken lock") = true;
                self.sqlite_backend = Arc::new(Mutex::new(StateBackend::Sqlite));
            }
        }
        self
    }

    /// Current SQLite backend name: "jsonl" | "shadow" | "sqlite".
    pub fn sqlite_backend_name(&self) -> &'static str {
        self.sqlite_backend.lock().expect("sqlite backend lock").name()
    }

    /// Shadow-mode SQLite write failures so far (D2 counter,
    /// visible to tests; production exposes no route for it).
    pub fn sqlite_shadow_failures(&self) -> u64 {
        *self.sqlite_shadow_failures.lock().expect("shadow failures lock")
    }

    /// Whether the SQLite state is known-broken (open failure or
    /// a SQLite-mode persist failure). Migrated-domain routes
    /// answer 5xx while this is set; chat and sessions are
    /// unaffected (they never migrated).
    pub fn sqlite_is_broken(&self) -> bool {
        *self.sqlite_broken.lock().expect("sqlite broken lock")
    }

    /// E2 slice: override the startup-resolved git binary
    /// (tests: force a fake git, or None for the
    /// `git_unavailable` path). Production never calls this.
    pub fn with_git_path(mut self, path: Option<PathBuf>) -> Self {
        self.git_path = path;
        self
    }

    /// P3-C: override the startup-resolved cargo binary
    /// (tests: force a fake cargo, or None for the
    /// `cargo_unavailable` path). Same seam as git.
    pub fn with_cargo_path(mut self, path: Option<PathBuf>) -> Self {
        self.cargo_path = path;
        self
    }

    /// E2 slice: claim a run for cancellation. The driver
    /// sees the claim before spawning and after the child
    /// exits, and finalizes the run as `cancelled`.
    pub(crate) fn mark_worker_cancelled(&self, run_id: &str) {
        self.worker_cancelled
            .lock()
            .expect("worker cancelled lock")
            .insert(run_id.to_string());
    }

    /// E2 slice: whether a cancel request has claimed this run.
    pub(crate) fn is_worker_cancelled(&self, run_id: &str) -> bool {
        self.worker_cancelled
            .lock()
            .expect("worker cancelled lock")
            .contains(run_id)
    }

    /// P0-E: this process's current WorkerSession id, if the
    /// recovery scan has run (SQLite mode only).
    pub fn current_worker_session(&self) -> Option<String> {
        self.current_worker_session.lock().expect("current worker lock").clone()
    }

    /// P0-E: whether the startup recovery scan failed. The
    /// binary checks this before binding (see main.rs) and
    /// refuses to start; in-process users fail closed through
    /// `sqlite_is_broken` as well.
    pub fn sqlite_recovery_failed(&self) -> bool {
        *self.recovery_failed.lock().expect("recovery failed lock")
    }

    /// P0-E recovery scan driver (plan §2.2): at most once per
    /// instance, SQLite mode only, never in pure JSONL mode
    /// (no sqlite connection) and never in shadow mode (the
    /// import-and-switch call site runs it once the backend
    /// becomes SQLite). A scan failure marks the state broken
    /// and records `recovery_failed`: a gateway that cannot
    /// establish its own worker identity must not open.
    fn run_recovery_scan(&self) {
        let Some(sqlite) = &self.sqlite else {
            return;
        };
        if *self.recovery_scanned.lock().expect("recovery scanned lock") {
            return;
        }
        *self.recovery_scanned.lock().expect("recovery scanned lock") = true;
        // E2-A §2.5: probe first (short lock, no transaction
        // held across the orphan kill), then the unknown
        // Effects go through `record_effect` (central key, map
        // insert, audit export — never the old inline key
        // computation), then the finalizing transaction.
        let stale = {
            let conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::recovery_probe(&conn) {
                Ok(stale) => stale,
                Err(detail) => {
                    eprintln!("P0-E ERROR: recovery scan failed ({detail}); migrated-domain surface is fail-closed, startup must not proceed");
                    *self.sqlite_broken.lock().expect("sqlite broken lock") = true;
                    *self.recovery_failed.lock().expect("recovery failed lock") = true;
                    return;
                }
            }
        };
        let mut finalized: Vec<(String, String, String)> = Vec::new();
        for stale_run in &stale {
            let effect_id = self.recovery_scan_effect(stale_run);
            // E2-A §2.3: an orphaned/lost run's Execution is not
            // terminal — it goes to `needs_review` (visible,
            // recoverable), never to a fabricated terminal word.
            if let Some(execution_id) = &stale_run.run.execution_id {
                close_execution(self, execution_id, "needs_review");
            }
            finalized.push((
                stale_run.run.run_id.clone(),
                stale_run.status.clone(),
                effect_id,
            ));
        }
        let mut conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::recovery_finalize(&mut conn, &self.origin_device_id, &finalized) {
            Ok(id) => {
                *self.current_worker_session.lock().expect("current worker lock") = Some(id);
            }
            Err(detail) => {
                eprintln!("P0-E ERROR: recovery scan failed ({detail}); migrated-domain surface is fail-closed, startup must not proceed");
                *self.sqlite_broken.lock().expect("sqlite broken lock") = true;
                *self.recovery_failed.lock().expect("recovery failed lock") = true;
            }
        }
        // E2-A §2.8: one-time reconciliation of historical
        // stranded "running" Executions (no new ones can be
        // produced after §2.3, so this is a no-op after the
        // first run that finds them). The finalize guard is
        // dropped first: reconcile re-locks the SQLite mutex
        // (std Mutex is not reentrant — holding it here would
        // deadlock the snapshot read).
        drop(conn);
        reconcile_stranded_executions(self);
    }

    /// E2-A §2.5: create (or reuse) the unknown Effect for one
    /// stale run, through `record_effect`. The key is
    /// deterministic (execution + `unknown_effect_identity` +
    /// `run_digest`, the P3-C vocabulary), so a scan that
    /// recorded the Effect but crashed before finalizing reuses
    /// the existing row instead of minting a duplicate (the
    /// effects table enforces `idempotency_key` UNIQUE). The
    /// probe outcome is stamped as the row's detail right
    /// after recording (the stamp re-persists the row, which
    /// the §2.6 Inbox guard makes safe).
    /// E2-B §3.3, Reader 2: the recovery scan reads
    /// `latest_plan_position` when finalizing a stale run and
    /// writes the position into the unknown Effect's
    /// reconciliation hint (its `detail`, alongside the probe
    /// outcome the scan already stamps there), for a human or
    /// Agent to reference when resuming. The hint is a
    /// reference, not a reconciliation: the Effect stays
    /// `unknown` and the Inbox row stays open. No checkpoint
    /// → the detail is unchanged.
    fn recovery_resume_hint(&self, execution_id: &str) -> Option<String> {
        let sqlite = self.sqlite.as_ref()?;
        if self.sqlite_is_broken() {
            return None;
        }
        let conn = sqlite.lock().expect("sqlite lock");
        let checkpoint = sqlite_store::latest_plan_position(&conn, execution_id).ok()??;
        let payload: serde_json::Value =
            serde_json::from_str(&checkpoint.payload_json).ok()?;
        let next = payload.get("next_effect_key").and_then(|v| v.as_str());
        let completed = payload
            .get("completed_keys")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        Some(match next {
            Some(key) => format!("resume at next_effect_key={key} ({completed} completed)"),
            None => format!("resume at end ({completed} completed, none pending)"),
        })
    }

    fn recovery_scan_effect(&self, stale: &sqlite_store::StaleWorkerRun) -> String {
        let (capability, operation, scope) =
            crate::worker::unknown_effect_identity(&stale.run, None);
        let digest = crate::worker::run_digest(&stale.run);
        // E2-B §3.3 Reader 2: the resume hint, appended to
        // the probe detail when the scan creates the Effect
        // row. A re-scanned run reuses its row (early return
        // below), which already carries the hint from its
        // first scan. Appended once: a detail that already
        // carries a hint is left alone.
        let hinted_detail = match &stale.run.execution_id {
            Some(execution_id) => match self.recovery_resume_hint(execution_id) {
                Some(hint) if !stale.detail.contains("resume at") => {
                    format!("{} | {hint}", stale.detail)
                }
                _ => stale.detail.clone(),
            },
            None => stale.detail.clone(),
        };
        if let Some(execution_id) = &stale.run.execution_id {
            let key = crate::effect_store::idempotency_key(
                execution_id,
                &capability,
                &operation,
                &scope,
                &digest,
            );
            if let Some(records) = effects_snapshot(self) {
                if let Some(existing) = records
                    .iter()
                    .find(|r| r.idempotency_key.as_deref() == Some(key.as_str()))
                {
                    return existing.effect_id.clone();
                }
            }
        }
        let record = record_effect(
            self,
            NewEffect {
                execution_id: stale.run.execution_id.clone(),
                capability,
                operation,
                scope,
                input_digest: Some(digest),
                approval_id: stale.run.approval_id.clone(),
                status: "unknown".to_string(),
            },
        );
        stamp_effect_detail(self, &record.effect_id, &hinted_detail);
        record.effect_id
    }

    /// P0-E clean-shutdown counterpart (plan §2.1), called by
    /// `serve` after the graceful shutdown completes.
    /// Best-effort: failures are logged and never block the
    /// exit, and a kill -9 simply never reaches this — the
    /// next startup's scan closes what this missed.
    pub fn sqlite_shutdown_cleanup(&self) {
        // Skipped when this process never established a current
        // worker (pure JSONL mode, or shadow mode before any
        // scan): a process with no worker identity of its own
        // must not end sessions it did not create.
        if self.current_worker_session().is_none() {
            return;
        }
        let Some(sqlite) = &self.sqlite else {
            return;
        };
        let mut conn = sqlite.lock().expect("sqlite lock");
        if let Err(detail) = sqlite_store::shutdown_cleanup(&mut conn) {
            eprintln!("warning: P0-E shutdown cleanup failed ({detail}); next startup scan will close the worker");
        }
    }

    /// E2-B §3.2 (sweeper): one pass over heartbeat-expired
    /// still-active assignments. Each expired assignment goes
    /// to the same lost terminal state as `shutdown_cleanup`
    /// (detail "heartbeat expired"), its Execution to
    /// `needs_review` with the recovery-scan Inbox shape
    /// (`review_required` + `unknown_requires_reconciliation`),
    /// in one transaction per assignment (see
    /// `sqlite_store::sweep_expired_assignment`). The sweeper
    /// never touches `worker_runs` rows — leftover runs are
    /// only collected by the next startup scan — and records
    /// no Effect (expiry is a liveness observation, not a
    /// side effect). Assignments with no lease (never
    /// heartbeated, e.g. takeover-held) are never expired.
    /// SQLite mode only; JSONL/shadow mode sweeps nothing.
    /// Returns how many assignments were lost. Failures are
    /// logged per assignment and never abort the pass.
    pub fn sweep_expired_assignments(&self) -> usize {
        if self.sqlite_mode() != StateBackend::Sqlite || self.sqlite_is_broken() {
            return 0;
        }
        let Some(sqlite) = &self.sqlite else {
            return 0;
        };
        let expired = {
            let conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::expired_active_assignments(&conn, sqlite_store::now_ms() as i64) {
                Ok(rows) => rows,
                Err(detail) => {
                    eprintln!("warning: E2-B assignment sweep list failed: {detail}");
                    return 0;
                }
            }
        };
        let mut swept = 0;
        for row in expired {
            let mut conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::sweep_expired_assignment(&mut conn, &row.assignment_id) {
                Ok(outcome) if outcome.assignment_lost => swept += 1,
                Ok(_) => {}
                Err(detail) => {
                    eprintln!(
                        "warning: E2-B assignment sweep failed for {}: {detail}",
                        row.assignment_id
                    );
                }
            }
        }
        swept
    }

    pub(crate) fn sqlite_mode(&self) -> StateBackend {
        *self.sqlite_backend.lock().expect("sqlite backend lock")
    }

    /// P0-D D4 explicit switch point: import folded JSONL state
    /// (D3, single transaction, re-runnable) and then switch
    /// the migrated-domain read path to SQLite. After this
    /// returns Ok, JSONL appends for the migrated domains stop
    /// and the JSONL files become generated audit exports
    /// (rewritten best-effort on later writes). The JSONL files
    /// themselves are never deleted. An import failure leaves
    /// the backend in shadow mode (JSONL authoritative).
    pub fn sqlite_import_and_switch(&self) -> Result<sqlite_store::ImportReport, String> {
        let Some(sqlite) = &self.sqlite else {
            return Err("sqlite not configured".to_string());
        };
        // D1-2 (Q-D1-21): build the candidate v1 Policy
        // document from the maps BEFORE taking the SQLite
        // lock (map locks first, never under the SQLite
        // lock — see the comment below).
        let policy_document = current_policy_document(self);
        let mut guard = sqlite.lock().expect("sqlite lock");
        let conn = &mut *guard;
        let executions = match &self.executions_path {
            Some(path) => sqlite_store::fold_executions(&load_executions(path)),
            None => Vec::new(),
        };
        let effects = match &self.effects_path {
            Some(path) => sqlite_store::fold_effects(&load_effects(path)),
            None => Vec::new(),
        };
        let approvals = match &self.approvals_path {
            Some(path) => sqlite_store::fold_approvals(&crate::approval_store::load_records(path)),
            None => Vec::new(),
        };
        let report = sqlite_store::import_folded(
            conn,
            &executions,
            &effects,
            &approvals,
            sqlite_store::now_ms(),
        )?;
        // D1-2 (Q-D1-21): one-time Policy import (v1 from
        // the JSONL-loaded document when the table is empty
        // and a policy file exists).
        if let Err(detail) = import_policy_version_once(self, &mut *conn, &policy_document) {
            eprintln!("warning: D1-2 policy import failed: {detail}");
        }
        // Reload the write-through cache from SQLite while the
        // connection is still in hand, then release it before
        // touching any map lock (persist call sites take the
        // SQLite lock while holding a map lock; reversing the
        // order here would risk deadlock under concurrency).
        let exec_rows = sqlite_store::list_executions(conn);
        let effect_rows = sqlite_store::list_effects(conn);
        let approval_rows = sqlite_store::list_approvals(conn);
        let numbers = sqlite_store::next_numbers(conn);
        let policy_versions = sqlite_store::list_policy_versions(conn);
        let next_mandate = sqlite_store::next_mandate_number(conn);
        drop(guard);
        if let Ok(rows) = exec_rows {
            let mut map = HashMap::new();
            for row in &rows {
                map.insert(row.execution_id.clone(), row.clone());
            }
            *self.executions.lock().expect("execution lock") = map;
        }
        if let Ok(rows) = effect_rows {
            let mut map = HashMap::new();
            for row in &rows {
                map.insert(row.effect_id.clone(), row.clone());
            }
            *self.effects.lock().expect("effect lock") = map;
        }
        if let Ok(rows) = approval_rows {
            let mut map = HashMap::new();
            for row in &rows {
                if let Some(status) = parse_approval_status(&row.status) {
                    map.insert(
                        row.tool_call_id.clone(),
                        ApprovalState {
                            tool: row.tool.clone(),
                            status,
                            binding: row.binding.clone(),
                            consumed: row.consumed,
                            operation_digest: row.operation_digest.clone(),
                            action_binding: row.action_binding.clone(),
                            effect_binding: row.effect_binding.clone(),
                            message: row.message.clone(),
                            command: row.command.clone(),
                            consumed_by_worker_session_id: row.consumed_by_worker_session_id.clone(),
                        },
                    );
                }
            }
            *self.approvals.lock().expect("approval lock") = map;
        }
        if let Ok((next_exec, next_effect, next_appr)) = numbers {
            let mut current = self.next_execution_id.lock().expect("execution id lock");
            *current = (*current).max(next_exec);
            drop(current);
            let mut current = self.next_effect_id.lock().expect("effect id lock");
            *current = (*current).max(next_effect);
            drop(current);
            let mut current = self.next_approval_id.lock().expect("approval id lock");
            *current = (*current).max(next_appr);
        }
        // D1-2 (Q-D1-21): the versions table overrides the
        // JSONL-loaded policy (map locks only — the SQLite
        // guard is already released).
        if let Ok(versions) = policy_versions {
            apply_policy_versions(self, &versions);
        }
        if let Ok(next_mandate) = next_mandate {
            let mut current = self.next_mandate_id.lock().expect("mandate id lock");
            *current = (*current).max(next_mandate);
        }
        *self.sqlite_backend.lock().expect("sqlite backend lock") = StateBackend::Sqlite;
        *self.sqlite_broken.lock().expect("sqlite broken lock") = false;
        // P0-E: first entry into SQLite mode on this instance
        // runs the recovery scan (guarded: a restart that
        // already scanned in with_sqlite_path skips it).
        // A scan failure marks the state broken again (see
        // run_recovery_scan); the import itself still reports
        // Ok, and the failure surfaces through
        // sqlite_recovery_failed / sqlite_is_broken.
        self.run_recovery_scan();
        Ok(report)
    }

    /// Test seam for P0-A provenance: override the origin device
    /// id this instance stamps on new Execution and Effect rows.
    /// Production resolves it once at startup from
    /// MOCK_ORIGIN_DEVICE_ID (see `resolve_origin_device_id`).
    pub fn with_origin_device_id(mut self, id: String) -> Self {
        self.origin_device_id = id;
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

    /// Test seam: load connector accounts and connection flags
    /// from a tagged JSONL file, mirroring with_sessions_path.
    /// Production state reads MOCK_CONNECTOR_STATE_PATH.
    pub fn with_connector_state_path(mut self, path: PathBuf) -> Self {
        let (accounts, connections) = load_connector_state(&path);
        let mut account_map = HashMap::new();
        for record in &accounts {
            account_map.insert(record.account_id.clone(), record.clone());
        }
        let mut connection_map = HashMap::new();
        for record in &connections {
            connection_map.insert(record.connector_id.clone(), record.connected);
        }
        self.next_account_id = Arc::new(Mutex::new(next_account_number(&accounts)));
        self.connector_accounts = Arc::new(Mutex::new(account_map));
        self.connector_connections = Arc::new(Mutex::new(connection_map));
        self.connector_state_path = Some(path);
        self
    }

    /// Test seam: load vault metadata from a tagged JSONL file,
    /// mirroring with_connector_state_path. Production state reads
    /// MOCK_VAULT_STATE_PATH. The secret backend is untouched: tests
    /// swap it with with_vault_secrets, production keeps Keychain.
    pub fn with_vault_state_path(mut self, path: PathBuf) -> Self {
        let (slots, catalog, custom, tickets) = load_vault_state(&path);
        let mut slot_map = HashMap::new();
        for record in &slots {
            slot_map.insert(record.handle.clone(), record.clone());
        }
        let mut catalog_map = HashMap::new();
        for record in &catalog {
            catalog_map.insert(record.id.clone(), record.clone());
        }
        let mut custom_map = HashMap::new();
        for record in &custom {
            custom_map.insert(record.id.clone(), record.clone());
        }
        let mut ticket_map = HashMap::new();
        for record in &tickets {
            ticket_map.insert(record.ticket_id.clone(), record.clone());
        }
        self.next_ticket_id = Arc::new(Mutex::new(next_ticket_number(&tickets)));
        self.vault_slots = Arc::new(Mutex::new(slot_map));
        self.vault_catalog = Arc::new(Mutex::new(catalog_map));
        self.vault_custom = Arc::new(Mutex::new(custom_map));
        self.vault_tickets = Arc::new(Mutex::new(ticket_map));
        self.vault_state_path = Some(path);
        self
    }

    /// Test seam: swap the secret backend (memory store in tests).
    pub fn with_vault_secrets(mut self, backend: Arc<dyn CredentialStore + Send + Sync>) -> Self {
        self.vault_secrets = backend;
        self
    }

    /// Test seam: load the staged model selection from a
    /// tagged JSONL file, mirroring with_policy_state_path.
    /// Production state reads MOCK_MODEL_STATE_PATH.
    pub fn with_model_state_path(mut self, path: PathBuf) -> Self {
        let staged = load_model_state(&path);
        self.model_staged = Arc::new(Mutex::new(staged));
        self.model_state_path = Some(path);
        self
    }

    /// Test seam: load peer relationship state from a
    /// tagged JSONL file, mirroring with_connector_state_path.
    /// Production state reads MOCK_PEER_STATE_PATH.
    pub fn with_peer_state_path(mut self, path: PathBuf) -> Self {
        let (peers, invites, inbox, requests) = load_peer_state(&path);
        let mut peer_map = HashMap::new();
        for record in &peers {
            peer_map.insert(record.muse_id.clone(), record.clone());
        }
        let mut invite_map = HashMap::new();
        for record in &invites {
            invite_map.insert(record.invite_id.clone(), record.clone());
        }
        let mut inbox_map = HashMap::new();
        for record in &inbox {
            inbox_map.insert(record.message_id.clone(), record.clone());
        }
        let mut request_map = HashMap::new();
        for record in &requests {
            request_map.insert(record.relationship_generation_id.clone(), record.clone());
        }
        self.peer_peers = Arc::new(Mutex::new(peer_map));
        self.peer_invites = Arc::new(Mutex::new(invite_map));
        self.peer_inbox = Arc::new(Mutex::new(inbox_map));
        self.peer_requests = Arc::new(Mutex::new(request_map));
        self.peer_state_path = Some(path);
        self
    }

    /// Test seam: load saved-space state from a tagged
    /// JSONL file, mirroring with_peer_state_path.
    /// Production state reads MOCK_SPACE_STATE_PATH.
    pub fn with_space_state_path(mut self, path: PathBuf) -> Self {
        let spaces = load_space_state(&path);
        let mut space_map = HashMap::new();
        for record in &spaces {
            space_map.insert(record.slug.clone(), record.clone());
        }
        self.spaces = Arc::new(Mutex::new(space_map));
        self.space_state_path = Some(path);
        self
    }

    /// Test seam: load feed overlays from a tagged JSONL
    /// file over the Mock seeds (last-wins by unit_id),
    /// mirroring with_space_state_path. Production state
    /// reads MOCK_FEED_STATE_PATH.
    pub fn with_feed_state_path(mut self, path: PathBuf) -> Self {
        let mut unit_map: HashMap<String, FeedUnit> =
            seed_feed_units().into_iter().map(|u| (u.unit_id.clone(), u)).collect();
        for record in load_feed_state(&path) {
            unit_map.insert(record.unit_id.clone(), record);
        }
        self.feed_units = Arc::new(Mutex::new(unit_map));
        self.feed_state_path = Some(path);
        self
    }

    /// Test seam: load chat seen markers and theme from a
    /// tagged JSONL file, mirroring with_feed_state_path.
    /// Production state reads MOCK_CHAT_STATE_PATH.
    pub fn with_chat_state_path(mut self, path: PathBuf) -> Self {
        let (markers, theme) = load_chat_state(&path);
        let mut seen_map = HashMap::new();
        for marker in &markers {
            seen_map.insert(marker.thread_id.clone(), marker.marker);
        }
        self.chat_seen = Arc::new(Mutex::new(seen_map));
        self.chat_theme = Arc::new(Mutex::new(theme));
        self.chat_state_path = Some(path);
        self
    }

    /// Test seam: load node records from a JSONL file,
    /// mirroring with_sessions_path. Production state reads
    /// MOCK_NODE_STATE_PATH.
    pub fn with_node_state_path(mut self, path: PathBuf) -> Self {
        let records = load_nodes(&path);
        let mut nodes = HashMap::new();
        for record in &records {
            nodes.insert(record.node_id.clone(), record.clone());
        }
        self.next_node_id = Arc::new(Mutex::new(next_node_number(&records)));
        self.nodes = Arc::new(Mutex::new(nodes));
        self.node_state_path = Some(path);
        self
    }

    /// Test seam: load workspace bindings from a JSONL file,
    /// mirroring with_node_state_path. Production state reads
    /// MOCK_BINDING_STATE_PATH.
    pub fn with_binding_state_path(mut self, path: PathBuf) -> Self {
        let records = load_bindings(&path);
        let mut bindings = HashMap::new();
        for record in &records {
            bindings.insert(record.workspace_id.clone(), record.clone());
        }
        self.next_workspace_id = Arc::new(Mutex::new(next_workspace_number(&records)));
        self.bindings = Arc::new(Mutex::new(bindings));
        self.binding_state_path = Some(path);
        self
    }

    /// Test seam: load projects from a JSONL file, mirroring
    /// with_node_state_path. Production state reads
    /// MOCK_PROJECT_STATE_PATH.
    pub fn with_project_state_path(mut self, path: PathBuf) -> Self {
        let records = load_projects(&path);
        let mut projects = HashMap::new();
        for record in &records {
            projects.insert(record.project_id.clone(), record.clone());
        }
        self.next_project_id = Arc::new(Mutex::new(next_project_number(&records)));
        self.projects = Arc::new(Mutex::new(projects));
        self.project_state_path = Some(path);
        self
    }

    /// Test seam: load locations from a JSONL file, mirroring
    /// with_node_state_path. Production state reads
    /// MOCK_LOCATION_STATE_PATH.
    pub fn with_location_state_path(mut self, path: PathBuf) -> Self {
        let records = load_locations(&path);
        let mut locations = HashMap::new();
        for record in &records {
            locations.insert(record.location_id.clone(), record.clone());
        }
        self.next_location_id = Arc::new(Mutex::new(next_location_number(&records)));
        self.locations = Arc::new(Mutex::new(locations));
        self.location_state_path = Some(path);
        self
    }

    /// Test seam: load worktrees from a JSONL file, mirroring
    /// with_node_state_path. Production state reads
    /// MOCK_WORKTREE_STATE_PATH.
    pub fn with_worktree_state_path(mut self, path: PathBuf) -> Self {
        let records = load_worktrees(&path);
        let mut worktrees = HashMap::new();
        for record in &records {
            worktrees.insert(record.worktree_id.clone(), record.clone());
        }
        self.next_worktree_id = Arc::new(Mutex::new(next_worktree_number(&records)));
        self.worktrees = Arc::new(Mutex::new(worktrees));
        self.worktree_state_path = Some(path);
        self
    }

    /// Test seam: load stream subscriptions, opens, pokes,
    /// and selections from a tagged JSONL file, mirroring
    /// with_node_state_path. Production state reads
    /// MOCK_STREAM_STATE_PATH.
    pub fn with_stream_state_path(mut self, path: PathBuf) -> Self {
        let data = load_stream_state(&path);
        let mut subscriptions = HashMap::new();
        for record in &data.subscriptions {
            subscriptions.insert(record.subscription_id.clone(), record.clone());
        }
        let mut selections = HashMap::new();
        for record in &data.selections {
            selections.insert(record.thread_id.clone(), record.clone());
        }
        self.next_subscription_id =
            Arc::new(Mutex::new(next_subscription_number(&data.subscriptions)));
        self.next_stream_id = Arc::new(Mutex::new(next_stream_number(&data.opens)));
        self.next_poke_order = Arc::new(Mutex::new(next_poke_order(&data.pokes)));
        self.stream_subscriptions = Arc::new(Mutex::new(subscriptions));
        self.stream_pokes = Arc::new(Mutex::new(data.pokes));
        self.stream_selections = Arc::new(Mutex::new(selections));
        self.stream_state_path = Some(path);
        self
    }

    /// Test seam: load voice records from a tagged JSONL
    /// file, mirroring with_stream_state_path. Custom voice
    /// seeds are merged with overlays (tombstones kept in
    /// the map so delete can tell unknown from deleted).
    /// Production state reads MOCK_VOICE_STATE_PATH.
    pub fn with_voice_state_path(mut self, path: PathBuf) -> Self {
        let data = load_voice_state(&path);
        let mut customs = HashMap::new();
        for record in merged_custom_voices(&data.custom_voices) {
            customs.insert(record.saved_voice_id.clone(), record);
        }
        self.voice_custom = Arc::new(Mutex::new(customs));
        self.voice_active = Arc::new(Mutex::new(data.active_voice.and_then(|r| r.voice_id)));
        self.voice_pipeline =
            Arc::new(Mutex::new(data.pipeline_selection.and_then(|r| r.profile_id)));
        self.voice_state_path = Some(path);
        self
    }

    /// Test seam: replace the mail source (tests use the
    /// mock source; production macOS uses AppleScript,
    /// other platforms keep the Unavailable source).
    pub fn with_mail_source(mut self, source: Arc<dyn MailSource>) -> Self {
        self.mail_source = source;
        self
    }

    /// Test seam: load permission policy state from a tagged
    /// JSONL file, mirroring with_vault_state_path. Production
    /// state reads MOCK_POLICY_STATE_PATH.
    pub fn with_policy_state_path(mut self, path: PathBuf) -> Self {
        let (defaults, overrides, connectors, rules, scopes, rules_history) =
            load_policy_state(&path);
        let mut connector_map = HashMap::new();
        for record in &connectors {
            connector_map.insert(record.connector_id.clone(), record.clone());
        }
        let mut scope_map = HashMap::new();
        for record in &scopes {
            scope_map.insert(record.key(), record.clone());
        }
        self.policy_defaults = Arc::new(Mutex::new(defaults));
        self.policy_overrides = Arc::new(Mutex::new(overrides));
        self.policy_connectors = Arc::new(Mutex::new(connector_map));
        // Task 13: fill the rules Arc in place, do NOT
        // replace it — the DeepSeek clients built earlier
        // hold this exact Arc, and replacing it would
        // silently unplug enforcement.
        *self.policy_rules.lock().expect("policy rules lock") = rules;
        // E2-B §3.4: retain the loaded version and every
        // historical version, so snapshots created before
        // this restart still resolve to their frozen rules.
        if let Some(loaded) = self.policy_rules.lock().expect("policy rules lock").clone() {
            retain_policy_rules_version(&self, &loaded);
        } else {
            retain_policy_rules_version(&self, &[]);
        }
        for version in &rules_history {
            retain_policy_rules_version(&self, version);
        }
        self.policy_scopes = Arc::new(Mutex::new(scope_map));
        self.policy_state_path = Some(path);
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

#[derive(Debug, Serialize)]
struct FileStatDto {
    path: String,
    is_dir: bool,
    size_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct RenameRequestDto {
    from: String,
    to: String,
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MkdirRequestDto {
    path: String,
    approval_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BatchReadItemDto { path: String, offset: Option<u64>, len: Option<u64> }
#[derive(Debug, Deserialize)]
struct BatchReadRequestDto { items: Vec<BatchReadItemDto> }
#[derive(Debug, Serialize)]
struct BatchReadItemResponseDto { path: String, ok: bool, #[serde(skip_serializing_if = "Option::is_none")] result: Option<ReadResponseDto>, #[serde(skip_serializing_if = "Option::is_none")] error: Option<String> }
#[derive(Debug, Serialize)]
struct BatchReadResponseDto { items: Vec<BatchReadItemResponseDto> }
#[derive(Debug, Deserialize)]
struct BatchStatsRequestDto { paths: Vec<String> }
#[derive(Debug, Serialize)]
struct BatchStatItemResponseDto { path: String, ok: bool, #[serde(skip_serializing_if = "Option::is_none")] stat: Option<FileStatDto>, #[serde(skip_serializing_if = "Option::is_none")] error: Option<String> }
#[derive(Debug, Serialize)]
struct BatchStatsResponseDto { items: Vec<BatchStatItemResponseDto> }
#[derive(Debug, Deserialize)]
struct SearchRequestDto { query: String, start_path: Option<String>, limit: Option<usize>, max_depth: Option<usize> }
#[derive(Debug, Serialize)]
struct SearchResponseDto { entries: Vec<FileEntryDto>, truncated: bool }
#[derive(Debug, Deserialize)]
struct RawQuery { path: String, format: Option<String> }
#[derive(Debug, Deserialize)]
struct SubscribeRequestDto { path: Option<String>, cursor: Option<String>, wait_ms: Option<u64> }
#[derive(Debug, Serialize)]
struct SubscribeResponseDto { changed: bool, cursor: String, #[serde(skip_serializing_if = "Option::is_none")] entries: Option<Vec<FileEntryDto>> }
#[derive(Debug, Deserialize)]
struct ThumbnailQuery { path: String, w: Option<u32>, format: Option<String> }
#[derive(Debug, Deserialize)]
struct ExportRequestDto { from: String, to: String, approval_id: Option<String> }
#[derive(Debug, Deserialize)]
struct LibraryRequestDto { kinds: Option<Vec<String>>, limit: Option<usize> }
#[derive(Debug, Serialize)]
struct LibraryItemDto { path: String, kind: String, is_dir: bool, size_bytes: u64, modified_ms: u128 }
#[derive(Debug, Serialize)]
struct LibraryResponseDto { items: Vec<LibraryItemDto>, truncated: bool }

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

async fn files_stat(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let rel = query.path.unwrap_or_default();
    let Ok(full) = resolve_file(&state.files_root, &rel) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(metadata) = std::fs::metadata(&full) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response();
    };
    Json(FileStatDto {
        path: rel,
        is_dir: metadata.is_dir(),
        size_bytes: if metadata.is_dir() { 0 } else { metadata.len() },
    }).into_response()
}

fn collect_tree(root: &Path, rel: &str, max_depth: usize) -> Result<Vec<FileEntryDto>, StatusCode> {
    let start = resolve_file(root, rel)?;
    let mut out = Vec::new();
    fn visit(dir: &Path, prefix: &str, depth: usize, max_depth: usize, out: &mut Vec<FileEntryDto>) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            let name = entry.file_name().to_string_lossy().into_owned();
            let path = if prefix.is_empty() { name.clone() } else { format!("{prefix}/{name}") };
            out.push(FileEntryDto { name, path: path.clone(), is_dir: meta.is_dir(), size_bytes: if meta.is_dir() { 0 } else { meta.len() } });
            if meta.is_dir() && depth < max_depth { visit(&entry.path(), &path, depth + 1, max_depth, out); }
        }
    }
    let _ = root;
    visit(&start, rel.trim_matches('/'), 0, max_depth, &mut out);
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

fn read_chunk_value(root: &Path, item: &BatchReadItemDto) -> Result<ReadResponseDto, String> {
    let full = resolve_file(root, &item.path).map_err(|_| "path escapes file root".to_string())?;
    let data = std::fs::read(&full).map_err(|_| "file not found".to_string())?;
    if full.is_dir() { return Err("cannot read directory".to_string()); }
    let offset = item.offset.unwrap_or(0);
    let len = item.len.unwrap_or(4096).min(1_048_576);
    let total = data.len() as u64;
    let start = (offset as usize).min(data.len());
    let end = start.saturating_add(len as usize).min(data.len());
    let chunk = &data[start..end];
    Ok(ReadResponseDto { path: item.path.clone(), offset, content_base64: base64::engine::general_purpose::STANDARD.encode(chunk), eof: end as u64 >= total, total_size: total, bytes_read: chunk.len() as u64 })
}

/// 对绝对目标做不依赖文件系统的词法折叠，避免 export
/// 用未经折叠的 `..` 绕过文件根前缀判断。
fn normalize_absolute_path(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() { return None; }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir => {
                let Some(last) = normalized.file_name() else { continue };
                if last == std::ffi::OsStr::new("/") { return None; }
                normalized.pop();
            }
        }
    }
    Some(normalized)
}

async fn files_reads(State(state): State<AppState>, Json(req): Json<BatchReadRequestDto>) -> impl IntoResponse {
    const MAX_ITEMS: usize = 128;
    const MAX_TOTAL: u64 = 8 * 1_048_576;
    if req.items.len() > MAX_ITEMS { return bad_request("too many read items"); }
    let requested: u64 = req.items.iter().map(|i| i.len.unwrap_or(4096).min(1_048_576)).sum();
    if requested > MAX_TOTAL { return (StatusCode::PAYLOAD_TOO_LARGE, Json(serde_json::json!({"error":"batch read exceeds byte limit"}))).into_response(); }
    let items = req.items.iter().map(|item| match read_chunk_value(&state.files_root, item) {
        Ok(result) => BatchReadItemResponseDto { path: item.path.clone(), ok: true, result: Some(result), error: None },
        Err(error) => BatchReadItemResponseDto { path: item.path.clone(), ok: false, result: None, error: Some(error) },
    }).collect();
    Json(BatchReadResponseDto { items }).into_response()
}

async fn files_stats(State(state): State<AppState>, Json(req): Json<BatchStatsRequestDto>) -> impl IntoResponse {
    if req.paths.len() > 256 { return bad_request("too many stat paths"); }
    let items = req.paths.iter().map(|path| {
        let result = resolve_file(&state.files_root, path).ok().and_then(|full| std::fs::metadata(full).ok().map(|m| FileStatDto { path: path.clone(), is_dir: m.is_dir(), size_bytes: if m.is_dir() { 0 } else { m.len() } }));
        match result { Some(stat) => BatchStatItemResponseDto { path: path.clone(), ok: true, stat: Some(stat), error: None }, None => BatchStatItemResponseDto { path: path.clone(), ok: false, stat: None, error: Some("file not found or invalid path".to_string()) } }
    }).collect();
    Json(BatchStatsResponseDto { items }).into_response()
}

async fn files_search(State(state): State<AppState>, Json(req): Json<SearchRequestDto>) -> impl IntoResponse {
    let Ok(start) = resolve_file(&state.files_root, req.start_path.as_deref().unwrap_or("")) else { return bad_request("path escapes file root"); };
    let limit = req.limit.unwrap_or(200).clamp(1, 200);
    let depth = req.max_depth.unwrap_or(16).min(16);
    let query = req.query.to_lowercase();
    let mut all = collect_tree(&state.files_root, req.start_path.as_deref().unwrap_or(""), depth).unwrap_or_default();
    all.retain(|e| e.path.to_lowercase().contains(&query));
    let truncated = all.len() > limit;
    all.truncate(limit);
    let _ = start;
    Json(SearchResponseDto { entries: all, truncated }).into_response()
}

async fn files_raw(State(state): State<AppState>, Query(query): Query<RawQuery>) -> Response {
    let Ok(full) = resolve_file(&state.files_root, &query.path) else { return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"path escapes file root"}))).into_response(); };
    let Ok(meta) = std::fs::metadata(&full) else { return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"file not found"}))).into_response(); };
    if meta.is_dir() { return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"cannot read directory"}))).into_response(); }
    if meta.len() > 1_048_576 { return (StatusCode::PAYLOAD_TOO_LARGE, Json(serde_json::json!({"error":"file exceeds read limit"}))).into_response(); }
    let Ok(bytes) = std::fs::read(&full) else { return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot read file"}))).into_response(); };
    let content_type = match full.extension().and_then(|x| x.to_str()).unwrap_or("").to_ascii_lowercase().as_str() { "png" => "image/png", "jpg" | "jpeg" => "image/jpeg", "gif" => "image/gif", "webp" => "image/webp", "json" => "application/json", "txt" | "md" => "text/plain; charset=utf-8", _ => "application/octet-stream" };
    if query.format.as_deref() == Some("json") { return Json(serde_json::json!({"path":query.path,"content_type":content_type,"content_base64":base64::engine::general_purpose::STANDARD.encode(bytes)})).into_response(); }
    Response::builder().status(StatusCode::OK).header(header::CONTENT_TYPE, content_type).header(header::CONTENT_DISPOSITION, "inline").body(Body::from(bytes)).unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

fn tree_cursor(entries: &[FileEntryDto]) -> String { crate::operation_binding::sha256_hex(serde_json::to_string(entries).unwrap_or_default().as_bytes()) }
async fn files_subscribe(State(state): State<AppState>, Json(req): Json<SubscribeRequestDto>) -> impl IntoResponse {
    let path = req.path.as_deref().unwrap_or("");
    let wait = req.wait_ms.unwrap_or(0).min(25_000);
    let mut entries = collect_tree(&state.files_root, path, 32).unwrap_or_default();
    let mut cursor = tree_cursor(&entries);
    if req.cursor.as_deref() != Some(cursor.as_str()) || wait == 0 { return Json(SubscribeResponseDto { changed: req.cursor.as_deref() != Some(cursor.as_str()), cursor, entries: Some(entries) }).into_response(); }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        entries = collect_tree(&state.files_root, path, 32).unwrap_or_default();
        cursor = tree_cursor(&entries);
        if req.cursor.as_deref() != Some(cursor.as_str()) || tokio::time::Instant::now() >= deadline { break; }
    }
    let changed = req.cursor.as_deref() != Some(cursor.as_str());
    Json(SubscribeResponseDto { changed, cursor, entries: if changed { Some(entries) } else { None } }).into_response()
}

async fn files_thumbnail(State(state): State<AppState>, Query(query): Query<ThumbnailQuery>) -> Response {
    let Ok(full) = resolve_file(&state.files_root, &query.path) else { return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"path escapes file root"}))).into_response(); };
    let Ok(meta) = std::fs::metadata(&full) else { return (StatusCode::NOT_FOUND, Json(serde_json::json!({"error":"file not found"}))).into_response(); };
    if meta.is_dir() { return (StatusCode::UNSUPPORTED_MEDIA_TYPE, Json(serde_json::json!({"error":"thumbnail supports images only"}))).into_response(); }
    let ext = full.extension().and_then(|x| x.to_str()).unwrap_or("").to_ascii_lowercase();
    if !matches!(ext.as_str(), "png" | "jpg" | "jpeg" | "gif" | "webp") { return (StatusCode::UNSUPPORTED_MEDIA_TYPE, Json(serde_json::json!({"error":"thumbnail supports png, jpeg, gif and webp"}))).into_response(); }
    let Ok(bytes) = std::fs::read(&full) else { return (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({"error":"cannot read image"}))).into_response(); };
    let Ok(image) = image::load_from_memory(&bytes) else { return (StatusCode::UNPROCESSABLE_ENTITY, Json(serde_json::json!({"error":"cannot decode image"}))).into_response(); };
    let width = query.w.unwrap_or(256).clamp(1, 512);
    // Downscale only: a source that already fits the box keeps its size.
    let thumb = if image.width() > width || image.height() > width { image.thumbnail(width, width) } else { image };
    let mut output = Cursor::new(Vec::new());
    if thumb.write_to(&mut output, image::ImageFormat::Jpeg).is_err() { return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot encode thumbnail"}))).into_response(); }
    let data = output.into_inner();
    if query.format.as_deref() == Some("json") { return Json(serde_json::json!({"path":query.path,"content_type":"image/jpeg","content_base64":base64::engine::general_purpose::STANDARD.encode(data)})).into_response(); }
    Response::builder().status(StatusCode::OK).header(header::CONTENT_TYPE, "image/jpeg").header(header::CONTENT_DISPOSITION, "inline").body(Body::from(data)).unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

async fn files_export(State(state): State<AppState>, Json(req): Json<ExportRequestDto>) -> impl IntoResponse {
    let Some(digest) = crate::operation_binding::export_digest(&req.from, &req.to) else { return bad_request("invalid export path"); };
    let Ok(source) = resolve_file(&state.files_root, &req.from) else { return bad_request("path escapes file root"); };
    let Some(target) = normalize_absolute_path(Path::new(&req.to)) else { return bad_request("export target must be an absolute path outside file root"); };
    let Ok(root) = std::fs::canonicalize(&state.files_root) else { return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot resolve file root"}))).into_response(); };
    let target_abs = if target.exists() {
        std::fs::canonicalize(&target).unwrap_or(target.clone())
    } else if let (Some(parent), Some(name)) = (target.parent(), target.file_name()) {
        std::fs::canonicalize(parent).map(|p| p.join(name)).unwrap_or(target.clone())
    } else {
        target.clone()
    };
    if !target_abs.is_absolute() || target_abs.starts_with(&root) { return bad_request("export target must be outside file root"); }
    if target.exists() { return (StatusCode::CONFLICT, Json(serde_json::json!({"error":"export target already exists"}))).into_response(); }
    // P0-C: bound Effect gates the export (see files_write).
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            let size = std::fs::metadata(&target).map(|m| m.len()).unwrap_or(0);
            return Json(serde_json::json!({"from":req.from,"to":req.to,"size_bytes":size,"effect_id":effect.effect_id,"replayed":true})).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.export", &req.approval_id, &digest, Some(&effect.effect_id)) { return *response; }
        if let Err(response) = start_bound_effect(&state, effect) { return *response; }
    } else if let Err(response) = consume_bound_approval(&state, "fs.export", &req.approval_id, &digest) { return *response; }
    if !source.is_file() { if let Some(effect) = &bound_effect { let _ = transition_effect(&state, &effect.effect_id, "failed", None, None); } return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error":"export source must be a file"}))).into_response(); }
    if let Some(parent) = target.parent() { if std::fs::create_dir_all(parent).is_err() { if let Some(effect) = &bound_effect { let _ = transition_effect(&state, &effect.effect_id, "failed", None, None); } return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot create export parent"}))).into_response(); } }
    match std::fs::copy(&source, &target) { Ok(bytes) => {
        // P0-C: drive the bound Effect; the scope is the
        // normalized source path (the digest already commits
        // to the absolute destination). Legacy approvals record
        // one terminal row.
        if let Some(effect) = &bound_effect {
            let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            // P0-R: the generic hook skips export rows (the
            // target path is not on the Effect row), so the
            // baseline is recorded here from the canonical
            // absolute target the copy actually wrote.
            record_export_evidence(&state, &effect.effect_id, effect.execution_id.as_deref(), &target_abs);
        } else {
            let scope = crate::operation_binding::normalize_path(&req.from).unwrap_or_default();
            let record = record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "export".to_string(), scope, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            record_export_evidence(&state, &record.effect_id, record.execution_id.as_deref(), &target_abs);
        }
        Json(serde_json::json!({"from":req.from,"to":req.to,"size_bytes":bytes})).into_response()
    }, Err(_) => { if let Some(effect) = &bound_effect { let _ = transition_effect(&state, &effect.effect_id, "failed", None, None); } (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"cannot export file"}))).into_response() } }
}

async fn files_library(State(state): State<AppState>, Json(req): Json<LibraryRequestDto>) -> impl IntoResponse {
    let limit = req.limit.unwrap_or(200).clamp(1, 1000);
    let allowed = req.kinds.unwrap_or_default();
    let all = collect_tree(&state.files_root, "", 32).unwrap_or_default();
    let mut items = Vec::new();
    for entry in all.into_iter().filter(|e| !e.is_dir) {
        let ext = Path::new(&entry.path).extension().and_then(|x| x.to_str()).unwrap_or("").to_ascii_lowercase();
        let kind = if matches!(ext.as_str(), "png" | "jpg" | "jpeg" | "gif" | "webp" | "bmp") { "image" } else if matches!(ext.as_str(), "mp4" | "mov" | "mkv" | "avi") { "video" } else if matches!(ext.as_str(), "mp3" | "wav" | "m4a" | "flac") { "audio" } else { "document" }.to_string();
        if !allowed.is_empty() && !allowed.iter().any(|k| k == &kind) { continue; }
        let modified_ms = std::fs::metadata(state.files_root.join(&entry.path)).ok().and_then(|m| m.modified().ok()).and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()).map(|d| d.as_millis()).unwrap_or(0);
        items.push(LibraryItemDto { path: entry.path, kind, is_dir: false, size_bytes: entry.size_bytes, modified_ms });
    }
    items.sort_by(|a, b| b.modified_ms.cmp(&a.modified_ms).then_with(|| a.path.cmp(&b.path)));
    let truncated = items.len() > limit;
    items.truncate(limit);
    Json(LibraryResponseDto { items, truncated }).into_response()
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
    // P0-C: the bound Effect (planned at approval request)
    // gates the write: replay returns the prior Effect without
    // writing, an unsettled Effect refuses, a planned one is
    // consumed bound to it and driven forward.
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            let size = std::fs::metadata(
                state.files_root.join(
                    crate::operation_binding::normalize_path(&req.path).unwrap_or_default(),
                ),
            )
            .map(|m| m.len())
            .unwrap_or(0);
            return Json(serde_json::json!({ "path": req.path, "size_bytes": size, "effect_id": effect.effect_id, "replayed": true })).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.write", &req.approval_id, &digest, Some(&effect.effect_id)) {
            return *response;
        }
        if let Err(response) = start_bound_effect(&state, effect) {
            return *response;
        }
    } else if let Err(response) = consume_bound_approval(&state, "fs.write", &req.approval_id, &digest) {
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
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            }
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
        Ok(size) => {
            // P0-C: drive the bound Effect to its terminal
            // state; legacy approvals record one terminal row.
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "write".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            }
            Json(serde_json::json!({ "path": req.path, "size_bytes": size })).into_response()
        }
        Err(_) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "write".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot write file" }))).into_response()
        }
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
    // P0-C: bound Effect gates the delete (see files_write).
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            return Json(serde_json::json!({ "deleted": true, "effect_id": effect.effect_id, "replayed": true })).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.delete", &req.approval_id, &digest, Some(&effect.effect_id)) {
            return *response;
        }
        if let Err(response) = start_bound_effect(&state, effect) {
            return *response;
        }
    } else if let Err(response) = consume_bound_approval(&state, "fs.delete", &req.approval_id, &digest) {
        return *response;
    }
    let Some(normalized) = crate::operation_binding::normalize_path(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if full.is_dir() {
        if let Some(effect) = &bound_effect {
            let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
        }
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "delete refuses directories" }))).into_response();
    }
    match std::fs::remove_file(&full) {
        Ok(()) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "delete".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            }
            Json(serde_json::json!({ "deleted": true })).into_response()
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "delete".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "file not found" }))).into_response()
        }
        Err(_) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "delete".to_string(), scope: normalized, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot delete file" }))).into_response()
        }
    }
}

async fn files_rename(
    State(state): State<AppState>,
    Json(req): Json<RenameRequestDto>,
) -> impl IntoResponse {
    let Some(digest) = crate::operation_binding::rename_digest(&req.from, &req.to) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(from) = resolve_file(&state.files_root, &req.from) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(to) = resolve_file(&state.files_root, &req.to) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    if req.approval_id.is_none() {
        return (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": "bound approval required" }))).into_response();
    }
    if to.exists() {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "rename target already exists" }))).into_response();
    }
    // P0-C: bound Effect gates the rename (see files_write).
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            return Json(serde_json::json!({ "from": req.from, "to": req.to, "effect_id": effect.effect_id, "replayed": true })).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.rename", &req.approval_id, &digest, Some(&effect.effect_id)) {
            return *response;
        }
        if let Err(response) = start_bound_effect(&state, effect) {
            return *response;
        }
    } else if let Err(response) = consume_bound_approval(&state, "fs.rename", &req.approval_id, &digest) {
        return *response;
    }
    if !from.exists() {
        if let Some(effect) = &bound_effect {
            let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
        }
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "source file not found" }))).into_response();
    }
    if let Some(parent) = to.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            }
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create parent directory" }))).into_response();
        }
    }
    match std::fs::rename(&from, &to) {
        Ok(()) => {
            // P0-C: drive the bound Effect; the scope names both
            // ends, normalized, in order. Legacy approvals record
            // one terminal row.
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            } else {
                let scope = format!(
                    "{} -> {}",
                    crate::operation_binding::normalize_path(&req.from).unwrap_or_default(),
                    crate::operation_binding::normalize_path(&req.to).unwrap_or_default()
                );
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "rename".to_string(), scope, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            }
            Json(serde_json::json!({ "from": req.from, "to": req.to })).into_response()
        }
        Err(_) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                let scope = format!(
                    "{} -> {}",
                    crate::operation_binding::normalize_path(&req.from).unwrap_or_default(),
                    crate::operation_binding::normalize_path(&req.to).unwrap_or_default()
                );
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "rename".to_string(), scope, input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot rename file" }))).into_response()
        }
    }
}

async fn files_mkdir(
    State(state): State<AppState>,
    Json(req): Json<MkdirRequestDto>,
) -> impl IntoResponse {
    let Some(digest) = crate::operation_binding::mkdir_digest(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    // P0-C: bound Effect gates the mkdir (see files_write).
    let bound_effect = match bound_write_gate(&state, &req.approval_id, &digest) {
        Ok(WriteGate::Proceed(effect)) => effect,
        Ok(WriteGate::Replay(effect)) => {
            return Json(serde_json::json!({ "path": req.path, "effect_id": effect.effect_id, "replayed": true })).into_response();
        }
        Err(response) => return *response,
    };
    if let Some(effect) = &bound_effect {
        if let Err(response) = consume_bound_approval_for_effect(&state, "fs.mkdir", &req.approval_id, &digest, Some(&effect.effect_id)) {
            return *response;
        }
        if let Err(response) = start_bound_effect(&state, effect) {
            return *response;
        }
    } else if let Err(response) = consume_bound_approval(&state, "fs.mkdir", &req.approval_id, &digest) {
        return *response;
    }
    let Some(normalized) = crate::operation_binding::normalize_path(&req.path) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    let Ok(full) = resolve_file(&state.files_root, &normalized) else {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path escapes file root" }))).into_response();
    };
    match std::fs::create_dir_all(&full) {
        Ok(()) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "succeeded", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "mkdir".to_string(), scope: normalized.clone(), input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "succeeded".to_string() });
            }
            Json(serde_json::json!({ "path": normalized })).into_response()
        }
        Err(_) => {
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(&state, &effect.effect_id, "failed", None, None);
            } else {
                record_effect(&state, NewEffect { execution_id: Some(adapter_execution(&state)), capability: "fs".to_string(), operation: "mkdir".to_string(), scope: normalized.clone(), input_digest: Some(digest), approval_id: req.approval_id.clone(), status: "failed".to_string() });
            }
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "cannot create directory" }))).into_response()
        }
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
        DeepseekOutcome::Denied(pattern) => {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "network rule denied model call",
                    "rule": pattern,
                })),
            )
                .into_response();
        }
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
    to_path: Option<String>,
    content_base64: Option<String>,
    append: Option<bool>,
    message: Option<String>,
    /// P3-C (§7): a client-presented Worker command spec.
    /// Never trusted: the handler recomputes the spec from
    /// the raw presentation below and rejects a mismatch.
    command: Option<CommandSpec>,
    /// P3-C (§7): raw Worker command presentation (the
    /// server resolves the worktree + binary + allowlist and
    /// computes the digest + spec from these).
    worktree_id: Option<String>,
    command_template: Option<String>,
    params: Option<serde_json::Value>,
    timeout_ms: Option<u64>,
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
    // P3-C: a worker.run approval binds the resolved command
    // (argv + cwd + timeout); the presented CommandSpec, when
    // present, is a claim verified against the server-computed
    // spec, never trusted.
    let mut command_spec: Option<CommandSpec> = None;
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
        "fs.rename" => {
            let (Some(from), Some(to)) = (req.path.as_deref(), req.to_path.as_deref()) else {
                return bad_request("fs.rename approval requires path and to_path");
            };
            match crate::operation_binding::rename_digest(from, to) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid path"),
            }
        }
        "fs.mkdir" => {
            let Some(path) = req.path.as_deref() else {
                return bad_request("fs.mkdir approval requires path");
            };
            match crate::operation_binding::mkdir_digest(path) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid path"),
            }
        }
        "fs.export" => {
            let (Some(from), Some(to)) = (req.path.as_deref(), req.to_path.as_deref()) else {
                return bad_request("fs.export approval requires path and to_path");
            };
            match crate::operation_binding::export_digest(from, to) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid export path"),
            }
        }
        "worker.run" => {
            let (Some(worktree_id), Some(command_template)) =
                (req.worktree_id.as_deref(), req.command_template.as_deref())
            else {
                return bad_request(
                    "worker.run approval requires worktree_id and command_template",
                );
            };
            // Flat args for the shared resolver: the raw
            // presentation, never a client hash.
            let mut flat = std::collections::BTreeMap::new();
            flat.insert("worktree_id".to_string(), worktree_id.to_string());
            flat.insert("command".to_string(), command_template.to_string());
            if let Some(timeout) = req.timeout_ms {
                flat.insert("timeout_ms".to_string(), timeout.to_string());
            }
            if let Some(serde_json::Value::Object(map)) = req.params.as_ref() {
                for (key, value) in map {
                    if let serde_json::Value::String(s) = value {
                        flat.insert(key.clone(), s.clone());
                    }
                }
            }
            let (digest, spec) = match worker_command_digest(&state, &flat) {
                Ok(pair) => pair,
                Err(err) => {
                    return (
                        err.status,
                        Json(serde_json::json!({ "error": err.code })),
                    )
                        .into_response();
                }
            };
            if let Some(presented) = &req.command {
                if presented != &spec {
                    return bad_request("command_mismatch");
                }
            }
            command_spec = Some(spec);
            Some(digest)
        }
        _ => None,
    };
    let mut next = state.next_approval_id.lock().expect("approval id lock");
    let id = format!("approval-{}", *next);
    *next += 1;
    let record = ApprovalState { tool: req.tool.clone(), status: ApprovalStatus::Pending, binding: None, consumed: false, operation_digest: operation_digest.clone(), action_binding: None, effect_binding: None, message: req.message.clone(), command: command_spec, consumed_by_worker_session_id: None };
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
    // The insert guard must be released before the planned
    // Effect below takes the approvals lock again for the
    // binding: std Mutexes are not reentrant, and holding
    // this guard across the second lock would deadlock.
    drop(approvals);
    // P0-C: a digest-bound approval request IS the plan for
    // its write, so the Effect is created here as `planned`
    // and the approval is bound to it (`effect_id +
    // input_digest`). The write handler later drives it
    // `planned -> authorized -> started -> terminal` instead
    // of recording a fresh row. A rejected or never-used
    // approval leaves its planned row planned: that is the
    // honest record (planned, never authorized), not a missing
    // Effect. Binding persistence is best-effort here because
    // the approval is already issued; the write handler can
    // still resolve the planned row through `approval_id` as
    // a fallback, so a lost binding line cannot authorize a
    // different write (the digest check still applies).
    if let Some(digest) = &operation_digest {
        let planned = match req.tool.as_str() {
            "fs.write" => req.path.as_deref().and_then(|p| {
                crate::operation_binding::normalize_path(p)
                    .map(|scope| ("fs", "write", scope))
            }),
            "fs.delete" => req.path.as_deref().and_then(|p| {
                crate::operation_binding::normalize_path(p)
                    .map(|scope| ("fs", "delete", scope))
            }),
            "fs.rename" => match (req.path.as_deref(), req.to_path.as_deref()) {
                (Some(from), Some(to)) => {
                    match (
                        crate::operation_binding::normalize_path(from),
                        crate::operation_binding::normalize_path(to),
                    ) {
                        (Some(from), Some(to)) => {
                            Some(("fs", "rename", format!("{from} -> {to}")))
                        }
                        _ => None,
                    }
                }
                _ => None,
            },
            "fs.mkdir" => req.path.as_deref().and_then(|p| {
                crate::operation_binding::normalize_path(p)
                    .map(|scope| ("fs", "mkdir", scope))
            }),
            "fs.export" => req.path.as_deref().and_then(|p| {
                crate::operation_binding::normalize_path(p)
                    .map(|scope| ("fs", "export", scope))
            }),
            // P3-C: worker.run is digest-only — no planned
            // Effect. The driver records the completion Effect
            // fresh at run completion (its digest needs
            // post-command observations that do not exist yet).
            _ => None,
        };
        if let Some((capability, operation, scope)) = planned {
            let effect = record_effect(
                &state,
                NewEffect {
                    execution_id: Some(adapter_execution(&state)),
                    capability: capability.to_string(),
                    operation: operation.to_string(),
                    scope,
                    input_digest: Some(digest.clone()),
                    approval_id: Some(id.clone()),
                    status: "planned".to_string(),
                },
            );
            let mut approvals = state.approvals.lock().expect("approval lock");
            if let Some(approval) = approvals.get_mut(&id) {
                approval.effect_binding = Some(effect.effect_id.clone());
                let bound = approval.clone();
                drop(approvals);
                if let Err(detail) = persist_approval(&state, &id, &bound) {
                    eprintln!("warning: approval effect binding persistence failed: {detail}");
                }
            }
        }
    }
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
    // P3-B window fix (U3): approving an already-approved,
    // unconsumed approval is an idempotent no-op, not an error.
    // A UI that decides-then-applies (PatchesView's "approve and
    // apply") otherwise cannot apply a file whose approval was
    // already decided elsewhere: the repeated decide 409s and the
    // apply step is never reached. Reject-on-approved and any
    // decide on a consumed or rejected record still 409 below;
    // the apply command remains the real approval gate.
    // D1-2 (Q-D1-24): the no-op and the single-decision guard
    // below now apply to unbound records too — a decision is
    // final whoever the record is bound to. The one exception
    // is an unbound record in Edited status: it was refined
    // via `approval_edit_message` (a pre-decision edit, not a
    // decision) and is still decidable. Bound records keep the
    // old shape: deciding Edited on them still 409s.
    if status == ApprovalStatus::Approved
        && record.status == ApprovalStatus::Approved
        && !record.consumed
    {
        let current = req.status.clone();
        let tool_call_id = req.tool_call_id.clone();
        drop(approvals);
        return Json(serde_json::json!({ "tool_call_id": tool_call_id, "status": current })).into_response();
    }
    let already_decided = record.consumed
        || (record.status != ApprovalStatus::Pending
            && !(record.status == ApprovalStatus::Edited && !bound));
    let edited_via_decide = bound && status == ApprovalStatus::Edited;
    if already_decided || edited_via_decide {
        return (StatusCode::CONFLICT, Json(serde_json::json!({"error":"loop_approval_already_decided_or_invalid"}))).into_response();
    }
    record.status = status;
    if let Err(detail) = persist_approval(&state, &req.tool_call_id, record) {
        if bound { record.status = ApprovalStatus::Pending; return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"approval_audit_failed"}))).into_response(); }
        eprintln!("warning: approval persistence failed: {detail}");
    }
    // P3-B (§4.5): a rejected approval rejects its PatchFile.
    // Runs after the approvals lock is released (the linkage
    // only touches SQLite).
    let rejected = status == ApprovalStatus::Rejected;
    let tool_call_id = req.tool_call_id.clone();
    drop(approvals);
    if rejected {
        link_patch_file_rejected_on_approval(&state, &tool_call_id);
    }
    Json(serde_json::json!({ "tool_call_id": req.tool_call_id, "status": req.status })).into_response()
}

// --- Egress approval query surface (Task 7) ---
//
// Local semantics only (route schemas are unknown; see the Task 7
// plan): views follow the local JSONL record shape, history is the
// persisted event lines in file order, and edit-message mirrors
// decide-edited: only a pending, unconsumed, unbound record may be
// edited, and a successful edit moves it to `edited`.

#[derive(Debug, Serialize)]
struct ApprovalView {
    approval_id: String,
    /// Local alias: existing clients name approval ids tool_call_id.
    tool_call_id: String,
    tool: String,
    status: String,
    consumed: bool,
    bound: bool,
    binding: Option<ApprovalBinding>,
    operation_digest: Option<String>,
    effect_binding: Option<String>,
    message: Option<String>,
    /// E2-A §2.7: the worker session that consumed this
    /// approval to create a run. Null when unconsumed.
    consumed_by_worker_session_id: Option<String>,
    /// D1-2 (Q-D1-24): the approval version (SQLite mode
    /// only; the in-memory map carries no version, so this
    /// is null in JSONL/shadow views). Null for unversioned
    /// rows.
    version: Option<u64>,
}

fn approval_number(approval_id: &str) -> u64 {
    approval_id.strip_prefix("approval-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

fn approval_view(approval_id: &str, record: &ApprovalState) -> ApprovalView {
    ApprovalView {
        approval_id: approval_id.to_string(),
        tool_call_id: approval_id.to_string(),
        tool: record.tool.clone(),
        status: approval_status_name(record.status).to_string(),
        consumed: record.consumed,
        bound: record.is_bound(),
        binding: record.binding.clone(),
        operation_digest: record.operation_digest.clone(),
        effect_binding: record.effect_binding.clone(),
        message: record.message.clone(),
        consumed_by_worker_session_id: record.consumed_by_worker_session_id.clone(),
        version: None,
    }
}

fn approval_view_from_file(record: &ApprovalFileRecord) -> ApprovalView {
    ApprovalView {
        approval_id: record.tool_call_id.clone(),
        tool_call_id: record.tool_call_id.clone(),
        tool: record.tool.clone(),
        status: record.status.clone(),
        consumed: record.consumed,
        bound: record.binding.is_some()
            || record.operation_digest.is_some()
            || record.effect_binding.is_some(),
        binding: record.binding.clone(),
        operation_digest: record.operation_digest.clone(),
        effect_binding: record.effect_binding.clone(),
        message: record.message.clone(),
        consumed_by_worker_session_id: record.consumed_by_worker_session_id.clone(),
        version: record.version,
    }
}

fn unknown_approval() -> axum::response::Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown approval_id" }))).into_response()
}

fn approval_editable(record: &ApprovalState) -> bool {
    record.status == ApprovalStatus::Pending && !record.consumed && !record.is_bound()
}

/// Approval views from the in-memory map (JSONL / shadow mode,
/// and the gate paths that mutate under the approvals lock).
fn approval_views_from_map(state: &AppState) -> Vec<ApprovalView> {
    let approvals = state.approvals.lock().expect("approval lock");
    let mut views: Vec<ApprovalView> =
        approvals.iter().map(|(id, record)| approval_view(id, record)).collect();
    views.sort_by_key(|view| approval_number(&view.approval_id));
    views
}

async fn approvals_list(State(state): State<AppState>) -> impl IntoResponse {
    // P0-D: SQLite mode reads SQLite; a broken database fails
    // closed (5xx) instead of serving a stale map.
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return sqlite_error_response();
        }
        let Some(sqlite) = &state.sqlite else {
            return sqlite_error_response();
        };
        let conn = sqlite.lock().expect("sqlite lock");
        return match sqlite_store::list_approvals(&conn) {
            Ok(rows) => {
                let mut views: Vec<ApprovalView> =
                    rows.iter().map(approval_view_from_file).collect();
                views.sort_by_key(|view| approval_number(&view.approval_id));
                Json(views).into_response()
            }
            Err(_) => sqlite_error_response(),
        };
    }
    Json(approval_views_from_map(&state)).into_response()
}

async fn approvals_history(State(state): State<AppState>) -> impl IntoResponse {
    // P0-D: in SQLite mode history is the approval event log in
    // global order (each payload is a full approval row), not
    // the generated export file (which holds one snapshot row
    // per approval). A broken database fails closed.
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return sqlite_error_response();
        }
        let Some(sqlite) = &state.sqlite else {
            return sqlite_error_response();
        };
        let conn = sqlite.lock().expect("sqlite lock");
        return match sqlite_store::list_events(&conn) {
            Ok(events) => {
                let mut views = Vec::new();
                for event in events {
                    if event.aggregate_kind != "approval" {
                        continue;
                    }
                    if let Ok(record) =
                        serde_json::from_str::<ApprovalFileRecord>(&event.payload_json)
                    {
                        views.push(approval_view_from_file(&record));
                    }
                }
                Json(views).into_response()
            }
            Err(_) => sqlite_error_response(),
        };
    }
    if let Some(path) = &state.approvals_path {
        let views: Vec<ApprovalView> =
            load_records(path).iter().map(approval_view_from_file).collect();
        return Json(views).into_response();
    }
    // No persistence configured: history degrades to the current
    // state, one entry per approval.
    Json(approval_views_from_map(&state)).into_response()
}

async fn approvals_get(
    State(state): State<AppState>,
    axum::extract::Path(approval_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return sqlite_error_response();
        }
        let Some(sqlite) = &state.sqlite else {
            return sqlite_error_response();
        };
        let conn = sqlite.lock().expect("sqlite lock");
        return match sqlite_store::get_approval(&conn, &approval_id) {
            Ok(Some(record)) => Json(approval_view_from_file(&record)).into_response(),
            Ok(None) => unknown_approval(),
            Err(_) => sqlite_error_response(),
        };
    }
    let approvals = state.approvals.lock().expect("approval lock");
    match approvals.get(&approval_id) {
        Some(record) => Json(approval_view(&approval_id, record)).into_response(),
        None => unknown_approval(),
    }
}

async fn approval_editable_message(
    State(state): State<AppState>,
    axum::extract::Path(approval_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let approvals = state.approvals.lock().expect("approval lock");
    match approvals.get(&approval_id) {
        Some(record) => Json(serde_json::json!({
            "approval_id": approval_id,
            "message": record.message,
            "editable": approval_editable(record),
        }))
        .into_response(),
        None => unknown_approval(),
    }
}

#[derive(Debug, Deserialize)]
struct EditMessageDto {
    message: Option<String>,
}

async fn approval_edit_message(
    State(state): State<AppState>,
    axum::extract::Path(approval_id): axum::extract::Path<String>,
    Json(req): Json<EditMessageDto>,
) -> impl IntoResponse {
    let Some(message) = req.message.filter(|m| !m.trim().is_empty()) else {
        return bad_request("message must not be empty");
    };
    let mut approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get_mut(&approval_id) else {
        return unknown_approval();
    };
    if !approval_editable(record) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "approval_not_editable" }))).into_response();
    }
    let old_message = record.message.clone();
    let old_status = record.status;
    record.message = Some(message);
    record.status = ApprovalStatus::Edited;
    let view = approval_view(&approval_id, record);
    if persist_approval(&state, &approval_id, record).is_err() {
        // Fail closed like approval_decide: an edit that cannot be
        // audited must not take effect.
        record.message = old_message;
        record.status = old_status;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "approval_audit_failed" }))).into_response();
    }
    Json(view).into_response()
}

/// Append one approval record to the persistence file, when
/// configured. Best-effort: a failure is logged to stderr and
/// the in-memory record (authoritative for this run) is kept.
/// P0-D: in shadow mode the SQLite upsert is best-effort too
/// (warn + count, never changes the Result); in SQLite mode
/// the SQLite upsert IS the persist (JSONL becomes the audit
/// export) and a failure returns Err, so the existing call
/// sites fail closed exactly as they did for a JSONL failure.
pub(crate) fn persist_approval(state: &AppState, tool_call_id: &str, approval: &ApprovalState) -> Result<(), String> {
    let record = ApprovalFileRecord {
        tool_call_id: tool_call_id.to_string(),
        tool: approval.tool.clone(),
        status: approval_status_name(approval.status).to_string(),
        binding: approval.binding.clone(),
        consumed: approval.consumed,
        operation_digest: approval.operation_digest.clone(),
        action_binding: approval.action_binding.clone(),
        effect_binding: approval.effect_binding.clone(),
        message: approval.message.clone(),
        command: approval.command.clone(),
        consumed_by_worker_session_id: approval.consumed_by_worker_session_id.clone(),
        // D1-2 (Q-D1-24): the version is computed inside
        // `upsert_approval_tx` (from the stored row), never
        // by the caller — the in-memory map carries no
        // version, so None here means "let the transaction
        // decide". In JSONL mode the record stays
        // unversioned (no version field is written).
        version: None,
        supersedes_version: None,
    };
    match state.sqlite_mode() {
        StateBackend::Sqlite => {
            let Some(sqlite) = &state.sqlite else {
                *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
                return Err("sqlite unavailable".to_string());
            };
            let mut conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::upsert_approval(&mut conn, &record) {
                Ok(()) => {
                    if let Some(path) = &state.approvals_path {
                        if let Err(detail) = sqlite_store::export_approvals(&conn, path) {
                            eprintln!("warning: P0-D approvals audit export failed: {detail}");
                        }
                    }
                    Ok(())
                }
                Err(detail) => {
                    *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
                    Err(detail)
                }
            }
        }
        StateBackend::Shadow => {
            if let Some(path) = &state.approvals_path {
                append_record(path, &record)?;
            }
            if let Some(sqlite) = &state.sqlite {
                let mut conn = sqlite.lock().expect("sqlite lock");
                if let Err(detail) = sqlite_store::upsert_approval(&mut conn, &record) {
                    *state.sqlite_shadow_failures.lock().expect("shadow failures lock") += 1;
                    eprintln!("warning: P0-D shadow approval write failed: {detail}");
                }
            }
            Ok(())
        }
        StateBackend::Jsonl => {
            let Some(path) = &state.approvals_path else { return Ok(()) };
            append_record(path, &record)
        }
    }
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
    drop(approvals);
    // P3-B (§4.5): a rejected approval rejects its PatchFile
    // (task artifact approvals carry no PatchFile; the
    // linkage is a no-op for them).
    link_patch_file_rejected_on_approval(state, approval_id);
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
    consume_bound_approval_for_effect(state, tool, tool_call_id, digest, None)
}

/// P0-C digest gate bound to an Effect (contract §5). Lookup,
/// tool match, status, digest match, Effect match and
/// consumption happen under a single approvals-lock acquisition
/// (the Effect itself is read under the effects lock, in the
/// approvals-before-effects order), so concurrent executions of
/// one approval serialize and only the first consumes it. When
/// `effect_id` is given, the approval must either already be
/// bound to that Effect or be unbound (in which case it is bound
/// here, at first consume, for approvals planned elsewhere such
/// as task execute); a binding to a different Effect is a
/// mismatch, and the bound Effect must carry the same digest
/// and sit at `planned` (or already `authorized`). Consumption
/// moves the Effect to `authorized`. A persistence failure
/// returns 500 without executing, and the in-memory consumed
/// flag stays set (fail closed, as in the loop path). With
/// `effect_id` None this is the legacy gate, unchanged: it
/// binds by digest only and moves no Effect.
pub(crate) fn consume_bound_approval_for_effect(
    state: &AppState,
    tool: &str,
    tool_call_id: &Option<String>,
    digest: &str,
    effect_id: Option<&str>,
) -> Result<(), Box<axum::response::Response>> {
    let forbidden = |error: &str| {
        Box::new(
            (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": error }))).into_response(),
        )
    };
    let internal = |error: &str| {
        Box::new(
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": error }))).into_response(),
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
    if let Some(effect_id) = effect_id {
        if let Some(bound) = &record.effect_binding {
            if bound != effect_id {
                return Err(forbidden("approval_binding_mismatch"));
            }
        }
        // Effect existence/digest/state, checked before the
        // approval is burned so a mismatched Effect never
        // consumes it.
        match effect_by_id(state, effect_id) {
            Some(effect)
                if effect.input_digest.as_deref() == Some(digest)
                    && (effect.status == "planned" || effect.status == "authorized") => {}
            _ => return Err(forbidden("approval_binding_mismatch")),
        }
        record.effect_binding = Some(effect_id.to_string());
    }
    record.consumed = true;
    if let Err(detail) = persist_approval(state, id, record) {
        eprintln!("warning: bound approval consumption persistence failed: {detail}");
        return Err(Box::new(
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "approval_audit_failed" }))).into_response(),
        ));
    }
    if let Some(effect_id) = effect_id {
        if effect_by_id(state, effect_id).is_some_and(|e| e.status == "planned")
            && transition_effect(state, effect_id, "authorized", None, None).is_err()
        {
            return Err(internal("effect_transition_failed"));
        }
    }
    Ok(())
}

/// Phase 2 (Q4) device approval checks, shared by the peek and
/// the consume so both agree. Loop-bound records can never pass
/// (a loop binding authorizes one loop call, not a device action);
/// digest-bound records can never pass either (their binding is a
/// parameter digest, and the device gate binds by action id, not
/// by digest — a record carrying both would be ambiguous, and the
/// request path never creates one). An already-bound or already-
/// consumed record is a reuse, not a fresh authorization.
fn device_approval_error(record: &ApprovalState, action_id: &str) -> Option<&'static str> {
    if record.binding.is_some() || record.operation_digest.is_some() {
        return Some("approval_binding_mismatch");
    }
    if record.tool != DEVICE_ACTION {
        return Some("approval_required");
    }
    if record.status == ApprovalStatus::Rejected {
        return Some("approval_rejected");
    }
    // Edited stays executable, as in may_execute: the edit flow
    // leaves the record in a directly executable state, and the
    // old status-only device gate already accepted it. Requiring
    // exactly Approved here would newly reject Edited device
    // approvals, a behavior regression, not a convergence.
    if record.status != ApprovalStatus::Approved && record.status != ApprovalStatus::Edited {
        return Some("approval_required");
    }
    if record.action_binding.as_ref().is_some_and(|bound| bound != action_id) || record.consumed {
        return Some("approval_binding_mismatch");
    }
    None
}

/// Phase 2 (Q4) device peek: validates without burning, so a
/// device_set that fails on the action side (unknown action, not
/// requested) never consumes the approval, and approval errors
/// keep their old precedence over action errors.
fn check_device_approval(
    state: &AppState,
    tool_call_id: &Option<String>,
    action_id: &str,
) -> Result<(), Box<axum::response::Response>> {
    let forbidden = |error: &str| {
        Box::new(
            (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": error }))).into_response(),
        )
    };
    let Some(id) = tool_call_id else {
        return Err(forbidden("approval_required"));
    };
    let approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get(id) else {
        return Err(forbidden("approval_required"));
    };
    match device_approval_error(record, action_id) {
        Some(error) => Err(forbidden(error)),
        None => Ok(()),
    }
}

/// Phase 2 (Q4) device gate: bind-on-consume. The first device use
/// of an approval binds it to that action id and consumes it, in
/// one lock acquisition, so a second use — for the same action or
/// another — fails with a binding mismatch. A persistence failure
/// returns 500 without executing, and the in-memory consumed flag
/// stays set (fail closed, as in the loop and digest paths).
/// P0-C device gate: the approval authorizes an Effect
/// (`effect_id + input_digest`), not the action record. When
/// `effect_id` is given, the approval must either already be
/// bound to that Effect or be unbound (bound here, at first
/// consume); a binding to a different Effect, or an Effect
/// carrying a different digest, is a mismatch, checked before
/// the approval is burned. Consumption moves the Effect to
/// `authorized`. `Approved | Edited` both execute, as in
/// `may_execute`. The legacy `action_binding` is still recorded
/// alongside, so phase-2 readers keep working. With
/// `effect_id` None (a pre-P0-C action with no planned Effect)
/// this is the legacy gate, unchanged. A persistence failure
/// returns 500 without executing, and the in-memory consumed
/// flag stays set (fail closed, as in the loop and digest
/// paths).
pub(crate) fn consume_device_approval_for_effect(
    state: &AppState,
    tool_call_id: &Option<String>,
    action_id: &str,
    effect_id: Option<&str>,
    digest: Option<&str>,
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
    if let Some(error) = device_approval_error(record, action_id) {
        return Err(forbidden(error));
    }
    if let Some(effect_id) = effect_id {
        if let Some(bound) = &record.effect_binding {
            if bound != effect_id {
                return Err(forbidden("approval_binding_mismatch"));
            }
        }
        match effect_by_id(state, effect_id) {
            Some(effect)
                if digest.is_none_or(|d| effect.input_digest.as_deref() == Some(d))
                    && (effect.status == "planned" || effect.status == "authorized") => {}
            _ => return Err(forbidden("approval_binding_mismatch")),
        }
        record.effect_binding = Some(effect_id.to_string());
    }
    record.action_binding = Some(action_id.to_string());
    record.consumed = true;
    if let Err(detail) = persist_approval(state, id, record) {
        eprintln!("warning: device approval consumption persistence failed: {detail}");
        return Err(Box::new(
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "approval_audit_failed" }))).into_response(),
        ));
    }
    if let Some(effect_id) = effect_id {
        if effect_by_id(state, effect_id).is_some_and(|e| e.status == "planned")
            && transition_effect(state, effect_id, "authorized", None, None).is_err()
        {
            return Err(Box::new(
                (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "effect_transition_failed" }))).into_response(),
            ));
        }
    }
    Ok(())
}

/// P0-C shared gate for approval-bound writes (files, agent
/// tool calls, task execute). Resolves the Effect the approval
/// authorizes and decides, before any consumption or write:
/// - `Proceed(Some(effect))`: the Effect is `planned` or
///   `authorized`; the caller consumes the approval bound to
///   it and drives it forward.
/// - `Proceed(None)`: no planned Effect exists (a legacy
///   approval); the caller uses the legacy gate and records a
///   terminal row directly.
/// - `Replay(effect)`: the write already reached a terminal
///   Effect (its own, or another Effect with the same
///   idempotency key): return the prior Effect, write nothing,
///   consume nothing.
///
/// An unsettled Effect (`unknown`, `reconciling`, in flight,
/// or a legacy row that cannot be driven) refuses with a 409
/// error response instead of a gate value: write nothing,
/// consume nothing.
/// A digest mismatch against a bound Effect is a 403, never a
/// fallback to the legacy gate: a bound approval can never
/// authorize different parameters.
pub(crate) enum WriteGate {
    Proceed(Option<EffectFileRecord>),
    Replay(EffectFileRecord),
}

pub(crate) fn bound_write_gate(
    state: &AppState,
    tool_call_id: &Option<String>,
    digest: &str,
) -> Result<WriteGate, Box<axum::response::Response>> {
    let forbidden = |error: &str| {
        Box::new(
            (StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": error }))).into_response(),
        )
    };
    let conflict = |error: &'static str, effect: &EffectFileRecord| {
        Box::new(
            (StatusCode::CONFLICT, Json(serde_json::json!({ "error": error, "effect_id": effect.effect_id, "status": effect.status }))).into_response(),
        )
    };
    let Some(id) = tool_call_id else {
        return Ok(WriteGate::Proceed(None));
    };
    let bound_id = {
        let approvals = state.approvals.lock().expect("approval lock");
        approvals.get(id).and_then(|r| r.effect_binding.clone())
    };
    match effect_for_approval(state, id, Some(digest)) {
        Some(effect) => match effect.status.as_str() {
            "planned" | "authorized" => {
                // E2-A §2.4: a bound Effect without an execution
                // id or digest has no key to check — refuse with
                // 409, not the store-broken 500 below.
                if idempotency_key_missing(
                    effect.execution_id.as_deref(),
                    effect.input_digest.as_deref(),
                ) {
                    return Err(Box::new(
                        (
                            StatusCode::CONFLICT,
                            Json(serde_json::json!({ "error": "effect_idempotency_unavailable" })),
                        )
                            .into_response(),
                    ));
                }
                match check_idempotency(
                    state,
                    effect.execution_id.as_deref(),
                    &effect.capability,
                    &effect.operation,
                    &effect.scope,
                    effect.input_digest.as_deref(),
                    Some(&effect.effect_id),
                ) {
                    IdempotencyDecision::Proceed => Ok(WriteGate::Proceed(Some(effect))),
                    IdempotencyDecision::Replay(other) => Ok(WriteGate::Replay(other)),
                    IdempotencyDecision::Blocked(other) => {
                        Err(conflict("effect_idempotency_blocked", &other))
                    }
                    // P0-D: broken migrated-domain store fails
                    // the bound write closed (5xx, not a guess).
                    IdempotencyDecision::Unavailable => Err(Box::new(
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(serde_json::json!({ "error": "sqlite_unavailable" })),
                        )
                            .into_response(),
                    )),
                }
            }
            "succeeded" | "failed" | "reconciled" => Ok(WriteGate::Replay(effect)),
            "unknown" => Err(conflict("effect_unknown", &effect)),
            "reconciling" => Err(conflict("effect_reconciling", &effect)),
            "started" => Err(conflict("effect_in_flight", &effect)),
            _ => Err(conflict("effect_legacy", &effect)),
        },
        None => {
            if let Some(bound_id) = bound_id {
                // Bound to an Effect that exists but carries a
                // different digest: tampering, fail closed, never
                // fall back to the digest-only gate.
                if effect_by_id(state, &bound_id).is_some() {
                    return Err(forbidden("approval_binding_mismatch"));
                }
                // Bound to an Effect that is missing entirely:
                // the Effects store was not persisted across a
                // restart (approvals persist independently), so
                // the planned row did not survive. Fall back to
                // the legacy digest gate, which still checks
                // tool, status, digest and single use: the
                // restart guarantee (an approved, unconsumed
                // approval executes exactly once after reload)
                // outranks attribution to a lost row, and the
                // caller records a fresh terminal row.
                return Ok(WriteGate::Proceed(None));
            }
            Ok(WriteGate::Proceed(None))
        }
    }
}

/// Drive a bound Effect from `authorized` to `started` after
/// consumption. Returns an error response on failure (the
/// approval is already consumed at this point: fail closed,
/// write nothing, and let the operator see the stuck Effect).
fn start_bound_effect(
    state: &AppState,
    effect: &EffectFileRecord,
) -> Result<(), Box<axum::response::Response>> {
    let current = effect_by_id(state, &effect.effect_id);
    match current.as_ref().map(|e| e.status.as_str()) {
        Some("authorized") => transition_effect(state, &effect.effect_id, "started", None, None)
            .map(|_| ())
            .map_err(|_| {
                Box::new(
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "effect_transition_failed" }))).into_response(),
                )
            }),
        Some("planned") => transition_effect_chain(state, &effect.effect_id, &["authorized", "started"])
            .map(|_| ())
            .map_err(|_| {
                Box::new(
                    (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "effect_transition_failed" }))).into_response(),
                )
            }),
        _ => Err(Box::new(
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "effect_transition_failed" }))).into_response(),
        )),
    }
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
            // P0-C: resolve the bound Effect before consuming.
            // A terminal Effect replays without dispatching; an
            // unsettled one refuses; a planned/authorized one is
            // consumed bound to it and driven forward. Legacy
            // approvals with no planned row keep the old gate
            // and terminal-row recording.
            let gate = match bound_write_gate(&state, &req.tool_call_id, &digest) {
                Ok(gate) => gate,
                Err(response) => return *response,
            };
            let bound_effect = match gate {
                WriteGate::Replay(effect) => {
                    return Json(serde_json::json!({
                        "result": "replayed",
                        "effect_id": effect.effect_id,
                        "replayed": true,
                    }))
                    .into_response();
                }
                WriteGate::Proceed(effect) => effect,
            };
            if let Some(effect) = &bound_effect {
                if let Err(response) = consume_bound_approval_for_effect(
                    &state,
                    &req.tool,
                    &req.tool_call_id,
                    &digest,
                    Some(&effect.effect_id),
                ) {
                    return *response;
                }
                if let Err(response) = start_bound_effect(&state, effect) {
                    return *response;
                }
            } else if let Err(response) =
                consume_bound_approval(&state, &req.tool, &req.tool_call_id, &digest)
            {
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
            let scope = req.args.get("path").cloned().unwrap_or_default();
            let operation = match req.tool.as_str() {
                "fs.write" => "write",
                _ => "delete",
            };
            let approval_id = req.tool_call_id.clone();
            let response = dispatch_agent_tool(&state, req);
            let succeeded = response.status().is_success();
            if let Some(effect) = &bound_effect {
                let _ = transition_effect(
                    &state,
                    &effect.effect_id,
                    if succeeded { "succeeded" } else { "failed" },
                    None,
                    None,
                );
            } else if succeeded {
                // Legacy fallback: one terminal row, new word.
                record_effect(
                    &state,
                    NewEffect {
                        execution_id: Some(adapter_execution(&state)),
                        capability: "fs".to_string(),
                        operation: operation.to_string(),
                        scope,
                        input_digest: Some(digest),
                        approval_id,
                        status: "succeeded".to_string(),
                    },
                );
            }
            return response;
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

// --- Connector registry (Task 9) ---
//
// The registry is static on purpose: it lists exactly the two
// Mock connectors this gateway implements, nothing invented.
// Accounts and connection flags are local state (see
// connector_store.rs); there is no OAuth, no real account,
// and no external redirect anywhere in this surface. The
// notes/mail data routes above are untouched by it.

struct ConnectorEntry {
    id: &'static str,
    name: &'static str,
    capabilities: &'static [&'static str],
}

const CONNECTOR_REGISTRY: &[ConnectorEntry] = &[
    ConnectorEntry { id: "notes", name: "Mock Notes", capabilities: &["notes.list", "notes.create"] },
    ConnectorEntry { id: "mail", name: "Mock Mail", capabilities: &["mail.list", "mail.send"] },
];

fn registry_entry(id: &str) -> Option<&'static ConnectorEntry> {
    CONNECTOR_REGISTRY.iter().find(|e| e.id == id)
}

#[derive(Debug, Serialize)]
struct ConnectorView {
    id: String,
    name: String,
    capabilities: Vec<String>,
    connected: bool,
}

#[derive(Debug, Serialize)]
struct ConnectorAccountView {
    account_id: String,
    connector_id: String,
    display_name: String,
    status: String,
    linked_ms: u64,
}

fn connector_view(state: &AppState, entry: &'static ConnectorEntry) -> ConnectorView {
    let connected = state
        .connector_connections
        .lock()
        .expect("connector connections lock")
        .get(entry.id)
        .copied()
        .unwrap_or(false);
    ConnectorView {
        id: entry.id.to_string(),
        name: entry.name.to_string(),
        capabilities: entry.capabilities.iter().map(|c| c.to_string()).collect(),
        connected,
    }
}

fn account_view(account: &ConnectorAccount) -> ConnectorAccountView {
    ConnectorAccountView {
        account_id: account.account_id.clone(),
        connector_id: account.connector_id.clone(),
        display_name: account.display_name.clone(),
        status: if account.unlinked { "unlinked".to_string() } else { "linked".to_string() },
        linked_ms: account.linked_ms,
    }
}

fn persist_connector_line(state: &AppState, line: &ConnectorStateLine) {
    let Some(path) = &state.connector_state_path else { return };
    append_connector_state(path, line);
}

fn unknown_connector() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown connector" }))).into_response()
}

async fn connectors_list(State(state): State<AppState>) -> Json<Vec<ConnectorView>> {
    Json(CONNECTOR_REGISTRY.iter().map(|e| connector_view(&state, e)).collect())
}

async fn connectors_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match registry_entry(&id) {
        Some(entry) => Json(connector_view(&state, entry)).into_response(),
        None => unknown_connector(),
    }
}

async fn connectors_accounts(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let Some(_) = registry_entry(&id) else {
        return unknown_connector();
    };
    let mut accounts: Vec<ConnectorAccountView> = state
        .connector_accounts
        .lock()
        .expect("connector accounts lock")
        .values()
        .filter(|a| a.connector_id == id && !a.unlinked)
        .map(account_view)
        .collect();
    accounts.sort_by_key(|a| account_number(&a.account_id));
    Json(accounts).into_response()
}

#[derive(Debug, Deserialize)]
struct AccountLinkDto {
    display_name: Option<String>,
}

async fn connectors_accounts_link(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(req): Json<AccountLinkDto>,
) -> impl IntoResponse {
    let Some(_) = registry_entry(&id) else {
        return unknown_connector();
    };
    let display_name = req
        .display_name
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Mock account".to_string());
    let mut next = state.next_account_id.lock().expect("account id lock");
    let account = ConnectorAccount {
        account_id: format!("account-{}", *next),
        connector_id: id,
        display_name,
        linked_ms: now_ms(),
        unlinked: false,
    };
    *next += 1;
    state
        .connector_accounts
        .lock()
        .expect("connector accounts lock")
        .insert(account.account_id.clone(), account.clone());
    persist_connector_line(&state, &ConnectorStateLine::Account(account.clone()));
    // Phase 2: a successful link mirrors into an Effect.
    record_connector_effect(&state, None, "connectors", "link", &account.account_id);
    Json(account_view(&account)).into_response()
}

async fn connectors_accounts_unlink(
    State(state): State<AppState>,
    axum::extract::Path((id, account_id)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    let Some(_) = registry_entry(&id) else {
        return unknown_connector();
    };
    // Validate first (404s keep their exact shape), then gate,
    // then mutate — the gate reads the Effects store with no
    // locks held, matching the P0-R discipline below.
    let account = {
        let accounts = state.connector_accounts.lock().expect("connector accounts lock");
        let Some(account) = accounts.get(&account_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown account" }))).into_response();
        };
        if account.connector_id != id {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown account" }))).into_response();
        }
        account.clone()
    };
    // E2-A §2.4: a repeat unlink with no intervening write is a
    // replay — return the current view, append no second
    // tombstone, record no second Effect row. (The old comment
    // below about "even when idempotent" described the
    // pre-gate behaviour: the first unlink still appends its
    // tombstone; only the replay skips it.)
    match check_connector_write(&state, "connectors", "unlink", &account_id) {
        IdempotencyDecision::Proceed => {}
        IdempotencyDecision::Replay(_) => {
            return Json(account_view(&account)).into_response();
        }
        IdempotencyDecision::Blocked(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
        IdempotencyDecision::Unavailable => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "sqlite_unavailable" })),
            )
                .into_response();
        }
    }
    let mut accounts = state.connector_accounts.lock().expect("connector accounts lock");
    let Some(account) = accounts.get_mut(&account_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown account" }))).into_response();
    };
    // Idempotent: unlinking an already-unlinked account succeeds
    // with its current state, mirroring session unpin/unarchive.
    account.unlinked = true;
    let updated = account.clone();
    persist_connector_line(&state, &ConnectorStateLine::Account(updated.clone()));
    // P0-R: the write-time Evidence hook inside the Effect
    // mirror re-locks the accounts map, so the guard must be
    // released first (the borrow ends at the clone above).
    drop(accounts);
    // Phase 2: a successful unlink mirrors into an Effect, even
    // when idempotent (the tombstone line was still appended).
    record_connector_effect(&state, None, "connectors", "unlink", &updated.account_id);
    Json(account_view(&updated)).into_response()
}

async fn connectors_connect_info(
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let Some(entry) = registry_entry(&id) else {
        return unknown_connector();
    };
    Json(serde_json::json!({
        "connector_id": entry.id,
        "flow": "mock",
        "requires_account": true,
        "description": "Local Mock flow: link a Mock account, then connect. No OAuth, no real account, no external redirect.",
    }))
    .into_response()
}

async fn connectors_connect(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let Some(entry) = registry_entry(&id) else {
        return unknown_connector();
    };
    let has_linked = state
        .connector_accounts
        .lock()
        .expect("connector accounts lock")
        .values()
        .any(|a| a.connector_id == id && !a.unlinked);
    if !has_linked {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "connector has no linked account" })))
            .into_response();
    }
    // E2-A §2.4: a repeat connect with no intervening write is
    // a replay — return the current view, write nothing, record
    // no second Effect row. (A connect after a disconnect is a
    // new write: the disconnect intervened, so the gate
    // Proceeds.)
    match check_connector_write(&state, "connectors", "connect", &id) {
        IdempotencyDecision::Proceed => {}
        IdempotencyDecision::Replay(_) => {
            return Json(connector_view(&state, entry)).into_response();
        }
        IdempotencyDecision::Blocked(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
        IdempotencyDecision::Unavailable => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "sqlite_unavailable" })),
            )
                .into_response();
        }
    }
    state
        .connector_connections
        .lock()
        .expect("connector connections lock")
        .insert(id.to_string(), true);
    persist_connector_line(
        &state,
        &ConnectorStateLine::Connection(ConnectorConnection {
            connector_id: id.to_string(),
            connected: true,
            updated_ms: now_ms(),
        }),
    );
    // Phase 2: a successful connect mirrors into an Effect.
    record_connector_effect(&state, None, "connectors", "connect", &id);
    Json(connector_view(&state, entry)).into_response()
}

async fn connectors_disconnect(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let Some(entry) = registry_entry(&id) else {
        return unknown_connector();
    };
    // E2-A §2.4: a repeat disconnect with no intervening write
    // is a replay — return the current view, write nothing,
    // record no second Effect row.
    match check_connector_write(&state, "connectors", "disconnect", &id) {
        IdempotencyDecision::Proceed => {}
        IdempotencyDecision::Replay(_) => {
            return Json(connector_view(&state, entry)).into_response();
        }
        IdempotencyDecision::Blocked(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
        IdempotencyDecision::Unavailable => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "sqlite_unavailable" })),
            )
                .into_response();
        }
    }
    // Disconnect flips only the connection flag; linked accounts
    // stay linked, so reconnecting needs no fresh link.
    state
        .connector_connections
        .lock()
        .expect("connector connections lock")
        .insert(id.to_string(), false);
    persist_connector_line(
        &state,
        &ConnectorStateLine::Connection(ConnectorConnection {
            connector_id: id.to_string(),
            connected: false,
            updated_ms: now_ms(),
        }),
    );
    // Phase 2: a successful disconnect mirrors into an Effect.
    record_connector_effect(&state, None, "connectors", "disconnect", &id);
    Json(connector_view(&state, entry)).into_response()
}

// --- Vault: credentials, catalog, custom connectors (Task 10) ---
//
// Metadata lives in vault_store.rs (tagged JSONL); secrets live
// only in the secret backend (OS keychain in production, memory
// in tests). No handler ever returns, logs, or echoes a secret:
// responses carry the handle, provider, timestamps, and the
// backend's redacted last-4 display only. Slot deletes remove
// the secret from the backend for real; catalog and
// custom-connector deletes are metadata tombstones only.

fn persist_vault_line(state: &AppState, line: &VaultStateLine) {
    let Some(path) = &state.vault_state_path else { return };
    append_vault_state(path, line);
}

fn unknown_credential() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown credential" }))).into_response()
}

fn unknown_catalog_entry() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown catalog entry" }))).into_response()
}

fn unknown_custom_connector() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown custom connector" }))).into_response()
}

fn unknown_ticket() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown capture ticket" }))).into_response()
}

fn store_failed() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "credential store failed" }))).into_response()
}

#[derive(Debug, Serialize)]
struct CredentialSlotView {
    handle: String,
    provider: Option<String>,
    redacted: Option<String>,
    created_ms: u64,
    updated_ms: u64,
}

fn credential_view(state: &AppState, slot: &CredentialSlot) -> CredentialSlotView {
    CredentialSlotView {
        handle: slot.handle.clone(),
        provider: slot.provider.clone(),
        redacted: state.vault_secrets.redacted(&slot.handle).ok(),
        created_ms: slot.created_ms,
        updated_ms: slot.updated_ms,
    }
}

#[derive(Debug, Serialize)]
struct CatalogView {
    id: String,
    display_name: String,
    description: Option<String>,
    created_ms: u64,
    updated_ms: u64,
}

fn catalog_view(entry: &CatalogEntry) -> CatalogView {
    CatalogView {
        id: entry.id.clone(),
        display_name: entry.display_name.clone(),
        description: entry.description.clone(),
        created_ms: entry.created_ms,
        updated_ms: entry.updated_ms,
    }
}

#[derive(Debug, Serialize)]
struct CustomConnectorView {
    id: String,
    display_name: String,
    description: Option<String>,
    created_ms: u64,
    updated_ms: u64,
}

fn custom_connector_view(def: &CustomConnectorDef) -> CustomConnectorView {
    CustomConnectorView {
        id: def.id.clone(),
        display_name: def.display_name.clone(),
        description: def.description.clone(),
        created_ms: def.created_ms,
        updated_ms: def.updated_ms,
    }
}

#[derive(Debug, Serialize)]
struct TicketView {
    ticket_id: String,
    provider: String,
    handle: String,
    status: TicketStatus,
    created_ms: u64,
    expires_ms: u64,
}

fn ticket_view(ticket: &CaptureTicket) -> TicketView {
    TicketView {
        ticket_id: ticket.ticket_id.clone(),
        provider: ticket.provider.clone(),
        handle: ticket.handle.clone(),
        status: ticket.status,
        created_ms: ticket.created_ms,
        expires_ms: ticket.expires_ms,
    }
}

/// A provider id appearing on a credential implies a catalog
/// entry; create it (display name defaults to the id) when absent.
/// There is no catalog create route in the spec, so this is how
/// entries come to exist for catalog/update/delete to manage.
fn ensure_catalog_entry(state: &AppState, provider: &str, now: u64) {
    let mut catalog = state.vault_catalog.lock().expect("vault catalog lock");
    if catalog.get(provider).is_some_and(|e| !e.deleted) {
        return;
    }
    let entry = CatalogEntry {
        id: provider.to_string(),
        display_name: provider.to_string(),
        description: None,
        created_ms: now,
        updated_ms: now,
        deleted: false,
    };
    catalog.insert(provider.to_string(), entry.clone());
    persist_vault_line(state, &VaultStateLine::Catalog(entry));
}

fn live_slot(slots: &HashMap<String, CredentialSlot>, handle: &str) -> Option<CredentialSlot> {
    slots.get(handle).filter(|s| !s.deleted).cloned()
}

#[derive(Debug, Deserialize)]
struct CustomRegisterDto {
    id: String,
    display_name: String,
    description: Option<String>,
}

async fn vault_custom_register(
    State(state): State<AppState>,
    Json(req): Json<CustomRegisterDto>,
) -> impl IntoResponse {
    let id = req.id.trim().to_string();
    let display_name = req.display_name.trim().to_string();
    if id.is_empty() || display_name.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "id and display_name must not be empty" }))).into_response();
    }
    let mut custom = state.vault_custom.lock().expect("vault custom lock");
    if custom.get(&id).is_some_and(|d| !d.deleted) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "custom connector already exists" }))).into_response();
    }
    let now = now_ms();
    let def = CustomConnectorDef {
        id: id.clone(),
        display_name,
        description: req.description.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
        created_ms: now,
        updated_ms: now,
        deleted: false,
    };
    custom.insert(id, def.clone());
    persist_vault_line(&state, &VaultStateLine::CustomConnector(def.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the custom map).
    drop(custom);
    // Phase 2: a successful custom-connector registration mirrors
    // into an Effect.
    record_connector_effect(&state, None, "custom-connectors", "register", &def.id);
    Json(custom_connector_view(&def)).into_response()
}

async fn vault_custom_list(State(state): State<AppState>) -> Json<Vec<CustomConnectorView>> {
    let mut defs: Vec<CustomConnectorView> = state
        .vault_custom
        .lock()
        .expect("vault custom lock")
        .values()
        .filter(|d| !d.deleted)
        .map(custom_connector_view)
        .collect();
    defs.sort_by(|a, b| a.id.cmp(&b.id));
    Json(defs)
}

#[derive(Debug, Deserialize)]
struct CustomDeleteDto {
    id: String,
}

async fn vault_custom_delete(
    State(state): State<AppState>,
    Json(req): Json<CustomDeleteDto>,
) -> impl IntoResponse {
    let mut custom = state.vault_custom.lock().expect("vault custom lock");
    let Some(def) = custom.get_mut(&req.id) else {
        return unknown_custom_connector();
    };
    if def.deleted {
        return unknown_custom_connector();
    }
    def.deleted = true;
    let updated = def.clone();
    persist_vault_line(&state, &VaultStateLine::CustomConnector(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the custom map).
    drop(custom);
    // Phase 2: a successful custom-connector delete mirrors into
    // an Effect.
    record_connector_effect(&state, None, "custom-connectors", "delete", &req.id);
    Json(custom_connector_view(&updated)).into_response()
}

#[derive(Debug, Deserialize)]
struct CredentialRegisterDto {
    handle: String,
    secret: String,
    provider: Option<String>,
}

async fn vault_credentials_register(
    State(state): State<AppState>,
    Json(req): Json<CredentialRegisterDto>,
) -> impl IntoResponse {
    let handle = req.handle.trim().to_string();
    if handle.is_empty() || req.secret.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "handle and secret must not be empty" }))).into_response();
    }
    let provider = req.provider.map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    let mut slots = state.vault_slots.lock().expect("vault slots lock");
    if slots.get(&handle).is_some_and(|s| !s.deleted) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "credential already exists" }))).into_response();
    }
    if state.vault_secrets.store(&handle, &req.secret).is_err() {
        return store_failed();
    }
    let now = now_ms();
    let slot = CredentialSlot {
        handle: handle.clone(),
        provider: provider.clone(),
        created_ms: now,
        updated_ms: now,
        deleted: false,
    };
    slots.insert(handle, slot.clone());
    persist_vault_line(&state, &VaultStateLine::Credential(slot.clone()));
    drop(slots);
    if let Some(provider) = &provider {
        ensure_catalog_entry(&state, provider, now);
    }
    // Phase 2: a successful credential registration mirrors into
    // an Effect. The incidental catalog ensure above is not a
    // second Effect: one call, one Effect. No secret material ever
    // enters an Effect — only the handle.
    record_connector_effect(&state, None, "credentials", "register", &slot.handle);
    Json(credential_view(&state, &slot)).into_response()
}

async fn vault_credentials_list(State(state): State<AppState>) -> Json<Vec<CredentialSlotView>> {
    let slots = state.vault_slots.lock().expect("vault slots lock");
    let mut views: Vec<CredentialSlotView> = slots
        .values()
        .filter(|s| !s.deleted)
        .map(|s| credential_view(&state, s))
        .collect();
    views.sort_by(|a, b| a.handle.cmp(&b.handle));
    Json(views)
}

#[derive(Debug, Deserialize)]
struct CredentialDeleteDto {
    handle: String,
}

async fn vault_credentials_delete(
    State(state): State<AppState>,
    Json(req): Json<CredentialDeleteDto>,
) -> impl IntoResponse {
    let mut slots = state.vault_slots.lock().expect("vault slots lock");
    let Some(slot) = slots.get_mut(&req.handle) else {
        return unknown_credential();
    };
    if slot.deleted {
        return unknown_credential();
    }
    // Real erasure: a missing backend entry is already-gone, any
    // other backend failure aborts before the tombstone.
    match state.vault_secrets.remove(&req.handle) {
        Ok(()) => {}
        Err(err) if err.code == agent_desktop::ErrorCode::InvalidInput => {}
        Err(_) => return store_failed(),
    }
    slot.deleted = true;
    let updated = slot.clone();
    persist_vault_line(&state, &VaultStateLine::Credential(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the slots map).
    drop(slots);
    // Phase 2: a successful credential delete (real backend
    // erasure + tombstone) mirrors into an Effect.
    record_connector_effect(&state, None, "credentials", "delete", &req.handle);
    Json(credential_view(&state, &updated)).into_response()
}

#[derive(Debug, Deserialize)]
struct CredentialCaptureDto {
    handle: String,
    secret: String,
}

async fn vault_credentials_capture(
    State(state): State<AppState>,
    Json(req): Json<CredentialCaptureDto>,
) -> impl IntoResponse {
    if req.secret.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "secret must not be empty" }))).into_response();
    }
    let mut slots = state.vault_slots.lock().expect("vault slots lock");
    let Some(slot) = slots.get_mut(&req.handle) else {
        return unknown_credential();
    };
    if slot.deleted {
        return unknown_credential();
    }
    if state.vault_secrets.store(&req.handle, &req.secret).is_err() {
        return store_failed();
    }
    slot.updated_ms = now_ms();
    let updated = slot.clone();
    persist_vault_line(&state, &VaultStateLine::Credential(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the slots map).
    drop(slots);
    // Phase 2: a successful credential capture (secret rotation)
    // mirrors into an Effect; the secret itself never does.
    record_connector_effect(&state, None, "credentials", "capture", &req.handle);
    Json(credential_view(&state, &updated)).into_response()
}

#[derive(Debug, Deserialize)]
struct CaptureRequestDto {
    provider: String,
    handle: String,
}

async fn vault_capture_request(
    State(state): State<AppState>,
    Json(req): Json<CaptureRequestDto>,
) -> impl IntoResponse {
    let provider = req.provider.trim().to_string();
    if provider.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "provider must not be empty" }))).into_response();
    }
    {
        let slots = state.vault_slots.lock().expect("vault slots lock");
        if live_slot(&slots, &req.handle).is_none() {
            return unknown_credential();
        }
    }
    let mut next = state.next_ticket_id.lock().expect("ticket id lock");
    let now = now_ms();
    let ticket = CaptureTicket {
        ticket_id: format!("ticket-{}", *next),
        provider,
        handle: req.handle,
        status: TicketStatus::Pending,
        created_ms: now,
        expires_ms: now + TICKET_TTL_MS,
    };
    *next += 1;
    state
        .vault_tickets
        .lock()
        .expect("vault tickets lock")
        .insert(ticket.ticket_id.clone(), ticket.clone());
    persist_vault_line(&state, &VaultStateLine::Ticket(ticket.clone()));
    Json(ticket_view(&ticket)).into_response()
}

#[derive(Debug, Deserialize)]
struct CaptureFulfillDto {
    ticket_id: String,
    secret: String,
}

async fn vault_capture_fulfill(
    State(state): State<AppState>,
    Json(req): Json<CaptureFulfillDto>,
) -> impl IntoResponse {
    let ticket = {
        let mut tickets = state.vault_tickets.lock().expect("vault tickets lock");
        let Some(ticket) = tickets.get_mut(&req.ticket_id) else {
            return unknown_ticket();
        };
        if ticket.status != TicketStatus::Pending {
            return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "ticket already fulfilled" }))).into_response();
        }
        if ticket_expired(ticket, now_ms()) {
            ticket.status = TicketStatus::Expired;
            let updated = ticket.clone();
            persist_vault_line(&state, &VaultStateLine::Ticket(updated));
            return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "ticket expired" }))).into_response();
        }
        ticket.clone()
    };
    if req.secret.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "secret must not be empty" }))).into_response();
    }
    let updated = {
        let mut slots = state.vault_slots.lock().expect("vault slots lock");
        let Some(slot) = slots.get_mut(&ticket.handle) else {
            return unknown_credential();
        };
        if slot.deleted {
            return unknown_credential();
        }
        if state.vault_secrets.store(&ticket.handle, &req.secret).is_err() {
            return store_failed();
        }
        slot.updated_ms = now_ms();
        let updated = slot.clone();
        persist_vault_line(&state, &VaultStateLine::Credential(updated.clone()));
        updated
    };
    let mut tickets = state.vault_tickets.lock().expect("vault tickets lock");
    let Some(ticket) = tickets.get_mut(&req.ticket_id) else {
        return unknown_ticket();
    };
    if ticket.status != TicketStatus::Pending {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "ticket already fulfilled" }))).into_response();
    }
    ticket.status = TicketStatus::Fulfilled;
    let updated_ticket = ticket.clone();
    persist_vault_line(&state, &VaultStateLine::Ticket(updated_ticket));
    // Phase 2: a successful capture fulfill mirrors into an
    // Effect for the materialized credential. The ticket request
    // itself is not an Effect: it mints an ephemeral exchange
    // token, and its durable outcome is this fulfill.
    record_connector_effect(&state, None, "credentials", "fulfill", &ticket.handle);
    Json(credential_view(&state, &updated)).into_response()
}

async fn vault_catalog_list(State(state): State<AppState>) -> Json<Vec<CatalogView>> {
    let mut entries: Vec<CatalogView> = state
        .vault_catalog
        .lock()
        .expect("vault catalog lock")
        .values()
        .filter(|e| !e.deleted)
        .map(catalog_view)
        .collect();
    entries.sort_by(|a, b| a.id.cmp(&b.id));
    Json(entries)
}

#[derive(Debug, Deserialize)]
struct CatalogDetailsQuery {
    id: String,
}

async fn vault_catalog_details(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<CatalogDetailsQuery>,
) -> impl IntoResponse {
    let catalog = state.vault_catalog.lock().expect("vault catalog lock");
    match catalog.get(&query.id).filter(|e| !e.deleted) {
        Some(entry) => Json(catalog_view(entry)).into_response(),
        None => unknown_catalog_entry(),
    }
}

#[derive(Debug, Deserialize)]
struct CatalogUpdateDto {
    id: String,
    display_name: Option<String>,
    description: Option<String>,
}

async fn vault_catalog_update(
    State(state): State<AppState>,
    Json(req): Json<CatalogUpdateDto>,
) -> impl IntoResponse {
    let display_name_provided = req.display_name.is_some();
    let display_name = req.display_name.as_ref().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
    if display_name_provided && display_name.is_none() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "display_name must not be empty" }))).into_response();
    }
    let mut catalog = state.vault_catalog.lock().expect("vault catalog lock");
    let Some(entry) = catalog.get_mut(&req.id) else {
        return unknown_catalog_entry();
    };
    if entry.deleted {
        return unknown_catalog_entry();
    }
    if let Some(display_name) = display_name {
        entry.display_name = display_name;
    }
    if let Some(description) = req.description {
        let description = description.trim().to_string();
        entry.description = if description.is_empty() { None } else { Some(description) };
    }
    entry.updated_ms = now_ms();
    let updated = entry.clone();
    persist_vault_line(&state, &VaultStateLine::Catalog(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the catalog map).
    drop(catalog);
    // Phase 2: a successful catalog update mirrors into an Effect.
    record_connector_effect(&state, None, "catalog", "update", &req.id);
    Json(catalog_view(&updated)).into_response()
}

#[derive(Debug, Deserialize)]
struct CatalogDeleteDto {
    id: String,
}

async fn vault_catalog_delete(
    State(state): State<AppState>,
    Json(req): Json<CatalogDeleteDto>,
) -> impl IntoResponse {
    let mut catalog = state.vault_catalog.lock().expect("vault catalog lock");
    let Some(entry) = catalog.get_mut(&req.id) else {
        return unknown_catalog_entry();
    };
    if entry.deleted {
        return unknown_catalog_entry();
    }
    entry.deleted = true;
    let updated = entry.clone();
    persist_vault_line(&state, &VaultStateLine::Catalog(updated.clone()));
    // P0-R: release the guard before the Effect mirror (its
    // write-time Evidence hook re-locks the catalog map).
    drop(catalog);
    // Phase 2: a successful catalog delete mirrors into an Effect.
    record_connector_effect(&state, None, "catalog", "delete", &req.id);
    Json(catalog_view(&updated)).into_response()
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
        .allow_methods([Method::GET, Method::POST, Method::PATCH, Method::DELETE])
        .allow_headers([header::CONTENT_TYPE])
}

// --- Permissions policy (Task 11) ---
//
// Local policy DATA surface: defaults, overrides, per-connector
// policies, network rules, and scopes. Nothing here enforces
// anything: the approval gates and the EgressPolicy allowlist
// keep their existing behaviour, and the mitm/tproxy/sni
// switches do not intercept real traffic (the Mock has no
// network machinery for that). The Settings UI says so plainly.
// Persistence is tagged JSONL in policy_store.rs; reset and
// revoke append tombstones so a restart cannot resurrect
// cleared records.

fn persist_policy_line(state: &AppState, line: &PolicyStateLine) {
    // D1-2 (Q-D1-21): in SQLite mode the policy_versions
    // table is the fact source; the JSONL file is a
    // read-only archive after the one-time v6 import and
    // is never appended again. JSONL/shadow mode keeps
    // appending (no versioning exists there).
    if state.sqlite_mode() == StateBackend::Sqlite {
        return;
    }
    let Some(path) = &state.policy_state_path else { return };
    append_policy_state(path, line);
}

// --- D1-2: Policy versions (Q-D1-21) ---
//
// Policy is one document with one version number. The
// document is the five policy components as canonical
// JSON; every /permissions/* write records the post-write
// document as a new row in `policy_versions` (SQLite mode
// only), and the JSONL file stops being appended — after
// the one-time v6 import it is a read-only archive.
//
// Ordering note (plan §3 reads "insert the version row,
// then update the memory map"): the write path updates
// the memory map first and the version row records the
// post-write document, so the helper always runs AFTER
// the map mutation. The version row therefore contains
// exactly the document the write produced; the plan's
// atomicity intent (one version row per write, never a
// stale document) is preserved, the literal order is not.

/// The five-component Policy document (Q-D1-21): the
/// versioned unit. `network_rules` is null when no rules
/// were ever set (distinct from an explicitly empty list).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolicyDocument {
    defaults: PolicyDefaults,
    overrides: PolicyOverrides,
    #[serde(default)]
    connector_policies: Vec<ConnectorPolicy>,
    #[serde(default)]
    network_rules: Option<Vec<NetworkRule>>,
    #[serde(default)]
    scopes: Vec<ScopeRecord>,
}

/// The current Policy document, built from the in-memory
/// maps (same five components as `policy_view`, but with
/// the raw records instead of view shapes, and rules as
/// an Option so "never set" stays null).
fn current_policy_document(state: &AppState) -> PolicyDocument {
    let mut connectors: Vec<ConnectorPolicy> = state
        .policy_connectors
        .lock()
        .expect("policy connectors lock")
        .values()
        .filter(|r| !r.deleted)
        .cloned()
        .collect();
    connectors.sort_by(|a, b| a.connector_id.cmp(&b.connector_id));
    let mut scopes: Vec<ScopeRecord> = state
        .policy_scopes
        .lock()
        .expect("policy scopes lock")
        .values()
        .filter(|r| !r.deleted)
        .cloned()
        .collect();
    scopes.sort_by(|a, b| {
        (a.scope_type.clone(), a.scope_id.clone())
            .cmp(&(b.scope_type.clone(), b.scope_id.clone()))
    });
    PolicyDocument {
        defaults: policy_defaults(state),
        overrides: policy_overrides(state),
        connector_policies: connectors,
        network_rules: state
            .policy_rules
            .lock()
            .expect("policy rules lock")
            .clone(),
        scopes,
    }
}

/// Apply a stored Policy document to the in-memory maps
/// (restart recovery: the versions table overrides the
/// JSONL-loaded state).
fn apply_policy_document(state: &AppState, doc: &PolicyDocument) {
    *state
        .policy_defaults
        .lock()
        .expect("policy defaults lock") = Some(doc.defaults.clone());
    *state
        .policy_overrides
        .lock()
        .expect("policy overrides lock") = Some(doc.overrides.clone());
    let mut connectors = HashMap::new();
    for record in &doc.connector_policies {
        connectors.insert(record.connector_id.clone(), record.clone());
    }
    *state
        .policy_connectors
        .lock()
        .expect("policy connectors lock") = connectors;
    *state.policy_rules.lock().expect("policy rules lock") = doc.network_rules.clone();
    let mut scopes = HashMap::new();
    for record in &doc.scopes {
        scopes.insert(record.key(), record.clone());
    }
    *state.policy_scopes.lock().expect("policy scopes lock") = scopes;
}

/// Record the current Policy document as a new version
/// row (SQLite mode only). Called at the end of every
/// /permissions/* write handler, after the in-memory maps
/// hold the post-write document. Best-effort: a
/// versioning failure must not fail a policy write that
/// already succeeded in memory (warn only). The document
/// is built before the SQLite lock is taken (map locks
/// first, never under the SQLite lock).
fn record_policy_version(state: &AppState) {
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return;
    }
    let document = current_policy_document(state);
    let Ok(document_value) = serde_json::to_value(&document) else {
        eprintln!("warning: D1-2 policy version skipped (cannot encode document)");
        return;
    };
    let document_json = sqlite_store::canonical_json(&document_value);
    let digest = sqlite_store::sha256_hex(document_json.as_bytes());
    let Some(sqlite) = &state.sqlite else { return };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = sqlite_store::insert_policy_version(
        &mut conn,
        &document_json,
        &digest,
        sqlite_store::now_ms(),
        &state.origin_device_id,
    ) {
        eprintln!("warning: D1-2 policy version insert failed: {detail}");
    }
}

/// One-time import of the JSONL policy as version 1, run
/// at v6 startup when `policy_versions` is empty and a
/// policy JSONL file exists. The document is passed in
/// pre-built (maps only, no SQLite lock held by the
/// caller). After the import the file is a read-only
/// archive. Returns true when a version was imported.
fn import_policy_version_once(
    state: &AppState,
    conn: &mut Connection,
    document: &PolicyDocument,
) -> Result<bool, String> {
    let Some(path) = &state.policy_state_path else {
        return Ok(false);
    };
    if !path.exists() {
        return Ok(false);
    }
    if !sqlite_store::list_policy_versions(conn)?.is_empty() {
        return Ok(false);
    }
    let document_value =
        serde_json::to_value(document).map_err(|e| format!("sqlite: cannot encode policy: {e}"))?;
    let document_json = sqlite_store::canonical_json(&document_value);
    let digest = sqlite_store::sha256_hex(document_json.as_bytes());
    sqlite_store::insert_policy_version(
        conn,
        &document_json,
        &digest,
        sqlite_store::now_ms(),
        &state.origin_device_id,
    )?;
    Ok(true)
}

/// Apply stored Policy versions to the in-memory maps:
/// retain every version's network rules (the frozen-rules
/// gate resolves by digest, so all versions stay
/// available) and apply the latest version's document.
/// No-op when the table is empty.
fn apply_policy_versions(state: &AppState, versions: &[sqlite_store::PolicyVersionRow]) {
    for version in versions {
        if let Ok(doc) = serde_json::from_str::<serde_json::Value>(&version.document_json) {
            if let Some(rules) = doc
                .get("network_rules")
                .and_then(|r| serde_json::from_value::<Vec<NetworkRule>>(r.clone()).ok())
            {
                retain_policy_rules_version(state, &rules);
            }
        }
    }
    if let Some(latest) = versions.last() {
        match serde_json::from_str::<PolicyDocument>(&latest.document_json) {
            Ok(doc) => apply_policy_document(state, &doc),
            Err(detail) => eprintln!(
                "warning: D1-2 latest policy version {} does not parse ({detail}); keeping loaded policy",
                latest.version_id
            ),
        }
    }
}

// --- D1-2: Mandates (Q-D1-22) ---
//
// A Mandate is an authorization snapshot: the goal text,
// the tool set the entry point may invoke, and the policy
// version live at its creation. Policy changes never
// re-approve a Mandate — the Mandate pins its creation
// version; a new Task/Execution binds the then-current
// version. Creation is SQLite-mode only and best-effort:
// a mandate failure must never fail the Task/Execution
// creation that triggered it (warn only).

/// D1-2 (Q-D1-22): the tool set a Mandate may authorize.
/// Task records declare no tool set of their own, so the
/// loop engine's whitelist stands in as the declared set
/// — the only tools an entry point can invoke today.
/// Local definition (no runtime evidence behind the
/// choice beyond the whitelist itself); keep in sync with
/// `loop_engine::normalize` (same 11 names, same order).
const MANDATE_TOOLS: &[&str] = &[
    "fs.list",
    "fs.read",
    "fs.write",
    "reminders.list",
    "reminders.set_completed",
    "email.threads",
    "email.message",
    "code.search",
    "git.read",
    "worker.run",
    "worker.wait",
];

/// The `allowed_json` document for a new Mandate
/// (canonical JSON): the goal, the entry point kind and
/// the tool set it may invoke. The policy's granted
/// scopes are NOT duplicated here — the bound
/// `policy_version_id` names the document that holds them.
fn mandate_allowed_json(goal_id: Option<&str>, goal: &str, entry: &str) -> String {
    let value = serde_json::json!({
        "goal_id": goal_id,
        "goal": goal,
        "entry": entry,
        "tools": MANDATE_TOOLS,
    });
    sqlite_store::canonical_json(&value)
}

/// The policy version a new Mandate binds: the latest
/// stored version, or a v1 recorded from the current
/// document when none exists yet (first-use baseline).
/// SQLite mode only; None otherwise. The document is
/// built from the maps before any SQLite lock is taken.
fn current_policy_version_id(state: &AppState) -> Option<String> {
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return None;
    }
    let latest = {
        let Some(sqlite) = &state.sqlite else {
            return None;
        };
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::latest_policy_version(&conn)
    };
    match latest {
        Ok(Some(row)) => Some(row.version_id),
        Ok(None) => {
            let document = current_policy_document(state);
            let Ok(document_value) = serde_json::to_value(&document) else {
                eprintln!(
                    "warning: D1-2 mandate policy baseline skipped (cannot encode document)"
                );
                return None;
            };
            let document_json = sqlite_store::canonical_json(&document_value);
            let digest = sqlite_store::sha256_hex(document_json.as_bytes());
            let Some(sqlite) = &state.sqlite else {
                return None;
            };
            let mut conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::insert_policy_version(
                &mut conn,
                &document_json,
                &digest,
                sqlite_store::now_ms(),
                &state.origin_device_id,
            ) {
                Ok(row) => Some(row.version_id),
                Err(detail) => {
                    eprintln!("warning: D1-2 mandate policy baseline failed: {detail}");
                    None
                }
            }
        }
        Err(detail) => {
            eprintln!("warning: D1-2 mandate policy lookup failed: {detail}");
            None
        }
    }
}

/// The goal title for a Mandate's `goal` text: the goals
/// table title when the goal exists, else the raw goal id,
/// else the empty string (no goal was named — the Mandate
/// still records the tool set + policy version).
fn mandate_goal_text(state: &AppState, goal_id: Option<&str>) -> String {
    let Some(goal_id) = goal_id else {
        return String::new();
    };
    let Some(sqlite) = &state.sqlite else {
        return goal_id.to_string();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_goal(&conn, goal_id) {
        Ok(Some(row)) => row.title,
        _ => goal_id.to_string(),
    }
}

/// Create one Mandate row. SQLite mode only, best-effort
/// (warn only, never fail the caller). Returns the row.
fn create_mandate(
    state: &AppState,
    task_id: Option<String>,
    execution_id: Option<String>,
    goal_id: Option<String>,
    entry: &str,
) -> Option<sqlite_store::MandateRow> {
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return None;
    }
    let policy_version_id = current_policy_version_id(state)?;
    let mut next = state.next_mandate_id.lock().expect("mandate id lock");
    let number = *next;
    *next += 1;
    drop(next);
    let goal = mandate_goal_text(state, goal_id.as_deref());
    let row = sqlite_store::MandateRow {
        mandate_id: format!("mandate-{number}"),
        task_id,
        execution_id,
        goal: goal.clone(),
        allowed_json: mandate_allowed_json(goal_id.as_deref(), &goal, entry),
        policy_version_id,
        status: "active".to_string(),
        created_ms: sqlite_store::now_ms() as i64,
        origin_device_id: state.origin_device_id.clone(),
    };
    let Some(sqlite) = &state.sqlite else {
        return None;
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::insert_mandate(&mut conn, &row) {
        Ok(()) => Some(row),
        Err(detail) => {
            eprintln!("warning: D1-2 mandate insert failed: {detail}");
            None
        }
    }
}

/// The Mandate hook for Task creation: one task-level row
/// (execution_id NULL).
fn create_task_mandate(state: &AppState, record: &TaskFileRecord) {
    create_mandate(
        state,
        Some(record.task_id.clone()),
        None,
        record.goal_id.clone(),
        "task",
    );
}

/// The Mandate hook for a Task goal change: supersede the
/// old task-level rows and create a fresh one binding the
/// current policy version.
fn supersede_task_mandates_and_create(state: &AppState, record: &TaskFileRecord) {
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return;
    }
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = sqlite_store::supersede_mandates_for_task(&mut conn, &record.task_id) {
        eprintln!("warning: D1-2 mandate supersede failed: {detail}");
        return;
    }
    drop(conn);
    create_mandate(
        state,
        Some(record.task_id.clone()),
        None,
        record.goal_id.clone(),
        "task",
    );
}

/// The Mandate hook for Execution creation. Adapter/direct
/// executions (no task, no loop) carry no goal and no
/// entry-point tool set: no Mandate. A task execution
/// reuses the task's active Mandate when it already binds
/// the current policy version; otherwise (policy changed
/// since the task was created) a fresh execution-level
/// row binds the current version — a new Execution binds
/// the new version (plan §3 item 2). A loop execution
/// always gets a fresh row.
fn bind_execution_mandate(state: &AppState, record: &ExecutionFileRecord) {
    if record.task_id.is_none() && record.loop_id.is_none() {
        return;
    }
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return;
    }
    if let Some(task_id) = &record.task_id {
        let current = current_policy_version_id(state);
        let reuse = {
            let Some(sqlite) = &state.sqlite else {
                return;
            };
            let conn = sqlite.lock().expect("sqlite lock");
            sqlite_store::list_active_task_mandates(&conn, task_id)
                .map(|rows| {
                    rows.iter()
                        .any(|row| current.as_deref() == Some(row.policy_version_id.as_str()))
                })
                .unwrap_or(false)
        };
        if reuse {
            return;
        }
        create_mandate(
            state,
            Some(task_id.clone()),
            Some(record.execution_id.clone()),
            record.goal_id.clone(),
            "task",
        );
        return;
    }
    create_mandate(
        state,
        None,
        Some(record.execution_id.clone()),
        record.goal_id.clone(),
        "loop",
    );
}

fn policy_defaults(state: &AppState) -> PolicyDefaults {
    state
        .policy_defaults
        .lock()
        .expect("policy defaults lock")
        .clone()
        .unwrap_or_default()
}

fn policy_overrides(state: &AppState) -> PolicyOverrides {
    state
        .policy_overrides
        .lock()
        .expect("policy overrides lock")
        .clone()
        .unwrap_or_default()
}

fn policy_effective(state: &AppState) -> EffectivePolicy {
    effective_policy(&policy_defaults(state), &policy_overrides(state))
}

fn store_policy_overrides(state: &AppState, overrides: PolicyOverrides) {
    *state.policy_overrides.lock().expect("policy overrides lock") = Some(overrides.clone());
    persist_policy_line(state, &PolicyStateLine::Overrides(overrides));
}

fn store_policy_defaults(state: &AppState, defaults: PolicyDefaults) {
    *state.policy_defaults.lock().expect("policy defaults lock") = Some(defaults.clone());
    persist_policy_line(state, &PolicyStateLine::Defaults(defaults));
}

#[derive(Debug, Serialize)]
struct ConnectorPolicyView {
    connector_id: String,
    allowed: bool,
    updated_ms: u64,
}

fn connector_policy_view(state: &AppState, connector_id: &str) -> ConnectorPolicyView {
    let map = state.policy_connectors.lock().expect("policy connectors lock");
    match map.get(connector_id).filter(|r| !r.deleted) {
        Some(record) => ConnectorPolicyView {
            connector_id: record.connector_id.clone(),
            allowed: record.allowed,
            updated_ms: record.updated_ms,
        },
        None => ConnectorPolicyView { connector_id: connector_id.to_string(), allowed: true, updated_ms: 0 },
    }
}

#[derive(Debug, Serialize)]
struct ScopeView {
    scope_type: String,
    scope_id: String,
    granted: bool,
    updated_ms: u64,
}

fn scope_view(record: &ScopeRecord) -> ScopeView {
    ScopeView {
        scope_type: record.scope_type.clone(),
        scope_id: record.scope_id.clone(),
        granted: record.granted,
        updated_ms: record.updated_ms,
    }
}

fn unknown_scope() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown scope" }))).into_response()
}

#[derive(Debug, Serialize)]
struct PolicyView {
    defaults: PolicyDefaults,
    active: EffectivePolicy,
    overrides: PolicyOverrides,
    connectors: Vec<ConnectorPolicyView>,
    scopes: Vec<ScopeView>,
    network_rules: Vec<NetworkRule>,
}

fn policy_view(state: &AppState) -> PolicyView {
    let mut connectors: Vec<ConnectorPolicyView> = state
        .policy_connectors
        .lock()
        .expect("policy connectors lock")
        .values()
        .filter(|r| !r.deleted)
        .map(|r| ConnectorPolicyView {
            connector_id: r.connector_id.clone(),
            allowed: r.allowed,
            updated_ms: r.updated_ms,
        })
        .collect();
    connectors.sort_by(|a, b| a.connector_id.cmp(&b.connector_id));
    let mut scopes: Vec<ScopeView> = state
        .policy_scopes
        .lock()
        .expect("policy scopes lock")
        .values()
        .filter(|r| !r.deleted)
        .map(scope_view)
        .collect();
    scopes.sort_by(|a, b| (&a.scope_type, &a.scope_id).cmp(&(&b.scope_type, &b.scope_id)));
    let rules = state
        .policy_rules
        .lock()
        .expect("policy rules lock")
        .clone()
        .unwrap_or_default();
    PolicyView {
        defaults: policy_defaults(state),
        active: policy_effective(state),
        overrides: policy_overrides(state),
        connectors,
        scopes,
        network_rules: rules,
    }
}

async fn policy_get(State(state): State<AppState>) -> Json<PolicyView> {
    Json(policy_view(&state))
}

async fn policy_active_get(State(state): State<AppState>) -> Json<EffectivePolicy> {
    Json(policy_effective(&state))
}

/// Three-state snooze field: absent means "no change", explicit
/// null means "clear", a number means "set". Plain
/// `Option<Option<u64>>` collapses absent and null into one
/// value under serde_json, so this helper keeps them apart.
fn deserialize_snooze<'de, D>(deserializer: D) -> Result<Option<Option<u64>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let inner = Option::<u64>::deserialize(deserializer)?;
    Ok(Some(inner))
}

#[derive(Debug, Deserialize)]
struct PolicyActiveDto {
    hitl_enabled: Option<bool>,
    mitm_enabled: Option<bool>,
    tproxy_enabled: Option<bool>,
    sni_rejection_enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_snooze")]
    hitl_snoozed_until_ms: Option<Option<u64>>,
}

async fn policy_active_update(
    State(state): State<AppState>,
    Json(req): Json<PolicyActiveDto>,
) -> Json<EffectivePolicy> {
    let mut overrides = policy_overrides(&state);
    if let Some(v) = req.hitl_enabled {
        overrides.hitl_enabled = Some(v);
    }
    if let Some(v) = req.mitm_enabled {
        overrides.mitm_enabled = Some(v);
    }
    if let Some(v) = req.tproxy_enabled {
        overrides.tproxy_enabled = Some(v);
    }
    if let Some(v) = req.sni_rejection_enabled {
        overrides.sni_rejection_enabled = Some(v);
    }
    if let Some(v) = req.hitl_snoozed_until_ms {
        overrides.hitl_snoozed_until_ms = v;
    }
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(policy_effective(&state))
}

#[derive(Debug, Deserialize)]
struct ConnectorPolicyDto {
    allowed: bool,
}

async fn policy_connector_update(
    State(state): State<AppState>,
    axum::extract::Path(connector): axum::extract::Path<String>,
    Json(req): Json<ConnectorPolicyDto>,
) -> impl IntoResponse {
    if registry_entry(&connector).is_none() {
        return unknown_connector();
    }
    let record = ConnectorPolicy {
        connector_id: connector.clone(),
        allowed: req.allowed,
        updated_ms: now_ms(),
        deleted: false,
    };
    state
        .policy_connectors
        .lock()
        .expect("policy connectors lock")
        .insert(connector.clone(), record.clone());
    persist_policy_line(&state, &PolicyStateLine::ConnectorPolicy(record));
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(connector_policy_view(&state, &connector)).into_response()
}

async fn policy_connector_reset(
    State(state): State<AppState>,
    axum::extract::Path(connector): axum::extract::Path<String>,
) -> impl IntoResponse {
    if registry_entry(&connector).is_none() {
        return unknown_connector();
    }
    let mut map = state.policy_connectors.lock().expect("policy connectors lock");
    if let Some(record) = map.get_mut(&connector).filter(|r| !r.deleted) {
        record.deleted = true;
        record.updated_ms = now_ms();
        let updated = record.clone();
        persist_policy_line(&state, &PolicyStateLine::ConnectorPolicy(updated));
    }
    drop(map);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(connector_policy_view(&state, &connector)).into_response()
}

#[derive(Debug, Serialize)]
struct HitlView {
    enabled: bool,
    snoozed_until_ms: Option<u64>,
}

fn hitl_view(state: &AppState) -> HitlView {
    let effective = policy_effective(state);
    HitlView { enabled: effective.hitl_enabled, snoozed_until_ms: effective.hitl_snoozed_until_ms }
}

#[derive(Debug, Deserialize)]
struct HitlDto {
    enabled: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_snooze")]
    snoozed_until_ms: Option<Option<u64>>,
}

async fn policy_hitl_update(
    State(state): State<AppState>,
    Json(req): Json<HitlDto>,
) -> Json<HitlView> {
    let mut overrides = policy_overrides(&state);
    if let Some(v) = req.enabled {
        overrides.hitl_enabled = Some(v);
    }
    if let Some(v) = req.snoozed_until_ms {
        overrides.hitl_snoozed_until_ms = v;
    }
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(hitl_view(&state))
}

async fn policy_hitl_snooze_delete(State(state): State<AppState>) -> Json<HitlView> {
    let mut overrides = policy_overrides(&state);
    if overrides.hitl_snoozed_until_ms.is_some() {
        overrides.hitl_snoozed_until_ms = None;
        overrides.updated_ms = now_ms();
        store_policy_overrides(&state, overrides);
    }
    // D1-2 (Q-D1-21): one version row per policy write
    // (even a no-op delete: the write was issued).
    record_policy_version(&state);
    Json(hitl_view(&state))
}

#[derive(Debug, Deserialize)]
struct SwitchDto {
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct SwitchView {
    enabled: bool,
}

async fn policy_mitm_update(
    State(state): State<AppState>,
    Json(req): Json<SwitchDto>,
) -> Json<SwitchView> {
    let mut overrides = policy_overrides(&state);
    overrides.mitm_enabled = Some(req.enabled);
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(SwitchView { enabled: policy_effective(&state).mitm_enabled })
}

async fn policy_tproxy_update(
    State(state): State<AppState>,
    Json(req): Json<SwitchDto>,
) -> Json<SwitchView> {
    let mut overrides = policy_overrides(&state);
    overrides.tproxy_enabled = Some(req.enabled);
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(SwitchView { enabled: policy_effective(&state).tproxy_enabled })
}

async fn policy_sni_rejection_update(
    State(state): State<AppState>,
    Json(req): Json<SwitchDto>,
) -> Json<SwitchView> {
    let mut overrides = policy_overrides(&state);
    overrides.sni_rejection_enabled = Some(req.enabled);
    overrides.updated_ms = now_ms();
    store_policy_overrides(&state, overrides);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(SwitchView { enabled: policy_effective(&state).sni_rejection_enabled })
}

#[derive(Debug, Deserialize)]
struct NetworkRulesDto {
    rules: Vec<NetworkRule>,
}

async fn policy_network_rules_update(
    State(state): State<AppState>,
    Json(req): Json<NetworkRulesDto>,
) -> impl IntoResponse {
    if req.rules.iter().any(|r| !r.valid()) {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid network rule" })))
            .into_response();
    }
    *state.policy_rules.lock().expect("policy rules lock") = Some(req.rules.clone());
    retain_policy_rules_version(&state, &req.rules);
    persist_policy_line(&state, &PolicyStateLine::NetworkRules { rules: req.rules.clone() });
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(req.rules).into_response()
}

async fn policy_reset(State(state): State<AppState>) -> Json<PolicyView> {
    let now = now_ms();
    store_policy_defaults(&state, PolicyDefaults { updated_ms: now, ..Default::default() });
    store_policy_overrides(&state, PolicyOverrides { updated_ms: now, ..Default::default() });
    *state.policy_rules.lock().expect("policy rules lock") = Some(Vec::new());
    retain_policy_rules_version(&state, &[]);
    persist_policy_line(&state, &PolicyStateLine::NetworkRules { rules: Vec::new() });
    let mut connectors = state.policy_connectors.lock().expect("policy connectors lock");
    for record in connectors.values_mut().filter(|r| !r.deleted) {
        record.deleted = true;
        record.updated_ms = now;
        let updated = record.clone();
        persist_policy_line(&state, &PolicyStateLine::ConnectorPolicy(updated));
    }
    connectors.retain(|_, r| !r.deleted);
    drop(connectors);
    let mut scopes = state.policy_scopes.lock().expect("policy scopes lock");
    for record in scopes.values_mut().filter(|r| !r.deleted) {
        record.deleted = true;
        record.updated_ms = now;
        let updated = record.clone();
        persist_policy_line(&state, &PolicyStateLine::Scope(updated));
    }
    scopes.retain(|_, r| !r.deleted);
    drop(scopes);
    // D1-2 (Q-D1-21): one version row per policy write —
    // the reset's five section mutations are one write.
    record_policy_version(&state);
    Json(policy_view(&state))
}

#[derive(Debug, Deserialize)]
struct RevokeDto {
    scope_type: Option<String>,
    scope_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct RevokeView {
    revoked: Vec<String>,
}

async fn policy_revoke(
    State(state): State<AppState>,
    Json(req): Json<RevokeDto>,
) -> Json<RevokeView> {
    let now = now_ms();
    let mut revoked = Vec::new();
    let mut scopes = state.policy_scopes.lock().expect("policy scopes lock");
    let target = match (&req.scope_type, &req.scope_id) {
        (Some(t), Some(i)) => Some(format!("{t}/{i}")),
        _ => None,
    };
    for record in scopes.values_mut().filter(|r| !r.deleted) {
        if target.as_ref().is_some_and(|t| t != &record.key()) {
            continue;
        }
        record.deleted = true;
        record.updated_ms = now;
        let updated = record.clone();
        persist_policy_line(&state, &PolicyStateLine::Scope(updated));
        revoked.push(record.key());
    }
    scopes.retain(|_, r| !r.deleted);
    revoked.sort();
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(RevokeView { revoked })
}

async fn policy_scope_get(
    State(state): State<AppState>,
    axum::extract::Path((scope_type, scope_id)): axum::extract::Path<(String, String)>,
) -> impl IntoResponse {
    let key = format!("{scope_type}/{scope_id}");
    let scopes = state.policy_scopes.lock().expect("policy scopes lock");
    match scopes.get(&key).filter(|r| !r.deleted) {
        Some(record) => Json(scope_view(record)).into_response(),
        None => unknown_scope(),
    }
}

#[derive(Debug, Deserialize)]
struct ScopeDto {
    granted: Option<bool>,
}

async fn policy_scope_update(
    State(state): State<AppState>,
    axum::extract::Path((scope_type, scope_id)): axum::extract::Path<(String, String)>,
    Json(req): Json<ScopeDto>,
) -> Json<ScopeView> {
    let record = ScopeRecord {
        scope_type: scope_type.clone(),
        scope_id: scope_id.clone(),
        granted: req.granted.unwrap_or(true),
        updated_ms: now_ms(),
        deleted: false,
    };
    state
        .policy_scopes
        .lock()
        .expect("policy scopes lock")
        .insert(record.key(), record.clone());
    persist_policy_line(&state, &PolicyStateLine::Scope(record.clone()));
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(scope_view(&record))
}

#[derive(Debug, Serialize)]
struct PolicyInventoryView {
    sections: Vec<&'static str>,
    connector_policies: usize,
    scopes: usize,
    network_rules: usize,
    defaults_customized: bool,
}

async fn policy_inventory(State(state): State<AppState>) -> Json<PolicyInventoryView> {
    let connector_policies = state
        .policy_connectors
        .lock()
        .expect("policy connectors lock")
        .values()
        .filter(|r| !r.deleted)
        .count();
    let scopes = state
        .policy_scopes
        .lock()
        .expect("policy scopes lock")
        .values()
        .filter(|r| !r.deleted)
        .count();
    let network_rules = state
        .policy_rules
        .lock()
        .expect("policy rules lock")
        .clone()
        .unwrap_or_default()
        .len();
    // "Customized" means the stored defaults differ from the
    // built-in ones: a reset stores a builtin-valued snapshot,
    // which must not report as customized.
    let defaults_customized = state
        .policy_defaults
        .lock()
        .expect("policy defaults lock")
        .as_ref()
        .is_some_and(|d| {
            d.hitl_enabled || d.mitm_enabled || d.tproxy_enabled || d.sni_rejection_enabled
        });
    Json(PolicyInventoryView {
        sections: vec![
            "hitl",
            "mitm",
            "tproxy",
            "sni_rejection",
            "network_rules",
            "connectors",
            "scopes",
        ],
        connector_policies,
        scopes,
        network_rules,
        defaults_customized,
    })
}

async fn policy_settings_get(State(state): State<AppState>) -> Json<PolicyDefaults> {
    Json(policy_defaults(&state))
}

#[derive(Debug, Deserialize)]
struct PolicySettingsDto {
    hitl_enabled: Option<bool>,
    mitm_enabled: Option<bool>,
    tproxy_enabled: Option<bool>,
    sni_rejection_enabled: Option<bool>,
}

async fn policy_settings_update(
    State(state): State<AppState>,
    Json(req): Json<PolicySettingsDto>,
) -> Json<PolicyDefaults> {
    let mut defaults = policy_defaults(&state);
    if let Some(v) = req.hitl_enabled {
        defaults.hitl_enabled = v;
    }
    if let Some(v) = req.mitm_enabled {
        defaults.mitm_enabled = v;
    }
    if let Some(v) = req.tproxy_enabled {
        defaults.tproxy_enabled = v;
    }
    if let Some(v) = req.sni_rejection_enabled {
        defaults.sni_rejection_enabled = v;
    }
    defaults.updated_ms = now_ms();
    store_policy_defaults(&state, defaults);
    // D1-2 (Q-D1-21): one version row per policy write.
    record_policy_version(&state);
    Json(policy_defaults(&state))
}

async fn connectors_permissions(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if registry_entry(&id).is_none() {
        return unknown_connector();
    }
    Json(connector_policy_view(&state, &id)).into_response()
}

// --- D1-2: Policy versions + Mandates (read surface) ---
//
// HTTP-only in this batch (plan §1): no frontend views
// yet — Policy/Mandate reads land here first, UI follows
// when a reader exists.

/// GET /policies/versions: every Policy version, oldest
/// first (document + digest + supersedes chain).
async fn policies_versions_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_policy_versions(&conn) {
        Ok(rows) => Json(rows).into_response(),
        Err(detail) => {
            eprintln!("warning: policy version list failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// GET /mandates: Mandate list, optionally filtered by
/// ?task_id= and/or ?execution_id= (same query-filter
/// precedent as /tasks/list).
#[derive(Debug, Deserialize)]
struct MandateListQuery {
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    execution_id: Option<String>,
}

async fn mandates_list(
    State(state): State<AppState>,
    Query(query): Query<MandateListQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_mandates(
        &conn,
        query.task_id.as_deref(),
        query.execution_id.as_deref(),
    ) {
        Ok(rows) => Json(rows).into_response(),
        Err(detail) => {
            eprintln!("warning: mandate list failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// GET /mandates/{id}: one Mandate row, or 404.
async fn mandates_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_mandate(&conn, &id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown mandate" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: mandate get failed ({detail})");
            sqlite_error_response()
        }
    }
}

// --- Model surface (Task 12) ---
//
// GET reports the running model config plus any staged
// selection; POST stages a selection that takes effect only
// after a restart. The running DeepSeekClient is never
// rebuilt mid-flight. The key is never accepted here and
// never appears in a response: only configured + the same
// ****last-4 redaction the Settings key status uses.

fn persist_model_line(state: &AppState, line: &ModelStateLine) {
    let Some(path) = &state.model_state_path else { return };
    append_model_state(path, line);
}

#[derive(Debug, Serialize)]
struct ModelView {
    provider: &'static str,
    model: String,
    base_url: String,
    key_configured: bool,
    key_redacted: Option<String>,
    staged_model: Option<String>,
    staged_base_url: Option<String>,
    restart_needed: bool,
}

/// Merge a staged model selection over a running config.
/// An explicit env value wins over staged on every restart;
/// otherwise a staged field replaces the running one. Pure
/// (env values passed in) so tests can cover it without
/// mutating process env. Public so the smoke test can build
/// a restarted state through the same merge production
/// uses at startup (from_deepseek_config).
pub fn merge_staged_config(
    mut config: DeepSeekConfig,
    staged: &Option<StagedModel>,
    env_model: Option<String>,
    env_base_url: Option<String>,
) -> DeepSeekConfig {
    let Some(staged) = staged else { return config };
    if env_model.as_ref().is_none_or(|m| m.is_empty()) {
        if let Some(m) = &staged.model {
            config.model = m.clone();
        }
    }
    if env_base_url.as_ref().is_none_or(|u| u.is_empty()) {
        if let Some(u) = &staged.base_url {
            config.base_url = u.clone();
        }
    }
    config
}

/// Is there a staged value a restart would still apply?
/// A staged field counts only when it differs from the
/// running config AND no env override explains the
/// difference (env wins over staged, so a staged value
/// shadowed by env is dormant, not pending). Pure for
/// the same reason as merge_staged_config.
fn model_restart_needed(
    running: &DeepSeekConfig,
    staged: &StagedModel,
    env_model: Option<&str>,
    env_base_url: Option<&str>,
) -> bool {
    let model_pending = staged.model.as_ref().is_some_and(|m| {
        m != &running.model && env_model.is_none_or(|e| e.is_empty())
    });
    let base_pending = staged.base_url.as_ref().is_some_and(|u| {
        u != &running.base_url && env_base_url.is_none_or(|e| e.is_empty())
    });
    model_pending || base_pending
}

fn model_view(state: &AppState) -> ModelView {
    let running = state.deepseek.config();
    let staged = state.model_staged.lock().expect("model staged lock").clone().unwrap_or_default();
    let model = staged.model.clone().unwrap_or_else(|| running.model.clone());
    let base_url = staged.base_url.clone().unwrap_or_else(|| running.base_url.clone());
    let env_model = std::env::var("DEEPSEEK_MODEL").ok().filter(|s| !s.is_empty());
    let env_base = std::env::var("DEEPSEEK_BASE_URL").ok().filter(|s| !s.is_empty());
    let restart_needed =
        model_restart_needed(running, &staged, env_model.as_deref(), env_base.as_deref());
    let (key_configured, key_redacted) = match &running.api_key {
        Some(key) => (true, Some(redact(key))),
        None => (false, None),
    };
    ModelView {
        provider: "deepseek",
        model,
        base_url,
        key_configured,
        key_redacted,
        staged_model: staged.model,
        staged_base_url: staged.base_url,
        restart_needed,
    }
}

async fn model_get(State(state): State<AppState>) -> Json<ModelView> {
    Json(model_view(&state))
}

#[derive(Debug, Deserialize)]
struct ModelSetDto {
    model: Option<String>,
    base_url: Option<String>,
}

async fn model_set(
    State(state): State<AppState>,
    Json(req): Json<ModelSetDto>,
) -> impl IntoResponse {
    let model = match req.model {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "model must not be empty" })))
                    .into_response();
            }
            Some(trimmed)
        }
        None => None,
    };
    let base_url = match req.base_url {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty()
                || (!trimmed.starts_with("http://") && !trimmed.starts_with("https://"))
            {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid base_url" })))
                    .into_response();
            }
            Some(trimmed)
        }
        None => None,
    };
    // All-absent body is a no-op success: it changes nothing
    // and reports the current view, mirroring idempotent reads.
    if model.is_none() && base_url.is_none() {
        return Json(model_view(&state)).into_response();
    }
    let mut staged = state.model_staged.lock().expect("model staged lock").clone().unwrap_or_default();
    if let Some(m) = model {
        staged.model = Some(m);
    }
    if let Some(u) = base_url {
        staged.base_url = Some(u);
    }
    staged.updated_ms = now_ms();
    *state.model_staged.lock().expect("model staged lock") = Some(staged.clone());
    persist_model_line(&state, &ModelStateLine::Model(staged));
    Json(model_view(&state)).into_response()
}

#[cfg(test)]
mod model_merge_tests {
    use super::*;
    use crate::deepseek::DeepSeekConfig;

    fn running() -> DeepSeekConfig {
        DeepSeekConfig {
            base_url: "https://api.deepseek.com".to_string(),
            model: "deepseek-flash".to_string(),
            api_key: None,
        }
    }

    fn staged() -> StagedModel {
        StagedModel {
            model: Some("deepseek-chat".to_string()),
            base_url: Some("https://staged.example.com".to_string()),
            updated_ms: 1,
        }
    }

    #[test]
    fn merge_applies_staged_without_env() {
        let merged = merge_staged_config(running(), &Some(staged()), None, None);
        assert_eq!(merged.model, "deepseek-chat");
        assert_eq!(merged.base_url, "https://staged.example.com");
        assert_eq!(merged.api_key, None);
    }

    #[test]
    fn merge_env_wins_over_staged() {
        // The caller (from_env_and_keychain) already put the
        // env values into config; merge only decides whether
        // staged may override. With env present, staged is
        // ignored and the env-carrying config stands.
        let mut env_config = running();
        env_config.model = "deepseek-reasoner".to_string();
        env_config.base_url = "https://env.example.com".to_string();
        let merged = merge_staged_config(
            env_config,
            &Some(staged()),
            Some("deepseek-reasoner".to_string()),
            Some("https://env.example.com".to_string()),
        );
        assert_eq!(merged.model, "deepseek-reasoner");
        assert_eq!(merged.base_url, "https://env.example.com");
    }

    #[test]
    fn merge_partial_staged_only_touches_set_fields() {
        let partial = StagedModel {
            model: Some("deepseek-chat".to_string()),
            base_url: None,
            updated_ms: 1,
        };
        let merged = merge_staged_config(running(), &Some(partial), None, None);
        assert_eq!(merged.model, "deepseek-chat");
        assert_eq!(merged.base_url, "https://api.deepseek.com");
    }

    #[test]
    fn restart_needed_env_shadow_is_dormant_not_pending() {
        let run = running();
        let stage = staged();
        assert!(model_restart_needed(&run, &stage, None, None));
        // After a restart that applied staged, running == staged.
        let mut applied = run.clone();
        applied.model = "deepseek-chat".to_string();
        applied.base_url = "https://staged.example.com".to_string();
        assert!(!model_restart_needed(&applied, &stage, None, None));
        // With env overrides explaining both differences, the
        // staged values are dormant, not pending.
        assert!(!model_restart_needed(
            &run,
            &stage,
            Some("deepseek-reasoner"),
            Some("https://env.example.com")
        ));
    }
}

// --- Config surface (Task 14, GET only) ---
//
// GET /config reports gateway facts, read-only. PATCH is
// deliberately NOT implemented: the gateway has no
// runtime-mutable self-owned config to write (files_root
// is a startup jail root, model belongs to /model, the
// key belongs to the Keychain), so a PATCH would invent
// a dangerous write semantic. Axum answers PATCH on this
// path with 405, which the smoke test asserts.

#[derive(Debug, Serialize)]
struct ConfigView {
    files_root: String,
    gateway_addr: String,
    provider: &'static str,
}

async fn config_get(State(state): State<AppState>) -> Json<ConfigView> {
    // Same fallback as the binary's bind address in main.rs.
    let gateway_addr =
        std::env::var("MOCK_GATEWAY_ADDR").unwrap_or_else(|_| "127.0.0.1:3001".to_string());
    Json(ConfigView {
        files_root: state.files_root.display().to_string(),
        gateway_addr,
        provider: "deepseek",
    })
}

// --- Peers surface (Task 15, record subset) ---
//
// Local semantics only (see peer_store.rs): peers form by
// accepting an invite; an invite doubles as its request
// record; inbox deliver creates a pending item, decline
// resolves it. SAS, avatar/photo, and the fs/write
// discretion route are excluded.

fn persist_peer_line(state: &AppState, line: &PeerStateLine) {
    let Some(path) = &state.peer_state_path else { return };
    append_peer_state(path, line);
}

fn peer_not_found() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown peer" })))
}

fn invite_not_found() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown invite" })))
}

fn message_not_found() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown inbox message" })))
}

fn conflict(message: &str) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::CONFLICT, Json(serde_json::json!({ "error": message })))
}

async fn peers_list(State(state): State<AppState>) -> Json<Vec<Peer>> {
    let mut peers: Vec<Peer> = state
        .peer_peers
        .lock()
        .expect("peer lock")
        .values()
        .filter(|p| !p.disconnected)
        .cloned()
        .collect();
    peers.sort_by(|a, b| a.muse_id.cmp(&b.muse_id));
    Json(peers)
}

async fn peers_get(
    State(state): State<AppState>,
    axum::extract::Path(muse_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.peer_peers.lock().expect("peer lock").get(&muse_id) {
        Some(peer) => Json(peer.clone()).into_response(),
        None => peer_not_found().into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct PeerUpdateDto {
    display_name: Option<String>,
    status: Option<String>,
}

async fn peers_update(
    State(state): State<AppState>,
    axum::extract::Path(muse_id): axum::extract::Path<String>,
    Json(req): Json<PeerUpdateDto>,
) -> impl IntoResponse {
    let display_name = match req.display_name {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                return bad_request("display_name must not be empty").into_response();
            }
            Some(trimmed)
        }
        None => None,
    };
    let status = match req.status {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                return bad_request("status must not be empty").into_response();
            }
            Some(trimmed)
        }
        None => None,
    };
    let mut peers = state.peer_peers.lock().expect("peer lock");
    let Some(peer) = peers.get_mut(&muse_id) else {
        return peer_not_found().into_response();
    };
    if peer.disconnected {
        return peer_not_found().into_response();
    }
    if let Some(name) = display_name {
        peer.display_name = name;
    }
    if let Some(status) = status {
        peer.status = status;
    }
    peer.updated_ms = now_ms();
    let updated = peer.clone();
    persist_peer_line(&state, &PeerStateLine::Peer(updated.clone()));
    Json(updated).into_response()
}

async fn peers_disconnect(
    State(state): State<AppState>,
    axum::extract::Path(muse_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut peers = state.peer_peers.lock().expect("peer lock");
    let Some(peer) = peers.get_mut(&muse_id) else {
        return peer_not_found().into_response();
    };
    // Idempotent: disconnecting an already-disconnected
    // peer succeeds without a new record.
    if !peer.disconnected {
        peer.disconnected = true;
        peer.updated_ms = now_ms();
        let updated = peer.clone();
        persist_peer_line(&state, &PeerStateLine::Peer(updated.clone()));
        return Json(updated).into_response();
    }
    Json(peer.clone()).into_response()
}

async fn peers_invites_list(State(state): State<AppState>) -> Json<Vec<PeerInvite>> {
    let mut invites: Vec<PeerInvite> = state
        .peer_invites
        .lock()
        .expect("peer lock")
        .values()
        .filter(|i| !i.revoked)
        .cloned()
        .collect();
    invites.sort_by_key(|i| (i.created_ms, i.invite_id.clone()));
    Json(invites)
}

#[derive(Debug, Deserialize)]
struct PeerInviteCreateDto {
    muse_id: String,
    display_name: String,
}

async fn peers_invites_create(
    State(state): State<AppState>,
    Json(req): Json<PeerInviteCreateDto>,
) -> impl IntoResponse {
    let muse_id = req.muse_id.trim().to_string();
    let display_name = req.display_name.trim().to_string();
    if muse_id.is_empty() || display_name.is_empty() {
        return bad_request("muse_id and display_name must not be empty").into_response();
    }
    let now = now_ms();
    let invite_id = {
        let invites = state.peer_invites.lock().expect("peer lock");
        let records: Vec<PeerInvite> = invites.values().cloned().collect();
        format!("invite-{}", next_invite_number(&records))
    };
    let invite = PeerInvite {
        invite_id: invite_id.clone(),
        muse_id: muse_id.clone(),
        display_name: display_name.clone(),
        status: "pending".to_string(),
        created_ms: now,
        updated_ms: now,
        revoked: false,
    };
    let request = PeerRequest {
        relationship_generation_id: invite_id.clone(),
        muse_id,
        display_name,
        status: "pending".to_string(),
        created_ms: now,
    };
    state.peer_invites.lock().expect("peer lock").insert(invite_id, invite.clone());
    state.peer_requests.lock().expect("peer lock").insert(request.relationship_generation_id.clone(), request.clone());
    persist_peer_line(&state, &PeerStateLine::Invite(invite.clone()));
    persist_peer_line(&state, &PeerStateLine::Request(request));
    Json(invite).into_response()
}

#[derive(Debug, Deserialize)]
struct PeerInviteIdDto {
    invite_id: String,
}

async fn peers_invites_accept(
    State(state): State<AppState>,
    Json(req): Json<PeerInviteIdDto>,
) -> impl IntoResponse {
    let mut invites = state.peer_invites.lock().expect("peer lock");
    let Some(invite) = invites.get_mut(&req.invite_id) else {
        return invite_not_found().into_response();
    };
    if invite.revoked {
        return conflict("invite revoked").into_response();
    }
    // Idempotent: accepting an already-accepted invite
    // succeeds without duplicating the peer.
    if invite.status != "accepted" {
        invite.status = "accepted".to_string();
        invite.updated_ms = now_ms();
        let updated = invite.clone();
        let mut peers = state.peer_peers.lock().expect("peer lock");
        let peer = Peer {
            muse_id: updated.muse_id.clone(),
            display_name: updated.display_name.clone(),
            status: "connected".to_string(),
            updated_ms: now_ms(),
            disconnected: false,
        };
        peers.insert(peer.muse_id.clone(), peer.clone());
        let mut requests = state.peer_requests.lock().expect("peer lock");
        if let Some(request) = requests.get_mut(&req.invite_id) {
            request.status = "accepted".to_string();
            let request = request.clone();
            persist_peer_line(&state, &PeerStateLine::Request(request));
        }
        persist_peer_line(&state, &PeerStateLine::Invite(updated.clone()));
        persist_peer_line(&state, &PeerStateLine::Peer(peer));
        return Json(updated).into_response();
    }
    Json(invite.clone()).into_response()
}

async fn peers_invites_open(
    State(state): State<AppState>,
    Json(req): Json<PeerInviteIdDto>,
) -> impl IntoResponse {
    let mut invites = state.peer_invites.lock().expect("peer lock");
    let Some(invite) = invites.get_mut(&req.invite_id) else {
        return invite_not_found().into_response();
    };
    if invite.revoked {
        return conflict("invite revoked").into_response();
    }
    if invite.status == "accepted" {
        return conflict("invite already accepted").into_response();
    }
    if invite.status != "opened" {
        invite.status = "opened".to_string();
        invite.updated_ms = now_ms();
        let updated = invite.clone();
        persist_peer_line(&state, &PeerStateLine::Invite(updated.clone()));
        return Json(updated).into_response();
    }
    Json(invite.clone()).into_response()
}

async fn peers_invites_revoke(
    State(state): State<AppState>,
    axum::extract::Path(invite_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut invites = state.peer_invites.lock().expect("peer lock");
    let Some(invite) = invites.get_mut(&invite_id) else {
        return invite_not_found().into_response();
    };
    // Idempotent: revoking an already-revoked invite succeeds.
    if !invite.revoked {
        invite.revoked = true;
        invite.updated_ms = now_ms();
        let updated = invite.clone();
        let mut requests = state.peer_requests.lock().expect("peer lock");
        if let Some(request) = requests.get_mut(&invite_id) {
            request.status = "revoked".to_string();
            let request = request.clone();
            persist_peer_line(&state, &PeerStateLine::Request(request));
        }
        persist_peer_line(&state, &PeerStateLine::Invite(updated.clone()));
        return Json(updated).into_response();
    }
    Json(invite.clone()).into_response()
}

async fn peers_inbox_list(State(state): State<AppState>) -> Json<Vec<PeerInboxMessage>> {
    let mut messages: Vec<PeerInboxMessage> =
        state.peer_inbox.lock().expect("peer lock").values().cloned().collect();
    messages.sort_by_key(|m| (m.created_ms, m.message_id.clone()));
    Json(messages)
}

#[derive(Debug, Deserialize)]
struct PeerInboxDeliverDto {
    from_muse_id: String,
    text: String,
}

async fn peers_inbox_deliver(
    State(state): State<AppState>,
    Json(req): Json<PeerInboxDeliverDto>,
) -> impl IntoResponse {
    let from_muse_id = req.from_muse_id.trim().to_string();
    let text = req.text.trim().to_string();
    if from_muse_id.is_empty() || text.is_empty() {
        return bad_request("from_muse_id and text must not be empty").into_response();
    }
    let message_id = {
        let inbox = state.peer_inbox.lock().expect("peer lock");
        let records: Vec<PeerInboxMessage> = inbox.values().cloned().collect();
        format!("inbox-{}", next_message_number(&records))
    };
    let message = PeerInboxMessage {
        message_id: message_id.clone(),
        from_muse_id,
        text,
        status: "pending".to_string(),
        created_ms: now_ms(),
    };
    state.peer_inbox.lock().expect("peer lock").insert(message_id, message.clone());
    persist_peer_line(&state, &PeerStateLine::Inbox(message.clone()));
    Json(message).into_response()
}

#[derive(Debug, Deserialize)]
struct PeerInboxDeclineDto {
    message_id: String,
}

async fn peers_inbox_decline(
    State(state): State<AppState>,
    Json(req): Json<PeerInboxDeclineDto>,
) -> impl IntoResponse {
    let mut inbox = state.peer_inbox.lock().expect("peer lock");
    let Some(message) = inbox.get_mut(&req.message_id) else {
        return message_not_found().into_response();
    };
    // Idempotent: declining an already-declined message succeeds.
    if message.status != "declined" {
        message.status = "declined".to_string();
        let updated = message.clone();
        persist_peer_line(&state, &PeerStateLine::Inbox(updated.clone()));
        return Json(updated).into_response();
    }
    Json(message.clone()).into_response()
}

async fn peers_requests_list(State(state): State<AppState>) -> Json<Vec<PeerRequest>> {
    let mut requests: Vec<PeerRequest> =
        state.peer_requests.lock().expect("peer lock").values().cloned().collect();
requests.sort_by_key(|r| (r.created_ms, r.relationship_generation_id.clone()));
    Json(requests)
}

async fn peers_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let peers = state.peer_peers.lock().expect("peer lock").values().filter(|p| !p.disconnected).count();
    let pending_invites = state
        .peer_invites
        .lock()
        .expect("peer lock")
        .values()
        .filter(|i| !i.revoked && (i.status == "pending" || i.status == "opened"))
        .count();
    let pending_inbox = state
        .peer_inbox
        .lock()
        .expect("peer lock")
        .values()
        .filter(|m| m.status == "pending")
        .count();
    let pending_requests = state
        .peer_requests
        .lock()
        .expect("peer lock")
        .values()
        .filter(|r| r.status == "pending")
        .count();
    Json(serde_json::json!({
        "peers": peers,
        "pending_invites": pending_invites,
        "pending_inbox": pending_inbox,
        "pending_requests": pending_requests,
    }))
}

// --- Spaces surface (Task 16, v2 record subset) ---
//
// Local semantics only (see space_store.rs): save
// upserts a named collection of session/file references;
// delete is a tombstone; the ?source=saved_space variant
// has the same record effect as a plain delete.

fn persist_space_line(state: &AppState, line: &SpaceStateLine) {
    let Some(path) = &state.space_state_path else { return };
    append_space_state(path, line);
}

async fn spaces_list(State(state): State<AppState>) -> Json<Vec<Space>> {
    let mut spaces: Vec<Space> = state
        .spaces
        .lock()
        .expect("space lock")
        .values()
        .filter(|s| !s.deleted)
        .cloned()
        .collect();
    spaces.sort_by(|a, b| a.slug.cmp(&b.slug));
    Json(spaces)
}

#[derive(Debug, Deserialize)]
struct SpaceSaveDto {
    title: Option<String>,
    items: Option<Vec<SpaceItemDto>>,
}

#[derive(Debug, Deserialize)]
struct SpaceItemDto {
    kind: String,
    #[serde(rename = "ref")]
    reference: String,
}

async fn spaces_save(
    State(state): State<AppState>,
    axum::extract::Path(slug): axum::extract::Path<String>,
    Json(req): Json<SpaceSaveDto>,
) -> impl IntoResponse {
    let title = match req.title {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                return bad_request("title must not be empty").into_response();
            }
            trimmed
        }
        None => slug.clone(),
    };
    let mut items = Vec::new();
    for item in req.items.unwrap_or_default() {
        let kind = item.kind.trim().to_string();
        let reference = item.reference.trim().to_string();
        if kind != "session" && kind != "file" {
            return bad_request("item kind must be session or file").into_response();
        }
        if reference.is_empty() {
            return bad_request("item ref must not be empty").into_response();
        }
        items.push(SpaceItem { kind, reference });
    }
    let now = now_ms();
    let mut spaces = state.spaces.lock().expect("space lock");
    let space = match spaces.get_mut(&slug) {
        // Resurrecting a deleted slug starts fresh: the
        // old created_ms does not survive the tombstone.
        Some(existing) if !existing.deleted => {
            existing.title = title;
            existing.items = items;
            existing.updated_ms = now;
            existing.clone()
        }
        _ => {
            let fresh = Space {
                slug: slug.clone(),
                title,
                items,
                created_ms: now,
                updated_ms: now,
                deleted: false,
            };
            spaces.insert(slug, fresh.clone());
            fresh
        }
    };
    persist_space_line(&state, &SpaceStateLine::Space(space.clone()));
    Json(space).into_response()
}

async fn spaces_delete(
    State(state): State<AppState>,
    axum::extract::Path(slug): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut spaces = state.spaces.lock().expect("space lock");
    let Some(space) = spaces.get_mut(&slug) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown space" })))
            .into_response();
    };
    // Idempotent: deleting an already-deleted space
    // succeeds without a new record.
    if !space.deleted {
        space.deleted = true;
        space.updated_ms = now_ms();
        let updated = space.clone();
        persist_space_line(&state, &SpaceStateLine::Space(updated.clone()));
        return Json(updated).into_response();
    }
    Json(space.clone()).into_response()
}

// --- Node registry surface (Task 19, record subset) ---
//
// Invoke-ack, invoke-result, and sync are excluded (see
// the Task 19 plan): receipt/sync protocol with a real
// node, and there is no local counterpart.

fn persist_node(state: &AppState, record: &Node) {
    let Some(path) = &state.node_state_path else { return };
    append_node(path, record);
}

fn persist_binding(state: &AppState, record: &WorkspaceBinding) {
    let Some(path) = &state.binding_state_path else { return };
    append_binding(path, record);
}

fn persist_project(state: &AppState, record: &Project) {
    let Some(path) = &state.project_state_path else { return };
    append_project(path, record);
}

fn persist_location(state: &AppState, record: &Location) {
    let Some(path) = &state.location_state_path else { return };
    append_location(path, record);
}

fn persist_worktree(state: &AppState, record: &Worktree) {
    let Some(path) = &state.worktree_state_path else { return };
    append_worktree(path, record);
}

fn node_number(node_id: &str) -> u64 {
    node_id.strip_prefix("node-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

#[derive(Debug, Deserialize)]
struct NodeRegisterDto {
    name: Option<String>,
}

async fn nodes_register(
    State(state): State<AppState>,
    Json(req): Json<NodeRegisterDto>,
) -> Json<Node> {
    let mut next = state.next_node_id.lock().expect("node id lock");
    let id = format!("node-{}", *next);
    *next += 1;
    let now = now_ms();
    let record = Node {
        node_id: id.clone(),
        name: req.name.filter(|n| !n.trim().is_empty()).unwrap_or(id),
        status: NODE_ONLINE.to_string(),
        last_heartbeat_ms: now,
        created_ms: now,
        updated_ms: now,
        unpaired: false,
    };
    state.nodes.lock().expect("node lock").insert(record.node_id.clone(), record.clone());
    persist_node(&state, &record);
    Json(record)
}

#[derive(Debug, Deserialize)]
struct NodeListDto {
    status: Option<String>,
}

async fn nodes_list(
    State(state): State<AppState>,
    Json(req): Json<NodeListDto>,
) -> impl IntoResponse {
    if let Some(status) = &req.status {
        if status != NODE_ONLINE && status != NODE_DISCONNECTED {
            return bad_request("unknown node status").into_response();
        }
    }
    let nodes = state.nodes.lock().expect("node lock");
    let mut records: Vec<Node> = nodes
        .values()
        .filter(|n| !n.unpaired && req.status.as_ref().is_none_or(|s| s == &n.status))
        .cloned()
        .collect();
    records.sort_by_key(|n| node_number(&n.node_id));
    Json(records).into_response()
}

#[derive(Debug, Deserialize)]
struct NodeIdDto {
    node_id: String,
}

/// Shared mutation path for heartbeat/disconnect: unknown
/// ids and tombstoned nodes both read as 404; the mutation
/// bumps `updated_ms` and appends a fresh snapshot.
fn mutate_node(state: &AppState, node_id: &str, mutate: impl FnOnce(&mut Node)) -> Response {
    let mut nodes = state.nodes.lock().expect("node lock");
    let Some(record) = nodes.get_mut(node_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown node" })))
            .into_response();
    };
    if record.unpaired {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown node" })))
            .into_response();
    }
    mutate(record);
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_node(state, &updated);
    Json(updated).into_response()
}

async fn nodes_heartbeat(
    State(state): State<AppState>,
    Json(req): Json<NodeIdDto>,
) -> impl IntoResponse {
    mutate_node(&state, &req.node_id, |n| {
        n.status = NODE_ONLINE.to_string();
        n.last_heartbeat_ms = now_ms();
    })
}

async fn nodes_disconnect(
    State(state): State<AppState>,
    Json(req): Json<NodeIdDto>,
) -> impl IntoResponse {
    mutate_node(&state, &req.node_id, |n| {
        n.status = NODE_DISCONNECTED.to_string();
    })
}

async fn nodes_unpair(
    State(state): State<AppState>,
    Json(req): Json<NodeIdDto>,
) -> impl IntoResponse {
    let mut nodes = state.nodes.lock().expect("node lock");
    let Some(record) = nodes.get_mut(&req.node_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown node" })))
            .into_response();
    };
    if !record.unpaired {
        record.unpaired = true;
        record.updated_ms = now_ms();
        let updated = record.clone();
        persist_node(&state, &updated);
    }
    // Re-unpair is idempotent success, mirroring the
    // delete-turn tombstone: no new snapshot, same record.
    Json(record.clone()).into_response()
}

// --- Engineering record surface (P3-A, record subset) ---
//
// WorkspaceBinding + Project + Location + Worktree: four
// JSONL record stores, no Effect, no approval loop (direct
// management actions, like the Task 9 connector surface).
// List hides archived rows; get returns archived rows with
// their status. Archive is a status flip, never a delete:
// there is no hard-delete route and directories are never
// removed. Creation validation failures are all 400.

fn workspace_number(id: &str) -> u64 {
    id.strip_prefix("workspace-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn project_number(id: &str) -> u64 {
    id.strip_prefix("project-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn location_number(id: &str) -> u64 {
    id.strip_prefix("location-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn worktree_number(id: &str) -> u64 {
    id.strip_prefix("worktree-").and_then(|s| s.parse().ok()).unwrap_or(0)
}

fn unknown_record(what: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": format!("unknown {what}") })),
    )
        .into_response()
}

/// P3-B archive check (plan §3.8 "non-terminal Patch"): a
/// Worktree is blocked while any of its PatchFiles is pending
/// or failed — the work the Patch still owes. A stale Patch
/// with no pending/failed file does not block: its files are
/// terminally refused and the Patch is an audit row. Without
/// SQLite there is no Patch store, so nothing blocks.
fn worktree_archive_blocked(state: &AppState, worktree_id: &str) -> Option<&'static str> {
    if state.sqlite_mode() != StateBackend::Sqlite || state.sqlite_is_broken() {
        return None;
    }
    let Some(sqlite) = &state.sqlite else {
        return None;
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::worktree_has_actionable_patch(&conn, worktree_id) {
        Ok(true) => Some("worktree_has_active_patch"),
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct BindingCreateDto {
    display_name: Option<String>,
    path: Option<String>,
}

async fn bindings_create(
    State(state): State<AppState>,
    Json(req): Json<BindingCreateDto>,
) -> impl IntoResponse {
    let Some(display_name) = req.display_name.filter(|n| !n.trim().is_empty()) else {
        return bad_request("display_name is required");
    };
    let Some(raw_path) = req.path.filter(|p| !p.trim().is_empty()) else {
        return bad_request("path is required");
    };
    // The picked directory is canonicalized at creation (a
    // symlinked pick binds its real target); a missing path
    // or a non-directory is a 400.
    let Ok(canonical) = Path::new(&raw_path).canonicalize() else {
        return bad_request("directory does not exist");
    };
    if !canonical.is_dir() {
        return bad_request("path is not a directory");
    }
    let canonical_root = canonical.to_string_lossy().into_owned();
    let mut next = state.next_workspace_id.lock().expect("workspace id lock");
    let id = format!("workspace-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = WorkspaceBinding {
        workspace_id: id.clone(),
        display_name,
        allowed_roots: vec![canonical_root],
        policy_digest: compute_policy_digest(&state),
        status: BINDING_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
    };
    state
        .bindings
        .lock()
        .expect("binding lock")
        .insert(record.workspace_id.clone(), record.clone());
    persist_binding(&state, &record);
    Json(record).into_response()
}

async fn bindings_list(State(state): State<AppState>) -> Json<Vec<WorkspaceBinding>> {
    let bindings = state.bindings.lock().expect("binding lock");
    let mut records: Vec<WorkspaceBinding> = bindings
        .values()
        .filter(|b| b.status == BINDING_ACTIVE)
        .cloned()
        .collect();
    records.sort_by_key(|b| workspace_number(&b.workspace_id));
    Json(records)
}

async fn bindings_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let bindings = state.bindings.lock().expect("binding lock");
    match bindings.get(&id) {
        Some(record) => Json(record.clone()).into_response(),
        None => unknown_record("binding"),
    }
}

async fn bindings_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut bindings = state.bindings.lock().expect("binding lock");
    let Some(record) = bindings.get_mut(&id) else {
        return unknown_record("binding");
    };
    if record.status == BINDING_ARCHIVED {
        return Json(record.clone()).into_response();
    }
    // Archive guard (plan §3.8): an active Location blocks.
    let blocked = state
        .locations
        .lock()
        .expect("location lock")
        .values()
        .any(|l| l.workspace_id == id && l.status == LOCATION_ACTIVE);
    if blocked {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "binding_has_active_locations" })),
        )
            .into_response();
    }
    record.status = BINDING_ARCHIVED.to_string();
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_binding(&state, &updated);
    Json(updated).into_response()
}

#[derive(Debug, Deserialize)]
struct ProjectCreateDto {
    name: Option<String>,
    description: Option<String>,
}

async fn projects_create(
    State(state): State<AppState>,
    Json(req): Json<ProjectCreateDto>,
) -> impl IntoResponse {
    let Some(name) = req.name.filter(|n| !n.trim().is_empty()) else {
        return bad_request("name is required");
    };
    let Some(description) = req.description.filter(|d| !d.trim().is_empty()) else {
        return bad_request("description is required");
    };
    let mut next = state.next_project_id.lock().expect("project id lock");
    let id = format!("project-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = Project {
        project_id: id.clone(),
        name,
        description,
        status: PROJECT_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
    };
    state
        .projects
        .lock()
        .expect("project lock")
        .insert(record.project_id.clone(), record.clone());
    persist_project(&state, &record);
    Json(record).into_response()
}

async fn projects_list(State(state): State<AppState>) -> Json<Vec<Project>> {
    let projects = state.projects.lock().expect("project lock");
    let mut records: Vec<Project> = projects
        .values()
        .filter(|p| p.status == PROJECT_ACTIVE)
        .cloned()
        .collect();
    records.sort_by_key(|p| project_number(&p.project_id));
    Json(records)
}

async fn projects_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let projects = state.projects.lock().expect("project lock");
    match projects.get(&id) {
        Some(record) => Json(record.clone()).into_response(),
        None => unknown_record("project"),
    }
}

async fn projects_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    // A project is pure metadata: archive is always allowed.
    // Existing references keep reading back with status
    // archived; nothing is deleted or unlinked.
    let mut projects = state.projects.lock().expect("project lock");
    let Some(record) = projects.get_mut(&id) else {
        return unknown_record("project");
    };
    if record.status == PROJECT_ARCHIVED {
        return Json(record.clone()).into_response();
    }
    record.status = PROJECT_ARCHIVED.to_string();
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_project(&state, &updated);
    Json(updated).into_response()
}

// --- Goals (D1-1) ---
//
// A Goal is a first-class object a Task can link to
// (goal_id), managed directly like Project: no Effect, no
// approval loop (direct management actions, same rationale
// as the server.rs:7334-7341 comment on the four JSONL
// stores). SQLite only, no JSONL mirror. Archive is a
// status flip, never a delete; list hides archived rows,
// detail still reads them. Writes require SQLite mode
// (same fail-closed rule as the assignment takeover);
// reads serve whenever SQLite is attached.

const GOAL_ACTIVE: &str = "active";

#[derive(Debug, Deserialize)]
struct GoalCreateDto {
    title: Option<String>,
    description: Option<String>,
}

async fn goals_create(
    State(state): State<AppState>,
    Json(req): Json<GoalCreateDto>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let Some(title) = req.title.filter(|t| !t.trim().is_empty()) else {
        return bad_request("title is required");
    };
    let mut next = state.next_goal_id.lock().expect("goal id lock");
    let id = format!("goal-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = sqlite_store::GoalRow {
        goal_id: id,
        title,
        description: req.description.unwrap_or_default(),
        status: GOAL_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = sqlite_store::insert_goal(&mut conn, &record) {
        eprintln!("warning: goal insert failed ({detail})");
        return sqlite_error_response();
    }
    Json(record).into_response()
}

async fn goals_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_goals(&conn) {
        Ok(records) => Json(records).into_response(),
        Err(detail) => {
            eprintln!("warning: goal list failed ({detail})");
            sqlite_error_response()
        }
    }
}

async fn goals_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_goal(&conn, &id) {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => unknown_record("goal"),
        Err(detail) => {
            eprintln!("warning: goal get failed ({detail})");
            sqlite_error_response()
        }
    }
}

async fn goals_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    // A goal is pure metadata: archive is always allowed.
    // Existing references keep reading back with status
    // archived; nothing is deleted or unlinked.
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::archive_goal(&mut conn, &id) {
        Ok(Some(record)) => Json(record).into_response(),
        Ok(None) => unknown_record("goal"),
        Err(detail) => {
            eprintln!("warning: goal archive failed ({detail})");
            sqlite_error_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct LocationCreateDto {
    workspace_id: Option<String>,
    name: Option<String>,
    path: Option<String>,
}

async fn locations_create(
    State(state): State<AppState>,
    Json(req): Json<LocationCreateDto>,
) -> impl IntoResponse {
    let Some(workspace_id) = req.workspace_id.filter(|w| !w.trim().is_empty()) else {
        return bad_request("workspace_id is required");
    };
    let Some(name) = req.name.filter(|n| !n.trim().is_empty()) else {
        return bad_request("name is required");
    };
    let Some(raw_path) = req.path.filter(|p| !p.trim().is_empty()) else {
        return bad_request("path is required");
    };
    // The binding must exist and be active.
    let allowed_roots = {
        let bindings = state.bindings.lock().expect("binding lock");
        let Some(binding) = bindings.get(&workspace_id) else {
            return bad_request("unknown workspace");
        };
        if binding.status != BINDING_ACTIVE {
            return bad_request("workspace is archived");
        }
        binding.allowed_roots.clone()
    };
    // The candidate must exist and be a directory, and after
    // canonicalizing BOTH sides it must sit inside one of the
    // binding's allowed roots (settings_store's
    // validate_working_dir pattern: a symlinked escape
    // canonicalizes outside the root and fails here).
    let Ok(candidate) = Path::new(&raw_path).canonicalize() else {
        return bad_request("directory does not exist");
    };
    if !candidate.is_dir() {
        return bad_request("path is not a directory");
    }
    let inside = allowed_roots.iter().any(|root| {
        Path::new(root)
            .canonicalize()
            .is_ok_and(|canonical_root| candidate.strip_prefix(&canonical_root).is_ok())
    });
    if !inside {
        return bad_request("directory is outside the workspace binding");
    }
    let canonical_root = candidate.to_string_lossy().into_owned();
    let root_sha256 = location_root_sha256(&workspace_id, &canonical_root);
    let mut next = state.next_location_id.lock().expect("location id lock");
    let id = format!("location-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = Location {
        location_id: id.clone(),
        workspace_id,
        name,
        canonical_root,
        root_sha256,
        status: LOCATION_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
    };
    state
        .locations
        .lock()
        .expect("location lock")
        .insert(record.location_id.clone(), record.clone());
    persist_location(&state, &record);
    Json(record).into_response()
}

async fn locations_list(State(state): State<AppState>) -> Json<Vec<Location>> {
    let locations = state.locations.lock().expect("location lock");
    let mut records: Vec<Location> = locations
        .values()
        .filter(|l| l.status == LOCATION_ACTIVE)
        .cloned()
        .collect();
    records.sort_by_key(|l| location_number(&l.location_id));
    Json(records)
}

async fn locations_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let locations = state.locations.lock().expect("location lock");
    match locations.get(&id) {
        Some(record) => Json(record.clone()).into_response(),
        None => unknown_record("location"),
    }
}

async fn locations_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut locations = state.locations.lock().expect("location lock");
    let Some(record) = locations.get_mut(&id) else {
        return unknown_record("location");
    };
    if record.status == LOCATION_ARCHIVED {
        return Json(record.clone()).into_response();
    }
    // Archive guard (plan §3.8): an active Worktree blocks.
    let blocked = state
        .worktrees
        .lock()
        .expect("worktree lock")
        .values()
        .any(|w| w.location_id == id && w.status == WORKTREE_ACTIVE);
    if blocked {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "location_has_active_worktrees" })),
        )
            .into_response();
    }
    record.status = LOCATION_ARCHIVED.to_string();
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_location(&state, &updated);
    Json(updated).into_response()
}

#[derive(Debug, Deserialize)]
struct WorktreeCreateDto {
    location_id: Option<String>,
    directory_relative: Option<String>,
    branch_label: Option<String>,
    project_id: Option<String>,
}

/// Create a Worktree record (P3-A records, P3-C driver):
/// validates the Location (exists, active), normalizes the
/// relative directory, requires the directory to already
/// exist inside the Location root, then allocates the id and
/// persists. Returns the record or a short error code (the
/// HTTP route maps it to 400; the driver only warns). P3-A
/// creates no directories and runs no `git worktree add` —
/// the P3-C driver calls this after the command created it.
pub(crate) fn create_worktree_record(
    state: &AppState,
    location_id: String,
    directory_relative: String,
    branch_label: String,
    project_id: Option<String>,
) -> Result<Worktree, &'static str> {
    if branch_label.len() > 4096 {
        return Err("branch_label is too long");
    }
    // The location must exist and be active.
    let canonical_root = {
        let locations = state.locations.lock().expect("location lock");
        let Some(location) = locations.get(&location_id) else {
            return Err("unknown location");
        };
        if location.status != LOCATION_ACTIVE {
            return Err("location is archived");
        }
        match location_canonical_root(location) {
            Some(root) => root,
            None => return Err("location directory is unavailable"),
        }
    };
    // A stated project must exist (an archived project may
    // still be referenced; it reads back with its status).
    if let Some(project_id) = &project_id {
        let projects = state.projects.lock().expect("project lock");
        if !projects.contains_key(project_id) {
            return Err("unknown project");
        }
    }
    // `directory_relative` is lexically normalized (`..` and
    // absolute paths rejected); "" means the Location root.
    let relative = if directory_relative.is_empty() {
        String::new()
    } else {
        match crate::operation_binding::normalize_path(&directory_relative) {
            Some(normalized) => normalized,
            None => return Err("directory escapes the location"),
        }
    };
    // Join, canonicalize, and containment-check back inside
    // the root; the directory must already exist.
    let Some(_target) =
        crate::code_search::worktree_canonical_dir(&canonical_root, &relative)
    else {
        return Err("directory does not exist or escapes the location");
    };
    let mut next = state.next_worktree_id.lock().expect("worktree id lock");
    let id = format!("worktree-{}", *next);
    *next += 1;
    drop(next);
    let now = now_ms();
    let record = Worktree {
        worktree_id: id.clone(),
        location_id,
        directory_relative: relative,
        branch_label,
        project_id,
        status: WORKTREE_ACTIVE.to_string(),
        created_ms: now,
        updated_ms: now,
    };
    state
        .worktrees
        .lock()
        .expect("worktree lock")
        .insert(record.worktree_id.clone(), record.clone());
    persist_worktree(state, &record);
    Ok(record)
}

async fn worktrees_create(
    State(state): State<AppState>,
    Json(req): Json<WorktreeCreateDto>,
) -> impl IntoResponse {
    let Some(location_id) = req.location_id.filter(|l| !l.trim().is_empty()) else {
        return bad_request("location_id is required");
    };
    match create_worktree_record(
        &state,
        location_id,
        req.directory_relative.unwrap_or_default(),
        req.branch_label.unwrap_or_default(),
        req.project_id,
    ) {
        Ok(record) => Json(record).into_response(),
        Err(code) => bad_request(code),
    }
}

async fn worktrees_list(State(state): State<AppState>) -> Json<Vec<Worktree>> {
    let worktrees = state.worktrees.lock().expect("worktree lock");
    let mut records: Vec<Worktree> = worktrees
        .values()
        .filter(|w| w.status == WORKTREE_ACTIVE)
        .cloned()
        .collect();
    records.sort_by_key(|w| worktree_number(&w.worktree_id));
    Json(records)
}

async fn worktrees_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let worktrees = state.worktrees.lock().expect("worktree lock");
    match worktrees.get(&id) {
        Some(record) => Json(record.clone()).into_response(),
        None => unknown_record("worktree"),
    }
}

async fn worktrees_archive(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut worktrees = state.worktrees.lock().expect("worktree lock");
    let Some(record) = worktrees.get_mut(&id) else {
        return unknown_record("worktree");
    };
    if record.status == WORKTREE_ARCHIVED {
        return Json(record.clone()).into_response();
    }
    // Archive guard (plan §3.8): a non-terminal Patch blocks.
    // P3-A has no Patch store; the check point lives in
    // worktree_archive_blocked for P3-B to wire.
    if let Some(code) = worktree_archive_blocked(&state, &id) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": code })),
        )
            .into_response();
    }
    record.status = WORKTREE_ARCHIVED.to_string();
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_worktree(&state, &updated);
    Json(updated).into_response()
}

// --- Chat record surface (Task 18, record subset) ---
//
// Threads are sessions (thread_id == session_id); the static
// /chat/history seed is untouched. Message indices are stored
// positions (oldest-first), stable across tombstones; views
// hide deleted turns but report stored indices. The stream
// form (subscribe, stream-events, cancel, stream, poke,
// sxs.select) is covered separately in Task 20; see
// stream_store.rs.

fn chat_turn_view(thread_id: &str, index: usize, turn: &SessionTurn) -> serde_json::Value {
    serde_json::json!({
        "thread_id": thread_id,
        "index": index,
        "role": turn.role,
        "text": turn.text,
        "created_ms": turn.created_ms,
        "model": turn.model,
        "finish_reason": turn.finish_reason,
        "deleted": turn.deleted,
    })
}

fn persist_chat_line(state: &AppState, line: &ChatStateLine) {
    let Some(path) = &state.chat_state_path else { return };
    append_chat_state(path, line);
}

/// Unread entries for one record: visible turns at or after
/// the thread's seen watermark.
fn thread_unread(record: &SessionRecord, marker: usize) -> usize {
    record
        .turns
        .iter()
        .enumerate()
        .filter(|(index, turn)| !turn.deleted && *index >= marker)
        .count()
}

async fn chat_themes(State(state): State<AppState>) -> Json<serde_json::Value> {
    let theme = state.chat_theme.lock().expect("chat theme lock").clone();
    Json(serde_json::json!({ "theme": theme, "available": CHAT_THEMES }))
}

#[derive(Debug, Deserialize)]
struct ChatThemeSetDto {
    theme: String,
}

async fn chat_themes_set(
    State(state): State<AppState>,
    Json(req): Json<ChatThemeSetDto>,
) -> impl IntoResponse {
    if !CHAT_THEMES.contains(&req.theme.as_str()) {
        return bad_request("unknown theme").into_response();
    }
    *state.chat_theme.lock().expect("chat theme lock") = req.theme.clone();
    persist_chat_line(&state, &ChatStateLine::Theme { theme: req.theme.clone() });
    Json(serde_json::json!({ "theme": req.theme, "available": CHAT_THEMES })).into_response()
}

async fn chat_unread_count(State(state): State<AppState>) -> Json<serde_json::Value> {
    let seen = state.chat_seen.lock().expect("chat seen lock");
    let sessions = state.sessions.lock().expect("session lock");
    let mut records: Vec<&SessionRecord> = sessions.values().filter(|r| !r.deleted).collect();
    records.sort_by_key(|r| (!r.pinned, session_number(&r.session_id)));
    let mut threads = Vec::new();
    let mut total = 0;
    for record in records {
        let marker = seen.get(&record.session_id).copied().unwrap_or(0);
        let unread = thread_unread(record, marker);
        total += unread;
        threads.push(serde_json::json!({ "thread_id": record.session_id, "unread": unread }));
    }
    Json(serde_json::json!({ "unread_count": total, "threads": threads }))
}

#[derive(Debug, Deserialize)]
struct ChatSeenDto {
    index: Option<usize>,
}

async fn chat_mark_seen(
    State(state): State<AppState>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
    Json(req): Json<ChatSeenDto>,
) -> impl IntoResponse {
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(&thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let len = record.turns.len();
    let marker = match req.index {
        Some(index) if index >= len => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
                .into_response();
        }
        Some(index) => index + 1,
        None => len,
    };
    let unread = thread_unread(record, marker);
    state.chat_seen.lock().expect("chat seen lock").insert(thread_id.clone(), marker);
    persist_chat_line(
        &state,
        &ChatStateLine::Seen(SeenMarker { thread_id: thread_id.clone(), marker, updated_ms: now_ms() }),
    );
    Json(serde_json::json!({ "thread_id": thread_id, "marker": marker, "unread": unread }))
        .into_response()
}

#[derive(Debug, Deserialize)]
struct ChatDeleteTurnDto {
    thread_id: String,
    index: usize,
}

async fn chat_delete_turn(
    State(state): State<AppState>,
    Json(req): Json<ChatDeleteTurnDto>,
) -> impl IntoResponse {
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(&req.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    if req.index >= record.turns.len() {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
            .into_response();
    }
    if !record.turns[req.index].deleted {
        record.turns[req.index].deleted = true;
        record.updated_ms = now_ms();
        let updated = record.clone();
        persist_session(&state, &updated);
    }
    let view = chat_turn_view(&req.thread_id, req.index, &record.turns[req.index]);
    Json(view).into_response()
}

#[derive(Debug, Deserialize)]
struct ChatMessageQuery {
    thread_id: String,
    index: usize,
}

async fn chat_message(
    State(state): State<AppState>,
    Query(query): Query<ChatMessageQuery>,
) -> impl IntoResponse {
    let sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get(&query.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let Some(turn) = record.turns.get(query.index) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
            .into_response();
    };
    Json(chat_turn_view(&query.thread_id, query.index, turn)).into_response()
}

#[derive(Debug, Deserialize)]
struct ChatHistoryWindowQuery {
    thread_id: String,
    offset: Option<usize>,
    limit: Option<usize>,
}

async fn chat_history_window(
    State(state): State<AppState>,
    Query(query): Query<ChatHistoryWindowQuery>,
) -> impl IntoResponse {
    let sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get(&query.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let visible: Vec<(usize, &SessionTurn)> =
        record.turns.iter().enumerate().filter(|(_, turn)| !turn.deleted).collect();
    let total_visible = visible.len();
    let offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(50).min(200);
    let entries: Vec<serde_json::Value> = visible
        .iter()
        .skip(offset)
        .take(limit)
        .map(|(index, turn)| chat_turn_view(&query.thread_id, *index, turn))
        .collect();
    Json(serde_json::json!({
        "thread_id": query.thread_id,
        "total_visible": total_visible,
        "entries": entries,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct ChatSearchQuery {
    q: String,
}

async fn chat_search(
    State(state): State<AppState>,
    Query(query): Query<ChatSearchQuery>,
) -> impl IntoResponse {
    let needle = query.q.trim().to_lowercase();
    if needle.is_empty() {
        return bad_request("search query must not be empty").into_response();
    }
    let sessions = state.sessions.lock().expect("session lock");
    let mut records: Vec<&SessionRecord> = sessions.values().filter(|r| !r.deleted).collect();
    records.sort_by_key(|r| (!r.pinned, session_number(&r.session_id)));
    let mut matches = Vec::new();
    for record in records {
        for (index, turn) in record.turns.iter().enumerate() {
            if turn.deleted {
                continue;
            }
            if turn.text.to_lowercase().contains(&needle) {
                matches.push(serde_json::json!({
                    "thread_id": record.session_id,
                    "index": index,
                    "role": turn.role,
                    "text": turn.text,
                }));
                if matches.len() >= 50 {
                    break;
                }
            }
        }
        if matches.len() >= 50 {
            break;
        }
    }
    Json(matches).into_response()
}

// --- Chat stream surface (Task 20, stream form subset) ---
//
// Local semantics only (see stream_store.rs and the Task 20
// plan): subscribe + stream-events as a long-poll record
// channel over a thread's visible turns, thread-level
// cancel, subscription-style stream open, poke event
// injection, and record-only sxs.select. Events are a
// projection, not a live token stream; cancel stops
// delivery, never an in-flight model call.

fn stream_thread_exists(state: &AppState, thread_id: &str) -> bool {
    let sessions = state.sessions.lock().expect("session lock");
    sessions.get(thread_id).is_some_and(|r| !r.deleted)
}

fn stream_thread_turns(state: &AppState, thread_id: &str) -> Option<Vec<SessionTurn>> {
    let sessions = state.sessions.lock().expect("session lock");
    let record = sessions.get(thread_id)?;
    if record.deleted {
        return None;
    }
    Some(record.turns.clone())
}

fn stream_thread_pokes(state: &AppState, thread_id: &str) -> Vec<StreamPoke> {
    state
        .stream_pokes
        .lock()
        .expect("stream pokes lock")
        .iter()
        .filter(|p| p.thread_id == thread_id)
        .cloned()
        .collect()
}

fn persist_stream_line(state: &AppState, line: &StreamLine) {
    let Some(path) = &state.stream_state_path else {
        return;
    };
    append_stream_line(path, line);
}

fn stream_event_view(event: &crate::stream_store::ProjectedEvent) -> serde_json::Value {
    serde_json::json!({
        "id": event.id,
        "type": event.event_type,
        "turn_index": event.turn_index,
        "text": event.text,
        "created_ms": event.created_ms,
    })
}

#[derive(Debug, Deserialize)]
struct StreamThreadDto {
    thread_id: String,
}

async fn chat_subscribe(
    State(state): State<AppState>,
    Json(req): Json<StreamThreadDto>,
) -> impl IntoResponse {
    let Some(turns) = stream_thread_turns(&state, &req.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    let pokes = stream_thread_pokes(&state, &req.thread_id);
    let events = project_thread_events(&turns, &pokes, &req.thread_id);
    let cursor = last_event_id(&events);
    let mut id_guard = state.next_subscription_id.lock().expect("subscription id lock");
    let number = *id_guard;
    *id_guard += 1;
    drop(id_guard);
    let record = StreamSubscription {
        subscription_id: format!("sub-{number}"),
        thread_id: req.thread_id.clone(),
        created_ms: now_ms(),
        cancelled: false,
    };
    state
        .stream_subscriptions
        .lock()
        .expect("stream subscriptions lock")
        .insert(record.subscription_id.clone(), record.clone());
    persist_stream_line(&state, &StreamLine::Subscription(record.clone()));
    Json(serde_json::json!({
        "subscription_id": record.subscription_id,
        "thread_id": record.thread_id,
        "cursor": cursor,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct StreamEventsDto {
    subscription_id: String,
    cursor: Option<String>,
    wait_ms: Option<u64>,
}

async fn chat_stream_events(
    State(state): State<AppState>,
    Json(req): Json<StreamEventsDto>,
) -> impl IntoResponse {
    let cursor = req.cursor.clone().unwrap_or_default();
    let wait = req.wait_ms.unwrap_or(0).min(25_000);
    let Some(subscription) = state
        .stream_subscriptions
        .lock()
        .expect("stream subscriptions lock")
        .get(&req.subscription_id)
        .cloned()
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown subscription" })),
        )
            .into_response();
    };
    if subscription.cancelled {
        return Json(serde_json::json!({
            "subscription_id": subscription.subscription_id,
            "cursor": cursor,
            "events": [],
            "cancelled": true,
        }))
        .into_response();
    }
    // Long-poll mirroring files_subscribe: re-check each
    // round so a cancel or a new turn lands promptly.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait);
    loop {
        let cancelled = state
            .stream_subscriptions
            .lock()
            .expect("stream subscriptions lock")
            .get(&req.subscription_id)
            .is_some_and(|s| s.cancelled);
        if cancelled {
            return Json(serde_json::json!({
                "subscription_id": subscription.subscription_id,
                "cursor": cursor,
                "events": [],
                "cancelled": true,
            }))
            .into_response();
        }
        let Some(turns) = stream_thread_turns(&state, &subscription.thread_id) else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "unknown thread" })),
            )
                .into_response();
        };
        let pokes = stream_thread_pokes(&state, &subscription.thread_id);
        let events = project_thread_events(&turns, &pokes, &subscription.thread_id);
        match events_after(&events, &cursor) {
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({ "error": "unknown cursor" })),
                )
                    .into_response();
            }
            Some(delivered) if !delivered.is_empty() || wait == 0 => {
                let next_cursor = delivered
                    .last()
                    .map(|e| e.id.clone())
                    .unwrap_or(cursor.clone());
                return Json(serde_json::json!({
                    "subscription_id": subscription.subscription_id,
                    "cursor": next_cursor,
                    "events": delivered.iter().map(stream_event_view).collect::<Vec<_>>(),
                    "cancelled": false,
                }))
                .into_response();
            }
            Some(_) => {
                if tokio::time::Instant::now() >= deadline {
                    return Json(serde_json::json!({
                        "subscription_id": subscription.subscription_id,
                        "cursor": cursor,
                        "events": [],
                        "cancelled": false,
                    }))
                    .into_response();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn chat_cancel(
    State(state): State<AppState>,
    Json(req): Json<StreamThreadDto>,
) -> impl IntoResponse {
    if !stream_thread_exists(&state, &req.thread_id) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let mut cancelled_lines = Vec::new();
    let mut count = 0;
    {
        let mut subscriptions = state.stream_subscriptions.lock().expect("stream subscriptions lock");
        for subscription in subscriptions
            .values_mut()
            .filter(|s| s.thread_id == req.thread_id && !s.cancelled)
        {
            subscription.cancelled = true;
            count += 1;
            cancelled_lines.push(StreamLine::Subscription(subscription.clone()));
        }
    }
    for line in &cancelled_lines {
        persist_stream_line(&state, line);
    }
    Json(serde_json::json!({
        "thread_id": req.thread_id,
        "cancelled_subscriptions": count,
    }))
    .into_response()
}

async fn chat_stream_open(
    State(state): State<AppState>,
    Json(req): Json<StreamThreadDto>,
) -> impl IntoResponse {
    if !stream_thread_exists(&state, &req.thread_id) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let mut id_guard = state.next_stream_id.lock().expect("stream id lock");
    let number = *id_guard;
    *id_guard += 1;
    drop(id_guard);
    let record = StreamOpen {
        stream_id: format!("stream-{number}"),
        thread_id: req.thread_id.clone(),
        created_ms: now_ms(),
    };
    persist_stream_line(&state, &StreamLine::StreamOpen(record.clone()));
    Json(serde_json::json!({
        "stream_id": record.stream_id,
        "thread_id": record.thread_id,
    }))
    .into_response()
}

async fn chat_poke(
    State(state): State<AppState>,
    Json(req): Json<StreamThreadDto>,
) -> impl IntoResponse {
    if !stream_thread_exists(&state, &req.thread_id) {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    }
    let mut order_guard = state.next_poke_order.lock().expect("poke order lock");
    let order = *order_guard;
    *order_guard += 1;
    drop(order_guard);
    let record = StreamPoke { thread_id: req.thread_id.clone(), created_ms: now_ms(), order };
    state.stream_pokes.lock().expect("stream pokes lock").push(record.clone());
    persist_stream_line(&state, &StreamLine::Poke(record.clone()));
    Json(serde_json::json!({
        "thread_id": record.thread_id,
        "event_id": format!("poke:{order}"),
        "created_ms": record.created_ms,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
struct SxsSelectDto {
    thread_id: String,
    index: usize,
}

async fn chat_sxs_select(
    State(state): State<AppState>,
    Json(req): Json<SxsSelectDto>,
) -> impl IntoResponse {
    let Some(turns) = stream_thread_turns(&state, &req.thread_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown thread" })))
            .into_response();
    };
    let Some(turn) = turns.get(req.index) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
            .into_response();
    };
    if turn.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown turn" })))
            .into_response();
    }
    let record = SxsSelection {
        thread_id: req.thread_id.clone(),
        index: req.index,
        updated_ms: now_ms(),
    };
    state
        .stream_selections
        .lock()
        .expect("stream selections lock")
        .insert(req.thread_id.clone(), record.clone());
    persist_stream_line(&state, &StreamLine::SxsSelect(record.clone()));
    Json(serde_json::json!({
        "thread_id": req.thread_id,
        "index": req.index,
    }))
    .into_response()
}

// --- Voice record surface (Task 21, record subset, D2) ---
//
// Local semantics only (see voice_store.rs and the Task 21
// plan): static [Mock] catalog and pipeline seeds, custom
// voice rename/delete over seeds, record-only active voice
// and pipeline selections, and an onboarding reset that
// clears just those two selections. No audio plays here.

fn persist_voice_line(state: &AppState, line: &VoiceLine) {
    let Some(path) = &state.voice_state_path else {
        return;
    };
    append_voice_line(path, line);
}

fn custom_voice_view(voice: &CustomVoice) -> serde_json::Value {
    serde_json::json!({
        "saved_voice_id": voice.saved_voice_id,
        "name": voice.name,
        "created_ms": voice.created_ms,
        "updated_ms": voice.updated_ms,
        "deleted": voice.deleted,
    })
}

async fn voice_catalog() -> Json<Vec<crate::voice_store::CatalogVoice>> {
    Json(catalog_voices())
}

async fn voice_catalog_filters() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "locales": catalog_locales() }))
}

async fn voice_available_voices() -> Json<Vec<crate::voice_store::CatalogVoice>> {
    Json(catalog_voices().into_iter().filter(|v| v.available).collect())
}

async fn voice_pipeline_profiles() -> Json<Vec<crate::voice_store::PipelineProfile>> {
    Json(pipeline_profiles())
}

#[derive(Debug, Deserialize)]
struct VoiceProfileSelectDto {
    profile_id: String,
}

async fn voice_pipeline_profile_select(
    State(state): State<AppState>,
    Json(req): Json<VoiceProfileSelectDto>,
) -> impl IntoResponse {
    if !pipeline_profiles().iter().any(|p| p.profile_id == req.profile_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown pipeline profile" })),
        )
            .into_response();
    }
    *state.voice_pipeline.lock().expect("voice pipeline lock") = Some(req.profile_id.clone());
    persist_voice_line(
        &state,
        &VoiceLine::PipelineSelection(PipelineSelectionRecord {
            profile_id: Some(req.profile_id.clone()),
            updated_ms: now_ms(),
        }),
    );
    Json(serde_json::json!({ "profile_id": req.profile_id })).into_response()
}

async fn voice_custom_voices_list(State(state): State<AppState>) -> Json<Vec<serde_json::Value>> {
    let customs = state.voice_custom.lock().expect("voice custom lock");
    let mut voices: Vec<&CustomVoice> = customs.values().filter(|v| !v.deleted).collect();
    voices.sort_by(|a, b| a.saved_voice_id.cmp(&b.saved_voice_id));
    Json(voices.iter().map(|v| custom_voice_view(v)).collect())
}

#[derive(Debug, Deserialize)]
struct VoiceRenameDto {
    name: String,
}

async fn voice_custom_voice_rename(
    State(state): State<AppState>,
    axum::extract::Path(saved_voice_id): axum::extract::Path<String>,
    Json(req): Json<VoiceRenameDto>,
) -> impl IntoResponse {
    let mut customs = state.voice_custom.lock().expect("voice custom lock");
    let Some(voice) = customs.get_mut(&saved_voice_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown custom voice" })),
        )
            .into_response();
    };
    if voice.deleted {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown custom voice" })),
        )
            .into_response();
    }
    if req.name.trim().is_empty() {
        return bad_request("name must not be empty").into_response();
    }
    voice.name = req.name.clone();
    voice.updated_ms = now_ms();
    let updated = voice.clone();
    drop(customs);
    persist_voice_line(&state, &VoiceLine::CustomVoice(updated.clone()));
    Json(custom_voice_view(&updated)).into_response()
}

async fn voice_custom_voice_delete(
    State(state): State<AppState>,
    axum::extract::Path(saved_voice_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut customs = state.voice_custom.lock().expect("voice custom lock");
    let Some(voice) = customs.get_mut(&saved_voice_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown custom voice" })),
        )
            .into_response();
    };
    if !voice.deleted {
        voice.deleted = true;
        voice.updated_ms = now_ms();
        let updated = voice.clone();
        drop(customs);
        persist_voice_line(&state, &VoiceLine::CustomVoice(updated.clone()));
        return Json(custom_voice_view(&updated)).into_response();
    }
    let view = custom_voice_view(voice);
    Json(view).into_response()
}

async fn voice_active_voice_get(State(state): State<AppState>) -> Json<serde_json::Value> {
    let voice_id = state.voice_active.lock().expect("voice active lock").clone();
    Json(serde_json::json!({ "voice_id": voice_id }))
}

#[derive(Debug, Deserialize)]
struct VoiceActiveSaveDto {
    voice_id: String,
}

async fn voice_active_voice_save(
    State(state): State<AppState>,
    Json(req): Json<VoiceActiveSaveDto>,
) -> impl IntoResponse {
    let in_catalog = catalog_voices().iter().any(|v| v.voice_id == req.voice_id);
    let in_custom = state
        .voice_custom
        .lock()
        .expect("voice custom lock")
        .get(&req.voice_id)
        .is_some_and(|v| !v.deleted);
    if !in_catalog && !in_custom {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown voice" })),
        )
            .into_response();
    }
    *state.voice_active.lock().expect("voice active lock") = Some(req.voice_id.clone());
    persist_voice_line(
        &state,
        &VoiceLine::ActiveVoice(ActiveVoiceRecord {
            voice_id: Some(req.voice_id.clone()),
            updated_ms: now_ms(),
        }),
    );
    Json(serde_json::json!({ "voice_id": req.voice_id })).into_response()
}

async fn voice_onboarding_reset(State(state): State<AppState>) -> Json<serde_json::Value> {
    // D2: clears only the two selections, back to null.
    // Custom voices are untouched.
    *state.voice_active.lock().expect("voice active lock") = None;
    *state.voice_pipeline.lock().expect("voice pipeline lock") = None;
    let now = now_ms();
    persist_voice_line(
        &state,
        &VoiceLine::ActiveVoice(ActiveVoiceRecord { voice_id: None, updated_ms: now }),
    );
    persist_voice_line(
        &state,
        &VoiceLine::PipelineSelection(PipelineSelectionRecord {
            profile_id: None,
            updated_ms: now,
        }),
    );
    Json(serde_json::json!({ "voice_id": null, "profile_id": null }))
}

// --- Email read surface (Task 22, batch 1, option A) ---
//
// Live queries against the configured MailSource, no
// sync cache, no approval loop (reads). Error mapping:
// NotFound -> 404, Unavailable -> 501, Denied -> 403,
// Upstream -> 502. The packaged Mac app uses the
// AppleScript source; other platforms answer 501.

/// P2: loop-tool rendering for mail reads. Plain text so the model
/// can cite message/thread ids directly. When the rendering exceeds
/// the loop's 4096-byte tool-result bound, a truncation notice
/// leads: it survives ToolMessage::from_outcome's cut from the
/// start, while the summary's truncated flag still reports the cut.
/// Bodies are never logged; only the rendered tool result (which
/// the model sees) carries the notice.
const EMAIL_TRUNCATION_NOTICE: &str =
    "（注：内容过长已截断，仅展示前一部分；摘要/引用时请注明基于已读部分。）";

fn email_rendered(rendered: String) -> String {
    if rendered.len() > 4096 {
        format!("{EMAIL_TRUNCATION_NOTICE}\n{rendered}")
    } else {
        rendered
    }
}

/// P2: generic loop-tool error text for mail failures. Mirrors
/// mail_error_response's public strings, never the underlying
/// detail (stderr may contain local paths).
fn email_error_text(err: MailError) -> String {
    match err {
        MailError::NotFound(what) => format!("unknown {what}"),
        MailError::Unavailable(_) => "mail source unavailable".to_string(),
        MailError::Denied => "mail automation denied".to_string(),
        MailError::Upstream(_) => "mail source error".to_string(),
    }
}

fn mail_error_response(err: MailError, not_found_what: &'static str) -> axum::response::Response {
    match err {
        MailError::NotFound(_) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("unknown {not_found_what}") })),
        )
            .into_response(),
        MailError::Unavailable(reason) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({ "error": reason })),
        )
            .into_response(),
        MailError::Denied => (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({ "error": "mail automation denied" })),
        )
            .into_response(),
        MailError::Upstream(_) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({ "error": "mail source error" })),
        )
            .into_response(),
    }
}

async fn email_mailbox(State(state): State<AppState>) -> impl IntoResponse {
    match state.mail_source.mailbox() {
        Ok(info) => Json(serde_json::to_value(info).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "mailbox"),
    }
}

async fn email_mailbox_setup(State(state): State<AppState>) -> impl IntoResponse {
    match state.mail_source.setup() {
        Ok(info) => Json(serde_json::to_value(info).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "mailbox"),
    }
}

#[derive(Debug, Deserialize)]
struct EmailThreadsQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

async fn email_threads_list(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<EmailThreadsQuery>,
) -> impl IntoResponse {
    let limit = clamp_limit(query.limit);
    let offset = query.offset.unwrap_or(0);
    match state.mail_source.threads(limit, offset) {
        Ok(threads) => Json(serde_json::to_value(threads).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "threads"),
    }
}

async fn email_thread_get(
    State(state): State<AppState>,
    axum::extract::Path(thread_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.mail_source.thread(&thread_id) {
        Ok(thread) => Json(serde_json::to_value(thread).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "thread"),
    }
}

async fn email_message_get(
    State(state): State<AppState>,
    axum::extract::Path(message_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.mail_source.message(&message_id) {
        Ok(message) => Json(serde_json::to_value(message).unwrap_or(serde_json::Value::Null)).into_response(),
        Err(err) => mail_error_response(err, "message"),
    }
}

async fn email_attachment_get(
    State(state): State<AppState>,
    axum::extract::Path((message_id, index)): axum::extract::Path<(String, usize)>,
) -> impl IntoResponse {
    match state.mail_source.attachment(&message_id, index) {
        Ok(data) => {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_str(&data.content_type)
                    .unwrap_or_else(|_| axum::http::HeaderValue::from_static("application/octet-stream")),
            );
            headers.insert(
                axum::http::header::CONTENT_DISPOSITION,
                axum::http::HeaderValue::from_str(&format!("attachment; filename=\"{}\"", data.filename))
                    .unwrap_or_else(|_| axum::http::HeaderValue::from_static("attachment")),
            );
            (headers, axum::body::Bytes::from(data.bytes)).into_response()
        }
        Err(err) => mail_error_response(err, "attachment"),
    }
}

// --- Feed surface (Task 17, unit subset) ---
//
// Local semantics only (see feed_store.rs): Mock seed
// units with list/infinite/status/get/delete/reaction/
// tap/seen. Prompt, regenerate, reorder, search,
// social-source-icon, and media are excluded.

fn persist_feed_line(state: &AppState, line: &FeedStateLine) {
    let Some(path) = &state.feed_state_path else { return };
    append_feed_state(path, line);
}

fn feed_visible_units(state: &AppState) -> Vec<FeedUnit> {
    let mut units: Vec<FeedUnit> = state
        .feed_units
        .lock()
        .expect("feed lock")
        .values()
        .filter(|u| !u.deleted)
        .cloned()
        .collect();
    units.sort_by_key(|u| (u.created_ms, u.unit_id.clone()));
    units
}

async fn feed_list(State(state): State<AppState>) -> Json<Vec<FeedUnit>> {
    Json(feed_visible_units(&state))
}

// Infinite scroll returns the same visible units: without
// a known pagination schema, inventing cursor params
// would be a fake semantic, so this is a documented alias.
async fn feed_infinite(State(state): State<AppState>) -> Json<Vec<FeedUnit>> {
    Json(feed_visible_units(&state))
}

async fn feed_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let units = state.feed_units.lock().expect("feed lock");
    let visible: Vec<&FeedUnit> = units.values().filter(|u| !u.deleted).collect();
    Json(serde_json::json!({
        "units": visible.len(),
        "unseen": visible.iter().filter(|u| !u.seen).count(),
    }))
}

async fn feed_unit_get(
    State(state): State<AppState>,
    axum::extract::Path(unit_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    match state.feed_units.lock().expect("feed lock").get(&unit_id) {
        Some(unit) => Json(unit.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response(),
    }
}

async fn feed_unit_delete(
    State(state): State<AppState>,
    axum::extract::Path(unit_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut units = state.feed_units.lock().expect("feed lock");
    let Some(unit) = units.get_mut(&unit_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    };
    // Idempotent: deleting an already-deleted unit
    // succeeds without a new record.
    if !unit.deleted {
        unit.deleted = true;
        unit.updated_ms = now_ms();
        let updated = unit.clone();
        persist_feed_line(&state, &FeedStateLine::Unit(updated.clone()));
        return Json(updated).into_response();
    }
    Json(unit.clone()).into_response()
}

#[derive(Debug, Deserialize)]
struct FeedReactionDto {
    reaction: String,
}

async fn feed_unit_reaction(
    State(state): State<AppState>,
    axum::extract::Path(unit_id): axum::extract::Path<String>,
    Json(req): Json<FeedReactionDto>,
) -> impl IntoResponse {
    let reaction = req.reaction.trim().to_string();
    if reaction.is_empty() {
        return bad_request("reaction must not be empty").into_response();
    }
    let mut units = state.feed_units.lock().expect("feed lock");
    let Some(unit) = units.get_mut(&unit_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    };
    if unit.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    }
    unit.reaction = Some(reaction);
    unit.updated_ms = now_ms();
    let updated = unit.clone();
    persist_feed_line(&state, &FeedStateLine::Unit(updated.clone()));
    Json(updated).into_response()
}

async fn feed_unit_tap(
    State(state): State<AppState>,
    axum::extract::Path(unit_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut units = state.feed_units.lock().expect("feed lock");
    let Some(unit) = units.get_mut(&unit_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    };
    if unit.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown unit" })))
            .into_response();
    }
    unit.tap_count += 1;
    unit.updated_ms = now_ms();
    let updated = unit.clone();
    persist_feed_line(&state, &FeedStateLine::Unit(updated.clone()));
    Json(updated).into_response()
}

#[derive(Debug, Deserialize)]
struct FeedSeenDto {
    unit_ids: Vec<String>,
}

async fn feed_units_seen(
    State(state): State<AppState>,
    Json(req): Json<FeedSeenDto>,
) -> Json<serde_json::Value> {
    let mut units = state.feed_units.lock().expect("feed lock");
    let mut seen = Vec::new();
    let mut unknown = Vec::new();
    for unit_id in req.unit_ids {
        match units.get_mut(&unit_id) {
            Some(unit) if !unit.deleted => {
                if !unit.seen {
                    unit.seen = true;
                    unit.updated_ms = now_ms();
                    let updated = unit.clone();
                    persist_feed_line(&state, &FeedStateLine::Unit(updated.clone()));
                }
                seen.push(unit_id);
            }
            _ => unknown.push(unit_id),
        }
    }
    Json(serde_json::json!({ "seen": seen, "unknown": unknown }))
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
    /// D1-1: structural links to the scene the task serves.
    /// Optional; absent stays None (unlinked).
    #[serde(default)]
    goal_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaskGetQuery {
    task_id: String,
    /// E2 slice: optional bound worktree for the execute
    /// step. Validated, and frozen on first use (scheme A,
    /// P3-C plan §2.3): a later execute naming a different
    /// worktree is 409 `task_binding_frozen_mismatch`.
    #[serde(default)]
    worktree_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaskUpdateDto {
    task_id: String,
    status: String,
    artifact_path: Option<String>,
    /// D1-1: link fields, same shape as the create DTO.
    /// Present values replace the link; absent leaves it.
    /// (No clearing through this route: absent and null
    /// are indistinguishable in the DTO.)
    #[serde(default)]
    goal_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
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

/// Migrated-domain read snapshots. In SQLite mode these read
/// SQLite (the authority); in shadow / JSONL mode they read
/// the in-memory maps. `None` means SQLite mode with no usable
/// connection (broken): callers must fail closed, never fall
/// back to the maps or to JSONL.
fn executions_snapshot(state: &AppState) -> Option<Vec<ExecutionFileRecord>> {
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return None;
        }
        let sqlite = state.sqlite.as_ref()?;
        let conn = sqlite.lock().expect("sqlite lock");
        return sqlite_store::list_executions(&conn).ok();
    }
    Some(
        state
            .executions
            .lock()
            .expect("execution lock")
            .values()
            .cloned()
            .collect(),
    )
}

fn effects_snapshot(state: &AppState) -> Option<Vec<EffectFileRecord>> {
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return None;
        }
        let sqlite = state.sqlite.as_ref()?;
        let conn = sqlite.lock().expect("sqlite lock");
        return sqlite_store::list_effects(&conn).ok();
    }
    Some(
        state
            .effects
            .lock()
            .expect("effect lock")
            .values()
            .cloned()
            .collect(),
    )
}

fn sqlite_error_response() -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": "sqlite_unavailable" })),
    )
        .into_response()
}

/// P0-D persist for executions. Shadow mode appends JSONL
/// (authoritative) and shadow-writes SQLite (failure warns +
/// counts, never changes the response). SQLite mode writes
/// SQLite in one transaction (event + state + derived Inbox)
/// and regenerates the JSONL audit export best-effort; no
/// JSONL append happens. A SQLite-mode failure marks the state
/// broken so migrated-domain routes fail closed.
/// Plus the P0-E creation-time workspace snapshot, written in
/// the SAME SQLite transaction
/// as the execution row (never a second transaction: a crash
/// between the two would leave an execution without its locked
/// snapshot, or a snapshot without its execution). Only
/// `create_execution` passes a snapshot; every later write of
/// the same execution passes None, so the snapshot row is
/// created exactly once and never rewritten.
pub(crate) fn persist_execution_with_snapshot(
    state: &AppState,
    record: &ExecutionFileRecord,
    snapshot: Option<&sqlite_store::SnapshotRow>,
) {
    match state.sqlite_mode() {
        StateBackend::Sqlite => {
            let Some(sqlite) = &state.sqlite else {
                *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
                eprintln!("warning: P0-D SQLite unavailable for execution {}", record.execution_id);
                return;
            };
            let mut conn = sqlite.lock().expect("sqlite lock");
            let result = match snapshot {
                Some(row) => sqlite_store::upsert_execution_with_snapshot(&mut conn, record, row),
                None => sqlite_store::upsert_execution(&mut conn, record),
            };
            match result {
                Ok(()) => {
                    if let Some(path) = &state.executions_path {
                        if let Err(detail) = sqlite_store::export_executions(&conn, path) {
                            eprintln!("warning: P0-D executions audit export failed: {detail}");
                        }
                    }
                }
                Err(detail) => {
                    *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
                    eprintln!("warning: P0-D SQLite execution persist failed: {detail}");
                }
            }
        }
        StateBackend::Shadow => {
            if let Some(path) = &state.executions_path {
                append_execution(path, record);
            }
            if let Some(sqlite) = &state.sqlite {
                let mut conn = sqlite.lock().expect("sqlite lock");
                let result = match snapshot {
                    Some(row) => {
                        sqlite_store::upsert_execution_with_snapshot(&mut conn, record, row)
                    }
                    None => sqlite_store::upsert_execution(&mut conn, record),
                };
                if let Err(detail) = result {
                    *state.sqlite_shadow_failures.lock().expect("shadow failures lock") += 1;
                    eprintln!("warning: P0-D shadow execution write failed: {detail}");
                }
            }
        }
        StateBackend::Jsonl => {
            if let Some(path) = &state.executions_path {
                append_execution(path, record);
            }
        }
    }
}

fn execution_number(execution_id: &str) -> u64 {
    execution_id.strip_prefix("execution-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

/// Phase 1 creation entry: Task Runner and Loop Engine each call
/// this when they start an attempt. The old task-N / loop-N id is
/// kept as a reference; exactly one of the two is set. The record
/// is created with status "running" and is not transitioned by
/// phase 1 (state convergence is phase 4). A task resume spawns a
/// fresh runner attempt, so it gets a fresh Execution; whether a
/// resume continues the same Execution is a phase 4 (Q6) decision,
/// not something this shell may pre-decide. Old paths are
/// untouched: this only adds a record.
/// P0-E snapshot content for one new Execution (plan §3.2).
/// Every value comes from a store this process already holds;
/// nothing is invented. `files_root` is the gateway state home
/// (the SQLite file's parent directory when SQLite is
/// configured, else the files root), made absolute — an
/// APPROXIMATION, not a modelled Workspace root: no Workspace
/// object exists yet, and this comment must stay attached to
/// the value until one does. `policy_binding_ids_json` holds
/// content digests, not record ids, because the policy store
/// has no policy-record ids to reference (network rules are a
/// list, permissions are a settings block). `memory_scope` is
/// the literal "default" placeholder (no memory-scope concept
/// exists yet; the column is NOT NULL, so it cannot be empty).
/// The snapshot id is tied to the execution number
/// (`snapshot-N` for `execution-N`): one execution, one
/// snapshot, no second counter that could race the id lock.
/// P3-A: the two policy digests the snapshot freezes as
/// `policy_binding_ids_json` entries, shared with the
/// binding's single `policy_digest` so no third digest
/// vocabulary exists. `network-rules` is the live network
/// rule list; `permissions` is the effective policy.
fn policy_digests(state: &AppState) -> (String, String) {
    let rules = state.policy_rules.lock().expect("policy rules lock").clone().unwrap_or_default();
    let rules_digest = crate::policy_store::network_rules_digest(&rules);
    let effective = policy_effective(state);
    let effective_json = serde_json::to_value(&effective).expect("effective policy serialize");
    let permissions_digest =
        sqlite_store::sha256_hex(sqlite_store::canonical_json(&effective_json).as_bytes());
    (rules_digest, permissions_digest)
}

/// E2-B §3.4: retain one network rules version under its
/// snapshot digest, so an Execution created while these rules
/// were live keeps resolving to them after a later change.
/// Old versions are never evicted (see the field doc).
fn retain_policy_rules_version(state: &AppState, rules: &[NetworkRule]) {
    let digest = crate::policy_store::network_rules_digest(rules);
    state
        .policy_rule_versions
        .lock()
        .expect("policy rule versions lock")
        .insert(digest, rules.to_vec());
}

/// E2-B §3.4 (snapshot gate): the network rules an
/// Execution was created under, resolved from its frozen
/// snapshot digest — NOT live policy. Returns None when the
/// Execution has no snapshot (JSONL mode), the snapshot
/// carries no network-rules digest, or the version is not
/// retained; the caller falls back to live rules then. Only
/// the network-rules component is gated: it is the only
/// policy enforced at execution time (the model deny check);
/// the snapshot's `permissions` digest has no execution-time
/// enforcement point to gate — its readers are config routes
/// and creation-time snapshots.
pub(crate) fn frozen_network_rules_for_execution(
    state: &AppState,
    execution_id: &str,
) -> Option<Vec<NetworkRule>> {
    let sqlite = state.sqlite.as_ref()?;
    if state.sqlite_is_broken() {
        return None;
    }
    let conn = sqlite.lock().expect("sqlite lock");
    let execution = sqlite_store::get_execution(&conn, execution_id).ok()??;
    let snapshot_id = execution.workspace_snapshot_id?;
    let snapshot = sqlite_store::get_snapshot(&conn, &snapshot_id).ok()??;
    let ids: Vec<String> = serde_json::from_str(&snapshot.policy_binding_ids_json).ok()?;
    let digest = ids.iter().find_map(|entry| {
        entry
            .strip_prefix("network-rules:sha256:")
            .map(|hex| hex.to_string())
    })?;
    state
        .policy_rule_versions
        .lock()
        .expect("policy rule versions lock")
        .get(&digest)
        .cloned()
}

/// P3-A: the binding's single policy digest, frozen at
/// binding creation: `sha256(canonical_json({network_rules,
/// effective_permissions}))` over the two digests above.
pub(crate) fn compute_policy_digest(state: &AppState) -> String {
    let (rules_digest, permissions_digest) = policy_digests(state);
    let value = serde_json::json!({
        "network_rules": rules_digest,
        "effective_permissions": permissions_digest,
    });
    sqlite_store::sha256_hex(sqlite_store::canonical_json(&value).as_bytes())
}

/// P3-A: a Location's root fingerprint:
/// `sha256(canonical_json({workspace_id, canonical_root}))`.
/// It detects "the root was rebound", not content drift.
pub(crate) fn location_root_sha256(workspace_id: &str, canonical_root: &str) -> String {
    let value = serde_json::json!({
        "workspace_id": workspace_id,
        "canonical_root": canonical_root,
    });
    sqlite_store::sha256_hex(sqlite_store::canonical_json(&value).as_bytes())
}

/// P3-A: the binding frozen into a new Execution's snapshot.
/// All three values come from the bound Worktree's Location;
/// `root_digest` is that Location's `root_sha256`.
#[derive(Debug, Clone)]
pub(crate) struct SnapshotBinding {
    pub location_id: String,
    pub worktree_id: String,
    pub root_digest: String,
}

/// Resolve a worktree id to its snapshot binding. The
/// worktree must exist and be active, and its Location must
/// exist and be active; anything else is a caller error.
pub(crate) fn resolve_worktree_binding(
    state: &AppState,
    worktree_id: &str,
) -> Result<SnapshotBinding, &'static str> {
    let worktrees = state.worktrees.lock().expect("worktree lock");
    let Some(worktree) = worktrees.get(worktree_id) else {
        return Err("unknown worktree");
    };
    if worktree.status != WORKTREE_ACTIVE {
        return Err("worktree is archived");
    }
    let location_id = worktree.location_id.clone();
    let worktree_id = worktree.worktree_id.clone();
    drop(worktrees);
    let locations = state.locations.lock().expect("location lock");
    let Some(location) = locations.get(&location_id) else {
        return Err("unknown location");
    };
    if location.status != LOCATION_ACTIVE {
        return Err("location is archived");
    }
    Ok(SnapshotBinding {
        location_id,
        worktree_id,
        root_digest: location.root_sha256.clone(),
    })
}

/// Canonical directory of a worktree: its Location's
/// canonical root joined with `directory_relative`, then
/// canonicalized and containment-checked back inside the
/// root (a symlinked `directory_relative` that escapes is
/// rejected). `None` when the directory does not exist or
/// escapes.
pub(crate) fn worktree_canonical_root(state: &AppState, worktree: &Worktree) -> Option<PathBuf> {
    let locations = state.locations.lock().expect("location lock");
    let location = locations.get(&worktree.location_id)?;
    let root = Path::new(&location.canonical_root);
    crate::code_search::worktree_canonical_dir(root, &worktree.directory_relative)
}

/// P3-A: the canonical root of a Location, for containment
/// checks. The stored `canonical_root` was canonicalized at
/// creation; re-canonicalizing here keeps the check honest
/// if the directory was replaced by a symlink since.
/// Canonical root of a Location, by id (P3-C: the driver
/// and the observe path relativize run paths against it).
/// None when the Location is missing, archived, or its
/// directory is unavailable.
pub(crate) fn location_root_for_id(state: &AppState, location_id: &str) -> Option<PathBuf> {
    let locations = state.locations.lock().expect("location lock");
    let location = locations.get(location_id)?;
    if location.status != LOCATION_ACTIVE {
        return None;
    }
    location_canonical_root(location)
}

fn location_canonical_root(location: &Location) -> Option<PathBuf> {
    let canonical = Path::new(&location.canonical_root).canonicalize().ok()?;
    if !canonical.is_dir() {
        return None;
    }
    Some(canonical)
}

fn build_workspace_snapshot(
    state: &AppState,
    snapshot_id: &str,
    instruction_digest: &str,
    binding: Option<&SnapshotBinding>,
) -> sqlite_store::SnapshotRow {
    let home = match &state.sqlite_path {
        Some(path) => path.parent().map(|parent| parent.to_path_buf()),
        None => None,
    }
    .unwrap_or_else(|| state.files_root.clone());
    let absolute = if home.is_absolute() {
        home.clone()
    } else {
        std::env::current_dir().unwrap_or_default().join(&home)
    };
    let files_root = absolute
        .canonicalize()
        .unwrap_or(absolute)
        .to_string_lossy()
        .into_owned();
    // D1-1 content digest: same canonical-JSON + SHA-256
    // algorithm as `snapshot_content_sha256`, applied to one
    // source's content (handle + status, no secret, no
    // timestamps) so a config drift changes the snapshot
    // hash while a routine heartbeat does not.
    let content_digest = |value: serde_json::Value| {
        sqlite_store::sha256_hex(sqlite_store::canonical_json(&value).as_bytes())
    };
    // Connector entries: connected connections, excluding a
    // connector whose accounts are all unlinked (connected
    // lives on the connection row, unlinked on the account row;
    // a connector with no accounts at all still counts when its
    // connection says connected). Each entry is
    // "<id>:sha256:<digest>" where the digest covers the
    // connection state and the account handles only.
    // Sorted, deduplicated.
    let connections = state.connector_connections.lock().expect("connector connections lock");
    let accounts = state.connector_accounts.lock().expect("connector accounts lock");
    let mut connector_ids: Vec<String> = connections
        .iter()
        .filter(|(_, connected)| **connected)
        .map(|(id, _)| id.clone())
        .filter(|id| {
            let rows: Vec<_> = accounts.values().filter(|a| &a.connector_id == id).collect();
            rows.is_empty() || rows.iter().any(|a| !a.unlinked)
        })
        .map(|id| {
            let mut handles: Vec<(&str, bool)> = accounts
                .values()
                .filter(|a| a.connector_id == id)
                .map(|a| (a.account_id.as_str(), a.unlinked))
                .collect();
            handles.sort();
            let digest = content_digest(serde_json::json!({
                "accounts": handles
                    .iter()
                    .map(|(account_id, unlinked)| serde_json::json!({
                        "account_id": account_id,
                        "unlinked": unlinked,
                    }))
                    .collect::<Vec<_>>(),
                "connected": true,
                "connector_id": id,
            }));
            format!("{id}:sha256:{digest}")
        })
        .collect();
    drop(accounts);
    drop(connections);
    connector_ids.sort();
    connector_ids.dedup();
    // Device entries: non-unpaired nodes, same
    // "<id>:sha256:<digest>" shape. The digest covers the
    // node's identity and status only — no heartbeat or
    // creation timestamps.
    let mut device_ids: Vec<String> = state
        .nodes
        .lock()
        .expect("node lock")
        .values()
        .filter(|node| !node.unpaired)
        .map(|node| {
            let digest = content_digest(serde_json::json!({
                "name": node.name,
                "node_id": node.node_id,
                "status": node.status,
                "unpaired": node.unpaired,
            }));
            format!("{}:sha256:{digest}", node.node_id)
        })
        .collect();
    device_ids.sort();
    device_ids.dedup();
    let (rules_digest, permissions_digest) = policy_digests(state);
    let policy_binding_ids_json = serde_json::to_string(&[
        format!("network-rules:sha256:{rules_digest}"),
        format!("permissions:sha256:{permissions_digest}"),
    ])
    .expect("policy bindings serialize");
    let mut row = sqlite_store::SnapshotRow {
        snapshot_id: snapshot_id.to_string(),
        content_sha256: String::new(),
        files_root,
        connector_ids_json: serde_json::to_string(&connector_ids).expect("connector ids serialize"),
        device_ids_json: serde_json::to_string(&device_ids).expect("device ids serialize"),
        policy_binding_ids_json,
        // D1-1 placeholder: the real scope value is filled
        // in by D1-5 (Q-D1-52); this batch only annotates the
        // hardcoded "default", it does not change the value.
        memory_scope: "default".to_string(),
        instruction_digest: instruction_digest.to_string(),
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
        binding_location_id: binding.map(|b| b.location_id.clone()),
        binding_worktree_id: binding.map(|b| b.worktree_id.clone()),
        binding_root_digest: binding.map(|b| b.root_digest.clone()),
    };
    row.content_sha256 = sqlite_store::snapshot_content_sha256(&row);
    row
}

pub(crate) fn create_execution(
    state: &AppState,
    task_id: Option<String>,
    loop_id: Option<String>,
) -> ExecutionFileRecord {
    // D1-1: a task execution serves the task's linked goal,
    // resolved from the Task record here. Tasks carry no
    // session in their request, so session_id stays None
    // for task executions (loop executions pass theirs
    // from the loop start caller).
    let goal_id = task_id.as_ref().and_then(|id| {
        state
            .tasks
            .lock()
            .expect("task lock")
            .get(id)
            .and_then(|record| record.goal_id.clone())
    });
    create_execution_with_binding(state, task_id, loop_id, None, None, goal_id)
}

/// P3-A: Execution creation with an optional frozen binding.
/// The snapshot's three binding columns come from here; every
/// other creation path passes `None` and keeps them NULL.
/// D1-1: `session_id` and `goal_id` are passed through by
/// the caller that has them (loop start passes both from
/// the StartLoop request; task executions resolve the goal
/// from the Task record in `create_execution` above and
/// pass no session; worker runs and adapters pass neither).
pub(crate) fn create_execution_with_binding(
    state: &AppState,
    task_id: Option<String>,
    loop_id: Option<String>,
    binding: Option<SnapshotBinding>,
    session_id: Option<String>,
    goal_id: Option<String>,
) -> ExecutionFileRecord {
    let mut next = state.next_execution_id.lock().expect("execution id lock");
    let number = *next;
    *next += 1;
    drop(next);
    let id = format!("execution-{number}");
    // P0-E fills the two P0-A reserved fields at creation.
    // Worker: the process's current WorkerSession, when one
    // exists (SQLite mode after the recovery scan; pure JSONL
    // mode has no WorkerSession concept, so this stays None).
    // Instruction template digest (plan §3.3): the task prompt
    // prefix, the loop system constant, or the empty string for
    // adapter executions — the template, never a synthesized
    // prompt (synthesis happens after creation, inside the
    // driver, and cannot be snapshotted here).
    let instruction_digest = if task_id.is_some() {
        sqlite_store::sha256_hex(crate::task_runner::PROMPT_PREFIX.as_bytes())
    } else if loop_id.is_some() {
        sqlite_store::sha256_hex(crate::deepseek_loop::SYSTEM.as_bytes())
    } else {
        sqlite_store::sha256_hex(b"")
    };
    let worker_session_id = state.current_worker_session();
    let snapshot = if state.sqlite.is_some() {
        Some(build_workspace_snapshot(
            state,
            &format!("snapshot-{number}"),
            &instruction_digest,
            binding.as_ref(),
        ))
    } else {
        None
    };
    let record = ExecutionFileRecord {
        execution_id: id,
        task_id,
        loop_id,
        session_id,
        goal_id,
        worker_session_id,
        workspace_snapshot_id: snapshot.as_ref().map(|row| row.snapshot_id.clone()),
        status: "running".to_string(),
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    };
    state.executions.lock().expect("execution lock").insert(record.execution_id.clone(), record.clone());
    persist_execution_with_snapshot(state, &record, snapshot.as_ref());
    // D1-2 (Q-D1-22): bind (or reuse) a Mandate for this
    // Execution.
    bind_execution_mandate(state, &record);
    record
}

fn task_number(task_id: &str) -> u64 {
    task_id.strip_prefix("task-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

fn effect_number(effect_id: &str) -> u64 {
    effect_id.strip_prefix("effect-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

/// P0-D persist for effects: same mode rules as
/// `persist_execution` (shadow dual-write; SQLite primary +
/// audit export; failure marks the state broken).
pub(crate) fn persist_effect(state: &AppState, record: &EffectFileRecord) {
    match state.sqlite_mode() {
        StateBackend::Sqlite => {
            let Some(sqlite) = &state.sqlite else {
                *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
                eprintln!("warning: P0-D SQLite unavailable for effect {}", record.effect_id);
                return;
            };
            let mut conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::upsert_effect(&mut conn, record) {
                Ok(()) => {
                    if let Some(path) = &state.effects_path {
                        if let Err(detail) = sqlite_store::export_effects(&conn, path) {
                            eprintln!("warning: P0-D effects audit export failed: {detail}");
                        }
                    }
                }
                Err(detail) => {
                    *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
                    eprintln!("warning: P0-D SQLite effect persist failed: {detail}");
                }
            }
        }
        StateBackend::Shadow => {
            if let Some(path) = &state.effects_path {
                append_effect(path, record);
            }
            if let Some(sqlite) = &state.sqlite {
                let mut conn = sqlite.lock().expect("sqlite lock");
                if let Err(detail) = sqlite_store::upsert_effect(&mut conn, record) {
                    *state.sqlite_shadow_failures.lock().expect("shadow failures lock") += 1;
                    eprintln!("warning: P0-D shadow effect write failed: {detail}");
                }
            }
        }
        StateBackend::Jsonl => {
            if let Some(path) = &state.effects_path {
                append_effect(path, record);
            }
        }
    }
}

/// P0-R: write one write-time Evidence row for a just-succeeded
/// Effect, best-effort. Evidence is derived data: it never fails
/// the effect write, never marks SQLite broken, and never runs
/// without a configured SQLite connection (the events-table
/// precedent: in pure JSONL mode Evidence simply does not
/// exist). Device rows are excluded here (their mirror comes
/// from reconcile, not from the write); export rows are
/// excluded (the target path is not on the Effect row — the
/// export sites call `record_export_evidence` explicitly).
/// Observation failure means no row, never a fabricated row.
pub(crate) fn record_write_time_evidence(state: &AppState, record: &EffectFileRecord) {
    if record.status != "succeeded" {
        return;
    }
    if matches!(record.capability.as_str(), "device" | "devices") {
        return;
    }
    if record.capability == "fs" && record.operation == "export" {
        return;
    }
    let Some(draft) = crate::evidence::write_time_observation(state, record) else {
        return;
    };
    record_evidence_best_effort(state, &draft);
}

/// Best-effort Evidence insert: warn-only, never
/// broken-marking, never counted as a shadow failure (Evidence
/// is derived, not authoritative state). No-op without a
/// configured SQLite connection.
///
/// E2-C §4.5: a successful insert also regenerates the
/// Evidence audit export (evidence.jsonl) when an export path
/// is configured — same best-effort contract as the insert
/// itself; an export failure only warns.
pub(crate) fn record_evidence_best_effort(state: &AppState, draft: &crate::evidence::OwnedEvidence) {
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let Ok(mut conn) = sqlite.lock() else {
        eprintln!("warning: evidence insert skipped (sqlite lock)");
        return;
    };
    if let Err(detail) = crate::sqlite_store::insert_evidence(&mut conn, &draft.as_draft()) {
        eprintln!("warning: evidence insert failed: {detail}");
        return;
    }
    export_evidence_best_effort(state, &conn);
}

/// E2-C §4.5: regenerate the Evidence audit export
/// (evidence.jsonl) from the connection the caller already
/// holds, when an export path is configured. Best-effort,
/// warn-only — an export failure never changes the caller's
/// response.
fn export_evidence_best_effort(state: &AppState, conn: &Connection) {
    if let Some(path) = &state.evidence_path {
        if let Err(detail) = crate::sqlite_store::export_evidence(conn, path) {
            eprintln!("warning: evidence export failed: {detail}");
        }
    }
}

/// P0-R: write-time Evidence for an export success, from the
/// absolute target path the Effect row does not carry.
/// Best-effort, same contract as `record_evidence_best_effort`.
pub(crate) fn record_export_evidence(
    state: &AppState,
    effect_id: &str,
    execution_id: Option<&str>,
    target_abs: &std::path::Path,
) {
    let Some(draft) = crate::evidence::export_write_time(state, effect_id, execution_id, target_abs)
    else {
        return;
    };
    record_evidence_best_effort(state, &draft);
}

/// Phase 2 dual-write entry: append one Effect line mirroring a
/// durable side effect, and keep it in the in-memory map for the
/// read-only `/effects` routes. Best-effort like every other JSONL
/// append here: a missing path means effects are not persisted for
/// this run, and an append failure only warns. The caller has
/// already produced the mirrored outcome; this never changes the
/// caller's response and never gates execution.
pub(crate) fn record_effect(state: &AppState, effect: NewEffect) -> EffectFileRecord {
    let mut next = state.next_effect_id.lock().expect("effect id lock");
    let id = format!("effect-{}", *next);
    *next += 1;
    // P0-A only adds the idempotency key and provenance. The key
    // is computed here, centrally, from the record's own fields:
    // it exists exactly when the execution id and the digest are
    // both present, so direct writes without an execution keep a
    // null key (forcing attribution is P0-B's job). No gate reads
    // the key yet, and no call site changes shape.
    let idempotency_key = match (&effect.execution_id, &effect.input_digest) {
        (Some(execution_id), Some(digest)) => Some(crate::effect_store::idempotency_key(
            execution_id,
            &effect.capability,
            &effect.operation,
            &effect.scope,
            digest,
        )),
        _ => None,
    };
    let record = EffectFileRecord {
        effect_id: id,
        execution_id: effect.execution_id,
        capability: effect.capability,
        operation: effect.operation,
        scope: effect.scope,
        input_digest: effect.input_digest,
        approval_id: effect.approval_id,
        status: effect.status,
        idempotency_key,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
        reconciliation: None,
        detail: None,
    };
    state.effects.lock().expect("effect lock").insert(record.effect_id.clone(), record.clone());
    persist_effect(state, &record);
    // P0-R: a directly recorded `succeeded` row gets its
    // write-time baseline now (planned/failed rows: no-op).
    record_write_time_evidence(state, &record);
    record
}

/// P0-C lifecycle driver: append one transition line for an
/// existing Effect and keep the in-memory map on the last line.
/// Returns the new row, or an HTTP status: 404 for an unknown
/// effect, 409 for a refused transition (including a legacy row
/// whose old status word is not in the lifecycle vocabulary:
/// history rows are never translated, so they cannot be driven
/// forward). A transition never rewrites history: the old line
/// stays in the JSONL file, last line wins. Persist appends are
/// best-effort like every other store here (memory is
/// authoritative for the run); a failed append only warns.
pub(crate) fn transition_effect(
    state: &AppState,
    effect_id: &str,
    to: &str,
    reconciliation: Option<Reconciliation>,
    detail: Option<String>,
) -> Result<EffectFileRecord, u16> {
    if !crate::effect_store::is_new_effect_status(to) {
        return Err(400);
    }
    // P0-D: in SQLite mode the current row comes from SQLite
    // (broken SQLite -> 500, never a stale map row).
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = effects_snapshot(state) else {
            return Err(500);
        };
        let Some(current) = records.into_iter().find(|r| r.effect_id == effect_id) else {
            return Err(404);
        };
        if !crate::effect_store::valid_effect_transition(&current.status, to) {
            return Err(409);
        }
        let mut next = current.clone();
        next.status = to.to_string();
        if reconciliation.is_some() {
            next.reconciliation = reconciliation;
        }
        if detail.is_some() {
            next.detail = detail;
        }
        state.effects.lock().expect("effect lock").insert(next.effect_id.clone(), next.clone());
        persist_effect(state, &next);
        if state.sqlite_is_broken() {
            return Err(500);
        }
        // P0-R: a transition to `succeeded` gets its write-time
        // baseline now (other targets: no-op).
        record_write_time_evidence(state, &next);
        return Ok(next);
    }
    let mut effects = state.effects.lock().expect("effect lock");
    let Some(current) = effects.get(effect_id).cloned() else {
        return Err(404);
    };
    if !crate::effect_store::valid_effect_transition(&current.status, to) {
        return Err(409);
    }
    let mut next = current.clone();
    next.status = to.to_string();
    if reconciliation.is_some() {
        next.reconciliation = reconciliation;
    }
    if detail.is_some() {
        next.detail = detail;
    }
    effects.insert(next.effect_id.clone(), next.clone());
    drop(effects);
    persist_effect(state, &next);
    // P0-R: same write-time baseline as the SQLite branch.
    record_write_time_evidence(state, &next);
    Ok(next)
}

/// Drive an Effect through a compressed chain (contract §4:
/// process states may be compressed, order never changes). Used
/// by flows with no waiting step between states, e.g. connector
/// writes. Stops at the first refused transition and reports it.
pub(crate) fn transition_effect_chain(
    state: &AppState,
    effect_id: &str,
    steps: &[&str],
) -> Result<EffectFileRecord, u16> {
    let mut last = state
        .effects
        .lock()
        .expect("effect lock")
        .get(effect_id)
        .cloned()
        .ok_or(404u16)?;
    for step in steps {
        last = transition_effect(state, effect_id, step, None, None)?;
    }
    Ok(last)
}

/// P0-C attribution update, not a transition: stamp the
/// authorizing approval onto an Effect whose approval was
/// unknown at plan time (device actions are planned at creation,
/// before any approval exists). Appends a same-status line so
/// the JSONL history stays last-line-wins; it never changes the
/// status and never gates anything.
pub(crate) fn stamp_effect_approval(
    state: &AppState,
    effect_id: &str,
    approval_id: &str,
) {
    // P0-D: in SQLite mode the row comes from SQLite; a broken
    // database stamps nothing.
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = effects_snapshot(state) else {
            return;
        };
        let Some(mut record) = records.into_iter().find(|r| r.effect_id == effect_id) else {
            return;
        };
        record.approval_id = Some(approval_id.to_string());
        state.effects.lock().expect("effect lock").insert(record.effect_id.clone(), record.clone());
        persist_effect(state, &record);
        return;
    }
    let mut effects = state.effects.lock().expect("effect lock");
    let Some(mut record) = effects.get(effect_id).cloned() else {
        return;
    };
    record.approval_id = Some(approval_id.to_string());
    effects.insert(record.effect_id.clone(), record.clone());
    drop(effects);
    persist_effect(state, &record);
}

pub(crate) fn effect_by_id(state: &AppState, effect_id: &str) -> Option<EffectFileRecord> {
    effects_snapshot(state)?.into_iter().find(|r| r.effect_id == effect_id)
}

/// The Effect an approval authorizes, resolved through the
/// P0-C binding first and, for approvals issued before P0-C
/// (whose planned row survives in the effects file even though
/// the approval row carries no binding), through the planned
/// row's `approval_id` as a fallback. The digest must match
/// when one is given: a bound Effect for different parameters
/// is a mismatch, never a candidate.
pub(crate) fn effect_for_approval(
    state: &AppState,
    approval_id: &str,
    digest: Option<&str>,
) -> Option<EffectFileRecord> {
    // Lock order discipline: never hold the effects lock while
    // taking the approvals lock (consume paths take approvals
    // first, then effects). Snapshot effects, release, then read
    // the approval binding. P0-D: the snapshot is SQLite in
    // SQLite mode; a broken database yields no candidate (the
    // caller treats that as a binding mismatch, fail closed).
    let mut records = effects_snapshot(state)?;
    records.sort_by_key(|r| effect_number(&r.effect_id));
    // Prefer the approval's own binding when it resolves.
    let bound = {
        let approvals = state.approvals.lock().expect("approval lock");
        approvals
            .get(approval_id)
            .and_then(|r| r.effect_binding.clone())
    };
    if let Some(bound) = bound {
        if let Some(record) = records.iter().find(|r| r.effect_id == bound).cloned() {
            if digest.is_none_or(|d| record.input_digest.as_deref() == Some(d)) {
                return Some(record);
            }
            return None;
        }
    }
    records.into_iter().find(|r| {
        r.approval_id.as_deref() == Some(approval_id)
            && digest.is_none_or(|d| r.input_digest.as_deref() == Some(d))
    })
}

/// P0-C idempotency enforcement (contract §3): before executing
/// a write carrying an Effect, look for a *different* Effect
/// with the same key. A terminal one means the write already
/// happened: replay it, never re-execute. An `unknown`,
/// `reconciling` or in-flight (`planned`/`authorized`/`started`)
/// one means the outcome is not settled: refuse, never
/// re-execute. No key (legacy unattributed write) means the old
/// behaviour: proceed. Lock order is approvals-before-effects
/// wherever both are needed; this helper takes only the effects
/// lock and never touches approvals.
pub(crate) enum IdempotencyDecision {
    Proceed,
    Replay(EffectFileRecord),
    Blocked(EffectFileRecord),
    /// P0-D: the migrated-domain store is unavailable (SQLite
    /// broken in SQLite mode). E2-A §2.4 adds the second cause:
    /// the key inputs are null (no execution id or no digest),
    /// so no key exists to check. Callers must refuse the write,
    /// never proceed on a missing row and never replay a
    /// fabricated one.
    Unavailable,
}

/// E2-A §2.4: whether an idempotency check has no key to
/// check (null execution id or digest). Callers test this
/// BEFORE `check_idempotency` so the two Unavailable causes
/// stay distinct: missing inputs → 409
/// `effect_idempotency_unavailable`; a broken store →
/// 500 `sqlite_unavailable`.
pub(crate) fn idempotency_key_missing(execution_id: Option<&str>, digest: Option<&str>) -> bool {
    execution_id.is_none() || digest.is_none()
}

pub(crate) fn check_idempotency(
    state: &AppState,
    execution_id: Option<&str>,
    capability: &str,
    operation: &str,
    scope: &str,
    digest: Option<&str>,
    exclude_effect_id: Option<&str>,
) -> IdempotencyDecision {
    // E2-A §2.4: an idempotency key needs both inputs. A null
    // execution id or digest is not a key at all: the write
    // cannot be proven new or a replay, so the check refuses
    // (Unavailable) instead of silently Proceeding. Callers
    // distinguish this from a broken store with
    // `idempotency_key_missing` (checked before the call):
    // missing inputs → 409 `effect_idempotency_unavailable`;
    // store-broken Unavailable → 500 `sqlite_unavailable`.
    let (Some(execution_id), Some(digest)) = (execution_id, digest) else {
        return IdempotencyDecision::Unavailable;
    };
    let key = crate::effect_store::idempotency_key(
        execution_id,
        capability,
        operation,
        scope,
        digest,
    );
    // P0-D: in SQLite mode the idempotency check reads SQLite;
    // a broken database refuses the write (Unavailable), it
    // never degrades to Proceed on an empty map.
    let Some(records) = effects_snapshot(state) else {
        return IdempotencyDecision::Unavailable;
    };
    let mut records: Vec<EffectFileRecord> = records
        .into_iter()
        .filter(|r| {
            r.idempotency_key.as_deref() == Some(key.as_str())
                && exclude_effect_id.is_none_or(|id| r.effect_id != id)
        })
        .collect();
    records.sort_by_key(|r| effect_number(&r.effect_id));
    match records.into_iter().next() {
        None => IdempotencyDecision::Proceed,
        Some(record) if crate::effect_store::is_terminal_effect_status(&record.status) => {
            IdempotencyDecision::Replay(record)
        }
        Some(record) => IdempotencyDecision::Blocked(record),
    }
}

// ---------------------------------------------------------------------------
// P3-B patches (plan §4).
//
// One Patch is one loop execution's change set inside one
// Worktree (grouping key (execution_id, worktree_id)); each
// PatchFile is one proposed file write with its own planned
// fs.write Effect and its own approval. Patch status is
// STORED state maintained by the single recompute function
// (sqlite_store::recompute_patch_status); read paths never
// recompute. The apply command and the loop entry share the
// three-way stale check below (§10-8a): a drifted proposal
// never writes, whatever entry it arrives through.
// ---------------------------------------------------------------------------

/// The three-way apply/check outcome (plan §4.4). `Write`
/// and `AlreadyNew` are the only outcomes that may land the
/// proposed content; `Stale` and `Unavailable` never write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PatchCheck {
    /// Current bytes equal the proposal-time old bytes (or
    /// the file is still absent when old_sha256 is null).
    Write,
    /// Current bytes already equal the proposed new bytes:
    /// skip the file write, records only.
    AlreadyNew,
    /// Current bytes match neither old nor new: drifted.
    Stale,
    /// The target cannot be resolved or read (worktree gone,
    /// symlink escape, unreadable): refuse, fail closed.
    Unavailable,
}

/// Resolve (worktree, canonical target, worktree-relative
/// path) for a proposed fs.write path. The path is joined
/// onto the files root, canonicalized (R7 pattern:
/// canonicalize the target when it exists, else canonicalize
/// the parent + join the name), and required to stay inside
/// the canonical files root; then it is stripped against
/// each ACTIVE Worktree's canonical root. Unique hit wins,
/// zero hits -> no Patch (e.g. P2 drafts), multiple (nested)
/// -> deepest/longest prefix. The relative path is stored
/// with forward slashes (cross-platform stable).
fn resolve_worktree_for_path(
    state: &AppState,
    path: &str,
) -> Option<(crate::worktree_store::Worktree, PathBuf, String)> {
    let files_root = std::fs::canonicalize(&state.files_root).ok()?;
    let joined = state.files_root.join(path);
    let canonical = if joined.exists() {
        std::fs::canonicalize(&joined).unwrap_or(joined.clone())
    } else if let (Some(parent), Some(name)) = (joined.parent(), joined.file_name()) {
        std::fs::canonicalize(parent)
            .map(|p| p.join(name))
            .unwrap_or(joined.clone())
    } else {
        return None;
    };
    if !canonical.starts_with(&files_root) {
        return None;
    }
    let worktrees: Vec<crate::worktree_store::Worktree> = state
        .worktrees
        .lock()
        .expect("worktree lock")
        .values()
        .filter(|w| w.status == crate::worktree_store::WORKTREE_ACTIVE)
        .cloned()
        .collect();
    let mut best: Option<(PathBuf, crate::worktree_store::Worktree, PathBuf)> = None;
    for worktree in worktrees {
        let Some(root) = worktree_canonical_root(state, &worktree) else {
            continue;
        };
        let Ok(relative) = canonical.strip_prefix(&root) else {
            continue;
        };
        let longer = best
            .as_ref()
            .is_none_or(|(best_root, _, _)| root.as_os_str().len() > best_root.as_os_str().len());
        if longer {
            best = Some((root, worktree, relative.to_path_buf()));
        }
    }
    let (_root, worktree, relative) = best?;
    let path_relative = relative
        .to_string_lossy()
        .replace('\\', "/");
    Some((worktree, canonical, path_relative))
}

/// Resolve the on-disk target of a PatchFile: the worktree's
/// canonical root joined with the worktree-relative path,
/// canonicalized (R7 pattern) and containment-checked back
/// inside the root. A symlinked escape, a missing
/// worktree/location, a `..` in the stored relative path, or
/// an unreadable parent all return None (Unavailable — never
/// a write). A missing parent falls back to the lexical join
/// (the write creates the parent directories).
fn resolve_patch_target(
    state: &AppState,
    patch: &sqlite_store::PatchRecord,
    file: &sqlite_store::PatchFileRecord,
) -> Option<PathBuf> {
    let rel = Path::new(&file.path_relative);
    if rel.is_absolute()
        || rel
            .components()
            .any(|c| matches!(c, Component::ParentDir))
    {
        return None;
    }
    let worktree = {
        let worktrees = state.worktrees.lock().expect("worktree lock");
        worktrees.get(&patch.worktree_id)?.clone()
    };
    let root = worktree_canonical_root(state, &worktree)?;
    let target = root.join(rel);
    let canonical = if target.exists() {
        std::fs::canonicalize(&target).unwrap_or(target.clone())
    } else if let (Some(parent), Some(name)) = (target.parent(), target.file_name()) {
        std::fs::canonicalize(parent)
            .map(|p| p.join(name))
            .unwrap_or(target.clone())
    } else {
        target.clone()
    };
    if !canonical.starts_with(&root) {
        return None;
    }
    Some(canonical)
}

/// The shared three-way stale check (plan §4.4 + §10-8a):
/// the apply command and the loop entry call this SAME
/// function, so a drifted proposal can never write through
/// either entry. The returned target is the resolved path
/// the check read (Some whenever the check could read the
/// current state, None on Unavailable).
fn check_patch_write(
    state: &AppState,
    patch: &sqlite_store::PatchRecord,
    file: &sqlite_store::PatchFileRecord,
) -> (PatchCheck, Option<PathBuf>) {
    let Some(target) = resolve_patch_target(state, patch, file) else {
        return (PatchCheck::Unavailable, None);
    };
    let current: Option<String> = match std::fs::read(&target) {
        Ok(bytes) => Some(crate::operation_binding::sha256_hex(&bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return (PatchCheck::Unavailable, None),
    };
    let check = match current.as_deref() {
        None if file.old_sha256.is_none() => PatchCheck::Write,
        None => PatchCheck::Stale,
        Some(hash) if Some(hash) == file.old_sha256.as_deref() => PatchCheck::Write,
        Some(hash) if hash == file.new_sha256 => PatchCheck::AlreadyNew,
        Some(_) => PatchCheck::Stale,
    };
    (check, Some(target))
}

/// Set one PatchFile's status (and recompute its Patch) in
/// its own transaction. Used by the loop entry's sync and by
/// the apply's stale marking; the apply success/failure
/// record updates use their own single-tx helpers instead.
fn sync_patch_file_status(
    state: &AppState,
    patch_file_id: &str,
    status: &str,
) -> Result<(sqlite_store::PatchFileRecord, sqlite_store::PatchRecord), String> {
    let Some(sqlite) = &state.sqlite else {
        return Err("sqlite_not_configured".to_string());
    };
    let mut conn = sqlite.lock().map_err(|_| "sqlite lock".to_string())?;
    let tx = conn.transaction().map_err(|e| format!("sqlite: {e}"))?;
    let out = sqlite_store::update_patch_file_status_tx(
        &tx,
        patch_file_id,
        status,
        now_ms() as i64,
    )?;
    tx.commit().map_err(|e| format!("sqlite: {e}"))?;
    Ok(out)
}

/// Stamp the Patch attribution into an Effect's `detail`
/// field (plan §4.2), mirroring `stamp_effect_approval`
/// above: the Effect row structure is unchanged, the
/// attribution is a detail string only, never a gate input.
fn stamp_effect_detail(state: &AppState, effect_id: &str, detail: &str) {
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = effects_snapshot(state) else {
            return;
        };
        let Some(mut record) = records.into_iter().find(|r| r.effect_id == effect_id) else {
            return;
        };
        record.detail = Some(detail.to_string());
        state
            .effects
            .lock()
            .expect("effect lock")
            .insert(record.effect_id.clone(), record.clone());
        persist_effect(state, &record);
        return;
    }
    let mut effects = state.effects.lock().expect("effect lock");
    let Some(mut record) = effects.get(effect_id).cloned() else {
        return;
    };
    record.detail = Some(detail.to_string());
    effects.insert(record.effect_id.clone(), record.clone());
    drop(effects);
    persist_effect(state, &record);
}

/// P3-B grouping (plan §4.2): called from `plan_loop_effect`
/// for a gated fs.write proposal. SQLite-only; in JSONL/
/// Shadow modes there is no Patch store and grouping is
/// skipped (loop behaviour unchanged). Finds or creates the
/// (execution, worktree) Patch, appends the PatchFile with
/// the gateway-computed diff, and stamps the attribution
/// into the Effect's detail field. Any failure warns on
/// stderr and leaves the loop unaffected (fail-open for
/// grouping; apply fails closed). The 1 MiB/file cap is
/// already enforced upstream by `normalize()`; a text over
/// the cap never reaches here, and grouping defensively
/// skips it anyway.
fn maybe_group_patch(
    state: &AppState,
    effect: &EffectFileRecord,
    approval_id: &str,
    path: &str,
    new_text: &str,
) {
    if state.sqlite_mode() != StateBackend::Sqlite || state.sqlite_is_broken() {
        return;
    }
    if new_text.len() > 1024 * 1024 {
        return;
    }
    let Some(execution_id) = effect.execution_id.as_deref() else {
        return;
    };
    let Some((worktree, canonical_target, path_relative)) =
        resolve_worktree_for_path(state, path)
    else {
        return;
    };
    // Proposal-time disk state: old bytes + hashes + diff.
    // The hash is over the exact bytes; the diff is over
    // lossy text (a non-UTF-8 old file shows replacement
    // characters in the review artifact, never in the check).
    let old_bytes = std::fs::read(&canonical_target).ok();
    let old_sha256 = old_bytes
        .as_ref()
        .map(|b| crate::operation_binding::sha256_hex(b));
    let old_text = old_bytes
        .as_ref()
        .map(|b| String::from_utf8_lossy(b).into_owned());
    let new_sha256 = crate::operation_binding::sha256_hex(new_text.as_bytes());
    let diff = crate::patch_store::unified_diff(old_text.as_deref(), new_text, &path_relative);
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let now = now_ms() as i64;
    let (patch, file) = {
        let mut conn = match sqlite.lock() {
            Ok(conn) => conn,
            Err(_) => {
                eprintln!("warning: patch grouping skipped (sqlite lock)");
                return;
            }
        };
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(detail) => {
                eprintln!("warning: patch grouping transaction failed: {detail}");
                return;
            }
        };
        let patch = match sqlite_store::find_latest_patch_tx(
            &tx,
            execution_id,
            &worktree.worktree_id,
        ) {
            Ok(Some(patch)) => patch,
            Ok(None) => {
                let snapshot_id = sqlite_store::get_execution(&tx, execution_id)
                    .ok()
                    .flatten()
                    .and_then(|e| e.workspace_snapshot_id);
                match sqlite_store::insert_patch_tx(
                    &tx,
                    execution_id,
                    snapshot_id.as_deref(),
                    &worktree.worktree_id,
                    worktree.project_id.as_deref(),
                    now,
                ) {
                    Ok(patch) => patch,
                    Err(detail) => {
                        eprintln!("warning: patch creation failed: {detail}");
                        return;
                    }
                }
            }
            Err(detail) => {
                eprintln!("warning: patch lookup failed: {detail}");
                return;
            }
        };
        let file = match sqlite_store::insert_patch_file_tx(
            &tx,
            &patch.patch_id,
            &path_relative,
            old_sha256.as_deref(),
            &new_sha256,
            &diff,
            new_text,
            &effect.effect_id,
            Some(approval_id),
            now,
        ) {
            Ok(file) => file,
            Err(detail) => {
                eprintln!("warning: patch file insert failed: {detail}");
                return;
            }
        };
        if let Err(detail) = tx.commit() {
            eprintln!("warning: patch grouping commit failed: {detail}");
            return;
        }
        (patch, file)
    };
    stamp_effect_detail(
        state,
        &effect.effect_id,
        &format!(
            "patch:{} patchfile:{} worktree:{} path:{}",
            patch.patch_id, file.patch_file_id, worktree.worktree_id, path_relative
        ),
    );
}

/// Sync a PatchFile after a loop write lands (plan §4.4/§4.5):
/// a succeeded loop write applies its PatchFile; a failed one
/// fails it (pending only); an unknown outcome changes nothing
/// (the write may or may not have landed — honest). No-op when
/// the approval has no PatchFile (a non-Patch write), and a
/// no-op when the file is already past the target state (the
/// loop-entry AlreadyNew/Stale paths sync first, then drive
/// the Effect through the same record path — the second sync
/// is idempotent). Best-effort: a sync failure warns; the
/// Effect transition already stands.
fn sync_patch_file_after_loop_write(state: &AppState, approval_id: &str, terminal: &str) {
    if state.sqlite_mode() != StateBackend::Sqlite || state.sqlite_is_broken() {
        return;
    }
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let file = {
        let conn = match sqlite.lock() {
            Ok(conn) => conn,
            Err(_) => return,
        };
        match sqlite_store::patch_file_by_approval(&conn, approval_id) {
            Ok(Some(file)) => file,
            _ => return,
        }
    };
    let target = match terminal {
        "succeeded" if file.status == "pending" || file.status == "failed" => "applied",
        "failed" if file.status == "pending" => "failed",
        _ => return,
    };
    if let Err(detail) = sync_patch_file_status(state, &file.patch_file_id, target) {
        eprintln!("warning: patch file sync after loop write failed: {detail}");
    }
}

/// P3-B loop entry (§10-8a): the SAME three-way stale
/// check as the apply command, run before an fs.write
/// dispatch. Returns Some(outcome) when the loop call is
/// settled here — the file was already new (records only,
/// no write), the proposal drifted (PatchFile -> stale, no
/// write), or the target is unresolvable (fail closed, no
/// write). None means dispatch proceeds. The bound Effect
/// is driven through the caller's `drive` function (the
/// same path the dispatch uses), so no Effect is stranded
/// in `started`.
fn check_loop_patch_write(
    state: &AppState,
    approval_id: &Option<String>,
    drive: &dyn Fn(&ToolOutcome),
) -> Option<ToolOutcome> {
    let approval_id = approval_id.as_ref()?;
    if state.sqlite_mode() != StateBackend::Sqlite || state.sqlite_is_broken() {
        return None;
    }
    let Some(sqlite) = &state.sqlite else {
        return None;
    };
    let (file, patch) = {
        let conn = sqlite.lock().expect("sqlite lock");
        let file =
            sqlite_store::patch_file_by_approval(&conn, approval_id).ok().flatten()?;
        let patch = sqlite_store::get_patch(&conn, &file.patch_id).ok().flatten()?;
        (file, patch)
    };
    match check_patch_write(state, &patch, &file) {
        (PatchCheck::Write, _) => {
            // A rejected file never writes, even when the
            // bytes still match the proposal: the explicit
            // reject (or the approval-reject linkage) is a
            // hard stop for this change set, and the loop
            // entry must refuse it just as the apply command
            // refuses it with patch_rejected. Without this,
            // approving the approval after rejecting the
            // Patch would let the loop land the rejected
            // write anyway. The file is already `rejected`,
            // so no sync is needed.
            if file.status == "rejected" {
                let outcome = ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: format!(
                        "patch_rejected: the patch was rejected; the write was refused and {} stays rejected",
                        file.patch_file_id
                    ),
                };
                drive(&outcome);
                Some(outcome)
            } else {
                None
            }
        }
        (PatchCheck::AlreadyNew, _) => {
            match sync_patch_file_status(state, &file.patch_file_id, "applied") {
                Ok(_) => {
                    let outcome = ToolOutcome::success(format!(
                        "file already contains the proposed content; no write performed ({} applied)",
                        file.patch_file_id
                    ));
                    drive(&outcome);
                    Some(outcome)
                }
                Err(detail) => {
                    eprintln!(
                        "warning: patch file sync failed after already-new loop check: {detail}"
                    );
                    let outcome = ToolOutcome::unknown();
                    drive(&outcome);
                    Some(outcome)
                }
            }
        }
        (PatchCheck::Stale, _) => {
            if let Err(detail) = sync_patch_file_status(state, &file.patch_file_id, "stale") {
                eprintln!("warning: patch file stale-marking failed: {detail}");
            }
            let outcome = ToolOutcome {
                status: crate::loop_engine::CallStatus::Failed,
                error: Some(ErrorCode::ToolFailed),
                body: format!(
                    "patch_stale: the file changed since the proposal; the write was refused and {} is marked stale",
                    file.patch_file_id
                ),
            };
            drive(&outcome);
            Some(outcome)
        }
        (PatchCheck::Unavailable, _) => {
            let outcome = ToolOutcome::unknown();
            drive(&outcome);
            Some(outcome)
        }
    }
}

/// Approval-reject linkage (plan §4.5): when an approval
/// bound to a PatchFile is rejected, the file becomes
/// `rejected` (pending/failed only; stale stays stale) and
/// its Patch recomputes. Best-effort: a SQLite failure
/// warns and leaves the file pending — the reject itself
/// already stands, and the file can never be applied on a
/// rejected approval anyway.
fn link_patch_file_rejected_on_approval(state: &AppState, approval_id: &str) {
    if state.sqlite_mode() != StateBackend::Sqlite || state.sqlite_is_broken() {
        return;
    }
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let (file, _patch) = {
        let conn = match sqlite.lock() {
            Ok(conn) => conn,
            Err(_) => return,
        };
        let Some(file) = sqlite_store::patch_file_by_approval(&conn, approval_id)
            .ok()
            .flatten()
        else {
            return;
        };
        if file.status != "pending" && file.status != "failed" {
            return;
        }
        (file, ())
    };
    if let Err(detail) =
        sync_patch_file_status(state, &file.patch_file_id, "rejected")
    {
        eprintln!("warning: patch file reject linkage failed: {detail}");
    }
}

/// Apply one PatchFile under ONE approvals lock hold (plan
/// §4.4). The three-way check, the approval burn and the
/// Effect authorize/start all serialize here, so a
/// concurrent apply of the same file cannot double-write.
/// `consume_loop_approval` and
/// `consume_bound_approval_for_effect` cannot be called
/// from inside this hold (a std Mutex is not reentrant — it
/// would deadlock), so the loop-approval validation below
/// mirrors their checks; keep in sync: Approved only (a
/// bound approval can never be Edited), unconsumed,
/// `effect_binding` is this file's Effect, the Effect
/// exists with the same digest at planned/authorized, and
/// the approval's `tool` is fs.write. Error codes reuse the
/// existing bound-gate replies (`approval_required`,
/// `approval_rejected`, `approval_binding_mismatch`,
/// `approval_audit_failed`, `effect_transition_failed`).
fn apply_patch_file_locked(
    state: &AppState,
    patch_id: &str,
    patch_file_id: &str,
    expected_old_sha256: Option<&str>,
    approval_id: &str,
) -> Result<(sqlite_store::PatchFileRecord, EffectFileRecord, bool), Box<Response>> {
    let not_found = |error: &str| -> Box<Response> {
        Box::new((StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": error }))).into_response())
    };
    let forbidden = |error: &str| -> Box<Response> {
        Box::new((StatusCode::FORBIDDEN, Json(serde_json::json!({ "error": error }))).into_response())
    };
    let conflict = |error: &str| -> Box<Response> {
        Box::new((StatusCode::CONFLICT, Json(serde_json::json!({ "error": error }))).into_response())
    };
    let bad_request = |error: &str| -> Box<Response> {
        Box::new((StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": error }))).into_response())
    };
    let internal = |error: &str| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error })),
        )
            .into_response()
    };
    // Patches are SQLite-only (plan §4.6).
    if state.sqlite_mode() != StateBackend::Sqlite {
        return Err(Box::new(sqlite_not_configured()));
    }
    if state.sqlite_is_broken() {
        return Err(Box::new(sqlite_error_response()));
    }
    let Some(sqlite) = &state.sqlite else {
        return Err(Box::new(sqlite_not_configured()));
    };
    let mut approvals = state.approvals.lock().expect("approval lock");
    let (patch, file) = {
        let conn = sqlite.lock().expect("sqlite lock");
        let patch = match sqlite_store::get_patch(&conn, patch_id) {
            Ok(Some(patch)) => patch,
            Ok(None) => return Err(not_found("unknown patch")),
            Err(_) => return Err(Box::new(internal("sqlite_unavailable"))),
        };
        let file = match sqlite_store::get_patch_file(&conn, patch_file_id) {
            Ok(Some(file)) => file,
            Ok(None) => return Err(not_found("unknown patch file")),
            Err(_) => return Err(Box::new(internal("sqlite_unavailable"))),
        };
        if file.patch_id != patch.patch_id {
            return Err(not_found("unknown patch file"));
        }
        (patch, file)
    };
    // Idempotent replay (plan §4.4): an already-applied file
    // returns 200 + replayed:true with no re-check/re-write.
    if file.status == "applied" {
        let effect = effect_by_id(state, &file.effect_id)
            .ok_or_else(|| internal("unknown effect"))?;
        return Ok((file, effect, true));
    }
    if patch.status == "rejected" {
        return Err(conflict("patch_rejected"));
    }
    if file.status == "rejected" {
        return Err(conflict("patch_file_rejected"));
    }
    if file.status == "stale" {
        return Err(conflict("patch_stale"));
    }
    if file.status != "pending" && file.status != "failed" {
        return Err(conflict("patch_file_not_appliable"));
    }
    if expected_old_sha256 != file.old_sha256.as_deref() {
        return Err(bad_request("expected_old_sha256_mismatch"));
    }
    // Loop-approval validation (mirror of
    // consume_loop_approval + consume_bound_approval_for_effect).
    let Some(record) = approvals.get_mut(approval_id) else {
        return Err(forbidden("approval_required"));
    };
    if record.tool != "fs.write" {
        return Err(forbidden("approval_required"));
    }
    if record.status == ApprovalStatus::Rejected {
        return Err(forbidden("approval_rejected"));
    }
    if record.status != ApprovalStatus::Approved {
        return Err(forbidden("approval_required"));
    }
    if record.consumed {
        return Err(forbidden("approval_binding_mismatch"));
    }
    if record.effect_binding.as_deref() != Some(file.effect_id.as_str()) {
        return Err(forbidden("approval_binding_mismatch"));
    }
    let digest = record.operation_digest.clone();
    let effect = match effect_by_id(state, &file.effect_id) {
        Some(effect)
            if effect.input_digest.as_deref() == digest.as_deref()
                && (effect.status == "planned" || effect.status == "authorized") => effect,
        _ => return Err(forbidden("approval_binding_mismatch")),
    };
    // E2-A §2.4: idempotency is the second door behind the
    // stale check — a replayed apply (same approval, same
    // input_digest) must not rewrite the file. The bound
    // Effect itself is excluded: it is the current attempt
    // (planned/authorized), not a prior write. A *different*
    // terminal Effect with the same key (same execution,
    // fs.write, scope, digest) proves the write already
    // landed, so the apply returns replayed without touching
    // the file. A bound Effect without an execution id or
    // digest has no key to check: 409, not a silent Proceed.
    if idempotency_key_missing(effect.execution_id.as_deref(), effect.input_digest.as_deref()) {
        return Err(Box::new(
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_unavailable" })),
            )
                .into_response(),
        ));
    }
    match check_idempotency(
        state,
        effect.execution_id.as_deref(),
        "fs",
        "write",
        &file.path_relative,
        effect.input_digest.as_deref(),
        Some(&effect.effect_id),
    ) {
        IdempotencyDecision::Proceed => {}
        IdempotencyDecision::Replay(other) => {
            return Ok((file, other, true));
        }
        IdempotencyDecision::Blocked(other) => {
            return Err(Box::new(
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
                )
                    .into_response(),
            ));
        }
        // Store broken: fail closed, never guess.
        IdempotencyDecision::Unavailable => {
            return Err(Box::new(internal("sqlite_unavailable")));
        }
    }
    // Three-way check, before the approval is burned: a
    // drifted proposal never consumes its approval.
    let (check, target) = check_patch_write(state, &patch, &file);
    if check == PatchCheck::Stale {
        if let Err(detail) = sync_patch_file_status(state, &file.patch_file_id, "stale") {
            eprintln!("warning: patch file stale-marking failed: {detail}");
        }
        return Err(conflict("patch_stale"));
    }
    if check == PatchCheck::Unavailable {
        return Err(Box::new(internal("patch_target_unavailable")));
    }
    // Burn the approval (persist failure -> 500 with the
    // in-memory consumed flag kept, the P0-C fail-closed
    // rule), then drive planned -> authorized -> started.
    record.consumed = true;
    if let Err(detail) = persist_approval(state, approval_id, record) {
        eprintln!("warning: bound approval consumption persistence failed: {detail}");
        return Err(Box::new(internal("approval_audit_failed")));
    }
    if effect.status == "planned"
        && transition_effect(state, &effect.effect_id, "authorized", None, None).is_err()
    {
        return Err(Box::new(internal("effect_transition_failed")));
    }
    start_bound_effect(state, &effect)?;
    // Write (or skip it on the AlreadyNew branch), then one
    // SQLite transaction for the records.
    let replayed = check == PatchCheck::AlreadyNew;
    if !replayed {
        let target = target.ok_or_else(|| internal("patch_target_unavailable"))?;
        if let Some(parent) = target.parent() {
            if std::fs::create_dir_all(parent).is_err() {
                return Err(Box::new(internal("patch_write_failed")));
            }
        }
        if std::fs::write(&target, &file.new_text).is_err() {
            // The approval is spent and the Effect is started;
            // leave an honest failed state (best-effort tx) so
            // the file is retryable through a fresh proposal.
            let mut failed = effect.clone();
            failed.status = "failed".to_string();
            let now = now_ms() as i64;
            let mut conn = sqlite.lock().expect("sqlite lock");
            if let Ok(tx) = conn.transaction() {
                let _ = sqlite_store::fail_patch_records_tx(
                    &tx,
                    &failed,
                    &file.patch_file_id,
                    now,
                );
                let _ = tx.commit();
            }
            state
                .effects
                .lock()
                .expect("effect lock")
                .insert(failed.effect_id.clone(), failed.clone());
            return Err(Box::new(internal("patch_write_failed")));
        }
    }
    let mut succeeded = effect.clone();
    succeeded.status = "succeeded".to_string();
    let owned_evidence = crate::evidence::write_time_observation(state, &succeeded);
    let evidence_draft = owned_evidence.as_ref().map(|o| o.as_draft());
    let now = now_ms() as i64;
    let (file, _patch) = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(_) => return Err(Box::new(internal("sqlite_unavailable"))),
        };
        let out = match sqlite_store::apply_patch_records_tx(
            &tx,
            &succeeded,
            evidence_draft.as_ref(),
            &file.patch_file_id,
            now,
        ) {
            Ok(out) => out,
            Err(detail) => {
                eprintln!("warning: patch apply records failed: {detail}");
                drop(tx);
                // The file is written (or was already new) but
                // the records did not land; mark the file failed
                // (best-effort) so a fresh proposal can recover
                // it instead of leaving it pending forever.
                let mut failed = effect.clone();
                failed.status = "failed".to_string();
                let mut conn = sqlite.lock().expect("sqlite lock");
                if let Ok(tx) = conn.transaction() {
                    let _ = sqlite_store::fail_patch_records_tx(
                        &tx,
                        &failed,
                        &file.patch_file_id,
                        now,
                    );
                    let _ = tx.commit();
                }
                state
                    .effects
                    .lock()
                    .expect("effect lock")
                    .insert(failed.effect_id.clone(), failed.clone());
                return Err(Box::new(internal("sqlite_unavailable")));
            }
        };
        if tx.commit().is_err() {
            return Err(Box::new(internal("sqlite_unavailable")));
        }
        out
    };
    state
        .effects
        .lock()
        .expect("effect lock")
        .insert(succeeded.effect_id.clone(), succeeded.clone());
    Ok((file, succeeded, replayed))
}

/// P0-C loop planning: a proposed gated call gets its planned
/// Effect at proposal time (the engine calls this right after
/// `request_loop_approval`), and the approval is bound to it.
/// The `(loop_id, call_id, tool)` triple stays a source
/// reference on the approval record; the gate checks
/// `effect_id + input_digest`. Non-gated tools never reach here.
/// Best-effort on the binding persist: the in-memory binding is
/// authoritative for the run, and a loop that restarts becomes
/// `Interrupted` anyway, so a lost binding cannot authorize a
/// later call.
pub(crate) fn plan_loop_effect(
    state: &AppState,
    approval_id: &str,
    binding: &ApprovalBinding,
    args: &std::collections::BTreeMap<String, String>,
) -> Option<EffectFileRecord> {
    let (capability, operation, scope) = match binding.tool.as_str() {
        "fs.write" => (
            "fs",
            "write",
            args.get("path").cloned().unwrap_or_default(),
        ),
        // E2-C unification: loop device writes use the
        // contract triple (device, reminders.set_completed),
        // the same identity the direct route records.
        "reminders.set_completed" => (
            "device",
            "reminders.set_completed",
            args.get("id").cloned().unwrap_or_default(),
        ),
        _ => return None,
    };
    // E2-C §4.2: the proposal-time plan step goes through the
    // shared ledger entry. A Replay/Blocked answer means this
    // exact call was already proposed in this loop — the
    // existing row is the audit trail — so no second planned
    // row is recorded (a second row would collide on the
    // UNIQUE idempotency key) and the approval proceeds
    // unbound; the dispatch gate and the record_loop_effect
    // fallback still apply.
    let record = match ledger_entry(
        state,
        execution_for_loop(state, &binding.loop_id),
        capability,
        operation,
        &scope,
        Some(crate::loop_engine::args_digest(&binding.tool, args)),
        Some(approval_id.to_string()),
    ) {
        LedgerEntryDecision::Entered(entry) => entry.effect,
        LedgerEntryDecision::Replay(_) | LedgerEntryDecision::Blocked(_) => return None,
    };
    let mut approvals = state.approvals.lock().expect("approval lock");
    if let Some(approval) = approvals.get_mut(approval_id) {
        approval.effect_binding = Some(record.effect_id.clone());
        let bound = approval.clone();
        drop(approvals);
        if let Err(detail) = persist_approval(state, approval_id, &bound) {
            eprintln!("warning: loop effect binding persistence failed: {detail}");
        }
    }
    // P3-B grouping (§4.2): a gated fs.write proposal joins
    // the (execution, worktree) Patch. Grouping is fail-open
    // (see maybe_group_patch); it never changes the proposal.
    if binding.tool == "fs.write" {
        if let Some(text) = args.get("text") {
            maybe_group_patch(
                state,
                &record,
                approval_id,
                args.get("path").map(String::as_str).unwrap_or(""),
                text,
            );
        }
    }
    Some(record)
}

/// P0-C device planning digest: the unified Q5 digest of the
/// executed (reminder, completed) pair, shared by actions_create
/// (planned row) and device_set (gate check). Identical to the
/// phase-2 digest, so no gate or consumption moves.
pub(crate) fn device_effect_digest(reminder_id: &str, target_completed: bool) -> String {
    crate::operation_binding::operation_digest(
        DEVICE_ACTION,
        Some("set_completed"),
        &std::collections::BTreeMap::from([
            ("id".to_string(), reminder_id.to_string()),
            ("completed".to_string(), target_completed.to_string()),
        ]),
    )
}

/// P0-C reconciliation driver, shared by `/effects/reconcile`
/// and `actions_refresh`. The caller guarantees the Effect is
/// `unknown` and hands over its device action. Returns the new
/// Effect row, or an error string: `effect_action_mismatch`
/// when the backing action is no longer `unknown`.
/// Hard constraint, mirroring the device rule: a failed or
/// inconclusive re-check never writes `unknown` as `failed`.
/// Only a conclusive proof lands on `reconciled`: a state match
/// (`completed`) or a missing target (`failed`, because a write
/// cannot land on a reminder that no longer exists). Anything
/// else (query error, unchanged state) returns the Effect to
/// `unknown` via `reconciling -> unknown`.
/// P0-R: a conclusive device verdict also writes its reconcile
/// Evidence mirror (best-effort, derived data): `verified` for
/// a completed re-check, `mismatch` for a failed one.
/// Inconclusive re-checks write no mirror.
pub(crate) fn reconcile_device_effect(
    state: &AppState,
    effect: &EffectFileRecord,
    action: &crate::device_store::DeviceActionRecord,
) -> Result<EffectFileRecord, String> {
    if effect.status != "unknown" {
        return Err("effect_not_unknown".to_string());
    }
    if action.status != "unknown" {
        return Err("effect_action_mismatch".to_string());
    }
    transition_effect(state, &effect.effect_id, "reconciling", None, None)
        .map_err(|_| "effect_transition_failed".to_string())?;
    let outcome =
        resolve_unknown(&state.device_backend, &action.reminder_id, action.target_completed);
    reconcile_device_effect_with_outcome(state, effect, outcome)
}

/// Same reconciliation driver, but with a re-check outcome the
/// caller already computed (so `actions_refresh` resolves once
/// and drives both the action and its Effect from that single
/// observation, never from two queries that could disagree).
pub(crate) fn reconcile_device_effect_with_outcome(
    state: &AppState,
    effect: &EffectFileRecord,
    outcome: ResolveOutcome,
) -> Result<EffectFileRecord, String> {
    match outcome {
        ResolveOutcome::Completed(reminder) => {
            let reconciliation = Reconciliation {
                result: "completed".to_string(),
                checked_ms: crate::effect_store::now_ms(),
                detail: None,
            };
            let record = transition_effect(
                state,
                &effect.effect_id,
                "reconciled",
                Some(reconciliation),
                None,
            )
            .map_err(|_| "effect_transition_failed".to_string())?;
            // P0-R reconcile mirror: the completed re-check is
            // observed as a verified Evidence row (best-effort).
            let draft = crate::evidence::OwnedEvidence {
                effect_id: effect.effect_id.clone(),
                execution_id: effect.execution_id.clone(),
                source: "reconcile".to_string(),
                verdict: "verified".to_string(),
                reason: "record_match".to_string(),
                observed_json: serde_json::to_string(&serde_json::json!({
                    "kind": "reminder",
                    "completed": reminder.completed,
                }))
                .unwrap_or_else(|_| "{}".to_string()),
                checked_ms: crate::effect_store::now_ms() as i64,
                origin_device_id: state.origin_device_id.clone(),
                schema_version: 1,
            };
            record_evidence_best_effort(state, &draft);
            Ok(record)
        }
        ResolveOutcome::Failed(e) => {
            let reconciliation = Reconciliation {
                result: "failed".to_string(),
                checked_ms: crate::effect_store::now_ms(),
                detail: Some(e.to_string()),
            };
            let record = transition_effect(
                state,
                &effect.effect_id,
                "reconciled",
                Some(reconciliation),
                None,
            )
            .map_err(|_| "effect_transition_failed".to_string())?;
            // P0-R reconcile mirror: the missing target is
            // observed as a mismatch Evidence row (best-effort).
            let draft = crate::evidence::OwnedEvidence {
                effect_id: effect.effect_id.clone(),
                execution_id: effect.execution_id.clone(),
                source: "reconcile".to_string(),
                verdict: "mismatch".to_string(),
                reason: "target_missing".to_string(),
                observed_json: serde_json::to_string(&serde_json::json!({"kind": "absent"}))
                    .unwrap_or_else(|_| "{}".to_string()),
                checked_ms: crate::effect_store::now_ms() as i64,
                origin_device_id: state.origin_device_id.clone(),
                schema_version: 1,
            };
            record_evidence_best_effort(state, &draft);
            Ok(record)
        }
        ResolveOutcome::StillUnknown(maybe_err) => {
            // Inconclusive: back to unknown, no reconciliation
            // conclusion attached (a stale conclusion must never
            // masquerade as a verdict). The detail, if any, is
            // carried on the action record by the caller.
            let _ = maybe_err;
            transition_effect(state, &effect.effect_id, "unknown", None, None)
                .map_err(|_| "effect_transition_failed".to_string())
        }
    }
}

/// P0-R generalized reconciliation driver, used by
/// P3-C: the Evidence a verify/reconcile needs for one
/// Effect. fs classes use the write-time baseline (source
/// `write`); a test run's only Evidence is source `test`
/// (the driver inserts it at completion). A failed lookup
/// degrades to None (the legacy path, which can only verify,
/// never falsely mismatch).
fn latest_evidence_for_effect(
    state: &AppState,
    effect: &EffectFileRecord,
) -> Option<crate::sqlite_store::EvidenceRow> {
    let source = if effect.capability == "test" && effect.operation == "run" {
        "test"
    } else {
        "write"
    };
    let sqlite = state.sqlite.as_ref()?;
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::latest_evidence_for_effect_by_source(
        &conn,
        &effect.effect_id,
        source,
    ) {
        Ok(row) => row,
        Err(detail) => {
            eprintln!("warning: evidence lookup failed: {detail}");
            None
        }
    }
}

/// `/effects/reconcile`. First half is the device path,
/// unchanged: status checks, move to `reconciling`, conclusion
/// write-back, failure constraints. Every other class goes
/// through the observation dispatch in `evidence.rs`:
/// - `unknown` and `started` are accepted; a `started` row
///   first crosses the legal edge `started -> unknown`.
/// - a landed observation reconciles with `completed` and a
///   verified mirror; a device-reminder-missing observation
///   reconciles with `failed` and a mismatch mirror; an
///   inconclusive observation returns the row to `unknown`
///   with no mirror and no conclusion.
/// - rows with no observation method answer
///   `effect_no_readback`; rows without the information an
///   observation needs answer `evidence_missing`.
///
/// The driver never rewrites a terminal state and never
/// invents a method for a class that has none.
pub(crate) fn reconcile_effect(
    state: &AppState,
    effect: &EffectFileRecord,
) -> Result<EffectFileRecord, String> {
    if effect.status != "unknown" && effect.status != "started" {
        return Err("effect_not_unknown".to_string());
    }
    // Validate before any mutation, so a 409 leaves the row
    // exactly as it was (a started row that fails validation
    // stays started, not silently crossed to unknown).
    let device_action = if matches!(effect.capability.as_str(), "device" | "devices") {
        let action = {
            let actions = state.actions.lock().expect("action lock");
            actions
                .values()
                .find(|a| {
                    a.effect_id.as_deref() == Some(&effect.effect_id)
                        || (effect.execution_id.is_some()
                            && a.execution_id == effect.execution_id)
                })
                .cloned()
        };
        match action {
            Some(action) => {
                if action.status != "unknown" {
                    return Err("effect_action_mismatch".to_string());
                }
                Some(action)
            }
            // E2-C unification: a device row without a backing
            // action (loop writes have none) falls through to
            // the generic reconcile path instead of 409ing —
            // the generic path recovers the target from the
            // row's own digest (see device_target_from_digest).
            None => None,
        }
    } else {
        None
    };
    let outcome = if device_action.is_none() {
        // Write-time baseline for classes that need it (export,
        // rename); test runs load their Test Evidence instead.
        // Absent in JSONL mode and for legacy rows; a failed
        // lookup degrades to the legacy path (which stays
        // honest: it can only verify, never falsely mismatch).
        let write_evidence = latest_evidence_for_effect(state, effect);
        match crate::evidence::reconcile_outcome(state, effect, write_evidence.as_ref()) {
            Ok(outcome) => Some(outcome),
            Err(crate::evidence::ObserveError::NoMethod) => {
                return Err("effect_no_readback".to_string());
            }
            Err(crate::evidence::ObserveError::MissingEvidence) => {
                return Err("evidence_missing".to_string());
            }
        }
    } else {
        None
    };
    // A `started` row first crosses the legal edge
    // `started -> unknown`, then follows the normal driver.
    let mut effect = effect.clone();
    if effect.status == "started" {
        effect = transition_effect(state, &effect.effect_id, "unknown", None, None)
            .map_err(|_| "effect_transition_failed".to_string())?;
    }
    if let Some(action) = device_action {
        return reconcile_device_effect(state, &effect, &action);
    }
    let outcome = outcome.expect("non-device reconcile has an outcome");
    match outcome {
        crate::evidence::ReconcileOutcome::Landed { observed, reason } => {
            transition_effect(state, &effect.effect_id, "reconciling", None, None)
                .map_err(|_| "effect_transition_failed".to_string())?;
            let reconciliation = Reconciliation {
                result: "completed".to_string(),
                checked_ms: crate::effect_store::now_ms(),
                detail: None,
            };
            let record = transition_effect(
                state,
                &effect.effect_id,
                "reconciled",
                Some(reconciliation),
                None,
            )
            .map_err(|_| "effect_transition_failed".to_string())?;
            let draft = crate::evidence::OwnedEvidence {
                effect_id: effect.effect_id.clone(),
                execution_id: effect.execution_id.clone(),
                source: "reconcile".to_string(),
                verdict: "verified".to_string(),
                reason: reason.to_string(),
                observed_json: serde_json::to_string(&observed).unwrap_or_else(|_| "{}".to_string()),
                checked_ms: crate::effect_store::now_ms() as i64,
                origin_device_id: state.origin_device_id.clone(),
                schema_version: 1,
            };
            record_evidence_best_effort(state, &draft);
            Ok(record)
        }
        crate::evidence::ReconcileOutcome::NotLanded { observed, reason } => {
            transition_effect(state, &effect.effect_id, "reconciling", None, None)
                .map_err(|_| "effect_transition_failed".to_string())?;
            let reconciliation = Reconciliation {
                result: "failed".to_string(),
                checked_ms: crate::effect_store::now_ms(),
                detail: None,
            };
            let record = transition_effect(
                state,
                &effect.effect_id,
                "reconciled",
                Some(reconciliation),
                None,
            )
            .map_err(|_| "effect_transition_failed".to_string())?;
            let draft = crate::evidence::OwnedEvidence {
                effect_id: effect.effect_id.clone(),
                execution_id: effect.execution_id.clone(),
                source: "reconcile".to_string(),
                verdict: "mismatch".to_string(),
                reason: reason.to_string(),
                observed_json: serde_json::to_string(&observed).unwrap_or_else(|_| "{}".to_string()),
                checked_ms: crate::effect_store::now_ms() as i64,
                origin_device_id: state.origin_device_id.clone(),
                schema_version: 1,
            };
            record_evidence_best_effort(state, &draft);
            Ok(record)
        }
        crate::evidence::ReconcileOutcome::Inconclusive { .. } => {
            transition_effect(state, &effect.effect_id, "reconciling", None, None)
                .map_err(|_| "effect_transition_failed".to_string())?;
            transition_effect(state, &effect.effect_id, "unknown", None, None)
                .map_err(|_| "effect_transition_failed".to_string())
        }
    }
}

/// E2-A §2.4: idempotency gate for the payload-free connector
/// writes (connect / disconnect / unlink). The recorded key
/// cannot identify a replay on its own: `record_connector_effect`
/// mints a fresh adapter Execution per write, so the full key is
/// unique per call, and the digest commits only to
/// (capability, operation, scope) — never to a payload. The
/// gate therefore matches on the latest Effect for the same
/// (capability, scope): only a repeat of the latest write,
/// with no intervening write to the same scope, is a replay.
/// That keeps the tested cycles honest — connect →
/// disconnect → connect is a new write (the disconnect
/// intervened), while connect → connect is a replay — and it
/// is why the payload-bearing writes (register / delete /
/// capture / fulfill / update) are NOT gated here: a
/// content-blind digest would swallow a distinct second
/// update, mask a consumed ticket, or turn a 404/409 into a
/// replay. Those routes either refuse the repeat (404/409,
/// no second row) or mint a genuinely new entity (link,
/// capture) or version (update), so no duplicate row exists
/// to close there.
pub(crate) fn check_connector_write(
    state: &AppState,
    capability: &str,
    operation: &str,
    scope: &str,
) -> IdempotencyDecision {
    let digest = crate::operation_binding::operation_digest(
        &format!("{capability}.{operation}"),
        None,
        &std::collections::BTreeMap::from([("id".to_string(), scope.to_string())]),
    );
    let Some(records) = effects_snapshot(state) else {
        return IdempotencyDecision::Unavailable;
    };
    let Some(prior) = records
        .iter()
        .filter(|r| r.capability == capability && r.scope == scope)
        .max_by_key(|r| effect_number(&r.effect_id))
    else {
        return IdempotencyDecision::Proceed;
    };
    if prior.operation != operation || prior.input_digest.as_deref() != Some(digest.as_str()) {
        return IdempotencyDecision::Proceed;
    }
    match prior.execution_id.as_deref() {
        Some(execution_id) => check_idempotency(
            state,
            Some(execution_id),
            capability,
            operation,
            scope,
            Some(&digest),
            None,
        ),
        // Legacy row without attribution: the identity match
        // itself proves the replay; judge by terminality.
        None => {
            if crate::effect_store::is_terminal_effect_status(&prior.status) {
                IdempotencyDecision::Replay(prior.clone())
            } else {
                IdempotencyDecision::Blocked(prior.clone())
            }
        }
    }
}

/// Phase 2 management-plane Effect helper: connector and vault
/// writes carry no approval, so the digest commits to the
/// operation and the affected id in the unified Q5 shape, and the
/// status is always `completed` (these handlers only return
/// success after the mutation persisted).
///
/// E2-C §4.3: the helper goes through the shared ledger entry.
/// A caller-provided Execution (a task/loop that triggered the
/// write) is reused, so the Effect is attributed to the attempt
/// that caused it; with no caller Execution — every direct route
/// today — the adapter Execution fallback is kept. The lifecycle
/// chain is unchanged: it still runs through the single
/// transition enforcement point (`transition_effect_chain`,
/// inside `ledger_entry_finish`), compressed planned →
/// authorized → started → succeeded as before. The idempotency
/// gate inside `ledger_entry` can only fire with a reused
/// Execution (a fresh adapter Execution makes the key unique);
/// the direct routes keep their route-level
/// `check_connector_write` replay guard (E2-A §2.4). A
/// Replay/Blocked answer records no second row.
///
/// `pub` (not `pub(crate)`) so the E2-C integration test can
/// drive the reuse path directly: no task/loop path performs a
/// connector write yet, so no HTTP route can supply a caller
/// Execution today.
pub fn record_connector_effect(
    state: &AppState,
    execution_id: Option<String>,
    capability: &str,
    operation: &str,
    scope: &str,
) {
    let digest = crate::operation_binding::operation_digest(
        &format!("{capability}.{operation}"),
        None,
        &std::collections::BTreeMap::from([("id".to_string(), scope.to_string())]),
    );
    // P0-C: management-plane writes have no separate plan or
    // approval step, so the lifecycle chain is compressed in
    // this helper (contract §4: process states may be
    // compressed, order never changes): planned at the write
    // request, authorized by the request itself, started, then
    // succeeded. The single in-memory row ends at `succeeded`;
    // the JSONL history holds the chain, last line wins.
    if let LedgerEntryDecision::Entered(entry) = ledger_entry(
        state,
        execution_id,
        capability,
        operation,
        scope,
        Some(digest),
        None,
    ) {
        let _ = ledger_entry_finish(state, &entry.effect.effect_id, LedgerOutcome::Succeeded);
    }
}

/// Execution of the attempt a loop belongs to. Phase 1 creates
/// exactly one Execution per loop_start, so a loop id maps to at
/// most one record; a loop started before phase 1 (or before
/// executions were persisted) has none, and its effects carry
/// `None` rather than a fabricated id.
pub(crate) fn execution_for_loop(state: &AppState, loop_id: &str) -> Option<String> {
    // P0-D: SQLite snapshot in SQLite mode (None when broken).
    executions_snapshot(state)?
        .into_iter()
        .find(|r| r.loop_id.as_deref() == Some(loop_id))
        .map(|r| r.execution_id.clone())
}

/// P3-A: the worktree id frozen in an Execution's snapshot,
/// if any. SQLite mode only: JSONL mode has no snapshot rows,
/// so the caller falls back to the files root.
fn execution_snapshot_binding_worktree(state: &AppState, execution_id: &str) -> Option<String> {
    let sqlite = state.sqlite.as_ref()?;
    if state.sqlite_is_broken() {
        return None;
    }
    let conn = sqlite.lock().expect("sqlite lock");
    let executions = sqlite_store::list_executions(&conn).ok()?;
    let execution = executions
        .into_iter()
        .find(|e| e.execution_id == execution_id)?;
    let snapshot_id = execution.workspace_snapshot_id?;
    sqlite_store::get_snapshot(&conn, &snapshot_id)
        .ok()?
        .and_then(|row| row.binding_worktree_id)
}

/// P3-A: resolve the search root for a code.search call. An
/// explicit worktree_id wins (it must exist and be active);
/// otherwise the calling loop's Execution snapshot supplies
/// the bound worktree; an unbound execution falls back to the
/// files root (today's file-tool behaviour).
fn resolve_code_search_root(
    state: &AppState,
    worktree_id: Option<&str>,
    binding: Option<&ApprovalBinding>,
) -> Result<PathBuf, &'static str> {
    if let Some(id) = worktree_id.filter(|id| !id.is_empty()) {
        let worktree = {
            let worktrees = state.worktrees.lock().expect("worktree lock");
            let Some(worktree) = worktrees.get(id) else {
                return Err("unknown worktree");
            };
            if worktree.status != WORKTREE_ACTIVE {
                return Err("worktree is archived");
            }
            worktree.clone()
        };
        return worktree_canonical_root(state, &worktree).ok_or("worktree directory unavailable");
    }
    if let Some(binding) = binding {
        if let Some(execution_id) = execution_for_loop(state, &binding.loop_id) {
            if let Some(bound_id) = execution_snapshot_binding_worktree(state, &execution_id) {
                let worktree = {
                    let worktrees = state.worktrees.lock().expect("worktree lock");
                    worktrees
                        .get(&bound_id)
                        .filter(|w| w.status == WORKTREE_ACTIVE)
                        .cloned()
                };
                if let Some(worktree) = worktree {
                    if let Some(root) = worktree_canonical_root(state, &worktree) {
                        return Ok(root);
                    }
                }
            }
        }
    }
    Ok(state.files_root.clone())
}

/// Execution of the current attempt of a task. A task resume spawns
/// a fresh runner attempt and a fresh Execution, so the latest
/// (largest execution-N) record for the task is the current
/// attempt. Whether a resume continues the same Execution is a
/// phase 4 (Q6) decision; until then, latest-wins is the honest
/// mapping for the attempt that just wrote.
pub(crate) fn execution_for_task(state: &AppState, task_id: &str) -> Option<String> {
    // P0-D: SQLite snapshot in SQLite mode (None when broken).
    executions_snapshot(state)?
        .into_iter()
        .filter(|r| r.task_id.as_deref() == Some(task_id))
        .max_by_key(|r| execution_number(&r.execution_id))
        .map(|r| r.execution_id.clone())
}

/// E2-A §2.2: Task status → Execution status. Terminal Task
/// words mirror to the Execution terminal (Q2 mapping):
/// completed→completed, failed/rejected→failed (Execution has
/// no `rejected` word; the in-repo precedent
/// `map_legacy_status_to_new` in effect_store.rs folds
/// "failed"|"rejected" to `failed`), cancelled→cancelled.
/// Non-terminal words (created/running/draft_ready/
/// interrupted) map to `running`: the attempt is still live.
/// A `rejected` Task is terminal, so its Execution closes as
/// `failed`, never as a still-running shell.
pub(crate) fn task_status_to_execution(task_status: &str) -> &'static str {
    match task_status {
        "completed" => "completed",
        "failed" | "rejected" => "failed",
        "cancelled" => "cancelled",
        _ => "running",
    }
}

/// E2-A §2.2: Loop status → Execution status. Completed→
/// completed, Failed→failed, Cancelled→cancelled,
/// NeedsReview→needs_review (Execution `needs_review` is a
/// non-terminal recoverable word while Loop `NeedsReview` ends
/// the round — the words are not equated, only mapped).
/// Created/Running/AwaitingApproval map to `running`.
/// Interrupted maps to `running` but is NEVER used to close an
/// Execution: a Loop Interrupted closes no Execution (the Task
/// stays resumable); only an explicit cancel path closes it.
/// The coupling helper only consults this for terminal Loop
/// statuses, so the Interrupted arm is a statement of that
/// rule, not a transition source.
/// (The plan's reverse display mapping — Execution status →
/// Task/Loop display word — has no gateway consumer: the
/// forward maps are many-to-one, so the reverse is only a
/// display hint, and the web UI reads Execution.status
/// directly. It is deferred rather than added as dead code.)
pub(crate) fn loop_status_to_execution(loop_status: &crate::loop_engine::LoopStatus) -> &'static str {
    use crate::loop_engine::LoopStatus;
    match loop_status {
        LoopStatus::Completed => "completed",
        LoopStatus::Failed => "failed",
        LoopStatus::Cancelled => "cancelled",
        LoopStatus::NeedsReview => "needs_review",
        _ => "running",
    }
}

/// E2-A §2.3: close one Execution to a terminal (or
/// `needs_review`) status, in both backends. In SQLite mode
/// the close runs `sqlite_store::transition_execution`
/// (validates the edge and sets the driving Assignment done
/// on terminal landing), then refreshes the in-memory map
/// and exports the audit. In JSONL/Shadow mode it validates
/// the edge against `valid_execution_transition`, updates
/// the map record and appends the line. A missing Execution,
/// an already-terminal one, or an illegal edge is a no-op
/// (the caller already decided the source is terminal;
/// closing twice must not error). Returns the new status
/// when a transition landed.
pub(crate) fn close_execution(state: &AppState, execution_id: &str, to: &str) -> Option<String> {
    close_execution_inner(state, execution_id, to, false)
}

/// E2-A §2.3 + P3-C §5.4: like `close_execution`, but the
/// close opens no Inbox row. Only the Worker driver uses it,
/// for a test run (a test result is a result, not something
/// to review). In JSONL/Shadow modes there is no Inbox side
/// effect to suppress; the flag only threads the SQLite path.
pub(crate) fn close_execution_no_inbox(
    state: &AppState,
    execution_id: &str,
    to: &str,
) -> Option<String> {
    close_execution_inner(state, execution_id, to, true)
}

fn close_execution_inner(
    state: &AppState,
    execution_id: &str,
    to: &str,
    suppress_inbox: bool,
) -> Option<String> {
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return None;
        }
        let Some(sqlite) = &state.sqlite else {
            return None;
        };
        let result = {
            let mut conn = sqlite.lock().expect("sqlite lock");
            if suppress_inbox {
                sqlite_store::transition_execution_no_inbox(&mut conn, execution_id, to)
            } else {
                sqlite_store::transition_execution(&mut conn, execution_id, to)
            }
        };
        match result {
            Ok(record) => {
                state
                    .executions
                    .lock()
                    .expect("execution lock")
                    .insert(record.execution_id.clone(), record.clone());
                refresh_executions_export(state);
                Some(record.status)
            }
            Err(sqlite_store::ExecutionTransitionError::NotFound)
            | Err(sqlite_store::ExecutionTransitionError::Illegal) => None,
            Err(sqlite_store::ExecutionTransitionError::Internal(detail)) => {
                *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
                eprintln!("warning: E2-A execution close failed: {detail}");
                None
            }
        }
    } else {
        let mut executions = state.executions.lock().expect("execution lock");
        let record = executions.get_mut(execution_id)?;
        if !crate::execution_store::valid_execution_transition(&record.status, to) {
            return None;
        }
        record.status = to.to_string();
        let updated = record.clone();
        drop(executions);
        persist_execution_with_snapshot(state, &updated, None);
        Some(updated.status)
    }
}

/// E2-A §2.3: mirror a terminal Task status onto its
/// Execution. Non-terminal Task words are no-ops.
pub(crate) fn close_execution_for_task(state: &AppState, task_id: &str, task_status: &str) {
    if !matches!(task_status, "completed" | "failed" | "rejected" | "cancelled") {
        return;
    }
    let Some(execution_id) = execution_for_task(state, task_id) else {
        return;
    };
    close_execution(state, &execution_id, task_status_to_execution(task_status));
}

/// E2-A §2.3: mirror a terminal Loop status onto its
/// Execution. Interrupted never closes (see
/// `loop_status_to_execution`); non-terminal words are no-ops.
pub(crate) fn close_execution_for_loop(
    state: &AppState,
    loop_id: &str,
    loop_status: &crate::loop_engine::LoopStatus,
) {
    use crate::loop_engine::LoopStatus;
    if !matches!(
        loop_status,
        LoopStatus::Completed | LoopStatus::NeedsReview | LoopStatus::Failed | LoopStatus::Cancelled
    ) {
        return;
    }
    let Some(execution_id) = execution_for_loop(state, loop_id) else {
        return;
    };
    close_execution(state, &execution_id, loop_status_to_execution(loop_status));
}

/// E2-A §2.3: re-run supersede. When a Task run/resume
/// creates a fresh Execution, cancel every OTHER still-
/// `"running"` Execution of that Task (a stale attempt left
/// running by a crash/restart, or by a manual status reset).
/// The new Execution is the current attempt; the
/// `execution_for_task` latest-wins read semantics are
/// unchanged.
pub(crate) fn supersede_old_task_executions(
    state: &AppState,
    task_id: &str,
    keep_execution_id: &str,
) {
    let Some(records) = executions_snapshot(state) else {
        return;
    };
    for record in records {
        if record.task_id.as_deref() != Some(task_id) {
            continue;
        }
        if record.execution_id == keep_execution_id {
            continue;
        }
        if record.status != "running" {
            continue;
        }
        close_execution(state, &record.execution_id, "cancelled");
    }
}

/// E2-A §2.8: one-time reconciliation of historical stranded
/// "running" Executions (their Task/Loop/Worker already
/// terminal). Applies the §2.3 mapping as transitions, one
/// `execution.reconciled` audit event per row in SQLite mode
/// — never a silent rewrite. Idempotent: after the first run
/// there are no stranded rows left, and the §2.3 coupling
/// prevents new ones, so later startups reconcile zero rows.
/// A Task/Loop/Worker that is itself still non-terminal leaves
/// its Execution running; an adapter Execution with no source
/// is left alone (nothing to map from); a superseded Worker's
/// Execution belongs to the takeover owner and is not touched.
/// Returns the reconciled count (logged when non-zero).
fn reconcile_stranded_executions(state: &AppState) -> usize {
    let executions = match executions_snapshot(state) {
        Some(records) => records,
        None => return 0,
    };
    let runs: Vec<crate::worker::WorkerRunRow> =
        if state.sqlite_mode() == StateBackend::Sqlite {
            match &state.sqlite {
                Some(sqlite) => {
                    let conn = sqlite.lock().expect("sqlite lock");
                    sqlite_store::list_worker_runs(&conn, None, None).unwrap_or_default()
                }
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
    let mut reconciled = 0usize;
    for exec in executions {
        if exec.status != "running" {
            continue;
        }
        let target: Option<&'static str> = if let Some(task_id) = &exec.task_id {
            state
                .tasks
                .lock()
                .expect("task lock")
                .get(task_id)
                .map(|t| task_status_to_execution(&t.status))
                .filter(|s| *s != "running")
        } else if let Some(loop_id) = &exec.loop_id {
            state
                .loops
                .get(loop_id)
                .ok()
                .map(|v| loop_status_to_execution(&v.record.status))
                .filter(|s| *s != "running")
        } else {
            runs.iter()
                .find(|r| r.execution_id.as_deref() == Some(exec.execution_id.as_str()))
                .and_then(|r| crate::worker::run_status_to_execution(&r.status))
        };
        let Some(target) = target else {
            continue;
        };
        // SQLite mode: reconcile_execution (audit event
        // `execution.reconciled`); other modes: close_execution
        // (the appended JSONL line is the audit).
        let landed = if state.sqlite_mode() == StateBackend::Sqlite {
            let Some(sqlite) = &state.sqlite else {
                continue;
            };
            if state.sqlite_is_broken() {
                continue;
            }
            let mut conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::reconcile_execution(&mut conn, &exec.execution_id, target) {
                Ok(record) => {
                    drop(conn);
                    state
                        .executions
                        .lock()
                        .expect("execution lock")
                        .insert(record.execution_id.clone(), record.clone());
                    refresh_executions_export(state);
                    true
                }
                Err(_) => false,
            }
        } else {
            close_execution(state, &exec.execution_id, target).is_some()
        };
        if landed {
            reconciled += 1;
        }
    }
    if reconciled > 0 {
        eprintln!("E2-A §2.8: reconciled {reconciled} stranded running executions");
    }
    reconciled
}

/// P0-B adapter: a minimal Execution for a write with no task or
/// loop attempt of its own (direct REST writes, connector/vault
/// management writes). One per write: the write IS the attempt.
/// The status stays "running": the adapter must not invent the
/// phase 4 transitions, and a failed write never reaches this
/// call because every call site records its Effect only on
/// success. Task, loop and device attempts never use this: they
/// resolve their own Execution (execution_for_task,
/// execution_for_loop, or the action record's execution id).
pub(crate) fn adapter_execution(state: &AppState) -> String {
    create_execution(state, None, None).execution_id
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
        execute_binding: None,
        goal_id: req.goal_id,
        project_id: req.project_id,
        workspace_id: req.workspace_id,
    };
    state.tasks.lock().expect("task lock").insert(record.task_id.clone(), record.clone());
    persist_task(&state, &record);
    // D1-2 (Q-D1-22): one task-level Mandate row.
    create_task_mandate(&state, &record);
    Json(record).into_response()
}

/// D1-1: list filters. Each is an exact match on the
/// task's link field; absent means no filter on that
/// dimension. No new route: the filters ride the existing
/// list query.
#[derive(Debug, Deserialize)]
struct TaskListQuery {
    #[serde(default)]
    goal_id: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    workspace_id: Option<String>,
}

async fn tasks_list(
    State(state): State<AppState>,
    Query(query): Query<TaskListQuery>,
) -> Json<Vec<TaskFileRecord>> {
    let mut records: Vec<TaskFileRecord> =
        state.tasks.lock().expect("task lock").values().cloned().collect();
    if let Some(goal_id) = &query.goal_id {
        records.retain(|r| r.goal_id.as_deref() == Some(goal_id.as_str()));
    }
    if let Some(project_id) = &query.project_id {
        records.retain(|r| r.project_id.as_deref() == Some(project_id.as_str()));
    }
    if let Some(workspace_id) = &query.workspace_id {
        records.retain(|r| r.workspace_id.as_deref() == Some(workspace_id.as_str()));
    }
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
    // D1-2 (Q-D1-22): a goal change supersedes the Task's
    // Mandates (computed before the move below).
    let goal_changed = req
        .goal_id
        .as_ref()
        .is_some_and(|goal_id| Some(goal_id) != record.goal_id.as_ref());
    record.status = req.status;
    if let Some(path) = req.artifact_path {
        record.artifact_path = Some(path);
    }
    // D1-1: changing a link does not freeze anything —
    // freezing is still `execute_binding`'s job.
    if let Some(goal_id) = req.goal_id {
        record.goal_id = Some(goal_id);
    }
    if let Some(project_id) = req.project_id {
        record.project_id = Some(project_id);
    }
    if let Some(workspace_id) = req.workspace_id {
        record.workspace_id = Some(workspace_id);
    }
    let updated = record.clone();
    persist_task(&state, &updated);
    drop(tasks);
    // D1-2 (Q-D1-22): a goal change supersedes the old
    // task-level Mandates and creates a fresh one.
    if goal_changed {
        supersede_task_mandates_and_create(&state, &updated);
    }
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
    if !crate::task_store::valid_task_status_transition(&record.status, "running") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
    record.status = "running".to_string();
    record.error = None;
    let updated = record.clone();
    persist_task(&state, &updated);
    drop(tasks);
    // P0 phase 1: the runner attempt gets its Execution shell record.
    // E2-A §2.3: a fresh attempt supersedes any still-running
    // Execution a crashed earlier attempt left behind.
    let execution = create_execution(&state, Some(req.task_id.clone()), None);
    supersede_old_task_executions(&state, &req.task_id, &execution.execution_id);
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
    let Some(mut record) = state
        .tasks
        .lock()
        .expect("task lock")
        .get(&req.task_id)
        .cloned()
    else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown task" })),
        )
            .into_response();
    };
    if !crate::task_store::valid_task_status_transition(&record.status, "completed") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
    // E2 slice, scheme A (P3-C plan §2.3): a stated worktree
    // must exist and be active, and the first execute that
    // names one freezes the binding into the task record. A
    // later execute naming a different worktree is 409
    // `task_binding_frozen_mismatch` — the Worker runs the
    // frozen binding, never a re-resolved one.
    if let Some(worktree_id) = &req.worktree_id {
        let binding = match resolve_worktree_binding(&state, worktree_id) {
            Ok(binding) => binding,
            Err(detail) => return bad_request(detail),
        };
        let frozen = TaskExecuteBinding {
            location_id: binding.location_id.clone(),
            worktree_id: binding.worktree_id.clone(),
            root_digest: binding.root_digest.clone(),
        };
        match &record.execute_binding {
            Some(existing) if *existing != frozen => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "task_binding_frozen_mismatch" })),
                )
                    .into_response();
            }
            Some(_) => {}
            None => {
                record.execute_binding = Some(frozen);
                state
                    .tasks
                    .lock()
                    .expect("task lock")
                    .insert(record.task_id.clone(), record.clone());
                persist_task(&state, &record);
            }
        }
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
    // P0-C: the task write is planned here (the execute request
    // is its first execution step; the task approval itself was
    // created by the runner, not by /approval/request, so no
    // planned row exists yet). A terminal Effect for the same
    // key replays the task record without writing; an unsettled
    // one refuses — except the execute's own planned row from
    // an earlier attempt (E2-A fix F-1, below): that row resumes
    // instead of refusing. Otherwise the approval is consumed
    // bound to the planned Effect and the Effect is driven
    // forward.
    let task_execution = execution_for_task(&state, &req.task_id);
    // E2-A §2.4: a task with no Execution has no key to check
    // — refuse with 409, not the store-broken 500 below.
    if idempotency_key_missing(task_execution.as_deref(), Some(&digest)) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "effect_idempotency_unavailable" })),
        )
            .into_response();
    }
    // E2-A fix F-1: an execute retried after its own first
    // attempt (approval_required before the approval was
    // decided) finds the planned Effect that first attempt left
    // behind. When the blocking row is that very row — status
    // `planned` and bound to this execute's approval — resume
    // it: reuse the existing row as `task_effect` and drive it
    // through the same tail below (consume → start → write →
    // completed). No second Effect row is created (a second
    // insert would collide on the UNIQUE idempotency key).
    // Any other unsettled row (started, authorized, or another
    // approval's) still refuses with 409.
    let task_effect = match check_idempotency(
        &state,
        task_execution.as_deref(),
        "tasks",
        "execute",
        &req.task_id,
        Some(&digest),
        None,
    ) {
        IdempotencyDecision::Replay(_) => {
            return Json(record).into_response();
        }
        IdempotencyDecision::Blocked(other) => {
            if other.status == "planned"
                && other.approval_id.as_deref() == Some(approval_id.as_str())
            {
                other
            } else {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
                )
                    .into_response();
            }
        }
        // P0-D: broken migrated-domain store fails closed.
        IdempotencyDecision::Unavailable => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "sqlite_unavailable" })),
            )
                .into_response();
        }
        IdempotencyDecision::Proceed => record_effect(
            &state,
            NewEffect {
                execution_id: task_execution,
                capability: "tasks".to_string(),
                operation: "execute".to_string(),
                scope: req.task_id.clone(),
                input_digest: Some(digest.clone()),
                approval_id: Some(approval_id.clone()),
                status: "planned".to_string(),
            },
        ),
    };
    if let Err(response) = consume_bound_approval_for_effect(
        &state,
        "fs.write",
        &Some(approval_id.clone()),
        &digest,
        Some(&task_effect.effect_id),
    ) {
        return *response;
    }
    if let Err(response) = start_bound_effect(&state, &task_effect) {
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
            let _ = transition_effect(&state, &task_effect.effect_id, "failed", None, None);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": "cannot create parent directory" })),
            )
                .into_response();
        }
    }
    if std::fs::write(&full, draft.as_bytes()).is_err() {
        crate::task_runner::fail(&state, &req.task_id, "产物写入失败");
        // P0-C: the artifact write was attempted and failed,
        // and the task now persists as failed — the planned
        // Effect drives to `failed`.
        let _ = transition_effect(&state, &task_effect.effect_id, "failed", None, None);
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": "cannot write file" })),
        )
            .into_response();
    }
    // P0-R: the tasks guard must be released before the Effect
    // transition below: its write-time Evidence hook re-locks
    // the tasks map to read the completed record.
    let updated = {
        let mut tasks = state.tasks.lock().expect("task lock");
        let Some(current) = tasks.get_mut(&req.task_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" }))).into_response();
        };
        current.status = "completed".to_string();
        current.error = None;
        current.clone()
    };
    persist_task(&state, &updated);
    // E2-A §2.3: the terminal Task closes its Execution.
    close_execution_for_task(&state, &req.task_id, "completed");
    // P0-C: a successful task write drives the planned
    // Effect to `succeeded`. The digest is the artifact
    // write digest the approval gate consumed; the
    // operation is the task execution, not the underlying
    // file write.
    let _ = transition_effect(&state, &task_effect.effect_id, "succeeded", None, None);
    Json(updated).into_response()
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
    if !crate::task_store::valid_task_status_transition(&record.status, "rejected") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
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
        drop(tasks);
        // E2-A §2.3: the terminal Task closes its Execution
        // (as `failed`: Execution has no `rejected` word).
        close_execution_for_task(&state, &req.task_id, "rejected");
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
    if !crate::task_store::valid_task_status_transition(&record.status, "running") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
    // Contract: resume is accepted, not completed. The returned
    // record is the pre-run snapshot (still interrupted); the
    // runner flips it to running or draft_ready promptly, and
    // callers poll until the status leaves interrupted. A
    // fabricated running record would lie about the runner's
    // state, and blocking here would stall on a model call.
    // P0 phase 1: a resume spawns a fresh runner attempt, so it
    // gets a fresh Execution shell record.
    // E2-A §2.3: a fresh attempt supersedes any still-running
    // Execution a crashed earlier attempt left behind.
    let execution = create_execution(&state, Some(req.task_id.clone()), None);
    supersede_old_task_executions(&state, &req.task_id, &execution.execution_id);
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
    // E2-A §2.2: the lifecycle entries enforce the edge table;
    // anything else is 409 `task_status_transition_invalid`.
    // (The manual /tasks/update override keeps its own
    // word-whitelist contract and is not governed by it.)
    if !crate::task_store::valid_task_status_transition(&record.status, "cancelled") {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task_status_transition_invalid" })))
            .into_response();
    }
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
                drop(tasks);
                // E2-A §2.3: the terminal Task closes its Execution.
                close_execution_for_task(&state, &req.task_id, "cancelled");
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
                drop(tasks);
                // E2-A §2.3: the terminal Task closes its Execution.
                close_execution_for_task(&state, &req.task_id, "cancelled");
                return Json(updated).into_response();
            }
            (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown task" })))
                .into_response()
        }
        _ => (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "task not cancellable" })))
            .into_response(),
    }
}

// --- Execution shell (P0 phase 1) ---
//
// Read-only observability for the phase-1 shell records. Creation
// happens only as a side effect of task-runner / loop attempts
// (see create_execution); there is deliberately no create route
// yet, and no status transitions until phase 4.

#[derive(Debug, Deserialize)]
struct ExecutionGetQuery {
    execution_id: String,
}

/// P0-E read view: the stored record plus the two derived
/// fields (plan §2.2). Additive only: old clients reading the
/// record fields see no change. Both routes build views
/// through this one function, which is the single supplier of
/// the recoverability judgement — there is no second copy of
/// the algorithm in the get path.
#[derive(Debug, Serialize)]
struct ExecutionView {
    #[serde(flatten)]
    record: ExecutionFileRecord,
    recoverable: bool,
    active_assignment_id: Option<String>,
}

/// Attach derived fields to records. In SQLite mode the
/// driving-assignment map comes from one SQLite query; a
/// failed map read fails the whole response closed (a view
/// computed as "no drivers" from a broken read would lie
/// about recoverability). Outside SQLite mode there is no
/// assignment table, so the map is empty by construction.
fn execution_views(
    state: &AppState,
    records: Vec<ExecutionFileRecord>,
) -> Option<Vec<ExecutionView>> {
    let active_map: HashMap<String, String> = if state.sqlite_mode() == StateBackend::Sqlite {
        let sqlite = state.sqlite.as_ref()?;
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::active_assignments_map(&conn).ok()?
    } else {
        HashMap::new()
    };
    Some(
        records
            .into_iter()
            .map(|record| {
                let active = active_map.get(&record.execution_id).cloned();
                let recoverable = crate::execution_store::execution_recoverable(
                    &record.status,
                    active.is_some(),
                );
                ExecutionView { record, recoverable, active_assignment_id: active }
            })
            .collect(),
    )
}

async fn executions_list(State(state): State<AppState>) -> impl IntoResponse {
    // P0-D: SQLite mode reads SQLite; a broken database fails
    // closed (5xx) instead of serving a stale map.
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = executions_snapshot(&state) else {
            return sqlite_error_response();
        };
        let Some(mut views) = execution_views(&state, records) else {
            return sqlite_error_response();
        };
        views.sort_by_key(|v| execution_number(&v.record.execution_id));
        return Json(views).into_response();
    }
    let mut records: Vec<ExecutionFileRecord> =
        state.executions.lock().expect("execution lock").values().cloned().collect();
    records.sort_by_key(|r| execution_number(&r.execution_id));
    let views = execution_views(&state, records).expect("non-SQLite views cannot fail");
    Json(views).into_response()
}

async fn executions_get(
    State(state): State<AppState>,
    Query(query): Query<ExecutionGetQuery>,
) -> impl IntoResponse {
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = executions_snapshot(&state) else {
            return sqlite_error_response();
        };
        let Some(record) = records.into_iter().find(|r| r.execution_id == query.execution_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown execution" })))
                .into_response();
        };
        let Some(mut views) = execution_views(&state, vec![record]) else {
            return sqlite_error_response();
        };
        return Json(views.remove(0)).into_response();
    }
    match state.executions.lock().expect("execution lock").get(&query.execution_id) {
        Some(record) => {
            let views =
                execution_views(&state, vec![record.clone()]).expect("non-SQLite views cannot fail");
            Json(views.into_iter().next().expect("one view")).into_response()
        }
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown execution" })))
            .into_response(),
    }
}

/// P0-E: after a takeover or transition rewrites an execution
/// row in SQLite mode, regenerate the JSONL audit export
/// best-effort (the P0-D rule: exports refresh on writes, and
/// a takeover/transition is a write even though it is not a
/// persist_execution call).
fn refresh_executions_export(state: &AppState) {
    if state.sqlite_mode() != StateBackend::Sqlite {
        return;
    }
    let (Some(sqlite), Some(path)) = (&state.sqlite, &state.executions_path) else {
        return;
    };
    let conn = sqlite.lock().expect("sqlite lock");
    if let Err(detail) = sqlite_store::export_executions(&conn, path) {
        eprintln!("warning: P0-E executions audit export failed: {detail}");
    }
}

// --- P0-E Assignment / Execution-transition routes (gateway
// only, no client) ---
//
// Takeover and transition are SQLite-plane operations: they
// require SQLite mode (not shadow, not JSONL). Shadow mode is
// a transient pre-switch state in production (startup
// auto-switches), and its JSONL authority would silently
// diverge from a takeover's SQLite-only assignment rows, so
// refusing there is the fail-closed choice, not an omission.
// Assignment reads, by contrast, touch SQLite-only tables
// with no JSONL counterpart, so they mirror the Inbox routes
// and serve shadow mode too.

#[derive(Debug, Deserialize)]
struct TakeoverDto {
    execution_id: String,
    worker_session_id: Option<String>,
}

async fn assignments_takeover(
    State(state): State<AppState>,
    Json(req): Json<TakeoverDto>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let result = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::takeover_assignment(
            &mut conn,
            &req.execution_id,
            req.worker_session_id.as_deref(),
            &state.origin_device_id,
        )
    };
    match result {
        Ok(outcome) => {
            // The connection guard is dropped before the map
            // lock (persist sites take them in the opposite
            // order); holding both at once would risk deadlock.
            state
                .executions
                .lock()
                .expect("execution lock")
                .insert(outcome.execution.execution_id.clone(), outcome.execution.clone());
            // JUDGEMENT CALL (plan is silent): after a
            // takeover, this process drives AS the takeover
            // worker, so it becomes the current worker: later
            // executions stamp it, and a clean shutdown ends
            // it (with its assignment) instead of only the
            // scan worker. Without this, a takeover worker's
            // assignment would survive a clean exit until the
            // next scan, contradicting §2.1 convergence.
            *state.current_worker_session.lock().expect("current worker lock") =
                outcome.assignment.worker_session_id.clone();
            refresh_executions_export(&state);
            Json(serde_json::json!({
                "assignment": outcome.assignment,
                "execution": outcome.execution,
                "checkpoint": outcome.checkpoint,
                "superseded": outcome.superseded,
            }))
            .into_response()
        }
        Err(sqlite_store::TakeoverError::Refused(code)) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": code })),
        )
            .into_response(),
        Err(sqlite_store::TakeoverError::Internal(detail)) => {
            *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
            eprintln!("warning: P0-E takeover failed: {detail}");
            sqlite_error_response()
        }
    }
}

async fn assignments_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_assignments(&conn) {
        Ok(rows) => Json(rows).into_response(),
        Err(detail) => {
            eprintln!("warning: P0-E assignments list failed: {detail}");
            sqlite_error_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct AssignmentGetQuery {
    assignment_id: String,
}

async fn assignments_get(
    State(state): State<AppState>,
    Query(query): Query<AssignmentGetQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_assignment(&conn, &query.assignment_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown assignment" })))
            .into_response(),
        Err(detail) => {
            eprintln!("warning: P0-E assignments get failed: {detail}");
            sqlite_error_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExecutionTransitionDto {
    execution_id: String,
    status: String,
}

async fn executions_transition(
    State(state): State<AppState>,
    Json(req): Json<ExecutionTransitionDto>,
) -> impl IntoResponse {
    if !crate::execution_store::valid_execution_status(&req.status) {
        return bad_request("unknown execution status");
    }
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let result = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::transition_execution(&mut conn, &req.execution_id, &req.status)
    };
    match result {
        Ok(record) => {
            state
                .executions
                .lock()
                .expect("execution lock")
                .insert(record.execution_id.clone(), record.clone());
            refresh_executions_export(&state);
            Json(record).into_response()
        }
        Err(sqlite_store::ExecutionTransitionError::NotFound) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown execution" })),
        )
            .into_response(),
        Err(sqlite_store::ExecutionTransitionError::Illegal) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "execution_transition_refused" })),
        )
            .into_response(),
        Err(sqlite_store::ExecutionTransitionError::Internal(detail)) => {
            *state.sqlite_broken.lock().expect("sqlite broken lock") = true;
            eprintln!("warning: P0-E execution transition failed: {detail}");
            sqlite_error_response()
        }
    }
}

// --- E2 slice: Worker runs (P3-C plan §3.6) ---
//
// SQLite mode only, like takeover/transition: the
// `worker_runs` table is SQLite. Read templates only in
// this slice, so no approval is required and no Effect is
// produced on success.

#[derive(Debug, Deserialize)]
pub(crate) struct WorkerRunCreateDto {
    worktree_id: String,
    command: String,
    #[serde(default)]
    params: Option<serde_json::Value>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    execution_id: Option<String>,
    /// P3-C: the approval authorizing this run, when one was
    /// presented. Reads need none; writes and tests carry one
    /// from the loop (already consumed there) or from a
    /// digest-bound /approval/request (consumed at creation).
    /// Stored on the run row for Effect attribution.
    #[serde(default)]
    approval_id: Option<String>,
}

/// P3-C run-creation failure: an HTTP status + a short
/// error code. The route renders it as JSON; the loop
/// `worker.run` arm maps it to a ToolOutcome.
pub(crate) struct WorkerCreateError {
    pub status: StatusCode,
    pub code: &'static str,
}

fn worker_create_error(status: StatusCode, code: &'static str) -> WorkerCreateError {
    WorkerCreateError { status, code }
}

fn worker_binding_code(detail: &'static str) -> &'static str {
    match detail {
        "unknown worktree" => "unknown_worktree",
        "worktree is archived" => "worktree_archived",
        "unknown location" => "unknown_location",
        "location is archived" => "location_archived",
        other => other,
    }
}

/// P3-C: the binary for a template. git templates use the
/// startup-resolved git; cargo-test uses cargo. None fails
/// closed with the template's 503 code.
fn worker_binary_for_command(
    state: &AppState,
    command: &str,
) -> Result<PathBuf, WorkerCreateError> {
    if command == "cargo-test" {
        state.cargo_path.clone().ok_or_else(|| {
            worker_create_error(StatusCode::SERVICE_UNAVAILABLE, "cargo_unavailable")
        })
    } else {
        state.git_path.clone().ok_or_else(|| {
            worker_create_error(StatusCode::SERVICE_UNAVAILABLE, "git_unavailable")
        })
    }
}

/// P3-C (§4.2): the worktree-add preconditions, checked
/// before the run is created (branch must not exist, target
/// path must be free, parent must exist). These are
/// best-effort gates, not guarantees: a concurrent creation
/// still fails the git command itself (exit != 0 → failed
/// run, known result). The branch check reads the repo
/// (`git show-ref` in the parent worktree); the path check
/// reads the filesystem under the Location root.
fn check_worktree_add_preconditions(
    state: &AppState,
    git: &Path,
    location_root: &Path,
    repo_root: &Path,
    path: &str,
    branch: &str,
) -> Result<(), WorkerCreateError> {
    let target = crate::worker::contained_path(location_root, path)
        .ok_or_else(|| worker_create_error(StatusCode::BAD_REQUEST, "invalid_path"))?;
    if target.exists() {
        return Err(worker_create_error(
            StatusCode::CONFLICT,
            "worktree_path_exists",
        ));
    }
    if let Some(parent) = target.parent() {
        if !parent.is_dir() {
            return Err(worker_create_error(StatusCode::BAD_REQUEST, "invalid_path"));
        }
    }
    let refname = format!("refs/heads/{branch}");
    let out = crate::worker::run_command_sync(
        state,
        &[
            git.to_string_lossy().into_owned(),
            "show-ref".to_string(),
            "--verify".to_string(),
            "--quiet".to_string(),
            refname,
        ],
        repo_root,
        git,
        std::time::Duration::from_secs(10),
    );
    if !out.timed_out && out.exit_code == Some(0) {
        return Err(worker_create_error(
            StatusCode::CONFLICT,
            "worktree_branch_exists",
        ));
    }
    Ok(())
}

/// Rebuild the run-route params JSON from flat loop-normalized
/// args (P3-C): the loop stores each parameter as its own
/// string key; the allowlist takes a JSON object. Only the
/// template's own keys are copied.
pub(crate) fn worker_run_params_from_args(
    args: &std::collections::BTreeMap<String, String>,
) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for key in ["path", "branch", "message", "package"] {
        if let Some(value) = args.get(key) {
            map.insert(
                key.to_string(),
                serde_json::Value::String(value.clone()),
            );
        }
    }
    serde_json::Value::Object(map)
}

/// P3-C (§7): the command digest for a loop `worker.run`
/// proposal, computed from the resolved command (never from
/// a client hash). Resolves the worktree (binding + Location
/// root), the binary, and the allowlisted command, then
/// returns the digest + the CommandSpec the approval carries.
/// The loop planning branch calls this — `complete_work` has
/// no AppState, so the proposal-time digest is only the
/// normalized-args digest, overwritten here.
pub(crate) fn worker_command_digest(
    state: &AppState,
    args: &std::collections::BTreeMap<String, String>,
) -> Result<(String, CommandSpec), WorkerCreateError> {
    let worktree_id = args
        .get("worktree_id")
        .ok_or_else(|| worker_create_error(StatusCode::BAD_REQUEST, "missing worktree_id"))?;
    let command = args
        .get("command")
        .ok_or_else(|| worker_create_error(StatusCode::BAD_REQUEST, "missing command"))?;
    let timeout_ms = args
        .get("timeout_ms")
        .and_then(|s| s.parse::<u64>().ok());
    let params = worker_run_params_from_args(args);
    let binding = resolve_worktree_binding(state, worktree_id)
        .map_err(|detail| worker_create_error(StatusCode::BAD_REQUEST, worker_binding_code(detail)))?;
    let worktree = {
        let worktrees = state.worktrees.lock().expect("worktree lock");
        worktrees
            .get(worktree_id)
            .cloned()
            .ok_or_else(|| worker_create_error(StatusCode::BAD_REQUEST, "unknown_worktree"))?
    };
    let cwd = worktree_canonical_root(state, &worktree)
        .ok_or_else(|| worker_create_error(StatusCode::BAD_REQUEST, "worktree_directory_missing"))?;
    let location_root = location_root_for_id(state, &binding.location_id)
        .ok_or_else(|| worker_create_error(StatusCode::BAD_REQUEST, "unknown_location"))?;
    let binary = worker_binary_for_command(state, command)?;
    let resolved = crate::worker::resolve_command(
        command,
        Some(&params),
        &binary,
        &cwd,
        timeout_ms,
        Some(&location_root),
    )
    .map_err(|error| worker_create_error(StatusCode::BAD_REQUEST, error.code()))?;
    if command == "git-worktree-add" {
        // The preconditions are 409s on the route; here a
        // failure fails the loop call (the model proposed a
        // command that cannot run as stated).
        let (path, branch) = match (args.get("path"), args.get("branch")) {
            (Some(p), Some(b)) => (p.as_str(), b.as_str()),
            _ => {
                return Err(worker_create_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_params",
                ));
            }
        };
        check_worktree_add_preconditions(
            state,
            &binary,
            &location_root,
            &cwd,
            path,
            branch,
        )?;
    }
    let cwd_str = cwd.to_string_lossy().into_owned();
    let argv_sha256 = crate::operation_binding::sha256_hex(
        serde_json::to_string(&resolved.argv)
            .unwrap_or_default()
            .as_bytes(),
    );
    let spec = CommandSpec {
        argv_template: command.clone(),
        argv_sha256,
        cwd: cwd_str.clone(),
        timeout_ms: resolved.timeout_ms,
    };
    let digest = crate::operation_binding::command_digest(
        &resolved.argv,
        &cwd_str,
        resolved.timeout_ms,
    );
    Ok((digest, spec))
}

/// P3-C (§7): validate a presented approval_id for a run
/// creation. Digest-bound approvals (from /approval/request)
/// are consumed here, under the approvals lock (tool +
/// approved + unconsumed + digest match, single-use); a
/// second presentation fails (already consumed).
/// Loop-bound approvals (from a loop's worker.run call,
/// already consumed by the loop in resume()) are verified
/// here: approved + consumed + the same tool + the same
/// digest. Their single-use rule is the run row's UNIQUE
/// index on approval_id (checked atomically at insert, and
/// fast-pathed by the caller before an Execution is made).
/// A missing approval, a wrong tool, or a digest mismatch
/// fails closed. Returns the approval id to store on the run
/// row (None when no approval was presented).
fn validate_worker_approval(
    state: &AppState,
    approval_id: &Option<String>,
    digest: &str,
) -> Result<Option<String>, WorkerCreateError> {
    let Some(id) = approval_id else {
        return Ok(None);
    };
    let is_loop_bound = {
        let approvals = state.approvals.lock().expect("approval lock");
        let Some(record) = approvals.get(id) else {
            return Err(worker_create_error(
                StatusCode::NOT_FOUND,
                "unknown_approval",
            ));
        };
        if record.tool != "worker.run" {
            return Err(worker_create_error(
                StatusCode::FORBIDDEN,
                "approval_required",
            ));
        }
        if record.status == ApprovalStatus::Rejected {
            return Err(worker_create_error(
                StatusCode::FORBIDDEN,
                "approval_rejected",
            ));
        }
        if record.status != ApprovalStatus::Approved {
            return Err(worker_create_error(
                StatusCode::FORBIDDEN,
                "approval_required",
            ));
        }
        if record.binding.is_some() {
            if !record.consumed
                || record.operation_digest.as_deref() != Some(digest)
            {
                return Err(worker_create_error(
                    StatusCode::FORBIDDEN,
                    "approval_binding_mismatch",
                ));
            }
            true
        } else {
            false
        }
    };
    if is_loop_bound {
        return Ok(Some(id.clone()));
    }
    // Digest-bound: consume atomically (the check above
    // already failed closed on status; the consume re-checks
    // tool + digest + unconsumed under the same lock).
    consume_bound_approval(state, "worker.run", approval_id, digest).map_err(|_| {
        worker_create_error(StatusCode::FORBIDDEN, "approval_binding_mismatch")
    })?;
    Ok(Some(id.clone()))
}

/// E2-C §4.1: the shared ledger entry for every synchronous
/// write (Device direct + loop, Connector). Only the ENTRY
/// bookkeeping is shared — resolve or build the Execution,
/// record the planned Effect, run the idempotency gate, drive
/// the row to its terminal status — and the drivers stay as
/// they are: nothing here spawns, queues, or runs a Worker,
/// and execution stays synchronous in the caller.
///
/// Shape note (deviation from the plan sketch, forced by the
/// code): the sketch shows one callback-shaped function
/// (plan → gate → execute → terminal). Every current caller
/// is two-phase — the planned row is created in one request
/// (actions_create / plan_loop_effect / the connector
/// handler) and executed in a later one, after an approval
/// is decided — and the Device execution is
/// `spawn_blocking` + await, which a synchronous closure
/// cannot hold. A literal callback parameter would therefore
/// have no caller, and an uncalled `pub(crate)` generic
/// function fails clippy's dead_code lint under `-D
/// warnings`. So the entry is split in two: `ledger_entry`
/// (plan + gate) and `ledger_entry_finish` (terminal
/// transition), and the caller's synchronous execution runs
/// between them. The "no spawn, no queue" discipline is
/// preserved: neither function spawns or queues.
///
/// Gate ordering (deliberate): the gate runs BEFORE the
/// planned row is recorded, so a Replay/Blocked refusal
/// leaves no stray planned row behind.
///
/// Device recovery semantics (§4.2): a Device write has no
/// worker_runs row for the recovery scan to claim. It either
/// completes inside the call — the Effect reaching a terminal
/// status through `ledger_entry_finish` — or the caller gets
/// an error and a failed/unknown Effect is recorded (the
/// timeout arm in `device_set` / the loop tool, and the
/// fallback terminal record in `device_set`).
pub(crate) enum LedgerOutcome {
    Succeeded,
    Failed,
    Unknown,
}

pub(crate) struct LedgerEntry {
    pub execution_id: String,
    pub effect: EffectFileRecord,
}

pub(crate) enum LedgerEntryDecision {
    Entered(LedgerEntry),
    Replay(EffectFileRecord),
    Blocked(EffectFileRecord),
}

/// E2-C §4.1 plan step: resolve the Execution (reuse the
/// caller's when given, else build a minimal one — exactly
/// `adapter_execution`) and record the planned Effect, after
/// the idempotency gate. A Replay/Blocked decision means the
/// caller must NOT execute: an identical write is already
/// settled (Replay) or still unsettled (Blocked), and the
/// existing row is the audit trail. `Unavailable` (broken
/// store, no snapshot to dedupe against) proceeds: recording
/// itself is best-effort and warns the same way.
pub(crate) fn ledger_entry(
    state: &AppState,
    execution_id: Option<String>,
    capability: &str,
    operation: &str,
    scope: &str,
    input_digest: Option<String>,
    approval_id: Option<String>,
) -> LedgerEntryDecision {
    let execution_id = execution_id.unwrap_or_else(|| adapter_execution(state));
    if let Some(digest) = &input_digest {
        match check_idempotency(
            state,
            Some(&execution_id),
            capability,
            operation,
            scope,
            Some(digest),
            None,
        ) {
            IdempotencyDecision::Proceed => {}
            IdempotencyDecision::Replay(other) => return LedgerEntryDecision::Replay(other),
            IdempotencyDecision::Blocked(other) => return LedgerEntryDecision::Blocked(other),
            IdempotencyDecision::Unavailable => {}
        }
    }
    let effect = record_effect(
        state,
        NewEffect {
            execution_id: Some(execution_id.clone()),
            capability: capability.to_string(),
            operation: operation.to_string(),
            scope: scope.to_string(),
            input_digest,
            approval_id,
            status: "planned".to_string(),
        },
    );
    LedgerEntryDecision::Entered(LedgerEntry {
        execution_id,
        effect,
    })
}

/// E2-C §4.1 finish step: drive a planned/authorized/
/// started Effect to its terminal status through the single
/// transition enforcement point (`transition_effect_chain`).
/// The chain is built from the row's current status, so a
/// caller that already drove the row part-way (approval
/// consume → authorized) lands on the same terminal path.
/// An already-terminal row with the same word is returned
/// unchanged (idempotent finish); a different terminal word
/// is a 409, and a missing row is a 404.
pub(crate) fn ledger_entry_finish(
    state: &AppState,
    effect_id: &str,
    outcome: LedgerOutcome,
) -> Result<EffectFileRecord, u16> {
    let terminal = match outcome {
        LedgerOutcome::Succeeded => "succeeded",
        LedgerOutcome::Failed => "failed",
        LedgerOutcome::Unknown => "unknown",
    };
    let Some(current) = effect_by_id(state, effect_id) else {
        return Err(404);
    };
    if current.status == terminal {
        return Ok(current);
    }
    if crate::effect_store::is_terminal_effect_status(&current.status) {
        return Err(409);
    }
    let steps: &[&str] = match current.status.as_str() {
        "planned" => &["authorized", "started", terminal],
        "authorized" => &["started", terminal],
        "started" => &[terminal],
        _ => return Err(409),
    };
    transition_effect_chain(state, effect_id, steps)
}

/// P3-C: the shared run-creation path (the HTTP route and the
/// loop `worker.run` arm). Validates the binding, resolves
/// the command, checks the worktree-add preconditions,
/// validates + consumes a presented approval, creates (or
/// reuses) the Execution, sets the deadline, inserts the run
/// row, and spawns the driver. The loop arm uses the same
/// helper, so a loop-proposed command and a direct route call
/// can never drift apart.
pub(crate) async fn create_worker_run_internal(
    state: &AppState,
    req: WorkerRunCreateDto,
) -> Result<crate::worker::WorkerRunRow, WorkerCreateError> {
    if state.sqlite_is_broken() {
        return Err(worker_create_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "sqlite_unavailable",
        ));
    }
    let Some(sqlite) = &state.sqlite else {
        return Err(worker_create_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "sqlite_not_configured",
        ));
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return Err(worker_create_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "sqlite_not_configured",
        ));
    }
    let binary = worker_binary_for_command(state, &req.command)?;
    let binding = match resolve_worktree_binding(state, &req.worktree_id) {
        Ok(binding) => binding,
        Err(detail) => {
            return Err(worker_create_error(
                StatusCode::BAD_REQUEST,
                worker_binding_code(detail),
            ));
        }
    };
    let worktree = match state
        .worktrees
        .lock()
        .expect("worktree lock")
        .get(&req.worktree_id)
        .cloned()
    {
        Some(worktree) => worktree,
        None => {
            return Err(worker_create_error(
                StatusCode::BAD_REQUEST,
                "unknown_worktree",
            ));
        }
    };
    let Some(cwd) = worktree_canonical_root(state, &worktree) else {
        return Err(worker_create_error(
            StatusCode::BAD_REQUEST,
            "worktree_directory_missing",
        ));
    };
    let location_root = location_root_for_id(state, &binding.location_id);
    let resolved = match crate::worker::resolve_command(
        &req.command,
        req.params.as_ref(),
        &binary,
        &cwd,
        req.timeout_ms,
        location_root.as_deref(),
    ) {
        Ok(resolved) => resolved,
        Err(error) => {
            return Err(worker_create_error(
                StatusCode::BAD_REQUEST,
                error.code(),
            ));
        }
    };
    if req.command == "git-worktree-add" {
        let (path, branch) = match req.params.as_ref() {
            Some(serde_json::Value::Object(map)) => {
                let path = map
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let branch = map
                    .get("branch")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                (path, branch)
            }
            _ => ("", ""),
        };
        let Some(location_root) = &location_root else {
            return Err(worker_create_error(
                StatusCode::BAD_REQUEST,
                "unknown_location",
            ));
        };
        check_worktree_add_preconditions(
            state,
            &binary,
            location_root,
            &cwd,
            path,
            branch,
        )?;
    }
    // The approval gate (§7): the digest commits the exact
    // resolved argv + cwd + timeout. A digest-bound approval
    // is consumed here; a loop-bound approval was already
    // consumed by the loop and is verified here. The gate
    // runs before any Execution is created, so a rejected
    // approval never leaves a stray Execution behind.
    let cwd_str = cwd.to_string_lossy().into_owned();
    let digest = crate::operation_binding::command_digest(
        &resolved.argv,
        &cwd_str,
        resolved.timeout_ms,
    );
    let approval_id = validate_worker_approval(state, &req.approval_id, &digest)?;
    // Loop-bound replay fast-path (before an Execution is
    // created; the atomic guard is the UNIQUE index at
    // insert).
    if let Some(id) = &approval_id {
        let exists = {
            let conn = sqlite.lock().expect("sqlite lock");
            sqlite_store::worker_run_exists_for_approval(&conn, id)
        };
        match exists {
            Ok(true) => {
                return Err(worker_create_error(
                    StatusCode::CONFLICT,
                    "approval_already_used",
                ));
            }
            Ok(false) => {}
            Err(detail) => {
                eprintln!("warning: E2 worker run approval check failed: {detail}");
                return Err(worker_create_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "sqlite_unavailable",
                ));
            }
        }
    }
    // A named Execution must exist, be non-terminal, and —
    // when its snapshot carries a binding — agree with the
    // requested worktree. An unnamed run gets a minimal
    // Execution frozen to this binding (P0-B precedent).
    let execution_id = match &req.execution_id {
        Some(execution_id) => {
            let execution = {
                let conn = sqlite.lock().expect("sqlite lock");
                sqlite_store::get_execution(&conn, execution_id)
            };
            let execution = match execution {
                Ok(Some(execution)) => execution,
                Ok(None) => {
                    return Err(worker_create_error(
                        StatusCode::NOT_FOUND,
                        "unknown_execution",
                    ));
                }
                Err(detail) => {
                    eprintln!("warning: E2 worker run execution read failed: {detail}");
                    return Err(worker_create_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "sqlite_unavailable",
                    ));
                }
            };
            if crate::execution_store::execution_status_group(&execution.status) == Some("terminal")
            {
                return Err(worker_create_error(
                    StatusCode::CONFLICT,
                    "execution_terminal",
                ));
            }
            if let Some(snapshot_id) = &execution.workspace_snapshot_id {
                let snapshot = {
                    let conn = sqlite.lock().expect("sqlite lock");
                    sqlite_store::get_snapshot(&conn, snapshot_id)
                };
                if let Ok(Some(snapshot)) = snapshot {
                    if let Some(bound) = snapshot.binding_worktree_id {
                        if bound != req.worktree_id {
                            return Err(worker_create_error(
                                StatusCode::CONFLICT,
                                "execution_binding_mismatch",
                            ));
                        }
                    }
                }
            }
            let busy = {
                let conn = sqlite.lock().expect("sqlite lock");
                sqlite_store::list_worker_runs(&conn, Some(execution_id), None)
            };
            match busy {
                Ok(rows)
                    if rows
                        .iter()
                        .any(|row| matches!(row.status.as_str(), "queued" | "running")) =>
                {
                    return Err(worker_create_error(
                        StatusCode::CONFLICT,
                        "execution_has_running_worker",
                    ));
                }
                Ok(_) => {}
                Err(detail) => {
                    eprintln!("warning: E2 worker run busy check failed: {detail}");
                    return Err(worker_create_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "sqlite_unavailable",
                    ));
                }
            }
            execution_id.clone()
        }
        None => {
            create_execution_with_binding(
                state,
                None,
                None,
                Some(SnapshotBinding {
                    location_id: binding.location_id.clone(),
                    worktree_id: binding.worktree_id.clone(),
                    root_digest: binding.root_digest.clone(),
                }),
                None,
                None,
            )
            .execution_id
        }
    };
    // Attribution (plan §2.3): the driving assignment, when
    // one exists, and this process's worker session.
    let (assignment_id, worker_session_id) = {
        let conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::active_assignment_for_execution(&conn, &execution_id) {
            Ok(Some(assignment)) => (
                Some(assignment.assignment_id),
                assignment
                    .worker_session_id
                    .or_else(|| state.current_worker_session()),
            ),
            _ => (None, state.current_worker_session()),
        }
    };
    // Budget (plan §2.4): the Execution deadline is now +
    // this run's timeout, or the stored (earlier) deadline —
    // never extended by a later run.
    let now = crate::effect_store::now_ms() as i64;
    {
        let mut conn = sqlite.lock().expect("sqlite lock");
        if let Err(detail) = sqlite_store::set_execution_deadline(
            &mut conn,
            &execution_id,
            now + resolved.timeout_ms as i64,
        ) {
            eprintln!("warning: E2 worker run deadline write failed: {detail}");
            return Err(worker_create_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sqlite_unavailable",
            ));
        }
    }
    let row = crate::worker::WorkerRunRow {
        run_id: String::new(),
        execution_id: Some(execution_id),
        assignment_id,
        worker_session_id,
        command: resolved.template.to_string(),
        argv_json: serde_json::to_string(&resolved.argv).unwrap_or_default(),
        cwd: cwd.to_string_lossy().into_owned(),
        timeout_ms: resolved.timeout_ms as i64,
        status: "queued".to_string(),
        exit_code: None,
        pid: None,
        started_ms: None,
        finished_ms: None,
        tail: String::new(),
        truncated: false,
        effect_id: None,
        approval_id,
        binding_location_id: Some(binding.location_id),
        binding_worktree_id: Some(binding.worktree_id),
        binding_root_digest: Some(binding.root_digest),
        schema_version: 1,
        progress_json: None,
    };
    let row = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::create_worker_run(&mut conn, &row)
    };
    let row = match row {
        Ok(row) => row,
        Err(detail) if detail == "execution_has_running_worker" => {
            return Err(worker_create_error(
                StatusCode::CONFLICT,
                "execution_has_running_worker",
            ));
        }
        Err(detail) if detail == "approval_already_used" => {
            return Err(worker_create_error(
                StatusCode::CONFLICT,
                "approval_already_used",
            ));
        }
        Err(detail) => {
            eprintln!("warning: E2 worker run create failed: {detail}");
            return Err(worker_create_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "sqlite_unavailable",
            ));
        }
    };
    // E2-A §2.7: mirror the same-tx writeback into the
    // in-memory map (the HTTP read side in SQLite mode reads
    // SQLite, but the gate paths read the map). No persist
    // call: the SQLite row is already updated by the insert
    // transaction, and a persist would write a spurious
    // `approval.decided` event for a field-only change.
    if let (Some(approval_id), Some(worker_session_id)) =
        (&row.approval_id, &row.worker_session_id)
    {
        let mut approvals = state.approvals.lock().expect("approval lock");
        if let Some(approval) = approvals.get_mut(approval_id) {
            approval.consumed_by_worker_session_id = Some(worker_session_id.clone());
        }
    }
    tokio::spawn(crate::worker::drive_worker_run(
        state.clone(),
        row.run_id.clone(),
    ));
    Ok(row)
}

async fn worker_runs_create(
    State(state): State<AppState>,
    Json(req): Json<WorkerRunCreateDto>,
) -> impl IntoResponse {
    match create_worker_run_internal(&state, req).await {
        Ok(row) => (StatusCode::CREATED, Json(row)).into_response(),
        Err(err) => (
            err.status,
            Json(serde_json::json!({ "error": err.code })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct WorkerRunsListQuery {
    #[serde(default)]
    execution_id: Option<String>,
    #[serde(default)]
    status: Option<String>,
}

async fn worker_runs_list(
    State(state): State<AppState>,
    Query(query): Query<WorkerRunsListQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_worker_runs(
        &conn,
        query.execution_id.as_deref(),
        query.status.as_deref(),
    ) {
        Ok(rows) => Json(rows).into_response(),
        Err(detail) => {
            eprintln!("warning: E2 worker runs list failed: {detail}");
            sqlite_error_response()
        }
    }
}

async fn worker_run_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_worker_run(&conn, &id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown_worker_run" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: E2 worker run get failed: {detail}");
            sqlite_error_response()
        }
    }
}

async fn worker_run_cancel(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    let row = {
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::get_worker_run(&conn, &id)
    };
    let row = match row {
        Ok(Some(row)) => row,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "unknown_worker_run" })),
            )
                .into_response();
        }
        Err(detail) => {
            eprintln!("warning: E2 worker run cancel read failed: {detail}");
            return sqlite_error_response();
        }
    };
    if crate::worker::is_terminal_run_status(&row.status) {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "worker_run_terminal" })),
        )
            .into_response();
    }
    // Claim first, kill second: the driver finalizes the
    // run as `cancelled` when it sees the claim (or when
    // the kill wakes its wait). A queued run with no pid
    // yet is claimed the same way and never spawns.
    state.mark_worker_cancelled(&id);
    if let Some(pid) = row.pid {
        crate::worker::kill_process_group(pid, "TERM");
    }
    let row = {
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::get_worker_run(&conn, &id)
    };
    match row {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown_worker_run" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: E2 worker run cancel re-read failed: {detail}");
            sqlite_error_response()
        }
    }
}

// Phase 2 read-only Effect routes, mirroring /executions: the old
// read paths keep serving the old records, and these routes expose
// the converged view alongside. No existing response changes.
#[derive(Debug, Deserialize)]
struct EffectGetQuery {
    effect_id: String,
}

async fn effects_list(State(state): State<AppState>) -> impl IntoResponse {
    // P0-D: SQLite mode reads SQLite; a broken database fails
    // closed (5xx) instead of serving a stale map.
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = effects_snapshot(&state) else {
            return sqlite_error_response();
        };
        let mut records = records;
        records.sort_by_key(|r| effect_number(&r.effect_id));
        return Json(records).into_response();
    }
    let mut records: Vec<EffectFileRecord> =
        state.effects.lock().expect("effect lock").values().cloned().collect();
    records.sort_by_key(|r| effect_number(&r.effect_id));
    Json(records).into_response()
}

async fn effects_get(
    State(state): State<AppState>,
    Query(query): Query<EffectGetQuery>,
) -> impl IntoResponse {
    if state.sqlite_mode() == StateBackend::Sqlite {
        let Some(records) = effects_snapshot(&state) else {
            return sqlite_error_response();
        };
        return match records.into_iter().find(|r| r.effect_id == query.effect_id) {
            Some(record) => Json(record).into_response(),
            None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
                .into_response(),
        };
    }
    match state.effects.lock().expect("effect lock").get(&query.effect_id) {
        Some(record) => Json(record.clone()).into_response(),
        None => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
            .into_response(),
    }
}

// --- P0-C Effect lifecycle routes (gateway only, no client) ---

#[derive(Debug, Deserialize)]
struct EffectTransitionDto {
    effect_id: String,
    status: String,
    reconciliation_result: Option<String>,
    reconciliation_detail: Option<String>,
}

/// P0-C: drive one Effect along its lifecycle. Only contract
/// transitions are accepted (400 for a word outside the
/// vocabulary, 409 for an illegal edge or a legacy row, 404
/// for an unknown id). Driving to `reconciled` requires a
/// reconciliation result: a reconciled row without a conclusion
/// is refused.
async fn effects_transition(
    State(state): State<AppState>,
    Json(req): Json<EffectTransitionDto>,
) -> impl IntoResponse {
    if !crate::effect_store::is_new_effect_status(&req.status) {
        return bad_request("unknown effect status");
    }
    if req.status == "reconciled" && req.reconciliation_result.is_none() {
        return bad_request("reconciled requires reconciliation_result");
    }
    let reconciliation = req.reconciliation_result.map(|result| Reconciliation {
        result,
        checked_ms: crate::effect_store::now_ms(),
        detail: req.reconciliation_detail.clone(),
    });
    match transition_effect(&state, &req.effect_id, &req.status, reconciliation, None) {
        Ok(record) => Json(record).into_response(),
        Err(404) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
            .into_response(),
        Err(400) => bad_request("unknown effect status"),
        // P0-D: broken SQLite fails closed (5xx), never 409.
        Err(500) => sqlite_error_response(),
        Err(_) => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "effect_transition_refused" })),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct EffectReconcileDto {
    effect_id: String,
}

/// P0-R: re-check an `unknown` or `started` Effect. Device
/// Effects keep their action-based re-check; every other class
/// goes through the observation dispatch: a landed observation
/// reconciles with `completed`, a device-reminder-missing one
/// with `failed`, an inconclusive one returns the row to
/// `unknown`. Rows with no observation method answer 409
/// `effect_no_readback`; rows without the information an
/// observation needs answer 409 `evidence_missing`. A
/// conclusive re-check lands on `reconciled` with its
/// conclusion; an inconclusive one returns the Effect to
/// `unknown`. The backing action record is updated to match,
/// when one exists.
async fn effects_reconcile(
    State(state): State<AppState>,
    Json(req): Json<EffectReconcileDto>,
) -> impl IntoResponse {
    let Some(effect) = effect_by_id(&state, &req.effect_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
            .into_response();
    };
    match reconcile_effect(&state, &effect) {
        Ok(record) => {
            // Mirror the verdict onto the action record, as
            // actions_refresh does (device class only; other
            // classes have no backing action to update).
            let mut actions = state.actions.lock().expect("action lock");
            if let Some(current) = actions.values_mut().find(|a| {
                a.effect_id.as_deref() == Some(&record.effect_id)
                    || (record.execution_id.is_some() && a.execution_id == record.execution_id)
            }) {
                match record.reconciliation.as_ref().map(|r| r.result.as_str()) {
                    Some("completed") => {
                        current.status = "completed".to_string();
                        current.detail = None;
                    }
                    Some("failed") => {
                        current.status = "failed".to_string();
                        current.detail = record
                            .reconciliation
                            .as_ref()
                            .and_then(|r| r.detail.clone());
                    }
                    _ => {}
                }
                let updated = current.clone();
                drop(actions);
                persist_action(&state, &updated);
            }
            Json(record).into_response()
        }
        Err(error) => {
            // Preserve the P0-C shape for the status gate:
            // `effect_not_unknown` carries the row's status.
            let mut body = serde_json::json!({ "error": error });
            if error == "effect_not_unknown" {
                body["status"] = serde_json::json!(effect.status);
            }
            (StatusCode::CONFLICT, Json(body)).into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
struct EffectVerifyDto {
    effect_id: String,
}

/// P0-R: verify a `succeeded` Effect against the world, without
/// consuming any approval, writing any idempotency key, or
/// changing the Effect row. The response is the new Evidence
/// row. Non-`succeeded` rows answer 409 `effect_not_verifiable`
/// (with the row's status); rows without the information an
/// observation needs answer 409 `evidence_missing`; rows with
/// no observation method answer 409 `effect_no_readback`.
/// A `mismatch` verdict opens a `verification_mismatch` Inbox
/// row (deduped); a later `verified` verdict auto-closes it.
/// A mismatch never enqueues Outbox. Evidence lives only in
/// SQLite: without a configured database this answers 500.
async fn effects_verify(
    State(state): State<AppState>,
    Json(req): Json<EffectVerifyDto>,
) -> impl IntoResponse {
    let Some(effect) = effect_by_id(&state, &req.effect_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown effect" })))
            .into_response();
    };
    if effect.status != "succeeded" {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "effect_not_verifiable", "status": effect.status })),
        )
            .into_response();
    }
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    // The backing action carries the target flag the Effect
    // row does not — when one exists. A device row without
    // one (loop writes have none) falls through to the
    // generic path below (E2-C unification), where the
    // target is recovered from the row's own digest; it must
    // not 409 here, or unification would be a regression
    // against the loop rows' old generic verify.
    let action_backed = if matches!(effect.capability.as_str(), "device" | "devices") {
        let actions = state.actions.lock().expect("action lock");
        actions
            .values()
            .find(|a| {
                a.effect_id.as_deref() == Some(&effect.effect_id)
                    || (effect.execution_id.is_some()
                        && a.execution_id == effect.execution_id)
            })
            .cloned()
    } else {
        None
    };
    let outcome = match action_backed {
        Some(action) => crate::evidence::device_verify_outcome(
            &state,
            action.target_completed,
            &action.reminder_id,
        ),
        None => {
            let write_evidence = latest_evidence_for_effect(&state, &effect);
            match crate::evidence::verify_outcome(&state, &effect, write_evidence.as_ref()) {
                Ok(outcome) => outcome,
                Err(crate::evidence::ObserveError::NoMethod) => {
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({ "error": "effect_no_readback" })),
                    )
                        .into_response();
                }
                Err(crate::evidence::ObserveError::MissingEvidence) => {
                    return (
                        StatusCode::CONFLICT,
                        Json(serde_json::json!({ "error": "evidence_missing" })),
                    )
                        .into_response();
                }
            }
        }
    };
    let (verdict, reason, observed) = match outcome {
        crate::evidence::VerifyOutcome::Verified { observed, reason } => {
            ("verified", reason, observed)
        }
        crate::evidence::VerifyOutcome::Mismatch { observed, reason } => {
            ("mismatch", reason, observed)
        }
        crate::evidence::VerifyOutcome::Inconclusive { observed, reason } => {
            ("inconclusive", reason, observed)
        }
    };
    let draft = crate::evidence::OwnedEvidence {
        effect_id: effect.effect_id.clone(),
        execution_id: effect.execution_id.clone(),
        source: "verify".to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        observed_json: serde_json::to_string(&observed).unwrap_or_else(|_| "{}".to_string()),
        checked_ms: crate::effect_store::now_ms() as i64,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::record_verification(&mut conn, &draft.as_draft()) {
        Ok(row) => {
            // E2-C §4.5: a verify writes Evidence too — the
            // export regenerates here as well.
            export_evidence_best_effort(&state, &conn);
            Json(row).into_response()
        }
        Err(_) => sqlite_error_response(),
    }
}

// --- P3-B patches (plan §4.7) ---
//
// Patches exist only in SQLite (plan §4.6): the routes
// answer 500 `sqlite_not_configured` without a configured
// SQLite database, and 500 `sqlite_unavailable` on a broken
// one — the same fail-closed gate as the Evidence routes.
// List/get read; apply/reject mutate and are SQLite-mode
// only (in Shadow mode there is no Patch store to mutate).

#[derive(Debug, Deserialize)]
struct PatchListQuery {
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PatchApplyRequest {
    #[serde(default)]
    expected_old_sha256: Option<String>,
    approval_id: String,
}

async fn patches_list(
    State(state): State<AppState>,
    Query(query): Query<PatchListQuery>,
) -> impl IntoResponse {
    if let Some(status) = &query.status {
        if !crate::patch_store::PATCH_STATUSES.contains(&status.as_str()) {
            return bad_request("unknown patch status");
        }
    }
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_patches(&conn) {
        Ok(rows) => {
            let rows: Vec<_> = match &query.status {
                Some(status) => rows
                    .into_iter()
                    .filter(|r| &r.status == status)
                    .collect(),
                None => rows,
            };
            Json(rows).into_response()
        }
        Err(_) => sqlite_error_response(),
    }
}

async fn patches_get(
    State(state): State<AppState>,
    axum::extract::Path(patch_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    let patch = match sqlite_store::get_patch(&conn, &patch_id) {
        Ok(Some(patch)) => patch,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown patch" })))
                .into_response()
        }
        Err(_) => return sqlite_error_response(),
    };
    let files = match sqlite_store::list_patch_files(&conn, &patch_id) {
        Ok(files) => files,
        Err(_) => return sqlite_error_response(),
    };
    Json(serde_json::json!({ "patch": patch, "files": files })).into_response()
}

async fn patches_apply(
    State(state): State<AppState>,
    axum::extract::Path((patch_id, patch_file_id)): axum::extract::Path<(String, String)>,
    Json(req): Json<PatchApplyRequest>,
) -> impl IntoResponse {
    if req.approval_id.trim().is_empty() {
        return bad_request("approval_id is required");
    }
    match apply_patch_file_locked(
        &state,
        &patch_id,
        &patch_file_id,
        req.expected_old_sha256.as_deref(),
        &req.approval_id,
    ) {
        Ok((patch_file, effect, replayed)) => {
            // E2-C §4.5: a fresh apply writes Evidence in-tx
            // (the export regenerates from SQLite, so on a
            // replay this is a no-op rewrite of the same rows).
            if let Some(sqlite) = &state.sqlite {
                if let Ok(conn) = sqlite.lock() {
                    export_evidence_best_effort(&state, &conn);
                }
            }
            Json(serde_json::json!({
                "patch_file": patch_file,
                "effect": effect,
                "replayed": replayed,
            }))
            .into_response()
        }
        Err(response) => *response,
    }
}

async fn patches_reject(
    State(state): State<AppState>,
    axum::extract::Path(patch_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_mode() != StateBackend::Sqlite {
        return sqlite_not_configured();
    }
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let patch = {
        let conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::get_patch(&conn, &patch_id) {
            Ok(Some(patch)) => patch,
            Ok(None) => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "unknown patch" })),
                )
                    .into_response()
            }
            Err(_) => return sqlite_error_response(),
        }
    };
    // Already rejected -> 200 idempotent (the get shape).
    if patch.status == "rejected" {
        let conn = sqlite.lock().expect("sqlite lock");
        let files = sqlite_store::list_patch_files(&conn, &patch_id).unwrap_or_default();
        return Json(serde_json::json!({ "patch": patch, "files": files })).into_response();
    }
    let mut conn = sqlite.lock().expect("sqlite lock");
    let tx = match conn.transaction() {
        Ok(tx) => tx,
        Err(_) => return sqlite_error_response(),
    };
    let patch = match sqlite_store::reject_patch_tx(&tx, &patch_id, now_ms() as i64) {
        Ok(patch) => patch,
        Err(code) if code == "patch_has_applied_files" => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "patch_has_applied_files" })),
            )
                .into_response()
        }
        Err(code) if code == "patch_not_found" => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "unknown patch" })),
            )
                .into_response()
        }
        Err(_) => return sqlite_error_response(),
    };
    if tx.commit().is_err() {
        return sqlite_error_response();
    }
    let files = sqlite_store::list_patch_files(&conn, &patch_id).unwrap_or_default();
    Json(serde_json::json!({ "patch": patch, "files": files })).into_response()
}

#[derive(Debug, Deserialize)]
struct EvidenceIdQuery {
    evidence_id: String,
}

/// P0-R: list Evidence rows (bare JSON array, seq order, like
/// `/effects/list`). Same SQLite gate as the Inbox routes.
async fn evidence_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::list_evidence(&conn) {
        Ok(rows) => Json(rows).into_response(),
        Err(_) => sqlite_error_response(),
    }
}

/// P0-R: get one Evidence row by id, or 404 `unknown evidence`.
async fn evidence_get(
    State(state): State<AppState>,
    Query(query): Query<EvidenceIdQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::get_evidence(&conn, &query.evidence_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown evidence" })),
        )
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

/// E2-C §4.5: return the Evidence audit export as JSONL — one
/// line per Evidence row, the same rows `evidence_list`
/// returns, generated from SQLite on every call (the single
/// source of truth; the audit FILE written at Evidence write
/// points holds the same content). Same guards as the list/get
/// reads: 500 without a configured SQLite database, 500 on a
/// broken one. Generating live — rather than serving the file —
/// keeps the export consistent with list/get even where the
/// audit file is not wired (the packaged app does not set
/// MOCK_EVIDENCE_PATH yet).
async fn evidence_export(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let conn = sqlite.lock().expect("sqlite lock");
    match crate::sqlite_store::list_evidence(&conn) {
        Ok(rows) => {
            let body = rows
                .iter()
                .filter_map(|r| serde_json::to_string(r).ok())
                .collect::<Vec<_>>()
                .join("\n");
            let body = if body.is_empty() {
                body
            } else {
                format!("{body}\n")
            };
            (
                StatusCode::OK,
                [("content-type", "application/jsonl")],
                body,
            )
                .into_response()
        }
        Err(_) => sqlite_error_response(),
    }
}

// --- P0-D Inbox / Outbox routes (gateway only, no client) ---
//
// Inbox first batch: approval_required, review_required,
// failed, unknown_requires_reconciliation. Rows are derived
// in the same SQLite transaction as the triggering transition
// (see sqlite_store); failed rows close only by user ack,
// the others close automatically on decision / reconciliation
// or by ack. Outbox first batch: local_notification only,
// pending -> sent | failed, no automatic retry, requeue is an
// internal function with no route. Without a configured
// SQLite database these routes answer 500 (the Inbox /
// Outbox live in SQLite); a broken database also answers 5xx.

#[derive(Debug, Deserialize)]
struct InboxListQuery {
    kind: Option<String>,
    open_only: Option<String>,
}

#[derive(Debug, Deserialize)]
struct InboxIdQuery {
    inbox_id: String,
}

#[derive(Debug, Deserialize)]
struct InboxAckDto {
    inbox_id: String,
}

#[derive(Debug, Deserialize)]
struct OutboxIdQuery {
    outbox_id: String,
}

fn sqlite_not_configured() -> axum::response::Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": "sqlite_not_configured" })),
    )
        .into_response()
}

async fn inbox_list(
    State(state): State<AppState>,
    Query(query): Query<InboxListQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    if state.sqlite_mode() != StateBackend::Sqlite && state.sqlite_mode() != StateBackend::Shadow {
        return sqlite_not_configured();
    }
    let open_only = query.open_only.as_deref() == Some("true") || query.open_only.as_deref() == Some("1");
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_inbox(&conn, query.kind.as_deref(), open_only) {
        Ok(rows) => Json(rows).into_response(),
        Err(_) => sqlite_error_response(),
    }
}

async fn inbox_get(
    State(state): State<AppState>,
    Query(query): Query<InboxIdQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_inbox(&conn, &query.inbox_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown inbox item" })))
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

async fn inbox_ack(
    State(state): State<AppState>,
    Json(req): Json<InboxAckDto>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::ack_inbox(&mut conn, &req.inbox_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown inbox item" })))
            .into_response(),
        Err(_) => sqlite_error_response(),
    }
}

async fn outbox_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_outbox(&conn) {
        Ok(rows) => Json(rows).into_response(),
        Err(_) => sqlite_error_response(),
    }
}

async fn outbox_get(
    State(state): State<AppState>,
    Query(query): Query<OutboxIdQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_outbox(&conn, &query.outbox_id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown outbox item" })))
            .into_response(),
        Err(_) => sqlite_error_response(),
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
    pinned: bool,
    archived: bool,
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
        pinned: false,
        archived: false,
        turns: Vec::new(),
    };
    state.sessions.lock().expect("session lock").insert(record.session_id.clone(), record.clone());
    persist_session(&state, &record);
    Json(record).into_response()
}

#[derive(Debug, Deserialize)]
struct SessionsListQuery {
    include_archived: Option<bool>,
}

fn session_summary(record: &SessionRecord) -> SessionSummary {
    SessionSummary {
        session_id: record.session_id.clone(),
        title: record.title.clone(),
        created_ms: record.created_ms,
        updated_ms: record.updated_ms,
        // Task 18: tombstoned turns are hidden everywhere, so
        // the count reports visible turns only.
        turn_count: record.turns.iter().filter(|turn| !turn.deleted).count(),
        pinned: record.pinned,
        archived: record.archived,
    }
}

async fn sessions_list(
    State(state): State<AppState>,
    Query(query): Query<SessionsListQuery>,
) -> Json<Vec<SessionSummary>> {
    let include_archived = query.include_archived.unwrap_or(false);
    let mut records: Vec<SessionSummary> = state
        .sessions
        .lock()
        .expect("session lock")
        .values()
        .filter(|r| !r.deleted && (include_archived || !r.archived))
        .map(session_summary)
        .collect();
    records.sort_by_key(|r| (!r.pinned, session_number(&r.session_id)));
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
            deleted: false,
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
        DeepseekOutcome::Denied(pattern) => {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "error": "network rule denied model call",
                    "rule": pattern,
                })),
            )
                .into_response();
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
        deleted: false,
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

#[derive(Debug, Deserialize)]
struct SessionIdDto {
    session_id: String,
}

#[derive(Debug, Deserialize)]
struct SessionRenameDto {
    session_id: String,
    title: String,
}

/// Shared mutation path for pin/unpin/archive/unarchive/rename:
/// unknown ids and tombstoned sessions both read as 404, matching
/// `sessions_get`/`sessions_delete`; the mutation bumps `updated_ms`
/// and appends a fresh snapshot, like deletion does.
fn mutate_session_flag(
    state: &AppState,
    session_id: &str,
    mutate: impl FnOnce(&mut SessionRecord),
) -> Response {
    let mut sessions = state.sessions.lock().expect("session lock");
    let Some(record) = sessions.get_mut(session_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    };
    if record.deleted {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown session" })))
            .into_response();
    }
    mutate(record);
    record.updated_ms = now_ms();
    let updated = record.clone();
    persist_session(state, &updated);
    Json(session_summary(&updated)).into_response()
}

async fn session_pin(State(state): State<AppState>, Json(req): Json<SessionIdDto>) -> impl IntoResponse {
    mutate_session_flag(&state, &req.session_id, |r| r.pinned = true)
}

async fn session_unpin(State(state): State<AppState>, Json(req): Json<SessionIdDto>) -> impl IntoResponse {
    mutate_session_flag(&state, &req.session_id, |r| r.pinned = false)
}

async fn session_archive(State(state): State<AppState>, Json(req): Json<SessionIdDto>) -> impl IntoResponse {
    mutate_session_flag(&state, &req.session_id, |r| r.archived = true)
}

async fn session_unarchive(State(state): State<AppState>, Json(req): Json<SessionIdDto>) -> impl IntoResponse {
    mutate_session_flag(&state, &req.session_id, |r| r.archived = false)
}

async fn session_rename(State(state): State<AppState>, Json(req): Json<SessionRenameDto>) -> impl IntoResponse {
    if req.title.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "title must not be empty" })))
            .into_response();
    }
    mutate_session_flag(&state, &req.session_id, |r| r.title = req.title.clone())
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

/// E2-D §6 (`worker.wait`): default wait budget, in ms.
/// The loop tool timeout (10 s) caps it from outside;
/// normalize rejects a larger `timeout_ms`.
const WORKER_WAIT_DEFAULT_TIMEOUT_MS: u64 = 10_000;
/// E2-D §6 (`worker.wait`): row-state poll interval. The
/// read is a cheap SQLite query; polling more often buys
/// nothing, polling less often risks missing a terminal
/// state inside a 10 s budget.
const WORKER_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

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
    // P0-B adapter: the attempt starts at creation, so the
    // Execution is created here and carried on the action record;
    // device_set attaches its Effect to it. The pin check above
    // already passed, so a rejected creation leaves no orphan.
    // (E2-C §4.2: the Execution is built inside `ledger_entry`
    // below, not here.)
    // P0-C: the action is planned here. Its Effect is created
    // as `planned` (contract triple, Q5 digest) and carried on
    // the action record; device_set drives it forward instead
    // of recording a fresh row. A never-executed action leaves
    // its planned row planned: recorded, never authorized.
    // E2-C §4.2: the plan step goes through the shared ledger
    // entry (the gate cannot fire on a fresh Execution, but
    // the decision is handled, not ignored).
    let digest = device_effect_digest(&req.reminder_id, req.target_completed);
    let (execution_id, planned) = match ledger_entry(
        &state,
        None,
        "device",
        "reminders.set_completed",
        &req.reminder_id,
        Some(digest),
        None,
    ) {
        LedgerEntryDecision::Entered(entry) => (entry.execution_id, entry.effect),
        LedgerEntryDecision::Replay(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_replay", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
        LedgerEntryDecision::Blocked(other) => {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
            )
                .into_response();
        }
    };
    let record = DeviceActionRecord {
        action_id: id,
        device: DEVICE_ID.to_string(),
        action: DEVICE_ACTION.to_string(),
        reminder_id: req.reminder_id,
        target_completed: req.target_completed,
        status: "requested".to_string(),
        detail: None,
        execution_id: Some(execution_id),
        effect_id: Some(planned.effect_id),
    };
    state.actions.lock().expect("action lock").insert(record.action_id.clone(), record.clone());
    persist_action(&state, &record);
    Json(record).into_response()
}

async fn device_set(
    State(state): State<AppState>,
    Json(req): Json<DeviceSetDto>,
) -> impl IntoResponse {
    // Phase 2 (Q4): peek first (approval errors keep their old
    // precedence over action errors, nothing burned), then bind
    // and consume only once the action is known executable — so a
    // failed set never burns the approval, and a lost consume race
    // still leaves the action requested for a fresh approval.
    if let Err(response) = check_device_approval(&state, &req.tool_call_id, &req.action_id) {
        return *response;
    }
    let (reminder_id, target, action_execution, action_effect) = {
        let actions = state.actions.lock().expect("action lock");
        let Some(record) = actions.get(&req.action_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
                .into_response();
        };
        if record.status != "requested" {
            return bad_request("action is not requested");
        }
        if let Some(response) = device_pin_error(&state, &record.reminder_id) {
            return response;
        }
        (
            record.reminder_id.clone(),
            record.target_completed,
            record.execution_id.clone(),
            record.effect_id.clone(),
        )
    };
    // P0-C: the action's own Effect gates the set. A terminal
    // Effect with a still-requested action is an inconsistent
    // state (e.g. operator-driven): fail closed, never execute.
    // An unsettled Effect refuses likewise. A different Effect
    // with the same idempotency key replays/blocks through the
    // same rule. Legacy actions without a planned Effect use
    // the key check alone. A planned Effect that is missing
    // entirely (the Effects store was not persisted across a
    // restart while actions were) falls back to the legacy
    // gate, like the file gate: the action binding and
    // single-use consumption still apply, and a fresh terminal
    // row is recorded below.
    let digest = device_effect_digest(&reminder_id, target);
    let mut action_effect_for_gate = action_effect.clone();
    if let Some(effect_id) = &action_effect {
        match effect_by_id(&state, effect_id).map(|e| e.status) {
            Some(status) if crate::effect_store::is_terminal_effect_status(&status) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_terminal", "effect_id": effect_id, "status": status })),
                )
                    .into_response();
            }
            Some(status) if status != "planned" && status != "authorized" => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_not_ready", "effect_id": effect_id, "status": status })),
                )
                    .into_response();
            }
            None => {
                action_effect_for_gate = None;
            }
            _ => {}
        }
    }
    // E2-A §2.4: the idempotency gate applies only when the
    // action carries an Execution. A pre-P0-B action has
    // none BY DESIGN (P0-B never fabricates a historical
    // association for it; locked by
    // `old_rows_read_unchanged_and_unattributed` in
    // p0b_adapters): its set proceeds and records a new row
    // with a null execution, as before — the gate is skipped,
    // not failed. The 409 `effect_idempotency_unavailable`
    // below is for the current shape, where the action DOES
    // carry an Execution but the digest is missing (a key
    // that is owed and absent, not one that never existed).
    if action_execution.is_some() {
        if idempotency_key_missing(action_execution.as_deref(), Some(&digest)) {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "effect_idempotency_unavailable" })),
            )
                .into_response();
        }
        match check_idempotency(
            &state,
            action_execution.as_deref(),
            "device",
            "reminders.set_completed",
            &reminder_id,
            Some(&digest),
            action_effect_for_gate.as_deref(),
        ) {
            IdempotencyDecision::Proceed => {}
            IdempotencyDecision::Replay(other) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_idempotency_replay", "effect_id": other.effect_id, "status": other.status })),
                )
                    .into_response();
            }
            IdempotencyDecision::Blocked(other) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "error": "effect_idempotency_blocked", "effect_id": other.effect_id, "status": other.status })),
                )
                    .into_response();
            }
            // P0-D: broken migrated-domain store fails closed.
            IdempotencyDecision::Unavailable => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": "sqlite_unavailable" })),
                )
                    .into_response();
            }
        }
    }
    if let Err(response) = consume_device_approval_for_effect(
        &state,
        &req.tool_call_id,
        &req.action_id,
        action_effect_for_gate.as_deref(),
        Some(&digest),
    ) {
        return *response;
    }
    // The planned device Effect was created before its approval
    // existed; stamp the authorizing approval onto it now.
    if let (Some(effect_id), Some(approval_id)) = (&action_effect_for_gate, &req.tool_call_id) {
        stamp_effect_approval(&state, effect_id, approval_id);
    }
    {
        let mut actions = state.actions.lock().expect("action lock");
        let Some(record) = actions.get_mut(&req.action_id) else {
            return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
                .into_response();
        };
        record.status = "approved".to_string();
        let approved = record.clone();
        persist_action(&state, &approved);
    }
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
    // P0-C: drive the action's planned Effect to its terminal
    // state (contract §4 mapping: completed -> succeeded,
    // failed -> failed, unknown -> unknown). awaiting_
    // authorization is terminal for the action but means
    // nothing was attempted: the Effect stays `authorized`
    // (approval passed, execution never started) rather than
    // taking a backwards transition to `planned` — the §4
    // `awaiting_authorization -> planned` row describes
    // single-row conversion, not a lifecycle transition, and
    // the action record itself carries the waiting detail.
    // Legacy actions without a planned Effect record one
    // terminal row, in the new word.
    let terminal = match updated.status.as_str() {
        "completed" => Some("succeeded"),
        "failed" => Some("failed"),
        "unknown" => Some("unknown"),
        _ => None,
    };
    if let Some(terminal) = terminal {
        let outcome = match terminal {
            "succeeded" => LedgerOutcome::Succeeded,
            "failed" => LedgerOutcome::Failed,
            _ => LedgerOutcome::Unknown,
        };
        // Drive the planned Effect when it exists in this run;
        // a fallback run (planned row lost across restart, or a
        // legacy action with no planned row) records a fresh
        // terminal row instead.
        let drive = updated
            .effect_id
            .as_deref()
            .filter(|id| effect_by_id(&state, id).is_some());
        match drive {
            Some(effect_id) => {
                // E2-C §4.2: the terminal transition goes through
                // the shared ledger finish step. The chain is
                // built from the row's current status, so the
                // authorized row takes started → terminal exactly
                // as before.
                let _ = ledger_entry_finish(&state, effect_id, outcome);
            }
            None => match &updated.execution_id {
                Some(execution_id) => {
                    // E2-C §4.2: the fallback row also goes
                    // through the shared ledger entry (plan +
                    // finish) on the action's own Execution. The
                    // gate cannot fire here — the planned row is
                    // gone and the Execution belongs to this
                    // action alone — and a Replay/Blocked answer
                    // would mean an identical row already covers
                    // the audit, so it is skipped, not recorded
                    // a second time.
                    if let LedgerEntryDecision::Entered(entry) = ledger_entry(
                        &state,
                        Some(execution_id.clone()),
                        "device",
                        "reminders.set_completed",
                        &updated.reminder_id,
                        Some(digest.clone()),
                        req.tool_call_id.clone(),
                    ) {
                        let _ =
                            ledger_entry_finish(&state, &entry.effect.effect_id, outcome);
                    }
                }
                None => {
                    // Legacy action with no Execution: the direct
                    // terminal record stays, as before (E2-A:
                    // legacy device_set actions are exempt from
                    // the gate).
                    record_effect(
                        &state,
                        NewEffect {
                            execution_id: updated.execution_id.clone(),
                            capability: "device".to_string(),
                            operation: "reminders.set_completed".to_string(),
                            scope: updated.reminder_id.clone(),
                            input_digest: Some(digest.clone()),
                            approval_id: req.tool_call_id.clone(),
                            status: terminal.to_string(),
                        },
                    );
                }
            },
        }
    }
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
    // P0-C: when the action has an `unknown` Effect, the same
    // single re-check observation drives both the action and
    // its Effect (unknown -> reconciling -> reconciled, or back
    // to unknown on an inconclusive re-check). Legacy actions
    // without an Effect keep the old action-only behaviour.
    let effect = current
        .effect_id
        .as_deref()
        .and_then(|id| effect_by_id(&state, id))
        .filter(|e| e.status == "unknown");
    if let Some(effect) = &effect {
        let _ = transition_effect(&state, &effect.effect_id, "reconciling", None, None);
    }
    let outcome =
        resolve_unknown(&state.device_backend, &current.reminder_id, current.target_completed);
    let mut actions = state.actions.lock().expect("action lock");
    let Some(record) = actions.get_mut(&req.action_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown action" })))
            .into_response();
    };
    match &outcome {
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
    drop(actions);
    if let Some(effect) = &effect {
        // The action above already reflects this outcome; the
        // Effect transition must agree with it, and a failed
        // re-check never writes unknown as failed (see the
        // helper's hard constraint).
        let _ = reconcile_device_effect_with_outcome(&state, effect, outcome);
    }
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
    // P3-A: an explicitly bound worktree must exist and be
    // active; its binding freezes into the Execution snapshot.
    // A bad id is a caller error, before the loop starts.
    let binding = match &req.worktree_id {
        None => None,
        Some(worktree_id) => match resolve_worktree_binding(&state, worktree_id) {
            Ok(binding) => Some(binding),
            Err(detail) => return bad_request(detail),
        },
    };
    let session_id = req.session_id.clone();
    let goal_id = req.goal_id.clone();
    let result = state.loops.start(req);
    if let Ok(view) = &result {
        // P0 phase 1: the loop attempt gets its Execution shell record.
        create_execution_with_binding(
            &state,
            None,
            Some(view.record.loop_id.clone()),
            binding,
            session_id,
            goal_id,
        );
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
        if view.record.status.terminal() {
            // E2-A §2.3: the manual resume ended the loop;
            // close its Execution (the driver never runs for
            // a stopping view, so this route owns the close).
            close_execution_for_loop(&state, &view.record.loop_id, &view.record.status);
        } else if view.record.status != LoopStatus::AwaitingApproval {
            spawn_driver(&state, &view.record.loop_id);
        }
    }
    loop_response(&state, result)
}
async fn loop_cancel(
    State(state): State<AppState>,
    Json(req): Json<LoopIdDto>,
) -> axum::response::Response {
    let result = state.loops.cancel(&req.loop_id);
    // E2-A §2.3: a cancelled loop closes its Execution.
    if let Ok(view) = &result {
        if view.record.status.terminal() {
            close_execution_for_loop(&state, &view.record.loop_id, &view.record.status);
        }
    }
    loop_response(&state, result)
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
        .route("/api/chat/themes", get(chat_themes))
        .route("/api/chat/themes/set", post(chat_themes_set))
        .route("/chat/unread-count", get(chat_unread_count))
        .route("/chat/{thread_id}/seen", post(chat_mark_seen))
        .route("/api/chat/delete-turn", post(chat_delete_turn))
        .route("/chat/message", get(chat_message))
        .route("/chat/history-window", get(chat_history_window))
        .route("/chat/search", get(chat_search))
        .route("/chat/subscribe", post(chat_subscribe))
        .route("/chat/stream-events", post(chat_stream_events))
        .route("/chat/cancel", post(chat_cancel))
        .route("/chat/stream", post(chat_stream_open))
        .route("/chat/poke", post(chat_poke))
        .route("/api/chat/sxs/select", post(chat_sxs_select))
        .route("/api/voice/catalog", get(voice_catalog))
        .route("/api/voice/catalog/filters", get(voice_catalog_filters))
        .route("/api/voice/available-voices", get(voice_available_voices))
        .route("/api/voice/pipeline-profiles", get(voice_pipeline_profiles))
        .route("/api/voice/pipeline-profile", post(voice_pipeline_profile_select))
        .route("/api/voice/custom-voices", get(voice_custom_voices_list))
        .route(
            "/api/voice/custom-voices/{saved_voice_id}",
            patch(voice_custom_voice_rename),
        )
        .route(
            "/api/voice/custom-voices/{saved_voice_id}",
            delete(voice_custom_voice_delete),
        )
        .route("/api/voice/active-voice", get(voice_active_voice_get))
        .route("/api/voice/active-voice", post(voice_active_voice_save))
        .route("/api/voice/onboarding/reset", post(voice_onboarding_reset))
        .route("/api/email/mailbox", get(email_mailbox))
        .route("/api/email/mailbox/setup", get(email_mailbox_setup))
        .route("/api/email/threads", get(email_threads_list))
        .route("/api/email/threads/{thread_id}", get(email_thread_get))
        .route("/api/email/messages/{message_id}", get(email_message_get))
        .route(
            "/api/email/messages/{message_id}/attachments/{index}",
            get(email_attachment_get),
        )
        .route("/api/nodes/register", post(nodes_register))
        .route("/api/nodes/list", post(nodes_list))
        .route("/api/nodes/heartbeat", post(nodes_heartbeat))
        .route("/api/nodes/disconnect", post(nodes_disconnect))
        .route("/api/nodes/unpair", post(nodes_unpair))
        // P3-A: engineering records (16 routes).
        .route("/bindings/create", post(bindings_create))
        .route("/bindings/list", get(bindings_list))
        .route("/bindings/{id}", get(bindings_get))
        .route("/bindings/{id}/archive", post(bindings_archive))
        .route("/projects/create", post(projects_create))
        .route("/projects/list", get(projects_list))
        .route("/projects/{id}", get(projects_get))
        .route("/projects/{id}/archive", post(projects_archive))
        // D1-1: goals (4 routes).
        .route("/goals/create", post(goals_create))
        .route("/goals/list", get(goals_list))
        .route("/goals/{id}", get(goals_get))
        .route("/goals/{id}/archive", post(goals_archive))
        // D1-2: policy versions + mandates (3 routes).
        .route("/policies/versions", get(policies_versions_list))
        .route("/mandates", get(mandates_list))
        .route("/mandates/{id}", get(mandates_get))
        .route("/locations/create", post(locations_create))
        .route("/locations/list", get(locations_list))
        .route("/locations/{id}", get(locations_get))
        .route("/locations/{id}/archive", post(locations_archive))
        .route("/worktrees/create", post(worktrees_create))
        .route("/worktrees/list", get(worktrees_list))
        .route("/worktrees/{id}", get(worktrees_get))
        .route("/worktrees/{id}/archive", post(worktrees_archive))
        .route("/patches/list", get(patches_list))
        .route("/patches/{id}", get(patches_get))
        .route(
            "/patches/{patch_id}/files/{patch_file_id}/apply",
            post(patches_apply),
        )
        .route("/patches/{id}/reject", post(patches_reject))
        .route("/approval/check", post(approval_check))
        .route("/files/list", get(files_list))
        .route("/files/read", get(files_read))
        .route("/files/stat", get(files_stat))
        .route("/files/write", post(files_write))
        .route("/files/delete", post(files_delete))
        .route("/files/rename", post(files_rename))
        .route("/files/mkdir", post(files_mkdir))
        .route("/files/reads", post(files_reads))
        .route("/files/stats", post(files_stats))
        .route("/files/search", post(files_search))
        .route("/files/raw", get(files_raw))
        .route("/files/subscribe", post(files_subscribe))
        .route("/files/thumbnail", get(files_thumbnail))
        .route("/files/export", post(files_export))
        .route("/files/library", post(files_library))
        .route("/approval/request", post(approval_request))
        .route("/approval/decide", post(approval_decide))
        .route("/approvals", get(approvals_list))
        .route("/approvals/history", get(approvals_history))
        .route("/approvals/{approval_id}", get(approvals_get))
        .route("/approvals/{approval_id}/editable-message", get(approval_editable_message))
        .route("/approvals/{approval_id}/edit-message", post(approval_edit_message))
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
        .route("/connectors", get(connectors_list))
        .route("/connectors/{id}", get(connectors_get))
        .route("/connectors/{id}/accounts", get(connectors_accounts))
        .route("/connectors/{id}/accounts/link", post(connectors_accounts_link))
        .route("/connectors/{id}/accounts/{account_id}/unlink", post(connectors_accounts_unlink))
        .route("/connectors/{id}/connect/info", get(connectors_connect_info))
        .route("/connectors/{id}/connect", post(connectors_connect))
        .route("/connectors/{id}/disconnect", post(connectors_disconnect))
        .route("/v1/custom-connectors/register", post(vault_custom_register))
        .route("/v1/custom-connectors/list", post(vault_custom_list))
        .route("/v1/custom-connectors/delete", post(vault_custom_delete))
        .route("/v1/credentials/register", post(vault_credentials_register))
        .route("/v1/credentials/list", get(vault_credentials_list))
        .route("/v1/credentials/delete", post(vault_credentials_delete))
        .route("/v1/credentials/capture", post(vault_credentials_capture))
        .route("/v1/credentials/capture/external-provider/request", post(vault_capture_request))
        .route("/v1/credentials/capture/external-provider/fulfill", post(vault_capture_fulfill))
        .route("/v1/credentials/catalog", get(vault_catalog_list))
        .route("/v1/credentials/catalog/details", get(vault_catalog_details))
        .route("/v1/credentials/catalog/update", post(vault_catalog_update))
        .route("/v1/credentials/catalog/delete", post(vault_catalog_delete))
        .route("/permissions", get(policy_get))
        .route("/permissions/active", get(policy_active_get))
        .route("/permissions/active", patch(policy_active_update))
        .route("/permissions/connectors/{connector}", patch(policy_connector_update))
        .route("/permissions/connectors/{connector}", delete(policy_connector_reset))
        .route("/permissions/hitl", patch(policy_hitl_update))
        .route("/permissions/hitl/snooze", delete(policy_hitl_snooze_delete))
        .route("/permissions/mitm", patch(policy_mitm_update))
        .route("/permissions/network-rules", patch(policy_network_rules_update))
        .route("/permissions/reset", post(policy_reset))
        .route("/permissions/revoke", post(policy_revoke))
        .route("/permissions/scopes/{scope_type}/{scope_id}", get(policy_scope_get))
        .route("/permissions/scopes/{scope_type}/{scope_id}", patch(policy_scope_update))
        .route("/permissions/sni-rejection", patch(policy_sni_rejection_update))
        .route("/permissions/tproxy", patch(policy_tproxy_update))
        .route("/permissions/inventory", get(policy_inventory))
        .route("/permissions/settings", get(policy_settings_get))
        .route("/permissions/settings", patch(policy_settings_update))
        .route("/connectors/{id}/permissions", get(connectors_permissions))
        .route("/model", get(model_get))
        .route("/model", post(model_set))
        .route("/config", get(config_get))
        .route("/peers", get(peers_list))
        .route("/peers/{muse_id}", get(peers_get))
        .route("/peers/{muse_id}", patch(peers_update))
        .route("/peers/{muse_id}", delete(peers_disconnect))
        .route("/peers/invites", get(peers_invites_list))
        .route("/peers/invites", post(peers_invites_create))
        .route("/peers/invites/accept", post(peers_invites_accept))
        .route("/peers/invites/open", post(peers_invites_open))
        .route("/peers/invites/{invite_id}", delete(peers_invites_revoke))
        .route("/peers/inbox", get(peers_inbox_list))
        .route("/peers/inbox/decline", post(peers_inbox_decline))
        .route("/peers/inbox/deliver", post(peers_inbox_deliver))
        .route("/peers/requests", get(peers_requests_list))
        .route("/peers/status", get(peers_status))
        .route("/spaces/v2", get(spaces_list))
        .route("/spaces/v2/{slug}/save", post(spaces_save))
        .route("/spaces/v2/{slug}", delete(spaces_delete))
        .route("/api/feed", get(feed_list))
        .route("/api/feed/infinite", get(feed_infinite))
        .route("/api/feed/status", get(feed_status))
        .route("/api/feed/units/{unit_id}", get(feed_unit_get))
        .route("/api/feed/units/{unit_id}", delete(feed_unit_delete))
        .route("/api/feed/units/{unit_id}/reaction", post(feed_unit_reaction))
        .route("/api/feed/units/{unit_id}/tap", post(feed_unit_tap))
        .route("/api/feed/units/seen", post(feed_units_seen))
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
        .route("/executions/list", get(executions_list))
        .route("/executions/get", get(executions_get))
        .route("/executions/transition", post(executions_transition))
        .route("/worker/runs", post(worker_runs_create))
        .route("/worker/runs/list", get(worker_runs_list))
        .route("/worker/runs/{id}", get(worker_run_get))
        .route("/worker/runs/{id}/cancel", post(worker_run_cancel))
        .route("/assignments/takeover", post(assignments_takeover))
        .route("/assignments/list", get(assignments_list))
        .route("/assignments/get", get(assignments_get))
        .route("/effects/list", get(effects_list))
        .route("/effects/get", get(effects_get))
        .route("/effects/transition", post(effects_transition))
        .route("/effects/reconcile", post(effects_reconcile))
        .route("/effects/verify", post(effects_verify))
        .route("/evidence/list", get(evidence_list))
        .route("/evidence/get", get(evidence_get))
        .route("/evidence/export", get(evidence_export))
        .route("/inbox/list", get(inbox_list))
        .route("/inbox/get", get(inbox_get))
        .route("/inbox/ack", post(inbox_ack))
        .route("/outbox/list", get(outbox_list))
        .route("/outbox/get", get(outbox_get))
        .route("/sessions/create", post(sessions_create))
        .route("/sessions/list", get(sessions_list))
        .route("/sessions/get", get(sessions_get))
        .route("/sessions/send", post(sessions_send))
        .route("/sessions/delete", post(sessions_delete))
        .route("/session/pin", post(session_pin))
        .route("/session/unpin", post(session_unpin))
        .route("/session/archive", post(session_archive))
        .route("/session/unarchive", post(session_unarchive))
        .route("/session/rename", post(session_rename))
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

/// E2-B §3.2: the background assignment sweeper. One task,
/// one pass every 10 s, forever; it spawns no commands and
/// records no Effect (see `AppState::sweep_expired_assignments`).
/// Started by `serve` at gateway startup, at the same level as
/// the recovery scan. The caller aborts the handle on shutdown;
/// a pass already in flight finishes under the SQLite lock.
pub fn spawn_assignment_sweeper(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;
            state.sweep_expired_assignments();
        }
    })
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
    // E2-B §3.2: the assignment sweeper runs for the life of
    // the server; abort it once the listener stops.
    let sweeper = spawn_assignment_sweeper(state.clone());
    let result = axum::serve(listener, app(state.clone()))
        .with_graceful_shutdown(shutdown)
        .await;
    sweeper.abort();
    // P0-E clean-shutdown hook: this is the ONE normal-exit
    // path (Ctrl-C graceful shutdown). The cleanup runs after
    // the listener stops, best-effort, and never changes the
    // serve result: a failed cleanup is logged, and the next
    // startup's recovery scan closes whatever it missed.
    // SIGTERM has no handler here (pre-existing: only Ctrl-C
    // is graceful), and kill -9 never reaches this by design.
    state.sqlite_shutdown_cleanup();
    result
}

#[cfg(test)]
mod p0e_tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mock-server-p0e-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    /// P0-E creation stamping, unit-level: the worker comes
    /// from the scan, the snapshot id tracks the execution
    /// number, and the instruction digest follows the driver
    /// (task prefix / loop system / adapter empty string).
    /// Pure JSONL mode (no SQLite) keeps both fields None.
    #[test]
    fn p0e_create_execution_stamps_worker_and_snapshot_per_driver() {
        let dir = temp_dir("create");
        let state = AppState::with_files_root_and_approvals(dir.join("files"), None)
            .with_sqlite_path(dir.join("gateway.sqlite3"));
        state.sqlite_import_and_switch().expect("switch");
        assert_eq!(state.current_worker_session().as_deref(), Some("worker-1"));

        let task_exec = create_execution(&state, Some("task-1".to_string()), None);
        let loop_exec = create_execution(&state, None, Some("loop-1".to_string()));
        let adapter_exec = create_execution(&state, None, None);
        assert_eq!(task_exec.worker_session_id.as_deref(), Some("worker-1"));
        assert_eq!(loop_exec.worker_session_id.as_deref(), Some("worker-1"));
        assert_eq!(adapter_exec.worker_session_id.as_deref(), Some("worker-1"));
        assert_eq!(
            task_exec.workspace_snapshot_id.as_deref(),
            Some("snapshot-1")
        );
        assert_eq!(
            loop_exec.workspace_snapshot_id.as_deref(),
            Some("snapshot-2")
        );
        assert_eq!(
            adapter_exec.workspace_snapshot_id.as_deref(),
            Some("snapshot-3")
        );
        let conn = state.sqlite.as_ref().unwrap().lock().expect("sqlite lock");
        let task_snap = sqlite_store::get_snapshot(&conn, "snapshot-1")
            .expect("snapshot-1")
            .expect("snapshot-1 row");
        assert_eq!(
            task_snap.instruction_digest,
            sqlite_store::sha256_hex(crate::task_runner::PROMPT_PREFIX.as_bytes())
        );
        let loop_snap = sqlite_store::get_snapshot(&conn, "snapshot-2")
            .expect("snapshot-2")
            .expect("snapshot-2 row");
        assert_eq!(
            loop_snap.instruction_digest,
            sqlite_store::sha256_hex(crate::deepseek_loop::SYSTEM.as_bytes())
        );
        let adapter_snap = sqlite_store::get_snapshot(&conn, "snapshot-3")
            .expect("snapshot-3")
            .expect("snapshot-3 row");
        assert_eq!(
            adapter_snap.instruction_digest,
            sqlite_store::sha256_hex(b"")
        );
        assert_ne!(
            task_snap.instruction_digest,
            loop_snap.instruction_digest
        );
        drop(conn);
        let _ = std::fs::remove_dir_all(&dir);

        // Pure JSONL mode: no worker, no snapshot, unchanged.
        let plain = AppState::with_files_root_and_approvals(dir.join("files"), None);
        let record = create_execution(&plain, Some("task-9".to_string()), None);
        assert_eq!(record.worker_session_id, None);
        assert_eq!(record.workspace_snapshot_id, None);
    }
}
