//! Axum HTTP transport for the Mock Gateway.
//!
//! This module is additive only: it exposes the pure-logic rules
//! already defined in `lib.rs` (seed history, stream sequences
//! 1..=4, dedup, cancel, approval gate) over HTTP. It must not
//! change any of those rules. If a rule needs to change, change
//! `lib.rs` and its unit tests first, then re-check this module.
//!
//! 聊天：无 key 时 Mock 回显；配置 key 后（env DEEPSEEK_API_KEY 或 Keychain deepseek）调用 DeepSeek deepseek-flash，失败返回 502。
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
use axum::{
    Json, Router,
    http::{header, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post},
};
use tower_http::cors::{AllowOrigin, CorsLayer};
use serde::{Deserialize, Serialize};

use crate::approval_store::{append_record, load_records, next_id_number, ApprovalFileRecord};
use crate::routes_common::{bad_request, now_ms};
use crate::routes_files::FileEntryDto;
use crate::routes_chat::StreamState;
use crate::routes_connectors::{MockNote, MockMessage};
use crate::loop_engine::{
    loop_tool_requires_approval, ApprovalBinding, LoopEngine, LoopError, LoopModel,
    LoopTools, Timeouts, ToolOutcome, ErrorCode,
};
use crate::device_backend::{
    execute_set, resolve_unknown, ExecuteOutcome, FakeReminderBackend,
    Reminder, ReminderBackend, ResolveOutcome,
};
#[cfg(target_os = "macos")]
use crate::eventkit_backend::EventKitReminderBackend;
use crate::device_store::{
    append_action, load_actions, next_action_number, DeviceActionRecord,
};
use crate::task_store::{append_task, load_tasks, next_task_number, TaskFileRecord};
use crate::execution_store::{
    append_execution, load_executions, next_execution_number, ExecutionFileRecord,
};
use crate::effect_store::{
    append_effect, load_effects, next_effect_number, EffectFileRecord, NewEffect,
    Reconciliation,
};
use crate::sqlite_store::{self, StateBackend};
use rusqlite::Connection;
use crate::session_store::{load_sessions, next_session_number, SessionRecord};
use crate::connector_store::{
    load_connector_state, next_account_number, ConnectorAccount,
};
use crate::vault_store::{
    load_vault_state, next_ticket_number, CatalogEntry, CaptureTicket, CredentialSlot,
    CustomConnectorDef,
};
use crate::policy_store::{
    effective_policy, load_policy_state, ConnectorPolicy, EffectivePolicy, NetworkRule,
    PolicyDefaults, PolicyOverrides, ScopeRecord,
};
use crate::model_store::{load_model_state, StagedModel};
use crate::peer_store::{
    load_peer_state, Peer, PeerInboxMessage, PeerInvite, PeerRequest,
};
use crate::space_store::{load_space_state, Space};
use crate::feed_store::{load_feed_state, seed_feed_units, FeedUnit};
use crate::chat_store::{
    load_chat_state, DEFAULT_CHAT_THEME,
};
use crate::node_store::{
    load_nodes, next_node_number, Node,
};
use crate::binding_store::{
    append_binding, load_bindings, next_workspace_number, WorkspaceBinding,
};
use crate::project_store::{
    append_project, load_projects, next_project_number, Project,
};
use crate::location_store::{
    append_location, load_locations, next_location_number, Location, LOCATION_ACTIVE,
};
use crate::worktree_store::{
    append_worktree, load_worktrees, next_worktree_number, Worktree, WORKTREE_ACTIVE,
};
use crate::stream_store::{
    load_stream_state, next_poke_order, next_stream_number, next_subscription_number,
    StreamPoke, StreamSubscription, SxsSelection,
};
use crate::mail_source::{MailError, MailSource, UnavailableMailSource};
use crate::contacts_source::{ContactsSource, UnavailableContactsSource};
use crate::imessage_source::{IMessageError, IMessageSource, UnavailableIMessageSource};
use crate::notes_source::NotesSource;
use crate::calendar_backend::{default_calendar_backend, CalendarBackend};
use crate::voice_store::{load_voice_state, merged_custom_voices, CustomVoice};
use agent_desktop::credential_store::{CredentialStore, KeychainCredentialStore};
use crate::deepseek::{DeepSeekClient, DeepSeekConfig};
use crate::may_execute_tool;


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
    pub(crate) tool: String,
    pub(crate) status: ApprovalStatus,
    pub(crate) binding: Option<ApprovalBinding>,
    pub(crate) consumed: bool,
    pub(crate) operation_digest: Option<String>,
    /// Phase 2 (Q4): device action id this approval was consumed
    /// for, set at first device use. Never set on loop-bound or
    /// digest-bound records.
    pub(crate) action_binding: Option<String>,
    /// P0-C (contract §5): the Effect this approval authorizes,
    /// set when the Effect is planned (loop proposal, device
    /// action creation, digest-bound approval request) or at
    /// first consume for approvals planned elsewhere (task
    /// execute). The binding is `effect_id + input_digest`:
    /// the gate checks both, never the execution device and
    /// never the action record alone. None for legacy approvals
    /// issued before P0-C, which keep their old gate.
    pub(crate) effect_binding: Option<String>,
    pub(crate) message: Option<String>,
    /// P3-C (§7): the resolved Worker command this approval
    /// authorizes. None for every other tool.
    pub(crate) command: Option<CommandSpec>,
    /// E2-A §2.7: the worker session that consumed this
    /// approval to create a run. None for old rows and for
    /// approvals never used to create a run.
    pub(crate) consumed_by_worker_session_id: Option<String>,
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

    pub(crate) fn is_bound(&self) -> bool {
        self.binding.is_some() || self.operation_digest.is_some() || self.effect_binding.is_some()
    }
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
    pub(crate) streams: Arc<Mutex<HashMap<String, StreamState>>>,
    pub(crate) files_root: PathBuf,
    pub(crate) approvals: Arc<Mutex<HashMap<String, ApprovalState>>>,
    pub(crate) next_approval_id: Arc<Mutex<u64>>,
    pub(crate) approvals_path: Option<PathBuf>,
    pub(crate) tasks: Arc<Mutex<HashMap<String, TaskFileRecord>>>,
    pub(crate) next_task_id: Arc<Mutex<u64>>,
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
    pub(crate) sqlite_broken: Arc<Mutex<bool>>,
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
    /// ORC-2: Executor worker steps currently being driven
    /// (task_id, step_no). A double `tasks_execute` for the
    /// same read step must not create two worker runs — the
    /// approval-consumption guard that protects file steps
    /// does not exist for reads (no approval), so the
    /// in-memory claim is the guard. Held only for the
    /// create + wait; released on every exit path. A crash
    /// mid-step leaves a stale claim until restart (same
    /// class as the other in-memory crash windows).
    pub(crate) executor_step_claims: Arc<Mutex<std::collections::HashSet<(String, i64)>>>,
    pub(crate) task_drafts: Arc<Mutex<HashMap<String, String>>>,
    pub(crate) task_drafts_dir: Option<PathBuf>,
    pub(crate) sessions: Arc<Mutex<HashMap<String, SessionRecord>>>,
    pub(crate) next_session_id: Arc<Mutex<u64>>,
    pub(crate) sessions_path: Option<PathBuf>,
    pub(crate) actions: Arc<Mutex<HashMap<String, DeviceActionRecord>>>,
    pub(crate) next_action_id: Arc<Mutex<u64>>,
    pub(crate) actions_path: Option<PathBuf>,
    pub(crate) device_backend: Arc<dyn ReminderBackend>,
    pub(crate) device_reminder_pin: Option<String>,
    pub(crate) notes: Arc<Mutex<Vec<MockNote>>>,
    pub(crate) messages: Arc<Mutex<Vec<MockMessage>>>,
    pub(crate) next_note_id: Arc<Mutex<u64>>,
    pub(crate) next_message_id: Arc<Mutex<u64>>,
    pub(crate) connector_accounts: Arc<Mutex<HashMap<String, ConnectorAccount>>>,
    pub(crate) next_account_id: Arc<Mutex<u64>>,
    pub(crate) connector_connections: Arc<Mutex<HashMap<String, bool>>>,
    pub(crate) connector_state_path: Option<PathBuf>,
    pub(crate) vault_slots: Arc<Mutex<HashMap<String, CredentialSlot>>>,
    pub(crate) vault_catalog: Arc<Mutex<HashMap<String, CatalogEntry>>>,
    pub(crate) vault_custom: Arc<Mutex<HashMap<String, CustomConnectorDef>>>,
    pub(crate) vault_tickets: Arc<Mutex<HashMap<String, CaptureTicket>>>,
    pub(crate) next_ticket_id: Arc<Mutex<u64>>,
    pub(crate) vault_state_path: Option<PathBuf>,
    pub(crate) vault_secrets: Arc<dyn CredentialStore + Send + Sync>,
    pub(crate) policy_defaults: Arc<Mutex<Option<PolicyDefaults>>>,
    pub(crate) policy_overrides: Arc<Mutex<Option<PolicyOverrides>>>,
    pub(crate) policy_connectors: Arc<Mutex<HashMap<String, ConnectorPolicy>>>,
    pub(crate) policy_rules: Arc<Mutex<Option<Vec<NetworkRule>>>>,
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
    pub(crate) policy_scopes: Arc<Mutex<HashMap<String, ScopeRecord>>>,
    pub(crate) policy_state_path: Option<PathBuf>,
    pub(crate) model_staged: Arc<Mutex<Option<StagedModel>>>,
    pub(crate) model_state_path: Option<PathBuf>,
    pub(crate) peer_peers: Arc<Mutex<HashMap<String, Peer>>>,
    pub(crate) peer_invites: Arc<Mutex<HashMap<String, PeerInvite>>>,
    pub(crate) peer_inbox: Arc<Mutex<HashMap<String, PeerInboxMessage>>>,
    pub(crate) peer_requests: Arc<Mutex<HashMap<String, PeerRequest>>>,
    pub(crate) peer_state_path: Option<PathBuf>,
    pub(crate) spaces: Arc<Mutex<HashMap<String, Space>>>,
    pub(crate) space_state_path: Option<PathBuf>,
    pub(crate) feed_units: Arc<Mutex<HashMap<String, FeedUnit>>>,
    pub(crate) feed_state_path: Option<PathBuf>,
    pub(crate) chat_seen: Arc<Mutex<HashMap<String, usize>>>,
    pub(crate) chat_theme: Arc<Mutex<String>>,
    pub(crate) chat_state_path: Option<PathBuf>,
    pub(crate) nodes: Arc<Mutex<HashMap<String, Node>>>,
    pub(crate) next_node_id: Arc<Mutex<u64>>,
    pub(crate) node_state_path: Option<PathBuf>,
    pub(crate) bindings: Arc<Mutex<HashMap<String, WorkspaceBinding>>>,
    pub(crate) next_workspace_id: Arc<Mutex<u64>>,
    binding_state_path: Option<PathBuf>,
    pub(crate) projects: Arc<Mutex<HashMap<String, Project>>>,
    pub(crate) next_project_id: Arc<Mutex<u64>>,
    project_state_path: Option<PathBuf>,
    // D1-1: goals live in SQLite only (no JSONL mirror, no
    // in-memory map); the counter is the id allocator,
    // seeded from the goals table on restart.
    pub(crate) next_goal_id: Arc<Mutex<u64>>,
    // D1-2: mandates live in SQLite only (same shape as
    // goals); the counter is the id allocator, seeded from
    // the mandates table on restart.
    next_mandate_id: Arc<Mutex<u64>>,
    // D1-5: memories live in SQLite only (same shape as
    // goals/mandates); the counter is the id allocator,
    // seeded from both memory tables on restart. One
    // global `memory-N` sequence across the two tables.
    pub(crate) next_memory_id: Arc<Mutex<u64>>,
    // D1-4: the Source Registry read cache (the SQLite
    // `sources` table is the fact source; this map is
    // rebuilt from it at startup). Empty until a SQLite
    // path is opened and seeded.
    pub(crate) source_registry: crate::source_registry::SourceRegistry,
    // D1-4: the last tree `files_subscribe` observed per
    // subscribed path (in-memory diff cache only — the
    // durable record is the `file.changed` events +
    // the `files-watcher` cursor in SQLite). A restart
    // clears it, so the first post-restart subscribe
    // re-establishes the baseline instead of replaying.
    pub(crate) files_last_tree: Arc<Mutex<HashMap<String, Vec<FileEntryDto>>>>,
    pub(crate) locations: Arc<Mutex<HashMap<String, Location>>>,
    pub(crate) next_location_id: Arc<Mutex<u64>>,
    location_state_path: Option<PathBuf>,
    pub(crate) worktrees: Arc<Mutex<HashMap<String, Worktree>>>,
    pub(crate) next_worktree_id: Arc<Mutex<u64>>,
    worktree_state_path: Option<PathBuf>,
    pub(crate) stream_subscriptions: Arc<Mutex<HashMap<String, StreamSubscription>>>,
    pub(crate) next_subscription_id: Arc<Mutex<u64>>,
    pub(crate) next_stream_id: Arc<Mutex<u64>>,
    pub(crate) stream_pokes: Arc<Mutex<Vec<StreamPoke>>>,
    pub(crate) next_poke_order: Arc<Mutex<u64>>,
    pub(crate) stream_selections: Arc<Mutex<HashMap<String, SxsSelection>>>,
    pub(crate) stream_state_path: Option<PathBuf>,
    pub(crate) voice_custom: Arc<Mutex<HashMap<String, CustomVoice>>>,
    pub(crate) voice_active: Arc<Mutex<Option<String>>>,
    pub(crate) voice_pipeline: Arc<Mutex<Option<String>>>,
    pub(crate) voice_state_path: Option<PathBuf>,
    /// D1-6b-3: `pub(crate)` like `calendar_backend`
    /// below — the Evidence readback in `evidence.rs`
    /// needs the mail source for the delete re-read.
    pub(crate) mail_source: Arc<dyn MailSource>,
    /// MCL-1: the iMessage source (AppleScript
    /// Messages.app on macOS, the 501 stub elsewhere).
    /// Reads go through this trait so tests can
    /// substitute a fake; the loop tool and the sync
    /// route both read through it.
    pub(crate) imessage_source: Arc<dyn IMessageSource>,
    /// MCL-2: the contacts source (AppleScript
    /// Contacts.app on macOS, the 501 stub elsewhere).
    /// Reads go through this trait so tests can
    /// substitute a fake; the sync route reads through
    /// it (the loop tool reads SQLite, not the source).
    pub(crate) contacts_source: Arc<dyn ContactsSource>,
    /// MCL-3 shell route (S-1, Q-S-02): the startup
    /// token the shell hands the sidecar at launch
    /// (`MOCK_LOCATION_SHELL_TOKEN` env). The shell
    /// must present it as `X-Shell-Token` on
    /// `POST /api/location/record-from-shell`; without
    /// it no fix is ever recorded. Memory only: never
    /// persisted, never logged, rotated on every
    /// gateway start. `None` (tests, or a gateway
    /// started without the token) means the route
    /// always rejects.
    pub(crate) location_shell_token: Option<String>,
    /// D1-6b-2: the calendar backend (EventKit on macOS,
    /// the 501 stub elsewhere). Reads delegate to the
    /// D1-6a free functions; writes go through this
    /// trait so tests can substitute a fake.
    pub(crate) calendar_backend: Arc<dyn CalendarBackend>,
    /// D1-6b-4: the notes source (AppleScript Notes.app
    /// on macOS, the 501 stub elsewhere). Reads and
    /// writes go through this trait so tests can
    /// substitute a fake.
    pub(crate) notes_source: Arc<dyn NotesSource>,
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
        // D1-2 (Q-D1-21): the JSONL policy load must run
        // BEFORE the SQLite path is opened. The v6
        // migration's one-time import reads
        // `current_policy_document` from the maps (so it
        // needs the JSONL-loaded document), and
        // `apply_policy_versions` runs last inside
        // with_sqlite_path / sqlite_import_and_switch so the
        // versions table — not the JSONL file — is the fact
        // source after v6. Loading the JSONL after SQLite
        // overwrote the restored maps with the stale default
        // on every restart (window: updated_ms reverted to
        // the default-doc time).
        if let Ok(policy_state_path) = std::env::var("MOCK_POLICY_STATE_PATH") {
            state = state.with_policy_state_path(PathBuf::from(policy_state_path));
        }
        // D1-4 (Q-D1-41): the connector JSONL load must run
        // BEFORE the SQLite path is opened, same as the
        // D1-2 policy load (F-1). The v8 seed reads the
        // linked accounts from the in-memory map, so
        // loading the JSONL after SQLite would seed a
        // registry with no account rows on first boot.
        if let Ok(connector_state_path) = std::env::var("MOCK_CONNECTOR_STATE_PATH") {
            state = state.with_connector_state_path(PathBuf::from(connector_state_path));
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
        if let Ok(vault_state_path) = std::env::var("MOCK_VAULT_STATE_PATH") {
            state = state.with_vault_state_path(PathBuf::from(vault_state_path));
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
        // MCL-1: on macOS the production iMessage source
        // asks Messages.app via AppleScript; every other
        // platform keeps the Unavailable source (501, never
        // fake data). Tests override via with_imessage_source.
        #[cfg(target_os = "macos")]
        {
            state = state.with_imessage_source(Arc::new(
                crate::imessage_source::AppleScriptIMessageSource::new(),
            ));
        }
        // MCL-2: on macOS the production contacts source
        // asks Contacts.app via AppleScript; every other
        // platform keeps the Unavailable source (501, never
        // fake data). Tests override via with_contacts_source.
        #[cfg(target_os = "macos")]
        {
            state = state.with_contacts_source(Arc::new(
                crate::contacts_source::AppleScriptContactsSource::new(),
            ));
        }
        if let Ok(pin) = std::env::var("MOCK_DEVICE_REMINDER_ID") {
            state = state.with_device_reminder_pin(pin);
        }
        // MCL-3 shell route (S-1, Q-S-02): the startup
        // token, read the same way as the reminder pin
        // above. Absent in tests (the test seam sets
        // it); absent in production only if the shell
        // never passed one — then the record-from-shell
        // route rejects every call (safe default).
        if let Ok(token) = std::env::var("MOCK_LOCATION_SHELL_TOKEN") {
            if !token.is_empty() {
                state = state.with_location_shell_token(token);
            }
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
            executor_step_claims: Arc::new(Mutex::new(std::collections::HashSet::new())),
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
            next_memory_id: Arc::new(Mutex::new(1)),
            source_registry: crate::source_registry::SourceRegistry::new(),
            files_last_tree: Arc::new(Mutex::new(HashMap::new())),
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
            imessage_source: Arc::new(UnavailableIMessageSource),
            contacts_source: Arc::new(UnavailableContactsSource),
            location_shell_token: None,
            calendar_backend: default_calendar_backend(),
            notes_source: crate::notes_source::default_notes_source(),
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
        // MCL-1: read-only iMessage thread listing. The
        // email.threads arm is the model: the source is
        // AppState.imessage_source (the same source the
        // /api/imessage/* routes use), the call runs on a
        // blocking thread because AppleScript is
        // synchronous. Reads produce no Effect and need no
        // approval (registered Some(false)). The render
        // carries the thread's native id + service +
        // participant count / hash as the citation anchor;
        // there is no message content to render (message
        // history is not readable). Deliberately not added
        // to dispatch_agent_tool, like the mail tools.
        if tool == "imessage.threads" {
            let limit = args
                .get("limit")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(50);
            let offset = args
                .get("offset")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            let source = Arc::clone(&self.imessage_source);
            return match tokio::task::spawn_blocking(move || source.threads(limit, offset))
                .await
            {
                Ok(Ok(threads)) => {
                    let mut out = String::new();
                    for t in &threads {
                        out.push_str(&format!(
                            "thread: {} | service: {} | participants: {} | group: {} | participants_hash: {}\n",
                            t.native_id,
                            t.service,
                            t.participant_count,
                            t.is_group,
                            t.participant_hash,
                        ));
                    }
                    if out.is_empty() {
                        out.push_str("（信息暂无会话。）\n");
                    }
                    // `email_rendered` is the shared 4096-char
                    // truncation helper (the notice is generic);
                    // no mail-specific text is rendered here.
                    ToolOutcome::success(email_rendered(out))
                }
                Ok(Err(err)) => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: imessage_error_text(err),
                },
                Err(_) => ToolOutcome::unknown(),
            };
        }
        // MCL-2: read-only contacts search. Unlike the
        // imessage.threads arm, this reads the SQLite
        // table (not the live source): the search is a
        // fact-table query, so it works off a synced
        // snapshot and never touches Contacts.app. Reads
        // produce no Effect and need no approval
        // (registered Some(false)). The render carries
        // contact_id + display_name + organization +
        // phones + emails (plaintext per Q-MCL-11) plus
        // the has_address / has_birthday flags; address
        // and birthday VALUES are never rendered (they
        // are never stored). Deliberately not added to
        // dispatch_agent_tool, like the mail tools.
        if tool == "contacts.search" {
            let query = args.get("q").cloned().unwrap_or_default();
            let limit = args
                .get("limit")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(50);
            let offset = args
                .get("offset")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            let Some(sqlite) = &self.sqlite else {
                return ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: "contacts search unavailable: SQLite is not configured".to_string(),
                };
            };
            let conn = sqlite.lock().expect("sqlite lock");
            return match crate::contacts_sync::search_contacts(
                &conn,
                crate::contacts_sync::CONTACTS_SOURCE_ID,
                &query,
                limit,
                offset,
            ) {
                Ok(rows) => {
                    let mut out = String::new();
                    for row in &rows {
                        out.push_str(&format!(
                            "contact: {} | name: {} | organization: {} | phones: {} | emails: {} | has_address: {} | has_birthday: {}\n",
                            row.contact_id,
                            row.display_name,
                            row.organization,
                            row.phones.join(", "),
                            row.emails.join(", "),
                            row.addresses_hash.is_some(),
                            row.birthday_hash.is_some(),
                        ));
                    }
                    if out.is_empty() {
                        out.push_str("（通讯录暂无匹配联系人。）\n");
                    }
                    ToolOutcome::success(email_rendered(out))
                }
                Err(detail) => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: detail,
                },
            };
        }
        // MCL-3: the latest recorded sample, read from
        // SQLite (the contacts.search arm is the model).
        // Deliberately NOT a live fix: a loop call must
        // never trigger Core Location (Q-MCL-18, manual
        // trigger only), and the render carries the grid
        // hash — never raw coordinates (Q-MCL-17: exact
        // values are shown only locally in the packaged
        // app, never exported; the model is an export).
        // Reads produce no Effect and need no approval
        // (registered Some(false)). Deliberately not
        // added to dispatch_agent_tool, like the mail
        // tools.
        if tool == "location.current" {
            let Some(sqlite) = &self.sqlite else {
                return ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: "location current unavailable: SQLite is not configured".to_string(),
                };
            };
            let conn = sqlite.lock().expect("sqlite lock");
            return match crate::location_record::latest_device_location(
                &conn,
                crate::location_record::LOCATION_SOURCE_ID,
            ) {
                Ok(Some(row)) => {
                    let grid_hash = crate::location_source::grid_hash_for(
                        row.latitude,
                        row.longitude,
                    );
                    let out = format!(
                        "sample: {} | grid_hash: {} | locality_hash: {} | accuracy_meters: {} | recorded_ms: {}\n",
                        row.sample_id,
                        grid_hash,
                        row.locality_hash,
                        row.accuracy_meters,
                        row.recorded_ms,
                    );
                    ToolOutcome::success(email_rendered(out))
                }
                Ok(None) => {
                    ToolOutcome::success(email_rendered("（尚未记录当前位置。）\n".to_string()))
                }
                Err(detail) => ToolOutcome {
                    status: crate::loop_engine::CallStatus::Failed,
                    error: Some(ErrorCode::ToolFailed),
                    body: detail,
                },
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
                // E2-D batch 2: loop runs are immediate and
                // unprioritized — the loop has no deferred
                // start or priority vocabulary (no new loop
                // tool, plan §2.2).
                start_at_ms: None,
                priority: None,
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
        // R4-F2 (Q-R4F-01/02): loop-only read of one text
        // file under the resolved root — explicit
        // worktree_id wins, else the loop's binding, else
        // files_root (the same root code.search uses).
        // Read-only: no Effect, no approval. The arm sits
        // AFTER the backend seam above (R4F-J1): a custom
        // backend owns fs.read in tests, and production
        // runs with backend None. The dispatch_agent_tool
        // fs.read arm is untouched: the HTTP agent-tools
        // route keeps its files_root-only read.
        if tool == "fs.read" {
            let path = args.get("path").cloned().unwrap_or_default();
            if path.is_empty() {
                return ToolOutcome::failed(ErrorCode::InvalidArguments);
            }
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
            let outcome =
                tokio::task::spawn_blocking(move || read_loop_file(&root, &path)).await;
            return match outcome {
                Ok(Ok(text)) => ToolOutcome::success(text),
                Ok(Err(code)) => ToolOutcome::failed(code),
                Err(_) => ToolOutcome::unknown(),
            };
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
    /// MOCK_EVIDENCE_PATH, and the packaged-app shell
    /// (`gateway_home.rs`) wires it (tails follow-up to
    /// E2-C) so the file is written in the packaged app
    /// too; no behaviour is lost either way, because the
    /// route never reads the file.
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
                    if let Ok(next_memory) = crate::memory_store::next_memory_number(&conn) {
                        let mut current = self.next_memory_id.lock().expect("memory id lock");
                        *current = (*current).max(next_memory);
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
                    // D1-4 (Q-D1-41): seed the Source
                    // Registry from existing state (the
                    // v8 one-time import) and rebuild the
                    // read cache. First-write-wins, so a
                    // restart is a no-op for existing rows.
                    let seed_inputs = snapshot_seed_inputs(&self);
                    seed_sources_from(&self, &mut conn, &seed_inputs);
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

    /// D1-3: the Outbox deliverer (`outbox-deliverer`), run
    /// from the assignment sweeper's 10 s tick — no second
    /// scheduler. Drains unseen `outbox.enqueued` /
    /// `outbox.requeued` events through
    /// `consume_outbox_events` (cursor advance + delivery in
    /// one transaction). Returns the number of rows
    /// delivered on this tick.
    pub fn deliver_pending_outbox(&self) -> usize {
        if self.sqlite_mode() != StateBackend::Sqlite || self.sqlite_is_broken() {
            return 0;
        }
        let Some(sqlite) = &self.sqlite else {
            return 0;
        };
        let mut conn = sqlite.lock().expect("sqlite lock");
        match sqlite_store::consume_outbox_events(
            &mut conn,
            "outbox-deliverer",
            &self.origin_device_id,
        ) {
            Ok(rows) => rows.len(),
            Err(detail) => {
                eprintln!("warning: D1-3 outbox deliver failed: {detail}");
                0
            }
        }
    }

    /// D1-3 C-1: the timed-Outbox row scan, run from the
    /// assignment sweeper's 10 s tick — no second
    /// scheduler. Delivers due timed rows
    /// (`deliver_at_ms <= now`); the pending-only guard in
    /// `set_outbox_status` is the anti-double-delivery
    /// guard, so a row the event channel just delivered
    /// (or a manual deliver just sent) is a no-op here.
    /// A failed row scan (DB error) only warns and
    /// returns 0 — the next tick retries, and no row is
    /// marked failed. A single row's failure never aborts
    /// the pass. Returns the number of rows delivered.
    /// D1-3 C-2: the scan dispatches by
    /// `destination_kind` (the Q-C-11 dispatch table, as
    /// a match branch): `local_notification` → the log
    /// sink (unchanged); `imessage_send` → the internal
    /// scheduled-send execute (async — the AppleScript
    /// call can take up to 30 s, so this function is
    /// async and the sweeper awaits it); an unknown kind
    /// is warned + skipped, never fatal.
    pub async fn deliver_due_outbox(&self) -> usize {
        if self.sqlite_mode() != StateBackend::Sqlite || self.sqlite_is_broken() {
            return 0;
        }
        let Some(sqlite) = &self.sqlite else {
            return 0;
        };
        let due = {
            let conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::due_outbox_rows(&conn, sqlite_store::now_ms() as i64) {
                Ok(rows) => rows,
                Err(detail) => {
                    eprintln!("warning: D1-3 C-1 due outbox list failed: {detail}");
                    return 0;
                }
            }
        };
        let mut delivered = 0;
        for row in &due {
            match row.destination_kind.as_str() {
                "local_notification" => {
                    let mut conn = sqlite.lock().expect("sqlite lock");
                    match sqlite_store::deliver_outbox_via_log_sink(&mut conn, &row.outbox_id) {
                        Ok(Some(sent)) if sent.status == "sent" => delivered += 1,
                        Ok(_) => {}
                        Err(detail) => {
                            eprintln!(
                                "warning: D1-3 C-1 due outbox deliver failed for {}: {detail}",
                                row.outbox_id
                            );
                        }
                    }
                }
                "imessage_send" => {
                    if crate::routes_imessage::deliver_scheduled_imessage_send(self, row).await
                    {
                        delivered += 1;
                    }
                }
                other => {
                    eprintln!(
                        "warning: D1-3 C-2 due outbox row {} has unknown destination_kind {other}, skipped",
                        row.outbox_id
                    );
                }
            }
        }
        delivered
    }

    /// E2-D batch 2: start due deferred runs, one pass.
    /// Rides the assignment sweeper's 10 s tick (plan
    /// §2.2) — no second scheduler. Fetches due runs in
    /// `(priority DESC, seq ASC)` order and spawns the
    /// existing driver for each one that passes the
    /// per-Execution gate (plan §2.5/§8.3): an Execution
    /// runs at most one run at a time, so a candidate
    /// waits when its Execution is already busy — a
    /// running run, a queued immediate run about to be
    /// claimed, or an execution this pass already started.
    /// The gate reads the due set, not just the candidate:
    /// two due deferred runs for one Execution must not
    /// block each other (the first starts, the second
    /// waits for a later tick). The conditional claim
    /// (`mark_worker_run_running`, `WHERE status='queued'`)
    /// is the atomic guard against a double start; the
    /// gate check only decides which runs this pass starts.
    /// Claims are awaited one at a time (bounded, below),
    /// so the start order is the fetch order and the gate
    /// check for the next run sees the previous run
    /// already claimed. A failure for one run is logged
    /// and never aborts the pass. Returns how many runs
    /// were started. SQLite mode only.
    pub async fn start_due_worker_runs(&self) -> usize {
        if self.sqlite_mode() != StateBackend::Sqlite || self.sqlite_is_broken() {
            return 0;
        }
        let Some(sqlite) = &self.sqlite else {
            return 0;
        };
        let due = {
            let conn = sqlite.lock().expect("sqlite lock");
            match sqlite_store::due_deferred_worker_runs(&conn, sqlite_store::now_ms() as i64) {
                Ok(rows) => rows,
                Err(detail) => {
                    eprintln!("warning: E2-D deferred start list failed: {detail}");
                    return 0;
                }
            }
        };
        let due_ids: std::collections::HashSet<&str> =
            due.iter().map(|row| row.run_id.as_str()).collect();
        let mut started_executions: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut started = 0;
        for row in &due {
            if let Some(execution_id) = &row.execution_id {
                // Already started one for this Execution
                // this pass — the next waits for a later
                // tick (the first run's claim lands before
                // this check runs, so the DB check below
                // would block it too; the set keeps the
                // reason explicit).
                if started_executions.contains(execution_id) {
                    continue;
                }
                let blocked = {
                    let conn = sqlite.lock().expect("sqlite lock");
                    sqlite_store::active_run_ids_for_execution(&conn, execution_id)
                };
                match blocked {
                    Ok(active) if active.iter().any(|id| !due_ids.contains(id.as_str())) => {
                        // Genuinely busy: a running run, or
                        // a queued immediate run about to
                        // be claimed. Wait for a later tick.
                        continue;
                    }
                    Ok(_) => {}
                    Err(detail) => {
                        eprintln!(
                            "warning: E2-D deferred start gate check failed for {}: {detail}",
                            row.run_id
                        );
                        continue;
                    }
                }
                started_executions.insert(execution_id.clone());
            }
            tokio::spawn(crate::worker::drive_worker_run(
                self.clone(),
                row.run_id.clone(),
            ));
            started += 1;
            self.await_run_claim(&row.run_id).await;
        }
        started
    }

    /// E2-D batch 2: wait (bounded) for a run's claim to
    /// land — the row leaving `queued` — so the next due
    /// run starts after this one and the start order is
    /// the fetch order. A timeout just moves on (the
    /// claim lands or the driver exits on its own);
    /// correctness never depends on this wait — the
    /// conditional claim is the guard, this only orders
    /// starts.
    async fn await_run_claim(&self, run_id: &str) {
        let Some(sqlite) = &self.sqlite else {
            return;
        };
        let deadline = tokio::time::Instant::now() + Duration::from_millis(2000);
        loop {
            let claimed = {
                let conn = sqlite.lock().expect("sqlite lock");
                sqlite_store::get_worker_run(&conn, run_id)
                    .ok()
                    .flatten()
                    .map(|row| row.status != "queued")
                    .unwrap_or(true)
            };
            if claimed || tokio::time::Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
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
        // D1-4: snapshot the seed inputs BEFORE taking
        // the SQLite lock (D1-2 policy-import rule).
        let seed_inputs = snapshot_seed_inputs(self);
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
        // D1-4 (Q-D1-41): one-time Source Registry seed
        // from existing state (same pattern as the Policy
        // import above).
        seed_sources_from(self, &mut *conn, &seed_inputs);
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

    /// Test seam: replace the iMessage source (tests use
    /// the fake source; production macOS uses AppleScript,
    /// other platforms keep the Unavailable source).
    pub fn with_imessage_source(mut self, source: Arc<dyn IMessageSource>) -> Self {
        self.imessage_source = source;
        self
    }

    /// Test seam: replace the contacts source (tests use
    /// the fake source; production macOS uses AppleScript,
    /// other platforms keep the Unavailable source).
    pub fn with_contacts_source(mut self, source: Arc<dyn ContactsSource>) -> Self {
        self.contacts_source = source;
        self
    }

    /// Test seam: set the shell token the
    /// record-from-shell route accepts (tests use a
    /// fixed token; production reads the env var in
    /// `from_deepseek_config`).
    pub fn with_location_shell_token(mut self, token: String) -> Self {
        self.location_shell_token = Some(token);
        self
    }

    /// Test seam: replace the calendar backend (tests use
    /// the fake backend; production macOS uses EventKit,
    /// other platforms keep the Unavailable backend).
    pub fn with_calendar_backend(mut self, backend: Arc<dyn CalendarBackend>) -> Self {
        self.calendar_backend = backend;
        self
    }

    /// Test seam: replace the notes source (tests use
    /// the fake source; production macOS uses
    /// AppleScript, other platforms keep the Unavailable
    /// source).
    pub fn with_notes_source(mut self, source: Arc<dyn NotesSource>) -> Self {
        self.notes_source = source;
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

pub(crate) fn parse_approval_status(s: &str) -> Option<ApprovalStatus> {
    match s {
        "not_required" => Some(ApprovalStatus::NotRequired),
        "pending" => Some(ApprovalStatus::Pending),
        "approved" => Some(ApprovalStatus::Approved),
        "rejected" => Some(ApprovalStatus::Rejected),
        "edited" => Some(ApprovalStatus::Edited),
        _ => None,
    }
}



#[derive(Debug, Deserialize)]
pub(crate) struct ToolCallDto {
    pub(crate) tool: String,
    pub(crate) args: HashMap<String, String>,
    pub(crate) tool_call_id: Option<String>,
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
pub(crate) fn approval_status_name(status: ApprovalStatus) -> &'static str {
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
pub(crate) fn check_approval(
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
pub(crate) fn check_device_approval(
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
pub(crate) fn start_bound_effect(
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

pub(crate) fn dispatch_agent_tool(state: &AppState, req: ToolCallDto) -> axum::response::Response {
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

// --- Connector registry (Task 9) ---
//
// The registry is static on purpose: it lists exactly the two
// Mock connectors this gateway implements, nothing invented.
// Accounts and connection flags are local state (see
// connector_store.rs); there is no OAuth, no real account,
// and no external redirect anywhere in this surface. The
// notes/mail data routes above are untouched by it.

pub(crate) struct ConnectorEntry {
    pub(crate) id: &'static str,
    pub(crate) name: &'static str,
    pub(crate) capabilities: &'static [&'static str],
}

pub(crate) const CONNECTOR_REGISTRY: &[ConnectorEntry] = &[
    ConnectorEntry { id: "notes", name: "Mock Notes", capabilities: &["notes.list", "notes.create"] },
    ConnectorEntry { id: "mail", name: "Mock Mail", capabilities: &["mail.list", "mail.send"] },
];

pub(crate) fn registry_entry(id: &str) -> Option<&'static ConnectorEntry> {
    CONNECTOR_REGISTRY.iter().find(|e| e.id == id)
}

pub(crate) fn unknown_connector() -> Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown connector" }))).into_response()
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
pub(crate) struct PolicyDocument {
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
pub(crate) fn current_policy_document(state: &AppState) -> PolicyDocument {
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

// --- D1-4: Source Registry seed ---
//
// The v8 seed reads existing state (static connector
// entries, linked connector accounts, the mail source,
// the reminder backend, the files root) into the
// `sources` table — the D1-2 Policy one-time-import
// pattern. The seed inputs are snapshotted from the
// in-memory maps BEFORE taking the SQLite lock (the
// D1-2 rule: map locks first, never under the SQLite
// lock). A seed failure warns only: the registry is
// metadata, and a failed seed must never fail startup.

/// Snapshot the v8 seed inputs from AppState. Call
/// before taking the SQLite lock.
fn snapshot_seed_inputs(state: &AppState) -> crate::source_registry::SeedInputs {
    let accounts = state
        .connector_accounts
        .lock()
        .expect("connector accounts lock")
        .values()
        .cloned()
        .collect();
    crate::source_registry::SeedInputs {
        accounts,
        files_root: state.files_root.clone(),
        origin_device_id: state.origin_device_id.clone(),
        device_backend: if std::env::var("MOCK_DEVICE_BACKEND").as_deref() == Ok("eventkit") {
            "eventkit"
        } else {
            "fake"
        },
    }
}

/// Seed the registry from `inputs` and rebuild the read
/// cache. First-write-wins, so a restart is a no-op for
/// rows that already exist.
fn seed_sources_from(
    state: &AppState,
    conn: &mut Connection,
    inputs: &crate::source_registry::SeedInputs,
) {
    let entries: Vec<(&'static str, &'static str, &'static [&'static str])> = CONNECTOR_REGISTRY
        .iter()
        .map(|e| (e.id, e.name, e.capabilities))
        .collect();
    match crate::source_registry::seed_sources(conn, inputs, &entries) {
        Ok(_) => {
            if let Err(detail) = state.source_registry.load(conn) {
                eprintln!("warning: D1-4 source registry load failed: {detail}");
            }
        }
        Err(detail) => eprintln!("warning: D1-4 source seed failed: {detail}"),
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
/// `loop_engine::normalize` (same 13 names, same order).
const MANDATE_TOOLS: &[&str] = &[
    "fs.list",
    "fs.read",
    "fs.write",
    "reminders.list",
    "reminders.set_completed",
    "email.threads",
    "email.message",
    "imessage.threads",
    "contacts.search",
    "location.current",
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
    mandate_allowed_json_for_tools(goal_id, goal, entry, MANDATE_TOOLS)
}

/// The `allowed_json` document for a direct-write
/// Mandate (D1-6b-1, Q-D1-6b-23): same shape, but the
/// tool set is the single write tool the Execution may
/// invoke, not the full loop tool list.
fn mandate_allowed_json_for_tools(
    goal_id: Option<&str>,
    goal: &str,
    entry: &str,
    tools: &[&str],
) -> String {
    let value = serde_json::json!({
        "goal_id": goal_id,
        "goal": goal,
        "entry": entry,
        "tools": tools,
    });
    sqlite_store::canonical_json(&value)
}

/// The policy version a new Mandate binds: the latest
/// stored version, or a v1 recorded from the current
/// document when none exists yet (first-use baseline).
/// SQLite mode only; None otherwise. The document is
/// built from the maps before any SQLite lock is taken.
pub(crate) fn current_policy_version_id(state: &AppState) -> Option<String> {
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
pub(crate) fn create_mandate(
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

/// Create one Mandate row for a direct write Execution
/// (D1-6b-1, Q-D1-6b-23). `create_execution_with_binding`
/// binds no Mandate for executions without a task or a
/// loop, so the write route calls this explicitly after
/// building its Execution: `allowed_json.tools` is the
/// single write tool, `goal_id` is empty, the entry is
/// `direct-write`. SQLite mode only, best-effort (warn
/// only, never fail the caller), like `create_mandate`.
pub(crate) fn create_write_mandate(
    state: &AppState,
    execution_id: &str,
    tool: &str,
) -> Option<sqlite_store::MandateRow> {
    if state.sqlite.is_none() || state.sqlite_mode() != StateBackend::Sqlite {
        return None;
    }
    let policy_version_id = current_policy_version_id(state)?;
    let mut next = state.next_mandate_id.lock().expect("mandate id lock");
    let number = *next;
    *next += 1;
    drop(next);
    let row = sqlite_store::MandateRow {
        mandate_id: format!("mandate-{number}"),
        task_id: None,
        execution_id: Some(execution_id.to_string()),
        goal: String::new(),
        allowed_json: mandate_allowed_json_for_tools(None, "", "direct-write", &[tool]),
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
            eprintln!("warning: D1-6b-1 write mandate insert failed: {detail}");
            None
        }
    }
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

pub(crate) fn policy_defaults(state: &AppState) -> PolicyDefaults {
    state
        .policy_defaults
        .lock()
        .expect("policy defaults lock")
        .clone()
        .unwrap_or_default()
}

pub(crate) fn policy_overrides(state: &AppState) -> PolicyOverrides {
    state
        .policy_overrides
        .lock()
        .expect("policy overrides lock")
        .clone()
        .unwrap_or_default()
}

pub(crate) fn policy_effective(state: &AppState) -> EffectivePolicy {
    effective_policy(&policy_defaults(state), &policy_overrides(state))
}


#[derive(Debug, Serialize)]
pub(crate) struct ConnectorPolicyView {
    pub(crate) connector_id: String,
    pub(crate) allowed: bool,
    pub(crate) updated_ms: u64,
}

pub(crate) fn connector_policy_view(state: &AppState, connector_id: &str) -> ConnectorPolicyView {
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

pub(crate) fn persist_binding(state: &AppState, record: &WorkspaceBinding) {
    let Some(path) = &state.binding_state_path else { return };
    append_binding(path, record);
}

pub(crate) fn persist_project(state: &AppState, record: &Project) {
    let Some(path) = &state.project_state_path else { return };
    append_project(path, record);
}

pub(crate) fn persist_location(state: &AppState, record: &Location) {
    let Some(path) = &state.location_state_path else { return };
    append_location(path, record);
}

pub(crate) fn persist_worktree(state: &AppState, record: &Worktree) {
    let Some(path) = &state.worktree_state_path else { return };
    append_worktree(path, record);
}

pub(crate) fn unknown_record(what: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": format!("unknown {what}") })),
    )
        .into_response()
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
        // D1-6b-1: the loop-tool text stays generic —
        // the send route distinguishes these itself.
        MailError::NotRunning | MailError::Timeout => "mail source error".to_string(),
    }
}

/// MCL-1: the imessage loop-tool error text (the
/// `email_error_text` precedent — generic, never a
/// mail error string).
fn imessage_error_text(err: IMessageError) -> String {
    match err {
        IMessageError::NotFound(what) => format!("unknown {what}"),
        IMessageError::Unavailable(_) => "imessage source unavailable".to_string(),
        IMessageError::Denied => "imessage automation denied".to_string(),
        IMessageError::Upstream(_) => "imessage source error".to_string(),
        IMessageError::NotRunning | IMessageError::Timeout => "imessage source error".to_string(),
    }
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
pub(crate) fn executions_snapshot(state: &AppState) -> Option<Vec<ExecutionFileRecord>> {
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

pub(crate) fn effects_snapshot(state: &AppState) -> Option<Vec<EffectFileRecord>> {
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

pub(crate) fn execution_number(execution_id: &str) -> u64 {
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
pub(crate) fn retain_policy_rules_version(state: &AppState, rules: &[NetworkRule]) {
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
    scope: &crate::memory_store::MemoryScope,
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
        // D1-5 (Q-D1-52): the snapshot freezes the scope
        // triple of the Execution's binding — the Task's
        // workspace/project links plus the caller's
        // session. No binding at all means the all-empty
        // (global) triple, canonical "{}". Old rows are
        // untouched (insert-only), so their frozen scope
        // stays byte-identical.
        memory_scope: scope.canonical(),
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

/// P0-E: a fresh attempt supersedes any still-running
/// Execution a crashed earlier attempt left behind.
/// (Moved from routes_tasks.rs for the ORC-2 runner:
/// the Execution is now created in the runner, at the
/// point execution actually starts, not in the run /
/// resume handlers.)
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
    // D1-5 (Q-D1-52): the scope triple frozen into the
    // snapshot is derived from the Execution's binding —
    // the Task record's workspace/project links (when
    // this is a task execution) plus the caller's
    // session_id. There is no server-side "current
    // scope": loop executions without a task carry no
    // workspace/project, adapter executions carry none
    // of the three. goal_id is NOT part of the triple.
    let scope = {
        let tasks = state.tasks.lock().expect("task lock");
        let task = task_id.as_ref().and_then(|id| tasks.get(id));
        crate::memory_store::MemoryScope {
            workspace_id: task.and_then(|t| t.workspace_id.clone()),
            project_id: task.and_then(|t| t.project_id.clone()),
            session_id: session_id.clone(),
        }
    };
    let snapshot = if state.sqlite.is_some() {
        Some(build_workspace_snapshot(
            state,
            &format!("snapshot-{number}"),
            &instruction_digest,
            binding.as_ref(),
            &scope,
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

pub(crate) fn effect_number(effect_id: &str) -> u64 {
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
/// ORC-4 (R4-J6): returns the new row's id on
/// success, so a caller that must name the row in a
/// follow-up prompt (the research report) can. None
/// keeps the old best-effort contract: a failure
/// warns, never fails the step it records. Existing
/// callers ignore the value.
pub(crate) fn record_evidence_best_effort(
    state: &AppState,
    draft: &crate::evidence::OwnedEvidence,
) -> Option<String> {
    let Some(sqlite) = &state.sqlite else {
        return None;
    };
    let Ok(mut conn) = sqlite.lock() else {
        eprintln!("warning: evidence insert skipped (sqlite lock)");
        return None;
    };
    let row = match crate::sqlite_store::insert_evidence(&mut conn, &draft.as_draft()) {
        Ok(row) => row,
        Err(detail) => {
            eprintln!("warning: evidence insert failed: {detail}");
            return None;
        }
    };
    export_evidence_best_effort(state, &conn);
    Some(row.evidence_id)
}

/// E2-C §4.5: regenerate the Evidence audit export
/// (evidence.jsonl) from the connection the caller already
/// holds, when an export path is configured. Best-effort,
/// warn-only — an export failure never changes the caller's
/// response.
pub(crate) fn export_evidence_best_effort(state: &AppState, conn: &Connection) {
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
pub(crate) enum PatchCheck {
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
pub(crate) fn check_patch_write(
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
pub(crate) fn sync_patch_file_status(
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
pub(crate) fn link_patch_file_rejected_on_approval(state: &AppState, approval_id: &str) {
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
pub(crate) fn latest_evidence_for_effect(
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

/// R4-F2: read one text file under a loop-resolved
/// root. Path rules mirror `search_code`:
/// `normalize_search_path` rejects `..`, absolute
/// paths, backslashes and NUL lexically; the joined
/// path must canonicalize inside the (already
/// canonical) root, so a symlinked escape fails.
/// A missing file is ToolFailed and other read
/// failures (non-UTF-8, directory) are ToolFailed
/// too — the same outcome the files_root fallthrough
/// maps a 404 / bad read to today.
fn read_loop_file(root: &Path, raw_path: &str) -> Result<String, ErrorCode> {
    let normalized = crate::code_search::normalize_search_path(raw_path)
        .ok_or(ErrorCode::InvalidArguments)?;
    if normalized.is_empty() {
        return Err(ErrorCode::InvalidArguments);
    }
    let canonical_root = root
        .canonicalize()
        .map_err(|_| ErrorCode::InvalidArguments)?;
    let joined =
        canonical_root.join(normalized.replace('/', std::path::MAIN_SEPARATOR_STR));
    let canonical = joined.canonicalize().map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
            ErrorCode::ToolFailed
        }
        _ => ErrorCode::InvalidArguments,
    })?;
    if !canonical.starts_with(&canonical_root) {
        return Err(ErrorCode::InvalidArguments);
    }
    std::fs::read_to_string(&canonical).map_err(|_| ErrorCode::ToolFailed)
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
    close_execution_inner(state, execution_id, to, false, None)
}

/// ORC-3 (Q-R3-04): like `close_execution`, but the
/// review_required row the needs_review close opens
/// names the failed step's Effect. Only the hand-back
/// path passes one; every other caller keeps the old
/// behaviour (no effect named).
pub(crate) fn close_execution_with_inbox(
    state: &AppState,
    execution_id: &str,
    to: &str,
    effect_id: Option<&str>,
) -> Option<String> {
    close_execution_inner(state, execution_id, to, false, effect_id)
}

/// ORC-3 (R3-J6): the current status word of one
/// Execution, for the resume transition in
/// `begin_execution` (a needs_review Execution
/// returns to running when execution resumes).
pub(crate) fn execution_status(state: &AppState, execution_id: &str) -> Option<String> {
    executions_snapshot(state)?
        .into_iter()
        .find(|record| record.execution_id == execution_id)
        .map(|record| record.status)
}

/// E2-A §2.3 + P3-C §5.4: like `close_execution`, but the
/// close opens no Inbox row. The Worker driver uses it
/// for a test run (a test result is a result, not
/// something to review); ORC-3 (R3-J18b) the Executor
/// step logic uses it too, for a failed cargo-test
/// step — the finalizer no longer closes Executor-step
/// runs, so the step logic suppresses the row itself.
/// In JSONL/Shadow modes there is no Inbox side
/// effect to suppress; the flag only threads the SQLite path.
pub(crate) fn close_execution_no_inbox(
    state: &AppState,
    execution_id: &str,
    to: &str,
) -> Option<String> {
    close_execution_inner(state, execution_id, to, true, None)
}

fn close_execution_inner(
    state: &AppState,
    execution_id: &str,
    to: &str,
    suppress_inbox: bool,
    inbox_effect_id: Option<&str>,
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
            } else if let Some(effect_id) = inbox_effect_id {
                sqlite_store::transition_execution_with_inbox(
                    &mut conn,
                    execution_id,
                    to,
                    Some(effect_id),
                )
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

/// P0-E: after a takeover or transition rewrites an execution
/// row in SQLite mode, regenerate the JSONL audit export
/// best-effort (the P0-D rule: exports refresh on writes, and
/// a takeover/transition is a write even though it is not a
/// persist_execution call).
pub(crate) fn refresh_executions_export(state: &AppState) {
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
    /// E2-D batch 2: run-at time (epoch ms). A future value
    /// defers the run until the sweeper's due pass; a past
    /// or absent value means immediate. Normalized at
    /// creation: only a future value is stored (NULL
    /// otherwise), so `start_at_ms IS NOT NULL` is exactly
    /// "deferred run awaiting due". Not covered by the
    /// approval digest (a scheduling parameter, not the
    /// executed command — plan §2.2).
    #[serde(default)]
    start_at_ms: Option<i64>,
    /// E2-D batch 2: plain integer priority, default 0,
    /// larger first. Effective only at the sweeper's
    /// due-fetch ordering (`(priority DESC, seq ASC)`).
    /// Not covered by the approval digest (plan §2.2).
    #[serde(default)]
    priority: Option<i64>,
}

impl WorkerRunCreateDto {
    /// ORC-2 (J3): build a run request from inside the
    /// gateway (the Executor). The fields are private
    /// to this module; the constructor is the exact
    /// equivalent of the JSON the POST /worker/runs
    /// handler deserializes — immediate, default
    /// priority, no timeout override.
    pub(crate) fn internal(
        worktree_id: String,
        command: String,
        params: Option<serde_json::Value>,
        execution_id: Option<String>,
        approval_id: Option<String>,
    ) -> Self {
        Self {
            worktree_id,
            command,
            params,
            timeout_ms: None,
            execution_id,
            approval_id,
            start_at_ms: None,
            priority: None,
        }
    }
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
    // E2-D batch 2 (Q-D2 (b)): normalize the deferred start
    // here, once. Only a future `start_at_ms` is a deferred
    // run; a past or absent value is immediate (NULL in the
    // row, so `start_at_ms IS NOT NULL` is exactly
    // "deferred run awaiting due"). The deadline anchors at
    // START, not at creation — a deferred run's budget
    // starts when it starts, so a run deferred an hour
    // cannot expire before it runs. `set_execution_deadline`
    // still takes the min, so a later run never extends an
    // Execution's budget.
    let now = crate::effect_store::now_ms() as i64;
    let deferred_start_at = req.start_at_ms.filter(|&at| at > now);
    {
        let mut conn = sqlite.lock().expect("sqlite lock");
        let deadline = match deferred_start_at {
            Some(at) => at + resolved.timeout_ms as i64,
            None => now + resolved.timeout_ms as i64,
        };
        if let Err(detail) = sqlite_store::set_execution_deadline(
            &mut conn,
            &execution_id,
            deadline,
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
        start_at_ms: deferred_start_at,
        priority: Some(req.priority.unwrap_or(0)),
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
    // E2-D batch 2: a deferred run is only a row until its
    // due time — the sweeper starts it. An immediate run
    // spawns here, as before. Double-spawn is impossible by
    // construction: immediate runs have NULL `start_at_ms`
    // (the sweeper's due query never matches them) and
    // deferred runs are never spawned here.
    if deferred_start_at.is_none() {
        tokio::spawn(crate::worker::drive_worker_run(
            state.clone(),
            row.run_id.clone(),
        ));
    }
    Ok(row)
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

pub(crate) const DEVICE_ID: &str = "mac-reminders";
pub(crate) const DEVICE_ACTION: &str = "reminders.set_completed";
pub(crate) const DEVICE_TIMEOUT_SECS: u64 = 10;

/// E2-D §6 (`worker.wait`): default wait budget, in ms.
/// The loop tool timeout (10 s) caps it from outside;
/// normalize rejects a larger `timeout_ms`.
const WORKER_WAIT_DEFAULT_TIMEOUT_MS: u64 = 10_000;
/// E2-D §6 (`worker.wait`): row-state poll interval. The
/// read is a cheap SQLite query; polling more often buys
/// nothing, polling less often risks missing a terminal
/// state inside a 10 s budget.
const WORKER_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Append one action record to the persistence file, when
/// configured. Best-effort, mirroring persist_task.
pub(crate) fn persist_action(state: &AppState, record: &DeviceActionRecord) {
    let Some(path) = &state.actions_path else { return };
    append_action(path, record);
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(crate::routes_misc::health))
        .route("/chat/history", get(crate::routes_chat::history))
        .route("/chat/stream/start", post(crate::routes_chat::stream_start))
        .route("/chat/stream/apply", post(crate::routes_chat::stream_apply))
        .route("/chat/stream/cancel", post(crate::routes_chat::stream_cancel))
        .route("/api/chat/themes", get(crate::routes_chat::chat_themes))
        .route("/api/chat/themes/set", post(crate::routes_chat::chat_themes_set))
        .route("/chat/unread-count", get(crate::routes_chat::chat_unread_count))
        .route("/chat/{thread_id}/seen", post(crate::routes_chat::chat_mark_seen))
        .route("/api/chat/delete-turn", post(crate::routes_chat::chat_delete_turn))
        .route("/chat/message", get(crate::routes_chat::chat_message))
        .route("/chat/history-window", get(crate::routes_chat::chat_history_window))
        .route("/chat/search", get(crate::routes_chat::chat_search))
        .route("/chat/subscribe", post(crate::routes_chat::chat_subscribe))
        .route("/chat/stream-events", post(crate::routes_chat::chat_stream_events))
        .route("/chat/cancel", post(crate::routes_chat::chat_cancel))
        .route("/chat/stream", post(crate::routes_chat::chat_stream_open))
        .route("/chat/poke", post(crate::routes_chat::chat_poke))
        .route("/api/chat/sxs/select", post(crate::routes_chat::chat_sxs_select))
        .route("/api/voice/catalog", get(crate::routes_voice::voice_catalog))
        .route("/api/voice/catalog/filters", get(crate::routes_voice::voice_catalog_filters))
        .route("/api/voice/available-voices", get(crate::routes_voice::voice_available_voices))
        .route("/api/voice/pipeline-profiles", get(crate::routes_voice::voice_pipeline_profiles))
        .route("/api/voice/pipeline-profile", post(crate::routes_voice::voice_pipeline_profile_select))
        .route("/api/voice/custom-voices", get(crate::routes_voice::voice_custom_voices_list))
        .route(
            "/api/voice/custom-voices/{saved_voice_id}",
            patch(crate::routes_voice::voice_custom_voice_rename),
        )
        .route(
            "/api/voice/custom-voices/{saved_voice_id}",
            delete(crate::routes_voice::voice_custom_voice_delete),
        )
        .route("/api/voice/active-voice", get(crate::routes_voice::voice_active_voice_get))
        .route("/api/voice/active-voice", post(crate::routes_voice::voice_active_voice_save))
        .route("/api/voice/onboarding/reset", post(crate::routes_voice::voice_onboarding_reset))
        .route("/api/email/mailbox", get(crate::routes_mail::email_mailbox))
        .route("/api/email/mailbox/setup", get(crate::routes_mail::email_mailbox_setup))
        .route("/api/email/threads", get(crate::routes_mail::email_threads_list))
        .route("/api/email/threads/{thread_id}", get(crate::routes_mail::email_thread_get))
        .route("/api/email/messages/{message_id}", get(crate::routes_mail::email_message_get))
        .route("/api/email/messages/{message_id}/attachments/{index}", get(crate::routes_mail::email_attachment_get))
        // D1-6a: manual mail sync (SQLite v10) + calendar
        // read-only (EventKit on macOS, 501 elsewhere).
        .route("/api/email/sync", post(crate::routes_mail::email_sync))
        // D1-6b-1: real mail send (SQLite v11) + sent readers.
        .route("/api/email/send", post(crate::routes_mail::email_send))
        .route("/api/email/sent", get(crate::routes_mail::email_sent_list))
        .route("/api/email/sent/{id}", get(crate::routes_mail::email_sent_get))
        // D1-6b-3: mail delete (Trash only, tombstone) +
        // synced readers. The DELETE shares the path with
        // the batch-1 live message read; the tool_call_id
        // travels as a query parameter (the desktop
        // delete_json sends no body).
        .route("/api/email/messages/{message_id}", delete(crate::routes_mail::email_message_delete))
        .route("/api/email/synced", get(crate::routes_mail::email_synced_list))
        .route("/api/email/synced/{id}", get(crate::routes_mail::email_synced_get))
        // D1-6b-2: real calendar create (SQLite v12). The
        // POST shares the path with the 6a live list; the
        // list itself is unchanged.
        .route("/api/calendar/events", post(crate::routes_data::calendar_events_create))
        .route("/api/calendar/events", get(crate::routes_data::calendar_events_list))
        .route("/api/calendar/events/{id}", get(crate::routes_data::calendar_event_get))
        // D1-6b-4: Notes read + write (SQLite v13).
        // Sync / list / get are the readers; create /
        // append / delete are the digest-bound writes.
        .route("/api/notes/sync", post(crate::routes_notes::notes_sync))
        .route("/api/notes", get(crate::routes_notes::notes_api_list))
        .route("/api/notes/{id}", get(crate::routes_notes::notes_api_get))
        .route("/api/notes/create", post(crate::routes_notes::notes_api_create))
        .route("/api/notes/{id}/append", post(crate::routes_notes::notes_api_append))
        .route("/api/notes/{id}/delete", post(crate::routes_notes::notes_api_delete))
        // MCL-1: iMessage threads (SQLite v14). Sync is
        // the manual trigger (fact recording only — no
        // Effect, no approval, no Outbox, like
        // POST /api/email/sync); list / get read the
        // synced rows, tombstones visible.
        .route("/api/imessage/sync", post(crate::routes_imessage::imessage_sync))
        .route("/api/imessage/threads", get(crate::routes_imessage::imessage_threads_list))
        .route("/api/imessage/threads/{id}", get(crate::routes_imessage::imessage_thread_get))
        // MCL-4: iMessage send (SQLite v17). Digest-bound
        // write, the 6b-1 send order; list / get read the
        // sent rows.
        .route("/api/imessage/send", post(crate::routes_imessage::imessage_send))
        .route("/api/imessage/sent", get(crate::routes_imessage::imessage_sent_list))
        .route("/api/imessage/sent/{id}", get(crate::routes_imessage::imessage_sent_get))
        .route("/api/contacts/sync", post(crate::routes_contacts::contacts_sync))
        .route("/api/contacts", get(crate::routes_contacts::contacts_list))
        .route("/api/contacts/search", get(crate::routes_contacts::contacts_search))
        .route("/api/contacts/{id}", get(crate::routes_contacts::contacts_get))
        // MCL-5: contacts write (create / update /
        // delete). Digest-bound writes, the 6b-1 send
        // order; the delete is a tombstone write (POST,
        // not HTTP DELETE — the memories-delete
        // precedent).
        .route("/api/contacts/create", post(crate::routes_contacts::contacts_create))
        .route(
            "/api/contacts/{id}/update",
            post(crate::routes_contacts::contacts_update),
        )
        .route(
            "/api/contacts/{id}/delete",
            post(crate::routes_contacts::contacts_delete),
        )
        // MCL-3 shell route (S-1): the shell acquires
        // the fix itself and POSTs it with the startup
        // token (fact recording only — no Effect, no
        // approval, no Outbox, like POST /api/email/sync);
        // list / get read the recorded rows, newest
        // first. The old sidecar-fetch route
        // (`POST /api/location/record`) was deleted with
        // the sidecar Core Location body (Q-S-03/Q-S-08).
        .route(
            "/api/location/record-from-shell",
            post(crate::routes_location::location_record_from_shell),
        )
        .route("/api/location/samples", get(crate::routes_location::location_samples_list))
        .route("/api/location/samples/{id}", get(crate::routes_location::location_sample_get))
        .route("/api/nodes/register", post(crate::routes_peers::nodes_register))
        .route("/api/nodes/list", post(crate::routes_peers::nodes_list))
        .route("/api/nodes/heartbeat", post(crate::routes_peers::nodes_heartbeat))
        .route("/api/nodes/disconnect", post(crate::routes_peers::nodes_disconnect))
        .route("/api/nodes/unpair", post(crate::routes_peers::nodes_unpair))
        // P3-A: engineering records (16 routes).
        .route("/bindings/create", post(crate::routes_engineering::bindings_create))
        .route("/bindings/list", get(crate::routes_engineering::bindings_list))
        .route("/bindings/{id}", get(crate::routes_engineering::bindings_get))
        .route("/bindings/{id}/archive", post(crate::routes_engineering::bindings_archive))
        .route("/projects/create", post(crate::routes_engineering::projects_create))
        .route("/projects/list", get(crate::routes_engineering::projects_list))
        .route("/projects/{id}", get(crate::routes_engineering::projects_get))
        .route("/projects/{id}/archive", post(crate::routes_engineering::projects_archive))
        // D1-1: goals (4 routes).
        .route("/goals/create", post(crate::routes_data::goals_create))
        .route("/goals/list", get(crate::routes_data::goals_list))
        .route("/goals/{id}", get(crate::routes_data::goals_get))
        .route("/goals/{id}/archive", post(crate::routes_data::goals_archive))
        .route("/memories/save", post(crate::routes_data::memories_save))
        .route("/memories/list", get(crate::routes_data::memories_list))
        .route("/memories/{id}", get(crate::routes_data::memories_get))
        .route("/memories/{id}/delete", post(crate::routes_data::memories_delete))
        // D1-2: policy versions + mandates (3 routes).
        .route("/policies/versions", get(crate::routes_approvals::policies_versions_list))
        .route("/mandates", get(crate::routes_approvals::mandates_list))
        .route("/mandates/{id}", get(crate::routes_approvals::mandates_get))
        // D1-4: sources (3 routes) + tails reconnect (1 route).
        .route("/sources/list", get(crate::routes_data::sources_list))
        .route("/sources/{id}", get(crate::routes_data::sources_get))
        .route("/sources/{id}/disconnect", post(crate::routes_data::sources_disconnect))
        .route("/sources/{id}/reconnect", post(crate::routes_data::sources_reconnect))
        .route("/locations/create", post(crate::routes_engineering::locations_create))
        .route("/locations/list", get(crate::routes_engineering::locations_list))
        .route("/locations/{id}", get(crate::routes_engineering::locations_get))
        .route("/locations/{id}/archive", post(crate::routes_engineering::locations_archive))
        .route("/worktrees/create", post(crate::routes_engineering::worktrees_create))
        .route("/worktrees/list", get(crate::routes_engineering::worktrees_list))
        .route("/worktrees/{id}", get(crate::routes_engineering::worktrees_get))
        .route("/worktrees/{id}/archive", post(crate::routes_engineering::worktrees_archive))
        .route("/patches/list", get(crate::routes_engineering::patches_list))
        .route("/patches/{id}", get(crate::routes_engineering::patches_get))
        .route(
            "/patches/{patch_id}/files/{patch_file_id}/apply",
            post(crate::routes_engineering::patches_apply),
        )
        .route("/patches/{id}/reject", post(crate::routes_engineering::patches_reject))
        .route("/approval/check", post(crate::routes_approvals::approval_check))
        .route("/files/list", get(crate::routes_files::files_list))
        .route("/files/read", get(crate::routes_files::files_read))
        .route("/files/stat", get(crate::routes_files::files_stat))
        .route("/files/write", post(crate::routes_files::files_write))
        .route("/files/delete", post(crate::routes_files::files_delete))
        .route("/files/rename", post(crate::routes_files::files_rename))
        .route("/files/mkdir", post(crate::routes_files::files_mkdir))
        .route("/files/reads", post(crate::routes_files::files_reads))
        .route("/files/stats", post(crate::routes_files::files_stats))
        .route("/files/search", post(crate::routes_files::files_search))
        .route("/files/raw", get(crate::routes_files::files_raw))
        .route("/files/subscribe", post(crate::routes_files::files_subscribe))
        .route("/files/thumbnail", get(crate::routes_files::files_thumbnail))
        .route("/files/export", post(crate::routes_files::files_export))
        .route("/files/library", post(crate::routes_files::files_library))
        .route("/approval/request", post(crate::routes_approvals::approval_request))
        .route("/approval/decide", post(crate::routes_approvals::approval_decide))
        .route("/approvals", get(crate::routes_approvals::approvals_list))
        .route("/approvals/history", get(crate::routes_approvals::approvals_history))
        .route("/approvals/{approval_id}", get(crate::routes_approvals::approvals_get))
        .route("/approvals/{approval_id}/editable-message", get(crate::routes_approvals::approval_editable_message))
        .route("/approvals/{approval_id}/edit-message", post(crate::routes_approvals::approval_edit_message))
        .route("/agent/tool/call", post(crate::routes_loop::agent_tool_call))
        .route("/agent/loop/start", post(crate::routes_loop::loop_start))
        .route("/agent/loop/get", get(crate::routes_loop::loop_get))
        .route("/agent/loop/resume", post(crate::routes_loop::loop_resume))
        .route("/agent/loop/cancel", post(crate::routes_loop::loop_cancel))
        .route("/agent/loop/list", get(crate::routes_loop::loops_list))
        .route("/agent/loop/research/start", post(crate::routes_loop::research_start))
        .route("/agent/loop/research/{id}", get(crate::routes_loop::research_get))
        .route("/connectors/notes/list", get(crate::routes_connectors::notes_list))
        .route("/connectors/notes/create", post(crate::routes_connectors::notes_create))
        .route("/connectors/mail/list", get(crate::routes_connectors::mail_list))
        .route("/connectors/mail/send", post(crate::routes_connectors::mail_send))
        .route("/connectors", get(crate::routes_connectors::connectors_list))
        .route("/connectors/{id}", get(crate::routes_connectors::connectors_get))
        .route("/connectors/{id}/accounts", get(crate::routes_connectors::connectors_accounts))
        .route("/connectors/{id}/accounts/link", post(crate::routes_connectors::connectors_accounts_link))
        .route("/connectors/{id}/accounts/{account_id}/unlink", post(crate::routes_connectors::connectors_accounts_unlink))
        .route("/connectors/{id}/connect/info", get(crate::routes_connectors::connectors_connect_info))
        .route("/connectors/{id}/connect", post(crate::routes_connectors::connectors_connect))
        .route("/connectors/{id}/disconnect", post(crate::routes_connectors::connectors_disconnect))
        .route("/v1/custom-connectors/register", post(crate::routes_connectors::vault_custom_register))
        .route("/v1/custom-connectors/list", post(crate::routes_connectors::vault_custom_list))
        .route("/v1/custom-connectors/delete", post(crate::routes_connectors::vault_custom_delete))
        .route("/v1/credentials/register", post(crate::routes_connectors::vault_credentials_register))
        .route("/v1/credentials/list", get(crate::routes_connectors::vault_credentials_list))
        .route("/v1/credentials/delete", post(crate::routes_connectors::vault_credentials_delete))
        .route("/v1/credentials/capture", post(crate::routes_connectors::vault_credentials_capture))
        .route("/v1/credentials/capture/external-provider/request", post(crate::routes_connectors::vault_capture_request))
        .route("/v1/credentials/capture/external-provider/fulfill", post(crate::routes_connectors::vault_capture_fulfill))
        .route("/v1/credentials/catalog", get(crate::routes_connectors::vault_catalog_list))
        .route("/v1/credentials/catalog/details", get(crate::routes_connectors::vault_catalog_details))
        .route("/v1/credentials/catalog/update", post(crate::routes_connectors::vault_catalog_update))
        .route("/v1/credentials/catalog/delete", post(crate::routes_connectors::vault_catalog_delete))
        .route("/permissions", get(crate::routes_permissions::policy_get))
        .route("/permissions/active", get(crate::routes_permissions::policy_active_get))
        .route("/permissions/active", patch(crate::routes_permissions::policy_active_update))
        .route("/permissions/connectors/{connector}", patch(crate::routes_permissions::policy_connector_update))
        .route("/permissions/connectors/{connector}", delete(crate::routes_permissions::policy_connector_reset))
        .route("/permissions/hitl", patch(crate::routes_permissions::policy_hitl_update))
        .route("/permissions/hitl/snooze", delete(crate::routes_permissions::policy_hitl_snooze_delete))
        .route("/permissions/mitm", patch(crate::routes_permissions::policy_mitm_update))
        .route("/permissions/network-rules", patch(crate::routes_permissions::policy_network_rules_update))
        .route("/permissions/reset", post(crate::routes_permissions::policy_reset))
        .route("/permissions/revoke", post(crate::routes_permissions::policy_revoke))
        .route("/permissions/scopes/{scope_type}/{scope_id}", get(crate::routes_permissions::policy_scope_get))
        .route("/permissions/scopes/{scope_type}/{scope_id}", patch(crate::routes_permissions::policy_scope_update))
        .route("/permissions/sni-rejection", patch(crate::routes_permissions::policy_sni_rejection_update))
        .route("/permissions/tproxy", patch(crate::routes_permissions::policy_tproxy_update))
        .route("/permissions/inventory", get(crate::routes_permissions::policy_inventory))
        .route("/permissions/settings", get(crate::routes_permissions::policy_settings_get))
        .route("/permissions/settings", patch(crate::routes_permissions::policy_settings_update))
        .route("/connectors/{id}/permissions", get(crate::routes_connectors::connectors_permissions))
        .route("/model", get(crate::routes_misc::model_get))
        .route("/model", post(crate::routes_misc::model_set))
        .route("/config", get(crate::routes_misc::config_get))
        .route("/peers", get(crate::routes_peers::peers_list))
        .route("/peers/{muse_id}", get(crate::routes_peers::peers_get))
        .route("/peers/{muse_id}", patch(crate::routes_peers::peers_update))
        .route("/peers/{muse_id}", delete(crate::routes_peers::peers_disconnect))
        .route("/peers/invites", get(crate::routes_peers::peers_invites_list))
        .route("/peers/invites", post(crate::routes_peers::peers_invites_create))
        .route("/peers/invites/accept", post(crate::routes_peers::peers_invites_accept))
        .route("/peers/invites/open", post(crate::routes_peers::peers_invites_open))
        .route("/peers/invites/{invite_id}", delete(crate::routes_peers::peers_invites_revoke))
        .route("/peers/inbox", get(crate::routes_peers::peers_inbox_list))
        .route("/peers/inbox/decline", post(crate::routes_peers::peers_inbox_decline))
        .route("/peers/inbox/deliver", post(crate::routes_peers::peers_inbox_deliver))
        .route("/peers/requests", get(crate::routes_peers::peers_requests_list))
        .route("/peers/status", get(crate::routes_peers::peers_status))
        .route("/spaces/v2", get(crate::routes_misc::spaces_list))
        .route("/spaces/v2/{slug}/save", post(crate::routes_misc::spaces_save))
        .route("/spaces/v2/{slug}", delete(crate::routes_misc::spaces_delete))
        .route("/api/feed", get(crate::routes_feed::feed_list))
        .route("/api/feed/infinite", get(crate::routes_feed::feed_infinite))
        .route("/api/feed/status", get(crate::routes_feed::feed_status))
        .route("/api/feed/units/{unit_id}", get(crate::routes_feed::feed_unit_get))
        .route("/api/feed/units/{unit_id}", delete(crate::routes_feed::feed_unit_delete))
        .route("/api/feed/units/{unit_id}/reaction", post(crate::routes_feed::feed_unit_reaction))
        .route("/api/feed/units/{unit_id}/tap", post(crate::routes_feed::feed_unit_tap))
        .route("/api/feed/units/seen", post(crate::routes_feed::feed_units_seen))
        .route("/tasks/create", post(crate::routes_tasks::tasks_create))
        .route("/tasks/list", get(crate::routes_tasks::tasks_list))
        .route("/tasks/get", get(crate::routes_tasks::tasks_get))
        .route("/tasks/update", post(crate::routes_tasks::tasks_update))
        .route("/tasks/run", post(crate::routes_tasks::tasks_run))
        .route("/tasks/draft", get(crate::routes_tasks::tasks_draft))
        .route("/tasks/execute", post(crate::routes_tasks::tasks_execute))
        .route("/tasks/reject", post(crate::routes_tasks::tasks_reject))
        .route("/tasks/resume", post(crate::routes_tasks::tasks_resume))
        .route("/tasks/cancel", post(crate::routes_tasks::tasks_cancel))
        .route("/tasks/plan/draft", post(crate::routes_tasks::tasks_plan_draft))
        .route("/tasks/plan/{id}/replan", post(crate::routes_tasks::tasks_plan_replan))
        .route("/tasks/plan", get(crate::routes_tasks::tasks_plan_list))
        .route("/tasks/plan/{id}", get(crate::routes_tasks::tasks_plan_get))
        .route("/executions/list", get(crate::routes_ledger::executions_list))
        .route("/executions/get", get(crate::routes_ledger::executions_get))
        .route("/executions/transition", post(crate::routes_ledger::executions_transition))
        .route("/worker/runs", post(crate::routes_ledger::worker_runs_create))
        .route("/worker/runs/list", get(crate::routes_ledger::worker_runs_list))
        .route("/worker/runs/{id}", get(crate::routes_ledger::worker_run_get))
        .route("/worker/runs/{id}/cancel", post(crate::routes_ledger::worker_run_cancel))
        .route("/assignments/takeover", post(crate::routes_ledger::assignments_takeover))
        .route("/assignments/list", get(crate::routes_ledger::assignments_list))
        .route("/assignments/get", get(crate::routes_ledger::assignments_get))
        .route("/effects/list", get(crate::routes_ledger::effects_list))
        .route("/effects/get", get(crate::routes_ledger::effects_get))
        .route("/effects/transition", post(crate::routes_ledger::effects_transition))
        .route("/effects/reconcile", post(crate::routes_ledger::effects_reconcile))
        .route("/effects/verify", post(crate::routes_ledger::effects_verify))
        .route("/evidence/list", get(crate::routes_ledger::evidence_list))
        .route("/evidence/get", get(crate::routes_ledger::evidence_get))
        .route("/evidence/export", get(crate::routes_ledger::evidence_export))
        .route("/inbox/list", get(crate::routes_ledger::inbox_list))
        .route("/inbox/get", get(crate::routes_ledger::inbox_get))
        .route("/inbox/ack", post(crate::routes_ledger::inbox_ack))
        .route("/outbox/list", get(crate::routes_ledger::outbox_list))
        .route("/outbox/get", get(crate::routes_ledger::outbox_get))
        .route("/outbox/{id}/deliver", post(crate::routes_ledger::outbox_deliver))
        .route("/outbox/{id}/requeue", post(crate::routes_ledger::outbox_requeue))
        .route("/outbox/{id}/schedule", post(crate::routes_ledger::outbox_schedule))
        .route("/sessions/create", post(crate::routes_chat::sessions_create))
        .route("/sessions/list", get(crate::routes_chat::sessions_list))
        .route("/sessions/get", get(crate::routes_chat::sessions_get))
        .route("/sessions/send", post(crate::routes_chat::sessions_send))
        .route("/sessions/delete", post(crate::routes_chat::sessions_delete))
        .route("/session/pin", post(crate::routes_chat::session_pin))
        .route("/session/unpin", post(crate::routes_chat::session_unpin))
        .route("/session/archive", post(crate::routes_chat::session_archive))
        .route("/session/unarchive", post(crate::routes_chat::session_unarchive))
        .route("/session/rename", post(crate::routes_chat::session_rename))
        .route("/devices/reminders/list", get(crate::routes_devices::reminders_list))
        .route("/devices/actions/create", post(crate::routes_devices::actions_create))
        .route("/devices/reminders/set", post(crate::routes_devices::device_set))
        .route("/devices/actions/mark_rejected", post(crate::routes_devices::actions_mark_rejected))
        .route("/devices/actions/refresh", post(crate::routes_devices::actions_refresh))
        .route("/devices/actions/list", get(crate::routes_devices::actions_list))
        .route("/devices/actions/get", get(crate::routes_devices::actions_get))
        .route("/devices/authorization", get(crate::routes_devices::devices_authorization))
        .route("/devices/authorization/request", post(crate::routes_devices::devices_authorization_request))
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
            // D1-3: the Outbox deliverer rides the same
            // 10 s tick — no second scheduler.
            state.deliver_pending_outbox();
            // D1-3 C-1: due timed Outbox rows ride the
            // same tick, after the event channel and
            // before due worker runs — no second scheduler.
            // D1-3 C-2: the due pass is async (a scheduled
            // send can take up to 30 s in AppleScript).
            state.deliver_due_outbox().await;
            // E2-D batch 2: due deferred runs start from
            // the same tick — no second scheduler.
            state.start_due_worker_runs().await;
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
