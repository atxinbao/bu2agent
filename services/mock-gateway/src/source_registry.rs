//! D1-4 (plan §5, Q-D1-41–Q-D1-44): the Source Registry.
//!
//! A source is one concrete, connectable, cleanable,
//! subscribable instance — a connector account, a device
//! backend, a mail source, a file root — never a
//! capability group (`ConnectorEntry` stays the kind /
//! template, it never becomes a row). The SQLite
//! `sources` table is the fact source; the in-memory map
//! in `SourceRegistry` is a read cache, rebuilt from
//! SQLite at startup.
//!
//! Seeding (plan §5 item 2): at v8 startup the registry
//! is seeded from existing state — the two static
//! connector entries, every linked connector account from
//! the connector JSONL, the mail source, the reminder
//! backend (Fake and EventKit are two backends of ONE
//! source, Q-D1-43), and the files root. Registration is
//! first-write-wins per `source_id` (see
//! `register_source_tx`): the JSONL is the import source
//! at startup, and after the first import the table —
//! not the JSONL — is the fact source (same pattern as
//! the D1-2 Policy import). An account linked after the
//! first seed gets its row on the next startup, not
//! immediately: the plan seeds at startup only and names
//! no live registration hook on link.
//!
//! Disconnect (Q-D1-42) flips only the row's `status`
//! flag; it deletes nothing and touches no history. The
//! flag has no enforcement point in D1-4 — no handler
//! refuses work for a disconnected source, and inventing
//! one would be inventing semantics (same stance as the
//! D1-2 permissions-digest decision, Q-D1-23). The flag
//! is the recorded fact; enforcement points are a later
//! decision.
//!
//! Health is `unknown` at registration for every seed:
//! no live health probe exists in D1-4, so `health()`
//! always reports unknown. `unregister` is declared by
//! the trait but not built: delete-registration is an
//! explicitly deferred, separately-approved operation
//! (Q-D1-42), so the provided implementation refuses
//! with a named error instead of silently doing nothing.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use serde_json::Value;

use crate::connector_store::ConnectorAccount;
use crate::sqlite_store::{self, EventRow, SourceRow};

/// Health vocabulary for the `sources.health` column
/// (plan §5: `unknown/ok/degraded`, initial unknown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceHealth {
    Unknown,
    Ok,
    Degraded,
}

impl SourceHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            SourceHealth::Unknown => "unknown",
            SourceHealth::Ok => "ok",
            SourceHealth::Degraded => "degraded",
        }
    }
}

/// One source instance. `register` / `unregister` /
/// `health` / `list` / `events` are the plan §5
/// interface; `list` and `events` are registry-level
/// reads (associated functions, no instance needed).
pub trait Source {
    /// The registered id (what lands in `sources.source_id`).
    fn source_id(&self) -> String;
    /// One of `connector|device|file|mail`.
    fn kind(&self) -> &'static str;
    fn display_name(&self) -> String;
    /// Connection params as JSON — never secrets.
    fn config_json(&self) -> Value;
    /// The scope this source covers (an account id, a
    /// backend name, the file root).
    fn scope(&self) -> String;
    /// No live probe exists in D1-4: always unknown.
    fn health(&self) -> SourceHealth {
        SourceHealth::Unknown
    }

    /// The row this source registers as. `cursor` starts
    /// empty (no tree observed yet); `dedup_key` is
    /// `{kind}:{scope}` (recorded, not enforced — the
    /// plan names the column but no UNIQUE); `status`
    /// starts `active` (the flag is purely the Registry's
    /// own disconnect flag, not the connector's live
    /// `connected` flag).
    fn to_row(&self, now_ms: u64, origin_device_id: &str) -> SourceRow {
        let kind = self.kind();
        let scope = self.scope();
        SourceRow {
            source_id: self.source_id(),
            kind: kind.to_string(),
            display_name: self.display_name(),
            config_json: sqlite_store::canonical_json(&self.config_json()),
            scope: scope.clone(),
            health: self.health().as_str().to_string(),
            cursor: String::new(),
            dedup_key: format!("{kind}:{scope}"),
            status: "active".to_string(),
            created_ms: now_ms as i64,
            updated_ms: now_ms as i64,
            origin_device_id: origin_device_id.to_string(),
        }
    }

    /// Register this source (first-write-wins; a second
    /// register returns the existing row, no rewrite, no
    /// second event). Returns the row and whether it was
    /// newly inserted.
    fn register(
        &self,
        conn: &mut Connection,
        origin_device_id: &str,
    ) -> Result<(SourceRow, bool), String> {
        let now = sqlite_store::now_ms();
        let row = self.to_row(now, origin_device_id);
        let tx = conn.transaction().map_err(|e| format!("sqlite: {e}"))?;
        let out = sqlite_store::register_source_tx(&tx, &row, origin_device_id)?;
        tx.commit().map_err(|e| format!("sqlite: {e}"))?;
        Ok(out)
    }

    /// Not built in D1-4: delete-registration is an
    /// explicitly deferred, separately-approved operation
    /// (Q-D1-42). Refuse loudly instead of silently
    /// doing nothing.
    fn unregister(&self, _conn: &mut Connection) -> Result<(), String> {
        Err(
            "source unregister is not built in D1-4: delete-registration is a separately-approved operation"
                .to_string(),
        )
    }

    /// All registered sources, insertion order.
    fn list(conn: &Connection) -> Result<Vec<SourceRow>, String> {
        sqlite_store::list_sources(conn)
    }

    /// The event stream for one source (its
    /// `source.registered` / `source.disconnected` events,
    /// oldest first).
    fn events(conn: &Connection, source_id: &str) -> Result<Vec<EventRow>, String> {
        sqlite_store::list_events_for(conn, "source", source_id)
    }
}

/// One static connector entry (notes / mail) as a
/// source. The entry is the kind / template; the row
/// stands for the connector's connection itself.
pub struct StaticConnectorSource {
    pub id: &'static str,
    pub name: &'static str,
    pub capabilities: &'static [&'static str],
}

impl Source for StaticConnectorSource {
    fn source_id(&self) -> String {
        Self::row_id(self.id)
    }

    fn kind(&self) -> &'static str {
        "connector"
    }

    fn display_name(&self) -> String {
        self.name.to_string()
    }

    fn config_json(&self) -> Value {
        serde_json::json!({
            "id": self.id,
            "name": self.name,
            "capabilities": self.capabilities,
        })
    }

    fn scope(&self) -> String {
        self.id.to_string()
    }
}

impl StaticConnectorSource {
    /// The registered id for a static entry.
    /// Local definition: the plan seeds "Mock 连接器
    /// notes/mail (kind=connector，配置为静态注册表条目)"
    /// but names no id shape; `connector-{id}` keeps
    /// capability rows distinct from account rows
    /// (`account-N`).
    pub fn row_id(id: &str) -> String {
        format!("connector-{id}")
    }
}

/// One linked connector account as a source (Q-D1-41:
/// accounts register per instance, so account-1 can be
/// disconnected while account-2 stays live). Unlinked
/// (tombstoned) accounts are never seeded.
pub struct ConnectorAccountSource {
    pub account: ConnectorAccount,
}

impl Source for ConnectorAccountSource {
    fn source_id(&self) -> String {
        self.account.account_id.clone()
    }

    fn kind(&self) -> &'static str {
        "connector"
    }

    fn display_name(&self) -> String {
        self.account.display_name.clone()
    }

    fn config_json(&self) -> Value {
        serde_json::json!({
            "connector_id": self.account.connector_id,
            "display_name": self.account.display_name,
        })
    }

    fn scope(&self) -> String {
        self.account.account_id.clone()
    }
}

/// The mail source (the `MailSource` trait's live-query
/// source — Mail.app on macOS, Mock in tests,
/// Unavailable elsewhere). One row: the source has no
/// account concept of its own.
pub struct MailSourceEntry;

impl Source for MailSourceEntry {
    fn source_id(&self) -> String {
        "mail-source".to_string()
    }

    fn kind(&self) -> &'static str {
        "mail"
    }

    fn display_name(&self) -> String {
        "Mail".to_string()
    }

    fn config_json(&self) -> Value {
        serde_json::json!({})
    }

    fn scope(&self) -> String {
        "mail".to_string()
    }
}

/// The calendar source (the `MailSourceEntry`
/// precedent): one row for the EventKit calendar
/// backend — the same backend serves the D1-6a live
/// reads and the D1-6b-2 create. One row: the source
/// has no account concept of its own (the calendar is
/// picked per create, Q-D1-6b-09).
pub struct CalendarSourceEntry;

impl Source for CalendarSourceEntry {
    fn source_id(&self) -> String {
        "calendar-source".to_string()
    }

    fn kind(&self) -> &'static str {
        "calendar"
    }

    fn display_name(&self) -> String {
        "Calendar".to_string()
    }

    fn config_json(&self) -> Value {
        serde_json::json!({})
    }

    fn scope(&self) -> String {
        "calendar".to_string()
    }
}

/// The reminder backend as one source (Q-D1-43): Fake
/// and EventKit are two backends of this single source,
/// never two rows. `backend` is the switch value
/// (`fake` / `eventkit`) — a connection param, not a
/// secret. Fake's in-memory data is demo data: it is
/// never persisted and never recovered after restart;
/// only `reminder-demo` is seeded.
pub struct ReminderDeviceSource {
    pub backend: &'static str,
}

impl Source for ReminderDeviceSource {
    fn source_id(&self) -> String {
        "device-reminders".to_string()
    }

    fn kind(&self) -> &'static str {
        "device"
    }

    fn display_name(&self) -> String {
        format!("Reminders ({})", self.backend)
    }

    fn config_json(&self) -> Value {
        serde_json::json!({ "backend": self.backend })
    }

    fn scope(&self) -> String {
        "reminders".to_string()
    }
}

/// The files root as one source. `scope` is the root
/// directory as configured (stored verbatim, not
/// canonicalized — the test compares the list to the
/// table, never to a canonical path).
pub struct FileRootSource {
    pub root: PathBuf,
}

impl Source for FileRootSource {
    fn source_id(&self) -> String {
        "files-root".to_string()
    }

    fn kind(&self) -> &'static str {
        "file"
    }

    fn display_name(&self) -> String {
        "Files".to_string()
    }

    fn config_json(&self) -> Value {
        serde_json::json!({})
    }

    fn scope(&self) -> String {
        self.root.display().to_string()
    }
}

/// Inputs the v8 seed reads from existing state. The
/// caller snapshots these BEFORE taking the SQLite lock
/// (the D1-2 policy-import rule: map locks first, never
/// under the SQLite lock — persist call sites take the
/// SQLite lock while holding a map lock, so reversing the
/// order here would risk deadlock under concurrency).
pub struct SeedInputs {
    /// Linked connector accounts (from the connector
    /// JSONL via the in-memory map); unlinked accounts
    /// are filtered out by the seed.
    pub accounts: Vec<ConnectorAccount>,
    pub files_root: PathBuf,
    pub origin_device_id: String,
    /// `fake` or `eventkit` — the `MOCK_DEVICE_BACKEND`
    /// switch value the production constructor reads.
    pub device_backend: &'static str,
}

/// Seed the registry at v8 startup from existing state
/// (plan §5 item 2). Every row is registered
/// first-write-wins, so re-running the seed on restart
/// is a no-op for rows that already exist — and an
/// account linked after the first seed gets its row on
/// the next startup. Returns the rows in seed order.
/// The static connector entries are passed in (they
/// live in the server's private static registry).
pub fn seed_sources(
    conn: &mut Connection,
    inputs: &SeedInputs,
    static_connectors: &[(&'static str, &'static str, &'static [&'static str])],
) -> Result<Vec<SourceRow>, String> {
    let mut rows: Vec<SourceRow> = Vec::new();
    for (id, name, capabilities) in static_connectors {
        let entry = StaticConnectorSource {
            id,
            name,
            capabilities,
        };
        let (row, _) = entry.register(&mut *conn, &inputs.origin_device_id)?;
        rows.push(row);
    }
    for account in &inputs.accounts {
        if account.unlinked {
            continue;
        }
        let source = ConnectorAccountSource {
            account: account.clone(),
        };
        let (row, _) = source.register(&mut *conn, &inputs.origin_device_id)?;
        rows.push(row);
    }
    let (row, _) = MailSourceEntry.register(&mut *conn, &inputs.origin_device_id)?;
    rows.push(row);
    let (row, _) = CalendarSourceEntry.register(&mut *conn, &inputs.origin_device_id)?;
    rows.push(row);
    let (row, _) = ReminderDeviceSource {
        backend: inputs.device_backend,
    }
    .register(&mut *conn, &inputs.origin_device_id)?;
    rows.push(row);
    let (row, _) = FileRootSource {
        root: inputs.files_root.clone(),
    }
    .register(&mut *conn, &inputs.origin_device_id)?;
    rows.push(row);
    Ok(rows)
}

/// The in-memory read cache: a map of the `sources`
/// table, rebuilt from SQLite at startup (`load`) and
/// reloaded after seeding. The table stays the fact
/// source — HTTP reads go to SQLite when it is
/// configured and fall back to this cache otherwise.
#[derive(Clone, Default)]
pub struct SourceRegistry {
    cache: Arc<Mutex<HashMap<String, SourceRow>>>,
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Rebuild the cache from the SQLite table.
    pub fn load(&self, conn: &Connection) -> Result<(), String> {
        let rows = sqlite_store::list_sources(conn)?;
        let mut map = HashMap::new();
        for row in rows {
            map.insert(row.source_id.clone(), row);
        }
        *self.cache.lock().expect("source registry lock") = map;
        Ok(())
    }

    pub fn list(&self) -> Vec<SourceRow> {
        let mut rows: Vec<SourceRow> = self
            .cache
            .lock()
            .expect("source registry lock")
            .values()
            .cloned()
            .collect();
        rows.sort_by(|a, b| a.source_id.cmp(&b.source_id));
        rows
    }

    pub fn get(&self, source_id: &str) -> Option<SourceRow> {
        self.cache
            .lock()
            .expect("source registry lock")
            .get(source_id)
            .cloned()
    }
}
