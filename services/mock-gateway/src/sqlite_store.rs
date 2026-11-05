//! P0-D SQLite persistence for the unified execution plane.
//!
//! One database file (`gateway.sqlite3`, opened through
//! `MOCK_SQLITE_PATH`) holds the migrated domains
//! (executions / effects / approvals) plus the event log, the
//! Inbox / Outbox, and the P0-E-driven tables (assignments,
//! worker sessions, checkpoints, workspace snapshots). The
//! remaining planes (tasks / loops / actions / sessions /
//! policies / chat) stay on JSONL and are untouched here.
//!
//! Rules frozen by the approved P0-D plan (pa-p0d-plan-draft.md):
//! - schema v1, hand-written ordered migrations, no
//!   down-migration, no v1 foreign keys, `db_meta` holds
//!   `db_schema_version` and `import_done`;
//! - one domain operation = one SQLite transaction (event row +
//!   state row + derived Inbox / Outbox rows commit together);
//! - `journal_mode = WAL` + `synchronous = FULL` on every table;
//! - a single `Mutex<Connection>` (held by `AppState`) serializes
//!   writers; this module never holds a lock across `.await`
//!   (all functions are synchronous; callers lock, call, drop);
//! - event payloads are post-state snapshots, never deltas;
//!   ordering is `seq` per aggregate and `id` globally, never
//!   wall-clock (`created_ms` is audit / display only);
//! - `idempotency_key` is UNIQUE at the DB level (application
//!   layer checks first, the DB is the backstop);
//! - Inbox dedup key is `source_event_id` UNIQUE; Outbox dedup
//!   key is `(execution_id, destination_kind)` UNIQUE;
//! - imported rows keep `schema_version = 0` / origin
//!   `"unknown"` verbatim, `created_ms` NULL (original time
//!   unknown, honestly empty), `updated_at_ms` = import time;
//! - the JSONL files are never deleted by this module.
//!
//! This module knows SQLite rows, not HTTP semantics: the server
//! maps its in-memory records to the file-record structs shared
//! with the JSONL stores and calls the `upsert_*` / import /
//! export functions here. `fold_matches_tables` is the test
//! proof that `fold(events) == tables` for the migrated domains.

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::approval_store::ApprovalFileRecord;
use crate::effect_store::EffectFileRecord;
use crate::execution_store::ExecutionFileRecord;

/// DB schema version. v1 is the P0-D baseline (10 tables);
/// v2 adds the P0-R evidence table (see SCHEMA_V2). Row-level
/// `schema_version` fields are a separate concept (the record
/// contract version, still 1 for P0-A+ rows) and are untouched.
pub const DB_SCHEMA_VERSION: i64 = 2;

/// Backend mode for the migrated domains. `Jsonl` is the
/// pre-P0-D behaviour (SQLite absent or the emergency diagnostic
/// override); `Shadow` is D2 (JSONL authoritative, SQLite
/// shadow-written, failures warned + counted, never fatal);
/// `Sqlite` is D4 (SQLite authoritative, JSONL appends stopped,
/// JSONL files become generated audit exports).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateBackend {
    Jsonl,
    Shadow,
    Sqlite,
}

impl StateBackend {
    pub fn name(self) -> &'static str {
        match self {
            StateBackend::Jsonl => "jsonl",
            StateBackend::Shadow => "shadow",
            StateBackend::Sqlite => "sqlite",
        }
    }
}

/// `MOCK_STATE_BACKEND=jsonl` is the emergency diagnostic
/// override (loud log at startup, non-steady-state). Any other
/// value (including unset) means SQLite is used when configured.
pub fn backend_override_from_env(value: Option<&str>) -> bool {
    value == Some("jsonl")
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

const SCHEMA_V1: &str = "
CREATE TABLE IF NOT EXISTS db_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate_kind TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    origin_device_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    UNIQUE (aggregate_kind, aggregate_id, seq)
);
CREATE TABLE IF NOT EXISTS executions (
    seq INTEGER NOT NULL,
    execution_id TEXT PRIMARY KEY,
    task_id TEXT,
    loop_id TEXT,
    session_id TEXT,
    worker_session_id TEXT,
    workspace_snapshot_id TEXT,
    status TEXT NOT NULL,
    origin_device_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_ms INTEGER,
    updated_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS effects (
    seq INTEGER NOT NULL,
    effect_id TEXT PRIMARY KEY,
    execution_id TEXT,
    capability TEXT NOT NULL,
    operation TEXT NOT NULL,
    scope TEXT NOT NULL,
    input_digest TEXT,
    approval_id TEXT,
    idempotency_key TEXT UNIQUE,
    status TEXT NOT NULL,
    reconciliation_result TEXT,
    reconciliation_detail TEXT,
    reconciliation_checked_ms INTEGER,
    detail TEXT,
    origin_device_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_ms INTEGER,
    updated_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS approvals (
    seq INTEGER NOT NULL,
    approval_id TEXT PRIMARY KEY,
    tool TEXT NOT NULL,
    status TEXT NOT NULL,
    binding_json TEXT,
    consumed INTEGER NOT NULL,
    operation_digest TEXT,
    action_binding TEXT,
    effect_binding TEXT,
    message TEXT,
    origin_device_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_ms INTEGER,
    updated_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS assignments (
    seq INTEGER NOT NULL,
    assignment_id TEXT PRIMARY KEY,
    execution_id TEXT,
    worker_session_id TEXT,
    origin_device_id TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT,
    lease_expires_ms INTEGER,
    heartbeat_ms INTEGER,
    schema_version INTEGER NOT NULL,
    created_ms INTEGER,
    updated_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS workspace_snapshots (
    seq INTEGER NOT NULL,
    snapshot_id TEXT PRIMARY KEY,
    content_sha256 TEXT NOT NULL,
    files_root TEXT NOT NULL,
    connector_ids_json TEXT NOT NULL,
    device_ids_json TEXT NOT NULL,
    policy_binding_ids_json TEXT NOT NULL,
    memory_scope TEXT NOT NULL,
    instruction_digest TEXT NOT NULL,
    origin_device_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    binding_location_id TEXT,
    binding_worktree_id TEXT,
    binding_root_digest TEXT,
    created_ms INTEGER
);
CREATE TABLE IF NOT EXISTS worker_sessions (
    seq INTEGER NOT NULL,
    worker_session_id TEXT PRIMARY KEY,
    origin_device_id TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT,
    schema_version INTEGER NOT NULL,
    created_ms INTEGER,
    updated_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS checkpoints (
    seq INTEGER NOT NULL,
    checkpoint_id TEXT PRIMARY KEY,
    execution_id TEXT,
    execution_seq INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    origin_device_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_ms INTEGER
);
CREATE TABLE IF NOT EXISTS inbox (
    seq INTEGER NOT NULL,
    inbox_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    execution_id TEXT,
    effect_id TEXT,
    approval_id TEXT,
    source_event_id TEXT NOT NULL UNIQUE,
    created_ms INTEGER NOT NULL,
    resolved_ms INTEGER,
    resolved_reason TEXT
);
CREATE TABLE IF NOT EXISTS outbox (
    seq INTEGER NOT NULL,
    outbox_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    payload_summary_json TEXT NOT NULL,
    destination_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    dedup_key TEXT NOT NULL UNIQUE,
    created_ms INTEGER NOT NULL,
    updated_at_ms INTEGER
);
";

/// P0-R v2 migration: the evidence table only. No FKs (same
/// convention as v1), no changes to existing tables, and no
/// backfill: evidence exists only for rows observed from P0-R
/// on. `observed_json` is a serialized JSON object; it never
/// carries file bytes, credential values, or session text
/// (enforced at the observation site, not by the schema).
const SCHEMA_V2: &str = "
CREATE TABLE IF NOT EXISTS evidence (
    seq INTEGER NOT NULL,
    evidence_id TEXT PRIMARY KEY,
    effect_id TEXT NOT NULL,
    execution_id TEXT,
    source TEXT NOT NULL,
    verdict TEXT NOT NULL,
    reason TEXT NOT NULL,
    observed_json TEXT NOT NULL,
    checked_ms INTEGER NOT NULL,
    origin_device_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    created_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_evidence_effect ON evidence(effect_id);
";

fn err(e: rusqlite::Error) -> String {
    format!("sqlite: {e}")
}

/// Open (creating parent directories) and migrate a database
/// file. Idempotent: re-opening an already-migrated file is a
/// no-op, and the schema version ends at 2 either way.
pub fn open_db(path: &Path) -> Result<Connection, String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| format!("sqlite: cannot create db dir: {e}"))?;
        }
    }
    let mut conn = Connection::open(path).map_err(err)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
        .map_err(err)?;
    apply_migrations(&mut conn)?;
    Ok(conn)
}

pub fn apply_migrations(conn: &mut Connection) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    tx.execute_batch("CREATE TABLE IF NOT EXISTS db_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);")
        .map_err(err)?;
    let version: i64 = tx
        .query_row(
            "SELECT value FROM db_meta WHERE key = 'db_schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(err)?
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    if version < 1 {
        tx.execute_batch(SCHEMA_V1).map_err(err)?;
        tx.execute(
            "INSERT INTO db_meta (key, value) VALUES ('db_schema_version', '1')
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [],
        )
        .map_err(err)?;
        tx.execute(
            "INSERT OR IGNORE INTO db_meta (key, value) VALUES ('import_done', '0')",
            [],
        )
        .map_err(err)?;
    }
    // P0-R v2 step: evidence table only. A fresh database runs
    // v1 then v2 in the same transaction; a v1 database runs
    // v2 only; an already-v2 database skips both. The version
    // is advanced to 2 only after the v2 DDL succeeds.
    if version < 2 {
        tx.execute_batch(SCHEMA_V2).map_err(err)?;
        tx.execute(
            "INSERT INTO db_meta (key, value) VALUES ('db_schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![DB_SCHEMA_VERSION.to_string()],
        )
        .map_err(err)?;
    }
    // P0-E additive column: databases migrated by P0-D have no
    // worker_sessions.detail, but the approved plan's recovery
    // scan records its note ("process ended" / "clean shutdown")
    // there. Nullable, idempotent, schema version stays 1: this
    // is a column backfill, not a v2 migration, and it changes
    // no existing row (old rows read detail as NULL). A missing
    // table (a partial fixture, never a real migrated database)
    // is a no-op, not an error: ALTER TABLE on a missing table
    // would fail the whole migration.
    backfill_column(&tx, "worker_sessions", "detail")?;
    // P3-A additive columns: databases migrated before P3-A
    // have no binding columns on workspace_snapshots. Nullable,
    // idempotent, schema version stays 2: this is a column
    // backfill, not a v3 migration, and it changes no existing
    // row (old rows read the three bindings as NULL).
    for column in [
        "binding_location_id",
        "binding_worktree_id",
        "binding_root_digest",
    ] {
        backfill_column(&tx, "workspace_snapshots", column)?;
    }
    tx.commit().map_err(err)?;
    Ok(())
}

/// Add a nullable TEXT column when the table exists and the
/// column is missing. Idempotent; a missing table is a no-op.
fn backfill_column(tx: &rusqlite::Transaction<'_>, table: &str, column: &str) -> Result<(), String> {
    let table_exists: bool = tx
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get::<_, i64>(0),
        )
        .map_err(err)
        .map(|count| count > 0)?;
    if !table_exists {
        return Ok(());
    }
    let has_column: bool = {
        let mut stmt = tx
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(err)?;
        let names = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(err)?
            .collect::<Result<Vec<String>, _>>()
            .map_err(err)?;
        names.iter().any(|name| name == column)
    };
    if !has_column {
        tx.execute(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"), [])
            .map_err(err)?;
    }
    Ok(())
}

pub fn schema_version(conn: &Connection) -> Result<i64, String> {
    conn.query_row(
        "SELECT value FROM db_meta WHERE key = 'db_schema_version'",
        [],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(err)?
    .and_then(|v| v.parse::<i64>().ok())
    .ok_or_else(|| "sqlite: db_schema_version missing".to_string())
}

pub fn import_done(conn: &Connection) -> Result<bool, String> {
    Ok(conn
        .query_row(
            "SELECT value FROM db_meta WHERE key = 'import_done'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(err)?
        .as_deref()
        == Some("1"))
}

fn set_import_done(tx: &rusqlite::Transaction<'_>) -> Result<(), String> {
    tx.execute(
        "INSERT INTO db_meta (key, value) VALUES ('import_done', '1')
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [],
    )
    .map_err(err)?;
    Ok(())
}

/// Numeric suffix of an `execution-N` / `effect-N` /
/// `approval-N` id, or 0 when the id does not carry one. The
/// `seq` column of a migrated row is exactly this suffix, so
/// counters are `MAX(seq) + 1` across both sources.
pub fn id_suffix(id: &str) -> i64 {
    id.rsplit('-')
        .next()
        .and_then(|n| n.parse::<i64>().ok())
        .unwrap_or(0)
}

fn next_seq_for(tx: &rusqlite::Transaction<'_>, table: &str, id_col: &str) -> Result<i64, String> {
    let max: Option<i64> = tx
        .query_row(
            &format!("SELECT MAX(seq) FROM {table}"),
            [],
            |row| row.get(0),
        )
        .map_err(err)?;
    let _ = id_col;
    Ok(max.unwrap_or(0) + 1)
}

/// Next id numbers (one past the largest suffix) per migrated
/// domain, from SQLite rows alone. Callers take the max of this
/// and the JSONL-derived counter when both sources exist.
pub fn next_numbers(conn: &Connection) -> Result<(u64, u64, u64), String> {
    let next = |table: &str, col: &str| -> Result<u64, String> {
        let mut stmt = conn
            .prepare(&format!("SELECT {col} FROM {table}"))
            .map_err(err)?;
        let ids = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(err)?
            .collect::<Result<Vec<String>, _>>()
            .map_err(err)?;
        Ok(ids
            .iter()
            .map(|id| id_suffix(id))
            .filter(|n| *n > 0)
            .max()
            .map(|n| (n + 1) as u64)
            .unwrap_or(1))
    };
    Ok((
        next("executions", "execution_id")?,
        next("effects", "effect_id")?,
        next("approvals", "approval_id")?,
    ))
}

// Eight arguments because an event row has eight meaningful
// fields; grouping them into a struct would only move the
// field list, not shrink it, and every producer site already
// passes them in this order.
#[allow(clippy::too_many_arguments)]
fn append_event(
    tx: &rusqlite::Transaction<'_>,
    kind: &str,
    aggregate_id: &str,
    event_type: &str,
    payload_json: &str,
    origin_device_id: &str,
    created_ms: i64,
    schema_version: i64,
) -> Result<i64, String> {
    let seq: i64 = tx
        .query_row(
            "SELECT MAX(seq) FROM events WHERE aggregate_kind = ?1 AND aggregate_id = ?2",
            params![kind, aggregate_id],
            |row| row.get::<_, Option<i64>>(0),
        )
        .map_err(err)?
        .unwrap_or(0)
        + 1;
    tx.execute(
        "INSERT INTO events (aggregate_kind, aggregate_id, seq, event_type, payload_json, created_ms, origin_device_id, schema_version)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![kind, aggregate_id, seq, event_type, payload_json, created_ms, origin_device_id, schema_version],
    )
    .map_err(err)?;
    Ok(tx.last_insert_rowid())
}

fn insert_inbox(
    tx: &rusqlite::Transaction<'_>,
    kind: &str,
    execution_id: Option<&str>,
    effect_id: Option<&str>,
    approval_id: Option<&str>,
    source_event_id: &str,
    created_ms: i64,
) -> Result<(), String> {
    let seq = next_seq_for(tx, "inbox", "inbox_id")?;
    // Dedup: one row per triggering event, enforced by the
    // UNIQUE(source_event_id) constraint; INSERT OR IGNORE
    // keeps a re-delivered trigger from duplicating a row.
    tx.execute(
        "INSERT OR IGNORE INTO inbox (seq, inbox_id, kind, execution_id, effect_id, approval_id, source_event_id, created_ms, resolved_ms, resolved_reason)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, NULL)",
        params![seq, format!("inbox-{seq}"), kind, execution_id, effect_id, approval_id, source_event_id, created_ms],
    )
    .map_err(err)?;
    Ok(())
}

fn auto_close_inbox(
    tx: &rusqlite::Transaction<'_>,
    kind: &str,
    id_col: &str,
    id_value: &str,
    resolved_ms: i64,
) -> Result<(), String> {
    tx.execute(
        &format!(
            "UPDATE inbox SET resolved_ms = ?1, resolved_reason = 'auto'
             WHERE kind = ?2 AND {id_col} = ?3 AND resolved_ms IS NULL"
        ),
        params![resolved_ms, kind, id_value],
    )
    .map_err(err)?;
    Ok(())
}

fn upsert_execution_tx(
    tx: &rusqlite::Transaction<'_>,
    record: &ExecutionFileRecord,
    event_type: &str,
) -> Result<(), String> {
    let now = now_ms() as i64;
    let created_ms: Option<i64> = tx
        .query_row(
            "SELECT created_ms FROM executions WHERE execution_id = ?1",
            params![record.execution_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(err)?
        .flatten();
    let created_ms = created_ms.unwrap_or(now);
    let seq = {
        let suffix = id_suffix(&record.execution_id);
        if suffix > 0 {
            suffix
        } else {
            next_seq_for(tx, "executions", "execution_id")?
        }
    };
    tx.execute(
        "INSERT INTO executions (seq, execution_id, task_id, loop_id, session_id, worker_session_id, workspace_snapshot_id, status, origin_device_id, schema_version, created_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(execution_id) DO UPDATE SET
           seq = excluded.seq, task_id = excluded.task_id, loop_id = excluded.loop_id,
           session_id = excluded.session_id, worker_session_id = excluded.worker_session_id,
           workspace_snapshot_id = excluded.workspace_snapshot_id, status = excluded.status,
           origin_device_id = excluded.origin_device_id, schema_version = excluded.schema_version,
           created_ms = excluded.created_ms, updated_at_ms = excluded.updated_at_ms",
        params![
            seq,
            record.execution_id,
            record.task_id,
            record.loop_id,
            record.session_id,
            record.worker_session_id,
            record.workspace_snapshot_id,
            record.status,
            record.origin_device_id,
            record.schema_version as i64,
            created_ms,
            now,
        ],
    )
    .map_err(err)?;
    let payload = serde_json::to_string(record).map_err(|e| format!("sqlite: cannot encode execution: {e}"))?;
    let event_id = append_event(
        tx,
        "execution",
        &record.execution_id,
        event_type,
        &payload,
        &record.origin_device_id,
        now,
        record.schema_version as i64,
    )?;
    let source = format!("event-{event_id}");
    match record.status.as_str() {
        "needs_review" => insert_inbox(
            tx,
            "review_required",
            Some(&record.execution_id),
            None,
            None,
            &source,
            now,
        )?,
        "failed" => insert_inbox(
            tx,
            "failed",
            Some(&record.execution_id),
            None,
            None,
            &source,
            now,
        )?,
        _ => {
            // A non-review status closes open review rows for
            // this execution; failed rows are never auto-closed
            // (they close only by user ack, see ack_inbox).
            auto_close_inbox(tx, "review_required", "execution_id", &record.execution_id, now)?;
        }
    }
    Ok(())
}

fn upsert_effect_tx(
    tx: &rusqlite::Transaction<'_>,
    record: &EffectFileRecord,
) -> Result<(), String> {
    let now = now_ms() as i64;
    let created_ms: Option<i64> = tx
        .query_row(
            "SELECT created_ms FROM effects WHERE effect_id = ?1",
            params![record.effect_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(err)?
        .flatten();
    let created_ms = created_ms.unwrap_or(now);
    let seq = {
        let suffix = id_suffix(&record.effect_id);
        if suffix > 0 {
            suffix
        } else {
            next_seq_for(tx, "effects", "effect_id")?
        }
    };
    let (recon_result, recon_detail, recon_checked) = match &record.reconciliation {
        Some(r) => (
            Some(r.result.clone()),
            r.detail.clone(),
            Some(r.checked_ms as i64),
        ),
        None => (None, None, None),
    };
    tx.execute(
        "INSERT INTO effects (seq, effect_id, execution_id, capability, operation, scope, input_digest, approval_id, idempotency_key, status, reconciliation_result, reconciliation_detail, reconciliation_checked_ms, detail, origin_device_id, schema_version, created_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
         ON CONFLICT(effect_id) DO UPDATE SET
           seq = excluded.seq, execution_id = excluded.execution_id, capability = excluded.capability,
           operation = excluded.operation, scope = excluded.scope, input_digest = excluded.input_digest,
           approval_id = excluded.approval_id, idempotency_key = excluded.idempotency_key,
           status = excluded.status, reconciliation_result = excluded.reconciliation_result,
           reconciliation_detail = excluded.reconciliation_detail,
           reconciliation_checked_ms = excluded.reconciliation_checked_ms, detail = excluded.detail,
           origin_device_id = excluded.origin_device_id, schema_version = excluded.schema_version,
           created_ms = excluded.created_ms, updated_at_ms = excluded.updated_at_ms",
        params![
            seq,
            record.effect_id,
            record.execution_id,
            record.capability,
            record.operation,
            record.scope,
            record.input_digest,
            record.approval_id,
            record.idempotency_key,
            record.status,
            recon_result,
            recon_detail,
            recon_checked,
            record.detail,
            record.origin_device_id,
            record.schema_version as i64,
            created_ms,
            now,
        ],
    )
    .map_err(err)?;
    let payload = serde_json::to_string(record).map_err(|e| format!("sqlite: cannot encode effect: {e}"))?;
    let event_id = append_event(
        tx,
        "effect",
        &record.effect_id,
        "effect.transitioned",
        &payload,
        &record.origin_device_id,
        now,
        record.schema_version as i64,
    )?;
    let source = format!("event-{event_id}");
    match record.status.as_str() {
        "failed" => insert_inbox(
            tx,
            "failed",
            record.execution_id.as_deref(),
            Some(&record.effect_id),
            None,
            &source,
            now,
        )?,
        "unknown" => insert_inbox(
            tx,
            "unknown_requires_reconciliation",
            record.execution_id.as_deref(),
            Some(&record.effect_id),
            None,
            &source,
            now,
        )?,
        "reconciled" => {
            auto_close_inbox(
                tx,
                "unknown_requires_reconciliation",
                "effect_id",
                &record.effect_id,
                now,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn upsert_approval_tx(
    tx: &rusqlite::Transaction<'_>,
    record: &ApprovalFileRecord,
) -> Result<(), String> {
    let now = now_ms() as i64;
    // The event type is inferred from the previous SQLite row,
    // so the server does not need to thread a reason through
    // every persist call site: new row -> requested, consumed
    // flip -> consumed, message change -> edited, status change
    // -> decided, binding-only change -> bound.
    let old: Option<(String, bool, Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT status, consumed, message, effect_binding FROM approvals WHERE approval_id = ?1",
            params![record.tool_call_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? != 0,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(err)?;
    let old_created: Option<i64> = tx
        .query_row(
            "SELECT created_ms FROM approvals WHERE approval_id = ?1",
            params![record.tool_call_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(err)?
        .flatten();
    let created_ms = old_created.unwrap_or(now);
    let event_type = match &old {
        None => "approval.requested",
        Some((old_status, old_consumed, old_message, old_binding)) => {
            if !*old_consumed && record.consumed {
                "approval.consumed"
            } else if old_message.as_deref() != record.message.as_deref() {
                "approval.edited"
            } else if old_status != &record.status {
                "approval.decided"
            } else if old_binding.as_deref() != record.effect_binding.as_deref() {
                "approval.bound"
            } else {
                "approval.decided"
            }
        }
    };
    let seq = {
        let suffix = id_suffix(&record.tool_call_id);
        if suffix > 0 {
            suffix
        } else {
            next_seq_for(tx, "approvals", "approval_id")?
        }
    };
    let binding_json = match &record.binding {
        Some(b) => Some(serde_json::to_string(b).map_err(|e| format!("sqlite: cannot encode binding: {e}"))?),
        None => None,
    };
    tx.execute(
        "INSERT INTO approvals (seq, approval_id, tool, status, binding_json, consumed, operation_digest, action_binding, effect_binding, message, origin_device_id, schema_version, created_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT(approval_id) DO UPDATE SET
           seq = excluded.seq, tool = excluded.tool, status = excluded.status,
           binding_json = excluded.binding_json, consumed = excluded.consumed,
           operation_digest = excluded.operation_digest, action_binding = excluded.action_binding,
           effect_binding = excluded.effect_binding, message = excluded.message,
           origin_device_id = excluded.origin_device_id, schema_version = excluded.schema_version,
           created_ms = excluded.created_ms, updated_at_ms = excluded.updated_at_ms",
        params![
            seq,
            record.tool_call_id,
            record.tool,
            record.status,
            binding_json,
            if record.consumed { 1 } else { 0 },
            record.operation_digest,
            record.action_binding,
            record.effect_binding,
            record.message,
            // Approvals predate the P0-A origin field in the
            // file record; new server rows carry "unknown" here
            // only when the caller did not stamp one. The file
            // record has no origin column, so "unknown" is the
            // honest value for rows imported through this path.
            "unknown",
            1i64,
            created_ms,
            now,
        ],
    )
    .map_err(err)?;
    let payload = serde_json::to_string(record).map_err(|e| format!("sqlite: cannot encode approval: {e}"))?;
    let event_id = append_event(
        tx,
        "approval",
        &record.tool_call_id,
        event_type,
        &payload,
        "unknown",
        now,
        1,
    )?;
    let source = format!("event-{event_id}");
    // A pending, Effect-bound approval needs an approval_required
    // row. The binding often lands after the request (the
    // request handler plans the Effect, then binds it), so the
    // trigger is "pending + bound + no open row", not "new row":
    // a second bind write must not duplicate the row.
    if record.status == "pending" && record.effect_binding.is_some() {
        let open: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM inbox WHERE kind = 'approval_required' AND approval_id = ?1 AND resolved_ms IS NULL",
                params![record.tool_call_id],
                |row| row.get(0),
            )
            .map_err(err)?;
        if open == 0 {
            insert_inbox(
                tx,
                "approval_required",
                None,
                None,
                Some(&record.tool_call_id),
                &source,
                now,
            )?;
        }
    }
    if record.status != "pending" || record.consumed {
        auto_close_inbox(
            tx,
            "approval_required",
            "approval_id",
            &record.tool_call_id,
            now,
        )?;
    }
    Ok(())
}

/// One transaction: upsert one execution row + its event (+
/// derived Inbox rows). Callers hold the `Mutex<Connection>`.
pub fn upsert_execution(conn: &mut Connection, record: &ExecutionFileRecord) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    upsert_execution_tx(&tx, record, "execution.created")?;
    tx.commit().map_err(err)?;
    Ok(())
}

pub fn upsert_effect(conn: &mut Connection, record: &EffectFileRecord) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    upsert_effect_tx(&tx, record)?;
    tx.commit().map_err(err)?;
    Ok(())
}

pub fn upsert_approval(conn: &mut Connection, record: &ApprovalFileRecord) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    upsert_approval_tx(&tx, record)?;
    tx.commit().map_err(err)?;
    Ok(())
}

// --- Reads (migrated domains) ---

pub fn list_executions(conn: &Connection) -> Result<Vec<ExecutionFileRecord>, String> {
    let mut stmt = conn
        .prepare("SELECT execution_id, task_id, loop_id, session_id, worker_session_id, workspace_snapshot_id, status, origin_device_id, schema_version FROM executions ORDER BY seq ASC, execution_id ASC")
        .map_err(err)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(ExecutionFileRecord {
                execution_id: row.get(0)?,
                task_id: row.get(1)?,
                loop_id: row.get(2)?,
                session_id: row.get(3)?,
                worker_session_id: row.get(4)?,
                workspace_snapshot_id: row.get(5)?,
                status: row.get(6)?,
                origin_device_id: row.get(7)?,
                schema_version: row.get::<_, i64>(8)? as u64,
            })
        })
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

pub fn list_effects(conn: &Connection) -> Result<Vec<EffectFileRecord>, String> {
    let mut stmt = conn
        .prepare("SELECT effect_id, execution_id, capability, operation, scope, input_digest, approval_id, idempotency_key, status, reconciliation_result, reconciliation_detail, reconciliation_checked_ms, detail, origin_device_id, schema_version FROM effects ORDER BY seq ASC, effect_id ASC")
        .map_err(err)?;
    let rows = stmt
        .query_map([], |row| {
            let (recon_result, recon_detail, recon_checked): (Option<String>, Option<String>, Option<i64>) =
                (row.get(9)?, row.get(10)?, row.get(11)?);
            let reconciliation = match (recon_result, recon_checked) {
                (Some(result), Some(checked_ms)) => Some(crate::effect_store::Reconciliation {
                    result,
                    checked_ms: checked_ms as u64,
                    detail: recon_detail,
                }),
                (Some(result), None) => Some(crate::effect_store::Reconciliation {
                    result,
                    checked_ms: 0,
                    detail: recon_detail,
                }),
                _ => None,
            };
            Ok(EffectFileRecord {
                effect_id: row.get(0)?,
                execution_id: row.get(1)?,
                capability: row.get(2)?,
                operation: row.get(3)?,
                scope: row.get(4)?,
                input_digest: row.get(5)?,
                approval_id: row.get(6)?,
                idempotency_key: row.get(7)?,
                status: row.get(8)?,
                reconciliation,
                detail: row.get(12)?,
                origin_device_id: row.get(13)?,
                schema_version: row.get::<_, i64>(14)? as u64,
            })
        })
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

fn approval_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ApprovalFileRecord> {
    let binding_json: Option<String> = row.get(4)?;
    let binding = match binding_json {
        Some(json) => Some(serde_json::from_str(&json).map_err(|_| rusqlite::Error::InvalidQuery)?),
        None => None,
    };
    Ok(ApprovalFileRecord {
        tool_call_id: row.get(1)?,
        tool: row.get(2)?,
        status: row.get(3)?,
        binding,
        consumed: row.get::<_, i64>(5)? != 0,
        operation_digest: row.get(6)?,
        action_binding: row.get(7)?,
        effect_binding: row.get(8)?,
        message: row.get(9)?,
    })
}

pub fn list_approvals(conn: &Connection) -> Result<Vec<ApprovalFileRecord>, String> {
    let mut stmt = conn
        .prepare("SELECT seq, approval_id, tool, status, binding_json, consumed, operation_digest, action_binding, effect_binding, message FROM approvals ORDER BY seq ASC, approval_id ASC")
        .map_err(err)?;
    let rows = stmt
        .query_map([], approval_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

pub fn get_approval(conn: &Connection, approval_id: &str) -> Result<Option<ApprovalFileRecord>, String> {
    conn.query_row(
        "SELECT seq, approval_id, tool, status, binding_json, consumed, operation_digest, action_binding, effect_binding, message FROM approvals WHERE approval_id = ?1",
        params![approval_id],
        approval_from_row,
    )
    .optional()
    .map_err(err)
}

/// Event row (audit shape, not a domain record).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventRow {
    pub id: i64,
    pub aggregate_kind: String,
    pub aggregate_id: String,
    pub seq: i64,
    pub event_type: String,
    pub payload_json: String,
    pub created_ms: i64,
    pub origin_device_id: String,
    pub schema_version: i64,
}

pub fn list_events(conn: &Connection) -> Result<Vec<EventRow>, String> {
    let mut stmt = conn
        .prepare("SELECT id, aggregate_kind, aggregate_id, seq, event_type, payload_json, created_ms, origin_device_id, schema_version FROM events ORDER BY id ASC")
        .map_err(err)?;
    let rows = stmt
        .query_map([], |row| {
            Ok(EventRow {
                id: row.get(0)?,
                aggregate_kind: row.get(1)?,
                aggregate_id: row.get(2)?,
                seq: row.get(3)?,
                event_type: row.get(4)?,
                payload_json: row.get(5)?,
                created_ms: row.get(6)?,
                origin_device_id: row.get(7)?,
                schema_version: row.get(8)?,
            })
        })
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// Projection equivalence for the migrated domains: folding the
/// event log (last payload per aggregate, in `seq` order) must
/// reproduce the state tables exactly. Inbox / Outbox events
/// are not part of this check (Inbox creation has no event by
/// design; Outbox rows are delivery state, not domain state).
pub fn fold_matches_tables(conn: &Connection) -> Result<bool, String> {
    let events = list_events(conn)?;
    let mut folded_executions: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    let mut folded_effects: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    let mut folded_approvals: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    for event in &events {
        let payload: serde_json::Value =
            serde_json::from_str(&event.payload_json).map_err(|e| format!("sqlite: bad event payload: {e}"))?;
        match event.aggregate_kind.as_str() {
            "execution" => {
                folded_executions.insert(event.aggregate_id.clone(), payload);
            }
            "effect" => {
                folded_effects.insert(event.aggregate_id.clone(), payload);
            }
            "approval" => {
                folded_approvals.insert(event.aggregate_id.clone(), payload);
            }
            _ => {}
        }
    }
    let table_value = |json: &str| -> Result<serde_json::Value, String> {
        serde_json::from_str(json).map_err(|e| format!("sqlite: bad table encoding: {e}"))
    };
    for row in list_executions(conn)? {
        let key = row.execution_id.clone();
        let expected = table_value(&serde_json::to_string(&row).map_err(|e| e.to_string())?)?;
        if folded_executions.get(&key) != Some(&expected) {
            return Ok(false);
        }
    }
    if folded_executions.len() != list_executions(conn)?.len() {
        return Ok(false);
    }
    for row in list_effects(conn)? {
        let key = row.effect_id.clone();
        let expected = table_value(&serde_json::to_string(&row).map_err(|e| e.to_string())?)?;
        if folded_effects.get(&key) != Some(&expected) {
            return Ok(false);
        }
    }
    if folded_effects.len() != list_effects(conn)?.len() {
        return Ok(false);
    }
    for row in list_approvals(conn)? {
        let key = row.tool_call_id.clone();
        let expected = table_value(&serde_json::to_string(&row).map_err(|e| e.to_string())?)?;
        if folded_approvals.get(&key) != Some(&expected) {
            return Ok(false);
        }
    }
    if folded_approvals.len() != list_approvals(conn)?.len() {
        return Ok(false);
    }
    Ok(true)
}

// --- D3 import ---

/// Folded (last-line-wins) JSONL rows for one domain, in file
/// order of their last occurrence.
pub fn fold_executions(records: &[ExecutionFileRecord]) -> Vec<ExecutionFileRecord> {
    let mut map: std::collections::HashMap<String, ExecutionFileRecord> =
        std::collections::HashMap::new();
    for record in records {
        map.insert(record.execution_id.clone(), record.clone());
    }
    let mut rows: Vec<ExecutionFileRecord> = map.into_values().collect();
    rows.sort_by_key(|r| id_suffix(&r.execution_id));
    rows
}

pub fn fold_effects(records: &[EffectFileRecord]) -> Vec<EffectFileRecord> {
    let mut map: std::collections::HashMap<String, EffectFileRecord> =
        std::collections::HashMap::new();
    for record in records {
        map.insert(record.effect_id.clone(), record.clone());
    }
    let mut rows: Vec<EffectFileRecord> = map.into_values().collect();
    rows.sort_by_key(|r| id_suffix(&r.effect_id));
    rows
}

pub fn fold_approvals(records: &[ApprovalFileRecord]) -> Vec<ApprovalFileRecord> {
    let mut map: std::collections::HashMap<String, ApprovalFileRecord> =
        std::collections::HashMap::new();
    for record in records {
        map.insert(record.tool_call_id.clone(), record.clone());
    }
    let mut rows: Vec<ApprovalFileRecord> = map.into_values().collect();
    rows.sort_by_key(|r| id_suffix(&r.tool_call_id));
    rows
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportReport {
    pub executions: usize,
    pub effects: usize,
    pub approvals: usize,
    pub already_done: bool,
}

/// D3: import folded JSONL state in a single transaction.
/// Existing SQLite rows are never overwritten (shadow rows are
/// newer than the JSONL snapshot); only missing aggregates are
/// inserted, each with one synthetic `imported` event
/// (`origin = import`, payload = the imported row). Re-running
/// after `import_done = 1` is a no-op.
pub fn import_folded(
    conn: &mut Connection,
    executions: &[ExecutionFileRecord],
    effects: &[EffectFileRecord],
    approvals: &[ApprovalFileRecord],
    import_ms: u64,
) -> Result<ImportReport, String> {
    if import_done(conn)? {
        return Ok(ImportReport {
            executions: 0,
            effects: 0,
            approvals: 0,
            already_done: true,
        });
    }
    let import_ms = import_ms as i64;
    let tx = conn.transaction().map_err(err)?;
    let mut report = ImportReport {
        executions: 0,
        effects: 0,
        approvals: 0,
        already_done: false,
    };
    for record in executions {
        let seq = {
            let suffix = id_suffix(&record.execution_id);
            if suffix > 0 {
                suffix
            } else {
                next_seq_for(&tx, "executions", "execution_id")?
            }
        };
        let changed = tx
            .execute(
                "INSERT OR IGNORE INTO executions (seq, execution_id, task_id, loop_id, session_id, worker_session_id, workspace_snapshot_id, status, origin_device_id, schema_version, created_ms, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?11)",
                params![
                    seq,
                    record.execution_id,
                    record.task_id,
                    record.loop_id,
                    record.session_id,
                    record.worker_session_id,
                    record.workspace_snapshot_id,
                    record.status,
                    record.origin_device_id,
                    record.schema_version as i64,
                    import_ms,
                ],
            )
            .map_err(err)?;
        if changed == 1 {
            let payload = serde_json::to_string(record).map_err(|e| e.to_string())?;
            append_event(
                &tx,
                "execution",
                &record.execution_id,
                "imported",
                &payload,
                "import",
                import_ms,
                record.schema_version as i64,
            )?;
            report.executions += 1;
        }
    }
    for record in effects {
        let seq = {
            let suffix = id_suffix(&record.effect_id);
            if suffix > 0 {
                suffix
            } else {
                next_seq_for(&tx, "effects", "effect_id")?
            }
        };
        let (recon_result, recon_detail, recon_checked) = match &record.reconciliation {
            Some(r) => (
                Some(r.result.clone()),
                r.detail.clone(),
                Some(r.checked_ms as i64),
            ),
            None => (None, None, None),
        };
        let changed = tx
            .execute(
                "INSERT OR IGNORE INTO effects (seq, effect_id, execution_id, capability, operation, scope, input_digest, approval_id, idempotency_key, status, reconciliation_result, reconciliation_detail, reconciliation_checked_ms, detail, origin_device_id, schema_version, created_ms, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, NULL, ?17)",
                params![
                    seq,
                    record.effect_id,
                    record.execution_id,
                    record.capability,
                    record.operation,
                    record.scope,
                    record.input_digest,
                    record.approval_id,
                    record.idempotency_key,
                    record.status,
                    recon_result,
                    recon_detail,
                    recon_checked,
                    record.detail,
                    record.origin_device_id,
                    record.schema_version as i64,
                    import_ms,
                ],
            )
            .map_err(err)?;
        if changed == 1 {
            let payload = serde_json::to_string(record).map_err(|e| e.to_string())?;
            append_event(
                &tx,
                "effect",
                &record.effect_id,
                "imported",
                &payload,
                "import",
                import_ms,
                record.schema_version as i64,
            )?;
            report.effects += 1;
        }
    }
    for record in approvals {
        let seq = {
            let suffix = id_suffix(&record.tool_call_id);
            if suffix > 0 {
                suffix
            } else {
                next_seq_for(&tx, "approvals", "approval_id")?
            }
        };
        let binding_json = match &record.binding {
            Some(b) => Some(serde_json::to_string(b).map_err(|e| e.to_string())?),
            None => None,
        };
        let changed = tx
            .execute(
                "INSERT OR IGNORE INTO approvals (seq, approval_id, tool, status, binding_json, consumed, operation_digest, action_binding, effect_binding, message, origin_device_id, schema_version, created_ms, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, NULL, ?13)",
                params![
                    seq,
                    record.tool_call_id,
                    record.tool,
                    record.status,
                    binding_json,
                    if record.consumed { 1 } else { 0 },
                    record.operation_digest,
                    record.action_binding,
                    record.effect_binding,
                    record.message,
                    "unknown",
                    0i64,
                    import_ms,
                ],
            )
            .map_err(err)?;
        if changed == 1 {
            let payload = serde_json::to_string(record).map_err(|e| e.to_string())?;
            append_event(
                &tx,
                "approval",
                &record.tool_call_id,
                "imported",
                &payload,
                "import",
                import_ms,
                0,
            )?;
            report.approvals += 1;
        }
    }
    set_import_done(&tx)?;
    tx.commit().map_err(err)?;
    Ok(report)
}

// --- Audit export (D4: generated JSONL, best-effort) ---

fn export_lines(path: &Path, lines: &[String]) -> Result<(), String> {
    let parent = path.parent().unwrap_or(Path::new("."));
    if !parent.as_os_str().is_empty() {
        std::fs::create_dir_all(parent).map_err(|e| format!("sqlite: cannot create export dir: {e}"))?;
    }
    let tmp = path.with_extension("jsonl.tmp");
    let body = if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    };
    std::fs::write(&tmp, body).map_err(|e| format!("sqlite: cannot write export temp: {e}"))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("sqlite: cannot publish export: {e}"))?;
    Ok(())
}

pub fn export_executions(conn: &Connection, path: &Path) -> Result<(), String> {
    let lines: Vec<String> = list_executions(conn)?
        .iter()
        .map(|r| serde_json::to_string(r).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    export_lines(path, &lines)
}

pub fn export_effects(conn: &Connection, path: &Path) -> Result<(), String> {
    let lines: Vec<String> = list_effects(conn)?
        .iter()
        .map(|r| serde_json::to_string(r).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    export_lines(path, &lines)
}

pub fn export_approvals(conn: &Connection, path: &Path) -> Result<(), String> {
    let lines: Vec<String> = list_approvals(conn)?
        .iter()
        .map(|r| serde_json::to_string(r).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    export_lines(path, &lines)
}

// --- Inbox / Outbox ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InboxRow {
    pub inbox_id: String,
    pub kind: String,
    pub execution_id: Option<String>,
    pub effect_id: Option<String>,
    pub approval_id: Option<String>,
    pub source_event_id: String,
    pub created_ms: i64,
    pub resolved_ms: Option<i64>,
    pub resolved_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboxRow {
    pub outbox_id: String,
    pub execution_id: String,
    pub payload_summary_json: String,
    pub destination_kind: String,
    pub status: String,
    pub attempts: i64,
    pub dedup_key: String,
    pub created_ms: i64,
    pub updated_at_ms: i64,
}

fn inbox_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<InboxRow> {
    Ok(InboxRow {
        inbox_id: row.get(0)?,
        kind: row.get(1)?,
        execution_id: row.get(2)?,
        effect_id: row.get(3)?,
        approval_id: row.get(4)?,
        source_event_id: row.get(5)?,
        created_ms: row.get(6)?,
        resolved_ms: row.get(7)?,
        resolved_reason: row.get(8)?,
    })
}

pub fn list_inbox(
    conn: &Connection,
    kind: Option<&str>,
    open_only: bool,
) -> Result<Vec<InboxRow>, String> {
    let sql = match (kind, open_only) {
        (Some(_), true) => "SELECT inbox_id, kind, execution_id, effect_id, approval_id, source_event_id, created_ms, resolved_ms, resolved_reason FROM inbox WHERE kind = ?1 AND resolved_ms IS NULL ORDER BY seq ASC",
        (Some(_), false) => "SELECT inbox_id, kind, execution_id, effect_id, approval_id, source_event_id, created_ms, resolved_ms, resolved_reason FROM inbox WHERE kind = ?1 ORDER BY seq ASC",
        (None, true) => "SELECT inbox_id, kind, execution_id, effect_id, approval_id, source_event_id, created_ms, resolved_ms, resolved_reason FROM inbox WHERE resolved_ms IS NULL ORDER BY seq ASC",
        (None, false) => "SELECT inbox_id, kind, execution_id, effect_id, approval_id, source_event_id, created_ms, resolved_ms, resolved_reason FROM inbox ORDER BY seq ASC",
    };
    let mut stmt = conn.prepare(sql).map_err(err)?;
    let rows = match kind {
        Some(k) => stmt
            .query_map(params![k], inbox_from_row)
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?,
        None => stmt
            .query_map([], inbox_from_row)
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?,
    };
    Ok(rows)
}

pub fn get_inbox(conn: &Connection, inbox_id: &str) -> Result<Option<InboxRow>, String> {
    conn.query_row(
        "SELECT inbox_id, kind, execution_id, effect_id, approval_id, source_event_id, created_ms, resolved_ms, resolved_reason FROM inbox WHERE inbox_id = ?1",
        params![inbox_id],
        inbox_from_row,
    )
    .optional()
    .map_err(err)
}

/// User ack: closes an open row with reason `acked`. Acking an
/// already-resolved row is a no-op returning the row (idempotent
/// ack). The ack itself is an `inbox.acked` event, so the action
/// is auditable; Inbox creation has no event by design (the
/// triggering transition's event already exists).
pub fn ack_inbox(conn: &mut Connection, inbox_id: &str) -> Result<Option<InboxRow>, String> {
    let tx = conn.transaction().map_err(err)?;
    let current: Option<InboxRow> = tx
        .query_row(
            "SELECT inbox_id, kind, execution_id, effect_id, approval_id, source_event_id, created_ms, resolved_ms, resolved_reason FROM inbox WHERE inbox_id = ?1",
            params![inbox_id],
            inbox_from_row,
        )
        .optional()
        .map_err(err)?;
    let Some(mut row) = current else {
        return Ok(None);
    };
    if row.resolved_ms.is_none() {
        let now = now_ms() as i64;
        tx.execute(
            "UPDATE inbox SET resolved_ms = ?1, resolved_reason = 'acked' WHERE inbox_id = ?2",
            params![now, inbox_id],
        )
        .map_err(err)?;
        row.resolved_ms = Some(now);
        row.resolved_reason = Some("acked".to_string());
        let payload = serde_json::to_string(&row).map_err(|e| e.to_string())?;
        append_event(&tx, "inbox", inbox_id, "inbox.acked", &payload, "unknown", now, 1)?;
    }
    tx.commit().map_err(err)?;
    Ok(Some(row))
}

/// P0-R evidence row: one observation of one Effect's
/// concluded state. `source` is `write` (observed at write
/// time, verdict `verified`), `verify` (a later read-only
/// observation), or `reconcile` (the mirror of a reconcile
/// conclusion). `verdict` is `verified`, `mismatch`, or
/// `inconclusive`. `observed_json` is a serialized JSON object
/// describing only what was observed (hashes, lengths, flags,
/// kinds) — never file bytes, credential values, or session
/// text. `schema_version` is the record contract version (1,
/// same as P0-A rows), not the database schema version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceRow {
    pub evidence_id: String,
    pub effect_id: String,
    pub execution_id: Option<String>,
    pub source: String,
    pub verdict: String,
    pub reason: String,
    pub observed_json: String,
    pub checked_ms: i64,
    pub origin_device_id: String,
    pub schema_version: i64,
    pub created_ms: i64,
}

/// Draft for one evidence insert. `observed_json` must already
/// be serialized by the caller (observation site).
pub struct EvidenceDraft<'a> {
    pub effect_id: &'a str,
    pub execution_id: Option<&'a str>,
    pub source: &'a str,
    pub verdict: &'a str,
    pub reason: &'a str,
    pub observed_json: &'a str,
    pub checked_ms: i64,
    pub origin_device_id: &'a str,
    pub schema_version: i64,
}

fn evidence_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EvidenceRow> {
    Ok(EvidenceRow {
        evidence_id: row.get(1)?,
        effect_id: row.get(2)?,
        execution_id: row.get(3)?,
        source: row.get(4)?,
        verdict: row.get(5)?,
        reason: row.get(6)?,
        observed_json: row.get(7)?,
        checked_ms: row.get(8)?,
        origin_device_id: row.get(9)?,
        schema_version: row.get(10)?,
        created_ms: row.get(11)?,
    })
}

fn insert_evidence_tx(
    tx: &rusqlite::Transaction<'_>,
    draft: &EvidenceDraft<'_>,
) -> Result<EvidenceRow, String> {
    let seq: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM evidence",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(err)?;
    let evidence_id = format!("evidence-{seq}");
    tx.execute(
        "INSERT INTO evidence (seq, evidence_id, effect_id, execution_id, source, verdict, reason, observed_json, checked_ms, origin_device_id, schema_version, created_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            seq,
            evidence_id,
            draft.effect_id,
            draft.execution_id,
            draft.source,
            draft.verdict,
            draft.reason,
            draft.observed_json,
            draft.checked_ms,
            draft.origin_device_id,
            draft.schema_version,
            draft.checked_ms,
        ],
    )
    .map_err(err)?;
    Ok(EvidenceRow {
        evidence_id,
        effect_id: draft.effect_id.to_string(),
        execution_id: draft.execution_id.map(str::to_string),
        source: draft.source.to_string(),
        verdict: draft.verdict.to_string(),
        reason: draft.reason.to_string(),
        observed_json: draft.observed_json.to_string(),
        checked_ms: draft.checked_ms,
        origin_device_id: draft.origin_device_id.to_string(),
        schema_version: draft.schema_version,
        created_ms: draft.checked_ms,
    })
}

/// Insert one evidence row in its own transaction. Evidence is
/// derived data: callers treat failure as best-effort (warn,
/// never fail the effect write that produced it).
pub fn insert_evidence(
    conn: &mut Connection,
    draft: &EvidenceDraft<'_>,
) -> Result<EvidenceRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let row = insert_evidence_tx(&tx, draft)?;
    tx.commit().map_err(err)?;
    Ok(row)
}

pub fn list_evidence(conn: &Connection) -> Result<Vec<EvidenceRow>, String> {
    let mut stmt = conn
        .prepare("SELECT seq, evidence_id, effect_id, execution_id, source, verdict, reason, observed_json, checked_ms, origin_device_id, schema_version, created_ms FROM evidence ORDER BY seq ASC")
        .map_err(err)?;
    let rows = stmt
        .query_map([], evidence_from_row)
        .map_err(err)?
        .collect::<Result<Vec<EvidenceRow>, _>>()
        .map_err(err)?;
    Ok(rows)
}

pub fn get_evidence(
    conn: &Connection,
    evidence_id: &str,
) -> Result<Option<EvidenceRow>, String> {
    conn.query_row(
        "SELECT seq, evidence_id, effect_id, execution_id, source, verdict, reason, observed_json, checked_ms, origin_device_id, schema_version, created_ms FROM evidence WHERE evidence_id = ?1",
        params![evidence_id],
        evidence_from_row,
    )
    .optional()
    .map_err(err)
}

pub fn latest_evidence_for_effect(
    conn: &Connection,
    effect_id: &str,
) -> Result<Option<EvidenceRow>, String> {
    conn.query_row(
        "SELECT seq, evidence_id, effect_id, execution_id, source, verdict, reason, observed_json, checked_ms, origin_device_id, schema_version, created_ms FROM evidence WHERE effect_id = ?1 ORDER BY seq DESC LIMIT 1",
        params![effect_id],
        evidence_from_row,
    )
    .optional()
    .map_err(err)
}

/// Latest write-time Evidence (source `write`) for an effect.
/// Verify compares against this baseline, never against a later
/// verify/reconcile row: a drifted file verified once as
/// `mismatch` must verify as `verified` again once restored,
/// which only the write-time baseline can express.
pub fn latest_write_evidence_for_effect(
    conn: &Connection,
    effect_id: &str,
) -> Result<Option<EvidenceRow>, String> {
    conn.query_row(
        "SELECT seq, evidence_id, effect_id, execution_id, source, verdict, reason, observed_json, checked_ms, origin_device_id, schema_version, created_ms FROM evidence WHERE effect_id = ?1 AND source = 'write' ORDER BY seq DESC LIMIT 1",
        params![effect_id],
        evidence_from_row,
    )
    .optional()
    .map_err(err)
}

/// Record one verify observation together with its Inbox
/// side effects, in a single transaction: a `mismatch` verdict
/// opens a `verification_mismatch` row (deduped against an
/// already-open row for the same effect), a `verified` verdict
/// auto-closes any open `verification_mismatch` row for the
/// effect. An `inconclusive` verdict touches no Inbox row.
/// The verify handler uses this; write-time and reconcile
/// mirrors use `insert_evidence` (they never open Inbox rows).
pub fn record_verification(
    conn: &mut Connection,
    draft: &EvidenceDraft<'_>,
) -> Result<EvidenceRow, String> {
    let tx = conn.transaction().map_err(err)?;
    let row = insert_evidence_tx(&tx, draft)?;
    if draft.verdict == "mismatch" {
        let open: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM inbox WHERE kind = 'verification_mismatch' AND effect_id = ?1 AND resolved_ms IS NULL",
                params![draft.effect_id],
                |r| r.get::<_, i64>(0),
            )
            .map_err(err)?;
        if open == 0 {
            let source_event_id = format!("evidence:{}", row.evidence_id);
            insert_inbox(
                &tx,
                "verification_mismatch",
                draft.execution_id,
                Some(draft.effect_id),
                None,
                &source_event_id,
                draft.checked_ms,
            )?;
        }
    } else if draft.verdict == "verified" {
        auto_close_inbox(
            &tx,
            "verification_mismatch",
            "effect_id",
            draft.effect_id,
            draft.checked_ms,
        )?;
    }
    tx.commit().map_err(err)?;
    Ok(row)
}

fn outbox_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxRow> {
    Ok(OutboxRow {
        outbox_id: row.get(0)?,
        execution_id: row.get(1)?,
        payload_summary_json: row.get(2)?,
        destination_kind: row.get(3)?,
        status: row.get(4)?,
        attempts: row.get(5)?,
        dedup_key: row.get(6)?,
        created_ms: row.get(7)?,
        updated_at_ms: row.get(8)?,
    })
}

pub fn list_outbox(conn: &Connection) -> Result<Vec<OutboxRow>, String> {
    let mut stmt = conn
        .prepare("SELECT outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms FROM outbox ORDER BY seq ASC")
        .map_err(err)?;
    let rows = stmt
        .query_map([], outbox_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

pub fn get_outbox(conn: &Connection, outbox_id: &str) -> Result<Option<OutboxRow>, String> {
    conn.query_row(
        "SELECT outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms FROM outbox WHERE outbox_id = ?1",
        params![outbox_id],
        outbox_from_row,
    )
    .optional()
    .map_err(err)
}

/// Enqueue a local-notification outbox row for one execution.
/// Dedup: one row per (execution, destination_kind); re-enqueue
/// returns the existing row with no new event. P0-D has exactly
/// one destination kind, `local_notification`.
pub fn enqueue_outbox(
    conn: &mut Connection,
    execution_id: &str,
    payload_summary_json: &str,
) -> Result<OutboxRow, String> {
    let dedup_key = format!("{execution_id}:local_notification");
    if let Some(existing) = conn
        .query_row(
            "SELECT outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms FROM outbox WHERE dedup_key = ?1",
            params![dedup_key],
            outbox_from_row,
        )
        .optional()
        .map_err(err)?
    {
        return Ok(existing);
    }
    let tx = conn.transaction().map_err(err)?;
    let now = now_ms() as i64;
    let seq = next_seq_for(&tx, "outbox", "outbox_id")?;
    let outbox_id = format!("outbox-{seq}");
    tx.execute(
        "INSERT INTO outbox (seq, outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, 'local_notification', 'pending', 0, ?5, ?6, ?6)",
        params![seq, outbox_id, execution_id, payload_summary_json, dedup_key, now],
    )
    .map_err(err)?;
    let row = OutboxRow {
        outbox_id: outbox_id.clone(),
        execution_id: execution_id.to_string(),
        payload_summary_json: payload_summary_json.to_string(),
        destination_kind: "local_notification".to_string(),
        status: "pending".to_string(),
        attempts: 0,
        dedup_key,
        created_ms: now,
        updated_at_ms: now,
    };
    let payload = serde_json::to_string(&row).map_err(|e| e.to_string())?;
    append_event(&tx, "outbox", &outbox_id, "outbox.enqueued", &payload, "unknown", now, 1)?;
    tx.commit().map_err(err)?;
    Ok(row)
}

fn set_outbox_status(
    tx: &rusqlite::Transaction<'_>,
    outbox_id: &str,
    status: &str,
    event_type: &str,
) -> Result<Option<OutboxRow>, String> {
    let current: Option<OutboxRow> = tx
        .query_row(
            "SELECT outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms FROM outbox WHERE outbox_id = ?1",
            params![outbox_id],
            outbox_from_row,
        )
        .optional()
        .map_err(err)?;
    let Some(mut row) = current else {
        return Ok(None);
    };
    if row.status != "pending" {
        return Ok(Some(row));
    }
    let now = now_ms() as i64;
    tx.execute(
        "UPDATE outbox SET status = ?1, attempts = attempts + 1, updated_at_ms = ?2 WHERE outbox_id = ?3",
        params![status, now, outbox_id],
    )
    .map_err(err)?;
    row.status = status.to_string();
    row.attempts += 1;
    row.updated_at_ms = now;
    let payload = serde_json::to_string(&row).map_err(|e| e.to_string())?;
    append_event(tx, "outbox", outbox_id, event_type, &payload, "unknown", now, 1)?;
    Ok(Some(row))
}

/// Mark a pending row sent or failed (one attempt). A failed row
/// is never retried automatically: it stays `failed` until an
/// explicit `requeue_outbox` call.
pub fn deliver_outbox(
    conn: &mut Connection,
    outbox_id: &str,
    success: bool,
) -> Result<Option<OutboxRow>, String> {
    let tx = conn.transaction().map_err(err)?;
    let row = set_outbox_status(&tx, outbox_id, if success { "sent" } else { "failed" }, if success {
        "outbox.sent"
    } else {
        "outbox.failed"
    })?;
    tx.commit().map_err(err)?;
    Ok(row)
}

/// Explicit requeue only: `failed -> pending` with an
/// `outbox.requeued` event. No route exposes this in P0-D.
pub fn requeue_outbox(conn: &mut Connection, outbox_id: &str) -> Result<Option<OutboxRow>, String> {
    let tx = conn.transaction().map_err(err)?;
    let current: Option<OutboxRow> = tx
        .query_row(
            "SELECT outbox_id, execution_id, payload_summary_json, destination_kind, status, attempts, dedup_key, created_ms, updated_at_ms FROM outbox WHERE outbox_id = ?1",
            params![outbox_id],
            outbox_from_row,
        )
        .optional()
        .map_err(err)?;
    let Some(mut row) = current else {
        return Ok(None);
    };
    if row.status == "failed" {
        let now = now_ms() as i64;
        tx.execute(
            "UPDATE outbox SET status = 'pending', updated_at_ms = ?1 WHERE outbox_id = ?2",
            params![now, outbox_id],
        )
        .map_err(err)?;
        row.status = "pending".to_string();
        row.updated_at_ms = now;
        let payload = serde_json::to_string(&row).map_err(|e| e.to_string())?;
        append_event(&tx, "outbox", outbox_id, "outbox.requeued", &payload, "unknown", now, 1)?;
    }
    tx.commit().map_err(err)?;
    Ok(Some(row))
}

/// The P0-D delivery sink: log only, no external call. Marks
/// the row sent through the same transactional path as any
/// delivery, so the log line and the state change cannot split.
pub fn deliver_outbox_via_log_sink(
    conn: &mut Connection,
    outbox_id: &str,
) -> Result<Option<OutboxRow>, String> {
    let row = deliver_outbox(conn, outbox_id, true)?;
    if let Some(row) = &row {
        eprintln!(
            "outbox local_notification delivered: execution {} summary {}",
            row.execution_id, row.payload_summary_json
        );
    }
    Ok(row)
}

// --- P0-E tables: test-seam inserts only (no production driver) ---
//
// P0-E drives these tables through the transactional functions
// further below (recovery scan, takeover, transition, snapshot
// creation); the seams stay for unit-level tests. Row structs
// are Serialize so event payloads and HTTP responses can carry
// the post-state row verbatim.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssignmentRow {
    pub assignment_id: String,
    pub execution_id: Option<String>,
    pub worker_session_id: Option<String>,
    pub origin_device_id: String,
    pub status: String,
    pub detail: Option<String>,
    pub lease_expires_ms: Option<i64>,
    pub heartbeat_ms: Option<i64>,
    pub schema_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerSessionRow {
    pub worker_session_id: String,
    pub origin_device_id: String,
    pub status: String,
    /// P0-E scan / shutdown note ("process ended" /
    /// "clean shutdown"); NULL for rows that never ended that
    /// way. Added as a nullable column, not a v2 migration.
    pub detail: Option<String>,
    pub schema_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointRow {
    pub checkpoint_id: String,
    pub execution_id: Option<String>,
    pub execution_seq: i64,
    pub kind: String,
    pub payload_json: String,
    pub origin_device_id: String,
    pub schema_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotRow {
    pub snapshot_id: String,
    pub content_sha256: String,
    pub files_root: String,
    pub connector_ids_json: String,
    pub device_ids_json: String,
    pub policy_binding_ids_json: String,
    pub memory_scope: String,
    pub instruction_digest: String,
    pub origin_device_id: String,
    pub schema_version: i64,
    /// P3-A: frozen binding (NULL when the execution is
    /// unbound): the Location id, the Worktree id, and the
    /// Location's root_sha256 fingerprint.
    pub binding_location_id: Option<String>,
    pub binding_worktree_id: Option<String>,
    pub binding_root_digest: Option<String>,
}

fn seam_seq(conn: &Connection, table: &str, id: &str) -> Result<i64, String> {
    let suffix = id_suffix(id);
    if suffix > 0 {
        return Ok(suffix);
    }
    let max: Option<i64> = conn
        .query_row(&format!("SELECT MAX(seq) FROM {table}"), [], |row| row.get(0))
        .map_err(err)?;
    Ok(max.unwrap_or(0) + 1)
}

/// Test seam: insert an assignment row directly. P0-D leaves
/// lease / heartbeat columns empty in production; the seam may
/// set them to prove the columns exist. No production event.
pub fn insert_assignment(conn: &mut Connection, row: &AssignmentRow) -> Result<(), String> {
    let seq = seam_seq(conn, "assignments", &row.assignment_id)?;
    let now = now_ms() as i64;
    conn.execute(
        "INSERT INTO assignments (seq, assignment_id, execution_id, worker_session_id, origin_device_id, status, detail, lease_expires_ms, heartbeat_ms, schema_version, created_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)",
        params![seq, row.assignment_id, row.execution_id, row.worker_session_id, row.origin_device_id, row.status, row.detail, row.lease_expires_ms, row.heartbeat_ms, row.schema_version, now],
    )
    .map_err(err)?;
    Ok(())
}

pub fn insert_worker_session(conn: &mut Connection, row: &WorkerSessionRow) -> Result<(), String> {
    let seq = seam_seq(conn, "worker_sessions", &row.worker_session_id)?;
    let now = now_ms() as i64;
    conn.execute(
        "INSERT INTO worker_sessions (seq, worker_session_id, origin_device_id, status, detail, schema_version, created_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        params![seq, row.worker_session_id, row.origin_device_id, row.status, row.detail, row.schema_version, now],
    )
    .map_err(err)?;
    Ok(())
}

pub fn insert_checkpoint(conn: &mut Connection, row: &CheckpointRow) -> Result<(), String> {
    let seq = seam_seq(conn, "checkpoints", &row.checkpoint_id)?;
    let now = now_ms() as i64;
    conn.execute(
        "INSERT INTO checkpoints (seq, checkpoint_id, execution_id, execution_seq, kind, payload_json, origin_device_id, schema_version, created_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![seq, row.checkpoint_id, row.execution_id, row.execution_seq, row.kind, row.payload_json, row.origin_device_id, row.schema_version, now],
    )
    .map_err(err)?;
    Ok(())
}

pub fn insert_snapshot(conn: &mut Connection, row: &SnapshotRow) -> Result<(), String> {
    let seq = seam_seq(conn, "workspace_snapshots", &row.snapshot_id)?;
    let now = now_ms() as i64;
    conn.execute(
        "INSERT INTO workspace_snapshots (seq, snapshot_id, content_sha256, files_root, connector_ids_json, device_ids_json, policy_binding_ids_json, memory_scope, instruction_digest, origin_device_id, schema_version, binding_location_id, binding_worktree_id, binding_root_digest, created_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![seq, row.snapshot_id, row.content_sha256, row.files_root, row.connector_ids_json, row.device_ids_json, row.policy_binding_ids_json, row.memory_scope, row.instruction_digest, row.origin_device_id, row.schema_version, row.binding_location_id, row.binding_worktree_id, row.binding_root_digest, now],
    )
    .map_err(err)?;
    Ok(())
}

pub fn count_table(conn: &Connection, table: &str) -> Result<i64, String> {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))
        .map_err(err)
}

/// Test-only integrity check. Production startup never runs
/// this (cost); a damaged DB surfaces as fail-closed errors.
pub fn integrity_check(conn: &Connection) -> Result<String, String> {
    conn.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
        .map_err(err)
}

/// D2 shadow comparison (test seam, never a production route):
/// folded JSONL rows vs SQLite rows, field by field. Returns
/// human-readable mismatches; empty means shadow == JSONL.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ShadowReport {
    pub mismatches: Vec<String>,
}

pub fn shadow_compare(
    conn: &Connection,
    executions: &[ExecutionFileRecord],
    effects: &[EffectFileRecord],
    approvals: &[ApprovalFileRecord],
) -> Result<ShadowReport, String> {
    let mut report = ShadowReport::default();
    let sqlite_executions = list_executions(conn)?;
    let folded_executions = fold_executions(executions);
    for row in &folded_executions {
        match sqlite_executions.iter().find(|r| r.execution_id == row.execution_id) {
            Some(found) if found == row => {}
            Some(found) => report.mismatches.push(format!(
                "execution {} differs: jsonl {found:?} vs sqlite {row:?}",
                row.execution_id
            )),
            None => report
                .mismatches
                .push(format!("execution {} missing in sqlite", row.execution_id)),
        }
    }
    let sqlite_effects = list_effects(conn)?;
    for row in &fold_effects(effects) {
        match sqlite_effects.iter().find(|r| r.effect_id == row.effect_id) {
            Some(found) if found == row => {}
            Some(found) => report.mismatches.push(format!(
                "effect {} differs: jsonl {found:?} vs sqlite {row:?}",
                row.effect_id
            )),
            None => report
                .mismatches
                .push(format!("effect {} missing in sqlite", row.effect_id)),
        }
    }
    let sqlite_approvals = list_approvals(conn)?;
    for row in &fold_approvals(approvals) {
        match sqlite_approvals.iter().find(|r| r.tool_call_id == row.tool_call_id) {
            Some(found) if found == row => {}
            Some(found) => report.mismatches.push(format!(
                "approval {} differs: jsonl {found:?} vs sqlite {row:?}",
                row.tool_call_id
            )),
            None => report
                .mismatches
                .push(format!("approval {} missing in sqlite", row.tool_call_id)),
        }
    }
    Ok(report)
}

// --- P0-E: recovery scan, takeover, execution transition,
// workspace-snapshot creation, checkpoints ---
//
// All production writes here are single transactions: the state
// row, its event row and any derived Assignment / Checkpoint
// rows commit together or not at all. Event payloads are
// post-state snapshots, exactly as in P0-D; `fold_matches_tables`
// ignores the `assignment` / `worker_session` / `checkpoint` /
// `workspace_snapshot` aggregate kinds, so the new event types
// cannot break the execution / effect / approval fold — but the
// execution events below (reassigned, transitioned) DO carry the
// full execution record and therefore MUST match the executions
// table row, or the fold fails. Both writers below guarantee
// that by serializing the same record they just wrote.

/// Canonical JSON for snapshot hashing (plan §3.2): object keys
/// sorted recursively, UTF-8, compact, no whitespace. Sorting is
/// done explicitly so the guarantee does not depend on
/// serde_json's map implementation.
pub fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = String::from("{");
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).expect("canonical key"));
                out.push(':');
                out.push_str(&canonical_json(&map[*key]));
            }
            out.push('}');
            out
        }
        serde_json::Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", parts.join(","))
        }
        other => serde_json::to_string(other).expect("canonical value"),
    }
}

/// SHA-256 hex, single implementation for the whole crate
/// (operation_binding owns the hasher).
pub fn sha256_hex(data: &[u8]) -> String {
    crate::operation_binding::sha256_hex(data)
}

/// Content hash of a snapshot row (plan §3.2): canonical JSON
/// of the row's content fields exactly as stored (the *_json
/// fields as strings, not re-parsed), excluding content_sha256
/// itself and the snapshot_id / seq / time columns.
pub fn snapshot_content_sha256(row: &SnapshotRow) -> String {
    let value = serde_json::json!({
        "binding_location_id": row.binding_location_id,
        "binding_root_digest": row.binding_root_digest,
        "binding_worktree_id": row.binding_worktree_id,
        "connector_ids_json": row.connector_ids_json,
        "device_ids_json": row.device_ids_json,
        "files_root": row.files_root,
        "instruction_digest": row.instruction_digest,
        "memory_scope": row.memory_scope,
        "policy_binding_ids_json": row.policy_binding_ids_json,
    });
    sha256_hex(canonical_json(&value).as_bytes())
}

fn execution_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ExecutionFileRecord> {
    Ok(ExecutionFileRecord {
        execution_id: row.get(0)?,
        task_id: row.get(1)?,
        loop_id: row.get(2)?,
        session_id: row.get(3)?,
        worker_session_id: row.get(4)?,
        workspace_snapshot_id: row.get(5)?,
        status: row.get(6)?,
        origin_device_id: row.get(7)?,
        schema_version: row.get::<_, i64>(8)? as u64,
    })
}

fn effect_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<EffectFileRecord> {
    let (recon_result, recon_detail, recon_checked): (Option<String>, Option<String>, Option<i64>) =
        (row.get(9)?, row.get(10)?, row.get(11)?);
    let reconciliation = match (recon_result, recon_checked) {
        (Some(result), Some(checked_ms)) => Some(crate::effect_store::Reconciliation {
            result,
            checked_ms: checked_ms as u64,
            detail: recon_detail,
        }),
        (Some(result), None) => Some(crate::effect_store::Reconciliation {
            result,
            checked_ms: 0,
            detail: recon_detail,
        }),
        _ => None,
    };
    Ok(EffectFileRecord {
        effect_id: row.get(0)?,
        execution_id: row.get(1)?,
        capability: row.get(2)?,
        operation: row.get(3)?,
        scope: row.get(4)?,
        input_digest: row.get(5)?,
        approval_id: row.get(6)?,
        idempotency_key: row.get(7)?,
        status: row.get(8)?,
        reconciliation,
        detail: row.get(12)?,
        origin_device_id: row.get(13)?,
        schema_version: row.get::<_, i64>(14)? as u64,
    })
}

fn assignment_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AssignmentRow> {
    Ok(AssignmentRow {
        assignment_id: row.get(0)?,
        execution_id: row.get(1)?,
        worker_session_id: row.get(2)?,
        origin_device_id: row.get(3)?,
        status: row.get(4)?,
        detail: row.get(5)?,
        lease_expires_ms: row.get(6)?,
        heartbeat_ms: row.get(7)?,
        schema_version: row.get(8)?,
    })
}

fn worker_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkerSessionRow> {
    Ok(WorkerSessionRow {
        worker_session_id: row.get(0)?,
        origin_device_id: row.get(1)?,
        status: row.get(2)?,
        detail: row.get(3)?,
        schema_version: row.get(4)?,
    })
}

fn checkpoint_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<CheckpointRow> {
    Ok(CheckpointRow {
        checkpoint_id: row.get(0)?,
        execution_id: row.get(1)?,
        execution_seq: row.get(2)?,
        kind: row.get(3)?,
        payload_json: row.get(4)?,
        origin_device_id: row.get(5)?,
        schema_version: row.get(6)?,
    })
}

fn snapshot_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SnapshotRow> {
    Ok(SnapshotRow {
        snapshot_id: row.get(0)?,
        content_sha256: row.get(1)?,
        files_root: row.get(2)?,
        connector_ids_json: row.get(3)?,
        device_ids_json: row.get(4)?,
        policy_binding_ids_json: row.get(5)?,
        memory_scope: row.get(6)?,
        instruction_digest: row.get(7)?,
        origin_device_id: row.get(8)?,
        schema_version: row.get(9)?,
        binding_location_id: row.get(10)?,
        binding_worktree_id: row.get(11)?,
        binding_root_digest: row.get(12)?,
    })
}

const EXECUTION_COLS: &str = "execution_id, task_id, loop_id, session_id, worker_session_id, workspace_snapshot_id, status, origin_device_id, schema_version";
const EFFECT_COLS: &str = "effect_id, execution_id, capability, operation, scope, input_digest, approval_id, idempotency_key, status, reconciliation_result, reconciliation_detail, reconciliation_checked_ms, detail, origin_device_id, schema_version";
const ASSIGNMENT_COLS: &str = "assignment_id, execution_id, worker_session_id, origin_device_id, status, detail, lease_expires_ms, heartbeat_ms, schema_version";
const WORKER_COLS: &str = "worker_session_id, origin_device_id, status, detail, schema_version";
const CHECKPOINT_COLS: &str = "checkpoint_id, execution_id, execution_seq, kind, payload_json, origin_device_id, schema_version";
const SNAPSHOT_COLS: &str = "snapshot_id, content_sha256, files_root, connector_ids_json, device_ids_json, policy_binding_ids_json, memory_scope, instruction_digest, origin_device_id, schema_version, binding_location_id, binding_worktree_id, binding_root_digest";

/// One transaction-local seq: the id suffix when the id carries
/// one, else MAX(seq) + 1 (mirrors the seam convention).
fn tx_seq(tx: &rusqlite::Transaction<'_>, table: &str, id: &str) -> Result<i64, String> {
    let suffix = id_suffix(id);
    if suffix > 0 {
        return Ok(suffix);
    }
    let max: Option<i64> = tx
        .query_row(&format!("SELECT MAX(seq) FROM {table}"), [], |row| row.get(0))
        .map_err(err)?;
    Ok(max.unwrap_or(0) + 1)
}

/// Next `prefix-N` id in a table: one past the largest numeric
/// suffix present. Computed inside the caller's transaction, so
/// under the single-writer connection mutex it cannot race.
fn next_prefixed_id(
    tx: &rusqlite::Transaction<'_>,
    table: &str,
    id_col: &str,
    prefix: &str,
) -> Result<String, String> {
    let mut stmt = tx
        .prepare(&format!("SELECT {id_col} FROM {table}"))
        .map_err(err)?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(err)?
        .collect::<Result<Vec<String>, _>>()
        .map_err(err)?;
    let max = ids
        .iter()
        .filter_map(|id| id.strip_prefix(prefix)?.parse::<i64>().ok())
        .max()
        .unwrap_or(0);
    Ok(format!("{prefix}{}", max + 1))
}

fn insert_worker_session_tx(
    tx: &rusqlite::Transaction<'_>,
    row: &WorkerSessionRow,
) -> Result<(), String> {
    let seq = tx_seq(tx, "worker_sessions", &row.worker_session_id)?;
    let now = now_ms() as i64;
    tx.execute(
        "INSERT INTO worker_sessions (seq, worker_session_id, origin_device_id, status, detail, schema_version, created_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        params![seq, row.worker_session_id, row.origin_device_id, row.status, row.detail, row.schema_version, now],
    )
    .map_err(err)?;
    Ok(())
}

fn insert_assignment_tx(
    tx: &rusqlite::Transaction<'_>,
    row: &AssignmentRow,
) -> Result<(), String> {
    let seq = tx_seq(tx, "assignments", &row.assignment_id)?;
    let now = now_ms() as i64;
    tx.execute(
        "INSERT INTO assignments (seq, assignment_id, execution_id, worker_session_id, origin_device_id, status, detail, lease_expires_ms, heartbeat_ms, schema_version, created_ms, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)",
        params![seq, row.assignment_id, row.execution_id, row.worker_session_id, row.origin_device_id, row.status, row.detail, row.lease_expires_ms, row.heartbeat_ms, row.schema_version, now],
    )
    .map_err(err)?;
    Ok(())
}

fn insert_checkpoint_tx(
    tx: &rusqlite::Transaction<'_>,
    row: &CheckpointRow,
) -> Result<(), String> {
    let seq = tx_seq(tx, "checkpoints", &row.checkpoint_id)?;
    let now = now_ms() as i64;
    tx.execute(
        "INSERT INTO checkpoints (seq, checkpoint_id, execution_id, execution_seq, kind, payload_json, origin_device_id, schema_version, created_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![seq, row.checkpoint_id, row.execution_id, row.execution_seq, row.kind, row.payload_json, row.origin_device_id, row.schema_version, now],
    )
    .map_err(err)?;
    Ok(())
}

fn insert_snapshot_tx(
    tx: &rusqlite::Transaction<'_>,
    row: &SnapshotRow,
) -> Result<(), String> {
    let seq = tx_seq(tx, "workspace_snapshots", &row.snapshot_id)?;
    let now = now_ms() as i64;
    tx.execute(
        "INSERT INTO workspace_snapshots (seq, snapshot_id, content_sha256, files_root, connector_ids_json, device_ids_json, policy_binding_ids_json, memory_scope, instruction_digest, origin_device_id, schema_version, binding_location_id, binding_worktree_id, binding_root_digest, created_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
        params![seq, row.snapshot_id, row.content_sha256, row.files_root, row.connector_ids_json, row.device_ids_json, row.policy_binding_ids_json, row.memory_scope, row.instruction_digest, row.origin_device_id, row.schema_version, row.binding_location_id, row.binding_worktree_id, row.binding_root_digest, now],
    )
    .map_err(err)?;
    Ok(())
}

fn append_assignment_event(
    tx: &rusqlite::Transaction<'_>,
    event_type: &str,
    row: &AssignmentRow,
) -> Result<(), String> {
    let payload =
        serde_json::to_string(row).map_err(|e| format!("sqlite: cannot encode assignment: {e}"))?;
    append_event(
        tx,
        "assignment",
        &row.assignment_id,
        event_type,
        &payload,
        &row.origin_device_id,
        now_ms() as i64,
        row.schema_version,
    )?;
    Ok(())
}

fn execution_row_tx(
    tx: &rusqlite::Transaction<'_>,
    execution_id: &str,
) -> Result<Option<ExecutionFileRecord>, String> {
    tx.query_row(
        &format!("SELECT {EXECUTION_COLS} FROM executions WHERE execution_id = ?1"),
        params![execution_id],
        execution_from_row,
    )
    .optional()
    .map_err(err)
}

fn execution_seq_tx(tx: &rusqlite::Transaction<'_>, execution_id: &str) -> Result<i64, String> {
    tx.query_row(
        "SELECT seq FROM executions WHERE execution_id = ?1",
        params![execution_id],
        |row| row.get(0),
    )
    .map_err(err)
}

fn worker_row_tx(
    tx: &rusqlite::Transaction<'_>,
    worker_session_id: &str,
) -> Result<Option<WorkerSessionRow>, String> {
    tx.query_row(
        &format!("SELECT {WORKER_COLS} FROM worker_sessions WHERE worker_session_id = ?1"),
        params![worker_session_id],
        worker_from_row,
    )
    .optional()
    .map_err(err)
}

/// Assignments currently driving an execution: status
/// `assigned` or `active`. `lost` and `done` are history, never
/// drivers (plan §2.2 recoverability definition).
fn active_assignments_tx(
    tx: &rusqlite::Transaction<'_>,
    execution_id: &str,
) -> Result<Vec<AssignmentRow>, String> {
    let mut stmt = tx
        .prepare(&format!(
            "SELECT {ASSIGNMENT_COLS} FROM assignments WHERE execution_id = ?1 AND status IN ('assigned', 'active') ORDER BY seq ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![execution_id], assignment_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

fn active_assignments_for_worker_tx(
    tx: &rusqlite::Transaction<'_>,
    worker_session_id: &str,
) -> Result<Vec<AssignmentRow>, String> {
    let mut stmt = tx
        .prepare(&format!(
            "SELECT {ASSIGNMENT_COLS} FROM assignments WHERE worker_session_id = ?1 AND status IN ('assigned', 'active') ORDER BY seq ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![worker_session_id], assignment_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

fn effects_for_execution_tx(
    tx: &rusqlite::Transaction<'_>,
    execution_id: &str,
) -> Result<Vec<EffectFileRecord>, String> {
    let mut stmt = tx
        .prepare(&format!(
            "SELECT {EFFECT_COLS} FROM effects WHERE execution_id = ?1 ORDER BY seq ASC, effect_id ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![execution_id], effect_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

fn mark_assignment_tx(
    tx: &rusqlite::Transaction<'_>,
    row: &AssignmentRow,
    status: &str,
    detail: &str,
    event_type: Option<&str>,
) -> Result<AssignmentRow, String> {
    let now = now_ms() as i64;
    let mut updated = row.clone();
    updated.status = status.to_string();
    updated.detail = Some(detail.to_string());
    tx.execute(
        "UPDATE assignments SET status = ?1, detail = ?2, updated_at_ms = ?3 WHERE assignment_id = ?4",
        params![status, detail, now, row.assignment_id],
    )
    .map_err(err)?;
    if let Some(event_type) = event_type {
        append_assignment_event(tx, event_type, &updated)?;
    }
    Ok(updated)
}

// --- P0-E reads (connection-level, for routes and tests) ---

pub fn get_execution(conn: &Connection, execution_id: &str) -> Result<Option<ExecutionFileRecord>, String> {
    conn.query_row(
        &format!("SELECT {EXECUTION_COLS} FROM executions WHERE execution_id = ?1"),
        params![execution_id],
        execution_from_row,
    )
    .optional()
    .map_err(err)
}

pub fn list_assignments(conn: &Connection) -> Result<Vec<AssignmentRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {ASSIGNMENT_COLS} FROM assignments ORDER BY seq ASC, assignment_id ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map([], assignment_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

pub fn get_assignment(conn: &Connection, assignment_id: &str) -> Result<Option<AssignmentRow>, String> {
    conn.query_row(
        &format!("SELECT {ASSIGNMENT_COLS} FROM assignments WHERE assignment_id = ?1"),
        params![assignment_id],
        assignment_from_row,
    )
    .optional()
    .map_err(err)
}

pub fn list_worker_sessions(conn: &Connection) -> Result<Vec<WorkerSessionRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {WORKER_COLS} FROM worker_sessions ORDER BY seq ASC, worker_session_id ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map([], worker_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// The single driving assignment of an execution, if any.
/// At most one exists in practice (takeover 409s on a second
/// driver); the earliest wins if history ever disagrees.
pub fn active_assignment_for_execution(
    conn: &Connection,
    execution_id: &str,
) -> Result<Option<AssignmentRow>, String> {
    conn.query_row(
        &format!(
            "SELECT {ASSIGNMENT_COLS} FROM assignments WHERE execution_id = ?1 AND status IN ('assigned', 'active') ORDER BY seq ASC LIMIT 1"
        ),
        params![execution_id],
        assignment_from_row,
    )
    .optional()
    .map_err(err)
}

/// Driving assignment per execution, for the derived read
/// fields on executions/list (one query, not one per row).
pub fn active_assignments_map(
    conn: &Connection,
) -> Result<std::collections::HashMap<String, String>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT execution_id, assignment_id FROM assignments WHERE execution_id IS NOT NULL AND status IN ('assigned', 'active') ORDER BY seq ASC",
        )
        .map_err(err)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(err)?
        .collect::<Result<Vec<(String, String)>, _>>()
        .map_err(err)?;
    let mut map = std::collections::HashMap::new();
    for (execution_id, assignment_id) in rows {
        map.entry(execution_id).or_insert(assignment_id);
    }
    Ok(map)
}

pub fn list_checkpoints_for_execution(
    conn: &Connection,
    execution_id: &str,
) -> Result<Vec<CheckpointRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {CHECKPOINT_COLS} FROM checkpoints WHERE execution_id = ?1 ORDER BY seq ASC, checkpoint_id ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map(params![execution_id], checkpoint_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// Latest `execution.plan_position` checkpoint for an
/// execution: highest checkpoint seq (insertion order).
/// NOTE on plan §4.3's "按 execution_seq 最大": a checkpoint's
/// execution_seq is its execution's seq, constant across all
/// checkpoints of one execution, so it cannot order them; the
/// checkpoint's own seq is the insertion order and is what
/// "latest" means here. Checkpoint rows are append-only.
pub fn latest_plan_position(
    conn: &Connection,
    execution_id: &str,
) -> Result<Option<CheckpointRow>, String> {
    conn.query_row(
        &format!(
            "SELECT {CHECKPOINT_COLS} FROM checkpoints WHERE execution_id = ?1 AND kind = 'execution.plan_position' ORDER BY seq DESC LIMIT 1"
        ),
        params![execution_id],
        checkpoint_from_row,
    )
    .optional()
    .map_err(err)
}

pub fn get_snapshot(conn: &Connection, snapshot_id: &str) -> Result<Option<SnapshotRow>, String> {
    conn.query_row(
        &format!("SELECT {SNAPSHOT_COLS} FROM workspace_snapshots WHERE snapshot_id = ?1"),
        params![snapshot_id],
        snapshot_from_row,
    )
    .optional()
    .map_err(err)
}

// --- P0-E writes ---

/// P0-E startup recovery scan (plan §2.2, SQLite mode only).
/// One transaction: every still-`active` WorkerSession →
/// `ended` (detail "process ended"); each such session's
/// `assigned` / `active` Assignments → `lost` (detail
/// "worker stopped", with an `assignment.lost` event each);
/// then a new `active` WorkerSession for this process, whose
/// id is returned and becomes the process's current worker.
/// Assignments under sessions that were already ended, or
/// with no session at all, are untouched: the scan closes
/// exactly the sessions it ends, nothing it did not see die.
/// A failure leaves the transaction uncommitted; the caller
/// fails the startup closed (no partial scan ever lands).
pub fn recovery_scan(conn: &mut Connection, origin_device_id: &str) -> Result<String, String> {
    let tx = conn.transaction().map_err(err)?;
    let now = now_ms() as i64;
    let ended: Vec<String> = {
        let mut stmt = tx
            .prepare("SELECT worker_session_id FROM worker_sessions WHERE status = 'active'")
            .map_err(err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        rows
    };
    for worker_id in &ended {
        tx.execute(
            "UPDATE worker_sessions SET status = 'ended', detail = 'process ended', updated_at_ms = ?1 WHERE worker_session_id = ?2",
            params![now, worker_id],
        )
        .map_err(err)?;
    }
    for worker_id in &ended {
        for row in active_assignments_for_worker_tx(&tx, worker_id)? {
            mark_assignment_tx(&tx, &row, "lost", "worker stopped", Some("assignment.lost"))?;
        }
    }
    let new_id = next_prefixed_id(&tx, "worker_sessions", "worker_session_id", "worker-")?;
    insert_worker_session_tx(
        &tx,
        &WorkerSessionRow {
            worker_session_id: new_id.clone(),
            origin_device_id: origin_device_id.to_string(),
            status: "active".to_string(),
            detail: None,
            schema_version: 1,
        },
    )?;
    tx.commit().map_err(err)?;
    Ok(new_id)
}

/// Clean-shutdown counterpart of the scan (plan §2.1), in
/// one transaction. JUDGEMENT CALL on the plan's singular
/// "current WorkerSession": this gateway is the sole writer
/// of its database, so every still-`active` session belongs
/// to this process's lineage (scan worker plus any takeover
/// workers it created) — ending only the one remembered as
/// "current" would leave a takeover worker active and its
/// assignment un-lost after a clean exit, breaking the plan's
/// own convergence claim ("after shutdown, old workers are
/// all ended, their assignments all lost"). So shutdown ends
/// ALL active sessions (detail "clean shutdown") and loses
/// all their active assignments (detail "worker stopped",
/// with `assignment.lost` events each), converging exactly to
/// the post-scan state, one step earlier. Best-effort at the
/// call site: a failure here never blocks exiting, and a
/// kill -9 simply skips this and lets the next scan close it.
pub fn shutdown_cleanup(conn: &mut Connection) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    let now = now_ms() as i64;
    let active: Vec<String> = {
        let mut stmt = tx
            .prepare("SELECT worker_session_id FROM worker_sessions WHERE status = 'active'")
            .map_err(err)?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        rows
    };
    for worker_id in &active {
        tx.execute(
            "UPDATE worker_sessions SET status = 'ended', detail = 'clean shutdown', updated_at_ms = ?1 WHERE worker_session_id = ?2",
            params![now, worker_id],
        )
        .map_err(err)?;
    }
    for worker_id in &active {
        for row in active_assignments_for_worker_tx(&tx, worker_id)? {
            mark_assignment_tx(&tx, &row, "lost", "worker stopped", Some("assignment.lost"))?;
        }
    }
    tx.commit().map_err(err)?;
    Ok(())
}

/// One transaction: upsert one execution row (with its
/// `execution.created` event and derived Inbox rows) plus its
/// creation-time workspace snapshot row. The snapshot id is
/// chosen by the caller (the server ties it to the execution
/// number, so no second counter can race the execution id).
pub fn upsert_execution_with_snapshot(
    conn: &mut Connection,
    record: &ExecutionFileRecord,
    snapshot: &SnapshotRow,
) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    upsert_execution_tx(&tx, record, "execution.created")?;
    insert_snapshot_tx(&tx, snapshot)?;
    tx.commit().map_err(err)?;
    Ok(())
}

/// Takeover refusal: a stable error code the route maps to
/// 409, versus an internal failure the route maps to 500.
/// Nothing is written on a refusal: the whole operation is
/// one transaction that only commits on success, so even a
/// worker row created for an omitted id rolls back with it.
#[derive(Debug)]
pub enum TakeoverError {
    Refused(&'static str),
    Internal(String),
}

impl TakeoverError {
    pub fn code(&self) -> String {
        match self {
            TakeoverError::Refused(code) => (*code).to_string(),
            TakeoverError::Internal(_) => "takeover_failed".to_string(),
        }
    }
}

fn takeover_internal<T>(result: Result<T, String>) -> Result<T, TakeoverError> {
    result.map_err(TakeoverError::Internal)
}

/// Outcome of a successful takeover: the new driving
/// assignment, the execution row after its worker update, the
/// plan-position checkpoint written at takeover, and any old
/// driving assignments this takeover superseded (normally
/// empty: validation refuses when one exists, see below).
#[derive(Debug, Clone)]
pub struct TakeoverResult {
    pub assignment: AssignmentRow,
    pub execution: ExecutionFileRecord,
    pub checkpoint: CheckpointRow,
    pub superseded: Vec<AssignmentRow>,
}

/// POST /assignments/takeover (plan §2.3), one transaction.
/// Validation, in order: execution exists, execution is not
/// terminal, the named worker (when given) exists and is
/// active, no driving assignment exists. The last check is
/// strict: ANY `assigned` / `active` row for the execution
/// refuses with `assignment_active` — that is what "no double
/// driving" means, and what makes a second takeover 409. The
/// supersede loop after validation is therefore defensive
/// only: it marks any driving row the check somehow missed
/// `lost` ("superseded by takeover") rather than ever leaving
/// two drivers, but under the strict check it finds none.
/// Success writes: the new `active` assignment, the
/// execution's worker update (with an `execution.reassigned`
/// event carrying the post-state record, no Inbox side
/// effects — takeover changes no status, so no status trigger
/// may fire), and one `execution.plan_position` checkpoint
/// snapshotting the Effect list (terminal keys completed, the
/// first non-terminal key next, or null when none remains).
/// Takeover changes no Execution status, no Conversation
/// Session and no Effect row.
pub fn takeover_assignment(
    conn: &mut Connection,
    execution_id: &str,
    worker_session_id: Option<&str>,
    origin_device_id: &str,
) -> Result<TakeoverResult, TakeoverError> {
    let tx = conn.transaction().map_err(err).map_err(TakeoverError::Internal)?;
    let run = || -> Result<TakeoverResult, TakeoverError> {
        let Some(mut execution) = takeover_internal(execution_row_tx(&tx, execution_id))? else {
            return Err(TakeoverError::Refused("execution_not_found"));
        };
        if crate::execution_store::execution_status_group(&execution.status)
            == Some("terminal")
        {
            return Err(TakeoverError::Refused("execution_terminal"));
        }
        let worker_id = match worker_session_id {
            Some(id) => {
                match takeover_internal(worker_row_tx(&tx, id))? {
                    None => return Err(TakeoverError::Refused("worker_session_not_found")),
                    Some(row) if row.status != "active" => {
                        return Err(TakeoverError::Refused("worker_session_not_active"))
                    }
                    Some(_) => id.to_string(),
                }
            }
            None => {
                let id = takeover_internal(next_prefixed_id(
                    &tx,
                    "worker_sessions",
                    "worker_session_id",
                    "worker-",
                ))?;
                takeover_internal(insert_worker_session_tx(
                    &tx,
                    &WorkerSessionRow {
                        worker_session_id: id.clone(),
                        origin_device_id: origin_device_id.to_string(),
                        status: "active".to_string(),
                        detail: None,
                        schema_version: 1,
                    },
                ))?;
                id
            }
        };
        let active = takeover_internal(active_assignments_tx(&tx, execution_id))?;
        if !active.is_empty() {
            return Err(TakeoverError::Refused("assignment_active"));
        }
        // Defensive only (see doc comment): validation above
        // already refused on any driving row.
        let mut superseded = Vec::new();
        for row in &active {
            superseded.push(takeover_internal(mark_assignment_tx(
                &tx,
                row,
                "lost",
                "superseded by takeover",
                Some("assignment.lost"),
            ))?);
        }
        let assignment = AssignmentRow {
            assignment_id: takeover_internal(next_prefixed_id(
                &tx,
                "assignments",
                "assignment_id",
                "assignment-",
            ))?,
            execution_id: Some(execution_id.to_string()),
            worker_session_id: Some(worker_id.clone()),
            origin_device_id: origin_device_id.to_string(),
            status: "active".to_string(),
            detail: None,
            lease_expires_ms: None,
            heartbeat_ms: None,
            schema_version: 1,
        };
        takeover_internal(insert_assignment_tx(&tx, &assignment))?;
        takeover_internal(append_assignment_event(&tx, "assignment.created", &assignment))?;
        // Worker update only: no status change, so no Inbox
        // trigger may fire. The event payload is the updated
        // record, keeping fold(events) == tables exact.
        let now = now_ms() as i64;
        takeover_internal(
            tx.execute(
                "UPDATE executions SET worker_session_id = ?1, updated_at_ms = ?2 WHERE execution_id = ?3",
                params![worker_id, now, execution_id],
            )
            .map_err(err),
        )?;
        execution.worker_session_id = Some(worker_id);
        let payload = serde_json::to_string(&execution)
            .map_err(|e| TakeoverError::Internal(format!("sqlite: cannot encode execution: {e}")))?;
        takeover_internal(append_event(
            &tx,
            "execution",
            execution_id,
            "execution.reassigned",
            &payload,
            &execution.origin_device_id,
            now,
            execution.schema_version as i64,
        ))?;
        // Plan-position checkpoint from the Effect list.
        let effects = takeover_internal(effects_for_execution_tx(&tx, execution_id))?;
        let mut completed_keys: Vec<String> = Vec::new();
        let mut next_effect_key: Option<String> = None;
        for effect in &effects {
            let terminal = crate::effect_store::is_terminal_effect_status(&effect.status);
            match (&effect.idempotency_key, terminal) {
                (Some(key), true) => completed_keys.push(key.clone()),
                (Some(key), false) if next_effect_key.is_none() => {
                    next_effect_key = Some(key.clone());
                }
                _ => {}
            }
        }
        let checkpoint = CheckpointRow {
            checkpoint_id: takeover_internal(next_prefixed_id(
                &tx,
                "checkpoints",
                "checkpoint_id",
                "checkpoint-",
            ))?,
            execution_id: Some(execution_id.to_string()),
            execution_seq: takeover_internal(execution_seq_tx(&tx, execution_id))?,
            kind: "execution.plan_position".to_string(),
            payload_json: serde_json::to_string(&serde_json::json!({
                "next_effect_key": next_effect_key,
                "completed_keys": completed_keys,
            }))
            .map_err(|e| TakeoverError::Internal(format!("sqlite: cannot encode checkpoint: {e}")))?,
            origin_device_id: origin_device_id.to_string(),
            schema_version: 1,
        };
        takeover_internal(insert_checkpoint_tx(&tx, &checkpoint))?;
        Ok(TakeoverResult { assignment, execution, checkpoint, superseded })
    };
    let result = run();
    match result {
        Ok(outcome) => {
            tx.commit().map_err(err).map_err(TakeoverError::Internal)?;
            Ok(outcome)
        }
        Err(error) => Err(error),
    }
}

/// Execution transition refusal: unknown id vs illegal edge.
/// The unknown-word case (400) is checked by the route before
/// calling here, from the same vocabulary function.
#[derive(Debug)]
pub enum ExecutionTransitionError {
    NotFound,
    Illegal,
    Internal(String),
}

/// POST /executions/transition (plan §4.2), one transaction:
/// the execution row (with an `execution.transitioned` event
/// and the P0-D Inbox status triggers — a real status change,
/// so review/failed rows open and close exactly as a status
/// write always did) plus, on a terminal landing, its driving
/// assignments → `done` (work under this assignment ended;
/// distinct from takeover's `lost`). Non-terminal landings
/// leave assignments untouched.
pub fn transition_execution(
    conn: &mut Connection,
    execution_id: &str,
    status: &str,
) -> Result<ExecutionFileRecord, ExecutionTransitionError> {
    let tx = conn.transaction().map_err(err).map_err(ExecutionTransitionError::Internal)?;
    let run = || -> Result<ExecutionFileRecord, ExecutionTransitionError> {
        let Some(mut record) = execution_row_tx(&tx, execution_id)
            .map_err(ExecutionTransitionError::Internal)?
        else {
            return Err(ExecutionTransitionError::NotFound);
        };
        if !crate::execution_store::valid_execution_transition(&record.status, status) {
            return Err(ExecutionTransitionError::Illegal);
        }
        record.status = status.to_string();
        upsert_execution_tx(&tx, &record, "execution.transitioned")
            .map_err(ExecutionTransitionError::Internal)?;
        if crate::execution_store::execution_status_group(status) == Some("terminal") {
            let now = now_ms() as i64;
            for row in active_assignments_tx(&tx, execution_id)
                .map_err(ExecutionTransitionError::Internal)?
            {
                tx.execute(
                    "UPDATE assignments SET status = 'done', updated_at_ms = ?1 WHERE assignment_id = ?2",
                    params![now, row.assignment_id],
                )
                .map_err(err)
                .map_err(ExecutionTransitionError::Internal)?;
            }
        }
        Ok(record)
    };
    match run() {
        Ok(record) => {
            tx.commit().map_err(err).map_err(ExecutionTransitionError::Internal)?;
            Ok(record)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect_store::Reconciliation;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("mock-sqlite-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir.join("gateway.sqlite3")
    }

    fn execution(id: &str, status: &str) -> ExecutionFileRecord {
        ExecutionFileRecord {
            execution_id: id.to_string(),
            task_id: Some("task-1".to_string()),
            loop_id: None,
            session_id: None,
            worker_session_id: None,
            workspace_snapshot_id: None,
            status: status.to_string(),
            origin_device_id: "mac-local".to_string(),
            schema_version: 1,
        }
    }

    fn effect(id: &str, status: &str) -> EffectFileRecord {
        EffectFileRecord {
            effect_id: id.to_string(),
            execution_id: Some("execution-1".to_string()),
            capability: "fs".to_string(),
            operation: "write".to_string(),
            scope: "a.txt".to_string(),
            input_digest: Some("abc".to_string()),
            approval_id: Some("approval-1".to_string()),
            status: status.to_string(),
            idempotency_key: Some(crate::effect_store::idempotency_key(
                "execution-1",
                "fs",
                "write",
                "a.txt",
                "abc",
            )),
            origin_device_id: "mac-local".to_string(),
            schema_version: 1,
            reconciliation: None,
            detail: None,
        }
    }

    fn approval(id: &str, status: &str) -> ApprovalFileRecord {
        ApprovalFileRecord {
            tool_call_id: id.to_string(),
            tool: "fs.write".to_string(),
            status: status.to_string(),
            binding: None,
            consumed: false,
            operation_digest: Some("digest-1".to_string()),
            action_binding: None,
            effect_binding: Some("effect-1".to_string()),
            message: Some("please write".to_string()),
        }
    }

    #[test]
    fn p0d_schema_applies_cleanly_and_reapply_is_idempotent() {
        let path = temp_db("schema");
        let mut conn = open_db(&path).expect("open");
        assert_eq!(schema_version(&conn).expect("version"), 2);
        assert!(!import_done(&conn).expect("import flag"));
        // Re-applying migrations changes nothing.
        apply_migrations(&mut conn).expect("reapply");
        assert_eq!(schema_version(&conn).expect("version again"), 2);
        // db_meta holds the two bootstrap rows (schema version
        // + import flag); every other table starts empty.
        assert_eq!(count_table(&conn, "db_meta").expect("count"), 2);
        for table in [
            "events",
            "executions",
            "effects",
            "approvals",
            "assignments",
            "workspace_snapshots",
            "worker_sessions",
            "checkpoints",
            "inbox",
            "outbox",
        ] {
            assert_eq!(count_table(&conn, table).expect("count"), 0, "{table}");
        }
        // WAL + FULL are the durable settings for every table.
        let journal: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .expect("journal_mode");
        assert_eq!(journal, "wal");
        let sync: i64 = conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .expect("synchronous");
        // synchronous=FULL reads back as 2.
        assert_eq!(sync, 2);
        assert_eq!(integrity_check(&conn).expect("integrity"), "ok");
    }

    #[test]
    fn p0d_upserts_events_fold_back_to_tables() {
        let path = temp_db("fold");
        let mut conn = open_db(&path).expect("open");
        upsert_execution(&mut conn, &execution("execution-1", "running")).expect("exec");
        upsert_effect(&mut conn, &effect("effect-1", "planned")).expect("effect");
        upsert_approval(&mut conn, &approval("approval-1", "pending")).expect("approval");
        // Drive the effect through transitions; each is an event.
        let mut moved = effect("effect-1", "authorized");
        upsert_effect(&mut conn, &moved).expect("authorized");
        moved.status = "started".to_string();
        upsert_effect(&mut conn, &moved).expect("started");
        moved.status = "unknown".to_string();
        upsert_effect(&mut conn, &moved).expect("unknown");
        moved.status = "reconciled".to_string();
        moved.reconciliation = Some(Reconciliation {
            result: "completed".to_string(),
            checked_ms: 42,
            detail: None,
        });
        upsert_effect(&mut conn, &moved).expect("reconciled");
        assert!(fold_matches_tables(&conn).expect("fold"));
        let events = list_events(&conn).expect("events");
        // Per-aggregate seq is monotone from 1, global id order
        // is the insertion order, never wall-clock order.
        let effect_events: Vec<&EventRow> = events
            .iter()
            .filter(|e| e.aggregate_kind == "effect")
            .collect();
        let seqs: Vec<i64> = effect_events.iter().map(|e| e.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
        assert!(events.windows(2).all(|w| w[0].id < w[1].id));
        // Inbox: unknown created a row, reconciled auto-closed it.
        let inbox = list_inbox(&conn, Some("unknown_requires_reconciliation"), false).expect("inbox");
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].resolved_reason.as_deref(), Some("auto"));
        // Approval pending + bound created approval_required;
        // deciding it auto-closes that row.
        let inbox = list_inbox(&conn, Some("approval_required"), false).expect("approval inbox");
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].resolved_ms, None);
        let mut decided = approval("approval-1", "approved");
        decided.consumed = false;
        upsert_approval(&mut conn, &decided).expect("decide");
        let inbox = list_inbox(&conn, Some("approval_required"), false).expect("approval inbox 2");
        assert_eq!(inbox[0].resolved_reason.as_deref(), Some("auto"));
        assert!(fold_matches_tables(&conn).expect("fold after decide"));
    }

    #[test]
    fn p0d_failed_inbox_closes_only_by_ack() {
        let path = temp_db("failed-inbox");
        let mut conn = open_db(&path).expect("open");
        upsert_effect(&mut conn, &effect("effect-1", "failed")).expect("failed");
        let open = list_inbox(&conn, Some("failed"), true).expect("open failed");
        assert_eq!(open.len(), 1);
        // A later non-failed write to the same effect does NOT
        // auto-close a failed row (failed is ack-only).
        let mut moved = effect("effect-1", "planned");
        moved.idempotency_key = None;
        upsert_effect(&mut conn, &moved).expect("replan");
        let open = list_inbox(&conn, Some("failed"), true).expect("open failed 2");
        assert_eq!(open.len(), 1);
        let acked = ack_inbox(&mut conn, &open[0].inbox_id)
            .expect("ack")
            .expect("row");
        assert_eq!(acked.resolved_reason.as_deref(), Some("acked"));
        assert!(list_inbox(&conn, Some("failed"), true).expect("open 3").is_empty());
        // Ack is idempotent: a second ack returns the row, no
        // second event.
        let events_before = list_events(&conn).expect("events").len();
        ack_inbox(&mut conn, &open[0].inbox_id).expect("ack again");
        assert_eq!(list_events(&conn).expect("events 2").len(), events_before);
    }

    #[test]
    fn p0d_outbox_dedup_no_auto_retry_and_explicit_requeue() {
        let path = temp_db("outbox");
        let mut conn = open_db(&path).expect("open");
        let first = enqueue_outbox(&mut conn, "execution-1", "{\"result\":\"ok\"}").expect("enqueue");
        assert_eq!(first.status, "pending");
        let again = enqueue_outbox(&mut conn, "execution-1", "{\"result\":\"other\"}").expect("dedup");
        assert_eq!(again.outbox_id, first.outbox_id);
        assert_eq!(again.payload_summary_json, "{\"result\":\"ok\"}");
        let failed = deliver_outbox(&mut conn, &first.outbox_id, false)
            .expect("fail")
            .expect("row");
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.attempts, 1);
        // No automatic retry: the row stays failed, attempts
        // frozen, until an explicit requeue.
        let still = get_outbox(&conn, &first.outbox_id).expect("get").expect("row");
        assert_eq!(still.status, "failed");
        assert_eq!(still.attempts, 1);
        let queued = requeue_outbox(&mut conn, &first.outbox_id).expect("requeue").expect("row");
        assert_eq!(queued.status, "pending");
        let sent = deliver_outbox_via_log_sink(&mut conn, &first.outbox_id)
            .expect("sink")
            .expect("row");
        assert_eq!(sent.status, "sent");
        assert_eq!(sent.attempts, 2);
    }

    #[test]
    fn p0d_import_folds_preserves_old_rows_and_is_idempotent() {
        let path = temp_db("import");
        let mut conn = open_db(&path).expect("open");
        // Shadow row first: import must never overwrite it.
        upsert_execution(&mut conn, &execution("execution-1", "running")).expect("shadow");
        let mut old_execution = execution("execution-1", "failed");
        old_execution.origin_device_id = "unknown".to_string();
        old_execution.schema_version = 0;
        let old_effect = effect("effect-9", "completed");
        let mut old_effect = old_effect;
        old_effect.origin_device_id = "unknown".to_string();
        old_effect.schema_version = 0;
        // The imported row must not share the sample's
        // idempotency key with the later duplicate-key probe.
        old_effect.idempotency_key = Some("imported-old-key".to_string());
        let jsonl_executions = vec![old_execution, execution("execution-2", "running")];
        let report = import_folded(
            &mut conn,
            &fold_executions(&jsonl_executions),
            &fold_effects(&[old_effect]),
            &fold_approvals(&[approval("approval-3", "pending")]),
            1_700_000_000_000,
        )
        .expect("import");
        assert!(!report.already_done);
        // execution-1 was already a shadow row: not re-imported.
        assert_eq!(report.executions, 1);
        assert_eq!(report.effects, 1);
        assert_eq!(report.approvals, 1);
        assert!(import_done(&conn).expect("import flag set"));
        // Shadow row survived verbatim; imported execution-2
        // has NULL created_ms (honest unknown) in the table.
        let executions = list_executions(&conn).expect("list");
        let shadow = executions.iter().find(|r| r.execution_id == "execution-1").expect("shadow row");
        assert_eq!(shadow.status, "running");
        let created: Option<i64> = conn
            .query_row(
                "SELECT created_ms FROM executions WHERE execution_id = 'execution-2'",
                [],
                |row| row.get(0),
            )
            .expect("created_ms");
        assert_eq!(created, None);
        // Every imported aggregate has exactly one imported event.
        let all_events = list_events(&conn).expect("events");
        let imported: Vec<&EventRow> = all_events
            .iter()
            .filter(|e| e.event_type == "imported")
            .collect();
        assert_eq!(imported.len(), 3);
        assert!(imported.iter().all(|e| e.origin_device_id == "import"));
        // Counters take the max of both sources: imported
        // effect-9 pushes the effect counter past 9.
        let (next_exec, next_effect, next_appr) = next_numbers(&conn).expect("counters");
        assert_eq!(next_exec, 3);
        assert_eq!(next_effect, 10);
        assert_eq!(next_appr, 4);
        assert!(fold_matches_tables(&conn).expect("fold after import"));
        // Re-running import is a no-op (single transaction,
        // import_done gate), even with different input.
        let again = import_folded(&mut conn, &[], &[], &[], 1_700_000_000_001).expect("reimport");
        assert!(again.already_done);
        assert_eq!(list_executions(&conn).expect("list 2").len(), 2);
        // Idempotency key UNIQUE is a DB-level backstop: a
        // second effect row with the same key fails the write.
        let mut dup = effect("effect-2", "planned");
        upsert_effect(&mut conn, &dup).expect("first key row");
        dup.effect_id = "effect-3".to_string();
        assert!(upsert_effect(&mut conn, &dup).is_err());
        assert_eq!(integrity_check(&conn).expect("integrity"), "ok");
    }

    #[test]
    fn p0d_import_keeps_both_device_label_sets_verbatim() {
        let path = temp_db("labels");
        let mut conn = open_db(&path).expect("open");
        // P0-D imports device effects verbatim, never relabels:
        // the old (devices, set) triple and the new
        // (device, reminders.set_completed) triple coexist.
        let mut old_device = effect("effect-1", "completed");
        old_device.capability = "devices".to_string();
        old_device.operation = "set".to_string();
        old_device.scope = "action-1".to_string();
        let mut new_device = effect("effect-2", "completed");
        new_device.capability = "device".to_string();
        new_device.operation = "reminders.set_completed".to_string();
        new_device.scope = "reminder-1".to_string();
        new_device.idempotency_key = Some("other-key".to_string());
        import_folded(&mut conn, &[], &fold_effects(&[old_device, new_device]), &[], 1000)
            .expect("import");
        let effects = list_effects(&conn).expect("list");
        assert_eq!(effects[0].capability, "devices");
        assert_eq!(effects[0].operation, "set");
        assert_eq!(effects[1].capability, "device");
        assert_eq!(effects[1].operation, "reminders.set_completed");
    }

    #[test]
    fn p0d_shadow_compare_and_export_roundtrip() {
        let path = temp_db("shadow-export");
        let mut conn = open_db(&path).expect("open");
        upsert_execution(&mut conn, &execution("execution-1", "running")).expect("exec");
        upsert_effect(&mut conn, &effect("effect-1", "planned")).expect("effect");
        upsert_approval(&mut conn, &approval("approval-1", "pending")).expect("approval");
        let report = shadow_compare(
            &conn,
            &[execution("execution-1", "running")],
            &[effect("effect-1", "planned")],
            &[approval("approval-1", "pending")],
        )
        .expect("compare");
        assert!(report.mismatches.is_empty(), "{:?}", report.mismatches);
        // A divergent JSONL row is reported, not silently fixed.
        let divergent = shadow_compare(
            &conn,
            &[execution("execution-1", "failed")],
            &[],
            &[],
        )
        .expect("compare 2");
        assert_eq!(divergent.mismatches.len(), 1);
        // Export: SQLite state -> JSONL -> load_* reads it back.
        let dir = path.parent().unwrap().to_path_buf();
        export_executions(&conn, &dir.join("executions.jsonl")).expect("export exec");
        export_effects(&conn, &dir.join("effects.jsonl")).expect("export effects");
        export_approvals(&conn, &dir.join("approvals.jsonl")).expect("export approvals");
        assert_eq!(crate::execution_store::load_executions(&dir.join("executions.jsonl")).len(), 1);
        assert_eq!(crate::effect_store::load_effects(&dir.join("effects.jsonl")).len(), 1);
        assert_eq!(crate::approval_store::load_records(&dir.join("approvals.jsonl")).len(), 1);
    }

    #[test]
    fn p0d_p0e_tables_exist_and_seam_rows_insert() {
        let path = temp_db("p0e-tables");
        let mut conn = open_db(&path).expect("open");
        // P0-D only builds these tables; P0-E drives them. The
        // seam proves the columns (including the reserved lease
        // columns, empty in P0-D production) exist and accept rows.
        insert_assignment(
            &mut conn,
            &AssignmentRow {
                assignment_id: "assignment-1".to_string(),
                execution_id: Some("execution-1".to_string()),
                worker_session_id: None,
                origin_device_id: "mac-local".to_string(),
                status: "assigned".to_string(),
                detail: None,
                lease_expires_ms: None,
                heartbeat_ms: None,
                schema_version: 1,
            },
        )
        .expect("assignment");
        insert_worker_session(
            &mut conn,
            &WorkerSessionRow {
                worker_session_id: "worker-1".to_string(),
                origin_device_id: "mac-local".to_string(),
                status: "active".to_string(),
                detail: None,
                schema_version: 1,
            },
        )
        .expect("worker");
        insert_checkpoint(
            &mut conn,
            &CheckpointRow {
                checkpoint_id: "checkpoint-1".to_string(),
                execution_id: Some("execution-1".to_string()),
                execution_seq: 1,
                kind: "paused".to_string(),
                payload_json: "{}".to_string(),
                origin_device_id: "mac-local".to_string(),
                schema_version: 1,
            },
        )
        .expect("checkpoint");
        insert_snapshot(
            &mut conn,
            &SnapshotRow {
                snapshot_id: "snapshot-1".to_string(),
                content_sha256: "abc".to_string(),
                files_root: "/tmp/root".to_string(),
                connector_ids_json: "[]".to_string(),
                device_ids_json: "[]".to_string(),
                policy_binding_ids_json: "[]".to_string(),
                memory_scope: "session".to_string(),
                instruction_digest: "def".to_string(),
                origin_device_id: "mac-local".to_string(),
                schema_version: 1,
                binding_location_id: None,
                binding_worktree_id: None,
                binding_root_digest: None,
            },
        )
        .expect("snapshot");
        assert_eq!(count_table(&conn, "assignments").expect("count"), 1);
        assert_eq!(count_table(&conn, "worker_sessions").expect("count"), 1);
        assert_eq!(count_table(&conn, "checkpoints").expect("count"), 1);
        assert_eq!(count_table(&conn, "workspace_snapshots").expect("count"), 1);
        // A failed transaction leaves no partial rows (atomicity
        // is the durability primitive the kill-9 test also leans on).
        {
            let mut tx_conn = open_db(&path).expect("reopen");
            let tx = tx_conn.transaction().expect("tx");
            tx.execute(
                "INSERT INTO assignments (seq, assignment_id, execution_id, worker_session_id, origin_device_id, status, detail, lease_expires_ms, heartbeat_ms, schema_version, created_ms, updated_at_ms) VALUES (99, 'assignment-99', NULL, NULL, 'mac-local', 'assigned', NULL, NULL, NULL, 1, 0, 0)",
                [],
            )
            .expect("insert in tx");
            // Deliberately drop without commit: rollback.
            drop(tx);
        }
        assert_eq!(count_table(&conn, "assignments").expect("count after rollback"), 1);
        // WAL replay: a second connection sees committed rows.
        let conn2 = open_db(&path).expect("reopen 2");
        assert_eq!(count_table(&conn2, "assignments").expect("count conn2"), 1);
        assert_eq!(integrity_check(&conn2).expect("integrity"), "ok");
    }

    #[test]
    fn p0d_event_payloads_carry_no_new_privacy_fields() {
        let path = temp_db("privacy");
        let mut conn = open_db(&path).expect("open");
        upsert_approval(&mut conn, &approval("approval-1", "pending")).expect("approval");
        let events = list_events(&conn).expect("events");
        assert_eq!(events.len(), 1);
        let payload: serde_json::Value =
            serde_json::from_str(&events[0].payload_json).expect("payload json");
        // The event payload field set is exactly the approval
        // file-record field set (Task 7 message included, same
        // as the JSONL line); no credential / body / context field.
        let keys: Vec<&str> = payload
            .as_object()
            .expect("object")
            .keys()
            .map(|k| k.as_str())
            .collect();
        for forbidden in ["secret", "password", "token", "api_key", "body", "context"] {
            assert!(!keys.contains(&forbidden), "{keys:?}");
        }
        assert!(keys.contains(&"message"));
    }

    // --- P0-E unit tests (seam-level; HTTP串联见 tests/p0e_recovery.rs) ---

    fn effect_with_key(id: &str, execution_id: &str, status: &str, key: &str) -> EffectFileRecord {
        EffectFileRecord {
            effect_id: id.to_string(),
            execution_id: Some(execution_id.to_string()),
            capability: "fs".to_string(),
            operation: "write".to_string(),
            scope: format!("{id}.txt"),
            input_digest: Some(format!("digest-{id}")),
            approval_id: None,
            status: status.to_string(),
            idempotency_key: Some(key.to_string()),
            origin_device_id: "mac-local".to_string(),
            schema_version: 1,
            reconciliation: None,
            detail: None,
        }
    }

    #[test]
    fn p0e_canonical_json_sorts_keys_and_snapshot_hash_is_stable() {
        let value: serde_json::Value =
            serde_json::from_str(r#"{"b":1,"a":{"d":2,"c":3}}"#).expect("json");
        assert_eq!(canonical_json(&value), r#"{"a":{"c":3,"d":2},"b":1}"#);
        let row = SnapshotRow {
            snapshot_id: "snapshot-1".to_string(),
            content_sha256: String::new(),
            files_root: "/tmp/root".to_string(),
            connector_ids_json: "[]".to_string(),
            device_ids_json: "[]".to_string(),
            policy_binding_ids_json: "[]".to_string(),
            memory_scope: "default".to_string(),
            instruction_digest: "abc".to_string(),
            origin_device_id: "mac-local".to_string(),
            schema_version: 1,
            binding_location_id: None,
            binding_worktree_id: None,
            binding_root_digest: None,
        };
        let hash = snapshot_content_sha256(&row);
        assert_eq!(hash, snapshot_content_sha256(&row));
        assert_eq!(hash.len(), 64);
        let mut changed = row.clone();
        changed.policy_binding_ids_json = "[\"network-rules:sha256:x\"]".to_string();
        assert_ne!(hash, snapshot_content_sha256(&changed));
    }

    #[test]
    fn p0e_migration_backfills_worker_detail_column() {
        // A database migrated by P0-D: schema version 1, but the
        // worker_sessions table predates the detail column.
        let dir = std::env::temp_dir()
            .join(format!("mock-sqlite-p0e-old-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("gateway.sqlite3");
        {
            let conn = Connection::open(&path).expect("open raw");
            conn.execute_batch(
                "CREATE TABLE db_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO db_meta VALUES ('db_schema_version', '1'), ('import_done', '0');
                 CREATE TABLE worker_sessions (seq INTEGER NOT NULL, worker_session_id TEXT PRIMARY KEY, origin_device_id TEXT NOT NULL, status TEXT NOT NULL, schema_version INTEGER NOT NULL, created_ms INTEGER, updated_at_ms INTEGER);",
            )
            .expect("old schema");
        }
        let mut conn = open_db(&path).expect("reopen");
        apply_migrations(&mut conn).expect("migrate");
        let mut stmt = conn
            .prepare("PRAGMA table_info(worker_sessions)")
            .expect("pragma");
        let names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        assert!(names.contains(&"detail".to_string()), "{names:?}");
        // P0-R: the v1 fixture migrates to v2 (evidence table)
        // as well as gaining the detail column.
        assert_eq!(schema_version(&conn).expect("version"), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn p3a_migration_backfills_snapshot_binding_columns() {
        // A database migrated before P3-A: schema version 2,
        // but the workspace_snapshots table predates the three
        // binding columns. The backfill adds them nullable and
        // the version stays 2 (column backfill, not a v3).
        let dir = std::env::temp_dir()
            .join(format!("mock-sqlite-p3a-old-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("gateway.sqlite3");
        {
            let conn = Connection::open(&path).expect("open raw");
            conn.execute_batch(
                "CREATE TABLE db_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO db_meta VALUES ('db_schema_version', '2'), ('import_done', '0');
                 CREATE TABLE workspace_snapshots (seq INTEGER NOT NULL, snapshot_id TEXT PRIMARY KEY, content_sha256 TEXT NOT NULL, files_root TEXT NOT NULL, connector_ids_json TEXT NOT NULL, device_ids_json TEXT NOT NULL, policy_binding_ids_json TEXT NOT NULL, memory_scope TEXT NOT NULL, instruction_digest TEXT NOT NULL, origin_device_id TEXT NOT NULL, schema_version INTEGER NOT NULL, created_ms INTEGER);",
            )
            .expect("old schema");
        }
        let mut conn = open_db(&path).expect("reopen");
        apply_migrations(&mut conn).expect("migrate");
        let mut stmt = conn
            .prepare("PRAGMA table_info(workspace_snapshots)")
            .expect("pragma");
        let names: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .expect("query")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        assert!(names.contains(&"binding_location_id".to_string()), "{names:?}");
        assert!(names.contains(&"binding_worktree_id".to_string()), "{names:?}");
        assert!(names.contains(&"binding_root_digest".to_string()), "{names:?}");
        assert_eq!(schema_version(&conn).expect("version"), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn p3a_snapshot_hash_covers_binding_keys() {
        let row = SnapshotRow {
            snapshot_id: "snapshot-1".to_string(),
            content_sha256: String::new(),
            files_root: "/tmp/root".to_string(),
            connector_ids_json: "[]".to_string(),
            device_ids_json: "[]".to_string(),
            policy_binding_ids_json: "[]".to_string(),
            memory_scope: "default".to_string(),
            instruction_digest: "abc".to_string(),
            origin_device_id: "mac-local".to_string(),
            schema_version: 1,
            binding_location_id: None,
            binding_worktree_id: None,
            binding_root_digest: None,
        };
        let hash = snapshot_content_sha256(&row);
        let mut bound = row.clone();
        bound.binding_location_id = Some("location-1".to_string());
        bound.binding_worktree_id = Some("worktree-1".to_string());
        bound.binding_root_digest = Some("deadbeef".to_string());
        assert_ne!(hash, snapshot_content_sha256(&bound));
    }

    #[test]
    fn p0e_recovery_scan_ends_only_active_and_loses_their_assignments() {
        let path = temp_db("p0e-scan");
        let mut conn = open_db(&path).expect("open");
        insert_worker_session(
            &mut conn,
            &WorkerSessionRow {
                worker_session_id: "worker-1".to_string(),
                origin_device_id: "mac-local".to_string(),
                status: "active".to_string(),
                detail: None,
                schema_version: 1,
            },
        )
        .expect("worker-1");
        insert_worker_session(
            &mut conn,
            &WorkerSessionRow {
                worker_session_id: "worker-2".to_string(),
                origin_device_id: "mac-local".to_string(),
                status: "ended".to_string(),
                detail: Some("clean shutdown".to_string()),
                schema_version: 1,
            },
        )
        .expect("worker-2");
        insert_assignment(
            &mut conn,
            &AssignmentRow {
                assignment_id: "assignment-1".to_string(),
                execution_id: Some("execution-1".to_string()),
                worker_session_id: Some("worker-1".to_string()),
                origin_device_id: "mac-local".to_string(),
                status: "active".to_string(),
                detail: None,
                lease_expires_ms: None,
                heartbeat_ms: None,
                schema_version: 1,
            },
        )
        .expect("assignment-1");
        insert_assignment(
            &mut conn,
            &AssignmentRow {
                assignment_id: "assignment-2".to_string(),
                execution_id: Some("execution-2".to_string()),
                worker_session_id: Some("worker-2".to_string()),
                origin_device_id: "mac-local".to_string(),
                status: "lost".to_string(),
                detail: Some("worker stopped".to_string()),
                lease_expires_ms: None,
                heartbeat_ms: None,
                schema_version: 1,
            },
        )
        .expect("assignment-2");
        let new_worker = recovery_scan(&mut conn, "mac-local").expect("scan");
        assert_eq!(new_worker, "worker-3");
        let workers = list_worker_sessions(&conn).expect("workers");
        let w1 = workers.iter().find(|w| w.worker_session_id == "worker-1").unwrap();
        assert_eq!(w1.status, "ended");
        assert_eq!(w1.detail.as_deref(), Some("process ended"));
        let w2 = workers.iter().find(|w| w.worker_session_id == "worker-2").unwrap();
        assert_eq!(w2.status, "ended");
        assert_eq!(w2.detail.as_deref(), Some("clean shutdown"));
        let w3 = workers.iter().find(|w| w.worker_session_id == "worker-3").unwrap();
        assert_eq!(w3.status, "active");
        let assignments = list_assignments(&conn).expect("assignments");
        let a1 = assignments.iter().find(|a| a.assignment_id == "assignment-1").unwrap();
        assert_eq!(a1.status, "lost");
        assert_eq!(a1.detail.as_deref(), Some("worker stopped"));
        let a2 = assignments.iter().find(|a| a.assignment_id == "assignment-2").unwrap();
        assert_eq!(a2.status, "lost");
        assert_eq!(a2.detail.as_deref(), Some("worker stopped"));
        // The scan's lost assignment carries an assignment.lost event.
        let events = list_events(&conn).expect("events");
        assert!(events.iter().any(|e| e.aggregate_kind == "assignment"
            && e.event_type == "assignment.lost"
            && e.aggregate_id == "assignment-1"));
    }

    #[test]
    fn p0e_takeover_writes_checkpoint_and_refuses_cleanly() {
        let path = temp_db("p0e-takeover");
        let mut conn = open_db(&path).expect("open");
        upsert_execution(&mut conn, &execution("execution-1", "running")).expect("exec");
        upsert_effect(
            &mut conn,
            &effect_with_key("effect-1", "execution-1", "succeeded", "key-1"),
        )
        .expect("effect-1");
        upsert_effect(
            &mut conn,
            &effect_with_key("effect-2", "execution-1", "planned", "key-2"),
        )
        .expect("effect-2");
        // Unknown execution: refused, nothing written.
        assert!(matches!(
            takeover_assignment(&mut conn, "execution-9", None, "mac-local"),
            Err(TakeoverError::Refused("execution_not_found"))
        ));
        assert_eq!(count_table(&conn, "worker_sessions").expect("count"), 0);
        let outcome =
            takeover_assignment(&mut conn, "execution-1", None, "mac-local").expect("takeover");
        assert_eq!(outcome.assignment.status, "active");
        assert_eq!(outcome.assignment.worker_session_id.as_deref(), Some("worker-1"));
        assert_eq!(outcome.execution.worker_session_id.as_deref(), Some("worker-1"));
        assert_eq!(outcome.checkpoint.kind, "execution.plan_position");
        let payload: serde_json::Value =
            serde_json::from_str(&outcome.checkpoint.payload_json).expect("checkpoint json");
        assert_eq!(payload["completed_keys"], serde_json::json!(["key-1"]));
        assert_eq!(payload["next_effect_key"], serde_json::json!("key-2"));
        assert!(outcome.superseded.is_empty());
        // Second takeover: the driving assignment refuses it,
        // and the refused (omitted-worker) attempt writes no
        // worker row either — refusal rolls the whole tx back.
        assert!(matches!(
            takeover_assignment(&mut conn, "execution-1", None, "mac-local"),
            Err(TakeoverError::Refused("assignment_active"))
        ));
        assert_eq!(count_table(&conn, "worker_sessions").expect("count"), 1);
        assert_eq!(count_table(&conn, "assignments").expect("count"), 1);
        // Ended worker named explicitly: refused as not active.
        insert_worker_session(
            &mut conn,
            &WorkerSessionRow {
                worker_session_id: "worker-9".to_string(),
                origin_device_id: "mac-local".to_string(),
                status: "ended".to_string(),
                detail: Some("process ended".to_string()),
                schema_version: 1,
            },
        )
        .expect("worker-9");
        // (Need a free execution for this check: execution-1 is
        // driven, so use a fresh one.)
        upsert_execution(&mut conn, &execution("execution-2", "running")).expect("exec-2");
        assert!(matches!(
            takeover_assignment(&mut conn, "execution-2", Some("worker-9"), "mac-local"),
            Err(TakeoverError::Refused("worker_session_not_active"))
        ));
        // Terminal execution: refused.
        upsert_execution(&mut conn, &execution("execution-3", "completed")).expect("exec-3");
        assert!(matches!(
            takeover_assignment(&mut conn, "execution-3", None, "mac-local"),
            Err(TakeoverError::Refused("execution_terminal"))
        ));
        // The reassigned event keeps fold(events) == tables.
        assert!(fold_matches_tables(&conn).expect("fold"));
        let events = list_events(&conn).expect("events");
        assert!(events.iter().any(|e| e.event_type == "execution.reassigned"));
        assert!(events.iter().any(|e| e.event_type == "assignment.created"));
    }

    #[test]
    fn p0e_transition_moves_status_and_dones_assignments_on_terminal() {
        let path = temp_db("p0e-transition");
        let mut conn = open_db(&path).expect("open");
        upsert_execution(&mut conn, &execution("execution-1", "running")).expect("exec");
        insert_worker_session(
            &mut conn,
            &WorkerSessionRow {
                worker_session_id: "worker-1".to_string(),
                origin_device_id: "mac-local".to_string(),
                status: "active".to_string(),
                detail: None,
                schema_version: 1,
            },
        )
        .expect("worker");
        insert_assignment(
            &mut conn,
            &AssignmentRow {
                assignment_id: "assignment-1".to_string(),
                execution_id: Some("execution-1".to_string()),
                worker_session_id: Some("worker-1".to_string()),
                origin_device_id: "mac-local".to_string(),
                status: "active".to_string(),
                detail: None,
                lease_expires_ms: None,
                heartbeat_ms: None,
                schema_version: 1,
            },
        )
        .expect("assignment");
        assert!(matches!(
            transition_execution(&mut conn, "execution-9", "running"),
            Err(ExecutionTransitionError::NotFound)
        ));
        assert!(transition_execution(&mut conn, "execution-1", "completed").is_ok());
        // Illegal: terminal has no outgoing edge; the row is
        // untouched by the refused attempt.
        assert!(matches!(
            transition_execution(&mut conn, "execution-1", "running"),
            Err(ExecutionTransitionError::Illegal)
        ));
        let record = get_execution(&conn, "execution-1").expect("get").unwrap();
        assert_eq!(record.status, "completed");
        let assignment = get_assignment(&conn, "assignment-1").expect("get").unwrap();
        assert_eq!(assignment.status, "done");
        // Non-terminal landing leaves assignments alone.
        upsert_execution(&mut conn, &execution("execution-2", "running")).expect("exec-2");
        let moved = transition_execution(&mut conn, "execution-2", "needs_review")
            .expect("to review");
        assert_eq!(moved.status, "needs_review");
        assert!(fold_matches_tables(&conn).expect("fold"));
        let events = list_events(&conn).expect("events");
        assert!(events.iter().any(|e| e.event_type == "execution.transitioned"));
    }
}
