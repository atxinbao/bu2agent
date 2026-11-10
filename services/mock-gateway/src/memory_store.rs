//! D1-5 (Q-D1-51–Q-D1-54): user/project memory layers.
//!
//! Two tables, same shape: `project_memories` and
//! `user_memories`. A memory is written only by an
//! explicit save (no automatic extraction from
//! executions or sessions); every row carries its
//! provenance in `source_json` — session, execution,
//! or source id, plus author and timestamp — and its
//! scope triple in `scope_json` (canonical JSON).
//! Deletes are tombstones — the text stays on disk,
//! and the UI must never claim "deleted from disk".
//!
//! Memory has NO Approval/Evidence chain. A memory
//! triggering an action still goes through the Effect
//! path; the action has its own approval/Evidence,
//! unrelated to the memory.
//!
//! Events: `memory.created` / `memory.deleted`,
//! aggregate_kind `"memory"`, payload = the post-state
//! row serialization, written in the same transaction
//! as the row (D1-2 `insert_mandate` pattern), so the
//! fold covers this domain.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use crate::sqlite_store::{append_event, canonical_json};

fn err(detail: impl std::fmt::Display) -> String {
    format!("sqlite: {detail}")
}

/// The scope triple (Q-D1-52). A missing dimension
/// means "global" for that dimension; the all-empty
/// triple serializes to the canonical `"{}"`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MemoryScope {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

impl MemoryScope {
    /// Canonical JSON of the triple (keys sorted).
    pub fn canonical(&self) -> String {
        let value = serde_json::to_value(self).expect("memory scope serializes");
        canonical_json(&value)
    }

    /// True when every dimension is absent (global scope).
    pub fn is_empty(&self) -> bool {
        self.workspace_id.is_none() && self.project_id.is_none() && self.session_id.is_none()
    }

    /// Scope-filter match: `self` is the filter; a
    /// missing dimension does not filter (Q-D1-52:
    /// cross-scope read is visible).
    pub fn matches(&self, other: &MemoryScope) -> bool {
        (self.workspace_id.is_none() || self.workspace_id == other.workspace_id)
            && (self.project_id.is_none() || self.project_id == other.project_id)
            && (self.session_id.is_none() || self.session_id == other.session_id)
    }

    /// Parse a stored canonical scope_json back into a
    /// triple. Unparseable rows match nothing rather
    /// than failing the list.
    pub fn from_json(json: &str) -> Option<MemoryScope> {
        serde_json::from_str(json).ok()
    }
}

/// Provenance (Q-D1-53). Required on every memory:
/// at least the author, plus whichever of the three
/// ids the caller can name (any combination, including
/// none), and the save timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemorySource {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    pub author: String,
    pub created_ms: u64,
}

impl MemorySource {
    /// Canonical JSON of the provenance record.
    pub fn canonical(&self) -> String {
        let value = serde_json::to_value(self).expect("memory source serializes");
        canonical_json(&value)
    }

    /// The `source_id` recorded in a stored source_json,
    /// if the row names one. Lenient on purpose: the
    /// source-cleanup cascade must never fail on a row
    /// whose provenance it cannot parse.
    pub fn source_id_of(source_json: &str) -> Option<String> {
        serde_json::from_str::<serde_json::Value>(source_json)
            .ok()?
            .get("source_id")?
            .as_str()
            .map(str::to_string)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryRow {
    pub memory_id: String,
    pub layer: String,
    pub scope_json: String,
    pub content: String,
    pub source_json: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    pub deleted: bool,
    pub origin_device_id: String,
    pub schema_version: i64,
}

fn memory_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MemoryRow> {
    let deleted: i64 = row.get(7)?;
    Ok(MemoryRow {
        memory_id: row.get(0)?,
        layer: row.get(1)?,
        scope_json: row.get(2)?,
        content: row.get(3)?,
        source_json: row.get(4)?,
        created_ms: row.get(5)?,
        updated_ms: row.get(6)?,
        deleted: deleted != 0,
        origin_device_id: row.get(8)?,
        schema_version: row.get(9)?,
    })
}

const MEMORY_COLS: &str = "memory_id, layer, scope_json, content, source_json, created_ms, updated_ms, deleted, origin_device_id, schema_version";

/// The table for a layer. Anything else is a caller
/// error — the two tables are the only layers.
pub fn table_for(layer: &str) -> Result<&'static str, String> {
    match layer {
        "project" => Ok("project_memories"),
        "user" => Ok("user_memories"),
        other => Err(format!("unknown memory layer: {other}")),
    }
}

/// Insert a row plus its `memory.created` event in one
/// transaction. The payload is the post-state row
/// serialization.
pub fn insert_memory(conn: &mut Connection, row: &MemoryRow) -> Result<(), String> {
    let tx = conn.transaction().map_err(err)?;
    insert_memory_tx(&tx, row)?;
    tx.commit().map_err(err)?;
    Ok(())
}

fn insert_memory_tx(tx: &Transaction, row: &MemoryRow) -> Result<(), String> {
    let table = table_for(&row.layer)?;
    tx.execute(
        &format!(
            "INSERT INTO {table} ({MEMORY_COLS})
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
        ),
        params![
            row.memory_id,
            row.layer,
            row.scope_json,
            row.content,
            row.source_json,
            row.created_ms as i64,
            row.updated_ms as i64,
            row.deleted as i64,
            row.origin_device_id,
            row.schema_version,
        ],
    )
    .map_err(err)?;
    let payload = serde_json::to_string(row).map_err(|e| e.to_string())?;
    append_event(
        tx,
        "memory",
        &row.memory_id,
        "memory.created",
        &payload,
        &row.origin_device_id,
        row.created_ms as i64,
        1,
    )?;
    Ok(())
}

/// All project-memory rows, oldest first, including
/// tombstones (fold input).
pub fn list_project_memories_all(conn: &Connection) -> Result<Vec<MemoryRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {MEMORY_COLS} FROM project_memories ORDER BY rowid ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map([], memory_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// All user-memory rows, oldest first, including
/// tombstones (fold input).
pub fn list_user_memories_all(conn: &Connection) -> Result<Vec<MemoryRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {MEMORY_COLS} FROM user_memories ORDER BY rowid ASC"
        ))
        .map_err(err)?;
    let rows = stmt
        .query_map([], memory_from_row)
        .map_err(err)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(err)?;
    Ok(rows)
}

/// Live rows (tombstones hidden), oldest first.
/// `layer` None means both layers. The scope filter
/// is exact per dimension; a missing dimension does
/// not filter.
pub fn list_memories(
    conn: &Connection,
    layer: Option<&str>,
    scope: &MemoryScope,
) -> Result<Vec<MemoryRow>, String> {
    let mut rows = match layer {
        None => {
            let mut all = list_project_memories_all(conn)?;
            all.extend(list_user_memories_all(conn)?);
            all
        }
        Some(layer) => {
            let table = table_for(layer)?;
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {MEMORY_COLS} FROM {table} ORDER BY rowid ASC"
                ))
                .map_err(err)?;
            let rows = stmt
                .query_map([], memory_from_row)
                .map_err(err)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(err)?;
            rows
        }
    };
    rows.retain(|row| {
        if row.deleted {
            return false;
        }
        match MemoryScope::from_json(&row.scope_json) {
            Some(row_scope) => scope.matches(&row_scope),
            None => false,
        }
    });
    rows.sort_by(|a, b| {
        (a.created_ms, a.memory_id.clone()).cmp(&(b.created_ms, b.memory_id.clone()))
    });
    Ok(rows)
}

/// One row by id, either layer. A tombstoned row is
/// still readable (goals-detail precedent: detail
/// reads archived rows, list hides them).
pub fn get_memory(conn: &Connection, memory_id: &str) -> Result<Option<MemoryRow>, String> {
    for table in ["project_memories", "user_memories"] {
        let row = conn
            .query_row(
                &format!("SELECT {MEMORY_COLS} FROM {table} WHERE memory_id = ?1"),
                params![memory_id],
                memory_from_row,
            )
            .optional()
            .map_err(err)?;
        if let Some(row) = row {
            return Ok(Some(row));
        }
    }
    Ok(None)
}

/// Tombstone a memory: `deleted = true`, text stays
/// on disk, plus a `memory.deleted` event with the
/// post-state row — same transaction. A re-delete is
/// a no-op returning the current row (no second
/// event), like re-archive / re-disconnect. Unknown
/// id returns None.
pub fn delete_memory(
    conn: &mut Connection,
    memory_id: &str,
    origin_device_id: &str,
) -> Result<Option<MemoryRow>, String> {
    let tx = conn.transaction().map_err(err)?;
    let found = delete_memory_tx(&tx, memory_id, origin_device_id)?;
    tx.commit().map_err(err)?;
    Ok(found)
}

fn delete_memory_tx(
    tx: &Transaction,
    memory_id: &str,
    origin_device_id: &str,
) -> Result<Option<MemoryRow>, String> {
    let row = {
        let mut found = None;
        for table in ["project_memories", "user_memories"] {
            let row = tx
                .query_row(
                    &format!("SELECT {MEMORY_COLS} FROM {table} WHERE memory_id = ?1"),
                    params![memory_id],
                    memory_from_row,
                )
                .optional()
                .map_err(err)?;
            if let Some(row) = row {
                found = Some((table, row));
                break;
            }
        }
        found
    };
    let Some((table, row)) = row else {
        return Ok(None);
    };
    if row.deleted {
        return Ok(Some(row));
    }
    let now = crate::sqlite_store::now_ms();
    tx.execute(
        &format!("UPDATE {table} SET deleted = 1, updated_ms = ?1 WHERE memory_id = ?2"),
        params![now as i64, memory_id],
    )
    .map_err(err)?;
    let row = tx
        .query_row(
            &format!("SELECT {MEMORY_COLS} FROM {table} WHERE memory_id = ?1"),
            params![memory_id],
            memory_from_row,
        )
        .map_err(err)?;
    let payload = serde_json::to_string(&row).map_err(|e| e.to_string())?;
    append_event(
        tx,
        "memory",
        &row.memory_id,
        "memory.deleted",
        &payload,
        origin_device_id,
        now as i64,
        1,
    )?;
    Ok(Some(row))
}

/// D1-5 (Q-D1-54) source-cleanup cascade: tombstone
/// every live memory whose provenance names this
/// source, one `memory.deleted` event per row. Runs
/// inside the caller's transaction — the disconnect
/// handler calls this in the same transaction as the
/// status flip. Memories of other sources are
/// untouched. NOT into Inbox.
pub fn tombstone_memories_for_source_tx(
    tx: &Transaction,
    source_id: &str,
    origin_device_id: &str,
) -> Result<usize, String> {
    let mut tombstoned = 0;
    for table in ["project_memories", "user_memories"] {
        let mut stmt = tx
            .prepare(&format!(
                "SELECT {MEMORY_COLS} FROM {table} WHERE deleted = 0 ORDER BY rowid ASC"
            ))
            .map_err(err)?;
        let live = stmt
            .query_map([], memory_from_row)
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        drop(stmt);
        for row in live {
            if MemorySource::source_id_of(&row.source_json).as_deref() != Some(source_id) {
                continue;
            }
            let now = crate::sqlite_store::now_ms();
            tx.execute(
                &format!("UPDATE {table} SET deleted = 1, updated_ms = ?1 WHERE memory_id = ?2"),
                params![now as i64, row.memory_id],
            )
            .map_err(err)?;
            let row = tx
                .query_row(
                    &format!("SELECT {MEMORY_COLS} FROM {table} WHERE memory_id = ?1"),
                    params![row.memory_id],
                    memory_from_row,
                )
                .map_err(err)?;
            let payload = serde_json::to_string(&row).map_err(|e| e.to_string())?;
            append_event(
                tx,
                "memory",
                &row.memory_id,
                "memory.deleted",
                &payload,
                origin_device_id,
                now as i64,
                1,
            )?;
            tombstoned += 1;
        }
    }
    Ok(tombstoned)
}

/// Next free memory number across both tables (for
/// seeding the in-memory `next_memory_id` counter on
/// restart, D1-1 `next_goal_number` pattern). Memory
/// ids are one global `memory-N` sequence across both
/// tables, so the fold map keyed by memory_id stays
/// unique.
pub fn next_memory_number(conn: &Connection) -> Result<u64, String> {
    let mut next: u64 = 1;
    for table in ["project_memories", "user_memories"] {
        let mut stmt = conn
            .prepare(&format!("SELECT memory_id FROM {table}"))
            .map_err(err)?;
        let ids = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(err)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(err)?;
        for id in ids {
            let suffix = crate::sqlite_store::id_suffix(&id);
            if suffix >= 0 {
                next = next.max((suffix as u64) + 1);
            }
        }
    }
    Ok(next)
}
