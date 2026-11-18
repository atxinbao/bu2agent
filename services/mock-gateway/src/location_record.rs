//! Location record cache: record one current-location
//! sample from a live source read into SQLite
//! (`device_locations`), mirroring `imessage_sync.rs` /
//! `contacts_sync.rs` (the mail `mail_sync.rs` patterns
//! are the model) — with one deliberate difference: a
//! location record is a single sample, not a sync pass,
//! so there is no cursor and no synced/skipped count.
//!
//! What a row records is the fix itself — raw latitude
//! / longitude + `accuracy_meters` in SQLite (Q-MCL-17)
//! — plus the two hashes (`grid_hash` /
//! `locality_hash`, LOCAL DEFINITIONs in
//! `location_source.rs`) and the country code the source
//! reported (NULL in MCL-3: Core Location alone has no
//! country). The payload recorded per row is the
//! redacted event payload (sample id, the two hashes,
//! accuracy, recorded time only); raw coordinates never
//! reach the event log.
//!
//! A record writes the row + its `location.recorded`
//! event in one transaction, and in the same transaction
//! prunes rows older than the 30-day retention
//! (Q-MCL-16 — the EXPLICIT exception to red line 9:
//! history rows are deleted here, by age only; the
//! events stay, so the recorded count is preserved in
//! the audit trail). Pruning rides on the record call
//! (the simplest testable mechanism): no background
//! sweeper, no separate schedule. The just-recorded row
//! (recorded_ms = now) is never pruned by its own call.
//!
//! Sample ids are `loc-sample-N` from max-suffix + 1
//! inside the transaction (the mail_sync.rs precedent):
//! a plain COUNT + 1 would reuse ids after a prune
//! (rows are deleted here, unlike contacts/threads),
//! and a reused id would make an old event point at a
//! different sample.

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use crate::location_source::{grid_hash_for, locality_hash_for, DeviceLocationView};
use crate::sqlite_store::{append_event, DB_SCHEMA_VERSION};

pub const LOCATION_SOURCE_ID: &str = "location-source";

/// Retention (Q-MCL-16): samples older than 30 days
/// are pruned at record time. In milliseconds, as an
/// i64 so the cutoff arithmetic stays in SQLite's
/// integer domain.
pub const LOCATION_RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;

/// One `device_locations` row (serde used by the list/
/// get routes and the desktop/TS clients). Raw
/// coordinates are PLAINTEXT here (Q-MCL-17: SQLite
/// stores them; the UI shows exact values only locally
/// in the packaged app, never logged or exported).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeviceLocationRow {
    pub sample_id: String,
    pub source_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_meters: f64,
    pub locality_hash: String,
    pub country_code: Option<String>,
    pub recorded_ms: i64,
    pub created_ms: i64,
    pub origin_device_id: String,
}

pub const DEVICE_LOCATION_COLS: &str = "sample_id, source_id, latitude, longitude, \
    accuracy_meters, locality_hash, country_code, recorded_ms, created_ms, origin_device_id";

fn device_location_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<DeviceLocationRow> {
    Ok(DeviceLocationRow {
        sample_id: row.get(0)?,
        source_id: row.get(1)?,
        latitude: row.get(2)?,
        longitude: row.get(3)?,
        accuracy_meters: row.get(4)?,
        locality_hash: row.get(5)?,
        country_code: row.get(6)?,
        recorded_ms: row.get(7)?,
        created_ms: row.get(8)?,
        origin_device_id: row.get(9)?,
    })
}

/// List recorded samples, newest first
/// (`recorded_ms` DESC — the plan §5 read mouth; ties
/// break by insertion order, `rowid` DESC, so two
/// samples recorded in the same millisecond still list
/// newest-first deterministically).
/// `limit` is clamped by the caller (the routes pass
/// `clamp_limit`); `offset` is a plain row offset.
pub fn list_device_locations(
    conn: &Connection,
    source_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<DeviceLocationRow>, String> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {DEVICE_LOCATION_COLS} FROM device_locations \
             WHERE source_id = ?1 ORDER BY recorded_ms DESC, rowid DESC LIMIT ?2 OFFSET ?3"
        ))
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![source_id, limit as i64, offset as i64],
            device_location_from_row,
        )
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

/// Get one recorded sample by its local `loc-sample-N`
/// id.
pub fn get_device_location(
    conn: &Connection,
    sample_id: &str,
) -> Result<Option<DeviceLocationRow>, String> {
    conn.query_row(
        &format!("SELECT {DEVICE_LOCATION_COLS} FROM device_locations WHERE sample_id = ?1"),
        params![sample_id],
        device_location_from_row,
    )
    .optional()
    .map_err(|e| e.to_string())
}

/// The latest recorded sample, if any (the
/// `location.current` loop tool reads this — never a
/// live fix, Q-MCL-18).
pub fn latest_device_location(
    conn: &Connection,
    source_id: &str,
) -> Result<Option<DeviceLocationRow>, String> {
    let rows = list_device_locations(conn, source_id, 1, 0)?;
    Ok(rows.into_iter().next())
}

/// The redacted event payload for one recorded sample:
/// sample id, grid hash, locality hash, accuracy,
/// recorded time — exactly the five approved fields
/// (plan §5). No latitude, no longitude, no country
/// code: the allowlist in `redact_event_payload` must
/// carry `sample_id`, `grid_hash`, `locality_hash`,
/// `accuracy_meters`, `recorded_ms`; anything else the
/// payload names is dropped there.
fn recorded_payload(row: &DeviceLocationRow, grid_hash: &str) -> String {
    let payload = serde_json::json!({
        "sample_id": row.sample_id,
        "grid_hash": grid_hash,
        "locality_hash": row.locality_hash,
        "accuracy_meters": row.accuracy_meters,
        "recorded_ms": row.recorded_ms,
    });
    serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
}

/// Delete rows older than the 30-day retention in the
/// caller's transaction. Returns the pruned count.
/// The just-recorded row is never old enough to match
/// (its `recorded_ms` is now); the prune runs before
/// the insert so the order cannot matter.
fn prune_expired_tx(tx: &Transaction<'_>, cutoff_ms: i64) -> Result<usize, String> {
    let pruned = tx
        .execute(
            "DELETE FROM device_locations WHERE recorded_ms < ?1",
            params![cutoff_ms],
        )
        .map_err(|e| e.to_string())?;
    Ok(pruned)
}

/// Record one current-location sample in a single
/// transaction: prune expired rows, insert the row,
/// append its `location.recorded` event. Returns the
/// row and the pruned count (the route returns the row;
/// the count is for tests and the caller, never a
/// separate response field).
pub fn record_location(
    conn: &mut Connection,
    view: &DeviceLocationView,
    source_id: &str,
    origin_device_id: &str,
) -> Result<(DeviceLocationRow, usize), String> {
    let now_ms = crate::sqlite_store::now_ms() as i64;
    let grid_hash = grid_hash_for(view.latitude, view.longitude);
    let locality_hash = locality_hash_for(view.latitude, view.longitude);
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let pruned = prune_expired_tx(&tx, now_ms - LOCATION_RETENTION_MS)?;
    // Max-suffix + 1 (the mail_sync.rs precedent):
    // COUNT + 1 would reuse ids after a prune.
    let next: i64 = tx
        .query_row(
            "SELECT COALESCE(MAX(CAST(substr(sample_id, 12) AS INTEGER)), 0) \
             FROM device_locations",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|e| e.to_string())?;
    let sample_id = format!("loc-sample-{}", next + 1);
    let row = DeviceLocationRow {
        sample_id,
        source_id: source_id.to_string(),
        latitude: view.latitude,
        longitude: view.longitude,
        accuracy_meters: view.accuracy_meters,
        locality_hash,
        country_code: view.country_code.clone(),
        recorded_ms: now_ms,
        created_ms: now_ms,
        origin_device_id: origin_device_id.to_string(),
    };
    tx.execute(
        "INSERT INTO device_locations (sample_id, source_id, latitude, longitude, \
         accuracy_meters, locality_hash, country_code, recorded_ms, created_ms, \
         origin_device_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8, ?9)",
        params![
            row.sample_id,
            row.source_id,
            row.latitude,
            row.longitude,
            row.accuracy_meters,
            row.locality_hash,
            row.country_code,
            now_ms,
            row.origin_device_id,
        ],
    )
    .map_err(|e| e.to_string())?;
    append_event(
        &tx,
        "location",
        &row.sample_id,
        "location.recorded",
        &recorded_payload(&row, &grid_hash),
        origin_device_id,
        now_ms,
        DB_SCHEMA_VERSION,
    )?;
    tx.commit().map_err(|e| e.to_string())?;
    Ok((row, pruned))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorded_payload_names_no_coordinates() {
        // The event payload must be the five approved
        // fields only. No latitude, no longitude, no
        // country code — even though the ROW carries
        // them (Q-MCL-17: SQLite stores raw coordinates,
        // events get the grid hash only).
        let row = DeviceLocationRow {
            sample_id: "loc-sample-1".to_string(),
            source_id: "location-source".to_string(),
            latitude: 31.2304,
            longitude: 121.4737,
            accuracy_meters: 35.0,
            locality_hash: "aa".repeat(32),
            country_code: Some("CN".to_string()),
            recorded_ms: 1_000,
            created_ms: 1_000,
            origin_device_id: "dev-1".to_string(),
        };
        let payload = recorded_payload(&row, &"bb".repeat(32));
        assert!(payload.contains("\"sample_id\":\"loc-sample-1\""));
        assert!(payload.contains("\"grid_hash\""));
        assert!(payload.contains("\"locality_hash\""));
        assert!(payload.contains("\"accuracy_meters\":35.0"));
        assert!(payload.contains("\"recorded_ms\":1000"));
        assert!(!payload.contains("31.2304"), "no latitude plaintext");
        assert!(!payload.contains("121.4737"), "no longitude plaintext");
        assert!(!payload.contains("CN"), "no country code");
        assert!(!payload.contains("latitude"), "no latitude field");
        assert!(!payload.contains("longitude"), "no longitude field");
        assert!(!payload.contains("country_code"), "no country field");
    }
}
