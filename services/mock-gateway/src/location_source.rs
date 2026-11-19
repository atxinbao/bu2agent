//! Current-location read surface, MCL-3 (shell route, S-1).
//!
//! One manual sample at a time: `POST /api/location/record-from-shell`
//! takes a fix the packaged app's shell process acquired
//! itself and stores it as a discrete `device_locations`
//! row. There is no background collection and no
//! trajectory (Q-MCL-18 / Q-MCL-19): the source is only
//! ever read through the manual record route and the
//! `location.current` loop tool, and the loop tool reads
//! the latest recorded row from SQLite — it never
//! triggers a live fix.
//!
//! # Q-MCL-15 form decision, rewritten by the shell plan (S-1)
//!
//! The fix is acquired in the SHELL main process
//! (`apps/shell/src-tauri/src/location_shell.rs`), not in
//! the gateway sidecar: the sidecar's `CLLocationManager`
//! is created with identifier `""` / bundlePath `""`
//! (Console diagnosis, window rounds 1-2), so locationd
//! refuses to attribute the request and the authorization
//! prompt never appears. The shell process IS properly
//! identified (subject=com.bu2ai.bu2agent in tccd), and
//! the S-0 probe proved it can prompt and read one fix
//! (35.0 m, auth wait 26,307 ms). The sidecar Core
//! Location body that MCL-3 shipped was DELETED by S-1
//! (Q-S-03); this module keeps only the fix shape, the
//! error type, the hash functions and the 501 stub. The
//! shell POSTs the raw fix to the gateway with the
//! startup token (`X-Shell-Token`); a fix that arrives
//! without the token never reaches SQLite (Q-S-02).
//!
//! The macOS fix acquisition still uses
//! `CLLocationManager::requestLocation` (one-shot) with
//! a delegate that records the first fix, and pumps the
//! acquiring thread's runloop while waiting — the
//! manager is created on the worker thread, so its
//! callbacks arrive on that thread's runloop. The
//! `Info.plist` must carry
//! `NSLocationWhenInUseUsageDescription`: without it
//! the authorization prompt cannot appear and every
//! record is `Denied`.
//!
//! The gateway never auto-launches anything for
//! location: there is no app to launch. When location
//! services are off system-wide the shell read is
//! `Unavailable` (501); when the user denied (or the
//! prompt is unanswered within the cap) it is `Denied`
//! (403) / `Timeout` (502) — the shell maps those to
//! the same strings the old gateway route used, and the
//! Tauri command surfaces them unchanged.
//!
//! Coordinates come from the system location service
//! only — never from request parameters (Q-MCL-15).
//! `country_code` is `None` unless the source provides
//! one: Core Location alone has no country (reverse
//! geocoding is a separate network service, not in
//! MCL-3), so the shell fix leaves it `None` and the
//! column stays NULL. `locality_hash` / `grid_hash` are
//! LOCAL DEFINITIONs below (deterministic pure functions
//! of the coordinates; the hashes never contain
//! coordinate plaintext).

use serde::{Deserialize, Serialize};

use crate::operation_binding::sha256_hex;

/// One current-location sample as the live source
/// reports it. No id, no timestamp: the record layer
/// assigns `loc-sample-N` and `recorded_ms` when the
/// row is written.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeviceLocationView {
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_meters: f64,
    /// None unless the source provides one (the macOS
    /// body never does in MCL-3 — see the module docs).
    pub country_code: Option<String>,
}

/// Source failures. Same six variants as `MailError`
/// (the same mapping: `NotFound` -> 404, `Unavailable`
/// -> 501, `Denied` -> 403, `Upstream` / `NotRunning` /
/// `Timeout` -> 502 on the read paths). Separate type so
/// no location error ever renders as a mail error.
/// S-1 deleted the gateway-side Core Location body
/// (Q-S-03): the only production `LocationSource` left
/// is `UnavailableLocationSource`, and the shell reports
/// its own failures as plain strings through the Tauri
/// command. The type stays so the trait, the stub and
/// tests share the six-variant shape.
#[derive(Debug, Clone)]
pub enum LocationError {
    NotFound(&'static str),
    Unavailable(String),
    Denied,
    Upstream(String),
    NotRunning,
    Timeout,
}

/// Live-query current-location source (one single fix
/// per call; no continuous updates).
pub trait LocationSource: Send + Sync {
    /// One current fix from the system location service.
    fn current(&self) -> Result<DeviceLocationView, LocationError>;
}

/// 1 km in degrees of latitude (LOCAL DEFINITION, the
/// grid unit for `grid_hash_for` / `locality_hash_for`).
/// 1° of latitude is ~111.32 km everywhere; longitude
/// degrees per km shrink with `cos(latitude)`.
const KM_PER_DEGREE_LAT: f64 = 111.32;

/// The 1 km grid cell indices for a fix (LOCAL
/// DEFINITION): the latitude index is
/// `floor(lat * 111.32)`; the longitude index is
/// `floor(lon * 111.32 * cos(lat))`, with the longitude
/// scale clamped to >= 1.0 so the grid stays defined
/// near the poles.
fn grid_indices(latitude: f64, longitude: f64) -> (i64, i64) {
    let lat_idx = (latitude * KM_PER_DEGREE_LAT).floor() as i64;
    let lon_scale = (KM_PER_DEGREE_LAT * latitude.to_radians().cos()).max(1.0);
    let lon_idx = (longitude * lon_scale).floor() as i64;
    (lat_idx, lon_idx)
}

/// The 1 km grid hash (Q-MCL-17, LOCAL DEFINITION):
/// sha256 of the grid cell CENTER, formatted to 6
/// decimals. Two samples in the same cell hash the
/// same; the hash names no coordinates (one-way) and
/// the CENTER — not the sample — is what is hashed, so
/// an event payload carries ~1 km precision, never the
/// fix. The longitude center reuses the center
/// latitude's scale so the center is the cell's
/// geographic center, not a rescaled corner.
pub fn grid_hash_for(latitude: f64, longitude: f64) -> String {
    let (lat_idx, lon_idx) = grid_indices(latitude, longitude);
    let center_lat = (lat_idx as f64 + 0.5) / KM_PER_DEGREE_LAT;
    let lon_scale_c = (KM_PER_DEGREE_LAT * center_lat.to_radians().cos()).max(1.0);
    let center_lon = (lon_idx as f64 + 0.5) / lon_scale_c;
    sha256_hex(format!("{center_lat:.6},{center_lon:.6}").as_bytes())
}

/// The locality hash (LOCAL DEFINITION): sha256 of the
/// grid cell INDEX pair (`grid:<lat_idx>,<lon_idx>`).
/// Same cell — same locality; the value is a place key,
/// not a point. It differs from `grid_hash_for` on
/// purpose: the grid hash names a (blurred) point for
/// the event payload, the locality hash names the cell
/// for the row.
pub fn locality_hash_for(latitude: f64, longitude: f64) -> String {
    let (lat_idx, lon_idx) = grid_indices(latitude, longitude);
    sha256_hex(format!("grid:{lat_idx},{lon_idx}").as_bytes())
}

/// Clamp a samples-list limit (the mail `clamp_limit`
/// precedent): default 50, maximum 200.
pub fn clamp_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(50).min(200)
}

/// Every call fails: the production source on platforms
/// without a real location source. Never fake data.
pub struct UnavailableLocationSource;

impl LocationSource for UnavailableLocationSource {
    fn current(&self) -> Result<DeviceLocationView, LocationError> {
        Err(LocationError::Unavailable(
            "location source unavailable on this platform".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grid_hash_is_deterministic_and_hides_coordinates() {
        // Same fix, same hash; the hash is a 64-hex
        // sha256 and names no coordinate plaintext.
        let a = grid_hash_for(31.2304, 121.4737);
        let b = grid_hash_for(31.2304, 121.4737);
        assert_eq!(a, b, "same fix must hash the same");
        assert_eq!(a.len(), 64, "sha256 hex");
        assert!(!a.contains("31.2304"), "no coordinate plaintext");
        assert!(!a.contains("121.4737"), "no coordinate plaintext");
        // A fix ~2 km away (different cell) hashes
        // differently.
        let far = grid_hash_for(31.2504, 121.4737);
        assert_ne!(a, far, "a different cell must hash differently");
        // A fix ~100 m away (same cell) hashes the same.
        let near = grid_hash_for(31.2313, 121.4737);
        assert_eq!(a, near, "same cell must hash the same");
    }

    #[test]
    fn locality_hash_is_deterministic_and_cell_keyed() {
        let a = locality_hash_for(31.2304, 121.4737);
        let b = locality_hash_for(31.2304, 121.4737);
        assert_eq!(a, b, "same fix must hash the same");
        assert_eq!(a.len(), 64, "sha256 hex");
        assert!(!a.contains("31.2304"), "no coordinate plaintext");
        // Same cell, different fix: same locality.
        let near = locality_hash_for(31.2313, 121.4746);
        assert_eq!(a, near, "same cell must share the locality");
        // Different cell: different locality.
        let far = locality_hash_for(31.2504, 121.4737);
        assert_ne!(a, far, "a different cell must differ");
        // The locality hash is a cell key, not a point:
        // it differs from the grid hash of the same fix.
        assert_ne!(a, grid_hash_for(31.2304, 121.4737));
    }

    #[test]
    fn grid_indices_handle_negative_longitudes() {
        // Western hemisphere: floor, not truncation, so
        // a fix just west of a boundary lands in the
        // cell west of it, deterministically.
        let a = grid_hash_for(40.7128, -74.0060);
        let b = grid_hash_for(40.7128, -74.0060);
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        let local = locality_hash_for(40.7128, -74.0060);
        assert_eq!(local.len(), 64);
    }
}
