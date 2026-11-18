//! Current-location read surface, MCL-3.
//!
//! One manual sample at a time: `POST /api/location/record`
//! asks the system location service for a single fix and
//! stores it as a discrete `device_locations` row. There
//! is no background collection and no trajectory (Q-MCL-18
//! / Q-MCL-19): the source is only ever read through the
//! manual record route and the `location.current` loop
//! tool, and the loop tool reads the latest recorded row
//! from SQLite — it never triggers a live fix.
//!
//! # Q-MCL-15 form decision (coordinator's call, from evidence)
//!
//! In-process binding, `objc2-core-location` 0.3.2. The
//! gateway runs as a Tauri sidecar inside the signed
//! `.app` bundle — `gateway_lifecycle.rs` `start_owned`,
//! `tauri.conf.json` `externalBin`, and `run-build.command`
//! copies `mock-gateway` into the bundle — so the gateway
//! process IS the packaged app (`com.bu2ai.bu2agent`) and
//! Core Location authorizes against that identity — the
//! same in-process form EventKit 0.4.0 already uses from
//! this sidecar (calendar read/create window-proven in
//! 6b-2). A Swift helper was rejected: it would be a
//! second executable needing its own bundling step and
//! its own TCC attribution question, and the MCL-0 S2
//! evidence covers a standalone Swift app, never a helper
//! subprocess of the gateway.
//!
//! `objc2-core-location` 0.3.2 resolves from crates.io
//! (verified by `cargo fetch` on the VM; default features
//! include `CLLocation`, `CLLocationManager` and
//! `CLLocationManagerDelegate`). The macOS body below
//! uses `CLLocationManager::requestLocation` (one-shot)
//! with a delegate that records the first fix, and pumps
//! this thread's runloop while waiting — the manager is
//! created on the calling (blocking) thread, so its
//! callbacks arrive on that thread's runloop. The
//! `Info.plist` must carry
//! `NSLocationWhenInUseUsageDescription` (the app
//! currently has only the Reminders keys): without it
//! the authorization prompt cannot appear and every
//! record is `Denied`.
//!
//! The gateway never auto-launches anything for
//! location: there is no app to launch. When location
//! services are off system-wide the read is
//! `Unavailable` (501); when the user denied (or the
//! prompt is unanswered within the cap) it is `Denied`
//! (403) / `Timeout` (502).
//!
//! Coordinates come from the system location service
//! only — never from request parameters (Q-MCL-15).
//! `country_code` is `None` unless the source provides
//! one: Core Location alone has no country (reverse
//! geocoding is a separate network service, not in
//! MCL-3), so the macOS body leaves it `None` and the
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
/// `NotFound` / `NotRunning` are unused by the Core
/// Location body (a current fix has no id to miss and
/// no app to be not-running); they are kept so the
/// mapping stays the shared six-variant shape.
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

/// Hard cap on one Core Location call (authorization
/// prompt + first fix share this single deadline, so
/// the desktop 120 s sync client always wins over the
/// gateway cap). A user slow at the prompt retries by
/// tapping record again; a cold first fix is normally
/// seconds (the MCL-0 S2 probe took 2.385 s with
/// authorization already granted).
#[cfg(target_os = "macos")]
const LOCATION_TIMEOUT_SECS: u64 = 60;

#[cfg(target_os = "macos")]
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[cfg(target_os = "macos")]
use objc2::define_class;
#[cfg(target_os = "macos")]
use objc2::msg_send;
#[cfg(target_os = "macos")]
use objc2::rc::Retained;
#[cfg(target_os = "macos")]
use objc2::runtime::{NSObject, NSObjectProtocol, ProtocolObject};
#[cfg(target_os = "macos")]
use objc2::{AnyThread, DefinedClass};
#[cfg(target_os = "macos")]
use objc2_core_location::{
    kCLLocationAccuracyBest, CLAuthorizationStatus, CLError, CLLocation, CLLocationManager,
    CLLocationManagerDelegate,
};
#[cfg(target_os = "macos")]
use objc2_foundation::{NSArray, NSDate, NSError, NSRunLoop};

/// Shared state between the Core Location delegate
/// callbacks and the waiting `current()` call. The
/// delegate writes; the waiter polls under the same
/// mutex while pumping the runloop.
#[cfg(target_os = "macos")]
#[derive(Debug, Default)]
struct LocationState {
    /// The first fix received: (latitude, longitude,
    /// horizontal accuracy in meters). First fix wins —
    /// `requestLocation` delivers one location (the
    /// newest last); a later stale update must not
    /// overwrite it.
    fix: Option<(f64, f64, f64)>,
    /// Terminal failure, if any.
    failure: Option<LocationError>,
    /// Authorization became decided while waiting for
    /// the prompt answer (either callback sets this;
    /// the waiter also re-reads the status).
    auth_decided: bool,
    /// `LocationUnknown` retries so far (at most one).
    retries: u32,
}

#[cfg(target_os = "macos")]
#[derive(Debug)]
struct LocationDelegateIvars {
    state: Arc<Mutex<LocationState>>,
}

#[cfg(target_os = "macos")]
define_class!(
    // SAFETY:
    // - The superclass NSObject does not have any subclassing requirements.
    // - `LocationDelegate` does not implement `Drop`.
    #[unsafe(super(NSObject))]
    #[ivars = LocationDelegateIvars]
    struct LocationDelegate;

    unsafe impl NSObjectProtocol for LocationDelegate {}

    unsafe impl CLLocationManagerDelegate for LocationDelegate {
        #[allow(non_snake_case)]
        #[unsafe(method(locationManager:didUpdateLocations:))]
        unsafe fn locationManager_didUpdateLocations(
            &self,
            _manager: &CLLocationManager,
            locations: &NSArray<CLLocation>,
        ) {
            // The last location in the array is the
            // newest fix.
            if let Some(location) = locations.lastObject() {
                let coordinate = unsafe { location.coordinate() };
                let accuracy = unsafe { location.horizontalAccuracy() };
                let mut state = self.ivars().state.lock().unwrap();
                if state.fix.is_none() {
                    state.fix = Some((coordinate.latitude, coordinate.longitude, accuracy));
                }
            }
        }

        #[allow(non_snake_case)]
        #[unsafe(method(locationManager:didFailWithError:))]
        unsafe fn locationManager_didFailWithError(
            &self,
            manager: &CLLocationManager,
            error: &NSError,
        ) {
            let code = error.code();
            let mut state = self.ivars().state.lock().unwrap();
            if state.fix.is_some() || state.failure.is_some() {
                return;
            }
            if code == CLError::LocationUnknown.0 && state.retries < 1 {
                // Transient: the fix was not ready yet.
                // One retry, then the failure below stands.
                state.retries += 1;
                drop(state);
                unsafe { manager.requestLocation() };
                return;
            }
            state.failure = Some(if code == CLError::Denied.0 {
                LocationError::Denied
            } else {
                // Never render the NSError text: it can
                // contain local paths. The code is enough
                // for the route mapping (Denied / Timeout
                // / Upstream).
                LocationError::Upstream("location lookup failed".to_string())
            });
        }

        #[allow(non_snake_case)]
        #[unsafe(method(locationManagerDidChangeAuthorization:))]
        unsafe fn locationManagerDidChangeAuthorization(&self, _manager: &CLLocationManager) {
            self.ivars().state.lock().unwrap().auth_decided = true;
        }

        #[allow(non_snake_case)]
        #[unsafe(method(locationManager:didChangeAuthorizationStatus:))]
        unsafe fn locationManager_didChangeAuthorizationStatus(
            &self,
            _manager: &CLLocationManager,
            _status: CLAuthorizationStatus,
        ) {
            self.ivars().state.lock().unwrap().auth_decided = true;
        }
    }
);

#[cfg(target_os = "macos")]
impl LocationDelegate {
    fn new(state: Arc<Mutex<LocationState>>) -> Retained<Self> {
        let this = Self::alloc().set_ivars(LocationDelegateIvars { state });
        unsafe { msg_send![super(this), init] }
    }
}

/// macOS-only real source: one Core Location fix via
/// `objc2-core-location`, in-process under the packaged
/// app's identity. See the module docs for the
/// best-effort caveat: the body is validated by window
/// validation on a real Mac (and CI's macOS job), not
/// by this VM.
#[cfg(target_os = "macos")]
pub struct CoreLocationSource;

#[cfg(target_os = "macos")]
impl Default for CoreLocationSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_os = "macos")]
impl CoreLocationSource {
    pub fn new() -> Self {
        Self
    }

    /// Pump this thread's runloop in short slices until
    /// `done` is true or the deadline passes. The Core
    /// Location delegate callbacks arrive on this
    /// thread's runloop (the manager is created on this
    /// thread, not the main thread), so without pumping
    /// nothing is ever delivered.
    fn pump_until(deadline: Instant, done: impl Fn() -> bool) {
        let runloop = NSRunLoop::currentRunLoop();
        while !done() {
            if Instant::now() >= deadline {
                break;
            }
            let until = NSDate::dateWithTimeIntervalSinceNow(0.1);
            runloop.runUntilDate(&until);
        }
    }
}

#[cfg(target_os = "macos")]
impl LocationSource for CoreLocationSource {
    fn current(&self) -> Result<DeviceLocationView, LocationError> {
        // One overall deadline for the whole call
        // (authorization prompt + first fix).
        let deadline = Instant::now() + Duration::from_secs(LOCATION_TIMEOUT_SECS);
        if !unsafe { CLLocationManager::locationServicesEnabled_class() } {
            return Err(LocationError::Unavailable(
                "location services are disabled".to_string(),
            ));
        }
        let manager = unsafe { CLLocationManager::new() };
        unsafe { manager.setDesiredAccuracy(kCLLocationAccuracyBest) };
        let state = Arc::new(Mutex::new(LocationState::default()));
        let delegate = LocationDelegate::new(Arc::clone(&state));
        // setDelegate holds a weak reference: keep the
        // type-erased retained object alive for the call.
        let delegate_obj: Retained<ProtocolObject<dyn CLLocationManagerDelegate>> =
            ProtocolObject::from_retained(delegate);
        unsafe { manager.setDelegate(Some(&delegate_obj)) };
        // Authorization first: NotDetermined prompts the
        // user (system dialog, once per app); Denied /
        // Restricted never prompts again.
        match unsafe { manager.authorizationStatus() } {
            CLAuthorizationStatus::Denied | CLAuthorizationStatus::Restricted => {
                return Err(LocationError::Denied);
            }
            CLAuthorizationStatus::NotDetermined => {
                unsafe { manager.requestWhenInUseAuthorization() };
                Self::pump_until(deadline, || {
                    state.lock().unwrap().auth_decided
                        || unsafe { manager.authorizationStatus() }
                            != CLAuthorizationStatus::NotDetermined
                });
                match unsafe { manager.authorizationStatus() } {
                    CLAuthorizationStatus::Denied | CLAuthorizationStatus::Restricted => {
                        return Err(LocationError::Denied);
                    }
                    CLAuthorizationStatus::NotDetermined => {
                        return Err(LocationError::Timeout);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        unsafe { manager.requestLocation() };
        Self::pump_until(deadline, || {
            let state = state.lock().unwrap();
            state.fix.is_some() || state.failure.is_some()
        });
        let state = state.lock().unwrap();
        if let Some((latitude, longitude, accuracy_meters)) = state.fix {
            return Ok(DeviceLocationView {
                latitude,
                longitude,
                accuracy_meters,
                // No reverse geocoder in MCL-3 (see the
                // module docs): the column stays NULL.
                country_code: None,
            });
        }
        if let Some(failure) = &state.failure {
            return Err(failure.clone());
        }
        Err(LocationError::Timeout)
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
