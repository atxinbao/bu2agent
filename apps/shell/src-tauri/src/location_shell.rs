//! Real location acquisition for the MCL-3 shell route
//! (S-1, plan `files/mcl3-shell-plan-draft.md` §3).
//!
//! The packaged app's SHELL main process acquires one
//! Core Location fix itself and hands it to the Tauri
//! command, which POSTs it to the gateway
//! (`POST /api/location/record-from-shell`) with the
//! startup token. The gateway never fetches a fix any
//! more: S-1 deleted its sidecar Core Location body
//! (Q-S-03) because the sidecar's `CLLocationManager`
//! is created with identifier `""` / bundlePath `""`
//! (Console diagnosis, window rounds 1-2), so
//! locationd refuses to attribute the request and the
//! authorization prompt never appears. The shell
//! process IS properly identified
//! (subject=com.bu2ai.bu2agent in tccd).
//!
//! Thread morphology: copied from the S-0 probe v2
//! (`location_probe.rs`, deleted by S-1 — the probe
//! PASSED on the Mac with this exact form: status ok,
//! 35.0 m, auth wait 26,307 ms, fix 4 ms). ONE
//! background worker thread + manual runloop pump; the
//! manager, the delegate and the waiting loops are all
//! created and used on that single spawned thread; no
//! objc2 object ever leaves it. Only plain data
//! crosses the thread boundary: the joined
//! `Result<LocationFix, String>` (`LocationFix` is
//! three f64s; the error is a short code string).
//! The v1 main-thread form (`run_on_main_thread` +
//! a channel carrying `Retained<CLLocationManager>`)
//! failed to COMPILE (E0277: `CLLocationManager` is
//! `!Send` by construction — every `extern_class!`
//! type contains `AnyObject`, whose `_p` field is
//! `OpaqueData = UnsafeCell<PhantomData<(*const
//! UnsafeCell<()>, PhantomPinned)>>`, objc2 0.6.5
//! `ffi/mod.rs:287`; `Retained<T>` is `Send` only if
//! `T: Send + Sync`, `rc/retained.rs:854-865`).
//! This module makes no `Send` claim about any objc2
//! type: if a value is an objc2 object, it stays on
//! the thread that created it.
//!
//! The shell sends the RAW fix only — no grid hash
//! (the gateway computes the hashes, Q-S-10) and no
//! country (Core Location alone has none; the column
//! stays NULL, Q-S-09). Error strings match the old
//! gateway route's wording, so the UI text the user
//! already saw does not change: denied,
//! "location access denied (check Location Services
//! permission)"; timeout, "location read timed out";
//! lookup failure, "location lookup failed";
//! unavailable, "location source unavailable on this
//! platform". Coordinates never reach any log: the
//! error paths carry short codes only, never a fix.
//!
//! NOT compiled on this VM: the shell crate is its own
//! workspace and this module is macOS-only; CI's shell
//! job (macOS, `cargo test`) is the only compile check
//! before the Mac window build, because `cargo test`
//! compiles the binary target including this module.
//! The code below was statically checked
//! symbol-by-symbol against the vendored
//! objc2-core-location 0.3.2 / objc2-foundation 0.3.2 /
//! objc2 0.6.5 sources in the VM cargo registry —
//! copied from the S-0 probe v2, which compiled and
//! ran on the Mac, with only the probe trigger /
//! result file / status line removed.

/// One fix as the shell reports it: raw coordinates +
/// accuracy. No id, no timestamp: the gateway assigns
/// `loc-sample-N` and `recorded_ms` when the row is
/// written. Plain data only — this is what crosses
/// the worker-thread join.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LocationFix {
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy_meters: f64,
}

/// Total budget for one acquisition (authorization
/// prompt + first fix share this single deadline, Q-S-05:
/// the timeout belongs to the shell now, not the
/// gateway). Same 60 s the old gateway route used, so
/// a user slow at the prompt retries by tapping
/// record again; a cold first fix is normally seconds
/// (the S-0 probe took 26,307 ms waiting on the user
/// and 4 ms for the fix once authorized).
#[cfg(target_os = "macos")]
const LOCATION_TIMEOUT_SECS: u64 = 60;

/// Acquire one fix on a worker thread and return it
/// (or the failure string). The Tauri command calls
/// this synchronously: it blocks up to the 60 s budget
/// while the user answers the authorization prompt,
/// exactly as the old command blocked on the gateway.
/// The join is the only cross-thread step, and it
/// carries plain data only (see the module docs).
pub fn acquire_fix() -> Result<LocationFix, String> {
    std::thread::spawn(acquire_fix_on_worker)
        .join()
        .map_err(|_| "location lookup failed".to_string())?
}

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
// `AnyThread` is imported ONLY for `alloc()` (it gives `alloc`
// its signature); it implies nothing about `Send` — see the
// module docs. `DefinedClass` provides `set_ivars`.
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
/// callbacks and the waiting loop. Copied from the S-0
/// probe v2: first fix wins, one `LocationUnknown`
/// retry, then the failure stands. Both sides run on
/// the SAME (worker) thread — the callbacks fire while
/// the runloop is pumped — so the mutex is never
/// contended across threads; it is kept because the
/// delegate can only reach the state through its ivars.
#[cfg(target_os = "macos")]
#[derive(Debug, Default)]
struct FixState {
    fix: Option<(f64, f64, f64)>,
    /// Short code only (`denied` / `lookup_failed`): never an
    /// NSError text, which can contain local paths.
    failure: Option<&'static str>,
    retries: u32,
}

#[cfg(target_os = "macos")]
#[derive(Debug)]
struct ShellLocationDelegateIvars {
    state: Arc<Mutex<FixState>>,
}

#[cfg(target_os = "macos")]
define_class!(
    // SAFETY: same as the S-0 probe v2:
    // - The superclass NSObject does not have any subclassing requirements.
    // - `ShellLocationDelegate` does not implement `Drop`.
    #[unsafe(super(NSObject))]
    #[ivars = ShellLocationDelegateIvars]
    struct ShellLocationDelegate;

    unsafe impl NSObjectProtocol for ShellLocationDelegate {}

    unsafe impl CLLocationManagerDelegate for ShellLocationDelegate {
        #[allow(non_snake_case)]
        #[unsafe(method(locationManager:didUpdateLocations:))]
        unsafe fn locationManager_didUpdateLocations(
            &self,
            _manager: &CLLocationManager,
            locations: &NSArray<CLLocation>,
        ) {
            // The last location in the array is the newest fix.
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
                // Transient: the fix was not ready yet. One
                // retry, then the failure below stands.
                state.retries += 1;
                drop(state);
                unsafe { manager.requestLocation() };
                return;
            }
            // Never render the NSError text: it can contain
            // local paths. The code is enough.
            state.failure = Some(if code == CLError::Denied.0 {
                "denied"
            } else {
                "lookup_failed"
            });
        }
    }
);

#[cfg(target_os = "macos")]
impl ShellLocationDelegate {
    fn new(state: Arc<Mutex<FixState>>) -> Retained<Self> {
        let this = Self::alloc().set_ivars(ShellLocationDelegateIvars { state });
        unsafe { msg_send![super(this), init] }
    }
}

/// Pump this thread's runloop in short slices until `done` is
/// true or the deadline passes. Copied from the S-0 probe v2
/// (which copied the gateway's `pump_until`): the manager
/// is created on this thread, so its delegate callbacks
/// arrive on this thread's runloop — without pumping,
/// nothing is ever delivered.
#[cfg(target_os = "macos")]
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

/// The whole acquisition runs on ONE spawned worker thread:
/// the manager, the delegate and both waiting loops are
/// created and used here, and no objc2 object ever leaves
/// this thread.
#[cfg(target_os = "macos")]
fn acquire_fix_on_worker() -> Result<LocationFix, String> {
    let deadline = Instant::now() + Duration::from_secs(LOCATION_TIMEOUT_SECS);

    // System-wide location services off: nothing to prompt for.
    if !unsafe { CLLocationManager::locationServicesEnabled_class() } {
        return Err("location services are disabled".to_string());
    }

    // Manager + delegate, both created on this thread and held
    // in locals for the whole call. `setDelegate` holds only a
    // WEAK reference, so the type-erased retained object must
    // stay alive: the local below outlives every pump slice,
    // and the call ends (thread exits) with the locals.
    let manager = unsafe { CLLocationManager::new() };
    unsafe { manager.setDesiredAccuracy(kCLLocationAccuracyBest) };
    let state = Arc::new(Mutex::new(FixState::default()));
    let delegate = ShellLocationDelegate::new(Arc::clone(&state));
    let delegate_obj: Retained<ProtocolObject<dyn CLLocationManagerDelegate>> =
        ProtocolObject::from_retained(delegate);
    unsafe { manager.setDelegate(Some(&delegate_obj)) };

    // Authorization first. Wait on the status itself, NOT on a
    // delegate callback: Core Location reports the *current*
    // status as soon as the delegate is set (still NotDetermined),
    // so a callback is not proof the user answered the prompt
    // (window round 1, W1-1 — the gateway's fix for the same
    // trap, applied here and in the probe).
    match unsafe { manager.authorizationStatus() } {
        CLAuthorizationStatus::Denied | CLAuthorizationStatus::Restricted => {
            return Err(
                "location access denied (check Location Services permission)".to_string(),
            );
        }
        CLAuthorizationStatus::NotDetermined => {
            unsafe { manager.requestWhenInUseAuthorization() };
            pump_until(deadline, || {
                let status = unsafe { manager.authorizationStatus() };
                status != CLAuthorizationStatus::NotDetermined
            });
            match unsafe { manager.authorizationStatus() } {
                CLAuthorizationStatus::Denied | CLAuthorizationStatus::Restricted => {
                    return Err(
                        "location access denied (check Location Services permission)"
                            .to_string(),
                    );
                }
                CLAuthorizationStatus::NotDetermined => {
                    return Err("location read timed out".to_string());
                }
                _ => {}
            }
        }
        // Already authorized: no prompt, straight to the fix.
        _ => {}
    }

    unsafe { manager.requestLocation() };
    pump_until(deadline, || {
        let state = state.lock().unwrap();
        state.fix.is_some() || state.failure.is_some()
    });
    let state = state.lock().unwrap();
    if let Some((latitude, longitude, accuracy_meters)) = state.fix {
        return Ok(LocationFix {
            latitude,
            longitude,
            accuracy_meters,
        });
    }
    if let Some(failure) = state.failure {
        return Err(if failure == "denied" {
            "location access denied (check Location Services permission)".to_string()
        } else {
            "location lookup failed".to_string()
        });
    }
    Err("location read timed out".to_string())
}

/// No location source on this platform: the command
/// fails the same way the old gateway route's 501 did.
#[cfg(not(target_os = "macos"))]
fn acquire_fix_on_worker() -> Result<LocationFix, String> {
    Err("location source unavailable on this platform".to_string())
}
