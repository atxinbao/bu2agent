//! Gateway lifecycle for the daily app: resolve, spawn, wait ready,
//! report status, restart, stop on exit.
//!
//! Only the real app runs this: shell setup is cfg(not(test)) gated,
//! so shell tests keep using the script-started gateway on the default
//! endpoint and never see a spawned gateway or real app-data records.
//!
//! Two API facts, verified against tauri-plugin-shell 2.5.0 sources:
//! - the spawned child is CommandChild and exposes pid() -> u32;
//! - a spawned sidecar inherits the shell's environment, so the gateway
//!   config set here with std::env::set_var reaches the child. The
//!   quit/reopen persistence acceptance proves the second one: if the
//!   child did not inherit the record paths, records would stay
//!   memory-only and vanish on reopen.
//!
//! Process control goes through the pid (libc signals) plus the
//! ps-based liveness check in gateway_home. The child handle is kept
//! for the app's lifetime and never used to kill: stop waits for
//! death (a zombie still frees the port) and proceeds after a bound.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use agent_desktop::chat_client::ChatClient;
use serde::Serialize;
use tauri::Manager;
use tauri_plugin_shell::process::CommandEvent;
use tauri_plugin_shell::ShellExt;

use crate::gateway_endpoint;
use crate::gateway_home::{self, GatewayOwnership};

/// Lifecycle state kept for the app's lifetime.
pub struct GatewayLifecycle {
    /// Handle of a gateway this shell started. Kept alive; never dropped
    /// early, never used to kill (signals go through the pid).
    child: Mutex<Option<tauri_plugin_shell::process::CommandChild>>,
    pid: Mutex<Option<u32>>,
    ownership: Mutex<GatewayOwnership>,
    port: Mutex<u16>,
    foreign_port: Mutex<bool>,
    last_error: Mutex<Option<String>>,
}

impl Default for GatewayLifecycle {
    fn default() -> Self {
        Self {
            child: Mutex::new(None),
            pid: Mutex::new(None),
            ownership: Mutex::new(GatewayOwnership::Ours),
            port: Mutex::new(3001),
            foreign_port: Mutex::new(false),
            last_error: Mutex::new(None),
        }
    }
}

impl Default for GatewayOwnership {
    fn default() -> Self {
        GatewayOwnership::Ours
    }
}

/// Status surfaced to the window. `state` is a fresh health check on
/// every call, never a cached claim.
#[derive(Debug, Serialize)]
pub struct GatewayStatus {
    pub endpoint: String,
    pub port: u16,
    pub ownership: &'static str,
    pub state: &'static str,
    pub foreign_port: bool,
    pub error: Option<String>,
}

fn health_ok(endpoint: &str) -> bool {
    ChatClient::new(endpoint)
        .and_then(|c| c.health())
        .unwrap_or(false)
}

fn wait_ready(endpoint: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        if health_ok(endpoint) {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// SIGTERM, then a bounded wait for death, then SIGKILL. Proceeds after
/// the bound either way: a zombie has already freed the port.
fn stop_pid(pid: u32) {
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }
    let start = Instant::now();
    while gateway_home::pid_is_live(pid) && start.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(100));
    }
    if gateway_home::pid_is_live(pid) {
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGKILL);
        }
    }
}

impl GatewayLifecycle {
    fn set_endpoint_info(&self, port: u16, ownership: GatewayOwnership) {
        *self.port.lock().expect("lifecycle lock poisoned") = port;
        *self.foreign_port.lock().expect("lifecycle lock poisoned") =
            ownership == GatewayOwnership::Ours && port != 3001;
        *self.ownership.lock().expect("lifecycle lock poisoned") = ownership;
    }

    fn set_error(&self, error: Option<String>) {
        *self.last_error.lock().expect("lifecycle lock poisoned") = error;
    }

    fn ownership(&self) -> GatewayOwnership {
        self.ownership.lock().expect("lifecycle lock poisoned").clone()
    }

    /// Whether this shell is attached to a foreign gateway.
    /// Settings working-dir edits are refused while attached:
    /// the foreign gateway's file root is unknown to the shell,
    /// so no directory can be validated against it.
    pub(crate) fn is_attached(&self) -> bool {
        self.ownership() != GatewayOwnership::Ours
    }

    fn port(&self) -> u16 {
        *self.port.lock().expect("lifecycle lock poisoned")
    }

    /// Spawn our own gateway on `port` and wait for readiness.
    /// On success the pid is stored and the lockfile written.
    fn start_owned(&self, app: &tauri::AppHandle, port: u16) -> Result<u32, String> {
        let data_dir = gateway_home::app_data_dir();
        let working_dir = gateway_home::default_working_dir();
        std::fs::create_dir_all(&data_dir)
            .map_err(|e| format!("cannot create app data dir: {e}"))?;
        std::fs::create_dir_all(&working_dir)
            .map_err(|e| format!("cannot create working dir: {e}"))?;
        let paths = gateway_home::gateway_paths(&working_dir);
        let addr = format!("127.0.0.1:{port}");
        for (key, value) in gateway_home::gateway_env(&paths, &addr) {
            std::env::set_var(&key, &value);
        }
        let (mut rx, child) = app
            .shell()
            .sidecar("mock-gateway")
            .map_err(|e| format!("cannot resolve gateway sidecar: {e}"))?
            .spawn()
            .map_err(|e| format!("cannot spawn gateway: {e}"))?;
        tauri::async_runtime::spawn(async move {
            while let Some(event) = rx.recv().await {
                match event {
                    CommandEvent::Stdout(bytes) => {
                        eprintln!("[gateway] {}", String::from_utf8_lossy(&bytes))
                    }
                    CommandEvent::Stderr(bytes) => {
                        eprintln!("[gateway:err] {}", String::from_utf8_lossy(&bytes))
                    }
                    _ => {}
                }
            }
        });
        let pid = child.pid();
        *self.child.lock().expect("lifecycle lock poisoned") = Some(child);
        *self.pid.lock().expect("lifecycle lock poisoned") = Some(pid);
        let endpoint = format!("http://{addr}");
        if !wait_ready(&endpoint, Duration::from_secs(15)) {
            return Err("gateway did not become ready".to_string());
        }
        gateway_home::write_lock(&gateway_home::GatewayLock {
            pid,
            port,
            started_at_ms: gateway_home::now_ms(),
        })
        .map_err(|e| format!("cannot write gateway lockfile: {e}"))?;
        Ok(pid)
    }

    /// Stop a gateway this shell started. Attached gateways are never
    /// touched. Safe to call repeatedly.
    pub fn stop_owned(&self) {
        if self.ownership() != GatewayOwnership::Ours {
            return;
        }
        if let Some(pid) = *self.pid.lock().expect("lifecycle lock poisoned") {
            stop_pid(pid);
        }
        *self.child.lock().expect("lifecycle lock poisoned") = None;
        *self.pid.lock().expect("lifecycle lock poisoned") = None;
        gateway_home::remove_lock();
    }

    fn status(&self) -> GatewayStatus {
        let endpoint = gateway_endpoint::base_url();
        let port = self.port();
        let ownership = self.ownership();
        GatewayStatus {
            state: if health_ok(&endpoint) { "ready" } else { "down" },
            endpoint,
            port,
            ownership: match ownership {
                GatewayOwnership::Ours => "owned",
                GatewayOwnership::Attached => "attached",
            },
            foreign_port: *self.foreign_port.lock().expect("lifecycle lock poisoned"),
            error: self.last_error.lock().expect("lifecycle lock poisoned").clone(),
        }
    }
}

/// App setup: resolve the endpoint, attach or spawn, wait ready.
/// Infallible by design: a failure is recorded as down + error text
/// and the window still opens, so the user sees the problem and can
/// retry with the restart entry instead of facing a crashed app.
pub fn setup(app: &tauri::AppHandle) {
    let lifecycle = app.state::<GatewayLifecycle>();
    let (port, ownership) = gateway_home::resolve_endpoint();
    lifecycle.set_endpoint_info(port, ownership.clone());
    gateway_endpoint::set_base_url(&format!("http://127.0.0.1:{port}"));
    match ownership {
        GatewayOwnership::Attached => {
            let endpoint = gateway_endpoint::base_url();
            if wait_ready(&endpoint, Duration::from_secs(5)) {
                lifecycle.set_error(None);
            } else {
                lifecycle.set_error(Some("attached gateway not responding".to_string()));
            }
        }
        GatewayOwnership::Ours => match lifecycle.start_owned(app, port) {
            Ok(_) => lifecycle.set_error(None),
            Err(error) => lifecycle.set_error(Some(error)),
        },
    }
}

/// Window status query: fresh health check, stored ownership/port.
#[tauri::command]
pub fn gateway_status(state: tauri::State<'_, GatewayLifecycle>) -> GatewayStatus {
    state.status()
}

/// Restart a gateway this shell owns. Attached gateways are refused:
/// this shell must never stop a gateway it did not start.
#[tauri::command]
pub fn gateway_restart(
    app: tauri::AppHandle,
    state: tauri::State<'_, GatewayLifecycle>,
) -> Result<GatewayStatus, String> {
    if state.ownership() != GatewayOwnership::Ours {
        return Err("attached gateway is not managed by this app".to_string());
    }
    state.stop_owned();
    let (port, ownership) = gateway_home::resolve_endpoint();
    state.set_endpoint_info(port, ownership.clone());
    gateway_endpoint::set_base_url(&format!("http://127.0.0.1:{port}"));
    match ownership {
        GatewayOwnership::Attached => {
            state.set_error(Some("another gateway took the port".to_string()));
        }
        GatewayOwnership::Ours => match state.start_owned(&app, port) {
            Ok(_) => state.set_error(None),
            Err(error) => state.set_error(Some(error)),
        },
    }
    Ok(state.status())
}
