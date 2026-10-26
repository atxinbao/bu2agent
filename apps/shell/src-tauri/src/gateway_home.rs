//! Daily-app home: fixed record directory, gateway lockfile, port choice.
//!
//! The packaged app must work with zero environment variables:
//! records (approvals, tasks, device actions, loops, sessions) live under a fixed
//! app-data directory, and the working file root is user-chosen
//! (full settings page in Round 2; until then a default under the
//! record directory). Environment overrides stay for development and
//! tests only and win when set, so existing scripts and the shell
//! regression suite keep working unchanged.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// Fixed record directory for the daily app.
/// Override for dev/tests with BU2AGENT_APP_DATA_DIR.
pub fn app_data_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("BU2AGENT_APP_DATA_DIR") {
        if !dir.is_empty() {
            return PathBuf::from(dir);
        }
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join("Library")
        .join("Application Support")
        .join("bu2agent")
}

/// Default working file root (a user-chosen directory replaces it in
/// Round 2; the UI shows the full path and a Finder reveal entry).
pub fn default_working_dir() -> PathBuf {
    app_data_dir().join("files")
}

/// All paths a shell-spawned gateway needs. Record JSONLs are siblings
/// of the record directory, never inside the file root (the shell file
/// test asserts the root starts empty).
#[derive(Debug, Clone)]
pub struct GatewayPaths {
    pub files_root: PathBuf,
    pub approvals_path: PathBuf,
    pub tasks_path: PathBuf,
    pub task_drafts_dir: PathBuf,
    pub actions_path: PathBuf,
    pub loops_path: PathBuf,
    pub sessions_path: PathBuf,
}

pub fn gateway_paths(working_dir: &Path) -> GatewayPaths {
    let data = app_data_dir();
    GatewayPaths {
        files_root: working_dir.to_path_buf(),
        approvals_path: data.join("approvals.jsonl"),
        tasks_path: data.join("tasks.jsonl"),
        task_drafts_dir: data.join("task_drafts"),
        actions_path: data.join("actions.jsonl"),
        loops_path: data.join("loops.jsonl"),
        sessions_path: data.join("sessions.jsonl"),
    }
}

/// Environment a shell-spawned gateway needs. Keys the developer already
/// set are left alone (a spawned child inherits the shell's env), so
/// this only fills gaps with the fixed daily-app paths. The shell never
/// invents a model key: the gateway resolves DeepSeek from its own
/// env/Keychain lookup at startup.
pub fn gateway_env(paths: &GatewayPaths, addr: &str) -> Vec<(String, String)> {
    let mut env = Vec::new();
    let mut push = |key: &str, value: &str| {
        if std::env::var_os(key).is_none() {
            env.push((key.to_string(), value.to_string()));
        }
    };
    push("MOCK_GATEWAY_ADDR", addr);
    push("MOCK_FILES_ROOT", &paths.files_root.to_string_lossy());
    push("MOCK_APPROVALS_PATH", &paths.approvals_path.to_string_lossy());
    push("MOCK_TASKS_PATH", &paths.tasks_path.to_string_lossy());
    push("MOCK_TASK_DRAFTS_PATH", &paths.task_drafts_dir.to_string_lossy());
    push("MOCK_DEVICE_ACTIONS_PATH", &paths.actions_path.to_string_lossy());
    push("MOCK_LOOPS_PATH", &paths.loops_path.to_string_lossy());
    push("MOCK_SESSIONS_PATH", &paths.sessions_path.to_string_lossy());
    // The daily app uses the real EventKit backend on macOS; a developer
    // override (already set) still wins via the gap-fill rule above.
    #[cfg(target_os = "macos")]
    push("MOCK_DEVICE_BACKEND", "eventkit");
    env
}

/// Gateway ownership record. Only a gateway the shell started (or
/// attached to, while its owner lives) may be stopped by the shell.
/// A foreign process on the same port is never stopped.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GatewayLock {
    pub pid: u32,
    pub port: u16,
    pub started_at_ms: u64,
}

pub fn lock_path() -> PathBuf {
    app_data_dir().join("gateway.lock.json")
}

pub fn read_lock() -> Option<GatewayLock> {
    let text = fs::read_to_string(lock_path()).ok()?;
    serde_json::from_str(&text).ok()
}

/// Best-effort atomic write: temp file + rename.
pub fn write_lock(lock: &GatewayLock) -> std::io::Result<()> {
    let dir = app_data_dir();
    fs::create_dir_all(&dir)?;
    let tmp = dir.join("gateway.lock.json.tmp");
    fs::write(&tmp, serde_json::to_string(lock).unwrap())?;
    fs::rename(&tmp, lock_path())
}

pub fn remove_lock() {
    let _ = fs::remove_file(lock_path());
}

/// Process liveness without new dependencies: `ps -p <pid>` exits 0
/// only for a live process visible to this user. A stale lock (dead
/// pid, unparsable file) never blocks startup.
pub fn pid_is_live(pid: u32) -> bool {
    Command::new("ps")
        .args(["-p", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// The lockfile's gateway, only when its owner process still lives.
pub fn live_lock() -> Option<GatewayLock> {
    let lock = read_lock()?;
    if pid_is_live(lock.pid) {
        Some(lock)
    } else {
        None
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Outcome of endpoint resolution (the spawn step turns Ours into a
/// running gateway; Attached means attach and never spawn or stop).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GatewayOwnership {
    Attached,
    Ours,
}

/// Port probe: true when nothing listens on 127.0.0.1:port right now.
/// A probe is not a reservation; the spawn step retries on bind failure.
pub fn port_is_free(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// Pick a free loopback port: 3001 when free, else an ephemeral port.
pub fn pick_free_port() -> u16 {
    if port_is_free(3001) {
        return 3001;
    }
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .unwrap_or(3001)
}

/// Resolve the endpoint the shell should use: a live lock's port
/// (attach), otherwise a free port for our own gateway. A foreign
/// occupant of 3001 shows up as a non-3001 pick; it is never killed.
/// Note: pid + port agreement is the ownership signal. Pid reuse in
/// the crash window is a rare risk; the lifecycle step's health poll
/// surfaces a mis-attach as a visible anomaly instead of silent use.
pub fn resolve_endpoint() -> (u16, GatewayOwnership) {
    if let Some(lock) = live_lock() {
        return (lock.port, GatewayOwnership::Attached);
    }
    (pick_free_port(), GatewayOwnership::Ours)
}
