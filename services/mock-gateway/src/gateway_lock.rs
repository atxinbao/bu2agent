//! Gateway lockfile: the attach signal for a running gateway.
//!
//! AO-3 (AO3-J1): the lockfile is the only attach signal the
//! shell reads (`live_lock` in the shell's `gateway_home`),
//! and until this batch only the shell's `start_owned` wrote
//! it — so a gateway started any other way (launchd, a
//! terminal, a test) could never be attached to. The
//! gateway binary now writes the lockfile itself on
//! startup, whoever started it, and removes it on a
//! graceful stop. This module mirrors the shell's
//! `gateway_home` rule and JSON shape exactly
//! (`{pid, port, started_at_ms}`, `BU2AGENT_APP_DATA_DIR`
//! override, else `$HOME/Library/Application Support/
//! bu2agent`, temp file + rename, `ps -p` liveness) —
//! the two copies must agree, because the shell reads
//! what this module writes. Only `main.rs` uses it:
//! `serve()` (the library entry the test harness uses)
//! never touches the lockfile, so tests stay isolated
//! unless they opt in through `BU2AGENT_APP_DATA_DIR`.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

/// The lockfile body, byte-compatible with the shell's
/// `GatewayLock`: same field names, same JSON shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GatewayLock {
    pub pid: u32,
    pub port: u16,
    pub started_at_ms: u64,
}

/// Fixed record directory for the gateway. Override
/// for dev/tests with BU2AGENT_APP_DATA_DIR; else the
/// shell's rule. A test that sets the variable keeps
/// the gateway's lockfile out of the real app-data
/// directory.
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

/// Process liveness without new dependencies: `ps -p
/// <pid>` exits 0 only for a live process visible to
/// this user. A stale lock (dead pid, unparsable
/// file) never blocks startup.
pub fn pid_is_live(pid: u32) -> bool {
    Command::new("ps")
        .args(["-p", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// The lockfile's gateway, only when its owner process
/// still lives.
pub fn live_lock() -> Option<GatewayLock> {
    let lock = read_lock()?;
    if pid_is_live(lock.pid) {
        Some(lock)
    } else {
        None
    }
}

/// Remove the lockfile, but only when it still names
/// this process — a gateway must never delete a lock
/// another (newer) gateway wrote.
pub fn remove_lock_if_ours(pid: u32) {
    if read_lock().map(|lock| lock.pid == pid).unwrap_or(false) {
        let _ = fs::remove_file(lock_path());
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
