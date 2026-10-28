//! Shell-wide Mock Gateway endpoint.
//!
//! All shell commands (chat, files, agent, connectors, tasks, devices,
//! loops) talk to one gateway. The endpoint is a single process-wide
//! value so the lifecycle step (port selection, attach-or-spawn) can set
//! it once and every command follows, without changing any invoke
//! signature the frontend already calls.
//!
//! Default is the gateway's own default address. Tests set it explicitly
//! to their fake server and restore the default afterwards; the shell
//! test suite runs single-threaded, so no test observes another test's
//! value mid-flight.

use std::sync::{OnceLock, RwLock};

const DEFAULT_BASE_URL: &str = "http://127.0.0.1:3001";

static ENDPOINT: OnceLock<RwLock<String>> = OnceLock::new();

fn lock() -> &'static RwLock<String> {
    ENDPOINT.get_or_init(|| RwLock::new(String::new()))
}

/// Current gateway base URL; the default when never set.
pub fn base_url() -> String {
    let guard = lock().read().expect("gateway endpoint lock poisoned");
    if guard.is_empty() {
        DEFAULT_BASE_URL.to_string()
    } else {
        guard.clone()
    }
}

/// Override the gateway base URL (lifecycle setup, tests).
pub fn set_base_url(url: &str) {
    let mut guard = lock().write().expect("gateway endpoint lock poisoned");
    *guard = url.to_string();
}

/// Restore the default gateway base URL.
#[cfg(test)]
pub fn reset_base_url() {
    set_base_url(DEFAULT_BASE_URL);
}

/// The default gateway base URL, for tests and status text.
#[cfg(test)]
pub fn default_base_url() -> &'static str {
    DEFAULT_BASE_URL
}
