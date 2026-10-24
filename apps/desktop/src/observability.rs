//! Redacted logging, fatal reporting and bug-report export.
//!
//! Rule: token, cookie, recovery secret, full user input, mail
//! body, local path text and attachment body must never reach
//! ordinary logs, fixtures or bug reports. This module is the
//! single place that rule is enforced for Rust-side logs.

/// Very small, explicit redaction pass. It is a safety net, not
/// a substitute for never logging secrets in the first place:
/// callers must still avoid passing secrets to log calls.
pub fn redact(text: &str) -> String {
    let mut out = text.to_string();
    for marker in [
        "token=",
        "Authorization:",
        "vesta-recovery-secret",
        "hatch-cvm-credentials",
    ] {
        if let Some(idx) = out.find(marker) {
            let end = (idx + marker.len() + 32).min(out.len());
            out.replace_range(idx..end, "[REDACTED]");
        }
    }
    out
}

/// Log categories, kept separate (baseline 9.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogCategory {
    Transport,
    Gateway,
    Permission,
    Tool,
    Stream,
    UiState,
}
