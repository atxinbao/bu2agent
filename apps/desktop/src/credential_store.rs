//! Credential store abstraction, Mock level only.
//!
//! Two stores share one trait. `MemoryCredentialStore` is
//! memory-only: a credential lives only as long as the process
//! that holds it, and a fresh store is empty. `KeychainCredentialStore`
//! (stage 2) persists through the OS keychain: macOS Keychain,
//! Windows Credential Manager, or Linux Secret Service, via the
//! `keyring` crate. Neither store ever logs a secret or returns
//! one in an error or a redacted display. Callers must use fake
//! tokens (for example `mock-token-...`) in every demo and test.
//!
//! A handle is not a secret: it names a slot. The redacted
//! display shows only the last 4 characters of a stored value,
//! so a log line can prove a slot is filled without leaking it.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::{ErrorCode, Result, UnifiedError, UserAction};

fn not_found() -> UnifiedError {
    UnifiedError {
        code: ErrorCode::InvalidInput,
        capability: Some("credentials".to_string()),
        retryable: false,
        user_action: UserAction::None,
        redacted_detail: "credential handle not found".to_string(),
    }
}

pub trait CredentialStore {
    fn store(&self, handle: &str, secret: &str) -> Result<()>;
    fn retrieve(&self, handle: &str) -> Result<String>;
    fn redacted(&self, handle: &str) -> Result<String>;
    fn remove(&self, handle: &str) -> Result<()>;
}

#[derive(Debug, Default)]
pub struct MemoryCredentialStore {
    slots: Mutex<HashMap<String, String>>,
}

impl MemoryCredentialStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl CredentialStore for MemoryCredentialStore {
    fn store(&self, handle: &str, secret: &str) -> Result<()> {
        if handle.is_empty() || secret.is_empty() {
            return Err(not_found());
        }
        self.slots
            .lock()
            .expect("credential lock")
            .insert(handle.to_string(), secret.to_string());
        Ok(())
    }

    fn retrieve(&self, handle: &str) -> Result<String> {
        self.slots
            .lock()
            .expect("credential lock")
            .get(handle)
            .cloned()
            .ok_or_else(not_found)
    }

    fn redacted(&self, handle: &str) -> Result<String> {
        Ok(redact(&self.retrieve(handle)?))
    }

    fn remove(&self, handle: &str) -> Result<()> {
        let removed = self.slots.lock().expect("credential lock").remove(handle).is_some();
        if removed {
            Ok(())
        } else {
            Err(not_found())
        }
    }
}

/// Last-4 redaction shared by both stores: a log line can prove
/// a slot is filled without leaking the secret. Public so the
/// gateway's model surface can redact the DeepSeek key the
/// same way without duplicating the logic.
pub fn redact(secret: &str) -> String {
    let tail: String = secret.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect();
    format!("****{tail}")
}

/// OS-keychain-backed store (stage 2). The service name groups
/// entries; the handle is the account name. Secrets never pass
/// through logs or error details: failures map to a generic
/// keychain error, and a missing entry maps to not_found, exactly
/// like the memory store.
pub struct KeychainCredentialStore {
    service: String,
}

fn keychain_failed() -> UnifiedError {
    UnifiedError {
        code: ErrorCode::PermissionDenied,
        capability: Some("credentials".to_string()),
        retryable: false,
        user_action: UserAction::None,
        redacted_detail: "keychain access failed".to_string(),
    }
}

impl KeychainCredentialStore {
    pub fn new() -> Self {
        Self { service: "bu2agent".to_string() }
    }

    pub fn with_service(service: &str) -> Self {
        Self { service: service.to_string() }
    }

    fn entry(&self, handle: &str) -> Result<keyring::Entry> {
        keyring::Entry::new(&self.service, handle).map_err(|_| keychain_failed())
    }
}

impl Default for KeychainCredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

impl CredentialStore for KeychainCredentialStore {
    fn store(&self, handle: &str, secret: &str) -> Result<()> {
        if handle.is_empty() || secret.is_empty() {
            return Err(not_found());
        }
        self.entry(handle)?.set_password(secret).map_err(|_| keychain_failed())
    }

    fn retrieve(&self, handle: &str) -> Result<String> {
        match self.entry(handle)?.get_password() {
            Ok(secret) => Ok(secret),
            Err(keyring::Error::NoEntry) => Err(not_found()),
            Err(_) => Err(keychain_failed()),
        }
    }

    fn redacted(&self, handle: &str) -> Result<String> {
        Ok(redact(&self.retrieve(handle)?))
    }

    fn remove(&self, handle: &str) -> Result<()> {
        match self.entry(handle)?.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Err(not_found()),
            Err(_) => Err(keychain_failed()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_shows_only_last_four() {
        assert_eq!(redact("mock-token-1234"), "****1234");
        assert_eq!(redact("ab"), "****ab");
    }

    #[test]
    fn memory_store_roundtrip_and_missing() {
        let store = MemoryCredentialStore::new();
        store.store("h", "mock-token-1234").expect("store");
        assert_eq!(store.retrieve("h").expect("retrieve"), "mock-token-1234");
        assert_eq!(store.redacted("h").expect("redacted"), "****1234");
        store.remove("h").expect("remove");
        assert!(store.retrieve("h").is_err());
    }

    /// Keychain roundtrip. Env-gated: it touches the real OS
    /// keychain (and may prompt once on macOS), so CI and plain
    /// `cargo test` runs skip it. Run it on a Mac with
    /// BU2AGENT_KEYCHAIN_TEST=1; it removes its own entry.
    #[test]
    fn keychain_roundtrip_when_enabled() {
        if std::env::var("BU2AGENT_KEYCHAIN_TEST").is_err() {
            return;
        }
        let store = KeychainCredentialStore::with_service("bu2agent-test");
        let _ = store.remove("roundtrip");
        store.store("roundtrip", "mock-token-1234").expect("keychain store");
        assert_eq!(
            store.retrieve("roundtrip").expect("keychain retrieve"),
            "mock-token-1234"
        );
        assert_eq!(store.redacted("roundtrip").expect("keychain redacted"), "****1234");
        store.remove("roundtrip").expect("keychain remove");
        assert!(store.retrieve("roundtrip").is_err());
    }
}
