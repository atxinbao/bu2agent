//! Credential store abstraction, Mock level only.
//!
//! Phase 4 rule, stated plainly: this store is memory-only.
//! It never writes a secret to disk, never logs one, and never
//! returns one in an error or a redacted display. The real
//! keychain (macOS Keychain, Windows Credential Manager,
//! Linux Secret Service) is a later phase; until then, a
//! credential lives only as long as the process that holds it,
//! and a fresh store is empty. Callers must use fake tokens
//! (for example `mock-token-...`) in every demo and test.
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
        let secret = self.retrieve(handle)?;
        let tail: String = secret.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect();
        Ok(format!("****{tail}"))
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
