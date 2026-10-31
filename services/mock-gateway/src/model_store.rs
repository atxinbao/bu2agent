//! Model selection state: the staged model/base_url from
//! POST /model (Task 12).
//!
//! The staged selection is DATA ONLY: the running gateway
//! keeps the DeepSeekClient it was built with at startup, and
//! the staged values take effect only after a restart. The
//! running client is never rebuilt mid-flight (in-flight loops
//! share it). No secret ever lives here: the API key stays on
//! the env/Keychain path, and POST /model does not accept one.
//!
//! Persistence mirrors policy_store.rs: tagged JSONL snapshots
//! in one file, loaded with last-write-wins. The staged record
//! is partial on purpose: only fields the user explicitly set
//! are stored, and reads merge them over the running config,
//! so a later env change to an un-set field still shows up.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// A staged model selection. `None` per field means "not set;
/// fall back to the running config at read time".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StagedModel {
    pub model: Option<String>,
    pub base_url: Option<String>,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ModelStateLine {
    #[serde(rename = "model")]
    Model(StagedModel),
}

/// Load the staged selection; malformed lines are skipped with
/// a warning, mirroring the other stores. Only the last
/// snapshot matters.
pub fn load_model_state(path: &Path) -> Option<StagedModel> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return None;
    };
    let mut staged = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<ModelStateLine>(line) {
            Ok(ModelStateLine::Model(record)) => staged = Some(record),
            Err(_) => eprintln!("warning: skipping malformed model state line in {}", path.display()),
        }
    }
    staged
}

/// Append one state snapshot; failures are stderr warnings only,
/// mirroring the other stores.
pub fn append_model_state(path: &Path, line: &ModelStateLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append model state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append model state to {}: {err}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_line_roundtrip() {
        let record = StagedModel {
            model: Some("deepseek-flash".to_string()),
            base_url: None,
            updated_ms: 7,
        };
        let line = serde_json::to_string(&ModelStateLine::Model(record)).unwrap();
        assert!(line.contains("\"kind\":\"model\""));
        let parsed: ModelStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, ModelStateLine::Model(_)));
    }
}
