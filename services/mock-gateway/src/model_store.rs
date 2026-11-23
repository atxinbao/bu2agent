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
/// ORC-2 (Q-ORC-08): the Planner/Executor role slots.
/// Both are optional and fall back through the chain
/// staged slot -> staged model -> running model
/// (`resolve_role_models`); the effective default of
/// either slot is the current model — never an automatic
/// switch to a cheaper model.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StagedModel {
    pub model: Option<String>,
    pub base_url: Option<String>,
    pub planner_model: Option<String>,
    pub executor_model: Option<String>,
    pub updated_ms: u64,
}

/// Resolve the Planner/Executor models for a call
/// (ORC-2, Q-ORC-08). The chain is read live from the
/// staged record at call time: a staged slot wins, else
/// the staged base model, else the running model. The
/// slots need no restart to take effect — the per-call
/// model override carries the model string per request,
/// so no client is rebuilt (the base `model` keeps its
/// Task 12 D3 restart semantics because the shared
/// client IS built at startup).
pub fn resolve_role_models(staged: &StagedModel, running_model: &str) -> (String, String) {
    let planner = staged
        .planner_model
        .clone()
        .or_else(|| staged.model.clone())
        .unwrap_or_else(|| running_model.to_string());
    let executor = staged
        .executor_model
        .clone()
        .or_else(|| staged.model.clone())
        .unwrap_or_else(|| running_model.to_string());
    (planner, executor)
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
            planner_model: Some("deepseek-planner".to_string()),
            executor_model: None,
            updated_ms: 7,
        };
        let line = serde_json::to_string(&ModelStateLine::Model(record)).unwrap();
        assert!(line.contains("\"kind\":\"model\""));
        let parsed: ModelStateLine = serde_json::from_str(&line).unwrap();
        assert!(matches!(parsed, ModelStateLine::Model(_)));
    }

    #[test]
    fn role_models_fall_back_through_staged_model_to_running() {
        // No slots staged: both roles fall back to the
        // staged base model, else the running model.
        let staged = StagedModel {
            model: Some("staged-base".to_string()),
            ..Default::default()
        };
        assert_eq!(
            resolve_role_models(&staged, "running"),
            ("staged-base".to_string(), "staged-base".to_string())
        );
        let empty = StagedModel::default();
        assert_eq!(
            resolve_role_models(&empty, "running"),
            ("running".to_string(), "running".to_string())
        );
        // A staged slot wins over the staged base model.
        let slotted = StagedModel {
            model: Some("staged-base".to_string()),
            planner_model: Some("planner-x".to_string()),
            ..Default::default()
        };
        assert_eq!(
            resolve_role_models(&slotted, "running"),
            ("planner-x".to_string(), "staged-base".to_string())
        );
    }
}
