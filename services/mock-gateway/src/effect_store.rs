//! Effect records persisted as JSONL, mirroring the execution store.
//!
//! Phase 2 (PA P0) convergence: one Effect line per durable side
//! effect, dual-written next to the old record it mirrors. The old
//! record stays authoritative for its existing read paths in this
//! phase; the Effect line is an append-only observation, never a
//! gate: append failures are stderr warnings, never fatal, and a
//! failed append never changes the response of the operation that
//! produced the effect.
//!
//! Field mapping (draft §3, Q4/Q5 as approved):
//! - `effect_id`: `effect-N`, a global counter like `execution-N`
//!   (Q1 numbering, new effects only).
//! - `execution_id`: the phase-1 `execution-N` of the attempt that
//!   produced the effect, when one exists. Loop calls and task
//!   writes resolve it from their loop/task id; direct REST writes
//!   (files, connectors) have no attempt and carry `None`.
//! - `capability` / `operation` / `scope`: the resource triple.
//!   Loop calls use the loop tool name split as
//!   (`fs`/`reminders`, `write`/`set_completed`, normalized path or
//!   reminder id); device actions use (`devices`, `set`, action id);
//!   direct writes use (`fs`, write/delete/rename/mkdir/export,
//!   normalized path); task writes use (`tasks`, `execute`, task
//!   id); connector writes use (`connectors`/`custom-connectors`/
//!   `credentials`/`catalog`, the management operation, the
//!   connector/account/credential/catalog id).
//! - `input_digest`: the Q5 unified digest
//!   (`operation_binding::operation_digest`), i.e. the same value
//!   the approval gate consumed, for approval-bound operations.
//!   Management-plane connector writes have no approval, so their
//!   digest commits to (`capability.operation`, `{id: scope}`).
//! - `approval_id`: the approval that authorized the operation, when
//!   one exists; `None` for management-plane writes.
//! - `status`: the existing status vocabulary of the mirrored
//!   record, carried verbatim. No new state machine is invented
//!   here: state convergence is phase 4. Loop calls use the
//!   `CallStatus` word (`succeeded`/`failed`/`unknown`); device
//!   actions use the action status (`completed`/`failed`/
//!   `unknown`/`awaiting_authorization`); one-shot REST writes
//!   record only their success outcome (`completed` for file and
//!   connector writes; task writes record `completed` on success
//!   and `failed` when the artifact write fails the task).
//!
//! Time and idempotency keys are Q7 (undecided): this phase records
//! no timestamps and no idempotency key beyond the existing
//! digest + approval id it already carries. A restart reloads
//! effects last-line-wins like every other JSONL store here; the
//! counter resumes past the largest `effect-N` suffix seen.
//!
//! P0-A (PA contract §3) adds the idempotency key and record
//! provenance, nothing else: `idempotency_key` is computed from
//! the execution id plus the effect semantic identity
//! (`capability`, `operation`, `scope`, `input_digest`) in the
//! `exec:<execution>:<capability>.<operation>:<scope>:<digest>`
//! encoding, and only when the execution id and the digest are
//! both present. Rows written without an execution (direct
//! writes, phase 2) keep a null key: forcing attribution is the
//! P0-B adapter's job, not this phase's. No gate reads the key
//! yet (enforcement is P0-C); no status word changes here.
//! Old lines without the new fields still parse: schema version
//! 0, origin "unknown", null key, never backfilled.
//!
//! P0-C (PA contract §4/§5) enforces the lifecycle on new rows:
//! `planned -> authorized -> started -> succeeded | failed |
//! unknown -> reconciling -> reconciled`. Transitions are checked
//! by `valid_effect_transition`; anything else is refused and
//! leaves the row unchanged. `reconciling -> unknown` is allowed:
//! an inconclusive re-check must return the Effect to `unknown`
//! (it may be re-checked later), never to `failed`. A re-check
//! that conclusively proves the outcome lands on `reconciled`
//! with its conclusion in `reconciliation`; the old `unknown`
//! line stays in the JSONL history, last line wins. Old rows keep
//! their old status word verbatim on read (never translated or
//! rewritten); `map_legacy_status_to_new` is only for deriving
//! the new word of a freshly recorded outcome. `NeedsReview`
//! never enters this vocabulary: it is an Execution-layer state.

use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// Origin reported for records written before P0-A added the
/// field, and for any line that omits it. Deliberately not a
/// device id: "unknown" means "no provenance recorded".
fn unknown_origin() -> String {
    "unknown".to_string()
}

/// P0-C reconciliation conclusion, written when an `unknown`
/// Effect is re-checked to a conclusive outcome. `result` is
/// `completed` (the re-check proved the write landed) or `failed`
/// (the re-check conclusively proved it cannot land, e.g. the
/// target reminder no longer exists). An inconclusive re-check
/// never produces one of these: it returns the Effect to
/// `unknown` with no `reconciliation` attached.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reconciliation {
    pub result: String,
    pub checked_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Wall-clock milliseconds since the Unix epoch, for
/// reconciliation timestamps. Best-effort: 0 when the clock is
/// unavailable (never a reason to refuse a re-check).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EffectFileRecord {
    pub effect_id: String,
    /// Phase-1 execution that produced this effect, when one exists.
    #[serde(default)]
    pub execution_id: Option<String>,
    /// Resource family: `fs`, `reminders`, `devices`, `tasks`,
    /// `connectors`, `custom-connectors`, `credentials`, `catalog`.
    pub capability: String,
    /// Operation within the capability, e.g. `write`, `set`,
    /// `execute`, `link`.
    pub operation: String,
    /// The affected resource: a normalized path, a reminder id, an
    /// action id, a task id, or a connector/credential/catalog id.
    /// Renames name both ends, normalized, as `from -> to`.
    pub scope: String,
    /// Q5 unified digest of the executed parameters, when the
    /// operation carries one.
    #[serde(default)]
    pub input_digest: Option<String>,
    /// Approval that authorized the operation, when one exists.
    #[serde(default)]
    pub approval_id: Option<String>,
    /// Existing status word of the mirrored record, verbatim.
    pub status: String,
    /// P0-A idempotency key, `exec:<execution>:<capability>.
    /// <operation>:<scope>:<digest>`; null when the execution id
    /// or the digest is missing (phase 2 rows, direct writes).
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Device that produced this effect, resolved at gateway
    /// startup (see `resolve_origin_device_id`); "unknown" for
    /// pre-P0-A lines.
    #[serde(default = "unknown_origin")]
    pub origin_device_id: String,
    /// Contract schema version: 1 for rows written by P0-A or
    /// later, 0 for pre-P0-A rows read without the field.
    #[serde(default)]
    pub schema_version: u64,
    /// P0-C reconciliation conclusion, present only on
    /// `reconciled` rows. Absent (null) on every other status,
    /// including `unknown` rows that survived an inconclusive
    /// re-check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation: Option<Reconciliation>,
    /// Optional free-text marker, e.g. the system-authorization
    /// note carried by an Effect whose device action is awaiting
    /// authorization. Never a status word and never a gate input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// P0-C lifecycle vocabulary (contract §4). `NeedsReview` is
/// deliberately absent: it belongs to the Execution layer.
pub fn new_effect_statuses() -> [&'static str; 8] {
    [
        "planned",
        "authorized",
        "started",
        "succeeded",
        "failed",
        "unknown",
        "reconciling",
        "reconciled",
    ]
}

pub fn is_new_effect_status(status: &str) -> bool {
    new_effect_statuses().contains(&status)
}

/// Terminal lifecycle states: no further transition is allowed
/// from any of these.
pub fn is_terminal_effect_status(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "reconciled")
}

/// P0-C transition enforcement (contract §4). Only the lifecycle
/// edges are allowed. In particular: `planned` may never jump to
/// `started`/`succeeded`/`failed`; a terminal state never moves
/// again; `unknown` may never jump straight to `succeeded`,
/// `failed` or `reconciled` (it must pass through `reconciling`);
/// and `reconciling` returns to `unknown` on an inconclusive
/// re-check, so a failed re-check never writes `unknown` as
/// `failed`.
pub fn valid_effect_transition(from: &str, to: &str) -> bool {
    matches!(
        (from, to),
        ("planned", "authorized")
            | ("authorized", "started")
            | ("started", "succeeded")
            | ("started", "failed")
            | ("started", "unknown")
            | ("unknown", "reconciling")
            | ("reconciling", "reconciled")
            | ("reconciling", "unknown")
    )
}

/// Contract §4 old-word mapping, for deriving the new word of a
/// freshly recorded outcome only. History rows are never passed
/// through this function on read: they keep their old word.
pub fn map_legacy_status_to_new(old: &str) -> Option<&'static str> {
    match old {
        "proposed" | "awaiting_approval" | "requested" | "awaiting_authorization" => {
            Some("planned")
        }
        "approved" => Some("authorized"),
        "running" => Some("started"),
        "completed" | "succeeded" => Some("succeeded"),
        "failed" | "rejected" => Some("failed"),
        "unknown" => Some("unknown"),
        _ => None,
    }
}

/// P0-A idempotency key (contract §3): the execution id plus the
/// effect semantic identity, in a readable encoding. Every input
/// is device-independent (paths normalized, digests unified), so
/// the same semantic effect in the same execution computes the
/// same key on any worker. The key deliberately carries no device
/// value: changing workers must not change the key.
pub fn idempotency_key(
    execution_id: &str,
    capability: &str,
    operation: &str,
    scope: &str,
    input_digest: &str,
) -> String {
    format!("exec:{execution_id}:{capability}.{operation}:{scope}:{input_digest}")
}

/// Arguments for recording one Effect: every field of
/// `EffectFileRecord` except the server-assigned `effect_id`.
/// Grouped into one struct so the record call stays under
/// clippy's argument limit and call sites read as a record
/// literal, not a positional argument list.
pub struct NewEffect {
    pub execution_id: Option<String>,
    pub capability: String,
    pub operation: String,
    pub scope: String,
    pub input_digest: Option<String>,
    pub approval_id: Option<String>,
    pub status: String,
}

pub fn load_effects(path: &Path) -> Vec<EffectFileRecord> {
    let Ok(content) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (index, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<EffectFileRecord>(line) {
            Ok(record) => out.push(record),
            Err(_) => {
                eprintln!(
                    "warning: skipping malformed effect record at line {} in {}",
                    index + 1,
                    path.display()
                );
            }
        }
    }
    out
}

pub fn append_effect(path: &Path, record: &EffectFileRecord) {
    let line = match serde_json::to_string(record) {
        Ok(line) => line,
        Err(_) => {
            eprintln!("warning: cannot serialize effect record {}", record.effect_id);
            return;
        }
    };
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("warning: cannot append effect record to {}: {e}", path.display());
            return;
        }
    };
    if let Err(e) = writeln!(file, "{line}") {
        eprintln!("warning: cannot write effect record to {}: {e}", path.display());
    }
}

/// Next effect number: one past the largest `effect-N` suffix seen, minimum 1.
pub fn next_effect_number(records: &[EffectFileRecord]) -> u64 {
    records
        .iter()
        .filter_map(|r| r.effect_id.strip_prefix("effect-")?.parse::<u64>().ok())
        .max()
        .map(|n| n + 1)
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("mock-effects-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn sample(id: &str) -> EffectFileRecord {
        EffectFileRecord {
            effect_id: id.to_string(),
            execution_id: Some("execution-1".to_string()),
            capability: "fs".to_string(),
            operation: "write".to_string(),
            scope: "a.txt".to_string(),
            input_digest: Some("abc".to_string()),
            approval_id: Some("approval-1".to_string()),
            status: "completed".to_string(),
            idempotency_key: Some(
                idempotency_key("execution-1", "fs", "write", "a.txt", "abc"),
            ),
            origin_device_id: "mac-local".to_string(),
            schema_version: 1,
            reconciliation: None,
            detail: None,
        }
    }

    #[test]
    fn next_effect_number_skips_gaps_and_unknown_ids() {
        assert_eq!(next_effect_number(&[]), 1);
        assert_eq!(
            next_effect_number(&[sample("effect-2"), sample("effect-7"), sample("other")]),
            8
        );
    }

    #[test]
    fn round_trip_skips_malformed_lines() {
        let dir = temp_dir("round-trip");
        let path = dir.join("effects.jsonl");
        append_effect(&path, &sample("effect-1"));
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).expect("open for append");
            writeln!(f, "not json").expect("append malformed");
        }
        append_effect(&path, &sample("effect-2"));
        let loaded = load_effects(&path);
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].effect_id, "effect-1");
        assert_eq!(loaded[1].effect_id, "effect-2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn old_lines_without_new_fields_still_parse() {
        let dir = temp_dir("compat");
        let path = dir.join("effects.jsonl");
        std::fs::write(
            &path,
            "{\"effect_id\":\"effect-1\",\"capability\":\"fs\",\"operation\":\"write\",\"scope\":\"a.txt\",\"status\":\"completed\"}",
        )
        .expect("write old line");
        let loaded = load_effects(&path);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].execution_id, None);
        assert_eq!(loaded[0].input_digest, None);
        assert_eq!(loaded[0].approval_id, None);
        // P0-A provenance defaults for pre-P0-A rows.
        assert_eq!(loaded[0].idempotency_key, None);
        assert_eq!(loaded[0].origin_device_id, "unknown");
        assert_eq!(loaded[0].schema_version, 0);
        // P0-C: history rows keep their old word verbatim and
        // carry no reconciliation or detail.
        assert_eq!(loaded[0].status, "completed");
        assert_eq!(loaded[0].reconciliation, None);
        assert_eq!(loaded[0].detail, None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lifecycle_allows_only_the_contract_edges() {
        // Full positive chain.
        for (from, to) in [
            ("planned", "authorized"),
            ("authorized", "started"),
            ("started", "succeeded"),
            ("started", "failed"),
            ("started", "unknown"),
            ("unknown", "reconciling"),
            ("reconciling", "reconciled"),
            // Inconclusive re-check returns to unknown (P0-C
            // choice, nailed by the hard constraint that a failed
            // re-check never writes unknown as failed).
            ("reconciling", "unknown"),
        ] {
            assert!(valid_effect_transition(from, to), "{from} -> {to}");
        }
        // Illegal jumps and terminal re-transitions are refused.
        for (from, to) in [
            ("planned", "started"),
            ("planned", "succeeded"),
            ("planned", "failed"),
            ("authorized", "succeeded"),
            ("unknown", "succeeded"),
            ("unknown", "failed"),
            ("unknown", "reconciled"),
            ("succeeded", "planned"),
            ("failed", "reconciling"),
            ("reconciled", "unknown"),
            ("planned", "planned"),
            ("started", "planned"),
            ("started", "authorized"),
            ("authorized", "planned"),
        ] {
            assert!(!valid_effect_transition(from, to), "{from} -> {to}");
        }
        // NeedsReview never enters the Effect vocabulary.
        assert!(!is_new_effect_status("needs_review"));
        assert!(is_terminal_effect_status("succeeded"));
        assert!(is_terminal_effect_status("failed"));
        assert!(is_terminal_effect_status("reconciled"));
        assert!(!is_terminal_effect_status("unknown"));
        assert!(!is_terminal_effect_status("reconciling"));
    }

    #[test]
    fn legacy_mapping_covers_the_contract_table() {
        assert_eq!(map_legacy_status_to_new("proposed"), Some("planned"));
        assert_eq!(map_legacy_status_to_new("awaiting_approval"), Some("planned"));
        assert_eq!(map_legacy_status_to_new("requested"), Some("planned"));
        assert_eq!(map_legacy_status_to_new("approved"), Some("authorized"));
        assert_eq!(map_legacy_status_to_new("running"), Some("started"));
        assert_eq!(map_legacy_status_to_new("completed"), Some("succeeded"));
        assert_eq!(map_legacy_status_to_new("failed"), Some("failed"));
        assert_eq!(map_legacy_status_to_new("rejected"), Some("failed"));
        assert_eq!(map_legacy_status_to_new("unknown"), Some("unknown"));
        assert_eq!(map_legacy_status_to_new("awaiting_authorization"), Some("planned"));
        assert_eq!(map_legacy_status_to_new("bogus"), None);
    }

    #[test]
    fn idempotency_key_is_stable_and_execution_scoped() {
        let key = idempotency_key("execution-1", "fs", "write", "a.txt", "abc");
        assert_eq!(key, "exec:execution-1:fs.write:a.txt:abc");
        // Same semantic effect, same execution, any worker: same key.
        assert_eq!(key, idempotency_key("execution-1", "fs", "write", "a.txt", "abc"));
        // Different execution: different key (no cross-execution dedup).
        assert_ne!(key, idempotency_key("execution-2", "fs", "write", "a.txt", "abc"));
        // Different scope or digest: different key.
        assert_ne!(key, idempotency_key("execution-1", "fs", "write", "b.txt", "abc"));
        assert_ne!(key, idempotency_key("execution-1", "fs", "write", "a.txt", "abd"));
        // No device value anywhere in the key.
        assert!(!key.contains("mac-local") && !key.contains("worker-9"));
    }
}
