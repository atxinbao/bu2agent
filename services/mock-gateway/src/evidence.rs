//! P0-R observation: read-back of concluded Effects against the
//! world, and write-time baselines (Evidence).
//!
//! Two different questions, kept apart on purpose:
//! - reconcile asks "did this write land?" for an `unknown` (or
//!   `started`) row. Anything but the device-reminder class can
//!   only prove "landed"; ambiguity returns to `unknown`, never
//!   `failed`.
//! - verify asks "does this `succeeded` conclusion still hold?"
//!   for a `succeeded` row, comparing the world against the
//!   write-time Evidence baseline (new rows) or a recomputation
//!   (legacy rows where the class allows it).
//!
//! Read-back paths come only from the Effect row (capability,
//! operation, scope, input_digest) and Evidence rows. Callers
//! never pass a path or content in: verify takes only an
//! effect id. File classes use the same normalization and root
//! containment as writes (`resolve_file`); export read-back
//! reads only the Evidence-recorded absolute path, regular files
//! only, symlinks refused as inconclusive. Connector classes
//! read only existence and flag bits, never credential values.
//! Observed JSON carries hashes, lengths, kinds, and flags only:
//! never file bytes, credential values, or session text.

use serde_json::{json, Value};
use std::path::Path;

use crate::effect_store::EffectFileRecord;
use crate::operation_binding::sha256_hex;
use crate::server::{resolve_file, AppState};
use crate::sqlite_store::{now_ms, EvidenceDraft, EvidenceRow};

/// Owned evidence payload, ready to serialize into an
/// `EvidenceDraft` at the store boundary.
pub struct OwnedEvidence {
    pub effect_id: String,
    pub execution_id: Option<String>,
    pub source: String,
    pub verdict: String,
    pub reason: String,
    pub observed_json: String,
    pub checked_ms: i64,
    pub origin_device_id: String,
    pub schema_version: i64,
}

impl OwnedEvidence {
    pub fn as_draft(&self) -> EvidenceDraft<'_> {
        EvidenceDraft {
            effect_id: &self.effect_id,
            execution_id: self.execution_id.as_deref(),
            source: &self.source,
            verdict: &self.verdict,
            reason: &self.reason,
            observed_json: &self.observed_json,
            checked_ms: self.checked_ms,
            origin_device_id: &self.origin_device_id,
            schema_version: self.schema_version,
        }
    }
}

fn owned(
    effect: &EffectFileRecord,
    origin_device_id: &str,
    source: &str,
    verdict: &str,
    reason: &str,
    observed: Value,
) -> OwnedEvidence {
    OwnedEvidence {
        effect_id: effect.effect_id.clone(),
        execution_id: effect.execution_id.clone(),
        source: source.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        observed_json: serde_json::to_string(&observed).unwrap_or_else(|_| "{}".to_string()),
        checked_ms: now_ms() as i64,
        origin_device_id: origin_device_id.to_string(),
        schema_version: 1,
    }
}

/// Current world state for one observation subject.
#[derive(Debug, Clone)]
pub enum Current {
    File { sha256: String, len: u64 },
    Absent,
    Dir,
    Symlink,
    Unreadable,
    /// Connector/vault subject: whether any record exists, and
    /// whether it is live (linked / connected / not deleted).
    Record { present: bool, active: bool },
    Reminder { completed: bool },
    ReminderMissing,
    /// The task record itself is missing.
    Missing,
    Task {
        status: String,
        artifact: Option<Box<Current>>,
    },
    Rename {
        source: Box<Current>,
        target: Box<Current>,
    },
}

impl Current {
    pub fn to_json(&self) -> Value {
        match self {
            Current::File { sha256, len } => json!({"kind": "file", "sha256": sha256, "len": len}),
            Current::Absent => json!({"kind": "absent"}),
            Current::Dir => json!({"kind": "dir"}),
            Current::Symlink => json!({"kind": "symlink"}),
            Current::Unreadable => json!({"kind": "unreadable"}),
            Current::Record { present, active } => {
                json!({"kind": "record", "present": present, "active": active})
            }
            Current::Reminder { completed } => json!({"kind": "reminder", "completed": completed}),
            Current::ReminderMissing => json!({"kind": "absent"}),
            Current::Missing => json!({"kind": "missing"}),
            Current::Task { status, artifact } => json!({
                "kind": "task",
                "status": status,
                "artifact": artifact.as_ref().map(|a| a.to_json()),
            }),
            Current::Rename { source, target } => json!({
                "kind": "rename",
                "source": source.to_json(),
                "target": target.to_json(),
            }),
        }
    }
}

/// Why an observation cannot be made at all (distinct from an
/// inconclusive observation, which is a row with a verdict).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserveError {
    /// No observation method exists for this class (future tool
    /// rows). Reconcile maps this to `effect_no_readback`.
    NoMethod,
    /// The row lacks the information an observation needs:
    /// legacy append / export / rename rows without write-time
    /// Evidence, or rows without an input digest. Both drivers
    /// map this to `evidence_missing`.
    MissingEvidence,
}

fn file_state(path: &Path) -> Current {
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Current::Absent,
        Err(_) => Current::Unreadable,
        Ok(md) => {
            if md.file_type().is_symlink() {
                Current::Symlink
            } else if md.is_dir() {
                Current::Dir
            } else if md.is_file() {
                match std::fs::read(path) {
                    Ok(bytes) => Current::File {
                        sha256: sha256_hex(&bytes),
                        len: bytes.len() as u64,
                    },
                    Err(_) => Current::Unreadable,
                }
            } else {
                Current::Unreadable
            }
        }
    }
}

fn root_file_state(state: &AppState, rel: &str) -> Current {
    match resolve_file(&state.files_root, rel) {
        Ok(full) => file_state(&full),
        Err(_) => Current::Unreadable,
    }
}

fn parse_rename_scope(scope: &str) -> Option<(String, String)> {
    scope
        .split_once(" -> ")
        .map(|(from, to)| (from.to_string(), to.to_string()))
}

fn baseline_field(evidence: &EvidenceRow, field: &str) -> Option<Value> {
    serde_json::from_str::<Value>(&evidence.observed_json)
        .ok()?
        .get(field)
        .cloned()
}

/// Whether a connector/vault operation expects the subject to
/// be live afterwards. Link/connect/register/update/capture/
/// fulfill expect live; unlink/disconnect/delete expect not live.
fn expects_active(capability: &str, operation: &str) -> Option<bool> {
    match (capability, operation) {
        ("connectors", "link")
        | ("connectors", "connect")
        | ("custom-connectors", "register")
        | ("credentials", "register")
        | ("credentials", "capture")
        | ("credentials", "fulfill")
        | ("catalog", "update") => Some(true),
        ("connectors", "unlink")
        | ("connectors", "disconnect")
        | ("custom-connectors", "delete")
        | ("credentials", "delete")
        | ("catalog", "delete") => Some(false),
        _ => None,
    }
}

fn connector_current(state: &AppState, capability: &str, operation: &str, scope: &str) -> Current {
    match capability {
        "connectors" => match operation {
            "link" | "unlink" => {
                let accounts = state.connector_accounts.lock().expect("connector lock");
                match accounts.get(scope) {
                    Some(a) => Current::Record {
                        present: true,
                        active: !a.unlinked,
                    },
                    None => Current::Record {
                        present: false,
                        active: false,
                    },
                }
            }
            "connect" | "disconnect" => {
                let conns = state
                    .connector_connections
                    .lock()
                    .expect("connector lock");
                match conns.get(scope) {
                    Some(true) => Current::Record {
                        present: true,
                        active: true,
                    },
                    Some(false) => Current::Record {
                        present: true,
                        active: false,
                    },
                    None => Current::Record {
                        present: false,
                        active: false,
                    },
                }
            }
            _ => Current::Unreadable,
        },
        "custom-connectors" => {
            let defs = state.vault_custom.lock().expect("vault lock");
            match defs.get(scope) {
                Some(d) => Current::Record {
                    present: true,
                    active: !d.deleted,
                },
                None => Current::Record {
                    present: false,
                    active: false,
                },
            }
        }
        "credentials" => {
            let slots = state.vault_slots.lock().expect("vault lock");
            match slots.get(scope) {
                Some(s) => Current::Record {
                    present: true,
                    active: !s.deleted,
                },
                None => Current::Record {
                    present: false,
                    active: false,
                },
            }
        }
        "catalog" => {
            let entries = state.vault_catalog.lock().expect("vault lock");
            match entries.get(scope) {
                Some(e) => Current::Record {
                    present: true,
                    active: !e.deleted,
                },
                None => Current::Record {
                    present: false,
                    active: false,
                },
            }
        }
        _ => Current::Unreadable,
    }
}

/// Read the current world state for an Effect row. `evidence`
/// is the row's write-time Evidence (source `write`), when one
/// exists; export needs it for the recorded absolute target
/// path, and no other class reads it here.
pub fn observe_current(
    state: &AppState,
    effect: &EffectFileRecord,
    evidence: Option<&EvidenceRow>,
) -> Result<Current, ObserveError> {
    match (effect.capability.as_str(), effect.operation.as_str()) {
        ("fs", "write") | ("fs", "delete") | ("fs", "mkdir") => {
            Ok(root_file_state(state, &effect.scope))
        }
        ("fs", "rename") => {
            let Some((from, to)) = parse_rename_scope(&effect.scope) else {
                return Err(ObserveError::MissingEvidence);
            };
            Ok(Current::Rename {
                source: Box::new(root_file_state(state, &from)),
                target: Box::new(root_file_state(state, &to)),
            })
        }
        ("fs", "export") => {
            let Some(evidence) = evidence else {
                return Err(ObserveError::MissingEvidence);
            };
            let Some(Value::String(target)) = baseline_field(evidence, "target_path") else {
                return Err(ObserveError::MissingEvidence);
            };
            Ok(file_state(Path::new(&target)))
        }
        ("tasks", "execute") => {
            let tasks = state.tasks.lock().expect("task lock");
            let Some(task) = tasks.get(&effect.scope) else {
                return Ok(Current::Missing);
            };
            let artifact = task
                .artifact_path
                .as_deref()
                .map(|p| Box::new(root_file_state(state, p)));
            Ok(Current::Task {
                status: task.status.clone(),
                artifact,
            })
        }
        ("reminders", "set_completed") => match state.device_backend.get(&effect.scope) {
            Ok(Some(r)) => Ok(Current::Reminder {
                completed: r.completed,
            }),
            Ok(None) => Ok(Current::ReminderMissing),
            Err(_) => Ok(Current::Unreadable),
        },
        ("connectors", _) | ("custom-connectors", _) | ("credentials", _) | ("catalog", _) => {
            if expects_active(&effect.capability, &effect.operation).is_none() {
                return Err(ObserveError::NoMethod);
            }
            Ok(connector_current(
                state,
                &effect.capability,
                &effect.operation,
                &effect.scope,
            ))
        }
        _ => Err(ObserveError::NoMethod),
    }
}

/// Reconcile verdict for one observation. `Landed` proves the
/// write landed; `NotLanded` is reserved for the
/// device-reminder class (target reminder no longer exists);
/// `Inconclusive` sends the row back to `unknown` with no
/// conclusion attached.
pub enum ReconcileOutcome {
    Landed { observed: Value, reason: &'static str },
    NotLanded { observed: Value, reason: &'static str },
    Inconclusive { observed: Value, reason: &'static str },
}

fn inconclusive(current: &Current) -> ReconcileOutcome {
    let reason = match current {
        Current::Unreadable => "io_error",
        _ => "state_unclear",
    };
    ReconcileOutcome::Inconclusive {
        observed: current.to_json(),
        reason,
    }
}

/// Map a current observation to a reconcile verdict. Device
/// classes never reach this function: the driver keeps the
/// action-based path for them.
pub fn reconcile_outcome(
    state: &AppState,
    effect: &EffectFileRecord,
    evidence: Option<&EvidenceRow>,
) -> Result<ReconcileOutcome, ObserveError> {
    let current = observe_current(state, effect, evidence)?;
    let outcome = match (effect.capability.as_str(), effect.operation.as_str()) {
        ("fs", "write") => {
            let Some(digest) = &effect.input_digest else {
                return Err(ObserveError::MissingEvidence);
            };
            match &current {
                Current::File { sha256, len } => {
                    // Overwrite recompute from the already-read
                    // content hash: no second read, no mode
                    // guess (the recompute is overwrite-only by
                    // construction; see the MissingEvidence rule
                    // for legacy rows in verify_outcome).
                    let params = std::collections::BTreeMap::from([
                        ("content_sha256".to_string(), sha256.clone()),
                        ("path".to_string(), effect.scope.clone()),
                    ]);
                    let recomputed =
                        crate::operation_binding::operation_digest("fs.write", Some("overwrite"), &params);
                    if recomputed == *digest {
                        ReconcileOutcome::Landed {
                            observed: json!({"kind": "file", "sha256": sha256, "len": len}),
                            reason: "hash_match",
                        }
                    } else {
                        inconclusive(&current)
                    }
                }
                _ => inconclusive(&current),
            }
        }
        ("fs", "delete") => match &current {
            Current::Absent => ReconcileOutcome::Landed {
                observed: current.to_json(),
                reason: "target_absent",
            },
            _ => inconclusive(&current),
        },
        ("fs", "mkdir") => match &current {
            Current::Dir => ReconcileOutcome::Landed {
                observed: current.to_json(),
                reason: "dir_present",
            },
            _ => inconclusive(&current),
        },
        ("fs", "rename") => {
            let Some(evidence) = evidence else {
                return Err(ObserveError::MissingEvidence);
            };
            let (Some(Value::String(sha)), Some(Value::Number(len))) = (
                baseline_field(evidence, "sha256"),
                baseline_field(evidence, "len"),
            ) else {
                return Err(ObserveError::MissingEvidence);
            };
            match &current {
                Current::Rename { source, target } => match (&**source, &**target) {
                    (
                        Current::Absent,
                        Current::File {
                            sha256, len: tlen,
                        },
                    ) if sha256 == &sha && len.as_u64() == Some(*tlen) => {
                        ReconcileOutcome::Landed {
                            observed: current.to_json(),
                            reason: "hash_match",
                        }
                    }
                    _ => inconclusive(&current),
                },
                _ => inconclusive(&current),
            }
        }
        ("fs", "export") => {
            let Some(evidence) = evidence else {
                return Err(ObserveError::MissingEvidence);
            };
            let (Some(Value::String(sha)), Some(Value::Number(len))) = (
                baseline_field(evidence, "sha256"),
                baseline_field(evidence, "len"),
            ) else {
                return Err(ObserveError::MissingEvidence);
            };
            match &current {
                Current::File {
                    sha256, len: tlen,
                } if sha256 == &sha && len.as_u64() == Some(*tlen) => {
                    ReconcileOutcome::Landed {
                        observed: current.to_json(),
                        reason: "hash_match",
                    }
                }
                _ => inconclusive(&current),
            }
        }
        ("tasks", "execute") => {
            let Some(digest) = &effect.input_digest else {
                return Err(ObserveError::MissingEvidence);
            };
            match &current {
                Current::Task {
                    status,
                    artifact: Some(artifact),
                } if status == "completed" => match &**artifact {
                    Current::File { sha256, .. } => {
                        let task = state.tasks.lock().expect("task lock");
                        let recomputed = task
                            .get(&effect.scope)
                            .and_then(|t| t.artifact_path.clone())
                            .map(|p| {
                                let params = std::collections::BTreeMap::from([
                                    ("content_sha256".to_string(), sha256.clone()),
                                    ("path".to_string(), p),
                                ]);
                                crate::operation_binding::operation_digest(
                                    "fs.write",
                                    Some("overwrite"),
                                    &params,
                                )
                            });
                        if recomputed.as_deref() == Some(digest.as_str()) {
                            ReconcileOutcome::Landed {
                                observed: current.to_json(),
                                reason: "hash_match",
                            }
                        } else {
                            inconclusive(&current)
                        }
                    }
                    _ => inconclusive(&current),
                },
                _ => inconclusive(&current),
            }
        }
        ("reminders", "set_completed") => match &current {
            Current::Reminder { completed: true } => ReconcileOutcome::Landed {
                observed: current.to_json(),
                reason: "record_match",
            },
            Current::ReminderMissing => ReconcileOutcome::NotLanded {
                observed: current.to_json(),
                reason: "target_missing",
            },
            _ => inconclusive(&current),
        },
        ("connectors", _) | ("custom-connectors", _) | ("credentials", _) | ("catalog", _) => {
            let Some(expected) = expects_active(&effect.capability, &effect.operation) else {
                return Err(ObserveError::NoMethod);
            };
            match &current {
                Current::Record { active, .. } if *active == expected => {
                    ReconcileOutcome::Landed {
                        observed: current.to_json(),
                        reason: "record_match",
                    }
                }
                _ => inconclusive(&current),
            }
        }
        _ => return Err(ObserveError::NoMethod),
    };
    Ok(outcome)
}

/// Verify verdict for one `succeeded` row.
pub enum VerifyOutcome {
    Verified { observed: Value, reason: &'static str },
    Mismatch { observed: Value, reason: &'static str },
    Inconclusive { observed: Value, reason: &'static str },
}

fn verify_inconclusive(current: &Current) -> VerifyOutcome {
    let reason = match current {
        Current::Unreadable => "io_error",
        Current::Symlink => "symlink_refused",
        _ => "state_unclear",
    };
    VerifyOutcome::Inconclusive {
        observed: current.to_json(),
        reason,
    }
}

fn file_vs_baseline(
    current: &Current,
    sha: &str,
    len: u64,
) -> VerifyOutcome {
    match current {
        Current::File {
            sha256, len: tlen,
        } if sha256 == sha && *tlen == len => VerifyOutcome::Verified {
            observed: current.to_json(),
            reason: "hash_match",
        },
        Current::File { .. } => VerifyOutcome::Mismatch {
            observed: current.to_json(),
            reason: "hash_mismatch",
        },
        Current::Absent => VerifyOutcome::Mismatch {
            observed: current.to_json(),
            reason: "target_missing",
        },
        Current::Dir => VerifyOutcome::Mismatch {
            observed: current.to_json(),
            reason: "type_mismatch",
        },
        _ => verify_inconclusive(current),
    }
}

fn record_vs_expected(
    current: &Current,
    expected_active: bool,
) -> VerifyOutcome {
    match current {
        Current::Record { present, active } if *present && *active == expected_active => {
            VerifyOutcome::Verified {
                observed: current.to_json(),
                reason: "record_match",
            }
        }
        Current::Record { present: false, .. } => VerifyOutcome::Mismatch {
            observed: current.to_json(),
            reason: "target_missing",
        },
        Current::Record { .. } => VerifyOutcome::Mismatch {
            observed: current.to_json(),
            reason: "record_mismatch",
        },
        _ => verify_inconclusive(current),
    }
}

/// Map a current observation to a verify verdict.
/// `write_evidence` is the row's write-time Evidence (source
/// `write`), when one exists. Legacy rows without one fall back
/// to recomputation where the class allows it; where it does
/// not (append / export / rename content), the outcome is
/// `MissingEvidence`, never a guessed mismatch: an overwrite
/// recompute cannot distinguish a drifted overwrite from an
/// append row, because the stored row records no mode.
pub fn verify_outcome(
    state: &AppState,
    effect: &EffectFileRecord,
    write_evidence: Option<&EvidenceRow>,
) -> Result<VerifyOutcome, ObserveError> {
    let current = observe_current(state, effect, write_evidence)?;
    let outcome = match (effect.capability.as_str(), effect.operation.as_str()) {
        ("fs", "write") => {
            if let Some(evidence) = write_evidence {
                let (Some(Value::String(sha)), Some(Value::Number(len))) = (
                    baseline_field(evidence, "sha256"),
                    baseline_field(evidence, "len"),
                ) else {
                    return Err(ObserveError::MissingEvidence);
                };
                file_vs_baseline(&current, &sha, len.as_u64().unwrap_or(0))
            } else {
                // Legacy row: recompute the overwrite digest.
                // A match proves the overwrite conclusion; a
                // non-match proves nothing (append vs drift),
                // so it is evidence_missing, not mismatch.
                let Some(digest) = &effect.input_digest else {
                    return Err(ObserveError::MissingEvidence);
                };
                match &current {
                    Current::File { sha256, .. } => {
                        let params = std::collections::BTreeMap::from([
                            ("content_sha256".to_string(), sha256.clone()),
                            ("path".to_string(), effect.scope.clone()),
                        ]);
                        let recomputed = crate::operation_binding::operation_digest(
                            "fs.write",
                            Some("overwrite"),
                            &params,
                        );
                        if recomputed == *digest {
                            VerifyOutcome::Verified {
                                observed: current.to_json(),
                                reason: "hash_match",
                            }
                        } else {
                            return Err(ObserveError::MissingEvidence);
                        }
                    }
                    _ => return Err(ObserveError::MissingEvidence),
                }
            }
        }
        ("fs", "delete") => match &current {
            Current::Absent => VerifyOutcome::Verified {
                observed: current.to_json(),
                reason: "target_absent",
            },
            Current::File { .. } | Current::Dir => VerifyOutcome::Mismatch {
                observed: current.to_json(),
                reason: "target_present",
            },
            _ => verify_inconclusive(&current),
        },
        ("fs", "mkdir") => match &current {
            Current::Dir => VerifyOutcome::Verified {
                observed: current.to_json(),
                reason: "dir_present",
            },
            Current::Absent => VerifyOutcome::Mismatch {
                observed: current.to_json(),
                reason: "target_missing",
            },
            Current::File { .. } => VerifyOutcome::Mismatch {
                observed: current.to_json(),
                reason: "not_a_directory",
            },
            _ => verify_inconclusive(&current),
        },
        ("fs", "rename") => {
            let Some(evidence) = write_evidence else {
                return Err(ObserveError::MissingEvidence);
            };
            let (Some(Value::String(sha)), Some(Value::Number(len))) = (
                baseline_field(evidence, "sha256"),
                baseline_field(evidence, "len"),
            ) else {
                return Err(ObserveError::MissingEvidence);
            };
            match &current {
                Current::Rename { source, target } => match (&**source, &**target) {
                    (
                        Current::Absent,
                        Current::File {
                            sha256, len: tlen,
                        },
                    ) if sha256 == &sha && len.as_u64() == Some(*tlen) => {
                        VerifyOutcome::Verified {
                            observed: current.to_json(),
                            reason: "hash_match",
                        }
                    }
                    (Current::Absent, Current::Absent) => VerifyOutcome::Mismatch {
                        observed: current.to_json(),
                        reason: "target_missing",
                    },
                    (Current::Absent, Current::File { .. }) => VerifyOutcome::Mismatch {
                        observed: current.to_json(),
                        reason: "hash_mismatch",
                    },
                    (Current::Absent, Current::Dir) => VerifyOutcome::Mismatch {
                        observed: current.to_json(),
                        reason: "type_mismatch",
                    },
                    (Current::Symlink, _) | (_, Current::Symlink) => {
                        verify_inconclusive(&current)
                    }
                    (Current::Unreadable, _) | (_, Current::Unreadable) => {
                        verify_inconclusive(&current)
                    }
                    _ => VerifyOutcome::Mismatch {
                        observed: current.to_json(),
                        reason: "target_present",
                    },
                },
                _ => verify_inconclusive(&current),
            }
        }
        ("fs", "export") => {
            let Some(evidence) = write_evidence else {
                return Err(ObserveError::MissingEvidence);
            };
            let (Some(Value::String(sha)), Some(Value::Number(len))) = (
                baseline_field(evidence, "sha256"),
                baseline_field(evidence, "len"),
            ) else {
                return Err(ObserveError::MissingEvidence);
            };
            file_vs_baseline(&current, &sha, len.as_u64().unwrap_or(0))
        }
        ("tasks", "execute") => match &current {
            Current::Missing => VerifyOutcome::Mismatch {
                observed: current.to_json(),
                reason: "target_missing",
            },
            Current::Task {
                status,
                artifact,
            } => {
                if status != "completed" {
                    return Ok(VerifyOutcome::Mismatch {
                        observed: current.to_json(),
                        reason: "record_mismatch",
                    });
                }
                let Some(artifact) = artifact else {
                    return Ok(VerifyOutcome::Mismatch {
                        observed: current.to_json(),
                        reason: "target_missing",
                    });
                };
                if let Some(evidence) = write_evidence {
                    let (Some(Value::String(sha)), Some(Value::Number(len))) = (
                        baseline_field(evidence, "sha256"),
                        baseline_field(evidence, "len"),
                    ) else {
                        return Err(ObserveError::MissingEvidence);
                    };
                    match &**artifact {
                        Current::File { .. } => file_vs_baseline(artifact, &sha, len.as_u64().unwrap_or(0)),
                        Current::Absent => VerifyOutcome::Mismatch {
                            observed: current.to_json(),
                            reason: "target_missing",
                        },
                        Current::Dir => VerifyOutcome::Mismatch {
                            observed: current.to_json(),
                            reason: "type_mismatch",
                        },
                        _ => verify_inconclusive(artifact),
                    }
                } else {
                    let Some(digest) = &effect.input_digest else {
                        return Err(ObserveError::MissingEvidence);
                    };
                    match &**artifact {
                        Current::File { sha256, .. } => {
                            let task = state.tasks.lock().expect("task lock");
                            let recomputed = task
                                .get(&effect.scope)
                                .and_then(|t| t.artifact_path.clone())
                                .map(|p| {
                                    let params = std::collections::BTreeMap::from([
                                        ("content_sha256".to_string(), sha256.clone()),
                                        ("path".to_string(), p),
                                    ]);
                                    crate::operation_binding::operation_digest(
                                        "fs.write",
                                        Some("overwrite"),
                                        &params,
                                    )
                                });
                            if recomputed.as_deref() == Some(digest.as_str()) {
                                VerifyOutcome::Verified {
                                    observed: current.to_json(),
                                    reason: "hash_match",
                                }
                            } else {
                                VerifyOutcome::Mismatch {
                                    observed: current.to_json(),
                                    reason: "hash_mismatch",
                                }
                            }
                        }
                        Current::Absent => VerifyOutcome::Mismatch {
                            observed: current.to_json(),
                            reason: "target_missing",
                        },
                        Current::Dir => VerifyOutcome::Mismatch {
                            observed: current.to_json(),
                            reason: "type_mismatch",
                        },
                        _ => verify_inconclusive(artifact),
                    }
                }
            }
            _ => verify_inconclusive(&current),
        },
        ("reminders", "set_completed") => match &current {
            Current::Reminder { completed: true } => VerifyOutcome::Verified {
                observed: current.to_json(),
                reason: "record_match",
            },
            Current::Reminder { completed: false } => VerifyOutcome::Mismatch {
                observed: current.to_json(),
                reason: "record_mismatch",
            },
            Current::ReminderMissing => VerifyOutcome::Mismatch {
                observed: current.to_json(),
                reason: "target_missing",
            },
            _ => verify_inconclusive(&current),
        },
        ("connectors", _) | ("custom-connectors", _) | ("credentials", _) | ("catalog", _) => {
            let Some(expected) = expects_active(&effect.capability, &effect.operation) else {
                return Err(ObserveError::NoMethod);
            };
            record_vs_expected(&current, expected)
        }
        _ => return Err(ObserveError::NoMethod),
    };
    Ok(outcome)
}

/// Verify a succeeded device row: the backing action carries
/// the target flag the Effect row does not. No action means
/// the expectation is unrecoverable: `MissingEvidence`.
pub fn device_verify_outcome(
    state: &AppState,
    target_completed: bool,
    reminder_id: &str,
) -> VerifyOutcome {
    match state.device_backend.get(reminder_id) {
        Ok(Some(r)) if r.completed == target_completed => VerifyOutcome::Verified {
            observed: json!({"kind": "reminder", "completed": r.completed}),
            reason: "record_match",
        },
        Ok(Some(r)) => VerifyOutcome::Mismatch {
            observed: json!({"kind": "reminder", "completed": r.completed}),
            reason: "record_mismatch",
        },
        Ok(None) => VerifyOutcome::Mismatch {
            observed: json!({"kind": "absent"}),
            reason: "target_missing",
        },
        Err(_) => VerifyOutcome::Inconclusive {
            observed: json!({"kind": "unreadable"}),
            reason: "io_error",
        },
    }
}

/// Write-time baseline for a just-succeeded Effect row, when
/// the class allows one. Device rows are excluded (their
/// mirror comes from reconcile, not from the write); export
/// rows are excluded (the target path is not on the Effect row
/// — see `export_write_time`). A `None` return is an honest
/// gap, never a fabricated row: the legacy fallback covers the
/// row later.
pub fn write_time_observation(
    state: &AppState,
    effect: &EffectFileRecord,
) -> Option<OwnedEvidence> {
    let observed = match (effect.capability.as_str(), effect.operation.as_str()) {
        ("fs", "write") => match root_file_state(state, &effect.scope) {
            Current::File { sha256, len } => json!({"kind": "file", "sha256": sha256, "len": len}),
            _ => return None,
        },
        ("fs", "delete") => match root_file_state(state, &effect.scope) {
            Current::Absent => json!({"kind": "absent"}),
            _ => return None,
        },
        ("fs", "mkdir") => match root_file_state(state, &effect.scope) {
            Current::Dir => json!({"kind": "dir"}),
            _ => return None,
        },
        ("fs", "rename") => {
            let (from, to) = parse_rename_scope(&effect.scope)?;
            if !matches!(root_file_state(state, &from), Current::Absent) {
                return None;
            }
            match root_file_state(state, &to) {
                Current::File { sha256, len } => {
                    json!({"kind": "file", "sha256": sha256, "len": len})
                }
                _ => return None,
            }
        }
        ("tasks", "execute") => {
            let tasks = state.tasks.lock().expect("task lock");
            let task = tasks.get(&effect.scope)?;
            if task.status != "completed" {
                return None;
            }
            let path = task.artifact_path.as_deref()?;
            match root_file_state(state, path) {
                Current::File { sha256, len } => {
                    json!({"kind": "artifact", "sha256": sha256, "len": len})
                }
                _ => return None,
            }
        }
        ("reminders", "set_completed") => match state.device_backend.get(&effect.scope) {
            Ok(Some(r)) if r.completed => json!({"kind": "reminder", "completed": true}),
            _ => return None,
        },
        ("connectors", _) | ("custom-connectors", _) | ("credentials", _) | ("catalog", _) => {
            let expected = expects_active(&effect.capability, &effect.operation)?;
            match connector_current(state, &effect.capability, &effect.operation, &effect.scope) {
                Current::Record { present, active } if present && active == expected => {
                    json!({"kind": "record", "present": present, "active": active})
                }
                _ => return None,
            }
        }
        _ => return None,
    };
    Some(owned(
        effect,
        &state.origin_device_id,
        "write",
        "verified",
        "written",
        observed,
    ))
}

/// Write-time baseline for an export success. The target path
/// is an absolute path outside the file root, recorded here
/// because the Effect row's scope names only the source.
pub fn export_write_time(
    state: &AppState,
    effect_id: &str,
    execution_id: Option<&str>,
    target_abs: &Path,
) -> Option<OwnedEvidence> {
    let Current::File { sha256, len } = file_state(target_abs) else {
        return None;
    };
    Some(OwnedEvidence {
        effect_id: effect_id.to_string(),
        execution_id: execution_id.map(str::to_string),
        source: "write".to_string(),
        verdict: "verified".to_string(),
        reason: "written".to_string(),
        observed_json: serde_json::to_string(&json!({
            "kind": "file",
            "sha256": sha256,
            "len": len,
            "target_path": target_abs.to_string_lossy(),
        }))
        .unwrap_or_else(|_| "{}".to_string()),
        checked_ms: now_ms() as i64,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    })
}
