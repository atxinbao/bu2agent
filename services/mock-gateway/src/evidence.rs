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
    /// P3-C: a worktree-add observation — whether the new
    /// worktree directory exists (as its HEAD hash), and
    /// whether the branch ref exists in the parent repo.
    GitWorktree {
        head_sha: Option<String>,
        branch_exists: bool,
    },
    /// P3-C: a commit observation — the recorded commit sha
    /// (found by exact message, when the Effect carries no
    /// baseline), and the worktree's current HEAD.
    GitCommit {
        found_sha: Option<String>,
        head_sha: Option<String>,
    },
    /// P3-C: a test-run observation — the stored Test Evidence
    /// summary (source `test`), when one exists.
    TestRun { summary: Option<Value> },
    /// D1-6b-2: a calendar create observation — the
    /// re-read event's fields against the fact row.
    /// `fields_match` is computed in `observe_current`
    /// from the fact row (the expected values) and the
    /// live re-read; the observed JSON carries ids /
    /// lengths / flags only, never title / location /
    /// notes plaintext.
    CalendarEvent {
        event_id: String,
        title_len: usize,
        start_ms: i64,
        end_ms: i64,
        all_day: bool,
        calendar_id: Option<String>,
        fields_match: bool,
    },
    /// D1-6b-2: the re-read found no event with the
    /// fact row's identifier.
    CalendarMissing,
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
            Current::GitWorktree {
                head_sha,
                branch_exists,
            } => json!({
                "kind": "worktree",
                "head_sha": head_sha,
                "branch_exists": branch_exists,
            }),
            Current::GitCommit {
                found_sha,
                head_sha,
            } => json!({
                "kind": "commit",
                "sha256": found_sha,
                "head_sha": head_sha,
            }),
            Current::TestRun { summary } => json!({
                "kind": "test-run",
                "summary": summary,
            }),
            Current::CalendarEvent {
                event_id,
                title_len,
                start_ms,
                end_ms,
                all_day,
                calendar_id,
                fields_match,
            } => json!({
                "kind": "calendar-event",
                "event_id": event_id,
                "title_len": title_len,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "all_day": all_day,
                "calendar_id": calendar_id,
                "fields_match": fields_match,
            }),
            Current::CalendarMissing => json!({"kind": "absent"}),
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

/// P3-C: the Worker run that produced one Effect, when the
/// Effect came from a run (git/test classes). Effect rows
/// carry no cwd/argv, so the run is the only source of the
/// repo path + command that made the Effect. None when the
/// run is gone (JSONL mode, or the row predates P3-C).
fn run_for_effect(state: &AppState, effect_id: &str) -> Option<crate::worker::WorkerRunRow> {
    let sqlite = state.sqlite.as_ref()?;
    let conn = sqlite.lock().expect("sqlite lock");
    crate::sqlite_store::get_worker_run_by_effect_id(&conn, effect_id)
        .ok()
        .flatten()
}

/// P3-C: the argv of a run, parsed from its stored JSON.
/// Empty on a corrupt row (the caller treats it as
/// unobservable, never invents a command).
fn run_argv(run: &crate::worker::WorkerRunRow) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(&run.argv_json).unwrap_or_default()
}

/// P3-C: the message of a git-commit run (the `-m` argument),
/// when the run was a commit. The commit message is the only
/// identity an unknown commit Effect carries: its digest
/// has no parent/tree, so reconcile matches by exact
/// message + reachability from HEAD.
fn commit_message(run: &crate::worker::WorkerRunRow) -> Option<String> {
    if run.command != "git-commit" {
        return None;
    }
    let argv = run_argv(run);
    argv.windows(2)
        .find(|w| w[0] == "-m")
        .map(|w| w[1].clone())
}

/// P3-C: find a commit with this exact full message among
/// the newest 200 commits reachable from HEAD. Returns the
/// sha, or None when no such commit exists (or git fails).
/// The message is matched as a whole (trimmed on both
/// sides); duplicate messages across runs can confuse the
/// match — tests and window validation use unique messages.
fn commit_by_message(
    state: &AppState,
    git: &std::path::Path,
    cwd: &std::path::Path,
    message: &str,
) -> Option<String> {
    let out = crate::worker::run_command_sync(
        state,
        &[
            git.to_string_lossy().into_owned(),
            "-C".to_string(),
            cwd.to_string_lossy().into_owned(),
            "log".to_string(),
            "--format=%H%x00%B%x1e".to_string(),
            "-n".to_string(),
            "200".to_string(),
            "HEAD".to_string(),
        ],
        cwd,
        git,
        std::time::Duration::from_secs(10),
    );
    if out.timed_out || out.exit_code != Some(0) {
        return None;
    }
    let wanted = message.trim();
    for record in out.stdout.split('\u{1e}') {
        let Some((sha, body)) = record.trim_start().split_once('\0') else {
            continue;
        };
        if body.trim() == wanted && !sha.trim().is_empty() {
            return Some(sha.trim().to_string());
        }
    }
    None
}

/// P3-C: does this ref exist in the repo at `cwd`?
/// (`git show-ref --verify` exits 0 only when it does.)
fn branch_exists(state: &AppState, git: &std::path::Path, cwd: &std::path::Path, branch: &str) -> bool {
    let out = crate::worker::run_command_sync(
        state,
        &[
            git.to_string_lossy().into_owned(),
            "-C".to_string(),
            cwd.to_string_lossy().into_owned(),
            "show-ref".to_string(),
            "--verify".to_string(),
            format!("refs/heads/{branch}"),
        ],
        cwd,
        git,
        std::time::Duration::from_secs(10),
    );
    !out.timed_out && out.exit_code == Some(0)
}

/// P3-C: is this sha a commit object in the repo at `cwd`?
fn commit_exists(state: &AppState, git: &std::path::Path, cwd: &std::path::Path, sha: &str) -> bool {
    let out = crate::worker::run_command_sync(
        state,
        &[
            git.to_string_lossy().into_owned(),
            "-C".to_string(),
            cwd.to_string_lossy().into_owned(),
            "cat-file".to_string(),
            "-t".to_string(),
            sha.to_string(),
        ],
        cwd,
        git,
        std::time::Duration::from_secs(10),
    );
    !out.timed_out && out.exit_code == Some(0) && out.stdout.trim() == "commit"
}

/// P3-C: is `sha` an ancestor of (or equal to) HEAD in the
/// repo at `cwd`? (`git merge-base --is-ancestor`.)
fn commit_is_ancestor(
    state: &AppState,
    git: &std::path::Path,
    cwd: &std::path::Path,
    sha: &str,
) -> bool {
    let out = crate::worker::run_command_sync(
        state,
        &[
            git.to_string_lossy().into_owned(),
            "-C".to_string(),
            cwd.to_string_lossy().into_owned(),
            "merge-base".to_string(),
            "--is-ancestor".to_string(),
            sha.to_string(),
            "HEAD".to_string(),
        ],
        cwd,
        git,
        std::time::Duration::from_secs(10),
    );
    !out.timed_out && out.exit_code == Some(0)
}

/// P3-C: observe the git classes. The run row gives the
/// repo path + command; a missing run or a missing git
/// binary reads as Unreadable (reconcile maps it to
/// Inconclusive, never to a false mismatch).
fn observe_git(
    state: &AppState,
    effect: &EffectFileRecord,
) -> Result<Current, ObserveError> {
    let Some(run) = run_for_effect(state, &effect.effect_id) else {
        return Ok(Current::Unreadable);
    };
    let Some(git) = state.git_path.clone() else {
        return Ok(Current::Unreadable);
    };
    let cwd = std::path::Path::new(&run.cwd);
    match effect.operation.as_str() {
        "worktree-add" => {
            // The Effect scope is the worktree's
            // Location-relative path; the run row carries the
            // absolute path (argv[3]: `git worktree add
            // <path> -b <branch>`). The parent repo is the
            // run's cwd (the Location root); the branch is
            // argv[5].
            let new_path = run_argv(&run).get(3).cloned();
            let head_sha = new_path.as_ref().and_then(|p| {
                crate::worker::git_read_sync(state, &git, &["-C", p, "rev-parse", "HEAD"], std::path::Path::new(p))
            });
            let branch = run_argv(&run)
                .windows(2)
                .find(|w| w[0] == "-b")
                .map(|w| w[1].clone());
            let branch_exists = branch
                .as_deref()
                .is_some_and(|b| branch_exists(state, &git, cwd, b));
            Ok(Current::GitWorktree {
                head_sha,
                branch_exists,
            })
        }
        "commit" => {
            // The recorded commit is found by exact message
            // (the unknown Effect's only identity); HEAD is
            // read fresh. A baseline sha (succeeded rows) is
            // compared by the caller, not here.
            let head_sha = crate::worker::git_read_sync(
                state,
                &git,
                &["-C", &run.cwd, "rev-parse", "HEAD"],
                cwd,
            );
            let found_sha = commit_message(&run)
                .and_then(|m| commit_by_message(state, &git, cwd, &m));
            Ok(Current::GitCommit {
                found_sha,
                head_sha,
            })
        }
        // Reads land nothing: the repo is readable (HEAD
        // resolves) or it is not. Reconcile maps both to
        // Inconclusive; the branch exists so reconcile does
        // not 409 `effect_no_readback`. The operation is the
        // command template name (`git-status`), as recorded
        // by the driver's unknown_effect_identity — not the
        // loop tool's `status` (no Effect ever carries that).
        "git-status" | "git-diff" => {
            let head_sha = crate::worker::git_read_sync(
                state,
                &git,
                &["-C", &run.cwd, "rev-parse", "HEAD"],
                cwd,
            );
            if head_sha.is_some() {
                Ok(Current::GitCommit {
                    found_sha: None,
                    head_sha,
                })
            } else {
                Ok(Current::Unreadable)
            }
        }
        _ => Err(ObserveError::NoMethod),
    }
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
        // E2-C unification: the device triple (new loop rows
        // and direct rows) and the legacy loop triple share
        // this observation — it only reads current state.
        ("reminders", "set_completed") | ("device", "reminders.set_completed") => {
            match state.device_backend.get(&effect.scope) {
                Ok(Some(r)) => Ok(Current::Reminder {
                    completed: r.completed,
                }),
                Ok(None) => Ok(Current::ReminderMissing),
                Err(_) => Ok(Current::Unreadable),
            }
        }
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
        // P3-C: git observe (closes the slice's known gap —
        // its timeout/cancel/orphan Effects use capability
        // `git` but had no readback until this branch landed).
        // The run row is the repo path + command source.
        ("git", _) => observe_git(state, effect),
        // P3-C: a test run's only observation is its stored
        // Test Evidence (source `test`); the caller loads it
        // by source (see latest_evidence_for_effect). No
        // Test Evidence row means no observation yet.
        ("test", "run") => {
            let summary = evidence.and_then(|e| {
                serde_json::from_str::<Value>(&e.observed_json).ok()
            });
            Ok(Current::TestRun { summary })
        }
        // D1-6b-2: a calendar create re-reads by the
        // EventKit identifier the fact row names. No fact
        // row (no successful create was ever recorded for
        // this Effect) means no observation is possible —
        // MissingEvidence, never a guess. A backend error
        // means the world could not be read — Unreadable,
        // which verifies Inconclusive.
        ("calendar", "create") => {
            let Some(sqlite) = state.sqlite.as_ref() else {
                return Err(ObserveError::MissingEvidence);
            };
            let conn = sqlite.lock().expect("sqlite lock");
            let Some(row) = crate::calendar_store::find_by_effect_id(&conn, &effect.effect_id)
                .map_err(|_| ObserveError::MissingEvidence)?
            else {
                return Err(ObserveError::MissingEvidence);
            };
            drop(conn);
            match state.calendar_backend.get_event(&row.event_id) {
                Ok(Some(current)) => {
                    let fields_match = current.calendar_id.as_deref()
                        == Some(row.calendar_id.as_str())
                        && current.title == row.title
                        && current.start_ms == row.start_ms
                        && current.end_ms == row.end_ms
                        && current.all_day == row.all_day;
                    Ok(Current::CalendarEvent {
                        event_id: row.event_id,
                        title_len: row.title.chars().count(),
                        start_ms: row.start_ms,
                        end_ms: row.end_ms,
                        all_day: row.all_day,
                        calendar_id: Some(row.calendar_id),
                        fields_match,
                    })
                }
                Ok(None) => Ok(Current::CalendarMissing),
                Err(_) => Ok(Current::Unreadable),
            }
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
/// rows with a backing action keep the action-based path in
/// the driver; device rows without one (loop writes, which
/// have no action record) reach this function, where the
/// reminders arm recovers the target from the row's own
/// digest (E2-C unification, A2).
pub fn reconcile_outcome(
    state: &AppState,
    effect: &EffectFileRecord,
    evidence: Option<&EvidenceRow>,
) -> Result<ReconcileOutcome, ObserveError> {
    // P3-C: a test run's landing cannot be observed post-hoc
    // (re-running the suite is a new execution, not a
    // readback). Its stored Test Evidence is trusted by
    // verify only; reconcile always returns Inconclusive.
    if effect.capability == "test" && effect.operation == "run" {
        let summary = evidence
            .and_then(|e| serde_json::from_str::<Value>(&e.observed_json).ok());
        return Ok(ReconcileOutcome::Inconclusive {
            observed: json!({"kind": "test-run", "summary": summary}),
            reason: "state_unclear",
        });
    }
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
        // E2-C unification: the device triple and the legacy
        // loop triple share one target-aware arm — the target
        // comes from the row's own digest (A2), never from a
        // default. A current state that does not match the
        // target stays Inconclusive (the write may still be in
        // flight), never a failed conclusion.
        ("device", "reminders.set_completed") | ("reminders", "set_completed") => match &current {
            Current::Reminder { completed } => match device_target_from_digest(effect) {
                Some(target) if *completed == target => ReconcileOutcome::Landed {
                    observed: current.to_json(),
                    reason: "record_match",
                },
                _ => inconclusive(&current),
            },
            Current::ReminderMissing => ReconcileOutcome::NotLanded {
                observed: current.to_json(),
                reason: "target_missing",
            },
            _ => inconclusive(&current),
        },
        // D1-6b-2: a calendar reconcile lands when the
        // re-read matches the fact row; anything else is
        // Inconclusive, never a false mismatch (the git
        // precedent — an unknown Effect's save may still
        // be in flight). Reachable only when a fact row
        // exists for a non-terminal Effect (record
        // succeeded, terminal transition failed).
        ("calendar", "create") => match &current {
            Current::CalendarEvent {
                fields_match: true, ..
            } => ReconcileOutcome::Landed {
                observed: current.to_json(),
                reason: "fields_match",
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
        // P3-C: git reconcile. A worktree-add lands when the
        // new worktree exists (HEAD readable) and the branch
        // ref exists in the parent repo. A commit lands when
        // a commit with the run's exact message is reachable
        // from HEAD (the unknown Effect's only identity).
        // Reads land nothing — always Inconclusive. Anything
        // else is Inconclusive, never a false mismatch.
        ("git", "worktree-add") => match &current {
            Current::GitWorktree {
                head_sha: Some(_),
                branch_exists: true,
            } => ReconcileOutcome::Landed {
                observed: current.to_json(),
                reason: "hash_match",
            },
            _ => inconclusive(&current),
        },
        ("git", "commit") => match &current {
            Current::GitCommit {
                found_sha: Some(_),
                ..
            } => ReconcileOutcome::Landed {
                observed: current.to_json(),
                reason: "hash_match",
            },
            _ => inconclusive(&current),
        },
        ("git", _) => inconclusive(&current),
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
        // E2-C unification: the device triple and the legacy
        // loop triple share one target-aware arm — the target
        // comes from the row's own digest (A2), never from a
        // default. An unrecoverable target (no digest on the
        // row) is Inconclusive, never a guess.
        ("device", "reminders.set_completed") | ("reminders", "set_completed") => match &current {
            Current::Reminder { completed } => match device_target_from_digest(effect) {
                Some(target) if *completed == target => VerifyOutcome::Verified {
                    observed: current.to_json(),
                    reason: "record_match",
                },
                Some(_) => VerifyOutcome::Mismatch {
                    observed: current.to_json(),
                    reason: "record_mismatch",
                },
                None => VerifyOutcome::Inconclusive {
                    observed: current.to_json(),
                    reason: "state_unclear",
                },
            },
            Current::ReminderMissing => VerifyOutcome::Mismatch {
                observed: current.to_json(),
                reason: "target_missing",
            },
            _ => verify_inconclusive(&current),
        },
        // D1-6b-2: a calendar create verifies against the
        // live re-read. Consistent fields → verified; a
        // field mismatch → mismatch (P0-R); a missing
        // event → inconclusive (the event may have been
        // deleted after creation — the create itself is
        // not disproven, so no Evidence row is written).
        ("calendar", "create") => match &current {
            Current::CalendarEvent {
                fields_match: true, ..
            } => VerifyOutcome::Verified {
                observed: current.to_json(),
                reason: "fields_match",
            },
            Current::CalendarEvent {
                fields_match: false,
                ..
            } => VerifyOutcome::Mismatch {
                observed: current.to_json(),
                reason: "field_mismatch",
            },
            _ => verify_inconclusive(&current),
        },
        ("connectors", _) | ("custom-connectors", _) | ("credentials", _) | ("catalog", _) => {
            let Some(expected) = expects_active(&effect.capability, &effect.operation) else {
                return Err(ObserveError::NoMethod);
            };
            record_vs_expected(&current, expected)
        }
        // P3-C: git verify. A worktree-add verifies when the
        // current HEAD equals the baseline head (the dir
        // missing is target_missing, a different HEAD is
        // hash_mismatch). A commit verifies when the baseline
        // sha exists AND is HEAD or an ancestor of HEAD (a
        // later commit on the same branch does not invalidate
        // it). No baseline is MissingEvidence, never a guess.
        ("git", "worktree-add") => {
            let Some(evidence) = write_evidence else {
                return Err(ObserveError::MissingEvidence);
            };
            let Some(Value::String(sha)) = baseline_field(evidence, "head_sha") else {
                return Err(ObserveError::MissingEvidence);
            };
            match &current {
                Current::GitWorktree {
                    head_sha: Some(current_sha),
                    ..
                } if current_sha == &sha => VerifyOutcome::Verified {
                    observed: current.to_json(),
                    reason: "hash_match",
                },
                Current::GitWorktree {
                    head_sha: None, ..
                } => VerifyOutcome::Mismatch {
                    observed: current.to_json(),
                    reason: "target_missing",
                },
                Current::GitWorktree { .. } => VerifyOutcome::Mismatch {
                    observed: current.to_json(),
                    reason: "hash_mismatch",
                },
                _ => verify_inconclusive(&current),
            }
        }
        ("git", "commit") => {
            let Some(evidence) = write_evidence else {
                return Err(ObserveError::MissingEvidence);
            };
            let Some(Value::String(sha)) = baseline_field(evidence, "sha256") else {
                return Err(ObserveError::MissingEvidence);
            };
            let run = run_for_effect(state, &effect.effect_id);
            let git = state.git_path.clone();
            match (run, git) {
                (Some(run), Some(git)) => {
                    let cwd = std::path::Path::new(&run.cwd);
                    if !commit_exists(state, &git, cwd, &sha) {
                        VerifyOutcome::Mismatch {
                            observed: current.to_json(),
                            reason: "target_missing",
                        }
                    } else if commit_is_ancestor(state, &git, cwd, &sha) {
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
                _ => verify_inconclusive(&current),
            }
        }
        // Reads have no landable state: verify is always
        // Inconclusive (the observe branch exists so verify
        // does not 409 `effect_no_readback`).
        ("git", "status") | ("git", "diff") => verify_inconclusive(&current),
        // P3-C: a test run verifies by trusting its stored
        // Test Evidence only — never by re-running the suite
        // (a re-run is a new execution). A summary present
        // means the run happened as recorded; absent means
        // MissingEvidence. (Only `succeeded` rows reach
        // verify at all, so a present summary is a pass.)
        ("test", "run") => match &current {
            Current::TestRun {
                summary: Some(summary),
            } => VerifyOutcome::Verified {
                observed: summary.clone(),
                reason: "test_passed",
            },
            Current::TestRun { summary: None } => {
                return Err(ObserveError::MissingEvidence);
            }
            _ => verify_inconclusive(&current),
        },
        _ => return Err(ObserveError::NoMethod),
    };
    Ok(outcome)
}

/// E2-C unification (A2): recover a device write's target
/// flag from the row's own digest, for rows with no backing
/// action (loop writes have none). The digest is the unified
/// Q5 digest — `operation_digest("reminders.set_completed",
/// Some("set_completed"), {id, completed})` — so trialling
/// both candidate values against the stored digest recovers
/// the target with no new column and no migration. This
/// relies on `operation_digest` being a stable pure function
/// with no version switching: if the digest algorithm ever
/// changes versions, this helper needs version dispatch, and
/// until then a miss returns `None` (the caller falls back
/// to Inconclusive, honest, never a misjudgement).
fn device_target_from_digest(effect: &EffectFileRecord) -> Option<bool> {
    let digest = effect.input_digest.as_ref()?;
    for candidate in ["true", "false"] {
        let params = std::collections::BTreeMap::from([
            ("id".to_string(), effect.scope.clone()),
            ("completed".to_string(), candidate.to_string()),
        ]);
        let trial = crate::operation_binding::operation_digest(
            "reminders.set_completed",
            Some("set_completed"),
            &params,
        );
        if &trial == digest {
            // Both candidates matching is negligible under
            // SHA-256; "true" is tried first, so it wins.
            return Some(candidate == "true");
        }
    }
    None
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
