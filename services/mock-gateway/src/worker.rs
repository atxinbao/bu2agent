//! E2 prerequisite slice: the single Worker driver (P3-C plan
//! §2–§3). The gateway spawns at most one child process per
//! run, argv-array only, allowlisted templates only, no shell,
//! no pipes, no redirects. One command, one child, one
//! process group (`setsid`): a kill clears the whole tree,
//! not just the child.
//!
//! Read-only by slice design: the only templates are `git
//! status` and `git diff`. They produce no Effect and no
//! Evidence on success — only an `unknown` Effect (with its
//! Inbox row, via the normal `record_effect` path) when the
//! outcome is unknown (timeout / cancelled / orphaned /
//! lost), per the P0-R honesty rule. Write templates
//! (worktree add / commit / cargo test), `git`/`test` Effect
//! vocabulary, Test Evidence and loop tools are P3-C, not
//! this slice.

use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;

use crate::server::AppState;
use crate::sqlite_store;

/// Tail kept on a `worker_runs` row: stdout+stderr, last N
/// bytes, never the whole output (output bomb guard, plan
/// §3.5/§9).
pub const WORKER_TAIL_LIMIT: usize = 4096;

/// Default per-run timeout for the read templates (plan
/// §3.3): 30s.
pub const WORKER_DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Caller-supplied `timeout_ms` ceiling (plan §2.4): 3600s.
pub const WORKER_MAX_TIMEOUT_MS: u64 = 3_600_000;

/// Grace between SIGTERM to the process group and SIGKILL
/// on the timeout path (plan §2.2): the group must be
/// confirmed dead, not merely unwatched.
pub const WORKER_KILL_GRACE_MS: u64 = 2_000;

/// How long the driver waits for inherited-pipe readers to
/// drain after the child exits before giving up on the tail
/// (a grandchild holding stdout open must not stall the
/// driver forever).
const READER_DRAIN_TIMEOUT_MS: u64 = 5_000;

/// Row of the `worker_runs` table (SQLite v4, plan §3.5).
/// `truncated` and the three `binding_*` columns are slice
/// additions the plan's field list implies elsewhere: the
/// truncated flag is §3.3/§4.1's output contract, and the
/// frozen binding is §2.3's run-row snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkerRunRow {
    pub run_id: String,
    pub execution_id: Option<String>,
    pub assignment_id: Option<String>,
    pub worker_session_id: Option<String>,
    pub command: String,
    pub argv_json: String,
    pub cwd: String,
    pub timeout_ms: i64,
    pub status: String,
    pub exit_code: Option<i64>,
    pub pid: Option<i64>,
    pub started_ms: Option<i64>,
    pub finished_ms: Option<i64>,
    pub tail: String,
    pub truncated: bool,
    pub effect_id: Option<String>,
    pub binding_location_id: Option<String>,
    pub binding_worktree_id: Option<String>,
    pub binding_root_digest: Option<String>,
    pub schema_version: i64,
}

/// Terminal run statuses: no further driving (plan §3.2).
pub fn is_terminal_run_status(status: &str) -> bool {
    matches!(
        status,
        "succeeded" | "failed" | "timeout" | "cancelled" | "lost" | "orphaned" | "superseded"
    )
}

fn now_ms() -> i64 {
    crate::effect_store::now_ms() as i64
}

// --- git resolution (plan §3.4) ---

/// Resolve the git binary once, at gateway startup:
/// `MOCK_GIT_PATH` wins when set (a set-but-missing value
/// fails closed to "no git", never silently falls back to
/// PATH); otherwise the first `git` on PATH. `None` means
/// Worker git commands fail closed with 503
/// `git_unavailable`.
pub fn resolve_git_path() -> Option<PathBuf> {
    if let Ok(value) = std::env::var("MOCK_GIT_PATH") {
        if !value.is_empty() {
            let candidate = PathBuf::from(&value);
            if candidate.is_file() {
                return Some(candidate);
            }
            return None;
        }
    }
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        for name in ["git", "git.exe"] {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

// --- process-group primitives ---
//
// No `libc` dependency is added for these: the Unix side
// drives the `kill` binary (argv, no shell) and the
// Windows side drives `taskkill`. Both are best-effort
// booleans; a failed kill never fabricates a status.

/// Unix `kill(pid, 0)` equivalent: is the pid alive?
/// Windows: `tasklist` filtered by pid. A probe failure
/// reads as "not alive" (fail-closed for the scan: a
/// process we cannot prove alive is `lost`, never silently
/// kept). A zombie counts as dead: `kill(pid, 0)` succeeds
/// for an unreaped zombie, and a scan that reads a zombie
/// as alive would "orphan" a process that is already gone
/// (Linux reads `/proc/<pid>/stat` for the Z state).
pub fn process_alive(pid: i64) -> bool {
    #[cfg(target_os = "linux")]
    {
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            // comm is parenthesized and may contain spaces:
            // the state is the field after the last ')'.
            if let Some(after) = stat.rsplit_once(") ") {
                if let Some(state) = after.1.split_whitespace().next() {
                    return state != "Z" && state != "X";
                }
            }
        }
        // No /proc entry (or unreadable): fall through to
        // kill -0 below.
    }
    #[cfg(unix)]
    {
        std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(windows)]
    {
        let output = std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output();
        match output {
            Ok(out) => String::from_utf8_lossy(&out.stdout).contains(&pid.to_string()),
            Err(_) => false,
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = pid;
        false
    }
}

/// Kill a whole process group: Unix `kill -<signal>
/// -<pgid>` (the child is its own group leader via
/// `setsid`, so pgid == child pid); Windows
/// `taskkill /PID <pid> /T /F` (signal ignored).
pub fn kill_process_group(pid: i64, signal: &str) -> bool {
    #[cfg(unix)]
    {
        // NOTE: `/usr/bin/kill` parses a bare `-<pgid>` as
        // an option (and silently kills nothing), so the
        // group id must follow `--`. Verified against a
        // real process tree: without `--`, exit 0 and both
        // processes survive; with `--`, the group dies.
        std::process::Command::new("kill")
            .args(["-s", signal, "--", &format!("-{pid}")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(windows)]
    {
        let _ = signal;
        std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (pid, signal);
        false
    }
}

// --- allowlist (plan §3.3, slice subset) ---

/// A resolved allowlisted command: argv to spawn (argv[0]
/// is the resolved git binary), its template name, and its
/// effective timeout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCommand {
    pub template: &'static str,
    pub argv: Vec<String>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    NotAllowlisted,
    BadParam(&'static str),
}

impl CommandError {
    pub fn code(&self) -> &'static str {
        match self {
            CommandError::NotAllowlisted => "command_not_allowlisted",
            CommandError::BadParam(code) => code,
        }
    }
}

/// Validate a `git diff` path argument: worktree-relative,
/// no shell metacharacters (argv is an array so they would
/// be inert anyway; the charset check is the second layer),
/// no absolute path, no `..` escape. The containment check
/// against the worktree root happens in `contained_path`.
fn validate_diff_path(path: &str) -> Result<(), CommandError> {
    if path.is_empty() {
        return Err(CommandError::BadParam("invalid_path"));
    }
    if path.contains('\0') || path.chars().any(|c| ";|&$<>`".contains(c)) {
        return Err(CommandError::BadParam("invalid_path"));
    }
    let as_path = Path::new(path);
    if as_path.is_absolute() {
        return Err(CommandError::BadParam("invalid_path"));
    }
    for component in as_path.components() {
        match component {
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(CommandError::BadParam("invalid_path"));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Resolve a worktree-relative path inside `cwd`: canonicalize
/// the longest existing prefix and re-append the remainder,
/// then require containment in the canonical cwd (P3-A
/// pattern: a symlinked segment that escapes is rejected).
/// `None` when the path escapes or the cwd is not a
/// directory.
pub fn contained_path(cwd: &Path, relative: &str) -> Option<PathBuf> {
    let cwd_canonical = cwd.canonicalize().ok()?;
    if !cwd_canonical.is_dir() {
        return None;
    }
    if relative.is_empty() || relative == "." {
        return Some(cwd_canonical);
    }
    let joined = cwd_canonical.join(relative.replace('/', std::path::MAIN_SEPARATOR_STR));
    // Walk up to the longest existing ancestor so a
    // not-yet-existing (or diff-only) path still checks.
    let mut existing = joined.as_path();
    let mut remainder: Vec<std::ffi::OsString> = Vec::new();
    while !existing.exists() {
        match existing.parent() {
            Some(parent) if parent != existing => {
                if let Some(name) = existing.file_name() {
                    remainder.push(name.to_os_string());
                }
                existing = parent;
            }
            _ => return None,
        }
    }
    let mut canonical = existing.canonicalize().ok()?;
    for part in remainder.iter().rev() {
        canonical.push(part);
    }
    if canonical.strip_prefix(&cwd_canonical).is_err() {
        return None;
    }
    Some(canonical)
}

/// Resolve a caller command to argv. Only the two read
/// templates exist in the slice; anything else (including
/// the P3-C write templates) is 400 `command_not_allowlisted`.
/// `params` is the raw JSON object from the request: status
/// takes none, diff takes an optional `path`.
pub fn resolve_command(
    command: &str,
    params: Option<&serde_json::Value>,
    git: &Path,
    cwd: &Path,
    timeout_ms: Option<u64>,
) -> Result<ResolvedCommand, CommandError> {
    let timeout = match timeout_ms {
        Some(0) => return Err(CommandError::BadParam("invalid_timeout")),
        Some(ms) if ms > WORKER_MAX_TIMEOUT_MS => {
            return Err(CommandError::BadParam("invalid_timeout"))
        }
        Some(ms) => ms,
        None => WORKER_DEFAULT_TIMEOUT_MS,
    };
    let git = git.to_string_lossy().into_owned();
    match command {
        "git-status" => {
            if let Some(serde_json::Value::Object(map)) = params {
                if !map.is_empty() {
                    return Err(CommandError::BadParam("invalid_params"));
                }
            } else if params.is_some() {
                return Err(CommandError::BadParam("invalid_params"));
            }
            Ok(ResolvedCommand {
                template: "git-status",
                argv: vec![
                    git,
                    "status".to_string(),
                    "--porcelain=v1".to_string(),
                    "--branch".to_string(),
                ],
                timeout_ms: timeout,
            })
        }
        "git-diff" => {
            let mut path: Option<String> = None;
            if let Some(serde_json::Value::Object(map)) = params {
                for (key, value) in map {
                    match (key.as_str(), value) {
                        ("path", serde_json::Value::String(p)) => {
                            validate_diff_path(p)?;
                            if contained_path(cwd, p).is_none() {
                                return Err(CommandError::BadParam("invalid_path"));
                            }
                            path = Some(p.clone());
                        }
                        _ => return Err(CommandError::BadParam("invalid_params")),
                    }
                }
            } else if params.is_some() {
                return Err(CommandError::BadParam("invalid_params"));
            }
            let mut argv = vec![
                git,
                "diff".to_string(),
                "--no-color".to_string(),
                "--no-ext-diff".to_string(),
            ];
            if let Some(p) = path {
                argv.push("--".to_string());
                argv.push(p);
            }
            Ok(ResolvedCommand {
                template: "git-diff",
                argv,
                timeout_ms: timeout,
            })
        }
        _ => Err(CommandError::NotAllowlisted),
    }
}

/// Truncate combined output to the tail: keep the last
/// `WORKER_TAIL_LIMIT` bytes, lossy UTF-8, with a flag.
pub fn truncate_tail(bytes: &[u8]) -> (String, bool) {
    if bytes.len() <= WORKER_TAIL_LIMIT {
        (String::from_utf8_lossy(bytes).into_owned(), false)
    } else {
        let tail = &bytes[bytes.len() - WORKER_TAIL_LIMIT..];
        (String::from_utf8_lossy(tail).into_owned(), true)
    }
}

fn combined_tail(stdout: &[u8], stderr: &[u8]) -> (String, bool) {
    let mut combined = Vec::with_capacity(stdout.len() + stderr.len());
    combined.extend_from_slice(stdout);
    combined.extend_from_slice(stderr);
    truncate_tail(&combined)
}

/// Spawn a pipe reader that appends chunks to a shared
/// buffer: the driver can abandon a reader whose pipe is
/// held open by a grandchild without losing what arrived.
fn spawn_reader(
    pipe: tokio::process::ChildStdout,
    shared: Arc<Mutex<Vec<u8>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut pipe = pipe;
        let mut chunk = [0u8; 8192];
        loop {
            match pipe.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(mut buf) = shared.lock() {
                        buf.extend_from_slice(&chunk[..n]);
                    }
                }
            }
        }
    })
}

fn spawn_stderr_reader(
    pipe: tokio::process::ChildStderr,
    shared: Arc<Mutex<Vec<u8>>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut pipe = pipe;
        let mut chunk = [0u8; 8192];
        loop {
            match pipe.read(&mut chunk).await {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(mut buf) = shared.lock() {
                        buf.extend_from_slice(&chunk[..n]);
                    }
                }
            }
        }
    })
}

async fn drain_readers(
    out_task: tokio::task::JoinHandle<()>,
    err_task: tokio::task::JoinHandle<()>,
) {
    let drain = async {
        let _ = out_task.await;
        let _ = err_task.await;
    };
    if tokio::time::timeout(Duration::from_millis(READER_DRAIN_TIMEOUT_MS), drain)
        .await
        .is_err()
    {
        // A grandchild holds a pipe open: abandon the
        // readers (their handles drop with the tasks) and
        // keep whatever was collected so far.
    }
}

/// Build the child command: argv array, `env_clear` plus the
/// minimal set {PATH, HOME, TMPDIR} (plan §3.4). Proxy
/// variables are never set; no credential ever enters the
/// environment. The git binary's directory leads PATH so the
/// resolved binary is the one that runs even under a thin
/// GUI PATH.
fn build_child_command(
    state: &AppState,
    argv: &[String],
    cwd: &Path,
    git_path: &Path,
) -> std::process::Command {
    let mut cmd = std::process::Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.env_clear();
    let mut path_value = String::new();
    if let Some(dir) = git_path.parent() {
        path_value.push_str(&dir.to_string_lossy());
        // PATH separator, not the file separator.
        path_value.push(if cfg!(windows) { ';' } else { ':' });
    }
    if let Some(inherited) = std::env::var_os("PATH") {
        path_value.push_str(&inherited.to_string_lossy());
    }
    if !path_value.is_empty() {
        cmd.env("PATH", path_value);
    }
    if let Some(home) = std::env::var_os("HOME") {
        cmd.env("HOME", home);
    } else {
        cmd.env("HOME", state.files_root.clone());
    }
    let tmpdir = std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    cmd.env("TMPDIR", tmpdir);
    cmd.current_dir(cwd);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Own process group: `kill_process_group` clears the
        // whole tree, grandchildren included.
        cmd.process_group(0);
    }
    cmd
}

/// The one Worker driver (plan §3.1): one `tokio::spawn`
/// task per run. Steps: deadline check → Assignment-active
/// check → spawn → wait with timeout → completion-time
/// Assignment check → run row + (unknown-only) Effect +
/// plan_position checkpoint (+ Execution transition on
/// cancel / deadline refusal only).
///
/// Every run-row write is conditional on the row still
/// being `queued`/`running`: a recovery scan that already
/// finalized the row (orphaned/lost) wins, and this driver
/// never overwrites it — and never writes an Effect for a
/// run the scan already closed.
pub async fn drive_worker_run(state: AppState, run_id: String) {
    let Some(sqlite) = state.sqlite.clone() else {
        return;
    };
    let Some(run) = ({
        let conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::get_worker_run(&conn, &run_id).ok().flatten()
    }) else {
        return;
    };
    if run.status != "queued" {
        return;
    }
    let started = now_ms();

    // Cancelled before the driver even started: no spawn.
    if state.is_worker_cancelled(&run_id) {
        finish_cancelled(&state, &run, started, String::new(), false).await;
        return;
    }
    // Deadline (plan §2.4): refuse to start, fail the
    // Execution with `budget_exceeded`.
    if let Some(execution_id) = &run.execution_id {
        let expired = {
            let conn = sqlite.lock().expect("sqlite lock");
            sqlite_store::get_execution_deadline(&conn, execution_id)
                .ok()
                .flatten()
                .is_some_and(|deadline| started >= deadline)
        };
        if expired {
            let (tail, truncated) =
                truncate_tail(b"budget_exceeded: execution deadline passed before start");
            let finished = sqlite_store::RunFinish {
                status: "failed".to_string(),
                exit_code: None,
                tail,
                truncated,
                effect_id: None,
            };
            let applied = {
                let mut conn = sqlite.lock().expect("sqlite lock");
                sqlite_store::finish_worker_run(&mut conn, &run_id, &finished)
                    .ok()
                    .unwrap_or(false)
            };
            if applied {
                let mut conn = sqlite.lock().expect("sqlite lock");
                let _ = sqlite_store::transition_execution(&mut conn, execution_id, "failed");
            }
            return;
        }
    }
    // Assignment-active at start (plan §3.1): a run whose
    // driver was already superseded never spawns.
    if let Some(assignment_id) = &run.assignment_id {
        let active = {
            let conn = sqlite.lock().expect("sqlite lock");
            sqlite_store::get_assignment(&conn, assignment_id)
                .ok()
                .flatten()
                .is_some_and(|row| row.status == "active")
        };
        if !active {
            let finished = sqlite_store::RunFinish {
                status: "superseded".to_string(),
                exit_code: None,
                tail: "assignment no longer active".to_string(),
                truncated: false,
                effect_id: None,
            };
            let mut conn = sqlite.lock().expect("sqlite lock");
            let _ = sqlite_store::finish_worker_run(&mut conn, &run_id, &finished);
            return;
        }
    }

    let Some(git_path) = state.git_path.clone() else {
        // Resolved at creation time; a missing binary here
        // (path deleted after creation) fails the run, never
        // spawns a fallback.
        let (tail, truncated) = truncate_tail(b"git_unavailable");
        let finished = sqlite_store::RunFinish {
            status: "failed".to_string(),
            exit_code: None,
            tail,
            truncated,
            effect_id: None,
        };
        let mut conn = sqlite.lock().expect("sqlite lock");
        let _ = sqlite_store::finish_worker_run(&mut conn, &run_id, &finished);
        return;
    };
    let argv: Vec<String> = match serde_json::from_str(&run.argv_json) {
        Ok(argv) => argv,
        Err(_) => {
            let (tail, truncated) = truncate_tail(b"invalid argv_json");
            let finished = sqlite_store::RunFinish {
                status: "failed".to_string(),
                exit_code: None,
                tail,
                truncated,
                effect_id: None,
            };
            let mut conn = sqlite.lock().expect("sqlite lock");
            let _ = sqlite_store::finish_worker_run(&mut conn, &run_id, &finished);
            return;
        }
    };
    if argv.is_empty() {
        return;
    }
    let cwd = PathBuf::from(&run.cwd);
    let std_cmd = build_child_command(&state, &argv, &cwd, &git_path);
    let mut cmd = tokio::process::Command::from(std_cmd);
    cmd.kill_on_drop(true);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(detail) => {
            let (tail, truncated) = truncate_tail(format!("spawn failed: {detail}").as_bytes());
            let finished = sqlite_store::RunFinish {
                status: "failed".to_string(),
                exit_code: None,
                tail,
                truncated,
                effect_id: None,
            };
            let mut conn = sqlite.lock().expect("sqlite lock");
            let _ = sqlite_store::finish_worker_run(&mut conn, &run_id, &finished);
            return;
        }
    };
    let pid = child.id().map(|id| id as i64);
    // Claim the row as running (conditional: the scan may
    // have finalized it while we spawned). If the claim
    // fails, kill what we just spawned and stop: the scan
    // owns this run now.
    let claimed = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::mark_worker_run_running(&mut conn, &run_id, pid, started)
            .ok()
            .unwrap_or(false)
    };
    if !claimed {
        if let Some(pid) = pid {
            kill_process_group(pid, "TERM");
            kill_process_group(pid, "KILL");
        }
        let _ = child.kill().await;
        return;
    }
    let stdout_shared = Arc::new(Mutex::new(Vec::new()));
    let stderr_shared = Arc::new(Mutex::new(Vec::new()));
    let out_task = child
        .stdout
        .take()
        .map(|pipe| spawn_reader(pipe, stdout_shared.clone()));
    let err_task = child
        .stderr
        .take()
        .map(|pipe| spawn_stderr_reader(pipe, stderr_shared.clone()));

    let timeout = Duration::from_millis(run.timeout_ms.max(0) as u64);
    // Poll the child in 200ms slices instead of one long
    // await: the loop is where both the timeout and the
    // cancel flag are observed. A single `timeout(..,
    // child.wait())` would leave a TERM-ignoring child
    // unnoticed after a cancel, because nothing would ever
    // wake the driver again.
    let wait_started = std::time::Instant::now();
    let mut outcome: Option<&'static str> = None;
    let mut status: Option<std::process::ExitStatus> = None;
    loop {
        match tokio::time::timeout(Duration::from_millis(200), child.wait()).await {
            Ok(Ok(exit)) => {
                status = Some(exit);
                break;
            }
            Ok(Err(_)) => {
                outcome = Some("failed");
                break;
            }
            Err(_) => {
                if state.is_worker_cancelled(&run_id) {
                    // Cancel (plan §2.2): kill the group,
                    // grace, then SIGKILL. The flag check
                    // after the loop finalizes `cancelled`.
                    if let Some(pid) = pid {
                        kill_process_group(pid, "TERM");
                        match tokio::time::timeout(
                            Duration::from_millis(WORKER_KILL_GRACE_MS),
                            child.wait(),
                        )
                        .await
                        {
                            Ok(Ok(exit)) => {
                                status = Some(exit);
                            }
                            Ok(Err(_)) => {
                                outcome = Some("failed");
                            }
                            Err(_) => {
                                kill_process_group(pid, "KILL");
                                status = child.wait().await.ok();
                            }
                        }
                    }
                    break;
                }
                if wait_started.elapsed() >= timeout {
                    // Timeout (plan §2.2): kill the process
                    // group, grace, then SIGKILL — never just
                    // drop the await and leave the tree writing.
                    if let Some(pid) = pid {
                        kill_process_group(pid, "TERM");
                        if tokio::time::timeout(
                            Duration::from_millis(WORKER_KILL_GRACE_MS),
                            child.wait(),
                        )
                        .await
                        .is_err()
                        {
                            kill_process_group(pid, "KILL");
                            let _ = child.wait().await;
                        }
                    }
                    outcome = Some("timeout");
                    break;
                }
            }
        }
    }
    // A wait error above (not a timeout) still leaves a
    // child to reap; the exit status is unknown.
    if outcome == Some("failed") && status.is_none() {
        status = child.wait().await.ok();
    }
    if let (Some(out), Some(err)) = (out_task, err_task) {
        drain_readers(out, err).await;
    }
    let stdout = stdout_shared.lock().map(|b| b.clone()).unwrap_or_default();
    let stderr = stderr_shared.lock().map(|b| b.clone()).unwrap_or_default();
    let (tail, truncated) = combined_tail(&stdout, &stderr);

    // Cancel wins over any exit the kill produced: the user
    // asked for it, the run did not finish on its own.
    if state.is_worker_cancelled(&run_id) {
        finish_cancelled(&state, &run, started, tail, truncated).await;
        return;
    }
    match outcome {
        Some("timeout") => {
            finish_unknown(&state, &run, "timeout", tail, truncated).await;
        }
        Some(_) => {
            // wait() I/O error: outcome unknown, no exit code.
            finish_unknown(&state, &run, "failed", tail, truncated).await;
        }
        None => {
            let exit = status.expect("wait returned without status");
            let code = exit.code().map(|c| c as i64);
            if exit.success() {
                finish_terminal(&state, &run, "succeeded", code, tail, truncated).await;
            } else {
                // A non-zero exit from a read command is a
                // known result (e.g. not a git repo), not an
                // unknown one: no Effect, no Inbox.
                finish_terminal(&state, &run, "failed", code, tail, truncated).await;
            }
        }
    }
}

/// Completion-time Assignment check (plan §2.2): the run's
/// Assignment must still be `active` before any Effect /
/// Evidence / transition write. A superseded run writes
/// nothing at all — no Effect, no checkpoint, no
/// transition.
async fn assignment_still_active(state: &AppState, run: &WorkerRunRow) -> bool {
    let Some(assignment_id) = &run.assignment_id else {
        return true;
    };
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let conn = sqlite.lock().expect("sqlite lock");
    sqlite_store::get_assignment(&conn, assignment_id)
        .ok()
        .flatten()
        .is_some_and(|row| row.status == "active")
}

/// Known terminal outcome (exit code): run row +
/// plan_position checkpoint, no Effect.
async fn finish_terminal(
    state: &AppState,
    run: &WorkerRunRow,
    status: &str,
    exit_code: Option<i64>,
    tail: String,
    truncated: bool,
) {
    if !assignment_still_active(state, run).await {
        finish_superseded(state, run).await;
        return;
    }
    let Some(sqlite) = state.sqlite.clone() else {
        return;
    };
    let finished = sqlite_store::RunFinish {
        status: status.to_string(),
        exit_code,
        tail,
        truncated,
        effect_id: None,
    };
    let applied = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::finish_worker_run(&mut conn, &run.run_id, &finished)
            .ok()
            .unwrap_or(false)
    };
    if applied {
        if let Some(execution_id) = &run.execution_id {
            let mut conn = sqlite.lock().expect("sqlite lock");
            let _ = sqlite_store::write_plan_position_checkpoint(
                &mut conn,
                execution_id,
                &state.origin_device_id,
            );
        }
    }
}

/// Unknown outcome (timeout / wait failure): run row + an
/// `unknown` Effect (which opens the Inbox row via the
/// normal upsert trigger) + plan_position checkpoint.
/// The Effect's scope is the run's cwd and its digest
/// commits the argv and the run id, so each run owns its
/// unknown row and no two runs share an idempotency key.
async fn finish_unknown(
    state: &AppState,
    run: &WorkerRunRow,
    run_status: &str,
    tail: String,
    truncated: bool,
) {
    if !assignment_still_active(state, run).await {
        finish_superseded(state, run).await;
        return;
    }
    let digest = sqlite_store::sha256_hex(
        sqlite_store::canonical_json(&serde_json::json!({
            "argv": run.argv_json,
            "command": run.command,
            "cwd": run.cwd,
            "run_id": run.run_id,
        }))
        .as_bytes(),
    );
    let effect = crate::server::record_effect(
        state,
        crate::effect_store::NewEffect {
            execution_id: run.execution_id.clone(),
            capability: "git".to_string(),
            operation: run.command.clone(),
            scope: run.cwd.clone(),
            input_digest: Some(digest),
            approval_id: None,
            status: "unknown".to_string(),
        },
    );
    // The run row's status names the reason; the Effect row
    // itself carries no output bytes.
    let Some(sqlite) = state.sqlite.clone() else {
        return;
    };
    let finished = sqlite_store::RunFinish {
        status: run_status.to_string(),
        exit_code: None,
        tail,
        truncated,
        effect_id: Some(effect.effect_id.clone()),
    };
    let applied = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::finish_worker_run(&mut conn, &run.run_id, &finished)
            .ok()
            .unwrap_or(false)
    };
    if applied {
        if let Some(execution_id) = &run.execution_id {
            let mut conn = sqlite.lock().expect("sqlite lock");
            let _ = sqlite_store::write_plan_position_checkpoint(
                &mut conn,
                execution_id,
                &state.origin_device_id,
            );
        }
    }
}

/// Cancelled: run row + `unknown` Effect (cancelled work
/// may or may not have landed — honest `unknown`, never
/// `failed`) + Execution `cancelled` (plan §2.2).
async fn finish_cancelled(
    state: &AppState,
    run: &WorkerRunRow,
    _started: i64,
    tail: String,
    truncated: bool,
) {
    // A pre-start cancel has no child and no Assignment
    // question beyond the same completion check.
    if !assignment_still_active(state, run).await {
        finish_superseded(state, run).await;
        return;
    }
    let digest = sqlite_store::sha256_hex(
        sqlite_store::canonical_json(&serde_json::json!({
            "argv": run.argv_json,
            "command": run.command,
            "cwd": run.cwd,
            "run_id": run.run_id,
        }))
        .as_bytes(),
    );
    let effect = crate::server::record_effect(
        state,
        crate::effect_store::NewEffect {
            execution_id: run.execution_id.clone(),
            capability: "git".to_string(),
            operation: run.command.clone(),
            scope: run.cwd.clone(),
            input_digest: Some(digest),
            approval_id: None,
            status: "unknown".to_string(),
        },
    );
    let Some(sqlite) = state.sqlite.clone() else {
        return;
    };
    let finished = sqlite_store::RunFinish {
        status: "cancelled".to_string(),
        exit_code: None,
        tail,
        truncated,
        effect_id: Some(effect.effect_id.clone()),
    };
    let applied = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::finish_worker_run(&mut conn, &run.run_id, &finished)
            .ok()
            .unwrap_or(false)
    };
    if applied {
        if let Some(execution_id) = &run.execution_id {
            let mut conn = sqlite.lock().expect("sqlite lock");
            let _ = sqlite_store::transition_execution(&mut conn, execution_id, "cancelled");
            let _ = sqlite_store::write_plan_position_checkpoint(
                &mut conn,
                execution_id,
                &state.origin_device_id,
            );
        }
    }
}

/// Superseded: the run row only. No Effect, no Evidence,
/// no checkpoint, no transition — the takeover owner now
/// owns this Execution's writes.
async fn finish_superseded(state: &AppState, run: &WorkerRunRow) {
    let Some(sqlite) = state.sqlite.clone() else {
        return;
    };
    let finished = sqlite_store::RunFinish {
        status: "superseded".to_string(),
        exit_code: None,
        tail: "assignment no longer active".to_string(),
        truncated: false,
        effect_id: None,
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    let _ = sqlite_store::finish_worker_run(&mut conn, &run.run_id, &finished);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_rejects_everything_not_listed() {
        let git = Path::new("/usr/bin/git");
        let cwd = std::env::temp_dir();
        for command in [
            "git",
            "cargo test",
            "git worktree add",
            "git commit",
            "sh",
            "",
        ] {
            assert_eq!(
                resolve_command(command, None, git, &cwd, None),
                Err(CommandError::NotAllowlisted),
                "{command}"
            );
        }
    }

    #[test]
    fn resolve_status_and_diff_argv() {
        let git = Path::new("/usr/bin/git");
        let cwd = std::env::temp_dir();
        let status = resolve_command("git-status", None, git, &cwd, None).expect("status");
        assert_eq!(status.template, "git-status");
        assert_eq!(status.timeout_ms, WORKER_DEFAULT_TIMEOUT_MS);
        assert_eq!(
            status.argv,
            vec!["/usr/bin/git", "status", "--porcelain=v1", "--branch"]
        );
        let diff = resolve_command(
            "git-diff",
            Some(&serde_json::json!({"path": "a.txt"})),
            git,
            &cwd,
            Some(5_000),
        )
        .expect("diff");
        assert_eq!(
            diff.argv,
            vec![
                "/usr/bin/git",
                "diff",
                "--no-color",
                "--no-ext-diff",
                "--",
                "a.txt"
            ]
        );
        assert_eq!(diff.timeout_ms, 5_000);
        let diff_no_path =
            resolve_command("git-diff", None, git, &cwd, None).expect("diff no path");
        assert!(!diff_no_path.argv.contains(&"--".to_string()));
    }

    #[test]
    fn diff_path_validation_rejects_escapes_and_shell() {
        let git = Path::new("/usr/bin/git");
        let cwd = std::env::temp_dir();
        for bad in [
            serde_json::json!({"path": "../x"}),
            serde_json::json!({"path": "/abs/x"}),
            serde_json::json!({"path": "a;b"}),
            serde_json::json!({"path": "a|b"}),
            serde_json::json!({"path": "$(x)"}),
            serde_json::json!({"path": "`x`"}),
            serde_json::json!({"path": ""}),
            serde_json::json!({"other": "x"}),
        ] {
            assert!(
                resolve_command("git-diff", Some(&bad), git, &cwd, None).is_err(),
                "{bad}"
            );
        }
        assert!(resolve_command(
            "git-diff",
            Some(&serde_json::json!({"path": 1})),
            git,
            &cwd,
            None
        )
        .is_err());
        // Timeout bounds.
        assert!(resolve_command("git-status", None, git, &cwd, Some(0)).is_err());
        assert!(resolve_command(
            "git-status",
            None,
            git,
            &cwd,
            Some(WORKER_MAX_TIMEOUT_MS + 1)
        )
        .is_err());
        // Status takes no params.
        assert!(resolve_command(
            "git-status",
            Some(&serde_json::json!({"path": "a.txt"})),
            git,
            &cwd,
            None
        )
        .is_err());
    }

    #[test]
    fn contained_path_rejects_symlink_escape() {
        let dir =
            std::env::temp_dir().join(format!("mock-worker-contained-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("root/sub")).unwrap();
        std::fs::create_dir_all(dir.join("outside")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.join("outside"), dir.join("root/escape")).unwrap();
        let root = dir.join("root");
        let canonical_root = root.canonicalize().unwrap();
        assert_eq!(
            contained_path(&root, "sub").unwrap(),
            canonical_root.join("sub")
        );
        assert_eq!(contained_path(&root, ".").unwrap(), canonical_root);
        #[cfg(unix)]
        assert!(contained_path(&root, "escape").is_none());
        assert!(contained_path(&root, "../outside").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tail_keeps_last_4096_bytes_only() {
        let big = vec![b'x'; 5000];
        let (tail, truncated) = truncate_tail(&big);
        assert!(truncated);
        assert_eq!(tail.len(), WORKER_TAIL_LIMIT);
        let (small, truncated) = truncate_tail(b"hi");
        assert!(!truncated);
        assert_eq!(small, "hi");
    }

    #[test]
    fn kill_process_group_kills_grandchildren() {
        // Test-only process tree (a shell script is the
        // fixture, not a production execution path): the
        // child spawns a grandchild that outlives it, and
        // the group kill must take both.
        #[cfg(unix)]
        {
            let dir = std::env::temp_dir().join(format!("mock-worker-kill-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let marker = dir.join("grandchild.pid");
            let script = dir.join("tree.sh");
            std::fs::write(
                &script,
                format!(
                    "#!/bin/sh\nsleep 30 & echo $! > {}\nwait\n",
                    marker.display()
                ),
            )
            .unwrap();
            let mut cmd = std::process::Command::new("/bin/sh");
            cmd.arg(&script);
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::null());
            {
                use std::os::unix::process::CommandExt;
                cmd.process_group(0);
            }
            let mut child = cmd.spawn().expect("spawn tree");
            let pid = child.id() as i64;
            // Wait for the grandchild to record its pid.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !marker.exists() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
            }
            let grandchild: i64 = std::fs::read_to_string(&marker)
                .expect("marker")
                .trim()
                .parse()
                .expect("pid");
            assert!(process_alive(pid));
            assert!(process_alive(grandchild));
            assert!(kill_process_group(pid, "TERM"));
            // A `sh` sitting in the `wait` builtin defers
            // SIGTERM until its jobs end, so TERM alone may
            // leave the shell standing (the grandchild dies
            // at once). The driver therefore follows TERM
            // with SIGKILL after a grace period; mirror that
            // two-step here instead of asserting on TERM.
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while (process_alive(pid) || process_alive(grandchild))
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(50));
            }
            if process_alive(pid) || process_alive(grandchild) {
                assert!(kill_process_group(pid, "KILL"));
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while (process_alive(pid) || process_alive(grandchild))
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(50));
            }
            assert!(!process_alive(pid), "child survived group KILL");
            assert!(!process_alive(grandchild), "grandchild survived group KILL");
            let _ = child.wait();
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
