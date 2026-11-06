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

/// E2-B §3.1 (lease/heartbeat, plan §13 numbers): the driver
/// writes a heartbeat every 5 s while a run is in flight;
/// the lease is the heartbeat + 30 s. Expiry never kills the
/// process and never retries — it only changes the visible
/// state of the assignment and its Execution (the sweeper,
/// server.rs). A failed heartbeat write is log-only: it must
/// not kill the run and must not record an Effect.
pub const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
pub const WORKER_LEASE_TTL_MS: i64 = 30_000;

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
    /// P3-C: the approval this run was authorized by, when
    /// one was presented at creation (digest-bound approvals
    /// are consumed at creation; loop-bound approvals were
    /// already consumed by the loop). The driver copies it
    /// onto the completion Effect for attribution.
    pub approval_id: Option<String>,
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

/// E2-A §2.8: Worker run status → Execution status for the
/// historical reconciliation (the same mapping as the §2.3
/// coupling: succeeded→completed, failed→failed,
/// cancelled→cancelled, timeout/orphaned/lost→needs_review).
/// `superseded` has no target: the takeover owner owns that
/// Execution now. Non-terminal run words return None.
pub fn run_status_to_execution(status: &str) -> Option<&'static str> {
    match status {
        "succeeded" => Some("completed"),
        "failed" => Some("failed"),
        "cancelled" => Some("cancelled"),
        "timeout" | "orphaned" | "lost" => Some("needs_review"),
        _ => None,
    }
}

fn now_ms() -> i64 {
    crate::effect_store::now_ms() as i64
}

// --- git resolution (plan §3.4) ---

/// Shared binary resolution: an env override wins when set
/// (a set-but-missing value fails closed to "no binary",
/// never silently falls back to PATH); otherwise the first
/// matching name on PATH.
fn resolve_binary(env_var: &str, names: &[&str]) -> Option<PathBuf> {
    if let Ok(value) = std::env::var(env_var) {
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
        for name in names {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// Resolve the git binary once, at gateway startup:
/// `MOCK_GIT_PATH` wins when set (a set-but-missing value
/// fails closed to "no git", never silently falls back to
/// PATH); otherwise the first `git` on PATH. `None` means
/// Worker git commands fail closed with 503
/// `git_unavailable`.
pub fn resolve_git_path() -> Option<PathBuf> {
    resolve_binary("MOCK_GIT_PATH", &["git", "git.exe"])
}

/// P3-C: the cargo binary, same shape as git
/// (`MOCK_CARGO_PATH` override, else PATH). `None` means
/// `cargo-test` runs fail closed with 503
/// `cargo_unavailable`. Tests pin it to the native fixture
/// through `AppState::with_cargo_path`, exactly like git.
pub fn resolve_cargo_path() -> Option<PathBuf> {
    resolve_binary("MOCK_CARGO_PATH", &["cargo", "cargo.exe"])
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
    #[cfg(target_os = "macos")]
    {
        // macOS 没有 /proc；kill -0 会把尚未回收的僵尸误判为存活。
        // 只读取状态列，避免将已经退出的进程送入恢复/终止流程。
        std::process::Command::new("/bin/ps")
            .args(["-p", &pid.to_string(), "-o", "stat="])
            .output()
            .ok()
            .filter(|out| out.status.success())
            .is_some_and(|out| {
                let state = String::from_utf8_lossy(&out.stdout);
                let state = state.trim();
                !state.is_empty() && !state.starts_with('Z') && !state.starts_with('X')
            })
    }
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
    #[cfg(all(unix, not(target_os = "macos")))]
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
/// P3-C: the loop engine reuses this for `git.read` /
/// `worker.run` path arguments (same rule, same error).
pub(crate) fn validate_diff_path(path: &str) -> Result<(), CommandError> {
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

/// Validate a worktree branch name (plan §3.3): ASCII
/// letters, digits, `.`, `_`, `-` only, no slash, non-empty.
/// The plan's charset is the whole rule; the length bound
/// keeps the argv bounded.
fn validate_branch_name(branch: &str) -> Result<(), CommandError> {
    if branch.is_empty() || branch.len() > 200 {
        return Err(CommandError::BadParam("invalid_branch"));
    }
    if !branch
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(CommandError::BadParam("invalid_branch"));
    }
    Ok(())
}

/// Validate a cargo package name (plan §3.3): ASCII letters,
/// digits, `_`, `-` only, non-empty. Same bound as branches.
fn validate_package_name(package: &str) -> Result<(), CommandError> {
    if package.is_empty() || package.len() > 200 {
        return Err(CommandError::BadParam("invalid_package"));
    }
    if !package
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
    {
        return Err(CommandError::BadParam("invalid_package"));
    }
    Ok(())
}

/// Extra environment for a template (plan §3.4): cargo test
/// runs offline, so no network is ever attempted. The driver
/// rebuilds this from the run's template (the run row stores
/// argv only), so this is the single source for the rule.
pub(crate) fn extra_env_for_command(template: &str) -> Vec<(String, String)> {
    match template {
        "cargo-test" => vec![("CARGO_NET_OFFLINE".to_string(), "true".to_string())],
        _ => Vec::new(),
    }
}

/// Whether a template produces an Effect at completion
/// (P3-C §4/§5): the write and test templates. Reads never
/// do (their terminal outcome is a known result, no Effect).
pub(crate) fn is_effect_command(template: &str) -> bool {
    matches!(
        template,
        "git-worktree-add" | "git-commit" | "cargo-test"
    )
}

/// Resolve a caller command to argv. The slice's two read
/// templates plus the P3-C write/test templates (§3.3):
/// git-worktree-add, git-commit, cargo-test. Anything else is
/// 400 `command_not_allowlisted`. `params` is the raw JSON
/// object from the request; `binary` is the resolved
/// executable for the template (git, or cargo for
/// cargo-test); `location_root` is the Location's canonical
/// root, used only by git-worktree-add (its path is relative
/// to the Location root, plan §4.2 — the argv carries the
/// absolute path because git resolves relative paths against
/// the child's cwd, not the Location root).
pub fn resolve_command(
    command: &str,
    params: Option<&serde_json::Value>,
    binary: &Path,
    cwd: &Path,
    timeout_ms: Option<u64>,
    location_root: Option<&Path>,
) -> Result<ResolvedCommand, CommandError> {
    // Per-template defaults (plan §3.3): reads 30s,
    // worktree-add / commit 60s, cargo test 600s.
    let default_timeout = match command {
        "git-worktree-add" | "git-commit" => 60_000,
        "cargo-test" => 600_000,
        _ => WORKER_DEFAULT_TIMEOUT_MS,
    };
    let timeout = match timeout_ms {
        Some(0) => return Err(CommandError::BadParam("invalid_timeout")),
        Some(ms) if ms > WORKER_MAX_TIMEOUT_MS => {
            return Err(CommandError::BadParam("invalid_timeout"))
        }
        Some(ms) => ms,
        None => default_timeout,
    };
    let binary = binary.to_string_lossy().into_owned();
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
                    binary,
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
                binary,
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
        "git-worktree-add" => {
            let Some(location_root) = location_root else {
                return Err(CommandError::BadParam("invalid_path"));
            };
            let (path, branch) = match params {
                Some(serde_json::Value::Object(map)) => {
                    let mut path: Option<String> = None;
                    let mut branch: Option<String> = None;
                    for (key, value) in map {
                        match (key.as_str(), value) {
                            ("path", serde_json::Value::String(p)) => {
                                validate_diff_path(p)?;
                                path = Some(p.clone());
                            }
                            ("branch", serde_json::Value::String(b)) => {
                                validate_branch_name(b)?;
                                branch = Some(b.clone());
                            }
                            _ => {
                                return Err(CommandError::BadParam("invalid_params"));
                            }
                        }
                    }
                    match (path, branch) {
                        (Some(path), Some(branch)) => (path, branch),
                        _ => return Err(CommandError::BadParam("invalid_params")),
                    }
                }
                _ => return Err(CommandError::BadParam("invalid_params")),
            };
            // The argv carries the absolute path (git resolves
            // relative paths against the child's cwd); the
            // Effect scope and the Worktree record keep the
            // Location-relative path.
            let Some(target) = contained_path(location_root, &path) else {
                return Err(CommandError::BadParam("invalid_path"));
            };
            Ok(ResolvedCommand {
                template: "git-worktree-add",
                argv: vec![
                    binary,
                    "worktree".to_string(),
                    "add".to_string(),
                    // Git for Windows 不接受普通盘符路径的 verbatim 前缀；
                    // 仅转换命令参数，绑定校验仍使用上面的 canonical 路径。
                    dunce::simplified(&target).to_string_lossy().into_owned(),
                    "-b".to_string(),
                    branch,
                ],
                timeout_ms: timeout,
            })
        }
        "git-commit" => {
            let message = match params {
                Some(serde_json::Value::Object(map)) => {
                    let mut message: Option<String> = None;
                    for (key, value) in map {
                        match (key.as_str(), value) {
                            ("message", serde_json::Value::String(m)) => {
                                message = Some(m.clone());
                            }
                            _ => {
                                return Err(CommandError::BadParam("invalid_params"));
                            }
                        }
                    }
                    message
                }
                _ => None,
            };
            let Some(message) = message else {
                return Err(CommandError::BadParam("invalid_params"));
            };
            if message.trim().is_empty() || message.chars().count() > 2000 {
                return Err(CommandError::BadParam("invalid_message"));
            }
            Ok(ResolvedCommand {
                template: "git-commit",
                argv: vec![
                    binary,
                    "commit".to_string(),
                    "--no-verify".to_string(),
                    "-m".to_string(),
                    message,
                ],
                timeout_ms: timeout,
            })
        }
        "cargo-test" => {
            let package = match params {
                Some(serde_json::Value::Object(map)) => {
                    let mut package: Option<String> = None;
                    for (key, value) in map {
                        match (key.as_str(), value) {
                            ("package", serde_json::Value::String(p)) => {
                                validate_package_name(p)?;
                                package = Some(p.clone());
                            }
                            _ => {
                                return Err(CommandError::BadParam("invalid_params"));
                            }
                        }
                    }
                    package
                }
                _ => None,
            };
            let Some(package) = package else {
                return Err(CommandError::BadParam("invalid_params"));
            };
            Ok(ResolvedCommand {
                template: "cargo-test",
                argv: vec![
                    binary,
                    "test".to_string(),
                    "-p".to_string(),
                    package,
                ],
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

/// Full combined output (stdout then stderr), lossy UTF-8,
/// untruncated: the test parser reads this, not the stored
/// tail — Cargo's trailing stderr progress would otherwise
/// push the `test result:` summary lines out of the tail
/// (P3-C window finding: passing runs parsed null counts).
fn combined_output(stdout: &[u8], stderr: &[u8]) -> String {
    let mut combined = Vec::with_capacity(stdout.len() + stderr.len());
    combined.extend_from_slice(stdout);
    combined.extend_from_slice(stderr);
    String::from_utf8_lossy(&combined).into_owned()
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
/// minimal set {PATH, HOME, TMPDIR} plus Windows toolchain discovery. Proxy
/// variables are never set; no credential ever enters the
/// environment. The binary's directory leads PATH so the
/// resolved binary is the one that runs even under a thin
/// GUI PATH. `extra_env` carries the template's additions
/// (P3-C: `CARGO_NET_OFFLINE=true` for cargo test).
fn build_child_command(
    state: &AppState,
    argv: &[String],
    cwd: &Path,
    binary_path: &Path,
    extra_env: &[(String, String)],
) -> std::process::Command {
    let mut cmd = std::process::Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    cmd.env_clear();
    let mut path_value = String::new();
    if let Some(dir) = binary_path.parent() {
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
    // Windows 的 rustc 依赖这些系统/SDK 变量定位 MSVC；丢失后可能
    // 误选 PATH 中 Git 自带的 GNU link.exe。只传递明确的工具链变量。
    #[cfg(windows)]
    for key in [
        "SystemRoot",
        "SystemDrive",
        "TEMP",
        "TMP",
        "USERPROFILE",
        "LOCALAPPDATA",
        "APPDATA",
        "ProgramFiles",
        "ProgramFiles(x86)",
        "ProgramW6432",
        "VSINSTALLDIR",
        "VCINSTALLDIR",
        "VCToolsInstallDir",
        "WindowsSdkDir",
        "WindowsSDKVersion",
        "UniversalCRTSdkDir",
        "UCRTVersion",
        "INCLUDE",
        "LIB",
        "LIBPATH",
    ] {
        if let Some(value) = std::env::var_os(key) {
            cmd.env(key, value);
        }
    }
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
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

/// Synchronous short command run (P3-C): the `git.read` loop
/// tool and the Evidence observe path run read-only git
/// commands synchronously. Same argv/env rules as the async
/// driver, with a wall-clock cap: on timeout the child is
/// killed and the outcome reports `timed_out`. No shell, no
/// credential, no retry. The caller decides what an exit code
/// or a timeout means for its class.
pub(crate) struct SyncRunOutput {
    pub exit_code: Option<i64>,
    pub stdout: String,
    pub timed_out: bool,
}

pub(crate) fn run_command_sync(
    state: &AppState,
    argv: &[String],
    cwd: &Path,
    binary_path: &Path,
    timeout: Duration,
) -> SyncRunOutput {
    let mut cmd = build_child_command(state, argv, cwd, binary_path, &[]);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(detail) => {
            eprintln!("warning: sync command spawn failed: {detail}");
            return SyncRunOutput {
                exit_code: None,
                stdout: String::new(),
                timed_out: false,
            };
        }
    };
    let deadline = std::time::Instant::now() + timeout;
    let exit = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    break None;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break None,
        }
    };
    // The child is dead (or killed): its pipes hold a finite
    // amount, so reading to EOF cannot block. Read-only git
    // commands spawn no grandchildren that hold the pipes.
    let stdout = child
        .stdout
        .take()
        .map(|mut pipe| {
            use std::io::Read;
            let mut buf = Vec::new();
            let _ = pipe.read_to_end(&mut buf);
            String::from_utf8_lossy(&buf).into_owned()
        })
        .unwrap_or_default();
    // stderr is drained (never stored) so a chatty child
    // cannot block on a full pipe; the callers only need
    // stdout + the exit code for read-only commands.
    let _ = child
        .stderr
        .take()
        .map(|mut pipe| {
            use std::io::Read;
            let mut buf = Vec::new();
            let _ = pipe.read_to_end(&mut buf);
        });
    SyncRunOutput {
        exit_code: exit.and_then(|status| status.code()).map(|c| c as i64),
        stdout,
        timed_out: exit.is_none(),
    }
}

/// Run a read-only git command synchronously and return its
/// trimmed stdout on exit 0, else None. The Evidence observe
/// path uses this (10s cap); a failure reads as "cannot
/// observe", never as a fabricated state.
pub(crate) fn git_read_sync(
    state: &AppState,
    git: &Path,
    args: &[&str],
    cwd: &Path,
) -> Option<String> {
    let argv: Vec<String> = std::iter::once(git.to_string_lossy().into_owned())
        .chain(args.iter().map(|a| a.to_string()))
        .collect();
    let out = run_command_sync(state, &argv, cwd, git, Duration::from_secs(10));
    if out.timed_out || out.exit_code != Some(0) {
        return None;
    }
    Some(out.stdout.trim().to_string())
}

/// Parsed `cargo test` summary (plan §5.1/§5.2): counts
/// summed over every `test result:` line in the full
/// combined output, failed test names (from the `failures:`
/// section) as sha256 only — names never leave the gateway
/// in plaintext (the observed_json rule: hashes, lengths,
/// kinds, flags only).
/// An output with no parseable summary line keeps counts null
/// (the exit code is ground truth; numbers are never
/// fabricated).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TestRunSummary {
    pub passed: Option<u64>,
    pub failed: Option<u64>,
    pub failed_sha256: Vec<String>,
}

fn parse_test_count(segment: &str, word: &str) -> Option<u64> {
    let rest = segment.trim().strip_suffix(word)?.trim();
    rest.split_whitespace().last()?.parse::<u64>().ok()
}

pub fn parse_test_output(output: &str) -> TestRunSummary {
    let mut passed: Option<u64> = None;
    let mut failed: Option<u64> = None;
    let mut failed_names: Vec<String> = Vec::new();
    let mut in_failures = false;
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("test result:") {
            in_failures = false;
            let rest = trimmed.trim_start_matches("test result:");
            for segment in rest.split(';') {
                if let Some(n) = parse_test_count(segment, "passed") {
                    passed = Some(passed.unwrap_or(0) + n);
                }
                if let Some(n) = parse_test_count(segment, "failed") {
                    failed = Some(failed.unwrap_or(0) + n);
                }
            }
            continue;
        }
        if trimmed == "failures:" {
            in_failures = true;
            continue;
        }
        if in_failures {
            if trimmed.is_empty() {
                in_failures = false;
            } else {
                failed_names.push(trimmed.to_string());
            }
        }
    }
    TestRunSummary {
        passed,
        failed,
        failed_sha256: failed_names
            .iter()
            .map(|name| crate::sqlite_store::sha256_hex(name.as_bytes()))
            .collect(),
    }
}

/// Relativize an absolute path against a canonical root:
/// `Some(relative)` when contained, else None. The path
/// itself need not exist (the worktree may not have been
/// created yet); containment is checked on the canonical
/// longest existing prefix, like `contained_path`.
fn relativize_to_root(root: &Path, absolute: &str) -> Option<String> {
    let root_canonical = root.canonicalize().ok()?;
    let path = Path::new(absolute);
    let mut existing = path;
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
    let relative = canonical.strip_prefix(&root_canonical).ok()?;
    // Effect scope 与 Worktree API 使用统一的 / 分隔格式，不能泄漏
    // Windows 本机分隔符，否则同一目标的摘要和记录校验会不一致。
    Some(
        relative
            .components()
            .map(|part| part.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/"),
    )
}

/// P3-C Effect identity for an unknown run (timeout / cancel /
/// orphaned / lost): the template's vocabulary, not the
/// template name. Reads keep the slice shape (the slice's
/// unknown read Effects are already in the wild as
/// ("git", "git-status" / "git-diff", cwd)); writes and tests
/// use the P3-C vocabulary (§4.4/Q5). `location_root` is the
/// run's Location canonical root when the caller can resolve
/// it (the driver); the recovery scan passes None and the
/// scope falls back to the absolute path (the Location root
/// is not in SQLite). The scope is display/identity only:
/// observe re-derives the absolute path from the run row.
pub(crate) fn unknown_effect_identity(
    run: &WorkerRunRow,
    location_root: Option<&Path>,
) -> (String, String, String) {
    let argv: Vec<String> = serde_json::from_str(&run.argv_json).unwrap_or_default();
    match run.command.as_str() {
        "git-worktree-add" => {
            let absolute = argv.get(3).cloned().unwrap_or_else(|| run.cwd.clone());
            let scope = match location_root {
                Some(root) => {
                    relativize_to_root(root, &absolute).unwrap_or(absolute)
                }
                None => absolute,
            };
            (
                "git".to_string(),
                "worktree-add".to_string(),
                scope,
            )
        }
        "git-commit" => {
            let scope = match location_root {
                Some(root) => {
                    relativize_to_root(root, &run.cwd).unwrap_or_else(|| run.cwd.clone())
                }
                None => run.cwd.clone(),
            };
            ("git".to_string(), "commit".to_string(), scope)
        }
        "cargo-test" => (
            "test".to_string(),
            "run".to_string(),
            "cargo-test".to_string(),
        ),
        _ => (
            "git".to_string(),
            run.command.clone(),
            run.cwd.clone(),
        ),
    }
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
    // The binary is the one resolved at creation (argv[0]):
    // a path deleted after creation fails the run, never
    // spawns a fallback. The 503 at creation already
    // distinguished git_unavailable / cargo_unavailable; the
    // tail here names the template's binary the same way.
    // No Effect: the command never ran, so nothing landed.
    let binary_path = PathBuf::from(&argv[0]);
    if !binary_path.is_file() {
        let code = if run.command == "cargo-test" {
            "cargo_unavailable"
        } else {
            "git_unavailable"
        };
        let (tail, truncated) = truncate_tail(code.as_bytes());
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
    let cwd = PathBuf::from(&run.cwd);
    let extra_env = extra_env_for_command(&run.command);
    let std_cmd = build_child_command(&state, &argv, &cwd, &binary_path, &extra_env);
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
    // E2-B §3.1: hold the lease from the moment the driver
    // starts driving (first heartbeat now), then every 5 s
    // at the top of the poll loop. A failed write is log-only
    // (see write_heartbeat).
    write_heartbeat(&state, &run);
    let mut last_heartbeat = std::time::Instant::now();
    let mut outcome: Option<&'static str> = None;
    let mut status: Option<std::process::ExitStatus> = None;
    loop {
        if last_heartbeat.elapsed() >= WORKER_HEARTBEAT_INTERVAL {
            write_heartbeat(&state, &run);
            last_heartbeat = std::time::Instant::now();
        }
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
    // The stored tail stays truncated at WORKER_TAIL_LIMIT;
    // the test parser reads the full combined output (see
    // combined_output).
    let output = combined_output(&stdout, &stderr);
    let (tail, truncated) = truncate_tail(output.as_bytes());

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
                finish_terminal(&state, &run, "succeeded", code, output, started).await;
            } else {
                // A non-zero exit is a known result (e.g. not
                // a git repo, a failing test, a rejected
                // commit), not an unknown one: no Inbox. Reads
                // write no Effect; writes and tests record a
                // `failed` Effect (+ Test Evidence for tests).
                finish_terminal(&state, &run, "failed", code, output, started).await;
            }
        }
    }
}

/// E2-B §3.1: one heartbeat write for the run's driving
/// assignment (`lease = heartbeat + WORKER_LEASE_TTL_MS`).
/// Log-only on failure — a heartbeat is a liveness signal,
/// not a side effect, so a failed write must not kill the run
/// and must not record an Effect. No assignment (an unbound
/// run) → nothing to hold, silently skipped.
fn write_heartbeat(state: &AppState, run: &WorkerRunRow) {
    let Some(assignment_id) = &run.assignment_id else {
        return;
    };
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let now = now_ms();
    let conn = sqlite.lock().expect("sqlite lock");
    // Best-effort hold: once the assignment is no longer
    // driving (swept lost, superseded, done), the lease is
    // no longer this driver's to hold, so stop writing. The
    // check-then-write is racy by design — a sweep landing
    // between the two still leaves a lost row whose heartbeat
    // advanced once, which no consumer reads — but the
    // steady state is a frozen heartbeat on a lost row.
    let still_driving = sqlite_store::get_assignment(&conn, assignment_id)
        .ok()
        .flatten()
        .is_some_and(|row| matches!(row.status.as_str(), "assigned" | "active"));
    if !still_driving {
        return;
    }
    if let Err(detail) = sqlite_store::update_assignment_heartbeat(
        &conn,
        assignment_id,
        now,
        now + WORKER_LEASE_TTL_MS,
    ) {
        eprintln!("warning: E2-B worker heartbeat write failed for {assignment_id}: {detail}");
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

/// Run-specific digest (slice formula): commits the argv,
/// command, cwd and run id, so each run owns its unknown /
/// test Effect row and no two runs share an idempotency key.
///
/// E2-A §2.4: this digest IS the Worker driver dedupe — the
/// driver deliberately has no `check_idempotency` call. A
/// duplicate run cannot be created through the route in the
/// first place: a digest-bound approval is single-use
/// (replay → 403 `approval_binding_mismatch`), the run row
/// carries a UNIQUE `approval_id` index (concurrent replay →
/// 409 `approval_already_used`), and one Execution admits at
/// most one queued/running run (→ 409
/// `execution_has_running_worker`). Locked by A4 in
/// tests/e2a_ledger.rs.
pub(crate) fn run_digest(run: &WorkerRunRow) -> String {
    crate::sqlite_store::sha256_hex(
        crate::sqlite_store::canonical_json(&serde_json::json!({
            "argv": run.argv_json,
            "command": run.command,
            "cwd": run.cwd,
            "run_id": run.run_id,
        }))
        .as_bytes(),
    )
}

/// P3-C completion Effect + Evidence for a write/test run
/// (§4.2/§4.4/§5.2). Returns the Effect id and the Evidence
/// rows to insert. The Effect is recorded first (its id goes
/// on the run row); the caller inserts the Evidence rows only
/// when the run-row write applied (a scan-finalized run gets
/// no Evidence). Reads return nothing: their terminal outcome
/// is a known result, no Effect.
///
/// - git-worktree-add, exit 0: Effect (git, worktree-add,
///   scope) succeeded + write-time Evidence (new worktree
///   HEAD) + the Worktree record (§4.2, after command
///   success; a record failure only warns — the git worktree
///   exists and the user can register it via the API).
///   Non-zero: failed Effect, no Evidence, no record.
/// - git-commit, exit 0: Effect (git, commit, scope)
///   succeeded + write-time Evidence (commit/parent/tree).
///   Non-zero: failed Effect, no Evidence.
/// - cargo-test: Effect (test, run, "cargo-test") succeeded
///   (exit 0) / failed (non-zero) + Test Evidence (source
///   "test", reason test_passed/test_failed, closed observed
///   set). No Patch change, no Inbox row (§5.4).
///
/// A git read that fails degrades the digest / drops the
/// baseline (empty-string hash, no Evidence row); the Effect
/// status itself follows the exit code, which is ground
/// truth. A failed read never fabricates a hash.
async fn completion_effect(
    state: &AppState,
    run: &WorkerRunRow,
    status: &str,
    exit_code: Option<i64>,
    // Full untruncated combined output (stdout then stderr):
    // the test parser reads this, not the stored tail.
    output: &str,
    started_ms: i64,
) -> (Option<String>, Vec<crate::evidence::OwnedEvidence>) {
    if !is_effect_command(&run.command) {
        return (None, Vec::new());
    }
    let argv: Vec<String> = serde_json::from_str(&run.argv_json).unwrap_or_default();
    let git = argv.first().map(PathBuf::from);
    let location_root = run
        .binding_location_id
        .as_deref()
        .and_then(|id| crate::server::location_root_for_id(state, id));
    let succeeded = status == "succeeded";
    // Each arm yields (capability, operation, scope, digest,
    // write-time baseline observed JSON, test observed JSON).
    // Only one of the two observed values is ever set.
    let (capability, operation, scope, digest, baseline, test_observed): (
        &str,
        &str,
        String,
        String,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
    ) = match run.command.as_str() {
        "git-worktree-add" => {
            let absolute = argv.get(3).cloned().unwrap_or_else(|| run.cwd.clone());
            let branch = argv.get(5).cloned().unwrap_or_default();
            let scope = location_root
                .as_deref()
                .and_then(|root| relativize_to_root(root, &absolute))
                .unwrap_or(absolute.clone());
            let cwd = Path::new(&run.cwd);
            let parent_head = git
                .as_ref()
                .and_then(|g| {
                    git_read_sync(state, g, &["-C", &run.cwd, "rev-parse", "HEAD"], cwd)
                })
                .unwrap_or_default();
            let params = std::collections::BTreeMap::from([
                ("path".to_string(), scope.clone()),
                ("branch".to_string(), branch.clone()),
                ("parent_head_sha".to_string(), parent_head),
            ]);
            let digest =
                crate::operation_binding::operation_digest("git.worktree-add", None, &params);
            let baseline = if succeeded {
                git.as_ref()
                    .and_then(|g| {
                        git_read_sync(
                            state,
                            g,
                            &["-C", &absolute, "rev-parse", "HEAD"],
                            Path::new(&absolute),
                        )
                    })
                    .map(|head_sha| {
                        serde_json::json!({"kind": "worktree", "head_sha": head_sha})
                    })
            } else {
                None
            };
            (
                "git",
                "worktree-add",
                scope,
                digest,
                baseline,
                None,
            )
        }
        "git-commit" => {
            let scope = location_root
                .as_deref()
                .and_then(|root| relativize_to_root(root, &run.cwd))
                .unwrap_or_else(|| run.cwd.clone());
            let cwd = Path::new(&run.cwd);
            let message = argv.get(4).cloned().unwrap_or_default();
            let head = git
                .as_ref()
                .and_then(|g| git_read_sync(state, g, &["-C", &run.cwd, "rev-parse", "HEAD"], cwd))
                .unwrap_or_default();
            let parent = git
                .as_ref()
                .and_then(|g| {
                    git_read_sync(state, g, &["-C", &run.cwd, "rev-parse", "HEAD~1"], cwd)
                })
                .unwrap_or_default();
            let tree = git
                .as_ref()
                .and_then(|g| {
                    git_read_sync(
                        state,
                        g,
                        &["-C", &run.cwd, "rev-parse", "HEAD^{tree}"],
                        cwd,
                    )
                })
                .unwrap_or_default();
            let params = std::collections::BTreeMap::from([
                ("parent_head_sha".to_string(), parent.clone()),
                ("tree_sha256".to_string(), tree.clone()),
                (
                    "message_sha256".to_string(),
                    crate::sqlite_store::sha256_hex(message.as_bytes()),
                ),
            ]);
            let digest =
                crate::operation_binding::operation_digest("git.commit", None, &params);
            let baseline = if succeeded && !head.is_empty() {
                Some(serde_json::json!({
                    "kind": "commit",
                    "sha256": head,
                    "parent_sha256": parent,
                    "tree_sha256": tree,
                }))
            } else {
                None
            };
            ("git", "commit", scope, digest, baseline, None)
        }
        "cargo-test" => {
            let summary = parse_test_output(output);
            let duration_ms = (now_ms() - started_ms).max(0) as u64;
            let observed = serde_json::json!({
                "kind": "test-run",
                "passed": summary.passed,
                "failed": summary.failed,
                "duration_ms": duration_ms,
                "exit_code": exit_code,
                "failed_sha256": summary.failed_sha256,
            });
            (
                "test",
                "run",
                "cargo-test".to_string(),
                run_digest(run),
                None,
                Some(observed),
            )
        }
        _ => return (None, Vec::new()),
    };
    let effect = crate::server::record_effect(
        state,
        crate::effect_store::NewEffect {
            execution_id: run.execution_id.clone(),
            capability: capability.to_string(),
            operation: operation.to_string(),
            scope: scope.clone(),
            input_digest: Some(digest),
            approval_id: run.approval_id.clone(),
            status: status.to_string(),
        },
    );
    let mut evidence = Vec::new();
    if let Some(observed) = baseline {
        evidence.push(crate::evidence::OwnedEvidence {
            effect_id: effect.effect_id.clone(),
            execution_id: run.execution_id.clone(),
            source: "write".to_string(),
            verdict: "verified".to_string(),
            reason: "written".to_string(),
            observed_json: serde_json::to_string(&observed)
                .unwrap_or_else(|_| "{}".to_string()),
            checked_ms: now_ms(),
            origin_device_id: state.origin_device_id.clone(),
            schema_version: 1,
        });
    }
    if let Some(observed) = test_observed {
        let (verdict, reason) = if succeeded {
            ("verified", "test_passed")
        } else {
            ("mismatch", "test_failed")
        };
        evidence.push(crate::evidence::OwnedEvidence {
            effect_id: effect.effect_id.clone(),
            execution_id: run.execution_id.clone(),
            source: "test".to_string(),
            verdict: verdict.to_string(),
            reason: reason.to_string(),
            observed_json: serde_json::to_string(&observed)
                .unwrap_or_else(|_| "{}".to_string()),
            checked_ms: now_ms(),
            origin_device_id: state.origin_device_id.clone(),
            schema_version: 1,
        });
    }
    // §4.2: the Worktree record is created after the command
    // succeeds. A failure here only warns: the git worktree
    // exists, and the record can be registered via the API.
    if succeeded && run.command == "git-worktree-add" {
        let branch = argv.get(5).cloned().unwrap_or_default();
        if let Some(location_id) = &run.binding_location_id {
            if let Err(detail) = crate::server::create_worktree_record(
                state,
                location_id.clone(),
                scope.clone(),
                branch,
                None,
            ) {
                eprintln!("warning: P3-C worktree record creation failed: {detail}");
            }
        }
    }
    (Some(effect.effect_id), evidence)
}

/// Known terminal outcome (exit code): run row + (for
/// write/test templates) the completion Effect and Evidence
/// + plan_position checkpoint. Reads write no Effect.
///
/// The output argument is the full untruncated combined
/// output (the test parser reads it); the stored tail is
/// truncated inside to WORKER_TAIL_LIMIT.
async fn finish_terminal(
    state: &AppState,
    run: &WorkerRunRow,
    status: &str,
    exit_code: Option<i64>,
    output: String,
    started_ms: i64,
) {
    if !assignment_still_active(state, run).await {
        finish_superseded(state, run).await;
        return;
    }
    let Some(sqlite) = state.sqlite.clone() else {
        return;
    };
    let (tail, truncated) = truncate_tail(output.as_bytes());
    let (effect_id, evidence) =
        completion_effect(state, run, status, exit_code, &output, started_ms).await;
    let finished = sqlite_store::RunFinish {
        status: status.to_string(),
        exit_code,
        tail,
        truncated,
        effect_id,
    };
    let applied = {
        let mut conn = sqlite.lock().expect("sqlite lock");
        sqlite_store::finish_worker_run(&mut conn, &run.run_id, &finished)
            .ok()
            .unwrap_or(false)
    };
    if applied {
        for draft in &evidence {
            crate::server::record_evidence_best_effort(state, draft);
        }
        if let Some(execution_id) = &run.execution_id {
            let mut conn = sqlite.lock().expect("sqlite lock");
            let _ = sqlite_store::write_plan_position_checkpoint(
                &mut conn,
                execution_id,
                &state.origin_device_id,
            );
            // E2-A §2.3: the terminal run closes its Execution
            // (succeeded→completed, failed→failed). The close
            // shares the `applied` gate with the checkpoint:
            // a superseded run never owned the Execution.
            // P3-C §5.4: a test run's failure is a result, not
            // something to review — its close opens no failed
            // Inbox row (the (test,run) Effect already opens
            // none), while every other failed run does.
            let target = if status == "succeeded" { "completed" } else { "failed" };
            drop(conn);
            if run.command == "cargo-test" {
                crate::server::close_execution_no_inbox(state, execution_id, target);
            } else {
                crate::server::close_execution(state, execution_id, target);
            }
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
    let location_root = run
        .binding_location_id
        .as_deref()
        .and_then(|id| crate::server::location_root_for_id(state, id));
    let (capability, operation, scope) =
        unknown_effect_identity(run, location_root.as_deref());
    let effect = crate::server::record_effect(
        state,
        crate::effect_store::NewEffect {
            execution_id: run.execution_id.clone(),
            capability,
            operation,
            scope,
            input_digest: Some(run_digest(run)),
            approval_id: run.approval_id.clone(),
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
            // E2-A §2.3: timeout / wait-failure is not a
            // terminal outcome — the Execution goes to
            // `needs_review` (visible, recoverable), never to
            // a fabricated terminal word.
            drop(conn);
            crate::server::close_execution(state, execution_id, "needs_review");
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
    let location_root = run
        .binding_location_id
        .as_deref()
        .and_then(|id| crate::server::location_root_for_id(state, id));
    let (capability, operation, scope) =
        unknown_effect_identity(run, location_root.as_deref());
    let effect = crate::server::record_effect(
        state,
        crate::effect_store::NewEffect {
            execution_id: run.execution_id.clone(),
            capability,
            operation,
            scope,
            input_digest: Some(run_digest(run)),
            approval_id: run.approval_id.clone(),
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
                resolve_command(command, None, git, &cwd, None, None),
                Err(CommandError::NotAllowlisted),
                "{command}"
            );
        }
    }

    #[test]
    fn resolve_status_and_diff_argv() {
        let git = Path::new("/usr/bin/git");
        let cwd = std::env::temp_dir();
        let status = resolve_command("git-status", None, git, &cwd, None, None).expect("status");
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
            None,
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
            resolve_command("git-diff", None, git, &cwd, None, None).expect("diff no path");
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
                resolve_command("git-diff", Some(&bad), git, &cwd, None, None).is_err(),
                "{bad}"
            );
        }
        assert!(resolve_command(
            "git-diff",
            Some(&serde_json::json!({"path": 1})),
            git,
            &cwd,
            None,
            None,
        )
        .is_err());
        // Timeout bounds.
        assert!(resolve_command("git-status", None, git, &cwd, Some(0), None).is_err());
        assert!(resolve_command(
            "git-status",
            None,
            git,
            &cwd,
            Some(WORKER_MAX_TIMEOUT_MS + 1),
            None,
        )
        .is_err());
        // Status takes no params.
        assert!(resolve_command(
            "git-status",
            Some(&serde_json::json!({"path": "a.txt"})),
            git,
            &cwd,
            None,
            None,
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

    #[cfg(target_os = "macos")]
    #[test]
    fn exited_unreaped_child_is_not_alive() {
        // 故意延迟 wait，确认僵尸不会被恢复扫描误判为活进程。
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id() as i64;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while process_alive(pid) && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let alive_before_reap = process_alive(pid);
        child.wait().unwrap();
        assert!(!alive_before_reap, "exited child reported alive before wait");
        assert!(!process_alive(pid));
    }

    #[test]
    fn relative_scope_uses_api_separators_before_and_after_creation() {
        let root = std::env::temp_dir().join(format!(
            "mock-worker-relative-scope-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let target = root.join("scratch").join("w1");
        // 未落盘的目标与成功创建后的目标必须得到同一 scope/摘要输入。
        assert_eq!(
            relativize_to_root(&root, &target.to_string_lossy()).as_deref(),
            Some("scratch/w1")
        );
        std::fs::create_dir_all(&target).unwrap();
        assert_eq!(
            relativize_to_root(&root, &target.to_string_lossy()).as_deref(),
            Some("scratch/w1")
        );
        assert_eq!(
            relativize_to_root(&root, &root.to_string_lossy()).as_deref(),
            Some("")
        );
        assert!(relativize_to_root(&target, &root.to_string_lossy()).is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolve_worktree_add_commit_and_cargo_test_argv() {
        let git = Path::new("/usr/bin/git");
        let cargo = Path::new("/usr/bin/cargo");
        let cwd = std::env::temp_dir();
        let root = std::env::temp_dir();
        // worktree-add: absolute argv path, Location-relative
        // containment, 60s default.
        let add = resolve_command(
            "git-worktree-add",
            Some(&serde_json::json!({"path": "wt-1", "branch": "scratch-1"})),
            git,
            &cwd,
            None,
            Some(&root),
        )
        .expect("worktree add");
        assert_eq!(add.template, "git-worktree-add");
        assert_eq!(add.timeout_ms, 60_000);
        assert_eq!(add.argv[1], "worktree");
        assert_eq!(add.argv[2], "add");
        assert_eq!(add.argv[4], "-b");
        assert_eq!(add.argv[5], "scratch-1");
        assert!(Path::new(&add.argv[3]).is_absolute());
        // 命令路径与已校验目标必须相同，Windows 普通路径不带 verbatim 前缀。
        assert_eq!(
            Path::new(&add.argv[3])
                .parent()
                .unwrap()
                .canonicalize()
                .unwrap(),
            root.canonicalize().unwrap()
        );
        #[cfg(windows)]
        assert!(!add.argv[3].starts_with(r"\\?\"), "{}", add.argv[3]);
        // commit: message in argv, 60s default.
        let commit = resolve_command(
            "git-commit",
            Some(&serde_json::json!({"message": "wip"})),
            git,
            &cwd,
            None,
            None,
        )
        .expect("commit");
        assert_eq!(commit.template, "git-commit");
        assert_eq!(commit.timeout_ms, 60_000);
        assert_eq!(
            commit.argv,
            vec!["/usr/bin/git", "commit", "--no-verify", "-m", "wip"]
        );
        // cargo-test: -p package, 600s default, cargo binary.
        let test = resolve_command(
            "cargo-test",
            Some(&serde_json::json!({"package": "mock-gateway"})),
            cargo,
            &cwd,
            None,
            None,
        )
        .expect("cargo test");
        assert_eq!(test.template, "cargo-test");
        assert_eq!(test.timeout_ms, 600_000);
        assert_eq!(
            test.argv,
            vec!["/usr/bin/cargo", "test", "-p", "mock-gateway"]
        );
    }

    #[test]
    fn resolve_write_templates_reject_bad_params() {
        let git = Path::new("/usr/bin/git");
        let cargo = Path::new("/usr/bin/cargo");
        let cwd = std::env::temp_dir();
        let root = std::env::temp_dir();
        // worktree-add: missing/escaping path, bad branch, no
        // location root.
        for bad in [
            serde_json::json!({"path": "wt-1"}),
            serde_json::json!({"branch": "b"}),
            serde_json::json!({"path": "../wt", "branch": "b"}),
            serde_json::json!({"path": "wt", "branch": "a/b"}),
            serde_json::json!({"path": "wt", "branch": "a b"}),
            serde_json::json!({"path": "wt", "branch": ""}),
            serde_json::json!({"path": "wt", "branch": "b", "extra": 1}),
        ] {
            assert!(
                resolve_command("git-worktree-add", Some(&bad), git, &cwd, None, Some(&root))
                    .is_err(),
                "{bad}"
            );
        }
        assert!(resolve_command(
            "git-worktree-add",
            Some(&serde_json::json!({"path": "wt", "branch": "b"})),
            git,
            &cwd,
            None,
            None
        )
        .is_err());
        // commit: missing/empty/overlong message.
        assert!(resolve_command("git-commit", None, git, &cwd, None, None).is_err());
        assert!(resolve_command(
            "git-commit",
            Some(&serde_json::json!({"message": ""})),
            git,
            &cwd,
            None,
            None
        )
        .is_err());
        assert!(resolve_command(
            "git-commit",
            Some(&serde_json::json!({"message": "x".repeat(2001)})),
            git,
            &cwd,
            None,
            None
        )
        .is_err());
        // cargo-test: missing/bad package.
        assert!(resolve_command("cargo-test", None, cargo, &cwd, None, None).is_err());
        assert!(resolve_command(
            "cargo-test",
            Some(&serde_json::json!({"package": "a/b"})),
            cargo,
            &cwd,
            None,
            None
        )
        .is_err());
        assert!(resolve_command(
            "cargo-test",
            Some(&serde_json::json!({"package": ""})),
            cargo,
            &cwd,
            None,
            None
        )
        .is_err());
    }

    #[test]
    fn parse_test_output_sums_suites_and_hashes_failures() {
        let tail = "running 3 tests\ntest a::one ... ok\ntest a::two ... FAILED\ntest a::three ... ok\n\nfailures:\n    a::two\n\ntest result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n\nrunning 1 test\ntest b::one ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n";
        let summary = parse_test_output(tail);
        assert_eq!(summary.passed, Some(3));
        assert_eq!(summary.failed, Some(1));
        assert_eq!(summary.failed_sha256.len(), 1);
        assert_eq!(
            summary.failed_sha256[0],
            crate::sqlite_store::sha256_hex(b"a::two")
        );
        // No summary line: counts stay null, never fabricated.
        let empty = parse_test_output("compiling foo\nerror: could not compile\n");
        assert_eq!(empty.passed, None);
        assert_eq!(empty.failed, None);
        assert!(empty.failed_sha256.is_empty());
    }

    #[test]
    fn parse_test_output_reads_full_output_beyond_tail_limit() {
        // P3-C window finding: a real `cargo test` run's
        // stderr progress fills the stored tail, pushing the
        // stdout summary lines out. The finish path parses
        // the full combined output, so summaries followed by
        // more than WORKER_TAIL_LIMIT bytes of trailing
        // stderr noise must still parse (and sum across
        // suites). If this test ever reads only the tail, the
        // counts come back null.
        let mut output = String::from(
            "running 2 tests\ntest a::one ... ok\ntest a::two ... ok\n\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n\nrunning 1 test\ntest b::one ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s\n",
        );
        output.push_str(&"Running unittests src/lib.rs (target/debug/deps/testpkg-abc)\n".repeat(200));
        assert!(
            output.len() > WORKER_TAIL_LIMIT,
            "fixture must exceed the tail limit: {}",
            output.len()
        );
        // The stored tail itself contains no summary line:
        // that is exactly the window shape.
        let (tail, truncated) = truncate_tail(output.as_bytes());
        assert!(truncated);
        assert!(!tail.contains("test result:"));
        let summary = parse_test_output(&output);
        assert_eq!(summary.passed, Some(3));
        assert_eq!(summary.failed, Some(0));
        assert!(summary.failed_sha256.is_empty());
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
                // 及时回收直属子进程；进程可能在探测和发信号之间正常退出。
                let _ = child.try_wait();
                std::thread::sleep(Duration::from_millis(50));
            }
            if process_alive(pid) || process_alive(grandchild) {
                let _ = kill_process_group(pid, "KILL");
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
