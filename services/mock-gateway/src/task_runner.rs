//! Deterministic task runner (Round 3, 3b): the gateway runs a task
//! from `created` to the approval point without the window, and from
//! `interrupted` back to the approval point when the draft survived.
//!
//! The runner reuses the manual flow's contract exactly: the same
//! fixed prompt, the same per-file caps, the same bound-approval
//! digest (overwrite), the same atomic consume on execute. The
//! window only changes who drives the steps, never what is
//! approved: a task approval is a file-write authorization (path +
//! content digest + mode), not a task-state lock. Execute therefore
//! never approves its own approval; the window decides approved
//! first, then calls execute. Reject only removes power, so it
//! decides rejected itself.
//!
//! Draft bodies live in the task_drafts sidecar (memory map plus
//! `<task_id>.md` files when a drafts dir is configured), never in
//! tasks.jsonl: the record carries only the digest and byte count.
//! Drafts are kept after reject/fail/cancel, inspectable, never
//! silently deleted; tasks have no delete endpoint in this round.
//!
//! Cancel is checked between steps, never by aborting an in-flight
//! model call: the call finishes, its result is discarded, and no
//! approval is created. A cancel that lands after the final
//! draft_ready check cannot interleave, because that check and
//! the transition share one lock acquisition.

use std::path::PathBuf;

use agent_desktop::approval::ApprovalStatus;
use serde::Deserialize;

use crate::deepseek::DeepseekOutcome;
use crate::evidence::OwnedEvidence;
use crate::model_store::resolve_role_models;
use crate::operation_binding;
use crate::plan_store::{
    create_plan, current_plan_for_task, validate_plan_steps, validate_replan_if, PlanReplanIfDto,
    PlanRow, PlanStepDto,
};
use crate::server::{
    approval_status, close_execution_for_task, create_execution, create_worker_run_internal,
    decide_approval_rejected, ensure_root, execution_for_task, persist_approval, persist_task,
    record_evidence_best_effort, resolve_file, worker_command_digest, ApprovalState, AppState,
    WorkerRunCreateDto,
};
use crate::sqlite_store::{canonical_json, now_ms};
use crate::task_store::TaskFileRecord;
use crate::worker::{is_effect_command, is_terminal_run_status};

/// Frontend parity (TasksView): chunk size is irrelevant gateway
/// side (whole-file read), per-file cap is 262144 bytes.
const MAX_FILE_BYTES: usize = 262144;

/// Verbatim fixed prompt from the manual flow (TasksView).
/// pub(crate): P0-E snapshots digest the driving template, so
/// the server's snapshot builder reads this constant rather
/// than a copy that could drift from it.
pub(crate) const PROMPT_PREFIX: &str =
    "请把以下目录文本整理成一份结构清晰的报告（标题、要点、结论）：\n\n";

/// ORC-2 (J1): the Planner prompt (LOCAL DEFINITION).
/// The Planner answers one question — does this task need
/// files changed — and, when it does, produces the
/// blueprint as strict JSON. The model returns JSON only:
/// no markdown fences, no commentary. A reply that is
/// not strict JSON, or whose steps fail the shared
/// ORC-1 validation, is a Planner failure (the task
/// fails; the runner never guesses or repairs it).
/// `verify_cmd` is constrained to the worker-run
/// allowlist shapes (J7): anything else cannot run
/// and fails the step at prepare time.
pub(crate) const PLANNER_PROMPT_PREFIX: &str =
    "你是任务的规划者（Planner）。只做规划，不写文件、不执行命令。\n\n任务目录下的文件内容如下：\n\n";

/// ORC-2 (J1): the Planner's strict-JSON contract,
/// appended to the file context. `answer` is required
/// exactly when `needs_file_change` is false; `steps`
/// must be non-empty exactly when it is true.
const PLANNER_JSON_CONTRACT: &str = "\n\n请判断这个任务是否需要修改文件。只返回一个严格 JSON 对象，不要 markdown 围栏、不要解释文字：\n{\"needs_file_change\": true|false, \"answer\": \"无需改文件时直接回答任务问题的文字；需要改文件时可省略\", \"steps\": [{\"step_no\": 1, \"kind\": \"file_write|command|verify|review\", \"path\": \"相对路径\", \"symbol\": \"函数或符号名\", \"change\": \"改动描述\", \"verify_cmd\": \"验证命令\", \"expected\": \"预期输出\"}], \"replan_if\": [{\"signal\": \"触发信号描述\", \"action\": \"stop_and_hand_back\"}]}\n规则：\n- needs_file_change 为 false 时，steps 用空数组，answer 必须给出直接回答。\n- 需要改文件时，steps 至少 1 步，step_no 从 1 连续编号；每步四件套 path/symbol/change/verify_cmd/expected 都必填。\n- verify_cmd 只允许以下形式：cargo test -p <包名>、git status、git diff、git diff -- <路径>；不许写其他命令。\n- replan_if 可为空；有信号时 action 只能是 stop_and_hand_back。\n";

/// ORC-2: when the user forces execution, the Planner
/// is told to produce steps even if it judges no file
/// change — the force overrides the judgment, but the
/// Executor still needs a blueprint. A model that
/// still returns no steps fails the plan validation
/// (the task fails honestly; there is nothing to run).
/// Only the fresh-plan prompt carries this note: a
/// reused plan already has its steps.
const PLANNER_FORCE_SUFFIX: &str = "\n\n注意：用户已选择强制执行。即使你判断无需修改文件，也必须给出可执行的步骤：将 needs_file_change 设为 true 并填写 steps，不要只返回直接回答。";
const PLANNER_REUSE_SUFFIX: &str = "\n\n任务已有蓝图，不要重新规划；只判断这次运行是否仍需修改文件。只返回一个严格 JSON 对象，不要 markdown 围栏、不要解释文字：\n{\"needs_file_change\": true|false, \"answer\": \"无需改文件时直接回答任务问题的文字\"}\n已有蓝图步骤如下：\n";

/// ORC-2 (J2): the Executor's per-step file-content
/// prompt (LOCAL DEFINITION). The model returns the
/// step's finished file content verbatim — no fences,
/// no commentary. An empty reply is a step failure.
const EXECUTOR_PROMPT_PREFIX: &str =
    "你是任务的执行者（Executor）。只产出文件内容，不解释。\n\n";

/// ORC-2 (J1): the Planner's reply, parsed strictly.
/// Unknown fields are ignored; a missing or mistyped
/// `needs_file_change` fails the parse (the judgment
/// is the one field the runner cannot default).
#[derive(Debug, Deserialize)]
struct PlannerOutput {
    needs_file_change: bool,
    #[serde(default)]
    answer: Option<String>,
    #[serde(default)]
    steps: Vec<PlanStepDto>,
    #[serde(default)]
    replan_if: Vec<PlanReplanIfDto>,
}

/// ORC-2: resolve the Planner/Executor models for this
/// run, live from the staged record (J6).
fn role_models(state: &AppState) -> (String, String) {
    let staged = state
        .model_staged
        .lock()
        .expect("model staged lock")
        .clone()
        .unwrap_or_default();
    resolve_role_models(&staged, &state.deepseek.config().model)
}

/// E2-B §3.4 (snapshot gate), shared by the Planner and
/// Executor calls: the model call for this task's
/// Execution is denied by the rules frozen in the
/// Execution's snapshot at creation. No Execution yet
/// (the Planner round runs before one exists) → live
/// policy, exactly as the old draft flow did.
fn frozen_rules_for(
    state: &AppState,
    task_id: &str,
) -> Option<Vec<crate::policy_store::NetworkRule>> {
    execution_for_task(state, task_id)
        .and_then(|execution_id| crate::server::frozen_network_rules_for_execution(state, &execution_id))
}

/// ORC-2: one audit Evidence row for a Planner/Executor
/// decision (triage, step failure). Best-effort like
/// every other Evidence write: a failure warns, never
/// fails the step it records.
fn record_orc_evidence(
    state: &AppState,
    effect_id: &str,
    execution_id: Option<&str>,
    source: &str,
    verdict: &str,
    reason: &str,
    observed: serde_json::Value,
) {
    let draft = OwnedEvidence {
        effect_id: effect_id.to_string(),
        execution_id: execution_id.map(str::to_string),
        source: source.to_string(),
        verdict: verdict.to_string(),
        reason: reason.to_string(),
        observed_json: serde_json::to_string(&observed).unwrap_or_else(|_| "{}".to_string()),
        checked_ms: now_ms() as i64,
        origin_device_id: state.origin_device_id.clone(),
        schema_version: 1,
    };
    record_evidence_best_effort(state, &draft);
}

/// ORC-2: the run's Execution is created here — at the
/// point execution actually starts — not in the run /
/// resume handlers. A triage run never reaches this
/// call, so it never has an Execution (Q-ORC-07).
/// A fresh attempt supersedes any still-running
/// Execution a crashed earlier attempt left behind.
fn begin_execution(state: &AppState, task_id: &str) -> String {
    let execution = create_execution(state, Some(task_id.to_string()), None);
    crate::server::supersede_old_task_executions(state, task_id, &execution.execution_id);
    execution.execution_id
}

/// ORC-2 (J20): a run that fails in the Planner round
/// still gets an Execution — the E2-A §2.3 invariant
/// (every terminal task closes its Execution) holds for
/// failed attempts. The Execution is created here and
/// closed by `fail()` immediately after. Triage (a
/// successful no-change judgment) is the only run with
/// no Execution (Q-ORC-07).
fn fail_plan_round(state: &AppState, task_id: &str, error: &str) {
    begin_execution(state, task_id);
    fail(state, task_id, error);
}

/// ORC-2 (J20): the Planner-round failure path when
/// the task may already have been cancelled while the
/// model call was in flight. A cancelled run is a
/// cancellation, not a failure: create the Execution
/// shell and close it as cancelled at once, instead
/// of creating a stranded running Execution that
/// `fail()` would then refuse to close.
fn fail_or_cancel_plan_round(state: &AppState, task_id: &str, error: &str) {
    let status = state
        .tasks
        .lock()
        .expect("task lock")
        .get(task_id)
        .map(|r| r.status.clone());
    if status.as_deref() == Some("cancelled") {
        begin_execution(state, task_id);
        crate::server::close_execution_for_task(state, task_id, "cancelled");
        return;
    }
    fail_plan_round(state, task_id, error);
}

/// ORC-2: fail the task for a step failure (Q-ORC-11)
/// and leave the audit row. The step never retries in
/// this batch, the batch never skips ahead, no Inbox
/// row is opened and nothing is re-planned (ORC-3).
/// Ten arguments is deliberate: the failure record
/// carries the step identity plus the worker run's
/// outcome, and no caller bundles them.
#[allow(clippy::too_many_arguments)]
fn fail_step(
    state: &AppState,
    task_id: &str,
    plan: &PlanRow,
    step_no: i64,
    step_kind: &str,
    failure: &str,
    execution_id: Option<&str>,
    run_id: Option<&str>,
    run_status: Option<&str>,
    exit_code: Option<i64>,
) {
    record_orc_evidence(
        state,
        &format!("executor:{task_id}:{step_no}"),
        execution_id,
        "executor",
        "verified",
        "step_failed",
        serde_json::json!({
            "task_id": task_id,
            "plan_id": plan.plan_id,
            "step_no": step_no,
            "step_kind": step_kind,
            "failure": failure,
            "run_id": run_id,
            "run_status": run_status,
            "exit_code": exit_code,
        }),
    );
    fail(state, task_id, &format!("步骤 {step_no} 失败：{failure}"));
}

/// ORC-2: create a bound `worker.run` approval for one
/// Executor step, mirroring `create_bound_write_approval`:
/// the digest is server-computed from the resolved
/// command (argv + cwd + timeout) via
/// `worker_command_digest`, never from a client hash.
/// The approval is decided by the user in the window;
/// the run creation consumes it (the `worker.run`
/// precedent). Reads need no approval and never call
/// this.
fn create_bound_worker_approval(
    state: &AppState,
    worktree_id: &str,
    command: &str,
    params: &serde_json::Value,
) -> Result<String, String> {
    let mut flat = std::collections::BTreeMap::new();
    flat.insert("worktree_id".to_string(), worktree_id.to_string());
    flat.insert("command".to_string(), command.to_string());
    if let Some(map) = params.as_object() {
        for (key, value) in map {
            if let serde_json::Value::String(s) = value {
                flat.insert(key.clone(), s.clone());
            }
        }
    }
    let (digest, _spec) = worker_command_digest(state, &flat)
        .map_err(|e| format!("worker digest failed: {}", e.code))?;
    let mut next = state.next_approval_id.lock().expect("approval id lock");
    let id = format!("approval-{}", *next);
    *next += 1;
    let record = ApprovalState::new(
        "worker.run".to_string(),
        ApprovalStatus::Pending,
        None,
        false,
        Some(digest),
    );
    if let Err(detail) = persist_approval(state, &id, &record) {
        return Err(format!("approval_audit_failed: {detail}"));
    }
    state.approvals.lock().expect("approval lock").insert(id.clone(), record);
    Ok(id)
}

/// ORC-2 (J7): map a step's `verify_cmd` to a worker-run
/// template, strictly. Worker runs only accept the
/// allowlisted templates (`git-status`, `git-diff`,
/// `git-worktree-add`, `git-commit`, `cargo-test`);
/// an arbitrary shell string can never run through
/// them, and the Executor never invents a shell of its
/// own — an unmappable command is a step failure, not
/// a guess. Only the read/test shapes a plan step can
/// honestly name are mapped here: `git-worktree-add`
/// and `git-commit` are writes the Planner must not
/// emit as a verify step, so they are deliberately
/// absent.
fn resolve_verify_command(
    verify_cmd: &str,
) -> Result<(String, serde_json::Value), String> {
    let cmd = verify_cmd.trim();
    if cmd == "git status" {
        return Ok(("git-status".to_string(), serde_json::json!({})));
    }
    if cmd == "git diff" {
        return Ok(("git-diff".to_string(), serde_json::json!({})));
    }
    if let Some(path) = cmd.strip_prefix("git diff -- ") {
        let path = path.trim();
        if path.is_empty() || path.contains(char::is_whitespace) {
            return Err(format!("verify command not allowlisted: {cmd}"));
        }
        return Ok(("git-diff".to_string(), serde_json::json!({ "path": path })));
    }
    if let Some(package) = cmd.strip_prefix("cargo test -p ") {
        let package = package.trim();
        if package.is_empty() || package.contains(char::is_whitespace) {
            return Err(format!("verify command not allowlisted: {cmd}"));
        }
        return Ok(("cargo-test".to_string(), serde_json::json!({ "package": package })));
    }
    Err(format!("verify command not allowlisted: {cmd}"))
}

fn draft_file_name(task_id: &str) -> String {
    format!("{task_id}.md")
}

/// Draft lookup: the memory map is authoritative for the run; the
/// sidecar files only reload it after a restart.
pub(crate) fn load_draft(state: &AppState, task_id: &str) -> Option<String> {
    state.task_drafts.lock().expect("draft lock").get(task_id).cloned()
}

/// Store a draft in memory and, when a drafts dir is configured,
/// mirror it to `<task_id>.md`. A file failure is a stderr warning
/// only, like every other store here; memory stays authoritative.
pub(crate) fn persist_draft(state: &AppState, task_id: &str, draft: &str) {
    state.task_drafts.lock().expect("draft lock").insert(task_id.to_string(), draft.to_string());
    if let Some(dir) = &state.task_drafts_dir {
        if std::fs::create_dir_all(dir).is_err() {
            eprintln!("warning: cannot create task drafts dir {}", dir.display());
            return;
        }
        let path = dir.join(draft_file_name(task_id));
        if let Err(e) = std::fs::write(&path, draft) {
            eprintln!("warning: cannot write task draft to {}: {e}", path.display());
        }
    }
}

/// Load `<task_id>.md` sidecars into memory. Malformed file names
/// and unreadable files are skipped with a warning.
pub(crate) fn load_draft_dir(dir: &std::path::Path) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(task_id) = name.strip_suffix(".md") else {
            continue;
        };
        match std::fs::read_to_string(entry.path()) {
            Ok(draft) => {
                out.insert(task_id.to_string(), draft);
            }
            Err(e) => {
                eprintln!("warning: cannot read task draft {}: {e}", entry.path().display());
            }
        }
    }
    out
}

fn set_status(state: &AppState, task_id: &str, status: &str, error: Option<String>) {
    let mut tasks = state.tasks.lock().expect("task lock");
    if let Some(record) = tasks.get_mut(task_id) {
        record.status = status.to_string();
        record.error = error;
        let updated = record.clone();
        persist_task(state, &updated);
    }
}

pub(crate) fn fail(state: &AppState, task_id: &str, error: &str) {
    // E2-A §2.2: `failed` is only reachable from running /
    // interrupted / draft_ready (the fail call sites). A fail
    // raced past a terminal transition (cancelled / completed /
    // rejected) is dropped, not forced: terminal is terminal.
    let from = state
        .tasks
        .lock()
        .expect("task lock")
        .get(task_id)
        .map(|r| r.status.clone());
    match from.as_deref() {
        Some(from) if crate::task_store::valid_task_status_transition(from, "failed") => {}
        _ => {
            eprintln!(
                "warning: refusing to fail task {} from {}",
                task_id,
                from.as_deref().unwrap_or("?")
            );
            return;
        }
    }
    set_status(state, task_id, "failed", Some(error.to_string()));
    // E2-A §2.3: the terminal Task closes its Execution.
    crate::server::close_execution_for_task(state, task_id, "failed");
}

/// Create a bound fs.write approval for a draft, mirroring the
/// approval_request handler: the digest is server-computed from
/// path + bytes + overwrite mode, and a persist failure fails
/// closed with no id issued.
fn create_bound_write_approval(
    state: &AppState,
    path: &str,
    data: &[u8],
) -> Result<String, String> {
    let Some(digest) = operation_binding::write_digest(path, data, false) else {
        return Err("产物路径无效".to_string());
    };
    let mut next = state.next_approval_id.lock().expect("approval id lock");
    let id = format!("approval-{}", *next);
    *next += 1;
    let record = ApprovalState::new(
        "fs.write".to_string(),
        ApprovalStatus::Pending,
        None,
        false,
        Some(digest),
    );
    if let Err(detail) = persist_approval(state, &id, &record) {
        return Err(format!("approval_audit_failed: {detail}"));
    }
    state.approvals.lock().expect("approval lock").insert(id.clone(), record);
    Ok(id)
}

/// Decide an approval rejected, failing closed like
/// approval_decide: a persist failure restores Pending and the
/// caller sees the error, never a silent rejection. The field
/// access lives in server.rs because ApprovalState fields are
/// private to that module.
pub(crate) fn decide_rejected(state: &AppState, approval_id: &str) -> Result<(), String> {
    decide_approval_rejected(state, approval_id)
}

/// Default artifact path, same rule as the manual flow: root tasks
/// write `report.md`, directory tasks write `{directory}/report.md`.
fn default_artifact_path(directory: &str) -> String {
    if directory.is_empty() {
        "report.md".to_string()
    } else {
        format!("{directory}/report.md")
    }
}

/// Entry point for the run/resume handlers. Handlers validate the
/// entry status; the runner re-checks under the lock so a second
/// spawn can never double-generate.
///
/// ORC-2: `force_execute` is the user's override of the
/// Planner's triage judgment (`tasks_run` +
/// `force_execute: true`, Q-ORC-07); `advance_to` is
/// Some(step_no) only when `tasks_execute` just applied
/// a step and the runner must continue with the next
/// one — the Planner round is skipped on that path (the
/// triage judgment was already made at the start of
/// the run). A fresh run always goes through the
/// Planner round, even for a task that already has a
/// plan (the plan is reused, not re-planned; J5).
pub(crate) async fn run_task(
    state: AppState,
    task_id: String,
    force_execute: bool,
    advance_to: Option<i64>,
) {
    let record = state.tasks.lock().expect("task lock").get(&task_id).cloned();
    let Some(record) = record else {
        return;
    };
    match record.status.as_str() {
        "created" => {
            // The run handler already claimed created -> running;
            // claim here too so a direct spawn is still safe.
            // The guard lives in its own block: a guard whose
            // scope spans the await below would make this future
            // non-Send, even after an explicit drop, because its
            // Drop must run past the await.
            {
                let mut tasks = state.tasks.lock().expect("task lock");
                if let Some(current) = tasks.get_mut(&task_id) {
                    if current.status == "created" {
                        current.status = "running".to_string();
                        current.error = None;
                        let updated = current.clone();
                        persist_task(&state, &updated);
                    }
                }
            }
            dispatch_body(state, record, force_execute, advance_to).await;
        }
        "running" => {
            // Claimed by the run handler; drive straight on.
            dispatch_body(state, record, force_execute, advance_to).await;
        }
        "completed" | "failed" => {
            // E2-A fix F-3 (Q1a): a terminal task re-run (run or
            // resume) claims `running` first, exactly like
            // `created`, then regenerates the draft and a fresh
            // approval. Without this arm the resume handler
            // would accept the completed/failed→running edge and
            // spawn a runner that silently does nothing, leaving
            // a stranded `running` Execution behind.
            {
                let mut tasks = state.tasks.lock().expect("task lock");
                if let Some(current) = tasks.get_mut(&task_id) {
                    if current.status == "completed" || current.status == "failed" {
                        current.status = "running".to_string();
                        current.error = None;
                        let updated = current.clone();
                        persist_task(&state, &updated);
                    }
                }
            }
            dispatch_body(state, record, force_execute, advance_to).await;
        }
        "interrupted" => {
            restore_or_regenerate(state, record).await;
        }
        "draft_ready" if advance_to.is_none() => {
            // ORC-2: the Executor's worker step, spawned by
            // `tasks_execute` after the user approved the
            // step. File steps never reach this arm —
            // `tasks_execute` applies them itself. A compat
            // task (no `executor_step_no`) is left alone:
            // its draft is applied by `tasks_execute`, never
            // by a re-spawned runner.
            execute_current_worker_step(state, record).await;
        }
        _ => {}
    }
}

/// ORC-2: a run that continues mid-Executor (the
/// previous step was just applied) skips the Planner
/// round and prepares the next step directly; every
/// other entry goes through the Planner round.
async fn dispatch_body(
    state: AppState,
    record: TaskFileRecord,
    force_execute: bool,
    advance_to: Option<i64>,
) {
    match advance_to {
        Some(step_no) => {
            continue_executor(state, record, step_no).await;
        }
        None => {
            plan_or_generate(state, record, force_execute).await;
        }
    }
}

/// ORC-2: continue the Executor at the given step
/// (called only from the `tasks_execute` advance).
/// The plan is re-read from SQLite; a missing plan
/// or step fails the task — the runner never guesses
/// which step comes next.
async fn continue_executor(state: AppState, record: TaskFileRecord, step_no: i64) {
    let task_id = record.task_id.clone();
    let Some(plan) = load_current_plan(&state, &task_id) else {
        fail(&state, &task_id, "蓝图缺失，无法继续执行");
        return;
    };
    prepare_executor_step(state, &task_id, &plan, step_no).await;
}

/// ORC-2: load the task's current plan from SQLite.
/// `None` when SQLite is unavailable or the task has
/// no plan row (the caller decides whether that is a
/// failure or a fresh-plan case).
pub(crate) fn load_current_plan(state: &AppState, task_id: &str) -> Option<PlanRow> {
    let sqlite = state.sqlite.as_ref()?;
    if state.sqlite_is_broken() {
        return None;
    }
    let conn = sqlite.lock().ok()?;
    current_plan_for_task(&conn, task_id).ok().flatten()
}

/// ORC-2 (J11): the `tasks_execute` gate for a worker
/// step — the step's approval (when one exists) must
/// already be Approved. A pending approval is not a
/// step failure; the user may still approve it. A
/// read step carries no approval and is always ready.
pub(crate) fn step_approval_ready(state: &AppState, approval_id: Option<&str>) -> bool {
    match approval_id {
        None => true,
        Some(id) => approval_status(state, id) == Some(ApprovalStatus::Approved),
    }
}

/// Interrupted tasks: a surviving draft returns straight to the
/// approval point (its approval is fully persisted, unlike a
/// loop's lost in-memory state); a rejected approval restores
/// the task to rejected, never to draft_ready; no draft means
/// regenerate from scratch.
async fn restore_or_regenerate(state: AppState, record: TaskFileRecord) {
    let Some(draft) = load_draft(&state, &record.task_id) else {
        // Same block-scope rule as run_task: the guard must not
        // span the generate await below.
        {
            let mut tasks = state.tasks.lock().expect("task lock");
            if let Some(current) = tasks.get_mut(&record.task_id) {
                if current.status != "interrupted" {
                    return;
                }
                current.status = "running".to_string();
                current.error = None;
                let updated = current.clone();
                persist_task(&state, &updated);
            }
        }
        plan_or_generate(state, record, false).await;
        return;
    };
    let artifact_path =
        record.artifact_path.clone().unwrap_or_else(|| default_artifact_path(&record.directory));
    let approval_id = match &record.approval_id {
        Some(id) => id.clone(),
        None => {
            // Crash window: the draft sidecar landed but the
            // draft_ready transition did not. Create the approval
            // now from the stored draft; it binds exactly this
            // draft, so no user decision is skipped.
            match create_bound_write_approval(&state, &artifact_path, draft.as_bytes()) {
                Ok(id) => id,
                Err(error) => {
                    fail(&state, &record.task_id, &error);
                    return;
                }
            }
        }
    };
    let mut tasks = state.tasks.lock().expect("task lock");
    let Some(current) = tasks.get_mut(&record.task_id) else {
        return;
    };
    if current.status != "interrupted" {
        return;
    }
    let approval_status = approval_status(&state, &approval_id);
    if approval_status == Some(ApprovalStatus::Rejected) {
        current.status = "rejected".to_string();
    } else {
        current.status = "draft_ready".to_string();
        current.artifact_path = Some(artifact_path);
        current.approval_id = Some(approval_id);
        current.draft_sha256 = Some(operation_binding::sha256_hex(draft.as_bytes()));
        current.draft_bytes = Some(draft.len() as u64);
    }
    let updated = current.clone();
    persist_task(&state, &updated);
}

/// ORC-2: read the task directory's files — the
/// listing rule and per-file cap of the old draft
/// flow, kept verbatim — and render them as prompt
/// context. Errors keep the old draft flow's messages.
fn read_task_files(state: &AppState, directory: &str) -> Result<Vec<String>, String> {
    if ensure_root(&state.files_root).is_err() {
        return Err("无法创建文件根".to_string());
    }
    let Ok(dir) = resolve_file(&state.files_root, directory) else {
        return Err("任务目录不在文件根内".to_string());
    };
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Err("任务目录不存在或不可读".to_string());
    };
    // Same listing rule as files_list: metadata follows symlinks,
    // directories are skipped, results sort by root-relative path.
    let mut files: Vec<(String, PathBuf)> = Vec::new();
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let path = if directory.is_empty() { name } else { format!("{directory}/{name}") };
        files.push((path, entry.path()));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    if files.is_empty() {
        return Err("目录下没有可读文件".to_string());
    }
    let mut parts: Vec<String> = Vec::new();
    for (path, full) in &files {
        let Ok(bytes) = std::fs::read(full) else {
            return Err(format!("生成失败：无法读取文件 {path}"));
        };
        // Per-file cap, cut at a UTF-8 boundary (the frontend's
        // TextDecoder would show a replacement char for a split
        // tail; dropping the tail is the same content, cleaner).
        let mut capped = &bytes[..bytes.len().min(MAX_FILE_BYTES)];
        while !capped.is_empty() && std::str::from_utf8(capped).is_err() {
            capped = &capped[..capped.len() - 1];
        }
        let text = String::from_utf8_lossy(capped);
        parts.push(format!("## {path}\n{text}"));
    }
    Ok(parts)
}

/// ORC-2 (J1): parse the Planner's reply as strict
/// JSON. `serde_json` accepts trailing whitespace and
/// nothing else — a fenced or commentary-wrapped reply
/// fails here, and the task fails with it.
fn parse_planner_output(text: &str) -> Result<PlannerOutput, String> {
    serde_json::from_str::<PlannerOutput>(text.trim()).map_err(|e| e.to_string())
}

/// ORC-2 (J15): plans live in SQLite (ORC-1's routes
/// are SQLite-only too). Return the shared connection
/// when SQLite mode is active, else None.
fn sqlite_for_plans(
    state: &AppState,
) -> Option<std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>> {
    if state.sqlite_is_broken() {
        return None;
    }
    let sqlite = state.sqlite.as_ref()?;
    if state.sqlite_mode() != crate::sqlite_store::StateBackend::Sqlite {
        return None;
    }
    Some(sqlite.clone())
}

/// ORC-2 (J15 revised): JSONL mode keeps the old
/// single-file draft flow. Plans live in SQLite and a
/// Planner round cannot write a plans row without it,
/// so when the state is not in SQLite mode the run
/// falls back to the pre-ORC-2 draft flow (model text
/// → draft → bound write approval → draft_ready).
/// Production always runs in SQLite mode, so option A
/// (every run plans first) holds there; the fallback
/// covers only the MOCK_STATE_BACKEND=jsonl emergency
/// override and the JSONL-mode tests that pin it.
async fn generate_body(state: AppState, record: TaskFileRecord) {
    let task_id = record.task_id.clone();
    let directory = record.directory.clone();
    let parts = match read_task_files(&state, &directory) {
        Ok(parts) => parts,
        Err(error) => {
            fail(&state, &task_id, &error);
            return;
        }
    };
    let prompt = format!("{PROMPT_PREFIX}{}", parts.join("\n\n"));
    // E2-B §3.4 (snapshot gate): the model call for this
    // task's Execution is denied by the rules frozen in the
    // Execution's snapshot at creation, not by live policy —
    // a mid-flight policy change must not change an in-flight
    // execution's permissions. No frozen rules resolve (no
    // Execution yet, JSONL mode, or an unretained version) →
    // live policy, exactly as before.
    let frozen_rules = crate::server::execution_for_task(&state, &task_id)
        .and_then(|execution_id| {
            crate::server::frozen_network_rules_for_execution(&state, &execution_id)
        });
    let draft = match state.deepseek.reply_with_rules(frozen_rules, &prompt).await {
        DeepseekOutcome::Disabled => {
            fail(&state, &task_id, "模型未配置");
            return;
        }
        DeepseekOutcome::Denied(pattern) => {
            fail(&state, &task_id, &format!("网络规则拒绝模型调用：{pattern}"));
            return;
        }
        DeepseekOutcome::Failed(_) => {
            fail(&state, &task_id, "模型调用失败");
            return;
        }
        DeepseekOutcome::Replied(text) => {
            if text.trim().is_empty() {
                fail(&state, &task_id, "模型没有返回报告草稿");
                return;
            }
            text
        }
    };
    // Cancel checkpoint: a cancelled run discards the draft.
    {
        let tasks = state.tasks.lock().expect("task lock");
        if tasks.get(&task_id).map(|r| r.status.as_str()) != Some("running") {
            return;
        }
    }
    let artifact_path = default_artifact_path(&directory);
    persist_draft(&state, &task_id, &draft);
    let approval_id = match create_bound_write_approval(&state, &artifact_path, draft.as_bytes()) {
        Ok(id) => id,
        Err(error) => {
            fail(&state, &task_id, &error);
            return;
        }
    };
    // Final transition shares one lock acquisition with its
    // running check, so a cancel cannot slip between them.
    let mut tasks = state.tasks.lock().expect("task lock");
    if let Some(current) = tasks.get_mut(&task_id) {
        if current.status != "running" {
            return;
        }
        current.status = "draft_ready".to_string();
        current.error = None;
        current.artifact_path = Some(artifact_path);
        current.approval_id = Some(approval_id);
        current.draft_sha256 = Some(operation_binding::sha256_hex(draft.as_bytes()));
        current.draft_bytes = Some(draft.len() as u64);
        let updated = current.clone();
        persist_task(&state, &updated);
    }
}

/// ORC-2: the Planner round replaces the old
/// single-file draft flow. Every `tasks_run` starts
/// here (option A); `draft_ready` compat tasks never
/// reach it — their draft is applied by
/// `tasks_execute` directly (Q-ORC-02).
///
/// The round:
///   1. reads the task files (same listing/caps as
///      the old draft flow);
///   2. JSONL mode (no plans table) falls back to the
///      old single-file draft flow (`generate_body`,
///      J15 revised) — the Planner round needs
///      SQLite, and production always runs there;
///   3. reuses an existing plan when the task has a
///      `plan_id` (the Planner still runs for the
///      triage judgment; J5);
///   4. triages (J4/J9/J13/J14): no file change and
///      no force → answer as the draft, Evidence
///      `no_file_change`, `running → draft_ready →
///      completed` in this run, no Execution, no
///      plan row, no write approval;
///   5. otherwise creates the Execution (only now —
///      a triage run never has one) and prepares
///      step 1. A run that FAILS in the Planner round
///      also gets an Execution (failed, J20) — the
///      E2-A §2.3 invariant holds for failed attempts;
///      triage is the only run with no Execution.
///
/// Cancel is observed at the lock point after the
/// model call: a cancelled run discards the plan but
/// still gets an Execution (cancelled, J20).
async fn plan_or_generate(state: AppState, record: TaskFileRecord, force_execute: bool) {
    let task_id = record.task_id.clone();
    let parts = match read_task_files(&state, &record.directory) {
        Ok(parts) => parts,
        Err(error) => {
            // J20: a failed attempt still gets an
            // Execution (failed) — see fail_plan_round.
            fail_or_cancel_plan_round(&state, &task_id, &error);
            return;
        }
    };
    let file_context = parts.join("\n\n");
    let sqlite = match sqlite_for_plans(&state) {
        Some(sqlite) => sqlite,
        None => {
            // J15 revised: JSONL mode cannot host a
            // Planner round (no plans table) — fall
            // back to the old single-file draft flow
            // instead of failing the run.
            generate_body(state, record).await;
            return;
        }
    };
    let (planner_model, _executor_model) = role_models(&state);
    let frozen_rules = frozen_rules_for(&state, &task_id);
    let existing_plan: Option<PlanRow> = record.plan_id.as_deref().and_then(|plan_id| {
        let conn = sqlite.lock().ok()?;
        crate::plan_store::get_plan(&conn, plan_id).ok().flatten()
    });
    let prompt = match &existing_plan {
        Some(plan) => format!(
            "{PLANNER_PROMPT_PREFIX}{file_context}{PLANNER_REUSE_SUFFIX}{}",
            plan.steps_json
        ),
        None => {
            let mut prompt =
                format!("{PLANNER_PROMPT_PREFIX}{file_context}{PLANNER_JSON_CONTRACT}");
            if force_execute {
                prompt.push_str(PLANNER_FORCE_SUFFIX);
            }
            prompt
        }
    };
    let reply = match state
        .deepseek
        .reply_with_rules_and_model(frozen_rules, &planner_model, &prompt)
        .await
    {
        DeepseekOutcome::Disabled => {
            fail_or_cancel_plan_round(&state, &task_id, "模型未配置");
            return;
        }
        DeepseekOutcome::Denied(pattern) => {
            fail_or_cancel_plan_round(
                &state,
                &task_id,
                &format!("网络规则拒绝模型调用：{pattern}"),
            );
            return;
        }
        DeepseekOutcome::Failed(_) => {
            fail_or_cancel_plan_round(&state, &task_id, "模型调用失败");
            return;
        }
        DeepseekOutcome::Replied(text) => text,
    };
    // Cancel checkpoint: a cancelled run discards the
    // reply BEFORE parsing — parsing a stale reply must
    // not create a failed Execution for a cancelled
    // task. The run still gets an Execution (J20) —
    // the cancel route already ran before the runner
    // observed the cancel, so the runner creates the
    // shell here and closes it as cancelled at once.
    {
        let tasks = state.tasks.lock().expect("task lock");
        let status = tasks.get(&task_id).map(|r| r.status.clone());
        if status.as_deref() != Some("running") {
            drop(tasks);
            if status.as_deref() == Some("cancelled") {
                begin_execution(&state, &task_id);
                crate::server::close_execution_for_task(&state, &task_id, "cancelled");
            }
            return;
        }
    }
    let output = match parse_planner_output(&reply) {
        Ok(output) => output,
        Err(detail) => {
            fail_or_cancel_plan_round(&state, &task_id, &format!("规划回合输出无效：{detail}"));
            return;
        }
    };
    if !output.needs_file_change && !force_execute {
        triage_answer(state, record, output, planner_model, existing_plan).await;
        return;
    }
    // Execution proceeds: resolve the plan (reuse, or
    // write the new one — validation first, so a bad
    // blueprint never reaches the plans table).
    let plan = match existing_plan {
        Some(plan) => plan,
        None => {
            let steps = match validate_plan_steps(&output.steps) {
                Ok(steps) => steps,
                Err(detail) => {
                    fail_or_cancel_plan_round(&state, &task_id, &format!("规划回合蓝图无效：{detail}"));
                    return;
                }
            };
            let replan_if = match validate_replan_if(&output.replan_if) {
                Ok(replan_if) => replan_if,
                Err(detail) => {
                    fail_or_cancel_plan_round(&state, &task_id, &format!("规划回合蓝图无效：{detail}"));
                    return;
                }
            };
            let steps_value = serde_json::to_value(&steps).unwrap_or(serde_json::Value::Null);
            let replan_value =
                serde_json::to_value(&replan_if).unwrap_or(serde_json::Value::Null);
            let digest = operation_binding::plan_digest(
                &task_id,
                &canonical_json(&steps_value),
                &canonical_json(&replan_value),
            );
            let row = {
                let mut conn = sqlite.lock().expect("sqlite lock");
                match create_plan(
                    &mut conn,
                    &task_id,
                    &steps,
                    &replan_if,
                    &planner_model,
                    &digest,
                    &state.origin_device_id,
                ) {
                    Ok(row) => row,
                    Err(detail) => {
                        fail_or_cancel_plan_round(&state, &task_id, &format!("蓝图写入失败：{detail}"));
                        return;
                    }
                }
            };
            // Point the task at its new plan.
            {
                let mut tasks = state.tasks.lock().expect("task lock");
                if let Some(current) = tasks.get_mut(&task_id) {
                    current.plan_id = Some(row.plan_id.clone());
                    let updated = current.clone();
                    persist_task(&state, &updated);
                }
            }
            row
        }
    };
    let _execution_id = begin_execution(&state, &task_id);
    prepare_executor_step(state, &task_id, &plan, 1).await;
}

/// ORC-2 (J4/J9/J13/J14): the fast-triage terminal
/// path. The Planner judged "no file change", so the
/// answer IS the deliverable: it goes into the task
/// draft (no write approval — there is nothing to
/// write), one Evidence row records the judgment
/// (`no_file_change`, answer hash + length only, never
/// the answer text), and the task walks the two legal
/// edges `running → draft_ready → completed` inside
/// this run. No plan row is created, no Execution
/// exists (a triage run never reaches
/// `begin_execution`), and a forced run never reaches
/// this function at all.
async fn triage_answer(
    state: AppState,
    record: TaskFileRecord,
    output: PlannerOutput,
    planner_model: String,
    existing_plan: Option<PlanRow>,
) {
    let task_id = record.task_id.clone();
    let answer = match output.answer.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(answer) => answer.to_string(),
        None => {
            fail_or_cancel_plan_round(&state, &task_id, "规划回合分流回答缺失");
            return;
        }
    };
    // J8: a missing or blank answer is a Planner
    // failure, not a silent completion — the caller
    // already returned on that case.
    record_orc_evidence(
        &state,
        &format!("triage:{task_id}"),
        None,
        "planner",
        "verified",
        "no_file_change",
        serde_json::json!({
            "task_id": task_id,
            "plan_id": existing_plan.as_ref().map(|p| p.plan_id.clone()),
            "planner_model": planner_model,
            "answer_len": answer.len(),
            "answer_sha256": operation_binding::sha256_hex(answer.as_bytes()),
        }),
    );
    persist_draft(&state, &task_id, &answer);
    // The two transitions share no lock acquisition: the
    // first guard MUST be dropped before the second lock
    // (std Mutex is not reentrant — a second lock in the
    // same scope deadlocks the runner and every tasks
    // reader with it).
    {
        let mut tasks = state.tasks.lock().expect("task lock");
        if let Some(current) = tasks.get_mut(&task_id) {
            if current.status != "running" {
                return;
            }
            current.status = "draft_ready".to_string();
            current.error = None;
            current.artifact_path = None;
            current.approval_id = None;
            current.draft_sha256 = Some(operation_binding::sha256_hex(answer.as_bytes()));
            current.draft_bytes = Some(answer.len() as u64);
            let updated = current.clone();
            persist_task(&state, &updated);
        }
    }
    // draft_ready → completed, in the same run. The
    // answer needs no approval (nothing is written),
    // so there is no user step between the two edges.
    {
        let mut tasks = state.tasks.lock().expect("task lock");
        if let Some(current) = tasks.get_mut(&task_id) {
            if current.status != "draft_ready" {
                return;
            }
            current.status = "completed".to_string();
            let updated = current.clone();
            persist_task(&state, &updated);
        }
    }
    close_execution_for_task(&state, &task_id, "completed");
}

/// ORC-2: the Executor's prepare checkpoints accept
/// a task that is `running` (a fresh step) or
/// `draft_ready` (the previous worker step just
/// executed — the task stays `draft_ready` across
/// worker steps, because `draft_ready → running` is
/// NOT a legal edge and no new edge may be added).
/// Any other status (cancelled, rejected, failed,
/// completed) means the run is over: prepare returns
/// without doing anything.
fn task_is_active(state: &AppState, task_id: &str) -> bool {
    matches!(
        state
            .tasks
            .lock()
            .expect("task lock")
            .get(task_id)
            .map(|r| r.status.as_str()),
        Some("running") | Some("draft_ready")
    )
}
///
/// - `file_write`: the Executor model produces the
///   step's file content (J2), then the runner creates
///   the bound fs.write approval. The draft is the
///   step's content; `artifact_path` is the step path.
/// - `command` / `verify` / `review`: the step's
///   `verify_cmd` is resolved to a worker-run template
///   (J7); effect commands get a bound worker.run
///   approval, reads get none. The draft is a
///   machine-readable step record (the step's
///   `verify_cmd`); the approval (when one exists) is
///   what `tasks_execute` checks before spawning.
///
/// A prepare-time failure (unmappable command, no
/// bound worktree, model failure, approval failure)
/// is a step failure (Q-ORC-11): Evidence +
/// task failed, no skip, no retry.
async fn prepare_executor_step(state: AppState, task_id: &str, plan: &PlanRow, step_no: i64) {
    let steps = match plan.steps() {
        Ok(steps) => steps,
        Err(detail) => {
            fail(&state, task_id, &format!("蓝图步骤解析失败：{detail}"));
            return;
        }
    };
    let Some(step) = steps.iter().find(|s| s.step_no == step_no) else {
        fail_step(
            &state,
            task_id,
            plan,
            step_no,
            "unknown",
            "蓝图步骤缺失",
            execution_for_task(&state, task_id).as_deref(),
            None,
            None,
            None,
        );
        return;
    };
    // Cancel checkpoint: only prepare while the run is
    // active (running, or draft_ready across worker
    // steps — see `task_is_active`).
    if !task_is_active(&state, task_id) {
        return;
    }
    match step.kind.as_str() {
        "file_write" => {
            prepare_file_write_step(state, task_id, plan, step).await;
        }
        "command" | "verify" | "review" => {
            prepare_worker_step(state, task_id, plan, step).await;
        }
        other => {
            fail_step(
                &state,
                task_id,
                plan,
                step_no,
                other,
                "蓝图步骤 kind 非法",
                execution_for_task(&state, task_id).as_deref(),
                None,
                None,
                None,
            );
        }
    }
}

/// ORC-2 (J2): a file-write step. The Executor model
/// produces the step's file content from the step's
/// change description plus the file's current content
/// (empty when the file does not exist yet); the
/// runner then creates the bound fs.write approval
/// for it. The content is the step's draft and the
/// step's path is its artifact_path — `tasks_execute`
/// applies exactly this pair.
async fn prepare_file_write_step(
    state: AppState,
    task_id: &str,
    plan: &PlanRow,
    step: &crate::plan_store::PlanStep,
) {
    let (_planner_model, executor_model) = role_models(&state);
    let current = read_step_file(&state, &step.path);
    let prompt = format!(
        "{EXECUTOR_PROMPT_PREFIX}步骤 {step_no}（共 {total} 步），蓝图 {plan_id}：\n- 文件：{path}\n- 符号：{symbol}\n- 改动：{change}\n- 验证：{verify_cmd}，预期：{expected}\n\n文件当前内容（不存在时为空）：\n{current}\n\n只返回改动后的文件完整内容，不要 markdown 围栏、不要解释文字。",
        step_no = step.step_no,
        total = plan.steps().map(|s| s.len()).unwrap_or(0),
        plan_id = plan.plan_id,
        path = step.path,
        symbol = step.symbol,
        change = step.change,
        verify_cmd = step.verify_cmd,
        expected = step.expected,
        current = current,
    );
    let frozen_rules = frozen_rules_for(&state, task_id);
    let content = match state
        .deepseek
        .reply_with_rules_and_model(frozen_rules, &executor_model, &prompt)
        .await
    {
        DeepseekOutcome::Disabled => {
            fail_step(
                &state,
                task_id,
                plan,
                step.step_no,
                &step.kind,
                "模型未配置",
                execution_for_task(&state, task_id).as_deref(),
                None,
                None,
                None,
            );
            return;
        }
        DeepseekOutcome::Denied(pattern) => {
            fail_step(
                &state,
                task_id,
                plan,
                step.step_no,
                &step.kind,
                &format!("网络规则拒绝模型调用：{pattern}"),
                execution_for_task(&state, task_id).as_deref(),
                None,
                None,
                None,
            );
            return;
        }
        DeepseekOutcome::Failed(_) => {
            fail_step(
                &state,
                task_id,
                plan,
                step.step_no,
                &step.kind,
                "模型调用失败",
                execution_for_task(&state, task_id).as_deref(),
                None,
                None,
                None,
            );
            return;
        }
        DeepseekOutcome::Replied(text) => {
            if text.trim().is_empty() {
                fail_step(
                    &state,
                    task_id,
                    plan,
                    step.step_no,
                    &step.kind,
                    "执行回合未返回文件内容",
                    execution_for_task(&state, task_id).as_deref(),
                    None,
                    None,
                    None,
                );
                return;
            }
            text
        }
    };
    // Cancel checkpoint: a cancelled run discards the
    // step content and creates no approval.
    if !task_is_active(&state, task_id) {
        return;
    }
    persist_draft(&state, task_id, &content);
    let approval_id = match create_bound_write_approval(&state, &step.path, content.as_bytes()) {
        Ok(id) => id,
        Err(error) => {
            fail_step(
                &state,
                task_id,
                plan,
                step.step_no,
                &step.kind,
                &error,
                execution_for_task(&state, task_id).as_deref(),
                None,
                None,
                None,
            );
            return;
        }
    };
    // Final transition shares one lock acquisition with
    // its running check, so a cancel cannot slip
    // between them. (A worker step's prepare runs while
    // the task is `draft_ready` — the status stays
    // `draft_ready`, only the step's draft + approval
    // change.)
    let mut tasks = state.tasks.lock().expect("task lock");
    if let Some(current) = tasks.get_mut(task_id) {
        if !matches!(current.status.as_str(), "running" | "draft_ready") {
            return;
        }
        current.status = "draft_ready".to_string();
        current.error = None;
        current.artifact_path = Some(step.path.clone());
        current.approval_id = Some(approval_id);
        current.executor_step_no = Some(step.step_no);
        current.draft_sha256 = Some(operation_binding::sha256_hex(content.as_bytes()));
        current.draft_bytes = Some(content.len() as u64);
        let updated = current.clone();
        persist_task(&state, &updated);
    }
}

/// ORC-2: read a step's target file (files_root
/// relative) for the Executor prompt. A missing or
/// unreadable file is not an error — the step may be
/// creating it — so the current content is empty.
fn read_step_file(state: &AppState, path: &str) -> String {
    let Ok(full) = resolve_file(&state.files_root, path) else {
        return String::new();
    };
    let Ok(bytes) = std::fs::read(full) else {
        return String::new();
    };
    let mut capped = &bytes[..bytes.len().min(MAX_FILE_BYTES)];
    while !capped.is_empty() && std::str::from_utf8(capped).is_err() {
        capped = &capped[..capped.len() - 1];
    }
    String::from_utf8_lossy(capped).into_owned()
}

/// ORC-2: prepare a worker step (`command` / `verify` /
/// `review`). The step's `verify_cmd` maps to a
/// worker-run template (J7); effect commands get a
/// bound worker.run approval, reads get none. The
/// draft is the step's `verify_cmd` itself — the
/// machine-readable record of what the step runs —
/// so `tasks_execute` can apply it without guessing.
async fn prepare_worker_step(
    state: AppState,
    task_id: &str,
    plan: &PlanRow,
    step: &crate::plan_store::PlanStep,
) {
    let execution_id = execution_for_task(&state, task_id);
    let (template, params) = match resolve_verify_command(&step.verify_cmd) {
        Ok(resolved) => resolved,
        Err(detail) => {
            fail_step(
                &state,
                task_id,
                plan,
                step.step_no,
                &step.kind,
                &detail,
                execution_id.as_deref(),
                None,
                None,
                None,
            );
            return;
        }
    };
    // J10: worker steps run in the task's frozen
    // execute binding. No binding → the step cannot
    // run anywhere; fail it rather than guessing a
    // worktree.
    let worktree_id = {
        let tasks = state.tasks.lock().expect("task lock");
        tasks
            .get(task_id)
            .and_then(|r| r.execute_binding.as_ref())
            .map(|binding| binding.worktree_id.clone())
    };
    let Some(worktree_id) = worktree_id else {
        fail_step(
            &state,
            task_id,
            plan,
            step.step_no,
            &step.kind,
            "任务未绑定工作区，请在任务详情里选择工作区后重试",
            execution_id.as_deref(),
            None,
            None,
            None,
        );
        return;
    };
    let approval_id = if is_effect_command(&template) {
        match create_bound_worker_approval(&state, &worktree_id, &template, &params) {
            Ok(id) => Some(id),
            Err(detail) => {
                fail_step(
                    &state,
                    task_id,
                    plan,
                    step.step_no,
                    &step.kind,
                    &detail,
                    execution_id.as_deref(),
                    None,
                    None,
                    None,
                );
                return;
            }
        }
    } else {
        None
    };
    let draft = step.verify_cmd.clone();
    persist_draft(&state, task_id, &draft);
    // Final transition shares one lock acquisition with
    // its running check, so a cancel cannot slip
    // between them. (A worker step's prepare runs while
    // the task is `draft_ready` — the status stays
    // `draft_ready`, only the step's draft + approval
    // change.)
    let mut tasks = state.tasks.lock().expect("task lock");
    if let Some(current) = tasks.get_mut(task_id) {
        if !matches!(current.status.as_str(), "running" | "draft_ready") {
            return;
        }
        current.status = "draft_ready".to_string();
        current.error = None;
        current.artifact_path = None;
        current.approval_id = approval_id;
        current.executor_step_no = Some(step.step_no);
        current.draft_sha256 = Some(operation_binding::sha256_hex(draft.as_bytes()));
        current.draft_bytes = Some(draft.len() as u64);
        let updated = current.clone();
        persist_task(&state, &updated);
    }
}

/// ORC-2 (J11): the Executor's worker-step execution,
/// spawned by `tasks_execute` after the user approved
/// the step. The in-memory claim guards a double
/// `tasks_execute` on a read step — the
/// approval-consumption guard that protects file steps
/// does not exist for reads (no approval), so the
/// claim is the guard. The claim is held only for
/// the create + wait; it is released on every exit
/// path (the inner function returns, the outer
/// removes the claim).
async fn execute_current_worker_step(state: AppState, record: TaskFileRecord) {
    let task_id = record.task_id.clone();
    let Some(step_no) = record.executor_step_no else {
        return;
    };
    let claimed = {
        let mut claims = state
            .executor_step_claims
            .lock()
            .expect("executor claim lock");
        claims.insert((task_id.clone(), step_no))
    };
    if !claimed {
        return;
    }
    execute_worker_step_inner(state.clone(), &record, step_no).await;
    state
        .executor_step_claims
        .lock()
        .expect("executor claim lock")
        .remove(&(task_id, step_no));
}

/// ORC-2 (J11): run one worker step to its terminal
/// status, then advance or fail. The worker run is
/// created through the internal `/worker/runs`
/// creation path (`create_worker_run_internal`,
/// J3) — no HTTP loopback — and driven by the
/// sweeper like any other run. The run's own Effect
/// and Evidence are recorded by the worker machinery;
/// this function only decides the batch's fate:
/// succeeded → next step (or complete the task when
/// the last step succeeded); any other terminal
/// status → step failure (Q-ORC-11).
async fn execute_worker_step_inner(state: AppState, record: &TaskFileRecord, step_no: i64) {
    let task_id = record.task_id.clone();
    let Some(plan) = load_current_plan(&state, &task_id) else {
        fail(&state, &task_id, "蓝图缺失，无法执行步骤");
        return;
    };
    let steps = match plan.steps() {
        Ok(steps) => steps,
        Err(detail) => {
            fail(&state, &task_id, &format!("蓝图步骤解析失败：{detail}"));
            return;
        }
    };
    let Some(step) = steps.iter().find(|s| s.step_no == step_no) else {
        fail_step(
            &state,
            &task_id,
            &plan,
            step_no,
            "unknown",
            "蓝图步骤缺失",
            execution_for_task(&state, &task_id).as_deref(),
            None,
            None,
            None,
        );
        return;
    };
    if !matches!(step.kind.as_str(), "command" | "verify" | "review") {
        // File steps never reach here (tasks_execute
        // applies them); a stale advance is a no-op.
        return;
    }
    let execution_id = execution_for_task(&state, &task_id);
    let (template, params) = match resolve_verify_command(&step.verify_cmd) {
        Ok(resolved) => resolved,
        Err(detail) => {
            fail_step(
                &state,
                &task_id,
                &plan,
                step.step_no,
                &step.kind,
                &detail,
                execution_id.as_deref(),
                None,
                None,
                None,
            );
            return;
        }
    };
    let worktree_id = {
        let tasks = state.tasks.lock().expect("task lock");
        tasks
            .get(&task_id)
            .and_then(|r| r.execute_binding.as_ref())
            .map(|binding| binding.worktree_id.clone())
    };
    let Some(worktree_id) = worktree_id else {
        fail_step(
            &state,
            &task_id,
            &plan,
            step.step_no,
            &step.kind,
            "任务未绑定工作区，请在任务详情里选择工作区后重试",
            execution_id.as_deref(),
            None,
            None,
            None,
        );
        return;
    };
    // Effect steps carry the approval the user decided
    // at the step's prepare; reads carry none. A
    // consumed or missing approval here means the step
    // was never properly approved — do not run it, and
    // do not fail the task (the user may still approve;
    // `tasks_execute` already 409s in that case).
    if is_effect_command(&template) {
        let approved = record.approval_id.as_deref().is_some_and(|approval_id| {
            approval_status(&state, approval_id) == Some(ApprovalStatus::Approved)
        });
        if !approved {
            return;
        }
    }
    let run = match create_worker_run_internal(
        &state,
        WorkerRunCreateDto::internal(
            worktree_id,
            template.clone(),
            Some(params),
            execution_id.clone(),
            record.approval_id.clone(),
        ),
    )
    .await
    {
        Ok(run) => run,
        Err(err) => {
            fail_step(
                &state,
                &task_id,
                &plan,
                step.step_no,
                &step.kind,
                &format!("worker run 创建失败：{}", err.code),
                execution_id.as_deref(),
                None,
                None,
                None,
            );
            return;
        }
    };
    // Wait for the run's terminal status (the sweeper
    // drives it). The wait is bounded: a run that
    // never reaches a terminal status within its
    // timeout + 60 s is a step failure, not an
    // indefinite hang.
    let run_id = run.run_id.clone();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let (status, exit_code, effect_id) = loop {
        if std::time::Instant::now() > deadline {
            fail_step(
                &state,
                &task_id,
                &plan,
                step.step_no,
                &step.kind,
                "worker run 等待超时",
                execution_id.as_deref(),
                Some(&run_id),
                None,
                None,
            );
            return;
        }
        let row = {
            let Some(sqlite) = state.sqlite.as_ref() else {
                fail_step(
                    &state,
                    &task_id,
                    &plan,
                    step.step_no,
                    &step.kind,
                    "SQLite 不可用",
                    execution_id.as_deref(),
                    Some(&run_id),
                    None,
                    None,
                );
                return;
            };
            let conn = sqlite.lock().expect("sqlite lock");
            crate::sqlite_store::get_worker_run(&conn, &run_id)
        };
        match row {
            Ok(Some(run_row)) => {
                if is_terminal_run_status(&run_row.status) {
                    break (
                        run_row.status.clone(),
                        run_row.exit_code,
                        run_row.effect_id.clone(),
                    );
                }
            }
            Ok(None) => {
                fail_step(
                    &state,
                    &task_id,
                    &plan,
                    step.step_no,
                    &step.kind,
                    "worker run 记录缺失",
                    execution_id.as_deref(),
                    Some(&run_id),
                    None,
                    None,
                );
                return;
            }
            Err(detail) => {
                fail_step(
                    &state,
                    &task_id,
                    &plan,
                    step.step_no,
                    &step.kind,
                    &format!("worker run 读取失败：{detail}"),
                    execution_id.as_deref(),
                    Some(&run_id),
                    None,
                    None,
                );
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };
    if status != "succeeded" {
        record_orc_evidence(
            &state,
            effect_id
                .as_deref()
                .unwrap_or(&format!("executor:{task_id}:{step_no}")),
            execution_id.as_deref(),
            "executor",
            "verified",
            "step_failed",
            serde_json::json!({
                "task_id": task_id,
                "plan_id": plan.plan_id,
                "step_no": step_no,
                "step_kind": step.kind,
                "failure": "worker_run_failed",
                "run_id": run_id,
                "run_status": status,
                "exit_code": exit_code,
            }),
        );
        fail(
            &state,
            &task_id,
            &format!("步骤 {step_no} 的 worker run 失败：{status}"),
        );
        return;
    }
    // The step succeeded. Advance: the next step, or
    // complete the task when the plan is exhausted.
    // The worker step executes while the task is
    // `draft_ready` (its approval point), and the task
    // STAYS `draft_ready` across worker steps —
    // `draft_ready → running` is not a legal edge and
    // no new edge may be added. Only the last step's
    // success walks the legal `draft_ready →
    // completed` edge. (The draft_ready arm only fires
    // from a fresh `tasks_execute` call, never from
    // this path.)
    let next = step_no + 1;
    let total = steps.len() as i64;
    if next > total {
        let updated = {
            let mut tasks = state.tasks.lock().expect("task lock");
            let Some(current) = tasks.get_mut(&task_id) else {
                return;
            };
            if current.status != "draft_ready" {
                return;
            }
            current.status = "completed".to_string();
            current.error = None;
            current.executor_step_no = None;
            current.clone()
        };
        persist_task(&state, &updated);
        close_execution_for_task(&state, &task_id, "completed");
        return;
    }
    {
        let mut tasks = state.tasks.lock().expect("task lock");
        if let Some(current) = tasks.get_mut(&task_id) {
            if !matches!(current.status.as_str(), "running" | "draft_ready") {
                return;
            }
            current.executor_step_no = Some(next);
            let updated = current.clone();
            persist_task(&state, &updated);
        }
    }
    prepare_executor_step(state, &task_id, &plan, next).await;
}
