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
use crate::loop_engine::{LoopError, LoopStatus};
use crate::evidence::OwnedEvidence;
use crate::model_store::resolve_role_models;
use crate::operation_binding;
use crate::plan_store::{
    create_plan, current_plan_for_task, validate_plan_steps, validate_replan_if, PlanReplanIfDto,
    PlanRow, PlanStepDto,
};
use crate::effect_store::NewEffect;
use crate::server::{
    approval_status, check_idempotency, close_execution, close_execution_for_task,
    close_execution_with_inbox, consume_bound_approval_for_effect, create_execution,
    create_worker_run_internal, decide_approval_rejected, ensure_root, execution_for_task,
    execution_status, persist_approval, persist_task, persist_worktree, record_effect,
    record_evidence_best_effort, resolve_file, start_bound_effect, transition_effect,
    worktree_canonical_root, worker_command_digest, ApprovalState, AppState, IdempotencyDecision,
    WorkerRunCreateDto,
};
use crate::sqlite_store::{canonical_json, get_worker_run, now_ms};
use crate::swarm_store::{SwarmRow, SwarmStep};
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
pub(crate) const PLANNER_JSON_CONTRACT: &str = "\n\n请判断这个任务是否需要修改文件。只返回一个严格 JSON 对象，不要 markdown 围栏、不要解释文字：\n{\"needs_file_change\": true|false, \"answer\": \"无需改文件时直接回答任务问题的文字；需要改文件时可省略\", \"steps\": [{\"step_no\": 1, \"kind\": \"file_write|command|verify|review\", \"path\": \"相对路径\", \"symbol\": \"函数或符号名\", \"change\": \"改动描述\", \"verify_cmd\": \"验证命令\", \"expected\": \"预期输出\"}], \"research\": {\"question\": \"需要先调研的问题；不需要调研时省略\"}, \"replan_if\": [{\"signal\": \"触发信号描述\", \"action\": \"stop_and_hand_back\"}]}\n规则：\n- needs_file_change 为 false 时，steps 用空数组，answer 必须给出直接回答。\n- 需要先调研代码或现状才能定蓝图时，只返回 research（needs_file_change 填 false，steps 用空数组，不产蓝图）；不需要调研时直接产蓝图，research 省略。\n- 需要改文件时，steps 至少 1 步，step_no 从 1 连续编号；每步四件套 path/symbol/change/verify_cmd/expected 都必填。\n- verify_cmd 只允许以下形式：cargo test -p <包名>、git status、git diff、git diff -- <路径>；不许写其他命令。\n- replan_if 可为空；有信号时 action 只能是 stop_and_hand_back。\n";

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

/// ORC-3 (Q-R3-01): the Re-plan-if judge prompt
/// (LOCAL DEFINITION). The judge answers one
/// question — does this step failure match one of the
/// step's `replan_if` signals — and returns only the
/// verdict plus the matched signal, never a new
/// blueprint. Strict JSON only, like the Planner
/// contract above. The prompt is built per failure by
/// `replan_judge_prompt`; this constant is the tail
/// (the contract + rules).
const REPLAN_JUDGE_CONTRACT: &str = "\n\n请判断这次步骤失败是否命中上面任一信号。只返回一个严格 JSON 对象，不要 markdown 围栏、不要解释文字：\n{\"triggered\": true|false, \"signal\": \"命中的信号原文，未命中时为 null\"}\n规则：\n- 只有当失败事实与某个信号描述的情形相符时，才返回 triggered true，并原样返回该信号。\n- 不相符、无法判断时，返回 triggered false、signal null。\n";

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
/// pub(crate): the replan route parses the Planner
/// round's reply with the same struct.
#[derive(Debug, Deserialize)]
pub(crate) struct PlannerOutput {
    pub(crate) needs_file_change: bool,
    #[serde(default)]
    pub(crate) answer: Option<String>,
    #[serde(default)]
    pub(crate) steps: Vec<PlanStepDto>,
    #[serde(default)]
    pub(crate) replan_if: Vec<PlanReplanIfDto>,
    /// ORC-4 (Q-R4-01): a first-round Planner reply
    /// that asks for research instead of a blueprint.
    /// The runner runs the research loop, injects the
    /// report summary into a second Planner prompt,
    /// and only then expects a blueprint.
    #[serde(default)]
    pub(crate) research: Option<ResearchRequest>,
}

/// ORC-4 (Q-R4-01): the Planner's research ask.
#[derive(Debug, Deserialize)]
pub(crate) struct ResearchRequest {
    pub(crate) question: String,
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

/// ORC-3 (Q-R3-01): the Planner slot, live from the
/// staged record — the re-plan judge and the replan
/// route's Planner round both call the planner slot,
/// never the executor slot and never a hardcoded
/// model. pub(crate): the replan route resolves it.
pub(crate) fn planner_model_for(state: &AppState) -> String {
    role_models(state).0
}

/// E2-B §3.4 (snapshot gate), shared by the Planner and
/// Executor calls: the model call for this task's
/// Execution is denied by the rules frozen in the
/// Execution's snapshot at creation. No Execution yet
/// (the Planner round runs before one exists) → live
/// policy, exactly as the old draft flow did.
/// pub(crate): the replan route's Planner round calls
/// with the same rules.
pub(crate) fn frozen_rules_for(
    state: &AppState,
    task_id: &str,
) -> Option<Vec<crate::policy_store::NetworkRule>> {
    execution_for_task(state, task_id)
        .and_then(|execution_id| crate::server::frozen_network_rules_for_execution(state, &execution_id))
}

/// ORC-2: one audit Evidence row for a Planner/Executor
/// decision (triage, step failure). Best-effort like
/// every other Evidence write: a failure warns, never
/// fails the step it records. pub(crate): the replan
/// route writes its `replan_exhausted` row with it.
pub(crate) fn record_orc_evidence(
    state: &AppState,
    effect_id: &str,
    execution_id: Option<&str>,
    source: &str,
    verdict: &str,
    reason: &str,
    observed: serde_json::Value,
) -> Option<String> {
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
    // ORC-4 (R4-J6): the new row's id, so the
    // research report can be named in the second
    // Planner prompt. Existing callers ignore it.
    record_evidence_best_effort(state, &draft)
}

/// ORC-3 (Q-R3-01): the Re-plan-if judge's reply,
/// parsed strictly. `triggered` is the one field the
/// evaluator cannot default; `signal` must name a
/// signal from the step's list when triggered.
#[derive(Debug, Deserialize)]
struct ReplanJudgment {
    triggered: bool,
    #[serde(default)]
    signal: Option<String>,
}

/// ORC-3 (Q-R3-01/02): the outcome of one trigger
/// evaluation. Triggered = the failure matched a
/// signal and the hand-back already landed;
/// NotTriggered = the evaluation ran (one
/// `replan_not_triggered` row) and the caller keeps
/// its original failure path; Aborted = the run was
/// cancelled while the judge call was in flight, so
/// the caller writes nothing and fails nothing.
pub(crate) enum ReplanEvaluation {
    Triggered,
    NotTriggered,
    Aborted,
}

/// ORC-3 (Q-R3-01): build the judge prompt for one
/// step failure: the failure facts plus the step's
/// `replan_if` signal list. The judge returns only a
/// verdict + the matched signal, never a blueprint.
#[allow(clippy::too_many_arguments)]
fn replan_judge_prompt(
    task_id: &str,
    plan: &PlanRow,
    step_no: i64,
    step_kind: &str,
    failure: &str,
    signals: &[crate::plan_store::ReplanIf],
    run_id: Option<&str>,
    run_status: Option<&str>,
    exit_code: Option<i64>,
) -> String {
    let signal_list = signals
        .iter()
        .map(|condition| format!("- {}", condition.signal))
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "你是任务的重规划判定者。只做判定，不产新蓝图、不写文件、不执行命令。\n\n失败事实如下：\n- 任务：{task_id}\n- 蓝图：{plan_id}\n- 步骤号：{step_no}（kind：{step_kind}）\n- 失败：{failure}\n- worker run：{run_id}，状态：{run_status}，退出码：{exit_code}\n\n该步骤的 replan_if 信号列表如下：\n{signal_list}\n{contract}",
        plan_id = plan.plan_id,
        run_id = run_id.unwrap_or("无"),
        run_status = run_status.unwrap_or("无"),
        exit_code = exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "无".to_string()),
        contract = REPLAN_JUDGE_CONTRACT,
    )
}

/// ORC-3 (Q-R3-05): build the replan route's Planner
/// prompt: the file context plus the old blueprint,
/// the stopped step number and the trigger signal.
/// pub(crate): the replan route builds it.
pub(crate) fn replan_planner_prompt(
    file_context: &str,
    old_plan: &PlanRow,
    step_no: i64,
    signal: Option<&str>,
) -> String {
    format!(
        "{PLANNER_PROMPT_PREFIX}{file_context}{PLANNER_JSON_CONTRACT}\n\n本次为重新规划：旧蓝图如下（JSON）：\n{steps_json}\n停住的步骤号：{step_no}\n触发信号：{signal}\n请基于旧蓝图重新规划：保留仍有效的步骤，修正导致失败的步骤；返回完整的新蓝图（steps 非空，needs_file_change 为 true）。",
        steps_json = old_plan.steps_json,
        signal = signal.unwrap_or("未记录——本次为手动重新规划"),
    )
}

/// ORC-3 (Q-R3-01/02/03/04): evaluate one step
/// failure against the step's `replan_if` signals and,
/// when one matches, hand the task back instead of
/// failing it. Exactly one `replan_*` Evidence row is
/// written per evaluation (R3-J10): `replan_triggered`
/// on a match, `replan_not_triggered` otherwise; the
/// observed carries only the Q-R3-04 fields (no
/// `failure` text — the pre-existing `step_failed`
/// row, when the caller writes one, keeps it).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn evaluate_step_failure(
    state: &AppState,
    task_id: &str,
    plan: &PlanRow,
    step_no: i64,
    step_kind: &str,
    failure: &str,
    execution_id: Option<&str>,
    effect_id: Option<&str>,
    run_id: Option<&str>,
    run_status: Option<&str>,
    exit_code: Option<i64>,
) -> ReplanEvaluation {
    let observed = |signal: Option<&str>| {
        serde_json::json!({
            "task_id": task_id,
            "plan_id": plan.plan_id,
            "step_no": step_no,
            "step_kind": step_kind,
            "signal": signal,
            "run_id": run_id,
            "run_status": run_status,
            "exit_code": exit_code,
        })
    };
    let evidence_effect = effect_id
        .map(str::to_string)
        .unwrap_or_else(|| format!("executor:{task_id}:{step_no}"));
    let record = |reason: &str, signal: Option<&str>| {
        record_orc_evidence(
            state,
            &evidence_effect,
            execution_id,
            "executor",
            "verified",
            reason,
            observed(signal),
        );
    };
    let signals = match plan.replan_if() {
        Ok(signals) => signals,
        Err(_) => {
            // A plan whose replan_if cannot parse is
            // corrupt; the evaluation cannot match
            // anything — record the miss and keep the
            // original failure path.
            record("replan_not_triggered", None);
            return ReplanEvaluation::NotTriggered;
        }
    };
    if signals.is_empty() {
        // R3-J12: no signals, no judge call — a match
        // is impossible by construction.
        record("replan_not_triggered", None);
        return ReplanEvaluation::NotTriggered;
    }
    let planner_model = planner_model_for(state);
    let prompt = replan_judge_prompt(
        task_id,
        plan,
        step_no,
        step_kind,
        failure,
        &signals,
        run_id,
        run_status,
        exit_code,
    );
    let reply = match state
        .deepseek
        .reply_with_rules_and_model(frozen_rules_for(state, task_id), &planner_model, &prompt)
        .await
    {
        DeepseekOutcome::Replied(text) => text,
        // R3-J2: the judge is best-effort — a disabled
        // model, a denied call, or a failed call
        // degrades to NOT triggered and must never
        // change the failure path.
        DeepseekOutcome::Disabled | DeepseekOutcome::Denied(_) | DeepseekOutcome::Failed(_) => {
            record("replan_not_triggered", None);
            return ReplanEvaluation::NotTriggered;
        }
    };
    // Cancel checkpoint: a cancelled run discards the
    // judgment — the cancel path owns the audit, so
    // this evaluation writes nothing.
    if !task_is_active(state, task_id) {
        return ReplanEvaluation::Aborted;
    }
    let judgment = match serde_json::from_str::<ReplanJudgment>(reply.trim()) {
        Ok(judgment) => judgment,
        Err(_) => {
            record("replan_not_triggered", None);
            return ReplanEvaluation::NotTriggered;
        }
    };
    let Some(signal) = judgment
        .signal
        .as_deref()
        .filter(|_| judgment.triggered)
    else {
        record("replan_not_triggered", None);
        return ReplanEvaluation::NotTriggered;
    };
    if !signals.iter().any(|condition| condition.signal == signal) {
        record("replan_not_triggered", None);
        return ReplanEvaluation::NotTriggered;
    }
    record("replan_triggered", Some(signal));
    if hand_back(state, task_id, execution_id, effect_id) {
        ReplanEvaluation::Triggered
    } else {
        ReplanEvaluation::Aborted
    }
}

/// ORC-3 (Q-R3-03/04): hand a task back after a
/// triggered failure. The task stays `draft_ready`
/// (direct assignment — `running → draft_ready` is
/// not a legal edge, and the hand-back is not a
/// failure); the Execution goes `needs_review`, which
/// opens the `review_required` Inbox row naming the
/// failed step's Effect when one exists. The Evidence
/// row is written before the transition (in the
/// caller). With no Execution there is nothing to
/// transition — the task still lands, and the gap is
/// a warning, not an error. The status check and the
/// assignment share one lock acquisition, so a cancel
/// cannot slip between them. Returns false when the
/// task is no longer active (the caller then treats
/// the evaluation as aborted).
fn hand_back(
    state: &AppState,
    task_id: &str,
    execution_id: Option<&str>,
    effect_id: Option<&str>,
) -> bool {
    {
        let mut tasks = state.tasks.lock().expect("task lock");
        let Some(current) = tasks.get_mut(task_id) else {
            return false;
        };
        if !matches!(current.status.as_str(), "running" | "draft_ready") {
            return false;
        }
        current.status = "draft_ready".to_string();
        current.error = None;
        let updated = current.clone();
        persist_task(state, &updated);
    }
    match execution_id {
        Some(id) => {
            close_execution_with_inbox(state, id, "needs_review", effect_id);
        }
        None => {
            eprintln!(
                "warning: ORC-3 hand-back for task {task_id} has no Execution; no Inbox row opened"
            );
        }
    }
    true
}

/// ORC-2: the run's Execution is created here — at the
/// point execution actually starts — not in the run /
/// resume handlers. A triage run never reaches this
/// call, so it never has an Execution (Q-ORC-07).
/// A fresh attempt supersedes any still-running
/// Execution a crashed earlier attempt left behind.
fn begin_execution(state: &AppState, task_id: &str) -> String {
    // ORC-3 (R3-J6): resuming after a hand-back closes
    // the review row: a `needs_review` Execution for
    // the task returns to `running` before the new
    // Execution is created. `supersede_old_task_executions`
    // only touches `running` ones, so without this the
    // row would stay open forever. The transition is
    // what auto-closes it (any non-review status does).
    if let Some(old_id) = execution_for_task(state, task_id) {
        if execution_status(state, &old_id).as_deref() == Some("needs_review") {
            close_execution(state, &old_id, "running");
        }
    }
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
///
/// ORC-3 (Q-R3-02): the failure is evaluated against
/// the step's `replan_if` signals BEFORE it is
/// recorded. A match hands the task back (no
/// `step_failed` row, no `fail()`); a cancelled run
/// writes nothing; anything else keeps the original
/// failure path below. No Effect exists yet at the
/// prepare-time failures this covers, so the
/// evaluation's `effect_id` is None here — the
/// worker-run branch passes the run's Effect itself.
#[allow(clippy::too_many_arguments)]
async fn fail_step(
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
    match evaluate_step_failure(
        state,
        task_id,
        plan,
        step_no,
        step_kind,
        failure,
        execution_id,
        None,
        run_id,
        run_status,
        exit_code,
    )
    .await
    {
        ReplanEvaluation::Triggered | ReplanEvaluation::Aborted => return,
        ReplanEvaluation::NotTriggered => {}
    }
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
    // ORC-5: a task with an active swarm is driven by
    // the swarm drive, not the serial runner — a
    // re-spawned runner would prepare the serial
    // step-1 approval over the swarm's steps. The
    // merge continuation spawns `run_task` only after
    // the swarm row is `completed`, so this guard
    // never blocks it.
    if let Some(sqlite) = &state.sqlite {
        let conn = sqlite.lock().expect("sqlite lock");
        if let Ok(Some(_)) = crate::swarm_store::active_swarm_for_task(&conn, &task_id) {
            return;
        }
    }
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
/// pub(crate): the replan route reads the same file
/// context the Planner round reads.
pub(crate) fn read_task_files(state: &AppState, directory: &str) -> Result<Vec<String>, String> {
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
/// pub(crate): the replan route parses the Planner
/// round's reply with the same strict rule.
pub(crate) fn parse_planner_output(text: &str) -> Result<PlannerOutput, String> {
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
/// ORC-4 (Q-R4-05): the research summary is the
/// report's final text truncated to 4000 chars
/// (char-boundary safe).
fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    text.chars().take(max_chars).collect()
}

/// ORC-4: the outcome of one Planner research round.
enum ResearchOutcome {
    /// Report ready: summary + Evidence row id.
    Reported { summary: String, evidence_id: Option<String> },
    /// No report (start failed, loop failed, timed
    /// out): the Planner continues without one.
    Failed,
    /// The task was cancelled mid-research.
    Aborted,
}

/// ORC-4: one Planner model call; a model-side
/// failure fails the plan round exactly like the
/// first-round call does. None means the round
/// already failed and the caller returns.
async fn planner_round_reply(
    state: &AppState,
    task_id: &str,
    frozen_rules: Option<Vec<crate::policy_store::NetworkRule>>,
    planner_model: &str,
    prompt: &str,
) -> Option<String> {
    match state
        .deepseek
        .reply_with_rules_and_model(frozen_rules, planner_model, prompt)
        .await
    {
        DeepseekOutcome::Disabled => {
            fail_or_cancel_plan_round(state, task_id, "模型未配置");
            None
        }
        DeepseekOutcome::Denied(pattern) => {
            fail_or_cancel_plan_round(
                state,
                task_id,
                &format!("网络规则拒绝模型调用：{pattern}"),
            );
            None
        }
        DeepseekOutcome::Failed(_) => {
            fail_or_cancel_plan_round(state, task_id, "模型调用失败");
            None
        }
        DeepseekOutcome::Replied(text) => Some(text),
    }
}

/// ORC-4 (Q-R4-01/Q-R4-09, R4-J3): run one research
/// loop for the Planner's question and write its
/// report (or failure) to Evidence. The internal
/// call goes through the same `start_research` the
/// HTTP route uses (R4-J2), so it gets the same
/// phase check. Best-effort: any failure leaves one
/// `research_failed` row and the Planner continues
/// without a report; the run never fails for
/// research alone. Polls get() every 500 ms until
/// terminal; the deadline is the loop's own budget
/// plus 30 s grace for the last step to land.
async fn run_planner_research(
    state: &AppState,
    task_id: &str,
    file_context: &str,
    question: &str,
    worktree_id: Option<String>,
) -> ResearchOutcome {
    let prompt = crate::routes_loop::research_prompt(file_context, worktree_id.as_deref(), Some(question));
    let loop_id = match crate::routes_loop::start_research(
        state, task_id, prompt, worktree_id, None, None,
    ) {
        Ok(view) => view.record.loop_id,
        Err(LoopError(_, code)) => {
            // R4-J8: no loop was started, so the row
            // carries task_id + the failure code only.
            record_orc_evidence(
                state,
                &format!("research:{task_id}"),
                None,
                "planner",
                "verified",
                "research_failed",
                serde_json::json!({
                    "task_id": task_id,
                    "error": code,
                }),
            );
            return ResearchOutcome::Failed;
        }
    };
    let budget_ms = state
        .loops
        .get(&loop_id)
        .map(|v| v.record.budget_ms)
        .unwrap_or(120_000);
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_millis(budget_ms + 30_000);
    loop {
        if std::time::Instant::now() >= deadline {
            record_orc_evidence(
                state,
                &format!("research:{task_id}"),
                None,
                "planner",
                "verified",
                "research_failed",
                serde_json::json!({
                    "loop_id": loop_id,
                    "task_id": task_id,
                    "error": "research_timeout",
                }),
            );
            return ResearchOutcome::Failed;
        }
        // Cancel checkpoint: a cancelled run stops
        // waiting; the caller handles the close.
        {
            let tasks = state.tasks.lock().expect("task lock");
            if tasks.get(task_id).map(|r| r.status.as_str()) != Some("running") {
                return ResearchOutcome::Aborted;
            }
        }
        match state.loops.get(&loop_id) {
            Ok(view) if view.record.status.terminal() => {
                if view.record.status == LoopStatus::Completed {
                    if let Some(text) = view.final_text.filter(|t| !t.trim().is_empty()) {
                        let summary = truncate_chars(&text, 4000);
                        let evidence_id = record_orc_evidence(
                            state,
                            &format!("research:{task_id}"),
                            None,
                            "planner",
                            "verified",
                            "research_report",
                            serde_json::json!({
                                "loop_id": loop_id,
                                "task_id": task_id,
                                "report_len": text.len(),
                                "report_sha256": operation_binding::sha256_hex(text.as_bytes()),
                                "summary_len": summary.len(),
                                // Q-R4-04 (user-approved
                                // exception): the report text
                                // itself, breaking the
                                // hash/count/id-only convention
                                // for observed_json.
                                "report_text": text,
                            }),
                        );
                        return ResearchOutcome::Reported { summary, evidence_id };
                    }
                }
                let error = view
                    .record
                    .error
                    .as_ref()
                    .and_then(|e| serde_json::to_value(e).ok())
                    .and_then(|v| v.as_str().map(str::to_string))
                    .unwrap_or_else(|| "unknown".to_string());
                record_orc_evidence(
                    state,
                    &format!("research:{task_id}"),
                    None,
                    "planner",
                    "verified",
                    "research_failed",
                    serde_json::json!({
                        "loop_id": loop_id,
                        "task_id": task_id,
                        "error": error,
                    }),
                );
                return ResearchOutcome::Failed;
            }
            Ok(_) => {}
            Err(_) => {
                record_orc_evidence(
                    state,
                    &format!("research:{task_id}"),
                    None,
                    "planner",
                    "verified",
                    "research_failed",
                    serde_json::json!({
                        "loop_id": loop_id,
                        "task_id": task_id,
                        "error": "unknown_loop",
                    }),
                );
                return ResearchOutcome::Failed;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

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
        .reply_with_rules_and_model(frozen_rules.clone(), &planner_model, &prompt)
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
    let mut output = match parse_planner_output(&reply) {
        Ok(output) => output,
        Err(detail) => {
            fail_or_cancel_plan_round(&state, &task_id, &format!("规划回合输出无效：{detail}"));
            return;
        }
    };
    // ORC-4 (Q-R4-01): the Planner asked for research
    // instead of a blueprint — and only on a fresh
    // plan (Q-R4-11: replan rounds never research).
    // R4-J7: a research request wins over any steps
    // the reply also carried; the contract says
    // return only the question. Exactly one research
    // round runs: a second-round reply that asks
    // again is parsed as a normal (failing) blueprint.
    if output.research.is_some() && existing_plan.is_none() {
        let question = output
            .research
            .as_ref()
            .map(|r| r.question.clone())
            .unwrap_or_default();
        let worktree_id = record
            .execute_binding
            .as_ref()
            .map(|b| b.worktree_id.clone());
        let research =
            run_planner_research(&state, &task_id, &file_context, &question, worktree_id).await;
        // A cancelled run stops here (same shell-close
        // as the cancel checkpoint above); a failed
        // research still gets its second Planner call,
        // without a report section (Q-R4-09).
        let prompt2 = match &research {
            ResearchOutcome::Aborted => {
                begin_execution(&state, &task_id);
                crate::server::close_execution_for_task(&state, &task_id, "cancelled");
                return;
            }
            ResearchOutcome::Reported { summary, evidence_id } => format!(
                "{prompt}\n\n调研报告摘要（research 报告，证据行 {}）：\n{}\n请基于以上调研产出蓝图。",
                evidence_id.as_deref().unwrap_or("未记录"),
                summary
            ),
            ResearchOutcome::Failed => prompt.clone(),
        };
        let Some(reply2) = planner_round_reply(
            &state,
            &task_id,
            frozen_rules.clone(),
            &planner_model,
            &prompt2,
        )
        .await
        else {
            return;
        };
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
        output = match parse_planner_output(&reply2) {
            Ok(output) => output,
            Err(detail) => {
                fail_or_cancel_plan_round(&state, &task_id, &format!("规划回合输出无效：{detail}"));
                return;
            }
        };
    }
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
                    false,
                    &[],
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
/// ORC-3 (R3-J16): prepare step 1 of a freshly
/// re-planned plan — the replan route's equivalent of
/// the step-1 prepare `plan_or_generate` runs after
/// its Planner round. Without it the re-planned batch
/// could not resume through `tasks_execute`: a file
/// step would 500 on the missing approval (the old
/// step's `artifact_path` still set) and a worker
/// step would run, but a file step reached through
/// the worker branch is a silent no-op. The task
/// stays `draft_ready`; the user approves + executes
/// exactly like an ORC-2 run.
pub(crate) async fn prepare_replanned_first_step(state: AppState, task_id: &str, plan: &PlanRow) {
    prepare_executor_step(state, task_id, plan, 1).await;
}

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
        ).await;
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
            ).await;
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
            ).await;
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
            ).await;
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
            ).await;
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
                ).await;
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
            ).await;
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
            ).await;
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
        ).await;
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
                ).await;
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
        ).await;
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
            ).await;
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
        ).await;
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
            ).await;
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
            ).await;
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
                ).await;
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
                ).await;
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
                ).await;
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };
    if status != "succeeded" {
        // ORC-3 (Q-R3-02): the worker-run failure is
        // evaluated before its `step_failed` row is
        // written. A match hands back with only the
        // `replan_triggered` row; a cancelled run
        // writes nothing; anything else keeps the
        // original `step_failed` row + `fail()` below.
        // The judge sees the run's outcome in words,
        // not just the `worker_run_failed` tag.
        let judge_failure = format!(
            "worker run 失败，状态 {status}，退出码 {}",
            exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "无".to_string())
        );
        match evaluate_step_failure(
            &state,
            &task_id,
            &plan,
            step_no,
            &step.kind,
            &judge_failure,
            execution_id.as_deref(),
            effect_id.as_deref(),
            Some(&run_id),
            Some(&status),
            exit_code,
        )
        .await
        {
            ReplanEvaluation::Triggered | ReplanEvaluation::Aborted => return,
            ReplanEvaluation::NotTriggered => {}
        }
        // ORC-3 (R3-J18b): P3-C §5.4 — a test step's
        // failure opens no `failed` Inbox row. The
        // worker-run finalizer used to close the
        // Execution suppressed for cargo-test runs,
        // but Executor-step runs no longer get that
        // close (R3-J18: the step logic owns it), so
        // the step logic suppresses here instead: close
        // the Execution as failed with no Inbox row
        // first, and `fail()` below then finds it
        // already terminal, so its close is a no-op.
        // Any other template keeps the unsuppressed
        // close (the finalizer's old behaviour).
        if template == "cargo-test" {
            if let Some(execution_id) = execution_id.as_deref() {
                crate::server::close_execution_no_inbox(&state, execution_id, "failed");
            }
        }
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

// --- ORC-5 swarm drive (Q-R5-04/05/06/07/10) ---
//
// The sweeper's new pass: after `start_due_worker_runs`
// in the same 10 s tick (appendix pin), it drives every
// non-terminal swarm one step. One pass per tick,
// swarms in creation order, each drive awaited in
// turn — a model-backed file-write prepare can stretch
// a tick, but it never blocks the other passes of the
// SAME tick (they already ran), and no lock is held
// across an await.
//
// State machine (swarm_store.rs): created (idle,
// fanout not yet called) → fanning_out (copy
// add-runs) → running (prepare + approvals +
// executions) → verifying (cross-check) → merging
// (diffs applied to the main worktree) → completed /
// failed / conflict (terminal). The task stays
// `draft_ready` through the whole swarm — there is
// NO `draft_ready → running` edge — so the swarm's
// own status carries progress; completion walks the
// legal `draft_ready → completed` edge, failure the
// legal `draft_ready → failed` edge.

/// ORC-5 (Q-R5-04): the swarm sweeper pass. Called
/// from `spawn_assignment_sweeper` after
/// `start_due_worker_runs`, same tick. Directly
/// callable in tests (the test harness serves with
/// no sweeper — the `e2d_long_tasks` precedent of
/// calling the pass directly).
pub(crate) async fn drive_ready_swarms(state: AppState) {
    let Some(sqlite) = &state.sqlite else {
        return;
    };
    let swarms = {
        let conn = sqlite.lock().expect("sqlite lock");
        match crate::swarm_store::list_swarms(&conn) {
            Ok(rows) => rows,
            Err(detail) => {
                eprintln!("warning: swarm drive list failed ({detail})");
                return;
            }
        }
    };
    for swarm in swarms {
        if matches!(
            swarm.status.as_str(),
            "completed" | "failed" | "conflict"
        ) {
            continue;
        }
        drive_swarm(state.clone(), swarm).await;
    }
}

/// The synthetic effect_id for swarm Evidence rows.
/// There is no real Effect behind a fan-out /
/// cross-check / merge row — the `executor:{task}:{step}`
/// and `research:{task}` precedents use the same
/// synthetic-label shape, and the row's `observed`
/// JSON carries the swarm + step ids.
fn swarm_effect_id(swarm_id: &str, step_no: Option<i64>) -> String {
    match step_no {
        Some(step_no) => format!("swarm:{swarm_id}:{step_no}"),
        None => format!("swarm:{swarm_id}"),
    }
}

/// Persist a swarm's steps + status. Logs and
/// returns false on failure (the drive step is
/// skipped, not forced).
fn swarm_persist(
    state: &AppState,
    swarm_id: &str,
    steps: &[SwarmStep],
    status: &str,
) -> bool {
    let Some(sqlite) = &state.sqlite else {
        return false;
    };
    let mut conn = sqlite.lock().expect("sqlite lock");
    match crate::swarm_store::update_swarm(&mut conn, swarm_id, steps, status) {
        Ok(()) => true,
        Err(detail) => {
            eprintln!("warning: swarm {swarm_id} persist failed ({detail})");
            false
        }
    }
}

/// The copy's canonical directory, or None when the
/// worktree record is gone.
fn swarm_copy_root(state: &AppState, worktree_id: &str) -> Option<PathBuf> {
    let worktree = state
        .worktrees
        .lock()
        .expect("worktree lock")
        .get(worktree_id)
        .cloned()?;
    worktree_canonical_root(state, &worktree)
}

/// ORC-2 `read_step_file` against a copy root
/// instead of `files_root`: same per-file cap, same
/// UTF-8 tail trim, same empty-on-missing rule (the
/// step may be creating the file).
fn read_step_file_at(root: &std::path::Path, path: &str) -> String {
    let Ok(full) = resolve_file(root, path) else {
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

/// Run git directly (ORC-5 R5-J5): the merge's
/// `git diff` / `git apply` / `git worktree remove`
/// / `git branch -D` run as direct `git` Commands
/// in the gateway, NOT via worker runs (a run tail
/// truncates, and a full diff cannot be carried in
/// one) and NOT via a new `git-worktree-remove`
/// template (it would need an approval no one can
/// give after merge, and conflicts with "no new loop
/// tools"). None when git is not configured.
fn swarm_git(
    state: &AppState,
    args: &[String],
    cwd: &std::path::Path,
) -> Option<crate::worker::SyncRunOutput> {
    let git = state.git_path.clone()?;
    let mut argv = vec![git.to_string_lossy().into_owned()];
    argv.extend(args.iter().cloned());
    Some(crate::worker::run_command_sync(
        state,
        &argv,
        cwd,
        &git,
        std::time::Duration::from_secs(30),
    ))
}

/// One swarm Evidence row (Q-R5-11). `observed`
/// carries ids + counts + hashes only — never step
/// text or board text.
fn swarm_evidence(
    state: &AppState,
    swarm_id: &str,
    step_no: Option<i64>,
    execution_id: Option<&str>,
    verdict: &str,
    reason: &str,
    observed: serde_json::Value,
) {
    record_orc_evidence(
        state,
        &swarm_effect_id(swarm_id, step_no),
        execution_id,
        "swarm",
        verdict,
        reason,
        observed,
    );
}

async fn drive_swarm(state: AppState, swarm: SwarmRow) {
    let task_id = swarm.task_id.clone();
    let Some(task) = state
        .tasks
        .lock()
        .expect("task lock")
        .get(&task_id)
        .cloned()
    else {
        // The task is gone (deleted mid-swarm): the
        // swarm cannot hand back to anything. Fail it
        // with a board note and stop.
        let steps = swarm.steps().unwrap_or_default();
        crate::routes_swarm::swarm_board_write(
            &state,
            &swarm.swarm_id,
            "swarm",
            "conflict",
            &format!("task_missing:{}", swarm.task_id),
        );
        swarm_persist(&state, &swarm.swarm_id, &steps, "failed");
        return;
    };
    let plan = {
        let Some(sqlite) = &state.sqlite else {
            return;
        };
        let conn = sqlite.lock().expect("sqlite lock");
        crate::plan_store::get_plan(&conn, &swarm.plan_id)
            .ok()
            .flatten()
    };
    let Some(plan) = plan else {
        let steps = swarm.steps().unwrap_or_default();
        crate::routes_swarm::swarm_board_write(
            &state,
            &swarm.swarm_id,
            "swarm",
            "conflict",
            &format!("plan_missing:{}", swarm.plan_id),
        );
        swarm_persist(&state, &swarm.swarm_id, &steps, "failed");
        fail(&state, &task_id, "蓝图缺失，无法继续 Swarm");
        return;
    };
    let Some(steps) = swarm.steps().ok() else {
        eprintln!(
            "warning: swarm {} steps undecodable, drive skipped",
            swarm.swarm_id
        );
        return;
    };
    match swarm.status.as_str() {
        // `created` is idle: fanout is a user action,
        // the drive has nothing to do.
        "created" => {}
        "fanning_out" => {
            drive_swarm_fanning_out(&state, &swarm, steps, &task, &plan).await;
        }
        "running" => {
            drive_swarm_running(&state, &swarm, steps, &task, &plan).await;
        }
        "verifying" => {
            drive_swarm_verifying(&state, &swarm, steps, &task, &plan).await;
        }
        "merging" => {
            drive_swarm_merging(&state, &swarm, steps, &task, &plan).await;
        }
        _ => {}
    }
}

/// Fan-out failure terminal: board `conflict` +
/// swarm `failed` + task `fail()` (Q-R5-12:
/// fan-out failure → `failed` Inbox row, via the
/// Execution close inside `fail()`).
fn swarm_terminal_failed(
    state: &AppState,
    swarm: &SwarmRow,
    steps: &[SwarmStep],
    task_id: &str,
    board_body: &str,
    error: &str,
) {
    crate::routes_swarm::swarm_board_write(state, &swarm.swarm_id, "swarm", "conflict", board_body);
    swarm_persist(state, &swarm.swarm_id, steps, "failed");
    fail(state, task_id, error);
}

/// Verification / merge failure terminal: board
/// `conflict` + `swarm_conflict` Evidence + ORC-3
/// hand-back (task `draft_ready` + `needs_review`
/// Execution close → one `review_required` Inbox
/// row, Q-R5-12) + swarm `conflict`.
/// The conflict detail rides in one struct so the
/// terminal stays under clippy's argument limit.
struct SwarmConflict<'a> {
    execution_id: Option<&'a str>,
    step_no: Option<i64>,
    board_body: String,
    observed: serde_json::Value,
}

fn swarm_terminal_conflict(
    state: &AppState,
    swarm: &SwarmRow,
    steps: &[SwarmStep],
    task_id: &str,
    conflict: SwarmConflict<'_>,
) {
    crate::routes_swarm::swarm_board_write(
        state,
        &swarm.swarm_id,
        "swarm",
        "conflict",
        &conflict.board_body,
    );
    swarm_evidence(
        state,
        &swarm.swarm_id,
        conflict.step_no,
        conflict.execution_id,
        "mismatch",
        "swarm_conflict",
        conflict.observed,
    );
    hand_back(state, task_id, conflict.execution_id, None);
    swarm_persist(state, &swarm.swarm_id, steps, "conflict");
}

async fn drive_swarm_fanning_out(
    state: &AppState,
    swarm: &SwarmRow,
    mut steps: Vec<SwarmStep>,
    task: &TaskFileRecord,
    _plan: &PlanRow,
) {
    let task_id = task.task_id.clone();
    let execution_id = execution_for_task(state, &task_id);
    let mut failed: Option<(i64, String)> = None;
    let mut all_succeeded = true;
    for step in steps.iter() {
        let Some(add_run_id) = &step.add_run_id else {
            all_succeeded = false;
            continue;
        };
        let run = {
            let Some(sqlite) = &state.sqlite else {
                return;
            };
            let conn = sqlite.lock().expect("sqlite lock");
            get_worker_run(&conn, add_run_id).ok().flatten()
        };
        match run {
            None => {
                failed = Some((step.step_no, "add_run_missing".to_string()));
                break;
            }
            Some(row) if row.status == "succeeded" => {}
            Some(row) if is_terminal_run_status(&row.status) => {
                failed = Some((step.step_no, format!("add_run_{}", row.status)));
                break;
            }
            Some(_) => {
                all_succeeded = false;
            }
        }
    }
    if let Some((step_no, reason)) = failed {
        swarm_terminal_failed(
            state,
            swarm,
            &steps,
            &task_id,
            &format!("fanout_failed:{step_no}:{reason}"),
            &format!("Swarm 副本创建失败（步骤 {step_no}）：{reason}"),
        );
        return;
    }
    if !all_succeeded {
        return;
    }
    // All add-runs succeeded: link the Worktree
    // records the run finisher created (branch
    // `swarm-{swarm_id}-{worker_id}`, R5-J4).
    for step in steps.iter_mut() {
        let branch = format!("swarm-{}-{}", swarm.swarm_id, step.worker_id);
        let worktree_id = state
            .worktrees
            .lock()
            .expect("worktree lock")
            .values()
            .find(|w| w.branch_label == branch)
            .map(|w| w.worktree_id.clone());
        match worktree_id {
            Some(id) => step.worktree_id = Some(id),
            None => {
                failed = Some((step.step_no, "worktree_record_missing".to_string()));
                break;
            }
        }
    }
    if let Some((step_no, reason)) = failed {
        swarm_terminal_failed(
            state,
            swarm,
            &steps,
            &task_id,
            &format!("fanout_failed:{step_no}:{reason}"),
            &format!("Swarm 副本创建失败（步骤 {step_no}）：{reason}"),
        );
        return;
    }
    for step in steps.iter() {
        swarm_evidence(
            state,
            &swarm.swarm_id,
            Some(step.step_no),
            execution_id.as_deref(),
            "verified",
            "swarm_fanout",
            serde_json::json!({
                "task_id": task_id,
                "swarm_id": swarm.swarm_id,
                "step_no": step.step_no,
                "worker_id": step.worker_id,
                "worktree_id": step.worktree_id,
            }),
        );
    }
    crate::routes_swarm::swarm_board_write(state, &swarm.swarm_id, "swarm", "status", "copies_ready");
    swarm_persist(state, &swarm.swarm_id, &steps, "running");
}

/// ORC-2 `prepare_file_write_step` against a copy:
/// the Executor model generates the content from
/// the copy's current file (same prompt shape, same
/// model call), and the fs.write approval is built
/// from path + content. The content is stored in
/// the step (SQLite-only, base64) — the drive's
/// apply reads it back, and the cross-check hashes
/// it. A model failure fails the step (aggregated
/// later, Q-R5-10 — the swarm does NOT fail here).
async fn swarm_prepare_file_step(
    state: &AppState,
    step: &mut SwarmStep,
    plan_step: &crate::plan_store::PlanStep,
    plan: &PlanRow,
    task: &TaskFileRecord,
) {
    use base64::Engine;
    let (_planner_model, executor_model) = role_models(state);
    let Some(copy_root) = step
        .worktree_id
        .as_deref()
        .and_then(|id| swarm_copy_root(state, id))
    else {
        step.run_status = "failed".to_string();
        step.failure = Some("副本目录缺失".to_string());
        return;
    };
    let current = read_step_file_at(&copy_root, &plan_step.path);
    let prompt = format!(
        "{EXECUTOR_PROMPT_PREFIX}步骤 {step_no}（共 {total} 步），蓝图 {plan_id}：\n- 文件：{path}\n- 符号：{symbol}\n- 改动：{change}\n- 验证：{verify_cmd}，预期：{expected}\n\n文件当前内容（不存在时为空）：\n{current}\n\n只返回改动后的文件完整内容，不要 markdown 围栏、不要解释文字。",
        step_no = plan_step.step_no,
        total = plan.steps().map(|s| s.len()).unwrap_or(0),
        plan_id = plan.plan_id,
        path = plan_step.path,
        symbol = plan_step.symbol,
        change = plan_step.change,
        verify_cmd = plan_step.verify_cmd,
        expected = plan_step.expected,
        current = current,
    );
    let frozen_rules = frozen_rules_for(state, &task.task_id);
    let content = match state
        .deepseek
        .reply_with_rules_and_model(frozen_rules, &executor_model, &prompt)
        .await
    {
        DeepseekOutcome::Disabled => {
            step.run_status = "failed".to_string();
            step.failure = Some("模型未配置".to_string());
            return;
        }
        DeepseekOutcome::Denied(pattern) => {
            step.run_status = "failed".to_string();
            step.failure = Some(format!("网络规则拒绝模型调用：{pattern}"));
            return;
        }
        DeepseekOutcome::Failed(_) => {
            step.run_status = "failed".to_string();
            step.failure = Some("模型调用失败".to_string());
            return;
        }
        DeepseekOutcome::Replied(text) => {
            if text.trim().is_empty() {
                step.run_status = "failed".to_string();
                step.failure = Some("执行回合未返回文件内容".to_string());
                return;
            }
            text
        }
    };
    // Cancel checkpoint (the serial prepare's rule):
    // a cancelled run discards the step content and
    // creates no approval; the next tick re-prepares.
    if !task_is_active(state, &task.task_id) {
        step.run_status = "pending".to_string();
        return;
    }
    let approval_id = match create_bound_write_approval(state, &plan_step.path, content.as_bytes())
    {
        Ok(id) => id,
        Err(error) => {
            step.run_status = "failed".to_string();
            step.failure = Some(error);
            return;
        }
    };
    step.content_base64 = Some(
        base64::engine::general_purpose::STANDARD.encode(content.as_bytes()),
    );
    step.content_sha256 = Some(operation_binding::sha256_hex(content.as_bytes()));
    step.approval_id = Some(approval_id);
    step.approval_kind = Some("fs.write".to_string());
    step.run_status = "waiting_approval".to_string();
}

/// ORC-2 `prepare_worker_step` against a copy: the
/// step's `verify_cmd` maps to a worker-run template
/// (J7); effect commands get a bound worker.run
/// approval, reads get none (and execute at once).
fn swarm_prepare_command_step(
    state: &AppState,
    step: &mut SwarmStep,
    plan_step: &crate::plan_store::PlanStep,
) {
    let Some(worktree_id) = step.worktree_id.clone() else {
        step.run_status = "failed".to_string();
        step.failure = Some("副本未就绪".to_string());
        return;
    };
    let (template, params) = match resolve_verify_command(&plan_step.verify_cmd) {
        Ok(resolved) => resolved,
        Err(detail) => {
            step.run_status = "failed".to_string();
            step.failure = Some(detail);
            return;
        }
    };
    if is_effect_command(&template) {
        match create_bound_worker_approval(state, &worktree_id, &template, &params) {
            Ok(id) => {
                step.approval_id = Some(id);
                step.approval_kind = Some("worker.run".to_string());
                step.run_status = "waiting_approval".to_string();
            }
            Err(detail) => {
                step.run_status = "failed".to_string();
                step.failure = Some(detail);
            }
        }
    } else {
        // A read step needs no approval: it is
        // executable now (the execute phase creates
        // its run on the same tick).
        step.run_status = "running".to_string();
    }
}

/// `apply_file_write_step` against a copy: consume
/// the fs.write approval bound to a planned swarm
/// Effect, write the stored content into the copy,
/// drive the Effect to `succeeded`, and write the
/// explicit write Evidence row (source `write`,
/// reason `written`, `{kind:"file", sha256, len}` —
/// the cross-check compares against this hash).
/// The Effect key is `swarm.apply:{swarm_id}:{path}`
/// (the swarm id in the scope, so two swarms
/// writing the same path with different content do
/// not collide on the idempotency key).
async fn swarm_apply_file_step(
    state: &AppState,
    swarm: &SwarmRow,
    step: &mut SwarmStep,
    plan_step: &crate::plan_store::PlanStep,
    execution_id: Option<&str>,
) -> Result<String, String> {
    let Some(approval_id) = step.approval_id.clone() else {
        return Err("步骤审批缺失".to_string());
    };
    let Some(content_base64) = step.content_base64.clone() else {
        return Err("步骤内容缺失".to_string());
    };
    use base64::Engine;
    let Ok(content) = base64::engine::general_purpose::STANDARD.decode(&content_base64) else {
        return Err("步骤内容解码失败".to_string());
    };
    let Some(digest) =
        operation_binding::write_digest(&plan_step.path, &content, false)
    else {
        return Err("产物路径无效".to_string());
    };
    let scope = format!("{}:{}", swarm.swarm_id, plan_step.path);
    if crate::server::idempotency_key_missing(execution_id, Some(&digest)) {
        return Err("effect_idempotency_unavailable".to_string());
    }
    let effect = match check_idempotency(
        state,
        execution_id,
        "swarm",
        "apply",
        &scope,
        Some(&digest),
        None,
    ) {
        // A retry after a crash between the write and
        // the step persist: the write already happened
        // — replay without consuming the approval again
        // or writing twice.
        IdempotencyDecision::Replay(effect) if effect.status == "succeeded" => {
            return Ok(effect.effect_id);
        }
        IdempotencyDecision::Replay(effect) => {
            return Err(format!("effect already {}", effect.status));
        }
        IdempotencyDecision::Blocked(other) => {
            if other.status == "planned"
                && other.approval_id.as_deref() == Some(approval_id.as_str())
            {
                other
            } else {
                return Err(format!(
                    "effect_idempotency_blocked:{}:{}",
                    other.effect_id, other.status
                ));
            }
        }
        IdempotencyDecision::Unavailable => {
            return Err("sqlite_unavailable".to_string());
        }
        IdempotencyDecision::Proceed => record_effect(
            state,
            NewEffect {
                execution_id: execution_id.map(str::to_string),
                capability: "swarm".to_string(),
                operation: "apply".to_string(),
                scope,
                input_digest: Some(digest.clone()),
                approval_id: Some(approval_id.clone()),
                status: "planned".to_string(),
            },
        ),
    };
    if let Err(response) = consume_bound_approval_for_effect(
        state,
        "fs.write",
        &Some(approval_id.clone()),
        &digest,
        Some(&effect.effect_id),
    ) {
        let status = response.status();
        return Err(format!("fs.write 审批消费失败（{status}）"));
    }
    if let Err(response) = start_bound_effect(state, &effect) {
        let status = response.status();
        return Err(format!("Effect 启动失败（{status}）"));
    }
    let Some(copy_root) = step
        .worktree_id
        .as_deref()
        .and_then(|id| swarm_copy_root(state, id))
    else {
        let _ = transition_effect(state, &effect.effect_id, "failed", None, None);
        return Err("副本目录缺失".to_string());
    };
    let Some(normalized) = operation_binding::normalize_path(&plan_step.path) else {
        let _ = transition_effect(state, &effect.effect_id, "failed", None, None);
        return Err("产物路径无效".to_string());
    };
    let Ok(full) = resolve_file(&copy_root, &normalized) else {
        let _ = transition_effect(state, &effect.effect_id, "failed", None, None);
        return Err("path escapes file root".to_string());
    };
    if let Some(parent) = full.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            let _ = transition_effect(state, &effect.effect_id, "failed", None, None);
            return Err("cannot create parent directory".to_string());
        }
    }
    if std::fs::write(&full, &content).is_err() {
        let _ = transition_effect(state, &effect.effect_id, "failed", None, None);
        return Err("产物写入失败".to_string());
    }
    let _ = transition_effect(state, &effect.effect_id, "succeeded", None, None);
    let sha256 = operation_binding::sha256_hex(&content);
    record_orc_evidence(
        state,
        &effect.effect_id,
        execution_id,
        "write",
        "verified",
        "written",
        serde_json::json!({
            "kind": "file",
            "sha256": sha256,
            "len": content.len(),
        }),
    );
    Ok(effect.effect_id)
}

async fn drive_swarm_running(
    state: &AppState,
    swarm: &SwarmRow,
    mut steps: Vec<SwarmStep>,
    task: &TaskFileRecord,
    plan: &PlanRow,
) {
    let task_id = task.task_id.clone();
    let execution_id = execution_for_task(state, &task_id);
    let plan_steps = plan.steps().unwrap_or_default();
    // Phase A: prepare each pending step (model call
    // for file writes, verify_cmd resolution +
    // approval for effect commands). A prepare
    // failure fails the step only — aggregation
    // waits until every step is terminal (Q-R5-10).
    for index in 0..steps.len() {
        if steps[index].run_status != "pending" {
            continue;
        }
        if steps[index].worktree_id.is_none() {
            steps[index].run_status = "failed".to_string();
            steps[index].failure = Some("副本未就绪".to_string());
            continue;
        }
        let Some(plan_step) = plan_steps
            .iter()
            .find(|s| s.step_no == steps[index].step_no)
        else {
            steps[index].run_status = "failed".to_string();
            steps[index].failure = Some("蓝图步骤缺失".to_string());
            continue;
        };
        steps[index].run_status = "preparing".to_string();
        if !swarm_persist(state, &swarm.swarm_id, &steps, "running") {
            return;
        }
        match plan_step.kind.as_str() {
            "file_write" => {
                swarm_prepare_file_step(state, &mut steps[index], plan_step, plan, task).await;
            }
            "command" => {
                swarm_prepare_command_step(state, &mut steps[index], plan_step);
            }
            _ => {
                steps[index].run_status = "failed".to_string();
                steps[index].failure = Some("不支持的步骤类型".to_string());
            }
        }
        if !swarm_persist(state, &swarm.swarm_id, &steps, "running") {
            return;
        }
    }
    // Phase B: execute. An Approved approval starts
    // its step (file write applies into the copy,
    // command spawns its worker run); a decided but
    // not Approved approval (rejected / edited)
    // fails the step — the approval is digest-bound
    // to the prepared command, an edit breaks the
    // binding. Approval-free read steps run at once.
    // Steps with a run already poll it.
    for index in 0..steps.len() {
        match steps[index].run_status.as_str() {
            "waiting_approval" => {
                let Some(approval_id) = steps[index].approval_id.clone() else {
                    steps[index].run_status = "failed".to_string();
                    steps[index].failure = Some("步骤审批缺失".to_string());
                    continue;
                };
                match approval_status(state, &approval_id) {
                    Some(ApprovalStatus::Approved) => {}
                    // Pending / NotRequired / unknown: not
                    // decided yet — wait for the next tick,
                    // do not fail the step. Only a decided
                    // non-approval (Rejected / Edited — an
                    // edit breaks the digest binding) fails
                    // it.
                    Some(ApprovalStatus::Pending)
                    | Some(ApprovalStatus::NotRequired)
                    | None => {
                        continue;
                    }
                    Some(_) => {
                        steps[index].run_status = "failed".to_string();
                        steps[index].failure = Some("审批未通过".to_string());
                        continue;
                    }
                }
                let Some(plan_step) = plan_steps
                    .iter()
                    .find(|s| s.step_no == steps[index].step_no)
                else {
                    steps[index].run_status = "failed".to_string();
                    steps[index].failure = Some("蓝图步骤缺失".to_string());
                    continue;
                };
                if plan_step.kind == "file_write" {
                    match swarm_apply_file_step(
                        state,
                        swarm,
                        &mut steps[index],
                        plan_step,
                        execution_id.as_deref(),
                    )
                    .await
                    {
                        Ok(effect_id) => {
                            steps[index].effect_id = Some(effect_id);
                            steps[index].run_status = "succeeded".to_string();
                        }
                        Err(detail) => {
                            steps[index].run_status = "failed".to_string();
                            steps[index].failure = Some(detail);
                        }
                    }
                } else {
                    steps[index] = swarm_execute_command_step(
                        state,
                        swarm,
                        steps[index].clone(),
                        plan_step,
                        execution_id.clone(),
                    )
                    .await;
                }
            }
            "running" => {
                if steps[index].run_id.is_some() {
                    steps[index] = swarm_poll_command_step(state, steps[index].clone());
                } else {
                    let Some(plan_step) = plan_steps
                        .iter()
                        .find(|s| s.step_no == steps[index].step_no)
                    else {
                        steps[index].run_status = "failed".to_string();
                        steps[index].failure = Some("蓝图步骤缺失".to_string());
                        continue;
                    };
                    steps[index] = swarm_execute_command_step(
                        state,
                        swarm,
                        steps[index].clone(),
                        plan_step,
                        execution_id.clone(),
                    )
                    .await;
                }
            }
            _ => {}
        }
        if !swarm_persist(state, &swarm.swarm_id, &steps, "running") {
            return;
        }
    }
    // Phase C: every step terminal → cross-check
    // next tick.
    if steps
        .iter()
        .all(|s| matches!(s.run_status.as_str(), "succeeded" | "failed"))
    {
        swarm_persist(state, &swarm.swarm_id, &steps, "verifying");
    }
}

/// Spawn a command step's worker run (approval
/// already Approved, or a read step with none).
/// The run carries the swarm_id, so it bypasses
/// the one-run-per-Execution guard — the N steps
/// run in parallel on the task's Execution.
async fn swarm_execute_command_step(
    state: &AppState,
    swarm: &SwarmRow,
    mut step: SwarmStep,
    plan_step: &crate::plan_store::PlanStep,
    execution_id: Option<String>,
) -> SwarmStep {
    let Some(worktree_id) = step.worktree_id.clone() else {
        step.run_status = "failed".to_string();
        step.failure = Some("副本未就绪".to_string());
        return step;
    };
    let (template, params) = match resolve_verify_command(&plan_step.verify_cmd) {
        Ok(resolved) => resolved,
        Err(detail) => {
            step.run_status = "failed".to_string();
            step.failure = Some(detail);
            return step;
        }
    };
    match create_worker_run_internal(
        state,
        WorkerRunCreateDto::internal(
            worktree_id,
            template,
            Some(params),
            execution_id,
            step.approval_id.clone(),
        )
        .with_swarm_id(swarm.swarm_id.clone()),
    )
    .await
    {
        Ok(row) => {
            step.run_id = Some(row.run_id);
            step.run_status = "running".to_string();
        }
        Err(error) => {
            step.run_status = "failed".to_string();
            step.failure = Some(format!("worker run 创建失败：{}", error.code));
        }
    }
    step
}

/// Poll a command step's run: succeeded stays
/// succeeded; any other terminal status fails the
/// step (the run's terminal Evidence row carries
/// the detail; the failure string names the status
/// + exit code).
fn swarm_poll_command_step(state: &AppState, mut step: SwarmStep) -> SwarmStep {
    let Some(run_id) = step.run_id.clone() else {
        return step;
    };
    let run = {
        let Some(sqlite) = &state.sqlite else {
            return step;
        };
        let conn = sqlite.lock().expect("sqlite lock");
        get_worker_run(&conn, &run_id).ok().flatten()
    };
    let Some(row) = run else {
        step.run_status = "failed".to_string();
        step.failure = Some("worker run 记录缺失".to_string());
        return step;
    };
    if row.status == "succeeded" {
        step.run_status = "succeeded".to_string();
        step.effect_id = row.effect_id;
    } else if is_terminal_run_status(&row.status) {
        step.run_status = "failed".to_string();
        step.failure = Some(format!(
            "worker run {}（exit {:?}）",
            row.status, row.exit_code
        ));
    }
    step
}

async fn drive_swarm_verifying(
    state: &AppState,
    swarm: &SwarmRow,
    steps: Vec<SwarmStep>,
    task: &TaskFileRecord,
    plan: &PlanRow,
) {
    let task_id = task.task_id.clone();
    let execution_id = execution_for_task(state, &task_id);
    // Cross-check each succeeded step (R5-J1: the
    // verdict is computed deterministically by the
    // gateway — a model cannot reliably compute
    // digests, and fs.read truncates long files).
    // file_write: the copy's file sha256 vs the
    // write Evidence hash stored on the step.
    // command: the run succeeded, so the terminal
    // record matches — `record_match`.
    let mut mismatch: Option<(i64, String)> = None;
    for step in steps.iter().filter(|s| s.run_status == "succeeded") {
        let plan_step = plan
            .steps()
            .ok()
            .and_then(|all| all.into_iter().find(|s| s.step_no == step.step_no));
        let verdict = match plan_step.as_ref().map(|s| s.kind.as_str()) {
            Some("file_write") => {
                let actual = step
                    .worktree_id
                    .as_deref()
                    .and_then(|id| swarm_copy_root(state, id))
                    .and_then(|root| {
                        let path = plan_step.as_ref().map(|s| s.path.clone()).unwrap_or_default();
                        let Ok(full) = resolve_file(&root, &path) else {
                            return None;
                        };
                        std::fs::read(full).ok()
                    })
                    .map(|bytes| operation_binding::sha256_hex(&bytes));
                match (actual, &step.content_sha256) {
                    (Some(actual), Some(expected)) if actual == *expected => "hash_match",
                    _ => "mismatch",
                }
            }
            Some("command") => "record_match",
            _ => "mismatch",
        };
        swarm_evidence(
            state,
            &swarm.swarm_id,
            Some(step.step_no),
            execution_id.as_deref(),
            verdict,
            "swarm_cross_check",
            serde_json::json!({
                "task_id": task_id,
                "swarm_id": swarm.swarm_id,
                "step_no": step.step_no,
                "worker_id": step.worker_id,
                "verdict": verdict,
            }),
        );
        crate::routes_swarm::swarm_board_write(
            state,
            &swarm.swarm_id,
            &step.worker_id,
            "result",
            &format!("cross_check:{}:{verdict}", step.step_no),
        );
        if verdict == "mismatch" && mismatch.is_none() {
            mismatch = Some((step.step_no, "cross_check_mismatch".to_string()));
        }
    }
    if let Some((step_no, reason)) = mismatch {
        swarm_terminal_conflict(
            state,
            swarm,
            &steps,
            &task_id,
            SwarmConflict {
                execution_id: execution_id.as_deref(),
                step_no: Some(step_no),
                board_body: format!("cross_check_mismatch:{step_no}"),
                observed: serde_json::json!({
                    "task_id": task_id,
                    "swarm_id": swarm.swarm_id,
                    "step_no": step_no,
                    "kind": "cross_check",
                    "reason": reason,
                }),
            },
        );
        return;
    }
    // No mismatch: a failed step goes through the
    // ORC-3 judge exactly once (Q-R5-10) — the
    // judge hands back or fails the task; either
    // way the swarm is done.
    if let Some(failed) = steps.iter().find(|s| s.run_status == "failed") {
        let plan_step = plan
            .steps()
            .ok()
            .and_then(|all| all.into_iter().find(|s| s.step_no == failed.step_no));
        let (run_status, exit_code) = failed
            .run_id
            .as_deref()
            .and_then(|run_id| {
                let sqlite = state.sqlite.as_ref()?;
                let conn = sqlite.lock().expect("sqlite lock");
                get_worker_run(&conn, run_id).ok().flatten()
            })
            .map(|row| (Some(row.status), row.exit_code))
            .unwrap_or((None, None));
        fail_step(
            state,
            &task_id,
            plan,
            failed.step_no,
            plan_step
                .as_ref()
                .map(|s| s.kind.as_str())
                .unwrap_or("unknown"),
            failed.failure.as_deref().unwrap_or("步骤失败"),
            execution_id.as_deref(),
            failed.run_id.as_deref(),
            run_status.as_deref(),
            exit_code,
        )
        .await;
        swarm_persist(state, &swarm.swarm_id, &steps, "failed");
        return;
    }
    swarm_persist(state, &swarm.swarm_id, &steps, "merging");
}

async fn drive_swarm_merging(
    state: &AppState,
    swarm: &SwarmRow,
    steps: Vec<SwarmStep>,
    task: &TaskFileRecord,
    plan: &PlanRow,
) {
    let task_id = task.task_id.clone();
    let execution_id = execution_for_task(state, &task_id);
    let Some(binding) = task.execute_binding.clone() else {
        swarm_terminal_failed(
            state,
            swarm,
            &steps,
            &task_id,
            "merge_failed:binding_missing",
            "Swarm 合并失败：任务未绑定工作区",
        );
        return;
    };
    let Some(parent) = state
        .worktrees
        .lock()
        .expect("worktree lock")
        .get(&binding.worktree_id)
        .cloned()
    else {
        swarm_terminal_failed(
            state,
            swarm,
            &steps,
            &task_id,
            "merge_failed:parent_missing",
            "Swarm 合并失败：主工作区记录缺失",
        );
        return;
    };
    let Some(main_root) = worktree_canonical_root(state, &parent) else {
        swarm_terminal_failed(
            state,
            swarm,
            &steps,
            &task_id,
            "merge_failed:parent_root_missing",
            "Swarm 合并失败：主工作区目录缺失",
        );
        return;
    };
    if state.git_path.is_none() {
        swarm_terminal_failed(
            state,
            swarm,
            &steps,
            &task_id,
            "merge_failed:git_unavailable",
            "Swarm 合并失败：git 不可用",
        );
        return;
    }
    // Merge order is the plan's own blueprint order
    // (step_no ascending, R5-J1) — no separate
    // Planner round for ordering.
    let parent_branch = parent.branch_label.clone();
    let mut merged_steps: Vec<i64> = Vec::new();
    let mut conflict: Option<(i64, String)> = None;
    for step in steps.iter().filter(|s| s.run_status == "succeeded") {
        let Some(copy_root) = step
            .worktree_id
            .as_deref()
            .and_then(|id| swarm_copy_root(state, id))
        else {
            conflict = Some((step.step_no, "copy_missing".to_string()));
            break;
        };
        // The copy's full change vs the branch point:
        // `git diff <base>` covers both committed-on-
        // branch and uncommitted working-tree changes
        // in one patch.
        let base = match swarm_git(
            state,
            &[
                "merge-base".to_string(),
                "HEAD".to_string(),
                parent_branch.clone(),
            ],
            &copy_root,
        ) {
            Some(out) if out.exit_code == Some(0) => out.stdout.trim().to_string(),
            _ => {
                conflict = Some((step.step_no, "merge_base_failed".to_string()));
                break;
            }
        };
        let patch = match swarm_git(state, &["diff".to_string(), base], &copy_root) {
            Some(out) if out.exit_code == Some(0) => out.stdout,
            _ => {
                conflict = Some((step.step_no, "diff_failed".to_string()));
                break;
            }
        };
        if patch.is_empty() {
            merged_steps.push(step.step_no);
            continue;
        }
        // `git apply` reads the patch from a file
        // (`run_command_sync` has no stdin): the patch
        // goes to a temp file and is removed right
        // after the apply, success or not.
        let patch_path = std::env::temp_dir().join(format!(
            "bu2agent-swarm-{}-{}.patch",
            swarm.swarm_id, step.step_no
        ));
        if std::fs::write(&patch_path, &patch).is_err() {
            conflict = Some((step.step_no, "patch_write_failed".to_string()));
            break;
        }
        // R5-F1: pin `-c core.autocrlf=false` on the
        // apply ONLY. Git for Windows defaults
        // `core.autocrlf=true`, and an unpinned apply
        // converts the merged content to CRLF (CI #167:
        // both Windows orc_swarm failures expected LF,
        // got CRLF). The pin must NOT extend to the
        // `git diff` above: with autocrlf=false the
        // diff compares raw bytes, so every untouched
        // file the copy checked out as CRLF appears as
        // a full-file rewrite (probe: 3 files, 4 CRs
        // in the patch, vs 1 file, 0 CRs unpinned).
        // It must NOT extend to the shared
        // `git-worktree-add` template either: that
        // template serves every worktree add, and its
        // positional argv consumers (worker.rs
        // `argv.get(3)`/`argv.get(5)`) assume the
        // current shape. The copy's CRLF checkout is
        // harmless — the diff normalizes it, and the
        // file-write step overwrites stepped files
        // with verbatim LF bytes.
        //
        // Known behavior, not a silent change: if the
        // main worktree's target file is itself CRLF,
        // the pinned apply fails to match the LF patch
        // (probe: `patch does not apply`) and the
        // swarm stops here with `apply_failed` ->
        // hand-back, main untouched. The unpinned
        // apply would have succeeded and landed CRLF,
        // which is exactly the CI failure's shape.
        let applied = swarm_git(
            state,
            &[
                "-c".to_string(),
                "core.autocrlf=false".to_string(),
                "apply".to_string(),
                "--".to_string(),
                patch_path.to_string_lossy().into_owned(),
            ],
            &main_root,
        );
        let _ = std::fs::remove_file(&patch_path);
        match applied {
            // `git apply` is atomic: a failing hunk
            // applies nothing, so the main worktree
            // is untouched and the swarm can stop
            // here, honestly, with the copies left
            // for inspection.
            Some(out) if out.exit_code == Some(0) => {
                merged_steps.push(step.step_no);
            }
            _ => {
                conflict = Some((step.step_no, "apply_failed".to_string()));
                break;
            }
        }
    }
    if let Some((step_no, reason)) = conflict {
        swarm_terminal_conflict(
            state,
            swarm,
            &steps,
            &task_id,
            SwarmConflict {
                execution_id: execution_id.as_deref(),
                step_no: Some(step_no),
                board_body: format!("merge_conflict:{step_no}"),
                observed: serde_json::json!({
                    "task_id": task_id,
                    "swarm_id": swarm.swarm_id,
                    "step_no": step_no,
                    "kind": "merge",
                    "reason": reason,
                }),
            },
        );
        return;
    }
    swarm_evidence(
        state,
        &swarm.swarm_id,
        None,
        execution_id.as_deref(),
        "verified",
        "swarm_merged",
        serde_json::json!({
            "task_id": task_id,
            "swarm_id": swarm.swarm_id,
            "merged_steps": merged_steps,
        }),
    );
    crate::routes_swarm::swarm_board_write(state, &swarm.swarm_id, "swarm", "result", "merged");
    // Cleanup (Q-R5-05): remove each merged copy
    // (`git worktree remove --force` + branch delete
    // per Q-ORC-19) and archive its Worktree record.
    // A removal failure leaves the copy listed in
    // `leftover_worktrees` — the merge already
    // succeeded, so cleanup never reopens it.
    for step in steps.iter().filter(|s| s.run_status == "succeeded") {
        let Some(worktree_id) = step.worktree_id.clone() else {
            continue;
        };
        let Some(copy_root) = swarm_copy_root(state, &worktree_id) else {
            continue;
        };
        let branch = format!("swarm-{}-{}", swarm.swarm_id, step.worker_id);
        let removed = swarm_git(
            state,
            &[
                "worktree".to_string(),
                "remove".to_string(),
                "--force".to_string(),
                copy_root.to_string_lossy().into_owned(),
            ],
            &main_root,
        );
        if !matches!(removed, Some(out) if out.exit_code == Some(0)) {
            eprintln!(
                "warning: swarm {} copy {} worktree remove failed, left listed",
                swarm.swarm_id, worktree_id
            );
            continue;
        }
        if !matches!(
            swarm_git(
                state,
                &["branch".to_string(), "-D".to_string(), branch],
                &main_root,
            ),
            Some(out) if out.exit_code == Some(0)
        ) {
            eprintln!(
                "warning: swarm {} branch for {} not deleted (worktree removed)",
                swarm.swarm_id, worktree_id
            );
        }
        let mut worktrees = state.worktrees.lock().expect("worktree lock");
        if let Some(record) = worktrees.get_mut(&worktree_id) {
            record.status = crate::worktree_store::WORKTREE_ARCHIVED.to_string();
            record.updated_ms = now_ms();
            let updated = record.clone();
            persist_worktree(state, &updated);
        }
    }
    if !swarm_persist(state, &swarm.swarm_id, &steps, "completed") {
        return;
    }
    // The task completes here (legal
    // `draft_ready → completed` edge), unless
    // unfanned plan steps remain — then it walks
    // `completed → running` and the serial runner
    // takes the next step, exactly like
    // `advance_executor_after_file_step` (both
    // writes under one lock acquisition, no await
    // between).
    let remaining: Vec<i64> = plan
        .steps()
        .map(|all| {
            all.into_iter()
                .map(|s| s.step_no)
                .filter(|no| !steps.iter().any(|s| s.step_no == *no))
                .collect()
        })
        .unwrap_or_default();
    let next = remaining.into_iter().min();
    let resume = {
        let mut tasks = state.tasks.lock().expect("task lock");
        let Some(current) = tasks.get_mut(&task_id) else {
            return;
        };
        current.status = "completed".to_string();
        current.error = None;
        current.executor_step_no = None;
        let completed = current.clone();
        persist_task(state, &completed);
        match next {
            Some(next_no) => {
                current.status = "running".to_string();
                current.executor_step_no = Some(next_no);
                let running = current.clone();
                persist_task(state, &running);
                Some(next_no)
            }
            None => None,
        }
    };
    match resume {
        Some(next_no) => {
            tokio::spawn(run_task(state.clone(), task_id, false, Some(next_no)));
        }
        None => {
            close_execution_for_task(state, &task_id, "completed");
        }
    }
}
