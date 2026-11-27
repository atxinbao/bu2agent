//! AO-4 (Q-AO-10): per-round memory loading for loops.
//!
//! Three layers, one budget:
//!
//! * Instruction layer — `user_memories` (layer
//!   `"user"`), full text, oldest first. AO4-J1: the
//!   stored data has no "instruction" / "notes" layer
//!   name — `table_for` only accepts `"project"` /
//!   `"user"`, and the save route rejects a scoped
//!   user row, so every user row is global. The only
//!   stored discriminator is the table, so the
//!   instruction layer is the user table and the notes
//!   layer is the project table; rows saved before
//!   AO-4 cannot be reclassified without a schema
//!   change, which is out of scope.
//! * Notes layer — `project_memories` (layer
//!   `"project"`), index only, oldest first. An index
//!   line is `id + layer + first 40 chars`; there is
//!   no loop tool to fetch the full row on demand in
//!   this batch, so the index is what the model sees.
//! * Progress layer — not a memory table: the latest
//!   Execution for the loop's session (AO4-J7) plus
//!   its task's current plan.
//!
//! Budget (AO4-J2/J3): `MEMORY_BUDGET_TOKENS`
//! tokens over the rendered text, estimated by
//! `estimate_tokens` — the gateway has no tokenizer,
//! so one CJK char counts as one token and every four
//! non-CJK chars as one token (conservative for
//! Chinese text). Priority is instruction-full, then
//! notes-index, then progress; a row that does not
//! fit whole degrades to its index line, never to a
//! mid-row truncation. "Instruction rows always load
//! in full" is a priority rule, not an absolute
//! guarantee — the ceiling wins when the layer alone
//! exceeds it.
//!
//! Failure semantics (AO4-J6): memory is advisory
//! context. SQLite unconfigured, broken, or any load
//! error means "no memory" — the loop still starts.
//! Only a real load error writes a warning; the
//! unconfigured state is normal and stays silent.

use crate::memory_store::{self, MemoryRow, MemoryScope};
use crate::plan_store;
use crate::server::{executions_snapshot, AppState};
use crate::sqlite_store::StateBackend;

/// AO-4 (Q-AO-10, local definition): the total token
/// budget for the memory text injected into one loop
/// round.
pub const MEMORY_BUDGET_TOKENS: usize = 4000;

/// Index lines carry the first N chars of the row
/// (AO4-J3).
pub const INDEX_SUMMARY_CHARS: usize = 40;

/// The memory text injected into a loop round, plus
/// how it was assembled (for tests and for the
/// resume-time reload comparison).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryContext {
    /// Rendered text, or empty when nothing loaded.
    pub text: String,
    /// Rows loaded in full.
    pub loaded_full: usize,
    /// Rows loaded as index lines only.
    pub loaded_index: usize,
    /// Rows (and progress lines) omitted entirely —
    /// not even their index line fit.
    pub omitted: usize,
    /// `estimate_tokens` over `text`.
    pub tokens_estimate: usize,
}

/// AO-4 (Q-AO-10, local definition): the token
/// estimate for the 4k budget. The gateway has no
/// tokenizer, so this is a heuristic — one CJK char
/// is about one token, ASCII runs about four chars
/// per token — and it errs on the side of
/// over-counting, so the ceiling is a real guard for
/// Chinese text.
pub fn estimate_tokens(text: &str) -> usize {
    let mut cjk = 0usize;
    let mut other = 0usize;
    for c in text.chars() {
        if is_cjk(c) {
            cjk += 1;
        } else {
            other += 1;
        }
    }
    cjk + other.div_ceil(4)
}

fn is_cjk(c: char) -> bool {
    matches!(c,
        '\u{4E00}'..='\u{9FFF}'
        | '\u{3400}'..='\u{4DBF}'
        | '\u{3000}'..='\u{303F}'
        | '\u{FF00}'..='\u{FFEF}')
}

fn first_chars(text: &str, n: usize) -> String {
    text.chars().take(n).collect()
}

fn render_full(row: &MemoryRow) -> String {
    format!("[{} {}] {}", row.memory_id, row.layer, row.content)
}

fn render_index(row: &MemoryRow) -> String {
    let summary = first_chars(&row.content, INDEX_SUMMARY_CHARS);
    let ellipsis = if row.content.chars().count() > INDEX_SUMMARY_CHARS {
        "…"
    } else {
        ""
    };
    format!(
        "[{} {} 索引] {}{}",
        row.memory_id, row.layer, summary, ellipsis
    )
}

/// Push a line if it fits the budget. Returns false
/// (line dropped, budget untouched) when it does not.
fn try_push(lines: &mut Vec<String>, used: &mut usize, line: String) -> bool {
    let cost = estimate_tokens(&line);
    if *used + cost > MEMORY_BUDGET_TOKENS {
        return false;
    }
    *used += cost;
    lines.push(line);
    true
}

/// Pure assembly (AO4-J3): instruction rows full
/// first, then notes rows as index lines, then
/// progress lines. A row that does not fit whole
/// degrades to its index line; a row whose index
/// line does not fit either is omitted entirely. A
/// section header is pushed only when its section is
/// non-empty and the header itself fits.
pub fn assemble_memory_context(
    instructions: &[MemoryRow],
    notes: &[MemoryRow],
    progress: &[String],
) -> MemoryContext {
    let mut lines: Vec<String> = Vec::new();
    let mut used = 0usize;
    let mut loaded_full = 0usize;
    let mut loaded_index = 0usize;
    let mut omitted = 0usize;

    if !instructions.is_empty()
        && used + estimate_tokens("[记忆·指令]") <= MEMORY_BUDGET_TOKENS
    {
        lines.push("[记忆·指令]".to_string());
        used += estimate_tokens("[记忆·指令]");
        for row in instructions {
            if try_push(&mut lines, &mut used, render_full(row)) {
                loaded_full += 1;
            } else if try_push(&mut lines, &mut used, render_index(row)) {
                loaded_index += 1;
            } else {
                omitted += 1;
            }
        }
    } else {
        omitted += instructions.len();
    }

    if !notes.is_empty() && used + estimate_tokens("[记忆·笔记]") <= MEMORY_BUDGET_TOKENS {
        lines.push("[记忆·笔记]".to_string());
        used += estimate_tokens("[记忆·笔记]");
        for row in notes {
            if try_push(&mut lines, &mut used, render_index(row)) {
                loaded_index += 1;
            } else {
                omitted += 1;
            }
        }
    } else {
        omitted += notes.len();
    }

    if !progress.is_empty() && used + estimate_tokens("[记忆·任务进度]") <= MEMORY_BUDGET_TOKENS
    {
        lines.push("[记忆·任务进度]".to_string());
        used += estimate_tokens("[记忆·任务进度]");
        for line in progress {
            if !try_push(&mut lines, &mut used, line.clone()) {
                omitted += 1;
            }
        }
    } else {
        omitted += progress.len();
    }

    let text = lines.join("\n");
    // The budget above is enforced on the sum of
    // per-line estimates; the joined text costs a
    // few tokens more or less (ceil rounding plus the
    // "\n" joiners), so the reported figure is
    // recomputed over the exact injected text.
    let tokens_estimate = estimate_tokens(&text);
    MemoryContext {
        text,
        loaded_full,
        loaded_index,
        omitted,
        tokens_estimate,
    }
}

/// The progress target (AO4-J7): the latest
/// Execution for a session, resolved without holding
/// the SQLite lock (the snapshot locks it
/// internally). `task_id` / `task_status` /
/// `plan_id` are None when that link is missing —
/// every miss degrades to a shorter line, never an
/// error.
struct ProgressTarget {
    execution_id: String,
    execution_status: String,
    task_id: Option<String>,
    task_status: Option<String>,
    plan_id: Option<String>,
}

fn resolve_progress_target(
    state: &AppState,
    session_id: Option<&str>,
) -> Option<ProgressTarget> {
    let session_id = session_id?;
    let executions = executions_snapshot(state)?;
    let execution = executions
        .into_iter()
        .filter(|r| r.session_id.as_deref() == Some(session_id))
        .max_by_key(|r| crate::sqlite_store::id_suffix(&r.execution_id))?;
    let (task_status, plan_id) = match &execution.task_id {
        Some(task_id) => match state.tasks.lock().expect("task lock").get(task_id) {
            Some(task) => (Some(task.status.clone()), task.plan_id.clone()),
            None => (None, None),
        },
        None => (None, None),
    };
    Some(ProgressTarget {
        execution_id: execution.execution_id,
        execution_status: execution.status,
        task_id: execution.task_id,
        task_status,
        plan_id,
    })
}

fn render_progress(target: &ProgressTarget, plan: Option<&plan_store::PlanRow>) -> Vec<String> {
    match (&target.task_id, &target.task_status, plan) {
        (Some(task_id), Some(task_status), Some(plan)) => {
            let steps = plan.steps().map(|steps| steps.len()).unwrap_or(0);
            vec![format!(
                "任务 {task_id}（{task_status}）：计划 {} v{}（{}），共 {steps} 步；执行 {}：{}",
                plan.plan_id,
                plan.version,
                plan.status,
                target.execution_id,
                target.execution_status,
            )]
        }
        (Some(task_id), Some(task_status), None) => vec![format!(
            "任务 {task_id}（{task_status}）：无计划；执行 {}：{}",
            target.execution_id, target.execution_status,
        )],
        (Some(task_id), None, _) => vec![format!(
            "执行 {}：{}（任务 {task_id} 记录缺失）",
            target.execution_id, target.execution_status,
        )],
        (None, _, _) => vec![format!(
            "执行 {}：{}（无关联任务）",
            target.execution_id, target.execution_status,
        )],
    }
}

/// Load the three layers for one loop round (AO4-J4:
/// the handler owns AppState/SQLite, so the load
/// happens here; the engine only inserts the
/// rendered text). Returns None when SQLite is not
/// the live backend or any read fails — the caller
/// treats that as "no memory" (AO4-J6), never as a
/// failed loop start. Only a real read error warns;
/// the unconfigured state is normal and silent.
pub fn load_loop_memory(state: &AppState, session_id: Option<&str>) -> Option<MemoryContext> {
    if state.sqlite.is_none()
        || state.sqlite_mode() != StateBackend::Sqlite
        || state.sqlite_is_broken()
    {
        return None;
    }
    // Progress target first: the snapshot locks
    // SQLite internally, so it must run before we
    // take the connection lock below.
    let target = resolve_progress_target(state, session_id);
    let Some(sqlite) = &state.sqlite else {
        return None;
    };
    let (instructions, notes, progress) = {
        let conn = sqlite.lock().expect("sqlite lock");
        let instructions =
            match memory_store::list_memories(&conn, Some("user"), &MemoryScope::default()) {
                Ok(rows) => rows,
                Err(detail) => {
                    eprintln!("warning: AO-4 instruction load failed: {detail}");
                    return None;
                }
            };
        let notes =
            match memory_store::list_memories(&conn, Some("project"), &MemoryScope::default()) {
                Ok(rows) => rows,
                Err(detail) => {
                    eprintln!("warning: AO-4 notes load failed: {detail}");
                    return None;
                }
            };
        let progress = match target {
            Some(target) => {
                let plan = match &target.task_id {
                    Some(task_id) => {
                        let by_pointer = target
                            .plan_id
                            .as_deref()
                            .and_then(|id| plan_store::get_plan(&conn, id).ok().flatten());
                        match by_pointer {
                            Some(row) => Some(row),
                            None => plan_store::list_plans(&conn, Some(task_id), 1, 0)
                                .ok()
                                .and_then(|mut rows| rows.pop()),
                        }
                    }
                    None => None,
                };
                render_progress(&target, plan.as_ref())
            }
            None => Vec::new(),
        };
        (instructions, notes, progress)
    };
    Some(assemble_memory_context(&instructions, &notes, &progress))
}
