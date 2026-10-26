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

use crate::deepseek::DeepseekOutcome;
use crate::operation_binding;
use crate::server::{
    approval_status, decide_approval_rejected, ensure_root, persist_approval, persist_task,
    resolve_file, ApprovalState, AppState,
};
use crate::task_store::TaskFileRecord;

/// Frontend parity (TasksView): chunk size is irrelevant gateway
/// side (whole-file read), per-file cap is 262144 bytes.
const MAX_FILE_BYTES: usize = 262144;

/// Verbatim fixed prompt from the manual flow (TasksView).
const PROMPT_PREFIX: &str =
    "请把以下目录文本整理成一份结构清晰的报告（标题、要点、结论）：\n\n";

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
    set_status(state, task_id, "failed", Some(error.to_string()));
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
pub(crate) async fn run_task(state: AppState, task_id: String) {
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
            generate_body(state, record).await;
        }
        "running" => {
            // Claimed by the run handler; drive straight on.
            generate_body(state, record).await;
        }
        "interrupted" => {
            restore_or_regenerate(state, record).await;
        }
        _ => {}
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
        generate_body(state, record).await;
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

/// The generate steps shared by fresh runs and interrupted
/// regenerations. Cancel is observed at the two lock points:
/// after the model call and at the draft_ready transition. A
/// cancelled run discards the draft and creates no approval.
async fn generate_body(state: AppState, record: TaskFileRecord) {
    let task_id = record.task_id.clone();
    let directory = record.directory.clone();
    if ensure_root(&state.files_root).is_err() {
        fail(&state, &task_id, "无法创建文件根");
        return;
    }
    let Ok(dir) = resolve_file(&state.files_root, &directory) else {
        fail(&state, &task_id, "任务目录不在文件根内");
        return;
    };
    let Ok(entries) = std::fs::read_dir(&dir) else {
        fail(&state, &task_id, "任务目录不存在或不可读");
        return;
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
        fail(&state, &task_id, "目录下没有可读文件");
        return;
    }
    let mut parts: Vec<String> = Vec::new();
    for (path, full) in &files {
        let Ok(bytes) = std::fs::read(full) else {
            fail(&state, &task_id, &format!("生成失败：无法读取文件 {path}"));
            return;
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
    let prompt = format!("{PROMPT_PREFIX}{}", parts.join("\n\n"));
    let draft = match state.deepseek.reply(&prompt).await {
        DeepseekOutcome::Disabled => {
            fail(&state, &task_id, "模型未配置");
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
