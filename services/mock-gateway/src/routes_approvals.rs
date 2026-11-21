//! Approval routes: /approval (3) + /approvals (5) +
//! /mandates (2) + /policies/versions (1) = 11.
//!
//! - POST /approval/check       -> approval gate result
//! - POST /approval/request      -> open a pending approval record (approval tools)
//! - POST /approval/decide       -> decide an approval record (approved/rejected/edited)
//! - GET  /approvals             -> current approval records, one per id
//! - GET  /approvals/history     -> current approval records, one per id (same as /approvals; the event log is the audit source, not this route)
//! - GET  /approvals/{id}        -> one approval record
//! - GET  /approvals/{id}/editable-message -> message + whether it may be edited
//! - POST /approvals/{id}/edit-message     -> edit an unbound pending approval's message
//!
//! The handlers moved here from `server.rs` (FU-5 S-9f). The 8
//! `//!` directory lines above moved byte-identical with this
//! batch (S-7 / S-9a / S-9d precedent): server.rs header 35 ->
//! 27, directory total stays 59 (27 server.rs + 13 files +
//! 1 misc + 9 chat + 1 loop + 8 approvals). No /policies or
//! /mandates directory lines exist. The header prose lines
//! ("approval gate", "approval records are appended as JSONL")
//! stay in `server.rs`: they are prose, not route lines.
//!
//! `ApprovalRequest` / `ApprovalResponse` move with
//! `approval_check` in this batch (S-9a kept them in server.rs
//! only because approval_check stayed for S-9f); they become
//! pub(crate) like every other moved DTO. The routes_chat.rs
//! comment saying they stay is now stale, left untouched as
//! out of scope.
//!
//! `ApprovalState` stays in `server.rs` (staying code constructs
//! literals; task_runner.rs builds records via
//! `ApprovalState::new`): the moved handlers reach it through
//! `crate::server::ApprovalState`. Five of its private fields
//! became pub(crate) in this batch (S-9a `effect_binding`
//! precedent) because the moved code needs them: `binding`
//! (approval_decide / approval_view), `message`
//! (approval_view / approval_edit_message),
//! `command` + `action_binding` (the approval_request literal),
//! `consumed_by_worker_session_id` (approval_view); `is_bound()`
//! became pub(crate) too (approval_view / approval_editable).
//! A struct literal with ANY private field fails E0451 outside
//! its module, so the fields go together -- `new()` + field
//! sets would need the same four fields pub(crate) anyway.
//! `CommandSpec` stays in `server.rs` (ApprovalState field) and
//! is imported back the same way. `parse_approval_status`
//! stays (staying callers at 638/1908/2533) as pub(crate);
//! `link_patch_file_rejected_on_approval` stays (staying
//! decide_approval_rejected) as pub(crate). `persist_approval`,
//! `approval_status_name`, `worker_command_digest`,
//! `adapter_execution`, `record_effect` were already pub(crate)
//! and stay; the moved code calls them qualified
//! `crate::server::...`. `record_effect` moves with S-9g;
//! `NewEffect` is imported from `crate::effect_store`
//! (routes_tasks.rs precedent). `ToolCallDto` stays (S-9d).
//! The AppState field `approvals_path` became pub(crate)
//! (approvals_history reads it); approvals /
//! next_approval_id / sqlite were already pub(crate).
//! The 13 /api/email routes stay in server.rs (S-9h, after
//! S-9g); the ledger routes stay (S-9g).

use agent_desktop::approval::ApprovalStatus;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::approval_store::{load_records, ApprovalFileRecord};
use crate::effect_store::NewEffect;
use crate::loop_engine::ApprovalBinding;
use crate::routes_common::{bad_request, sqlite_error_response, sqlite_not_configured};
use crate::server::{AppState, ApprovalState, CommandSpec};
use crate::sqlite_store::{self, StateBackend};
use crate::{may_execute_tool, tool_requires_approval};


#[derive(Debug, Deserialize)]
pub(crate) struct ApprovalRequest {
    pub(crate) status: String,
    pub(crate) sensitive: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct ApprovalResponse {
    pub(crate) may_execute: bool,
}

pub(crate) async fn approval_check(Json(req): Json<ApprovalRequest>) -> impl IntoResponse {
    let Some(status) = crate::server::parse_approval_status(&req.status) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "unknown approval status" })),
        )
            .into_response();
    };
    Json(ApprovalResponse { may_execute: may_execute_tool(status, req.sensitive) }).into_response()
}

// --- Agent invocation + approval loop (Phase 3) ---

#[derive(Debug, Deserialize)]
pub(crate) struct ApprovalRequestDto {
    tool: String,
    path: Option<String>,
    to_path: Option<String>,
    content_base64: Option<String>,
    append: Option<bool>,
    message: Option<String>,
    /// D1-6b-1 (Q-D1-6b-22): the mail send presentation.
    /// The server recomputes `mail_send_digest` from
    /// these fields, never from a client hash — the
    /// approval binds the exact message, not the bare
    /// ability to send mail.
    to: Option<Vec<String>>,
    subject: Option<String>,
    body: Option<String>,
    /// D1-6b-2 (Q-D1-6b-22): the calendar create
    /// presentation. The server recomputes
    /// `calendar_create_digest` from these fields, never
    /// from a client hash — the approval binds the exact
    /// event, not the bare ability to create events.
    /// `start_ms` / `end_ms` / `all_day` are typed
    /// (the TS and desktop clients send real JSON
    /// numbers / booleans, the mail `to` precedent);
    /// `location` / `notes` are optional.
    calendar_id: Option<String>,
    title: Option<String>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    all_day: Option<bool>,
    location: Option<String>,
    notes: Option<String>,
    /// D1-6b-3 (Q-D1-6b-22): the mail delete
    /// presentation — the native id of the message to
    /// delete. The server recomputes
    /// `mail_delete_digest` from it, never from a client
    /// hash — the approval binds the exact message, not
    /// the bare ability to delete mail.
    native_id: Option<String>,
    /// MCL-4 (plan §6): the iMessage send
    /// presentation — the local `thread-N` id. The
    /// server recomputes `imessage_send_digest` from the
    /// thread id and `body`, never from a client hash —
    /// the approval binds the exact message in the exact
    /// thread, not the bare ability to send iMessages.
    thread_id: Option<String>,
    /// D1-6b-4 (Q-D1-6b-22): the note create / append /
    /// delete presentation. The server recomputes the
    /// note digests from these fields, never from a
    /// client hash — the approval binds the exact note
    /// write, not the bare ability to write notes.
    /// `folder` / `name` bind a create, `note_id` +
    /// `base_body_hash` bind an append, `note_id` alone
    /// binds a delete. `body` is the shared mail.send
    /// field, reused here for the note body / appended
    /// text (one body slot, distinguished by tool). The
    /// Mock `notes.create` stays tool-only (these fields
    /// absent).
    folder: Option<String>,
    name: Option<String>,
    note_id: Option<String>,
    base_body_hash: Option<String>,
    /// MCL-5 (plan §6): the contacts create / update /
    /// delete presentation. The server recomputes the
    /// contact digests from these fields, never from a
    /// client hash — the approval binds the exact card
    /// write, not the bare ability to write contacts.
    /// `display_name` (+ `organization` / `job_title` /
    /// `phones` / `emails`) binds a create; the same
    /// fields with `contact_id` bind an update;
    /// `contact_id` alone binds a delete. `phones` /
    /// `emails` arrive as JSON arrays (the mail.send
    /// `to` precedent).
    contact_id: Option<String>,
    display_name: Option<String>,
    organization: Option<String>,
    job_title: Option<String>,
    phones: Option<Vec<String>>,
    emails: Option<Vec<String>>,
    /// P3-C (§7): a client-presented Worker command spec.
    /// Never trusted: the handler recomputes the spec from
    /// the raw presentation below and rejects a mismatch.
    command: Option<CommandSpec>,
    /// P3-C (§7): raw Worker command presentation (the
    /// server resolves the worktree + binary + allowlist and
    /// computes the digest + spec from these).
    worktree_id: Option<String>,
    command_template: Option<String>,
    params: Option<serde_json::Value>,
    timeout_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ApprovalRequestResponse {
    tool_call_id: Option<String>,
    requires_approval: bool,
    status: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ApprovalDecideDto {
    tool_call_id: String,
    status: String,
}

pub(crate) async fn approval_request(
    State(state): State<AppState>,
    Json(req): Json<ApprovalRequestDto>,
) -> impl IntoResponse {
    let Some(requires) = tool_requires_approval(&req.tool) else {
        return bad_request("unknown tool");
    };
    if !requires {
        return Json(ApprovalRequestResponse {
            tool_call_id: None,
            requires_approval: false,
            status: "not_required".to_string(),
        })
        .into_response();
    }
    // File tools must present their full operation so the server can
    // bind the approval to it; the digest is computed here from the
    // parameters the client presented, never from a client hash.
    // Connector tools keep tool-only requests and stay unbound.
    // P3-C: a worker.run approval binds the resolved command
    // (argv + cwd + timeout); the presented CommandSpec, when
    // present, is a claim verified against the server-computed
    // spec, never trusted.
    let mut command_spec: Option<CommandSpec> = None;
    let operation_digest = match req.tool.as_str() {
        "fs.write" => {
            let (Some(path), Some(content_base64)) = (req.path.as_deref(), req.content_base64.as_deref()) else {
                return bad_request("fs.write approval requires path and content_base64");
            };
            let Ok(data) = base64::engine::general_purpose::STANDARD.decode(content_base64) else {
                return bad_request("invalid base64 content");
            };
            match crate::operation_binding::write_digest(path, &data, req.append.unwrap_or(false)) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid path"),
            }
        }
        "fs.delete" => {
            let Some(path) = req.path.as_deref() else {
                return bad_request("fs.delete approval requires path");
            };
            match crate::operation_binding::delete_digest(path) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid path"),
            }
        }
        "fs.rename" => {
            let (Some(from), Some(to)) = (req.path.as_deref(), req.to_path.as_deref()) else {
                return bad_request("fs.rename approval requires path and to_path");
            };
            match crate::operation_binding::rename_digest(from, to) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid path"),
            }
        }
        "fs.mkdir" => {
            let Some(path) = req.path.as_deref() else {
                return bad_request("fs.mkdir approval requires path");
            };
            match crate::operation_binding::mkdir_digest(path) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid path"),
            }
        }
        "fs.export" => {
            let (Some(from), Some(to)) = (req.path.as_deref(), req.to_path.as_deref()) else {
                return bad_request("fs.export approval requires path and to_path");
            };
            match crate::operation_binding::export_digest(from, to) {
                Some(digest) => Some(digest),
                None => return bad_request("invalid export path"),
            }
        }
        "mail.send" => {
            // D1-6b-1 (Q-D1-6b-22): when the mail fields
            // are presented, the digest is computed here
            // from them — same normalization (deduplicated,
            // sorted recipients; body hashed server-side)
            // as the send route, so the two digests match.
            // When they are entirely absent the request
            // stays tool-only (the plan's "optional
            // fields"): the approvals-query smoke tests
            // use bare `mail.send` as the unbound
            // connector-tool example, and the Mock route
            // keeps its tool-only gate. A tool-only
            // record can never authorize the real send —
            // the send route rejects a missing digest
            // with 403 `approval_binding_mismatch`.
            // A partial presentation is a client error.
            // No planned Effect is created for mail.send
            // here (the `worker.run` precedent): the send
            // route plans its own Effect on its write
            // Execution (Q-D1-6b-23) and binds this
            // approval to it at consume time.
            if req.to.is_none() && req.subject.is_none() && req.body.is_none() {
                None
            } else {
                let (Some(to), Some(subject), Some(body)) = (
                    req.to.as_deref(),
                    req.subject.as_deref(),
                    req.body.as_deref(),
                ) else {
                    return bad_request("mail.send approval requires to, subject and body");
                };
                if subject.is_empty() || body.is_empty() {
                    return bad_request("mail.send approval requires non-empty subject and body");
                }
                let recipients = crate::mail_send::normalize_recipients(to);
                if recipients.is_empty() {
                    return bad_request("mail.send approval requires at least one recipient");
                }
                let body_hash = crate::mail_send::mail_body_hash(body);
                Some(crate::operation_binding::mail_send_digest(
                    &recipients,
                    subject,
                    &body_hash,
                ))
            }
        }
        "imessage.send" => {
            // MCL-4 (plan §6): when the thread id +
            // body are presented, the digest is computed
            // here from them — same inputs (thread id +
            // server-side body hash) as the send route,
            // so the two digests match. When they are
            // entirely absent the request stays tool-only
            // (the mail.send precedent): a tool-only
            // record can never authorize the real send —
            // the send route rejects a missing digest
            // with 403 `approval_binding_mismatch`. A
            // partial presentation is a client error. No
            // planned Effect is created for imessage.send
            // here (the `worker.run` precedent): the send
            // route plans its own Effect on its write
            // Execution (Q-D1-6b-23) and binds this
            // approval to it at consume time.
            if req.thread_id.is_none() && req.body.is_none() {
                None
            } else {
                let (Some(thread_id), Some(body)) =
                    (req.thread_id.as_deref(), req.body.as_deref())
                else {
                    return bad_request("imessage.send approval requires thread_id and body");
                };
                if thread_id.is_empty() || body.is_empty() {
                    return bad_request(
                        "imessage.send approval requires non-empty thread_id and body",
                    );
                }
                let body_hash = crate::operation_binding::sha256_hex(body.as_bytes());
                Some(crate::operation_binding::imessage_send_digest(
                    thread_id, &body_hash,
                ))
            }
        }
        "calendar.create" => {
            // D1-6b-2 (Q-D1-6b-22): when the calendar
            // fields are presented, the digest is computed
            // here from them — same normalization (empty
            // location / notes count as absent; title /
            // location / notes hashed server-side) as the
            // create route, so the two digests match. When
            // they are entirely absent the request stays
            // tool-only (the mail.send precedent): a
            // tool-only record can never authorize the
            // real create — the create route rejects a
            // missing digest with 403
            // `approval_binding_mismatch`. A partial
            // presentation is a client error. No planned
            // Effect is created for calendar.create here
            // (the `worker.run` precedent): the create
            // route plans its own Effect on its write
            // Execution (Q-D1-6b-23) and binds this
            // approval to it at consume time.
            if req.calendar_id.is_none()
                && req.title.is_none()
                && req.start_ms.is_none()
                && req.end_ms.is_none()
                && req.all_day.is_none()
                && req.location.is_none()
                && req.notes.is_none()
            {
                None
            } else {
                let (Some(calendar_id), Some(title), Some(start_ms), Some(end_ms), Some(all_day)) = (
                    req.calendar_id.as_deref(),
                    req.title.as_deref(),
                    req.start_ms,
                    req.end_ms,
                    req.all_day,
                ) else {
                    return bad_request(
                        "calendar.create approval requires calendar_id, title, start_ms, end_ms and all_day",
                    );
                };
                if calendar_id.is_empty() || title.is_empty() {
                    return bad_request(
                        "calendar.create approval requires non-empty calendar_id and title",
                    );
                }
                if end_ms <= start_ms {
                    return bad_request("calendar.create approval requires end_ms after start_ms");
                }
                let location = req.location.as_deref().filter(|s| !s.is_empty());
                let notes = req.notes.as_deref().filter(|s| !s.is_empty());
                let title_hash = crate::operation_binding::sha256_hex(title.as_bytes());
                let location_hash =
                    crate::operation_binding::sha256_hex(location.unwrap_or("").as_bytes());
                let notes_hash = crate::calendar_store::calendar_notes_hash(notes);
                Some(crate::operation_binding::calendar_create_digest(
                    calendar_id,
                    &title_hash,
                    start_ms,
                    end_ms,
                    all_day,
                    &location_hash,
                    &notes_hash,
                ))
            }
        }
        "mail.delete" => {
            // D1-6b-3 (Q-D1-6b-22): when the native id is
            // presented, the digest is computed here from
            // it — the same input the delete route resolves
            // its path id to, so the two digests match.
            // When it is absent the request stays
            // tool-only (the mail.send precedent): a
            // tool-only record can never authorize the
            // real delete — the delete route rejects a
            // missing digest with 403
            // `approval_binding_mismatch`. An empty id is
            // a client error. No planned Effect is created
            // here (the `worker.run` precedent): the
            // delete route plans its own Effect on its
            // write Execution (Q-D1-6b-23) and binds this
            // approval to it at consume time.
            if req.native_id.is_none() {
                None
            } else {
                let Some(native_id) = req.native_id.as_deref() else {
                    return bad_request("mail.delete approval requires native_id");
                };
                if native_id.is_empty() {
                    return bad_request("mail.delete approval requires non-empty native_id");
                }
                Some(crate::operation_binding::mail_delete_digest(native_id))
            }
        }
        "notes.create" => {
            // D1-6b-4 (Q-D1-6b-22): when the folder /
            // name / body are presented, the digest is
            // computed here from them — same
            // normalization (hashes server-side) as the
            // create route, so the two digests match.
            // When they are entirely absent the request
            // stays tool-only (the mail.send precedent):
            // the Mock `notes.create` keeps its tool-only
            // gate, and a tool-only record can never
            // authorize the real create — the create
            // route rejects a missing digest with 403
            // `approval_binding_mismatch`. A partial
            // presentation is a client error. No planned
            // Effect is created here (the `worker.run`
            // precedent): the create route plans its own
            // Effect on its write Execution (Q-D1-6b-23)
            // and binds this approval to it at consume
            // time.
            if req.folder.is_none() && req.name.is_none() && req.body.is_none() {
                None
            } else {
                let (Some(folder), Some(name), Some(body)) = (
                    req.folder.as_deref(),
                    req.name.as_deref(),
                    req.body.as_deref(),
                ) else {
                    return bad_request("notes.create approval requires folder, name and body");
                };
                if folder.is_empty() || name.is_empty() || body.is_empty() {
                    return bad_request("notes.create approval requires non-empty folder, name and body");
                }
                let folder_hash = crate::operation_binding::sha256_hex(folder.as_bytes());
                let name_hash = crate::operation_binding::sha256_hex(name.as_bytes());
                let body_hash = crate::operation_binding::sha256_hex(body.as_bytes());
                Some(crate::operation_binding::note_create_digest(
                    &folder_hash,
                    &name_hash,
                    &body_hash,
                ))
            }
        }
        "notes.append" => {
            // D1-6b-4 (Q-D1-6b-22): the append digest
            // binds the note id, the base body hash (the
            // version the approver saw), and the hash of
            // the appended text — the same inputs the
            // append route recomputes, so the two digests
            // match. When all three are absent the
            // request stays tool-only (the mail.send
            // precedent); a partial presentation is a
            // client error. No planned Effect is created
            // here (the `worker.run` precedent).
            if req.note_id.is_none() && req.base_body_hash.is_none() && req.body.is_none() {
                None
            } else {
                let (Some(note_id), Some(base_body_hash), Some(body)) = (
                    req.note_id.as_deref(),
                    req.base_body_hash.as_deref(),
                    req.body.as_deref(),
                ) else {
                    return bad_request(
                        "notes.append approval requires note_id, base_body_hash and body",
                    );
                };
                if note_id.is_empty() || base_body_hash.is_empty() || body.is_empty() {
                    return bad_request(
                        "notes.append approval requires non-empty note_id, base_body_hash and body",
                    );
                }
                let new_body_hash = crate::operation_binding::sha256_hex(body.as_bytes());
                Some(crate::operation_binding::note_append_digest(
                    note_id,
                    base_body_hash,
                    &new_body_hash,
                ))
            }
        }
        "notes.delete" => {
            // D1-6b-4 (Q-D1-6b-22): when the note id is
            // presented, the digest is computed here from
            // it — the same id the delete route resolves
            // its path id to, so the two digests match.
            // When it is absent the request stays
            // tool-only (the mail.send precedent): a
            // tool-only record can never authorize the
            // real delete. An empty id is a client error.
            // No planned Effect is created here (the
            // `worker.run` precedent).
            if req.note_id.is_none() {
                None
            } else {
                let Some(note_id) = req.note_id.as_deref() else {
                    return bad_request("notes.delete approval requires note_id");
                };
                if note_id.is_empty() {
                    return bad_request("notes.delete approval requires non-empty note_id");
                }
                Some(crate::operation_binding::note_delete_digest(note_id))
            }
        }
        "contacts.create" => {
            // MCL-5 (plan §6): when the display name is
            // presented, the digest is computed here from
            // the normalized fields — the same inputs the
            // create route recomputes, so the two digests
            // match. When all five fields are absent the
            // request stays tool-only (the mail.send
            // precedent): a tool-only record can never
            // authorize the real create — the create route
            // rejects a missing digest with 403
            // `approval_binding_mismatch`. A present-but-
            // empty display name is a client error (a
            // card with an empty name is not a card);
            // phones / emails / organization / job title
            // are optional (a card with just a name is
            // valid). No planned Effect is created here
            // (the `worker.run` precedent): the create
            // route plans its own Effect on its write
            // Execution (Q-D1-6b-23) and binds this
            // approval to it at consume time.
            if req.display_name.is_none()
                && req.organization.is_none()
                && req.job_title.is_none()
                && req.phones.is_none()
                && req.emails.is_none()
            {
                None
            } else {
                let Some(display_name) = req.display_name.as_deref() else {
                    return bad_request(
                        "contacts.create approval requires display_name",
                    );
                };
                if display_name.trim().is_empty() {
                    return bad_request(
                        "contacts.create approval requires non-empty display_name",
                    );
                }
                let content = crate::contacts_source::ContactContent::normalized(
                    display_name,
                    req.organization.as_deref().unwrap_or(""),
                    req.job_title.as_deref().unwrap_or(""),
                    req.phones.as_deref().unwrap_or(&[]),
                    req.emails.as_deref().unwrap_or(&[]),
                );
                Some(crate::operation_binding::contact_create_digest(
                    &content.display_name,
                    &content.organization,
                    &content.job_title,
                    &content.phones,
                    &content.emails,
                ))
            }
        }
        "contacts.update" => {
            // MCL-5 (plan §6): when the contact id +
            // display name are presented, the digest is
            // computed here from the normalized fields —
            // the same inputs the update route recomputes,
            // so the two digests match. The base content
            // hash is NOT in the digest: it is a
            // concurrency guard, not part of the approved
            // content, so a retry with a fresh base and
            // the same unconsumed approval still matches.
            // When all six fields are absent the request
            // stays tool-only (the mail.send precedent);
            // a partial presentation is a client error.
            // No planned Effect is created here (the
            // `worker.run` precedent).
            if req.contact_id.is_none()
                && req.display_name.is_none()
                && req.organization.is_none()
                && req.job_title.is_none()
                && req.phones.is_none()
                && req.emails.is_none()
            {
                None
            } else {
                let (Some(contact_id), Some(display_name)) =
                    (req.contact_id.as_deref(), req.display_name.as_deref())
                else {
                    return bad_request(
                        "contacts.update approval requires contact_id and display_name",
                    );
                };
                if contact_id.is_empty() || display_name.trim().is_empty() {
                    return bad_request(
                        "contacts.update approval requires non-empty contact_id and display_name",
                    );
                }
                let content = crate::contacts_source::ContactContent::normalized(
                    display_name,
                    req.organization.as_deref().unwrap_or(""),
                    req.job_title.as_deref().unwrap_or(""),
                    req.phones.as_deref().unwrap_or(&[]),
                    req.emails.as_deref().unwrap_or(&[]),
                );
                Some(crate::operation_binding::contact_update_digest(
                    contact_id,
                    &content.display_name,
                    &content.organization,
                    &content.job_title,
                    &content.phones,
                    &content.emails,
                ))
            }
        }
        "contacts.delete" => {
            // MCL-5 (plan §6): when the contact id is
            // presented, the digest is computed here from
            // it — the same id the delete route resolves
            // its path id to, so the two digests match.
            // When it is absent the request stays
            // tool-only (the mail.send precedent): a
            // tool-only record can never authorize the
            // real delete. An empty id is a client error.
            // No planned Effect is created here (the
            // `worker.run` precedent).
            if req.contact_id.is_none() {
                None
            } else {
                let Some(contact_id) = req.contact_id.as_deref() else {
                    return bad_request("contacts.delete approval requires contact_id");
                };
                if contact_id.is_empty() {
                    return bad_request("contacts.delete approval requires non-empty contact_id");
                }
                Some(crate::operation_binding::contact_delete_digest(contact_id))
            }
        }
        "worker.run" => {
            let (Some(worktree_id), Some(command_template)) =
                (req.worktree_id.as_deref(), req.command_template.as_deref())
            else {
                return bad_request(
                    "worker.run approval requires worktree_id and command_template",
                );
            };
            // Flat args for the shared resolver: the raw
            // presentation, never a client hash.
            let mut flat = std::collections::BTreeMap::new();
            flat.insert("worktree_id".to_string(), worktree_id.to_string());
            flat.insert("command".to_string(), command_template.to_string());
            if let Some(timeout) = req.timeout_ms {
                flat.insert("timeout_ms".to_string(), timeout.to_string());
            }
            if let Some(serde_json::Value::Object(map)) = req.params.as_ref() {
                for (key, value) in map {
                    if let serde_json::Value::String(s) = value {
                        flat.insert(key.clone(), s.clone());
                    }
                }
            }
            let (digest, spec) = match crate::server::worker_command_digest(&state, &flat) {
                Ok(pair) => pair,
                Err(err) => {
                    return (
                        err.status,
                        Json(serde_json::json!({ "error": err.code })),
                    )
                        .into_response();
                }
            };
            if let Some(presented) = &req.command {
                if presented != &spec {
                    return bad_request("command_mismatch");
                }
            }
            command_spec = Some(spec);
            Some(digest)
        }
        _ => None,
    };
    let mut next = state.next_approval_id.lock().expect("approval id lock");
    let id = format!("approval-{}", *next);
    *next += 1;
    let record = ApprovalState { tool: req.tool.clone(), status: ApprovalStatus::Pending, binding: None, consumed: false, operation_digest: operation_digest.clone(), action_binding: None, effect_binding: None, message: req.message.clone(), command: command_spec, consumed_by_worker_session_id: None };
    let mut approvals = state.approvals.lock().expect("approval lock");
    if let Err(detail) = crate::server::persist_approval(&state, &id, &record) {
        // Bound records fail closed like loop approvals: an id whose
        // pending line is not durable must not be issued.
        if operation_digest.is_some() {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"approval_audit_failed"}))).into_response();
        }
        eprintln!("warning: approval persistence failed: {detail}");
    }
    approvals.insert(id.clone(), record);
    // The insert guard must be released before the planned
    // Effect below takes the approvals lock again for the
    // binding: std Mutexes are not reentrant, and holding
    // this guard across the second lock would deadlock.
    drop(approvals);
    // P0-C: a digest-bound approval request IS the plan for
    // its write, so the Effect is created here as `planned`
    // and the approval is bound to it (`effect_id +
    // input_digest`). The write handler later drives it
    // `planned -> authorized -> started -> terminal` instead
    // of recording a fresh row. A rejected or never-used
    // approval leaves its planned row planned: that is the
    // honest record (planned, never authorized), not a missing
    // Effect. Binding persistence is best-effort here because
    // the approval is already issued; the write handler can
    // still resolve the planned row through `approval_id` as
    // a fallback, so a lost binding line cannot authorize a
    // different write (the digest check still applies).
    if let Some(digest) = &operation_digest {
        let planned = match req.tool.as_str() {
            "fs.write" => req.path.as_deref().and_then(|p| {
                crate::operation_binding::normalize_path(p)
                    .map(|scope| ("fs", "write", scope))
            }),
            "fs.delete" => req.path.as_deref().and_then(|p| {
                crate::operation_binding::normalize_path(p)
                    .map(|scope| ("fs", "delete", scope))
            }),
            "fs.rename" => match (req.path.as_deref(), req.to_path.as_deref()) {
                (Some(from), Some(to)) => {
                    match (
                        crate::operation_binding::normalize_path(from),
                        crate::operation_binding::normalize_path(to),
                    ) {
                        (Some(from), Some(to)) => {
                            Some(("fs", "rename", format!("{from} -> {to}")))
                        }
                        _ => None,
                    }
                }
                _ => None,
            },
            "fs.mkdir" => req.path.as_deref().and_then(|p| {
                crate::operation_binding::normalize_path(p)
                    .map(|scope| ("fs", "mkdir", scope))
            }),
            "fs.export" => req.path.as_deref().and_then(|p| {
                crate::operation_binding::normalize_path(p)
                    .map(|scope| ("fs", "export", scope))
            }),
            // P3-C: worker.run is digest-only — no planned
            // Effect. The driver records the completion Effect
            // fresh at run completion (its digest needs
            // post-command observations that do not exist yet).
            _ => None,
        };
        if let Some((capability, operation, scope)) = planned {
            let effect = crate::server::record_effect(
                &state,
                NewEffect {
                    execution_id: Some(crate::server::adapter_execution(&state)),
                    capability: capability.to_string(),
                    operation: operation.to_string(),
                    scope,
                    input_digest: Some(digest.clone()),
                    approval_id: Some(id.clone()),
                    status: "planned".to_string(),
                },
            );
            let mut approvals = state.approvals.lock().expect("approval lock");
            if let Some(approval) = approvals.get_mut(&id) {
                approval.effect_binding = Some(effect.effect_id.clone());
                let bound = approval.clone();
                drop(approvals);
                if let Err(detail) = crate::server::persist_approval(&state, &id, &bound) {
                    eprintln!("warning: approval effect binding persistence failed: {detail}");
                }
            }
        }
    }
    Json(ApprovalRequestResponse {
        tool_call_id: Some(id),
        requires_approval: true,
        status: "pending".to_string(),
    })
    .into_response()
}

pub(crate) async fn approval_decide(
    State(state): State<AppState>,
    Json(req): Json<ApprovalDecideDto>,
) -> impl IntoResponse {
    let Some(status) = crate::server::parse_approval_status(&req.status) else {
        return bad_request("unknown approval status");
    };
    if !matches!(status, ApprovalStatus::Approved | ApprovalStatus::Rejected | ApprovalStatus::Edited) {
        return bad_request("decision must be approved, rejected or edited");
    }
    let mut approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get_mut(&req.tool_call_id) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown tool_call_id" }))).into_response();
    };
    // Bound records (loop binding or operation digest) fail closed on
    // decide persistence, exactly like loop approvals already did:
    // a decision that cannot be audited must not take effect.
    let bound = record.binding.is_some() || record.operation_digest.is_some();
    // P3-B window fix (U3): approving an already-approved,
    // unconsumed approval is an idempotent no-op, not an error.
    // A UI that decides-then-applies (PatchesView's "approve and
    // apply") otherwise cannot apply a file whose approval was
    // already decided elsewhere: the repeated decide 409s and the
    // apply step is never reached. Reject-on-approved and any
    // decide on a consumed or rejected record still 409 below;
    // the apply command remains the real approval gate.
    // D1-2 (Q-D1-24): the no-op and the single-decision guard
    // below now apply to unbound records too — a decision is
    // final whoever the record is bound to. The one exception
    // is an unbound record in Edited status: it was refined
    // via `approval_edit_message` (a pre-decision edit, not a
    // decision) and is still decidable. Bound records keep the
    // old shape: deciding Edited on them still 409s.
    if status == ApprovalStatus::Approved
        && record.status == ApprovalStatus::Approved
        && !record.consumed
    {
        let current = req.status.clone();
        let tool_call_id = req.tool_call_id.clone();
        drop(approvals);
        return Json(serde_json::json!({ "tool_call_id": tool_call_id, "status": current })).into_response();
    }
    let already_decided = record.consumed
        || (record.status != ApprovalStatus::Pending
            && (record.status != ApprovalStatus::Edited || bound));
    let edited_via_decide = bound && status == ApprovalStatus::Edited;
    if already_decided || edited_via_decide {
        return (StatusCode::CONFLICT, Json(serde_json::json!({"error":"loop_approval_already_decided_or_invalid"}))).into_response();
    }
    record.status = status;
    if let Err(detail) = crate::server::persist_approval(&state, &req.tool_call_id, record) {
        if bound { record.status = ApprovalStatus::Pending; return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error":"approval_audit_failed"}))).into_response(); }
        eprintln!("warning: approval persistence failed: {detail}");
    }
    // P3-B (§4.5): a rejected approval rejects its PatchFile.
    // Runs after the approvals lock is released (the linkage
    // only touches SQLite).
    let rejected = status == ApprovalStatus::Rejected;
    let tool_call_id = req.tool_call_id.clone();
    drop(approvals);
    if rejected {
        crate::server::link_patch_file_rejected_on_approval(&state, &tool_call_id);
    }
    Json(serde_json::json!({ "tool_call_id": req.tool_call_id, "status": req.status })).into_response()
}

// --- Egress approval query surface (Task 7) ---
//
// Local semantics only (route schemas are unknown; see the Task 7
// plan): views follow the local JSONL record shape, history is the
// persisted event lines in file order, and edit-message mirrors
// decide-edited: only a pending, unconsumed, unbound record may be
// edited, and a successful edit moves it to `edited`.

#[derive(Debug, Serialize)]
pub(crate) struct ApprovalView {
    approval_id: String,
    /// Local alias: existing clients name approval ids tool_call_id.
    tool_call_id: String,
    tool: String,
    status: String,
    consumed: bool,
    bound: bool,
    binding: Option<ApprovalBinding>,
    operation_digest: Option<String>,
    effect_binding: Option<String>,
    message: Option<String>,
    /// E2-A §2.7: the worker session that consumed this
    /// approval to create a run. Null when unconsumed.
    consumed_by_worker_session_id: Option<String>,
    /// D1-2 (Q-D1-24): the approval version (SQLite mode
    /// only; the in-memory map carries no version, so this
    /// is null in JSONL/shadow views). Null for unversioned
    /// rows.
    version: Option<u64>,
    /// D1-2 (Q-D1-24): the version this write superseded,
    /// set only on decided/edited writes (SQLite mode
    /// only; the in-memory map carries no version, so this
    /// is null in JSONL/shadow views). Null when no write
    /// has superseded anything yet.
    supersedes_version: Option<u64>,
}

pub(crate) fn approval_number(approval_id: &str) -> u64 {
    approval_id.strip_prefix("approval-").and_then(|n| n.parse::<u64>().ok()).unwrap_or(0)
}

pub(crate) fn approval_view(approval_id: &str, record: &ApprovalState) -> ApprovalView {
    ApprovalView {
        approval_id: approval_id.to_string(),
        tool_call_id: approval_id.to_string(),
        tool: record.tool.clone(),
        status: crate::server::approval_status_name(record.status).to_string(),
        consumed: record.consumed,
        bound: record.is_bound(),
        binding: record.binding.clone(),
        operation_digest: record.operation_digest.clone(),
        effect_binding: record.effect_binding.clone(),
        message: record.message.clone(),
        consumed_by_worker_session_id: record.consumed_by_worker_session_id.clone(),
        version: None,
        // In-memory map carries no version chain (see
        // `version` above); null, same as version.
        supersedes_version: None,
    }
}

pub(crate) fn approval_view_from_file(record: &ApprovalFileRecord) -> ApprovalView {
    ApprovalView {
        approval_id: record.tool_call_id.clone(),
        tool_call_id: record.tool_call_id.clone(),
        tool: record.tool.clone(),
        status: record.status.clone(),
        consumed: record.consumed,
        bound: record.binding.is_some()
            || record.operation_digest.is_some()
            || record.effect_binding.is_some(),
        binding: record.binding.clone(),
        operation_digest: record.operation_digest.clone(),
        effect_binding: record.effect_binding.clone(),
        message: record.message.clone(),
        consumed_by_worker_session_id: record.consumed_by_worker_session_id.clone(),
        version: record.version,
        supersedes_version: record.supersedes_version,
    }
}

pub(crate) fn unknown_approval() -> axum::response::Response {
    (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown approval_id" }))).into_response()
}

pub(crate) fn approval_editable(record: &ApprovalState) -> bool {
    record.status == ApprovalStatus::Pending && !record.consumed && !record.is_bound()
}

/// Approval views from the in-memory map (JSONL / shadow mode,
/// and the gate paths that mutate under the approvals lock).
pub(crate) fn approval_views_from_map(state: &AppState) -> Vec<ApprovalView> {
    let approvals = state.approvals.lock().expect("approval lock");
    let mut views: Vec<ApprovalView> =
        approvals.iter().map(|(id, record)| approval_view(id, record)).collect();
    views.sort_by_key(|view| approval_number(&view.approval_id));
    views
}

pub(crate) async fn approvals_list(State(state): State<AppState>) -> impl IntoResponse {
    // P0-D: SQLite mode reads SQLite; a broken database fails
    // closed (5xx) instead of serving a stale map.
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return sqlite_error_response();
        }
        let Some(sqlite) = &state.sqlite else {
            return sqlite_error_response();
        };
        let conn = sqlite.lock().expect("sqlite lock");
        return match sqlite_store::list_approvals(&conn) {
            Ok(rows) => {
                let mut views: Vec<ApprovalView> =
                    rows.iter().map(approval_view_from_file).collect();
                views.sort_by_key(|view| approval_number(&view.approval_id));
                Json(views).into_response()
            }
            Err(_) => sqlite_error_response(),
        };
    }
    Json(approval_views_from_map(&state)).into_response()
}

pub(crate) async fn approvals_history(State(state): State<AppState>) -> impl IntoResponse {
    // FU-1 (Q-R0-02): in SQLite mode history reads the `approvals`
    // table — one row per approval, latest state — exactly like
    // `GET /approvals`. The old read (the approval event log in
    // global order) was a lossy projection: `ApprovalView` drops
    // `event_type` and timestamps, so callers could not tell a
    // requested row from a decided one. The real audit source is
    // the `events` table itself (fold + `list_events_for` still
    // read it); this route no longer pretends to be it. A broken
    // database fails closed.
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return sqlite_error_response();
        }
        let Some(sqlite) = &state.sqlite else {
            return sqlite_error_response();
        };
        let conn = sqlite.lock().expect("sqlite lock");
        return match sqlite_store::list_approvals(&conn) {
            Ok(rows) => {
                let mut views: Vec<ApprovalView> =
                    rows.iter().map(approval_view_from_file).collect();
                views.sort_by_key(|view| approval_number(&view.approval_id));
                Json(views).into_response()
            }
            Err(_) => sqlite_error_response(),
        };
    }
    if let Some(path) = &state.approvals_path {
        let views: Vec<ApprovalView> =
            load_records(path).iter().map(approval_view_from_file).collect();
        return Json(views).into_response();
    }
    // No persistence configured: history degrades to the current
    // state, one entry per approval.
    Json(approval_views_from_map(&state)).into_response()
}

pub(crate) async fn approvals_get(
    State(state): State<AppState>,
    axum::extract::Path(approval_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_mode() == StateBackend::Sqlite {
        if state.sqlite_is_broken() {
            return sqlite_error_response();
        }
        let Some(sqlite) = &state.sqlite else {
            return sqlite_error_response();
        };
        let conn = sqlite.lock().expect("sqlite lock");
        return match sqlite_store::get_approval(&conn, &approval_id) {
            Ok(Some(record)) => Json(approval_view_from_file(&record)).into_response(),
            Ok(None) => unknown_approval(),
            Err(_) => sqlite_error_response(),
        };
    }
    let approvals = state.approvals.lock().expect("approval lock");
    match approvals.get(&approval_id) {
        Some(record) => Json(approval_view(&approval_id, record)).into_response(),
        None => unknown_approval(),
    }
}

pub(crate) async fn approval_editable_message(
    State(state): State<AppState>,
    axum::extract::Path(approval_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let approvals = state.approvals.lock().expect("approval lock");
    match approvals.get(&approval_id) {
        Some(record) => Json(serde_json::json!({
            "approval_id": approval_id,
            "message": record.message,
            "editable": approval_editable(record),
        }))
        .into_response(),
        None => unknown_approval(),
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct EditMessageDto {
    message: Option<String>,
}

pub(crate) async fn approval_edit_message(
    State(state): State<AppState>,
    axum::extract::Path(approval_id): axum::extract::Path<String>,
    Json(req): Json<EditMessageDto>,
) -> impl IntoResponse {
    let Some(message) = req.message.filter(|m| !m.trim().is_empty()) else {
        return bad_request("message must not be empty");
    };
    let mut approvals = state.approvals.lock().expect("approval lock");
    let Some(record) = approvals.get_mut(&approval_id) else {
        return unknown_approval();
    };
    if !approval_editable(record) {
        return (StatusCode::CONFLICT, Json(serde_json::json!({ "error": "approval_not_editable" }))).into_response();
    }
    let old_message = record.message.clone();
    let old_status = record.status;
    record.message = Some(message);
    record.status = ApprovalStatus::Edited;
    let view = approval_view(&approval_id, record);
    if crate::server::persist_approval(&state, &approval_id, record).is_err() {
        // Fail closed like approval_decide: an edit that cannot be
        // audited must not take effect.
        record.message = old_message;
        record.status = old_status;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({ "error": "approval_audit_failed" }))).into_response();
    }
    Json(view).into_response()
}

// --- D1-2: Policy versions + Mandates (read surface) ---
//
// HTTP-only in this batch (plan §1): no frontend views
// yet — Policy/Mandate reads land here first, UI follows
// when a reader exists.

/// GET /policies/versions: every Policy version, oldest
/// first (document + digest + supersedes chain).
pub(crate) async fn policies_versions_list(State(state): State<AppState>) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_policy_versions(&conn) {
        Ok(rows) => Json(rows).into_response(),
        Err(detail) => {
            eprintln!("warning: policy version list failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// GET /mandates: Mandate list, optionally filtered by
/// ?task_id= and/or ?execution_id= (same query-filter
/// precedent as /tasks/list).
#[derive(Debug, Deserialize)]
pub(crate) struct MandateListQuery {
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    execution_id: Option<String>,
}

pub(crate) async fn mandates_list(
    State(state): State<AppState>,
    Query(query): Query<MandateListQuery>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::list_mandates(
        &conn,
        query.task_id.as_deref(),
        query.execution_id.as_deref(),
    ) {
        Ok(rows) => Json(rows).into_response(),
        Err(detail) => {
            eprintln!("warning: mandate list failed ({detail})");
            sqlite_error_response()
        }
    }
}

/// GET /mandates/{id}: one Mandate row, or 404.
pub(crate) async fn mandates_get(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> impl IntoResponse {
    if state.sqlite_is_broken() {
        return sqlite_error_response();
    }
    let Some(sqlite) = &state.sqlite else {
        return sqlite_not_configured();
    };
    let conn = sqlite.lock().expect("sqlite lock");
    match sqlite_store::get_mandate(&conn, &id) {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown mandate" })),
        )
            .into_response(),
        Err(detail) => {
            eprintln!("warning: mandate get failed ({detail})");
            sqlite_error_response()
        }
    }
}
