//! Agent invocation client: Rust shell -> Mock Gateway, with the
//! approval loop closed end to end.
//!
//! Flow for one tool call, in order, with no step skippable:
//! 1. Client-side registry gate: unknown tools are rejected
//!    before any request; tools that need approval are not
//!    executed without a decided approval record.
//! 2. `POST /approval/request` opens a pending record for
//!    approval tools (non-approval tools need no record).
//!    Round 2: file tools present the exact operation in the
//!    request, so the server binds the record to it.
//! 3. `POST /approval/decide` records the user's decision.
//! 4. `POST /agent/tool/call` executes, and the server re-checks
//!    its own record: a call with no record, a pending record,
//!    or a rejected record is refused server-side, even if the
//!    client claimed approval. The client gate is defense in
//!    depth; the server gate is the enforcement point.
//!
//! `echo` is a Mock-only demo tool (no approval), not a real
//! tool; the registry tools are `fs.list` / `fs.read` (no
//! approval) and `fs.write` / `fs.delete` (approval).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::file_client::to_base64;
use crate::http_client::{gateway_error, transport_error, HttpClient};
use crate::tools::ToolRegistry;
use crate::{ErrorCode, Result, UnifiedError, UserAction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResult {
    pub tool: String,
    pub result: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalRequest {
    pub tool_call_id: Option<String>,
    pub requires_approval: bool,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolDecision {
    /// Stop at a pending record and report its id.
    Ask,
    /// Decide approved, then call.
    Approve,
    /// Decide rejected, then call (the server must refuse).
    Reject,
}

#[derive(Debug)]
pub enum AgentError {
    Unified(UnifiedError),
    /// Approval is pending; the caller holds this id and must
    /// decide before the tool can run.
    ApprovalPending { tool_call_id: String },
}

impl From<UnifiedError> for AgentError {
    fn from(e: UnifiedError) -> Self {
        AgentError::Unified(e)
    }
}

fn denied(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::PermissionDenied,
        capability: Some("agent".to_string()),
        retryable: false,
        user_action: UserAction::RequestApproval,
        redacted_detail: detail.to_string(),
    }
}

fn invalid(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::InvalidInput,
        capability: Some("agent".to_string()),
        retryable: false,
        user_action: UserAction::None,
        redacted_detail: detail.to_string(),
    }
}

/// Decode one invoke-transport string arg as a JSON
/// array of strings (the mail.send `to` shape): the
/// invoke transport only carries string args, so list
/// values arrive as JSON array strings. A missing arg
/// or a non-array value is an invalid-input error, not
/// a silent empty list.
fn decode_string_list(
    args: &HashMap<String, String>,
    tool: &str,
    field: &str,
) -> std::result::Result<Vec<String>, UnifiedError> {
    let raw = args
        .get(field)
        .ok_or_else(|| invalid(&format!("{tool} approval requires {field}")))?;
    serde_json::from_str(raw)
        .map_err(|_| invalid(&format!("{tool} approval {field} must be a JSON array of strings")))
}

#[derive(Debug, Serialize)]
struct ApprovalRequestDto {
    tool: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    append: Option<bool>,
    /// D1-6b-1: the mail send presentation. The server
    /// recomputes `mail_send_digest` from these fields;
    /// a client hash is never trusted.
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<String>,
    /// MCL-4: the iMessage send presentation. The server
    /// recomputes `imessage_send_digest` from the thread
    /// id + body; a client hash is never trusted.
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    /// D1-6b-2: the calendar create presentation. The
    /// server recomputes `calendar_create_digest` from
    /// these fields; a client hash is never trusted.
    #[serde(skip_serializing_if = "Option::is_none")]
    calendar_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    start_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    all_day: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes: Option<String>,
    /// D1-6b-3: the mail delete presentation. The server
    /// recomputes `mail_delete_digest` from this native
    /// id; a client hash is never trusted.
    #[serde(skip_serializing_if = "Option::is_none")]
    native_id: Option<String>,
    /// D1-6b-4: the note create / append / delete
    /// presentation. The server recomputes the note
    /// digests from these fields; a client hash is never
    /// trusted. `body` (the shared mail.send field) is
    /// reused for the note body / appended text.
    #[serde(skip_serializing_if = "Option::is_none")]
    folder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    note_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_body_hash: Option<String>,
    /// MCL-5: the contacts write presentation. The
    /// server recomputes the contact digests from these
    /// fields; a client hash is never trusted. `phones`
    /// / `emails` arrive as JSON array strings on the
    /// invoke transport (the mail.send `to` shape).
    #[serde(skip_serializing_if = "Option::is_none")]
    contact_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    phones: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    emails: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ApprovalRequestResponse {
    tool_call_id: Option<String>,
    requires_approval: bool,
    status: String,
}

#[derive(Debug, Serialize)]
struct ApprovalDecideDto {
    tool_call_id: String,
    status: String,
}

#[derive(Debug, Serialize)]
struct ToolCallDto {
    tool: String,
    args: HashMap<String, String>,
    tool_call_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ToolCallResponse {
    tool: String,
    result: serde_json::Value,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct AgentClient {
    http: HttpClient,
    registry: ToolRegistry,
}

impl AgentClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)?, registry: ToolRegistry::phase4() })
    }

    fn requires_approval(&self, tool: &str) -> Option<bool> {
        if tool == "echo" {
            return Some(false);
        }
        self.registry.get(tool).map(|spec| spec.requires_approval)
    }

    fn post_approval(
        &self,
        dto: &ApprovalRequestDto,
        requires: bool,
    ) -> std::result::Result<ApprovalRequest, AgentError> {
        let body = serde_json::to_string(dto)
            .map_err(|_| transport_error("cannot encode approval request"))?;
        let (status, resp_body) = self.http.request("POST", "/approval/request", Some(&body))?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status).into());
        }
        let dto: ApprovalRequestResponse = serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))?;
        if dto.requires_approval != requires {
            return Err(transport_error("mock gateway disagrees on tool approval flag").into());
        }
        Ok(ApprovalRequest {
            tool_call_id: dto.tool_call_id,
            requires_approval: dto.requires_approval,
            status: dto.status,
        })
    }

    pub fn request_approval(&self, tool: &str) -> std::result::Result<ApprovalRequest, AgentError> {
        let Some(requires) = self.requires_approval(tool) else {
            return Err(invalid("unknown tool").into());
        };
        self.post_approval(
            &ApprovalRequestDto { tool: tool.to_string(), path: None, content_base64: None, append: None, to: None, subject: None, body: None, thread_id: None, calendar_id: None, title: None, start_ms: None, end_ms: None, all_day: None, location: None, notes: None, native_id: None, folder: None, name: None, note_id: None, base_body_hash: None, contact_id: None, display_name: None, organization: None, job_title: None, phones: None, emails: None },
            requires,
        )
    }

    /// Bound approval request (round 2 unified execution): file
    /// tools must present the exact operation, so the server can
    /// bind the record to it. Agent writes are always overwrite
    /// (the tool call has no append mode), so the request binds
    /// `append: false`. Other tools fall back to the tool-only
    /// request; unknown tools are rejected before any request.
    pub fn request_approval_for(
        &self,
        tool: &str,
        args: Option<&HashMap<String, String>>,
    ) -> std::result::Result<ApprovalRequest, AgentError> {
        let Some(requires) = self.requires_approval(tool) else {
            return Err(invalid("unknown tool").into());
        };
        let dto = match tool {
            "fs.write" => {
                let args = args.ok_or_else(|| invalid("fs.write approval requires path and text"))?;
                let path = args.get("path").ok_or_else(|| invalid("fs.write approval requires path"))?;
                let text = args.get("text").ok_or_else(|| invalid("fs.write approval requires text"))?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: Some(path.clone()),
                    content_base64: Some(to_base64(text.as_bytes())),
                    append: Some(false),
                    to: None,
                    subject: None,
                    body: None,
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            "fs.delete" => {
                let args = args.ok_or_else(|| invalid("fs.delete approval requires path"))?;
                let path = args.get("path").ok_or_else(|| invalid("fs.delete approval requires path"))?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: Some(path.clone()),
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: None,
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            // D1-6b-1: a mail send approval binds the exact
            // message (Q-D1-6b-22). `to` arrives as a JSON
            // array string — the invoke transport only
            // carries string args, and the server
            // recomputes the digest from the decoded
            // list, never from a client hash.
            "mail.send" => {
                let args = args
                    .ok_or_else(|| invalid("mail.send approval requires to, subject and body"))?;
                let to_raw = args
                    .get("to")
                    .ok_or_else(|| invalid("mail.send approval requires to"))?;
                let to: Vec<String> = serde_json::from_str(to_raw).map_err(|_| {
                    invalid("mail.send approval to must be a JSON array of addresses")
                })?;
                let subject = args
                    .get("subject")
                    .ok_or_else(|| invalid("mail.send approval requires subject"))?;
                let body = args
                    .get("body")
                    .ok_or_else(|| invalid("mail.send approval requires body"))?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: Some(to),
                    subject: Some(subject.clone()),
                    body: Some(body.clone()),
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            // MCL-4 revisit: an iMessage send approval binds
            // the thread id + body (Q-MCL-22). The server
            // recomputes `imessage_send_digest` from them,
            // never from a client hash. Both fields must be
            // present and non-empty (the gateway's two 400s);
            // a missing field is a client error here, not a
            // tool-only request — a tool-only record could
            // never authorize the real send.
            "imessage.send" => {
                let args = args
                    .ok_or_else(|| invalid("imessage.send approval requires thread_id and body"))?;
                let (Some(thread_id), Some(body)) = (args.get("thread_id"), args.get("body"))
                else {
                    return Err(
                        invalid("imessage.send approval requires thread_id and body").into(),
                    );
                };
                if thread_id.is_empty() || body.is_empty() {
                    return Err(invalid(
                        "imessage.send approval requires non-empty thread_id and body",
                    )
                    .into());
                }
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: Some(body.clone()),
                    thread_id: Some(thread_id.clone()),
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            // D1-6b-2: a calendar create approval binds the
            // exact event (Q-D1-6b-22). The invoke
            // transport only carries string args, so
            // start/end/all-day arrive as strings and are
            // parsed here; the server recomputes the digest
            // from the decoded values, never from a client
            // hash.
            "calendar.create" => {
                let args = args.ok_or_else(|| {
                    invalid("calendar.create approval requires calendar_id, title, start_ms, end_ms and all_day")
                })?;
                let calendar_id = args
                    .get("calendar_id")
                    .ok_or_else(|| invalid("calendar.create approval requires calendar_id"))?;
                let title = args
                    .get("title")
                    .ok_or_else(|| invalid("calendar.create approval requires title"))?;
                let start_ms_raw = args
                    .get("start_ms")
                    .ok_or_else(|| invalid("calendar.create approval requires start_ms"))?;
                let start_ms: i64 = start_ms_raw
                    .parse()
                    .map_err(|_| invalid("calendar.create approval start_ms must be an integer"))?;
                let end_ms_raw = args
                    .get("end_ms")
                    .ok_or_else(|| invalid("calendar.create approval requires end_ms"))?;
                let end_ms: i64 = end_ms_raw
                    .parse()
                    .map_err(|_| invalid("calendar.create approval end_ms must be an integer"))?;
                let all_day_raw = args
                    .get("all_day")
                    .ok_or_else(|| invalid("calendar.create approval requires all_day"))?;
                let all_day: bool = all_day_raw.parse().map_err(|_| {
                    invalid("calendar.create approval all_day must be true or false")
                })?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: None,
                    thread_id: None,
                    calendar_id: Some(calendar_id.clone()),
                    title: Some(title.clone()),
                    start_ms: Some(start_ms),
                    end_ms: Some(end_ms),
                    all_day: Some(all_day),
                    location: args.get("location").cloned(),
                    notes: args.get("notes").cloned(),
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            // D1-6b-3: a mail delete approval binds the
            // native id (Q-D1-6b-22). The server
            // recomputes the digest from it, never from a
            // client hash.
            "mail.delete" => {
                let args = args
                    .ok_or_else(|| invalid("mail.delete approval requires native_id"))?;
                let native_id = args
                    .get("native_id")
                    .ok_or_else(|| invalid("mail.delete approval requires native_id"))?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: None,
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: Some(native_id.clone()),
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            // D1-6b-4: a note create approval binds the
            // exact note (Q-D1-6b-22): folder, name and
            // body. The server recomputes the digest
            // from these fields, never from a client hash.
            "notes.create" => {
                let args = args.ok_or_else(|| {
                    invalid("notes.create approval requires folder, name and body")
                })?;
                let folder = args
                    .get("folder")
                    .ok_or_else(|| invalid("notes.create approval requires folder"))?;
                let name = args
                    .get("name")
                    .ok_or_else(|| invalid("notes.create approval requires name"))?;
                let body = args
                    .get("body")
                    .ok_or_else(|| invalid("notes.create approval requires body"))?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: Some(body.clone()),
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: Some(folder.clone()),
                    name: Some(name.clone()),
                    note_id: None,
                    base_body_hash: None,
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            // D1-6b-4: a note append approval binds the
            // note id, the base body hash (the version
            // the approver saw), and the appended text
            // (Q-D1-6b-22). The server recomputes the
            // digest from these fields, never from a
            // client hash.
            "notes.append" => {
                let args = args.ok_or_else(|| {
                    invalid("notes.append approval requires note_id, base_body_hash and body")
                })?;
                let note_id = args
                    .get("note_id")
                    .ok_or_else(|| invalid("notes.append approval requires note_id"))?;
                let base_body_hash = args.get("base_body_hash").ok_or_else(|| {
                    invalid("notes.append approval requires base_body_hash")
                })?;
                let body = args
                    .get("body")
                    .ok_or_else(|| invalid("notes.append approval requires body"))?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: Some(body.clone()),
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: Some(note_id.clone()),
                    base_body_hash: Some(base_body_hash.clone()),
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            // D1-6b-4: a note delete approval binds the
            // note id (Q-D1-6b-22). The server recomputes
            // the digest from it, never from a client hash.
            "notes.delete" => {
                let args = args
                    .ok_or_else(|| invalid("notes.delete approval requires note_id"))?;
                let note_id = args
                    .get("note_id")
                    .ok_or_else(|| invalid("notes.delete approval requires note_id"))?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: None,
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: Some(note_id.clone()),
                    base_body_hash: None,
                    contact_id: None,
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            // MCL-5: a contacts create approval binds the
            // exact content (Q-MCL-22). `phones` /
            // `emails` arrive as JSON array strings (the
            // mail.send `to` shape); the server
            // recomputes the digest from the decoded
            // values, never from a client hash.
            "contacts.create" => {
                let args = args.ok_or_else(|| {
                    invalid("contacts.create approval requires display_name, phones and emails")
                })?;
                let display_name = args
                    .get("display_name")
                    .ok_or_else(|| invalid("contacts.create approval requires display_name"))?;
                let phones = decode_string_list(args, "contacts.create", "phones")?;
                let emails = decode_string_list(args, "contacts.create", "emails")?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: None,
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: None,
                    display_name: Some(display_name.clone()),
                    organization: Some(args.get("organization").cloned().unwrap_or_default()),
                    job_title: Some(args.get("job_title").cloned().unwrap_or_default()),
                    phones: Some(phones),
                    emails: Some(emails),
                }
            }
            // MCL-5: a contacts update approval binds the
            // contact id and the exact new content
            // (Q-MCL-22). The base hash is NOT part of
            // the digest (a retry with a fresh base and
            // the same content still matches); the
            // server recomputes the digest from these
            // fields, never from a client hash.
            "contacts.update" => {
                let args = args.ok_or_else(|| {
                    invalid("contacts.update approval requires contact_id, display_name, phones and emails")
                })?;
                let contact_id = args
                    .get("contact_id")
                    .ok_or_else(|| invalid("contacts.update approval requires contact_id"))?;
                let display_name = args
                    .get("display_name")
                    .ok_or_else(|| invalid("contacts.update approval requires display_name"))?;
                let phones = decode_string_list(args, "contacts.update", "phones")?;
                let emails = decode_string_list(args, "contacts.update", "emails")?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: None,
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: Some(contact_id.clone()),
                    display_name: Some(display_name.clone()),
                    organization: Some(args.get("organization").cloned().unwrap_or_default()),
                    job_title: Some(args.get("job_title").cloned().unwrap_or_default()),
                    phones: Some(phones),
                    emails: Some(emails),
                }
            }
            // MCL-5: a contacts delete approval binds the
            // contact id (Q-MCL-22, the notes.delete
            // shape — the local id, not the native id).
            // The server recomputes the digest from it,
            // never from a client hash.
            "contacts.delete" => {
                let args = args
                    .ok_or_else(|| invalid("contacts.delete approval requires contact_id"))?;
                let contact_id = args
                    .get("contact_id")
                    .ok_or_else(|| invalid("contacts.delete approval requires contact_id"))?;
                ApprovalRequestDto {
                    tool: tool.to_string(),
                    path: None,
                    content_base64: None,
                    append: None,
                    to: None,
                    subject: None,
                    body: None,
                    thread_id: None,
                    calendar_id: None,
                    title: None,
                    start_ms: None,
                    end_ms: None,
                    all_day: None,
                    location: None,
                    notes: None,
                    native_id: None,
                    folder: None,
                    name: None,
                    note_id: None,
                    base_body_hash: None,
                    contact_id: Some(contact_id.clone()),
                    display_name: None,
                    organization: None,
                    job_title: None,
                    phones: None,
                    emails: None,
                }
            }
            _ => {
                return self.request_approval(tool);
            }
        };
        self.post_approval(&dto, requires)
    }

    pub fn decide_approval(
        &self,
        tool_call_id: &str,
        decision: ToolDecision,
    ) -> std::result::Result<(), AgentError> {
        let status = match decision {
            ToolDecision::Approve => "approved",
            ToolDecision::Reject => "rejected",
            ToolDecision::Ask => return Err(invalid("Ask is not a decision").into()),
        };
        let body = serde_json::to_string(&ApprovalDecideDto {
            tool_call_id: tool_call_id.to_string(),
            status: status.to_string(),
        })
        .map_err(|_| transport_error("cannot encode approval decision"))?;
        let (http_status, _) = self.http.request("POST", "/approval/decide", Some(&body))?;
        if !(200..300).contains(&http_status) {
            return Err(gateway_error(http_status).into());
        }
        Ok(())
    }

    pub fn call_tool(
        &self,
        tool: &str,
        args: &HashMap<String, String>,
        tool_call_id: Option<&str>,
    ) -> std::result::Result<ToolResult, AgentError> {
        if self.requires_approval(tool).is_none() {
            return Err(invalid("unknown tool").into());
        }
        let body = serde_json::to_string(&ToolCallDto {
            tool: tool.to_string(),
            args: args.clone(),
            tool_call_id: tool_call_id.map(|id| id.to_string()),
        })
        .map_err(|_| transport_error("cannot encode tool call"))?;
        let (status, resp_body) = self.http.request("POST", "/agent/tool/call", Some(&body))?;
        if status == 403 {
            // Neutral here: a direct call carries no decision
            // context, so it must not claim a user rejection.
            // `run_tool` rewords this by the decision it made.
            return Err(denied("tool call refused by the server approval gate").into());
        }
        if !(200..300).contains(&status) {
            return Err(gateway_error(status).into());
        }
        let dto: ToolCallResponse = serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))?;
        Ok(ToolResult { tool: dto.tool, result: dto.result })
    }

    /// Run one tool call through the full loop.
    /// - Non-approval tools: called directly.
    /// - Approval tools + Ask: request, then stop with
    ///   `ApprovalPending` (nothing executed).
    /// - Approval tools + Approve/Reject: request, decide,
    ///   then call (Reject must be refused server-side).
    pub fn run_tool(
        &self,
        tool: &str,
        args: &HashMap<String, String>,
        decision: ToolDecision,
    ) -> std::result::Result<ToolResult, AgentError> {
        let Some(requires) = self.requires_approval(tool) else {
            return Err(invalid("unknown tool").into());
        };
        if !requires {
            return self.call_tool(tool, args, None);
        }
        let request = self.request_approval_for(tool, Some(args))?;
        let Some(tool_call_id) = request.tool_call_id else {
            return Err(transport_error("approval tool returned no tool_call_id").into());
        };
        match decision {
            ToolDecision::Ask => Err(AgentError::ApprovalPending { tool_call_id }),
            ToolDecision::Approve | ToolDecision::Reject => {
                self.decide_approval(&tool_call_id, decision)?;
                match self.call_tool(tool, args, Some(&tool_call_id)) {
                    // The decision is known here, so the 403
                    // wording can be exact: an approved call
                    // refused after approval is a mismatch (the
                    // operation changed, was already used, or
                    // expired), never a user rejection.
                    Err(AgentError::Unified(e))
                        if e.code == ErrorCode::PermissionDenied =>
                    {
                        Err(match decision {
                            ToolDecision::Approve => denied("approved operation does not match this execution (changed, already used, or expired); nothing was executed"),
                            ToolDecision::Reject => denied("tool call refused: approval was rejected"),
                            ToolDecision::Ask => unreachable!(),
                        }
                        .into())
                    }
                    other => other,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    /// What the capture server observed.
    struct CapturedRequest {
        count: usize,
        body: Option<String>,
    }

    /// A loopback server that records the requests it
    /// receives. With `response` it answers the first
    /// request with a canned 200 JSON body; without it,
    /// it only counts connections for `wait_ms` and then
    /// reports — the "no gateway request" proof for
    /// client-side rejections.
    fn spawn_capture_server(
        response: Option<String>,
        wait_ms: u64,
    ) -> (u16, mpsc::Receiver<CapturedRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind capture server");
        let port = listener.local_addr().expect("capture port").port();
        listener
            .set_nonblocking(true)
            .expect("nonblocking capture server");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_millis(wait_ms);
            let mut count = 0usize;
            let mut body: Option<String> = None;
            while std::time::Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        count += 1;
                        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                        let mut buf = Vec::new();
                        let mut chunk = [0u8; 1024];
                        loop {
                            match stream.read(&mut chunk) {
                                Ok(0) => break,
                                Ok(n) => {
                                    buf.extend_from_slice(&chunk[..n]);
                                    if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        let text = String::from_utf8_lossy(&buf);
                        let content_len = text.lines().find_map(|line| {
                            line.strip_prefix("Content-Length:")
                                .and_then(|v| v.trim().parse::<usize>().ok())
                        });
                        if let Some(len) = content_len {
                            let head_end =
                                text.find("\r\n\r\n").map(|i| i + 4).unwrap_or(text.len());
                            let mut body_bytes = buf[head_end..].to_vec();
                            while body_bytes.len() < len {
                                match stream.read(&mut chunk) {
                                    Ok(0) => break,
                                    Ok(n) => body_bytes.extend_from_slice(&chunk[..n]),
                                    Err(_) => break,
                                }
                            }
                            body = Some(String::from_utf8_lossy(&body_bytes).into_owned());
                        }
                        if let Some(response) = &response {
                            let wire = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                response.len(),
                                response
                            );
                            let _ = stream.write_all(wire.as_bytes());
                        }
                        break;
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
            let _ = tx.send(CapturedRequest { count, body });
        });
        (port, rx)
    }

    fn client_on(port: u16) -> AgentClient {
        AgentClient::new(&format!("http://127.0.0.1:{port}")).expect("test client")
    }

    fn args(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn invalid_input(err: AgentError) -> UnifiedError {
        match err {
            AgentError::Unified(e) => {
                assert_eq!(e.code, ErrorCode::InvalidInput);
                e
            }
            other => panic!("expected InvalidInput, got {other:?}"),
        }
    }

    #[test]
    fn registry_contains_imessage_send() {
        let registry = ToolRegistry::phase4();
        let spec = registry
            .get("imessage.send")
            .expect("imessage.send registered");
        assert!(spec.requires_approval);
        assert_eq!(spec.capability, "imessage");
        // The sibling read stays approval-free.
        let threads = registry
            .get("imessage.threads")
            .expect("imessage.threads registered");
        assert!(!threads.requires_approval);
    }

    #[test]
    fn imessage_send_approval_rejects_missing_or_empty_fields_without_a_request() {
        // One capture server for all five client-side
        // rejections: none of them may reach the gateway.
        let (port, rx) = spawn_capture_server(None, 500);
        let client = client_on(port);
        let cases: Vec<Option<HashMap<String, String>>> = vec![
            None,
            Some(args(&[("body", "hello")])),
            Some(args(&[("thread_id", "thread-1")])),
            Some(args(&[("thread_id", ""), ("body", "hello")])),
            Some(args(&[("thread_id", "thread-1"), ("body", "")])),
        ];
        for (i, case) in cases.into_iter().enumerate() {
            let err = client
                .request_approval_for("imessage.send", case.as_ref())
                .unwrap_err();
            let detail = invalid_input(err);
            assert!(
                detail
                    .redacted_detail
                    .contains("imessage.send approval requires"),
                "case {i}: {}",
                detail.redacted_detail
            );
        }
        let captured = rx
            .recv_timeout(Duration::from_secs(3))
            .expect("capture report");
        assert_eq!(
            captured.count, 0,
            "client-side rejections must not hit the gateway"
        );
        assert_eq!(captured.body, None);
    }

    #[test]
    fn imessage_send_approval_posts_thread_id_and_body() {
        let canned = r#"{"tool_call_id":"approval-1","requires_approval":true,"status":"pending"}"#
            .to_string();
        let (port, rx) = spawn_capture_server(Some(canned), 5_000);
        let client = client_on(port);
        let request = client
            .request_approval_for(
                "imessage.send",
                Some(&args(&[("thread_id", "thread-1"), ("body", "r1-test")])),
            )
            .expect("valid approval request");
        assert_eq!(request.tool_call_id.as_deref(), Some("approval-1"));
        assert!(request.requires_approval);
        assert_eq!(request.status, "pending");
        let captured = rx
            .recv_timeout(Duration::from_secs(3))
            .expect("capture report");
        assert_eq!(captured.count, 1);
        let body = captured.body.expect("captured request body");
        let value: serde_json::Value = serde_json::from_str(&body).expect("request body is JSON");
        assert_eq!(value["tool"], "imessage.send");
        assert_eq!(value["thread_id"], "thread-1");
        assert_eq!(value["body"], "r1-test");
    }

    #[test]
    fn unknown_tools_still_rejected_before_any_request() {
        let client = AgentClient::new("http://127.0.0.1:3001").expect("test client");
        for tool in ["does.not.exist", "imessage.message"] {
            let err = client.request_approval_for(tool, None).unwrap_err();
            let detail = invalid_input(err);
            assert_eq!(detail.redacted_detail, "unknown tool");
            let err = client.request_approval(tool).unwrap_err();
            let detail = invalid_input(err);
            assert_eq!(detail.redacted_detail, "unknown tool");
        }
    }
}
