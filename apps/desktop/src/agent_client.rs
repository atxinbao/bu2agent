//! Agent invocation client: Rust shell -> Mock Gateway, with the
//! approval loop closed end to end.
//!
//! Flow for one tool call, in order, with no step skippable:
//! 1. Client-side registry gate: unknown tools are rejected
//!    before any request; tools that need approval are not
//!    executed without a decided approval record.
//! 2. `POST /approval/request` opens a pending record for
//!    approval tools (non-approval tools need no record).
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

#[derive(Debug, Serialize)]
struct ApprovalRequestDto {
    tool: String,
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

    pub fn request_approval(&self, tool: &str) -> std::result::Result<ApprovalRequest, AgentError> {
        let Some(requires) = self.requires_approval(tool) else {
            return Err(invalid("unknown tool").into());
        };
        let body = serde_json::to_string(&ApprovalRequestDto { tool: tool.to_string() })
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
            return Err(denied("tool call refused: approval required or rejected").into());
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
        let request = self.request_approval(tool)?;
        let Some(tool_call_id) = request.tool_call_id else {
            return Err(transport_error("approval tool returned no tool_call_id").into());
        };
        match decision {
            ToolDecision::Ask => Err(AgentError::ApprovalPending { tool_call_id }),
            ToolDecision::Approve | ToolDecision::Reject => {
                self.decide_approval(&tool_call_id, decision)?;
                self.call_tool(tool, args, Some(&tool_call_id))
            }
        }
    }
}
