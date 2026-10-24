//! Egress approval / HITL state machine.
//!
//! Approval is server-side (Mock Gateway) state, not a UI-local
//! variable. The frontend only displays it and collects a decision;
//! execution must check approval before running a sensitive call.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalStatus {
    NotRequired,
    Pending,
    Approved,
    Rejected,
    Edited,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalRecord {
    pub invocation_id: String,
    pub request_summary: String, // redacted summary, never raw args
    pub status: ApprovalStatus,
    pub decided_at: Option<String>, // ISO 8601
}

impl ApprovalRecord {
    pub fn may_execute(&self) -> bool {
        matches!(self.status, ApprovalStatus::NotRequired | ApprovalStatus::Approved | ApprovalStatus::Edited)
    }
}

/// Gate check: returns true only when execution is allowed.
/// Unapproved sensitive calls must be refused here, even if
/// the UI was bypassed.
pub fn may_execute(status: ApprovalStatus, sensitive: bool) -> bool {
    if !sensitive {
        return true;
    }
    matches!(status, ApprovalStatus::NotRequired | ApprovalStatus::Approved | ApprovalStatus::Edited)
}
