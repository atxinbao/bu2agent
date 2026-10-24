//! Contract tests for the Rust client shell skeleton.
//!
//! These tests pin the rules in ARCHITECTURE.md and in
//! packages/contract/schemas/*.json. A field rename, a new
//! terminal state, a wider bridge whitelist or a weaker approval
//! gate must break a test here -- that is the point.
//!
//! Execution (hybrid loop): pre-screened with `cargo test` in the
//! VM mirror, then finally verified on the Mac with
//! `run-tests.command` / `cargo test`. Do not report them as passing
//! until a run has actually happened.

use agent_desktop::approval::{self, ApprovalRecord, ApprovalStatus};
use agent_desktop::bridge;
use agent_desktop::file_client::MAX_AUTH_RETRIES;
use agent_desktop::gateway::GatewayTargetKey;
use agent_desktop::node::TurnState;
use agent_desktop::observability::redact;
use agent_desktop::permission::{PermissionSnapshot, SystemStatus};
use agent_desktop::session::SessionManager;
use agent_desktop::tools::ToolRegistry;

fn target(generation: u64) -> GatewayTargetKey {
    GatewayTargetKey {
        vm_name: "mock-vm-1".to_string(),
        gateway_url: "https://mock.local/gateway".to_string(),
        vm_id: None,
        generation,
    }
}

#[test]
fn generation_bump_invalidates_old_target() {
    let current = target(2);
    assert!(current.is_stale(&target(1)));
    assert!(!current.is_stale(&target(2)));
}

#[test]
fn url_or_vm_change_invalidates_old_target() {
    let current = target(1);
    let mut other = target(1);
    other.gateway_url = "https://other.local/gateway".to_string();
    assert!(current.is_stale(&other));
    let mut other2 = target(1);
    other2.vm_name = "mock-vm-2".to_string();
    assert!(current.is_stale(&other2));
}

#[test]
fn bridge_whitelist_is_closed() {
    assert!(bridge::is_allowed("gateway.request"));
    assert!(bridge::is_allowed("fs.read"));
    assert!(bridge::is_allowed("approval.decide"));
    // Anything not listed must be refused, even if it sounds plausible.
    assert!(!bridge::is_allowed("shell.exec"));
    assert!(!bridge::is_allowed("fs.read_all"));
    assert!(!bridge::is_allowed(""));
}

#[test]
fn bridge_version_mismatch_fails() {
    assert!(bridge::check_version(bridge::BRIDGE_VERSION));
    assert!(!bridge::check_version("999"));
    assert!(!bridge::check_version(""));
}

#[test]
fn approval_gate_blocks_unapproved_sensitive_calls() {
    assert!(!approval::may_execute(ApprovalStatus::Pending, true));
    assert!(!approval::may_execute(ApprovalStatus::Rejected, true));
    assert!(approval::may_execute(ApprovalStatus::Approved, true));
    assert!(approval::may_execute(ApprovalStatus::Edited, true));
    assert!(approval::may_execute(ApprovalStatus::NotRequired, true));
    // Non-sensitive calls do not need approval.
    assert!(approval::may_execute(ApprovalStatus::Pending, false));
}

#[test]
fn approval_record_gate_matches_free_function() {
    let rec = ApprovalRecord {
        invocation_id: "inv-1".to_string(),
        request_summary: "fs.write (redacted)".to_string(),
        status: ApprovalStatus::Pending,
        decided_at: None,
    };
    assert!(!rec.may_execute());
    assert!(!approval::may_execute(rec.status, true));
}

#[test]
fn turn_terminal_states_are_exactly_three() {
    assert!(TurnState::Completed.is_terminal());
    assert!(TurnState::Cancelled.is_terminal());
    assert!(TurnState::Failed.is_terminal());
    for s in [
        TurnState::Created,
        TurnState::Submitted,
        TurnState::Thinking,
        TurnState::ToolCallPending,
        TurnState::AwaitingApproval,
        TurnState::ToolRunning,
        TurnState::Streaming,
    ] {
        assert!(!s.is_terminal(), "{s:?} must not be terminal");
    }
}

#[test]
fn permission_snapshot_gates_capability() {
    let granted = PermissionSnapshot {
        capability: "files".to_string(),
        system_status: SystemStatus::Granted,
        scope: vec!["read".to_string()],
        source: "test".to_string(),
        updated_at: "2026-10-24T00:00:00Z".to_string(),
    };
    assert!(granted.can_proceed());
    let denied = PermissionSnapshot { system_status: SystemStatus::Denied, ..granted.clone() };
    assert!(!denied.can_proceed());
}

#[test]
fn auth_refresh_retries_at_most_once() {
    assert_eq!(MAX_AUTH_RETRIES, 1);
}

#[test]
fn session_logout_clears_target() {
    let mut session = SessionManager::new();
    session.activate(target(1));
    assert!(session.target_key.is_some());
    session.logout();
    assert!(session.target_key.is_none());
}

#[test]
fn phase1_tool_registry_requires_approval_for_writes() {
    let registry = ToolRegistry::phase1();
    let write = registry.get("fs.write").expect("fs.write must be registered");
    assert!(write.requires_approval);
    let list = registry.get("fs.list").expect("fs.list must be registered");
    assert!(!list.requires_approval);
    assert!(registry.get("mail.send").is_none());
}

#[test]
fn redact_removes_token_marker() {
    let out = redact("token=abc123secret");
    assert!(out.contains("[REDACTED]"));
    assert!(!out.contains("abc123secret"));
}
