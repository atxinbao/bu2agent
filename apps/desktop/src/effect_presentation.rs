//! Effect presentation mapping (P0-R plan §3.2): gateway words
//! to UI words, presentation layer only.
//!
//! The gateway keeps exactly one lifecycle vocabulary (P0-C's
//! eight words); this module never writes back to the gateway.
//! It derives the display word from the gateway status plus the
//! latest Evidence verdict (for `succeeded` rows) or the
//! reconciliation conclusion (for `reconciled` rows). The
//! document vocabulary has no `failed` or `reconciling` word:
//! failed rows display as `failed`, reconciling rows as
//! `unknown（核查中）`. `observed` is not a status at all -- it
//! is the derived fact that a row carries Evidence, shown as
//! the Evidence verdict badge next to the display word.
//!
//! Next-step copy never claims an external service confirmed
//! anything: Mock connectors have no external truth to check,
//! so a `verified` connector row means only that the gateway
//! record matches the write-time expectation.

/// Display word for one Effect row, per P0-R plan §3.2.
/// `latest_evidence_verdict` is the verdict of the row's
/// latest Evidence row (`verified` / `mismatch` /
/// `inconclusive`), or None when the row has no Evidence.
/// `reconciliation_result` is the `reconciliation.result` of
/// a `reconciled` row (`completed` / `failed`), else None.
/// Unrecognized verdicts / results fall back defensively:
/// an unknown `succeeded` verdict reads as undetermined, an
/// unknown `reconciled` result reads as unknown, and an
/// unknown status word reads as unknown -- never as verified.
pub fn present_effect_status(
    status: &str,
    latest_evidence_verdict: Option<&str>,
    reconciliation_result: Option<&str>,
) -> &'static str {
    match status {
        "planned" => "proposed",
        "authorized" => "approved",
        "started" => "dispatched",
        "succeeded" => match latest_evidence_verdict {
            None => "dispatched 已完成（未验证）",
            Some("verified") => "verified",
            Some("mismatch") => "已完成（已漂移）",
            Some(_) => "dispatched 已完成（验证未定）",
        },
        "failed" => "failed",
        "unknown" => "unknown",
        "reconciling" => "unknown（核查中）",
        "reconciled" => match reconciliation_result {
            Some("completed") => "verified",
            Some("failed") => "failed",
            _ => "unknown",
        },
        _ => "unknown",
    }
}

/// Next step for one Evidence reason code. Verified-class
/// reasons need no action; mismatch-class reasons need a
/// human to check the drift (verification never repairs);
/// inconclusive-class reasons cannot settle, so retry later
/// or escalate to a human. Unknown codes fail closed to
/// human review.
pub fn next_step_for_reason(reason: &str) -> &'static str {
    match reason {
        // Verified class: the observation matches the
        // write-time expectation (gateway record only for
        // connector rows -- never "externally confirmed").
        // `target_absent` is a delete expectation matched,
        // `dir_present` a mkdir expectation matched.
        "written" | "hash_match" | "record_match" | "target_absent" | "dir_present" => "无需操作",
        // Mismatch class: the conclusion drifted; a human
        // decides whether to rewrite, accept, or roll back.
        "hash_mismatch" | "target_missing" | "target_present" | "not_a_directory"
        | "record_mismatch" | "type_mismatch" => "人工核查",
        // Inconclusive class: the observation could not
        // settle (read error, missing write-time evidence).
        "io_error" | "evidence_missing" | "symlink_refused" | "state_unclear" => "稍后重试或人工核查",
        _ => "人工核查",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planned_maps_to_proposed() {
        assert_eq!(present_effect_status("planned", None, None), "proposed");
    }

    #[test]
    fn authorized_maps_to_approved() {
        assert_eq!(present_effect_status("authorized", None, None), "approved");
    }

    #[test]
    fn started_maps_to_dispatched() {
        assert_eq!(present_effect_status("started", None, None), "dispatched");
    }

    #[test]
    fn succeeded_without_evidence_is_unverified() {
        assert_eq!(
            present_effect_status("succeeded", None, None),
            "dispatched 已完成（未验证）"
        );
    }

    #[test]
    fn succeeded_with_verified_evidence_is_verified() {
        assert_eq!(present_effect_status("succeeded", Some("verified"), None), "verified");
    }

    #[test]
    fn succeeded_with_mismatch_evidence_is_drifted_not_verified() {
        let shown = present_effect_status("succeeded", Some("mismatch"), None);
        assert_eq!(shown, "已完成（已漂移）");
        assert_ne!(shown, "verified");
    }

    #[test]
    fn succeeded_with_inconclusive_evidence_is_undetermined() {
        assert_eq!(
            present_effect_status("succeeded", Some("inconclusive"), None),
            "dispatched 已完成（验证未定）"
        );
    }

    #[test]
    fn failed_maps_to_failed() {
        assert_eq!(present_effect_status("failed", None, None), "failed");
    }

    #[test]
    fn unknown_maps_to_unknown() {
        assert_eq!(present_effect_status("unknown", None, None), "unknown");
    }

    #[test]
    fn reconciling_maps_to_unknown_in_progress() {
        assert_eq!(present_effect_status("reconciling", None, None), "unknown（核查中）");
    }

    #[test]
    fn reconciled_completed_maps_to_verified() {
        assert_eq!(present_effect_status("reconciled", None, Some("completed")), "verified");
    }

    #[test]
    fn reconciled_failed_maps_to_failed() {
        assert_eq!(present_effect_status("reconciled", None, Some("failed")), "failed");
    }

    #[test]
    fn next_step_verified_class_needs_no_action() {
        for reason in ["written", "hash_match", "record_match", "target_absent", "dir_present"] {
            assert_eq!(next_step_for_reason(reason), "无需操作", "{reason}");
        }
    }

    #[test]
    fn next_step_mismatch_class_needs_human_review() {
        for reason in [
            "hash_mismatch",
            "target_missing",
            "target_present",
            "not_a_directory",
            "record_mismatch",
            "type_mismatch",
        ] {
            assert_eq!(next_step_for_reason(reason), "人工核查", "{reason}");
        }
    }

    #[test]
    fn next_step_inconclusive_class_retries_or_escalates() {
        for reason in ["io_error", "evidence_missing", "symlink_refused", "state_unclear"] {
            assert_eq!(next_step_for_reason(reason), "稍后重试或人工核查", "{reason}");
        }
    }

    #[test]
    fn next_step_unknown_reason_fails_closed_to_human_review() {
        assert_eq!(next_step_for_reason("something_new"), "人工核查");
    }

    #[test]
    fn next_step_copy_never_claims_external_confirmation() {
        for reason in [
            "written",
            "hash_match",
            "hash_mismatch",
            "target_missing",
            "target_present",
            "not_a_directory",
            "record_match",
            "record_mismatch",
            "io_error",
            "evidence_missing",
        ] {
            let step = next_step_for_reason(reason);
            assert!(!step.contains("外部服务"), "{reason}: {step}");
            assert!(!step.contains("已确认"), "{reason}: {step}");
        }
    }
}
