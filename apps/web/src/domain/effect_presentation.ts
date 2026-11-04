// Effect presentation mapping (P0-R plan §3.2): gateway words
// to UI words, presentation layer only.
//
// The gateway keeps exactly one lifecycle vocabulary (P0-C's
// eight words); this module never writes back to the gateway.
// It derives the display word from the gateway status plus the
// latest Evidence verdict (for succeeded rows) or the
// reconciliation conclusion (for reconciled rows). The
// document vocabulary has no failed or reconciling word:
// failed rows display as failed, reconciling rows as
// unknown（核查中）. observed is not a status at all -- it is
// the derived fact that a row carries Evidence, shown as the
// Evidence verdict badge next to the display word.
//
// Next-step copy never claims an external service confirmed
// anything: Mock connectors have no external truth to check,
// so a verified connector row means only that the gateway
// record matches the write-time expectation.

// Display word for one Effect row, per P0-R plan §3.2.
// latestVerdict is the verdict of the row's latest Evidence
// row (verified / mismatch / inconclusive), or null when the
// row has no Evidence. reconciliationResult is the
// reconciliation.result of a reconciled row (completed /
// failed), else null. Unrecognized verdicts / results fall
// back defensively: an unknown succeeded verdict reads as
// undetermined, an unknown reconciled result reads as
// unknown, and an unknown status word reads as unknown --
// never as verified.
export function presentEffectStatus(
  status: string,
  latestVerdict?: string | null,
  reconciliationResult?: string | null,
): string {
  switch (status) {
    case "planned":
      return "proposed";
    case "authorized":
      return "approved";
    case "started":
      return "dispatched";
    case "succeeded":
      switch (latestVerdict ?? null) {
        case null:
          return "dispatched 已完成（未验证）";
        case "verified":
          return "verified";
        case "mismatch":
          return "已完成（已漂移）";
        default:
          return "dispatched 已完成（验证未定）";
      }
    case "failed":
      return "failed";
    case "unknown":
      return "unknown";
    case "reconciling":
      return "unknown（核查中）";
    case "reconciled":
      switch (reconciliationResult ?? null) {
        case "completed":
          return "verified";
        case "failed":
          return "failed";
        default:
          return "unknown";
      }
    default:
      return "unknown";
  }
}

// Next step for one Evidence reason code. Verified-class
// reasons need no action; mismatch-class reasons need a
// human to check the drift (verification never repairs);
// inconclusive-class reasons cannot settle, so retry later
// or escalate to a human. Unknown codes fail closed to
// human review.
export function nextStepForReason(reason: string): string {
  switch (reason) {
    // Verified class: the observation matches the
    // write-time expectation (gateway record only for
    // connector rows -- never "externally confirmed").
    // target_absent is a delete expectation matched,
    // dir_present a mkdir expectation matched.
    case "written":
    case "hash_match":
    case "record_match":
    case "target_absent":
    case "dir_present":
      return "无需操作";
    // Mismatch class: the conclusion drifted; a human
    // decides whether to rewrite, accept, or roll back.
    case "hash_mismatch":
    case "target_missing":
    case "target_present":
    case "not_a_directory":
    case "record_mismatch":
    case "type_mismatch":
      return "人工核查";
    // Inconclusive class: the observation could not
    // settle (read error, missing write-time evidence).
    case "io_error":
    case "evidence_missing":
    case "symlink_refused":
    case "state_unclear":
      return "稍后重试或人工核查";
    default:
      return "人工核查";
  }
}
