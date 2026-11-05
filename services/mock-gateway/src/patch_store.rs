//! Patch domain helpers (P3-B): status vocabulary and the
//! gateway-computed unified diff.
//!
//! The diff is a review artifact only: apply writes the
//! proposed bytes stored on the PatchFile row (`new_text`),
//! never reconstructs content from the diff, so the diff
//! never has to be a patch-applier input — it only has to be
//! an accurate, readable unified diff of the proposal. Old
//! content is the proposal-time disk state; new content is
//! the proposed text. A missing old file uses `/dev/null`
//! semantics (full add).

/// Patch statuses (plan §4.5 truth table).
pub const PATCH_PROPOSED: &str = "proposed";
pub const PATCH_PARTIALLY_APPLIED: &str = "partially_applied";
pub const PATCH_APPLIED: &str = "applied";
pub const PATCH_REJECTED: &str = "rejected";
pub const PATCH_STALE: &str = "stale";

/// PatchFile statuses (plan §4.5 state machine).
pub const PATCHFILE_PENDING: &str = "pending";
pub const PATCHFILE_APPLIED: &str = "applied";
pub const PATCHFILE_FAILED: &str = "failed";
pub const PATCHFILE_REJECTED: &str = "rejected";
pub const PATCHFILE_STALE: &str = "stale";

/// All Patch statuses, for the `status` list filter.
pub const PATCH_STATUSES: [&str; 5] = [
    PATCH_PROPOSED,
    PATCH_PARTIALLY_APPLIED,
    PATCH_APPLIED,
    PATCH_REJECTED,
    PATCH_STALE,
];

/// Hunk context lines on each side (unified diff default).
const CONTEXT: usize = 3;
/// LCS cell cap: above this, the differ falls back to a
/// common-prefix/common-suffix single hunk (still a valid
/// unified diff, just less minimal). A 1 MiB file is ~25k
/// lines worst case; the cap keeps the table bounded.
const LCS_CELL_CAP: usize = 4_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Equal(usize, usize),
    Delete(usize),
    Insert(usize),
}

fn diff_ops(old: &[&str], new: &[&str]) -> Vec<Op> {
    let n = old.len();
    let m = new.len();
    if n.checked_mul(m).is_none_or(|cells| cells > LCS_CELL_CAP) {
        // Fallback: common prefix + common suffix, one hunk.
        let mut prefix = 0;
        while prefix < n.min(m) && old[prefix] == new[prefix] {
            prefix += 1;
        }
        let mut suffix = 0;
        while suffix < (n - prefix).min(m - prefix)
            && old[n - 1 - suffix] == new[m - 1 - suffix]
        {
            suffix += 1;
        }
        let mut ops = Vec::with_capacity(n + m);
        for i in 0..prefix {
            ops.push(Op::Equal(i, i));
        }
        for i in prefix..(n - suffix) {
            ops.push(Op::Delete(i));
        }
        for j in prefix..(m - suffix) {
            ops.push(Op::Insert(j));
        }
        for k in 0..suffix {
            ops.push(Op::Equal(n - suffix + k, m - suffix + k));
        }
        return ops;
    }
    // Classic LCS table, walked back greedily (deletes before
    // inserts on ties, so a replaced line reads `-` then `+`).
    let mut dp = vec![vec![0u32; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i][j] = if old[i] == new[j] {
                dp[i + 1][j + 1] + 1
            } else {
                dp[i + 1][j].max(dp[i][j + 1])
            };
        }
    }
    let mut ops = Vec::with_capacity(n + m);
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if old[i] == new[j] {
            ops.push(Op::Equal(i, j));
            i += 1;
            j += 1;
        } else if dp[i + 1][j] >= dp[i][j + 1] {
            ops.push(Op::Delete(i));
            i += 1;
        } else {
            ops.push(Op::Insert(j));
            j += 1;
        }
    }
    while i < n {
        ops.push(Op::Delete(i));
        i += 1;
    }
    while j < m {
        ops.push(Op::Insert(j));
        j += 1;
    }
    ops
}

/// `@@ -a,b +c,d @@` range: a count of 1 drops the `,1`,
/// a count of 0 shows the line before the hunk.
fn format_range(start_1based: usize, count: usize) -> String {
    if count == 0 {
        format!("{},0", start_1based.saturating_sub(1))
    } else if count == 1 {
        format!("{start_1based}")
    } else {
        format!("{start_1based},{count}")
    }
}

/// Gateway-computed unified diff of a proposal. `old` is
/// `None` for a file that does not exist yet (`/dev/null`
/// semantics: the whole new content is added). The diff is
/// deterministic: same inputs, same text.
pub fn unified_diff(old: Option<&str>, new: &str, path_relative: &str) -> String {
    let mut out = String::new();
    match old {
        None => {
            out.push_str("--- /dev/null\n");
            out.push_str(&format!("+++ b/{path_relative}\n"));
        }
        Some(_) => {
            out.push_str(&format!("--- a/{path_relative}\n"));
            out.push_str(&format!("+++ b/{path_relative}\n"));
        }
    }
    let old_lines: Vec<&str> = old.map(|t| t.lines().collect()).unwrap_or_default();
    let new_lines: Vec<&str> = new.lines().collect();
    let old_ends_nl = old.map(|t| t.ends_with('\n')).unwrap_or(true);
    let new_ends_nl = new.ends_with('\n');
    let ops = diff_ops(&old_lines, &new_lines);
    if ops.iter().all(|op| matches!(op, Op::Equal(..))) {
        return out;
    }
    // Group into hunks: a change region plus up to CONTEXT
    // equal lines on each side; regions separated by at most
    // 2*CONTEXT equal lines merge into one hunk.
    let mut hunks: Vec<(usize, usize)> = Vec::new();
    let mut idx = 0;
    while idx < ops.len() {
        if matches!(ops[idx], Op::Equal(..)) {
            idx += 1;
            continue;
        }
        let prev_end = hunks.last().map(|h| h.1).unwrap_or(0);
        let start = idx.saturating_sub(CONTEXT).max(prev_end);
        // Extend past the change; merge a later change when
        // the equal gap to it is at most 2*CONTEXT lines.
        let mut last_change = idx;
        let mut j = idx + 1;
        while j < ops.len() {
            match ops[j] {
                Op::Equal(..) => {
                    if j - last_change > 2 * CONTEXT {
                        break;
                    }
                    j += 1;
                }
                _ => {
                    last_change = j;
                    j += 1;
                }
            }
        }
        let end = (last_change + 1 + CONTEXT).min(ops.len());
        hunks.push((start, end));
        idx = end;
    }
    for (start, end) in hunks {
        let mut old_count = 0usize;
        let mut new_count = 0usize;
        for op in &ops[start..end] {
            match *op {
                Op::Equal(..) => {
                    old_count += 1;
                    new_count += 1;
                }
                Op::Delete(_) => old_count += 1,
                Op::Insert(_) => new_count += 1,
            }
        }
        // Lines before the hunk (0-based counts); a hunk with
        // no lines on a side starts at the line before it.
        let old_before = ops[..start]
            .iter()
            .filter(|op| matches!(op, Op::Equal(..) | Op::Delete(_)))
            .count();
        let new_before = ops[..start]
            .iter()
            .filter(|op| matches!(op, Op::Equal(..) | Op::Insert(_)))
            .count();
        let old_start = if old_count == 0 {
            old_before
        } else {
            old_before + 1
        };
        let new_start = if new_count == 0 {
            new_before
        } else {
            new_before + 1
        };
        out.push_str(&format!(
            "@@ -{} +{} @@\n",
            format_range(old_start, old_count),
            format_range(new_start, new_count),
        ));
        for op in &ops[start..end] {
            match *op {
                Op::Equal(i, _) => {
                    out.push(' ');
                    out.push_str(old_lines[i]);
                    out.push('\n');
                    if i + 1 == old_lines.len() && !old_ends_nl {
                        out.push_str("\\ No newline at end of file\n");
                    }
                }
                Op::Delete(i) => {
                    out.push('-');
                    out.push_str(old_lines[i]);
                    out.push('\n');
                    if i + 1 == old_lines.len() && !old_ends_nl {
                        out.push_str("\\ No newline at end of file\n");
                    }
                }
                Op::Insert(j) => {
                    out.push('+');
                    out.push_str(new_lines[j]);
                    out.push('\n');
                    if j + 1 == new_lines.len() && !new_ends_nl {
                        out.push_str("\\ No newline at end of file\n");
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_file_uses_dev_null_and_adds_all_lines() {
        let diff = unified_diff(None, "a\nb\n", "src/new.txt");
        assert!(diff.starts_with("--- /dev/null\n+++ b/src/new.txt\n"));
        assert!(diff.contains("@@ -0,0 +1,2 @@\n"));
        assert!(diff.contains("+a\n+b\n"));
    }

    #[test]
    fn changed_line_shows_minus_plus_with_context() {
        let diff = unified_diff(
            Some("one\ntwo\nthree\nfour\n"),
            "one\nTWO\nthree\nfour\n",
            "x.txt",
        );
        assert!(diff.starts_with("--- a/x.txt\n+++ b/x.txt\n"));
        assert!(diff.contains("-two\n"));
        assert!(diff.contains("+TWO\n"));
        assert!(diff.contains(" one\n"));
        assert!(diff.contains(" three\n"));
    }

    #[test]
    fn identical_content_has_headers_only() {
        let diff = unified_diff(Some("same\n"), "same\n", "x.txt");
        assert_eq!(diff, "--- a/x.txt\n+++ b/x.txt\n");
    }

    #[test]
    fn missing_trailing_newline_is_marked() {
        let diff = unified_diff(Some("a\n"), "a\nb", "x.txt");
        assert!(diff.contains("+b\n\\ No newline at end of file\n"));
    }

    #[test]
    fn far_apart_changes_become_two_hunks() {
        let old: Vec<String> = (0..20).map(|i| format!("line {i}")).collect();
        let mut new = old.clone();
        new[1] = "CHANGED-1".to_string();
        new[18] = "CHANGED-18".to_string();
        let diff = unified_diff(
            Some(&old.join("\n")),
            &new.join("\n"),
            "x.txt",
        );
        assert_eq!(diff.matches("@@").count() / 2, 2, "{diff}");
        assert!(diff.contains("-line 1\n"));
        assert!(diff.contains("+CHANGED-1\n"));
        assert!(diff.contains("-line 18\n"));
        assert!(diff.contains("+CHANGED-18\n"));
    }
}
