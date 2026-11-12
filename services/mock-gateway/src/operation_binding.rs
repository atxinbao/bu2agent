//! Server-computed operation binding for the unified file execution path.
//!
//! The public approval flow used to check only tool and status, so an
//! approved write could be replayed with different content, and the
//! same approval could execute any number of times. The unified path
//! closes both holes: the server computes a digest from the exact
//! execution parameters at approval time and again at execution time,
//! and the two digests must match before a single-use consumption.
//!
//! Both sides call the same digest functions below, so approval and
//! execution can never drift apart. A client-supplied hash is never
//! trusted: the server hashes the bytes it actually received.
//!
//! Path rules are the single normalization both approval families
//! share (rooted, parent, backslash and prefix paths are rejected;
//! normal components are rejoined with `/`). Phase 2 (Q5) merged
//! the loop engine's copy into this function: the loop module now
//! delegates here, keeping only its fs.list empty-path allowance.
//! Parity tests in both modules guard the shared rules.

use std::collections::BTreeMap;
use std::path::{Component, Path};

use sha2::{Digest, Sha256};

/// Normalize a client-supplied relative path with the loop engine's
/// rules. Returns `None` for anything that is not a plain relative
/// path inside the file root.
pub fn normalize_path(raw: &str) -> Option<String> {
    if raw.contains(['\0', '\\'])
        || Path::new(raw).has_root()
        || Path::new(raw)
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
    {
        return None;
    }
    let path = Path::new(raw)
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    if path.is_empty() || path.len() > 4096 {
        return None;
    }
    Some(path)
}

/// Lowercase hex SHA-256 of raw bytes.
pub fn sha256_hex(data: &[u8]) -> String {
    format!("{:x}", Sha256::digest(data))
}

/// D1-6a: ISO8601 / RFC3339 timestamps without a date
/// crate (the workspace has no chrono/time dependency,
/// and this batch must not add one for two small
/// conversions). The mail sync and the calendar backend
/// both need epoch milliseconds: Mail's display dates
/// and EventKit's ISO8601 strings.
///
/// `iso8601_to_ms` parses `YYYY-MM-DDTHH:MM:SS[.fff]`
/// with a `Z` or `±HH:MM` offset (a space may replace
/// the `T`; a missing offset means UTC). Anything else
/// returns 0 — callers treat 0 as "unknown date", never
/// as 1970-01-01 (AppleScript's locale display dates,
/// for example, do not parse and land at 0 by design).
///
/// `ms_to_iso8601_utc` formats epoch milliseconds as
/// UTC `YYYY-MM-DDTHH:MM:SS.mmmZ`. Both use Howard
/// Hinnant's civil-date algorithms (days-from-civil /
/// civil-from-days), which are exact for the proleptic
/// Gregorian calendar on either side of the epoch.
pub fn iso8601_to_ms(value: &str) -> i64 {
    let bytes = value.as_bytes();
    if bytes.len() < 19 {
        return 0;
    }
    let num = |from: usize, len: usize| -> Option<i64> {
        value.get(from..from + len)?.parse::<i64>().ok()
    };
    if bytes[4] != b'-' || bytes[7] != b'-' || (bytes[10] != b'T' && bytes[10] != b' ') || bytes[13] != b':' || bytes[16] != b':' {
        return 0;
    }
    let (year, month, day, hour, minute, second) = match (
        num(0, 4),
        num(5, 2),
        num(8, 2),
        num(11, 2),
        num(14, 2),
        num(17, 2),
    ) {
        (Some(y), Some(mo), Some(d), Some(h), Some(mi), Some(s)) => (y, mo, d, h, mi, s),
        _ => return 0,
    };
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return 0;
    }
    // Fractional seconds and the zone designator follow
    // the seconds; the zone is `Z` or `±HH:MM`.
    let mut idx = 19;
    let mut millis: i64 = 0;
    if idx < bytes.len() && bytes[idx] == b'.' {
        let start = idx + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        if end == start {
            return 0;
        }
        let digits = &value[start..end];
        let scale = 10i64.pow((3usize.saturating_sub(digits.len())) as u32);
        let frac: i64 = digits.parse().ok().unwrap_or(0);
        millis = if digits.len() >= 3 {
            frac / 10i64.pow((digits.len() - 3) as u32)
        } else {
            frac * scale
        };
        idx = end;
    }
    let mut offset_ms: i64 = 0;
    if idx < bytes.len() {
        match bytes[idx] {
            b'Z' | b'z' => idx += 1,
            b'+' | b'-' => {
                let sign = if bytes[idx] == b'-' { -1 } else { 1 };
                if bytes.len() < idx + 6 || bytes[idx + 3] != b':' {
                    return 0;
                }
                let (oh, om) = match (num(idx + 1, 2), num(idx + 4, 2)) {
                    (Some(h), Some(m)) => (h, m),
                    _ => return 0,
                };
                if oh > 23 || om > 59 {
                    return 0;
                }
                offset_ms = sign * (oh * 3_600_000 + om * 60_000);
                idx += 6;
            }
            _ => return 0,
        }
    }
    if idx != bytes.len() {
        return 0;
    }
    let days = days_from_civil(year, month as u32, day as u32);
    days * 86_400_000 + hour * 3_600_000 + minute * 60_000 + second * 1000 + millis - offset_ms
}

pub fn ms_to_iso8601_utc(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    let hour = rem / 3_600_000;
    let minute = (rem % 3_600_000) / 60_000;
    let second = (rem % 60_000) / 1000;
    let millis = rem % 1000;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Proleptic Gregorian date for days since 1970-01-01.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Q5 unified digest: every approval-bound operation commits to a
/// (tool, mode, params) triple, serialized as one JSON tuple and
/// hashed. `mode` is `Some` for operations whose mode changes the
/// effect (file overwrite vs append, reminder set_completed) and
/// `None` for operations with a single mode (delete, mkdir, rename,
/// export), whose historical shape is the bare (tool, params) pair.
/// All digest functions below delegate here, and the loop engine's
/// `args_digest` is the same function over loop-normalized args, so
/// the two approval families can never drift apart again. A digest
/// is only meaningful next to the tool it was computed for: callers
/// always check tool + digest together.
pub fn operation_digest(
    tool: &str,
    mode: Option<&str>,
    params: &BTreeMap<String, String>,
) -> String {
    let payload = match mode {
        Some(mode) => serde_json::to_string(&(tool, mode, params)).expect("string map"),
        None => serde_json::to_string(&(tool, params)).expect("string map"),
    };
    sha256_hex(payload.as_bytes())
}

/// Digest for a Worker command (P3-C §7): the resolved argv,
/// the working directory and the effective timeout, serialized
/// as one JSON tuple and hashed. The loop planning branch and
/// the run route both compute this from the resolved command,
/// never from a client hash — so an approval for one command
/// can never authorize a different argv, cwd or timeout. The
/// commit message is committed by the argv hash (argv contains
/// `-m <message>`); it appears in plaintext nowhere in the
/// approval record, only as `message_sha256` in the commit
/// Effect's digest inputs (§4.4).
pub fn command_digest(argv: &[String], cwd: &str, timeout_ms: u64) -> String {
    let payload = serde_json::to_string(&(argv, cwd, timeout_ms)).expect("command digest");
    sha256_hex(payload.as_bytes())
}

/// Digest for a file write, computed from the normalized path, the
/// SHA-256 of the decoded content bytes, and the write mode. The
/// mode is part of the digest so an overwrite approval can never be
/// replayed as an append (or the reverse). Returns `None` when the
/// path fails normalization.
pub fn write_digest(path_raw: &str, content: &[u8], append: bool) -> Option<String> {
    let path = normalize_path(path_raw)?;
    let mode = if append { "append" } else { "overwrite" };
    let params = BTreeMap::from([
        ("content_sha256".to_string(), sha256_hex(content)),
        ("path".to_string(), path),
    ]);
    Some(operation_digest("fs.write", Some(mode), &params))
}

/// Digest for a file delete, computed from the normalized path.
/// Returns `None` when the path fails normalization.
pub fn delete_digest(path_raw: &str) -> Option<String> {
    let path = normalize_path(path_raw)?;
    let params = BTreeMap::from([("path".to_string(), path)]);
    Some(operation_digest("fs.delete", None, &params))
}

/// Digest for an idempotent directory creation bound to one path.
pub fn mkdir_digest(path_raw: &str) -> Option<String> {
    let path = normalize_path(path_raw)?;
    let params = BTreeMap::from([("path".to_string(), path)]);
    Some(operation_digest("fs.mkdir", None, &params))
}

/// Digest for moving an entry between two normalized paths.
pub fn rename_digest(from_raw: &str, to_raw: &str) -> Option<String> {
    let from = normalize_path(from_raw)?;
    let to = normalize_path(to_raw)?;
    let params = BTreeMap::from([("from".to_string(), from), ("to".to_string(), to)]);
    Some(operation_digest("fs.rename", None, &params))
}

/// Digest for exporting a root file to one explicit absolute destination.
pub fn export_digest(from_raw: &str, to_raw: &str) -> Option<String> {
    let from = normalize_path(from_raw)?;
    let to = Path::new(to_raw);
    if !to.is_absolute() || to.to_string_lossy().len() > 4096 {
        return None;
    }
    let params = BTreeMap::from([
        ("from".to_string(), from),
        ("to".to_string(), to.to_string_lossy().into_owned()),
    ]);
    Some(operation_digest("fs.export", None, &params))
}

/// Digest for a real mail send (D1-6b-1, Q-D1-6b-22).
/// LOCAL DEFINITION: the digest commits to the sorted,
/// deduplicated recipients (as one hash), the subject
/// hash, and the body hash — raw text lives only in
/// request memory, never in the digest inputs. Both the
/// approval-request path and the send route call this
/// same function over the same normalized inputs, so the
/// two digests can never drift apart. `body_hash` is the
/// caller's sha256 of the body (the body itself is never
/// hashed twice or stored).
pub fn mail_send_digest(to_sorted: &[String], subject: &str, body_hash: &str) -> String {
    let params = BTreeMap::from([
        ("body_hash".to_string(), body_hash.to_string()),
        (
            "recipients_hash".to_string(),
            sha256_hex(to_sorted.join(",").as_bytes()),
        ),
        ("subject_hash".to_string(), sha256_hex(subject.as_bytes())),
    ]);
    operation_digest("mail.send", None, &params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_rejects_unsafe_paths() {
        assert_eq!(normalize_path(""), None);
        assert_eq!(normalize_path("/absolute"), None);
        assert_eq!(normalize_path("../escape"), None);
        assert_eq!(normalize_path("a/../../b"), None);
        assert_eq!(normalize_path("a\\b"), None);
        assert_eq!(normalize_path("a\0b"), None);
        assert_eq!(normalize_path(&"a".repeat(4097)), None);
    }

    #[test]
    fn normalize_rejoins_normal_components() {
        assert_eq!(normalize_path("a/./b"), Some("a/b".to_string()));
        assert_eq!(normalize_path("a//b"), Some("a/b".to_string()));
        assert_eq!(normalize_path("a/b/"), Some("a/b".to_string()));
        assert_eq!(normalize_path("report.md"), Some("report.md".to_string()));
    }

    #[test]
    fn digests_are_stable_but_sensitive_to_every_parameter() {
        let base = write_digest("a.txt", b"hello", false).expect("digest");
        assert_eq!(write_digest("a.txt", b"hello", false).expect("digest"), base);
        assert_ne!(write_digest("b.txt", b"hello", false).expect("digest"), base);
        assert_ne!(write_digest("a.txt", b"helloo", false).expect("digest"), base);
        assert_ne!(write_digest("a.txt", b"hello", true).expect("digest"), base);
        assert_ne!(delete_digest("a.txt").expect("digest"), base);
        assert_eq!(delete_digest("a/./b.txt").expect("digest"), delete_digest("a/b.txt").expect("digest"));
    }

    #[test]
    fn digests_reject_bad_paths() {
        assert_eq!(write_digest("../x", b"hi", false), None);
        assert_eq!(delete_digest("/x"), None);
        assert_eq!(mkdir_digest("../x"), None);
        assert_eq!(rename_digest("a", "/x"), None);
    }

    #[test]
    fn mail_send_digest_is_stable_and_sensitive() {
        // D1-6b-1: inputs are hashes (recipients joined
        // then hashed, subject hashed, body pre-hashed);
        // raw text never enters the digest inputs.
        let body_hash = sha256_hex(b"hello body");
        let to = vec![
            "a@example.invalid".to_string(),
            "b@example.invalid".to_string(),
        ];
        let base = mail_send_digest(&to, "Hello", &body_hash);
        assert_eq!(mail_send_digest(&to, "Hello", &body_hash), base);
        // Recipient order does not matter: callers pass
        // the sorted list, so a permutation digests the
        // same — and the unsorted callers' job is to sort
        // first, not the digest's.
        assert_ne!(
            mail_send_digest(
                &[
                    "b@example.invalid".to_string(),
                    "a@example.invalid".to_string(),
                ],
                "Hello",
                &body_hash
            ),
            base
        );
        assert_ne!(
            mail_send_digest(&to, "Other", &body_hash),
            base,
            "subject changes the digest"
        );
        assert_ne!(
            mail_send_digest(&to, "Hello", &sha256_hex(b"other body")),
            base,
            "body hash changes the digest"
        );
        assert_ne!(
            mail_send_digest(&["c@example.invalid".to_string()], "Hello", &body_hash),
            base,
            "recipients change the digest"
        );
        assert_ne!(
            mail_send_digest(&to, "Hello", &body_hash),
            write_digest("a.txt", b"hello", false).expect("digest")
        );
    }

    #[test]
    fn iso8601_round_trips_and_offsets() {
        assert_eq!(ms_to_iso8601_utc(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601_to_ms("1970-01-01T00:00:00.000Z"), 0);
        assert_eq!(iso8601_to_ms("1970-01-01T00:00:00Z"), 0);
        // 2026-11-01T10:00:00Z is the Mock mail seed date.
        let ms = iso8601_to_ms("2026-11-01T10:00:00Z");
        assert_eq!(ms_to_iso8601_utc(ms), "2026-11-01T10:00:00.000Z");
        assert_eq!(iso8601_to_ms("2026-11-01T10:00:00.500Z"), ms + 500);
        assert_eq!(iso8601_to_ms("2026-11-01T18:00:00+08:00"), ms);
        assert_eq!(iso8601_to_ms("2026-11-01T02:00:00-08:00"), ms);
        // Leap day round-trips; pre-epoch dates work too.
        assert_eq!(
            ms_to_iso8601_utc(iso8601_to_ms("2024-02-29T12:00:00Z")),
            "2024-02-29T12:00:00.000Z"
        );
        assert_eq!(
            ms_to_iso8601_utc(iso8601_to_ms("1969-12-31T23:59:59.999Z")),
            "1969-12-31T23:59:59.999Z"
        );
    }

    #[test]
    fn iso8601_unparseable_is_zero() {
        assert_eq!(iso8601_to_ms(""), 0);
        assert_eq!(iso8601_to_ms("not a date"), 0);
        // AppleScript locale display dates do not parse.
        assert_eq!(iso8601_to_ms("Saturday, November 1, 2026 at 10:00:00 AM"), 0);
        assert_eq!(iso8601_to_ms("2026-13-01T10:00:00Z"), 0);
        assert_eq!(iso8601_to_ms("2026-11-01"), 0);
        // A missing zone designator means UTC (local definition).
        assert_eq!(
            iso8601_to_ms("2026-11-01T10:00:00"),
            iso8601_to_ms("2026-11-01T10:00:00Z")
        );
    }

    #[test]
    fn rename_and_mkdir_digests_are_stable_and_sensitive() {
        let rename = rename_digest("a/./from.txt", "b/to.txt").expect("digest");
        assert_eq!(rename_digest("a/from.txt", "b/to.txt").expect("digest"), rename);
        assert_ne!(rename_digest("a/other.txt", "b/to.txt").expect("digest"), rename);
        assert_ne!(rename_digest("a/from.txt", "b/other.txt").expect("digest"), rename);
        let mkdir = mkdir_digest("a/./new").expect("digest");
        assert_eq!(mkdir_digest("a/new").expect("digest"), mkdir);
        assert_ne!(mkdir_digest("a/other").expect("digest"), mkdir);
        assert_ne!(mkdir, delete_digest("a/new").expect("digest"));
        let out = if cfg!(windows) { "C:\\tmp\\out.txt" } else { "/tmp/out.txt" };
        let other = if cfg!(windows) { "C:\\tmp\\other.txt" } else { "/tmp/other.txt" };
        let export = export_digest("a/file.txt", out).expect("digest");
        assert_eq!(export_digest("a/./file.txt", out), Some(export.clone()));
        assert_ne!(export_digest("b/file.txt", out).unwrap(), export);
        assert_ne!(export_digest("a/file.txt", other).unwrap(), export);
        assert_eq!(export_digest("a/file.txt", "relative.txt"), None);
    }
}
