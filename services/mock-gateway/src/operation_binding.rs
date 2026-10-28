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
//! Path rules mirror the loop engine's normalization (rooted, parent,
//! backslash and prefix paths are rejected; normal components are
//! rejoined with `/`). The loop module is deliberately untouched;
//! parity tests here guard the shared rules.

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
    Some(sha256_hex(
        serde_json::to_string(&("fs.write", mode, params))
            .expect("string map")
            .as_bytes(),
    ))
}

/// Digest for a file delete, computed from the normalized path.
/// Returns `None` when the path fails normalization.
pub fn delete_digest(path_raw: &str) -> Option<String> {
    let path = normalize_path(path_raw)?;
    let params = BTreeMap::from([("path".to_string(), path)]);
    Some(sha256_hex(
        serde_json::to_string(&("fs.delete", params))
            .expect("string map")
            .as_bytes(),
    ))
}

/// Digest for an idempotent directory creation bound to one path.
pub fn mkdir_digest(path_raw: &str) -> Option<String> {
    let path = normalize_path(path_raw)?;
    let params = BTreeMap::from([("path".to_string(), path)]);
    Some(sha256_hex(
        serde_json::to_string(&("fs.mkdir", params))
            .expect("string map")
            .as_bytes(),
    ))
}

/// Digest for moving an entry between two normalized paths.
pub fn rename_digest(from_raw: &str, to_raw: &str) -> Option<String> {
    let from = normalize_path(from_raw)?;
    let to = normalize_path(to_raw)?;
    let params = BTreeMap::from([("from".to_string(), from), ("to".to_string(), to)]);
    Some(sha256_hex(
        serde_json::to_string(&("fs.rename", params))
            .expect("string map")
            .as_bytes(),
    ))
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
    fn rename_and_mkdir_digests_are_stable_and_sensitive() {
        let rename = rename_digest("a/./from.txt", "b/to.txt").expect("digest");
        assert_eq!(rename_digest("a/from.txt", "b/to.txt").expect("digest"), rename);
        assert_ne!(rename_digest("a/other.txt", "b/to.txt").expect("digest"), rename);
        assert_ne!(rename_digest("a/from.txt", "b/other.txt").expect("digest"), rename);
        let mkdir = mkdir_digest("a/./new").expect("digest");
        assert_eq!(mkdir_digest("a/new").expect("digest"), mkdir);
        assert_ne!(mkdir_digest("a/other").expect("digest"), mkdir);
        assert_ne!(mkdir, delete_digest("a/new").expect("digest"));
    }
}
