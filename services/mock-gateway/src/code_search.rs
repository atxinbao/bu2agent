//! code.search: read-only content search over one canonical root.
//!
//! P3-A loop tool. Case-sensitive substring match per line
//! (grep default), worktree-relative paths in the results,
//! 1-based line numbers, hit line ±1 line of context,
//! truncated to 4096 bytes per result. Zero Effect: results
//! go only to the model context, never to events, persisted
//! loop text, or logs — the caller must not log the query or
//! the hits either.
//!
//! Skips, by design:
//! - binary files (first 8 KiB contains NUL),
//! - secret filenames (`*.pem`, `*.key`, `.env*`,
//!   `*credentials*`, `*secret*`, matched on the file name,
//!   case-insensitive),
//! - symlinked directories (never entered); symlinked files
//!   are canonical-checked one by one and skipped on escape.
//! - A single file is scanned only up to 1 MiB (streaming scan
//!   stops there, outcome is `truncated`). At most 50 results
//!   are returned; more matches set `truncated`.

use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Response cap: at most this many hits are returned.
pub const CODE_SEARCH_MAX_RESULTS: usize = 50;
/// Per-result cap: hit line ±1 line, truncated to this many bytes.
pub const CODE_SEARCH_RESULT_BYTES: usize = 4096;
/// Per-file scan cap: streaming stops here, outcome is truncated.
pub const CODE_SEARCH_FILE_BYTES: u64 = 1024 * 1024;
/// Binary probe: a NUL in the first bytes of this size means binary.
pub const CODE_SEARCH_BINARY_PROBE_BYTES: usize = 8192;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodeSearchHit {
    /// Worktree-relative path with `/` separators.
    pub path: String,
    /// 1-based line number of the hit.
    pub line: u64,
    /// Hit line ±1 line, joined with `\n`, truncated to
    /// [`CODE_SEARCH_RESULT_BYTES`] bytes.
    pub context: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodeSearchOutcome {
    pub results: Vec<CodeSearchHit>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeSearchError {
    /// The root does not exist or is not a directory.
    NotADirectory,
    /// The `path` prefix escapes the root (`..`, absolute,
    /// backslash, NUL, or a symlinked prefix).
    PathEscapesRoot,
}

impl std::fmt::Display for CodeSearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodeSearchError::NotADirectory => write!(f, "search root is not a directory"),
            CodeSearchError::PathEscapesRoot => write!(f, "search path escapes the root"),
        }
    }
}

/// A secret filename is skipped: `*.pem`, `*.key`, `.env*`,
/// `*credentials*`, `*secret*`, matched on the file name.
pub fn is_secret_filename(file_name: &str) -> bool {
    let lower = file_name.to_lowercase();
    lower.ends_with(".pem")
        || lower.ends_with(".key")
        || lower.starts_with(".env")
        || lower.contains("credentials")
        || lower.contains("secret")
}

/// Validate a worktree-relative search prefix. `""` means the
/// root itself. Returns the normalized `/`-joined prefix, or
/// `None` for absolute paths, `..`, backslashes, NUL, or an
/// over-long path.
pub fn normalize_search_path(raw: &str) -> Option<String> {
    if raw.is_empty() {
        return Some(String::new());
    }
    if raw.contains(['\0', '\\']) || Path::new(raw).has_root() {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    for component in Path::new(raw).components() {
        match component {
            Component::Normal(s) => parts.push(s.to_string_lossy().into_owned()),
            Component::CurDir => {}
            _ => return None,
        }
    }
    let joined = parts.join("/");
    if joined.len() > 4096 {
        return None;
    }
    Some(joined)
}

fn truncate_bytes(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// Search `root` (already canonical) under the optional
/// `subpath` prefix for case-sensitive substring `query`.
/// The scan is streaming, one line at a time, bounded per
/// file; entries are visited in sorted name order so results
/// are deterministic across platforms.
pub fn search_code(
    root: &Path,
    query: &str,
    subpath: &str,
    max_results: usize,
) -> Result<CodeSearchOutcome, CodeSearchError> {
    // An empty query matches every line; the contract rejects
    // it (normalize + the loop arm both 400). The pure search
    // stays safe for direct callers: zero results, no scan.
    if query.is_empty() {
        return Ok(CodeSearchOutcome {
            results: Vec::new(),
            truncated: false,
        });
    }
    let canonical_root = root
        .canonicalize()
        .map_err(|_| CodeSearchError::NotADirectory)?;
    if !canonical_root.is_dir() {
        return Err(CodeSearchError::NotADirectory);
    }
    let prefix = normalize_search_path(subpath).ok_or(CodeSearchError::PathEscapesRoot)?;
    let start = if prefix.is_empty() {
        canonical_root.clone()
    } else {
        canonical_root.join(prefix.replace('/', std::path::MAIN_SEPARATOR_STR))
    };
    let start = start
        .canonicalize()
        .map_err(|_| CodeSearchError::PathEscapesRoot)?;
    if start != canonical_root && !start.starts_with(&canonical_root) {
        return Err(CodeSearchError::PathEscapesRoot);
    }
    if !start.is_dir() {
        return Err(CodeSearchError::NotADirectory);
    }
    let mut state = SearchState {
        query,
        max_results,
        results: Vec::new(),
        total_matches: 0,
        truncated: false,
    };
    walk(&start, &canonical_root, &mut state);
    let truncated = state.truncated || state.total_matches > state.results.len();
    Ok(CodeSearchOutcome {
        results: state.results,
        truncated,
    })
}

struct SearchState<'a> {
    query: &'a str,
    max_results: usize,
    results: Vec<CodeSearchHit>,
    total_matches: usize,
    truncated: bool,
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .map(|rel| {
            rel.components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/")
        })
        .unwrap_or_default()
}

fn walk(dir: &Path, root: &Path, state: &mut SearchState<'_>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.filter_map(|entry| entry.ok()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_symlink() {
            // Symlinked directories are never entered; a
            // symlinked file is canonical-checked and skipped
            // on escape. A surviving link is read through its
            // canonical target but reported under its own name.
            let Ok(canonical) = path.canonicalize() else {
                continue;
            };
            if canonical.is_dir() {
                continue;
            }
            if !canonical.starts_with(root) {
                continue;
            }
            scan_file(&canonical, &relative_path(root, &path), state);
        } else if file_type.is_dir() {
            walk(&path, root, state);
        } else if file_type.is_file() {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if is_secret_filename(&file_name) {
                continue;
            }
            scan_file(&path, &relative_path(root, &path), state);
        }
    }
}

/// Stream one file line by line. Binary files are skipped on
/// the first 8 KiB containing NUL; the scan stops at the 1 MiB
/// cap and marks the outcome truncated.
fn scan_file(path: &Path, relative: &str, state: &mut SearchState<'_>) {
    let Ok(file) = std::fs::File::open(path) else {
        return;
    };
    let mut reader = BufReader::new(file);
    // Binary probe: a NUL in the first buffered bytes means
    // binary. `fill_buf` only peeks; the line scan below reads
    // from the start.
    match reader.fill_buf() {
        Ok(probe) => {
            let probe_len = probe.len().min(CODE_SEARCH_BINARY_PROBE_BYTES);
            if probe[..probe_len].contains(&0) {
                return;
            }
        }
        Err(_) => return,
    }
    let mut lines: Vec<String> = Vec::new();
    let mut bytes_read: u64 = 0;
    let mut over_cap = false;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(n) => {
                bytes_read += n as u64;
                if bytes_read > CODE_SEARCH_FILE_BYTES {
                    over_cap = true;
                    break;
                }
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }
                lines.push(line);
            }
            Err(_) => return,
        }
    }
    if over_cap {
        state.truncated = true;
    }
    for (index, line) in lines.iter().enumerate() {
        if line.contains(state.query) {
            state.total_matches += 1;
            if state.results.len() >= state.max_results {
                continue;
            }
            let mut context_lines: Vec<&str> = Vec::new();
            if index > 0 {
                context_lines.push(lines[index - 1].as_str());
            }
            context_lines.push(line.as_str());
            if index + 1 < lines.len() {
                context_lines.push(lines[index + 1].as_str());
            }
            let context = truncate_bytes(&context_lines.join("\n"), CODE_SEARCH_RESULT_BYTES);
            state.results.push(CodeSearchHit {
                path: relative.to_string(),
                line: (index as u64) + 1,
                context,
            });
        }
    }
}

/// Resolve a worktree-relative path for display: the
/// canonical root of the worktree directory.
pub fn worktree_canonical_dir(location_root: &Path, directory_relative: &str) -> Option<PathBuf> {
    let joined = if directory_relative.is_empty() {
        location_root.to_path_buf()
    } else {
        location_root.join(directory_relative.replace('/', std::path::MAIN_SEPARATOR_STR))
    };
    let canonical = joined.canonicalize().ok()?;
    if canonical.strip_prefix(location_root).is_err() {
        return None;
    }
    if !canonical.is_dir() {
        return None;
    }
    Some(canonical)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create test parent");
        }
        std::fs::write(&path, content).expect("write test file");
        path
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir()
            .join(format!("bu2agent-code-search-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn normalize_search_path_rules() {
        assert_eq!(normalize_search_path("").as_deref(), Some(""));
        assert_eq!(normalize_search_path("src").as_deref(), Some("src"));
        assert_eq!(normalize_search_path("./src/").as_deref(), Some("src"));
        assert_eq!(normalize_search_path("../x"), None);
        assert_eq!(normalize_search_path("/abs"), None);
        assert_eq!(normalize_search_path("a\\b"), None);
    }

    #[test]
    fn secret_filename_rules() {
        assert!(is_secret_filename("id_rsa.pem"));
        assert!(is_secret_filename("key.key"));
        assert!(is_secret_filename(".env"));
        assert!(is_secret_filename(".env.local"));
        assert!(is_secret_filename("my-credentials.json"));
        assert!(is_secret_filename("client_secret.json"));
        assert!(!is_secret_filename("notes.md"));
        assert!(!is_secret_filename("pem.txt"));
    }

    #[test]
    fn search_finds_hit_with_context_and_skips_binary_and_secrets() {
        let dir = temp_dir("basic");
        write(&dir, "a.txt", b"one\nneedle here\nthree\n");
        write(&dir, "sub/b.txt", b"nothing\n");
        write(&dir, "bin.dat", b"\x00needle\x00");
        write(&dir, "id.pem", b"needle\n");
        let out = search_code(&dir, "needle", "", 50).expect("search");
        assert!(!out.truncated);
        assert_eq!(out.results.len(), 1);
        assert_eq!(out.results[0].path, "a.txt");
        assert_eq!(out.results[0].line, 2);
        assert_eq!(out.results[0].context, "one\nneedle here\nthree");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn search_path_prefix_escapes() {
        let dir = temp_dir("escape");
        write(&dir, "a.txt", b"needle\n");
        assert_eq!(
            search_code(&dir, "needle", "..", 50),
            Err(CodeSearchError::PathEscapesRoot)
        );
        assert_eq!(
            search_code(&dir, "needle", "/etc", 50),
            Err(CodeSearchError::PathEscapesRoot)
        );
        let out = search_code(&dir, "needle", "", 50).expect("search");
        assert_eq!(out.results.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
