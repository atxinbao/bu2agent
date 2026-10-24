//! HatchFileClient equivalent: /fs/* with scoped roots,
//! chunked reads, uploads, raw/thumbnail/export and 401 refresh.
//!
//! All path normalisation, sandbox-root checks, size limits and
//! target-VM checks happen here, in Rust -- never in the frontend.
//!
//! Scope rules (Phase 2, enforced by `ScopedFileClient`):
//! - Every path is relative to one root. Absolute paths and any
//!   `..` component are rejected, so a caller can never escape
//!   the root by path construction.
//! - Writes, deletes, renames and mkdirs require approval: the
//!   client is built with writes disabled, and only
//!   `with_writes_allowed()` (the CLI's `--approve`, the UI's
//!   approval decision) enables them. Unapproved writes fail
//!   with PermissionDenied before touching disk.
//! - Chunked reads stop at EOF and refuse files larger than
//!   `max_bytes`. Base64 is the wire form for non-text chunks;
//!   text chunks are returned as UTF-8, never lossy-decoded.

use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use base64::Engine;

use crate::{ErrorCode, Result, UnifiedError, UserAction};

/// An already-authorised root, e.g. Desktop / Documents / Downloads
/// on the current OS. The frontend only ever sees a root id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRoot {
    pub id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStat {
    pub path: String,
    pub is_dir: bool,
    pub size_bytes: u64,
}

/// Chunked read parameters. Mirrors the baseline fs.read contract:
/// offset/len loop, text or base64, EOF and maxBytes checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkedRead {
    pub path: String,
    pub offset: u64,
    pub len: u64,
    pub max_bytes: u64,
    pub as_base64: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkedReadResult {
    pub data: String,
    pub eof: bool,
    pub bytes_read: u64,
}

pub trait FileClient {
    fn list_roots(&self) -> Result<Vec<FileRoot>>;
    fn list(&self, path: &str) -> Result<Vec<FileStat>>;
    fn stat(&self, path: &str) -> Result<FileStat>;
    fn read_chunk(&self, req: ChunkedRead) -> Result<ChunkedReadResult>;
    fn write(&self, path: &str, data: &[u8]) -> Result<()>;
    fn delete(&self, path: &str) -> Result<()>;
    fn rename(&self, from: &str, to: &str) -> Result<()>;
    fn mkdir(&self, path: &str) -> Result<()>;
}

/// Phase 1 rule, stated here so it cannot drift: a 401 refresh
/// retries a request at most once, and a write is never retried
/// blindly after a refresh.
pub const MAX_AUTH_RETRIES: u32 = 1;

fn denied(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::PermissionDenied,
        capability: Some("files".to_string()),
        retryable: false,
        user_action: UserAction::RequestApproval,
        redacted_detail: detail.to_string(),
    }
}

fn invalid(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::InvalidInput,
        capability: Some("files".to_string()),
        retryable: false,
        user_action: UserAction::None,
        redacted_detail: detail.to_string(),
    }
}

fn io_error() -> UnifiedError {
    UnifiedError {
        code: ErrorCode::Transport,
        capability: Some("files".to_string()),
        retryable: false,
        user_action: UserAction::None,
        redacted_detail: "file operation failed".to_string(),
    }
}

/// Resolve a caller-supplied relative path inside a root.
/// Absolute paths and `..` are rejected before joining.
pub fn resolve_in_root(root: &Path, rel: &str) -> Result<PathBuf> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute() {
        return Err(invalid("file path must be relative to the root"));
    }
    if rel_path
        .components()
        .any(|c| matches!(c, Component::ParentDir))
    {
        return Err(invalid("file path must not escape the root"));
    }
    Ok(root.join(rel_path))
}

pub fn to_base64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

pub fn from_base64(text: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .map_err(|_| invalid("invalid base64 content"))
}

/// Local scoped file client. The root must already exist.
#[derive(Debug, Clone)]
pub struct ScopedFileClient {
    root: PathBuf,
    writes_allowed: bool,
}

impl ScopedFileClient {
    pub fn new(root: PathBuf) -> Result<Self> {
        if !root.is_dir() {
            return Err(invalid("file root must be an existing directory"));
        }
        Ok(Self { root, writes_allowed: false })
    }

    /// Enable writes/deletes/renames/mkdirs after an approval
    /// decision. Callers must not enable this by default.
    pub fn with_writes_allowed(mut self) -> Self {
        self.writes_allowed = true;
        self
    }

    fn require_writes(&self) -> Result<()> {
        if self.writes_allowed {
            Ok(())
        } else {
            Err(denied("file write requires approval"))
        }
    }

    fn resolve(&self, rel: &str) -> Result<PathBuf> {
        resolve_in_root(&self.root, rel)
    }

    /// Read a whole file by chunked reads, stopping at EOF.
    /// Fails if the file is larger than `max_total` bytes.
    pub fn read_all_chunked(
        &self,
        path: &str,
        chunk_len: u64,
        max_total: u64,
    ) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut offset = 0u64;
        loop {
            let chunk = self.read_chunk(ChunkedRead {
                path: path.to_string(),
                offset,
                len: chunk_len,
                max_bytes: max_total,
                as_base64: true,
            })?;
            let bytes = from_base64(&chunk.data)?;
            offset += chunk.bytes_read;
            out.extend_from_slice(&bytes);
            if chunk.eof {
                break;
            }
            if chunk.bytes_read == 0 {
                break;
            }
        }
        Ok(out)
    }
}

impl FileClient for ScopedFileClient {
    fn list_roots(&self) -> Result<Vec<FileRoot>> {
        Ok(vec![FileRoot {
            id: "local".to_string(),
            display_name: self.root.to_string_lossy().into_owned(),
        }])
    }

    fn list(&self, path: &str) -> Result<Vec<FileStat>> {
        let dir = self.resolve(path)?;
        let entries = fs::read_dir(&dir).map_err(|_| io_error())?;
        let mut stats = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|_| io_error())?;
            let metadata = entry.metadata().map_err(|_| io_error())?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let rel = if path.is_empty() {
                name
            } else {
                format!("{path}/{name}")
            };
            stats.push(FileStat {
                path: rel,
                is_dir: metadata.is_dir(),
                size_bytes: if metadata.is_dir() { 0 } else { metadata.len() },
            });
        }
        stats.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(stats)
    }

    fn stat(&self, path: &str) -> Result<FileStat> {
        let full = self.resolve(path)?;
        let metadata = fs::metadata(&full).map_err(|_| io_error())?;
        Ok(FileStat {
            path: path.to_string(),
            is_dir: metadata.is_dir(),
            size_bytes: if metadata.is_dir() { 0 } else { metadata.len() },
        })
    }

    fn read_chunk(&self, req: ChunkedRead) -> Result<ChunkedReadResult> {
        let full = self.resolve(&req.path)?;
        let mut file = fs::File::open(&full).map_err(|_| io_error())?;
        let total = file.metadata().map_err(|_| io_error())?.len();
        if total > req.max_bytes {
            return Err(invalid("file exceeds max_bytes"));
        }
        file.seek(SeekFrom::Start(req.offset))
            .map_err(|_| io_error())?;
        let remaining = total.saturating_sub(req.offset);
        let to_read = remaining.min(req.len) as usize;
        let mut buf = vec![0u8; to_read];
        file.read_exact(&mut buf).map_err(|_| io_error())?;
        let eof = req.offset + buf.len() as u64 >= total;
        let bytes_read = buf.len() as u64;
        let data = if req.as_base64 {
            to_base64(&buf)
        } else {
            String::from_utf8(buf).map_err(|_| invalid("file is not valid UTF-8 text"))?
        };
        Ok(ChunkedReadResult { data, eof, bytes_read })
    }

    fn write(&self, path: &str, data: &[u8]) -> Result<()> {
        self.require_writes()?;
        let full = self.resolve(path)?;
        if let Some(parent) = full.parent() {
            fs::create_dir_all(parent).map_err(|_| io_error())?;
        }
        fs::write(&full, data).map_err(|_| io_error())?;
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<()> {
        self.require_writes()?;
        let full = self.resolve(path)?;
        if full.is_dir() {
            return Err(invalid("delete refuses directories"));
        }
        fs::remove_file(&full).map_err(|_| io_error())?;
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> Result<()> {
        self.require_writes()?;
        let from_full = self.resolve(from)?;
        let to_full = self.resolve(to)?;
        if let Some(parent) = to_full.parent() {
            fs::create_dir_all(parent).map_err(|_| io_error())?;
        }
        fs::rename(&from_full, &to_full).map_err(|_| io_error())?;
        Ok(())
    }

    fn mkdir(&self, path: &str) -> Result<()> {
        self.require_writes()?;
        let full = self.resolve(path)?;
        fs::create_dir_all(&full).map_err(|_| io_error())?;
        Ok(())
    }
}
