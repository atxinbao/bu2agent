//! File gateway client: Rust shell -> Mock Gateway file endpoints.
//!
//! Phase 2 file mainline over HTTP: list, chunked read, write,
//! delete, all inside the server's scoped root (see server.rs).
//! Paths are relative; absolute paths and `..` are rejected
//! client-side too, so a bad path never even reaches the server.
//!
//! 401 rule, implemented exactly as file_client.rs states it:
//! - Reads (list / read) may retry at most once after a 401,
//!   after calling the caller's refresh callback once.
//! - Writes and deletes are NEVER retried after a 401: a retry
//!   could duplicate a write. They return the 401 error at once.
//!
//! The Mock Gateway itself never returns 401, so the retry path
//! is covered by a fake-server test, not by the live Mock --
//! that limitation is stated, not hidden.

use serde::{Deserialize, Serialize};

use crate::file_client::{from_base64, to_base64, MAX_AUTH_RETRIES};
use crate::http_client::{gateway_error, transport_error, HttpClient};
use crate::{ErrorCode, Result, UnifiedError, UserAction};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayFileEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChunk {
    pub data: Vec<u8>,
    pub eof: bool,
    pub total_size: u64,
    pub bytes_read: u64,
}

#[derive(Debug, Deserialize)]
struct ReadResponseDto {
    content_base64: String,
    eof: bool,
    total_size: u64,
    bytes_read: u64,
}

#[derive(Debug, Serialize)]
struct WriteRequest {
    path: String,
    content_base64: String,
    append: bool,
}

#[derive(Debug, Serialize)]
struct DeleteRequest {
    path: String,
}

fn denied(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::PermissionDenied,
        capability: Some("files".to_string()),
        retryable: false,
        user_action: UserAction::RequestApproval,
        redacted_detail: detail.to_string(),
    }
}

fn check_relative(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(transport_error("file path must not be empty"));
    }
    if path.starts_with('/') || path.split('/').any(|part| part == "..") {
        return Err(transport_error("file path must not escape the root"));
    }
    Ok(())
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct FileGatewayClient {
    http: HttpClient,
}

impl FileGatewayClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    /// GET with at most one 401 retry. `refresh` is called once,
    /// only when the first attempt actually returned 401.
    fn get_with_retry<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        refresh: &mut dyn FnMut() -> Result<()>,
    ) -> Result<T> {
        let mut attempts = 0u32;
        loop {
            let (status, body) = self.http.request("GET", path, None)?;
            if status == 401 && attempts < MAX_AUTH_RETRIES {
                attempts += 1;
                refresh()?;
                continue;
            }
            if !(200..300).contains(&status) {
                return Err(gateway_error(status));
            }
            return serde_json::from_str(&body)
                .map_err(|_| transport_error("mock gateway returned invalid JSON"));
        }
    }

    pub fn list(&self, path: &str) -> Result<Vec<GatewayFileEntry>> {
        let mut refresh = || Ok(());
        self.list_with_refresh(path, &mut refresh)
    }

    pub fn list_with_refresh(
        &self,
        path: &str,
        refresh: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Vec<GatewayFileEntry>> {
        if !path.is_empty() {
            check_relative(path)?;
        }
        let query = if path.is_empty() {
            "/files/list".to_string()
        } else {
            format!("/files/list?path={path}")
        };
        self.get_with_retry(&query, refresh)
    }

    pub fn read_chunk(
        &self,
        path: &str,
        offset: u64,
        len: u64,
    ) -> Result<FileChunk> {
        let mut refresh = || Ok(());
        self.read_chunk_with_refresh(path, offset, len, &mut refresh)
    }

    pub fn read_chunk_with_refresh(
        &self,
        path: &str,
        offset: u64,
        len: u64,
        refresh: &mut dyn FnMut() -> Result<()>,
    ) -> Result<FileChunk> {
        check_relative(path)?;
        let query = format!("/files/read?path={path}&offset={offset}&len={len}");
        let dto: ReadResponseDto = self.get_with_retry(&query, refresh)?;
        Ok(FileChunk {
            data: from_base64(&dto.content_base64)?,
            eof: dto.eof,
            total_size: dto.total_size,
            bytes_read: dto.bytes_read,
        })
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
            let chunk = self.read_chunk(path, offset, chunk_len)?;
            if out.len() as u64 + chunk.bytes_read > max_total {
                return Err(transport_error("file exceeds max_total"));
            }
            offset += chunk.bytes_read;
            out.extend_from_slice(&chunk.data);
            if chunk.eof || chunk.bytes_read == 0 {
                break;
            }
        }
        Ok(out)
    }

    /// Write (overwrite, or append when `append` is true).
    /// Requires approval and is NEVER retried on 401.
    pub fn write(
        &self,
        path: &str,
        data: &[u8],
        approved: bool,
        append: bool,
    ) -> Result<u64> {
        if !approved {
            return Err(denied("file write requires approval"));
        }
        check_relative(path)?;
        let body = serde_json::to_string(&WriteRequest {
            path: path.to_string(),
            content_base64: to_base64(data),
            append,
        })
        .map_err(|_| transport_error("cannot encode file write request"))?;
        let (status, resp_body) = self.http.request("POST", "/files/write", Some(&body))?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        let value: serde_json::Value = serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))?;
        Ok(value
            .get("size_bytes")
            .and_then(|v| v.as_u64())
            .unwrap_or(data.len() as u64))
    }

    /// Delete a file. Requires approval and is NEVER retried on 401.
    pub fn delete(&self, path: &str, approved: bool) -> Result<()> {
        if !approved {
            return Err(denied("file delete requires approval"));
        }
        check_relative(path)?;
        let body = serde_json::to_string(&DeleteRequest { path: path.to_string() })
            .map_err(|_| transport_error("cannot encode file delete request"))?;
        let (status, _) = self.http.request("POST", "/files/delete", Some(&body))?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        Ok(())
    }
}
