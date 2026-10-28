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
//! Round 2 unified execution: writes and deletes execute under
//! a server-side bound approval. The client requests an approval
//! carrying the exact operation, decides it from the window's
//! confirmation, and executes with its id; the server consumes
//! the record exactly once and refuses any changed replay.
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayFileStat {
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
    approval_id: String,
}

#[derive(Debug, Serialize)]
struct DeleteRequest {
    path: String,
    approval_id: String,
}

#[derive(Debug, Serialize)]
struct RenameRequest {
    from: String,
    to: String,
    approval_id: String,
}

#[derive(Debug, Serialize)]
struct MkdirRequest {
    path: String,
    approval_id: String,
}

/// Bound approval request (round 2 unified execution): file
/// tools must present the exact operation so the server can
/// bind the record to it; `None` fields are omitted.
#[derive(Debug, Serialize)]
struct ApprovalRequestDto {
    tool: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    append: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct ApprovalRequestResponse {
    tool_call_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ApprovalDecideDto {
    tool_call_id: String,
    status: String,
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

    pub fn stat(&self, path: &str) -> Result<GatewayFileStat> {
        if !path.is_empty() {
            check_relative(path)?;
        }
        let mut refresh = || Ok(());
        self.get_with_retry(&format!("/files/stat?path={path}"), &mut refresh)
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

    /// Request a bound approval for one file operation, then
    /// decide it approved. `approved` means the window already
    /// confirmed: the record is requested and decided here so
    /// the server can bind and consume it on execute. A 403 on
    /// execute after approval can only mean the operation no
    /// longer matches the record (changed, already used, or
    /// expired) -- never a user rejection, which would have
    /// stopped before this point.
    fn request_bound_approval(
        &self,
        tool: &str,
        path: &str,
        to_path: Option<&str>,
        content: Option<(&[u8], bool)>,
    ) -> Result<String> {
        let (content_base64, append) = match content {
            Some((data, append)) => (Some(to_base64(data)), Some(append)),
            None => (None, None),
        };
        let body = serde_json::to_string(&ApprovalRequestDto {
            tool: tool.to_string(),
            path: Some(path.to_string()),
            to_path: to_path.map(str::to_string),
            content_base64,
            append,
        })
        .map_err(|_| transport_error("cannot encode approval request"))?;
        let (status, resp_body) = self.http.request("POST", "/approval/request", Some(&body))?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        let dto: ApprovalRequestResponse = serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))?;
        dto.tool_call_id
            .ok_or_else(|| transport_error("approval tool returned no tool_call_id"))
    }

    fn decide_bound_approval(&self, tool_call_id: &str) -> Result<()> {
        let body = serde_json::to_string(&ApprovalDecideDto {
            tool_call_id: tool_call_id.to_string(),
            status: "approved".to_string(),
        })
        .map_err(|_| transport_error("cannot encode approval decision"))?;
        let (status, _) = self.http.request("POST", "/approval/decide", Some(&body))?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        Ok(())
    }

    /// Write (overwrite, or append when `append` is true).
    /// Requires approval and is NEVER retried on 401. The
    /// write executes under the bound approval requested and
    /// decided above; the server consumes it exactly once.
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
        let approval_id = self.request_bound_approval("fs.write", path, None, Some((data, append)))?;
        self.decide_bound_approval(&approval_id)?;
        let body = serde_json::to_string(&WriteRequest {
            path: path.to_string(),
            content_base64: to_base64(data),
            append,
            approval_id,
        })
        .map_err(|_| transport_error("cannot encode file write request"))?;
        let (status, resp_body) = self.http.request("POST", "/files/write", Some(&body))?;
        if status == 403 {
            return Err(denied("approved file operation does not match this execution (changed, already used, or expired); nothing was written"));
        }
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

    /// Delete a file. Requires approval and is NEVER retried on
    /// 401. Executes under a bound approval, as `write` does.
    pub fn delete(&self, path: &str, approved: bool) -> Result<()> {
        if !approved {
            return Err(denied("file delete requires approval"));
        }
        check_relative(path)?;
        let approval_id = self.request_bound_approval("fs.delete", path, None, None)?;
        self.decide_bound_approval(&approval_id)?;
        let body = serde_json::to_string(&DeleteRequest {
            path: path.to_string(),
            approval_id,
        })
        .map_err(|_| transport_error("cannot encode file delete request"))?;
        let (status, _) = self.http.request("POST", "/files/delete", Some(&body))?;
        if status == 403 {
            return Err(denied("approved file operation does not match this execution (changed, already used, or expired); nothing was deleted"));
        }
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        Ok(())
    }

    pub fn rename(&self, from: &str, to: &str, approved: bool) -> Result<()> {
        if !approved {
            return Err(denied("file rename requires approval"));
        }
        check_relative(from)?;
        check_relative(to)?;
        let approval_id = self.request_bound_approval("fs.rename", from, Some(to), None)?;
        self.decide_bound_approval(&approval_id)?;
        let body = serde_json::to_string(&RenameRequest { from: from.to_string(), to: to.to_string(), approval_id })
            .map_err(|_| transport_error("cannot encode file rename request"))?;
        let (status, _) = self.http.request("POST", "/files/rename", Some(&body))?;
        if status == 403 {
            return Err(denied("approved file operation does not match this execution (changed, already used, or expired); nothing was renamed"));
        }
        if status == 409 {
            return Err(gateway_error_with_detail("rename target already exists; nothing was renamed"));
        }
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        Ok(())
    }

    pub fn mkdir(&self, path: &str, approved: bool) -> Result<()> {
        if !approved {
            return Err(denied("file mkdir requires approval"));
        }
        check_relative(path)?;
        let approval_id = self.request_bound_approval("fs.mkdir", path, None, None)?;
        self.decide_bound_approval(&approval_id)?;
        let body = serde_json::to_string(&MkdirRequest { path: path.to_string(), approval_id })
            .map_err(|_| transport_error("cannot encode file mkdir request"))?;
        let (status, _) = self.http.request("POST", "/files/mkdir", Some(&body))?;
        if status == 403 {
            return Err(denied("approved file operation does not match this execution (changed, already used, or expired); directory was not created"));
        }
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        Ok(())
    }
}

fn gateway_error_with_detail(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::Gateway,
        capability: Some("gateway".to_string()),
        retryable: false,
        user_action: UserAction::Retry,
        redacted_detail: detail.to_string(),
    }
}
