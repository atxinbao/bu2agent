//! Tauri invoke commands for the chat line, backed by the Rust
//! `ChatClient` (apps/desktop).
//!
//! Until now the frontend called the Mock Gateway over HTTP
//! directly. In a Tauri window it now goes through these
//! commands instead, so the Rust client owns the transport:
//! localhost-only, no TLS, Mock only -- the frontend never
//! sees the gateway URL. The plain HTTP client stays as the
//! fallback for a browser preview without a shell.
//!
//! 聊天生成可能等待模型数十秒，通过 spawn_blocking 运行同步客户端，避免阻塞窗口。
//!
//! Error strings are safe to surface: `UnifiedError` is
//! redacted by construction (bodies never land in it, only
//! the HTTP status / transport cause).

use agent_desktop::approval::{may_execute, ApprovalStatus};
use agent_desktop::chat_client::{ChatClient, HistoryMessage};
use agent_desktop::gateway::SubscriptionRecord;
use agent_desktop::UnifiedError;
use serde::{Deserialize, Serialize};

/// Mock Gateway only, localhost only. The address is the shell-wide
/// endpoint (default 127.0.0.1:3001); the lifecycle step may move it
/// after port selection, and every command follows.
fn client() -> Result<ChatClient, String> {
    ChatClient::new(&crate::gateway_endpoint::base_url()).map_err(err_string)
}

fn err_string(error: UnifiedError) -> String {
    format!("{:?}: {}", error.code, error.redacted_detail)
}

/// Wire shape for a stream record. Mirrors the HTTP DTOs
/// (snake_case, payload as a JSON string) so the frontend
/// mapping code is identical for both transports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecordDto {
    stream_id: String,
    sequence: u64,
    event_type: String,
    payload: String,
    resume_cursor: Option<String>,
}

impl From<SubscriptionRecord> for StreamRecordDto {
    fn from(record: SubscriptionRecord) -> Self {
        Self {
            stream_id: record.stream_id,
            sequence: record.sequence,
            event_type: record.event_type,
            payload: record.payload,
            resume_cursor: record.resume_cursor,
        }
    }
}

impl From<StreamRecordDto> for SubscriptionRecord {
    fn from(dto: StreamRecordDto) -> Self {
        Self {
            stream_id: dto.stream_id,
            sequence: dto.sequence,
            event_type: dto.event_type,
            payload: dto.payload,
            resume_cursor: dto.resume_cursor,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ApplyResultDto {
    kept: Vec<StreamRecordDto>,
    last_applied_sequence: u64,
}

#[tauri::command]
pub fn gateway_health() -> bool {
    client()
        .and_then(|c| c.health().map_err(err_string))
        .unwrap_or(false)
}

#[tauri::command]
pub fn gateway_history() -> Result<Vec<HistoryMessage>, String> {
    client()?.history().map_err(err_string)
}

#[tauri::command(rename_all = "snake_case")]
pub async fn gateway_stream_start(
    stream_id: String,
    turn_id: String,
    text: String,
) -> Result<Vec<StreamRecordDto>, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let records = client()?.start_stream(&stream_id, &turn_id, &text).map_err(err_string)?;
        Ok(records.into_iter().map(StreamRecordDto::from).collect())
    }).await.map_err(|_| "聊天任务执行失败".to_string())?
}

#[tauri::command(rename_all = "snake_case")]
pub fn gateway_stream_apply(
    stream_id: String,
    records: Vec<StreamRecordDto>,
) -> Result<ApplyResultDto, String> {
    let input: Vec<SubscriptionRecord> =
        records.into_iter().map(SubscriptionRecord::from).collect();
    let applied = client()?
        .apply_stream(&stream_id, &input)
        .map_err(err_string)?;
    Ok(ApplyResultDto {
        kept: applied
            .kept
            .into_iter()
            .map(StreamRecordDto::from)
            .collect(),
        last_applied_sequence: applied.last_applied_sequence,
    })
}

#[tauri::command(rename_all = "snake_case")]
pub fn gateway_stream_cancel(stream_id: String) -> Result<bool, String> {
    client()?.cancel_stream(&stream_id).map_err(err_string)
}

#[tauri::command]
pub fn gateway_approval_check(status: String, sensitive: bool) -> Result<bool, String> {
    let parsed = match status.as_str() {
        "not_required" => ApprovalStatus::NotRequired,
        "pending" => ApprovalStatus::Pending,
        "approved" => ApprovalStatus::Approved,
        "rejected" => ApprovalStatus::Rejected,
        "edited" => ApprovalStatus::Edited,
        _ => return Err(format!("unknown approval status: {status}")),
    };
    Ok(may_execute(parsed, sensitive))
}
