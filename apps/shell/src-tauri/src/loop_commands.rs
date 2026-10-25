//! 模型步骤在阻塞线程等待，窗口与取消命令仍可响应。

use agent_desktop::loop_client::{ApprovalBinding, LoopClient, LoopSnapshot};

pub struct LoopTransport {
    pub base_url: String,
}
impl Default for LoopTransport {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:3001".into(),
        }
    }
}
fn error(e: agent_desktop::UnifiedError) -> String {
    format!("{:?}: {}", e.code, e.redacted_detail)
}

#[tauri::command(rename_all = "snake_case")]
pub async fn loop_start(
    state: tauri::State<'_, LoopTransport>,
    prompt: String,
    max_calls: Option<usize>,
    budget_ms: Option<u64>,
) -> Result<LoopSnapshot, String> {
    let base = state.base_url.clone();
    tauri::async_runtime::spawn_blocking(move || {
        LoopClient::new(&base)
            .and_then(|c| c.start(&prompt, max_calls, budget_ms))
            .map_err(error)
    })
    .await
    .map_err(|_| "循环任务执行失败，请查询状态".to_string())?
}
#[tauri::command(rename_all = "snake_case")]
pub async fn loop_get(
    state: tauri::State<'_, LoopTransport>,
    loop_id: String,
) -> Result<LoopSnapshot, String> {
    let base = state.base_url.clone();
    tauri::async_runtime::spawn_blocking(move || {
        LoopClient::new(&base)
            .and_then(|c| c.get(&loop_id))
            .map_err(error)
    })
    .await
    .map_err(|_| "循环查询失败".to_string())?
}
#[tauri::command(rename_all = "snake_case")]
pub async fn loop_resume(
    state: tauri::State<'_, LoopTransport>,
    loop_id: String,
    approval_id: Option<String>,
    binding: Option<ApprovalBinding>,
) -> Result<LoopSnapshot, String> {
    let base = state.base_url.clone();
    tauri::async_runtime::spawn_blocking(move || {
        LoopClient::new(&base)
            .and_then(|c| c.resume(&loop_id, approval_id.as_deref(), binding))
            .map_err(error)
    })
    .await
    .map_err(|_| "循环推进响应未确认，请查询状态".to_string())?
}
#[tauri::command(rename_all = "snake_case")]
pub async fn loop_cancel(
    state: tauri::State<'_, LoopTransport>,
    loop_id: String,
) -> Result<LoopSnapshot, String> {
    let base = state.base_url.clone();
    tauri::async_runtime::spawn_blocking(move || {
        LoopClient::new(&base)
            .and_then(|c| c.cancel(&loop_id))
            .map_err(error)
    })
    .await
    .map_err(|_| "取消响应未确认，请查询状态".to_string())?
}
