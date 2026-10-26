//! 模型步骤在阻塞线程等待，窗口与取消命令仍可响应。
//!
//! 网关地址取壳级共享端点（见 gateway_endpoint），生命周期步骤
//! （端口选择、附着或启动）设定后所有命令跟随；invoke 签名不变，
//! 前端调用无需改动。

use agent_desktop::loop_client::{ApprovalBinding, LoopClient, LoopSnapshot};

fn error(e: agent_desktop::UnifiedError) -> String {
    format!("{:?}: {}", e.code, e.redacted_detail)
}

fn base() -> String {
    crate::gateway_endpoint::base_url()
}

#[tauri::command(rename_all = "snake_case")]
pub async fn loop_start(
    prompt: String,
    max_calls: Option<usize>,
    budget_ms: Option<u64>,
) -> Result<LoopSnapshot, String> {
    let base = base();
    tauri::async_runtime::spawn_blocking(move || {
        LoopClient::new(&base)
            .and_then(|c| c.start(&prompt, max_calls, budget_ms))
            .map_err(error)
    })
    .await
    .map_err(|_| "循环任务执行失败，请查询状态".to_string())?
}
#[tauri::command(rename_all = "snake_case")]
pub async fn loop_get(loop_id: String) -> Result<LoopSnapshot, String> {
    let base = base();
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
    loop_id: String,
    approval_id: Option<String>,
    binding: Option<ApprovalBinding>,
) -> Result<LoopSnapshot, String> {
    let base = base();
    tauri::async_runtime::spawn_blocking(move || {
        LoopClient::new(&base)
            .and_then(|c| c.resume(&loop_id, approval_id.as_deref(), binding))
            .map_err(error)
    })
    .await
    .map_err(|_| "循环推进响应未确认，请查询状态".to_string())?
}
#[tauri::command(rename_all = "snake_case")]
pub async fn loop_list() -> Result<Vec<LoopSnapshot>, String> {
    let base = base();
    tauri::async_runtime::spawn_blocking(move || {
        LoopClient::new(&base).and_then(|c| c.list()).map_err(error)
    })
    .await
    .map_err(|_| "循环列表查询失败".to_string())?
}
#[tauri::command(rename_all = "snake_case")]
pub async fn loop_cancel(loop_id: String) -> Result<LoopSnapshot, String> {
    let base = base();
    tauri::async_runtime::spawn_blocking(move || {
        LoopClient::new(&base)
            .and_then(|c| c.cancel(&loop_id))
            .map_err(error)
    })
    .await
    .map_err(|_| "取消响应未确认，请查询状态".to_string())?
}
