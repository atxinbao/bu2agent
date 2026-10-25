//! 单轮工具循环：网关推进一步，审批等待不计预算，终态只归档迟到结果。
//! 模型通过 trait 接入；未配置模型时不启动循环，测试使用假模型/假工具。

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::{Component, Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::server::AppState;

pub type BackendFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LoopStatus {
    Created,
    Running,
    AwaitingApproval,
    Completed,
    NeedsReview,
    Failed,
    Cancelled,
    Interrupted,
}

impl LoopStatus {
    pub fn terminal(&self) -> bool {
        !matches!(self, Self::Created | Self::Running | Self::AwaitingApproval)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CallStatus {
    Proposed,
    AwaitingApproval,
    Running,
    Succeeded,
    Failed,
    Unknown,
    Rejected,
    Duplicate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidTool,
    InvalidArguments,
    CapExceeded,
    BudgetExceeded,
    ModelFailed,
    ModelTimeout,
    ToolFailed,
    Unconfirmed,
    UserRejected,
    Interrupted,
    AuditUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentDigest {
    pub sha256: String,
    pub bytes: usize,
}

pub fn digest(text: &str) -> ContentDigest {
    ContentDigest {
        sha256: format!("{:x}", Sha256::digest(text.as_bytes())),
        bytes: text.len(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovalBinding {
    pub loop_id: String,
    pub call_id: usize,
    pub tool: String,
    pub args_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSummary {
    pub status: CallStatus,
    pub error: Option<ErrorCode>,
    pub body_digest: ContentDigest,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LateResult {
    pub arrived_at_ms: u64,
    pub result: ResultSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRecord {
    pub call_id: usize,
    pub tool: String,
    pub path: Option<String>,
    pub args_sha256: String,
    pub content_digest: Option<ContentDigest>,
    pub status: CallStatus,
    pub error: Option<ErrorCode>,
    pub approval_id: Option<String>,
    pub binding: Option<ApprovalBinding>,
    pub submitted: bool,
    pub duplicate_of: Option<usize>,
    pub result: Option<ResultSummary>,
    pub late: Option<LateResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopRecord {
    pub loop_id: String,
    pub prompt_digest: ContentDigest,
    pub model: String,
    pub status: LoopStatus,
    pub error: Option<ErrorCode>,
    pub max_calls: usize,
    pub proposal_count: usize,
    pub model_attempts: usize,
    pub budget_ms: u64,
    pub used_ms: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub queue_position: usize,
    pub calls: Vec<CallRecord>,
    pub unknown_writes: Vec<usize>,
    pub unknown_reads: Vec<usize>,
    pub late_model_arrivals_ms: Vec<u64>,
    pub final_digest: Option<ContentDigest>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoopView {
    #[serde(flatten)]
    pub record: LoopRecord,
    pub in_flight: bool,
    pub remaining_ms: u64,
    /// 最终回答只在当前进程内可用；重启后保留摘要和结构化未知项。
    pub final_text: Option<String>,
    /// 仅待审批时展示内存中的拟写全文；不进入 LoopRecord 或 JSONL。
    pub pending_write: Option<PendingWrite>,
    pub approval_status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PendingWrite {
    pub call_id: usize,
    pub path: String,
    pub text: String,
    pub args_sha256: String,
}

#[derive(Debug, Clone)]
pub struct Proposal {
    pub tool: String,
    pub args: Value,
    /// 提供方协议原文只留内存，不参与执行参数或审计序列化。
    pub wire: Option<WireCall>,
}

#[derive(Debug, Clone)]
pub struct WireCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Clone)]
pub enum ModelReply {
    Calls {
        proposals: Vec<Proposal>,
        content: Option<String>,
    },
    Final(String),
}

#[derive(Debug, Clone)]
pub enum Message {
    User(String),
    AssistantCalls {
        proposals: Vec<Proposal>,
        content: Option<String>,
    },
    Tool {
        call_id: usize,
        result: ToolMessage,
        duplicate_of: Option<usize>,
    },
}

#[derive(Debug, Clone)]
pub struct ToolMessage {
    pub body: String,
    pub summary: ResultSummary,
}

impl ToolMessage {
    fn from_outcome(outcome: ToolOutcome) -> Self {
        let body_digest = digest(&outcome.body);
        let mut end = outcome.body.len().min(4096);
        while !outcome.body.is_char_boundary(end) {
            end -= 1;
        }
        Self {
            summary: ResultSummary {
                status: outcome.status,
                error: outcome.error,
                truncated: end < outcome.body.len(),
                body_digest,
            },
            body: outcome.body[..end].to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ToolOutcome {
    pub status: CallStatus,
    pub error: Option<ErrorCode>,
    pub body: String,
}

impl ToolOutcome {
    pub fn success(body: String) -> Self {
        Self {
            status: CallStatus::Succeeded,
            error: None,
            body,
        }
    }
    pub fn failed(error: ErrorCode) -> Self {
        Self {
            status: CallStatus::Failed,
            error: Some(error),
            body: String::new(),
        }
    }
    pub fn unknown() -> Self {
        Self {
            status: CallStatus::Unknown,
            error: Some(ErrorCode::Unconfirmed),
            body: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ModelError {
    Failed,
    Timeout,
}

pub trait LoopModel: Send + Sync {
    fn name(&self) -> &str;
    fn complete(
        &self,
        messages: Vec<Message>,
        timeout: Duration,
    ) -> BackendFuture<Result<ModelReply, ModelError>>;
}

pub trait LoopTools: Send + Sync {
    fn execute(&self, tool: String, args: BTreeMap<String, String>) -> BackendFuture<ToolOutcome>;
}

#[derive(Clone)]
pub struct Timeouts {
    pub model: Duration,
    pub tool: Duration,
}
impl Default for Timeouts {
    fn default() -> Self {
        Self {
            model: Duration::from_secs(30),
            tool: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartLoop {
    pub prompt: String,
    pub max_calls: Option<usize>,
    pub budget_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeLoop {
    pub loop_id: String,
    pub approval_id: Option<String>,
    pub binding: Option<ApprovalBinding>,
}

#[derive(Debug, Clone)]
pub struct LoopError(pub u16, pub &'static str);

struct LiveLoop {
    record: LoopRecord,
    messages: Vec<Message>,
    arguments: Vec<Option<BTreeMap<String, String>>>,
    results: Vec<Option<ToolMessage>>,
    final_text: Option<String>,
    active: Option<(Instant, Duration)>,
}

impl LiveLoop {
    fn update_unknown(&mut self) {
        self.record.unknown_writes = self
            .record
            .calls
            .iter()
            .filter(|c| {
                c.tool == "fs.write"
                    && c.submitted
                    && matches!(c.status, CallStatus::Unknown | CallStatus::Running)
            })
            .map(|c| c.call_id)
            .collect();
        self.record.unknown_reads = self
            .record
            .calls
            .iter()
            .filter(|c| c.tool != "fs.write" && c.status == CallStatus::Unknown)
            .map(|c| c.call_id)
            .collect();
    }
    fn view(&self) -> LoopView {
        let mut record = self.record.clone();
        if let Some((start, limit)) = self.active {
            record.used_ms = record
                .used_ms
                .saturating_add(millis(start.elapsed().min(limit)))
                .min(record.budget_ms);
        }
        LoopView {
            remaining_ms: record.budget_ms.saturating_sub(record.used_ms),
            record,
            in_flight: self.active.is_some(),
            final_text: self.final_text.clone(),
            pending_write: if self.record.status == LoopStatus::AwaitingApproval {
                let i = self.record.queue_position;
                self.arguments
                    .get(i)
                    .and_then(Option::as_ref)
                    .and_then(|args| {
                        Some(PendingWrite {
                            call_id: self.record.calls.get(i)?.call_id,
                            path: args.get("path")?.clone(),
                            text: args.get("text")?.clone(),
                            args_sha256: self.record.calls.get(i)?.args_sha256.clone(),
                        })
                    })
            } else {
                None
            },
            approval_status: None,
        }
    }
    fn finish(&mut self, status: LoopStatus, error: Option<ErrorCode>) {
        self.record.status = status;
        self.record.error = error;
        self.update_unknown();
        if self.record.status != LoopStatus::Completed {
            // 非干净完成由程序给出说明，不让模型文字冒充已确认成功。
            self.final_text = Some(format!(
                "循环状态 {:?}，原因 {:?}；未确认写入 {:?}；未确认读取 {:?}",
                self.record.status,
                self.record.error,
                self.record.unknown_writes,
                self.record.unknown_reads
            ));
        }
    }
    fn deliver(&mut self, index: usize, result: ToolMessage, duplicate_of: Option<usize>) {
        let call = &mut self.record.calls[index];
        call.status = if duplicate_of.is_some() {
            CallStatus::Duplicate
        } else {
            result.summary.status.clone()
        };
        call.error = result.summary.error.clone();
        call.duplicate_of = duplicate_of;
        call.result = Some(result.summary.clone());
        self.messages.push(Message::Tool {
            call_id: call.call_id,
            result: result.clone(),
            duplicate_of,
        });
        self.results[index] = Some(result);
        self.record.queue_position = index + 1;
        self.record.status = LoopStatus::Running;
        self.update_unknown();
    }
}

struct EngineState {
    loops: HashMap<String, LiveLoop>,
    next_id: u64,
}

pub struct LoopEngine {
    state: Mutex<EngineState>,
    path: Option<PathBuf>,
    model: Option<Arc<dyn LoopModel>>,
    tools: Option<Arc<dyn LoopTools>>,
    timeouts: Timeouts,
}

fn now() -> u64 {
    millis(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
    )
}
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// 参数只接受现有文件工具模式；写入仅覆盖，不默默接受额外字段。
pub fn normalize(tool: &str, args: &Value) -> Result<BTreeMap<String, String>, ErrorCode> {
    if !matches!(tool, "fs.list" | "fs.read" | "fs.write") {
        return Err(ErrorCode::InvalidTool);
    }
    let object = args.as_object().ok_or(ErrorCode::InvalidArguments)?;
    let allowed = if tool == "fs.write" {
        vec!["path", "text"]
    } else {
        vec!["path"]
    };
    if object.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err(ErrorCode::InvalidArguments);
    }
    let raw = match object.get("path") {
        Some(Value::String(s)) => s.as_str(),
        None if tool == "fs.list" => "",
        _ => return Err(ErrorCode::InvalidArguments),
    };
    // Windows 的 /path 有根但没有盘符，is_absolute 不会拒绝它。
    // 必须在规范化丢弃 RootDir 前拒绝所有带根路径。
    if raw.contains(['\0', '\\'])
        || Path::new(raw).has_root()
        || Path::new(raw)
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(ErrorCode::InvalidArguments);
    }
    let path = Path::new(raw)
        .components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    if path.len() > 4096 || (path.is_empty() && tool != "fs.list") {
        return Err(ErrorCode::InvalidArguments);
    }
    let mut out = BTreeMap::from([("path".into(), path)]);
    if tool == "fs.write" {
        let text = object
            .get("text")
            .and_then(Value::as_str)
            .ok_or(ErrorCode::InvalidArguments)?;
        if text.len() > 1024 * 1024 {
            return Err(ErrorCode::InvalidArguments);
        }
        out.insert("text".into(), text.to_string());
    }
    Ok(out)
}

pub fn args_digest(tool: &str, args: &BTreeMap<String, String>) -> String {
    // 固定覆盖语义也纳入摘要，未来若加入写入模式，不会复用旧授权。
    digest(&serde_json::to_string(&(tool, "overwrite", args)).expect("string map")).sha256
}

impl LoopEngine {
    pub fn new(
        root: &Path,
        path: Option<PathBuf>,
        model: Option<Arc<dyn LoopModel>>,
        tools: Option<Arc<dyn LoopTools>>,
        timeouts: Timeouts,
    ) -> Result<Self, String> {
        if timeouts.model > Duration::from_secs(30)
            || timeouts.tool > Duration::from_secs(10)
            || timeouts.model.is_zero()
            || timeouts.tool.is_zero()
        {
            return Err("invalid_loop_timeouts".into());
        }
        let mut loops = HashMap::new();
        let mut next_id = 1;
        if let Some(p) = &path {
            crate::loop_store::outside_root(root, p)?;
            for mut record in crate::loop_store::load(p)? {
                if let Some(n) = record
                    .loop_id
                    .strip_prefix("loop-")
                    .and_then(|x| x.parse::<u64>().ok())
                {
                    next_id = next_id.max(n.saturating_add(1));
                }
                if !record.status.terminal() {
                    record.status = LoopStatus::Interrupted;
                    record.error = Some(ErrorCode::Interrupted);
                    record.updated_at_ms = now();
                }
                // 已取消循环也可能还有在途写；重启后同样不能把它留作 running。
                for call in &mut record.calls {
                    if call.status == CallStatus::Running {
                        call.status = CallStatus::Unknown;
                        call.error = Some(ErrorCode::Interrupted);
                    }
                }
                let mut live = LiveLoop {
                    record,
                    messages: vec![],
                    arguments: vec![],
                    results: vec![],
                    final_text: None,
                    active: None,
                };
                live.update_unknown();
                crate::loop_store::append(p, &live.record)?;
                loops.insert(live.record.loop_id.clone(), live);
            }
        }
        Ok(Self {
            state: Mutex::new(EngineState { loops, next_id }),
            path,
            model,
            tools,
            timeouts,
        })
    }

    fn save(&self, live: &mut LiveLoop) -> Result<(), LoopError> {
        live.record.updated_at_ms = now();
        live.update_unknown();
        if let Some(path) = &self.path {
            if crate::loop_store::append(path, &live.record).is_err() {
                live.active = None;
                live.finish(LoopStatus::Failed, Some(ErrorCode::AuditUnavailable));
                return Err(LoopError(500, "loop_audit_write_failed"));
            }
        }
        Ok(())
    }

    pub fn start(&self, request: StartLoop) -> Result<LoopView, LoopError> {
        let model = self
            .model
            .as_ref()
            .ok_or(LoopError(503, "loop_model_not_configured"))?;
        let max_calls = request.max_calls.unwrap_or(5);
        let budget_ms = request.budget_ms.unwrap_or(120_000);
        if max_calls == 0
            || max_calls > 5
            || budget_ms == 0
            || budget_ms > 120_000
            || request.prompt.trim().is_empty()
            || request.prompt.len() > 64 * 1024
        {
            return Err(LoopError(400, "invalid_loop_limits_or_prompt"));
        }
        let mut state = self.state.lock().unwrap();
        let id = format!("loop-{}", state.next_id);
        state.next_id += 1;
        let record = LoopRecord {
            loop_id: id.clone(),
            prompt_digest: digest(&request.prompt),
            model: model.name().into(),
            status: LoopStatus::Created,
            error: None,
            max_calls,
            proposal_count: 0,
            model_attempts: 0,
            budget_ms,
            used_ms: 0,
            created_at_ms: now(),
            updated_at_ms: now(),
            queue_position: 0,
            calls: vec![],
            unknown_writes: vec![],
            unknown_reads: vec![],
            late_model_arrivals_ms: vec![],
            final_digest: None,
        };
        let mut live = LiveLoop {
            record,
            messages: vec![Message::User(request.prompt)],
            arguments: vec![],
            results: vec![],
            final_text: None,
            active: None,
        };
        self.save(&mut live)?;
        let view = live.view();
        state.loops.insert(id, live);
        Ok(view)
    }

    pub fn get(&self, id: &str) -> Result<LoopView, LoopError> {
        self.state
            .lock()
            .unwrap()
            .loops
            .get(id)
            .map(LiveLoop::view)
            .ok_or(LoopError(404, "unknown_loop"))
    }

    pub fn cancel(&self, id: &str) -> Result<LoopView, LoopError> {
        let mut state = self.state.lock().unwrap();
        let live = state
            .loops
            .get_mut(id)
            .ok_or(LoopError(404, "unknown_loop"))?;
        if !live.record.status.terminal() {
            live.finish(LoopStatus::Cancelled, None);
            self.save(live)?;
        }
        Ok(live.view())
    }

    pub async fn resume(
        self: &Arc<Self>,
        app: AppState,
        request: ResumeLoop,
    ) -> Result<LoopView, LoopError> {
        let work;
        let limit;
        {
            let mut state = self.state.lock().unwrap();
            let live = state
                .loops
                .get_mut(&request.loop_id)
                .ok_or(LoopError(404, "unknown_loop"))?;
            if live.record.status.terminal() {
                return Err(LoopError(409, "loop_is_terminal"));
            }
            if live.active.is_some() {
                return Err(LoopError(409, "loop_step_in_flight"));
            }
            let remaining = live.record.budget_ms.saturating_sub(live.record.used_ms);
            if remaining == 0 {
                live.finish(LoopStatus::Failed, Some(ErrorCode::BudgetExceeded));
                self.save(live)?;
                return Ok(live.view());
            }
            let index = live.record.queue_position;
            if index < live.record.calls.len() {
                if live.record.calls[index].status == CallStatus::AwaitingApproval {
                    let call = &live.record.calls[index];
                    if request.approval_id != call.approval_id || request.binding != call.binding {
                        return Err(LoopError(403, "approval_binding_mismatch"));
                    }
                    let status = app.loop_approval_status(
                        call.approval_id.as_ref().unwrap(),
                        call.binding.as_ref().unwrap(),
                    )?;
                    match status {
                        agent_desktop::approval::ApprovalStatus::Rejected => {
                            let result = ToolMessage::from_outcome(ToolOutcome {
                                status: CallStatus::Rejected,
                                error: Some(ErrorCode::UserRejected),
                                body: String::new(),
                            });
                            live.deliver(index, result, None);
                            self.save(live)?;
                            return Ok(live.view());
                        }
                        agent_desktop::approval::ApprovalStatus::Approved => {}
                        _ => return Err(LoopError(409, "approval_not_approved")),
                    }
                } else {
                    if request.approval_id.is_some() || request.binding.is_some() {
                        return Err(LoopError(403, "unexpected_approval"));
                    }
                    if let Some(error) = live.record.calls[index].error.clone() {
                        live.deliver(
                            index,
                            ToolMessage::from_outcome(ToolOutcome::failed(error)),
                            None,
                        );
                        self.save(live)?;
                        return Ok(live.view());
                    }
                    if live.record.calls[index].tool == "fs.write" {
                        let duplicate = (0..index).find(|&i| {
                            let old = &live.record.calls[i];
                            old.args_sha256 == live.record.calls[index].args_sha256
                                && (old.submitted || old.status == CallStatus::Rejected)
                        });
                        if let Some(old) = duplicate {
                            let result = live.results[old].clone().expect("delivered result");
                            live.deliver(index, result, Some(old + 1));
                            self.save(live)?;
                            return Ok(live.view());
                        }
                        let call = &mut live.record.calls[index];
                        let binding = ApprovalBinding {
                            loop_id: live.record.loop_id.clone(),
                            call_id: call.call_id,
                            tool: call.tool.clone(),
                            args_sha256: call.args_sha256.clone(),
                        };
                        call.approval_id = Some(app.request_loop_approval(binding.clone())?);
                        call.binding = Some(binding);
                        call.status = CallStatus::AwaitingApproval;
                        live.record.status = LoopStatus::AwaitingApproval;
                        self.save(live)?;
                        return Ok(live.view());
                    }
                }
                let call = &mut live.record.calls[index];
                let args = live.arguments[index].clone().expect("validated arguments");
                // 消费批准、预留执行与取消共享循环锁；预留是步骤开始的线性化点。
                if let (Some(id), Some(binding)) = (&call.approval_id, &call.binding) {
                    app.consume_loop_approval(id, binding)?;
                }
                call.status = CallStatus::Running;
                call.submitted = true;
                work = Work::Tool {
                    index,
                    tool: call.tool.clone(),
                    args,
                    binding: call.binding.clone(),
                    approval_id: call.approval_id.clone(),
                };
                limit = self.timeouts.tool.min(Duration::from_millis(remaining));
            } else {
                if request.approval_id.is_some() || request.binding.is_some() {
                    return Err(LoopError(403, "unexpected_approval"));
                }
                work = Work::Model {
                    messages: live.messages.clone(),
                    closing: live.record.proposal_count >= live.record.max_calls,
                };
                live.record.model_attempts += 1;
                limit = self.timeouts.model.min(Duration::from_millis(remaining));
            }
            live.record.status = LoopStatus::Running;
            live.active = Some((Instant::now(), limit));
            self.save(live)?;
        }
        // 独立任务拥有推进过程；HTTP 客户端断连不能遗失步骤结果或解锁状态。
        let engine = Arc::clone(self);
        let id = request.loop_id;
        tokio::spawn(async move { engine.run_work(app, id, work, limit).await })
            .await
            .map_err(|_| LoopError(500, "loop_worker_failed"))?
    }

    async fn run_work(
        self: Arc<Self>,
        app: AppState,
        id: String,
        work: Work,
        limit: Duration,
    ) -> Result<LoopView, LoopError> {
        let mut job = match &work {
            Work::Model { messages, .. } => {
                let model = self.model.clone().expect("configured model");
                let messages = messages.clone();
                tokio::spawn(
                    async move { WorkResult::Model(model.complete(messages, limit).await) },
                )
            }
            Work::Tool {
                tool,
                args,
                binding,
                approval_id,
                ..
            } => {
                let (tool, args, binding, approval_id) = (
                    tool.clone(),
                    args.clone(),
                    binding.clone(),
                    approval_id.clone(),
                );
                let tools = self.tools.clone();
                tokio::spawn(async move {
                    WorkResult::Tool(
                        app.execute_loop_tool(tool, args, approval_id, binding, tools)
                            .await,
                    )
                })
            }
        };
        match tokio::time::timeout(limit, &mut job).await {
            Ok(result) => self.complete_work(&id, &work, result.ok(), false),
            Err(_) => {
                let view = self.complete_work(&id, &work, None, true);
                let engine = Arc::clone(&self);
                tokio::spawn(async move {
                    let result = job.await.ok();
                    engine.archive_late(&id, &work, result);
                });
                view
            }
        }
    }

    fn complete_work(
        &self,
        id: &str,
        work: &Work,
        result: Option<WorkResult>,
        timeout: bool,
    ) -> Result<LoopView, LoopError> {
        let mut state = self.state.lock().unwrap();
        let live = state.loops.get_mut(id).unwrap();
        if let Some((start, limit)) = live.active.take() {
            live.record.used_ms = live
                .record
                .used_ms
                .saturating_add(millis(start.elapsed().min(limit)))
                .min(live.record.budget_ms);
        }
        if live.record.status.terminal() {
            if timeout {
                // 到达等待截止不等于结果已到达；真正的迟到时间由归档任务记录。
                if let Work::Tool { index, .. } = work {
                    live.record.calls[*index].status = CallStatus::Unknown;
                    live.record.calls[*index].error = Some(ErrorCode::Unconfirmed);
                }
                live.update_unknown();
            } else {
                Self::record_late(live, work, result);
            }
            live.finish(live.record.status.clone(), live.record.error.clone());
            self.save(live)?;
            return Ok(live.view());
        }
        let exhausted = live.record.used_ms >= live.record.budget_ms;
        match work {
            Work::Model { closing, .. } => {
                if exhausted {
                    live.finish(LoopStatus::Failed, Some(ErrorCode::BudgetExceeded));
                    if result.is_some() {
                        live.record.late_model_arrivals_ms.push(now());
                    }
                } else if timeout
                    || matches!(result, Some(WorkResult::Model(Err(ModelError::Timeout))))
                {
                    live.finish(LoopStatus::Failed, Some(ErrorCode::ModelTimeout));
                } else if let Some(WorkResult::Model(Ok(reply))) = result {
                    match reply {
                        ModelReply::Final(text) if !text.trim().is_empty() => {
                            live.record.final_digest = Some(digest(&text));
                            live.final_text = Some(text);
                            live.update_unknown();
                            if live.record.unknown_writes.is_empty() {
                                live.finish(LoopStatus::Completed, None);
                                if !live.record.unknown_reads.is_empty() {
                                    live.final_text.as_mut().unwrap().push_str(&format!(
                                        "\n未确认读取：{:?}",
                                        live.record.unknown_reads
                                    ));
                                }
                            } else {
                                live.finish(LoopStatus::NeedsReview, None);
                            }
                        }
                        ModelReply::Calls { proposals, content } if !proposals.is_empty() => {
                            live.messages.push(Message::AssistantCalls {
                                proposals: proposals.clone(),
                                content,
                            });
                            for proposal in proposals {
                                live.record.proposal_count += 1;
                                let normal = normalize(&proposal.tool, &proposal.args);
                                let error = if *closing
                                    || live.record.proposal_count > live.record.max_calls
                                {
                                    Some(ErrorCode::CapExceeded)
                                } else {
                                    normal.as_ref().err().cloned()
                                };
                                let args = normal.ok();
                                let call_id = live.record.calls.len() + 1;
                                // 非法工具名不原样写盘，避免把模型任意文本带入审计。
                                let tool = if matches!(
                                    proposal.tool.as_str(),
                                    "fs.list" | "fs.read" | "fs.write"
                                ) {
                                    proposal.tool.clone()
                                } else {
                                    "invalid_tool".into()
                                };
                                let hash = args
                                    .as_ref()
                                    .map(|a| args_digest(&proposal.tool, a))
                                    .unwrap_or_else(|| digest(&proposal.args.to_string()).sha256);
                                live.record.calls.push(CallRecord {
                                    call_id,
                                    tool,
                                    path: args.as_ref().and_then(|a| a.get("path").cloned()),
                                    args_sha256: hash,
                                    content_digest: args
                                        .as_ref()
                                        .and_then(|a| a.get("text").map(|t| digest(t))),
                                    status: if error.is_some() {
                                        CallStatus::Failed
                                    } else {
                                        CallStatus::Proposed
                                    },
                                    error,
                                    approval_id: None,
                                    binding: None,
                                    submitted: false,
                                    duplicate_of: None,
                                    result: None,
                                    late: None,
                                });
                                live.arguments.push(args);
                                live.results.push(None);
                            }
                            if *closing {
                                live.finish(LoopStatus::Failed, Some(ErrorCode::CapExceeded));
                            }
                        }
                        _ => live.finish(LoopStatus::Failed, Some(ErrorCode::ModelFailed)),
                    }
                } else {
                    live.finish(LoopStatus::Failed, Some(ErrorCode::ModelFailed));
                }
            }
            Work::Tool { index, .. } => {
                let outcome = match result {
                    Some(WorkResult::Tool(outcome)) => outcome,
                    _ => ToolOutcome::unknown(),
                };
                if exhausted {
                    live.record.calls[*index].status = CallStatus::Unknown;
                    live.record.calls[*index].error = Some(ErrorCode::Unconfirmed);
                    if !timeout {
                        live.record.calls[*index].late = Some(LateResult {
                            arrived_at_ms: now(),
                            result: ToolMessage::from_outcome(outcome).summary,
                        });
                    }
                    live.finish(LoopStatus::Failed, Some(ErrorCode::BudgetExceeded));
                } else {
                    live.deliver(*index, ToolMessage::from_outcome(outcome), None);
                }
            }
        }
        self.save(live)?;
        Ok(live.view())
    }

    fn record_late(live: &mut LiveLoop, work: &Work, result: Option<WorkResult>) {
        match work {
            Work::Model { .. } => live.record.late_model_arrivals_ms.push(now()),
            Work::Tool { index, .. } => {
                let outcome = match result {
                    Some(WorkResult::Tool(outcome)) => outcome,
                    _ => ToolOutcome::unknown(),
                };
                let summary = ToolMessage::from_outcome(outcome).summary;
                let call = &mut live.record.calls[*index];
                if call.status == CallStatus::Running {
                    call.status = summary.status.clone();
                    call.result = Some(summary.clone());
                }
                // 已经给模型的 unknown 不回写为成功；迟到结果另行归档。
                call.late = Some(LateResult {
                    arrived_at_ms: now(),
                    result: summary,
                });
                live.update_unknown();
            }
        }
    }

    fn archive_late(&self, id: &str, work: &Work, result: Option<WorkResult>) {
        let mut state = self.state.lock().unwrap();
        if let Some(live) = state.loops.get_mut(id) {
            Self::record_late(live, work, result);
            if live.record.status.terminal() {
                live.finish(live.record.status.clone(), live.record.error.clone());
            }
            if self.save(live).is_err() {
                eprintln!("warning: cannot persist late loop metadata");
            }
        }
    }
}

enum Work {
    Model {
        messages: Vec<Message>,
        closing: bool,
    },
    Tool {
        index: usize,
        tool: String,
        args: BTreeMap<String, String>,
        binding: Option<ApprovalBinding>,
        approval_id: Option<String>,
    },
}
enum WorkResult {
    Model(Result<ModelReply, ModelError>),
    Tool(ToolOutcome),
}
