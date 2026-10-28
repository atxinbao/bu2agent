//! 本地假 DeepSeek HTTP 服务 + 内存文件工具；不读真实凭据或操作真实文件/提醒。

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use mock_gateway::deepseek::{DeepSeekClient, DeepSeekConfig};
use mock_gateway::deepseek_loop::DeepSeekLoopModel;
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};

struct Response {
    status: u16,
    body: String,
    delay: Duration,
}
impl Response {
    fn json(body: Value) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
            delay: Duration::ZERO,
        }
    }
}
#[derive(Clone, Default)]
struct Provider {
    responses: Arc<Mutex<VecDeque<Response>>>,
    requests: Arc<Mutex<Vec<Value>>>,
}
async fn completion(
    State(p): State<Provider>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, String) {
    assert_eq!(headers["authorization"], "Bearer fake-loop-key");
    p.requests.lock().unwrap().push(body);
    let reply = p
        .responses
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected retry/model request");
    tokio::time::sleep(reply.delay).await;
    (StatusCode::from_u16(reply.status).unwrap(), reply.body)
}

#[derive(Clone, Default)]
struct MemoryTools {
    files: Arc<Mutex<BTreeMap<String, String>>>,
    executions: Arc<AtomicUsize>,
    unknown_write: Arc<Mutex<bool>>,
}
impl LoopTools for MemoryTools {
    fn execute(&self, tool: String, args: BTreeMap<String, String>) -> BackendFuture<ToolOutcome> {
        self.executions.fetch_add(1, Ordering::SeqCst);
        let this = self.clone();
        Box::pin(async move {
            let mut files = this.files.lock().unwrap();
            match tool.as_str() {
                "fs.list" => ToolOutcome::success(
                    serde_json::to_string(&files.keys().collect::<Vec<_>>()).unwrap(),
                ),
                "fs.read" => files
                    .get(&args["path"])
                    .map(|s| ToolOutcome::success(s.clone()))
                    .unwrap_or_else(|| ToolOutcome::failed(ErrorCode::ToolFailed)),
                "fs.write" => {
                    if *this.unknown_write.lock().unwrap() {
                        return ToolOutcome::unknown();
                    }
                    files.insert(args["path"].clone(), args["text"].clone());
                    ToolOutcome::success("memory write confirmed".into())
                }
                _ => panic!("invalid tool reached executor"),
            }
        })
    }
}

static NEXT: AtomicUsize = AtomicUsize::new(1);
struct Harness {
    dir: PathBuf,
    provider: Provider,
    tools: MemoryTools,
    config: DeepSeekConfig,
    url: String,
    client: reqwest::Client,
    servers: Vec<tokio::task::JoinHandle<()>>,
}
impl Harness {
    async fn new(responses: Vec<Value>) -> Self {
        Self::custom(
            responses.into_iter().map(Response::json).collect(),
            Timeouts::default(),
        )
        .await
    }
    async fn custom(responses: Vec<Response>, timeouts: Timeouts) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "bu2agent-deepseek-loop-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let provider = Provider::default();
        provider.responses.lock().unwrap().extend(responses);
        let (base, fake) = serve(
            Router::new()
                .route("/chat/completions", post(completion))
                .with_state(provider.clone()),
        )
        .await;
        let config = DeepSeekConfig {
            base_url: format!("{base}/"),
            model: "fake-model-override".into(),
            api_key: Some("fake-loop-key".into()),
        };
        let model = DeepSeekLoopModel::new(config.clone()).unwrap().unwrap();
        let tools = MemoryTools::default();
        let state = AppState::with_files_root_and_approvals(
            dir.join("files"),
            Some(dir.join("approvals.jsonl")),
        )
        .with_deepseek(DeepSeekClient::new(config.clone()))
        .with_loop_backends(
            Some(dir.join("loops.jsonl")),
            Some(Arc::new(model)),
            Some(Arc::new(tools.clone())),
            timeouts,
        )
        .unwrap()
        // Single-step assertions: the driver stays off here and
        // is covered by driver_smoke with the flag on.
        .with_auto_drive(false);
        let (url, gateway) = serve(app(state)).await;
        Self {
            dir,
            provider,
            tools,
            config,
            url,
            client: reqwest::Client::new(),
            servers: vec![fake, gateway],
        }
    }
    async fn post(&self, path: &str, value: Value) -> (u16, Value) {
        let response = self
            .client
            .post(format!("{}{path}", self.url))
            .json(&value)
            .send()
            .await
            .unwrap();
        (response.status().as_u16(), response.json().await.unwrap())
    }
    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{body}");
        body
    }
    async fn start(&self) -> String {
        self.ok(
            "/agent/loop/start",
            json!({"prompt":"private prompt sentinel"}),
        )
        .await["loop_id"]
            .as_str()
            .unwrap()
            .into()
    }
    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id":id})).await
    }
    async fn decide(&self, view: &Value, decision: &str) -> Value {
        let call = &view["calls"][view["queue_position"].as_u64().unwrap() as usize];
        self.ok(
            "/approval/decide",
            json!({"tool_call_id":call["approval_id"],"status":decision}),
        )
        .await;
        self.ok("/agent/loop/resume", json!({"loop_id":view["loop_id"],"approval_id":call["approval_id"],"binding":call["binding"]})).await
    }
    fn requests(&self) -> Vec<Value> {
        self.provider.requests.lock().unwrap().clone()
    }
    fn audit(&self) -> String {
        std::fs::read_to_string(self.dir.join("loops.jsonl")).unwrap()
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        for server in &self.servers {
            server.abort();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
async fn serve(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (
        url,
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    )
}
fn call(id: &str, name: &str, args: Value) -> Value {
    json!({"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}})
}
fn calls(items: Vec<Value>) -> Value {
    json!({"choices":[{"message":{"role":"assistant","content":"assistant plan sentinel","tool_calls":items},"finish_reason":"tool_calls"}]})
}
fn final_reply() -> Value {
    json!({"choices":[{"message":{"role":"assistant","content":"private final sentinel"},"finish_reason":"stop"}]})
}
fn results(request: &Value) -> Vec<(String, Value)> {
    request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "tool")
        .map(|m| {
            (
                m["tool_call_id"].as_str().unwrap().into(),
                serde_json::from_str(m["content"].as_str().unwrap()).unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn indexed_non_stream_calls_keep_array_order_ids_and_write_approval() {
    let mut items = vec![
        call("indexed-list", "fs_list", json!({})),
        call("indexed-read", "fs_read", json!({"path":"seed"})),
        call(
            "indexed-write",
            "fs_write",
            json!({"path":"report","text":"indexed write body"}),
        ),
    ];
    // 元数据不参与重排；实际执行顺序必须仍是响应数组顺序。
    for (item, index) in items.iter_mut().zip([7, 2, 0]) {
        item["index"] = json!(index);
    }
    let h = Harness::new(vec![calls(items), final_reply()]).await;
    h.tools
        .files
        .lock()
        .unwrap()
        .insert("seed".into(), "indexed read body".into());
    let id = h.start().await;
    assert_eq!(h.step(&id).await["proposal_count"], 3);
    h.step(&id).await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    assert_eq!(waiting["calls"][2]["tool"], "fs.write");
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 2);
    assert!(!h.tools.files.lock().unwrap().contains_key("report"));
    h.decide(&waiting, "approved").await;
    assert_eq!(h.step(&id).await["status"], "completed");
    assert_eq!(
        h.tools.files.lock().unwrap()["report"],
        "indexed write body"
    );
    let requests = h.requests();
    let returned = results(&requests[1]);
    assert_eq!(
        returned
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["indexed-list", "indexed-read", "indexed-write"]
    );
    assert_eq!(returned[1].1["body"], "indexed read body");
    for wire in requests[1]["messages"][2]["tool_calls"].as_array().unwrap() {
        assert!(wire.get("index").is_none());
    }
    for private in ["indexed-list", "indexed read body", "indexed write body"] {
        assert!(!h.audit().contains(private));
    }
}

#[tokio::test]
async fn invalid_index_and_unknown_envelope_fields_still_fail_without_execution() {
    let mut items = Vec::new();
    for (i, index) in [Value::Null, json!("0"), json!(-1), json!(0.5)]
        .into_iter()
        .enumerate()
    {
        let mut item = call(&format!("bad-index-{i}"), "fs_read", json!({"path":"seed"}));
        item["index"] = index;
        items.push(item);
    }
    let mut extra = call(
        "extra-envelope",
        "fs_write",
        json!({"path":"out","text":"never"}),
    );
    extra["index"] = json!(0);
    extra["unexpected"] = json!(true);
    items.push(extra);
    let h = Harness::new(vec![calls(items), final_reply()]).await;
    let id = h.start().await;
    h.step(&id).await;
    for _ in 0..5 {
        h.step(&id).await;
    }
    let result = h.step(&id).await;
    assert_eq!(result["status"], "completed");
    assert_eq!(result["proposal_count"], 5);
    for item in result["calls"].as_array().unwrap() {
        assert_eq!(item["error"], "invalid_arguments");
        assert!(item["approval_id"].is_null());
    }
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
    assert!(results(&h.requests()[1])
        .iter()
        .all(|(_, r)| r["error"] == "invalid_arguments"));
}

#[tokio::test]
async fn multi_call_ids_history_schema_and_bound_write_roundtrip() {
    let h = Harness::new(vec![
        calls(vec![
            call("wire-list-A", "fs_list", json!({})),
            call("wire-read-B", "fs_read", json!({"path":"seed"})),
        ]),
        calls(vec![call(
            "wire-write-C",
            "fs_write",
            json!({"path":"./report", "text":"private write sentinel"}),
        )]),
        final_reply(),
    ])
    .await;
    h.tools
        .files
        .lock()
        .unwrap()
        .insert("seed".into(), "private read sentinel".into());
    let id = h.start().await;
    assert_eq!(h.step(&id).await["proposal_count"], 2);
    h.step(&id).await;
    h.step(&id).await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 2);
    assert_eq!(waiting["calls"][2]["path"], "report");
    let binding = &waiting["calls"][2]["binding"];
    assert_eq!(
        binding["args_sha256"],
        args_digest(
            "fs.write",
            &BTreeMap::from([
                ("path".into(), "report".into()),
                ("text".into(), "private write sentinel".into())
            ])
        )
    );
    assert_eq!(
        h.post(
            "/agent/loop/resume",
            json!({"loop_id":id,"approval_id":"wrong","binding":binding})
        )
        .await
        .0,
        403
    );
    h.decide(&waiting, "approved").await;
    assert_eq!(h.step(&id).await["status"], "completed");
    assert_eq!(
        h.tools.files.lock().unwrap()["report"],
        "private write sentinel"
    );
    let requests = h.requests();
    assert_eq!(requests.len(), 3);
    let first = &requests[0];
    assert_eq!(first["model"], "fake-model-override");
    assert_eq!(first["stream"], false);
    assert_eq!(first["thinking"]["type"], "disabled");
    assert_eq!(first["messages"][0]["role"], "system");
    assert_eq!(first["messages"][1]["role"], "user");
    assert_eq!(first["tools"].as_array().unwrap().len(), 5);
    for (i, name) in [
        "fs_list",
        "fs_read",
        "fs_write",
        "reminders_list",
        "reminders_set_completed",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(first["tools"][i]["function"]["name"], *name);
        assert_eq!(
            first["tools"][i]["function"]["parameters"]["additionalProperties"],
            false
        );
    }
    assert_eq!(
        requests[1]["messages"][2]["content"],
        "assistant plan sentinel"
    );
    assert_eq!(
        results(&requests[2])
            .iter()
            .map(|(id, _)| id.as_str())
            .collect::<Vec<_>>(),
        ["wire-list-A", "wire-read-B", "wire-write-C"]
    );
    assert_eq!(results(&requests[1])[1].1["body"], "private read sentinel");
    for secret in [
        "private prompt sentinel",
        "private write sentinel",
        "private read sentinel",
        "private final sentinel",
        "assistant plan sentinel",
        "wire-write-C",
        "fake-loop-key",
    ] {
        assert!(!h.audit().contains(secret));
    }
    assert_eq!(std::fs::read_dir(h.dir.join("files")).unwrap().count(), 0);
}

#[tokio::test]
async fn invalid_proposals_count_and_return_errors_without_execution() {
    let mut malformed = call("bad-json", "fs_write", json!({}));
    malformed["function"]["arguments"] = json!("{broken-json");
    let mut extra_envelope = call("extra-envelope", "fs_read", json!({"path":"seed"}));
    extra_envelope["function"]["unexpected"] = json!(true);
    let h = Harness::new(vec![
        calls(vec![
            malformed,
            call("bad-tool", "fs_delete", json!({"path":"seed"})),
            call(
                "extra-arg",
                "fs_write",
                json!({"path":"seed","text":"x","append":true}),
            ),
            call("escape", "fs_read", json!({"path":"../seed"})),
            extra_envelope,
        ]),
        final_reply(),
    ])
    .await;
    let id = h.start().await;
    assert_eq!(h.step(&id).await["proposal_count"], 5);
    for _ in 0..5 {
        h.step(&id).await;
    }
    let view = h.step(&id).await;
    assert_eq!(view["status"], "completed");
    assert_eq!(view["model_attempts"], 2);
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
    for call in view["calls"].as_array().unwrap() {
        assert!(call["approval_id"].is_null());
    }
    let request = &h.requests()[1];
    assert_eq!(
        request["messages"][2]["tool_calls"][0]["function"]["arguments"],
        "{broken-json"
    );
    let returned = results(request);
    assert_eq!(returned.len(), 5);
    assert_eq!(returned[1].1["error"], "invalid_tool");
    for i in [0, 2, 3, 4] {
        assert_eq!(returned[i].1["error"], "invalid_arguments");
    }
}

#[tokio::test]
async fn excess_proposals_receive_cap_errors_and_closing_cannot_execute() {
    let proposals = (0..6)
        .map(|i| call(&format!("p-{i}"), "fs_list", json!({})))
        .collect();
    let h = Harness::new(vec![
        calls(proposals),
        calls(vec![call(
            "closing",
            "fs_write",
            json!({"path":"out","text":"no"}),
        )]),
    ])
    .await;
    let id = h.start().await;
    h.step(&id).await;
    for _ in 0..6 {
        h.step(&id).await;
    }
    let view = h.step(&id).await;
    assert_eq!(view["error"], "cap_exceeded");
    assert_eq!(view["proposal_count"], 7);
    assert_eq!(view["calls"][5]["status"], "failed");
    assert!(view["calls"][6]["approval_id"].is_null());
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 5);
    assert_eq!(results(&h.requests()[1])[5].1["error"], "cap_exceeded");
    assert_eq!(h.requests().len(), 2);
}

#[tokio::test]
async fn non_object_and_wrong_typed_arguments_are_invalid_proposals() {
    let h = Harness::new(vec![
        calls(vec![
            call("array", "fs_list", json!([])),
            call("null", "fs_list", Value::Null),
            call("path-number", "fs_read", json!({"path":42})),
            call("missing-text", "fs_write", json!({"path":"out"})),
            call("unadvertised-alias", "fs.read", json!({"path":"seed"})),
        ]),
        final_reply(),
    ])
    .await;
    let id = h.start().await;
    h.step(&id).await;
    for _ in 0..5 {
        h.step(&id).await;
    }
    assert_eq!(h.step(&id).await["status"], "completed");
    let returned = results(&h.requests()[1]);
    for (_, result) in &returned[..4] {
        assert_eq!(result["error"], "invalid_arguments");
    }
    assert_eq!(returned[4].1["error"], "invalid_tool");
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn adapter_itself_enforces_http_deadline_without_engine_timer() {
    let h = Harness::custom(
        vec![Response {
            status: 200,
            body: final_reply().to_string(),
            delay: Duration::from_millis(200),
        }],
        Timeouts::default(),
    )
    .await;
    let model = DeepSeekLoopModel::new(h.config.clone()).unwrap().unwrap();
    let result = model
        .complete(
            vec![Message::User("test".into())],
            Duration::from_millis(40),
        )
        .await;
    assert!(matches!(result, Err(ModelError::Timeout)));
    assert_eq!(h.requests().len(), 1);
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn missing_or_non_string_arguments_still_count_as_invalid_proposals() {
    let mut missing = call("missing", "fs_write", json!({}));
    missing["function"]
        .as_object_mut()
        .unwrap()
        .remove("arguments");
    let mut object = call("object", "fs_read", json!({}));
    object["function"]["arguments"] = json!({"path":"seed"});
    let h = Harness::new(vec![calls(vec![missing, object]), final_reply()]).await;
    let id = h.start().await;
    let view = h.step(&id).await;
    assert_eq!(view["proposal_count"], 2);
    assert_eq!(view["status"], "running");
    h.step(&id).await;
    h.step(&id).await;
    assert_eq!(h.step(&id).await["status"], "completed");
    let request = &h.requests()[1];
    let returned = results(request);
    assert_eq!(returned[0].0, "missing");
    assert_eq!(returned[1].0, "object");
    for (_, result) in &returned {
        assert_eq!(result["error"], "invalid_arguments");
    }
    for item in request["messages"][2]["tool_calls"].as_array().unwrap() {
        assert_eq!(item["function"]["arguments"], "");
    }
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn duplicate_unknown_write_keeps_distinct_wire_ids_and_needs_review() {
    let h = Harness::new(vec![
        calls(vec![call(
            "first",
            "fs_write",
            json!({"path":"out","text":"x"}),
        )]),
        calls(vec![call(
            "duplicate",
            "fs_write",
            json!({"path":"./out","text":"x"}),
        )]),
        final_reply(),
    ])
    .await;
    *h.tools.unknown_write.lock().unwrap() = true;
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    h.decide(&waiting, "approved").await;
    h.step(&id).await;
    let dup = h.step(&id).await;
    assert_eq!(dup["calls"][1]["duplicate_of"], 1);
    assert!(dup["calls"][1]["approval_id"].is_null());
    assert_eq!(h.step(&id).await["status"], "needs_review");
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 1);
    let returned = results(&h.requests()[2]);
    assert_eq!(returned[0].0, "first");
    assert_eq!(returned[1].0, "duplicate");
    assert_eq!(returned[1].1["status"], "unknown");
    assert_eq!(returned[1].1["duplicate_of"], 1);
}

#[tokio::test]
async fn rejected_write_is_returned_and_duplicate_never_reopens_approval() {
    let h = Harness::new(vec![
        calls(vec![
            call("one", "fs_write", json!({"path":"out","text":"x"})),
            call("two", "fs_write", json!({"path":"out","text":"x"})),
        ]),
        final_reply(),
    ])
    .await;
    let id = h.start().await;
    h.step(&id).await;
    let waiting = h.step(&id).await;
    h.decide(&waiting, "rejected").await;
    h.step(&id).await;
    assert_eq!(h.step(&id).await["status"], "completed");
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
    let returned = results(&h.requests()[1]);
    assert_eq!(returned[0].1["status"], "rejected");
    assert_eq!(returned[1].1["error"], "user_rejected");
    assert_eq!(returned[1].1["duplicate_of"], 1);
}

#[tokio::test]
async fn utf8_tool_body_is_bounded_on_wire_and_absent_from_audit() {
    let h = Harness::new(vec![
        calls(vec![call("read", "fs_read", json!({"path":"seed"}))]),
        final_reply(),
    ])
    .await;
    h.tools
        .files
        .lock()
        .unwrap()
        .insert("seed".into(), "汉".repeat(2000));
    let id = h.start().await;
    h.step(&id).await;
    h.step(&id).await;
    h.step(&id).await;
    let returned = results(&h.requests()[1]);
    assert_eq!(returned[0].1["body"].as_str().unwrap().len(), 4095);
    assert_eq!(returned[0].1["original_bytes"], 6000);
    assert_eq!(returned[0].1["truncated"], true);
    assert!(!h.audit().contains('汉'));
}

#[tokio::test]
async fn http_errors_end_without_retry_or_provider_body_leak() {
    for status in [302, 401, 429, 500, 503] {
        let h = Harness::custom(
            vec![Response {
                status,
                body: "sensitive provider error".into(),
                delay: Duration::from_millis(15),
            }],
            Timeouts::default(),
        )
        .await;
        let id = h.start().await;
        let view = h.step(&id).await;
        assert_eq!(view["error"], "model_failed");
        assert!(view["used_ms"].as_u64().unwrap() >= 10);
        assert_eq!(
            h.post("/agent/loop/resume", json!({"loop_id":id})).await.0,
            409
        );
        assert_eq!(h.requests().len(), 1);
        assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
        assert!(!h.audit().contains("sensitive provider error"));
    }
}

#[tokio::test]
async fn malformed_json_and_unpairable_or_truncated_responses_fail_whole_step() {
    let mut missing_id = call("id", "fs_read", json!({"path":"seed"}));
    missing_id.as_object_mut().unwrap().remove("id");
    let mut truncated = calls(vec![call("id", "fs_read", json!({"path":"seed"}))]);
    truncated["choices"][0]["finish_reason"] = json!("length");
    let bodies = vec![
        "broken-json".into(),
        json!({"choices":[]}).to_string(),
        calls(vec![missing_id]).to_string(),
        calls(vec![
            call("same", "fs_list", json!({})),
            call("same", "fs_list", json!({})),
        ])
        .to_string(),
        truncated.to_string(),
        json!({"choices":[{"message":{"role":"assistant","content":""},"finish_reason":"stop"}]})
            .to_string(),
        json!({"choices":[{"message":{"role":"user","content":"spoof"},"finish_reason":"stop"}]})
            .to_string(),
    ];
    for body in bodies {
        let h = Harness::custom(
            vec![Response {
                status: 200,
                body,
                delay: Duration::ZERO,
            }],
            Timeouts::default(),
        )
        .await;
        let id = h.start().await;
        let view = h.step(&id).await;
        assert_eq!(view["error"], "model_failed");
        assert_eq!(view["proposal_count"], 0);
        assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
        assert_eq!(h.requests().len(), 1);
    }
}

#[tokio::test]
async fn model_http_timeout_charges_time_without_retry_or_late_tool_execution() {
    let h = Harness::custom(
        vec![Response {
            status: 200,
            body: calls(vec![call(
                "late",
                "fs_write",
                json!({"path":"out","text":"no"}),
            )])
            .to_string(),
            delay: Duration::from_millis(200),
        }],
        Timeouts {
            model: Duration::from_millis(40),
            tool: Duration::from_secs(10),
        },
    )
    .await;
    let id = h.start().await;
    let view = h.step(&id).await;
    assert_eq!(view["error"], "model_timeout");
    assert!(view["used_ms"].as_u64().unwrap() >= 35);
    tokio::time::sleep(Duration::from_millis(220)).await;
    assert_eq!(h.requests().len(), 1);
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
    assert!(!h.audit().contains("late\""));
}

#[tokio::test]
async fn remaining_budget_limits_real_http_wait() {
    let h = Harness::custom(
        vec![Response {
            status: 200,
            body: final_reply().to_string(),
            delay: Duration::from_millis(200),
        }],
        Timeouts::default(),
    )
    .await;
    let id = h
        .ok("/agent/loop/start", json!({"prompt":"task","budget_ms":40}))
        .await["loop_id"]
        .as_str()
        .unwrap()
        .to_string();
    let view = h.step(&id).await;
    assert_eq!(view["status"], "failed");
    assert_eq!(view["error"], "budget_exceeded");
    assert_eq!(view["used_ms"], 40);
    assert_eq!(view["remaining_ms"], 0);
    assert_eq!(h.requests().len(), 1);
}

#[tokio::test]
async fn cancel_pending_http_response_archives_without_enqueuing_calls() {
    let h = Harness::custom(
        vec![Response {
            status: 200,
            body: calls(vec![call(
                "cancelled-wire",
                "fs_write",
                json!({"path":"out","text":"no"}),
            )])
            .to_string(),
            delay: Duration::from_millis(120),
        }],
        Timeouts::default(),
    )
    .await;
    let id = h.start().await;
    let pending = h
        .client
        .post(format!("{}/agent/loop/resume", h.url))
        .json(&json!({"loop_id":id}));
    let request =
        tokio::spawn(async move { pending.send().await.unwrap().json::<Value>().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.ok("/agent/loop/cancel", json!({"loop_id":id})).await;
    let view = request.await.unwrap();
    assert_eq!(view["status"], "cancelled");
    assert_eq!(view["proposal_count"], 0);
    assert_eq!(view["late_model_arrivals_ms"].as_array().unwrap().len(), 1);
    assert_eq!(h.tools.executions.load(Ordering::SeqCst), 0);
    assert!(!h.audit().contains("cancelled-wire"));
}

#[tokio::test]
async fn configured_production_builder_enables_loop_and_keeps_chat_request_unchanged() {
    let mut h = Harness::new(vec![final_reply(), final_reply()]).await;
    let state = AppState::with_files_root_and_approvals(h.dir.join("files"), None)
        .with_deepseek_backends(h.config.clone(), Some(h.dir.join("production-loops.jsonl")))
        .unwrap()
        .with_auto_drive(false);
    let (url, server) = serve(app(state)).await;
    h.url = url;
    h.servers.push(server);
    let id = h.start().await;
    assert_eq!(h.step(&id).await["status"], "completed");
    h.ok(
        "/chat/stream/start",
        json!({"stream_id":"chat","turn_id":"turn","text":"hello"}),
    )
    .await;
    let requests = h.requests();
    assert!(requests[0]["tools"].is_array());
    assert_eq!(
        requests[1],
        json!({"model":"fake-model-override","messages":[{"role":"user","content":"hello"}],"stream":false})
    );
}

#[tokio::test]
async fn no_key_builder_keeps_loop_503_and_chat_echo() {
    let mut h = Harness::new(vec![]).await;
    let state = AppState::with_files_root_and_approvals(h.dir.join("files"), None)
        .with_deepseek_backends(DeepSeekConfig::disabled(), None)
        .unwrap();
    let (url, server) = serve(app(state)).await;
    h.url = url;
    h.servers.push(server);
    assert_eq!(
        h.post("/agent/loop/start", json!({"prompt":"task"}))
            .await
            .0,
        503
    );
    let view = h
        .ok(
            "/chat/stream/start",
            json!({"stream_id":"chat","turn_id":"turn","text":"hello"}),
        )
        .await;
    assert!(view.to_string().contains("hello"));
    assert!(h.requests().is_empty());
}
