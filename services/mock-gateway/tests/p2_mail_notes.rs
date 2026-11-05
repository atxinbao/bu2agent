//! P2 tests: mail/notes — real read, cite, summarize, draft Artifact.
//! M1 whitelist/approval; M2 loop reads mock source; M3 4096-byte
//! truncation; M4 loop draft write (approval + Effect + dedup);
//! M5 direct save + verify; M6 draft drift mismatch + Inbox +
//! auto-close; M8 truncation honesty notice. M7 is the full suite
//! + clippy run itself, not a test here.
//!
//! Tests use MockMailSource seeds (and one long-body test source)
//! only; never the real Mail.app. Cross-platform: temp dirs,
//! ports, SQLite only. No kill command, no POSIX signals, no
//! Unix-only path spellings.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::loop_engine::*;
use mock_gateway::mail_source::{
    AttachmentData, MailError, MailSource, MailboxInfo, MessageView, MockMailSource,
    SetupInfo, ThreadView,
};
use mock_gateway::server::{app, AppState};
use mock_gateway::tool_requires_approval;
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p2-{}-{}-{}",
        tag,
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Base64 for "# draft one\nsummary text\n" (ASCII, no encoder needed).
const DRAFT_B64: &str = "IyBkcmFmdCBvbmUKc3VtbWFyeSB0ZXh0Cg==";
const DRAFT_TEXT: &str = "# draft one\nsummary text\n";

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
    contexts: Mutex<Vec<Vec<Message>>>,
    started: tokio::sync::Notify,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(
                replies
                    .into_iter()
                    .map(|r| (Duration::ZERO, Ok(r)))
                    .collect(),
            ),
            contexts: Mutex::new(vec![]),
            started: tokio::sync::Notify::new(),
        }
    }
}

impl LoopModel for ScriptModel {
    fn name(&self) -> &str {
        "scripted-test"
    }

    fn complete(
        &self,
        messages: Vec<Message>,
        _timeout: Duration,
    ) -> BackendFuture<Result<ModelReply, ModelError>> {
        self.contexts.lock().unwrap().push(messages);
        let (delay, reply) = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        self.started.notify_one();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            reply
        })
    }
}

/// Test-only source: mock threads, but message "long-1" carries a
/// 5000-byte body so the loop's 4096-byte tool-result bound bites.
struct LongBodyMailSource {
    long_body: String,
}

impl MailSource for LongBodyMailSource {
    fn mailbox(&self) -> Result<MailboxInfo, MailError> {
        MockMailSource.mailbox()
    }

    fn setup(&self) -> Result<SetupInfo, MailError> {
        MockMailSource.setup()
    }

    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<ThreadView>, MailError> {
        MockMailSource.threads(limit, offset)
    }

    fn thread(&self, thread_id: &str) -> Result<ThreadView, MailError> {
        MockMailSource.thread(thread_id)
    }

    fn message(&self, message_id: &str) -> Result<MessageView, MailError> {
        if message_id == "long-1" {
            Ok(MessageView {
                message_id: "long-1".into(),
                from: "long@mock.invalid".into(),
                to: vec!["me@mock.invalid".into()],
                date: "2026-11-02T10:00:00Z".into(),
                subject: "Long Body".into(),
                body_text: self.long_body.clone(),
                attachments: vec![],
            })
        } else {
            MockMailSource.message(message_id)
        }
    }

    fn attachment(&self, message_id: &str, index: usize) -> Result<AttachmentData, MailError> {
        MockMailSource.attachment(message_id, index)
    }
}

fn proposal(tool: &str, args: Value) -> Proposal {
    Proposal {
        tool: tool.into(),
        args,
        wire: None,
    }
}

fn calls(items: Vec<Proposal>) -> ModelReply {
    ModelReply::Calls {
        proposals: items,
        content: None,
    }
}

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

fn read_threads_proposal() -> Proposal {
    proposal("email.threads", json!({}))
}

fn read_message_proposal(id: &str) -> Proposal {
    proposal("email.message", json!({"id": id}))
}

fn write_proposal(path: &str, text: &str) -> Proposal {
    proposal("fs.write", json!({"path": path, "text": text}))
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    model: Arc<ScriptModel>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>, mail: Arc<dyn MailSource>) -> Self {
        let dir = temp_dir("harness");
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let state = AppState::with_files_root_and_approvals(
            root.clone(),
            Some(dir.join("approvals.jsonl")),
        )
        .with_loop_backends(
            Some(dir.join("loops.jsonl")),
            Some(model.clone()),
            None,
            Timeouts::default(),
        )
        .unwrap()
        .with_auto_drive(false)
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_mail_source(mail);
        let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
        state.sqlite_import_and_switch().expect("switch");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server =
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
            model,
        }
    }

    fn files_root(&self) -> PathBuf {
        self.dir.join("files")
    }

    async fn post(&self, path: &str, value: Value) -> (u16, Value) {
        let response = self
            .client
            .post(format!("{}{}", self.url, path))
            .json(&value)
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let response = self
            .client
            .get(format!("{}{}", self.url, path))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn get_ok(&self, path: &str) -> Value {
        let (status, body) = self.get(path).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn start(&self) -> String {
        self.ok("/agent/loop/start", json!({"prompt": "summarize the mail"}))
            .await["loop_id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id": id})).await
    }

    /// Step until the loop reaches a terminal status (bounded).
    async fn drive(&self, id: &str) -> Value {
        let mut last = Value::Null;
        for _ in 0..16 {
            last = self.step(id).await;
            let status = last["status"].as_str().unwrap_or("");
            if matches!(status, "completed" | "failed" | "cancelled") {
                break;
            }
        }
        last
    }

    /// Tool results the scripted model has seen, deduped by call
    /// id (message history repeats them in later contexts).
    fn tool_messages(&self) -> Vec<(String, bool)> {
        let mut out: Vec<(usize, String, bool)> = Vec::new();
        for messages in self.model.contexts.lock().unwrap().iter() {
            for m in messages {
                if let Message::Tool {
                    call_id,
                    result,
                    ..
                } = m
                {
                    if !out.iter().any(|(id, _, _)| id == call_id) {
                        out.push((*call_id, result.body.clone(), result.summary.truncated));
                    }
                }
            }
        }
        out.into_iter()
            .map(|(_, body, truncated)| (body, truncated))
            .collect()
    }

    async fn effects(&self) -> Vec<Value> {
        self.get_ok("/effects/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn effect(&self, id: &str) -> Value {
        self.get_ok(&format!("/effects/get?effect_id={id}")).await
    }

    async fn verify(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/verify", json!({"effect_id": effect_id}))
            .await
    }

    async fn inbox(&self, query: &str) -> Vec<Value> {
        let (status, body) = self.get(&format!("/inbox/list{query}")).await;
        assert_eq!(status, 200, "{body}");
        body.as_array().unwrap().clone()
    }

    async fn decide(&self, id: &str, status: &str) {
        self.ok(
            "/approval/decide",
            json!({"tool_call_id": id, "status": status}),
        )
        .await;
    }

    async fn request_approval(&self, tool: &str, extra: Value) -> String {
        let mut req = json!({"tool": tool});
        if let Value::Object(map) = extra {
            for (k, v) in map {
                req[k] = v;
            }
        }
        let body = self.ok("/approval/request", req).await;
        body["tool_call_id"].as_str().unwrap().to_string()
    }

    /// Approved bound file write; returns the Effect id of the
    /// succeeded row (latest matching row). Copied from the P0-R
    /// harness: the direct /files/write path with an approval.
    async fn write(&self, path: &str, content_b64: &str, append: bool) -> String {
        let approval = self
            .request_approval(
                "fs.write",
                json!({"path": path, "content_base64": content_b64, "append": append}),
            )
            .await;
        self.decide(&approval, "approved").await;
        let (status, body) = self
            .post(
                "/files/write",
                json!({"path": path, "content_base64": content_b64, "append": append, "approval_id": approval}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        let effects = self.effects().await;
        effects
            .iter()
            .rev()
            .find(|e| {
                e["capability"] == "fs" && e["operation"] == "write" && e["scope"] == path
            })
            .expect("effect row")["effect_id"]
            .as_str()
            .unwrap()
            .to_string()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ---------------------------------------------------------------- M1

#[test]
fn m1_email_tools_whitelisted_read_only() {
    assert_eq!(tool_requires_approval("email.threads"), Some(false));
    assert_eq!(tool_requires_approval("email.message"), Some(false));
    // Mock mail tools keep their existing registrations; unknown
    // mail tools are still unknown.
    assert_eq!(tool_requires_approval("mail.send"), Some(true));
    assert_eq!(tool_requires_approval("mail.list"), Some(false));
    assert_eq!(tool_requires_approval("email.unknown"), None);
    assert!(!loop_tool_requires_approval("email.threads"));
    assert!(!loop_tool_requires_approval("email.message"));

    // normalize: threads defaults, clamps, rejects extras.
    let norm = normalize("email.threads", &json!({})).unwrap();
    assert_eq!(norm.get("limit").map(String::as_str), Some("50"));
    assert_eq!(norm.get("offset").map(String::as_str), Some("0"));
    assert_eq!(
        normalize("email.threads", &json!({"limit": 10}))
            .unwrap()
            .get("limit")
            .map(String::as_str),
        Some("10")
    );
    assert_eq!(
        normalize("email.threads", &json!({"limit": 500}))
            .unwrap()
            .get("limit")
            .map(String::as_str),
        Some("200")
    );
    assert_eq!(
        normalize("email.threads", &json!({"offset": 3}))
            .unwrap()
            .get("offset")
            .map(String::as_str),
        Some("3")
    );
    assert!(normalize("email.threads", &json!({"limit": "50"})).is_err());
    assert!(normalize("email.threads", &json!({"bogus": 1})).is_err());
    // normalize: message id rules mirror reminders.set_completed.
    assert_eq!(
        normalize("email.message", &json!({"id": "msg-seed-1"}))
            .unwrap()
            .get("id")
            .map(String::as_str),
        Some("msg-seed-1")
    );
    assert!(normalize("email.message", &json!({})).is_err());
    assert!(normalize("email.message", &json!({"id": ""})).is_err());
    assert!(normalize("email.message", &json!({"id": "x", "extra": 1})).is_err());
    assert!(normalize("mail.send", &json!({})).is_err());
    // Digest: stable per args, sensitive to the message id, and
    // computed from args only (never the body).
    let a = args_digest(
        "email.message",
        &normalize("email.message", &json!({"id": "msg-seed-1"})).unwrap(),
    );
    let b = args_digest(
        "email.message",
        &normalize("email.message", &json!({"id": "msg-seed-1"})).unwrap(),
    );
    let c = args_digest(
        "email.message",
        &normalize("email.message", &json!({"id": "msg-seed-2"})).unwrap(),
    );
    assert_eq!(a, b);
    assert_ne!(a, c);
}

// ---------------------------------------------------------------- M2

#[tokio::test]
async fn m2_loop_reads_mock_source_like_routes() {
    let h = Harness::serve(
        vec![
            calls(vec![
                read_threads_proposal(),
                read_message_proposal("msg-seed-1"),
            ]),
            done(),
        ],
        Arc::new(MockMailSource),
    )
    .await;
    let loop_id = h.start().await;
    let final_view = h.drive(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    let seen = h.tool_messages();
    assert_eq!(seen.len(), 2, "{seen:?}");
    let (threads_body, threads_truncated) = &seen[0];
    assert!(!threads_truncated);
    assert!(threads_body.contains("thread-"), "{threads_body}");
    assert!(threads_body.contains("Mock Thread Alpha"), "{threads_body}");
    assert!(threads_body.contains("msg-seed-1"), "{threads_body}");
    let (message_body, message_truncated) = &seen[1];
    assert!(!message_truncated);
    assert!(message_body.contains("message: msg-seed-1"), "{message_body}");
    assert!(message_body.contains("Mock Thread Alpha"), "{message_body}");
    assert!(
        message_body.contains("Hello Alpha, this is the first mock message."),
        "{message_body}"
    );
    assert!(message_body.contains("alice@mock.invalid"), "{message_body}");
    // Same facts via the HTTP routes.
    let threads = h.get_ok("/api/email/threads?limit=50").await;
    let ids: Vec<&str> = threads
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["thread_id"].as_str().unwrap())
        .collect();
    assert!(
        ids.iter().any(|id| threads_body.contains(id)),
        "{threads_body} vs {ids:?}"
    );
    let message = h.get_ok("/api/email/messages/msg-seed-1").await;
    assert_eq!(message["subject"], "Mock Thread Alpha");
    assert_eq!(
        message["body_text"],
        "Hello Alpha, this is the first mock message."
    );
    // Reads produce no Effect.
    assert!(h.effects().await.is_empty());
}

// ---------------------------------------------------------------- M3

#[tokio::test]
async fn m3_long_message_body_truncates_at_4096() {
    let long_body = "x".repeat(5000);
    let h = Harness::serve(
        vec![calls(vec![read_message_proposal("long-1")]), done()],
        Arc::new(LongBodyMailSource {
            long_body: long_body.clone(),
        }),
    )
    .await;
    let loop_id = h.start().await;
    let final_view = h.drive(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    let seen = h.tool_messages();
    assert_eq!(seen.len(), 1, "{seen:?}");
    let (body, truncated) = &seen[0];
    assert!(*truncated);
    assert_eq!(body.len(), 4096);
    assert!(body.contains("message: long-1"), "{body}");
}

// ---------------------------------------------------------------- M4

#[tokio::test]
async fn m4_loop_draft_write_approval_effect_dedup() {
    let draft_text = "# 草稿\n摘要：Mock Thread Alpha。\n引用：message:msg-seed-1\n";
    let h = Harness::serve(
        vec![
            calls(vec![
                read_message_proposal("msg-seed-1"),
                write_proposal("drafts/draft-1.md", draft_text),
                write_proposal("drafts/draft-1.md", draft_text),
            ]),
            done(),
        ],
        Arc::new(MockMailSource),
    )
    .await;
    let loop_id = h.start().await;
    h.step(&loop_id).await;
    let mut waiting = Value::Null;
    for _ in 0..8 {
        waiting = h.step(&loop_id).await;
        if waiting["status"] == "awaiting_approval" {
            break;
        }
    }
    assert_eq!(waiting["status"], "awaiting_approval");
    let queue = waiting["queue_position"].as_u64().unwrap() as usize;
    let call = &waiting["calls"][queue];
    let binding = call["binding"].clone();
    let approval = call["approval_id"].as_str().unwrap().to_string();

    // The read ran first (no approval, no Effect); the write is
    // proposed with a planned Effect bound to the approval.
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["status"], "planned");
    assert_eq!(effects[0]["capability"], "fs");
    assert_eq!(effects[0]["operation"], "write");
    assert_eq!(effects[0]["scope"], "drafts/draft-1.md");
    assert_eq!(effects[0]["approval_id"], approval);

    h.decide(&approval, "approved").await;
    let dispatched = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop_id, "approval_id": approval, "binding": binding}),
        )
        .await;
    assert_eq!(dispatched["calls"][queue]["status"], "succeeded");
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("drafts/draft-1.md")).unwrap(),
        draft_text
    );
    let effects = h.effects().await;
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0]["status"], "succeeded");

    // The duplicate proposal is delivered without a dispatch.
    let dup = h.step(&loop_id).await;
    assert_eq!(dup["calls"][queue + 1]["status"], "duplicate");
    let final_view = h.drive(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    assert_eq!(h.effects().await.len(), 1, "one write Effect only");
}

// ---------------------------------------------------------------- M5

#[tokio::test]
async fn m5_direct_draft_save_verifies() {
    let h = Harness::serve(vec![], Arc::new(MockMailSource)).await;
    let effect_id = h.write("drafts/draft-1.md", DRAFT_B64, false).await;
    let effect = h.effect(&effect_id).await;
    assert_eq!(effect["capability"], "fs");
    assert_eq!(effect["operation"], "write");
    assert_eq!(effect["scope"], "drafts/draft-1.md");
    assert_eq!(effect["status"], "succeeded");
    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["reason"], "hash_match");
    // The draft file is visible in the file listing too (same
    // fact in FilesView as in NotesView).
    let listed = h.get_ok("/files/list?path=drafts").await;
    assert!(
        listed
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["path"] == "drafts/draft-1.md"),
        "{listed}"
    );
}

// ---------------------------------------------------------------- M6

#[tokio::test]
async fn m6_draft_drift_mismatch_inbox_then_verified_auto_closes() {
    let h = Harness::serve(vec![], Arc::new(MockMailSource)).await;
    let effect_id = h.write("drafts/draft-1.md", DRAFT_B64, false).await;

    // Drift the draft outside the gateway.
    std::fs::write(
        h.files_root().join("drafts/draft-1.md"),
        "# draft one\nCHANGED\n",
    )
    .unwrap();
    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "mismatch");
    assert_eq!(row["reason"], "hash_mismatch");

    // A verification_mismatch Inbox row opens.
    let open = h
        .inbox("?kind=verification_mismatch&open_only=true")
        .await;
    assert_eq!(open.len(), 1, "{open:?}");
    assert_eq!(open[0]["effect_id"], effect_id);

    // Restore the draft: verified, and the Inbox row auto-closes.
    std::fs::write(h.files_root().join("drafts/draft-1.md"), DRAFT_TEXT).unwrap();
    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(
        h.inbox("?kind=verification_mismatch&open_only=true")
            .await
            .len(),
        0
    );
}

// ---------------------------------------------------------------- M8

#[tokio::test]
async fn m8_truncation_notice_present_only_when_truncated() {
    // Truncated case: long body carries the honesty notice.
    let long_body = "x".repeat(5000);
    let h = Harness::serve(
        vec![calls(vec![read_message_proposal("long-1")]), done()],
        Arc::new(LongBodyMailSource { long_body }),
    )
    .await;
    let loop_id = h.start().await;
    let final_view = h.drive(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    let seen = h.tool_messages();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].0.contains("基于已读部分"), "{}", seen[0].0);
    assert!(seen[0].1);

    // Untruncated case: short seed body carries no notice.
    let h2 = Harness::serve(
        vec![calls(vec![read_message_proposal("msg-seed-1")]), done()],
        Arc::new(MockMailSource),
    )
    .await;
    let loop_id2 = h2.start().await;
    let final_view2 = h2.drive(&loop_id2).await;
    assert_eq!(final_view2["status"], "completed");
    let seen2 = h2.tool_messages();
    assert_eq!(seen2.len(), 1, "{seen2:?}");
    assert!(!seen2[0].0.contains("基于已读部分"), "{}", seen2[0].0);
    assert!(!seen2[0].1);
}
