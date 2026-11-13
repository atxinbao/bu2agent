//! D1-6b-1 tests (plan §2): real mail send end to end —
//! digest-bound approval + write Execution + single-tool
//! Mandate + `sent_messages` fact row + `mail.sent`
//! event + Effect, idempotency, approval gates,
//! redaction, and error mapping.
//!
//! HTTP routes only, plus `sqlite_store` reads (sent
//! rows / effects / mandates / events / inbox) where
//! HTTP cannot reach the state. The mail source is a
//! test-only fake (no AppleScript, no real Mail.app):
//! it records sends and can be switched into the
//! not-running / denied / timeout / unavailable failure
//! modes. "Sent" means Mail.app accepted the message,
//! never that it was delivered (Q-D1-6b-06) — no test
//! here asserts delivery.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::mail_send::{
    get_sent_message, list_sent_messages, mail_body_hash, normalize_recipients, SentMessageRow,
};
use mock_gateway::mail_source::{
    AttachmentData, MailError, MailSource, MailboxInfo, MessageView, SentMessage, SetupInfo,
    ThreadView,
};
use mock_gateway::operation_binding::{mail_send_digest, sha256_hex};
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-d1-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FakeMode {
    Ok,
    NotRunning,
    Denied,
    Timeout,
    Unavailable,
}

/// Test-only fake mail source: records every send and
/// answers the failure modes on demand. The production
/// AppleScript source is never exercised here (no
/// Mail.app in the VM); the script form itself is
/// guarded by the `send_script` unit test in
/// `mail_source.rs`, and window validation on a real Mac
/// is the proof of the real send.
struct FakeMailSource {
    mode: Mutex<FakeMode>,
    send_count: AtomicUsize,
    sent: Mutex<Vec<(Vec<String>, String, String)>>,
}

impl FakeMailSource {
    fn new() -> Self {
        Self {
            mode: Mutex::new(FakeMode::Ok),
            send_count: AtomicUsize::new(0),
            sent: Mutex::new(Vec::new()),
        }
    }

    fn set_mode(&self, mode: FakeMode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn send_count(&self) -> usize {
        self.send_count.load(Ordering::SeqCst)
    }

    fn sent(&self) -> Vec<(Vec<String>, String, String)> {
        self.sent.lock().unwrap().clone()
    }
}

impl MailSource for FakeMailSource {
    fn mailbox(&self) -> Result<MailboxInfo, MailError> {
        Ok(MailboxInfo {
            configured: true,
            address: Some("test@example.invalid".to_string()),
            name: Some("Test".to_string()),
            source: "test-fake".to_string(),
        })
    }

    fn setup(&self) -> Result<SetupInfo, MailError> {
        Err(MailError::Unavailable(
            "mail unavailable in tests".to_string(),
        ))
    }

    fn threads(&self, _limit: usize, _offset: usize) -> Result<Vec<ThreadView>, MailError> {
        Ok(vec![])
    }

    fn thread(&self, _thread_id: &str) -> Result<ThreadView, MailError> {
        Err(MailError::NotFound("thread"))
    }

    fn message(&self, _message_id: &str) -> Result<MessageView, MailError> {
        Err(MailError::NotFound("message"))
    }

    fn attachment(&self, _message_id: &str, _index: usize) -> Result<AttachmentData, MailError> {
        Err(MailError::NotFound("attachment"))
    }

    fn send(&self, to: &[String], subject: &str, body: &str) -> Result<SentMessage, MailError> {
        match *self.mode.lock().unwrap() {
            FakeMode::Ok => {
                self.send_count.fetch_add(1, Ordering::SeqCst);
                self.sent.lock().unwrap().push((
                    to.to_vec(),
                    subject.to_string(),
                    body.to_string(),
                ));
                Ok(SentMessage {
                    native_id: Some("4242".to_string()),
                })
            }
            FakeMode::NotRunning => Err(MailError::NotRunning),
            FakeMode::Denied => Err(MailError::Denied),
            FakeMode::Timeout => Err(MailError::Timeout),
            FakeMode::Unavailable => Err(MailError::Unavailable(
                "mail source unavailable in tests".to_string(),
            )),
        }
    }
}

fn build_state(dir: &Path, mail: Arc<dyn MailSource>) -> AppState {
    let state = AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_tasks_path(dir.join("tasks.jsonl"))
    .with_task_drafts_dir(dir.join("drafts"))
    .with_deepseek(mock_gateway::deepseek::DeepSeekClient::new(
        mock_gateway::deepseek::DeepSeekConfig {
            base_url: "http://127.0.0.1:1".to_string(),
            model: "fixture".to_string(),
            api_key: Some("fake-d1-key".to_string()),
        },
    ))
    .with_auto_drive(false)
    .with_executions_path(dir.join("executions.jsonl"))
    .with_effects_path(dir.join("effects.jsonl"))
    .with_evidence_path(dir.join("evidence.jsonl"))
    .with_binding_state_path(dir.join("bindings.jsonl"))
    .with_project_state_path(dir.join("projects.jsonl"))
    .with_location_state_path(dir.join("locations.jsonl"))
    .with_worktree_state_path(dir.join("worktrees.jsonl"))
    .with_mail_source(mail);
    let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    state
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    // Kept alive for the test's lifetime (dropping the
    // handle detaches the server task; the field exists
    // so the task is tracked, not read).
    #[allow(dead_code)]
    server: Option<tokio::task::JoinHandle<()>>,
    mail: Arc<FakeMailSource>,
}

impl Harness {
    async fn serve() -> Self {
        let dir = temp_dir("mail-send");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let mail = Arc::new(FakeMailSource::new());
        let state = build_state(&dir, mail.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap(),
            server: Some(server),
            mail,
        }
    }

    fn db(&self) -> rusqlite::Connection {
        let conn = sqlite_store::open_db(&self.dir.join("gateway.sqlite3")).expect("open db");
        conn.busy_timeout(Duration::from_secs(5))
            .expect("busy timeout");
        conn
    }

    fn schema_version(&self) -> i64 {
        let conn = self.db();
        let raw: String = conn
            .query_row(
                "SELECT value FROM db_meta WHERE key = 'db_schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        raw.parse().unwrap()
    }

    fn sent_rows(&self) -> Vec<SentMessageRow> {
        let conn = self.db();
        list_sent_messages(&conn).expect("list sent")
    }

    fn mail_effects(&self) -> Vec<mock_gateway::effect_store::EffectFileRecord> {
        let conn = self.db();
        sqlite_store::list_effects(&conn)
            .expect("effects")
            .into_iter()
            .filter(|e| e.capability == "mail")
            .collect()
    }

    fn mail_events(&self) -> Vec<sqlite_store::EventRow> {
        let conn = self.db();
        sqlite_store::list_events(&conn)
            .expect("events")
            .into_iter()
            .filter(|e| e.event_type == "mail.sent")
            .collect()
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

    /// Request a digest-bound `mail.send` approval and
    /// decide it approved. Returns the tool_call_id.
    async fn approved_mail_send(&self, to: &[&str], subject: &str, body: &str) -> String {
        let (status, requested) = self
            .post(
                "/approval/request",
                json!({"tool": "mail.send", "to": to, "subject": subject, "body": body}),
            )
            .await;
        assert_eq!(status, 200, "approval request: {requested}");
        assert_eq!(requested["requires_approval"], true);
        let id = requested["tool_call_id"].as_str().unwrap().to_string();
        let (status, decided) = self
            .post(
                "/approval/decide",
                json!({"tool_call_id": id, "status": "approved"}),
            )
            .await;
        assert_eq!(status, 200, "approval decide: {decided}");
        id
    }

    /// Request a `mail.send` approval and decide it
    /// rejected. Returns the tool_call_id.
    async fn rejected_mail_send(&self, to: &[&str], subject: &str, body: &str) -> String {
        let (status, requested) = self
            .post(
                "/approval/request",
                json!({"tool": "mail.send", "to": to, "subject": subject, "body": body}),
            )
            .await;
        assert_eq!(status, 200, "approval request: {requested}");
        let id = requested["tool_call_id"].as_str().unwrap().to_string();
        let (status, decided) = self
            .post(
                "/approval/decide",
                json!({"tool_call_id": id, "status": "rejected"}),
            )
            .await;
        assert_eq!(status, 200, "approval decide: {decided}");
        id
    }

    async fn send(&self, value: Value) -> (u16, Value) {
        self.post("/api/email/send", value).await
    }

    fn send_body(
        to: &[&str],
        subject: &str,
        body: &str,
        key: &str,
        tool_call_id: Option<&str>,
    ) -> Value {
        let mut value = json!({
            "to": to,
            "subject": subject,
            "body": body,
            "idempotency_key": key,
        });
        if let Some(id) = tool_call_id {
            value["tool_call_id"] = json!(id);
        }
        value
    }
}

const TO: [&str; 2] = ["b@example.invalid", "a@example.invalid"];
const SUBJECT: &str = "Hello SD1";
const BODY: &str = "Body of SD1.";

/// SD1: send success end to end — fake send + decided
/// digest-bound approval → 200, `sent-N` row +
/// `mail.sent` event + Effect succeeded; `body_hash`
/// == sha256(body); the body is in neither the table
/// nor the event.
#[tokio::test]
async fn sd1_send_end_to_end() {
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    assert_eq!(DB_SCHEMA_VERSION, 12);

    let approval = h.approved_mail_send(&TO, SUBJECT, BODY).await;
    // The send presents the recipients in the other
    // order: the digest normalizes (dedup + sort) on
    // both sides, so it still matches the approval.
    let (status, sent) = h
        .send(Harness::send_body(
            &["a@example.invalid", "b@example.invalid"],
            SUBJECT,
            BODY,
            "key-sd1",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "send: {sent}");
    assert_eq!(sent["sent_id"], "sent-1");
    assert_eq!(sent["source_id"], "mail-source");
    assert_eq!(sent["native_id"], "4242");
    assert_eq!(
        sent["to_json"],
        "[\"a@example.invalid\",\"b@example.invalid\"]"
    );
    assert_eq!(sent["subject"], SUBJECT);
    assert_eq!(sent["body_hash"], sha256_hex(BODY.as_bytes()));
    assert_eq!(sent["replayed"], false);
    assert!(!sent["policy_version"].as_str().unwrap().is_empty());
    let effect_id = sent["effect_id"].as_str().unwrap().to_string();
    assert!(effect_id.starts_with("effect-"));

    // The fake saw exactly one send, with the
    // normalized recipient list.
    assert_eq!(h.mail.send_count(), 1);
    assert_eq!(
        h.mail.sent(),
        vec![(
            vec![
                "a@example.invalid".to_string(),
                "b@example.invalid".to_string()
            ],
            SUBJECT.to_string(),
            BODY.to_string()
        )]
    );

    // The fact row.
    let rows = h.sent_rows();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.sent_id, "sent-1");
    assert_eq!(row.source_id, "mail-source");
    assert_eq!(row.native_id.as_deref(), Some("4242"));
    assert_eq!(row.idempotency_key, "key-sd1");
    assert_eq!(row.to_json, "[\"a@example.invalid\",\"b@example.invalid\"]");
    assert_eq!(row.subject, SUBJECT);
    assert_eq!(row.body_hash, mail_body_hash(BODY));
    assert_eq!(row.effect_id, effect_id);
    assert!(row.sent_ms > 0);
    assert!(!row.policy_version.is_empty());
    // The body itself is in no column: the row has no
    // body field at all.
    let row_json = serde_json::to_string(row).unwrap();
    assert!(!row_json.contains(BODY), "row leaks body: {row_json}");

    // The Effect: ("mail", "send", idempotency_key),
    // succeeded, bound to the approval and the write
    // Execution, digest-bound to the exact message.
    let effects = h.mail_effects();
    assert_eq!(effects.len(), 1);
    let effect = &effects[0];
    assert_eq!(effect.effect_id, effect_id);
    assert_eq!(effect.capability, "mail");
    assert_eq!(effect.operation, "send");
    assert_eq!(effect.scope, "key-sd1");
    assert_eq!(effect.status, "succeeded");
    assert_eq!(effect.approval_id.as_deref(), Some(approval.as_str()));
    let execution_id = effect.execution_id.clone().expect("write execution");
    let expected_digest = mail_send_digest(
        &normalize_recipients(&TO.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
        SUBJECT,
        &mail_body_hash(BODY),
    );
    assert_eq!(
        effect.input_digest.as_deref(),
        Some(expected_digest.as_str())
    );

    // The Mandate (Q-D1-6b-23): one row on the write
    // Execution, tools = the single write tool,
    // goal empty.
    let conn = h.db();
    let mandates = sqlite_store::list_mandates(&conn, None, Some(&execution_id)).expect("mandates");
    assert_eq!(mandates.len(), 1, "one write mandate");
    let allowed: Value = serde_json::from_str(&mandates[0].allowed_json).expect("allowed_json");
    assert_eq!(allowed["tools"], json!(["mail.send"]));
    assert_eq!(allowed["goal_id"], Value::Null);
    assert_eq!(allowed["entry"], "direct-write");
    assert_eq!(mandates[0].policy_version_id, row.policy_version);
    drop(conn);

    // The event: exactly one `mail.sent`, payload only
    // the whitelisted fields.
    let events = h.mail_events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.aggregate_kind, "mail");
    assert_eq!(event.aggregate_id, "sent-1");
    let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
    let keys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    for kept in [
        "sent_id",
        "source_id",
        "native_id",
        "body_hash",
        "subject_len",
        "recipient_count",
        "recipient_hash",
        "date_ms",
        "created_ms",
        "execution_id",
    ] {
        assert!(keys.contains(&kept), "missing {kept} in {payload}");
    }
    assert_eq!(keys.len(), 10, "no extra fields: {payload}");
    assert_eq!(payload["sent_id"], "sent-1");
    assert_eq!(payload["native_id"], "4242");
    assert_eq!(payload["body_hash"], sha256_hex(BODY.as_bytes()));
    assert_eq!(payload["subject_len"], SUBJECT.chars().count());
    assert_eq!(payload["recipient_count"], 2);
    assert_eq!(
        payload["recipient_hash"],
        sha256_hex("[\"a@example.invalid\",\"b@example.invalid\"]".as_bytes())
    );
    assert_eq!(payload["date_ms"], row.sent_ms);
    assert_eq!(payload["created_ms"], row.created_ms);
    assert_eq!(payload["execution_id"], execution_id);
    assert!(!event.payload_json.contains(BODY), "event leaks body");
    assert!(!event.payload_json.contains(SUBJECT), "event leaks subject");

    // The readers see the row.
    let (status, listed) = h.get("/api/email/sent").await;
    assert_eq!(status, 200);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["sent_id"], "sent-1");
    let (status, detail) = h.get("/api/email/sent/sent-1").await;
    assert_eq!(status, 200);
    assert_eq!(detail["sent_id"], "sent-1");
    let (status, missing) = h.get("/api/email/sent/sent-99").await;
    assert_eq!(status, 404, "unknown sent id: {missing}");

    // No Evidence row: there is no readback in 6b-1,
    // so verify stays 409 `effect_no_readback` — never
    // faked. (The Effect row itself is the audit trail.)
    let (status, verify) = h
        .post("/effects/verify", json!({"effect_id": effect_id}))
        .await;
    assert_eq!(status, 409, "verify without readback: {verify}");
    assert_eq!(verify["error"], "effect_no_readback");

    let conn = h.db();
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// SD2: idempotency — same-key replay returns the
/// original row (no second send, no second Effect);
/// same key + different content is a 409; no key is a
/// 400; empty fields are 400s.
#[tokio::test]
async fn sd2_idempotency() {
    let h = Harness::serve().await;
    let approval = h.approved_mail_send(&TO, SUBJECT, BODY).await;
    let (status, first) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd2",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "first send: {first}");
    assert_eq!(first["sent_id"], "sent-1");
    assert_eq!(h.mail.send_count(), 1);

    // Replay: same key + same content, no approval at
    // all — the replay check runs before the approval
    // gate, so a dropped retry needs no new approval.
    let (status, replay) = h
        .send(Harness::send_body(&TO, SUBJECT, BODY, "key-sd2", None))
        .await;
    assert_eq!(status, 200, "replay: {replay}");
    assert_eq!(replay["sent_id"], "sent-1");
    assert_eq!(replay["replayed"], true);
    assert_eq!(h.mail.send_count(), 1, "no second send");
    assert_eq!(h.mail_effects().len(), 1, "no second Effect");
    assert_eq!(h.sent_rows().len(), 1, "no second row");
    assert_eq!(h.mail_events().len(), 1, "no second event");

    // Same key + different content → 409 (a fresh
    // approval for the new content does not save it).
    let other_approval = h.approved_mail_send(&TO, "Different subject", BODY).await;
    let (status, conflict) = h
        .send(Harness::send_body(
            &TO,
            "Different subject",
            BODY,
            "key-sd2",
            Some(&other_approval),
        ))
        .await;
    assert_eq!(status, 409, "key conflict: {conflict}");
    assert_eq!(conflict["error"], "idempotency_key_conflict");
    assert_eq!(h.mail.send_count(), 1);

    // No key → 400, both spellings (empty string and
    // omitted field).
    let (status, no_key) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "",
            Some(&other_approval),
        ))
        .await;
    assert_eq!(status, 400, "empty key: {no_key}");
    let (status, no_key) = h
        .send(json!({"to": TO, "subject": SUBJECT, "body": BODY, "tool_call_id": other_approval}))
        .await;
    assert_eq!(status, 400, "omitted key: {no_key}");

    // Empty fields → 400.
    for (to, subject, body) in [
        (vec![], SUBJECT, BODY),
        (TO.to_vec(), "", BODY),
        (TO.to_vec(), SUBJECT, ""),
    ] {
        let (status, bad) = h
            .send(Harness::send_body(
                &to,
                subject,
                body,
                "key-sd2-empty",
                Some(&other_approval),
            ))
            .await;
        assert_eq!(status, 400, "empty field: {bad}");
    }
    assert_eq!(h.mail.send_count(), 1);
}

/// SD3: approval gates — no approval 403, rejected
/// approval 403, digest mismatch 409 (approval
/// unconsumed, no orphaned planned row), consumed
/// approval replay 409.
#[tokio::test]
async fn sd3_approval_gates() {
    let h = Harness::serve().await;

    // No approval at all.
    let (status, body) = h
        .send(Harness::send_body(&TO, SUBJECT, BODY, "key-sd3-none", None))
        .await;
    assert_eq!(status, 403, "no approval: {body}");
    assert_eq!(body["error"], "approval_required");

    // Unknown tool_call_id.
    let (status, body) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd3-unknown",
            Some("approval-999999"),
        ))
        .await;
    assert_eq!(status, 403, "unknown approval: {body}");
    assert_eq!(body["error"], "approval_required");

    // Rejected approval.
    let rejected = h.rejected_mail_send(&TO, SUBJECT, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd3-rejected",
            Some(&rejected),
        ))
        .await;
    assert_eq!(status, 403, "rejected approval: {body}");
    assert_eq!(body["error"], "approval_rejected");

    // Digest mismatch: approval for content A, send
    // content B. 409, approval unconsumed, and no
    // orphaned planned row is left behind.
    let approval = h.approved_mail_send(&TO, SUBJECT, BODY).await;
    let (status, mismatch) = h
        .send(Harness::send_body(
            &TO,
            "A different subject",
            BODY,
            "key-sd3-mismatch",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "digest mismatch: {mismatch}");
    assert_eq!(mismatch["error"], "approval_digest_mismatch");
    assert_eq!(h.mail.send_count(), 0, "mismatch must not send");
    let planned: Vec<_> = h
        .mail_effects()
        .into_iter()
        .filter(|e| e.approval_id.as_deref() == Some(approval.as_str()) && e.status == "planned")
        .collect();
    assert!(
        planned.is_empty(),
        "mismatch left an orphaned planned row: {planned:?}"
    );
    // The approval is unconsumed: the correct content
    // still sends with the same approval id.
    let (status, sent) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd3-correct",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "retry with the same approval: {sent}");
    assert_eq!(h.mail.send_count(), 1);

    // Consumed-approval replay: the same approval id
    // with different content (and a new key) is a 409,
    // and no second Effect is planned for it.
    let (status, replay) = h
        .send(Harness::send_body(
            &TO,
            "Yet another subject",
            BODY,
            "key-sd3-consumed",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "consumed replay: {replay}");
    assert_eq!(replay["error"], "approval_already_consumed");
    assert_eq!(h.mail_effects().len(), 1, "no second Effect");
    assert_eq!(h.mail.send_count(), 1);
}

/// SD4: redaction — the `mail.sent` payload carries
/// only whitelisted fields; no recipient plaintext,
/// subject, or body anywhere in the event log; the fold
/// holds.
#[tokio::test]
async fn sd4_redaction() {
    let secret_body = "The launch code is hunter2 and the key is sk-live-TEST123.";
    let secret_subject = "Quarterly results (confidential)";
    let h = Harness::serve().await;
    let approval = h
        .approved_mail_send(&["boss@example.invalid"], secret_subject, secret_body)
        .await;
    let (status, sent) = h
        .send(Harness::send_body(
            &["boss@example.invalid"],
            secret_subject,
            secret_body,
            "key-sd4",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "send: {sent}");

    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "mail.sent")
            .count(),
        1
    );
    for event in &events {
        for forbidden in [
            "hunter2",
            "sk-live-TEST123",
            "Quarterly results (confidential)",
            "boss@example.invalid",
            "The launch code is",
        ] {
            assert!(
                !event.payload_json.contains(forbidden),
                "event {} leaks {forbidden}: {}",
                event.event_type,
                event.payload_json
            );
        }
    }
    let sent_event = events.iter().find(|e| e.event_type == "mail.sent").unwrap();
    let payload: Value = serde_json::from_str(&sent_event.payload_json).unwrap();
    let keys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert_eq!(keys.len(), 10, "no extra fields: {payload}");
    assert_eq!(payload["body_hash"], sha256_hex(secret_body.as_bytes()));
    assert_eq!(payload["subject_len"], secret_subject.chars().count());
    assert_eq!(payload["recipient_count"], 1);
    // The body is in no row either (only its hash).
    let row = get_sent_message(&conn, "sent-1")
        .expect("get")
        .expect("row");
    let row_json = serde_json::to_string(&row).unwrap();
    assert!(!row_json.contains("hunter2"), "row leaks body: {row_json}");
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// SD5: error mapping — Mail not running 502 (Effect
/// failed), Automation denied 403 (Effect failed),
/// script timeout 502 (Effect unknown + Inbox row),
/// non-macOS/unavailable 501 (Effect failed).
#[tokio::test]
async fn sd5_error_mapping() {
    let h = Harness::serve().await;

    // Mail not running → 502, Effect failed, no row.
    h.mail.set_mode(FakeMode::NotRunning);
    let approval = h.approved_mail_send(&TO, SUBJECT, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd5-not-running",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 502, "not running: {body}");
    assert_eq!(body["error"], "Mail is not running");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.mail.send_count(), 0);
    assert!(h.sent_rows().is_empty());
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Automation denied → 403, Effect failed.
    h.mail.set_mode(FakeMode::Denied);
    let approval = h.approved_mail_send(&TO, SUBJECT, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd5-denied",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 403, "denied: {body}");
    assert_eq!(body["error"], "mail automation denied");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Script timeout → 502, Effect unknown + an
    // `unknown_requires_reconciliation` Inbox row. The
    // send may or may not have happened: no row, no
    // guess.
    h.mail.set_mode(FakeMode::Timeout);
    let approval = h.approved_mail_send(&TO, SUBJECT, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd5-timeout",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 502, "timeout: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "unknown");
    assert!(h.sent_rows().is_empty(), "timeout must not record a row");
    let conn = h.db();
    let inbox = sqlite_store::list_inbox(&conn, Some("unknown_requires_reconciliation"), true)
        .expect("inbox");
    assert!(
        inbox
            .iter()
            .any(|row| row.effect_id.as_deref() == Some(effect_id.as_str())),
        "no unknown_requires_reconciliation row for {effect_id}: {inbox:?}"
    );
    drop(conn);

    // Unavailable (the non-macOS production source) →
    // 501, Effect failed.
    h.mail.set_mode(FakeMode::Unavailable);
    let approval = h.approved_mail_send(&TO, SUBJECT, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd5-unavailable",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 501, "unavailable: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");
}

/// Q-D1-6b-24: a disconnected mail-source rejects new
/// writes with 409 `source_disconnected`; reads are
/// unaffected.
#[tokio::test]
async fn sd_disconnected_source_rejects_writes() {
    let h = Harness::serve().await;
    let (status, disconnected) = h.post("/sources/mail-source/disconnect", json!({})).await;
    assert_eq!(status, 200, "disconnect: {disconnected}");
    assert_eq!(disconnected["status"], "disconnected");

    let approval = h.approved_mail_send(&TO, SUBJECT, BODY).await;
    let (status, body) = h
        .send(Harness::send_body(
            &TO,
            SUBJECT,
            BODY,
            "key-sd-disconnected",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "disconnected write: {body}");
    assert_eq!(body["error"], "source_disconnected");
    assert_eq!(h.mail.send_count(), 0, "disconnected must not send");
    assert!(h.mail_effects().is_empty(), "no Effect for a refused write");

    // Reads are unaffected by the disconnect.
    let (status, mailbox) = h.get("/api/email/mailbox").await;
    assert_eq!(status, 200, "mailbox read: {mailbox}");
}
