//! D1-6b-3 tests (plan §3): real mail delete end to
//! end — digest-bound approval + write Execution +
//! single-tool Mandate + tombstone + `mail.deleted`
//! event + Effect, re-delete idempotency, approval /
//! digest gates, redaction, and error mapping.
//!
//! HTTP routes only, plus `sqlite_store` reads (synced
//! rows / effects / mandates / events / inbox) where
//! HTTP cannot reach the state. The mail source is a
//! test-only fake (no AppleScript, no real Mail.app): it
//! keeps a seeded inbox + a trash pile, moves messages
//! inbox → trash on delete, and can be switched into
//! the not-running / denied / timeout / unavailable
//! failure modes. Delete means the fake accepted the
//! move to Trash — never "gone from every copy"
//! (Q-D1-6b-06 by analogy). The AppleScript form itself
//! is guarded by the `delete_script` unit test in
//! `mail_source.rs`, and window validation on a real Mac
//! is the proof of the real delete (in particular
//! whether `delete` means Trash or purge).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::mail_source::{
    group_threads, AttachmentData, MailError, MailSource, MailboxInfo, MessageView, RawMessageMeta,
    SentMessage, SetupInfo, ThreadView,
};
use mock_gateway::mail_sync::{get_mail_message, list_mail_messages, MailMessageRow};
use mock_gateway::operation_binding::sha256_hex;
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

/// Test-only fake mail source: a seeded inbox plus a
/// trash pile. `delete` moves inbox → trash (the Trash
/// contract, Q-D1-6b-12); a message already in trash or
/// never seen reads as NotFound. `message` reads the
/// inbox only (the production `find_meta` precedent).
struct FakeMailSource {
    mode: Mutex<FakeMode>,
    delete_count: AtomicUsize,
    inbox: Mutex<Vec<MessageView>>,
    trash: Mutex<Vec<MessageView>>,
}

impl FakeMailSource {
    fn new(messages: Vec<MessageView>) -> Self {
        Self {
            mode: Mutex::new(FakeMode::Ok),
            delete_count: AtomicUsize::new(0),
            inbox: Mutex::new(messages),
            trash: Mutex::new(Vec::new()),
        }
    }

    fn set_mode(&self, mode: FakeMode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn delete_count(&self) -> usize {
        self.delete_count.load(Ordering::SeqCst)
    }

    fn inbox_ids(&self) -> Vec<String> {
        self.inbox
            .lock()
            .unwrap()
            .iter()
            .map(|m| m.message_id.clone())
            .collect()
    }

    fn trash_ids(&self) -> Vec<String> {
        self.trash
            .lock()
            .unwrap()
            .iter()
            .map(|m| m.message_id.clone())
            .collect()
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
        let window = self.inbox.lock().unwrap().clone();
        let metas: Vec<RawMessageMeta> = window
            .iter()
            .map(|m| RawMessageMeta {
                id: m.message_id.clone(),
                subject: m.subject.clone(),
                sender: m.from.clone(),
                recipients: m.to.clone(),
                date: m.date.clone(),
            })
            .collect();
        let bodies: HashMap<String, String> = window
            .iter()
            .map(|m| (m.message_id.clone(), m.body_text.clone()))
            .collect();
        Ok(group_threads(&metas, &bodies))
    }

    fn thread(&self, thread_id: &str) -> Result<ThreadView, MailError> {
        self.threads(200, 0)?
            .into_iter()
            .find(|t| t.thread_id == thread_id)
            .ok_or(MailError::NotFound("thread"))
    }

    fn message(&self, message_id: &str) -> Result<MessageView, MailError> {
        self.inbox
            .lock()
            .unwrap()
            .iter()
            .find(|m| m.message_id == message_id)
            .cloned()
            .ok_or(MailError::NotFound("message"))
    }

    fn attachment(&self, _message_id: &str, _index: usize) -> Result<AttachmentData, MailError> {
        Err(MailError::NotFound("attachment"))
    }

    fn send(&self, to: &[String], subject: &str, body: &str) -> Result<SentMessage, MailError> {
        // The delete tests never send; the args are
        // ignored (the mock never really sends — the
        // 6b-1 `MockMailSource` precedent).
        let _ = (to, subject, body);
        match *self.mode.lock().unwrap() {
            FakeMode::Ok => Ok(SentMessage {
                native_id: Some("4242".to_string()),
            }),
            FakeMode::NotRunning => Err(MailError::NotRunning),
            FakeMode::Denied => Err(MailError::Denied),
            FakeMode::Timeout => Err(MailError::Timeout),
            FakeMode::Unavailable => Err(MailError::Unavailable(
                "mail source unavailable in tests".to_string(),
            )),
        }
    }

    fn delete(&self, message_id: &str) -> Result<(), MailError> {
        match *self.mode.lock().unwrap() {
            FakeMode::Ok => {}
            FakeMode::NotRunning => return Err(MailError::NotRunning),
            FakeMode::Denied => return Err(MailError::Denied),
            FakeMode::Timeout => return Err(MailError::Timeout),
            FakeMode::Unavailable => {
                return Err(MailError::Unavailable(
                    "mail source unavailable in tests".to_string(),
                ));
            }
        }
        let mut inbox = self.inbox.lock().unwrap();
        let Some(pos) = inbox.iter().position(|m| m.message_id == message_id) else {
            return Err(MailError::NotFound("message"));
        };
        let message = inbox.remove(pos);
        self.trash.lock().unwrap().push(message);
        self.delete_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn seed_message(id: &str, from: &str, subject: &str, body: &str) -> MessageView {
    MessageView {
        message_id: id.to_string(),
        from: from.to_string(),
        to: vec!["test@example.invalid".to_string()],
        date: "2026-11-13T10:00:00Z".to_string(),
        subject: subject.to_string(),
        body_text: body.to_string(),
        attachments: vec![],
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
        Self::serve_with(vec![
            seed_message(
                "1001",
                "alice@example.invalid",
                "Alpha launch (confidential)",
                "Alpha body hunter2",
            ),
            seed_message("1002", "bob@example.invalid", "Beta", "Beta body"),
        ])
        .await
    }

    async fn serve_with(messages: Vec<MessageView>) -> Self {
        let dir = temp_dir("mail-delete");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let mail = Arc::new(FakeMailSource::new(messages));
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

    /// Restart the gateway from the same directory (the
    /// MD reader/restart test): the DB, the fake inbox,
    /// and the tombstones all survive.
    async fn restart(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        let state = build_state(&self.dir, self.mail.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server = Some(tokio::spawn(async move {
            axum::serve(listener, app(state)).await.unwrap()
        }));
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

    fn synced_rows(&self) -> Vec<MailMessageRow> {
        let conn = self.db();
        list_mail_messages(&conn, mock_gateway::mail_sync::MAIL_SOURCE_ID).expect("list synced")
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
            .filter(|e| e.event_type == "mail.deleted")
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

    async fn delete(&self, path: &str) -> (u16, Value) {
        let response = self
            .client
            .delete(format!("{}{}", self.url, path))
            .send()
            .await
            .unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn sync(&self) -> (u16, Value) {
        self.post("/api/email/sync", json!({})).await
    }

    /// Request a digest-bound `mail.delete` approval and
    /// decide it approved. Returns the tool_call_id.
    async fn approved_mail_delete(&self, native_id: &str) -> String {
        let (status, requested) = self
            .post(
                "/approval/request",
                json!({"tool": "mail.delete", "native_id": native_id}),
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

    /// Request a `mail.delete` approval and decide it
    /// rejected. Returns the tool_call_id.
    async fn rejected_mail_delete(&self, native_id: &str) -> String {
        let (status, requested) = self
            .post(
                "/approval/request",
                json!({"tool": "mail.delete", "native_id": native_id}),
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

    async fn delete_message(&self, message_id: &str, tool_call_id: Option<&str>) -> (u16, Value) {
        let path = match tool_call_id {
            Some(id) => format!("/api/email/messages/{message_id}?tool_call_id={id}"),
            None => format!("/api/email/messages/{message_id}"),
        };
        self.delete(&path).await
    }

    async fn verify(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/verify", json!({"effect_id": effect_id}))
            .await
    }
}

/// MD1: delete success end to end — sync seeds two
/// rows, decided digest-bound approval → 200, the fake
/// moves inbox → trash, the row is tombstoned (text
/// retained), one `mail.deleted` event, Effect
/// succeeded, verify reads back absent → verified, and
/// a re-sync does not resurrect the row.
#[tokio::test]
async fn md1_delete_end_to_end() {
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    // 6b-3 adds no version bump: the schema stays v12.
    assert_eq!(DB_SCHEMA_VERSION, 12);

    let (status, synced) = h.sync().await;
    assert_eq!(status, 200, "sync: {synced}");
    assert_eq!(synced["synced_count"], 2);
    let rows = h.synced_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].message_id, "mail-1");
    assert_eq!(rows[0].native_id, "1001");
    assert_eq!(rows[1].message_id, "mail-2");
    assert_eq!(rows[1].native_id, "1002");

    let approval = h.approved_mail_delete("1001").await;
    let (status, deleted) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 200, "delete: {deleted}");
    assert_eq!(deleted["message_id"], "mail-1");
    assert_eq!(deleted["native_id"], "1001");
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["replayed"], false);
    let effect_id = deleted["effect_id"].as_str().unwrap().to_string();

    // The fake moved inbox → trash, exactly once.
    assert_eq!(h.mail.delete_count(), 1);
    assert_eq!(h.mail.inbox_ids(), vec!["1002"]);
    assert_eq!(h.mail.trash_ids(), vec!["1001"]);

    // The row is tombstoned, text retained, body hash
    // only.
    let conn = h.db();
    let row = get_mail_message(&conn, "mail-1")
        .expect("get")
        .expect("row");
    assert!(row.deleted);
    assert!(row.updated_ms > 0);
    assert_eq!(row.subject, "Alpha launch (confidential)");
    assert_eq!(row.body_hash, sha256_hex("Alpha body hunter2".as_bytes()));
    drop(conn);

    // The Mandate (Q-D1-6b-23): one row on the write
    // Execution, tools = the single write tool, goal
    // empty.
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "succeeded");
    assert_eq!(effect.operation, "delete");
    assert_eq!(effect.scope, "1001");
    let execution_id = effect.execution_id.clone().expect("execution");
    let conn = h.db();
    let mandates = sqlite_store::list_mandates(&conn, None, Some(&execution_id)).expect("mandates");
    assert_eq!(mandates.len(), 1, "one write mandate");
    let allowed: Value = serde_json::from_str(&mandates[0].allowed_json).expect("allowed_json");
    assert_eq!(allowed["tools"], json!(["mail.delete"]));
    assert_eq!(allowed["goal_id"], Value::Null);
    assert_eq!(allowed["entry"], "direct-write");
    drop(conn);

    // The event: exactly one `mail.deleted`, payload
    // only the whitelisted fields.
    let events = h.mail_events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.aggregate_kind, "mail");
    let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
    let keys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert_eq!(keys.len(), 6, "no extra fields: {payload}");
    assert_eq!(payload["message_id"], "mail-1");
    assert_eq!(payload["native_id"], "1001");
    assert_eq!(payload["execution_id"], execution_id);

    // Verify reads back: absent IS the success
    // condition for a delete — verified, target_absent.
    let (status, verified) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "verify: {verified}");
    assert_eq!(verified["verdict"], "verified");
    let conn = h.db();
    let evidence = sqlite_store::list_evidence(&conn).expect("evidence");
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].verdict, "verified");
    assert_eq!(evidence[0].reason, "target_absent");
    drop(conn);

    // Re-sync must not resurrect the tombstone: no new
    // rows, the list still shows it with deleted=true
    // (approved plan §3 / Q-D1-6b-14), GET-by-id still
    // returns it with deleted=true.
    let (status, resynced) = h.sync().await;
    assert_eq!(status, 200, "re-sync: {resynced}");
    assert_eq!(resynced["synced_count"], 0);
    assert_eq!(resynced["total_count"], 2);
    assert_eq!(h.synced_rows().len(), 2);
    assert_eq!(h.synced_rows()[0].message_id, "mail-1");
    assert!(h.synced_rows()[0].deleted);
    assert_eq!(h.synced_rows()[1].message_id, "mail-2");
    assert!(!h.synced_rows()[1].deleted);
    let (status, rows) = h.get("/api/email/synced").await;
    assert_eq!(status, 200, "synced list after re-sync: {rows}");
    assert_eq!(rows.as_array().unwrap().len(), 2);
    assert_eq!(rows[0]["message_id"], "mail-1");
    assert_eq!(rows[0]["deleted"], true);
    assert_eq!(rows[1]["message_id"], "mail-2");
    assert_eq!(rows[1]["deleted"], false);
    let (status, row) = h.get("/api/email/synced/mail-1").await;
    assert_eq!(status, 200, "synced get: {row}");
    assert_eq!(row["deleted"], true);
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));
}

/// MD2: approval / digest gates — no approval 403,
/// rejected 403, tool-only 403 `approval_binding_mismatch`,
/// digest mismatch 409 (approval unconsumed, no orphaned
/// Effect), consumed approval 409.
#[tokio::test]
async fn md2_approval_digest_gates() {
    let h = Harness::serve().await;
    let (status, _) = h.sync().await;
    assert_eq!(status, 200);

    // No approval at all.
    let (status, body) = h.delete_message("mail-1", None).await;
    assert_eq!(status, 403, "no approval: {body}");
    assert_eq!(body["error"], "approval_required");
    assert!(h.mail_effects().is_empty());
    assert_eq!(h.mail.delete_count(), 0);

    // Rejected approval.
    let rejected = h.rejected_mail_delete("1001").await;
    let (status, body) = h.delete_message("mail-1", Some(&rejected)).await;
    assert_eq!(status, 403, "rejected: {body}");
    assert_eq!(body["error"], "approval_rejected");
    assert!(h.mail_effects().is_empty());

    // Tool-only approval (no native_id presented): it
    // can never authorize a digest-bound write.
    let (status, requested) = h
        .post("/approval/request", json!({"tool": "mail.delete"}))
        .await;
    assert_eq!(status, 200, "tool-only request: {requested}");
    let tool_only = requested["tool_call_id"].as_str().unwrap().to_string();
    let (status, _) = h
        .post(
            "/approval/decide",
            json!({"tool_call_id": tool_only, "status": "approved"}),
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = h.delete_message("mail-1", Some(&tool_only)).await;
    assert_eq!(status, 403, "tool-only: {body}");
    assert_eq!(body["error"], "approval_binding_mismatch");
    assert!(h.mail_effects().is_empty());
    assert_eq!(h.mail.delete_count(), 0);

    // Digest mismatch: approval binds 1002, delete asks
    // for 1001 — 409, no orphaned planned row, the row
    // intact, the approval unconsumed.
    let wrong = h.approved_mail_delete("1002").await;
    let (status, body) = h.delete_message("mail-1", Some(&wrong)).await;
    assert_eq!(status, 409, "mismatch: {body}");
    assert_eq!(body["error"], "approval_digest_mismatch");
    assert!(h.mail_effects().is_empty());
    assert!(!h.synced_rows()[0].deleted);

    // A correct approval still works after the mismatch.
    let approval = h.approved_mail_delete("1001").await;
    let (status, deleted) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 200, "correct retry: {deleted}");
    assert_eq!(h.mail.delete_count(), 1);

    // The same approval id cannot authorize a second
    // delete.
    let (status, body) = h.delete_message("mail-2", Some(&approval)).await;
    assert_eq!(status, 409, "consumed replay: {body}");
    assert_eq!(body["error"], "approval_already_consumed");
    assert_eq!(h.mail_effects().len(), 1, "no second Effect");
    assert_eq!(h.mail.delete_count(), 1);
}

/// MD3: re-delete idempotency — deleting a tombstoned
/// row (by local id or by native id) returns 200 with
/// the current row and `replayed: true`, no second
/// event, no second Effect, no second fake delete.
#[tokio::test]
async fn md3_redelete_idempotency() {
    let h = Harness::serve().await;
    let (status, _) = h.sync().await;
    assert_eq!(status, 200);

    let approval = h.approved_mail_delete("1001").await;
    let (status, first) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 200, "first delete: {first}");
    let effect_id = first["effect_id"].as_str().unwrap().to_string();
    assert_eq!(first["replayed"], false);
    assert_eq!(h.mail.delete_count(), 1);

    // Re-delete by local id, with a fresh approval for
    // the same native id.
    let second_approval = h.approved_mail_delete("1001").await;
    let (status, second) = h.delete_message("mail-1", Some(&second_approval)).await;
    assert_eq!(status, 200, "re-delete: {second}");
    assert_eq!(second["message_id"], "mail-1");
    assert_eq!(second["replayed"], true);
    assert_eq!(second["effect_id"], effect_id);

    // Re-delete by native id.
    let third_approval = h.approved_mail_delete("1001").await;
    let (status, third) = h.delete_message("1001", Some(&third_approval)).await;
    assert_eq!(status, 200, "re-delete by native id: {third}");
    assert_eq!(third["replayed"], true);
    assert_eq!(third["effect_id"], effect_id);

    assert_eq!(h.mail.delete_count(), 1, "no second fake delete");
    assert_eq!(h.mail_events().len(), 1, "no second event");
    assert_eq!(h.mail_effects().len(), 1, "no second Effect");
}

/// MD4: redaction + fold — no subject / sender / body
/// plaintext anywhere in the event log; the
/// `mail.deleted` payload carries exactly the six
/// whitelisted fields; the fold holds.
#[tokio::test]
async fn md4_redaction_and_fold() {
    let secret_body = "The launch code is hunter2 and the key is <redacted>";
    let secret_subject = "Quarterly results (confidential)";
    let h = Harness::serve_with(vec![seed_message(
        "1001",
        "boss@example.invalid",
        secret_subject,
        secret_body,
    )])
    .await;
    let (status, _) = h.sync().await;
    assert_eq!(status, 200);

    let approval = h.approved_mail_delete("1001").await;
    let (status, deleted) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 200, "delete: {deleted}");

    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "mail.deleted")
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
    let deleted_event = events
        .iter()
        .find(|e| e.event_type == "mail.deleted")
        .unwrap();
    let payload: Value = serde_json::from_str(&deleted_event.payload_json).unwrap();
    assert_eq!(
        payload.as_object().unwrap().len(),
        6,
        "no extra fields: {payload}"
    );
    // The body is in no row either (only its hash).
    let row = get_mail_message(&conn, "mail-1")
        .expect("get")
        .expect("row");
    let row_json = serde_json::to_string(&row).unwrap();
    assert!(!row_json.contains("hunter2"), "row leaks body: {row_json}");
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// MD5: error mapping — unknown id 404, disconnected
/// 409 (reads unaffected), never-synced delete 200 with
/// no fact row, Mail not running 502 (Effect failed),
/// Automation denied 403 (Effect failed), script
/// timeout 502 (Effect unknown + Inbox row), non-macOS /
/// unavailable 501 (Effect failed).
#[tokio::test]
async fn md5_error_mapping() {
    let h = Harness::serve().await;
    let (status, _) = h.sync().await;
    assert_eq!(status, 200);

    // Unknown id → 404, before any approval is needed.
    let (status, body) = h.delete_message("mail-99", None).await;
    assert_eq!(status, 404, "unknown id: {body}");
    assert_eq!(body["error"], "unknown message");
    let (status, body) = h.delete_message("not-an-id", None).await;
    assert_eq!(status, 404, "unknown non-numeric id: {body}");

    // Disconnected source rejects the write with 409;
    // reads are unaffected.
    let (status, disconnected) = h.post("/sources/mail-source/disconnect", json!({})).await;
    assert_eq!(status, 200, "disconnect: {disconnected}");
    assert_eq!(disconnected["status"], "disconnected");
    let approval = h.approved_mail_delete("1001").await;
    let (status, body) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 409, "disconnected write: {body}");
    assert_eq!(body["error"], "source_disconnected");
    assert_eq!(h.mail.delete_count(), 0, "disconnected must not delete");
    assert!(h.mail_effects().is_empty(), "no Effect for a refused write");
    let (status, rows) = h.get("/api/email/synced").await;
    assert_eq!(status, 200, "synced read: {rows}");
    assert_eq!(rows.as_array().unwrap().len(), 2);
    // Restore for the remaining cases (no reconnect
    // route — direct sqlite3, the 6b-1 R2 precedent).
    let conn = h.db();
    conn.execute(
        "UPDATE sources SET status = 'active' WHERE source_id = 'mail-source'",
        [],
    )
    .expect("restore");
    drop(conn);

    // Mail not running → 502, Effect failed, row intact.
    h.mail.set_mode(FakeMode::NotRunning);
    let approval = h.approved_mail_delete("1001").await;
    let (status, body) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 502, "not running: {body}");
    assert_eq!(body["error"], "Mail is not running");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.mail.delete_count(), 0);
    assert!(!h.synced_rows()[0].deleted);
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Automation denied → 403, Effect failed.
    h.mail.set_mode(FakeMode::Denied);
    let approval = h.approved_mail_delete("1001").await;
    let (status, body) = h.delete_message("mail-1", Some(&approval)).await;
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
    // delete may or may not have happened: no tombstone,
    // no guess.
    h.mail.set_mode(FakeMode::Timeout);
    let approval = h.approved_mail_delete("1001").await;
    let (status, body) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 502, "timeout: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "unknown");
    assert!(!h.synced_rows()[0].deleted, "timeout must not tombstone");
    assert!(
        h.mail_events().is_empty(),
        "timeout must not record the event"
    );
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
    let approval = h.approved_mail_delete("1001").await;
    let (status, body) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 501, "unavailable: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");
}

/// MD5 (never-synced): deleting a message that was
/// never synced is allowed (Q-D1-6b-15) — the
/// real-mailbox delete executes, no empty fact row is
/// created, the response carries no local id.
#[tokio::test]
async fn md5_never_synced_delete() {
    let h = Harness::serve_with(vec![seed_message(
        "7777",
        "carol@example.invalid",
        "Never synced",
        "Never synced body",
    )])
    .await;
    // No sync: the gateway has no row for 7777.
    assert!(h.synced_rows().is_empty());

    let approval = h.approved_mail_delete("7777").await;
    let (status, deleted) = h.delete_message("7777", Some(&approval)).await;
    assert_eq!(status, 200, "never-synced delete: {deleted}");
    assert_eq!(deleted["message_id"], Value::Null);
    assert_eq!(deleted["native_id"], "7777");
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["replayed"], false);
    let effect_id = deleted["effect_id"].as_str().unwrap().to_string();

    assert_eq!(h.mail.delete_count(), 1);
    assert_eq!(h.mail.inbox_ids().len(), 0);
    assert_eq!(h.mail.trash_ids(), vec!["7777"]);
    assert!(h.synced_rows().is_empty(), "no empty fact row");
    let effect = h
        .mail_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "succeeded");
    assert_eq!(effect.scope, "7777");
    let events = h.mail_events();
    assert_eq!(events.len(), 1);
    let payload: Value = serde_json::from_str(&events[0].payload_json).unwrap();
    assert_eq!(payload["message_id"], "7777");
    assert_eq!(payload["native_id"], "7777");
    // Verify reads back absent → verified.
    let (status, verified) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "verify: {verified}");
    assert_eq!(verified["verdict"], "verified");
}

/// Reader + restart (plan §4 MD5, the 6b-1/6b-2
/// disconnect extra by analogy): the synced list hides
/// tombstones, GET-by-id returns them, unknown ids 404,
/// and the rows + tombstones survive a gateway restart.
#[tokio::test]
async fn md_reader_and_restart() {
    let mut h = Harness::serve().await;
    let (status, _) = h.sync().await;
    assert_eq!(status, 200);

    let (status, rows) = h.get("/api/email/synced").await;
    assert_eq!(status, 200, "synced list: {rows}");
    assert_eq!(rows.as_array().unwrap().len(), 2);
    let (status, row) = h.get("/api/email/synced/mail-1").await;
    assert_eq!(status, 200, "synced get: {row}");
    assert_eq!(row["native_id"], "1001");
    assert_eq!(row["deleted"], false);
    let (status, body) = h.get("/api/email/synced/mail-99").await;
    assert_eq!(status, 404, "unknown synced: {body}");

    // Delete one, then restart: the tombstone and the
    // surviving row must both be there afterwards, and
    // the delete still works after restart.
    let approval = h.approved_mail_delete("1001").await;
    let (status, _) = h.delete_message("mail-1", Some(&approval)).await;
    assert_eq!(status, 200);
    h.restart().await;

    let (status, rows) = h.get("/api/email/synced").await;
    assert_eq!(status, 200, "synced list after restart: {rows}");
    assert_eq!(rows.as_array().unwrap().len(), 2);
    assert_eq!(rows[0]["message_id"], "mail-1");
    assert_eq!(rows[0]["deleted"], true);
    assert_eq!(rows[1]["message_id"], "mail-2");
    assert_eq!(rows[1]["deleted"], false);
    let (status, row) = h.get("/api/email/synced/mail-1").await;
    assert_eq!(status, 200, "tombstone after restart: {row}");
    assert_eq!(row["deleted"], true);
    let approval = h.approved_mail_delete("1002").await;
    let (status, deleted) = h.delete_message("mail-2", Some(&approval)).await;
    assert_eq!(status, 200, "delete after restart: {deleted}");
    assert_eq!(deleted["message_id"], "mail-2");
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));
}
