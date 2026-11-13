//! D1-6a tests (plan §7, Y1–Y5): mail incremental sync
//! into SQLite (v10 `mail_messages`) + calendar
//! read-only + the `redact_event_payload` privacy choke
//! point.
//!
//! HTTP routes only, plus `sqlite_store` reads (mail
//! rows / events / fold) where HTTP cannot reach the
//! state. The mail source is a mutable in-test mock (no
//! AppleScript, no real Mail.app): it holds the inbox
//! in newest-first order and enforces the batch-1
//! 50-message window itself via the production
//! `group_threads`, so Y1–Y3 run identically on Linux
//! and macOS.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::mail_source::{
    group_threads, normalize_subject, thread_id_for, AttachmentData, MailError, MailSource,
    MailboxInfo, MessageView, MockMailSource, RawMessageMeta, SentMessage, SetupInfo, ThreadView,
};
use mock_gateway::mail_sync::{list_mail_messages, MailMessageRow};
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

/// A mutable in-test mail source: the inbox in
/// newest-first order, protected by a Mutex so the test
/// can prepend arrivals between syncs. `threads()`
/// enforces the batch-1 50-message window (first 50 in
/// inbox order) through the production `group_threads`;
/// `message()` returns the stored view verbatim.
struct MutableMockMailSource {
    messages: Mutex<Vec<MessageView>>,
}

impl MutableMockMailSource {
    fn new(messages: Vec<MessageView>) -> Self {
        Self {
            messages: Mutex::new(messages),
        }
    }

    fn prepend(&self, message: MessageView) {
        self.messages.lock().unwrap().insert(0, message);
    }

    fn window(&self) -> Vec<MessageView> {
        self.messages
            .lock()
            .unwrap()
            .iter()
            .take(50)
            .cloned()
            .collect()
    }
}

impl MailSource for MutableMockMailSource {
    fn mailbox(&self) -> Result<MailboxInfo, MailError> {
        Ok(MailboxInfo {
            configured: true,
            address: Some("test@example.invalid".to_string()),
            name: Some("Test".to_string()),
            source: "test-mock".to_string(),
        })
    }

    fn setup(&self) -> Result<SetupInfo, MailError> {
        Err(MailError::Unavailable(
            "mail unavailable in tests".to_string(),
        ))
    }

    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<ThreadView>, MailError> {
        let window = self.window();
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
        Ok(group_threads(&metas, &bodies)
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect())
    }

    fn thread(&self, thread_id: &str) -> Result<ThreadView, MailError> {
        self.threads(200, 0)?
            .into_iter()
            .find(|t| t.thread_id == thread_id)
            .ok_or(MailError::NotFound("thread"))
    }

    fn message(&self, message_id: &str) -> Result<MessageView, MailError> {
        self.messages
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
        MockMailSource.send(to, subject, body)
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
    server: Option<tokio::task::JoinHandle<()>>,
    mail: Arc<MutableMockMailSource>,
}

impl Harness {
    async fn serve(messages: Vec<MessageView>) -> Self {
        let dir = temp_dir("sync");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let mail = Arc::new(MutableMockMailSource::new(messages));
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

    async fn stop_server(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
    }

    /// Restart on a deleted database: the rebuild path
    /// (Y3). The old server is stopped first, so its
    /// SQLite connection is dropped before the files go.
    async fn restart_fresh_db(&mut self) {
        self.stop_server().await;
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(
                self.dir
                    .join(format!("gateway.sqlite3{suffix}")),
            );
        }
        self.start_server().await;
    }

    async fn start_server(&mut self) {
        let state = build_state(&self.dir, self.mail.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        self.url = format!("http://{}", listener.local_addr().unwrap());
        self.server = Some(
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() }),
        );
    }

    fn db(&self) -> rusqlite::Connection {
        let conn = sqlite_store::open_db(&self.dir.join("gateway.sqlite3")).expect("open db");
        conn.busy_timeout(Duration::from_secs(5))
            .expect("busy timeout");
        conn
    }

    fn mail_rows(&self) -> Vec<MailMessageRow> {
        let conn = self.db();
        list_mail_messages(&conn, "mail-source").expect("list mail")
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

    async fn sync(&self) -> (u16, Value) {
        self.post("/api/email/sync", json!({})).await
    }

    async fn sync_ok(&self) -> Value {
        let (status, body) = self.sync().await;
        assert_eq!(status, 200, "sync: {body}");
        body
    }
}

fn seed_message(n: usize) -> MessageView {
    // Inbox order is newest-first: native-1 is the
    // newest, native-60 the oldest. Positions 5 and 6
    // share a thread ("Release notes" / "Re: Release
    // notes") and stay adjacent, so the sync's scan
    // order (thread-major) equals inbox order and
    // mail-N maps to native-N exactly.
    let (subject, date) = match n {
        3 => (
            "Test subject 3".to_string(),
            // Same instant as 10:00Z, written with an
            // offset — exercises the parser's zone arm.
            "2026-11-01T18:00:00+08:00".to_string(),
        ),
        5 => ("Release notes".to_string(), "2026-11-01T10:00:00Z".to_string()),
        6 => (
            "Re: Release notes".to_string(),
            "2026-11-01T10:00:00Z".to_string(),
        ),
        7 => (
            "Test subject 7".to_string(),
            // AppleScript locale display dates do not
            // parse: date_ms lands at 0 ("unknown date").
            "Saturday, November 1, 2026 at 10:00:00 AM".to_string(),
        ),
        _ => (
            format!("Test subject {n}"),
            "2026-11-01T10:00:00Z".to_string(),
        ),
    };
    MessageView {
        message_id: format!("native-{n}"),
        from: format!("sender-{n}@example.invalid"),
        to: vec!["me@example.invalid".to_string()],
        date,
        subject,
        body_text: format!("Body of message {n}."),
        attachments: vec![],
    }
}

fn seed_60() -> Vec<MessageView> {
    (1..=60).map(seed_message).collect()
}

/// Y1: first sync imports exactly the inbox first 50,
/// with stable local ids in scan order and thread
/// attribution matching batch-1 `thread_id_for`.
#[tokio::test]
async fn y1_first_sync_imports_inbox_first_50() {
    let h = Harness::serve(seed_60()).await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    // 6b-4 bumps the schema to v13 (notes table).
    assert_eq!(DB_SCHEMA_VERSION, 13);

    let result = h.sync_ok().await;
    assert_eq!(result["synced_count"], 50);
    assert_eq!(result["skipped_count"], 0);
    assert_eq!(result["total_count"], 50);
    let cursor = result["cursor"].as_str().unwrap();
    assert_eq!(cursor.len(), 64, "cursor is a sha256 hex");
    assert!(cursor.chars().all(|c| c.is_ascii_hexdigit()));

    let rows = h.mail_rows();
    assert_eq!(rows.len(), 50);
    for (i, row) in rows.iter().enumerate() {
        let n = i + 1;
        assert_eq!(row.message_id, format!("mail-{n}"));
        assert_eq!(row.native_id, format!("native-{n}"));
        assert_eq!(row.source_id, "mail-source");
        assert_eq!(row.thread_id, thread_id_for(&normalize_subject(&row.subject)));
        assert_eq!(
            row.body_hash,
            sha256_hex(format!("Body of message {n}.").as_bytes())
        );
        // The body itself is never stored anywhere in
        // the row: there is no body column at all, and
        // the hash is the only body-derived value.
        assert!(!row.body_hash.contains("Body of message"));
    }
    // The shared-subject pair lands in one thread; the
    // Re: prefix is stripped by normalize_subject.
    let thread = thread_id_for("Release notes");
    assert_eq!(rows[4].thread_id, thread);
    assert_eq!(rows[5].thread_id, thread);
    assert_ne!(rows[0].thread_id, thread);
    // Offset dates parse to the same instant; locale
    // display dates land at 0.
    assert_eq!(rows[2].date_ms, rows[0].date_ms);
    assert_ne!(rows[2].date_ms, 0);
    assert_eq!(rows[6].date_ms, 0);
    // Exactly one mail.synced event per new row.
    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    let synced: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == "mail.synced")
        .collect();
    assert_eq!(synced.len(), 50);
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// Y2: a second sync adds no duplicates; a newly
/// arrived message adds only its row.
#[tokio::test]
async fn y2_second_sync_adds_no_duplicates() {
    let h = Harness::serve(seed_60()).await;
    let first = h.sync_ok().await;
    let first_cursor = first["cursor"].as_str().unwrap().to_string();

    let (status, second) = h.sync().await;
    assert_eq!(status, 200);
    assert_eq!(second["synced_count"], 0);
    assert_eq!(second["skipped_count"], 50);
    assert_eq!(second["total_count"], 50);
    assert_eq!(second["cursor"].as_str().unwrap(), first_cursor);

    h.mail.prepend(seed_message(0));
    let third = h.sync_ok().await;
    assert_eq!(third["synced_count"], 1);
    // Skipped is 49, not 50: the window slid — the
    // arrival pushes native-50 (now 51st) out of the
    // newest-50 window, so it is neither synced nor
    // skipped in this pass. That is batch-1 window
    // semantics, not a sync bug: the window is always
    // the newest 50.
    assert_eq!(third["skipped_count"], 49);
    assert_eq!(third["total_count"], 51);
    assert_ne!(third["cursor"].as_str().unwrap(), first_cursor);
    let rows = h.mail_rows();
    assert_eq!(rows.len(), 51);
    let last = rows.last().unwrap();
    assert_eq!(last.message_id, "mail-51");
    assert_eq!(last.native_id, "native-0");
    // The first 50 rows are untouched by the arrival.
    assert_eq!(rows[0].message_id, "mail-1");
    assert_eq!(rows[0].native_id, "native-1");
    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    assert_eq!(
        events.iter().filter(|e| e.event_type == "mail.synced").count(),
        51
    );
}

/// Y3: delete the DB, rebuild, re-sync → the same
/// local message_ids, no duplicates, no loss. The inbox
/// is unchanged between the delete and the re-sync, so
/// the deterministic scan order reproduces the same
/// ids (an arrival in between would shift sequential
/// ids — a known limitation of counter-from-scan-order,
/// recorded in the D1-6a report; no mapping survives a
/// DB delete to remap against).
#[tokio::test]
async fn y3_rebuild_reassigns_the_same_local_ids() {
    let mut h = Harness::serve(seed_60()).await;
    h.sync_ok().await;
    let before: HashMap<String, String> = h
        .mail_rows()
        .into_iter()
        .map(|r| (r.native_id, r.message_id))
        .collect();
    assert_eq!(before.len(), 50);

    h.restart_fresh_db().await;
    // 6b-4 bumps the schema to v13 (notes table).
    assert_eq!(h.schema_version(), 13);
    assert_eq!(h.mail_rows().len(), 0);
    let result = h.sync_ok().await;
    assert_eq!(result["synced_count"], 50);
    assert_eq!(result["skipped_count"], 0);
    assert_eq!(result["total_count"], 50);
    let after: HashMap<String, String> = h
        .mail_rows()
        .into_iter()
        .map(|r| (r.native_id, r.message_id))
        .collect();
    assert_eq!(after, before, "local ids stable across rebuild");
    // A second sync after the rebuild is a no-op.
    let again = h.sync_ok().await;
    assert_eq!(again["synced_count"], 0);
    assert_eq!(again["skipped_count"], 50);
    let conn = h.db();
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// Y4: calendar list returns 501 on non-macOS (the
/// EventKit backend does not exist there, and the
/// gateway never fakes data). On macOS the same route
/// must answer from the real store: 200 with a list,
/// or 403 when the user has denied calendar access —
/// either is a live answer, never a 501.
#[tokio::test]
async fn y4_calendar_list_is_501_off_macos() {
    let h = Harness::serve(vec![]).await;
    let (status, body) = h.get("/api/calendar/events").await;
    #[cfg(not(target_os = "macos"))]
    {
        assert_eq!(status, 501, "calendar list: {body}");
        assert_eq!(body["error"], "calendar unavailable on this platform");
        let (status, body) = h.get("/api/calendar/events/some-id").await;
        assert_eq!(status, 501, "calendar detail: {body}");
        assert_eq!(body["error"], "calendar unavailable on this platform");
    }
    #[cfg(target_os = "macos")]
    {
        assert!(
            status == 200 || status == 403,
            "calendar list must be live (200) or denied (403), got {status}: {body}"
        );
        if status == 200 {
            assert!(body.as_array().is_some(), "list returns an array");
        }
    }
}

/// Y5: redaction. Fixed samples only: a message whose
/// body holds a secret-shaped string, whose subject and
/// sender are distinctive — none of the three may
/// appear in any event payload, and the mail.synced
/// payload carries only the whitelisted fields.
#[tokio::test]
async fn y5_no_body_or_secret_in_the_event_log() {
    let secret_message = MessageView {
        message_id: "native-secret".to_string(),
        from: "boss@example.invalid".to_string(),
        to: vec!["me@example.invalid".to_string()],
        date: "2026-11-01T10:00:00Z".to_string(),
        subject: "Quarterly results (confidential)".to_string(),
        body_text: "The launch code is hunter2 and the key is sk-live-TEST123.".to_string(),
        attachments: vec![],
    };
    let h = Harness::serve(vec![secret_message]).await;
    let result = h.sync_ok().await;
    assert_eq!(result["synced_count"], 1);

    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    assert_eq!(
        events.iter().filter(|e| e.event_type == "mail.synced").count(),
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
    // The mail.synced payload is exactly the whitelist:
    // ids, hashes, lengths, counts — nothing else.
    let synced = events
        .iter()
        .find(|e| e.event_type == "mail.synced")
        .unwrap();
    let payload: Value = serde_json::from_str(&synced.payload_json).unwrap();
    let keys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    for kept in [
        "message_id",
        "source_id",
        "native_id",
        "thread_id",
        "body_hash",
        "subject_len",
        "sender_hash",
        "date_ms",
        "created_ms",
    ] {
        assert!(keys.contains(&kept), "missing {kept} in {payload}");
    }
    assert_eq!(keys.len(), 9, "no extra fields: {payload}");
    assert_eq!(
        payload["body_hash"],
        sha256_hex("The launch code is hunter2 and the key is sk-live-TEST123.".as_bytes())
    );
    assert_eq!(
        payload["subject_len"],
        "Quarterly results (confidential)".chars().count()
    );
    assert_eq!(
        payload["sender_hash"],
        sha256_hex("boss@example.invalid".as_bytes())
    );
    // The fold still holds after the sync: redact did
    // not mangle any row-serialization payload.
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}
