//! MCL-2 tests (plan §4, C1–C5): contacts read — sync
//! add / skip / tombstone-no-resurrect, search hit,
//! restart consistency, redact no PII plaintext in
//! events. C0 is the registration unit test (the six
//! loop-tool points, without a server); C4 is the
//! disconnect check and C6 the loop-dispatch e2e test
//! (the MCL-1 extras).
//!
//! Tests use a mutable fake source only; never the real
//! Contacts.app. Cross-platform: temp dirs, ports,
//! SQLite only. No kill command, no POSIX signals, no
//! Unix-only path spellings. JSON bodies are built with
//! `json!` (the test-hygiene skill); time budgets are
//! generous (60 s client timeout, no ms-class bounds).
//!
//! The PII split under test (Q-MCL-11): list / get /
//! search responses DO carry plaintext display_name /
//! phones / emails (display, dial, and send need
//! them) — C1 asserts that. Event payloads must NOT
//! (hashes / counts / flags only) — C5 asserts that.
//! Address / birthday VALUES are never stored anywhere.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::contacts_source::{
    addresses_hash_for, birthday_hash_for, ContactAddress, ContactContent, ContactView,
    ContactsError, ContactsSource,
};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
use mock_gateway::tool_requires_approval;
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-mcl2-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct ScriptModel {
    steps: Mutex<VecDeque<ModelReply>>,
    contexts: Mutex<Vec<Vec<Message>>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(replies.into()),
            contexts: Mutex::new(vec![]),
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
        let reply = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move { Ok(reply) })
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

/// Mutable fake contacts source: the test controls the
/// contact set (add a contact between syncs for C1).
/// Names / numbers / emails are fictional (555-01xx
/// numbers, .invalid domains) and never leave this
/// file — only their hashes reach the event log.
struct MutableFakeContactsSource {
    contacts: Mutex<Vec<ContactView>>,
    next_id: AtomicUsize,
}

impl MutableFakeContactsSource {
    fn seeded() -> Arc<Self> {
        Arc::new(Self {
            contacts: Mutex::new(vec![contact_view_a(), contact_view_b()]),
            // Seeded cards are person-a / person-b; new
            // cards take fake-person-N from 3 up.
            next_id: AtomicUsize::new(3),
        })
    }

    fn add(&self, view: ContactView) {
        self.contacts.lock().unwrap().push(view);
    }
}

impl ContactsSource for MutableFakeContactsSource {
    fn contacts(&self, limit: usize, offset: usize) -> Result<Vec<ContactView>, ContactsError> {
        let views = self.contacts.lock().unwrap().clone();
        Ok(views.into_iter().skip(offset).take(limit).collect())
    }

    fn contact_content(&self, native_id: &str) -> Result<Option<ContactContent>, ContactsError> {
        let views = self.contacts.lock().unwrap();
        Ok(views
            .iter()
            .find(|v| v.native_id == native_id)
            .map(|v| ContactContent {
                display_name: v.display_name.clone(),
                organization: v.organization.clone(),
                job_title: v.job_title.clone(),
                phones: v.phones.clone(),
                emails: v.emails.clone(),
            }))
    }

    fn create_contact(&self, content: &ContactContent) -> Result<String, ContactsError> {
        let native_id = format!(
            "fake-person-{}",
            self.next_id.fetch_add(1, Ordering::SeqCst)
        );
        self.contacts.lock().unwrap().push(ContactView {
            native_id: native_id.clone(),
            display_name: content.display_name.clone(),
            organization: content.organization.clone(),
            job_title: content.job_title.clone(),
            phones: content.phones.clone(),
            emails: content.emails.clone(),
            addresses: vec![],
            birthday: None,
        });
        Ok(native_id)
    }

    fn update_contact(
        &self,
        native_id: &str,
        content: &ContactContent,
    ) -> Result<(), ContactsError> {
        let mut views = self.contacts.lock().unwrap();
        let Some(view) = views.iter_mut().find(|v| v.native_id == native_id) else {
            return Err(ContactsError::NotFound("unknown contact"));
        };
        // Full replace of the writable fields only:
        // addresses / birthday are untouched (address /
        // birthday writes are deferred).
        view.display_name = content.display_name.clone();
        view.organization = content.organization.clone();
        view.job_title = content.job_title.clone();
        view.phones = content.phones.clone();
        view.emails = content.emails.clone();
        Ok(())
    }

    fn delete_contact(&self, native_id: &str) -> Result<(), ContactsError> {
        let mut views = self.contacts.lock().unwrap();
        let Some(pos) = views.iter().position(|v| v.native_id == native_id) else {
            return Err(ContactsError::NotFound("unknown contact"));
        };
        views.remove(pos);
        Ok(())
    }
}

/// The full card: name / org / job title / two phones
/// / one email / two addresses / a birthday.
fn contact_view_a() -> ContactView {
    ContactView {
        native_id: "person-a".to_string(),
        display_name: "Ada Lovelace".to_string(),
        organization: "Analytical Engines".to_string(),
        job_title: "Mathematician".to_string(),
        phones: vec!["+15550101001".to_string(), "+15550101002".to_string()],
        emails: vec!["ada@mock.invalid".to_string()],
        addresses: vec![
            ContactAddress {
                street: "1 Mock St".to_string(),
                city: "Springfield".to_string(),
                state: "IL".to_string(),
                postal_code: "62701".to_string(),
                country: "USA".to_string(),
            },
            ContactAddress {
                street: "2 Other Ave".to_string(),
                city: "Shelbyville".to_string(),
                state: "IL".to_string(),
                postal_code: "62702".to_string(),
                country: "USA".to_string(),
            },
        ],
        birthday: Some("1990-05-17".to_string()),
    }
}

/// The minimal card: a name, nothing else.
fn contact_view_b() -> ContactView {
    ContactView {
        native_id: "person-b".to_string(),
        display_name: "Bob".to_string(),
        organization: String::new(),
        job_title: String::new(),
        phones: vec![],
        emails: vec![],
        addresses: vec![],
        birthday: None,
    }
}

fn build_state(
    dir: &Path,
    model: Arc<ScriptModel>,
    source: Arc<MutableFakeContactsSource>,
) -> AppState {
    let state = AppState::with_files_root_and_approvals(
        dir.join("files"),
        Some(dir.join("approvals.jsonl")),
    )
    .with_tasks_path(dir.join("tasks.jsonl"))
    .with_task_drafts_dir(dir.join("drafts"))
    .with_loop_backends(
        Some(dir.join("loops.jsonl")),
        Some(model),
        None,
        Timeouts::default(),
    )
    .unwrap()
    .with_deepseek(mock_gateway::deepseek::DeepSeekClient::new(
        mock_gateway::deepseek::DeepSeekConfig {
            base_url: "http://127.0.0.1:1".to_string(),
            model: "fixture".to_string(),
        },
        mock_gateway::deepseek::DeepSeekCredentialBroker::test_key("fake-mcl2-key"),
    ))
    .with_auto_drive(false)
    .with_executions_path(dir.join("executions.jsonl"))
    .with_effects_path(dir.join("effects.jsonl"))
    .with_evidence_path(dir.join("evidence.jsonl"))
    .with_binding_state_path(dir.join("bindings.jsonl"))
    .with_project_state_path(dir.join("projects.jsonl"))
    .with_location_state_path(dir.join("locations.jsonl"))
    .with_worktree_state_path(dir.join("worktrees.jsonl"))
    .with_contacts_source(source)
    .with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    state
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: Option<tokio::task::JoinHandle<()>>,
    model: Arc<ScriptModel>,
    source: Arc<MutableFakeContactsSource>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let source = MutableFakeContactsSource::seeded();
        let state = build_state(&dir, model.clone(), source.clone());
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
            model,
            source,
        }
    }

    /// Restart the gateway on the same directory (same
    /// SQLite file, same JSONL files, same fake source):
    /// the production restart path, not a fresh database.
    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let model = Arc::new(ScriptModel::new(vec![]));
        self.model = model.clone();
        let state = build_state(&self.dir, model, self.source.clone());
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

    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{path}: {body}");
        body
    }

    async fn ok_get(&self, path: &str) -> Value {
        let (status, body) = self.get(path).await;
        assert_eq!(status, 200, "{path}: {body}");
        body
    }

    async fn sync(&self) -> Value {
        self.ok("/api/contacts/sync", json!({})).await
    }

    async fn start(&self) -> String {
        self.ok(
            "/agent/loop/start",
            json!({"prompt": "search the contacts"}),
        )
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
                    call_id, result, ..
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
        self.ok_get("/effects/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }
}

fn contacts_event_payloads(h: &Harness) -> Vec<String> {
    let conn = h.db();
    let mut stmt = conn
        .prepare(
            "SELECT payload_json FROM events WHERE aggregate_kind = 'contacts' \
             AND event_type = 'contact.synced' ORDER BY seq",
        )
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn source_cursor(h: &Harness) -> String {
    let conn = h.db();
    conn.query_row(
        "SELECT cursor FROM sources WHERE source_id = 'contacts-source'",
        [],
        |row| row.get::<_, String>(0),
    )
    .unwrap()
}

// ---------------------------------------------------------------- C0

#[test]
fn c0_loop_tool_registration_points() {
    // lib.rs: a read, no approval.
    assert_eq!(tool_requires_approval("contacts.search"), Some(false));
    // loop_engine.rs: the loop's own gate agrees.
    assert!(!loop_tool_requires_approval("contacts.search"));
    // loop_engine.rs normalize: defaults, clamp, reject.
    let norm = normalize("contacts.search", &json!({})).unwrap();
    assert_eq!(norm["q"], "");
    assert_eq!(norm["limit"], "50");
    assert_eq!(norm["offset"], "0");
    let norm = normalize("contacts.search", &json!({"q": "ada", "limit": 500})).unwrap();
    assert_eq!(norm["q"], "ada");
    assert_eq!(norm["limit"], "200", "clamps like the list route");
    assert!(normalize("contacts.search", &json!({"bogus": 1})).is_err());
    assert!(normalize("contacts.search", &json!({"q": 1})).is_err());
}

// ---------------------------------------------------------------- C1

#[tokio::test]
async fn c1_sync_add_skip_tombstone_no_resurrect() {
    let h = Harness::serve(vec![]).await;
    // First sync adds both seeded contacts.
    let first = h.sync().await;
    assert_eq!(first["synced_count"], 2);
    assert_eq!(first["skipped_count"], 0);
    assert_eq!(first["total_count"], 2);
    let cursor = first["cursor"].as_str().unwrap().to_string();
    assert!(!cursor.is_empty());
    assert_eq!(source_cursor(&h), cursor);

    // The list carries plaintext display_name / phones /
    // emails (Q-MCL-11: display, dial, and send need
    // them) and the address / birthday HASHES (NULL when
    // the card has none).
    let contacts = h.ok_get("/api/contacts").await;
    let rows = contacts.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["contact_id"], "contact-1");
    assert_eq!(rows[0]["native_id"], "person-a");
    assert_eq!(rows[0]["display_name"], "Ada Lovelace");
    assert_eq!(rows[0]["organization"], "Analytical Engines");
    assert_eq!(rows[0]["job_title"], "Mathematician");
    assert_eq!(rows[0]["phones"], json!(["+15550101001", "+15550101002"]));
    assert_eq!(rows[0]["emails"], json!(["ada@mock.invalid"]));
    assert_eq!(
        rows[0]["addresses_hash"],
        addresses_hash_for(&contact_view_a().addresses).unwrap()
    );
    assert_eq!(rows[0]["birthday_hash"], birthday_hash_for("1990-05-17"));
    assert_eq!(rows[1]["contact_id"], "contact-2");
    assert_eq!(rows[1]["display_name"], "Bob");
    assert_eq!(rows[1]["phones"], json!([]));
    assert_eq!(rows[1]["emails"], json!([]));
    assert_eq!(rows[1]["addresses_hash"], Value::Null);
    assert_eq!(rows[1]["birthday_hash"], Value::Null);

    // Get one row; unknown id 404s.
    let one = h.ok_get("/api/contacts/contact-1").await;
    assert_eq!(one["native_id"], "person-a");
    let (status, _) = h.get("/api/contacts/contact-99").await;
    assert_eq!(status, 404);

    // Second sync skips both; the cursor does not move.
    let second = h.sync().await;
    assert_eq!(second["synced_count"], 0);
    assert_eq!(second["skipped_count"], 2);
    assert_eq!(second["total_count"], 2);
    assert_eq!(second["cursor"].as_str().unwrap(), cursor);

    // Exactly two `contact.synced` events — one per
    // contact, none for the skipped re-syncs.
    assert_eq!(contacts_event_payloads(&h).len(), 2);

    // A new contact appears in the live source: the
    // cursor advances and only the new row syncs.
    h.source.add(ContactView {
        native_id: "person-c".to_string(),
        display_name: "Carol".to_string(),
        organization: String::new(),
        job_title: String::new(),
        phones: vec!["+15550101003".to_string()],
        emails: vec![],
        addresses: vec![],
        birthday: None,
    });
    let third = h.sync().await;
    assert_eq!(third["synced_count"], 1);
    assert_eq!(third["skipped_count"], 2);
    assert_eq!(third["total_count"], 3);
    let cursor2 = third["cursor"].as_str().unwrap().to_string();
    assert_ne!(cursor2, cursor, "cursor must advance on a new contact");
    assert_eq!(source_cursor(&h), cursor2);
    assert_eq!(contacts_event_payloads(&h).len(), 3);

    // Tombstone contact-1 by direct SQL (there is no
    // delete route in MCL-2); the next sync must NOT
    // resurrect it.
    h.db()
        .execute(
            "UPDATE contacts SET deleted = 1 WHERE contact_id = 'contact-1'",
            [],
        )
        .unwrap();
    let fourth = h.sync().await;
    assert_eq!(fourth["synced_count"], 0, "tombstone must not resurrect");
    assert_eq!(fourth["skipped_count"], 3);
    assert_eq!(fourth["total_count"], 3);
    assert_eq!(fourth["cursor"].as_str().unwrap(), cursor2);
    let rows = h.ok_get("/api/contacts").await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 3, "tombstones stay visible");
    assert_eq!(rows[0]["deleted"], true);
    let one = h.ok_get("/api/contacts/contact-1").await;
    assert_eq!(one["deleted"], true, "get returns the tombstone");
    assert_eq!(contacts_event_payloads(&h).len(), 3, "no new events");
}

// ---------------------------------------------------------------- C2

#[tokio::test]
async fn c2_search_hit() {
    let h = Harness::serve(vec![]).await;
    h.sync().await;

    // Name hit (case-insensitive substring).
    let hits = h.ok_get("/api/contacts/search?q=ada").await;
    assert_eq!(hits.as_array().unwrap().len(), 1);
    assert_eq!(hits[0]["contact_id"], "contact-1");
    let hits = h.ok_get("/api/contacts/search?q=ADA").await;
    assert_eq!(hits.as_array().unwrap().len(), 1, "case-insensitive");

    // Phone and email hits (Q-MCL-11 plaintext fields).
    let hits = h.ok_get("/api/contacts/search?q=01002").await;
    assert_eq!(hits.as_array().unwrap().len(), 1);
    assert_eq!(hits[0]["contact_id"], "contact-1");
    let hits = h.ok_get("/api/contacts/search?q=mock.invalid").await;
    assert_eq!(hits.as_array().unwrap().len(), 1);
    assert_eq!(hits[0]["contact_id"], "contact-1");

    // No hit, and a hit on the second contact.
    let hits = h.ok_get("/api/contacts/search?q=zzz-no-such").await;
    assert_eq!(hits.as_array().unwrap().len(), 0);
    let hits = h.ok_get("/api/contacts/search?q=bob").await;
    assert_eq!(hits.as_array().unwrap().len(), 1);
    assert_eq!(hits[0]["contact_id"], "contact-2");

    // Missing or empty q behaves like the list route.
    let hits = h.ok_get("/api/contacts/search").await;
    assert_eq!(hits.as_array().unwrap().len(), 2);
    let hits = h.ok_get("/api/contacts/search?q=").await;
    assert_eq!(hits.as_array().unwrap().len(), 2);
    let hits = h.ok_get("/api/contacts/search?q=%20%20").await;
    assert_eq!(hits.as_array().unwrap().len(), 2, "blank q is empty q");
}

// ---------------------------------------------------------------- C3

#[tokio::test]
async fn c3_restart_keeps_rows_and_cursor() {
    let mut h = Harness::serve(vec![]).await;
    let first = h.sync().await;
    let cursor = first["cursor"].as_str().unwrap().to_string();
    let before = h.ok_get("/api/contacts").await;

    h.restart().await;

    let after = h.ok_get("/api/contacts").await;
    assert_eq!(after, before, "rows must survive a restart");
    assert_eq!(source_cursor(&h), cursor, "cursor must survive a restart");

    // A post-restart sync is idempotent, not a replay.
    let again = h.sync().await;
    assert_eq!(again["synced_count"], 0);
    assert_eq!(again["skipped_count"], 2);
    assert_eq!(again["cursor"].as_str().unwrap(), cursor);
    assert_eq!(contacts_event_payloads(&h).len(), 2);
}

// ---------------------------------------------------------------- C4

#[tokio::test]
async fn c4_disconnect_leaves_reads_unaffected() {
    let h = Harness::serve(vec![]).await;
    h.sync().await;

    let disconnected = h.ok("/sources/contacts-source/disconnect", json!({})).await;
    assert_eq!(disconnected["status"], "disconnected");

    // List + get + search still read (6b Q-24:
    // disconnect is a flag flip only; reads unaffected).
    let rows = h.ok_get("/api/contacts").await;
    assert_eq!(rows.as_array().unwrap().len(), 2);
    let one = h.ok_get("/api/contacts/contact-1").await;
    assert_eq!(one["native_id"], "person-a");
    let hits = h.ok_get("/api/contacts/search?q=ada").await;
    assert_eq!(hits.as_array().unwrap().len(), 1);

    // Sync is fact recording, like mail sync: it still
    // works after disconnect (only writes are gated).
    let again = h.sync().await;
    assert_eq!(again["synced_count"], 0);
    assert_eq!(again["skipped_count"], 2);
}

// ---------------------------------------------------------------- C5

#[tokio::test]
async fn c5_redact_keeps_no_pii_plaintext_in_events() {
    let h = Harness::serve(vec![]).await;
    h.sync().await;

    // No event payload names a display name, an
    // organization, a job title, a phone or email value,
    // or any address / birthday value.
    for payload in contacts_event_payloads(&h) {
        assert!(!payload.contains("Ada Lovelace"), "{payload}");
        assert!(!payload.contains("Analytical Engines"), "{payload}");
        assert!(!payload.contains("Mathematician"), "{payload}");
        assert!(!payload.contains("+15550101001"), "{payload}");
        assert!(!payload.contains("+15550101002"), "{payload}");
        assert!(!payload.contains("ada@mock.invalid"), "{payload}");
        assert!(!payload.contains("Mock St"), "{payload}");
        assert!(!payload.contains("1990-05-17"), "{payload}");
        assert!(!payload.contains("Bob"), "{payload}");
        // The payload carries hashes / counts / flags
        // instead — the 13-field allowlist.
        assert!(payload.contains("display_name_hash"), "{payload}");
        assert!(payload.contains("organization_hash"), "{payload}");
        assert!(payload.contains("phone_count"), "{payload}");
        assert!(payload.contains("phone_hash"), "{payload}");
        assert!(payload.contains("email_count"), "{payload}");
        assert!(payload.contains("email_hash"), "{payload}");
        assert!(payload.contains("has_address"), "{payload}");
        assert!(payload.contains("has_birthday"), "{payload}");
    }

    // THE DISTINCTION: list / get / search responses
    // legitimately carry the same plaintext (Q-MCL-11) —
    // the redact rule covers EVENT payloads only, never
    // list responses. This assertion locks that in: if
    // a later change strips plaintext from the list, the
    // display / dial use case breaks and this test says so.
    let list = h.ok_get("/api/contacts").await;
    let serialized = serde_json::to_string(&list).unwrap();
    assert!(serialized.contains("Ada Lovelace"), "{serialized}");
    assert!(serialized.contains("+15550101001"), "{serialized}");
    assert!(serialized.contains("ada@mock.invalid"), "{serialized}");
    // ...but even the list never carries an address or
    // birthday value (they are never stored at all).
    assert!(!serialized.contains("Mock St"), "{serialized}");
    assert!(!serialized.contains("1990-05-17"), "{serialized}");
}

// ---------------------------------------------------------------- C6

#[tokio::test]
async fn c6_loop_tool_searches_contacts() {
    let h = Harness::serve(vec![
        calls(vec![proposal("contacts.search", json!({"q": "ada"}))]),
        done(),
    ])
    .await;
    h.sync().await;
    let loop_id = h.start().await;
    let final_view = h.drive(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    let seen = h.tool_messages();
    assert_eq!(seen.len(), 1, "{seen:?}");
    let (body, truncated) = &seen[0];
    assert!(!truncated);
    assert!(body.contains("contact: contact-1"), "{body}");
    assert!(body.contains("name: Ada Lovelace"), "{body}");
    assert!(body.contains("organization: Analytical Engines"), "{body}");
    assert!(body.contains("phones: +15550101001"), "{body}");
    assert!(body.contains("emails: ada@mock.invalid"), "{body}");
    assert!(body.contains("has_address: true"), "{body}");
    assert!(body.contains("has_birthday: true"), "{body}");
    // The second contact does not match the query.
    assert!(!body.contains("contact: contact-2"), "{body}");
    // Address / birthday VALUES never reach the model.
    assert!(!body.contains("Mock St"), "{body}");
    assert!(!body.contains("1990-05-17"), "{body}");
    // Reads produce no Effect.
    assert!(h.effects().await.is_empty());
}
