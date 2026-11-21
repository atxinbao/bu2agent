//! MCL-5 tests (plan §6, W1–W11): contacts write —
//! create / update / delete end to end — digest-bound
//! approval + write Execution + single-tool Mandate +
//! fact row + `contact.created` / `contact.updated` /
//! `contact.deleted` event + Effect, idempotency,
//! update conflict protection (409 + retry), approval
//! gates, disconnect, redaction, error mapping, and
//! restart.
//!
//! HTTP routes only, plus `sqlite_store` reads (rows /
//! effects / events / inbox) where HTTP cannot reach
//! the state. The contacts source is a test-only fake
//! (no AppleScript, no real Contacts.app): it holds
//! cards in memory and can be switched into the
//! not-running / denied / timeout / unavailable failure
//! modes. The production AppleScript source is never
//! exercised here (no Contacts.app in the VM); the
//! script forms themselves are guarded by the
//! write-script unit tests in `contacts_source.rs`, and
//! window validation on a real Mac is the proof of the
//! real writes. Every write names an already-synced
//! contact (update / delete) or creates a new one; a
//! delete is a tombstone, never a hard delete.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::contacts_source::{
    ContactContent, ContactView, ContactsError, ContactsSource,
};
use mock_gateway::contacts_sync::ContactRow;
use mock_gateway::operation_binding::contact_content_hash;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store::{self, DB_SCHEMA_VERSION};
use mock_gateway::tool_requires_approval;
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-mcl5-{tag}-{}-{}",
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

/// Test-only fake contacts source: the seeded cards
/// plus recordable writes with the failure modes on
/// demand. The read and write modes are separate: the
/// update conflict re-read must stay live while the
/// write itself fails (W7), and the external-edit
/// simulation (W2) mutates the live card directly
/// without touching the stored row. Names / numbers /
/// emails are fictional (555-01xx numbers, .invalid
/// domains) and never leave this file — only their
/// hashes reach the gateway. The production AppleScript
/// source is never exercised here (no Contacts.app in
/// the VM); the script forms are guarded by the
/// write-script unit tests in `contacts_source.rs`, and
/// window validation on a real Mac is the proof of the
/// real writes.
struct MutableFakeContactsSource {
    contacts: Mutex<Vec<ContactView>>,
    next_id: AtomicUsize,
    read_mode: Mutex<FakeMode>,
    write_mode: Mutex<FakeMode>,
}

impl MutableFakeContactsSource {
    fn seeded() -> Arc<Self> {
        Arc::new(Self {
            contacts: Mutex::new(vec![contact_view_a(), contact_view_b()]),
            next_id: AtomicUsize::new(3),
            read_mode: Mutex::new(FakeMode::Ok),
            write_mode: Mutex::new(FakeMode::Ok),
        })
    }

    fn set_write_mode(&self, mode: FakeMode) {
        *self.write_mode.lock().unwrap() = mode;
    }

    fn write_mode(&self) -> FakeMode {
        *self.write_mode.lock().unwrap()
    }

    fn read_mode(&self) -> FakeMode {
        *self.read_mode.lock().unwrap()
    }

    fn check_write(&self) -> Result<(), ContactsError> {
        match self.write_mode() {
            FakeMode::Ok => Ok(()),
            FakeMode::NotRunning => Err(ContactsError::NotRunning),
            FakeMode::Denied => Err(ContactsError::Denied),
            FakeMode::Timeout => Err(ContactsError::Timeout),
            FakeMode::Unavailable => Err(ContactsError::Unavailable(
                "contacts source unavailable in tests".to_string(),
            )),
        }
    }

    /// Simulate an external edit (a user editing the
    /// card in Contacts.app): the live card changes,
    /// the stored row does not — the update conflict
    /// check must catch it.
    fn edit_live(&self, native_id: &str, content: &ContactContent) {
        let mut views = self.contacts.lock().unwrap();
        if let Some(view) = views.iter_mut().find(|v| v.native_id == native_id) {
            view.display_name = content.display_name.clone();
            view.organization = content.organization.clone();
            view.job_title = content.job_title.clone();
            view.phones = content.phones.clone();
            view.emails = content.emails.clone();
        }
    }

    /// Simulate an external delete: the live card is
    /// gone, the stored row is not.
    fn remove_live(&self, native_id: &str) {
        let mut views = self.contacts.lock().unwrap();
        if let Some(pos) = views.iter().position(|v| v.native_id == native_id) {
            views.remove(pos);
        }
    }

    /// Put a card back into the live enumeration (a
    /// restored card keeps its native id): the sync
    /// must still skip its tombstone, never resurrect
    /// it.
    fn add_live(&self, view: ContactView) {
        self.contacts.lock().unwrap().push(view);
    }
}

impl ContactsSource for MutableFakeContactsSource {
    fn contacts(&self, limit: usize, offset: usize) -> Result<Vec<ContactView>, ContactsError> {
        let views = self.contacts.lock().unwrap().clone();
        Ok(views.into_iter().skip(offset).take(limit).collect())
    }

    fn contact_content(
        &self,
        native_id: &str,
    ) -> Result<Option<ContactContent>, ContactsError> {
        match self.read_mode() {
            FakeMode::Ok => {}
            FakeMode::NotRunning => return Err(ContactsError::NotRunning),
            FakeMode::Denied => return Err(ContactsError::Denied),
            FakeMode::Timeout => return Err(ContactsError::Timeout),
            FakeMode::Unavailable => {
                return Err(ContactsError::Unavailable(
                    "contacts source unavailable in tests".to_string(),
                ))
            }
        }
        let views = self.contacts.lock().unwrap();
        Ok(views.iter().find(|v| v.native_id == native_id).map(|v| {
            ContactContent {
                display_name: v.display_name.clone(),
                organization: v.organization.clone(),
                job_title: v.job_title.clone(),
                phones: v.phones.clone(),
                emails: v.emails.clone(),
            }
        }))
    }

    fn create_contact(&self, content: &ContactContent) -> Result<String, ContactsError> {
        self.check_write()?;
        let native_id = format!("fake-person-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
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
        self.check_write()?;
        let mut views = self.contacts.lock().unwrap();
        let Some(view) = views.iter_mut().find(|v| v.native_id == native_id) else {
            return Err(ContactsError::NotFound("unknown contact"));
        };
        view.display_name = content.display_name.clone();
        view.organization = content.organization.clone();
        view.job_title = content.job_title.clone();
        view.phones = content.phones.clone();
        view.emails = content.emails.clone();
        Ok(())
    }

    fn delete_contact(&self, native_id: &str) -> Result<(), ContactsError> {
        self.check_write()?;
        let mut views = self.contacts.lock().unwrap();
        let Some(pos) = views.iter().position(|v| v.native_id == native_id) else {
            return Err(ContactsError::NotFound("unknown contact"));
        };
        views.remove(pos);
        Ok(())
    }
}

fn contact_view_a() -> ContactView {
    ContactView {
        native_id: "person-a".to_string(),
        display_name: "Ada Lovelace".to_string(),
        organization: "Analytical Engines".to_string(),
        job_title: "Mathematician".to_string(),
        phones: vec!["+15550101001".to_string(), "+15550101002".to_string()],
        emails: vec!["ada@mock.invalid".to_string()],
        addresses: vec![],
        birthday: None,
    }
}

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

fn build_state(dir: &Path, source: Arc<dyn ContactsSource>) -> AppState {
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
            api_key: Some("fake-mcl5-key".to_string()),
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
    .with_contacts_source(source);
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
    source: Arc<MutableFakeContactsSource>,
}

impl Harness {
    async fn serve() -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let source = MutableFakeContactsSource::seeded();
        let state = build_state(&dir, source.clone());
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
        let state = build_state(&self.dir, self.source.clone());
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

    fn contact_rows(&self) -> Vec<ContactRow> {
        let conn = self.db();
        mock_gateway::contacts_sync::list_contacts(&conn, "contacts-source", 200, 0)
            .expect("list contacts")
    }

    fn contacts_effects(&self) -> Vec<mock_gateway::effect_store::EffectFileRecord> {
        let conn = self.db();
        sqlite_store::list_effects(&conn)
            .expect("effects")
            .into_iter()
            .filter(|e| e.capability == "contacts")
            .collect()
    }

    fn contacts_events(&self, event_type: &str) -> Vec<sqlite_store::EventRow> {
        let conn = self.db();
        sqlite_store::list_events(&conn)
            .expect("events")
            .into_iter()
            .filter(|e| e.event_type == event_type)
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
        let response = self.client.get(format!("{}{}", self.url, path)).send().await.unwrap();
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

    /// Sync the seeded contacts so updates / deletes
    /// have real `contact-N` rows to name.
    async fn sync(&self) -> Value {
        self.ok("/api/contacts/sync", json!({})).await
    }

    /// Request an approval and decide it approved.
    /// Returns the tool_call_id.
    async fn approved(&self, tool: &str, presentation: Value) -> String {
        let mut body = presentation;
        body["tool"] = json!(tool);
        let (status, requested) = self.post("/approval/request", body).await;
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

    /// Request an approval and decide it rejected.
    /// Returns the tool_call_id.
    async fn rejected(&self, tool: &str, presentation: Value) -> String {
        let mut body = presentation;
        body["tool"] = json!(tool);
        let (status, requested) = self.post("/approval/request", body).await;
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

    /// Request a tool-only approval (no presentation
    /// fields) and decide it approved. Returns the
    /// tool_call_id.
    async fn approved_tool_only(&self, tool: &str) -> String {
        let (status, requested) = self.post("/approval/request", json!({"tool": tool})).await;
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

}

fn content_hash_of(row: &Value) -> String {
    row["content_hash"].as_str().unwrap().to_string()
}

// ---------------------------------------------------------------- W1

#[tokio::test]
async fn w1_create_end_to_end() {
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    assert_eq!(h.schema_version(), 18, "F-1 adds the v18 idempotency index");
    let synced = h.sync().await;
    assert_eq!(synced["synced_count"], 2);

    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, created) = h
        .post(
            "/api/contacts/create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
                "idempotency_key": "key-w1",
                "tool_call_id": approval,
            }),
        )
        .await;
    assert_eq!(status, 200, "create: {created}");
    assert_eq!(created["contact_id"], "contact-3");
    assert_eq!(created["native_id"], "fake-person-3");
    assert_eq!(created["display_name"], "Carol Danvers");
    assert_eq!(created["organization"], "Starforce");
    assert_eq!(created["job_title"], "Pilot");
    assert_eq!(created["phones"], json!(["+15550101003"]));
    assert_eq!(created["emails"], json!(["carol@mock.invalid"]));
    assert_eq!(created["deleted"], false);
    assert_eq!(created["replayed"], false);
    assert_eq!(created["idempotency_key"], "key-w1");
    assert!(created["effect_id"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(created["policy_version"].as_str().is_some_and(|s| !s.is_empty()));
    assert_eq!(
        created["content_hash"],
        contact_content_hash(
            "Carol Danvers",
            "Starforce",
            "Pilot",
            &["+15550101003".to_string()],
            &["carol@mock.invalid".to_string()],
        )
    );
    // The live card exists in the fake source now.
    let live = h
        .source
        .contact_content("fake-person-3")
        .unwrap()
        .expect("live card must exist");
    assert_eq!(live.display_name, "Carol Danvers");
    assert_eq!(live.phones, vec!["+15550101003".to_string()]);

    // One row, one event, one Effect.
    let rows = h.contact_rows();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].contact_id, "contact-3");
    assert_eq!(rows[2].idempotency_key.as_deref(), Some("key-w1"));
    let events = h.contacts_events("contact.created");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].aggregate_kind, "contacts");
    assert_eq!(events[0].aggregate_id, "contact-3");
    let payload: Value = serde_json::from_str(&events[0].payload_json).unwrap();
    assert_eq!(payload["contact_id"], "contact-3");
    assert_eq!(payload["native_id"], "fake-person-3");
    assert_eq!(payload["phone_count"], 1);
    assert_eq!(payload["email_count"], 1);
    assert_eq!(payload["has_address"], false);
    assert_eq!(payload["has_birthday"], false);
    assert_eq!(payload["content_hash"], created["content_hash"]);
    let effects = h.contacts_effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].status, "succeeded");
    assert_eq!(effects[0].operation, "create");
    assert_eq!(effects[0].effect_id, created["effect_id"].as_str().unwrap());
    assert_eq!(
        payload["execution_id"],
        effects[0].execution_id.as_deref().unwrap(),
        "event and Effect name the same Execution"
    );

    // A re-sync sees the live card already present:
    // nothing new, cursor unchanged.
    let again = h.sync().await;
    assert_eq!(again["synced_count"], 0);
    assert_eq!(again["skipped_count"], 3);
    assert_eq!(again["total_count"], 3);
}

// ---------------------------------------------------------------- W2

#[tokio::test]
async fn w2_update_end_to_end_conflict_and_retry() {
    let h = Harness::serve().await;
    h.sync().await;
    let detail = h.ok_get("/api/contacts/contact-1").await;
    assert_eq!(detail["display_name"], "Ada Lovelace");
    let base = content_hash_of(&detail);

    // Update with the fresh base: full replace.
    let approval = h
        .approved(
            "contacts.update",
            json!({
                "contact_id": "contact-1",
                "display_name": "Ada Byron",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101009"],
                "emails": ["ada.byron@mock.invalid"],
            }),
        )
        .await;
    let (status, updated) = h
        .post(
            "/api/contacts/contact-1/update",
            json!({
                "display_name": "Ada Byron",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101009"],
                "emails": ["ada.byron@mock.invalid"],
                "base_content_hash": base,
                "idempotency_key": "key-w2a",
                "tool_call_id": approval,
            }),
        )
        .await;
    assert_eq!(status, 200, "update: {updated}");
    assert_eq!(updated["contact_id"], "contact-1");
    assert_eq!(updated["display_name"], "Ada Byron");
    assert_eq!(updated["job_title"], "Countess");
    assert_eq!(updated["phones"], json!(["+15550101009"]));
    assert_eq!(updated["emails"], json!(["ada.byron@mock.invalid"]));
    assert_eq!(updated["replayed"], false);
    assert_eq!(updated["idempotency_key"], "key-w2a");
    assert_ne!(updated["content_hash"], base, "the hash must move");
    let live = h
        .source
        .contact_content("person-a")
        .unwrap()
        .expect("live card must exist");
    assert_eq!(live.display_name, "Ada Byron");
    assert_eq!(live.phones, vec!["+15550101009".to_string()]);
    assert_eq!(h.contacts_events("contact.updated").len(), 1);
    assert_eq!(h.contacts_effects().len(), 1);

    // An external edit (live card changes, stored row
    // does not): the same update with the now-stale base
    // is 409 with the fresh base hash — and the approval
    // is NOT burned.
    let external = ContactContent {
        display_name: "Ada External".to_string(),
        organization: "Analytical Engines".to_string(),
        job_title: "Countess".to_string(),
        phones: vec!["+15550101999".to_string()],
        emails: vec!["ada.byron@mock.invalid".to_string()],
    };
    h.source.edit_live("person-a", &external);
    let stale_base = content_hash_of(&h.ok_get("/api/contacts/contact-1").await);
    let conflict_approval = h
        .approved(
            "contacts.update",
            json!({
                "contact_id": "contact-1",
                "display_name": "Ada Byron II",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101010"],
                "emails": ["ada.byron@mock.invalid"],
            }),
        )
        .await;
    let (status, conflict) = h
        .post(
            "/api/contacts/contact-1/update",
            json!({
                "display_name": "Ada Byron II",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101010"],
                "emails": ["ada.byron@mock.invalid"],
                "base_content_hash": stale_base,
                "idempotency_key": "key-w2b",
                "tool_call_id": conflict_approval,
            }),
        )
        .await;
    assert_eq!(status, 409, "conflict: {conflict}");
    assert_eq!(conflict["error"], "contact_changed");
    let current_hash = contact_content_hash(
        "Ada External",
        "Analytical Engines",
        "Countess",
        &["+15550101999".to_string()],
        &["ada.byron@mock.invalid".to_string()],
    );
    assert_eq!(conflict["current_hash"], current_hash);
    assert_eq!(h.contacts_events("contact.updated").len(), 1, "no second event");
    assert_eq!(h.contacts_effects().len(), 1, "no second Effect");

    // Retry with the SAME (unconsumed) approval and the
    // fresh base: the approved content is unchanged, so
    // the digest still matches and the write succeeds.
    let (status, retried) = h
        .post(
            "/api/contacts/contact-1/update",
            json!({
                "display_name": "Ada Byron II",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101010"],
                "emails": ["ada.byron@mock.invalid"],
                "base_content_hash": current_hash,
                "idempotency_key": "key-w2b",
                "tool_call_id": conflict_approval,
            }),
        )
        .await;
    assert_eq!(status, 200, "retry: {retried}");
    assert_eq!(retried["display_name"], "Ada Byron II");
    assert_eq!(retried["replayed"], false);
    assert_eq!(h.contacts_events("contact.updated").len(), 2);
    assert_eq!(h.contacts_effects().len(), 2);

    // The live card is gone (deleted externally): the
    // update is 404, not a write against a ghost.
    h.source.remove_live("person-b");
    let base_b = content_hash_of(&h.ok_get("/api/contacts/contact-2").await);
    let gone_approval = h
        .approved(
            "contacts.update",
            json!({
                "contact_id": "contact-2",
                "display_name": "Bob II",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
            }),
        )
        .await;
    let (status, gone) = h
        .post(
            "/api/contacts/contact-2/update",
            json!({
                "display_name": "Bob II",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
                "base_content_hash": base_b,
                "idempotency_key": "key-w2c",
                "tool_call_id": gone_approval,
            }),
        )
        .await;
    assert_eq!(status, 404, "gone: {gone}");
    assert_eq!(gone["error"], "unknown contact");
}

// ---------------------------------------------------------------- W3

#[tokio::test]
async fn w3_delete_tombstone_and_sync_skip() {
    let h = Harness::serve().await;
    h.sync().await;

    let approval = h
        .approved("contacts.delete", json!({ "contact_id": "contact-2" }))
        .await;
    let (status, deleted) = h
        .post(
            "/api/contacts/contact-2/delete",
            json!({ "idempotency_key": "key-w3", "tool_call_id": approval }),
        )
        .await;
    assert_eq!(status, 200, "delete: {deleted}");
    assert_eq!(deleted["contact_id"], "contact-2");
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["replayed"], false);
    assert_eq!(deleted["idempotency_key"], "key-w3");
    assert!(deleted["effect_id"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(h
        .source
        .contact_content("person-b")
        .unwrap()
        .is_none(), "live card must be gone");
    assert_eq!(h.contacts_events("contact.deleted").len(), 1);
    assert_eq!(h.contacts_effects().len(), 1);
    assert_eq!(h.contacts_effects()[0].operation, "delete");
    assert_eq!(h.contacts_effects()[0].status, "succeeded");

    // The tombstone stays visible in list + get.
    let rows = h.ok_get("/api/contacts").await;
    assert_eq!(rows.as_array().unwrap().len(), 2);
    assert_eq!(rows[1]["deleted"], true);
    let one = h.ok_get("/api/contacts/contact-2").await;
    assert_eq!(one["deleted"], true);

    // The card is restored into the live enumeration
    // (same native id): the next sync must NOT
    // resurrect the tombstone — the MCL-2 W8 check.
    h.source.add_live(contact_view_b());
    let again = h.sync().await;
    assert_eq!(again["synced_count"], 0, "tombstone must not resurrect");
    assert_eq!(again["skipped_count"], 2);
    assert_eq!(again["total_count"], 2);
    let one = h.ok_get("/api/contacts/contact-2").await;
    assert_eq!(one["deleted"], true, "still a tombstone after sync");
    assert_eq!(h.contacts_events("contact.deleted").len(), 1, "no second event");

    // A re-delete with a NEW key is 200 replayed, no
    // second event, no second Effect — and needs no
    // approval at all (the tombstone is the proof).
    let (status, replayed) = h
        .post(
            "/api/contacts/contact-2/delete",
            json!({ "idempotency_key": "key-w3b" }),
        )
        .await;
    assert_eq!(status, 200, "re-delete: {replayed}");
    assert_eq!(replayed["deleted"], true);
    assert_eq!(replayed["replayed"], true);
    assert_eq!(h.contacts_events("contact.deleted").len(), 1);
    assert_eq!(h.contacts_effects().len(), 1);

    // An update on the tombstone is 404, never a
    // resurrection through the write path.
    let base = content_hash_of(&h.ok_get("/api/contacts/contact-2").await);
    let tomb_approval = h
        .approved(
            "contacts.update",
            json!({
                "contact_id": "contact-2",
                "display_name": "Bob II",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
            }),
        )
        .await;
    let (status, gone) = h
        .post(
            "/api/contacts/contact-2/update",
            json!({
                "display_name": "Bob II",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
                "base_content_hash": base,
                "idempotency_key": "key-w3c",
                "tool_call_id": tomb_approval,
            }),
        )
        .await;
    assert_eq!(status, 404, "update on tombstone: {gone}");
    assert_eq!(gone["error"], "unknown contact");
}

// ---------------------------------------------------------------- W4

fn create_body(display_name: &str, key: &str, approval: Option<&str>) -> Value {
    let mut body = json!({
        "display_name": display_name,
        "organization": "Starforce",
        "job_title": "Pilot",
        "phones": ["+15550101003"],
        "emails": ["carol@mock.invalid"],
        "idempotency_key": key,
    });
    if let Some(id) = approval {
        body["tool_call_id"] = json!(id);
    }
    body
}

#[tokio::test]
async fn w4_idempotency() {
    let h = Harness::serve().await;
    h.sync().await;

    // Create: same key + same content replays the
    // original row (no second card, no second Effect,
    // no second event) and needs no approval.
    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, first) = h.post("/api/contacts/create", create_body("Carol Danvers", "key-w4", Some(&approval))).await;
    assert_eq!(status, 200, "first create: {first}");
    assert_eq!(first["contact_id"], "contact-3");
    assert_eq!(first["replayed"], false);
    assert_eq!(h.contact_rows().len(), 3);

    let (status, replay) = h.post("/api/contacts/create", create_body("Carol Danvers", "key-w4", None)).await;
    assert_eq!(status, 200, "replay: {replay}");
    assert_eq!(replay["contact_id"], "contact-3");
    assert_eq!(replay["replayed"], true);
    assert_eq!(h.contact_rows().len(), 3, "replay builds no new row");
    assert_eq!(h.contacts_effects().len(), 1, "replay builds no new Effect");
    assert_eq!(h.contacts_events("contact.created").len(), 1, "replay emits no new event");
    assert!(h.source.contact_content("fake-person-3").unwrap().is_some());
    assert!(h.source.contact_content("fake-person-4").unwrap().is_none(), "replay must not write");

    // Same key + different content → 409.
    let (status, conflict) = h.post("/api/contacts/create", create_body("Carol Other", "key-w4", None)).await;
    assert_eq!(status, 409, "conflict: {conflict}");
    assert_eq!(conflict["error"], "idempotency_key_conflict");
    assert_eq!(h.contact_rows().len(), 3, "conflict must not write");

    // No key → 400.
    let approval2 = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Dave",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
            }),
        )
        .await;
    let (status, missing) = h
        .post(
            "/api/contacts/create",
            json!({
                "display_name": "Dave",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
                "tool_call_id": approval2,
            }),
        )
        .await;
    assert_eq!(status, 400, "missing key: {missing}");
    assert_eq!(missing["error"], "idempotency_key is required");

    // Update replay: same key + same content, no
    // approval needed.
    let base = content_hash_of(&h.ok_get("/api/contacts/contact-1").await);
    let update_body = |key: &str, approval: Option<&str>| {
        let mut body = json!({
            "display_name": "Ada Byron",
            "organization": "Analytical Engines",
            "job_title": "Countess",
            "phones": ["+15550101009"],
            "emails": ["ada.byron@mock.invalid"],
            "base_content_hash": base,
            "idempotency_key": key,
        });
        if let Some(id) = approval {
            body["tool_call_id"] = json!(id);
        }
        body
    };
    let update_approval = h
        .approved(
            "contacts.update",
            json!({
                "contact_id": "contact-1",
                "display_name": "Ada Byron",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101009"],
                "emails": ["ada.byron@mock.invalid"],
            }),
        )
        .await;
    let (status, first) = h
        .post("/api/contacts/contact-1/update", update_body("key-w4u", Some(&update_approval)))
        .await;
    assert_eq!(status, 200, "first update: {first}");
    assert_eq!(first["replayed"], false);
    let (status, replay) = h
        .post("/api/contacts/contact-1/update", update_body("key-w4u", None))
        .await;
    assert_eq!(status, 200, "update replay: {replay}");
    assert_eq!(replay["replayed"], true);
    assert_eq!(h.contacts_events("contact.updated").len(), 1);

    // Update with a missing key → 400.
    let (status, missing) = h
        .post(
            "/api/contacts/contact-1/update",
            json!({
                "display_name": "Ada Byron",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101009"],
                "emails": ["ada.byron@mock.invalid"],
                "base_content_hash": content_hash_of(&h.ok_get("/api/contacts/contact-1").await),
                "tool_call_id": update_approval,
            }),
        )
        .await;
    assert_eq!(status, 400, "update missing key: {missing}");
    assert_eq!(missing["error"], "idempotency_key is required");

    // Delete replay: same key, no approval needed.
    let delete_approval = h
        .approved("contacts.delete", json!({ "contact_id": "contact-2" }))
        .await;
    let (status, first) = h
        .post(
            "/api/contacts/contact-2/delete",
            json!({ "idempotency_key": "key-w4d", "tool_call_id": delete_approval }),
        )
        .await;
    assert_eq!(status, 200, "first delete: {first}");
    assert_eq!(first["replayed"], false);
    let (status, replay) = h
        .post(
            "/api/contacts/contact-2/delete",
            json!({ "idempotency_key": "key-w4d" }),
        )
        .await;
    assert_eq!(status, 200, "delete replay: {replay}");
    assert_eq!(replay["replayed"], true);
    assert_eq!(h.contacts_events("contact.deleted").len(), 1);

    // Delete with a missing key → 400.
    let (status, missing) = h
        .post("/api/contacts/contact-1/delete", json!({ "tool_call_id": delete_approval }))
        .await;
    assert_eq!(status, 400, "delete missing key: {missing}");
    assert_eq!(missing["error"], "idempotency_key is required");
}

// ---------------------------------------------------------------- W5

#[tokio::test]
async fn w5_approval_gates() {
    let h = Harness::serve().await;
    h.sync().await;

    // No approval → 403 approval_required.
    let (status, body) = h
        .post("/api/contacts/create", create_body("Carol Danvers", "key-w5a", None))
        .await;
    assert_eq!(status, 403, "missing approval: {body}");
    assert_eq!(body["error"], "approval_required");

    // Rejected approval → 403 approval_rejected.
    let rejected = h
        .rejected(
            "contacts.create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, body) = h
        .post("/api/contacts/create", create_body("Carol Danvers", "key-w5b", Some(&rejected)))
        .await;
    assert_eq!(status, 403, "rejected: {body}");
    assert_eq!(body["error"], "approval_rejected");

    // Tool-only approval → 403 approval_binding_mismatch.
    let tool_only = h.approved_tool_only("contacts.create").await;
    let (status, body) = h
        .post("/api/contacts/create", create_body("Carol Danvers", "key-w5c", Some(&tool_only)))
        .await;
    assert_eq!(status, 403, "tool-only: {body}");
    assert_eq!(body["error"], "approval_binding_mismatch");

    // Digest mismatch → 409, and the approval stays
    // unconsumed: the same approval succeeds when the
    // presented content matches it.
    let mismatch_approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, body) = h
        .post("/api/contacts/create", create_body("Carol Other", "key-w5d", Some(&mismatch_approval)))
        .await;
    assert_eq!(status, 409, "digest mismatch: {body}");
    assert_eq!(body["error"], "approval_digest_mismatch");
    let (status, ok) = h
        .post("/api/contacts/create", create_body("Carol Danvers", "key-w5d", Some(&mismatch_approval)))
        .await;
    assert_eq!(status, 200, "same approval, matching content: {ok}");
    assert_eq!(ok["contact_id"], "contact-3");

    // Consumed approval → 409 approval_already_consumed.
    // The approval above was just consumed by the
    // successful create; a second write naming it (new
    // key, so no idempotency replay) is a 409.
    let (status, body) = h
        .post("/api/contacts/create", create_body("Dave", "key-w5e", Some(&mismatch_approval)))
        .await;
    assert_eq!(status, 409, "consumed: {body}");
    assert_eq!(body["error"], "approval_already_consumed");

    // Wrong-tool approval → 403 approval_required (the
    // peek only accepts the exact tool).
    let create_approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Eve",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
            }),
        )
        .await;
    let base = content_hash_of(&h.ok_get("/api/contacts/contact-1").await);
    let (status, body) = h
        .post(
            "/api/contacts/contact-1/update",
            json!({
                "display_name": "Ada Byron",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101009"],
                "emails": ["ada.byron@mock.invalid"],
                "base_content_hash": base,
                "idempotency_key": "key-w5f",
                "tool_call_id": create_approval,
            }),
        )
        .await;
    assert_eq!(status, 403, "wrong tool: {body}");
    assert_eq!(body["error"], "approval_required");

    // No row, no event, no Effect from any rejected
    // gate above.
    assert_eq!(h.contact_rows().len(), 3);
    assert_eq!(h.contacts_events("contact.created").len(), 1);
    assert_eq!(h.contacts_effects().len(), 1);
}

// ---------------------------------------------------------------- W6

#[tokio::test]
async fn w6_disconnect_rejects_writes_reads_unaffected() {
    let h = Harness::serve().await;
    h.sync().await;
    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, _) = h.post("/sources/contacts-source/disconnect", json!({})).await;
    assert_eq!(status, 200, "disconnect");

    // Writes are 409 while disconnected — the gate runs
    // before the approval peek, so no tool_call_id is
    // needed to prove it.
    let (status, body) = h.post("/api/contacts/create", create_body("Carol Danvers", "key-w6", None)).await;
    assert_eq!(status, 409, "create while disconnected: {body}");
    assert_eq!(body["error"], "source_disconnected");
    let base = content_hash_of(&h.ok_get("/api/contacts/contact-1").await);
    let (status, body) = h
        .post(
            "/api/contacts/contact-1/update",
            json!({
                "display_name": "Ada Byron",
                "organization": "Analytical Engines",
                "job_title": "Countess",
                "phones": ["+15550101009"],
                "emails": ["ada.byron@mock.invalid"],
                "base_content_hash": base,
                "idempotency_key": "key-w6u",
            }),
        )
        .await;
    assert_eq!(status, 409, "update while disconnected: {body}");
    assert_eq!(body["error"], "source_disconnected");
    let (status, body) = h
        .post("/api/contacts/contact-2/delete", json!({ "idempotency_key": "key-w6d" }))
        .await;
    assert_eq!(status, 409, "delete while disconnected: {body}");
    assert_eq!(body["error"], "source_disconnected");
    assert_eq!(h.contacts_effects().len(), 0, "no Effect was planned");

    // Reads are unaffected by the disconnect.
    let rows = h.ok_get("/api/contacts").await;
    assert_eq!(rows.as_array().unwrap().len(), 2);

    // Reconnect: the same approved write goes through.
    let (status, _) = h.post("/sources/contacts-source/reconnect", json!({})).await;
    assert_eq!(status, 200, "reconnect");
    let (status, created) = h
        .post("/api/contacts/create", create_body("Carol Danvers", "key-w6", Some(&approval)))
        .await;
    assert_eq!(status, 200, "create after reconnect: {created}");
    assert_eq!(created["contact_id"], "contact-3");
}

// ---------------------------------------------------------------- W7

#[tokio::test]
async fn w7_error_mapping() {
    let h = Harness::serve().await;
    h.sync().await;

    // NotRunning → 502, Effect failed.
    h.source.set_write_mode(FakeMode::NotRunning);
    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, body) = h.post("/api/contacts/create", create_body("Carol Danvers", "key-w7a", Some(&approval))).await;
    assert_eq!(status, 502, "not running: {body}");
    assert_eq!(body["error"], "Contacts is not running");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effects = h.contacts_effects();
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0].effect_id, effect_id);
    assert_eq!(effects[0].status, "failed");
    assert_eq!(h.contact_rows().len(), 2, "no row on failure");
    h.source.set_write_mode(FakeMode::Ok);

    // Denied → 403, Effect failed.
    h.source.set_write_mode(FakeMode::Denied);
    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Dave",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
            }),
        )
        .await;
    let (status, body) = h
        .post(
            "/api/contacts/create",
            json!({
                "display_name": "Dave",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
                "idempotency_key": "key-w7b",
                "tool_call_id": approval,
            }),
        )
        .await;
    assert_eq!(status, 403, "denied: {body}");
    assert_eq!(body["error"], "contacts automation denied");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.contacts_effects().len(), 2);
    assert_eq!(h.contacts_effects()[1].effect_id, effect_id);
    assert_eq!(h.contacts_effects()[1].status, "failed");
    h.source.set_write_mode(FakeMode::Ok);

    // Timeout → 502, Effect unknown, and an open
    // unknown_requires_reconciliation Inbox row names
    // the Effect.
    h.source.set_write_mode(FakeMode::Timeout);
    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Eve",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
            }),
        )
        .await;
    let (status, body) = h
        .post(
            "/api/contacts/create",
            json!({
                "display_name": "Eve",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
                "idempotency_key": "key-w7c",
                "tool_call_id": approval,
            }),
        )
        .await;
    assert_eq!(status, 502, "timeout: {body}");
    assert_eq!(body["error"], "contacts create timed out");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.contacts_effects().len(), 3);
    assert_eq!(h.contacts_effects()[2].effect_id, effect_id);
    assert_eq!(h.contacts_effects()[2].status, "unknown");
    let inbox = h.ok_get("/inbox/list?open_only=true").await;
    let open: Vec<&Value> = inbox
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["kind"] == "unknown_requires_reconciliation")
        .collect();
    assert_eq!(open.len(), 1, "one open unknown row: {inbox}");
    assert_eq!(open[0]["effect_id"], effect_id);
    h.source.set_write_mode(FakeMode::Ok);

    // Unavailable → 501, Effect failed.
    h.source.set_write_mode(FakeMode::Unavailable);
    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Frank",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
            }),
        )
        .await;
    let (status, body) = h
        .post(
            "/api/contacts/create",
            json!({
                "display_name": "Frank",
                "organization": "",
                "job_title": "",
                "phones": [],
                "emails": [],
                "idempotency_key": "key-w7d",
                "tool_call_id": approval,
            }),
        )
        .await;
    assert_eq!(status, 501, "unavailable: {body}");
    assert_eq!(body["error"], "contacts source unavailable in tests");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.contacts_effects().len(), 4);
    assert_eq!(h.contacts_effects()[3].effect_id, effect_id);
    assert_eq!(h.contacts_effects()[3].status, "failed");
    assert_eq!(h.contact_rows().len(), 2, "no row on any failure");
    h.source.set_write_mode(FakeMode::Ok);
}

// ---------------------------------------------------------------- W8

#[tokio::test]
async fn w8_redaction() {
    let h = Harness::serve().await;
    h.sync().await;

    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, created) = h.post("/api/contacts/create", create_body("Carol Danvers", "key-w8", Some(&approval))).await;
    assert_eq!(status, 200, "create: {created}");
    let base = content_hash_of(&h.ok_get("/api/contacts/contact-3").await);
    let update_approval = h
        .approved(
            "contacts.update",
            json!({
                "contact_id": "contact-3",
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Major",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, updated) = h
        .post(
            "/api/contacts/contact-3/update",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Major",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
                "base_content_hash": base,
                "idempotency_key": "key-w8u",
                "tool_call_id": update_approval,
            }),
        )
        .await;
    assert_eq!(status, 200, "update: {updated}");
    let delete_approval = h
        .approved("contacts.delete", json!({ "contact_id": "contact-3" }))
        .await;
    let (status, deleted) = h
        .post(
            "/api/contacts/contact-3/delete",
            json!({ "idempotency_key": "key-w8d", "tool_call_id": delete_approval }),
        )
        .await;
    assert_eq!(status, 200, "delete: {deleted}");

    // No contact.* WRITE event payload carries PII
    // plaintext; every one carries hashes / counts /
    // ids only. (Synced events use the 13-field
    // `synced_payload`, which has neither content_hash
    // nor execution_id by design — the write payloads
    // are the ones that must carry them.)
    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    let pii = [
        "Carol Danvers", "Starforce", "Pilot", "Major",
        "+15550101003", "carol@mock.invalid",
    ];
    let mut contact_events = 0;
    for event in &events {
        if !matches!(
            event.event_type.as_str(),
            "contact.created" | "contact.updated" | "contact.deleted"
        ) {
            continue;
        }
        contact_events += 1;
        for secret in pii {
            assert!(
                !event.payload_json.contains(secret),
                "{} payload leaks {secret}: {}",
                event.event_type,
                event.payload_json
            );
        }
        let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
        assert!(payload["content_hash"].as_str().is_some_and(|s| !s.is_empty()));
        assert!(payload["execution_id"].as_str().is_some_and(|s| !s.is_empty()));
    }
    assert_eq!(contact_events, 3, "one event per write");

    // The list response still carries plaintext (the C5
    // distinction): privacy lives in events, not in the
    // user's own rows.
    let rows = h.ok_get("/api/contacts").await;
    assert_eq!(rows[2]["display_name"], "Carol Danvers");
    assert_eq!(rows[2]["phones"], json!(["+15550101003"]));
}

// ---------------------------------------------------------------- W9

#[tokio::test]
async fn w9_registration() {
    // The three write tools require approval — and they
    // are NOT loop tools: the loop whitelist rejects
    // them, so no agent can invoke a contacts write
    // without an approval.
    assert_eq!(tool_requires_approval("contacts.create"), Some(true));
    assert_eq!(tool_requires_approval("contacts.update"), Some(true));
    assert_eq!(tool_requires_approval("contacts.delete"), Some(true));
    assert_eq!(tool_requires_approval("contacts.search"), Some(false));
    for tool in ["contacts.create", "contacts.update", "contacts.delete"] {
        assert!(
            mock_gateway::loop_engine::normalize(tool, &json!({})).is_err(),
            "{tool} must not be a loop tool"
        );
    }
}

// ---------------------------------------------------------------- W10

#[tokio::test]
async fn w10_restart_persists_and_replays() {
    let mut h = Harness::serve().await;
    h.sync().await;
    let approval = h
        .approved(
            "contacts.create",
            json!({
                "display_name": "Carol Danvers",
                "organization": "Starforce",
                "job_title": "Pilot",
                "phones": ["+15550101003"],
                "emails": ["carol@mock.invalid"],
            }),
        )
        .await;
    let (status, created) = h.post("/api/contacts/create", create_body("Carol Danvers", "key-w10", Some(&approval))).await;
    assert_eq!(status, 200, "create: {created}");
    let created_hash = created["content_hash"].as_str().unwrap().to_string();

    h.restart().await;

    // The row, its idempotency key and its events
    // survive the restart.
    let detail = h.ok_get("/api/contacts/contact-3").await;
    assert_eq!(detail["display_name"], "Carol Danvers");
    assert_eq!(detail["idempotency_key"], "key-w10");
    assert_eq!(detail["content_hash"], created_hash);
    assert_eq!(h.contacts_events("contact.created").len(), 1);

    // A replay after the restart returns the original
    // row, no approval needed, no second Effect.
    let (status, replay) = h.post("/api/contacts/create", create_body("Carol Danvers", "key-w10", None)).await;
    assert_eq!(status, 200, "replay after restart: {replay}");
    assert_eq!(replay["contact_id"], "contact-3");
    assert_eq!(replay["replayed"], true);
    assert_eq!(h.contacts_effects().len(), 1);
    assert_eq!(h.contact_rows().len(), 3);
}

// F-1：通过真实 HTTP 审批链创建测试事实，后续重放不再提供审批。
async fn f1_create(h: &Harness, key: &str) -> Value {
    let mut body = create_body("F1 Person", key, None);
    let approval = h.approved("contacts.create", body.clone()).await;
    body["tool_call_id"] = json!(approval);
    h.ok("/api/contacts/create", body).await
}

async fn f1_update(h: &Harness, row: &Value, key: &str, name: &str) -> Value {
    let id = row["contact_id"].as_str().unwrap();
    let mut body = create_body(name, key, None);
    body["contact_id"] = json!(id);
    body["base_content_hash"] = row["content_hash"].clone();
    let approval = h.approved("contacts.update", body.clone()).await;
    body["tool_call_id"] = json!(approval);
    h.ok(&format!("/api/contacts/{id}/update"), body).await
}

async fn f1_delete(h: &Harness, id: &str, key: &str) -> Value {
    let approval = h
        .approved("contacts.delete", json!({"contact_id": id}))
        .await;
    h.ok(
        &format!("/api/contacts/{id}/delete"),
        json!({"idempotency_key": key, "tool_call_id": approval}),
    )
    .await
}

fn f1_counts(h: &Harness) -> (usize, usize, usize, usize, usize) {
    (
        h.contact_rows().len(),
        h.contacts_effects().len(),
        h.contacts_events("contact.created").len(),
        h.contacts_events("contact.updated").len(),
        h.contacts_events("contact.deleted").len(),
    )
}

#[tokio::test]
async fn w11_create_delete_restart_replay() {
    let mut h = Harness::serve().await;
    let row = f1_create(&h, "w11-create").await;
    let id = row["contact_id"].as_str().unwrap();
    let tombstone = f1_delete(&h, id, "w11-delete").await;
    let before = f1_counts(&h);
    h.restart().await;
    let replay = h
        .ok(
            "/api/contacts/create",
            create_body("F1 Person", "w11-create", None),
        )
        .await;
    assert_eq!(replay["contact_id"], row["contact_id"]);
    assert_eq!(replay["deleted"], true);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["effect_id"], tombstone["effect_id"]);
    assert_eq!(f1_counts(&h), before);
    let (status, conflict) = h
        .post(
            "/api/contacts/create",
            create_body("Changed", "w11-create", None),
        )
        .await;
    assert_eq!(status, 409);
    assert_eq!(conflict["error"], "idempotency_key_conflict");
    assert_eq!(f1_counts(&h), before);
    let delete_replay = h
        .ok(
            &format!("/api/contacts/{id}/delete"),
            json!({"idempotency_key":"w11-delete"}),
        )
        .await;
    assert_eq!(delete_replay["replayed"], true);
    assert_eq!(f1_counts(&h), before);
}

#[tokio::test]
async fn w12_multiple_updates_replay_current_row() {
    let mut h = Harness::serve().await;
    let row = f1_create(&h, "w12-create").await;
    let u1 = f1_update(&h, &row, "w12-u1", "First").await;
    let u2 = f1_update(&h, &u1, "w12-u2", "Second").await;
    let id = row["contact_id"].as_str().unwrap();
    let before = f1_counts(&h);
    assert_eq!(before, (1, 3, 1, 2, 0));
    h.restart().await;
    for (key, name, base) in [("w12-u1", "First", &row), ("w12-u2", "Second", &u1)] {
        let mut body = create_body(name, key, None);
        body["base_content_hash"] = base["content_hash"].clone();
        let replay = h
            .ok(&format!("/api/contacts/{id}/update"), body.clone())
            .await;
        assert_eq!(replay["replayed"], true);
        assert_eq!(replay["content_hash"], u2["content_hash"]);
        assert_eq!(replay["display_name"], "Second");
        body["display_name"] = json!("Wrong");
        let (status, conflict) = h.post(&format!("/api/contacts/{id}/update"), body).await;
        assert_eq!(status, 409);
        assert_eq!(conflict["error"], "idempotency_key_conflict");
        assert_eq!(f1_counts(&h), before);
    }
    // 创建键在多次更新之后仍有效，返回当前内容。
    let replay = h
        .ok(
            "/api/contacts/create",
            create_body("F1 Person", "w12-create", None),
        )
        .await;
    assert_eq!(replay["display_name"], "Second");
    assert_eq!(f1_counts(&h), before);
}

#[tokio::test]
async fn w13_update_replay_after_delete_and_cross_contact_conflict() {
    let mut h = Harness::serve().await;
    let row = f1_create(&h, "w13-create").await;
    let id = row["contact_id"].as_str().unwrap();
    f1_update(&h, &row, "w13-update", "Updated").await;
    f1_delete(&h, id, "w13-delete").await;
    let other = f1_create(&h, "w13-other").await;
    let before = f1_counts(&h);
    h.restart().await;
    let mut body = create_body("Updated", "w13-update", None);
    body["base_content_hash"] = row["content_hash"].clone();
    let replay = h
        .ok(&format!("/api/contacts/{id}/update"), body.clone())
        .await;
    assert_eq!(replay["deleted"], true);
    assert_eq!(replay["replayed"], true);
    let other_id = other["contact_id"].as_str().unwrap();
    let (status, conflict) = h
        .post(&format!("/api/contacts/{other_id}/update"), body.clone())
        .await;
    assert_eq!(status, 409);
    assert_eq!(conflict["error"], "idempotency_key_conflict");
    // 同键跨操作也冲突；新的墓碑更新仍是404。
    let (status, conflict) = h
        .post(
            &format!("/api/contacts/{id}/delete"),
            json!({"idempotency_key":"w13-update"}),
        )
        .await;
    assert_eq!(status, 409);
    assert_eq!(conflict["error"], "idempotency_key_conflict");
    body["idempotency_key"] = json!("w13-new");
    let (status, _) = h.post(&format!("/api/contacts/{id}/update"), body).await;
    assert_eq!(status, 404);
    assert_eq!(f1_counts(&h), before);
}

#[tokio::test]
async fn f1_v18_migration_is_typed_repeatable_and_does_not_backfill() {
    let h = Harness::serve().await;
    let row = f1_create(&h, "legacy-key").await;
    let before = f1_counts(&h);
    let mut conn = h.db();
    // 仅在隔离测试库模拟v17；生产旧库不回填残存最新键。
    conn.execute_batch("DROP TABLE contact_idempotency; UPDATE db_meta SET value='17' WHERE key='db_schema_version';").unwrap();
    sqlite_store::apply_migrations(&mut conn).unwrap();
    sqlite_store::apply_migrations(&mut conn).unwrap();
    assert_eq!(sqlite_store::schema_version(&conn).unwrap(), 18);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM contact_idempotency", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    let ty: String = conn
        .query_row(
            "SELECT type FROM pragma_table_info('contact_idempotency') WHERE name='created_ms'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(ty, "INTEGER");
    assert_eq!(
        h.ok_get(&format!(
            "/api/contacts/{}",
            row["contact_id"].as_str().unwrap()
        ))
        .await["idempotency_key"],
        "legacy-key"
    );
    assert_eq!(f1_counts(&h), before);
}

#[tokio::test]
async fn f1_index_failure_rolls_back_contact_and_event() {
    use mock_gateway::contacts_sync::{
        record_contact_created, record_contact_deleted, record_contact_updated,
    };
    let h = Harness::serve().await;
    let row = f1_create(&h, "rollback-seed").await;
    let mut conn = h.db();
    let stored =
        mock_gateway::contacts_sync::get_contact(&conn, row["contact_id"].as_str().unwrap())
            .unwrap()
            .unwrap();
    let before = f1_counts(&h);
    let content = ContactContent::normalized("Changed", "", "", &[], &[]);
    conn.execute_batch("CREATE TRIGGER reject_contact_key BEFORE INSERT ON contact_idempotency BEGIN SELECT RAISE(ABORT, 'test injected failure'); END;").unwrap();
    assert!(record_contact_created(
        &mut conn,
        "new-native",
        &content,
        "fail-create",
        "e",
        "x",
        "p",
        "test"
    )
    .is_err());
    assert!(record_contact_updated(
        &mut conn,
        &stored,
        &content,
        "fail-update",
        "e",
        "x",
        "p",
        "test"
    )
    .is_err());
    assert!(
        record_contact_deleted(&mut conn, &stored, "fail-delete", "e", "x", "p", "test").is_err()
    );
    assert_eq!(f1_counts(&h), before);
    let after = mock_gateway::contacts_sync::get_contact(&conn, &stored.contact_id)
        .unwrap()
        .unwrap();
    assert_eq!(after.content_hash, stored.content_hash);
    assert!(!after.deleted);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM contact_idempotency", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}
