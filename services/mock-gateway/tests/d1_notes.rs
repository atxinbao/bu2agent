//! D1-6b-4 tests (plan §3): real Notes read + write
//! end to end — digest-bound approval + write
//! Execution + single-tool Mandate + tombstone +
//! `note.*` events + Effect, append conflict 409,
//! redaction, and error mapping.
//!
//! HTTP routes only, plus `sqlite_store` reads (note
//! rows / effects / mandates / events / inbox) where
//! HTTP cannot reach the state. The Notes source is a
//! test-only fake (no AppleScript, no real Notes.app):
//! it keeps seeded folders of notes, creates /
//! replaces / deletes by native id, and can be
//! switched into the not-running / denied / timeout /
//! unavailable failure modes. The fake stores
//! plaintext verbatim — the real Notes.app stores HTML
//! and the gateway converts (paragraph-per-line); the
//! fake's simplification is documented on
//! `FakeNotesSource`, and the HTML conversion itself is
//! guarded by the `plain_text_to_html` unit tests in
//! `notes_source.rs`. Window validation on a real Mac
//! is the proof of the real AppleScript forms.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::notes_source::{
    CreatedNote, NoteView, NotesError, NotesSource, NOTES_SOURCE_ID,
};
use mock_gateway::notes_sync::{get_note_row, list_notes, NoteRow};
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

/// Test-only fake Notes source: seeded folders of
/// notes. `create_note` stores the body as plaintext
/// verbatim (the real Notes.app stores HTML; the
/// gateway's paragraph-per-line conversion is guarded
/// separately in `notes_source.rs`). `set_body` replaces
/// the stored plaintext whole. `delete_note` removes
/// the note from its folder. A locked note's plaintext
/// is still readable here — the lock is modelled by
/// the `password_protected` flag, which the sync and
/// append paths check; the real Notes.app refuses to
/// return a locked note's body at all.
struct FakeNotesState {
    mode: FakeMode,
    folders: HashMap<String, Vec<NoteView>>,
    create_count: usize,
    set_count: usize,
    delete_count: usize,
    next_native: usize,
}

#[derive(Clone)]
struct FakeNotesSource(Arc<Mutex<FakeNotesState>>);

impl FakeNotesSource {
    fn new(seeds: Vec<NoteView>) -> Self {
        let mut folders: HashMap<String, Vec<NoteView>> = HashMap::new();
        for seed in seeds {
            folders.entry(seed.folder.clone()).or_default().push(seed);
        }
        Self(Arc::new(Mutex::new(FakeNotesState {
            mode: FakeMode::Ok,
            folders,
            create_count: 0,
            set_count: 0,
            delete_count: 0,
            next_native: 0,
        })))
    }

    fn set_mode(&self, mode: FakeMode) {
        self.0.lock().unwrap().mode = mode;
    }

    fn check(&self) -> Result<(), NotesError> {
        match self.0.lock().unwrap().mode {
            FakeMode::Ok => Ok(()),
            FakeMode::NotRunning => Err(NotesError::NotRunning),
            FakeMode::Denied => Err(NotesError::Denied),
            FakeMode::Timeout => Err(NotesError::Timeout),
            FakeMode::Unavailable => Err(NotesError::Unavailable(
                "notes unavailable on this platform".to_string(),
            )),
        }
    }

    fn create_count(&self) -> usize {
        self.0.lock().unwrap().create_count
    }

    fn set_count(&self) -> usize {
        self.0.lock().unwrap().set_count
    }

    fn delete_count(&self) -> usize {
        self.0.lock().unwrap().delete_count
    }

    fn plaintext(&self, folder: &str, native_id: &str) -> Option<String> {
        self.0
            .lock()
            .unwrap()
            .folders
            .get(folder)
            .and_then(|notes| {
                notes
                    .iter()
                    .find(|n| n.native_id == native_id)
                    .map(|n| n.plaintext.clone())
            })
    }

    fn note_ids(&self, folder: &str) -> Vec<String> {
        self.0
            .lock()
            .unwrap()
            .folders
            .get(folder)
            .map(|notes| notes.iter().map(|n| n.native_id.clone()).collect())
            .unwrap_or_default()
    }

    /// Simulate an edit made in Notes.app outside the
    /// gateway (the NT3 conflict setup): change the
    /// stored plaintext directly.
    fn set_plaintext(&self, folder: &str, native_id: &str, text: &str) -> bool {
        let mut state = self.0.lock().unwrap();
        let Some(notes) = state.folders.get_mut(folder) else {
            return false;
        };
        let Some(note) = notes.iter_mut().find(|n| n.native_id == native_id) else {
            return false;
        };
        note.plaintext = text.to_string();
        true
    }

    /// Simulate locking a live note in Notes.app (the
    /// NT5 locked-append setup).
    fn set_locked(&self, folder: &str, native_id: &str, locked: bool) -> bool {
        let mut state = self.0.lock().unwrap();
        let Some(notes) = state.folders.get_mut(folder) else {
            return false;
        };
        let Some(note) = notes.iter_mut().find(|n| n.native_id == native_id) else {
            return false;
        };
        note.password_protected = locked;
        true
    }

    /// Simulate restoring a note from Recently Deleted
    /// back into its folder (the NT4 anti-resurrect
    /// setup): the note is live again, but its fact row
    /// is tombstoned — a sync must still skip it.
    fn restore_note(&self, view: NoteView) {
        let mut state = self.0.lock().unwrap();
        let notes = state.folders.entry(view.folder.clone()).or_default();
        if !notes.iter().any(|n| n.native_id == view.native_id) {
            notes.push(view);
        }
    }
}

impl NotesSource for FakeNotesSource {
    fn folder_exists(&self, folder: &str) -> Result<bool, NotesError> {
        self.check()?;
        Ok(self.0.lock().unwrap().folders.contains_key(folder))
    }

    fn list_notes(&self, folder: &str) -> Result<Vec<NoteView>, NotesError> {
        self.check()?;
        self.0
            .lock()
            .unwrap()
            .folders
            .get(folder)
            .cloned()
            .ok_or(NotesError::NotFound)
    }

    fn get_note(&self, folder: &str, native_id: &str) -> Result<Option<NoteView>, NotesError> {
        self.check()?;
        Ok(self
            .0
            .lock()
            .unwrap()
            .folders
            .get(folder)
            .and_then(|notes| notes.iter().find(|n| n.native_id == native_id).cloned()))
    }

    fn create_note(
        &self,
        folder: &str,
        name: &str,
        body: &str,
    ) -> Result<CreatedNote, NotesError> {
        self.check()?;
        let mut state = self.0.lock().unwrap();
        if !state.folders.contains_key(folder) {
            return Err(NotesError::NotFound);
        }
        state.next_native += 1;
        state.create_count += 1;
        let native_id = format!("fake-{}", state.next_native);
        let view = NoteView {
            native_id: native_id.clone(),
            folder: folder.to_string(),
            name: name.to_string(),
            plaintext: body.to_string(),
            password_protected: false,
        };
        state
            .folders
            .get_mut(folder)
            .expect("folder present")
            .push(view);
        Ok(CreatedNote {
            native_id,
            name: name.to_string(),
            plaintext: body.to_string(),
        })
    }

    fn set_body(&self, folder: &str, native_id: &str, body: &str) -> Result<String, NotesError> {
        self.check()?;
        let mut state = self.0.lock().unwrap();
        let Some(notes) = state.folders.get_mut(folder) else {
            return Err(NotesError::NotFound);
        };
        let Some(note) = notes.iter_mut().find(|n| n.native_id == native_id) else {
            return Err(NotesError::NotFound);
        };
        note.plaintext = body.to_string();
        state.set_count += 1;
        Ok(body.to_string())
    }

    fn delete_note(&self, folder: &str, native_id: &str) -> Result<(), NotesError> {
        self.check()?;
        let mut state = self.0.lock().unwrap();
        let Some(notes) = state.folders.get_mut(folder) else {
            return Err(NotesError::NotFound);
        };
        let Some(pos) = notes.iter().position(|n| n.native_id == native_id) else {
            return Err(NotesError::NotFound);
        };
        notes.remove(pos);
        state.delete_count += 1;
        Ok(())
    }
}

fn seed_note(native_id: &str, name: &str, plaintext: &str) -> NoteView {
    NoteView {
        native_id: native_id.to_string(),
        folder: "bu2agent-test".to_string(),
        name: name.to_string(),
        plaintext: plaintext.to_string(),
        password_protected: false,
    }
}

fn seed_locked(native_id: &str, name: &str) -> NoteView {
    NoteView {
        native_id: native_id.to_string(),
        folder: "bu2agent-test".to_string(),
        name: name.to_string(),
        plaintext: String::new(),
        password_protected: true,
    }
}

fn build_state(dir: &Path, notes: Option<Arc<dyn NotesSource>>) -> AppState {
    let mut state = AppState::with_files_root_and_approvals(
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
    .with_worktree_state_path(dir.join("worktrees.jsonl"));
    if let Some(notes) = notes {
        state = state.with_notes_source(notes);
    }
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
    notes: Arc<FakeNotesSource>,
}

impl Harness {
    async fn serve() -> Self {
        Self::serve_with(vec![
            seed_note("n-1", "Alpha plan", "Alpha body hunter2"),
            seed_note("n-2", "Beta", "Beta body"),
            seed_locked("n-3", "Locked vault"),
        ])
        .await
    }

    async fn serve_with(seeds: Vec<NoteView>) -> Self {
        let dir = temp_dir("notes");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let notes = Arc::new(FakeNotesSource::new(seeds));
        let state = build_state(&dir, Some(notes.clone() as Arc<dyn NotesSource>));
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
            notes,
        }
    }

    /// Serve with the production default source (the
    /// non-macOS stub on Linux): every Notes call 501s.
    /// The fake is kept in the struct but never wired.
    async fn serve_default() -> Self {
        let dir = temp_dir("notes-default");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let notes = Arc::new(FakeNotesSource::new(vec![]));
        let state = build_state(&dir, None);
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
            notes,
        }
    }

    /// Restart the gateway from the same directory (the
    /// NT1 reader/restart check): the DB, the fake
    /// folders, and the tombstones all survive.
    async fn restart(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        let state = build_state(&self.dir, Some(self.notes.clone() as Arc<dyn NotesSource>));
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

    fn note_rows(&self) -> Vec<NoteRow> {
        let conn = self.db();
        list_notes(&conn, NOTES_SOURCE_ID).expect("list notes")
    }

    fn note_row(&self, note_id: &str) -> Option<NoteRow> {
        let conn = self.db();
        get_note_row(&conn, note_id).expect("get note")
    }

    fn note_effects(&self) -> Vec<mock_gateway::effect_store::EffectFileRecord> {
        let conn = self.db();
        sqlite_store::list_effects(&conn)
            .expect("effects")
            .into_iter()
            .filter(|e| e.capability == "notes")
            .collect()
    }

    fn note_events(&self) -> Vec<sqlite_store::EventRow> {
        let conn = self.db();
        sqlite_store::list_events(&conn)
            .expect("events")
            .into_iter()
            .filter(|e| e.event_type.starts_with("note."))
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

    async fn sync(&self, folder: &str) -> (u16, Value) {
        self.post("/api/notes/sync", json!({"folder": folder})).await
    }

    /// Request a digest-bound approval for `tool` with
    /// the given presentation fields and decide it
    /// approved. Returns the tool_call_id.
    async fn approved(&self, tool: &str, fields: Value) -> String {
        let mut req = json!({"tool": tool});
        for (key, value) in fields.as_object().unwrap() {
            req[key] = value.clone();
        }
        let (status, requested) = self.post("/approval/request", req).await;
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

    /// Request a digest-bound approval for `tool` with
    /// the given presentation fields and decide it
    /// rejected. Returns the tool_call_id.
    async fn rejected(&self, tool: &str, fields: Value) -> String {
        let mut req = json!({"tool": tool});
        for (key, value) in fields.as_object().unwrap() {
            req[key] = value.clone();
        }
        let (status, requested) = self.post("/approval/request", req).await;
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

    async fn create_note(
        &self,
        folder: &str,
        name: &str,
        body: &str,
        key: &str,
        tool_call_id: Option<&str>,
    ) -> (u16, Value) {
        let mut req = json!({
            "folder": folder,
            "name": name,
            "body": body,
            "idempotency_key": key,
        });
        if let Some(id) = tool_call_id {
            req["tool_call_id"] = json!(id);
        }
        self.post("/api/notes/create", req).await
    }

    async fn append_note(
        &self,
        note_id: &str,
        text: &str,
        base_body_hash: &str,
        key: &str,
        tool_call_id: Option<&str>,
    ) -> (u16, Value) {
        let mut req = json!({
            "text": text,
            "base_body_hash": base_body_hash,
            "idempotency_key": key,
        });
        if let Some(id) = tool_call_id {
            req["tool_call_id"] = json!(id);
        }
        self.post(&format!("/api/notes/{note_id}/append"), req).await
    }

    async fn delete_note(&self, note_id: &str, tool_call_id: Option<&str>) -> (u16, Value) {
        // The notes delete route is POST (plan §3), not
        // DELETE — the `tool_call_id` travels as a query
        // parameter because the desktop `delete_json`
        // sends no body (the 6b-3 D-5 precedent).
        let path = match tool_call_id {
            Some(id) => format!("/api/notes/{note_id}/delete?tool_call_id={id}"),
            None => format!("/api/notes/{note_id}/delete"),
        };
        self.post(&path, json!({})).await
    }

    async fn verify(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/verify", json!({"effect_id": effect_id}))
            .await
    }
}

/// NT1: sync end to end — two unlocked notes sync to
/// rows + `note.synced` events with body hashes only,
/// the locked note is excluded and counted separately,
/// a second sync skips, unknown folder 404s, missing
/// folder 400s, rows + schema survive a restart, and
/// the fold holds.
#[tokio::test]
async fn nt1_sync_end_to_end() {
    let mut h = Harness::serve().await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    // 6b-4 bumps the schema to v13.
    assert_eq!(DB_SCHEMA_VERSION, 13);

    let (status, synced) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200, "sync: {synced}");
    assert_eq!(synced["synced_count"], 2);
    assert_eq!(synced["skipped_count"], 0);
    assert_eq!(synced["locked_count"], 1);
    assert_eq!(synced["total_count"], 2);
    assert_eq!(synced["cursor"].as_str().unwrap().len(), 64);

    let rows = h.note_rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].note_id, "note-1");
    assert_eq!(rows[0].source_id, "notes-source");
    assert_eq!(rows[0].native_id, "n-1");
    assert_eq!(rows[0].folder, "bu2agent-test");
    assert_eq!(rows[0].name, "Alpha plan");
    assert_eq!(rows[0].body_hash, sha256_hex("Alpha body hunter2".as_bytes()));
    assert!(!rows[0].deleted);
    assert_eq!(rows[1].note_id, "note-2");
    assert_eq!(rows[1].native_id, "n-2");
    assert_eq!(rows[1].body_hash, sha256_hex("Beta body".as_bytes()));

    // The body never lands in a row or an event (Q-N1);
    // the locked note never lands anywhere (Q-N2).
    let row_json = serde_json::to_string(&rows).unwrap();
    assert!(!row_json.contains("Alpha body hunter2"), "row leaks body");
    assert!(!row_json.contains("Locked vault"), "row leaks locked note");
    let (status, got) = h.get("/api/notes/note-1").await;
    assert_eq!(status, 200, "get: {got}");
    assert_eq!(got["name"], "Alpha plan");
    assert_eq!(got["body_hash"], sha256_hex("Alpha body hunter2".as_bytes()));
    assert!(got.get("plaintext").is_none(), "row must not carry plaintext");
    let (status, listed) = h.get("/api/notes").await;
    assert_eq!(status, 200, "list: {listed}");
    assert_eq!(listed.as_array().unwrap().len(), 2);
    let (status, missing) = h.get("/api/notes/note-99").await;
    assert_eq!(status, 404, "unknown get: {missing}");
    assert_eq!(missing["error"], "unknown note");

    // The events: exactly two `note.synced`, each with
    // exactly the seven whitelisted fields, no folder /
    // name / body plaintext anywhere.
    let events = h.note_events();
    assert_eq!(events.len(), 2);
    for event in &events {
        assert_eq!(event.aggregate_kind, "note");
        let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
        assert_eq!(
            payload.as_object().unwrap().len(),
            7,
            "no extra fields: {payload}"
        );
        assert_eq!(payload["folder_hash"], sha256_hex("bu2agent-test".as_bytes()));
        assert!(!event.payload_json.contains("Alpha plan"));
        assert!(!event.payload_json.contains("Alpha body hunter2"));
        assert!(!event.payload_json.contains("bu2agent-test"));
    }

    // A second sync skips both rows (locked still
    // counted separately, never synced).
    let (status, resynced) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200, "re-sync: {resynced}");
    assert_eq!(resynced["synced_count"], 0);
    assert_eq!(resynced["skipped_count"], 2);
    assert_eq!(resynced["locked_count"], 1);
    assert_eq!(resynced["total_count"], 2);
    assert_eq!(h.note_rows().len(), 2);
    assert_eq!(h.note_events().len(), 2, "no second sync event");

    // Unknown folder 404s; a missing folder is the
    // route's own 400 (Q-N4: folder must pre-exist).
    let (status, body) = h.sync("no-such-folder").await;
    assert_eq!(status, 404, "unknown folder: {body}");
    assert_eq!(body["error"], "unknown folder");
    let (status, body) = h.post("/api/notes/sync", json!({})).await;
    assert_eq!(status, 400, "missing folder: {body}");

    // Restart: rows + schema survive, fold holds.
    h.restart().await;
    assert_eq!(h.schema_version(), 13);
    assert_eq!(h.note_rows().len(), 2);
    assert_eq!(h.note_rows()[0].note_id, "note-1");
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));
}

/// NT2: create end to end — decided digest-bound
/// approval → 200, the fake stores the note, one row +
/// `note.created` event + succeeded Effect + Mandate,
/// verify reads back the hash → verified, same-key
/// replay 200s, same-key different-content 409s, and
/// the approval / digest gates hold.
#[tokio::test]
async fn nt2_create_end_to_end() {
    let h = Harness::serve().await;
    let (status, _) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200);

    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "d1-6b note", "body": "hello\nworld"}),
        )
        .await;
    let (status, created) = h
        .create_note("bu2agent-test", "d1-6b note", "hello\nworld", "key-create-1", Some(&approval))
        .await;
    assert_eq!(status, 200, "create: {created}");
    assert_eq!(created["note_id"], "note-3");
    assert_eq!(created["native_id"], "fake-1");
    assert_eq!(created["folder"], "bu2agent-test");
    assert_eq!(created["name"], "d1-6b note");
    assert_eq!(created["body_hash"], sha256_hex("hello\nworld".as_bytes()));
    assert_eq!(created["replayed"], false);
    let effect_id = created["effect_id"].as_str().unwrap().to_string();

    // The fake stored the note once, with the exact
    // plaintext (its verbatim-plaintext simplification).
    assert_eq!(h.notes.create_count(), 1);
    assert_eq!(
        h.notes.plaintext("bu2agent-test", "fake-1").as_deref(),
        Some("hello\nworld")
    );

    // The Effect (notes, create, key-create-1)
    // succeeded; the Mandate (Q-D1-6b-23): one row on
    // the write Execution, tools = the single write
    // tool, goal empty.
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "succeeded");
    assert_eq!(effect.operation, "create");
    assert_eq!(effect.scope, "key-create-1");
    let execution_id = effect.execution_id.clone().expect("execution");
    let conn = h.db();
    let mandates = sqlite_store::list_mandates(&conn, None, Some(&execution_id)).expect("mandates");
    assert_eq!(mandates.len(), 1, "one write mandate");
    let allowed: Value = serde_json::from_str(&mandates[0].allowed_json).expect("allowed_json");
    assert_eq!(allowed["tools"], json!(["notes.create"]));
    assert_eq!(allowed["goal_id"], Value::Null);
    assert_eq!(allowed["entry"], "direct-write");
    drop(conn);

    // The event: exactly one `note.created`, payload
    // only the eight whitelisted fields.
    let events = h.note_events();
    assert_eq!(events.len(), 3, "2 synced + 1 created");
    let created_event = events
        .iter()
        .find(|e| e.event_type == "note.created")
        .expect("note.created");
    let payload: Value = serde_json::from_str(&created_event.payload_json).unwrap();
    assert_eq!(payload.as_object().unwrap().len(), 8, "no extra fields: {payload}");
    assert_eq!(payload["note_id"], "note-3");
    assert_eq!(payload["native_id"], "fake-1");
    assert_eq!(payload["execution_id"], execution_id);
    assert!(!created_event.payload_json.contains("hello\nworld"));

    // Verify reads back: the live hash matches the
    // fact row → verified, fields_match.
    let (status, verified) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "verify: {verified}");
    assert_eq!(verified["verdict"], "verified");
    let conn = h.db();
    let evidence = sqlite_store::list_evidence(&conn).expect("evidence");
    assert_eq!(evidence.len(), 1);
    assert_eq!(evidence[0].verdict, "verified");
    assert_eq!(evidence[0].reason, "fields_match");
    drop(conn);

    // Same key + same content replays 200 with no
    // second create, no second Effect — and needs no
    // approval (the replay check precedes the peek).
    let (status, replayed) = h
        .create_note("bu2agent-test", "d1-6b note", "hello\nworld", "key-create-1", None)
        .await;
    assert_eq!(status, 200, "replay: {replayed}");
    assert_eq!(replayed["note_id"], "note-3");
    assert_eq!(replayed["replayed"], true);
    assert_eq!(h.notes.create_count(), 1);
    assert_eq!(h.note_effects().len(), 1);

    // Same key + different content → 409.
    let (status, body) = h
        .create_note("bu2agent-test", "d1-6b note", "different body", "key-create-1", None)
        .await;
    assert_eq!(status, 409, "key conflict: {body}");
    assert_eq!(body["error"], "idempotency_key_conflict");
    assert_eq!(h.notes.create_count(), 1);

    // Unknown folder → 404, nothing burned.
    let wrong_folder = h
        .approved(
            "notes.create",
            json!({"folder": "no-such-folder", "name": "x", "body": "y"}),
        )
        .await;
    let (status, body) = h
        .create_note("no-such-folder", "x", "y", "key-create-2", Some(&wrong_folder))
        .await;
    assert_eq!(status, 404, "unknown folder: {body}");
    assert_eq!(body["error"], "unknown folder");
    assert_eq!(h.notes.create_count(), 1);
    assert_eq!(h.note_effects().len(), 1, "no orphaned Effect");
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));
}

/// NT2 (gates): no approval 403, rejected 403,
/// tool-only 403 `approval_binding_mismatch`, digest
/// mismatch 409 (approval unconsumed, no orphaned
/// Effect), consumed approval 409.
#[tokio::test]
async fn nt2_approval_digest_gates() {
    let h = Harness::serve().await;
    let (status, _) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200);

    // No approval at all.
    let (status, body) = h
        .create_note("bu2agent-test", "gated", "body", "key-gate-1", None)
        .await;
    assert_eq!(status, 403, "no approval: {body}");
    assert_eq!(body["error"], "approval_required");
    assert!(h.note_effects().is_empty());
    assert_eq!(h.notes.create_count(), 0);

    // Rejected approval.
    let rejected = h
        .rejected(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "gated", "body": "body"}),
        )
        .await;
    let (status, body) = h
        .create_note("bu2agent-test", "gated", "body", "key-gate-1", Some(&rejected))
        .await;
    assert_eq!(status, 403, "rejected: {body}");
    assert_eq!(body["error"], "approval_rejected");
    assert!(h.note_effects().is_empty());

    // Tool-only approval (no folder / name / body
    // presented): it can never authorize a digest-bound
    // write.
    let (status, requested) = h.post("/approval/request", json!({"tool": "notes.create"})).await;
    assert_eq!(status, 200, "tool-only request: {requested}");
    let tool_only = requested["tool_call_id"].as_str().unwrap().to_string();
    let (status, _) = h
        .post(
            "/approval/decide",
            json!({"tool_call_id": tool_only, "status": "approved"}),
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = h
        .create_note("bu2agent-test", "gated", "body", "key-gate-1", Some(&tool_only))
        .await;
    assert_eq!(status, 403, "tool-only: {body}");
    assert_eq!(body["error"], "approval_binding_mismatch");
    assert!(h.note_effects().is_empty());
    assert_eq!(h.notes.create_count(), 0);

    // Digest mismatch: approval binds another body —
    // 409, no orphaned planned row, the approval
    // unconsumed.
    let wrong = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "gated", "body": "other body"}),
        )
        .await;
    let (status, body) = h
        .create_note("bu2agent-test", "gated", "body", "key-gate-1", Some(&wrong))
        .await;
    assert_eq!(status, 409, "mismatch: {body}");
    assert_eq!(body["error"], "approval_digest_mismatch");
    assert!(h.note_effects().is_empty());
    assert_eq!(h.notes.create_count(), 0);

    // A correct approval still works after the mismatch.
    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "gated", "body": "body"}),
        )
        .await;
    let (status, created) = h
        .create_note("bu2agent-test", "gated", "body", "key-gate-1", Some(&approval))
        .await;
    assert_eq!(status, 200, "correct retry: {created}");
    assert_eq!(h.notes.create_count(), 1);

    // The same approval id cannot authorize a second
    // create.
    let (status, body) = h
        .create_note("bu2agent-test", "gated2", "body2", "key-gate-2", Some(&approval))
        .await;
    assert_eq!(status, 409, "consumed replay: {body}");
    assert_eq!(body["error"], "approval_already_consumed");
    assert_eq!(h.note_effects().len(), 1, "no second Effect");
    assert_eq!(h.notes.create_count(), 1);
}

/// NT3: append end to end — approved append replaces
/// the live plaintext, the row hash advances, one
/// `note.updated` event + succeeded Effect, same-key
/// replay 200s, same-key different-text 409s.
#[tokio::test]
async fn nt3_append_end_to_end() {
    let h = Harness::serve().await;
    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "append target", "body": "first line"}),
        )
        .await;
    let (status, created) = h
        .create_note("bu2agent-test", "append target", "first line", "key-nt3-create", Some(&approval))
        .await;
    assert_eq!(status, 200, "setup create: {created}");
    let base = sha256_hex("first line".as_bytes());
    assert_eq!(created["body_hash"], base);
    assert_eq!(created["note_id"], "note-1");

    let append_approval = h
        .approved(
            "notes.append",
            json!({"note_id": "note-1", "base_body_hash": base, "body": "\nsecond line"}),
        )
        .await;
    let (status, appended) = h
        .append_note("note-1", "\nsecond line", &base, "key-nt3-append", Some(&append_approval))
        .await;
    assert_eq!(status, 200, "append: {appended}");
    assert_eq!(appended["note_id"], "note-1");
    assert_eq!(
        appended["body_hash"],
        sha256_hex("first line\nsecond line".as_bytes())
    );
    assert_eq!(appended["replayed"], false);
    // The response carries the ROW, whose `effect_id`
    // is the create Effect's id (record_appended never
    // overwrites it — the create replay compares the
    // create Effect's digest through it). The append
    // Effect is found by operation + scope.
    let create_effect_id = appended["effect_id"].as_str().unwrap().to_string();
    let append_effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.operation == "append" && e.scope == "note-1")
        .expect("append effect");
    assert_ne!(append_effect.effect_id, create_effect_id);
    let effect_id = append_effect.effect_id.clone();

    // The fake replaced the plaintext once, with the
    // concatenated text (no added separator).
    assert_eq!(h.notes.set_count(), 1);
    assert_eq!(
        h.notes.plaintext("bu2agent-test", "fake-1").as_deref(),
        Some("first line\nsecond line")
    );

    // The Effect (notes, append, note-1) succeeded; the
    // event is exactly one `note.updated` with the nine
    // whitelisted fields.
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "succeeded");
    assert_eq!(effect.operation, "append");
    assert_eq!(effect.scope, "note-1");
    let events = h.note_events();
    let updated = events
        .iter()
        .find(|e| e.event_type == "note.updated")
        .expect("note.updated");
    let payload: Value = serde_json::from_str(&updated.payload_json).unwrap();
    assert_eq!(payload.as_object().unwrap().len(), 9, "no extra fields: {payload}");
    assert_eq!(payload["note_id"], "note-1");
    assert_eq!(payload["body_hash"], sha256_hex("first line\nsecond line".as_bytes()));

    // Verify reads back the new hash → verified.
    let (status, verified) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "verify: {verified}");
    assert_eq!(verified["verdict"], "verified");

    // Same key + same (base, text) replays 200 with no
    // second write.
    let (status, replayed) = h
        .append_note("note-1", "\nsecond line", &base, "key-nt3-append", None)
        .await;
    assert_eq!(status, 200, "replay: {replayed}");
    assert_eq!(replayed["replayed"], true);
    assert_eq!(
        replayed["body_hash"],
        sha256_hex("first line\nsecond line".as_bytes())
    );
    assert_eq!(h.notes.set_count(), 1);
    assert_eq!(h.note_effects().len(), 2, "create + append only");

    // Same key + different text → 409.
    let (status, body) = h
        .append_note("note-1", "\ndifferent text", &base, "key-nt3-append", None)
        .await;
    assert_eq!(status, 409, "key conflict: {body}");
    assert_eq!(body["error"], "idempotency_key_conflict");
    assert_eq!(h.notes.set_count(), 1);
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));
}

/// NT3 (conflict): the live text changed since the
/// base was read → 409 `note_changed` carrying the
/// fresh base hash, no write, approval not burned;
/// retrying with the fresh base succeeds.
#[tokio::test]
async fn nt3_append_conflict() {
    let h = Harness::serve().await;
    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "conflict target", "body": "first line"}),
        )
        .await;
    let (status, created) = h
        .create_note(
            "bu2agent-test",
            "conflict target",
            "first line",
            "key-nt3c-create",
            Some(&approval),
        )
        .await;
    assert_eq!(status, 200, "setup create: {created}");
    let stale_base = sha256_hex("first line".as_bytes());

    // An edit made in Notes.app outside the gateway.
    assert!(h
        .notes
        .set_plaintext("bu2agent-test", "fake-1", "edited elsewhere"));

    let conflict_approval = h
        .approved(
            "notes.append",
            json!({"note_id": "note-1", "base_body_hash": stale_base, "body": "\nthird line"}),
        )
        .await;
    let (status, body) = h
        .append_note("note-1", "\nthird line", &stale_base, "key-nt3c-append", Some(&conflict_approval))
        .await;
    assert_eq!(status, 409, "conflict: {body}");
    assert_eq!(body["error"], "note_changed");
    let fresh_base = body["current_body_hash"].as_str().unwrap().to_string();
    assert_eq!(fresh_base, sha256_hex("edited elsewhere".as_bytes()));
    // No write happened, no Effect was planned, the
    // approval was not burned.
    assert_eq!(h.notes.set_count(), 0);
    assert_eq!(
        h.notes.plaintext("bu2agent-test", "fake-1").as_deref(),
        Some("edited elsewhere")
    );
    assert_eq!(h.note_effects().len(), 1, "create Effect only");
    assert!(h.note_events().iter().all(|e| e.event_type != "note.updated"));

    // Retry with the fresh base from the 409: the same
    // text lands on the edited text.
    let retry_approval = h
        .approved(
            "notes.append",
            json!({"note_id": "note-1", "base_body_hash": fresh_base, "body": "\nthird line"}),
        )
        .await;
    let (status, appended) = h
        .append_note("note-1", "\nthird line", &fresh_base, "key-nt3c-append", Some(&retry_approval))
        .await;
    assert_eq!(status, 200, "retry: {appended}");
    assert_eq!(
        appended["body_hash"],
        sha256_hex("edited elsewhere\nthird line".as_bytes())
    );
    assert_eq!(h.notes.set_count(), 1);
    assert_eq!(
        h.notes.plaintext("bu2agent-test", "fake-1").as_deref(),
        Some("edited elsewhere\nthird line")
    );
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));
}

/// NT4: delete end to end — decided digest-bound
/// approval → 200, the fake removes the note, the row
/// is tombstoned (text retained), one `note.deleted`
/// event, Effect succeeded, verify reads back absent
/// → verified, re-delete replays 200, a restore +
/// re-sync does not resurrect the row, and the list
/// still shows the tombstone.
#[tokio::test]
async fn nt4_delete_end_to_end() {
    let h = Harness::serve_with(vec![seed_note(
        "n-1",
        "Quarterly results (confidential)",
        "The launch code is hunter2 and the key is <redacted>",
    )])
    .await;
    let (status, _) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200);

    let approval = h
        .approved("notes.delete", json!({"note_id": "note-1"}))
        .await;
    let (status, deleted) = h.delete_note("note-1", Some(&approval)).await;
    assert_eq!(status, 200, "delete: {deleted}");
    assert_eq!(deleted["note_id"], "note-1");
    assert_eq!(deleted["native_id"], "n-1");
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["replayed"], false);
    let effect_id = deleted["effect_id"].as_str().unwrap().to_string();

    // The fake removed the note exactly once.
    assert_eq!(h.notes.delete_count(), 1);
    assert!(h.notes.note_ids("bu2agent-test").is_empty());

    // The row is tombstoned, text retained, body hash
    // only.
    let row = h.note_row("note-1").expect("row");
    assert!(row.deleted);
    assert!(row.updated_ms > 0);
    assert_eq!(row.name, "Quarterly results (confidential)");
    assert_eq!(
        row.body_hash,
        sha256_hex("The launch code is hunter2 and the key is <redacted>".as_bytes())
    );

    // The Effect (notes, delete, note-1) succeeded; the
    // Mandate is the single delete tool.
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "succeeded");
    assert_eq!(effect.operation, "delete");
    assert_eq!(effect.scope, "note-1");
    let execution_id = effect.execution_id.clone().expect("execution");
    let conn = h.db();
    let mandates = sqlite_store::list_mandates(&conn, None, Some(&execution_id)).expect("mandates");
    assert_eq!(mandates.len(), 1, "one write mandate");
    let allowed: Value = serde_json::from_str(&mandates[0].allowed_json).expect("allowed_json");
    assert_eq!(allowed["tools"], json!(["notes.delete"]));
    drop(conn);

    // The event: exactly one `note.deleted`, payload
    // only the nine whitelisted fields.
    let events = h.note_events();
    assert_eq!(events.len(), 2, "1 synced + 1 deleted");
    let deleted_event = events
        .iter()
        .find(|e| e.event_type == "note.deleted")
        .expect("note.deleted");
    let payload: Value = serde_json::from_str(&deleted_event.payload_json).unwrap();
    assert_eq!(payload.as_object().unwrap().len(), 9, "no extra fields: {payload}");
    assert_eq!(payload["note_id"], "note-1");
    assert_eq!(payload["native_id"], "n-1");
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

    // Re-delete with a fresh approval: 200 replayed,
    // same effect id, no second fake delete, no second
    // event, no second Effect.
    let second_approval = h
        .approved("notes.delete", json!({"note_id": "note-1"}))
        .await;
    let (status, second) = h.delete_note("note-1", Some(&second_approval)).await;
    assert_eq!(status, 200, "re-delete: {second}");
    assert_eq!(second["replayed"], true);
    assert_eq!(second["effect_id"], effect_id);
    assert_eq!(h.notes.delete_count(), 1);
    assert_eq!(h.note_events().len(), 2);
    assert_eq!(h.note_effects().len(), 1);

    // Restore the note into the folder (Recently
    // Deleted → back) and re-sync: the tombstone must
    // NOT resurrect — no new row, the list still shows
    // it with deleted=true.
    h.notes.restore_note(seed_note(
        "n-1",
        "Quarterly results (confidential)",
        "The launch code is hunter2 and the key is <redacted>",
    ));
    let (status, resynced) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200, "re-sync: {resynced}");
    assert_eq!(resynced["synced_count"], 0);
    assert_eq!(resynced["total_count"], 1);
    let rows = h.note_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].note_id, "note-1");
    assert!(rows[0].deleted);
    let (status, listed) = h.get("/api/notes").await;
    assert_eq!(status, 200, "list after re-sync: {listed}");
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["deleted"], true);
    let (status, got) = h.get("/api/notes/note-1").await;
    assert_eq!(status, 200, "get tombstone: {got}");
    assert_eq!(got["deleted"], true);
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));
}

/// NT4 (redaction): no note title / body plaintext
/// anywhere in the event log, and the body is in no
/// row either (only its hash).
#[tokio::test]
async fn nt4_redaction_and_fold() {
    let h = Harness::serve_with(vec![seed_note(
        "n-1",
        "Quarterly results (confidential)",
        "The launch code is hunter2 and the key is <redacted>",
    )])
    .await;
    let (status, _) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200);

    let approval = h
        .approved("notes.delete", json!({"note_id": "note-1"}))
        .await;
    let (status, deleted) = h.delete_note("note-1", Some(&approval)).await;
    assert_eq!(status, 200, "delete: {deleted}");

    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "note.deleted")
            .count(),
        1
    );
    for event in &events {
        for forbidden in [
            "hunter2",
            "sk-live-TEST123",
            "Quarterly results (confidential)",
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
    // The body is in no row either (only its hash).
    let row = get_note_row(&conn, "note-1").expect("get").expect("row");
    let row_json = serde_json::to_string(&row).unwrap();
    assert!(!row_json.contains("hunter2"), "row leaks body: {row_json}");
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// NT5: error mapping — unknown id 404, disconnected
/// 409 (reads unaffected), locked append 403
/// `note_locked`, Notes not running 502 (Effect
/// failed), Automation denied 403 (Effect failed),
/// script timeout 502 (Effect unknown + Inbox row),
/// unavailable 501 (Effect failed), and the
/// non-macOS default source 501s.
#[tokio::test]
async fn nt5_error_mapping() {
    let h = Harness::serve().await;
    let (status, _) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200);

    // Unknown id → 404, before any approval is needed.
    let (status, body) = h.get("/api/notes/note-99").await;
    assert_eq!(status, 404, "unknown get: {body}");
    assert_eq!(body["error"], "unknown note");
    let (status, body) = h
        .append_note("note-99", "x", &sha256_hex(b"x"), "key-nt5-1", None)
        .await;
    assert_eq!(status, 404, "unknown append: {body}");
    assert_eq!(body["error"], "unknown note");
    let (status, body) = h.delete_note("note-99", None).await;
    assert_eq!(status, 404, "unknown delete: {body}");
    assert_eq!(body["error"], "unknown note");
    // The fold holds here. It is NOT asserted after
    // the disconnect + direct-sqlite restore below:
    // the restore changes the sources row without a
    // `source.registered` event, so the sources fold
    // legitimately diverges (the 6b-1 R2d artifact — a
    // reconnect route is still the recorded follow-up).
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));

    // Disconnected source rejects the write with 409;
    // reads are unaffected.
    let (status, disconnected) = h
        .post("/sources/notes-source/disconnect", json!({}))
        .await;
    assert_eq!(status, 200, "disconnect: {disconnected}");
    assert_eq!(disconnected["status"], "disconnected");
    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "blocked", "body": "body"}),
        )
        .await;
    let (status, body) = h
        .create_note("bu2agent-test", "blocked", "body", "key-nt5-2", Some(&approval))
        .await;
    assert_eq!(status, 409, "disconnected write: {body}");
    assert_eq!(body["error"], "source_disconnected");
    assert_eq!(h.notes.create_count(), 0, "disconnected must not create");
    assert!(h.note_effects().is_empty(), "no Effect for a refused write");
    let (status, rows) = h.get("/api/notes").await;
    assert_eq!(status, 200, "list read: {rows}");
    assert_eq!(rows.as_array().unwrap().len(), 2);
    let (status, synced) = h.sync("bu2agent-test").await;
    assert_eq!(status, 200, "sync read: {synced}");
    // Restore for the remaining cases (no reconnect
    // route — direct sqlite3, the 6b-1 R2 precedent).
    let conn = h.db();
    conn.execute(
        "UPDATE sources SET status = 'active' WHERE source_id = 'notes-source'",
        [],
    )
    .expect("restore");
    drop(conn);

    // Locked live note → 403 `note_locked`, no write,
    // no Effect (the re-read returns before planning).
    assert!(h.notes.set_locked("bu2agent-test", "n-1", true));
    let base = sha256_hex("Alpha body hunter2".as_bytes());
    let locked_approval = h
        .approved(
            "notes.append",
            json!({"note_id": "note-1", "base_body_hash": base, "body": "more"}),
        )
        .await;
    let (status, body) = h
        .append_note("note-1", "more", &base, "key-nt5-3", Some(&locked_approval))
        .await;
    assert_eq!(status, 403, "locked append: {body}");
    assert_eq!(body["error"], "note_locked");
    assert_eq!(h.notes.set_count(), 0);
    assert!(h.note_effects().is_empty());
    assert!(h.notes.set_locked("bu2agent-test", "n-1", false));

    // Notes not running → 502, Effect failed, no row.
    // (The folder pre-check defers to the execute step,
    // so the failure is recorded on the Effect.)
    h.notes.set_mode(FakeMode::NotRunning);
    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "down", "body": "body"}),
        )
        .await;
    let (status, body) = h
        .create_note("bu2agent-test", "down", "body", "key-nt5-4", Some(&approval))
        .await;
    assert_eq!(status, 502, "not running: {body}");
    assert_eq!(body["error"], "Notes is not running");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.notes.create_count(), 0);
    assert!(h.note_row("note-3").is_none());
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Automation denied → 403, Effect failed.
    h.notes.set_mode(FakeMode::Denied);
    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "denied", "body": "body"}),
        )
        .await;
    let (status, body) = h
        .create_note("bu2agent-test", "denied", "body", "key-nt5-5", Some(&approval))
        .await;
    assert_eq!(status, 403, "denied: {body}");
    assert_eq!(body["error"], "notes access denied");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Script timeout → 502, Effect unknown + an
    // `unknown_requires_reconciliation` Inbox row. The
    // create may or may not have happened: no row, no
    // guess.
    h.notes.set_mode(FakeMode::Timeout);
    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "slow", "body": "body"}),
        )
        .await;
    let (status, body) = h
        .create_note("bu2agent-test", "slow", "body", "key-nt5-6", Some(&approval))
        .await;
    assert_eq!(status, 502, "timeout: {body}");
    assert_eq!(body["error"], "notes source error");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "unknown");
    assert!(h.note_row("note-3").is_none(), "timeout must not record a row");
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

    // Unavailable (the fake's non-macOS shape) → 501,
    // Effect failed.
    h.notes.set_mode(FakeMode::Unavailable);
    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "gone", "body": "body"}),
        )
        .await;
    let (status, body) = h
        .create_note("bu2agent-test", "gone", "body", "key-nt5-7", Some(&approval))
        .await;
    assert_eq!(status, 501, "unavailable: {body}");
    assert_eq!(body["error"], "notes unavailable on this platform");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");
    h.notes.set_mode(FakeMode::Ok);

    // Append's re-read fails the same way, but before
    // planning: 502 with no effect id, no Effect.
    h.notes.set_mode(FakeMode::NotRunning);
    let append_approval = h
        .approved(
            "notes.append",
            json!({"note_id": "note-1", "base_body_hash": base, "body": "more"}),
        )
        .await;
    let effects_before = h.note_effects().len();
    let (status, body) = h
        .append_note("note-1", "more", &base, "key-nt5-8", Some(&append_approval))
        .await;
    assert_eq!(status, 502, "append not running: {body}");
    assert_eq!(body["error"], "Notes is not running");
    assert!(body.get("effect_id").is_none(), "no Effect was planned");
    assert_eq!(h.note_effects().len(), effects_before);
    h.notes.set_mode(FakeMode::Ok);

    // Delete not running → 502, Effect failed, row
    // intact.
    h.notes.set_mode(FakeMode::NotRunning);
    let delete_approval = h.approved("notes.delete", json!({"note_id": "note-1"})).await;
    let (status, body) = h.delete_note("note-1", Some(&delete_approval)).await;
    assert_eq!(status, 502, "delete not running: {body}");
    assert_eq!(body["error"], "Notes is not running");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.notes.delete_count(), 0);
    assert!(!h.note_row("note-1").unwrap().deleted);
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Delete timeout → 502, Effect unknown + Inbox
    // row, row intact (no tombstone on ambiguity).
    h.notes.set_mode(FakeMode::Timeout);
    let delete_approval = h.approved("notes.delete", json!({"note_id": "note-1"})).await;
    let (status, body) = h.delete_note("note-1", Some(&delete_approval)).await;
    assert_eq!(status, 502, "delete timeout: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "unknown");
    assert!(!h.note_row("note-1").unwrap().deleted, "timeout must not tombstone");
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
    h.notes.set_mode(FakeMode::Ok);
    // No fold assertion here: the direct-sqlite
    // source restore above changed the sources row
    // without a `source.registered` event, so the
    // sources fold legitimately diverges (the 6b-1 R2d
    // artifact). The fold was asserted before the
    // disconnect, above.
}

/// NT5 (non-macOS): the production default source on
/// Linux is the unavailable stub — sync 501s, and a
/// write 501s with a failed Effect recorded.
/// The gate below is a real attribute, not a comment:
/// on macOS the default source is AppleScript, so the
/// 501 assertions are impossible there (this was the
/// ci #109 macOS failure).
#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn nt5_non_macos_unavailable() {
    let h = Harness::serve_default().await;
    let (status, body) = h.sync("bu2agent-test").await;
    assert_eq!(status, 501, "default sync: {body}");
    assert_eq!(body["error"], "notes unavailable on this platform");

    let approval = h
        .approved(
            "notes.create",
            json!({"folder": "bu2agent-test", "name": "x", "body": "y"}),
        )
        .await;
    let (status, body) = h
        .create_note("bu2agent-test", "x", "y", "key-nt5-default", Some(&approval))
        .await;
    assert_eq!(status, 501, "default create: {body}");
    assert_eq!(body["error"], "notes unavailable on this platform");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .note_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");
    assert!(sqlite_store::fold_matches_tables(&h.db()).expect("fold"));
}
