//! D1-6b-2 tests (plan §3): real calendar create end to
//! end — digest-bound approval + write Execution +
//! single-tool Mandate + `calendar_events` fact row +
//! `calendar.created` event + Effect, idempotency,
//! approval gates, calendar selection, redaction, and
//! error mapping.
//!
//! HTTP routes only, plus `sqlite_store` reads (created
//! rows / effects / mandates / events / inbox) where
//! HTTP cannot reach the state. The calendar backend is
//! a test-only fake (no EventKit, no real Calendar): it
//! records creates and can be switched into the denied /
//! unverified / unavailable failure modes. "Created"
//! means the backend accepted the event, never that it
//! was delivered anywhere. Window validation on a real
//! Mac is the proof of the real EventKit save (the
//! identifier-recovery re-list included).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::calendar_backend::{
    CalendarBackend, CalendarError, CalendarEvent, CreatedCalendarEvent,
};
use mock_gateway::calendar_store::{
    calendar_notes_hash, get_calendar_event, list_calendar_events, CalendarEventRow,
};
use mock_gateway::operation_binding::{calendar_create_digest, sha256_hex};
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
    Denied,
    Unverified,
    Unavailable,
}

/// Test-only fake calendar backend: two known calendars,
/// an in-memory event store, and the failure modes on
/// demand. The production EventKit backend is never
/// exercised here (no EventKit in the VM); the save +
/// re-list form itself is macOS-only, and window
/// validation on a real Mac is the proof of the real
/// save.
struct FakeCalendarBackend {
    mode: Mutex<FakeMode>,
    calendars: Vec<String>,
    events: Mutex<Vec<CalendarEvent>>,
    create_count: AtomicUsize,
}

impl FakeCalendarBackend {
    fn new() -> Self {
        Self {
            mode: Mutex::new(FakeMode::Ok),
            calendars: vec!["cal-work".to_string(), "cal-home".to_string()],
            events: Mutex::new(Vec::new()),
            create_count: AtomicUsize::new(0),
        }
    }

    fn set_mode(&self, mode: FakeMode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn create_count(&self) -> usize {
        self.create_count.load(Ordering::SeqCst)
    }

    fn stored(&self) -> Vec<CalendarEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Test-only mutation: drop the event (the CC1
    /// verify-inconclusive step: the event was deleted
    /// after creation).
    fn remove_event(&self, event_id: &str) {
        self.events
            .lock()
            .unwrap()
            .retain(|e| e.event_id != event_id);
    }

    /// Test-only mutation: put an event back with
    /// caller-chosen fields (the CC1 verify-mismatch
    /// step: the stored event no longer matches the
    /// fact row).
    fn insert_event(&self, event: CalendarEvent) {
        self.events.lock().unwrap().push(event);
    }
}

impl CalendarBackend for FakeCalendarBackend {
    fn list_events(&self) -> Result<Vec<CalendarEvent>, CalendarError> {
        let mut events = self.stored();
        events.sort_by_key(|e| (e.start_ms, e.event_id.clone()));
        Ok(events)
    }

    fn get_event(&self, event_id: &str) -> Result<Option<CalendarEvent>, CalendarError> {
        Ok(self.stored().into_iter().find(|e| e.event_id == event_id))
    }

    fn calendar_exists(&self, calendar_id: &str) -> Result<bool, CalendarError> {
        match *self.mode.lock().unwrap() {
            FakeMode::Unavailable => Err(CalendarError::Unavailable(
                "calendar unavailable in tests".to_string(),
            )),
            _ => Ok(self.calendars.iter().any(|c| c == calendar_id)),
        }
    }

    fn create_event(
        &self,
        calendar_id: &str,
        title: &str,
        start_ms: i64,
        end_ms: i64,
        all_day: bool,
        location: Option<&str>,
        notes: Option<&str>,
    ) -> Result<CreatedCalendarEvent, CalendarError> {
        match *self.mode.lock().unwrap() {
            FakeMode::Unavailable => {
                return Err(CalendarError::Unavailable(
                    "calendar unavailable in tests".to_string(),
                ));
            }
            FakeMode::Denied => return Err(CalendarError::Denied),
            FakeMode::Unverified => {
                // The save landed but the identifier is
                // lost: store the event, then report
                // Unverified (the route must answer
                // Unknown, never guess a row).
                let n = self.create_count.fetch_add(1, Ordering::SeqCst) + 1;
                self.events.lock().unwrap().push(CalendarEvent {
                    event_id: format!("fake-event-{n}"),
                    title: title.to_string(),
                    start_ms,
                    end_ms,
                    all_day,
                    calendar_id: Some(calendar_id.to_string()),
                    calendar_title: None,
                    location: location.map(str::to_string),
                    notes: notes.map(str::to_string),
                });
                return Err(CalendarError::Unverified);
            }
            FakeMode::Ok => {}
        }
        if !self.calendars.iter().any(|c| c == calendar_id) {
            return Err(CalendarError::NotFound);
        }
        let n = self.create_count.fetch_add(1, Ordering::SeqCst) + 1;
        let event = CalendarEvent {
            event_id: format!("fake-event-{n}"),
            title: title.to_string(),
            start_ms,
            end_ms,
            all_day,
            calendar_id: Some(calendar_id.to_string()),
            calendar_title: None,
            location: location.map(str::to_string),
            notes: notes.map(str::to_string),
        };
        self.events.lock().unwrap().push(event.clone());
        Ok(CreatedCalendarEvent {
            event_id: event.event_id,
            title: event.title,
            start_ms: event.start_ms,
            end_ms: event.end_ms,
            all_day: event.all_day,
            calendar_id: event.calendar_id,
            location: event.location,
            notes: event.notes,
        })
    }
}

fn build_state(dir: &Path, calendar: Arc<dyn CalendarBackend>) -> AppState {
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
            api_key: Some("test-key".to_string()),
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
    .with_calendar_backend(calendar);
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
    calendar: Arc<FakeCalendarBackend>,
}

impl Harness {
    async fn serve() -> Self {
        let dir = temp_dir("calendar-create");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let calendar = Arc::new(FakeCalendarBackend::new());
        let state = build_state(&dir, calendar.clone());
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
            calendar,
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

    fn created_rows(&self) -> Vec<CalendarEventRow> {
        let conn = self.db();
        list_calendar_events(&conn).expect("list created")
    }

    fn calendar_effects(&self) -> Vec<mock_gateway::effect_store::EffectFileRecord> {
        let conn = self.db();
        sqlite_store::list_effects(&conn)
            .expect("effects")
            .into_iter()
            .filter(|e| e.capability == "calendar")
            .collect()
    }

    fn calendar_events(&self) -> Vec<sqlite_store::EventRow> {
        let conn = self.db();
        sqlite_store::list_events(&conn)
            .expect("events")
            .into_iter()
            .filter(|e| e.event_type == "calendar.created")
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

    /// Request a digest-bound `calendar.create` approval
    /// and decide it approved. Returns the tool_call_id.
    #[allow(clippy::too_many_arguments)]
    async fn approved_calendar_create(
        &self,
        calendar_id: &str,
        title: &str,
        start_ms: i64,
        end_ms: i64,
        all_day: bool,
        location: Option<&str>,
        notes: Option<&str>,
    ) -> String {
        let mut request = json!({
            "tool": "calendar.create",
            "calendar_id": calendar_id,
            "title": title,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "all_day": all_day,
        });
        if let Some(location) = location {
            request["location"] = json!(location);
        }
        if let Some(notes) = notes {
            request["notes"] = json!(notes);
        }
        let (status, requested) = self.post("/approval/request", request).await;
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

    /// Request a `calendar.create` approval and decide it
    /// rejected. Returns the tool_call_id.
    async fn rejected_calendar_create(
        &self,
        calendar_id: &str,
        title: &str,
        start_ms: i64,
        end_ms: i64,
        all_day: bool,
    ) -> String {
        let (status, requested) = self
            .post(
                "/approval/request",
                json!({
                    "tool": "calendar.create",
                    "calendar_id": calendar_id,
                    "title": title,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "all_day": all_day,
                }),
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

    async fn create(&self, value: Value) -> (u16, Value) {
        self.post("/api/calendar/events", value).await
    }

    #[allow(clippy::too_many_arguments)]
    fn create_body(
        calendar_id: &str,
        title: &str,
        start_ms: i64,
        end_ms: i64,
        all_day: bool,
        location: Option<&str>,
        notes: Option<&str>,
        key: &str,
        tool_call_id: Option<&str>,
    ) -> Value {
        let mut value = json!({
            "calendar_id": calendar_id,
            "title": title,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "all_day": all_day,
            "idempotency_key": key,
        });
        if let Some(location) = location {
            value["location"] = json!(location);
        }
        if let Some(notes) = notes {
            value["notes"] = json!(notes);
        }
        if let Some(id) = tool_call_id {
            value["tool_call_id"] = json!(id);
        }
        value
    }

    async fn verify(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/verify", json!({"effect_id": effect_id}))
            .await
    }
}

const CAL: &str = "cal-work";
const TITLE: &str = "CC1 planning";
const START: i64 = 1_795_000_000_000;
const END: i64 = 1_795_003_600_000;
const LOCATION: &str = "Room 3";
const NOTES: &str = "Bring the quarterly numbers.";

/// CC1: create success end to end — fake create +
/// decided digest-bound approval → 200, fact row +
/// `calendar.created` event + Effect succeeded; the
/// notes body is in neither the table nor the event.
/// Verify then reads back: consistent → verified;
/// deleted → inconclusive (no mismatch row); tampered
/// → mismatch (mismatch row opens). Idempotency
/// (the §3 replay rule) rides along: same key + same
/// content replays, same key + different content 409s,
/// no key 400s.
#[tokio::test]
async fn cc1_create_end_to_end() {
    let h = Harness::serve().await;
    assert_eq!(h.schema_version(), DB_SCHEMA_VERSION);
    // 6b-4 bumps the schema to v13 (notes table).
    assert_eq!(DB_SCHEMA_VERSION, 13);

    let approval = h
        .approved_calendar_create(CAL, TITLE, START, END, false, Some(LOCATION), Some(NOTES))
        .await;
    let (status, created) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            Some(LOCATION),
            Some(NOTES),
            "key-cc1",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "create: {created}");
    assert_eq!(created["event_id"], "fake-event-1");
    assert_eq!(created["source_id"], "calendar-source");
    assert_eq!(created["calendar_id"], CAL);
    assert_eq!(created["title"], TITLE);
    assert_eq!(created["start_ms"], START);
    assert_eq!(created["end_ms"], END);
    assert_eq!(created["all_day"], false);
    assert_eq!(created["location"], LOCATION);
    assert_eq!(created["notes_hash"], sha256_hex(NOTES.as_bytes()));
    assert_eq!(created["replayed"], false);
    assert!(!created["policy_version"].as_str().unwrap().is_empty());
    let effect_id = created["effect_id"].as_str().unwrap().to_string();
    assert!(effect_id.starts_with("effect-"));

    // The fake saw exactly one create, with the exact
    // fields.
    assert_eq!(h.calendar.create_count(), 1);
    let stored = h.calendar.stored();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].event_id, "fake-event-1");
    assert_eq!(stored[0].calendar_id.as_deref(), Some(CAL));

    // The fact row.
    let rows = h.created_rows();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.event_id, "fake-event-1");
    assert_eq!(row.source_id, "calendar-source");
    assert_eq!(row.calendar_id, CAL);
    assert_eq!(row.title, TITLE);
    assert_eq!(row.start_ms, START);
    assert_eq!(row.end_ms, END);
    assert!(!row.all_day);
    assert_eq!(row.location.as_deref(), Some(LOCATION));
    assert_eq!(row.notes_hash, calendar_notes_hash(Some(NOTES)));
    assert_eq!(row.idempotency_key, "key-cc1");
    assert_eq!(row.effect_id, effect_id);
    assert!(row.created_ms > 0);
    assert!(!row.policy_version.is_empty());
    // The notes body itself is in no column: the row
    // has no notes field at all.
    let row_json = serde_json::to_string(row).unwrap();
    assert!(!row_json.contains(NOTES), "row leaks notes: {row_json}");

    // The Effect: ("calendar", "create",
    // idempotency_key), succeeded, bound to the approval
    // and the write Execution, digest-bound to the exact
    // event.
    let effects = h.calendar_effects();
    assert_eq!(effects.len(), 1);
    let effect = &effects[0];
    assert_eq!(effect.effect_id, effect_id);
    assert_eq!(effect.capability, "calendar");
    assert_eq!(effect.operation, "create");
    assert_eq!(effect.scope, "key-cc1");
    assert_eq!(effect.status, "succeeded");
    assert_eq!(effect.approval_id.as_deref(), Some(approval.as_str()));
    let execution_id = effect.execution_id.clone().expect("write execution");
    let expected_digest = calendar_create_digest(
        CAL,
        &sha256_hex(TITLE.as_bytes()),
        START,
        END,
        false,
        &sha256_hex(LOCATION.as_bytes()),
        &calendar_notes_hash(Some(NOTES)),
    );
    assert_eq!(
        effect.input_digest.as_deref(),
        Some(expected_digest.as_str())
    );

    // The Mandate (Q-D1-6b-23): one row on the write
    // Execution, tools = the single write tool, goal
    // empty.
    let conn = h.db();
    let mandates = sqlite_store::list_mandates(&conn, None, Some(&execution_id)).expect("mandates");
    assert_eq!(mandates.len(), 1, "one write mandate");
    let allowed: Value = serde_json::from_str(&mandates[0].allowed_json).expect("allowed_json");
    assert_eq!(allowed["tools"], json!(["calendar.create"]));
    assert_eq!(allowed["goal_id"], Value::Null);
    assert_eq!(allowed["entry"], "direct-write");
    assert_eq!(mandates[0].policy_version_id, row.policy_version);
    drop(conn);

    // The event: exactly one `calendar.created`,
    // payload only the whitelisted fields.
    let events = h.calendar_events();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.aggregate_kind, "calendar");
    assert_eq!(event.aggregate_id, "fake-event-1");
    let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
    let keys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    for kept in [
        "event_id",
        "calendar_id",
        "source_id",
        "title_len",
        "start_ms",
        "end_ms",
        "all_day",
        "created_ms",
        "execution_id",
    ] {
        assert!(keys.contains(&kept), "missing {kept} in {payload}");
    }
    assert_eq!(keys.len(), 9, "no extra fields: {payload}");
    assert_eq!(payload["event_id"], "fake-event-1");
    assert_eq!(payload["calendar_id"], CAL);
    assert_eq!(payload["title_len"], TITLE.chars().count());
    assert_eq!(payload["start_ms"], START);
    assert_eq!(payload["end_ms"], END);
    assert_eq!(payload["all_day"], false);
    assert_eq!(payload["created_ms"], row.created_ms);
    assert_eq!(payload["execution_id"], execution_id);
    assert!(!event.payload_json.contains(TITLE), "event leaks title");
    assert!(
        !event.payload_json.contains(LOCATION),
        "event leaks location"
    );
    assert!(!event.payload_json.contains(NOTES), "event leaks notes");

    // The live list still shows the event (the 6a live
    // query is unchanged by the create).
    let (status, listed) = h.get("/api/calendar/events").await;
    assert_eq!(status, 200);
    assert_eq!(listed.as_array().unwrap().len(), 1);
    assert_eq!(listed[0]["event_id"], "fake-event-1");

    // Verify, consistent re-read → verified.
    let (status, verified) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "verify: {verified}");
    assert_eq!(verified["verdict"], "verified");
    assert_eq!(verified["reason"], "fields_match");
    let observed: Value =
        serde_json::from_str(verified["observed_json"].as_str().unwrap()).expect("observed");
    assert_eq!(observed["kind"], "calendar-event");
    assert_eq!(observed["fields_match"], true);
    assert!(
        !verified["observed_json"].as_str().unwrap().contains(TITLE),
        "observed leaks title"
    );

    // Verify, event deleted after creation →
    // inconclusive (the create itself is not disproven)
    // and no mismatch Inbox row opens.
    h.calendar.remove_event("fake-event-1");
    let (status, inconclusive) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "verify deleted: {inconclusive}");
    assert_eq!(inconclusive["verdict"], "inconclusive");
    assert_eq!(inconclusive["reason"], "state_unclear");
    let conn = h.db();
    let mismatch =
        sqlite_store::list_inbox(&conn, Some("verification_mismatch"), true).expect("inbox");
    assert!(
        mismatch
            .iter()
            .all(|r| r.effect_id.as_deref() != Some(effect_id.as_str())),
        "inconclusive must not open a mismatch row: {mismatch:?}"
    );
    drop(conn);

    // Verify, event present with tampered fields →
    // mismatch, and a mismatch Inbox row opens.
    h.calendar.insert_event(CalendarEvent {
        event_id: "fake-event-1".to_string(),
        title: "Tampered title".to_string(),
        start_ms: START,
        end_ms: END,
        all_day: false,
        calendar_id: Some(CAL.to_string()),
        calendar_title: None,
        location: Some(LOCATION.to_string()),
        notes: Some(NOTES.to_string()),
    });
    let (status, mismatch) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "verify tampered: {mismatch}");
    assert_eq!(mismatch["verdict"], "mismatch");
    assert_eq!(mismatch["reason"], "field_mismatch");
    let conn = h.db();
    let mismatch_rows =
        sqlite_store::list_inbox(&conn, Some("verification_mismatch"), true).expect("inbox");
    assert!(
        mismatch_rows
            .iter()
            .any(|r| r.effect_id.as_deref() == Some(effect_id.as_str())),
        "no mismatch row for {effect_id}: {mismatch_rows:?}"
    );

    // Idempotency (the §3 replay rule): same key + same
    // content replays the original row — no second
    // create, no second Effect — even with the already
    // consumed approval (the replay answers before the
    // approval peek).
    let (status, replay) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            Some(LOCATION),
            Some(NOTES),
            "key-cc1",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "replay: {replay}");
    assert_eq!(replay["event_id"], "fake-event-1");
    assert_eq!(replay["replayed"], true);
    assert_eq!(h.calendar.create_count(), 1);
    assert_eq!(h.calendar_effects().len(), 1);
    assert_eq!(h.created_rows().len(), 1);

    // Same key + different content is a 409.
    let other_approval = h
        .approved_calendar_create(CAL, "Another title", START, END, false, None, None)
        .await;
    let (status, conflict) = h
        .create(Harness::create_body(
            CAL,
            "Another title",
            START,
            END,
            false,
            None,
            None,
            "key-cc1",
            Some(&other_approval),
        ))
        .await;
    assert_eq!(status, 409, "key conflict: {conflict}");
    assert_eq!(conflict["error"], "idempotency_key_conflict");
    assert_eq!(h.calendar.create_count(), 1);

    // No key is a 400 (empty and omitted).
    let (status, no_key) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "",
            Some(&other_approval),
        ))
        .await;
    assert_eq!(status, 400, "empty key: {no_key}");
    let (status, no_key) = h
        .create(json!({
            "calendar_id": CAL,
            "title": TITLE,
            "start_ms": START,
            "end_ms": END,
            "all_day": false,
            "tool_call_id": other_approval,
        }))
        .await;
    assert_eq!(status, 400, "omitted key: {no_key}");

    // Empty fields → 400.
    for body in [
        Harness::create_body(
            "",
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc1-empty",
            Some(&other_approval),
        ),
        Harness::create_body(
            CAL,
            "",
            START,
            END,
            false,
            None,
            None,
            "key-cc1-empty",
            Some(&other_approval),
        ),
    ] {
        let (status, bad) = h.create(body).await;
        assert_eq!(status, 400, "empty field: {bad}");
    }

    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// CC2: approval gates — no approval 403, rejected
/// approval 403, digest mismatch 409 (approval
/// unconsumed, no orphaned planned row), consumed
/// approval replay 409.
#[tokio::test]
async fn cc2_approval_gates() {
    let h = Harness::serve().await;

    // No approval at all.
    let (status, body) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc3-none",
            None,
        ))
        .await;
    assert_eq!(status, 403, "no approval: {body}");
    assert_eq!(body["error"], "approval_required");

    // Unknown tool_call_id.
    let (status, body) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc3-unknown",
            Some("approval-999999"),
        ))
        .await;
    assert_eq!(status, 403, "unknown approval: {body}");
    assert_eq!(body["error"], "approval_required");

    // Rejected approval.
    let rejected = h
        .rejected_calendar_create(CAL, TITLE, START, END, false)
        .await;
    let (status, body) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc3-rejected",
            Some(&rejected),
        ))
        .await;
    assert_eq!(status, 403, "rejected approval: {body}");
    assert_eq!(body["error"], "approval_rejected");

    // Digest mismatch: approval for content A, create
    // content B. 409, approval unconsumed, and no
    // orphaned planned row is left behind.
    let approval = h
        .approved_calendar_create(CAL, TITLE, START, END, false, None, None)
        .await;
    let (status, mismatch) = h
        .create(Harness::create_body(
            CAL,
            "A different title",
            START,
            END,
            false,
            None,
            None,
            "key-cc3-mismatch",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "digest mismatch: {mismatch}");
    assert_eq!(mismatch["error"], "approval_digest_mismatch");
    assert_eq!(h.calendar.create_count(), 0, "mismatch must not create");
    let planned: Vec<_> = h
        .calendar_effects()
        .into_iter()
        .filter(|e| e.approval_id.as_deref() == Some(approval.as_str()) && e.status == "planned")
        .collect();
    assert!(
        planned.is_empty(),
        "mismatch left an orphaned planned row: {planned:?}"
    );
    // The approval is unconsumed: the correct content
    // still creates with the same approval id.
    let (status, created) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc3-correct",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "retry with the same approval: {created}");
    assert_eq!(h.calendar.create_count(), 1);

    // Consumed-approval replay: the same approval id
    // with different content (and a new key) is a 409,
    // and no second Effect is planned for it.
    let (status, replay) = h
        .create(Harness::create_body(
            CAL,
            "Yet another title",
            START,
            END,
            false,
            None,
            None,
            "key-cc3-consumed",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "consumed replay: {replay}");
    assert_eq!(replay["error"], "approval_already_consumed");
    assert_eq!(h.calendar_effects().len(), 1, "no second Effect");
    assert_eq!(h.calendar.create_count(), 1);
}

/// CC3: calendar selection — an unknown
/// calendar id is a 404 with a failed Effect and no
/// fact row; a second known calendar creates fine. No
/// silent fall into a default calendar (Q-D1-6b-09).
#[tokio::test]
async fn cc3_calendar_selection() {
    let h = Harness::serve().await;

    let approval = h
        .approved_calendar_create("cal-nope", TITLE, START, END, false, None, None)
        .await;
    let (status, body) = h
        .create(Harness::create_body(
            "cal-nope",
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc4-unknown",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 404, "unknown calendar: {body}");
    assert_eq!(body["error"], "unknown calendar");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.calendar.create_count(), 0);
    assert!(h.created_rows().is_empty());
    let effect = h
        .calendar_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // A second known calendar creates fine.
    let approval = h
        .approved_calendar_create("cal-home", TITLE, START, END, true, None, None)
        .await;
    let (status, created) = h
        .create(Harness::create_body(
            "cal-home",
            TITLE,
            START,
            END,
            true,
            None,
            None,
            "key-cc4-home",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "known calendar: {created}");
    assert_eq!(created["calendar_id"], "cal-home");
    assert_eq!(created["all_day"], true);
}

/// CC4: redaction — the
/// `calendar.created` payload carries only whitelisted
/// fields; no title, location, or notes plaintext
/// anywhere in the event log; the fold holds.
#[tokio::test]
async fn cc4_redaction() {
    let secret_title = "Board meeting (confidential)";
    let secret_location = "Vault room 7";
    let secret_notes = "The launch code is hunter2 and the key is <redacted>";
    let h = Harness::serve().await;
    let approval = h
        .approved_calendar_create(
            CAL,
            secret_title,
            START,
            END,
            false,
            Some(secret_location),
            Some(secret_notes),
        )
        .await;
    let (status, created) = h
        .create(Harness::create_body(
            CAL,
            secret_title,
            START,
            END,
            false,
            Some(secret_location),
            Some(secret_notes),
            "key-cc5",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 200, "create: {created}");

    let conn = h.db();
    let events = sqlite_store::list_events(&conn).expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|e| e.event_type == "calendar.created")
            .count(),
        1
    );
    for event in &events {
        for forbidden in [
            "hunter2",
            "sk-live-TEST123",
            "Board meeting (confidential)",
            "Vault room 7",
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
    let created_event = events
        .iter()
        .find(|e| e.event_type == "calendar.created")
        .unwrap();
    let payload: Value = serde_json::from_str(&created_event.payload_json).unwrap();
    let keys: Vec<&str> = payload
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    // Exactly the 9 whitelisted fields — no title,
    // location, notes, or notes hash in the event.
    for kept in [
        "event_id",
        "calendar_id",
        "source_id",
        "title_len",
        "start_ms",
        "end_ms",
        "all_day",
        "created_ms",
        "execution_id",
    ] {
        assert!(keys.contains(&kept), "missing {kept} in {payload}");
    }
    assert_eq!(keys.len(), 9, "no extra fields: {payload}");
    // The notes body is in no row either (only its hash).
    let row = get_calendar_event(&conn, "fake-event-1")
        .expect("get")
        .expect("row");
    let row_json = serde_json::to_string(&row).unwrap();
    assert!(!row_json.contains("hunter2"), "row leaks notes: {row_json}");
    assert_eq!(row.notes_hash, sha256_hex(secret_notes.as_bytes()));
    assert!(sqlite_store::fold_matches_tables(&conn).expect("fold"));
}

/// CC5: error mapping — end at or before start 400,
/// Automation denied 403 (Effect failed), unrecoverable
/// save identifier 502 (Effect unknown + Inbox row),
/// non-macOS/unavailable 501 (Effect failed).
#[tokio::test]
async fn cc5_error_mapping() {
    let h = Harness::serve().await;

    // End at or before start → 400 (equal and before).
    // The approval is for the valid range: validation
    // answers before the approval peek, so its content
    // cannot matter here.
    let approval = h
        .approved_calendar_create(CAL, TITLE, START, END, false, None, None)
        .await;
    for (start, end) in [(START, START), (END, START)] {
        let (status, bad) = h
            .create(Harness::create_body(
                CAL,
                TITLE,
                start,
                end,
                false,
                None,
                None,
                "key-cc5-range",
                Some(&approval),
            ))
            .await;
        assert_eq!(status, 400, "bad range {start}..{end}: {bad}");
    }
    assert_eq!(h.calendar.create_count(), 0);

    // Automation denied → 403, Effect failed.
    h.calendar.set_mode(FakeMode::Denied);
    let approval = h
        .approved_calendar_create(CAL, TITLE, START, END, false, None, None)
        .await;
    let (status, body) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc6-denied",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 403, "denied: {body}");
    assert_eq!(body["error"], "calendar access denied");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.calendar.create_count(), 0);
    assert!(h.created_rows().is_empty());
    let effect = h
        .calendar_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");

    // Unrecoverable save identifier → 502, Effect
    // unknown + an `unknown_requires_reconciliation`
    // Inbox row. The save may or may not have landed:
    // no fact row, no guess.
    h.calendar.set_mode(FakeMode::Unverified);
    let approval = h
        .approved_calendar_create(CAL, TITLE, START, END, false, None, None)
        .await;
    let (status, body) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc6-unverified",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 502, "unverified: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    assert_eq!(h.calendar.create_count(), 1);
    let effect = h
        .calendar_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "unknown");
    assert!(
        h.created_rows().is_empty(),
        "unverified must not record a row"
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

    // Unavailable (the non-macOS production backend) →
    // 501, Effect failed.
    h.calendar.set_mode(FakeMode::Unavailable);
    let approval = h
        .approved_calendar_create(CAL, TITLE, START, END, false, None, None)
        .await;
    let (status, body) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc6-unavailable",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 501, "unavailable: {body}");
    let effect_id = body["effect_id"].as_str().unwrap().to_string();
    let effect = h
        .calendar_effects()
        .into_iter()
        .find(|e| e.effect_id == effect_id)
        .expect("effect");
    assert_eq!(effect.status, "failed");
}

/// Q-D1-6b-24: a disconnected calendar-source rejects
/// new writes with 409 `source_disconnected`; reads are
/// unaffected.
#[tokio::test]
async fn cc_disconnected_source_rejects_writes() {
    let h = Harness::serve().await;
    let (status, disconnected) = h
        .post("/sources/calendar-source/disconnect", json!({}))
        .await;
    assert_eq!(status, 200, "disconnect: {disconnected}");
    assert_eq!(disconnected["status"], "disconnected");

    let approval = h
        .approved_calendar_create(CAL, TITLE, START, END, false, None, None)
        .await;
    let (status, body) = h
        .create(Harness::create_body(
            CAL,
            TITLE,
            START,
            END,
            false,
            None,
            None,
            "key-cc-disconnected",
            Some(&approval),
        ))
        .await;
    assert_eq!(status, 409, "disconnected write: {body}");
    assert_eq!(body["error"], "source_disconnected");
    assert_eq!(h.calendar.create_count(), 0, "disconnected must not create");

    // Reads are unaffected by the disconnect.
    let (status, listed) = h.get("/api/calendar/events").await;
    assert_eq!(status, 200, "disconnected read: {listed}");
}
