//! MCL-3 tests (plan §5, L1–L4 + extras): location read
//! — record once, list newest-first, 30-day rolling
//! delete, redact no coordinate plaintext in events.
//! L0 is the registration unit test (the loop-tool
//! points, without a server); L5 is the disconnect
//! check, L6 the source-error mapping, L7 the
//! loop-dispatch e2e test (the MCL-1/MCL-2 extras).
//!
//! Tests use a mutable fake source only; never Core
//! Location. Cross-platform: temp dirs, ports, SQLite
//! only. No kill command, no POSIX signals, no
//! Unix-only path spellings. JSON bodies are built with
//! `json!` (the test-hygiene skill); time budgets are
//! generous (60 s client timeout, no ms-class bounds —
//! the rolling-delete margins are whole days).
//!
//! The privacy split under test (Q-MCL-17): list / get
//! responses DO carry raw latitude / longitude /
//! accuracy (the packaged app shows exact values
//! locally) — L1 asserts that. Event payloads must NOT
//! (grid hash / locality hash / accuracy / time only)
//! — L4 asserts that. The loop tool render carries the
//! grid hash only, never raw coordinates — L7 asserts
//! that.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use mock_gateway::location_record::LOCATION_RETENTION_MS;
use mock_gateway::location_source::{
    grid_hash_for, locality_hash_for, DeviceLocationView, LocationError, LocationSource,
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
        "bu2agent-mcl3-{tag}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
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

/// Mutable fake location source: the test controls the
/// fix (move the device between records for L2) and can
/// make the next read fail (L6). Coordinates are
/// fictional (Shanghai, .invalid-free — they are only
/// ever asserted by their hashes in events).
struct MutableFakeLocationSource {
    view: Mutex<DeviceLocationView>,
    fail: Mutex<Option<LocationError>>,
}

impl MutableFakeLocationSource {
    fn seeded() -> Arc<Self> {
        Arc::new(Self {
            view: Mutex::new(DeviceLocationView {
                latitude: 31.2304,
                longitude: 121.4737,
                accuracy_meters: 35.0,
                country_code: None,
            }),
            fail: Mutex::new(None),
        })
    }

    fn set_view(&self, view: DeviceLocationView) {
        *self.view.lock().unwrap() = view;
    }

    fn set_fail(&self, fail: Option<LocationError>) {
        *self.fail.lock().unwrap() = fail;
    }
}

impl LocationSource for MutableFakeLocationSource {
    fn current(&self) -> Result<DeviceLocationView, LocationError> {
        if let Some(fail) = self.fail.lock().unwrap().clone() {
            return Err(fail);
        }
        Ok(self.view.lock().unwrap().clone())
    }
}

fn build_state(
    dir: &Path,
    model: Arc<ScriptModel>,
    source: Arc<MutableFakeLocationSource>,
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
            api_key: Some("fake-mcl3-key".to_string()),
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
    .with_location_source(source)
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
    source: Arc<MutableFakeLocationSource>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let source = MutableFakeLocationSource::seeded();
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

    async fn record(&self) -> Value {
        self.ok("/api/location/record", json!({})).await
    }

    async fn start(&self) -> String {
        self.ok(
            "/agent/loop/start",
            json!({"prompt": "where am I"}),
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

fn location_event_payloads(h: &Harness) -> Vec<String> {
    let conn = h.db();
    let mut stmt = conn
        .prepare(
            "SELECT payload_json FROM events WHERE aggregate_kind = 'location' \
             AND event_type = 'location.recorded' ORDER BY seq",
        )
        .unwrap();
    stmt.query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn location_source_row(h: &Harness) -> Value {
    h
        .db()
        .query_row(
            "SELECT source_id, kind, status FROM sources WHERE source_id = 'location-source'",
            [],
            |row| {
                Ok(json!({
                    "source_id": row.get::<_, String>(0).unwrap(),
                    "kind": row.get::<_, String>(1).unwrap(),
                    "status": row.get::<_, String>(2).unwrap(),
                }))
            },
        )
        .unwrap()
}

// ---------------------------------------------------------------- L0

#[test]
fn l0_loop_tool_registration_points() {
    // lib.rs: a read, no approval.
    assert_eq!(tool_requires_approval("location.current"), Some(false));
    // loop_engine.rs: the loop's own gate agrees.
    assert!(!loop_tool_requires_approval("location.current"));
    // loop_engine.rs normalize: no args at all (the
    // reminders.list pattern).
    let norm = normalize("location.current", &json!({})).unwrap();
    assert!(norm.is_empty(), "no args means no normalized values");
    assert!(normalize("location.current", &json!({"bogus": 1})).is_err());
    assert!(normalize("location.current", &json!({"limit": 50})).is_err());
}

// ---------------------------------------------------------------- L1

#[tokio::test]
async fn l1_record_once() {
    let h = Harness::serve(vec![]).await;

    // The Registry seed exists (kind "location", active)
    // before any record.
    let seed = location_source_row(&h);
    assert_eq!(seed["source_id"], "location-source");
    assert_eq!(seed["kind"], "location");
    assert_eq!(seed["status"], "active");

    // One manual record: the row carries the raw fix
    // (Q-MCL-17: SQLite stores raw coordinates).
    let row = h.record().await;
    assert_eq!(row["sample_id"], "loc-sample-1");
    assert_eq!(row["source_id"], "location-source");
    assert_eq!(row["latitude"], 31.2304);
    assert_eq!(row["longitude"], 121.4737);
    assert_eq!(row["accuracy_meters"], 35.0);
    assert_eq!(
        row["locality_hash"],
        locality_hash_for(31.2304, 121.4737)
    );
    assert_eq!(row["country_code"], Value::Null);
    assert!(row["recorded_ms"].as_i64().unwrap() > 0);
    assert_eq!(row["recorded_ms"], row["created_ms"]);

    // The list carries the same row; get by id works;
    // unknown id 404s.
    let rows = h.ok_get("/api/location/samples").await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["sample_id"], "loc-sample-1");
    let one = h.ok_get("/api/location/samples/loc-sample-1").await;
    assert_eq!(one["latitude"], 31.2304);
    let (status, _) = h.get("/api/location/samples/loc-sample-99").await;
    assert_eq!(status, 404);

    // Exactly one `location.recorded` event.
    assert_eq!(location_event_payloads(&h).len(), 1);
}

// ---------------------------------------------------------------- L2

#[tokio::test]
async fn l2_list_newest_first() {
    let h = Harness::serve(vec![]).await;
    h.record().await;

    // The device moves: the second record is a
    // different fix.
    h.source.set_view(DeviceLocationView {
        latitude: 31.24,
        longitude: 121.48,
        accuracy_meters: 12.5,
        country_code: Some("CN".to_string()),
    });
    let second = h.record().await;
    assert_eq!(second["sample_id"], "loc-sample-2");
    assert_eq!(second["latitude"], 31.24);
    assert_eq!(second["country_code"], "CN");

    // Newest first (the second record heads the list),
    // even if both records landed in the same
    // millisecond (the rowid tie-break).
    let rows = h.ok_get("/api/location/samples").await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["sample_id"], "loc-sample-2");
    assert_eq!(rows[1]["sample_id"], "loc-sample-1");

    // Limit clamps like the list route (default 50, max
    // 200): a limit of 500 is accepted and returns the
    // two rows, not an error.
    let rows = h.ok_get("/api/location/samples?limit=500").await;
    assert_eq!(rows.as_array().unwrap().len(), 2);
    let rows = h.ok_get("/api/location/samples?limit=1").await;
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["sample_id"], "loc-sample-2");

    // Two events, one per record.
    assert_eq!(location_event_payloads(&h).len(), 2);
}

// ---------------------------------------------------------------- L3

#[tokio::test]
async fn l3_rolling_delete_prunes_at_record_time() {
    let h = Harness::serve(vec![]).await;
    let now = now_ms();
    let old_ms = now - LOCATION_RETENTION_MS - 24 * 60 * 60 * 1000; // 31 days ago
    let kept_ms = now - LOCATION_RETENTION_MS + 60 * 60 * 1000; // 30 days ago minus 1 h
    {
        let conn = h.db();
        // An expired row (direct SQL: the record route
        // always stamps now) and a just-inside row.
        conn.execute(
            "INSERT INTO device_locations (sample_id, source_id, latitude, longitude, \
             accuracy_meters, locality_hash, country_code, recorded_ms, created_ms, \
             origin_device_id) VALUES ('loc-sample-5', 'location-source', 31.0, 121.0, \
             50.0, 'aa', NULL, ?1, ?1, 'dev-1')",
            rusqlite::params![old_ms],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO device_locations (sample_id, source_id, latitude, longitude, \
             accuracy_meters, locality_hash, country_code, recorded_ms, created_ms, \
             origin_device_id) VALUES ('loc-sample-6', 'location-source', 31.1, 121.1, \
             40.0, 'bb', NULL, ?1, ?1, 'dev-1')",
            rusqlite::params![kept_ms],
        )
        .unwrap();
    }
    let before = h.ok_get("/api/location/samples").await;
    assert_eq!(before.as_array().unwrap().len(), 2);

    // One record prunes the expired row in the same
    // transaction — and the new id is max-suffix + 1
    // (loc-sample-7). COUNT + 1 after the prune would
    // be loc-sample-2: no collision here, but the
    // sequence would run backwards (5, 6, 2) — and if
    // the HIGHEST rows had been pruned, COUNT + 1 would
    // reuse an id an old `location.recorded` event still
    // names. Max-suffix + 1 never reuses.
    let row = h.record().await;
    assert_eq!(row["sample_id"], "loc-sample-7");

    let rows = h.ok_get("/api/location/samples").await;
    let rows = rows.as_array().unwrap();
    assert_eq!(rows.len(), 2, "expired pruned, kept + new remain");
    assert_eq!(rows[0]["sample_id"], "loc-sample-7");
    assert_eq!(rows[1]["sample_id"], "loc-sample-6");
    let (status, _) = h.get("/api/location/samples/loc-sample-5").await;
    assert_eq!(status, 404, "the pruned row is gone");

    // The prune deletes ROWS, not events (Q-MCL-16):
    // the new record's event exists; the prune itself
    // writes no event.
    assert_eq!(location_event_payloads(&h).len(), 1);

    // A restart does not resurrect the pruned row.
    let mut h = h;
    h.restart().await;
    let rows = h.ok_get("/api/location/samples").await;
    assert_eq!(rows.as_array().unwrap().len(), 2);
}

// ---------------------------------------------------------------- L4

#[tokio::test]
async fn l4_redact_keeps_no_coordinate_plaintext_in_events() {
    let h = Harness::serve(vec![]).await;
    h.record().await;

    // No event payload names a coordinate value.
    for payload in location_event_payloads(&h) {
        assert!(!payload.contains("31.2304"), "{payload}");
        assert!(!payload.contains("121.4737"), "{payload}");
        assert!(!payload.contains("latitude"), "{payload}");
        assert!(!payload.contains("longitude"), "{payload}");
        // The payload carries the five approved fields
        // instead — the allowlist.
        assert!(payload.contains("\"sample_id\":\"loc-sample-1\""), "{payload}");
        assert!(payload.contains("grid_hash"), "{payload}");
        assert!(payload.contains("locality_hash"), "{payload}");
        assert!(payload.contains("\"accuracy_meters\":35.0"), "{payload}");
        assert!(payload.contains("recorded_ms"), "{payload}");
        // The grid hash is the hash of the cell CENTER,
        // not of the fix: it differs from a hash of the
        // raw coordinate string.
        assert!(
            payload.contains(&grid_hash_for(31.2304, 121.4737)),
            "{payload}"
        );
    }

    // THE DISTINCTION: list / get responses legitimately
    // carry the same raw coordinates (Q-MCL-17) — the
    // redact rule covers EVENT payloads only, never list
    // responses. This assertion locks that in: if a later
    // change strips coordinates from the list, the local
    // display use case breaks and this test says so.
    let list = h.ok_get("/api/location/samples").await;
    let serialized = serde_json::to_string(&list).unwrap();
    assert!(serialized.contains("31.2304"), "{serialized}");
    assert!(serialized.contains("121.4737"), "{serialized}");
}

// ---------------------------------------------------------------- L5

#[tokio::test]
async fn l5_disconnect_rejects_new_records_reads_unaffected() {
    let h = Harness::serve(vec![]).await;
    h.record().await;

    let disconnected = h.ok("/sources/location-source/disconnect", json!({})).await;
    assert_eq!(disconnected["status"], "disconnected");

    // A new record is rejected with 409
    // `source_disconnected` (6b Q-24).
    let (status, body) = h.post("/api/location/record", json!({})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "source_disconnected");

    // Reads are unaffected: list + get still work.
    let rows = h.ok_get("/api/location/samples").await;
    assert_eq!(rows.as_array().unwrap().len(), 1);
    let one = h.ok_get("/api/location/samples/loc-sample-1").await;
    assert_eq!(one["latitude"], 31.2304);

    // No new row, no new event while disconnected.
    assert_eq!(location_event_payloads(&h).len(), 1);

    // Reconnect: records work again.
    let reconnected = h.ok("/sources/location-source/reconnect", json!({})).await;
    assert_eq!(reconnected["status"], "active");
    let row = h.record().await;
    assert_eq!(row["sample_id"], "loc-sample-2");
    assert_eq!(location_event_payloads(&h).len(), 2);
}

// ---------------------------------------------------------------- L6

#[tokio::test]
async fn l6_source_error_mapping() {
    let h = Harness::serve(vec![]).await;

    // Denied -> 403 (the user refused location access).
    h.source
        .set_fail(Some(LocationError::Denied));
    let (status, _) = h.post("/api/location/record", json!({})).await;
    assert_eq!(status, 403);

    // Unavailable -> 501 (no location source on this
    // platform / services off).
    h.source
        .set_fail(Some(LocationError::Unavailable("no source".to_string())));
    let (status, _) = h.post("/api/location/record", json!({})).await;
    assert_eq!(status, 501);

    // Timeout -> 502 (no fix within the cap).
    h.source.set_fail(Some(LocationError::Timeout));
    let (status, _) = h.post("/api/location/record", json!({})).await;
    assert_eq!(status, 502);

    // Upstream -> 502 as well.
    h.source
        .set_fail(Some(LocationError::Upstream("boom".to_string())));
    let (status, _) = h.post("/api/location/record", json!({})).await;
    assert_eq!(status, 502);

    // No row and no event from any failed record.
    let rows = h.ok_get("/api/location/samples").await;
    assert_eq!(rows.as_array().unwrap().len(), 0);
    assert!(location_event_payloads(&h).is_empty());

    // Clearing the failure records normally again.
    h.source.set_fail(None);
    let row = h.record().await;
    assert_eq!(row["sample_id"], "loc-sample-1");
}

// ---------------------------------------------------------------- L7

#[tokio::test]
async fn l7_loop_tool_reads_latest_sample() {
    let h = Harness::serve(vec![
        calls(vec![proposal("location.current", json!({}))]),
        done(),
    ])
    .await;
    h.record().await;
    let loop_id = h.start().await;
    let final_view = h.drive(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    let seen = h.tool_messages();
    assert_eq!(seen.len(), 1, "{seen:?}");
    let (body, truncated) = &seen[0];
    assert!(!truncated);
    assert!(body.contains("sample: loc-sample-1"), "{body}");
    assert!(
        body.contains(&grid_hash_for(31.2304, 121.4737)),
        "{body}"
    );
    assert!(
        body.contains(&locality_hash_for(31.2304, 121.4737)),
        "{body}"
    );
    assert!(body.contains("accuracy_meters: 35"), "{body}");
    // Raw coordinates never reach the model (Q-MCL-17:
    // the model is an export; exact values are shown
    // only locally in the packaged app).
    assert!(!body.contains("31.2304"), "{body}");
    assert!(!body.contains("121.4737"), "{body}");
    assert!(!body.contains("latitude"), "{body}");
    assert!(!body.contains("longitude"), "{body}");
    // Reads produce no Effect, and no new row: the loop
    // tool never triggers a live fix (Q-MCL-18).
    assert!(h.effects().await.is_empty());
    let rows = h.ok_get("/api/location/samples").await;
    assert_eq!(rows.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn l7_loop_tool_empty_before_first_record() {
    let h = Harness::serve(vec![
        calls(vec![proposal("location.current", json!({}))]),
        done(),
    ])
    .await;
    let loop_id = h.start().await;
    let final_view = h.drive(&loop_id).await;
    assert_eq!(final_view["status"], "completed");
    let seen = h.tool_messages();
    assert_eq!(seen.len(), 1, "{seen:?}");
    assert!(seen[0].0.contains("尚未记录当前位置"), "{:?}", seen[0].0);
}
