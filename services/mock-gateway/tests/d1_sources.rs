//! D1-4 tests (plan §5, S1–S4): the Source Registry —
//! seeding from existing state (all four kinds), the
//! disconnect flag flip (history untouched, reconnect
//! restores), file changes entering the event log with a
//! persisted `files-watcher` cursor, and cleanup
//! isolation across sources + restart.
//!
//! HTTP routes only, plus `sqlite_store` reads and the
//! pub store seams (`reconnect_source`,
//! `fold_matches_tables`) where HTTP cannot reach the
//! state (there is no HTTP reconnect route in this batch
//! — only disconnect — and the 10 s sweeper tick is not
//! the files-watcher). No loops are started, so the model
//! never gets a call.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use mock_gateway::sqlite_store;
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

struct ScriptModel {
    steps: Mutex<VecDeque<ModelReply>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(replies.into()),
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
        let _ = messages;
        let reply = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move { Ok(reply) })
    }
}

fn build_state(dir: &Path, model: Arc<ScriptModel>) -> AppState {
    build_state_inner(dir, model, None)
}

/// Same as `build_state`, but with a connector JSONL
/// file loaded BEFORE the SQLite path is opened (the
/// production constructor order after the D1-4 reorder:
/// the v8 seed reads linked accounts from the map).
fn build_state_inner(
    dir: &Path,
    model: Arc<ScriptModel>,
    connector_path: Option<PathBuf>,
) -> AppState {
    let mut state = AppState::with_files_root_and_approvals(
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
    if let Some(path) = connector_path {
        state = state.with_connector_state_path(path);
    }
    let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
    state.sqlite_import_and_switch().expect("switch");
    state
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: Option<tokio::task::JoinHandle<()>>,
    model: Arc<ScriptModel>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        let dir = temp_dir("harness");
        std::fs::create_dir_all(dir.join("files")).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let state = build_state(&dir, model.clone());
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
        }
    }

    /// Restart the gateway on the same directory (same
    /// SQLite file, same JSONL files): the production
    /// restart path, not a fresh database.
    async fn restart(&mut self) {
        if let Some(old) = self.server.take() {
            old.abort();
            let _ = old.await;
        }
        let model = Arc::new(ScriptModel::new(vec![]));
        self.model = model.clone();
        let state = build_state(&self.dir, model);
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

    async fn sources(&self) -> Vec<Value> {
        self.ok_get("/sources/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    fn row_count(&self, table: &str) -> i64 {
        let conn = self.db();
        conn.query_row(
            &format!("SELECT COUNT(*) FROM {table}"),
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn file_changed_events(&self) -> Vec<Value> {
        let conn = self.db();
        let events = sqlite_store::list_events(&conn).unwrap();
        events
            .iter()
            .filter(|e| e.event_type == "file.changed")
            .map(|e| serde_json::from_str(&e.payload_json).unwrap())
            .collect()
    }
}

/// S1: the Registry lists source rows of all four kinds
/// (connector/device/file/mail, ≥1 each); `GET
/// /sources/list` matches the SQLite `sources` table.
#[tokio::test]
async fn test_s1_registry_lists_all_kinds() {
    let h = Harness::serve(vec![]).await;
    let list = h.sources().await;
    let kinds: Vec<&str> = list
        .iter()
        .map(|r| r["kind"].as_str().unwrap())
        .collect();
    for kind in ["connector", "device", "file", "mail"] {
        assert!(
            kinds.contains(&kind),
            "missing kind {kind} in {list:?}"
        );
    }
    let ids: Vec<&str> = list
        .iter()
        .map(|r| r["source_id"].as_str().unwrap())
        .collect();
    for id in [
        "connector-notes",
        "connector-mail",
        "device-reminders",
        "files-root",
        "mail-source",
    ] {
        assert!(ids.contains(&id), "missing source {id} in {ids:?}");
    }
    for row in &list {
        assert_eq!(row["health"], "unknown", "health starts unknown: {row}");
        assert_eq!(row["status"], "active", "seed status is active: {row}");
    }
    // The HTTP list matches the table row-for-row.
    let conn = h.db();
    let table = sqlite_store::list_sources(&conn).unwrap();
    assert_eq!(
        serde_json::to_value(&table).unwrap(),
        serde_json::to_value(&list).unwrap(),
        "GET /sources/list must match the sources table"
    );
    // GET /connectors is unchanged: the static 2 entries.
    let connectors = h.ok_get("/connectors").await;
    let connector_ids: Vec<&str> = connectors
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    assert_eq!(connector_ids, vec!["notes", "mail"]);
    // The fold covers the seeded rows.
    assert!(
        sqlite_store::fold_matches_tables(&conn).unwrap(),
        "fold must cover the seeded sources"
    );
}

/// S1b: the connector JSONL one-time import — a linked
/// account becomes a source row, an unlinked (tombstoned)
/// account does not.
#[tokio::test]
async fn test_s1b_connector_jsonl_import() {
    let dir = temp_dir("connectors");
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let connector_path = dir.join("connectors.jsonl");
    let lines = [
        json!({"kind":"account","account_id":"account-1","connector_id":"notes","display_name":"Notes 1","linked_ms":1000}),
        json!({"kind":"account","account_id":"account-2","connector_id":"mail","display_name":"Mail 2","linked_ms":1001,"unlinked":true}),
        json!({"kind":"connection","connector_id":"notes","connected":true,"updated_ms":1002}),
    ];
    let text: Vec<String> = lines.iter().map(|v| serde_json::to_string(v).unwrap()).collect();
    std::fs::write(&connector_path, text.join("\n")).unwrap();
    let model = Arc::new(ScriptModel::new(vec![]));
    let state = build_state_inner(&dir, model, Some(connector_path));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();
    let response = client
        .get(format!("{url}/sources/list"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let list: Value = response.json().await.unwrap();
    let rows = list.as_array().unwrap();
    let account_row = rows
        .iter()
        .find(|r| r["source_id"] == "account-1")
        .expect("linked account-1 must seed a source row");
    assert_eq!(account_row["kind"], "connector");
    assert_eq!(account_row["scope"], "account-1");
    let config: Value =
        serde_json::from_str(account_row["config_json"].as_str().unwrap()).unwrap();
    assert_eq!(config["connector_id"], "notes");
    assert!(
        rows.iter().all(|r| r["source_id"] != "account-2"),
        "unlinked account-2 must not seed a row: {rows:?}"
    );
    server.abort();
}

/// S2: disconnect one source — only that row's flag
/// flips, other rows untouched; that source's history
/// Effect/Inbox rows untouched; reconnect restores.
#[tokio::test]
async fn test_s2_disconnect_flag_flip_only() {
    let h = Harness::serve(vec![]).await;
    let before = h.sources().await;
    let effects_before = h.row_count("effects");
    let inbox_before = h.row_count("inbox");
    let disconnected = h
        .ok("/sources/device-reminders/disconnect", json!({}))
        .await;
    assert_eq!(disconnected["status"], "disconnected");
    assert_eq!(disconnected["source_id"], "device-reminders");
    // Only that row changed (status + updated_ms); every
    // other row is byte-identical.
    let after = h.sources().await;
    assert_eq!(before.len(), after.len());
    for row in &after {
        let id = row["source_id"].as_str().unwrap();
        let old = before
            .iter()
            .find(|r| r["source_id"] == id)
            .unwrap_or_else(|| panic!("row {id} vanished"));
        if id == "device-reminders" {
            assert_eq!(row["status"], "disconnected");
            assert_ne!(row["updated_ms"], old["updated_ms"]);
        } else {
            assert_eq!(row, old, "row {id} must be untouched by the disconnect");
        }
    }
    // History Effect/Inbox rows untouched.
    assert_eq!(h.row_count("effects"), effects_before);
    assert_eq!(h.row_count("inbox"), inbox_before);
    // The disconnect is recorded as one event.
    let conn = h.db();
    let events = sqlite_store::list_events_for(&conn, "source", "device-reminders").unwrap();
    assert_eq!(events.len(), 2, "registered + disconnected: {events:?}");
    assert_eq!(events.last().unwrap().event_type, "source.disconnected");
    // Re-disconnect is a 200 no-op (no second event).
    let again = h
        .ok("/sources/device-reminders/disconnect", json!({}))
        .await;
    assert_eq!(again["status"], "disconnected");
    let events = sqlite_store::list_events_for(&conn, "source", "device-reminders").unwrap();
    assert_eq!(events.len(), 2, "re-disconnect must not append an event");
    // Unknown id 404s.
    let (status, _) = h.post("/sources/no-such-source/disconnect", json!({})).await;
    assert_eq!(status, 404);
    // Reconnect (store-level: no HTTP reconnect route in
    // this batch) restores the flag.
    let mut conn = h.db();
    let restored = sqlite_store::reconnect_source(&mut conn, "device-reminders", "test")
        .unwrap()
        .expect("device-reminders must exist");
    assert_eq!(restored.status, "active");
    let row = h.ok_get("/sources/device-reminders").await;
    assert_eq!(row["status"], "active");
    assert!(
        sqlite_store::fold_matches_tables(&conn).unwrap(),
        "fold must hold after disconnect + reconnect"
    );
}

/// S3: a file change enters the event log — after
/// modifying a file, a `file.changed` event appears and
/// the `files-watcher` cursor advances; after restart
/// the cursor does not rewind (already-consumed changes
/// are not replayed).
#[tokio::test]
async fn test_s3_file_change_enters_event_log() {
    let mut h = Harness::serve(vec![]).await;
    // Baseline subscribe establishes the server-side
    // baseline: no events yet.
    let baseline = h
        .ok("/files/subscribe", json!({"path": "", "wait_ms": 0}))
        .await;
    let cursor0 = baseline["cursor"].as_str().unwrap().to_string();
    assert!(h.file_changed_events().is_empty());
    // Modify one file (new file, new size).
    std::fs::write(h.dir.join("files").join("probe.txt"), "hello d1-4").unwrap();
    let changed = h
        .ok(
            "/files/subscribe",
            json!({"path": "", "cursor": cursor0, "wait_ms": 5000}),
        )
        .await;
    assert_eq!(changed["changed"], true);
    let cursor1 = changed["cursor"].as_str().unwrap().to_string();
    assert_ne!(cursor1, cursor0);
    // Exactly one file.changed event: path hash + the
    // cursor transition, never file content.
    let events = h.file_changed_events();
    assert_eq!(events.len(), 1, "one changed file, one event: {events:?}");
    let payload = &events[0];
    assert_eq!(
        payload["path_hash"],
        sqlite_store::sha256_hex(b"probe.txt"),
        "path hash identifies the file: {payload}"
    );
    assert_eq!(payload["old_cursor"], cursor0);
    assert_eq!(payload["new_cursor"], cursor1);
    assert!(
        !payload.to_string().contains("hello d1-4"),
        "no file content in the event: {payload}"
    );
    assert!(
        !payload.to_string().contains("secret")
            && !payload.to_string().contains("password"),
        "no secret-shaped strings: {payload}"
    );
    // The files-watcher cursor advanced to the event id,
    // and the file source row's cursor is the new tree.
    let conn = h.db();
    let cursors = sqlite_store::list_event_cursors(&conn).unwrap();
    let watcher = cursors
        .iter()
        .find(|c| c.consumer_name == "files-watcher")
        .expect("files-watcher cursor must exist");
    let all_events = sqlite_store::list_events(&conn).unwrap();
    let changed_id = all_events
        .iter()
        .find(|e| e.event_type == "file.changed")
        .unwrap()
        .id;
    assert_eq!(watcher.last_event_id, changed_id);
    let file_row = h.ok_get("/sources/files-root").await;
    assert_eq!(file_row["cursor"], cursor1);
    // Restart: no replay, cursor does not rewind.
    h.restart().await;
    let again = h
        .ok(
            "/files/subscribe",
            json!({"path": "", "cursor": cursor1, "wait_ms": 0}),
        )
        .await;
    assert_eq!(again["changed"], false);
    assert_eq!(h.file_changed_events().len(), 1, "no replayed events");
    let conn = h.db();
    let cursors = sqlite_store::list_event_cursors(&conn).unwrap();
    let watcher = cursors
        .iter()
        .find(|c| c.consumer_name == "files-watcher")
        .unwrap();
    assert_eq!(watcher.last_event_id, changed_id, "cursor must not rewind");
    let file_row = h.ok_get("/sources/files-root").await;
    assert_eq!(file_row["cursor"], cursor1);
    assert!(
        sqlite_store::fold_matches_tables(&conn).unwrap(),
        "fold must hold after file changes + restart"
    );
}

/// S4: cleanup isolation — after disconnecting the mail
/// source, connector/device/file states and event streams
/// are unaffected; after restart the Registry is
/// row-by-row readable.
#[tokio::test]
async fn test_s4_cleanup_isolation() {
    let mut h = Harness::serve(vec![]).await;
    let before = h.sources().await;
    let connectors_before = h.ok_get("/connectors").await;
    let conn = h.db();
    let events_before = sqlite_store::list_events(&conn).unwrap().len();
    let disconnected = h.ok("/sources/mail-source/disconnect", json!({})).await;
    assert_eq!(disconnected["status"], "disconnected");
    // Only the mail row changed; every other row is
    // byte-identical.
    let after = h.sources().await;
    assert_eq!(before.len(), after.len());
    for row in &after {
        let id = row["source_id"].as_str().unwrap();
        let old = before
            .iter()
            .find(|r| r["source_id"] == id)
            .unwrap_or_else(|| panic!("row {id} vanished"));
        if id == "mail-source" {
            assert_eq!(row["status"], "disconnected");
        } else {
            assert_eq!(row, old, "row {id} must be untouched by the mail disconnect");
        }
    }
    // GET /connectors is unchanged (still static + connected).
    assert_eq!(h.ok_get("/connectors").await, connectors_before);
    // Exactly one new event (the mail disconnect); no other
    // aggregate's stream grew.
    let conn = h.db();
    let events_after = sqlite_store::list_events(&conn).unwrap();
    assert_eq!(events_after.len(), events_before + 1);
    assert_eq!(
        events_after.last().unwrap().event_type,
        "source.disconnected"
    );
    assert_eq!(
        events_after.last().unwrap().aggregate_id,
        "mail-source"
    );
    // Restart: the Registry is row-by-row readable, the
    // mail row stays disconnected, the others stay active.
    h.restart().await;
    let list = h.sources().await;
    assert_eq!(list.len(), before.len());
    for row in &list {
        let id = row["source_id"].as_str().unwrap();
        if id == "mail-source" {
            assert_eq!(row["status"], "disconnected");
        } else {
            assert_eq!(row["status"], "active");
        }
    }
    let conn = h.db();
    assert!(
        sqlite_store::fold_matches_tables(&conn).unwrap(),
        "fold must hold after disconnect + restart"
    );
}
