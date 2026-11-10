//! D1-5 tests (plan §6, M1–M5): memory layers —
//! explicit save + provenance round-trip, cross-scope
//! read-visible / write-rejected, snapshot freezing the
//! real scope triple, source-cleanup cascade on
//! disconnect, and restart recovery (memories +
//! tombstones survive byte-identical).
//!
//! HTTP routes only, plus `sqlite_store` reads and the
//! pub store seams (`list_project_memories_all`,
//! `list_user_memories_all`, `fold_matches_tables`)
//! where HTTP cannot reach the state.

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

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

fn build_state(dir: &Path, model: Arc<ScriptModel>) -> AppState {
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
        let dir = temp_dir("memory");
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

    fn row_count(&self, table: &str) -> i64 {
        let conn = self.db();
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
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

    async fn execution_for_loop(&self, loop_id: &str) -> Value {
        let list = self.ok_get("/executions/list").await;
        list.as_array()
            .unwrap()
            .iter()
            .find(|e| e["loop_id"] == loop_id)
            .cloned()
            .unwrap_or_else(|| panic!("no execution for loop {loop_id}: {list}"))
    }
}

/// M1: save + provenance + GET round-trip. A project
/// memory keeps its canonical scope triple; a user
/// memory is global ("{}"). Validation rejects a
/// missing content/source/author, a bad layer, and a
/// scoped user memory; unknown ids 404.
#[tokio::test]
async fn m1_save_provenance_round_trip() {
    let h = Harness::serve(vec![]).await;
    let (status, _) = h
        .post(
            "/memories/save",
            json!({"layer": "project", "source": {"author": "tester"}}),
        )
        .await;
    assert_eq!(status, 400, "missing content must be rejected");
    let (status, _) = h
        .post(
            "/memories/save",
            json!({"layer": "project", "content": "x"}),
        )
        .await;
    assert_eq!(status, 400, "missing source must be rejected");
    let (status, _) = h
        .post(
            "/memories/save",
            json!({"layer": "project", "content": "x", "source": {}}),
        )
        .await;
    assert_eq!(status, 400, "missing author must be rejected");
    let (status, _) = h
        .post(
            "/memories/save",
            json!({"layer": "bogus", "content": "x", "source": {"author": "t"}}),
        )
        .await;
    assert_eq!(status, 400, "unknown layer must be rejected");
    let (status, _) = h
        .post(
            "/memories/save",
            json!({"layer": "user", "content": "x",
                   "scope": {"workspace_id": "ws-1"},
                   "source": {"author": "t"}}),
        )
        .await;
    assert_eq!(status, 400, "a scoped user memory must be rejected");
    let m1 = h
        .ok(
            "/memories/save",
            json!({
                "layer": "project",
                "content": "prefers concise replies",
                "scope": {"workspace_id": "ws-1", "project_id": "p-1", "session_id": "s-1"},
                "current_scope": {"workspace_id": "ws-1", "project_id": "p-1", "session_id": "s-1"},
                "source": {"session_id": "s-1", "author": "tester"},
            }),
        )
        .await;
    assert_eq!(m1["memory_id"], "memory-1");
    assert_eq!(m1["layer"], "project");
    assert_eq!(
        m1["scope_json"],
        "{\"project_id\":\"p-1\",\"session_id\":\"s-1\",\"workspace_id\":\"ws-1\"}"
    );
    assert_eq!(m1["content"], "prefers concise replies");
    assert_eq!(m1["deleted"], false);
    let source: Value =
        serde_json::from_str(m1["source_json"].as_str().unwrap()).unwrap();
    assert_eq!(source["author"], "tester");
    assert_eq!(source["session_id"], "s-1");
    assert!(source["created_ms"].as_u64().is_some());
    let got = h.ok_get("/memories/memory-1").await;
    assert_eq!(got, m1, "GET by id must round-trip the saved row");
    let m2 = h
        .ok(
            "/memories/save",
            json!({
                "layer": "user",
                "content": "likes tea",
                "source": {"author": "tester"},
            }),
        )
        .await;
    assert_eq!(m2["memory_id"], "memory-2");
    assert_eq!(m2["scope_json"], "{}");
    let (status, body) = h.get("/memories/memory-999").await;
    assert_eq!(status, 404);
    assert_eq!(body["error"], "unknown memory");
    let conn = h.db();
    let events = sqlite_store::list_events_for(&conn, "memory", "memory-1").unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "memory.created");
    assert!(
        sqlite_store::fold_matches_tables(&conn).unwrap(),
        "fold must hold after saves"
    );
}

/// M2: cross-scope read is visible, cross-scope write
/// is rejected. The ws-2 row shows in a ws-2-filtered
/// list (read-visible); a save scoped ws-2 with a
/// current scope of ws-1 gets 409
/// `memory_scope_mismatch`.
#[tokio::test]
async fn m2_cross_scope_read_visible_write_rejected() {
    let h = Harness::serve(vec![]).await;
    let ws1 = h
        .ok(
            "/memories/save",
            json!({
                "layer": "project", "content": "ws-1 note",
                "scope": {"workspace_id": "ws-1"},
                "current_scope": {"workspace_id": "ws-1"},
                "source": {"author": "tester"},
            }),
        )
        .await;
    let ws2 = h
        .ok(
            "/memories/save",
            json!({
                "layer": "project", "content": "ws-2 note",
                "scope": {"workspace_id": "ws-2"},
                "current_scope": {"workspace_id": "ws-2"},
                "source": {"author": "tester"},
            }),
        )
        .await;
    let (status, body) = h
        .post(
            "/memories/save",
            json!({
                "layer": "project", "content": "sneaky",
                "scope": {"workspace_id": "ws-2"},
                "current_scope": {"workspace_id": "ws-1"},
                "source": {"author": "tester"},
            }),
        )
        .await;
    assert_eq!(status, 409);
    assert_eq!(body["error"], "memory_scope_mismatch");
    let list = h.ok_get("/memories/list?workspace_id=ws-2").await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["memory_id"], ws2["memory_id"]);
    let list = h.ok_get("/memories/list?workspace_id=ws-1").await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["memory_id"], ws1["memory_id"]);
    let list = h.ok_get("/memories/list").await;
    assert_eq!(list.as_array().unwrap().len(), 2);
    let list = h.ok_get("/memories/list?layer=user").await;
    assert_eq!(list.as_array().unwrap().len(), 0);
}

/// M3: the snapshot freezes the real scope triple. A
/// loop execution freezes the caller's session; a task
/// execution freezes the Task record's workspace +
/// project links; old snapshot rows stay byte-identical
/// after later executions.
#[tokio::test]
async fn m3_snapshot_freezes_real_scope_triple() {
    let h = Harness::serve(vec![done()]).await;
    let started = h
        .ok(
            "/agent/loop/start",
            json!({"prompt": "m3 loop", "session_id": "m3-session"}),
        )
        .await;
    let loop_id = started["loop_id"].as_str().unwrap().to_string();
    let exec = h.execution_for_loop(&loop_id).await;
    let snap1_id = exec["workspace_snapshot_id"]
        .as_str()
        .unwrap()
        .to_string();
    let snap1 = {
        let conn = h.db();
        sqlite_store::get_snapshot(&conn, &snap1_id)
            .expect("read snapshot-1")
            .expect("snapshot-1 exists")
    };
    assert_eq!(snap1.memory_scope, "{\"session_id\":\"m3-session\"}");
    let snap1_json = serde_json::to_string(&snap1).unwrap();
    std::fs::create_dir_all(h.dir.join("files").join("proj")).unwrap();
    let task = h
        .ok(
            "/tasks/create",
            json!({"directory": "proj", "workspace_id": "m3-ws", "project_id": "m3-pj"}),
        )
        .await;
    let task_id = task["task_id"].as_str().unwrap().to_string();
    let (status, _) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, 200);
    let list = h.ok_get("/executions/list").await;
    let exec = list
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["task_id"] == task_id)
        .unwrap_or_else(|| panic!("no execution for task {task_id}: {list}"));
    let snap2_id = exec["workspace_snapshot_id"]
        .as_str()
        .unwrap()
        .to_string();
    let snap2 = {
        let conn = h.db();
        sqlite_store::get_snapshot(&conn, &snap2_id)
            .expect("read snapshot-2")
            .expect("snapshot-2 exists")
    };
    assert_eq!(
        snap2.memory_scope,
        "{\"project_id\":\"m3-pj\",\"workspace_id\":\"m3-ws\"}"
    );
    let snap1_again = {
        let conn = h.db();
        sqlite_store::get_snapshot(&conn, &snap1_id)
            .expect("re-read snapshot-1")
            .expect("snapshot-1 exists")
    };
    assert_eq!(
        serde_json::to_string(&snap1_again).unwrap(),
        snap1_json,
        "old snapshot rows stay byte-identical"
    );
}

/// M4: disconnecting a source tombstones every memory
/// whose provenance names it (one `memory.deleted`
/// event each); memories of other sources are
/// untouched; nothing goes into Inbox.
#[tokio::test]
async fn m4_source_cleanup_cascade() {
    let h = Harness::serve(vec![]).await;
    let _a = h
        .ok(
            "/memories/save",
            json!({
                "layer": "project", "content": "reminders note",
                "source": {"source_id": "device-reminders", "author": "tester"},
            }),
        )
        .await;
    let _b = h
        .ok(
            "/memories/save",
            json!({
                "layer": "project", "content": "mail note",
                "source": {"source_id": "mail-source", "author": "tester"},
            }),
        )
        .await;
    assert_eq!(h.row_count("inbox"), 0);
    let disconnected = h
        .ok("/sources/device-reminders/disconnect", json!({}))
        .await;
    assert_eq!(disconnected["status"], "disconnected");
    let got = h.ok_get("/memories/memory-1").await;
    assert_eq!(got["deleted"], true);
    assert_eq!(got["content"], "reminders note", "tombstone keeps the text on disk");
    let list = h.ok_get("/memories/list").await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["memory_id"], "memory-2");
    let got_b = h.ok_get("/memories/memory-2").await;
    assert_eq!(got_b["deleted"], false);
    let conn = h.db();
    let events_a = sqlite_store::list_events_for(&conn, "memory", "memory-1").unwrap();
    assert_eq!(events_a.len(), 2, "created + deleted: {events_a:?}");
    assert_eq!(events_a.last().unwrap().event_type, "memory.deleted");
    let events_b = sqlite_store::list_events_for(&conn, "memory", "memory-2").unwrap();
    assert_eq!(events_b.len(), 1);
    assert_eq!(events_b[0].event_type, "memory.created");
    assert_eq!(h.row_count("inbox"), 0, "the cascade must not touch Inbox");
    assert!(
        sqlite_store::fold_matches_tables(&conn).unwrap(),
        "fold must hold after the cascade"
    );
}

/// M5: restart recovery — memories and tombstones
/// survive byte-identical, the id counter reseeds
/// (next save is memory-3, not a collision), and the
/// fold still holds.
#[tokio::test]
async fn m5_restart_recovery() {
    let mut h = Harness::serve(vec![]).await;
    let _m1 = h
        .ok(
            "/memories/save",
            json!({"layer": "project", "content": "keep me", "source": {"author": "tester"}}),
        )
        .await;
    let _m2 = h
        .ok(
            "/memories/save",
            json!({"layer": "user", "content": "drop me", "source": {"author": "tester"}}),
        )
        .await;
    let deleted = h.ok("/memories/memory-2/delete", json!({})).await;
    assert_eq!(deleted["deleted"], true);
    assert_eq!(deleted["content"], "drop me");
    let again = h.ok("/memories/memory-2/delete", json!({})).await;
    assert_eq!(again["deleted"], true);
    let conn = h.db();
    let events = sqlite_store::list_events_for(&conn, "memory", "memory-2").unwrap();
    assert_eq!(events.len(), 2, "created + one deleted: {events:?}");
    let (status, _) = h.post("/memories/memory-999/delete", json!({})).await;
    assert_eq!(status, 404);
    let before = {
        let conn = h.db();
        let mut all =
            mock_gateway::memory_store::list_project_memories_all(&conn).unwrap();
        all.extend(mock_gateway::memory_store::list_user_memories_all(&conn).unwrap());
        all.iter()
            .map(|row| serde_json::to_string(row).unwrap())
            .collect::<Vec<_>>()
    };
    h.restart().await;
    let after = {
        let conn = h.db();
        let mut all =
            mock_gateway::memory_store::list_project_memories_all(&conn).unwrap();
        all.extend(mock_gateway::memory_store::list_user_memories_all(&conn).unwrap());
        all.iter()
            .map(|row| serde_json::to_string(row).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(after, before, "memory rows must survive restart byte-identical");
    let got = h.ok_get("/memories/memory-2").await;
    assert_eq!(got["deleted"], true, "the tombstone survives restart");
    let list = h.ok_get("/memories/list").await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    let m3 = h
        .ok(
            "/memories/save",
            json!({"layer": "project", "content": "after restart", "source": {"author": "tester"}}),
        )
        .await;
    assert_eq!(m3["memory_id"], "memory-3");
    let conn = h.db();
    assert!(
        sqlite_store::fold_matches_tables(&conn).unwrap(),
        "fold must hold after restart"
    );
}
