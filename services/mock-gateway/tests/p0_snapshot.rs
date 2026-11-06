//! P0 stage 0 snapshot tests: read-only assertions pinning the
//! current behavior of the five JSONL stores and the three state
//! machines before any unification work (PA-ARCHITECTURE.md §8,
//! plan draft §6 stage 0).
//!
//! These tests assert what the code does today; they do not bless
//! it as the target model. If any assertion here disagrees with the
//! plan draft §1 inventory, that is a "plan-draft discrepancy" to
//! report, never a reason to change product code to fit the test.
//!
//! Organization mirrors the existing smoke tests: integration
//! tests over the public store APIs plus HTTP drives of AppState
//! with temp-dir JSONL paths. No product code is touched.

use mock_gateway::approval_store::{
    append_record, load_records, next_id_number, ApprovalFileRecord,
};
use mock_gateway::device_store::{
    append_action, load_actions, next_action_number, valid_action_status, DeviceActionRecord,
};
use mock_gateway::loop_engine::{
    BackendFuture, CallRecord, CallStatus, ContentDigest, ErrorCode, LoopEngine, LoopRecord,
    LoopStatus, LoopTools, Message, ModelError, ModelReply, Proposal, ResumeLoop, Timeouts,
    ToolOutcome,
};
use mock_gateway::loop_store::{append as append_loop, load as load_loops};
use mock_gateway::server::{app, AppState};
use mock_gateway::session_store::{
    append_session, load_sessions, next_session_number, SessionRecord, SessionTurn,
    MAX_CONTEXT_BYTES, MAX_INPUT_BYTES, MAX_TURNS, TITLE_CHARS,
};
use mock_gateway::task_store::{append_task, load_tasks, next_task_number, TaskFileRecord};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ---------------------------------------------------------------- HTTP harness

async fn start_server(state: AppState) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind test listener");
    let addr = listener.local_addr().expect("test listener addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app(state)).await.expect("serve test server");
    });
    (addr, handle)
}

async fn raw(addr: std::net::SocketAddr, request: &str) -> (String, String) {
    let mut stream = TcpStream::connect(addr).await.expect("connect test server");
    stream.write_all(request.as_bytes()).await.expect("write test request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read test response");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    (head.lines().next().unwrap_or("").to_string(), body.to_string())
}

async fn post(addr: std::net::SocketAddr, path: &str, json: &str) -> (String, String) {
    raw(
        addr,
        &format!(
            "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            json.len(),
            json
        ),
    )
    .await
}

async fn get(addr: std::net::SocketAddr, path: &str) -> (String, String) {
    raw(
        addr,
        &format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"),
    )
    .await
}

fn temp_base(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("mock-p0-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create temp base");
    dir
}

fn json_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(path).expect("read jsonl");
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("jsonl line parses"))
        .collect()
}

fn last_approval_record(path: &std::path::Path, id: &str) -> serde_json::Value {
    json_lines(path)
        .into_iter()
        .rfind(|v| v["tool_call_id"] == id)
        .unwrap_or_else(|| panic!("no approval record for {id}"))
}

fn task_record(
    id: &str,
    directory: &str,
    status: &str,
) -> TaskFileRecord {
    TaskFileRecord {
        task_id: id.to_string(),
        directory: directory.to_string(),
        artifact_path: None,
        status: status.to_string(),
        error: None,
        approval_id: None,
        draft_sha256: None,
        draft_bytes: None,
        execute_binding: None,
    }
}

fn action_record(id: &str, status: &str) -> DeviceActionRecord {
    DeviceActionRecord {
        action_id: id.to_string(),
        device: "mac-reminders".to_string(),
        action: "reminders.set_completed".to_string(),
        reminder_id: "reminder-1".to_string(),
        target_completed: true,
        status: status.to_string(),
        detail: None,
        // P0-B adapter field; snapshot fixtures model pre-P0-B
        // rows, which carry no execution id.
        execution_id: None,
        // P0-C field; pre-P0-C rows carry no planned Effect.
        effect_id: None,
    }
}

fn approval_record(id: &str, tool: &str, status: &str) -> ApprovalFileRecord {
    ApprovalFileRecord {
        tool_call_id: id.to_string(),
        tool: tool.to_string(),
        status: status.to_string(),
        binding: None,
        consumed: false,
        operation_digest: None,
        action_binding: None,
        // P0-C field; snapshot fixtures model pre-P0-C rows,
        // which carry no Effect binding.
        effect_binding: None,
        message: None,
    }
}

// ---------------------------------------------------------------- State sets

#[test]
fn loop_and_call_status_sets_snapshot() {
    // LoopStatus: exactly 8 variants; only Created/Running/
    // AwaitingApproval are non-terminal, everything else (including
    // NeedsReview and Interrupted) is terminal.
    let terminal: &[(LoopStatus, bool)] = &[
        (LoopStatus::Created, false),
        (LoopStatus::Running, false),
        (LoopStatus::AwaitingApproval, false),
        (LoopStatus::Completed, true),
        (LoopStatus::NeedsReview, true),
        (LoopStatus::Failed, true),
        (LoopStatus::Cancelled, true),
        (LoopStatus::Interrupted, true),
    ];
    for (status, expected) in terminal {
        assert_eq!(status.terminal(), *expected, "terminal({status:?})");
        // The wire form is snake_case (serde rename_all).
        let wire = serde_json::to_string(status).expect("serialize loop status");
        assert!(wire.starts_with('"') && wire.ends_with('"'), "wire: {wire}");
    }
    assert_eq!(serde_json::to_string(&LoopStatus::AwaitingApproval).unwrap(), "\"awaiting_approval\"");
    assert_eq!(serde_json::to_string(&LoopStatus::NeedsReview).unwrap(), "\"needs_review\"");

    // CallStatus: exactly 8 variants, snake_case wire names.
    let calls: &[(CallStatus, &str)] = &[
        (CallStatus::Proposed, "\"proposed\""),
        (CallStatus::AwaitingApproval, "\"awaiting_approval\""),
        (CallStatus::Running, "\"running\""),
        (CallStatus::Succeeded, "\"succeeded\""),
        (CallStatus::Failed, "\"failed\""),
        (CallStatus::Unknown, "\"unknown\""),
        (CallStatus::Rejected, "\"rejected\""),
        (CallStatus::Duplicate, "\"duplicate\""),
    ];
    for (status, wire) in calls {
        assert_eq!(&serde_json::to_string(status).unwrap(), wire, "{status:?}");
    }

    // ErrorCode set is part of the loop contract surface; pin its
    // size and a few wire names so a silent addition is visible.
    assert_eq!(serde_json::to_string(&ErrorCode::DriverLimit).unwrap(), "\"driver_limit\"");
    assert_eq!(serde_json::to_string(&ErrorCode::AuditUnavailable).unwrap(), "\"audit_unavailable\"");
}

#[test]
fn device_status_set_snapshot() {
    // Device actions: exactly 7 statuses in the allowlist.
    for status in [
        "requested",
        "approved",
        "completed",
        "rejected",
        "failed",
        "unknown",
        "awaiting_authorization",
    ] {
        assert!(valid_action_status(status), "{status} must be valid");
    }
    assert!(!valid_action_status("bogus"));
    assert!(!valid_action_status(""));
    assert!(!valid_action_status("interrupted"));
}

#[test]
fn session_store_snapshot() {
    let base = temp_base("sessions");
    let path = base.join("sessions.jsonl");
    let turn = |role: &str, text: &str| SessionTurn {
        role: role.to_string(),
        text: text.to_string(),
        created_ms: 1,
        model: None,
        finish_reason: None,
        deleted: false,
    };
    append_session(
        &path,
        &SessionRecord {
            session_id: "session-1".to_string(),
            title: "first".to_string(),
            created_ms: 1,
            updated_ms: 1,
            deleted: false,
            pinned: false,
            archived: false,
            turns: vec![turn("user", "hello")],
        },
    );
    append_session(
        &path,
        &SessionRecord {
            session_id: "session-1".to_string(),
            title: "first".to_string(),
            created_ms: 1,
            updated_ms: 2,
            deleted: true,
            pinned: false,
            archived: false,
            turns: vec![turn("user", "hello"), turn("assistant", "hi")],
        },
    );
    // Store level: lines stay in file order; the fold (last wins)
    // happens in the server, not here.
    let loaded = load_sessions(&path);
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[0].turns.len(), 1);
    assert!(!loaded[0].deleted);
    assert_eq!(loaded[1].turns.len(), 2);
    assert!(loaded[1].deleted, "deletion is a tombstone line, not erasure");
    assert_eq!(next_session_number(&loaded), 2);

    // Context/selection constants pinned by the draft.
    assert_eq!(MAX_TURNS, 10);
    assert_eq!(MAX_CONTEXT_BYTES, 32 * 1024);
    assert_eq!(MAX_INPUT_BYTES, 64 * 1024);
    assert_eq!(TITLE_CHARS, 40);
    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn bad_line_divergence_snapshot() {
    // D6: the loop audit is the only store whose load fails the
    // whole batch on one bad line; the other four skip bad lines.
    let base = temp_base("bad-lines");

    let approvals = base.join("approvals.jsonl");
    std::fs::write(
        &approvals,
        "{\"tool_call_id\":\"approval-1\",\"tool\":\"fs.write\",\"status\":\"pending\"}\nnot json\n{\"tool_call_id\":\"approval-2\",\"tool\":\"echo\",\"status\":\"pending\"}\n",
    )
    .expect("seed approvals");
    assert_eq!(load_records(&approvals).len(), 2);

    let tasks = base.join("tasks.jsonl");
    std::fs::write(
        &tasks,
        "{\"task_id\":\"task-1\",\"directory\":\"\",\"artifact_path\":null,\"status\":\"created\"}\nnot json\n{\"task_id\":\"task-2\",\"directory\":\"\",\"artifact_path\":null,\"status\":\"completed\"}\n",
    )
    .expect("seed tasks");
    assert_eq!(load_tasks(&tasks).len(), 2);

    let actions = base.join("actions.jsonl");
    std::fs::write(
        &actions,
        "{\"action_id\":\"action-1\",\"device\":\"mac-reminders\",\"action\":\"reminders.set_completed\",\"reminder_id\":\"reminder-1\",\"target_completed\":true,\"status\":\"requested\",\"detail\":null}\nnot json\n",
    )
    .expect("seed actions");
    assert_eq!(load_actions(&actions).len(), 1);

    let sessions = base.join("sessions.jsonl");
    std::fs::write(
        &sessions,
        "{\"session_id\":\"session-1\",\"title\":\"t\",\"created_ms\":1,\"updated_ms\":1,\"turns\":[]}\nnot json\n",
    )
    .expect("seed sessions");
    assert_eq!(load_sessions(&sessions).len(), 1);

    let loops = base.join("loops.jsonl");
    append_loop(
        &loops,
        &LoopRecord {
            loop_id: "loop-1".to_string(),
            prompt_digest: ContentDigest { sha256: "x".to_string(), bytes: 0 },
            model: "m".to_string(),
            status: LoopStatus::Created,
            error: None,
            max_calls: 5,
            proposal_count: 0,
            model_attempts: 0,
            budget_ms: 120_000,
            used_ms: 0,
            created_at_ms: 0,
            updated_at_ms: 0,
            queue_position: 0,
            calls: vec![],
            unknown_writes: vec![],
            unknown_reads: vec![],
            late_model_arrivals_ms: vec![],
            final_digest: None,
        },
    )
    .expect("seed loop");
    std::fs::write(
        &loops,
        format!(
            "{}\nnot json\n",
            std::fs::read_to_string(&loops).expect("read seeded loops")
        ),
    )
    .expect("append bad loop line");
    let err = load_loops(&loops).expect_err("loop load must fail on a bad line");
    assert_eq!(err, "loop_audit_invalid");

    let _ = std::fs::remove_dir_all(&base);
}

#[test]
fn last_wins_and_counters_snapshot() {
    let base = temp_base("last-wins");

    // Approvals: lines stay in order; last line per id wins at fold.
    let approvals = base.join("approvals.jsonl");
    append_record(&approvals, &approval_record("approval-1", "fs.write", "pending"))
        .expect("append");
    append_record(&approvals, &approval_record("approval-1", "fs.write", "approved"))
        .expect("append");
    append_record(&approvals, &approval_record("approval-2", "echo", "pending"))
        .expect("append");
    let loaded = load_records(&approvals);
    assert_eq!(loaded.len(), 3);
    assert_eq!(loaded[1].status, "approved");
    assert_eq!(next_id_number(&loaded), 3);

    // Tasks: same shape, counter continues past the largest id.
    let tasks = base.join("tasks.jsonl");
    append_task(&tasks, &task_record("task-1", "", "created"));
    append_task(&tasks, &task_record("task-1", "", "completed"));
    let loaded = load_tasks(&tasks);
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[1].status, "completed");
    assert_eq!(next_task_number(&loaded), 2);

    // Actions: same shape.
    let actions = base.join("actions.jsonl");
    append_action(&actions, &action_record("action-1", "requested"));
    append_action(&actions, &action_record("action-1", "completed"));
    let loaded = load_actions(&actions);
    assert_eq!(loaded.len(), 2);
    assert_eq!(loaded[1].status, "completed");
    assert_eq!(next_action_number(&loaded), 2);

    // Loops differ: load folds by loop_id, so two lines for one
    // loop come back as a single record, last wins.
    let loops = base.join("loops.jsonl");
    let loop_record = |status: LoopStatus| LoopRecord {
        loop_id: "loop-1".to_string(),
        prompt_digest: ContentDigest { sha256: "x".to_string(), bytes: 0 },
        model: "m".to_string(),
        status,
        error: None,
        max_calls: 5,
        proposal_count: 0,
        model_attempts: 0,
        budget_ms: 120_000,
        used_ms: 0,
        created_at_ms: 0,
        updated_at_ms: 0,
        queue_position: 0,
        calls: vec![],
        unknown_writes: vec![],
        unknown_reads: vec![],
        late_model_arrivals_ms: vec![],
        final_digest: None,
    };
    append_loop(&loops, &loop_record(LoopStatus::Created)).expect("append loop");
    append_loop(&loops, &loop_record(LoopStatus::Completed)).expect("append loop");
    let loaded = load_loops(&loops).expect("load loops");
    assert_eq!(loaded.len(), 1, "loop load folds by loop_id");
    assert_eq!(loaded[0].status, LoopStatus::Completed);

    let _ = std::fs::remove_dir_all(&base);
}

// ---------------------------------------------------------------- Task HTTP

#[tokio::test]
async fn task_update_accepts_only_five_statuses() {
    // tasks_update validates against a fixed five-status set; the
    // runner/restart/cancel statuses are not settable this way.
    let base = temp_base("task-update");
    let files_root = base.join("files");
    std::fs::create_dir_all(&files_root).expect("create files root");
    let state = AppState::with_files_root_and_approvals(files_root.clone(), None);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/tasks/create", r#"{"directory":""}"#).await;
    assert!(status.contains("200"), "create: {status} {body}");
    let created: serde_json::Value = serde_json::from_str(&body).expect("create json");
    assert_eq!(created["task_id"], "task-1");
    assert_eq!(created["status"], "created");

    for status in ["draft_ready", "completed", "rejected", "failed", "created"] {
        let (s, b) = post(
            addr,
            "/tasks/update",
            &format!(r#"{{"task_id":"task-1","status":"{status}","artifact_path":null}}"#),
        )
        .await;
        assert!(s.contains("200"), "update to {status}: {s} {b}");
        let updated: serde_json::Value = serde_json::from_str(&b).expect("update json");
        assert_eq!(updated["status"], status);
    }
    for status in ["running", "interrupted", "cancelled", "bogus"] {
        let (s, b) = post(
            addr,
            "/tasks/update",
            &format!(r#"{{"task_id":"task-1","status":"{status}","artifact_path":null}}"#),
        )
        .await;
        assert!(s.contains("400"), "update to {status} must be 400: {s} {b}");
    }

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn task_restart_converts_running_to_interrupted() {
    // Fold first, then convert: only the latest record per task
    // may convert, and only converted records are appended.
    let base = temp_base("task-restart");
    let files_root = base.join("files");
    std::fs::create_dir_all(&files_root).expect("create files root");
    let tasks_path = base.join("tasks.jsonl");
    append_task(&tasks_path, &task_record("task-1", "", "running"));
    append_task(&tasks_path, &task_record("task-2", "", "running"));
    append_task(&tasks_path, &task_record("task-2", "", "completed"));
    append_task(&tasks_path, &task_record("task-3", "", "cancelled"));
    append_task(&tasks_path, &task_record("task-4", "", "interrupted"));

    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_tasks_path(tasks_path.clone());
    let (addr, server) = start_server(state).await;
    let (status, body) = get(addr, "/tasks/list").await;
    assert!(status.contains("200"), "list: {status} {body}");
    let listed: Vec<serde_json::Value> = serde_json::from_str(&body).expect("list json");
    let by_id = |id: &str| {
        listed
            .iter()
            .find(|t| t["task_id"] == id)
            .unwrap_or_else(|| panic!("missing {id}"))
            .clone()
    };
    assert_eq!(by_id("task-1")["status"], "interrupted");
    assert_eq!(by_id("task-2")["status"], "completed", "fold first: stale running must not convert");
    assert_eq!(by_id("task-3")["status"], "cancelled");
    assert_eq!(by_id("task-4")["status"], "interrupted");

    // Only the converted record was appended: task-1 gains one
    // interrupted line, task-2 gains none.
    let lines = json_lines(&tasks_path);
    assert_eq!(lines.len(), 6, "5 seeded + 1 converted line");
    let task1: Vec<&serde_json::Value> =
        lines.iter().filter(|v| v["task_id"] == "task-1").collect();
    assert_eq!(task1.len(), 2);
    assert_eq!(task1[1]["status"], "interrupted");
    let task2: Vec<&serde_json::Value> =
        lines.iter().filter(|v| v["task_id"] == "task-2").collect();
    assert_eq!(task2.len(), 2, "completed task must not gain a line");

    // Resume contract: accepted, not completed. The immediate
    // response is the pre-run snapshot, still interrupted.
    let (status, body) = post(addr, "/tasks/resume", r#"{"task_id":"task-1"}"#).await;
    assert!(status.contains("200"), "resume interrupted: {status} {body}");
    let resumed: serde_json::Value = serde_json::from_str(&body).expect("resume json");
    assert_eq!(resumed["status"], "interrupted");
    // A non-interrupted task is not resumable.
    let (status, _body) = post(addr, "/tasks/resume", r#"{"task_id":"task-2"}"#).await;
    assert!(status.contains("409"), "resume completed must be 409: {status}");

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}

// ---------------------------------------------------------------- Loop restart

fn loop_fixture(id: &str, status: LoopStatus, calls: Vec<CallRecord>) -> LoopRecord {
    LoopRecord {
        loop_id: id.to_string(),
        prompt_digest: ContentDigest { sha256: "x".to_string(), bytes: 0 },
        model: "m".to_string(),
        status,
        error: None,
        max_calls: 5,
        proposal_count: 0,
        model_attempts: 0,
        budget_ms: 120_000,
        used_ms: 0,
        created_at_ms: 0,
        updated_at_ms: 0,
        queue_position: 0,
        calls,
        unknown_writes: vec![],
        unknown_reads: vec![],
        late_model_arrivals_ms: vec![],
        final_digest: None,
    }
}

fn call_fixture(id: usize, tool: &str, status: CallStatus) -> CallRecord {
    CallRecord {
        call_id: id,
        tool: tool.to_string(),
        path: Some("out".to_string()),
        args_sha256: "abc".to_string(),
        content_digest: None,
        status,
        error: None,
        approval_id: None,
        binding: None,
        submitted: false,
        duplicate_of: None,
        result: None,
        late: None,
    }
}

#[tokio::test]
async fn loop_restart_converts_non_terminal_to_interrupted() {
    // Loop restart is terminal and lossy: non-terminal records
    // become Interrupted, in-flight calls become Unknown, and the
    // in-memory conversation (messages/results/final_text) is
    // rebuilt empty, so resume on the old loop is a 409.
    let base = temp_base("loop-restart");
    let root = base.join("files");
    std::fs::create_dir_all(&root).expect("create files root");
    let loops_path = base.join("loops.jsonl");
    append_loop(
        &loops_path,
        &loop_fixture(
            "loop-1",
            LoopStatus::Running,
            vec![
                call_fixture(0, "fs.write", CallStatus::Running),
                call_fixture(1, "fs.read", CallStatus::Succeeded),
            ],
        ),
    )
    .expect("seed loop-1");
    append_loop(&loops_path, &loop_fixture("loop-2", LoopStatus::AwaitingApproval, vec![]))
        .expect("seed loop-2");
    append_loop(
        &loops_path,
        &loop_fixture("loop-3", LoopStatus::Completed, vec![call_fixture(
            0,
            "fs.read",
            CallStatus::Succeeded,
        )]),
    )
    .expect("seed loop-3");
    append_loop(
        &loops_path,
        &loop_fixture(
            "loop-4",
            LoopStatus::Cancelled,
            vec![call_fixture(0, "fs.write", CallStatus::Running)],
        ),
    )
    .expect("seed loop-4");

    let engine = Arc::new(
        LoopEngine::new(&root, Some(loops_path), None, None, Timeouts::default())
            .expect("engine new"),
    );

    let view = engine.get("loop-1").expect("get loop-1");
    assert_eq!(view.record.status, LoopStatus::Interrupted);
    assert_eq!(view.record.error, Some(ErrorCode::Interrupted));
    assert_eq!(view.record.calls[0].status, CallStatus::Unknown);
    assert_eq!(view.record.calls[0].error, Some(ErrorCode::Interrupted));
    assert_eq!(view.record.calls[1].status, CallStatus::Succeeded, "finished calls are untouched");
    assert_eq!(view.final_text, None, "restarted loop has no final text in memory");

    let view = engine.get("loop-2").expect("get loop-2");
    assert_eq!(view.record.status, LoopStatus::Interrupted);

    let view = engine.get("loop-3").expect("get loop-3");
    assert_eq!(view.record.status, LoopStatus::Completed, "terminal records are untouched");

    // A cancelled loop keeps its status, but its in-flight call
    // still converts to Unknown: it must not be left as running.
    let view = engine.get("loop-4").expect("get loop-4");
    assert_eq!(view.record.status, LoopStatus::Cancelled);
    assert_eq!(view.record.calls[0].status, CallStatus::Unknown);

    let dummy = AppState::with_files_root_and_approvals(root.clone(), None);
    let err = engine
        .resume(
            dummy,
            ResumeLoop { loop_id: "loop-1".to_string(), approval_id: None, binding: None },
        )
        .await
        .expect_err("resume on an interrupted loop must fail");
    assert_eq!(err.0, 409);
    assert_eq!(err.1, "loop_is_terminal");

    let _ = std::fs::remove_dir_all(&base);
}

// ---------------------------------------------------------------- Device reload

#[tokio::test]
async fn device_unknown_survives_reload() {
    // Device actions are not auto-resumed: with_actions_path only
    // folds lines into a map and appends nothing, so unknown stays
    // unknown until a manual refresh resolves it.
    let base = temp_base("device-reload");
    let files_root = base.join("files");
    std::fs::create_dir_all(&files_root).expect("create files root");
    let actions_path = base.join("actions.jsonl");
    append_action(&actions_path, &action_record("action-1", "unknown"));
    append_action(&actions_path, &action_record("action-2", "requested"));
    append_action(&actions_path, &action_record("action-3", "completed"));

    let state = AppState::with_files_root_and_approvals(files_root.clone(), None)
        .with_actions_path(actions_path.clone());
    let (addr, server) = start_server(state).await;
    let (status, body) = get(addr, "/devices/actions/get?action_id=action-1").await;
    assert!(status.contains("200"), "get unknown: {status} {body}");
    let got: serde_json::Value = serde_json::from_str(&body).expect("get json");
    assert_eq!(got["status"], "unknown");
    let (status, body) = get(addr, "/devices/actions/get?action_id=action-2").await;
    assert!(status.contains("200"), "get requested: {status} {body}");
    let got: serde_json::Value = serde_json::from_str(&body).expect("get json");
    assert_eq!(got["status"], "requested");

    // No conversion append happened on load.
    assert_eq!(json_lines(&actions_path).len(), 3);

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}

// ---------------------------------------------------------------- Approvals fold

#[tokio::test]
async fn approvals_fold_last_wins_via_http() {
    let base = temp_base("approvals-fold");
    let files_root = base.join("files");
    std::fs::create_dir_all(&files_root).expect("create files root");
    let approvals_path = base.join("approvals.jsonl");
    append_record(&approvals_path, &approval_record("approval-1", "fs.write", "pending"))
        .expect("append");
    append_record(&approvals_path, &approval_record("approval-1", "fs.write", "approved"))
        .expect("append");
    append_record(&approvals_path, &approval_record("approval-2", "echo", "pending"))
        .expect("append");

    let state =
        AppState::with_files_root_and_approvals(files_root.clone(), Some(approvals_path));
    let (addr, server) = start_server(state).await;
    let (status, body) = get(addr, "/approvals").await;
    assert!(status.contains("200"), "list: {status} {body}");
    let listed: Vec<serde_json::Value> = serde_json::from_str(&body).expect("list json");
    assert_eq!(listed.len(), 2, "folded by id, not one view per line");
    let first = listed.iter().find(|v| v["tool_call_id"] == "approval-1").expect("approval-1");
    assert_eq!(first["status"], "approved");
    let second = listed.iter().find(|v| v["tool_call_id"] == "approval-2").expect("approval-2");
    assert_eq!(second["status"], "pending");

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}

// ---------------------------------------------------------------- Approval models

use mock_gateway::device_backend::{FakeReminderBackend, Reminder};
use mock_gateway::loop_engine::LoopModel;
use std::collections::VecDeque;
use std::time::Duration;

// Scripted model + in-memory file tools, mirroring loop_smoke's
// harness at minimum size: one write proposal, then done.
struct ScriptModel {
    steps: Mutex<VecDeque<ModelReply>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self { steps: Mutex::new(replies.into_iter().collect()) }
    }
}

impl LoopModel for ScriptModel {
    fn name(&self) -> &str {
        "scripted-snapshot"
    }

    fn complete(
        &self,
        _messages: Vec<Message>,
        _timeout: Duration,
    ) -> BackendFuture<Result<ModelReply, ModelError>> {
        let reply = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move { Ok(reply) })
    }
}

struct FakeFiles {
    files: Arc<Mutex<BTreeMap<String, String>>>,
    writes: Arc<AtomicUsize>,
}

impl LoopTools for FakeFiles {
    fn execute(&self, tool: String, args: BTreeMap<String, String>) -> BackendFuture<ToolOutcome> {
        let files = Arc::clone(&self.files);
        let writes = Arc::clone(&self.writes);
        Box::pin(async move {
            match tool.as_str() {
                "fs.write" => {
                    writes.fetch_add(1, Ordering::SeqCst);
                    files
                        .lock()
                        .unwrap()
                        .insert(args["path"].clone(), args["text"].clone());
                    ToolOutcome::success("saved and verified".into())
                }
                "fs.read" => files
                    .lock()
                    .unwrap()
                    .get(&args["path"])
                    .map(|s| ToolOutcome::success(s.clone()))
                    .unwrap_or_else(|| ToolOutcome::failed(ErrorCode::ToolFailed)),
                "fs.list" => ToolOutcome::success(
                    serde_json::to_string(&files.lock().unwrap().keys().collect::<Vec<_>>())
                        .unwrap(),
                ),
                _ => ToolOutcome::failed(ErrorCode::ToolFailed),
            }
        })
    }
}

#[tokio::test]
async fn loop_binding_consumption_snapshot() {
    // Model 1: loop-bound approval. The approval carries a
    // binding (loop/call/tool/args digest); resume enforces the
    // binding match (403 on mismatch), consumes exactly once at
    // execution, and carries no operation_digest.
    let base = temp_base("loop-binding");
    let root = base.join("files");
    std::fs::create_dir_all(&root).expect("create files root");
    let approvals_path = base.join("approvals.jsonl");
    let loops_path = base.join("loops.jsonl");
    let files_map: Arc<Mutex<BTreeMap<String, String>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let writes = Arc::new(AtomicUsize::new(0));
    let model = Arc::new(ScriptModel::new(vec![
        ModelReply::Calls {
            proposals: vec![Proposal {
                tool: "fs.write".to_string(),
                args: serde_json::json!({"path": "memo.txt", "text": "hello"}),
                wire: None,
            }],
            content: None,
        },
        ModelReply::Final("finished".to_string()),
    ]));
    let state = AppState::with_files_root_and_approvals(root.clone(), Some(approvals_path.clone()))
        .with_loop_backends(
            Some(loops_path),
            Some(model),
            Some(Arc::new(FakeFiles { files: Arc::clone(&files_map), writes: Arc::clone(&writes) })),
            Timeouts::default(),
        )
        .expect("loop backends")
        .with_auto_drive(false);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/agent/loop/start", r#"{"prompt":"snapshot task"}"#).await;
    assert!(status.contains("200"), "start: {status} {body}");
    let loop_id = serde_json::from_str::<serde_json::Value>(&body)
        .expect("start json")["loop_id"]
        .as_str()
        .expect("loop_id")
        .to_string();

    // One resume is one engine step: the first step only queues
    // the model's write proposal (still running, no approval
    // yet); the second step requests the approval for it.
    let (status, body) = post(addr, "/agent/loop/resume", &format!(r#"{{"loop_id":"{loop_id}"}}"#)).await;
    assert!(status.contains("200"), "first resume: {status} {body}");
    let view: serde_json::Value = serde_json::from_str(&body).expect("view json");
    assert_eq!(view["status"], "running");
    assert_eq!(view["calls"].as_array().expect("calls array").len(), 1);

    let (status, body) = post(addr, "/agent/loop/resume", &format!(r#"{{"loop_id":"{loop_id}"}}"#)).await;
    assert!(status.contains("200"), "second resume: {status} {body}");
    let view: serde_json::Value = serde_json::from_str(&body).expect("view json");
    assert_eq!(view["status"], "awaiting_approval");
    let qp = view["queue_position"].as_u64().expect("queue_position") as usize;
    let call = &view["calls"][qp];
    let approval_id = call["approval_id"].as_str().expect("approval_id").to_string();
    let binding = call["binding"].clone();
    assert_eq!(binding["tool"], "fs.write");
    assert_eq!(binding["loop_id"], loop_id);
    // Call ids are 1-based (calls.len() + 1 at proposal time),
    // while queue_position is the 0-based index into calls: the
    // binding must point at the queued call, whatever its id is.
    assert_eq!(binding["call_id"], call["call_id"]);
    assert_eq!(call["call_id"], 1);
    assert!(!binding["args_sha256"].as_str().expect("args digest").is_empty());

    // A tampered binding is refused and burns nothing.
    let mut tampered = binding.clone();
    tampered["args_sha256"] = serde_json::json!("0".repeat(64));
    let (status, _body) = post(
        addr,
        "/agent/loop/resume",
        &format!(r#"{{"loop_id":"{loop_id}","approval_id":"{approval_id}","binding":{tampered}}}"#),
    )
    .await;
    assert!(status.contains("403"), "tampered binding must be 403: {status}");
    assert_eq!(writes.load(Ordering::SeqCst), 0, "mismatch must not execute");

    let (status, body) = post(
        addr,
        "/approval/decide",
        &format!(r#"{{"tool_call_id":"{approval_id}","status":"approved"}}"#),
    )
    .await;
    assert!(status.contains("200"), "decide: {status} {body}");

    let (status, body) = post(
        addr,
        "/agent/loop/resume",
        &format!(r#"{{"loop_id":"{loop_id}","approval_id":"{approval_id}","binding":{binding}}}"#),
    )
    .await;
    assert!(status.contains("200"), "approved resume: {status} {body}");
    let view: serde_json::Value = serde_json::from_str(&body).expect("view json");
    assert_eq!(view["calls"][qp]["status"], "succeeded");
    assert_eq!(writes.load(Ordering::SeqCst), 1, "exactly one execution");
    assert_eq!(
        files_map.lock().unwrap().get("memo.txt").map(String::as_str),
        Some("hello")
    );

    // Finish the loop (the driver is off, so step manually unless
    // it already completed).
    let (status, body) = get(addr, &format!("/agent/loop/get?loop_id={loop_id}")).await;
    assert!(status.contains("200"), "get: {status} {body}");
    let view: serde_json::Value = serde_json::from_str(&body).expect("get json");
    if view["status"] != "completed" {
        let (status, body) = post(addr, "/agent/loop/resume", &format!(r#"{{"loop_id":"{loop_id}"}}"#)).await;
        assert!(status.contains("200"), "final resume: {status} {body}");
        let view: serde_json::Value = serde_json::from_str(&body).expect("final view json");
        assert_eq!(view["status"], "completed");
    }

    // Phase 2 (Q5): the loop model now carries the unified digest
    // alongside the binding: consumed, bound, and the digest equals
    // the binding's wire digest. The gate checks the reference
    // triple plus this stored digest, never the wire field alone.
    let record = last_approval_record(&approvals_path, &approval_id);
    assert_eq!(record["consumed"], true);
    assert_eq!(record["binding"]["loop_id"], loop_id);
    assert_eq!(record["binding"]["tool"], "fs.write");
    assert_eq!(record["operation_digest"], record["binding"]["args_sha256"]);

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}

fn b64(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

async fn approve_write(
    addr: std::net::SocketAddr,
    path: &str,
    content: &[u8],
    append: bool,
) -> String {
    let (status, body) = post(
        addr,
        "/approval/request",
        &format!(
            "{{\"tool\":\"fs.write\",\"path\":\"{path}\",\"content_base64\":\"{}\",\"append\":{append}}}",
            b64(content)
        ),
    )
    .await;
    assert!(status.contains("200"), "request: {status} {body}");
    let id = serde_json::from_str::<serde_json::Value>(&body)
        .expect("request json")["tool_call_id"]
        .as_str()
        .expect("approval id")
        .to_string();
    let (status, body) = post(
        addr,
        "/approval/decide",
        &format!(r#"{{"tool_call_id":"{id}","status":"approved"}}"#),
    )
    .await;
    assert!(status.contains("200"), "decide: {status} {body}");
    id
}

#[tokio::test]
async fn operation_digest_consumption_snapshot() {
    // Model 2: operation-digest approval. The server computes the
    // digest from the exact execution parameters; the record
    // carries no binding, execution consumes exactly once, and a
    // parameter mismatch refuses without burning the approval.
    let base = temp_base("digest");
    let files_root = base.join("files");
    std::fs::create_dir_all(&files_root).expect("create files root");
    let approvals_path = base.join("approvals.jsonl");
    let state = AppState::with_files_root_and_approvals(
        files_root.clone(),
        Some(approvals_path.clone()),
    );
    let (addr, server) = start_server(state).await;

    let id = approve_write(addr, "a.txt", b"hello", false).await;
    let (status, body) = post(
        addr,
        "/files/write",
        &format!(
            "{{\"path\":\"a.txt\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}",
            b64(b"hello")
        ),
    )
    .await;
    assert!(status.contains("200"), "write: {status} {body}");
    assert_eq!(std::fs::read(files_root.join("a.txt")).expect("read written"), b"hello");
    let record = last_approval_record(&approvals_path, &id);
    assert_eq!(record["consumed"], true);
    assert!(record["binding"].is_null(), "digest approvals carry no binding");
    assert!(!record["operation_digest"].as_str().expect("digest").is_empty());
    // P0-C idempotency (contract §3, task item 4): a second
    // execution with the same approval replays the terminal
    // Effect instead of executing again — 200 with
    // replayed=true, file content unchanged. (A *different*
    // write with the spent approval is still 403.)
    let (status, body) = post(
        addr,
        "/files/write",
        &format!(
            "{{\"path\":\"a.txt\",\"content_base64\":\"{}\",\"approval_id\":\"{id}\"}}",
            b64(b"hello")
        ),
    )
    .await;
    assert!(status.contains("200"), "second write must replay: {status} {body}");
    assert!(body.contains("\"replayed\":true"), "replay flag: {body}");
    assert_eq!(std::fs::read(files_root.join("a.txt")).expect("read written"), b"hello");

    // Mismatch path: changed content, then changed path, both
    // refused without consuming; the exact write still works.
    let id2 = approve_write(addr, "b.txt", b"hello", false).await;
    let (status, _body) = post(
        addr,
        "/files/write",
        &format!(
            "{{\"path\":\"b.txt\",\"content_base64\":\"{}\",\"approval_id\":\"{id2}\"}}",
            b64(b"helloo")
        ),
    )
    .await;
    assert!(status.contains("403"), "changed content must be 403: {status}");
    let (status, _body) = post(
        addr,
        "/files/write",
        &format!(
            "{{\"path\":\"c.txt\",\"content_base64\":\"{}\",\"approval_id\":\"{id2}\"}}",
            b64(b"hello")
        ),
    )
    .await;
    assert!(status.contains("403"), "changed path must be 403: {status}");
    assert_eq!(last_approval_record(&approvals_path, &id2)["consumed"], false);
    assert!(!files_root.join("b.txt").exists());
    assert!(!files_root.join("c.txt").exists());
    let (status, body) = post(
        addr,
        "/files/write",
        &format!(
            "{{\"path\":\"b.txt\",\"content_base64\":\"{}\",\"approval_id\":\"{id2}\"}}",
            b64(b"hello")
        ),
    )
    .await;
    assert!(status.contains("200"), "exact write after mismatches: {status} {body}");
    assert_eq!(last_approval_record(&approvals_path, &id2)["consumed"], true);

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}

#[tokio::test]
async fn device_check_only_consumption_snapshot() {
    // Phase 2 (Q4) REPLACES model 3: device_set now binds the
    // approval to the action id and consumes it at first use, so
    // the record is approved, consumed, and bound to action-1
    // after a successful set. This test was intentionally updated
    // for the Q4 change; it is not a regression.
    // Model 3 (pre-phase-2): device check-only. device_set
    // verified the approval (no binding, tool match, approved
    // status) but never set consumed.
    let base = temp_base("device-check");
    let files_root = base.join("files");
    std::fs::create_dir_all(&files_root).expect("create files root");
    let approvals_path = base.join("approvals.jsonl");
    let fake = Arc::new(FakeReminderBackend::new());
    fake.add_reminder(Reminder {
        id: "reminder-1".to_string(),
        title: "Test reminder 1".to_string(),
        completed: false,
    });
    let state = AppState::with_files_root_and_approvals(
        files_root.clone(),
        Some(approvals_path.clone()),
    )
    .with_device_backend(fake);
    let (addr, server) = start_server(state).await;

    let (status, body) = post(addr, "/approval/request", r#"{"tool":"reminders.set_completed"}"#).await;
    assert!(status.contains("200"), "request: {status} {body}");
    let approval_id = serde_json::from_str::<serde_json::Value>(&body)
        .expect("request json")["tool_call_id"]
        .as_str()
        .expect("approval id")
        .to_string();
    let (status, body) = post(
        addr,
        "/approval/decide",
        &format!(r#"{{"tool_call_id":"{approval_id}","status":"approved"}}"#),
    )
    .await;
    assert!(status.contains("200"), "decide: {status} {body}");

    let (status, body) = post(
        addr,
        "/devices/actions/create",
        r#"{"reminder_id":"reminder-1","target_completed":true}"#,
    )
    .await;
    assert!(status.contains("200"), "create action: {status} {body}");
    let action_id = serde_json::from_str::<serde_json::Value>(&body)
        .expect("create json")["action_id"]
        .as_str()
        .expect("action id")
        .to_string();
    assert_eq!(action_id, "action-1");

    let (status, body) = post(
        addr,
        "/devices/reminders/set",
        &format!(r#"{{"action_id":"{action_id}","tool_call_id":"{approval_id}"}}"#),
    )
    .await;
    assert!(status.contains("200"), "set: {status} {body}");
    let done: serde_json::Value = serde_json::from_str(&body).expect("set json");
    assert_eq!(done["status"], "completed");

    let record = last_approval_record(&approvals_path, &approval_id);
    assert_eq!(record["status"], "approved");
    assert_eq!(record["consumed"], true, "phase 2 (Q4): device set consumes the approval");
    assert_eq!(record["action_binding"], "action-1", "phase 2 (Q4): device set binds to the action");
    assert!(record["binding"].is_null());
    assert!(record["operation_digest"].is_null());

    server.abort();
    let _ = std::fs::remove_dir_all(&base);
}
