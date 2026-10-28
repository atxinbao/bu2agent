//! Shell command tests that need no window.
//! Run single-threaded: the file test asserts the root starts
//! empty, and all tests share one server's state.
use std::collections::HashMap;

use crate::tool_commands::*;

/// 本地假 HTTP 响应验证真实 IPC 与桌面 DTO；状态机语义由网关集成测试负责。
#[test]
fn loop_workflow_invoke_roundtrip() {
    use crate::loop_commands::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use serde_json::{json, Value};
    let binding = json!({"loop_id":"loop-8","call_id":1,"tool":"fs.write","args_sha256":"hash"});
    let mut view = json!({
        "loop_id":"loop-8","prompt_digest":{"sha256":"prompt-hash","bytes":6},"model":"fixture",
        "status":"created","error":null,"max_calls":3,"proposal_count":0,"model_attempts":0,
        "budget_ms":45000,"used_ms":0,"created_at_ms":1,"updated_at_ms":1,"queue_position":0,
        "calls":[],"unknown_writes":[],"unknown_reads":[],"late_model_arrivals_ms":[],"final_digest":null,
        "in_flight":false,"remaining_ms":45000,"final_text":null,"pending_write":null,"pending_reminder":null,"approval_status":null
    });
    let created = view.clone();
    view["status"] = json!("awaiting_approval");
    view["proposal_count"] = json!(1);
    view["calls"] = json!([{"call_id":1,"tool":"fs.write","path":"out","args_sha256":"hash",
        "content_digest":{"sha256":"content-hash","bytes":6},"status":"awaiting_approval","error":null,
        "approval_id":"approval-9","binding":binding,"submitted":false,"duplicate_of":null,"result":null,"late":null}]);
    view["pending_write"] = json!({"call_id":1,"path":"out","text":"你好","args_sha256":"hash"});
    view["approval_status"] = json!("pending");
    let waiting = view.clone();
    view["status"] = json!("completed");
    view["pending_write"] = Value::Null;
    view["approval_status"] = Value::Null;
    view["final_text"] = json!("完成");
    let completed = view.clone();
    view["status"] = json!("cancelled");
    let cancelled = view;
    let expectations = vec![
        ("POST", "/agent/loop/start", json!({"prompt":"你好","max_calls":3,"budget_ms":45000}), created.clone()),
        ("POST", "/agent/loop/resume", json!({"loop_id":"loop-8","approval_id":null,"binding":null}), waiting.clone()),
        ("GET", "/agent/loop/get?loop_id=loop-8", Value::Null, waiting.clone()),
        ("GET", "/agent/loop/list", Value::Null, json!([waiting.clone()])),
        ("POST", "/agent/loop/resume", json!({"loop_id":"loop-8","approval_id":"approval-9","binding":binding}), completed.clone()),
        ("POST", "/agent/loop/cancel", json!({"loop_id":"loop-8"}), cancelled.clone()),
    ];
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        for (method, path, expected, response) in expectations {
            let (mut socket, _) = listener.accept().unwrap();
            socket.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
            let mut bytes = Vec::new();
            let (header_end, len) = loop {
                let mut chunk = [0; 1024];
                let n = socket.read(&mut chunk).unwrap();
                assert!(n > 0); bytes.extend_from_slice(&chunk[..n]);
                if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..i]);
                    assert!(headers.starts_with(&format!("{method} {path} HTTP/1.1")));
                    let len = headers.lines().find_map(|s| s.strip_prefix("Content-Length: ")).unwrap_or("0").parse::<usize>().unwrap();
                    break (i + 4, len);
                }
            };
            while bytes.len() < header_end + len {
                let mut chunk = [0; 1024]; let n = socket.read(&mut chunk).unwrap(); assert!(n > 0); bytes.extend_from_slice(&chunk[..n]);
            }
            let body: Value = if len == 0 { Value::Null } else { serde_json::from_slice(&bytes[header_end..header_end + len]).unwrap() };
            assert_eq!(body, expected);
            let body = response.to_string();
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body).unwrap();
        }
    });
    crate::gateway_endpoint::set_base_url(&base_url);
    let app = tauri::test::mock_builder()
        .invoke_handler(tauri::generate_handler![loop_start, loop_get, loop_list, loop_resume, loop_cancel])
        .build(tauri::test::mock_context(tauri::test::noop_assets())).unwrap();
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default()).build().unwrap();
    let invoke = |cmd: &str, body: Value| {
        tauri::test::get_ipc_response(&webview, tauri::webview::InvokeRequest {
            cmd:cmd.into(),callback:tauri::ipc::CallbackFn(0),error:tauri::ipc::CallbackFn(1),
            url:"tauri://localhost".parse().unwrap(),body:tauri::ipc::InvokeBody::Json(body),
            headers:Default::default(),invoke_key:tauri::test::INVOKE_KEY.into(),
        }).unwrap_or_else(|e| panic!("{cmd}: {e:?}")).deserialize::<Value>().unwrap()
    };
    assert_eq!(invoke("loop_start", json!({"prompt":"你好","max_calls":3,"budget_ms":45000})), created);
    assert_eq!(invoke("loop_resume", json!({"loop_id":"loop-8","approval_id":null,"binding":null})), waiting);
    assert_eq!(invoke("loop_get", json!({"loop_id":"loop-8"})), waiting);
    assert_eq!(invoke("loop_list", json!({})), json!([waiting]));
    assert_eq!(invoke("loop_resume", json!({"loop_id":"loop-8","approval_id":"approval-9","binding":binding})), completed);
    assert_eq!(invoke("loop_cancel", json!({"loop_id":"loop-8"})), cancelled);
    server.join().unwrap();
    crate::gateway_endpoint::reset_base_url();
}

/// 真实 Mock 网关未配置循环模型时，错误经异步 IPC 返回，不能变成回显成功。
#[test]
fn loop_unconfigured_invoke_errors() {
    use crate::loop_commands::*;
    crate::gateway_endpoint::reset_base_url();
    let app = tauri::test::mock_builder()
        .invoke_handler(tauri::generate_handler![loop_start, loop_get])
        .build(tauri::test::mock_context(tauri::test::noop_assets())).unwrap();
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default()).build().unwrap();
    let error = |cmd: &str, body: serde_json::Value| {
        tauri::test::get_ipc_response(&webview, tauri::webview::InvokeRequest {
            cmd:cmd.into(),callback:tauri::ipc::CallbackFn(0),error:tauri::ipc::CallbackFn(1),
            url:"tauri://localhost".parse().unwrap(),body:tauri::ipc::InvokeBody::Json(body),
            headers:Default::default(),invoke_key:tauri::test::INVOKE_KEY.into(),
        }).unwrap_err().to_string()
    };
    assert!(error("loop_start", serde_json::json!({"prompt":"test","max_calls":5,"budget_ms":120000})).contains("503"));
    assert!(error("loop_get", serde_json::json!({"loop_id":"loop-999999"})).contains("404"));
    assert!(error("loop_get", serde_json::json!({"loopId":"loop-1"})).contains("loop_id"));
}

/// 经过真正的 invoke 参数解码，避免直接调用 Rust 函数遗漏桥接字段名错误。
#[test]
fn chat_invoke_roundtrip() {
    use crate::gateway_commands::*;
    let app = tauri::test::mock_builder()
        .invoke_handler(tauri::generate_handler![gateway_stream_start, gateway_stream_apply, gateway_stream_cancel])
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .expect("build mock shell");
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default()).build().unwrap();
    let invoke = |cmd: &str, body: serde_json::Value| {
        tauri::test::get_ipc_response(&webview, tauri::webview::InvokeRequest {
            cmd: cmd.to_string(),
            callback: tauri::ipc::CallbackFn(0),
            error: tauri::ipc::CallbackFn(1),
            url: "tauri://localhost".parse().unwrap(),
            body: tauri::ipc::InvokeBody::Json(body),
            headers: Default::default(),
            invoke_key: tauri::test::INVOKE_KEY.to_string(),
        }).expect("invoke response").deserialize::<serde_json::Value>().unwrap()
    };
    let records = invoke("gateway_stream_start", serde_json::json!({
        "stream_id": "shell-invoke-chat", "turn_id": "shell-invoke-turn", "text": "桥接测试\n\"你好\""
    }));
    assert_eq!(records.as_array().unwrap().len(), 4);
    let applied = invoke("gateway_stream_apply", serde_json::json!({"stream_id":"shell-invoke-chat", "records":records}));
    assert_eq!(applied["kept"].as_array().unwrap().len(), 4);
    let payload: serde_json::Value = serde_json::from_str(applied["kept"][1]["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload["text"], "桥接测试\n\"你好\"");
    assert_eq!(invoke("gateway_stream_cancel", serde_json::json!({"stream_id":"shell-invoke-chat"})), true);
}

fn args(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn file_commands_roundtrip() {
    let entries = file_list("".to_string()).expect("list root");
    assert!(entries.is_empty(), "file root should start empty");
    assert!(
        file_write(
            "shell-test.txt".to_string(),
            "hello shell".to_string(),
            false,
            false
        )
        .is_err(),
        "unapproved write must be refused client-side"
    );
    let size = file_write(
        "shell-test.txt".to_string(),
        "hello shell".to_string(),
        true,
        false,
    )
    .expect("approved write");
    assert_eq!(size, 11);
    let entries = file_list("".to_string()).expect("list after write");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, "shell-test.txt");
    assert!(!entries[0].is_dir);
    let text =
        file_read_text("shell-test.txt".to_string(), 4096, 1048576).expect("read back");
    assert_eq!(text, "hello shell");
    assert!(
        file_delete("shell-test.txt".to_string(), false).is_err(),
        "unapproved delete must be refused client-side"
    );
    file_delete("shell-test.txt".to_string(), true).expect("approved delete");
    assert!(
        file_list("".to_string()).expect("list after delete").is_empty(),
        "file root should be empty again"
    );
}

#[test]
fn agent_commands_roundtrip() {
    let result = agent_run_tool(
        "echo".to_string(),
        args(&[("text", "hi")]),
        "approve".to_string(),
    )
    .expect("echo");
    assert_eq!(result.tool, "echo");
    assert_eq!(result.result, serde_json::json!("hi"));
    let pending = agent_run_tool(
        "fs.write".to_string(),
        args(&[("path", "shell-agent.txt"), ("text", "from shell")]),
        "ask".to_string(),
    );
    let err = pending.expect_err("ask must not execute");
    assert!(err.starts_with("approval_pending:"), "ask error: {err}");
    let written = agent_run_tool(
        "fs.write".to_string(),
        args(&[("path", "shell-agent.txt"), ("text", "from shell")]),
        "approve".to_string(),
    )
    .expect("approved write");
    assert_eq!(written.result["size_bytes"], serde_json::json!(10));
    let read = agent_run_tool(
        "fs.read".to_string(),
        args(&[("path", "shell-agent.txt")]),
        "approve".to_string(),
    )
    .expect("read back");
    assert_eq!(read.result, serde_json::json!("from shell"));
    let request = agent_request_approval(
        "fs.delete".to_string(),
        Some(args(&[("path", "shell-agent.txt")])),
    )
    .expect("request delete");
    assert!(request.requires_approval);
    let id = request.tool_call_id.expect("delete approval id");
    agent_decide_approval(id.clone(), "approve".to_string()).expect("decide approved");
    let deleted = agent_call_tool(
        "fs.delete".to_string(),
        args(&[("path", "shell-agent.txt")]),
        Some(id),
    )
    .expect("call delete");
    assert_eq!(deleted.result["deleted"], serde_json::json!(true));
    let echo_request = agent_request_approval("echo".to_string(), None).expect("request echo");
    assert!(!echo_request.requires_approval);
    assert_eq!(echo_request.tool_call_id, None);
}

#[test]
fn connector_commands_roundtrip() {
    let note = connector_notes_create(
        "Shell note".to_string(),
        "written by the shell test".to_string(),
        "approve".to_string(),
    )
    .expect("create note");
    assert!(note.id.starts_with("note-"), "note id: {}", note.id);
    assert_eq!(note.title, "Shell note");
    let notes = connector_notes_list().expect("list notes");
    assert!(notes.iter().any(|n| n.id == note.id));
    let pending = connector_notes_create(
        "Nope".to_string(),
        "never created".to_string(),
        "ask".to_string(),
    );
    let err = pending.expect_err("ask must not create");
    assert!(err.starts_with("approval_pending:"), "ask error: {err}");
    let message = connector_mail_send(
        "someone@mock.local".to_string(),
        "Hello".to_string(),
        "from the shell test".to_string(),
        "approve".to_string(),
    )
    .expect("send mail");
    assert!(message.id.starts_with("mail-"), "mail id: {}", message.id);
    let messages = connector_mail_list().expect("list mail");
    assert!(messages.iter().any(|m| m.id == message.id));
    assert!(
        connector_mail_send(
            "someone@example.com".to_string(),
            "Hi".to_string(),
            "x".to_string(),
            "approve".to_string()
        )
        .is_err(),
        "outside-domain mail must be refused"
    );
}

#[test]
fn task_commands_roundtrip() {
    let task = task_create("".to_string()).expect("create task");
    assert!(task.task_id.starts_with("task-"), "task id: {}", task.task_id);
    assert_eq!(task.status, "created");
    assert_eq!(task.artifact_path, None);
    let tasks = task_list().expect("list tasks");
    assert!(tasks.iter().any(|t| t.task_id == task.task_id));
    let fetched = task_get(task.task_id.clone()).expect("get task");
    assert_eq!(fetched.directory, "");
    let draft = task_update(task.task_id.clone(), "draft_ready".to_string(), None)
        .expect("draft ready");
    assert_eq!(draft.status, "draft_ready");
    let done = task_update(
        task.task_id.clone(),
        "completed".to_string(),
        Some("report.md".to_string()),
    )
    .expect("complete");
    assert_eq!(done.status, "completed");
    assert_eq!(done.artifact_path, Some("report.md".to_string()));
    assert!(
        task_update(task.task_id.clone(), "bogus".to_string(), None).is_err(),
        "unknown status must be refused"
    );
    assert!(
        task_get("task-999999".to_string()).is_err(),
        "unknown task must be refused"
    );
}

/// 使用前端实际传入的字段名，覆盖任务流程依赖的文件、审批和任务 IPC。
#[test]
fn task_workflow_invoke_roundtrip() {
    let app = tauri::test::mock_builder()
        .invoke_handler(tauri::generate_handler![
            file_write, file_read_text, file_delete,
            agent_request_approval, agent_decide_approval, agent_call_tool,
            task_create, task_get, task_update,
        ])
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .expect("build mock shell");
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
        .build().unwrap();
    let invoke = |cmd: &str, body: serde_json::Value| {
        tauri::test::get_ipc_response(&webview, tauri::webview::InvokeRequest {
            cmd: cmd.to_string(),
            callback: tauri::ipc::CallbackFn(0),
            error: tauri::ipc::CallbackFn(1),
            url: "tauri://localhost".parse().unwrap(),
            body: tauri::ipc::InvokeBody::Json(body),
            headers: Default::default(),
            invoke_key: tauri::test::INVOKE_KEY.to_string(),
        }).unwrap_or_else(|err| panic!("{cmd}: {err:?}"))
            .deserialize::<serde_json::Value>().unwrap()
    };
    let path = "shell-task-invoke.txt";
    let text = "任务材料：\"引号\"\n第二行";
    invoke("file_write", serde_json::json!({
        "path": path, "text": text, "approved": true, "append": false
    }));
    assert_eq!(invoke("file_read_text", serde_json::json!({
        "path": path, "chunk_len": 7, "max_total": 1048576
    })), text);
    let task = invoke("task_create", serde_json::json!({"directory": ""}));
    let id = task["task_id"].as_str().unwrap();
    assert_eq!(invoke("task_get", serde_json::json!({"task_id": id})), task);
    let draft = invoke("task_update", serde_json::json!({
        "task_id": id, "status": "draft_ready", "artifact_path": null
    }));
    assert_eq!(draft["status"], "draft_ready");
    let approval = invoke("agent_request_approval", serde_json::json!({"tool":"fs.write","args":{"path": path, "text": "生成的报告"}}));
    let approval_id = &approval["tool_call_id"];
    invoke("agent_decide_approval", serde_json::json!({
        "tool_call_id": approval_id, "decision": "approve"
    }));
    let written = invoke("agent_call_tool", serde_json::json!({
        "tool": "fs.write", "args": {"path": path, "text": "生成的报告"},
        "tool_call_id": approval_id
    }));
    assert_eq!(written["result"]["size_bytes"], "生成的报告".len());
    let done = invoke("task_update", serde_json::json!({
        "task_id": id, "status": "completed", "artifact_path": path
    }));
    assert_eq!(done["status"], "completed");
    assert_eq!(done["artifact_path"], path);
    assert_eq!(invoke("task_get", serde_json::json!({"task_id": id})), done);
    assert_eq!(invoke("file_read_text", serde_json::json!({
        "path": path, "chunk_len": 4096, "max_total": 1048576
    })), "生成的报告");
    invoke("file_delete", serde_json::json!({"path":path,"approved":true}));
}

#[test]
fn device_commands_roundtrip() {
    let reminders = device_reminders_list().expect("list reminders");
    let demo = reminders.iter().find(|r| r.id == "reminder-demo").expect("demo reminder");
    let target = !demo.completed;
    let action = device_actions_create(demo.id.clone(), target).expect("create action");
    assert!(action.action_id.starts_with("action-"), "action id: {}", action.action_id);
    assert_eq!(action.status, "requested");
    assert_eq!(action.reminder_id, "reminder-demo");
    assert_eq!(action.target_completed, target);
    let request =
        agent_request_approval("reminders.set_completed".to_string(), None).expect("request approval");
    assert!(request.requires_approval);
    let id = request.tool_call_id.expect("approval id");
    agent_decide_approval(id.clone(), "approve".to_string()).expect("decide approved");
    let done = device_reminder_set(action.action_id.clone(), id).expect("set reminder");
    assert_eq!(done.status, "completed");
    let fetched = device_action_get(action.action_id.clone()).expect("get action");
    assert_eq!(fetched.status, "completed");
    let reminders = device_reminders_list().expect("relist reminders");
    let demo = reminders.iter().find(|r| r.id == "reminder-demo").expect("demo reminder");
    assert_eq!(demo.completed, target);
    let actions = device_actions_list().expect("list actions");
    assert!(actions.iter().any(|a| a.action_id == action.action_id));
    // Rejection path: a second action, rejected, never changes the device.
    let second =
        device_actions_create("reminder-demo".to_string(), !target).expect("create second action");
    let request2 =
        agent_request_approval("reminders.set_completed".to_string(), None).expect("request second approval");
    let id2 = request2.tool_call_id.expect("second approval id");
    agent_decide_approval(id2.clone(), "reject".to_string()).expect("decide rejected");
    let rejected =
        device_action_mark_rejected(second.action_id.clone(), id2).expect("mark rejected");
    assert_eq!(rejected.status, "rejected");
    let reminders = device_reminders_list().expect("relist after rejection");
    let demo = reminders.iter().find(|r| r.id == "reminder-demo").expect("demo reminder");
    assert_eq!(demo.completed, target, "rejection must not touch the device");
    // Refresh on a non-unknown action returns it unchanged.
    let refreshed = device_action_refresh(action.action_id.clone()).expect("refresh completed");
    assert_eq!(refreshed.status, "completed");
    assert!(
        device_action_get("action-999999".to_string()).is_err(),
        "unknown action must be refused"
    );
}

/// 使用前端实际传入的字段名，覆盖设备流程依赖的提醒、审批和动作 IPC。
#[test]
fn device_workflow_invoke_roundtrip() {
    let app = tauri::test::mock_builder()
        .invoke_handler(tauri::generate_handler![
            device_reminders_list, device_actions_create, device_reminder_set,
            device_action_mark_rejected, device_action_refresh, device_actions_list,
            device_action_get, agent_request_approval, agent_decide_approval,
        ])
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .expect("build mock shell");
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
        .build().unwrap();
    let invoke = |cmd: &str, body: serde_json::Value| {
        tauri::test::get_ipc_response(&webview, tauri::webview::InvokeRequest {
            cmd: cmd.to_string(),
            callback: tauri::ipc::CallbackFn(0),
            error: tauri::ipc::CallbackFn(1),
            url: "tauri://localhost".parse().unwrap(),
            body: tauri::ipc::InvokeBody::Json(body),
            headers: Default::default(),
            invoke_key: tauri::test::INVOKE_KEY.to_string(),
        }).unwrap_or_else(|err| panic!("{cmd}: {err:?}"))
            .deserialize::<serde_json::Value>().unwrap()
    };
    let reminders = invoke("device_reminders_list", serde_json::json!({}));
    let demo = reminders.as_array().unwrap().iter().find(|r| r["id"] == "reminder-demo").expect("demo reminder");
    let current = demo["completed"].as_bool().unwrap();
    let target = !current;
    let action = invoke("device_actions_create", serde_json::json!({"reminder_id": "reminder-demo", "target_completed": target}));
    let action_id = action["action_id"].as_str().unwrap();
    assert_eq!(action["status"], "requested");
    let approval = invoke("agent_request_approval", serde_json::json!({"tool": "reminders.set_completed"}));
    let approval_id = &approval["tool_call_id"];
    invoke("agent_decide_approval", serde_json::json!({"tool_call_id": approval_id, "decision": "approve"}));
    let done = invoke("device_reminder_set", serde_json::json!({"action_id": action_id, "tool_call_id": approval_id}));
    assert_eq!(done["status"], "completed");
    assert_eq!(invoke("device_action_get", serde_json::json!({"action_id": action_id})), done);
    let reminders = invoke("device_reminders_list", serde_json::json!({}));
    let demo = reminders.as_array().unwrap().iter().find(|r| r["id"] == "reminder-demo").expect("demo reminder");
    assert_eq!(demo["completed"], target);
    assert_eq!(invoke("device_action_refresh", serde_json::json!({"action_id": action_id}))["status"], "completed");
    let action2 = invoke("device_actions_create", serde_json::json!({"reminder_id": "reminder-demo", "target_completed": current}));
    let action2_id = action2["action_id"].as_str().unwrap();
    let approval2 = invoke("agent_request_approval", serde_json::json!({"tool": "reminders.set_completed"}));
    let approval2_id = &approval2["tool_call_id"];
    invoke("agent_decide_approval", serde_json::json!({"tool_call_id": approval2_id, "decision": "reject"}));
    let rejected = invoke("device_action_mark_rejected", serde_json::json!({"action_id": action2_id, "tool_call_id": approval2_id}));
    assert_eq!(rejected["status"], "rejected");
    let list = invoke("device_actions_list", serde_json::json!({}));
    assert!(list.as_array().unwrap().iter().any(|a| a["action_id"] == action_id));
}

/// Settings working directory through real invoke decoding:
/// save/load roundtrip, root reset, out-of-root and missing
/// refusals, and invalid stored values loading as invalid
/// (never silently reinterpreted). Runs against isolated temp
/// dirs: BU2AGENT_APP_DATA_DIR and MOCK_FILES_ROOT are swapped
/// for this test only and restored after (single-threaded run).
/// Key commands are NOT tested here: they touch the real
/// Keychain handle, which no test may mutate. The picker
/// command is NOT tested either: it needs a real dialog and
/// the dialog plugin is not registered in test builds.
#[test]
fn settings_working_dir_invoke_roundtrip() {
    use crate::gateway_lifecycle::GatewayLifecycle;
    use crate::settings_commands::*;
    use serde_json::json;
    let prev_data = std::env::var("BU2AGENT_APP_DATA_DIR").ok();
    let prev_root = std::env::var("MOCK_FILES_ROOT").ok();
    let base = std::env::temp_dir().join(format!("bu2agent-settings-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let root = base.join("files");
    let data = base.join("data");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&data).unwrap();
    std::env::set_var("BU2AGENT_APP_DATA_DIR", &data);
    std::env::set_var("MOCK_FILES_ROOT", &root);
    let app = tauri::test::mock_builder()
        .manage(GatewayLifecycle::default())
        .invoke_handler(tauri::generate_handler![
            settings_get,
            settings_save_working_dir,
            settings_clear_working_dir
        ])
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .unwrap();
    let webview =
        tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
            .build()
            .unwrap();
    let invoke = |cmd: &str, body: serde_json::Value| {
        tauri::test::get_ipc_response(
            &webview,
            tauri::webview::InvokeRequest {
                cmd: cmd.to_string(),
                callback: tauri::ipc::CallbackFn(0),
                error: tauri::ipc::CallbackFn(1),
                url: "tauri://localhost".parse().unwrap(),
                body: tauri::ipc::InvokeBody::Json(body),
                headers: Default::default(),
                invoke_key: tauri::test::INVOKE_KEY.to_string(),
            },
        )
        .unwrap_or_else(|e| panic!("{cmd}: {e:?}"))
        .deserialize::<serde_json::Value>()
        .unwrap()
    };
    let invoke_err = |cmd: &str, body: serde_json::Value| {
        tauri::test::get_ipc_response(
            &webview,
            tauri::webview::InvokeRequest {
                cmd: cmd.to_string(),
                callback: tauri::ipc::CallbackFn(0),
                error: tauri::ipc::CallbackFn(1),
                url: "tauri://localhost".parse().unwrap(),
                body: tauri::ipc::InvokeBody::Json(body),
                headers: Default::default(),
                invoke_key: tauri::test::INVOKE_KEY.to_string(),
            },
        )
        .unwrap_err()
        .to_string()
    };
    let got = invoke("settings_get", json!({}));
    assert_eq!(got["default_working_dir"], serde_json::Value::Null);
    assert_eq!(got["working_dir_valid"], true);
    assert_eq!(got["working_dir_editable"], true);
    let projects = root.join("projects");
    std::fs::create_dir_all(&projects).unwrap();
    let saved = invoke(
        "settings_save_working_dir",
        json!({"path": projects.to_string_lossy()}),
    );
    assert_eq!(saved["default_working_dir"], "projects");
    assert_eq!(saved["working_dir_valid"], true);
    assert_eq!(invoke("settings_get", json!({}))["default_working_dir"], "projects");
    let saved = invoke(
        "settings_save_working_dir",
        json!({"path": root.to_string_lossy()}),
    );
    assert_eq!(saved["default_working_dir"], serde_json::Value::Null);
    let outside = base.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    assert!(invoke_err(
        "settings_save_working_dir",
        json!({"path": outside.to_string_lossy()}),
    )
    .contains("outside the file root"));
    assert!(invoke_err(
        "settings_save_working_dir",
        json!({"path": base.join("gone").to_string_lossy()}),
    )
    .contains("does not exist"));
    let cleared = invoke("settings_clear_working_dir", json!({}));
    assert_eq!(cleared["default_working_dir"], serde_json::Value::Null);
    // Hand-corrupted settings must load as invalid, not as a
    // silent fallback: missing dir, absolute path, escape.
    for bad in ["gone", "/absolute", "../escape"] {
        std::fs::write(
            data.join("settings.json"),
            format!("{{\"default_working_dir\":\"{bad}\"}}"),
        )
        .unwrap();
        let got = invoke("settings_get", json!({}));
        assert_eq!(got["working_dir_valid"], false, "stored: {bad}");
    }
    match prev_data {
        Some(v) => std::env::set_var("BU2AGENT_APP_DATA_DIR", v),
        None => std::env::remove_var("BU2AGENT_APP_DATA_DIR"),
    }
    match prev_root {
        Some(v) => std::env::set_var("MOCK_FILES_ROOT", v),
        None => std::env::remove_var("MOCK_FILES_ROOT"),
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// A symlink inside the root pointing outside it must be
/// refused: canonicalization on both sides defeats the escape.
#[cfg(unix)]
#[test]
fn settings_working_dir_symlink_escape_refused() {
    use crate::gateway_lifecycle::GatewayLifecycle;
    use crate::settings_commands::*;
    use serde_json::json;
    let prev_data = std::env::var("BU2AGENT_APP_DATA_DIR").ok();
    let prev_root = std::env::var("MOCK_FILES_ROOT").ok();
    let base = std::env::temp_dir().join(format!("bu2agent-settings-link-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let root = base.join("files");
    let data = base.join("data");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&data).unwrap();
    std::env::set_var("BU2AGENT_APP_DATA_DIR", &data);
    std::env::set_var("MOCK_FILES_ROOT", &root);
    let outside = base.join("outside");
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
    let app = tauri::test::mock_builder()
        .manage(GatewayLifecycle::default())
        .invoke_handler(tauri::generate_handler![settings_save_working_dir])
        .build(tauri::test::mock_context(tauri::test::noop_assets()))
        .unwrap();
    let webview =
        tauri::WebviewWindowBuilder::new(&app, "main", Default::default())
            .build()
            .unwrap();
    let err = tauri::test::get_ipc_response(
        &webview,
        tauri::webview::InvokeRequest {
            cmd: "settings_save_working_dir".to_string(),
            callback: tauri::ipc::CallbackFn(0),
            error: tauri::ipc::CallbackFn(1),
            url: "tauri://localhost".parse().unwrap(),
            body: tauri::ipc::InvokeBody::Json(json!({"path": root.join("link").to_string_lossy()})),
            headers: Default::default(),
            invoke_key: tauri::test::INVOKE_KEY.to_string(),
        },
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("outside the file root"), "{err}");
    match prev_data {
        Some(v) => std::env::set_var("BU2AGENT_APP_DATA_DIR", v),
        None => std::env::remove_var("BU2AGENT_APP_DATA_DIR"),
    }
    match prev_root {
        Some(v) => std::env::set_var("MOCK_FILES_ROOT", v),
        None => std::env::remove_var("MOCK_FILES_ROOT"),
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// Reminders authorization status through the settings
/// command, against the script-started gateway. Only the
/// enum membership is asserted: the value depends on the
/// backend (Fake here, EventKit in the daily app).
#[test]
fn device_authorization_roundtrip() {
    use crate::settings_commands::device_authorization;
    crate::gateway_endpoint::reset_base_url();
    let status = device_authorization().expect("authorization");
    assert!(
        ["authorized", "denied", "not_determined"].contains(&status.as_str()),
        "unexpected status: {status}"
    );
}

/// 会话命令通过真实 IPC 验证字段命名、DTO、隔离与删除；使用回归服务的假模型。
#[test]
fn session_workflow_invoke_roundtrip() {
    use crate::session_commands::*;
    use serde_json::{json, Value};
    crate::gateway_endpoint::reset_base_url();
    let app = tauri::test::mock_builder()
        .invoke_handler(tauri::generate_handler![session_create, session_list, session_get, session_send, session_delete])
        .build(tauri::test::mock_context(tauri::test::noop_assets())).unwrap();
    let webview = tauri::WebviewWindowBuilder::new(&app, "main", Default::default()).build().unwrap();
    let invoke = |cmd: &str, body: Value| {
        tauri::test::get_ipc_response(&webview, tauri::webview::InvokeRequest {
            cmd: cmd.into(), callback: tauri::ipc::CallbackFn(0), error: tauri::ipc::CallbackFn(1),
            url: "tauri://localhost".parse().unwrap(), body: tauri::ipc::InvokeBody::Json(body),
            headers: Default::default(), invoke_key: tauri::test::INVOKE_KEY.into(),
        }).unwrap_or_else(|e| panic!("{cmd}: {e:?}")).deserialize::<Value>().unwrap()
    };
    let a = invoke("session_create", json!({"title":"IPC A"}));
    let b = invoke("session_create", json!({"title":"IPC B"}));
    let id = &a["session_id"];
    let sent = invoke("session_send", json!({"session_id":id,"text":"仅属于 A"}));
    assert_eq!(sent["turns"].as_array().unwrap().len(), 2);
    assert_eq!(sent["turns"][1]["text"], "仅属于 A");
    assert_eq!(invoke("session_get", json!({"session_id":id})), sent);
    assert_eq!(invoke("session_get", json!({"session_id":b["session_id"]}))["turns"], json!([]));
    assert!(invoke("session_list", json!({})).as_array().unwrap().iter().any(|s| s["session_id"] == *id && s["turn_count"] == 2));
    invoke("session_delete", json!({"session_id":id}));
    assert!(!invoke("session_list", json!({})).as_array().unwrap().iter().any(|s| s["session_id"] == *id));
    invoke("session_delete", json!({"session_id":b["session_id"]}));
}
