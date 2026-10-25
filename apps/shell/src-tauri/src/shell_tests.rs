//! Shell command tests that need no window.
//! Run single-threaded: the file test asserts the root starts
//! empty, and all tests share one server's state.
use std::collections::HashMap;

use crate::tool_commands::*;

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
    let request = agent_request_approval("fs.delete".to_string()).expect("request delete");
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
    let echo_request = agent_request_approval("echo".to_string()).expect("request echo");
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
    let approval = invoke("agent_request_approval", serde_json::json!({"tool":"fs.write"}));
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
        agent_request_approval("reminders.set_completed".to_string()).expect("request approval");
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
        agent_request_approval("reminders.set_completed".to_string()).expect("request second approval");
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
