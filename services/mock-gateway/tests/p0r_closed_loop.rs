//! P0-R tests: local file/task closed loop on the delivered
//! kernel — write-time Evidence, post-hoc verify, generalized
//! reconcile, and the R1–R18 acceptance matrix (plan §5).
//!
//! Gateway kernel only: no client, no UI. R1–R18 map:
//! R1 overwrite verify; R2 drift mismatch + Inbox + auto-close;
//! R3 deleted target mismatch; R4 append verify via write-time
//! hash+len; R5 legacy append verify -> evidence_missing;
//! R6 delete/mkdir/rename verify; R7 export via Evidence target
//! path; R8 task product verify; R9 connector flags + secret
//! desensitization; R10 loop-fs unknown reconcile; R11
//! loop-reminder unknown reconcile; R12 started residual across
//! a real kill -9 restart; R13 409 taxonomy; R14 planned verify
//! refusals (rejected + cancelled loop); R15 replay/verify
//! non-interference; R16 restart persistence + no-approval
//! verify + caller-path ignored; R17 cross-view same fact;
//! R18 is the full suite + clippy run itself, not a test here.
//!
//! Cross-platform: temp dirs, ports, SQLite, and
//! `std::process::Child::kill()` only. No kill command, no
//! POSIX signals, no Unix-only path spellings.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_desktop::credential_store::MemoryCredentialStore;
use mock_gateway::device_backend::{FakeFault, FakeReminderBackend, Reminder};
use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p0r-{}-{}-{}",
        tag,
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

// Base64 literals for the fixed test contents (no encoder
// needed): "hello", "appended".
const HELLO_B64: &str = "aGVsbG8=";
const APPENDED_B64: &str = "YXBwZW5kZWQ=";

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
    fake: Arc<FakeReminderBackend>,
}

impl Harness {
    async fn serve_sqlite(
        dir: PathBuf,
        files_root: PathBuf,
        fake: Arc<FakeReminderBackend>,
        model_url: Option<String>,
    ) -> Self {
        let mut state = AppState::with_files_root_and_approvals(
            files_root,
            Some(dir.join("approvals.jsonl")),
        )
        .with_tasks_path(dir.join("tasks.jsonl"))
        .with_task_drafts_dir(dir.join("drafts"))
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_actions_path(dir.join("actions.jsonl"))
        .with_connector_state_path(dir.join("connectors.jsonl"))
        .with_vault_state_path(dir.join("vault.jsonl"))
        .with_vault_secrets(Arc::new(MemoryCredentialStore::new()))
        .with_device_backend(Arc::clone(&fake) as Arc<dyn mock_gateway::device_backend::ReminderBackend>);
        // /tasks/run refuses without a configured model; the
        // task test uses a local fake DeepSeek HTTP service
        // (never the real API, never a real credential).
        if let Some(base) = model_url {
            state = state.with_deepseek(mock_gateway::deepseek::DeepSeekClient::new(
                mock_gateway::deepseek::DeepSeekConfig {
                    base_url: base,
                    model: "fixture-3b".to_string(),
                    api_key: Some("test-key".to_string()),
                },
            ));
        }
        let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
        state.sqlite_import_and_switch().expect("switch");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
            fake,
        }
    }

    async fn serve_jsonl(
        dir: PathBuf,
        files_root: PathBuf,
        fake: Arc<FakeReminderBackend>,
    ) -> Self {
        let state = AppState::with_files_root_and_approvals(
            files_root,
            Some(dir.join("approvals.jsonl")),
        )
        .with_tasks_path(dir.join("tasks.jsonl"))
        .with_task_drafts_dir(dir.join("drafts"))
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_actions_path(dir.join("actions.jsonl"))
        .with_connector_state_path(dir.join("connectors.jsonl"))
        .with_vault_state_path(dir.join("vault.jsonl"))
        .with_vault_secrets(Arc::new(MemoryCredentialStore::new()))
        .with_device_backend(Arc::clone(&fake) as Arc<dyn mock_gateway::device_backend::ReminderBackend>);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
            fake,
        }
    }

    async fn new() -> Self {
        let dir = temp_dir("harness");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        let fake = Arc::new(FakeReminderBackend::new());
        Self::serve_sqlite(dir, files_root, fake, None).await
    }

    async fn new_with_model() -> Self {
        let dir = temp_dir("harness-model");
        let files_root = dir.join("files");
        std::fs::create_dir_all(&files_root).unwrap();
        let fake = Arc::new(FakeReminderBackend::new());
        let (addr, _server) = start_fake_model("task artifact body").await;
        Self::serve_sqlite(dir, files_root, fake, Some(format!("http://{addr}"))).await
    }

    fn stop(self) {
        self.server.abort();
    }

    fn files_root(&self) -> PathBuf {
        self.dir.join("files")
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

    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let response = self.client.get(format!("{}{}", self.url, path)).send().await.unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn effects(&self) -> Vec<Value> {
        let (status, body) = self.get("/effects/list").await;
        assert_eq!(status, 200, "{body}");
        body.as_array().unwrap().clone()
    }

    async fn effect(&self, id: &str) -> Value {
        let (status, body) = self.get(&format!("/effects/get?effect_id={id}")).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn request_approval(&self, tool: &str, extra: Value) -> String {
        let mut req = json!({"tool": tool});
        if let Value::Object(map) = extra {
            for (k, v) in map {
                req[k] = v;
            }
        }
        let body = self.ok("/approval/request", req).await;
        body["tool_call_id"].as_str().unwrap().to_string()
    }

    async fn decide(&self, id: &str, status: &str) {
        self.ok("/approval/decide", json!({"tool_call_id": id, "status": status}))
            .await;
    }

    async fn approval(&self, id: &str) -> Value {
        let (status, body) = self.get(&format!("/approvals/{id}")).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    /// Approved bound file write; returns the Effect id of the
    /// succeeded row (latest matching row).
    async fn write(&self, path: &str, content_b64: &str, append: bool) -> String {
        let approval = self
            .request_approval(
                "fs.write",
                json!({"path": path, "content_base64": content_b64, "append": append}),
            )
            .await;
        self.decide(&approval, "approved").await;
        let (status, body) = self
            .post(
                "/files/write",
                json!({"path": path, "content_base64": content_b64, "append": append, "approval_id": approval}),
            )
            .await;
        assert_eq!(status, 200, "{body}");
        self.latest_effect("fs", "write", path).await
    }

    async fn delete(&self, path: &str) -> String {
        let approval = self
            .request_approval("fs.delete", json!({"path": path}))
            .await;
        self.decide(&approval, "approved").await;
        let (status, body) = self
            .post("/files/delete", json!({"path": path, "approval_id": approval}))
            .await;
        assert_eq!(status, 200, "{body}");
        self.latest_effect("fs", "delete", path).await
    }

    async fn mkdir(&self, path: &str) -> String {
        let approval = self
            .request_approval("fs.mkdir", json!({"path": path}))
            .await;
        self.decide(&approval, "approved").await;
        let (status, body) = self
            .post("/files/mkdir", json!({"path": path, "approval_id": approval}))
            .await;
        assert_eq!(status, 200, "{body}");
        self.latest_effect("fs", "mkdir", path).await
    }

    async fn rename(&self, from: &str, to: &str) -> String {
        let approval = self
            .request_approval("fs.rename", json!({"path": from, "to_path": to}))
            .await;
        self.decide(&approval, "approved").await;
        let (status, body) = self
            .post("/files/rename", json!({"from": from, "to": to, "approval_id": approval}))
            .await;
        assert_eq!(status, 200, "{body}");
        self.latest_effect("fs", "rename", &format!("{from} -> {to}"))
            .await
    }

    async fn export(&self, from: &str, to: &str) -> String {
        let approval = self
            .request_approval("fs.export", json!({"path": from, "to_path": to}))
            .await;
        self.decide(&approval, "approved").await;
        let (status, body) = self
            .post("/files/export", json!({"from": from, "to": to, "approval_id": approval}))
            .await;
        assert_eq!(status, 200, "{body}");
        self.latest_effect("fs", "export", from).await
    }

    async fn latest_effect(&self, capability: &str, operation: &str, scope: &str) -> String {
        let effects = self.effects().await;
        effects
            .iter()
            .rev()
            .find(|e| {
                e["capability"] == capability
                    && e["operation"] == operation
                    && e["scope"] == scope
            })
            .expect("effect row")
            ["effect_id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn verify(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/verify", json!({"effect_id": effect_id})).await
    }

    async fn reconcile(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/reconcile", json!({"effect_id": effect_id}))
            .await
    }

    async fn evidence(&self) -> Vec<Value> {
        let (status, body) = self.get("/evidence/list").await;
        assert_eq!(status, 200, "{body}");
        body.as_array().unwrap().clone()
    }

    async fn evidence_for(&self, effect_id: &str) -> Vec<Value> {
        self.evidence()
            .await
            .into_iter()
            .filter(|e| e["effect_id"] == effect_id)
            .collect()
    }

    async fn inbox(&self, query: &str) -> Vec<Value> {
        let (status, body) = self.get(&format!("/inbox/list{query}")).await;
        assert_eq!(status, 200, "{body}");
        body.as_array().unwrap().clone()
    }
}

fn find_effect<'a>(effects: &'a [Value], capability: &str, operation: &str, scope: &str) -> &'a Value {
    effects
        .iter()
        .find(|e| {
            e["capability"] == capability && e["operation"] == operation && e["scope"] == scope
        })
        .expect("effect row")
}

/// Evidence rows carry `observed_json` as a serialized JSON
/// string (a TEXT column); parse it before asserting on fields.
fn observed(row: &Value) -> Value {
    serde_json::from_str(row["observed_json"].as_str().unwrap()).unwrap()
}

// ---------------------------------------------------------------- fake model server

/// Minimal canned DeepSeek chat-completion responder for the
/// task flow (same shape as effect_smoke's): reads one request,
/// answers the fixed body, closes. Never the real API.
async fn start_fake_model(text: &str) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = serde_json::to_string(
        &json!({"choices": [{"message": {"content": text}, "finish_reason": "stop"}]}),
    )
    .unwrap();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                continue;
            };
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let mut content_length = 0usize;
                let mut header_end = None;
                while header_end.is_none() {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
                        header_end = Some(pos + 4);
                        let head = String::from_utf8_lossy(&buf[..pos]).into_owned();
                        for line in head.lines() {
                            if let Some(v) = line.strip_prefix("Content-Length:") {
                                content_length = v.trim().parse().unwrap_or(0);
                            }
                        }
                    }
                }
                let header_end = header_end.unwrap();
                while buf.len() < header_end + content_length {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    (addr, handle)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

// ---------------------------------------------------------------- R1

#[tokio::test]
async fn r1_overwrite_verify_verified_and_effect_untouched() {
    let h = Harness::new().await;
    let effect_id = h.write("a.txt", HELLO_B64, false).await;
    assert_eq!(h.effect(&effect_id).await["status"], "succeeded");

    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["effect_id"], effect_id);
    assert_eq!(row["source"], "verify");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["reason"], "hash_match");
    assert_eq!(observed(&row)["kind"], "file");

    // Verify never changes the Effect row.
    let after = h.effect(&effect_id).await;
    assert_eq!(after["status"], "succeeded");
    assert!(after["reconciliation"].is_null());
    let rows = h.evidence_for(&effect_id).await;
    assert_eq!(rows.len(), 2, "write-time baseline + verify row");
    assert_eq!(rows[0]["source"], "write");
    assert_eq!(rows[1]["source"], "verify");
}

// ---------------------------------------------------------------- R2

#[tokio::test]
async fn r2_drift_mismatch_inbox_then_verified_auto_closes() {
    let h = Harness::new().await;
    let effect_id = h.write("b.txt", HELLO_B64, false).await;

    // Drift the file outside the gateway.
    std::fs::write(h.files_root().join("b.txt"), "drifted").unwrap();
    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "mismatch");
    assert_eq!(row["reason"], "hash_mismatch");

    // A verification_mismatch Inbox row opens.
    let open = h
        .inbox("?kind=verification_mismatch&open_only=true")
        .await;
    assert_eq!(open.len(), 1, "{open:?}");
    assert_eq!(open[0]["effect_id"], effect_id);
    let inbox_id = open[0]["inbox_id"].as_str().unwrap().to_string();

    // A second mismatch does not duplicate the open row.
    let (status, _) = h.verify(&effect_id).await;
    assert_eq!(status, 200);
    assert_eq!(
        h.inbox("?kind=verification_mismatch&open_only=true").await.len(),
        1
    );

    // Restore the file: verified, and the Inbox row auto-closes.
    std::fs::write(h.files_root().join("b.txt"), "hello").unwrap();
    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(
        h.inbox("?kind=verification_mismatch&open_only=true").await.len(),
        0
    );
    let all = h.inbox("?kind=verification_mismatch").await;
    let closed = all.iter().find(|r| r["inbox_id"] == inbox_id).unwrap();
    assert!(closed["resolved_ms"].is_number());
    assert_eq!(closed["resolved_reason"], "auto");

    // Ack also closes: drift again, then ack.
    std::fs::write(h.files_root().join("b.txt"), "drifted").unwrap();
    let (status, _) = h.verify(&effect_id).await;
    assert_eq!(status, 200);
    let open = h
        .inbox("?kind=verification_mismatch&open_only=true")
        .await;
    assert_eq!(open.len(), 1);
    let (status, acked) = h
        .post(
            "/inbox/ack",
            json!({"inbox_id": open[0]["inbox_id"]}),
        )
        .await;
    assert_eq!(status, 200, "{acked}");
    assert_eq!(acked["resolved_reason"], "acked");
    assert_eq!(
        h.inbox("?kind=verification_mismatch&open_only=true").await.len(),
        0
    );
}

// ---------------------------------------------------------------- R3

#[tokio::test]
async fn r3_deleted_target_mismatch() {
    let h = Harness::new().await;
    let effect_id = h.write("c.txt", HELLO_B64, false).await;
    std::fs::remove_file(h.files_root().join("c.txt")).unwrap();
    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "mismatch");
    assert_eq!(row["reason"], "target_missing");
    // The Effect row itself is untouched by the mismatch.
    assert_eq!(h.effect(&effect_id).await["status"], "succeeded");
}

// ---------------------------------------------------------------- R4

#[tokio::test]
async fn r4_append_verify_via_write_time_hash_len() {
    let h = Harness::new().await;
    let effect_id = h.write("d.txt", HELLO_B64, false).await;
    let append_id = h.write("d.txt", APPENDED_B64, true).await;
    // "hello" + "appended" = 13 bytes; the write-time baseline
    // records exactly that, so verify passes without knowing
    // the mode from the row.
    let (status, row) = h.verify(&append_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["reason"], "hash_match");
    assert_eq!(observed(&row)["len"], 13);
    assert_eq!(h.effect(&effect_id).await["status"], "succeeded");
}

// ---------------------------------------------------------------- R5

#[tokio::test]
async fn r5_legacy_append_verify_evidence_missing() {
    // Phase 1 (JSONL mode): a real append write. Its Effect row
    // has an append-mode digest and no Evidence (JSONL mode has
    // no Evidence at all).
    let dir = temp_dir("legacy-append");
    let files_root = dir.join("files");
    std::fs::create_dir_all(&files_root).unwrap();
    let fake = Arc::new(FakeReminderBackend::new());
    let h1 = Harness::serve_jsonl(dir.clone(), files_root.clone(), Arc::clone(&fake)).await;
    h1.write("legacy.txt", HELLO_B64, false).await;
    let legacy_id = h1.write("legacy.txt", APPENDED_B64, true).await;
    h1.stop();

    // Phase 2: import into SQLite, then verify. The overwrite
    // recompute cannot match an append digest, and the row
    // records no mode — so the honest answer is
    // evidence_missing, never a guessed mismatch, and no
    // Evidence row is fabricated.
    let h2 = Harness::serve_sqlite(dir, files_root, fake, None).await;
    assert_eq!(h2.effect(&legacy_id).await["status"], "succeeded");
    let (status, body) = h2.verify(&legacy_id).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "evidence_missing");
    assert!(h2.evidence_for(&legacy_id).await.is_empty());
}

// ---------------------------------------------------------------- R6

#[tokio::test]
async fn r6_delete_mkdir_rename_verify() {
    let h = Harness::new().await;
    // Delete: absent target verifies; a recreated target mismatches.
    let del_id = h.write("e.txt", HELLO_B64, false).await;
    let del_effect = h.delete("e.txt").await;
    assert_ne!(del_id, del_effect);
    let (status, row) = h.verify(&del_effect).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["reason"], "target_absent");
    std::fs::write(h.files_root().join("e.txt"), "hello").unwrap();
    let (status, row) = h.verify(&del_effect).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "mismatch");
    assert_eq!(row["reason"], "target_present");

    // Mkdir: present dir verifies; a removed dir mismatches.
    let dir_effect = h.mkdir("mydir").await;
    let (status, row) = h.verify(&dir_effect).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["reason"], "dir_present");
    std::fs::remove_dir(h.files_root().join("mydir")).unwrap();
    let (status, row) = h.verify(&dir_effect).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "mismatch");
    assert_eq!(row["reason"], "target_missing");

    // Rename: target content verifies; a modified target mismatches.
    h.write("old.txt", HELLO_B64, false).await;
    let ren_effect = h.rename("old.txt", "new.txt").await;
    let (status, row) = h.verify(&ren_effect).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["reason"], "hash_match");
    std::fs::write(h.files_root().join("new.txt"), "drifted").unwrap();
    let (status, row) = h.verify(&ren_effect).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "mismatch");
    assert_eq!(row["reason"], "hash_mismatch");
}

// ---------------------------------------------------------------- R7

#[tokio::test]
async fn r7_export_verify_reads_evidence_target_path() {
    let h = Harness::new().await;
    h.write("src.txt", HELLO_B64, false).await;
    let export_dir = h.dir.join("exports");
    std::fs::create_dir_all(&export_dir).unwrap();
    let target = export_dir.join("out.txt");
    let target_str = target.to_string_lossy().to_string();
    let export_id = h.export("src.txt", &target_str).await;

    // The Evidence baseline records the absolute target path.
    // The gateway canonicalizes it (canonicalize(parent) for a
    // not-yet-existing target), so the expectation must be
    // canonicalized the same way: on macOS temp_dir() sits
    // under symlinked /var (-> /private/var) and on Windows
    // canonicalize adds the \\?\ prefix, while the raw test
    // path has neither. Comparing against the raw path fails
    // deterministically on macOS + Windows and passes on Linux
    // (CI 41815085670). The request still carries the raw path;
    // only the expectation is canonicalized.
    let expected = std::fs::canonicalize(&export_dir)
        .unwrap()
        .join("out.txt");
    let expected_str = expected.to_string_lossy().to_string();
    let baseline = h
        .evidence_for(&export_id)
        .await
        .into_iter()
        .find(|e| e["source"] == "write")
        .expect("export write-time baseline");
    assert_eq!(observed(&baseline)["target_path"], expected_str);

    let (status, row) = h.verify(&export_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");

    // Delete the TARGET while the source still exists: verify
    // must report the target missing, proving it reads the
    // Evidence path and not the scope (source) path.
    std::fs::remove_file(&target).unwrap();
    assert!(h.files_root().join("src.txt").exists());
    let (status, row) = h.verify(&export_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "mismatch");
    assert_eq!(row["reason"], "target_missing");
}

// ---------------------------------------------------------------- R8

#[tokio::test]
async fn r8_task_product_verify() {
    let h = Harness::new_with_model().await;
    let notes = h.files_root().join("notes");
    std::fs::create_dir_all(&notes).unwrap();
    std::fs::write(notes.join("a.txt"), "alpha").unwrap();
    let (_, task_body) = h.post("/tasks/create", json!({"directory": "notes"})).await;
    let task_id = task_body["task_id"].as_str().unwrap().to_string();
    let (status, _) = h.post("/tasks/run", json!({"task_id": task_id})).await;
    assert_eq!(status, 200);
    let mut task = Value::Null;
    // 30 s budget (was 5 s): the 5 s bound is tight on slow CI
    // runners under full parallel load (macOS/Windows), and this
    // poll only waits longer before failing — no semantic change.
    for _ in 0..200 {
        let (_, got) = h.get(&format!("/tasks/get?task_id={task_id}")).await;
        if got["status"] == "draft_ready" {
            task = got;
            break;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    assert_eq!(task["status"], "draft_ready", "task never reached draft_ready: {task}");
    let task_approval = task["approval_id"].as_str().unwrap().to_string();
    h.decide(&task_approval, "approved").await;
    let (status, executed) = h.post("/tasks/execute", json!({"task_id": task_id})).await;
    assert_eq!(status, 200, "{executed}");
    assert_eq!(executed["status"], "completed");
    let artifact = executed["artifact_path"].as_str().unwrap().to_string();
    let artifact_abs = h.files_root().join(&artifact);
    assert!(artifact_abs.exists(), "artifact {artifact} missing");

    let effects = h.effects().await;
    let task_effect = find_effect(&effects, "tasks", "execute", &task_id);
    let effect_id = task_effect["effect_id"].as_str().unwrap().to_string();
    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["reason"], "hash_match");

    // Modify the artifact: the task conclusion no longer holds.
    std::fs::write(&artifact_abs, "tampered artifact").unwrap();
    let (status, row) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "mismatch");
    assert_eq!(row["reason"], "hash_mismatch");
    // The task record itself still says completed: verify
    // reports drift, it does not rewrite the task.
    let (_, got) = h.get(&format!("/tasks/get?task_id={task_id}")).await;
    assert_eq!(got["status"], "completed");
}

// ---------------------------------------------------------------- R9

#[tokio::test]
async fn r9_connector_flags_and_secret_desensitization() {
    let h = Harness::new().await;
    // Link: account present + linked verifies.
    let (_, linked) = h
        .post("/connectors/mail/accounts/link", json!({"display_name": "Mock"}))
        .await;
    let account_id = linked["account_id"].as_str().unwrap().to_string();
    let effects = h.effects().await;
    let link_effect = find_effect(&effects, "connectors", "link", &account_id);
    let link_id = link_effect["effect_id"].as_str().unwrap().to_string();
    let (status, row) = h.verify(&link_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    assert_eq!(row["reason"], "record_match");

    // Unlink: tombstone verifies; re-linking would mismatch the
    // unlink row (flags moved on).
    let (status, _) = h
        .post(
            &format!("/connectors/mail/accounts/{account_id}/unlink"),
            json!({}),
        )
        .await;
    assert_eq!(status, 200);
    let effects = h.effects().await;
    let unlink_effect = find_effect(&effects, "connectors", "unlink", &account_id);
    let unlink_id = unlink_effect["effect_id"].as_str().unwrap().to_string();
    let (status, row) = h.verify(&unlink_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");

    // Vault capture with a known secret: the Evidence observed
    // JSON must never contain the secret value.
    let secret = "s3cr3t-test-value-9f27";
    let (status, _) = h.post(
        "/v1/credentials/register",
        json!({"handle": "h1", "secret": secret, "provider": "test-provider"}),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = h
        .post("/v1/credentials/capture", json!({"handle": "h1", "secret": secret}))
        .await;
    assert_eq!(status, 200);
    let effects = h.effects().await;
    let cap_effect = find_effect(&effects, "credentials", "capture", "h1");
    let cap_id = cap_effect["effect_id"].as_str().unwrap().to_string();
    let (status, row) = h.verify(&cap_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    let observed_str = row["observed_json"].as_str().unwrap().to_string();
    assert!(
        !observed_str.contains(secret),
        "Evidence observed JSON must never carry credential values: {observed_str}"
    );
    // The full evidence list is equally clean.
    for e in h.evidence().await {
        assert!(!e["observed_json"].as_str().unwrap().contains(secret));
    }
}

// ---------------------------------------------------------------- loop harness

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
}

impl ScriptModel {
    fn new(replies: Vec<ModelReply>) -> Self {
        Self {
            steps: Mutex::new(
                replies
                    .into_iter()
                    .map(|r| (Duration::ZERO, Ok(r)))
                    .collect(),
            ),
        }
    }
}

impl LoopModel for ScriptModel {
    fn name(&self) -> &str {
        "scripted-test"
    }
    fn complete(
        &self,
        _messages: Vec<Message>,
        _timeout: Duration,
    ) -> BackendFuture<Result<ModelReply, ModelError>> {
        let (delay, reply) = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            reply
        })
    }
}

#[derive(Clone, Default)]
struct FakeFiles {
    files: Arc<Mutex<BTreeMap<String, String>>>,
}

impl LoopTools for FakeFiles {
    fn execute(&self, tool: String, args: BTreeMap<String, String>) -> BackendFuture<ToolOutcome> {
        let this = self.clone();
        Box::pin(async move {
            let mut files = this.files.lock().unwrap();
            match tool.as_str() {
                "fs.write" => {
                    files.insert(args["path"].clone(), args["text"].clone());
                    ToolOutcome::success("saved and verified".into())
                }
                "fs.read" => files
                    .get(&args["path"])
                    .map(|s| ToolOutcome::success(s.clone()))
                    .unwrap_or_else(|| ToolOutcome::failed(ErrorCode::ToolFailed)),
                "fs.list" => ToolOutcome::success(
                    serde_json::to_string(&files.keys().collect::<Vec<_>>()).unwrap(),
                ),
                _ => panic!("invalid tool reached executor"),
            }
        })
    }
}

fn proposal(tool: &str, args: Value) -> Proposal {
    Proposal {
        tool: tool.into(),
        args,
        wire: None,
    }
}

fn write_proposal(path: &str, text: &str) -> Proposal {
    proposal("fs.write", json!({"path": path, "text": text}))
}

fn reminder_proposal(id: &str) -> Proposal {
    proposal("reminders.set_completed", json!({"id": id, "completed": true}))
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

struct LoopHarness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    fake: Arc<FakeReminderBackend>,
}

impl LoopHarness {
    async fn new(replies: Vec<ModelReply>) -> Self {
        let dir = temp_dir("loop");
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let files = FakeFiles::default();
        let fake = Arc::new(FakeReminderBackend::new());
        let state = AppState::with_files_root_and_approvals(
            root.clone(),
            Some(dir.join("approvals.jsonl")),
        )
        .with_loop_backends(
            Some(dir.join("loops.jsonl")),
            Some(model),
            Some(Arc::new(files)),
            Timeouts::default(),
        )
        .unwrap()
        .with_auto_drive(false)
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_device_backend(Arc::clone(&fake) as Arc<dyn mock_gateway::device_backend::ReminderBackend>)
        .with_sqlite_path(dir.join("gateway.sqlite3"));
        state.sqlite_import_and_switch().expect("switch");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        // Detached: the loop server runs until the test runtime
        // ends; no handle is needed (nothing stops it mid-test).
        tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            fake,
        }
    }

    fn files_root(&self) -> PathBuf {
        self.dir.join("files")
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

    async fn ok(&self, path: &str, value: Value) -> Value {
        let (status, body) = self.post(path, value).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        let response = self.client.get(format!("{}{}", self.url, path)).send().await.unwrap();
        let status = response.status().as_u16();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        (status, body)
    }

    async fn start(&self) -> String {
        self.ok("/agent/loop/start", json!({"prompt": "private task prompt"}))
            .await["loop_id"]
            .as_str()
            .unwrap()
            .into()
    }

    async fn step(&self, id: &str) -> Value {
        self.ok("/agent/loop/resume", json!({"loop_id": id})).await
    }

    async fn effects(&self) -> Vec<Value> {
        let (status, body) = self.get("/effects/list").await;
        assert_eq!(status, 200, "{body}");
        body.as_array().unwrap().clone()
    }

    async fn effect(&self, id: &str) -> Value {
        let (status, body) = self.get(&format!("/effects/get?effect_id={id}")).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn transition(&self, effect_id: &str, status: &str) -> (u16, Value) {
        self.post(
            "/effects/transition",
            json!({"effect_id": effect_id, "status": status}),
        )
        .await
    }

    async fn reconcile(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/reconcile", json!({"effect_id": effect_id}))
            .await
    }

    async fn verify(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/verify", json!({"effect_id": effect_id})).await
    }

    async fn evidence_for(&self, effect_id: &str) -> Vec<Value> {
        let (status, body) = self.get("/evidence/list").await;
        assert_eq!(status, 200, "{body}");
        body.as_array()
            .unwrap()
            .iter()
            .filter(|e| e["effect_id"] == effect_id)
            .cloned()
            .collect()
    }

    /// Drive a planned Effect to `unknown` through the legal
    /// edges, as a stuck write would leave it.
    async fn drive_to_unknown(&self, effect_id: &str) {
        for to in ["authorized", "started", "unknown"] {
            let (status, _) = self.transition(effect_id, to).await;
            assert_eq!(status, 200, "to {to}");
        }
        assert_eq!(self.effect(effect_id).await["status"], "unknown");
    }

    /// Step a fresh loop until its proposal waits for approval,
    /// and return the planned Effect id for the proposal.
    async fn propose(&self, capability: &str, operation: &str, scope: &str) -> String {
        let loop_id = self.start().await;
        self.step(&loop_id).await;
        let waiting = self.step(&loop_id).await;
        assert_eq!(waiting["status"], "awaiting_approval", "{waiting}");
        let effects = self.effects().await;
        let eff = find_effect(&effects, capability, operation, scope);
        assert_eq!(eff["status"], "planned");
        eff["effect_id"].as_str().unwrap().to_string()
    }
}

// ---------------------------------------------------------------- R10

#[tokio::test]
async fn r10_loop_fs_unknown_reconcile() {
    let h = LoopHarness::new(vec![
        calls(vec![write_proposal("loop.txt", "loop body")]),
        calls(vec![write_proposal("loop2.txt", "second body")]),
    ])
    .await;

    // Case 1: the write actually landed (same content on disk).
    let effect_id = h.propose("fs", "write", "loop.txt").await;
    h.drive_to_unknown(&effect_id).await;
    std::fs::write(h.files_root().join("loop.txt"), "loop body").unwrap();
    let (status, body) = h.reconcile(&effect_id).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "reconciled");
    assert_eq!(body["reconciliation"]["result"], "completed");
    let mirrors: Vec<_> = h
        .evidence_for(&effect_id)
        .await
        .into_iter()
        .filter(|e| e["source"] == "reconcile")
        .collect();
    assert_eq!(mirrors.len(), 1);
    assert_eq!(mirrors[0]["verdict"], "verified");
    assert_eq!(mirrors[0]["reason"], "hash_match");

    // Case 2: content differs (drift, not the proposed write).
    // An ambiguous re-check never becomes `failed`: back to
    // `unknown`, no mirror, no conclusion.
    let effect2 = h.propose("fs", "write", "loop2.txt").await;
    h.drive_to_unknown(&effect2).await;
    std::fs::write(h.files_root().join("loop2.txt"), "something else").unwrap();
    let (status, body) = h.reconcile(&effect2).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "unknown");
    assert!(body["reconciliation"].is_null());
    assert!(h
        .evidence_for(&effect2)
        .await
        .iter()
        .all(|e| e["source"] != "reconcile"));
}

// ---------------------------------------------------------------- R11

#[tokio::test]
async fn r11_loop_reminder_unknown_reconcile() {
    let h = LoopHarness::new(vec![
        calls(vec![reminder_proposal("rem-1")]),
        calls(vec![reminder_proposal("rem-2")]),
    ])
    .await;

    // Case A: the target reminder no longer exists — the only
    // class allowed a `failed` reconcile conclusion.
    // (E2-C unification: the loop row now carries the unified
    // device triple; reconcile assertions unchanged.)
    let effect_id = h.propose("device", "reminders.set_completed", "rem-1").await;
    h.drive_to_unknown(&effect_id).await;
    let (status, body) = h.reconcile(&effect_id).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "reconciled");
    assert_eq!(body["reconciliation"]["result"], "failed");
    let mirrors: Vec<_> = h
        .evidence_for(&effect_id)
        .await
        .into_iter()
        .filter(|e| e["source"] == "reconcile")
        .collect();
    assert_eq!(mirrors.len(), 1);
    assert_eq!(mirrors[0]["verdict"], "mismatch");
    assert_eq!(mirrors[0]["reason"], "target_missing");

    // Case B: the reminder exists and is completed — landed.
    let effect2 = h.propose("device", "reminders.set_completed", "rem-2").await;
    h.drive_to_unknown(&effect2).await;
    h.fake.add_reminder(Reminder {
        id: "rem-2".to_string(),
        title: "Test reminder 2".to_string(),
        completed: true,
    });
    let (status, body) = h.reconcile(&effect2).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "reconciled");
    assert_eq!(body["reconciliation"]["result"], "completed");
    let mirrors: Vec<_> = h
        .evidence_for(&effect2)
        .await
        .into_iter()
        .filter(|e| e["source"] == "reconcile")
        .collect();
    assert_eq!(mirrors.len(), 1);
    assert_eq!(mirrors[0]["verdict"], "verified");
    assert_eq!(mirrors[0]["reason"], "record_match");
}

// ---------------------------------------------------------------- R14

#[tokio::test]
async fn r14_planned_verify_refused() {
    // Rejected approval: the planned row stays planned.
    let h = Harness::new().await;
    let approval = h
        .request_approval("fs.write", json!({"path": "r.txt", "content_base64": HELLO_B64}))
        .await;
    h.decide(&approval, "rejected").await;
    let effects = h.effects().await;
    let eff = find_effect(&effects, "fs", "write", "r.txt");
    assert_eq!(eff["status"], "planned");
    let effect_id = eff["effect_id"].as_str().unwrap().to_string();
    let (status, body) = h.verify(&effect_id).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_not_verifiable");
    assert_eq!(body["status"], "planned");
    assert_eq!(h.effect(&effect_id).await["status"], "planned");

    // Cancelled loop: the planned row stays planned.
    let lh = LoopHarness::new(vec![
        calls(vec![write_proposal("c.txt", "cancelled body")]),
        done(),
    ])
    .await;
    let loop_id = lh.start().await;
    lh.step(&loop_id).await;
    let waiting = lh.step(&loop_id).await;
    assert_eq!(waiting["status"], "awaiting_approval");
    let effects = lh.effects().await;
    let eff = find_effect(&effects, "fs", "write", "c.txt");
    assert_eq!(eff["status"], "planned");
    let effect_id = eff["effect_id"].as_str().unwrap().to_string();
    let (status, _) = lh
        .post("/agent/loop/cancel", json!({"loop_id": loop_id}))
        .await;
    assert_eq!(status, 200);
    let (status, body) = lh.verify(&effect_id).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_not_verifiable");
    assert_eq!(body["status"], "planned");
    assert_eq!(lh.effect(&effect_id).await["status"], "planned");
}

// ---------------------------------------------------------------- R12

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn wait_health(url: &str) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    for _ in 0..60 {
        if let Ok(response) = client.get(format!("{url}/health")).send().await {
            if response.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("gateway did not become healthy at {url}");
}

fn spawn_gateway(bin: &str, dir: &std::path::Path, port: u16) -> std::process::Child {
    std::process::Command::new(bin)
        .arg("--mock-chat")
        .env("MOCK_GATEWAY_ADDR", format!("127.0.0.1:{port}"))
        .env("MOCK_FILES_ROOT", dir.join("files"))
        .env("MOCK_APPROVALS_PATH", dir.join("approvals.jsonl"))
        .env("MOCK_EXECUTIONS_PATH", dir.join("executions.jsonl"))
        .env("MOCK_EFFECTS_PATH", dir.join("effects.jsonl"))
        .env("MOCK_SQLITE_PATH", dir.join("gateway.sqlite3"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn gateway")
}

#[tokio::test]
async fn r12_started_residual_across_kill9_restart_reconciles() {
    // Cross-platform kill form only: `Child::kill()` (SIGKILL
    // on Unix, TerminateProcess on Windows). Never the `kill`
    // command (P0-E Windows lesson).
    let bin = std::env::var("CARGO_BIN_EXE_mock-gateway").expect("mock-gateway test binary");
    let dir = temp_dir("kill9-started");
    std::fs::create_dir_all(dir.join("files")).unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let post = |url: &str, path: &str, value: Value| {
        let client = client.clone();
        let url = url.to_string();
        let path = path.to_string();
        async move {
            let response = client
                .post(format!("{url}{path}"))
                .json(&value)
                .send()
                .await
                .unwrap();
            let status = response.status().as_u16();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            (status, body)
        }
    };
    let get = |url: &str, path: &str| {
        let client = client.clone();
        let url = url.to_string();
        let path = path.to_string();
        async move {
            let response = client.get(format!("{url}{path}")).send().await.unwrap();
            let status = response.status().as_u16();
            let body: Value = response.json().await.unwrap_or(Value::Null);
            (status, body)
        }
    };

    let port = free_port();
    let url = format!("http://127.0.0.1:{port}");
    let mut child = spawn_gateway(&bin, &dir, port);
    wait_health(&url).await;

    // Land the file content first with a normal bound write.
    let (status, approval) = post(
        &url,
        "/approval/request",
        json!({"tool": "fs.write", "path": "crash.txt", "content_base64": HELLO_B64}),
    )
    .await;
    assert_eq!(status, 200, "{approval}");
    let approval_id = approval["tool_call_id"].as_str().unwrap().to_string();
    let (status, _) = post(
        &url,
        "/approval/decide",
        json!({"tool_call_id": approval_id, "status": "approved"}),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = post(
        &url,
        "/files/write",
        json!({"path": "crash.txt", "content_base64": HELLO_B64, "approval_id": approval_id}),
    )
    .await;
    assert_eq!(status, 200);

    // Plan the same write again and drive it to `started` —
    // the crash residual: the write was dispatched, its outcome
    // never recorded.
    let (status, approval2) = post(
        &url,
        "/approval/request",
        json!({"tool": "fs.write", "path": "crash.txt", "content_base64": HELLO_B64}),
    )
    .await;
    assert_eq!(status, 200, "{approval2}");
    let (_, effects) = get(&url, "/effects/list").await;
    let planned = effects
        .as_array()
        .unwrap()
        .iter()
        .find(|e| {
            e["capability"] == "fs" && e["operation"] == "write" && e["scope"] == "crash.txt" && e["status"] == "planned"
        })
        .expect("planned row")
        .clone();
    let effect_id = planned["effect_id"].as_str().unwrap().to_string();
    for to in ["authorized", "started"] {
        let (status, _) = post(
            &url,
            "/effects/transition",
            json!({"effect_id": effect_id, "status": to}),
        )
        .await;
        assert_eq!(status, 200, "to {to}");
    }

    // Kill -9, then restart on a fresh port over the same dir.
    child.kill().expect("kill gateway");
    let _ = child.wait();
    let port2 = free_port();
    let url2 = format!("http://127.0.0.1:{port2}");
    let mut child2 = spawn_gateway(&bin, &dir, port2);
    wait_health(&url2).await;

    // The started row survived the restart, and its file
    // content matches the digest: reconcile lands it.
    let (status, effect) = get(&url2, &format!("/effects/get?effect_id={effect_id}")).await;
    assert_eq!(status, 200, "{effect}");
    assert_eq!(effect["status"], "started");
    let (status, body) = post(
        &url2,
        "/effects/reconcile",
        json!({"effect_id": effect_id}),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "reconciled");
    assert_eq!(body["reconciliation"]["result"], "completed");
    let (_, evidence) = get(&url2, "/evidence/list").await;
    let mirror = evidence
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["effect_id"] == effect_id && e["source"] == "reconcile")
        .expect("reconcile mirror");
    assert_eq!(mirror["verdict"], "verified");

    child2.kill().expect("kill gateway again");
    let _ = child2.wait();
}

// ---------------------------------------------------------------- R13

#[tokio::test]
async fn r13_reconcile_409_taxonomy() {
    // Crafted legacy JSONL: an unknown fs row without a digest
    // (pre-P0-A shape), an unknown row with no observation
    // method, and an unknown device row whose backing action is
    // already completed. Actions stay JSONL (P0-D scope).
    let dir = temp_dir("crafted-409");
    let files_root = dir.join("files");
    std::fs::create_dir_all(&files_root).unwrap();
    std::fs::write(
        dir.join("effects.jsonl"),
        "{\"effect_id\":\"effect-1\",\"capability\":\"fs\",\"operation\":\"write\",\"scope\":\"old.txt\",\"status\":\"unknown\"}\n{\"effect_id\":\"effect-2\",\"capability\":\"bogus\",\"operation\":\"frob\",\"scope\":\"x\",\"status\":\"unknown\"}\n{\"effect_id\":\"effect-9\",\"capability\":\"device\",\"operation\":\"set_completed\",\"scope\":\"r1\",\"status\":\"unknown\"}\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("actions.jsonl"),
        "{\"action_id\":\"action-1\",\"device\":\"test\",\"action\":\"reminders.set_completed\",\"reminder_id\":\"r1\",\"target_completed\":true,\"status\":\"completed\",\"effect_id\":\"effect-9\"}\n",
    )
    .unwrap();
    let fake = Arc::new(FakeReminderBackend::new());
    let h = Harness::serve_sqlite(dir, files_root, fake, None).await;

    // Non-unknown rows refuse with their status preserved.
    let ok_id = h.write("ok.txt", HELLO_B64, false).await;
    let (status, body) = h.reconcile(&ok_id).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_not_unknown");
    assert_eq!(body["status"], "succeeded");

    // Planned rows refuse the same way.
    let approval = h
        .request_approval("fs.write", json!({"path": "p.txt", "content_base64": HELLO_B64}))
        .await;
    let _ = approval;
    let effects = h.effects().await;
    let planned = find_effect(&effects, "fs", "write", "p.txt");
    assert_eq!(planned["status"], "planned");
    let (status, body) = h
        .reconcile(planned["effect_id"].as_str().unwrap())
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_not_unknown");

    // Legacy row without the information to observe.
    let (status, body) = h.reconcile("effect-1").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "evidence_missing");
    assert_eq!(h.effect("effect-1").await["status"], "unknown");

    // Row with no observation method.
    let (status, body) = h.reconcile("effect-2").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_no_readback");
    assert_eq!(h.effect("effect-2").await["status"], "unknown");

    // Device row whose backing action is no longer unknown.
    let (status, body) = h.reconcile("effect-9").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "effect_action_mismatch");
    assert_eq!(h.effect("effect-9").await["status"], "unknown");
}

// ---------------------------------------------------------------- R15

#[tokio::test]
async fn r15_replay_then_verify_and_double_verify() {
    let h = Harness::new().await;
    let approval = h
        .request_approval("fs.write", json!({"path": "replay.txt", "content_base64": HELLO_B64}))
        .await;
    h.decide(&approval, "approved").await;
    let (status, first) = h
        .post(
            "/files/write",
            json!({"path": "replay.txt", "content_base64": HELLO_B64, "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{first}");
    // Replay: same approval, same params — no second write.
    let (status, replayed) = h
        .post(
            "/files/write",
            json!({"path": "replay.txt", "content_base64": HELLO_B64, "approval_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{replayed}");
    assert_eq!(replayed["replayed"], true);
    let effect_id = replayed["effect_id"].as_str().unwrap().to_string();
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("replay.txt")).unwrap(),
        "hello"
    );

    // Verify twice: two verify rows, latest wins, Effect
    // untouched, approval not consumed a second time.
    let (status, row1) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row1}");
    assert_eq!(row1["verdict"], "verified");
    let (status, row2) = h.verify(&effect_id).await;
    assert_eq!(status, 200, "{row2}");
    assert_eq!(row2["verdict"], "verified");
    assert_ne!(row1["evidence_id"], row2["evidence_id"]);
    let verify_rows: Vec<_> = h
        .evidence_for(&effect_id)
        .await
        .into_iter()
        .filter(|e| e["source"] == "verify")
        .collect();
    assert_eq!(verify_rows.len(), 2);
    assert_eq!(verify_rows.last().unwrap()["evidence_id"], row2["evidence_id"]);
    assert_eq!(h.effect(&effect_id).await["status"], "succeeded");
    assert_eq!(h.approval(&approval).await["consumed"], true);
}

// ---------------------------------------------------------------- R16

#[tokio::test]
async fn r16_restart_persists_evidence_and_verify_needs_no_approval() {
    let dir = temp_dir("restart-evidence");
    let files_root = dir.join("files");
    std::fs::create_dir_all(&files_root).unwrap();
    let fake = Arc::new(FakeReminderBackend::new());
    let h1 = Harness::serve_sqlite(dir.clone(), files_root.clone(), Arc::clone(&fake), None).await;
    // Rename, so the verify must read the row's scope (the
    // request carries no path at all — a caller-supplied path
    // field is ignored, not trusted).
    h1.write("before.txt", HELLO_B64, false).await;
    let ren_id = h1.rename("before.txt", "after.txt").await;
    let (status, row) = h1.verify(&ren_id).await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
    h1.stop();

    // Restart over the same database: Evidence survives, and
    // verify still needs no approval (the request takes only an
    // effect id; an extra path field is ignored).
    let h2 = Harness::serve_sqlite(dir, files_root, fake, None).await;
    let rows = h2.evidence_for(&ren_id).await;
    assert!(!rows.is_empty(), "evidence must survive restart");
    assert!(rows.iter().any(|e| e["source"] == "write"));
    let (status, row) = h2
        .post(
            "/effects/verify",
            json!({"effect_id": ren_id, "path": "attacker/chosen.txt"}),
        )
        .await;
    assert_eq!(status, 200, "{row}");
    assert_eq!(row["verdict"], "verified");
}

// ---------------------------------------------------------------- R17

#[tokio::test]
async fn r17_cross_view_same_fact() {
    // One device execution, seen from three views: the Effect
    // list, the Inbox list, and the Evidence list must agree on
    // the same effect id, status, and execution id.
    let h = Harness::new().await;
    h.fake.set_fault(Some(FakeFault::Timeout));
    let (_, created) = h
        .post(
            "/devices/actions/create",
            json!({"reminder_id": "reminder-1", "target_completed": true}),
        )
        .await;
    let action_id = created["action_id"].as_str().unwrap().to_string();
    let effect_id = created["effect_id"].as_str().unwrap().to_string();
    let approval = h.request_approval("reminders.set_completed", json!({})).await;
    h.decide(&approval, "approved").await;
    let (status, body) = h
        .post(
            "/devices/reminders/set",
            json!({"action_id": action_id, "tool_call_id": approval}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "unknown");
    let execution_id = h.effect(&effect_id).await["execution_id"]
        .as_str()
        .unwrap()
        .to_string();

    // The unknown row opened an Inbox row for the same execution.
    let inbox_rows = h
        .inbox("?kind=unknown_requires_reconciliation&open_only=true")
        .await;
    let inbox_row = inbox_rows
        .iter()
        .find(|r| r["effect_id"] == effect_id)
        .expect("unknown inbox row");
    assert_eq!(inbox_row["execution_id"], execution_id);

    // Reconcile: the write landed late.
    h.fake.set_fault(None);
    h.fake.add_reminder(Reminder {
        id: "reminder-1".to_string(),
        title: "Test reminder 1".to_string(),
        completed: true,
    });
    let (status, reconciled) = h.reconcile(&effect_id).await;
    assert_eq!(status, 200, "{reconciled}");
    assert_eq!(reconciled["status"], "reconciled");

    // Three views, one fact.
    let effects = h.effects().await;
    let eff_view = effects
        .iter()
        .find(|e| e["effect_id"] == effect_id)
        .expect("effect view");
    assert_eq!(eff_view["status"], "reconciled");
    assert_eq!(eff_view["execution_id"], execution_id);
    let inbox_all = h.inbox("?kind=unknown_requires_reconciliation").await;
    let inbox_view = inbox_all
        .iter()
        .find(|r| r["effect_id"] == effect_id)
        .expect("inbox view");
    assert!(inbox_view["resolved_ms"].is_number(), "reconcile auto-closes");
    assert_eq!(inbox_view["resolved_reason"], "auto");
    let evidence = h.evidence_for(&effect_id).await;
    let mirror = evidence
        .iter()
        .find(|e| e["source"] == "reconcile")
        .expect("reconcile mirror");
    assert_eq!(mirror["verdict"], "verified");
    assert_eq!(mirror["execution_id"], execution_id);
    // And the three views agree row by row: same effect id in
    // each list for this execution.
    let eff_ids: Vec<_> = effects
        .iter()
        .filter(|e| e["execution_id"] == execution_id)
        .map(|e| e["effect_id"].as_str().unwrap())
        .collect();
    assert_eq!(eff_ids, vec![effect_id.as_str()]);
    let inbox_ids: Vec<_> = inbox_all
        .iter()
        .filter(|r| r["execution_id"] == execution_id)
        .map(|r| r["effect_id"].as_str().unwrap())
        .collect();
    assert_eq!(inbox_ids, vec![effect_id.as_str()]);
    let all_evidence = h.evidence().await;
    let ev_ids: Vec<_> = all_evidence
        .iter()
        .filter(|e| e["execution_id"] == execution_id)
        .map(|e| e["effect_id"].as_str().unwrap())
        .collect();
    assert_eq!(ev_ids, vec![effect_id.as_str()]);
}
