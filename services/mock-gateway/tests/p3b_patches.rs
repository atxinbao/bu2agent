//! P3-B patch tests (plan §6.1 C4–C9): grouping (C4),
//! gateway-computed diff (C5), apply incl. AlreadyNew +
//! replay (C6), stale 409 + loop-entry stale (C7),
//! partial/reject truth table (C8), restart (C9). C10
//! (transport/UI) and C11 (full regression) are the web
//! build + full-suite runs, not tests here.
//!
//! Tests use temp dirs and the HTTP routes only; no real git,
//! no subprocesses. Paths are compared canonicalized against
//! canonicalized. SHA-256 values are the `sha256sum` of the
//! seed/proposed bytes (see the constants below).

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mock_gateway::loop_engine::*;
use mock_gateway::server::{app, AppState};
use serde_json::{json, Value};
use tokio::net::TcpListener;

static NEXT: AtomicUsize = AtomicUsize::new(1);

/// sha256("old a\n")
const OLD_A_SHA: &str = "c8518d7e574626e44644fb5dd1348aec70ed95b22336992a981336f5a0c69be3";
/// sha256("new a\n")
const NEW_A_SHA: &str = "77981ea7ac834d42c8597b6879cf9e934679a54e817b2cd2ef461160026d7714";
/// sha256("old b\n")
const OLD_B_SHA: &str = "4a817ab691a5a179b5becbaa44b5f7046ab58a8d6e2daba2737392c767591d80";
/// sha256("new b\n")
const NEW_B_SHA: &str = "ab1a29c10ccb9ceec5a9e4453f1aaf261b81869eaadbf3426e378a99347b08af";
/// sha256("new file\n")
const NEW_FILE_SHA: &str = "0f15384d18789b1ebf3043dc7b6bc27273c8576373fbeb6f3e15854b588141c0";
/// sha256("drifted\n")
const DRIFTED_SHA: &str = "893105a87e63183bf7c064d2248bffc21f64f311cf22c700cf89d211d50e355d";

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "bu2agent-p3b-patches-{}-{}-{}",
        tag,
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

struct ScriptModel {
    steps: Mutex<VecDeque<(Duration, Result<ModelReply, ModelError>)>>,
    contexts: Mutex<Vec<Vec<Message>>>,
    started: tokio::sync::Notify,
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
            contexts: Mutex::new(vec![]),
            started: tokio::sync::Notify::new(),
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
        let (delay, reply) = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra model call");
        self.started.notify_one();
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            reply
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

fn calls(items: Vec<Proposal>) -> ModelReply {
    ModelReply::Calls {
        proposals: items,
        content: None,
    }
}

fn done() -> ModelReply {
    ModelReply::Final("finished".into())
}

fn write_proposal(path: &str, text: &str) -> Proposal {
    proposal("fs.write", json!({"path": path, "text": text}))
}

/// A write proposal carrying a model-submitted `diff` arg:
/// normalize() drops it (only path+text survive), so the
/// stored diff must be the gateway computation, never this.
fn write_proposal_with_diff(path: &str, text: &str, diff: &str) -> Proposal {
    proposal(
        "fs.write",
        json!({"path": path, "text": text, "diff": diff}),
    )
}

struct Harness {
    dir: PathBuf,
    url: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<()>,
}

impl Harness {
    async fn serve(replies: Vec<ModelReply>) -> Self {
        Self::serve_in(temp_dir("harness"), replies).await
    }

    async fn serve_in(dir: PathBuf, replies: Vec<ModelReply>) -> Self {
        let root = dir.join("files");
        std::fs::create_dir_all(&root).unwrap();
        let model = Arc::new(ScriptModel::new(replies));
        let state = AppState::with_files_root_and_approvals(
            root.clone(),
            Some(dir.join("approvals.jsonl")),
        )
        .with_loop_backends(
            Some(dir.join("loops.jsonl")),
            Some(model.clone()),
            None,
            Timeouts::default(),
        )
        .unwrap()
        .with_auto_drive(false)
        .with_executions_path(dir.join("executions.jsonl"))
        .with_effects_path(dir.join("effects.jsonl"))
        .with_binding_state_path(dir.join("bindings.jsonl"))
        .with_project_state_path(dir.join("projects.jsonl"))
        .with_location_state_path(dir.join("locations.jsonl"))
        .with_worktree_state_path(dir.join("worktrees.jsonl"));
        let state = state.with_sqlite_path(dir.join("gateway.sqlite3"));
        state.sqlite_import_and_switch().expect("switch");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server =
            tokio::spawn(async move { axum::serve(listener, app(state)).await.unwrap() });
        Self {
            dir,
            url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            server,
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
        assert_eq!(status, 200, "{path}: {body}");
        body
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

    async fn get_ok(&self, path: &str) -> Value {
        let (status, body) = self.get(path).await;
        assert_eq!(status, 200, "{body}");
        body
    }

    async fn start(&self, worktree_id: Option<&str>) -> String {
        let mut req = json!({"prompt": "edit the code"});
        if let Some(w) = worktree_id {
            req["worktree_id"] = json!(w);
        }
        self.ok("/agent/loop/start", req).await["loop_id"]
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

    /// Step until a call awaits approval; returns its
    /// approval id + binding + queue index (the index to
    /// read the call's status from a later resume view).
    async fn await_approval(&self, loop_id: &str) -> (String, Value, usize) {
        let mut view = Value::Null;
        for _ in 0..8 {
            view = self.step(loop_id).await;
            if view["status"] == "awaiting_approval" {
                break;
            }
        }
        assert_eq!(view["status"], "awaiting_approval", "{view}");
        let queue = view["queue_position"].as_u64().unwrap() as usize;
        let call = &view["calls"][queue];
        (
            call["approval_id"].as_str().unwrap().to_string(),
            call["binding"].clone(),
            queue,
        )
    }

    /// Decide approved, then resume the loop with that
    /// approval + binding; returns the resume view.
    async fn approve_and_resume(
        &self,
        loop_id: &str,
        approval: &str,
        binding: &Value,
    ) -> Value {
        self.decide(approval, "approved").await;
        self.ok(
            "/agent/loop/resume",
            json!({"loop_id": loop_id, "approval_id": approval, "binding": binding}),
        )
        .await
    }

    async fn decide(&self, id: &str, status: &str) {
        self.ok(
            "/approval/decide",
            json!({"tool_call_id": id, "status": status}),
        )
        .await;
    }

    async fn patches(&self) -> Vec<Value> {
        self.get_ok("/patches/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn patch(&self, id: &str) -> Value {
        self.get_ok(&format!("/patches/{id}")).await
    }

    async fn effects(&self) -> Vec<Value> {
        self.get_ok("/effects/list")
            .await
            .as_array()
            .unwrap()
            .clone()
    }

    async fn effect(&self, id: &str) -> Value {
        self.get_ok(&format!("/effects/get?effect_id={id}")).await
    }

    async fn verify(&self, effect_id: &str) -> (u16, Value) {
        self.post("/effects/verify", json!({"effect_id": effect_id}))
            .await
    }

    async fn apply(
        &self,
        patch_id: &str,
        patch_file_id: &str,
        expected_old_sha256: Option<&str>,
        approval_id: &str,
    ) -> (u16, Value) {
        let mut req = json!({"approval_id": approval_id});
        if let Some(old) = expected_old_sha256 {
            req["expected_old_sha256"] = json!(old);
        }
        self.post(
            &format!("/patches/{patch_id}/files/{patch_file_id}/apply"),
            req,
        )
        .await
    }

    /// Two repos under the files root, each with its own
    /// binding / location / worktree. Returns the two
    /// worktree ids. repo/src/a.txt seeds "old a\n",
    /// repo2/src/b.txt seeds "old b\n".
    async fn setup_repos(&self) -> (String, String) {
        let repo = self.files_root().join("repo");
        let repo2 = self.files_root().join("repo2");
        std::fs::create_dir_all(repo.join("src")).unwrap();
        std::fs::create_dir_all(repo2.join("src")).unwrap();
        std::fs::write(repo.join("src/a.txt"), "old a\n").unwrap();
        std::fs::write(repo2.join("src/b.txt"), "old b\n").unwrap();
        let repo_str = repo.to_string_lossy().into_owned();
        let repo2_str = repo2.to_string_lossy().into_owned();
        let b1 = self
            .ok(
                "/bindings/create",
                json!({"display_name": "repo", "path": repo_str}),
            )
            .await;
        let w1 = b1["workspace_id"].as_str().unwrap().to_string();
        let l1 = self
            .ok(
                "/locations/create",
                json!({"workspace_id": w1, "name": "root", "path": repo_str}),
            )
            .await;
        let loc1 = l1["location_id"].as_str().unwrap().to_string();
        let wt1 = self
            .ok(
                "/worktrees/create",
                json!({"location_id": loc1, "directory_relative": "", "branch_label": "main"}),
            )
            .await;
        let worktree1 = wt1["worktree_id"].as_str().unwrap().to_string();
        let b2 = self
            .ok(
                "/bindings/create",
                json!({"display_name": "repo2", "path": repo2_str}),
            )
            .await;
        let w2 = b2["workspace_id"].as_str().unwrap().to_string();
        let l2 = self
            .ok(
                "/locations/create",
                json!({"workspace_id": w2, "name": "root", "path": repo2_str}),
            )
            .await;
        let loc2 = l2["location_id"].as_str().unwrap().to_string();
        let wt2 = self
            .ok(
                "/worktrees/create",
                json!({"location_id": loc2, "directory_relative": "", "branch_label": "main"}),
            )
            .await;
        let worktree2 = wt2["worktree_id"].as_str().unwrap().to_string();
        (worktree1, worktree2)
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ---------------------------------------------------------------- C4

#[tokio::test]
async fn c4_patch_grouping_merge_split_and_outside_excluded() {
    let h = Harness::serve(vec![
        calls(vec![
            write_proposal("repo/src/a.txt", "new a\n"),
            write_proposal("repo/src/a2.txt", "new file\n"),
        ]),
        done(),
        calls(vec![write_proposal("repo2/src/b.txt", "new b\n")]),
        done(),
        calls(vec![write_proposal("drafts/draft-1.md", "# draft\n")]),
        done(),
    ])
    .await;
    let (worktree1, worktree2) = h.setup_repos().await;

    // Loop 1, same execution + worktree: two proposals merge
    // into ONE Patch.
    let loop1 = h.start(Some(&worktree1)).await;
    let (approval1, binding1, queue1) = h.await_approval(&loop1).await;
    let patches = h.patches().await;
    assert_eq!(patches.len(), 1, "{patches:?}");
    assert_eq!(patches[0]["worktree_id"], worktree1);
    assert_eq!(patches[0]["status"], "proposed");
    let patch1 = h.patch("patch-1").await;
    assert_eq!(patch1["patch"]["patch_id"], "patch-1");
    let files = patch1["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "{files:?}");
    let file1 = &files[0];
    assert_eq!(file1["patch_file_id"], "patchfile-1");
    assert_eq!(file1["path_relative"], "src/a.txt");
    assert_eq!(file1["old_sha256"], OLD_A_SHA);
    assert_eq!(file1["new_sha256"], NEW_A_SHA);
    assert_eq!(
        file1["diff"],
        "--- a/src/a.txt\n+++ b/src/a.txt\n@@ -1 +1 @@\n-old a\n+new a\n"
    );
    // new_text never leaves the gateway (C1 DTO shape).
    assert!(file1.get("new_text").is_none(), "{file1}");
    assert_eq!(file1["approval_id"], approval1);
    assert_eq!(file1["status"], "pending");
    // Attribution is stamped into the Effect's detail field.
    let effect_id = file1["effect_id"].as_str().unwrap().to_string();
    let effect = h.effect(&effect_id).await;
    assert_eq!(effect["status"], "planned");
    assert!(
        effect["detail"]
            .as_str()
            .unwrap()
            .contains("patch:patch-1 patchfile:patchfile-1"),
        "{}",
        effect["detail"]
    );
    // The Patch keys on the proposing execution.
    assert_eq!(patches[0]["execution_id"], effect["execution_id"]);

    // Approve + resume the first write: the PatchFile syncs
    // to applied via the loop-write path. The Patch has ONE
    // file so far (a2.txt not yet proposed) -> "applied".
    let resumed = h.approve_and_resume(&loop1, &approval1, &binding1).await;
    assert_eq!(resumed["calls"][queue1]["status"], "succeeded");
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/a.txt")).unwrap(),
        "new a\n"
    );
    let patch1 = h.patch("patch-1").await;
    assert_eq!(patch1["files"][0]["status"], "applied");
    assert_eq!(patch1["patch"]["status"], "applied");

    // Second proposal, same execution + worktree: appends to
    // the SAME Patch (one Patch per (execution, worktree)),
    // flipping it back to partially_applied.
    let (approval2, binding2, queue2) = h.await_approval(&loop1).await;
    let patch1 = h.patch("patch-1").await;
    assert_eq!(patch1["patch"]["status"], "partially_applied");
    let files = patch1["files"].as_array().unwrap();
    assert_eq!(files.len(), 2, "{files:?}");
    let file2 = &files[1];
    assert_eq!(file2["patch_file_id"], "patchfile-2");
    assert_eq!(file2["path_relative"], "src/a2.txt");
    assert!(file2["old_sha256"].is_null(), "{file2}");
    assert_eq!(file2["new_sha256"], NEW_FILE_SHA);
    assert_eq!(
        file2["diff"],
        "--- /dev/null\n+++ b/src/a2.txt\n@@ -0,0 +1 @@\n+new file\n"
    );
    assert_eq!(file2["approval_id"], approval2);
    let resumed = h.approve_and_resume(&loop1, &approval2, &binding2).await;
    assert_eq!(resumed["calls"][queue2]["status"], "succeeded");
    assert_eq!(h.patch("patch-1").await["patch"]["status"], "applied");
    assert_eq!(h.drive(&loop1).await["status"], "completed");

    // Loop 2, different worktree: a DIFFERENT Patch.
    let loop2 = h.start(Some(&worktree2)).await;
    let (approval3, binding3, _queue3) = h.await_approval(&loop2).await;
    let patches = h.patches().await;
    assert_eq!(patches.len(), 2, "{patches:?}");
    let patch2 = h.patch("patch-2").await;
    assert_eq!(patch2["patch"]["worktree_id"], worktree2);
    assert_eq!(patch2["files"][0]["path_relative"], "src/b.txt");
    assert_eq!(patch2["files"][0]["old_sha256"], OLD_B_SHA);
    assert_eq!(patch2["files"][0]["new_sha256"], NEW_B_SHA);
    h.approve_and_resume(&loop2, &approval3, &binding3).await;
    assert_eq!(h.drive(&loop2).await["status"], "completed");

    // Loop 3, a write outside every worktree (P2 drafts):
    // no Patch, no PatchFile — the draft Effect is untouched.
    let loop3 = h.start(Some(&worktree1)).await;
    let (approval4, binding4, _queue4) = h.await_approval(&loop3).await;
    assert_eq!(h.patches().await.len(), 2);
    h.approve_and_resume(&loop3, &approval4, &binding4).await;
    assert_eq!(h.drive(&loop3).await["status"], "completed");
    let draft_effects: Vec<_> = h
        .effects()
        .await
        .into_iter()
        .filter(|e| e["scope"] == "drafts/draft-1.md")
        .collect();
    assert_eq!(draft_effects.len(), 1);
    assert_eq!(draft_effects[0]["status"], "succeeded");
    assert!(draft_effects[0]["detail"].is_null(), "{:?}", draft_effects[0]);
}

// ---------------------------------------------------------------- C5

#[tokio::test]
async fn c5_diff_hashes_context_and_1mib_cap() {
    let h = Harness::serve(vec![
        calls(vec![write_proposal("repo/src/c.txt", "one\nTWO\nthree\n")]),
        done(),
        calls(vec![write_proposal(
            "repo/src/big.txt",
            &"x".repeat(1024 * 1024 + 1),
        )]),
        done(),
        // A model-submitted `diff` arg: normalize() only allows
        // path+text for fs.write, so the whole proposal is
        // rejected (InvalidArguments) before any planning — the
        // model cannot inject a diff at all, and no PatchFile
        // or Effect is created for it.
        calls(vec![write_proposal_with_diff(
            "repo/src/d.txt",
            "x\n",
            "FAKE-DIFF-FROM-MODEL: this must never be stored",
        )]),
        done(),
    ])
    .await;
    let (worktree1, _) = h.setup_repos().await;
    std::fs::write(
        h.files_root().join("repo/src/c.txt"),
        "one\ntwo\nthree\n",
    )
    .unwrap();

    // Multi-line file, middle line changed: the diff carries
    // 3 lines of context on each side.
    let loop1 = h.start(Some(&worktree1)).await;
    let (approval1, binding1, _queue1) = h.await_approval(&loop1).await;
    let patch1 = h.patch("patch-1").await;
    let file1 = &patch1["files"][0];
    assert_eq!(file1["path_relative"], "src/c.txt");
    assert_eq!(
        file1["diff"],
        "--- a/src/c.txt\n+++ b/src/c.txt\n@@ -1,3 +1,3 @@\n one\n-two\n+TWO\n three\n"
    );
    // Old/new hashes are the sha256 of the exact bytes.
    assert_ne!(file1["old_sha256"], file1["new_sha256"]);
    assert_eq!(file1["old_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(file1["new_sha256"].as_str().unwrap().len(), 64);
    h.approve_and_resume(&loop1, &approval1, &binding1).await;
    assert_eq!(h.drive(&loop1).await["status"], "completed");

    // Over the 1 MiB/file cap: normalize() rejects the
    // proposal (InvalidArguments) before any planning — no
    // PatchFile, no Effect, the loop still completes.
    let loop2 = h.start(Some(&worktree1)).await;
    assert_eq!(h.drive(&loop2).await["status"], "completed");
    assert_eq!(h.patches().await.len(), 1, "cap write must not group");
    assert!(
        !h.files_root().join("repo/src/big.txt").exists(),
        "cap write must not land"
    );
    assert_eq!(
        h.effects()
            .await
            .iter()
            .filter(|e| e["scope"] == "repo/src/big.txt")
            .count(),
        0
    );

    // Model-submitted diff: the call fails invalid_arguments
    // at normalize time; nothing is grouped or recorded.
    let loop3 = h.start(Some(&worktree1)).await;
    assert_eq!(h.drive(&loop3).await["status"], "completed");
    assert_eq!(h.patches().await.len(), 1, "diff-arg write must not group");
    assert_eq!(
        h.effects()
            .await
            .iter()
            .filter(|e| e["scope"] == "repo/src/d.txt")
            .count(),
        0
    );
    assert!(
        !h.files_root().join("repo/src/d.txt").exists(),
        "diff-arg write must not land"
    );
}

// ---------------------------------------------------------------- C6

#[tokio::test]
async fn c6_apply_per_file_replay_and_already_new() {
    let h = Harness::serve(vec![
        calls(vec![write_proposal("repo/src/new.txt", "new file\n")]),
        done(),
        calls(vec![write_proposal("repo/src/a.txt", "new a\n")]),
        done(),
    ])
    .await;
    let (worktree1, _) = h.setup_repos().await;

    // Part A: apply through the command (the loop never
    // dispatches this write). Approve, apply, verify.
    let loop1 = h.start(Some(&worktree1)).await;
    let (approval1, binding1, queue1) = h.await_approval(&loop1).await;
    h.decide(&approval1, "approved").await;
    let (status, applied) = h
        .apply("patch-1", "patchfile-1", None, &approval1)
        .await;
    assert_eq!(status, 200, "{applied}");
    assert_eq!(applied["replayed"], false);
    assert_eq!(applied["patch_file"]["status"], "applied");
    assert_eq!(applied["effect"]["status"], "succeeded");
    assert_eq!(applied["effect"]["effect_id"], applied["patch_file"]["effect_id"]);
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/new.txt")).unwrap(),
        "new file\n"
    );
    assert_eq!(h.patch("patch-1").await["patch"]["status"], "applied");
    // Write-time Evidence + verify: verified / hash_match.
    let (status, verified) = h.verify(applied["effect"]["effect_id"].as_str().unwrap()).await;
    assert_eq!(status, 200, "{verified}");
    assert_eq!(verified["verdict"], "verified");
    assert_eq!(verified["reason"], "hash_match");
    // Repeat apply: 200 + replayed:true, no re-check/re-write.
    let (status, replayed) = h
        .apply("patch-1", "patchfile-1", None, &approval1)
        .await;
    assert_eq!(status, 200, "{replayed}");
    assert_eq!(replayed["replayed"], true);
    assert_eq!(replayed["patch_file"]["status"], "applied");
    // The loop replays from the succeeded Effect and completes
    // (the resume-after-apply path: the approval is consumed,
    // the bound Effect is succeeded, so the call delivers the
    // replay result instead of failing the resume).
    let resumed = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop1, "approval_id": approval1, "binding": binding1}),
        )
        .await;
    assert_eq!(resumed["calls"][queue1]["status"], "succeeded");
    assert_eq!(h.drive(&loop1).await["status"], "completed");
    assert_eq!(h.effects().await.len(), 1, "one write Effect only");

    // Part B: AlreadyNew — the file already holds the new
    // bytes (the crash-recovery case: written, records not
    // updated). Apply skips the write, records only.
    let loop2 = h.start(Some(&worktree1)).await;
    let (approval2, _binding2, _queue2) = h.await_approval(&loop2).await;
    std::fs::write(h.files_root().join("repo/src/a.txt"), "new a\n").unwrap();
    h.decide(&approval2, "approved").await;
    let (status, applied) = h
        .apply("patch-2", "patchfile-2", Some(OLD_A_SHA), &approval2)
        .await;
    assert_eq!(status, 200, "{applied}");
    assert_eq!(applied["replayed"], true, "already-new must skip the write");
    assert_eq!(applied["patch_file"]["status"], "applied");
    assert_eq!(applied["effect"]["status"], "succeeded");
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/a.txt")).unwrap(),
        "new a\n"
    );
    let (status, verified) = h.verify(applied["effect"]["effect_id"].as_str().unwrap()).await;
    assert_eq!(status, 200, "{verified}");
    assert_eq!(verified["verdict"], "verified");
    assert_eq!(verified["reason"], "hash_match");
}

// ---------------------------------------------------------------- C7

#[tokio::test]
async fn c7_stale_apply_409_and_loop_entry_stale() {
    let h = Harness::serve(vec![
        calls(vec![write_proposal("repo/src/a.txt", "new a\n")]),
        done(),
        calls(vec![write_proposal("repo/src/a.txt", "second a\n")]),
        done(),
    ])
    .await;
    let (worktree1, _) = h.setup_repos().await;

    // Part A: propose, approve, drift the file, apply -> 409.
    let loop1 = h.start(Some(&worktree1)).await;
    let (approval1, binding1, queue1) = h.await_approval(&loop1).await;
    h.decide(&approval1, "approved").await;
    std::fs::write(h.files_root().join("repo/src/a.txt"), "drifted\n").unwrap();
    let (status, body) = h
        .apply("patch-1", "patchfile-1", Some(OLD_A_SHA), &approval1)
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "patch_stale");
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/a.txt")).unwrap(),
        "drifted\n",
        "stale apply must not touch the file"
    );
    // The approval is NOT consumed by a stale apply.
    let approval = h.get_ok(&format!("/approvals/{approval1}")).await;
    assert_eq!(approval["consumed"], false, "{approval}");
    let patch1 = h.patch("patch-1").await;
    assert_eq!(patch1["files"][0]["status"], "stale");
    assert_eq!(patch1["patch"]["status"], "stale");
    // Resuming the loop now also refuses (the loop entry
    // calls the SAME check): the call fails, file untouched.
    let resumed = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop1, "approval_id": approval1, "binding": binding1}),
        )
        .await;
    assert_eq!(resumed["calls"][queue1]["status"], "failed");
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/a.txt")).unwrap(),
        "drifted\n"
    );
    // Drive loop1 to completion so its queued `done()` reply
    // is consumed (loop2 must get its own model replies).
    assert_eq!(h.drive(&loop1).await["status"], "completed");

    // Part B: a fresh proposal drifts between proposal and
    // dispatch — the loop entry itself refuses (patch_stale).
    let loop2 = h.start(Some(&worktree1)).await;
    let (approval2, binding2, queue2) = h.await_approval(&loop2).await;
    // Proposal-time old is "drifted\n"; drift again before dispatch.
    let patch2 = h.patch("patch-2").await;
    assert_eq!(patch2["files"][0]["old_sha256"], DRIFTED_SHA);
    std::fs::write(h.files_root().join("repo/src/a.txt"), "old a\n").unwrap();
    let resumed = h.approve_and_resume(&loop2, &approval2, &binding2).await;
    assert_eq!(resumed["calls"][queue2]["status"], "failed");
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/a.txt")).unwrap(),
        "old a\n",
        "loop-entry stale must not touch the file"
    );
    let patch2 = h.patch("patch-2").await;
    assert_eq!(patch2["files"][0]["status"], "stale");
    assert_eq!(patch2["patch"]["status"], "stale");
}

// ---------------------------------------------------------------- C8

#[tokio::test]
async fn c8_partial_reject_truth_table() {
    let h = Harness::serve(vec![
        calls(vec![
            write_proposal("repo/src/a.txt", "new a\n"),
            write_proposal("repo/src/a2.txt", "new file\n"),
        ]),
        done(),
        calls(vec![
            write_proposal("repo/src/a.txt", "second a\n"),
            write_proposal("repo/src/a2.txt", "second a\n"),
        ]),
        done(),
        calls(vec![write_proposal("repo/src/a.txt", "third a\n")]),
        done(),
    ])
    .await;
    let (worktree1, _) = h.setup_repos().await;

    // Patch-1: apply one file via the command. The Patch has
    // ONE file so far (file2 not yet proposed) -> "applied".
    let loop1 = h.start(Some(&worktree1)).await;
    let (approval1, binding1, _queue1) = h.await_approval(&loop1).await;
    h.decide(&approval1, "approved").await;
    let (status, _) = h
        .apply("patch-1", "patchfile-1", Some(OLD_A_SHA), &approval1)
        .await;
    assert_eq!(status, 200);
    assert_eq!(h.patch("patch-1").await["patch"]["status"], "applied");
    // Rejecting a Patch with an applied file -> 409.
    let (status, body) = h.post("/patches/patch-1/reject", json!({})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "patch_has_applied_files");
    // Resume the loop (the first call replays), then reject
    // the second file's approval -> linkage -> rejected.
    // The Patch stays partially_applied (one file applied).
    let resumed = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop1, "approval_id": approval1, "binding": binding1}),
        )
        .await;
    let _ = resumed;
    let (approval2, binding2, _queue2) = h.await_approval(&loop1).await;
    // File2 appended to the same Patch -> partially_applied.
    assert_eq!(
        h.patch("patch-1").await["patch"]["status"],
        "partially_applied"
    );
    h.decide(&approval2, "rejected").await;
    let patch1 = h.patch("patch-1").await;
    assert_eq!(patch1["files"][1]["status"], "rejected");
    assert_eq!(patch1["patch"]["status"], "partially_applied");
    // Deliver the rejection by resuming WITH the rejected
    // approval (a plain drive cannot advance a parked loop),
    // then drive to completion.
    let resumed = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop1, "approval_id": approval2, "binding": binding2}),
        )
        .await;
    assert_eq!(resumed["calls"][1]["status"], "rejected");
    assert_eq!(h.drive(&loop1).await["status"], "completed");

    // Patch-2: reject BOTH approvals -> all rejected -> Patch rejected.
    let loop2 = h.start(Some(&worktree1)).await;
    let (approval3, binding3, _queue3) = h.await_approval(&loop2).await;
    h.decide(&approval3, "rejected").await;
    let patch2 = h.patch("patch-2").await;
    assert_eq!(patch2["files"][0]["status"], "rejected");
    // One file so far, all rejected -> "rejected".
    assert_eq!(patch2["patch"]["status"], "rejected");
    // Deliver the rejection by resuming WITH it, so the loop
    // advances to the second call.
    let resumed = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop2, "approval_id": approval3, "binding": binding3}),
        )
        .await;
    assert_eq!(resumed["calls"][0]["status"], "rejected");
    let (approval4, binding4, _queue4) = h.await_approval(&loop2).await;
    // File2 appended to the rejected Patch -> back to
    // "proposed" (rejected + pending); the rejected file stays.
    assert_eq!(h.patch("patch-2").await["patch"]["status"], "proposed");
    h.decide(&approval4, "rejected").await;
    let patch2 = h.patch("patch-2").await;
    assert_eq!(patch2["files"][1]["status"], "rejected");
    assert_eq!(patch2["patch"]["status"], "rejected");
    let resumed = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop2, "approval_id": approval4, "binding": binding4}),
        )
        .await;
    assert_eq!(resumed["calls"][1]["status"], "rejected");
    assert_eq!(h.drive(&loop2).await["status"], "completed");

    // Patch-3: explicit reject with 0 files applied -> Patch
    // rejected, files rejected; the approval stays decidable
    // (not auto-revoked) but apply then refuses patch_rejected.
    let loop3 = h.start(Some(&worktree1)).await;
    let (approval5, binding5, _queue5) = h.await_approval(&loop3).await;
    let (status, rejected) = h.post("/patches/patch-3/reject", json!({})).await;
    assert_eq!(status, 200, "{rejected}");
    assert_eq!(rejected["patch"]["status"], "rejected");
    assert_eq!(rejected["files"][0]["status"], "rejected");
    // Reject again: 200 idempotent.
    let (status, _) = h.post("/patches/patch-3/reject", json!({})).await;
    assert_eq!(status, 200);
    // The approval is still decidable one by one.
    h.decide(&approval5, "approved").await;
    let (status, body) = h
        .apply("patch-3", "patchfile-5", Some(OLD_A_SHA), &approval5)
        .await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "patch_rejected");
    // Unknown patch / file -> 404; stale client hash -> 400.
    let (status, body) = h
        .apply("patch-99", "patchfile-5", Some(OLD_A_SHA), &approval5)
        .await;
    assert_eq!(status, 404, "{body}");
    let (status, body) = h
        .apply("patch-3", "patchfile-99", Some(OLD_A_SHA), &approval5)
        .await;
    assert_eq!(status, 404, "{body}");
    let (status, body) = h.get("/patches/patch-99").await;
    assert_eq!(status, 404, "{body}");
    let (status, body) = h.get("/patches/list?status=bogus").await;
    assert_eq!(status, 400, "{body}");
    // The loop entry refuses the rejected file too (not just
    // the apply command): resuming with the approved approval
    // fails the call, the file stays untouched, and the loop
    // then completes on its queued `done()`.
    let resumed = h
        .ok(
            "/agent/loop/resume",
            json!({"loop_id": loop3, "approval_id": approval5, "binding": binding5}),
        )
        .await;
    assert_eq!(resumed["calls"][0]["status"], "failed");
    assert_eq!(
        std::fs::read_to_string(h.files_root().join("repo/src/a.txt")).unwrap(),
        "new a\n",
        "rejected file untouched by the loop write (loop1's content stands; loop2 never wrote)"
    );
    assert_eq!(h.drive(&loop3).await["status"], "completed");
}

// ---------------------------------------------------------------- C9

#[tokio::test]
async fn c9_restart_persists_patches_effects_and_counters() {
    let dir = temp_dir("restart");
    let h = Harness::serve_in(
        dir.clone(),
        vec![
            calls(vec![write_proposal("repo/src/a.txt", "new a\n")]),
            done(),
            calls(vec![write_proposal("repo/src/a2.txt", "new file\n")]),
            done(),
        ],
    )
    .await;
    let (worktree1, worktree2) = h.setup_repos().await;

    // Patch-1 applied via the loop; patch-2 left proposed.
    let loop1 = h.start(Some(&worktree1)).await;
    let (approval1, binding1, _queue1) = h.await_approval(&loop1).await;
    h.approve_and_resume(&loop1, &approval1, &binding1).await;
    assert_eq!(h.drive(&loop1).await["status"], "completed");
    assert_eq!(h.patch("patch-1").await["patch"]["status"], "applied");
    let loop2 = h.start(Some(&worktree1)).await;
    let (_approval2, _binding2, _queue2) = h.await_approval(&loop2).await;
    assert_eq!(h.patch("patch-2").await["patch"]["status"], "proposed");

    // Abort the server WITHOUT dropping the harness: Drop
    // removes the temp dir, and the restart must read the
    // same state files back.
    let first = h;
    first.server.abort();
    std::mem::forget(first);

    // Restart: same state files; Patch/PatchFile/Effect/
    // Evidence readable, counters continue, the §3.6 frozen
    // snapshot binding still readable.
    let h = Harness::serve_in(
        dir.clone(),
        vec![calls(vec![write_proposal(
            "repo2/src/b.txt",
            "new b\n",
        )]),
        done()],
    )
    .await;
    let patches = h.patches().await;
    assert_eq!(patches.len(), 2, "{patches:?}");
    assert_eq!(h.patch("patch-1").await["patch"]["status"], "applied");
    assert_eq!(h.patch("patch-2").await["patch"]["status"], "proposed");
    assert_eq!(
        h.patch("patch-2").await["files"][0]["old_sha256"],
        Value::Null
    );
    let effects = h.effects().await;
    assert_eq!(effects.len(), 2, "applied + planned Effects survive: {effects:?}");
    assert!(
        effects
            .iter()
            .any(|e| e["scope"] == "repo/src/a.txt" && e["status"] == "succeeded"),
        "{effects:?}"
    );
    assert!(
        effects
            .iter()
            .any(|e| e["scope"] == "repo/src/a2.txt" && e["status"] == "planned"),
        "{effects:?}"
    );
    assert_eq!(h.get_ok("/worktrees/list").await.as_array().unwrap().len(), 2);
    let conn =
        mock_gateway::sqlite_store::open_db(&dir.join("gateway.sqlite3")).expect("open db");
    let executions =
        mock_gateway::sqlite_store::list_executions(&conn).expect("list executions");
    let execution = executions
        .iter()
        .find(|e| e.loop_id.as_deref() == Some(loop1.as_str()))
        .expect("loop execution");
    let snapshot_id = execution.workspace_snapshot_id.clone().expect("snapshot id");
    let snapshot = mock_gateway::sqlite_store::get_snapshot(&conn, &snapshot_id)
        .expect("get snapshot")
        .expect("snapshot row");
    assert_eq!(
        snapshot.binding_worktree_id.as_deref(),
        Some(worktree1.as_str())
    );
    drop(conn);

    // A new proposal after restart takes the next counter
    // (patch-3), never reusing patch-1/patch-2. The original
    // repo2 worktree (worktree-2) still resolves after the
    // restart — no re-registration needed.
    let loop3 = h.start(Some(&worktree2)).await;
    let (_approval3, _binding3, _queue3) = h.await_approval(&loop3).await;
    let patch3 = h.patch("patch-3").await;
    assert_eq!(patch3["patch"]["worktree_id"], worktree2);
    assert_eq!(patch3["files"][0]["old_sha256"], OLD_B_SHA);
    assert_eq!(patch3["files"][0]["new_sha256"], NEW_B_SHA);
}
