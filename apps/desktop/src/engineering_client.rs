//! Engineering record client: Rust shell -> Mock Gateway
//! engineering endpoints (P3-A, record subset).
//!
//! WorkspaceBinding, Project, Location, and Worktree records
//! only: create, list, get, archive. The gateway stores the
//! records (see server.rs); archive is a status flip, never a
//! delete, and no call here writes a file or runs a command.
//! Localhost only, no TLS, Mock only -- same transport rules
//! as the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::effects_client::EffectRecord;
use crate::http_client::{transport_error, HttpClient};
use crate::Result;

/// One Patch aggregate (P3-B, plan §4.1). Field names are
/// the gateway's snake_case JSON names, verbatim (C1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Patch {
    pub patch_id: String,
    pub execution_id: String,
    #[serde(default)]
    pub workspace_snapshot_id: Option<String>,
    pub worktree_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    pub status: String,
    pub created_ms: i64,
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
}

/// One PatchFile row (P3-B, plan §4.1). `new_text` never
/// leaves the gateway, so there is no such field here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchFile {
    pub patch_file_id: String,
    pub patch_id: String,
    pub path_relative: String,
    #[serde(default)]
    pub old_sha256: Option<String>,
    pub new_sha256: String,
    pub diff: String,
    pub effect_id: String,
    #[serde(default)]
    pub approval_id: Option<String>,
    pub status: String,
    pub created_ms: i64,
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
}

/// GET /patches/{id}: the Patch with its files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchDetail {
    pub patch: Patch,
    pub files: Vec<PatchFile>,
}

/// POST .../apply: the applied file, its Effect, and whether
/// the write was a replay of an already-applied file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchApplyResult {
    pub patch_file: PatchFile,
    pub effect: EffectRecord,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub workspace_id: String,
    pub display_name: String,
    pub allowed_roots: Vec<String>,
    pub policy_digest: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Project {
    pub project_id: String,
    pub name: String,
    pub description: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub location_id: String,
    pub workspace_id: String,
    pub name: String,
    pub canonical_root: String,
    pub root_sha256: String,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Worktree {
    pub worktree_id: String,
    pub location_id: String,
    pub directory_relative: String,
    #[serde(default)]
    pub branch_label: String,
    #[serde(default)]
    pub project_id: Option<String>,
    pub status: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

/// One Worker run row (P3-C, plan §3.6). Field names are
/// the gateway's snake_case JSON names, verbatim. The run
/// is the unit of Worker execution: one allowlisted command
/// in one worktree, driven by the slice's Worker driver.
/// `tail` is the last 4096 bytes of combined output;
/// `effect_id` is set at completion (the driver records the
/// completion Effect then).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRun {
    pub run_id: String,
    #[serde(default)]
    pub execution_id: Option<String>,
    #[serde(default)]
    pub assignment_id: Option<String>,
    #[serde(default)]
    pub worker_session_id: Option<String>,
    pub command: String,
    pub argv_json: String,
    pub cwd: String,
    pub timeout_ms: i64,
    pub status: String,
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub started_ms: Option<i64>,
    #[serde(default)]
    pub finished_ms: Option<i64>,
    pub tail: String,
    pub truncated: bool,
    #[serde(default)]
    pub effect_id: Option<String>,
    #[serde(default)]
    pub approval_id: Option<String>,
    #[serde(default)]
    pub binding_location_id: Option<String>,
    #[serde(default)]
    pub binding_worktree_id: Option<String>,
    #[serde(default)]
    pub binding_root_digest: Option<String>,
    pub schema_version: i64,
    /// E2-D §5: latest in-flight progress JSON text
    /// {"elapsed_ms","output_bytes","tail_lines"}, null
    /// before the driver's first 5 s write.
    #[serde(default)]
    pub progress_json: Option<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct EngineeringClient {
    http: HttpClient,
}

impl EngineeringClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn create_binding(
        &self,
        display_name: &str,
        path: &str,
    ) -> Result<WorkspaceBinding> {
        let body = serde_json::to_string(&serde_json::json!({
            "display_name": display_name,
            "path": path,
        }))
        .map_err(|_| transport_error("cannot encode binding create request"))?;
        self.http.post_json("/bindings/create", &body)
    }

    pub fn list_bindings(&self) -> Result<Vec<WorkspaceBinding>> {
        self.http.get_json("/bindings/list")
    }

    pub fn get_binding(&self, workspace_id: &str) -> Result<WorkspaceBinding> {
        self.http
            .get_json(&format!("/bindings/{workspace_id}"))
    }

    pub fn archive_binding(&self, workspace_id: &str) -> Result<WorkspaceBinding> {
        self.http.post_json(
            &format!("/bindings/{workspace_id}/archive"),
            "{}",
        )
    }

    pub fn create_project(&self, name: &str, description: &str) -> Result<Project> {
        let body = serde_json::to_string(&serde_json::json!({
            "name": name,
            "description": description,
        }))
        .map_err(|_| transport_error("cannot encode project create request"))?;
        self.http.post_json("/projects/create", &body)
    }

    pub fn list_projects(&self) -> Result<Vec<Project>> {
        self.http.get_json("/projects/list")
    }

    pub fn get_project(&self, project_id: &str) -> Result<Project> {
        self.http.get_json(&format!("/projects/{project_id}"))
    }

    pub fn archive_project(&self, project_id: &str) -> Result<Project> {
        self.http
            .post_json(&format!("/projects/{project_id}/archive"), "{}")
    }

    pub fn create_location(
        &self,
        workspace_id: &str,
        name: &str,
        path: &str,
    ) -> Result<Location> {
        let body = serde_json::to_string(&serde_json::json!({
            "workspace_id": workspace_id,
            "name": name,
            "path": path,
        }))
        .map_err(|_| transport_error("cannot encode location create request"))?;
        self.http.post_json("/locations/create", &body)
    }

    pub fn list_locations(&self) -> Result<Vec<Location>> {
        self.http.get_json("/locations/list")
    }

    pub fn get_location(&self, location_id: &str) -> Result<Location> {
        self.http.get_json(&format!("/locations/{location_id}"))
    }

    pub fn archive_location(&self, location_id: &str) -> Result<Location> {
        self.http
            .post_json(&format!("/locations/{location_id}/archive"), "{}")
    }

    pub fn create_worktree(
        &self,
        location_id: &str,
        directory_relative: &str,
        branch_label: &str,
        project_id: Option<&str>,
    ) -> Result<Worktree> {
        let body = serde_json::to_string(&serde_json::json!({
            "location_id": location_id,
            "directory_relative": directory_relative,
            "branch_label": branch_label,
            "project_id": project_id,
        }))
        .map_err(|_| transport_error("cannot encode worktree create request"))?;
        self.http.post_json("/worktrees/create", &body)
    }

    pub fn list_worktrees(&self) -> Result<Vec<Worktree>> {
        self.http.get_json("/worktrees/list")
    }

    pub fn get_worktree(&self, worktree_id: &str) -> Result<Worktree> {
        self.http.get_json(&format!("/worktrees/{worktree_id}"))
    }

    pub fn archive_worktree(&self, worktree_id: &str) -> Result<Worktree> {
        self.http
            .post_json(&format!("/worktrees/{worktree_id}/archive"), "{}")
    }

    pub fn list_patches(&self, status: Option<&str>) -> Result<Vec<Patch>> {
        let path = match status {
            Some(status) if !status.is_empty() => {
                format!("/patches/list?status={status}")
            }
            _ => "/patches/list".to_string(),
        };
        self.http.get_json(&path)
    }

    pub fn get_patch(&self, patch_id: &str) -> Result<PatchDetail> {
        self.http.get_json(&format!("/patches/{patch_id}"))
    }

    pub fn apply_patch(
        &self,
        patch_id: &str,
        patch_file_id: &str,
        expected_old_sha256: Option<&str>,
        approval_id: &str,
    ) -> Result<PatchApplyResult> {
        let body = serde_json::to_string(&serde_json::json!({
            "expected_old_sha256": expected_old_sha256,
            "approval_id": approval_id,
        }))
        .map_err(|_| transport_error("cannot encode patch apply request"))?;
        self.http.post_json(
            &format!("/patches/{patch_id}/files/{patch_file_id}/apply"),
            &body,
        )
    }

    pub fn reject_patch(&self, patch_id: &str) -> Result<PatchDetail> {
        self.http
            .post_json(&format!("/patches/{patch_id}/reject"), "{}")
    }

    /// P3-C: enqueue a Worker run. `params` is the template's
    /// raw JSON object (path/branch/message/package); the
    /// gateway resolves the worktree + binary + allowlist and
    /// computes the digest server-side. `approval_id` is the
    /// authorizing approval, when one was presented (a UI
    /// direct click carries none — the click is the
    /// authorization; a loop `worker.run` carries its
    /// approval, already consumed by the loop).
    pub fn create_worker_run(
        &self,
        worktree_id: &str,
        command: &str,
        params: Option<serde_json::Value>,
        timeout_ms: Option<u64>,
        approval_id: Option<&str>,
    ) -> Result<WorkerRun> {
        let body = serde_json::to_string(&serde_json::json!({
            "worktree_id": worktree_id,
            "command": command,
            "params": params,
            "timeout_ms": timeout_ms,
            "approval_id": approval_id,
        }))
        .map_err(|_| transport_error("cannot encode worker run create request"))?;
        self.http.post_json("/worker/runs", &body)
    }

    pub fn list_worker_runs(
        &self,
        execution_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<WorkerRun>> {
        let mut path = "/worker/runs/list".to_string();
        let mut first = true;
        for (key, value) in [
            ("execution_id", execution_id),
            ("status", status),
        ] {
            if let Some(value) = value.filter(|v| !v.is_empty()) {
                path.push_str(if first { "?" } else { "&" });
                path.push_str(&format!("{key}={value}"));
                first = false;
            }
        }
        self.http.get_json(&path)
    }

    pub fn get_worker_run(&self, run_id: &str) -> Result<WorkerRun> {
        self.http.get_json(&format!("/worker/runs/{run_id}"))
    }

    pub fn cancel_worker_run(&self, run_id: &str) -> Result<WorkerRun> {
        self.http
            .post_json(&format!("/worker/runs/{run_id}/cancel"), "{}")
    }
}
