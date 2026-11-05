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
}
