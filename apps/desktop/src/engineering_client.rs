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

use crate::http_client::{transport_error, HttpClient};
use crate::Result;

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
}
