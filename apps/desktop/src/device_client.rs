//! Device action client: Rust shell -> Mock Gateway device endpoints.
//!
//! Device records only: list reminders, create an action, execute a
//! set, record a rejection, refresh an unknown action, list/get
//! actions. The frontend drives these step by step; the gateway
//! stores the records (see server.rs). Localhost only, no TLS,
//! Mock only -- same transport rules as the other desktop clients.

use serde::{Deserialize, Serialize};

use crate::http_client::{transport_error, HttpClient};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceReminder {
    pub id: String,
    pub title: String,
    pub completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceActionRecord {
    pub action_id: String,
    pub device: String,
    pub action: String,
    pub reminder_id: String,
    pub target_completed: bool,
    pub status: String,
    pub detail: Option<String>,
}

#[derive(Debug, Serialize)]
struct ActionCreateRequest {
    reminder_id: String,
    target_completed: bool,
}

#[derive(Debug, Serialize)]
struct DeviceSetRequest {
    action_id: String,
    tool_call_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct ActionRejectRequest {
    action_id: String,
    tool_call_id: String,
}

#[derive(Debug, Serialize)]
struct ActionRefreshRequest {
    action_id: String,
}

#[derive(Debug, Deserialize)]
struct AuthorizationStatusDto {
    status: String,
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct DeviceClient {
    http: HttpClient,
}

impl DeviceClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn list_reminders(&self) -> Result<Vec<DeviceReminder>> {
        self.http.get_json("/devices/reminders/list")
    }

    /// Current Reminders authorization status, as the gateway
    /// reports it: "authorized" | "denied" | "not_determined".
    pub fn authorization(&self) -> Result<String> {
        let dto: AuthorizationStatusDto = self.http.get_json("/devices/authorization")?;
        Ok(dto.status)
    }

    pub fn create_action(
        &self,
        reminder_id: &str,
        target_completed: bool,
    ) -> Result<DeviceActionRecord> {
        let body = serde_json::to_string(&ActionCreateRequest {
            reminder_id: reminder_id.to_string(),
            target_completed,
        })
        .map_err(|_| transport_error("cannot encode device action create request"))?;
        self.http.post_json("/devices/actions/create", &body)
    }

    pub fn set_reminder(&self, action_id: &str, tool_call_id: &str) -> Result<DeviceActionRecord> {
        let body = serde_json::to_string(&DeviceSetRequest {
            action_id: action_id.to_string(),
            tool_call_id: Some(tool_call_id.to_string()),
        })
        .map_err(|_| transport_error("cannot encode device set request"))?;
        self.http.post_json("/devices/reminders/set", &body)
    }

    pub fn mark_rejected(
        &self,
        action_id: &str,
        tool_call_id: &str,
    ) -> Result<DeviceActionRecord> {
        let body = serde_json::to_string(&ActionRejectRequest {
            action_id: action_id.to_string(),
            tool_call_id: tool_call_id.to_string(),
        })
        .map_err(|_| transport_error("cannot encode device mark rejected request"))?;
        self.http.post_json("/devices/actions/mark_rejected", &body)
    }

    pub fn refresh_action(&self, action_id: &str) -> Result<DeviceActionRecord> {
        let body = serde_json::to_string(&ActionRefreshRequest {
            action_id: action_id.to_string(),
        })
        .map_err(|_| transport_error("cannot encode device refresh request"))?;
        self.http.post_json("/devices/actions/refresh", &body)
    }

    pub fn list_actions(&self) -> Result<Vec<DeviceActionRecord>> {
        self.http.get_json("/devices/actions/list")
    }

    pub fn get_action(&self, action_id: &str) -> Result<DeviceActionRecord> {
        self.http.get_json(&format!("/devices/actions/get?action_id={action_id}"))
    }
}
