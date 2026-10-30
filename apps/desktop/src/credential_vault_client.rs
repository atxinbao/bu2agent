//! Vault client: Rust shell -> Mock Gateway credentials, catalog,
//! custom connectors, and capture tickets (Task 10).
//!
//! These are local metadata calls, so they carry no approval loop
//! (same decision as the Task 9 registry calls). The secret itself
//! is the payload by design, so no egress payload check runs on
//! it: the base URL is still allowlisted to the localhost Mock,
//! and secrets are never logged anywhere on this path.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::{transport_error, HttpClient};
use crate::Result;

/// One stored credential slot, as the gateway reports it. The
/// secret itself is never in this struct: only the backend's
/// redacted last-4 display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialSlotView {
    pub handle: String,
    pub provider: Option<String>,
    pub redacted: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
}

/// One catalog entry: provider/definition metadata, no secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEntryView {
    pub id: String,
    pub display_name: String,
    pub description: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
}

/// One user-registered custom-connector definition. Metadata only;
/// custom connectors are not merged into the /connectors registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomConnectorView {
    pub id: String,
    pub display_name: String,
    pub description: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
}

/// One external-provider capture ticket. Local flow only: no real
/// provider is ever contacted, and the ticket never carries a secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureTicketView {
    pub ticket_id: String,
    pub provider: String,
    pub handle: String,
    pub status: String,
    pub created_ms: u64,
    pub expires_ms: u64,
}

#[derive(Debug, Serialize)]
struct CredentialRegisterDto {
    handle: String,
    secret: String,
    provider: Option<String>,
}

#[derive(Debug, Serialize)]
struct CredentialDeleteDto {
    handle: String,
}

#[derive(Debug, Serialize)]
struct CredentialCaptureDto {
    handle: String,
    secret: String,
}

#[derive(Debug, Serialize)]
struct CaptureRequestDto {
    provider: String,
    handle: String,
}

#[derive(Debug, Serialize)]
struct CaptureFulfillDto {
    ticket_id: String,
    secret: String,
}

#[derive(Debug, Serialize)]
struct CatalogUpdateDto {
    id: String,
    display_name: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Serialize)]
struct CatalogDeleteDto {
    id: String,
}

#[derive(Debug, Serialize)]
struct CustomRegisterDto {
    id: String,
    display_name: String,
    description: Option<String>,
}

#[derive(Debug, Serialize)]
struct CustomDeleteDto {
    id: String,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct CredentialVaultClient {
    http: HttpClient,
}

impl CredentialVaultClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn register_credential(
        &self,
        handle: &str,
        secret: &str,
        provider: Option<&str>,
    ) -> std::result::Result<CredentialSlotView, AgentError> {
        let body = serde_json::to_string(&CredentialRegisterDto {
            handle: handle.to_string(),
            secret: secret.to_string(),
            provider: provider.map(|s| s.to_string()),
        })
        .map_err(|_| transport_error("cannot encode credential register request"))?;
        let slot: CredentialSlotView = self.http.post_json("/v1/credentials/register", &body)?;
        Ok(slot)
    }

    pub fn list_credentials(&self) -> std::result::Result<Vec<CredentialSlotView>, AgentError> {
        let slots: Vec<CredentialSlotView> = self.http.get_json("/v1/credentials/list")?;
        Ok(slots)
    }

    pub fn delete_credential(&self, handle: &str) -> std::result::Result<CredentialSlotView, AgentError> {
        let body = serde_json::to_string(&CredentialDeleteDto { handle: handle.to_string() })
            .map_err(|_| transport_error("cannot encode credential delete request"))?;
        let slot: CredentialSlotView = self.http.post_json("/v1/credentials/delete", &body)?;
        Ok(slot)
    }

    pub fn capture(&self, handle: &str, secret: &str) -> std::result::Result<CredentialSlotView, AgentError> {
        let body = serde_json::to_string(&CredentialCaptureDto {
            handle: handle.to_string(),
            secret: secret.to_string(),
        })
        .map_err(|_| transport_error("cannot encode credential capture request"))?;
        let slot: CredentialSlotView = self.http.post_json("/v1/credentials/capture", &body)?;
        Ok(slot)
    }

    pub fn capture_request(
        &self,
        provider: &str,
        handle: &str,
    ) -> std::result::Result<CaptureTicketView, AgentError> {
        let body = serde_json::to_string(&CaptureRequestDto {
            provider: provider.to_string(),
            handle: handle.to_string(),
        })
        .map_err(|_| transport_error("cannot encode capture request"))?;
        let ticket: CaptureTicketView =
            self.http.post_json("/v1/credentials/capture/external-provider/request", &body)?;
        Ok(ticket)
    }

    pub fn capture_fulfill(
        &self,
        ticket_id: &str,
        secret: &str,
    ) -> std::result::Result<CredentialSlotView, AgentError> {
        let body = serde_json::to_string(&CaptureFulfillDto {
            ticket_id: ticket_id.to_string(),
            secret: secret.to_string(),
        })
        .map_err(|_| transport_error("cannot encode capture fulfill request"))?;
        let slot: CredentialSlotView =
            self.http.post_json("/v1/credentials/capture/external-provider/fulfill", &body)?;
        Ok(slot)
    }

    pub fn catalog_list(&self) -> std::result::Result<Vec<CatalogEntryView>, AgentError> {
        let entries: Vec<CatalogEntryView> = self.http.get_json("/v1/credentials/catalog")?;
        Ok(entries)
    }

    pub fn catalog_details(&self, id: &str) -> std::result::Result<CatalogEntryView, AgentError> {
        let entry: CatalogEntryView =
            self.http.get_json(&format!("/v1/credentials/catalog/details?id={id}"))?;
        Ok(entry)
    }

    pub fn catalog_update(
        &self,
        id: &str,
        display_name: Option<&str>,
        description: Option<&str>,
    ) -> std::result::Result<CatalogEntryView, AgentError> {
        let body = serde_json::to_string(&CatalogUpdateDto {
            id: id.to_string(),
            display_name: display_name.map(|s| s.to_string()),
            description: description.map(|s| s.to_string()),
        })
        .map_err(|_| transport_error("cannot encode catalog update request"))?;
        let entry: CatalogEntryView = self.http.post_json("/v1/credentials/catalog/update", &body)?;
        Ok(entry)
    }

    pub fn catalog_delete(&self, id: &str) -> std::result::Result<CatalogEntryView, AgentError> {
        let body = serde_json::to_string(&CatalogDeleteDto { id: id.to_string() })
            .map_err(|_| transport_error("cannot encode catalog delete request"))?;
        let entry: CatalogEntryView = self.http.post_json("/v1/credentials/catalog/delete", &body)?;
        Ok(entry)
    }

    pub fn custom_register(
        &self,
        id: &str,
        display_name: &str,
        description: Option<&str>,
    ) -> std::result::Result<CustomConnectorView, AgentError> {
        let body = serde_json::to_string(&CustomRegisterDto {
            id: id.to_string(),
            display_name: display_name.to_string(),
            description: description.map(|s| s.to_string()),
        })
        .map_err(|_| transport_error("cannot encode custom connector register request"))?;
        let def: CustomConnectorView = self.http.post_json("/v1/custom-connectors/register", &body)?;
        Ok(def)
    }

    pub fn custom_list(&self) -> std::result::Result<Vec<CustomConnectorView>, AgentError> {
        let defs: Vec<CustomConnectorView> = self.http.post_json("/v1/custom-connectors/list", "{}")?;
        Ok(defs)
    }

    pub fn custom_delete(&self, id: &str) -> std::result::Result<CustomConnectorView, AgentError> {
        let body = serde_json::to_string(&CustomDeleteDto { id: id.to_string() })
            .map_err(|_| transport_error("cannot encode custom connector delete request"))?;
        let def: CustomConnectorView = self.http.post_json("/v1/custom-connectors/delete", &body)?;
        Ok(def)
    }
}
