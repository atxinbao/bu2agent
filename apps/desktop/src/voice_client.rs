//! Voice client: Rust shell -> Mock Gateway voice record
//! surface (Task 21, record subset, D2).
//!
//! Mirrors the other domain clients: no approval loop
//! (local record/selection operations, like the other
//! metadata surfaces), and the base URL is allowlisted to
//! the localhost Mock. Selections are record-only: nothing
//! here plays audio; see voice_store.rs on the gateway.

use serde::{Deserialize, Serialize};

use crate::agent_client::AgentError;
use crate::egress::EgressPolicy;
use crate::http_client::HttpClient;
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogVoice {
    pub voice_id: String,
    pub name: String,
    pub locale: String,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogFilters {
    pub locales: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineProfile {
    pub profile_id: String,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineSelection {
    pub profile_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomVoice {
    pub saved_voice_id: String,
    pub name: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveVoice {
    pub voice_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OnboardingReset {
    pub voice_id: Option<String>,
    pub profile_id: Option<String>,
}

/// `base_url` must look like `http://127.0.0.1:3001` and pass
/// the egress allowlist (localhost Mock only).
#[derive(Debug, Clone)]
pub struct VoiceClient {
    http: HttpClient,
}

impl VoiceClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let policy = EgressPolicy::mock();
        policy.check_url(base_url)?;
        Ok(Self { http: HttpClient::new(base_url)? })
    }

    pub fn catalog(&self) -> std::result::Result<Vec<CatalogVoice>, AgentError> {
        Ok(self.http.get_json("/api/voice/catalog")?)
    }

    pub fn catalog_filters(&self) -> std::result::Result<CatalogFilters, AgentError> {
        Ok(self.http.get_json("/api/voice/catalog/filters")?)
    }

    pub fn available_voices(&self) -> std::result::Result<Vec<CatalogVoice>, AgentError> {
        Ok(self.http.get_json("/api/voice/available-voices")?)
    }

    pub fn pipeline_profiles(&self) -> std::result::Result<Vec<PipelineProfile>, AgentError> {
        Ok(self.http.get_json("/api/voice/pipeline-profiles")?)
    }

    pub fn select_pipeline_profile(
        &self,
        profile_id: &str,
    ) -> std::result::Result<PipelineSelection, AgentError> {
        let body = serde_json::json!({ "profile_id": profile_id }).to_string();
        Ok(self.http.post_json("/api/voice/pipeline-profile", &body)?)
    }

    pub fn custom_voices(&self) -> std::result::Result<Vec<CustomVoice>, AgentError> {
        Ok(self.http.get_json("/api/voice/custom-voices")?)
    }

    pub fn rename_custom_voice(
        &self,
        saved_voice_id: &str,
        name: &str,
    ) -> std::result::Result<CustomVoice, AgentError> {
        let body = serde_json::json!({ "name": name }).to_string();
        Ok(self.http.patch_json(
            &format!("/api/voice/custom-voices/{saved_voice_id}"),
            &body,
        )?)
    }

    pub fn delete_custom_voice(
        &self,
        saved_voice_id: &str,
    ) -> std::result::Result<CustomVoice, AgentError> {
        Ok(self.http.delete_json(&format!("/api/voice/custom-voices/{saved_voice_id}"))?)
    }

    pub fn active_voice(&self) -> std::result::Result<ActiveVoice, AgentError> {
        Ok(self.http.get_json("/api/voice/active-voice")?)
    }

    pub fn save_active_voice(&self, voice_id: &str) -> std::result::Result<ActiveVoice, AgentError> {
        let body = serde_json::json!({ "voice_id": voice_id }).to_string();
        Ok(self.http.post_json("/api/voice/active-voice", &body)?)
    }

    pub fn onboarding_reset(&self) -> std::result::Result<OnboardingReset, AgentError> {
        Ok(self.http.post_json("/api/voice/onboarding/reset", "{}")?)
    }
}
