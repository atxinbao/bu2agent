//! DeepSeek chat completions client (non-streaming first cut).
//!
//! The gateway calls DeepSeek's OpenAI-compatible chat API when
//! a key is configured; otherwise chat keeps the Mock echo
//! behavior. The endpoint is `POST {base_url}/chat/completions`
//! with `stream: false`; DeepSeek's base URL has no `/v1`
//! prefix (https://api.deepseek.com/chat/completions).
//!
//! Key resolution, in order: env `DEEPSEEK_API_KEY`, then the
//! OS keychain handle `deepseek` (service `bu2agent`, the
//! stage 2 store). No key means the client is disabled and the
//! caller falls back to echo; a configured key that fails
//! returns the provider status (or None for transport errors),
//! never the key, never a response body.
//!
//! Only a complete, non-empty answer enters the stream: a
//! truncated or empty completion is a failure, never a
//! complete answer.

use std::time::Duration;

use agent_desktop::credential_store::{CredentialStore, KeychainCredentialStore};
use serde::{Deserialize, Serialize};

/// Official DeepSeek API base URL (no `/v1` prefix).
pub const DEFAULT_BASE_URL: &str = "https://api.deepseek.com";
/// Current DeepSeek fast model (DeepSeek-V4.1-Flash).
pub const DEFAULT_MODEL: &str = "deepseek-flash";
/// Keychain handle for the DeepSeek key (service `bu2agent`).
pub const KEY_HANDLE: &str = "deepseek";
const TIMEOUT_SECS: u64 = 30;
const CONNECT_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Clone)]
pub struct DeepSeekConfig {
    pub base_url: String,
    pub model: String,
    pub api_key: Option<String>,
}

impl DeepSeekConfig {
    /// Env wins for the key; the keychain is the stored-key path.
    /// Base URL and model are env-overridable, defaulting to the
    /// official endpoint and `deepseek-flash`.
    pub fn from_env_and_keychain() -> Self {
        let api_key = std::env::var("DEEPSEEK_API_KEY")
            .ok()
            .filter(|k| !k.is_empty())
            .or_else(|| KeychainCredentialStore::new().retrieve(KEY_HANDLE).ok());
        Self {
            base_url: std::env::var("DEEPSEEK_BASE_URL")
                .ok()
                .filter(|u| !u.is_empty())
                .unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
            model: std::env::var("DEEPSEEK_MODEL")
                .ok()
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| DEFAULT_MODEL.to_string()),
            api_key,
        }
    }

    /// No key: the caller falls back to the Mock echo.
    pub fn disabled() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            model: DEFAULT_MODEL.to_string(),
            api_key: None,
        }
    }

    fn completions_url(&self) -> String {
        format!("{}/chat/completions", self.base_url.trim_end_matches('/'))
    }
}

/// What a chat turn gets: no key (echo), a model reply, or a
/// failure carrying only the provider HTTP status (None when
/// the request never completed). Secrets never appear here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeepseekOutcome {
    Disabled,
    Replied(String),
    Failed(Option<u16>),
}

#[derive(Debug, Clone, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Clone, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    stream: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatChoice {
    message: ChatReplyMessage,
    finish_reason: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ChatReplyMessage {
    content: Option<String>,
}

fn first_content(response: &ChatResponse) -> Option<String> {
    let choice = response.choices.first()?;
    if choice.finish_reason != "stop" {
        return None;
    }
    choice.message.content.clone().filter(|text| !text.trim().is_empty())
}

#[derive(Debug, Clone)]
pub struct DeepSeekClient {
    config: DeepSeekConfig,
    http: reqwest::Client,
}

impl DeepSeekClient {
    pub fn new(config: DeepSeekConfig) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(TIMEOUT_SECS))
            .build()
            .unwrap_or_default();
        Self { config, http }
    }

    pub fn config(&self) -> &DeepSeekConfig {
        &self.config
    }

    pub async fn reply(&self, user_text: &str) -> DeepseekOutcome {
        self.reply_with_history(&[], user_text).await
    }

    /// Session send: prior complete turns first, then the current
    /// user message. The caller owns trimming; this only sends
    /// what it is given, in order.
    pub async fn reply_with_history(
        &self,
        history: &[(String, String)],
        user_text: &str,
    ) -> DeepseekOutcome {
        let Some(key) = &self.config.api_key else {
            return DeepseekOutcome::Disabled;
        };
        let mut messages: Vec<ChatMessage> = history
            .iter()
            .map(|(role, content)| ChatMessage { role: role.clone(), content: content.clone() })
            .collect();
        messages.push(ChatMessage { role: "user".to_string(), content: user_text.to_string() });
        let request = ChatRequest { model: self.config.model.clone(), messages, stream: false };
        let sent =
            self.http.post(self.config.completions_url()).bearer_auth(key).json(&request).send().await;
        let response = match sent {
            Ok(r) => r,
            Err(_) => return DeepseekOutcome::Failed(None),
        };
        if !response.status().is_success() {
            return DeepseekOutcome::Failed(Some(response.status().as_u16()));
        }
        match response.json::<ChatResponse>().await {
            Ok(parsed) => match first_content(&parsed) {
                Some(text) => DeepseekOutcome::Replied(text),
                None => DeepseekOutcome::Failed(None),
            },
            Err(_) => DeepseekOutcome::Failed(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completions_url_trims_trailing_slash() {
        let config = DeepSeekConfig {
            base_url: "https://api.deepseek.com/".to_string(),
            model: DEFAULT_MODEL.to_string(),
            api_key: None,
        };
        assert_eq!(config.completions_url(), "https://api.deepseek.com/chat/completions");
    }

    #[test]
    fn first_content_reads_first_choice() {
        let parsed: ChatResponse = serde_json::from_str(
            r#"{"choices":[{"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}"#,
        )
        .expect("parse");
        assert_eq!(first_content(&parsed), Some("hi".to_string()));
    }

    #[test]
    fn first_content_missing_is_none() {
        let parsed: ChatResponse =
            serde_json::from_str(r#"{"choices":[]}"#).expect("parse");
        assert_eq!(first_content(&parsed), None);
        let parsed: ChatResponse = serde_json::from_str(
            r#"{"choices":[{"message":{"role":"assistant","content":null},"finish_reason":"stop"}]}"#,
        )
        .expect("parse");
        assert_eq!(first_content(&parsed), None);
    }

    #[test]
    fn first_content_truncated_or_empty_is_none() {
        let parsed: ChatResponse = serde_json::from_str(
            r#"{"choices":[{"message":{"role":"assistant","content":"partial"},"finish_reason":"length"}]}"#,
        )
        .expect("parse");
        assert_eq!(first_content(&parsed), None);
        let parsed: ChatResponse = serde_json::from_str(
            r#"{"choices":[{"message":{"role":"assistant","content":"   "},"finish_reason":"stop"}]}"#,
        )
        .expect("parse");
        assert_eq!(first_content(&parsed), None);
    }

    #[test]
    fn disabled_config_has_no_key() {
        assert!(DeepSeekConfig::disabled().api_key.is_none());
    }
}
