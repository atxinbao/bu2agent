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
//! AO-6 (Q-AO6-02): the key lives only in the credential
//! broker (`DeepSeekCredentialBroker`, below), resolved once
//! at startup. `DeepSeekConfig` carries no key — the loop
//! layer, the planner slot and every `config()` reader see
//! only base URL + model. The two POST funnels ask the
//! broker for the key at call time; nothing else may read
//! it (`key()` is crate-visible so the loop model can use
//! it, but only the two funnels call it — see F1).
//!
//! Only a complete, non-empty answer enters the stream: a
//! truncated or empty completion is a failure, never a
//! complete answer.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_desktop::credential_store::{redact, CredentialStore, KeychainCredentialStore};
use serde::{Deserialize, Serialize};

use crate::policy_store::NetworkRule;

/// Official DeepSeek API base URL (no `/v1` prefix).
pub const DEFAULT_BASE_URL: &str = "https://api.deepseek.com";
/// Current DeepSeek fast model (DeepSeek-V4.1-Flash).
pub const DEFAULT_MODEL: &str = "deepseek-flash";
/// Keychain handle for the DeepSeek key (service `bu2agent`).
pub const KEY_HANDLE: &str = "deepseek";
const TIMEOUT_SECS: u64 = 30;
const CONNECT_TIMEOUT_SECS: u64 = 10;

/// AO-6 (Q-AO6-02): the running model config carries no
/// credential. The key lives only in the broker below;
/// this struct is safe to clone, log and hand to the loop
/// layer — it can never leak a key because it has none.
#[derive(Debug, Clone)]
pub struct DeepSeekConfig {
    pub base_url: String,
    pub model: String,
}

impl DeepSeekConfig {
    /// Base URL and model are env-overridable, defaulting to the
    /// official endpoint and `deepseek-flash`. The key is NOT
    /// resolved here — see `DeepSeekCredentialBroker`.
    pub fn from_env() -> Self {
        Self {
            base_url: std::env::var("DEEPSEEK_BASE_URL")
                .ok()
                .filter(|u| !u.is_empty())
                .unwrap_or_else(|| DEFAULT_BASE_URL.to_string()),
            model: std::env::var("DEEPSEEK_MODEL")
                .ok()
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| DEFAULT_MODEL.to_string()),
        }
    }

    /// No key: the caller falls back to the Mock echo.
    pub fn disabled() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            model: DEFAULT_MODEL.to_string(),
        }
    }

    fn completions_url(&self) -> String {
        format!("{}/chat/completions", self.base_url.trim_end_matches('/'))
    }
}

/// AO-6 (Q-AO6-02): the credential broker. The DeepSeek key,
/// resolved once at startup (Q-AO6-01: startup-once, the
/// AO3-J2 behaviour is preserved — a keychain locked at
/// startup means keyless for the process lifetime), lives
/// only in this struct's private field. The chat client and
/// the loop model each hold a clone and ask it for the key
/// at POST time; the loop engine, the planner slot and every
/// `config()` reader never see it.
///
/// Deliberately no `Debug` derive: a derived `{:?}` would
/// print the key. The manual impl below prints only whether
/// a key is configured.
#[derive(Clone)]
pub struct DeepSeekCredentialBroker {
    key: Option<String>,
}

impl fmt::Debug for DeepSeekCredentialBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepSeekCredentialBroker")
            .field("key_configured", &self.has_key())
            .finish()
    }
}

impl DeepSeekCredentialBroker {
    /// Env wins for the key; the keychain is the stored-key path.
    pub fn from_env_and_keychain() -> Self {
        let key = std::env::var("DEEPSEEK_API_KEY")
            .ok()
            .filter(|k| !k.is_empty())
            .or_else(|| KeychainCredentialStore::new().retrieve(KEY_HANDLE).ok());
        Self { key }
    }

    /// No key: the callers fall back (chat echoes, loops 503).
    pub fn disabled() -> Self {
        Self { key: None }
    }

    /// Test/fake injection only: production keys come from
    /// env/keychain via `from_env_and_keychain`.
    pub fn test_key(key: &str) -> Self {
        Self {
            key: Some(key.to_string()),
        }
    }

    pub fn has_key(&self) -> bool {
        self.key.as_deref().is_some_and(|k| !k.is_empty())
    }

    /// Only the two POST funnels may call this (chat client
    /// below, loop model in `deepseek_loop.rs`). The key is
    /// never handed to the loop engine, the planner, a log
    /// line or a response.
    pub(crate) fn key(&self) -> Option<&str> {
        self.key.as_deref().filter(|k| !k.is_empty())
    }

    /// The same ****last-4 redaction the Settings key status
    /// uses (see `model_view`): the full key never leaves
    /// this struct.
    pub(crate) fn redacted_key(&self) -> Option<String> {
        self.key.as_deref().map(redact)
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
    /// Task 13: a network rule denied the call before any
    /// POST was made (the pattern is user-visible; the key
    /// was never sent). Handled as 403 at the call sites,
    /// never as a silent echo or a provider 502.
    Denied(String),
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
    choice
        .message
        .content
        .clone()
        .filter(|text| !text.trim().is_empty())
}

#[derive(Debug, Clone)]
pub struct DeepSeekClient {
    config: DeepSeekConfig,
    /// AO-6 (Q-AO6-02): the broker holds the key; the config
    /// does not. `config()` below returns the keyless config.
    broker: DeepSeekCredentialBroker,
    http: reqwest::Client,
    /// Task 13: shared network-rules handle (the same Arc
    /// AppState holds, so rule edits apply live). None
    /// means no enforcement (all pre-Task-13 call sites).
    network_rules: Option<Arc<Mutex<Option<Vec<NetworkRule>>>>>,
}

impl DeepSeekClient {
    pub fn new(config: DeepSeekConfig, broker: DeepSeekCredentialBroker) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(TIMEOUT_SECS))
            .build()
            .unwrap_or_default();
        Self {
            config,
            broker,
            http,
            network_rules: None,
        }
    }

    pub fn with_network_rules(mut self, rules: Arc<Mutex<Option<Vec<NetworkRule>>>>) -> Self {
        self.network_rules = Some(rules);
        self
    }

    /// The deny pattern blocking this client's completions
    /// URL, if any. Checked before every POST so a denied
    /// host never receives the request or the key.
    fn denied_pattern(&self) -> Option<String> {
        let rules = self.network_rules.as_ref()?;
        let guard = rules.lock().ok()?;
        let rules = guard.clone().unwrap_or_default();
        Self::denied_pattern_for(&rules, &self.config.completions_url())
    }

    /// E2-B §3.4: the same deny check against caller-supplied
    /// rules — the frozen rules an Execution was created under,
    /// resolved by the caller from the snapshot (see
    /// `frozen_network_rules_for_execution`). The live handle
    /// above is the fallback when no frozen rules resolve.
    fn denied_pattern_for(rules: &[NetworkRule], url: &str) -> Option<String> {
        crate::policy_store::denied_rule(rules, url)
    }

    /// AO-6 (Q-AO6-02): the running config — base URL +
    /// model only. The key is not in this struct anymore,
    /// so no reader of `config()` can ever see it.
    pub fn config(&self) -> &DeepSeekConfig {
        &self.config
    }

    /// AO-6 (Q-AO6-02): the broker, for call sites that
    /// build a loop model from this client (planner slot).
    /// They pass the broker through; they never read the key.
    pub(crate) fn broker(&self) -> &DeepSeekCredentialBroker {
        &self.broker
    }

    /// Whether a key is configured (chat replies / loops
    /// run); the key itself is never exposed here.
    pub fn has_key(&self) -> bool {
        self.broker.has_key()
    }

    pub async fn reply(&self, user_text: &str) -> DeepseekOutcome {
        self.reply_with_history(&[], user_text).await
    }

    /// E2-B §3.4: like `reply`, but the deny check runs
    /// against the given frozen rules when present (an
    /// Execution's snapshot rules); None falls back to the
    /// live handle exactly as `reply` does.
    pub async fn reply_with_rules(
        &self,
        rules: Option<Vec<NetworkRule>>,
        user_text: &str,
    ) -> DeepseekOutcome {
        self.reply_with_history_with_rules(rules, &[], user_text).await
    }

    /// ORC-2 (Q-ORC-08): like `reply_with_rules`, but the
    /// request carries the given model instead of the
    /// shared config's. The shared config is never
    /// mutated per call — two concurrent runs calling
    /// different role models must not race each other.
    pub async fn reply_with_rules_and_model(
        &self,
        rules: Option<Vec<NetworkRule>>,
        model: &str,
        user_text: &str,
    ) -> DeepseekOutcome {
        self.reply_with_history_with_rules_and_model(rules, &[], model, user_text)
            .await
    }

    /// Session send: prior complete turns first, then the current
    /// user message. The caller owns trimming; this only sends
    /// what it is given, in order.
    pub async fn reply_with_history(
        &self,
        history: &[(String, String)],
        user_text: &str,
    ) -> DeepseekOutcome {
        self.reply_with_history_with_rules(None, history, user_text).await
    }

    /// E2-B §3.4: like `reply_with_history`, with frozen
    /// rules for the deny check (see `reply_with_rules`).
    pub async fn reply_with_history_with_rules(
        &self,
        rules: Option<Vec<NetworkRule>>,
        history: &[(String, String)],
        user_text: &str,
    ) -> DeepseekOutcome {
        let model = self.config.model.clone();
        self.reply_with_history_with_rules_and_model(rules, history, &model, user_text)
            .await
    }

    /// ORC-2: the single funnel every reply path runs
    /// through. `model` is the model string for this one
    /// request (the shared config's model for the plain
    /// paths, a role model for the Planner/Executor).
    pub async fn reply_with_history_with_rules_and_model(
        &self,
        rules: Option<Vec<NetworkRule>>,
        history: &[(String, String)],
        model: &str,
        user_text: &str,
    ) -> DeepseekOutcome {
        // AO-6 (Q-AO6-02): the key comes from the broker at
        // POST time. The deny check below still runs first,
        // so a denied host never receives the request or the
        // key (Task 13 discipline, unchanged).
        let Some(key) = self.broker.key() else {
            return DeepseekOutcome::Disabled;
        };
        let denied = match &rules {
            Some(frozen) => Self::denied_pattern_for(frozen, &self.config.completions_url()),
            None => self.denied_pattern(),
        };
        if let Some(pattern) = denied {
            return DeepseekOutcome::Denied(pattern);
        }
        let mut messages: Vec<ChatMessage> = history
            .iter()
            .map(|(role, content)| ChatMessage { role: role.clone(), content: content.clone() })
            .collect();
        messages.push(ChatMessage { role: "user".to_string(), content: user_text.to_string() });
        let request = ChatRequest { model: model.to_string(), messages, stream: false };
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
        };
        assert_eq!(
            config.completions_url(),
            "https://api.deepseek.com/chat/completions"
        );
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
    fn disabled_broker_has_no_key() {
        assert!(!DeepSeekCredentialBroker::disabled().has_key());
        assert!(DeepSeekCredentialBroker::disabled().key().is_none());
        assert!(DeepSeekCredentialBroker::test_key("test-key").has_key());
    }

    #[test]
    fn broker_debug_never_prints_the_key() {
        // AO-6 F1 (unit half): a derived Debug would print
        // the key; the manual impl prints only the flag.
        // Expected value computed here, not copied from the
        // code's output: the struct name + key_configured.
        let shown = format!("{:?}", DeepSeekCredentialBroker::test_key("sk-ao6-secret"));
        assert_eq!(shown, "DeepSeekCredentialBroker { key_configured: true }");
        assert!(!shown.contains("sk-ao6-secret"));
    }
}
