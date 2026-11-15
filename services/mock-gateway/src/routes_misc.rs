//! - GET  /health                -> {"status":"ok"}
//!
//! Misc routes with no larger domain: /health, /model, /config,
//! /spaces (v2). The rest of the route directory lives in
//! `server.rs`; the line above moved here with its handler (FU-5 S-0).

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};

use agent_desktop::credential_store::redact;

use crate::deepseek::DeepSeekConfig;
use crate::model_store::{append_model_state, ModelStateLine, StagedModel};
use crate::routes_common::{bad_request, now_ms};
use crate::server::AppState;
use crate::space_store::{append_space_state, Space, SpaceItem, SpaceStateLine};

pub(crate) async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

// --- Model surface (Task 12) ---
//
// GET reports the running model config plus any staged
// selection; POST stages a selection that takes effect only
// after a restart. The running DeepSeekClient is never
// rebuilt mid-flight. The key is never accepted here and
// never appears in a response: only configured + the same
// ****last-4 redaction the Settings key status uses.

fn persist_model_line(state: &AppState, line: &ModelStateLine) {
    let Some(path) = &state.model_state_path else { return };
    append_model_state(path, line);
}

#[derive(Debug, Serialize)]
pub(crate) struct ModelView {
    provider: &'static str,
    model: String,
    base_url: String,
    key_configured: bool,
    key_redacted: Option<String>,
    staged_model: Option<String>,
    staged_base_url: Option<String>,
    restart_needed: bool,
}

/// Is there a staged value a restart would still apply?
/// A staged field counts only when it differs from the
/// running config AND no env override explains the
/// difference (env wins over staged, so a staged value
/// shadowed by env is dormant, not pending). Pure for
/// the same reason as merge_staged_config.
fn model_restart_needed(
    running: &DeepSeekConfig,
    staged: &StagedModel,
    env_model: Option<&str>,
    env_base_url: Option<&str>,
) -> bool {
    let model_pending = staged.model.as_ref().is_some_and(|m| {
        m != &running.model && env_model.is_none_or(|e| e.is_empty())
    });
    let base_pending = staged.base_url.as_ref().is_some_and(|u| {
        u != &running.base_url && env_base_url.is_none_or(|e| e.is_empty())
    });
    model_pending || base_pending
}

fn model_view(state: &AppState) -> ModelView {
    let running = state.deepseek.config();
    let staged = state.model_staged.lock().expect("model staged lock").clone().unwrap_or_default();
    let model = staged.model.clone().unwrap_or_else(|| running.model.clone());
    let base_url = staged.base_url.clone().unwrap_or_else(|| running.base_url.clone());
    let env_model = std::env::var("DEEPSEEK_MODEL").ok().filter(|s| !s.is_empty());
    let env_base = std::env::var("DEEPSEEK_BASE_URL").ok().filter(|s| !s.is_empty());
    let restart_needed =
        model_restart_needed(running, &staged, env_model.as_deref(), env_base.as_deref());
    let (key_configured, key_redacted) = match &running.api_key {
        Some(key) => (true, Some(redact(key))),
        None => (false, None),
    };
    ModelView {
        provider: "deepseek",
        model,
        base_url,
        key_configured,
        key_redacted,
        staged_model: staged.model,
        staged_base_url: staged.base_url,
        restart_needed,
    }
}

pub(crate) async fn model_get(State(state): State<AppState>) -> Json<ModelView> {
    Json(model_view(&state))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ModelSetDto {
    model: Option<String>,
    base_url: Option<String>,
}

pub(crate) async fn model_set(
    State(state): State<AppState>,
    Json(req): Json<ModelSetDto>,
) -> impl IntoResponse {
    let model = match req.model {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "model must not be empty" })))
                    .into_response();
            }
            Some(trimmed)
        }
        None => None,
    };
    let base_url = match req.base_url {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty()
                || (!trimmed.starts_with("http://") && !trimmed.starts_with("https://"))
            {
                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "invalid base_url" })))
                    .into_response();
            }
            Some(trimmed)
        }
        None => None,
    };
    // All-absent body is a no-op success: it changes nothing
    // and reports the current view, mirroring idempotent reads.
    if model.is_none() && base_url.is_none() {
        return Json(model_view(&state)).into_response();
    }
    let mut staged = state.model_staged.lock().expect("model staged lock").clone().unwrap_or_default();
    if let Some(m) = model {
        staged.model = Some(m);
    }
    if let Some(u) = base_url {
        staged.base_url = Some(u);
    }
    staged.updated_ms = now_ms();
    *state.model_staged.lock().expect("model staged lock") = Some(staged.clone());
    persist_model_line(&state, &ModelStateLine::Model(staged));
    Json(model_view(&state)).into_response()
}

#[cfg(test)]
mod model_merge_tests {
    use super::*;
    use crate::deepseek::DeepSeekConfig;
    use crate::server::merge_staged_config;

    fn running() -> DeepSeekConfig {
        DeepSeekConfig {
            base_url: "https://api.deepseek.com".to_string(),
            model: "deepseek-flash".to_string(),
            api_key: None,
        }
    }

    fn staged() -> StagedModel {
        StagedModel {
            model: Some("deepseek-chat".to_string()),
            base_url: Some("https://staged.example.com".to_string()),
            updated_ms: 1,
        }
    }

    #[test]
    fn merge_applies_staged_without_env() {
        let merged = merge_staged_config(running(), &Some(staged()), None, None);
        assert_eq!(merged.model, "deepseek-chat");
        assert_eq!(merged.base_url, "https://staged.example.com");
        assert_eq!(merged.api_key, None);
    }

    #[test]
    fn merge_env_wins_over_staged() {
        // The caller (from_env_and_keychain) already put the
        // env values into config; merge only decides whether
        // staged may override. With env present, staged is
        // ignored and the env-carrying config stands.
        let mut env_config = running();
        env_config.model = "deepseek-reasoner".to_string();
        env_config.base_url = "https://env.example.com".to_string();
        let merged = merge_staged_config(
            env_config,
            &Some(staged()),
            Some("deepseek-reasoner".to_string()),
            Some("https://env.example.com".to_string()),
        );
        assert_eq!(merged.model, "deepseek-reasoner");
        assert_eq!(merged.base_url, "https://env.example.com");
    }

    #[test]
    fn merge_partial_staged_only_touches_set_fields() {
        let partial = StagedModel {
            model: Some("deepseek-chat".to_string()),
            base_url: None,
            updated_ms: 1,
        };
        let merged = merge_staged_config(running(), &Some(partial), None, None);
        assert_eq!(merged.model, "deepseek-chat");
        assert_eq!(merged.base_url, "https://api.deepseek.com");
    }

    #[test]
    fn restart_needed_env_shadow_is_dormant_not_pending() {
        let run = running();
        let stage = staged();
        assert!(model_restart_needed(&run, &stage, None, None));
        // After a restart that applied staged, running == staged.
        let mut applied = run.clone();
        applied.model = "deepseek-chat".to_string();
        applied.base_url = "https://staged.example.com".to_string();
        assert!(!model_restart_needed(&applied, &stage, None, None));
        // With env overrides explaining both differences, the
        // staged values are dormant, not pending.
        assert!(!model_restart_needed(
            &run,
            &stage,
            Some("deepseek-reasoner"),
            Some("https://env.example.com")
        ));
    }
}

// --- Config surface (Task 14, GET only) ---
//
// GET /config reports gateway facts, read-only. PATCH is
// deliberately NOT implemented: the gateway has no
// runtime-mutable self-owned config to write (files_root
// is a startup jail root, model belongs to /model, the
// key belongs to the Keychain), so a PATCH would invent
// a dangerous write semantic. Axum answers PATCH on this
// path with 405, which the smoke test asserts.

#[derive(Debug, Serialize)]
pub(crate) struct ConfigView {
    files_root: String,
    gateway_addr: String,
    provider: &'static str,
}

pub(crate) async fn config_get(State(state): State<AppState>) -> Json<ConfigView> {
    // Same fallback as the binary's bind address in main.rs.
    let gateway_addr =
        std::env::var("MOCK_GATEWAY_ADDR").unwrap_or_else(|_| "127.0.0.1:3001".to_string());
    Json(ConfigView {
        files_root: state.files_root.display().to_string(),
        gateway_addr,
        provider: "deepseek",
    })
}

// --- Spaces surface (Task 16, v2 record subset) ---
//
// Local semantics only (see space_store.rs): save
// upserts a named collection of session/file references;
// delete is a tombstone; the ?source=saved_space variant
// has the same record effect as a plain delete.

fn persist_space_line(state: &AppState, line: &SpaceStateLine) {
    let Some(path) = &state.space_state_path else { return };
    append_space_state(path, line);
}

pub(crate) async fn spaces_list(State(state): State<AppState>) -> Json<Vec<Space>> {
    let mut spaces: Vec<Space> = state
        .spaces
        .lock()
        .expect("space lock")
        .values()
        .filter(|s| !s.deleted)
        .cloned()
        .collect();
    spaces.sort_by(|a, b| a.slug.cmp(&b.slug));
    Json(spaces)
}

#[derive(Debug, Deserialize)]
pub(crate) struct SpaceSaveDto {
    title: Option<String>,
    items: Option<Vec<SpaceItemDto>>,
}

#[derive(Debug, Deserialize)]
struct SpaceItemDto {
    kind: String,
    #[serde(rename = "ref")]
    reference: String,
}

pub(crate) async fn spaces_save(
    State(state): State<AppState>,
    axum::extract::Path(slug): axum::extract::Path<String>,
    Json(req): Json<SpaceSaveDto>,
) -> impl IntoResponse {
    let title = match req.title {
        Some(raw) => {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                return bad_request("title must not be empty").into_response();
            }
            trimmed
        }
        None => slug.clone(),
    };
    let mut items = Vec::new();
    for item in req.items.unwrap_or_default() {
        let kind = item.kind.trim().to_string();
        let reference = item.reference.trim().to_string();
        if kind != "session" && kind != "file" {
            return bad_request("item kind must be session or file").into_response();
        }
        if reference.is_empty() {
            return bad_request("item ref must not be empty").into_response();
        }
        items.push(SpaceItem { kind, reference });
    }
    let now = now_ms();
    let mut spaces = state.spaces.lock().expect("space lock");
    let space = match spaces.get_mut(&slug) {
        // Resurrecting a deleted slug starts fresh: the
        // old created_ms does not survive the tombstone.
        Some(existing) if !existing.deleted => {
            existing.title = title;
            existing.items = items;
            existing.updated_ms = now;
            existing.clone()
        }
        _ => {
            let fresh = Space {
                slug: slug.clone(),
                title,
                items,
                created_ms: now,
                updated_ms: now,
                deleted: false,
            };
            spaces.insert(slug, fresh.clone());
            fresh
        }
    };
    persist_space_line(&state, &SpaceStateLine::Space(space.clone()));
    Json(space).into_response()
}

pub(crate) async fn spaces_delete(
    State(state): State<AppState>,
    axum::extract::Path(slug): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut spaces = state.spaces.lock().expect("space lock");
    let Some(space) = spaces.get_mut(&slug) else {
        return (StatusCode::NOT_FOUND, Json(serde_json::json!({ "error": "unknown space" })))
            .into_response();
    };
    // Idempotent: deleting an already-deleted space
    // succeeds without a new record.
    if !space.deleted {
        space.deleted = true;
        space.updated_ms = now_ms();
        let updated = space.clone();
        persist_space_line(&state, &SpaceStateLine::Space(updated.clone()));
        return Json(updated).into_response();
    }
    Json(space.clone()).into_response()
}
