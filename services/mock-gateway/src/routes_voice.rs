//! Voice routes: /api/voice catalog, pipeline profiles, custom voices,
//! active voice, onboarding reset.
//!
//! The handlers moved here from `server.rs` (FU-5 S-3). The rest of the
//! route directory lives in `server.rs`. There is no `//!` directory line
//! for voice in the `server.rs` header, so no directory line moved with
//! this batch. The voice store (`voice_store.rs`) already holds the
//! catalog seeds, record types, and persistence; only the handlers move.

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;

use crate::routes_common::{bad_request, now_ms};
use crate::server::AppState;
use crate::voice_store::{
    append_voice_line, catalog_locales, catalog_voices, pipeline_profiles,
    ActiveVoiceRecord, CustomVoice, PipelineSelectionRecord, VoiceLine,
};

// --- Voice record surface (Task 21, record subset, D2) ---
//
// Local semantics only (see voice_store.rs and the Task 21
// plan): static [Mock] catalog and pipeline seeds, custom
// voice rename/delete over seeds, record-only active voice
// and pipeline selections, and an onboarding reset that
// clears just those two selections. No audio plays here.

pub(crate) fn persist_voice_line(state: &AppState, line: &VoiceLine) {
    let Some(path) = &state.voice_state_path else {
        return;
    };
    append_voice_line(path, line);
}

pub(crate) fn custom_voice_view(voice: &CustomVoice) -> serde_json::Value {
    serde_json::json!({
        "saved_voice_id": voice.saved_voice_id,
        "name": voice.name,
        "created_ms": voice.created_ms,
        "updated_ms": voice.updated_ms,
        "deleted": voice.deleted,
    })
}

pub(crate) async fn voice_catalog() -> Json<Vec<crate::voice_store::CatalogVoice>> {
    Json(catalog_voices())
}

pub(crate) async fn voice_catalog_filters() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "locales": catalog_locales() }))
}

pub(crate) async fn voice_available_voices() -> Json<Vec<crate::voice_store::CatalogVoice>> {
    Json(catalog_voices().into_iter().filter(|v| v.available).collect())
}

pub(crate) async fn voice_pipeline_profiles() -> Json<Vec<crate::voice_store::PipelineProfile>> {
    Json(pipeline_profiles())
}

#[derive(Debug, Deserialize)]
pub(crate) struct VoiceProfileSelectDto {
    profile_id: String,
}

pub(crate) async fn voice_pipeline_profile_select(
    State(state): State<AppState>,
    Json(req): Json<VoiceProfileSelectDto>,
) -> impl IntoResponse {
    if !pipeline_profiles().iter().any(|p| p.profile_id == req.profile_id) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown pipeline profile" })),
        )
            .into_response();
    }
    *state.voice_pipeline.lock().expect("voice pipeline lock") = Some(req.profile_id.clone());
    persist_voice_line(
        &state,
        &VoiceLine::PipelineSelection(PipelineSelectionRecord {
            profile_id: Some(req.profile_id.clone()),
            updated_ms: now_ms(),
        }),
    );
    Json(serde_json::json!({ "profile_id": req.profile_id })).into_response()
}

pub(crate) async fn voice_custom_voices_list(State(state): State<AppState>) -> Json<Vec<serde_json::Value>> {
    let customs = state.voice_custom.lock().expect("voice custom lock");
    let mut voices: Vec<&CustomVoice> = customs.values().filter(|v| !v.deleted).collect();
    voices.sort_by(|a, b| a.saved_voice_id.cmp(&b.saved_voice_id));
    Json(voices.iter().map(|v| custom_voice_view(v)).collect())
}

#[derive(Debug, Deserialize)]
pub(crate) struct VoiceRenameDto {
    name: String,
}

pub(crate) async fn voice_custom_voice_rename(
    State(state): State<AppState>,
    axum::extract::Path(saved_voice_id): axum::extract::Path<String>,
    Json(req): Json<VoiceRenameDto>,
) -> impl IntoResponse {
    let mut customs = state.voice_custom.lock().expect("voice custom lock");
    let Some(voice) = customs.get_mut(&saved_voice_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown custom voice" })),
        )
            .into_response();
    };
    if voice.deleted {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown custom voice" })),
        )
            .into_response();
    }
    if req.name.trim().is_empty() {
        return bad_request("name must not be empty").into_response();
    }
    voice.name = req.name.clone();
    voice.updated_ms = now_ms();
    let updated = voice.clone();
    drop(customs);
    persist_voice_line(&state, &VoiceLine::CustomVoice(updated.clone()));
    Json(custom_voice_view(&updated)).into_response()
}

pub(crate) async fn voice_custom_voice_delete(
    State(state): State<AppState>,
    axum::extract::Path(saved_voice_id): axum::extract::Path<String>,
) -> impl IntoResponse {
    let mut customs = state.voice_custom.lock().expect("voice custom lock");
    let Some(voice) = customs.get_mut(&saved_voice_id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown custom voice" })),
        )
            .into_response();
    };
    if !voice.deleted {
        voice.deleted = true;
        voice.updated_ms = now_ms();
        let updated = voice.clone();
        drop(customs);
        persist_voice_line(&state, &VoiceLine::CustomVoice(updated.clone()));
        return Json(custom_voice_view(&updated)).into_response();
    }
    let view = custom_voice_view(voice);
    Json(view).into_response()
}

pub(crate) async fn voice_active_voice_get(State(state): State<AppState>) -> Json<serde_json::Value> {
    let voice_id = state.voice_active.lock().expect("voice active lock").clone();
    Json(serde_json::json!({ "voice_id": voice_id }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct VoiceActiveSaveDto {
    voice_id: String,
}

pub(crate) async fn voice_active_voice_save(
    State(state): State<AppState>,
    Json(req): Json<VoiceActiveSaveDto>,
) -> impl IntoResponse {
    let in_catalog = catalog_voices().iter().any(|v| v.voice_id == req.voice_id);
    let in_custom = state
        .voice_custom
        .lock()
        .expect("voice custom lock")
        .get(&req.voice_id)
        .is_some_and(|v| !v.deleted);
    if !in_catalog && !in_custom {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown voice" })),
        )
            .into_response();
    }
    *state.voice_active.lock().expect("voice active lock") = Some(req.voice_id.clone());
    persist_voice_line(
        &state,
        &VoiceLine::ActiveVoice(ActiveVoiceRecord {
            voice_id: Some(req.voice_id.clone()),
            updated_ms: now_ms(),
        }),
    );
    Json(serde_json::json!({ "voice_id": req.voice_id })).into_response()
}

pub(crate) async fn voice_onboarding_reset(State(state): State<AppState>) -> Json<serde_json::Value> {
    // D2: clears only the two selections, back to null.
    // Custom voices are untouched.
    *state.voice_active.lock().expect("voice active lock") = None;
    *state.voice_pipeline.lock().expect("voice pipeline lock") = None;
    let now = now_ms();
    persist_voice_line(
        &state,
        &VoiceLine::ActiveVoice(ActiveVoiceRecord { voice_id: None, updated_ms: now }),
    );
    persist_voice_line(
        &state,
        &VoiceLine::PipelineSelection(PipelineSelectionRecord {
            profile_id: None,
            updated_ms: now,
        }),
    );
    Json(serde_json::json!({ "voice_id": null, "profile_id": null }))
}
