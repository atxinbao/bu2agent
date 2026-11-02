//! Voice record surface (Task 21, record subset, D2).
//!
//! The voice domain splits into a record side that can be
//! built locally and a real-time side that cannot: calls,
//! dictation, and avatar-video preparation need real audio
//! and media endpoints, and onboarding/reset's flow state
//! is unknown. D2 covers 11 record routes plus a narrowly
//! defined onboarding reset (it clears the active voice
//! and pipeline selection back to null and touches nothing
//! else — see the Task 21 plan).
//!
//! Seeds are static and labeled [Mock]: they are not the
//! original product's real voices. The catalog and
//! pipeline profiles live in code and are never persisted;
//! custom voices start from seeds and are overlaid by
//! JSONL lines (rename/delete), mirroring the feed store's
//! seed-plus-overlay shape. Active voice and pipeline
//! selection are single last-wins records. Nothing here
//! plays audio: selections are record-only.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// One catalog voice (static seed, never persisted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogVoice {
    pub voice_id: String,
    pub name: String,
    pub locale: String,
    pub available: bool,
}

/// One pipeline profile (static seed, never persisted).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineProfile {
    pub profile_id: String,
    pub name: String,
    pub description: String,
}

/// One saved custom voice: seed plus JSONL overlay.
/// Delete is a tombstone, not erasure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomVoice {
    pub saved_voice_id: String,
    pub name: String,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(default)]
    pub deleted: bool,
}

/// Active voice selection (single last-wins record).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveVoiceRecord {
    pub voice_id: Option<String>,
    pub updated_ms: u64,
}

/// Pipeline selection (single last-wins record).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSelectionRecord {
    pub profile_id: Option<String>,
    pub updated_ms: u64,
}

/// One tagged line in voice.jsonl.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum VoiceLine {
    #[serde(rename = "custom-voice")]
    CustomVoice(CustomVoice),
    #[serde(rename = "active-voice")]
    ActiveVoice(ActiveVoiceRecord),
    #[serde(rename = "pipeline-selection")]
    PipelineSelection(PipelineSelectionRecord),
}

/// Everything loaded from voice.jsonl.
#[derive(Debug, Default)]
pub struct VoiceStateData {
    pub custom_voices: Vec<CustomVoice>,
    pub active_voice: Option<ActiveVoiceRecord>,
    pub pipeline_selection: Option<PipelineSelectionRecord>,
}

/// Static [Mock] catalog seeds.
pub fn catalog_voices() -> Vec<CatalogVoice> {
    vec![
        CatalogVoice {
            voice_id: "voice-seed-1".to_string(),
            name: "Mock Voice Alpha".to_string(),
            locale: "en-US".to_string(),
            available: true,
        },
        CatalogVoice {
            voice_id: "voice-seed-2".to_string(),
            name: "Mock Voice Beta".to_string(),
            locale: "zh-CN".to_string(),
            available: true,
        },
        CatalogVoice {
            voice_id: "voice-seed-3".to_string(),
            name: "Mock Voice Gamma".to_string(),
            locale: "es-ES".to_string(),
            available: true,
        },
    ]
}

/// Filter dimensions computed from the catalog seeds.
pub fn catalog_locales() -> Vec<String> {
    let mut locales: Vec<String> = catalog_voices().iter().map(|v| v.locale.clone()).collect();
    locales.sort();
    locales.dedup();
    locales
}

/// Static [Mock] pipeline profile seeds.
pub fn pipeline_profiles() -> Vec<PipelineProfile> {
    vec![
        PipelineProfile {
            profile_id: "pipeline-seed-standard".to_string(),
            name: "Standard".to_string(),
            description: "Default pipeline".to_string(),
        },
        PipelineProfile {
            profile_id: "pipeline-seed-low-latency".to_string(),
            name: "Low latency".to_string(),
            description: "Low latency pipeline".to_string(),
        },
    ]
}

/// Static [Mock] custom voice seeds.
pub fn custom_voice_seeds() -> Vec<CustomVoice> {
    vec![
        CustomVoice {
            saved_voice_id: "saved-voice-1".to_string(),
            name: "Mock Saved 1".to_string(),
            created_ms: 0,
            updated_ms: 0,
            deleted: false,
        },
        CustomVoice {
            saved_voice_id: "saved-voice-2".to_string(),
            name: "Mock Saved 2".to_string(),
            created_ms: 0,
            updated_ms: 0,
            deleted: false,
        },
    ]
}

/// Load voice lines; malformed lines are skipped with a
/// warning, mirroring the other stores. Last line per
/// custom voice id wins; active voice and pipeline
/// selection keep only their latest record.
pub fn load_voice_state(path: &Path) -> VoiceStateData {
    let Ok(text) = std::fs::read_to_string(path) else {
        return VoiceStateData::default();
    };
    let mut data = VoiceStateData::default();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<VoiceLine>(line) {
            Ok(VoiceLine::CustomVoice(record)) => {
                if let Some(existing) = data
                    .custom_voices
                    .iter_mut()
                    .find(|v| v.saved_voice_id == record.saved_voice_id)
                {
                    *existing = record;
                } else {
                    data.custom_voices.push(record);
                }
            }
            Ok(VoiceLine::ActiveVoice(record)) => data.active_voice = Some(record),
            Ok(VoiceLine::PipelineSelection(record)) => data.pipeline_selection = Some(record),
            Err(_) => eprintln!(
                "warning: skipping malformed voice state line in {}",
                path.display()
            ),
        }
    }
    data
}

/// Append one voice line; failures are stderr warnings
/// only, mirroring the other stores.
pub fn append_voice_line(path: &Path, line: &VoiceLine) {
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("warning: cannot append voice state to {}: {err}", path.display());
            return;
        }
    };
    if let Err(err) = writeln!(file, "{}", serde_json::to_string(line).unwrap_or_default()) {
        eprintln!("warning: cannot append voice state to {}: {err}", path.display());
    }
}

/// Merge seed custom voices with JSONL overlays (last-wins
/// per id), tombstones included. Order is by saved voice
/// id for a deterministic list.
pub fn merged_custom_voices(overlays: &[CustomVoice]) -> Vec<CustomVoice> {
    let mut merged = custom_voice_seeds();
    for overlay in overlays {
        if let Some(existing) = merged
            .iter_mut()
            .find(|v| v.saved_voice_id == overlay.saved_voice_id)
        {
            *existing = overlay.clone();
        } else {
            merged.push(overlay.clone());
        }
    }
    merged.sort_by(|a, b| a.saved_voice_id.cmp(&b.saved_voice_id));
    merged
}

/// Merged custom voices minus tombstones (the list view).
pub fn visible_custom_voices(overlays: &[CustomVoice]) -> Vec<CustomVoice> {
    let mut merged = merged_custom_voices(overlays);
    merged.retain(|v| !v.deleted);
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeds_and_filters_agree() {
        let catalog = catalog_voices();
        assert_eq!(catalog.len(), 3);
        assert!(catalog.iter().all(|v| v.available));
        assert_eq!(catalog_locales(), vec!["en-US", "es-ES", "zh-CN"]);
        assert_eq!(pipeline_profiles().len(), 2);
        assert_eq!(visible_custom_voices(&[]).len(), 2);
    }

    #[test]
    fn overlay_rename_and_tombstone_apply() {
        let overlays = vec![
            CustomVoice {
                saved_voice_id: "saved-voice-1".to_string(),
                name: "Renamed".to_string(),
                created_ms: 0,
                updated_ms: 5,
                deleted: false,
            },
            CustomVoice {
                saved_voice_id: "saved-voice-2".to_string(),
                name: "Mock Saved 2".to_string(),
                created_ms: 0,
                updated_ms: 6,
                deleted: true,
            },
        ];
        let visible = visible_custom_voices(&overlays);
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].saved_voice_id, "saved-voice-1");
        assert_eq!(visible[0].name, "Renamed");
    }
}
