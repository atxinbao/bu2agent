//! Settings storage for the Round 2 settings page.
//!
//! One JSON file under the fixed app-data directory (never inside
//! the file root, never mixed into an audit JSONL):
//! `{app_data_dir}/settings.json` with a single field,
//! `default_working_dir`: a path RELATIVE to the file root, or
//! null for the root itself. The file root itself never moves
//! this round (no MOCK_FILES_ROOT change): the default working
//! directory is only the default location for new files and
//! tasks, and an explicitly entered path always wins.
//!
//! Validation (the user's rule, enforced here, not in the view):
//! only directories inside the current file root are accepted.
//! Both sides are canonicalized first, so a symlink inside the
//! root pointing outside it is refused, not followed. A stored
//! value that no longer resolves (deleted, moved, or outside
//! the root) loads as invalid: the page must show that and ask
//! for a re-selection, never silently fall back to another
//! location for writes.
//!
//! A missing or unparsable settings file loads as defaults.
//! Writes are atomic (temp file + rename), like the lockfile.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::gateway_home::{app_data_dir, default_working_dir};

/// On-disk settings. Only the default working directory lives
/// here this round; model credentials live in the Keychain, and
/// audit records live in their own JSONLs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SettingsFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_working_dir: Option<String>,
}

pub fn settings_path() -> PathBuf {
    app_data_dir().join("settings.json")
}

/// The file root in force: an explicit MOCK_FILES_ROOT wins
/// (dev/test override); otherwise the daily-app default.
/// When the shell is attached to a foreign gateway, this is
/// the shell's default root, not the foreign root -- the
/// commands refuse working-dir edits while attached, so this
/// value is never used to validate against a root it is not.
pub fn file_root() -> PathBuf {
    if let Ok(root) = std::env::var("MOCK_FILES_ROOT") {
        if !root.is_empty() {
            return PathBuf::from(root);
        }
    }
    default_working_dir()
}

pub fn load_settings() -> SettingsFile {
    let text = match fs::read_to_string(settings_path()) {
        Ok(text) => text,
        Err(_) => return SettingsFile::default(),
    };
    serde_json::from_str(&text).unwrap_or_default()
}

pub fn save_settings(settings: &SettingsFile) -> Result<(), String> {
    let dir = app_data_dir();
    fs::create_dir_all(&dir).map_err(|e| format!("cannot create settings directory: {e}"))?;
    let tmp = dir.join("settings.json.tmp");
    let text =
        serde_json::to_string(settings).map_err(|e| format!("cannot encode settings: {e}"))?;
    fs::write(&tmp, text).map_err(|e| format!("cannot write settings: {e}"))?;
    fs::rename(&tmp, settings_path()).map_err(|e| format!("cannot save settings: {e}"))?;
    Ok(())
}

/// Canonicalize a directory, refusing missing paths and files.
fn canonical_dir(path: &Path) -> Result<PathBuf, String> {
    let canonical = fs::canonicalize(path).map_err(|_| {
        format!(
            "directory does not exist or is not accessible: {}",
            path.display()
        )
    })?;
    if !canonical.is_dir() {
        return Err(format!("not a directory: {}", path.display()));
    }
    Ok(canonical)
}

/// Validate an absolute candidate directory and return its path
/// relative to the file root ("" for the root itself). Both sides
/// are canonicalized, so symlinks cannot smuggle a path outside.
pub fn validate_working_dir(root: &Path, candidate: &Path) -> Result<String, String> {
    let root = canonical_dir(root)?;
    let candidate = canonical_dir(candidate)?;
    let relative = candidate
        .strip_prefix(&root)
        .map_err(|_| "directory is outside the file root".to_string())?;
    Ok(relative.to_string_lossy().into_owned())
}

/// Outcome of resolving the stored default directory.
#[derive(Debug, Clone)]
pub struct ResolvedWorkingDir {
    /// Root-relative path as stored ("" = the root itself).
    pub relative: String,
    /// Absolute path for display (root-joined, not canonicalized
    /// when invalid, so the page can show what broke).
    pub absolute_display: String,
    pub valid: bool,
}

pub fn resolve_stored(root: &Path, stored: &Option<String>) -> ResolvedWorkingDir {
    let relative = stored.clone().unwrap_or_default();
    if relative.is_empty() {
        return ResolvedWorkingDir {
            relative: String::new(),
            absolute_display: root.to_string_lossy().into_owned(),
            valid: true,
        };
    }
    let stored_path = Path::new(&relative);
    if stored_path.is_absolute() {
        // Borrow ends before the move below: computing the
        // display string first keeps `relative` movable.
        let absolute_display = stored_path.to_string_lossy().into_owned();
        return ResolvedWorkingDir {
            relative,
            absolute_display,
            valid: false,
        };
    }
    let joined = root.join(stored_path);
    match canonical_dir(&joined) {
        Ok(canonical) => {
            let valid = canonical_dir(root)
                .map(|root| canonical.starts_with(&root))
                .unwrap_or(false);
            ResolvedWorkingDir {
                relative,
                absolute_display: canonical.to_string_lossy().into_owned(),
                valid,
            }
        }
        Err(_) => ResolvedWorkingDir {
            relative,
            absolute_display: joined.to_string_lossy().into_owned(),
            valid: false,
        },
    }
}
