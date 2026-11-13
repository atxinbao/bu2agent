//! D1-6b-4 (Q-D1-6b-16–21): Notes.app read + write via
//! AppleScript — the only viable path (D1-6a Notes
//! investigation). Notes.app has a full scripting
//! dictionary; the Notes database is never read or
//! written (fragile, largest privacy surface, never a
//! sync source); CloudKit cannot read the user's
//! existing Notes container.
//!
//! What the investigation + the cited public docs
//! establish (verified against the sources, not
//! assumed):
//!
//! - `note` fields: `name` / `id` / `container` /
//!   `body` (HTML, read/write) / `plaintext` (read-only)
//!   / `creation date` / `modification date` /
//!   `password protected` / `shared`.
//! - Ops: `make new note`, `delete`, `move`; no direct
//!   edit — change = read + `set body` full replace.
//! - Folder forms (apple-skill doc): `get name of every
//!   note in folder "Notes"`, `get plaintext of note
//!   "Note Title"`, `delete note "Note Title"`,
//!   `make new note at folder "Notes" with properties
//!   {name:"Title", body:"<h1>Title</h1><p>Body
//!   text</p>"}`. Folder names are case-sensitive.
//! - Note body uses **HTML, not plain text** (the doc's
//!   gotcha). The gateway takes plain text and converts
//!   it to minimal HTML (`plain_text_to_html`); the
//!   fact row records the hash of the re-read
//!   `plaintext`, never of the request text, so a
//!   Notes normalization can never silently corrupt the
//!   Evidence baseline.
//! - Deleting moves to **"Recently Deleted" — not
//!   permanent** (the doc's gotcha). The 6b-3 mail
//!   "Trash only" rule is the same shape here.
//! - Writes can take 30+ s (investigation), so the
//!   runner cap is 60 s, not mail's 30 s.
//! - Locked notes (`password protected`): bodies are
//!   unreadable; sync excludes them (Q-D1-6b-17).
//!
//! UNVERIFIED (best-effort, guard-tested, Mac window
//! validation is the real proof — the 6b-1 send-script
//! and 6b-3 delete-script precedents):
//!
//! - The folder-scoped `first note of folder "X" whose
//!   id is "Y"` lookup form.
//! - The per-note `try` fallback in the folder dump
//!   (a locked note must not abort the whole dump).
//! - `make new note` accepting `body` as given, and the
//!   plaintext round-trip of the HTML conversion
//!   (paragraph-per-line; a trailing empty line may or
//!   may not survive — the row records the re-read
//!   hash either way).
//! - Whether `password protected of n as string`
//!   renders `"true"` / `"false"`.
//!
//! Privacy (red line 7): the trait returns plaintext
//! to the gateway for hashing only; the `notes` table
//! stores `body_hash`, never the body. The script
//! output carries plaintext across the osascript
//! boundary (unavoidable — the hash must be computed
//! from the live text), but it lives only in the
//! runner's temp files, which are removed either way,
//! and never in a log, event, or Evidence row.
//!
//! Plaintext transport: `escText` escapes backslash,
//! return, linefeed, and tab into two-character
//! sequences, and `unescape_plaintext` reverses it in
//! Rust — the round-trip is exact, so an append
//! writes back the true text instead of the
//! newline-flattened `cleanText` form (mail's dump
//! flattens newlines to spaces, which is fine for a
//! display dump but would destroy a note on write-back).

use std::sync::Arc;

/// The Registry row this source writes against (the
/// `mail-source` precedent in `mail_sync.rs`).
pub const NOTES_SOURCE_ID: &str = "notes-source";

/// One note as the source returns it. `plaintext` is
/// the exact live text (unescaped); `body_hash` is
/// computed from it by the caller, never stored here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoteView {
    pub native_id: String,
    pub folder: String,
    pub name: String,
    pub plaintext: String,
    pub password_protected: bool,
}

/// What a create returns once the new note is re-read:
/// the Notes id plus the stored name / plaintext. The
/// re-read is load-bearing — the fact row records the
/// hash of what Notes actually stored, not of the
/// request text (the calendar identifier-recovery
/// precedent: never key a fact row on a guess).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedNote {
    pub native_id: String,
    pub name: String,
    pub plaintext: String,
}

/// Source failures. Handlers map these the same way as
/// `MailError`: `NotFound` -> 404, `Unavailable` ->
/// 501, `Denied` -> 403, `Upstream` -> 502.
/// `NotRunning` (Notes.app is not running) and
/// `Timeout` (the script was killed at the gateway's
/// 60 s cap) are separate variants because a write
/// route must tell them apart: a timeout leaves the
/// write ambiguous (Effect unknown), while "not
/// running" is a definite gateway-side failure (Effect
/// failed). `Unverified` is the create/append re-read
/// failure after a landed write: the write may or may
/// not have stored, so the outcome stays Unknown
/// (P0-R), never Failed.
#[derive(Debug, Clone)]
pub enum NotesError {
    Unavailable(String),
    Denied,
    NotFound,
    Upstream(String),
    NotRunning,
    Timeout,
    Unverified,
}

/// Notes read + write source. Reads are folder-scoped
/// (Q-D1-6b-19: user-named folder only, never the whole
/// library); the folder is always named by the caller,
/// so a note moved out of its folder reads as missing.
/// `get_note` returns `None` when no note with that id
/// is in the folder. `set_body` returns the post-write
/// re-read plaintext (the caller hashes it); a write
/// whose re-read fails is `Unverified`.
pub trait NotesSource: Send + Sync {
    fn folder_exists(&self, folder: &str) -> Result<bool, NotesError>;
    fn list_notes(&self, folder: &str) -> Result<Vec<NoteView>, NotesError>;
    fn get_note(&self, folder: &str, native_id: &str) -> Result<Option<NoteView>, NotesError>;
    fn create_note(&self, folder: &str, name: &str, body: &str) -> Result<CreatedNote, NotesError>;
    fn set_body(&self, folder: &str, native_id: &str, body: &str) -> Result<String, NotesError>;
    fn delete_note(&self, folder: &str, native_id: &str) -> Result<(), NotesError>;
}

/// The non-macOS stub: every call fails with the same
/// never-fake-data 501 wording as
/// `UnavailableMailSource` / `UnavailableCalendarBackend`.
pub struct UnavailableNotesSource;

impl NotesSource for UnavailableNotesSource {
    fn folder_exists(&self, _folder: &str) -> Result<bool, NotesError> {
        Err(NotesError::Unavailable(
            "notes unavailable on this platform".to_string(),
        ))
    }

    fn list_notes(&self, _folder: &str) -> Result<Vec<NoteView>, NotesError> {
        Err(NotesError::Unavailable(
            "notes unavailable on this platform".to_string(),
        ))
    }

    fn get_note(&self, _folder: &str, _native_id: &str) -> Result<Option<NoteView>, NotesError> {
        Err(NotesError::Unavailable(
            "notes unavailable on this platform".to_string(),
        ))
    }

    fn create_note(&self, _folder: &str, _name: &str, _body: &str) -> Result<CreatedNote, NotesError> {
        Err(NotesError::Unavailable(
            "notes unavailable on this platform".to_string(),
        ))
    }

    fn set_body(&self, _folder: &str, _native_id: &str, _body: &str) -> Result<String, NotesError> {
        Err(NotesError::Unavailable(
            "notes unavailable on this platform".to_string(),
        ))
    }

    fn delete_note(&self, _folder: &str, _native_id: &str) -> Result<(), NotesError> {
        Err(NotesError::Unavailable(
            "notes unavailable on this platform".to_string(),
        ))
    }
}

/// D1-6b-4: the production source for `AppState` —
/// AppleScript on macOS, the 501 stub everywhere else.
/// Tests override via `with_notes_source` (the
/// `with_mail_source` precedent).
pub fn default_notes_source() -> Arc<dyn NotesSource> {
    #[cfg(target_os = "macos")]
    {
        Arc::new(AppleScriptNotesSource)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Arc::new(UnavailableNotesSource)
    }
}

/// Escape one value for an AppleScript double-quoted
/// string literal (the `mail_source.rs` precedent:
/// backslash and quote are escaped, and newlines /
/// carriage returns / tabs become their escape
/// sequences; any other control character becomes a
/// space — it must never reach the script verbatim).
#[cfg(any(target_os = "macos", test))]
fn applescript_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out
}

/// Convert plain text to the minimal HTML the Notes
/// `body` property takes (the doc's "body uses HTML"
/// gotcha). Each line becomes one paragraph; HTML
/// metacharacters are escaped first so user text can
/// never break out of the markup. The conversion is
/// one-way by design: the fact row records the hash of
/// the re-read `plaintext`, never of this HTML, so
/// whatever Notes normalizes is the recorded truth.
#[cfg(any(target_os = "macos", test))]
fn plain_text_to_html(text: &str) -> String {
    let mut out = String::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        out.push_str("<p>");
        for c in line.chars() {
            match c {
                '&' => out.push_str("&amp;"),
                '<' => out.push_str("&lt;"),
                '>' => out.push_str("&gt;"),
                c => out.push(c),
            }
        }
        out.push_str("</p>");
    }
    out
}

/// Reverse `escText` (below): `\\` -> `\`, `\n` ->
/// linefeed, `\r` -> return, `\t` -> tab. Any other
/// backslash sequence is kept verbatim (both
/// characters), so an unpaired trailing backslash can
/// never silently vanish.
#[cfg(any(target_os = "macos", test))]
fn unescape_plaintext(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// Shared script handlers (the `mail_source.rs`
/// `DATE_HANDLERS` precedent): `cleanText` flattens a
/// single-line display field; `escText` escapes a
/// multi-line body for exact round-trip transport (see
/// the module docs). Both live at top level and are
/// called with `my`, because `text item delimiters`
/// must not be resolved inside `tell application
/// "Notes"`.
#[cfg(any(target_os = "macos", test))]
const TEXT_HANDLERS: &str = r#"on cleanText(t)
  set AppleScript's text item delimiters to {tab, return, linefeed}
  set parts to text items of t
  set AppleScript's text item delimiters to " "
  return parts as string
end cleanText

on escText(t)
  set AppleScript's text item delimiters to "\\"
  set parts to text items of t
  set AppleScript's text item delimiters to "\\\\"
  set t to parts as string
  set AppleScript's text item delimiters to return
  set parts to text items of t
  set AppleScript's text item delimiters to "\\n"
  set t to parts as string
  set AppleScript's text item delimiters to linefeed
  set parts to text items of t
  set AppleScript's text item delimiters to "\\n"
  set t to parts as string
  set AppleScript's text item delimiters to tab
  set parts to text items of t
  set AppleScript's text item delimiters to "\\t"
  set t to parts as string
  return t
end escText
"#;

/// Sentinel a lookup script returns when no note
/// matches (the mail `find_meta` precedent): the
/// caller maps it to `NotFound` instead of guessing.
/// Gated like the script builders and the dump parser
/// (macOS-or-test): a Linux lib build has no caller.
#[cfg(any(target_os = "macos", test))]
const NOTE_NOT_FOUND: &str = "«BU2AGENT_NOTE_NOT_FOUND»";

/// AppleScript for the folder list: one folder name per
/// line. Plain string construction (no macOS calls) so
/// the guard test runs on every platform.
#[cfg(any(target_os = "macos", test))]
fn folders_script() -> String {
    r#"with timeout of 30 seconds
  tell application "Notes"
    set out to ""
    repeat with f in folders
      set out to out & (name of f as string) & return
    end repeat
    return out
  end tell
end timeout"#
        .to_string()
}

/// AppleScript for one folder's dump: one line per
/// note, fields separated by character id 31 — id,
/// name (`cleanText`), plaintext (`escText`), locked
/// flag (`"true"` / `"false"`). A locked note's
/// plaintext read fails, so each note is wrapped in
/// its own `try`: the fallback line keeps the id and
/// name with an empty body and `locked=true`, and a
/// note whose id itself is unreadable is skipped
/// (it cannot be synced without an id). Plain string
/// construction so the guard test runs on every
/// platform. AppleScript if-grammar note (the mail U2
/// 502 fix): a single-line `if ... then ...` is
/// self-contained and must NOT be followed by `end
/// if`; every `if` below is therefore multi-line.
#[cfg(any(target_os = "macos", test))]
fn folder_notes_script(folder: &str) -> String {
    format!(
        r#"{TEXT_HANDLERS}with timeout of 60 seconds
  tell application "Notes"
    set out to ""
    repeat with n in notes of folder "{folder}"
      try
        set out to out & (id of n as string) & (character id 31) & my cleanText(name of n) & (character id 31) & my escText(plaintext of n) & (character id 31) & (password protected of n as string) & return
      on error
        try
          set out to out & (id of n as string) & (character id 31) & my cleanText(name of n) & (character id 31) & "" & (character id 31) & "true" & return
        on error
          set dummy to 0
        end try
      end try
    end repeat
    return out
  end tell
end timeout"#,
        TEXT_HANDLERS = TEXT_HANDLERS,
        folder = applescript_escape(folder),
    )
}

/// AppleScript for one note by id, folder-scoped.
/// Returns the same four fields as the dump, or the
/// not-found sentinel. Plain string construction so
/// the guard test runs on every platform.
#[cfg(any(target_os = "macos", test))]
fn find_note_script(folder: &str, native_id: &str) -> String {
    format!(
        r#"{TEXT_HANDLERS}with timeout of 30 seconds
  tell application "Notes"
    try
      set n to first note of folder "{folder}" whose id is "{native_id}"
      return (id of n as string) & (character id 31) & my cleanText(name of n) & (character id 31) & my escText(plaintext of n) & (character id 31) & (password protected of n as string)
    on error
      return "{sentinel}"
    end try
  end tell
end timeout"#,
        TEXT_HANDLERS = TEXT_HANDLERS,
        folder = applescript_escape(folder),
        native_id = applescript_escape(native_id),
        sentinel = NOTE_NOT_FOUND,
    )
}

/// AppleScript for creating one note (best-effort —
/// the `make new note` form is doc-verified, but
/// whether the `name` property sticks vs the title
/// being derived from the body is UNVERIFIED: the
/// re-read below is authoritative either way). The
/// new note is re-read in the same script so the
/// caller never keys the fact row on the request. The
/// body is the caller's plain text converted to
/// minimal HTML (`plain_text_to_html`). Plain string
/// construction so the guard test runs on every
/// platform.
#[cfg(any(target_os = "macos", test))]
fn create_note_script(folder: &str, name: &str, body_html: &str) -> String {
    format!(
        r#"{TEXT_HANDLERS}with timeout of 60 seconds
  tell application "Notes"
    set n to make new note at folder "{folder}" with properties {{name:"{name}", body:"{body_html}"}}
    return (id of n as string) & (character id 31) & my cleanText(name of n) & (character id 31) & my escText(plaintext of n)
  end tell
end timeout"#,
        TEXT_HANDLERS = TEXT_HANDLERS,
        folder = applescript_escape(folder),
        name = applescript_escape(name),
        body_html = applescript_escape(body_html),
    )
}

/// AppleScript for replacing one note's body
/// (best-effort — the `set body` full-replace form is
/// the investigation's documented write path; the
/// post-write re-read is authoritative). The caller
/// has already verified the note exists and is not
/// locked, so any failure here is a genuine write
/// failure (`Upstream`), not a lookup miss. Plain
/// string construction so the guard test runs on every
/// platform.
#[cfg(any(target_os = "macos", test))]
fn set_body_script(folder: &str, native_id: &str, body_html: &str) -> String {
    format!(
        r#"{TEXT_HANDLERS}with timeout of 60 seconds
  tell application "Notes"
    set n to first note of folder "{folder}" whose id is "{native_id}"
    set body of n to "{body_html}"
    return my escText(plaintext of n)
  end tell
end timeout"#,
        TEXT_HANDLERS = TEXT_HANDLERS,
        folder = applescript_escape(folder),
        native_id = applescript_escape(native_id),
        body_html = applescript_escape(body_html),
    )
}

/// AppleScript for deleting one note by id
/// (best-effort — `delete` is the documented form and
/// the doc verifies it moves to "Recently Deleted",
/// not permanent; window validation confirms on a real
/// Mac). A lookup miss returns the not-found sentinel
/// (the mail `delete_script` precedent: any failure
/// inside the `try` reports as not-found, so a
/// concurrent delete reads as `NotFound`, never a
/// guess). Plain string construction so the guard test
/// runs on every platform.
#[cfg(any(target_os = "macos", test))]
fn delete_note_script(folder: &str, native_id: &str) -> String {
    format!(
        r#"with timeout of 30 seconds
  tell application "Notes"
    try
      set n to first note of folder "{folder}" whose id is "{native_id}"
      delete n
      return "deleted"
    on error
      return "{sentinel}"
    end try
  end tell
end timeout"#,
        folder = applescript_escape(folder),
        native_id = applescript_escape(native_id),
        sentinel = NOTE_NOT_FOUND,
    )
}

/// Parse one dump line into a `NoteView`. Four
/// unit-separated fields: native id, name, escaped
/// plaintext, locked flag. A malformed line is
/// skipped, never guessed.
#[cfg(any(target_os = "macos", test))]
fn parse_note_line(folder: &str, line: &str) -> Option<NoteView> {
    let parts: Vec<&str> = line.split('\u{1f}').collect();
    // Exactly four fields: a short line is malformed
    // (a note id alone is not a note), never guessed.
    // The plaintext is escaped, so a real separator
    // inside it cannot add a fifth field.
    if parts.len() != 4 {
        return None;
    }
    let native_id = parts[0].to_string();
    let name = parts[1].to_string();
    let plaintext = unescape_plaintext(parts[2]);
    let locked = parts[3] == "true";
    if native_id.is_empty() {
        return None;
    }
    Some(NoteView {
        native_id,
        folder: folder.to_string(),
        name,
        plaintext,
        password_protected: locked,
    })
}

/// Parse a whole dump into notes, skipping the
/// not-found sentinel and blank lines.
#[cfg(any(target_os = "macos", test))]
fn parse_notes_dump(folder: &str, dump: &str) -> Vec<NoteView> {
    dump.split(['\r', '\n'])
        .filter(|line| !line.trim().is_empty() && line.trim() != NOTE_NOT_FOUND)
        .filter_map(|line| parse_note_line(folder, line))
        .collect()
}

/// The AppleScript source. The `run_script` runner and
/// the `notes_running` check mirror `mail_source.rs`
/// (temp files, not pipes; 60 s cap, not 30 s — the
/// investigation: Notes writes can take 30+ s).
#[cfg(target_os = "macos")]
pub struct AppleScriptNotesSource;

/// Hard cap on one `osascript` invocation (see the
/// module docs). Expiry maps to `Timeout`.
#[cfg(target_os = "macos")]
const SCRIPT_TIMEOUT_SECS: u64 = 60;

#[cfg(target_os = "macos")]
static SCRIPT_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(target_os = "macos")]
impl AppleScriptNotesSource {
    /// Is Notes.app running? Uses `pgrep` (no permission
    /// needed) so a read never launches Notes as a side
    /// effect. If `pgrep` itself is missing, proceed and
    /// let the script report the real error.
    fn notes_running() -> bool {
        match std::process::Command::new("pgrep")
            .args(["-x", "Notes"])
            .output()
        {
            Ok(output) => output.status.success(),
            Err(_) => true,
        }
    }

    /// Run one AppleScript program, capped at
    /// `SCRIPT_TIMEOUT_SECS`. Maps automation denial to
    /// `Denied`, a missing Notes.app to `NotRunning`
    /// (never launch Notes.app for a read or a write),
    /// a timeout to `Timeout` (the caller must treat
    /// the outcome as unknown, never guessed), and
    /// everything else to a generic `Upstream`. Output
    /// goes to temp files, not pipes (the mail
    /// precedent: a large dump could fill a pipe buffer
    /// while the parent polls, deadlocking the wait;
    /// temp files also make the timeout kill clean).
    /// Error strings stay generic on purpose:
    /// automation stderr can contain local paths.
    fn run_script(script: &str) -> Result<String, NotesError> {
        use std::fs;
        use std::process::{Command, Stdio};
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};
        if !Self::notes_running() {
            return Err(NotesError::NotRunning);
        }
        let n = SCRIPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let stem = format!("bu2agent-notes-script-{}-{n}", std::process::id());
        let out_path = std::env::temp_dir().join(format!("{stem}.out"));
        let err_path = std::env::temp_dir().join(format!("{stem}.err"));
        let out_file = fs::File::create(&out_path)
            .map_err(|_| NotesError::Upstream("could not capture Notes script output".to_string()))?;
        let err_file = fs::File::create(&err_path)
            .map_err(|_| NotesError::Upstream("could not capture Notes script output".to_string()))?;
        let mut child = Command::new("osascript")
            .arg("-e")
            .arg(script)
            .stdout(Stdio::from(out_file))
            .stderr(Stdio::from(err_file))
            .spawn()
            .map_err(|_| NotesError::Upstream("osascript unavailable".to_string()))?;
        let deadline = Instant::now() + Duration::from_secs(SCRIPT_TIMEOUT_SECS);
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let stdout = fs::read(&out_path)
                        .map(|b| String::from_utf8_lossy(&b).to_string())
                        .unwrap_or_default();
                    let stderr = fs::read(&err_path)
                        .map(|b| String::from_utf8_lossy(&b).to_string())
                        .unwrap_or_default();
                    let _ = fs::remove_file(&out_path);
                    let _ = fs::remove_file(&err_path);
                    if status.success() {
                        return Ok(stdout);
                    }
                    let lower = stderr.to_lowercase();
                    if lower.contains("not allowed")
                        || lower.contains("denied")
                        || lower.contains("not authorized")
                    {
                        return Err(NotesError::Denied);
                    }
                    return Err(NotesError::Upstream("Notes automation failed".to_string()));
                }
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        let _ = fs::remove_file(&out_path);
                        let _ = fs::remove_file(&err_path);
                        return Err(NotesError::Timeout);
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = fs::remove_file(&out_path);
                    let _ = fs::remove_file(&err_path);
                    return Err(NotesError::Upstream("Notes automation failed".to_string()));
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl NotesSource for AppleScriptNotesSource {
    fn folder_exists(&self, folder: &str) -> Result<bool, NotesError> {
        let out = Self::run_script(&folders_script())?;
        Ok(out
            .split(['\r', '\n'])
            .any(|line| line.trim() == folder))
    }

    fn list_notes(&self, folder: &str) -> Result<Vec<NoteView>, NotesError> {
        let dump = Self::run_script(&folder_notes_script(folder))?;
        Ok(parse_notes_dump(folder, &dump))
    }

    fn get_note(&self, folder: &str, native_id: &str) -> Result<Option<NoteView>, NotesError> {
        let out = Self::run_script(&find_note_script(folder, native_id))?;
        Ok(parse_notes_dump(folder, &out).into_iter().next())
    }

    fn create_note(&self, folder: &str, name: &str, body: &str) -> Result<CreatedNote, NotesError> {
        let body_html = plain_text_to_html(body);
        let out = Self::run_script(&create_note_script(folder, name, &body_html))?;
        // Strip only osascript's own trailing newline:
        // the plaintext travels escaped, so a real
        // trailing newline here is the terminator, never
        // note content.
        let out = out.trim_end_matches(['\r', '\n']);
        let mut parts = out.split('\u{1f}');
        let Some(native_id) = parts.next().filter(|s| !s.is_empty()) else {
            return Err(NotesError::Unverified);
        };
        let name = parts.next().unwrap_or("").to_string();
        let plaintext = unescape_plaintext(parts.next().unwrap_or(""));
        // The request body is non-empty, so an empty
        // re-read means the write did not stick or the
        // re-read failed — Unverified, never a guessed
        // empty row.
        if plaintext.is_empty() {
            return Err(NotesError::Unverified);
        }
        Ok(CreatedNote {
            native_id: native_id.to_string(),
            name,
            plaintext,
        })
    }

    fn set_body(&self, folder: &str, native_id: &str, body: &str) -> Result<String, NotesError> {
        let body_html = plain_text_to_html(body);
        let out = Self::run_script(&set_body_script(folder, native_id, &body_html))?;
        // Strip only osascript's own trailing newline
        // (see create_note): the plaintext travels
        // escaped, so a real trailing newline here is
        // the terminator, never note content.
        let plaintext = unescape_plaintext(out.trim_end_matches(['\r', '\n']));
        if plaintext.is_empty() {
            return Err(NotesError::Unverified);
        }
        Ok(plaintext)
    }

    fn delete_note(&self, folder: &str, native_id: &str) -> Result<(), NotesError> {
        let out = Self::run_script(&delete_note_script(folder, native_id))?;
        if out.trim() == "deleted" {
            Ok(())
        } else {
            Err(NotesError::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notes_scripts_carry_the_doc_verified_forms() {
        // Folder addressing by name, `make new note at
        // folder ... with properties {name, body}`,
        // `set body` full replace, `delete`, and the
        // plaintext read — the apple-skill doc forms.
        let dump = folder_notes_script("bu2agent-test");
        assert!(dump.contains("notes of folder \"bu2agent-test\""));
        assert!(dump.contains("plaintext of n"));
        assert!(dump.contains("password protected of n"));
        assert!(dump.contains("on escText(t)"));
        let create = create_note_script("bu2agent-test", "d1-6b title", "<p>hi</p>");
        assert!(create.contains("make new note at folder \"bu2agent-test\""));
        assert!(create.contains("with properties {name:\"d1-6b title\""));
        assert!(create.contains("plaintext of n"));
        let set = set_body_script("bu2agent-test", "x-coredata://n/1", "<p>new</p>");
        assert!(set.contains("set body of n to"));
        assert!(set.contains("whose id is \"x-coredata://n/1\""));
        let delete = delete_note_script("bu2agent-test", "x-coredata://n/1");
        assert!(delete.contains("delete n"));
        assert!(delete.contains(NOTE_NOT_FOUND));
        let find = find_note_script("bu2agent-test", "x-coredata://n/1");
        assert!(find.contains("first note of folder \"bu2agent-test\" whose id is"));
        assert!(find.contains(NOTE_NOT_FOUND));
        let folders = folders_script();
        assert!(folders.contains("repeat with f in folders"));
    }

    #[test]
    fn plain_text_to_html_escapes_and_wraps_lines() {
        assert_eq!(plain_text_to_html("hello"), "<p>hello</p>");
        assert_eq!(
            plain_text_to_html("a & b\n<c>"),
            "<p>a &amp; b</p><p>&lt;c&gt;</p>"
        );
        assert_eq!(plain_text_to_html(""), "<p></p>");
    }

    #[test]
    fn plaintext_escape_round_trips_exactly() {
        // The escText / unescape_plaintext pair must be
        // exact: a literal backslash-n in the note must
        // survive, newlines and tabs must come back, and
        // a trailing backslash must not vanish.
        let cases = [
            "plain",
            "two\nlines",
            "tab\there",
            "literal \\n not a newline",
            "backslash \\ end",
            "carriage\rreturn",
            "mixed \\n\n\\t\t end\\",
        ];
        for original in cases {
            // Simulate escText in Rust: backslash first,
            // then return / linefeed / tab.
            let escaped = original
                .replace('\\', "\\\\")
                .replace('\r', "\\r")
                .replace('\n', "\\n")
                .replace('\t', "\\t");
            assert_eq!(unescape_plaintext(&escaped), original, "case: {original:?}");
        }
        // Unpaired escapes stay verbatim, never vanish.
        assert_eq!(unescape_plaintext("a\\qb"), "a\\qb");
        assert_eq!(unescape_plaintext("a\\"), "a\\");
    }

    #[test]
    fn notes_dump_parses_locked_and_multiline_notes() {
        // One unlocked note with an escaped newline in
        // the body, one locked note with an empty body,
        // one malformed line (skipped), one sentinel.
        let dump = [
            "id-1\u{1f}First\u{1f}line one\\nline two\u{1f}false",
            "id-2\u{1f}Locked\u{1f}\u{1f}true",
            "no-separators-here",
            NOTE_NOT_FOUND,
            "",
        ]
        .join("\n");
        let notes = parse_notes_dump("bu2agent-test", &dump);
        assert_eq!(notes.len(), 2);
        assert_eq!(notes[0].native_id, "id-1");
        assert_eq!(notes[0].plaintext, "line one\nline two");
        assert!(!notes[0].password_protected);
        assert_eq!(notes[1].native_id, "id-2");
        assert_eq!(notes[1].plaintext, "");
        assert!(notes[1].password_protected);
        assert_eq!(notes[1].folder, "bu2agent-test");
    }

    #[test]
    fn unavailable_source_is_never_fake_data() {
        let source = UnavailableNotesSource;
        assert!(matches!(
            source.folder_exists("x"),
            Err(NotesError::Unavailable(_))
        ));
        assert!(matches!(
            source.list_notes("x"),
            Err(NotesError::Unavailable(_))
        ));
    }
}
