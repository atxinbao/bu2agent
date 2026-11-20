//! iMessage thread read surface, MCL-1 (threads-only).
//!
//! MCL-1 syncs the Messages.app thread list into SQLite
//! (`imessage_threads`). It does NOT read message history:
//! the MCL-0 probe + supplement proved AppleScript has no
//! message element (the dictionary exposes only id / name /
//! account / participant on chat; a real-session read of
//! `messages of chat 1` errors -1700), so there is no
//! `messages` method here at all — a `messages of`
//! guard test below locks that in. chat.db is read-only
//! investigation, never a fact source (red line 7).
//!
//! What the dictionary DOES expose (probe-verified,
//! `Messages.sdef` read firsthand):
//!
//! - `count chats` enumerates threads; each chat has `id`
//!   (a guid string — the probe's self-sent session id
//!   looked like `any;-;+8619505710926`), `name` (missing
//!   value when unset), and `account`.
//! - `service type of account of chat` is the enumeration
//!   SMS / iMessage / RCS. The `service` column stores
//!   that string verbatim; RCS is a possible third value
//!   from the dictionary (the plan names iMessage/SMS).
//! - `participants of chat` are the OTHER participants
//!   (the probe's self-sent session had exactly 1); each
//!   participant has a `handle` (phone / email text).
//!   Only the handle HASH + count are stored (Q-MCL-07):
//!   `participant_hash` is the sha256 of the canonical
//!   JSON of the sorted per-handle sha256 hexes, so it is
//!   order-independent and names no one. `is_group` is
//!   `participant_count > 1` (local definition).
//!
//! `last_message_ms` / `unread_count` have no message
//! data source under AppleScript sync and stay NULL —
//! never fabricated (plan §3, re-approved 2026-11-18).
//! The chat `name` has no column in the approved table
//! and is not carried in the dump: names resolve later
//! via the contacts fact table (Q-MCL-07's reasoning).
//!
//! The source is a trait so the mechanism can change
//! without touching routes, clients, or UI:
//!
//! - `AppleScriptIMessageSource` (macOS only): asks
//!   Messages.app via `osascript`. The dump script form
//!   is best-effort and window validation on a real Mac
//!   is the proof, as with the batch-1 mail read scripts.
//! - `UnavailableIMessageSource`: every call fails with
//!   `IMessageError::Unavailable` (501, never fake data).
//!
//! MCL-4 adds `send` to the trait: one plain-text
//! message to one already-synced thread, addressed by
//! the thread's AppleScript chat id. The `send` verb is
//! the MCL-0 probe-verified form (`send "…" to first
//! chat` exited 0 with the reply bubble visible); the
//! `chat id` addressing targets the requested thread
//! instead of whichever chat happens to be first —
//! window validation on a real Mac is the proof of that
//! addressing, same best-effort caveat as the dump
//! script. "Sent" means Messages.app accepted the send,
//! never that the other side received it (Q-MCL-21);
//! the script has no read-back of the sent message's id
//! (that would need message history — falsified), so
//! `SentIMessage.native_id` stays `None` in MCL-4.
//!
//! The gateway checks the Messages.app process state
//! FIRST (pgrep) and never auto-launches it: B2-0 proved
//! osascript auto-launches an app it is told to control.
//! Each `osascript` invocation is capped at 30 seconds
//! by the gateway (the mail_source.rs pattern).

use serde::{Deserialize, Serialize};

use crate::operation_binding::sha256_hex;

/// One thread (chat) as the live source reports it.
/// `native_id` is the chat's AppleScript id (opaque
/// guid string); there is no message content anywhere
/// in this struct — MCL-1 is threads-only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IMessageThreadView {
    pub native_id: String,
    /// Verbatim `service type` of the chat's account:
    /// "iMessage", "SMS", or "RCS" (the dictionary's
    /// third enumerator).
    pub service: String,
    pub participant_count: usize,
    /// sha256 of the canonical JSON of the sorted
    /// per-handle sha256 hexes (Q-MCL-07: hash + count
    /// only, never a handle or a name).
    pub participant_hash: String,
    /// Local definition: more than one participant.
    pub is_group: bool,
}

/// Source failures. Same six variants as `MailError`
/// (the same mapping: `NotFound` -> 404, `Unavailable`
/// -> 501, `Denied` -> 403, `Upstream` / `NotRunning` /
/// `Timeout` -> 502 on the read paths). Separate type so
/// no imessage error ever renders as a mail error.
#[derive(Debug, Clone)]
pub enum IMessageError {
    NotFound(&'static str),
    Unavailable(String),
    Denied,
    Upstream(String),
    NotRunning,
    Timeout,
}

/// Live-query iMessage thread source (no sync cache in
/// MCL-1; the sync writes the fact rows).
pub trait IMessageSource: Send + Sync {
    /// List threads, newest enumeration order first.
    /// `limit`/`offset` apply in Rust over the full
    /// enumeration (threads are metadata-only, so one
    /// script enumerates all chats); the sync passes
    /// `usize::MAX` to take them all.
    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<IMessageThreadView>, IMessageError>;

    /// MCL-4: send one plain-text message to the chat
    /// with this AppleScript id (`native_id` of a synced
    /// `imessage_threads` row). "Sent" means
    /// Messages.app accepted the send, never that the
    /// other side received it (Q-MCL-21). The body is
    /// passed to the script only (never logged); the
    /// returned id is `None` whenever the script cannot
    /// read it back — the normal case in MCL-4, never
    /// fabricated.
    fn send(&self, native_id: &str, body: &str) -> Result<SentIMessage, IMessageError>;
}

/// MCL-4: the outcome of one accepted send. `native_id`
/// is the sent message's Messages id when the script
/// can read it back — `None` in MCL-4 (the send script
/// has no read-back; reading the sent message would need
/// message history, falsified by the MCL-0 probe).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentIMessage {
    pub native_id: Option<String>,
}

/// The participant hash for one thread: sha256 of the
/// canonical JSON of the sorted per-handle sha256 hexes.
/// Order-independent: the same participants in any
/// order hash the same; the handles themselves never
/// leave this function.
pub fn participant_hash_for(handles: &[String]) -> String {
    let mut hashes: Vec<String> = handles
        .iter()
        .map(|h| sha256_hex(h.as_bytes()))
        .collect();
    hashes.sort();
    let json = serde_json::to_string(&hashes).unwrap_or_else(|_| "[]".to_string());
    sha256_hex(json.as_bytes())
}

/// Parse the AppleScript threads dump: one chat per
/// line, fields separated by ASCII 31, in the order id,
/// service type, participant handles (comma-joined).
/// Lines with the wrong field count or an empty id are
/// skipped; empty handles are dropped before the count
/// and the hash. `service` is stored verbatim (even an
/// empty string — never guessed).
pub fn parse_threads_dump(text: &str) -> Vec<IMessageThreadView> {
    let mut views = Vec::new();
    // AppleScript writes `return` as a bare CR on macOS;
    // split both CR and LF so the dump format is stable
    // across the gateway and test platforms (the
    // mail_source.rs `parse_dump` precedent).
    for line in text.split(['\r', '\n']) {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\u{1f}').collect();
        if fields.len() != 3 {
            continue;
        }
        let native_id = fields[0].to_string();
        if native_id.trim().is_empty() {
            continue;
        }
        let service = fields[1].to_string();
        let handles: Vec<String> = fields[2]
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let participant_count = handles.len();
        views.push(IMessageThreadView {
            native_id,
            service,
            participant_hash: participant_hash_for(&handles),
            participant_count,
            is_group: participant_count > 1,
        });
    }
    views
}

/// Clamp a threads-list limit (the mail `clamp_limit`
/// precedent): default 50, maximum 200.
pub fn clamp_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(50).min(200)
}

/// Every call fails: the production source on platforms
/// without a real Messages source. Never fake data.
pub struct UnavailableIMessageSource;

impl IMessageSource for UnavailableIMessageSource {
    fn threads(&self, _limit: usize, _offset: usize) -> Result<Vec<IMessageThreadView>, IMessageError> {
        Err(IMessageError::Unavailable(
            "imessage source unavailable on this platform".to_string(),
        ))
    }

    fn send(&self, _native_id: &str, _body: &str) -> Result<SentIMessage, IMessageError> {
        Err(IMessageError::Unavailable(
            "imessage source unavailable on this platform".to_string(),
        ))
    }
}

/// AppleScript for the threads dump. Plain string
/// construction (no macOS calls) so the guard test can
/// run on every platform. One script enumerates all
/// chats: threads are metadata-only (id, service type,
/// participant handles), so no window is needed — the
/// mail 50-message window exists because bodies are
/// materialized lazily, which does not apply here.
#[cfg(any(target_os = "macos", test))]
fn threads_dump_script() -> String {
    r#"with timeout of 60 seconds
  tell application "Messages"
    set totalCount to count chats
    if totalCount is 0 then
      return ""
    end if
    set out to ""
    repeat with c in chats
      set svc to (service type of account of c as string)
      set handles to ""
      repeat with p in participants of c
        set handles to handles & (handle of p as string) & ","
      end repeat
      set out to out & (id of c as string) & (character id 31) & svc & (character id 31) & handles & return
    end repeat
    return out
  end tell
end timeout"#
        .to_string()
}

/// Escape one value for an AppleScript double-quoted
/// string literal (the `mail_source.rs`
/// `applescript_escape` precedent, copied verbatim):
/// backslash and quote are escaped, and newlines /
/// carriage returns / tabs become their escape
/// sequences (AppleScript interprets `\n`, `\r`, `\t`
/// inside literals). Any other control character
/// becomes a space — it must never reach the script
/// verbatim, where it could break the literal or the
/// `osascript -e` argument.
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

/// AppleScript for sending one plain-text message to
/// one chat (MCL-4, best-effort — the `send` verb is
/// probe-verified, the `chat id` addressing is NOT:
/// window validation on a real Mac is the proof, same
/// as the batch-1 read scripts and the 6b-1 send
/// script). Plain string construction (no macOS calls)
/// so the guard test runs on every platform. The chat
/// is addressed by its AppleScript id — never `first
/// chat`, which would send to whichever chat happens to
/// be first regardless of the requested thread. No
/// read-back: the script returns nothing, so
/// `native_id` stays `None` (reading the sent message
/// would need message history — falsified by the MCL-0
/// probe). Multi-line only: a single-line
/// if-then-else is illegal AppleScript (the MCL-2
/// lesson) — this script has no conditional at all. The
/// body travels inside the `osascript -e` argument
/// only, never into a log or a file.
#[cfg(any(target_os = "macos", test))]
fn send_script(native_id: &str, body: &str) -> String {
    format!(
        r#"with timeout of 30 seconds
  tell application "Messages"
    send "{body}" to chat id "{native_id}"
  end tell
end timeout"#,
        body = applescript_escape(body),
        native_id = applescript_escape(native_id),
    )
}

/// Hard cap on one `osascript` invocation (the
/// mail_source.rs pattern). Expiry maps to `Timeout`.
#[cfg(target_os = "macos")]
const SCRIPT_TIMEOUT_SECS: u64 = 30;

/// Uniqueness for the temp files `run_script` captures
/// output in (gateway requests run concurrently).
#[cfg(target_os = "macos")]
static SCRIPT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(target_os = "macos")]
use std::{
    fs,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// macOS-only real source: asks Messages.app via
/// `osascript`. See the module docs for the best-effort
/// caveat: the script form below is validated by window
/// validation on a real Mac, not by this VM.
#[cfg(target_os = "macos")]
pub struct AppleScriptIMessageSource;

#[cfg(target_os = "macos")]
impl AppleScriptIMessageSource {
    pub fn new() -> Self {
        Self
    }

    /// Is Messages.app running? Uses `pgrep` (no
    /// permission needed) so a read never launches
    /// Messages as a side effect. If `pgrep` itself is
    /// missing, proceed and let the script report the
    /// real error (the mail_source.rs precedent).
    fn messages_running() -> bool {
        match Command::new("pgrep").args(["-x", "Messages"]).output() {
            Ok(output) => output.status.success(),
            Err(_) => true,
        }
    }

    /// Run one AppleScript program, capped at
    /// `SCRIPT_TIMEOUT_SECS` (the mail_source.rs
    /// `run_script` precedent: temp files, not pipes;
    /// automation denial -> `Denied`; missing app ->
    /// `NotRunning`; timeout -> `Timeout`; everything
    /// else -> generic `Upstream`). Error strings stay
    /// generic on purpose: automation stderr can contain
    /// local paths.
    fn run_script(script: &str) -> Result<String, IMessageError> {
        if !Self::messages_running() {
            return Err(IMessageError::NotRunning);
        }
        let n = SCRIPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let stem = format!("bu2agent-imessage-script-{}-{n}", std::process::id());
        let out_path = std::env::temp_dir().join(format!("{stem}.out"));
        let err_path = std::env::temp_dir().join(format!("{stem}.err"));
        let out_file = fs::File::create(&out_path).map_err(|_| {
            IMessageError::Upstream("could not capture Messages script output".to_string())
        })?;
        let err_file = fs::File::create(&err_path).map_err(|_| {
            IMessageError::Upstream("could not capture Messages script output".to_string())
        })?;
        let mut child = Command::new("osascript")
            .arg("-e")
            .arg(script)
            .stdout(Stdio::from(out_file))
            .stderr(Stdio::from(err_file))
            .spawn()
            .map_err(|_| IMessageError::Upstream("osascript unavailable".to_string()))?;
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
                        return Err(IMessageError::Denied);
                    }
                    return Err(IMessageError::Upstream("Messages automation failed".to_string()));
                }
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        let _ = fs::remove_file(&out_path);
                        let _ = fs::remove_file(&err_path);
                        return Err(IMessageError::Timeout);
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = fs::remove_file(&out_path);
                    let _ = fs::remove_file(&err_path);
                    return Err(IMessageError::Upstream("Messages automation failed".to_string()));
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl Default for AppleScriptIMessageSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_os = "macos")]
impl IMessageSource for AppleScriptIMessageSource {
    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<IMessageThreadView>, IMessageError> {
        let dump = Self::run_script(&threads_dump_script())?;
        let views = parse_threads_dump(&dump);
        Ok(views.into_iter().skip(offset).take(limit).collect())
    }

    fn send(&self, native_id: &str, body: &str) -> Result<SentIMessage, IMessageError> {
        // MCL-4: the send form is best-effort (see
        // `send_script`); window validation on a real Mac
        // is the proof. `run_script` already gates on
        // Messages.app running (Q-MCL-21 / 6b Q-07: never
        // launch Messages.app for a send) and maps the
        // 30 s kill to `Timeout`. The script returns
        // nothing, so native_id stays None — never
        // fabricated.
        let script = send_script(native_id, body);
        let _ = Self::run_script(&script)?;
        Ok(SentIMessage { native_id: None })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_script_enumerates_threads_without_reading_messages() {
        // MCL-1 is threads-only: the dump must enumerate
        // chats + participants and must NEVER ask for
        // message history (the dictionary has no message
        // element; a real-session read errors -1700).
        // This guard runs on every platform because the
        // builder is plain string construction.
        let script = threads_dump_script();
        assert!(script.contains("count chats"), "must enumerate chats");
        assert!(
            script.contains("service type of account of"),
            "must read the service type"
        );
        assert!(
            script.contains("participants of"),
            "must enumerate participants"
        );
        assert!(script.contains("handle of"), "must read handles");
        assert!(
            !script.contains("messages of"),
            "must never read message history"
        );
        assert!(
            !script.contains("content of"),
            "must never read message content"
        );
    }

    #[test]
    fn send_script_targets_one_chat_and_never_reads_messages() {
        // MCL-4 is send-only: the script must address
        // the requested chat by id and must NEVER ask
        // for message history (the dictionary has no
        // message element; a real-session read errors
        // -1700). It must not use `first chat` — that
        // would send to whichever chat happens to be
        // first, regardless of the requested thread.
        // This guard runs on every platform because the
        // builder is plain string construction.
        let script = send_script("any;-;+15550101001", "hello");
        assert!(
            script.contains("send \"hello\" to chat id \"any;-;+15550101001\""),
            "must send to the requested chat id: {script}"
        );
        assert!(
            !script.contains("first chat"),
            "must never send to first chat: {script}"
        );
        assert!(
            !script.contains("messages of"),
            "must never read message history"
        );
        assert!(
            !script.contains("content of"),
            "must never read message content"
        );
        // Multi-line only: a single-line if-then-else
        // is illegal AppleScript (the MCL-2 lesson) —
        // this script has no conditional at all.
        assert!(!script.contains("if "), "send script has no conditional");
        // The body is escaped into the literal, never
        // interpolated raw: a quote cannot break out.
        let tricky = send_script("chat-1", "say \"hi\"\nbye");
        assert!(
            tricky.contains("say \\\"hi\\\"\\nbye"),
            "body must be escaped: {tricky}"
        );
    }

    #[test]
    fn participant_hash_is_order_independent_and_hides_handles() {
        let a = participant_hash_for(&[
            "+15551234567".to_string(),
            "friend@example.invalid".to_string(),
        ]);
        let b = participant_hash_for(&[
            "friend@example.invalid".to_string(),
            "+15551234567".to_string(),
        ]);
        assert_eq!(a, b, "same participants must hash the same");
        assert_eq!(a.len(), 64, "sha256 hex");
        assert!(!a.contains("+15551234567"), "no handle in the hash");
        assert!(
            !a.contains("friend@example.invalid"),
            "no handle in the hash"
        );
        let single = participant_hash_for(&["+15551234567".to_string()]);
        assert_ne!(a, single, "different sets must hash differently");
    }

    #[test]
    fn dump_parsing_reads_threads_and_marks_groups() {
        let dump = "chat-1\u{1f}iMessage\u{1f}+15551234567,\nchat-2\u{1f}SMS\u{1f}+15550001111,other@example.invalid,\nbad line\nchat-3\u{1f}iMessage\u{1f}\n";
        let views = parse_threads_dump(dump);
        assert_eq!(views.len(), 3);
        assert_eq!(views[0].native_id, "chat-1");
        assert_eq!(views[0].service, "iMessage");
        assert_eq!(views[0].participant_count, 1);
        assert!(!views[0].is_group);
        assert_eq!(views[1].participant_count, 2);
        assert!(views[1].is_group);
        // No participants: count 0, not a group, hash of
        // the empty set.
        assert_eq!(views[2].participant_count, 0);
        assert!(!views[2].is_group);
        assert_eq!(
            views[2].participant_hash,
            participant_hash_for(&[]),
            "empty set hashes deterministically"
        );
    }

    #[test]
    fn dump_parsing_accepts_macos_carriage_returns() {
        let dump = "chat-1\u{1f}iMessage\u{1f}+15551234567,\rchat-2\u{1f}SMS\u{1f}+15550001111,\r";
        let views = parse_threads_dump(dump);
        assert_eq!(views.len(), 2);
        assert_eq!(views[1].service, "SMS");
    }

    #[test]
    fn dump_parsing_skips_malformed_lines() {
        let dump = "chat-1\u{1f}iMessage\n\u{1f}iMessage\u{1f}+15551234567,\nchat-2\u{1f}iMessage\u{1f}+15550001111,\n";
        let views = parse_threads_dump(dump);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].native_id, "chat-2");
    }
}
