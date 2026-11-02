//! Email read surface, batch 1 (Task 22, option A).
//!
//! Batch 1 is read-only: mailbox, mailbox/setup, threads
//! list/get, message get, attachment get. No incremental
//! sync cache: every call queries the source live (plan
//! S6). No approval loop: these are reads.
//!
//! The source is a trait so the mechanism can change
//! without touching routes, clients, or UI:
//!
//! - `MockMailSource`: seeded [Mock] threads/messages for
//!   VM tests and smoke tests. Never used by the packaged
//!   app: production on non-macOS is `UnavailableMailSource`
//!   (501, never fake data — plan S8).
//! - `AppleScriptMailSource` (macOS only): asks Mail.app
//!   via `osascript`. This is the batch-1 realization of
//!   option A, NOT the direct Envelope Index read the
//!   plan originally described: the Envelope Index schema
//!   is unverified, and a guessed schema must not be
//!   shipped. AppleScript is the product's own Automation
//!   approach (MailSender's mechanism), and window
//!   validation on a real Mac is the moment of truth for
//!   the script forms below — message addressing, the
//!   attachment `save` form, and the date string format
//!   are all best-effort and will be corrected from real
//!   Mac errors. A future direct-DB reader needs the
//!   Envelope Index schema captured from a real Mac first
//!   (see the Task 22 plan §7).
//! - `UnavailableMailSource`: every call fails with
//!   `MailError::Unavailable` (plan S8).
//!
//! Threading is a local definition: Mail.app's AppleScript
//! dictionary exposes no thread id, so messages are
//! grouped by normalized subject (leading Re:/Fwd:
//! prefixes stripped, case-insensitive) and the thread id
//! is a stable hash of that normalized subject. The
//! message date is a display string (AppleScript's date
//! string is locale-dependent; it is passed through, not
//! normalized to milliseconds).
//!
//! Batch-1 window (option A, approved 2026-11-03): every
//! AppleScript loop scans at most the first 200 messages
//! of the inbox, in Mail's inbox order, and the metadata
//! dump carries each message's snippet, so threads/thread
//! need exactly one script call. Whether Mail's inbox
//! order is newest-first or oldest-first is unverified;
//! window re-validation checks the returned dates and
//! flips the range if it is wrong. Messages outside the
//! window read as 404. Each `osascript` invocation is
//! capped at 30 seconds by the gateway (the AppleScript
//! `with timeout` values bound single Apple events, not
//! a whole loop, so they cannot bound a call); expiry is
//! `Upstream` (502), never a hang.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Mailbox metadata (plan S1: unconfigured is a normal
/// state, reported as `configured: false`, not a 404).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailboxInfo {
    pub configured: bool,
    pub address: Option<String>,
    pub name: Option<String>,
    pub source: String,
}

/// Setup status and guidance (plan S2: descriptive only,
/// it performs no configuration).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetupInfo {
    pub configured: bool,
    pub source: String,
    pub steps: Vec<String>,
}

/// One thread: a subject-group of messages (local
/// definition, see the module docs).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadView {
    pub thread_id: String,
    pub subject: String,
    pub message_ids: Vec<String>,
    pub message_count: usize,
    pub snippet: String,
}

/// One message. `date` is a display string (see the
/// module docs). `body_text` is plain text only (plan
/// S4); HTML is not expanded in batch 1.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageView {
    pub message_id: String,
    pub from: String,
    pub to: Vec<String>,
    pub date: String,
    pub subject: String,
    pub body_text: String,
    pub attachments: Vec<AttachmentMeta>,
}

/// Attachment metadata. `index` is the position used by
/// the attachment route (plan S5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttachmentMeta {
    pub index: usize,
    pub name: String,
    pub content_type: String,
    pub size: usize,
}

/// Downloaded attachment bytes.
#[derive(Debug, Clone)]
pub struct AttachmentData {
    pub bytes: Vec<u8>,
    pub content_type: String,
    pub filename: String,
}

/// Source failures. Handlers map these to statuses:
/// `NotFound` -> 404, `Unavailable` -> 501, `Denied` ->
/// 403, `Upstream` -> 502. Error strings are generic on
/// purpose: automation stderr can contain local paths.
#[derive(Debug, Clone)]
pub enum MailError {
    NotFound(&'static str),
    Unavailable(String),
    Denied,
    Upstream(String),
}

/// Live-query email source (no sync cache in batch 1).
pub trait MailSource: Send + Sync {
    fn mailbox(&self) -> Result<MailboxInfo, MailError>;
    fn setup(&self) -> Result<SetupInfo, MailError>;
    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<ThreadView>, MailError>;
    fn thread(&self, thread_id: &str) -> Result<ThreadView, MailError>;
    fn message(&self, message_id: &str) -> Result<MessageView, MailError>;
    fn attachment(&self, message_id: &str, index: usize) -> Result<AttachmentData, MailError>;
}

/// One message's metadata, as dumped by the AppleScript
/// list call (single-line fields; see `parse_dump`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawMessageMeta {
    pub id: String,
    pub subject: String,
    pub sender: String,
    pub recipients: Vec<String>,
    pub date: String,
}

/// Strip leading Re:/Fwd:/Fw: prefixes (case-insensitive)
/// and surrounding whitespace, repeatedly.
pub fn normalize_subject(subject: &str) -> String {
    let mut current = subject.trim();
    loop {
        let lower = current.to_lowercase();
        let mut stripped = None;
        for prefix in ["re:", "fwd:", "fw:"] {
            if let Some(rest) = lower.strip_prefix(prefix) {
                stripped = Some(&current[prefix.len()..]);
                let _ = rest;
                break;
            }
        }
        match stripped {
            Some(rest) => current = rest.trim(),
            None => return current.to_string(),
        }
    }
}

/// Stable thread id for a normalized subject (FNV-1a).
pub fn thread_id_for(normalized_subject: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in normalized_subject.to_lowercase().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("thread-{hash:016x}")
}

/// Group metadata into threads, preserving first-seen
/// order. The thread subject is the first message's
/// original subject; the snippet is the first message's
/// body, truncated to 80 chars. Grouping takes a lookup
/// for snippets: the mock passes full bodies, the
/// AppleScript source passes the snippets carried by its
/// metadata dump.
pub fn group_threads(
    metas: &[RawMessageMeta],
    bodies: &HashMap<String, String>,
) -> Vec<ThreadView> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, Vec<&RawMessageMeta>> = HashMap::new();
    for meta in metas {
        let thread_id = thread_id_for(&normalize_subject(&meta.subject));
        if !groups.contains_key(&thread_id) {
            order.push(thread_id.clone());
        }
        groups.entry(thread_id).or_default().push(meta);
    }
    order
        .into_iter()
        .filter_map(|thread_id| {
            let messages = groups.get(&thread_id)?;
            let first = messages.first()?;
            let body = bodies.get(&first.id).cloned().unwrap_or_default();
            let snippet: String = body.chars().take(80).collect();
            Some(ThreadView {
                thread_id,
                subject: first.subject.clone(),
                message_ids: messages.iter().map(|m| m.id.clone()).collect(),
                message_count: messages.len(),
                snippet,
            })
        })
        .collect()
}

/// Parse the AppleScript metadata dump: one message per
/// line, fields separated by ASCII 31, in the order id,
/// subject, sender, recipients (comma-joined), date, and
/// optionally a snippet (first 80 chars of the body).
/// Subjects, senders and snippets are sanitized by the
/// script (no tabs, newlines, or ASCII 31); malformed
/// lines are skipped. Returns the metadata plus a
/// snippet lookup by message id (5-field lines carry no
/// snippet and are absent from the map).
pub fn parse_dump(text: &str) -> (Vec<RawMessageMeta>, HashMap<String, String>) {
    let mut metas = Vec::new();
    let mut snippets = HashMap::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\u{1f}').collect();
        if fields.len() != 5 && fields.len() != 6 {
            continue;
        }
        let recipients = fields[3]
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if fields.len() == 6 {
            snippets.insert(fields[0].to_string(), fields[5].to_string());
        }
        metas.push(RawMessageMeta {
            id: fields[0].to_string(),
            subject: fields[1].to_string(),
            sender: fields[2].to_string(),
            recipients,
            date: fields[4].to_string(),
        });
    }
    (metas, snippets)
}

/// Clamp a threads-list limit (plan S3): default 50,
/// maximum 200.
pub fn clamp_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(50).min(200)
}

/// Seeded [Mock] data for tests. Thread 1 has two
/// messages (the second carries one attachment); thread
/// 2 has one message.
pub fn mock_messages() -> Vec<MessageView> {
    vec![
        MessageView {
            message_id: "msg-seed-1".to_string(),
            from: "alice@mock.invalid".to_string(),
            to: vec!["me@mock.invalid".to_string()],
            date: "2026-11-01T10:00:00Z".to_string(),
            subject: "Mock Thread Alpha".to_string(),
            body_text: "Hello Alpha, this is the first mock message.".to_string(),
            attachments: vec![],
        },
        MessageView {
            message_id: "msg-seed-2".to_string(),
            from: "bob@mock.invalid".to_string(),
            to: vec!["me@mock.invalid".to_string()],
            date: "2026-11-01T11:00:00Z".to_string(),
            subject: "Re: Mock Thread Alpha".to_string(),
            body_text: "Replying to Alpha with one attachment.".to_string(),
            attachments: vec![AttachmentMeta {
                index: 0,
                name: "notes.txt".to_string(),
                content_type: "text/plain".to_string(),
                size: 17,
            }],
        },
        MessageView {
            message_id: "msg-seed-3".to_string(),
            from: "carol@mock.invalid".to_string(),
            to: vec!["me@mock.invalid".to_string()],
            date: "2026-11-02T09:00:00Z".to_string(),
            subject: "Mock Thread Beta".to_string(),
            body_text: "Hello Beta, a second mock thread.".to_string(),
            attachments: vec![],
        },
    ]
}

/// Mock source: seeds only, used by VM tests and smoke
/// tests. Never used by the packaged app.
pub struct MockMailSource;

impl MockMailSource {
    fn all_threads(&self) -> Vec<ThreadView> {
        let messages = mock_messages();
        let metas: Vec<RawMessageMeta> = messages
            .iter()
            .map(|m| RawMessageMeta {
                id: m.message_id.clone(),
                subject: m.subject.clone(),
                sender: m.from.clone(),
                recipients: m.to.clone(),
                date: m.date.clone(),
            })
            .collect();
        let bodies: HashMap<String, String> = messages
            .iter()
            .map(|m| (m.message_id.clone(), m.body_text.clone()))
            .collect();
        group_threads(&metas, &bodies)
    }
}

impl MailSource for MockMailSource {
    fn mailbox(&self) -> Result<MailboxInfo, MailError> {
        Ok(MailboxInfo {
            configured: true,
            address: Some("mock@example.invalid".to_string()),
            name: Some("Mock Mailbox".to_string()),
            source: "mock".to_string(),
        })
    }

    fn setup(&self) -> Result<SetupInfo, MailError> {
        Ok(SetupInfo {
            configured: true,
            source: "mock".to_string(),
            steps: vec!["This is the mock source used by tests; no setup applies.".to_string()],
        })
    }

    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<ThreadView>, MailError> {
        let all = self.all_threads();
        Ok(all.into_iter().skip(offset).take(limit).collect())
    }

    fn thread(&self, thread_id: &str) -> Result<ThreadView, MailError> {
        self.all_threads()
            .into_iter()
            .find(|t| t.thread_id == thread_id)
            .ok_or(MailError::NotFound("thread"))
    }

    fn message(&self, message_id: &str) -> Result<MessageView, MailError> {
        mock_messages()
            .into_iter()
            .find(|m| m.message_id == message_id)
            .ok_or(MailError::NotFound("message"))
    }

    fn attachment(&self, message_id: &str, index: usize) -> Result<AttachmentData, MailError> {
        if message_id == "msg-seed-2" && index == 0 {
            return Ok(AttachmentData {
                bytes: b"hello attachment\n".to_vec(),
                content_type: "text/plain".to_string(),
                filename: "notes.txt".to_string(),
            });
        }
        Err(MailError::NotFound("attachment"))
    }
}

/// Every call fails: the production source on platforms
/// without a real Mail source (plan S8). Never fake data.
pub struct UnavailableMailSource;

impl MailSource for UnavailableMailSource {
    fn mailbox(&self) -> Result<MailboxInfo, MailError> {
        Err(MailError::Unavailable("mail source unavailable on this platform".to_string()))
    }

    fn setup(&self) -> Result<SetupInfo, MailError> {
        Err(MailError::Unavailable("mail source unavailable on this platform".to_string()))
    }

    fn threads(&self, _limit: usize, _offset: usize) -> Result<Vec<ThreadView>, MailError> {
        Err(MailError::Unavailable("mail source unavailable on this platform".to_string()))
    }

    fn thread(&self, _thread_id: &str) -> Result<ThreadView, MailError> {
        Err(MailError::Unavailable("mail source unavailable on this platform".to_string()))
    }

    fn message(&self, _message_id: &str) -> Result<MessageView, MailError> {
        Err(MailError::Unavailable("mail source unavailable on this platform".to_string()))
    }

    fn attachment(&self, _message_id: &str, _index: usize) -> Result<AttachmentData, MailError> {
        Err(MailError::Unavailable("mail source unavailable on this platform".to_string()))
    }
}

#[cfg(target_os = "macos")]
fn content_type_for(filename: &str) -> String {
    let lower = filename.to_lowercase();
    let ext = lower.rsplit('.').next().unwrap_or("");
    match ext {
        "txt" | "md" => "text/plain".to_string(),
        "html" | "htm" => "text/html".to_string(),
        "pdf" => "application/pdf".to_string(),
        "png" => "image/png".to_string(),
        "jpg" | "jpeg" => "image/jpeg".to_string(),
        "gif" => "image/gif".to_string(),
        "json" => "application/json".to_string(),
        _ => "application/octet-stream".to_string(),
    }
}

/// macOS-only real source: asks Mail.app via `osascript`.
/// See the module docs for the best-effort caveat: every
/// script form below is validated by window validation
/// on a real Mac, not by this VM.
#[cfg(target_os = "macos")]
pub struct AppleScriptMailSource;

#[cfg(target_os = "macos")]
use std::{
    fs,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// Batch-1 read window (see the module docs): every
/// AppleScript loop scans at most this many messages of
/// the inbox, in Mail's inbox order.
#[cfg(target_os = "macos")]
const INBOX_WINDOW: usize = 200;

/// Hard cap on one `osascript` invocation (see the
/// module docs). Expiry maps to `Upstream` (502).
#[cfg(target_os = "macos")]
const SCRIPT_TIMEOUT_SECS: u64 = 30;

/// Uniqueness for the temp files `run_script` captures
/// output in (gateway requests run concurrently).
#[cfg(target_os = "macos")]
static SCRIPT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(target_os = "macos")]
impl AppleScriptMailSource {
    pub fn new() -> Self {
        Self
    }

    /// Is Mail.app running? Uses `pgrep` (no permission
    /// needed) so a read never launches Mail as a side
    /// effect. If `pgrep` itself is missing, proceed and
    /// let the script report the real error.
    fn mail_running() -> bool {
        match Command::new("pgrep").args(["-x", "Mail"]).output() {
            Ok(output) => output.status.success(),
            Err(_) => true,
        }
    }

    /// Run one AppleScript program, capped at
    /// `SCRIPT_TIMEOUT_SECS`. Maps automation denial to
    /// `Denied` (the spec's MailSender detects exactly
    /// this), a missing Mail to `Upstream`, a timeout to
    /// `Upstream`, and everything else to a generic
    /// `Upstream`. Output goes to temp files, not pipes:
    /// a large dump could fill a pipe buffer while the
    /// parent polls, deadlocking the wait; temp files
    /// also make the timeout kill clean (no leaked
    /// process, temp files removed either way). Error
    /// strings stay generic on purpose: automation
    /// stderr can contain local paths.
    fn run_script(script: &str) -> Result<String, MailError> {
        if !Self::mail_running() {
            return Err(MailError::Upstream("Mail is not running".to_string()));
        }
        let n = SCRIPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let stem = format!("bu2agent-mail-script-{}-{n}", std::process::id());
        let out_path = std::env::temp_dir().join(format!("{stem}.out"));
        let err_path = std::env::temp_dir().join(format!("{stem}.err"));
        let out_file = fs::File::create(&out_path)
            .map_err(|_| MailError::Upstream("could not capture Mail script output".to_string()))?;
        let err_file = fs::File::create(&err_path)
            .map_err(|_| MailError::Upstream("could not capture Mail script output".to_string()))?;
        let mut child = Command::new("osascript")
            .arg("-e")
            .arg(script)
            .stdout(Stdio::from(out_file))
            .stderr(Stdio::from(err_file))
            .spawn()
            .map_err(|_| MailError::Upstream("osascript unavailable".to_string()))?;
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
                        return Err(MailError::Denied);
                    }
                    return Err(MailError::Upstream("Mail automation failed".to_string()));
                }
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        let _ = fs::remove_file(&out_path);
                        let _ = fs::remove_file(&err_path);
                        return Err(MailError::Upstream("Mail script timed out".to_string()));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = fs::remove_file(&out_path);
                    let _ = fs::remove_file(&err_path);
                    return Err(MailError::Upstream("Mail automation failed".to_string()));
                }
            }
        }
    }

    fn inbox_metadata(&self) -> Result<(Vec<RawMessageMeta>, HashMap<String, String>), MailError> {
        // One line per message; fields separated by
        // character id 31: id, subject, sender, recipients
        // (comma-joined), date, snippet (first 80 chars
        // of the body, sanitized like subject/sender).
        // The loop scans at most INBOX_WINDOW messages:
        // it enumerates `messages of inbox` (the form
        // the pre-window script already proved) and
        // exits once the counter passes the window, so
        // a huge inbox never pays a full scan and no
        // range form is needed at all. Carrying the
        // snippet here is what lets threads/thread
        // answer with one script call instead of N+1.
        // `with timeout` bounds single Apple events only;
        // the process cap in run_script bounds the call.
        // AppleScript if grammar note: a single-line
        // `if cond then statement` is self-contained
        // and must NOT be followed by `end if`; every
        // if below is therefore written multi-line
        // (see the U2 502 fix: the v1 window prelude
        // used the single-line form plus `end if`,
        // which fails osascript compilation outright).
        let raw_script = r#"on cleanText(t)
  set AppleScript's text item delimiters to {tab, return, linefeed}
  set parts to text items of t
  set AppleScript's text item delimiters to " "
  return parts as string
end cleanText
with timeout of 60 seconds
  tell application "Mail"
    set out to ""
    set idx to 0
    repeat with m in messages of inbox
      set idx to idx + 1
      if idx > __WINDOW__ then
        exit repeat
      end if
      set addrs to ""
      repeat with r in to recipients of m
        set addrs to addrs & address of r & ","
      end repeat
      set fullContent to content of m
      if (count of fullContent) > 80 then
        set snip to text 1 thru 80 of fullContent
      else
        set snip to fullContent
      end if
      set out to out & (id of m as string) & (character id 31) & my cleanText(subject of m) & (character id 31) & my cleanText(sender of m) & (character id 31) & addrs & (character id 31) & (date received of m as string) & (character id 31) & my cleanText(snip) & return
    end repeat
    return out
  end tell
end timeout"#;
        let script = raw_script.replace("__WINDOW__", &INBOX_WINDOW.to_string());
        let dump = Self::run_script(&script)?;
        Ok(parse_dump(&dump))
    }

    fn find_meta(&self, message_id: &str) -> Result<RawMessageMeta, MailError> {
        // Mail ids are integers; a non-numeric id can
        // never match, and must not reach the script.
        if message_id.parse::<i64>().is_err() {
            return Err(MailError::NotFound("message"));
        }
        self.inbox_metadata()?
            .0
            .into_iter()
            .find(|m| m.id == message_id)
            .ok_or(MailError::NotFound("message"))
    }

    /// Attachment names for one message, via a windowed
    /// match-by-id loop (shared by `message` and
    /// `attachment` so neither re-reads the full inbox).
    fn attachment_names(&self, message_id: &str) -> Result<Vec<String>, MailError> {
        let script = format!(
            r#"with timeout of 60 seconds
  tell application "Mail"
    set idx to 0
    repeat with m in messages of inbox
      set idx to idx + 1
      if idx > __WINDOW__ then
        exit repeat
      end if
      if (id of m as string) = "{message_id}" then
        set out to ""
        repeat with a in mail attachments of m
          set out to out & (name of a as string) & return
        end repeat
        return out
      end if
    end repeat
    return ""
  end tell
end timeout"#,
            message_id = message_id
        );
        let script = script.replace("__WINDOW__", &INBOX_WINDOW.to_string());
        let names = Self::run_script(&script)?;
        Ok(names
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(str::to_string)
            .collect())
    }
}

#[cfg(target_os = "macos")]
impl Default for AppleScriptMailSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_os = "macos")]
impl MailSource for AppleScriptMailSource {
    fn mailbox(&self) -> Result<MailboxInfo, MailError> {
        if !Self::mail_running() {
            return Err(MailError::Upstream("Mail is not running".to_string()));
        }
        let script = r#"with timeout of 30 seconds
  tell application "Mail"
    set addrs to email addresses of first account
    set nm to user name of first account
    return (nm & (character id 31) & (first item of addrs as string))
  end tell
end timeout"#;
        match Self::run_script(script) {
            Ok(out) => {
                let mut parts = out.trim().split('\u{1f}');
                let name = parts.next().unwrap_or("").to_string();
                let address = parts.next().unwrap_or("").to_string();
                if address.is_empty() {
                    return Ok(MailboxInfo {
                        configured: false,
                        address: None,
                        name: None,
                        source: "applescript".to_string(),
                    });
                }
                Ok(MailboxInfo {
                    configured: true,
                    address: Some(address),
                    name: if name.is_empty() { None } else { Some(name) },
                    source: "applescript".to_string(),
                })
            }
            Err(MailError::Upstream(_)) => Ok(MailboxInfo {
                configured: false,
                address: None,
                name: None,
                source: "applescript".to_string(),
            }),
            Err(other) => Err(other),
        }
    }

    fn setup(&self) -> Result<SetupInfo, MailError> {
        let configured = self.mailbox()?.configured;
        Ok(SetupInfo {
            configured,
            source: "applescript".to_string(),
            steps: vec![
                "Open Mail.app and add an account in Mail settings.".to_string(),
                "Allow automation when macOS prompts for Mail access.".to_string(),
            ],
        })
    }

    fn threads(&self, limit: usize, offset: usize) -> Result<Vec<ThreadView>, MailError> {
        // One script total: the metadata dump already
        // carries each message's snippet, so grouping
        // needs no per-thread content fetch (the old N+1
        // is gone). Limit/offset still apply in Rust,
        // now over the windowed list.
        let (metas, snippets) = self.inbox_metadata()?;
        let grouped = group_threads(&metas, &snippets);
        Ok(grouped.into_iter().skip(offset).take(limit).collect())
    }

    fn thread(&self, thread_id: &str) -> Result<ThreadView, MailError> {
        let (metas, snippets) = self.inbox_metadata()?;
        group_threads(&metas, &snippets)
            .into_iter()
            .find(|t| t.thread_id == thread_id)
            .ok_or(MailError::NotFound("thread"))
    }

    fn message(&self, message_id: &str) -> Result<MessageView, MailError> {
        let meta = self.find_meta(message_id)?;
        // Loop-and-match avoids depending on Mail's
        // `message id` addressing form; a sentinel marks
        // "not found" (content is returned whole, so
        // multiline bodies survive intact). The loop is
        // windowed like every other loop here.
        let script = format!(
            r#"with timeout of 60 seconds
  tell application "Mail"
    set idx to 0
    repeat with m in messages of inbox
      set idx to idx + 1
      if idx > __WINDOW__ then
        exit repeat
      end if
      if (id of m as string) = "{message_id}" then
        return content of m
      end if
    end repeat
    return "«BU2AGENT_MESSAGE_NOT_FOUND»"
  end tell
end timeout"#,
            message_id = message_id
        );
        let script = script.replace("__WINDOW__", &INBOX_WINDOW.to_string());
        let body = Self::run_script(&script)?;
        let body = body.strip_suffix('\n').unwrap_or(&body).to_string();
        if body.trim() == "«BU2AGENT_MESSAGE_NOT_FOUND»" {
            return Err(MailError::NotFound("message"));
        }
        // Attachment metadata for this message.
        let names = self.attachment_names(message_id)?;
        let attachments = names
            .into_iter()
            .enumerate()
            .map(|(index, name)| AttachmentMeta {
                index,
                name: name.clone(),
                content_type: content_type_for(&name),
                size: 0,
            })
            .collect();
        Ok(MessageView {
            message_id: meta.id,
            from: meta.sender,
            to: meta.recipients,
            date: meta.date,
            subject: meta.subject,
            body_text: body,
            attachments,
        })
    }

    fn attachment(&self, message_id: &str, index: usize) -> Result<AttachmentData, MailError> {
        let meta = self.find_meta(message_id)?;
        let _ = meta;
        // Save the attachment to a temp file, read it
        // back, and delete it. The temp name carries only
        // the process id and numeric ids (no user input).
        let path = std::env::temp_dir().join(format!(
            "bu2agent-mail-attachment-{}-{message_id}-{index}",
            std::process::id()
        ));
        let path_str = path.to_string_lossy().to_string();
        let script = format!(
            r#"with timeout of 60 seconds
  tell application "Mail"
    set idx to 0
    repeat with m in messages of inbox
      set idx to idx + 1
      if idx > __WINDOW__ then
        exit repeat
      end if
      if (id of m as string) = "{message_id}" then
        set att to item {item} of (get mail attachments of m)
        save att in file "{path_str}"
        return "ok"
      end if
    end repeat
    return "«BU2AGENT_MESSAGE_NOT_FOUND»"
  end tell
end timeout"#,
            message_id = message_id,
            item = index + 1,
            path_str = path_str
        );
        let script = script.replace("__WINDOW__", &INBOX_WINDOW.to_string());
        match Self::run_script(&script) {
            Ok(out) if out.trim() == "ok" => {
                let bytes = std::fs::read(&path).map_err(|_| {
                    MailError::Upstream("could not read saved attachment".to_string())
                })?;
                let _ = std::fs::remove_file(&path);
                // Names only (not a full message read):
                // a names-script failure still falls back
                // to a synthetic filename, as before.
                let filename = self
                    .attachment_names(message_id)
                    .ok()
                    .and_then(|names| names.into_iter().nth(index))
                    .unwrap_or_else(|| format!("attachment-{index}"));
                Ok(AttachmentData {
                    content_type: content_type_for(&filename),
                    filename,
                    bytes,
                })
            }
            Ok(_) => Err(MailError::NotFound("attachment")),
            Err(MailError::Upstream(_)) => Err(MailError::NotFound("attachment")),
            Err(other) => Err(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_normalization_strips_reply_prefixes() {
        assert_eq!(normalize_subject("Re: Fwd: Hello"), "Hello");
        assert_eq!(normalize_subject("  RE:  Hello  "), "Hello");
        assert_eq!(normalize_subject("Hello"), "Hello");
        assert_eq!(normalize_subject("Re:Re: Hello"), "Hello");
    }

    #[test]
    fn thread_ids_are_stable_for_replies() {
        let a = thread_id_for(&normalize_subject("Mock Thread Alpha"));
        let b = thread_id_for(&normalize_subject("Re: Mock Thread Alpha"));
        assert_eq!(a, b);
        assert!(a.starts_with("thread-"));
    }

    #[test]
    fn dump_parsing_skips_malformed_lines() {
        let dump = "1\u{1f}Hello\u{1f}alice@x\u{1f}me@x\u{1f}2026-01-01\nbad line\n2\u{1f}Hi\u{1f}bob@x\u{1f}me@x,other@x\u{1f}2026-01-02";
        let (metas, snippets) = parse_dump(dump);
        assert_eq!(metas.len(), 2);
        assert_eq!(metas[1].recipients, vec!["me@x", "other@x"]);
        // 5-field lines carry no snippet.
        assert!(snippets.is_empty());
    }

    #[test]
    fn dump_parsing_reads_snippets() {
        let dump = "1\u{1f}Hello\u{1f}alice@x\u{1f}me@x\u{1f}2026-01-01\u{1f}Hello body\nbad line\n2\u{1f}Hi\u{1f}bob@x\u{1f}me@x\u{1f}2026-01-02\u{1f}Hi body";
        let (metas, snippets) = parse_dump(dump);
        assert_eq!(metas.len(), 2);
        assert_eq!(snippets.get("1").map(String::as_str), Some("Hello body"));
        assert_eq!(snippets.get("2").map(String::as_str), Some("Hi body"));
    }

    #[test]
    fn group_threads_uses_snippet_map() {
        let metas = vec![
            RawMessageMeta {
                id: "1".to_string(),
                subject: "Hello".to_string(),
                sender: "alice@x".to_string(),
                recipients: vec!["me@x".to_string()],
                date: "2026-01-01".to_string(),
            },
            RawMessageMeta {
                id: "2".to_string(),
                subject: "Re: Hello".to_string(),
                sender: "bob@x".to_string(),
                recipients: vec!["me@x".to_string()],
                date: "2026-01-02".to_string(),
            },
        ];
        let snippets: HashMap<String, String> =
            [("1".to_string(), "x".repeat(100))].into_iter().collect();
        let threads = group_threads(&metas, &snippets);
        assert_eq!(threads.len(), 1);
        assert_eq!(threads[0].message_count, 2);
        // Snippet is the first message's snippet, cut to 80 chars.
        assert_eq!(threads[0].snippet.len(), 80);
        assert!(threads[0].snippet.chars().all(|c| c == 'x'));
    }

    #[test]
    fn mock_groups_replies_into_one_thread() {
        let source = MockMailSource;
        let threads = source.threads(50, 0).expect("threads");
        assert_eq!(threads.len(), 2);
        assert_eq!(threads[0].message_count, 2);
        assert_eq!(threads[0].message_ids, vec!["msg-seed-1", "msg-seed-2"]);
        assert_eq!(threads[1].message_ids, vec!["msg-seed-3"]);
        assert!(threads[0].snippet.starts_with("Hello Alpha"));
    }

    #[test]
    fn mock_pagination_and_404s() {
        let source = MockMailSource;
        assert_eq!(source.threads(1, 1).expect("page").len(), 1);
        assert!(source.threads(50, 10).expect("empty page").is_empty());
        assert!(matches!(source.thread("nope"), Err(MailError::NotFound("thread"))));
        assert!(matches!(source.message("nope"), Err(MailError::NotFound("message"))));
        assert!(matches!(source.attachment("msg-seed-1", 0), Err(MailError::NotFound("attachment"))));
        assert_eq!(clamp_limit(None), 50);
        assert_eq!(clamp_limit(Some(500)), 200);
    }

    #[test]
    fn unavailable_source_fails_everywhere() {
        let source = UnavailableMailSource;
        assert!(matches!(source.mailbox(), Err(MailError::Unavailable(_))));
        assert!(matches!(source.threads(50, 0), Err(MailError::Unavailable(_))));
    }
}
