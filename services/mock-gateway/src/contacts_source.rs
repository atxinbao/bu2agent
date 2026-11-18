//! Contacts read surface, MCL-2.
//!
//! MCL-2 syncs the Contacts.app card list into SQLite
//! (`contacts`). The read path is AppleScript (the
//! re-approved Q-MCL-10): the MCL-0 probe + supplement
//! verified AppleScript contacts CRUD on 182 cards
//! (create 0.414 s, rename + add phone/email 0.254 s,
//! delete 0.187 s) and the native id (`UUID:ABPerson`
//! string) stable across a Contacts.app quit/reopen.
//!
//! What the dump asks for — and nothing else (a guard
//! test below locks the property list in):
//!
//! - `id` of the person (the native id),
//! - `name` (display name), `organization`, `job title`,
//! - `phones` / `emails` values (labels are not carried:
//!   the fact table stores the values only),
//! - `addresses` (street / city / state / zip / country),
//!   carried as a hash only — see below,
//! - `birth date`, carried as a hash only.
//!
//! PII shape (Q-MCL-11): display_name / organization /
//! job_title and phones / emails are PLAINTEXT in
//! SQLite (display, dial, send all need them); addresses
//! and birthday are hash / existence-flag ONLY —
//! `addresses_hash` is the sha256 of the canonical JSON
//! of the sorted address list, NULL when the contact has
//! no addresses; `birthday_hash` is the sha256 of the
//! ISO-8601 date string, NULL when there is no birthday.
//! NULL is the existence flag. Event payloads carry
//! hashes / counts only, never plaintext (red line 7).
//!
//! Honest gaps (plan §12 Q-MCL-10 residual): non-empty
//! address / birthday readability was NOT verified by
//! the probe (both test contacts had empty address /
//! birthday) — the hash code paths here are unit-tested
//! with fake data only, and the MCL-2 window script
//! must check real non-empty fields; if AppleScript
//! cannot read them, Q-MCL-11's hash rule gets
//! revisited then. Contacts native-id stability is
//! proven only across app restart, not OS reinstall /
//! account migration.
//!
//! The source is a trait so the mechanism can change
//! without touching routes, clients, or UI:
//!
//! - `AppleScriptContactsSource` (macOS only): asks
//!   Contacts.app via `osascript`. The dump script form
//!   is best-effort and window validation on a real Mac
//!   is the proof, as with the mail / imessage scripts.
//! - `UnavailableContactsSource`: every call fails with
//!   `ContactsError::Unavailable` (501, never fake data).
//!
//! The gateway checks the Contacts.app process state
//! FIRST (pgrep) and never auto-launches it: B2-0 proved
//! osascript auto-launches an app it is told to control.
//! Each `osascript` invocation is capped at 30 seconds
//! by the gateway (the mail_source.rs pattern).

use serde::{Deserialize, Serialize};

use crate::operation_binding::sha256_hex;

/// One postal address as the live source reports it.
/// Field order is the canonical order (street, city,
/// state, postal_code, country); serde writes struct
/// fields in declaration order, so `to_string` on this
/// struct IS the canonical JSON used for hashing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactAddress {
    pub street: String,
    pub city: String,
    pub state: String,
    pub postal_code: String,
    pub country: String,
}

/// One contact (card) as the live source reports it.
/// `native_id` is the card's AppleScript id (a
/// `UUID:ABPerson` string); `birthday` is the ISO-8601
/// date string (`YYYY-MM-DD`) or `None` when the card
/// has no birth date. Phones / emails are value-only
/// (labels are not carried — the fact table stores the
/// values only, Q-MCL-11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContactView {
    pub native_id: String,
    pub display_name: String,
    pub organization: String,
    pub job_title: String,
    pub phones: Vec<String>,
    pub emails: Vec<String>,
    pub addresses: Vec<ContactAddress>,
    pub birthday: Option<String>,
}

/// Source failures. Same six variants as `MailError`
/// (the same mapping: `NotFound` -> 404, `Unavailable`
/// -> 501, `Denied` -> 403, `Upstream` / `NotRunning` /
/// `Timeout` -> 502 on the read paths). Separate type so
/// no contacts error ever renders as a mail error.
#[derive(Debug, Clone)]
pub enum ContactsError {
    NotFound(&'static str),
    Unavailable(String),
    Denied,
    Upstream(String),
    NotRunning,
    Timeout,
}

/// Live-query contacts source (no sync cache in MCL-2;
/// the sync writes the fact rows).
pub trait ContactsSource: Send + Sync {
    /// List contacts, enumeration order first.
    /// `limit`/`offset` apply in Rust over the full
    /// enumeration (cards are metadata-only, so one
    /// script enumerates all people); the sync passes
    /// `usize::MAX` to take them all.
    fn contacts(&self, limit: usize, offset: usize) -> Result<Vec<ContactView>, ContactsError>;
}

/// sha256 of the canonical JSON of the sorted string
/// list. Order-independent: the same values in any
/// order hash the same; the values themselves never
/// leave this function (the phone / email hash pattern,
/// like `participant_hash_for` in imessage_source.rs).
pub fn string_list_hash(values: &[String]) -> String {
    let mut sorted: Vec<&str> = values.iter().map(String::as_str).collect();
    sorted.sort();
    let json = serde_json::to_string(&sorted).unwrap_or_else(|_| "[]".to_string());
    sha256_hex(json.as_bytes())
}

/// The addresses hash for one contact: sha256 of the
/// canonical JSON of the sorted address list — each
/// address serializes to its canonical JSON (struct
/// field order), the strings sort, and the array of
/// those strings is the hashed document. `None` when
/// the contact has no addresses: NULL is the existence
/// flag (Q-MCL-11).
pub fn addresses_hash_for(addresses: &[ContactAddress]) -> Option<String> {
    if addresses.is_empty() {
        return None;
    }
    let mut items: Vec<String> = addresses
        .iter()
        .map(|a| serde_json::to_string(a).unwrap_or_else(|_| "{}".to_string()))
        .collect();
    items.sort();
    let json = format!("[{}]", items.join(","));
    Some(sha256_hex(json.as_bytes()))
}

/// The birthday hash for one contact: sha256 of the
/// ISO-8601 date string (`YYYY-MM-DD`). `None` when
/// there is no birth date: NULL is the existence flag
/// (Q-MCL-11).
pub fn birthday_hash_for(birthday: &str) -> String {
    sha256_hex(birthday.as_bytes())
}

/// Parse the AppleScript contacts dump: one contact per
/// line, fields separated by ASCII 31, in the order id,
/// display name, organization, job title, phones
/// (comma-joined), emails (comma-joined), addresses
/// (`;`-joined records, `|`-joined fields in the order
/// street | city | state | zip | country), birthday
/// (`YYYY-MM-DD`, empty when none). Lines with the wrong
/// field count or an empty id are skipped; empty phones /
/// emails are dropped. A malformed address record is
/// dropped (the contact itself is kept). Text properties
/// arrive as `""` when unset — the script guards
/// `missing value` for them, so an empty string here
/// means "no value", never the literal "missing value".
pub fn parse_contacts_dump(text: &str) -> Vec<ContactView> {
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
        if fields.len() != 8 {
            continue;
        }
        let native_id = fields[0].to_string();
        if native_id.trim().is_empty() {
            continue;
        }
        let phones: Vec<String> = fields[4]
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let emails: Vec<String> = fields[5]
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let mut addresses = Vec::new();
        if !fields[6].trim().is_empty() {
            for record in fields[6].split(';') {
                if record.trim().is_empty() {
                    continue;
                }
                let parts: Vec<&str> = record.split('|').collect();
                if parts.len() != 5 {
                    continue;
                }
                addresses.push(ContactAddress {
                    street: parts[0].trim().to_string(),
                    city: parts[1].trim().to_string(),
                    state: parts[2].trim().to_string(),
                    postal_code: parts[3].trim().to_string(),
                    country: parts[4].trim().to_string(),
                });
            }
        }
        let birthday = fields[7].trim();
        views.push(ContactView {
            native_id,
            display_name: fields[1].to_string(),
            organization: fields[2].to_string(),
            job_title: fields[3].to_string(),
            phones,
            emails,
            addresses,
            birthday: if birthday.is_empty() {
                None
            } else {
                Some(birthday.to_string())
            },
        });
    }
    views
}

/// Clamp a contacts-list limit (the mail `clamp_limit`
/// precedent): default 50, maximum 200.
pub fn clamp_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(50).min(200)
}

/// Every call fails: the production source on platforms
/// without a real Contacts source. Never fake data.
pub struct UnavailableContactsSource;

impl ContactsSource for UnavailableContactsSource {
    fn contacts(&self, _limit: usize, _offset: usize) -> Result<Vec<ContactView>, ContactsError> {
        Err(ContactsError::Unavailable(
            "contacts source unavailable on this platform".to_string(),
        ))
    }
}

/// AppleScript for the contacts dump. Plain string
/// construction (no macOS calls) so the guard test can
/// run on every platform. One script enumerates all
/// people: cards are metadata-only (id, name, phones,
/// emails, addresses, birthday), so no window is needed
/// — the mail 50-message window exists because bodies
/// are materialized lazily, which does not apply here.
///
/// The script asks for id / name / organization / job
/// title / phones / emails / addresses / birth date
/// ONLY: no notes, no vCard export, no images, no urls.
/// Unset text properties are guarded against `missing
/// value` (an unguarded coercion of `missing value` to
/// string fails the whole script — and most cards have
/// no organization, job title, address, or birthday).
#[cfg(any(target_os = "macos", test))]
fn contacts_dump_script() -> String {
    r#"with timeout of 120 seconds
  tell application "Contacts"
    set totalCount to count people
    if totalCount is 0 then
      return ""
    end if
    set out to ""
    repeat with p in people
      set pid to (id of p as string)
      set pname to (name of p as string)
      set orgVal to organization of p
      if orgVal is missing value then
        set orgVal to ""
      else
        set orgVal to (orgVal as string)
      end if
      set titleVal to job title of p
      if titleVal is missing value then
        set titleVal to ""
      else
        set titleVal to (titleVal as string)
      end if
      set phoneVals to ""
      repeat with ph in phones of p
        set phoneVals to phoneVals & (value of ph as string) & ","
      end repeat
      set emailVals to ""
      repeat with em in emails of p
        set emailVals to emailVals & (value of em as string) & ","
      end repeat
      set addrVals to ""
      repeat with a in addresses of p
        set aStreet to street of a
        if aStreet is missing value then
          set aStreet to ""
        else
          set aStreet to (aStreet as string)
        end if
        set aCity to city of a
        if aCity is missing value then
          set aCity to ""
        else
          set aCity to (aCity as string)
        end if
        set aState to state of a
        if aState is missing value then
          set aState to ""
        else
          set aState to (aState as string)
        end if
        set aZip to zip of a
        if aZip is missing value then
          set aZip to ""
        else
          set aZip to (aZip as string)
        end if
        set aCountry to country of a
        if aCountry is missing value then
          set aCountry to ""
        else
          set aCountry to (aCountry as string)
        end if
        set addrVals to addrVals & aStreet & "|" & aCity & "|" & aState & "|" & aZip & "|" & aCountry & ";"
      end repeat
      set bdVal to birth date of p
      if bdVal is missing value then
        set bdStr to ""
      else
        set bdYear to (year of bdVal as string)
        set bdMonth to (month of bdVal as integer)
        if bdMonth < 10 then
          set bdMonthStr to "0" & (bdMonth as string)
        else
          set bdMonthStr to (bdMonth as string)
        end if
        set bdDay to (day of bdVal as integer)
        if bdDay < 10 then
          set bdDayStr to "0" & (bdDay as string)
        else
          set bdDayStr to (bdDay as string)
        end if
        set bdStr to bdYear & "-" & bdMonthStr & "-" & bdDayStr
      end if
      set out to out & pid & (character id 31) & pname & (character id 31) & orgVal & (character id 31) & titleVal & (character id 31) & phoneVals & (character id 31) & emailVals & (character id 31) & addrVals & (character id 31) & bdStr & return
    end repeat
    return out
  end tell
end timeout"#
        .to_string()
}

/// Hard cap on one `osascript` invocation (the
/// mail_source.rs pattern). Expiry maps to `Timeout`.
/// 120 s, not mail's 30 s: a full dump over ~184 cards
/// takes ~30 s, so 30 s straddled the real duration —
/// the identical first sync passed while the second
/// timed out (MCL-2 round 4, W3-1). Matches the desktop
/// sync client's 120 s timeout. Keep in step with the
/// script's own `with timeout` (see the
/// `dump_script_timeout_covers_gateway_cap` test).
#[cfg(target_os = "macos")]
const SCRIPT_TIMEOUT_SECS: u64 = 120;

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

/// macOS-only real source: asks Contacts.app via
/// `osascript`. See the module docs for the best-effort
/// caveat: the script form below is validated by window
/// validation on a real Mac, not by this VM.
#[cfg(target_os = "macos")]
pub struct AppleScriptContactsSource;

#[cfg(target_os = "macos")]
impl AppleScriptContactsSource {
    pub fn new() -> Self {
        Self
    }

    /// Is Contacts.app running? Uses `pgrep` (no
    /// permission needed) so a read never launches
    /// Contacts as a side effect. If `pgrep` itself is
    /// missing, proceed and let the script report the
    /// real error (the mail_source.rs precedent).
    fn contacts_running() -> bool {
        match Command::new("pgrep").args(["-x", "Contacts"]).output() {
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
    fn run_script(script: &str) -> Result<String, ContactsError> {
        if !Self::contacts_running() {
            return Err(ContactsError::NotRunning);
        }
        let n = SCRIPT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let stem = format!("bu2agent-contacts-script-{}-{n}", std::process::id());
        let out_path = std::env::temp_dir().join(format!("{stem}.out"));
        let err_path = std::env::temp_dir().join(format!("{stem}.err"));
        let out_file = fs::File::create(&out_path).map_err(|_| {
            ContactsError::Upstream("could not capture Contacts script output".to_string())
        })?;
        let err_file = fs::File::create(&err_path).map_err(|_| {
            ContactsError::Upstream("could not capture Contacts script output".to_string())
        })?;
        let mut child = Command::new("osascript")
            .arg("-e")
            .arg(script)
            .stdout(Stdio::from(out_file))
            .stderr(Stdio::from(err_file))
            .spawn()
            .map_err(|_| ContactsError::Upstream("osascript unavailable".to_string()))?;
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
                        return Err(ContactsError::Denied);
                    }
                    return Err(ContactsError::Upstream(
                        "Contacts automation failed".to_string(),
                    ));
                }
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        let _ = fs::remove_file(&out_path);
                        let _ = fs::remove_file(&err_path);
                        return Err(ContactsError::Timeout);
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = fs::remove_file(&out_path);
                    let _ = fs::remove_file(&err_path);
                    return Err(ContactsError::Upstream(
                        "Contacts automation failed".to_string(),
                    ));
                }
            }
        }
    }
}

#[cfg(target_os = "macos")]
impl Default for AppleScriptContactsSource {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(target_os = "macos")]
impl ContactsSource for AppleScriptContactsSource {
    fn contacts(&self, limit: usize, offset: usize) -> Result<Vec<ContactView>, ContactsError> {
        let dump = Self::run_script(&contacts_dump_script())?;
        let views = parse_contacts_dump(&dump);
        Ok(views.into_iter().skip(offset).take(limit).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dump_script_asks_only_for_id_name_phones_emails_addresses_birthday() {
        // MCL-2 asks the Contacts dictionary for the
        // approved fields only: id / name / organization /
        // job title / phones / emails / addresses / birth
        // date. No notes, no vCard export, no images, no
        // urls, no social profiles. This guard runs on
        // every platform because the builder is plain
        // string construction.
        let script = contacts_dump_script();
        assert!(script.contains("count people"), "must enumerate people");
        assert!(script.contains("id of p"), "must read the id");
        assert!(script.contains("name of p"), "must read the name");
        assert!(
            script.contains("organization of p"),
            "must read the organization"
        );
        assert!(script.contains("job title of p"), "must read the job title");
        assert!(script.contains("phones of p"), "must read phones");
        assert!(script.contains("value of ph"), "must read phone values");
        assert!(script.contains("emails of p"), "must read emails");
        assert!(script.contains("value of em"), "must read email values");
        assert!(script.contains("addresses of p"), "must read addresses");
        assert!(
            script.contains("birth date of p"),
            "must read the birth date"
        );
        for forbidden in ["note of", "vcard", "image of", "urls of", "social profile"] {
            assert!(
                !script.to_lowercase().contains(forbidden),
                "must never ask for {forbidden}"
            );
        }
    }

    #[test]
    fn dump_script_has_no_single_line_if_else() {
        // AppleScript's single-line `if` form takes no
        // `else`: `if c then s1 else s2` on one line is a
        // -2741 compile error ("Expected end of line, etc.
        // but found 'else'"), so the whole dump script
        // fails to compile and every sync 502s with
        // Upstream "Contacts automation failed". The MCL-2
        // window validation on real Contacts data caught
        // this (round 2, W1-1); the substring guard test
        // above cannot see it. Every `else` must sit on
        // its own line.
        let script = contacts_dump_script();
        for (i, line) in script.lines().enumerate() {
            assert!(
                !(line.contains("then") && line.contains("else")),
                "line {} is a single-line if-then-else: {line}",
                i + 1
            );
        }
    }

    #[test]
    fn dump_script_timeout_covers_gateway_cap() {
        // The gateway kills osascript at SCRIPT_TIMEOUT_SECS
        // (120 s); the script's own `with timeout` must be
        // at least that long, or a slow dump fails inside
        // AppleScript (-1712) instead of surfacing the
        // gateway's clean `Timeout` error. The literal 120
        // here is SCRIPT_TIMEOUT_SECS — the const itself is
        // macOS-only, so this platform-independent test pins
        // the number; change both together.
        let script = contacts_dump_script();
        let marker = "with timeout of ";
        let start = script.find(marker).expect("script must set a timeout");
        let rest = &script[start + marker.len()..];
        let end = rest.find(" seconds").expect("timeout must be in seconds");
        let script_secs: u64 = rest[..end]
            .trim()
            .parse()
            .expect("script timeout must be a number");
        assert!(
            script_secs >= 120,
            "script timeout ({script_secs}s) must cover the gateway cap (120s)"
        );
    }

    #[test]
    fn string_list_hash_is_order_independent_and_hides_values() {
        let a = string_list_hash(&[
            "+15550101001".to_string(),
            "friend@mock.invalid".to_string(),
        ]);
        let b = string_list_hash(&[
            "friend@mock.invalid".to_string(),
            "+15550101001".to_string(),
        ]);
        assert_eq!(a, b, "same values must hash the same");
        assert_eq!(a.len(), 64, "sha256 hex");
        assert!(!a.contains("+15550101001"), "no value in the hash");
        assert!(!a.contains("friend@mock.invalid"), "no value in the hash");
        let single = string_list_hash(&["+15550101001".to_string()]);
        assert_ne!(a, single, "different sets must hash differently");
    }

    #[test]
    fn addresses_hash_is_none_without_addresses_and_stable_otherwise() {
        assert_eq!(addresses_hash_for(&[]), None, "NULL is the existence flag");
        let addr = ContactAddress {
            street: "1 Mock St".to_string(),
            city: "Springfield".to_string(),
            state: "IL".to_string(),
            postal_code: "62701".to_string(),
            country: "USA".to_string(),
        };
        let one = addresses_hash_for(std::slice::from_ref(&addr)).unwrap();
        assert_eq!(one.len(), 64, "sha256 hex");
        assert!(!one.contains("Mock St"), "no address value in the hash");
        // Order-independent: the same addresses in any
        // order hash the same.
        let other = ContactAddress {
            street: "2 Other Ave".to_string(),
            city: "Shelbyville".to_string(),
            state: "IL".to_string(),
            postal_code: "62702".to_string(),
            country: "USA".to_string(),
        };
        let ab = addresses_hash_for(&[addr.clone(), other.clone()]).unwrap();
        let ba = addresses_hash_for(&[other, addr]).unwrap();
        assert_eq!(ab, ba, "same addresses must hash the same");
        assert_ne!(ab, one, "different sets must hash differently");
    }

    #[test]
    fn birthday_hash_is_sha256_of_the_iso_date() {
        assert_eq!(
            birthday_hash_for("1990-05-17"),
            sha256_hex("1990-05-17".as_bytes())
        );
    }

    #[test]
    fn dump_parsing_reads_full_and_minimal_contacts() {
        let dump = concat!(
            "person-a\u{1f}Ada Lovelace\u{1f}Analytical Engines\u{1f}Mathematician\u{1f}+15550101001,+15550101002,\u{1f}ada@mock.invalid,\u{1f}1 Mock St|Springfield|IL|62701|USA;2 Other Ave|Shelbyville|IL|62702|USA;\u{1f}1990-05-17\n",
            "person-b\u{1f}Bob\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\n",
            "bad line\n",
            "person-c\u{1f}Carol\u{1f}\u{1f}\u{1f}+15550101003,\u{1f}\u{1f}9 Bad Rd|Nowhere|XX|00000|USA;garbage-record\u{1f}\n",
        );
        let views = parse_contacts_dump(dump);
        assert_eq!(views.len(), 3);
        assert_eq!(views[0].native_id, "person-a");
        assert_eq!(views[0].display_name, "Ada Lovelace");
        assert_eq!(views[0].organization, "Analytical Engines");
        assert_eq!(views[0].job_title, "Mathematician");
        assert_eq!(views[0].phones, vec!["+15550101001", "+15550101002"]);
        assert_eq!(views[0].emails, vec!["ada@mock.invalid"]);
        assert_eq!(views[0].addresses.len(), 2);
        assert_eq!(views[0].addresses[0].street, "1 Mock St");
        assert_eq!(views[0].addresses[1].postal_code, "62702");
        assert_eq!(views[0].birthday.as_deref(), Some("1990-05-17"));
        // Minimal card: empty text, no phones / emails /
        // addresses, no birthday.
        assert_eq!(views[1].display_name, "Bob");
        assert!(views[1].phones.is_empty());
        assert!(views[1].emails.is_empty());
        assert!(views[1].addresses.is_empty());
        assert_eq!(views[1].birthday, None);
        // Malformed address record dropped, contact kept.
        assert_eq!(views[2].addresses.len(), 1);
        assert_eq!(views[2].birthday, None);
    }

    #[test]
    fn dump_parsing_accepts_macos_carriage_returns() {
        let dump = "person-a\u{1f}Ada\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\rperson-b\u{1f}Bob\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\r";
        let views = parse_contacts_dump(dump);
        assert_eq!(views.len(), 2);
        assert_eq!(views[1].display_name, "Bob");
    }

    #[test]
    fn dump_parsing_skips_malformed_lines() {
        let dump = "person-a\u{1f}Ada\n\u{1f}Ada\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\nperson-b\u{1f}Bob\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\u{1f}\n";
        let views = parse_contacts_dump(dump);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].native_id, "person-b");
    }
}
