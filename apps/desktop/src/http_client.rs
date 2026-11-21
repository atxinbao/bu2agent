//! Minimal blocking HTTP client for Mock Gateway targets.
//!
//! Shared by the chat client and the file gateway client so the
//! transport plumbing lives in one place. HTTP only, localhost
//! only, no TLS: this is a Mock-only client. The real gateway
//! adapter must add TLS before any non-Mock target is used.
//! Error bodies are never copied into `UnifiedError`: they may
//! contain user text. Only the HTTP status is reported, with
//! one structured exception: a network-rule denial carries a
//! `rule` field naming user-configured policy (not user
//! text), and that field alone is surfaced so a blocked call
//! can say which rule blocked it.
//!
//! Phase 5: every socket gets a connect, read, and write
//! timeout (default 10 seconds), so a hung Mock server fails
//! the request instead of hanging the CLI forever. Tests use
//! `with_timeout` for a shorter bound.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::{ErrorCode, Result, UnifiedError, UserAction};

pub fn transport_error(detail: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::Transport,
        capability: Some("gateway".to_string()),
        retryable: true,
        user_action: UserAction::Retry,
        redacted_detail: detail.to_string(),
    }
}

pub fn gateway_error(status: u16) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::Gateway,
        capability: Some("gateway".to_string()),
        retryable: status >= 500,
        user_action: UserAction::Retry,
        redacted_detail: format!("mock gateway returned HTTP {status}"),
    }
}

/// Detail for a gateway error response. The body is never
/// copied wholesale; only structured machine fields are
/// appended: the `rule` field of a network-rule denial
/// (Task 13 window validation: the 403 itself passed, but
/// the rule name never reached the UI), and — as one group —
/// a whitelisted `error` machine code with its
/// `current_hash` (MCL-5 W2: the contacts update conflict
/// 409 must reach the UI so it can retry with the fresh
/// base). The code is surfaced only when it is a lowercase
/// snake machine code (`^[a-z][a-z0-9_]*$`) AND the hash is
/// a 64-hex sha256; the pair is only meaningful together —
/// a bare code has no retry base, and surfacing one alone
/// would change the redaction expectations the existing
/// tests pin (`{"error":"x","rule":42}` stays redacted).
fn gateway_error_detail(status: u16, body: &str) -> String {
    let base = format!("mock gateway returned HTTP {status}");
    let parsed = serde_json::from_str::<serde_json::Value>(body).ok();
    let rule = parsed
        .as_ref()
        .and_then(|value| value.get("rule")?.as_str().map(str::to_string))
        .filter(|rule| !rule.is_empty());
    let conflict = parsed.as_ref().and_then(|value| {
        let error = value.get("error")?.as_str()?;
        let hash = value.get("current_hash")?.as_str()?;
        if is_machine_code(error) && is_sha256_hex(hash) {
            Some(format!("{error}; current_hash={hash}"))
        } else {
            None
        }
    });
    let mut detail = base;
    if let Some(rule) = rule {
        detail.push_str(&format!(" (network rule: {rule})"));
    }
    if let Some(conflict) = conflict {
        detail.push_str(&format!(" ({conflict})"));
    }
    detail
}

fn is_machine_code(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

pub fn gateway_error_with_body(status: u16, body: &str) -> UnifiedError {
    UnifiedError {
        code: ErrorCode::Gateway,
        capability: Some("gateway".to_string()),
        retryable: status >= 500,
        user_action: UserAction::Retry,
        redacted_detail: gateway_error_detail(status, body),
    }
}

/// `base_url` must look like `http://127.0.0.1:3001`.
#[derive(Debug, Clone)]
pub struct HttpClient {
    host: String,
    port: u16,
    timeout_ms: u64,
}

impl HttpClient {
    pub fn new(base_url: &str) -> Result<Self> {
        Self::with_timeout(base_url, 10_000)
    }

    pub fn with_timeout(base_url: &str, timeout_ms: u64) -> Result<Self> {
        let stripped = base_url
            .strip_prefix("http://")
            .ok_or_else(|| transport_error("gateway base_url must start with http://"))?;
        let hostport = stripped.trim_end_matches('/');
        let (host, port) = match hostport.split_once(':') {
            Some((h, p)) => (
                h.to_string(),
                p.parse::<u16>()
                    .map_err(|_| transport_error("gateway base_url has an invalid port"))?,
            ),
            None => (hostport.to_string(), 80),
        };
        if host.is_empty() {
            return Err(transport_error("gateway base_url has an empty host"));
        }
        Ok(Self { host, port, timeout_ms })
    }

    /// Raw request. Returns (status, body) for every HTTP status,
    /// including 4xx/5xx, so callers can implement their own retry
    /// rules (notably: reads may retry once on 401, writes never
    /// retry blindly -- see file_gateway_client).
    pub fn request(&self, method: &str, path: &str, body: Option<&str>) -> Result<(u16, String)> {
        self.request_with_headers(method, path, body, &[])
    }

    /// Raw request with extra headers (MCL-3 shell route:
    /// `X-Shell-Token` on the record-from-shell POST).
    /// Callers must pass header-safe names/values (no
    /// CR/LF); the shell token is generated hex (or a
    /// validated dev override), so it is safe by
    /// construction.
    pub fn request_with_headers(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
        headers: &[(&str, &str)],
    ) -> Result<(u16, String)> {
        let timeout = Duration::from_millis(self.timeout_ms);
        let addr = format!("{}:{}", self.host, self.port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .ok_or_else(|| transport_error("cannot resolve mock gateway host"))?;
        let mut stream = TcpStream::connect_timeout(&addr, timeout)
            .map_err(|_| transport_error("cannot connect to mock gateway"))?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|_| transport_error("cannot set mock gateway read timeout"))?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|_| transport_error("cannot set mock gateway write timeout"))?;
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
            self.host
        );
        for (name, value) in headers {
            req.push_str(&format!("{name}: {value}\r\n"));
        }
        if let Some(body) = body {
            req.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                body.len()
            ));
        }
        req.push_str("\r\n");
        if let Some(body) = body {
            req.push_str(body);
        }
        stream
            .write_all(req.as_bytes())
            .map_err(|_| transport_error("cannot write mock gateway request"))?;
        let mut raw = String::new();
        stream
            .read_to_string(&mut raw)
            .map_err(|_| transport_error("cannot read mock gateway response"))?;
        let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
        let status: u16 = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| transport_error("mock gateway returned an unreadable status"))?;
        Ok((status, body.to_string()))
    }

    /// Raw byte request, mirroring request() but without
    /// the UTF-8 body assumption (Task 22: attachment
    /// downloads are arbitrary bytes, not JSON).
    pub fn request_bytes(&self, method: &str, path: &str) -> Result<(u16, Vec<u8>)> {
        let timeout = Duration::from_millis(self.timeout_ms);
        let addr = format!("{}:{}", self.host, self.port)
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .ok_or_else(|| transport_error("cannot resolve mock gateway host"))?;
        let mut stream = TcpStream::connect_timeout(&addr, timeout)
            .map_err(|_| transport_error("cannot connect to mock gateway"))?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|_| transport_error("cannot set mock gateway read timeout"))?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|_| transport_error("cannot set mock gateway write timeout"))?;
        let req = format!("{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", self.host);
        stream
            .write_all(req.as_bytes())
            .map_err(|_| transport_error("cannot write mock gateway request"))?;
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .map_err(|_| transport_error("cannot read mock gateway response"))?;
        let split = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|pos| pos + 4)
            .unwrap_or(raw.len());
        let (head, body) = raw.split_at(split);
        let head = String::from_utf8_lossy(head);
        let status: u16 = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .ok_or_else(|| transport_error("mock gateway returned an unreadable status"))?;
        Ok((status, body.to_vec()))
    }

    /// GET returning raw bytes (Task 22: attachments).
    /// On a non-2xx status the error body is deliberately
    /// not copied into the error (redaction policy).
    pub fn get_bytes(&self, path: &str) -> Result<Vec<u8>> {
        let (status, body) = self.request_bytes("GET", path)?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        Ok(body)
    }

    pub fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let (status, body) = self.request("GET", path, None)?;
        if !(200..300).contains(&status) {
            return Err(gateway_error_with_body(status, &body));
        }
        serde_json::from_str(&body).map_err(|_| transport_error("mock gateway returned invalid JSON"))
    }

    pub fn post_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &str,
    ) -> Result<T> {
        let (status, resp_body) = self.request("POST", path, Some(body))?;
        if !(200..300).contains(&status) {
            return Err(gateway_error_with_body(status, &resp_body));
        }
        serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))
    }

    /// POST with a JSON body and extra headers, mirroring
    /// post_json (MCL-3 shell route: the record-from-shell
    /// POST carries `X-Shell-Token`).
    pub fn post_json_with_headers<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &str,
        headers: &[(&str, &str)],
    ) -> Result<T> {
        let (status, resp_body) = self.request_with_headers("POST", path, Some(body), headers)?;
        if !(200..300).contains(&status) {
            return Err(gateway_error_with_body(status, &resp_body));
        }
        serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))
    }

    /// PATCH with a JSON body, mirroring post_json (Task 11:
    /// the permissions surface is the first PATCH consumer).
    pub fn patch_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &str,
    ) -> Result<T> {
        let (status, resp_body) = self.request("PATCH", path, Some(body))?;
        if !(200..300).contains(&status) {
            return Err(gateway_error_with_body(status, &resp_body));
        }
        serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))
    }

    /// DELETE with no body, mirroring get_json (Task 11: the
    /// connector-reset and hitl-snooze routes).
    pub fn delete_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let (status, body) = self.request("DELETE", path, None)?;
        if !(200..300).contains(&status) {
            return Err(gateway_error_with_body(status, &body));
        }
        serde_json::from_str(&body).map_err(|_| transport_error("mock gateway returned invalid JSON"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denial_detail_names_the_rule() {
        let detail = gateway_error_detail(
            403,
            r#"{"error":"network rule denied model call","rule":"api.deepseek.com"}"#,
        );
        assert_eq!(
            detail,
            "mock gateway returned HTTP 403 (network rule: api.deepseek.com)"
        );
    }

    #[test]
    fn other_bodies_stay_redacted() {
        // No rule field: only the status survives, never body text.
        let detail = gateway_error_detail(502, r#"{"error":"user secret text here"}"#);
        assert_eq!(detail, "mock gateway returned HTTP 502");
        assert!(!detail.contains("secret"));
        // A non-string rule is not surfaced either.
        let detail = gateway_error_detail(403, r#"{"error":"x","rule":42}"#);
        assert_eq!(detail, "mock gateway returned HTTP 403");
        // Malformed body degrades to the plain status form.
        let detail = gateway_error_detail(403, "not json");
        assert_eq!(detail, "mock gateway returned HTTP 403");
    }

    #[test]
    fn conflict_detail_surfaces_machine_code_and_hash() {
        // MCL-5 W2: the contacts update conflict 409 must
        // reach the UI as a machine code + fresh base hash.
        let hash = "0123456789abcdef".repeat(4);
        assert_eq!(hash.len(), 64);
        let detail = gateway_error_detail(
            409,
            &format!(r#"{{"error":"contact_changed","current_hash":"{hash}"}}"#),
        );
        assert_eq!(
            detail,
            format!("mock gateway returned HTTP 409 (contact_changed; current_hash={hash})")
        );
        // Illegal error text stays redacted even with a valid hash.
        let detail = gateway_error_detail(
            409,
            &format!(r#"{{"error":"user secret text here","current_hash":"{hash}"}}"#),
        );
        assert_eq!(detail, "mock gateway returned HTTP 409");
        assert!(!detail.contains("secret"));
        // A non-hex hash stays redacted even with a valid code.
        let detail = gateway_error_detail(
            409,
            r#"{"error":"contact_changed","current_hash":"not-a-sha256-hex"}"#,
        );
        assert_eq!(detail, "mock gateway returned HTTP 409");
        assert!(!detail.contains("contact_changed"));
        // A bare machine code without a hash is not surfaced.
        let detail = gateway_error_detail(409, r#"{"error":"contact_changed"}"#);
        assert_eq!(detail, "mock gateway returned HTTP 409");
    }
}
