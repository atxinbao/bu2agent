//! Minimal blocking HTTP client for Mock Gateway targets.
//!
//! Shared by the chat client and the file gateway client so the
//! transport plumbing lives in one place. HTTP only, localhost
//! only, no TLS: this is a Mock-only client. The real gateway
//! adapter must add TLS before any non-Mock target is used.
//! Error bodies are never copied into `UnifiedError`: they may
//! contain user text. Only the HTTP status is reported.
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

    pub fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let (status, body) = self.request("GET", path, None)?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
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
            return Err(gateway_error(status));
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
            return Err(gateway_error(status));
        }
        serde_json::from_str(&resp_body)
            .map_err(|_| transport_error("mock gateway returned invalid JSON"))
    }

    /// DELETE with no body, mirroring get_json (Task 11: the
    /// connector-reset and hitl-snooze routes).
    pub fn delete_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let (status, body) = self.request("DELETE", path, None)?;
        if !(200..300).contains(&status) {
            return Err(gateway_error(status));
        }
        serde_json::from_str(&body).map_err(|_| transport_error("mock gateway returned invalid JSON"))
    }
}
