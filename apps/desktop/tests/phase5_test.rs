//! Phase 5 tests: bounded retry and client timeouts.
//!
//! Retry: a flaky operation that fails twice with a
//! retryable error succeeds on the third attempt; a
//! non-retryable error stops after one attempt, no matter
//! how many attempts remain.
//! Timeout: a server that accepts but never responds must
//! fail the request within the configured bound, not hang.

use agent_desktop::http_client::{transport_error, HttpClient};
use agent_desktop::retry::RetryPolicy;
use agent_desktop::{ErrorCode, UnifiedError, UserAction};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn non_retryable() -> UnifiedError {
    UnifiedError {
        code: ErrorCode::PermissionDenied,
        capability: Some("test".to_string()),
        retryable: false,
        user_action: UserAction::None,
        redacted_detail: "refused".to_string(),
    }
}

#[test]
fn retry_succeeds_after_transient_failures() {
    let policy = RetryPolicy { max_attempts: 3, base_delay_ms: 1 };
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = Arc::clone(&attempts);
    let result = policy.run(|| {
        let n = attempts_clone.fetch_add(1, Ordering::SeqCst) + 1;
        if n < 3 {
            Err(transport_error("flaky"))
        } else {
            Ok("done")
        }
    });
    assert_eq!(result.expect("retry should succeed"), "done");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[test]
fn retry_stops_at_once_on_non_retryable_error() {
    let policy = RetryPolicy { max_attempts: 5, base_delay_ms: 1 };
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = Arc::clone(&attempts);
    let result: agent_desktop::Result<&str> = policy.run(|| {
        attempts_clone.fetch_add(1, Ordering::SeqCst);
        Err(non_retryable())
    });
    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn client_times_out_on_silent_server() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind silent server");
    let addr = listener.local_addr().expect("silent server addr");
    // Accept connections and never respond, in the background.
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(30));
                drop(stream);
            });
        }
    });
    let client = HttpClient::with_timeout(&format!("http://{addr}"), 500).expect("client");
    let started = Instant::now();
    let result = client.request("GET", "/health", None);
    assert!(result.is_err(), "silent server must fail, not hang");
    assert!(started.elapsed() < Duration::from_secs(10), "timeout must bound the wait");
}
