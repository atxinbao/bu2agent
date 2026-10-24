//! Bounded retry policy for transient failures (Phase 5).
//!
//! Only errors marked `retryable` are retried, and only up to
//! `max_attempts` total tries. Non-retryable errors (bad
//! input, permission denied, approval refused) stop at once:
//! retrying them cannot help and, for writes, could repeat a
//! side effect. Delays grow linearly (base * attempt) and
//! are capped by the caller's base: this is a Mock-level
//! policy, not a production backoff with jitter.
//!
//! Callers that already have their own retry rule (notably
//! the 401 rule in file_gateway_client: reads once, writes
//! never) keep that rule; this policy is for generic
//! transient transport failures, not a replacement for it.

use std::time::Duration;

use crate::Result;

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 3, base_delay_ms: 100 }
    }
}

impl RetryPolicy {
    pub fn run<T, F>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Result<T>,
    {
        let attempts = self.max_attempts.max(1);
        let mut last_error = None;
        for attempt in 1..=attempts {
            match operation() {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if !error.retryable || attempt == attempts {
                        return Err(error);
                    }
                    last_error = Some(error);
                    std::thread::sleep(Duration::from_millis(self.base_delay_ms * attempt as u64));
                }
            }
        }
        Err(last_error.expect("retry loop always leaves an error"))
    }
}
