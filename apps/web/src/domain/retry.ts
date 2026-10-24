// Bounded retry policy for the TS Domain Store (Phase 5).
//
// Same rule as the Rust RetryPolicy: only errors the caller
// marks retryable are retried, and only up to maxAttempts
// total tries. Non-retryable errors stop at once. Delays
// grow linearly (baseDelayMs * attempt). This is a
// Mock-level policy, not a production backoff with jitter.

export interface RetryPolicy {
  maxAttempts: number;
  baseDelayMs: number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxAttempts: 3,
  baseDelayMs: 100,
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function withRetry<T>(
  policy: RetryPolicy,
  operation: () => Promise<T>,
  isRetryable: (error: unknown) => boolean,
): Promise<T> {
  const attempts = Math.max(1, policy.maxAttempts);
  let lastError: unknown = new Error("retry loop ended without an error");
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      if (!isRetryable(error) || attempt === attempts) {
        throw error;
      }
      lastError = error;
      await sleep(policy.baseDelayMs * attempt);
    }
  }
  throw lastError;
}
