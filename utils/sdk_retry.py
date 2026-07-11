"""ak-wty: SDK retry policy for chunk-processing errors.

F12's per-run non-determinism ("Fatal error in message reader" on
chunks 3-4, per infra's [DIAG 5 ANOMALIES]) has the same signature
as ak-8cp which we WONTFIX'd because it looked environmental. That
call was too fast — under production load the error clearly retries
successfully on the next attempt. Prior code swallowed the error and
returned zero-count for the chunk, causing silent under-parse on the
whole file.

DESIGN:

  is_retryable_sdk_error(error_msg) → bool
    True iff the error signature matches a KNOWN-transient pattern.
    Falses out on schema-validation / auth / structured-output
    errors that would just fail again on retry.

  retry_delay_seconds(attempt) → float
    Exponential backoff, capped: 1s, 2s, 4s. attempt is 0-indexed
    (0 = first retry, so delay before attempt 1).

  MAX_RETRIES = 3
    Total attempts including original = 4 (original + 3 retries).
    Beyond that we log with fileID + chunk indices and bail.

Pure-Python (no framework deps) so it's testable without booting
the SDK client.
"""

from __future__ import annotations


# 3 retries = 4 total attempts. Any higher and the pipeline hangs
# on genuinely-broken chunks; any lower and legit transient failures
# still leak through.
MAX_RETRIES = 3

# Substrings we RETRY on. All matched case-insensitively.
_RETRYABLE_SIGNATURES = (
    "fatal error in message reader",  # F12 canonical signature
    "message reader",                  # broader match
    "stream",                          # any streaming interrupt
    "connection",                      # network hiccup
    "timeout",                         # timeouts
    "rate_limit",                      # rate-limit (retry after backoff)
    "rate limit",                      # phrasing variant
    "server error",                    # generic 5xx wrapper
    "internal error",                  # generic 5xx wrapper
    "temporarily unavailable",         # explicit transient
    "temporary failure",
    "read timed out",
    "deadline exceeded",
    "eof",                             # raw stream EOF
    "broken pipe",
)

# Substrings we DO NOT retry on — these are terminal per attempt.
_NON_RETRYABLE_SIGNATURES = (
    "schema",                          # structured-output validation
    "invalid_request",                 # 4xx malformed
    "authentication",                  # bad creds
    "unauthorized",                    # 401
    "forbidden",                       # 403
    "not found",                       # 404
    "permission",                      # permission model rejection
)


def is_retryable_sdk_error(error_msg) -> bool:
    """ak-wty predicate: return True iff `error_msg` matches a
    known-transient signature. Non-string / None / empty inputs
    return False.

    Non-retryable signatures take PRIORITY — if the message hits
    both a retryable and a non-retryable pattern (rare), we treat
    it as non-retryable to avoid a retry storm on a genuinely
    broken request.
    """
    if not error_msg or not isinstance(error_msg, str):
        return False
    lower = error_msg.lower()

    # Non-retryable takes priority.
    for sig in _NON_RETRYABLE_SIGNATURES:
        if sig in lower:
            return False

    for sig in _RETRYABLE_SIGNATURES:
        if sig in lower:
            return True

    return False


def retry_delay_seconds(attempt: int, *, base: float = 1.0,
                        cap: float = 30.0) -> float:
    """ak-wty backoff: exponential 1s → 2s → 4s → …, capped.

    attempt is 0-indexed:
      attempt=0 → delay 1s before the first retry
      attempt=1 → delay 2s before the second retry
      attempt=2 → delay 4s before the third retry

    The cap prevents pathological backoff on a very high retry
    count (though MAX_RETRIES enforces the ceiling anyway).
    """
    if attempt < 0:
        attempt = 0
    delay = base * (2 ** attempt)
    return min(delay, cap)
