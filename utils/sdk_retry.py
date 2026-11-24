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

import re


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


# ── Closed-set SDK error codes (ak-sdkerr) ─────────────────────────
# utils.sdk_runner no longer returns raw SDK text as `error` (a result
# error's text can be model output quoting statement / email content).
# It returns `<kind>:<detail>[<marker>]`, e.g.
#   assistant_error:rate_limit[rate_limit]
#   result_error:error_during_execution[overloaded]
#   sdk_exception:ProcessError[sdk_message_reader]
# The marker is classified from the raw text BEFORE it is dropped, using
# the same substrings as the legacy signatures below. Non-retryable
# markers take priority, exactly like is_retryable_sdk_error.
_NON_RETRYABLE_MARKERS = (
    ("schema", ("schema",)),
    ("invalid_request", ("invalid_request",)),
    ("authentication", ("authentication", "unauthorized")),
    ("permission", ("permission", "forbidden")),
    ("not_found", ("not found",)),
    ("max_turns", ("maximum number of turns", "max_turns")),
)
_RETRYABLE_MARKERS = (
    ("sdk_message_reader", ("fatal error in message reader", "message reader")),
    ("rate_limit", ("rate_limit", "rate limit")),
    ("overloaded", ("overloaded",)),
    ("timeout", ("timeout", "timed out", "deadline exceeded")),
    ("stream", ("stream", "eof", "broken pipe")),
    ("connection", ("connection",)),
    ("server_error", ("server error", "server_error", "internal error",
                      "temporarily unavailable", "temporary failure")),
)
RETRYABLE_MARKERS = frozenset(m for m, _ in _RETRYABLE_MARKERS)
NON_RETRYABLE_MARKERS = frozenset(m for m, _ in _NON_RETRYABLE_MARKERS)
SDK_ERROR_KINDS = ("assistant_error", "result_error", "sdk_exception")
_CODE_RE = re.compile(
    r"^(assistant_error|result_error|sdk_exception):[A-Za-z0-9_]{1,64}\[([a-z0-9_]+)\]$")
_DETAIL_RE = re.compile(r"^[A-Za-z0-9_]{1,64}$")


def sdk_error_marker(text) -> str:
    """Closed-set marker for raw SDK error text ('none' if no match).
    The text itself is never returned."""
    if not isinstance(text, str) or not text.strip():
        return "none"
    lower = text.lower()
    for marker, subs in _NON_RETRYABLE_MARKERS + _RETRYABLE_MARKERS:
        if any(sub in lower for sub in subs):
            return marker
    return "none"


def sdk_error_code(kind: str, detail, raw_text=None) -> str:
    """Build `<kind>:<detail>[<marker>]`. `detail` must already be a
    closed-set token (SDK error enum, result subtype, exception type);
    anything else collapses to 'other'. `raw_text` is only classified.

    ak-aufe review: the marker comes from `raw_text` ONLY, never from the
    detail token. An exception TYPE name such as CLIConnectionError must
    not by itself make "Failed to start Claude Code" look retryable."""
    if kind not in SDK_ERROR_KINDS:
        kind = "sdk_exception"
    d = detail if isinstance(detail, str) and _DETAIL_RE.match(detail) else "other"
    marker = sdk_error_marker(raw_text or "")
    return f"{kind}:{d}[{marker}]"


_CODE_SEARCH_RE = re.compile(
    r"(?:assistant_error|result_error|sdk_exception):[A-Za-z0-9_]{1,64}\[([a-z0-9_]+)\]")
STREAM_MARKERS = frozenset({"stream", "connection", "sdk_message_reader"})


def sdk_error_marker_of(text):
    """The marker of the SDK error code inside `text` (it may be wrapped,
    e.g. 'Claude error: <code>'), or None if `text` carries no code."""
    if not isinstance(text, str):
        return None
    m = _CODE_SEARCH_RE.search(text)
    return m.group(1) if m else None


_FRIENDLY = (
    (("rate_limit", "overloaded"),
     "The assistant is busy right now. Please try again in a minute."),
    (("timeout", "stream", "connection", "sdk_message_reader", "server_error"),
     "The assistant hit a temporary problem. Please try again."),
    (("max_turns",),
     "The assistant ran out of steps for this request. Try a narrower question."),
    (("authentication", "permission"),
     "The assistant couldn't authenticate. Check the server's assistant credentials."),
)
_FRIENDLY_DEFAULT = "The assistant ran into an error. Please try again."


def sdk_error_user_message(code) -> str:
    """ak-95w0: fixed, user-facing text for a closed-set SDK error code
    (never echoes the code's source text, which is already dropped)."""
    marker = sdk_error_marker_of(code) or "none"
    for markers, text in _FRIENDLY:
        if marker in markers:
            return text
    return _FRIENDLY_DEFAULT


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
    # ak-sdkerr: a closed-set code -> decide on its marker alone.
    m = _CODE_RE.match(error_msg.strip())
    if m:
        return m.group(2) in RETRYABLE_MARKERS

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
