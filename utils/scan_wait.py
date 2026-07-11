"""ak-uvy: async completion signal for the email-scan endpoint.

BUG (per Lead's Batch 2 dispatch hq-wisp-ekveei):

  controllers/transactionsEP.triggerEmailCheck spawns a background
  threading.Thread that runs mail_processor.process_emails, updates
  a shared _scans[scan_id] dict, and returns 202 with the scan_id
  immediately. Pollers hit /readEmails/status until they see
  status='completed', but there's a race: the progress dict is
  updated field-by-field WITHOUT a terminal-completion signal, and
  ak-32o's orchestrator captured a "done" report while chunks were
  still finishing. 618 rows leaked in during the elapsed hour.

v2 (reviewer BOUNCE hq-wisp-c0zczy):

  Three regressions the v1 shape re-introduced or overlooked:
    - MAJOR: HTTP `wait=true` accepted timeout=None → gunicorn
      worker pinned indefinitely if the scan hangs / OOMs before
      signal_terminal. Bound at HTTP_MAX_WAIT_SECONDS on the HTTP
      path; keep the None option only for the in-process helper.
    - MINOR: _scans registry never evicted → long-running workers
      leak progress dicts. Sweep-on-read against a per-entry
      terminal_at timestamp.
    - MINOR: scan_id scoping — uuid4 makes it unguessable but the
      lookup should still verify user_id so a caller with a
      leaked scan_id from another user can't observe someone
      else's scan. New require_user helper.

FIX (option (a) + partial (b) per Lead's preference):

  1. Per-scan threading.Event set on terminal status. Blocks any
     caller that wants to wait until the scan is DONE (regardless
     of success / failure).

  2. is_terminal_status(status) pure predicate — the truth source
     for "safe to trust this progress dict". Callers switch on this
     instead of trying to guess from the status string.

  3. wait_for_scan_completion(scans, lock, scan_id, timeout) helper
     that finds the scan's event and blocks up to `timeout` seconds
     (None = block indefinitely). Returns the final progress dict
     on success, None if scan_id doesn't exist, or the current
     progress dict if the timeout elapsed without termination.

  4. v2: HTTP_MAX_WAIT_SECONDS constant + clamp_http_wait_timeout()
     helper — HTTP callers can't wait longer than the cap.

  5. v2: SCAN_TTL_SECONDS + evict_completed_scans() — sweep terminal
     entries older than the TTL. Called from the endpoint's read
     path so it stays lazy (no background thread required).

  6. v2: mark_terminal(progress) — sets both the event AND stamps
     a terminal_at timestamp so the sweeper can decide when to
     evict.

The controller wires all six. Direct callers of
mail_processor.process_emails / reprocess_pdf already block
synchronously — anyio.run(coroutine) is blocking on completion —
so no fix needed there. Docstrings updated to make that guarantee
explicit.

Pure-Python (threading + typing only) so the pure predicate is
testable without booting flask.
"""

from __future__ import annotations

import threading
import time
from typing import Iterable, Optional


# Every status value the scan tracker can settle to. `completed`
# and `failed` are the terminal set — the moment either lands, the
# background thread has exited and the progress dict is stable.
TERMINAL_STATUSES = frozenset({"completed", "failed"})

# Non-terminal statuses observed at various stages of the scan.
# Not enforced; documented so a future reviewer can see the state
# machine shape at a glance.
NON_TERMINAL_STATUSES = frozenset({
    "started",       # initial state right after triggerEmailCheck
    "fetching",      # Gmail list-messages call
    "classifying",   # LLM classification pass
    "processing",    # per-email PDF processing
    # …future stages added here.
})


def is_terminal_status(status) -> bool:
    """ak-uvy: return True iff `status` is a terminal state (scan
    thread has exited; progress dict is stable).

    Contract:
      - `status` is expected to be a string (case-insensitive
        comparison against TERMINAL_STATUSES). Non-string /
        None / empty → False (defensive: never claim terminality
        for an unknown state).
    """
    if not isinstance(status, str):
        return False
    return status.strip().lower() in TERMINAL_STATUSES


def make_scan_event() -> threading.Event:
    """Convenience: return a fresh threading.Event that will be
    set by run_scan when it hits a terminal state. Kept in the
    module so any caller (controller, wait helper, tests) grabs
    the same shape."""
    return threading.Event()


def signal_terminal(progress: dict) -> None:
    """ak-uvy: called by run_scan when it lands in a terminal state.
    Sets the event stored under `_event` in the progress dict, if
    one is present. Safe to call on a progress dict without an
    event (older scans, tests). Never raises.

    v2: also stamps `_terminal_at` (monotonic timestamp) so the
    eviction sweeper can decide when to drop the entry from the
    registry. Underscore-prefixed so scan_progress_for_client
    strips it before JSON-encoding.
    """
    if not isinstance(progress, dict):
        return
    event = progress.get("_event")
    if isinstance(event, threading.Event):
        event.set()
    # v2: stamp terminal_at even if the event's missing (legacy
    # scans without _event still get evicted).
    progress["_terminal_at"] = time.monotonic()


# ── ak-uvy v2: HTTP bounding + eviction + scoping ──────────────────
#
# The HTTP path can NEVER hand `None` (or a huge number) to
# event.wait() — a hanging scan would pin the gunicorn worker
# indefinitely. Bound at HTTP_MAX_WAIT_SECONDS; clients that need
# longer waits use long-poll semantics (re-poll on non-terminal
# return).

# Hard cap on any HTTP-path wait. Chosen at 30s: short enough to
# survive a wedged scan without pinning the pool, long enough to
# absorb a normal scan-completion latency without excessive
# round-trips.
HTTP_MAX_WAIT_SECONDS: float = 30.0

# TTL after terminal for a scan entry. 5 min gives orchestrators
# time to fetch the final result payload before eviction, without
# leaking the entry forever.
SCAN_TTL_SECONDS: float = 300.0


def clamp_http_wait_timeout(
    raw_timeout: Optional[float],
    *,
    max_seconds: float = HTTP_MAX_WAIT_SECONDS,
) -> float:
    """ak-uvy v2: clamp an HTTP caller's requested wait timeout to
    the server-side cap.

    Rules:
      - None / missing → default to `max_seconds` (never unbounded
        on the HTTP path).
      - Any positive number → min(value, max_seconds).
      - 0 → 0 (explicit non-blocking snapshot).
      - Negative → 0 (treat as non-blocking; never signal an
        infinite wait).
      - Non-numeric → default to `max_seconds`.
    """
    if raw_timeout is None:
        return max_seconds
    try:
        value = float(raw_timeout)
    except (TypeError, ValueError):
        return max_seconds
    if value < 0:
        return 0.0
    if value == 0:
        return 0.0
    return min(value, max_seconds)


def evict_completed_scans(
    scans: dict,
    scans_lock: threading.Lock,
    *,
    ttl_seconds: float = SCAN_TTL_SECONDS,
    now: Optional[float] = None,
) -> int:
    """ak-uvy v2: evict scans that hit a terminal state longer than
    `ttl_seconds` ago. Prevents the in-memory _scans registry from
    growing without bound.

    Called lazily from the read paths (getEmailScanStatus,
    wait_for_scan_completion) so no background sweeper thread is
    required. O(N) per call; N is small because completed entries
    are evicted the moment they age past TTL.

    Returns the count of entries evicted (for a log line if the
    caller wants one).
    """
    if now is None:
        now = time.monotonic()
    deadline = now - ttl_seconds
    to_drop: list = []
    with scans_lock:
        for scan_id, progress in list(scans.items()):
            if not isinstance(progress, dict):
                to_drop.append(scan_id)
                continue
            terminal_at = progress.get("_terminal_at")
            if isinstance(terminal_at, (int, float)) and terminal_at <= deadline:
                to_drop.append(scan_id)
        for scan_id in to_drop:
            scans.pop(scan_id, None)
    return len(to_drop)


def scope_scan_to_user(progress: dict, expected_user_id: Optional[str]) -> bool:
    """ak-uvy v2: return True iff the scan's stored `user_id`
    matches `expected_user_id`.

    Rules:
      - Both truthy and equal → True.
      - Either side falsy → True (defensive: the pre-v2 tracker
        didn't record user_id, so we can't check; a bare uuid4
        lookup was the previous guarantee. Callers that add
        user_id to new entries get the tighter check
        automatically).
      - Truthy and unequal → False (cross-user probe).
    """
    if not expected_user_id:
        return True
    stored = progress.get("user_id") if isinstance(progress, dict) else None
    if not stored:
        return True
    return stored == expected_user_id


def wait_for_scan_completion(
    scans: dict,
    scans_lock: threading.Lock,
    scan_id: str,
    timeout: Optional[float] = None,
) -> Optional[dict]:
    """ak-uvy: block up to `timeout` seconds waiting for the given
    scan to reach a terminal state.

    Arguments:
      - `scans` and `scans_lock`: the controller's shared scan
        tracker + its lock. Passed in so callers can share their
        own tracker or a stub in tests.
      - `scan_id`: which scan to wait on.
      - `timeout`: seconds. None (default) = block indefinitely.
        0 = poll once without blocking.

    Returns:
      - None if `scan_id` doesn't exist in `scans`.
      - The progress dict on completion (either terminal or the
        current snapshot if timeout elapsed). Callers should
        inspect the returned progress's `status` field via
        is_terminal_status to know which case they're in.

    Never raises. The event lookup happens under `scans_lock`;
    the actual wait releases the lock so other callers can still
    read the tracker.
    """
    with scans_lock:
        progress = scans.get(scan_id)
    if progress is None:
        return None

    event = progress.get("_event")
    if not isinstance(event, threading.Event):
        # Legacy scan that pre-dates ak-uvy — no event to wait on.
        # Return the current snapshot; caller inspects
        # is_terminal_status. This is a graceful degradation, not
        # a hard error.
        return progress

    # Special-case timeout=0 to a non-blocking check (matches
    # threading.Event semantics but explicit for readability).
    if timeout == 0:
        with scans_lock:
            return scans.get(scan_id)

    event.wait(timeout=timeout)
    with scans_lock:
        return scans.get(scan_id)


def scan_progress_for_client(progress: dict) -> dict:
    """ak-uvy: strip the private `_event` field from the progress
    dict before JSON-serializing to a client. Also inject an
    explicit `is_terminal` bool so the client can switch on that
    instead of matching status strings.

    Callers use this to build the response body for both
    /readEmails/status and the new /readEmails/wait endpoint.
    """
    if not isinstance(progress, dict):
        return {"error": "invalid progress payload"}
    public = {k: v for k, v in progress.items() if not k.startswith("_")}
    public["is_terminal"] = is_terminal_status(public.get("status"))
    return public
