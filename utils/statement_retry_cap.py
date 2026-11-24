"""ak-7ahz (2026-11-24): retry cap for statement emails that keep failing.

After ak-n0cg / ak-fc7z a statement PDF that can't be ingested is stamped
'failed' and retried every mail cycle (<= 7x/night). With n0cg's fail-safe
ledger rule, a bank's NON-statement PDF (interest / TDS certificate, T&C
notice) or a dormant 0-txn statement now fails 'no_rows' forever and
re-runs a full PDF agent pass each cycle; an unlockable PDF fails
'unlock_failed' forever. There was no terminal state.

Rule: after MAX_IDENTICAL_FAILURES consecutive failures with the SAME
reason code (ak-fc7z's unlock_failed | ingest_incomplete | no_rows), the
email is stamped NEEDS_ATTENTION, which automatic processing skips. A
different reason resets the run to 1 (something changed). Success clears
it. A manual retry (POST /retryStatementEmail) puts it back to 'failed'
with the counter cleared.

Storage, NO migration: the counter lives in the existing
processedEmails.extraction_summary JSON under ATTEMPTS_KEY, and status is
the existing String(20) column ('needs_attention' = 15 chars). So the
code works on the current schema, with no ALTER to gate on. Leaf module.
"""
from __future__ import annotations

import datetime as _dt

MAX_IDENTICAL_FAILURES = 3
NEEDS_ATTENTION = "needs_attention"
ATTEMPTS_KEY = "_attempts"
# Statuses automatic processing must not pick up again.
TERMINAL_DB_STATUSES = ("processed", NEEDS_ATTENTION)


def previous_attempts(extraction_summary):
    """{'reason', 'count', 'last_at'} from a stored summary, or None."""
    if not isinstance(extraction_summary, dict):
        return None
    a = extraction_summary.get(ATTEMPTS_KEY)
    if (isinstance(a, dict) and isinstance(a.get("reason"), str)
            and isinstance(a.get("count"), int) and not isinstance(a.get("count"), bool)):
        return a
    return None


def next_stamp(status, reason, prev_summary, now=None):
    """(status_to_stamp, attempts_or_None).

    status / reason are ak-fc7z's final_email_status output ('success' |
    'error', reason code or None). prev_summary is the row's stored
    extraction_summary (None for a first attempt).
    """
    if status != "error" or not reason:
        return status, None
    prev = previous_attempts(prev_summary)
    count = prev["count"] + 1 if prev and prev["reason"] == reason else 1
    attempts = {
        "reason": reason,
        "count": count,
        "last_at": (now or _dt.datetime.utcnow()).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if count >= MAX_IDENTICAL_FAILURES:
        return NEEDS_ATTENTION, attempts
    return "error", attempts


def with_attempts(summary, attempts):
    """The summary to store: the agent's metadata plus the counter."""
    out = dict(summary) if isinstance(summary, dict) else {}
    out.pop(ATTEMPTS_KEY, None)
    if attempts:
        out[ATTEMPTS_KEY] = attempts
    return out or None
