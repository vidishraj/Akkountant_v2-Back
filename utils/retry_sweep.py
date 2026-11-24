"""ak-i3tc (2026-11-24): retry sweep for emails that fell out of the fetch window.

The scheduled mail task (checkMailUnifiedTask) fetches only the LAST 24
HOURS. "Stamped failed → retried next run" therefore lasted about one
night: once an email aged out of the window it was never fetched again,
whatever its status. 53 hdfcbank.net statement rows have sat 'pending'
since March this way. No code in this repo writes 'pending' (the only
writer, processed_emails_upsert, always maps to processed / skipped /
failed), so those rows are a legacy state, treated here as failed once
stale.

Each cycle, besides the 24h window, the mail pipeline re-fetches by Gmail
id the rows select_retry_ids picks:
  * status 'failed' with processing_type 'pdf' (statement ingest; the
    expensive, money-relevant path), or
  * status 'pending' (any type) older than PENDING_STALE_HOURS,
  * last ATTEMPTED within RETRY_WINDOW_DAYS (ak-7ahz's
    _attempts.last_at, else processed_at),
  * OLDEST last attempt first (round-robin), at most MAX_RETRIES_PER_RUN
    per cycle.
Failed PDF rows that age out of the window are surfaced (aged_out_ids →
GET /statementAttention "agedOut"), not silently dropped.
'needs_attention' (ak-7ahz terminal), 'processed' and 'skipped' are never
swept. ak-7ahz's counter bounds the repeats per email: three identical
failures → needs_attention. Rows older than the window (the March ones)
are left for an Overseer-approved one-off recovery, not auto-swept.
Leaf module.
"""
from __future__ import annotations

import datetime as _dt

RETRY_WINDOW_DAYS = 45
PENDING_STALE_HOURS = 6
MAX_RETRIES_PER_RUN = 20
SWEEP_STATUSES = ("failed", "pending")


def window_start(now=None):
    return (now or _dt.datetime.utcnow()) - _dt.timedelta(days=RETRY_WINDOW_DAYS)


def last_attempt_at(processed_at, extraction_summary):
    """ak-i3tc review minor 2: when the row was last ATTEMPTED.

    processed_at is set when the row is first stamped and never moves
    on update; ak-7ahz's counter records each failed attempt as
    extraction_summary["_attempts"]["last_at"] (ISO, UTC 'Z'). Use that
    when present, else processed_at."""
    a = extraction_summary.get("_attempts") if isinstance(extraction_summary, dict) else None
    if isinstance(a, dict) and isinstance(a.get("last_at"), str):
        try:
            return _dt.datetime.strptime(a["last_at"], "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass
    return processed_at if isinstance(processed_at, _dt.datetime) else None


def _rows_with_last(rows):
    for row in rows:
        gid, status, ptype, processed_at = row[:4]
        summary = row[4] if len(row) > 4 else None
        yield gid, status, ptype, last_attempt_at(processed_at, summary)


def select_retry_ids(rows, now=None, exclude=()):
    """rows: (gmail_id, status, processing_type, processed_at[, extraction_summary]).
    Returns the gmail ids to re-fetch this cycle.

    Review minor 1: OLDEST last attempt first, so a backlog larger than
    the per-cycle cap rotates through every row (round-robin: a row just
    attempted moves to the back) instead of the newest N starving the rest.
    """
    now = now or _dt.datetime.utcnow()
    start = window_start(now)
    stale = now - _dt.timedelta(hours=PENDING_STALE_HOURS)
    excluded = set(exclude)
    picked = []
    for gid, status, ptype, last in _rows_with_last(rows):
        if not gid or gid in excluded or last is None or last < start:
            continue
        if status == "failed" and ptype == "pdf":
            picked.append((last, gid))
        elif status == "pending" and last <= stale:
            picked.append((last, gid))
    picked.sort()
    out, seen = [], set()
    for _last, gid in picked:
        if gid not in seen:
            seen.add(gid)
            out.append(gid)
        if len(out) >= MAX_RETRIES_PER_RUN:
            break
    return out


def aged_out_ids(rows, now=None):
    """Review minor 2: failed PDF rows whose last attempt is OLDER than the
    window. The sweep no longer retries them, so they are surfaced
    (statementAttention 'agedOut') instead of going silent. Legacy
    'pending' rows (the March backfill) are deliberately not listed;
    their recovery is a one-off."""
    start = window_start(now)
    return [gid for gid, status, ptype, last in _rows_with_last(rows)
            if gid and status == "failed" and ptype == "pdf"
            and last is not None and last < start]
