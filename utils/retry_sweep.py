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
  * last stamped within RETRY_WINDOW_DAYS,
  * newest first, at most MAX_RETRIES_PER_RUN per cycle.
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


def select_retry_ids(rows, now=None, exclude=()):
    """rows: iterables of (gmail_id, status, processing_type, processed_at).
    Returns the gmail ids to re-fetch this cycle, newest first."""
    now = now or _dt.datetime.utcnow()
    start = window_start(now)
    stale = now - _dt.timedelta(hours=PENDING_STALE_HOURS)
    excluded = set(exclude)
    picked = []
    for gid, status, ptype, at in rows:
        if not gid or gid in excluded or not isinstance(at, _dt.datetime):
            continue
        if at < start:
            continue
        if status == "failed" and ptype == "pdf":
            picked.append((at, gid))
        elif status == "pending" and at <= stale:
            picked.append((at, gid))
    picked.sort(reverse=True)
    out, seen = [], set()
    for _at, gid in picked:
        if gid not in seen:
            seen.add(gid)
            out.append(gid)
        if len(out) >= MAX_RETRIES_PER_RUN:
            break
    return out
