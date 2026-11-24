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
# ak-6k35: a swept id that never gets stamped (Gmail can't fetch it, or
# the classifier drops it) used to keep its old last attempt and sit at
# the FRONT of the oldest-first queue forever. Each sweep pick records
# extraction_summary[SWEEP_KEY] = {misses, last_swept_at}; any real
# processing stamp replaces the summary (dropping it). After
# MAX_SWEEP_MISSES unanswered sweeps the id is skipped and listed in
# agedOut. Misses only count while no newer attempt (real or a manual
# retry) has happened since the last sweep.
SWEEP_KEY = "_sweep"
MAX_SWEEP_MISSES = 3
_ISO = "%Y-%m-%dT%H:%M:%SZ"


def sql_prefilter(model, now=None):
    """ak-6k35: coarse SQL filter for the sweep query (exact rules are in
    select_retry_ids / aged_out_ids): failed PDF rows of any age (a
    manual retry can revive an old one) + pending rows first stamped
    inside the window."""
    from sqlalchemy import and_, or_
    return or_(
        and_(model.status == "failed", model.processing_type == "pdf"),
        and_(model.status == "pending", model.processed_at >= window_start(now)),
    )


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


def _parse_iso(v):
    try:
        return _dt.datetime.strptime(v, _ISO) if isinstance(v, str) else None
    except ValueError:
        return None


def sweep_misses(summary, last_attempt):
    """(effective misses, last_swept_at). Misses older than the last real
    or manual attempt don't count (that attempt answered them)."""
    sw = summary.get(SWEEP_KEY) if isinstance(summary, dict) else None
    if not isinstance(sw, dict):
        return 0, None
    last_swept = _parse_iso(sw.get("last_swept_at"))
    misses = sw.get("misses") if isinstance(sw.get("misses"), int) else 0
    if last_swept is None or (last_attempt is not None and last_attempt > last_swept):
        return 0, last_swept
    return misses, last_swept


def mark_swept(summary, now=None):
    """The summary to store when the sweep picks a row: misses + 1."""
    now = now or _dt.datetime.utcnow()
    out = dict(summary) if isinstance(summary, dict) else {}
    last = last_attempt_at(None, out)
    misses, _ = sweep_misses(out, last)
    out[SWEEP_KEY] = {"misses": misses + 1, "last_swept_at": now.strftime(_ISO)}
    return out


def _rows_with_last(rows):
    """(gid, status, ptype, queue_key, misses, last_attempt)."""
    for row in rows:
        gid, status, ptype, processed_at = row[:4]
        summary = row[4] if len(row) > 4 else None
        last = last_attempt_at(processed_at, summary)
        misses, last_swept = sweep_misses(summary, last)
        key = max(t for t in (last, last_swept) if t is not None) if (last or last_swept) else None
        yield gid, status, ptype, key, misses, last


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
    for gid, status, ptype, key, misses, last in _rows_with_last(rows):
        if not gid or gid in excluded or last is None or last < start:
            continue
        if misses >= MAX_SWEEP_MISSES:
            continue          # never answered: listed in agedOut instead
        # Queue on the later of last attempt / last sweep, so a row just
        # swept (even one that wasn't stamped) moves to the back.
        if status == "failed" and ptype == "pdf":
            picked.append((key, gid))
        elif status == "pending" and last <= stale:
            picked.append((key, gid))
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
    """Rows the sweep no longer retries, surfaced in statementAttention
    'agedOut' instead of going silent:
      * failed PDF rows whose last attempt is OLDER than the window;
      * (ak-6k35) failed-PDF / pending rows swept MAX_SWEEP_MISSES times
        without ever being fetched or processed.
    Legacy 'pending' rows (the March backfill) are outside the window and
    deliberately not listed; their recovery is a one-off."""
    start = window_start(now)
    out = []
    for gid, status, ptype, _key, misses, last in _rows_with_last(rows):
        if not gid or last is None:
            continue
        if status == "failed" and ptype == "pdf" and last < start:
            out.append(gid)
        elif status in SWEEP_STATUSES and misses >= MAX_SWEEP_MISSES and last >= start:
            out.append(gid)   # ak-6k35: swept N times, never fetched/processed
    return out
