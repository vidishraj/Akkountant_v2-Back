"""ak-n0cg / ak-v3mf m3 (2026-11-24): server-side sanity for values the
mail agents extract from attacker-influenced email bodies.

The mail agents read untrusted email/PDF content and pass extracted
values to our MCP tools. ak-m0f6 + ak-jmbw removed every confidentiality
path; what remains is INTEGRITY: a prompt-injected email could make the
agent write fabricated ledger rows. These checks run inside the tool
handlers (they apply in every permission mode) before anything is
written:

  * amount: numeric, finite, |amount| <= MAX_ABS_AMOUNT (zero allowed
    for ledger rows: a statement can carry a real Rs 0.00 line);
  * date: parseable by the app's own date formats
    (utils.DateTimeUtil.datetime_formats, the list the insert path
    parses with), OR one of the common bank spellings in
    EXTRA_INPUT_FORMATS / ISO-8601 with 'T', which are NORMALISED to
    a canonical string the insert path accepts; within
    [MIN_DATE, today + MAX_FUTURE_DAYS];
  * caps: rows per batch call, rows per source email.

Leaf module: no Flask/DB imports.
"""
from __future__ import annotations

import datetime as _dt
import math

MAX_ABS_AMOUNT = 1e9
MIN_DATE = _dt.date(1990, 1, 1)
MAX_FUTURE_DAYS = 31
# A monthly bank statement has a few hundred rows; this only stops a
# runaway / injected flood, not real statements.
MAX_BATCH_ROWS = 2000
# ROWS (not calls) per source email per run. An alert email produces
# 1-2 rows; a statement pasted into an email body can carry a few
# hundred, so the headroom is generous. Only a flood trips it.
MAX_ROWS_PER_EMAIL = 500
# Spellings banks commonly use that datetime_formats does not cover.
# strptime month names are case-insensitive, so '24-NOV-26' matches.
EXTRA_INPUT_FORMATS = (
    "%d-%b-%Y", "%d-%b-%y", "%d-%B-%Y", "%d %b %y",
    "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M",
)


def _date_formats():
    from utils.DateTimeUtil import datetime_formats
    return datetime_formats


def check_amount(value, *, field="amount", allow_zero=False,
                 allow_negative=True):
    """Return None if OK, else a short reason (no value echoed)."""
    if isinstance(value, bool):
        return f"{field} is not a number"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return f"{field} is not a number"
    if not math.isfinite(v):
        return f"{field} is not finite"
    if v == 0 and not allow_zero:
        return f"{field} is zero"
    if v < 0 and not allow_negative:
        return f"{field} is negative"
    if abs(v) > MAX_ABS_AMOUNT:
        return f"{field} exceeds the maximum allowed magnitude"
    return None


def normalise_date(value):
    """(string_for_the_insert_path, date) or (None, None).

    A value the app's own formats already parse is returned UNCHANGED
    (row reference hashes include the date string, so rewriting it
    would break dedup against earlier ingests). Other accepted
    spellings are rewritten to '%Y-%m-%d' / '%Y-%m-%d %H:%M:%S'.
    """
    if not isinstance(value, str) or not value.strip():
        return None, None
    s = value.strip()
    for fmt in _date_formats():
        try:
            return s, _dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    parsed = None
    for fmt in EXTRA_INPUT_FORMATS:
        try:
            parsed = _dt.datetime.strptime(s, fmt)
            break
        except ValueError:
            continue
    if parsed is None and "T" in s:
        try:
            parsed = _dt.datetime.fromisoformat(
                s[:-1] + "+00:00" if s.endswith("Z") else s
            ).replace(tzinfo=None)
        except ValueError:
            parsed = None
    if parsed is None:
        return None, None
    if (parsed.hour, parsed.minute, parsed.second) == (0, 0, 0) and ":" not in s:
        return parsed.strftime("%Y-%m-%d"), parsed.date()
    return parsed.strftime("%Y-%m-%d %H:%M:%S"), parsed.date()


def parse_date(value):
    """date or None (see normalise_date)."""
    return normalise_date(value)[1]


def check_date(value, *, field="date", today=None):
    return check_and_normalise_date(value, field=field, today=today)[1]


def check_and_normalise_date(value, *, field="date", today=None):
    """(normalised_string, None) if OK, else (None, reason)."""
    out, d = normalise_date(value)
    if d is None:
        return None, f"{field} is not a recognised date"
    today = today or _dt.date.today()
    if d < MIN_DATE:
        return None, f"{field} is implausibly old"
    if d > today + _dt.timedelta(days=MAX_FUTURE_DAYS):
        return None, f"{field} is too far in the future"
    return out, None


def first_error(*checks):
    for c in checks:
        if c:
            return c
    return None
