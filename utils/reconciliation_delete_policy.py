"""ak-a0m: reconciliation delete policy — zero-loss critical.

When a statement arrives for a (user, bank, period) window, the
reconciliation service used to delete overlapping rows to keep the
ledger clean. Two policy bugs got layered on top of that:

  ak-a0m v1 caught the FIRST:
    Old: "delete every statement-source row for this (user, bank,
         period), EXCEPT rows in the current file_id".
    Bug: legitimate PRIOR statements covering overlapping periods
         (e.g. HDFC ships a quarterly summary overlapping three
         monthly statements) get their rows nuked when a new
         statement arrives. Hard zero-loss violation.
    Fix (v1): DELETED the cross-file statement-delete entirely.
              Statement-source rows are always protected.

  ak-a0m v2 catches the SECOND (reviewer BOUNCE hq-wisp-35ufm6):
    Old (post-v1): "delete every email-source alert for this
                    (user, bank, period) unconditionally".
    Bug: an email alert only has a canonical statement replacement
         if a statement row for the SAME tx actually landed. If the
         statement is under-parsed (missed rows) OR the tx is
         alert-only (never appears on a statement), the alert gets
         silently deleted with nothing replacing it. Silent loss.
    Fix (v2): match-before-delete. Only delete email alerts whose
              exact tx has a matching statement row in the same
              period. See is_email_matched_by_statement below.

  Rationale (symmetric zero-loss):
    - email-source alerts are only "replaceable" if the statement
      actually contains the matching tx. Preserve on zero-match.
    - statement-source rows are the canonical ledger; never
      cross-file deleted.
    - chunk-overlap re-reads of the SAME statement collapse via
      ak-8l5 dedup at insert time — no delete required for either.

Match strategy (v2):
    1. Primary: bank_reference_id — same key ak-8l5 uses for tx
       identity. Fast, exact, no false positives.
    2. Fallback: (bank, date_iso, amount_2dp, normalized_desc) tuple
       for rows that don't carry a bank_reference_id (extractor
       drift, ref-less legacy tx). Same normalization shape as ak-8l5's
       backstop so a single source of truth exists.

The helpers below are used by services/reconciliationService.py's
replace_email_transactions_with_statement to decide, at query time,
which rows to delete. Pure Python so it can be tested without booting
flask/SQLAlchemy.
"""

from __future__ import annotations

from typing import Optional


def should_delete_on_statement_arrival(
    source: Optional[str], file_id: Optional[str],
) -> bool:
    """ak-a0m policy: return True iff the given row is a candidate
    for deletion when a new statement arrives for its
    (user, bank, period) window.

    Contract:
      - Row is deletable IFF source='email' (case-insensitive) AND
        fileID is null. This is the "email alert" pattern; the
        canonical replacement is the statement covering the same
        transaction.
      - Statement-source rows (source='statement', case-insensitive)
        are NEVER deleted, regardless of fileID. This is the
        cross-file protection ak-a0m adds.
      - Rows with an unrecognized / missing source are treated as
        NOT deletable (defensive: better to preserve than to drop
        based on a NULL / unexpected classifier).

    Examples:
      >>> should_delete_on_statement_arrival("email", None)
      True
      >>> should_delete_on_statement_arrival("Email", None)   # case-insens
      True
      >>> should_delete_on_statement_arrival("statement", "file_A")
      False
      >>> should_delete_on_statement_arrival("statement", None)
      False
      >>> should_delete_on_statement_arrival("email", "file_A")
      False   # email + file_id is not a normal shape; preserve.
      >>> should_delete_on_statement_arrival(None, None)
      False   # unknown source; preserve.
    """
    if not source:
        return False
    if source.strip().lower() != "email":
        return False
    if file_id is not None:
        # email rows with a non-null fileID are unusual (the email-
        # ingest path doesn't attach a fileID). Preserve them — the
        # zero-loss preference is always "keep, don't drop".
        return False
    return True


# ── SQL filter helpers ───────────────────────────────────────────────
#
# When we want the same policy expressed as a SQLAlchemy filter list
# (for a bulk DELETE), the caller uses this helper so the policy
# stays in one place. Pure-Python + SQLAlchemy imports; the callers
# already carry the ORM dependencies.


def build_email_deletion_filters(
    txn_model,
    user_id: str,
    bank: str,
    period_start,
    period_end,
    *,
    func=None,
):
    """ak-a0m helper: assemble the SQLAlchemy filter list that
    selects CANDIDATE rows for match-before-delete on statement
    arrival.

    Returns a list of filter expressions equivalent to:
      user = user_id
      AND bank = bank
      AND LOWER(TRIM(source)) = 'email'    # ak-a0m v2: matches
                                            # should_delete_on_
                                            # statement_arrival's
                                            # source.strip().lower()
                                            # predicate exactly
      AND fileID IS NULL
      AND date BETWEEN period_start AND period_end

    ak-a0m v2: this returns CANDIDATES, not deletes. The service
    then applies is_email_matched_by_statement per-row and only
    deletes those with a matching statement-side row.

    `func` is the sqlalchemy.func module — accept it as a param so
    the caller doesn't have to re-import it. The tx model class is
    injected too so this helper doesn't force an import from
    models.transactions (which pulls in flask via the metadata).
    """
    if func is None:
        from sqlalchemy import func as _sa_func
        func = _sa_func

    return [
        txn_model.user == user_id,
        txn_model.bank == bank,
        # ak-a0m v2 MINOR fix: SQL now trims + lowers to align with
        # the Python predicate's source.strip().lower() shape. Prior
        # divergence gave false confidence in unit tests when the
        # source column had leading/trailing whitespace.
        func.lower(func.trim(txn_model.source)) == "email",
        txn_model.fileID.is_(None),
        txn_model.date.between(period_start, period_end),
    ]


def build_statement_lookup_filters(
    txn_model,
    user_id: str,
    bank: str,
    period_start,
    period_end,
    *,
    func=None,
):
    """ak-a0m v2 helper: assemble the SQLAlchemy filter list that
    selects statement-source rows for the match-before-delete
    lookup. Same shape as build_email_deletion_filters but for
    the statement side:

      user = user_id
      AND bank = bank
      AND LOWER(TRIM(source)) = 'statement'
      AND date BETWEEN period_start AND period_end

    Note the ABSENCE of a fileID filter — statement rows can carry
    any fileID (or none), and we scope the match set to whatever's
    in the period.
    """
    if func is None:
        from sqlalchemy import func as _sa_func
        func = _sa_func

    return [
        txn_model.user == user_id,
        txn_model.bank == bank,
        func.lower(func.trim(txn_model.source)) == "statement",
        txn_model.date.between(period_start, period_end),
    ]


# ── ak-a0m v2: match-before-delete helpers ──────────────────────────
#
# Reviewer BOUNCE hq-wisp-35ufm6: a blanket email-side delete is
# silent loss when the statement doesn't cover the alert (under-parse
# OR genuinely alert-only tx). Symmetric zero-loss requires matching
# each email candidate against the statement side before deleting.
#
# Primary match: bank_reference_id (ak-8l5's stable tx identity).
# Fallback match: (bank, date, amount, normalized-description) tuple
# for rows without a ref.


import re as _re

_WS_RE = _re.compile(r"\s+")
_TRAILING_PUNCT = ".,:;"


def normalize_details_for_match(details) -> str:
    """ak-a0m v2: whitespace-collapse + upper + trailing-punct-strip.

    Matches ak-8l5's normalize_description_for_backstop shape so the
    fallback tuple match and the ak-8l5 backstop use the same
    normalization convention. Reimplemented locally (rather than
    imported from utils.reference_id) so this module stays
    framework-free — the reference_id module pulls hashlib but not
    flask, so a future consolidation is fine; today's cost is 4
    lines of Python.
    """
    text = (details or "").strip()
    text = _WS_RE.sub(" ", text)
    text = text.upper()
    text = text.rstrip(_TRAILING_PUNCT)
    return text


def _date_key(dt) -> str:
    """Stringify a date/datetime for the tuple index. Uses the ISO
    date part (YYYY-MM-DD) so a statement row with time=00:00 still
    matches an email row with a full timestamp on the same day."""
    if dt is None:
        return ""
    if hasattr(dt, "date") and callable(dt.date):
        try:
            return dt.date().isoformat()
        except Exception:  # pragma: no cover — defensive
            pass
    if hasattr(dt, "isoformat"):
        try:
            iso = dt.isoformat()
            # Trim time portion if present.
            return iso.split("T", 1)[0] if "T" in iso else iso[:10]
        except Exception:  # pragma: no cover
            pass
    return str(dt)[:10]


def _amount_key(amount) -> str:
    """Stringify amount to 2dp for exact tuple matching (float-noise
    tolerant within ₹0.005)."""
    try:
        return f"{float(amount):.2f}"
    except (TypeError, ValueError):
        return ""


def build_statement_match_index(statement_rows):
    """ak-a0m v2: index a statement-side row set for O(1) lookup.

    Returns (by_ref, by_tuple):
      by_ref: dict[bank_reference_id → list[row]] — first-hit list;
              caller can inspect the row(s) or just check presence.
      by_tuple: dict[(bank, date_key, amount_key, normdesc) → list[row]]

    Both indexes accept the same rows so the primary and fallback
    match paths run in one pass over the candidates.
    """
    by_ref: dict = {}
    by_tuple: dict = {}
    for row in statement_rows:
        ref = getattr(row, "bank_reference_id", None)
        if ref:
            by_ref.setdefault(ref, []).append(row)
        key = (
            getattr(row, "bank", None),
            _date_key(getattr(row, "date", None)),
            _amount_key(getattr(row, "amount", None)),
            normalize_details_for_match(getattr(row, "details", "")),
        )
        by_tuple.setdefault(key, []).append(row)
    return by_ref, by_tuple


def is_email_matched_by_statement(
    email_row, by_ref, by_tuple,
) -> bool:
    """ak-a0m v2 predicate: return True iff `email_row` has a
    matching statement row in the pre-built index.

    Primary: bank_reference_id (both sides must carry it AND agree).
    Fallback: (bank, date, amount, normalized_desc) tuple.

    Returns False if neither path hits — the caller preserves the
    email row (zero-loss policy).
    """
    # Primary: ref match.
    ref = getattr(email_row, "bank_reference_id", None)
    if ref and by_ref.get(ref):
        return True

    # Fallback: tuple match.
    key = (
        getattr(email_row, "bank", None),
        _date_key(getattr(email_row, "date", None)),
        _amount_key(getattr(email_row, "amount", None)),
        normalize_details_for_match(getattr(email_row, "details", "")),
    )
    if key in by_tuple:
        return True

    return False
