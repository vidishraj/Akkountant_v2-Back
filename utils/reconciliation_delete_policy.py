"""ak-a0m: reconciliation delete policy — zero-loss critical.

When a statement arrives for a (user, bank, period) window, the
reconciliation service used to delete overlapping rows to keep the
ledger clean. The pre-ak-a0m implementation had a policy bug:

  Old: "delete every statement-source row for this (user, bank,
       period), EXCEPT rows in the current file_id".
  Bug: legitimate PRIOR statements covering overlapping periods
       (e.g. HDFC ships a quarterly summary that overlaps three
       monthly statements) get their rows nuked when the new
       statement arrives. This is a hard zero-loss violation — the
       Overseer's constraint requires no transaction ever silently
       vanishes.

Root cause of F4/F7/F9 silent row deletions in the ak-32o re-parse
run (131 rows this batch, per infra's [DIAG 5 ANOMALIES]).

  New (this module):
    Only rows that are BOTH source='email' AND fileID IS NULL are
    replaceable on statement arrival. Statement-source rows —
    regardless of which file — are NEVER cross-file deleted. Chunked
    re-reads of the SAME statement are already handled by ak-8l5's
    referenceID PK collision path; no delete is needed for them
    either.

  Rationale (verbatim from the review discussion):
    - email-source alerts (source='email', fileID=null) are the
      only rows whose canonical replacement is a statement. Overwrite.
    - statement-source rows (source='statement', fileID=<anything>)
      are the canonical ledger; never overwrite. If two statements
      legitimately cover overlapping periods, both stay.
    - chunk-overlap re-reads of the SAME statement collapse via
      ak-8l5 dedup at insert time — no delete required.

The function below is used by services/reconciliationService.py's
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
    selects rows deletable on statement arrival.

    Returns a list of filter expressions equivalent to:
      user = user_id
      AND bank = bank
      AND LOWER(source) = 'email'
      AND fileID IS NULL
      AND date BETWEEN period_start AND period_end

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
        func.lower(txn_model.source) == "email",
        txn_model.fileID.is_(None),
        txn_model.date.between(period_start, period_end),
    ]
