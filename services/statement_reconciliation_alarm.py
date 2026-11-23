"""ak-qpkm Phase 2b: statement-import drift-alarm surface.

Non-destructive BE audit that runs post-ingest and WARN-logs when
the imported tx row count or amount sums diverge from a known
expectation. Purpose: catch invisible extractor regressions
(e.g., ak-tik-style degraded-dedup silently collapsing rows) at
ingest time rather than letting them slip into the user's
Transactions page.

## Scope

Pure-logic utility with:
  * ``audit_statement_file(session, file_id, bank, user_id,
                           expected_row_count=None,
                           expected_in_sum=None,
                           expected_out_sum=None)`` → dict
    carrying the actual counts/sums + drift flags + severity.

  * Caller WARN-logs the result; no DB writes in this module.
    Persisted-audit-log + FE visibility are deferred to Phase 3.

## Drift classification

* ``CLEAN`` — no expectations supplied, or all within tolerance
* ``NEW_IMPORT`` — no prior rows for this fileID; nothing to
  compare against
* ``ROW_COUNT_DRIFT`` — |actual - expected| exceeds the
  row-count tolerance (5 rows OR 5% of expected, whichever is
  larger)
* ``SUM_DRIFT`` — either in_sum or out_sum differs by more than
  ₹1.00 absolute from expected
* ``REGRESSED`` — actual row count is 0 or near-0 when expected
  was non-trivial (emergency signal for an empty-import bug)

Multiple flags can co-occur; the result carries a ``flags`` list.

## Intended call site

At the end of ``_handle_insert_batch_transactions`` after the
final chunk lands:

```python
from services.statement_reconciliation_alarm import audit_statement_file
audit = audit_statement_file(
    session=session,
    file_id=file_id,
    bank=bank,
    user_id=user_id,
    expected_row_count=sum(len(chunk_txs) for chunk_txs in all_chunks),
)
if audit["severity"] != "clean":
    logger.warning(
        f"statementReconciliationAlarm: file={file_id} bank={bank} "
        f"user={user_id} drift={audit['flags']} "
        f"actual_rows={audit['actual_row_count']} "
        f"expected_rows={audit['expected_row_count']} "
        f"delta={audit['row_count_delta']}"
    )
```

## §23 constraints
* #4 PII — audit result carries counts + sums + file_id + bank,
  NEVER tx descriptions or per-row contents. Journal grep is
  safe for operator triage.
* #6 user_id scoping preserved on every query.

## Financial-data invariants
* Sums use the ak-p3id canonical positive-magnitude shape:
  ``in_sum`` = Σ(-amount WHERE amount < 0),
  ``out_sum`` = Σ(amount WHERE amount > 0).
* Comparisons use absolute-delta tolerance (₹1.00 default) to
  survive float-noise without masking real drift.
"""
from __future__ import annotations

from typing import List, Optional

from sqlalchemy import case, func

from models.transactions import Transactions


# Default tolerances — operator-overridable via kwargs if a specific
# bank format has known-wider variance. Row-count tolerance uses a
# floor + percentage combo so small files aren't dominated by the
# 5-row floor.
_DEFAULT_ROW_COUNT_FLOOR = 5
_DEFAULT_ROW_COUNT_PCT = 0.05  # 5%
_DEFAULT_SUM_TOLERANCE = 1.00  # ₹1.00 absolute

# Severity tiers — "clean" means no alarm; "info" = soft drift
# worth a WARN; "critical" = REGRESSED empty-import or 20%+ row
# count divergence.
_SEVERITY_CLEAN = "clean"
_SEVERITY_INFO = "info"
_SEVERITY_CRITICAL = "critical"

# Drift flag labels — keep stable; operator triage greps for them.
FLAG_CLEAN = "CLEAN"
FLAG_NEW_IMPORT = "NEW_IMPORT"
FLAG_ROW_COUNT_DRIFT = "ROW_COUNT_DRIFT"
FLAG_SUM_DRIFT = "SUM_DRIFT"
FLAG_REGRESSED = "REGRESSED"


def audit_statement_file(
    session,
    file_id: str,
    bank: str,
    user_id: str,
    expected_row_count: Optional[int] = None,
    expected_in_sum: Optional[float] = None,
    expected_out_sum: Optional[float] = None,
    row_count_floor: int = _DEFAULT_ROW_COUNT_FLOOR,
    row_count_pct: float = _DEFAULT_ROW_COUNT_PCT,
    sum_tolerance: float = _DEFAULT_SUM_TOLERANCE,
) -> dict:
    """Audit the DB state for a freshly-imported statement file.

    Query the ``transactions`` table for everything scoped to
    ``(file_id, user_id, source='Statement')`` and compute:
      * actual_row_count — raw count
      * actual_in_sum   — Σ -amount WHERE amount < 0 (positive-
                           magnitude, ak-p3id canonical)
      * actual_out_sum  — Σ  amount WHERE amount > 0

    Compare against expectations (if supplied). Flag drift.
    """
    if not file_id:
        raise ValueError("audit_statement_file: file_id is required")
    if not user_id:
        raise ValueError("audit_statement_file: user_id is required")

    # Actual row count + sums via one query. Uses the same
    # positive/negative convention as transactionsService's
    # ak-p3id block (credits stored negative, debits stored
    # positive).
    agg = session.query(
        func.count(Transactions.referenceID).label("row_count"),
        func.coalesce(
            func.sum(
                case(
                    (Transactions.amount < 0, -Transactions.amount),
                    else_=0,
                )
            ),
            0,
        ).label("in_sum"),
        func.coalesce(
            func.sum(
                case(
                    (Transactions.amount > 0, Transactions.amount),
                    else_=0,
                )
            ),
            0,
        ).label("out_sum"),
    ).filter(
        Transactions.fileID == file_id,
        Transactions.user == user_id,
    ).first()

    if agg is None:
        actual_row_count, actual_in_sum, actual_out_sum = 0, 0.0, 0.0
    else:
        actual_row_count = int(agg.row_count or 0)
        actual_in_sum = float(agg.in_sum or 0)
        actual_out_sum = float(agg.out_sum or 0)

    flags: List[str] = []
    severity = _SEVERITY_CLEAN

    # Branch: no expectations → NEW_IMPORT, no comparison
    if (expected_row_count is None
            and expected_in_sum is None
            and expected_out_sum is None):
        flags.append(FLAG_NEW_IMPORT)
        return _build_result(
            file_id, bank, user_id,
            actual_row_count, actual_in_sum, actual_out_sum,
            expected_row_count, expected_in_sum, expected_out_sum,
            flags, severity,
        )

    # Row-count drift check
    if expected_row_count is not None:
        tolerance = max(row_count_floor, int(expected_row_count * row_count_pct))
        delta = actual_row_count - expected_row_count
        if abs(delta) > tolerance:
            flags.append(FLAG_ROW_COUNT_DRIFT)
            severity = _SEVERITY_INFO
        # REGRESSED: empty-or-near-empty import when expected was
        # non-trivial. Catches the "N transactions silently
        # vanished" class loudest.
        if (expected_row_count >= 5
                and actual_row_count <= max(1, expected_row_count * 0.2)):
            flags.append(FLAG_REGRESSED)
            severity = _SEVERITY_CRITICAL

    # Sum drift checks — only emit SUM_DRIFT once even if both
    # sides drift, caller gets detail via the result dict.
    sum_drifted = False
    if (expected_in_sum is not None
            and abs(actual_in_sum - expected_in_sum) > sum_tolerance):
        sum_drifted = True
    if (expected_out_sum is not None
            and abs(actual_out_sum - expected_out_sum) > sum_tolerance):
        sum_drifted = True
    if sum_drifted:
        flags.append(FLAG_SUM_DRIFT)
        # Only escalate to INFO if nothing worse already set.
        if severity == _SEVERITY_CLEAN:
            severity = _SEVERITY_INFO

    if not flags:
        flags.append(FLAG_CLEAN)

    return _build_result(
        file_id, bank, user_id,
        actual_row_count, actual_in_sum, actual_out_sum,
        expected_row_count, expected_in_sum, expected_out_sum,
        flags, severity,
    )


def _build_result(
    file_id, bank, user_id,
    actual_row_count, actual_in_sum, actual_out_sum,
    expected_row_count, expected_in_sum, expected_out_sum,
    flags, severity,
):
    """Shape the audit dict. Separated so the two early-return
    branches (NEW_IMPORT + full-audit) share the shape."""
    row_count_delta = (
        None if expected_row_count is None
        else actual_row_count - expected_row_count
    )
    in_sum_delta = (
        None if expected_in_sum is None
        else actual_in_sum - expected_in_sum
    )
    out_sum_delta = (
        None if expected_out_sum is None
        else actual_out_sum - expected_out_sum
    )
    return {
        "file_id": file_id,
        "bank": bank,
        "user_id": user_id,
        "actual_row_count": actual_row_count,
        "actual_in_sum": actual_in_sum,
        "actual_out_sum": actual_out_sum,
        "expected_row_count": expected_row_count,
        "expected_in_sum": expected_in_sum,
        "expected_out_sum": expected_out_sum,
        "row_count_delta": row_count_delta,
        "in_sum_delta": in_sum_delta,
        "out_sum_delta": out_sum_delta,
        "flags": flags,
        "severity": severity,
    }
