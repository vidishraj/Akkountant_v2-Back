"""ak-1rh: structured return contract for reprocess_pdf.

BUG (per Lead's Batch 2 dispatch hq-wisp-ekveei):

  services/mailProcessorService.reprocess_pdf returned
    {"status": "success", "transactions_inserted": <count>}
  UNCONDITIONALLY, ignoring whatever _run_pdf_analysis actually did.
  On ak-32o run 1, all 12 files reported "success" while files
  4 / 7 / 8 / 9 silently produced 0 tx (the chunks had died with
  MySQL 1040 or SDK "Fatal error in message reader"). Callers
  (backfill drivers, the ak-32o orchestrator, admin endpoints)
  had no way to detect the failure without post-hoc DB counting.

FIX:

  Return a structured status keyed off the ak-bwe-v3
  _run_pdf_analysis summary + actual transactions_inserted count.

  {
    "status": "success" | "partial" | "failed" | "empty",
    "gmail_id": <id>,
    "file_id": <resolved>,
    "chunks_ok": int,
    "chunks_failed": int,
    "chunks_total": int,
    "transactions_inserted": int,
    "failures": [
      {"page_range": [start, end], "error_type": <str>, "message": <str>}
    ],
    "processing_mode": <str>,
    "analysis_reason": <str-or-None>,  # populated on failure paths
  }

STATUS ladder:

  - "success": every chunk succeeded (analysis_summary.success=True
    AND no failed_chunks) AND transactions_inserted > 0.
  - "empty": every chunk succeeded AND transactions_inserted == 0.
    Ambiguous — legit empty statement OR silent extractor miss.
    Log-visible in the log field but callers can treat as
    non-critical unless they want stricter.
  - "partial": some chunks failed AND transactions_inserted > 0.
    Ledger has some rows; a re-run will retry the failed chunks
    with ak-8l5 dedup absorbing the already-done ones.
  - "failed": every chunk failed OR analysis_summary.success=False
    with transactions_inserted == 0. Full re-run required.

The status field is what callers switch on. transactions_inserted
+ chunks_* fields carry the raw counts for logging / metrics.

Pure-Python (no framework deps) so it's testable without booting
flask / SQLAlchemy.
"""

from __future__ import annotations

from typing import Any


def _coerce_int(x, default=0) -> int:
    try:
        return int(x)
    except (TypeError, ValueError):
        return default


def _list_or_empty(x) -> list:
    if isinstance(x, list):
        return x
    return []


def summarize_reprocess_result(
    analysis_summary: Any,
    transactions_inserted: int,
) -> dict:
    """ak-1rh: turn an ak-bwe-v3 _run_pdf_analysis summary plus a
    freshly-counted transactions_inserted number into the structured
    reprocess_pdf return dict.

    Contract (mirrors the module docstring):
      - analysis_summary: whatever _run_pdf_analysis returned.
        Expected shape:
          {"success": bool, "failed_chunks": list[[start,end]],
           "total_chunks": int, "reason": str (only on failure)}
        None / non-dict → treated as failure with zero counts.
      - transactions_inserted: caller-provided count of rows in the
        DB after the reprocess (post-any-dedup).

    Return keys (see module docstring for meaning).
    """
    if not isinstance(analysis_summary, dict):
        analysis_summary = {}

    success = bool(analysis_summary.get("success"))
    failed_chunks = _list_or_empty(analysis_summary.get("failed_chunks"))
    total_chunks = _coerce_int(analysis_summary.get("total_chunks"))
    reason = analysis_summary.get("reason")

    chunks_failed = len(failed_chunks)
    chunks_ok = max(total_chunks - chunks_failed, 0)
    tx_count = _coerce_int(transactions_inserted)

    # Status ladder — order matters. Test the failure paths FIRST
    # so a 0-tx run with failed_chunks is classified as failure not
    # empty.
    if not success and chunks_failed > 0 and tx_count == 0:
        status = "failed"
    elif not success and tx_count == 0:
        # analysis reports failure, no chunks tracked (e.g. pdf_open
        # failed before we could split into chunks) — still failed.
        status = "failed"
    elif chunks_failed > 0 and tx_count > 0:
        # Partial: some chunks landed rows, some didn't. Re-run
        # retries the failed ones; ak-8l5 dedup absorbs the done
        # ones.
        status = "partial"
    elif chunks_failed > 0 and tx_count == 0:
        # All chunks failed. Every-chunk-failure with 0 tx.
        status = "failed"
    elif success and tx_count == 0:
        # Everything succeeded but zero rows landed. Ambiguous:
        # legit empty statement OR silent extractor miss. Callers
        # inspect analysis_reason + the log for a determination.
        status = "empty"
    elif success and tx_count > 0:
        status = "success"
    else:
        # Belt-and-braces catch-all. Anything reaching here means
        # the summary was internally inconsistent — treat as
        # failure.
        status = "failed"

    failures = [
        {
            "page_range": pair if isinstance(pair, list) else [pair, pair],
            "error_type": _classify_reason(reason),
            "message": str(reason) if reason else "chunk_failed",
        }
        for pair in failed_chunks
    ]

    return {
        "status": status,
        "chunks_ok": chunks_ok,
        "chunks_failed": chunks_failed,
        "chunks_total": total_chunks,
        "transactions_inserted": tx_count,
        "failures": failures,
        "analysis_reason": reason,
    }


# ── Error classification (best-effort from the summary's reason
# field) ────────────────────────────────────────────────────────────

_ERROR_TYPE_KEYWORDS = (
    ("mysql_1040", ("1040", "too many connections", "max_connections")),
    ("sdk_message_reader", (
        "fatal error in message reader",
        "message reader",
    )),
    ("pdf_password", ("password", "encrypted", "authenticate")),
    ("pdf_open", ("pdf_open", "fitz", "not a pdf", "cannot open")),
    ("extraction_empty", ("returned 0 transactions", "extraction_empty")),
    ("stream", ("stream", "connection reset", "eof")),
    ("rate_limit", ("rate_limit", "rate limit")),
    ("timeout", ("timeout", "deadline exceeded")),
    ("analysis_exception", ("analysis_exception",)),
    ("unknown_chunk_result_shape", ("unknown_chunk_result_shape",)),
)


def _classify_reason(reason) -> str:
    """Best-effort classification of a reason string into one of the
    canonical error_type labels. Falls through to 'unknown' when no
    keyword matches — callers can grep the raw `message` field for
    more detail."""
    if not reason or not isinstance(reason, str):
        return "unknown"
    lower = reason.lower()
    for label, keywords in _ERROR_TYPE_KEYWORDS:
        if any(kw in lower for kw in keywords):
            return label
    return "unknown"
