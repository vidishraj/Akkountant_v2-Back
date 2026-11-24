"""ak-fc7z (2026-11-24): the final processedEmails status for a PDF email.

Measured on prod: 43 of 65 'processed' bank-statement PDFs have 0 linked
transaction rows. Two paths stamped them 'processed': the PDF password
unlock failed and the agent ran anyway on a locked file, or no batch
landed ("No transactions found"). n0cg made the service the stamp
authority; this decides WHAT it stamps:

  * any PDF of the email failed to unlock        → error, unlock_failed
  * any PDF's ingest failed (chunks/rejections)  → error, ingest_incomplete
  * a LEDGER email (pdf_ingest_context.is_ledger_statement: fail-safe,
    i.e. anything but a positive non-ledger label from a non-bank
    sender) with 0 landed rows in total        → error, no_rows
  * otherwise                                    → success

Positively non-ledger PDFs (CAS / brokerage / investment confirmations,
NPS, EPF, gold) from non-bank senders may legitimately land 0 ledger
rows, so the zero-row rule does not apply to them. "Landed" = rows the insert path
accepted (inserted + deduplicated), so a re-run of an already-ingested
statement still counts. No Flask/DB imports.
"""
from __future__ import annotations

from contextvars import ContextVar

# The SAME classifier n0cg's per-category chunk rule uses.
from utils.pdf_ingest_context import DEFAULT_PDF_CATEGORY, is_ledger_statement  # noqa: F401

UNLOCK_FAILED = "unlock_failed"
INGEST_INCOMPLETE = "ingest_incomplete"
NO_ROWS = "no_rows"

# Per-email landed-row counter. Set by _process_single_pdf_email around
# one email; the two batch-insert sites add to it. A ContextVar so
# concurrent workers (threads) never share a counter; anyio.run and the
# SDK's in-process tool handlers inherit the same dict.
LANDED_ROWS: ContextVar = ContextVar("ak_fc7z_landed_rows", default=None)


def add_landed(result):
    """Count an insert result's landed rows into the current email."""
    counter = LANDED_ROWS.get()
    if counter is None or not isinstance(result, dict):
        return
    try:
        counter["rows"] += int(result.get("inserted") or 0) + int(result.get("duplicates") or 0)
    except (TypeError, ValueError):
        pass


def add_recorded():
    """Count one accepted record-only insert_* (EPF/PPF/Gold/investment)."""
    counter = LANDED_ROWS.get()
    if counter is not None:
        counter["recorded"] = counter.get("recorded", 0) + 1


def final_email_status(category, landed_rows, failures, sender=None):
    """(status, reason) where status is the report_result vocabulary
    ('success' | 'error') and reason is None or one of the constants.

    failures: list of reason constants recorded per PDF.
    """
    if UNLOCK_FAILED in failures:
        return "error", UNLOCK_FAILED
    if failures:
        return "error", INGEST_INCOMPLETE
    if is_ledger_statement(category, sender) and not landed_rows:
        return "error", NO_ROWS
    return "success", None


REASON_TEXT = {
    UNLOCK_FAILED: "unlock_failed: the PDF password could not be found or was wrong; the agent was not run",
    INGEST_INCOMPLETE: "ingest_incomplete: failed or rejected chunks; retried next run",
    NO_ROWS: "no_rows: a statement PDF produced no transactions; retried next run",
}
