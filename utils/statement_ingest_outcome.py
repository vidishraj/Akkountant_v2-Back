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

# ak-zejx: referenceIDs written for the statement being ingested RIGHT NOW
# (set by _process_single_pdf_email / reprocess_pdf around one email). Rows
# of the same statement that are NOT in it came from an EARLIER run, so the
# batch insert can recognise a re-ingest copy independently of the bank id
# (the referenceID hash folds the bank in; see
# mailProcessorToolExecutor._drop_previous_run_copies).
STATEMENT_RUN_REFS: ContextVar = ContextVar("ak_zejx_statement_run_refs", default=None)
# Earlier-run rows a skipped incoming row already matched are recorded in
# the same set as (_CONSUMED, referenceID) markers, so every chunk / call
# sharing the run sees them (tuples never collide with string refs).
_CONSUMED = "ak_zejx_consumed_prior"


def consumed_prior_refs(run_refs):
    """Earlier-run referenceIDs already matched in this run."""
    return {e[1] for e in (run_refs or ()) if isinstance(e, tuple) and e[:1] == (_CONSUMED,)}


def mark_prior_consumed(run_refs, refs):
    if run_refs is not None:
        run_refs.update((_CONSUMED, r) for r in refs if r)


# ak-d0mc: which incoming row (skipped as a copy) consumed which earlier
# row, as (_SKIPPED, earlier_ref, bank, row_json) markers, so a later chunk
# whose PK-path row OWNS that earlier row can release the skipped row.
_SKIPPED = "ak_d0mc_skipped_for"


def _norm_bank(bank):
    return (bank or "UNKNOWN").upper()


def record_skip(run_refs, earlier_ref, bank, txn, file_id=None, gmail_id=None):
    import json
    if run_refs is not None and earlier_ref:
        run_refs.add((_SKIPPED, earlier_ref, bank or "UNKNOWN", file_id, gmail_id,
                      json.dumps(txn, sort_keys=True, default=str)))


def pop_skip(run_refs, earlier_ref):
    """(bank, file_id, gmail_id, row) of the incoming row that consumed
    earlier_ref, removing the marker; None if no skipped row consumed it."""
    import json
    for e in list(run_refs or ()):
        if isinstance(e, tuple) and e[:2] == (_SKIPPED, earlier_ref):
            run_refs.discard(e)
            return e[2], e[3], e[4], json.loads(e[5])
    return None


# ak-d0mc review minor 1: a released row of ANOTHER bank (multi-PDF email)
# that can't be placed in the current batch waits here, as
# (_PENDING, bank, file_id, gmail_id, row_json). That bank's next batch in
# the same run inserts it; whatever is left is flushed at run end
# (mailProcessorToolExecutor.flush_pending_releases). Never dropped.
_PENDING = "ak_d0mc_pending_release"


def add_pending(run_refs, bank, file_id, gmail_id, txn):
    import json
    if run_refs is not None:
        run_refs.add((_PENDING, bank or "UNKNOWN", file_id, gmail_id,
                      json.dumps(txn, sort_keys=True, default=str)))


def take_pending(run_refs, bank=None):
    """Remove and return pending released rows [(bank, file_id, gmail_id,
    row)], only those of `bank` when given."""
    import json
    out = []
    for e in list(run_refs or ()):
        if isinstance(e, tuple) and e[:1] == (_PENDING,) and (
                bank is None or _norm_bank(e[1]) == _norm_bank(bank)):
            run_refs.discard(e)
            out.append((e[1], e[2], e[3], json.loads(e[4])))
    return out


# ak-b5sv: run-level ak-tik dedup signal. Set by process_emails around a
# whole run; every accepted insert_batch_transactions result adds its
# degraded_dedup_rows / dual_tier_tuples (ak-qic6) here via add_landed, so
# the run summary (and from it /readEmails/status and the AgentChat
# ingest_notice SSE event) can report them. Previously they were dropped
# right after each chunk.
DEDUP_SIGNAL_KEYS = ("degraded_dedup_rows", "dual_tier_tuples")
DEDUP_SIGNAL: ContextVar = ContextVar("ak_b5sv_dedup_signal", default=None)


def new_dedup_signal():
    return {k: 0 for k in DEDUP_SIGNAL_KEYS}


def dedup_count(value):
    """A non-negative int from a result field; anything else is 0."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0
    return value


def _add_dedup_signal(result):
    signal = DEDUP_SIGNAL.get()
    if signal is None:
        return
    for key in DEDUP_SIGNAL_KEYS:
        signal[key] = signal.get(key, 0) + dedup_count(result.get(key))


def add_landed(result):
    """Count an insert result's landed rows into the current email (and
    its dedup signal into the current run, ak-b5sv)."""
    if not isinstance(result, dict):
        return
    _add_dedup_signal(result)
    counter = LANDED_ROWS.get()
    if counter is None:
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
