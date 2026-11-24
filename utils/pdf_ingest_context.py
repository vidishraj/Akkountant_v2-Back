"""ak-n0cg round 3 (2026-11-24): PDF-ingest category rules + per-email
report metadata. Shared with ak-fc7z.

Ledger statements (classifier category 'bank_statement': bank AND
credit-card statements) put rows in the ledger through
insert_batch_transactions, so a chunk only succeeds once a batch landed.
Every other PDF category (brokerage/CAS, contract notes, EPF passbooks,
NPS, gold receipts) is recorded through the record-only insert_* tools,
or legitimately has nothing to ingest. Requiring a batch call there made
those emails fail forever, and the batch follow-up prompt pushed
investment rows in as bank transactions.

PDF_REPORT: the service owns the processedEmails STATUS in PDF mode, but
the agent's report_result METADATA (extraction_summary, items_extracted,
category) is still worth keeping. The handler records the latest report
here and _process_single_pdf_email stamps it with the server's status.
A ContextVar so concurrent workers never share one; anyio.run and the
SDK's in-process tool handlers inherit the same dict.
"""
from __future__ import annotations

from contextvars import ContextVar

LEDGER_STATEMENT_CATEGORIES = frozenset({"bank_statement"})
DEFAULT_PDF_CATEGORY = "bank_statement"   # the stamp's existing default

# Round 4: the label comes from the mail.classify LLM (it reads untrusted
# email), so it can only EXEMPT a PDF from the landed-rows rule when it is
# a POSITIVE non-ledger label. These are the classifier's categories whose
# PDFs are recorded via the record-only tools, or hold nothing to ingest.
NON_LEDGER_CATEGORIES = frozenset({
    "investment_confirmation", "brokerage_statement", "epf_passbook",
    "nps_statement", "gold_receipt", "freelance_payment",
    "freelance_contract", "freelance_correspondence",
})
# The classifier's full vocabulary (model_category must be one of these).
KNOWN_CATEGORIES = NON_LEDGER_CATEGORIES | LEDGER_STATEMENT_CATEGORIES | {
    "transaction_alert", "unknown",
}
MAX_SUMMARY_BYTES = 32 * 1024

PDF_REPORT: ContextVar = ContextVar("ak_n0cg_pdf_report", default=None)


def sender_is_bank(sender):
    """Server-side: the From domain maps to a known bank
    (services.bankFormatRules.get_bank_from_sender)."""
    if not sender:
        return False
    from services.bankFormatRules import get_bank_from_sender
    try:
        return get_bank_from_sender(str(sender)) != "UNKNOWN"
    except Exception:
        return False


def is_ledger_statement(category, sender=None):
    """Fail-safe toward ledger (round 4).

    Ledger (needs a landed batch) unless BOTH: the label is a positive
    non-ledger category AND the sender is not a known bank. So
    'bank_statement', 'transaction_alert', 'unknown', a missing label,
    or anything from a bank domain all count as ledger: a bank statement
    the classifier mislabels can't skip the batch requirement. A bank's
    genuinely non-statement PDF then fails loudly instead of a statement
    being lost.
    """
    if sender_is_bank(sender):
        return True
    return category not in NON_LEDGER_CATEGORIES


def record_report(args):
    """Keep the agent's report_result metadata (last report wins, as the
    old per-call upsert did). Never its status."""
    holder = PDF_REPORT.get()
    if holder is None or not isinstance(args, dict):
        return
    summary = args.get("extraction_summary")
    if isinstance(summary, dict) and _json_size(summary) <= MAX_SUMMARY_BYTES:
        holder["extraction_summary"] = summary
    items = args.get("items_extracted")
    if isinstance(items, int) and not isinstance(items, bool) and 0 <= items <= 1_000_000:
        holder["items_extracted"] = items
    cat = args.get("category")
    if isinstance(cat, str) and cat in KNOWN_CATEGORIES:
        holder["model_category"] = cat


def _json_size(obj):
    import json
    try:
        return len(json.dumps(obj, default=str).encode())
    except (TypeError, ValueError):
        return MAX_SUMMARY_BYTES + 1
