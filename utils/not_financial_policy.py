"""ak-v6zp: when a classifier-dropped ('skipped' / 'not_financial') email
is classified again.

Re-classifying every not_financial email every hourly cycle spent the SDK
on the same newsletters. Skipping all of them, though, would let ONE
classifier false negative hide a financial email forever (review MAJOR,
lead decision 2026-11-24). So:

  * a LIKELY-financial email (known bank sender domain, or it carries a
    PDF) is re-classified automatically until the classifier has said
    not_financial MAX_NF_VERDICTS times (counted in the row's
    extraction_summary[NF_VERDICTS_KEY]);
  * every other not_financial email (newsletters, promos) is skipped;
  * a manual scan can still ask for all of them (GET /readEmails
    ?reclassify=true), over at most MAX_RECLASSIFY_DAYS.
Leaf module (pure; the bank-domain map is a lazy import).
"""
from __future__ import annotations

from datetime import datetime

NF_VERDICTS_KEY = "_nf_verdicts"
MAX_NF_VERDICTS = 3
MAX_RECLASSIFY_DAYS = 31
SCAN_DATE_FORMAT = "%Y/%m/%d"   # GmailServiceUtils.findAllEmailsInInterval


def nf_verdicts(summary) -> int:
    """Consistent not_financial verdicts recorded so far (0 if none or
    malformed)."""
    if not isinstance(summary, dict):
        return 0
    n = summary.get(NF_VERDICTS_KEY)
    if isinstance(n, bool) or not isinstance(n, int) or n < 0:
        return 0
    return n


def with_nf_verdict(summary) -> dict:
    """The row's summary with one more not_financial verdict counted
    (other keys, e.g. the ak-7ahz _attempts counter, kept)."""
    out = dict(summary) if isinstance(summary, dict) else {}
    out[NF_VERDICTS_KEY] = nf_verdicts(summary) + 1
    return out


def is_likely_financial(sender, has_pdf) -> bool:
    """Known bank sender domain (DOMAIN_TO_BANK, subdomain-aware) or a
    PDF attachment."""
    if has_pdf:
        return True
    from services.bankFormatRules import get_bank_from_sender
    try:
        return get_bank_from_sender(sender or "") != "UNKNOWN"
    except Exception:
        return False


def should_reclassify(sender, has_pdf, summary) -> bool:
    """True = send this already-not_financial email to the classifier
    again on an automatic cycle."""
    return is_likely_financial(sender, has_pdf) and nf_verdicts(summary) < MAX_NF_VERDICTS


def reclassify_range_error(date_from, date_to):
    """None if [date_from, date_to] (SCAN_DATE_FORMAT) is a valid range of
    at most MAX_RECLASSIFY_DAYS days, else a short error message."""
    try:
        start = datetime.strptime(str(date_from), SCAN_DATE_FORMAT)
        end = datetime.strptime(str(date_to), SCAN_DATE_FORMAT)
    except (TypeError, ValueError):
        return "reclassify needs dateFrom and dateTo as YYYY/MM/DD"
    if end < start:
        return "dateTo is before dateFrom"
    if (end - start).days > MAX_RECLASSIFY_DAYS:
        return f"reclassify is limited to {MAX_RECLASSIFY_DAYS} days"
    return None
