"""ak-zejx (2026-11-24): re-derive the bank of existing statement rows.

Prod (super_reviewer on ak-iu8i): among the first 100 statements, 32 carry
bank == 'bank_statement' (the CATEGORY sitting in the bank field: the
statements view falls back to processedEmails.category when
extraction_summary has no 'bank'), including SBI / BoB / Paytm; 16 Bank
of Baroda statements are stored as 'BOI'. ak-iu8i fixed the mapping for
new ingests only.

Decision per statement row (pure; no DB):
  * the SENDER DOMAIN decides the bank FAMILY (bankFormatRules, the same
    server-side map ingest uses). An unmapped domain → skip (reported);
  * a current value already in that family → untouched (idempotent; a
    card variant such as Millenia_Credit is never downgraded);
  * otherwise the exact id comes from STATEMENT CONTENT EVIDENCE: the
    bank ids on the transactions extracted from that statement, if
    exactly one of them belongs to the family; else the family's only id;
  * a family with several ids (HDFC debit / Millenia / Regalia, Yes
    debit / ACE) and no single in-family content evidence → skip as
    ambiguous, never guessed.
"""
from __future__ import annotations

from collections import Counter

FAMILY_IDS = {
    "HDFC": ("HDFC_DEBIT", "Millenia_Credit", "HDFC_REGALIA"),
    "ICICI": ("ICICI_AMAZON_PAY",),
    "YES": ("YES_BANK_DEBIT", "YES_BANK_ACE"),
    "BOI": ("BOI",),
    "BOB": ("BOB",),
    "SBI": ("SBI",),
    "SBI_CARD": ("SBI_CARD",),
    "AXIS": ("AXIS",),
    "KOTAK": ("KOTAK",),
    "PAYTM": ("PAYTM",),
}
FAMILY_OF = {bank: fam for fam, ids in FAMILY_IDS.items() for bank in ids}

FIX = "fix"
ALREADY_CORRECT = "already_correct"
SKIP_UNMAPPED_DOMAIN = "skip_unmapped_domain"
SKIP_AMBIGUOUS = "skip_ambiguous_variant"


def family(bank_id):
    return FAMILY_OF.get(bank_id) if isinstance(bank_id, str) else None


def decide(sender, current_bank, linked_banks=()):
    """(new_bank | None, outcome)."""
    from services.bankFormatRules import get_bank_from_sender
    domain_bank = get_bank_from_sender(sender or "")
    fam = family(domain_bank)
    if fam is None:
        return None, SKIP_UNMAPPED_DOMAIN
    if family(current_bank) == fam:
        return None, ALREADY_CORRECT
    evidence = {b for b in (linked_banks or ()) if family(b) == fam}
    if len(evidence) == 1:
        return evidence.pop(), FIX
    ids = FAMILY_IDS[fam]
    if len(ids) == 1:
        return ids[0], FIX
    return None, SKIP_AMBIGUOUS


def current_bank_of(extraction_summary, category):
    """The value the statements view shows as the bank (same fallback as
    TransactionService.fetchProcessedStatements)."""
    summary = extraction_summary if isinstance(extraction_summary, dict) else {}
    return summary.get("bank", category or "Unknown")


class Plan:
    """Counts + the per-row changes; never holds statement content."""

    def __init__(self):
        self.changes = []                 # (gmail_id, old, new)
        self.mapping = Counter()          # (old, new) -> n
        self.outcomes = Counter()         # outcome -> n
        self.skipped_domains = Counter()  # (outcome, domain) -> n
        self.reingest_risk = []           # (gmail_id, linked txn count), status != processed

    def add(self, gmail_id, old, new, outcome, domain):
        self.outcomes[outcome] += 1
        if outcome == FIX:
            self.changes.append((gmail_id, old, new))
            self.mapping[(old, new)] += 1
        elif outcome != ALREADY_CORRECT:
            self.skipped_domains[(outcome, domain or "(none)")] += 1

    def report_lines(self):
        lines = ["statement rows by outcome:"]
        for k, n in sorted(self.outcomes.items()):
            lines.append(f"  {k:<24} {n}")
        lines.append("planned bank changes (old -> new: count):")
        for (old, new), n in sorted(self.mapping.items()):
            lines.append(f"  {old!s:<16} -> {new:<16} {n}")
        if self.skipped_domains:
            lines.append("skipped (outcome, sender domain: count):")
            for (outcome, dom), n in sorted(self.skipped_domains.items()):
                lines.append(f"  {outcome:<24} {dom:<32} {n}")
        return lines
