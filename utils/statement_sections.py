"""ak-ifc: HDFC combined-statement section detector + reconciliation.

HDFC monthly PDFs are COMBINED statements: they contain multiple
sub-accounts (savings, credit card, fixed deposit, mutual fund,
recurring deposit / PPF, etc.) stitched together. The pre-ak-ifc
extractor treated the whole file as one savings statement, so
transactions from every other sub-account leaked into the savings
fileID. Concrete damage per infra's ak-4b3 Lane C investigation:

  Apr-2026: +₹292,000 EXTRA debits vs the savings-summary total
            (non-savings debits treated as savings)
  May-2026: -₹50,000 credit missed + -₹109,000 debit missed
            (savings rows classified into the wrong section OR
             dropped when the LLM ran out of context on non-savings
             sections)
  Affects: all 12 HDFC files, not just Apr / May.

This module has two responsibilities:

  1. Section-boundary detection so the extractor pre-filters raw
     text to just the SAVINGS section before the LLM sees it.
  2. Reconciliation backstop (ak-ifc v2 review, reviewer's proposal):
     because a PARTIAL savings-header miss silently under-parses
     (the pre-filter drops savings text between the missed header
     and the next real header), we compare extracted savings totals
     against the PDF's OWN stated savings summary. If they diverge
     beyond a small tolerance, the caller falls back to unfiltered
     extraction — preserving Overseer's zero-loss guarantee at the
     cost of accepting the multi-account contamination we're
     otherwise trying to filter (over-parse is recoverable via
     dedup; under-parse is silent loss).

Pure-Python, no framework deps.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Iterable, Optional


class SectionType(str, Enum):
    """Enumerated account types HDFC combined statements can contain.

    Values are stable string keys so they can be logged, persisted,
    or compared without version drift. UNKNOWN is the fallback for
    sections whose header didn't match any recognized pattern.
    """
    SAVINGS = "SAVINGS"
    CURRENT = "CURRENT"
    CREDIT_CARD = "CREDIT_CARD"
    FIXED_DEPOSIT = "FIXED_DEPOSIT"
    MUTUAL_FUND = "MUTUAL_FUND"
    RECURRING_DEPOSIT = "RECURRING_DEPOSIT"
    PPF = "PPF"
    UNKNOWN = "UNKNOWN"


# HDFC section headers observed in real statements. Ordered by
# specificity so more-specific patterns match first (e.g. "RECURRING
# DEPOSIT" before "DEPOSIT") — Python's regex alternation is
# left-to-right, but we use a list of individual patterns so ordering
# is explicit + auditable.
#
# Header phrasings tolerated per section (ak-dby broadening):
#   1. "Statement of Account for : X"  (canonical, existing)
#   2. "Statement for : X"              (elided "of Account")
#   3. "X Account"                      (bare, e.g. "Savings Account")
#   4. "X A/C" or "X Bank Account"     (abbreviated forms)
#   5. "Account Type: X"                (typed line)
#   6. "Account: <num> (X)"             (parenthetical, ak-dby)
#
# Case-insensitive. The SectionType we emit is normalized.
#
# ak-dby: the pre-ak-dby regex only matched shape (1) / (2). The
# Vidish_Raj_2026 monthly layout uses shape (3) / (4) plus tabular
# banners without the "Statement for" prefix, so every 2026 file
# came back with savings_spans=0 and triggered the full-text
# fallback. Fallback IS correct for zero-loss BUT before ak-dby the
# fallback didn't set reconciliation_fallback=True, so the strip
# endpoint never ran and non-savings sub-accounts leaked into the
# savings fileID. See mailProcessorService's savings_span_count==0
# branch for the flag-setting fix.
_HDFC_SECTION_PATTERNS: list[tuple[SectionType, re.Pattern[str]]] = [
    # ── SAVINGS ────────────────────────────────────────────────
    (
        SectionType.SAVINGS,
        re.compile(
            r"statement\s+(?:of\s+account\s+)?for\s*:?\s*savings",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.SAVINGS,
        # ak-dby: bare "Savings Account" / "Savings A/C" /
        # "Savings Bank Account" banner. Anchored to line start
        # (^\s*) so a narration line mentioning "savings account
        # balance" mid-string can't false-positive.
        re.compile(
            r"^\s*savings\s+(?:bank\s+)?(?:account|a\s*/?\s*c)\b",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.SAVINGS,
        # ak-dby: "Account Type : Savings" typed line.
        re.compile(
            r"^\s*account\s+type\s*[:\-]?\s*savings",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.SAVINGS,
        # ak-dby: parenthetical "Account: 50100XXXX (SAVINGS)"
        # form some HDFC combined layouts use.
        re.compile(
            r"^\s*account\s*[:\-]?[^\n]{0,60}\(\s*savings\s*\)",
            re.IGNORECASE,
        ),
    ),
    # ── CURRENT ────────────────────────────────────────────────
    (
        SectionType.CURRENT,
        re.compile(
            r"statement\s+(?:of\s+account\s+)?for\s*:?\s*current",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.CURRENT,
        re.compile(
            r"^\s*current\s+(?:bank\s+)?(?:account|a\s*/?\s*c)\b",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.CURRENT,
        re.compile(
            r"^\s*account\s+type\s*[:\-]?\s*current",
            re.IGNORECASE,
        ),
    ),
    # ── CREDIT_CARD ────────────────────────────────────────────
    (
        SectionType.CREDIT_CARD,
        re.compile(
            r"statement\s+(?:of\s+account\s+)?for\s*:?\s*credit\s*card",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.CREDIT_CARD,
        re.compile(
            # "Credit Card Account", "Credit Card A/C",
            # "Credit Card Statement" — banner variants. Anchored to
            # line start to avoid narration false-positives.
            r"^\s*credit\s*card\s+(?:account|a\s*/?\s*c|statement)\b",
            re.IGNORECASE,
        ),
    ),
    # ── RECURRING_DEPOSIT (before FIXED_DEPOSIT — substring guard) ──
    (
        SectionType.RECURRING_DEPOSIT,
        re.compile(
            r"statement\s+(?:of\s+account\s+)?for\s*:?\s*recurring\s*deposit",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.RECURRING_DEPOSIT,
        re.compile(
            r"^\s*recurring\s*deposit\s+(?:account|a\s*/?\s*c)\b",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.RECURRING_DEPOSIT,
        re.compile(
            # "RD A/C" or "RD Account" — the RD abbreviation on
            # combined layouts.
            r"^\s*rd\s+(?:account|a\s*/?\s*c)\b",
            re.IGNORECASE,
        ),
    ),
    # ── FIXED_DEPOSIT ──────────────────────────────────────────
    (
        SectionType.FIXED_DEPOSIT,
        re.compile(
            r"statement\s+(?:of\s+account\s+)?for\s*:?\s*fixed\s*deposit",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.FIXED_DEPOSIT,
        re.compile(
            r"^\s*fixed\s*deposit\s+(?:account|a\s*/?\s*c)\b",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.FIXED_DEPOSIT,
        re.compile(
            # "FD A/C" or "FD Account" — the FD abbreviation.
            r"^\s*fd\s+(?:account|a\s*/?\s*c)\b",
            re.IGNORECASE,
        ),
    ),
    # ── MUTUAL_FUND ────────────────────────────────────────────
    (
        SectionType.MUTUAL_FUND,
        re.compile(
            r"statement\s+(?:of\s+account\s+)?for\s*:?\s*mutual\s*fund",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.MUTUAL_FUND,
        re.compile(
            r"^\s*mutual\s*fund\s+(?:account|a\s*/?\s*c|folio)\b",
            re.IGNORECASE,
        ),
    ),
    # ── PPF ────────────────────────────────────────────────────
    (
        SectionType.PPF,
        re.compile(
            r"statement\s+(?:of\s+account\s+)?for\s*:?\s*(?:ppf|public\s+provident)",
            re.IGNORECASE,
        ),
    ),
    (
        SectionType.PPF,
        re.compile(
            r"^\s*(?:ppf|public\s+provident\s+fund)\s+(?:account|a\s*/?\s*c)\b",
            re.IGNORECASE,
        ),
    ),
]


@dataclass(frozen=True)
class SectionSpan:
    """A contiguous slice of the raw statement text belonging to one
    account section. Line indices are 0-indexed on the split-by-newline
    array of the RAW file text (so callers can map to their own
    line-numbering scheme via a simple `+1`).

    end_line is inclusive of the last line in this section — i.e.
    `raw_lines[start_line:end_line+1]` gives the section's text.
    """
    section: SectionType
    start_line: int
    end_line: int
    header_line: int  # the line that carried the section-header pattern


def detect_hdfc_sections(raw_text: str) -> list[SectionSpan]:
    """Scan raw HDFC statement text and return one SectionSpan per
    detected sub-account.

    Rules:
      - Every text file starts with an implicit UNKNOWN section from
        line 0 up to the first header line (typically account-holder
        details, address block, statement summary).
      - Each header line begins a new SectionSpan whose type is
        determined by the first pattern to match.
      - Section runs until the next header line (or EOF).
      - Adjacent same-type spans are NOT auto-merged. HDFC's savings
        section often has a "Statement of Account for : SAVINGS
        ACCOUNT ..." followed later by "Statement for : SAVINGS
        ACCOUNT ..." (re-header on a new page); each header opens
        its own span. Callers that only care about "is this a
        savings line" (which is the typical use case) walk all
        SectionType.SAVINGS spans without caring which is which —
        `keep_only_sections` does exactly that.

    Returns spans in order of appearance in the source text.
    """
    lines = raw_text.split("\n")
    header_hits: list[tuple[int, SectionType]] = []
    for line_idx, line in enumerate(lines):
        # First-match wins — the pattern list is ordered specific-first.
        for section_type, pattern in _HDFC_SECTION_PATTERNS:
            if pattern.search(line):
                header_hits.append((line_idx, section_type))
                break

    spans: list[SectionSpan] = []
    if not header_hits:
        # No section headers found → treat whole file as one UNKNOWN
        # section. This matches the pre-ak-ifc behavior for
        # single-account statements (they had no section headers and
        # the LLM saw the whole thing).
        if lines:
            spans.append(
                SectionSpan(
                    section=SectionType.UNKNOWN,
                    start_line=0,
                    end_line=len(lines) - 1,
                    header_line=-1,
                ),
            )
        return spans

    # Leading UNKNOWN span from line 0 to just before the first
    # header. Callers usually strip this (account-holder / summary
    # text isn't a transaction).
    first_header_line = header_hits[0][0]
    if first_header_line > 0:
        spans.append(
            SectionSpan(
                section=SectionType.UNKNOWN,
                start_line=0,
                end_line=first_header_line - 1,
                header_line=-1,
            ),
        )

    # One span per header, running to (next_header - 1) or EOF.
    for i, (header_line, section_type) in enumerate(header_hits):
        if i + 1 < len(header_hits):
            end_line = header_hits[i + 1][0] - 1
        else:
            end_line = len(lines) - 1
        spans.append(
            SectionSpan(
                section=section_type,
                start_line=header_line,
                end_line=end_line,
                header_line=header_line,
            ),
        )

    return spans


def keep_only_sections(
    raw_text: str,
    keep: Iterable[SectionType],
    *,
    replace_stripped_with_placeholder: bool = True,
) -> tuple[str, list[SectionSpan]]:
    """Return `raw_text` filtered to only the requested section types.

    Line numbers of the kept lines are PRESERVED in the returned text
    — stripped lines are either removed OR replaced with an empty
    placeholder line (default: TRUE) so downstream line-number
    annotations still point at the right file-level position.

    The chunker in mailProcessorService adds `[Ln]` file-level line
    markers that the LLM echoes back into `line_position` for the
    ak-8l5 dedup engine. If we compressed the stripped sections out
    entirely, those markers would be lopsided — some point at the
    original file positions, some at the compressed. Placeholder-lines
    keep the markers stable.

    Returns (filtered_text, spans) so callers can log what got
    stripped without re-scanning.
    """
    keep_set = frozenset(keep)
    spans = detect_hdfc_sections(raw_text)
    lines = raw_text.split("\n")

    if replace_stripped_with_placeholder:
        # Blank out lines that don't belong to a kept section, keeping
        # the array length identical so line numbers stay stable.
        keep_mask = [False] * len(lines)
        for span in spans:
            if span.section in keep_set:
                for i in range(span.start_line, min(span.end_line + 1, len(lines))):
                    keep_mask[i] = True
        filtered_lines = [
            lines[i] if keep_mask[i] else ""
            for i in range(len(lines))
        ]
        return "\n".join(filtered_lines), spans

    # Compact mode (rare — used when the caller re-numbers lines on
    # its own downstream)
    kept: list[str] = []
    for span in spans:
        if span.section in keep_set:
            kept.extend(lines[span.start_line:span.end_line + 1])
    return "\n".join(kept), spans


def summarize_sections(spans: Iterable[SectionSpan]) -> str:
    """Human-friendly one-line summary for logging.
    Example: 'SAVINGS(120-450) CREDIT_CARD(451-800) UNKNOWN(0-119)'.
    Callers use this to make section drift visible in prod logs.
    """
    parts = []
    for s in spans:
        parts.append(f"{s.section.value}({s.start_line}-{s.end_line})")
    return " ".join(parts)


# TODO(ak-ifc follow-up): BOI combined statements — Lead flagged that
# BOI may have the same issue. When we get a real BOI combined-file
# sample, add a _BOI_SECTION_PATTERNS + detect_boi_sections mirror.
# The keep_only_sections helper is bank-agnostic (it takes the
# `spans` output shape), so only the pattern list needs bank-specific
# work.


# ── ak-ifc v2: SAVINGS-summary reconciliation backstop ──────────────
#
# Reviewer flagged (MINOR 1, stealth-MAJOR under zero-loss constraint):
# a PARTIAL savings-header miss produces a silent under-parse. Example:
# a statement has two "SAVINGS ACCOUNT" re-headers, one on page 3 and
# one on page 12. If the regex catches the first but the second is
# slightly-off (extra whitespace, alternate wording), the range 12..N
# gets stripped from the LLM's view even though it's really savings.
# Result: transactions between page 12 and the next real header
# vanish — no error, no warning, just missing rows.
#
# Backstop design: parse the PDF's OWN savings summary (opening bal,
# closing bal, total debits, total credits) from the raw text; sum
# what the extractor extracted; compare. If mismatched beyond a small
# tolerance, the caller (mailProcessorService._run_text_chunk) falls
# back to unfiltered extraction. Over-parse is recoverable via
# ak-8l5 dedup; under-parse is silent loss.


@dataclass(frozen=True)
class HdfcSavingsSummary:
    """The PDF's own stated totals for the SAVINGS section.

    All fields are optional because different HDFC statement layouts
    surface different subsets. The reconciler only compares fields
    that are present on BOTH sides (extracted vs stated).

    Positive convention:
      - total_debits: sum of money OUT of the account (positive)
      - total_credits: sum of money INTO the account (positive; kept
        as a positive magnitude so the caller compares against
        abs(sum(negatives)) from the extracted rows)
      - opening_balance, closing_balance: signed as they appear in
        the statement (CR/DR annotations already normalized out;
        usually positive)
    """
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None
    total_debits: Optional[float] = None
    total_credits: Optional[float] = None
    debit_count: Optional[int] = None
    credit_count: Optional[int] = None


# HDFC's savings-summary block appears at the head of the SAVINGS
# section in various shapes across the layout revisions. We tolerate
# common variants:
#
#   "Opening Balance : 12,345.67"
#   "Opening Balance    12345.67"
#   "OPENING BALANCE  ₹12,345.67 CR"
#
#   "Total Debits : 45,678.00" or "Amount of Debits" or "Debits Amt"
#   "Total Credits : 33,333.33" or "Amount of Credits" or "Credits Amt"
#
#   "Closing Balance : 100.00"
#
# The regexes below are all case-insensitive and tolerant of ₹ / Rs /
# INR prefixes plus DR/CR suffixes. Amount strings can contain commas
# (Indian numbering — "1,00,000") and an optional decimal.
_NUM_RE = r"(?:\d{1,3}(?:,\d{2,3})*|\d+)(?:\.\d+)?"
_CURRENCY_PREFIX = r"(?:₹|Rs\.?|INR)?\s*"
_DR_CR_SUFFIX = r"\s*(?:DR|CR|Dr|Cr)?"


def _amount(match_str: str) -> Optional[float]:
    """Parse an amount string like '1,23,456.78' into a float.
    Returns None on garbage."""
    if not match_str:
        return None
    stripped = match_str.replace(",", "").strip()
    try:
        return float(stripped)
    except ValueError:
        return None


_HDFC_SUMMARY_PATTERNS = {
    "opening_balance": re.compile(
        r"opening\s*balance\s*[:\-]?\s*"
        + _CURRENCY_PREFIX + r"(" + _NUM_RE + r")" + _DR_CR_SUFFIX,
        re.IGNORECASE,
    ),
    "closing_balance": re.compile(
        r"closing\s*balance\s*[:\-]?\s*"
        + _CURRENCY_PREFIX + r"(" + _NUM_RE + r")" + _DR_CR_SUFFIX,
        re.IGNORECASE,
    ),
    "total_debits": re.compile(
        r"(?:total|amount\s*of|amt\s*of|amt)\s*debits?\s*[:\-]?\s*"
        + _CURRENCY_PREFIX + r"(" + _NUM_RE + r")" + _DR_CR_SUFFIX,
        re.IGNORECASE,
    ),
    "total_credits": re.compile(
        r"(?:total|amount\s*of|amt\s*of|amt)\s*credits?\s*[:\-]?\s*"
        + _CURRENCY_PREFIX + r"(" + _NUM_RE + r")" + _DR_CR_SUFFIX,
        re.IGNORECASE,
    ),
    "debit_count": re.compile(
        r"(?:no\.?\s*of|number\s*of|count\s*of)\s*debits?\s*[:\-]?\s*(\d+)",
        re.IGNORECASE,
    ),
    "credit_count": re.compile(
        r"(?:no\.?\s*of|number\s*of|count\s*of)\s*credits?\s*[:\-]?\s*(\d+)",
        re.IGNORECASE,
    ),
}


def parse_hdfc_savings_summary(
    raw_text: str,
    *,
    spans: Optional[Iterable[SectionSpan]] = None,
) -> Optional[HdfcSavingsSummary]:
    """Extract the SAVINGS-section summary from raw statement text.

    If `spans` is provided (typically from detect_hdfc_sections), we
    scan ONLY the SAVINGS-section lines — this avoids false-positive
    matches on other sections' summaries (e.g. the credit-card
    section's "Total Debits" line). If `spans` is None we scan the
    whole file, which is fine when the caller knows the file is
    single-account or when they just want a best-effort read.

    Returns HdfcSavingsSummary populated with whichever fields we
    could recognize. Fields we couldn't parse are left as None so
    the reconciler naturally skips them. Returns None if not even a
    single field could be parsed (there's nothing to reconcile
    against; caller must not fabricate a divergence).
    """
    lines = raw_text.split("\n")

    # If spans were provided, extract only the SAVINGS-section lines.
    if spans is not None:
        savings_ranges = [
            (s.start_line, min(s.end_line, len(lines) - 1))
            for s in spans if s.section == SectionType.SAVINGS
        ]
        if not savings_ranges:
            # No savings section detected — nothing to parse.
            return None
        scan_text = "\n".join(
            lines[a] for lo, hi in savings_ranges for a in range(lo, hi + 1)
        )
    else:
        scan_text = raw_text

    fields = {}
    for field_name, pattern in _HDFC_SUMMARY_PATTERNS.items():
        m = pattern.search(scan_text)
        if not m:
            continue
        raw = m.group(1)
        if field_name in ("debit_count", "credit_count"):
            try:
                fields[field_name] = int(raw)
            except ValueError:
                continue
        else:
            amt = _amount(raw)
            if amt is not None:
                fields[field_name] = amt

    if not fields:
        return None

    return HdfcSavingsSummary(**fields)


@dataclass(frozen=True)
class ReconciliationResult:
    """Output of check_hdfc_savings_reconciliation.

    - diverged: True iff the extracted totals disagree with the PDF's
      stated summary beyond `tolerance`.
    - reason: short human-readable summary; logged when diverged=True.
    - checked_fields: list of field names we actually compared. Empty
      when the summary was missing entirely (caller must not treat
      that as "clean" — the check simply couldn't run).
    - extracted_debits / extracted_credits: sums the reconciler
      computed from the extracted rows, for the caller's log.
    - stated_debits / stated_credits: the PDF's own totals, for the
      caller's log.
    """
    diverged: bool
    reason: str
    checked_fields: tuple = ()
    extracted_debits: Optional[float] = None
    extracted_credits: Optional[float] = None
    stated_debits: Optional[float] = None
    stated_credits: Optional[float] = None


def check_hdfc_reconciliation_from_totals(
    extracted_debits: float,
    extracted_credits: float,
    summary: Optional[HdfcSavingsSummary],
    *,
    tolerance: float = 1.0,
) -> ReconciliationResult:
    """ak-ifc v3: totals-only variant. Same decision logic as
    check_hdfc_savings_reconciliation, but takes pre-aggregated
    debit / credit sums (from a DB `SELECT SUM(amount) WHERE
    amount > 0 GROUP BY ...` shape) instead of iterating rows.

    The file-level orchestrator in mailProcessorService uses this
    directly so the reconciler doesn't need to be handed a fake
    two-row list to represent the aggregated totals.
    """
    return check_hdfc_savings_reconciliation(
        [{"amount": float(extracted_debits)},
         {"amount": -float(extracted_credits)}],
        summary,
        tolerance=tolerance,
    )


def check_hdfc_savings_reconciliation(
    extracted_rows: Iterable[dict],
    summary: Optional[HdfcSavingsSummary],
    *,
    tolerance: float = 1.0,
) -> ReconciliationResult:
    """Reviewer's proposal: compare extracted savings totals against
    the PDF's stated savings summary; return ReconciliationResult
    with diverged=True iff we're outside `tolerance`.

    `extracted_rows` is an iterable of dicts with (at minimum) an
    `amount` field. Convention (matches the extractor's output):
      - amount > 0 → debit (money out)
      - amount < 0 → credit (money in)

    `summary` is a HdfcSavingsSummary (usually from
    parse_hdfc_savings_summary). If None, we return a
    diverged=False result with reason="no summary parsed" so the
    caller knows the check couldn't run (this is NOT a clean pass —
    caller MUST log the skip so it's visible if summaries stop
    parsing across all files at once).

    `tolerance` is the max allowed |extracted - stated| in ₹. Default
    ₹1 accommodates rounding at the summary line (some HDFC layouts
    round the summary to the nearest rupee). Increase if the caller
    encounters legit noise, but never past ~10% of the expected
    magnitude (which would defeat the point).
    """
    debit_sum = 0.0
    credit_sum = 0.0
    for row in extracted_rows:
        try:
            amt = float(row.get("amount", 0))
        except (TypeError, ValueError):
            continue
        if amt > 0:
            debit_sum += amt
        elif amt < 0:
            credit_sum += -amt  # magnitude, matches summary convention

    if summary is None:
        return ReconciliationResult(
            diverged=False,
            reason="no summary parsed",
            checked_fields=(),
            extracted_debits=round(debit_sum, 2),
            extracted_credits=round(credit_sum, 2),
        )

    checked = []
    delta_msgs = []

    if summary.total_debits is not None:
        checked.append("total_debits")
        delta = abs(debit_sum - summary.total_debits)
        if delta > tolerance:
            delta_msgs.append(
                f"debits: extracted={debit_sum:.2f} vs "
                f"stated={summary.total_debits:.2f} "
                f"(delta={delta:.2f})"
            )

    if summary.total_credits is not None:
        checked.append("total_credits")
        delta = abs(credit_sum - summary.total_credits)
        if delta > tolerance:
            delta_msgs.append(
                f"credits: extracted={credit_sum:.2f} vs "
                f"stated={summary.total_credits:.2f} "
                f"(delta={delta:.2f})"
            )

    if not checked:
        # Summary was parsed but the specific fields we care about
        # (total_debits / total_credits) weren't in it. Treat as
        # "check unable to run" — same shape as summary=None.
        return ReconciliationResult(
            diverged=False,
            reason="summary parsed but no total_debits / total_credits",
            checked_fields=tuple(checked),
            extracted_debits=round(debit_sum, 2),
            extracted_credits=round(credit_sum, 2),
            stated_debits=summary.total_debits,
            stated_credits=summary.total_credits,
        )

    if delta_msgs:
        return ReconciliationResult(
            diverged=True,
            reason="; ".join(delta_msgs),
            checked_fields=tuple(checked),
            extracted_debits=round(debit_sum, 2),
            extracted_credits=round(credit_sum, 2),
            stated_debits=summary.total_debits,
            stated_credits=summary.total_credits,
        )

    return ReconciliationResult(
        diverged=False,
        reason="within tolerance",
        checked_fields=tuple(checked),
        extracted_debits=round(debit_sum, 2),
        extracted_credits=round(credit_sum, 2),
        stated_debits=summary.total_debits,
        stated_credits=summary.total_credits,
    )


# ── ak-ex2: post-fallback non-savings row stripper ──────────────────
#
# When ak-ifc-v3's file-level reconciliation detects divergence, it
# re-runs every chunk with force_no_mask=True so previously-missed
# savings rows are recovered. That re-run ALSO re-admits non-savings
# sub-account rows (credit card, fixed deposit, mutual fund, RD, PPF)
# into the savings fileID. The `reconciliation_fallback=True` tag on
# fileDetails flags these files; ak-ex2 strips the actual over-parse.
#
# Strategy: for each tx row on a tagged file, locate the row in the
# RAW file text and determine which section (SAVINGS vs. non-SAVINGS)
# its narration line belongs to. Delete rows whose section is
# non-SAVINGS. `line_position` is NOT stored on Transactions, so we
# match by (amount, description) narration overlap against the raw
# text. Ambiguous matches (multiple candidates OR no candidate) are
# left alone — conservative: never delete something we can't classify
# confidently.


def build_line_to_section_map(
    spans: Iterable[SectionSpan],
    num_lines: int,
) -> list[SectionType]:
    """Given a list of SectionSpans (0-indexed line ranges) and the
    total number of lines in the raw text, return a list[SectionType]
    of length `num_lines` where index i is the section that line i
    belongs to.

    Lines not covered by any span become UNKNOWN (defensive — the
    section detector already emits an UNKNOWN leading span for the
    account-holder / summary header, and each header line opens its
    own span, so uncovered lines are rare edge cases).

    Used by the ak-ex2 stripper to look up a row's section given
    its located line index in one O(1) hit.
    """
    section_by_line: list[SectionType] = [SectionType.UNKNOWN] * num_lines
    for span in spans:
        stop = min(span.end_line + 1, num_lines)
        for i in range(span.start_line, stop):
            section_by_line[i] = span.section
    return section_by_line


# Amount rendering variants observed in HDFC PDFs. Statements
# sometimes show "1,234.56", sometimes "1234.56", sometimes with a
# trailing " CR" / " DR" suffix, sometimes with "Rs. " / "₹" prefix.
# We match a canonical .2f magnitude both with and without commas
# and let the caller decide how strict to be about surrounding
# formatting.
_AMOUNT_NUM_ONLY_RE = re.compile(r"\d[\d,]*(?:\.\d{1,2})?")


def _amount_variants(amount: float) -> list[str]:
    """Yield string forms of `amount` that are likely to appear in
    HDFC narration lines. Uses the ABSOLUTE magnitude (sign is
    inferred by the extractor's positive-debit / negative-credit
    convention and doesn't appear on the source line in HDFC's
    layout — CR/DR is a text suffix, not a sign).

    Emits:
      - "1234.56"  (comma-less)
      - "1,234.56" (comma-grouped, Indian numbering)
      - "1234"     (no decimal for whole rupee)
    """
    mag = round(abs(float(amount)), 2)
    fixed = f"{mag:.2f}"
    # Comma-less: identity of fixed.
    variants = [fixed]
    # Comma-grouped Indian numbering: "1,00,000.50" for 100000.50.
    # Split integer / fraction parts, group integer part from the
    # right with lakh convention (last 3 digits, then groups of 2).
    int_part, _, frac_part = fixed.partition(".")
    if len(int_part) > 3:
        # Reverse, take first 3, then chunks of 2.
        rev = int_part[::-1]
        head = rev[:3][::-1]
        rest = rev[3:]
        groups = [rest[i:i+2][::-1] for i in range(0, len(rest), 2)]
        grouped = ",".join(reversed(groups)) + "," + head
        variants.append(f"{grouped}.{frac_part}")
    # Whole-rupee shape (no decimal) — some HDFC layouts drop the
    # trailing .00 in narration lines even when the summary carries
    # it. Only meaningful for whole numbers.
    if frac_part == "00":
        variants.append(int_part)
        if len(int_part) > 3:
            variants.append(grouped)
    return list(dict.fromkeys(variants))  # dedup, preserve order


def _description_tokens(description: str) -> set[str]:
    """Tokenize a description narration into upper-case alphanumeric
    tokens for cheap overlap scoring."""
    words = re.findall(r"[A-Za-z0-9]+", (description or "").upper())
    # Drop 1-2 char tokens (too many false-positive matches).
    return {w for w in words if len(w) >= 3}


def find_row_line_in_raw_text(
    amount: float,
    description: str,
    raw_lines: Iterable[str],
) -> Optional[int]:
    """Locate the raw-file line that a tx row was extracted from.

    Heuristic: scan each raw line for the amount (in any of the
    common HDFC render forms). For each amount hit, score by how
    many description tokens overlap that line. If ONE line has the
    highest score AND at least one description token match → return
    that line's 0-indexed position. If no match, or a tie, return
    None (caller treats as "can't classify — leave alone").

    Line indices are 0-indexed into the `raw_lines` sequence (which
    should be the same shape the caller feeds to detect_hdfc_sections
    so span indices align).

    Deliberately conservative: prefer false-negative (row left
    alone) over false-positive (wrong row deleted). The reconciliation
    fallback already ensures no savings tx is lost; ak-ex2's job is
    the over-parse cleanup, and missing a few over-parse rows is
    much less costly than deleting a legit savings row.
    """
    variants = _amount_variants(amount)
    desc_tokens = _description_tokens(description)
    if not desc_tokens:
        return None  # empty desc → nothing to score against

    lines = list(raw_lines)
    scored: list[tuple[int, int]] = []  # (line_idx, overlap_score)
    for i, line in enumerate(lines):
        # Cheap amount-presence check first (skip the tokenization
        # for lines that don't carry the amount at all).
        if not any(v in line for v in variants):
            continue
        line_tokens = _description_tokens(line)
        overlap = len(desc_tokens & line_tokens)
        if overlap > 0:
            scored.append((i, overlap))

    if not scored:
        return None
    # Pick the highest-overlap line. If tied on top score → ambiguous.
    scored.sort(key=lambda x: (-x[1], x[0]))
    if len(scored) >= 2 and scored[0][1] == scored[1][1]:
        return None
    return scored[0][0]


def classify_row_section(
    amount: float,
    description: str,
    raw_lines: Iterable[str],
    section_by_line: list[SectionType],
) -> SectionType:
    """Combine find_row_line_in_raw_text + build_line_to_section_map
    into a single classification call. UNKNOWN if the row can't be
    located confidently (ambiguous / no match). Caller treats
    UNKNOWN as "leave alone" — don't delete.
    """
    lines = list(raw_lines)
    line_idx = find_row_line_in_raw_text(amount, description, lines)
    if line_idx is None or line_idx >= len(section_by_line):
        return SectionType.UNKNOWN
    return section_by_line[line_idx]
