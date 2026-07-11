"""ak-ifc regression tests for utils/statement_sections.py.

Bug ak-ifc (dispatch hq-wisp-nnhr2e): HDFC monthly PDFs are COMBINED
statements — savings + credit card + fixed deposit + mutual fund +
recurring deposit stitched into one file. The pre-fix extractor
treated the whole file as one savings statement, letting other
sub-accounts leak into the savings fileID:

  Apr-2026 file: +₹292k over-parse (non-savings debits landed in
                 the savings section)
  May-2026 file: -₹50k credit + -₹109k debit missed
  All 12 HDFC files affected.

These tests lock in the section-header detector so future HDFC
statement-format tweaks (a new sub-account, changed header phrasing)
fail loudly instead of silently regressing to the pre-fix leak.

Pure-Python; no framework deps.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.statement_sections import (
    SectionType,
    SectionSpan,
    detect_hdfc_sections,
    keep_only_sections,
    summarize_sections,
)


# ── Section header pattern matching ─────────────────────────────────


class TestSectionHeaderPatterns(unittest.TestCase):
    """Every phrasing HDFC has ever emitted in real statements should
    match the expected SectionType."""

    def _classify(self, header_line):
        spans = detect_hdfc_sections(header_line + "\n(some content)\n")
        # Leading UNKNOWN prefix span is only added when the header
        # doesn't start at line 0; here the header IS line 0, so no
        # leading span exists — spans[0] is the section we want.
        self.assertGreaterEqual(len(spans), 1)
        return spans[0].section

    def test_savings_canonical(self):
        self.assertEqual(
            self._classify("Statement of Account for : SAVINGS ACCOUNT 12345******9012"),
            SectionType.SAVINGS,
        )

    def test_savings_terse(self):
        self.assertEqual(
            self._classify("Statement for : SAVINGS ACCOUNT"),
            SectionType.SAVINGS,
        )

    def test_savings_all_caps(self):
        self.assertEqual(
            self._classify("STATEMENT OF ACCOUNT FOR : SAVINGS ACCOUNT"),
            SectionType.SAVINGS,
        )

    def test_savings_lowercase(self):
        self.assertEqual(
            self._classify("statement of account for: savings account"),
            SectionType.SAVINGS,
        )

    def test_current(self):
        self.assertEqual(
            self._classify("Statement of Account for : CURRENT ACCOUNT"),
            SectionType.CURRENT,
        )

    def test_credit_card_two_words(self):
        self.assertEqual(
            self._classify("Statement of Account for : CREDIT CARD"),
            SectionType.CREDIT_CARD,
        )

    def test_credit_card_one_word(self):
        self.assertEqual(
            self._classify("Statement of Account for : CREDITCARD ..."),
            SectionType.CREDIT_CARD,
        )

    def test_fixed_deposit(self):
        self.assertEqual(
            self._classify("Statement of Account for : FIXED DEPOSIT"),
            SectionType.FIXED_DEPOSIT,
        )

    def test_recurring_deposit_before_fixed(self):
        """Ordering guard: RECURRING must be checked before FIXED
        because 'FIXED DEPOSIT' would substring-match 'RECURRING
        DEPOSIT' if we searched in the wrong order.
        """
        self.assertEqual(
            self._classify("Statement of Account for : RECURRING DEPOSIT"),
            SectionType.RECURRING_DEPOSIT,
        )

    def test_mutual_fund(self):
        self.assertEqual(
            self._classify("Statement of Account for : MUTUAL FUND"),
            SectionType.MUTUAL_FUND,
        )

    def test_ppf(self):
        self.assertEqual(
            self._classify("Statement of Account for : PPF"),
            SectionType.PPF,
        )

    def test_public_provident_alias(self):
        self.assertEqual(
            self._classify("Statement of Account for : PUBLIC PROVIDENT FUND"),
            SectionType.PPF,
        )

    def test_unrelated_line_is_not_a_header(self):
        """Random narration line must not trigger a section."""
        spans = detect_hdfc_sections(
            "Some txn narration mentioning savings account balance\n"
            "amount 1234\n"
        )
        # No section header found → whole file falls back to a single
        # UNKNOWN span (documented behavior in detect_hdfc_sections).
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].section, SectionType.UNKNOWN)


# ── Multi-section boundary logic ────────────────────────────────────


class TestMultiSection(unittest.TestCase):
    """Real combined statements have multiple section headers in
    sequence. Boundaries must be correct so downstream filters keep
    only the intended rows."""

    def test_leading_unknown_span_before_first_header(self):
        """Lines 0-3 (account holder details, address block) come
        BEFORE any 'Statement of Account for :' header. Those get
        their own UNKNOWN span at the start."""
        text = (
            "Vidish Rajkumar\n"
            "12 Main Street\n"
            "Bengaluru 560001\n"
            "\n"
            "Statement of Account for : SAVINGS ACCOUNT ****9012\n"
            "DATE  NARRATION  AMOUNT\n"
            "01/04  Rent 25000\n"
        )
        spans = detect_hdfc_sections(text)
        self.assertEqual(spans[0].section, SectionType.UNKNOWN)
        self.assertEqual(spans[0].start_line, 0)
        # Header is on line 4 (0-indexed); UNKNOWN runs 0..3.
        self.assertEqual(spans[0].end_line, 3)
        self.assertEqual(spans[1].section, SectionType.SAVINGS)
        self.assertEqual(spans[1].start_line, 4)

    def test_five_section_hdfc_layout(self):
        """Full-shape HDFC combined statement — 5 sections in
        expected order (savings, cc, fd, mf, rd)."""
        text = "\n".join([
            "Vidish Rajkumar",
            "Statement of Account for : SAVINGS ACCOUNT",  # line 1
            "savings row 1",
            "savings row 2",
            "Statement of Account for : CREDIT CARD",       # line 4
            "cc row 1",
            "cc row 2",
            "Statement of Account for : FIXED DEPOSIT",     # line 7
            "fd row",
            "Statement of Account for : MUTUAL FUND",       # line 9
            "mf row",
            "Statement of Account for : RECURRING DEPOSIT", # line 11
            "rd row",
        ])
        spans = detect_hdfc_sections(text)
        # Expect 6 spans: leading UNKNOWN + 5 sections.
        section_types = [s.section for s in spans]
        self.assertEqual(section_types, [
            SectionType.UNKNOWN,
            SectionType.SAVINGS,
            SectionType.CREDIT_CARD,
            SectionType.FIXED_DEPOSIT,
            SectionType.MUTUAL_FUND,
            SectionType.RECURRING_DEPOSIT,
        ])

    def test_section_end_is_inclusive_of_last_line(self):
        """A section's end_line must point at the last line BELONGING
        to that section, not the line where the next header starts."""
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",  # line 0
            "row A",  # line 1
            "row B",  # line 2
            "Statement of Account for : CREDIT CARD",  # line 3
            "cc row",  # line 4
        ])
        spans = detect_hdfc_sections(text)
        # Filter to sections (skip any leading UNKNOWN — none here
        # since first line IS a header).
        savings = next(s for s in spans if s.section == SectionType.SAVINGS)
        credit = next(s for s in spans if s.section == SectionType.CREDIT_CARD)
        # Savings starts at header line 0, ends at line 2 (the last
        # 'row B' before the next header).
        self.assertEqual(savings.start_line, 0)
        self.assertEqual(savings.end_line, 2)
        # Credit starts at header line 3, ends at line 4 (EOF).
        self.assertEqual(credit.start_line, 3)
        self.assertEqual(credit.end_line, 4)

    def test_multiple_savings_sections_stay_separate(self):
        """HDFC statements sometimes have the savings section split
        by a re-header on a new page. Each 'Statement for : SAVINGS'
        creates its OWN span so accounting stays honest — we don't
        want to coalesce them into one and drop content between."""
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT ****9012",
            "row A",
            "Statement of Account for : SAVINGS ACCOUNT ****9012",
            "row B",
        ])
        spans = detect_hdfc_sections(text)
        savings_spans = [s for s in spans if s.section == SectionType.SAVINGS]
        # Two savings spans (one per header line).
        self.assertEqual(len(savings_spans), 2)


# ── keep_only_sections filter ────────────────────────────────────────


class TestKeepOnlySections(unittest.TestCase):
    """The filter that strips non-savings lines from the raw text
    before it's handed to the LLM."""

    def test_savings_kept_others_blanked(self):
        text = "\n".join([
            "PROLOGUE",                                       # 0 UNKNOWN
            "Statement of Account for : SAVINGS ACCOUNT",     # 1 SAVINGS start
            "savings row 1",                                  # 2 SAVINGS
            "Statement of Account for : CREDIT CARD",         # 3 CC start
            "cc row 1",                                       # 4 CC
        ])
        filtered, spans = keep_only_sections(text, [SectionType.SAVINGS])
        lines = filtered.split("\n")
        # Non-savings lines are blanked but the array length stays
        # identical so line-numbering downstream still lines up.
        self.assertEqual(len(lines), 5)
        self.assertEqual(lines[0], "")  # UNKNOWN prologue blanked
        # Savings header + row both kept.
        self.assertIn("SAVINGS ACCOUNT", lines[1])
        self.assertEqual(lines[2], "savings row 1")
        # CC header + row blanked.
        self.assertEqual(lines[3], "")
        self.assertEqual(lines[4], "")

    def test_no_savings_section_returns_all_blanks(self):
        """Statement with only credit-card content should filter to
        an entirely blank text — nothing to extract."""
        text = "\n".join([
            "Statement of Account for : CREDIT CARD",
            "cc row 1",
            "cc row 2",
        ])
        filtered, _ = keep_only_sections(text, [SectionType.SAVINGS])
        # All non-blank content stripped.
        self.assertEqual(filtered.strip(), "")

    def test_compact_mode_removes_blanks(self):
        """Non-default compact mode removes stripped lines entirely.
        Useful when the caller manages its own line numbering."""
        text = "\n".join([
            "PROLOGUE",
            "Statement of Account for : SAVINGS ACCOUNT",
            "savings row",
            "Statement of Account for : CREDIT CARD",
            "cc row",
        ])
        filtered, _ = keep_only_sections(
            text, [SectionType.SAVINGS],
            replace_stripped_with_placeholder=False,
        )
        self.assertIn("SAVINGS ACCOUNT", filtered)
        self.assertIn("savings row", filtered)
        self.assertNotIn("CREDIT CARD", filtered)
        self.assertNotIn("cc row", filtered)
        self.assertNotIn("PROLOGUE", filtered)

    def test_multiple_types_kept(self):
        """The keep-list is a set — multiple sections can be kept in
        one pass. Useful if we ever wire in HDFC_REGALIA extraction
        from the CREDIT_CARD section on the same file."""
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "savings row",
            "Statement of Account for : CREDIT CARD",
            "cc row",
            "Statement of Account for : FIXED DEPOSIT",
            "fd row",
        ])
        filtered, _ = keep_only_sections(
            text,
            [SectionType.SAVINGS, SectionType.CREDIT_CARD],
        )
        self.assertIn("savings row", filtered)
        self.assertIn("cc row", filtered)
        self.assertNotIn("fd row", filtered)


# ── Backward compat: single-account statement ────────────────────────


class TestSingleAccountFallback(unittest.TestCase):
    """Pre-ak-ifc single-account HDFC statements (no combined-format
    section headers) must still be handled correctly — the caller in
    mailProcessorService falls back to full-text extraction when the
    detector finds zero savings spans."""

    def test_no_headers_gives_single_unknown_span(self):
        """A single-account statement has no 'Statement of Account
        for :' headers. Detector returns one UNKNOWN span covering
        the whole file."""
        text = "\n".join([
            "some savings-style narration",
            "01/04  Rent Payment  25000",
            "02/04  Salary Credit  -50000",
        ])
        spans = detect_hdfc_sections(text)
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].section, SectionType.UNKNOWN)
        self.assertEqual(spans[0].start_line, 0)
        self.assertEqual(spans[0].end_line, 2)

    def test_single_account_filter_blanks_everything(self):
        """With keep=[SAVINGS] and no SAVINGS section detected, the
        filter returns all-blank. mailProcessorService's caller
        detects this (savings_span_count == 0) and falls back to
        unfiltered extraction so we don't drop the whole file."""
        text = "01/04  Rent  25000\n02/04  Salary  -50000\n"
        filtered, _ = keep_only_sections(text, [SectionType.SAVINGS])
        self.assertEqual(filtered.strip(), "")


# ── summarize_sections helper ────────────────────────────────────────


class TestSummarize(unittest.TestCase):
    def test_summary_format(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "row A",
            "Statement of Account for : CREDIT CARD",
            "cc row",
        ])
        spans = detect_hdfc_sections(text)
        summary = summarize_sections(spans)
        self.assertIn("SAVINGS(0-1)", summary)
        self.assertIn("CREDIT_CARD(2-3)", summary)


# ── ak-ifc v2: SAVINGS-summary reconciliation ───────────────────────


from utils.statement_sections import (
    HdfcSavingsSummary,
    ReconciliationResult,
    check_hdfc_reconciliation_from_totals,
    check_hdfc_savings_reconciliation,
    parse_hdfc_savings_summary,
)


class TestParseSavingsSummary(unittest.TestCase):
    """parse_hdfc_savings_summary: extracts opening/closing balance
    and total debits/credits from raw HDFC text."""

    def test_basic_totals_extracted(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "Opening Balance : 12,345.67",
            "Total Debits : 45,678.00",
            "Total Credits : 33,333.33",
            "Closing Balance : 100.00",
        ])
        summary = parse_hdfc_savings_summary(text)
        self.assertIsNotNone(summary)
        self.assertAlmostEqual(summary.opening_balance, 12345.67)
        self.assertAlmostEqual(summary.closing_balance, 100.00)
        self.assertAlmostEqual(summary.total_debits, 45678.00)
        self.assertAlmostEqual(summary.total_credits, 33333.33)

    def test_alternate_amount_of_wording(self):
        """Some HDFC layouts say "Amount of Debits" instead of "Total
        Debits". Both should parse."""
        text = "\n".join([
            "Statement of Account for : SAVINGS",
            "Amount of Debits: 1,00,000.50",
            "Amount of Credits: 2,50,000.00",
        ])
        summary = parse_hdfc_savings_summary(text)
        self.assertIsNotNone(summary)
        self.assertAlmostEqual(summary.total_debits, 100000.50)
        self.assertAlmostEqual(summary.total_credits, 250000.00)

    def test_currency_prefix_tolerated(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS",
            "Opening Balance ₹ 12,345.67",
            "Closing Balance Rs. 100.00",
        ])
        summary = parse_hdfc_savings_summary(text)
        self.assertIsNotNone(summary)
        self.assertAlmostEqual(summary.opening_balance, 12345.67)
        self.assertAlmostEqual(summary.closing_balance, 100.00)

    def test_dr_cr_suffix_tolerated(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS",
            "Opening Balance 12,345.67 CR",
            "Closing Balance 100.00 Cr",
        ])
        summary = parse_hdfc_savings_summary(text)
        self.assertIsNotNone(summary)
        self.assertAlmostEqual(summary.opening_balance, 12345.67)

    def test_no_summary_returns_none(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS",
            "01/04  Rent Payment  25000",
            "02/04  Salary  -50000",
        ])
        summary = parse_hdfc_savings_summary(text)
        self.assertIsNone(summary)

    def test_partial_summary_returns_populated(self):
        """Only some fields present → we return a summary with those
        set and the rest as None. The reconciler skips None fields."""
        text = "\n".join([
            "Statement of Account for : SAVINGS",
            "Total Debits: 45,678.00",
            # no credits, no opening/closing
        ])
        summary = parse_hdfc_savings_summary(text)
        self.assertIsNotNone(summary)
        self.assertAlmostEqual(summary.total_debits, 45678.00)
        self.assertIsNone(summary.total_credits)

    def test_debit_credit_counts(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS",
            "No. of Debits: 42",
            "Number of Credits: 8",
            "Total Debits: 45678.00",
        ])
        summary = parse_hdfc_savings_summary(text)
        self.assertIsNotNone(summary)
        self.assertEqual(summary.debit_count, 42)
        self.assertEqual(summary.credit_count, 8)

    def test_spans_scoping_ignores_other_sections(self):
        """When `spans` is provided, we scan only the SAVINGS-section
        lines — the credit-card section's own totals must NOT bleed
        into the SAVINGS summary parse."""
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",  # line 0
            "Total Debits: 45,000.00",                     # line 1
            "Total Credits: 30,000.00",                    # line 2
            "Statement of Account for : CREDIT CARD",      # line 3
            "Total Debits: 999,999.99",                    # line 4 (must NOT be picked up)
            "Total Credits: 111,111.11",                   # line 5
        ])
        spans = detect_hdfc_sections(text)
        summary = parse_hdfc_savings_summary(text, spans=spans)
        self.assertIsNotNone(summary)
        # Must pick up the savings-section totals, NOT the CC totals.
        self.assertAlmostEqual(summary.total_debits, 45000.00)
        self.assertAlmostEqual(summary.total_credits, 30000.00)

    def test_spans_scoping_no_savings_returns_none(self):
        """If spans has no SAVINGS section, we return None (nothing
        to reconcile against — a strict view over the caller's
        section knowledge)."""
        text = "\n".join([
            "Statement of Account for : CREDIT CARD",
            "Total Debits: 45,000.00",
        ])
        spans = detect_hdfc_sections(text)
        summary = parse_hdfc_savings_summary(text, spans=spans)
        self.assertIsNone(summary)


class TestCheckReconciliation(unittest.TestCase):
    """check_hdfc_savings_reconciliation compares extracted totals
    against a HdfcSavingsSummary and flags divergence beyond tolerance."""

    def test_within_tolerance_clean(self):
        rows = [
            {"amount": 100.00},  # debit
            {"amount": 200.00},  # debit
            {"amount": -50.00},  # credit
        ]
        summary = HdfcSavingsSummary(
            total_debits=300.00, total_credits=50.00,
        )
        recon = check_hdfc_savings_reconciliation(rows, summary)
        self.assertFalse(recon.diverged)
        self.assertEqual(recon.reason, "within tolerance")
        self.assertAlmostEqual(recon.extracted_debits, 300.0)
        self.assertAlmostEqual(recon.extracted_credits, 50.0)

    def test_debits_diverge_flags(self):
        rows = [
            {"amount": 100.00},   # debit
            {"amount": 200.00},   # debit — extracted total = 300
        ]
        summary = HdfcSavingsSummary(
            total_debits=350.00, total_credits=0.0,
        )
        recon = check_hdfc_savings_reconciliation(rows, summary)
        self.assertTrue(recon.diverged)
        self.assertIn("debits", recon.reason)
        self.assertIn("300.00", recon.reason)
        self.assertIn("350.00", recon.reason)

    def test_credits_diverge_flags(self):
        rows = [
            {"amount": -100.00},  # credit magnitude 100
        ]
        summary = HdfcSavingsSummary(
            total_debits=0.0, total_credits=200.00,  # extracted misses 100
        )
        recon = check_hdfc_savings_reconciliation(rows, summary)
        self.assertTrue(recon.diverged)
        self.assertIn("credits", recon.reason)

    def test_1_rupee_rounding_ignored(self):
        """Default tolerance is ₹1 to absorb HDFC's summary-line
        rounding. Extracted 299.60 vs stated 300.00 → clean."""
        rows = [
            {"amount": 149.30},
            {"amount": 150.30},   # sum 299.60
        ]
        summary = HdfcSavingsSummary(total_debits=300.00, total_credits=0.0)
        recon = check_hdfc_savings_reconciliation(rows, summary)
        self.assertFalse(recon.diverged)

    def test_missing_summary_returns_skip(self):
        """summary=None → check couldn't run; NOT a clean pass. The
        caller inspects reason and logs the skip."""
        rows = [{"amount": 100.0}]
        recon = check_hdfc_savings_reconciliation(rows, None)
        self.assertFalse(recon.diverged)
        self.assertEqual(recon.reason, "no summary parsed")
        # extracted sums still computed for the caller's log:
        self.assertAlmostEqual(recon.extracted_debits, 100.0)

    def test_summary_without_totals_returns_skip(self):
        """Summary has only opening/closing balance, not total_debits
        / total_credits → nothing to reconcile against."""
        rows = [{"amount": 100.0}]
        summary = HdfcSavingsSummary(
            opening_balance=1000.0, closing_balance=900.0,
        )
        recon = check_hdfc_savings_reconciliation(rows, summary)
        self.assertFalse(recon.diverged)
        self.assertIn("no total_debits", recon.reason)

    def test_partial_check_only_stated_fields(self):
        """Summary has only total_debits (no total_credits). We check
        debits and skip credits — no false-positive divergence on
        the credit side."""
        rows = [
            {"amount": 100.0},
            {"amount": -5000.0},  # credit that summary doesn't mention
        ]
        summary = HdfcSavingsSummary(total_debits=100.0)
        recon = check_hdfc_savings_reconciliation(rows, summary)
        self.assertFalse(recon.diverged)
        self.assertEqual(recon.checked_fields, ("total_debits",))

    def test_ignores_non_numeric_amount(self):
        rows = [
            {"amount": 100.0},
            {"amount": "garbage"},   # skipped
            {"amount": None},        # skipped
        ]
        summary = HdfcSavingsSummary(total_debits=100.0, total_credits=0.0)
        recon = check_hdfc_savings_reconciliation(rows, summary)
        self.assertFalse(recon.diverged)

    def test_custom_tolerance(self):
        rows = [{"amount": 100.0}]
        summary = HdfcSavingsSummary(total_debits=110.0, total_credits=0.0)
        # ₹5 tolerance → diverged (delta=10)
        recon = check_hdfc_savings_reconciliation(rows, summary, tolerance=5.0)
        self.assertTrue(recon.diverged)
        # ₹20 tolerance → clean
        recon = check_hdfc_savings_reconciliation(rows, summary, tolerance=20.0)
        self.assertFalse(recon.diverged)


class TestReconciliationEndToEnd(unittest.TestCase):
    """End-to-end shape: raw text → parse summary → reconcile
    extracted rows. Mirrors what mailProcessorService does per-file."""

    def test_clean_run(self):
        raw = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "Opening Balance : 12,345.67",
            "Total Debits : 300.00",
            "Total Credits : 50.00",
            "Closing Balance : 12,095.67",
        ])
        spans = detect_hdfc_sections(raw)
        summary = parse_hdfc_savings_summary(raw, spans=spans)
        rows = [
            {"amount": 100.00},
            {"amount": 200.00},
            {"amount": -50.00},
        ]
        recon = check_hdfc_savings_reconciliation(rows, summary)
        self.assertFalse(recon.diverged)

    def test_multi_chunk_clean_via_totals(self):
        """ak-ifc v3 file-level baseline: three chunks each extracted
        cleanly; aggregated totals match summary; no fallback."""
        raw = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "Total Debits : 900.00",
            "Total Credits : 300.00",
        ])
        summary = parse_hdfc_savings_summary(raw)
        self.assertIsNotNone(summary)
        # Simulate three chunks' contributions to the DB, aggregated.
        chunk1_debits, chunk1_credits = 300.0, 100.0
        chunk2_debits, chunk2_credits = 400.0, 150.0
        chunk3_debits, chunk3_credits = 200.0, 50.0
        agg_debits = chunk1_debits + chunk2_debits + chunk3_debits
        agg_credits = chunk1_credits + chunk2_credits + chunk3_credits
        recon = check_hdfc_reconciliation_from_totals(
            agg_debits, agg_credits, summary,
        )
        self.assertFalse(recon.diverged)

    def test_multi_chunk_chunk2_missed_via_totals(self):
        """ak-ifc v3 file-level divergence: three chunks, chunk 2's
        savings were silently under-parsed (partial header miss);
        aggregated totals fall short → divergence detected → caller
        would trigger force_no_mask re-run."""
        raw = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "Total Debits : 900.00",
            "Total Credits : 300.00",
        ])
        summary = parse_hdfc_savings_summary(raw)
        # chunk 2 contributed nothing due to header miss:
        agg_debits = 300.0 + 0.0 + 200.0        # 500 vs stated 900
        agg_credits = 100.0 + 0.0 + 50.0        # 150 vs stated 300
        recon = check_hdfc_reconciliation_from_totals(
            agg_debits, agg_credits, summary,
        )
        self.assertTrue(recon.diverged)
        # Both sides diverge:
        self.assertIn("debits", recon.reason)
        self.assertIn("credits", recon.reason)

    def test_multi_chunk_no_summary_skips(self):
        """ak-ifc v3 file-level: file has no summary → SKIP path;
        the caller logs and proceeds without fallback (no false alarm
        on statement layouts that omit the summary block)."""
        summary = None
        agg_debits, agg_credits = 500.0, 150.0
        recon = check_hdfc_reconciliation_from_totals(
            agg_debits, agg_credits, summary,
        )
        self.assertFalse(recon.diverged)
        self.assertEqual(recon.reason, "no summary parsed")

    def test_partial_header_miss_causes_divergence(self):
        """Realistic scenario: statement had two SAVINGS re-headers.
        Regex catches the first, misses the second, so ~half the
        savings text is stripped from the LLM's view → extracted
        totals fall short of stated totals → divergence flagged →
        caller falls back to unfiltered extraction. (This test
        simulates the extracted-side; the actual header-miss is
        simulated by the extractor returning a partial row set.)"""
        raw = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "Total Debits : 500.00",
            "Total Credits : 100.00",
        ])
        summary = parse_hdfc_savings_summary(raw)
        self.assertIsNotNone(summary)
        # Simulate partial extraction — only 200 of 500 debits.
        partial_rows = [{"amount": 200.00}, {"amount": -100.00}]
        recon = check_hdfc_savings_reconciliation(partial_rows, summary)
        self.assertTrue(recon.diverged)
        self.assertIn("debits", recon.reason)


# ── ak-dby: broadened HDFC section-header patterns ─────────────────
#
# The pre-ak-dby regex only matched "Statement of Account for : X".
# The 2026 Vidish_Raj_ monthly layout uses bare / abbreviated banners
# instead (Savings Account / Savings A/C / Account: … (SAVINGS)),
# which broke section detection on every 2026 file — falling back to
# unfiltered extraction WITHOUT setting reconciliation_fallback=True.
# These tests pin down the expanded patterns so a future regression
# on any of them is loud.


class TestBroadenedSavingsHeaders(unittest.TestCase):
    """ak-dby: alternate SAVINGS header forms observed in 2026
    HDFC monthly layouts."""

    def _first_span(self, text):
        spans = detect_hdfc_sections(text)
        # Skip the leading UNKNOWN span if present.
        for s in spans:
            if s.section != SectionType.UNKNOWN:
                return s
        return None

    def test_savings_account_bare(self):
        text = "Header line\nSAVINGS ACCOUNT\nrow"
        span = self._first_span(text)
        self.assertIsNotNone(span)
        self.assertEqual(span.section, SectionType.SAVINGS)

    def test_savings_ac_abbrev(self):
        text = "Header\nSAVINGS A/C 50100XXXX\nrow"
        span = self._first_span(text)
        self.assertIsNotNone(span)
        self.assertEqual(span.section, SectionType.SAVINGS)

    def test_savings_bank_account(self):
        text = "Header\nSavings Bank Account\nrow"
        span = self._first_span(text)
        self.assertIsNotNone(span)
        self.assertEqual(span.section, SectionType.SAVINGS)

    def test_account_type_savings(self):
        text = "Header\nAccount Type : Savings\nrow"
        span = self._first_span(text)
        self.assertIsNotNone(span)
        self.assertEqual(span.section, SectionType.SAVINGS)

    def test_parenthetical_savings(self):
        text = "Header\nAccount: 50100XXXXX (SAVINGS)\nrow"
        span = self._first_span(text)
        self.assertIsNotNone(span)
        self.assertEqual(span.section, SectionType.SAVINGS)


class TestBroadenedCreditCardHeaders(unittest.TestCase):
    def _first_span(self, text):
        spans = detect_hdfc_sections(text)
        for s in spans:
            if s.section != SectionType.UNKNOWN:
                return s
        return None

    def test_credit_card_account(self):
        text = "Header\nCREDIT CARD ACCOUNT\nrow"
        span = self._first_span(text)
        self.assertEqual(span.section, SectionType.CREDIT_CARD)

    def test_credit_card_ac(self):
        text = "Header\nCredit Card A/C\nrow"
        span = self._first_span(text)
        self.assertEqual(span.section, SectionType.CREDIT_CARD)

    def test_credit_card_statement_banner(self):
        text = "Header\nCredit Card Statement\nrow"
        span = self._first_span(text)
        self.assertEqual(span.section, SectionType.CREDIT_CARD)


class TestBroadenedFixedRecurringHeaders(unittest.TestCase):
    """Both bare and abbreviated forms — with the RECURRING-before-
    FIXED ordering guard intact so RD substrings don't hijack FD."""

    def _all_sections(self, text):
        return [s.section for s in detect_hdfc_sections(text)
                if s.section != SectionType.UNKNOWN]

    def test_fd_abbrev(self):
        text = "Header\nFD A/C 12345\nrow"
        sections = self._all_sections(text)
        self.assertIn(SectionType.FIXED_DEPOSIT, sections)
        self.assertNotIn(SectionType.RECURRING_DEPOSIT, sections)

    def test_rd_abbrev(self):
        text = "Header\nRD Account 55555\nrow"
        sections = self._all_sections(text)
        self.assertIn(SectionType.RECURRING_DEPOSIT, sections)
        self.assertNotIn(SectionType.FIXED_DEPOSIT, sections)

    def test_recurring_deposit_bare(self):
        text = "Header\nRECURRING DEPOSIT ACCOUNT\nrow"
        sections = self._all_sections(text)
        self.assertIn(SectionType.RECURRING_DEPOSIT, sections)

    def test_fixed_deposit_bare(self):
        text = "Header\nFIXED DEPOSIT A/C\nrow"
        sections = self._all_sections(text)
        self.assertIn(SectionType.FIXED_DEPOSIT, sections)


class TestBroadenedRealisticMultiSection(unittest.TestCase):
    """Emulate the 2026 Vidish_Raj_ layout: a combined statement
    where every section uses bare banner headers (no "Statement of
    Account for" prefix). Detector must find every section."""

    def test_2026_style_layout(self):
        text = "\n".join([
            "HDFC Bank Combined Statement",
            "SAVINGS ACCOUNT 50100XXXXX",  # bare banner
            "01/04  Salary Credit  50,000.00",
            "02/04  UPI Payment  1,234.56",
            "CREDIT CARD A/C",              # abbrev banner
            "03/04  Amazon Purchase  2,500.00",
            "FD A/C 22345",                 # FD abbrev
            "05/04  FD Interest  750.00",
            "RD ACCOUNT 33345",             # RD abbrev
            "06/04  RD Deposit  5,000.00",
            "MUTUAL FUND FOLIO 44345",      # MF folio banner
            "07/04  MF NAV  120.50",
        ])
        sections = [
            s.section for s in detect_hdfc_sections(text)
            if s.section != SectionType.UNKNOWN
        ]
        self.assertIn(SectionType.SAVINGS, sections)
        self.assertIn(SectionType.CREDIT_CARD, sections)
        self.assertIn(SectionType.FIXED_DEPOSIT, sections)
        self.assertIn(SectionType.RECURRING_DEPOSIT, sections)
        self.assertIn(SectionType.MUTUAL_FUND, sections)


class TestNarrationDoesNotFalsePositive(unittest.TestCase):
    """ak-dby's bare-banner patterns are anchored to line start so a
    transaction narration mentioning "savings account" mid-line can't
    trip them. This test locks that anchor guarantee."""

    def test_narration_with_savings_account_mid_line(self):
        text = "Some txn narration mentioning savings account balance"
        spans = detect_hdfc_sections(text)
        self.assertEqual(len(spans), 1)
        self.assertEqual(spans[0].section, SectionType.UNKNOWN)

    def test_narration_with_credit_card_mid_line(self):
        text = "Payment to Amazon via credit card A/C"
        spans = detect_hdfc_sections(text)
        self.assertEqual(spans[0].section, SectionType.UNKNOWN)

    def test_narration_with_fd_mid_line(self):
        text = "Interest posting from FD Account renewal"
        spans = detect_hdfc_sections(text)
        self.assertEqual(spans[0].section, SectionType.UNKNOWN)

    def test_leading_whitespace_still_matches(self):
        """Anchors allow leading whitespace — indentation on banner
        lines shouldn't hide a real section header."""
        text = "Header\n   SAVINGS ACCOUNT 50100XXXX\nrow"
        spans = detect_hdfc_sections(text)
        savings = [s for s in spans if s.section == SectionType.SAVINGS]
        self.assertTrue(savings)


class TestExistingPatternsStillMatch(unittest.TestCase):
    """Belt-and-braces regression guard: the canonical "Statement of
    Account for : X" phrasings — which the 47 pre-existing tests
    already cover — still match after ak-dby's additions."""

    def test_canonical_savings(self):
        text = "Header\nStatement of Account for : SAVINGS ACCOUNT\nrow"
        spans = detect_hdfc_sections(text)
        savings = [s for s in spans if s.section == SectionType.SAVINGS]
        self.assertTrue(savings, "canonical SAVINGS phrasing regressed")

    def test_canonical_credit_card(self):
        text = "Header\nStatement for : CREDIT CARD\nrow"
        spans = detect_hdfc_sections(text)
        cc = [s for s in spans if s.section == SectionType.CREDIT_CARD]
        self.assertTrue(cc, "canonical CREDIT_CARD phrasing regressed")


if __name__ == "__main__":
    print("ak-ifc statement-section detector regression tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
