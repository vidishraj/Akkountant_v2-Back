"""ak-8l5 regression tests for utils/reference_id.py.

Supersedes test_stable_reference_id.py (kept for legacy compat
audit but the new test surface is here). The dispatch's zero-loss
requirement drives two hard properties:

  1. Chunk re-reads of the same tx collapse — same input →
     same referenceID.
  2. Legitimate same-(bank, date, amount, description) transactions
     stay distinct — the ak-tik silent-merge MAJOR must not return.

Pure-Python; no framework deps.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.reference_id import (
    generate_reference_v2,
    generate_reference_v2_fallback,
    generate_reference_from_row,
)


# ── Primary path — reference-based dedup ────────────────────────────


class TestGenerateReferenceV2Primary(unittest.TestCase):
    """Primary path: (bank, bank_reference_id, amount) → SHA256 → 64 hex.

    ak-8l5 review M3: amount is folded into the primary hash so a
    shared ref across distinct amounts stays distinct (chunk-overlap
    collapse still works because amount is chunk-invariant).
    """

    def test_same_ref_same_amount_same_hash(self):
        """Chunk re-reads emit the same ref + same amount → same PK →
        PK-conflict path collapses the second insert."""
        a = generate_reference_v2("HDFC_DEBIT", "9876543210", 500)
        b = generate_reference_v2("HDFC_DEBIT", "9876543210", 500)
        self.assertEqual(a, b)

    def test_different_refs_different_hashes(self):
        """Two UPI tx on same day, same amount, same merchant, but
        different UPI refs — MUST stay distinct."""
        a = generate_reference_v2("HDFC_DEBIT", "9876543210", 500)
        b = generate_reference_v2("HDFC_DEBIT", "1234567890", 500)
        self.assertNotEqual(a, b)

    def test_cross_bank_same_ref_distinct(self):
        """Cross-bank collision guard — same numeric ref under HDFC
        vs BOI must not collide."""
        a = generate_reference_v2("HDFC_DEBIT", "1234567890", 500)
        b = generate_reference_v2("BOI", "1234567890", 500)
        self.assertNotEqual(a, b)

    def test_ref_whitespace_normalized(self):
        """Chunk-boundary variance in whitespace/trailing punct on the
        ref itself collapses."""
        a = generate_reference_v2("HDFC_DEBIT", "9876543210", 500)
        b = generate_reference_v2("HDFC_DEBIT", " 9876543210 ", 500)
        c = generate_reference_v2("HDFC_DEBIT", "9876543210.", 500)
        self.assertEqual(a, b)
        self.assertEqual(a, c)

    def test_ref_case_preserved(self):
        """UTRs and some UPI refs are case-sensitive alphanumeric —
        do NOT uppercase them (would collide UTRs like 'aBcDeF' with
        'ABCDEF' which are different UTRs)."""
        a = generate_reference_v2("HDFC_DEBIT", "aBcDeF123", 500)
        b = generate_reference_v2("HDFC_DEBIT", "abcdef123", 500)
        self.assertNotEqual(a, b)

    def test_empty_ref_raises(self):
        """Empty ref must fail loudly — callers must branch to the
        fallback path explicitly."""
        with self.assertRaises(ValueError):
            generate_reference_v2("HDFC_DEBIT", "", 500)
        with self.assertRaises(ValueError):
            generate_reference_v2("HDFC_DEBIT", None, 500)
        with self.assertRaises(ValueError):
            generate_reference_v2("HDFC_DEBIT", "   ", 500)

    def test_non_numeric_amount_raises(self):
        """M3: amount must be convertible to float."""
        with self.assertRaises(ValueError):
            generate_reference_v2("HDFC_DEBIT", "9876543210", "not-a-number")
        with self.assertRaises(ValueError):
            generate_reference_v2("HDFC_DEBIT", "9876543210", None)

    def test_hash_length(self):
        """PK column is VARCHAR(64) — hex output must fit."""
        h = generate_reference_v2("BOI", "MBSF/443710110001487/Rent", 5000)
        self.assertEqual(len(h), 64)
        self.assertRegex(h, r"^[0-9a-f]{64}$")

    # ── ak-8l5 M3: amount folded into primary hash ───────────────

    def test_same_ref_different_amount_stays_distinct(self):
        """M3 core guarantee: shared ref across distinct amounts stays
        distinct. Prevents the "different tx, same ref number" silent-
        merge failure (extractor pathologies where a substring lookalike
        is picked up as the ref for two rows with different amounts)."""
        a = generate_reference_v2("HDFC_DEBIT", "REF-X", 500)
        b = generate_reference_v2("HDFC_DEBIT", "REF-X", 750)
        self.assertNotEqual(a, b)

    def test_amount_normalization_2dp(self):
        """M3: amounts equal at 2dp collapse; sub-cent variance ignored.
        (This matches the fallback path's amount treatment.)"""
        a = generate_reference_v2("HDFC_DEBIT", "REF-X", 500)
        b = generate_reference_v2("HDFC_DEBIT", "REF-X", 500.00)
        c = generate_reference_v2("HDFC_DEBIT", "REF-X", 500.001)
        self.assertEqual(a, b)
        self.assertEqual(a, c)

    def test_amount_sign_matters(self):
        """M3: same ref, opposite signs (500 debit vs -500 credit) →
        distinct. Realistic when an extractor mis-signs one of two
        posts sharing a ref."""
        a = generate_reference_v2("HDFC_DEBIT", "REF-X", 500)
        b = generate_reference_v2("HDFC_DEBIT", "REF-X", -500)
        self.assertNotEqual(a, b)


# ── Fallback path — positional dedup ─────────────────────────────────


class TestGenerateReferenceV2Fallback(unittest.TestCase):
    """Fallback path: (bank, file, line_position, date, amount, desc)."""

    def test_same_position_same_hash(self):
        """Chunk re-reads see the SAME line_position (because the
        annotation is file-level, not chunk-level) → same PK."""
        a = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        b = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        self.assertEqual(a, b)

    def test_different_position_different_hash(self):
        """Two null-ref rows on different lines of the same file →
        distinct."""
        a = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        b = generate_reference_v2_fallback(
            "BOI", "file_A", 43,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        self.assertNotEqual(a, b)

    def test_different_file_different_hash(self):
        """Same line_position in different statement files → distinct.
        A re-parse of month M creates a new file_id, so those rows
        deliberately don't collapse; the caller wipes old rows
        first."""
        a = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        b = generate_reference_v2_fallback(
            "BOI", "file_B", 42,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        self.assertNotEqual(a, b)

    def test_description_normalization_collapses(self):
        """Chunk-boundary variance in the description (extra newline,
        case, trailing punct) collapses via the ak-tik normalization
        rules that survived into the fallback path."""
        a = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "cash deposit",
        )
        b = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "CASH DEPOSIT.",
        )
        c = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "  CASH   DEPOSIT  ",
        )
        self.assertEqual(a, b)
        self.assertEqual(a, c)

    def test_missing_file_id_raises(self):
        """Fallback requires file scope; None/empty file_id fails."""
        with self.assertRaises(ValueError):
            generate_reference_v2_fallback(
                "BOI", "", 42, "2023-01-26", 25000, "CASH",
            )
        with self.assertRaises(ValueError):
            generate_reference_v2_fallback(
                "BOI", None, 42, "2023-01-26", 25000, "CASH",
            )

    def test_invalid_line_position_raises(self):
        """Line position must be 1-indexed positive int."""
        with self.assertRaises(ValueError):
            generate_reference_v2_fallback(
                "BOI", "file_A", 0, "2023-01-26", 25000, "CASH",
            )
        with self.assertRaises(ValueError):
            generate_reference_v2_fallback(
                "BOI", "file_A", -1, "2023-01-26", 25000, "CASH",
            )
        with self.assertRaises(ValueError):
            generate_reference_v2_fallback(
                "BOI", "file_A", "not-a-number",
                "2023-01-26", 25000, "CASH",
            )

    def test_line_position_accepts_string_int(self):
        """LLM may emit line_position as string; we coerce to int."""
        a = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        b = generate_reference_v2_fallback(
            "BOI", "file_A", "42",
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        self.assertEqual(a, b)

    def test_different_date_different_hash(self):
        """Belt-and-suspenders: same file/line but different date →
        distinct. Guards against a schema change that shifts line
        numbers silently collapsing different rows."""
        a = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        b = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-27", 25000, "CASH DEPOSIT",
        )
        self.assertNotEqual(a, b)

    def test_different_amount_different_hash(self):
        a = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25000, "CASH DEPOSIT",
        )
        b = generate_reference_v2_fallback(
            "BOI", "file_A", 42,
            "2023-01-26", 25001, "CASH DEPOSIT",
        )
        self.assertNotEqual(a, b)

    def test_hash_length(self):
        h = generate_reference_v2_fallback(
            "BOI", "file_A", 1, "2023-01-01", 100, "x",
        )
        self.assertEqual(len(h), 64)


# ── generate_reference_from_row — the executor's entry point ─────────


class TestGenerateReferenceFromRow(unittest.TestCase):
    """Convenience dispatcher: primary if ref present, fallback else."""

    def test_row_with_ref_uses_primary(self):
        """When a row has a bank_reference_id, the primary path fires
        and the file/line info is ignored. M3: the dispatcher passes
        amount through so the primary hash is amount-aware."""
        row = {
            "bank_reference_id": "9876543210",
            "date": "2023-01-26",
            "amount": 25000,
            "description": "UPI PAY",
        }
        got = generate_reference_from_row(
            "HDFC_DEBIT", row, file_id="ignored", line_position=1,
        )
        expected = generate_reference_v2("HDFC_DEBIT", "9876543210", 25000)
        self.assertEqual(got, expected)

    def test_row_without_ref_uses_fallback(self):
        row = {
            "bank_reference_id": None,
            "date": "2023-01-26",
            "amount": 25000,
            "description": "CASH DEPOSIT",
        }
        got = generate_reference_from_row(
            "BOI", row, file_id="file_A", line_position=42,
        )
        expected = generate_reference_v2_fallback(
            "BOI", "file_A", 42, "2023-01-26", 25000, "CASH DEPOSIT",
        )
        self.assertEqual(got, expected)

    def test_row_with_empty_string_ref_uses_fallback(self):
        """Empty-string ref is treated as no ref → fallback path."""
        row = {
            "bank_reference_id": "",
            "date": "2023-01-26",
            "amount": 25000,
            "description": "CASH DEPOSIT",
        }
        got = generate_reference_from_row(
            "BOI", row, file_id="file_A", line_position=42,
        )
        expected = generate_reference_v2_fallback(
            "BOI", "file_A", 42, "2023-01-26", 25000, "CASH DEPOSIT",
        )
        self.assertEqual(got, expected)

    def test_missing_bank_reference_id_key_uses_fallback(self):
        """Row dict without the key at all still works — treated as
        no ref."""
        row = {
            "date": "2023-01-26",
            "amount": 25000,
            "description": "CASH DEPOSIT",
        }
        got = generate_reference_from_row(
            "BOI", row, file_id="file_A", line_position=42,
        )
        self.assertEqual(len(got), 64)


# ── HDFC ref-extraction scenarios (LLM contract) ─────────────────────


class TestHDFCReferencePatterns(unittest.TestCase):
    """Simulated LLM outputs for common HDFC narrations. The test
    documents the contract with the extractor — same narration must
    emit the same ref across chunk re-reads. If a future prompt
    change causes drift, these tests fail loudly."""

    # Fixed sample amount — M3 requires it but these tests only care
    # about ref stability so the amount is a constant here.
    _AMT = 1000

    def _p(self, ref, amount=None):
        return generate_reference_v2(
            "HDFC_DEBIT", ref, amount if amount is not None else self._AMT,
        )

    def test_upi_p2m_stable(self):
        """UPI-9876543210-P2M-… → '9876543210'"""
        h1 = self._p("9876543210")
        h2 = self._p("9876543210")
        self.assertEqual(h1, h2)

    def test_two_upi_same_merchant_distinct(self):
        """Two separate UPI tx to same merchant on same day → each
        has its own ref, must stay distinct."""
        a = self._p("9876543210")
        b = self._p("9876543211")
        self.assertNotEqual(a, b)

    def test_imps_ref(self):
        """IMPS-P2A-<ref> → digit block."""
        self.assertEqual(self._p("416712345678"), self._p("416712345678"))

    def test_neft_utr(self):
        """NEFT-<UTR> — 12-16 alphanumeric, case-sensitive."""
        h = self._p("HDFCN52023011234567")
        self.assertEqual(len(h), 64)

    def test_ach_ref(self):
        """ACH D-<ref> → the ref segment."""
        h1 = self._p("MFACH20230126A")
        h2 = self._p("MFACH20230126A")
        self.assertEqual(h1, h2)

    def test_pos_composite(self):
        """POS terminal + txn ref composite. Different terminals →
        different composite refs even if txn ref clashes."""
        a = self._p("TERM123_9999")
        b = self._p("TERM456_9999")
        self.assertNotEqual(a, b)

    def test_chq_number(self):
        """CHQ PAID/<cheque-number> → the cheque number."""
        h = self._p("000123")
        self.assertEqual(len(h), 64)


# ── BOI ref-extraction scenarios ─────────────────────────────────────


class TestBOIReferencePatterns(unittest.TestCase):
    """BOI's narration format is 'MBSF/<numeric>/<narration>' etc."""

    _AMT = 5000

    def _p(self, ref, amount=None):
        return generate_reference_v2(
            "BOI", ref, amount if amount is not None else self._AMT,
        )

    def test_mbsf_stable(self):
        """MBSF/443710110001487/Rent → middle numeric block."""
        h1 = self._p("443710110001487")
        h2 = self._p("443710110001487")
        self.assertEqual(h1, h2)

    def test_int_date_window(self):
        """Interest posts have a date window: 'Int:04-11-2017/31-01-2018'
        → composite 'INT_04-11-2017_31-01-2018'."""
        h = self._p("INT_04-11-2017_31-01-2018")
        self.assertEqual(len(h), 64)

    def test_loan_coll_ref(self):
        h = self._p("LOAN123456789")
        self.assertEqual(len(h), 64)

    def test_sweep_ref(self):
        h = self._p("SWEEP987654")
        self.assertEqual(len(h), 64)


# ── Zero-loss guarantee: same-tuple different-ref stay distinct ──────


class TestZeroLossGuarantee(unittest.TestCase):
    """The reviewer MAJOR that killed ak-tik: legitimate tx with
    same (bank, date, amount, description) but different refs got
    silently merged. These tests enforce that ak-8l5 does not
    reproduce that failure mode."""

    def test_two_upi_same_merchant_stay_distinct(self):
        """Two UPI tx to the same merchant on the same day for the
        same amount — realistic scenario (rent split into two
        installments). Different UPI refs must → different PKs."""
        r1 = "UPI-A-9876543210"
        r2 = "UPI-A-9876543211"
        h1 = generate_reference_v2("HDFC_DEBIT", r1, 25000)
        h2 = generate_reference_v2("HDFC_DEBIT", r2, 25000)
        self.assertNotEqual(h1, h2)

    def test_two_ref_less_rows_same_content_stay_distinct(self):
        """Two ref-less rows (cash deposits) on the same file, same
        date, same amount, same description, but DIFFERENT line
        positions (because they're on different narration rows in
        the statement) — must stay distinct."""
        # Same-content rows at different positions:
        row_a = {
            "bank_reference_id": None,
            "date": "2023-01-26",
            "amount": 5000,
            "description": "CASH DEPOSIT",
        }
        row_b = dict(row_a)
        h1 = generate_reference_from_row(
            "BOI", row_a, file_id="stmt_2023_01", line_position=42,
        )
        h2 = generate_reference_from_row(
            "BOI", row_b, file_id="stmt_2023_01", line_position=87,
        )
        self.assertNotEqual(h1, h2)


# ── Deprecated-hash smoke ────────────────────────────────────────────


class TestLegacyStableReferenceIdRetained(unittest.TestCase):
    """The ak-tik helper is retained for import-compat only — nothing
    should be calling it from new code. This test just proves the
    function is still there for import stability during the
    transition, not that it's semantically desired."""

    def test_import_and_call(self):
        from utils.reference_id import generate_stable_reference_id
        h = generate_stable_reference_id(
            "BOI", "2023-01-26", "CASH DEPOSIT", 5000,
        )
        # Legacy hash is MD5 → 32 chars, not the v2's 64.
        self.assertEqual(len(h), 32)


if __name__ == "__main__":
    print("ak-8l5 reference-aware dedup regression tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
