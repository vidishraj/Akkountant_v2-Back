"""ak-8l5 review MAJOR: insert-time zero-loss backstop tests.

Reviewer's MAJOR against ak-8l5 v1: even with per-tx ref extraction,
the LLM can still hallucinate or miss a ref, so two distinct rows can
end up sharing a referenceID. The extraction layer alone cannot
guarantee "no transactions whatsoever lost" (Overseer's hard
requirement). The v2 fix adds a code-level safety net at INSERT time:

  1. Compute referenceID via the ref-aware scheme.
  2. Try to insert. On IntegrityError (PK collision), load existing.
  3. If existing.(date, amount, normalized-description) MATCHES the
     incoming row → chunk-overlap dedup path, silent drop.
  4. If DIFFERS → suffix incoming.referenceID with "-dupN" and retry.
     Both rows land; nothing is dropped.

These tests exercise `utils/insert_backstop` directly against an
in-memory stub session/model, so no flask / SQLAlchemy import chain
is required (matches the ak-tik-era test extraction pattern).
"""

import os
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.insert_backstop import (
    apply_backstop_on_collision,
    find_disambiguated_ref,
    rows_content_equal,
)
from utils.reference_id import normalize_description_for_backstop


# ── Stub session + Transactions class ────────────────────────────────


class _StubTxnRow:
    """Stand-in for the Transactions ORM object. Attribute-only —
    the backstop reads referenceID, date, amount, details, bank, user."""

    def __init__(self, **kw):
        self.referenceID = kw.get("referenceID", "")
        self.date = kw.get("date")
        self.amount = kw.get("amount")
        self.details = kw.get("details", "")
        self.bank = kw.get("bank", "HDFC_DEBIT")
        self.user = kw.get("user", "u_test")


class _StubSession:
    """In-memory session that simulates PK enforcement on referenceID.

    add() stages a row; commit() moves staged → committed OR raises
    an Exception if the staged referenceID already exists in the
    committed set. query(Transactions).filter_by(referenceID=X).first()
    returns the committed row matching X, or None.
    """

    def __init__(self):
        self.committed = {}  # referenceID → row
        self._staged = None

    def add(self, obj):
        self._staged = obj

    def commit(self):
        if self._staged is None:
            return
        obj = self._staged
        self._staged = None
        if obj.referenceID in self.committed:
            raise Exception(f"stub PK collision: {obj.referenceID}")
        self.committed[obj.referenceID] = obj

    def rollback(self):
        self._staged = None

    def query(self, _model):
        return _StubQuery(self.committed)


class _StubQuery:
    def __init__(self, committed):
        self._committed = committed
        self._filter_ref = None

    def filter_by(self, **kw):
        self._filter_ref = kw.get("referenceID")
        return self

    def first(self):
        if self._filter_ref is None:
            return None
        return self._committed.get(self._filter_ref)


def _run_insert_flow(session, incoming, logger=None):
    """Simulate the service-layer flow: try commit, on failure invoke
    the backstop. Returns the backstop outcome (or "clean" if the
    initial insert succeeded).
    """
    if logger is None:
        logger = MagicMock()
    try:
        session.add(incoming)
        session.commit()
        return "clean"
    except Exception:
        session.rollback()
    return apply_backstop_on_collision(
        session=session,
        model_class=_StubTxnRow,
        incoming=incoming,
        normalize_desc=normalize_description_for_backstop,
        logger=logger,
    )


# ── rows_content_equal ───────────────────────────────────────────────


class TestRowsContentEqual(unittest.TestCase):
    """The comparator underneath the backstop. Same tuple → match;
    any field differs → not match. Description is normalized so
    chunk-boundary variance doesn't trip a false-differ."""

    def _row(self, **kw):
        return _StubTxnRow(**kw)

    def test_exact_match(self):
        date = datetime(2023, 1, 26)
        a = self._row(date=date, amount=500, details="UPI PAY")
        b = self._row(date=date, amount=500, details="UPI PAY")
        self.assertTrue(rows_content_equal(a, b, normalize_description_for_backstop))

    def test_desc_normalization_matches(self):
        date = datetime(2023, 1, 26)
        a = self._row(date=date, amount=500, details="  UPI Pay.")
        b = self._row(date=date, amount=500, details="upi   PAY")
        self.assertTrue(rows_content_equal(a, b, normalize_description_for_backstop))

    def test_amount_2dp_tolerant(self):
        date = datetime(2023, 1, 26)
        a = self._row(date=date, amount=500.0, details="X")
        b = self._row(date=date, amount=500.001, details="X")
        self.assertTrue(rows_content_equal(a, b, normalize_description_for_backstop))

    def test_different_amount_not_equal(self):
        date = datetime(2023, 1, 26)
        a = self._row(date=date, amount=500, details="X")
        b = self._row(date=date, amount=750, details="X")
        self.assertFalse(rows_content_equal(a, b, normalize_description_for_backstop))

    def test_different_date_not_equal(self):
        a = self._row(date=datetime(2023, 1, 26), amount=500, details="X")
        b = self._row(date=datetime(2023, 1, 27), amount=500, details="X")
        self.assertFalse(rows_content_equal(a, b, normalize_description_for_backstop))

    def test_different_desc_not_equal(self):
        date = datetime(2023, 1, 26)
        a = self._row(date=date, amount=500, details="MERCHANT_A")
        b = self._row(date=date, amount=500, details="MERCHANT_B")
        self.assertFalse(rows_content_equal(a, b, normalize_description_for_backstop))


# ── find_disambiguated_ref ───────────────────────────────────────────


class TestFindDisambiguatedRef(unittest.TestCase):
    """Suffix generator: first unused `<base>-dupN`. Enforces
    VARCHAR(64) width guard."""

    def _seed(self, refs):
        sess = _StubSession()
        for r in refs:
            sess.committed[r] = _StubTxnRow(referenceID=r)
        return sess

    def test_first_available_is_dup1(self):
        sess = self._seed(["ref-abc"])
        out = find_disambiguated_ref(sess, _StubTxnRow, "ref-abc")
        self.assertEqual(out, "ref-abc-dup1")

    def test_skips_taken_suffixes(self):
        sess = self._seed(["ref-abc", "ref-abc-dup1", "ref-abc-dup2"])
        out = find_disambiguated_ref(sess, _StubTxnRow, "ref-abc")
        self.assertEqual(out, "ref-abc-dup3")

    def test_truncates_to_varchar_64(self):
        base = "a" * 64
        sess = self._seed([base])
        out = find_disambiguated_ref(sess, _StubTxnRow, base)
        self.assertLessEqual(len(out), 64)
        self.assertTrue(out.endswith("-dup1"))
        # Prefix should be a truncation of base.
        self.assertTrue(base.startswith(out[:-5]))

    def test_max_attempts_exhausts_raises(self):
        base = "ref-abc"
        # Seed 5 collisions; ask for max_attempts=5 so all are taken.
        sess = self._seed([base] + [f"{base}-dup{i}" for i in range(1, 6)])
        with self.assertRaises(RuntimeError):
            find_disambiguated_ref(sess, _StubTxnRow, base, max_attempts=5)


# ── apply_backstop_on_collision ──────────────────────────────────────


class TestBackstopSameContentSilentDrop(unittest.TestCase):
    """PK collision + matching (date, amount, normalized-desc) → the
    intended chunk-overlap dedup path. Silent drop."""

    def test_matching_content_drops(self):
        sess = _StubSession()
        date = datetime(2023, 1, 26)

        existing = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="UPI PAY MERCHANT",
        )
        sess.committed[existing.referenceID] = existing

        incoming = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="UPI PAY MERCHANT",
        )
        outcome = _run_insert_flow(sess, incoming)
        self.assertEqual(outcome, "dropped_matching")
        self.assertEqual(len(sess.committed), 1)

    def test_normalized_desc_match_drops(self):
        """Whitespace / case / trailing-punct variance in desc must
        still be seen as MATCHING."""
        sess = _StubSession()
        date = datetime(2023, 1, 26)

        existing = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="  UPI  Pay merchant.",
        )
        sess.committed[existing.referenceID] = existing

        incoming = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="UPI PAY MERCHANT",
        )
        outcome = _run_insert_flow(sess, incoming)
        self.assertEqual(outcome, "dropped_matching")
        self.assertEqual(len(sess.committed), 1)


class TestBackstopDifferingContentSuffixAndKeep(unittest.TestCase):
    """PK collision + DIFFERING content → suffix + retry; both rows
    preserved. This is the reviewer MAJOR."""

    def test_different_amount_suffixes_and_keeps_both(self):
        sess = _StubSession()
        date = datetime(2023, 1, 26)

        existing = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="UPI PAY MERCHANT",
        )
        sess.committed[existing.referenceID] = existing

        incoming = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=750.0,  # differs
            details="UPI PAY MERCHANT",
        )
        outcome = _run_insert_flow(sess, incoming)
        self.assertEqual(outcome, "disambiguated")
        self.assertEqual(len(sess.committed), 2)
        self.assertIn("ref-abc", sess.committed)
        self.assertIn("ref-abc-dup1", sess.committed)
        self.assertEqual(incoming.referenceID, "ref-abc-dup1")

    def test_different_date_suffixes_and_keeps_both(self):
        sess = _StubSession()

        existing = _StubTxnRow(
            referenceID="ref-abc", date=datetime(2023, 1, 26),
            amount=500.0, details="UPI PAY MERCHANT",
        )
        sess.committed[existing.referenceID] = existing

        incoming = _StubTxnRow(
            referenceID="ref-abc", date=datetime(2023, 1, 27),  # differs
            amount=500.0, details="UPI PAY MERCHANT",
        )
        outcome = _run_insert_flow(sess, incoming)
        self.assertEqual(outcome, "disambiguated")
        self.assertEqual(len(sess.committed), 2)

    def test_different_desc_suffixes_and_keeps_both(self):
        sess = _StubSession()
        date = datetime(2023, 1, 26)

        existing = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="UPI PAY MERCHANT_A",
        )
        sess.committed[existing.referenceID] = existing

        incoming = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="UPI PAY MERCHANT_B",  # differs beyond normalization
        )
        outcome = _run_insert_flow(sess, incoming)
        self.assertEqual(outcome, "disambiguated")
        self.assertEqual(len(sess.committed), 2)


class TestBackstopSuffixUniqueness(unittest.TestCase):
    """Multiple distinct collisions on the same base_ref → each gets
    a distinct -dupN suffix."""

    def test_two_distinct_collisions_get_distinct_suffixes(self):
        sess = _StubSession()
        date = datetime(2023, 1, 26)

        existing = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="MERCHANT_A",
        )
        sess.committed[existing.referenceID] = existing

        incoming1 = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=750.0,
            details="MERCHANT_B",
        )
        incoming2 = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=900.0,
            details="MERCHANT_C",
        )
        # Run one by one (simulates the service loop).
        r1 = _run_insert_flow(sess, incoming1)
        r2 = _run_insert_flow(sess, incoming2)
        self.assertEqual(r1, "disambiguated")
        self.assertEqual(r2, "disambiguated")
        self.assertEqual(len(sess.committed), 3)
        self.assertIn("ref-abc-dup1", sess.committed)
        self.assertIn("ref-abc-dup2", sess.committed)

    def test_suffix_stays_within_varchar_64(self):
        sess = _StubSession()
        date = datetime(2023, 1, 26)

        # base_ref exactly 64 chars — the realistic case
        # (SHA-256 hex output is always 64 chars).
        base_ref = "a" * 64
        existing = _StubTxnRow(
            referenceID=base_ref, date=date, amount=500.0,
            details="EXISTING",
        )
        sess.committed[existing.referenceID] = existing

        incoming = _StubTxnRow(
            referenceID=base_ref, date=date, amount=750.0,
            details="INCOMING",
        )
        outcome = _run_insert_flow(sess, incoming)
        self.assertEqual(outcome, "disambiguated")
        for k in sess.committed:
            self.assertLessEqual(len(k), 64)


class TestBackstopMixedBatch(unittest.TestCase):
    """Mixed batch — clean insert, silent-drop collision, and
    suffix-and-keep collision all in the same run."""

    def test_mixed_batch_all_three_paths(self):
        sess = _StubSession()
        date = datetime(2023, 1, 26)

        existing = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="MERCHANT_A",
        )
        sess.committed[existing.referenceID] = existing

        clean = _StubTxnRow(
            referenceID="ref-xyz", date=date, amount=100.0,
            details="MERCHANT_D",
        )
        matching_dup = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=500.0,
            details="MERCHANT_A",
        )
        differing_dup = _StubTxnRow(
            referenceID="ref-abc", date=date, amount=750.0,
            details="MERCHANT_B",
        )

        outcomes = [
            _run_insert_flow(sess, clean),
            _run_insert_flow(sess, matching_dup),
            _run_insert_flow(sess, differing_dup),
        ]
        self.assertEqual(outcomes[0], "clean")
        self.assertEqual(outcomes[1], "dropped_matching")
        self.assertEqual(outcomes[2], "disambiguated")
        self.assertEqual(len(sess.committed), 3)
        self.assertIn("ref-abc", sess.committed)
        self.assertIn("ref-xyz", sess.committed)
        self.assertIn("ref-abc-dup1", sess.committed)


if __name__ == "__main__":
    print("ak-8l5 insert-time zero-loss backstop tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
