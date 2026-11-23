"""ak-qpkm Phase 2b: tests for the statement-import drift alarm.

Exercises ``audit_statement_file`` against a real in-memory SQLite
+ synthetic Transactions fixture. Covers the five drift-flag
branches + severity escalation + the ak-p3id positive-magnitude
sum convention.
"""
from __future__ import annotations

import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    import models  # noqa: F401 — register declarative
    from models.investmentHistory import InvestmentHistory  # noqa: F401
    from models.transactions import Transactions, ProcessingMethod
    from services.statement_reconciliation_alarm import (
        FLAG_CLEAN,
        FLAG_NEW_IMPORT,
        FLAG_REGRESSED,
        FLAG_ROW_COUNT_DRIFT,
        FLAG_SUM_DRIFT,
        audit_statement_file,
    )
    _DEPS_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _DEPS_OK = False
    _SKIP_REASON = f"import failed: {_exc}"


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestStatementReconciliationAlarm(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Transactions.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.session = self._SessionFactory()

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add(self, amount, *, file_id="f1", user="u1", **kwargs):
        """Insert a tx using the HDFC-style convention (credits
        stored negative, debits stored positive)."""
        from datetime import date as _date
        kwargs.setdefault("date", _date(2026, 11, 23))
        kwargs.setdefault("referenceID", f"ref-{amount}-{kwargs['date']}")
        kwargs.setdefault("details", "test tx")
        kwargs.setdefault("bank", "HDFC_DEBIT")
        kwargs.setdefault("source", "Statement")
        kwargs.setdefault(
            "processed_via", ProcessingMethod.CLAUDE_CODE,
        )
        row = Transactions(
            amount=amount, user=user, fileID=file_id, **kwargs,
        )
        self.session.add(row)
        self.session.commit()
        return row

    # ─── NEW_IMPORT branch ────────────────────────────────────

    def test_no_expectations_returns_new_import(self):
        """No expected_* supplied + no prior rows → NEW_IMPORT."""
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
        )
        self.assertIn(FLAG_NEW_IMPORT, audit["flags"])
        self.assertEqual(audit["severity"], "clean")
        self.assertEqual(audit["actual_row_count"], 0)

    def test_new_import_with_actual_rows(self):
        """Actual counts/sums computed even without expectations."""
        self._add(-1000, referenceID="r1")  # credit (In 1000)
        self._add(500, referenceID="r2")    # debit (Out 500)
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
        )
        self.assertEqual(audit["actual_row_count"], 2)
        self.assertEqual(audit["actual_in_sum"], 1000.0)
        self.assertEqual(audit["actual_out_sum"], 500.0)
        self.assertIn(FLAG_NEW_IMPORT, audit["flags"])

    # ─── CLEAN branch ─────────────────────────────────────────

    def test_clean_when_within_tolerance(self):
        """Actual ≈ expected within row-count + sum tolerance →
        CLEAN. Pre-ak-p3id convention (credit stored negative,
        debit stored positive) honored."""
        for i in range(10):
            self._add(-100, referenceID=f"in-{i}")  # In 1000
        for i in range(10):
            self._add(50, referenceID=f"out-{i}")  # Out 500
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_row_count=20,
            expected_in_sum=1000.0,
            expected_out_sum=500.0,
        )
        self.assertEqual(audit["flags"], [FLAG_CLEAN])
        self.assertEqual(audit["severity"], "clean")
        self.assertEqual(audit["row_count_delta"], 0)

    def test_small_drift_within_row_floor_is_clean(self):
        """Delta within the 5-row floor is CLEAN."""
        for i in range(10):
            self._add(-100, referenceID=f"in-{i}")
        # Actual=10, Expected=12 → delta=-2, floor=5 → CLEAN
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_row_count=12,
        )
        self.assertNotIn(FLAG_ROW_COUNT_DRIFT, audit["flags"])
        self.assertEqual(audit["severity"], "clean")

    # ─── ROW_COUNT_DRIFT ──────────────────────────────────────

    def test_row_count_drift_beyond_floor(self):
        """Delta > 5 rows → ROW_COUNT_DRIFT."""
        for i in range(10):
            self._add(-100, referenceID=f"in-{i}")
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_row_count=20,  # delta=-10, floor=5, 5%→1 → 10>5
        )
        self.assertIn(FLAG_ROW_COUNT_DRIFT, audit["flags"])
        self.assertEqual(audit["row_count_delta"], -10)
        self.assertEqual(audit["severity"], "info")

    def test_row_count_drift_pct_over_floor(self):
        """For large-expected files, 5% of expected exceeds the 5
        floor → 5% applies."""
        for i in range(90):
            self._add(-100, referenceID=f"in-{i}")
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_row_count=200,
            # delta=-110, tolerance=max(5, 200*0.05)=10 → 110>10
        )
        self.assertIn(FLAG_ROW_COUNT_DRIFT, audit["flags"])

    # ─── SUM_DRIFT ────────────────────────────────────────────

    def test_sum_drift_in_sum(self):
        """|actual_in_sum - expected_in_sum| > ₹1.00 → SUM_DRIFT."""
        self._add(-10000, referenceID="in-1")  # In 10000
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_in_sum=15000.0,  # delta -5000 >> 1.00
        )
        self.assertIn(FLAG_SUM_DRIFT, audit["flags"])
        self.assertEqual(audit["in_sum_delta"], -5000.0)
        self.assertEqual(audit["severity"], "info")

    def test_sum_drift_out_sum(self):
        """Debit-side drift also flags SUM_DRIFT."""
        self._add(5000, referenceID="out-1")  # Out 5000
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_out_sum=7500.0,
        )
        self.assertIn(FLAG_SUM_DRIFT, audit["flags"])

    def test_sum_within_tolerance_is_clean(self):
        """₹0.50 drift (within ₹1.00 tolerance) → CLEAN on sum."""
        self._add(-10000, referenceID="in-1")
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_in_sum=10000.50,  # delta 0.50 < 1.00
        )
        self.assertNotIn(FLAG_SUM_DRIFT, audit["flags"])

    # ─── REGRESSED (critical) ─────────────────────────────────

    def test_regressed_empty_import_when_expected_nontrivial(self):
        """0 rows imported when 100 expected → REGRESSED critical."""
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_row_count=100,
        )
        self.assertIn(FLAG_REGRESSED, audit["flags"])
        self.assertIn(FLAG_ROW_COUNT_DRIFT, audit["flags"])
        self.assertEqual(audit["severity"], "critical")

    def test_regressed_near_empty_import(self):
        """20% of expected or less, with expected >= 5 → REGRESSED."""
        self._add(-1, referenceID="in-1")
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_row_count=50,  # 1 is < 50*0.2=10 → regressed
        )
        self.assertIn(FLAG_REGRESSED, audit["flags"])

    def test_not_regressed_when_expected_trivial(self):
        """Expected < 5 → REGRESSED threshold not triggered."""
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
            expected_row_count=3,  # below REGRESSED floor
        )
        self.assertNotIn(FLAG_REGRESSED, audit["flags"])

    # ─── User + file scoping ───────────────────────────────────

    def test_audit_is_user_scoped(self):
        """u1's audit must not see u2's rows."""
        self._add(-1000, user="u1", referenceID="u1-in")
        self._add(-9999, user="u2", referenceID="u2-in")
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
        )
        self.assertEqual(audit["actual_in_sum"], 1000.0)

    def test_audit_is_file_scoped(self):
        """file_id scoping: rows from file_B don't contribute to
        file_A audit."""
        self._add(-1000, file_id="file_A", referenceID="fileA-in")
        self._add(-5000, file_id="file_B", referenceID="fileB-in")
        audit = audit_statement_file(
            session=self.session,
            file_id="file_A", bank="HDFC_DEBIT", user_id="u1",
        )
        self.assertEqual(audit["actual_in_sum"], 1000.0)

    # ─── Required-arg validation ───────────────────────────────

    def test_raises_on_missing_file_id(self):
        with self.assertRaises(ValueError):
            audit_statement_file(
                session=self.session,
                file_id="", bank="HDFC_DEBIT", user_id="u1",
            )

    def test_raises_on_missing_user_id(self):
        with self.assertRaises(ValueError):
            audit_statement_file(
                session=self.session,
                file_id="f1", bank="HDFC_DEBIT", user_id="",
            )

    # ─── §23 #4 PII guard ─────────────────────────────────────

    def test_result_dict_excludes_tx_descriptions(self):
        """Audit result must NEVER carry per-tx descriptions —
        operator triage grep is safe for journal exposure."""
        self._add(-5000, referenceID="in-1", details="PRIVATE TX NARRATION")
        audit = audit_statement_file(
            session=self.session,
            file_id="f1", bank="HDFC_DEBIT", user_id="u1",
        )
        audit_str = str(audit)
        self.assertNotIn("PRIVATE TX NARRATION", audit_str)
        self.assertNotIn("details", audit)
        self.assertNotIn("description", audit)


if __name__ == "__main__":
    unittest.main()
