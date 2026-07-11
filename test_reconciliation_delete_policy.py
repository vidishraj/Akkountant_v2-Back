"""ak-a0m regression tests — reconciliation delete policy.

Root cause of the F4/F7/F9 silent row deletions (131 rows this batch)
was reconciliationService.replace_email_transactions_with_statement
deleting statement-source rows from OTHER files whose periods
overlapped the newly-arrived statement. Overseer's zero-loss
constraint requires this class of bug never comes back.

These tests pin down the ak-a0m delete policy at the pure-Python
level (no flask / SQLAlchemy boot required):

  should_delete_on_statement_arrival(source, file_id) → bool

Contract:
  - Row is deletable IFF source='email' (case-insensitive) AND
    fileID IS NULL — the classic email-alert path a statement
    replaces.
  - Statement-source rows (any fileID) are NEVER deletable, even if
    the new statement overlaps their period.
  - Unknown / malformed source → NOT deletable (defensive: preserve
    over delete).

The realistic scenario from the reviewer / Lead spec is exercised
end-to-end at the bottom (File A + File B overlap + email alerts).
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.reconciliation_delete_policy import (
    should_delete_on_statement_arrival,
)


class TestEmailAlertDeletable(unittest.TestCase):
    """The email-alert path is the ONLY row shape a statement
    supersedes. Both source='email' AND fileID IS NULL must be
    true."""

    def test_email_lowercase_null_fileid_deletable(self):
        self.assertTrue(
            should_delete_on_statement_arrival("email", None)
        )

    def test_email_mixed_case_normalized(self):
        for form in ("Email", "EMAIL", "eMaIl"):
            with self.subTest(form=form):
                self.assertTrue(
                    should_delete_on_statement_arrival(form, None)
                )

    def test_email_whitespace_normalized(self):
        """Trim surrounding whitespace before comparing — extractor
        might round-trip an email-source label with padding."""
        self.assertTrue(
            should_delete_on_statement_arrival(" email ", None)
        )


class TestStatementRowsProtected(unittest.TestCase):
    """The MAJOR ak-a0m guarantee: statement-source rows are NEVER
    deleted cross-file, regardless of fileID or overlap."""

    def test_statement_with_file_a_not_deletable(self):
        self.assertFalse(
            should_delete_on_statement_arrival("statement", "file_A")
        )

    def test_statement_with_null_file_not_deletable(self):
        """Some legacy statement rows lost their fileID during the
        pre-ak-8l5 dedup regressions. Even then, source='statement'
        alone is enough protection under ak-a0m."""
        self.assertFalse(
            should_delete_on_statement_arrival("statement", None)
        )

    def test_statement_mixed_case_still_protected(self):
        for form in ("Statement", "STATEMENT", "sTaTeMeNt"):
            with self.subTest(form=form):
                self.assertFalse(
                    should_delete_on_statement_arrival(form, "any_file")
                )


class TestEdgeCases(unittest.TestCase):
    """Defensive: preserve rows we can't confidently classify."""

    def test_null_source_not_deletable(self):
        self.assertFalse(
            should_delete_on_statement_arrival(None, None)
        )

    def test_empty_source_not_deletable(self):
        self.assertFalse(
            should_delete_on_statement_arrival("", None)
        )

    def test_unknown_source_not_deletable(self):
        """Future ingest sources (e.g. 'webhook', 'manual') must not
        be silently dropped."""
        for form in ("webhook", "manual", "unknown", "api"):
            with self.subTest(form=form):
                self.assertFalse(
                    should_delete_on_statement_arrival(form, None)
                )

    def test_email_with_non_null_fileid_not_deletable(self):
        """email rows with a non-null fileID are unusual (email path
        doesn't attach a fileID today). ak-a0m preserves them —
        zero-loss preference is always keep-over-drop."""
        self.assertFalse(
            should_delete_on_statement_arrival("email", "some_file")
        )


class TestScenarioFromLeadSpec(unittest.TestCase):
    """The exact scenario from akkountant_lead's dispatch:

      Ingest File A (statement) with tx Feb 1-15
      Ingest File B (statement) with tx Feb 10-20 (overlap Feb 10-15)
      After: A's tx AND B's tx all preserved.
      Only email-source alerts in Feb 1-20 replaced by whichever
      statement covers them.
    """

    def _rows(self):
        return [
            # Two statements, overlapping period:
            {"source": "statement", "fileID": "A", "date": "2026-02-01"},
            {"source": "statement", "fileID": "A", "date": "2026-02-15"},
            {"source": "statement", "fileID": "B", "date": "2026-02-10"},
            {"source": "statement", "fileID": "B", "date": "2026-02-20"},
            # Email alerts across the union period:
            {"source": "email", "fileID": None, "date": "2026-02-02"},
            {"source": "email", "fileID": None, "date": "2026-02-18"},
            # An email row that (bug-of-the-day) somehow got a fileID
            # — must survive under ak-a0m's defensive policy:
            {"source": "email", "fileID": "orphan_A", "date": "2026-02-05"},
        ]

    def test_only_email_alerts_deletable(self):
        rows = self._rows()
        deletable = [
            r for r in rows
            if should_delete_on_statement_arrival(r["source"], r["fileID"])
        ]
        # Exactly the 2 email-null-fileID rows should be marked:
        self.assertEqual(len(deletable), 2)
        for r in deletable:
            self.assertEqual(r["source"], "email")
            self.assertIsNone(r["fileID"])

    def test_all_statement_rows_preserved(self):
        rows = self._rows()
        statement_rows = [r for r in rows if r["source"] == "statement"]
        for r in statement_rows:
            with self.subTest(row=r):
                self.assertFalse(
                    should_delete_on_statement_arrival(
                        r["source"], r["fileID"],
                    ),
                    f"regression: statement row {r} marked deletable "
                    f"— zero-loss violation returned",
                )

    def test_email_with_fileid_preserved(self):
        rows = self._rows()
        anomaly = [
            r for r in rows
            if r["source"] == "email" and r["fileID"] is not None
        ]
        self.assertEqual(len(anomaly), 1)
        self.assertFalse(
            should_delete_on_statement_arrival(
                anomaly[0]["source"], anomaly[0]["fileID"],
            )
        )


class TestBuildEmailDeletionFilters(unittest.TestCase):
    """The SQL-filter builder assembles the exact same policy as the
    pure predicate. We can't fully execute it without SQLAlchemy /
    a live DB, but we can verify the shape (5 clauses in the expected
    order) so a future rewrite doesn't accidentally drop one."""

    def test_returns_five_filter_clauses(self):
        from utils.reconciliation_delete_policy import (
            build_email_deletion_filters,
        )

        # Minimal stand-in for the Transactions ORM class — every
        # attribute exposed here is used by the builder.
        class _StubTxn:
            class _Col:
                def __init__(self, name):
                    self.name = name

                def __eq__(self, other):
                    return ("=", self.name, other)

                def is_(self, other):
                    return ("IS", self.name, other)

                def between(self, a, b):
                    return ("BETWEEN", self.name, a, b)

            user = _Col("user")
            bank = _Col("bank")
            source = _Col("source")
            fileID = _Col("fileID")
            date = _Col("date")

        class _StubFunc:
            @staticmethod
            def lower(col):
                return col  # let __eq__ handle the comparison

        filters = build_email_deletion_filters(
            _StubTxn, "u1", "HDFC_DEBIT",
            "2026-02-01", "2026-02-28", func=_StubFunc,
        )
        # 5 clauses: user, bank, LOWER(source)='email', fileID IS NULL,
        # date BETWEEN [start, end].
        self.assertEqual(len(filters), 5)
        # Verify the fileID-is-null clause is present (the ak-a0m
        # protection against email-alert-with-fileID rows getting
        # nuked):
        self.assertTrue(any(f == ("IS", "fileID", None) for f in filters))
        # Verify the LOWER(source)='email' clause is present:
        self.assertTrue(any(f == ("=", "source", "email") for f in filters))


if __name__ == "__main__":
    print("ak-a0m reconciliation delete policy regression tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
