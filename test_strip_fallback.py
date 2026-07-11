"""ak-ex2 tests for the non-savings row stripper (pure helpers).

The service-layer method + admin endpoint depend on flask/SQLAlchemy
and are covered by integration testing at lead-verify. These tests
exercise the pure-Python decision helpers in utils/statement_sections
that drive the strip decision:

  find_row_line_in_raw_text  → where in the raw text is this tx?
  build_line_to_section_map  → which section is each line in?
  classify_row_section       → combining the two: SAVINGS / non-SAVINGS?
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ak-ex2-v2 test note: importing `models.strippedTransactionsAudit`
# triggers SQLAlchemy mapper configuration which walks every mapped
# class including User.investment_history — that relationship points
# at `InvestmentHistory` which isn't imported by models/__init__.py.
# Pre-import it explicitly so mapper resolution succeeds when we
# hit the audit model in the tests below.
from models import investmentHistory as _iv  # noqa: F401

from utils.statement_sections import (
    SectionType,
    build_line_to_section_map,
    classify_row_section,
    detect_hdfc_sections,
    find_row_line_in_raw_text,
)


# ── find_row_line_in_raw_text ────────────────────────────────────────


class TestFindRowLine(unittest.TestCase):
    """Row → raw-text-line matching heuristic."""

    def test_finds_line_by_amount_and_desc(self):
        raw = [
            "01/04/2026  Salary Credit HDFC0001234  50,000.00",
            "02/04/2026  UPI-Merchant-A  1,234.56",
            "03/04/2026  Rent Payment  25,000.00",
        ]
        idx = find_row_line_in_raw_text(
            amount=1234.56, description="UPI Merchant A", raw_lines=raw,
        )
        self.assertEqual(idx, 1)

    def test_matches_comma_less_amount(self):
        raw = [
            "01/04/2026  Salary Credit HDFC0001234  50000.00",
        ]
        idx = find_row_line_in_raw_text(
            amount=50000, description="Salary Credit HDFC", raw_lines=raw,
        )
        self.assertEqual(idx, 0)

    def test_matches_whole_rupee_no_decimal(self):
        raw = [
            "01/04/2026  Rent Payment via UPI  25000",
        ]
        idx = find_row_line_in_raw_text(
            amount=25000.00, description="Rent Payment UPI", raw_lines=raw,
        )
        self.assertEqual(idx, 0)

    def test_indian_comma_grouping(self):
        raw = [
            "05/04/2026  Rent Received  1,00,000.00",
        ]
        idx = find_row_line_in_raw_text(
            amount=100000, description="Rent Received", raw_lines=raw,
        )
        self.assertEqual(idx, 0)

    def test_ambiguous_tie_returns_none(self):
        """Two lines with the same amount + same overlap → ambiguous
        → return None so the caller leaves the row alone."""
        raw = [
            "01/04/2026  UPI Merchant  100.00",
            "02/04/2026  UPI Merchant  100.00",
        ]
        idx = find_row_line_in_raw_text(
            amount=100.00, description="UPI Merchant", raw_lines=raw,
        )
        self.assertIsNone(idx)

    def test_no_match_returns_none(self):
        raw = [
            "01/04/2026  Rent Payment  25,000.00",
        ]
        idx = find_row_line_in_raw_text(
            amount=999.99, description="Something Else", raw_lines=raw,
        )
        self.assertIsNone(idx)

    def test_no_desc_returns_none(self):
        """Empty description → nothing to score against → None
        (conservative — never delete based on amount alone)."""
        raw = ["01/04  something  100.00"]
        idx = find_row_line_in_raw_text(
            amount=100.00, description="", raw_lines=raw,
        )
        self.assertIsNone(idx)

    def test_short_desc_tokens_ignored(self):
        """Description with only 1-2 char tokens → no useful overlap
        → None."""
        raw = ["01/04  a b c  100.00"]
        idx = find_row_line_in_raw_text(
            amount=100.00, description="a b c", raw_lines=raw,
        )
        self.assertIsNone(idx)

    def test_scoring_prefers_higher_overlap(self):
        """Two amount hits, one has richer desc overlap → picks that."""
        raw = [
            "01/04  UPI Random Thing  500.00",
            "02/04  UPI Merchant A Card Payment  500.00",
        ]
        idx = find_row_line_in_raw_text(
            amount=500.00,
            description="UPI Merchant A Card Payment",
            raw_lines=raw,
        )
        self.assertEqual(idx, 1)

    def test_amount_absent_no_match(self):
        raw = ["01/04  Salary Credit  no amount here"]
        idx = find_row_line_in_raw_text(
            amount=50000.00, description="Salary Credit", raw_lines=raw,
        )
        self.assertIsNone(idx)


# ── build_line_to_section_map ────────────────────────────────────────


class TestBuildLineToSectionMap(unittest.TestCase):
    """Given spans + total line count, build a per-line section map."""

    def test_maps_covered_lines(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",  # 0
            "01/04  Rent  1000",                            # 1
            "Statement of Account for : CREDIT CARD",       # 2
            "02/04  CC payment  500",                       # 3
        ])
        spans = detect_hdfc_sections(text)
        m = build_line_to_section_map(spans, num_lines=4)
        self.assertEqual(m[0], SectionType.SAVINGS)
        self.assertEqual(m[1], SectionType.SAVINGS)
        self.assertEqual(m[2], SectionType.CREDIT_CARD)
        self.assertEqual(m[3], SectionType.CREDIT_CARD)

    def test_uncovered_lines_are_unknown(self):
        text = "Statement of Account for : SAVINGS\nrow"
        spans = detect_hdfc_sections(text)
        # Ask for more lines than the text has — extras become UNKNOWN
        # (defensive; caller shouldn't hit this but shouldn't crash).
        m = build_line_to_section_map(spans, num_lines=5)
        self.assertEqual(m[0], SectionType.SAVINGS)
        self.assertEqual(m[4], SectionType.UNKNOWN)

    def test_leading_unknown_span_populated(self):
        text = "\n".join([
            "Vidishraj Something",                          # 0 UNKNOWN
            "Account holder details",                        # 1 UNKNOWN
            "Statement of Account for : SAVINGS ACCOUNT",   # 2 SAVINGS
            "row A",                                         # 3 SAVINGS
        ])
        spans = detect_hdfc_sections(text)
        m = build_line_to_section_map(spans, num_lines=4)
        self.assertEqual(m[0], SectionType.UNKNOWN)
        self.assertEqual(m[1], SectionType.UNKNOWN)
        self.assertEqual(m[2], SectionType.SAVINGS)
        self.assertEqual(m[3], SectionType.SAVINGS)


# ── classify_row_section (end-to-end) ────────────────────────────────


class TestClassifyRowSection(unittest.TestCase):
    """The combined decision: find the row's line + look up its
    section. UNKNOWN = "can't classify → leave alone"."""

    def _setup(self, text):
        raw_lines = text.split("\n")
        spans = detect_hdfc_sections(text)
        section_by_line = build_line_to_section_map(spans, len(raw_lines))
        return raw_lines, section_by_line

    def test_savings_row_classified_as_savings(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "01/04/2026  Rent Payment via UPI  25,000.00",
        ])
        raw, m = self._setup(text)
        section = classify_row_section(
            amount=25000, description="Rent Payment UPI",
            raw_lines=raw, section_by_line=m,
        )
        self.assertEqual(section, SectionType.SAVINGS)

    def test_credit_card_row_classified_as_credit_card(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "01/04/2026  Rent Payment via UPI  25,000.00",
            "Statement of Account for : CREDIT CARD",
            "02/04/2026  Amazon Purchase Cardmember  1,500.00",
        ])
        raw, m = self._setup(text)
        section = classify_row_section(
            amount=1500, description="Amazon Purchase Cardmember",
            raw_lines=raw, section_by_line=m,
        )
        self.assertEqual(section, SectionType.CREDIT_CARD)

    def test_ambiguous_row_returns_unknown(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "01/04  UPI Merchant  100.00",
            "Statement of Account for : CREDIT CARD",
            "02/04  UPI Merchant  100.00",  # same amount + same desc
        ])
        raw, m = self._setup(text)
        section = classify_row_section(
            amount=100, description="UPI Merchant",
            raw_lines=raw, section_by_line=m,
        )
        self.assertEqual(section, SectionType.UNKNOWN)

    def test_row_not_in_raw_returns_unknown(self):
        """Tx exists in DB but its narration line is missing from
        raw text (extractor pathology). Classification = UNKNOWN;
        stripper leaves the row alone."""
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "01/04  Rent  25000",
        ])
        raw, m = self._setup(text)
        section = classify_row_section(
            amount=99999.99, description="Missing Row",
            raw_lines=raw, section_by_line=m,
        )
        self.assertEqual(section, SectionType.UNKNOWN)


# ── Combined end-to-end scenario ─────────────────────────────────────


class TestStripDecisionScenarios(unittest.TestCase):
    """Simulate the strip-decision loop end-to-end on realistic
    multi-section text. For each row, decide keep/delete."""

    def _decide(self, rows, text):
        raw = text.split("\n")
        spans = detect_hdfc_sections(text)
        section_by_line = build_line_to_section_map(spans, len(raw))
        keep = []
        delete = []
        skip = []  # UNKNOWN → leave alone
        for row in rows:
            section = classify_row_section(
                row["amount"], row["description"], raw, section_by_line,
            )
            if section == SectionType.SAVINGS:
                keep.append(row)
            elif section == SectionType.UNKNOWN:
                skip.append(row)
            else:
                delete.append(row)
        return keep, delete, skip

    def test_realistic_combined_statement(self):
        text = "\n".join([
            "Vidishraj",                                     # 0 UNKNOWN
            "Account Details",                                # 1 UNKNOWN
            "Statement of Account for : SAVINGS ACCOUNT",     # 2 SAVINGS
            "Opening Balance 12,345.67",                      # 3 SAVINGS
            "01/04/2026 Salary Credit HDFC0001234 50,000.00", # 4 SAVINGS
            "02/04/2026 UPI-Groceries-Merchant 1,234.56",     # 5 SAVINGS
            "Statement of Account for : CREDIT CARD",         # 6 CC
            "03/04/2026 Amazon Purchase Cardmember 2,500.00", # 7 CC
            "04/04/2026 Restaurant Cardmember 800.00",        # 8 CC
            "Statement of Account for : FIXED DEPOSIT",       # 9 FD
            "05/04/2026 FD Interest Post 750.00",            # 10 FD
        ])
        rows = [
            {"amount": 50000.00, "description": "Salary Credit HDFC0001234"},
            {"amount": 1234.56, "description": "UPI Groceries Merchant"},
            {"amount": 2500.00, "description": "Amazon Purchase Cardmember"},
            {"amount": 800.00, "description": "Restaurant Cardmember"},
            {"amount": 750.00, "description": "FD Interest Post"},
        ]
        keep, delete, skip = self._decide(rows, text)
        self.assertEqual(len(keep), 2, f"expected 2 savings kept, got {keep}")
        self.assertEqual(len(delete), 3, f"expected 3 non-savings deleted, got {delete}")
        self.assertEqual(len(skip), 0)

    def test_only_savings_rows_no_deletes(self):
        text = "\n".join([
            "Statement of Account for : SAVINGS ACCOUNT",
            "01/04 Rent Payment 25,000.00",
            "02/04 Salary Credit 50,000.00",
        ])
        rows = [
            {"amount": 25000.00, "description": "Rent Payment"},
            {"amount": 50000.00, "description": "Salary Credit"},
        ]
        keep, delete, skip = self._decide(rows, text)
        self.assertEqual(len(keep), 2)
        self.assertEqual(len(delete), 0)

    def test_never_deletes_unlocatable_row(self):
        """A row that can't be pinned to a specific line must NOT be
        deleted — conservatism protects legit rows from bad heuristic
        matches."""
        text = "\n".join([
            "Statement of Account for : CREDIT CARD",
            "01/04 CC Row 100.00",
        ])
        rows = [
            # Amount + desc don't match anything in the raw text.
            {"amount": 99999.00, "description": "Phantom Row"},
        ]
        keep, delete, skip = self._decide(rows, text)
        self.assertEqual(len(delete), 0, "must never delete an unlocatable row")
        self.assertEqual(len(skip), 1)


# ── ak-ex2-v2: snapshot-before-delete audit-flow tests ──────────────
#
# These tests exercise the ORDERING contract of the ak-ex2-v2 flow
# without booting flask/SQLAlchemy: the audit rows MUST land before
# the DELETE fires, and BOTH must roll back together on any error.
# We use a lightweight stub session that records every call in order.


class _StubTxn:
    """Stand-in for a Transactions ORM object."""

    def __init__(self, **kw):
        self.referenceID = kw.get("referenceID", "")
        self.date = kw.get("date")
        self.details = kw.get("details", "")
        self.amount = kw.get("amount")
        self.tag = kw.get("tag")
        self.fileID = kw.get("fileID")
        self.source = kw.get("source", "Statement")
        self.bank = kw.get("bank", "HDFC_DEBIT")
        self.user = kw.get("user", "u_test")
        self.processed_via = kw.get("processed_via")
        self.gmail_message_id = kw.get("gmail_message_id")
        self.transfer_group_id = kw.get("transfer_group_id")
        self.bank_reference_id = kw.get("bank_reference_id")


class _RecordingSession:
    """Records add_all / flush / commit / rollback / delete-query
    calls in a single ordered list, so tests can assert the flow
    happened in the required sequence."""

    def __init__(self):
        self.events = []  # list[str]
        self.audit_rows_added = []
        self.deleted_refs = []

    def add_all(self, rows):
        self.events.append(f"add_all({len(rows)})")
        self.audit_rows_added.extend(rows)

    def flush(self):
        self.events.append("flush")

    def commit(self):
        self.events.append("commit")

    def rollback(self):
        self.events.append("rollback")


class TestAuditModelConstructor(unittest.TestCase):
    """The audit model's from_transaction() copies the transactions
    columns verbatim and stamps the audit-scope fields."""

    def test_from_transaction_copies_all_columns(self):
        from models.strippedTransactionsAudit import StrippedTransactionsAudit
        from datetime import datetime as _dt

        txn = _StubTxn(
            referenceID="ref-abc", date=_dt(2026, 4, 1),
            details="UPI-Merchant-A", amount=1234.56, tag="",
            fileID="file_HDFC_2026_04", source="Statement",
            bank="HDFC_DEBIT", user="u_1",
            processed_via=None, gmail_message_id="gm1",
            transfer_group_id=None, bank_reference_id="9876543210",
        )
        row = StrippedTransactionsAudit.from_transaction(
            txn, reason="ak-ex2 non-savings",
            file_id="file_HDFC_2026_04",
        )
        # Mirrored columns:
        self.assertEqual(row.referenceID, "ref-abc")
        self.assertEqual(row.details, "UPI-Merchant-A")
        self.assertEqual(row.amount, 1234.56)
        self.assertEqual(row.fileID, "file_HDFC_2026_04")
        self.assertEqual(row.source, "Statement")
        self.assertEqual(row.bank, "HDFC_DEBIT")
        self.assertEqual(row.user, "u_1")
        self.assertEqual(row.gmail_message_id, "gm1")
        self.assertEqual(row.bank_reference_id, "9876543210")
        # Audit-scope columns:
        self.assertEqual(row.stripped_reason, "ak-ex2 non-savings")
        self.assertEqual(row.stripped_file_id, "file_HDFC_2026_04")

    def test_recovery_round_trip_shape(self):
        """The audit row's mirrored columns must be identical to the
        source transaction — this is the contract that lets the
        manual restore
          INSERT INTO transactions SELECT <mirrored cols> FROM
            stripped_transactions_audit WHERE stripped_file_id=?
        produce a byte-identical row."""
        from models.strippedTransactionsAudit import StrippedTransactionsAudit
        from datetime import datetime as _dt

        txn = _StubTxn(
            referenceID="ref-xyz", date=_dt(2026, 5, 1),
            details="Salary Credit", amount=50000.00, tag="salary",
            fileID="f_may", source="Statement", bank="HDFC_DEBIT",
            user="u_2", processed_via=None,
            gmail_message_id="gm2", transfer_group_id="tg_a",
            bank_reference_id="REF-X",
        )
        row = StrippedTransactionsAudit.from_transaction(
            txn, reason="ak-ex2 non-savings", file_id="f_may",
        )
        mirrored = (
            "referenceID", "date", "details", "amount", "tag",
            "fileID", "source", "bank", "user", "processed_via",
            "gmail_message_id", "transfer_group_id",
            "bank_reference_id",
        )
        for col in mirrored:
            self.assertEqual(
                getattr(row, col), getattr(txn, col),
                f"column {col} not mirrored verbatim",
            )


class TestSnapshotBeforeDeleteOrdering(unittest.TestCase):
    """The strip flow must snapshot rows into the audit table BEFORE
    the DELETE runs. This test verifies the event order using the
    stub recorder — independent of the real DB backend."""

    def _simulate_strip_flow(self, session, rows_to_delete, delete_fn):
        """Mirror the shape of strip_non_savings_from_fallback_file's
        snapshot-then-delete block. `delete_fn` is called to perform
        the DELETE (records events on the session)."""
        from models.strippedTransactionsAudit import StrippedTransactionsAudit
        audit_rows = [
            StrippedTransactionsAudit.from_transaction(
                r, reason="ak-ex2 non-savings", file_id=r.fileID,
            )
            for r in rows_to_delete
        ]
        try:
            session.add_all(audit_rows)
            session.flush()
        except Exception:
            session.rollback()
            raise
        try:
            delete_fn()
            session.commit()
        except Exception:
            session.rollback()
            raise
        return len(audit_rows)

    def test_snapshot_flushes_before_delete_runs(self):
        session = _RecordingSession()
        rows = [
            _StubTxn(referenceID="r1", fileID="f", amount=100, details="A"),
            _StubTxn(referenceID="r2", fileID="f", amount=200, details="B"),
        ]

        def do_delete():
            session.events.append("delete(r1,r2)")
            session.deleted_refs = ["r1", "r2"]

        count = self._simulate_strip_flow(session, rows, do_delete)
        self.assertEqual(count, 2)
        # Required order:
        #   1. add_all(audit_rows)
        #   2. flush         (snapshot visible in this tx)
        #   3. delete(...)   (transactions removed)
        #   4. commit        (both persisted together)
        self.assertEqual(
            session.events,
            ["add_all(2)", "flush", "delete(r1,r2)", "commit"],
        )
        # Audit rows are of the correct type + count:
        from models.strippedTransactionsAudit import StrippedTransactionsAudit
        self.assertEqual(len(session.audit_rows_added), 2)
        for r in session.audit_rows_added:
            self.assertIsInstance(r, StrippedTransactionsAudit)

    def test_delete_failure_rolls_back_snapshot_too(self):
        """If the DELETE fails after a successful snapshot, rollback
        must undo BOTH so the audit table doesn't accumulate stale
        rows for tx that survived."""
        session = _RecordingSession()
        rows = [
            _StubTxn(referenceID="r1", fileID="f", amount=100, details="A"),
        ]

        def do_delete_bomb():
            session.events.append("delete_attempt")
            raise RuntimeError("simulated DELETE failure")

        with self.assertRaises(RuntimeError):
            self._simulate_strip_flow(session, rows, do_delete_bomb)
        # Required order: add_all → flush → delete_attempt → rollback
        self.assertEqual(
            session.events,
            ["add_all(1)", "flush", "delete_attempt", "rollback"],
        )
        self.assertNotIn("commit", session.events)

    def test_snapshot_failure_aborts_before_delete(self):
        """If the SNAPSHOT insert itself fails, the DELETE must not
        run — better to leave the over-parse in place (recoverable
        via re-strip once the audit table works) than to hard-delete
        without a recovery trail."""

        class _SnapshotFailingSession(_RecordingSession):
            def add_all(self, rows):
                self.events.append("add_all_attempt")
                raise RuntimeError("simulated snapshot failure")

        session = _SnapshotFailingSession()
        rows = [_StubTxn(referenceID="r1", fileID="f", amount=100, details="A")]

        def do_delete():  # pragma: no cover — must not run
            session.events.append("delete")

        with self.assertRaises(RuntimeError):
            self._simulate_strip_flow(session, rows, do_delete)
        # Required order: add_all_attempt → rollback (no flush, no delete)
        self.assertEqual(
            session.events,
            ["add_all_attempt", "rollback"],
        )
        self.assertNotIn("delete", session.events)
        self.assertNotIn("commit", session.events)


class TestRecoveryRoundTrip(unittest.TestCase):
    """Simulate the manual restore contract:
       stripped rows → snapshot → DELETE → later, INSERT-SELECT back
       into transactions produces byte-identical rows.

    Uses the stub audit model + from_transaction() copy, then
    reconstructs Transactions-like dicts from the snapshot and
    verifies field-for-field equality with the originals.
    """

    def test_round_trip_produces_identical_columns(self):
        from models.strippedTransactionsAudit import StrippedTransactionsAudit
        from datetime import datetime as _dt

        originals = [
            _StubTxn(
                referenceID="r1", date=_dt(2026, 4, 1),
                details="Amazon Purchase Cardmember", amount=2500.0,
                tag="cc", fileID="f_apr", source="Statement",
                bank="HDFC_DEBIT", user="u_1", processed_via=None,
                gmail_message_id="gm1", transfer_group_id=None,
                bank_reference_id=None,
            ),
            _StubTxn(
                referenceID="r2", date=_dt(2026, 4, 5),
                details="FD Interest Post", amount=750.0,
                tag="fd", fileID="f_apr", source="Statement",
                bank="HDFC_DEBIT", user="u_1", processed_via=None,
                gmail_message_id="gm1", transfer_group_id=None,
                bank_reference_id="FD-750",
            ),
        ]
        # Snapshot before delete:
        snapshots = [
            StrippedTransactionsAudit.from_transaction(
                r, reason="ak-ex2 non-savings", file_id="f_apr",
            )
            for r in originals
        ]
        # Simulate the manual restore: build a "restored" row from
        # the snapshot's mirrored columns.
        restored = []
        for snap in snapshots:
            restored.append(_StubTxn(
                referenceID=snap.referenceID, date=snap.date,
                details=snap.details, amount=snap.amount, tag=snap.tag,
                fileID=snap.fileID, source=snap.source, bank=snap.bank,
                user=snap.user, processed_via=snap.processed_via,
                gmail_message_id=snap.gmail_message_id,
                transfer_group_id=snap.transfer_group_id,
                bank_reference_id=snap.bank_reference_id,
            ))

        mirrored = (
            "referenceID", "date", "details", "amount", "tag",
            "fileID", "source", "bank", "user", "processed_via",
            "gmail_message_id", "transfer_group_id",
            "bank_reference_id",
        )
        for orig, res in zip(originals, restored):
            for col in mirrored:
                self.assertEqual(
                    getattr(orig, col), getattr(res, col),
                    f"round-trip lost {col}",
                )


if __name__ == "__main__":
    print("ak-ex2 non-savings-row-stripper tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
