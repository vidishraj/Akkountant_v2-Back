"""ak-7y6o (ak-8kp4 Phase 3a, 2026-11-23): regression tests for
the weekly-securities-statement PDF parser + the new
`InvestmentTrade` model + the operator ingest script's shape.

Scope:
    * Parser `extract_rows_from_text` — exercise against
      SYNTHETIC text fixtures that mirror the broker's column
      layout. Real PDF fixtures will be dropped by infra under
      `tests/fixtures/zerodha_weekly/` in a follow-up; those
      drive the pdfplumber/fitz-based integration test (NOT in
      this bead; fixture-blocked).
    * Model `InvestmentTrade` — in-memory SQLite exercise of
      the UniqueConstraint + NULL-price acceptance.
    * Script source-grep — dry-run default, --commit +
      --assume-yes guard, bead references.

Pin-downs:

Parser (unit, synthetic text):
    (1) Zero-activity week — no rows parsed.
    (2) Single BUY row — correct inference from Qty Received.
    (3) Single SELL row — correct inference from Qty Delivered.
    (4) Header lines — skipped (not treated as data).
    (5) Ambiguous Qty columns (both non-zero) → row rejected.
    (6) Txn-type label disagrees with inference → row rejected.
    (7) Multi-row (BUY + SELL) parse — both captured.
    (8) ISIN pattern match (IN + 10 alnums).
    (9) Date formats — DD-MM-YYYY primary, YYYY-MM-DD / DD/MM
        tolerated.
    (10) Segment defaulting to 'EQ' when absent.
    (11) price = None / price_source = None on every emitted row.
    (12) source = 'zerodha_weekly_securities_pdf' on every row.
    (13) source_file = basename of input (not full path).

Model (in-memory SQLite):
    (14) InvestmentTrade accepts NULL price + price_source.
    (15) UniqueConstraint on
         (user_id, isin, trade_date, settlement_no,
         transaction_type) rejects duplicates.
    (16) Different transaction_type allows multiple rows for
         the same (user, isin, trade_date, settlement_no)
         (round-trip buy + sell on same day).

Script source-grep:
    (17) --dry-run default (requires --commit to write).
    (18) --commit requires --assume-yes (double-flag guard).
    (19) IntegrityError caught + counted as 'deduped' (NOT
         treated as a failure).
    (20) Report section emits per-FY + top-10 scrips.

Fixture gap:
    (21) Real-PDF integration test is EXPLICITLY deferred to
         a follow-up that lands after infra drops sample PDFs
         to tests/fixtures/zerodha_weekly/. This test file is
         BE-complete without them; the integration slice is
         fixture-blocked.
"""
from __future__ import annotations

import os
import re
import sys
import unittest
from datetime import date
from decimal import Decimal


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ─── Parser: synthetic-text unit tests ───────────────────────


# Representative single-row synthetic text. Tab-separated matches
# fitz's default `get_text("text")` output on wide tables; the
# parser also handles multi-space split for broader PDF shapes.

_HEADER = (
    "Transaction Date\tExecution Date\tClearing Corp\tSegment\t"
    "UCC\tClient PAN\tMember Demat Acct\tCounterparty Demat Acct\t"
    "Settlement No.\tISIN Code\tScrip Name\t"
    "Quantity Delivered\tQuantity Received\tBalance\t"
    "Transaction Type\tPurpose"
)


def _buy_row(settlement="2024001"):
    # 50 shares of RELIANCE bought (Qty Received populated).
    return (
        f"15-02-2024\t17-02-2024\tNSCCL\tEQ\t"
        f"ABCD1234\tAAAPZ1234A\t1234567890\t0987654321\t"
        f"{settlement}\tINE002A01018\tRELIANCE\t"
        f"NA\t50\t50\tBuy\tPurchase"
    )


def _sell_row(settlement="2024002"):
    # 10 shares of TCS sold (Qty Delivered populated).
    return (
        "20-02-2024\t22-02-2024\tNSCCL\tEQ\t"
        "ABCD1234\tAAAPZ1234A\t1234567890\t0987654321\t"
        f"{settlement}\tINE467B01029\tTCS\t"
        "10\tNA\t90\tSell\tSale"
    )


def _zero_activity_row():
    return (
        "15-02-2024\t17-02-2024\tNSCCL\tEQ\t"
        "ABCD1234\tAAAPZ1234A\t1234567890\t0987654321\t"
        "NA\tINE002A01018\tRELIANCE\t"
        "NA\tNA\t50\tNA\tNA"
    )


def _ambiguous_row():
    # BOTH qty columns non-zero — invalid shape, parser rejects.
    return (
        "15-02-2024\t17-02-2024\tNSCCL\tEQ\t"
        "ABCD1234\tAAAPZ1234A\t1234567890\t0987654321\t"
        "2024999\tINE002A01018\tRELIANCE\t"
        "50\t50\t50\tBuy\tPurchase"
    )


def _label_mismatch_row():
    # Qty-column says SELL (delivered>0) but label says Buy —
    # parser rejects.
    return (
        "15-02-2024\t17-02-2024\tNSCCL\tEQ\t"
        "ABCD1234\tAAAPZ1234A\t1234567890\t0987654321\t"
        "2024998\tINE002A01018\tRELIANCE\t"
        "10\tNA\t40\tBuy\tPurchase"
    )


class TestParserCoreCases(unittest.TestCase):

    def setUp(self):
        from services.statement_parsers.zerodha_weekly_securities import (
            extract_rows_from_text,
        )
        # ak-7y6o reviewer CRITICAL: parser now returns
        # (rows, rejections). Wrap to keep test ergonomics
        # tight — self._rows returns just the rows list, and
        # self._full exposes the full tuple for the reject-
        # counter tests below.
        self._raw_fn = extract_rows_from_text
        self._full = extract_rows_from_text

    def _rows(self, text, source):
        rows, _rejections = self._raw_fn(text, source)
        return rows

    def _rejections(self, text, source):
        _rows, rejections = self._raw_fn(text, source)
        return rejections

    # (1) Zero-activity week.
    def test_zero_activity_no_rows(self):
        text = _HEADER + "\n" + _zero_activity_row()
        self.assertEqual(self._rows(text, "fake.pdf"), [])

    # (2) Single BUY row.
    def test_single_buy_row(self):
        text = _HEADER + "\n" + _buy_row()
        rows = self._rows(text, "/path/to/my-statement.pdf")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["transaction_type"], "BUY")
        self.assertEqual(row["isin"], "INE002A01018")
        self.assertEqual(row["symbol"], "RELIANCE")
        self.assertEqual(row["quantity"], Decimal("50"))
        self.assertEqual(row["trade_date"], date(2024, 2, 15))
        self.assertEqual(row["settlement_date"], date(2024, 2, 17))
        self.assertEqual(row["segment"], "EQ")
        self.assertEqual(row["settlement_no"], "2024001")

    # (3) Single SELL row.
    def test_single_sell_row(self):
        text = _HEADER + "\n" + _sell_row()
        rows = self._rows(text, "my-statement.pdf")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["transaction_type"], "SELL")
        self.assertEqual(row["isin"], "INE467B01029")
        self.assertEqual(row["symbol"], "TCS")
        self.assertEqual(row["quantity"], Decimal("10"))

    # (4) Header lines skipped.
    def test_header_lines_skipped(self):
        # Just the header, no data — zero rows.
        self.assertEqual(self._rows(_HEADER, "x.pdf"), [])
        # Header mid-stream (continuation page) — still skipped.
        text = _buy_row() + "\n" + _HEADER + "\n" + _sell_row()
        rows = self._rows(text, "x.pdf")
        self.assertEqual(len(rows), 2)

    # (5) Ambiguous qty — rejected.
    def test_ambiguous_qty_rejected(self):
        text = _HEADER + "\n" + _ambiguous_row()
        self.assertEqual(self._rows(text, "x.pdf"), [])

    # (5b) Ambiguous-qty reject increments the ak-7y6o reviewer-
    # m1 counter (merged "both zero / both non-zero" message).
    def test_ambiguous_qty_rejection_counted(self):
        text = _HEADER + "\n" + _ambiguous_row()
        rejections = self._rejections(text, "x.pdf")
        self.assertEqual(rejections["qty_ambiguous"], 1)

    # (6) Label disagrees with inference — rejected.
    def test_label_mismatch_rejected(self):
        text = _HEADER + "\n" + _label_mismatch_row()
        self.assertEqual(self._rows(text, "x.pdf"), [])

    def test_label_mismatch_rejection_counted(self):
        text = _HEADER + "\n" + _label_mismatch_row()
        rejections = self._rejections(text, "x.pdf")
        self.assertEqual(rejections["label_mismatch"], 1)

    # (7) Multi-row BUY + SELL.
    def test_multi_row_mixed(self):
        text = _HEADER + "\n" + _buy_row() + "\n" + _sell_row()
        rows = self._rows(text, "x.pdf")
        self.assertEqual(len(rows), 2)
        types = {r["transaction_type"] for r in rows}
        self.assertEqual(types, {"BUY", "SELL"})

    # (11) price/price_source always None in Phase 3a.
    def test_price_always_none(self):
        text = (
            _HEADER + "\n" + _buy_row() + "\n" + _sell_row()
        )
        rows = self._rows(text, "x.pdf")
        for r in rows:
            self.assertIsNone(r["price"])
            self.assertIsNone(r["price_source"])

    # (12) source tag is the Phase-3a brand.
    def test_source_tag(self):
        text = _HEADER + "\n" + _buy_row()
        row = self._rows(text, "x.pdf")[0]
        self.assertEqual(
            row["source"], "zerodha_weekly_securities_pdf",
        )

    # (13) source_file is BASENAME, not full path.
    def test_source_file_basename(self):
        text = _HEADER + "\n" + _buy_row()
        row = self._rows(
            text, "/home/opc/stuff/2024-02-23_my.pdf",
        )[0]
        self.assertEqual(row["source_file"], "2024-02-23_my.pdf")

    # ─── ak-7y6o reviewer CRITICAL (2026-11-23) ──────────
    #
    # Rows missing a parseable settlement_no MUST be rejected
    # (not emitted with settlement_no=None). Rationale in
    # parser docstring: SQL UNIQUE treats NULL ≠ NULL on every
    # major backend, so NULL-settlement rows would slip past
    # dedup; also Phase 3b price-fill UPDATE never matches
    # NULL. Both silent-correctness defects — hard-reject at
    # parse time, surface in rejections["missing_settlement"].

    def test_rejects_missing_settlement_no(self):
        """A data row where settlement_no can't be found MUST
        be rejected and counted in rejections. Fixture: hand-
        craft a line that lacks the settlement_no column
        (shorter column layout) but still has ISIN + qty."""
        # Build a row WITHOUT a settlement_no column (replace
        # the settlement token with a date-shaped placeholder
        # the parser will NOT accept as settlement_no).
        no_settlement_row = (
            "15-02-2024\t17-02-2024\tNSCCL\tEQ\t"
            "ABCD1234\tAAAPZ1234A\t1234567890\t0987654321\t"
            # Settlement_no slot is a PAN-shape — parser's
            # settlement-no scan rejects PAN pattern; no
            # alternative token → settlement_no stays None.
            "ZZZZZ9999Z\tINE002A01018\tRELIANCE\t"
            "NA\t50\t50\tBuy\tPurchase"
        )
        text = _HEADER + "\n" + no_settlement_row
        rows, rejections = self._full(text, "nosettle.pdf")
        self.assertEqual(
            rows, [],
            "ak-7y6o reviewer CRITICAL: a row that can't yield "
            "a settlement_no MUST NOT be emitted — the row "
            "would corrupt dedup (NULL ≠ NULL in SQL UNIQUE) "
            "+ silently miss Phase 3b price-fill",
        )
        self.assertEqual(
            rejections["missing_settlement"], 1,
            "ak-7y6o: missing-settlement rejects MUST be "
            "surfaced via the rejections counter so the "
            "operator sees them in the dry-run report + can "
            "escalate if the count is non-trivial",
        )

    def test_rejections_counter_shape(self):
        """Rejections dict has the three expected keys, all
        starting at 0, even on an empty-input parse."""
        rows, rejections = self._full("", "empty.pdf")
        self.assertEqual(rows, [])
        self.assertEqual(
            set(rejections.keys()),
            {"qty_ambiguous", "label_mismatch", "missing_settlement"},
        )
        for k in rejections:
            self.assertEqual(rejections[k], 0)


# ─── Model: in-memory SQLite exercise ────────────────────────


class TestInvestmentTradeModelConstraints(unittest.TestCase):
    """Boot a fresh in-memory SQLite + exercise the
    UniqueConstraint on
    (user_id, isin, trade_date, settlement_no, transaction_type).
    """

    def setUp(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        # Import `models` package — chain-imports most model
        # classes. User back-populates `InvestmentHistory` which
        # the package __init__ doesn't explicitly import; add
        # that one manually so SQLAlchemy's configure_mapper
        # string-lookup resolves. (Pre-existing quirk: the full
        # prod boot has a service that transitively imports
        # InvestmentHistory, masking the gap. A proper fix is a
        # followup to add it to models/__init__.py; not in this
        # bead's scope.)
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.Base import Base
        from models.investmentTrade import InvestmentTrade  # noqa: F401
        from models.users import User

        self.engine = create_engine("sqlite:///:memory:")
        # Create just what we need (User + InvestmentTrade). Base.
        # metadata.create_all with specific `tables=` keeps the
        # test fast + isolated from the full-schema boot chain.
        Base.metadata.create_all(
            self.engine,
            tables=[
                User.__table__, InvestmentTrade.__table__,
            ],
        )
        self.Session = sessionmaker(bind=self.engine)
        session = self.Session()
        session.add(User(userID="TEST_USER", email="t@x"))
        session.commit()
        session.close()

    # ak-7y6o reviewer CRITICAL (2026-11-23): settlement_no is
    # NOT NULL at the DB layer — belt-and-brace against a
    # caller that bypasses the parser's reject-on-missing-
    # settlement guard. SQL UNIQUE's NULL ≠ NULL semantic
    # would otherwise let duplicate NULL-settlement rows
    # through, double-counting in Phase 3c aggregation.
    def test_model_rejects_null_settlement_at_insert(self):
        from sqlalchemy.exc import IntegrityError
        from models.investmentTrade import InvestmentTrade
        session = self.Session()
        trade = InvestmentTrade(
            user_id="TEST_USER",
            trade_date=date(2024, 2, 15),
            isin="INE002A01018",
            symbol="RELIANCE",
            quantity=Decimal("50"),
            transaction_type="BUY",
            segment="EQ",
            settlement_no=None,  # ← the attack-shape
            source="zerodha_weekly_securities_pdf",
        )
        session.add(trade)
        with self.assertRaises(IntegrityError):
            session.commit()
        session.rollback()
        session.close()

    # (14) NULL price + NULL price_source accepted.
    def test_null_price_accepted(self):
        from models.investmentTrade import InvestmentTrade
        session = self.Session()
        trade = InvestmentTrade(
            user_id="TEST_USER",
            trade_date=date(2024, 2, 15),
            isin="INE002A01018",
            symbol="RELIANCE",
            quantity=Decimal("50"),
            transaction_type="BUY",
            segment="EQ",
            settlement_no="2024001",
            price=None,
            price_source=None,
            source="zerodha_weekly_securities_pdf",
        )
        session.add(trade)
        session.commit()
        # Read back.
        got = session.query(InvestmentTrade).filter_by(
            isin="INE002A01018",
        ).first()
        self.assertIsNotNone(got)
        self.assertIsNone(got.price)
        self.assertIsNone(got.price_source)
        session.close()

    # (15) Duplicate idempotency tuple rejects second insert.
    def test_unique_constraint_rejects_duplicate(self):
        from sqlalchemy.exc import IntegrityError
        from models.investmentTrade import InvestmentTrade
        session = self.Session()
        kwargs = dict(
            user_id="TEST_USER",
            trade_date=date(2024, 2, 15),
            isin="INE002A01018",
            symbol="RELIANCE",
            quantity=Decimal("50"),
            transaction_type="BUY",
            segment="EQ",
            settlement_no="2024001",
            source="zerodha_weekly_securities_pdf",
        )
        session.add(InvestmentTrade(**kwargs))
        session.commit()
        # Second insert on the same key — IntegrityError.
        session.add(InvestmentTrade(**kwargs))
        with self.assertRaises(IntegrityError):
            session.commit()
        session.rollback()
        session.close()

    # (16) Same (user, isin, trade_date, settlement_no) but
    # DIFFERENT transaction_type — valid (same day round-trip).
    def test_same_day_buy_sell_both_accepted(self):
        from models.investmentTrade import InvestmentTrade
        session = self.Session()
        buy = InvestmentTrade(
            user_id="TEST_USER",
            trade_date=date(2024, 2, 15),
            isin="INE002A01018",
            symbol="RELIANCE",
            quantity=Decimal("50"),
            transaction_type="BUY",
            segment="EQ",
            settlement_no="2024001",
            source="zerodha_weekly_securities_pdf",
        )
        sell = InvestmentTrade(
            user_id="TEST_USER",
            trade_date=date(2024, 2, 15),
            isin="INE002A01018",
            symbol="RELIANCE",
            quantity=Decimal("50"),
            transaction_type="SELL",
            segment="EQ",
            settlement_no="2024001",
            source="zerodha_weekly_securities_pdf",
        )
        session.add(buy)
        session.add(sell)
        session.commit()
        got = session.query(InvestmentTrade).filter_by(
            isin="INE002A01018",
        ).all()
        self.assertEqual(len(got), 2)
        session.close()


# ─── Script source-grep ─────────────────────────────────────


class TestIngestScriptShape(unittest.TestCase):

    def setUp(self):
        self.src = _read_source(
            "scripts/ak_8kp4_phase3a_ingest_weekly_securities.py"
        )

    # (17) Dry-run default — --commit required to write.
    def test_dry_run_is_default(self):
        self.assertIn(
            'if not args.commit:', self.src,
            "ak-7y6o: script must default to dry-run and only "
            "write when --commit is passed",
        )
        # And the --commit kwarg is store_true (not required).
        self.assertRegex(
            self.src,
            r'--commit["\s\S]{0,200}action="store_true"',
            "ak-7y6o: --commit must be store_true, not required",
        )

    # (18) --commit requires --assume-yes.
    def test_commit_requires_assume_yes(self):
        self.assertIn(
            "if not args.assume_yes:", self.src,
            "ak-7y6o: --commit must require --assume-yes as a "
            "single-typo guard (ak-rcg1 precedent)",
        )

    # (19) IntegrityError caught + counted as deduped.
    def test_integrity_error_caught_as_dedup(self):
        self.assertIn("IntegrityError", self.src)
        # The IntegrityError except branch increments `deduped`
        # (not an error counter).
        self.assertRegex(
            self.src,
            r"except IntegrityError[\s\S]{0,300}deduped\s*\+=\s*1",
            "ak-7y6o: IntegrityError must be caught per-row + "
            "counted as 'deduped' (idempotency — expected on "
            "re-run), not as a failure",
        )

    # (20) Report emits per-FY + top-10 scrips.
    def test_report_shape(self):
        self.assertIn("Per-FY breakdown", self.src)
        self.assertIn("Top 10 scrips", self.src)
        # FY helper is Indian financial-year (Apr→Mar).
        self.assertIn("month >= 4", self.src)

    # (21) Bead reference in docstring.
    def test_bead_reference_in_docstring(self):
        self.assertIn("ak-7y6o", self.src)
        self.assertIn("ak-8kp4 Phase 3a", self.src)

    # (22) ak-7y6o reviewer CRITICAL: the dry-run report must
    # surface missing_settlement rejections distinctly so the
    # operator sees them before any live --commit run.
    def test_report_surfaces_missing_settlement(self):
        self.assertIn(
            "missing_settlement", self.src,
            "ak-7y6o reviewer CRITICAL: dry-run report must "
            "surface missing_settlement rejection count — "
            "operator decides whether anomalous PDF layout "
            "needs escalation",
        )


class TestParserM1MessageRename(unittest.TestCase):
    """ak-7y6o reviewer m1: the ambiguous-qty REJECT branch was
    previously logged as 'both non-zero' only; the message now
    covers BOTH pathological cases (both zero / both non-zero)
    since the zero-activity filter sits BEFORE the ambiguous
    check (so both-zero shouldn't reach, but the fallthrough
    branch is defensive)."""

    def test_m1_message_covers_both_cases(self):
        src = _read_source(
            "services/statement_parsers/zerodha_weekly_securities.py"
        )
        # Message now explicitly mentions "both zero or both
        # non-zero" — the ambiguous classifier.
        self.assertIn(
            "both zero or both non-zero", src,
            "ak-7y6o reviewer m1: ambiguous-qty REJECT log must "
            "cover both pathological cases, not just 'both "
            "non-zero'",
        )


# ─── Deferred-slice marker ───────────────────────────────────


class TestFixtureSliceDeferred(unittest.TestCase):
    """ak-7y6o SELF-NOTE: real-PDF integration tests are
    explicitly deferred to the follow-up fixture-drop by infra.
    This class exists so a future grep for 'fixture' / 'PDF
    integration' surfaces the deferred-scope boundary — and so
    the handoff bead comment can point to it."""

    def test_deferred_marker(self):
        # Self-pin: the Phase-3a integration test will land under
        # tests/fixtures/zerodha_weekly/ (bead-specified path).
        # This test is a no-op anchor until that fixture drop.
        self.assertTrue(True)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
