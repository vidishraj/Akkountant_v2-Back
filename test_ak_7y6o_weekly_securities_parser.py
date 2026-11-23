"""ak-7y6o / ak-4t7f (ak-8kp4 Phase 3a): regression tests for the
weekly-securities-statement PDF parser, the `InvestmentTrade` model,
and the operator ingest script's shape.

ak-4t7f: the original tests fed synthetic ONE-LINE rows, a layout
the real PDFs never produce (fitz emits them one cell per line), so
the parser shipped yielding 0 rows on all prod PDFs. The fixtures
below mirror the REAL extraction shape, measured over the Overseer's
statements:

  * `Table.extract()` matrices: 21 physical columns, the broker's
    exact header labels, demat columns split over two sub-columns
    (one empty), '\\n' inside wrapped cells, YYYY-MM-DD dates,
    "10.000"-style quantities, all-"NA" zero-activity rows, and a
    SETTLED table followed by a PENDING-obligations table.
  * a PDF generated at test time with the same ruled two-table
    layout, run through the real `parse_weekly_securities_pdf`
    (`page.find_tables()`). Skipped if PyMuPDF isn't installed.

All values are SYNTHETIC: dummy PAN / name / demat numbers, and
ISINs with valid check digits that are not issued securities
(INE000T0101x). No real holdings live in this repo.
"""
from __future__ import annotations

import logging
import os
import re
import sys
import tempfile
import unittest
from datetime import date
from decimal import Decimal


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ─── Real-shaped synthetic fixtures ──────────────────────────

ISIN_A = "INE000T01012"
ISIN_B = "INE000T01020"
ISIN_C = "INE000T01038"
FAKE_PAN = "ABCDE1234F"
FAKE_DEMAT = "1234567890123456"
FAKE_CP_DEMAT = "6543210987654321"

# Exactly what Table.extract() yields for the header row: 21
# physical columns; the two demat headers each span 2 sub-columns,
# so their second cell is None. Some labels wrap ('\n').
HEADER = [
    "Transaction\nDate", "Execution\nDate",
    "Clearing\nCorporation/Clearing\nMember", "Segment\nType",
    "Unique\nClient\nCode\n(UCC)", "Back\nOffice\nClient\nCode",
    "Client Name", "Client PAN", "Member\nDemat\nAccount\nNo.", None,
    "Counterparty Demat Account No.", None, "Settlement No.",
    "ISIN\nCode", "Scrip Name", "Quantity Delivered (Qty.)",
    "Quantity\nReceived\n(Qty.)", "Balance\n(Qty.)", "Trf. Ref.\nNo.",
    "Transaction Type", "Purpose",
]
# The DP-ID / Client-ID sub-header row under the demat columns.
SUBHEADER = [""] * 8 + ["DP", "ID", "DP", "ID"] + [""] * 9
NA_ROW = ["NA"] * 21


def settled_row(
    isin=ISIN_A, scrip="SYNTH CO LTD-EQ\n10/-", delivered="0.000",
    received="5.000", balance="5.000", settlement="2024001",
    txn_date="2024-02-16", exec_date="2024-02-16",
    txn_type="Transactions\nwithin DP", purpose="payout",
):
    """A SETTLED-section data row, real cell shapes. Defaults are
    a BUY (Qty Received > 0, purpose payout)."""
    return [
        txn_date, exec_date, "NSE Clearing\n/ Synth Broker\nLtd", "EQ",
        "XX0000", "XX0000", "TEST USER", FAKE_PAN,
        "", FAKE_DEMAT, FAKE_CP_DEMAT, "", settlement,
        isin, scrip, delivered, received, balance, "1234567",
        txn_type, purpose,
    ]


def sell_row(**kw):
    kw.setdefault("delivered", "3.000")
    kw.setdefault("received", "0.000")
    kw.setdefault("balance", "2.000")
    kw.setdefault("txn_type", "Early Pay-\nin")
    kw.setdefault("purpose", "payin")
    kw.setdefault("settlement", "2024002")
    kw.setdefault("isin", ISIN_B)
    return settled_row(**kw)


def pending_row(isin=ISIN_C, settlement="2024003"):
    """PENDING-section row. Broker-side convention: a client BUY
    shows as purpose=payin with Qty Delivered > 0 and a negative
    balance; Execution Date / Ref / Type are NA."""
    return [
        "2024-02-15", "NA", "NSE Clearing\n/ Synth Broker\nLtd", "EQ",
        "XX0000", "XX0000", "TEST USER", FAKE_PAN,
        FAKE_DEMAT, "", "", FAKE_CP_DEMAT, settlement,
        isin, "SYNTH TWO LTD", "4.000", "0", "-4.000", "NA", "NA",
        "payin",
    ]


def doc(settled_rows, pending_rows=None):
    """One page with the two tables, as parse_weekly_securities_pdf
    hands them to extract_rows_from_tables."""
    return [
        {"section": "settled", "page": 0,
         "rows": [HEADER, SUBHEADER] + list(settled_rows)},
        {"section": "pending", "page": 0,
         "rows": [HEADER, SUBHEADER] + list(pending_rows or [NA_ROW])},
    ]


class TestParserRealShapedTables(unittest.TestCase):

    def setUp(self):
        from services.statement_parsers import zerodha_weekly_securities as m
        self.m = m

    def parse(self, tables, src="/x/2024-02-16_weekly.pdf"):
        return self.m.extract_rows_from_tables(tables, src)

    def test_zero_activity_week(self):
        rows, rej, st = self.parse(doc([NA_ROW]))
        self.assertEqual(rows, [])
        self.assertEqual(sum(rej.values()), 0)
        self.assertEqual(st["zero_activity"], 2)  # one per table
        self.assertEqual(st["isin_rows"], 0)

    def test_buy_row_fields(self):
        rows, rej, _ = self.parse(doc([settled_row()]))
        self.assertEqual(len(rows), 1, rej)
        r = rows[0]
        self.assertEqual(r["transaction_type"], "BUY")
        self.assertEqual(r["quantity"], Decimal("5.000"))
        self.assertEqual(r["isin"], ISIN_A)
        self.assertEqual(r["symbol"], "SYNTH CO LTD-EQ 10/-")  # wrap joined
        self.assertEqual(r["trade_date"], date(2024, 2, 16))
        self.assertEqual(r["settlement_date"], date(2024, 2, 16))
        self.assertEqual(r["settlement_no"], "2024001")
        self.assertEqual(r["segment"], "EQ")
        self.assertIsNone(r["price"])
        self.assertIsNone(r["price_source"])
        self.assertEqual(r["source"], "zerodha_weekly_securities_pdf")
        self.assertEqual(r["source_file"], "2024-02-16_weekly.pdf")

    def test_sell_row(self):
        rows, rej, _ = self.parse(doc([sell_row()]))
        self.assertEqual(len(rows), 1, rej)
        self.assertEqual(rows[0]["transaction_type"], "SELL")
        self.assertEqual(rows[0]["quantity"], Decimal("3.000"))

    def test_scrip_name_ending_in_number_not_taken_as_qty(self):
        # Real scrip names end in e.g. "RS 1"; column mapping by
        # header label must keep that out of the qty columns.
        rows, _, _ = self.parse(doc([settled_row(scrip="SYNTH-EQ RS 1")]))
        self.assertEqual(rows[0]["symbol"], "SYNTH-EQ RS 1")
        self.assertEqual(rows[0]["quantity"], Decimal("5.000"))

    def test_pending_rows_skipped_and_counted(self):
        # Pending obligations settle in a later statement's SETTLED
        # table; ingesting them would double-count. Their reversed
        # payin/delivered convention must not be read as a SELL.
        rows, rej, st = self.parse(doc([settled_row()], [pending_row()]))
        self.assertEqual([r["isin"] for r in rows], [ISIN_A])
        self.assertEqual(st["pending_skipped"], 1)
        self.assertEqual(st["isin_rows"], 2)
        self.assertEqual(sum(rej.values()), 0)

    def test_columns_mapped_by_header_not_position(self):
        # Swap the two qty columns in header AND data: a positional
        # parser would flip BUY↔SELL; a label-mapped one must not.
        hdr = list(HEADER)
        hdr[15], hdr[16] = hdr[16], hdr[15]
        row = settled_row()
        row[15], row[16] = row[16], row[15]
        tables = [{"section": "settled", "page": 0, "rows": [hdr, row]}]
        rows, rej, _ = self.parse(tables)
        self.assertEqual(rows[0]["transaction_type"], "BUY", rej)

    def test_multi_row_mixed(self):
        rows, _, _ = self.parse(doc([settled_row(), sell_row()]))
        self.assertEqual(
            sorted(r["transaction_type"] for r in rows), ["BUY", "SELL"],
        )

    def test_dd_mm_yyyy_tolerated(self):
        rows, _, _ = self.parse(doc([settled_row(txn_date="16-02-2024")]))
        self.assertEqual(rows[0]["trade_date"], date(2024, 2, 16))

    # ── multi-page ──

    def test_continuation_page_without_header_inherits_map(self):
        tables = [
            {"section": "settled", "page": 0,
             "rows": [HEADER, SUBHEADER, settled_row()]},
            # page 2: no title above, no header row
            {"section": None, "page": 1, "rows": [sell_row()]},
            {"section": "pending", "page": 1,
             "rows": [HEADER, SUBHEADER, NA_ROW]},
        ]
        rows, rej, _ = self.parse(tables)
        self.assertEqual(len(rows), 2, rej)

    def test_row_split_across_page_break_is_merged(self):
        whole = settled_row(scrip="SYNTH CO LTD-EQ", txn_type="Transactions")
        top = list(whole)
        tail = [""] * 21
        tail[14] = "10/-"          # scrip name tail
        tail[19] = "within DP"     # txn-type tail
        tables = [
            {"section": "settled", "page": 0,
             "rows": [HEADER, SUBHEADER, top]},
            {"section": None, "page": 1, "rows": [tail, sell_row()]},
        ]
        rows, rej, _ = self.parse(tables)
        self.assertEqual(len(rows), 2, rej)
        self.assertEqual(rows[0]["symbol"], "SYNTH CO LTD-EQ 10/-")

    def test_headed_table_without_section_fails_closed(self):
        # A table with its OWN header row but no section title is
        # not a continuation: reject, don't inherit "settled".
        tables = [
            {"section": "settled", "page": 0,
             "rows": [HEADER, SUBHEADER, settled_row()]},
            {"section": None, "page": 1,
             "rows": [HEADER, SUBHEADER, sell_row()]},
        ]
        rows, rej, _ = self.parse(tables)
        self.assertEqual([r["isin"] for r in rows], [ISIN_A])
        self.assertEqual(rej["malformed"], 1)

    def test_short_continuation_row_does_not_crash(self):
        tables = [
            {"section": "settled", "page": 0,
             "rows": [HEADER, SUBHEADER, settled_row()]},
            {"section": None, "page": 1, "rows": [["stray"]]},
        ]
        rows, rej, _ = self.parse(tables)
        self.assertEqual(len(rows), 1, rej)

    def test_unmapped_table_rows_rejected_not_dropped(self):
        tables = [{"section": "settled", "page": 0, "rows": [settled_row()]}]
        rows, rej, _ = self.parse(tables)
        self.assertEqual(rows, [])
        self.assertEqual(rej["malformed"], 1)

    # ── rejections ──

    def test_ambiguous_qty_rejected(self):
        for d_, r_ in (("5.000", "5.000"), ("0.000", "0.000")):
            rows, rej, _ = self.parse(doc([settled_row(delivered=d_, received=r_)]))
            self.assertEqual(rows, [])
            self.assertEqual(rej["qty_ambiguous"], 1)

    def test_label_mismatch_rejected(self):
        # Qty says BUY, purpose says payin (SELL).
        rows, rej, _ = self.parse(doc([settled_row(purpose="payin")]))
        self.assertEqual(rows, [])
        self.assertEqual(rej["label_mismatch"], 1)

    def test_missing_settlement_rejected(self):
        # NULL settlement defeats UNIQUE dedup (NULL ≠ NULL) and the
        # Phase 3b price-fill join — hard reject.
        for bad in ("", "NA", FAKE_PAN):
            rows, rej, _ = self.parse(doc([settled_row(settlement=bad)]))
            self.assertEqual(rows, [], bad)
            self.assertEqual(rej["missing_settlement"], 1, bad)

    def test_bad_isin_checksum_rejected(self):
        rows, rej, st = self.parse(doc([settled_row(isin="INE000T01013")]))
        self.assertEqual(rows, [])
        self.assertEqual(rej["malformed"], 1)
        self.assertEqual(st["isin_rows"], 0)

    def test_unparseable_qty_rejected_not_zeroed(self):
        rows, rej, _ = self.parse(doc([settled_row(received="5,O00")]))
        self.assertEqual(rows, [])
        self.assertEqual(rej["malformed"], 1)

    def test_over_length_symbol_rejected(self):
        rows, rej, _ = self.parse(doc([settled_row(scrip="X" * 51)]))
        self.assertEqual(rows, [])
        self.assertEqual(rej["malformed"], 1)

    def test_rejections_counter_shape(self):
        rows, rej, st = self.parse([])
        self.assertEqual(rows, [])
        self.assertEqual(
            set(rej),
            {"qty_ambiguous", "label_mismatch", "missing_settlement",
             "malformed"},
        )
        self.assertFalse(any(rej.values()))
        self.assertFalse(any(st.values()))

    # ── secrecy ──

    def test_reject_logs_carry_no_financial_values(self):
        bad = [
            settled_row(delivered="5.000"),             # qty_ambiguous
            settled_row(purpose="payin", settlement="2024009"),  # label
            settled_row(settlement="NA"),               # missing_settlement
        ]
        with self.assertLogs(self.m._LOG, level="WARNING") as cm:
            self.parse(doc(bad), src="/x/statement.pdf")
        blob = "\n".join(cm.output)
        self.assertEqual(len(cm.output), 3)
        for secret in (ISIN_A, "SYNTH", "5.000", "2024-02-16",
                       "2024009", FAKE_PAN, FAKE_DEMAT, "TEST USER"):
            self.assertNotIn(secret, blob)

    def test_isin_checksum_helper(self):
        f = self.m.isin_checksum_ok
        self.assertTrue(f(ISIN_A))
        self.assertFalse(f("INE000T01013"))  # same stem, bad check digit
        self.assertFalse(f("INZ0000316"))    # wrong length

    def test_text_isin_count_excludes_broker_reg_no(self):
        # The header's "SEBI Reg.No.: INZ…" passes the ISIN checksum
        # by accident; the text-side count must not include it.
        txt = f"SEBI Reg.No.: INZ000031633\n{ISIN_A}\n{ISIN_B}\nINE000T01013"
        self.assertEqual(self.m.count_text_isins(txt), 2)


# ─── Real-layout PDF through fitz find_tables ────────────────


def _build_statement_pdf(path, settled_rows, pending_rows):
    """Render a one-page statement with the broker's real layout:
    title lines, then two ruled 21-column tables (header +
    sub-header + data) on an A4-landscape page."""
    import fitz
    page_w, page_h = 842, 595
    d = fitz.open()
    page = d.new_page(width=page_w, height=page_h)
    page.insert_text((7, 30), "Synth Broker Ltd", fontsize=7)
    page.insert_text((7, 110), "SEBI Reg.No.: INZ000031633", fontsize=7)
    # 21 columns; the scrip column is wider (as on the real PDF).
    widths = [36] * 21
    widths[14] = 80
    xs = [8]
    for w in widths:
        xs.append(xs[-1] + w)

    def table(y, rows):
        heights = [34, 9] + [20] * len(rows)
        all_rows = [HEADER, SUBHEADER] + rows
        yy = y
        for h, row in zip(heights, all_rows):
            for c in range(21):
                rect = fitz.Rect(xs[c], yy, xs[c + 1], yy + h)
                page.draw_rect(rect, color=(0, 0, 0), width=0.5)
                txt = row[c] or ""
                if txt:
                    page.insert_textbox(rect + (1, 1, -1, -1), txt,
                                        fontsize=4.2, fontname="helv")
            yy += h
        return yy

    page.insert_text(
        (7, 183),
        "Statement of Accounts of Securities/Commodities for the "
        "period from 2024-02-12 to 2024-02-17 (Monday to Saturday):",
        fontsize=7)
    page.insert_text(
        (7, 201),
        "Statement of Account reflecting Clear Balance as on the "
        "last date of statement:", fontsize=7)
    y = table(212, settled_rows)
    page.insert_text(
        (7, y + 16),
        "Pending Obligations as on 2024-02-17 (as on Saturday):",
        fontsize=7)
    table(y + 24, pending_rows)
    d.save(path)
    d.close()


try:
    import fitz as _fitz  # noqa: F401
    _HAVE_FITZ = hasattr(_fitz.Page, "find_tables")
except ImportError:  # pragma: no cover
    _HAVE_FITZ = False


@unittest.skipUnless(_HAVE_FITZ, "PyMuPDF >= 1.23 not installed")
class TestParserRealLayoutPdf(unittest.TestCase):
    """End-to-end over a PDF with the real ruled layout. This is
    the test that would have caught the 0-rows-on-prod defect."""

    def setUp(self):
        from services.statement_parsers.zerodha_weekly_securities import (
            parse_weekly_securities_pdf,
        )
        self.parse_pdf = parse_weekly_securities_pdf
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def _pdf(self, settled, pending):
        p = os.path.join(self.tmp.name, "2024-02-17_weekly-securities-statement_HA6023.pdf")
        _build_statement_pdf(p, settled, pending)
        return p

    def test_get_text_is_one_cell_per_line(self):
        # Pin the property that broke the old line parser.
        import fitz
        p = self._pdf([settled_row()], [NA_ROW])
        with fitz.open(p) as d:
            text = d[0].get_text("text")
        isin_line = next(l for l in text.splitlines() if ISIN_A in l)
        self.assertEqual(isin_line.strip(), ISIN_A)

    def test_rows_parsed_and_reconciled(self):
        p = self._pdf([settled_row(), sell_row()], [pending_row()])
        rows, rej, st = self.parse_pdf(p)
        self.assertEqual(
            sorted((r["isin"], r["transaction_type"], r["quantity"]) for r in rows),
            [(ISIN_A, "BUY", Decimal("5.000")),
             (ISIN_B, "SELL", Decimal("3.000"))],
            (rej, st),
        )
        self.assertEqual(st["pending_skipped"], 1)
        self.assertEqual(st["text_isins"], 3)
        self.assertEqual(st["unaccounted_isins"], 0)
        self.assertEqual(sum(rej.values()), 0)
        self.assertEqual(
            st["text_isins"],
            len(rows) + sum(rej.values()) + st["pending_skipped"],
        )

    def test_zero_activity_pdf(self):
        rows, rej, st = self.parse_pdf(self._pdf([NA_ROW], [NA_ROW]))
        self.assertEqual(rows, [])
        self.assertEqual(st["text_isins"], 0)  # INZ reg-no excluded
        self.assertEqual(st["zero_activity"], 2)

    def test_isins_with_no_tables_warns_loudly(self):
        import fitz
        p = os.path.join(self.tmp.name, "flat.pdf")
        d = fitz.open()
        d.new_page().insert_text((50, 50), f"{ISIN_A} SYNTH 5.000")
        d.save(p)
        d.close()
        with self.assertLogs(
            "services.statement_parsers.zerodha_weekly_securities",
            level="WARNING",
        ) as cm:
            rows, rej, st = self.parse_pdf(p)
        self.assertEqual(rows, [])
        self.assertEqual(st["unaccounted_isins"], 1)
        self.assertIn("did not reach", "\n".join(cm.output))
        self.assertNotIn(ISIN_A, "\n".join(cm.output))


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

    # (20) Report: per-FY aggregates; ak-4t7f secrecy — NO per-row
    # dump and NO per-scrip table in logs.
    def test_report_shape(self):
        self.assertIn("Per-FY breakdown", self.src)
        self.assertIn("month >= 4", self.src)  # Indian FY (Apr→Mar)
        self.assertIn("ISIN reconciliation", self.src)

    def test_report_logs_aggregates_only(self):
        self.assertNotIn("Top 10 scrips", self.src)
        self.assertNotIn("First 5", self.src)
        self.assertNotIn("most_common", self.src)
        # The live-path failure WARN used to log ISIN + date.
        self.assertNotIn('row.get("isin")', self.src)
        self.assertNotIn('row.get("trade_date")', self.src)

    def test_isolated_mode_finds_repo_modules(self):
        # ak-4t7f: `python3 -I` drops the repo root from sys.path.
        self.assertIn("sys.path.insert(0, _REPO_ROOT)", self.src)
        self.assertNotIn("python3 -I scripts/", self.src)

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


# ─── Live insert path: behavioral (in-memory SQLite) ─────────


def _load_ingest_script():
    import importlib.util
    here = os.path.dirname(os.path.abspath(__file__))
    spec = importlib.util.spec_from_file_location(
        "ak_8kp4_phase3a_ingest",
        os.path.join(here, "scripts",
                     "ak_8kp4_phase3a_ingest_weekly_securities.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestLiveInsertRows(unittest.TestCase):
    """ak-4t7f: the live path used to session.rollback() on any
    IntegrityError, discarding earlier un-committed inserts while
    still counting them, and it treated ANY IntegrityError as a
    dedup. Exercise insert_rows against real SQLite SAVEPOINTs."""

    def setUp(self):
        from sqlalchemy import create_engine, event
        from sqlalchemy.orm import sessionmaker
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.Base import Base
        from models.investmentTrade import InvestmentTrade
        from models.users import User

        self.Trade = InvestmentTrade
        self.engine = create_engine("sqlite:///:memory:")

        # pysqlite SAVEPOINT recipe (SQLAlchemy docs): take
        # transaction control away from the driver so
        # begin_nested() emits a real SAVEPOINT.
        @event.listens_for(self.engine, "connect")
        def _connect(dbapi_conn, _rec):
            dbapi_conn.isolation_level = None

        @event.listens_for(self.engine, "begin")
        def _begin(conn):
            conn.exec_driver_sql("BEGIN")

        Base.metadata.create_all(
            self.engine, tables=[User.__table__, InvestmentTrade.__table__],
        )
        self.Session = sessionmaker(bind=self.engine)
        with self.Session() as s:
            s.add(User(userID="TEST_USER", email="t@x"))
            s.commit()
        self.script = _load_ingest_script()
        self.log = logging.getLogger("test_ak_4t7f_insert")

    def tearDown(self):
        self.engine.dispose()

    def _row(self, settlement, **kw):
        r = {
            "trade_date": date(2024, 2, 16), "settlement_date": None,
            "isin": ISIN_A, "symbol": "SYNTH", "quantity": Decimal("5"),
            "transaction_type": "BUY", "segment": "EQ",
            "settlement_no": settlement, "price": None,
            "price_source": None, "source": "zerodha_weekly_securities_pdf",
            "source_file": "x.pdf",
        }
        r.update(kw)
        return r

    def _run(self, rows):
        with self.Session() as s:
            out = self.script.insert_rows(
                s, self.Trade, "TEST_USER", rows, self.log,
            )
            s.commit()
        with self.Session() as s:
            persisted = sorted(t.settlement_no for t in s.query(self.Trade))
        return out, persisted

    def test_duplicate_mid_batch_keeps_earlier_rows(self):
        rows = [self._row("2024001"), self._row("2024002"),
                self._row("2024001"),  # dup of row 1, mid-batch
                self._row("2024003")]
        (ins, dedup, failed, ins_rows), persisted = self._run(rows)
        self.assertEqual((ins, dedup, failed), (3, 1, 0))
        self.assertEqual(len(ins_rows), 3)
        self.assertEqual(persisted, ["2024001", "2024002", "2024003"])

    def test_rerun_is_all_dedup(self):
        rows = [self._row("2024001"), self._row("2024002")]
        self._run(rows)
        (ins, dedup, failed, _), persisted = self._run(rows)
        self.assertEqual((ins, dedup, failed), (0, 2, 0))
        self.assertEqual(len(persisted), 2)

    def test_non_dedup_integrity_error_counts_as_failure(self):
        # NOT NULL violation — an IntegrityError that is NOT the
        # idempotency key; must be failed, not deduped.
        rows = [self._row("2024001"), self._row("2024002", symbol=None),
                self._row("2024003")]
        with self.assertLogs(self.log, level="WARNING") as cm:
            (ins, dedup, failed, _), persisted = self._run(rows)
        self.assertEqual((ins, dedup, failed), (2, 0, 1))
        self.assertEqual(persisted, ["2024001", "2024003"])
        self.assertNotIn("2024002", "\n".join(cm.output))

    def test_mysql_duplicate_classification(self):
        class Orig(Exception):
            def __init__(self, errno, msg):
                super().__init__(msg)
                self.errno = errno

        class Exc(Exception):
            def __init__(self, orig):
                self.orig = orig

        f = self.script.is_idempotency_duplicate
        self.assertTrue(f(Exc(Orig(1062, "Duplicate entry 'x' for key "
                                         "'investmentTrades.uq_trade_idempotent'"))))
        self.assertFalse(f(Exc(Orig(1062, "Duplicate entry 'x' for key "
                                          "'PRIMARY'"))))
        self.assertFalse(f(Exc(Orig(1452, "Cannot add or update a child "
                                          "row: a foreign key constraint "
                                          "fails"))))

    def test_script_exit_codes_wired(self):
        src = _read_source(
            "scripts/ak_8kp4_phase3a_ingest_weekly_securities.py")
        self.assertRegex(src, r"session\.get\(User, user_id\) is None"
                              r"[\s\S]{0,200}return 5")
        self.assertRegex(src, r"if failed:[\s\S]{0,300}return 6")


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
