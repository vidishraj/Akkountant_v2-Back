"""ak-d06w (BE for ak-9i7a): the Statements view list.

  * every statement-type row is listed (processed as a PDF, or a statement
    category), not only category == 'bank_statement', so misclassified /
    NULL-category statements are visible
  * the bank filter matches the row's resolved bank, not the category
  * the per-row `bank` is never a category name
  * bounded (limit clamped) and uid-scoped
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import os
import sys
import tempfile
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


class _DB(unittest.TestCase):
    def setUp(self):
        try:
            import flask
            import sqlalchemy as sa
            from sqlalchemy.orm import sessionmaker
            import models  # noqa: F401
            from models import investmentHistory  # noqa: F401
            from models.processedEmails import ProcessedEmails
            from models.statementPeriods import StatementPeriod
            self.TS = _load("services/transactionsService.py", "_akd06w_transactionsService")
        except Exception as e:  # pragma: no cover - env without deps
            self.skipTest(f"app deps unavailable: {e}")
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        StatementPeriod.__table__.create(eng)
        self.session = sessionmaker(bind=eng)()
        self.PE = ProcessedEmails
        self.app = flask.Flask("akd06w")
        self.ctx = self.app.test_request_context("/")
        self.ctx.push()
        flask.g.db = types.SimpleNamespace(session=self.session)
        self.ts = self.TS.TransactionService()
        self.cwd = tempfile.mkdtemp()
        self._old = os.getcwd()
        os.chdir(self.cwd)
        self._seed()

    def tearDown(self):
        os.chdir(self._old)
        self.ctx.pop()

    def _row(self, gid, sender, category, ptype="pdf", user="u1", summary=None, minute=0):
        self.session.add(self.PE(
            gmail_id=gid, user_id=user, sender=sender, subject=f"subj {gid}",
            category=category, processing_type=ptype, status="processed",
            extraction_summary=summary,
            processed_at=dt.datetime(2026, 11, 24, 10, minute)))

    def _seed(self):
        self._row("g_hdfc", "HDFC <alerts@hdfcbank.net>", "bank_statement", minute=1)
        self._row("g_cc", "HDFC <alerts@hdfcbank.net>", "bank_statement",
                  summary={"bank": "Millenia_Credit"}, minute=2)
        self._row("g_null", "SBI <cbssbi.cas@alerts.sbi.co.in>", None, minute=3)
        self._row("g_mis", "ICICI <statements@icicibank.com>", "transaction_alert", minute=4)
        self._row("g_cas", "CAMS <donotreply@camsonline.com>", "brokerage_statement",
                  ptype="text", summary={"bank": "CAMS"}, minute=5)
        self._row("g_alert", "HDFC <alerts@hdfcbank.net>", "transaction_alert",
                  ptype="text", minute=6)                               # not a statement
        self._row("g_other", "SBI <x@sbi.co.in>", "bank_statement", user="u2", minute=7)
        self.session.commit()

    def _ids(self, out):
        return [r["fileID"][3:] for r in out["results"]]


class TestListing(_DB):
    def test_all_statement_rows_listed_uid_scoped(self):
        out = self.ts.fetchProcessedStatements("u1")
        self.assertEqual(set(self._ids(out)), {"g_hdfc", "g_cc", "g_null", "g_mis", "g_cas"})
        self.assertEqual(out["total_count"], 5)

    def test_bank_is_resolved_never_a_category(self):
        out = self.ts.fetchProcessedStatements("u1")
        by = {r["fileID"][3:]: r["bank"] for r in out["results"]}
        self.assertEqual(by["g_hdfc"], "HDFC_DEBIT")          # sender map
        self.assertEqual(by["g_cc"], "Millenia_Credit")       # known enum from summary wins
        self.assertEqual(by["g_null"], "SBI")
        self.assertEqual(by["g_mis"], "ICICI_AMAZON_PAY")
        self.assertEqual(by["g_cas"], "CAMS")                 # free-text summary fallback
        cats = {"bank_statement", "transaction_alert", "brokerage_statement"}
        self.assertFalse(cats & set(by.values()))


class TestBankFilter(_DB):
    def test_filter_matches_resolved_bank(self):
        f = self.ts.fetchProcessedStatements
        self.assertEqual(self._ids(f("u1", filters={"bank": "SBI"})), ["g_null"])
        self.assertEqual(self._ids(f("u1", filters={"bank": "hdfc_debit"})), ["g_hdfc"])
        self.assertEqual(self._ids(f("u1", filters={"bank": "Millenia_Credit"})), ["g_cc"])
        self.assertEqual(self._ids(f("u1", filters={"bank": "ICICI_AMAZON_PAY"})), ["g_mis"])

    def test_filter_no_longer_matches_category(self):
        out = self.ts.fetchProcessedStatements("u1", filters={"bank": "bank_statement"})
        self.assertEqual((out["total_count"], out["results"]), (0, []))

    def test_filter_is_uid_scoped_and_paged(self):
        self._row("g_hdfc2", "HDFC <alerts@hdfcbank.net>", "bank_statement", minute=8)
        self.session.commit()
        f = self.ts.fetchProcessedStatements
        p1 = f("u1", page=1, limit=1, filters={"bank": "HDFC_DEBIT"})
        p2 = f("u1", page=2, limit=1, filters={"bank": "HDFC_DEBIT"})
        self.assertEqual((p1["total_count"], p2["total_count"]), (2, 2))
        self.assertEqual(self._ids(p1) + self._ids(p2), ["g_hdfc2", "g_hdfc"])   # desc order kept
        self.assertEqual(self._ids(f("u2", filters={"bank": "SBI"})), ["g_other"])


class TestBounds(_DB):
    def test_limit_and_page_clamped(self):
        out = self.ts.fetchProcessedStatements("u1", page=0, limit=10_000)
        self.assertEqual((out["page"], out["page_size"]), (1, 200))
        out = self.ts.fetchProcessedStatements("u1", page="x", limit="y")
        self.assertEqual((out["page"], out["page_size"]), (1, 100))
        out = self.ts.fetchProcessedStatements("u1", page=1, limit=-5)
        self.assertEqual(out["page_size"], 1)
        self.assertEqual(len(out["results"]), 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
