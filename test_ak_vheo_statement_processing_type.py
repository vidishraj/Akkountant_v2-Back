"""ak-vheo (2026-11-24): /fetchFileDetailsEnhanced rows carry processing_type.

The Statements list offers Retry on failed TEXT (non-PDF) rows (ak-788j
made them revivable). has_pdf only says the PDF file exists on disk (a
failed PDF usually has none), so the FE can't tell text from PDF without
the row's processing_type. Additive field: "pdf" | "text" | None.
Real TransactionService on sqlite. Synthetic data.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

try:
    import flask
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    import models  # noqa: F401
    from models import investmentHistory  # noqa: F401
    from models.processedEmails import ProcessedEmails
    from models.statementPeriods import StatementPeriod
    from services import transactionsService as TS
    _HAVE = True
except Exception:  # pragma: no cover
    _HAVE = False


@unittest.skipUnless(_HAVE, "app deps not installed")
class TestProcessingTypeInStatementRows(unittest.TestCase):

    def setUp(self):
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        StatementPeriod.__table__.create(eng)
        self.s = sessionmaker(bind=eng)()
        self.ctx = flask.Flask("akvheo").app_context()
        self.ctx.push()
        flask.g.db = types.SimpleNamespace(session=self.s)
        self.ts = TS.TransactionService()
        now = dt.datetime.utcnow()
        for i, (gid, ptype, status) in enumerate(
                [("p", "pdf", "failed"), ("t", "text", "failed"), ("legacy", None, "processed")]):
            self.s.add(ProcessedEmails(gmail_id=gid, user_id="u", status=status,
                                       processing_type=ptype, category="bank_statement",
                                       processed_at=now - dt.timedelta(minutes=i)))
        self.s.commit()

    def tearDown(self):
        self.ctx.pop()

    def test_rows_carry_processing_type(self):
        rows = self.ts.fetchProcessedStatements("u")["results"]
        got = {r["fileID"]: r["processing_type"] for r in rows}
        self.assertEqual(got, {"pe_p": "pdf", "pe_t": "text", "pe_legacy": None})

    def test_failed_pdf_without_file_is_still_pdf(self):
        row = next(r for r in self.ts.fetchProcessedStatements("u")["results"] if r["fileID"] == "pe_p")
        self.assertEqual((row["has_pdf"], row["processing_type"], row["status"]),
                         (False, "pdf", "failed"))

    def test_existing_keys_unchanged(self):
        row = self.ts.fetchProcessedStatements("u")["results"][0]
        for k in ("fileID", "fileName", "bank", "uploadDate", "email_date", "statementCount",
                  "category", "status", "items_extracted", "extraction_summary", "summary_text",
                  "sender", "subject", "has_pdf", "user", "period_start", "period_end"):
            self.assertIn(k, row)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
