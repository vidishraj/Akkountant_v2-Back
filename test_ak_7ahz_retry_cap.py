"""ak-7ahz (2026-11-24): retry cap for statement emails that keep failing.

After N (=3) consecutive failures with the SAME ak-fc7z reason code a PDF
email is stamped TERMINAL 'needs_attention' and automatic processing
skips it; a different reason restarts the count; success clears it; a
manual POST /retryStatementEmail puts it back to 'failed'. The counter
lives in processedEmails.extraction_summary["_attempts"] (no migration).
GET /statementAttention feeds the app's attention item.
Real _process_single_pdf_email / handler / ProcessedEmails table (sqlite).
Synthetic data only.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import statement_retry_cap as R  # noqa: E402

try:
    import anyio  # noqa: F401
    import fitz
    import flask
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    from services import mailProcessorService as MPS
    from services import mailProcessorTools as MPT
    from services import transactionsService as TS
    from utils.processed_emails_upsert import map_tool_status_to_db
    _HAVE_APP = True
except Exception:  # pragma: no cover
    _HAVE_APP = False

HDFC = "HDFC Bank <alerts@hdfcbank.net>"
ROWS = [{"date": "20/11/2026", "amount": 100, "description": "d1"}]


class TestRule(unittest.TestCase):
    NOW = dt.datetime(2026, 11, 24, 1, 0, 0)

    def _chain(self, reasons):
        summary, out = None, []
        for r in reasons:
            status, attempts = R.next_stamp("error" if r else "success", r, summary, now=self.NOW)
            out.append((status, attempts["count"] if attempts else None))
            summary = R.with_attempts({"agent": "meta"}, attempts)
        return out

    def test_three_identical_failures_go_terminal(self):
        self.assertEqual(self._chain(["no_rows"] * 3),
                         [("error", 1), ("error", 2), (R.NEEDS_ATTENTION, 3)])

    def test_a_different_reason_restarts_the_count(self):
        self.assertEqual(self._chain(["no_rows", "no_rows", "unlock_failed", "unlock_failed"]),
                         [("error", 1), ("error", 2), ("error", 1), ("error", 2)])

    def test_success_clears(self):
        self.assertEqual(self._chain(["no_rows", "no_rows", None, "no_rows"]),
                         [("error", 1), ("error", 2), ("success", None), ("error", 1)])

    def test_summary_merge_and_malformed_counter(self):
        _, a = R.next_stamp("error", "no_rows", None, now=self.NOW)
        merged = R.with_attempts({"funds": 3, "_attempts": "junk"}, a)
        self.assertEqual(merged, {"funds": 3, "_attempts": {
            "reason": "no_rows", "count": 1, "last_at": "2026-11-24T01:00:00Z"}})
        self.assertIsNone(R.with_attempts(None, None))
        self.assertEqual(R.with_attempts({"_attempts": a}, None), None)
        for bad in ({"_attempts": {"reason": "no_rows", "count": True}},
                    {"_attempts": {"reason": 5, "count": 2}}, "x", None):
            self.assertEqual(R.next_stamp("error", "no_rows", bad)[1]["count"], 1, bad)

    def test_terminal_statuses(self):
        self.assertEqual(set(R.TERMINAL_DB_STATUSES), {"processed", "needs_attention"})
        self.assertLessEqual(len(R.NEEDS_ATTENTION), 20)      # fits status String(20)


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestEndToEnd(unittest.TestCase):

    def setUp(self):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.processedEmails import ProcessedEmails
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        self.session = sessionmaker(bind=eng)()
        self.PE = ProcessedEmails
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        for a in ("transaction_service", "investment_service",
                  "invoice_service", "reconciliation_service"):
            setattr(svc, a, mock.MagicMock())
        svc.transaction_service.db.session = self.session
        self.svc = svc

        def exec_(tn, args, user_id, **kw):
            if tn == "insert_batch_transactions":
                rows = args["transactions"]
                return ({"inserted": len(rows), "duplicates": 0} if rows
                        else {"result": "no_transactions", "inserted": 0})
            return {"result": "ok"}
        self._p = mock.patch.object(MPS, "execute_mail_tool", side_effect=exec_)
        self._p.start()
        self.app = flask.Flask("ak7ahz")
        self.ctx = self.app.app_context()
        self.ctx.push()
        self.dir = tempfile.mkdtemp()

    def tearDown(self):
        self.ctx.pop()
        self._p.stop()

    def _pdf(self, password=None):
        path = os.path.join(self.dir, f"s{len(os.listdir(self.dir))}.pdf")
        doc = fitz.open()
        doc.new_page()
        if password:
            doc.save(path, encryption=fitz.PDF_ENCRYPT_AES_256, user_pw=password,
                     owner_pw=password + "o")
        else:
            doc.save(path)
        doc.close()
        return path

    def _cycle(self, rows=None, locked=False):
        """One real mail-cycle pass over the same email."""
        svc = self.svc

        async def fake_chunk(pdf_path, email, user_id, password, ps, pe, *a, **kw):
            ic = {"value": False}
            h = {t.name: t.handler for t in svc._build_sdk_tools(
                user_id, insert_called=ic, tool_names=MPT.PDF_TOOL_NAMES)}
            await h["insert_batch_transactions"]({
                "transactions": [dict(r) for r in (rows or [])], "bank": "HDFC_DEBIT",
                "source": "Statement", "gmail_message_id": "g1"})
            await h["report_result"]({"gmail_message_id": "g1", "status": "success",
                                      "sender": "s", "subject": "x", "processing_type": "pdf",
                                      "extraction_summary": {"agent": "meta"}})
            return svc._chunk_insert_result(ic, ps, pe, ledger=svc._is_ledger_email(email))

        email = {"gmail_id": "g1", "sender": HDFC, "subject": "stmt",
                 "_category": "bank_statement",
                 "_local_pdf_paths": [self._pdf("pw" if locked else None)]}
        with mock.patch.object(svc, "_run_pdf_chunk_async", side_effect=fake_chunk), \
             mock.patch.object(svc, "_pdf_has_usable_text", return_value=False), \
             mock.patch.object(svc, "_get_statement_password", return_value=None), \
             mock.patch.object(svc, "_persist_pdf", return_value=None), \
             mock.patch.object(svc, "_post_chunk_reconciliation"):
            svc._process_single_pdf_email(email, "u", "image")
        self.session.expire_all()
        return self.session.query(self.PE).filter_by(gmail_id="g1").one()

    def test_three_no_rows_cycles_go_terminal_and_are_skipped(self):
        seen = []
        for _ in range(3):
            row = self._cycle(rows=[])
            seen.append((row.status, row.extraction_summary["_attempts"]["count"]))
        self.assertEqual(seen, [("failed", 1), ("failed", 2), ("needs_attention", 3)])
        self.assertTrue(row.error_message.startswith("no_rows"))   # reason kept
        self.assertEqual(row.extraction_summary["agent"], "meta")    # metadata kept
        # Automatic processing no longer picks it up; a failed one is.
        kept = self.svc._filter_already_processed(
            [{"gmail_id": "g1"}, {"gmail_id": "other"}], "u")
        self.assertEqual([e["gmail_id"] for e in kept], ["other"])

    def test_unlock_failures_also_cap(self):
        statuses = [self._cycle(locked=True).status for _ in range(3)]
        self.assertEqual(statuses, ["failed", "failed", "needs_attention"])

    def test_changed_reason_restarts_and_success_clears(self):
        self._cycle(rows=[])
        self._cycle(rows=[])
        row = self._cycle(locked=True)
        self.assertEqual((row.status, row.extraction_summary["_attempts"]),
                         ("failed", {**row.extraction_summary["_attempts"], "reason": "unlock_failed", "count": 1}))
        row = self._cycle(rows=ROWS)
        self.assertEqual(row.status, "processed")
        self.assertNotIn("_attempts", row.extraction_summary or {})

    # ── attention feed + manual retry (real TransactionService) ────────

    def _ts(self):
        flask.g.db = types.SimpleNamespace(session=self.session)
        return TS.TransactionService()

    def test_attention_feed_and_manual_retry(self):
        for _ in range(3):
            self._cycle(rows=[])
        ts = self._ts()
        feed = ts.fetchStatementAttention("u")
        self.assertEqual(feed["count"], 1)
        self.assertEqual(feed["by_reason"], {"no_rows": 1})
        item, = feed["items"]
        self.assertEqual((item["gmail_id"], item["reason"], item["attempts"]), ("g1", "no_rows", 3))
        self.assertNotIn("agent", item)                     # metadata only

        body, code = ts.retryStatementEmail("u", "g1")
        self.assertEqual((code, body["status"]), (200, "failed"))
        self.session.expire_all()
        row = self.session.query(self.PE).filter_by(gmail_id="g1").one()
        self.assertEqual(row.status, "failed")
        # ak-8gfj: a FRESH counter (count 0, last attempt now), not none,
        # so the sweep window includes it again.
        self.assertEqual((row.extraction_summary["_attempts"]["count"],
                          row.extraction_summary["_attempts"]["reason"]), (0, "no_rows"))
        self.assertEqual(ts.fetchStatementAttention("u")["count"], 0)
        # Back in the queue, and the count starts over.
        self.assertEqual(len(self.svc._filter_already_processed([{"gmail_id": "g1"}], "u")), 1)
        self.assertEqual(self._cycle(rows=[]).extraction_summary["_attempts"]["count"], 1)

    def test_retry_guards(self):
        ts = self._ts()
        self.assertEqual(ts.retryStatementEmail("u", "missing")[1], 404)
        self.assertEqual(ts.retryStatementEmail("u", "")[1], 400)
        self.assertEqual(ts.retryStatementEmail("u", None)[1], 400)
        self._cycle(rows=ROWS)                               # processed, not terminal
        body, code = ts.retryStatementEmail("u", "g1")
        self.assertEqual((code, body["error_code"]), (409, "NOT_NEEDS_ATTENTION"))

    def test_status_map_and_error_message_for_needs_attention(self):
        self.assertEqual(map_tool_status_to_db("needs_attention"), "needs_attention")
        for _ in range(3):
            row = self._cycle(locked=True)
        self.assertTrue(row.error_message.startswith("unlock_failed"))

    def test_routes_registered(self):
        src = open(os.path.join(HERE, "app.py")).read()
        self.assertIn("('/statementAttention', 'GET', self.transactionEP.statementAttention)", src)
        self.assertIn("('/retryStatementEmail', 'POST', self.transactionEP.retryStatementEmail)", src)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
