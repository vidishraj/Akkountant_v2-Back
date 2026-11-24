"""ak-i3tc review minors (2026-11-24), on top of f20b236:
  1. the sweep picks the OLDEST last attempt first (round-robin), so a
     backlog bigger than the per-cycle cap rotates instead of starving;
  2. the 45-day window keys on the LAST attempt (ak-7ahz
     _attempts.last_at, else processed_at), and failed PDF rows that age
     out are surfaced (WARN + GET /statementAttention "agedOut");
  3. temp PDFs in <cwd>/tmp are removed on every path: processed, failed,
     raised, and link-based PDFs left behind when a run fails before PDF
     processing.
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

from utils import retry_sweep as S  # noqa: E402

try:
    import fitz
    import flask
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    from services import mailProcessorService as MPS
    from services import mailProcessorTools as MPT
    from services import transactionsService as TS
    _HAVE_APP = True
except Exception:  # pragma: no cover
    _HAVE_APP = False

NOW = dt.datetime(2026, 11, 24, 1, 0, 0)


def ago(**kw):
    return NOW - dt.timedelta(**kw)


def attempts(at):
    return {"_attempts": {"reason": "no_rows", "count": 1,
                          "last_at": at.strftime("%Y-%m-%dT%H:%M:%SZ")}}


class TestOrderingAndWindow(unittest.TestCase):

    def test_round_robin_rotates_a_backlog(self):
        rows = {g: ago(days=d) for g, d in (("a", 3), ("b", 2), ("c", 1))}
        with mock.patch.object(S, "MAX_RETRIES_PER_RUN", 2):
            def cycle(now):
                picked = S.select_retry_ids(
                    [(g, "failed", "pdf", ago(days=40), attempts(t)) for g, t in rows.items()],
                    now=now)
                for g in picked:                       # attempted → back of the queue
                    rows[g] = now
                return picked
            self.assertEqual(cycle(NOW), ["a", "b"])
            self.assertEqual(cycle(NOW + dt.timedelta(hours=1)), ["c", "a"])
            # b (tried at NOW) is now oldest; a and c tie at NOW+1h → id order.
            self.assertEqual(cycle(NOW + dt.timedelta(hours=2)), ["b", "a"])

    def test_window_keys_on_last_attempt(self):
        rows = [
            # first stamped 100 days ago, but last tried 2 days ago → swept
            ("first_old_recent_try", "failed", "pdf", ago(days=100), attempts(ago(days=2))),
            # first stamped 50 days ago and last tried then → aged out
            ("first_recent_old_try", "failed", "pdf", ago(days=50), attempts(ago(days=50))),
            ("no_counter", "failed", "pdf", ago(days=3), None),           # processed_at
            ("bad_counter", "failed", "pdf", ago(days=4), {"_attempts": {"last_at": "junk"}}),
        ]
        self.assertEqual(S.select_retry_ids(rows, now=NOW),
                         ["bad_counter", "no_counter", "first_old_recent_try"])
        self.assertEqual(S.aged_out_ids(rows, now=NOW), ["first_recent_old_try"])

    def test_aged_out_lists_failed_pdf_only(self):
        rows = [
            ("aged", "failed", "pdf", ago(days=60), attempts(ago(days=50))),
            ("fresh", "failed", "pdf", ago(days=60), attempts(ago(days=1))),
            ("text", "failed", "text", ago(days=60), None),
            ("march", "pending", "pdf", ago(days=260), None),
        ]
        self.assertEqual(S.aged_out_ids(rows, now=NOW), ["aged"])


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class _DB(unittest.TestCase):

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
        self.app = flask.Flask("aki3tc-fu")
        self.ctx = self.app.app_context()
        self.ctx.push()
        self.tmp = tempfile.mkdtemp(dir=os.environ.get("TMPDIR"))

    def tearDown(self):
        self.ctx.pop()

    def _row(self, gid, status="failed", ptype="pdf", at=None, summary=None):
        self.session.add(self.PE(gmail_id=gid, user_id="u", status=status, processing_type=ptype,
                                 category="bank_statement", subject="stmt",
                                 processed_at=at or dt.datetime.utcnow(),
                                 extraction_summary=summary, error_message="no_rows: x"))
        self.session.commit()


class TestServiceAndFeed(_DB):

    def test_sweep_uses_last_attempt_and_warns_aged_out(self):
        utc = dt.datetime.utcnow()
        self._row("recent_try", at=utc - dt.timedelta(days=100),
                  summary=attempts(utc - dt.timedelta(days=1)))
        self._row("aged", at=utc - dt.timedelta(days=100),
                  summary=attempts(utc - dt.timedelta(days=60)))
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["recent_try"])
        warned = " ".join(str(c) for c in self.svc.logger.warning.call_args_list)
        self.assertIn("1 failed statement email(s) aged out", warned)

    def test_attention_feed_lists_aged_out(self):
        utc = dt.datetime.utcnow()
        self._row("aged", at=utc - dt.timedelta(days=100),
                  summary=attempts(utc - dt.timedelta(days=60)))
        self._row("fresh", at=utc - dt.timedelta(days=2))
        flask.g.db = types.SimpleNamespace(session=self.session)
        feed = TS.TransactionService().fetchStatementAttention("u")
        self.assertEqual(feed["agedOut"]["count"], 1)
        item, = feed["agedOut"]["items"]
        self.assertEqual((item["gmail_id"], item["reason"]), ("aged", "no_rows"))
        self.assertEqual(feed["count"], 0)          # needs_attention list unchanged


class TestTempPdfCleanup(_DB):

    def _pdf(self, name):
        path = os.path.join(self.tmp, name)
        d = fitz.open()
        d.new_page()
        d.save(path)
        d.close()
        return path

    def _process(self, chunk_side_effect):
        svc = self.svc
        path = self._pdf("mail_pipeline_g1_0.pdf")
        email = {"gmail_id": "g1", "sender": "x@hdfcbank.net", "subject": "s",
                 "_category": "bank_statement", "_gmail_api": object()}
        with mock.patch.object(svc, "_download_pdf_attachments", return_value=[(path, "s.pdf")]), \
             mock.patch.object(svc, "_run_pdf_chunk_async", side_effect=chunk_side_effect), \
             mock.patch.object(svc, "_pdf_has_usable_text", return_value=False), \
             mock.patch.object(svc, "_get_statement_password", return_value=None), \
             mock.patch.object(svc, "_persist_pdf", return_value=None), \
             mock.patch.object(svc, "_post_chunk_reconciliation"):
            svc._process_single_pdf_email(email, "u", "image")
        return path

    def test_removed_after_success_failure_and_raise(self):
        svc = self.svc

        async def ok(pdf_path, email, user_id, password, ps, pe, *a, **kw):
            ic = {"value": False}
            h = {t.name: t.handler for t in svc._build_sdk_tools(
                user_id, insert_called=ic, tool_names=MPT.PDF_TOOL_NAMES)}
            with mock.patch.object(MPS, "execute_mail_tool",
                                   return_value={"inserted": 1, "duplicates": 0}):
                await h["insert_batch_transactions"]({
                    "transactions": [{"date": "20/11/2026", "amount": 1, "description": "d"}],
                    "bank": "HDFC_DEBIT", "source": "Statement", "gmail_message_id": "g1"})
            return svc._chunk_insert_result(ic, ps, pe, ledger=True)

        async def fails(*a, **kw):
            return {"called": False}

        async def boom(*a, **kw):
            raise RuntimeError("chunk crashed")

        for side in (ok, fails, boom):
            self.session.query(self.PE).delete()
            path = self._process(side)
            self.assertFalse(os.path.exists(path), side.__name__)

    def test_link_based_pdf_removed_when_run_fails_before_pdf_processing(self):
        svc = self.svc
        leftover = self._pdf("hdfc_smart_g2.pdf")
        pdf_email = {"gmail_id": "g2", "sender": "x@hdfcbank.net", "_local_pdf_paths": [leftover]}
        gmail = mock.MagicMock()
        gmail.findAllEmailsInInterval.return_value = iter([{"gmail_id": "t1"}, {"gmail_id": "g2"}])
        svc.transaction_service.gmailService = gmail
        svc.transaction_service.fetchGmailTokenForUser.return_value = {"token": "t"}
        with mock.patch.object(svc, "_retry_sweep_ids", return_value=[]), \
             mock.patch.object(svc, "_classify_emails", side_effect=lambda e, user_id=None: list(e)), \
             mock.patch.object(svc, "_save_emails_to_disk"), \
             mock.patch.object(svc, "_separate_emails", return_value=([{"gmail_id": "t1"}], [pdf_email])), \
             mock.patch.object(svc, "_process_text_emails_batch", side_effect=RuntimeError("text batch")), \
             mock.patch.object(svc, "_process_pdf_emails") as pdf_step:
            summary = svc.process_emails("u", "2026/11/23", "2026/11/24")
        pdf_step.assert_not_called()
        self.assertTrue(summary["errors"])
        self.assertFalse(os.path.exists(leftover))

    def test_cleanup_helper_tolerates_shapes(self):
        a = self._pdf("a.pdf")
        b = self._pdf("b.pdf")
        n = self.svc._cleanup_local_pdfs([{"_local_pdf_paths": [a, (b, "b.pdf"), "/nonexistent/x.pdf"]},
                                          {}, {"_local_pdf_paths": None}])
        self.assertEqual(n, 2)
        self.assertFalse(os.path.exists(a) or os.path.exists(b))


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
