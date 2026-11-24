"""ak-788j (2026-11-24): manual retry of a failed TEXT (non-PDF) statement email.

Failed text rows are not swept automatically (ak-i3tc sweeps failed PDF
rows only), and POST /retryStatementEmail used to answer 409
NOT_NEEDS_ATTENTION for them, so once the 24h fetch window passed they
were never retried. Now a click flags the row (retry_sweep.MANUAL_KEY);
the sweep picks a flagged failed text row like a failed PDF; the next
real processing stamp replaces the summary and drops the flag, so each
click buys exactly one retry. Real TransactionService + sweep on sqlite.
Synthetic data.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import retry_sweep as S  # noqa: E402
from utils import statement_retry_cap as R  # noqa: E402

try:
    import flask
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    from services import mailProcessorService as MPS
    from services import transactionsService as TS
    from utils.processed_emails_upsert import upsert_processed_email
    _HAVE_APP = True
except Exception:  # pragma: no cover
    _HAVE_APP = False

NOW = dt.datetime(2026, 11, 24, 1, 0, 0)


class TestSelection(unittest.TestCase):

    def _rows(self, summary):
        return [("t", "failed", "text", NOW - dt.timedelta(days=2), summary)]

    def test_unflagged_failed_text_is_not_swept(self):
        self.assertEqual(S.select_retry_ids(self._rows(None), now=NOW), [])
        self.assertEqual(S.aged_out_ids(self._rows(None), now=NOW), [])

    def test_flagged_failed_text_is_swept(self):
        self.assertEqual(S.select_retry_ids(self._rows(S.with_manual_retry(None)), now=NOW), ["t"])

    def test_flag_must_be_true(self):
        self.assertFalse(S.manual_retry_requested({S.MANUAL_KEY: "yes"}))
        self.assertFalse(S.manual_retry_requested(None))
        self.assertTrue(S.manual_retry_requested(S.with_manual_retry({"a": 1})))

    def test_flagged_row_given_up_after_misses_is_aged_out(self):
        summary = S.with_manual_retry(None)
        for i in range(S.MAX_SWEEP_MISSES):
            summary = S.mark_swept(summary, now=NOW + dt.timedelta(minutes=i))
        rows = [("t", "failed", "text", NOW - dt.timedelta(days=2), summary)]
        later = NOW + dt.timedelta(hours=1)
        self.assertEqual(S.select_retry_ids(rows, now=later), [])
        self.assertEqual(S.aged_out_ids(rows, now=later), ["t"])


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestManualTextRetry(unittest.TestCase):

    def setUp(self):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.processedEmails import ProcessedEmails
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        self.session = sessionmaker(bind=eng)()
        self.PE = ProcessedEmails
        self.app = flask.Flask("ak788j")
        self.ctx = self.app.app_context()
        self.ctx.push()
        flask.g.db = types.SimpleNamespace(session=self.session)
        self.ts = TS.TransactionService()
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        svc.transaction_service = mock.MagicMock()
        svc.transaction_service.db.session = self.session
        self.svc = svc
        self.utc = dt.datetime.utcnow()

    def tearDown(self):
        self.ctx.pop()

    def _row(self, gid, status="failed", ptype="text", first_days=10, summary=None):
        self.session.add(self.PE(gmail_id=gid, user_id="u", status=status, processing_type=ptype,
                                 category="bank_statement",
                                 processed_at=self.utc - dt.timedelta(days=first_days),
                                 extraction_summary=summary, error_message="parse failed"))
        self.session.commit()

    def _get(self, gid):
        self.session.expire_all()
        return self.session.query(self.PE).filter_by(gmail_id=gid).one()

    def _stamp_failed_again(self, gid):
        # What the text path's report_result does on another failure.
        upsert_processed_email(self.session, self.PE, gmail_id=gid, user_id="u",
                               processing_type="text", status="error",
                               extraction_summary={"note": "still failing"},
                               error_message="parse failed")

    def test_unflagged_text_failure_is_not_swept(self):
        self._row("t")
        self.assertEqual(self.svc._retry_sweep_ids("u"), [])

    def test_click_queues_exactly_one_retry(self):
        self._row("t")
        body, code = self.ts.retryStatementEmail("u", "t")
        self.assertEqual((code, body["status"], body["queued"]), (200, "failed", True))
        self.assertTrue(S.manual_retry_requested(self._get("t").extraction_summary))
        # Second click while it's waiting: already queued.
        body, code = self.ts.retryStatementEmail("u", "t")
        self.assertEqual((code, body["error_code"]), (409, "ALREADY_QUEUED"))
        # The sweep (real SQL prefilter on sqlite) picks it.
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["t"])
        # It fails again: the real stamp drops the flag, so no more sweeps.
        self._stamp_failed_again("t")
        row = self._get("t")
        self.assertEqual(row.status, "failed")
        self.assertFalse(S.manual_retry_requested(row.extraction_summary))
        self.assertEqual(self.svc._retry_sweep_ids("u"), [])
        # A new click buys another retry.
        self.assertEqual(self.ts.retryStatementEmail("u", "t")[1], 200)
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["t"])

    def test_success_clears_flag(self):
        self._row("t")
        self.ts.retryStatementEmail("u", "t")
        upsert_processed_email(self.session, self.PE, gmail_id="t", user_id="u",
                               processing_type="text", status="success",
                               extraction_summary={"ok": 1})
        row = self._get("t")
        self.assertEqual((row.status, row.extraction_summary), ("processed", {"ok": 1}))
        self.assertEqual(self.svc._retry_sweep_ids("u"), [])

    def test_unanswered_sweeps_surface_in_aged_out_and_can_be_retried(self):
        self._row("t")
        self.ts.retryStatementEmail("u", "t")
        for _ in range(S.MAX_SWEEP_MISSES):
            self.assertEqual(self.svc._retry_sweep_ids("u"), ["t"])   # never stamped
        self.assertEqual(self.svc._retry_sweep_ids("u"), [])
        self.assertEqual(self.ts.fetchStatementAttention("u")["agedOut"]["count"], 1)
        self.assertEqual(self.ts.retryStatementEmail("u", "t")[1], 200)  # given up → revivable
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["t"])

    def test_flagged_row_past_the_window_is_reclickable_not_a_permanent_409(self):
        # Review MAJOR probe: flagged + misses=3 + last attempt 50 days ago.
        old = self.utc - dt.timedelta(days=50)
        summary = S.with_manual_retry({"_attempts": {"reason": "manual_retry", "count": 0,
                                                     "last_at": old.strftime("%Y-%m-%dT%H:%M:%SZ")}})
        summary["_sweep"] = {"misses": 3, "last_swept_at": old.strftime("%Y-%m-%dT%H:%M:%SZ")}
        self._row("t", first_days=60, summary=summary)
        self.assertEqual(self.svc._retry_sweep_ids("u"), [])
        body, code = self.ts.retryStatementEmail("u", "t")
        self.assertEqual(code, 200, body)
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["t"])
        self.assertEqual(self.ts.retryStatementEmail("u", "t")[0]["error_code"], "ALREADY_QUEUED")

    def test_flag_match_is_escaped(self):
        # Review minor 1: '_' must not act as a LIKE wildcard.
        self._row("t", summary={"Xmanual_retry": True})
        self._row("u2", summary={"_manualXretry": True})
        self.session.expire_all()
        hits = self.session.query(self.PE.gmail_id).filter(S.sql_prefilter(self.PE)).all()
        self.assertEqual(hits, [])

    def test_needs_attention_text_row_is_flagged(self):
        self._row("na", status=R.NEEDS_ATTENTION)
        self.assertEqual(self.ts.retryStatementEmail("u", "na")[1], 200)
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["na"])

    def test_pdf_and_processed_unchanged(self):
        self._row("p", ptype="pdf", first_days=2)
        self._row("done", status="processed")
        self.assertEqual(self.ts.retryStatementEmail("u", "p")[0]["error_code"], "ALREADY_QUEUED")
        self.assertFalse(S.manual_retry_requested(self._get("p").extraction_summary))
        self.assertEqual(self.ts.retryStatementEmail("u", "done")[0]["error_code"],
                         "NOT_NEEDS_ATTENTION")


if __name__ == "__main__":
    unittest.main()
