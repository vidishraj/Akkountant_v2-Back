"""ak-8gfj (2026-11-24): the statement Retry button revives aged-out rows.

POST /retryStatementEmail now also accepts a failed PDF row that aged out
of the ak-i3tc sweep window (GET /statementAttention "agedOut"). Any
manual requeue (needs_attention retry, aged-out retry, the unlock_failed
requeue on a password change) stamps a FRESH counter (count 0, last
attempt = now) instead of clearing it. Clearing fell back to
processed_at, so a row first stamped > 45 days ago stayed aged out after
a "successful" retry and was never swept (latent ak-7ahz/i3tc gap,
fixed here). Real TransactionService + sweep on sqlite. Synthetic data.
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
    _HAVE_APP = True
except Exception:  # pragma: no cover
    _HAVE_APP = False

NOW = dt.datetime(2026, 11, 24, 1, 0, 0)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


class TestFreshAttempts(unittest.TestCase):

    def test_shape_and_reason_sources(self):
        prev = {"_attempts": {"reason": "no_rows", "count": 3, "last_at": "x"}}
        self.assertEqual(R.fresh_attempts(prev, now=NOW),
                         {"reason": "no_rows", "count": 0, "last_at": iso(NOW)})
        self.assertEqual(R.fresh_attempts(None, "unlock_failed: password needed", now=NOW)["reason"],
                         "unlock_failed")
        self.assertEqual(R.fresh_attempts(None, None, now=NOW)["reason"], "manual_retry")

    def test_next_identical_failure_counts_one(self):
        summary = R.with_attempts({}, R.fresh_attempts(
            {"_attempts": {"reason": "no_rows", "count": 3, "last_at": "x"}}, now=NOW))
        status, a = R.next_stamp("error", "no_rows", summary, now=NOW)
        self.assertEqual((status, a["count"]), ("error", 1))     # a new 3-strike budget


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestRetryAgedOut(unittest.TestCase):

    def setUp(self):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.processedEmails import ProcessedEmails
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        self.session = sessionmaker(bind=eng)()
        self.PE = ProcessedEmails
        self.app = flask.Flask("ak8gfj")
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

    def _row(self, gid, status, ptype="pdf", first_days=100, last_days=None, msg="no_rows: x"):
        summary = None
        if last_days is not None:
            summary = {"_attempts": {"reason": "no_rows", "count": 2,
                                     "last_at": iso(self.utc - dt.timedelta(days=last_days))}}
        self.session.add(self.PE(gmail_id=gid, user_id="u", status=status, processing_type=ptype,
                                 category="bank_statement",
                                 processed_at=self.utc - dt.timedelta(days=first_days),
                                 extraction_summary=summary, error_message=msg))
        self.session.commit()

    def _get(self, gid):
        self.session.expire_all()
        return self.session.query(self.PE).filter_by(gmail_id=gid).one()

    def test_aged_out_failed_row_is_revived_and_swept(self):
        self._row("aged", "failed", last_days=60)
        self.assertEqual(self.ts.fetchStatementAttention("u")["agedOut"]["count"], 1)
        self.assertEqual(self.svc._retry_sweep_ids("u"), [])
        body, code = self.ts.retryStatementEmail("u", "aged")
        self.assertEqual((code, body["status"], body["queued"]), (200, "failed", True))
        a = self._get("aged").extraction_summary["_attempts"]
        self.assertEqual((a["reason"], a["count"]), ("no_rows", 0))
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["aged"])          # back in the sweep
        self.assertEqual(self.ts.fetchStatementAttention("u")["agedOut"]["count"], 0)

    def test_old_needs_attention_retry_is_not_left_aged_out(self):
        # The latent gap: first stamped 100 days ago; clearing the counter
        # used to leave it outside the window after a "successful" retry.
        self._row("na", R.NEEDS_ATTENTION, last_days=1)
        self.assertEqual(self.ts.retryStatementEmail("u", "na")[1], 200)
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["na"])

    def test_unlock_requeue_of_old_row_is_swept(self):
        self._row("lock", R.NEEDS_ATTENTION, last_days=1, msg="unlock_failed: password needed")
        row = self._get("lock")
        row.extraction_summary = {"_attempts": {"reason": "unlock_failed", "count": 3,
                                                "last_at": iso(self.utc - dt.timedelta(days=1))}}
        self.session.commit()
        self.assertEqual(self.ts.requeueUnlockFailed("u"), 1)
        self.assertEqual(self.svc._retry_sweep_ids("u"), ["lock"])
        self.assertEqual(self._get("lock").extraction_summary["_attempts"]["reason"], "unlock_failed")

    def test_guards(self):
        self._row("queued", "failed", first_days=2)           # inside the window
        self._row("done", "processed", first_days=100)
        body, code = self.ts.retryStatementEmail("u", "queued")
        self.assertEqual((code, body["error_code"]), (409, "ALREADY_QUEUED"))
        body, code = self.ts.retryStatementEmail("u", "done")
        self.assertEqual((code, body["error_code"]), (409, "NOT_NEEDS_ATTENTION"))
        self.assertEqual(self._get("done").status, "processed")          # untouched
        # ak-788j: failed TEXT rows are now revivable (test_ak_788j_text_retry.py).

    def test_aged_out_without_counter_uses_error_reason(self):
        self._row("legacy", "failed", first_days=90, msg="ingest_incomplete: chunks failed")
        self.assertEqual(self.ts.retryStatementEmail("u", "legacy")[1], 200)
        self.assertEqual(self._get("legacy").extraction_summary["_attempts"]["reason"],
                         "ingest_incomplete")


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
