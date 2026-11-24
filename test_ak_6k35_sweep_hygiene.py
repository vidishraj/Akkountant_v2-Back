"""ak-6k35 (2026-11-24): retry-sweep hygiene (ak-i3tc review minors).

1. Starvation: a swept id that is never stamped (Gmail can't fetch it,
   or the classifier drops it) kept its old last attempt and occupied the
   front of the oldest-first queue forever. Each pick now records
   extraction_summary["_sweep"] = {misses, last_swept_at}: the row moves
   to the back, a real processing stamp clears it, and after 3 unanswered
   sweeps it's skipped and listed in agedOut. A newer real or manual
   attempt makes older misses not count.
2. A coarse SQL prefilter keeps the ~2.7k legacy March 'pending' rows and
   text failures out of every cycle's query.
3. A per-run temp subdir for downloaded PDFs (concurrent scheduled +
   manual runs used the same file names), removed after the run.
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
    import flask
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    from services import mailProcessorService as MPS
    from services import transactionsService as TS
    from utils import processed_emails_upsert as U
    _HAVE_APP = True
except Exception:  # pragma: no cover
    _HAVE_APP = False

NOW = dt.datetime(2026, 11, 24, 1, 0, 0)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def att(t):
    return {"_attempts": {"reason": "no_rows", "count": 1, "last_at": iso(t)}}


class TestMissRules(unittest.TestCase):

    def test_unanswered_row_moves_back_then_is_skipped(self):
        rows = {"stuck": att(NOW - dt.timedelta(days=10)),
                "other": att(NOW - dt.timedelta(days=5))}
        seen = []
        with mock.patch.object(S, "MAX_RETRIES_PER_RUN", 1):
            for i in range(8):
                now = NOW + dt.timedelta(hours=i)
                picked = S.select_retry_ids(
                    [(g, "failed", "pdf", NOW - dt.timedelta(days=30), summ)
                     for g, summ in rows.items()], now=now)
                seen.extend(picked)
                for g in picked:
                    if g == "stuck":          # never fetched: only the sweep stamp
                        rows[g] = S.mark_swept(rows[g], now)
                    else:                     # processed and failed again: a real attempt
                        rows[g] = att(now)
        self.assertEqual(seen[:2], ["stuck", "other"])      # no longer hogs the front
        self.assertEqual(seen.count("stuck"), S.MAX_SWEEP_MISSES)
        self.assertEqual(seen[-1], "other")
        rows_list = [(g, "failed", "pdf", NOW - dt.timedelta(days=30), summ) for g, summ in rows.items()]
        self.assertEqual(S.aged_out_ids(rows_list, now=NOW + dt.timedelta(hours=8)), ["stuck"])

    def test_newer_attempt_cancels_misses(self):
        summ = S.mark_swept(S.mark_swept(S.mark_swept(att(NOW - dt.timedelta(days=3)),
                                                      NOW - dt.timedelta(days=2)),
                                         NOW - dt.timedelta(days=2)), NOW - dt.timedelta(days=2))
        self.assertEqual(summ["_sweep"]["misses"], 3)
        row = ("g", "failed", "pdf", NOW - dt.timedelta(days=30), summ)
        self.assertEqual(S.select_retry_ids([row], now=NOW), [])
        summ["_attempts"]["last_at"] = iso(NOW - dt.timedelta(days=1))   # e.g. a manual retry
        self.assertEqual(S.select_retry_ids([row], now=NOW), ["g"])
        self.assertEqual(S.aged_out_ids([row], now=NOW), [])

    def test_mark_swept_counts_and_restarts(self):
        s1 = S.mark_swept(None, NOW)
        self.assertEqual(s1["_sweep"], {"misses": 1, "last_swept_at": iso(NOW)})
        s2 = S.mark_swept(s1, NOW + dt.timedelta(hours=1))
        self.assertEqual(s2["_sweep"]["misses"], 2)
        s3 = dict(s2, **att(NOW + dt.timedelta(hours=2)))                # answered
        self.assertEqual(S.mark_swept(s3, NOW + dt.timedelta(hours=3))["_sweep"]["misses"], 1)


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
        self.app = flask.Flask("ak6k35")
        self.ctx = self.app.app_context()
        self.ctx.push()
        self.utc = dt.datetime.utcnow()

    def tearDown(self):
        self.ctx.pop()

    def _row(self, gid, status="failed", ptype="pdf", first_days=10, summary=None):
        self.session.add(self.PE(gmail_id=gid, user_id="u", status=status, processing_type=ptype,
                                 category="bank_statement",
                                 processed_at=self.utc - dt.timedelta(days=first_days),
                                 extraction_summary=summary))
        self.session.commit()

    def _get(self, gid):
        self.session.expire_all()
        return self.session.query(self.PE).filter_by(gmail_id=gid).one()


class TestSweepInService(_DB):

    def _cycle(self, fetchable=()):
        svc = self.svc
        gmail = mock.MagicMock()
        gmail.findAllEmailsInInterval.return_value = iter([])
        gmail.iter_emails_by_ids.side_effect = lambda u, t, ids: iter(
            [{"gmail_id": i, "sender": "x@hdfcbank.net"} for i in ids if i in fetchable])
        svc.transaction_service.gmailService = gmail
        svc.transaction_service.fetchGmailTokenForUser.return_value = {"token": "t"}
        with mock.patch.object(svc, "_classify_emails", return_value=[]):
            svc.process_emails("u", "2026/11/23", "2026/11/24")
        return gmail.iter_emails_by_ids.call_args[0][2] if gmail.iter_emails_by_ids.called else []

    def test_unfetchable_id_stops_after_three_and_is_listed(self):
        self._row("gone")
        picks = [self._cycle() for _ in range(S.MAX_SWEEP_MISSES + 1)]
        self.assertEqual(picks, [["gone"]] * S.MAX_SWEEP_MISSES + [[]])
        self.assertEqual(self._get("gone").extraction_summary["_sweep"]["misses"], S.MAX_SWEEP_MISSES)
        flask.g.db = types.SimpleNamespace(session=self.session)
        aged = TS.TransactionService().fetchStatementAttention("u")["agedOut"]
        self.assertEqual([i["gmail_id"] for i in aged["items"]], ["gone"])

    def test_real_stamp_clears_the_sweep_marker(self):
        self._row("g1")
        self._cycle()
        self.assertIn("_sweep", self._get("g1").extraction_summary)
        U.upsert_processed_email(self.session, self.PE, gmail_id="g1", user_id="u",
                                 status="error",
                                 extraction_summary={"_attempts": {"reason": "no_rows", "count": 2,
                                                                    "last_at": iso(self.utc)}})
        self.assertNotIn("_sweep", self._get("g1").extraction_summary)

    def test_sql_prefilter_excludes_legacy_pending_and_text(self):
        self._row("march", status="pending", first_days=260)
        self._row("text", status="failed", ptype="text")
        self._row("pdf_old", status="failed", first_days=300)     # revivable → still queried
        self._row("pend_new", status="pending", first_days=1)
        got = {r.gmail_id for r in self.session.query(self.PE).filter(
            self.PE.user_id == "u", S.sql_prefilter(self.PE)).all()}
        self.assertEqual(got, {"pdf_old", "pend_new"})


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestPerRunTmp(_DB):

    def test_each_run_uses_and_removes_its_own_subdir(self):
        svc = self.svc
        seen = []
        cwd = tempfile.mkdtemp(dir=os.environ.get("TMPDIR"))

        def separate(emails, user_id, token):
            d = svc._tmp_dir()
            os.makedirs(d, exist_ok=True)
            open(os.path.join(d, "smart_statement_g1.pdf"), "wb").close()
            seen.append(d)
            return [], []
        gmail = mock.MagicMock()
        gmail.findAllEmailsInInterval.side_effect = lambda *a: iter([{"gmail_id": "g1"}])
        svc.transaction_service.gmailService = gmail
        svc.transaction_service.fetchGmailTokenForUser.return_value = {"token": "t"}
        with mock.patch.object(MPS.os, "getcwd", return_value=cwd), \
             mock.patch.object(svc, "_retry_sweep_ids", return_value=[]), \
             mock.patch.object(svc, "_classify_emails", side_effect=lambda e, user_id=None: list(e)), \
             mock.patch.object(svc, "_save_emails_to_disk"), \
             mock.patch.object(svc, "_separate_emails", side_effect=separate):
            svc.process_emails("u", "a", "b")
            svc.process_emails("u", "a", "b")
            outside = svc._tmp_dir()
        self.assertEqual(len(set(seen)), 2)                       # distinct per run
        for d in seen:
            self.assertTrue(os.path.basename(d).startswith("run_"))
            self.assertEqual(os.path.dirname(d), os.path.join(cwd, "tmp"))
            self.assertFalse(os.path.exists(d))                   # removed after the run
        self.assertEqual(outside, os.path.join(cwd, "tmp"))       # reprocess paths: shared tmp

    def test_download_sites_use_the_run_dir(self):
        src = open(os.path.join(HERE, "services", "mailProcessorService.py")).read()
        self.assertEqual(src.count("tmp_dir = self._tmp_dir()"), 2)
        self.assertNotIn('tmp_dir = os.path.join(os.getcwd(), "tmp")', src)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
