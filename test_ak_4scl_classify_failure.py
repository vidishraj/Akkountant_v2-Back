"""ak-4scl (P1, ak-1m97 follow-up, 2026-11-24): a classifier outage must
be LOUD, not "No financial emails found".

On 2026-11-24 every mail agent raised at construction (ak-1m97), the
classifier swallowed each batch's exception, returned [], and the run
logged "No financial emails found in the date range" while mail aged out
of the 24h window. Now:
  * every batch failing → ClassificationFailed → process_emails records
    it in summary["errors"] (ERROR "Mail processing pipeline error");
  * a partial failure is recorded in summary["errors"] too;
  * checkMailUnifiedTask logs ERROR when a run reports errors;
  * a failed batch still records NOTHING (no processedEmails row), so its
    emails are fetched again next cycle;
  * an outage is not an unanswered email: this cycle's sweep marks
    (ak-6k35 misses) are undone, so swept ids aren't pushed to agedOut.
Synthetic data only.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
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
    _HAVE = True
except Exception:  # pragma: no cover
    _HAVE = False

EMAILS = [{"gmail_id": f"g{i}", "sender": "x@shop.example"} for i in range(3)]
OUTAGE = TypeError("ClaudeAgentOptions.__init__() got an unexpected keyword argument 'tools'")


class TestRestoreSweep(unittest.TestCase):

    def test_restores_previous_marker_exactly(self):
        t1, t2 = dt.datetime(2026, 11, 23, 1), dt.datetime(2026, 11, 24, 1)
        one = S.mark_swept(None, t1)
        two = S.mark_swept(one, t2)
        back = S.restore_sweep(two, one["_sweep"], two["_sweep"])
        self.assertEqual(back["_sweep"], one["_sweep"])          # misses AND last_swept_at
        self.assertNotIn("_sweep", S.restore_sweep(one, None, one["_sweep"]))

    def test_no_op_unless_row_still_carries_this_runs_stamp(self):
        t1, t2 = dt.datetime(2026, 11, 23, 1), dt.datetime(2026, 11, 24, 1)
        earlier = S.mark_swept(None, t1)
        stamped = S.mark_swept(None, t2)["_sweep"]
        self.assertEqual(S.restore_sweep(earlier, None, stamped), earlier)
        self.assertEqual(S.restore_sweep({"x": 1}, None, stamped), {"x": 1})
        self.assertIsNone(S.restore_sweep(None, None, stamped))


@unittest.skipUnless(_HAVE, "app deps not installed")
class TestLoudFailure(unittest.TestCase):

    def _svc(self, session=None):
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        svc.transaction_service = mock.MagicMock()
        if session is not None:
            svc.transaction_service.db.session = session
        svc._stamp_processed_email = mock.MagicMock()
        gmail = mock.MagicMock()
        gmail.findAllEmailsInInterval.return_value = iter([dict(e) for e in EMAILS])
        gmail.iter_emails_by_ids.side_effect = lambda u, t, ids: iter([{"gmail_id": i} for i in ids])
        svc.transaction_service.gmailService = gmail
        svc.transaction_service.fetchGmailTokenForUser.return_value = {"token": "t"}
        return svc

    def test_all_batches_failing_raises(self):
        svc = self._svc()
        with mock.patch.object(svc, "_run_classification_batch", side_effect=OUTAGE):
            with self.assertRaises(MPS.ClassificationFailed):
                svc._classify_emails([dict(e) for e in EMAILS], user_id="u")
        svc._stamp_processed_email.assert_not_called()      # nothing recorded

    def test_pipeline_reports_error_not_no_financial_emails(self):
        svc = self._svc()
        with mock.patch.object(svc, "_run_classification_batch", side_effect=OUTAGE), \
             mock.patch.object(svc, "_retry_sweep_ids", return_value=[]), \
             mock.patch.object(svc, "_filter_already_processed", side_effect=lambda e, u: e):
            summary = svc.process_emails("u", "a", "b")
        # Review MAJOR 2: pre-ak-ouu2 the outer handler appends str(e),
        # post-ouu2 "Mail processing pipeline error (<type>)": accept both.
        self.assertTrue(any("ClassificationFailed" in e or "classification failed for all" in e
                            for e in summary["errors"]), summary)
        self.assertFalse(any("unexpected keyword" in e for e in summary["errors"]), summary)
        infos = " ".join(str(c) for c in svc.logger.info.call_args_list)
        self.assertNotIn("No financial emails found", infos)
        self.assertTrue(svc.logger.error.called)

    def test_partial_failure_recorded_in_summary(self):
        svc = self._svc()
        calls = {"n": 0}

        def batch(b):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            return []
        with mock.patch.object(MPS, "CLASSIFICATION_BATCH_SIZE", 2), \
             mock.patch.object(svc, "_run_classification_batch", side_effect=batch), \
             mock.patch.object(svc, "_retry_sweep_ids", return_value=[]), \
             mock.patch.object(svc, "_filter_already_processed", side_effect=lambda e, u: e), \
             mock.patch.object(svc, "_save_emails_to_disk"):
            summary = svc.process_emails("u", "a", "b")
        self.assertEqual(len(summary["errors"]), 1)
        self.assertIn("classification failed for 1 batch(es)", summary["errors"][0])
        self.assertIn("RuntimeError", summary["errors"][0])
        self.assertNotIn("transient", summary["errors"][0])        # type-only (minor 1)

    def test_next_cycle_retries_the_unrecorded_emails(self):
        svc = self._svc()
        with mock.patch.object(svc, "_run_classification_batch", side_effect=OUTAGE), \
                self.assertRaises(MPS.ClassificationFailed):
            svc._classify_emails([dict(e) for e in EMAILS], user_id="u")
        with mock.patch.object(svc, "_run_classification_batch",
                               return_value=[{"gmail_id": "g1", "category": "bank_statement"}]):
            out = svc._classify_emails([dict(e) for e in EMAILS], user_id="u")
        self.assertEqual([e["gmail_id"] for e in out], ["g1"])

    def test_outage_does_not_count_as_a_sweep_miss(self):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.processedEmails import ProcessedEmails
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        session = sessionmaker(bind=eng)()
        session.add(ProcessedEmails(gmail_id="swept", user_id="u", status="failed",
                                    processing_type="pdf", category="bank_statement",
                                    processed_at=dt.datetime.utcnow() - dt.timedelta(days=3)))
        session.commit()
        svc = self._svc(session)
        svc.transaction_service.gmailService.findAllEmailsInInterval.return_value = iter([])
        ctx = flask.Flask("ak4scl").app_context()
        ctx.push()
        try:
            for _ in range(S.MAX_SWEEP_MISSES + 1):
                with mock.patch.object(svc, "_run_classification_batch", side_effect=OUTAGE):
                    summary = svc.process_emails("u", "a", "b")
                self.assertTrue(summary["errors"])
        finally:
            ctx.pop()
        session.expire_all()
        row = session.query(ProcessedEmails).filter_by(gmail_id="swept").one()
        self.assertNotIn("_sweep", row.extraction_summary or {})
        self.assertEqual(svc._retry_sweep_ids("u"), ["swept"])     # still swept

    def _sqlite(self, rows):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.processedEmails import ProcessedEmails
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        session = sessionmaker(bind=eng)()
        for gid, days, summary in rows:
            session.add(ProcessedEmails(gmail_id=gid, user_id="u", status="failed",
                                        processing_type="pdf", category="bank_statement",
                                        processed_at=dt.datetime.utcnow() - dt.timedelta(days=days),
                                        extraction_summary=summary))
        session.commit()
        return session, ProcessedEmails

    def _sweep_of(self, session, PE, gid):
        session.expire_all()
        return (session.query(PE).filter_by(gmail_id=gid).one().extraction_summary or {}).get("_sweep")

    def test_failed_mark_means_unmark_is_a_no_op(self):
        # Review MAJOR 1: this cycle's mark fails (swallowed); the outage
        # must NOT undo the earlier cycles' misses.
        prior = S.mark_swept(S.mark_swept(None, dt.datetime.utcnow() - dt.timedelta(hours=2)),
                             dt.datetime.utcnow() - dt.timedelta(hours=1))
        session, PE = self._sqlite([("swept", 3, prior)])
        svc = self._svc(session)
        svc.transaction_service.gmailService.findAllEmailsInInterval.return_value = iter([])
        ctx = flask.Flask("ak4scl").app_context()
        ctx.push()
        try:
            with mock.patch.object(S, "mark_swept", side_effect=RuntimeError("db")), \
                 mock.patch.object(svc, "_run_classification_batch", side_effect=OUTAGE):
                summary = svc.process_emails("u", "a", "b")
        finally:
            ctx.pop()
        self.assertTrue(summary["errors"])
        self.assertEqual(self._sweep_of(session, PE, "swept"), prior["_sweep"])   # misses 2 kept

    def test_partial_outage_unmarks_only_the_failed_batch(self):
        session, PE = self._sqlite([("old", 5, None), ("new", 3, None)])
        svc = self._svc(session)
        svc.transaction_service.gmailService.findAllEmailsInInterval.return_value = iter([])
        calls = {"n": 0}

        def batch(b):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            return []
        ctx = flask.Flask("ak4scl").app_context()
        ctx.push()
        try:
            with mock.patch.object(MPS, "CLASSIFICATION_BATCH_SIZE", 1), \
                 mock.patch.object(svc, "_run_classification_batch", side_effect=batch), \
                 mock.patch.object(svc, "_record_not_financial"), \
                 mock.patch.object(svc, "_save_emails_to_disk"):
                summary = svc.process_emails("u", "a", "b")
        finally:
            ctx.pop()
        self.assertEqual(len(summary["errors"]), 1)
        self.assertIsNone(self._sweep_of(session, PE, "old"))           # failed batch: undone
        self.assertEqual(self._sweep_of(session, PE, "new")["misses"], 1)  # answered batch: kept

    def test_task_logs_error_for_a_failed_run(self):
        src = open(os.path.join(HERE, "services", "tasks", "checkMailUnifiedTask.py")).read()
        self.assertIn('if result.get("errors"):', src)
        self.assertIn("self.logger.error(", src.split('if result.get("errors"):')[1][:200])


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
