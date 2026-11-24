"""ak-i3tc (2026-11-24): retry sweep for emails that fell out of the 24h
fetch window, plus ak-7ahz's four review minors.

Prod (infra): the scheduled mail task fetches yesterday→today only, so a
failed (or legacy 'pending') email was never fetched again once it aged
out. 2,694 'pending' rows from the March backfill (53 hdfcbank.net
statements, 28 with zero transactions) are stranded that way. No code
writes 'pending' now.

Under test: each cycle re-fetches by id the failed PDF / stale-pending
rows from the last 45 days (cap 20, newest first, never needs_attention);
it composes with ak-7ahz so "3 identical failures → needs_attention" is
reachable across nights. ak-7ahz minors: the upsert preserves the retry
counter; a needs_attention summary_text; 401 without a firebase id; an
unlock_failed requeue on a password / personal-info change.
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
    from utils import processed_emails_upsert as U
    from utils import statement_retry_cap as R
    _HAVE_APP = True
except Exception:  # pragma: no cover
    _HAVE_APP = False

NOW = dt.datetime(2026, 11, 24, 1, 0, 0)
HDFC = "HDFC Bank <alerts@hdfcbank.net>"


def ago(**kw):
    return NOW - dt.timedelta(**kw)


class TestSelection(unittest.TestCase):

    def test_rule(self):
        rows = [
            ("f_pdf", "failed", "pdf", ago(days=2)),            # yes
            ("f_text", "failed", "text", ago(days=2)),          # text failures not swept
            ("p_stale", "pending", "text", ago(hours=7)),       # yes (stale pending)
            ("p_fresh", "pending", "pdf", ago(hours=1)),        # in flight
            ("old", "failed", "pdf", ago(days=46)),             # outside the window
            ("march", "pending", "pdf", ago(days=260)),         # the March backlog
            ("na", "needs_attention", "pdf", ago(days=1)),      # terminal
            ("done", "processed", "pdf", ago(days=1)),
            ("skip", "skipped", "text", ago(days=1)),
            ("bad_at", "failed", "pdf", None),
            (None, "failed", "pdf", ago(days=1)),
        ]
        self.assertEqual(S.select_retry_ids(rows, now=NOW), ["p_stale", "f_pdf"])
        self.assertEqual(S.select_retry_ids(rows, now=NOW, exclude={"f_pdf"}), ["p_stale"])

    def test_cap_newest_first(self):
        rows = [(f"g{i}", "failed", "pdf", ago(hours=i + 1)) for i in range(50)]
        out = S.select_retry_ids(rows, now=NOW)
        self.assertEqual(len(out), S.MAX_RETRIES_PER_RUN)
        self.assertLessEqual(S.MAX_RETRIES_PER_RUN, 20)
        self.assertEqual(out[0], "g0")
        self.assertEqual(out[-1], f"g{S.MAX_RETRIES_PER_RUN - 1}")


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class _DB(unittest.TestCase):

    def setUp(self):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.processedEmails import ProcessedEmails
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        self.eng = eng
        self.session = sessionmaker(bind=eng)()
        self.PE = ProcessedEmails
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        for a in ("transaction_service", "investment_service",
                  "invoice_service", "reconciliation_service"):
            setattr(svc, a, mock.MagicMock())
        svc.transaction_service.db.session = self.session
        self.svc = svc
        self.app = flask.Flask("aki3tc")
        self.ctx = self.app.app_context()
        self.ctx.push()

    def tearDown(self):
        self.ctx.pop()

    def _row(self, gid, status, ptype="pdf", at=None, summary=None):
        self.session.add(self.PE(gmail_id=gid, user_id="u", status=status,
                                 processing_type=ptype, category="bank_statement",
                                 processed_at=at or dt.datetime.utcnow(),
                                 extraction_summary=summary))
        self.session.commit()


class TestWindowDropOut(_DB):
    """The 24h window no longer sees the email; the sweep still does."""

    def _run_cycle(self, window_emails=()):
        svc = self.svc
        gmail = mock.MagicMock()
        gmail.findAllEmailsInInterval.return_value = iter(list(window_emails))
        gmail.iter_emails_by_ids.side_effect = lambda u, t, ids: iter(
            [{"gmail_id": i, "sender": HDFC, "subject": "s"} for i in ids])
        svc.transaction_service.gmailService = gmail
        svc.transaction_service.fetchGmailTokenForUser.return_value = {"token": "t"}
        seen = []

        def classify(emails, user_id=None):
            seen.extend(e["gmail_id"] for e in emails)
            return []
        with mock.patch.object(svc, "_classify_emails", side_effect=classify):
            summary = svc.process_emails("u", "2026/11/23", "2026/11/24")
        return summary, seen, gmail

    def test_failed_statement_outside_window_is_refetched_and_processed(self):
        self._row("g_old", "failed", at=dt.datetime.utcnow() - dt.timedelta(days=2))
        summary, seen, gmail = self._run_cycle(window_emails=[])
        gmail.iter_emails_by_ids.assert_called_once_with("u", {"token": "t"}, ["g_old"])
        self.assertEqual(seen, ["g_old"])             # reached classification
        self.assertEqual(summary["retry_sweep"], 1)

    def test_not_swept(self):
        utc = dt.datetime.utcnow()
        self._row("na", "needs_attention", at=utc - dt.timedelta(days=1))
        self._row("done", "processed", at=utc - dt.timedelta(days=1))
        self._row("march", "pending", at=utc - dt.timedelta(days=260))
        self._row("fresh", "pending", at=utc - dt.timedelta(hours=1))
        summary, seen, gmail = self._run_cycle()
        gmail.iter_emails_by_ids.assert_not_called()
        self.assertNotIn("retry_sweep", summary)

    def test_in_window_email_is_not_fetched_twice(self):
        self._row("g1", "failed", at=dt.datetime.utcnow() - dt.timedelta(hours=8))
        summary, seen, gmail = self._run_cycle(
            window_emails=[{"gmail_id": "g1", "sender": HDFC, "subject": "s"}])
        gmail.iter_emails_by_ids.assert_not_called()
        self.assertEqual(seen, ["g1"])

    def test_sweep_lookup_failure_skips_sweep_only(self):
        with mock.patch.object(self.svc.transaction_service.db.session, "query",
                               side_effect=RuntimeError("db")):
            self.assertEqual(self.svc._retry_sweep_ids("u"), [])


class TestComposesWithRetryCap(_DB):
    """Across nights: sweep → fail → sweep → fail → sweep → needs_attention
    → no longer swept. Real _process_single_pdf_email + handler."""

    def test_three_nights_to_terminal(self):
        svc = self.svc

        def exec_(tn, args, user_id, **kw):
            if tn == "insert_batch_transactions":
                return {"result": "no_transactions", "inserted": 0}
            return {"result": "ok"}

        async def chunk(pdf_path, email, user_id, password, ps, pe, *a, **kw):
            ic = {"value": False}
            h = {t.name: t.handler for t in svc._build_sdk_tools(
                user_id, insert_called=ic, tool_names=MPT.PDF_TOOL_NAMES)}
            await h["insert_batch_transactions"]({"transactions": [], "bank": "HDFC_DEBIT",
                                                  "source": "Statement", "gmail_message_id": "g1"})
            return svc._chunk_insert_result(ic, ps, pe, ledger=True)

        tmp = tempfile.mkdtemp()

        def night():
            path = os.path.join(tmp, f"{len(os.listdir(tmp))}.pdf")
            d = fitz.open()
            d.new_page()
            d.save(path)
            d.close()
            email = {"gmail_id": "g1", "sender": HDFC, "subject": "stmt",
                     "_category": "bank_statement", "_local_pdf_paths": [path]}
            with mock.patch.object(MPS, "execute_mail_tool", side_effect=exec_), \
                 mock.patch.object(svc, "_run_pdf_chunk_async", side_effect=chunk), \
                 mock.patch.object(svc, "_pdf_has_usable_text", return_value=False), \
                 mock.patch.object(svc, "_get_statement_password", return_value=None), \
                 mock.patch.object(svc, "_persist_pdf", return_value=None), \
                 mock.patch.object(svc, "_post_chunk_reconciliation"):
                svc._process_single_pdf_email(email, "u", "image")
            self.session.expire_all()
            return self.session.query(self.PE).filter_by(gmail_id="g1").one().status

        statuses = [night()]                      # night 1: inside the 24h window
        for _ in range(2):                        # nights 2-3: only the sweep finds it
            self.assertEqual(svc._retry_sweep_ids("u"), ["g1"])
            statuses.append(night())
        self.assertEqual(statuses, ["failed", "failed", R.NEEDS_ATTENTION])
        self.assertEqual(svc._retry_sweep_ids("u"), [])   # night 4: terminal, not swept


class TestRetryCapMinors(_DB):

    def _ts(self):
        flask.g.db = types.SimpleNamespace(session=self.session)
        return TS.TransactionService()

    # minor 1
    def test_upsert_preserves_counter_unless_success(self):
        att = {"reason": "no_rows", "count": 2, "last_at": "x"}
        up = lambda **kw: U.upsert_processed_email(self.session, self.PE, gmail_id="g1",
                                                   user_id="u", **kw)
        up(status="error", extraction_summary={"_attempts": att, "a": 1})
        up(status="skipped", extraction_summary=None)            # e.g. a not_financial stamp
        row = self.session.query(self.PE).filter_by(gmail_id="g1").one()
        self.assertEqual(row.extraction_summary, {"_attempts": att})
        up(status="error", extraction_summary={"b": 2})          # report_result metadata only
        self.session.expire_all()
        self.assertEqual(self.session.query(self.PE).one().extraction_summary,
                         {"b": 2, "_attempts": att})
        new = dict(att, count=3)
        up(status="needs_attention", extraction_summary={"_attempts": new})
        self.session.expire_all()
        self.assertEqual(self.session.query(self.PE).one().extraction_summary["_attempts"], new)
        up(status="success", extraction_summary={"c": 3})
        self.session.expire_all()
        self.assertEqual(self.session.query(self.PE).one().extraction_summary, {"c": 3})

    # minor 2
    def test_statements_view_flags_needs_attention(self):
        from models.statementPeriods import StatementPeriod
        StatementPeriod.__table__.create(self.eng)
        self._row("g1", "needs_attention",
                  summary={"_attempts": {"reason": "unlock_failed", "count": 3, "last_at": "x"}})
        row = self.session.query(self.PE).one()
        row.error_message = "unlock_failed: password needed"
        row.items_extracted = 4
        self.session.commit()
        out = self._ts().fetchProcessedStatements("u")
        items = out["data"] if isinstance(out, dict) and "data" in out else out
        item = next(i for i in (items if isinstance(items, list) else items.get("results", []))
                    if i.get("status") == "needs_attention")
        self.assertEqual(item["summary_text"], "Needs attention: unlock_failed: password needed")

    # minor 4
    def test_requeue_only_unlock_failed(self):
        for gid, reason in (("u1", "unlock_failed"), ("n1", "no_rows")):
            self._row(gid, "needs_attention",
                      summary={"_attempts": {"reason": reason, "count": 3, "last_at": "x"}, "k": 1})
        self.assertEqual(self._ts().requeueUnlockFailed("u"), 1)
        self.session.expire_all()
        by = {r.gmail_id: r for r in self.session.query(self.PE).all()}
        self.assertEqual((by["u1"].status, by["u1"].extraction_summary), ("failed", {"k": 1}))
        self.assertEqual(by["n1"].status, "needs_attention")

    def test_requeue_never_raises(self):
        ts = self._ts()
        with mock.patch.object(self.session, "query", side_effect=RuntimeError("db")):
            self.assertEqual(ts.requeueUnlockFailed("u"), 0)


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestControllers(unittest.TestCase):

    def setUp(self):
        from controllers import transactionsEP as EP
        self.EP = EP
        c = EP.TransactionController.__new__(EP.TransactionController)
        c.TransactionService = mock.MagicMock()
        c.TransactionService.setOptedBanks.return_value = object()
        c.TransactionService.retryStatementEmail.return_value = ({"ok": 1}, 200)
        c.TransactionService.fetchStatementAttention.return_value = {"count": 0}
        c.logger = mock.MagicMock()
        self.c = c
        self.app = flask.Flask("aki3tc-ep")

    def _call(self, view, uid, **kw):
        with self.app.test_request_context("/x", **kw):
            if uid:
                flask.g.firebase_id = uid
            resp = getattr(self.c, view)()
        body, code = resp if isinstance(resp, tuple) else (resp, resp.status_code)
        return code

    # minor 3
    def test_401_without_firebase_id(self):
        self.assertEqual(self._call("statementAttention", None), 401)
        self.assertEqual(self._call("retryStatementEmail", None, method="POST",
                                    json={"gmail_id": "g1"}), 401)
        self.c.TransactionService.retryStatementEmail.assert_not_called()
        self.assertEqual(self._call("statementAttention", "u"), 200)
        self.assertEqual(self._call("retryStatementEmail", "u", method="POST",
                                    json={"gmail_id": "g1"}), 200)

    # minor 4: wiring
    def test_password_change_requeues(self):
        self.assertEqual(self._call("setOptedBanks", "u", method="POST",
                                    json={"banks": {"BOI": "pw"}}), 200)
        self.c.TransactionService.requeueUnlockFailed.assert_called_once_with("u")

    def test_personal_info_change_requeues(self):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.userPersonalInfo import UserPersonalInfo
        eng = sa.create_engine("sqlite://")
        UserPersonalInfo.__table__.create(eng)
        session = sessionmaker(bind=eng)()
        with self.app.test_request_context("/x", method="POST", json={"first_name": "A"}):
            flask.g.firebase_id = "u"
            flask.g.db = types.SimpleNamespace(session=session)
            resp = self.c.updatePersonalInfo()
        code = resp[1] if isinstance(resp, tuple) else resp.status_code
        self.assertEqual(code, 200)
        self.c.TransactionService.requeueUnlockFailed.assert_called_once_with("u")


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
