"""ak-v6zp (2026-11-24): stop re-classifying the same not_financial mail
every cycle, and give those rows an email_date.

Before:
  * ak-iu8i stamps classifier-dropped emails status 'skipped' / category
    'not_financial', but _filter_already_processed only skipped
    'processed' / 'needs_attention', so the same ~35 emails went back to
    the classifier (SDK spend) every hourly cycle.
  * _stamp_processed_email passed email.get("date"), but the Gmail fetch
    dict carries the date as 'time' ('%Y-%m-%d %H:%M:%S'), a format the
    upsert parser didn't accept either, so every service stamp stored a
    NULL email_date.
After: skipped/not_financial is skipped unless a manual scan opts in
(GET /readEmails?reclassify=true); the date is stored and a missing one
is filled on update (never overwriting a known date). The i3tc / 6k35
sweep never selects skipped rows, so it is unaffected.

The service methods are AST-lifted from services/mailProcessorService.py
(the module imports anyio / the SDK) and run against a real
processedEmails table on in-memory SQLite. Synthetic data only.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils.processed_emails_upsert import _parse_email_date, upsert_processed_email  # noqa: E402

try:
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    import models  # noqa: F401
    from models import investmentHistory  # noqa: F401
    from models.processedEmails import ProcessedEmails
    _OK, _SKIP = True, ""
except Exception as _exc:  # pragma: no cover
    _OK, _SKIP = False, f"deps: {_exc}"

SRC = os.path.join(HERE, "services", "mailProcessorService.py")
METHODS = ("_filter_already_processed", "_record_not_financial", "_stamp_processed_email")


def _lift_service():
    with open(SRC) as fh:
        tree = ast.parse(fh.read())
    consts = [n for n in tree.body if isinstance(n, ast.Assign)
              and any(isinstance(t, ast.Name) and t.id == "NOT_FINANCIAL" for t in n.targets)]
    assert consts, "NOT_FINANCIAL constant missing"
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "MailProcessorService")
    fns = [n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in METHODS]
    assert len(fns) == len(METHODS)
    shim = ast.ClassDef(name="Shim", bases=[], keywords=[], body=fns, decorator_list=[])
    mod = ast.Module(body=consts + [shim], type_ignores=[])
    ast.fix_missing_locations(mod)
    # The lifted methods use module-level names of mailProcessorService
    # (ak-q9c8 error_tag on the lookup-failure path).
    from utils.safe_errors import error_frames, error_tag
    ns = {"error_tag": error_tag, "error_frames": error_frames}
    exec(compile(mod, "<lift:v6zp>", "exec"), ns)
    return ns["Shim"], ns["NOT_FINANCIAL"]


def _email(gid, time="2026-11-24 06:30:00", **kw):
    return {"gmail_id": gid, "message_id": gid, "sender": "News <news@shop.example>",
            "subject": "Weekly deals", "time": time, "_has_pdf": False, **kw}


@unittest.skipUnless(_OK, _SKIP)
class _Db(unittest.TestCase):

    def setUp(self):
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        self.session = sessionmaker(bind=eng)()
        Shim, self.NF = _lift_service()
        svc = Shim()
        svc.logger = mock.MagicMock()
        svc.transaction_service = mock.MagicMock()
        svc.transaction_service.db.session = self.session
        self.svc = svc

    def tearDown(self):
        self.session.close()

    def row(self, gid):
        self.session.expire_all()
        return self.session.query(ProcessedEmails).filter_by(gmail_id=gid, user_id="u").first()

    def put(self, gid, status, category="bank_statement", email_date=None, ptype="pdf",
            sender=None, summary=None):
        self.session.add(ProcessedEmails(gmail_id=gid, user_id="u", status=status,
                                         category=category, processing_type=ptype,
                                         email_date=email_date, items_extracted=0,
                                         sender=sender, extraction_summary=summary))
        self.session.commit()

    def kept(self, ids, **kw):
        return [e["gmail_id"] for e in self.svc._filter_already_processed(
            [{"gmail_id": g} for g in ids], "u", **kw)]


class TestFilter(_Db):

    def setUp(self):
        super().setUp()
        self.put("processed", "processed")
        self.put("attention", "needs_attention")
        self.put("nf", "skipped", category="not_financial", ptype="text")
        self.put("skipped_other", "skipped", category="promotional")
        self.put("failed", "failed")
        self.put("pending", "pending")
        self.all = ["processed", "attention", "nf", "skipped_other", "failed", "pending", "new"]

    def test_not_financial_skipped_by_default(self):
        self.assertEqual(self.kept(self.all), ["skipped_other", "failed", "pending", "new"])

    def test_manual_reclassify_opts_back_in(self):
        self.assertEqual(self.kept(self.all, reclassify_not_financial=True),
                         ["nf", "skipped_other", "failed", "pending", "new"])

    def test_only_this_users_rows_count(self):
        self.session.add(ProcessedEmails(gmail_id="theirs", user_id="other", status="skipped",
                                         category="not_financial", processing_type="text",
                                         items_extracted=0))
        self.session.commit()
        self.assertEqual(self.kept(["theirs"]), ["theirs"])

    def test_lookup_failure_lets_everything_through(self):
        self.svc.transaction_service.db.session = mock.MagicMock(
            query=mock.MagicMock(side_effect=RuntimeError("db down")))
        self.assertEqual(self.kept(["nf", "new"]), ["nf", "new"])


HDFC = "HDFC Bank <alerts@hdfcbank.net>"


class TestLikelyFinancialRetry(_Db):
    """Review MAJOR (lead decision): a likely-financial email the classifier
    dropped is retried automatically, up to 3 not_financial verdicts."""

    def test_bank_sender_misclassified_once_is_retried_and_recovered(self):
        e = _email("hd1", sender=HDFC)
        self.svc._record_not_financial(dict(e), "u")            # false negative
        self.assertEqual(self.row("hd1").extraction_summary["_nf_verdicts"], 1)
        self.assertEqual(self.kept(["hd1"]), ["hd1"])            # retried next cycle
        # This time the classifier gets it right and it is processed.
        self.svc._stamp_processed_email(dict(e), "u", category="bank_statement", status="success")
        self.assertEqual(self.row("hd1").status, "processed")
        self.assertEqual(self.kept(["hd1"]), [])

    def test_three_verdict_cap(self):
        e = _email("hd2", sender=HDFC)
        seen = []
        for _ in range(4):
            if not self.kept(["hd2"]):
                break
            seen.append(1)
            self.svc._record_not_financial(dict(e), "u")
        self.assertEqual(len(seen), 3)                            # classified exactly 3 times
        self.assertEqual(self.row("hd2").extraction_summary["_nf_verdicts"], 3)
        self.assertEqual(self.kept(["hd2"]), [])

    def test_pdf_from_unknown_sender_is_retried(self):
        self.svc._record_not_financial(_email("pdf1", _has_pdf=True), "u")
        self.assertEqual(self.kept(["pdf1"]), ["pdf1"])

    def test_newsletter_skipped_after_one_verdict(self):
        self.svc._record_not_financial(_email("news1"), "u")
        self.assertEqual(self.kept(["news1"]), [])

    def test_row_sender_used_when_fetch_dict_lacks_it(self):
        self.put("hd3", "skipped", category="not_financial", ptype="text", sender=HDFC,
                 summary={"_nf_verdicts": 1})
        self.assertEqual(self.kept(["hd3"]), ["hd3"])

    def test_counter_keeps_other_keys_and_tolerates_junk(self):
        self.put("hd4", "skipped", category="not_financial", ptype="text", sender=HDFC,
                 summary={"_nf_verdicts": "junk", "_attempts": {"reason": "no_rows", "count": 1}})
        self.svc._record_not_financial(_email("hd4", sender=HDFC), "u")
        summ = self.row("hd4").extraction_summary
        self.assertEqual(summ["_nf_verdicts"], 1)
        self.assertEqual(summ["_attempts"]["count"], 1)

    def test_other_skip_reason_restarts_the_count(self):
        self.put("hd5", "skipped", category="promotional", ptype="text", sender=HDFC,
                 summary={"_nf_verdicts": 3})
        self.svc._record_not_financial(_email("hd5", sender=HDFC), "u")
        self.assertEqual(self.row("hd5").extraction_summary["_nf_verdicts"], 1)
        self.assertEqual(self.kept(["hd5"]), ["hd5"])


class TestReclassifyRange(unittest.TestCase):

    def test_range_cap(self):
        from utils.not_financial_policy import reclassify_range_error as err
        self.assertIsNone(err("2026/11/01", "2026/12/02"))         # 31 days
        self.assertIn("31 days", err("2026/11/01", "2026/12/03"))
        self.assertIsNotNone(err("2026/11/10", "2026/11/01"))
        for bad in ((None, "2026/11/01"), ("2026-11-01", "2026-11-02"), ("x", "y")):
            self.assertIsNotNone(err(*bad), bad)

    def test_endpoint_enforces_it_before_scanning(self):
        with open(os.path.join(HERE, "controllers", "transactionsEP.py")) as fh:
            src = fh.read()
        i = src.index("def triggerEmailCheck(")
        body = src[i:src.index("\n    def ", i + 10)]
        self.assertLess(body.index("reclassify_range_error(dateFrom, dateTo)"),
                        body.index("def run_scan"))
        self.assertIn('"RECLASSIFY_RANGE"}), 400', body)


class TestReclassifyEndpoint(unittest.TestCase):
    """The real triggerEmailCheck: a too-long manual reclassify is a 400
    and no scan starts."""

    def setUp(self):
        try:
            import flask
        except Exception as e:  # pragma: no cover
            self.skipTest(f"flask: {e}")
        import importlib.util
        import types
        stub = types.ModuleType("services.transactionsService")
        stub.TransactionService = object
        self._mods = mock.patch.dict(sys.modules, {"services.transactionsService": stub})
        self._mods.start()
        spec = importlib.util.spec_from_file_location(
            "_akv6zp_transactionsEP", os.path.join(HERE, "controllers", "transactionsEP.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self.mp = mock.MagicMock()
        self.ctrl = mod.TransactionController(mock.MagicMock(), mail_processor=self.mp)
        self.flask = flask
        self.app = flask.Flask("akv6zp")

    def tearDown(self):
        self._mods.stop()

    def call(self, qs):
        with self.app.test_request_context(f"/readEmails?{qs}"):
            self.flask.g.firebase_id = "u"
            with mock.patch("threading.Thread") as th:
                resp = self.ctrl.triggerEmailCheck()
        body, status = (resp if isinstance(resp, tuple) else (resp, resp.status_code))[:2]
        return status, body.get_json(), th

    def test_too_long_or_bad_range_rejected_without_scanning(self):
        for qs in ("dateFrom=2026/09/01&dateTo=2026/11/24&reclassify=true",
                   "dateFrom=2026-11-01&dateTo=2026-11-02&reclassify=1",
                   "reclassify=yes"):
            status, body, th = self.call(qs)
            self.assertEqual((status, body["error_code"]), (400, "RECLASSIFY_RANGE"), qs)
            th.assert_not_called()
        self.mp.process_emails.assert_not_called()

    def test_long_range_without_reclassify_unchanged(self):
        status, _body, th = self.call("dateFrom=2026/01/01&dateTo=2026/11/24")
        self.assertNotEqual(status, 400)
        th.assert_called_once()


class TestCycleAndEmailDate(_Db):

    def test_two_cycles_classify_once(self):
        # Cycle 1: classifier drops it -> stamped skipped/not_financial.
        e = _email("g1")
        self.assertEqual(self.kept(["g1"]), ["g1"])
        self.svc._record_not_financial(dict(e), "u")
        row = self.row("g1")
        self.assertEqual((row.status, row.category), ("skipped", self.NF))
        self.assertEqual(row.email_date, datetime(2026, 11, 24, 6, 30, 0))
        # Cycle 2: never reaches the classifier again.
        self.assertEqual(self.kept(["g1"]), [])

    def test_missing_email_date_filled_on_restamp_never_overwritten(self):
        self.put("legacy", "skipped", category="not_financial", ptype="text", email_date=None)
        self.svc._record_not_financial(_email("legacy", time="2026-11-20 09:00:00"), "u")
        self.assertEqual(self.row("legacy").email_date, datetime(2026, 11, 20, 9, 0, 0))
        known = datetime(2026, 1, 1, 0, 0, 0)
        self.put("known", "skipped", category="not_financial", ptype="text", email_date=known)
        self.svc._record_not_financial(_email("known", time="2026-11-20 09:00:00"), "u")
        self.assertEqual(self.row("known").email_date, known)

    def test_explicit_date_key_still_preferred(self):
        self.svc._stamp_processed_email(
            _email("g2", time="2026-11-24 06:30:00", date="2026-11-01"), "u",
            category=self.NF, status="skipped")
        self.assertEqual(self.row("g2").email_date, datetime(2026, 11, 1))

    def test_real_outcome_rows_are_never_restamped(self):
        self.put("failedrow", "failed")
        self.svc._record_not_financial(_email("failedrow"), "u")
        self.assertEqual(self.row("failedrow").status, "failed")


class TestParseEmailDate(unittest.TestCase):

    def test_everything_is_naive_utc(self):
        want = datetime(2026, 11, 24, 6, 30)
        for raw in ("2026-11-24 06:30:00", "2026-11-24T12:00:00+05:30", "2026-11-24T06:30:00Z",
                    "Mon, 24 Nov 2026 12:00:00 +0530"):
            got = _parse_email_date(raw)
            self.assertEqual((got, got.tzinfo), (want, None), raw)
        from datetime import timezone
        aware = datetime(2026, 11, 24, 12, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        self.assertEqual(_parse_email_date(aware), want)

    def test_fetch_format_and_existing_formats(self):
        self.assertEqual(_parse_email_date("2026-11-24 06:30:00"), datetime(2026, 11, 24, 6, 30))
        self.assertEqual(_parse_email_date("2026-11-24"), datetime(2026, 11, 24))
        self.assertEqual(_parse_email_date("24/11/2026"), datetime(2026, 11, 24))
        self.assertIsNone(_parse_email_date("Mon, 24 Nov 2026"))
        self.assertIsNone(_parse_email_date(None))


class TestSweepInteraction(unittest.TestCase):
    """i3tc / 6k35: the retry sweep picks failed-pdf and stale pending rows
    only, so skipped/not_financial rows are never re-fetched by it."""

    def test_sweep_never_selects_not_financial(self):
        from utils import retry_sweep
        now = datetime.utcnow()
        old = now - timedelta(hours=retry_sweep.PENDING_STALE_HOURS + 1)
        rows = [
            ("nf", "skipped", "text", old, None),
            ("nf_pdf", "skipped", "pdf", old, None),
            ("failed_pdf", "failed", "pdf", old, None),
        ]
        picked = retry_sweep.select_retry_ids(rows, now=now)
        self.assertIn("failed_pdf", picked)
        self.assertNotIn("nf", picked)
        self.assertNotIn("nf_pdf", picked)


class TestWiring(unittest.TestCase):

    def setUp(self):
        with open(SRC) as fh:
            self.mps = fh.read()
        with open(os.path.join(HERE, "controllers", "transactionsEP.py")) as fh:
            self.ep = fh.read()

    def test_process_emails_threads_the_flag(self):
        i = self.mps.index("    def process_emails(")
        sig = self.mps[i:self.mps.index("):", i)]
        self.assertIn("reclassify_not_financial=False", sig)
        body = self.mps[i:self.mps.index("\n    def ", i + 10)]
        self.assertIn("if reclassify_not_financial:", body)
        self.assertIn("all_emails, user_id, reclassify_not_financial=True)", body)
        # Default call keeps the pre-v6zp shape (ak-4scl fixtures stub it).
        self.assertIn("unprocessed = self._filter_already_processed(all_emails, user_id)", body)

    def test_only_the_manual_scan_endpoint_opts_in(self):
        i = self.ep.index("def triggerEmailCheck(")
        body = self.ep[i:self.ep.index("\n    def ", i + 10)]
        self.assertIn("request.args.get('reclassify')", body)
        self.assertIn("reclassify_not_financial=reclassify", body)
        # Scheduled task + agent tools never opt in.
        for path in ("services/tasks/checkMailUnifiedTask.py", "services/agent_tool_executor.py"):
            with open(os.path.join(HERE, path)) as fh:
                self.assertNotIn("reclassify", fh.read(), path)

    def test_stamp_uses_fetch_time(self):
        self.assertIn('email_date=email.get("date") or email.get("time")', self.mps)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
