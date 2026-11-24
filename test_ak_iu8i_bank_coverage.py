"""ak-iu8i (2026-11-24): bank statement coverage gaps.

Measured root causes (prod facts from infra, code paths measured here):
  A. convert_to_sql_datetime raised AttributeError for any bank id missing
     from DateStatementEnum (SBI / Axis / Kotak / UNKNOWN …).
  B. insertTransactions SWALLOWED that (logged, returned 0 errors), so the
     batch reported "inserted N" and the email was stamped processed with
     0 rows (live case: a yes.bank.in statement on 11-21, extracted then
     lost as bank 'UNKNOWN').
  C. get_bank_from_sender missed BoB entirely, the .bank.in domains
     (Yes Bank's switch = the regression), Paytm, and escaped headers.
Fixes under test, plus the lead's additions: classifier-dropped emails
are recorded skipped/'not_financial' (WARN for bank PDFs), and an
unknown/missing tool status maps to 'failed', not 'processed'.
Hard rule: an ambiguous numeric date for an unknown bank is REJECTED,
never guessed (a dd/mm vs mm/dd swap is a critical accounting error).
Synthetic data only.
"""
from __future__ import annotations

import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from services import bankFormatRules as B  # noqa: E402
from enums.DateFormatEnum import DateStatementEnum  # noqa: E402
from enums.BanksEnum import BankEnums  # noqa: E402
from utils.DateTimeUtil import DateTimeUtil, parse_unambiguous_date  # noqa: E402

try:
    import anyio
    import flask
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    from services import mailProcessorService as MPS
    from services import mailProcessorTools as MPT
    from services import mailProcessorToolExecutor as EX
    from services import transactionsService as TS
    _HAVE_APP = True
except Exception:  # pragma: no cover
    _HAVE_APP = False


class TestSenderMapping(unittest.TestCase):

    CASES = {
        "BoB <estatement@bankofbaroda.com>": "BOB",
        "x@bankofbaroda.co.in": "BOB",
        "x@alerts.bankofbaroda.bank.in": "BOB",
        "x@bobfinancial.com": "BOB",
        "Yes Bank <estatement@yes.bank.in>": "YES_BANK_DEBIT",   # the regression
        "x@alerts.yesbank.bank.in": "YES_BANK_DEBIT",
        "x@yesbank.in": "YES_BANK_DEBIT",                        # old domain still works
        "x@sbi.bank.in": "SBI",
        "x@cbsalerts.sbi.co.in": "SBI",
        "x@icicibank.bank.in": "ICICI_AMAZON_PAY",
        "x@axisbank.bank.in": "AXIS",
        "x@kotak.bank.in": "KOTAK",
        "x@paytmbank.com": "PAYTM",
        "x@sbicard.com": "SBI_CARD",
        "&quot;HDFC Bank&quot; &lt;alerts@hdfcbank.net&gt;": "HDFC_DEBIT",
        '"HDFC Bank" <alerts@hdfcbank.net>': "HDFC_DEBIT",
        "HDFC &lt;alerts&#64;hdfcbank.net&gt;": "HDFC_DEBIT",      # entity-encoded @
        "alerts@hdfcbank.net.": "HDFC_DEBIT",
        "<alerts@HDFCBANK.NET>": "HDFC_DEBIT",
    }

    def test_known_senders(self):
        for sender, bank in self.CASES.items():
            self.assertEqual(B.get_bank_from_sender(sender), bank, sender)

    def test_unknown_and_spoof_shapes(self):
        for sender in ("HDFC Bank",                      # display name only: never guessed
                       "x@paytm.com",                    # wallet/shopping, not the Payments Bank
                       "x@bankofbaroda.com.evil.example",
                       "x@evilbankofbaroda.com",
                       "x@yes.bank.in.evil.example",
                       "", None):
            self.assertEqual(B.get_bank_from_sender(sender or ""), "UNKNOWN", sender)

    def test_every_mapped_bank_can_be_parsed_and_listed(self):
        # The root cause of A: a mapped bank id without a date-format entry.
        for bank in set(B.DOMAIN_TO_BANK.values()):
            self.assertIn(bank, DateStatementEnum.__members__, bank)
            self.assertIn(bank, BankEnums.__members__, bank)
            self.assertEqual(DateTimeUtil().convert_to_sql_datetime("20/11/2026", bank),
                             "2026-11-20 00:00:00", bank)

    def test_password_domains_are_subdomain_and_escape_tolerant(self):
        self.assertEqual(B.get_banks_for_domain("alerts.bankofbaroda.bank.in"), ["BOB"])
        self.assertEqual(B.sender_domain("&lt;a@b.sbi.bank.in&gt;"), "b.sbi.bank.in")


class TestDateParsing(unittest.TestCase):

    def test_unambiguous_only_for_unknown_bank(self):
        ok = {"2026-11-05": (2026, 11, 5), "2026-11-05 10:30:00": (2026, 11, 5),
              "24-Nov-2026": (2026, 11, 24), "24 Nov 2026": (2026, 11, 24),
              "Nov 24, 2026": (2026, 11, 24), "13/05/2026": (2026, 5, 13),
              "05/05/2026": (2026, 5, 5), "25-12-2026": (2026, 12, 25),
              "24-Nov-26": (2026, 11, 24)}
        for s, ymd in ok.items():
            d = parse_unambiguous_date(s)
            self.assertEqual((d.year, d.month, d.day), ymd, s)
        # Review minor 1: a 2-digit-year numeric date is DD-MM-YY or
        # YY-MM-DD, so it's rejected even when day > 12.
        for s in ("05/06/2026", "01/12/2026", "12-01-2026", "05/13/2026",
                  "26-11-24", "24/11/26", "25-12-26", "garbage", "", None):
            self.assertIsNone(parse_unambiguous_date(s), s)

    def test_ambiguous_date_for_unknown_bank_is_rejected_loudly(self):
        u = DateTimeUtil()
        for bank in ("UNKNOWN", "FEDERAL", None):
            with self.assertRaises(ValueError) as cm:
                u.convert_to_sql_datetime("05/06/2026", bank)
            self.assertIn("not guessed", str(cm.exception))
            self.assertNotIn("05/06/2026", str(cm.exception))     # no row value echoed
        self.assertEqual(u.convert_to_sql_datetime("24-Nov-2026", "UNKNOWN"), "2026-11-24 00:00:00")

    def test_known_bank_behaviour_unchanged(self):
        u = DateTimeUtil()
        # The extraction contract is DD/MM/YYYY: 05/06 is 5 June.
        self.assertEqual(u.convert_to_sql_datetime("05/06/2026", "BOB"), "2026-06-05 00:00:00")
        self.assertEqual(u.convert_to_sql_datetime("05/06/2026", "HDFC_DEBIT"), "2026-06-05 00:00:00")
        self.assertEqual(u.convert_to_sql_datetime("2026-06-05", "BOI"), "2026-06-05 00:00:00")
        self.assertEqual(u.convert_to_sql_datetime_date("05-06-2026", "EPF_STATEMENT").month, 6)
        with self.assertRaises(ValueError):
            u.convert_to_sql_datetime("not a date", "HDFC_DEBIT")


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestRealInsertPath(unittest.TestCase):
    """The REAL TransactionService + executor + handler on sqlite.

    SQLite's Date/DateTime types reject the string dates MySQL accepts;
    their bind/result processors are made pass-through for this test only
    so the production string path runs unchanged.
    """

    @classmethod
    def setUpClass(cls):
        from sqlalchemy.dialects import sqlite as sq
        cls._patches = [mock.patch.object(sq.DATETIME, "bind_processor",
                                          lambda self, d: (lambda v: v)),
                        mock.patch.object(sq.DATE, "bind_processor",
                                          lambda self, d: (lambda v: v)),
                        mock.patch.object(sq.DATETIME, "result_processor",
                                          lambda self, d, c: None),
                        mock.patch.object(sq.DATE, "result_processor",
                                          lambda self, d, c: None)]
        for p in cls._patches:
            p.start()

    @classmethod
    def tearDownClass(cls):
        for p in cls._patches:
            p.stop()

    def setUp(self):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.transactions import Transactions
        from models.fileDetails import FileDetails
        eng = sa.create_engine("sqlite://")
        from models.statementPeriods import StatementPeriod
        for t in (FileDetails.__table__, Transactions.__table__, StatementPeriod.__table__):
            t.create(eng)
        self.session = sessionmaker(bind=eng)()
        self.T, self.FD = Transactions, FileDetails
        self.app = flask.Flask("akiu8i")
        self.ctx = self.app.app_context()
        self.ctx.push()
        flask.g.db = types.SimpleNamespace(session=self.session)
        self.ts = TS.TransactionService()

    def tearDown(self):
        self.ctx.pop()

    def _args(self, bank, date, gid):
        return {"transactions": [{"date": date, "amount": 100, "description": "synthetic",
                                  "bank_reference_id": None, "line_position": 1}],
                "bank": bank, "source": "Statement", "gmail_message_id": gid,
                "file_id": f"mail_pipeline_u_{bank}_{gid}"}

    def test_failed_insert_raises_not_reports_success(self):
        with self.assertRaises(TS.TransactionInsertError):
            self.ts.insertTransactions(
                [{"reference": "r1", "date": "05/06/2026", "description": "x", "amount": 1}],
                "FEDERAL", "u", [], "Statement", fileId=None)
        self.assertEqual(self.session.query(self.T).count(), 0)

    def test_executor_reports_error_and_cleans_its_file_row(self):
        out = EX.execute_mail_tool("insert_batch_transactions",
                                   self._args("FEDERAL", "05/06/2026", "g1"), "u",
                                   transaction_service=self.ts)
        self.assertIn("error", out)
        self.assertNotIn("inserted", out)
        self.assertEqual(self.session.query(self.T).count(), 0)
        self.assertEqual(self.session.query(self.FD).count(), 0)

    def test_cleanup_failure_is_warned(self):
        # Review minor 3.
        with mock.patch.object(self.ts, "deleteFileDetails", side_effect=RuntimeError("db")), \
                self.assertLogs(EX.logger.name, level="WARNING") as cm:
            out = EX.execute_mail_tool("insert_batch_transactions",
                                       self._args("FEDERAL", "05/06/2026", "g9"), "u",
                                       transaction_service=self.ts)
        self.assertIn("error", out)
        self.assertTrue(any("cleanup failed after a failed insert (RuntimeError)" in m for m in cm.output))

    def test_yes_bank_in_statement_now_lands(self):
        bank = B.get_bank_from_sender("Yes Bank <estatement@yes.bank.in>")
        out = EX.execute_mail_tool("insert_batch_transactions",
                                   self._args(bank, "20/11/2026", "g2"), "u",
                                   transaction_service=self.ts)
        self.assertEqual(out.get("result"), "success")
        row, = self.session.query(self.T).all()
        self.assertEqual((row.bank, str(row.date)[:10]), ("YES_BANK_DEBIT", "2026-11-20"))

    def test_bob_statement_lands_with_contract_date(self):
        out = EX.execute_mail_tool("insert_batch_transactions",
                                   self._args("BOB", "05/06/2026", "g3"), "u",
                                   transaction_service=self.ts)
        self.assertEqual(out.get("result"), "success")
        self.assertEqual(str(self.session.query(self.T).one().date)[:10], "2026-06-05")

    def test_chunk_fails_through_the_real_handler(self):
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        svc.transaction_service = self.ts
        for a in ("investment_service", "invoice_service", "reconciliation_service"):
            setattr(svc, a, None)
        for bank, date, gid, landed in (("FEDERAL", "05/06/2026", "g4", False),
                                        ("BOB", "05/06/2026", "g5", True)):
            ic = {"value": False}
            h = {t.name: t.handler for t in svc._build_sdk_tools(
                "u", insert_called=ic, tool_names=MPT.PDF_TOOL_NAMES)}
            anyio.run(lambda: h["insert_batch_transactions"](self._args(bank, date, gid)))
            self.assertEqual(svc._chunk_insert_result(ic, 1, 1)["called"], landed, bank)


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestNotFinancialRecorded(unittest.TestCase):

    def _svc(self, existing=None):
        """existing: {gmail_id: status} rows already in processedEmails."""
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        svc._stamp_processed_email = mock.MagicMock()
        svc.transaction_service = mock.MagicMock()
        existing = existing or {}

        def filter_by(gmail_id=None, user_id=None):
            m = mock.MagicMock()
            m.first.return_value = (types.SimpleNamespace(status=existing[gmail_id])
                                    if gmail_id in existing else None)
            return m
        svc.transaction_service.db.session.query.return_value.filter_by.side_effect = filter_by
        return svc

    EMAILS = [{"gmail_id": "a", "sender": "x@shop.example", "_has_pdf": False},
              {"gmail_id": "b", "sender": "BoB <s@bankofbaroda.com>", "_has_pdf": True},
              {"gmail_id": "c", "sender": "alerts@hdfcbank.net", "_has_pdf": True}]

    def test_dropped_emails_recorded_and_bank_pdf_warned(self):
        svc = self._svc()
        with mock.patch.object(svc, "_run_classification_batch",
                               return_value=[{"gmail_id": "c", "category": "bank_statement"}]):
            fin = svc._classify_emails([dict(e) for e in self.EMAILS], user_id="u")
        self.assertEqual([e["gmail_id"] for e in fin], ["c"])
        stamped = {c.args[0]["gmail_id"]: c.kwargs for c in svc._stamp_processed_email.call_args_list}
        self.assertEqual(set(stamped), {"a", "b"})
        for kw in stamped.values():
            self.assertEqual((kw["category"], kw["status"]), ("not_financial", "skipped"))
        warned = [str(c) for c in svc.logger.warning.call_args_list]
        self.assertEqual(len(warned), 1)
        self.assertIn("'b'", warned[0])
        self.assertIn("bankofbaroda.com", warned[0])

    def test_existing_failed_row_is_not_overwritten(self):
        # Review minor 2: a 'failed' row keeps its status + reason.
        svc = self._svc(existing={"a": "failed", "b": "skipped"})
        with mock.patch.object(svc, "_run_classification_batch", return_value=[]):
            svc._classify_emails([dict(e) for e in self.EMAILS[:2]], user_id="u")
        stamped = [c.args[0]["gmail_id"] for c in svc._stamp_processed_email.call_args_list]
        self.assertEqual(stamped, ["b"])

    def test_failed_classifier_batch_records_nothing(self):
        # ak-4scl: when every batch fails the classifier now RAISES (a
        # loud pipeline failure, not "No financial emails"), and a failed
        # batch still records NOTHING, so the emails stay unprocessed and
        # are fetched again next cycle.
        svc = self._svc()
        with mock.patch.object(svc, "_run_classification_batch", side_effect=RuntimeError("x")):
            with self.assertRaises(MPS.ClassificationFailed):
                svc._classify_emails([dict(e) for e in self.EMAILS], user_id="u")
        svc._stamp_processed_email.assert_not_called()

    def test_partially_failed_classification_records_only_the_good_batch(self):
        svc = self._svc()
        calls = {"n": 0}

        def batch(b):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            return []
        with mock.patch.object(MPS, "CLASSIFICATION_BATCH_SIZE", 2), \
                mock.patch.object(svc, "_run_classification_batch", side_effect=batch):
            self.assertEqual(svc._classify_emails([dict(e) for e in self.EMAILS], user_id="u"), [])
        stamped = {c.args[0]["gmail_id"] for c in svc._stamp_processed_email.call_args_list}
        self.assertEqual(stamped, {"c"})      # batch 1 (a, b) failed → not recorded


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestStoredPasswordLookup(unittest.TestCase):

    def test_banks_without_order_hint_use_the_shared_map(self):
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        svc.transaction_service = mock.MagicMock()
        q = svc.transaction_service.db.session.query.return_value
        asked = []

        def filter_by(**kw):
            asked.append(kw.get("bank"))
            m = mock.MagicMock()
            m.first.return_value = types.SimpleNamespace(password_hash="stored")
            return m
        q.filter_by.side_effect = filter_by
        for sender, bank in (("x@sbi.bank.in", "SBI"), ("s@bankofbaroda.com", "BOB"),
                             ("e@yes.bank.in", "YES_BANK_DEBIT")):
            asked.clear()
            pw = svc._get_statement_password("u", {"sender": sender}, pdf_path=None)
            self.assertEqual(pw, "stored", sender)
            self.assertEqual(asked[0], bank, sender)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
