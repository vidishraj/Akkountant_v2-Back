"""ak-fc7z (2026-11-24): statement PDFs must never be stamped 'processed'
with zero transactions.

Prod (infra, read-only): 43 of 65 'processed' bank-statement PDFs have 0
linked rows. Paths: the PDF unlock failed and the agent ran on a locked
file, or no batch landed. Fixes under test:
  1. server-side unlock check BEFORE any agent run → 'failed'
     (unlock_failed), the agent is never called;
  2. email-level rule: a ledger statement with 0 landed rows in total →
     'failed' (no_rows); positively non-ledger PDFs from non-bank senders
     may land 0;
  3. text mode's "Claude returned 0 transactions" on a statement → the
     same rule;
  4. items_extracted = rows that actually landed (+ accepted record-only
     inserts), not the model's self-report;
  5. non-ledger PDFs never use text mode (whose rows go to the BANK
     batch insert).
All data synthetic. Real handler, real executor validation, real
ProcessedEmails table (in-memory sqlite).
"""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import statement_ingest_outcome as O  # noqa: E402

try:
    import anyio  # noqa: F401
    import fitz
    import flask
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    from services import mailProcessorService as MPS
    from services import mailProcessorTools as MPT
    _HAVE_APP = True
except Exception:  # pragma: no cover
    _HAVE_APP = False

HDFC = "HDFC Bank <alerts@hdfcbank.net>"
CAMS = "CAMS <statements@camsonline.example>"
ROWS = [{"date": "20/11/2026", "amount": 100, "description": "d1"},
        {"date": "21/11/2026", "amount": -50, "description": "d2"}]
REPORT = {"gmail_message_id": "g1", "status": "success", "sender": "s",
          "subject": "x", "processing_type": "pdf", "items_extracted": 99}


class TestFinalEmailStatus(unittest.TestCase):

    def test_rule(self):
        f = O.final_email_status
        self.assertEqual(f("bank_statement", 0, [O.UNLOCK_FAILED]), ("error", O.UNLOCK_FAILED))
        self.assertEqual(f("bank_statement", 5, [O.INGEST_INCOMPLETE, O.UNLOCK_FAILED]),
                         ("error", O.UNLOCK_FAILED))
        self.assertEqual(f("brokerage_statement", 0, [O.INGEST_INCOMPLETE], CAMS),
                         ("error", O.INGEST_INCOMPLETE))
        self.assertEqual(f("bank_statement", 0, []), ("error", O.NO_ROWS))
        self.assertEqual(f(None, 0, []), ("error", O.NO_ROWS))           # fail-safe
        self.assertEqual(f("transaction_alert", 0, []), ("error", O.NO_ROWS))
        self.assertEqual(f("brokerage_statement", 0, [], HDFC), ("error", O.NO_ROWS))
        self.assertEqual(f("brokerage_statement", 0, [], CAMS), ("success", None))
        self.assertEqual(f("epf_passbook", 0, [], "x@epfindia.example"), ("success", None))
        self.assertEqual(f("bank_statement", 3, []), ("success", None))

    def test_reason_texts_carry_the_reason_code(self):
        for code in (O.UNLOCK_FAILED, O.INGEST_INCOMPLETE, O.NO_ROWS):
            self.assertTrue(O.REASON_TEXT[code].startswith(code))

    def test_counters_are_per_context(self):
        self.assertIsNone(O.LANDED_ROWS.get())
        O.add_landed({"inserted": 3})           # no counter set → no-op
        c = {"rows": 0, "recorded": 0}
        tok = O.LANDED_ROWS.set(c)
        try:
            O.add_landed({"inserted": 2, "duplicates": 3})
            O.add_landed({"result": "no_transactions", "inserted": 0})
            O.add_landed("junk")
            O.add_recorded()
        finally:
            O.LANDED_ROWS.reset(tok)
        self.assertEqual(c, {"rows": 5, "recorded": 1})


@unittest.skipUnless(_HAVE_APP, "app deps not installed")
class TestEmailLevelStamp(unittest.TestCase):

    def setUp(self):
        import models  # noqa: F401
        from models import investmentHistory  # noqa: F401
        from models.processedEmails import ProcessedEmails
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        for a in ("transaction_service", "investment_service",
                  "invoice_service", "reconciliation_service"):
            setattr(svc, a, mock.MagicMock())
        eng = sa.create_engine("sqlite://")
        ProcessedEmails.__table__.create(eng)
        from models.transactions import Transactions
        Transactions.__table__.create(eng)   # text mode counts rows per file
        self.session = sessionmaker(bind=eng)()
        svc.transaction_service.db.session = self.session
        self.svc, self.PE = svc, ProcessedEmails
        self.batch_result = None     # None → "lands" every row as inserted
        real = MPS.execute_mail_tool

        def exec_(tn, args, user_id, **kw):
            if tn == "insert_batch_transactions":
                if self.batch_result is not None:
                    return dict(self.batch_result)
                if not args["transactions"]:
                    return {"result": "no_transactions", "inserted": 0}
                return {"inserted": len(args["transactions"]), "duplicates": 0}
            if tn.startswith("insert_"):
                return real(tn, args, user_id, **kw)       # record-only, no DB
            return {"result": "ok"}
        self._p = mock.patch.object(MPS, "execute_mail_tool", side_effect=exec_)
        self._p.start()
        self._ctx = flask.Flask("akfc7z").app_context()
        self._ctx.push()
        self.dir = tempfile.mkdtemp()
        self.chunk_runs = 0

    def tearDown(self):
        self._ctx.pop()
        self._p.stop()

    def _pdf(self, password=None):
        path = os.path.join(self.dir, f"s{len(os.listdir(self.dir))}.pdf")
        doc = fitz.open()
        doc.new_page()
        if password:
            doc.save(path, encryption=fitz.PDF_ENCRYPT_AES_256,
                     user_pw=password, owner_pw=password + "-owner")
        else:
            doc.save(path)
        doc.close()
        return path

    def _batch(self, rows):
        return ("insert_batch_transactions", {
            "transactions": [dict(r) for r in rows], "bank": "HDFC_DEBIT",
            "source": "Statement", "gmail_message_id": "g1"})

    def _run(self, scripts, category="bank_statement", sender=HDFC,
             password_found=None, locked_with=None):
        """Real _process_single_pdf_email; each PDF's chunk runs its
        script through the real handler and returns the real chunk
        result with the real category rule."""
        svc = self.svc
        queue = list(scripts)

        async def fake_chunk(pdf_path, email, user_id, password, page_start,
                             page_end, *a, **kw):
            self.chunk_runs += 1
            ic = {"value": False, "inserted": 0, "duplicates": 0}
            tools = svc._build_sdk_tools(user_id, insert_called=ic,
                                         tool_names=MPT.PDF_TOOL_NAMES)
            h = {t.name: t.handler for t in tools}
            for tool, args in queue.pop(0):
                await h[tool](dict(args))
            return svc._chunk_insert_result(ic, page_start, page_end,
                                            ledger=svc._is_ledger_email(email))

        email = {"gmail_id": "g1", "sender": sender, "subject": "stmt",
                 "_category": category,
                 "_local_pdf_paths": [self._pdf(locked_with) for _ in scripts]}
        with mock.patch.object(svc, "_run_pdf_chunk_async", side_effect=fake_chunk), \
             mock.patch.object(svc, "_pdf_has_usable_text", return_value=False), \
             mock.patch.object(svc, "_get_statement_password", return_value=password_found), \
             mock.patch.object(svc, "_persist_pdf", return_value=None), \
             mock.patch.object(svc, "_post_chunk_reconciliation"):
            svc._process_single_pdf_email(email, "u", "image")
        row, = self.session.query(self.PE).filter_by(gmail_id="g1").all()
        return row

    # 1. unlock
    def test_unlock_failure_stamps_error_and_never_runs_the_agent(self):
        for found in (None, "wrong-password"):
            self.session.query(self.PE).delete()
            row = self._run([[self._batch(ROWS)]], password_found=found, locked_with="right-pw")
            self.assertEqual(row.status, "failed", found)
            self.assertTrue(row.error_message.startswith(O.UNLOCK_FAILED))
            self.assertEqual(row.items_extracted, 0)
        self.assertEqual(self.chunk_runs, 0)

    def test_correct_password_unlocks_and_ingests(self):
        row = self._run([[self._batch(ROWS), ("report_result", REPORT)]],
                        password_found="right-pw", locked_with="right-pw")
        self.assertEqual(row.status, "processed")
        self.assertEqual(self.chunk_runs, 1)

    def test_one_locked_pdf_fails_the_whole_email(self):
        svc = self.svc
        good = self._pdf()
        locked = self._pdf("right-pw")
        email = {"gmail_id": "g1", "sender": HDFC, "subject": "s",
                 "_category": "bank_statement", "_local_pdf_paths": [good, locked]}
        queue = [[self._batch(ROWS)]]

        async def fake_chunk(pdf_path, email, user_id, password, ps, pe, *a, **kw):
            ic = {"value": False}
            h = {t.name: t.handler for t in svc._build_sdk_tools(
                user_id, insert_called=ic, tool_names=MPT.PDF_TOOL_NAMES)}
            for tool, args in queue.pop(0):
                await h[tool](dict(args))
            return svc._chunk_insert_result(ic, ps, pe, ledger=True)
        with mock.patch.object(svc, "_run_pdf_chunk_async", side_effect=fake_chunk), \
             mock.patch.object(svc, "_pdf_has_usable_text", return_value=False), \
             mock.patch.object(svc, "_get_statement_password", return_value=None), \
             mock.patch.object(svc, "_persist_pdf", return_value=None), \
             mock.patch.object(svc, "_post_chunk_reconciliation"):
            svc._process_single_pdf_email(email, "u", "image")
        row, = self.session.query(self.PE).filter_by(gmail_id="g1").all()
        self.assertEqual(row.status, "failed")
        self.assertTrue(row.error_message.startswith(O.UNLOCK_FAILED))
        self.assertEqual(row.items_extracted, 2)           # the good PDF's rows

    # 2. zero rows
    def test_statement_with_only_empty_batches_is_no_rows(self):
        row = self._run([[self._batch([]), ("report_result", REPORT)]])
        self.assertEqual(row.status, "failed")
        self.assertTrue(row.error_message.startswith(O.NO_ROWS))
        self.assertEqual(row.items_extracted, 0)            # not the model's 99

    def test_mislabelled_bank_pdf_with_zero_rows_is_no_rows(self):
        row = self._run([[self._batch([])]], category="brokerage_statement", sender=HDFC)
        self.assertEqual((row.status, row.error_message[:7]), ("failed", O.NO_ROWS))

    def test_cas_with_zero_rows_is_processed_items_are_recorded_inserts(self):
        script = [("insert_investment", {"service_type": "Mutual_Funds", "date": "2026-11-20",
                                         "amount": 5000, "quantity": 10}),
                  ("insert_investment", {"service_type": "Mutual_Funds", "date": "2026-11-21",
                                         "amount": 2500, "quantity": 5}),
                  ("report_result", REPORT)]
        row = self._run([script], category="brokerage_statement", sender=CAMS)
        self.assertEqual(row.status, "processed")
        self.assertEqual(row.items_extracted, 2)

    # 4. items_extracted
    def test_items_extracted_is_landed_rows_not_model_report(self):
        row = self._run([[self._batch(ROWS), ("report_result", REPORT)]])
        self.assertEqual((row.status, row.items_extracted), ("processed", 2))

    def test_dedup_only_rerun_counts_as_landed(self):
        self.batch_result = {"inserted": 0, "duplicates": 2}
        row = self._run([[self._batch(ROWS)]])
        self.assertEqual((row.status, row.items_extracted), ("processed", 2))

    def test_rows_summed_across_pdfs(self):
        row = self._run([[self._batch(ROWS)], [self._batch(ROWS[:1])]])
        self.assertEqual((row.status, row.items_extracted), ("processed", 3))

    # 3. text mode zero transactions
    def test_text_mode_zero_transactions_statement_is_no_rows(self):
        svc = self.svc

        async def zero_txn_chunk(*a, **kw):
            # The real _run_text_chunk return when Claude reports 0 rows.
            return {"called": True, "inserted": 0, "duplicates": 0}

        email = {"gmail_id": "g1", "sender": HDFC, "subject": "s",
                 "_category": "bank_statement", "_local_pdf_paths": [self._pdf()]}
        with mock.patch.object(svc, "_run_text_chunk", side_effect=zero_txn_chunk), \
             mock.patch.object(svc, "_pdf_has_usable_text", return_value=True), \
             mock.patch.object(svc, "_get_statement_password", return_value=None), \
             mock.patch.object(svc, "_persist_pdf", return_value=None), \
             mock.patch.object(svc, "_post_chunk_reconciliation"), \
             mock.patch.object(svc, "_run_hdfc_file_level_reconciliation",
                               new=mock.AsyncMock(), create=True):
            svc._process_single_pdf_email(email, "u", "image")
        row, = self.session.query(self.PE).filter_by(gmail_id="g1").all()
        self.assertEqual(row.status, "failed")
        self.assertTrue(row.error_message.startswith(O.NO_ROWS))

    # 5. text-mode routing
    def test_non_ledger_pdf_never_uses_text_mode(self):
        svc = self.svc
        seen = []

        async def chunk(*a, **kw):
            seen.append(a[-1] if a else kw.get("processing_mode"))
            return {"called": True}
        email = {"gmail_id": "g1", "sender": CAMS, "_category": "brokerage_statement"}
        with mock.patch.object(svc, "_run_pdf_chunk_async", side_effect=chunk), \
             mock.patch.object(svc, "_run_text_chunk") as text_chunk, \
             mock.patch.object(svc, "_pdf_has_usable_text", return_value=True), \
             mock.patch.object(svc, "_post_chunk_reconciliation"):
            for mode in ("text", "image"):
                svc._run_pdf_analysis(self._pdf(), email, "u", None, mode)
        text_chunk.assert_not_called()
        self.assertEqual(seen, ["image", "image"])

    def test_ledger_pdf_with_text_layer_still_uses_text_mode(self):
        svc = self.svc
        email = {"gmail_id": "g1", "sender": HDFC, "_category": "bank_statement"}
        with mock.patch.object(svc, "_run_text_chunk",
                               new=mock.AsyncMock(return_value={"called": True, "inserted": 1})) as tc, \
             mock.patch.object(svc, "_run_pdf_chunk_async") as image_chunk, \
             mock.patch.object(svc, "_pdf_has_usable_text", return_value=True), \
             mock.patch.object(svc, "_post_chunk_reconciliation"), \
             mock.patch.object(svc, "_run_hdfc_file_level_reconciliation",
                               new=mock.AsyncMock(), create=True):
            svc._run_pdf_analysis(self._pdf(), email, "u", None, "image")
        self.assertTrue(tc.await_count >= 1)
        image_chunk.assert_not_called()

    def _text_chunk(self, email, insert_result):
        """The REAL _run_text_chunk, with only the SDK call and the DB
        insert stubbed."""
        import types as _t
        run = mock.AsyncMock(return_value=_t.SimpleNamespace(
            structured_output={"transactions": [dict(r) for r in ROWS]}, text="", error=None))
        counter = {"rows": 0, "recorded": 0}
        tok = O.LANDED_ROWS.set(counter)
        try:
            with mock.patch.object(MPS, "run_query_collect", new=run), \
                 mock.patch("services.mailProcessorToolExecutor._handle_insert_batch_transactions",
                           return_value=insert_result) as ins:
                out = anyio.run(lambda: self.svc._run_text_chunk(
                    mock.MagicMock(system_prompt="sys"), self._pdf(), email, "u", None, 1, 1, 1, preset_file_id="f1"))
        finally:
            O.LANDED_ROWS.reset(tok)
        return out, counter, ins

    def test_text_chunk_counts_landed_rows(self):
        email = {"gmail_id": "g1", "sender": "x@bankofindia.co.in", "_category": "bank_statement"}
        out, counter, ins = self._text_chunk(email, {"inserted": 1, "duplicates": 1})
        ins.assert_called_once()
        self.assertTrue(out["called"])
        self.assertEqual(counter["rows"], 2)

    def test_text_insert_guard_refuses_non_ledger(self):
        email = {"gmail_id": "g1", "sender": CAMS, "_category": "brokerage_statement"}
        out, counter, ins = self._text_chunk(email, {"inserted": 2, "duplicates": 0})
        ins.assert_not_called()                 # never filed as bank txns
        self.assertEqual((out["called"], out["rejected"]), (False, 1))
        self.assertEqual(counter["rows"], 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
