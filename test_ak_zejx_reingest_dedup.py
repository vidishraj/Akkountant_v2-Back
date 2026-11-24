"""ak-zejx review MAJOR (2026-11-24): re-ingesting a statement under a
corrected bank id must not duplicate its transactions.

utils.reference_id folds the BANK into both dedup hashes, so a statement
first ingested as BOI and re-ingested as BOB (the i3tc sweep, an 8gfj
revive, iu8i's new mapping) minted NEW referenceIDs: every row doubled and
the ledger double-counted. Rewriting referenceIDs isn't possible: the
fallback hash needs a line_position that isn't stored, and transferLinks
FK onto referenceID. So statement re-ingest now dedups
BANK-AGNOSTICALLY: rows of the same statement written by an EARLIER run
are a multiset of (date, amount, normalised description) that incoming
rows consume. Chunks of the SAME run never dedup against each other, so
legit identical rows on different pages stay distinct, and two identical
earlier rows absorb exactly two incoming ones.
Real TransactionService + executor + tables on sqlite. Synthetic data only.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import os
import sys
import types
import unittest
from decimal import Decimal
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

try:
    import flask
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    import models  # noqa: F401
    from models import investmentHistory  # noqa: F401
    from models.transactions import Transactions
    from models.fileDetails import FileDetails
    from models.processedEmails import ProcessedEmails
    from models.statementPeriods import StatementPeriod
    from services import mailProcessorToolExecutor as EX
    from services import transactionsService as TS
    from utils import statement_ingest_outcome as O
    _HAVE = True
except Exception:  # pragma: no cover
    _HAVE = False

COFFEE = {"date": "20/11/2026", "amount": 100, "description": "UPI COFFEE",
          "bank_reference_id": None, "line_position": 1}
# A legit second identical coffee sits on another statement line.
COFFEE2 = dict(COFFEE, line_position=3)
RENT = {"date": "21/11/2026", "amount": 25000, "description": "NEFT RENT",
        "bank_reference_id": "N123", "line_position": 2}


@unittest.skipUnless(_HAVE, "app deps not installed")
class TestReingestDedup(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from sqlalchemy.dialects import sqlite as sq
        cls._p = [mock.patch.object(sq.DATETIME, "bind_processor", lambda s, d: (lambda v: v)),
                  mock.patch.object(sq.DATE, "bind_processor", lambda s, d: (lambda v: v)),
                  mock.patch.object(sq.DATETIME, "result_processor", lambda s, d, c: None),
                  mock.patch.object(sq.DATE, "result_processor", lambda s, d, c: None)]
        for p in cls._p:
            p.start()

    @classmethod
    def tearDownClass(cls):
        for p in cls._p:
            p.stop()

    def setUp(self):
        eng = sa.create_engine("sqlite://")
        for t in (FileDetails.__table__, Transactions.__table__, StatementPeriod.__table__,
                  ProcessedEmails.__table__):
            t.create(eng)
        self.s = sessionmaker(bind=eng)()
        self.ctx = flask.Flask("akzejx").app_context()
        self.ctx.push()
        flask.g.db = types.SimpleNamespace(session=self.s)
        self.ts = TS.TransactionService()

    def tearDown(self):
        self.ctx.pop()

    def ingest(self, bank, rows, gid="g1", run=None):
        """One statement batch; run=None → no ingest context (old path)."""
        token = O.STATEMENT_RUN_REFS.set(run) if run is not None else None
        try:
            return EX.execute_mail_tool("insert_batch_transactions", {
                "transactions": [dict(r) for r in rows], "bank": bank, "source": "Statement",
                "gmail_message_id": gid, "file_id": f"mail_pipeline_u_{bank}_{gid}"},
                "u", transaction_service=self.ts)
        finally:
            if token is not None:
                O.STATEMENT_RUN_REFS.reset(token)

    def count(self):
        self.s.expire_all()
        return self.s.query(Transactions).count()

    def test_reingest_under_corrected_bank_adds_nothing(self):
        first = self.ingest("BOI", [COFFEE, COFFEE2, RENT], run=set())
        self.assertEqual(first["inserted"], 3)            # two identical coffees are legit
        again = self.ingest("BOB", [COFFEE, COFFEE2, RENT], run=set())
        self.assertEqual((again["inserted"], again["duplicates"]), (0, 3))
        self.assertEqual(self.count(), 3)

    def test_without_the_dedup_the_ledger_would_double(self):
        # The pre-fix behaviour, reproduced with the ingest context unset.
        self.ingest("BOI", [COFFEE, RENT], run=set())
        self.ingest("BOB", [COFFEE, RENT])                 # no context → old path
        self.assertEqual(self.count(), 4)

    def test_new_rows_on_reingest_still_land(self):
        self.ingest("BOI", [COFFEE], run=set())
        out = self.ingest("BOB", [COFFEE, RENT], run=set())
        self.assertEqual((out["inserted"], out["duplicates"]), (1, 1))
        self.assertEqual(self.count(), 2)

    def test_partial_earlier_ingest_is_completed_not_doubled(self):
        self.ingest("BOI", [COFFEE], run=set())            # one of two coffees landed
        out = self.ingest("BOB", [COFFEE, COFFEE2], run=set())
        self.assertEqual(out["inserted"], 1)
        self.assertEqual(self.count(), 2)

    def test_same_run_chunks_never_dedup_each_other(self):
        run = set()
        self.ingest("BOB", [COFFEE], run=run)              # chunk 1
        out = self.ingest("BOB", [dict(COFFEE, line_position=7)], run=run)   # chunk 2, same tuple
        self.assertEqual(out["inserted"], 1)
        self.assertEqual(self.count(), 2)

    def test_same_bank_reingest_unchanged(self):
        self.ingest("BOB", [COFFEE, RENT], run=set())
        out = self.ingest("BOB", [COFFEE, RENT], run=set())
        self.assertEqual((out["inserted"], self.count()), (0, 2))

    def test_after_fix_linked_bank_rewrite_still_no_duplicates(self):
        self.ingest("BOI", [COFFEE, RENT], run=set())
        for t in self.s.query(Transactions).all():          # what --fix-linked does
            t.bank = "BOB"
        self.s.commit()
        out = self.ingest("BOB", [COFFEE, RENT], run=set())
        self.assertEqual((out["inserted"], self.count()), (0, 2))

    def test_consumed_earlier_row_is_not_matched_again_by_a_later_chunk(self):
        # Review MAJOR (2nd round): an earlier partial run landed ONE coffee;
        # the re-ingest's chunk 1 coffee is that copy (skipped), chunk 2's
        # coffee is the genuine second one and must land.
        self.ingest("BOI", [COFFEE], run=set())
        run = set()
        c1 = self.ingest("BOB", [COFFEE], run=run)
        c2 = self.ingest("BOB", [COFFEE2], run=run)
        self.assertEqual((c1["inserted"], c2["inserted"]), (0, 1))
        self.assertEqual(self.count(), 2)

    def test_two_earlier_rows_absorb_exactly_two_across_chunks(self):
        self.ingest("BOI", [COFFEE, COFFEE2], run=set())
        run = set()
        outs = [self.ingest("BOB", [dict(COFFEE, line_position=n)], run=run)["inserted"]
                for n in (1, 3, 5)]
        self.assertEqual(outs, [0, 0, 1])                  # a third coffee is new
        self.assertEqual(self.count(), 3)

    def test_earlier_rows_found_by_anchored_file_id_without_file_details(self):
        # Review minor: rows linked only by the ingest fileID shape
        # mail_pipeline_<user>_<BANK>_<gmail> (no fileDetails row) still count.
        self.ingest("BOI", [COFFEE, RENT], run=set())
        self.s.query(FileDetails).delete()
        self.s.commit()
        out = self.ingest("BOB", [COFFEE, RENT], run=set())
        self.assertEqual((out["inserted"], self.count()), (0, 2))

    def test_anchored_file_id_does_not_match_a_suffix_gmail_id(self):
        self.ingest("BOI", [COFFEE], gid="xg1", run=set())
        self.s.query(FileDetails).delete()
        self.s.commit()
        out = self.ingest("BOB", [COFFEE], gid="g1", run=set())
        self.assertEqual(out["inserted"], 1)

    def test_result_does_not_depend_on_row_order(self):
        # ak-d0mc (2): the earlier run landed only the line-3 coffee. The
        # re-ingest's line-3 coffee is that row (PK path); its line-1 coffee
        # is genuinely new. In one pass, listing line 1 first let it take
        # line 3's earlier row: both were dropped and the ledger kept 1.
        for order in ([COFFEE, COFFEE2], [COFFEE2, COFFEE]):
            with self.subTest(first=order[0]["line_position"]):
                self.s.query(Transactions).delete()
                self.s.query(FileDetails).delete()
                self.s.commit()
                self.ingest("BOB", [COFFEE2], run=set())
                out = self.ingest("BOB", order, run=set())
                self.assertEqual((out["inserted"], self.count()), (1, 2))

    # ak-d0mc review: the reviewer's scenario table as regression tests
    # (attachments concept dropped; one run set per ingest call group).
    A = {"date": "22/11/2026", "amount": 50, "description": "UPI TEA",
         "bank_reference_id": None, "line_position": 10}

    def test_s2_boi_to_bob_adds_nothing(self):
        self.ingest("BOI", [COFFEE, COFFEE2, RENT], run=set())
        self.ingest("BOB", [COFFEE, COFFEE2, RENT], run=set())
        self.assertEqual(self.count(), 3)

    def test_s3_one_earlier_two_incoming_across_chunks(self):
        self.ingest("BOI", [COFFEE], run=set())
        run = set()
        self.ingest("BOB", [COFFEE], run=run)
        self.ingest("BOB", [COFFEE2], run=run)
        self.assertEqual(self.count(), 2)

    def test_s4_two_identical_first_ingest_then_bank_change(self):
        self.ingest("BOB", [COFFEE, COFFEE2], run=set())
        self.assertEqual(self.count(), 2)
        self.ingest("BOI", [COFFEE, COFFEE2], run=set())
        self.assertEqual(self.count(), 2)

    def test_s5_third_ingest_after_a_mixed_bank_reingest(self):
        self.ingest("BOI", [COFFEE], run=set())
        self.ingest("BOB", [COFFEE, RENT], run=set())          # coffee deduped, rent new
        self.assertEqual(self.count(), 2)
        self.ingest("BOB", [COFFEE, RENT], run=set())          # third ingest: nothing new
        self.assertEqual(self.count(), 2)

    def test_s7_cross_chunk_order(self):
        # earlier {A'} only; chunk 1 = [A] (new hash), chunk 2 = [A'] (PK).
        # Chunk 1 can't know A' is coming, so A is first taken as a copy of
        # A'; chunk 2's PK-path A' owns that earlier row, so A is released
        # and inserted there. No bank-column rule (breaks --fix-linked).
        a2 = dict(self.A, line_position=11)
        self.ingest("BOB", [a2], run=set())
        run = set()
        self.assertEqual(self.ingest("BOB", [self.A], run=run)["inserted"], 0)
        self.assertEqual(self.ingest("BOB", [a2], run=run)["inserted"], 1)
        self.assertEqual(self.count(), 2)

    def test_s7_released_row_takes_another_free_earlier_row(self):
        # earlier {A' under BOI, A'' under BOB}; chunk 1 [A] takes one; when
        # chunk 2's PK row claims A'', A moves to the remaining free slot.
        a2, a3 = dict(self.A, line_position=11), dict(self.A, line_position=12)
        self.ingest("BOI", [a2], run=set())
        self.ingest("BOB", [a3])                   # no ingest context: both earlier rows exist
        self.assertEqual(self.count(), 2)
        run = set()
        self.ingest("BOB", [self.A], run=run)
        self.ingest("BOB", [a3], run=run)
        self.assertEqual(self.count(), 2)          # A = the BOI copy; a3 = itself

    def banks(self):
        self.s.expire_all()
        return sorted(t.bank for t in self.s.query(Transactions).all())

    def _two_bank_release(self):
        """Multi-PDF email (SBI savings + SBI_CARD card), identical coffee on
        both. Earlier run landed only SBI's line-3 coffee. Re-ingest: the
        card's coffee (call 1) is first taken as a copy of it; SBI's PK row
        (call 2) owns it, so the card's coffee is released, and it belongs
        to ANOTHER bank than the releasing call."""
        self.ingest("SBI", [COFFEE2], run=set())
        run = set()
        self.assertEqual(self.ingest("SBI_CARD", [COFFEE], run=run)["inserted"], 0)
        self.assertEqual(self.ingest("SBI", [COFFEE2], run=run)["inserted"], 0)
        return run

    def test_other_bank_release_is_inserted_at_run_end(self):
        # ak-d0mc review minor 1: never dropped with only a warning.
        run = self._two_bank_release()
        self.assertEqual(self.count(), 1)                  # pending, not lost
        landed = {"rows": 0, "recorded": 0}
        t1, t2 = O.STATEMENT_RUN_REFS.set(run), O.LANDED_ROWS.set(landed)
        try:
            self.assertEqual(EX.flush_pending_releases(self.ts, "u"), 1)
            self.assertEqual(EX.flush_pending_releases(self.ts, "u"), 0)   # once only
        finally:
            O.LANDED_ROWS.reset(t2)
            O.STATEMENT_RUN_REFS.reset(t1)
        self.assertEqual(self.banks(), ["SBI", "SBI_CARD"])  # nothing lost, own bank
        self.assertEqual(landed["rows"], 1)
        self.s.expire_all()
        card = self.s.query(Transactions).filter_by(bank="SBI_CARD").one()
        self.assertEqual((str(card.date)[:10], Decimal(str(card.amount))),
                         ("2026-11-20", Decimal("100")))   # JSON round-trip inserted fine

    def test_other_bank_release_is_inserted_by_that_banks_next_batch(self):
        run = self._two_bank_release()
        out = self.ingest("SBI_CARD", [RENT], run=run)     # the card's next chunk
        self.assertEqual(out["inserted"], 2)               # rent + the released coffee
        self.assertEqual(self.banks(), ["SBI", "SBI_CARD", "SBI_CARD"])
        t1 = O.STATEMENT_RUN_REFS.set(run)
        try:
            self.assertEqual(EX.flush_pending_releases(self.ts, "u"), 0)
        finally:
            O.STATEMENT_RUN_REFS.reset(t1)

    def test_flush_failure_fails_the_email_not_silent(self):
        from services import mailProcessorService as MPS
        run = self._two_bank_release()
        svc = MPS.MailProcessorService.__new__(MPS.MailProcessorService)
        svc.logger = mock.MagicMock()
        svc.transaction_service = mock.MagicMock()
        svc.transaction_service.insertTransactions.side_effect = RuntimeError("db down")
        t1 = O.STATEMENT_RUN_REFS.set(run)
        try:
            self.assertEqual(svc._flush_released_rows("u"), [O.INGEST_INCOMPLETE])
        finally:
            O.STATEMENT_RUN_REFS.reset(t1)
        self.assertTrue(svc.logger.error.called)

    def test_service_flushes_at_run_end(self):
        src = open(os.path.join(HERE, "services", "mailProcessorService.py")).read()
        self.assertIn("failures = list(failures) + self._flush_released_rows(user_id)", src)
        self.assertIn("if self._flush_released_rows(user_id):", src)

    def test_other_statement_is_not_matched(self):
        self.ingest("BOI", [COFFEE], gid="g1", run=set())
        out = self.ingest("BOB", [COFFEE], gid="g2", run=set())   # a different statement
        self.assertEqual(out["inserted"], 1)

    def test_service_sets_context_per_email_and_full_reprocess_only(self):
        src = open(os.path.join(HERE, "services", "mailProcessorService.py")).read()
        self.assertIn("_run_refs_token = ingest_outcome.STATEMENT_RUN_REFS.set(set())", src)
        self.assertIn("set() if not only_chunks else None)", src)


@unittest.skipUnless(_HAVE, "app deps not installed")
class TestBackfillReport(unittest.TestCase):

    def setUp(self):
        eng = sa.create_engine("sqlite://")
        for t in (FileDetails.__table__, Transactions.__table__, ProcessedEmails.__table__,
                  StatementPeriod.__table__):
            t.create(eng)
        self.s = sessionmaker(bind=eng)()
        spec = importlib.util.spec_from_file_location(
            "zejx_script2", os.path.join(HERE, "scripts", "zejx_statement_bank_backfill.py"))
        self.script = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.script)

    def _txn(self, ref, fid, bank="BOI"):
        self.s.add(Transactions(referenceID=ref, date=dt.datetime(2026, 3, 1), details="d",
                                amount=Decimal("1.00"), fileID=fid, source="Statement",
                                bank=bank, user="u_1"))

    def test_reingest_risk_reported_for_non_processed(self):
        for gid, status in (("g1", "failed"), ("g2", "processed")):
            self.s.add(ProcessedEmails(gmail_id=gid, user_id="u_1", sender="x@bankofbaroda.com",
                                       category="bank_statement", status=status,
                                       extraction_summary={"bank": "BOI"}))
            self._txn(f"r_{gid}", f"mail_pipeline_u_1_BOI_{gid}")
        self.s.commit()
        plan = self.script.build_plan(self.s)
        self.assertEqual(len(plan.changes), 2)
        self.assertEqual(plan.reingest_risk, [("g1", 1)])

    def test_linkage_is_anchored(self):
        self._txn("a", "mail_pipeline_u_1_BOI_g1")          # exact shape → linked
        self._txn("b", "mail_pipeline_u_1_BOI_xg1")         # gid suffix lookalike → not linked
        self._txn("c", "other_prefix_u_1_BOI_g1")           # different shape → not linked
        self._txn("d", "mail_pipeline_uX1_BOI_g1")          # '_' in the user id is escaped
        self.s.commit()
        _files, txns = self.script._linked(self.s, "u_1", "g1")
        self.assertEqual({t.referenceID for t in txns}, {"a"})


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
