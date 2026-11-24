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
