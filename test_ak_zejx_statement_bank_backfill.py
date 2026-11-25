"""ak-zejx (2026-11-24): backfill the bank id of existing statement rows.

Prod: 32/100 statements show bank 'bank_statement' (the category
fallback), incl. SBI / BoB / Paytm; 16 Bank of Baroda statements are
stored as BOI. The backfill re-derives the bank from the sender domain
(family) + the statement's own transactions (exact variant), touches only
mis-mapped rows, is idempotent, and dry-runs by default with counts only.
Real ProcessedEmails / FileDetails / Transactions tables on sqlite.
Synthetic data only.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import io
import os
import sys
import types
import unittest
from contextlib import contextmanager, redirect_stdout
from decimal import Decimal
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import statement_bank_backfill as SB  # noqa: E402

try:
    import sqlalchemy as sa
    from sqlalchemy.orm import sessionmaker
    import models  # noqa: F401
    from models import investmentHistory  # noqa: F401
    from models.processedEmails import ProcessedEmails
    from models.fileDetails import FileDetails
    from models.transactions import Transactions
    from models.statementPeriods import StatementPeriod
    _HAVE = True
except Exception:  # pragma: no cover
    _HAVE = False


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "zejx_script", os.path.join(HERE, "scripts", "zejx_statement_bank_backfill.py"))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class TestDecide(unittest.TestCase):

    def test_rules(self):
        cases = [
            # sender, current, linked txn banks -> (new, outcome)
            ("x@bankofbaroda.bank.in", "bank_statement", set(), ("BOB", SB.FIX)),
            ("x@bankofbaroda.com", "BOI", {"BOI"}, ("BOB", SB.FIX)),     # wrong family
            ("x@alerts.sbi.bank.in", "bank_statement", set(), ("SBI", SB.FIX)),
            ("x@paytmbank.com", "bank_statement", set(), ("PAYTM", SB.FIX)),
            ("x@hdfcbank.net", "bank_statement", {"Millenia_Credit"}, ("Millenia_Credit", SB.FIX)),
            ("x@hdfcbank.net", "bank_statement", set(), (None, SB.SKIP_AMBIGUOUS)),
            ("x@hdfcbank.net", "bank_statement", {"HDFC_DEBIT", "Millenia_Credit"},
             (None, SB.SKIP_AMBIGUOUS)),
            ("x@hdfcbank.net", "Millenia_Credit", set(), (None, SB.ALREADY_CORRECT)),
            ("x@yes.bank.in", "YES_BANK_ACE", set(), (None, SB.ALREADY_CORRECT)),
            ("x@yes.bank.in", "bank_statement", {"YES_BANK_DEBIT"}, ("YES_BANK_DEBIT", SB.FIX)),
            ("x@yesbank.email", "bank_statement", set(), (None, SB.SKIP_UNMAPPED_DOMAIN)),
            ("x@paytm.com", "bank_statement", set(), (None, SB.SKIP_UNMAPPED_DOMAIN)),
            ("HDFC Bank", "bank_statement", set(), (None, SB.SKIP_UNMAPPED_DOMAIN)),
        ]
        for sender, cur, linked, want in cases:
            self.assertEqual(SB.decide(sender, cur, linked), want, (sender, cur, linked))

    def test_current_bank_matches_the_statements_view_fallback(self):
        self.assertEqual(SB.current_bank_of(None, "bank_statement"), "bank_statement")
        self.assertEqual(SB.current_bank_of({"bank": "BOI"}, "bank_statement"), "BOI")
        self.assertEqual(SB.current_bank_of({}, None), "Unknown")


@unittest.skipUnless(_HAVE, "app deps not installed")
class TestBackfill(unittest.TestCase):

    def setUp(self):
        eng = sa.create_engine("sqlite://")
        for t in (FileDetails.__table__, Transactions.__table__, ProcessedEmails.__table__,
                  StatementPeriod.__table__):
            t.create(eng)
        self.s = sessionmaker(bind=eng)()
        self.script = _load_script()
        self.n = 0

    def email(self, gid, sender, bank=None, category="bank_statement", txn_banks=()):
        summary = {"bank": bank, "inserted": 1} if bank else None
        self.s.add(ProcessedEmails(gmail_id=gid, user_id="u", sender=sender, subject="SECRET SUBJECT",
                                   category=category, status="processed",
                                   processing_type="pdf", extraction_summary=summary))
        if txn_banks:
            fid = f"mail_pipeline_u_X_{gid}"
            self.s.add(FileDetails(fileID=fid, fileName="f.pdf", fileSize=1, statementCount=1,
                                   bank=list(txn_banks)[0], user="u", gmail_message_id=gid,
                                   uploadDate=dt.date(2026, 3, 1)))
            for b in txn_banks:
                self.n += 1
                self.s.add(Transactions(referenceID=f"r{self.n}", date=dt.datetime(2026, 3, 1),
                                        details="d", amount=Decimal("1.00"), fileID=fid,
                                        source="Statement", bank=b, user="u"))
        self.s.commit()

    def seed(self):
        self.email("bob_placeholder", "x@bankofbaroda.bank.in")
        self.email("bob_as_boi", "BoB <x@bankofbaroda.com>", bank="BOI", txn_banks=("BOI", "BOI"))
        self.email("sbi", "x@alerts.sbi.bank.in")
        self.email("hdfc_card", "x@hdfcbank.net", txn_banks=("Millenia_Credit",))
        self.email("hdfc_unknown", "x@hdfcbank.net")
        self.email("good", "x@hdfcbank.net", bank="HDFC_REGALIA")
        self.email("unmapped", "x@yesbank.email")
        self.email("alert", "x@bankofbaroda.com", category="transaction_alert")   # not a statement

    def test_plan_counts_and_no_content(self):
        self.seed()
        plan = self.script.build_plan(self.s)
        self.assertEqual(dict(plan.mapping), {
            ("bank_statement", "BOB"): 1, ("BOI", "BOB"): 1,
            ("bank_statement", "SBI"): 1, ("bank_statement", "Millenia_Credit"): 1})
        self.assertEqual(plan.outcomes[SB.SKIP_AMBIGUOUS], 1)
        self.assertEqual(plan.outcomes[SB.SKIP_UNMAPPED_DOMAIN], 1)
        self.assertEqual(plan.outcomes[SB.ALREADY_CORRECT], 1)
        text = "\n".join(plan.report_lines())
        self.assertNotIn("SECRET", text)
        self.assertNotIn("x@", text)                       # domains only, never addresses
        self.assertIn("yesbank.email", text)

    def test_apply_is_targeted_and_idempotent(self):
        self.seed()
        counts = self.script.apply_plan(self.s, self.script.build_plan(self.s), fix_linked=True)
        self.s.commit()
        self.assertEqual(counts, {"processedEmails": 4, "fileDetails": 1, "transactions": 2})
        rows = {r.gmail_id: r for r in self.s.query(ProcessedEmails).all()}
        self.assertEqual(rows["bob_as_boi"].extraction_summary, {"bank": "BOB", "inserted": 1})
        self.assertEqual(rows["sbi"].extraction_summary, {"bank": "SBI"})
        self.assertIsNone(rows["hdfc_unknown"].extraction_summary)          # ambiguous: untouched
        self.assertEqual(rows["good"].extraction_summary["bank"], "HDFC_REGALIA")
        self.assertIsNone(rows["alert"].extraction_summary)                 # other category untouched
        self.assertEqual({t.bank for t in self.s.query(Transactions).filter(
            Transactions.fileID.like("%bob_as_boi")).all()}, {"BOB"})
        self.assertEqual({t.bank for t in self.s.query(Transactions).filter(
            Transactions.fileID.like("%hdfc_card")).all()}, {"Millenia_Credit"})
        again = self.script.build_plan(self.s)                               # idempotent
        self.assertEqual(again.changes, [])

    def test_linked_rows_untouched_without_flag(self):
        self.seed()
        self.script.apply_plan(self.s, self.script.build_plan(self.s), fix_linked=False)
        self.s.commit()
        self.assertEqual({t.bank for t in self.s.query(Transactions).filter(
            Transactions.fileID.like("%bob_as_boi")).all()}, {"BOI"})

    @contextmanager
    def _fake_app(self):
        # ak-3oh review: the script opens a bare session (no app import);
        # tests hand it the sqlite session instead.
        modes = []

        def opener(read_only):
            modes.append(read_only)
            return self.s, (lambda: None)
        with mock.patch.object(self.script, "_open_session", opener), \
                mock.patch.object(self.s, "rollback", lambda: None):
            self.open_modes = modes
            yield

    def test_script_never_imports_app(self):
        src = open(self.script.__file__).read()
        self.assertNotIn("from app import", src)
        self.assertNotIn("import app", src)
        self.assertLess(src.index('os.environ["AK_SKIP_SCHEDULERS"] = "1"'),
                        src.index("from utils import statement_bank_backfill"))
        self.assertIn("SET SESSION TRANSACTION READ ONLY", src)

    def test_dry_run_opens_read_only_commit_does_not(self):
        self.seed()
        with self._fake_app(), redirect_stdout(io.StringIO()):
            self.script.main([])
            self.script.main(["--commit", "--assume-yes"])
        self.assertEqual(self.open_modes, [True, False])

    def test_main_dry_run_writes_nothing(self):
        self.seed()
        before = [(r.gmail_id, r.extraction_summary) for r in self.s.query(ProcessedEmails).all()]
        out = io.StringIO()
        with self._fake_app(), redirect_stdout(out):
            rc = self.script.main(["--fix-linked"])
        self.assertEqual(rc, 0)
        self.s.expire_all()
        self.assertEqual(before, [(r.gmail_id, r.extraction_summary)
                                  for r in self.s.query(ProcessedEmails).all()])
        text = out.getvalue()
        self.assertIn("mode=DRY-RUN", text)
        self.assertIn("BOI              -> BOB", text)
        self.assertIn("transactions=2", text)
        self.assertNotIn("SECRET", text)

    def test_main_commit_requires_assume_yes_then_applies(self):
        self.seed()
        with self._fake_app(), redirect_stdout(io.StringIO()):
            self.assertEqual(self.script.main(["--commit"]), 2)
            self.assertEqual(self.script.main(["--commit", "--assume-yes"]), 0)
        self.s.expire_all()
        self.assertEqual(self.s.query(ProcessedEmails).filter_by(gmail_id="sbi").one()
                         .extraction_summary, {"bank": "SBI"})


class TestImportLeaksNoEnv(unittest.TestCase):
    """Loading the script as a module (tests do) must not set the flag in
    the importing process; running it as __main__ still does."""

    def test_loading_module_does_not_set_flag(self):
        import importlib.util
        from unittest import mock
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AK_SKIP_SCHEDULERS", None)
            spec = importlib.util.spec_from_file_location("zejx_noleak", os.path.join(HERE, "scripts", "zejx_statement_bank_backfill.py"))
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            self.assertNotIn("AK_SKIP_SCHEDULERS", os.environ)
        src = open(os.path.join(HERE, "scripts", "zejx_statement_bank_backfill.py")).read()
        self.assertIn('if __name__ == "__main__":\n    os.environ["AK_SKIP_SCHEDULERS"] = "1"', src)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
