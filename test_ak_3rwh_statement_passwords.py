"""ak-3rwh: GET /statementPasswords (status only) + PUT
/statementPasswords/<bank> (set/replace one bank, requeue that bank's
unlock_failed emails), behind utils/statement_password_store.

Invariants pinned here:
  * no response ever contains a stored password (or a mask/length of it)
  * the password is never passed to a logger
  * PUT requeues ONLY the rows whose sender maps to that bank
  * the pipeline's domain -> bank try-order is unchanged by the move
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SECRET = "S3cr3t-3rwh-pw"
HDFC = "HDFC Bank <alerts@hdfcbank.net>"
YES = "YES BANK <estatements@yesbank.in>"
BOI = "BOI <noreply@alerts.bankofindia.bank.in>"


def _load(rel, name):
    # From file under a private name: other test files stub
    # sys.modules["services.transactionsService"] (see ak-qkii).
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestSharedBankMap(unittest.TestCase):
    def setUp(self):
        self.bfr = _load("services/bankFormatRules.py", "_ak3rwh_bankFormatRules")

    def test_pipeline_order_preserved(self):
        f = self.bfr.statement_password_banks
        self.assertEqual(f("hdfcbank.net"), ["Millenia_Credit", "HDFC_DEBIT", "HDFC_REGALIA"])
        self.assertEqual(f("alerts.hdfcbank.bank.in"), ["Millenia_Credit", "HDFC_DEBIT", "HDFC_REGALIA"])
        self.assertEqual(f("yesbank.in"), ["YES_BANK_DEBIT", "YES_BANK_ACE"])
        self.assertEqual(f("alerts.bankofindia.bank.in"), ["BOI"])
        # No order hint -> shared DOMAIN_TO_BANK fallback (ak-iu8i).
        self.assertEqual(f("sbi.co.in"), ["SBI"])
        self.assertEqual(f("evil.example.com"), [])
        self.assertEqual(f(""), [])

    def test_returns_copies(self):
        got = self.bfr.statement_password_banks("hdfcbank.net")
        got.append("X")
        self.assertNotIn("X", self.bfr.STATEMENT_PASSWORD_DOMAIN_BANKS["hdfcbank.net"])


class _DB(unittest.TestCase):
    """Real SQLite tables + real TransactionService."""

    def setUp(self):
        try:
            import flask
            import sqlalchemy as sa
            from sqlalchemy.orm import sessionmaker
            import models  # noqa: F401
            from models import investmentHistory  # noqa: F401
            from models import User, StatementPasswords
            from models.processedEmails import ProcessedEmails
            self.TS = _load("services/transactionsService.py", "_ak3rwh_transactionsService")
            from utils import statement_password_store as store
        except Exception as e:  # pragma: no cover - env without deps
            self.skipTest(f"app deps unavailable: {e}")
        eng = sa.create_engine("sqlite://")
        for t in (User.__table__, StatementPasswords.__table__, ProcessedEmails.__table__):
            t.create(eng)
        self.session = sessionmaker(bind=eng)()
        self.User, self.SP, self.PE, self.store = User, StatementPasswords, ProcessedEmails, store
        self.app = flask.Flask("ak3rwh")
        self.ctx = self.app.test_request_context("/")
        self.ctx.push()
        flask.g.db = types.SimpleNamespace(session=self.session)
        self.session.add(User(userID="u1", optedBanks="HDFC_DEBIT,BOI"))
        self.session.commit()
        self.ts = self.TS.TransactionService()
        self.ts.logger = mock.MagicMock()

    def tearDown(self):
        self.ctx.pop()

    def _attention(self, gmail_id, sender, reason="unlock_failed"):
        from utils.statement_retry_cap import NEEDS_ATTENTION
        self.session.add(self.PE(
            gmail_id=gmail_id, user_id="u1", sender=sender, status=NEEDS_ATTENTION,
            processing_type="pdf",
            extraction_summary={"_attempts": {"reason": reason, "count": 3,
                                              "last_at": "2026-11-24T00:00:00Z"}},
        ))
        self.session.commit()

    def _status(self, gmail_id):
        self.session.expire_all()
        return self.session.query(self.PE).filter_by(gmail_id=gmail_id).one().status


class TestStore(_DB):
    def test_validate(self):
        v, E = self.store.validate_password, self.store.InvalidStatementPassword
        self.assertEqual(v(" a b "), " a b ")            # stored exactly as typed
        self.assertEqual(v("x" * 128), "x" * 128)
        for bad in ("", "x" * 129, None, 123, ["a"], "a\x00b"):
            with self.assertRaises(E):
                v(bad)
        try:
            v("x" * 129 + SECRET)
        except E as e:
            self.assertNotIn(SECRET, str(e))

    def test_set_then_replace_and_status(self):
        self.store.set_password(self.session, "u1", "BOI", "first")
        self.store.set_password(self.session, "u1", "BOI", SECRET)
        rows = self.session.query(self.SP).filter_by(user="u1").all()
        self.assertEqual([(r.bank, r.password_hash) for r in rows], [("BOI", SECRET)])
        self.session.add(self.SP(bank="SBI", password_hash="", user="u1"))
        self.session.commit()
        self.assertEqual(self.store.banks_with_password(self.session, "u1"), {"BOI"})


class TestStatusEndpointService(_DB):
    def test_status_shape_and_counts(self):
        self.store.set_password(self.session, "u1", "BOI", SECRET)
        self._attention("g_hdfc", HDFC)
        self._attention("g_yes", YES)
        self._attention("g_rows", HDFC, reason="no_rows")      # not an unlock failure
        out = self.ts.getStatementPasswordStatus("u1")
        by = {b["bank"]: b for b in out["banks"]}
        from enums.BanksEnum import BankEnums
        self.assertEqual(set(by), {b.value for b in BankEnums})
        self.assertEqual(by["BOI"], {"bank": "BOI", "opted": True, "hasPassword": True,
                                     "unlockFailedCount": 0, "needsPassword": False})
        self.assertEqual(by["HDFC_DEBIT"]["unlockFailedCount"], 1)
        self.assertTrue(by["HDFC_DEBIT"]["opted"])
        self.assertEqual(by["Millenia_Credit"]["unlockFailedCount"], 1)
        self.assertEqual(by["YES_BANK_ACE"]["unlockFailedCount"], 1)
        self.assertTrue(by["YES_BANK_DEBIT"]["needsPassword"])
        self.assertFalse(by["SBI"]["needsPassword"])
        self.assertNotIn(SECRET, json.dumps(out))
        for b in out["banks"]:
            self.assertEqual(set(b), {"bank", "opted", "hasPassword",
                                      "unlockFailedCount", "needsPassword"})


class TestSetEndpointService(_DB):
    def test_rejects_unknown_bank_and_bad_password(self):
        body, code = self.ts.setStatementPassword("u1", "NOPE", SECRET)
        self.assertEqual((code, body["error_code"]), (400, "UNKNOWN_BANK"))
        body, code = self.ts.setStatementPassword("u1", "BOI", "")
        self.assertEqual((code, body["error_code"]), (400, "INVALID_PASSWORD"))
        body, code = self.ts.setStatementPassword("u1", "BOI", SECRET + "x" * 200)
        self.assertEqual(code, 400)
        self.assertNotIn(SECRET, json.dumps(body))
        self.assertEqual(self.session.query(self.SP).count(), 0)

    def test_unknown_user_404(self):
        body, code = self.ts.setStatementPassword("ghost", "BOI", SECRET)
        self.assertEqual(code, 404)
        self.assertEqual(self.session.query(self.SP).count(), 0)

    def test_set_requeues_only_that_bank(self):
        self._attention("g_hdfc", HDFC)
        self._attention("g_yes", YES)
        self._attention("g_boi", BOI)
        body, code = self.ts.setStatementPassword("u1", "HDFC_DEBIT", SECRET)
        self.assertEqual(code, 200)
        self.assertEqual(body, {"bank": "HDFC_DEBIT", "hasPassword": True, "requeued": 1})
        self.assertEqual(self._status("g_hdfc"), "failed")
        self.assertEqual(self._status("g_yes"), "needs_attention")
        self.assertEqual(self._status("g_boi"), "needs_attention")
        self.assertNotIn(SECRET, json.dumps(body))
        self.assertNotIn(SECRET, repr(self.ts.logger.mock_calls))
        # Stored where the pipeline reads it today (_get_statement_password).
        row = self.session.query(self.SP).filter_by(user="u1", bank="HDFC_DEBIT").one()
        self.assertEqual(row.password_hash, SECRET)
        # optedBanks untouched (unlike legacy setOptedBanks).
        self.assertEqual(self.session.query(self.User).one().optedBanks, "HDFC_DEBIT,BOI")

    def test_unscoped_requeue_unchanged(self):
        self._attention("g_hdfc", HDFC)
        self._attention("g_yes", YES)
        self.assertEqual(self.ts.requeueUnlockFailed("u1"), 2)


class TestController(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import flask
            cls.ep = _load("controllers/transactionsEP.py", "_ak3rwh_transactionsEP")
        except Exception as e:  # pragma: no cover
            raise unittest.SkipTest(f"controller import failed: {e}")
        cls.app = flask.Flask("ak3rwh-ep")

    def _call(self, method, *args, json_body=None, uid="u1"):
        import flask
        svc = mock.MagicMock()
        c = self.ep.TransactionController(svc)
        c.logger = mock.MagicMock()
        self._svc, self._c = svc, c
        return svc, c, (json_body, uid, method, args)

    def _run(self, c, method, args, json_body, uid):
        import flask
        kw = {"method": "PUT", "json": json_body} if json_body is not None else {"method": "PUT", "data": "x"}
        with self.app.test_request_context("/", **kw):
            if uid:
                flask.g.firebase_id = uid
            r, status = getattr(c, method)(*args)
            return r.get_data(as_text=True), status

    def test_put_db_error_fixed_message(self):
        svc, c, _ = self._call("putStatementPassword")

        def boom(*a, **k):
            raise RuntimeError(f"(IntegrityError) [parameters: ('BOI', '{SECRET}', 'u1')]")
        svc.setStatementPassword.side_effect = boom
        body, status = self._run(c, "putStatementPassword", ("BOI",), {"password": SECRET}, "u1")
        self.assertEqual(status, 500)
        self.assertNotIn(SECRET, body)
        self.assertNotIn(SECRET, repr(c.logger.mock_calls))
        self.assertIn("RuntimeError", repr(c.logger.mock_calls))

    def test_put_requires_password_and_auth(self):
        svc, c, _ = self._call("putStatementPassword")
        body, status = self._run(c, "putStatementPassword", ("BOI",), {}, "u1")
        self.assertEqual(status, 400)
        body, status = self._run(c, "putStatementPassword", ("BOI",), None, "u1")
        self.assertEqual(status, 400)
        body, status = self._run(c, "putStatementPassword", ("BOI",), {"password": SECRET}, None)
        self.assertEqual(status, 401)
        svc.setStatementPassword.assert_not_called()

    def test_put_passes_through(self):
        svc, c, _ = self._call("putStatementPassword")
        svc.setStatementPassword.return_value = ({"bank": "BOI", "hasPassword": True, "requeued": 0}, 200)
        body, status = self._run(c, "putStatementPassword", ("BOI",), {"password": SECRET}, "u1")
        self.assertEqual(status, 200)
        svc.setStatementPassword.assert_called_once_with("u1", "BOI", SECRET)
        self.assertNotIn(SECRET, body)

    def test_get_error_fixed_message(self):
        svc, c, _ = self._call("getStatementPasswords")
        svc.getStatementPasswordStatus.side_effect = RuntimeError(SECRET)
        import flask
        with self.app.test_request_context("/"):
            flask.g.firebase_id = "u1"
            r, status = c.getStatementPasswords()
        self.assertEqual(status, 500)
        self.assertNotIn(SECRET, r.get_data(as_text=True))
        self.assertNotIn(SECRET, repr(c.logger.mock_calls))


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
