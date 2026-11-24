"""ak-67q4 SECURITY: password-touching error paths must not echo the
exception text.

A SQLAlchemy error's str() carries the bound parameters, so
`jsonify({"error": str(e)})` in setOptedBanks (and its siblings)
reflected statement-PDF passwords back to the client and into the
logs. Each test makes the dependency raise an exception whose message
contains a secret, then asserts the secret is in neither the response
body nor any logger call, and that the log still names the exception
type.
"""
from __future__ import annotations

import importlib.util
import os
import re
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SECRET = "S3cr3t-PDF-pw-67q4"


class _BoomError(Exception):
    pass


def _boom(*_a, **_k):
    raise _BoomError(
        f"(IntegrityError) [parameters: ('HDFC_DEBIT', '{SECRET}', 'uid1')]"
    )


def _load(rel, name):
    # Load from file under a private name: other test files stub entries
    # in sys.modules (see ak-qkii), so a plain import is order-dependent.
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _calls_text(m):
    return repr(m.mock_calls)


class TestControllerErrorsDoNotEcho(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import flask  # noqa: F401
            cls.ep = _load("controllers/transactionsEP.py", "_ak67q4_transactionsEP")
        except Exception as e:  # pragma: no cover - env without deps
            raise unittest.SkipTest(f"controller import failed: {e}")
        cls.app = flask.Flask("ak67q4")

    def _ctrl(self):
        svc = mock.MagicMock()
        mp = mock.MagicMock()
        c = self.ep.TransactionController(svc, mail_processor=mp)
        c.logger = mock.MagicMock()
        return c, svc, mp

    def _call(self, ctrl, method, body, db=None):
        import flask
        with self.app.test_request_context("/", method="POST", json=body):
            flask.g.firebase_id = "uid1"
            if db is not None:
                flask.g.db = db
            resp = getattr(ctrl, method)()
        if isinstance(resp, tuple):
            r, status = resp[0], resp[1]
        else:
            r, status = resp, resp.status_code
        return r.get_data(as_text=True), status

    def _assert_clean(self, body, status, ctrl):
        self.assertEqual(status, 500)
        self.assertNotIn(SECRET, body)
        self.assertNotIn("parameters", body)
        logs = _calls_text(ctrl.logger)
        self.assertNotIn(SECRET, logs)
        self.assertIn("_BoomError", logs)

    def test_set_opted_banks(self):
        c, svc, _ = self._ctrl()
        svc.setOptedBanks.side_effect = _boom
        body, status = self._call(c, "setOptedBanks", {"banks": {"HDFC_DEBIT": SECRET}})
        self._assert_clean(body, status, c)

    def test_update_personal_info(self):
        c, _, _ = self._ctrl()
        db = mock.MagicMock()
        # Update path (existing row) so commit() is what raises.
        db.session.query.return_value.filter_by.return_value.first.return_value = mock.MagicMock()
        db.session.commit.side_effect = _boom
        body, status = self._call(c, "updatePersonalInfo", {"pan_number": SECRET}, db=db)
        self._assert_clean(body, status, c)
        db.session.rollback.assert_called_once()

    def test_reprocess_pdf(self):
        c, _, mp = self._ctrl()
        mp.reprocess_pdf.side_effect = _boom
        body, status = self._call(c, "reprocessPdf", {"gmail_id": "g1", "password": SECRET})
        self._assert_clean(body, status, c)

    def test_strip_fallback_rows(self):
        c, svc, _ = self._ctrl()
        svc.strip_non_savings_from_fallback_file.side_effect = _boom
        body, status = self._call(c, "stripFallbackRows", {"fileId": "f1", "password": SECRET})
        self._assert_clean(body, status, c)


class TestUnlockHelpersDoNotEcho(unittest.TestCase):
    def test_try_unlock_pdf_logs_type_only(self):
        try:
            pu = _load("utils/PdfPasswordUtil.py", "_ak67q4_PdfPasswordUtil")
        except Exception as e:  # pragma: no cover
            self.skipTest(f"PdfPasswordUtil import failed: {e}")
        log = mock.MagicMock()
        with mock.patch.object(pu, "logger", log), \
                mock.patch.object(pu.fitz, "open", side_effect=_boom):
            self.assertIsNone(pu.try_unlock_pdf("/nonexistent.pdf", [SECRET]))
        self.assertNotIn(SECRET, _calls_text(log))
        self.assertIn("_BoomError", _calls_text(log))

    def test_pre_unlock_pdf_logs_type_only(self):
        try:
            mps = _load("services/mailProcessorService.py", "_ak67q4_mailProcessorService")
        except Exception as e:  # pragma: no cover
            self.skipTest(f"mailProcessorService import failed: {e}")
        fake = mock.MagicMock()
        with mock.patch.object(mps.fitz, "open", side_effect=_boom):
            out = mps.MailProcessorService._pre_unlock_pdf(fake, "/x.pdf", SECRET)
        self.assertIsNone(out)
        self.assertNotIn(SECRET, _calls_text(fake.logger))
        self.assertIn("_BoomError", _calls_text(fake.logger))


class TestNoExceptionTextInPasswordPaths(unittest.TestCase):
    """Source guard for paths not exercised above: no `{e}` / `str(e)`
    in the swept password-touching functions."""

    SWEPT = {
        "services/mailProcessorService.py": [
            "reprocess_pdf", "_get_smart_statement_password",
            "_get_statement_password", "_try_personal_info_passwords",
            "_pre_unlock_pdf",
        ],
        "utils/PdfPasswordUtil.py": ["try_unlock_pdf"],
        "controllers/transactionsEP.py": [
            "setOptedBanks", "updatePersonalInfo", "reprocessPdf",
            "stripFallbackRows",
        ],
    }

    def _body(self, src, fn):
        m = re.search(
            rf"\n(\s*)def {fn}\(.*?(?=\n\1(?:def |@)|\Z)", src, re.DOTALL,
        )
        self.assertIsNotNone(m, fn)
        return m.group(0)

    def test_no_exception_text(self):
        leak = re.compile(r"\{e\}|\{e!r\}|\{exc\}|str\(e\)|repr\(e\)")
        for rel, fns in self.SWEPT.items():
            with open(os.path.join(HERE, rel)) as f:
                src = f.read()
            for fn in fns:
                body = re.sub(r"#[^\n]*", "", self._body(src, fn))  # ignore comments
                self.assertIsNone(
                    leak.search(body), f"{rel}:{fn} still formats the exception text",
                )


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
