"""ak-ouu2 SECURITY: no exception text to clients or logs on the
remaining shared paths.

  * @Logger.standardLogger (every decorated endpoint): generic 500 body,
    log = exception type + traceback frames, never the message
  * strip_non_savings lookup / query errors: type only
  * mail pipeline summary errors (returned to the client): type only
  * the pipeline resolves stored-password banks with the ONE shared rule
    (bankFormatRules.statement_password_banks), same as /statementPasswords
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

SECRET = "S3cr3t-ouu2-[parameters: ('BOI', 'pw')]"


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _src(rel):
    with open(os.path.join(HERE, rel)) as f:
        return f.read()


class TestStandardLogger(unittest.TestCase):
    def setUp(self):
        try:
            import flask
            self.L = _load("utils/logger.py", "_akouu2_logger")
        except Exception as e:  # pragma: no cover
            self.skipTest(f"flask unavailable: {e}")
        self.app = flask.Flask("akouu2")

    def _ctrl(self):
        L = self.L

        class C:
            logger = mock.MagicMock()

            @L.Logger.standardLogger
            def boom(self):
                raise ValueError(SECRET)

            @L.Logger.standardLogger
            def ok(self):
                return "fine"
        return C()

    def test_generic_body_and_type_only_log(self):
        c = self._ctrl()
        with self.app.app_context():
            resp, status = c.boom()
        body = resp.get_data(as_text=True)
        self.assertEqual(status, 500)
        self.assertEqual(resp.get_json(), {"Error": "Internal server error"})
        self.assertNotIn("S3cr3t", body)
        logs = repr(c.logger.mock_calls)
        self.assertNotIn("S3cr3t", logs)
        self.assertIn("ValueError", logs)
        self.assertIn("boom", logs)          # traceback frames kept for debugging

    def test_happy_path_unchanged(self):
        c = self._ctrl()
        self.assertEqual(c.ok(), "fine")


class TestSourceGuards(unittest.TestCase):
    LEAK = re.compile(r"\{e\}|\{e!r\}|\{exc\}|str\(e\)|repr\(e\)")

    def _body(self, src, fn):
        m = re.search(rf"\n(\s*)def {fn}\(.*?(?=\n\1(?:def |@)|\Z)", src, re.DOTALL)
        self.assertIsNotNone(m, fn)
        return re.sub(r"#[^\n]*", "", m.group(0))

    def test_strip_service_has_no_exception_text(self):
        body = self._body(_src("services/transactionsService.py"),
                          "strip_non_savings_from_fallback_file")
        self.assertIsNone(self.LEAK.search(body))

    def test_pipeline_summary_errors_type_only(self):
        src = _src("services/mailProcessorService.py")
        self.assertNotIn('summary["errors"].append(str(e))', src)
        self.assertNotIn('f"Mail processing pipeline error: {e}"', src)

    def test_logger_has_no_exception_text(self):
        body = self._body(_src("utils/logger.py"), "standardLogger")
        self.assertIsNone(self.LEAK.search(body))
        self.assertNotIn("e.__str__()", body)

    def test_plaintext_column_documented(self):
        self.assertIn("PLAINTEXT", _src("models/statementPasswords.py"))


class TestOneBankResolutionRule(unittest.TestCase):
    def setUp(self):
        try:
            self.M = _load("services/mailProcessorService.py", "_akouu2_mps")
        except Exception as e:  # pragma: no cover
            self.skipTest(f"mailProcessorService import failed: {e}")
        self.bfr = _load("services/bankFormatRules.py", "_akouu2_bfr")

    def _queried_banks(self, sender):
        svc = self.M.MailProcessorService.__new__(self.M.MailProcessorService)
        svc.logger = mock.MagicMock()
        svc.transaction_service = mock.MagicMock()
        q = svc.transaction_service.db.session.query.return_value
        q.filter_by.return_value.first.return_value = None
        with mock.patch.object(svc, "_try_personal_info_passwords", return_value=None):
            svc._get_statement_password("u1", {"sender": sender}, pdf_path=None)
        return [c.kwargs["bank"] for c in q.filter_by.call_args_list]

    def test_pipeline_matches_shared_rule(self):
        for sender, domain in (
            ("HDFC <alerts@hdfcbank.net>", "hdfcbank.net"),
            ("HDFC <a@alerts.hdfcbank.bank.in>", "alerts.hdfcbank.bank.in"),
            ("BOI <n@alerts.bankofindia.bank.in>", "alerts.bankofindia.bank.in"),
            ("SBI <s@sbi.co.in>", "sbi.co.in"),
            ("x <x@evil.example.com>", "evil.example.com"),
        ):
            self.assertEqual(self._queried_banks(sender),
                             self.bfr.statement_password_banks(domain), sender)
        self.assertEqual(self._queried_banks("HDFC <alerts@hdfcbank.net>"),
                         ["Millenia_Credit", "HDFC_DEBIT", "HDFC_REGALIA"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
