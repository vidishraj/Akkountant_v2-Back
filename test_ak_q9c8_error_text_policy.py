"""ak-q9c8: ONE error-text policy across transactionsEP and
mailProcessorService (extends ak-67q4 / ak-ouu2).

  * client-facing / persisted text: fixed message + exception TYPE
  * logs: type + traceback frames; never `{e}`, `str(e)`, `exc_info=True`
"""
from __future__ import annotations

import importlib.util
import io
import os
import re
import sys
import tokenize
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils.safe_errors import error_frames, error_label, error_tag, error_type  # noqa: E402
from utils.reprocess_status import _classify_reason  # noqa: E402

SECRET = "S3cr3t-q9c8 [parameters: ('BOI', 'pw', 'PAN123')]"


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _code_only(rel):
    """Source with comments and docstrings/plain strings removed, but
    f-strings kept (that is where `{e}` would be formatted)."""
    with open(os.path.join(HERE, rel)) as f:
        src = f.read()
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            continue
        if tok.type == tokenize.STRING and not re.match(r"^[rbuRBU]*[fF]", tok.string):
            continue
        out.append(tok.string)
    return " ".join(out)


class TestHelper(unittest.TestCase):
    def test_type_and_frames_without_message(self):
        def inner():
            raise ValueError(SECRET)
        try:
            inner()
        except ValueError as e:
            self.assertEqual(error_type(e), "ValueError")
            frames = error_frames(e)
        self.assertIn("inner", frames)
        self.assertNotIn("S3cr3t", frames)


def _raised(exc, cause=None):
    try:
        if cause is not None:
            raise exc from cause
        raise exc
    except BaseException as caught:
        return caught


class TestIncidentLabelsSurvive(unittest.TestCase):
    """Review MAJOR: classification happens server-side BEFORE the
    message is dropped; only the closed-set label is kept."""

    def _cases(self):
        try:
            from sqlalchemy.exc import OperationalError
            mysql = OperationalError("SELECT 1", {"pw": SECRET},
                                     Exception("(1040, 'Too many connections')"))
        except Exception:  # pragma: no cover
            mysql = RuntimeError("(1040, 'Too many connections')")
        return [
            (_raised(mysql), "mysql_1040"),
            (_raised(RuntimeError("Fatal error in message reader")), "sdk_message_reader"),
            (_raised(ValueError("document is encrypted; password required")), "pdf_password"),
            (_raised(ConnectionError("connection reset by peer")), "stream"),
            (_raised(RuntimeError("rate_limit exceeded")), "rate_limit"),
            (_raised(KeyError("x")), "unknown"),
        ]

    def test_label_from_message_server_side(self):
        for exc, label in self._cases():
            self.assertEqual(error_label(exc), label, repr(type(exc)))

    def test_tag_carries_label_not_message(self):
        for exc, label in self._cases():
            tag = error_tag(exc)
            self.assertEqual(tag, f"{type(exc).__name__}[{label}]")
            self.assertNotIn("S3cr3t", tag)
            self.assertNotIn("Too many", tag)

    def test_reason_strings_reclassify_to_the_same_label(self):
        # The exact reason shapes mailProcessorService persists.
        mysql, reader, enc = (c for c, _ in self._cases()[:3])
        self.assertEqual(_classify_reason(f"analysis_exception: {error_tag(mysql)}"), "mysql_1040")
        self.assertEqual(_classify_reason(f"top_level_exception: {error_tag(reader)}"), "sdk_message_reader")
        self.assertEqual(_classify_reason(f"pdf_open_failed: {error_tag(enc)}"), "pdf_password")
        # No label -> old prefix behaviour unchanged.
        self.assertEqual(_classify_reason(f"pdf_open_failed: {error_tag(_raised(KeyError('x')))}"), "pdf_open")
        self.assertEqual(_classify_reason(f"analysis_exception: {error_tag(_raised(KeyError('x')))}"),
                         "analysis_exception")
        # Unknown bracket content is ignored, not trusted.
        self.assertEqual(_classify_reason("analysis_exception: X[bogus]"), "analysis_exception")

    def test_label_found_through_cause_chain(self):
        try:
            from sqlalchemy.exc import OperationalError
            inner = OperationalError("SELECT 1", {}, Exception("(1040, 'Too many connections')"))
        except Exception:  # pragma: no cover
            inner = RuntimeError("1040")
        outer = _raised(RuntimeError("wrapped"), cause=_raised(inner))
        self.assertEqual(error_label(outer), "mysql_1040")

    def test_frames_walk_the_chain_without_messages(self):
        outer = _raised(RuntimeError("outer " + SECRET), cause=_raised(ValueError("inner " + SECRET)))
        frames = error_frames(outer)
        self.assertIn("caused by / during ValueError", frames)
        self.assertNotIn("S3cr3t", frames)


class TestPolicySourceGuard(unittest.TestCase):
    LEAK = re.compile(r"\{e\}|\{e!r\}|\{exc\}|str\(\s*e\s*\)|repr\(\s*e\s*\)|e\.__str__\(\)|exc_info\s*=\s*True")

    def test_no_exception_text_anywhere_in_either_file(self):
        for rel in ("controllers/transactionsEP.py", "services/mailProcessorService.py"):
            hits = self.LEAK.findall(_code_only(rel))
            self.assertEqual(hits, [], f"{rel} still formats exception text: {hits}")

    def test_reason_prefixes_kept(self):
        src = _code_only("services/mailProcessorService.py")
        for prefix in ("pdf_open_failed: ", "analysis_exception: ", "top_level_exception: "):
            self.assertIn(prefix, src)


class TestEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import flask
            cls.ep = _load("controllers/transactionsEP.py", "_akq9c8_transactionsEP")
        except Exception as e:  # pragma: no cover
            raise unittest.SkipTest(f"controller import failed: {e}")
        cls.app = flask.Flask("akq9c8")

    def _ctrl(self):
        svc = mock.MagicMock()
        c = self.ep.TransactionController(svc)
        c.logger = mock.MagicMock()
        return c, svc

    def _assert_clean(self, resp, status, c):
        body = resp.get_data(as_text=True)
        self.assertEqual(status, 500)
        self.assertNotIn("S3cr3t", body)
        self.assertIn("RuntimeError", body)
        self.assertNotIn("S3cr3t", repr(c.logger.mock_calls))

    def test_delete_local_statement(self):
        import flask
        c, svc = self._ctrl()
        svc.deleteLocalStatement.side_effect = RuntimeError(SECRET)
        with self.app.test_request_context("/?fileId=local_BOI_s.pdf"):
            flask.g.firebase_id = "u1"
            resp, status = c.deleteFile.__wrapped__(c)
        self._assert_clean(resp, status, c)

    def test_download_local_statement(self):
        import flask
        c, svc = self._ctrl()
        with mock.patch.object(c, "downloadLocalStatement_internal", side_effect=RuntimeError(SECRET)):
            with self.app.test_request_context("/?fileId=local_BOI_s.pdf"):
                flask.g.firebase_id = "u1"
                resp, status = c.downloadFile.__wrapped__(c)
        self._assert_clean(resp, status, c)

    def test_send_file_failure(self):
        c, svc = self._ctrl()
        svc.downloadLocalStatement.return_value = {"file_path": "/x.pdf", "filename": "x.pdf"}
        with mock.patch.object(self.ep, "send_file", side_effect=RuntimeError(SECRET), create=True), \
                mock.patch("flask.send_file", side_effect=RuntimeError(SECRET)):
            with self.app.test_request_context("/"):
                resp, status = c.downloadLocalStatement_internal("u1", "BOI", "x.pdf")
        self._assert_clean(resp, status, c)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
