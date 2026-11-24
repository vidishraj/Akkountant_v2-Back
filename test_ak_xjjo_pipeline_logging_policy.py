"""ak-xjjo: utils/pipeline_logging serialises exceptions under the ONE
error-text policy (utils/safe_errors, ak-q9c8): message = Type[label]
(label classified server-side), traceback = frames only, never the
exception text."""
from __future__ import annotations

import os
import re
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils.pipeline_logging import _serialize_exception, build_pipeline_event  # noqa: E402

SECRET = "S3cr3t-xjjo-PAN"


def _mysql_1040():
    try:
        from sqlalchemy.exc import OperationalError
        return OperationalError("INSERT ...", {"pan": SECRET},
                                Exception("(1040, 'Too many connections')"))
    except Exception:  # pragma: no cover
        return RuntimeError(f"(1040, 'Too many connections') {SECRET}")


def _raised(exc, cause=None):
    try:
        if cause is not None:
            raise exc from cause
        raise exc
    except BaseException as caught:
        return caught


class TestSerialize(unittest.TestCase):
    def test_none(self):
        self.assertEqual(_serialize_exception(None), (None, None))

    def test_no_exception_text_anywhere(self):
        e = _raised(_mysql_1040())
        payload = build_pipeline_event("chunk_error", file_id="f1", exception=e)
        self.assertEqual(payload["message"], f"{type(e).__name__}[mysql_1040]")
        self.assertEqual(payload["error_type"], "mysql_1040")
        blob = repr(payload)
        self.assertNotIn(SECRET, blob)
        self.assertNotIn("Too many connections", blob)

    def test_cause_chain_frames_and_label(self):
        e = _raised(RuntimeError("wrapper " + SECRET), cause=_raised(_mysql_1040()))
        payload = build_pipeline_event("chunk_error", exception=e)
        self.assertEqual(payload["error_type"], "mysql_1040")      # found via __cause__
        self.assertIn("caused by / during", payload["traceback"])
        self.assertNotIn(SECRET, repr(payload))

    def test_caller_message_and_error_type_take_precedence(self):
        e = _raised(_mysql_1040())
        p = build_pipeline_event("chunk_error", exception=e, message="rate_limit hit")
        self.assertEqual((p["message"], p["error_type"]), ("rate_limit hit", "rate_limit"))
        p = build_pipeline_event("chunk_error", exception=e, error_type="analysis_exception")
        self.assertEqual(p["error_type"], "analysis_exception")
        self.assertNotIn(SECRET, repr(p))

    def test_source_guard(self):
        with open(os.path.join(HERE, "utils", "pipeline_logging.py")) as f:
            src = re.sub(r"#[^\n]*", "", f.read())
        code = re.sub(r'"""[\s\S]*?"""', "", src)
        self.assertNotRegex(code, r"str\(\s*exc\s*\)|repr\(\s*exc\s*\)|format_exception\(")


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
