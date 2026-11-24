"""ak-95w0: the last raw-text paths.

  * sdk_runner's CLI stderr logger keeps diagnostic tokens only
    (request id, HTTP status, closed-set marker, length), never the line
  * the agent chat SSE `error` event carries a fixed message + the
    closed-set code, never `SDK exception: {e}` / ResultMessage.result
  * agentService exception logging (conversation persistence, attachment
    cleanup, the stream_chat catch-all) and agentEP's producer crash log
    type + label + frames, never the message
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

from utils.sdk_retry import sdk_error_code, sdk_error_user_message  # noqa: E402

SECRET = "S3cr3t-95w0 SALARY CREDIT 98,765.00 from statement"


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _src(rel):
    with open(os.path.join(HERE, rel)) as f:
        return re.sub(r"#[^\n]*", "", f.read())


class TestFriendlyMessage(unittest.TestCase):
    def test_mapping_is_fixed_text(self):
        cases = {
            sdk_error_code("result_error", "error", "rate limit reached " + SECRET):
                "The assistant is busy right now. Please try again in a minute.",
            sdk_error_code("result_error", "error", "Overloaded"):
                "The assistant is busy right now. Please try again in a minute.",
            sdk_error_code("sdk_exception", "ProcessError", "Stream closed"):
                "The assistant hit a temporary problem. Please try again.",
            sdk_error_code("result_error", "error_max_turns", "Reached maximum number of turns"):
                "The assistant ran out of steps for this request. Try a narrower question.",
            sdk_error_code("assistant_error", "authentication_failed", "authentication_failed"):
                "The assistant couldn't authenticate. Check the server's assistant credentials.",
            sdk_error_code("result_error", "error", SECRET):
                "The assistant ran into an error. Please try again.",
        }
        for code, text in cases.items():
            self.assertEqual(sdk_error_user_message(code), text, code)
            self.assertNotIn("S3cr3t", sdk_error_user_message(code))
        self.assertEqual(sdk_error_user_message(None), "The assistant ran into an error. Please try again.")


class TestStderrLogger(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.R = _load("utils/sdk_runner.py", "_ak95w0_sdk_runner")
        except Exception as e:  # pragma: no cover
            raise unittest.SkipTest(f"sdk_runner import failed: {e}")

    def _log(self, line):
        with mock.patch.object(self.R, "_logger") as lg:
            self.R._make_sdk_stderr_logger("mail.pdf")(line)
        (msg,), _ = lg.info.call_args
        return msg

    def test_tokens_only(self):
        msg = self._log(f"API Error: status 529 overloaded req_011CUAbcDEF123xyz {SECRET} amount 500")
        self.assertNotIn("S3cr3t", msg)
        self.assertNotIn("SALARY", msg)
        self.assertIn("agent=mail.pdf", msg)
        self.assertIn("marker=overloaded", msg)
        self.assertIn("request_id=req_011CUAbcDEF123xyz", msg)
        self.assertIn("http_status=529", msg)                   # not the '500' amount
        self.assertRegex(msg, r"len=\d+")

    def test_plain_line(self):
        msg = self._log("Prompt echo: " + SECRET)
        self.assertNotIn("S3cr3t", msg)
        self.assertIn("marker=none request_id=- http_status=-", msg)

    def test_never_raises(self):
        with mock.patch.object(self.R, "_logger") as lg:
            lg.info.side_effect = RuntimeError("log down")
            self.R._make_sdk_stderr_logger("x")("line")          # swallowed


class TestAgentServiceLogging(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.A = _load("services/agentService.py", "_ak95w0_agentService")
        except Exception as e:  # pragma: no cover
            raise unittest.SkipTest(f"agentService import failed: {e}")

    def test_log_exc_type_and_frames_only(self):
        svc = self.A.AgentService.__new__(self.A.AgentService)
        svc.logger = mock.MagicMock()
        try:
            raise RuntimeError(f"(IntegrityError) [parameters: ('{SECRET}',)]")
        except Exception:
            svc._log_exc("append user message failed for conv=%s", 7)
        (msg,), _ = svc.logger.error.call_args
        self.assertIn("append user message failed for conv=7", msg)
        self.assertIn("RuntimeError[", msg)
        self.assertIn("test_log_exc_type_and_frames_only", msg)   # frames kept
        self.assertNotIn("S3cr3t", msg)


class TestSourceGuards(unittest.TestCase):
    def test_agent_service_chat_paths(self):
        src = _src("services/agentService.py")
        self.assertNotIn('f"SDK exception: {e}"', src)
        self.assertNotIn('f"Claude error: {message.error}"', src)
        self.assertNotIn('message.result or "Query failed"', src)
        self.assertNotIn('{"message": str(e)}', src)
        self.assertNotIn("self.logger.exception(", src)
        self.assertIn('"message": sdk_error_user_message(error_msg)', src)

    def test_agent_ep_producer_crash(self):
        src = _src("controllers/agentEP.py")
        m = re.search(r"def _producer_error_to_sse\(.*?(?=\n        def )", src, re.DOTALL)
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertNotRegex(body, r"\{exc\}|str\(exc\)|logger\.exception")

    def test_stderr_logger_does_not_log_line(self):
        src = _src("utils/sdk_runner.py")
        self.assertNotIn("line={line", src)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
