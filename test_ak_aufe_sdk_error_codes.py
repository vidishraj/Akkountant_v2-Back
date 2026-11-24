"""ak-aufe: SdkRunResult.error is a closed-set code, never raw SDK text.

  assistant_error:<enum>[marker] / result_error:<subtype>[marker] /
  sdk_exception:<Type>[marker]

The marker (rate_limit, overloaded, sdk_message_reader, stream, ...) is
classified from the raw text BEFORE it is dropped, so retry decisions
(is_retryable_sdk_error), the mail pipeline's `"rate_limit" in err`
check and reprocess_status labels keep working.
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils.sdk_retry import (  # noqa: E402
    NON_RETRYABLE_MARKERS, RETRYABLE_MARKERS, is_retryable_sdk_error,
    sdk_error_code, sdk_error_marker, sdk_error_marker_of,
)
from utils.reprocess_status import _classify_reason  # noqa: E402

SECRET = "S3cr3t-aufe statement line: SALARY CREDIT 1,23,456.00"


class TestCodes(unittest.TestCase):
    def test_shapes_and_no_raw_text(self):
        self.assertEqual(sdk_error_code("assistant_error", "rate_limit", "rate_limit"),
                         "assistant_error:rate_limit[rate_limit]")
        c = sdk_error_code("result_error", "error_during_execution", SECRET + " Overloaded")
        self.assertEqual(c, "result_error:error_during_execution[overloaded]")
        c = sdk_error_code("sdk_exception", "ProcessError", "Fatal error in message reader " + SECRET)
        self.assertEqual(c, "sdk_exception:ProcessError[sdk_message_reader]")
        self.assertEqual(sdk_error_code("result_error", "error", SECRET),
                         "result_error:error[none]")
        for code in (c, sdk_error_code("result_error", "x", SECRET)):
            self.assertNotIn("S3cr3t", code)
            self.assertNotIn("SALARY", code)

    def test_detail_must_be_a_token(self):
        self.assertEqual(sdk_error_code("result_error", "has spaces " + SECRET, None),
                         "result_error:other[none]")
        self.assertEqual(sdk_error_code("result_error", None, None), "result_error:other[none]")
        self.assertEqual(sdk_error_code("bogus_kind", "X", None), "sdk_exception:X[none]")

    def test_marker_priority_matches_legacy(self):
        # non-retryable wins, as in is_retryable_sdk_error
        self.assertEqual(sdk_error_marker("schema validation failed on stream"), "schema")
        self.assertEqual(sdk_error_marker("401 unauthorized"), "authentication")
        self.assertEqual(sdk_error_marker("Reached maximum number of turns (8)"), "max_turns")
        self.assertEqual(sdk_error_marker("read timed out"), "timeout")
        self.assertEqual(sdk_error_marker("500 server error"), "server_error")
        self.assertEqual(sdk_error_marker(None), "none")

    def test_retry_decisions_on_codes(self):
        for m in RETRYABLE_MARKERS:
            self.assertTrue(is_retryable_sdk_error(f"result_error:x[{m}]"), m)
        for m in NON_RETRYABLE_MARKERS | {"none"}:
            self.assertFalse(is_retryable_sdk_error(f"result_error:x[{m}]"), m)
        # Same decision as the legacy substring path on the raw text.
        for raw in ("Fatal error in message reader", "rate limit reached", "stream reset",
                    "connection reset by peer", "deadline exceeded", "internal error",
                    "broken pipe", "schema validation failed", "403 forbidden", "plain failure"):
            self.assertEqual(is_retryable_sdk_error(sdk_error_code("sdk_exception", "E", raw)),
                             is_retryable_sdk_error(raw), raw)
        # New: overloaded (529) is transient.
        self.assertTrue(is_retryable_sdk_error(sdk_error_code("result_error", "e", "Overloaded")))

    def test_downstream_label_and_rate_limit_checks(self):
        rl = sdk_error_code("result_error", "error", "rate_limit_error")
        self.assertIn("rate_limit", f"Claude error: {rl}")          # MPS text-batch check
        self.assertEqual(_classify_reason(f"Claude error: {rl}"), "rate_limit")
        mr = sdk_error_code("sdk_exception", "ProcessError", "Fatal error in message reader")
        self.assertEqual(_classify_reason(mr), "sdk_message_reader")


class TestReviewFixes(unittest.TestCase):
    """ak-aufe review: the MAJOR (stream-error rebuild) + retry delta + labels."""

    def test_type_name_never_drives_the_marker(self):
        start = sdk_error_code("sdk_exception", "CLIConnectionError",
                               "Failed to start Claude Code: not found")
        self.assertEqual(start, "sdk_exception:CLIConnectionError[not_found]")
        self.assertFalse(is_retryable_sdk_error(start))
        self.assertEqual(is_retryable_sdk_error(start),
                         is_retryable_sdk_error("SDK exception: Failed to start Claude Code: not found"))
        bare = sdk_error_code("sdk_exception", "CLIConnectionError", "Failed to start Claude Code")
        self.assertEqual(bare, "sdk_exception:CLIConnectionError[none]")
        self.assertFalse(is_retryable_sdk_error(bare))

    def test_marker_of(self):
        c = sdk_error_code("sdk_exception", "ProcessError", "Stream closed")
        self.assertEqual(sdk_error_marker_of(f"Claude error: {c}"), "stream")
        self.assertIsNone(sdk_error_marker_of("Claude error: rate_limit"))
        self.assertIsNone(sdk_error_marker_of(None))

    def test_new_labels_counted(self):
        self.assertEqual(_classify_reason("result_error:e[overloaded]"), "overloaded")
        self.assertEqual(_classify_reason("sdk_exception:X[connection]"), "connection")
        self.assertEqual(_classify_reason("connection reset by peer"), "stream")   # unchanged
        self.assertEqual(_classify_reason("too many connections"), "mysql_1040")   # unchanged


class TestStreamRebuildDecision(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.M = _load("services/mailProcessorService.py", "_akaufe_mps")
        except Exception as e:  # pragma: no cover
            raise unittest.SkipTest(f"mailProcessorService import failed: {e}")

    def _wrapped(self, kind, detail, raw):
        return f"Claude error: {sdk_error_code(kind, detail, raw)}"

    def test_stream_drops_still_rebuild(self):
        f = self.M.is_stream_error_text
        for kind, detail, raw in (
            ("sdk_exception", "ProcessError", "Fatal error in message reader: stream closed"),
            ("sdk_exception", "CLIConnectionError", "Stream closed"),
            ("sdk_exception", "BrokenPipeError", "[Errno 32] Broken pipe"),
            ("result_error", "error_during_execution", "connection reset by peer " + SECRET),
        ):
            self.assertTrue(f(self._wrapped(kind, detail, raw)), raw)

    def test_non_stream_does_not_rebuild(self):
        f = self.M.is_stream_error_text
        self.assertFalse(f(self._wrapped("result_error", "error", "rate limit reached")))
        self.assertFalse(f(self._wrapped("sdk_exception", "CLIConnectionError",
                                         "Failed to start Claude Code")))
        self.assertFalse(f("Claude error: rate_limit"))            # plain timeout string
        self.assertFalse(f(None))

    def test_agent_output_keywords_still_apply(self):
        f = self.M.is_stream_error_text
        self.assertTrue(f(None, "...the stream closed unexpectedly"))
        self.assertTrue(f("Claude error: stream error"))           # legacy plain string


class _Assistant:
    def __init__(self, error=None):
        self.error, self.content = error, []


class _Result:
    def __init__(self, result, subtype="error_during_execution"):
        self.is_error, self.result, self.subtype = True, result, subtype
        self.structured_output, self.usage = None, None


class TestRunQueryCollect(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.R = _load("utils/sdk_runner.py", "_akaufe_sdk_runner")
        except Exception as e:  # pragma: no cover
            raise unittest.SkipTest(f"sdk_runner import failed: {e}")

    def _run(self, gen_fn):
        R = self.R
        opts = mock.MagicMock(stderr=lambda _l: None, model="m")
        with mock.patch.object(R, "sdk_query", gen_fn), \
                mock.patch.object(R, "AssistantMessage", _Assistant), \
                mock.patch.object(R, "ResultMessage", _Result), \
                mock.patch.object(R, "init_tool_names", return_value=None), \
                mock.patch.object(R, "emit_agent_run") as emit:
            res = asyncio.run(R.run_query_collect(agent="t", options=opts, prompt="p"))
        self.assertNotIn("S3cr3t", repr(emit.call_args))
        return res

    def test_result_error_text_dropped(self):
        async def gen(prompt, options):
            yield _Result(SECRET + " rate limit hit")
        res = self._run(gen)
        self.assertEqual(res.error, "result_error:error_during_execution[rate_limit]")
        self.assertTrue(is_retryable_sdk_error(res.error))

    def test_assistant_error_enum(self):
        async def gen(prompt, options):
            yield _Assistant(error="authentication_failed")
        res = self._run(gen)
        self.assertEqual(res.error, "assistant_error:authentication_failed[authentication]")
        self.assertFalse(is_retryable_sdk_error(res.error))

    def test_exception_message_dropped(self):
        async def gen(prompt, options):
            raise RuntimeError("Fatal error in message reader: " + SECRET)
            yield  # pragma: no cover
        res = self._run(gen)
        self.assertEqual(res.error, "sdk_exception:RuntimeError[sdk_message_reader]")
        self.assertTrue(is_retryable_sdk_error(res.error))


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod          # dataclasses resolve via sys.modules
    spec.loader.exec_module(mod)
    return mod


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
