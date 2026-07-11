"""ak-wty regression tests — SDK retry policy.

Pure-Python coverage of the retry-decision helpers in
utils/sdk_retry.py. The service-layer integration
(services/mailProcessorService.py) exercises the same helpers in an
async loop; env-limited on the worktree Python (no SDK / no flask).
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.sdk_retry import (
    MAX_RETRIES,
    is_retryable_sdk_error,
    retry_delay_seconds,
)


class TestIsRetryableSignatures(unittest.TestCase):
    """Known-transient error signatures return True."""

    def test_fatal_error_message_reader(self):
        """F12 canonical signature — the whole reason ak-wty exists."""
        self.assertTrue(
            is_retryable_sdk_error("Fatal error in message reader")
        )

    def test_case_insensitive(self):
        for form in (
            "FATAL ERROR IN MESSAGE READER",
            "fatal error in message reader",
            "Fatal Error In Message Reader",
        ):
            with self.subTest(form=form):
                self.assertTrue(is_retryable_sdk_error(form))

    def test_broader_message_reader_match(self):
        self.assertTrue(
            is_retryable_sdk_error("message reader interrupted")
        )

    def test_stream_error(self):
        self.assertTrue(is_retryable_sdk_error("stream reset by server"))

    def test_connection_error(self):
        self.assertTrue(
            is_retryable_sdk_error("connection reset by peer")
        )

    def test_timeout_error(self):
        self.assertTrue(is_retryable_sdk_error("read timed out"))
        self.assertTrue(is_retryable_sdk_error("request timeout"))
        self.assertTrue(is_retryable_sdk_error("deadline exceeded"))

    def test_rate_limit(self):
        self.assertTrue(is_retryable_sdk_error("rate_limit exceeded"))
        self.assertTrue(is_retryable_sdk_error("Rate Limit reached"))

    def test_5xx_wrapper(self):
        self.assertTrue(is_retryable_sdk_error("500 server error"))
        self.assertTrue(is_retryable_sdk_error("internal error"))
        self.assertTrue(
            is_retryable_sdk_error("temporarily unavailable")
        )

    def test_stream_eof(self):
        self.assertTrue(is_retryable_sdk_error("unexpected EOF"))
        self.assertTrue(is_retryable_sdk_error("broken pipe"))


class TestIsRetryableNonRetryable(unittest.TestCase):
    """Terminal error signatures return False — a retry would just
    fail again and burn time."""

    def test_schema_validation(self):
        """Structured-output schema validation failures don't retry —
        the LLM's response doesn't match the JSON schema and a
        second call likely produces the same shape."""
        self.assertFalse(
            is_retryable_sdk_error("schema validation failed")
        )

    def test_auth_401(self):
        self.assertFalse(
            is_retryable_sdk_error("401 unauthorized")
        )

    def test_auth_403(self):
        self.assertFalse(is_retryable_sdk_error("403 forbidden"))

    def test_invalid_request(self):
        self.assertFalse(
            is_retryable_sdk_error("invalid_request bad model")
        )

    def test_authentication_failure(self):
        self.assertFalse(
            is_retryable_sdk_error("authentication error")
        )

    def test_permission_denied(self):
        self.assertFalse(
            is_retryable_sdk_error("permission denied on model")
        )

    def test_non_retryable_takes_priority_over_retryable(self):
        """If a message contains BOTH retryable and non-retryable
        signatures, the non-retryable path wins — safer default."""
        self.assertFalse(
            is_retryable_sdk_error(
                "connection: schema validation failed"
            )
        )


class TestIsRetryableFalseInputs(unittest.TestCase):
    """Non-string / None / empty inputs return False."""

    def test_none_returns_false(self):
        self.assertFalse(is_retryable_sdk_error(None))

    def test_empty_string_returns_false(self):
        self.assertFalse(is_retryable_sdk_error(""))

    def test_whitespace_only_returns_false(self):
        self.assertFalse(is_retryable_sdk_error("   "))

    def test_non_string_returns_false(self):
        self.assertFalse(is_retryable_sdk_error(500))
        self.assertFalse(is_retryable_sdk_error(["stream", "error"]))
        self.assertFalse(is_retryable_sdk_error({"error": "stream"}))


class TestRetryDelay(unittest.TestCase):
    """Exponential backoff, capped."""

    def test_first_retry_is_1s(self):
        self.assertEqual(retry_delay_seconds(0), 1.0)

    def test_second_retry_is_2s(self):
        self.assertEqual(retry_delay_seconds(1), 2.0)

    def test_third_retry_is_4s(self):
        self.assertEqual(retry_delay_seconds(2), 4.0)

    def test_capped_at_default(self):
        # 2^20 = 1048576 — capped to default 30s.
        self.assertEqual(retry_delay_seconds(20), 30.0)

    def test_negative_attempt_treated_as_zero(self):
        self.assertEqual(retry_delay_seconds(-1), 1.0)

    def test_custom_base_and_cap(self):
        # base=0.5 → 0.5, 1.0, 2.0
        self.assertEqual(retry_delay_seconds(0, base=0.5), 0.5)
        self.assertEqual(retry_delay_seconds(1, base=0.5), 1.0)
        self.assertEqual(retry_delay_seconds(2, base=0.5), 2.0)
        # cap=1.5 → clip at 1.5
        self.assertEqual(retry_delay_seconds(10, base=1.0, cap=1.5), 1.5)


class TestMaxRetriesConstant(unittest.TestCase):
    """MAX_RETRIES defines the total retry ceiling."""

    def test_max_retries_is_3(self):
        """3 retries = 4 total attempts. Higher would hang the
        pipeline on genuinely-broken chunks; lower would leak legit
        transient failures."""
        self.assertEqual(MAX_RETRIES, 3)


if __name__ == "__main__":
    print("ak-wty SDK retry policy regression tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
