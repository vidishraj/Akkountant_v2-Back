"""ak-iove: unit tests for utils.sse_heartbeat.wrap_with_heartbeats.

Pins the SSE keep-alive contract:
  * Heartbeats emit ONLY on silent gaps beyond the interval
  * No heartbeats when the inner generator is actively streaming
  * Sentinel terminates cleanly (normal or exception path)
  * Producer exception → on_producer_error yields events before sentinel

The wrapper is a pure function (no Flask, no logger) so this test runs
in bare env without any of the services chain. Same discipline as
test_money_utils / test_agent_file_attachment_sanitizer.

Run:
    python3 -m unittest test_sse_heartbeat
"""

from __future__ import annotations

import os
import sys
import time
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


try:
    from utils.sse_heartbeat import (
        wrap_with_heartbeats,
        HEARTBEAT_BYTES,
        DEFAULT_HEARTBEAT_INTERVAL_SEC,
    )
    _IMPORT_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _IMPORT_OK = False
    _SKIP_REASON = f"import chain unavailable: {_exc}"


class TestImportChannel(unittest.TestCase):
    """Un-decorated guard — fails LOUD if the pure-utils import breaks."""

    def test_import_channel_is_live(self):
        self.assertTrue(_IMPORT_OK, _SKIP_REASON)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestConstants(unittest.TestCase):
    """The heartbeat frame shape is contract with the FE polyfill.
    Regression guard so a future edit doesn't accidentally change the
    bytes to something the polyfill wouldn't discard."""

    def test_heartbeat_is_sse_comment_frame(self):
        # SSE spec: lines starting with `:` are comments. Two trailing
        # newlines end the frame. Polyfill parsers require the frame
        # boundary.
        self.assertTrue(HEARTBEAT_BYTES.startswith(b":"))
        self.assertTrue(HEARTBEAT_BYTES.endswith(b"\n\n"))
        # No `data:` prefix — comment frames must not carry data.
        self.assertNotIn(b"data:", HEARTBEAT_BYTES)

    def test_default_interval_reasonable(self):
        # < 10s so we're well under iOS 26's reap threshold (~few s),
        # > 1s so wire cost stays trivial.
        self.assertGreater(DEFAULT_HEARTBEAT_INTERVAL_SEC, 1.0)
        self.assertLess(DEFAULT_HEARTBEAT_INTERVAL_SEC, 10.0)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestActiveStreaming(unittest.TestCase):
    """Zero-heartbeat guarantee during active streaming — the wrapper
    must NOT interleave heartbeats when the inner generator is yielding
    events faster than the interval."""

    def test_empty_inner_terminates_without_heartbeat(self):
        """An inner generator that yields nothing and finishes
        immediately produces no heartbeats — sentinel arrives first."""
        def producer():
            return iter([])

        result = list(wrap_with_heartbeats(
            producer, interval_sec=1.0,
        ))
        self.assertEqual(result, [])

    def test_fast_inner_no_heartbeats(self):
        """Inner yields 3 events immediately, then finishes. No
        heartbeats should appear in the output."""
        def producer():
            return iter([b"data: a\n\n", b"data: b\n\n", b"data: c\n\n"])

        result = list(wrap_with_heartbeats(
            producer, interval_sec=1.0,
        ))
        self.assertEqual(
            result, [b"data: a\n\n", b"data: b\n\n", b"data: c\n\n"]
        )
        # Extra guard: no heartbeat bytes in the mix.
        self.assertNotIn(HEARTBEAT_BYTES, result)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestSilentGap(unittest.TestCase):
    """Heartbeat MUST emit during silent gaps beyond the interval.
    Uses a small interval (0.1s) + a slow-yield generator so tests
    stay fast."""

    def test_heartbeat_emitted_after_silent_gap(self):
        """Inner yields one event, then sleeps 0.35s, then yields the
        second event. With interval=0.1s the wrapper should emit 3
        heartbeats in the gap (at ~0.1s, ~0.2s, ~0.3s)."""
        def producer():
            yield b"data: first\n\n"
            time.sleep(0.35)
            yield b"data: second\n\n"

        interval = 0.1
        result = list(wrap_with_heartbeats(
            producer, interval_sec=interval,
        ))
        # Sequence: real, heartbeat*, real. Heartbeat count varies by
        # scheduler jitter — assert at least 2 heartbeats fired.
        self.assertEqual(result[0], b"data: first\n\n")
        self.assertEqual(result[-1], b"data: second\n\n")
        heartbeats = [r for r in result[1:-1] if r == HEARTBEAT_BYTES]
        self.assertGreaterEqual(
            len(heartbeats), 2,
            f"Expected at least 2 heartbeats during 0.35s gap at "
            f"interval={interval}s, got {len(heartbeats)}. Full result: "
            f"{result}"
        )
        # Everything between the two real events must be heartbeat frames.
        for r in result[1:-1]:
            self.assertEqual(
                r, HEARTBEAT_BYTES,
                f"Non-heartbeat frame appeared during silent gap: {r!r}"
            )

    def test_heartbeat_before_first_event_if_slow_start(self):
        """Inner sleeps BEFORE yielding — first bytes on the wire
        should be heartbeat frames, keeping the connection alive until
        the real stream produces its opening event."""
        def producer():
            time.sleep(0.25)
            yield b"data: opening\n\n"

        result = list(wrap_with_heartbeats(
            producer, interval_sec=0.1,
        ))
        # First items should be heartbeats.
        self.assertEqual(result[0], HEARTBEAT_BYTES)
        # The real event lands at the end.
        self.assertEqual(result[-1], b"data: opening\n\n")


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestProducerException(unittest.TestCase):
    """Producer exceptions must not deadlock the consumer or leak the
    producer thread. Sentinel is always sent in the `finally` block."""

    def test_producer_exception_terminates_cleanly(self):
        """Inner raises after one event. Without an error handler, we
        should see the one event then termination (no sentinel leak
        into the output)."""
        class _Boom(RuntimeError):
            pass

        def producer():
            yield b"data: before-boom\n\n"
            raise _Boom("kaboom")

        result = list(wrap_with_heartbeats(
            producer, interval_sec=1.0,
        ))
        self.assertEqual(result, [b"data: before-boom\n\n"])

    def test_producer_exception_with_error_handler(self):
        """When on_producer_error is provided, its yielded events land
        BEFORE termination."""
        def producer():
            yield b"data: pre\n\n"
            raise RuntimeError("test-crash")

        seen_exceptions = []

        def _err_to_sse(exc):
            seen_exceptions.append(exc)
            yield b'event: error\ndata: {"message":"crashed"}\n\n'

        result = list(wrap_with_heartbeats(
            producer,
            interval_sec=1.0,
            on_producer_error=_err_to_sse,
        ))
        self.assertEqual(len(seen_exceptions), 1)
        self.assertIsInstance(seen_exceptions[0], RuntimeError)
        self.assertEqual(
            result,
            [
                b"data: pre\n\n",
                b'event: error\ndata: {"message":"crashed"}\n\n',
            ]
        )

    def test_error_handler_itself_crashes_still_terminates(self):
        """Belt-and-suspenders: if the error handler ALSO raises, the
        wrapper still sends the sentinel + consumer terminates. No
        deadlock."""
        def producer():
            raise RuntimeError("outer")

        def _bad_handler(_exc):
            raise RuntimeError("handler-crashed")
            yield  # unreachable, makes it a generator

        result = list(wrap_with_heartbeats(
            producer,
            interval_sec=1.0,
            on_producer_error=_bad_handler,
        ))
        # No events, but no hang either — clean termination.
        self.assertEqual(result, [])


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestCustomHeartbeatBytes(unittest.TestCase):
    """The heartbeat bytes are overridable — useful if a caller wants
    a comment with a different label (e.g. `: ping\\n\\n`)."""

    def test_custom_heartbeat_bytes(self):
        def producer():
            time.sleep(0.15)
            yield b"data: x\n\n"

        result = list(wrap_with_heartbeats(
            producer,
            interval_sec=0.1,
            heartbeat_bytes=b": custom-ping\n\n",
        ))
        self.assertIn(b": custom-ping\n\n", result)
        self.assertNotIn(HEARTBEAT_BYTES, result)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
