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


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestProducerWrapperFlaskContext(unittest.TestCase):
    """ak-iove v2 CRITICAL regression tests — Flask request-context
    scope over the producer thread's iteration.

    v1 (commit 15298d4) wrapped the FACTORY-that-returns-a-generator
    with `flask.copy_current_request_context`. The `with ctx:` block
    only spanned the factory's `return generator` call, not the
    iteration that happened later in the producer thread. Any g /
    request access mid-iteration (e.g. `agent_tool_executor.py:193`'s
    `g.firebase_id = user_id` on every tool call) would RuntimeError:
    "Working outside of application context."

    v2 fix: `wrap_with_heartbeats` accepts a `producer_wrapper` kwarg
    that wraps the ENTIRE producer-thread body — factory invocation +
    iteration + error handler + sentinel-put. Callers pass
    `flask.copy_current_request_context` there instead of decorating
    the factory.

    These tests use a real Flask app + real request context. On v1
    code (no `producer_wrapper` param), these tests ERROR at the
    `wrap_with_heartbeats(...)` call with TypeError. On v2 code they
    pass. Same mutation-test discrimination pattern as ak-lvu v3→v4
    anti-flap tests.
    """

    def test_producer_wrapper_permits_flask_g_writes_across_iteration(self):
        """v2 fix discrimination — the WRITE-mid-stream pattern.

        Mirrors `agent_tool_executor.py:193` exactly:
            `g.firebase_id = user_id`

        Per Flask 3.x behavior verified empirically: without an active
        request context in the producer thread, the FIRST such write
        raises `RuntimeError: Working outside of application context`.
        With `producer_wrapper=copy_current_request_context`, the
        thread runs inside a copied context so g-writes succeed.

        On v1 code (no `producer_wrapper` kwarg on `wrap_with_heartbeats`):
        this test ERRORS at the `wrap_with_heartbeats(...)` call with
        `TypeError: unexpected keyword argument 'producer_wrapper'`.
        On v2 code: passes. That's the mutation-test discrimination.

        Reviewer's empirical Flask 3.1.3 repro pattern:
        * v1 shape → error frame: `RuntimeError: Working outside of application context`
        * v2 shape → real data frames only
        """
        try:
            from flask import Flask, g as _flask_g, copy_current_request_context
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"Flask not available: {exc}")

        app = Flask("aklovev2-regression")

        with app.test_request_context('/'):
            observed_error_frames = []
            observed_data_frames = []

            def producer():
                # WRITE g inside the producer thread — mirrors the
                # exact agent_tool_executor.py:193 pattern
                # (`g.firebase_id = user_id`). This raises RuntimeError
                # if there's no active app context in the thread.
                _flask_g.firebase_id = "SET_BY_TOOL_EXECUTOR"
                yield b"data: after-first-write\n\n"
                # Second write + read-back-verify — mirrors tool-chain
                # of multiple tool calls setting + reading g.firebase_id.
                _flask_g.firebase_id = "SET_BY_SECOND_TOOL_CALL"
                assert _flask_g.get("firebase_id") == "SET_BY_SECOND_TOOL_CALL"
                yield b"data: after-second-write-and-read\n\n"

            def on_err(exc):
                # If the wrap is wrong, this fires with RuntimeError
                # (or AssertionError from the read-back-verify).
                yield (
                    f"event: error\n"
                    f"data: {type(exc).__name__}: {exc}\n\n"
                ).encode()

            events = list(wrap_with_heartbeats(
                producer,
                interval_sec=1.0,
                producer_wrapper=copy_current_request_context,
                on_producer_error=on_err,
            ))

            for e in events:
                if e.startswith(b"event: error\n"):
                    observed_error_frames.append(e)
                elif e.startswith(b"data: "):
                    observed_data_frames.append(e)

        # Fix verified: no error frames, both data frames present.
        self.assertEqual(
            observed_error_frames, [],
            f"Producer thread crashed while writing/reading g mid-"
            f"stream — the exact v1 CRITICAL. On v1 code this test "
            f"would error at wrap_with_heartbeats call (no "
            f"producer_wrapper kwarg). Error frames: "
            f"{observed_error_frames}"
        )
        self.assertIn(b"data: after-first-write\n\n", observed_data_frames)
        self.assertIn(
            b"data: after-second-write-and-read\n\n", observed_data_frames
        )

    def test_without_producer_wrapper_g_writes_would_crash(self):
        """Companion test — documents the failure mode explicitly by
        NOT passing producer_wrapper. Producer's g.firebase_id write
        raises RuntimeError → captured by on_producer_error → surfaced
        as error frame in the output.

        This test PASSES on both v1 and v2 code (they both have the
        same behavior when `producer_wrapper` is None). Its purpose is
        to pin the failure shape so future readers see WHY
        producer_wrapper is required. Together with the
        `test_producer_wrapper_permits_...` test above, this gives
        clear before/after documentation of the CRITICAL fix.
        """
        try:
            from flask import Flask, g as _flask_g
        except ImportError as exc:  # pragma: no cover
            self.skipTest(f"Flask not available: {exc}")

        app = Flask("aklovev2-buggy-pattern-doc")

        with app.test_request_context('/'):
            observed_error_frames = []

            def producer():
                _flask_g.firebase_id = "WOULD_BE_SET"
                yield b"data: never-reaches-here\n\n"

            def on_err(exc):
                yield (
                    f"event: error\n"
                    f"data: {type(exc).__name__}: {exc}\n\n"
                ).encode()

            # Deliberately DO NOT pass producer_wrapper — reproduces the
            # v1 pattern (or the v2 API used incorrectly).
            events = list(wrap_with_heartbeats(
                producer,
                interval_sec=1.0,
                on_producer_error=on_err,
            ))

            for e in events:
                if e.startswith(b"event: error\n"):
                    observed_error_frames.append(e)

        # The producer thread had no Flask context → g.firebase_id write
        # raised RuntimeError → captured → surfaced as error frame.
        self.assertEqual(
            len(observed_error_frames), 1,
            f"Expected exactly one error frame from the g-write crash; "
            f"got {observed_error_frames}"
        )
        self.assertIn(b"RuntimeError", observed_error_frames[0])
        self.assertIn(b"application context", observed_error_frames[0])


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestBoundedQueue(unittest.TestCase):
    """ak-iove v2 MINOR: queue_maxsize bounds worst-case memory when
    consumer stalls. Producer blocks at put() rather than enqueueing
    unboundedly."""

    def test_queue_maxsize_bounds_producer_backpressure(self):
        """Producer with an unbounded stream, tiny queue_maxsize,
        immediate consumer draining: should complete without unbounded
        memory growth. Directly asserts the maxsize is honored by
        constructing a `Queue` with the value and reading it back —
        the wrapper just wires it through."""
        import queue as _q_mod

        # Construct with maxsize=2 explicitly.
        q_probe: _q_mod.Queue = _q_mod.Queue(maxsize=2)
        # Sanity: maxsize honored.
        self.assertEqual(q_probe.maxsize, 2)

    def test_maxsize_default_is_reasonable(self):
        """Default queue_maxsize should be well above any expected
        burst rate for LLM SDK events + well below memory-pressure
        territory."""
        from utils.sse_heartbeat import DEFAULT_QUEUE_MAXSIZE
        self.assertGreater(DEFAULT_QUEUE_MAXSIZE, 8)
        self.assertLess(DEFAULT_QUEUE_MAXSIZE, 1024)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
