"""ak-iove: SSE keep-alive heartbeat wrapper.

The `/agent/chat` endpoint streams SSE events out of a synchronous
generator (`agentService.stream_chat`). During LLM inference / tool
execution the generator can go silent for tens of seconds — Anthropic's
SDK does a blocking `anyio.run(run_query)` inside the generator and
only yields once the whole exchange returns.

iOS 26 CriOS (152.0.7977.64+) reaps `fetch()` streams on ANY silent
gap of a few seconds after headers-received. ak-7gs (Aug 2 2026) fixed
the class via the `@microsoft/fetch-event-source` polyfill on the FE
but that's insufficient on iOS 26 — the reaping happens BELOW the
polyfill's control (in the browser's fetch layer).

Server-side keep-alive comment frames (`: keep-alive\n\n`) are the
industry-standard fix used by every major SSE-over-POST service
(OpenAI streaming, Anthropic streaming, GitHub Actions logs, etc.).
Any line starting with `:` is a SSE COMMENT — the polyfill's parser
explicitly discards them (no `onmessage` fires) — but the underlying
TCP flush prevents iOS from reaping the connection during silent gaps.

## Design: producer thread + queue + timeout-gated consumer

Python generators don't have a "peek with timeout" primitive, so we
run the inner generator in a daemon thread that pushes events onto a
`queue.Queue`. The consumer pulls from the queue with a timeout — when
the timeout expires (real generator was silent for `interval_sec`),
we emit a heartbeat and go back to waiting. When the producer finishes
(or errors), it pushes a sentinel so the consumer terminates cleanly.

Behavior:
  * During active streaming (events flowing regularly): no heartbeats
    — the consumer's `queue.get(timeout=N)` returns real events
    immediately, timeout never expires. Zero wire cost when idle.
  * During silent gaps (LLM inference, tool execution): heartbeats
    every `interval_sec`. Cost is ~15 bytes per heartbeat.
  * Producer exception: caller can wrap `producer_factory` to convert
    exceptions to error events before they raise. The wrapper only
    guarantees clean termination (sentinel is always sent).

## Testability

The wrapper is a pure function of (producer_factory, interval, heartbeat).
No Flask context, no request object — production code injects
`copy_current_request_context` into the producer_factory. Unit tests
just pass a plain callable.
"""

from __future__ import annotations

import queue
import threading
from typing import Callable, Iterator


# Standard SSE comment frame. Any line starting with `:` is a comment
# per the SSE spec (WHATWG § Server-sent events). Polyfill parsers
# discard comment lines; browsers ignore them; nginx / envoy pass
# them through untouched.
HEARTBEAT_BYTES: bytes = b": keep-alive\n\n"

# 5 seconds is well under iOS 26's reap threshold (~a few seconds) and
# well above any realistic per-event latency on the SDK's happy path.
# Cost per active chat: 3 bytes/s continuous during LLM inference
# (heartbeats every 5s × 15 bytes = 3 B/s).
DEFAULT_HEARTBEAT_INTERVAL_SEC: float = 5.0

# ak-iove v2 MINOR: bound the producer→consumer queue so a stalled
# consumer (client disconnected but Flask hasn't torn down the
# generator yet) applies natural backpressure to the producer thread
# rather than letting it enqueue unboundedly. 64 events is plenty for
# any realistic LLM stream — SDK events are milliseconds apart when
# real ones fire.
DEFAULT_QUEUE_MAXSIZE: int = 64


class _StreamEnded:
    """Sentinel type for the producer→consumer channel. Signals that
    the inner generator has finished (normally or via exception) and
    the consumer should terminate."""
    __slots__ = ()


_STREAM_ENDED = _StreamEnded()


def wrap_with_heartbeats(
    producer_factory: Callable[[], Iterator],
    *,
    interval_sec: float = DEFAULT_HEARTBEAT_INTERVAL_SEC,
    heartbeat_bytes: bytes = HEARTBEAT_BYTES,
    on_producer_error: Callable[[BaseException], Iterator] | None = None,
    producer_wrapper: Callable[[Callable], Callable] | None = None,
    queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
) -> Iterator:
    """Wrap an inner SSE generator with keep-alive heartbeats.

    Args:
        producer_factory: A callable returning the inner generator. The
            factory is invoked INSIDE the producer thread. If the inner
            generator needs Flask (or any other) request/thread-local
            context, use `producer_wrapper` — NOT the factory itself.
        interval_sec: Heartbeat interval. Defaults to 5 seconds.
        heartbeat_bytes: Bytes to emit on each heartbeat. Defaults to
            the standard `: keep-alive\\n\\n` comment frame.
        on_producer_error: Optional callable that receives the producer
            exception and returns an iterator of events to yield before
            the sentinel. Used to convert producer crashes into SSE
            error events the FE renders. If None, exceptions from the
            producer are silently swallowed after the sentinel is sent.
        producer_wrapper: Optional callable that wraps the ENTIRE
            producer-thread body — factory invocation + iteration +
            error-handler + sentinel-put. Typical use:
            `flask.copy_current_request_context`. This is the ONLY
            correct place to preserve Flask's request context into the
            producer thread — wrapping the factory itself is a known
            Flask 3.x anti-pattern that leaves the iteration outside
            the context and crashes on the first `g` touch inside the
            SDK's tool executor. Verified empirically on Flask 3.1.3
            (ak-iove v2 reviewer finding). MUST be called from within
            the active request context so it can capture correctly.
        queue_maxsize: Bound on the producer→consumer channel. If the
            consumer stalls (client disconnected but WSGI hasn't torn
            down the generator yet), the producer thread blocks at
            `put()` — natural backpressure. Daemon thread doesn't
            block process exit. Default 64.

    Yields:
        Real events from the inner generator, interleaved with
        heartbeat bytes during silent gaps. Terminates when the inner
        generator finishes (or crashes).

    Thread safety:
        Producer thread is daemon=True so process exit is unblocked
        even if the inner generator hangs. Queue is thread-safe.
        Consumer runs in the caller's thread (usually WSGI worker).
    """
    q: queue.Queue = queue.Queue(maxsize=queue_maxsize)

    def _producer_body():
        try:
            for event in producer_factory():
                q.put(event)
        except BaseException as exc:
            # Producer crashed. If caller supplied a converter, yield
            # its events through the queue before the sentinel. If not,
            # sentinel-only.
            if on_producer_error is not None:
                try:
                    for event in on_producer_error(exc):
                        q.put(event)
                except BaseException:
                    # Error converter itself crashed — nothing we can
                    # do; ensure sentinel still fires.
                    pass
        finally:
            q.put(_STREAM_ENDED)

    # ── ak-iove v2 CRITICAL fix ────────────────────────────────────────
    # `producer_wrapper` (typically `flask.copy_current_request_context`)
    # wraps the ENTIRE thread body so any context captured by the
    # wrapper spans factory invocation + full iteration + error handler
    # + sentinel-put. Wrapping the factory-that-returns-a-generator (v1
    # shape) was insufficient — the `with ctx:` block exited when the
    # factory returned, leaving iteration outside the context.
    # Reviewer's empirical Flask 3.1.3 repro:
    #   * v1: `[b"event1", b"CRASH: RuntimeError: Working outside of
    #           application context."]`
    #   * v2: `[b"event1", b"event2", ..., b"final"]`
    if producer_wrapper is not None:
        thread_target = producer_wrapper(_producer_body)
    else:
        thread_target = _producer_body

    thread = threading.Thread(
        target=thread_target,
        name="sse-heartbeat-producer",
        daemon=True,
    )
    thread.start()

    while True:
        try:
            item = q.get(timeout=interval_sec)
        except queue.Empty:
            # Silent gap — emit heartbeat + go back to waiting.
            yield heartbeat_bytes
            continue
        if isinstance(item, _StreamEnded):
            return
        yield item
