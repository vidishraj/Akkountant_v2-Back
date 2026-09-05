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
) -> Iterator:
    """Wrap an inner SSE generator with keep-alive heartbeats.

    Args:
        producer_factory: A callable returning the inner generator. In
            production this is wrapped with `copy_current_request_context`
            so the producer thread inherits Flask's request/g context.
        interval_sec: Heartbeat interval. Defaults to 5 seconds.
        heartbeat_bytes: Bytes to emit on each heartbeat. Defaults to
            the standard `: keep-alive\\n\\n` comment frame.
        on_producer_error: Optional callable that receives the producer
            exception and returns an iterator of events to yield before
            the sentinel. Used to convert producer crashes into SSE
            error events the FE renders. If None, exceptions from the
            producer are silently swallowed after the sentinel is sent
            (caller should log via a wrapper).

    Yields:
        Real events from the inner generator, interleaved with
        heartbeat bytes during silent gaps. Terminates when the inner
        generator finishes (or crashes).

    Thread safety:
        Producer thread is daemon=True so process exit is unblocked
        even if the inner generator hangs. Queue is thread-safe.
        Consumer runs in the caller's thread (usually WSGI worker).
    """
    q: queue.Queue = queue.Queue()

    def _producer_wrapper():
        try:
            for event in producer_factory():
                q.put(event)
        except BaseException as exc:
            # Producer crashed. If caller supplied a converter, yield
            # its events through the queue before the sentinel. If not,
            # sentinel-only (caller loses the exception — that's why
            # `on_producer_error` exists).
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

    thread = threading.Thread(
        target=_producer_wrapper,
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
