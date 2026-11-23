"""ak-vue6 Phase 1: Flask-side Kite LTP endpoints.

Two surfaces, both backed by the UNIX-socket IPC the
``kite_ws_daemon`` serves (``services/kite_ltp_ipc.py``):

  * ``GET /api/kite/ltp/<symbol>``      — single-symbol snapshot
  * ``GET /api/kite/stream/ltp``        — SSE stream (1 Hz
                                          deltas + reconnect-
                                          required signal)

Auth: g.firebase_id guaranteed set by the ak-c8l
before_request middleware. Single-user app today — the uid
is used for log-correlation only (the daemon's tick dict
holds market-wide data, not per-user).

Design notes
------------
* The daemon runs in a SEPARATE systemd unit. The Flask side
  is a lightweight client — nothing here imports KiteTicker /
  the WS code, so the dep chain for app.service stays flat.
* IPC errors (daemon down / booting) → 503 with a clear
  reason so the FE can render an "LTP temporarily
  unavailable" banner rather than a generic 500.
* SSE emits ``event: reconnect_required`` when the daemon
  reports that state — FE walks the same Kite reconnect flow
  as every other 401 response.
* SSE uses ``wrap_with_heartbeats`` (ak-iove) — keeps iOS 26
  / Safari / Chrome from reaping the stream during tick-less
  gaps + survives the 1Hz poll cadence without extra bytes.
"""
from __future__ import annotations

import json
import time
from typing import Iterator

from flask import (
    Response, copy_current_request_context, g, jsonify,
    stream_with_context,
)

from services.kite_ltp_ipc import (
    STATE_RECONNECT,
    KiteLtpClient,
    KiteLtpIpcError,
)
from utils.logger import Logger
from utils.sse_heartbeat import (
    DEFAULT_HEARTBEAT_INTERVAL_SEC,
    wrap_with_heartbeats,
)


# ak-vue6: SSE poll cadence against the daemon. Overseer spec
# called for 1Hz deltas on the FE; we poll the daemon at this
# interval and emit a delta event ONLY when the tick dict
# changed. State changes (reconnect_required) are emitted
# immediately on the next poll.
_SSE_POLL_INTERVAL_SECONDS = 1.0

# ak-vue6: safety upper bound on how long a single SSE
# connection stays open. Overseer didn't specify; 1 hour is
# a reasonable default so a client that silently walked off
# frees up server resources on a slow timeline. FE should
# reconnect transparently.
_SSE_MAX_DURATION_SECONDS = 3600


class KiteLtpController:
    """Thin Flask handlers over the ``KiteLtpClient``."""

    def __init__(self, ipc_client=None):
        # Allow injection for tests; prod uses the default
        # socket-path client.
        self._client = ipc_client if ipc_client is not None else KiteLtpClient()
        self.logger = Logger(__name__).get_logger()

    # ───── Single-symbol snapshot ──────────────────────────

    @Logger.standardLogger
    def getLtp(self, symbol):
        """GET /api/kite/ltp/<symbol> — latest LTP for one symbol.

        Returns:
          * 200 ``{"sym": ..., "ltp": ..., "ts": ...}`` — hit.
          * 400 — empty / missing symbol.
          * 404 ``{"error": "not_watched"}`` — symbol is not in
            the daemon's subscription set (holdings don't
            cover it). FE might fall back to a REST poll of
            /kite/holdings for coverage data.
          * 503 ``{"error": ...}`` — daemon unreachable
            (booting / crashed). systemd should restart within
            seconds; FE can retry.
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        if not symbol or not isinstance(symbol, str):
            return jsonify({"error": "symbol is required"}), 400

        try:
            tick = self._client.get(symbol)
        except KiteLtpIpcError as exc:
            self.logger.warning(
                f"Kite LTP IPC unreachable for user={user_id}: "
                f"{type(exc).__name__}"
            )
            return jsonify({
                "error": "LTP stream temporarily unavailable",
                "reason": str(exc)[:120],
            }), 503
        except Exception as exc:
            self.logger.error(
                f"Kite LTP read failed for user={user_id}: "
                f"{type(exc).__name__}"
            )
            return jsonify({"error": "LTP read failed"}), 500

        if tick is None:
            return jsonify({
                "sym": symbol,
                "error": "not_watched",
            }), 404
        return jsonify(tick), 200

    # ───── SSE stream ──────────────────────────────────────

    @Logger.standardLogger
    def streamLtp(self):
        """GET /api/kite/stream/ltp — SSE live LTP stream.

        Event shape:
          * ``event: ltp\\ndata: {"<SYM>": {"ltp": ..., "ts": ...}, ...}\\n\\n``
            - delta since the prior emitted snapshot; first
              event carries the full snapshot so the FE has
              initial state.
          * ``event: reconnect_required\\ndata: {...}\\n\\n``
            - daemon state flipped to reconnect_required; FE
              walks the Kite login flow + reconnects to this
              endpoint after.
          * ``: keep-alive\\n\\n`` — heartbeat frame (iOS 26
              reap mitigation via ``wrap_with_heartbeats``).

        Terminates on:
          * Client disconnect (natural).
          * Daemon reconnect_required (stream closes after
            emitting the event).
          * ``_SSE_MAX_DURATION_SECONDS`` elapsed — FE expected
            to reconnect transparently.
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        client = self._client

        def _ltp_delta_producer() -> Iterator[bytes]:
            """Inner SSE event generator — iterated by
            ``wrap_with_heartbeats`` in a daemon thread. Yields
            events as bytes.

            Loop: poll daemon once per
            ``_SSE_POLL_INTERVAL_SECONDS``, compute symbols that
            changed vs the prior-emitted snapshot, emit only
            changed symbols. First iteration emits the full
            snapshot as the baseline.
            """
            start = time.time()
            prior: dict = {}
            first = True
            while (time.time() - start) < _SSE_MAX_DURATION_SECONDS:
                try:
                    resp = client.get_all()
                except KiteLtpIpcError:
                    # Daemon briefly unreachable. Emit a
                    # reconnect_required event once + exit so
                    # the FE can reconnect.
                    yield _sse("reconnect_required", {
                        "state": "daemon_unreachable",
                        "detail": "retry soon",
                    })
                    return
                except Exception:
                    # Defensive: unknown IPC failure. Close the
                    # stream cleanly; FE reconnects.
                    return

                # Short-circuit on reconnect_required state.
                if resp.get("state") == STATE_RECONNECT:
                    yield _sse("reconnect_required", {
                        "state": STATE_RECONNECT,
                        "detail": resp.get("detail", ""),
                    })
                    return

                ticks = resp.get("ticks", {}) or {}
                if first:
                    yield _sse("ltp", ticks)
                    prior = dict(ticks)
                    first = False
                else:
                    # Delta = symbols present in current with
                    # a DIFFERENT ltp than prior, plus any new
                    # symbols. Drop pure stale (unchanged).
                    delta = {}
                    for sym, tick in ticks.items():
                        p = prior.get(sym)
                        if p is None or p.get("ltp") != tick.get("ltp"):
                            delta[sym] = tick
                    if delta:
                        yield _sse("ltp", delta)
                        prior.update(delta)
                time.sleep(_SSE_POLL_INTERVAL_SECONDS)

        def generate():
            # wrap_with_heartbeats emits keepalives when the inner
            # producer goes silent (tick-less gap, overnight, etc).
            # copy_current_request_context so the producer thread
            # can still touch g if needed (currently it doesn't,
            # but defense-in-depth per ak-iove v2 CRITICAL fix).
            for event in wrap_with_heartbeats(
                _ltp_delta_producer,
                interval_sec=DEFAULT_HEARTBEAT_INTERVAL_SEC,
                producer_wrapper=copy_current_request_context,
            ):
                yield event

        response = Response(
            stream_with_context(generate()),
            content_type="text/event-stream",
        )
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response


def _sse(event_name: str, payload: dict) -> bytes:
    """Format a single SSE event frame as bytes.

    Follows the WHATWG SSE spec: ``event: <name>\\ndata: <JSON>\\n\\n``.
    JSON is compact (separators=(",",":")) so bandwidth stays
    bounded on fat-portfolio deltas.
    """
    body = json.dumps(payload, separators=(",", ":"))
    return f"event: {event_name}\ndata: {body}\n\n".encode("utf-8")
