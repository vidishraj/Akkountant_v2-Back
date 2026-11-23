"""ak-vue6 Phase 1: KiteTicker WebSocket daemon — Overseer-approved
real-time LTP streaming (ak-vigj-adjacent — no new tables).

## Overseer-approved design (2026-11-23)

* Separate systemd unit ``ak-kite-ws.service``.
* In-process latest-tick dict + UNIX-domain socket IPC (no
  Redis).
* Subscribe to holdings' instrument_tokens on boot; re-sync
  after ``sync_kite_holdings`` completion signal (future
  hook — see ``_reload_subscription_set`` call-sites).
* Token-refresh: on token-expired, mark state
  ``reconnect_required`` and exit non-zero; systemd Restart=
  always retries after the operator walks the Kite login
  flow.
* Auto-subscribe holdings — no per-symbol opt-in.
* RSS target: 50–80 MB.

## Structure

The daemon has two long-lived threads:

1. **IPC server thread** — binds the UNIX socket, serves
   ``get`` / ``get_all`` / ``state`` requests over a simple
   JSON-line protocol (``services/kite_ltp_ipc.py``). Snapshot-
   read semantics: the caller only ever sees the LATEST tick
   per symbol. Zero backpressure risk (daemon is SOLE writer
   to the dict).
2. **WS main loop thread** — opens KiteTicker, subscribes to
   the resolved instrument_tokens, runs the SDK's event loop.
   The SDK handles reconnect/backoff internally; on token-
   expired it surfaces via ``on_close`` / ``on_error`` with a
   Kite-specific error code → daemon flips state to
   ``reconnect_required`` and triggers a clean exit so systemd
   restarts (fresh DB read picks up the new token).

## Thread safety

* ``_latest_ticks`` dict: guarded by ``_ticks_lock`` (RLock).
  Reads are O(1) copies for IPC responses; writes happen only
  from the KiteTicker on_ticks callback. RLock > Lock here
  because logging within callbacks can trigger re-entry
  patterns on misconfigured handler chains (defensive).
* ``_state`` string: guarded by ``_state_lock``. One writer
  (the main loop) + many readers (IPC server threads +
  tests). Lock is small and contended rarely.
* ``_subscription_set``: only mutated by the main-loop thread
  (reload on boot + post-sync hook). IPC server threads do
  NOT touch it.

## Security — financial-data secrecy

LTP log lines carry ONLY the symbol + price. Never log
portfolio-size context (holdings count, qty, buy price). The
daemon doesn't know that context anyway — it only has the
instrument_token subscription set, resolved from the DB at
boot. This is the invariant per Overseer's spec (``LTP log
lines must NOT include portfolio-size or holding-qty
context``).

## Mock-friendliness

``kiteconnect.KiteTicker`` is the production WS client; tests
inject a mock through the module-level ``_KiteTickerFactory``
seam so the daemon's loop + dict-update logic can exercise
against canned on_ticks payloads WITHOUT a network connection.
See ``test_kite_ws_daemon.py``.
"""
from __future__ import annotations

import json
import logging
import os
import socketserver
import sys
import threading
import time
from typing import Callable, Dict, Optional, Set

from services.kite_ltp_ipc import (
    DEFAULT_SOCKET_PATH,
    OP_GET,
    OP_GET_ALL,
    OP_STATE,
    STATE_INIT,
    STATE_OK,
    STATE_RECONNECT,
    decode_request,
    encode_response,
    now_ts,
)


# The seam the production daemon wires to kiteconnect.KiteTicker.
# Tests replace this with a stub via ``set_ticker_factory``. Keeps
# the daemon's logic independent of a real network connection.
#
# Signature: ``(api_key, access_token) -> ticker-like object``.
# The ticker-like object must expose:
#   * on_ticks: callable(ws, ticks) -> None   (SETTER)
#   * on_connect: callable(ws, response) -> None  (SETTER)
#   * on_close: callable(ws, code, reason) -> None (SETTER)
#   * on_error: callable(ws, code, reason) -> None (SETTER)
#   * subscribe(tokens: list[int])
#   * set_mode(mode, tokens: list[int])  — mode constants on class
#   * MODE_LTP constant
#   * connect(threaded=False, disable_ssl_verification=False)
_KiteTickerFactory: Optional[Callable] = None


def set_ticker_factory(factory: Optional[Callable]) -> None:
    """Install a test-side stub factory. Call with None to
    reset to production (lazy-imports kiteconnect.KiteTicker).
    """
    global _KiteTickerFactory
    _KiteTickerFactory = factory


def _resolve_ticker_factory() -> Callable:
    """Return either the test-installed stub or the real
    ``kiteconnect.KiteTicker``. Lazy import so this module is
    importable in envs without kiteconnect installed (tests).
    """
    if _KiteTickerFactory is not None:
        return _KiteTickerFactory
    from kiteconnect import KiteTicker  # pragma: no cover — prod path
    return lambda api_key, access_token: KiteTicker(
        api_key=api_key, access_token=access_token,
    )


# Kite close codes that indicate the access token is dead and a
# reconnect walk is required. Observed in production + per Kite
# docs: 1006 (abnormal close) + 403-ish errors on handshake.
# Treated conservatively: any close code we don't recognize as
# transient (1000 normal / 1001 going-away) triggers a token
# re-check at next loop iteration.
_TRANSIENT_CLOSE_CODES = {1000, 1001, 1012}


class KiteWsDaemon:
    """Long-lived WebSocket producer + UNIX-socket snapshot server.

    Lifecycle:
        1. ``build(...)`` constructs the daemon from (api_key,
           access_token_provider, subscription_provider).
        2. ``start_ipc_server(socket_path)`` binds the UNIX
           socket in a background thread.
        3. ``run_ws_loop()`` enters the main WS-connect loop
           (blocking). Returns when the daemon exits (clean
           stop, or token-expired signal).

    Exits non-zero (via ``sys.exit(1)``) on token-expired so
    systemd Restart=always picks up the next iteration with a
    fresh token.
    """

    def __init__(
        self,
        api_key: str,
        access_token_provider: Callable[[], Optional[str]],
        subscription_provider: Callable[[], Dict[str, int]],
        logger: Optional[logging.Logger] = None,
    ):
        """
        Args:
            api_key: Kite Connect app's API key.
            access_token_provider: zero-arg callable returning the
                CURRENT access token from the DB (or None if no
                user has walked login). Called fresh on every
                reconnect attempt so a post-expiry re-login lands
                without a daemon bounce.
            subscription_provider: zero-arg callable returning
                ``{"<EXCHANGE:SYMBOL>": instrument_token}``. Called
                on boot + post-sync. Keys are the human-readable
                form the IPC consumer queries by.
            logger: optional logger; a stderr logger is created if
                None.
        """
        self.api_key = api_key
        self._token_provider = access_token_provider
        self._subscription_provider = subscription_provider
        self.logger = logger or logging.getLogger("kite_ws_daemon")

        self._latest_ticks: Dict[str, dict] = {}
        self._ticks_lock = threading.RLock()

        # Reverse map: instrument_token (int from Kite) → symbol
        # (EXCHANGE:SYM string). KiteTicker reports ticks keyed by
        # instrument_token; we store them keyed by symbol so IPC
        # consumers (FE code) query the human-readable form.
        self._token_to_symbol: Dict[int, str] = {}

        # State machine string — one of STATE_INIT / STATE_OK /
        # STATE_RECONNECT. Written only by the main loop; read by
        # IPC server threads via the state lock.
        self._state = STATE_INIT
        self._state_detail = ""
        self._state_lock = threading.RLock()

        # Signal the main loop sets when it's time to exit.
        self._exit_event = threading.Event()

        # IPC server + its thread handle.
        self._server: Optional[_IpcServer] = None
        self._server_thread: Optional[threading.Thread] = None

    # ───── State helpers ───────────────────────────────────

    def _set_state(self, state: str, detail: str = "") -> None:
        with self._state_lock:
            self._state = state
            self._state_detail = detail
        self.logger.info(
            f"state → {state}" + (f" ({detail})" if detail else "")
        )

    def get_state(self) -> dict:
        """Read-copy of the daemon state — IPC server calls this."""
        with self._state_lock:
            return {
                "state": self._state,
                "detail": self._state_detail,
            }

    # ───── Tick store ──────────────────────────────────────

    def get_tick(self, sym: str) -> Optional[dict]:
        """O(1) snapshot read for the single-symbol IPC op."""
        with self._ticks_lock:
            return self._latest_ticks.get(sym)

    def snapshot_ticks(self) -> dict:
        """Copy of the whole dict — for the get_all IPC op.

        Shallow copy; per-tick dicts are immutable from the
        producer's perspective (writer always replaces the
        whole entry, never mutates in-place).
        """
        with self._ticks_lock:
            return dict(self._latest_ticks)

    # ───── KiteTicker callback hooks ───────────────────────

    def _on_ticks(self, ws, ticks) -> None:
        """Called by KiteTicker on each tick batch.

        ``ticks`` is a list of dicts; each dict carries
        ``instrument_token`` + ``last_price``. We resolve the
        token back to our EXCHANGE:SYMBOL key and update the
        dict under the lock.

        Logs ONLY counts — never per-symbol / per-price detail
        (financial-data secrecy invariant per Overseer).
        """
        ts = now_ts()
        updated = 0
        with self._ticks_lock:
            for t in ticks:
                token = t.get("instrument_token")
                if token is None:
                    continue
                sym = self._token_to_symbol.get(token)
                if sym is None:
                    # Tick for a token we didn't subscribe to —
                    # Kite occasionally emits echoes after an
                    # unsubscribe in-flight. Drop silently.
                    continue
                price = t.get("last_price")
                if price is None:
                    continue
                self._latest_ticks[sym] = {
                    "sym": sym,
                    "ltp": float(price),
                    "ts": ts,
                }
                updated += 1
        if updated > 0 and self._state != STATE_OK:
            self._set_state(STATE_OK)

    def _on_connect(self, ws, response) -> None:
        """Subscribe on connect — called by KiteTicker."""
        tokens = sorted(self._token_to_symbol.keys())
        if not tokens:
            self.logger.warning(
                "No subscribed tokens on connect — holdings "
                "empty or unresolved; daemon will serve empty "
                "ticks dict until a subscription-reload happens"
            )
            return
        try:
            ws.subscribe(tokens)
            # LTP mode = cheapest + sufficient for last_price only
            # streams. Change to MODE_QUOTE if the FE ever needs
            # OHLC/volume in real time; MODE_FULL for market
            # depth.
            ws.set_mode(ws.MODE_LTP, tokens)
            self.logger.info(
                f"Subscribed to {len(tokens)} instrument_tokens"
            )
        except Exception as exc:
            # Non-fatal: log + let the WS deliver nothing; state
            # stays INIT until first tick arrives. Subscribe
            # failure doesn't warrant exiting.
            self.logger.error(
                f"subscribe failed: {type(exc).__name__}"
            )

    def _on_close(self, ws, code, reason) -> None:
        """Handle a WS close event.

        Kite's client auto-reconnects for transient closes (we
        don't need our own exponential backoff — the SDK ships
        one). Our only interest is detecting the token-expired
        case so systemd can be asked to restart with a fresh
        token.
        """
        # Logging: 'reason' can carry token fragments in some
        # SDK versions — strip to just the code + short text.
        reason_tag = str(reason)[:40] if reason else ""
        self.logger.info(f"ws closed: code={code} reason={reason_tag}")
        if code and code in _TRANSIENT_CLOSE_CODES:
            return
        # Non-transient close → re-check token at next iteration.
        # If the token is still valid, SDK reconnect gets us back
        # up; if it's dead, run_ws_loop detects and exits.
        self._set_state(
            STATE_RECONNECT,
            f"ws close code={code}",
        )

    def _on_error(self, ws, code, reason) -> None:
        reason_tag = str(reason)[:40] if reason else ""
        self.logger.error(f"ws error: code={code} reason={reason_tag}")

    # ───── Subscription set management ─────────────────────

    def reload_subscriptions(self) -> int:
        """Re-pull the subscription map from the provider and
        refresh ``_token_to_symbol``. Returns the new count.

        Called on boot and (future) after
        ``sync_kite_holdings`` completes. If the daemon is
        already connected, the next ``_on_connect`` call
        (post-reconnect) will re-subscribe with the fresh set;
        for the Phase 1 scope we accept a one-reconnect delay
        rather than live-mutating the KiteTicker subscription
        (which the SDK supports but adds thread-safety
        complexity — deferred).
        """
        try:
            mapping = self._subscription_provider()
        except Exception as exc:
            self.logger.error(
                f"subscription provider failed: "
                f"{type(exc).__name__}"
            )
            return 0
        new_token_to_symbol = {
            int(token): sym for sym, token in mapping.items()
        }
        # Swap under the ticks lock so IPC readers don't see a
        # half-updated reverse map vs latest_ticks.
        with self._ticks_lock:
            self._token_to_symbol = new_token_to_symbol
            # Drop ticks for de-subscribed symbols.
            keep = set(new_token_to_symbol.values())
            self._latest_ticks = {
                sym: tick for sym, tick in self._latest_ticks.items()
                if sym in keep
            }
        return len(new_token_to_symbol)

    # ───── IPC server ──────────────────────────────────────

    def start_ipc_server(
        self, socket_path: str = DEFAULT_SOCKET_PATH,
    ) -> None:
        """Bind the UNIX socket + start the request-handling
        thread. Non-blocking. The thread terminates when
        ``stop()`` is called.
        """
        # Remove stale socket from a prior crashed run. systemd
        # Type=simple doesn't clean the socket for us.
        if os.path.exists(socket_path):
            try:
                os.unlink(socket_path)
            except OSError as exc:
                self.logger.error(
                    f"stale socket unlink failed at {socket_path}: "
                    f"{type(exc).__name__}"
                )

        # Ensure parent dir exists (defense in depth — infra
        # tmpfiles.d handles /run/akkountant but dev-run might
        # target a user dir). mode 0700 on the parent + 0600
        # on the socket itself.
        parent = os.path.dirname(socket_path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, mode=0o700, exist_ok=True)

        self._server = _IpcServer(socket_path, _IpcHandler, daemon=self)
        # Set tight socket perms (0600) BEFORE accepting the first
        # client so a racing opc-level process can't connect in the
        # window.
        try:
            os.chmod(socket_path, 0o600)
        except OSError as exc:
            self.logger.warning(
                f"socket chmod 0600 failed: {type(exc).__name__}"
            )

        self._server_thread = threading.Thread(
            target=self._server.serve_forever,
            name="kite-ltp-ipc",
            daemon=True,
        )
        self._server_thread.start()
        self.logger.info(f"IPC server bound at {socket_path}")

    def stop(self) -> None:
        """Clean shutdown: terminate the IPC server + signal
        main loop exit. Idempotent.
        """
        self._exit_event.set()
        if self._server is not None:
            try:
                self._server.shutdown()
                self._server.server_close()
            except OSError:
                pass
            self._server = None

    # ───── WS main loop ────────────────────────────────────

    def run_ws_loop(self) -> int:
        """Enter the KiteTicker loop. Blocks until:
          * ``stop()`` is called (returns 0), OR
          * Token-expired detected (sets state + returns 1 so
            the caller sys.exit(1) and systemd restarts).

        Return value is the intended process exit code.
        """
        # Boot-time subscription load. Empty-set is NOT fatal —
        # a fresh install may have zero holdings; the daemon
        # still serves an empty ticks dict + state=OK once a
        # tick (even for nothing) arrives, or stays INIT.
        count = self.reload_subscriptions()
        self.logger.info(
            f"boot: {count} instrument_tokens in subscription set"
        )

        while not self._exit_event.is_set():
            token = self._token_provider()
            if not token:
                self.logger.warning(
                    "no kite access_token — state=reconnect_required, "
                    "waiting for operator to walk login flow"
                )
                self._set_state(
                    STATE_RECONNECT,
                    "no access_token on file",
                )
                # Don't tight-loop; sleep bounded so systemd
                # journal doesn't flood. operator reconnect
                # typically happens within minutes.
                if self._exit_event.wait(timeout=30):
                    return 0
                continue

            factory = _resolve_ticker_factory()
            ticker = factory(self.api_key, token)
            ticker.on_ticks = self._on_ticks
            ticker.on_connect = self._on_connect
            ticker.on_close = self._on_close
            ticker.on_error = self._on_error

            try:
                # threaded=False → connect() blocks the main thread
                # (ticker owns the reactor loop). Returns when
                # connection closes. Our on_close callback already
                # marks state=reconnect_required on non-transient
                # close codes.
                ticker.connect(threaded=False)
            except Exception as exc:
                # Log the class only — ``str(exc)`` may carry
                # token fragments on some SDK versions.
                self.logger.error(
                    f"ticker.connect raised {type(exc).__name__}"
                )

            # Post-connect check: if state flipped to reconnect_
            # required during the lifetime of this connection,
            # exit non-zero. systemd restarts; next boot re-reads
            # the token from DB.
            if self.get_state()["state"] == STATE_RECONNECT:
                self.logger.info(
                    "exiting with code 1 — systemd will restart with "
                    "fresh token read"
                )
                return 1

            # Transient close + state stays OK/INIT → loop and
            # reconnect with the same token. Brief backoff so a
            # hard network partition doesn't spin.
            if self._exit_event.wait(timeout=5):
                return 0

        return 0


# ───── IPC server plumbing ─────────────────────────────────


class _IpcServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    """Thread-per-connection UNIX stream server.

    Thread-per-connection is correct here: requests are tiny
    (< 1KB JSON) and short-lived (one roundtrip). Pooling would
    add complexity for zero benefit at this throughput.
    """

    # Tells the server to close dangling threads when the main
    # process exits — prevents the systemd stop from hanging on
    # a stuck client.
    daemon_threads = True
    # Allow immediate re-bind on restart (socket file is unlinked
    # on boot anyway, but the SO_REUSEADDR-equivalent on UDS is
    # the right default).
    allow_reuse_address = True

    def __init__(self, socket_path, handler_cls, daemon):
        self.daemon = daemon  # the KiteWsDaemon instance
        super().__init__(socket_path, handler_cls)


class _IpcHandler(socketserver.StreamRequestHandler):
    """Reads ONE JSON line; writes ONE JSON line; closes."""

    def handle(self) -> None:
        daemon: KiteWsDaemon = self.server.daemon
        try:
            line = self.rfile.readline()
            if not line:
                return
            try:
                req = decode_request(line)
            except ValueError as exc:
                self.wfile.write(encode_response({
                    "error": "bad_request",
                    "detail": str(exc)[:80],
                }))
                return

            op = req.get("op")
            if op == OP_GET:
                sym = req.get("sym")
                if not isinstance(sym, str) or not sym:
                    self.wfile.write(encode_response({
                        "error": "bad_request",
                        "detail": "missing sym",
                    }))
                    return
                tick = daemon.get_tick(sym)
                if tick is None:
                    self.wfile.write(encode_response({
                        "sym": sym,
                        "error": "not_watched",
                    }))
                    return
                self.wfile.write(encode_response(tick))
                return

            if op == OP_GET_ALL:
                self.wfile.write(encode_response({
                    "ticks": daemon.snapshot_ticks(),
                    **daemon.get_state(),
                }))
                return

            if op == OP_STATE:
                self.wfile.write(encode_response(daemon.get_state()))
                return

            self.wfile.write(encode_response({
                "error": "unknown_op",
                "detail": str(op)[:40],
            }))
        except (BrokenPipeError, ConnectionResetError):
            # Client walked off mid-write — nothing we can do;
            # don't surface as a daemon error.
            return
        except Exception as exc:
            # Last-resort logging — do not propagate; another
            # client's connection should still be serveable.
            daemon.logger.error(
                f"IPC handler failed: {type(exc).__name__}"
            )
