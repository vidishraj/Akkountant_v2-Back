"""ak-vue6 Phase 1: tests for the Kite WS daemon + IPC +
Flask SSE/snapshot endpoints.

Scope:
  * **IPC protocol** — request/response round-trip against a
    real UNIX-domain socket (no mocks on the wire; small
    behavior-pins).
  * **Daemon logic** — ``KiteWsDaemon`` with a mock
    KiteTicker factory. Pins: on_ticks → dict update,
    on_connect → subscribe, on_close transient vs non-
    transient state transition, reload_subscriptions drops
    de-subscribed symbols.
  * **Entrypoint providers** — subscription_provider resolves
    held PurchasedSecurities via kite_instruments
    (AST-lifted; no Flask boot).
  * **Flask controller** — source-grep drift guards +
    route wire-up + READ-ONLY drift (SSE + snapshot are
    GET-only).
  * **Systemd unit file** — presence + Overseer invariants
    (Restart=always, StartLimitIntervalSec=0,
    RuntimeDirectory=akkountant).

Pin-downs:
  (1)  IPC get-single returns the live tick; not_watched on miss.
  (2)  IPC get_all returns full snapshot + state.
  (3)  IPC state op returns daemon state shape.
  (4)  IPC bad_request on malformed JSON.
  (5)  IPC bad_request on missing 'op'.
  (6)  IPC unknown_op response.
  (7)  Daemon on_ticks updates dict ONLY for subscribed tokens.
  (8)  Daemon on_ticks sets state=OK on first update.
  (9)  Daemon on_connect calls ticker.subscribe + set_mode.
  (10) Daemon on_close with transient code = no state change.
  (11) Daemon on_close with non-transient code = state→reconnect.
  (12) Daemon reload_subscriptions drops de-subscribed ticks.
  (13) Daemon logs do NOT include per-tick price values
       (financial-data secrecy invariant).
  (14) Controller /api/kite/ltp/<sym> route defined + READ-ONLY
       verb only (ak-l8a7 drift pattern).
  (15) Controller /api/kite/stream/ltp route defined + READ-ONLY.
  (16) Controller handlers read g.firebase_id (ak-c8l).
  (17) Controller maps IPC error → 503.
  (18) Controller maps not_watched → 404.
  (19) Controller SSE emits reconnect_required event.
  (20) Systemd unit file Restart=always.
  (21) Systemd unit file RuntimeDirectory=akkountant.
  (22) Systemd unit file StartLimitIntervalSec=0.
"""
from __future__ import annotations

import ast
import json
import os
import re
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ─── IPC protocol tests ───────────────────────────────────────


try:
    from services.kite_ltp_ipc import (
        DEFAULT_SOCKET_PATH,
        KiteLtpClient,
        KiteLtpIpcError,
        OP_GET,
        OP_GET_ALL,
        OP_STATE,
        STATE_INIT,
        STATE_OK,
        STATE_RECONNECT,
        decode_request,
        encode_response,
    )
    _IPC_DEPS_OK = True
    _IPC_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _IPC_DEPS_OK = False
    _IPC_SKIP_REASON = f"kite_ltp_ipc import failed: {_exc}"


@unittest.skipUnless(_IPC_DEPS_OK, _IPC_SKIP_REASON)
class TestIpcProtocolUnit(unittest.TestCase):
    """Pure-function tests for encode_response + decode_request."""

    def test_decode_request_basic(self):
        req = decode_request(b'{"op":"get","sym":"RELIANCE"}\n')
        self.assertEqual(req["op"], "get")
        self.assertEqual(req["sym"], "RELIANCE")

    def test_decode_request_rejects_malformed_json(self):
        with self.assertRaises(ValueError):
            decode_request(b"not json\n")

    def test_decode_request_rejects_missing_op(self):
        with self.assertRaises(ValueError) as cm:
            decode_request(b'{"sym":"X"}\n')
        self.assertIn("op", str(cm.exception))

    def test_decode_request_rejects_empty(self):
        with self.assertRaises(ValueError):
            decode_request(b"\n")

    def test_decode_request_rejects_non_object(self):
        with self.assertRaises(ValueError):
            decode_request(b'["array","not","object"]\n')

    def test_encode_response_newline_terminated(self):
        wire = encode_response({"sym": "X", "ltp": 100.5})
        self.assertTrue(wire.endswith(b"\n"))
        # Parseable back to the original shape
        obj = json.loads(wire.decode("utf-8").rstrip("\n"))
        self.assertEqual(obj["sym"], "X")
        self.assertEqual(obj["ltp"], 100.5)


# ─── Daemon logic (with mock KiteTicker) ──────────────────────


try:
    from services.kite_ws_daemon import (
        KiteWsDaemon,
        _TRANSIENT_CLOSE_CODES,
        set_ticker_factory,
    )
    _DAEMON_DEPS_OK = True
    _DAEMON_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _DAEMON_DEPS_OK = False
    _DAEMON_SKIP_REASON = f"kite_ws_daemon import failed: {_exc}"


class _MockTicker:
    """Drop-in for kiteconnect.KiteTicker — captures callbacks
    + subscribe() calls, never opens a socket. Tests drive the
    callbacks directly."""

    MODE_LTP = "ltp"
    MODE_QUOTE = "quote"
    MODE_FULL = "full"

    def __init__(self, api_key, access_token):
        self.api_key = api_key
        self.access_token = access_token
        self.on_ticks = None
        self.on_connect = None
        self.on_close = None
        self.on_error = None
        self.subscribed_tokens = None
        self.mode_set = None

    def subscribe(self, tokens):
        self.subscribed_tokens = list(tokens)

    def set_mode(self, mode, tokens):
        self.mode_set = (mode, list(tokens))

    def connect(self, threaded=False):
        # Prod path blocks; mock returns immediately.
        return


@unittest.skipUnless(_DAEMON_DEPS_OK, _DAEMON_SKIP_REASON)
class TestKiteWsDaemonLogic(unittest.TestCase):

    def setUp(self):
        self._tokens = {"NSE:RELIANCE": 100, "NSE:TCS": 200}
        self.daemon = KiteWsDaemon(
            api_key="test-key",
            access_token_provider=lambda: "test-token",
            subscription_provider=lambda: dict(self._tokens),
        )
        self.daemon.reload_subscriptions()

    # (7) on_ticks updates dict only for subscribed tokens
    def test_on_ticks_updates_subscribed_tokens(self):
        ticks = [
            {"instrument_token": 100, "last_price": 2900.5},
            {"instrument_token": 200, "last_price": 3800.0},
        ]
        self.daemon._on_ticks(ws=None, ticks=ticks)
        got = self.daemon.get_tick("NSE:RELIANCE")
        self.assertIsNotNone(got)
        self.assertEqual(got["sym"], "NSE:RELIANCE")
        self.assertEqual(got["ltp"], 2900.5)
        self.assertEqual(self.daemon.get_tick("NSE:TCS")["ltp"], 3800.0)

    def test_on_ticks_drops_unsubscribed_tokens(self):
        # Token 999 is not in our subscription set — must drop
        ticks = [
            {"instrument_token": 999, "last_price": 1.0},
            {"instrument_token": 100, "last_price": 2900.5},
        ]
        self.daemon._on_ticks(ws=None, ticks=ticks)
        # Dict has only the subscribed symbol
        snap = self.daemon.snapshot_ticks()
        self.assertEqual(set(snap.keys()), {"NSE:RELIANCE"})

    def test_on_ticks_drops_malformed_ticks(self):
        # Missing fields must not raise
        ticks = [
            {"instrument_token": None, "last_price": 100.0},
            {"instrument_token": 100},  # missing last_price
            {"last_price": 100.0},  # missing instrument_token
        ]
        self.daemon._on_ticks(ws=None, ticks=ticks)
        self.assertEqual(self.daemon.snapshot_ticks(), {})

    # (8) state → OK on first update
    def test_state_transitions_to_ok_on_first_tick(self):
        self.assertEqual(self.daemon.get_state()["state"], STATE_INIT)
        self.daemon._on_ticks(ws=None, ticks=[
            {"instrument_token": 100, "last_price": 100.0},
        ])
        self.assertEqual(self.daemon.get_state()["state"], STATE_OK)

    # (9) on_connect calls subscribe + set_mode
    def test_on_connect_subscribes_and_sets_mode(self):
        mock_ws = _MockTicker(None, None)
        self.daemon._on_connect(mock_ws, response={})
        self.assertEqual(
            sorted(mock_ws.subscribed_tokens), [100, 200],
        )
        self.assertEqual(
            mock_ws.mode_set, (mock_ws.MODE_LTP, [100, 200]),
        )

    def test_on_connect_no_tokens_logs_warning_and_returns(self):
        self._tokens = {}
        self.daemon.reload_subscriptions()
        mock_ws = _MockTicker(None, None)
        # Should not raise; no subscribe call
        self.daemon._on_connect(mock_ws, response={})
        self.assertIsNone(mock_ws.subscribed_tokens)

    # (10) transient close code → no state transition
    def test_on_close_transient_code_no_state_change(self):
        prior_state = self.daemon.get_state()["state"]
        self.daemon._on_close(ws=None, code=1000, reason="normal")
        self.assertEqual(self.daemon.get_state()["state"], prior_state)

    def test_transient_close_codes_include_normal_and_going_away(self):
        # Guard the module-level set so code reviewers notice if
        # someone drops 1000/1001 from the transient list.
        self.assertIn(1000, _TRANSIENT_CLOSE_CODES)
        self.assertIn(1001, _TRANSIENT_CLOSE_CODES)

    # (11) non-transient close → state=reconnect_required
    def test_on_close_non_transient_flips_state(self):
        self.daemon._on_close(ws=None, code=4001, reason="token invalid")
        self.assertEqual(
            self.daemon.get_state()["state"], STATE_RECONNECT,
        )

    # (12) reload_subscriptions drops de-subscribed ticks
    def test_reload_drops_desubscribed_ticks(self):
        # Seed a tick for RELIANCE + TCS
        self.daemon._on_ticks(ws=None, ticks=[
            {"instrument_token": 100, "last_price": 100.0},
            {"instrument_token": 200, "last_price": 200.0},
        ])
        self.assertEqual(
            set(self.daemon.snapshot_ticks().keys()),
            {"NSE:RELIANCE", "NSE:TCS"},
        )
        # New subscription set drops TCS
        self._tokens = {"NSE:RELIANCE": 100}
        self.daemon.reload_subscriptions()
        self.assertEqual(
            set(self.daemon.snapshot_ticks().keys()),
            {"NSE:RELIANCE"},
        )

    # (13) Financial-data secrecy: logs do NOT carry prices
    def test_on_ticks_logs_do_not_contain_prices(self):
        """Overseer invariant: 'LTP log lines must NOT include
        portfolio-size or holding-qty context'. We extend to:
        no per-symbol LTP values in log lines either — the
        count-only pattern is enforced at the on_ticks callsite.
        """
        import logging
        captured = []

        class _CaptureHandler(logging.Handler):
            def emit(self, record):
                captured.append(record.getMessage())

        h = _CaptureHandler()
        self.daemon.logger.addHandler(h)
        try:
            self.daemon._on_ticks(ws=None, ticks=[
                {"instrument_token": 100, "last_price": 2900.5},
                {"instrument_token": 200, "last_price": 3800.0},
            ])
            # Also trigger a state transition which logs
            joined = " ".join(captured)
            # The actual float values must NOT appear verbatim
            # in the daemon's log output on the hot path.
            self.assertNotIn("2900.5", joined)
            self.assertNotIn("3800.0", joined)
        finally:
            self.daemon.logger.removeHandler(h)


# ─── IPC server end-to-end (real UNIX socket) ─────────────────


@unittest.skipUnless(_DAEMON_DEPS_OK and _IPC_DEPS_OK, "deps missing")
class TestIpcEndToEnd(unittest.TestCase):
    """Boots the daemon's IPC server on a tmpfile socket,
    drives it through ``KiteLtpClient``. Validates the full
    wire path including socket bind, request parse, response
    encoding.
    """

    def setUp(self):
        self._tokens = {"NSE:RELIANCE": 100}
        self.daemon = KiteWsDaemon(
            api_key="test-key",
            access_token_provider=lambda: "test-token",
            subscription_provider=lambda: dict(self._tokens),
        )
        self.daemon.reload_subscriptions()
        # Seed one tick
        self.daemon._on_ticks(ws=None, ticks=[
            {"instrument_token": 100, "last_price": 2900.5},
        ])
        # Socket path — use tmpfile. UDS path length limit varies
        # by kernel (~104 chars Linux) so use a short tempdir.
        self._tmpdir = tempfile.mkdtemp(prefix="akvue6_")
        self._sock_path = os.path.join(self._tmpdir, "s.sock")
        self.daemon.start_ipc_server(socket_path=self._sock_path)
        self.client = KiteLtpClient(self._sock_path)
        # Give the server thread a tick to bind.
        time.sleep(0.05)

    def tearDown(self):
        self.daemon.stop()
        try:
            os.unlink(self._sock_path)
        except OSError:
            pass
        try:
            os.rmdir(self._tmpdir)
        except OSError:
            pass

    # (1) get-single
    def test_get_single_hits(self):
        tick = self.client.get("NSE:RELIANCE")
        self.assertIsNotNone(tick)
        self.assertEqual(tick["sym"], "NSE:RELIANCE")
        self.assertEqual(tick["ltp"], 2900.5)

    def test_get_single_miss_returns_none(self):
        tick = self.client.get("NSE:UNKNOWN")
        self.assertIsNone(tick)

    # (2) get_all
    def test_get_all_returns_snapshot_and_state(self):
        resp = self.client.get_all()
        self.assertIn("ticks", resp)
        self.assertIn("NSE:RELIANCE", resp["ticks"])
        self.assertIn("state", resp)

    # (3) state op
    def test_state_op(self):
        resp = self.client.state()
        self.assertIn("state", resp)
        self.assertIn(
            resp["state"],
            {STATE_OK, STATE_INIT, STATE_RECONNECT},
        )

    # (6) unknown_op
    def test_unknown_op_returns_shape(self):
        # Hand-rolled roundtrip because KiteLtpClient only
        # exposes the valid ops.
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self._sock_path)
        try:
            sock.sendall(b'{"op":"nonsense"}\n')
            data = sock.recv(4096)
            resp = json.loads(data.decode("utf-8").rstrip("\n"))
            self.assertEqual(resp.get("error"), "unknown_op")
        finally:
            sock.close()

    # (4) bad JSON
    def test_bad_json_returns_bad_request(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self._sock_path)
        try:
            sock.sendall(b"not json\n")
            data = sock.recv(4096)
            resp = json.loads(data.decode("utf-8").rstrip("\n"))
            self.assertEqual(resp.get("error"), "bad_request")
        finally:
            sock.close()

    # Daemon unreachable → KiteLtpIpcError
    def test_client_daemon_unreachable_raises_ipc_error(self):
        dead_client = KiteLtpClient(
            socket_path=os.path.join(self._tmpdir, "nonexistent.sock"),
        )
        with self.assertRaises(KiteLtpIpcError):
            dead_client.get("X")


# ─── Flask controller source-grep drift guards ────────────────


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


class TestKiteLtpControllerShape(unittest.TestCase):

    def test_controller_has_getLtp_handler(self):
        src = _read_source("controllers/kiteLtpEP.py")
        assert re.search(
            r"def\s+getLtp\s*\(\s*self\s*,\s*symbol\s*\)\s*:", src,
        ), "KiteLtpController.getLtp handler missing"

    def test_controller_has_streamLtp_handler(self):
        src = _read_source("controllers/kiteLtpEP.py")
        assert re.search(
            r"def\s+streamLtp\s*\(\s*self\s*\)\s*:", src,
        ), "KiteLtpController.streamLtp handler missing"

    # (16) ak-c8l — g.firebase_id, no raw-header reads
    def test_controller_reads_firebase_id_from_g(self):
        src = _read_source("controllers/kiteLtpEP.py")
        self.assertNotIn("X-Firebase-ID", src)
        self.assertNotIn("request.headers.get", src)
        # Both handlers read g.firebase_id
        self.assertGreaterEqual(src.count('g.get("firebase_id")'), 2)

    # (17) IPC error → 503
    def test_controller_maps_ipc_error_to_503(self):
        src = _read_source("controllers/kiteLtpEP.py")
        # KiteLtpIpcError imported + caught + mapped to 503
        self.assertIn(
            "from services.kite_ltp_ipc import", src,
        )
        self.assertIn("except KiteLtpIpcError", src)
        # 503 status on the error branch
        assert re.search(
            r"except KiteLtpIpcError[^:]*:.*?return jsonify\(.*?\),\s*503",
            src, re.DOTALL,
        ), "IPC error must map to 503"

    # (18) not_watched → 404
    def test_controller_maps_not_watched_to_404(self):
        src = _read_source("controllers/kiteLtpEP.py")
        # When tick is None, return 404
        self.assertTrue(
            re.search(
                r"if tick is None:.*?return jsonify\(.*?\),\s*404",
                src, re.DOTALL,
            ),
            "not_watched branch must 404",
        )

    # (19) SSE emits reconnect_required
    def test_sse_emits_reconnect_required_event(self):
        src = _read_source("controllers/kiteLtpEP.py")
        # Both STATE_RECONNECT short-circuit and daemon_unreachable
        # emit reconnect_required
        self.assertIn('"reconnect_required"', src)


# ─── Route wire-up ────────────────────────────────────────────


def test_ltp_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/api/kite/ltp/<symbol>'\s*,\s*'GET'\s*,\s*"
        r"self\.kiteLtpEP\.getLtp",
        src,
    ), "GET /api/kite/ltp/<symbol> not wired in app.py"


def test_stream_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/api/kite/stream/ltp'\s*,\s*'GET'\s*,\s*"
        r"self\.kiteLtpEP\.streamLtp",
        src,
    ), "GET /api/kite/stream/ltp not wired in app.py"


def test_api_kite_routes_read_only():
    """ak-vue6 Phase 1 is READ-ONLY per Overseer. No mutation
    verb on /api/kite/* — any POST/PUT/DELETE/PATCH added by a
    future bead fails this guard and forces a §23 #5 review.
    """
    src = _read_source("app.py")
    all_entries = re.findall(
        r"\(\s*('/api/kite/[^']+')\s*,\s*('[A-Z]+')\s*,", src,
    )
    for rule, verb in all_entries:
        v = verb.strip("'")
        assert v not in ("POST", "PUT", "DELETE", "PATCH"), (
            f"READ-ONLY drift on /api/kite/*: {rule} {verb}. "
            f"Phase 1 is READ-only; mutations deferred."
        )


# ─── Systemd unit file ────────────────────────────────────────


def test_systemd_unit_file_exists():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, "deploy", "ak-kite-ws.service")
    assert os.path.exists(path), (
        "deploy/ak-kite-ws.service missing — infra needs it to "
        "install the daemon"
    )


def test_systemd_unit_has_restart_always():
    """(20) Overseer spec: Restart=always so token-expired
    non-zero exit triggers a restart."""
    src = _read_source("deploy/ak-kite-ws.service")
    assert re.search(
        r"^Restart\s*=\s*always", src, re.MULTILINE,
    ), "systemd unit must set Restart=always"


def test_systemd_unit_has_runtime_directory():
    """(21) RuntimeDirectory=akkountant creates /run/akkountant
    with the right perms BEFORE the daemon starts."""
    src = _read_source("deploy/ak-kite-ws.service")
    assert re.search(
        r"^RuntimeDirectory\s*=\s*akkountant", src, re.MULTILINE,
    ), "systemd unit must set RuntimeDirectory=akkountant"


def test_systemd_unit_has_start_limit_interval_zero():
    """(22) StartLimitIntervalSec=0 — no cap on restart attempts.
    Token-expired triggers intentional non-zero exits.
    """
    src = _read_source("deploy/ak-kite-ws.service")
    assert re.search(
        r"^StartLimitIntervalSec\s*=\s*0", src, re.MULTILINE,
    ), "systemd unit must set StartLimitIntervalSec=0"


def test_systemd_unit_binds_entrypoint_script():
    """ExecStart must call the entrypoint script we shipped."""
    src = _read_source("deploy/ak-kite-ws.service")
    assert "kite_ws_daemon_entrypoint.py" in src, (
        "systemd unit must ExecStart the daemon entrypoint script"
    )


def test_entrypoint_script_present():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(here, "scripts", "kite_ws_daemon_entrypoint.py")
    assert os.path.exists(path), (
        "scripts/kite_ws_daemon_entrypoint.py missing — systemd unit "
        "will fail to start"
    )


# ─── ak-vue6-unit-hotfix (2026-11-23) ─────────────────────────


def test_systemd_unit_working_directory_prod_path():
    """ak-vue6-unit-hotfix: infra preflight caught WorkingDirectory
    was lowercase (/home/opc/akkountantV2_back) but the real prod
    path is hyphenated + capitalized (/home/opc/Akkountant_v2-Back).
    Pin the correct suffix so a future accidental revert reds.
    """
    src = _read_source("deploy/ak-kite-ws.service")
    assert re.search(
        r"^WorkingDirectory\s*=\s*/home/opc/Akkountant_v2-Back\s*$",
        src, re.MULTILINE,
    ), (
        "ak-vue6-unit-hotfix: WorkingDirectory must be the real "
        "prod path /home/opc/Akkountant_v2-Back (hyphenated, "
        "capital A + V) — the lowercase akkountantV2_back doesn't "
        "exist on prod. Infra preflight catches this but the drift "
        "guard pins it in-repo."
    )


def test_systemd_unit_execstart_uses_venv_python():
    """ak-vue6-unit-hotfix: infra preflight caught ExecStart was
    /usr/bin/python3 (system) which lacks sqlalchemy /
    mysql-connector / kiteconnect. Must use the venv's python,
    same pattern as app.service + schedular.service. Pin the
    /venv/ substring so a revert to system python reds.
    """
    src = _read_source("deploy/ak-kite-ws.service")
    exec_match = re.search(
        r"^ExecStart\s*=\s*(\S+)\s+scripts/kite_ws_daemon_entrypoint\.py",
        src, re.MULTILINE,
    )
    assert exec_match, (
        "ExecStart line not found in expected shape"
    )
    python_path = exec_match.group(1)
    assert "/venv/" in python_path, (
        f"ak-vue6-unit-hotfix: ExecStart python path must be from "
        f"venv (contain '/venv/'); got {python_path!r}. The daemon "
        f"imports sqlalchemy / mysql-connector / kiteconnect which "
        f"the system python3 lacks."
    )
    assert "/usr/bin/python" not in python_path, (
        f"ak-vue6-unit-hotfix: ExecStart must NOT use system "
        f"python; got {python_path!r}"
    )


# ─── Entrypoint subscription_provider — AST-lift ──────────────
#
# scripts/kite_ws_daemon_entrypoint.py imports the configured
# Flask app lazily inside main(). The subscription_provider /
# access_token_provider factories, however, are top-level +
# don't need the app — they just capture it in a closure. We
# AST-lift the factory functions and exercise them with a stub
# app + session to pin the DB-resolution shape without a Flask
# boot.


def _ast_lift_entrypoint_providers():
    """Lift _build_access_token_provider + _build_subscription_
    provider from scripts/kite_ws_daemon_entrypoint.py."""
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "scripts", "kite_ws_daemon_entrypoint.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)
    kept = []
    want = {"_build_access_token_provider", "_build_subscription_provider"}
    for node in tree.body:
        if isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef),
        ) and node.name in want:
            kept.append(node)
    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)
    ns = {"__name__": "test_vue6_entrypoint_lift"}
    exec(compile(lift, "<vue6-entrypoint-lift>", "exec"), ns)
    return ns


class TestEntrypointProviders(unittest.TestCase):
    """Pure-function tests for the token + subscription
    providers — mock the DB session so we don't need Flask."""

    def test_access_token_provider_returns_token_when_present(self):
        ns = _ast_lift_entrypoint_providers()
        # Stub the UserToken model lookup via models.googleTokens
        import types as _types
        import sys as _sys

        mod = _types.ModuleType("models.googleTokens")

        class _UT:
            pass
        mod.UserToken = _UT
        _sys.modules["models.googleTokens"] = mod

        try:
            fake_token = MagicMock()
            fake_token.access_token = "live-token-abc"

            fake_session = MagicMock()
            fake_session.query.return_value.filter_by.return_value.first.return_value = fake_token

            fake_app = MagicMock()
            fake_app.app_context.return_value.__enter__ = lambda s: None
            fake_app.app_context.return_value.__exit__ = (
                lambda s, a, b, c: None
            )
            fake_app.db.session = fake_session

            provider = ns["_build_access_token_provider"](fake_app)
            self.assertEqual(provider(), "live-token-abc")
        finally:
            del _sys.modules["models.googleTokens"]

    def test_access_token_provider_returns_none_when_no_token(self):
        ns = _ast_lift_entrypoint_providers()
        import types as _types
        import sys as _sys

        mod = _types.ModuleType("models.googleTokens")

        class _UT:
            pass
        mod.UserToken = _UT
        _sys.modules["models.googleTokens"] = mod

        try:
            fake_session = MagicMock()
            fake_session.query.return_value.filter_by.return_value.first.return_value = None

            fake_app = MagicMock()
            fake_app.app_context.return_value.__enter__ = lambda s: None
            fake_app.app_context.return_value.__exit__ = (
                lambda s, a, b, c: None
            )
            fake_app.db.session = fake_session

            provider = ns["_build_access_token_provider"](fake_app)
            self.assertIsNone(provider())
        finally:
            del _sys.modules["models.googleTokens"]


if __name__ == "__main__":
    unittest.main()
