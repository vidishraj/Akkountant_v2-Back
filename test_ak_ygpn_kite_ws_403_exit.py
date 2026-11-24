"""ak-ygpn + ak-m69p (2026-11-24): Kite WS daemon.

ak-ygpn: on a 403 (Kite rejecting a stale access token) the daemon must
exit non-zero so systemd restarts it and the next boot re-reads the token
from the DB. Before: kiteconnect's reconnecting factory retried the same
stale token forever inside reactor.run(), so ticker.connect() never
returned and run_ws_loop never reached its exit check.

The fake ticker below behaves like the real one in the way that matters:
connect() BLOCKS (the reactor) and keeps "retrying" (re-firing the close
callbacks) until stop() is called. autobahn reports a failed upgrade as an
unclean close code 1006 whose reason carries "403 - Forbidden", firing
on_error then on_close.

ak-m69p: close / error logs carry the code only (never the reason), and
the systemd start limit is finite and in [Unit] (see test_kite_ws_daemon).
No network.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from services import kite_ws_daemon as kd  # noqa: E402
from services.kite_ltp_ipc import STATE_RECONNECT  # noqa: E402

UPGRADE_403 = "WebSocket connection upgrade failed (403 - Forbidden) SECRET_TOKEN_FRAGMENT"
NETWORK_DROP = "connection was closed uncleanly (peer dropped the TCP connection) SECRET_TOKEN_FRAGMENT"


class _ReactorTicker:
    """connect() blocks like reactor.run() and re-fires `script` every
    `period` seconds (the SDK's retry loop) until stop() is called or the
    safety timeout expires."""

    MODE_LTP = "ltp"
    instances = []

    def __init__(self, api_key, access_token, script, period=0.01, timeout=3.0):
        self.on_ticks = self.on_connect = self.on_close = self.on_error = None
        self.on_noreconnect = None
        self.script = script
        self.period = period
        self.timeout = timeout
        self.stopped = threading.Event()
        self.stop_retry_called = False
        self.fires = 0
        _ReactorTicker.instances.append(self)

    def subscribe(self, tokens):
        pass

    def set_mode(self, mode, tokens):
        pass

    def stop_retry(self):
        self.stop_retry_called = True

    def stop(self):
        self.stopped.set()

    def connect(self, threaded=False):
        deadline = time.monotonic() + self.timeout
        while not self.stopped.is_set() and time.monotonic() < deadline:
            self.script(self)
            self.fires += 1
            self.stopped.wait(self.period)


def _fire_close(code, reason):
    def script(t):
        if t.on_error:
            t.on_error(t, code, reason)
        if t.on_close:
            t.on_close(t, code, reason)
    return script


class _Base(unittest.TestCase):

    def setUp(self):
        _ReactorTicker.instances = []
        self.log_records = []
        logger = logging.getLogger(f"akygpn.{id(self)}")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False

        class H(logging.Handler):
            def emit(h, record):
                self.log_records.append(record.getMessage())
        logger.addHandler(H())
        self.daemon = kd.KiteWsDaemon(
            api_key="k", access_token_provider=lambda: "stale-token",
            subscription_provider=lambda: {"NSE:X": 1}, logger=logger,
        )
        self._delay = mock.patch.object(kd, "EXIT_DELAY_S", 0.05)
        self._delay.start()

    def tearDown(self):
        self._delay.stop()
        kd.set_ticker_factory(None)
        self.daemon.stop()

    def use(self, script, **kw):
        kd.set_ticker_factory(lambda key, tok: _ReactorTicker(key, tok, script, **kw))

    def run_loop_in_thread(self):
        out = {}
        th = threading.Thread(target=lambda: out.setdefault("rc", self.daemon.run_ws_loop()),
                              daemon=True)
        th.start()
        return th, out


class TestAuthFailureDetection(unittest.TestCase):

    def test_table(self):
        yes = [(403, None), (1006, UPGRADE_403), (1006, b"upgrade failed (403 - Forbidden)"),
               (None, "HTTP 403"), (0, "403 Forbidden")]
        no = [(1006, NETWORK_DROP), (1006, None), (1006, ""), (1000, "normal"),
              (1006, "code 14030 retry"), (1006, "4031"), (None, None)]
        for code, reason in yes:
            self.assertTrue(kd._is_auth_failure(code, reason), (code, reason))
        for code, reason in no:
            self.assertFalse(kd._is_auth_failure(code, reason), (code, reason))

    def test_unprintable_reason_is_not_auth_failure(self):
        class Bad:
            def __str__(self):
                raise RuntimeError("boom")
        self.assertFalse(kd._is_auth_failure(1006, Bad()))


class TestExitOn403(_Base):

    def test_403_exits_non_zero_promptly(self):
        self.use(_fire_close(1006, UPGRADE_403))
        t0 = time.monotonic()
        rc = self.daemon.run_ws_loop()
        self.assertEqual(rc, 1)
        self.assertLess(time.monotonic() - t0, 2.0)  # not the fake's 3s safety timeout
        (ticker,) = _ReactorTicker.instances      # one connect attempt, no re-loop
        self.assertTrue(ticker.stopped.is_set())
        self.assertTrue(ticker.stop_retry_called)
        self.assertEqual(ticker.fires, 1)          # stopped on the first rejection
        self.assertEqual(self.daemon.get_state()["state"], STATE_RECONNECT)

    def test_either_callback_alone_is_enough(self):
        # autobahn fires on_error only for unclean closes; a clean close
        # with a 403 reason arrives via on_close alone, and vice versa.
        def only_close(t):
            t.on_close(t, 1006, UPGRADE_403)

        def only_error(t):
            t.on_error(t, 1006, UPGRADE_403)
        for script in (only_close, only_error):
            _ReactorTicker.instances = []
            self.daemon._fatal_ws_failure = False
            self.use(script)
            self.assertEqual(self.daemon.run_ws_loop(), 1, script.__name__)
            self.assertTrue(_ReactorTicker.instances[0].stopped.is_set(), script.__name__)

    def test_403_as_code_also_exits(self):
        self.use(_fire_close(403, None))
        self.assertEqual(self.daemon.run_ws_loop(), 1)

    def test_exit_waits_the_pause_and_honours_stop(self):
        self.use(_fire_close(1006, UPGRADE_403))
        with mock.patch.object(kd, "EXIT_DELAY_S", 5):
            th, out = self.run_loop_in_thread()
            time.sleep(0.3)
            self.assertTrue(th.is_alive())          # pausing, not exited yet
            self.daemon.stop()                      # e.g. systemctl stop
            th.join(2)
        self.assertFalse(th.is_alive())
        self.assertEqual(out["rc"], 0)

    def test_noreconnect_exits_non_zero(self):
        def script(t):
            if t.on_noreconnect:
                t.on_noreconnect(t)
        self.use(script, timeout=1.0)
        th, out = self.run_loop_in_thread()
        th.join(5)
        self.assertFalse(th.is_alive(), "daemon did not exit after the SDK gave up")
        self.daemon.stop()
        self.assertEqual(out.get("rc"), 1)
        self.assertTrue(_ReactorTicker.instances[0].stopped.is_set())


class TestTransientStaysAlive(_Base):

    def test_network_drop_keeps_sdk_retrying(self):
        self.use(_fire_close(1006, NETWORK_DROP), timeout=10.0)
        th, out = self.run_loop_in_thread()
        time.sleep(0.4)
        ticker = _ReactorTicker.instances[0]
        self.assertTrue(th.is_alive())
        self.assertFalse(ticker.stopped.is_set())
        self.assertFalse(ticker.stop_retry_called)
        self.assertGreater(ticker.fires, 5)        # SDK kept retrying
        self.assertFalse(self.daemon._fatal_ws_failure)
        ticker.stop()                              # end the fake reactor
        self.daemon.stop()
        th.join(3)
        self.assertFalse(th.is_alive())


class TestReviewPacing(_Base):
    """ak-ygpn review MAJOR: no fast-exit path may trip the unit's start
    limit (600s / 20). Only exit-2 misconfiguration exits at once."""

    def test_token_provider_db_error_is_paced_not_fatal(self):
        def boom():
            raise RuntimeError("OperationalError: mysql down postgres://u:SECRETPW@h")
        self.daemon._token_provider = boom
        th, out = self.run_loop_in_thread()
        time.sleep(0.3)
        self.assertTrue(th.is_alive())                 # waiting 30s, not crashed/exited
        self.assertEqual(self.daemon.get_state()["state"], STATE_RECONNECT)
        self.assertEqual(_ReactorTicker.instances, [])  # never tried to connect
        self.daemon.stop()
        th.join(3)
        self.assertEqual(out.get("rc"), 0)
        joined = "\n".join(self.log_records)
        self.assertIn("RuntimeError", joined)
        self.assertNotIn("SECRETPW", joined)

    def test_plain_connect_return_exits_paced_without_reconnecting(self):
        # connect() returning with state OK/INIT: the reactor can't be
        # restarted, so never re-call connect(); exit 1 after the pause.
        calls = []

        class OneShot:
            MODE_LTP = "ltp"

            def __init__(s, *a):
                s.on_ticks = s.on_connect = s.on_close = s.on_error = s.on_noreconnect = None

            def connect(s, threaded=False):
                calls.append(1)
        kd.set_ticker_factory(lambda k, t: OneShot())
        th, out = self.run_loop_in_thread()
        th.join(3)
        alive = th.is_alive()
        self.daemon.stop()
        self.assertFalse(alive, "re-called connect() instead of exiting")
        self.assertEqual(out.get("rc"), 1)
        self.assertEqual(len(calls), 1)

    def test_every_loop_exit_waits_the_pause(self):
        scripts = {
            "reconnect_required close": lambda t: t.on_close(t, 4000, "custom"),
            "403": _fire_close(1006, UPGRADE_403),
        }
        for name, script in scripts.items():
            _ReactorTicker.instances = []
            self.daemon._fatal_ws_failure = False
            self.daemon._exit_event.clear()

            def once(t, script=script):
                script(t)
                t.stop()      # connect() returns after the first event
            self.use(once)
            with mock.patch.object(kd, "EXIT_DELAY_S", 5):
                th, out = self.run_loop_in_thread()
                time.sleep(0.3)
                self.assertTrue(th.is_alive(), name)   # pausing before exit 1
                self.daemon._exit_event.set()
                th.join(2)
            self.assertEqual(out.get("rc"), 0, name)
            self.assertEqual(len(_ReactorTicker.instances), 1, name)


class TestEntrypointPacing(unittest.TestCase):

    def setUp(self):
        import importlib
        sys.path.insert(0, os.path.join(HERE, "scripts"))
        self.ep = importlib.import_module("kite_ws_daemon_entrypoint")

    def test_unexpected_exception_sleeps_then_exits_1(self):
        slept = []

        def main():
            raise ImportError("mysql connect failed for mysql://u:SECRETPW@h")
        with self.assertLogs("kite_ws_daemon", level="ERROR") as cm:
            rc = self.ep.run(main, sleep=slept.append)
        self.assertEqual(rc, 1)
        self.assertEqual(slept, [self.ep.UNEXPECTED_EXIT_DELAY_S])
        self.assertGreaterEqual(self.ep.UNEXPECTED_EXIT_DELAY_S, 30)
        self.assertNotIn("SECRETPW", "\n".join(cm.output))

    def test_misconfig_and_normal_exits_are_immediate(self):
        for code in (2, 0, 1):
            slept = []
            self.assertEqual(self.ep.run(lambda c=code: c, sleep=slept.append), code)
            self.assertEqual(slept, [])

    def test_docstring_no_longer_claims_unlimited_restarts(self):
        doc = self.ep.__doc__
        self.assertNotIn("StartLimitIntervalSec=0", doc)
        self.assertIn("StartLimitBurst=20", doc)


class TestLogHygiene(_Base):
    """ak-m69p: close / error log the code only."""

    def test_reason_never_logged(self):
        ws = mock.MagicMock()
        for reason in (UPGRADE_403, NETWORK_DROP, "SECRET_TOKEN_FRAGMENT 403 x",
                       "SECRET_TOKEN_FRAGMENT drop"):
            self.daemon._on_close(ws, 1006, reason)
            self.daemon._on_error(ws, 1006, reason)
        joined = "\n".join(self.log_records)
        self.assertIn("code=1006", joined)
        self.assertNotIn("SECRET_TOKEN_FRAGMENT", joined)
        self.assertNotIn("Forbidden", joined)
        self.assertNotIn("peer dropped", joined)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
