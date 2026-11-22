"""ak-99gt (2026-11-22): unit tests for the runtime 301-WARN probe
in utils/nse_health.py.

Complements the ak-3hkb CI drift guard. Covers the three branches
``probe()`` walks:

  1. 200 response → no WARN (steady state after ak-3hkb landed .in)
  2. 3xx response → WARN with Location header + current URL +
     ak-3hkb bead-pointer so journalctl grep lands directly on
     the migration signal.
  3. Any exception (requests error, DNS, timeout, missing
     ``_BASE_URL`` on nse_patch) → fail-soft INFO; probe MUST NOT
     block scheduler boot.

Also includes a source-level drift guard asserting the scheduler
calls ``probe()`` from ``start_scheduler()`` — missing this wiring
turns ak-99gt into dead code.
"""
import ast
import logging
import os
import sys
import unittest
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_HERE = os.path.dirname(os.path.abspath(__file__))
_SCHEDULER_PATH = os.path.join(_HERE, "services", "tasks", "scheduler.py")


def _source(path):
    with open(path) as fh:
        return fh.read()


def _fresh_import_nse_health():
    """Reload ``utils.nse_health`` so each test gets a clean slate.
    Required because the module's late imports in ``probe()`` are
    re-done per call anyway, but defensive against any future
    module-state addition."""
    import importlib
    if "utils.nse_health" in sys.modules:
        return importlib.reload(sys.modules["utils.nse_health"])
    import utils.nse_health
    return utils.nse_health


def _real_nse_patch_module():
    """Return the REAL utils.nse_patch module. In isolation this
    is a fresh import; in full-suite an earlier test has already
    real-imported it (test pollution). Either way, probe()'s
    ``from utils import nse_patch`` late binding resolves to the
    attribute on the ``utils`` package, which is set at submodule
    import time and NOT reset by ``patch.dict(sys.modules, ...)``.
    So tests must patch attributes on THIS module directly
    (``patch.object(mod, "_BASE_URL", <val>)``), not replace the
    module in sys.modules."""
    import utils.nse_patch
    return utils.nse_patch


class _CaptureHandler(logging.Handler):
    """Collect every emitted record for in-test inspection. Resets
    per test via setUp."""

    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record):
        self.records.append(record)

    def messages_at(self, level):
        return [
            r.getMessage() for r in self.records if r.levelno == level
        ]


class _FakeResponse:
    """Minimal requests.Response stand-in. Only the attributes
    probe() reads."""

    def __init__(self, status_code, location=""):
        self.status_code = status_code
        self.headers = {"Location": location} if location else {}


class TestAkt99gtProbeBehavior(unittest.TestCase):

    def setUp(self):
        self.capture = _CaptureHandler()
        self.capture.setLevel(logging.DEBUG)
        self.logger = logging.getLogger("test_nse_health.capture")
        self.logger.setLevel(logging.DEBUG)
        # Clear any handler left from a prior test + attach fresh.
        self.logger.handlers.clear()
        self.logger.addHandler(self.capture)
        self.logger.propagate = False

    # ── 1: 200 response — silent (no WARN) ──────────────────────────

    def test_steady_state_200_logs_info_no_warning(self):
        nse_health = _fresh_import_nse_health()
        nse_patch = _real_nse_patch_module()
        fake_requests = MagicMock()
        fake_requests.head.return_value = _FakeResponse(200)

        with patch.dict(sys.modules, {"requests": fake_requests}), \
                patch.object(nse_patch, "_BASE_URL",
                             "https://www.nseindia.in/api/..."):
            nse_health.probe(logger=self.logger)

        # No WARN — the steady-state .in URL responds 200.
        self.assertEqual(
            self.capture.messages_at(logging.WARNING), [],
            "200 steady-state must NOT fire a WARN — would create "
            "journalctl noise masking the actual migration signal",
        )
        # Diagnostic INFO still logged so operators see the probe
        # ran (useful when debugging "is this feature even wired?").
        info_msgs = self.capture.messages_at(logging.INFO)
        self.assertTrue(
            any("no redirect" in m for m in info_msgs),
            f"expected 'no redirect' INFO line; got {info_msgs}",
        )

    # ── 2: 3xx response — WARN with full context ────────────────────

    def test_301_redirect_fires_warning_with_location(self):
        nse_health = _fresh_import_nse_health()
        nse_patch = _real_nse_patch_module()
        fake_requests = MagicMock()
        fake_requests.head.return_value = _FakeResponse(
            301,
            location="https://www.nseindia.co.jp/api/...BHEL",
        )

        with patch.dict(sys.modules, {"requests": fake_requests}), \
                patch.object(nse_patch, "_BASE_URL",
                             "https://www.nseindia.in/api/..."):
            nse_health.probe(logger=self.logger)

        warn_msgs = self.capture.messages_at(logging.WARNING)
        self.assertEqual(
            len(warn_msgs), 1,
            f"expected exactly 1 WARN on 301 response; got "
            f"{len(warn_msgs)}: {warn_msgs}",
        )
        msg = warn_msgs[0]
        # The grep anchor + full-context fields operators need.
        self.assertIn("NSE may have moved", msg)
        self.assertIn("nseindia.co.jp", msg)      # new Location
        self.assertIn("nseindia.in", msg)         # current _BASE_URL
        self.assertIn("301", msg)                 # status code
        self.assertIn("ak-3hkb", msg)             # bead pointer

    def test_302_redirect_also_fires_warning(self):
        """Edge: 302 is in the 3xx band too. Same signal as 301."""
        nse_health = _fresh_import_nse_health()
        nse_patch = _real_nse_patch_module()
        fake_requests = MagicMock()
        fake_requests.head.return_value = _FakeResponse(
            302, location="https://example.org/",
        )

        with patch.dict(sys.modules, {"requests": fake_requests}), \
                patch.object(nse_patch, "_BASE_URL",
                             "https://www.nseindia.in/api/..."):
            nse_health.probe(logger=self.logger)

        warn_msgs = self.capture.messages_at(logging.WARNING)
        self.assertEqual(len(warn_msgs), 1)
        self.assertIn("302", warn_msgs[0])

    def test_4xx_does_not_fire_warning(self):
        """A 404 (symbol unlisted / endpoint moved under a 404 shape)
        is NOT the migration signal — don't false-alarm. Logged at
        INFO so operators still see the probe ran."""
        nse_health = _fresh_import_nse_health()
        nse_patch = _real_nse_patch_module()
        fake_requests = MagicMock()
        fake_requests.head.return_value = _FakeResponse(404)

        with patch.dict(sys.modules, {"requests": fake_requests}), \
                patch.object(nse_patch, "_BASE_URL",
                             "https://www.nseindia.in/api/..."):
            nse_health.probe(logger=self.logger)

        self.assertEqual(self.capture.messages_at(logging.WARNING), [])

    # ── 3: Fail-soft — any exception swallowed ──────────────────────

    def test_network_error_swallowed_as_info(self):
        nse_health = _fresh_import_nse_health()
        nse_patch = _real_nse_patch_module()
        fake_requests = MagicMock()
        fake_requests.head.side_effect = ConnectionError("DNS down")

        with patch.dict(sys.modules, {"requests": fake_requests}), \
                patch.object(nse_patch, "_BASE_URL",
                             "https://www.nseindia.in/api/..."):
            # MUST NOT raise.
            nse_health.probe(logger=self.logger)

        # Logged at INFO with error class + message.
        info_msgs = self.capture.messages_at(logging.INFO)
        self.assertTrue(
            any("probe skipped due to error" in m for m in info_msgs),
            f"expected 'probe skipped due to error' INFO; got {info_msgs}",
        )
        self.assertTrue(
            any("ConnectionError" in m for m in info_msgs),
            "error class must appear in the INFO line",
        )
        # And NOT WARN — the probe failed, not the pin.
        self.assertEqual(self.capture.messages_at(logging.WARNING), [])

    def test_timeout_swallowed_as_info(self):
        nse_health = _fresh_import_nse_health()
        nse_patch = _real_nse_patch_module()
        fake_requests = MagicMock()
        fake_requests.head.side_effect = TimeoutError("read timed out")

        with patch.dict(sys.modules, {"requests": fake_requests}), \
                patch.object(nse_patch, "_BASE_URL",
                             "https://www.nseindia.in/api/..."):
            nse_health.probe(logger=self.logger)

        self.assertEqual(self.capture.messages_at(logging.WARNING), [])

    def test_missing_base_url_skips_cleanly(self):
        """If utils.nse_patch loses its _BASE_URL (unlikely — would
        be an ak-3hkb-level regression), the probe must not crash."""
        nse_health = _fresh_import_nse_health()
        nse_patch = _real_nse_patch_module()
        fake_requests = MagicMock()

        with patch.dict(sys.modules, {"requests": fake_requests}), \
                patch.object(nse_patch, "_BASE_URL", None):
            nse_health.probe(logger=self.logger)

        # No HEAD request was made — probe short-circuited.
        fake_requests.head.assert_not_called()
        # INFO about skipping.
        info_msgs = self.capture.messages_at(logging.INFO)
        self.assertTrue(
            any("_BASE_URL not set" in m for m in info_msgs),
            f"expected '_BASE_URL not set' skip message; got {info_msgs}",
        )
        self.assertEqual(self.capture.messages_at(logging.WARNING), [])


# ── 4: Scheduler wiring drift guard ──────────────────────────────────


class TestAkt99gtSchedulerWiring(unittest.TestCase):
    """TaskScheduler.start_scheduler must call nse_health.probe. If
    this wiring regresses, the probe becomes dead code."""

    def setUp(self):
        self.src = _source(_SCHEDULER_PATH)
        self.tree = ast.parse(self.src)

    def _method_source(self, class_name, method_name):
        for node in self.tree.body:
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                for item in node.body:
                    if (
                        isinstance(item, ast.FunctionDef)
                        and item.name == method_name
                    ):
                        return ast.get_source_segment(self.src, item)
        return None

    def test_start_scheduler_imports_nse_health_probe(self):
        body = self._method_source("TaskScheduler", "start_scheduler")
        self.assertIsNotNone(body, "start_scheduler not found")
        self.assertIn("from utils.nse_health import probe", body)

    def test_start_scheduler_calls_probe_before_threads(self):
        """Probe must fire BEFORE the daemon threads kick off so a
        slow HEAD doesn't race the thread-boundary guards. Position
        check via source text (ast.get_source_segment preserves
        line order)."""
        body = self._method_source("TaskScheduler", "start_scheduler")
        self.assertIsNotNone(body)
        probe_pos = body.find("_nse_health_probe(")
        threads_pos = body.find("_run_overdue_scheduler, daemon=True")
        self.assertGreater(probe_pos, -1, "probe() call missing")
        self.assertGreater(threads_pos, -1, "thread spawn missing")
        self.assertLess(
            probe_pos, threads_pos,
            "nse_health.probe() must fire BEFORE scheduler threads "
            "start — otherwise a slow HEAD races the per-thread "
            "boundary assertions in ak-l0u",
        )

    def test_probe_call_is_try_wrapped(self):
        """Belt-and-braces: even though probe() swallows its own
        exceptions, start_scheduler must try/except around it so a
        future refactor that lets probe() raise can't take down the
        scheduler."""
        body = self._method_source("TaskScheduler", "start_scheduler")
        self.assertIsNotNone(body)
        # Find the try: block containing the probe call.
        self.assertIn("try:", body)
        self.assertIn("_nse_health_probe(logger=self.logger)", body)
        self.assertIn("except Exception", body)
        # And the except branch logs but doesn't re-raise — grep
        # for the fail-soft message tag.
        self.assertIn("ignoring (", body)


if __name__ == "__main__":
    unittest.main(verbosity=2)
