"""ak-l8a7: tests for the Kite per-order drill-down endpoints.

Two thin READ-ONLY wrappers:
  * GET /kite/orders/<order_id>/history → KiteService.get_order_history
  * GET /kite/orders/<order_id>/trades  → KiteService.get_order_trades

Same source-grep pattern as ak-zi54 / ak-h1mq / ak-ue7l — full
controller import pulls Flask + Firebase + Google chain
unavailable in CI, so we assert the shape from source.

What these tests pin down:
  (1) InvestmentService passthroughs delegate to the correct
      KiteService methods and are one-line-return (§23 #2 CONSUME).
  (2) Controller handlers exist with @Logger.standardLogger
      decorator + the expected signatures.
  (3) Handlers read firebase_id via g.get (ak-c8l hygiene).
  (4) Handlers catch KiteAuthError → _kiteReconnectResponse.
  (5) Handlers refuse empty / non-string order_id (400 before
      hitting Kite).
  (6) Handlers return 200 with the expected JSON key.
  (7) Handlers return 500 on unexpected exceptions.
  (8) Routes wired in app.py with the correct verb (GET).
  (9) READ-ONLY drift guard — no POST/PUT/DELETE/PATCH route
      against /kite/orders/<order_id>/{history,trades}.
  (10) KiteService wrappers defined + delegate to the right
       SDK method names.
"""

from __future__ import annotations

import os
import re
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ── (1) InvestmentService passthroughs ────────────────────────


class TestInvestmentServicePassthroughs(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("services/InvestmentService.py")

    def test_fetch_order_history_passthrough_defined(self):
        self.assertIn(
            "def fetchKiteOrderHistory(self, userId, order_id):",
            self.src,
        )
        # Must delegate to KiteService.get_order_history.
        self.assertTrue(
            re.search(
                r"def fetchKiteOrderHistory\(self, userId, order_id\):"
                r".*?return\s+self\.StockService\.kite_service"
                r"\.get_order_history\(\s*userId\s*,\s*order_id\s*,?\s*\)",
                self.src, re.DOTALL,
            ),
            "fetchKiteOrderHistory must delegate to "
            "KiteService.get_order_history(userId, order_id)",
        )

    def test_fetch_order_trades_passthrough_defined(self):
        self.assertIn(
            "def fetchKiteOrderTrades(self, userId, order_id):",
            self.src,
        )
        self.assertTrue(
            re.search(
                r"def fetchKiteOrderTrades\(self, userId, order_id\):"
                r".*?return\s+self\.StockService\.kite_service"
                r"\.get_order_trades\(\s*userId\s*,\s*order_id\s*,?\s*\)",
                self.src, re.DOTALL,
            ),
            "fetchKiteOrderTrades must delegate to "
            "KiteService.get_order_trades(userId, order_id)",
        )

    def test_passthrough_doesnt_reshape_history_response(self):
        """§23 #2 CONSUME: passthrough body MUST be a one-line
        return-delegate (plus docstring). FE consumes Kite's
        native order_history shape."""
        m = re.search(
            r'def fetchKiteOrderHistory\(self, userId, order_id\):\n'
            r'\s*"""[\s\S]*?"""\s*\n'
            r'\s*return\s+self\.StockService\.kite_service'
            r'\.get_order_history\(\s*\n?\s*userId,\s*order_id,?\s*\n?\s*\)\n',
            self.src,
        )
        self.assertIsNotNone(
            m,
            "fetchKiteOrderHistory must be a one-line return (docstring "
            "+ return delegate) — no reshaping allowed per §23 #2",
        )

    def test_passthrough_doesnt_reshape_trades_response(self):
        m = re.search(
            r'def fetchKiteOrderTrades\(self, userId, order_id\):\n'
            r'\s*"""[\s\S]*?"""\s*\n'
            r'\s*return\s+self\.StockService\.kite_service'
            r'\.get_order_trades\(\s*\n?\s*userId,\s*order_id,?\s*\n?\s*\)\n',
            self.src,
        )
        self.assertIsNotNone(
            m,
            "fetchKiteOrderTrades must be a one-line return (docstring "
            "+ return delegate) — no reshaping allowed per §23 #2",
        )


# ── (2) Controller handlers ───────────────────────────────────


class TestInvestmentsEPOrderHistoryHandler(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("controllers/investmentsEP.py")

    def test_history_handler_defined_with_logger_decorator(self):
        self.assertTrue(
            re.search(
                r"@Logger\.standardLogger\n"
                r"\s+def fetchKiteOrderHistory\(self, order_id\):",
                self.src,
            ),
            "fetchKiteOrderHistory handler missing or not "
            "decorated with @Logger.standardLogger",
        )

    def test_trades_handler_defined_with_logger_decorator(self):
        self.assertTrue(
            re.search(
                r"@Logger\.standardLogger\n"
                r"\s+def fetchKiteOrderTrades\(self, order_id\):",
                self.src,
            ),
            "fetchKiteOrderTrades handler missing or not "
            "decorated with @Logger.standardLogger",
        )

    def _handler_body(self, name):
        m = re.search(
            rf"def {name}\(self, order_id\):(.*?)(?=\n    @Logger"
            r"|\n    # ─|\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, f"{name} body not found")
        return m.group(1)

    def test_history_handler_reads_firebase_id_from_g(self):
        """ak-c8l IDOR sweep discipline: handler must read
        ``g.get('firebase_id')`` and NEVER the raw X-Firebase-ID
        header."""
        body = self._handler_body("fetchKiteOrderHistory")
        self.assertIn("g.get('firebase_id')", body)
        self.assertNotIn(
            'request.headers.get("X-Firebase-ID")', body,
            "fetchKiteOrderHistory reads raw X-Firebase-ID — "
            "ak-c8l IDOR regression",
        )
        self.assertNotIn(
            'request.headers.get(\'X-Firebase-ID\')', body,
        )

    def test_trades_handler_reads_firebase_id_from_g(self):
        body = self._handler_body("fetchKiteOrderTrades")
        self.assertIn("g.get('firebase_id')", body)
        self.assertNotIn(
            'request.headers.get("X-Firebase-ID")', body,
            "fetchKiteOrderTrades reads raw X-Firebase-ID — "
            "ak-c8l IDOR regression",
        )

    def test_history_handler_validates_order_id(self):
        """order_id must be a non-empty string — defensive
        pre-Kite check so a missing / malformed param returns
        400 instead of a confusing upstream error."""
        body = self._handler_body("fetchKiteOrderHistory")
        self.assertIn("if not order_id", body)
        # 400 must fire BEFORE the service call
        self.assertTrue(
            re.search(
                r"if not order_id[^:]*:.*?return jsonify\(.*?\),\s*400",
                body, re.DOTALL,
            ),
            "fetchKiteOrderHistory must 400 on empty order_id",
        )

    def test_trades_handler_validates_order_id(self):
        body = self._handler_body("fetchKiteOrderTrades")
        self.assertIn("if not order_id", body)
        self.assertTrue(
            re.search(
                r"if not order_id[^:]*:.*?return jsonify\(.*?\),\s*400",
                body, re.DOTALL,
            ),
            "fetchKiteOrderTrades must 400 on empty order_id",
        )

    def test_history_handler_catches_kite_auth_error(self):
        """KiteAuthError must route through _kiteReconnectResponse —
        the 6 AM IST daily token-expiry shape."""
        body = self._handler_body("fetchKiteOrderHistory")
        self.assertIn("except KiteAuthError", body)
        self.assertIn("self._kiteReconnectResponse", body)

    def test_trades_handler_catches_kite_auth_error(self):
        body = self._handler_body("fetchKiteOrderTrades")
        self.assertIn("except KiteAuthError", body)
        self.assertIn("self._kiteReconnectResponse", body)

    def test_history_handler_returns_200_with_history_key(self):
        """Response shape: {"history": [...]}. FE depends on key name."""
        self.assertTrue(
            re.search(
                r'return jsonify\(\{"history": history\}\), 200',
                self.src,
            ),
            "fetchKiteOrderHistory must return 200 with 'history' key",
        )

    def test_trades_handler_returns_200_with_trades_key(self):
        """Response shape: {"trades": [...]}. FE depends on key name."""
        self.assertTrue(
            re.search(
                r'return jsonify\(\{"trades": trades\}\), 200',
                self.src,
            ),
            "fetchKiteOrderTrades must return 200 with 'trades' key",
        )

    def test_history_handler_returns_500_on_unexpected_exception(self):
        self.assertIn('"Failed to fetch order history"', self.src)
        self.assertTrue(
            re.search(
                r'"Failed to fetch order history"\}\), 500', self.src,
            ),
        )

    def test_trades_handler_returns_500_on_unexpected_exception(self):
        self.assertIn('"Failed to fetch order trades"', self.src)
        self.assertTrue(
            re.search(
                r'"Failed to fetch order trades"\}\), 500', self.src,
            ),
        )


# ── (3) Route wire-up ─────────────────────────────────────────


def test_history_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/kite/orders/<order_id>/history'\s*,\s*'GET'\s*,\s*"
        r"self\.investmentEP\.fetchKiteOrderHistory",
        src,
    ), (
        "GET /kite/orders/<order_id>/history route not wired in app.py"
    )


def test_trades_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/kite/orders/<order_id>/trades'\s*,\s*'GET'\s*,\s*"
        r"self\.investmentEP\.fetchKiteOrderTrades",
        src,
    ), (
        "GET /kite/orders/<order_id>/trades route not wired in app.py"
    )


# ── (4) READ-ONLY drift guard — reject mutations ──────────────


def test_order_drilldown_routes_are_read_only():
    """ak-l8a7 is READ-ONLY per bead. Any mutation verb
    (POST / PUT / DELETE / PATCH) against the drill-down
    endpoints would indicate accidental introduction of
    order-mutation paths (which are explicitly deferred to a
    separate bead). Broadened from ak-zi54's GET-only pattern
    per the ak-1gme precedent.
    """
    src = _read_source("app.py")
    # Scan for any route-tuple targeting /kite/orders/<...> with
    # a mutation verb.
    all_entries = re.findall(
        r"\(\s*('/kite/orders/<[^>]+>[^']*')\s*,\s*('[A-Z]+')\s*,",
        src,
    )
    for rule, verb in all_entries:
        v = verb.strip("'")
        assert v not in ("POST", "PUT", "DELETE", "PATCH"), (
            f"READ-ONLY drift: {rule} {verb} must be GET-only "
            f"(order mutations deferred to a separate bead)."
        )


# ── (5) KiteService wrapper regression guards ─────────────────


def test_kite_service_get_order_history_defined():
    src = _read_source("services/KiteService.py")
    assert re.search(
        r"def get_order_history\(self, user_id, order_id\):", src,
    ), (
        "KiteService.get_order_history missing — "
        "/kite/orders/<id>/history endpoint will 500"
    )
    # Must call Kite SDK's order_history (NOT order_trades by mistake).
    assert re.search(
        r"k\.order_history\(order_id=order_id\)", src,
    ), (
        "KiteService.get_order_history must call k.order_history "
        "(SDK method name)"
    )


def test_kite_service_get_order_trades_defined():
    src = _read_source("services/KiteService.py")
    assert re.search(
        r"def get_order_trades\(self, user_id, order_id\):", src,
    ), (
        "KiteService.get_order_trades missing — "
        "/kite/orders/<id>/trades endpoint will 500"
    )
    assert re.search(
        r"k\.order_trades\(order_id=order_id\)", src,
    ), (
        "KiteService.get_order_trades must call k.order_trades "
        "(SDK method name)"
    )


def test_kite_service_wrappers_route_through_call():
    """Both wrappers must go through ``_call`` so KiteTokenExpired
    surfaces as KiteAuthError (reconnect-prompt pattern). A bare
    ``self.kite.order_history(...)`` would skip the token-liveness
    translation and the FE would see opaque 500s on daily expiry.
    """
    src = _read_source("services/KiteService.py")
    # get_order_history body goes through _call
    m = re.search(
        r"def get_order_history\(self, user_id, order_id\):(.*?)"
        r"(?=\n    def [a-zA-Z_]|\nclass )",
        src, re.DOTALL,
    )
    assert m, "get_order_history body not found"
    assert "self._call(" in m.group(1), (
        "get_order_history must route through self._call so "
        "KiteTokenExpired surfaces as KiteAuthError"
    )
    # get_order_trades body goes through _call
    m = re.search(
        r"def get_order_trades\(self, user_id, order_id\):(.*?)"
        r"(?=\n    def [a-zA-Z_]|\nclass )",
        src, re.DOTALL,
    )
    assert m, "get_order_trades body not found"
    assert "self._call(" in m.group(1), (
        "get_order_trades must route through self._call so "
        "KiteTokenExpired surfaces as KiteAuthError"
    )


# ── (6) Distinctness guard: history ≠ trades ──────────────────


def test_history_and_trades_delegate_to_different_methods():
    """The bead's core semantic: /history (status transitions)
    and /trades (per-fill breakdown) are two DIFFERENT Kite SDK
    calls. A refactor that collapses them would break both
    surfaces.
    """
    src = _read_source("services/InvestmentService.py")
    assert re.search(
        r"def fetchKiteOrderHistory.*?get_order_history\("
        r"\s*userId,\s*order_id",
        src, re.DOTALL,
    ), "fetchKiteOrderHistory must delegate to get_order_history"
    assert re.search(
        r"def fetchKiteOrderTrades.*?get_order_trades\("
        r"\s*userId,\s*order_id",
        src, re.DOTALL,
    ), "fetchKiteOrderTrades must delegate to get_order_trades"


if __name__ == "__main__":
    unittest.main()
