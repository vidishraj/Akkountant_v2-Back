"""ak-zi54: tests for the Kite today's-orders endpoint.

One thin READ-ONLY wrapper:
  * GET /kite/orders → KiteService.get_orders

Unlike /kite/trades (which returns FILLED orders only), this
returns Kite's full daily order book across every status
(OPEN / COMPLETE / CANCELLED / REJECTED / TRIGGER_PENDING).

Same shape as the ak-h1mq pattern: InvestmentService passthrough
+ controller handler + route wire-up + always-run drift guards.
Source-grep tests (full controller import pulls the Flask +
Firebase + Google chain unavailable in CI).
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


# ── (1) InvestmentService passthrough ────────────────────────────────


class TestInvestmentServicePassthrough(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("services/InvestmentService.py")

    def test_fetch_orders_passthrough_defined(self):
        self.assertIn(
            "def fetchKiteOrders(self, userId):", self.src,
        )
        # Must delegate to KiteService.get_orders — not re-implement
        # or reshape.
        self.assertTrue(
            re.search(
                r"def fetchKiteOrders.*?"
                r"return\s+self\.StockService\.kite_service"
                r"\.get_orders\(userId\)",
                self.src, re.DOTALL,
            ),
            "fetchKiteOrders must delegate to KiteService.get_orders",
        )

    def test_passthrough_doesnt_reshape_kite_response(self):
        """§23 #2 CONSUME: passthrough body MUST be a one-line
        return-delegate (plus docstring). No intermediate
        processing — FE consumes Kite's native order shape."""
        m = re.search(
            r'def fetchKiteOrders\(self, userId\):\n'
            r'\s*"""[\s\S]*?"""\s*\n'
            r'\s*return\s+self\.StockService\.kite_service'
            r'\.get_orders\(userId\)\n',
            self.src,
        )
        self.assertIsNotNone(
            m,
            "fetchKiteOrders must be a one-line return (docstring + "
            "return delegate) — no reshaping allowed per §23 #2",
        )


# ── (2) Controller handler ───────────────────────────────────────────


class TestInvestmentsEPOrdersHandler(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("controllers/investmentsEP.py")

    def test_orders_handler_defined(self):
        self.assertTrue(
            re.search(
                r"@Logger\.standardLogger\n"
                r"\s+def fetchKiteOrders\(self\):",
                self.src,
            ),
            "fetchKiteOrders handler missing or not decorated "
            "with @Logger.standardLogger",
        )

    def test_handler_reads_firebase_id_from_g(self):
        """Handler must read ``g.get('firebase_id')``, NOT the raw
        X-Firebase-ID header (ak-c8l IDOR sweep discipline applied
        to new paths)."""
        m = re.search(
            r"def fetchKiteOrders\(self\):(.*?)except Exception",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, "fetchKiteOrders body not found")
        body = m.group(1)
        self.assertIn(
            "g.get('firebase_id')", body,
            "fetchKiteOrders must read verified uid via "
            "g.get('firebase_id') — not the raw X-Firebase-ID header",
        )
        self.assertNotIn(
            'request.headers.get("X-Firebase-ID")', body,
            "fetchKiteOrders must not read raw X-Firebase-ID header "
            "(ak-c8l IDOR regression)",
        )

    def test_handler_catches_kite_auth_error(self):
        """KiteAuthError (incl. KiteTokenExpired) must route through
        _kiteReconnectResponse so the FE gets the reconnect-prompt
        shape on the daily 6 AM IST token-expiry. Dropping this
        branch makes the endpoint return opaque 500s."""
        m = re.search(
            r"def fetchKiteOrders\(self\):(.*?)(?=\n    @Logger"
            r"|\n    # ─|\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, "fetchKiteOrders body not found")
        body = m.group(1)
        self.assertIn(
            "except KiteAuthError", body,
            "fetchKiteOrders must handle KiteAuthError for "
            "reconnect prompt",
        )
        self.assertIn(
            "self._kiteReconnectResponse", body,
            "fetchKiteOrders must route KiteAuthError through "
            "_kiteReconnectResponse",
        )

    def test_handler_returns_200_with_orders_key(self):
        """Response shape: {\"orders\": [...]}. FE depends on the
        exact key name."""
        self.assertTrue(
            re.search(
                r'return jsonify\(\{"orders": orders\}\), 200',
                self.src,
            ),
            "fetchKiteOrders must return 200 with 'orders' key",
        )

    def test_handler_returns_500_on_unexpected_exception(self):
        self.assertIn(
            '"Failed to fetch orders"', self.src,
        )
        self.assertTrue(
            re.search(
                r'"Failed to fetch orders"\}\), 500', self.src,
            ),
            "fetchKiteOrders fallback must return 500",
        )


# ── (3) Route wire-up ────────────────────────────────────────────────


def test_orders_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/kite/orders'\s*,\s*'GET'\s*,\s*"
        r"self\.investmentEP\.fetchKiteOrders",
        src,
    ), "GET /kite/orders route not wired in app.py"


def test_orders_route_is_GET_only():
    """ak-zi54 is READ-ONLY per bead — a POST route here would
    indicate accidental mutation-path introduction (placing /
    modifying / cancelling orders). Drift guard catches that
    regression."""
    src = _read_source("app.py")
    assert not re.search(
        r"'/kite/orders'\s*,\s*'POST'", src,
    ), (
        "/kite/orders must stay READ-ONLY (GET-only) — POST route "
        "would indicate accidental mutation-path (order placement "
        "is explicitly deferred to a separate bead)."
    )


# ── (4) KiteService regression guard ─────────────────────────────────


def test_kite_service_get_orders_still_defined():
    """KiteService.get_orders (line 330-ish per bead) is the fetch
    path. Guard against someone removing it thinking it was unused
    — the endpoint will 500 on absent method."""
    src = _read_source("services/KiteService.py")
    assert re.search(
        r"def get_orders\(self, user_id\):", src,
    ), "KiteService.get_orders missing — /kite/orders endpoint will 500"


# ── (5) Kite trades vs orders are distinct ───────────────────────────


def test_orders_endpoint_is_distinct_from_trades():
    """The bead's core semantic: /kite/orders ≠ /kite/trades.
    - /kite/trades → KiteService.get_trades → FILLED orders only
    - /kite/orders → KiteService.get_orders → every order today
      across all statuses

    Drift guard: the two passthroughs must delegate to DIFFERENT
    underlying methods. A refactor that collapses them would
    strip the status-chip semantic the FE depends on.
    """
    src = _read_source("services/InvestmentService.py")
    # fetchKiteTrades delegates to get_trades.
    assert re.search(
        r"def fetchKiteTrades.*?get_trades\(userId\)",
        src, re.DOTALL,
    ), "fetchKiteTrades must delegate to get_trades"
    # fetchKiteOrders delegates to get_orders.
    assert re.search(
        r"def fetchKiteOrders.*?get_orders\(userId\)",
        src, re.DOTALL,
    ), "fetchKiteOrders must delegate to get_orders"


if __name__ == "__main__":
    unittest.main()
