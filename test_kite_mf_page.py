"""ak-h1mq: tests for the Kite MF-page BE endpoints.

Three thin READ-ONLY wrappers over existing ``KiteService``
methods:

  * GET /kite/mf/holdings → KiteService.get_mf_holdings
  * GET /kite/mf/orders   → KiteService.get_mf_orders
  * GET /kite/mf/sips     → KiteService.get_mf_sips

Scope: wire-up + response shape + KiteAuthError reconnect-prompt
handling + §23 #2 (CONSUME: Kite's native shape flows through
unchanged). READ-ONLY per bead — no destructive paths wired.

Pure source-grep + wire-up assertions — full controller import
pulls the Flask + Firebase + Google chain unavailable in CI. Same
pattern as test_kite_bundle2 / test_reconciliation_issues_endpoint.
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


# ── (1) InvestmentService passthroughs ───────────────────────────────


class TestInvestmentServicePassthroughs(unittest.TestCase):
    """The three MF methods must live on InvestmentService as
    thin pass-throughs to KiteService — matches the existing Kite
    wrapper pattern (``fetchKiteTrades`` / ``fetchKiteHoldings``
    etc.) so the controller stays uniform."""

    def setUp(self):
        self.src = _read_source("services/InvestmentService.py")

    def test_fetch_mf_holdings_passthrough_defined(self):
        self.assertIn(
            "def fetchKiteMFHoldings(self, userId):", self.src,
        )
        # Must delegate to KiteService.get_mf_holdings — not re-
        # implement or reshape.
        self.assertTrue(
            re.search(
                r"def fetchKiteMFHoldings.*?"
                r"return\s+self\.StockService\.kite_service"
                r"\.get_mf_holdings\(userId\)",
                self.src, re.DOTALL,
            ),
            "fetchKiteMFHoldings must delegate to "
            "KiteService.get_mf_holdings",
        )

    def test_fetch_mf_orders_passthrough_defined(self):
        self.assertIn(
            "def fetchKiteMFOrders(self, userId):", self.src,
        )
        self.assertTrue(
            re.search(
                r"def fetchKiteMFOrders.*?"
                r"return\s+self\.StockService\.kite_service"
                r"\.get_mf_orders\(userId\)",
                self.src, re.DOTALL,
            ),
            "fetchKiteMFOrders must delegate to "
            "KiteService.get_mf_orders",
        )

    def test_fetch_mf_sips_passthrough_defined(self):
        self.assertIn(
            "def fetchKiteMFSips(self, userId):", self.src,
        )
        self.assertTrue(
            re.search(
                r"def fetchKiteMFSips.*?"
                r"return\s+self\.StockService\.kite_service"
                r"\.get_mf_sips\(userId\)",
                self.src, re.DOTALL,
            ),
            "fetchKiteMFSips must delegate to KiteService.get_mf_sips",
        )

    def test_passthroughs_dont_reshape_kite_response(self):
        """§23 #2 CONSUME: the passthrough must NOT transform /
        serialize / filter the Kite response — FE consumes Kite's
        native shape directly. Pin by asserting the function body
        is a single ``return`` statement (no intermediate
        processing).
        """
        for fn in ("fetchKiteMFHoldings", "fetchKiteMFOrders",
                   "fetchKiteMFSips"):
            # Match the function body — one ``return`` statement.
            # Allow a docstring before the return.
            m = re.search(
                rf'def {fn}\(self, userId\):\n'
                rf'\s*"""[\s\S]*?"""\s*\n'
                rf'\s*return\s+self\.StockService\.kite_service'
                rf'\.get_mf_\w+\(userId\)\n',
                self.src,
            )
            self.assertIsNotNone(
                m,
                f"{fn} must be a one-line return (docstring + "
                f"return delegate) — no reshaping allowed per §23 #2",
            )


# ── (2) Controller endpoints ─────────────────────────────────────────


class TestInvestmentsEPMFHandlers(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("controllers/investmentsEP.py")

    def test_mf_holdings_handler_defined(self):
        self.assertTrue(
            re.search(
                r"@Logger\.standardLogger\n"
                r"\s+def fetchKiteMFHoldings\(self\):",
                self.src,
            ),
            "fetchKiteMFHoldings handler missing or not decorated "
            "with @Logger.standardLogger",
        )

    def test_mf_orders_handler_defined(self):
        self.assertTrue(
            re.search(
                r"@Logger\.standardLogger\n"
                r"\s+def fetchKiteMFOrders\(self\):",
                self.src,
            ),
        )

    def test_mf_sips_handler_defined(self):
        self.assertTrue(
            re.search(
                r"@Logger\.standardLogger\n"
                r"\s+def fetchKiteMFSips\(self\):",
                self.src,
            ),
        )

    def test_handlers_read_firebase_id_from_g(self):
        """Every MF handler must read ``g.get('firebase_id')``, NOT
        the raw X-Firebase-ID header. Keeps uniformity with the
        ak-c8l IDOR fix sweep — raw-header reads would bypass JWT
        verification in prod."""
        for fn in ("fetchKiteMFHoldings", "fetchKiteMFOrders",
                   "fetchKiteMFSips"):
            m = re.search(
                rf"def {fn}\(self\):(.*?)except Exception",
                self.src, re.DOTALL,
            )
            self.assertIsNotNone(m, f"{fn} body not found")
            body = m.group(1)
            self.assertIn(
                "g.get('firebase_id')", body,
                f"{fn} must read verified uid via g.get('firebase_id')",
            )
            # And must NOT read the raw X-Firebase-ID header (ak-c8l
            # drift guard applied to the new endpoints).
            self.assertNotIn(
                'request.headers.get("X-Firebase-ID")', body,
                f"{fn} must not read raw X-Firebase-ID header",
            )

    def test_handlers_catch_kite_auth_error_for_reconnect_prompt(self):
        """KiteAuthError (including the subtype KiteTokenExpired)
        must surface via self._kiteReconnectResponse so the FE
        prompts the user to re-auth. Dropping that branch would
        make the endpoints return opaque 500s on the daily 6 AM
        IST token-expiry."""
        for fn in ("fetchKiteMFHoldings", "fetchKiteMFOrders",
                   "fetchKiteMFSips"):
            m = re.search(
                rf"def {fn}\(self\):(.*?)(?=\n    @Logger\.standardLogger"
                rf"|\n    # ─|\n    def [a-zA-Z_])",
                self.src, re.DOTALL,
            )
            self.assertIsNotNone(m, f"{fn} body not found")
            body = m.group(1)
            self.assertIn(
                "except KiteAuthError", body,
                f"{fn} must handle KiteAuthError for reconnect prompt",
            )
            self.assertIn(
                "self._kiteReconnectResponse", body,
                f"{fn} must route KiteAuthError through "
                f"_kiteReconnectResponse",
            )

    def test_handlers_return_200_on_success(self):
        for fn, shape in (
            ("fetchKiteMFHoldings", "holdings"),
            ("fetchKiteMFOrders", "orders"),
            ("fetchKiteMFSips", "sips"),
        ):
            m = re.search(
                rf'return jsonify\(\{{"{shape}": \w+\}}\), 200',
                self.src,
            )
            self.assertIsNotNone(
                m, f"{fn} must return 200 with key {shape!r}",
            )

    def test_handlers_return_500_on_unexpected_exception(self):
        """Fallback catch-all must return 500, not re-raise.
        Matches the existing Kite-wrapper pattern (fetchKiteTrades
        etc.)."""
        for fn, msg in (
            ("fetchKiteMFHoldings", "Failed to fetch MF holdings"),
            ("fetchKiteMFOrders", "Failed to fetch MF orders"),
            ("fetchKiteMFSips", "Failed to fetch MF SIPs"),
        ):
            m = re.search(
                rf'"{re.escape(msg)}"\}}\), 500', self.src,
            )
            self.assertIsNotNone(
                m, f"{fn} must return 500 with message {msg!r}",
            )


# ── (3) Route wire-up ────────────────────────────────────────────────


def test_mf_holdings_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/kite/mf/holdings'\s*,\s*'GET'\s*,\s*"
        r"self\.investmentEP\.fetchKiteMFHoldings",
        src,
    ), "GET /kite/mf/holdings route not wired in app.py"


def test_mf_orders_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/kite/mf/orders'\s*,\s*'GET'\s*,\s*"
        r"self\.investmentEP\.fetchKiteMFOrders",
        src,
    ), "GET /kite/mf/orders route not wired in app.py"


def test_mf_sips_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/kite/mf/sips'\s*,\s*'GET'\s*,\s*"
        r"self\.investmentEP\.fetchKiteMFSips",
        src,
    ), "GET /kite/mf/sips route not wired in app.py"


def test_all_three_routes_use_GET_method():
    """ak-h1mq is READ-ONLY per bead — a POST route here would
    indicate accidental mutation-path introduction. Drift guard
    catches that regression."""
    src = _read_source("app.py")
    for path in ("/kite/mf/holdings", "/kite/mf/orders", "/kite/mf/sips"):
        # No POST route for these paths — verify no 'POST' sits
        # adjacent to the path string.
        assert not re.search(
            rf"'{re.escape(path)}'\s*,\s*'POST'", src,
        ), (
            f"{path} must stay READ-ONLY (GET-only) — POST route "
            f"would indicate accidental mutation-path"
        )


# ── (4) KiteService method presence (regression guard) ──────────────


def test_kite_service_mf_methods_still_defined():
    """The 3 BE methods already existed before ak-h1mq (lines
    342/348/354 per bead). Guard against someone removing them
    thinking they were unused dead code — they're the fetch path
    for the endpoints."""
    src = _read_source("services/KiteService.py")
    for method in ("get_mf_holdings", "get_mf_orders", "get_mf_sips"):
        assert re.search(
            rf"def {method}\(self, user_id\):", src,
        ), f"KiteService.{method} missing — endpoints will 500"


if __name__ == "__main__":
    unittest.main()
