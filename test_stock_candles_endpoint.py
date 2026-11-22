"""ak-ue7l: tests for the per-stock OHLC candles endpoint.

One READ-ONLY endpoint serving from the LOCAL
``stockHistoricalCandles`` cache (populated daily by
``BackfillHistoricalCandlesTask`` / ak-4o41) — NOT from live Kite.
Serving live would burn Kite's 3 req/s historical rate limit per
chart render AND would 401 on the daily 6 AM IST token-expiry
window. ak-4o41 exists specifically so chart loads don't do that.

Shape:
  GET /investments/stock/<security_code>/candles?range=<N>d|y
    → {"candles": [...], "security_code": "..."}

ak-1gme broadened drift-guard regex applied (``POST|PUT|DELETE|
PATCH``) — ak-h1mq used POST-only which missed PUT/DELETE/PATCH.
Catches a wider class of accidental-mutation-path regressions.

Source-grep tests + AST-lifted ``_parse_range_days`` pure-logic
tests. Full controller import pulls the Flask + Firebase + Google
chain unavailable in CI.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ── AST-lift _parse_range_days for pure-logic tests ──────────────────


def _ast_lift_parse_range_days():
    """Lift ``InvestmentService._parse_range_days`` + the three
    ``_CANDLES_*`` constants it depends on so the parser can be
    tested without the full Flask + Firebase + Google chain."""
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "InvestmentService.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)

    want_constants = {
        "_CANDLES_MIN_DAYS", "_CANDLES_MAX_DAYS",
        "_CANDLES_DEFAULT_DAYS",
    }
    kept = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            # Pull both the class-level constants + the method.
            kept_class_body = []
            for child in node.body:
                if isinstance(child, ast.Assign) and len(child.targets) == 1:
                    t = child.targets[0]
                    if isinstance(t, ast.Name) and t.id in want_constants:
                        kept_class_body.append(child)
                if (
                    isinstance(
                        child, (ast.FunctionDef, ast.AsyncFunctionDef)
                    )
                    and child.name == "_parse_range_days"
                ):
                    child.decorator_list = []
                    kept_class_body.append(child)
            if kept_class_body:
                stub_class = ast.ClassDef(
                    name="_Stub",
                    bases=[],
                    keywords=[],
                    body=kept_class_body,
                    decorator_list=[],
                )
                kept.append(stub_class)
    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)
    ns = {"__name__": "test_ue7l_parse_lift"}
    exec(compile(lift, "<parse-range-lift>", "exec"), ns)
    return ns["_Stub"]


_STUB = _ast_lift_parse_range_days()


# ── (1) _parse_range_days pure-logic ─────────────────────────────────


class TestParseRangeDays(unittest.TestCase):

    def setUp(self):
        self.stub = _STUB()

    def test_default_90_on_none(self):
        self.assertEqual(self.stub._parse_range_days(None), 90)

    def test_default_90_on_empty(self):
        self.assertEqual(self.stub._parse_range_days(""), 90)
        self.assertEqual(self.stub._parse_range_days("   "), 90)

    def test_default_90_on_malformed(self):
        self.assertEqual(
            self.stub._parse_range_days("garbage"), 90,
        )
        # Missing unit.
        self.assertEqual(self.stub._parse_range_days("90"), 90)
        # Wrong unit.
        self.assertEqual(self.stub._parse_range_days("90m"), 90)

    def test_parses_days_suffix(self):
        self.assertEqual(self.stub._parse_range_days("7d"), 7)
        self.assertEqual(self.stub._parse_range_days("30d"), 30)
        self.assertEqual(self.stub._parse_range_days("90d"), 90)
        self.assertEqual(self.stub._parse_range_days("365d"), 365)

    def test_parses_years_suffix(self):
        self.assertEqual(self.stub._parse_range_days("1y"), 365)
        self.assertEqual(self.stub._parse_range_days("2y"), 730)

    def test_case_insensitive(self):
        self.assertEqual(self.stub._parse_range_days("90D"), 90)
        self.assertEqual(self.stub._parse_range_days("1Y"), 365)

    def test_clamp_upper_bound(self):
        """730 is the ceiling (2× the 365-day cold-start). 5y would
        try 1825 which must clamp to 730."""
        self.assertEqual(self.stub._parse_range_days("5y"), 730)
        self.assertEqual(self.stub._parse_range_days("9999d"), 730)

    def test_clamp_lower_bound(self):
        """0d would return 0 which is a no-op range; clamp to 1."""
        self.assertEqual(self.stub._parse_range_days("0d"), 1)


# ── (2) Service-method structure (source-grep) ───────────────────────


class TestFetchStockCandlesSource(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("services/InvestmentService.py")

    def test_method_defined(self):
        self.assertIn(
            "def fetchStockCandles(self, security_code, range_str=None):",
            self.src,
        )

    def test_queries_local_cache_not_live_kite(self):
        """ak-ue7l architectural invariant: serve from local cache,
        NEVER call KiteService.get_historical_data per request."""
        m = re.search(
            r"def fetchStockCandles.*?(?=\n    def |\n    #:|$)",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, "fetchStockCandles body not found")
        body = m.group(0)
        self.assertIn("StockHistoricalCandle", body)
        self.assertIn("instrument_token", body)
        # Must NOT call KiteService live — that's exactly the
        # anti-pattern ak-4o41 was created to avoid.
        self.assertNotIn(
            "get_historical_data", body,
            "fetchStockCandles must NOT call KiteService.get_"
            "historical_data per request — defeats ak-4o41 cache",
        )
        self.assertNotIn(
            "kite_service.", body,
            "fetchStockCandles must not call kite_service.* "
            "per request",
        )

    def test_resolves_security_code_via_stock_list(self):
        """The symbol→token resolution must go through the same
        JsonDownloadService.getStockList() map that
        BackfillHistoricalCandlesTask uses to POPULATE the cache.
        Any other resolver would pick a different token than the
        backfill task did, and the query would return empty."""
        m = re.search(
            r"def fetchStockCandles.*?def _parse_range_days",
            self.src, re.DOTALL,
        )
        body = m.group(0)
        self.assertIn(
            "JsonDownloadService.getStockList()", body,
        )
        self.assertIn('row.get("stockCode")', body)
        self.assertIn('row.get("instrument_token")', body)

    def test_response_shape_has_six_ohlcv_fields(self):
        """Per bead: {date, open, high, low, close, volume}."""
        m = re.search(
            r"def fetchStockCandles.*?def _parse_range_days",
            self.src, re.DOTALL,
        )
        body = m.group(0)
        for field in ("date", "open", "high", "low", "close", "volume"):
            self.assertIn(f'"{field}"', body)

    def test_sorted_ascending(self):
        """Candles sorted date ASC so the FE chart draws
        left-to-right."""
        m = re.search(
            r"def fetchStockCandles.*?def _parse_range_days",
            self.src, re.DOTALL,
        )
        body = m.group(0)
        self.assertIn(
            "StockHistoricalCandle.trade_date.asc()", body,
        )


# ── (3) Controller handler ───────────────────────────────────────────


class TestFetchStockCandlesHandler(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("controllers/investmentsEP.py")

    def test_handler_defined_with_security_code_path_param(self):
        self.assertTrue(
            re.search(
                r"@Logger\.standardLogger\n"
                r"\s+def fetchStockCandles\(self, security_code\):",
                self.src,
            ),
            "fetchStockCandles handler missing or wrong signature "
            "— security_code must be a path parameter",
        )

    def test_handler_reads_firebase_id_from_g(self):
        """ak-c8l IDOR sweep discipline on new paths."""
        m = re.search(
            r"def fetchStockCandles\(self, security_code\):(.*?)"
            r"except Exception",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        body = m.group(1)
        self.assertIn("g.get('firebase_id')", body)
        self.assertNotIn(
            'request.headers.get("X-Firebase-ID")', body,
        )

    def test_handler_passes_range_param_through(self):
        m = re.search(
            r"def fetchStockCandles\(self, security_code\):(.*?)"
            r"except Exception",
            self.src, re.DOTALL,
        )
        body = m.group(1)
        self.assertIn("request.args.get('range')", body)

    def test_response_shape_candles_plus_security_code(self):
        self.assertTrue(
            re.search(
                r'"candles":\s*candles,\s*\n\s*"security_code":\s*security_code',
                self.src,
            ),
            "Response must include both candles + security_code so "
            "FE can validate the path-param echo",
        )

    def test_500_fallback(self):
        self.assertIn('"Failed to fetch candles"', self.src)


# ── (4) Route wire-up with broadened mutation-method drift guard ─────


def test_candles_route_wired():
    src = _read_source("app.py")
    assert re.search(
        r"'/investments/stock/<security_code>/candles'\s*,\s*'GET'\s*,"
        r"\s*\n?\s*self\.investmentEP\.fetchStockCandles",
        src,
    ), "GET /investments/stock/<security_code>/candles not wired"


def test_candles_route_read_only_broadened_guard():
    """ak-1gme broadened drift-guard: block POST / PUT / DELETE /
    PATCH on the candles path. ak-h1mq's POST-only guard would
    have missed a PUT/DELETE/PATCH regression.
    """
    src = _read_source("app.py")
    # Any mutation method registered for this path = regression.
    assert not re.search(
        r"'/investments/stock/<security_code>/candles'\s*,\s*"
        r"'(POST|PUT|DELETE|PATCH)'",
        src,
    ), (
        "/investments/stock/<security_code>/candles must stay "
        "READ-ONLY (GET-only). POST/PUT/DELETE/PATCH route would "
        "indicate accidental mutation-path."
    )


# ── (5) ak-4o41 regression guard — cache-fill task still exists ──────


def test_backfill_task_still_exists():
    """ak-ue7l depends on stockHistoricalCandles being populated.
    BackfillHistoricalCandlesTask (ak-4o41) is the writer. Guard
    against someone removing it — this endpoint would serve an
    empty cache forever."""
    src = _read_source("services/tasks/BackfillHistoricalCandlesTask.py")
    assert "class BackfillHistoricalCandlesTask" in src, (
        "BackfillHistoricalCandlesTask missing — ak-ue7l depends "
        "on it populating stockHistoricalCandles"
    )


if __name__ == "__main__":
    unittest.main()
