"""ak-p8ix Bead C (2026-11-22): tests for the investment agent
analyst layer + WealthDigest grounding + conversation history
bounding.

Three independent surfaces, each with mutation-discipline coverage:

  1. ``services/portfolio_analytics.py`` — 4 pure(-ish) functions:
     portfolio_allocation / benchmark_compare / position_sizing /
     concentration_risk. Each exercised with a MagicMock
     InvestmentService stub + per-method validation / happy-path /
     edge-case tests.

  2. ``WealthDigestTask`` grounding — source-level drift guards
     that the task now (a) passes a non-empty allowed_tools list,
     (b) bumps max_turns to 6, (c) registers the 5 Bead-B web
     wrappers via an MCP server, (d) updated system prompt
     references the web tools + the public-identifier discipline.

  3. ``agentService._format_conversation`` — the new sliding
     window: keeps the most recent _HISTORY_MAX_MESSAGES, drops
     older, emits telemetry.

All tests avoid importing the full Flask / Firebase chain —
stub pattern mirrors prior tests in the arc.
"""
import ast
import os
import re
import sys
import types
import unittest
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_HERE = os.path.dirname(os.path.abspath(__file__))


def _read_source(rel_path):
    with open(os.path.join(_HERE, rel_path), "r") as f:
        return f.read()


# claude_agent_sdk + anyio stubs — same pattern as
# test_investment_agent_foundation. Withdraw after imports so later
# suites that detect SDK-absence still skip correctly.
_INSTALLED_STUBS = []
for _pkg in ("claude_agent_sdk", "anyio"):
    if _pkg not in sys.modules:
        _m = types.ModuleType(_pkg)
        _m.__path__ = []
        _m.__getattr__ = lambda name, pkg=_pkg: MagicMock(name=f"{pkg}.{name}")
        sys.modules[_pkg] = _m
        _INSTALLED_STUBS.append(_pkg)

from services.portfolio_analytics import (  # noqa: E402
    portfolio_allocation,
    benchmark_compare,
    position_sizing,
    concentration_risk,
)
from services.agent_tools import INVESTMENT_TOOLS  # noqa: E402

for _pkg in _INSTALLED_STUBS:
    sys.modules.pop(_pkg, None)


# ── Service stubs ────────────────────────────────────────────────────


def _service_stub(*, msn_summaries=None, epg_fetches=None,
                  fo_summary=None, user_securities=None,
                  portfolio_history=None):
    """Build a MagicMock-shaped service covering the subset
    ``portfolio_analytics`` reads.

    ak-p8ix REWORK CRITICAL: ``user_securities`` stub now returns
    the REAL contract — a **LIST** of row dicts (keyed by
    ``buyCode``, with ``buyQuant`` / ``buyPrice`` / ``info.lastPrice``
    + optional ``current_value`` for Kite-enriched Stocks), OR
    ``{"error": "..."}`` on empty. The pre-rework stub returned
    ``{"data": [...]}`` which encoded the fictional contract the
    bug reader asserted + hid the real bug (per ak-ucta precedent
    the reviewer called out: clone anti-pattern — stub encoded
    author's assumption, not the system's actual contract).

    ``user_securities`` dict shape:
      {
        "Stocks": [row, row, ...],          # or {"error": ...}
        "Mutual_Funds": [row, ...],
        "NPS": [row, ...],
      }
    """
    svc = types.SimpleNamespace()

    _ms = msn_summaries or {}
    _epg = epg_fetches or {}
    _us = user_securities or {}

    def fetchSummary(asset, user_id):
        return _ms.get(asset, {"currentValue": 0.0})

    def fetchActiveSecurities(epg_enum, user_id):
        return _epg.get(getattr(epg_enum, "name", str(epg_enum)), {"net": 0.0})

    def fetchFOSummary(user_id):
        return fo_summary or {}

    def fetchUserSecurities(service_type_value, user_id):
        """Real contract: list on success, {"error": ...} on empty."""
        return _us.get(service_type_value, {"error": "No active securities"})

    def getPortfolioHistory(user_id, days=None):
        return portfolio_history or []

    svc.fetchSummary = fetchSummary
    svc.fetchActiveSecurities = fetchActiveSecurities
    svc.fetchFOSummary = fetchFOSummary
    svc.fetchUserSecurities = fetchUserSecurities
    svc.getPortfolioHistory = getPortfolioHistory
    return svc


def _stock_row(buy_code, *, qty=None, last_price=None,
               current_value=None):
    """Build a Stocks-style enriched row matching the real
    InvestmentService.fetchUserSecurities shape.

    Mirror of the InvestmentService.fetchUserSecurities enrichment
    loop (L248-275): row has ``buyCode``, ``buyQuant``, ``buyPrice``,
    ``info.{symbol, lastPrice, change, pChange, previousClose}``,
    and (for Kite-enriched rows) a top-level ``current_value``.
    """
    row = {
        "buyCode": buy_code,
        "buyQuant": _dec(qty) if qty is not None else _dec("0"),
        "buyPrice": _dec("0"),
        "info": {
            "symbol": buy_code,
            "lastPrice": _dec(last_price) if last_price is not None else _dec("0"),
            "change": _dec("0"),
            "pChange": _dec("0"),
            "previousClose": _dec("0"),
        },
    }
    if current_value is not None:
        row["current_value"] = _dec(current_value)
    return row


def _mf_row(buy_code, *, qty=None, last_price=None):
    """MF row: no current_value (Kite-fold is Stocks-only) — value
    derives from ``buyQuant × info.lastPrice``."""
    return {
        "buyCode": buy_code,
        "buyQuant": _dec(qty) if qty is not None else _dec("0"),
        "buyPrice": _dec("0"),
        "info": {
            "lastPrice": _dec(last_price) if last_price is not None else _dec("0"),
            "scheme_id": buy_code,
            "fundHouse": "TEST",
            "schemeType": "TEST",
            "companyName": "TEST",
        },
    }


def _nps_row(buy_code, *, qty=None, nav=None):
    """NPS row matching the real enriched shape from
    InvestmentService.fetchUserSecurities L298-307.

    Critical difference vs MF: NPS keeps NAV under ``info.nav``
    (NOT ``info.lastPrice``). The enrichment loop copies the
    NPS rate's ``nav`` field verbatim without renaming. So the
    value derivation path is ``buyQuant × info.nav`` — the
    ``lastPrice``-only fallback silently returned 0 for every
    NPS row before ak-p8ix re-review surfaced the gap.

    No ``current_value`` key — Kite fold-up is Stocks-only.
    """
    return {
        "buyCode": buy_code,
        "buyQuant": _dec(qty) if qty is not None else _dec("0"),
        "buyPrice": _dec("0"),
        "info": {
            "nav": _dec(nav) if nav is not None else _dec("0"),
            "name": buy_code,
            "scheme_id": buy_code,
            "date": None,
            "yesterday": _dec("0"),
            "lastWeek": _dec("0"),
            "sixMonthsAgo": _dec("0"),
            "pfm_name": "TEST",
        },
    }


def _dec(v):
    """Decimal coercion for test fixtures. Mirrors
    portfolio_analytics._to_decimal; duplicated here to keep this
    test file independent of its module-internal helper."""
    from decimal import Decimal
    if v is None:
        return Decimal("0")
    return Decimal(str(v))


# ── 1. portfolio_allocation ──────────────────────────────────────────


class TestPortfolioAllocation(unittest.TestCase):

    def test_single_asset_class_100_percent(self):
        """Stocks-only portfolio → 100% Stocks, 0% everything else."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
        )
        result = portfolio_allocation(svc, "user1")
        self.assertEqual(result["tool"], "portfolio_allocation")
        self.assertEqual(result["total"], 100000.0)
        by_type = {b["type"]: b for b in result["by_asset_type"]}
        self.assertAlmostEqual(by_type["Stocks"]["pct"], 100.0, places=2)
        self.assertEqual(by_type["Mutual_Funds"]["pct"], 0.0)
        self.assertEqual(by_type["Gold"]["pct"], 0.0)

    def test_cross_asset_split_percentages_sum_to_100(self):
        svc = _service_stub(
            msn_summaries={
                "Stocks": {"currentValue": 300000.0},
                "Mutual_Funds": {"currentValue": 500000.0},
                "NPS": {"currentValue": 100000.0},
            },
            epg_fetches={
                "EPF": {"net": 100000.0},
            },
        )
        result = portfolio_allocation(svc, "user1")
        self.assertEqual(result["total"], 1000000.0)
        total_pct = sum(b["pct"] for b in result["by_asset_type"])
        self.assertAlmostEqual(total_pct, 100.0, places=2)
        by_type = {b["type"]: b["pct"] for b in result["by_asset_type"]}
        self.assertAlmostEqual(by_type["Mutual_Funds"], 50.0, places=2)
        self.assertAlmostEqual(by_type["Stocks"], 30.0, places=2)

    def test_sorted_by_value_descending(self):
        svc = _service_stub(
            msn_summaries={
                "Stocks": {"currentValue": 100.0},
                "Mutual_Funds": {"currentValue": 300.0},
                "NPS": {"currentValue": 200.0},
            },
        )
        result = portfolio_allocation(svc, "user1")
        values = [b["value"] for b in result["by_asset_type"]]
        self.assertEqual(values, sorted(values, reverse=True))

    def test_fo_pnl_excluded_from_allocation_but_surfaced_separately(self):
        """ak-p8ix REWORK MAJOR 1: FO P&L is NOT part of the allocation
        total or by_asset_type (it's a P&L figure, not a position
        value). Must appear as a separate top-level ``fo_pnl``
        field instead. Pre-rework test erroneously asserted FO as
        a slice."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 1000.0}},
            fo_summary={"totalPnl": 500.0},
        )
        result = portfolio_allocation(svc, "user1")
        # Separate top-level line — Decimal/money-rounded.
        self.assertEqual(result["fo_pnl"], 500.0)
        # NOT in by_asset_type.
        types_seen = [b["type"] for b in result["by_asset_type"]]
        self.assertNotIn("FO", types_seen)
        # NOT in the allocation total — total = Stocks(1000) only.
        self.assertEqual(result["total"], 1000.0)
        # Stocks slice is 100% of allocation (no FO distortion).
        by_type = {b["type"]: b for b in result["by_asset_type"]}
        self.assertAlmostEqual(by_type["Stocks"]["pct"], 100.0, places=2)

    def test_fo_pnl_absent_when_empty(self):
        """fetchFOSummary returning {} (no FO account) → ``fo_pnl``
        is null (vs. 0, which could be a legitimate flat P&L)."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 1000.0}},
            fo_summary={},
        )
        result = portfolio_allocation(svc, "user1")
        self.assertIsNone(result["fo_pnl"])
        types_seen = [b["type"] for b in result["by_asset_type"]]
        self.assertNotIn("FO", types_seen)

    def test_fo_negative_pnl_does_not_distort_allocation(self):
        """ak-p8ix REWORK MAJOR 1: a FO loss (negative pnl) would
        pre-rework have been added to total — making the allocation
        percentages misleading for every asset slice. Now excluded,
        so allocation is unaffected."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 10000.0}},
            fo_summary={"totalPnl": -3000.0},   # realized loss
        )
        result = portfolio_allocation(svc, "user1")
        self.assertEqual(result["fo_pnl"], -3000.0)
        self.assertEqual(result["total"], 10000.0)  # Stocks only, no FO drag
        by_type = {b["type"]: b for b in result["by_asset_type"]}
        self.assertAlmostEqual(by_type["Stocks"]["pct"], 100.0, places=2)

    def test_total_zero_does_not_divide_by_zero(self):
        """New user with no investments → all pct=0, no crash."""
        svc = _service_stub()
        result = portfolio_allocation(svc, "user1")
        self.assertEqual(result["total"], 0.0)
        for b in result["by_asset_type"]:
            self.assertEqual(b["pct"], 0.0)

    def test_fetch_failure_falls_back_to_zero(self):
        """One asset's fetch raising → that asset's value defaults
        to 0; total computes from the rest. Matches WealthDigest
        'unavailable asset types fall out' convention."""
        svc = _service_stub(
            msn_summaries={
                "Stocks": {"currentValue": 1000.0},
                "Mutual_Funds": {"currentValue": 2000.0},
            },
        )
        svc.fetchSummary = MagicMock(side_effect=lambda a, u: (
            {"currentValue": 1000.0} if a == "Stocks"
            else (_ for _ in ()).throw(RuntimeError("DB down"))
        ))
        result = portfolio_allocation(svc, "user1")
        self.assertEqual(result["total"], 1000.0)

    def test_as_of_date_reserved_not_implemented(self):
        svc = _service_stub()
        with self.assertRaises(NotImplementedError):
            portfolio_allocation(svc, "user1", as_of_date="2026-01-01")


# ── 2. benchmark_compare ─────────────────────────────────────────────


class TestBenchmarkCompare(unittest.TestCase):

    def test_computes_equity_basket_return_over_window(self):
        """ak-p8ix REWORK MAJOR 2: field name is
        ``equity_basket_return_pct`` (NOT ``portfolio_return_pct``)
        because the return projects today's EQUITY quantities onto
        past dates — MF/NPS/EPG aren't included."""
        from datetime import date, timedelta
        today = date.today()
        from_date = (today - timedelta(days=5)).isoformat()
        to_date = today.isoformat()
        svc = _service_stub(portfolio_history=[
            {"date": from_date, "total_value": 100000.0},
            {"date": to_date, "total_value": 110000.0},
        ])
        result = benchmark_compare(svc, "user1", from_date, to_date)
        self.assertEqual(result["tool"], "benchmark_compare")
        self.assertEqual(result["from_value"], 100000.0)
        self.assertEqual(result["to_value"], 110000.0)
        # NEW name (equity_basket_return_pct) present…
        self.assertAlmostEqual(
            result["equity_basket_return_pct"], 10.0, places=2,
        )
        # …old name (portfolio_return_pct) GONE. A future refactor
        # that accidentally restores the old name trips here
        # before misleading any user.
        self.assertNotIn("portfolio_return_pct", result)

    def test_scope_and_caveat_fields_present(self):
        """ak-p8ix REWORK MAJOR 2: the return dict must carry both
        a ``scope`` tag + a ``caveat`` narrative so the agent can't
        accidentally present the number as whole-portfolio."""
        from datetime import date, timedelta
        today = date.today()
        from_date = (today - timedelta(days=5)).isoformat()
        svc = _service_stub(portfolio_history=[
            {"date": from_date, "total_value": 100.0},
            {"date": today.isoformat(), "total_value": 110.0},
        ])
        result = benchmark_compare(
            svc, "user1", from_date, today.isoformat(),
        )
        self.assertEqual(result["scope"], "stocks_only_current_basket")
        self.assertIn("caveat", result)
        caveat = result["caveat"]
        # Must name MF/NPS/EPG explicitly so the agent knows the
        # exclusions when passing on the warning.
        self.assertIn("MF", caveat)
        self.assertIn("NPS", caveat)
        # And must explicitly say the return is NOT whole-portfolio.
        self.assertTrue(
            "whole-portfolio" in caveat or "not included" in caveat,
            f"caveat must disclose non-equity exclusion; got: {caveat!r}",
        )

    def test_benchmark_defaults_to_nifty_50(self):
        from datetime import date, timedelta
        today = date.today()
        from_date = (today - timedelta(days=5)).isoformat()
        svc = _service_stub(portfolio_history=[
            {"date": from_date, "total_value": 100.0},
            {"date": today.isoformat(), "total_value": 110.0},
        ])
        result = benchmark_compare(
            svc, "user1", from_date, today.isoformat(),
        )
        self.assertEqual(result["benchmark"]["name"], "Nifty 50")
        self.assertIn("Nifty 50", result["benchmark"]["canonical_query"])

    def test_benchmark_shape_has_plan_fields(self):
        from datetime import date, timedelta
        today = date.today()
        from_date = (today - timedelta(days=5)).isoformat()
        svc = _service_stub(portfolio_history=[
            {"date": from_date, "total_value": 100.0},
            {"date": today.isoformat(), "total_value": 110.0},
        ])
        result = benchmark_compare(
            svc, "user1", from_date, today.isoformat(),
            benchmark="Sensex",
        )
        plan = result["benchmark"]
        self.assertEqual(plan["name"], "Sensex")
        for key in ("canonical_query", "sources", "extraction",
                    "instructions"):
            self.assertIn(key, plan)

    def test_invalid_date_returns_structured_error(self):
        svc = _service_stub()
        result = benchmark_compare(
            svc, "user1", "not-a-date", "2026-01-01",
        )
        self.assertEqual(result["error_class"], "invalid_date")

    def test_insufficient_data_returns_structured_error(self):
        from datetime import date, timedelta
        today = date.today()
        from_date = (today - timedelta(days=5)).isoformat()
        svc = _service_stub(portfolio_history=[
            {"date": from_date, "total_value": 100.0},
        ])
        result = benchmark_compare(
            svc, "user1", from_date, today.isoformat(),
        )
        self.assertEqual(result["error_class"], "insufficient_data")


# ── 3. position_sizing ───────────────────────────────────────────────
#
# ak-p8ix REWORK CRITICAL: these tests now exercise the REAL
# fetchUserSecurities contract — a LIST of enriched row dicts
# keyed by ``buyCode``, with ``current_value`` (Kite-enriched
# Stocks) or ``buyQuant × info.lastPrice`` (fallback). Built via
# ``_stock_row()`` / ``_mf_row()`` fixtures. Pre-rework tests
# built ``{"data": [...]}`` with a fictional ``currentValue``
# camelCase key — a clone of the author's assumption, not the
# real contract, which is why 50/50 passed against broken code.


class TestPositionSizing(unittest.TestCase):

    def test_buy_direction_when_under_target(self):
        """Kite-enriched Stocks row with current_value=5000 vs
        target=10% of 100000 total → buy ₹5000 more."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            user_securities={
                "Stocks": [
                    _stock_row("RELIANCE", qty=5, last_price=1000,
                               current_value=5000),
                ],
            },
        )
        result = position_sizing(
            svc, "user1", symbol="RELIANCE",
            target_allocation_pct=10.0,
        )
        self.assertEqual(result["tool"], "position_sizing")
        self.assertEqual(result["direction"], "buy")
        self.assertAlmostEqual(result["delta_rupees"], 5000.0, places=2)

    def test_trim_direction_when_over_target(self):
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            user_securities={
                "Stocks": [
                    _stock_row("RELIANCE", qty=20, last_price=1000,
                               current_value=20000),
                ],
            },
        )
        result = position_sizing(
            svc, "user1", symbol="RELIANCE",
            target_allocation_pct=10.0,
        )
        self.assertEqual(result["direction"], "trim")
        self.assertAlmostEqual(result["delta_rupees"], -10000.0, places=2)

    def test_hold_direction_within_one_rupee(self):
        """Within ₹1 of target → 'hold' rather than a trivial buy/trim."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            user_securities={
                "Stocks": [
                    _stock_row("RELIANCE", qty=10, last_price=1000.05,
                               current_value=10000.5),
                ],
            },
        )
        result = position_sizing(
            svc, "user1", symbol="RELIANCE",
            target_allocation_pct=10.0,
        )
        self.assertEqual(result["direction"], "hold")

    def test_uses_buyQuant_times_lastPrice_fallback_without_current_value(self):
        """ak-p8ix REWORK CRITICAL: non-Kite rows (MF / pre-sync
        Stocks) have no ``current_value`` key. Value must derive
        from ``buyQuant × info.lastPrice`` fallback."""
        svc = _service_stub(
            msn_summaries={"Mutual_Funds": {"currentValue": 100000.0}},
            user_securities={
                "Mutual_Funds": [
                    _mf_row("120503", qty=100, last_price=50),  # = 5000
                ],
            },
        )
        result = position_sizing(
            svc, "user1", symbol="120503",
            target_allocation_pct=10.0,
        )
        self.assertNotIn("error_class", result)
        # current = 100 × 50 = 5000; target = 10% × 100000 = 10000.
        self.assertAlmostEqual(result["current_value"], 5000.0, places=2)
        self.assertAlmostEqual(result["delta_rupees"], 5000.0, places=2)

    def test_symbol_not_found_returns_structured_error(self):
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
        )
        result = position_sizing(
            svc, "user1", symbol="MADEUP", target_allocation_pct=10.0,
        )
        self.assertEqual(result["error_class"], "symbol_not_found")

    def test_symbol_not_found_when_fetchUserSecurities_returns_error_dict(self):
        """ak-p8ix REWORK CRITICAL regression guard:
        fetchUserSecurities' real empty-case returns
        ``{"error": "No active securities"}`` (NOT an empty list,
        NOT ``{"data": []}``). Helper must skip the error-dict
        case cleanly — a search in that case returns not-found
        without crashing."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            # No user_securities entry → stub returns the real
            # {"error": ...} shape per _service_stub.
        )
        result = position_sizing(
            svc, "user1", symbol="RELIANCE", target_allocation_pct=10.0,
        )
        self.assertEqual(result["error_class"], "symbol_not_found")

    def test_invalid_target_out_of_range_returns_error(self):
        svc = _service_stub()
        result = position_sizing(
            svc, "user1", symbol="X", target_allocation_pct=150,
        )
        self.assertEqual(result["error_class"], "invalid_target")

    def test_invalid_target_non_numeric_returns_error(self):
        svc = _service_stub()
        result = position_sizing(
            svc, "user1", symbol="X", target_allocation_pct="ten",
        )
        self.assertEqual(result["error_class"], "invalid_target")

    def test_empty_symbol_returns_error(self):
        svc = _service_stub()
        result = position_sizing(
            svc, "user1", symbol="   ", target_allocation_pct=5,
        )
        self.assertEqual(result["error_class"], "invalid_symbol")

    def test_symbol_match_is_case_insensitive(self):
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            user_securities={
                "Stocks": [
                    _stock_row("RELIANCE", qty=5, last_price=1000,
                               current_value=5000),
                ],
            },
        )
        result = position_sizing(
            svc, "user1", symbol="reliance", target_allocation_pct=10.0,
        )
        self.assertEqual(result.get("symbol"), "reliance")
        self.assertNotIn("error_class", result)

    def test_denominator_excludes_fo_pnl(self):
        """ak-p8ix REWORK MAJOR 1 propagation: position_sizing's
        denominator (total_portfolio) must NOT include FO P&L.
        Pre-rework, FO=+10000 would inflate the denominator and
        under-size the target."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            fo_summary={"totalPnl": 10000.0},
            user_securities={
                "Stocks": [
                    _stock_row("RELIANCE", qty=5, last_price=1000,
                               current_value=5000),
                ],
            },
        )
        result = position_sizing(
            svc, "user1", symbol="RELIANCE",
            target_allocation_pct=10.0,
        )
        # total_portfolio = 100000 (NOT 110000)
        self.assertEqual(result["total_portfolio"], 100000.0)
        # target = 10% × 100000 = 10000; current = 5000 → buy 5000.
        self.assertAlmostEqual(result["delta_rupees"], 5000.0, places=2)

    def test_nps_value_derives_from_info_nav_not_lastPrice(self):
        """ak-p8ix re-review MAJOR: NPS rows carry per-unit NAV at
        ``info.nav`` (NOT ``info.lastPrice``) per
        InvestmentService.py:298-307. Pre-fix, _row_value would
        multiply ``buyQuant × None → 0`` for every NPS row and
        position_sizing would recommend buying the FULL target
        amount even when the user already held the position.

        Here: user holds 100 units of an NPS scheme at NAV ₹50 =
        ₹5000. Portfolio total is ₹100k, target is 10% (₹10k).
        Correct behaviour: current_value=5000, buy ₹5000 more.
        Broken behaviour: current_value=0 (NAV misread), buy the
        FULL ₹10k target. The delta_rupees assertion locks this
        correctness — a bug that makes current_value=0 would also
        make delta_rupees=10000.
        """
        svc = _service_stub(
            msn_summaries={"NPS": {"currentValue": 100000.0}},
            user_securities={
                "NPS": [
                    _nps_row("SM001001", qty=100, nav=50),  # = 5000
                ],
            },
        )
        result = position_sizing(
            svc, "user1", symbol="SM001001",
            target_allocation_pct=10.0,
        )
        self.assertNotIn("error_class", result)
        # Correctness guard: current_value must be 5000, NOT 0.
        self.assertAlmostEqual(result["current_value"], 5000.0, places=2)
        # Downstream correctness: if current were misread as 0, the
        # agent would quote the user buying ₹10k; actual ask is ₹5k.
        self.assertAlmostEqual(result["delta_rupees"], 5000.0, places=2)
        self.assertEqual(result["direction"], "buy")


# ── 4. concentration_risk ────────────────────────────────────────────


class TestConcentrationRisk(unittest.TestCase):

    def test_flags_position_over_threshold(self):
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            user_securities={
                "Stocks": [
                    _stock_row("RELIANCE", qty=15, last_price=1000,
                               current_value=15000),
                    _stock_row("TCS", qty=5, last_price=1000,
                               current_value=5000),
                ],
            },
        )
        result = concentration_risk(svc, "user1", threshold_pct=10)
        flagged_syms = [f["security"] for f in result["flagged"]]
        self.assertIn("RELIANCE", flagged_syms)
        self.assertNotIn("TCS", flagged_syms)

    def test_flags_mf_without_current_value_via_buyQuant_fallback(self):
        """ak-p8ix REWORK CRITICAL: an MF row (no current_value key)
        with buyQuant × info.lastPrice over threshold MUST flag."""
        svc = _service_stub(
            msn_summaries={"Mutual_Funds": {"currentValue": 100000.0}},
            user_securities={
                "Mutual_Funds": [
                    _mf_row("120503", qty=300, last_price=50),  # = 15000
                    _mf_row("999999", qty=10, last_price=100),  # = 1000
                ],
            },
        )
        result = concentration_risk(svc, "user1", threshold_pct=10)
        flagged_syms = [f["security"] for f in result["flagged"]]
        # 15000 of 100000 = 15% > 10%, flagged.
        self.assertIn("120503", flagged_syms)
        # 1000 of 100000 = 1% < 10%, NOT flagged.
        self.assertNotIn("999999", flagged_syms)

    def test_no_positions_over_threshold_returns_empty_flagged(self):
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            user_securities={
                "Stocks": [
                    _stock_row("RELIANCE", qty=5, last_price=1000,
                               current_value=5000),
                    _stock_row("TCS", qty=5, last_price=1000,
                               current_value=5000),
                ],
            },
        )
        result = concentration_risk(svc, "user1", threshold_pct=10)
        self.assertEqual(result["flagged"], [])

    def test_default_threshold_is_10_pct(self):
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 1000.0}},
        )
        result = concentration_risk(svc, "user1")
        self.assertEqual(result["threshold_pct"], 10.0)

    def test_out_of_range_threshold_clamps_to_default(self):
        """threshold_pct=150 is nonsense → silently defaults to 10%."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 1000.0}},
        )
        result = concentration_risk(svc, "user1", threshold_pct=150)
        self.assertEqual(result["threshold_pct"], 10.0)

    def test_sorted_by_pct_descending(self):
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            user_securities={
                "Stocks": [
                    _stock_row("A", qty=15, last_price=1000,
                               current_value=15000),
                    _stock_row("B", qty=25, last_price=1000,
                               current_value=25000),
                    _stock_row("C", qty=11, last_price=1000,
                               current_value=11000),
                ],
            },
        )
        result = concentration_risk(svc, "user1", threshold_pct=10)
        pcts = [f["pct"] for f in result["flagged"]]
        self.assertEqual(pcts, sorted(pcts, reverse=True))

    def test_total_zero_returns_empty_flagged(self):
        """No investments → no flags (not a crash)."""
        svc = _service_stub()
        result = concentration_risk(svc, "user1")
        self.assertEqual(result["total"], 0.0)
        self.assertEqual(result["flagged"], [])

    def test_epg_aggregate_flagged_when_over_threshold(self):
        """A single EPG asset type (e.g., 50% Gold) should surface
        as a concentration signal too, not just per-position stocks."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 50000.0}},
            epg_fetches={"Gold": {"net": 50000.0}},
        )
        result = concentration_risk(svc, "user1", threshold_pct=10)
        agg = [f for f in result["flagged"] if f["asset_type"] == "Gold"]
        self.assertEqual(len(agg), 1)
        self.assertAlmostEqual(agg[0]["pct"], 50.0, places=1)

    def test_fetchUserSecurities_error_dict_handled_cleanly(self):
        """ak-p8ix REWORK CRITICAL regression guard:
        fetchUserSecurities returning ``{"error": "No active securities"}``
        must NOT crash concentration_risk + must NOT flag anything
        from that empty asset. Mirrors the position_sizing error-dict
        test."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 1000.0}},
            # No user_securities entry → stub returns error-dict per
            # the real contract.
        )
        result = concentration_risk(svc, "user1", threshold_pct=10)
        # Empty-holdings path: no per-stock flags (nothing iterated),
        # no crash.
        self.assertEqual(
            [f for f in result["flagged"] if f["asset_type"] == "Stocks"],
            [],
        )

    def test_denominator_excludes_fo_pnl(self):
        """ak-p8ix REWORK MAJOR 1 propagation: concentration_risk's
        denominator (total) must NOT include FO P&L. Pre-rework, a
        FO loss would deflate the denominator + falsely flag
        otherwise-diversified positions."""
        svc = _service_stub(
            msn_summaries={"Stocks": {"currentValue": 100000.0}},
            fo_summary={"totalPnl": -50000.0},  # -50% of stocks
            user_securities={
                "Stocks": [
                    _stock_row("RELIANCE", qty=9, last_price=1000,
                               current_value=9000),
                ],
            },
        )
        result = concentration_risk(svc, "user1", threshold_pct=10)
        # 9000/100000 = 9% < 10%, must NOT flag.
        # Pre-rework: 9000/(100000-50000) = 18% WOULD have flagged.
        flagged_syms = [f["security"] for f in result["flagged"]]
        self.assertNotIn("RELIANCE", flagged_syms)

    def test_flags_nps_over_threshold_via_info_nav_fallback(self):
        """ak-p8ix re-review MAJOR: NPS rows store per-unit NAV at
        ``info.nav`` (NOT ``info.lastPrice``).  _row_value must
        pick it up via the nav-fallback, otherwise a user with 50%+
        of their portfolio in a single NPS scheme would NEVER see a
        concentration flag. Pre-fix, this test fails: NPS row
        valued at 0 → no flag.
        """
        svc = _service_stub(
            msn_summaries={"NPS": {"currentValue": 100000.0}},
            user_securities={
                "NPS": [
                    _nps_row("SM001001", qty=1500, nav=50),  # = 75000
                    _nps_row("SM002001", qty=100, nav=50),   # = 5000
                ],
            },
        )
        result = concentration_risk(svc, "user1", threshold_pct=10)
        flagged_syms = [f["security"] for f in result["flagged"]]
        # 75000 of 100000 = 75% → MUST flag.
        self.assertIn("SM001001", flagged_syms)
        # 5000 of 100000 = 5% < 10% → MUST NOT flag.
        self.assertNotIn("SM002001", flagged_syms)
        # Correctness guard: the flagged entry's pct must reflect
        # the real value (not 0 from the misread-NAV path).
        flagged_sm001001 = next(
            f for f in result["flagged"] if f["security"] == "SM001001"
        )
        self.assertAlmostEqual(flagged_sm001001["pct"], 75.0, places=1)

    def test_nps_holding_does_not_report_zero_value(self):
        """ak-p8ix re-review MAJOR regression guard: the flagged-item
        ``value`` field for an NPS position must reflect
        ``buyQuant × info.nav`` — NEVER 0. A silent-zero here is
        exactly the class of bug the fix targets: the row is
        technically iterated but valued at 0, so downstream code
        (agent-quoted figures, concentration ranking) sees the
        position as non-existent.
        """
        svc = _service_stub(
            msn_summaries={"NPS": {"currentValue": 20000.0}},
            user_securities={
                "NPS": [
                    _nps_row("SM001001", qty=200, nav=100),  # = 20000
                ],
            },
        )
        # threshold low enough to force flag so we can inspect value
        result = concentration_risk(svc, "user1", threshold_pct=10)
        nps_flags = [
            f for f in result["flagged"] if f["security"] == "SM001001"
        ]
        self.assertEqual(len(nps_flags), 1, (
            "NPS position must be flagged at 100% concentration — "
            "a zero-valued row would be silently missing"
        ))
        # Correctness guard: value must be 20000 (not 0).
        self.assertAlmostEqual(nps_flags[0]["value"], 20000.0, places=2)
        self.assertAlmostEqual(nps_flags[0]["pct"], 100.0, places=1)


# ── 5. Agent-tools registration + executor dispatch ──────────────────


_ANALYST_TOOL_NAMES = (
    "portfolio_allocation",
    "benchmark_compare",
    "position_sizing",
    "concentration_risk",
)


@pytest.mark.parametrize("tool_name", _ANALYST_TOOL_NAMES)
def test_analyst_tool_registered_in_INVESTMENT_TOOLS(tool_name):
    by_name = {t["name"]: t for t in INVESTMENT_TOOLS}
    assert tool_name in by_name, (
        f"{tool_name} missing from INVESTMENT_TOOLS"
    )
    tool = by_name[tool_name]
    assert tool["description"].strip()
    assert tool["input_schema"]["type"] == "object"


@pytest.mark.parametrize("tool_name", _ANALYST_TOOL_NAMES)
def test_analyst_tool_dispatched_in_executor(tool_name):
    src = _read_source("services/agent_tool_executor.py")
    pattern = re.compile(
        rf'elif\s+tool_name\s*==\s*["\']{re.escape(tool_name)}["\']\s*:'
    )
    assert pattern.search(src), (
        f"{tool_name} has no dispatch branch in _execute_investment_tool"
    )


def test_analyst_tools_classified_as_read_for_destructiveness():
    """ak-npdf classifier integration: analyst tools READ data, never
    mutate — must classify 'read' (implicit default)."""
    from services.agent_tools import get_tool_destructiveness
    for name in _ANALYST_TOOL_NAMES:
        assert get_tool_destructiveness(name) == "read", (
            f"{name} must classify as 'read' (analyst tools read-only)"
        )


# ── 6. WealthDigest grounding — source drift guards ─────────────────


class TestWealthDigestGrounding(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("services/tasks/WealthDigestTask.py")

    def test_digest_allowed_tools_contains_websearch_and_wrappers(self):
        """Digest's allowlist must name both SDK built-ins + all 5
        Bead-B wrappers (per bead scope)."""
        # Scope grep to the _DIGEST_ALLOWED_TOOLS block.
        m = re.search(
            r"_DIGEST_ALLOWED_TOOLS\s*=\s*\[(.*?)\]",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, "_DIGEST_ALLOWED_TOOLS block missing")
        block = m.group(1)
        self.assertIn('"WebSearch"', block)
        self.assertIn('"WebFetch"', block)
        for wrapper in (
            "fetch_stock_news", "fetch_earnings_calendar",
            "fetch_company_announcements", "fetch_macro_context",
            "fetch_fund_factsheet",
        ):
            self.assertIn(wrapper, block, (
                f"_DIGEST_ALLOWED_TOOLS missing wrapper: {wrapper}"
            ))

    def test_digest_max_turns_bumped_to_6(self):
        """Pre-ak-p8ix was max_turns=3; bumped to 6 for the web-
        lookup loop + synthesis + schema-error retry headroom."""
        self.assertIn("max_turns=6", self.src)
        # Negative: the pre-ak-p8ix literal must be gone from the
        # options block (may still appear in comments describing
        # the bump).
        m = re.search(
            r"options\s*=\s*ClaudeAgentOptions\((.*?)\)",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        options_block = m.group(1)
        self.assertNotIn("max_turns=3", options_block)

    def test_digest_registers_mcp_server(self):
        """The 5 wrappers can only reach the model via an MCP server;
        the digest must register one per-run via
        _build_digest_mcp_server."""
        self.assertIn("def _build_digest_mcp_server", self.src)
        self.assertIn("mcp_servers=", self.src)

    def test_digest_prompt_mentions_web_tools(self):
        """System prompt must document the available web tools so
        the agent knows to call them."""
        self.assertIn("fetch_stock_news", self.src)
        self.assertIn("fetch_macro_context", self.src)
        self.assertIn("WebSearch", self.src)

    def test_digest_prompt_drops_do_not_invent_news_clause(self):
        """Pre-ak-p8ix prompt said 'DO NOT invent news'; now it
        allows web-sourced news. The pre-fix sentence must be gone
        OR reframed — the current language says 'sourced from live
        web lookups' + 'DO NOT silently pretend web failed'."""
        # Positive: the new language is present.
        self.assertIn("sourced from live web lookups", self.src)
        # Negative: the pre-fix absolute 'DO NOT invent' absolute
        # should not be the dominant phrasing on the news field —
        # a watch_items description may still reference the word
        # 'invent' in prose, so we pin on a distinguishing fragment.
        self.assertNotIn(
            "DO NOT invent news** to fill the field", self.src,
            "pre-ak-p8ix 'DO NOT invent news to fill the field' "
            "clause must be removed — digest now has web tools",
        )

    def test_digest_prompt_has_pii_discipline(self):
        """Public-identifiers-only rule (same shape as ak-pb4n
        chat agent) must appear in the digest prompt too — same
        attack class, same mitigation.

        ak-p8ix REWORK MINOR 2: reworded so scheme codes + tickers
        are explicitly PUBLIC identifiers (AMFI/BSE publish them
        openly) + the ``fetch_fund_factsheet(scheme_code)`` /
        ``fetch_stock_news(ticker)`` cadence doesn't contradict
        the rule. Pre-rework the prompt listed 'scheme codes' as
        PII-forbidden, which would make the agent balk at its own
        wrappers' canonical queries."""
        # Positive: the new personal-identifier rule.
        self.assertIn("NEVER send the user's PERSONAL identifiers",
                      self.src)
        # Positive: public-market-identifier affordance so the
        # agent knows tickers / scheme codes are OK to send.
        self.assertIn("Public market identifiers are allowed",
                      self.src)
        # Negative: pre-rework contradictory listing of
        # "scheme codes" as PII-forbidden must be gone — would
        # prevent the agent from using its own wrappers.
        self.assertNotIn(
            "scheme codes, account numbers", self.src,
            "pre-ak-p8ix-REWORK PII clause listing scheme codes "
            "as forbidden still present — would prevent "
            "fetch_fund_factsheet(scheme_code) from firing",
        )


# ── 7. _format_conversation sliding window ──────────────────────────


class TestFormatConversationWindow(unittest.TestCase):
    """Ast-lifted test — importing agentService pulls Google auth."""

    def setUp(self):
        src = _read_source("services/agentService.py")
        tree = ast.parse(src)
        self.ns = {}
        # Pull _HISTORY_MAX_MESSAGES + MAX_TURNS assignments + the
        # _format_conversation method.
        for node in tree.body:
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(t, ast.Name)
                    and t.id in ("_HISTORY_MAX_MESSAGES", "MAX_TURNS")
                    for t in node.targets
                )
            ):
                exec(
                    compile(ast.Module([node], []), "<extract>", "exec"),
                    self.ns,
                )
            if isinstance(node, ast.ClassDef) and node.name == "AgentService":
                for item in node.body:
                    if (
                        isinstance(item, ast.FunctionDef)
                        and item.name == "_format_conversation"
                    ):
                        item.decorator_list = []
                        exec(
                            compile(ast.Module([item], []), "<extract>",
                                    "exec"),
                            self.ns,
                        )
        self.fmt = self.ns["_format_conversation"]
        self.max_msgs = self.ns["_HISTORY_MAX_MESSAGES"]

    def test_no_history_returns_single_message_content(self):
        result = self.fmt([{"role": "user", "content": "hi"}])
        self.assertEqual(result, "hi")

    def test_empty_messages_returns_empty_string(self):
        self.assertEqual(self.fmt([]), "")

    def test_short_history_included_entirely(self):
        messages = [
            {"role": "user", "content": "q1"},
            {"role": "assistant", "content": "a1"},
            {"role": "user", "content": "q2"},
        ]
        result = self.fmt(messages)
        self.assertIn("User: q1", result)
        self.assertIn("Assistant: a1", result)
        self.assertIn("User's latest message: q2", result)

    def test_long_history_prunes_oldest_messages(self):
        """Simulate N+5 history messages + a latest; expect only
        the most recent N history messages to appear. Uses
        colon-delimited markers to avoid substring collisions
        (``m1`` is a substring of ``m10``)."""
        N = self.max_msgs
        messages = []
        # N + 5 history messages (alternating user/assistant).
        # Use markers that won't collide as substrings: "msg-XXXX".
        for i in range(N + 5):
            role = "user" if i % 2 == 0 else "assistant"
            messages.append({
                "role": role,
                "content": f"msg-{i:04d}",
            })
        # Latest message.
        messages.append({"role": "user", "content": "latest"})

        result = self.fmt(messages)
        # Oldest 5 pruned (msg-0000 through msg-0004).
        for i in range(5):
            self.assertNotIn(f"msg-{i:04d}", result)
        # Window-boundary (msg-0005) kept.
        self.assertIn(f"msg-{5:04d}", result)
        # Last history (msg-{N+4:04d}) kept.
        self.assertIn(f"msg-{N + 4:04d}", result)
        self.assertIn("User's latest message: latest", result)

    def test_latest_message_always_passes_through(self):
        """Even if N+500 history, the latest message must appear."""
        N = self.max_msgs
        messages = [
            {"role": "user", "content": f"m{i}"}
            for i in range(N + 500)
        ]
        messages.append({"role": "assistant", "content": "LATEST"})
        result = self.fmt(messages)
        self.assertIn("LATEST", result)

    def test_partial_assistant_messages_dropped_before_window(self):
        """partial_assistant messages are not counted toward the
        window and are never rendered."""
        messages = [
            {"role": "user", "content": "q"},
            {"role": "partial_assistant", "content": "PARTIAL_DO_NOT_SEE"},
            {"role": "assistant", "content": "a"},
            {"role": "user", "content": "latest"},
        ]
        result = self.fmt(messages)
        self.assertNotIn("PARTIAL_DO_NOT_SEE", result)

    def test_telemetry_logger_called_with_pruned_count(self):
        """When a logger is passed + history exceeds the window,
        the telemetry log line fires with the pruned count."""
        N = self.max_msgs
        messages = [
            {"role": "user", "content": f"m{i}"}
            for i in range(N + 7)
        ]
        messages.append({"role": "user", "content": "latest"})
        logger = MagicMock()
        self.fmt(messages, logger=logger)
        logger.info.assert_called_once()
        call = logger.info.call_args
        # Grep the format string + the pruned arg.
        fmt_str = call.args[0]
        self.assertIn("history_window", fmt_str)
        self.assertIn("pruned=%d", fmt_str)
        # args beyond the format string carry the metrics.
        args_tail = call.args[1:]
        self.assertIn(7, args_tail)  # pruned_count = total_history (N+7) - N = 7


if __name__ == "__main__":
    unittest.main(verbosity=2)
