"""ak-p8ix (Investment Agent Bead C, 2026-11-22): server-side
analytics for the investment agent.

Four pure(-ish) functions the agent can call via the MCP tool
layer. "Pure-ish" because they READ from InvestmentService
sub-services (StockService / MFService / NPSService / EPFService /
PPFService / GoldService / FOService) — but they do NO write and
do NO network. Each function returns a JSON-serializable dict
shaped for direct agent consumption.

Design rationale: keep the MATH server-side. The pre-ak-p8ix
investment agent was computing allocation / concentration /
position sizing in-prompt from raw holdings dumps, which (a)
drifted across agent runs on floating-point serialization and
(b) wasted context budget re-deriving arithmetic. The agent now
calls a named tool and quotes the result verbatim — same discipline
``fetch_portfolio_summary`` already applies to the per-asset
totals.

Scope + out-of-scope:
  * ``portfolio_allocation`` and ``concentration_risk`` are
    always-now valuations. ``as_of_date`` is reserved in the
    public signature (bead spec) but currently raises
    NotImplementedError when passed — the snapshot-table
    query path for arbitrary past dates lands in a follow-up.
  * ``benchmark_compare`` computes the PORTFOLIO return
    internally from stockHistoricalCandles (ak-4o41 Bundle 1
    output). The BENCHMARK return itself (Nifty 50 default)
    is emitted as a structured search PLAN — the agent calls
    WebSearch on the plan and combines. Matches the Bead B
    wrapper pattern — keeps this module network-free + the
    benchmark fetch surfaces in tool_events for auditability.
  * Kite holdings are not a separate allocation slice: per the
    ak-n9im audit, ``sync_kite_holdings`` UPSERTs Kite stocks
    INTO PurchasedSecurities under service_type=Stocks, so
    they're already counted in the Stocks slice.
  * FO-P&L is included additively (``fetchFOSummary`` returns a
    realized+unrealized number). Appears as a distinct line
    ``FO`` in the allocation breakdown so the user can see it
    isolated.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional


# Asset types we always report. Kite is NOT a separate slice
# (synced-into-Stocks per ak-n9im). FO is folded in on demand.
_MSN_TYPES = ("Stocks", "Mutual_Funds", "NPS")
_EPG_TYPES = ("EPF", "PF", "Gold")  # PF == PPF per EPGEnum

# concentration_risk default threshold. 10% is the conventional
# "concentrated position" bar in Indian retail portfolio advice.
_DEFAULT_CONCENTRATION_PCT = 10.0

# benchmark_compare default. Nifty 50 is the standard retail
# benchmark for equity-heavy portfolios in India.
_DEFAULT_BENCHMARK = "Nifty 50"


def _to_float(x):
    """Coerce Decimal / numeric-string / None to float for the
    JSON return shape. None → 0.0."""
    if x is None:
        return 0.0
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _asset_value(investment_service, asset_type, user_id):
    """Current rupee value for one asset type.

    Dispatches per the existing InvestmentService entry points:
      * MSN types (Stocks / Mutual_Funds / NPS):
        ``fetchSummary(asset, user_id)['currentValue']``.
      * EPG types (EPF / PF / Gold):
        ``fetchActiveSecurities(EPGEnum.<asset>, user_id)['net']``
        (deposits + accrued profit).

    Returns 0.0 on any lookup failure — matches the ak-ran
    WealthDigest convention of "unavailable asset types fall out
    of the allocation rather than aborting the whole computation".
    """
    from enums.MsnEnum import MSNENUM
    from enums.EPGEnum import EPGEnum
    try:
        if asset_type in _MSN_TYPES:
            summary = investment_service.fetchSummary(asset_type, user_id)
            if isinstance(summary, dict):
                return _to_float(summary.get("currentValue"))
            # The schema-loaded result may be a marshmallow object;
            # walk .get fallback only.
            return _to_float(getattr(summary, "currentValue", 0))
        if asset_type in _EPG_TYPES:
            epg_type = EPGEnum[asset_type]
            payload = investment_service.fetchActiveSecurities(
                epg_type, user_id,
            )
            if isinstance(payload, dict):
                # PPF / Gold / EPF all return `net` as current value
                # (deposits + accrued profit). See each *Service.fetchComplete.
                return _to_float(payload.get("net"))
            return 0.0
    except Exception:
        return 0.0
    return 0.0


def _fo_pnl(investment_service, user_id):
    """Optional FO add-on. Returns (pnl_float, present_bool).

    The F&O side is additive to the portfolio total; include it
    as a separate line so the user can see it isolated. Missing /
    empty FO service → (0.0, False) so the allocation dict shows
    FO as zero without claiming it's present.
    """
    try:
        summary = investment_service.fetchFOSummary(user_id)
        if not isinstance(summary, dict):
            return 0.0, False
        # fetchFOSummary returns {"totalPnl": X, ...} per the
        # FOService shape. Realized+unrealized combined.
        pnl = summary.get("totalPnl")
        if pnl is None:
            return 0.0, False
        return _to_float(pnl), True
    except Exception:
        return 0.0, False


def portfolio_allocation(investment_service, user_id,
                        as_of_date: Optional[str] = None) -> dict:
    """Current cross-asset allocation % by rupee value.

    Args:
        investment_service: ``services.InvestmentService.InvestmentService``
            instance.
        user_id: Firebase user id.
        as_of_date: RESERVED (per bead spec). Currently raises
            NotImplementedError — the snapshot-table query path
            for arbitrary past dates lands in a follow-up. Absent
            → "now" valuation.

    Returns:
        ``{tool, as_of, total, fo_pnl, by_asset_type: [{type, value, pct}]}``.

        ``by_asset_type`` is sorted by value descending so the
        largest slice renders first. Assets with zero value are
        still included (the agent + FE need to know "we have no
        PPF" explicitly, not inferred from omission).
    """
    if as_of_date is not None:
        raise NotImplementedError(
            "portfolio_allocation: as_of_date is reserved per bead "
            "ak-p8ix but not yet implemented. Requires the "
            "snapshot-table query path which ships in a follow-up. "
            "Omit as_of_date to get the current valuation."
        )

    buckets = []
    total = 0.0
    for asset in _MSN_TYPES + _EPG_TYPES:
        value = _asset_value(investment_service, asset, user_id)
        buckets.append({"type": asset, "value": value})
        total += value

    fo_pnl, fo_present = _fo_pnl(investment_service, user_id)
    if fo_present:
        buckets.append({"type": "FO", "value": fo_pnl})
        total += fo_pnl

    # Compute pct. Guard total==0 (new user, no investments yet)
    # so we don't ZeroDivisionError — report 0% for everything.
    for b in buckets:
        b["pct"] = round(
            (b["value"] / total * 100.0) if total else 0.0, 4,
        )

    # Sort by value descending (largest slice first).
    buckets.sort(key=lambda b: b["value"], reverse=True)

    from datetime import datetime
    return {
        "tool": "portfolio_allocation",
        "as_of": datetime.utcnow().isoformat() + "Z",
        "total": round(total, 2),
        "fo_pnl": round(fo_pnl, 2) if fo_present else None,
        "by_asset_type": buckets,
    }


def benchmark_compare(investment_service, user_id,
                      from_date: str, to_date: str,
                      benchmark: Optional[str] = None) -> dict:
    """Portfolio return over [from_date, to_date] + benchmark
    WebSearch plan.

    Portfolio side (deterministic, from local candles): uses
    ``getPortfolioHistory`` (ak-4o41 Bundle 1) to extract the
    total portfolio value on the first + last trading days
    within the window, then computes the simple return %.

    Benchmark side: returns a structured search plan the agent
    calls WebSearch on. Matches the Bead B wrapper pattern —
    keeps this module network-free + lands the benchmark
    canonical query in tool_events for auditability. Default
    benchmark: Nifty 50.

    Args:
        investment_service: ``InvestmentService`` instance.
        user_id: Firebase user id.
        from_date, to_date: ``"YYYY-MM-DD"`` strings. If parsing
            fails or the window has fewer than 2 trading days,
            return a structured error.
        benchmark: optional index name (default Nifty 50).

    Returns:
        On success:
          ``{tool, from_date, to_date, from_value, to_value,
             portfolio_return_pct, benchmark: {name,
             canonical_query, sources, extraction, instructions}}``
        On insufficient-data failure:
          ``{tool, error_class, message, from_date, to_date}``
    """
    from datetime import date as _date

    try:
        _date.fromisoformat(from_date)
        _date.fromisoformat(to_date)
    except (ValueError, TypeError) as exc:
        return {
            "tool": "benchmark_compare",
            "error_class": "invalid_date",
            "message": (
                f"from_date / to_date must be ISO-8601 'YYYY-MM-DD' "
                f"strings. Got from_date={from_date!r} "
                f"to_date={to_date!r} ({exc})"
            ),
        }

    # Compute how many days back from_date is; getPortfolioHistory
    # takes `days` and returns the trailing N-day window. Compute
    # days from today to from_date; filter by to_date on return.
    today = _date.today()
    try:
        fd = _date.fromisoformat(from_date)
        td = _date.fromisoformat(to_date)
        days = (today - fd).days + 1
        if days < 2:
            days = 2
    except Exception:
        days = 30

    try:
        history = investment_service.getPortfolioHistory(
            user_id, days=days,
        )
    except Exception as exc:
        return {
            "tool": "benchmark_compare",
            "error_class": "portfolio_history_failed",
            "message": str(exc),
            "from_date": from_date, "to_date": to_date,
        }
    if not isinstance(history, list):
        history = []

    # Filter to the requested window + sort ascending by date.
    in_window = []
    for row in history:
        row_date = row.get("date") if isinstance(row, dict) else None
        if not row_date:
            continue
        try:
            rd = _date.fromisoformat(str(row_date))
        except Exception:
            continue
        if fd <= rd <= td:
            in_window.append(
                (rd, _to_float(row.get("total_value")))
            )
    in_window.sort(key=lambda t: t[0])

    if len(in_window) < 2:
        return {
            "tool": "benchmark_compare",
            "error_class": "insufficient_data",
            "message": (
                f"Need >=2 trading days in [{from_date}, {to_date}] "
                f"to compute portfolio return; got "
                f"{len(in_window)}. Candles may be missing for "
                "part of the window — check BackfillHistoricalCandlesTask "
                "status."
            ),
            "from_date": from_date, "to_date": to_date,
        }

    from_value = in_window[0][1]
    to_value = in_window[-1][1]
    if from_value == 0:
        portfolio_return_pct = None
    else:
        portfolio_return_pct = round(
            (to_value - from_value) / from_value * 100.0, 4,
        )

    bench_name = (benchmark or _DEFAULT_BENCHMARK).strip() or _DEFAULT_BENCHMARK

    return {
        "tool": "benchmark_compare",
        "from_date": from_date,
        "to_date": to_date,
        "from_value": round(from_value, 2),
        "to_value": round(to_value, 2),
        "portfolio_return_pct": portfolio_return_pct,
        # Benchmark side as a WebSearch plan. Matches Bead B pattern.
        "benchmark": {
            "name": bench_name,
            "canonical_query": (
                f"{bench_name} index total return from "
                f"{from_date} to {to_date}"
            ),
            "sources": [
                "nseindia.in (official)",
                "livemint.com", "economictimes.indiatimes.com",
                "moneycontrol.com",
            ],
            "extraction": [
                "index value on start date",
                "index value on end date",
                "total return % (prefer total-return index over "
                "price-only where available)",
            ],
            "instructions": (
                "Call WebSearch with 'canonical_query', extract the "
                "start + end index values per 'extraction', and "
                "compute simple return. Compare to "
                "'portfolio_return_pct' above — outperformance = "
                "portfolio − benchmark. Cite the source for every "
                "number."
            ),
        },
    }


def position_sizing(investment_service, user_id, symbol: str,
                    target_allocation_pct: float) -> dict:
    """Compute rupee delta to reach target allocation % for a
    symbol.

    Formula: ``delta = (target_pct / 100) × total_portfolio − current_position``.
    Positive delta = buy more; negative = trim.

    Relies on ``portfolio_allocation`` to establish ``total`` and
    reads the current position value via ``findSecurity`` which
    returns the local Stocks/MF cache row INCLUDING live LTP from
    the latest rate fetch.

    Args:
        investment_service: ``InvestmentService`` instance.
        user_id: Firebase user id.
        symbol: stock symbol / scheme code (searched across
            Stocks + MF caches).
        target_allocation_pct: 0-100 float. Values outside that
            range return an invalid_target error.

    Returns:
        ``{tool, symbol, current_value, current_pct,
           target_pct, target_value, delta_rupees, direction,
           total_portfolio}``
        On symbol not found: ``{tool, error_class, message, symbol}``
    """
    if not isinstance(target_allocation_pct, (int, float)):
        return {
            "tool": "position_sizing",
            "error_class": "invalid_target",
            "message": (
                "target_allocation_pct must be a number in [0, 100]. "
                f"Got {target_allocation_pct!r}"
            ),
        }
    if not (0 <= target_allocation_pct <= 100):
        return {
            "tool": "position_sizing",
            "error_class": "invalid_target",
            "message": (
                f"target_allocation_pct must be in [0, 100]. "
                f"Got {target_allocation_pct}. For sizing a NEW "
                "position use the target weight you want to hold "
                "post-buy, not a multiplier."
            ),
        }
    if not symbol or not isinstance(symbol, str) or not symbol.strip():
        return {
            "tool": "position_sizing",
            "error_class": "invalid_symbol",
            "message": "symbol is required and must be a non-empty string",
        }
    symbol = symbol.strip()

    # Total portfolio from the allocation tool (DRY — one source
    # of truth for 'total').
    alloc = portfolio_allocation(investment_service, user_id)
    total = alloc["total"]

    # Current position value — look up the symbol via findSecurity
    # across both the Stocks and MF caches. Returns (value, ambiguous_flag).
    current_value = _find_symbol_value(investment_service, user_id, symbol)
    if current_value is None:
        return {
            "tool": "position_sizing",
            "error_class": "symbol_not_found",
            "message": (
                f"{symbol!r} not found in local Stocks / MF caches. "
                "Use search_securities first to resolve the exact "
                "name → code mapping."
            ),
            "symbol": symbol,
        }

    current_pct = round(
        (current_value / total * 100.0) if total else 0.0, 4,
    )
    target_value = (target_allocation_pct / 100.0) * total if total else 0.0
    delta = target_value - current_value

    if abs(delta) < 1.0:
        direction = "hold"       # within ₹1 of target — no-op
    elif delta > 0:
        direction = "buy"
    else:
        direction = "trim"

    return {
        "tool": "position_sizing",
        "symbol": symbol,
        "total_portfolio": total,
        "current_value": round(current_value, 2),
        "current_pct": current_pct,
        "target_pct": target_allocation_pct,
        "target_value": round(target_value, 2),
        "delta_rupees": round(delta, 2),
        "direction": direction,
    }


def _find_symbol_value(investment_service, user_id, symbol):
    """Lookup the current rupee value of a single symbol across
    the user's Stocks + MF holdings. Returns None if unmatched.

    Best-effort — walks ``fetchUserSecurities`` for each MSN type
    and matches on securityCode / schemeCode / schemeName /
    stockCode (case-insensitive, first non-null hit wins). Each
    row's ``currentValue`` field is the ``buyQuant × last_price``
    product the per-service calculateStockRates emits.
    """
    from enums.MsnEnum import MSNENUM
    sym_lower = symbol.lower()
    for mtype in ("Stocks", "Mutual_Funds", "NPS"):
        try:
            payload = investment_service.fetchUserSecurities(
                MSNENUM[mtype].value, user_id,
            )
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        rows = (
            payload.get("data")
            or payload.get("results")
            or []
        )
        for row in rows:
            if not isinstance(row, dict):
                continue
            # Try the common identifier columns in priority order.
            for key in ("securityCode", "stockCode", "schemeCode",
                        "schemeName", "name"):
                v = row.get(key)
                if isinstance(v, str) and v.strip().lower() == sym_lower:
                    return _to_float(row.get("currentValue"))
    return None


def concentration_risk(investment_service, user_id,
                       threshold_pct: float | None = None) -> dict:
    """Flag positions above ``threshold_pct`` of total portfolio
    value.

    Args:
        investment_service: ``InvestmentService`` instance.
        user_id: Firebase user id.
        threshold_pct: optional float; defaults to 10%. Values <0
            or >100 default silently to 10% (clamped) — concentration
            analysis above 100% is nonsense; below 0 also nonsense.

    Returns:
        ``{tool, threshold_pct, total, flagged: [{security, value,
           pct, asset_type}]}``. ``flagged`` is sorted by pct
        descending (most concentrated first).

        Empty ``flagged`` means every individual position is within
        the threshold — the agent should report "well-diversified
        at the N% bar" rather than imply there's nothing to see.
    """
    if (
        not isinstance(threshold_pct, (int, float))
        or threshold_pct < 0 or threshold_pct > 100
    ):
        threshold_pct = _DEFAULT_CONCENTRATION_PCT

    # Use portfolio_allocation to establish total (DRY).
    alloc = portfolio_allocation(investment_service, user_id)
    total = alloc["total"]
    if total == 0:
        return {
            "tool": "concentration_risk",
            "threshold_pct": threshold_pct,
            "total": 0.0,
            "flagged": [],
        }

    # Walk every stock / MF / NPS position (individual rows,
    # NOT asset-type buckets) and flag ones over threshold.
    # EPF / PPF / Gold are reported at the asset-type level
    # since they don't have per-position rows the way equity
    # does (EPF = one aggregate per employer; Gold = per-purchase
    # rows but semantically one asset class).
    from enums.MsnEnum import MSNENUM
    flagged = []
    for mtype in ("Stocks", "Mutual_Funds", "NPS"):
        try:
            payload = investment_service.fetchUserSecurities(
                MSNENUM[mtype].value, user_id,
            )
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        rows = (
            payload.get("data")
            or payload.get("results")
            or []
        )
        for row in rows:
            if not isinstance(row, dict):
                continue
            val = _to_float(row.get("currentValue"))
            pct = (val / total * 100.0) if total else 0.0
            if pct >= threshold_pct:
                name = (
                    row.get("securityCode")
                    or row.get("stockCode")
                    or row.get("schemeCode")
                    or row.get("schemeName")
                    or row.get("name")
                    or "<unnamed>"
                )
                flagged.append({
                    "security": name,
                    "asset_type": mtype,
                    "value": round(val, 2),
                    "pct": round(pct, 4),
                })

    # EPG asset-type-level check — for EPF / PPF / Gold a single
    # asset-type over threshold is also a concentration signal.
    for et in ("EPF", "PF", "Gold"):
        for bucket in alloc["by_asset_type"]:
            if bucket["type"] == et and bucket["pct"] >= threshold_pct:
                flagged.append({
                    "security": f"({et} aggregate)",
                    "asset_type": et,
                    "value": bucket["value"],
                    "pct": bucket["pct"],
                })

    flagged.sort(key=lambda f: f["pct"], reverse=True)

    return {
        "tool": "concentration_risk",
        "threshold_pct": threshold_pct,
        "total": round(total, 2),
        "flagged": flagged,
    }
