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

ak-p8ix REWORK (reviewer verdict on acf26c8, 2026-11-22):

  * CRITICAL — ``position_sizing`` + ``concentration_risk``
    were reading a fictional ``fetchUserSecurities`` contract
    (``{"data": [...]}`` / ``{"results": [...]}`` with a
    ``currentValue`` camelCase key). Reality: the method
    returns a LIST of enriched row dicts (or
    ``{"error": "..."}`` on empty), keyed by ``buyCode``,
    with ``current_value`` (snake_case, Kite-enriched) or
    ``buyQuant × info.lastPrice`` as the per-row value.
    Fix: iterate the LIST + derive value from ``current_value``
    when present else the ``buyQuant × info.lastPrice`` fallback.
  * MAJOR 1 — FO P&L is REMOVED from ``by_asset_type`` and
    from the allocation ``total``. It's a profit/loss figure,
    not a current position value, and folding it into the
    denominator distorts every % slice (including the
    downstream denominator used by ``position_sizing`` +
    ``concentration_risk``). Now surfaced as a separate
    top-level ``fo_pnl`` field that the agent/UI reports
    independently.
  * MAJOR 2 — ``benchmark_compare.portfolio_return_pct``
    renamed to ``equity_basket_return_pct``. The return
    computed from ``stockHistoricalCandles`` covers ONLY the
    equity slice (constant-composition projection of today's
    held stocks onto past dates); non-equity assets aren't
    included. Caller must disclose the scope when quoting.
    Return dict now also carries explicit ``scope`` + ``caveat``
    fields so the agent can't accidentally present the number
    as whole-portfolio.
  * MINOR — money arithmetic switched to ``Decimal`` via the
    ``_to_decimal`` helper. Matches rig constraint #2
    (accounting correctness) for money sums. Final return
    values round via Decimal ROUND_HALF_UP → float at the
    JSON boundary.

Scope + out-of-scope:
  * ``portfolio_allocation`` and ``concentration_risk`` are
    always-now valuations. ``as_of_date`` is reserved in the
    public signature (bead spec) but currently raises
    NotImplementedError when passed — the snapshot-table
    query path for arbitrary past dates lands in a follow-up.
  * ``benchmark_compare`` computes the EQUITY return
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
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
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


#: Two-places quantizer for money rounding at the JSON boundary.
#: Everything summed internally stays Decimal; we only drop to float
#: when the result lands in the return dict so JSON encoding stays
#: cheap. ROUND_HALF_UP matches the ak-ad1 fixture rounding we use
#: for IBJA gold + the rest of the money path.
_MONEY_QUANT = Decimal("0.01")


def _to_decimal(x):
    """Coerce int / float / numeric-string / Decimal / None → Decimal.
    None or any coerce failure → Decimal('0'). For money arithmetic
    (ak-p8ix REWORK MINOR per rig constraint #2 — accounting
    correctness). Uses str() first so float inputs don't retain
    binary-float imprecision."""
    if x is None:
        return Decimal("0")
    if isinstance(x, Decimal):
        return x
    try:
        return Decimal(str(x))
    except (TypeError, ValueError, ArithmeticError):
        return Decimal("0")


def _money(x) -> float:
    """Coerce to Decimal + quantize to 2 places + drop to float for
    the JSON return shape. Call ONCE per field at the return-dict
    boundary; keep Decimal through the sums above it."""
    d = _to_decimal(x).quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)
    return float(d)


def _pct(numerator, denominator, places: int = 4) -> float:
    """Decimal-safe percentage with zero-denominator guard. Returns
    0.0 on zero denominator (matches rig fail-closed convention for
    money ratios)."""
    num = _to_decimal(numerator)
    den = _to_decimal(denominator)
    if den == 0:
        return 0.0
    quant = Decimal("1").scaleb(-places)  # 10^-places
    return float(
        (num / den * Decimal("100")).quantize(
            quant, rounding=ROUND_HALF_UP,
        )
    )


def _to_float(x):
    """Legacy float coercion kept for existing callers that don't
    need Decimal precision (search / lookup / sort keys)."""
    if x is None:
        return 0.0
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _asset_value(investment_service, asset_type, user_id) -> Decimal:
    """Current rupee value for one asset type, as Decimal.

    Dispatches per the existing InvestmentService entry points:
      * MSN types (Stocks / Mutual_Funds / NPS):
        ``fetchSummary(asset, user_id)['currentValue']``.
      * EPG types (EPF / PF / Gold):
        ``fetchActiveSecurities(EPGEnum.<asset>, user_id)['net']``
        (deposits + accrued profit).

    Returns Decimal('0') on any lookup failure — matches the ak-ran
    WealthDigest convention of "unavailable asset types fall out
    of the allocation rather than aborting the whole computation".
    """
    from enums.MsnEnum import MSNENUM
    from enums.EPGEnum import EPGEnum
    try:
        if asset_type in _MSN_TYPES:
            summary = investment_service.fetchSummary(asset_type, user_id)
            if isinstance(summary, dict):
                return _to_decimal(summary.get("currentValue"))
            return _to_decimal(getattr(summary, "currentValue", 0))
        if asset_type in _EPG_TYPES:
            epg_type = EPGEnum[asset_type]
            payload = investment_service.fetchActiveSecurities(
                epg_type, user_id,
            )
            if isinstance(payload, dict):
                # PPF / Gold / EPF all return `net` as current value
                # (deposits + accrued profit). See each *Service.fetchComplete.
                return _to_decimal(payload.get("net"))
            return Decimal("0")
    except Exception:
        return Decimal("0")
    return Decimal("0")


def _fo_pnl(investment_service, user_id):
    """Optional FO add-on. Returns (pnl_decimal, present_bool).

    ak-p8ix REWORK MAJOR 1: FO P&L is a profit/loss figure, NOT a
    current position value. Folding it into the allocation ``total``
    distorts every % slice + the ``position_sizing`` /
    ``concentration_risk`` denominators. The caller (which used to
    add this to total) now surfaces it as a SEPARATE top-level
    ``fo_pnl`` field — never in ``total`` or ``by_asset_type``.
    """
    try:
        summary = investment_service.fetchFOSummary(user_id)
        if not isinstance(summary, dict):
            return Decimal("0"), False
        # fetchFOSummary returns {"totalPnl": X, ...} per the
        # FOService shape. Realized+unrealized combined.
        pnl = summary.get("totalPnl")
        if pnl is None:
            return Decimal("0"), False
        return _to_decimal(pnl), True
    except Exception:
        return Decimal("0"), False


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

    # ak-p8ix REWORK MAJOR 1: allocation covers POSITION-VALUE
    # asset types ONLY. FO P&L is surfaced below as a separate
    # top-level field; it never enters the denominator.
    buckets = []
    total = Decimal("0")
    for asset in _MSN_TYPES + _EPG_TYPES:
        value_d = _asset_value(investment_service, asset, user_id)
        buckets.append({"type": asset, "_value_d": value_d})
        total += value_d

    # FO handled independently of the allocation %. See MAJOR 1
    # above for rationale.
    fo_pnl_d, fo_present = _fo_pnl(investment_service, user_id)

    # Per-bucket pct + float coercion at the JSON boundary.
    for b in buckets:
        b["value"] = _money(b["_value_d"])
        b["pct"] = _pct(b["_value_d"], total)
        del b["_value_d"]

    # Sort by value descending (largest slice first).
    buckets.sort(key=lambda b: b["value"], reverse=True)

    from datetime import datetime
    return {
        "tool": "portfolio_allocation",
        "as_of": datetime.utcnow().isoformat() + "Z",
        "total": _money(total),
        # ak-p8ix REWORK MAJOR 1: separate line. None when FO
        # service is not wired for this user; a number (possibly
        # negative — P&L can be a loss) otherwise. The agent MUST
        # report this as "F&O profit/loss" and NOT as part of the
        # allocation breakdown. See tool description.
        "fo_pnl": _money(fo_pnl_d) if fo_present else None,
        "by_asset_type": buckets,
    }


def benchmark_compare(investment_service, user_id,
                      from_date: str, to_date: str,
                      benchmark: Optional[str] = None) -> dict:
    """Equity-basket return over [from_date, to_date] + benchmark
    WebSearch plan.

    ak-p8ix REWORK MAJOR 2 (naming + scope disclosure):

    The return computed here covers ONLY the EQUITY slice of the
    portfolio. ``getPortfolioHistory`` (ak-4o41 Bundle 1) projects
    TODAY's held stock quantities onto each past date's close
    from ``stockHistoricalCandles``. Non-equity assets (MF / NPS /
    EPF / PF / Gold) have their own historical shapes and are NOT
    included in this projection.

    Consequence: for an MF/NPS/EPG-heavy portfolio this return is
    NOT whole-portfolio performance. Pre-rework the field was
    named ``portfolio_return_pct`` which read as whole-portfolio
    and would mislead the agent + the user. Renamed to
    ``equity_basket_return_pct`` + ``scope`` + ``caveat`` fields
    added so the agent MUST disclose the scope when quoting.

    Benchmark side (unchanged): returns a structured search plan
    the agent calls WebSearch on. Matches the Bead B wrapper
    pattern — keeps this module network-free + lands the benchmark
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
             equity_basket_return_pct, scope, caveat, benchmark:
             {name, canonical_query, sources, extraction,
             instructions}}``
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

    from_value_d = _to_decimal(in_window[0][1])
    to_value_d = _to_decimal(in_window[-1][1])
    if from_value_d == 0:
        equity_basket_return_pct = None
    else:
        # Decimal percentage: (to - from) / from × 100, HALF_UP to 4dp.
        equity_basket_return_pct = _pct(
            to_value_d - from_value_d, from_value_d,
        )

    bench_name = (benchmark or _DEFAULT_BENCHMARK).strip() or _DEFAULT_BENCHMARK

    return {
        "tool": "benchmark_compare",
        "from_date": from_date,
        "to_date": to_date,
        "from_value": _money(from_value_d),
        "to_value": _money(to_value_d),
        # ak-p8ix REWORK MAJOR 2 (rename): was portfolio_return_pct.
        # Scope-accurate name — this is the EQUITY slice only.
        "equity_basket_return_pct": equity_basket_return_pct,
        # Explicit scope + caveat so the agent can't present this
        # as whole-portfolio by accident.
        "scope": "stocks_only_current_basket",
        "caveat": (
            "Return projects today's held stock quantities onto "
            "past dates using stockHistoricalCandles. Non-equity "
            "assets (MF / NPS / EPF / PF / Gold) are NOT included. "
            "For MF/NPS/EPG-heavy portfolios this materially "
            "misrepresents whole-portfolio performance; disclose "
            "the scope to the user when quoting this number."
        ),
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
    # of truth for 'total'). Post ak-p8ix REWORK MAJOR 1 the total
    # no longer includes FO P&L, so the denominator here is a clean
    # position-value total (no distortion from P&L signs).
    alloc = portfolio_allocation(investment_service, user_id)
    total_d = _to_decimal(alloc["total"])

    # Current position value — Decimal via _find_symbol_value.
    current_d = _find_symbol_value(investment_service, user_id, symbol)
    if current_d is None:
        return {
            "tool": "position_sizing",
            "error_class": "symbol_not_found",
            "message": (
                f"{symbol!r} not found in local Stocks / MF / NPS "
                "holdings. Use search_securities first to resolve "
                "the exact name → code mapping (the match-key is "
                "the statement buyCode / scheme code, not the "
                "human-readable name)."
            ),
            "symbol": symbol,
        }

    target_d = _to_decimal(target_allocation_pct) / Decimal("100") * total_d
    delta_d = target_d - current_d

    # Hold-band: within ₹1 of target → no-op. Decimal comparison
    # avoids float-imprecision near the boundary.
    if abs(delta_d) < Decimal("1"):
        direction = "hold"
    elif delta_d > 0:
        direction = "buy"
    else:
        direction = "trim"

    return {
        "tool": "position_sizing",
        "symbol": symbol,
        "total_portfolio": _money(total_d),
        "current_value": _money(current_d),
        "current_pct": _pct(current_d, total_d),
        "target_pct": target_allocation_pct,
        "target_value": _money(target_d),
        "delta_rupees": _money(delta_d),
        "direction": direction,
    }


def _iter_user_security_rows(investment_service, user_id):
    """Yield ``(asset_type, row_dict)`` tuples over EVERY MSN-type
    holding the user has.

    ak-p8ix REWORK CRITICAL: ``InvestmentService.fetchUserSecurities``
    actually returns a **LIST** of enriched row dicts on success
    (``return activeSecurities`` at InvestmentService.py:375), or
    ``{"error": "No active securities"}`` on empty. The pre-rework
    helpers read a fictional ``{"data": [...]}`` /
    ``{"results": [...]}`` shape and SKIPPED every real row via a
    ``not isinstance(payload, dict)`` guard — which meant
    ``position_sizing`` always returned ``symbol_not_found`` and
    ``concentration_risk`` always reported "well-diversified".

    Row shape (per InvestmentService.fetchUserSecurities body):
      * ``buyCode`` — statement identifier (THE identifier field;
        NOT ``securityCode`` or ``stockCode``).
      * ``buyQuant`` — statement-settled quantity (Decimal).
      * ``buyPrice`` — per-unit cost (Decimal).
      * ``info`` — nested dict with ``lastPrice`` (NAV / live LTP
        depending on asset type).
      * ``current_value`` — snake_case, PRESENT for Kite-enriched
        Stocks rows (ak-yz9c fold-in); ABSENT for non-Kite rows.
    """
    from enums.MsnEnum import MSNENUM
    for mtype in ("Stocks", "Mutual_Funds", "NPS"):
        try:
            payload = investment_service.fetchUserSecurities(
                MSNENUM[mtype].value, user_id,
            )
        except Exception:
            continue
        # Empty-holdings path returns {"error": "..."} — skip dicts.
        if not isinstance(payload, list):
            continue
        for row in payload:
            if not isinstance(row, dict):
                continue
            yield mtype, row


def _row_value(row) -> Decimal:
    """Per-row rupee value. Prefers Kite-enriched ``current_value``
    (snake_case), falls back to ``buyQuant × info.lastPrice``.

    Returns Decimal('0') if neither source yields a value — the
    row is still iterable in concentration_risk but won't flag.
    """
    # Kite-enriched row (Stocks only) carries current_value directly.
    cv = row.get("current_value")
    if cv is not None:
        return _to_decimal(cv)
    # Fallback for non-Kite rows (MF / NPS / pre-sync Stocks):
    # buyQuant × info.lastPrice.
    qty = _to_decimal(row.get("buyQuant"))
    info = row.get("info") or {}
    last_price = _to_decimal(info.get("lastPrice"))
    return qty * last_price


def _row_identifier(row) -> Optional[str]:
    """Statement identifier for a holding row. Real contract uses
    ``buyCode`` (NOT ``securityCode``). Also inspects ``info.symbol``
    / ``info.name`` as a human-readable fallback for display, but
    the authoritative match-key is ``buyCode``."""
    bc = row.get("buyCode")
    if isinstance(bc, str) and bc.strip():
        return bc.strip()
    info = row.get("info") or {}
    for k in ("symbol", "scheme_id", "name"):
        v = info.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _find_symbol_value(investment_service, user_id, symbol):
    """Lookup the current rupee value of a single symbol across
    the user's Stocks + MF + NPS holdings. Returns None if
    unmatched.

    ak-p8ix REWORK CRITICAL: walks the REAL fetchUserSecurities
    contract (list of row dicts keyed by ``buyCode``). Matches
    case-insensitively against ``buyCode`` first (the authoritative
    statement identifier), then against ``info.symbol`` /
    ``info.name`` so the agent can match on either the exact
    scheme-code OR the user-visible name.
    """
    sym_lower = symbol.lower()
    for _mtype, row in _iter_user_security_rows(investment_service, user_id):
        candidates = [row.get("buyCode")]
        info = row.get("info") or {}
        candidates.extend([info.get("symbol"), info.get("name")])
        for cand in candidates:
            if (
                isinstance(cand, str)
                and cand.strip().lower() == sym_lower
            ):
                return _row_value(row)
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
    threshold_d = _to_decimal(threshold_pct)

    # Use portfolio_allocation to establish total (DRY). Post
    # ak-p8ix REWORK MAJOR 1 this excludes FO (correct — we're
    # measuring concentration in POSITION value).
    alloc = portfolio_allocation(investment_service, user_id)
    total_d = _to_decimal(alloc["total"])
    if total_d == 0:
        return {
            "tool": "concentration_risk",
            "threshold_pct": threshold_pct,
            "total": 0.0,
            "flagged": [],
        }

    # ak-p8ix REWORK CRITICAL: walk the REAL fetchUserSecurities
    # contract (list of row dicts). Per-row value from
    # `current_value` (Kite-enriched) or `buyQuant × info.lastPrice`
    # (fallback) — see _row_value for the derivation + rationale.
    flagged = []
    for mtype, row in _iter_user_security_rows(investment_service, user_id):
        val_d = _row_value(row)
        if val_d <= 0:
            continue
        pct_val = _pct(val_d, total_d)
        if pct_val < threshold_pct:
            continue
        name = _row_identifier(row) or "<unnamed>"
        flagged.append({
            "security": name,
            "asset_type": mtype,
            "value": _money(val_d),
            "pct": pct_val,
        })

    # EPG asset-type-level check — for EPF / PPF / Gold a single
    # asset-type over threshold is also a concentration signal
    # (per-position rows don't exist the way equity does — EPF is
    # one aggregate per employer; Gold rows exist per-purchase but
    # semantically one asset class).
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
        "total": _money(total_d),
        "flagged": flagged,
    }
