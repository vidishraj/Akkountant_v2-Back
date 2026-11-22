"""ak-rcg1 (ak-ofir Phase 2B): pure synthesis of past-dated
InvestmentSnapshot rows from the Phase-2A historical caches +
existing SecurityTransactions / DepositSecurities tables.

Called by the ``scripts/synthesize_historical_snapshots.py``
backfill CLI. Factored into a pure module so the per-asset-class
synthesis logic can be tested without booting Flask / Firebase /
Google chain — matches the ``portfolio_history_aggregator`` pattern
(ak-qeex).

## Contract

Each public function below takes concrete inputs (dicts / tuples /
Decimals) and returns a plain dict matching the live daemon's
``InvestmentSnapshot`` row shape so synthesized rows are
visually indistinguishable from daemon-written ones in operator
audit:

    {
        'date': date,
        'user': str,
        'investment_type': str,  # Stocks | MF | NPS | EPF | PPF | Gold
        'total_invested': Decimal,  # quantized 0.01 ROUND_DOWN
        'current_value': Decimal,
        'profit': Decimal,
        'profit_percent': Decimal,  # quantized 0.0001 ROUND_DOWN
    }

Rounding: matches ``services/tasks/investmentSnapshotTask.py:
_make_row`` — Decimal quantize 0.01 ROUND_DOWN for money,
0.0001 ROUND_DOWN for percent. ROUND_DOWN **truncates toward
zero** (NOT HALF_DOWN, which rounds-to-even on ties); the two
differ by up to one paise at the quantize boundary, so this
distinction is load-bearing for daemon-parity. Intentionally kept
parallel so synthesis rows are byte-identical to daemon rows for
the same inputs (reduces audit noise).

## Per-asset-class strategies

### Stocks / MF / NPS

Reconstruct per-buyId state as-of ``target_date`` by walking
``SecurityTransactions`` chronologically (events are
(buy|sell, date, qty, price) per buyId). Buys compound into a
weighted-avg price + cumulative qty; sells reduce qty but leave
price unchanged (matches ``MfService.buySecurity`` /
``NpsService.buySecurity`` / ``StocksService`` mutation shape).

A buyId whose first transaction is AFTER target_date is skipped
entirely (the user didn't own it yet).

Value at D = Σ (qty_at_D × historical_price(buyCode, D)) over the
PRICED subset only.
Invested at D = Σ (qty_at_D × avg_price_at_D) over the SAME
PRICED subset — ak-rcg1 re-review MAJOR fix (2026-11-22). The
pre-fix behaviour summed invested over ALL holdings while value
summed only over priced holdings, so ``profit = value − invested``
was computed over DIFFERENT holding sets. On partial-coverage
dates (50-99%) that made the profit figure outright wrong — a
slice that was genuinely +100% could record 0% profit because
cost basis for the unpriced half was counted but the value wasn't.
Per §2 accounting-correctness, the profit line is the invariant
the fix protects: value is inherently undercounted on partial
dates (bounded by the coverage floor below), but profit now
honestly reflects the priced subset's cost basis.

If more than half of a slice's holdings have missing NAVs, the
slice is OMITTED entirely from the output (strict coverage rule,
same discipline as portfolio_history_aggregator's
``expected_coverage`` cut). The 50% floor caps the magnitude of
the value undercount: at worst, the reported value reflects half
of the holdings — the profit arithmetic above it is honest for
exactly those holdings.

### EPF / PPF

Deterministic: delegate to the existing
``calculateTransactionTable`` (filtered deposits where
``date <= target_date``). Final balance = ``current_value``,
``totalContributions`` = ``total_invested``, ``profit`` = difference.

Approximation: the walker only accrues interest ON deposit months.
Between the last deposit and target_date, no additional interest
is applied. Matches the LIVE DAEMON's calculation shape exactly —
same approximation daemon writes for current-date snapshots. The
resulting trend-chart shows month-by-month step-ups rather than
daily interest drift (accurate for the deposit granularity).

### Gold

qty × ``goldHistoricalRates.rate_per_gram(target_date)`` × purity
multiplier (24K → 1.0, 22K → 0.916, 18K → 0.75). If no rate for
target_date → OMIT the gold slice (don't invent zeros). Per
Overseer 2026-11-22 ak-ofir §5 scope-cut, gold historical is
opt-in via operator-supplied CSV; dates without operator data are
simply absent from the trend, not misrepresented as ₹0.

## Idempotency

Pure functions; DB UPSERT lives in the caller. Re-running with the
same inputs produces the same output row dicts — the caller's
``INSERT ... ON DUPLICATE KEY UPDATE`` keyed on
``(date, user, investment_type)`` handles dupe safety.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as date_cls
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Iterable, List, Optional, Tuple

# Quantization targets — must match
# services/tasks/investmentSnapshotTask.py:_make_row so synthesis
# rows are byte-identical to daemon rows.
_MONEY_QUANT = Decimal("0.01")
_PCT_QUANT = Decimal("0.0001")

# Per-purity gold scaling — anchored on 24-carat (999 purity) as
# the single stored rate_per_gram. 22K ≈ 91.6% of 24K; 18K = 75%.
# Matches the IBJA scraper's carat-vs-999 ratio.
_GOLD_PURITY_SCALE = {
    "24": Decimal("1.0000"),
    "22": Decimal("0.9160"),
    "18": Decimal("0.7500"),
}

# Live-daemon TYPE_LABELS parity — synthesized rows MUST use these
# exact keys so getInvestmentSnapshots queries can pick them up
# alongside daemon-written rows.
TYPE_LABEL_STOCKS = "Stocks"
TYPE_LABEL_MF = "MF"
TYPE_LABEL_NPS = "NPS"
TYPE_LABEL_EPF = "EPF"
TYPE_LABEL_PPF = "PPF"
TYPE_LABEL_GOLD = "Gold"

# Coverage floor per-asset slice. If the fraction of held buyIds
# with a historical price lookup on target_date falls below this,
# we OMIT the whole slice from the output rather than publish a
# materially-undercounted number. Matches the strict-coverage rule
# portfolio_history_aggregator applies on the stock-trend side.
_SLICE_COVERAGE_FLOOR = Decimal("0.5")


@dataclass(frozen=True)
class TxEvent:
    """One SecurityTransactions event — enough to reconstruct the
    per-buyId state at any target date."""
    buy_id: str
    buy_code: str
    tx_date: date_cls
    qty: Decimal
    price: Decimal
    tx_type: str  # 'buy' | 'sell'


@dataclass(frozen=True)
class DepositEvent:
    """One DepositSecurities row — enough to replay EPF/PPF
    deposit-driven state."""
    buy_id: str
    deposit_date: date_cls
    employee_amount: Decimal  # also used for PPF (single amount)
    employer_amount: Optional[Decimal]  # None for PPF


@dataclass(frozen=True)
class GoldHolding:
    """One (buyId, qty, goldType) triple from DepositSecurities +
    GoldDetails joined."""
    buy_id: str
    deposit_date: date_cls
    qty_grams: Decimal
    gold_type: str  # '18' | '22' | '24'


def _money(x) -> Decimal:
    """Quantize to 0.01 ROUND_DOWN (truncate toward zero) — matches
    daemon ``_make_row`` rounding at
    services/tasks/investmentSnapshotTask.py:199-202.
    ROUND_DOWN differs from HALF_DOWN by up to one paise at the
    quantize boundary; swapping modes silently breaks daemon
    parity. See the daemon-parity test's tie-breaking assertion
    for the pin-down."""
    return Decimal(str(x)).quantize(_MONEY_QUANT, rounding=ROUND_DOWN)


def _pct(x) -> Decimal:
    return Decimal(str(x)).quantize(_PCT_QUANT, rounding=ROUND_DOWN)


def reconstruct_msn_holdings_at(
    events: Iterable[TxEvent],
    target_date: date_cls,
) -> Dict[str, Tuple[Decimal, Decimal, str]]:
    """Replay SecurityTransactions events up to target_date to
    reconstruct per-buyId state.

    Matches the LIVE MUTATION SHAPE of MfService.buySecurity /
    NpsService.buySecurity / Stocks service:
      * On buy: new_qty = old_qty + buy_qty;
                new_avg_price = ((old_qty × old_price) +
                                 (buy_qty × buy_price)) / new_qty
      * On sell: qty -= sell_qty (price unchanged)

    Returns ``{buy_id: (qty_at_D, avg_price_at_D, buy_code)}``.
    buyIds whose first tx is > target_date are absent from the map.
    Zero-qty rows (fully sold) are also absent — match the daemon's
    ``buyQuant > 0`` filter.
    """
    # Sort tx chronologically; ties resolved by tx_type ('buy'
    # before 'sell' so same-day buy-then-sell order is natural).
    sorted_events = sorted(
        events, key=lambda e: (e.tx_date, e.tx_type),
    )
    state: Dict[str, Tuple[Decimal, Decimal, str]] = {}
    for evt in sorted_events:
        if evt.tx_date > target_date:
            break
        cur = state.get(evt.buy_id)
        if evt.tx_type == "buy":
            if cur is None:
                state[evt.buy_id] = (
                    evt.qty, evt.price, evt.buy_code,
                )
            else:
                old_qty, old_price, buy_code = cur
                new_qty = old_qty + evt.qty
                if new_qty > 0:
                    new_price = (
                        (old_qty * old_price) + (evt.qty * evt.price)
                    ) / new_qty
                else:
                    # Degenerate — buy-after-full-sell with zero
                    # resulting qty. Keep the new buy as the base.
                    new_price = evt.price
                state[evt.buy_id] = (new_qty, new_price, buy_code)
        elif evt.tx_type == "sell":
            if cur is None:
                # Sell-before-buy — ignore (data integrity issue;
                # defensive-skip rather than crash the backfill).
                continue
            old_qty, old_price, buy_code = cur
            new_qty = old_qty - evt.qty
            state[evt.buy_id] = (new_qty, old_price, buy_code)
        # else: unknown tx_type — skip (defensive).

    # Drop zero-or-negative-qty rows — matches live ``buyQuant > 0``
    # filter in all MSN services.
    return {
        buy_id: (qty, price, buy_code)
        for buy_id, (qty, price, buy_code) in state.items()
        if qty > 0
    }


def synthesize_msn_slice(
    *,
    target_date: date_cls,
    user_id: str,
    investment_type: str,  # 'Stocks' | 'MF' | 'NPS'
    events: Iterable[TxEvent],
    price_lookup,  # (buy_code, target_date) -> Optional[Decimal]
) -> Optional[dict]:
    """Synthesize one investmentSnapshot row for an MSN slice.

    Returns the row dict ready for UPSERT, or ``None`` when the
    coverage floor isn't met (< 50% of held buyIds have a price
    for target_date) — strict-coverage rule matches
    portfolio_history_aggregator.

    Also returns ``None`` when the reconstructed holdings map is
    empty at target_date (user didn't own anything of this type on
    that day — no row is correct here, not a zero row).
    """
    holdings = reconstruct_msn_holdings_at(events, target_date)
    if not holdings:
        return None

    # ak-rcg1 re-review MAJOR fix (2026-11-22): accumulate
    # total_invested + current_value over the SAME holding set
    # (the priced subset). Pre-fix, total_invested summed over ALL
    # holdings while current_value summed only over PRICED
    # holdings → profit = value − invested was computed over
    # DIFFERENT holding sets, producing a "lie on the trend chart"
    # on partial-coverage dates. Concrete case: 4 holdings each
    # 10u@₹100 (invested ₹4000), only 2 priced @₹200 (value ₹4000)
    # → old code recorded profit=₹0 / 0% when the slice was
    # genuinely +100%. Per §2 accounting-correctness rig constraint,
    # profit MUST reflect the priced subset's cost basis, not the
    # full holdings' cost basis. The coverage floor still bounds
    # the slice-value undercount; the PROFIT lie is what this fix
    # closes.
    total_invested = Decimal("0")
    current_value = Decimal("0")
    priced_count = 0
    for buy_id, (qty, avg_price, buy_code) in holdings.items():
        price = price_lookup(buy_code, target_date)
        if price is None:
            continue
        total_invested += qty * avg_price
        current_value += qty * price
        priced_count += 1

    coverage = (
        Decimal(priced_count) / Decimal(len(holdings))
        if holdings else Decimal("0")
    )
    if coverage < _SLICE_COVERAGE_FLOOR:
        return None

    profit = current_value - total_invested
    if total_invested > 0:
        profit_pct = (profit / total_invested) * Decimal("100")
    else:
        profit_pct = Decimal("0")

    return _row(
        target_date, user_id, investment_type,
        total_invested, current_value, profit, profit_pct,
    )


def synthesize_epg_deposit_slice(
    *,
    target_date: date_cls,
    user_id: str,
    investment_type: str,  # 'EPF' | 'PPF'
    deposits: Iterable[DepositEvent],
    calc_transaction_table,
    # calc_transaction_table(filtered_deposits) -> (transactions,
    #     net_profit, final_balance, running_interest)
) -> Optional[dict]:
    """Synthesize one investmentSnapshot row for EPF or PPF.

    Delegates to the service's existing calculateTransactionTable
    with deposits filtered to ``date <= target_date``. Matches the
    live daemon's shape exactly — same rounding, same interest
    approximation (deposit-month-driven accrual).

    Returns ``None`` when the user had no deposits by target_date
    (no row is correct; don't publish a zero row).
    """
    filtered = [d for d in deposits if d.deposit_date <= target_date]
    if not filtered:
        return None

    sorted_deposits = sorted(filtered, key=lambda d: d.deposit_date)
    _, net_profit, final_balance, _ = calc_transaction_table(
        sorted_deposits,
    )
    if final_balance is None:
        return None

    # calculateTransactionTable returns floats + Decimals mixed.
    # Coerce via str() → Decimal for lossless boundary.
    total_invested = Decimal(str(final_balance)) - Decimal(str(net_profit))
    current_value = Decimal(str(final_balance))
    profit = Decimal(str(net_profit))
    if total_invested > 0:
        profit_pct = (profit / total_invested) * Decimal("100")
    else:
        profit_pct = Decimal("0")

    return _row(
        target_date, user_id, investment_type,
        total_invested, current_value, profit, profit_pct,
    )


def synthesize_gold_slice(
    *,
    target_date: date_cls,
    user_id: str,
    holdings: Iterable[GoldHolding],
    gold_rate_per_gram_24k: Optional[Decimal],
    # Operator-supplied value for target_date, or None if no rate.
    deposit_amount_lookup,
    # deposit_amount_lookup(buy_id) -> Decimal — resolves the
    # original DepositSecurities.depositAmount (₹) for a buyId.
    # Separate arg so test fixtures can supply a plain dict without
    # importing the DB model.
) -> Optional[dict]:
    """Synthesize one investmentSnapshot row for Gold.

    Per Overseer 2026-11-22 scope-cut: gold historical is opt-in via
    operator-supplied rates. If no rate for ``target_date`` → OMIT
    the slice (returns None). No zero rows.

    Also returns None when the user held no gold on target_date.
    """
    filtered = [h for h in holdings if h.deposit_date <= target_date]
    if not filtered:
        return None
    if gold_rate_per_gram_24k is None:
        return None

    total_invested = Decimal("0")
    current_value = Decimal("0")
    for h in filtered:
        orig_cost = Decimal(str(deposit_amount_lookup(h.buy_id)))
        total_invested += orig_cost
        purity = _GOLD_PURITY_SCALE.get(h.gold_type, Decimal("1.0000"))
        current_value += h.qty_grams * gold_rate_per_gram_24k * purity

    profit = current_value - total_invested
    if total_invested > 0:
        profit_pct = (profit / total_invested) * Decimal("100")
    else:
        profit_pct = Decimal("0")

    return _row(
        target_date, user_id, TYPE_LABEL_GOLD,
        total_invested, current_value, profit, profit_pct,
    )


def _row(
    target_date, user_id, investment_type,
    total_invested, current_value, profit, profit_pct,
):
    """Assemble the InvestmentSnapshot-shaped dict with rounding
    that matches the live daemon's ``_make_row`` byte-for-byte."""
    return {
        "date": target_date,
        "user": user_id,
        "investment_type": investment_type,
        "total_invested": _money(total_invested),
        "current_value": _money(current_value),
        "profit": _money(profit),
        "profit_percent": _pct(profit_pct),
    }
