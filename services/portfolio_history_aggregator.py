"""ak-qeex: pure aggregation helper for /investment-snapshot/history.

Extracted from ``InvestmentService.getPortfolioHistory`` so the
aggregation + coverage-omission logic can be unit-tested in isolation
without needing a real DB session or the full InvestmentService import
chain (which transitively pulls Google OAuth + Firebase + statement
parsers).

Reviewer verdict on ak-4o41 (3202ea6) called out MINOR-2: Phase-C
aggregation had no automated test. This module is the testable seam
so that gap closes.
"""
from decimal import Decimal


def aggregate_portfolio_history(token_qty, candles):
    """Pure aggregation + coverage-omission for the portfolio trend.

    Groups ``candles`` by ``trade_date``, computes
    ``Σ (token_qty[token] × close)`` per date, and OMITS dates where
    fewer than ``len(token_qty)`` tokens reported — strict coverage
    rule prevents phantom dips when one holding's candle is missing
    (would otherwise drag that date's total down by the missing
    instrument's full contribution and mislead the trend chart).

    Args:
      token_qty: ``{instrument_token: Decimal_qty}`` — held positions
                 after dedupe.
      candles: iterable of ``(instrument_token, trade_date, close)``
               tuples — direct output of the StockHistoricalCandle
               query (``.filter(...).all()``). ``close`` may be
               ``Decimal`` or numeric.

    Returns:
      ``[{date: 'YYYY-MM-DD', total_value: float}, ...]`` sorted
      ascending by date. Dates with incomplete coverage omitted.
      Empty ``token_qty`` returns ``[]`` immediately.
    """
    if not token_qty:
        return []
    per_date_sum = {}
    per_date_count = {}
    for token, trade_date, close in candles:
        # Defense-in-depth: skip candles for tokens we don't hold.
        # The .in_() SQL filter should already prevent this, but a
        # future query-widening refactor shouldn't produce KeyError.
        if token not in token_qty:
            continue
        per_date_sum.setdefault(trade_date, Decimal(0))
        per_date_count.setdefault(trade_date, 0)
        per_date_sum[trade_date] += token_qty[token] * close
        per_date_count[trade_date] += 1

    expected_coverage = len(token_qty)
    result = []
    for d in sorted(per_date_sum.keys()):
        if per_date_count[d] < expected_coverage:
            # Incomplete-coverage date — omit.
            continue
        result.append({
            'date': d.strftime('%Y-%m-%d'),
            'total_value': float(per_date_sum[d]),
        })
    return result
