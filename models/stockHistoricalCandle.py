"""ak-4o41 Bundle 1: historical EOD candle cache.

Per-instrument-per-day OHLCV rows, populated by
``BackfillHistoricalCandlesTask`` from Kite's historical_data endpoint.
Consumed by the ``/investment-snapshot/history`` endpoint to compute
per-date portfolio value aggregates (``sum(buyQuant_i × close_i)``
across all held instruments) for the WealthDigest PortfolioTrendChart.

## Scope

Daily candles only (``interval='day'`` on the Kite endpoint). Intraday
intervals are out of scope for this bundle — the trend chart is a
30-365 day rolling view where sub-day granularity adds no signal.

## Shape rationale

* ``instrument_token`` is Kite's canonical NSE/BSE token (NOT trading
  symbol). Stable across renames — ``RELIANCE`` and ``RELIANCE-EQ`` both
  map to the same token, so a symbol rename / series change doesn't
  break historical lookups.
* No ``user_id`` column: NSE/BSE EOD candles are market-wide. Every user
  reads the same candle. (Single-user app today; holds under multi-user
  if ever added.)
* Unique constraint ``(instrument_token, trade_date)`` enables
  idempotent upsert — re-running the backfill task over an overlap
  window doesn't produce dupes.
* ``Numeric(14, 4)`` matches NSE price precision (paise); ``BigInteger``
  volume handles mid/large-cap daily turnover (NSE's max daily volume
  on a single stock ≈ 10⁹, well within signed 64-bit).

## Related arcs

* ak-874: closed by this bundle. That bead originally proposed
  aggregating from ``InvestmentSnapshot`` (per-day per-type totals),
  but Kite historical gives deep backfill the snapshot task can't
  (snapshot task starts at install-time; Kite reaches back years).
* ak-f1a: tracks "dead Kite wrappers". The ``get_historical_data`` +
  ``get_ltp`` + ``get_quote`` + ``get_ohlc`` wrappers added in this
  bundle are USED (not dead) — this bundle is the "make them live"
  commit.
"""
from sqlalchemy import Column, Integer, BigInteger, Date, Numeric, UniqueConstraint, Index

from models.Base import Base


class StockHistoricalCandle(Base):
    __tablename__ = 'stockHistoricalCandles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Kite instrument_token (not exchange_token; see Kite docs — the
    # `instrument_token` field is the one historical_data accepts).
    instrument_token = Column(BigInteger, nullable=False)
    trade_date = Column(Date, nullable=False)
    open = Column(Numeric(14, 4), nullable=False)
    high = Column(Numeric(14, 4), nullable=False)
    low = Column(Numeric(14, 4), nullable=False)
    close = Column(Numeric(14, 4), nullable=False)
    volume = Column(BigInteger, nullable=False, default=0)

    __table_args__ = (
        # Idempotent upsert key — repeated backfill of the same date
        # must not produce duplicate rows. ``ON DUPLICATE KEY UPDATE``
        # against this UK is how the backfill task preserves at-most-one
        # row per (token, date).
        UniqueConstraint('instrument_token', 'trade_date',
                         name='uq_candle_token_date'),
        # History lookup by token across a date window (the
        # ``/investment-snapshot/history`` per-holding fetch shape).
        Index('ix_candle_token_date', 'instrument_token', 'trade_date'),
        # Portfolio aggregation shape — group-by date across many tokens
        # for the N-day window query.
        Index('ix_candle_date', 'trade_date'),
    )
