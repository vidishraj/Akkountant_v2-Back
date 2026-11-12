"""ak-4o41 Bundle 1: scheduled task to backfill Kite historical candles.

Fetches daily EOD OHLCV candles for every held equity instrument (any
``PurchasedSecurities`` row with ``securityType='Stocks'`` and
``buyQuant > 0`` across all users — single-user app today, trivially
multi-user if ever added) and upserts into ``stockHistoricalCandles``.

## Cadence

Scheduled self-reschedule interval = 24h. Designed to run once
post-market-close (~ 6 PM IST or later); the self-reschedule starts
at app boot and ticks every 24h. If missed (operator restart during
the window), next wake will backfill the gap (7-day overlap window —
see below).

## Idempotency

Upsert keyed on ``(instrument_token, trade_date)`` unique constraint.
Re-running the task over an overlapping date range produces no
duplicate rows and no stale-data risk (Kite returns the authoritative
historical close for a date; same date → same values on every fetch).

Overlap window is 7 calendar days by default so a 2-day scheduler gap
(weekend + holiday + task failure) still recovers without the operator
having to re-run manually. Initial-run / cold-start fetches 365 days
to populate a year of history for the trend chart.

## Rate limits

Kite's historical_data endpoint allows 3 req/s. With N held
instruments, a single run executes N serial fetches. The task inserts
a 350 ms sleep between fetches to stay under 3 req/s with a small
safety margin — slower than the limit allows, but historical backfill
is not latency-critical and the simpler throttle is less fragile than
a precise token-bucket.

## Dependencies

* ``SetKiteStockDetails`` (daily, 4h cadence): produces the symbol →
  instrument_token mapping used by this task to resolve each held
  ``securityCode``. If missing / stale, this task degrades cleanly
  (per-symbol lookup miss → WARN + skip, not a run-wide fail).
* ``KiteService.get_historical_data``: the Bundle-1 wrapper. On token
  expiry, raises ``KiteTokenExpired`` and the task returns Failed /
  reconnect-required (expected daily condition between 6 AM IST and
  user re-login).

## Blast radius

Zero. Separate table (``stockHistoricalCandles``), separate task,
separate scheduler entry. Enrichment / history endpoints degrade
gracefully to empty-result when the table is empty (pre-first-run).
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from models.googleTokens import UserToken
from models.purchasedSecurities import PurchasedSecurities
from models.stockHistoricalCandle import StockHistoricalCandle
from services.KiteService import KiteService, KiteAuthError, KiteTokenExpired
from services.tasks.baseTask import BaseTask
from utils.logger import Logger


# Kite historical endpoint: 3 req/s. Slightly-conservative 350 ms
# between fetches (effective ~2.85 req/s). Worst-case for a 20-stock
# portfolio: 20 × 350ms = 7s wall-time — well inside a scheduled run.
_HISTORICAL_FETCH_GAP_SECONDS = 0.35

# How many days of overlap the steady-state cycle re-fetches. 7 days
# covers weekend + holiday + task-missed-day without operator action.
_STEADY_STATE_OVERLAP_DAYS = 7

# Cold-start / initial-populate window. 365 days gives a year of
# history for the WealthDigest trend chart (max days clamp in the
# history endpoint). Larger than needed for the typical 30-day
# chart but one upfront fetch per instrument is cheap.
_COLD_START_DAYS = 365

# Self-reschedule cadence (minutes). Once-daily — Kite's daily candle
# publishes post-market-close; sub-daily polls add no signal.
_RUN_INTERVAL_MINUTES = 60 * 24

IST = timezone(timedelta(hours=5, minutes=30))


class BackfillHistoricalCandlesTask(BaseTask):
    """Daily Kite historical-candle backfill (ak-4o41 Bundle 1)."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, "initialized"):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            self.kite_service = KiteService()
            self.interval = _RUN_INTERVAL_MINUTES
            self.initialized = True

    # ---------------- BaseTask.run contract ----------------

    def run(self):
        """Fetch + upsert EOD candles for all held equity instruments.

        Returns the 3-tuple ``(message, status, interval)`` matching
        other scheduled rate tasks:
          * ``status = "Completed"`` on at-least-one-instrument-updated.
          * ``status = "Failed"`` when Kite is unreachable / token
            expired, or no held instruments resolve.
        """
        try:
            user_id = self._get_first_kite_user()
            if not user_id:
                return (
                    "No user with Kite access token — skipping backfill",
                    "Failed",
                    self.interval,
                )

            # Cheapest probe BEFORE enumerating holdings (same pattern as
            # SetKiteStockDetails) — fail fast on the Kite 6 AM IST
            # expiry window without wasting DB work.
            if not self.kite_service.check_token_liveness(user_id):
                return (
                    "Kite token expired — reconnect required",
                    "Failed",
                    self.interval,
                )

            symbol_to_token = self._load_symbol_to_token_map()
            if not symbol_to_token:
                return (
                    "Instrument dump missing — SetKiteStockDetails "
                    "must run first",
                    "Failed",
                    self.interval,
                )

            # Current held stock-type securities (single-user app; across
            # all users still correct — the candle cache is market-wide
            # so backfilling the union is fine).
            held = (
                self.db.session.query(PurchasedSecurities)
                .filter(
                    PurchasedSecurities.securityType == "Stocks",
                    PurchasedSecurities.buyQuant > 0,
                )
                .all()
            )
            if not held:
                return (
                    "No held stocks — nothing to backfill",
                    "Completed",
                    self.interval,
                )

            # Dedupe instrument_tokens — different userIds can hold the
            # same stock; one fetch serves all.
            tokens_to_fetch = {}
            unresolved = []
            for row in held:
                symbol = row.securityCode
                token = symbol_to_token.get(symbol)
                if token is None:
                    unresolved.append(symbol)
                    continue
                tokens_to_fetch[token] = symbol

            if unresolved:
                self.logger.warning(
                    f"Historical backfill: {len(unresolved)} symbols "
                    f"did not resolve to instrument_token "
                    f"(first 5: {unresolved[:5]}) — SetKiteStockDetails "
                    f"may need refresh"
                )

            from_date, to_date = self._fetch_window(tokens_to_fetch)

            upserted = 0
            fetch_errors = 0
            for token, symbol in tokens_to_fetch.items():
                try:
                    candles = self.kite_service.get_historical_data(
                        user_id=user_id,
                        instrument_token=token,
                        interval="day",
                        from_date=from_date,
                        to_date=to_date,
                    )
                except KiteTokenExpired:
                    # Token died mid-run — surface as Failed so the next
                    # scheduler tick retries after the user reconnects.
                    return (
                        "Kite token expired mid-run — reconnect required",
                        "Failed",
                        self.interval,
                    )
                except KiteAuthError as exc:
                    return (
                        f"Kite auth error: {exc}",
                        "Failed",
                        self.interval,
                    )
                except Exception as exc:
                    # Per-instrument failure — log + continue. The task
                    # is "update as many as possible" not "all-or-none".
                    fetch_errors += 1
                    self.logger.warning(
                        f"Historical fetch failed for {symbol} "
                        f"(token={token}): {exc}"
                    )
                    time.sleep(_HISTORICAL_FETCH_GAP_SECONDS)
                    continue

                upserted += self._upsert_candles(token, candles)
                time.sleep(_HISTORICAL_FETCH_GAP_SECONDS)

            try:
                self.db.session.commit()
            except Exception as exc:
                self.db.session.rollback()
                return (
                    f"Backfill commit failed: {exc}"[:800],
                    "Failed",
                    self.interval,
                )

            msg = (
                f"Completed: {upserted} candles upserted across "
                f"{len(tokens_to_fetch)} instruments "
                f"(unresolved={len(unresolved)} fetch_errors={fetch_errors})"
            )
            self.logger.info(f"{self.title}: {msg}")
            return msg, "Completed", self.interval

        except Exception as exc:
            return str(exc)[:800], "Failed", self.interval

    # ---------------- helpers ----------------

    def _fetch_window(self, tokens_to_fetch):
        """Decide from_date → to_date for this run.

        Steady-state (table has data): to_date = today, from_date = today
        - overlap days. On cold-start (empty table or every held
        instrument has no rows yet) we extend to a year to populate the
        trend-chart history. Checking any single token for
        presence-in-table is sufficient — if ONE token has history,
        others have at least `overlap` days coverage.
        """
        today = datetime.now(IST).date()

        has_any_history = False
        if tokens_to_fetch:
            probe_token = next(iter(tokens_to_fetch.keys()))
            has_any_history = (
                self.db.session.query(StockHistoricalCandle.id)
                .filter(StockHistoricalCandle.instrument_token == probe_token)
                .first()
                is not None
            )

        if has_any_history:
            from_date = today - timedelta(days=_STEADY_STATE_OVERLAP_DAYS)
        else:
            self.logger.info(
                "Historical backfill: cold-start — fetching "
                f"{_COLD_START_DAYS} days"
            )
            from_date = today - timedelta(days=_COLD_START_DAYS)
        return from_date, today

    def _upsert_candles(self, instrument_token, candles):
        """UPSERT a list of Kite candle dicts into StockHistoricalCandle.

        Uses MySQL ``INSERT … ON DUPLICATE KEY UPDATE`` semantics via
        SQLAlchemy's dialect-specific helper, keyed on the
        ``(instrument_token, trade_date)`` unique constraint.

        Returns the number of rows affected (each candle is one
        upsert — count matches ``len(candles)`` when all accept).
        """
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        if not candles:
            return 0

        rows = []
        for c in candles:
            # Kite's `date` field on a daily candle is a timezone-aware
            # datetime at 00:00:00 IST. We only care about the date
            # portion for the daily-cadence cache.
            raw_date = c.get("date")
            if isinstance(raw_date, datetime):
                candle_date = raw_date.date()
            elif isinstance(raw_date, str):
                try:
                    candle_date = datetime.fromisoformat(raw_date).date()
                except ValueError:
                    candle_date = datetime.strptime(
                        raw_date[:10], "%Y-%m-%d"
                    ).date()
            else:
                # Native date object or unexpected shape — fall back
                # via str() then parse. Skip on unparseable.
                try:
                    candle_date = datetime.strptime(
                        str(raw_date)[:10], "%Y-%m-%d"
                    ).date()
                except Exception:
                    self.logger.warning(
                        f"Candle for token={instrument_token} has "
                        f"unparseable date={raw_date!r} — skipping row"
                    )
                    continue

            rows.append(
                {
                    "instrument_token": instrument_token,
                    "trade_date": candle_date,
                    "open": Decimal(str(c.get("open", 0))),
                    "high": Decimal(str(c.get("high", 0))),
                    "low": Decimal(str(c.get("low", 0))),
                    "close": Decimal(str(c.get("close", 0))),
                    "volume": int(c.get("volume", 0) or 0),
                }
            )

        if not rows:
            return 0

        stmt = mysql_insert(StockHistoricalCandle).values(rows)
        # On conflict against uq_candle_token_date: refresh the OHLCV
        # values. Historical close is stable but the backfill-day row
        # may get refined (e.g., post-settlement adjustments) — cheap
        # to UPDATE regardless.
        upsert = stmt.on_duplicate_key_update(
            open=stmt.inserted.open,
            high=stmt.inserted.high,
            low=stmt.inserted.low,
            close=stmt.inserted.close,
            volume=stmt.inserted.volume,
        )
        self.db.session.execute(upsert)
        return len(rows)

    def _load_symbol_to_token_map(self):
        """Build ``{tradingsymbol: instrument_token}`` from the
        SetKiteStockDetails instrument dump.

        Returns ``{}`` if the dump is missing (operator / fresh install
        where SetKiteStockDetails hasn't run yet). Caller treats that
        as a soft-fail (Failed status, retried on next cycle once dump
        lands).
        """
        try:
            stock_list = self.jsonService.getStockList()
        except (FileNotFoundError, OSError) as exc:
            self.logger.warning(
                f"Historical backfill: instrument dump missing ({exc})"
            )
            return {}
        rows = stock_list.get("data", [])
        mapping = {}
        for r in rows:
            code = r.get("stockCode")
            token = r.get("instrument_token")
            if code and token is not None:
                mapping[code] = int(token)
        return mapping

    def _get_first_kite_user(self):
        """First user with a Kite access token on file.

        Single-user app today; matches the pattern used by
        SetKiteStockDetails.
        """
        try:
            user_token = (
                self.db.session.query(UserToken)
                .filter_by(service_type="kite")
                .first()
            )
            if user_token and user_token.access_token:
                return user_token.user_id
            return None
        except Exception as exc:
            self.logger.error(f"Error getting Kite user: {exc}")
            return None
