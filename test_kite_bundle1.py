"""Tests for ak-4o41 Kite Bundle 1 (historical candles + live OHLC enrichment).

Covers the four Phase-A/C/D surfaces where test discipline carries
value without needing a real DB or a live Kite client:

  * KiteService.get_historical_data / get_ltp / get_ohlc / get_quote
    — wrapper passthrough + token-expiry propagation (mocked kite SDK).
  * StocksService._fetch_today_open_batch — success + all failure
    classes degrade to empty-dict so the fallback path preserves
    ak-w4p's no-500-on-Kite-failure invariant.
  * StocksService.fetch_kite_holdings — day_change_amount uses today's
    open when /ohlc succeeds, falls back to close_price when it fails.

Phase-B (BackfillHistoricalCandlesTask) + Phase-C (getPortfolioHistory
aggregation) exercise the real DB (MySQL ``ON DUPLICATE KEY UPDATE``
upsert + multi-table joins). Those are covered by an operator-run
manual test plan after mayor merge (per Lead's "standard review, no
Fable" dispatch — no money-math, read-only data).

## Mutation-test discipline (rig-std from ak-lvu arc)

Pre-fold (KiteService wrappers absent + StocksService._fetch_today_open_batch
absent): every test errors on AttributeError / ImportError. Post-fold:
full pass. Same-binary-opposite-outcome discipline.
"""
import sys
import types
from unittest.mock import MagicMock

import pytest

from test_kite_token_expiry import (  # noqa: E402
    _install_fake_kiteconnect,
    _install_fake_module,
    _install_import_stubs,
    _project_modules_loaded,
    _withdraw_stubs,
)

_modules_before = _project_modules_loaded()

_install_fake_kiteconnect()
_install_import_stubs()
for _pkg in (
    "nsepython",
    "nsepython.rahu",
    "nsepythonserver",
    "nsepythonserver.rahu",
    "anyio",
    "claude_agent_sdk",
    "aiohttp",
):
    _install_fake_module(_pkg)

from services.KiteService import (  # noqa: E402
    KiteAuthError,
    KiteService,
    KiteTokenExpired,
)
from services.StocksService import StocksService  # noqa: E402
# Capture kite_exceptions.TokenException via the already-imported
# module (not a fresh `from services.KiteService import ...` at test
# time — that re-triggers the kiteconnect import chain after stubs
# are withdrawn and fails on ModuleNotFoundError).
import services.KiteService as _kite_module  # noqa: E402
_FakeTokenException = _kite_module.kite_exceptions.TokenException

_withdraw_stubs(_project_modules_loaded() - _modules_before)


# ── KiteService wrapper passthrough (Phase A) ────────────────────────────

def _kite_service_with_stub(kite_stub):
    """Build a KiteService with the SDK client + auth path stubbed."""
    svc = KiteService.__new__(KiteService)
    svc.logger = MagicMock()
    svc._authed_kite = MagicMock(return_value=kite_stub)
    svc._mark_token_expired = MagicMock()
    return svc


def test_get_historical_data_passes_through_sdk_call_with_correct_args():
    """The wrapper must call ``kite.historical_data`` with keyword args
    matching Kite's SDK shape; a positional-arg or renamed-key regression
    would silently break the backfill task.
    """
    kite_stub = MagicMock()
    kite_stub.historical_data.return_value = [
        {"date": "2026-11-10", "open": 1, "high": 2, "low": 0, "close": 1, "volume": 100}
    ]
    svc = _kite_service_with_stub(kite_stub)

    result = svc.get_historical_data(
        user_id="u1",
        instrument_token=12345,
        interval="day",
        from_date="2026-10-01",
        to_date="2026-11-10",
    )

    kite_stub.historical_data.assert_called_once_with(
        instrument_token=12345,
        from_date="2026-10-01",
        to_date="2026-11-10",
        interval="day",
        continuous=False,
        oi=False,
    )
    assert len(result) == 1


def test_get_historical_data_propagates_token_expired():
    """Token-rejection mid-fetch must surface as KiteTokenExpired (not
    as a bare exception), matching every other wrapper's semantic.
    """
    kite_stub = MagicMock()
    kite_stub.historical_data.side_effect = _FakeTokenException(
        "token rejected"
    )
    svc = _kite_service_with_stub(kite_stub)

    with pytest.raises(KiteTokenExpired):
        svc.get_historical_data(
            user_id="u1",
            instrument_token=1,
            interval="day",
            from_date="2026-01-01",
            to_date="2026-01-02",
        )
    # Expired-token side effect — _mark_token_expired called via _call.
    svc._mark_token_expired.assert_called_once_with("u1")


def test_get_ltp_passes_batch_instruments_to_sdk():
    kite_stub = MagicMock()
    kite_stub.ltp.return_value = {"NSE:INFY": {"instrument_token": 1, "last_price": 1600}}
    svc = _kite_service_with_stub(kite_stub)

    result = svc.get_ltp(user_id="u1", instruments=["NSE:INFY", "NSE:TCS"])

    kite_stub.ltp.assert_called_once_with(["NSE:INFY", "NSE:TCS"])
    assert result == {"NSE:INFY": {"instrument_token": 1, "last_price": 1600}}


def test_get_ohlc_passes_batch_instruments_to_sdk():
    kite_stub = MagicMock()
    kite_stub.ohlc.return_value = {
        12345: {
            "instrument_token": 12345,
            "last_price": 1600,
            "ohlc": {"open": 1580, "high": 1610, "low": 1575, "close": 1585},
        }
    }
    svc = _kite_service_with_stub(kite_stub)

    result = svc.get_ohlc(user_id="u1", instruments=[12345])

    kite_stub.ohlc.assert_called_once_with([12345])
    assert result[12345]["ohlc"]["open"] == 1580


def test_get_quote_passes_batch_instruments_to_sdk():
    kite_stub = MagicMock()
    kite_stub.quote.return_value = {"NSE:INFY": {"depth": {}, "last_price": 1600}}
    svc = _kite_service_with_stub(kite_stub)

    result = svc.get_quote(user_id="u1", instruments=["NSE:INFY"])

    kite_stub.quote.assert_called_once_with(["NSE:INFY"])
    assert "NSE:INFY" in result


def test_ohlc_propagates_token_expired():
    """Same expired-token semantic as get_historical_data — regression
    guard against a later refactor that forgets to route through
    _call().
    """
    kite_stub = MagicMock()
    kite_stub.ohlc.side_effect = _FakeTokenException("expired")
    svc = _kite_service_with_stub(kite_stub)

    with pytest.raises(KiteTokenExpired):
        svc.get_ohlc(user_id="u1", instruments=[1])


# ── StocksService._fetch_today_open_batch (Phase D) ──────────────────────

def _stocks_service():
    """StocksService with only the Kite seam stubbed."""
    svc = StocksService.__new__(StocksService)
    svc.logger = MagicMock()
    svc.kite_service = MagicMock()
    return svc


def test_fetch_today_open_batch_returns_token_to_open_map_on_success():
    svc = _stocks_service()
    svc.kite_service.get_ohlc.return_value = {
        12345: {
            "instrument_token": 12345,
            "last_price": 1600,
            "ohlc": {"open": 1580, "high": 1610, "low": 1575, "close": 1585},
        },
        67890: {
            "instrument_token": 67890,
            "last_price": 300,
            "ohlc": {"open": 295, "high": 305, "low": 294, "close": 298},
        },
    }

    result = svc._fetch_today_open_batch(
        "u1", [{"instrument_token": 12345}, {"instrument_token": 67890}]
    )

    assert result == {12345: 1580.0, 67890: 295.0}
    svc.kite_service.get_ohlc.assert_called_once_with("u1", [12345, 67890])


def test_fetch_today_open_batch_empty_holdings_returns_empty_without_calling_kite():
    svc = _stocks_service()
    result = svc._fetch_today_open_batch("u1", [])
    assert result == {}
    svc.kite_service.get_ohlc.assert_not_called()


def test_fetch_today_open_batch_degrades_to_empty_on_auth_failure():
    """Rig CRITICAL: Kite auth failure MUST NOT 500 the enrichment path.
    Returns empty dict so caller falls back to close_price-basis math.
    """
    svc = _stocks_service()
    svc.kite_service.get_ohlc.side_effect = KiteTokenExpired("token expired")

    result = svc._fetch_today_open_batch("u1", [{"instrument_token": 1}])
    assert result == {}


def test_fetch_today_open_batch_degrades_to_empty_on_unexpected_error():
    svc = _stocks_service()
    svc.kite_service.get_ohlc.side_effect = RuntimeError("connection reset")

    result = svc._fetch_today_open_batch("u1", [{"instrument_token": 1}])
    assert result == {}


def test_fetch_today_open_batch_skips_rows_with_missing_token():
    svc = _stocks_service()
    svc.kite_service.get_ohlc.return_value = {}
    svc._fetch_today_open_batch(
        "u1", [{"instrument_token": None}, {"tradingsymbol": "INFY"}]
    )
    # Empty valid-token list → no Kite call
    svc.kite_service.get_ohlc.assert_not_called()


def test_fetch_today_open_batch_handles_malformed_ohlc_response():
    """Kite is known to emit incomplete payloads on stripped-down
    responses. Each malformed entry gets silently skipped; valid
    entries still returned.
    """
    svc = _stocks_service()
    svc.kite_service.get_ohlc.return_value = {
        1: {"instrument_token": 1, "ohlc": {"open": 100}},          # valid
        2: "not a dict",                                             # skip
        3: {"instrument_token": 3, "ohlc": None},                    # skip
        4: {"instrument_token": 4, "ohlc": {"open": None}},          # skip
        5: {"instrument_token": 5, "ohlc": {"open": "not-a-number"}}  # skip
    }

    result = svc._fetch_today_open_batch(
        "u1",
        [{"instrument_token": i} for i in (1, 2, 3, 4, 5)],
    )
    assert result == {1: 100.0}


# ── fetch_kite_holdings integration (Phase D) ────────────────────────────

def _holding(**overrides):
    """ak-yz9c reference fixture, extended with ``instrument_token`` for
    the Bundle-1 OHLC refinement.
    """
    base = {
        "tradingsymbol": "ANANTRAJ",
        "instrument_token": 12345,
        "exchange": "NSE",
        "isin": "INE242C01024",
        "product": "CNC",
        "quantity": 250,
        "t1_quantity": 201,
        "realised_quantity": 250,
        "authorised_quantity": 0,
        "collateral_quantity": 0,
        "average_price": 1500.0,
        "last_price": 1600.0,
        "close_price": 1585.0,  # yesterday's close
        "pnl": 250_000.0,
        "day_change": 15.0,
        "day_change_percentage": 0.95,
    }
    base.update(overrides)
    return base


def test_fetch_kite_holdings_uses_today_open_when_ohlc_succeeds():
    """THE Lead-flagged semantic: when /ohlc succeeds, day_change_amount
    uses today's `open` as the basis — NOT yesterday's close_price.

    Fixture: holding has close_price=1585 (yesterday) + /ohlc returns
    today_open=1580. last_price=1600. total_qty=451.
      correct (post-fold):     451 × (1600 − 1580) = 9020
      wrong   (pre-fold shape): 451 × (1600 − 1585) = 6765

    Mutation discriminator: a swap to the wrong basis gives 6765, not 9020.
    """
    svc = _stocks_service()
    svc.kite_service.get_holdings.return_value = [_holding()]
    svc.kite_service.get_ohlc.return_value = {
        12345: {
            "instrument_token": 12345,
            "ohlc": {"open": 1580, "high": 1610, "low": 1575, "close": 1585},
        }
    }

    rows = svc.fetch_kite_holdings("u1")

    assert len(rows) == 1
    assert rows[0]["today_open"] == 1580.0
    assert rows[0]["day_change_amount"] == pytest.approx(9020.0)
    # Negative assertion: must NOT fall back to the close_price-basis math
    assert rows[0]["day_change_amount"] != pytest.approx(6765.0)


def test_fetch_kite_holdings_falls_back_to_close_price_when_ohlc_fails():
    """Graceful degradation: if /ohlc returns {} (auth expired / network
    error), day_change_amount reverts to the ak-w4p pre-ak-4o41 math
    using close_price. No 500, enrichment still ships.
    """
    svc = _stocks_service()
    svc.kite_service.get_holdings.return_value = [_holding()]
    svc.kite_service.get_ohlc.side_effect = KiteTokenExpired("expired")

    rows = svc.fetch_kite_holdings("u1")

    assert rows[0]["today_open"] is None
    # Fallback basis: close_price (1585). 451 × (1600 − 1585) = 6765.
    assert rows[0]["day_change_amount"] == pytest.approx(6765.0)


def test_fetch_kite_holdings_mixed_ohlc_coverage_uses_per_token_fallback():
    """Partial OHLC coverage: one token in the batch has OHLC, another
    doesn't. Each token's day_change_amount independently uses the best
    available basis.
    """
    svc = _stocks_service()
    svc.kite_service.get_holdings.return_value = [
        _holding(tradingsymbol="A", instrument_token=1),
        _holding(tradingsymbol="B", instrument_token=2),
    ]
    # Only token 1 has /ohlc data
    svc.kite_service.get_ohlc.return_value = {
        1: {"instrument_token": 1, "ohlc": {"open": 1580}},
    }

    rows = svc.fetch_kite_holdings("u1")
    by_sym = {r["symbol"]: r for r in rows}

    # Token 1: uses today_open=1580
    assert by_sym["A"]["today_open"] == 1580.0
    assert by_sym["A"]["day_change_amount"] == pytest.approx(9020.0)
    # Token 2: no OHLC — falls back to close_price=1585
    assert by_sym["B"]["today_open"] is None
    assert by_sym["B"]["day_change_amount"] == pytest.approx(6765.0)


def test_fetch_kite_holdings_preserves_all_ak_yz9c_folded_fields():
    """Regression guard: adding the Bundle-1 OHLC refinement must not
    disturb the ak-yz9c folded fields (settled_qty, t1_qty, total_qty,
    invested, current_value, unrealized_pnl). Spot-check each on the
    reference fixture.
    """
    svc = _stocks_service()
    svc.kite_service.get_holdings.return_value = [_holding()]
    svc.kite_service.get_ohlc.return_value = {}  # fallback path

    row = svc.fetch_kite_holdings("u1")[0]

    assert row["settled_qty"] == 250
    assert row["t1_qty"] == 201
    assert row["total_qty"] == 451
    assert row["invested"] == pytest.approx(676_500.0)
    assert row["current_value"] == pytest.approx(721_600.0)
    assert row["unrealized_pnl"] == pytest.approx(45_100.0)


def test_KITE_ROW_FIELDS_includes_today_open_for_enrichment_overlay():
    """The ``today_open`` field must be overlaid onto enriched
    securities rows via InvestmentService.fetchUserSecurities' loop
    over ``KITE_ROW_FIELDS``. Without this inclusion, the FE wouldn't
    see today_open on the per-row shape.
    """
    assert "today_open" in StocksService.KITE_ROW_FIELDS


# ── ak-qeex: fail-closed poison-basis guard (reviewer MAJOR) ─────────────

def test_day_change_amount_fail_closed_on_today_open_zero():
    """ak-qeex MAJOR: Kite OHLC can emit ``today_open=0`` on pre-open
    ticks. The ak-4o41 initial resolve used ``today_open if today_open
    is not None else close_price``, which passed ``0`` through as the
    basis (0 ≠ None). Resulting day_change_amount =
    qty × (last_price − 0) = qty × last_price = the full market value
    masquerading as "today's change" (order-of-magnitude error on the
    summary card).

    Post-fix: basis==0 triggers fail-closed → day_change_amount = 0
    ("unknown day change"), NOT the poison value.

    Fixture: qty=451, last_price=1600. Poison path: 451 × 1600 =
    721_600. Fail-closed path: 0.
    """
    svc = _stocks_service()
    svc.kite_service.get_holdings.return_value = [_holding()]
    svc.kite_service.get_ohlc.return_value = {
        12345: {
            "instrument_token": 12345,
            # Kite OHLC emits 0 on pre-open ticks
            "ohlc": {"open": 0, "high": 0, "low": 0, "close": 1585},
        }
    }

    row = svc.fetch_kite_holdings("u1")[0]

    assert row["today_open"] == 0.0
    # Fail-closed: 0, NOT the poison value 721_600
    assert row["day_change_amount"] == 0.0
    assert row["day_change_amount"] != pytest.approx(721_600.0)


def test_day_change_amount_fail_closed_on_both_bases_missing():
    """Cold-sync edge: both ``today_open`` and ``close_price`` absent /
    0. Earlier code would compute ``qty × last_price`` as "day change"
    (poison). Fail-closed guard returns 0 instead.
    """
    svc = _stocks_service()
    svc.kite_service.get_holdings.return_value = [
        _holding(close_price=0)  # no yesterday close either
    ]
    svc.kite_service.get_ohlc.return_value = {}  # no OHLC

    row = svc.fetch_kite_holdings("u1")[0]

    assert row["today_open"] is None
    assert row["day_change_amount"] == 0.0
    # Negative: poison value would have been 451 × 1600 = 721_600
    assert row["day_change_amount"] != pytest.approx(721_600.0)


def test_day_change_amount_still_computed_when_basis_present():
    """Regression guard on the fix: positive today_open or positive
    close_price must STILL produce the correct day-change math (the
    guard must only catch poison, not every path).
    """
    svc = _stocks_service()
    # (a) today_open positive → used
    svc.kite_service.get_holdings.return_value = [_holding()]
    svc.kite_service.get_ohlc.return_value = {
        12345: {"instrument_token": 12345, "ohlc": {"open": 1580}}
    }
    row = svc.fetch_kite_holdings("u1")[0]
    assert row["day_change_amount"] == pytest.approx(9020.0)

    # Reset singleton for the second scenario (cls._instance is sticky)
    StocksService._instance = None
    # (b) today_open missing, close_price positive → close_price used
    svc2 = _stocks_service()
    svc2.kite_service.get_holdings.return_value = [_holding()]
    svc2.kite_service.get_ohlc.return_value = {}
    row2 = svc2.fetch_kite_holdings("u1")[0]
    assert row2["day_change_amount"] == pytest.approx(6765.0)


# ── ak-qeex MINOR-2: Phase-C aggregation + coverage-omission ────────────

def test_aggregate_portfolio_history_basic_sum_per_date():
    """THE Phase-C aggregation core: for each date, sum qty × close
    across all tokens that have a candle for that date.

    Fixture: 2 tokens (100 qty + 50 qty). 3 trading days. Both tokens
    report every day. Expected per-date totals = full Σ(qty × close).
    """
    from datetime import date
    from decimal import Decimal
    from services.portfolio_history_aggregator import aggregate_portfolio_history

    token_qty = {
        111: Decimal("100"),
        222: Decimal("50"),
    }
    candles = [
        # (token, date, close)
        (111, date(2026, 11, 10), Decimal("1600")),
        (222, date(2026, 11, 10), Decimal("300")),
        (111, date(2026, 11, 11), Decimal("1620")),
        (222, date(2026, 11, 11), Decimal("305")),
        (111, date(2026, 11, 12), Decimal("1630")),
        (222, date(2026, 11, 12), Decimal("310")),
    ]

    result = aggregate_portfolio_history(token_qty, candles)

    assert len(result) == 3
    assert result[0] == {"date": "2026-11-10", "total_value": 100 * 1600 + 50 * 300}
    assert result[1] == {"date": "2026-11-11", "total_value": 100 * 1620 + 50 * 305}
    assert result[2] == {"date": "2026-11-12", "total_value": 100 * 1630 + 50 * 310}


def test_aggregate_portfolio_history_omits_incomplete_coverage_dates():
    """THE coverage-omission invariant: a date where ONE held token
    has no candle is DROPPED entirely — no phantom dip from the
    missing instrument's zero contribution.

    Fixture: 2 tokens held. Day 1 and Day 3 have both candles; Day 2
    only token 111 reports. Expected output: Day 1 and Day 3 only.
    If the fold had included Day 2 with just token 111's contribution
    (100 × 1620 = 162_000), it would appear as a sharp dip vs Day 1's
    155_000 and Day 3's 178_000.

    Mutation discriminator: a naive aggregator that emits every seen
    date returns 3 rows with Day 2 total_value = 162_000; the correct
    output is 2 rows (Day 2 OMITTED).
    """
    from datetime import date
    from decimal import Decimal
    from services.portfolio_history_aggregator import aggregate_portfolio_history

    token_qty = {
        111: Decimal("100"),
        222: Decimal("50"),
    }
    candles = [
        (111, date(2026, 11, 10), Decimal("1600")),
        (222, date(2026, 11, 10), Decimal("300")),
        # Day 2: token 111 reports, token 222 is MISSING
        (111, date(2026, 11, 11), Decimal("1620")),
        (111, date(2026, 11, 12), Decimal("1630")),
        (222, date(2026, 11, 12), Decimal("310")),
    ]

    result = aggregate_portfolio_history(token_qty, candles)

    # Day 2 OMITTED (incomplete coverage)
    assert len(result) == 2
    dates = [r["date"] for r in result]
    assert "2026-11-11" not in dates
    assert dates == ["2026-11-10", "2026-11-12"]
    # Total values unaffected
    assert result[0]["total_value"] == 100 * 1600 + 50 * 300
    assert result[1]["total_value"] == 100 * 1630 + 50 * 310


def test_aggregate_portfolio_history_empty_token_qty_returns_empty():
    """Pre-first-holding edge: no held stocks → ``[]`` regardless of
    candle content. Avoids division / shape errors in a cold-start
    portfolio.
    """
    from services.portfolio_history_aggregator import aggregate_portfolio_history
    assert aggregate_portfolio_history({}, []) == []


def test_aggregate_portfolio_history_output_sorted_ascending():
    """The FE renders a line chart left-to-right in chronological
    order. The aggregator must emit entries sorted ascending by date
    so FE doesn't have to resort.
    """
    from datetime import date
    from decimal import Decimal
    from services.portfolio_history_aggregator import aggregate_portfolio_history

    token_qty = {1: Decimal("1")}
    # Deliberately out-of-order input
    candles = [
        (1, date(2026, 11, 12), Decimal("103")),
        (1, date(2026, 11, 10), Decimal("101")),
        (1, date(2026, 11, 11), Decimal("102")),
    ]

    result = aggregate_portfolio_history(token_qty, candles)
    dates = [r["date"] for r in result]
    assert dates == ["2026-11-10", "2026-11-11", "2026-11-12"]


def test_aggregate_portfolio_history_skips_unexpected_tokens():
    """Defense-in-depth: if the candle query returns a token NOT in
    ``token_qty`` (would require a buggy .in_() filter widening), the
    aggregator skips it rather than KeyError'ing.
    """
    from datetime import date
    from decimal import Decimal
    from services.portfolio_history_aggregator import aggregate_portfolio_history

    token_qty = {111: Decimal("100")}
    candles = [
        (111, date(2026, 11, 10), Decimal("1600")),
        (999, date(2026, 11, 10), Decimal("500")),  # un-held token
    ]

    result = aggregate_portfolio_history(token_qty, candles)
    # Only token 111's contribution — 999 silently skipped
    assert result == [{"date": "2026-11-10", "total_value": 100 * 1600}]


def test_aggregate_portfolio_history_fractional_quantity():
    """Decimal quantities (e.g. a MF-ish 12.5 units; not typical for
    Indian equity but the type permits it) must be preserved through
    the Σ(qty × close) math without float-precision drift.
    """
    from datetime import date
    from decimal import Decimal
    from services.portfolio_history_aggregator import aggregate_portfolio_history

    token_qty = {1: Decimal("12.5")}
    candles = [(1, date(2026, 11, 10), Decimal("100.37"))]

    result = aggregate_portfolio_history(token_qty, candles)
    # 12.5 × 100.37 = 1254.625 — exact Decimal math; float coerce
    # preserves it bit-for-bit at this magnitude.
    assert result[0]["total_value"] == 1254.625
