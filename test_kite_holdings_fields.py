"""Tests for the Kite holdings field expansion (ak-w4p Part B).

These pin the ACCOUNTING CONTRACT agreed with the frontend on ak-w4p /
ak-9we, not just the field names:

  * `quantity` (and the `buyQuant` derived from it) is SETTLED-ONLY and never
    includes `t1_quantity`. Kite uses the same split, and the UI combines the
    two for display while disclosing it.
  * T+1 market value is surfaced separately as `pending_t1_value` and must
    NOT enter currentValue / profit. Our cost basis is statement-derived and
    only covers settled shares, so valuing pending shares at market inside
    P&L would book their entire market value as profit -- a ~1L Friday buy
    would read as ~1L of phantom profit on Monday.
  * Kite's `average_price` is a cross-check hint only and must never
    overwrite our statement-derived `buyPrice`.

Coverage:
  1. All contract fields present at the row's top level, snake_case
  2. `quantity` stays settled-only (excludes t1_quantity)
  3. pending_t1_value == t1_quantity * last_price
  4. pending_t1_value is zero when nothing is pending
  5. Missing/None Kite fields degrade to 0 rather than blowing up
  6. MTF fields default to 0 when the account does not use MTF
  7. The DB sync writes settled-only quantity (no T+1 double-count)
  8. The DB sync does not invent rows for purely-pending holdings
  9. Kite `average_price` never overwrites our stored buyPrice
 10. An expired Kite session degrades the enrichment index to empty
"""
import sys
import types
from unittest.mock import MagicMock

import pytest

# Importing this first sets up (and tears down) the kiteconnect / Google-stack
# import stubs.
from test_kite_token_expiry import (  # noqa: E402
    _install_fake_kiteconnect,
    _install_fake_module,
    _install_import_stubs,
    _project_modules_loaded,
    _withdraw_stubs,
)

_modules_before = _project_modules_loaded()

# That module withdraws its own stubs on the way out, so re-install them here:
# kiteconnect + the Google stack for KiteService/BaseService, plus the NSE
# price-feed library and AI helper stack that StocksService pulls in. None of
# them are installed in CI or exercised here -- the Kite seam is stubbed
# directly on the service instance.
_install_fake_kiteconnect()
_install_import_stubs()
for _pkg in ("nsepython", "nsepython.rahu", "nsepythonserver", "nsepythonserver.rahu",
             "anyio", "claude_agent_sdk"):
    _install_fake_module(_pkg)

from services.KiteService import KiteTokenExpired  # noqa: E402
from services.StocksService import StocksService  # noqa: E402

# Withdraw the stubs again -- leaving them in sys.modules would make later
# test modules import mocks instead of skipping on the real absent packages.
_withdraw_stubs(_project_modules_loaded() - _modules_before)


def _kite_holding(**overrides):
    """A representative Kite /portfolio/holdings row."""
    holding = {
        "tradingsymbol": "INFY",
        "exchange": "NSE",
        "isin": "INE009A01021",
        "product": "CNC",
        "quantity": 10,          # settled
        "t1_quantity": 4,        # bought, not yet in demat
        "realised_quantity": 10,
        "authorised_quantity": 0,
        "collateral_quantity": 0,
        "collateral_type": None,
        "average_price": 1500.0,
        "last_price": 1600.0,
        "close_price": 1580.0,
        "pnl": 1000.0,
        "day_change": 20.0,
        "day_change_percentage": 1.2658,
    }
    holding.update(overrides)
    return holding


def _service(holdings=None, raises=None):
    """StocksService with only the Kite seam stubbed."""
    svc = StocksService.__new__(StocksService)
    svc.logger = MagicMock()
    svc.kite_service = MagicMock()
    if raises is not None:
        svc.kite_service.get_holdings.side_effect = raises
    else:
        svc.kite_service.get_holdings.return_value = holdings or []
    return svc


# ── 1: wire contract ───────────────────────────────────────────────────────

CONTRACT_FIELDS = (
    "t1_quantity",
    "realised_quantity",
    "authorised_quantity",
    "collateral_quantity",
    "close_price",
    "day_change",
    "day_change_percentage",
    "average_price",
    "last_price",
    "pending_t1_value",
)


@pytest.mark.parametrize("field", CONTRACT_FIELDS)
def test_contract_field_present_at_row_top_level(field):
    row = _service([_kite_holding()]).fetch_kite_holdings("user-1")[0]
    assert field in row, f"{field} missing from holdings row"
    assert "info" not in row, "fields must stay flat, not nested under `info`"


# ── 2-4: settled vs pending split ──────────────────────────────────────────

def test_quantity_is_settled_only_and_excludes_t1():
    """The core double-count guard. If `quantity` ever starts including
    t1_quantity, the UI -- which renders `quantity + t1_quantity` -- silently
    double-counts every pending share."""
    row = _service([_kite_holding(quantity=10, t1_quantity=4)]).fetch_kite_holdings("u")[0]

    assert row["quantity"] == 10
    assert row["t1_quantity"] == 4
    assert row["quantity"] != 14, "t1_quantity must not be folded into quantity"


def test_pending_t1_value_is_quantity_times_last_price():
    row = _service([_kite_holding(t1_quantity=4, last_price=1600.0)]).fetch_kite_holdings("u")[0]
    assert row["pending_t1_value"] == pytest.approx(6400.0)


def test_pending_t1_value_is_zero_when_nothing_pending():
    row = _service([_kite_holding(t1_quantity=0)]).fetch_kite_holdings("u")[0]
    assert row["pending_t1_value"] == 0


# ── 5-6: defensive coercion ────────────────────────────────────────────────

def test_absent_optional_fields_default_to_zero():
    """Older/pruned Kite payloads may omit these entirely; the row must still
    be renderable rather than carrying None into arithmetic."""
    bare = {
        "tradingsymbol": "INFY",
        "quantity": 5,
        "average_price": 100.0,
        "last_price": 110.0,
        "pnl": 50.0,
        "product": "CNC",
        "exchange": "NSE",
        "isin": "INE009A01021",
    }
    row = _service([bare]).fetch_kite_holdings("u")[0]

    assert row["t1_quantity"] == 0
    assert row["day_change"] == 0
    assert row["close_price"] == 0
    assert row["pending_t1_value"] == 0


def test_none_valued_fields_are_coerced_to_zero():
    row = _service([_kite_holding(t1_quantity=None, last_price=None,
                                  day_change=None)]).fetch_kite_holdings("u")[0]
    assert row["t1_quantity"] == 0
    assert row["last_price"] == 0
    assert row["day_change"] == 0
    assert row["pending_t1_value"] == 0


def test_mtf_fields_default_to_zero_without_mtf():
    row = _service([_kite_holding()]).fetch_kite_holdings("u")[0]
    assert row["mtf_quantity"] == 0
    assert row["mtf_average_price"] == 0


def test_mtf_fields_are_read_when_present():
    row = _service([_kite_holding(mtf={"quantity": 3, "average_price": 1490.0})]) \
        .fetch_kite_holdings("u")[0]
    assert row["mtf_quantity"] == 3
    assert row["mtf_average_price"] == pytest.approx(1490.0)


# ── 7-9: the DB sync must not absorb pending shares ────────────────────────

def _sync_service(holdings, existing=None):
    """StocksService wired for sync_kite_holdings_to_db with a stub session."""
    svc = _service(holdings)
    svc.genericUtil = MagicMock()
    svc.genericUtil.generate_custom_buyID.return_value = "BUY123"
    svc.dateTimeUtil = MagicMock()
    svc.dateTimeUtil.getCurrentDatetimeSqlFormat.return_value = "2026-09-04 10:00:00"

    query = MagicMock()
    query.filter.return_value.first.return_value = existing
    session = MagicMock()
    session.query.return_value = query
    svc._stub_db = types.SimpleNamespace(session=session)
    type(svc).db = property(lambda self: self._stub_db)
    svc._session = session
    return svc


def test_sync_persists_settled_quantity_only():
    """`buyQuant` feeds the invested/profit maths, so writing settled+T+1 here
    would inflate cost basis against shares we may not even own yet."""
    svc = _sync_service([_kite_holding(quantity=10, t1_quantity=4)])
    svc.sync_kite_holdings_to_db("user-1")

    added = svc._session.add.call_args[0][0]
    assert added.buyQuant == 10, "T+1 shares must not be persisted as settled"


def test_sync_skips_holdings_that_are_entirely_pending():
    """A holding that is 100% T+1 has quantity 0 and must not create a row --
    the statement that establishes its real cost basis has not arrived yet."""
    svc = _sync_service([_kite_holding(quantity=0, t1_quantity=7)])
    result = svc.sync_kite_holdings_to_db("user-1")

    svc._session.add.assert_not_called()
    assert result["synced"] == 0


def test_sync_does_not_let_kite_average_price_rewrite_matching_rows():
    """Our buyPrice is statement-derived and authoritative. Kite's
    average_price is a cross-check hint only; when quantities already agree
    the stored basis must be left completely untouched."""
    existing = types.SimpleNamespace(buyQuant=10, buyPrice=1234.56)
    svc = _sync_service([_kite_holding(quantity=10, average_price=1500.0)],
                        existing=existing)

    svc.sync_kite_holdings_to_db("user-1")

    assert existing.buyPrice == 1234.56, "statement-derived basis was overwritten"


# ── 10: enrichment degrades instead of failing ─────────────────────────────

def test_expired_kite_session_yields_empty_enrichment_index():
    """The securities list is otherwise built from our own DB + the NSE price
    feed, so a dead Kite token must never take the whole endpoint down."""
    svc = _service(raises=KiteTokenExpired("expired"))

    index, status = svc.kite_holdings_index("user-1")

    assert index == {}
    assert status["reconnect_required"] is True


def test_unexpected_kite_error_also_degrades_quietly():
    svc = _service(raises=RuntimeError("connection reset"))

    index, status = svc.kite_holdings_index("user-1")

    assert index == {}
    assert status["connected"] is False
    # Not an auth problem, so the UI should not nag the user to reconnect.
    assert status["reconnect_required"] is False


def test_enrichment_index_is_keyed_by_trading_symbol():
    svc = _service([_kite_holding(tradingsymbol="INFY"),
                    _kite_holding(tradingsymbol="TCS")])

    index, status = svc.kite_holdings_index("user-1")

    assert set(index) == {"INFY", "TCS"}
    assert status["connected"] is True
