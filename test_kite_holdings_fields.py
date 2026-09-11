"""Tests for the Kite holdings field expansion — ak-yz9c fold contract.

## Policy-chain history (why the pre-2026-09-11 invariants were inverted)

The prior pin (ak-w4p, ak-9we, Aug 2026) had this contract:
  * `quantity` was SETTLED-ONLY and never included `t1_quantity`.
  * `pending_t1_value` was surfaced separately and had to NOT enter
    currentValue/profit.
  * Kite's `average_price` was a cross-check hint only and must never
    overwrite our statement-derived `buyPrice`.

Rationale at that time: "valuing pending shares at market inside P&L would
book their entire market value as profit -- a ~1L Friday buy would read
as ~1L of phantom profit on Monday." That framing was correct under a
statement-authoritative cost basis where we didn't have a T+1 cost.

That framing was superseded by:
  * **2026-09-04 flatten-on-sync policy** — Kite's `average_price` is
    broker-canonical for Kite-enriched views; DB `buyPrice` remains
    statement-authoritative for statement-derived surfaces.
  * **2026-09-11 Overseer directive (ak-yz9c)** — T+1 shares are treated
    as **owned** for accounting: fold into per-holding qty/avg/cost AND
    into portfolio Invested / TotalAssetValue / Change / %Change / day P&L.
    Chip stays as visual "not yet in demat" indicator. `Pending (T+1)`
    summary column removed entirely.
  * Overseer Q1 confirm: "Remove [Pending T+1 summary col] entirely — once
    T+1 folds into Invested + Total Asset Value + per-holding cost/avg,
    the separate summary line becomes redundant. Chip is the only T+1
    indicator needed."
  * Overseer Q2 confirm: "Yes, include [T+1 in day P&L] — consistent with
    'treat T+1 as bought.' If cost basis includes T+1, day P&L on those
    shares should too. Otherwise top-line Change wouldn't reconcile with
    summed per-holding P&Ls."

The pre-fold pin was correct under the prior policy. This pin overturns
it under the new policy. Both pins served the accounting-correctness
CRITICAL constraint — different framings, both truthful in their era.

## Wire-naming decision (Option (i), agreed with akkountant_frontend 2026-09-11)

Kite's raw `quantity` field on `fetch_kite_holdings` output KEEPS its
pre-fold semantic (settled shares only, matches Kite's own SDK
semantic). Adding a NEW field `total_qty` for the blended value avoids
the double-count trap where FE code doing `quantity + t1_quantity`
would silently double the T+1 shares post-semantic-flip. Wire changes
are ADDITIVE + one deletion (`pending_t1_value`).

## Kite `average_price` semantic — verified empirically

Kite's `holdings.average_price` is the broker-canonical WEIGHTED AVG for
the NET position (settled + T+1 combined). Verified via:
  1. Kite Connect docs (kite.trade/docs/connect/v3/portfolio/):
     "Average price at which the net holding is bought"
  2. Real behavior on T+2 settlement: `average_price` is UNCHANGED — T+1
     shares just reclassify from `t1_quantity` bucket into `quantity`
     bucket with no numeric change to `average_price`. This proves the
     value was already blended pre-settlement.

No separate `t1_average_price` field exists in the Kite response. Under
Path A (Kite-canonical for the fold), we use `average_price` directly.

(Rig-standard #7 banked from ak-yz9c: contract-verification discipline
for external systems — verify via BOTH docs AND observed real behavior.
Neither signal alone is sufficient.)

## The new contract (post-2026-09-11)

Per-holding response row (`fetch_kite_holdings` and the fields
overlaid via `StocksService.KITE_ROW_FIELDS`):

  * `settled_qty` — Kite's `quantity` field (settled shares only). Under
                    Option (i) naming, `settled_qty` is the explicit
                    Kite-view settled count; the Kite `quantity` field
                    keeps its own semantic on the raw response.
  * `t1_qty` — renamed from Kite's `t1_quantity` for symmetry with
               `settled_qty`. Bought-but-unsettled shares (T+1 window).
  * `total_qty` — settled_qty + t1_qty (BLENDED total). The fold
                  discriminator field: FE reads this for display
                  quantity in Row 1/2 summary.
  * `average_price` — Kite's broker-canonical blended avg (unchanged
                      value; authority upgraded from "hint" to canonical
                      per 2026-09-04 policy).
  * `invested` — total_qty × average_price (folded cost basis).
  * `current_value` — total_qty × last_price.
  * `unrealized_pnl` — current_value − invested.
  * `day_change_amount` — total_qty × (last_price − close_price)
                          (folds T+1 in per Overseer Q2).
  * `pending_t1_value` — REMOVED (Overseer Q1).

FE derives portfolio-level aggregates by summing per-row emitted values
(agreed with akkountant_frontend 2026-09-11: no separate portfolio
endpoint — sum-of-visible-rows is drift-proof by construction).

## Test coverage

Fold-math (the ak-yz9c core, mutation-test discriminated):
  1. Additive granularity fields (`settled_qty`, `t1_qty`) present
  2. `total_qty` is BLENDED (settled + t1)
  3. `invested` = total_qty × average_price
  4. `current_value` = total_qty × last_price
  5. `unrealized_pnl` = current_value − invested
  6. `day_change_amount` = total_qty × (last_price − close_price)
  7. Fold math holds at t1_qty=0 (settled-only degenerate)
  8. Fold math holds at settled_qty=0 (pure T+1 degenerate)
  9. Fold math holds at settled_qty=0 AND t1_qty=0 (empty position)
 10. `pending_t1_value` is absent from every row
 11. All numeric fields are defined and finite even when Kite omits values
 12. Portfolio-level sums = row-sums by construction (FE agg semantic)

Preserved invariants (unchanged from ak-w4p):
 13. MTF fields default to 0 when unused; read when present
 14. Missing/None Kite fields degrade to 0 (no None-into-arithmetic)
 15. The DB sync (`sync_kite_holdings_to_db`) still writes settled-only
     `buyQuant` — statement-DB is not authoritative for Kite-view, but
     the statement pipeline itself must not double-count T+1 into the
     `PurchasedSecurities` table
 16. The DB sync skips purely-pending holdings (no synthetic buy row
     for T+1-only positions)
 17. The DB sync does not let Kite's `average_price` overwrite an
     existing DB `buyPrice` — the DB row is the statement-derived
     record, distinct from Kite's view
 18. Enrichment index degrades to empty on Kite auth failure
 19. Enrichment index degrades to empty on unexpected Kite error
 20. Enrichment index is keyed by trading symbol
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
    """A representative Kite /portfolio/holdings row.

    Defaults use the ak-yz9c reference discriminator fixture:
    settled=250, t1=201, avg=1500, last=1600, close=1580.
    Blended totals:
      total_qty = 451
      invested = 451 × 1500 = 676_500
      current_value = 451 × 1600 = 721_600
      unrealized_pnl = 721_600 − 676_500 = 45_100
      day_change_amount = 451 × (1600 − 1580) = 9_020
    Overrides any field for negative / edge-case cases.
    """
    holding = {
        "tradingsymbol": "ANANTRAJ",
        "exchange": "NSE",
        "isin": "INE242C01024",
        "product": "CNC",
        "quantity": 250,           # settled (Kite raw field)
        "t1_quantity": 201,        # bought, not yet in demat (Kite raw field)
        "realised_quantity": 250,
        "authorised_quantity": 0,
        "collateral_quantity": 0,
        "collateral_type": None,
        "average_price": 1500.0,   # Kite blended broker-canonical
        "last_price": 1600.0,
        "close_price": 1580.0,
        "pnl": 250_000.0,          # Kite's own (settled-only, ignored post-fold)
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


# ── 1: additive fields present ─────────────────────────────────────────────

CONTRACT_FIELDS_POST_FOLD = (
    # Additive granularity — the fold-in-explicit fields
    "settled_qty",
    "t1_qty",
    "total_qty",
    # Prices (Kite broker-canonical)
    "average_price",
    "last_price",
    "close_price",
    # Folded money math
    "invested",
    "current_value",
    "unrealized_pnl",
    "day_change_amount",
    # Rate (unchanged)
    "day_change_percentage",
    # Preserved from pre-fold
    "realised_quantity",
    "authorised_quantity",
    "collateral_quantity",
    "mtf_quantity",
    "mtf_average_price",
)


@pytest.mark.parametrize("field", CONTRACT_FIELDS_POST_FOLD)
def test_contract_field_present_at_row_top_level(field):
    row = _service([_kite_holding()]).fetch_kite_holdings("u")[0]
    assert field in row, f"{field} missing from post-fold holdings row"
    assert "info" not in row, "fields must stay flat, not nested under `info`"


def test_removed_pending_t1_value_field_is_absent():
    """Overseer Q1 (2026-09-11): the Pending (T+1) summary column is
    redundant once T+1 folds into Invested + Total Asset Value + per-
    holding cost/avg. FE-side removes the render at MSNSummary.tsx
    L198-208; BE-side removes the field entirely. A stray emit would
    tempt a future FE consumer to re-introduce the double-count path.
    """
    row = _service([_kite_holding()]).fetch_kite_holdings("u")[0]
    assert "pending_t1_value" not in row, (
        "pending_t1_value must be REMOVED post-ak-yz9c fold — Overseer Q1 "
        "confirmed redundant once T+1 folds into Invested / TotalAssetValue."
    )


# ── 2: ak-yz9c fold — the core money-math discriminator ────────────────────

def test_total_qty_is_settled_plus_t1_qty():
    """THE core fold invariant.

    Pre-fold: `total_qty` did not exist. `t1_quantity` was surfaced
              alongside `quantity` (settled) and FE combined them for
              display; `pending_t1_value` disclosed the pending rupee
              amount separately.
    Post-fold: `total_qty = settled_qty + t1_qty` is the single BE-side
              blended quantity that drives every folded money-math
              field. FE reads this directly for row/summary display.

    Mutation discriminator: on pre-fold code, `total_qty` field wouldn't
    exist and this test errors on KeyError. On a naive fold that
    computes `total_qty` off Kite's `quantity` (settled-only) it would
    be 250 not 451. Same test binary, opposite outcome per commit — the
    ak-lvu-arc discipline.
    """
    row = _service([_kite_holding(quantity=250, t1_quantity=201)]) \
        .fetch_kite_holdings("u")[0]

    assert row["settled_qty"] == 250
    assert row["t1_qty"] == 201
    assert row["total_qty"] == 451, (
        f"total_qty must be BLENDED (250 settled + 201 t1 = 451); "
        f"got {row['total_qty']}."
    )


def test_kite_quantity_field_semantic_preserved_alongside_new_settled_qty():
    """Wire naming Option (i): Kite's raw `quantity` field on the
    `fetch_kite_holdings` output retains its pre-fold semantic (settled
    only, matching Kite's own SDK). `settled_qty` is the new explicit
    Kite-view field.

    Under Option (i) FE keeps its `buyQuant` (DB settled) unchanged; new
    `settled_qty`/`t1_qty`/`total_qty` are additive overlays. This test
    guards against a naive fold that silently swaps `quantity`'s
    semantic (which would double-count on any FE consumer still doing
    `quantity + t1_quantity`).
    """
    row = _service([_kite_holding(quantity=250, t1_quantity=201)]) \
        .fetch_kite_holdings("u")[0]

    # Raw Kite `quantity` unchanged from pre-fold semantic
    assert row["quantity"] == 250, (
        "`quantity` field on fetch_kite_holdings must retain pre-fold "
        "settled-only semantic (Option (i) wire naming, agreed 2026-09-11)."
    )
    # New explicit fold-in fields are additive
    assert row["settled_qty"] == 250
    assert row["t1_qty"] == 201
    assert row["total_qty"] == 451


def test_invested_uses_total_qty_and_kite_average_price():
    """`invested` = total_qty × Kite broker-canonical avg.

    Uses the ak-yz9c reference fixture: 451 × 1500 = 676_500.
    On pre-fold code, `invested` field wouldn't exist. On a naive
    settled-only fold, this would be 250 × 1500 = 375_000 — the failing
    value the mutation discriminator catches.
    """
    row = _service([_kite_holding(quantity=250, t1_quantity=201,
                                  average_price=1500.0)]) \
        .fetch_kite_holdings("u")[0]

    assert row["invested"] == pytest.approx(676_500.0), (
        f"invested must fold T+1 in (451 × 1500 = 676_500); "
        f"got {row['invested']} — check that total_qty uses the blended total."
    )


def test_current_value_uses_total_qty_and_last_price():
    """`current_value` = total_qty × last_price.

    Reference fixture: 451 × 1600 = 721_600. Naive settled-only would give
    250 × 1600 = 400_000 (the mutation-test failing value).
    """
    row = _service([_kite_holding(quantity=250, t1_quantity=201,
                                  last_price=1600.0)]) \
        .fetch_kite_holdings("u")[0]

    assert row["current_value"] == pytest.approx(721_600.0)


def test_unrealized_pnl_is_current_value_minus_invested():
    """`unrealized_pnl` = current_value − invested.

    Reference fixture: 721_600 − 676_500 = 45_100. Naive pre-fold Kite
    `pnl` would be settled-only P&L ≈ 250 × (1600 − 1500) = 25_000; the
    mutation-test discriminator catches that shape.
    """
    row = _service([_kite_holding(quantity=250, t1_quantity=201,
                                  average_price=1500.0, last_price=1600.0)]) \
        .fetch_kite_holdings("u")[0]

    assert row["unrealized_pnl"] == pytest.approx(45_100.0)
    # Also assert the constituent invariant: pnl = total_qty × (last − avg)
    assert row["unrealized_pnl"] == pytest.approx(
        row["total_qty"] * (row["last_price"] - row["average_price"])
    )


def test_day_change_amount_folds_t1_shares_per_overseer_q2():
    """`day_change_amount` = total_qty × (last − close).

    Overseer Q2 answer: "Yes — include T+1 in day P&L. Consistent with
    'treat T+1 as bought.' If cost basis includes T+1, day P&L on those
    shares should too."

    Reference fixture: 451 × (1600 − 1580) = 451 × 20 = 9_020. Naive
    settled-only would be 250 × 20 = 5_000 (mutation-discriminated).
    """
    row = _service([_kite_holding(quantity=250, t1_quantity=201,
                                  last_price=1600.0, close_price=1580.0)]) \
        .fetch_kite_holdings("u")[0]

    assert row["day_change_amount"] == pytest.approx(9_020.0)


# ── 3: fold math at degenerate boundaries ──────────────────────────────────

def test_fold_math_holds_when_t1_qty_zero():
    """Settled-only holding — the fold degenerates to identity math.

    total_qty == settled_qty, all folded fields equal their settled-only
    values. Prevents a fold implementation that only works with T+1
    present from silently regressing the vast majority of holdings.
    """
    row = _service([_kite_holding(quantity=100, t1_quantity=0,
                                  average_price=2000.0, last_price=2100.0,
                                  close_price=2080.0)]) \
        .fetch_kite_holdings("u")[0]

    assert row["settled_qty"] == 100
    assert row["t1_qty"] == 0
    assert row["total_qty"] == 100
    assert row["invested"] == pytest.approx(200_000.0)          # 100 × 2000
    assert row["current_value"] == pytest.approx(210_000.0)     # 100 × 2100
    assert row["unrealized_pnl"] == pytest.approx(10_000.0)     # 210k − 200k
    assert row["day_change_amount"] == pytest.approx(2_000.0)   # 100 × 20


def test_fold_math_holds_when_settled_qty_zero():
    """Pure T+1 holding — bought Friday, still un-settled Monday morning.

    total_qty == t1_qty, all folded fields reflect T+1-only math. This is
    the case where the old contract emitted a row with quantity=0 and
    non-zero pending_t1_value; the new contract emits a fully-folded row
    with the T+1 shares treated as owned.
    """
    row = _service([_kite_holding(quantity=0, t1_quantity=75,
                                  average_price=800.0, last_price=850.0,
                                  close_price=820.0)]) \
        .fetch_kite_holdings("u")[0]

    assert row["settled_qty"] == 0
    assert row["t1_qty"] == 75
    assert row["total_qty"] == 75
    assert row["invested"] == pytest.approx(60_000.0)      # 75 × 800
    assert row["current_value"] == pytest.approx(63_750.0) # 75 × 850
    assert row["unrealized_pnl"] == pytest.approx(3_750.0) # 63.75k − 60k
    assert row["day_change_amount"] == pytest.approx(2_250.0)  # 75 × 30


def test_fold_math_zero_position_produces_zero_row():
    """quantity=0 AND t1_quantity=0 — a fully-empty holding (post-sell,
    still in the payload). All folded numerics are 0. Guards against a
    DivisionByZero shape in a percent calculation somewhere downstream.
    """
    row = _service([_kite_holding(quantity=0, t1_quantity=0,
                                  average_price=1000.0, last_price=1200.0,
                                  close_price=1180.0)]) \
        .fetch_kite_holdings("u")[0]

    assert row["settled_qty"] == 0
    assert row["t1_qty"] == 0
    assert row["total_qty"] == 0
    assert row["invested"] == 0
    assert row["current_value"] == 0
    assert row["unrealized_pnl"] == 0
    assert row["day_change_amount"] == 0


# ── 4: portfolio-level fold arithmetic (FE aggregation semantic) ───────────

def test_portfolio_sums_are_row_sums_across_mixed_settled_and_t1():
    """FE aggregates portfolio totals by summing per-row emitted values.

    Verify that summing across a mixed portfolio (settled+t1, t1-only,
    settled-only) gives the total the FE will show. If the per-row math
    is right, the sum is right by construction — this test exists so a
    future refactor that changes per-row math is forced to demonstrate
    the aggregate is still correct.
    """
    holdings = [
        # ak-yz9c reference: 250 settled + 201 t1 @ 1500 avg / 1600 last
        _kite_holding(quantity=250, t1_quantity=201, average_price=1500.0,
                      last_price=1600.0, close_price=1580.0),
        # Pure T+1 (Friday buy, Monday morning): 75 t1 @ 800/850
        _kite_holding(tradingsymbol="INFY", quantity=0, t1_quantity=75,
                      average_price=800.0, last_price=850.0, close_price=820.0),
        # Settled-only steady holding: 100 @ 2000/2100
        _kite_holding(tradingsymbol="TCS", quantity=100, t1_quantity=0,
                      average_price=2000.0, last_price=2100.0, close_price=2080.0),
    ]
    rows = _service(holdings).fetch_kite_holdings("u")

    total_invested = sum(r["invested"] for r in rows)
    total_current = sum(r["current_value"] for r in rows)
    total_pnl = sum(r["unrealized_pnl"] for r in rows)
    total_day_change = sum(r["day_change_amount"] for r in rows)

    # Per-row: 676_500 + 60_000 + 200_000
    assert total_invested == pytest.approx(936_500.0)
    # Per-row: 721_600 + 63_750 + 210_000
    assert total_current == pytest.approx(995_350.0)
    # Sum(pnl) == sum(current) − sum(invested)
    assert total_pnl == pytest.approx(58_850.0)
    assert total_pnl == pytest.approx(total_current - total_invested)
    # Day change: 9_020 + 2_250 + 2_000
    assert total_day_change == pytest.approx(13_270.0)


# ── 5: defensive coercion ──────────────────────────────────────────────────

def test_absent_optional_fields_default_to_zero():
    """Older/pruned Kite payloads may omit these entirely; the row must
    still be renderable rather than carrying None into arithmetic. Every
    folded numeric collapses to 0 rather than crashing.
    """
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

    assert row["settled_qty"] == 5
    assert row["t1_qty"] == 0
    assert row["total_qty"] == 5           # settled + 0 = 5
    assert row["invested"] == pytest.approx(500.0)   # 5 × 100
    assert row["current_value"] == pytest.approx(550.0)  # 5 × 110
    assert row["close_price"] == 0
    assert row["day_change_amount"] == pytest.approx(550.0)  # 5 × (110 − 0)


def test_none_valued_fields_are_coerced_to_zero():
    """Kite is known to emit `null` for optional numeric fields on
    stripped-down responses. Coerce to 0 so arithmetic never gets None.
    """
    row = _service([_kite_holding(t1_quantity=None, last_price=None,
                                  day_change=None, close_price=None,
                                  average_price=None)]) \
        .fetch_kite_holdings("u")[0]

    assert row["t1_qty"] == 0
    assert row["last_price"] == 0
    assert row["close_price"] == 0
    assert row["average_price"] == 0
    # Folded math with all-None inputs collapses to 0 across the board.
    assert row["invested"] == 0
    assert row["current_value"] == 0
    assert row["unrealized_pnl"] == 0
    assert row["day_change_amount"] == 0


def test_mtf_fields_default_to_zero_without_mtf():
    row = _service([_kite_holding()]).fetch_kite_holdings("u")[0]
    assert row["mtf_quantity"] == 0
    assert row["mtf_average_price"] == 0


def test_mtf_fields_are_read_when_present():
    row = _service([_kite_holding(mtf={"quantity": 3, "average_price": 1490.0})]) \
        .fetch_kite_holdings("u")[0]
    assert row["mtf_quantity"] == 3
    assert row["mtf_average_price"] == pytest.approx(1490.0)


# ── 6: DB sync — statement pipeline must not absorb T+1 shares ─────────────
#
# The Kite fold-in applies to the DISPLAY / P&L path. The statement-DB
# (`PurchasedSecurities`) is a separate ledger — it holds statement-
# derived rows only. T+1 shares must NOT enter that table, because their
# statement-derived cost basis doesn't exist yet (the statement is
# generated post-settlement). These invariants pre-date ak-yz9c and are
# preserved unchanged: Kite-view is broker-canonical, statement-DB is
# statement-authoritative for its own row set.

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
    """`sync_kite_holdings_to_db` writes only the settled portion into the
    `PurchasedSecurities` table. The Kite fold-in operates on the display
    path — statement-DB rows must still reflect their statement-derived
    basis, and folding T+1 into the DB would create rows with no basis at
    all until the statement arrives.

    Verifies the statement-pipeline invariant survives the display fold.
    """
    svc = _sync_service([_kite_holding(quantity=10, t1_quantity=4)])
    svc.sync_kite_holdings_to_db("user-1")

    added = svc._session.add.call_args[0][0]
    assert added.buyQuant == 10, (
        "T+1 shares must not be persisted into the statement-DB as settled — "
        "statement rows are statement-derived, not broker-derived. "
        "The ak-yz9c fold applies to the DISPLAY path, not the DB write path."
    )


def test_sync_skips_holdings_that_are_entirely_pending():
    """A holding that is 100% T+1 has kite.quantity=0. No DB row should
    be created — the statement that establishes real cost basis hasn't
    arrived yet. Kite view folds this in via the DISPLAY path; the
    statement pipeline still waits for the statement.
    """
    svc = _sync_service([_kite_holding(quantity=0, t1_quantity=7)])
    result = svc.sync_kite_holdings_to_db("user-1")

    svc._session.add.assert_not_called()
    assert result["synced"] == 0


def test_sync_does_not_let_kite_average_price_rewrite_matching_db_rows():
    """The statement-DB `buyPrice` is statement-authoritative for the
    statement-DB row set. Kite's `average_price` is broker-canonical for
    the Kite DISPLAY path (post-2026-09-04 flatten-on-sync) but NOT for
    the statement pipeline — those two ledgers remain distinct.

    A future refactor that lets Kite flatten INTO the statement-DB would
    silently overwrite statement-derived cost history with broker's
    reconciled view; guarded here.
    """
    existing = types.SimpleNamespace(buyQuant=10, buyPrice=1234.56)
    svc = _sync_service([_kite_holding(quantity=10, average_price=1500.0)],
                        existing=existing)

    svc.sync_kite_holdings_to_db("user-1")

    assert existing.buyPrice == 1234.56, (
        "statement-derived basis was overwritten in DB; the Kite fold-in "
        "changes the DISPLAY authority, not the statement-DB write path."
    )


# ── 7: enrichment degrades instead of failing ──────────────────────────────

def test_expired_kite_session_yields_empty_enrichment_index():
    """The securities list is otherwise built from our own DB + the NSE
    price feed, so a dead Kite token must never take the whole endpoint
    down. Rows just render without the optional Kite fields.
    """
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
