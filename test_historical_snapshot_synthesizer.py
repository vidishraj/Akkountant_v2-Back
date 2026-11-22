"""ak-rcg1 (ak-ofir Phase 2B) tests: pure synthesizer logic.

The synthesizer module (``services/historical_snapshot_synthesizer
.py``) is imported directly — no AST lift needed because the module
only pulls stdlib (dataclasses, decimal, datetime, typing). The
Flask / Firebase / Google chain only enters via the CLI
(``scripts/synthesize_historical_snapshots.py``) which these tests
don't exercise.

Scope:
  1. ``reconstruct_msn_holdings_at`` — chronological-replay
     reconstructs per-buyId qty + weighted-avg price at a target
     date, mirroring MfService / NpsService / StocksService buy-
     then-sell mutations.
  2. ``synthesize_msn_slice`` — row shape, invested / value /
     profit math, coverage floor, no-holdings-returns-None.
  3. ``synthesize_epg_deposit_slice`` — delegates to a
     calculateTransactionTable stub, filters deposits by date,
     no-deposits-returns-None.
  4. ``synthesize_gold_slice`` — purity scaling, no-rate-returns-
     None, no-holdings-returns-None.
  5. Row shape parity with the live daemon's
     ``_make_row`` (keys + rounding).
"""

from __future__ import annotations

import os
import sys
import unittest
from datetime import date
from decimal import Decimal


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


try:
    from services.historical_snapshot_synthesizer import (
        DepositEvent,
        GoldHolding,
        TYPE_LABEL_EPF,
        TYPE_LABEL_GOLD,
        TYPE_LABEL_MF,
        TYPE_LABEL_NPS,
        TYPE_LABEL_PPF,
        TYPE_LABEL_STOCKS,
        TxEvent,
        reconstruct_msn_holdings_at,
        synthesize_epg_deposit_slice,
        synthesize_gold_slice,
        synthesize_msn_slice,
    )
    _DEPS_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _DEPS_OK = False
    _SKIP_REASON = f"synthesizer import failed: {_exc}"


# ── (1) reconstruct_msn_holdings_at ──────────────────────────────────


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestReconstructMSNHoldings(unittest.TestCase):

    def test_single_buy_returns_qty_price_buycode(self):
        events = [
            TxEvent(
                buy_id="B1", buy_code="RELIANCE",
                tx_date=date(2026, 1, 15),
                qty=Decimal("10"), price=Decimal("2500"),
                tx_type="buy",
            ),
        ]
        state = reconstruct_msn_holdings_at(events, date(2026, 6, 1))
        self.assertIn("B1", state)
        qty, price, buy_code = state["B1"]
        self.assertEqual(qty, Decimal("10"))
        self.assertEqual(price, Decimal("2500"))
        self.assertEqual(buy_code, "RELIANCE")

    def test_future_buy_not_in_state(self):
        """A buy whose date > target_date must not appear in state."""
        events = [
            TxEvent(
                buy_id="B1", buy_code="RELIANCE",
                tx_date=date(2026, 6, 15),
                qty=Decimal("10"), price=Decimal("2500"),
                tx_type="buy",
            ),
        ]
        state = reconstruct_msn_holdings_at(events, date(2026, 6, 1))
        self.assertEqual(state, {})

    def test_two_buys_weighted_avg_price(self):
        """Two buys at different prices → qty = sum, price =
        weighted avg. Matches MfService.buySecurity mutation
        (L74-75).
        """
        events = [
            TxEvent(
                "B1", "RELIANCE", date(2026, 1, 15),
                Decimal("10"), Decimal("2500"), "buy",
            ),
            TxEvent(
                "B1", "RELIANCE", date(2026, 3, 20),
                Decimal("5"), Decimal("2800"), "buy",
            ),
        ]
        state = reconstruct_msn_holdings_at(events, date(2026, 6, 1))
        qty, price, _ = state["B1"]
        self.assertEqual(qty, Decimal("15"))
        # ((10*2500) + (5*2800)) / 15 = (25000 + 14000) / 15 = 39000/15
        self.assertEqual(price, Decimal("39000") / Decimal("15"))

    def test_sell_reduces_qty_not_price(self):
        """On sell, qty reduces; avg price unchanged. Matches
        MfService.sellSecurity shape (L104-106: purchase.buyQuant
        -= sellQuant; price untouched)."""
        events = [
            TxEvent(
                "B1", "RELIANCE", date(2026, 1, 15),
                Decimal("10"), Decimal("2500"), "buy",
            ),
            TxEvent(
                "B1", "RELIANCE", date(2026, 3, 20),
                Decimal("3"), Decimal("3000"), "sell",
            ),
        ]
        state = reconstruct_msn_holdings_at(events, date(2026, 6, 1))
        qty, price, _ = state["B1"]
        self.assertEqual(qty, Decimal("7"))
        self.assertEqual(price, Decimal("2500"))

    def test_fully_sold_row_omitted(self):
        """qty <= 0 → row excluded (matches live buyQuant > 0
        filter)."""
        events = [
            TxEvent(
                "B1", "RELIANCE", date(2026, 1, 15),
                Decimal("10"), Decimal("2500"), "buy",
            ),
            TxEvent(
                "B1", "RELIANCE", date(2026, 3, 20),
                Decimal("10"), Decimal("3000"), "sell",
            ),
        ]
        state = reconstruct_msn_holdings_at(events, date(2026, 6, 1))
        self.assertEqual(state, {})

    def test_sell_before_target_but_future_buy_ignored(self):
        """Sell tx before target counted; subsequent buy after
        target NOT counted."""
        events = [
            TxEvent(
                "B1", "RELIANCE", date(2026, 1, 15),
                Decimal("10"), Decimal("2500"), "buy",
            ),
            TxEvent(
                "B1", "RELIANCE", date(2026, 3, 20),
                Decimal("4"), Decimal("3000"), "sell",
            ),
            TxEvent(
                "B1", "RELIANCE", date(2026, 7, 10),
                Decimal("5"), Decimal("2700"), "buy",
            ),
        ]
        state = reconstruct_msn_holdings_at(events, date(2026, 6, 1))
        qty, price, _ = state["B1"]
        self.assertEqual(qty, Decimal("6"))
        self.assertEqual(price, Decimal("2500"))

    def test_sell_before_buy_defensively_skipped(self):
        """Data-integrity: sell event with no prior buy → skip,
        not crash."""
        events = [
            TxEvent(
                "B_ORPHAN", "X", date(2026, 1, 10),
                Decimal("5"), Decimal("100"), "sell",
            ),
        ]
        state = reconstruct_msn_holdings_at(events, date(2026, 6, 1))
        self.assertEqual(state, {})


# ── (2) synthesize_msn_slice ─────────────────────────────────────────


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestSynthesizeMSNSlice(unittest.TestCase):

    def test_basic_row_shape(self):
        """Happy path: one holding, price available → row fields
        all present + math correct."""
        events = [
            TxEvent(
                "B1", "RELIANCE", date(2026, 1, 15),
                Decimal("10"), Decimal("2500"), "buy",
            ),
        ]
        price_lookup = lambda bc, d: Decimal("3000") if bc == "RELIANCE" else None
        row = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            investment_type=TYPE_LABEL_STOCKS,
            events=events,
            price_lookup=price_lookup,
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["user"], "u1")
        self.assertEqual(row["investment_type"], "Stocks")
        self.assertEqual(row["date"], date(2026, 6, 1))
        # qty × avg_price = 10 × 2500 = 25000
        self.assertEqual(row["total_invested"], Decimal("25000.00"))
        # qty × historical_price = 10 × 3000 = 30000
        self.assertEqual(row["current_value"], Decimal("30000.00"))
        # profit = 30000 - 25000 = 5000
        self.assertEqual(row["profit"], Decimal("5000.00"))
        # profit_pct = 5000/25000 × 100 = 20.0000
        self.assertEqual(row["profit_percent"], Decimal("20.0000"))

    def test_no_holdings_returns_none(self):
        """User didn't own this asset type on target_date → None
        (NOT a zero row — zero would be semantically wrong)."""
        row = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            investment_type=TYPE_LABEL_STOCKS,
            events=[],
            price_lookup=lambda bc, d: Decimal("100"),
        )
        self.assertIsNone(row)

    def test_coverage_below_floor_returns_none(self):
        """3 holdings, only 1 has a price (33%) — below 50% floor.
        Row OMITTED to prevent under-counted trend-chart dip."""
        events = [
            TxEvent("B1", "A", date(2026, 1, 1),
                    Decimal("10"), Decimal("100"), "buy"),
            TxEvent("B2", "B", date(2026, 1, 1),
                    Decimal("10"), Decimal("100"), "buy"),
            TxEvent("B3", "C", date(2026, 1, 1),
                    Decimal("10"), Decimal("100"), "buy"),
        ]
        price_lookup = lambda bc, d: (
            Decimal("200") if bc == "A" else None
        )
        row = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_MF,
            events=events, price_lookup=price_lookup,
        )
        self.assertIsNone(row)

    def test_coverage_at_floor_returns_row(self):
        """2 of 4 holdings priced = 50% exactly — row INCLUDED.

        ak-rcg1 re-review MAJOR (2026-11-22): PRE-FIX this test
        pinned a LIE — total_invested summed over all 4 holdings
        (₹4000) while current_value summed only over the 2 priced
        ones (₹4000), so profit=₹0 / 0% when the priced subset
        was genuinely +100%. POST-FIX: both sums are over the
        SAME priced subset (invested = 2 × 10 × 100 = ₹2000;
        value = 2 × 10 × 200 = ₹4000; profit = +₹2000 / +100%).
        That's the honest answer for "what the slice I can price
        is worth vs what it cost." The reported value is still
        bounded by coverage (the un-priced half is absent) —
        that's by design of the floor rule — but the PROFIT line
        no longer gaslights the operator / trend chart.
        """
        events = [
            TxEvent(f"B{i}", f"S{i}", date(2026, 1, 1),
                    Decimal("10"), Decimal("100"), "buy")
            for i in range(4)
        ]
        price_lookup = lambda bc, d: (
            Decimal("200") if bc in ("S0", "S1") else None
        )
        row = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_MF,
            events=events, price_lookup=price_lookup,
        )
        self.assertIsNotNone(row)
        # POST-FIX honest math — both sums over the priced subset.
        # invested = 2 × (10 × 100) = 2000 (priced only)
        self.assertEqual(row["total_invested"], Decimal("2000.00"))
        # value = 2 × (10 × 200) = 4000 (priced only)
        self.assertEqual(row["current_value"], Decimal("4000.00"))
        # profit = 4000 - 2000 = 2000 (honest; pre-fix bug = 0)
        self.assertEqual(row["profit"], Decimal("2000.00"))
        # profit_pct = 2000 / 2000 × 100 = 100.0000 (pre-fix = 0)
        self.assertEqual(row["profit_percent"], Decimal("100.0000"))

    def test_mf_nav_flow_identical_shape(self):
        """MF uses the same row shape (label = 'MF'). The synth
        doesn't change behaviour between Stocks/MF/NPS — asserts
        the shape is identical regardless of investment_type."""
        events = [
            TxEvent(
                "M1", "120503", date(2026, 1, 15),
                Decimal("100"), Decimal("50"), "buy",
            ),
        ]
        row = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_MF,
            events=events,
            price_lookup=lambda bc, d: Decimal("52"),
        )
        self.assertEqual(row["investment_type"], "MF")
        # invested = 100 × 50 = 5000; value = 100 × 52 = 5200.
        self.assertEqual(row["total_invested"], Decimal("5000.00"))
        self.assertEqual(row["current_value"], Decimal("5200.00"))
        self.assertEqual(row["profit"], Decimal("200.00"))

    def test_zero_invested_no_divide_by_zero(self):
        """Edge: invested = 0 (shouldn't happen in practice — qty >
        0 with price > 0 → invested > 0, but defensive) → profit_pct
        = 0 not crash."""
        events = [
            TxEvent(
                "B1", "ZERO_PRICE", date(2026, 1, 1),
                Decimal("10"), Decimal("0"), "buy",
            ),
        ]
        row = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_STOCKS,
            events=events,
            price_lookup=lambda bc, d: Decimal("100"),
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["profit_percent"], Decimal("0.0000"))

    def test_user_id_propagates(self):
        """user_id string is passed through verbatim — synth
        doesn't fabricate or alias."""
        events = [
            TxEvent(
                "B1", "X", date(2026, 1, 1),
                Decimal("1"), Decimal("1"), "buy",
            ),
        ]
        row = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="firebase-user-12345",
            investment_type=TYPE_LABEL_STOCKS,
            events=events,
            price_lookup=lambda bc, d: Decimal("1"),
        )
        self.assertEqual(row["user"], "firebase-user-12345")


# ── (3) synthesize_epg_deposit_slice ─────────────────────────────────


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestSynthesizeEPGDepositSlice(unittest.TestCase):

    def test_epf_single_deposit(self):
        deposits = [
            DepositEvent(
                buy_id="D1",
                deposit_date=date(2026, 1, 15),
                employee_amount=Decimal("5000"),
                employer_amount=Decimal("5000"),
            ),
        ]

        # Stub calculateTransactionTable that returns (tx, profit,
        # final_balance, running_interest). Receives the
        # DepositEvent list verbatim from the synthesizer — the CLI
        # adapter wraps into legacy .depositAmount shape OUTSIDE
        # the pure synthesizer; this stub exercises the pure path.
        def calc(dl):
            self.assertEqual(len(dl), 1)
            self.assertEqual(dl[0].employee_amount, Decimal("5000"))
            return ([], 0.0, 10000.0, 0.0)

        row = synthesize_epg_deposit_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_EPF,
            deposits=deposits, calc_transaction_table=calc,
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["investment_type"], "EPF")
        self.assertEqual(row["current_value"], Decimal("10000.00"))
        self.assertEqual(row["total_invested"], Decimal("10000.00"))
        self.assertEqual(row["profit"], Decimal("0.00"))

    def test_epf_future_deposits_filtered_out(self):
        """Deposits with date > target_date MUST not reach calc."""
        deposits = [
            DepositEvent(
                "D1", date(2026, 1, 15),
                Decimal("5000"), Decimal("5000"),
            ),
            DepositEvent(
                "D2", date(2026, 9, 15),
                Decimal("5000"), Decimal("5000"),
            ),
        ]

        calc_calls = []

        def calc(dl):
            calc_calls.append([d.deposit_date for d in dl])
            return ([], 0, 10000.0, 0)

        synthesize_epg_deposit_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_EPF,
            deposits=deposits, calc_transaction_table=calc,
        )
        self.assertEqual(len(calc_calls), 1)
        self.assertEqual(calc_calls[0], [date(2026, 1, 15)])

    def test_no_deposits_before_target_returns_none(self):
        """No deposits by target_date → None (not zero row)."""
        deposits = [
            DepositEvent(
                "D1", date(2026, 9, 15),
                Decimal("5000"), Decimal("5000"),
            ),
        ]
        row = synthesize_epg_deposit_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_EPF,
            deposits=deposits,
            calc_transaction_table=lambda dl: ([], 0, 0, 0),
        )
        self.assertIsNone(row)

    def test_calc_returns_none_final_balance_yields_none(self):
        """calculateTransactionTable returning None (service-side
        error path) → synth returns None, doesn't crash."""
        deposits = [
            DepositEvent(
                "D1", date(2026, 1, 15),
                Decimal("5000"), Decimal("5000"),
            ),
        ]
        row = synthesize_epg_deposit_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_EPF,
            deposits=deposits,
            calc_transaction_table=lambda dl: (None, None, None, None),
        )
        self.assertIsNone(row)

    def test_ppf_label_passes_through(self):
        """PPF uses the same synth path; label differs."""
        deposits = [
            DepositEvent(
                "D1", date(2026, 1, 15),
                Decimal("100000"), None,
            ),
        ]
        row = synthesize_epg_deposit_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_PPF,
            deposits=deposits,
            calc_transaction_table=lambda dl: (
                [], 2800.0, 102800.0, 0.0,
            ),
        )
        self.assertEqual(row["investment_type"], "PPF")
        self.assertEqual(row["total_invested"], Decimal("100000.00"))
        self.assertEqual(row["profit"], Decimal("2800.00"))


# ── (4) synthesize_gold_slice ────────────────────────────────────────


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestSynthesizeGoldSlice(unittest.TestCase):

    def test_24k_full_purity(self):
        holdings = [
            GoldHolding(
                buy_id="G1", deposit_date=date(2026, 1, 15),
                qty_grams=Decimal("10"), gold_type="24",
            ),
        ]
        deposit_amounts = {"G1": Decimal("60000")}
        row = synthesize_gold_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            holdings=holdings,
            gold_rate_per_gram_24k=Decimal("7000"),
            deposit_amount_lookup=deposit_amounts.get,
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["investment_type"], "Gold")
        self.assertEqual(row["total_invested"], Decimal("60000.00"))
        # 10g × ₹7000/g × 1.0 = 70000
        self.assertEqual(row["current_value"], Decimal("70000.00"))

    def test_22k_purity_scaled(self):
        """22K gold worth 91.6% of 24K per-gram rate."""
        holdings = [
            GoldHolding(
                "G1", date(2026, 1, 15),
                Decimal("10"), "22",
            ),
        ]
        row = synthesize_gold_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            holdings=holdings,
            gold_rate_per_gram_24k=Decimal("7000"),
            deposit_amount_lookup=lambda b: Decimal("55000"),
        )
        # 10 × 7000 × 0.916 = 64120
        self.assertEqual(row["current_value"], Decimal("64120.00"))

    def test_18k_purity_scaled(self):
        holdings = [
            GoldHolding(
                "G1", date(2026, 1, 15),
                Decimal("10"), "18",
            ),
        ]
        row = synthesize_gold_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            holdings=holdings,
            gold_rate_per_gram_24k=Decimal("7000"),
            deposit_amount_lookup=lambda b: Decimal("50000"),
        )
        # 10 × 7000 × 0.75 = 52500
        self.assertEqual(row["current_value"], Decimal("52500.00"))

    def test_no_rate_returns_none(self):
        """ak-rcg1 scope-cut per Overseer 2026-11-22: gold is opt-in
        via operator rate file. No rate for date → None, NOT zero."""
        row = synthesize_gold_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            holdings=[
                GoldHolding(
                    "G1", date(2026, 1, 15),
                    Decimal("10"), "24",
                ),
            ],
            gold_rate_per_gram_24k=None,
            deposit_amount_lookup=lambda b: Decimal("50000"),
        )
        self.assertIsNone(row)

    def test_no_holdings_returns_none(self):
        row = synthesize_gold_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            holdings=[],
            gold_rate_per_gram_24k=Decimal("7000"),
            deposit_amount_lookup=lambda b: Decimal("0"),
        )
        self.assertIsNone(row)

    def test_future_purchase_filtered(self):
        """Gold bought AFTER target_date not in the snapshot value."""
        holdings = [
            GoldHolding(
                "G1", date(2026, 1, 15),
                Decimal("10"), "24",
            ),
            GoldHolding(
                "G2", date(2026, 9, 1),  # after target
                Decimal("5"), "24",
            ),
        ]
        row = synthesize_gold_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            holdings=holdings,
            gold_rate_per_gram_24k=Decimal("7000"),
            deposit_amount_lookup=lambda b: Decimal("50000"),
        )
        # Only G1 counts: 10 × 7000 × 1.0 = 70000
        self.assertEqual(row["current_value"], Decimal("70000.00"))

    def test_mixed_purity_holdings(self):
        holdings = [
            GoldHolding(
                "G1", date(2026, 1, 1),
                Decimal("5"), "24",
            ),
            GoldHolding(
                "G2", date(2026, 2, 1),
                Decimal("10"), "22",
            ),
        ]
        deposit_amounts = {"G1": Decimal("35000"), "G2": Decimal("60000")}
        row = synthesize_gold_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            holdings=holdings,
            gold_rate_per_gram_24k=Decimal("7000"),
            deposit_amount_lookup=deposit_amounts.get,
        )
        # G1: 5 × 7000 × 1.0 = 35000
        # G2: 10 × 7000 × 0.916 = 64120
        # Total: 99120
        self.assertEqual(row["current_value"], Decimal("99120.00"))
        self.assertEqual(row["total_invested"], Decimal("95000.00"))


# ── (5) Daemon-parity row shape ──────────────────────────────────────


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestRowShapeDaemonParity(unittest.TestCase):
    """Synthesized rows MUST match the live daemon's row shape so
    getInvestmentSnapshots sees uniform results regardless of row
    origin."""

    def _sample_row(self):
        events = [
            TxEvent(
                "B1", "RELIANCE", date(2026, 1, 15),
                Decimal("10"), Decimal("2500"), "buy",
            ),
        ]
        return synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            investment_type=TYPE_LABEL_STOCKS,
            events=events,
            price_lookup=lambda bc, d: Decimal("3000"),
        )

    def test_row_carries_daemon_fields(self):
        """The 7 keys _make_row writes must appear in the synth
        output — drift would make synthesized rows incompatible
        with the InvestmentSnapshot UPSERT schema."""
        row = self._sample_row()
        expected_keys = {
            "date", "user", "investment_type",
            "total_invested", "current_value", "profit",
            "profit_percent",
        }
        self.assertEqual(set(row.keys()), expected_keys)

    def test_money_fields_quantized_to_2dp(self):
        """Money quantize = 0.01 (matches daemon _make_row)."""
        row = self._sample_row()
        for key in ("total_invested", "current_value", "profit"):
            val = row[key]
            # Must be a Decimal with ≤2dp precision.
            exp = -val.as_tuple().exponent
            self.assertLessEqual(
                exp, 2,
                f"{key} not quantized to 2dp: {val}",
            )

    def test_percent_field_quantized_to_4dp(self):
        row = self._sample_row()
        exp = -row["profit_percent"].as_tuple().exponent
        self.assertLessEqual(
            exp, 4,
            f"profit_percent not quantized to 4dp: "
            f"{row['profit_percent']}",
        )

    def test_money_rounding_mode_is_ROUND_DOWN_not_HALF_DOWN(self):
        """ak-rcg1 re-review MINOR (2026-11-22): the daemon's
        ``_make_row`` uses ROUND_DOWN (truncate toward zero), NOT
        HALF_DOWN (round half to even on ties). The two differ by
        up to one paise at the quantize boundary — a silent
        maintainer swap would ship green under the 2dp/4dp
        'quantized' assertions above but break byte-identical
        parity with daemon rows.

        Tie-breaking pin-down: craft inputs where the raw
        current_value is exactly .125 (midway between .12 and
        .13). ROUND_DOWN truncates → .12. HALF_DOWN would round
        to .12 too (ties-round-down flavour differs by direction
        from HALF_EVEN, but both end at .12 for this input), so
        we need an asymmetric midpoint: use .5 → ROUND_DOWN
        truncates to .5 while HALF_EVEN would round to .5. The
        cleanest discriminator is a trailing digit >= 5 that
        ROUND_DOWN truncates but HALF_UP/HALF_EVEN would push
        upward.

        Concrete fixture: 1 unit × ₹0.129 price = ₹0.129 raw.
        ROUND_DOWN → ₹0.12 (truncate the .009 trailing). HALF_UP
        or HALF_EVEN → ₹0.13 (round up the trailing 9). Asserting
        .12 pins ROUND_DOWN specifically.
        """
        events = [
            TxEvent(
                "B1", "X", date(2026, 1, 1),
                Decimal("1"), Decimal("0.129"), "buy",
            ),
        ]
        row = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1",
            investment_type=TYPE_LABEL_STOCKS,
            events=events,
            price_lookup=lambda bc, d: Decimal("0.129"),
        )
        # Raw math = ₹0.129. ROUND_DOWN → ₹0.12 (truncate, match
        # daemon). HALF_UP / HALF_EVEN / HALF_DOWN-with-upward-tie
        # would all give ₹0.13 — any of those would fail this
        # assertion, so a silent mode-swap reds hard here.
        self.assertEqual(
            row["total_invested"], Decimal("0.12"),
            "total_invested rounding regressed — expected "
            "ROUND_DOWN (truncate 0.129 → 0.12). Any other mode "
            "produces 0.13 and breaks daemon-parity.",
        )
        self.assertEqual(
            row["current_value"], Decimal("0.12"),
            "current_value rounding regressed — expected "
            "ROUND_DOWN (truncate 0.129 → 0.12).",
        )

    def test_type_labels_match_daemon_write_values(self):
        """TYPE_LABEL_* constants must match the string values the
        daemon writes to investmentSnapshots.investment_type. ak-h36s
        documented this vocabulary — snapshot task writes 'Stocks',
        'MF', 'NPS', 'PPF', 'EPF', 'Gold' (NOT 'Mutual_Funds',
        NOT 'PF'). Drift here = synth rows under one vocabulary +
        daemon rows under another = chart shows two trend lines
        instead of one."""
        self.assertEqual(TYPE_LABEL_STOCKS, "Stocks")
        self.assertEqual(TYPE_LABEL_MF, "MF")
        self.assertEqual(TYPE_LABEL_NPS, "NPS")
        self.assertEqual(TYPE_LABEL_EPF, "EPF")
        self.assertEqual(TYPE_LABEL_PPF, "PPF")
        self.assertEqual(TYPE_LABEL_GOLD, "Gold")


# ── (6) Idempotency / re-run ─────────────────────────────────────────


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestIdempotency(unittest.TestCase):
    """Pure functions re-run with the same inputs produce the same
    outputs. The caller's UPSERT handles DB-side dedupe; the synth
    just needs to be deterministic."""

    def test_same_inputs_same_outputs(self):
        events = [
            TxEvent(
                "B1", "X", date(2026, 1, 1),
                Decimal("10"), Decimal("100"), "buy",
            ),
        ]
        r1 = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_STOCKS,
            events=events, price_lookup=lambda bc, d: Decimal("150"),
        )
        r2 = synthesize_msn_slice(
            target_date=date(2026, 6, 1),
            user_id="u1", investment_type=TYPE_LABEL_STOCKS,
            events=events, price_lookup=lambda bc, d: Decimal("150"),
        )
        self.assertEqual(r1, r2)


# ── (7) Wire-up drift guards (always-run, no skipUnless) ─────────────
#
# These catch the specific removal-of-source mutation that the
# skipUnless-gated classes above only report as "all skipped". A
# red here means ak-rcg1's core artefact has been deleted/moved —
# surfaces loudly rather than quietly.


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_synthesizer_module_exists_at_expected_path():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        here, "services", "historical_snapshot_synthesizer.py",
    )
    assert os.path.isfile(path), (
        "ak-rcg1 core module missing: "
        "services/historical_snapshot_synthesizer.py. Its removal "
        "would silently revert the entire bead."
    )


def test_cli_script_exists_at_expected_path():
    here = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(
        here, "scripts", "synthesize_historical_snapshots.py",
    )
    assert os.path.isfile(path), (
        "ak-rcg1 CLI script missing: "
        "scripts/synthesize_historical_snapshots.py. The pure "
        "synthesizer module is useless without its CLI entry point."
    )


def test_cli_requires_both_commit_and_assume_yes():
    """Safety belt: the CLI MUST require BOTH --commit AND
    --assume-yes to write. A single-typo guard.
    """
    src = _read_source(
        "scripts/synthesize_historical_snapshots.py",
    )
    assert "--assume-yes" in src, (
        "CLI safety belt missing: the --assume-yes flag is the "
        "second-gate protection against accidental commits."
    )
    assert "args.commit and not args.assume_yes" in src, (
        "CLI must guard: --commit without --assume-yes must be a "
        "rejected combination (safety belt against single-typo "
        "accidental writes)."
    )


def test_dry_run_is_default_mode():
    """The CLI mandate per bead: 'MUST run with --dry-run flag
    first'. That means dry-run is DEFAULT and commit is opt-in,
    NOT the reverse."""
    src = _read_source(
        "scripts/synthesize_historical_snapshots.py",
    )
    # The --commit flag must be action='store_true' (default
    # False) — opt-in semantics.
    import re
    assert re.search(
        r'"--commit",\s*action\s*=\s*"store_true"',
        src,
    ) or re.search(
        r"'--commit',\s*action\s*=\s*'store_true'",
        src,
    ), (
        "CLI --commit must be store_true (default dry-run). Making "
        "commit the default would be an accounting-correctness "
        "regression."
    )


def test_daemon_overlap_capped_by_default():
    """The default run stops at today-1, never writing a row for
    today (which the daemon owns). An operator must explicitly
    pass --allow-daemon-overlap to write over the daemon's zone."""
    src = _read_source(
        "scripts/synthesize_historical_snapshots.py",
    )
    assert "--allow-daemon-overlap" in src, (
        "CLI must expose --allow-daemon-overlap so operator can "
        "consciously override the daemon-write-zone guard."
    )
    assert "today - timedelta(days=1)" in src, (
        "Default end-of-window must be today - 1 (don't overwrite "
        "daemon's forward-only rows)."
    )


if __name__ == "__main__":
    unittest.main()
