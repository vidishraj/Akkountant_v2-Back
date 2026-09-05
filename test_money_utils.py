"""ak-lvu: unit tests for services.money_utils — the Decimal-money primitives
under every write / recompute / display path in the Freelance subsystem.

Every case here corresponds to a real failure mode from the super-review:
  * float ingress silently rounding on tenth-of-a-paisa boundaries
    (super-review AC-5)
  * `int(total_earnings)` truncation (dashboardService L191-192, AC-5)
  * Server-side recompute-and-validate for subtotal / tax_amount / total
    (super-review AC-2)
  * "Within one paisa is agreement" epsilon comparison for the client-
    supplied-vs-server-recomputed diff decision (bead A.2)

Pure-stdlib module (Decimal only), so this test file can run in bare env
without pulling services.Base_Service / google.oauth2 / firebase — no
AST-lift needed. Kept alongside the sanitizer / migration-gap / parse-
content siblings for the mayor-side verify pass.

Run:
    python3 -m unittest test_money_utils
    python3 -m pytest test_money_utils.py
"""

from __future__ import annotations

import os
import sys
import unittest
from decimal import Decimal


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


try:
    from services.money_utils import (
        money,
        q2,
        q4,
        within_epsilon,
        sum_money,
        recompute_invoice_totals,
        MoneyError,
        DEFAULT_EPSILON,
    )
    _IMPORT_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _IMPORT_OK = False
    _SKIP_REASON = f"import chain unavailable: {_exc}"


class TestImportChannel(unittest.TestCase):
    """Un-decorated guard — fails LOUD if the module import breaks (no
    Base_Service chain here, so this really is just "does the file
    parse")."""

    def test_import_channel_is_live(self):
        self.assertTrue(_IMPORT_OK, _SKIP_REASON)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestMoneyCoercion(unittest.TestCase):
    """`money(x)` is the single ingress. Everything past this point is
    Decimal; anything ambiguous / lossy / non-numeric must raise instead
    of silently producing wrong money."""

    def test_int_passes(self):
        self.assertEqual(money(100), Decimal("100"))

    def test_str_passes(self):
        self.assertEqual(money("123.45"), Decimal("123.45"))
        self.assertEqual(money("  123.45  "), Decimal("123.45"))  # trims

    def test_decimal_passes(self):
        self.assertEqual(money(Decimal("83.25")), Decimal("83.25"))

    def test_float_via_str_avoids_ieee_residue(self):
        """`Decimal(0.1)` produces the IEEE 754 binary residue
        (`0.10000000000000000555…`). `Decimal(str(0.1))` produces the
        human-visible value. The invariant: money(0.1) == money("0.1")
        — otherwise 0.1 + 0.2 sums lie."""
        self.assertEqual(money(0.1), money("0.1"))
        self.assertEqual(money(0.1) + money(0.2), Decimal("0.3"))

    def test_none_raises(self):
        """None is a caller error (missing field), NOT "assume zero" —
        the recompute pass fills omitted client fields explicitly and
        must not conflate absent-vs-zero."""
        with self.assertRaises(MoneyError):
            money(None)

    def test_empty_string_raises(self):
        with self.assertRaises(MoneyError):
            money("")
        with self.assertRaises(MoneyError):
            money("   ")

    def test_bool_rejected(self):
        """bool is int-subclass in Python; True → Decimal(1) would be a
        semantic bug on a money field. Explicit rejection."""
        with self.assertRaises(MoneyError):
            money(True)
        with self.assertRaises(MoneyError):
            money(False)

    def test_non_numeric_str_raises(self):
        with self.assertRaises(MoneyError):
            money("abc")

    def test_unsupported_type_raises(self):
        with self.assertRaises(MoneyError):
            money([1, 2, 3])
        with self.assertRaises(MoneyError):
            money({"amount": 100})


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestQuantization(unittest.TestCase):
    """q2 (money, 2dp) and q4 (fx, 4dp) — the storage / display quantize
    step. Both use accounting-style ROUND_HALF_UP so ₹0.005 → ₹0.01 and
    not banker's-rounding ₹0.00."""

    def test_q2_rounds_half_up(self):
        # ₹1.005 → ₹1.01 (accounting), NOT ₹1.00 (banker's).
        self.assertEqual(q2("1.005"), Decimal("1.01"))
        self.assertEqual(q2("1.015"), Decimal("1.02"))

    def test_q2_already_2dp_stable(self):
        self.assertEqual(q2("123.45"), Decimal("123.45"))

    def test_q2_less_than_2dp_pads(self):
        self.assertEqual(q2("100"), Decimal("100.00"))

    def test_q2_more_than_2dp_truncates_up(self):
        self.assertEqual(q2("1.2345"), Decimal("1.23"))
        self.assertEqual(q2("1.2355"), Decimal("1.24"))

    def test_q2_accepts_ingress_types(self):
        # q2 should coerce non-Decimal input via money().
        self.assertEqual(q2(1), Decimal("1.00"))
        self.assertEqual(q2("1.5"), Decimal("1.50"))
        self.assertEqual(q2(1.5), Decimal("1.50"))

    def test_q4_rounds_half_up(self):
        # FX rate quantization: 83.12345 → 83.1235 (round up on 5).
        self.assertEqual(q4("83.12345"), Decimal("83.1235"))

    def test_q4_already_4dp_stable(self):
        self.assertEqual(q4("83.2500"), Decimal("83.2500"))

    def test_q4_pads_short(self):
        self.assertEqual(q4("83"), Decimal("83.0000"))


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestWithinEpsilon(unittest.TestCase):
    """The client-supplied-vs-server-recomputed comparison. Default
    epsilon is ₹0.01 (one paisa). Values within → agreement (no
    overwrite / no warning); outside → overwrite with server value."""

    def test_exact_match(self):
        self.assertTrue(within_epsilon("100.00", "100.00"))

    def test_within_default_epsilon(self):
        # ₹0.005 diff → within ₹0.01 epsilon.
        self.assertTrue(within_epsilon("100.00", "100.005"))
        self.assertTrue(within_epsilon("100.00", "100.01"))
        self.assertTrue(within_epsilon("100.01", "100.00"))

    def test_outside_default_epsilon(self):
        # ₹0.02 diff → outside ₹0.01 epsilon.
        self.assertFalse(within_epsilon("100.00", "100.02"))

    def test_symmetric(self):
        """|a - b| means the check must be direction-agnostic — a client
        low by ₹0.02 must reject the same way as high by ₹0.02."""
        self.assertFalse(within_epsilon("100.00", "99.98"))
        self.assertFalse(within_epsilon("99.98", "100.00"))

    def test_custom_epsilon(self):
        # Caller can pass a tighter or looser tolerance.
        self.assertTrue(
            within_epsilon("100.00", "100.005", epsilon=Decimal("0.005"))
        )
        # Exactly at the boundary: `<=` — inclusive.
        self.assertTrue(
            within_epsilon("100.00", "100.005", epsilon=Decimal("0.005"))
        )

    def test_default_epsilon_is_one_paisa(self):
        """Sanity check on the constant — ₹0.01."""
        self.assertEqual(DEFAULT_EPSILON, Decimal("0.01"))


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestSumMoney(unittest.TestCase):
    """`sum_money` seeds a Decimal("0") so the result is always
    Decimal. Bad values propagate as MoneyError rather than contaminating
    the running total silently."""

    def test_empty_iterable_returns_zero(self):
        self.assertEqual(sum_money([]), Decimal("0"))

    def test_basic_sum(self):
        self.assertEqual(
            sum_money([Decimal("10.00"), Decimal("20.00"), Decimal("30.00")]),
            Decimal("60.00"),
        )

    def test_mixed_types_coerced(self):
        # int + str + float + Decimal all promote to Decimal via money().
        self.assertEqual(sum_money([10, "20.50", 30.25]), Decimal("60.75"))

    def test_none_in_iterable_raises(self):
        """A None mid-stream is caller error — must NOT silently ignore."""
        with self.assertRaises(MoneyError):
            sum_money([Decimal("10.00"), None, Decimal("30.00")])


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestRecomputeInvoiceTotals(unittest.TestCase):
    """The heart of AC-2 — server-side recompute of subtotal / tax_amount
    / total from line items. Every case here corresponds to a real
    client shape the write path receives (FE payload, LLM tool call,
    mail-pipeline extract).

    Invariant: subtotal + tax_amount == total, ALWAYS, for the returned
    triple. If a case violates this, the recompute pass is broken.
    """

    def test_no_items_zero_totals(self):
        sub, tax, tot = recompute_invoice_totals([], tax_rate=0)
        self.assertEqual(sub, Decimal("0.00"))
        self.assertEqual(tax, Decimal("0.00"))
        self.assertEqual(tot, Decimal("0.00"))

    def test_single_item_no_tax(self):
        items = [{"quantity": 2, "rate": 100}]
        sub, tax, tot = recompute_invoice_totals(items, tax_rate=0)
        self.assertEqual(sub, Decimal("200.00"))
        self.assertEqual(tax, Decimal("0.00"))
        self.assertEqual(tot, Decimal("200.00"))

    def test_gst_18_percent(self):
        # 1 × 1000 = 1000 subtotal, 18% GST = 180, total = 1180.
        items = [{"quantity": 1, "rate": 1000}]
        sub, tax, tot = recompute_invoice_totals(items, tax_rate=18)
        self.assertEqual(sub, Decimal("1000.00"))
        self.assertEqual(tax, Decimal("180.00"))
        self.assertEqual(tot, Decimal("1180.00"))

    def test_multi_item(self):
        items = [
            {"quantity": 2, "rate": 100},   # 200
            {"quantity": 3, "rate": 50},    # 150
            {"quantity": 1, "rate": 25.50}, # 25.50
        ]
        sub, tax, tot = recompute_invoice_totals(items, tax_rate=0)
        self.assertEqual(sub, Decimal("375.50"))
        self.assertEqual(tot, Decimal("375.50"))

    def test_fractional_qty_and_rate(self):
        # Freelance timesheet: 1.5h × ₹499.99 = ₹749.985 → q2 → ₹749.99.
        items = [{"quantity": "1.5", "rate": "499.99"}]
        sub, _, _ = recompute_invoice_totals(items, tax_rate=0)
        self.assertEqual(sub, Decimal("749.99"))

    def test_tax_rate_none_treated_as_zero(self):
        """`None` for tax_rate is the "no tax" case — must not raise."""
        items = [{"quantity": 1, "rate": 100}]
        sub, tax, tot = recompute_invoice_totals(items, tax_rate=None)
        self.assertEqual(tax, Decimal("0.00"))
        self.assertEqual(tot, Decimal("100.00"))

    def test_missing_qty_raises(self):
        """Client payload with an item missing `quantity` must fail
        loudly — recompute won't silently assume 1."""
        with self.assertRaises(MoneyError):
            recompute_invoice_totals([{"rate": 100}], tax_rate=0)

    def test_missing_rate_raises(self):
        with self.assertRaises(MoneyError):
            recompute_invoice_totals([{"quantity": 1}], tax_rate=0)

    def test_orm_object_items_supported(self):
        """The recompute helper is also used inside status-recompute
        paths where items are already ORM InvoiceItem rows. Attribute-
        access fallback covers that case without a dict conversion."""
        class _StubItem:
            def __init__(self, quantity, rate):
                self.quantity = quantity
                self.rate = rate

        items = [_StubItem(2, 100), _StubItem(1, 50)]
        sub, _, _ = recompute_invoice_totals(items, tax_rate=0)
        self.assertEqual(sub, Decimal("250.00"))

    def test_returned_triple_satisfies_subtotal_plus_tax_equals_total(self):
        """The invariant. If this ever fails, the write path can produce
        invoices with total ≠ subtotal + tax_amount — the exact bug
        AC-2 flagged."""
        items = [{"quantity": 3, "rate": "33.33"}]
        sub, tax, tot = recompute_invoice_totals(items, tax_rate="12.5")
        self.assertEqual(sub + tax, tot)

    def test_high_precision_preserved_through_quantize(self):
        """Rate with more than 2dp precision (e.g. sub-rupee per-unit
        pricing) still produces a correctly-quantized 2dp subtotal.
        Sub-paisa fractions round HALF_UP."""
        items = [{"quantity": "100", "rate": "0.005"}]
        # 100 × 0.005 = 0.500 → q2 → 0.50 (exactly, no rounding needed)
        sub, _, _ = recompute_invoice_totals(items, tax_rate=0)
        self.assertEqual(sub, Decimal("0.50"))

    def test_tax_produces_2dp_rounded_up(self):
        """A tax computation that lands on 0.005 must round UP to 0.01
        (accounting convention) — the same reason q2 uses ROUND_HALF_UP."""
        items = [{"quantity": 1, "rate": "0.10"}]
        # 0.10 × 5% = 0.005 → q2 → 0.01 (HALF_UP)
        _, tax, _ = recompute_invoice_totals(items, tax_rate=5)
        self.assertEqual(tax, Decimal("0.01"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
