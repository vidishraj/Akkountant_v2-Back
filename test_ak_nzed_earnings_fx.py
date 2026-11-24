"""ak-nzed (2026-11-24): INR earnings must never be silently understated.

earningsService used to `except: continue/pass` when an unpaid invoice's
FX conversion failed (or no currency service was passed), so that
invoice vanished from the INR earnings / outstanding / per-client totals.
Now (mirroring invoiceService's total_inr=None → balance unknown):
  * the total is None (UNKNOWN) when any invoice couldn't be converted,
    with the known part, the unconverted count and currencies alongside;
  * a vintage fallback rate is used on the read path but flagged
    approximate with its vintage;
  * the dead legacy currencyService.convert_to_inr / 1:1 fallback is gone.
Real functions against an in-memory SQLite fixture. Synthetic data only.
"""
from __future__ import annotations

import os
import sys
import unittest
from datetime import date
from decimal import Decimal
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

try:
    import models  # noqa: F401  (mapper config: User → InvestmentHistory)
    from models import investmentHistory  # noqa: F401
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    from services import earningsService as E
    from models.freelance_management import (
        Invoice, InvoicePayment, InvoiceStatusEnum, CurrencyEnum, Customer,
    )
    _OK = True
except Exception as _exc:  # pragma: no cover
    _OK = False

D = Decimal


class FakeFx:
    """USD live at 83, EUR only via the 2024 fallback at 90, JPY fails."""

    def __init__(self):
        self.calls = []

    def convert_to_inr_with_source(self, amount, currency, *, allow_fallback=False):
        self.calls.append((currency, allow_fallback))
        if currency == "USD":
            return {"inr_amount": (D(amount) * 83).quantize(D("0.01")),
                    "fx_rate_source": "exchangerate-api-v6:live"}
        if currency == "EUR":
            return {"inr_amount": (D(amount) * 90).quantize(D("0.01")),
                    "fx_rate_source": "fallback:2024-Q4"}
        raise RuntimeError("rate API down")


@unittest.skipUnless(_OK, "freelance deps not importable")
class TestEarningsFx(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        for t in (Customer.__table__, Invoice.__table__, InvoicePayment.__table__):
            t.create(self.engine)
        self.SF = scoped_session(sessionmaker(bind=self.engine))
        self.s = self.SF()
        self.n = 0

    def tearDown(self):
        self.SF.remove()

    def inv(self, total, currency, status, paid_inr=None, client="Acme"):
        self.n += 1
        inv = Invoice(
            invoice_number=f"INV-{self.n}", user_id="u1", project_name="p",
            total=D(total), subtotal=D(total), currency=getattr(CurrencyEnum, currency),
            status=status, issue_date=date(2026, 6, 1), due_date=date(2026, 7, 1),
            from_name="Self", from_email="s@example.com", from_address="a",
            to_name=client, to_email="c@example.com", to_address="a",
        )
        self.s.add(inv)
        self.s.flush()
        if paid_inr is not None:
            self.s.add(InvoicePayment(
                invoice_id=inv.id, amount_received=D(paid_inr), inr_amount=D(paid_inr),
                original_amount=D(paid_inr), original_currency="INR",
                payment_date=date(2026, 6, 2)))
        self.s.commit()
        return inv

    def base(self):
        self.inv("1000", "INR", InvoiceStatusEnum.paid, paid_inr="1000")
        self.inv("100", "USD", InvoiceStatusEnum.sent)              # 8,300 outstanding
        self.inv("200", "USD", InvoiceStatusEnum.partially_paid,    # 16,600 - 8,300
                 paid_inr="8300")

    def detailed(self, fx=None, **kw):
        return E.get_earnings_in_inr_detailed(self.s, "u1", include_unpaid=True,
                                             currency_service=fx, **kw)

    def test_paid_only_needs_no_fx_and_is_unchanged(self):
        self.base()
        self.inv("5000", "JPY", InvoiceStatusEnum.sent)
        total = E.get_earnings_in_inr(self.s, "u1")
        self.assertEqual(total, D("9300.00"))                       # payments only
        self.assertIsInstance(total, Decimal)

    def test_all_convertible_exact(self):
        self.base()
        d = self.detailed(FakeFx())
        self.assertEqual(d["total"], D("1000") + D("8300") + D("8300") + D("8300"))
        self.assertEqual((d["unconverted_count"], d["approximate"], d["rate_vintage"]),
                         (0, False, None))

    def test_one_failing_rate_makes_total_unknown_not_smaller(self):
        self.base()
        self.inv("5000", "JPY", InvoiceStatusEnum.overdue)
        d = self.detailed(FakeFx())
        self.assertIsNone(d["total"])                               # never a smaller number
        self.assertEqual(d["known_total"], D("25900.00"))
        self.assertEqual((d["unconverted_count"], d["unconverted_currencies"]), (1, ["JPY"]))
        self.assertIsNone(E.get_earnings_in_inr(self.s, "u1", include_unpaid=True,
                                                currency_service=FakeFx()))

    def test_no_currency_service_is_unknown_not_skipped(self):
        self.base()
        d = self.detailed(None)
        self.assertIsNone(d["total"])
        self.assertEqual((d["unconverted_count"], d["unconverted_currencies"]), (2, ["USD"]))
        self.assertEqual(d["known_total"], D("9300.00"))            # payments still counted

    def test_partially_paid_failure_keeps_payments_in_known_part(self):
        self.inv("300", "JPY", InvoiceStatusEnum.partially_paid, paid_inr="150")
        d = self.detailed(FakeFx())
        self.assertIsNone(d["total"])
        self.assertEqual(d["known_total"], D("150.00"))

    def test_fallback_rate_is_flagged_approximate(self):
        self.inv("10", "EUR", InvoiceStatusEnum.sent)
        fx = FakeFx()
        d = self.detailed(fx)
        self.assertEqual(d["total"], D("900.00"))
        self.assertEqual((d["approximate"], d["rate_vintage"]), (True, "2024-Q4"))
        self.assertTrue(all(af for _c, af in fx.calls))             # read path only

    def test_by_client_unknown_per_client(self):
        self.inv("1000", "INR", InvoiceStatusEnum.paid, paid_inr="1000", client="Beta")
        self.inv("100", "USD", InvoiceStatusEnum.sent, client="Zeta")
        self.inv("5000", "JPY", InvoiceStatusEnum.sent, client="Zeta")
        self.inv("10", "EUR", InvoiceStatusEnum.sent, client="Gamma")
        rows = {r["client"]: r for r in E.get_earnings_by_client(
            self.s, "u1", include_unpaid=True, currency_service=FakeFx())}
        self.assertIsNone(rows["Zeta"]["earnings"])
        self.assertEqual(rows["Zeta"]["known_earnings"], D("8300.00"))
        self.assertEqual(rows["Zeta"]["unconverted_currencies"], ["JPY"])
        self.assertEqual(rows["Beta"]["earnings"], D("1000.00"))
        self.assertEqual((rows["Gamma"]["earnings"], rows["Gamma"]["approximate"],
                          rows["Gamma"]["rate_vintage"]), (D("900.00"), True, "2024-Q4"))
        order = [r["client"] for r in E.get_earnings_by_client(
            self.s, "u1", include_unpaid=True, currency_service=FakeFx())]
        self.assertEqual(order, ["Zeta", "Beta", "Gamma"])          # ranked by the known part

    def test_by_client_paid_only_shape(self):
        self.base()
        r, = E.get_earnings_by_client(self.s, "u1")
        self.assertEqual((r["earnings"], r["unconverted_count"]), (D("9300.00"), 0))

    def test_warning_names_currency_not_amount(self):
        self.inv("98765", "JPY", InvoiceStatusEnum.sent)
        with mock.patch.object(E, "logger") as lg:
            self.detailed(FakeFx())
        msg = str(lg.warning.call_args)
        self.assertIn("JPY", msg)
        self.assertNotIn("98765", msg)


class TestDeadConverterRemoved(unittest.TestCase):

    def test_no_legacy_1to1_fallback(self):
        src = open(os.path.join(HERE, "services", "currencyService.py")).read()
        self.assertNotIn("def convert_to_inr(", src)
        self.assertNotIn("def _fallback_convert_to_inr(", src)
        self.assertNotIn(".get(from_currency.upper(), 1.0)", src)

    def test_dashboard_never_floats_an_unknown(self):
        src = open(os.path.join(HERE, "services", "dashboardService.py")).read()
        self.assertIn('"earnings": float(row["earnings"]) if row["earnings"] is not None else None', src)
        self.assertIn('"earningsByClientCombinedFx": earnings_by_client_combined_fx', src)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
