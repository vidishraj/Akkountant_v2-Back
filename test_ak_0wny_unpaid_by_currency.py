"""ak-0wny (2026-11-24, ACCOUNTING): Freelance "Unpaid by currency" INR is
BE-authoritative.

Before: the dashboard returned only native unpaid sums per currency and the
FE converted them with its own rate table (static fallback USD 88 vs the
BE's 83.25), so a stale / fallback rate misstated the multi-currency
unpaid total. Now earningsService.get_unpaid_by_currency converts each
currency's native unpaid sum once, reports rate / source / as-of /
approximate, and the total is None (unknown) when any currency has no rate
(ak-nzed principle). Hand-computed fixtures; synthetic data only.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

try:
    import models  # noqa: F401
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
AS_OF = datetime(2026, 11, 24, 10, 0, 0)


class FakeFx:
    """USD live/cached at 83.5000 (as of AS_OF); EUR only via the 2024-Q4
    fallback at 90.7500 (and only if allowed); GBP has no rate at all."""

    def __init__(self):
        self.calls = []

    def convert_to_inr_with_source(self, amount, currency, *, allow_fallback=False):
        self.calls.append((currency, D(amount), allow_fallback))
        if currency == "USD":
            rate = D("83.5000")
            return {"inr_amount": (D(amount) * rate).quantize(D("0.01")), "fx_rate": rate,
                    "fx_rate_source": "exchangerate-api-v6:cached:2.0h", "rate_as_of": AS_OF}
        if currency == "EUR":
            if not allow_fallback:
                raise RuntimeError("CurrencyUnavailableError")
            rate = D("90.7500")
            return {"inr_amount": (D(amount) * rate).quantize(D("0.01")), "fx_rate": rate,
                    "fx_rate_source": "fallback:2024-Q4", "rate_as_of": None}
        raise ValueError(f"Currency {currency} not supported")


@unittest.skipUnless(_OK, "freelance deps not importable")
class TestUnpaidByCurrency(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        for t in (Customer.__table__, Invoice.__table__, InvoicePayment.__table__):
            t.create(self.engine)
        self.SF = scoped_session(sessionmaker(bind=self.engine))
        self.s = self.SF()
        self.n = 0

    def tearDown(self):
        self.SF.remove()

    def inv(self, total, currency, status, payments=(), user="u1"):
        self.n += 1
        inv = Invoice(
            invoice_number=f"INV-{self.n}", user_id=user, project_name="p",
            total=D(total), subtotal=D(total), currency=getattr(CurrencyEnum, currency),
            status=status, issue_date=date(2026, 6, 1), due_date=date(2026, 7, 1),
            from_name="Self", from_email="s@example.com", from_address="a",
            to_name="Acme", to_email="c@example.com", to_address="a",
        )
        self.s.add(inv)
        self.s.flush()
        for orig_amount, orig_cur, inr in payments:
            self.s.add(InvoicePayment(
                invoice_id=inv.id, amount_received=D(inr), inr_amount=D(inr),
                original_amount=D(orig_amount), original_currency=orig_cur,
                payment_date=date(2026, 6, 2)))
        self.s.commit()
        return inv

    def run_(self, fx=None):
        return E.get_unpaid_by_currency(self.s, "u1", currency_service=fx)

    def base(self):
        S = InvoiceStatusEnum
        self.inv("50000.00", "INR", S.sent)
        self.inv("10000.00", "INR", S.overdue)
        self.inv("100.00", "USD", S.sent)
        # partially paid 250 USD, 100 USD received -> 150 outstanding
        self.inv("250.00", "USD", S.partially_paid, payments=[("100.00", "USD", "8350.00")])
        # excluded: paid, draft, other user
        self.inv("999.00", "USD", S.paid, payments=[("999.00", "USD", "83416.50")])
        self.inv("777.00", "USD", S.draft)
        self.inv("555.00", "USD", S.sent, user="someone_else")

    def test_native_amounts_and_exact_inr(self):
        self.base()
        out = self.run_(FakeFx())
        rows = {r["currency"]: r for r in out["rows"]}
        self.assertEqual(list(rows), ["INR", "USD"])
        self.assertEqual((rows["INR"]["amount"], rows["INR"]["count"]), (D("60000.00"), 2))
        self.assertEqual((rows["USD"]["amount"], rows["USD"]["count"]), (D("250.00"), 2))
        self.assertEqual(rows["INR"]["inr_amount"], D("60000.00"))
        self.assertEqual(rows["INR"]["fx_rate_source"], "identity")
        # 250.00 USD x 83.5000 = 20,875.00 INR (one rate per currency)
        self.assertEqual(rows["USD"]["inr_amount"], D("20875.00"))
        self.assertEqual(rows["USD"]["fx_rate"], D("83.5000"))
        self.assertEqual(rows["USD"]["rate_as_of"], AS_OF)
        self.assertFalse(rows["USD"]["approximate"])
        t = out["total"]
        self.assertEqual(t["total"], D("80875.00"))
        self.assertEqual(t["known_total"], D("80875.00"))
        self.assertEqual((t["unconverted_currencies"], t["approximate"], t["rate_vintage"]),
                         ([], False, None))
        self.assertEqual(t["rate_as_of"], AS_OF)

    def test_converts_per_invoice_like_the_earnings_path(self):
        # Review minor 2: per-invoice conversion, summed (same as
        # _outstanding_inr), not one conversion of the currency's sum.
        self.base()
        fx = FakeFx()
        self.run_(fx)
        self.assertEqual(sorted(fx.calls), [("USD", D("100.00"), True), ("USD", D("250.00"), True)])

    def test_missing_rate_makes_total_unknown_not_smaller(self):
        self.base()
        self.inv("40.00", "GBP", InvoiceStatusEnum.overdue)
        out = self.run_(FakeFx())
        gbp = next(r for r in out["rows"] if r["currency"] == "GBP")
        self.assertIsNone(gbp["inr_amount"])
        self.assertEqual(gbp["amount"], D("40.00"))          # native still reported
        self.assertIsNone(out["total"]["total"])             # NEVER 80,875 for this set
        self.assertEqual(out["total"]["known_total"], D("80875.00"))
        self.assertEqual(out["total"]["unconverted_currencies"], ["GBP"])

    def test_no_currency_service_is_unknown_for_foreign_but_inr_exact(self):
        self.base()
        out = self.run_(None)
        rows = {r["currency"]: r for r in out["rows"]}
        self.assertEqual(rows["INR"]["inr_amount"], D("60000.00"))
        self.assertIsNone(rows["USD"]["inr_amount"])
        self.assertIsNone(out["total"]["total"])

    def test_fallback_is_approximate_with_vintage_under_option_a(self):
        self.inv("100.00", "EUR", InvoiceStatusEnum.sent)
        with mock.patch.dict(os.environ, {E.UNPAID_FX_FALLBACK_ENV: "1"}):
            out = self.run_(FakeFx())
        row = out["rows"][0]
        self.assertEqual(row["inr_amount"], D("9075.00"))
        self.assertTrue(row["approximate"])
        self.assertIsNone(row["rate_as_of"])
        self.assertEqual(out["total"]["total"], D("9075.00"))
        self.assertTrue(out["total"]["approximate"])
        self.assertEqual(out["total"]["rate_vintage"], "2024-Q4")

    def test_option_b_switch_makes_fallback_currency_unknown(self):
        self.inv("100.00", "EUR", InvoiceStatusEnum.sent)
        with mock.patch.dict(os.environ, {E.UNPAID_FX_FALLBACK_ENV: "false"}):
            out = self.run_(FakeFx())
        self.assertIsNone(out["rows"][0]["inr_amount"])
        self.assertIsNone(out["total"]["total"])

    def test_switch_env_parsing_defaults_to_option_a(self):
        env = dict(os.environ)
        env.pop(E.UNPAID_FX_FALLBACK_ENV, None)
        with mock.patch.dict(os.environ, env, clear=True):
            self.assertIs(E.unpaid_fx_allow_fallback(), True)   # lead decision 2026-11-24
        for raw, want in (("", True), ("1", True), ("true", True), ("yes", True),
                          ("0", False), ("false", False), ("No", False), ("off", False)):
            with mock.patch.dict(os.environ, {E.UNPAID_FX_FALLBACK_ENV: raw}):
                self.assertIs(E.unpaid_fx_allow_fallback(), want, raw)

    def test_nothing_unpaid_is_known_zero(self):
        self.inv("100.00", "USD", InvoiceStatusEnum.paid, payments=[("100.00", "USD", "8350.00")])
        out = self.run_(FakeFx())
        self.assertEqual(out["rows"], [])
        self.assertEqual(out["total"], {"total": D("0.00"), "known_total": D("0.00"),
                                        "unconverted_currencies": [], "approximate": False,
                                        "rate_vintage": None, "rate_as_of": None})

    def test_cross_currency_payment_is_subtracted_review_critical(self):
        # ak-0wny review CRITICAL: a USD invoice paid IN INR. Was: nothing
        # subtracted (200 USD -> 16,700 INR, overstated). Now INR space:
        # 200 x 83.5 = 16,700 - 8,350 received = 8,350 INR = 100 USD.
        self.inv("200.00", "USD", InvoiceStatusEnum.partially_paid,
                 payments=[("8350.00", "INR", "8350.00")])
        out = self.run_(FakeFx())
        row = out["rows"][0]
        self.assertEqual((row["amount"], row["inr_amount"]), (D("100.00"), D("8350.00")))
        self.assertEqual(out["total"]["total"], D("8350.00"))

    def test_legacy_payment_row_is_subtracted(self):
        # Legacy payment: original_amount NULL, INR only in amount_received.
        inv = self.inv("300.00", "USD", InvoiceStatusEnum.partially_paid)
        self.s.add(InvoicePayment(invoice_id=inv.id, amount_received=D("8350.00"),
                                  inr_amount=None, original_amount=None, original_currency=None,
                                  payment_date=date(2026, 6, 2)))
        self.s.commit()
        out = self.run_(FakeFx())
        row = out["rows"][0]
        # 300 x 83.5 = 25,050 - 8,350 = 16,700 INR = 200 USD
        self.assertEqual((row["amount"], row["inr_amount"]), (D("200.00"), D("16700.00")))

    def test_reviewer_probe_and_reconciles_with_earnings_path(self):
        # /home/opc/data/tmp/rev-0wny/probe_0wny.py, hand-computed.
        S = InvoiceStatusEnum
        self.inv("10000.00", "INR", S.partially_paid, payments=[("4000.00", "INR", "4000.00")])
        self.inv("200.00", "USD", S.partially_paid, payments=[("50.00", "USD", "4175.00")])
        self.inv("1000.00", "USD", S.partially_paid, payments=[("41750.00", "INR", "41750.00")])
        self.inv("100.00", "EUR", S.sent)
        fx = FakeFx()
        with mock.patch.dict(os.environ, {E.UNPAID_FX_FALLBACK_ENV: "1"}):
            out = self.run_(fx)
        rows = {r["currency"]: r for r in out["rows"]}
        self.assertEqual((rows["INR"]["amount"], rows["INR"]["inr_amount"]), (D("6000.00"), D("6000.00")))
        # 150 USD (same-currency) + 500 USD (INR-paid) = 650 USD = 54,275 INR
        self.assertEqual((rows["USD"]["amount"], rows["USD"]["inr_amount"]), (D("650.00"), D("54275.00")))
        self.assertEqual((rows["EUR"]["inr_amount"], rows["EUR"]["approximate"]), (D("9075.00"), True))
        self.assertEqual(out["total"]["total"], D("69350.00"))      # was 111,100 (overstated)
        self.assertTrue(out["total"]["approximate"])
        # The two dashboard unpaid figures must agree to the paisa:
        # outstanding by the earnings path = (paid + outstanding) - paid.
        combined = E.get_earnings_in_inr_detailed(self.s, "u1", include_unpaid=True, currency_service=fx)
        paid = E.get_earnings_in_inr(self.s, "u1")
        self.assertEqual(combined["known_total"] - paid, out["total"]["total"])
        with mock.patch.dict(os.environ, {E.UNPAID_FX_FALLBACK_ENV: "0"}):
            out_b = self.run_(fx)
        self.assertIsNone(out_b["total"]["total"])
        self.assertEqual(out_b["total"]["known_total"], D("60275.00"))
        self.assertEqual(out_b["total"]["unconverted_currencies"], ["EUR"])

    def test_unconvertible_currency_with_cross_currency_payment_has_unknown_native(self):
        self.inv("40.00", "GBP", InvoiceStatusEnum.partially_paid,
                 payments=[("1000.00", "INR", "1000.00")])
        row = self.run_(FakeFx())["rows"][0]
        self.assertIsNone(row["amount"])
        self.assertIsNone(row["inr_amount"])

    def test_overpaid_partial_floors_at_zero(self):
        self.inv("100.00", "USD", InvoiceStatusEnum.partially_paid,
                 payments=[("120.00", "USD", "10020.00")])
        out = self.run_(FakeFx())
        self.assertEqual(out["rows"][0]["amount"], D("0.00"))
        self.assertEqual(out["total"]["total"], D("0.00"))

    def test_oldest_rate_as_of_reported(self):
        older = AS_OF - timedelta(hours=9)

        class TwoRates(FakeFx):
            def convert_to_inr_with_source(self, amount, currency, *, allow_fallback=False):
                r = super().convert_to_inr_with_source(amount, "USD", allow_fallback=allow_fallback)
                if currency == "AUD":
                    r = dict(r, rate_as_of=older)
                return r
        self.inv("10.00", "USD", InvoiceStatusEnum.sent)
        self.inv("10.00", "AUD", InvoiceStatusEnum.sent)
        out = self.run_(TwoRates())
        self.assertEqual(out["total"]["rate_as_of"], older)

    def test_paid_invoice_in_unrated_currency_does_not_poison_totals(self):
        # Refactor guard: _outstanding_inr must not even try to convert a
        # PAID invoice, so an unrated currency there leaves totals known.
        self.inv("100.00", "GBP", InvoiceStatusEnum.paid, payments=[("100.00", "GBP", "10550.00")])
        self.inv("100.00", "USD", InvoiceStatusEnum.sent)
        fx = FakeFx()
        d = E.get_earnings_in_inr_detailed(self.s, "u1", include_unpaid=True, currency_service=fx)
        self.assertEqual(d["total"], D("10550.00") + D("8350.00"))
        self.assertNotIn("GBP", [c for c, _a, _f in fx.calls])

    def test_user_required(self):
        with self.assertRaises(ValueError):
            E.get_unpaid_by_currency(self.s, "", currency_service=FakeFx())


class TestCurrencyServiceRateAsOf(unittest.TestCase):
    """convert_to_inr_with_source gains an additive rate_as_of."""

    def setUp(self):
        from services.currencyService import CurrencyService
        self.svc = CurrencyService.__new__(CurrencyService)
        self.svc.logger = mock.MagicMock()
        self.svc.fallback_rates_to_inr = {"USD": 83.25, "INR": 1.0}

    def test_cached_rate_as_of_is_fetch_time(self):
        with mock.patch.object(self.svc, "_get_usd_rates_with_age",
                               return_value=({"INR": 84.0, "USD": 1.0}, 3.0)):
            r = self.svc.convert_to_inr_with_source(D("10"), "USD")
        self.assertEqual(r["inr_amount"], D("840.00"))
        self.assertAlmostEqual((r["converted_at"] - r["rate_as_of"]).total_seconds(), 3 * 3600, delta=1)

    def test_live_rate_as_of_is_now(self):
        with mock.patch.object(self.svc, "_get_usd_rates_with_age",
                               return_value=({"INR": 84.0, "USD": 1.0}, 0.0)):
            r = self.svc.convert_to_inr_with_source(D("10"), "USD")
        self.assertEqual(r["rate_as_of"], r["converted_at"])

    def test_fallback_has_no_rate_as_of(self):
        with mock.patch.object(self.svc, "_get_usd_rates_with_age", side_effect=RuntimeError("down")):
            r = self.svc.convert_to_inr_with_source(D("10"), "USD", allow_fallback=True)
        self.assertEqual(r["fx_rate_source"], "fallback:2024-Q4")
        self.assertIsNone(r["rate_as_of"])
        self.assertEqual(r["inr_amount"], D("832.50"))


def _lift_dashboard_helpers():
    with open(os.path.join(HERE, "services", "dashboardService.py")) as fh:
        tree = ast.parse(fh.read())
    fns = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ("_f", "_iso_utc")]
    mod = ast.Module(body=fns, type_ignores=[])
    ast.fix_missing_locations(mod)
    ns = {}
    exec(compile(mod, "<lift:dashboard helpers>", "exec"), ns)
    return ns["_f"], ns["_iso_utc"]


class TestDashboardWiring(unittest.TestCase):

    def setUp(self):
        with open(os.path.join(HERE, "services", "dashboardService.py")) as fh:
            self.src = fh.read()

    def test_helpers(self):
        f, iso = _lift_dashboard_helpers()
        self.assertIsNone(f(None))
        self.assertEqual(f(D("20875.00")), 20875.0)
        self.assertIsNone(iso(None))
        self.assertEqual(iso(datetime(2026, 11, 24, 10, 0, 0, 123456)), "2026-11-24T10:00:00Z")

    def test_payload_fields(self):
        self.assertIn("get_unpaid_by_currency(", self.src)
        for key in ('"inrAmount"', '"fxRate"', '"fxRateSource"', '"rateAsOf"', '"approximate"',
                    '"unpaidTotalInr": unpaid_total_inr', '"knownTotal"', '"unconvertedCurrencies"',
                    '"rateVintage"'):
            self.assertIn(key, self.src)
        # Existing row keys kept (additive change only).
        for key in ('"currency": row["currency"]', '"amount": _f(row["amount"])',
                    '"count": row["count"]'):
            self.assertIn(key, self.src)

    def test_no_second_unpaid_computation_left(self):
        self.assertNotIn("unpaid_invoices = session.query(Invoice)", self.src)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
