"""ak-dxh0 (P1, ACCOUNTING): dedicated Record-payment endpoint for invoices.

POST /freelance/invoices/<id>/payments {amount, currency, receivedDate,
inrAmount?, method?, note?, nonce} APPENDS a payment (the legacy `payment`
on PUT replaces the single one), uid-scoped, idempotent per client nonce,
and recomputes paid / partially_paid / overdue server-side through the
existing _recompute_invoice_status (same-currency compare, else the ak-bvku
cross-currency rules).

Real InvoiceService on in-memory sqlite (the ak-lvu integration boot); only
the external FX API is stubbed. Synthetic data only.
"""
from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import test_invoice_service_integration as I  # noqa: E402  (installs import shims)

if I._IMPORT_OK:
    from flask import g
    from models.freelance_management import InvoicePayment


def _today():
    return datetime.utcnow().date()


@unittest.skipUnless(I._IMPORT_OK, I._SKIP_REASON)
class TestRecordPayment(unittest.TestCase):

    def setUp(self):
        from services.invoiceService import InvoiceService
        InvoiceService._instance = None
        self.app, self.db, self.ctx = I._boot_integration_env()
        from models.Base import Base
        from models.freelance_management import InvoicePaymentNonce
        Base.metadata.create_all(bind=self.db.engine, tables=[InvoicePaymentNonce.__table__])
        g.firebase_id = 'TEST_USER'
        self.svc = InvoiceService()
        self.svc.currency_service = I._StubCurrencyService()
        from services.invoiceService import PaymentError
        self.PaymentError = PaymentError

    def tearDown(self):
        self.ctx.pop()
        from services.invoiceService import InvoiceService
        InvoiceService._instance = None

    def _invoice(self, number="INV-DXH0-1", currency="USD", total=100, status="sent",
                 due_days=30):
        due = (_today() + timedelta(days=due_days)).isoformat()
        out = self.svc.create_invoice({
            "invoiceNumber": number, "projectName": "dxh0", "issueDate": "2026-11-01",
            "dueDate": due,
            "from": {"name": "V", "email": "v@example.com", "address": "..."},
            "to": {"name": "C", "email": "c@example.com", "address": "..."},
            "currency": currency,
            "items": [{"description": "work", "quantity": 1, "rate": total}],
            "tax": {"rate": 0, "amount": 0}, "subtotal": total, "total": total,
            "status": status,
        })
        return out

    def _pay(self, ref, nonce, amount, currency, received=None, **extra):
        body = {"nonce": nonce, "amount": amount, "currency": currency,
                "receivedDate": (received or _today()).isoformat()}
        body.update(extra)
        return self.svc.record_payment(ref, body)

    def _err(self, ref, body):
        with self.assertRaises(self.PaymentError) as cm:
            self.svc.record_payment(ref, body)
        return cm.exception.code, cm.exception.status

    def _payment_count(self):
        self.db.session.expire_all()
        return self.db.session.query(InvoicePayment).count()

    # ── partial then full ─────────────────────────────────────────────
    def test_partial_then_full_payment_same_currency(self):
        inv = self._invoice()
        body, code = self._pay(inv["invoiceNumber"], "nonce-0001", 40, "USD")
        self.assertEqual((code, body["duplicate"]), (201, False))
        self.assertEqual(body["invoice"]["status"], "partially_paid")
        self.assertEqual(body["payment"]["inrAmount"], 3330.0)          # 40 x 83.25
        body, code = self._pay(inv["invoiceNumber"], "nonce-0002", 60, "USD")
        self.assertEqual((code, body["invoice"]["status"]), (201, "paid"))
        self.assertEqual(self._payment_count(), 2)                       # appended, not replaced
        self.assertEqual(body["invoice"]["totalPaidINR"], 8325.0)
        self.assertEqual([p["amount"] for p in body["invoice"]["payments"]], [40.0, 60.0])

    def test_inr_invoice_partial_then_full(self):
        inv = self._invoice(number="INV-DXH0-INR", currency="INR", total=50000)
        self.assertEqual(self._pay(inv["invoiceNumber"], "nonce-inr-1", 20000, "INR")[0]["invoice"]["status"],
                         "partially_paid")
        from models.freelance_management import Invoice
        uuid = self.db.session.query(Invoice).filter_by(
            invoice_number="INV-DXH0-INR").one().id
        body, _ = self._pay(uuid, "nonce-inr-2", 30000, "INR")           # by invoice uuid
        self.assertEqual(body["invoice"]["status"], "paid")
        self.assertEqual(body["payment"]["fxRateSource"], "identity")

    def test_draft_receiving_money_is_not_left_draft(self):
        inv = self._invoice(number="INV-DXH0-DRAFT", status="draft")
        body, _ = self._pay(inv["invoiceNumber"], "nonce-draft", 100, "USD")
        self.assertEqual(body["invoice"]["status"], "paid")

    def test_overdue_partial_becomes_partially_paid(self):
        inv = self._invoice(number="INV-DXH0-OD", due_days=-5)
        body, _ = self._pay(inv["invoiceNumber"], "nonce-od-01", 10, "USD")
        self.assertEqual(body["invoice"]["status"], "partially_paid")

    # ── cross-currency ────────────────────────────────────────────────
    def test_cross_currency_explicit_inr_is_authoritative(self):
        # USD invoice, the bank credited INR 8,300 for $100 (user-supplied).
        inv = self._invoice(number="INV-DXH0-X1")
        body, code = self._pay(inv["invoiceNumber"], "nonce-x1-01", 100, "USD",
                               received=_today() - timedelta(days=20), inrAmount=8300)
        p = body["payment"]
        self.assertEqual((code, p["inrAmount"], p["fxRateSource"], p["fxApproximate"]),
                         (201, 8300.0, "user-supplied", False))
        self.assertEqual(p["fxRate"], 83.0)
        self.assertEqual(body["invoice"]["status"], "paid")             # USD 100 of 100

    def test_cross_currency_today_rate_for_past_date_is_flagged_approximate(self):
        inv = self._invoice(number="INV-DXH0-X2")
        body, _ = self._pay(inv["invoiceNumber"], "nonce-x2-01", 50, "USD",
                            received=_today() - timedelta(days=10))
        p = body["payment"]
        self.assertTrue(p["fxApproximate"])
        self.assertEqual((p["inrAmount"], p["fxRateSource"]), (4162.5, "test-stub:live"))
        body, _ = self._pay(inv["invoiceNumber"], "nonce-x2-02", 50, "USD")
        self.assertFalse(body["payment"]["fxApproximate"])               # received today

    def test_usd_invoice_paid_in_inr_uses_bvku_rules(self):
        # Payment currency != invoice currency -> ak-bvku cross-currency
        # branch: today's-rate compare (8325 INR covers $100 @ 83.25).
        inv = self._invoice(number="INV-DXH0-X3")
        body, _ = self._pay(inv["invoiceNumber"], "nonce-x3-01", 4000, "INR")
        self.assertEqual(body["invoice"]["status"], "partially_paid")
        body, _ = self._pay(inv["invoiceNumber"], "nonce-x3-02", 4325, "INR")
        self.assertEqual(body["invoice"]["status"], "paid")

    def test_fx_outage_fails_closed(self):
        from services.currencyService import CurrencyUnavailableError

        def down(amount, currency, *, allow_fallback=False):
            # Like the real service: a stale vintage rate only on opt-in.
            if not allow_fallback:
                raise CurrencyUnavailableError("down")
            return {"inr_amount": Decimal("1"), "fx_rate": Decimal("0.1"),
                    "fx_rate_source": "fallback:2024", "converted_at": datetime.utcnow()}
        inv = self._invoice(number="INV-DXH0-FX")
        self.svc.currency_service.convert_to_inr_with_source = down
        code = self._err(inv["invoiceNumber"], {"nonce": "nonce-fx-01", "amount": 10,
                                                "currency": "USD",
                                                "receivedDate": _today().isoformat()})
        self.assertEqual(code, ("FX_UNAVAILABLE", 503))
        self.assertEqual(self._payment_count(), 0)
        # The INR amount received still records it.
        body, code = self._pay(inv["invoiceNumber"], "nonce-fx-01", 10, "USD", inrAmount=832)
        self.assertEqual(code, 201)

    # ── idempotency ───────────────────────────────────────────────────
    def test_duplicate_nonce_returns_the_same_payment_once(self):
        inv = self._invoice(number="INV-DXH0-DUP")
        first, code1 = self._pay(inv["invoiceNumber"], "nonce-dup-01", 40, "USD")
        again, code2 = self._pay(inv["invoiceNumber"], "nonce-dup-01", 40, "USD")
        self.assertEqual((code1, code2, again["duplicate"]), (201, 200, True))
        self.assertEqual(again["payment"]["id"], first["payment"]["id"])
        self.assertEqual(self._payment_count(), 1)
        self.assertEqual(again["invoice"]["status"], "partially_paid")
        self.assertEqual(again["invoice"]["totalPaidINR"], 3330.0)       # not double-counted

    def test_reused_nonce_with_a_different_payload_is_409(self):
        inv = self._invoice(number="INV-DXH0-REUSE")
        self._pay(inv["invoiceNumber"], "nonce-re-001", 40, "USD")
        code = self._err(inv["invoiceNumber"], {"nonce": "nonce-re-001", "amount": 41,
                                                "currency": "USD",
                                                "receivedDate": _today().isoformat()})
        self.assertEqual(code, ("NONCE_REUSED", 409))
        self.assertEqual(self._payment_count(), 1)

    def test_internal_keys_are_not_exposed(self):
        inv = self._invoice(number="INV-DXH0-KEYS")
        body, _ = self._pay(inv["invoiceNumber"], "nonce-keys-1", 100, "USD")
        self.assertEqual(body["payment"]["breakdown"], {})
        self.assertEqual(body["invoice"]["payment"]["breakdown"], {})

    # ── scoping + validation ──────────────────────────────────────────
    def test_other_users_invoice_is_404(self):
        inv = self._invoice(number="INV-DXH0-UID")
        g.firebase_id = 'SOMEONE_ELSE'
        self.assertEqual(self._err(inv["invoiceNumber"], {"nonce": "nonce-uid-01", "amount": 1,
                                                          "currency": "USD",
                                                          "receivedDate": _today().isoformat()}),
                         ("NOT_FOUND", 404))
        g.firebase_id = 'TEST_USER'
        self.assertEqual(self._payment_count(), 0)

    def test_validation(self):
        inv = self._invoice(number="INV-DXH0-VAL")
        ref = inv["invoiceNumber"]
        ok = {"nonce": "nonce-val-01", "amount": 10, "currency": "USD",
              "receivedDate": _today().isoformat()}
        bad = [
            dict(ok, nonce="short"),
            dict(ok, amount=0),
            dict(ok, amount=-5),
            dict(ok, amount="abc"),
            dict(ok, currency="US"),
            dict(ok, receivedDate="24/11/2026"),
            dict(ok, receivedDate=(_today() + timedelta(days=1)).isoformat()),
            dict(ok, inrAmount=0),
            dict(ok, currency="INR", inrAmount=11),
            dict(ok, method="x" * 101),
        ]
        for b in bad:
            with self.subTest(b=b):
                self.assertEqual(self._err(ref, b)[1], 400)
        self.assertEqual(self._payment_count(), 0)
        self.assertEqual(self._err(ref, dict(ok, currency="XYZ")), ("VALIDATION", 400))

    def test_legacy_put_cannot_replace_one_of_several_payments(self):
        inv = self._invoice(number="INV-DXH0-PUT")
        self._pay(inv["invoiceNumber"], "nonce-put-01", 40, "USD")
        self._pay(inv["invoiceNumber"], "nonce-put-02", 20, "USD")
        with self.assertRaises(ValueError):
            self.svc.update_invoice(inv["invoiceNumber"], {
                "payment": {"originalAmount": 10, "originalCurrency": "USD"}})
        self.assertEqual(self._payment_count(), 2)

    def test_fe_whole_invoice_put_echo_is_a_no_op(self):
        # The FE PUTs the whole invoice (legacy `payment` echo included) on
        # every save / status change: must keep working with 2+ payments.
        inv = self._invoice(number="INV-DXH0-ECHO")
        self._pay(inv["invoiceNumber"], "nonce-echo-01", 40, "USD")
        self._pay(inv["invoiceNumber"], "nonce-echo-02", 20, "USD", inrAmount=1700)
        current = self.svc.get_invoice_by_number(inv["invoiceNumber"])
        out = self.svc.update_invoice(inv["invoiceNumber"], dict(current, projectName="renamed"))
        self.assertEqual(out["projectName"], "renamed")
        self.assertEqual(self._payment_count(), 2)
        self.assertEqual(out["status"], "partially_paid")              # 60 of 100
        self.assertEqual(out["totalPaidINR"], 3330.0 + 1700.0)
        # Legacy-alias-only echo (old tabs) is a no-op too.
        legacy = {"amountReceived": current["payment"]["amountReceived"]}
        self.svc.update_invoice(inv["invoiceNumber"], {"payment": legacy})
        self.assertEqual(self._payment_count(), 2)

    def test_legacy_echo_hides_internal_source_suffix(self):
        inv = self._invoice(number="INV-DXH0-SRC")
        self._pay(inv["invoiceNumber"], "nonce-src-01", 10, "USD",
                  received=_today() - timedelta(days=3))
        out = self.svc.get_invoice_by_number(inv["invoiceNumber"])
        self.assertNotIn("|", out["payment"]["fxRateSource"] or "")

    # ── review fixes ──────────────────────────────────────────────────
    def test_single_payment_echo_put_does_not_reopen_the_nonce(self):
        # Reviewer probe P4: POST nonce A -> whole-invoice PUT echoing the
        # payment (breakdown {} since internal keys are hidden) -> retry A.
        inv = self._invoice(number="INV-DXH0-P4", total=1000)
        body, _ = self._pay(inv["invoiceNumber"], "nonce-p4-a", 400, "USD")
        self.svc.update_invoice(inv["invoiceNumber"], {"notes": "x",
                                                       "payment": body["invoice"]["payment"]})
        again, code = self._pay(inv["invoiceNumber"], "nonce-p4-a", 400, "USD")
        self.assertEqual((code, again["duplicate"]), (200, True))
        self.assertEqual(self._payment_count(), 1)
        self.assertEqual(again["invoice"]["status"], "partially_paid")   # 400 of 1000

    def test_breakdown_echo_keeps_internal_keys(self):
        from services.invoiceService import _merge_breakdown
        self.assertEqual(_merge_breakdown({"_k": 1, "fee": 2}, {}), {"_k": 1})
        self.assertEqual(_merge_breakdown({"_k": 1}, {"fee": 3, "_k": 9}), {"fee": 3, "_k": 1})
        self.assertEqual(_merge_breakdown({"fee": 2}, {"fee": 3}), {"fee": 3})
        self.assertIsNone(_merge_breakdown(None, None))

    def test_nonce_is_unique_per_invoice_in_the_db(self):
        from sqlalchemy.exc import IntegrityError
        from models.freelance_management import InvoicePaymentNonce
        inv = self._invoice(number="INV-DXH0-UQ")
        self._pay(inv["invoiceNumber"], "nonce-uq-001", 10, "USD")
        seen = self.db.session.query(InvoicePaymentNonce).one()
        self.db.session.add(InvoicePaymentNonce(invoice_id=seen.invoice_id, nonce="nonce-uq-001",
                                                fingerprint="x"))
        with self.assertRaises(IntegrityError):
            self.db.session.flush()
        self.db.session.rollback()

    def test_multi_payment_echo_that_edits_details_is_409(self):
        # Reviewer probe P5 + minors 1/2: a matched echo that changes the
        # date / method / note is not silently dropped: 409.
        inv = self._invoice(number="INV-DXH0-P5", total=1000)
        self._pay(inv["invoiceNumber"], "nonce-p5-a", 400, "USD")
        body, _ = self._pay(inv["invoiceNumber"], "nonce-p5-b", 100, "USD")
        for change in ({"paymentDate": "2020-01-01"}, {"paymentMethod": "EVIL"},
                       {"notes": "changed"}, {"breakdown": {"fee": 1}}):
            with self.subTest(change=change):
                echo = dict(body["invoice"]["payment"], **change)
                with self.assertRaises(self.PaymentError) as cm:
                    self.svc.update_invoice(inv["invoiceNumber"], {"payment": echo})
                self.assertEqual((cm.exception.code, cm.exception.status),
                                 ("PAYMENTS_EDIT_CONFLICT", 409))
        # INR alias + a conflicting currency is not an echo either.
        alias = {"amountReceived": body["invoice"]["payments"][0]["inrAmount"],
                 "originalCurrency": "EUR"}
        with self.assertRaises(self.PaymentError):
            self.svc.update_invoice(inv["invoiceNumber"], {"payment": alias})
        self.db.session.expire_all()
        dates = sorted(str(p.payment_date) for p in self.db.session.query(InvoicePayment).all())
        self.assertNotIn("2020-01-01", dates)
        self.assertEqual(self._payment_count(), 2)

    def _add_legacy_payment(self, number, amount_received):
        """A pre-ak-lvu row: only amount_received (INR), no FX metadata."""
        from models.freelance_management import Invoice
        import datetime as _dt
        inv = self.db.session.query(Invoice).filter_by(invoice_number=number).one()
        self.db.session.add(InvoicePayment(
            invoice_id=inv.id, amount_received=Decimal(str(amount_received)),
            payment_method="bank_transfer", payment_date=_dt.date(2025, 6, 1),
            created_at=_dt.datetime(2025, 6, 1)))
        self.db.session.commit()

    def test_legacy_row_plus_new_payment_whole_invoice_put_still_saves(self):
        # Review MAJOR (2nd): an old partially paid invoice (legacy row)
        # gets its balance via POST; a later whole-invoice save that edits
        # the invoice notes must not 409.
        inv = self._invoice(number="INV-DXH0-LEG", total=100)
        self._add_legacy_payment("INV-DXH0-LEG", 4000)
        self._pay(inv["invoiceNumber"], "nonce-leg-01", 50, "USD")
        current = self.svc.get_invoice_by_number(inv["invoiceNumber"])
        for echo in [current["payment"]] + current["payments"]:
            with self.subTest(echo=echo.get("id")):
                body = dict(current, notes="edited", payment=echo)
                out = self.svc.update_invoice(inv["invoiceNumber"], body)
                self.assertEqual(out["notes"], "edited")
                self.assertEqual(self._payment_count(), 2)
        # A real change to the legacy row is still refused.
        legacy_echo = next(p for p in current["payments"] if p["fxRate"] is None) \
            if any(p["fxRate"] is None for p in current["payments"]) else current["payment"]
        changed = {"id": legacy_echo["id"], "amountReceived": 1}
        with self.assertRaises(self.PaymentError):
            self.svc.update_invoice(inv["invoiceNumber"], {"payment": changed})

    def test_update_controller_maps_payment_error_status(self):
        src = open(os.path.join(HERE, "controllers", "invoiceEP.py")).read()
        upd = src.split("def update_invoice")[1].split("def delete_invoice")[0]
        self.assertIn("except PaymentError as e:", upd)
        self.assertLess(upd.index("except PaymentError"), upd.index("except ValueError"))

    def test_route_is_registered(self):
        src = open(os.path.join(HERE, "app.py")).read()
        self.assertIn("('/freelance/invoices/<invoiceId>/payments', 'POST', "
                      "self.invoiceEP.record_payment)", src)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
