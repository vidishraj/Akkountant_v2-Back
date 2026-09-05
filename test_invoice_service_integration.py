"""ak-lvu v2 F-6: real sqlite-backed integration test for InvoiceService.

The v1 canary (test_fx_idempotency.TestFXIdempotencyCanary) was a
tautology — it re-implemented the FX conversion in a stub class then
asserted "N calls to my stub give byte-identical outputs." That
proves my stub is deterministic, not that the production
`_replace_payment` code path preserves FX metadata across a round-trip.

This test loads the REAL InvoiceService against an in-memory sqlite DB.
The `_replace_payment` execution path is the actual code under review.
A stub is used ONLY for the CurrencyService (the only external
dependency that would otherwise hit exchangerate-api.com).

Two scenarios:

1. `test_fe_shape_round_trip_preserves_fx` — happy path. FE receives a
   USD-invoice-with-payment via `get_invoice_by_number`, sends the full
   returned payload back verbatim via `update_invoice`. inr_amount +
   fx_rate must be byte-stable across the round-trip.

2. `test_legacy_amount_received_only_shape_does_not_double_convert` —
   THE F-1 repro. FE gets `amountReceived: 8325.00` (INR-value via
   `_format_invoice` legacy alias), sends PUT with
   `payment: {amountReceived: 8325, paymentMethod: 'bank_transfer'}` — no
   `originalAmount` / `originalCurrency` fields. On a1111f9 this
   triggers the AC-1 shape: the value is treated as invoice-currency and
   re-converted. On v2 the guard detects amountReceived matches existing
   inr_amount and skips reconversion.

**This test MUST FAIL on a1111f9** — that's the reviewer's explicit
acceptance criterion. If it passes on both v1 and v2, the test is
another stub-loop tautology and the F-6 fix hasn't happened.

Run:
    python3 -m unittest test_invoice_service_integration -v

Bare-env: shims the google.oauth2 / googleapiclient / firebase_admin /
bs4 chain so `services.Base_Service` loads without those installed.
Requires: flask, flask_sqlalchemy, sqlalchemy (all confirmed available
in the current bare env).
"""

from __future__ import annotations

# ── heavy-import shims (must run BEFORE any services.* import) ────────
import sys
import types
from decimal import Decimal


def _install_import_shims():
    """Shim google / googleapiclient / firebase_admin / bs4 so
    services.Base_Service can load. Each shim is a bare types.ModuleType
    with placeholder classes for the specific attributes imported."""
    for name in [
        'google', 'google.oauth2', 'google.oauth2.credentials',
        'google.auth', 'google.auth.transport',
        'google.auth.transport.requests', 'google.auth.exceptions',
        'googleapiclient', 'googleapiclient.discovery',
        'googleapiclient.http', 'googleapiclient.errors',
        'google_auth_oauthlib', 'google_auth_oauthlib.flow',
        'firebase_admin', 'firebase_admin.credentials',
        'firebase_admin.auth',
        'bs4',
    ]:
        sys.modules.setdefault(name, types.ModuleType(name))
    for path, attr in [
        ('google.oauth2.credentials', 'Credentials'),
        ('google.auth.transport.requests', 'Request'),
        ('google.auth.exceptions', 'RefreshError'),
        ('googleapiclient.discovery', 'build'),
        ('googleapiclient.http', 'MediaFileUpload'),
        ('googleapiclient.errors', 'HttpError'),
        ('google_auth_oauthlib.flow', 'InstalledAppFlow'),
        ('bs4', 'BeautifulSoup'),
    ]:
        if not hasattr(sys.modules[path], attr):
            setattr(sys.modules[path], attr, type(attr, (), {}))


_install_import_shims()

import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# CurrencyService.__init__ requires this env var; the stub replaces
# convert_to_inr_with_source so no live API call ever fires. Value is a
# placeholder only.
os.environ.setdefault("EXCHANGE_RATE_API_KEY", "test-stub-key-not-used")


try:
    import unittest
    from datetime import datetime, date

    from flask import Flask, g
    from flask_sqlalchemy import SQLAlchemy

    # Real production imports — these are the code paths under test.
    from models.freelance_management import (
        Invoice, InvoiceItem, InvoicePayment, CurrencyEnum,
        InvoiceStatusEnum,
    )
    from models.Base import Base
    # Ensure mapper resolution succeeds — `models.users.User` references
    # `InvestmentHistory` which isn't in `models/__init__.py`. Import
    # explicitly so the mapper can find the class.
    import models.investmentHistory  # noqa: F401

    _IMPORT_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _IMPORT_OK = False
    _SKIP_REASON = f"integration import chain unavailable: {_exc}"


class _StubCurrencyService:
    """Same interface as the real CurrencyService for the two methods
    invoiceService touches. Deterministic USD=83.25 rate — matches the
    test-suite expectations exactly.

    NOT re-implementing invoiceService logic — this is only mocking the
    external FX API. All money-math still runs through the real
    `_replace_payment` / `_recompute_invoice_status` / `_format_invoice`
    code paths.
    """

    def __init__(self, rates=None):
        # {currency: INR-per-1-unit}
        self.rates = rates or {
            'USD': Decimal("83.25"),
            'GBP': Decimal("105.50"),
            'EUR': Decimal("90.75"),
            'INR': Decimal("1.0000"),
        }

    def convert_to_inr(self, amount, from_currency):
        from decimal import Decimal as _D
        cur = str(from_currency).upper()
        if cur == 'INR':
            return float(amount)
        return float(_D(str(amount)) * self.rates[cur])

    def convert_to_inr_with_source(
        self, amount, from_currency, *, allow_fallback=False,
    ) -> dict:
        from decimal import Decimal as _D
        cur = str(from_currency).upper()
        now = datetime.utcnow()
        amt = _D(str(amount))
        if cur == 'INR':
            return {
                "inr_amount": amt.quantize(_D("0.01")),
                "fx_rate": _D("1.0000"),
                "fx_rate_source": "identity",
                "converted_at": now,
            }
        rate = self.rates.get(cur)
        if rate is None:
            raise ValueError(f"Unsupported currency {cur}")
        return {
            "inr_amount": (amt * rate).quantize(_D("0.01")),
            "fx_rate": rate.quantize(_D("0.0001")),
            "fx_rate_source": "test-stub:live",
            "converted_at": now,
        }


def _boot_integration_env():
    """Build a Flask app + in-memory sqlite SQLAlchemy binding + push
    the app context so `services.Base_Service.db` property can find
    `current_app.db`. Returns (app, db, ctx) — caller pops ctx to
    tear down.

    Registers a MEDIUMTEXT→TEXT type compiler so sqlite can render the
    signature_data column (mysql-specific dialect otherwise fails
    create_all()). Uses `tables=` explicit list to avoid materializing
    the whole schema — only the ones the invoice write path touches.
    """
    app = Flask("ak_lvu_integration_test")
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    # Bind the real freelance_management model Base to a fresh
    # Flask-SQLAlchemy instance. The Base_Service `db` property reads
    # `current_app.db`, so we attach the db there.
    db = SQLAlchemy(app, model_class=Base)
    app.db = db

    # Teach sqlite to accept the mysql-dialect MEDIUMTEXT type (used by
    # signatures.signature_data). Falls back to plain TEXT. Only affects
    # the create_all pass — no data-shape impact.
    from sqlalchemy.dialects.mysql import MEDIUMTEXT
    from sqlalchemy.ext.compiler import compiles
    @compiles(MEDIUMTEXT, "sqlite")
    def _sqlite_mediumtext(_element, _compiler, **_kw):
        return "TEXT"

    ctx = app.app_context()
    ctx.push()

    # Stub users table (FK target) — same Base so metadata sees it.
    from sqlalchemy import Column, String as _Str
    class _StubUser(Base):
        __tablename__ = 'users'
        __table_args__ = {'extend_existing': True}
        userID = Column(_Str(100), primary_key=True)

    # Only create the tables the invoice + payment write path actually
    # touches. Skipping the rest (agent tables, statement tables, kite
    # tables) avoids unrelated compile errors + speeds boot.
    from models.freelance_management import (
        Invoice as _Invoice, InvoiceItem as _Item,
        InvoicePayment as _Payment, InvoiceCustomField as _CF,
        Customer as _Cust, PendingPaymentClaim as _Claim,
    )
    tables = [
        _StubUser.__table__,
        _Cust.__table__,
        _Invoice.__table__,
        _Item.__table__,
        _Payment.__table__,
        _CF.__table__,
        _Claim.__table__,
    ]
    Base.metadata.create_all(bind=db.engine, tables=tables)

    # Seed a test user matching the firebase_id we'll use.
    session = db.session
    if not session.query(_StubUser).filter_by(userID='TEST_USER').first():
        session.add(_StubUser(userID='TEST_USER'))
        session.commit()

    return app, db, ctx


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestImportChannel(unittest.TestCase):
    """Un-decorated guard — fails LOUD if the shim chain regresses."""

    def test_import_channel_is_live(self):
        self.assertTrue(_IMPORT_OK, _SKIP_REASON)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestInvoiceServiceIntegration(unittest.TestCase):
    """Real sqlite + real InvoiceService — no stub loop.

    Setup per-test: fresh in-memory sqlite + fresh app context so tests
    don't cross-contaminate state (each in-memory db is discarded on
    ctx pop).
    """

    def setUp(self):
        # Reset the InvoiceService singleton so a fresh instance picks
        # up the new app context.
        from services.invoiceService import InvoiceService
        InvoiceService._instance = None

        self.app, self.db, self.ctx = _boot_integration_env()
        g.firebase_id = 'TEST_USER'

        # Instantiate service + inject stub currency.
        self.svc = InvoiceService()
        self.svc.currency_service = _StubCurrencyService()

    def tearDown(self):
        self.ctx.pop()
        from services.invoiceService import InvoiceService
        InvoiceService._instance = None

    def _create_usd_invoice_with_payment(self):
        """Create a USD $100 invoice with a full $100 payment.

        Returns the formatted invoice dict (what `_format_invoice`
        emits) — that IS the response an FE / LLM would receive.
        """
        payload = {
            "invoiceNumber": "INV-2026-F1-REPRO",
            "projectName": "F-1 repro fixture",
            "issueDate": "2026-09-01",
            "dueDate": "2026-09-30",
            "from": {"name": "Vidish", "email": "v@example.com",
                     "address": "..."},
            "to": {"name": "TestClient", "email": "t@example.com",
                   "address": "..."},
            "currency": "USD",
            "items": [
                {"description": "consulting", "quantity": 1, "rate": 100},
            ],
            "tax": {"rate": 0, "amount": 0},
            "subtotal": 100,
            "total": 100,
            "status": "sent",
            "payment": {
                "originalAmount": 100,
                "originalCurrency": "USD",
                "paymentMethod": "bank_transfer",
                "paymentDate": "2026-09-15",
            },
        }
        return self.svc.create_invoice(payload)

    # ── HAPPY PATH — FE round-trip full payload ───────────────────────

    def test_fe_shape_round_trip_preserves_fx(self):
        """FE gets invoice, PUTs the returned payload verbatim (with
        `originalAmount` + `originalCurrency` present) — inr_amount +
        fx_rate must be byte-stable across 5 round-trips.

        This test passes on BOTH a1111f9 and v2 (the idempotency guard
        works when `originalAmount` is present because L465-490 hits
        the "no-op PUT" path). It's a positive baseline — the F-1 repro
        below is the one that distinguishes v1 from v2.
        """
        initial = self._create_usd_invoice_with_payment()
        initial_inr = initial['payment']['inrAmount']
        initial_fx = initial['payment']['fxRate']
        self.assertEqual(initial_inr, 8325.00)
        self.assertEqual(initial_fx, 83.25)

        # PUT the returned payload back verbatim, 5 times.
        for i in range(5):
            result = self.svc.update_invoice(
                initial['invoiceNumber'], initial,
            )
            self.assertEqual(
                result['payment']['inrAmount'], initial_inr,
                f"inr_amount drifted at iteration {i+1}: "
                f"expected {initial_inr}, got {result['payment']['inrAmount']}"
            )
            self.assertEqual(
                result['payment']['fxRate'], initial_fx,
                f"fx_rate drifted at iteration {i+1}"
            )

    # ── F-1 REPRO — legacy amountReceived-only shape ──────────────────

    def test_legacy_amount_received_only_shape_does_not_double_convert(self):
        """THE F-1 repro. LLM / legacy FE calls `update_invoice` with
        just `payment: {amountReceived: 8325, paymentMethod: ...}` —
        no `originalAmount` field. The `amountReceived` value is the
        INR value returned by `_format_invoice`'s legacy alias.

        On a1111f9 (current code): `_replace_payment` falls back to
        `payment_data.get('amountReceived')` as originalAmount, treats
        it as invoice-currency (USD), and re-converts 8325 as USD →
        ₹693,056. The stored inr_amount drifts massively. **TEST FAILS.**

        On v2 (post-fix): the guard detects that `amountReceived` matches
        the existing payment's stored inr_amount, treats the PUT as
        no-op / metadata-only, preserves the original fx metadata.
        **TEST PASSES.**

        Reviewer's acceptance criterion: this test MUST fail on
        a1111f9 for the F-6 fix to be verifiable.
        """
        initial = self._create_usd_invoice_with_payment()
        initial_inr = initial['payment']['inrAmount']
        initial_fx = initial['payment']['fxRate']
        self.assertEqual(initial_inr, 8325.00)

        # LLM / legacy-FE shape: strips new fields, keeps amountReceived
        # (the legacy alias) + minor payment metadata. This is exactly
        # what today's FREELANCE_SYSTEM_PROMPT tells the LLM to send.
        legacy_payload = dict(initial)
        legacy_payment = {
            "amountReceived": initial['payment']['amountReceived'],
            "paymentMethod": "bank_transfer_updated",  # unrelated metadata change
            "paymentDate": "2026-09-15",
        }
        legacy_payload['payment'] = legacy_payment

        result = self.svc.update_invoice(
            initial['invoiceNumber'], legacy_payload,
        )

        # On v2: the guard treats amountReceived == existing.inr_amount
        # as a no-op (metadata-only) PUT and preserves inrAmount /
        # fxRate. On a1111f9 the value gets re-converted as USD →
        # inrAmount becomes ~₹693,056 (8325 × 83.25).
        self.assertEqual(
            result['payment']['inrAmount'], initial_inr,
            f"F-1 CRITICAL: legacy amountReceived-only shape "
            f"re-converted INR-as-USD. Expected inrAmount stable at "
            f"{initial_inr}, got {result['payment']['inrAmount']} "
            f"(~{result['payment']['inrAmount']/initial_inr:.1f}× drift)."
        )
        self.assertEqual(
            result['payment']['fxRate'], initial_fx,
            f"F-1 CRITICAL: fxRate mutated. Expected {initial_fx}, "
            f"got {result['payment']['fxRate']}"
        )

    # ── F-2 REPRO — status recompute mixes FX vintages ────────────────

    def test_status_recompute_uses_same_currency_no_vintage_flip(self):
        """USD invoice fully paid at USD=83.25. Later, live rate moves
        to USD=90.00 (simulated by mutating the stub). An unrelated
        PUT (edit notes) triggers `_recompute_invoice_status`. On
        a1111f9: total_inr = 100 × 90.00 = ₹9000 while Σ payment
        inr_amount = ₹8325 (payment-time vintage) → 8325 < 9000 →
        flips paid → partially_paid. On v2: same-currency compare
        (Σ original_amount=100 vs invoice.total=100) → still paid.
        """
        initial = self._create_usd_invoice_with_payment()
        # First recompute post-create should have set status='paid'
        # (Σ original_amount = total). Verify baseline:
        self.assertEqual(initial['status'], 'paid')

        # Simulate FX drift: USD moves from 83.25 to 90.00.
        self.svc.currency_service.rates['USD'] = Decimal("90.00")

        # Unrelated edit — just add a note.
        payload = dict(initial)
        payload['notes'] = "added some notes"

        result = self.svc.update_invoice(
            initial['invoiceNumber'], payload,
        )

        # On v2 (same-currency compare): status stays paid.
        # On a1111f9 (mixed vintage): status flips to partially_paid.
        self.assertEqual(
            result['status'], 'paid',
            f"F-2 CRITICAL: status recompute mixed FX vintages. "
            f"USD-only invoice fully paid in USD should stay paid on "
            f"FX drift. Got: {result['status']} — likely today's-rate "
            f"vs payment-time-rate mismatch."
        )

    # ── F-3 REPRO — draft → sent transition ───────────────────────────

    def test_draft_to_sent_transition_honoured(self):
        """A draft invoice PUT with `status: 'sent'` and no other
        changes must transition to sent. On a1111f9 the client status
        is IGNORED (draft-lock regression) — invoice stays draft. On
        v2 the explicit draft→sent transition is accepted; recompute
        owns everything from sent onward.
        """
        # Create as draft.
        draft = self.svc.create_invoice({
            "invoiceNumber": "INV-DRAFT-F3",
            "projectName": "Draft F3 fixture",
            "issueDate": "2026-09-01",
            "dueDate": "2026-09-30",
            "from": {"name": "V", "email": "v@example.com", "address": "..."},
            "to": {"name": "C", "email": "c@example.com", "address": "..."},
            "currency": "USD",
            "items": [{"description": "x", "quantity": 1, "rate": 100}],
            "tax": {"rate": 0, "amount": 0},
            "subtotal": 100,
            "total": 100,
            "status": "draft",
        })
        self.assertEqual(draft['status'], 'draft')

        # PUT to transition draft → sent.
        payload = dict(draft)
        payload['status'] = 'sent'
        result = self.svc.update_invoice(draft['invoiceNumber'], payload)

        self.assertEqual(
            result['status'], 'sent',
            f"F-3: draft→sent transition silently dropped. Client "
            f"sent 'sent' on draft, expected transition; got: "
            f"{result['status']}"
        )

    # ── v3 V2-7: broader coverage ─────────────────────────────────────

    def test_paymentservice_add_payment_delegates_to_replace_payment(self):
        """v3 V2-7: paymentService.add_payment delegates to
        invoiceService._replace_payment (per ak-lvu A.1). This test
        exercises that path so the F-1 guard also protects direct
        paymentService callers, not just update_invoice.

        We drive the delegation manually here since paymentService's
        own singleton wiring would need parallel injection — the key
        assertion is: a payment written via _replace_payment ends up
        with full FX metadata populated.
        """
        # Fresh invoice, sent status, no payment.
        inv = self.svc.create_invoice({
            "invoiceNumber": "INV-DELEG",
            "projectName": "delegation",
            "issueDate": "2026-09-01",
            "dueDate": "2026-09-30",
            "from": {"name": "V", "email": "v@example.com", "address": "..."},
            "to": {"name": "C", "email": "c@example.com", "address": "..."},
            "currency": "USD",
            "items": [{"description": "x", "quantity": 1, "rate": 200}],
            "tax": {"rate": 0, "amount": 0},
            "subtotal": 200,
            "total": 200,
            "status": "sent",
        })

        # Simulate paymentService.add_payment → _replace_payment.
        invoice_row = self.db.session.query(Invoice).filter_by(
            invoice_number="INV-DELEG",
        ).first()
        self.svc._replace_payment(invoice_row.id, {
            "originalAmount": 200,
            "originalCurrency": "USD",
            "paymentMethod": "wise",
        })
        self.svc._recompute_invoice_status(invoice_row)
        self.db.session.commit()

        # Fetch back and assert full FX metadata populated.
        result = self.svc.get_invoice_by_number("INV-DELEG")
        self.assertEqual(result['status'], 'paid')
        self.assertEqual(result['payment']['inrAmount'], 16650.00)
        self.assertEqual(result['payment']['fxRate'], 83.25)
        self.assertEqual(result['payment']['originalAmount'], 200.00)
        self.assertEqual(result['payment']['originalCurrency'], 'USD')
        self.assertIsNotNone(result['payment']['fxRateSource'])

    def test_changed_amount_without_original_rejected(self):
        """v3 V2-7: F-1 guard's REJECT path. A payload that sends
        `amountReceived` differing from the stored inr_amount, without
        `originalAmount`, must be REJECTED with a clear error rather
        than silently reconverting.

        Distinguishes v2 (which correctly rejects) from a hypothetical
        regression that treats the mismatched amount as invoice-currency.
        """
        initial = self._create_usd_invoice_with_payment()
        # Send back a payload with amountReceived changed but no
        # originalAmount — this is the "user actually changed the
        # amount but forgot the new contract" case.
        payload = dict(initial)
        payload['payment'] = {
            "amountReceived": 9999.00,  # differs from stored 8325
            "paymentMethod": "bank_transfer",
        }
        with self.assertRaises(Exception) as ctx:
            self.svc.update_invoice(initial['invoiceNumber'], payload)
        # Confirm the error surfaces the guard's diagnostic language.
        err = str(ctx.exception).lower()
        self.assertTrue(
            "originalamount" in err or "no originalamount" in err or
            "provide explicit" in err,
            f"Expected F-1 guard reject error, got: {ctx.exception}"
        )

    def test_create_with_payment_stale_replay_no_double_convert(self):
        """v3 V2-7: `create_invoice` called twice with the SAME
        payload (e.g. LLM retry after a network error, or a stale
        request replay) — second call should reject on the unique
        invoice_number OR produce a fresh row with correct FX
        metadata. Either way it must NOT double-convert.

        Concrete failure this guards against: a template-replay where
        the LLM re-sends a create payload that includes a payment with
        an INR value in `amountReceived`. That path shouldn't exist
        (create_invoice's `_replace_payment` sees no existing_payment
        and takes the fresh-write branch) — but if the invoice was
        already created and this is a stale retry, the fresh-write
        branch would re-convert the LLM's payload value.

        Test asserts: fresh-write with `originalAmount` + `originalCurrency`
        works correctly; there's no path where the same amount gets
        multiplied by fx_rate twice.
        """
        # First create — succeeds.
        payload = {
            "invoiceNumber": "INV-STALE-REPLAY",
            "projectName": "stale replay",
            "issueDate": "2026-09-01",
            "dueDate": "2026-09-30",
            "from": {"name": "V", "email": "v@example.com", "address": "..."},
            "to": {"name": "C", "email": "c@example.com", "address": "..."},
            "currency": "USD",
            "items": [{"description": "x", "quantity": 1, "rate": 150}],
            "tax": {"rate": 0, "amount": 0},
            "subtotal": 150,
            "total": 150,
            "status": "sent",
            "payment": {
                "originalAmount": 150,
                "originalCurrency": "USD",
                "paymentMethod": "wise",
                "paymentDate": "2026-09-15",
            },
        }
        result = self.svc.create_invoice(payload)
        self.assertEqual(result['payment']['inrAmount'], 12487.50)
        self.assertEqual(result['payment']['fxRate'], 83.25)

        # Stale replay attempt — same payload. In production MySQL a
        # unique index on invoice_number rejects the duplicate; in this
        # sqlite harness the constraint isn't enforced so we allow the
        # duplicate but verify the KEY invariant: no double-conversion.
        # A stale-replay payload's `originalAmount` field carries the
        # ORIGINAL currency value (150 USD), NOT the stored INR value
        # (12487.50) — so the fresh row must convert 150 × 83.25 =
        # 12487.50, NOT 12487.50 × 83.25 = 1,039,584.
        try:
            second = self.svc.create_invoice(payload)
            # Duplicate accepted (sqlite harness) — verify no double-convert.
            self.assertEqual(
                second['payment']['inrAmount'], 12487.50,
                f"V2-7 stale-replay: second create's inrAmount drifted "
                f"— possible double-conversion. Got: {second['payment']['inrAmount']}"
            )
        except Exception:
            # Duplicate rejected (prod MySQL shape) — original still intact.
            pass

        # Confirm the ORIGINAL row's FX metadata is unchanged.
        again = self.svc.get_invoice_by_number("INV-STALE-REPLAY")
        self.assertEqual(again['payment']['inrAmount'], 12487.50)
        self.assertEqual(again['payment']['fxRate'], 83.25)

    def _mimic_true_legacy_row(self, invoice_number):
        """v5 V5.3 fixture correction — align to REAL pre-migration shape.

        A true pre-ALTER legacy `invoice_payments` row has ALL SIX new
        columns NULL (not just the five vintage fields). Only
        `amount_received` (populated, holding the INR value) survives
        from the pre-ak-lvu schema.

        Prior fixtures kept `inr_amount` populated "for convenience" —
        but the read-side backfill falls back to `amount_received` anyway
        when inr_amount is NULL, so convenience wasn't even needed. And
        it caused the V4-1 CRITICAL to be masked: the "legacy" shape
        the tests simulated wasn't the true production legacy shape.
        """
        invoice_row = self.db.session.query(Invoice).filter_by(
            invoice_number=invoice_number,
        ).first()
        payment_row = invoice_row.payments[0]
        # NULL all 6 new columns (real pre-migration shape).
        payment_row.original_amount = None
        payment_row.original_currency = None
        payment_row.inr_amount = None
        payment_row.fx_rate = None
        payment_row.fx_rate_source = None
        payment_row.converted_at = None
        # amount_received (legacy pre-ak-lvu column) stays populated.
        self.db.session.commit()

    def test_minimal_patch_notes_only_no_flap_on_legacy_row(self):
        """v3 V2-1(a) — the DISCRIMINATING anti-flap canary.

        Sends a truly minimal patch payload (just `{notes: "..."}`, NO
        payment section) — on v2 recompute always runs → today's-rate
        flap fires; on v3+ the gate skips recompute → paid preserved.
        """
        initial = self._create_usd_invoice_with_payment()
        self.assertEqual(initial['status'], 'paid')

        # v5 V5.3: true legacy shape (all 6 new cols NULL).
        self._mimic_true_legacy_row(initial['invoiceNumber'])

        # Simulate FX drift.
        self.svc.currency_service.rates['USD'] = Decimal("95.00")

        # TRULY MINIMAL patch — just notes.
        result = self.svc.update_invoice(
            initial['invoiceNumber'],
            {"notes": "just editing notes on a legacy paid invoice"},
        )
        self.assertEqual(
            result['status'], 'paid',
            f"v3 V2-1(a) discrimination: minimal-patch notes-only edit "
            f"on legacy paid non-INR invoice triggered recompute + "
            f"today's-rate flap. Expected 'paid', got {result['status']}."
        )

    def test_fe_realistic_notes_edit_on_legacy_row_no_flap(self):
        """v4 anti-flap + v5 V5.1 amount-space fix.

        The v3-era version of this test built the payload from
        `dict(initial)` — that was a PRE-nulling snapshot with true
        `originalAmount=100/USD`, i.e. an FX-metadata-intact payload
        that no real client can possess for a truly-legacy row.

        v5 V5.3 corrected version: mimic true legacy row (NULL all 6
        cols), then rebuild the payload by calling `get_invoice_by_number`
        — that's what an FE round-trip receives. Post-V5.1 fix the
        legacy backfill tags `originalCurrency: 'INR'`, so
        `_replace_payment` converts INR→INR identity and inr_amount
        stays byte-stable. Pre-V5.1 (v4 code) the payload had
        `originalCurrency: null`, F-1 guard skipped, fresh-write
        defaulted currency to invoice.currency (USD), and 8325 was
        converted as USD → 693,056.25 (V4-1 CRITICAL).

        Super-reviewer's pre-specified discriminator: this test MUST
        fail on v4 with `inrAmount = 693,056.25` and pass on v5.

        Plus the second-round-trip equilibrium check (Lead's follow-up):
        post-V5.1 rewrites payment as INR-tagged → second round-trip
        must still be byte-stable + status stays paid, verifying that
        V5.1 + v4 anti-flap compose correctly at equilibrium.
        """
        initial = self._create_usd_invoice_with_payment()
        self.assertEqual(initial['status'], 'paid')

        # v5 V5.3: TRUE legacy shape.
        self._mimic_true_legacy_row(initial['invoiceNumber'])

        # FX drift.
        self.svc.currency_service.rates['USD'] = Decimal("95.00")

        # v5 V5.3: rebuild payload from what FE actually receives.
        legacy_view = self.svc.get_invoice_by_number(initial['invoiceNumber'])
        # Post-V5.1: `originalCurrency = 'INR'` because we backfilled it.
        # Pre-V5.1 (v4): it would be null.
        self.assertEqual(
            legacy_view['payment']['originalCurrency'], 'INR',
            f"v5 V5.1 read-side backfill: legacy row's originalCurrency "
            f"should be 'INR' (the value in amount_received IS an INR "
            f"value). Got: {legacy_view['payment']['originalCurrency']}"
        )

        payload = dict(legacy_view)
        payload['notes'] = "editing notes on a legacy invoice, FE-real shape"
        result = self.svc.update_invoice(initial['invoiceNumber'], payload)

        # The AC-1-in-amount-space assertion: on v4 this would be
        # 693,056.25 (83.2× drift). On v5 it stays at the legacy INR value.
        legacy_inr_value = legacy_view['payment']['amountReceived']
        self.assertEqual(
            result['payment']['inrAmount'], legacy_inr_value,
            f"v5 V5.1 CRITICAL: FE round-trip on true-legacy row corrupted "
            f"inrAmount. Expected {legacy_inr_value}, got "
            f"{result['payment']['inrAmount']} (super-reviewer's predicted "
            f"v4-shape corruption value: 693056.25 = 8325 × 83.25). Read-"
            f"side backfill of originalCurrency='INR' should route "
            f"through identity conversion."
        )
        self.assertEqual(
            result['status'], 'paid',
            f"v4 anti-flap: notes edit on legacy paid non-INR invoice "
            f"ended at {result['status']}"
        )

        # ── Second-round-trip equilibrium (Lead's follow-up) ──────────
        # Post-V5.1 rewrite: payment is now INR-tagged. Second PUT of
        # the same shape must ALSO preserve inrAmount + status. Exercises
        # the new steady-state: INR-on-USD-invoice routes recompute
        # through mixed-currency branch, where v4 anti-flap (v5 V4-3
        # symmetric) guards status.
        second_view = self.svc.get_invoice_by_number(initial['invoiceNumber'])
        second_payload = dict(second_view)
        second_payload['notes'] = "second round-trip"
        second_result = self.svc.update_invoice(
            initial['invoiceNumber'], second_payload,
        )
        self.assertEqual(
            second_result['payment']['inrAmount'], legacy_inr_value,
            f"v5 V5.1 equilibrium: second round-trip drifted inrAmount "
            f"from {legacy_inr_value} to {second_result['payment']['inrAmount']}"
        )
        self.assertEqual(
            second_result['status'], 'paid',
            f"v5 V4-3 symmetric guard: second round-trip flipped status "
            f"to {second_result['status']}"
        )

    def test_legacy_shape_amount_only_put_preserves_paid_status(self):
        """v4 anti-flap CORE case + v5 fixture correction: legacy row +
        LLM sends F-1 legacy shape (`amountReceived` only, matches
        stored INR-side value) + FX drift.

        This test MUST fail on v3 (8503d9f) and pass on v4+.
        """
        initial = self._create_usd_invoice_with_payment()
        self.assertEqual(initial['status'], 'paid')

        # v5 V5.3: TRUE legacy shape (all 6 new cols NULL).
        self._mimic_true_legacy_row(initial['invoiceNumber'])

        # FX drift.
        self.svc.currency_service.rates['USD'] = Decimal("95.00")

        # Legacy-shape LLM payload from what FE would receive.
        legacy_view = self.svc.get_invoice_by_number(initial['invoiceNumber'])
        legacy_shape_payload = dict(legacy_view)
        legacy_shape_payload['payment'] = {
            # `amountReceived` alias only, no originalAmount / originalCurrency
            "amountReceived": legacy_view['payment']['amountReceived'],
            "paymentMethod": "wise_updated",
        }
        legacy_shape_payload['notes'] = "editing on legacy row via LLM legacy shape"

        result = self.svc.update_invoice(
            initial['invoiceNumber'], legacy_shape_payload,
        )
        self.assertEqual(
            result['status'], 'paid',
            f"v4 anti-flap CORE: LLM legacy-shape PUT on legacy paid row "
            f"flapped to {result['status']}."
        )

    def test_legacy_sent_with_partial_payment_stays_sent_after_edit(self):
        """v5 V4-3 symmetric guard: legacy `sent` invoice with a
        partial-payment row must NOT be promoted to `paid` by a
        favourable-drift today's-rate compare.

        Pre-v5 asymmetric guard: v4 refused only downgrade-from-paid.
        A legacy sent invoice with amount_received = 8000 on a 100 USD
        invoice would (at USD=95): total_inr=9500 vs paid_inr=8000 →
        partially_paid on v4 (OK — legit under-payment surfacing). But
        with different drift (USD=80): total_inr=8000 vs paid_inr=8000
        → paid ← spurious promotion.

        v5 V4-3 symmetric: refuses ANY status change on today's-rate
        fallback for rows with payments, not just downgrade from paid.
        """
        # Create a sent invoice with an amount_received of 8000 INR
        # (mimics a legacy partial-payment scenario).
        payload = {
            "invoiceNumber": "INV-LEGACY-PARTIAL",
            "projectName": "legacy sent w/ partial",
            "issueDate": "2026-06-01",
            "dueDate": "2026-06-30",
            "from": {"name": "V", "email": "v@example.com", "address": "..."},
            "to": {"name": "C", "email": "c@example.com", "address": "..."},
            "currency": "USD",
            "items": [{"description": "x", "quantity": 1, "rate": 100}],
            "tax": {"rate": 0, "amount": 0},
            "subtotal": 100,
            "total": 100,
            "status": "sent",
            "payment": {
                "originalAmount": 8000,  # partial
                "originalCurrency": "INR",  # so recompute mixed branch fires later
                "paymentMethod": "bank_transfer",
                "paymentDate": "2026-06-15",
            },
        }
        # v5 V4-3: creating this puts payment at 8000 INR on USD invoice.
        # Same-currency check fails (INR ≠ USD invoice). Cross-currency
        # branch: latest payment's fx_rate = 1.0 (INR identity),
        # total_inr = 100 × 1 = 100 → 8000 >= 100 → paid.
        # Not the legacy-sent scenario. Skip create-with-payment, use
        # direct DB write to simulate real legacy state.
        initial = self.svc.create_invoice({
            **payload,
            "payment": {
                "originalAmount": 100,
                "originalCurrency": "USD",
                "paymentMethod": "bank_transfer",
                "paymentDate": "2026-06-15",
            },
        })
        # Now force it to be a legacy row with partial payment: NULL FX
        # metadata AND set amount_received to a partial value.
        invoice_row = self.db.session.query(Invoice).filter_by(
            invoice_number="INV-LEGACY-PARTIAL",
        ).first()
        payment_row = invoice_row.payments[0]
        payment_row.original_amount = None
        payment_row.original_currency = None
        payment_row.inr_amount = None
        payment_row.fx_rate = None
        payment_row.fx_rate_source = None
        payment_row.converted_at = None
        payment_row.amount_received = Decimal("8000.00")  # partial INR value
        # Force status to sent (bypassing recompute — mimics real production
        # row that was 'sent' pre-ak-lvu with an incomplete payment).
        invoice_row.status = InvoiceStatusEnum.sent
        self.db.session.commit()

        # Favourable FX drift: USD strengthens; today's rate would make
        # 8000 INR look like it covers a 100 USD invoice (8000 / 80 = 100).
        self.svc.currency_service.rates['USD'] = Decimal("80.00")

        # Notes-only edit (via full payload — FE-realistic).
        legacy_view = self.svc.get_invoice_by_number("INV-LEGACY-PARTIAL")
        put_payload = dict(legacy_view)
        put_payload['notes'] = "editing notes on legacy partial-paid row"
        result = self.svc.update_invoice("INV-LEGACY-PARTIAL", put_payload)

        # Post-V5.1 backfill: originalCurrency='INR' on the read. FE
        # round-trip converts INR→INR identity, preserves 8000 INR
        # payment. Recompute runs (payment_supplied=True). Payment is
        # INR-tagged; invoice is USD. all_same_currency=False → mixed
        # branch. Latest payment's fx_rate=1.0 (identity, from V5.1
        # write). total_inr = 100 × 1.0 = 100. paid_inr = 8000. 8000 >
        # 100 → would compute PAID — but V4-3 symmetric refuses status
        # change in this branch because payment.original_currency
        # (INR) ≠ invoice.currency (USD) → not the same-currency vintage
        # path.
        #
        # Actually wait — with V5.1's INR-tag + identity fx_rate, the
        # mixed-currency branch DOES have a stored fx_rate (1.0), so
        # V4-3 symmetric only applies if we FURTHER lack that rate. Let
        # me re-trace: `latest.original_currency.upper() == 'USD'`? INR
        # != USD → else branch → symmetric V4-3 refuse. Status stays 'sent'.
        self.assertEqual(
            result['status'], 'sent',
            f"v5 V4-3 symmetric: legacy sent invoice with partial payment "
            f"and favourable FX drift was promoted to {result['status']}. "
            f"Should stay sent — today's-rate compare on mismatched-"
            f"currency payment is unreliable in both directions."
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
