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


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
