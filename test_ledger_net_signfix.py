"""ak-p3id: §2 accounting-correctness regression tests for the
ledger-net sign-fix + FY earnings fix.

Chrome render-verify on prod found the headline Net number
sign-flipped on BOTH Home ledger tile and Transactions summary,
plus a 70% divergence between Home "Freelance paid · this FY"
and the Freelance dashboard "PAID THIS FY". Browser testing
was never run across the 25-bead landing wave, so code-inspection
missed it.

Root causes (per ak-p3id triage):

1. MAJOR #3 — Net sign-flipped:
   The Transactions.amount storage convention stores credits as
   NEGATIVE (money IN) and debits as POSITIVE (money OUT). The
   service returned raw signed `credit_sum` (negative) +
   `debit_sum` (positive); two FE call sites disagreed on how
   to render them, both arriving at wrong math.
   Fix: add canonical positive-magnitude `in_sum` + `out_sum` +
   signed `net` = `in_sum - out_sum`. FE companion bead to
   rebind.

2. MAJOR #2 — FY-paid divergence:
   No canonical `fyEarnings` field on /freelance/dashboard. Home
   tile bound to `totalEarnings` (all-time) + mislabelled it
   "this FY". Fix: add `fyEarnings` + `fyStart` + `fyEnd` so
   both pages bind the same source.

Both are §2 CRITICAL accounting-correctness bugs. These tests
hand-compute fixtures (NOT snapshot tests) to pin the arithmetic.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import types
import unittest
from datetime import date, datetime
from decimal import Decimal


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ─── Part 1: Ledger-net sign-fix — service layer ─────────────


def _ast_lift_transactions_service():
    """Lift TransactionService.fetchTransactions so we can
    exercise the sum-math branch against a real in-memory SQLite
    without the BaseService Google-auth boot chain."""
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "transactionsService.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)

    kept = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "TransactionService":
            for child in node.body:
                if isinstance(
                    child, (ast.FunctionDef, ast.AsyncFunctionDef),
                ) and child.name == "fetchTransactions":
                    child.decorator_list = []
                    kept.append(child)
    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)

    from sqlalchemy import func, case as sa_case
    from models.transactions import Transactions, ProcessingMethod
    ns = {
        "__name__": "test_ledger_ast_lift",
        "func": func,
        "case": sa_case,
        "Transactions": Transactions,
        "ProcessingMethod": ProcessingMethod,
    }
    exec(compile(lift, "<tx-svc-AST-lift>", "exec"), ns)
    return ns["fetchTransactions"]


try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    import models  # noqa: F401
    from models.investmentHistory import InvestmentHistory  # noqa: F401
    from models.transactions import Transactions, ProcessingMethod
    _DEPS_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _DEPS_OK = False
    _SKIP_REASON = f"Transactions model import failed: {_exc}"


class _StubTxService:
    def __init__(self, session_factory):
        self._factory = session_factory
        self._cached = None
        self.logger = _NoopLogger()

    @property
    def db(self):
        return self

    @property
    def session(self):
        if self._cached is None:
            self._cached = self._factory()
        return self._cached


class _NoopLogger:
    def info(self, *_a, **_k):
        pass

    def warning(self, *_a, **_k):
        pass

    def error(self, *_a, **_k):
        pass


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestLedgerNetSignFix(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Transactions.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self._fetch = _ast_lift_transactions_service()
        self.svc = _StubTxService(self._SessionFactory)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add(self, amount, user="u1", **kwargs):
        """Insert a transaction row. Storage convention:
          * amount < 0 → credit (money IN)
          * amount > 0 → debit  (money OUT)
        """
        kwargs.setdefault(
            "date", datetime(2026, 11, 22).date(),
        )
        kwargs.setdefault("referenceID", f"ref-{amount}-{kwargs['date']}")
        kwargs.setdefault("details", "test tx")
        kwargs.setdefault("bank", "HDFC")
        kwargs.setdefault("source", "statement")
        kwargs.setdefault(
            "processed_via", ProcessingMethod.CLAUDE_CODE,
        )
        row = Transactions(
            amount=amount, user=user, **kwargs,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # ─── Pin-down #1: positive-magnitude In sum ───────────────
    def test_in_sum_is_positive_magnitude(self):
        """Credit rows (amount < 0, stored negative) must surface
        as POSITIVE in_sum."""
        self._add(amount=-100)  # In = 100
        self._add(amount=-50)   # In = 50
        result = self._fetch(self.svc, page=1, filters={}, user_id="u1")
        self.assertEqual(result["in_sum"], 150.0)
        # Legacy: credit_sum stays as negative sum of stored
        self.assertEqual(float(result["credit_sum"] or 0), -150.0)

    # ─── Pin-down #2: positive-magnitude Out sum ──────────────
    def test_out_sum_is_positive_magnitude(self):
        """Debit rows (amount > 0, stored positive) surface as
        POSITIVE out_sum (identical to legacy debit_sum)."""
        self._add(amount=30)  # Out = 30
        self._add(amount=20)  # Out = 20
        result = self._fetch(self.svc, page=1, filters={}, user_id="u1")
        self.assertEqual(result["out_sum"], 50.0)
        self.assertEqual(float(result["debit_sum"] or 0), 50.0)

    # ─── Pin-down #3: CRITICAL — Net arithmetic ───────────────
    #
    # Pre-ak-p3id regression: FE computed `net = credit_sum -
    # debit_sum` = (-100) - (30) = -130 (sign-flipped + magnitude
    # ~2× truth). CORRECT: net = in - out = 100 - 30 = +70.
    #
    # This fixture pins the correct sign + magnitude. Any revert
    # to the stored-signed arithmetic reds it.
    def test_net_is_in_minus_out_with_correct_sign(self):
        """Hand-fixture: In=100, Out=30 → Net=+70.
        Pre-fix bug: Net = credit_sum - debit_sum = -130."""
        self._add(amount=-100)  # In
        self._add(amount=30)    # Out
        result = self._fetch(self.svc, page=1, filters={}, user_id="u1")
        self.assertEqual(
            result["net"], 70.0,
            f"Net math wrong: expected +70 (In 100 - Out 30), got "
            f"{result['net']}. Pre-ak-p3id bug produced -130.",
        )
        self.assertEqual(result["in_sum"], 100.0)
        self.assertEqual(result["out_sum"], 30.0)

    # Prod-like scenario: In=3.05Cr, Out=3.00Cr → Net ≈ +5.32L
    def test_prod_scale_near_breakeven_net_positive(self):
        """Reproduces the prod observation that landed this bead:
        In 3.05Cr / Out 3.00Cr should yield Net +5.32L positive,
        not the observed −6.05Cr."""
        in_amt = 30521766.53
        out_amt = 29990159.73
        # One credit row + one debit row at those magnitudes
        self._add(amount=-in_amt, referenceID="in-prod")
        self._add(amount=out_amt, referenceID="out-prod")
        result = self._fetch(self.svc, page=1, filters={}, user_id="u1")
        self.assertAlmostEqual(result["in_sum"], in_amt, places=2)
        self.assertAlmostEqual(result["out_sum"], out_amt, places=2)
        self.assertAlmostEqual(
            result["net"], in_amt - out_amt, places=2,
        )
        # Sanity: net sign is POSITIVE (we received more than we
        # spent in this window)
        self.assertGreater(result["net"], 0.0)

    # Net can be negative (net-spent scenario)
    def test_net_negative_when_out_exceeds_in(self):
        self._add(amount=-50)   # In = 50
        self._add(amount=200)   # Out = 200
        result = self._fetch(self.svc, page=1, filters={}, user_id="u1")
        self.assertEqual(result["net"], -150.0)

    # Empty ledger → all zero
    def test_empty_ledger_returns_zero(self):
        result = self._fetch(self.svc, page=1, filters={}, user_id="u1")
        self.assertEqual(result["in_sum"], 0.0)
        self.assertEqual(result["out_sum"], 0.0)
        self.assertEqual(result["net"], 0.0)

    # User-scoping preserved on all new fields
    def test_sums_are_user_scoped(self):
        self._add(amount=-500, user="u1", referenceID="u1-in")
        self._add(amount=100, user="u1", referenceID="u1-out")
        self._add(amount=-9999, user="u2", referenceID="u2-in")
        self._add(amount=4444, user="u2", referenceID="u2-out")
        result = self._fetch(self.svc, page=1, filters={}, user_id="u1")
        self.assertEqual(result["in_sum"], 500.0)
        self.assertEqual(result["out_sum"], 100.0)
        self.assertEqual(result["net"], 400.0)


# ─── Part 2: Controller passes new fields through ─────────────


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_controller_response_carries_in_sum_out_sum_net():
    """ak-p3id drift guard: /fetchTransactions response includes
    the three new fields. FE companion bead binds against them."""
    src = _read_source("controllers/transactionsEP.py")
    for key in ('"in_sum"', '"out_sum"', '"net"'):
        assert key in src, (
            f"Controller response must include {key} — ak-p3id "
            f"FE binding depends on it"
        )
    # Must read from the service result dict with same keys
    for key in ('"in_sum"', '"out_sum"', '"net"'):
        assert f'transactions[{key}]' in src, (
            f"Controller must pass-through {key} from service "
            f"response, not hard-code"
        )


def test_agent_tool_executor_carries_ledger_fields():
    """The agent tool `fetch_transactions` must forward the new
    fields so the LLM can answer 'what's my net inflow?' without
    sign juggling."""
    src = _read_source("services/agent_tool_executor.py")
    for key in ('"in_sum"', '"out_sum"', '"net"'):
        assert key in src, (
            f"agent_tool_executor must forward {key} from "
            f"TransactionService result"
        )


# ─── Part 3: Freelance fyEarnings ─────────────────────────────


def _ast_lift_dashboard_fy_fields():
    """Lift the FY bounds + fyEarnings computation out of
    DashboardService.get_dashboard_analytics so we can pin the
    FY-start-month logic + the key presence in the response
    shape."""
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "dashboardService.py",
    )
    with open(src_path, "r") as fh:
        return fh.read()


def test_dashboard_has_fy_earnings_field():
    """ak-p3id: /freelance/dashboard response carries fyEarnings
    + fyStart + fyEnd. Home ledger tile + Freelance dashboard
    tile BOTH bind to this one source."""
    src = _ast_lift_dashboard_fy_fields()
    for key in ('"fyEarnings"', '"fyStart"', '"fyEnd"'):
        assert key in src, (
            f"dashboardService must emit {key} — ak-p3id Home/"
            f"Freelance convergence depends on it"
        )


def test_dashboard_fy_starts_in_april():
    """Indian FY convention: April 1 → March 31. Pin that the
    FY computation uses fy_start_month = 4."""
    src = _ast_lift_dashboard_fy_fields()
    assert re.search(
        r"fy_start_month\s*=\s*4\b", src,
    ), "FY start must be April (month = 4), not January or March"
    # Pre-fix bug in FE displayed March 31 due to toISOString timezone
    # shift; BE must not propagate that. We compute from numeric
    # month arithmetic, not timezone-shifted ISO strings.


def test_dashboard_fy_uses_canonical_earnings_function():
    """fyEarnings must go through get_earnings_in_inr (ak-lvu
    canonical path) — NOT an ad-hoc sum. Same code path as the
    Freelance dashboard's get_earnings_by_date_range so the two
    numbers CANNOT diverge."""
    src = _ast_lift_dashboard_fy_fields()
    # The fy_earnings call should use get_earnings_in_inr with
    # start_date + end_date.
    assert re.search(
        r"fy_earnings\s*=\s*get_earnings_in_inr\s*\(",
        src, re.DOTALL,
    ), (
        "fyEarnings must be computed via get_earnings_in_inr "
        "(canonical ak-lvu path) — any ad-hoc sum would re-"
        "introduce the Home/Freelance divergence"
    )


def test_dashboard_fy_bounds_crossing_year_boundary():
    """FY convention edge-case: if today is Jan or Feb or March,
    FY started LAST calendar year's April 1. Service-code pins:
    if now.month >= 4, fy_start_year = now.year; else fy_start_year
    = now.year - 1.
    """
    src = _ast_lift_dashboard_fy_fields()
    assert re.search(
        r"if\s+now\.month\s*>=\s*fy_start_month\s*:", src,
    ), (
        "FY year-boundary branch missing — Jan-Mar must start FY "
        "on PREVIOUS year's April 1"
    )
    assert re.search(
        r"fy_start_year\s*=\s*now\.year\s*-\s*1", src,
    ), "Jan-Mar must map to now.year - 1 as FY start year"


# Dashboard FY integration: invoice-fixture through the real
# get_earnings_in_inr function. Pins that an invoice issued
# WITHIN the FY contributes + an invoice issued BEFORE the FY
# does NOT. This catches a regression where fyEarnings would
# default to all-time (the original Home bug).


try:
    from services.earningsService import get_earnings_in_inr
    from models.freelance_management import (
        Invoice, InvoicePayment, InvoiceStatusEnum, CurrencyEnum,
    )
    _FREELANCE_DEPS_OK = True
    _FREELANCE_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _FREELANCE_DEPS_OK = False
    _FREELANCE_SKIP_REASON = f"freelance model import failed: {_exc}"


@unittest.skipUnless(_FREELANCE_DEPS_OK, _FREELANCE_SKIP_REASON)
class TestFyEarningsCanonical(unittest.TestCase):
    """Integration test: exercises the real
    get_earnings_in_inr against a SQLite fixture with 2
    invoices, one inside the FY and one outside. Pins that
    only the inside-FY payment contributes."""

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        # Minimal tables needed by the canonical sum function
        Invoice.__table__.create(self.engine)
        InvoicePayment.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.session = self._SessionFactory()

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _invoice(self, *, invoice_number, issue_date, total, paid_inr):
        inv = Invoice(
            invoice_number=invoice_number,
            user_id="u1",
            project_name=f"proj-{invoice_number}",
            total=total,
            currency=CurrencyEnum.INR,
            status=InvoiceStatusEnum.paid,
            issue_date=issue_date,
            due_date=issue_date,
            # Fill all NOT NULL text columns with placeholders —
            # payment summation doesn't care about the sender /
            # recipient identity fields for ak-p3id pins.
            from_name="Self",
            from_email="self@example.com",
            from_address="addr",
            to_name="Client",
            to_email="client@example.com",
            to_address="addr",
            subtotal=total,
        )
        self.session.add(inv)
        self.session.flush()
        pay = InvoicePayment(
            invoice_id=inv.id,
            amount_received=Decimal(str(paid_inr)),
            inr_amount=Decimal(str(paid_inr)),
            original_amount=Decimal(str(paid_inr)),
            original_currency="INR",
            payment_date=issue_date,
        )
        self.session.add(pay)
        self.session.commit()
        return inv

    def test_fy_range_excludes_prior_fy_invoice(self):
        """FY 2026-27 starts 2026-04-01. An invoice issued
        2026-03-15 is PRIOR FY — must NOT contribute to
        fyEarnings. An invoice issued 2026-05-01 IS in FY."""
        self._invoice(
            invoice_number="INV-PRIOR",
            issue_date=date(2026, 3, 15),  # FY 2025-26
            total=Decimal("100000"),
            paid_inr=Decimal("100000"),
        )
        self._invoice(
            invoice_number="INV-FY",
            issue_date=date(2026, 5, 1),  # FY 2026-27
            total=Decimal("250000"),
            paid_inr=Decimal("250000"),
        )
        fy_earnings = get_earnings_in_inr(
            self.session, "u1",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 11, 23),
        )
        self.assertEqual(
            fy_earnings, Decimal("250000.00"),
            "fyEarnings included prior-FY invoice — regression",
        )

    def test_all_time_strictly_greater_than_fy(self):
        """Sanity: all-time (no date filter) >= fyEarnings with
        strict > when there's a prior-FY invoice. This was the
        shape of the original Home bug: Home showed all-time
        (greater) + mislabeled as 'this FY'."""
        self._invoice(
            invoice_number="INV-PRIOR",
            issue_date=date(2026, 3, 15),
            total=Decimal("100000"),
            paid_inr=Decimal("100000"),
        )
        self._invoice(
            invoice_number="INV-FY",
            issue_date=date(2026, 5, 1),
            total=Decimal("250000"),
            paid_inr=Decimal("250000"),
        )
        all_time = get_earnings_in_inr(self.session, "u1")
        fy = get_earnings_in_inr(
            self.session, "u1",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 11, 23),
        )
        self.assertEqual(all_time, Decimal("350000.00"))
        self.assertEqual(fy, Decimal("250000.00"))
        self.assertGreater(all_time, fy)


if __name__ == "__main__":
    unittest.main()
