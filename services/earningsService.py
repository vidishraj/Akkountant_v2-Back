"""ak-lvu A.3 — canonical get_earnings_in_inr.

Pre-ak-lvu the codebase had THREE divergent earnings formulas across
dashboardService (twice) and customerService — one used `func.sum(Invoice.total)`
(wrong on non-INR: adds up mixed currencies as if they were INR), one
summed `payment.amount_received` treating it as INR (correct on write,
wrong after AC-1 double-convert), one branched on currency and used
different fields per branch. AC-3 CRITICAL closed by this module.

Contract (from bead A.3):
    Σ payments (in INR) of PAID invoices, one currency (INR)

`include_unpaid=True` extends the sum to unpaid invoices' `total`
converted to INR (dashboard uses this for the "outstanding" combined
view). Payment INR values are read via the tolerant helper so pre-
migration NULL rows fall back to `amount_received` (legacy INR shape).

Kept in a separate module (not on any Service class) so import from
dashboard / customer / mail-executor is a plain function call — no
service-instance juggling. Uses BaseService.db via the passed
session so callers don't need to construct a service.
"""

from __future__ import annotations

from decimal import Decimal
from datetime import date
from typing import Optional

from sqlalchemy import and_
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import joinedload

from models.freelance_management import (
    Invoice, InvoicePayment, InvoiceStatusEnum, CurrencyEnum,
)
from services.money_utils import money, q2, _is_migration_gap


def get_earnings_in_inr(
    session,
    user_id: str,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    customer_id: Optional[str] = None,
    include_unpaid: bool = False,
    currency_service=None,
) -> Decimal:
    """Sum earnings for `user_id`, in INR, per the ak-lvu A.3 contract.

    Args:
        session: SQLAlchemy session (BaseService().db.session for the
            common case).
        user_id: firebase_id — mandatory. Every query is user-scoped
            (single-user-by-design still applies).
        start_date / end_date: inclusive-range filter on `Invoice.issue_date`.
            Optional; both None = all-time.
        customer_id: restrict to one customer's invoices. Optional.
        include_unpaid: also include unpaid invoices, converting
            `Invoice.total` to INR via currency_service. Used by the
            dashboard "combined outstanding" view.
        currency_service: injected CurrencyService; only required if
            include_unpaid=True AND there are non-INR unpaid invoices.

    Returns:
        Decimal (2dp-quantized) — total earnings in INR.
    """
    if not user_id:
        raise ValueError("get_earnings_in_inr: user_id is required")

    query = session.query(Invoice).options(joinedload(Invoice.payments))
    filters = [Invoice.user_id == user_id]
    if customer_id:
        filters.append(Invoice.customer_id == customer_id)
    if start_date:
        filters.append(Invoice.issue_date >= start_date)
    if end_date:
        filters.append(Invoice.issue_date <= end_date)

    # Include partially_paid too — a partially-paid invoice HAS earned
    # money already. Only "sent" and "overdue" are un-earned; "paid" and
    # "partially_paid" contribute their inr_amount sum. Drafts are
    # excluded (not yet issued).
    if include_unpaid:
        # Full sweep excluding drafts.
        filters.append(Invoice.status != InvoiceStatusEnum.draft)
    else:
        filters.append(Invoice.status.in_((
            InvoiceStatusEnum.paid, InvoiceStatusEnum.partially_paid,
        )))

    invoices = query.filter(and_(*filters)).all()

    total = Decimal("0")
    for inv in invoices:
        # For any invoice with payments, sum the INR values of those
        # payments — that's the actual money received, and it accounts
        # for partial payments correctly.
        paid_sum = _sum_inr_payments(inv.payments)
        total += paid_sum

        # For include_unpaid: add the OUTSTANDING amount for unpaid /
        # partially-paid invoices, converted from invoice.total (or
        # remaining balance) via currency service.
        if include_unpaid and inv.status in (
            InvoiceStatusEnum.sent, InvoiceStatusEnum.overdue,
        ):
            # Fully unpaid: convert invoice.total to INR.
            currency_str = (
                inv.currency.value if hasattr(inv.currency, 'value')
                else str(inv.currency)
            )
            if currency_str == 'INR':
                total += money(inv.total)
            else:
                if currency_service is None:
                    # Skip — no way to convert without the service. Log
                    # via the caller (dashboardService) if this matters.
                    continue
                try:
                    fx = currency_service.convert_to_inr_with_source(
                        money(inv.total), currency_str, allow_fallback=True,
                    )
                    total += fx["inr_amount"]
                except Exception:
                    # Best-effort — skip on failure. Read-side already
                    # tolerates FX outages elsewhere.
                    continue
        elif include_unpaid and inv.status == InvoiceStatusEnum.partially_paid:
            # Partially paid: the "outstanding" portion is (total - paid).
            # Convert the balance in invoice-currency space via a ratio:
            #   total_inr ≈ inv.total × fx_rate
            #   paid_inr = paid_sum
            #   remaining_inr = total_inr - paid_sum
            # For simplicity we convert the full total then subtract the
            # already-summed paid amount.
            currency_str = (
                inv.currency.value if hasattr(inv.currency, 'value')
                else str(inv.currency)
            )
            if currency_str == 'INR':
                total_inr = money(inv.total)
            else:
                if currency_service is None:
                    continue
                try:
                    fx = currency_service.convert_to_inr_with_source(
                        money(inv.total), currency_str, allow_fallback=True,
                    )
                    total_inr = fx["inr_amount"]
                except Exception:
                    continue
            remaining = total_inr - paid_sum
            if remaining > Decimal("0"):
                total += remaining

    return q2(total)


def _sum_inr_payments(payments) -> Decimal:
    """Sum `payment.inr_amount` across a payment list, tolerant of
    pre-migration rows (fall back to `amount_received` which stored the
    INR value in the legacy shape)."""
    total = Decimal("0")
    for p in payments:
        try:
            inr = p.inr_amount
        except (OperationalError, ProgrammingError) as exc:
            if _is_migration_gap(exc):
                inr = None
            else:
                raise
        if inr is not None:
            total += money(inr)
        elif p.amount_received is not None:
            total += money(p.amount_received)
    return total


def get_earnings_by_month(
    session,
    user_id: str,
    *,
    months_back: int = 12,
) -> list:
    """Bucket earnings by year-month for the trailing N months.

    Returns list of {"month": "YYYY-MM", "earnings": Decimal} entries,
    OLDEST first. Same INR-canonical contract as get_earnings_in_inr.
    """
    from datetime import datetime as _dt
    now = _dt.now()
    result = []
    for i in range(months_back - 1, -1, -1):
        target_month = now.month - i
        target_year = now.year
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        # Compute month bounds.
        from calendar import monthrange
        start = date(target_year, target_month, 1)
        end = date(target_year, target_month, monthrange(target_year, target_month)[1])
        earnings = get_earnings_in_inr(
            session, user_id, start_date=start, end_date=end,
        )
        result.append({
            "month": f"{target_year:04d}-{target_month:02d}",
            "earnings": earnings,
        })
    return result


def get_earnings_by_client(
    session,
    user_id: str,
    *,
    limit: int = 10,
    include_unpaid: bool = False,
    currency_service=None,
) -> list:
    """Return top-N clients by INR earnings.

    Result: list of {"client": name, "earnings": Decimal}, sorted DESC by
    earnings, capped at `limit`. Consistent with get_earnings_in_inr.
    """
    query = session.query(Invoice).options(
        joinedload(Invoice.payments), joinedload(Invoice.customer),
    ).filter(Invoice.user_id == user_id)

    if include_unpaid:
        query = query.filter(Invoice.status != InvoiceStatusEnum.draft)
    else:
        query = query.filter(Invoice.status.in_((
            InvoiceStatusEnum.paid, InvoiceStatusEnum.partially_paid,
        )))

    invoices = query.all()

    per_client: dict = {}
    for inv in invoices:
        client_name = inv.customer.name if inv.customer else inv.to_name
        paid_sum = _sum_inr_payments(inv.payments)
        per_client[client_name] = per_client.get(client_name, Decimal("0")) + paid_sum

        if include_unpaid and inv.status in (
            InvoiceStatusEnum.sent, InvoiceStatusEnum.overdue,
        ):
            currency_str = (
                inv.currency.value if hasattr(inv.currency, 'value')
                else str(inv.currency)
            )
            if currency_str == 'INR':
                per_client[client_name] += money(inv.total)
            elif currency_service:
                try:
                    fx = currency_service.convert_to_inr_with_source(
                        money(inv.total), currency_str, allow_fallback=True,
                    )
                    per_client[client_name] += fx["inr_amount"]
                except Exception:
                    pass

    ranked = sorted(per_client.items(), key=lambda x: x[1], reverse=True)[:limit]
    return [
        {"client": name, "earnings": q2(amount)}
        for name, amount in ranked
    ]
