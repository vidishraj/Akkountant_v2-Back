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
from utils.logger import Logger

logger = Logger(__name__).get_logger()


def get_earnings_in_inr(
    session,
    user_id: str,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    customer_id: Optional[str] = None,
    include_unpaid: bool = False,
    currency_service=None,
) -> Optional[Decimal]:
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
        Decimal (2dp-quantized) total earnings in INR, or None when the
        total is UNKNOWN: ak-nzed, an unpaid invoice that couldn't be
        converted to INR is never silently dropped (that understated the
        total). Only include_unpaid=True can return None; paid earnings
        sum stored INR payment values and need no FX. Use
        get_earnings_in_inr_detailed for the known part + the reasons.
    """
    return get_earnings_in_inr_detailed(
        session, user_id, start_date=start_date, end_date=end_date,
        customer_id=customer_id, include_unpaid=include_unpaid,
        currency_service=currency_service,
    )["total"]


def get_earnings_in_inr_detailed(
    session,
    user_id: str,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    customer_id: Optional[str] = None,
    include_unpaid: bool = False,
    currency_service=None,
) -> dict:
    """ak-nzed: get_earnings_in_inr plus what the total is missing.

    Returns {
        "total": Decimal | None   — None when any invoice is unconverted,
        "known_total": Decimal    — the part that WAS converted (partial),
        **_fx_meta(...)           — unconverted_count / unconverted_currencies
                                     / approximate / rate_vintage,
    }
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

    acc = _new_fx_acc()
    total = Decimal("0")
    for inv in invoices:
        # Payments: the INR money actually received (no FX needed).
        paid_sum = _sum_inr_payments(inv.payments)
        total += paid_sum
        if include_unpaid:
            total += _outstanding_inr(inv, paid_sum, currency_service, acc)

    return _with_fx_meta({"known_total": q2(total)}, acc)


def _new_fx_acc():
    return {"unconverted": [], "vintages": set()}


def _invoice_currency(inv) -> str:
    return (inv.currency.value if hasattr(inv.currency, 'value')
            else str(inv.currency)).upper()


def _invoice_total_inr(inv, currency_service, acc) -> Optional[Decimal]:
    """invoice.total in INR, or None (recorded in acc) when it can't be
    converted. ak-nzed: never a silent skip. A vintage fallback rate is
    used (read path) but flagged as approximate."""
    currency_str = _invoice_currency(inv)
    if currency_str == 'INR':
        return money(inv.total)
    if currency_service is None:
        reason = "no_currency_service"
        fx = None
    else:
        try:
            fx = currency_service.convert_to_inr_with_source(
                money(inv.total), currency_str, allow_fallback=True,
            )
            reason = None
        except Exception as exc:
            fx, reason = None, type(exc).__name__
    if fx is None:
        acc["unconverted"].append(currency_str)
        logger.warning(
            f"[ak-nzed] invoice in {currency_str} not convertible to INR "
            f"({reason}); total reported as unknown, not understated"
        )
        return None
    source = str(fx.get("fx_rate_source") or "")
    if source.startswith("fallback:"):
        acc["vintages"].add(source.split(":", 1)[1])
    return fx["inr_amount"]


def _outstanding_inr(inv, paid_sum, currency_service, acc) -> Decimal:
    """The unpaid part of a sent / overdue / partially_paid invoice in
    INR; Decimal 0 for other statuses or when unconvertible (then acc
    records it, so the caller's total becomes unknown)."""
    if inv.status in (InvoiceStatusEnum.sent, InvoiceStatusEnum.overdue):
        total_inr = _invoice_total_inr(inv, currency_service, acc)
        return total_inr if total_inr is not None else Decimal("0")
    if inv.status == InvoiceStatusEnum.partially_paid:
        # Outstanding = total_inr - already-received INR.
        total_inr = _invoice_total_inr(inv, currency_service, acc)
        if total_inr is None:
            return Decimal("0")
        remaining = total_inr - paid_sum
        return remaining if remaining > Decimal("0") else Decimal("0")
    return Decimal("0")


def _with_fx_meta(out, acc):
    """Attach total (None if anything is unconverted) + FX provenance."""
    unconverted = acc["unconverted"]
    out["total"] = None if unconverted else out["known_total"]
    out["unconverted_count"] = len(unconverted)
    out["unconverted_currencies"] = sorted(set(unconverted))
    out["approximate"] = bool(acc["vintages"])
    out["rate_vintage"] = ", ".join(sorted(acc["vintages"])) or None
    return out


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

    Result: list of {"client", "earnings": Decimal | None,
    "known_earnings": Decimal, "unconverted_count", "unconverted_currencies",
    "approximate", "rate_vintage"}, sorted DESC by known_earnings, capped
    at `limit`. Consistent with get_earnings_in_inr. ak-nzed: "earnings"
    is None (unknown) when one of the client's invoices couldn't be
    converted to INR; it is never silently smaller.
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
        entry = per_client.setdefault(client_name, {"known": Decimal("0"), "acc": _new_fx_acc()})
        paid_sum = _sum_inr_payments(inv.payments)
        entry["known"] += paid_sum
        # ak-lvu v2 F-7 parity with get_earnings_in_inr(include_unpaid):
        # outstanding sent/overdue/partially_paid amounts. ak-nzed: an
        # unconvertible invoice makes THIS client's total unknown
        # instead of silently vanishing from it.
        if include_unpaid:
            entry["known"] += _outstanding_inr(inv, paid_sum, currency_service, entry["acc"])

    # Ranked by the KNOWN part (a client with an unknown total still
    # ranks by what's known; its earnings field is None).
    ranked = sorted(per_client.items(), key=lambda x: x[1]["known"], reverse=True)[:limit]
    out = []
    for name, entry in ranked:
        row = _with_fx_meta({"known_total": q2(entry["known"])}, entry["acc"])
        out.append({
            "client": name,
            "earnings": row["total"],
            "known_earnings": row["known_total"],
            "unconverted_count": row["unconverted_count"],
            "unconverted_currencies": row["unconverted_currencies"],
            "approximate": row["approximate"],
            "rate_vintage": row["rate_vintage"],
        })
    return out
