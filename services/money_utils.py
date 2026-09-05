"""ak-lvu: Decimal money-math primitives used by invoice/payment/dashboard/customer.

The Freelance subsystem historically ran money through `float(x)` at every
service ingress point (invoiceService/paymentService/dashboardService/
customerService) even though the schema columns are DECIMAL(12,2). That
produced silent 0.01-off rounding on totals, `int()` truncation of
displayed earnings, and let three divergent SUM(...) formulas exist
across dashboard/customer/agent-tool responses without any one of them
being demonstrably right.

This module centralises the ingress: everything that touches money goes
through `money(x)` on the way in, `q2(x)` on the way to storage / display,
`q4(x)` on FX rates, and `recompute_invoice_totals(...)` for the
line-item → subtotal → tax → total pass. Comparisons use `within_epsilon`
against a Decimal ₹0.01 tolerance rather than `float ==`.

Design notes:
  * `money(x)` accepts int / float / str / Decimal / None. `None` is a
    caller-error signal (missing field) rather than silently → 0 — the
    invoice recompute pass fills omitted client fields explicitly and
    should not conflate "field absent" with "field zero".
  * `q2()` uses `ROUND_HALF_UP` (accounting convention) not Python's
    default banker's rounding. A ₹1.005 total quantizes to ₹1.01, not
    ₹1.00 — matches user expectations on invoices.
  * `q4()` is separate from `q2()` so FX rate math preserves 4dp
    precision before the final INR value quantizes back to 2dp (avoids
    "0.0001 lost per conversion" drift on high-volume repeated ops).
  * `within_epsilon` defaults to ₹0.01 — one paisa. This is the
    "client-supplied total diverged from server recompute" tolerance
    per bead A.2. Callers can override for tighter/looser windows.

Runs without Flask or SQLAlchemy — pure stdlib Decimal — so it's
importable from tests without triggering the services.Base_Service
firebase chain (same AST-lift pattern as sanitize_display_name).
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable


# Quantize targets. Money is 2dp end-to-end; FX rates keep 4dp precision
# through arithmetic so the final INR quantize doesn't shed sub-paisa
# accuracy over multi-hop conversions.
_MONEY_QUANT = Decimal("0.01")
_FX_QUANT = Decimal("0.0001")

# Epsilon for "client value diverges from server recompute" comparison
# (bead A.2). ₹0.01 is one paisa — tighter than any float rounding could
# drift, looser than any deterministic Decimal recompute could miss.
DEFAULT_EPSILON = Decimal("0.01")


class MoneyError(ValueError):
    """Raised on invalid money input — distinct from ValueError so
    callers can catch just money-shape errors without swallowing other
    validation faults."""


def money(value) -> Decimal:
    """Coerce a value into Decimal without loss.

    Accepts int / float / str / Decimal. Rejects None + empty string +
    obviously non-numeric input — those are caller bugs, not "assume
    zero" cases.

    `float` inputs are stringified first (`Decimal(str(x))`) so we get
    the human-visible representation (0.1 → "0.1" → Decimal("0.1")) not
    the IEEE 754 binary residue (Decimal(0.1) →
    "0.1000000000000000055511151231257827021181583404541015625").
    """
    if value is None:
        raise MoneyError("money(): value is None (missing field)")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        # bool is int-subclass in Python; catch it explicitly so True
        # doesn't silently become Decimal("1") on a money field.
        raise MoneyError(f"money(): bool {value!r} is not a monetary value")
    if isinstance(value, (int,)):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise MoneyError("money(): empty string")
        try:
            return Decimal(stripped)
        except InvalidOperation as exc:
            raise MoneyError(f"money(): {value!r} is not a number") from exc
    raise MoneyError(f"money(): unsupported type {type(value).__name__}")


def q2(value) -> Decimal:
    """Quantize a Decimal to money precision (2dp, ROUND_HALF_UP).

    Applied at storage / display boundaries. Accounting-style rounding
    (₹1.005 → ₹1.01) rather than Python's default banker's-rounding
    (which would go to ₹1.00 or ₹1.02 depending on the preceding digit).
    """
    d = value if isinstance(value, Decimal) else money(value)
    return d.quantize(_MONEY_QUANT, rounding=ROUND_HALF_UP)


def q4(value) -> Decimal:
    """Quantize to FX-rate precision (4dp, ROUND_HALF_UP).

    Applied to fx_rate storage. Kept separate from q2 so the FX chain
    doesn't shed sub-paisa precision before the final INR conversion
    quantizes back to money.
    """
    d = value if isinstance(value, Decimal) else money(value)
    return d.quantize(_FX_QUANT, rounding=ROUND_HALF_UP)


def within_epsilon(
    a, b, epsilon: Decimal = DEFAULT_EPSILON
) -> bool:
    """True if |a - b| <= epsilon (Decimal-safe).

    Used by the invoice recompute pass to decide "client value matches
    server recompute" — anything within one paisa is treated as agreement
    (no overwrite / no warning); anything outside overwrites with the
    server value.
    """
    a_d = a if isinstance(a, Decimal) else money(a)
    b_d = b if isinstance(b, Decimal) else money(b)
    return abs(a_d - b_d) <= epsilon


def sum_money(values: Iterable) -> Decimal:
    """Sum an iterable of money-shaped values, safely.

    `sum()` seeded with 0 would produce `Decimal + int` results that
    don't quantize predictably. Seeding with Decimal("0") keeps the
    result a Decimal throughout, and any None / bad value in the
    iterable raises MoneyError immediately rather than silently
    contaminating the total.
    """
    total = Decimal("0")
    for v in values:
        total += money(v)
    return total


def recompute_invoice_totals(
    items: list,
    tax_rate,
) -> tuple[Decimal, Decimal, Decimal]:
    """Recompute (subtotal, tax_amount, total) from line items + tax rate.

    Implements bead A.2 — the server-side authoritative recomputation:

      subtotal   = Σ (item.quantity × item.rate)
      tax_amount = subtotal × (tax_rate / 100)   # rate is a percent
      total      = subtotal + tax_amount

    Each item is a dict with `quantity` and `rate` keys (camelCase FE
    contract). Missing values in an individual item raise MoneyError so
    a bad line surfaces at write time rather than producing a wrong
    total silently.

    All three return values are already quantized to money (2dp).

    Args:
      items: list of {quantity, rate, ...} dicts (or ORM objects with
        `.quantity` / `.rate` attributes — falls back to attr access
        if dict lookup misses).
      tax_rate: percent, e.g. Decimal("18") for 18% GST. May be None or
        0 — both mean "no tax" and produce tax_amount = ₹0.00.

    Returns:
      (subtotal, tax_amount, total), all Decimal quantized to 2dp.
    """
    subtotal = Decimal("0")
    for item in items:
        qty = _item_get(item, "quantity")
        rate = _item_get(item, "rate")
        subtotal += money(qty) * money(rate)

    if tax_rate is None:
        tax_pct = Decimal("0")
    else:
        tax_pct = money(tax_rate)

    # Tax rate is a percent (e.g. 18 for 18%), so divide by 100. Using
    # Decimal("100") keeps the result Decimal without any float ops.
    tax_amount = subtotal * (tax_pct / Decimal("100"))
    total = subtotal + tax_amount

    return q2(subtotal), q2(tax_amount), q2(total)


def _item_get(item, key: str):
    """Attribute/dict polymorphism helper — used inside the recompute
    pass so it accepts both incoming dict payloads (create/update) and
    already-ORM InvoiceItem rows (recompute-on-status-change)."""
    if isinstance(item, dict):
        if key not in item:
            raise MoneyError(f"item missing required field {key!r}: {item!r}")
        return item[key]
    # ORM-object path: raise if attr missing.
    if not hasattr(item, key):
        raise MoneyError(
            f"item {type(item).__name__} missing attribute {key!r}"
        )
    return getattr(item, key)


# ── migration-gap helper (ak-lvu Q1 A: declared-nullable + tolerant-service) ──
# The 6 new InvoicePayment columns (original_amount, original_currency,
# inr_amount, fx_rate, fx_rate_source, converted_at) are declared nullable
# in the ORM so `db.create_all()` picks up any missing TABLE (the new
# PendingPaymentClaim) automatically — but the ADD COLUMN on existing
# invoice_payments rows is a manual ALTER TABLE step. Between BE deploy
# and infra running the ALTER, service reads that touch these columns
# would raise `sqlalchemy.exc.OperationalError: Unknown column ...`.
#
# The `_is_migration_gap` helper (borrowed pattern from ak-5vg wealth-
# digest arc, and originally ak-ifc reconciliation_fallback) lets service
# code catch that specific error and gracefully return None / skip the
# new-column path — so a slightly-out-of-order deploy is a "feature
# temporarily degraded" rather than a "money endpoint 500s".
#
# Kept in money_utils so both invoiceService (write path) and
# paymentService (read path) can import from one place without a
# circular dep.

# String fragments MySQL / SQLAlchemy emit on missing-column errors.
# Matched case-insensitively so a driver-specific casing difference
# ("Unknown column" vs "unknown column") doesn't skip the catch.
_MIGRATION_GAP_SIGNATURES = (
    "unknown column",           # MySQL — the common case
    "no such column",           # SQLite fallback for local dev
    "does not exist",           # Postgres phrasing (defensive)
    "column not found",         # some drivers
)


def _is_migration_gap(exc: BaseException) -> bool:
    """True if `exc` looks like the "column not yet added by ALTER"
    error rather than a genuine DB fault.

    Caller pattern (invoiceService / paymentService):
        try:
            return payment.inr_amount
        except OperationalError as exc:
            if _is_migration_gap(exc):
                self.logger.warning("ak-lvu: pre-migration payment row, degrading gracefully")
                return None
            raise
    """
    if exc is None:
        return False
    msg = str(exc).lower()
    return any(sig in msg for sig in _MIGRATION_GAP_SIGNATURES)
