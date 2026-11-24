"""ak-3oh (2026-11-24): read-only audit of non-INR invoice payments written
before ak-lvu.

Pre-ak-lvu, InvoicePayment.amount_received held the INR value (no FX
metadata). The AC-1 bug: a PUT that echoed that INR value back as if it were
in the invoice's currency re-converted it, so a paid USD 100 invoice could
store ~83 x 83 INR instead of ~8,325. ak-lvu fixed the write path; old rows
were never reviewed.

Classification is by ORDER OF MAGNITUDE, so coarse reference rates suffice:
for a PAID invoice, r = paid amount_received / invoice.total (invoice
currency). With the currency's INR rate R:
    r ~ R**1  -> OK (INR, as intended)
    r ~ R**2  -> DOUBLE_CONVERTED (the AC-1 class, overstated earnings)
    r ~ R**0  -> UNCONVERTED (stored in invoice currency, understated)
    otherwise -> AMBIGUOUS (manual review)
Distances are compared in log space; the tolerance is capped below half the
gap between adjacent levels, so classes never overlap (JPY's R ~ 0.56 is
close to 1). Partially paid invoices can't be judged from a ratio:
counted, not classified.

Rows written after ak-lvu (original_amount present) are checked for
internal consistency instead: inr_amount == original_amount x fx_rate, and
the V4-1 shape (an INR value stored as the invoice-currency
original_amount: original_amount / total ~ R).

Pure functions; no DB, no logging. The report holds counts and internal
invoice IDS only, never amounts, ratios or invoice numbers. JPY levels are
only ~1.8x apart, so JPY results are labelled approximate.
"""
from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation

# Coarse INR per unit (order-of-magnitude reference only; never used to
# convert money). Mirrors CurrencyService.fallback_rates_to_inr.
REFERENCE_INR_RATES = {
    "USD": 83.25, "GBP": 105.50, "EUR": 90.75, "JPY": 0.56,
    "CAD": 61.50, "AUD": 54.25,
}
# Rates within 2x of 1: the R**0 / R**1 / R**2 levels sit close together.
APPROXIMATE_CURRENCIES = frozenset(c for c, r in REFERENCE_INR_RATES.items() if 0.5 < r < 2)
MAX_TOLERANCE = 1.35          # +-35%: FX drift across years + TDS / fees
CONSISTENCY_TOLERANCE = 0.01  # 1% for inr_amount vs original x fx_rate

OK = "ok_inr"
DOUBLE_CONVERTED = "suspect_double_converted"
UNCONVERTED = "suspect_unconverted"
AMBIGUOUS = "ambiguous_manual_review"
PARTIAL = "partial_not_classifiable"
NO_PAYMENT = "paid_without_payment_row"
UNKNOWN_CURRENCY = "unknown_currency"
ZERO_TOTAL = "zero_total"
POST_OK = "post_lvu_consistent"
POST_INR_MISMATCH = "post_lvu_inr_mismatch"
POST_ORIGINAL_IS_INR = "post_lvu_original_amount_looks_inr"
MIXED = "mixed_legacy_and_post_lvu"

SUSPECT_CLASSES = (DOUBLE_CONVERTED, UNCONVERTED, AMBIGUOUS, NO_PAYMENT,
                   POST_INR_MISMATCH, POST_ORIGINAL_IS_INR, MIXED, UNKNOWN_CURRENCY)


def _dec(v):
    try:
        return Decimal(str(v)) if v is not None else None
    except (InvalidOperation, ValueError):
        return None


def tolerance_for(rate):
    """Log-space tolerance: at most MAX_TOLERANCE and strictly below half
    the gap between adjacent levels (R**0, R**1, R**2)."""
    gap = abs(math.log(rate))
    return min(math.log(MAX_TOLERANCE), gap / 2 * 0.95)


def magnitude_class(ratio, rate):
    """OK / DOUBLE_CONVERTED / UNCONVERTED / AMBIGUOUS for ratio ~ rate**k."""
    if ratio is None or ratio <= 0 or rate is None or rate <= 0 or rate == 1:
        return AMBIGUOUS
    lr, lrate = math.log(ratio), math.log(rate)
    tol = tolerance_for(rate)
    for k, label in ((1, OK), (2, DOUBLE_CONVERTED), (0, UNCONVERTED)):
        if abs(lr - k * lrate) <= tol:
            return label
    return AMBIGUOUS


def classify_invoice(currency, status, total, payments):
    """One non-INR invoice -> a class label.

    payments: dicts {amount_received, original_amount, original_currency,
    inr_amount, fx_rate}."""
    currency = (currency or "").upper()
    rate = REFERENCE_INR_RATES.get(currency)
    if rate is None:
        return UNKNOWN_CURRENCY
    total_d = _dec(total)
    if not payments:
        return NO_PAYMENT if status == "paid" else None
    legacy = [p for p in payments if p.get("original_amount") is None]
    post = [p for p in payments if p.get("original_amount") is not None]
    if legacy and post:
        return MIXED
    if post:
        for p in post:
            inr, orig, fx = _dec(p.get("inr_amount")), _dec(p.get("original_amount")), _dec(p.get("fx_rate"))
            if inr is not None and orig is not None and fx is not None and orig > 0:
                expected = orig * fx
                if expected > 0 and abs(inr - expected) / expected > Decimal(str(CONSISTENCY_TOLERANCE)):
                    return POST_INR_MISMATCH
        same_cur = [p for p in post if (p.get("original_currency") or "").upper() == currency]
        if status == "paid" and same_cur and total_d and total_d > 0:
            orig_sum = sum((_dec(p.get("original_amount")) or Decimal("0")) for p in same_cur)
            if magnitude_class(float(orig_sum / total_d), rate) == OK:
                return POST_ORIGINAL_IS_INR      # INR value stored as invoice currency
        return POST_OK
    if status != "paid":
        return PARTIAL
    if not total_d or total_d <= 0:
        return ZERO_TOTAL
    paid = sum((_dec(p.get("amount_received")) or Decimal("0")) for p in legacy)
    return magnitude_class(float(paid / total_d), rate)


class Report:
    """Counts + invoice numbers per class. Never holds amounts."""

    def __init__(self):
        self.by_class = {}
        self.by_currency = {}
        self.scanned = 0
        self.approximate = set()

    def add(self, invoice_number, currency, label):
        self.scanned += 1
        if label is None:
            return
        self.by_class.setdefault(label, []).append(str(invoice_number))
        if (currency or "").upper() in APPROXIMATE_CURRENCIES:
            self.approximate.add((currency or "").upper())
        self.by_currency[(currency or "?").upper()] = self.by_currency.get(
            (currency or "?").upper(), 0) + 1

    def lines(self):
        out = [f"non-INR invoices scanned: {self.scanned}"]
        for cur in sorted(self.by_currency):
            out.append(f"  classified in {cur}: {self.by_currency[cur]}")
        for label in sorted(self.by_class):
            out.append(f"{label}: {len(self.by_class[label])}")
        for label in SUSPECT_CLASSES:
            if self.by_class.get(label):
                out.append(f"{label} invoice ids: {', '.join(sorted(self.by_class[label]))}")
        for cur in sorted(self.approximate):
            out.append(f"note: {cur} classes are APPROXIMATE (levels under 2x apart); "
                       f"review those invoices manually")
        return out
