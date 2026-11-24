"""ak-n0cg (2026-11-24): server-side evidence for mail-filed payment claims.

mark_invoice_paid (ak-lvu) never flips an invoice; it files a
PendingPaymentClaim for later reconciliation. The mail agent reads
attacker-influenceable email, so the claim must carry SERVER-computed
evidence binding it to the email that triggered it, not just the model's
say-so:

  * sender: the From address belongs to the invoice's customer (exact
    address, or same non-freemail domain), or is a known payment
    processor (PayPal/Wise/Stripe/Razorpay/Payoneer receipts are the
    freelance-payment emails the prompt handles);
  * amount: the claimed amount equals the invoice total (and currency,
    when the model supplies one, matches);
  * reference: the invoice number appears in the email subject/body.

`all_match` False → the claim is flagged requires_user_confirmation.
Nothing here auto-confirms anything; it records the evidence and the flag.
Only flags and the sender DOMAIN are returned, never email content
(personal data). Leaf module: stdlib only.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from email.utils import parseaddr

PAYMENT_PROCESSOR_DOMAINS = frozenset({
    "paypal.com", "paypal.in", "wise.com", "transferwise.com",
    "stripe.com", "razorpay.com", "payoneer.com",
})
FREEMAIL_DOMAINS = frozenset({
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com",
    "live.com", "yahoo.com", "yahoo.co.in", "icloud.com", "me.com",
    "proton.me", "protonmail.com", "aol.com", "zoho.com",
})
AMOUNT_TOLERANCE = Decimal("0.01")


def _addr(value):
    _name, addr = parseaddr(str(value or ""))
    addr = addr.strip().lower()
    return addr if "@" in addr else ""


def _domain(addr):
    return addr.rsplit("@", 1)[1] if "@" in addr else ""


def _in_domain_set(domain, domains):
    return any(domain == d or domain.endswith("." + d) for d in domains)


def _dec(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def evaluate_claim_evidence(*, sender, subject, body, invoice_number,
                            invoice_total, invoice_currency,
                            claimed_amount, claimed_currency,
                            customer_emails):
    s_addr = _addr(sender)
    s_dom = _domain(s_addr)
    cust_addrs = {a for a in (_addr(e) for e in (customer_emails or [])) if a}
    cust_doms = {_domain(a) for a in cust_addrs} - FREEMAIL_DOMAINS

    if s_addr and s_addr in cust_addrs:
        sender_match = "customer"
    elif s_dom and s_dom in cust_doms:
        sender_match = "customer_domain"
    elif s_dom and _in_domain_set(s_dom, PAYMENT_PROCESSOR_DOMAINS):
        sender_match = "processor"
    else:
        sender_match = "none"

    total = _dec(invoice_total)
    claimed = _dec(claimed_amount)
    amount_match = (total is not None and claimed is not None
                    and abs(claimed - total) <= AMOUNT_TOLERANCE)
    inv_cur = str(getattr(invoice_currency, "value", invoice_currency) or "").upper()
    cl_cur = str(claimed_currency or "").upper()
    currency_match = (not cl_cur) or (cl_cur == inv_cur)

    ref = str(invoice_number or "").strip().lower()
    hay = f"{subject or ''}\n{body or ''}".lower()
    reference_in_email = bool(ref) and ref in hay

    all_match = (sender_match != "none" and amount_match and currency_match
                 and reference_in_email)
    return {
        "sender_match": sender_match,
        "sender_domain": s_dom or None,
        "amount_match": amount_match,
        "currency_match": currency_match,
        "reference_in_email": reference_in_email,
        "all_match": all_match,
    }
