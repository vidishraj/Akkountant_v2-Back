"""
Bank-specific format rules for PDF statement parsing.

Maps sender domains to bank identifiers and provides bank-specific
parsing instructions that get injected into the Claude agent's prompt.
"""

# Maps sender email domain → bank identifier
DOMAIN_TO_BANK = {
    # HDFC
    "hdfcbank.net": "HDFC_DEBIT",
    "hdfcbank.com": "HDFC_DEBIT",
    "hdfcbank.bank.in": "HDFC_DEBIT",
    # ICICI
    "icicibank.com": "ICICI_AMAZON_PAY",
    "icicibank.co.in": "ICICI_AMAZON_PAY",
    # YES Bank
    "yesbank.in": "YES_BANK_DEBIT",
    "yesbank.co.in": "YES_BANK_DEBIT",
    # Bank of India
    "bankofindia.co.in": "BOI",
    "bankofindia.com": "BOI",
    # ak-2ql: 2026-03-07 RBI-mandated `.bank.in` TLD migration.
    # New sender domain: alerts.bankofindia.bank.in. Mirrors the
    # earlier HDFC migration (see hdfcbank.bank.in above).
    "bankofindia.bank.in": "BOI",
    # Axis
    "axisbank.com": "AXIS",
    # Kotak
    "kotak.com": "KOTAK",
    # SBI
    "sbi.co.in": "SBI",
}

# Bank-specific parsing instructions injected into the chunk prompt.
# These describe the column format and sign/date rules for each bank.
BANK_FORMAT_RULES = {
    "HDFC_DEBIT": """\
#### HDFC Smart Statement Column Format (CRITICAL)
HDFC savings/current account statements have these columns:
  `Date | Narration | Chq./Ref.No. | Value Dt | Withdrawal Amt (Dr) | Deposit Amt (Cr) | Closing Balance`

The extracted text for each transaction row will contain TWO amount fields before the closing balance:
- **First amount** = Withdrawal Amount (debit)
- **Second amount** = Deposit Amount (credit)
- One of these will be `0.00` and the other will be the actual amount.

Rules for determining sign:
- If Withdrawal Amount is non-zero and Deposit Amount is `0.00` → this is a **debit** → amount is **positive**
- If Deposit Amount is non-zero and Withdrawal Amount is `0.00` → this is a **credit** → amount is **negative**
- NEVER look at just one number — always identify BOTH the withdrawal and deposit fields to determine the sign
- The Closing Balance column comes AFTER both amounts — do NOT confuse it with a transaction amount

Each transaction in the extracted text spans multiple lines in this order:
```
<DATE>                              ← Transaction Date (USE THIS)
<NARRATION LINE 1>                  ← Narration (may wrap to next line)
<NARRATION LINE 2 if wrapped>
<REF NUMBER>                        ← Ref number (digits only)
<DATE>                              ← Value Date (IGNORE — do NOT use as transaction date)
<AMOUNT or 0.00>                    ← Withdrawal Amount
<AMOUNT or 0.00>                    ← Deposit Amount
<BALANCE>                           ← Closing Balance
```

**DATE RULE**: Each transaction has TWO dates. The **first date** (before the narration) is the Transaction Date — always use this. The **second date** (after the ref number, before the amounts) is the Value Date — IGNORE it. They often differ by 1-2 days.

**SIGN RULE**: After the Value Date come two amounts then the closing balance:
- First amount = Withdrawal (debit) → positive if non-zero
- Second amount = Deposit (credit) → negative if non-zero
- One will always be `0.00`
""",
}


def get_bank_from_sender(sender_email):
    """Extract bank identifier from sender email domain.

    Args:
        sender_email: Full sender string, e.g. 'HDFC Bank <alerts@hdfcbank.net>'

    Returns:
        Bank identifier string (e.g. 'HDFC_DEBIT') or 'UNKNOWN'

    ak-x6p fix (hq-wisp-i30l3w): banks increasingly send from
    subdomain-prefixed addresses (`alerts.bankofindia.bank.in`,
    `noreply.hdfcbank.com`, etc.) that don't hit exact match against
    DOMAIN_TO_BANK's canonical keys. Post-fix we try exact match
    FIRST (backward-compatible), then a dot-boundary suffix match
    to resolve subdomain-prefixed senders to their base bank.

    The BOI backfill (ak-5oi) surfaced this: BOI's new sender is
    `noreply-estatement@alerts.bankofindia.bank.in`, and the
    domain-lookup returned "UNKNOWN". Downstream that "UNKNOWN"
    hint got injected into the LLM prompt ("Use bank=\"UNKNOWN\"...")
    and stamped onto fileDetails.bank — the visible ak-x6p symptom.
    """
    if "@" not in sender_email:
        return "UNKNOWN"
    domain = sender_email.split("@")[-1].strip().rstrip(">").lower()
    # Exact match — preserves prior behavior for senders whose
    # domain IS the canonical key (e.g. `noreply@hdfcbank.net`).
    if domain in DOMAIN_TO_BANK:
        return DOMAIN_TO_BANK[domain]
    # Dot-boundary suffix match — resolves `alerts.<canonical>`,
    # `noreply.<canonical>`, `mail.<canonical>`, etc. The leading dot
    # requirement prevents `evilbankofindia.bank.in` from silently
    # matching `bankofindia.bank.in`.
    for known_domain, bank_id in DOMAIN_TO_BANK.items():
        if domain.endswith("." + known_domain):
            return bank_id
    return "UNKNOWN"


def get_banks_for_domain(sender_domain):
    """List of possible bank identifiers for a raw domain string.

    Used by mailProcessorService's password-lookup helpers, which
    historically maintained their own inline domain→bank maps in
    two places (lines 1122 + 1150 of mailProcessorService.py).
    Same subdomain-blind problem as get_bank_from_sender: the sender
    `alerts.bankofindia.bank.in` misses the map's `bankofindia.bank.in`
    key. Returning a suffix-tolerant list here lets those callers
    stop duplicating the map data.

    Returns a list because a single canonical domain can host
    multiple bank identifiers (e.g. hdfcbank.net covers HDFC_DEBIT,
    Millenia_Credit, HDFC_REGALIA — the callers need to try each
    password key in turn). We preserve the original insertion order
    of DOMAIN_TO_BANK so existing "which bank to try first" behavior
    isn't perturbed.

    ak-x6p fix (hq-wisp-i30l3w): also fixes the parallel password-
    lookup subdomain miss.
    """
    if not sender_domain:
        return []
    domain = sender_domain.strip().rstrip(">").lower()
    if not domain:
        return []
    matched = []
    # Exact match — walk DOMAIN_TO_BANK preserving insertion order so
    # callers that want "closest sibling first" (e.g. try the primary
    # HDFC_DEBIT password before Millenia_Credit) still get that.
    for known_domain, bank_id in DOMAIN_TO_BANK.items():
        if domain == known_domain and bank_id not in matched:
            matched.append(bank_id)
    if matched:
        return matched
    # Suffix match — same dot-boundary rule as get_bank_from_sender.
    for known_domain, bank_id in DOMAIN_TO_BANK.items():
        if domain.endswith("." + known_domain) and bank_id not in matched:
            matched.append(bank_id)
    return matched


def get_format_rules(bank):
    """Return bank-specific format rules string for prompt injection.

    Returns empty string if no rules are defined for the given bank.
    """
    return BANK_FORMAT_RULES.get(bank, "")
