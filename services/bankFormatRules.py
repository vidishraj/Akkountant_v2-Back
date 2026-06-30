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
    """
    if "@" not in sender_email:
        return "UNKNOWN"
    domain = sender_email.split("@")[-1].strip().rstrip(">").lower()
    return DOMAIN_TO_BANK.get(domain, "UNKNOWN")


def get_format_rules(bank):
    """Return bank-specific format rules string for prompt injection.

    Returns empty string if no rules are defined for the given bank.
    """
    return BANK_FORMAT_RULES.get(bank, "")
