"""
MCP tool definitions for the mail processor agent.
These tools allow the AI to insert extracted financial data into the database
and manage PDF processing during autonomous email scanning.
"""

MAIL_PROCESSOR_TOOLS = [
    {
        "name": "insert_transaction",
        "description": "Insert a single bank transaction extracted from an email or statement. Amount should be positive for debits (money out) and negative for credits (money in).",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Transaction date and time. Include time if available in the email (e.g. DD/MM/YYYY HH:MM:SS or DD-MM-YYYY HH:MM). If only date is available, use DD/MM/YYYY or DD-MM-YYYY."
                },
                "description": {
                    "type": "string",
                    "description": "Transaction description (merchant name, transfer details, etc.)"
                },
                "amount": {
                    "type": "number",
                    "description": "Transaction amount. Positive = debit (money out), negative = credit (money in)."
                },
                "bank": {
                    "type": "string",
                    "description": "Bank name (e.g. HDFC_DEBIT, HDFC_REGALIA, ICICI_AMAZON_PAY, YES_BANK_DEBIT, YES_BANK_ACE, BOI, Millenia_Credit)"
                },
                "source": {
                    "type": "string",
                    "enum": ["Email", "Statement"],
                    "description": "Whether this came from an email alert or a PDF statement"
                },
                "bank_reference_id": {
                    "type": ["string", "null"],
                    "description": (
                        "ak-8l5: per-transaction bank-native identifier "
                        "extracted from the narration/description. Used as "
                        "the primary dedup key so chunk re-reads collapse "
                        "while legitimate same-tuple tx stay distinct. "
                        "Examples: HDFC UPI ref ('UPI-9876543210-P2M-…' → "
                        "'9876543210'), IMPS ref, NEFT UTR, BOI MBSF "
                        "number (middle numeric block of MBSF/…/…), "
                        "cheque number. RETURN NULL if the row genuinely "
                        "has no per-tx ref (cash deposit, interest post, "
                        "monthly fee) — the positional fallback handles "
                        "those. NEVER guess. Same tx across two chunks "
                        "must extract to the same ref string."
                    )
                },
                "line_position": {
                    "type": ["integer", "null"],
                    "description": (
                        "ak-8l5: 1-indexed line number within the RAW "
                        "statement file text. Populate when source is "
                        "Statement so ref-less rows can use the "
                        "positional-fallback hash. Leave null for Email "
                        "source (behavioral dedup handles that path)."
                    )
                },
                "gmail_message_id": {
                    "type": "string",
                    "description": "Gmail message ID for deduplication (optional)"
                },
            },
            # ak-8l5 M2: bank_reference_id + line_position are now
            # required on the schema. Both accept null, but the field
            # must be present in the payload — this makes the
            # extractor's "did I forget to think about it" pathology
            # visible instead of silent. Zero-loss is enforced at the
            # insert-time backstop regardless, so this is defense-in-
            # depth; a hallucinated non-null ref is caught by the
            # backstop's (date, amount, desc)-differs check.
            "required": [
                "date", "description", "amount", "bank", "source",
                "bank_reference_id", "line_position",
            ]
        }
    },
    {
        "name": "insert_batch_transactions",
        "description": (
            "Insert multiple transactions at once (e.g. all transactions from "
            "a bank statement PDF). Each row must carry the ak-8l5 dedup "
            "fields (bank_reference_id + line_position) — see the "
            "insert_transaction docstring for extraction rules per bank. "
            "Same tx read from two adjacent PDF chunks MUST emit the same "
            "bank_reference_id and the same line_position so the second "
            "insert becomes a no-op via the PK conflict path."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "transactions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "date": {"type": "string", "description": "DD/MM/YYYY HH:MM:SS or DD-MM-YYYY HH:MM. If only date available, DD/MM/YYYY or DD-MM-YYYY."},
                            "description": {"type": "string"},
                            "amount": {"type": "number", "description": "Positive=debit, negative=credit"},
                            "bank": {"type": "string"},
                            "bank_reference_id": {
                                "type": ["string", "null"],
                                "description": (
                                    "ak-8l5 primary dedup key. Extract the "
                                    "per-tx bank-native identifier from the "
                                    "narration (UPI ref, IMPS ref, NEFT UTR, "
                                    "MBSF number, cheque number). NULL is "
                                    "correct for genuinely ref-less rows "
                                    "(cash deposit / interest / fee) — the "
                                    "positional fallback handles those. "
                                    "Never guess. Same tx in adjacent "
                                    "chunks MUST extract the same ref."
                                )
                            },
                            "line_position": {
                                "type": ["integer", "null"],
                                "description": (
                                    "ak-8l5 positional fallback key. "
                                    "1-indexed line offset in the RAW file "
                                    "text (add chunk base if the file is "
                                    "chunked). Populate for every row so "
                                    "ref-less rows can dedup by position."
                                )
                            },
                        },
                        # ak-8l5 M2: dedup fields are required (nullable)
                        # on every batch row — same rationale as
                        # insert_transaction. Makes extractor drift
                        # visible; backstop enforces zero-loss.
                        "required": [
                            "date", "description", "amount",
                            "bank_reference_id", "line_position",
                        ]
                    },
                    "description": "Array of transaction objects to insert"
                },
                "bank": {
                    "type": "string",
                    "description": "Bank name for all transactions in this batch"
                },
                "source": {
                    "type": "string",
                    "enum": ["Email", "Statement"],
                    "description": "Source type for all transactions"
                },
                "file_id": {
                    "type": "string",
                    "description": "File ID to associate transactions with (for statements)"
                },
                "gmail_message_id": {
                    "type": "string",
                    "description": "Gmail message ID for deduplication"
                },
                "period_start": {
                    "type": "string",
                    "description": "Statement period start date in DD/MM/YYYY or YYYY-MM-DD format. Required for bank statements to enable reconciliation."
                },
                "period_end": {
                    "type": "string",
                    "description": "Statement period end date in DD/MM/YYYY or YYYY-MM-DD format. Required for bank statements to enable reconciliation."
                },
            },
            "required": ["transactions", "bank", "source"]
        }
    },
    {
        "name": "insert_epf_deposit",
        "description": "Insert an EPF (Employee Provident Fund) contribution. Typically extracted from EPF passbook PDFs.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Contribution date in DD-MM-YYYY format"
                },
                "employee_amount": {
                    "type": "number",
                    "description": "Employee's EPF contribution amount in INR"
                },
                "employer_amount": {
                    "type": "number",
                    "description": "Employer's EPF contribution amount in INR"
                },
                "description": {
                    "type": "string",
                    "description": "Description (e.g. 'Contribution for Jan 2025')"
                },
            },
            "required": ["date", "employee_amount", "employer_amount", "description"]
        }
    },
    {
        "name": "insert_ppf_deposit",
        "description": "Insert a PPF (Public Provident Fund) deposit.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Deposit date in DD-MM-YYYY format"
                },
                "amount": {
                    "type": "number",
                    "description": "Deposit amount in INR"
                },
                "description": {
                    "type": "string",
                    "description": "Description (e.g. 'Monthly deposit')"
                },
            },
            "required": ["date", "amount", "description"]
        }
    },
    {
        "name": "insert_gold_purchase",
        "description": "Insert a gold purchase record.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date": {
                    "type": "string",
                    "description": "Purchase date in DD-MM-YYYY format"
                },
                "amount": {
                    "type": "number",
                    "description": "Total amount paid in INR"
                },
                "quantity": {
                    "type": "number",
                    "description": "Quantity in grams"
                },
                "gold_type": {
                    "type": "string",
                    "enum": ["18", "22", "24"],
                    "description": "Gold purity (18/22/24 carat)"
                },
                "description": {
                    "type": "string",
                    "description": "Description (e.g. 'Gold coin', 'Digital gold')"
                },
            },
            "required": ["date", "amount", "quantity", "gold_type", "description"]
        }
    },
    {
        "name": "insert_investment",
        "description": "Insert a stock, mutual fund, or NPS purchase. Typically from investment confirmation emails.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Mutual_Funds", "NPS", "Stocks"],
                    "description": "Type of investment"
                },
                "scheme_code": {
                    "type": "string",
                    "description": "Scheme/security code"
                },
                "date": {
                    "type": "string",
                    "description": "Purchase date in DD-MM-YYYY format"
                },
                "quantity": {
                    "type": "number",
                    "description": "Units/shares purchased"
                },
                "amount": {
                    "type": "number",
                    "description": "Total amount invested in INR"
                },
            },
            "required": ["service_type", "date", "amount"]
        }
    },
    {
        "name": "mark_invoice_paid",
        # ak-lvu v2 F-8: terminology aligned with MP-3 claim-not-fact
        # semantics. Files a payment CLAIM — does NOT flip status.
        "description": (
            "File a payment CLAIM against a freelance invoice based on "
            "a payment-confirmation email. Does NOT flip the invoice "
            "status — writes a pending_payment_claim row that will be "
            "reconciled later (auto-matched against bank credits or "
            "manually confirmed via the claims-review UI). Return value "
            "includes claim_id for audit."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "description": "Invoice number the claim references."
                },
                "payment_date": {
                    "type": "string",
                    "description": "Claimed payment date in YYYY-MM-DD (as stated in mail)."
                },
                "amount_received": {
                    "type": "number",
                    "description": "Claimed amount received (in mail's currency)."
                },
                "currency": {
                    "type": "string",
                    "description": (
                        "Currency of the claimed amount (e.g. 'USD', "
                        "'INR'). Optional — defaults to invoice.currency."
                    ),
                },
                "payment_method": {
                    "type": "string",
                    "description": "Claimed payment method (e.g. 'bank_transfer', 'paypal', 'wise')."
                },
            },
            "required": ["invoice_number"]
        }
    },
    {
        "name": "get_pdf_pages",
        "description": "Render PDF pages as images for vision analysis. Supports fetching multiple pages at once to reduce round-trips. Returns page images and pagination info. Use page_range (e.g. '1-3') to fetch multiple pages in one call.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pdf_path": {
                    "type": "string",
                    "description": "Path to the PDF file on disk"
                },
                "password": {
                    "type": "string",
                    "description": "PDF password if the file is encrypted (optional)"
                },
                "page_number": {
                    "type": "integer",
                    "description": "Single page number to render (1-indexed). Use page_range instead for multiple pages."
                },
                "page_range": {
                    "type": "string",
                    "description": "Range of pages to fetch, e.g. '1-3', '4-6', or 'all'. Preferred over page_number for efficiency. Each page is returned as a separate image."
                },
            },
            "required": ["pdf_path"]
        }
    },
    {
        "name": "save_attachment",
        "description": "Save a processed PDF attachment to local storage for record keeping.",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Current path to the file"
                },
                "category": {
                    "type": "string",
                    "enum": ["bank_statement", "epf_passbook", "investment_confirmation", "gold_receipt", "freelance_payment", "freelance_contract", "freelance_correspondence"],
                    "description": "Category for organizing the file"
                },
                "filename": {
                    "type": "string",
                    "description": "Desired filename for the saved file"
                },
            },
            "required": ["source_path", "category", "filename"]
        }
    },
    {
        "name": "report_result",
        "description": "Report the processing outcome for a single email. Call this after processing each email to log the result. You MUST include gmail_message_id, sender, subject, email_date, and processing_type for every email.",
        "input_schema": {
            "type": "object",
            "properties": {
                "gmail_message_id": {
                    "type": "string",
                    "description": "Gmail message ID (from the email metadata)"
                },
                "sender": {
                    "type": "string",
                    "description": "Email sender address (From field)"
                },
                "subject": {
                    "type": "string",
                    "description": "Email subject line"
                },
                "email_date": {
                    "type": "string",
                    "description": "When the email was received, in YYYY-MM-DD format"
                },
                "category": {
                    "type": "string",
                    "description": "Detected category: transaction_alert, bank_statement, investment_confirmation, epf_passbook, gold_receipt, freelance_payment, freelance_contract, freelance_correspondence, or unknown"
                },
                "processing_type": {
                    "type": "string",
                    "enum": ["text", "pdf"],
                    "description": "How the email was processed — 'text' for email body analysis, 'pdf' for PDF attachment analysis"
                },
                "status": {
                    "type": "string",
                    "enum": ["success", "skipped", "error"],
                    "description": "Processing status"
                },
                "items_extracted": {
                    "type": "integer",
                    "description": "Number of data items extracted (transactions, deposits, etc.)"
                },
                "extraction_summary": {
                    "type": "object",
                    "description": "Compact result details, e.g. {\"inserted\": 5, \"duplicates\": 2, \"bank\": \"HDFC_DEBIT\"}"
                },
                "message": {
                    "type": "string",
                    "description": "Additional context or error message"
                },
            },
            "required": ["status", "gmail_message_id", "sender", "subject", "processing_type"]
        }
    },
]

# System prompt for batch text email processing (used with haiku for speed)
TEXT_EMAIL_SYSTEM_PROMPT = """\
You are a financial email processor for a personal finance app called Akkountant.
You will receive a batch of financial emails. For each email, classify it and extract structured data.

## Categories
- transaction_alert: Bank transaction notifications (UPI, card swipe, NEFT, IMPS, etc.)
- investment_confirmation: Mutual fund/stock/NPS purchase confirmations
- freelance_payment: Payment receipts from PayPal, Wise, Razorpay, Stripe
- unknown: Not a financial email or cannot extract data

## Extraction Rules

### For transaction_alert:
- Extract date, description (merchant/recipient), amount, and bank
- Amount should be POSITIVE for debits (money spent) and NEGATIVE for credits (money received)
- Bank should be one of: Millenia_Credit, HDFC_DEBIT, HDFC_REGALIA, ICICI_AMAZON_PAY, YES_BANK_DEBIT, YES_BANK_ACE, BOI
- If the bank doesn't match these exactly, use the closest match or the bank name from the email
- HDFC_REGALIA is for Visa Regalia USD forex card transactions — use this for any email mentioning "Regalia"

### For investment_confirmation:
- Extract date, scheme_name, quantity (units), amount, and type (buy/sell)
- Call the appropriate tool (insert_investment, insert_epf_deposit, etc.) to record the data
- Only call if you have at least date and amount
- In report_result, include the extracted investment data in extraction_summary:
  {"service_type": "Mutual_Funds", "investments": [{"scheme_code": "...", "date": "...", "amount": ..., "quantity": ...}]}

### For freelance_payment:
- Extract date, client, amount, currency, invoice_number
- Try to match against existing invoices using mark_invoice_paid (ak-lvu:
  this tool FILES A CLAIM — it does NOT flip the invoice status to paid.
  The claim gets reconciled later via bank-credit match or Overseer
  manual confirmation. Return value includes claim_id for audit trail.)

### For freelance_contract:
- Extract client name, contract type (SOW/NDA/agreement), date, key terms if visible
- In extraction_summary include: {"client": "...", "contract_type": "...", "date": "..."}

### For freelance_correspondence:
- Extract client name, topic/subject summary
- In extraction_summary include: {"client": "...", "topic": "..."}

## Instructions
1. Process each email one by one
2. Call the appropriate MCP tool for each extracted item
3. Call report_result for EVERY email processed — include all metadata:
   - gmail_message_id: the Gmail ID from the email header
   - sender: the From address
   - subject: the email subject line
   - email_date: the email date in YYYY-MM-DD format
   - processing_type: always "text" for these emails
   - category: what you classified the email as
   - items_extracted: count of items you inserted
   - extraction_summary: object with details like {"inserted": N, "duplicates": M, "bank": "BANK_NAME"}
4. Skip promotional, OTP, security, or non-financial emails (still call report_result with status "skipped")
5. Skip EMI amortization schedules / EMI conversion notices — these are NOT bank statements or transactions. Report with status "skipped".
6. If unsure about an email, classify as unknown and report with status "skipped"
"""

# System prompt for PDF analysis (used for both image and text modes)
PDF_SYSTEM_PROMPT = """\
You are a financial document analyzer for a personal finance app called Akkountant.
You will analyze financial PDFs from email attachments and extract all financial data.

## Document Types

### Bank Statement
- Extract ALL transactions: date, description, amount (positive=debit, negative=credit), running balance
- Call insert_batch_transactions with all transactions at once
- Bank should be one of: Millenia_Credit, HDFC_DEBIT, HDFC_REGALIA, ICICI_AMAZON_PAY, YES_BANK_DEBIT, YES_BANK_ACE, BOI
- HDFC_REGALIA is for Visa Regalia USD forex card statements — use this for any PDF mentioning "Regalia"
- Ignore header rows, summary sections, and promotional content
- Pay attention to date formats — Indian banks use DD/MM/YYYY or DD-MM-YYYY
- **IMPORTANT: Extract the statement period** from the document header:
  - Look for "Statement Period", "Period", "From/To" dates in the header
  - For savings/current accounts: typically month-based (e.g., 01/01/2025 to 31/01/2025)
  - For credit cards: billing cycles (e.g., 03/11/2024 to 02/12/2024)
  - Always include `period_start` and `period_end` when calling `insert_batch_transactions`
  - Format as DD/MM/YYYY (matching the transaction date format)

### ak-8l5 per-transaction identifiers (bank_reference_id) — REQUIRED

Every row you emit MUST carry both `bank_reference_id` (nullable) and
`line_position` (1-indexed integer within the RAW statement text —
add the chunk's base line offset if you're processing a chunk). These
two fields drive the dedup engine: chunk re-reads of the same row
must produce the same values so the second insert becomes a no-op
via the PK conflict path, while distinct rows must produce different
values.

**bank_reference_id — extraction rules per bank.** Look for the
per-transaction identifier bank writers put in the narration column.
Return NULL if the row genuinely has no per-tx ref (cash deposit,
interest post, monthly fee) — never guess.

HDFC_DEBIT — search narration in this order:
  - "UPI-<digits>-P2M-..." or "UPI-<digits>-P2A-..." → the digit block
  - "UPI/<digits>/..." → the digit block
  - "IMPS-P2A-<digits>-..." → the digit block
  - "NEFT-<UTR>-..." → the UTR (12-16 alphanumeric)
  - "ACH D-<ref>-..." / "ACH C-<ref>-..." → the ref segment
  - "POS <terminal-id> <txn-ref>" → composite `<terminal>_<txn-ref>`
  - "ATM WDL <machine-id> <txn-ref>" → composite `<machine>_<txn>`
  - "CHQ PAID/<cheque-number>" → the cheque number
  - "CASH DEP", "INT PD", "FEE", "MIN BAL CHG" → NULL (no per-tx ref)

BOI — search narration in this order:
  - "MBSF/<numeric>/<narration>" → the middle numeric block (this
    is a stable per-tx ref, NOT the branch code)
  - "Int:<start-date>/<end-date>" → composite `INT_<start>_<end>`
    (interest posts have a date-window that's the ref)
  - "LOAN COLL/<ref>" / "EMI/<ref>" / "SI/<ref>" → the trailing ref
  - "SWEEP TRF/<numeric>" → the numeric block
  - "BY CASH-<branch-code>-<location>" → NULL (branch code is NOT
    per-tx unique — treat these as ref-less; positional fallback
    engages)
  - Cheque txns → the cheque number
  - "Chg" / "Fee" / plain interest lines → NULL

Other banks — same principle: extract only if the ref is
per-transaction unique AND stable across chunk re-reads. If the
candidate identifier is really a merchant ID or a branch code
(same value for many transactions), return NULL and let the
positional fallback engage.

**line_position — 1-indexed line offset in the RAW statement text.**
When the file is chunked, ADD the chunk's base offset to the local
line number before emitting so the position stays stable across
chunk boundaries. A tx that visually straddles pages 3-4 must emit
the SAME line_position no matter which chunk sees it — the position
is where the row's date/amount appear in the raw file text, not the
page number.

### EPF Passbook
- Extract all contribution entries: date, employee_deposit, employer_deposit, description
- Call insert_epf_deposit for each contribution entry
- Look for columns like: Date, Description, Employee Share, Employer Share, Pension Contribution

### Investment Statement (MF/NPS/Stock)
- Extract holdings and transaction history
- For each purchase: call insert_investment with scheme details, date, quantity, amount
- In report_result, include extraction_summary with:
  {"service_type": "Mutual_Funds", "investments": [{"scheme_code": "...", "date": "...", "amount": ..., "quantity": ...}]}

### Gold Receipt
- Extract purchase details: date, amount, quantity (grams), gold type (purity)
- Call insert_gold_purchase
- In report_result, include extraction_summary with:
  {"service_type": "Gold", "purchases": [{"date": "...", "amount": ..., "quantity": ..., "gold_type": "..."}]}

## Instructions
1. Extract data from the PDF content provided (images or text).
2. Call insert_batch_transactions (or the appropriate insert tool) with ALL extracted data.
   IMPORTANT: Always call insert_batch_transactions after extracting — never skip it.
3. Call report_result when done — include all metadata:
   - gmail_message_id: the Gmail ID from the email header
   - sender: the From address
   - subject: the email subject line
   - email_date: the email date in YYYY-MM-DD format
   - processing_type: always "pdf" for these emails
   - category: the document type (bank_statement, epf_passbook, investment_confirmation, gold_receipt, etc.)
   - items_extracted: count of items you inserted
   - extraction_summary: object with details like {"inserted": N, "duplicates": M, "bank": "BANK_NAME"}
4. If the PDF is password-protected and fails to open, report an error via report_result with status "error"

## SKIP — Do NOT extract data from these:
- EMI amortization schedules / EMI conversion notices (subjects like "EMI on Card", "EMI conversion confirmation")
  These contain repayment schedules, NOT actual bank transactions. Report them with status "skipped" and category "unknown".
- Loan disbursement letters, insurance policy documents, reward point statements

## Important
- Be thorough — extract EVERY transaction/entry from the document
- Handle multi-page documents correctly (don't re-process headers on subsequent pages)
- Watch for special characters in amounts (commas as thousand separators in Indian format: 1,00,000)
- Dates in Indian format: DD/MM/YYYY (not US MM/DD/YYYY)
"""
