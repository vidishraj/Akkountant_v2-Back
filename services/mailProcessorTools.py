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
                "gmail_message_id": {
                    "type": "string",
                    "description": "Gmail message ID for deduplication (optional)"
                },
            },
            "required": ["date", "description", "amount", "bank", "source"]
        }
    },
    {
        "name": "insert_batch_transactions",
        "description": "Insert multiple transactions at once (e.g. all transactions from a bank statement PDF). Each item follows the same schema as insert_transaction.",
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
                        },
                        "required": ["date", "description", "amount"]
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
        "description": "Mark a freelance invoice as paid based on payment confirmation email.",
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "description": "The invoice number to mark as paid"
                },
                "payment_date": {
                    "type": "string",
                    "description": "Payment date in YYYY-MM-DD format"
                },
                "amount_received": {
                    "type": "number",
                    "description": "Amount received"
                },
                "payment_method": {
                    "type": "string",
                    "description": "Payment method (e.g. 'bank_transfer', 'paypal', 'wise')"
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
- Try to match against existing invoices using mark_invoice_paid

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
