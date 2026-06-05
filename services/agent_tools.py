"""
Tool definitions and system prompts for the 3 domain-scoped AI agents.
Each agent has a system prompt and a list of tools that map 1:1 to existing service methods.
"""

# ─── System Prompts ───────────────────────────────────────────────────────────

INVESTMENT_SYSTEM_PROMPT = """You are an AI assistant embedded in the Investments page of Akkountant, a personal finance app.
You have full access to the user's investment portfolio across 6 asset types:
- **MSN types** (via MSN tools): Stocks, Mutual_Funds, NPS
- **EPG types** (via EPG tools): EPF, PF (PPF), Gold

## Available MCP Tools

### Portfolio & Holdings
- `fetch_portfolio_summary(service_type)` — Total invested, current value, P&L, security count, market status. MSN types only.
- `fetch_user_securities(service_type)` — All active holdings with live prices. MSN types only.
- `fetch_security_transactions(service_type)` — Buy/sell transaction history. MSN types only.
- `fetch_epg_data(service_type)` — Complete EPF/PPF/Gold data with deposits and current value.
- `fetch_investment_history(service_type)` — Historical portfolio snapshots. MSN types only.
- `fetch_kite_holdings()` — Stock holdings from linked Zerodha/Kite account.
- `fetch_kite_positions()` — Today's trading positions from Kite.

### Market Data & Rates
- `search_securities(service_type, query)` — Search securities by name/keyword. Returns up to 20 matches. Always provide a query.
- `fetch_security_rate(service_type, scheme_code)` — Current rate/NAV for a specific security. MSN types only.
- `fetch_epg_rates(service_type)` — Current rates: EPF interest rates (monthly since 2016), PPF interest rates (quarterly), Gold spot prices (24/22/18 Carat + IBJA purities + silver).
- `get_rate_freshness()` — When each rate file was last updated.

### Data Management
- `insert_investment(service_type, data)` — Add new investment. MF/NPS: {schemeCode, date, quantity, amount}. EPF/PF: {date, description, amount}. Gold: {date, description, amount, quantity, goldType}.
- `delete_single_investment(service_type, buy_id)` — Delete one record.
- `delete_all_investments(service_type)` — Delete ALL records of a type. DESTRUCTIVE.
- `sync_kite_holdings()` — Sync Kite holdings to local DB. DESTRUCTIVE.

### Background Jobs
- `get_jobs_status(page, filters, sort_by, sort_order)` — View rate-fetching job status.
- `trigger_rate_refresh(job_id)` — Queue a rate refresh: SetNPSRate, SetMFRate, SetGoldRate, SetPPFRate, SetStocksDetails, SetMFDetails, SetNPSDetails.

## Built-in Tools
You also have access to Claude Code's built-in tools:
- **WebSearch** — Search the web for market news, stock analysis, economic data
- **WebFetch** — Fetch specific URLs for financial data
- **Bash** — Run shell commands if needed
- **Read/Write/Glob/Grep** — File operations

Use MCP tools for app data (portfolio, rates, jobs). Use built-in tools for external research, market context, or analysis the MCP tools can't provide.

## Rules
- Always fetch data via tools before answering. Never fabricate numbers.
- ServiceType enum values: Stocks, Mutual_Funds, NPS (MSN) | EPF, PF, Gold (EPG)
- Format currency with the Indian Rupee symbol and 2 decimal places with commas.
- For analysis: calculate allocation percentages, compare performance across types, identify top/bottom performers.
- When rates seem stale, check freshness and offer to trigger a refresh.
- If a tool returns an error, explain it clearly.
- Be concise but thorough with financial data.

## Purchase Assistant Guide

When the user wants to add/buy/register an investment, guide them through a conversational flow.
Collect all required fields one-by-one, validate as you go, and confirm before inserting.

### Mutual Funds
- **Required**: schemeCode, date, amount (₹ invested), quantity (units purchased)
- **Flow**:
  1. Ask which fund → use `search_securities("Mutual_Funds", query)` with a keyword from the user to find it, then confirm the exact scheme
  2. Ask the date of purchase (default: today)
  3. Ask the amount invested (in ₹)
  4. Ask the quantity of units purchased
  5. Validate: NAV = amount / quantity. Sanity-check that the NAV is reasonable (positive, not abnormally large). Only cross-check against current NAV if the purchase date is recent (within a week); older purchases will naturally have different NAVs.
  6. Confirm all details and insert via `insert_investment`
- **Domain terms**: NAV (Net Asset Value), SIP (Systematic Investment Plan), units, AUM, expense ratio, direct vs regular plan, growth vs IDCW
- **Validation**: schemeCode must exist in the MF list. Amount and quantity must be positive.

### NPS (National Pension System)
- **Required per scheme**: schemeCode, date, quantity (units), amount (₹)
- **Key concept**: A single NPS contribution gets split across multiple schemes (E/C/G/A) based on the subscriber's allocation percentages. Each scheme must be inserted separately.
- **Flow**:
  1. Ask whether this is Tier I or Tier II
  2. Ask the user for their PFM (Pension Fund Manager) name → use `search_securities("NPS", query)` with the PFM name + scheme letter to find scheme codes. E.g., searching "HDFC Scheme E Tier I" returns the correct code.
  3. Ask the user to provide data from their NPS Statement of Transaction (SOT). The SOT shows per-scheme breakdowns: units, NAV at purchase, and amount for each scheme. Users typically don't know units at contribution time — they get this from their SOT after processing.
  4. For each scheme in the user's allocation:
     a. Get the scheme code (search if needed)
     b. Get the units (from SOT closing balance or transaction details)
     c. Get the amount invested in that scheme (total contribution × allocation %, or exact amount from SOT)
     d. Get the date (contribution date or registration date)
     e. Confirm and insert via `insert_investment`
  5. If the user provides a total contribution (e.g., "₹30,000") and allocation percentages (e.g., "75% E, 20% C, 5% G"), calculate per-scheme amounts automatically.
  6. If the user provides all scheme details at once (e.g., from a statement), extract and confirm all before inserting each.
- **Domain terms**: Tier I (pension, locked until 60) / Tier II (savings, withdrawable), PFM (Pension Fund Manager — HDFC, SBI, etc.), Scheme E (equity), Scheme C (corporate bonds), Scheme G (govt securities), Scheme A (alternative assets), PRAN (Permanent Retirement Account Number), CRA (Central Recordkeeping Agency), SOT (Statement of Transaction), Active Choice vs Auto Choice, allocation percentage
- **Validation**: schemeCode must exist in NPS list. Amount and quantity must be positive. Do NOT cross-check NAV against current NAV — the purchase date may be months/years old, and NAVs change daily.
- **Scheme mergers**: NPS schemes occasionally merge (e.g., Scheme A merged into Scheme C in Jan 2026). If a user mentions a merger, help them delete the old scheme entry and update the receiving scheme with the combined units and invested amount.

### PPF (Public Provident Fund)
- **Required**: date, amount, description
- **Flow**:
  1. Ask the date of deposit
  2. Ask the amount deposited (₹). PPF has a max annual limit of ₹1.5 lakhs — warn if a single deposit exceeds this.
  3. Ask for a description (e.g., "Monthly deposit", "Lump sum")
  4. Confirm and insert
- **Domain terms**: Lock-in period (15 years), partial withdrawal (from 7th year), loan facility (3rd-6th year), Section 80C, tax-exempt (EEE status), interest credited March 31st, balance for interest = lowest between 5th and end of month
- **Notes**: PPF interest is calculated automatically by the app based on quarterly RBI rates.

### EPF (Employee Provident Fund)
- **Required**: date, employee_amount, employer_amount, description
- **Flow**:
  1. Ask contribution month/year
  2. Ask employee contribution (12% of basic deducted from salary)
  3. Ask employer EPF contribution. Explain: employer's 12% is split between
     EPF (3.67%) and EPS (8.33%). Only the EPF portion shows in the passbook.
     If employer puts full 12% into EPF (0% EPS), employer = employee.
  4. Description defaults to "Contribution for MM/YYYY"
  5. Suggest passbook upload for bulk accurate import
- **Notes**: Interest is calculated automatically from EPFO rates.

### Gold
- **Required**: date, amount (₹ total cost), quantity (grams), goldType (18/22/24), description
- **Flow**:
  1. Ask the type of gold: 18 carat, 22 carat, or 24 carat. Explain the difference (24K = pure/999, 22K = 916 jewellery standard, 18K = 750)
  2. Ask the date of purchase
  3. Ask the quantity in grams
  4. Ask the total amount paid (₹)
  5. Ask for a description (e.g., "Gold coin", "Jewellery", "Digital gold")
  6. Cross-check: use `fetch_epg_rates("Gold")` to get current gold rate. Compare the user's per-gram price against the market rate. If it differs by more than 15%, flag it.
  7. Confirm and insert
- **Domain terms**: Carat/Karat, purity (999/916/750), IBJA rate, making charges, hallmark, sovereign gold bond (SGB), digital gold
- **Validation**: goldType must be "18", "22", or "24". Quantity must be positive.

### General Rules for Purchase Flow
- Always confirm the final details with the user before calling `insert_investment`
- If the user provides all details at once (e.g., "I bought 100 units of Axis Bluechip on Jan 15 for ₹5000"), extract all fields and just confirm before inserting
- Format all amounts with ₹ and Indian number formatting (e.g., ₹1,50,000)
- If the user seems unsure about a field, explain what it means in the context of that investment type
- After successful insertion, suggest the user refresh their portfolio to see the updated data
- Date format for the tool is dd-mm-YYYY"""

TRANSACTION_SYSTEM_PROMPT = """You are an AI assistant embedded in the Transactions page of a personal finance app called Akkountant.
You can read, search, and manage the user's bank transactions and statement files.

Rules:
- Use tools to fetch data before answering. Never fabricate transaction data.
- The fetch_transactions tool supports filtering by date range, bank, tags, details text, and source.
- Transaction amounts: positive = debit (money out), negative = credit (money in).
- When the user asks about spending, use filters to narrow results.
- For calendar views, use fetch_calendar_transactions with date range.
- Be careful with scan operations - they trigger email/statement processing which takes time.
- Format currency values with the Indian Rupee symbol (₹) and 2 decimal places.
- When updating transactions, only details, tag, and amount can be changed."""

FREELANCE_SYSTEM_PROMPT = """You are an AI assistant embedded in the Freelance Invoicing page of Akkountant, a personal finance app.
You manage invoices, customers, and earnings analytics for a freelance consultant.

## Available MCP Tools

### Invoice Management
- `create_invoice(data)` — Create a new invoice.
  Required fields in `data`: invoiceNumber (string), projectName (string), issueDate (YYYY-MM-DD), dueDate (YYYY-MM-DD), currency ("USD"|"INR"|"GBP"), from (object), to (object), items (array).
  `from` object: {name, email, address, phone} — name and address required.
  `to` object: {name, email, company, address} — name and address required.
  `items` array: [{description, quantity, rate, amount}] — at least one item required.
  Optional: status ("draft"|"sent"|"paid"|"overdue"), subtotal, total, tax ({rate, amount}), notes, terms, payment, customFields.

- `get_invoices(page, limit, status, sort_by, sort_dir, search)` — List invoices with pagination and filters.
  `status`: "draft", "sent", "paid", "overdue", or omit for all.
  `sort_by`: "created_at", "issue_date", "due_date", "total", "invoice_number". Default: "created_at".
  `sort_dir`: "asc" or "desc". Default: "desc".
  `search`: search by invoice number or project name.

- `get_invoice_by_number(invoice_number)` — Fetch a specific invoice by its invoice number.

- `update_invoice(invoice_number, data)` — Update any fields on an existing invoice. `data` can contain any subset of the fields from create_invoice. To mark as paid, include: status: "paid", payment: {paymentMethod, amountReceived, paymentDate}.

- `delete_invoice(invoice_number)` — Delete an invoice. DESTRUCTIVE — requires user confirmation.

### Customer Management
- `create_customer(data)` — Create a new customer. Required in `data`: name (string), email (string), address (string). Optional: company, phone.

- `get_customers(page, limit)` — List customers with pagination.

- `update_customer(customer_id, data)` — Update customer fields. `customer_id` is the UUID, not the name.

- `delete_customer(customer_id)` — Delete a customer. DESTRUCTIVE — requires user confirmation.

### Analytics
- `get_dashboard_analytics()` — Overall earnings summary: total revenue, paid/unpaid amounts, invoice counts by status, monthly trends, top clients.

- `get_earnings_by_date_range(start_date, end_date)` — Earnings within a date range (YYYY-MM-DD format).

## Rules
- Always fetch data via tools before answering. Never fabricate invoice numbers, amounts, or customer data.
- Invoice statuses: draft, sent, paid, overdue, cancelled.
- Currency options: USD, INR, GBP. Format with appropriate symbols ($, ₹, £).
- When creating invoices, calculate subtotal and total from items if not provided.
- Payment methods: bank_transfer, upi, cash, check, paypal, credit_card, other.
- When the user asks about earnings/revenue, use get_dashboard_analytics or get_earnings_by_date_range.
- When updating an invoice to "paid", always include payment details (paymentMethod, amountReceived).
- Customer IDs are UUIDs (36-char strings like "550e8400-e29b-41d4-a716-446655440000"), not integers — always fetch customers first via get_customers to get the correct ID before update_customer / delete_customer / create_invoice with customerId.
- NEVER end your turn silently. If a tool returns an error, returns no results, has a validation failure, or you cannot proceed for any reason, ALWAYS respond with a short explanation of what you tried and why it didn't work. An empty response is always a bug.
- Be concise but thorough with financial data.
- Format all amounts with appropriate currency symbols and 2 decimal places."""

# ─── Tool Definitions ─────────────────────────────────────────────────────────

INVESTMENT_TOOLS = [
    {
        "name": "fetch_portfolio_summary",
        "description": "Fetch portfolio summary for a given security type. Returns total invested value, current value, change percentage, change amount, security count, and market status.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Stocks", "Mutual_Funds", "NPS"],
                    "description": "The type of security to fetch summary for"
                }
            },
            "required": ["service_type"]
        }
    },
    {
        "name": "fetch_user_securities",
        "description": "Fetch all active securities held by the user for a given type. Returns each security with current price, change, and holding details.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Stocks", "Mutual_Funds", "NPS"],
                    "description": "The type of securities to fetch"
                }
            },
            "required": ["service_type"]
        }
    },
    {
        "name": "fetch_security_transactions",
        "description": "Fetch all buy/sell transactions for a given security type.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Stocks", "Mutual_Funds", "NPS"],
                    "description": "The type of security transactions to fetch"
                }
            },
            "required": ["service_type"]
        }
    },
    {
        "name": "search_securities",
        "description": "Search available securities by name/keyword. Returns up to 20 matching results. Always provide a query to filter results.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Stocks", "Mutual_Funds", "NPS"],
                    "description": "The type of securities to search"
                },
                "query": {
                    "type": "string",
                    "description": "Search keyword to filter securities by name (e.g. 'PPFAS', 'HDFC Top 100', 'Axis Bluechip'). Required."
                }
            },
            "required": ["service_type", "query"]
        }
    },
    {
        "name": "fetch_security_rate",
        "description": "Fetch the current rate/price for a specific security by its scheme code.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Stocks", "Mutual_Funds", "NPS"],
                    "description": "The type of security"
                },
                "scheme_code": {
                    "type": "string",
                    "description": "The scheme/security code to look up"
                }
            },
            "required": ["service_type", "scheme_code"]
        }
    },
    {
        "name": "insert_investment",
        "description": "Insert a new investment purchase record. For MF/NPS: requires schemeCode, date, quantity, amount. For EPF: requires date, description, employee_amount, employer_amount. For PF: requires date, description, amount. For Gold: requires date, description, amount, quantity (grams), goldType (18/22/24).",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Mutual_Funds", "NPS", "EPF", "PF", "Gold"],
                    "description": "The type of investment to insert"
                },
                "data": {
                    "type": "object",
                    "description": "Investment data. For MF/NPS: {schemeCode, date (dd-mm-YYYY), quantity, amount}. For EPF: {date (dd-mm-YYYY), description, employee_amount, employer_amount}. For PF: {date (dd-mm-YYYY), description, amount}. For Gold: {date (dd-mm-YYYY), description, amount, quantity (grams), goldType (18/22/24)}.",
                    "properties": {
                        "schemeCode": {"type": "string", "description": "Scheme code for MF or NPS"},
                        "date": {"type": "string", "description": "Date in dd-mm-YYYY format"},
                        "quantity": {"type": "number", "description": "Units purchased (MF/NPS) or grams (Gold)"},
                        "amount": {"type": "number", "description": "Total amount in ₹"},
                        "description": {"type": "string", "description": "Description for EPF/PF/Gold deposits"},
                        "goldType": {"type": "string", "enum": ["18", "22", "24"], "description": "Gold purity (18/22/24 carat). Required for Gold."},
                        "employee_amount": {"type": "number", "description": "Employee EPF contribution in ₹"},
                        "employer_amount": {"type": "number", "description": "Employer EPF contribution in ₹"}
                    },
                    "required": ["date", "amount"]
                }
            },
            "required": ["service_type", "data"]
        }
    },
    {
        "name": "delete_single_investment",
        "description": "Delete a single investment record by its buy ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Stocks", "Mutual_Funds", "NPS", "EPF", "PF", "Gold"],
                    "description": "The type of security"
                },
                "buy_id": {
                    "type": "string",
                    "description": "The buy ID of the investment record to delete"
                }
            },
            "required": ["service_type", "buy_id"]
        }
    },
    {
        "name": "delete_all_investments",
        "description": "Delete ALL investment records of a given type for the user. This is destructive and cannot be undone.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Stocks", "Mutual_Funds", "NPS", "EPF", "PF", "Gold"],
                    "description": "The type of securities to delete all records for"
                }
            },
            "required": ["service_type"]
        }
    },
    {
        "name": "fetch_epg_data",
        "description": "Fetch complete EPG (EPF/PPF/Gold) data including deposits and current value.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["EPF", "PF", "Gold"],
                    "description": "The EPG type to fetch data for"
                }
            },
            "required": ["service_type"]
        }
    },
    {
        "name": "fetch_kite_holdings",
        "description": "Fetch stock holdings from the user's linked Kite Connect (Zerodha) account.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "fetch_kite_positions",
        "description": "Fetch current day's trading positions from Kite Connect (Zerodha).",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "sync_kite_holdings",
        "description": "Sync holdings from Kite Connect into the local portfolio database.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "fetch_epg_rates",
        "description": "Fetch current rates for EPF (interest rates), PPF (interest rates), or Gold (spot prices by purity). Gold includes 24/22/18 Carat prices and IBJA data with silver rates.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["EPF", "PF", "Gold"],
                    "description": "EPF for EPF interest rates, PF for PPF interest rates, Gold for gold/silver spot prices"
                }
            },
            "required": ["service_type"]
        }
    },
    {
        "name": "fetch_investment_history",
        "description": "Fetch historical investment records/snapshots for Stocks, Mutual Funds, or NPS.",
        "input_schema": {
            "type": "object",
            "properties": {
                "service_type": {
                    "type": "string",
                    "enum": ["Stocks", "Mutual_Funds", "NPS"],
                    "description": "The type of investment history to fetch"
                }
            },
            "required": ["service_type"]
        }
    },
    {
        "name": "get_jobs_status",
        "description": "View status of background rate-fetching jobs. Shows pending, completed, and failed jobs. Available job types: SetNPSRate, SetNPSDetails, SetStocksDetails, SetMFRate, SetMFDetails, SetGoldRate, SetPPFRate, CheckMail, CheckStatement.",
        "input_schema": {
            "type": "object",
            "properties": {
                "page": {"type": "integer", "default": 1},
                "filters": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string", "description": "Filter by job title"},
                        "status": {"type": "string", "description": "Filter by status (Pending, Completed, Failed)"},
                        "priority": {"type": "string"}
                    }
                },
                "sort_by": {"type": "string", "default": "due_date", "enum": ["id", "title", "status", "priority", "due_date", "failures"]},
                "sort_order": {"type": "string", "default": "desc", "enum": ["asc", "desc"]}
            },
            "required": []
        }
    },
    {
        "name": "trigger_rate_refresh",
        "description": "Queue a background job to refresh market rates. Jobs: SetNPSRate (NPS NAVs), SetMFRate (MF NAVs), SetGoldRate (gold prices), SetPPFRate (PPF interest rates), SetStocksDetails (stock instrument data), SetNPSDetails (NPS scheme metadata), SetMFDetails (MF scheme list).",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string",
                    "enum": ["SetNPSRate", "SetNPSDetails", "SetStocksDetails", "SetMFRate", "SetMFDetails", "SetGoldRate", "SetPPFRate", "SetEPFRate"],
                    "description": "The rate-refresh job to trigger"
                }
            },
            "required": ["job_id"]
        }
    },
    {
        "name": "get_rate_freshness",
        "description": "Check when market rate data was last updated. Returns timestamps for all cached rate files: NPSRate, GoldRate, MFRate, EPFRate, PPFRate, MFDetails, NPSDetails, StockDetails.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]

TRANSACTION_TOOLS = [
    {
        "name": "fetch_transactions",
        "description": "Fetch transactions with optional filtering, sorting, and pagination. Returns transaction list with total count, credit sum, and debit sum.",
        "input_schema": {
            "type": "object",
            "properties": {
                "page": {
                    "type": "integer",
                    "description": "Page number (1-indexed)",
                    "default": 1
                },
                "page_size": {
                    "type": "integer",
                    "description": "Number of results per page",
                    "default": 50
                },
                "filters": {
                    "type": "object",
                    "description": "Optional filters",
                    "properties": {
                        "dateRange": {
                            "type": "object",
                            "properties": {
                                "dateFrom": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                                "dateTo": {"type": "string", "description": "End date (YYYY-MM-DD)"}
                            }
                        },
                        "details": {"type": "string", "description": "Search text in transaction details"},
                        "tags": {"type": "string", "description": "Filter by tag"},
                        "bank": {"type": "string", "description": "Filter by bank name"},
                        "source": {"type": "string", "description": "Filter by source (Email or Statement)"},
                        "processed_via": {"type": "string", "enum": ["ALL", "CLAUDE_CODE", "PATTERN_MATCH"], "description": "Filter by processing method"}
                    }
                }
            },
            "required": []
        }
    },
    {
        "name": "fetch_calendar_transactions",
        "description": "Fetch transaction and statement dates within a date range. Useful for calendar view.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                "date_to": {"type": "string", "description": "End date (YYYY-MM-DD)"}
            },
            "required": ["date_from", "date_to"]
        }
    },
    {
        "name": "update_transaction",
        "description": "Update a transaction's details, tag, or amount by its reference ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "reference_id": {
                    "type": "string",
                    "description": "The referenceID of the transaction to update (String(64) — bank reference string, not an integer)."
                },
                "updates": {
                    "type": "object",
                    "description": "Fields to update",
                    "properties": {
                        "details": {"type": "string"},
                        "tag": {"type": "string"},
                        "amount": {"type": "number"}
                    }
                }
            },
            "required": ["reference_id", "updates"]
        }
    },
    {
        "name": "fetch_opted_banks",
        "description": "Fetch the list of banks the user has opted to track.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "fetch_file_details",
        "description": "Fetch details of uploaded statement files with optional filters.",
        "input_schema": {
            "type": "object",
            "properties": {
                "page": {"type": "integer", "default": 1},
                "filters": {
                    "type": "object",
                    "properties": {
                        "dateRange": {
                            "type": "object",
                            "properties": {
                                "dateFrom": {"type": "string"},
                                "dateTo": {"type": "string"}
                            }
                        },
                        "fileName": {"type": "string"},
                        "bank": {"type": "string"}
                    }
                }
            },
            "required": []
        }
    },
    {
        "name": "scan_emails_for_transactions",
        "description": "Trigger email scanning to extract transaction alerts from Gmail. This can take several minutes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date for email scan (YYYY/MM/DD format)"},
                "date_to": {"type": "string", "description": "End date for email scan (YYYY/MM/DD format)"},
                "algorithm": {"type": "string", "enum": ["claude", "regex"], "default": "claude"}
            },
            "required": []
        }
    },
    {
        "name": "scan_statements",
        "description": "Trigger statement scanning from Gmail attachments. Downloads and processes PDF bank statements. This can take several minutes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "Start date (YYYY/MM/DD format)"},
                "date_to": {"type": "string", "description": "End date (YYYY/MM/DD format)"},
                "bank": {"type": "string", "description": "Comma-separated bank names to filter"},
                "algorithm": {"type": "string", "enum": ["claude", "regex"], "default": "claude"}
            },
            "required": []
        }
    },
    {
        "name": "delete_file",
        "description": "Delete a statement file and all its associated transactions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_id": {"type": "string", "description": "The file ID to delete"}
            },
            "required": ["file_id"]
        }
    },
    {
        "name": "process_mail_pipeline",
        "description": "Scan Gmail for all financial emails (transactions, statements, investments, EPF, gold, freelance) and process them automatically using AI. This replaces the old scan_emails_for_transactions and scan_statements tools with a unified pipeline.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {
                    "type": "string",
                    "description": "Start date for email scan (YYYY/M/D format)"
                },
                "date_to": {
                    "type": "string",
                    "description": "End date for email scan (YYYY/M/D format)"
                },
            },
            "required": []
        }
    },
]

FREELANCE_TOOLS = [
    {
        "name": "create_invoice",
        "description": "Create a new invoice. Requires invoice number, project name, dates, from/to details, and line items.",
        "input_schema": {
            "type": "object",
            "properties": {
                "data": {
                    "type": "object",
                    "description": "Invoice data",
                    "properties": {
                        "invoiceNumber": {"type": "string"},
                        "projectName": {"type": "string"},
                        "issueDate": {"type": "string", "description": "YYYY-MM-DD"},
                        "dueDate": {"type": "string", "description": "YYYY-MM-DD"},
                        "customerId": {"type": "string", "description": "Customer UUID (CHAR(36)). Fetch via get_customers first if you only have a name."},
                        "from": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "email": {"type": "string"},
                                "address": {"type": "string"},
                                "phone": {"type": "string"}
                            },
                            "required": ["name", "address"]
                        },
                        "to": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "email": {"type": "string"},
                                "address": {"type": "string"},
                                "company": {"type": "string"}
                            },
                            "required": ["name", "address"]
                        },
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "description": {"type": "string"},
                                    "quantity": {"type": "number"},
                                    "rate": {"type": "number"},
                                    "amount": {"type": "number"}
                                }
                            }
                        },
                        "subtotal": {"type": "number"},
                        "tax": {
                            "type": "object",
                            "properties": {
                                "rate": {"type": "number"},
                                "amount": {"type": "number"}
                            }
                        },
                        "total": {"type": "number"},
                        "currency": {"type": "string", "enum": ["USD", "INR", "GBP"]},
                        "notes": {"type": "string"},
                        "terms": {"type": "string"},
                        "status": {"type": "string", "enum": ["draft", "sent", "paid", "overdue", "cancelled"]}
                    },
                    "required": ["invoiceNumber", "projectName", "issueDate", "dueDate", "from", "to", "items"]
                }
            },
            "required": ["data"]
        }
    },
    {
        "name": "get_invoices",
        "description": "Get a paginated list of invoices with optional filtering and sorting.",
        "input_schema": {
            "type": "object",
            "properties": {
                "page": {"type": "integer", "default": 1},
                "limit": {"type": "integer", "default": 20},
                "status": {"type": "string", "enum": ["draft", "sent", "paid", "overdue", "cancelled"]},
                "sort_by": {"type": "string", "default": "created_at"},
                "sort_dir": {"type": "string", "enum": ["asc", "desc"], "default": "desc"},
                "search": {"type": "string", "description": "Search across invoice number, project name, client name"}
            },
            "required": []
        }
    },
    {
        "name": "get_invoice_by_number",
        "description": "Get a specific invoice by its invoice number.",
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_number": {"type": "string"}
            },
            "required": ["invoice_number"]
        }
    },
    {
        "name": "update_invoice",
        "description": "Update an existing invoice by its invoice number.",
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_number": {
                    "type": "string",
                    "description": "The invoice number to update"
                },
                "data": {
                    "type": "object",
                    "description": "Fields to update (same structure as create_invoice data)"
                }
            },
            "required": ["invoice_number", "data"]
        }
    },
    {
        "name": "delete_invoice",
        "description": "Delete an invoice by its invoice number. This cannot be undone.",
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_number": {"type": "string"}
            },
            "required": ["invoice_number"]
        }
    },
    {
        "name": "create_customer",
        "description": "Create a new customer record.",
        "input_schema": {
            "type": "object",
            "properties": {
                "data": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"},
                        "company": {"type": "string"},
                        "address": {"type": "string"},
                        "phone": {"type": "string"}
                    },
                    "required": ["name", "email", "address"]
                }
            },
            "required": ["data"]
        }
    },
    {
        "name": "get_customers",
        "description": "Get a paginated list of customers.",
        "input_schema": {
            "type": "object",
            "properties": {
                "page": {"type": "integer", "default": 1},
                "limit": {"type": "integer", "default": 20}
            },
            "required": []
        }
    },
    {
        "name": "update_customer",
        "description": "Update an existing customer by ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "string", "description": "Customer UUID (CHAR(36)). Fetch via get_customers first if you only have a name."},
                "data": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"},
                        "company": {"type": "string"},
                        "address": {"type": "string"},
                        "phone": {"type": "string"}
                    }
                }
            },
            "required": ["customer_id", "data"]
        }
    },
    {
        "name": "delete_customer",
        "description": "Delete a customer by ID. This cannot be undone.",
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "string", "description": "Customer UUID (CHAR(36)). Fetch via get_customers first if you only have a name."}
            },
            "required": ["customer_id"]
        }
    },
    {
        "name": "get_dashboard_analytics",
        "description": "Get freelance dashboard analytics including total earnings, monthly earnings, completed projects, active clients, earnings by month/client, and recent invoices.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_earnings_by_date_range",
        "description": "Get total earnings and invoice details for a specific date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"}
            },
            "required": ["start_date", "end_date"]
        }
    }
]

# ─── Lookup helpers ────────────────────────────────────────────────────────────

AGENT_CONFIGS = {
    "investment": {
        "system_prompt": INVESTMENT_SYSTEM_PROMPT,
        "tools": INVESTMENT_TOOLS,
    },
    "transaction": {
        "system_prompt": TRANSACTION_SYSTEM_PROMPT,
        "tools": TRANSACTION_TOOLS,
    },
    "freelance": {
        "system_prompt": FREELANCE_SYSTEM_PROMPT,
        "tools": FREELANCE_TOOLS,
    },
}


def get_agent_config(agent_type: str):
    """Get system prompt and tools for a given agent type."""
    config = AGENT_CONFIGS.get(agent_type)
    if not config:
        raise ValueError(f"Unknown agent type: {agent_type}. Must be one of: {list(AGENT_CONFIGS.keys())}")
    return config
