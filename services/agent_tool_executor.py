"""
Tool executor that dispatches agent tool calls to existing service methods.
No new business logic — just routing + unwrapping Flask responses.
"""

import json
from flask import g, Response
from enums.MsnEnum import MSNENUM
from enums.EPGEnum import EPGEnum
from utils.logger import Logger

logger = Logger(__name__).get_logger()


# ak-e7g: sentinel key on the executor's return dict that the SDK-tool
# handler (services/agentService.py::_build_sdk_tools) lifts into
# is_error=True so the LLM sees the validation failure and can retry
# with the correct shape. Keep this key name in sync with the check
# there — the two are a private contract between the layers.
TOOL_ERROR_KEY = "_tool_error"


class ToolValidationError(Exception):
    """Raised when an agent tool's input fails per-tool validation
    before the underlying service is called. ak-e7g moved per-
    service_type shape enforcement here after the top-level allOf
    on insert_investment's input_schema was rejected by the Anthropic
    API."""


# Per-service_type required-field shape for insert_investment. Kept as
# a module-level table (rather than inline in the executor branch) so
# the test suite can import + drive it without spinning up the whole
# execute_tool stack. Order matches the tool description in
# services/agent_tools.py:insert_investment for grep-ability.
INSERT_INVESTMENT_REQUIRED_FIELDS = {
    "Mutual_Funds": ("schemeCode", "date", "quantity", "amount"),
    "NPS":          ("schemeCode", "date", "quantity", "amount"),
    "EPF":          ("date", "description", "employee_amount", "employer_amount"),
    "PF":           ("date", "description", "amount"),
    "Gold":         ("date", "description", "amount", "quantity", "goldType"),
}


def _missing_or_empty(data, fields):
    """Return the subset of `fields` that are missing from `data` or
    present but set to a value we treat as unfilled (None / empty string).
    Zero and False are treated as PRESENT — only missing keys / None /
    empty-string count as omissions."""
    if not isinstance(data, dict):
        return list(fields)
    missing = []
    for f in fields:
        if f not in data:
            missing.append(f)
            continue
        v = data[f]
        if v is None or (isinstance(v, str) and v.strip() == ""):
            missing.append(f)
    return missing


def _validate_insert_investment(tool_input):
    """Per-service_type required-field enforcement for insert_investment.

    ak-e7g (P0 hotfix): the JSON Schema top-level allOf that used to
    enforce this shape was rejected by the Anthropic API, breaking the
    Investment Assistant chat entirely. Moving the check here keeps
    ak-bgc's protection intent: reject an EPF call missing the two-half
    split BEFORE it hits EPFService (which still has a silent 50/50
    split fallback for bare `amount` that we don't want to depend on).

    Raises ToolValidationError with a descriptive message on failure.
    Returns silently on success. Never returns a value — callers should
    proceed to the service dispatch on a clean return.
    """
    if not isinstance(tool_input, dict):
        raise ToolValidationError(
            "insert_investment: tool_input must be an object"
        )
    stype = tool_input.get("service_type")
    if stype not in INSERT_INVESTMENT_REQUIRED_FIELDS:
        raise ToolValidationError(
            "insert_investment: 'service_type' must be one of "
            f"{sorted(INSERT_INVESTMENT_REQUIRED_FIELDS)} "
            f"(got {stype!r})"
        )
    data = tool_input.get("data")
    if not isinstance(data, dict):
        raise ToolValidationError(
            "insert_investment: 'data' must be an object with the "
            f"required fields for service_type='{stype}' "
            f"({list(INSERT_INVESTMENT_REQUIRED_FIELDS[stype])})"
        )
    required = INSERT_INVESTMENT_REQUIRED_FIELDS[stype]
    missing = _missing_or_empty(data, required)
    if missing:
        hint = ""
        # ak-bgc guard: EPF with bare `amount` (and no halves) was the
        # original silent 50/50-split bug. Call it out explicitly so
        # the LLM doesn't just retry by shuffling `amount` around.
        if stype == "EPF" and (
            "employee_amount" in missing or "employer_amount" in missing
        ) and "amount" in data:
            hint = (
                " Note: EPF does NOT accept a single 'amount' field — "
                "provide the two-half split as 'employee_amount' + "
                "'employer_amount' (the total = employee + employer)."
            )
        raise ToolValidationError(
            f"insert_investment for service_type='{stype}' requires "
            f"{list(required)}. Missing/empty: {missing}.{hint}"
        )


def _resolve_service_type(raw_type: str):
    """Convert a string service type to the appropriate enum."""
    if raw_type in MSNENUM.__members__:
        return MSNENUM[raw_type]
    if raw_type in EPGEnum.__members__:
        return EPGEnum[raw_type]
    raise ValueError(f"Unknown service type: {raw_type}")


def _unwrap_response(result):
    """Unwrap Flask jsonify responses into plain dicts for Claude."""
    if result is None:
        return {"result": "success"}

    # Handle (response, status_code) tuples from Flask controllers
    if isinstance(result, tuple):
        resp, status = result[0], result[1] if len(result) > 1 else 200
        return _unwrap_response(resp)

    # Handle Flask Response objects (from jsonify)
    if isinstance(result, Response):
        try:
            return json.loads(result.get_data(as_text=True))
        except (json.JSONDecodeError, Exception):
            return {"result": result.get_data(as_text=True)}

    # Handle SQLAlchemy model objects
    if hasattr(result, '__dict__') and hasattr(result, '__tablename__'):
        return {k: v for k, v in result.__dict__.items() if k != '_sa_instance_state'}

    # Handle lists of SQLAlchemy objects
    if isinstance(result, list):
        processed = []
        for item in result:
            if hasattr(item, '__dict__') and hasattr(item, '__tablename__'):
                processed.append({k: v for k, v in item.__dict__.items() if k != '_sa_instance_state'})
            else:
                processed.append(item)
        return {"results": processed}

    # Handle dicts and primitives
    if isinstance(result, (dict, str, int, float, bool)):
        return result

    # Fallback
    return {"result": str(result)}


def _make_serializable(obj):
    """Ensure the result is JSON-serializable."""
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_serializable(item) for item in obj]
    if hasattr(obj, 'value'):  # Enum
        return obj.value
    if hasattr(obj, 'isoformat'):  # datetime/date
        return obj.isoformat()
    if hasattr(obj, '__float__'):  # Decimal
        return float(obj)
    return obj


def execute_tool(agent_type, tool_name, tool_input, user_id,
                 investment_service=None, transaction_service=None,
                 invoice_service=None, customer_service=None,
                 dashboard_service=None, mail_processor=None,
                 file_attachment_service=None, conversation_id=None):
    """
    Execute a tool call by routing to the appropriate existing service method.
    Returns a JSON-serializable dict with the tool result.

    ak-9dz: `attach_file_to_chat` is agent-type-agnostic (registered in
    both INVESTMENT_TOOLS + FREELANCE_TOOLS). It's handled BEFORE the
    per-agent switch so both agents share the same executor path.
    """
    # Ensure flask g context has firebase_id for services that read it
    g.firebase_id = user_id

    try:
        # ak-9dz: shared MCP tool for agent-produced file downloads.
        # Dispatched before the per-agent switch so any agent whose
        # tool list contains this def routes through the same handler.
        if tool_name == "attach_file_to_chat":
            result = _execute_attach_file_to_chat(
                tool_input, user_id,
                file_attachment_service=file_attachment_service,
                conversation_id=conversation_id,
            )
        elif agent_type == "investment":
            result = _execute_investment_tool(tool_name, tool_input, user_id, investment_service)
        elif agent_type == "transaction":
            result = _execute_transaction_tool(tool_name, tool_input, user_id, transaction_service, mail_processor)
        elif agent_type == "freelance":
            result = _execute_freelance_tool(tool_name, tool_input, user_id,
                                             invoice_service, customer_service, dashboard_service)
        else:
            return {"error": f"Unknown agent type: {agent_type}"}

        unwrapped = _unwrap_response(result)
        return _make_serializable(unwrapped)

    except ToolValidationError as e:
        # ak-e7g: surface handler-layer validation failures as tool_errors
        # (is_error=True in the SDK response) so the LLM sees the error
        # text and can retry with the correct shape. Distinguished from
        # generic Exception below so we don't spam logs at ERROR level
        # for what's really an expected-shape-mismatch retry path.
        logger.info(f"Tool validation error [{agent_type}/{tool_name}]: {e}")
        return {TOOL_ERROR_KEY: True, "message": str(e)}
    except Exception as e:
        logger.error(f"Tool execution error [{agent_type}/{tool_name}]: {e}")
        return {"error": str(e)}


def _execute_investment_tool(tool_name, tool_input, user_id, service):
    if tool_name == "fetch_portfolio_summary":
        return service.fetchSummary(tool_input["service_type"], user_id)

    elif tool_name == "fetch_user_securities":
        return service.fetchUserSecurities(MSNENUM[tool_input["service_type"]].value, user_id)

    elif tool_name == "fetch_security_transactions":
        return service.fetchSecurityTransactions(MSNENUM[tool_input["service_type"]].value, user_id)

    elif tool_name == "search_securities":
        query = tool_input.get("query", "").strip().lower()
        all_securities = service.fetchAllSecurities(MSNENUM[tool_input["service_type"]])
        items = all_securities.get("data", []) if isinstance(all_securities, dict) else (all_securities if isinstance(all_securities, list) else [])

        def _matches(item, q):
            searchable = " ".join([
                str(item.get("schemeName", "")),
                str(item.get("name", "")),
                str(item.get("stockCode", "")),
                str(item.get("pfm_name", "")),
            ]).lower()
            return all(word in searchable for word in q.split())

        if query:
            filtered = [item for item in items if _matches(item, query)]
            return {"data": filtered[:20], "total_matches": len(filtered), "query": query}
        # No query: return truncated to prevent buffer overflow
        return {"data": items[:20], "total": len(items), "truncated": True}

    elif tool_name == "fetch_security_rate":
        stype = MSNENUM[tool_input["service_type"]]
        return service.fetchSecuritySchemeRate(stype.value, tool_input["scheme_code"])

    elif tool_name == "insert_investment":
        # ak-e7g: validate per-service_type shape BEFORE dispatch.
        # Raises ToolValidationError which is caught in execute_tool and
        # surfaced as is_error=True to the LLM.
        _validate_insert_investment(tool_input)
        stype = _resolve_service_type(tool_input["service_type"])
        return service.insertSecurityPurchase(stype, user_id, tool_input["data"])

    elif tool_name == "delete_single_investment":
        stype = _resolve_service_type(tool_input["service_type"])
        return service.deleteSingleRecord(stype, tool_input["buy_id"])

    elif tool_name == "delete_all_investments":
        stype = _resolve_service_type(tool_input["service_type"])
        return service.deleteAll(stype, user_id)

    elif tool_name == "fetch_epg_data":
        stype = EPGEnum[tool_input["service_type"]]
        return service.fetchActiveSecurities(stype, user_id)

    elif tool_name == "fetch_kite_holdings":
        return service.fetchKiteHoldings(user_id)

    elif tool_name == "fetch_kite_positions":
        return service.fetchKitePositions(user_id)

    elif tool_name == "sync_kite_holdings":
        return service.syncKiteHoldings(user_id)

    elif tool_name == "fetch_epg_rates":
        stype = EPGEnum[tool_input["service_type"]]
        return service.fetchRateForEPG(stype)

    elif tool_name == "fetch_investment_history":
        stype = MSNENUM[tool_input["service_type"]]
        return service.fetchHistory(stype, user_id)

    elif tool_name == "get_jobs_status":
        page = tool_input.get("page", 1)
        filters = tool_input.get("filters")
        sort_by = tool_input.get("sort_by", "due_date")
        sort_order = tool_input.get("sort_order", "desc")
        return service.getJobsTable(page, filters, sort_by, sort_order)

    elif tool_name == "trigger_rate_refresh":
        return service.setJobsTable(tool_input["job_id"], user_id)

    elif tool_name == "get_rate_freshness":
        return service.getFileTimeStamps()

    # ── ak-rde9 Bead A (2026-11-12): 8 previously-dead service methods ──

    elif tool_name == "fetch_realized_pnl":
        stype = MSNENUM[tool_input["service_type"]].value
        return service.fetchRealizedPnL(stype, user_id)

    elif tool_name == "fetch_fo_summary":
        return service.fetchFOSummary(user_id)

    elif tool_name == "fetch_fo_trades":
        return service.fetchFOTrades(user_id)

    elif tool_name == "fetch_investment_snapshots":
        return service.getInvestmentSnapshots(
            user_id,
            date_from=tool_input.get("date_from"),
            date_to=tool_input.get("date_to"),
            investment_type=tool_input.get("investment_type"),
        )

    elif tool_name == "fetch_investment_emails":
        return service.fetchInvestmentEmails(
            user_id,
            category=tool_input.get("category"),
            page=tool_input.get("page", 1),
            page_size=tool_input.get("page_size", 50),
            service_type=tool_input.get("service_type"),
        )

    elif tool_name == "fetch_email_body":
        return service.fetchEmailBody(user_id, tool_input["gmail_id"])

    elif tool_name == "get_kite_login_url":
        return {"login_url": service.getKiteLoginUrl()}

    elif tool_name == "get_kite_token_status":
        return service.getKiteTokenStatus(user_id)

    # ── ak-zai2 Kite Bundle 2 (2026-11-12): 2 previously-dead wrappers ──

    elif tool_name == "get_kite_trades":
        return service.fetchKiteTrades(user_id)

    elif tool_name == "get_kite_margins":
        return service.fetchKiteMargins(user_id)

    # ── ak-dx98 Kite Bundle 3 (2026-11-12): monitoring wrappers ──
    # Both read-only, no-param, account-level — same shape as the
    # Bundle 2 wrappers above. GTTs carry LTP-distance enrichment
    # on active triggers so the agent can answer "how close is my
    # stop-loss to firing" without a follow-up OHLC call.

    elif tool_name == "get_kite_gtts":
        return service.fetchKiteGtts(user_id)

    elif tool_name == "get_kite_alerts":
        return service.fetchKiteAlerts(user_id)

    # ── ak-6k2m Bead B (2026-11-12): web intelligence wrappers ──
    #
    # These wrappers return a structured SEARCH PLAN (canonical query +
    # suggested sources + extraction hints) rather than performing live
    # HTTP server-side. The agent then calls the SDK's WebSearch /
    # WebFetch built-ins on the plan. Rationale: (a) keeps queries
    # consistent across chats, (b) makes the agent's reasoning
    # auditable (the exact query lands in tool_events), (c) leaves
    # live-HTTP concerns (rate-limits, scraping drift, cert pinning)
    # on the SDK side where they already belong.
    #
    # Each wrapper trips the web-fetch-used flag at the handler-closure
    # layer (agentService._build_sdk_tools) — the executor itself
    # doesn't carry that state. See WEB_WRAPPER_TOOLS there.

    elif tool_name == "fetch_stock_news":
        return _execute_fetch_stock_news(tool_input)

    elif tool_name == "fetch_earnings_calendar":
        return _execute_fetch_earnings_calendar(user_id, service)

    elif tool_name == "fetch_company_announcements":
        return _execute_fetch_company_announcements(tool_input)

    elif tool_name == "fetch_macro_context":
        return _execute_fetch_macro_context()

    elif tool_name == "fetch_fund_factsheet":
        return _execute_fetch_fund_factsheet(tool_input)

    else:
        return {"error": f"Unknown investment tool: {tool_name}"}


# ── ak-6k2m Bead B (2026-11-12): web-wrapper implementations ─────────
#
# Each helper is a thin builder: validate args → build a canonical
# search plan → return it. No HTTP; the SDK does the live fetch via
# WebSearch / WebFetch on the returned plan. This makes every helper
# unit-testable without mocking the network.


# Cap on the days_back window — matches the input_schema at the tool
# def. Enforced here too so a non-SDK caller (test harness) can't
# sneak past the schema.
_FETCH_STOCK_NEWS_MAX_DAYS_BACK = 30
# Default recency window for fetch_stock_news when caller omits
# days_back. Matches the tool def.
_FETCH_STOCK_NEWS_DEFAULT_DAYS_BACK = 7


def _execute_fetch_stock_news(tool_input):
    """Build a canonical WebSearch plan for stock news.

    Returns ``{tool, symbol, days_back, canonical_query, date_hint,
    extraction, instructions}``. The agent then calls WebSearch on
    ``canonical_query``.

    Validation:
      * ``symbol`` required, non-empty string after strip.
      * ``days_back`` optional; defaults to 7; clamped to [1, 30].
    """
    if not isinstance(tool_input, dict):
        raise ToolValidationError(
            "fetch_stock_news: tool_input must be an object"
        )
    symbol = tool_input.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ToolValidationError(
            "fetch_stock_news: 'symbol' is required and must be a "
            "non-empty string"
        )
    symbol = symbol.strip()

    days_back_raw = tool_input.get(
        "days_back", _FETCH_STOCK_NEWS_DEFAULT_DAYS_BACK,
    )
    try:
        days_back = int(days_back_raw)
    except (TypeError, ValueError):
        raise ToolValidationError(
            f"fetch_stock_news: 'days_back' must be an integer "
            f"(got {days_back_raw!r})"
        )
    # Clamp rather than raise — the agent sometimes sends values that
    # make sense semantically (e.g., 90) but are outside the schema.
    # A clamp is friendlier than an is_error.
    if days_back < 1:
        days_back = 1
    elif days_back > _FETCH_STOCK_NEWS_MAX_DAYS_BACK:
        days_back = _FETCH_STOCK_NEWS_MAX_DAYS_BACK

    return {
        "tool": "fetch_stock_news",
        "symbol": symbol,
        "days_back": days_back,
        "canonical_query": (
            f"{symbol} stock news last {days_back} days"
        ),
        "date_hint": (
            f"filter to items published within the last "
            f"{days_back} days"
        ),
        "extraction": [
            "headline",
            "source (official exchange > Reuters / Bloomberg > "
            "Mint / ET / MC > other)",
            "publish date (verify within window)",
            "one-sentence summary",
        ],
        "instructions": (
            "Call WebSearch with 'canonical_query', then filter the "
            "results per 'date_hint' + 'extraction'. Be explicit "
            "about the SOURCE for every number you cite back to the "
            "user."
        ),
    }


def _execute_fetch_earnings_calendar(user_id, service):
    """Build per-symbol WebSearch plans for the user's held stocks.

    Reads ``service.fetchUserSecurities('Stocks', user_id)`` to scope
    the queries to actually-held tickers. Returns ``{tool, symbols,
    plans, instructions}`` — one plan per symbol, each with a
    canonical query the agent can feed to WebSearch.

    On a service fetch failure (service missing, DB down), returns a
    structured ``{tool, error_class, message, ...}`` shape rather
    than raising, so the LLM can fall back to free-form WebSearch.
    """
    try:
        holdings = service.fetchUserSecurities(
            MSNENUM["Stocks"].value, user_id,
        )
    except Exception as exc:
        logger.warning(
            f"fetch_earnings_calendar: holdings fetch failed: {exc}",
        )
        return {
            "tool": "fetch_earnings_calendar",
            "error_class": "holdings_fetch_failed",
            "message": str(exc),
            "fallback": (
                "Ask the user which symbols to look up, then call "
                "WebSearch per symbol with the query template "
                "'<SYMBOL> upcoming earnings date NSE BSE'."
            ),
        }

    unwrapped = _unwrap_response(holdings)
    items = []
    if isinstance(unwrapped, dict):
        items = (
            unwrapped.get("data")
            or unwrapped.get("results")
            or []
        )
    elif isinstance(unwrapped, list):
        items = unwrapped

    symbols = []
    for item in items:
        if not isinstance(item, dict):
            continue
        sym = item.get("stockCode") or item.get("symbol") or item.get("name")
        if isinstance(sym, str) and sym.strip():
            s = sym.strip()
            if s not in symbols:
                symbols.append(s)

    plans = [
        {
            "symbol": s,
            "canonical_query": f"{s} upcoming earnings date NSE BSE",
        }
        for s in symbols
    ]

    return {
        "tool": "fetch_earnings_calendar",
        "symbols": symbols,
        "plans": plans,
        "instructions": (
            "For each entry in 'plans', call WebSearch with the "
            "'canonical_query'. Extract the next earnings date + "
            "expected EPS / revenue if the source includes it. "
            "If 'symbols' is empty, the user holds no stocks — "
            "tell them so and skip the WebSearch loop."
        ),
    }


def _execute_fetch_company_announcements(tool_input):
    """Build a WebFetch plan for a symbol's BSE / NSE announcements.

    Returns canonical lookup URLs on both exchanges + extraction
    hints. The agent then calls WebFetch on each URL and the
    model picks the most-recent material disclosures.
    """
    if not isinstance(tool_input, dict):
        raise ToolValidationError(
            "fetch_company_announcements: tool_input must be an object"
        )
    symbol = tool_input.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ToolValidationError(
            "fetch_company_announcements: 'symbol' is required and "
            "must be a non-empty string"
        )
    symbol = symbol.strip().upper()

    return {
        "tool": "fetch_company_announcements",
        "symbol": symbol,
        # Canonical public pages — the agent WebFetches these; we
        # don't hard-code API paths which drift more often than the
        # consumer pages.
        "urls": [
            {
                "exchange": "BSE",
                "url": (
                    f"https://www.bseindia.com/corporates/ann.html"
                    f"?scrip={symbol}"
                ),
            },
            {
                "exchange": "NSE",
                "url": (
                    f"https://www.nseindia.com/companies-listing/"
                    f"corporate-filings-announcements?symbol={symbol}"
                ),
            },
        ],
        "extraction": [
            "disclosure date",
            "subject line / category (dividend, board-meeting, "
            "acquisition, insider-trading, …)",
            "one-sentence outcome / summary",
            "link to the full filing PDF if present",
        ],
        "instructions": (
            "Call WebFetch on BOTH URLs if both are reachable — "
            "coverage overlaps but isn't identical. Rank "
            "disclosures by date (most recent first) and surface "
            "material ones (dividends, acquisitions, "
            "board-meeting outcomes, fundraising). Cite the "
            "exchange + date for every item."
        ),
    }


def _execute_fetch_macro_context():
    """Build canonical WebSearch plans for Indian macro indicators
    the investment agent commonly needs.

    Returns three plans (repo rate, inflation print, FOMC calendar)
    with per-plan queries + suggested sources.
    """
    return {
        "tool": "fetch_macro_context",
        "plans": [
            {
                "indicator": "rbi_repo_rate",
                "canonical_query": (
                    "RBI repo rate current India latest MPC decision"
                ),
                "sources": [
                    "rbi.org.in (official)",
                    "livemint.com", "economictimes.indiatimes.com",
                ],
            },
            {
                "indicator": "india_cpi_inflation",
                "canonical_query": (
                    "India CPI inflation latest print MoSPI YoY"
                ),
                "sources": [
                    "mospi.gov.in (official)",
                    "livemint.com", "reuters.com",
                ],
            },
            {
                "indicator": "fomc_upcoming",
                "canonical_query": (
                    "FOMC next meeting date schedule Federal Reserve"
                ),
                "sources": [
                    "federalreserve.gov (official)",
                    "reuters.com",
                ],
            },
        ],
        "instructions": (
            "Call WebSearch per plan. Prefer the official source "
            "when present. For repo rate + CPI, cite the exact "
            "percentage AND the announcement / print date. For "
            "FOMC, cite the next meeting date."
        ),
    }


def _execute_fetch_fund_factsheet(tool_input):
    """Build a canonical search plan for a Mutual Fund scheme
    factsheet by AMFI schemeCode.

    Returns {queries, sources, extraction}; the agent then calls
    WebSearch to resolve the schemeCode → fund name → factsheet
    PDF, and WebFetch to pull the factsheet.
    """
    if not isinstance(tool_input, dict):
        raise ToolValidationError(
            "fetch_fund_factsheet: tool_input must be an object"
        )
    scheme_code = tool_input.get("scheme_code")
    if not isinstance(scheme_code, str) or not scheme_code.strip():
        raise ToolValidationError(
            "fetch_fund_factsheet: 'scheme_code' is required and "
            "must be a non-empty string (use "
            "search_securities('Mutual_Funds', <name>) to resolve "
            "a user-supplied name first)"
        )
    scheme_code = scheme_code.strip()

    return {
        "tool": "fetch_fund_factsheet",
        "scheme_code": scheme_code,
        "queries": [
            f"AMFI scheme code {scheme_code} factsheet latest",
            f"mutual fund scheme {scheme_code} expense ratio top "
            f"holdings monthly factsheet",
        ],
        "sources": [
            "amfiindia.com (official)",
            "valueresearchonline.com",
            "moneycontrol.com",
            "The AMC's own site (HDFC AMC / ICICI Prudential / "
            "SBI MF / etc.) — factsheet PDF is authoritative.",
        ],
        "extraction": [
            "scheme full name",
            "NAV + NAV date",
            "expense ratio (direct + regular if listed)",
            "top 10 holdings with % weight",
            "asset allocation (equity / debt / cash)",
            "benchmark index",
        ],
        "instructions": (
            "Call WebSearch with 'queries' to find the latest "
            "monthly factsheet (prefer the AMC's own PDF). Then "
            "WebFetch the PDF URL. Extract per 'extraction'. "
            "If the schemeCode doesn't resolve, ask the user to "
            "name the fund so you can search_securities first."
        ),
    }


def _execute_transaction_tool(tool_name, tool_input, user_id, service, mail_processor=None):
    if tool_name == "fetch_transactions":
        page = tool_input.get("page", 1)
        page_size = tool_input.get("page_size", 50)
        filters = tool_input.get("filters", {})
        result = service.fetchTransactions(page=page, filters=filters, page_size=page_size)
        # Format SQLAlchemy transaction objects
        formatted_results = []
        for t in result.get("results", []):
            if hasattr(t, '__dict__'):
                row = {}
                for key, value in t.__dict__.items():
                    if key != '_sa_instance_state':
                        if hasattr(value, 'value'):
                            row[key] = value.value
                        elif hasattr(value, 'isoformat'):
                            row[key] = value.isoformat()
                        else:
                            row[key] = value
                formatted_results.append(row)
            else:
                formatted_results.append(t)
        return {
            "count": result.get("count", 0),
            "credit_sum": result.get("credit_sum", 0),
            "debit_sum": result.get("debit_sum", 0),
            "page": result.get("page", page),
            "results": formatted_results
        }

    elif tool_name == "fetch_calendar_transactions":
        return service.fetchTransactionDates(tool_input["date_from"], tool_input["date_to"])

    elif tool_name == "update_transaction":
        return service.updateTransaction(tool_input["reference_id"], tool_input["updates"])

    elif tool_name == "fetch_opted_banks":
        return service.fetchBanksOptedByUser(user_id)

    elif tool_name == "fetch_file_details":
        page = tool_input.get("page", 1)
        filters = tool_input.get("filters", {})
        result = service.fetchFileDetails(page=page, filters=filters)
        formatted_results = []
        for fd in result.get("results", []):
            if hasattr(fd, '__dict__'):
                row = {k: v for k, v in fd.__dict__.items() if k != '_sa_instance_state'}
                formatted_results.append(row)
            else:
                formatted_results.append(fd)
        return {
            "count": result.get("count", 0),
            "page": result.get("page", page),
            "results": formatted_results
        }

    elif tool_name == "scan_emails_for_transactions":
        # Redirected to unified mail pipeline
        if mail_processor:
            date_from = tool_input.get("date_from")
            date_to = tool_input.get("date_to")
            return mail_processor.process_emails(user_id, date_from, date_to)
        return {"error": "Mail processor not configured. Use process_mail_pipeline instead."}

    elif tool_name == "scan_statements":
        # Redirected to unified mail pipeline
        if mail_processor:
            date_from = tool_input.get("date_from")
            date_to = tool_input.get("date_to")
            return mail_processor.process_emails(user_id, date_from, date_to)
        return {"error": "Mail processor not configured. Use process_mail_pipeline instead."}

    elif tool_name == "delete_file":
        return service.deleteFile(user_id, tool_input["file_id"])

    elif tool_name == "process_mail_pipeline":
        if not mail_processor:
            return {"error": "Mail processor service not configured"}
        date_from = tool_input.get("date_from")
        date_to = tool_input.get("date_to")
        return mail_processor.process_emails(user_id, date_from, date_to)

    else:
        return {"error": f"Unknown transaction tool: {tool_name}"}


_FREELANCE_REQUIRED_PARAMS = {
    "create_invoice": ["data"],
    "get_invoice_by_number": ["invoice_number"],
    "update_invoice": ["invoice_number", "data"],
    "delete_invoice": ["invoice_number"],
    "create_customer": ["data"],
    "update_customer": ["customer_id", "data"],
    "delete_customer": ["customer_id"],
    "get_earnings_by_date_range": ["start_date", "end_date"],
}


def _execute_freelance_tool(tool_name, tool_input, user_id,
                            invoice_service, customer_service, dashboard_service):
    # Validate services
    if invoice_service is None and tool_name in ("create_invoice", "get_invoices", "get_invoice_by_number", "update_invoice", "delete_invoice"):
        return {"error": "Invoice service not available"}
    if customer_service is None and tool_name in ("create_customer", "get_customers", "update_customer", "delete_customer"):
        return {"error": "Customer service not available"}
    if dashboard_service is None and tool_name in ("get_dashboard_analytics", "get_earnings_by_date_range"):
        return {"error": "Dashboard service not available"}

    # Validate required parameters
    required = _FREELANCE_REQUIRED_PARAMS.get(tool_name, [])
    missing = [k for k in required if k not in tool_input]
    if missing:
        return {"error": f"{tool_name} requires: {', '.join(missing)}"}

    if tool_name == "create_invoice":
        data = tool_input["data"]
        # Validate nested required fields
        for section in ("from", "to"):
            obj = data.get(section, {})
            if not obj.get("name"):
                return {"error": f"'{section}.name' is required for create_invoice"}
            if not obj.get("address"):
                return {"error": f"'{section}.address' is required for create_invoice"}
        if not data.get("items") or len(data["items"]) == 0:
            return {"error": "At least one item is required for create_invoice"}
        return invoice_service.create_invoice(data)

    elif tool_name == "get_invoices":
        return invoice_service.get_invoices(
            page=tool_input.get("page", 1),
            limit=tool_input.get("limit", 20),
            status=tool_input.get("status"),
            sort_by=tool_input.get("sort_by", "created_at"),
            sort_order=tool_input.get("sort_dir", "desc"),
            search=tool_input.get("search")
        )

    elif tool_name == "get_invoice_by_number":
        return invoice_service.get_invoice_by_number(tool_input["invoice_number"])

    elif tool_name == "update_invoice":
        return invoice_service.update_invoice(tool_input["invoice_number"], tool_input["data"])

    elif tool_name == "delete_invoice":
        return invoice_service.delete_invoice(tool_input["invoice_number"])

    elif tool_name == "create_customer":
        data = tool_input["data"]
        if not data.get("name"):
            return {"error": "Customer name is required"}
        if not data.get("email"):
            return {"error": "Customer email is required"}
        if not data.get("address"):
            return {"error": "Customer address is required"}
        return customer_service.create_customer(data)

    elif tool_name == "get_customers":
        return customer_service.get_customers(
            page=tool_input.get("page", 1),
            page_size=tool_input.get("limit", 20)
        )

    elif tool_name == "update_customer":
        return customer_service.update_customer(tool_input["customer_id"], tool_input["data"])

    elif tool_name == "delete_customer":
        return customer_service.delete_customer(tool_input["customer_id"])

    elif tool_name == "get_dashboard_analytics":
        return dashboard_service.get_dashboard_analytics()

    elif tool_name == "get_earnings_by_date_range":
        return dashboard_service.get_earnings_by_date_range(
            tool_input["start_date"], tool_input["end_date"]
        )

    else:
        return {"error": f"Unknown freelance tool: {tool_name}"}


# ─── Shared handlers (agent-type-agnostic) ──────────────────────────────

def _execute_attach_file_to_chat(tool_input, user_id, *,
                                 file_attachment_service, conversation_id):
    """ak-9dz: dispatch for the shared `attach_file_to_chat` MCP tool.

    Registered in both INVESTMENT_TOOLS and FREELANCE_TOOLS (see
    `services/agent_tools.py`). Routed BEFORE the per-agent switch in
    `execute_tool` so both agents share the same handler.

    Input contract (per tool schema):
      * local_path: absolute filesystem path where the agent wrote the file
      * display_name: human-friendly filename for the chat card + download

    Returns the content-block dict emitted by AgentFileAttachmentService.attach
    (`{type: "file_attachment", url, name, size_bytes, mime_type, uuid}`).
    The SDK's MCP bridge serializes this into a tool result the FE
    detects and renders as a download card.

    Errors surface via ToolValidationError (missing args / sanitizer
    rejection) so the LLM sees is_error=True and can retry; genuine
    infra failures (FileNotFound / OSError / DB error) bubble as raw
    Exception → caller wraps as {"error": str(e)}.
    """
    if file_attachment_service is None:
        # Should be impossible in production (agentService wires it),
        # but guard so the tool doesn't silently no-op if the wiring
        # ever regresses.
        raise RuntimeError(
            "attach_file_to_chat: file_attachment_service not wired"
        )
    local_path = tool_input.get("local_path")
    display_name = tool_input.get("display_name")
    if not local_path or not isinstance(local_path, str):
        raise ToolValidationError(
            "attach_file_to_chat: `local_path` is required (absolute "
            "filesystem path where the file exists)."
        )
    if not display_name or not isinstance(display_name, str):
        raise ToolValidationError(
            "attach_file_to_chat: `display_name` is required (filename "
            "to show in the chat card)."
        )
    try:
        return file_attachment_service.attach(
            user_id=user_id,
            local_path=local_path,
            display_name=display_name,
            conversation_id=conversation_id,
            message_id=None,  # v1: assistant msg row is persisted post-turn
        )
    except (ValueError, FileNotFoundError) as exc:
        # Caller-visible input errors (bad display_name / missing source
        # file) are ToolValidationError so the LLM can retry with
        # corrected args. Distinguished from raw infra faults which
        # bubble as generic Exception.
        raise ToolValidationError(f"attach_file_to_chat: {exc}") from exc
