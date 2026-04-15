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
                 dashboard_service=None, mail_processor=None):
    """
    Execute a tool call by routing to the appropriate existing service method.
    Returns a JSON-serializable dict with the tool result.
    """
    # Ensure flask g context has firebase_id for services that read it
    g.firebase_id = user_id

    try:
        if agent_type == "investment":
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

    else:
        return {"error": f"Unknown investment tool: {tool_name}"}


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
