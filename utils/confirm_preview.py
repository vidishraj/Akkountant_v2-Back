"""ak-2ye9: structured preview + outcome payloads for assistant write
proposals (the confirm card).

Preview fields are built ONLY from the tool-call args that the confirm
token (utils/confirm_tokens.py, ak-dixt) binds via args_hash. The token
cannot verify against different args, so what the card shows is exactly
what gets written. Values are shown as the WRITE will store them:
  * date: parsed with the same converter the insert uses (stored date,
    ISO YYYY-MM-DD); if it can't be parsed the raw string is shown with
    `unparsed: true` (the write would reject it too)
  * NAV: computed exactly like InvestmentService.insertSecurityPurchase
    (`float(amount) / float(quantity)`, stored unrounded as buyPrice),
    flagged `derived`
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

# tool -> (op, op_label, done_message)
_TOOL_OPS = {
    "insert_investment": ("insert", "Record purchase", "Recorded."),
    "delete_single_investment": ("delete", "Delete investment record", "Deleted."),
    "delete_all_investments": ("delete_all", "Delete all investment records", "Deleted."),
    "delete_file": ("delete", "Delete file and its transactions", "Deleted."),
    "delete_invoice": ("delete", "Delete invoice", "Deleted."),
    "delete_customer": ("delete", "Delete customer", "Deleted."),
    "update_invoice": ("update", "Update invoice", "Updated."),
    "update_customer": ("update", "Update customer", "Updated."),
    "sync_kite_holdings": ("sync", "Sync Kite holdings", "Synced."),
    "scan_emails_for_transactions": ("scan", "Scan emails for transactions", "Done."),
    "scan_statements": ("scan", "Scan statements", "Done."),
    "process_mail_pipeline": ("scan", "Process financial emails", "Done."),
}
_DEFAULT_OP = ("update", "Apply change", "Done.")

NAV_SERVICE_TYPES = ("Mutual_Funds", "NPS")

# (args key, preview key, label) — top level of the tool args.
_TOP_FIELDS = (
    ("service_type", "asset", "Asset"),
    ("buy_id", "buy_id", "Buy ID"),
    ("file_id", "file_id", "File"),
    ("invoice_number", "invoice_number", "Invoice"),
    ("customer_id", "customer_id", "Customer"),
)
# Inside args["data"] (insert_investment). Order = what, how much, when.
_DATA_FIELDS = (
    ("schemeCode", "scheme", "Scheme"),
    ("description", "description", "Description"),
    ("goldType", "gold_type", "Gold purity"),
    ("quantity", "units", "Units"),
    ("amount", "amount", "Amount"),
    ("employee_amount", "employee_amount", "Employee contribution"),
    ("employer_amount", "employer_amount", "Employer contribution"),
    ("date", "date", "Date"),
)

OUTCOME_STATUSES = ("recorded", "failed", "not_executed", "cancelled")


def _scalar(v) -> bool:
    return isinstance(v, (str, int, float, bool)) and v is not None


def _positive_float(v) -> Optional[float]:
    if isinstance(v, bool) or not isinstance(v, (int, float, str)):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f) or f <= 0:
        return None
    return f


def derive_nav(amount, units) -> Optional[float]:
    """The per-unit price the MF/NPS insert stores: exactly
    `float(amount) / float(quantity)` (InvestmentService
    .insertSecurityPurchase), unrounded. None unless both > 0."""
    a, u = _positive_float(amount), _positive_float(units)
    if a is None or u is None:
        return None
    return a / u


def parse_write_date(service_type, raw) -> Optional[str]:
    """ISO date the insert will store for `raw`, or None if the insert's
    own parser would reject it. Mirrors the write paths: every
    insert_investment type converts with DateTimeUtil using the
    EPF_STATEMENT format; EPF additionally maps MM/YYYY to 01/MM/YYYY
    (EPFService.insertDeposit)."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        from utils.DateTimeUtil import DateTimeUtil
        from enums.DateFormatEnum import DateStatementEnum
        date_str = raw
        if service_type == "EPF" and not ("/" in raw and len(raw.split("/")) == 3):
            month, year = raw.split("/")
            date_str = f"01/{month}/{year}"
        parsed = DateTimeUtil().convert_to_sql_datetime_date(
            date_str, DateStatementEnum.EPF_STATEMENT.name)
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return None


def build_preview(tool: str, args: Any) -> Dict[str, Any]:
    op, op_label, _ = _TOOL_OPS.get(tool, _DEFAULT_OP)
    fields: List[Dict[str, Any]] = []
    a = args if isinstance(args, dict) else {}
    for src, key, label in _TOP_FIELDS:
        if src in a and _scalar(a[src]):
            fields.append({"key": key, "label": label, "value": a[src]})
    data = a.get("data")
    if isinstance(data, dict) and tool == "insert_investment":
        asset = a.get("service_type")
        nav = derive_nav(data.get("amount"), data.get("quantity")) \
            if asset in NAV_SERVICE_TYPES else None
        for src, key, label in _DATA_FIELDS:
            if src in data and _scalar(data[src]):
                lbl = "Grams" if (key == "units" and asset == "Gold") else label
                field = {"key": key, "label": lbl, "value": data[src]}
                if key == "date":
                    iso = parse_write_date(asset, data[src])
                    if iso is None:
                        field["unparsed"] = True
                    else:
                        field["value"] = iso
                fields.append(field)
            if key == "units" and nav is not None:
                fields.append({"key": "nav", "label": "NAV", "value": nav, "derived": True})
    elif isinstance(data, dict) and data:
        # e.g. update_invoice: name the fields being changed.
        fields.append({"key": "changes", "label": "Changes", "value": sorted(str(k) for k in data)})
    return {"op": op, "op_label": op_label, "fields": fields}


def build_outcome(confirm_id: str, tool: str, status: str,
                  interrupted: bool = False) -> Dict[str, Any]:
    """`interrupted`: the confirmed call started but never reported a
    result, so whether anything was written is unknown."""
    if status not in OUTCOME_STATUSES:
        raise ValueError(f"unknown outcome status: {status}")
    done_msg = _TOOL_OPS.get(tool, _DEFAULT_OP)[2]
    failed_msg = ("Interrupted — check your holdings before retrying."
                  if interrupted else
                  "Failed — the tool reported an error. Check before retrying.")
    message = {
        "recorded": done_msg,
        "failed": failed_msg,
        "not_executed": "Not applied — nothing recorded.",
        "cancelled": "Cancelled — nothing recorded.",
    }[status]
    return {
        "confirm_id": confirm_id,
        "tool": tool,
        "status": status,
        "recorded": status == "recorded",
        "message": message,
    }


__all__ = ["build_preview", "build_outcome", "derive_nav", "OUTCOME_STATUSES"]
