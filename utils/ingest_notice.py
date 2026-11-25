"""ak-b5sv (2026-11-24): AgentChat `ingest_notice` SSE event.

The ak-tik degraded-dedup signal (degraded_dedup_rows / dual_tier_tuples,
summed per run by MailProcessorService.process_emails) reached the
email-scan banner (ak-dngn) but not AgentChat, where statement imports run
through the transaction agent's scan tools. This builds the event payload
the agent stream emits after such a tool returns, so the frontend can show
the SAME banner.

Event (contract sent to frontend, see bead ak-b5sv):
  {"type": "ingest_notice", "tool": <scan tool name>,
   "degraded_dedup_rows": int >= 0, "dual_tier_tuples": int >= 0}
Emitted only when at least one count is > 0, once per scan tool call.
Leaf module (no Flask / SDK imports).
"""
from __future__ import annotations

from utils.statement_ingest_outcome import DEDUP_SIGNAL_KEYS, dedup_count

# The transaction-agent tools that run MailProcessorService.process_emails
# (services/agent_tool_executor._execute_transaction_tool).
INGEST_TOOLS = frozenset({
    "scan_emails_for_transactions",
    "scan_statements",
    "process_mail_pipeline",
})

EVENT_TYPE = "ingest_notice"


def ingest_notice_from_result(tool_name, result):
    """Payload for an ingest_notice event, or None if there is nothing
    to report (not an ingest tool, not a summary dict, or both counts 0)."""
    if tool_name not in INGEST_TOOLS or not isinstance(result, dict):
        return None
    counts = {k: dedup_count(result.get(k)) for k in DEDUP_SIGNAL_KEYS}
    if not any(counts.values()):
        return None
    return {"tool": tool_name, **counts}
