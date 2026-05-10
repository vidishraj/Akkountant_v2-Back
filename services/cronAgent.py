"""
AI-powered cron orchestrator that intelligently decides what investment data to refresh.

Runs as a daemon thread every 30 minutes, using Claude to analyze data freshness
and trigger appropriate refresh jobs via the existing TaskScheduler pipeline.
"""

import json
import threading
import time

import anyio
from flask import Response, g

from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    SdkMcpTool,
    create_sdk_mcp_server,
    AssistantMessage,
    ResultMessage,
    TextBlock,
)
from collections import deque
from utils.logger import Logger

MCP_SERVER_NAME = "cron-agent-tools"
MAX_HISTORY = 5  # Keep last 5 cycle summaries for context

SYSTEM_PROMPT = """\
You are an autonomous cron scheduler for a personal finance app. Your job is to check
what investment data is stale and trigger refresh jobs when appropriate.

## Workflow

1. Call `get_rate_freshness` to see the last-updated timestamps of all cached data files.
2. Call `get_jobs_status` with `filters={"status":"Pending"}` to see what jobs are already queued.
3. Decide which data types need refreshing based on the freshness thresholds below.
4. Call `trigger_rate_refresh` for each data type that is stale and not already queued.

## Freshness Thresholds

| Data Type | Threshold |
|-----------|-----------|
| SetNPSRate | 6 h (market hours 9-17 IST), 18 h outside |
| SetMFRate | 6 h (market hours), 18 h outside |
| SetGoldRate | 6 h (market hours), 18 h outside |
| SetPPFRate | 6 h (market hours), 18 h outside |
| SetMFDetails | 24 h |
| SetNPSDetails | 24 h |
| SetStocksDetails | 24 h |
| EPFRate | 7 days (changes very infrequently) |

Market hours: 09:00 – 17:00 IST (UTC+5:30), Monday – Friday.

## Rules

- Do NOT trigger a refresh if a Pending or Overdue job for the same title already exists.
- Do NOT trigger a refresh if the data type has failures >= 5 in its most recent job.
- If everything is fresh and nothing needs refreshing, just say so.
- Output a brief structured summary of what you checked and what you triggered. Keep it concise
  — this is a log entry, not a conversation.
"""

CRON_USER_ID = "CRON_AGENT"


class CronAgent:
    """AI-powered cron orchestrator that runs as a daemon thread."""

    def __init__(self, flask_app, investment_service, interval_seconds=1800):
        self.logger = Logger(__name__).get_logger()
        self.flask_app = flask_app
        self.investment_service = investment_service
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread = None
        self._history = deque(maxlen=MAX_HISTORY)  # Rolling conversation history

    def start(self):
        """Launch the daemon thread."""
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Signal the daemon to shut down."""
        self._stop_event.set()

    # ── Main loop ──────────────────────────────────────────────────────

    def _run_loop(self):
        """Wait 30 s for Flask to initialize, then run on interval."""
        from utils.DateTimeUtil import is_within_allowed_window, seconds_until_allowed_window

        # Startup delay
        if self._stop_event.wait(timeout=30):
            return

        while not self._stop_event.is_set():
            if not is_within_allowed_window():
                wait = seconds_until_allowed_window()
                self.logger.info(f"CronAgent: outside 1-7AM IST window, sleeping {wait/3600:.1f}h")
                if self._stop_event.wait(timeout=wait):
                    break
                continue

            try:
                self._run_once()
            except Exception as e:
                self.logger.error(f"CronAgent cycle error: {e}", exc_info=True)

            # Interruptible sleep
            if self._stop_event.wait(timeout=self.interval_seconds):
                break

    def _build_prompt_with_history(self):
        """Build the user prompt including summaries of the last N cycles."""
        parts = ["Run a cron check now. Check freshness, check pending jobs, "
                 "and trigger any needed refreshes."]

        if self._history:
            parts.append("\n\n## Previous cycle summaries (most recent first):")
            for i, entry in enumerate(reversed(self._history), 1):
                parts.append(f"\n### Cycle -{i}:\n{entry}")

        return "\n".join(parts)

    def _run_once(self):
        """Execute a single AI-driven cron cycle inside Flask app context."""
        with self.flask_app.app_context():
            g.firebase_id = CRON_USER_ID

            sdk_tools = self._build_tools()
            mcp_server = create_sdk_mcp_server(
                name=MCP_SERVER_NAME,
                tools=sdk_tools,
            )

            options = ClaudeAgentOptions(
                model="sonnet",
                system_prompt=SYSTEM_PROMPT,
                max_turns=10,
                mcp_servers={MCP_SERVER_NAME: mcp_server},
                permission_mode="bypassPermissions",
                # MCP tool names listed explicitly (defensive — see deep-study §7.2)
                allowed_tools=[
                    f"mcp__{MCP_SERVER_NAME}__{t.name}" for t in sdk_tools
                ],
            )

            prompt_text = self._build_prompt_with_history()
            text_parts = []
            error_msg = None

            async def run_query():
                nonlocal error_msg

                async def make_prompt():
                    yield {
                        "type": "user",
                        "session_id": "",
                        "message": {
                            "role": "user",
                            "content": prompt_text,
                        },
                        "parent_tool_use_id": None,
                    }

                async for message in query(prompt=make_prompt(), options=options):
                    if isinstance(message, AssistantMessage):
                        if message.error:
                            error_msg = f"Claude error: {message.error}"
                            return
                        for block in message.content:
                            if isinstance(block, TextBlock):
                                text_parts.append(block.text)
                    elif isinstance(message, ResultMessage):
                        if message.is_error:
                            error_msg = message.result or "Query failed"

            anyio.run(run_query)

            if error_msg:
                self.logger.error(f"CronAgent query error: {error_msg}")
                self._history.append(f"[ERROR] {error_msg}")
            else:
                summary = "".join(text_parts).strip()
                self.logger.info(f"CronAgent cycle complete:\n{summary}")
                self._history.append(summary)

    # ── Tool building ──────────────────────────────────────────────────

    def _build_tools(self):
        """Build the 3 MCP tools the cron agent can use."""
        tools = []

        # 1. get_rate_freshness
        async def handle_freshness(args):
            result = self._execute_tool("get_rate_freshness", args)
            return {
                "content": [{"type": "text", "text": json.dumps(result, default=str)}]
            }

        tools.append(SdkMcpTool(
            name="get_rate_freshness",
            description=(
                "Returns last-updated timestamps for all cached rate/data files. "
                "Use this to determine which data types are stale and need refreshing."
            ),
            input_schema={
                "type": "object",
                "properties": {},
                "required": [],
            },
            handler=handle_freshness,
        ))

        # 2. get_jobs_status
        async def handle_jobs_status(args):
            result = self._execute_tool("get_jobs_status", args)
            return {
                "content": [{"type": "text", "text": json.dumps(result, default=str)}]
            }

        tools.append(SdkMcpTool(
            name="get_jobs_status",
            description=(
                "Query the jobs table with optional filters. Use to check for existing "
                "Pending/Overdue jobs to avoid creating duplicates. Supports filters: "
                "title, status, priority; sort_by, sort_order, page, page_size."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "page": {"type": "integer", "default": 1},
                    "filters": {
                        "type": "object",
                        "properties": {
                            "title": {"type": "string"},
                            "status": {"type": "string"},
                            "priority": {"type": "string"},
                        },
                    },
                    "sort_by": {
                        "type": "string",
                        "default": "due_date",
                        "enum": ["id", "title", "status", "priority", "due_date", "failures"],
                    },
                    "sort_order": {
                        "type": "string",
                        "default": "desc",
                        "enum": ["asc", "desc"],
                    },
                    "page_size": {"type": "integer", "default": 50},
                },
                "required": [],
            },
            handler=handle_jobs_status,
        ))

        # 3. trigger_rate_refresh
        async def handle_trigger(args):
            result = self._execute_tool("trigger_rate_refresh", args)
            return {
                "content": [{"type": "text", "text": json.dumps(result, default=str)}]
            }

        tools.append(SdkMcpTool(
            name="trigger_rate_refresh",
            description=(
                "Create a new Pending job to refresh a specific data type. "
                "Valid job_id values: SetNPSRate, SetNPSDetails, SetStocksDetails, "
                "SetMFRate, SetMFDetails, SetGoldRate, SetPPFRate, SetEPFRate."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "enum": [
                            "SetNPSRate", "SetNPSDetails", "SetStocksDetails",
                            "SetMFRate", "SetMFDetails", "SetGoldRate", "SetPPFRate", "SetEPFRate",
                        ],
                        "description": "The job type to trigger.",
                    },
                },
                "required": ["job_id"],
            },
            handler=handle_trigger,
        ))

        return tools

    # ── Tool execution ─────────────────────────────────────────────────

    def _execute_tool(self, tool_name, args):
        """Route tool calls to the appropriate service method."""
        try:
            if tool_name == "get_rate_freshness":
                result = self.investment_service.getFileTimeStamps()
            elif tool_name == "get_jobs_status":
                result = self.investment_service.getJobsTable(
                    page=args.get("page", 1),
                    filters=args.get("filters"),
                    sort_by=args.get("sort_by", "due_date"),
                    sort_order=args.get("sort_order", "desc"),
                    page_size=args.get("page_size", 50),
                    limit=args.get("page_size", 50),
                )
            elif tool_name == "trigger_rate_refresh":
                job_id = args.get("job_id")
                result = self.investment_service.setJobsTable(job_id, CRON_USER_ID)
            else:
                return {"error": f"Unknown tool: {tool_name}"}

            unwrapped = self._unwrap_response(result)
            return self._make_serializable(unwrapped)
        except Exception as e:
            self.logger.error(f"CronAgent tool error ({tool_name}): {e}", exc_info=True)
            return {"error": str(e)}

    # ── Serialization helpers ──────────────────────────────────────────

    @staticmethod
    def _unwrap_response(result):
        """Unwrap Flask jsonify responses into plain dicts."""
        if result is None:
            return {"result": "success"}
        if isinstance(result, tuple):
            return CronAgent._unwrap_response(result[0])
        if isinstance(result, Response):
            try:
                return json.loads(result.get_data(as_text=True))
            except (json.JSONDecodeError, Exception):
                return {"result": result.get_data(as_text=True)}
        if isinstance(result, (dict, str, int, float, bool)):
            return result
        if isinstance(result, list):
            return {"results": result}
        return {"result": str(result)}

    @staticmethod
    def _make_serializable(obj):
        """Ensure the result is JSON-serializable."""
        if isinstance(obj, dict):
            return {k: CronAgent._make_serializable(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [CronAgent._make_serializable(item) for item in obj]
        if hasattr(obj, 'value'):  # Enum
            return obj.value
        if hasattr(obj, 'isoformat'):  # datetime/date
            return obj.isoformat()
        if hasattr(obj, '__float__'):  # Decimal
            return float(obj)
        return obj
