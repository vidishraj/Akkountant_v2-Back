"""
Core agent orchestration service.
Uses claude_agent_sdk to route queries through Claude Code CLI (no API key needed).
Tool calls are handled via an in-process MCP server.
"""

import json
import os
import anyio
from services.Base_Service import BaseService
from services.agent_tools import get_agent_config
from services.agent_tool_executor import execute_tool
from utils.logger import Logger
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    SdkMcpTool,
    create_sdk_mcp_server,
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)

MCP_SERVER_NAME = "agent_tools"


# Tools that require user confirmation before execution
DESTRUCTIVE_TOOLS = {
    "delete_single_investment",
    "delete_all_investments",
    "delete_file",
    "delete_invoice",
    "delete_customer",
    "scan_emails_for_transactions",
    "scan_statements",
    "sync_kite_holdings",
}

# Tools that mutate data (used to signal frontend to refresh)
MUTATION_TOOLS = {
    "insert_investment",
    "delete_single_investment",
    "delete_all_investments",
    "sync_kite_holdings",
    "update_transaction",
    "scan_emails_for_transactions",
    "scan_statements",
    "delete_file",
    "create_invoice",
    "update_invoice",
    "delete_invoice",
    "create_customer",
    "update_customer",
    "delete_customer",
    "trigger_rate_refresh",
}

MAX_TURNS = 20


class AgentService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AgentService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()
        self.investment_service = None
        self.transaction_service = None
        self.invoice_service = None
        self.customer_service = None
        self.dashboard_service = None

    def set_services(self, investment_service=None, transaction_service=None,
                     invoice_service=None, customer_service=None,
                     dashboard_service=None):
        """Called from app.py to inject existing service instances."""
        self.investment_service = investment_service
        self.transaction_service = transaction_service
        self.invoice_service = invoice_service
        self.customer_service = customer_service
        self.dashboard_service = dashboard_service

    def stream_chat(self, agent_type, messages, user_id, confirmed_tools=None):
        """
        Generator that yields SSE events for the agent chat.
        Uses claude_agent_sdk with MCP tools to process queries.

        Event types:
        - text: {"type": "text", "content": "..."}
        - tool_exec: {"type": "tool_exec", "tool": "..."}
        - confirm: {"type": "confirm", "tool": "...", "input": {...}, "message": "..."}
        - done: {"type": "done", "mutations": [...]}
        - error: {"type": "error", "message": "..."}
        """
        confirmed_tools = confirmed_tools or []
        tool_events = []  # Collected intermediate events (tool_exec, confirm)
        mutations = []

        try:
            config = get_agent_config(agent_type)

            # Build MCP tools from tool definitions
            sdk_tools = self._build_sdk_tools(
                agent_type, config["tools"], user_id,
                tool_events, mutations, confirmed_tools,
            )

            # Create in-process MCP server
            mcp_server = create_sdk_mcp_server(
                name=MCP_SERVER_NAME,
                tools=sdk_tools,
            )

            # Format conversation history as prompt text
            prompt_text = self._format_conversation(messages)

            # Configure SDK options — no allowed_tools restriction so the agent
            # can use built-in tools (WebSearch, Bash, etc.) alongside MCP tools
            options = ClaudeAgentOptions(
                system_prompt=config["system_prompt"],
                mcp_servers={MCP_SERVER_NAME: mcp_server},
                permission_mode="bypassPermissions",
                max_turns=MAX_TURNS,
                model="sonnet",
            )

            # Run the query (blocking — collects all results then yields)
            text_parts = []
            error_msg = None

            async def run_query():
                nonlocal error_msg

                # Use AsyncIterable prompt to avoid SDK bug where string prompts
                # close stdin before MCP control responses can be written back.
                async def make_prompt():
                    yield {
                        "type": "user",
                        "session_id": "",
                        "message": {"role": "user", "content": prompt_text},
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
                yield self._sse_event("error", {"message": error_msg})
                return

            # Yield collected tool/confirm events
            for event_type, data in tool_events:
                yield self._sse_event(event_type, data)

            # Yield final text
            full_text = "".join(text_parts)
            if full_text:
                yield self._sse_event("text", {"content": full_text})

            # Done
            yield self._sse_event("done", {"mutations": mutations})

        except Exception as e:
            self.logger.error(f"Agent error: {e}")
            yield self._sse_event("error", {"message": str(e)})

    def _build_sdk_tools(self, agent_type, tool_defs, user_id,
                         tool_events, mutations, confirmed_tools):
        """Build SdkMcpTool objects from tool definitions with handler closures."""
        sdk_tools = []

        for tool_def in tool_defs:
            tool_name = tool_def["name"]

            # Use default-arg capture to bind loop variable
            async def handler(args, _tn=tool_name, _at=agent_type):
                # Check if destructive and not confirmed
                if _tn in DESTRUCTIVE_TOOLS and _tn not in confirmed_tools:
                    msg = AgentService._get_confirmation_message(_tn, args)
                    tool_events.append(("confirm", {
                        "tool": _tn,
                        "input": args,
                        "message": msg,
                    }))
                    return {
                        "content": [{
                            "type": "text",
                            "text": (
                                f"Action cancelled — user confirmation required for {_tn}. "
                                "Please inform the user and ask them to confirm."
                            ),
                        }],
                        "is_error": True,
                    }

                # Record tool execution event
                tool_events.append(("tool_exec", {"tool": _tn}))

                # Execute via existing services
                result = execute_tool(
                    _at, _tn, args, user_id,
                    investment_service=self.investment_service,
                    transaction_service=self.transaction_service,
                    invoice_service=self.invoice_service,
                    customer_service=self.customer_service,
                    dashboard_service=self.dashboard_service,
                )

                # Track mutations
                if _tn in MUTATION_TOOLS:
                    mutations.append(_tn)

                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps(result, default=str),
                    }]
                }

            sdk_tools.append(SdkMcpTool(
                name=tool_name,
                description=tool_def["description"],
                input_schema=tool_def["input_schema"],
                handler=handler,
            ))

        return sdk_tools

    @staticmethod
    def _format_conversation(messages):
        """Format message history into a prompt string for claude_agent_sdk."""
        if not messages:
            return ""

        # Single message — return content directly
        if len(messages) == 1:
            content = messages[0].get("content", "")
            return content if isinstance(content, str) else str(content)

        # Multi-turn — include history
        history_parts = []
        for msg in messages[:-1]:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "partial_assistant":
                continue
            if isinstance(content, str):
                prefix = "User" if role == "user" else "Assistant"
                history_parts.append(f"{prefix}: {content}")

        latest_msg = messages[-1]
        latest_content = latest_msg.get("content", "")
        if not isinstance(latest_content, str):
            latest_content = str(latest_content)

        if history_parts:
            history = "\n\n".join(history_parts)
            return (
                f"Previous conversation:\n{history}\n\n"
                f"User's latest message: {latest_content}"
            )

        return latest_content

    @staticmethod
    def _sse_event(event_type, data):
        """Format an SSE event string."""
        payload = {"type": event_type, **data}
        return f"data: {json.dumps(payload, default=str)}\n\n"

    @staticmethod
    def _get_confirmation_message(tool_name, tool_input):
        """Generate a human-readable confirmation message for destructive tools."""
        messages = {
            "delete_single_investment": f"Delete investment record (buy ID: {tool_input.get('buy_id', 'unknown')})?",
            "delete_all_investments": f"Delete ALL {tool_input.get('service_type', '')} investments? This cannot be undone.",
            "delete_file": f"Delete file {tool_input.get('file_id', 'unknown')} and all its transactions?",
            "delete_invoice": f"Delete invoice {tool_input.get('invoice_number', 'unknown')}?",
            "delete_customer": f"Delete customer (ID: {tool_input.get('customer_id', 'unknown')})?",
            "scan_emails_for_transactions": "Scan Gmail for transaction emails? This may take several minutes.",
            "scan_statements": "Scan Gmail for bank statements? This may take several minutes.",
            "sync_kite_holdings": "Sync holdings from Kite Connect into local database?",
        }
        return messages.get(tool_name, f"Execute {tool_name}?")
