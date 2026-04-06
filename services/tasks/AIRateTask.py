"""
Base class for tasks that use Claude AI + WebSearch to fetch rate data.
Replaces brittle HTML/PDF scraping with AI-driven web search.
"""

import json
import re

import anyio
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    AssistantMessage,
    ResultMessage,
    TextBlock,
)

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class AIRateTask(BaseTask):
    """Base for tasks that use Claude + WebSearch to fetch rate data."""

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):
            super().__init__(title, priority)
            self.ai_logger = Logger(__name__).get_logger()

    def fetch_rates_via_ai(self, prompt: str, timeout: int = 120) -> dict | None:
        """
        Call Claude with built-in web search, return parsed JSON.
        Uses claude_agent_sdk query() with bypassPermissions so the agent
        can use WebSearch without user approval.
        """
        system_prompt = (
            "You are a data extraction assistant. Your job is to search the web "
            "for the requested financial rate data and return ONLY a valid JSON object. "
            "Do NOT include any markdown, code fences, explanatory text, or conversation. "
            "Return raw JSON only."
        )

        options = ClaudeAgentOptions(
            model="sonnet",
            system_prompt=system_prompt,
            max_turns=8,
            permission_mode="bypassPermissions",
        )

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
                        "content": prompt,
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

        try:
            anyio.run(run_query)
        except Exception as e:
            self.ai_logger.error(f"AI rate fetch failed: {e}")
            return None

        if error_msg:
            self.ai_logger.error(f"AI rate fetch error: {error_msg}")
            return None

        raw_response = "".join(text_parts).strip()
        if not raw_response:
            self.ai_logger.error("AI returned empty response")
            return None

        return self._extract_json(raw_response)

    def _extract_json(self, text: str) -> dict | None:
        """Extract a JSON object from AI response text."""
        # Try direct parse first
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try extracting from markdown code fences
        fence_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
        if fence_match:
            try:
                return json.loads(fence_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try finding any JSON object in the text
        # Find the first { and last } to extract JSON
        first_brace = text.find('{')
        last_brace = text.rfind('}')
        if first_brace != -1 and last_brace > first_brace:
            candidate = text[first_brace:last_brace + 1]
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass

        self.ai_logger.error(f"Could not extract JSON from AI response: {text[:300]}...")
        return None
