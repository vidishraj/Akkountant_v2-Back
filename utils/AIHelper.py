"""
Standalone AI helper for calling Claude with web search.
No imports from services/ to avoid circular dependencies.
"""

import json
import re

import anyio
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    AssistantMessage,
    TextBlock,
)
from utils.logger import Logger

_logger = Logger(__name__).get_logger()


def fetch_via_ai(prompt: str, system: str = None) -> tuple[dict | None, str]:
    """
    Call Claude with built-in web search to fetch structured data.
    Returns (parsed JSON dict or None, error_detail string).
    """
    if system is None:
        system = (
            "You are a financial data assistant. "
            "Use web search to find the requested data. "
            "Respond with ONLY a valid JSON object — no markdown, no explanation."
        )

    # allowed_tools is required: with permission_mode='bypassPermissions' and a
    # tool-implying system prompt, missing allowed_tools makes built-in tools
    # silently unavailable and the SDK returns zero TextBlocks (April outage shape).
    options = ClaudeAgentOptions(
        model="sonnet",
        system_prompt=system,
        max_turns=6,
        permission_mode="bypassPermissions",
        allowed_tools=["WebSearch", "WebFetch"],
    )

    text_parts = []
    error_msg = None

    async def run():
        nonlocal error_msg

        async def make_prompt():
            yield {
                "type": "user",
                "session_id": "",
                "message": {"role": "user", "content": prompt},
                "parent_tool_use_id": None,
            }

        async for message in query(prompt=make_prompt(), options=options):
            if isinstance(message, AssistantMessage):
                if message.error:
                    error_msg = str(message.error)
                    return
                for block in message.content:
                    if isinstance(block, TextBlock):
                        text_parts.append(block.text)

    try:
        anyio.run(run)
    except Exception as e:
        detail = f"fetch_via_ai exception: {e}"
        _logger.error(detail)
        return None, detail

    if error_msg:
        detail = f"fetch_via_ai assistant error: {error_msg}"
        _logger.error(detail)
        return None, detail

    raw = "".join(text_parts).strip()
    if not raw:
        detail = "fetch_via_ai: empty response"
        _logger.warning(detail)
        return None, detail

    parsed = _extract_json(raw)
    if parsed is None:
        return None, "fetch_via_ai: could not extract JSON from response"
    return parsed, ""


def _extract_json(text: str) -> dict | None:
    """Extract a JSON object from text that may contain markdown fences or prose."""
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try inside ```json ... ```
    fence = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if fence:
        try:
            return json.loads(fence.group(1))
        except json.JSONDecodeError:
            pass

    # Try first { to last }
    first = text.find('{')
    last = text.rfind('}')
    if first != -1 and last > first:
        try:
            return json.loads(text[first:last + 1])
        except json.JSONDecodeError:
            pass

    _logger.warning(f"fetch_via_ai: could not extract JSON: {text[:300]}")
    return None
