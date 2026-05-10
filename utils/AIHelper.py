"""
Standalone AI helper for calling Claude with web search.
No imports from services/ to avoid circular dependencies.
"""

import json
import re

import anyio
from claude_agent_sdk import ClaudeAgentOptions

from utils.logger import Logger
from utils.sdk_runner import run_query_collect

_logger = Logger(__name__).get_logger()


def fetch_via_ai(prompt: str, system: str = None) -> tuple[dict | None, str]:
    """
    Call Claude with built-in web search to fetch structured data.

    Returns:
        (parsed_json, "") on success.
        (None, detail) on any failure. `detail` is non-empty and short —
        callers should embed it in their log path / job result so failures
        are diagnosable without grepping journalctl.
    """
    if system is None:
        system = (
            "You are a financial data assistant. "
            "Use web search to find the requested data. "
            "Respond with ONLY a valid JSON object — no markdown, no explanation."
        )

    # allowed_tools is required: with permission_mode='bypassPermissions' and
    # no allowed_tools, the agent silently returns zero TextBlocks because it
    # cannot use any tools and the system prompt forbids non-JSON text.
    # Mirrors the AIRateTask fix in f15ef18.
    options = ClaudeAgentOptions(
        model="sonnet",
        system_prompt=system,
        max_turns=6,
        permission_mode="bypassPermissions",
        allowed_tools=["WebSearch", "WebFetch"],
    )

    async def make_prompt():
        yield {
            "type": "user",
            "session_id": "",
            "message": {"role": "user", "content": prompt},
            "parent_tool_use_id": None,
        }

    async def run():
        return await run_query_collect(
            agent="stocks.ipo", options=options, prompt=make_prompt(),
        )

    try:
        result = anyio.run(run)
    except Exception as e:
        detail = f"SDK exception: {e}"
        _logger.error(f"fetch_via_ai failed: {e}")
        return None, detail

    if result.error:
        _logger.error(f"fetch_via_ai error: {result.error}")
        return None, f"Provider error: {result.error}"

    raw = result.text.strip()
    if not raw:
        _logger.warning("fetch_via_ai: empty response")
        return None, "Empty response (no TextBlocks — likely missing allowed_tools or model refused)"

    parsed = _extract_json(raw)
    if parsed is None:
        head = raw[:300].replace("\n", " ")
        return None, f"JSON extract failed. Response head: {head}"
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
