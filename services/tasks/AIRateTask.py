"""
Base class for tasks that use Claude AI + WebSearch to fetch rate data.
Replaces brittle HTML/PDF scraping with AI-driven web search.
"""

import json
import re

import anyio
from claude_agent_sdk import ClaudeAgentOptions

from services.tasks.baseTask import BaseTask
from utils.logger import Logger
from utils.sdk_runner import run_query_collect


class AIRateTask(BaseTask):
    """Base for tasks that use Claude + WebSearch to fetch rate data."""

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):
            super().__init__(title, priority)
            self.ai_logger = Logger(__name__).get_logger()

    def fetch_rates_via_ai(self, prompt: str, timeout: int = 120,
                           agent: str = "rate.unknown") -> tuple[dict | None, str]:
        """
        Call Claude with built-in WebSearch / WebFetch, return (parsed JSON, error detail).

        Returns:
            (jsonData, "") on success.
            (None, detail) on any failure. `detail` is non-empty and short — caller
            should embed it in the job result so failures are diagnosable from
            jobs.result alone instead of journalctl.
        """
        # Failure-shape note: the previous gag ("Return ONLY a valid JSON object.
        # Do NOT include any markdown, code fences, explanatory text, or
        # conversation. Return raw JSON only.") matched the SetGoldRate empty-
        # TextBlocks shape (turns=0 tools_called=0 latency_ms~4s status=ok).
        # Hypothesis: with prose forbidden AND web search needed, sonnet has
        # no permitted way to narrate its plan ("I'll search for X first"),
        # so it gives up before invoking WebSearch. Softer prompt permits
        # narration; _extract_json downstream is permissive (handles raw JSON,
        # markdown-fenced JSON, and first-{ to last-} extraction).
        system_prompt = (
            "You are a data extraction assistant. Search the web for the "
            "requested financial rate data and return a JSON object matching "
            "the structure described in the user prompt. You may briefly "
            "describe what you are searching for as you work. Your final "
            "response MUST contain a JSON object — either as the entire "
            "response, or wrapped in ```json ... ``` markdown fences, or as a "
            "single recognisable {...} block in your reply. The JSON is "
            "extracted programmatically downstream, so the JSON content "
            "matters more than the surrounding prose."
        )

        # allowed_tools is required: with permission_mode='bypassPermissions' and
        # no allowed_tools, the agent silently returns zero TextBlocks because it
        # cannot use any tools and the system prompt forbids non-JSON text.
        options = ClaudeAgentOptions(
            model="sonnet",
            system_prompt=system_prompt,
            max_turns=8,
            permission_mode="bypassPermissions",
            allowed_tools=["WebSearch", "WebFetch"],
        )

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

        async def run_query():
            return await run_query_collect(
                agent=agent, options=options, prompt=make_prompt(),
            )

        try:
            result = anyio.run(run_query)
        except Exception as e:
            detail = f"SDK exception: {e}"
            self.ai_logger.error(f"AI rate fetch failed: {e}")
            return None, detail

        if result.error:
            self.ai_logger.error(f"AI rate fetch error: {result.error}")
            return None, f"Provider error: {result.error}"

        raw_response = result.text.strip()
        if not raw_response:
            self.ai_logger.error("AI returned empty response")
            return None, "Empty response (no TextBlocks — likely missing allowed_tools or model refused)"

        parsed = self._extract_json(raw_response)
        if parsed is None:
            head = raw_response[:300].replace("\n", " ")
            return None, f"JSON extract failed. Response head: {head}"
        return parsed, ""

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
