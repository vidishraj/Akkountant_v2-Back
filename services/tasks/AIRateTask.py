"""
Base class for tasks that fetch financial rate data.

v2 architecture (post-hq-wisp-bommx): fetch a known upstream URL via
`requests`, then extract structured JSON via sonnet with `output_format`
JSON schema. Replaces v1's SDK-driven `WebSearch` + `WebFetch` shape, which
had been silently returning zero TextBlocks (`turns=0 tools_called=0`) for
rate-fetcher flows. See `utils/web_extract.py` for the shared helper.

Subclasses declare `URL`, `EXTRACTION_SYSTEM_PROMPT`, `OUTPUT_SCHEMA`, and
the `agent` label, plus any post-processing (e.g. PPF's period-to-monthly
expansion). The base handles the fetch + extract + JSON unwrapping.
"""

from __future__ import annotations  # PEP 604 (`dict | None`) on 3.9 worktree env

from services.tasks.baseTask import BaseTask
from utils.logger import Logger
from utils.web_extract import fetch_and_extract


class AIRateTask(BaseTask):
    """Base for tasks that fetch + extract rate data from a known upstream URL."""

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):
            super().__init__(title, priority)
            self.ai_logger = Logger(__name__).get_logger()

    def fetch_rates_via_ai(
        self, *, url: str, system_prompt: str, schema: dict, agent: str,
        extra_context: str = "", headers: dict | None = None,
    ) -> tuple[dict | None, str]:
        """
        v2 entry point: fetch `url`, extract `schema`-shaped JSON.

        Returns:
            (jsonData, "") on success — already schema-validated.
            (None, detail) on failure. `detail` is short and embeddable in
            the job result so failures are diagnosable from jobs.result alone
            instead of journalctl.
        """
        result, err = fetch_and_extract(
            url=url,
            system_prompt=system_prompt,
            schema=schema,
            agent=agent,
            extra_context=extra_context,
            headers=headers,
        )
        if err:
            self.ai_logger.error(f"{agent} fetch_and_extract failed: {err}")
            return None, err
        return result, ""
