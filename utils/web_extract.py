"""
Shared helper: GET a known upstream URL via `requests`, then ask sonnet to
extract structured JSON from the response body using `output_format` JSON
schema.

This replaces the brittle SDK-driven `WebSearch` + `WebFetch` shape used by
AIRateTask v1 and AIHelper v1, which had been silently returning zero
TextBlocks for the rate-fetcher flows (`turns=0 tools_called=0` — model
declining to even attempt the search under `bypassPermissions` + tool-
implying prompt). The replacement mirrors the only sonnet+structured-output
site in the codebase that's been healthy: the PDF text-mode flow at
`services/mailProcessorService.py:1425-1434`.

Why `requests` and not `httpx`: `requests~=2.32.3` is already pinned in
requirements.txt and used by ~4 sibling services (NpsService,
currencyService, StatementDownloadService, SetStockOldCodes). No reason to
add a new dependency for a sync GET; `anyio.run` wraps the SDK piece.

Refs: hq-wisp-bommx (Family B v2 architecture directive).
"""

from __future__ import annotations  # PEP 604 (`dict | None`) on 3.9 worktree env

import anyio
import requests
from claude_agent_sdk import ClaudeAgentOptions

from utils.logger import Logger
from utils.sdk_runner import run_query_collect, _make_sdk_stderr_logger

_logger = Logger(__name__).get_logger()


# Default user-agent — some Indian govt sites reject empty / `python-requests/*`.
_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def fetch_and_extract(
    *,
    url: str,
    system_prompt: str,
    schema: dict,
    agent: str,
    extra_context: str = "",
    headers: dict | None = None,
    timeout: int = 30,
    max_chars: int = 200_000,
    model: str = "sonnet",
) -> tuple[dict | None, str]:
    """
    GET `url`, then ask sonnet to extract `schema`-shaped JSON from the body.

    Args:
        url: HTTP(S) URL to fetch synchronously via `requests.get`.
        system_prompt: System prompt for the extraction agent.
        schema: JSON Schema dict for `output_format` (the SDK enforces this).
        agent: Label for the `agent_run` log line (e.g. "rate.gold").
        extra_context: Optional extra text prepended to the user message —
            used by stocks.ipo to pass per-call instructions (e.g. the list
            of ISINs the caller needs prices for) alongside the page body.
        headers: Optional request headers override. Defaults to a desktop UA.
        timeout: requests timeout in seconds.
        max_chars: Truncate the response body to this many chars before
            handing it to sonnet (prevents IPC bloat on huge pages). The
            extraction agent gets a note in the user message if truncation
            happened, so it can flag low-confidence answers.
        model: SDK model alias or pinned model_id. Defaults to "sonnet" for
            backward compatibility. Per-call override exists for diag /
            probe scenarios (hq-wisp-ezveu: rate.gold runs on "haiku" to
            distinguish sonnet-alias-flip from account-wide failure).

    Returns:
        (parsed_dict, "") on success — schema-validated.
        (None, detail) on any failure. `detail` is a short string suitable
        for the job result column so failures are diagnosable without
        journalctl access.
    """
    # ── 1. Fetch the upstream HTML/JSON ──────────────────────────────
    req_headers = headers if headers is not None else _DEFAULT_HEADERS
    try:
        resp = requests.get(url, headers=req_headers, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        return None, f"HTTP fetch failed for {url}: {e}"

    body = resp.text
    truncated = False
    if max_chars and len(body) > max_chars:
        truncated = True
        original_len = len(body)
        body = body[:max_chars]
        _logger.warning(
            f"{agent}: truncated upstream response {original_len} → {max_chars} chars"
        )

    # ── 2. Ask the LLM to extract structured JSON via output_format ──
    # v3.4 (hq-wisp-wzagg): stderr callback wired DIRECTLY in constructor
    # (not late-mutated by run_query_collect) because v3.3's late-mutate
    # didn't fire — the SDK reads options at transport-create time and may
    # not see post-construction mutations under all paths. extra_args adds
    # the CLI's --debug-to-stderr flag for verbose stderr emission so
    # request_id + resolved model_id are surfaced even on fast-fail exits.
    #
    # v5 (hq-wisp-a0byf): allowed_tools=[] locks the model to the
    # structured_output path only. Without it, sonnet has occasionally
    # wandered into Grep/Read/etc. on larger HTML payloads (rate.epf
    # symptom post-v4: tools_called>0, structured_output=None). The HTML
    # is already in the user message; no tool is ever the right answer.
    options = ClaudeAgentOptions(
        model=model,
        system_prompt=system_prompt,
        max_turns=3,  # Allow retries for structured output validation
        permission_mode="bypassPermissions",
        output_format={"type": "json_schema", "schema": schema},
        allowed_tools=[],
        stderr=_make_sdk_stderr_logger(agent),
        extra_args={"debug-to-stderr": None},
    )

    parts = []
    if extra_context:
        parts.append(extra_context)
    parts.append(f"Source URL: {url}")
    if truncated:
        parts.append(
            f"NOTE: Upstream response was truncated at {max_chars} characters. "
            "If you cannot find the requested data in this excerpt, return "
            "whatever subset you can find — partial data is better than none."
        )
    parts.append("Response body:")
    parts.append(body)
    user_message = "\n\n".join(parts)

    async def make_prompt():
        yield {
            "type": "user",
            "session_id": "",
            "message": {"role": "user", "content": user_message},
            "parent_tool_use_id": None,
        }

    async def run_query():
        return await run_query_collect(
            agent=agent, options=options, prompt=make_prompt(),
        )

    try:
        result = anyio.run(run_query)
    except Exception as e:
        return None, f"SDK exception during extraction: {e}"

    if result.error:
        return None, f"Sonnet extraction error: {result.error}"
    if result.structured_output is None:
        # Output_format is supposed to force structured output. v5 splits
        # this error into two arms so future-you can tell them apart:
        #   - tools_called>0 → model wandered into a tool instead of
        #     emitting structured_output. Pre-v5 this happened when
        #     allowed_tools wasn't locked (rate.epf post-v4 symptom).
        #   - tools_called==0 → model fast-failed without trying — usually
        #     an API-level 400 (e.g. invalid property keys, the v4 root
        #     cause) where the CLI subprocess exited before the model
        #     could respond.
        if result.tool_calls > 0:
            return None, (
                f"Sonnet wandered into tools (called={result.tool_calls}) "
                "instead of structured_output"
            )
        return None, "Sonnet returned no structured_output (output_format ignored)"

    return result.structured_output, ""
