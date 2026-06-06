"""
Shared runner for claude_agent_sdk invocations.

Emits one structured `agent_run` log line per SDK query so the SDK fleet is
observable from journalctl with a single grep:

    journalctl -u akkountant | grep 'agent_run '

Two entry points:

- `run_query_collect(...)` — full wrapper. Runs `claude_agent_sdk.query`,
  collects TextBlocks + structured_output, tracks turns/tool_calls/latency,
  emits the log line, returns an `SdkRunResult`. Use this for simple
  fire-and-collect patterns.

- `emit_agent_run(...)` — bare metrics emitter. Use this when the call site
  needs to drive the query loop itself (streaming chat, mid-flight MCP
  rebuild, structured_output post-processing). Track metrics manually and
  call this once at the end.
"""

from __future__ import annotations  # PEP 604 (`dict | None`) on 3.9 worktree env

import time
from dataclasses import dataclass

from claude_agent_sdk import (
    query as sdk_query,
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)
from utils.logger import Logger

_logger = Logger("agent_run").get_logger()


def emit_agent_run(*, agent, model, turns, tools_called, latency_ms,
                   status, error_class=None):
    """Emit one structured `agent_run` log line.

    Format (space-separated key=value, grep-friendly):
      agent_run agent=X model=Y turns=N tools_called=N latency_ms=N status=S [error_class=C]
    """
    parts = [
        "agent_run",
        f"agent={agent}",
        f"model={model or 'default'}",
        f"turns={turns}",
        f"tools_called={tools_called}",
        f"latency_ms={latency_ms}",
        f"status={status}",
    ]
    if error_class:
        parts.append(f"error_class={error_class}")
    _logger.info(" ".join(parts))


@dataclass
class SdkRunResult:
    text: str
    structured_output: dict | None
    tool_calls: int
    turns: int
    latency_ms: int
    error: str | None
    error_class: str | None


async def run_query_collect(*, agent, options, prompt):
    """Run sdk_query, collect TextBlocks + structured_output, emit log line.

    `prompt` may be a string or an AsyncIterable per the SDK contract.
    """
    start = time.monotonic()
    text_parts = []
    tool_calls = 0
    turns = 0
    structured_output = None
    error_msg = None
    error_class = None

    try:
        async for message in sdk_query(prompt=prompt, options=options):
            if isinstance(message, AssistantMessage):
                turns += 1
                if getattr(message, "error", None):
                    error_msg = f"Assistant error: {message.error}"
                    error_class = "assistant_error"
                    break
                for block in message.content:
                    if isinstance(block, TextBlock):
                        text_parts.append(block.text)
                    elif isinstance(block, ToolUseBlock):
                        tool_calls += 1
            elif isinstance(message, ResultMessage):
                if getattr(message, "is_error", False):
                    error_msg = message.result or "Query failed"
                    error_class = "result_error"
                if getattr(message, "structured_output", None) is not None:
                    structured_output = message.structured_output
    except Exception as e:
        error_msg = f"SDK exception: {e}"
        error_class = e.__class__.__name__

    latency_ms = int((time.monotonic() - start) * 1000)
    emit_agent_run(
        agent=agent,
        model=getattr(options, "model", None),
        turns=turns,
        tools_called=tool_calls,
        latency_ms=latency_ms,
        status="error" if error_msg else "ok",
        error_class=error_class,
    )

    return SdkRunResult(
        text="".join(text_parts),
        structured_output=structured_output,
        tool_calls=tool_calls,
        turns=turns,
        latency_ms=latency_ms,
        error=error_msg,
        error_class=error_class,
    )
