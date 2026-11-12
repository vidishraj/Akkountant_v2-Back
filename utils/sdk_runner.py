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


def _make_sdk_stderr_logger(agent):
    """Return a callback that logs CLI subprocess stderr as `sdk_stderr` lines.

    The bundled Claude Code CLI emits anthropic request_id, resolved model_id,
    HTTP status and warnings on stderr. Without this callback the SDK
    silently discards those lines (types.py:746). Wiring this in is pure
    observability — no behavior change — and unblocks cross-family
    root-cause diagnosis when chat.* and rate.* fail with identical
    `turns=0 tools_called=0 status=ok` shapes (see hq-wisp-l2jfz, ezveu).

    Lines are trimmed to 500 chars to bound journal volume.
    """
    def cb(line):
        try:
            _logger.info(f"sdk_stderr agent={agent} line={line[:500]}")
        except Exception:
            # Never let observability take down the call site.
            pass
    return cb


def emit_agent_run(*, agent, model, turns, tools_called, latency_ms,
                   status, error_class=None,
                   user_id=None, tool_names=None,
                   input_tokens=None, output_tokens=None):
    """Emit one structured `agent_run` log line.

    Format (space-separated key=value, grep-friendly):
      agent_run agent=X model=Y turns=N tools_called=N latency_ms=N status=S
        [error_class=C] [user=U] [tool_names=comma,sep,list]
        [input_tokens=N output_tokens=N]

    ak-rde9 Bead A (2026-11-12) extensions — all new fields optional
    and backwards-compatible with pre-ak-rde9 callers:

    * ``user_id``: Firebase user id. Emitted as ``user=<id>`` so
      per-user agent activity is grep-able across a shared log file.
    * ``tool_names``: ordered list of tool names invoked this turn.
      Emitted as ``tool_names=name1,name2,...`` so an operator can
      see WHICH tools were called without scraping the per-tool
      ``tool_exec`` events separately. Commas are ascii-safe in log
      parsing; no tool name contains a comma.
    * ``input_tokens`` / ``output_tokens``: usage counts from the
      SDK ResultMessage (when exposed). Emitted as
      ``input_tokens=N output_tokens=N``. Absent fields are skipped
      (not emitted as ``None``) so downstream parsers don't see
      stray key=None values.
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
    if user_id is not None:
        parts.append(f"user={user_id}")
    if tool_names:
        parts.append("tool_names=" + ",".join(tool_names))
    if input_tokens is not None:
        parts.append(f"input_tokens={input_tokens}")
    if output_tokens is not None:
        parts.append(f"output_tokens={output_tokens}")
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


async def run_query_collect(*, agent, options, prompt, user_id=None):
    """Run sdk_query, collect TextBlocks + structured_output, emit log line.

    `prompt` may be a string or an AsyncIterable per the SDK contract.

    ak-rde9 Bead A (2026-11-12): now also accumulates tool_names (list)
    and input_tokens / output_tokens (from ResultMessage.usage when
    exposed by the SDK) and threads them into the ``agent_run`` log
    line for per-turn observability. ``user_id`` is a new optional
    kwarg the caller can pass to associate the turn with a Firebase
    user id in logs.
    """
    start = time.monotonic()
    text_parts = []
    tool_calls = 0
    tool_names = []
    turns = 0
    structured_output = None
    error_msg = None
    error_class = None
    input_tokens = None
    output_tokens = None

    # Auto-wire stderr capture if the caller didn't supply one. This covers
    # every run_query_collect call site (rate.*, mailProcessor extract paths,
    # any future single-shot SDK use) without requiring per-site changes.
    # Skip if the caller already set a callback (don't clobber custom routing).
    if getattr(options, "stderr", None) is None:
        options.stderr = _make_sdk_stderr_logger(agent)

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
                        # ak-rde9: record the tool name for the
                        # observability log. Guard against unexpected
                        # block shapes.
                        name = getattr(block, "name", None)
                        if name:
                            tool_names.append(str(name))
            elif isinstance(message, ResultMessage):
                if getattr(message, "is_error", False):
                    error_msg = message.result or "Query failed"
                    error_class = "result_error"
                if getattr(message, "structured_output", None) is not None:
                    structured_output = message.structured_output
                # ak-rde9: capture token usage when SDK exposes it on
                # the ResultMessage. Shape per claude_agent_sdk varies
                # by version — defensively walk likely attribute paths.
                usage = getattr(message, "usage", None)
                if usage is not None:
                    input_tokens = (
                        getattr(usage, "input_tokens", None)
                        if not isinstance(usage, dict)
                        else usage.get("input_tokens")
                    )
                    output_tokens = (
                        getattr(usage, "output_tokens", None)
                        if not isinstance(usage, dict)
                        else usage.get("output_tokens")
                    )
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
        user_id=user_id,
        tool_names=tool_names,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
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
