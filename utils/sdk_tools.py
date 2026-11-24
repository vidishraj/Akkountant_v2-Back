"""ak-m0f6 (2026-11-24): actually RESTRICT the tools a Claude agent can use.

``ClaudeAgentOptions.allowed_tools`` is a PRE-APPROVAL list (CLI
``--allowedTools``), NOT a restriction, and it is emitted only when
non-empty. Without ``tools=``, the CLI offers its whole built-in set
(Bash, Read, Edit, Write, Glob, Grep, ...) plus every MCP server from
CLI-level config. Measured on claude-agent-sdk 0.1.36 / CLI 2.1.42
(synthetic canary file, no personal data): with
permission_mode="bypassPermissions" + allowed_tools=[one MCP tool], the
init tool list had 186 entries and the model used ``Read`` to return the
canary file's contents. ak-vhc2's allowed_tools drift guard therefore
did not restrict anything.

The restricting knobs:
  * ``tools=[...]``      -> ``--tools a,b``: the ONLY built-ins offered;
                            ``tools=[]`` -> ``--tools ""``: none at all.
  * ``--strict-mcp-config`` (via ``extra_args``): ignore MCP servers from
                            CLI/user config, so only the ``mcp_servers=``
                            passed in code are offered.
Measured with both: init tools == exactly the code-declared MCP tools
(+ ``StructuredOutput`` when ``output_format`` is set, + the listed
built-ins), MCP tools stay callable, and the canary read is impossible.

Every ClaudeAgentOptions in the app goes through ``restricted_tool_options``
so the effective tool set is derived from the SAME list that pre-approves
them; the drift guard (test_ak_vhc2_claude_agent_options_allowed_tools.py)
pins it.
"""
from __future__ import annotations

import dataclasses
import inspect

STRICT_MCP_FLAG = "strict-mcp-config"
TOOLS_FLAG = "tools"
MCP_PREFIX = "mcp__"

# ak-1m97 (2026-11-24): prod runs claude-agent-sdk 0.1.11, whose
# ClaudeAgentOptions has NO ``tools`` field, so ``tools=`` raised TypeError
# in every agent (mail classify, cronAgent, chat) and the mail pipeline
# processed nothing. The CLI that SDK spawns (its bundled Claude Code
# 2.0.57, measured by infra) DOES accept ``--tools`` and
# ``--strict-mcp-config``, and 0.1.x maps ``extra_args`` {flag: None} →
# ``--flag`` and {flag: value} → ``--flag value``. So on an SDK without
# the field we pass the SAME CLI flag through extra_args: identical
# containment. As belt and braces, every known built-in that isn't
# explicitly allowed also goes into ``disallowed_tools``
# (``--disallowedTools``) when the SDK has that field.
KNOWN_BUILTIN_TOOLS = (
    "Agent", "Bash", "BashOutput", "Edit", "ExitPlanMode", "Glob", "Grep",
    "KillShell", "KillBash", "LS", "ListMcpResourcesTool", "MultiEdit",
    "NotebookEdit", "NotebookRead", "Read", "ReadMcpResourceTool",
    "SlashCommand", "Skill", "Task", "TodoRead", "TodoWrite", "WebFetch",
    "WebSearch", "Write",
)


def options_fields(options_cls=None) -> set:
    """The keyword fields the installed ClaudeAgentOptions accepts
    (feature detection, never a version string)."""
    if options_cls is None:
        from claude_agent_sdk import ClaudeAgentOptions as options_cls
    try:
        return {f.name for f in dataclasses.fields(options_cls)}
    except TypeError:
        return set(inspect.signature(options_cls).parameters)


def builtin_tools(allowed_tools) -> list:
    """The CLI built-ins among ``allowed_tools`` (short names, i.e. not
    ``mcp__<server>__<tool>``). These are the ONLY built-ins an agent
    may be offered."""
    return [t for t in (allowed_tools or []) if not str(t).startswith(MCP_PREFIX)]


def restricted_tool_options(allowed_tools, extra_args=None, options_cls=None) -> dict:
    """ClaudeAgentOptions kwargs that make ``allowed_tools`` the agent's
    EFFECTIVE tool set:

        ClaudeAgentOptions(..., **restricted_tool_options([...]))

    * ``allowed_tools``: pre-approval (unchanged semantics);
    * the only built-ins offered = exactly the built-ins in that list
      (often none): ``tools=`` when the installed SDK has that field,
      else the identical ``--tools <csv|"">`` CLI flag via extra_args
      (ak-1m97), plus ``disallowed_tools`` = every other known built-in;
    * ``extra_args``: caller's extra args + ``--strict-mcp-config``.
    MCP tools come only from the ``mcp_servers=`` the caller passes.
    """
    allowed = list(allowed_tools or [])
    builtins = builtin_tools(allowed)
    merged = dict(extra_args or {})
    merged[STRICT_MCP_FLAG] = None
    out = {"allowed_tools": allowed, "extra_args": merged}
    fields = options_fields(options_cls)
    if TOOLS_FLAG in fields:
        out["tools"] = builtins
    else:
        merged[TOOLS_FLAG] = ",".join(builtins)
        if "disallowed_tools" in fields:
            out["disallowed_tools"] = [t for t in KNOWN_BUILTIN_TOOLS if t not in builtins]
    return out
