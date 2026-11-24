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

STRICT_MCP_FLAG = "strict-mcp-config"
MCP_PREFIX = "mcp__"


def builtin_tools(allowed_tools) -> list:
    """The CLI built-ins among ``allowed_tools`` (short names, i.e. not
    ``mcp__<server>__<tool>``). These are the ONLY built-ins an agent
    may be offered."""
    return [t for t in (allowed_tools or []) if not str(t).startswith(MCP_PREFIX)]


def restricted_tool_options(allowed_tools, extra_args=None) -> dict:
    """ClaudeAgentOptions kwargs that make ``allowed_tools`` the agent's
    EFFECTIVE tool set:

        ClaudeAgentOptions(..., **restricted_tool_options([...]))

    * ``allowed_tools``: pre-approval (unchanged semantics);
    * ``tools``: exactly the built-ins in that list (often ``[]``);
    * ``extra_args``: caller's extra args + ``--strict-mcp-config``.
    MCP tools come only from the ``mcp_servers=`` the caller passes.
    """
    allowed = list(allowed_tools or [])
    merged = dict(extra_args or {})
    merged[STRICT_MCP_FLAG] = None
    return {
        "allowed_tools": allowed,
        "tools": builtin_tools(allowed),
        "extra_args": merged,
    }
