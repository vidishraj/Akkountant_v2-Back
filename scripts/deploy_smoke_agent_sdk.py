#!/usr/bin/env python3
"""ak-ck9l: deploy-time smoke for the agent SDK, run in the PROD venv.

Fails (exit 1) if the installed claude-agent-sdk can't build a restricted
ClaudeAgentOptions through utils.sdk_tools.restricted_tool_options, or if the
resulting CLI argv doesn't actually restrict tools. Catches the ak-1m97 class
(code assumes an SDK option the prod venv lacks → every agent TypeErrors).

Deliberately does NOT import app.py (that runs DB DDL + starts schedulers) and
does NOT spawn the claude CLI: _build_command() only renders argv.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

FORBIDDEN_BUILTINS = {"Bash", "Read", "Write", "Edit", "MultiEdit", "Glob", "Grep",
                      "NotebookEdit", "LS", "Task", "WebFetch", "WebSearch"}
# (allowed list as call sites pass it, built-ins that must remain offered)
CASES = [
    ([], set()),                                                    # mail.classify / pdf.text
    (["mcp__mail_processor_tools__report_result"], set()),          # mail agents (MCP only)
    (["WebSearch", "WebFetch", "mcp__agent_tools__x"], {"WebSearch", "WebFetch"}),  # research
]


_BARE = object()  # flag present with no value


def _flag_value(cmd, flag):
    """Value after ``flag``; None if absent; _BARE if it has no value (a bare
    ``--tools`` must not be mistaken for ``--tools ""``)."""
    if flag not in cmd:
        return None
    i = cmd.index(flag)
    if i + 1 >= len(cmd) or str(cmd[i + 1]).startswith("--"):
        return _BARE
    return cmd[i + 1]


def main() -> int:
    import claude_agent_sdk as sdk
    from claude_agent_sdk import ClaudeAgentOptions
    from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport
    from utils.sdk_tools import restricted_tool_options

    print(f"[smoke] claude-agent-sdk {getattr(sdk, '__version__', '?')}")
    failures = []
    for allowed, expect_builtins in CASES:
        try:
            opts = ClaudeAgentOptions(**restricted_tool_options(allowed))
            cmd = SubprocessCLITransport(prompt="smoke", options=opts)._build_command()
        except Exception as exc:  # the ak-1m97 TypeError lands here
            failures.append(f"{allowed!r}: {type(exc).__name__}: {exc}")
            continue
        tools = _flag_value(cmd, "--tools")
        offered = {t for t in (tools if isinstance(tools, str) else "").split(",") if t}
        if tools is None:
            failures.append(f"{allowed!r}: argv has no --tools (built-ins unrestricted)")
        elif tools is _BARE:
            failures.append(f"{allowed!r}: argv has a bare --tools with no value")
        elif offered != expect_builtins:
            failures.append(f"{allowed!r}: --tools offers {sorted(offered)}, expected {sorted(expect_builtins)}")
        if (offered - expect_builtins) & FORBIDDEN_BUILTINS:
            failures.append(f"{allowed!r}: forbidden built-ins offered: {sorted(offered & FORBIDDEN_BUILTINS)}")
        if "--strict-mcp-config" not in cmd:
            failures.append(f"{allowed!r}: argv lacks --strict-mcp-config")
        print(f"[smoke] {allowed!r}: --tools={tools!r} strict={'--strict-mcp-config' in cmd}")
    if failures:
        print("[smoke] FAIL:\n  " + "\n  ".join(failures))
        return 1
    print("[smoke] OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
