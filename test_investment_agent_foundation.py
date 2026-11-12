"""ak-rde9 Bead A — Investment Agent Foundation tests.

Covers three independent surfaces that can be verified without a live
Claude SDK or Flask DB:

  1. Model pin — all 3 agent dispatchers reference the same
     ``claude-sonnet-4-7`` constant. Guards against alias-drift (same
     incident class as ak-znk gold unit-drift) and against future
     divergence where one dispatcher's pin gets bumped without the
     others.
  2. Tool registration + dispatch parity — each of the 8 new tools
     has a definition in ``INVESTMENT_TOOLS`` and a dispatch branch
     in ``_execute_investment_tool``. Missing either half yields a
     runtime "Unknown investment tool" response.
  3. ``emit_agent_run`` observability — richer fields (user_id,
     tool_names, input_tokens, output_tokens) land in the emitted
     log line when provided; absent when not (backwards-compat).

## Mutation discipline

Pre-fold (model still ``"sonnet"`` + 8 new tool defs absent + new
emit fields absent): every test fails. Post-fold: full pass.
"""
import logging
import re
import sys
import types
from unittest.mock import MagicMock

import pytest

# claude_agent_sdk + dependent transitive imports aren't installed in
# CI. Stub-module-only approach is sufficient here — the tests exercise
# source-text (model pins, dispatch branch presence) and a single pure
# function (emit_agent_run), neither of which calls into the SDK.
#
# Track which stubs WE installed so the module-scope teardown below
# can withdraw them. Leaving stubs in sys.modules leaks into every
# later test file in the same pytest session — breaking tests that
# legitimately skip when claude_agent_sdk is absent (e.g.
# test_agent_attachments' agentService-import-skip pattern).
_INSTALLED_STUBS = []
for _pkg in (
    "claude_agent_sdk",
    "anyio",
):
    if _pkg not in sys.modules:
        _m = types.ModuleType(_pkg)
        _m.__path__ = []
        _m.__getattr__ = lambda name, pkg=_pkg: MagicMock(name=f"{pkg}.{name}")
        sys.modules[_pkg] = _m
        _INSTALLED_STUBS.append(_pkg)

from services.agent_tools import INVESTMENT_TOOLS  # noqa: E402
from utils.sdk_runner import emit_agent_run  # noqa: E402

# Withdraw ONLY the stub modules themselves BEFORE pytest runs any
# other test file. Leaving them in sys.modules leaks into later
# suites — breaks tests that legitimately skip when claude_agent_sdk
# is absent (test_agent_attachments pattern).
#
# Keep the project modules (utils.sdk_runner, services.agent_tools)
# in sys.modules — they already have their own (stub-captured)
# claude_agent_sdk reference bound in their module namespace, so
# dropping the stub from sys.modules doesn't disturb them. This also
# keeps caplog assertions below working against the SAME _logger
# instance that emit_agent_run writes to.
for _pkg in _INSTALLED_STUBS:
    sys.modules.pop(_pkg, None)


# ── 1: Model pin convergence ─────────────────────────────────────────────

_EXPECTED_MODEL_ID = "claude-sonnet-4-7"


def _read_source(rel_path):
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_agentService_defines_pinned_model_constant():
    """`services/agentService.py` must define ``AGENT_MODEL_ID`` as the
    pinned claude-sonnet-4-7 string. This is the canonical constant the
    other dispatchers should match.
    """
    src = _read_source("services/agentService.py")
    assert f'AGENT_MODEL_ID = "{_EXPECTED_MODEL_ID}"' in src


def test_cronAgent_pins_same_model_id():
    """`services/cronAgent.py` must pin the SAME model string (not
    reuse the ``"sonnet"`` alias). Guards against alias-drift.
    """
    src = _read_source("services/cronAgent.py")
    assert f'_AGENT_MODEL_ID = "{_EXPECTED_MODEL_ID}"' in src
    # Negative: no remaining references to the bare alias in a `model=`
    # keyword-argument context.
    assert 'model="sonnet"' not in src


def test_WealthDigestTask_pins_same_model_id():
    """`services/tasks/WealthDigestTask.py` must pin the same model
    string via ``_MODEL``.
    """
    src = _read_source("services/tasks/WealthDigestTask.py")
    assert f'_MODEL = "{_EXPECTED_MODEL_ID}"' in src
    assert '_MODEL = "sonnet"' not in src


# ── 2: Tool registration + dispatch parity ───────────────────────────────

_NEW_TOOL_NAMES = {
    "fetch_realized_pnl",
    "fetch_fo_summary",
    "fetch_fo_trades",
    "fetch_investment_snapshots",
    "fetch_investment_emails",
    "fetch_email_body",
    "get_kite_login_url",
    "get_kite_token_status",
}


@pytest.mark.parametrize("tool_name", sorted(_NEW_TOOL_NAMES))
def test_new_tool_registered_in_INVESTMENT_TOOLS(tool_name):
    """Every bead-named tool must have an entry in INVESTMENT_TOOLS
    with the required fields (name, description, input_schema).
    """
    by_name = {t["name"]: t for t in INVESTMENT_TOOLS}
    assert tool_name in by_name, (
        f"{tool_name} missing from INVESTMENT_TOOLS — agent cannot invoke "
        f"a tool that isn't registered on the MCP server"
    )
    tool_def = by_name[tool_name]
    assert "description" in tool_def and tool_def["description"].strip()
    assert "input_schema" in tool_def
    assert tool_def["input_schema"].get("type") == "object"


@pytest.mark.parametrize("tool_name", sorted(_NEW_TOOL_NAMES))
def test_new_tool_dispatched_in_executor(tool_name):
    """Every bead-named tool must have a dispatch branch in
    ``_execute_investment_tool`` (grep the source). Missing branch →
    executor falls through to "Unknown investment tool" even though
    the tool is registered → silent breakage.

    Source-level grep is sufficient here: the dispatch is a grep-
    auditable ``elif tool_name == "<name>":`` string, and importing
    the executor to call it directly would pull the full
    InvestmentService chain (Google OAuth + Firebase + statement
    parsers, none of which belong in a dispatch-parity test).
    """
    src = _read_source("services/agent_tool_executor.py")
    pattern = re.compile(
        rf'elif\s+tool_name\s*==\s*["\']{re.escape(tool_name)}["\']\s*:'
    )
    assert pattern.search(src), (
        f"{tool_name} has no dispatch branch in _execute_investment_tool"
    )


def test_INVESTMENT_TOOLS_tool_names_are_unique():
    """Regression guard against a copy-paste that accidentally
    registers two tools with the same name — the second silently
    shadows the first on the MCP server.
    """
    names = [t["name"] for t in INVESTMENT_TOOLS]
    assert len(names) == len(set(names)), (
        f"Duplicate tool names in INVESTMENT_TOOLS: "
        f"{[n for n in names if names.count(n) > 1]}"
    )


# ── 3: emit_agent_run observability extensions ──────────────────────────

def _emit_and_capture(caplog, **kwargs):
    """Call emit_agent_run and return the single log line it emitted."""
    import utils.sdk_runner as _sdk_runner
    _sdk_runner._logger.setLevel(logging.INFO)
    caplog.set_level(logging.INFO, logger=_sdk_runner._logger.name)
    caplog.clear()
    emit_agent_run(**kwargs)
    records = [r for r in caplog.records
               if r.getMessage().startswith("agent_run")]
    assert len(records) == 1, (
        f"expected exactly 1 agent_run log line, got {len(records)}: "
        f"{[r.getMessage() for r in records]}"
    )
    return records[0].getMessage()


def test_emit_agent_run_minimal_backwards_compat(caplog):
    """Pre-ak-rde9 callers (no user_id / tool_names / tokens) must
    still produce a parseable log line with the base fields only.
    New-field keys are absent (not emitted as ``None``) so downstream
    log parsers aren't surprised by stray ``key=None`` sections.
    """
    line = _emit_and_capture(
        caplog,
        agent="chat.investment", model="claude-sonnet-4-7",
        turns=3, tools_called=2, latency_ms=1234, status="ok",
    )
    assert "agent=chat.investment" in line
    assert "model=claude-sonnet-4-7" in line
    assert "turns=3" in line
    assert "tools_called=2" in line
    assert "latency_ms=1234" in line
    assert "status=ok" in line
    # New ak-rde9 fields absent when not provided
    assert "user=" not in line
    assert "tool_names=" not in line
    assert "input_tokens=" not in line
    assert "output_tokens=" not in line


def test_emit_agent_run_emits_user_id_when_provided(caplog):
    line = _emit_and_capture(
        caplog,
        agent="chat.investment", model=_EXPECTED_MODEL_ID,
        turns=1, tools_called=0, latency_ms=50, status="ok",
        user_id="firebase-abc-123",
    )
    assert "user=firebase-abc-123" in line


def test_emit_agent_run_emits_tool_names_as_comma_list(caplog):
    """tool_names list rendered as ``tool_names=a,b,c`` — grep-friendly
    + ordered (list order preserved) so operators can see the call
    sequence per turn.
    """
    line = _emit_and_capture(
        caplog,
        agent="chat.investment", model=_EXPECTED_MODEL_ID,
        turns=2, tools_called=3, latency_ms=800, status="ok",
        tool_names=["fetch_portfolio_summary", "fetch_user_securities",
                    "fetch_realized_pnl"],
    )
    assert ("tool_names=fetch_portfolio_summary,"
            "fetch_user_securities,fetch_realized_pnl") in line


def test_emit_agent_run_omits_tool_names_when_empty(caplog):
    """Empty list is falsy — must NOT emit ``tool_names=``. Prevents a
    stray empty key in logs.
    """
    line = _emit_and_capture(
        caplog,
        agent="cron", model=_EXPECTED_MODEL_ID,
        turns=1, tools_called=0, latency_ms=100, status="ok",
        tool_names=[],
    )
    assert "tool_names=" not in line


def test_emit_agent_run_emits_token_counts_when_provided(caplog):
    line = _emit_and_capture(
        caplog,
        agent="wealth_digest", model=_EXPECTED_MODEL_ID,
        turns=1, tools_called=0, latency_ms=5000, status="ok",
        input_tokens=1234, output_tokens=567,
    )
    assert "input_tokens=1234" in line
    assert "output_tokens=567" in line


def test_emit_agent_run_omits_token_counts_when_none(caplog):
    """Partial token info (SDK versions that don't expose usage) must
    not emit ``input_tokens=None`` — just omit.
    """
    line = _emit_and_capture(
        caplog,
        agent="chat.investment", model=_EXPECTED_MODEL_ID,
        turns=1, tools_called=0, latency_ms=100, status="ok",
        input_tokens=None, output_tokens=None,
    )
    assert "input_tokens=" not in line
    assert "output_tokens=" not in line


def test_emit_agent_run_preserves_error_class_ordering(caplog):
    """Regression guard: error_class must still be emitted alongside
    status when the turn errored out.
    """
    line = _emit_and_capture(
        caplog,
        agent="chat.investment", model=_EXPECTED_MODEL_ID,
        turns=0, tools_called=0, latency_ms=50, status="error",
        error_class="SDKTimeout", user_id="u",
    )
    assert "status=error" in line
    assert "error_class=SDKTimeout" in line
    assert "user=u" in line
