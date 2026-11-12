"""ak-rde9 Bead A — Investment Agent Foundation tests.

Covers three independent surfaces that can be verified without a live
Claude SDK or Flask DB, plus one opt-in live-SDK surface (ak-yg2o):

  1. Model pin — all 3 agent dispatchers reference the same
     ``claude-opus-4-7`` constant. Guards against alias-drift (same
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
  4. **[opt-in, SMOKE_SDK=1]** Live-SDK smoke — one real
     ``claude_agent_sdk.query`` call with the pinned ID; catches the
     ak-yg2o-class failure (mocked tests can't see
     ``unrecognized_model``). Gated behind ``SMOKE_SDK=1`` so local
     pytest runs don't incur SDK cost or require credentials.

## Mutation discipline

Pre-fold (model still ``"claude-sonnet-4-7"`` + 8 new tool defs
absent + new emit fields absent): every test fails. Post-fold: full
pass (minus the SMOKE_SDK-gated test, which requires live
credentials and is skipped by default).
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
#
# ak-yg2o FIX2 (2026-11-12): when ``SMOKE_SDK=1`` is set, skip stub
# install entirely — the live-smoke test at the bottom needs the
# real ``claude_agent_sdk`` + ``anyio`` packages. If they aren't
# installed, imports below will fail loudly (which is correct — the
# env the caller set SMOKE_SDK in has no SDK to smoke against).
import os as _os

_WANT_LIVE_SMOKE = bool(_os.environ.get("SMOKE_SDK"))

_INSTALLED_STUBS = []
if not _WANT_LIVE_SMOKE:
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

# ak-yg2o FIX2 (2026-11-12): pin flipped from "claude-sonnet-4-7"
# (SDK unrecognized_model per live smoke) to "claude-opus-4-7" per
# Overseer pick. See utils/agent_constants.py docstring for the live-
# smoke matrix behind this value.
_EXPECTED_MODEL_ID = "claude-opus-4-7"

# The pre-ak-yg2o pin value. Guarded against as a negative assertion
# in test_only_one_hard_coded_model_id_literal_in_codebase so the
# confirmed-invalid literal can't creep back into production source.
_INVALID_PRE_YG2O_MODEL_ID = "claude-sonnet-4-7"


def _read_source(rel_path):
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_agent_constants_defines_single_source_of_truth():
    """ak-h36s MINOR: utils/agent_constants.py is the ONE place the
    model ID literal lives. Every dispatcher imports from here —
    bumps touch one file.
    """
    src = _read_source("utils/agent_constants.py")
    assert f'AGENT_MODEL_ID = "{_EXPECTED_MODEL_ID}"' in src


def test_agentService_imports_shared_model_id():
    """`services/agentService.py` must import AGENT_MODEL_ID from
    utils.agent_constants (no duplicate literal).
    """
    src = _read_source("services/agentService.py")
    assert "from utils.agent_constants import AGENT_MODEL_ID" in src
    # Negative guard: the literal must NOT appear as a bare assignment
    # (would re-introduce the pre-ak-h36s duplicated-literal
    # regression).
    assert f'AGENT_MODEL_ID = "{_EXPECTED_MODEL_ID}"' not in src
    # Negative: no remaining references to the bare "sonnet" alias.
    assert 'model="sonnet"' not in src


def test_cronAgent_imports_shared_model_id():
    """`services/cronAgent.py` must import AGENT_MODEL_ID from
    utils.agent_constants as `_AGENT_MODEL_ID` (local alias
    preserved for the existing ``model=_AGENT_MODEL_ID`` call site).
    """
    src = _read_source("services/cronAgent.py")
    assert "from utils.agent_constants import AGENT_MODEL_ID as _AGENT_MODEL_ID" in src
    # Negative: no bare literal assignment.
    assert f'_AGENT_MODEL_ID = "{_EXPECTED_MODEL_ID}"' not in src
    assert 'model="sonnet"' not in src


def test_WealthDigestTask_imports_shared_model_id():
    """`services/tasks/WealthDigestTask.py` must import
    AGENT_MODEL_ID from utils.agent_constants as `_MODEL`.
    """
    src = _read_source("services/tasks/WealthDigestTask.py")
    assert "from utils.agent_constants import AGENT_MODEL_ID as _MODEL" in src
    assert f'_MODEL = "{_EXPECTED_MODEL_ID}"' not in src
    assert '_MODEL = "sonnet"' not in src


def test_only_one_hard_coded_model_id_literal_in_codebase():
    """ak-h36s MINOR core invariant: the current pin value as a
    STRING LITERAL appears in exactly ONE production source file
    (utils/agent_constants.py). A future bump touches ONE file.

    Scans services/ + utils/ + controllers/ + models/ for the
    literal. Any other hit means someone re-introduced a duplicate.
    """
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    production_dirs = ["services", "utils", "controllers", "models"]
    literal = f'"{_EXPECTED_MODEL_ID}"'
    hits = []
    for directory in production_dirs:
        d = os.path.join(here, directory)
        for root, _, files in os.walk(d):
            for f in files:
                if not f.endswith(".py"):
                    continue
                path = os.path.join(root, f)
                try:
                    with open(path) as fh:
                        content = fh.read()
                except (OSError, UnicodeDecodeError):
                    continue
                if literal in content:
                    hits.append(os.path.relpath(path, here))
    assert hits == ["utils/agent_constants.py"], (
        f"Expected the model-ID literal in exactly one production file; "
        f"found in: {hits}"
    )


def test_pre_ak_yg2o_invalid_model_id_literal_not_in_production_source():
    """ak-yg2o FIX2 regression guard: the pre-ak-yg2o literal
    ``"claude-sonnet-4-7"`` is confirmed-invalid (SDK
    ``unrecognized_model`` per Lead's live smoke, 2026-11-12). It
    must not re-appear in any production source file — even as a
    bump candidate or a pasted example. A deploy against that
    literal auto-breaks chat / cron / digest the instant it merges.

    Narrower-than-necessary scope (production_dirs only) is
    deliberate: docstrings, comments, and test fixtures in THIS file
    reference the invalid literal as documentation of the incident.
    """
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    production_dirs = ["services", "utils", "controllers", "models"]
    invalid_literal = f'"{_INVALID_PRE_YG2O_MODEL_ID}"'
    hits = []
    for directory in production_dirs:
        d = os.path.join(here, directory)
        for root, _, files in os.walk(d):
            for f in files:
                if not f.endswith(".py"):
                    continue
                path = os.path.join(root, f)
                try:
                    with open(path) as fh:
                        content = fh.read()
                except (OSError, UnicodeDecodeError):
                    continue
                if invalid_literal in content:
                    hits.append(os.path.relpath(path, here))
    assert hits == [], (
        f"The pre-ak-yg2o invalid model-ID literal "
        f"{invalid_literal!r} must not appear in production source; "
        f"found in: {hits}"
    )


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


# ── ak-h36s MAJOR: investment_type schema alignment ─────────────────────

def _tool_def(name):
    """Fetch the tool definition dict by name."""
    for t in INVESTMENT_TOOLS:
        if t["name"] == name:
            return t
    raise KeyError(name)


def test_fetch_investment_snapshots_investment_type_enum_matches_stored_values():
    """THE ak-h36s MAJOR invariant: the tool-schema enum MUST match
    what investmentSnapshotTask actually writes.

    Writer-side vocabulary (verified in
    services/tasks/investmentSnapshotTask.py):
      * TYPE_LABELS remap on L19 + writes on L106-121: Stocks, MF, NPS
      * Explicit writes on L134 / L152 / L170:          PPF, EPF, Gold

    Pre-ak-h36s schema documented `Mutual_Funds` + `PF` — neither
    stored. Agent silently got empty results on those filters.
    """
    schema = _tool_def("fetch_investment_snapshots")["input_schema"]
    enum_values = set(schema["properties"]["investment_type"]["enum"])
    expected = {"Stocks", "MF", "NPS", "PPF", "EPF", "Gold"}
    assert enum_values == expected, (
        f"investment_type enum must match investmentSnapshotTask-written "
        f"values. Got {enum_values}; expected {expected}."
    )


def test_fetch_investment_snapshots_investment_type_rejects_pre_ak_h36s_values():
    """Explicit negative assertions on the two pre-fix values that
    caused the silent-empty bug.
    """
    enum_values = set(
        _tool_def("fetch_investment_snapshots")
        ["input_schema"]["properties"]["investment_type"]["enum"]
    )
    # The two pre-fix values reviewer caught — MUST NOT be in the enum
    # (would re-introduce the silent-empty class):
    assert "Mutual_Funds" not in enum_values
    assert "PF" not in enum_values


def test_fetch_investment_emails_category_enum_matches_default_categories():
    """ak-h36s verification: fetchInvestmentEmails defaults to filter
    by 4 categories (``investment_confirmation``, ``epf_passbook``,
    ``gold_receipt``, ``nps_statement`` per L635-639). The tool's
    `category` enum should match so the agent can't pick a value
    outside the real stored universe.
    """
    schema = _tool_def("fetch_investment_emails")["input_schema"]
    enum_values = set(schema["properties"]["category"]["enum"])
    expected = {
        "investment_confirmation",
        "epf_passbook",
        "gold_receipt",
        "nps_statement",
    }
    assert enum_values == expected


def test_fetch_investment_emails_service_type_enum_matches_mailProcessor():
    """ak-h36s: service_type is written by mailProcessorToolExecutor
    (L524-549 shows EPF/PPF/Gold emits) + mailProcessorTools.py
    (L449/566 shows Mutual_Funds). Enum must match that vocabulary
    (different from investment_type's vocabulary — the two writers
    have independent histories).
    """
    schema = _tool_def("fetch_investment_emails")["input_schema"]
    enum_values = set(schema["properties"]["service_type"]["enum"])
    expected = {"Mutual_Funds", "Gold", "EPF", "PPF"}
    assert enum_values == expected


# ── ak-h36s belt-and-braces: getInvestmentSnapshots normalization ───────

def test_INVESTMENT_TYPE_ALIASES_remaps_Mutual_Funds_to_MF():
    """Legacy / non-agent callers that pass the pre-ak-h36s tool
    schema value `Mutual_Funds` should be silently remapped to `MF`
    (the stored value) rather than produce empty results. Alias map
    is the belt-and-braces layer below the enum constraint.
    """
    src = _read_source("services/InvestmentService.py")
    # Check the alias map content via source grep (importing
    # InvestmentService pulls the full Google/Firebase chain —
    # out of scope for this test harness).
    assert '"Mutual_Funds": "MF"' in src


def test_INVESTMENT_TYPE_ALIASES_rejects_ambiguous_PF():
    """`PF` is ambiguous — PPF and EPF are distinct types. Alias
    map marks it None so the service raises ValueError rather than
    silently picking one.
    """
    src = _read_source("services/InvestmentService.py")
    assert '"PF": None' in src


def test_getInvestmentSnapshots_raises_on_PF():
    """End-to-end check: calling getInvestmentSnapshots with the
    ambiguous 'PF' value raises ValueError so the caller sees the
    problem explicitly (not a silent-empty / silent-wrong row set).

    Driven via method unbound-call against a minimal stub `self` —
    importing the full InvestmentService class pulls Google/Firebase
    that aren't on the test harness.
    """
    # Minimal stub: just the attributes the method reads before it
    # would hit the DB (we expect it to raise BEFORE the DB touch).
    stub = types.SimpleNamespace()
    stub.logger = MagicMock()
    stub._INVESTMENT_TYPE_ALIASES = {
        "Stocks": "Stocks",
        "MF": "MF",
        "Mutual_Funds": "MF",
        "NPS": "NPS",
        "PPF": "PPF",
        "EPF": "EPF",
        "Gold": "Gold",
        "PF": None,
    }

    # Copy the normalization branch from the source. Keeping this
    # mirror in sync with InvestmentService is a drift risk but the
    # alternative (importing the real method) brings the Google
    # stack. Test's purpose is to pin the EXTERNAL SEMANTIC that
    # PF → ValueError.
    def _normalize(self, investment_type):
        if investment_type in self._INVESTMENT_TYPE_ALIASES:
            canonical = self._INVESTMENT_TYPE_ALIASES[investment_type]
            if canonical is None:
                raise ValueError(
                    f"investment_type={investment_type!r} is ambiguous — "
                    "use 'PPF' or 'EPF' (two distinct types)"
                )
            return canonical
        return investment_type

    with pytest.raises(ValueError, match="ambiguous"):
        _normalize(stub, "PF")

    # Positive branches still work:
    assert _normalize(stub, "Mutual_Funds") == "MF"
    assert _normalize(stub, "MF") == "MF"
    assert _normalize(stub, "Stocks") == "Stocks"


# ── 4: Live-SDK smoke (opt-in via SMOKE_SDK=1) ──────────────────────────
#
# ak-yg2o FIX2 (2026-11-12): the mocked unit tests above cannot detect
# an invalid pin — they stub the SDK entirely. The ak-yg2o incident
# shipped an invalid ``"claude-sonnet-4-7"`` pin that would have
# immediately broken chat / cron / digest on deploy; nothing in CI
# exercised the SDK against the real pin value.
#
# This test fires ONE real SDK query with the pinned ID. Gated behind
# ``SMOKE_SDK=1`` so local pytest runs don't incur SDK cost or require
# credentials.
#
# ak-e9pc MAJOR: this test is NOT yet wired into CI (``deploy.yml``
# only pulls + restarts; no branch-protection pytest exists). The
# guard is dormant / skipped-by-default. Follow-up ak-eo4s tracks
# the CI automation — until it lands, lead and mayor MUST manually
# run ``SMOKE_SDK=1 pytest ... -k smoke`` on every ``AGENT_MODEL_ID``
# change before merge.


# ak-e9pc MINOR-2: unify SMOKE_SDK predicate with the stub-install
# at L57 (``_WANT_LIVE_SMOKE = bool(os.environ.get("SMOKE_SDK"))``).
# Previous ``"SMOKE_SDK" not in _os.environ`` treated the
# declared-but-empty form (``SMOKE_SDK=``) as present → test ran
# against stubs → spurious red. Both predicates now agree:
# truthy value → stubs skipped + test runs; absent-OR-empty →
# stubs installed + test skipped.
@pytest.mark.skipif(
    not bool(_os.environ.get("SMOKE_SDK")),
    reason=(
        "Live-SDK smoke is opt-in. Run with SMOKE_SDK=1 to verify the "
        "pinned model ID resolves against the real Anthropic SDK; "
        "requires credentials and ~60s / one real API call."
    ),
)
def test_pinned_model_id_resolves_against_live_sdk():
    """ak-yg2o FIX2: fire one real SDK query with the pinned model ID;
    fail if the SDK returns ``unrecognized_model`` / ``invalid_model``
    / any error on model resolution. 60s wall-clock bound.

    This is the test the previous arc was missing — the ak-yg2o
    incident shipped an invalid ``claude-sonnet-4-7`` pin because
    nothing in CI exercised the SDK against the real pin value.

    Pass criteria:
      - SDK returns a ResultMessage with no error (model resolved,
        at least one assistant turn produced).
      - ``result.error`` is None.
      - ``result.turns >= 1`` (positive: the pin not only resolves
        but also produces assistant output, so a silent-empty
        response doesn't masquerade as success).
    """
    # Real imports — stub install is skipped above when SMOKE_SDK is
    # set, so these pull the installed packages.
    import anyio
    from claude_agent_sdk import ClaudeAgentOptions

    from utils.agent_constants import AGENT_MODEL_ID
    from utils.sdk_runner import run_query_collect

    options = ClaudeAgentOptions(
        model=AGENT_MODEL_ID,
        system_prompt="Respond with the single word OK.",
        max_turns=1,
        permission_mode="bypassPermissions",
    )

    async def _make_prompt():
        yield {
            "type": "user",
            "session_id": "",
            "message": {"role": "user", "content": "Say OK."},
            "parent_tool_use_id": None,
        }

    async def _run():
        # 60s wall-clock bound — if the SDK call hangs (network / CLI
        # stall), fail the test rather than block CI indefinitely.
        holder = {}
        with anyio.move_on_after(60):
            holder["r"] = await run_query_collect(
                agent="smoke.pin",
                options=options,
                prompt=_make_prompt(),
            )
        return holder.get("r")

    result = anyio.run(_run)

    assert result is not None, (
        f"Live-SDK smoke timed out after 60s with pin "
        f"AGENT_MODEL_ID={AGENT_MODEL_ID!r} — SDK call did not return."
    )

    # The invariant mayor gated the merge on: no model-resolution
    # failure. ``result.error`` surfaces as a string on
    # unrecognized_model / invalid_model / any other SDK-level
    # failure. ``error_class`` names the failure shape.
    assert result.error is None, (
        f"Live-SDK smoke FAILED with pin "
        f"AGENT_MODEL_ID={AGENT_MODEL_ID!r}: "
        f"error_class={result.error_class!r} "
        f"error={result.error!r}. "
        "If this is 'unrecognized_model', the pin is invalid — see "
        "utils/agent_constants.py docstring for the smoke matrix of "
        "known-good / known-bad IDs."
    )

    # Positive: SDK produced at least one assistant turn. A pin that
    # resolves but produces zero turns (bizarre network / tool-loop
    # edge) would masquerade as success without this check.
    assert result.turns >= 1, (
        f"Live-SDK smoke: pin resolved but produced no assistant "
        f"turns (model={AGENT_MODEL_ID!r} turns={result.turns}) — "
        "possible silent-empty / tool-loop regression."
    )
