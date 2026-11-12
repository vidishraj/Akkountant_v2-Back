"""ak-6k2m Bead B — Investment Agent Web Intelligence tests.

Covers five independent surfaces introduced by the web-intelligence
core, none of which require a live SDK / live HTTP / Flask DB:

  1. ``compute_allowed_tools`` adds ``WebSearch`` + ``WebFetch`` to
     the investment agent's toolbox, but NOT to transaction or
     freelance agents.
  2. All 5 domain wrappers are registered in ``INVESTMENT_TOOLS``
     with the expected names + required-arg schemas.
  3. Each wrapper's executor branch produces a well-shaped search
     plan (tool tag, canonical query / URLs / plans, extraction
     hints, instructions string). Validation failures raise
     ``ToolValidationError`` with a descriptive message.
  4. The ``_build_sdk_tools`` handler closure gates
     ``insert_investment`` behind a confirm SSE event when
     ``web_fetch_state["used"]`` is True and the tool isn't in
     ``confirmed_tools`` — and lets it through when either
     condition isn't met.
  5. The SDK-built-in ``WebSearch`` / ``WebFetch`` names are the
     canonical identifiers the stream_chat main-loop observer
     watches for — same literals end-to-end.

## Mutation discipline

Pre-fold (no WebSearch/WebFetch in allowed_tools + 5 wrappers
missing from INVESTMENT_TOOLS + no web-gate on insert): every test
fails. Post-fold: full pass.
"""
import sys
import types
from unittest.mock import MagicMock

import pytest


# Same stub-install pattern as test_investment_agent_foundation —
# claude_agent_sdk + anyio aren't installed in CI; stubs let imports
# resolve. Withdrawn after imports so later test files don't see them.
#
# NOTE: we deliberately DO NOT import services/agentService at test
# collection time — it transitively pulls utils.GoogleServiceSingleton
# which imports google.auth / google.oauth2 / googleapiclient, none of
# which stub cleanly through MagicMock __getattr__ (the import system
# needs real ModuleType submodule hierarchies, and
# google.auth.exceptions.RefreshError is pulled by an explicit
# `from ... import` that bypasses the stub's __getattr__).
#
# Instead we grep agentService source text for the invariants we care
# about (same pattern as test_wealth_digest's _source_code_only). The
# executor tests go through real imports because
# services.agent_tool_executor doesn't pull the Google chain.
_INSTALLED_STUBS = []
for _pkg in ("claude_agent_sdk", "anyio"):
    if _pkg not in sys.modules:
        _m = types.ModuleType(_pkg)
        _m.__path__ = []
        _m.__getattr__ = lambda name, pkg=_pkg: MagicMock(name=f"{pkg}.{name}")
        sys.modules[_pkg] = _m
        _INSTALLED_STUBS.append(_pkg)

from services.agent_tools import INVESTMENT_TOOLS  # noqa: E402
from services.agent_tool_executor import (  # noqa: E402
    ToolValidationError,
    _execute_fetch_stock_news,
    _execute_fetch_earnings_calendar,
    _execute_fetch_company_announcements,
    _execute_fetch_macro_context,
    _execute_fetch_fund_factsheet,
    _FETCH_STOCK_NEWS_MAX_DAYS_BACK,
    _FETCH_STOCK_NEWS_DEFAULT_DAYS_BACK,
)

# Withdraw ONLY the stubs we installed (keep project modules), so
# later test files don't pick up stale stubs. Mirrors
# test_investment_agent_foundation's pattern.
for _pkg in _INSTALLED_STUBS:
    sys.modules.pop(_pkg, None)


def _read_source(rel_path):
    """Read a project source file as text — used by the
    agentService-side tests that would otherwise pull the Google
    auth chain on import."""
    import os
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# Expected invariants extracted from the agentService source. These
# mirror the module-scope constants we can't import directly.
WEB_WRAPPER_TOOLS = frozenset({
    "fetch_stock_news",
    "fetch_earnings_calendar",
    "fetch_company_announcements",
    "fetch_macro_context",
    "fetch_fund_factsheet",
})
_WEB_BUILT_IN_TOOLS = ("WebSearch", "WebFetch")
# Mirrors services.agentService.MCP_SERVER_NAME. Guarded below.
MCP_SERVER_NAME = "agent_tools"


# ── 1: compute_allowed_tools — web built-ins for investment only ────
#
# We exec() a self-contained copy of compute_allowed_tools pulled
# from agentService source. Importing the function directly would
# pull the Google auth chain on import — see the module-scope note.
# The source-grep below guards that the exec'd copy matches the
# production body.


def _load_compute_allowed_tools():
    """Pull compute_allowed_tools + its _WEB_BUILT_IN_TOOLS constant
    + MCP_SERVER_NAME out of the agentService source and exec them
    into a namespace so the tests can call the REAL function body
    without importing the module (and its Google auth chain)."""
    import ast
    src = _read_source("services/agentService.py")
    tree = ast.parse(src)
    ns = {}
    # Collect: the function def + module-level assignments it depends
    # on (MCP_SERVER_NAME, READ_ATTACHMENT_TOOL_NAME,
    # _WEB_BUILT_IN_TOOLS).
    wanted_names = {
        "MCP_SERVER_NAME",
        "READ_ATTACHMENT_TOOL_NAME",
        "_WEB_BUILT_IN_TOOLS",
    }
    want_func = "compute_allowed_tools"
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in wanted_names:
                    exec(compile(ast.Module([node], []), "<extract>", "exec"), ns)
        elif isinstance(node, ast.FunctionDef) and node.name == want_func:
            exec(compile(ast.Module([node], []), "<extract>", "exec"), ns)
    return ns[want_func], ns


def test_compute_allowed_tools_adds_web_built_ins_for_investment():
    """ak-6k2m: WebSearch + WebFetch must appear in the investment
    agent's allowed_tools regardless of attachment state."""
    compute_allowed_tools, _ = _load_compute_allowed_tools()
    tools = compute_allowed_tools(
        agent_type="investment",
        mcp_tool_names=["mcp__agent_tools__fetch_portfolio_summary"],
        has_attachments=False,
    )
    assert "WebSearch" in tools
    assert "WebFetch" in tools


def test_compute_allowed_tools_adds_web_built_ins_with_attachments():
    """Attachments branch should NOT drop the web built-ins —
    the two features coexist on the investment persona."""
    compute_allowed_tools, ns = _load_compute_allowed_tools()
    tools = compute_allowed_tools(
        agent_type="investment",
        mcp_tool_names=["mcp__agent_tools__fetch_portfolio_summary"],
        has_attachments=True,
    )
    assert "WebSearch" in tools
    assert "WebFetch" in tools
    # The attachment-branch MCP tool is still appended.
    assert (
        f"mcp__{ns['MCP_SERVER_NAME']}__{ns['READ_ATTACHMENT_TOOL_NAME']}"
        in tools
    )


def test_compute_allowed_tools_no_web_for_transaction_agent():
    """ak-6k2m Q5 scope: transaction + freelance personas operate
    on the Overseer's own data via MCP — no web access."""
    compute_allowed_tools, _ = _load_compute_allowed_tools()
    tools = compute_allowed_tools(
        agent_type="transaction",
        mcp_tool_names=["mcp__agent_tools__fetch_transactions"],
        has_attachments=False,
    )
    assert "WebSearch" not in tools
    assert "WebFetch" not in tools


def test_compute_allowed_tools_no_web_for_freelance_agent():
    compute_allowed_tools, _ = _load_compute_allowed_tools()
    tools = compute_allowed_tools(
        agent_type="freelance",
        mcp_tool_names=["mcp__agent_tools__create_invoice"],
        has_attachments=False,
    )
    assert "WebSearch" not in tools
    assert "WebFetch" not in tools


def test_WEB_BUILT_IN_TOOLS_literal_matches_sdk_names():
    """Guards against a rename / typo of the canonical SDK built-in
    names. If this ever drifts, ``stream_chat``'s main-loop observer
    silently stops firing and the web-gate goes dormant."""
    _, ns = _load_compute_allowed_tools()
    assert ns["_WEB_BUILT_IN_TOOLS"] == ("WebSearch", "WebFetch")


def test_MCP_SERVER_NAME_matches_expected_literal():
    """Sanity check — the invariant we mirror in the test-local
    MCP_SERVER_NAME still matches agentService's canonical value."""
    _, ns = _load_compute_allowed_tools()
    assert ns["MCP_SERVER_NAME"] == MCP_SERVER_NAME


# ── 2: 5 wrappers registered in INVESTMENT_TOOLS ────────────────────


_WEB_WRAPPER_NAMES = (
    "fetch_stock_news",
    "fetch_earnings_calendar",
    "fetch_company_announcements",
    "fetch_macro_context",
    "fetch_fund_factsheet",
)


def _tool_def(name):
    return next((t for t in INVESTMENT_TOOLS if t["name"] == name), None)


@pytest.mark.parametrize("name", _WEB_WRAPPER_NAMES)
def test_web_wrapper_registered_in_INVESTMENT_TOOLS(name):
    """Each of the 5 wrappers must appear in INVESTMENT_TOOLS with
    an input_schema — missing either half would surface as
    "Unknown investment tool" at dispatch time."""
    tdef = _tool_def(name)
    assert tdef is not None, f"{name} missing from INVESTMENT_TOOLS"
    assert "description" in tdef and tdef["description"]
    assert "input_schema" in tdef
    assert tdef["input_schema"]["type"] == "object"


def test_fetch_stock_news_requires_symbol():
    schema = _tool_def("fetch_stock_news")["input_schema"]
    assert "symbol" in schema["required"]
    # days_back is optional with a default.
    assert "days_back" in schema["properties"]
    assert schema["properties"]["days_back"].get("default") == (
        _FETCH_STOCK_NEWS_DEFAULT_DAYS_BACK
    )


def test_fetch_company_announcements_requires_symbol():
    schema = _tool_def("fetch_company_announcements")["input_schema"]
    assert "symbol" in schema["required"]


def test_fetch_fund_factsheet_requires_scheme_code():
    schema = _tool_def("fetch_fund_factsheet")["input_schema"]
    assert "scheme_code" in schema["required"]


def test_fetch_earnings_calendar_takes_no_args():
    schema = _tool_def("fetch_earnings_calendar")["input_schema"]
    assert schema["required"] == []


def test_fetch_macro_context_takes_no_args():
    schema = _tool_def("fetch_macro_context")["input_schema"]
    assert schema["required"] == []


def test_WEB_WRAPPER_TOOLS_set_matches_INVESTMENT_TOOLS_registry():
    """The constant in agentService must list exactly the 5
    wrapper names registered as tool defs. Drift between the two
    means the web-gate either misses a tool (silent — insert
    goes through un-gated after a wrapper call) or gates on a
    non-existent tool (dead code / confusion)."""
    src = _read_source("services/agentService.py")
    for name in _WEB_WRAPPER_NAMES:
        assert f'"{name}"' in src, (
            f"{name} missing from agentService source — "
            "WEB_WRAPPER_TOOLS set is drifted"
        )
    # And the test-local mirror matches the registry.
    assert WEB_WRAPPER_TOOLS == set(_WEB_WRAPPER_NAMES)


# ── 3: Wrapper executor shape + validation ──────────────────────────


def test_fetch_stock_news_builds_canonical_plan():
    out = _execute_fetch_stock_news(
        {"symbol": "RELIANCE", "days_back": 5}
    )
    assert out["tool"] == "fetch_stock_news"
    assert out["symbol"] == "RELIANCE"
    assert out["days_back"] == 5
    assert "canonical_query" in out
    assert "RELIANCE" in out["canonical_query"]
    assert "5 days" in out["canonical_query"]
    assert "extraction" in out and isinstance(out["extraction"], list)
    assert "instructions" in out


def test_fetch_stock_news_defaults_days_back():
    out = _execute_fetch_stock_news({"symbol": "INFY"})
    assert out["days_back"] == _FETCH_STOCK_NEWS_DEFAULT_DAYS_BACK


def test_fetch_stock_news_clamps_days_back_to_max():
    out = _execute_fetch_stock_news(
        {"symbol": "INFY", "days_back": 999}
    )
    assert out["days_back"] == _FETCH_STOCK_NEWS_MAX_DAYS_BACK


def test_fetch_stock_news_clamps_days_back_to_min():
    out = _execute_fetch_stock_news(
        {"symbol": "INFY", "days_back": 0}
    )
    assert out["days_back"] == 1


def test_fetch_stock_news_rejects_missing_symbol():
    with pytest.raises(ToolValidationError, match="symbol"):
        _execute_fetch_stock_news({})


def test_fetch_stock_news_rejects_empty_symbol():
    with pytest.raises(ToolValidationError, match="symbol"):
        _execute_fetch_stock_news({"symbol": "   "})


def test_fetch_stock_news_rejects_non_integer_days_back():
    with pytest.raises(ToolValidationError, match="days_back"):
        _execute_fetch_stock_news(
            {"symbol": "INFY", "days_back": "last week"}
        )


def test_fetch_earnings_calendar_scopes_to_held_symbols():
    """Reads fetch_user_securities via service, extracts stock
    codes, builds per-symbol WebSearch plans."""
    service = MagicMock()
    service.fetchUserSecurities.return_value = {
        "data": [
            {"stockCode": "RELIANCE"},
            {"stockCode": "INFY"},
            {"stockCode": "TCS"},
        ]
    }
    out = _execute_fetch_earnings_calendar("user1", service)
    assert out["tool"] == "fetch_earnings_calendar"
    assert set(out["symbols"]) == {"RELIANCE", "INFY", "TCS"}
    assert len(out["plans"]) == 3
    for plan in out["plans"]:
        assert "canonical_query" in plan
        assert "upcoming earnings" in plan["canonical_query"]


def test_fetch_earnings_calendar_falls_back_on_service_error():
    """A DB / service failure must not crash the turn — the wrapper
    returns a structured error with a free-form fallback the LLM can
    read + act on."""
    service = MagicMock()
    service.fetchUserSecurities.side_effect = RuntimeError("DB down")
    out = _execute_fetch_earnings_calendar("user1", service)
    assert out["tool"] == "fetch_earnings_calendar"
    assert out["error_class"] == "holdings_fetch_failed"
    assert "DB down" in out["message"]
    assert "WebSearch" in out["fallback"]


def test_fetch_earnings_calendar_deduplicates_symbols():
    """A user holding the same stock across accounts shouldn't
    produce duplicate plans — dedupe by first-seen order."""
    service = MagicMock()
    service.fetchUserSecurities.return_value = {
        "data": [
            {"stockCode": "RELIANCE"},
            {"stockCode": "RELIANCE"},  # dup
            {"stockCode": "INFY"},
        ]
    }
    out = _execute_fetch_earnings_calendar("user1", service)
    assert out["symbols"] == ["RELIANCE", "INFY"]


def test_fetch_earnings_calendar_empty_holdings():
    service = MagicMock()
    service.fetchUserSecurities.return_value = {"data": []}
    out = _execute_fetch_earnings_calendar("user1", service)
    assert out["symbols"] == []
    assert out["plans"] == []


def test_fetch_company_announcements_builds_hybrid_nse_url_bse_query():
    """ak-pb4n MINOR-2: NSE keys on ticker-symbol so we emit a
    canonical WebFetch URL. BSE keys on numeric scrip code, which
    we can't resolve server-side, so BSE routes through a
    WebSearch query instead (search engine resolves to the correct
    scrip-coded page at search time)."""
    out = _execute_fetch_company_announcements({"symbol": "reliance"})
    assert out["tool"] == "fetch_company_announcements"
    assert out["symbol"] == "RELIANCE"

    # NSE: WebFetch URL(s), symbol-keyed.
    assert "nse_urls" in out
    assert isinstance(out["nse_urls"], list) and out["nse_urls"]
    for u in out["nse_urls"]:
        assert u["exchange"] == "NSE"
        assert "RELIANCE" in u["url"]
        # Canonical NSE announcements page.
        assert "nseindia.com" in u["url"]
        assert "symbol=RELIANCE" in u["url"]

    # BSE: WebSearch query/queries, NOT a ticker-keyed URL (which
    # would 404 — BSE requires numeric scrip code we don't carry).
    assert "bse_queries" in out
    assert isinstance(out["bse_queries"], list) and out["bse_queries"]
    for q in out["bse_queries"]:
        assert q["exchange"] == "BSE"
        assert "canonical_query" in q
        assert "RELIANCE" in q["canonical_query"]
        assert "BSE" in q["canonical_query"]

    assert isinstance(out["extraction"], list)


def test_fetch_company_announcements_bse_is_not_broken_ticker_url():
    """ak-pb4n MINOR-2 regression guard: the pre-fix BSE URL form
    ``bseindia.com/.../ann.html?scrip=<TICKER>`` is broken because
    BSE requires a numeric scrip code, not the trading ticker.
    The wrapper must NOT emit that URL under any field."""
    out = _execute_fetch_company_announcements({"symbol": "RELIANCE"})
    serialized = str(out)
    # The broken form: ticker as scrip query param on bseindia.com.
    assert "ann.html?scrip=RELIANCE" not in serialized
    # And nothing shaped like "bse...<ticker-form>" should exist.
    # (Legitimate BSE references are inside canonical_query strings,
    # which are human-readable — they contain 'BSE' but not a URL.)
    for q in out.get("bse_queries", []):
        assert "http" not in q.get("canonical_query", "")


def test_fetch_company_announcements_rejects_missing_symbol():
    with pytest.raises(ToolValidationError, match="symbol"):
        _execute_fetch_company_announcements({})


def test_fetch_company_announcements_system_prompt_summary_matches_return_shape():
    """ak-7izj: system-prompt wrapper summary (L107 area) must
    mention the ACTUAL return keys — nse_urls + bse_queries — not
    the pre-ak-pb4n `{urls, extraction}`. Doc drift from the BSE
    WebSearch switch would make the summary lie to the agent at
    tool-selection time."""
    src = _read_source("services/agent_tools.py")
    # New shape must be named verbatim in the summary line.
    assert "{nse_urls, bse_queries, extraction}" in src, (
        "Web Intelligence wrapper summary for "
        "fetch_company_announcements must reference "
        "{nse_urls, bse_queries, extraction} — ak-7izj doc fix "
        "missing or regressed"
    )
    # Old shape must be gone — that key doesn't exist in the return
    # anymore and claiming it does misleads the agent at call time.
    # Scope to the wrapper-summary bullet so a hit elsewhere (docs
    # block citing history) wouldn't false-positive; the bullet
    # starts with the backticked tool name.
    import re
    bullet_m = re.search(
        r"- `fetch_company_announcements\(symbol\)`[^\n]*(?:\n[^\n]*)*?"
        r"(?=\n\s*- |\n\n)",
        src,
    )
    assert bullet_m is not None, "could not locate wrapper summary bullet"
    assert "{urls, extraction}" not in bullet_m.group(0), (
        "pre-ak-7izj `{urls, extraction}` return-shape still present "
        "in the system-prompt wrapper summary — doc drift back to "
        "the broken pre-ak-pb4n form"
    )


def test_fetch_company_announcements_tool_def_description_reflects_hybrid_shape():
    """ak-7izj: tool-def description must describe the hybrid
    NSE-WebFetch / BSE-WebSearch plan — not the pre-ak-pb4n "WebFetch
    the URLs" phrasing which told the agent to WebFetch a URL that
    would 404 on BSE. Grep scoped to the tool def block."""
    import re
    src = _read_source("services/agent_tools.py")
    # Grab the tool-def dict for fetch_company_announcements.
    m = re.search(
        r'"name":\s*"fetch_company_announcements".*?"required":\s*\[\s*"symbol"\s*\]',
        src, re.DOTALL,
    )
    assert m is not None, (
        "could not locate fetch_company_announcements tool def"
    )
    block = m.group(0)
    # Distinguishing phrasing from the ak-7izj rewrite.
    assert "NSE as a WebFetch URL" in block, (
        "tool description must name 'NSE as a WebFetch URL' — "
        "ak-7izj rewrite missing"
    )
    assert "BSE as a WebSearch query" in block, (
        "tool description must name 'BSE as a WebSearch query' — "
        "ak-7izj rewrite missing"
    )
    # Negative: the pre-ak-7izj symbol-input description that told
    # the agent BSE is a URL lookup must be gone.
    assert "shape the BSE / NSE lookup URL" not in block, (
        "pre-ak-7izj input-schema description that implied BSE is "
        "a URL lookup is still present — ak-7izj doc drift"
    )


def test_fetch_macro_context_returns_three_plans():
    out = _execute_fetch_macro_context()
    assert out["tool"] == "fetch_macro_context"
    indicators = {p["indicator"] for p in out["plans"]}
    assert indicators == {
        "rbi_repo_rate", "india_cpi_inflation", "fomc_upcoming",
    }
    for plan in out["plans"]:
        assert "canonical_query" in plan
        assert "sources" in plan
        assert isinstance(plan["sources"], list)


def test_fetch_fund_factsheet_builds_canonical_plan():
    out = _execute_fetch_fund_factsheet({"scheme_code": "120503"})
    assert out["tool"] == "fetch_fund_factsheet"
    assert out["scheme_code"] == "120503"
    assert any("120503" in q for q in out["queries"])
    assert "sources" in out and isinstance(out["sources"], list)
    assert "extraction" in out


def test_fetch_fund_factsheet_rejects_missing_scheme_code():
    with pytest.raises(ToolValidationError, match="scheme_code"):
        _execute_fetch_fund_factsheet({})


# ── 4: Confirm-gate on insert_investment ────────────────────────────
#
# The gate's logic lives inside the handler closure created by
# ``_build_sdk_tools``. Testing the real closure requires wiring up
# services / Flask / SDK stubs — overkill for the rule we want to
# pin. We instead extract the IDENTICAL decision expression into a
# local reproduction + exercise its three branches. Belt-and-braces
# source grep guards that the check in the production file still
# matches the same literal tuple.


def _web_gate_should_fire(tool_name, web_used, confirmed_tools):
    """Mirror of the branch in services/agentService.py
    _build_sdk_tools handler. Keep in sync with the source — a drift
    test below guards that the production file still carries the
    same four conditions."""
    return (
        tool_name == "insert_investment"
        and web_used
        and tool_name not in confirmed_tools
    )


def test_web_gate_fires_on_insert_after_web_lookup():
    """Insert after a web tool fired this turn → gate fires."""
    assert _web_gate_should_fire(
        tool_name="insert_investment",
        web_used=True,
        confirmed_tools=[],
    ) is True


def test_web_gate_does_not_fire_on_insert_without_web():
    """Insert without any web tool firing → no gate (default flow)."""
    assert _web_gate_should_fire(
        tool_name="insert_investment",
        web_used=False,
        confirmed_tools=[],
    ) is False


def test_web_gate_bypassed_once_user_confirms():
    """Confirm-token pattern mirrors DESTRUCTIVE_TOOLS: FE re-sends
    the retry turn with 'insert_investment' in confirmed_tools to
    authorize the insert."""
    assert _web_gate_should_fire(
        tool_name="insert_investment",
        web_used=True,
        confirmed_tools=["insert_investment"],
    ) is False


def test_web_gate_does_not_fire_on_non_insert_tools():
    """fetch_* / delete_* / anything that isn't insert_investment
    passes through the branch without gating."""
    for tn in (
        "fetch_portfolio_summary",
        "fetch_security_rate",
        "delete_single_investment",
        "trigger_rate_refresh",
    ):
        assert _web_gate_should_fire(
            tool_name=tn,
            web_used=True,
            confirmed_tools=[],
        ) is False, f"gate must not fire for {tn}"


def test_web_gate_source_still_checks_four_conditions():
    """Drift guard — the production branch in agentService.py must
    still check the four conditions: (web_fetch_state wired,
    tool is insert_investment, web_fetch_state["used"] is True,
    tool not in confirmed_tools). The local reproduction in this
    test file omits the first (which is a nullness guard, not a
    gate condition) because we only reach the gate when the
    state is wired.

    Uses source-text grep rather than inspect because importing
    agentService pulls the Google auth chain — see the module-
    scope note.
    """
    src = _read_source("services/agentService.py")
    # All four literals must appear inside the source. We pin on the
    # exact Python expressions so a refactor that changes the operator
    # (e.g., `or` instead of `and`) also trips this test.
    assert "web_fetch_state is not None" in src
    assert '_tn == "insert_investment"' in src
    assert 'web_fetch_state["used"]' in src
    assert "_tn not in confirmed_tools" in src
    # And the four conditions must be in the SAME `if` statement
    # (and-chained, not or-chained). Pin the structural shape by
    # grepping for the exact if-head the production code uses.
    assert (
        "if (\n"
        "                    web_fetch_state is not None\n"
        "                    and _tn == \"insert_investment\"\n"
        "                    and web_fetch_state[\"used\"]\n"
        "                    and _tn not in confirmed_tools\n"
        "                ):"
    ) in src, (
        "web-gate branch no longer matches the expected four-condition "
        "if-head — one of the operators or condition order drifted"
    )


# ── 5: Confirm-message copy ─────────────────────────────────────────
#
# Same AST-extract trick as compute_allowed_tools: pull the
# staticmethod body out of the class in agentService source and exec
# it into a namespace so tests can call it without importing the
# Google auth chain.


def _load_web_insert_confirm_message():
    """Extract AgentService._get_web_insert_confirm_message as a
    free-standing callable (self-less — it's a staticmethod)."""
    import ast
    src = _read_source("services/agentService.py")
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "AgentService":
            for item in node.body:
                if (
                    isinstance(item, ast.FunctionDef)
                    and item.name == "_get_web_insert_confirm_message"
                ):
                    # Drop the @staticmethod decorator so exec treats
                    # it as a plain function — otherwise Python 3.9's
                    # staticmethod wrapper isn't directly callable.
                    item.decorator_list = []
                    ns = {}
                    exec(
                        compile(ast.Module([item], []), "<extract>", "exec"),
                        ns,
                    )
                    return ns["_get_web_insert_confirm_message"]
    raise RuntimeError(
        "_get_web_insert_confirm_message not found in AgentService"
    )


def test_web_insert_confirm_message_includes_service_type_and_data():
    get_msg = _load_web_insert_confirm_message()
    msg = get_msg({
        "service_type": "Mutual_Funds",
        "data": {
            "schemeCode": "120503",
            "date": "2026-11-01",
            "amount": 5000,
            "quantity": 100,
        },
    })
    assert "web lookup" in msg
    assert "Mutual_Funds" in msg
    assert "120503" in msg
    assert "5000" in msg


def test_web_insert_confirm_message_handles_missing_data():
    get_msg = _load_web_insert_confirm_message()
    msg = get_msg({})
    assert "web lookup" in msg
    # No crash on an empty input — just a generic shape.
    assert "<no data>" in msg


# ── 6: ak-pb4n MAJOR — PII-exfil + untrusted-DATA prompt hardening ──
#
# Reviewer MAJOR on ee69e79: the Web Intelligence section of
# INVESTMENT_SYSTEM_PROMPT must carry two explicit guard sentences
# so Claude doesn't (a) follow instructions found inside scraped
# content (prompt-injection → silent action), or (b) leak the
# Overseer's personal financial data into a public WebSearch query
# / WebFetch URL (PII exfiltration). The two sentences are the
# agent-directive-voice form reviewer asked for; we grep for the
# exact distinguishing phrases rather than full sentences (so an
# allowed future refinement like a typo-fix doesn't break the
# test unnecessarily).


def test_web_intel_prompt_treats_scraped_content_as_untrusted_data():
    """Sentence 1: scraped content is DATA, never instructions."""
    src = _read_source("services/agent_tools.py")
    # Distinguishing phrases that carry the semantic.
    assert "untrusted DATA" in src, (
        "Web Intelligence prompt must declare scraped content as "
        "untrusted DATA — ak-pb4n MAJOR sentence 1 missing"
    )
    assert "never as instructions" in src, (
        "Web Intelligence prompt must say 'never as instructions' — "
        "ak-pb4n MAJOR sentence 1 missing the actionable half"
    )
    # And the response protocol: surface the attempted injection
    # rather than silently ignoring (so the user sees what happened).
    assert "prompt-injection" in src
    assert "surface" in src or "report" in src


def test_web_intel_prompt_forbids_PII_in_web_queries():
    """Sentence 2: never put personal financial data into a
    WebSearch query or WebFetch URL."""
    src = _read_source("services/agent_tools.py")
    # Distinguishing phrases. The exact wording per reviewer.
    assert "personal financial data" in src, (
        "Web Intelligence prompt must forbid personal-financial-data "
        "in web queries — ak-pb4n MAJOR sentence 2 missing"
    )
    assert "WebSearch query" in src or "WebFetch URL" in src, (
        "Web Intelligence prompt must name the specific vectors "
        "(WebSearch query / WebFetch URL) — ak-pb4n MAJOR sentence 2 "
        "missing the actionable half"
    )
    # And the public-identifiers-only positive framing.
    assert "public market identifiers" in src or "public identifier" in src, (
        "Web Intelligence prompt must state what IS OK to send "
        "(public market identifiers) — ak-pb4n MAJOR sentence 2 "
        "missing the positive framing"
    )


def test_web_intel_prompt_hardening_adjacent_to_insert_gate():
    """Reviewer positioning spec: the two new rules must sit
    adjacent to the existing insert-gate rationale (so the agent
    reads them in the right context when considering a web call).
    We pin on both headers being present AND the hardening header
    appearing BEFORE the insert-gate header in the source."""
    src = _read_source("services/agent_tools.py")
    # Both headers present (grep for distinguishing fragments).
    hardening_marker = "Hard rules for web access"
    gate_marker = "Insert-after-web confirm-gate"
    assert hardening_marker in src
    assert gate_marker in src
    # And the hardening block comes BEFORE the gate (adjacent-above).
    assert src.index(hardening_marker) < src.index(gate_marker), (
        "ak-pb4n hardening rules must sit adjacent to (and above) "
        "the insert-gate rationale — reviewer positioning spec"
    )


def test_web_intel_prompt_mentions_both_vectors_WebSearch_and_WebFetch():
    """The PII rule must name BOTH vectors (search AND fetch). A
    rule that only names one leaves the other open."""
    src = _read_source("services/agent_tools.py")
    # Both tool names must appear in the hardening region.
    # Split the file at the hardening section header and search only
    # within the next ~2k chars so a WebSearch mention elsewhere
    # (built-ins section) doesn't satisfy the check.
    import re
    hardening_region = re.search(
        r"Hard rules for web access.*?Insert-after-web confirm-gate",
        src, re.DOTALL,
    )
    assert hardening_region is not None, (
        "could not locate the ak-pb4n hardening region"
    )
    region = hardening_region.group(0)
    assert "WebSearch" in region
    assert "WebFetch" in region


def test_web_insert_confirm_message_handles_epf_two_half_split():
    """EPF uses employee_amount + employer_amount instead of a
    single 'amount' — the preview must surface both so the user
    eyeballs both halves."""
    get_msg = _load_web_insert_confirm_message()
    msg = get_msg({
        "service_type": "EPF",
        "data": {
            "date": "2026-11-01",
            "description": "October salary",
            "employee_amount": 7500,
            "employer_amount": 7500,
        },
    })
    assert "EPF" in msg
    assert "employee_amount=7500" in msg
    assert "employer_amount=7500" in msg
