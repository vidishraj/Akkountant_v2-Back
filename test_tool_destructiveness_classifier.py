"""ak-npdf (2026-11-13): tests for the tool destructiveness classifier.

Future-proofs the FE confirm-gate tone selector (ak-6lyp FE-7
MINOR follow-up). Before ak-npdf, the FE picked tone via
``tool.startsWith("delete")`` — correct for today's 5 destructive
tools but mis-classifies a hypothetical ``purge_*`` / ``remove_*``
/ ``wipe_*`` / ``clear_history_*`` which would wrongly get the
softer mutation tone.

What this file verifies without a Flask app or DB:

  1. The classifier helper (``get_tool_destructiveness``) returns
     the right value for every member of each explicit set.
  2. The default ("read") fires for every tool NOT explicitly
     classified — sampled across each registry.
  3. The ``destructiveness`` field is stamped onto every
     INVESTMENT_TOOLS / TRANSACTION_TOOLS / FREELANCE_TOOLS def
     at module load (no tool def is left without the field).
  4. Every member of agentService's trigger-gate
     ``DESTRUCTIVE_TOOLS`` set is also present in EITHER the
     destructive OR mutation classifier set here. A tool that
     triggers the gate but has destructiveness="read" would fire
     a confirm dialog with neutral tone — the bug the bead
     flagged, just flipped.
  5. Classifier sets are frozensets (immutable — a future caller
     mutating them would silently shift classification).
  6. Source-text drift guards: both confirm-event emissions in
     agentService._build_sdk_tools include
     ``get_tool_destructiveness(_tn)`` on the payload. A future
     refactor that drops the field restores the FE's
     name-prefix heuristic silently.
"""
import os
import sys
import types
from unittest.mock import MagicMock

import pytest


# Same claude_agent_sdk + anyio stub pattern as
# test_investment_agent_foundation / test_investment_web_intelligence —
# agent_tools.py's web-wrapper imports pull services.agent_tool_executor
# which imports flask indirectly; stubs keep the collection step clean.
_INSTALLED_STUBS = []
for _pkg in ("claude_agent_sdk", "anyio"):
    if _pkg not in sys.modules:
        _m = types.ModuleType(_pkg)
        _m.__path__ = []
        _m.__getattr__ = lambda name, pkg=_pkg: MagicMock(name=f"{pkg}.{name}")
        sys.modules[_pkg] = _m
        _INSTALLED_STUBS.append(_pkg)

from services.agent_tools import (  # noqa: E402
    INVESTMENT_TOOLS,
    TRANSACTION_TOOLS,
    FREELANCE_TOOLS,
    TOOL_DESTRUCTIVENESS_DESTRUCTIVE,
    TOOL_DESTRUCTIVENESS_MUTATION,
    get_tool_destructiveness,
)

for _pkg in _INSTALLED_STUBS:
    sys.modules.pop(_pkg, None)


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ── 1: Classifier returns correct value for each explicit set member ─


@pytest.mark.parametrize("name", sorted(TOOL_DESTRUCTIVENESS_DESTRUCTIVE))
def test_destructive_set_classifies_as_destructive(name):
    assert get_tool_destructiveness(name) == "destructive"


@pytest.mark.parametrize("name", sorted(TOOL_DESTRUCTIVENESS_MUTATION))
def test_mutation_set_classifies_as_mutation(name):
    assert get_tool_destructiveness(name) == "mutation"


# ── 2: Default = "read" for every tool NOT in an explicit set ────────


@pytest.mark.parametrize("name", [
    # Investment reads
    "fetch_portfolio_summary", "fetch_user_securities",
    "fetch_security_rate", "search_securities",
    "get_rate_freshness", "fetch_investment_snapshots",
    "get_kite_login_url", "get_kite_token_status",
    "get_kite_trades", "get_kite_margins",
    "get_kite_gtts", "get_kite_alerts",
    # Web wrappers (ak-6k2m) — return search plans, no writes
    "fetch_stock_news", "fetch_earnings_calendar",
    "fetch_company_announcements", "fetch_macro_context",
    "fetch_fund_factsheet",
    # Transaction reads
    "fetch_transactions", "fetch_calendar_transactions",
    "fetch_opted_banks", "fetch_file_details",
    # Freelance reads
    "get_invoices", "get_customers",
    "get_invoice_by_number", "get_dashboard_analytics",
    "get_earnings_by_date_range",
])
def test_read_tools_default_to_read(name):
    assert get_tool_destructiveness(name) == "read"


def test_unknown_tool_defaults_to_read():
    """Future/unknown tool names fall back to the neutral tone.
    New fetch_* / get_* tools get safe default without an explicit
    classifier entry. The DRIFT TO WATCH is the opposite — a new
    destructive tool landing without explicit classification; that's
    covered by test_DESTRUCTIVE_TOOLS_members_classified_as_destructive
    below."""
    assert get_tool_destructiveness("totally_made_up_tool") == "read"
    assert get_tool_destructiveness("purge_everything") == "read"


# ── 3: destructiveness stamped onto every tool def ───────────────────


@pytest.mark.parametrize("registry_name,registry", [
    ("INVESTMENT_TOOLS", INVESTMENT_TOOLS),
    ("TRANSACTION_TOOLS", TRANSACTION_TOOLS),
    ("FREELANCE_TOOLS", FREELANCE_TOOLS),
])
def test_every_tool_def_has_destructiveness_field(registry_name, registry):
    """Module-load stamp pass must leave NO tool def without the
    field. A new tool added after ak-npdf lands will get the field
    for free on next module load (because the stamp loop iterates
    the registry). If the stamp loop ever regresses, the FE can't
    rely on the field being present."""
    missing = [
        t["name"] for t in registry if "destructiveness" not in t
    ]
    assert missing == [], (
        f"{registry_name} tool defs missing 'destructiveness' field: "
        f"{missing}"
    )


@pytest.mark.parametrize("registry_name,registry", [
    ("INVESTMENT_TOOLS", INVESTMENT_TOOLS),
    ("TRANSACTION_TOOLS", TRANSACTION_TOOLS),
    ("FREELANCE_TOOLS", FREELANCE_TOOLS),
])
def test_every_tool_def_destructiveness_is_valid(registry_name, registry):
    """Every stamped value must be one of the three allowed values.
    Catches a future typo (e.g. 'destuctive' or 'mutaion')."""
    allowed = {"destructive", "mutation", "read"}
    bad = [
        (t["name"], t.get("destructiveness"))
        for t in registry
        if t.get("destructiveness") not in allowed
    ]
    assert bad == [], (
        f"{registry_name} tool defs with invalid destructiveness: {bad}"
    )


def test_stamped_values_match_classifier():
    """Cross-check: the value on each tool def must match what the
    helper returns for that name. A divergence indicates the stamp
    loop didn't fire for a tool (e.g. a late-appended tool def
    after the stamp pass)."""
    for registry in (INVESTMENT_TOOLS, TRANSACTION_TOOLS, FREELANCE_TOOLS):
        for t in registry:
            expected = get_tool_destructiveness(t["name"])
            actual = t.get("destructiveness")
            assert actual == expected, (
                f"{t['name']} stamped={actual!r} but classifier "
                f"returns {expected!r}"
            )


# ── 4: Trigger-gate ∩ tone drift guard ──────────────────────────────


def test_DESTRUCTIVE_TOOLS_members_classified_as_destructive_or_mutation():
    """Every tool in agentService's trigger-gate ``DESTRUCTIVE_TOOLS``
    set must be classified either destructive OR mutation (never
    read). Otherwise the FE would get a confirm event with neutral
    tone for an action that genuinely writes/deletes — the exact
    class of mis-classification the bead flags, inverted.

    Specifically: every ``delete_*`` must be destructive;
    everything else in the trigger set can be mutation (e.g.,
    sync_kite_holdings is upsert per ak-n9im).

    Source-grep rather than import — agentService.py pulls the
    google auth chain.
    """
    src = _read_source("services/agentService.py")
    # Narrow grep the DESTRUCTIVE_TOOLS block (immediately after
    # the pin-model-id import block; closes with a `}`).
    import re
    m = re.search(
        r"DESTRUCTIVE_TOOLS\s*=\s*\{([^}]*)\}",
        src, re.DOTALL,
    )
    assert m is not None, (
        "could not locate DESTRUCTIVE_TOOLS set in agentService source"
    )
    body = m.group(1)
    trigger_set = {
        tok.strip().strip('",\'')
        for tok in body.split(",")
        if tok.strip().strip('",\'')
    }
    # Every trigger-set member must be classified non-read.
    read_leakers = [
        name for name in trigger_set
        if get_tool_destructiveness(name) == "read"
    ]
    assert read_leakers == [], (
        f"DESTRUCTIVE_TOOLS members leaking to 'read' tone: "
        f"{read_leakers}. Add them to "
        f"TOOL_DESTRUCTIVENESS_DESTRUCTIVE or "
        f"TOOL_DESTRUCTIVENESS_MUTATION."
    )
    # Every delete_* in the trigger set must be destructive (stronger).
    delete_mis_tone = [
        name for name in trigger_set
        if name.startswith("delete_")
        and get_tool_destructiveness(name) != "destructive"
    ]
    assert delete_mis_tone == [], (
        f"delete_* members of DESTRUCTIVE_TOOLS NOT classified "
        f"'destructive': {delete_mis_tone}. UX regression — these "
        f"would get the softer mutation tone."
    )


# ── 5: Set types are frozen ──────────────────────────────────────────


def test_classifier_sets_are_frozensets():
    """Immutable — a future caller `TOOL_DESTRUCTIVENESS_DESTRUCTIVE
    .add("foo")` would silently shift classification. frozenset
    makes that a TypeError."""
    assert isinstance(TOOL_DESTRUCTIVENESS_DESTRUCTIVE, frozenset)
    assert isinstance(TOOL_DESTRUCTIVENESS_MUTATION, frozenset)


def test_classifier_sets_are_disjoint():
    """A tool can't be both destructive AND mutation — the
    classifier returns the first match (destructive), so an overlap
    would be dead-code in the mutation set."""
    overlap = (
        TOOL_DESTRUCTIVENESS_DESTRUCTIVE & TOOL_DESTRUCTIVENESS_MUTATION
    )
    assert overlap == frozenset(), (
        f"destructive + mutation sets overlap: {overlap}"
    )


# ── 6: Confirm events carry the field ────────────────────────────────


def test_destructive_confirm_event_includes_destructiveness_field():
    """Source-grep drift guard: the DESTRUCTIVE_TOOLS confirm event
    in agentService._build_sdk_tools must include
    `get_tool_destructiveness(_tn)` on the payload. A future refactor
    that drops this restores the FE's name-prefix heuristic bug."""
    src = _read_source("services/agentService.py")
    # Scope grep to the DESTRUCTIVE_TOOLS branch.
    anchor = "Check if destructive and not confirmed"
    i = src.find(anchor)
    assert i != -1, "could not locate destructive-confirm branch"
    # Grab the next ~1500 chars — the branch body.
    chunk = src[i:i + 1500]
    assert "get_tool_destructiveness(_tn)" in chunk, (
        "destructive-confirm event payload must include "
        "destructiveness=get_tool_destructiveness(_tn) — ak-npdf "
        "fix dropped / regressed"
    )


def test_web_insert_confirm_event_includes_destructiveness_field():
    """Same drift guard for the ak-6k2m web-gate confirm event.
    Payload is a different code path but the FE consumes the same
    event type — missing field here = half-fixed."""
    src = _read_source("services/agentService.py")
    anchor = 'reason": "web_fetch_prior_in_turn"'
    i = src.find(anchor)
    assert i != -1, "could not locate web-gate confirm branch"
    # The destructiveness field should appear adjacent to the reason tag.
    chunk = src[max(0, i - 600):i + 600]
    assert "get_tool_destructiveness(_tn)" in chunk, (
        "web-gate confirm event payload must include "
        "destructiveness=get_tool_destructiveness(_tn) — ak-npdf "
        "fix dropped / regressed"
    )
