"""ak-c2gl Bead D: regression tests for the INVESTMENT_SYSTEM_PROMPT
refresh.

Scope: the prompt is a BE artefact the investment agent sees every
turn. Bead D's deliverable is the prompt content itself. These tests
pin down the bead's four scope items so a future rewrite (or merge
accident) doesn't silently drop a section:

  1. **Tool catalogue** — every tool in the registered INVESTMENT_TOOLS
     list is mentioned by name in the prompt. Pre-ak-c2gl, 12 tools
     (8 Bead A dead-methods + 4 Kite Bundle 2/3 monitoring +
     4 Bead C analytics) existed as registered tools but were absent
     from the catalogue, so the agent couldn't discover them.

  2. **Analysis Assistant Guide** — sister section to the existing
     Purchase Assistant Guide. Must exist as a `## Analysis Assistant
     Guide` heading and cover the 5 analytical question classes
     (allocation / rebalance / concentration / historical / news).

  3. **When-to-web-search decision tree** — explicit routing rules
     so the model stops WebSearching for internal data (regression
     caught multiple times in prior arcs). The section anchors on
     "decision tree" and names the internal tools for each question
     class.

  4. **Prompt housekeeping** — model-pinning note, confirm-gate
     flow note, history-bounding note. All three must land as
     Rule bullets so the model has grounding for its own behaviour.

These are PROMPT-CONTENT assertions — source-grep tests in the
spirit of test_kite_bundle2's registration tests. The cost is minor
(the assertions catch missing SECTIONS not every WORD; they don't
over-fit to prose), the benefit is high (merge/rewrite regressions
go red immediately instead of surfacing as a quiet "the agent no
longer suggests benchmark_compare for performance queries").
"""

import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# Stub the claude_agent_sdk / anyio imports so loading agent_tools
# doesn't need them (same pattern as test_kite_bundle2).
import types
from unittest.mock import MagicMock

_INSTALLED_STUBS = []
for _pkg in ("claude_agent_sdk", "anyio"):
    if _pkg not in sys.modules:
        _m = types.ModuleType(_pkg)
        _m.__path__ = []
        _m.__getattr__ = lambda name, pkg=_pkg: MagicMock(name=f"{pkg}.{name}")
        sys.modules[_pkg] = _m
        _INSTALLED_STUBS.append(_pkg)

from services.agent_tools import (  # noqa: E402
    INVESTMENT_SYSTEM_PROMPT,
    INVESTMENT_TOOLS,
)

for _pkg in _INSTALLED_STUBS:
    sys.modules.pop(_pkg, None)


# ── (1) Tool catalogue coverage ──────────────────────────────────────


class TestInvestmentPromptToolCatalogue(unittest.TestCase):
    """Every registered INVESTMENT_TOOLS entry must appear in the
    system prompt by name. Pre-ak-c2gl: 12 tools were registered but
    absent from the prompt catalogue → agent couldn't discover them.
    """

    def _assert_in_prompt(self, tool_name, context_msg=""):
        self.assertIn(
            tool_name, INVESTMENT_SYSTEM_PROMPT,
            f"Tool {tool_name!r} is registered in INVESTMENT_TOOLS "
            f"but missing from INVESTMENT_SYSTEM_PROMPT catalogue. "
            f"{context_msg}",
        )

    def test_bead_a_dead_method_tools_in_catalogue(self):
        """ak-rde9 Bead A exposed 8 previously-dead methods. All 8
        must appear in the prompt catalogue so the agent discovers
        them."""
        for name in (
            "fetch_realized_pnl",
            "fetch_fo_summary",
            "fetch_fo_trades",
            "fetch_investment_snapshots",
            "fetch_investment_emails",
            "fetch_email_body",
            "get_kite_login_url",
            "get_kite_token_status",
        ):
            self._assert_in_prompt(
                name, "Bead A (ak-rde9) dead-method exposure.",
            )

    def test_bead_b_web_wrappers_in_catalogue(self):
        """ak-6k2m Bead B's 5 web-wrapper scaffolds."""
        for name in (
            "fetch_stock_news",
            "fetch_earnings_calendar",
            "fetch_company_announcements",
            "fetch_macro_context",
            "fetch_fund_factsheet",
        ):
            self._assert_in_prompt(
                name, "Bead B (ak-6k2m) web wrappers.",
            )

    def test_bead_c_analytics_tools_in_catalogue(self):
        """ak-p8ix Bead C's 4 deterministic analytics tools."""
        for name in (
            "portfolio_allocation",
            "benchmark_compare",
            "position_sizing",
            "concentration_risk",
        ):
            self._assert_in_prompt(
                name, "Bead C (ak-p8ix) analytics layer.",
            )

    def test_kite_bundle2_and_3_tools_in_catalogue(self):
        """ak-zai2 (Bundle 2) + ak-dx98 (Bundle 3) monitoring tools."""
        for name in (
            "get_kite_trades", "get_kite_margins",
            "get_kite_gtts", "get_kite_alerts",
        ):
            self._assert_in_prompt(
                name, "Kite Bundles 2+3 monitoring surfaces.",
            )

    def test_builtin_web_tools_named(self):
        """WebSearch + WebFetch must be named so the agent knows the
        built-in free-form escape hatch exists alongside the
        structured wrappers."""
        self._assert_in_prompt("WebSearch")
        self._assert_in_prompt("WebFetch")

    def test_every_registered_tool_name_in_prompt(self):
        """Drift guard: every name in INVESTMENT_TOOLS must appear
        somewhere in the prompt. Adding a new tool without catalog-
        uing it in the prompt makes the agent blind to it — this
        test red-flags that gap on CI."""
        missing = []
        for tool in INVESTMENT_TOOLS:
            name = tool.get("name")
            if not name:
                continue
            if name not in INVESTMENT_SYSTEM_PROMPT:
                missing.append(name)
        self.assertEqual(
            missing, [],
            f"These registered tools are absent from the prompt "
            f"catalogue: {missing}. Add a one-liner for each so the "
            f"agent can discover them.",
        )


# ── (2) Analysis Assistant Guide section ─────────────────────────────


class TestAnalysisAssistantGuide(unittest.TestCase):
    """Bead D scope item 2: new section counterpart to the existing
    Purchase Assistant Guide. Must cover the 5 analytical question
    classes."""

    def test_section_heading_present(self):
        self.assertIn(
            "## Analysis Assistant Guide", INVESTMENT_SYSTEM_PROMPT,
            "Bead D added a sister section to Purchase Assistant "
            "Guide; the exact `## Analysis Assistant Guide` heading "
            "must be present as a stable anchor.",
        )

    def test_covers_historical_performance_queries(self):
        """Historical queries route through fetch_investment_snapshots
        (NOT agent math on raw holdings)."""
        import re
        self.assertIn(
            "fetch_investment_snapshots", INVESTMENT_SYSTEM_PROMPT,
        )
        # Must say use the tool, not derive via agent math. Allow
        # cross-line match (section heading + bullet) via DOTALL.
        self.assertTrue(
            re.search(
                r"(?:historical|performance|growth).*"
                r"fetch_investment_snapshots",
                INVESTMENT_SYSTEM_PROMPT, re.IGNORECASE | re.DOTALL,
            ),
            "Analysis Guide must route historical/performance "
            "queries to fetch_investment_snapshots.",
        )

    def test_covers_allocation_questions(self):
        """Allocation routes through portfolio_allocation (verbatim
        quoting; no sum-and-divide)."""
        # Both the tool reference AND the "verbatim" discipline note
        # must appear in the prompt.
        self.assertIn("portfolio_allocation", INVESTMENT_SYSTEM_PROMPT)
        self.assertIn("verbatim", INVESTMENT_SYSTEM_PROMPT.lower())

    def test_covers_rebalance_questions(self):
        """Rebalance / 'how much more to buy' routes through
        position_sizing."""
        self.assertIn("position_sizing", INVESTMENT_SYSTEM_PROMPT)
        # Direction field must be named so the model knows how to
        # interpret the return.
        self.assertIn("direction", INVESTMENT_SYSTEM_PROMPT)

    def test_covers_concentration_risk_questions(self):
        self.assertIn("concentration_risk", INVESTMENT_SYSTEM_PROMPT)
        # Empty-flagged handling: must instruct the agent to speak
        # the diversified result explicitly.
        self.assertIn("well-diversified", INVESTMENT_SYSTEM_PROMPT)

    def test_benchmark_scope_disclosure_mandated(self):
        """benchmark_compare returns an equity-basket return. The
        prompt MUST instruct the agent to disclose scope rather than
        saying 'portfolio return'."""
        self.assertIn(
            "equity basket", INVESTMENT_SYSTEM_PROMPT.lower(),
            "Analysis Guide must mandate 'equity basket' scope "
            "language when quoting benchmark_compare.",
        )
        self.assertIn("caveat", INVESTMENT_SYSTEM_PROMPT.lower())

    def test_covers_news_earnings_macro_queries(self):
        """News / earnings / macro route through the 5 Bead B
        wrappers; prompt must name them together in an analytical
        context."""
        prompt = INVESTMENT_SYSTEM_PROMPT
        for tool in (
            "fetch_stock_news",
            "fetch_earnings_calendar",
            "fetch_macro_context",
            "fetch_company_announcements",
            "fetch_fund_factsheet",
        ):
            self.assertIn(tool, prompt)
        # Cite-source discipline must be explicit (every web number
        # carries a URL) — a recurring prior-arc drift.
        self.assertRegex(
            prompt, r"[Cc]ite.*source",
            msg="Analysis Guide must mandate source citation for "
                "every web-derived number.",
        )

    def test_present_facts_not_recommendations(self):
        """Accounting-correctness + regulatory stance: present facts,
        don't recommend buy/sell. Pre-ak-c2gl, the prompt had no such
        guard rail in the analysis context."""
        # Case-insensitive match since prose casing varies.
        prompt_lower = INVESTMENT_SYSTEM_PROMPT.lower()
        self.assertRegex(
            prompt_lower,
            r"(?:present facts|do not.*recommend|not advisory)",
            msg="Analysis Guide must forbid buy/sell recommendations "
                "and anchor on fact-presentation.",
        )


# ── (3) When-to-web-search decision tree ─────────────────────────────


class TestWebVsToolDecisionTree(unittest.TestCase):
    """Bead D scope item 3: explicit routing rules for the model.
    Recurring prior-arc drift: model WebSearching for internal data
    (wrong numbers + PII leak surface). The decision tree names
    internal tools per question class."""

    def test_decision_tree_section_present(self):
        self.assertRegex(
            INVESTMENT_SYSTEM_PROMPT,
            r"(?:when to use what|decision tree|route.*question)",
            msg="Bead D requires an explicit decision-tree section "
                "naming internal vs external routing.",
        )

    def test_internal_data_tools_named(self):
        """'What's my X' routes to the internal tool, not WebSearch."""
        prompt = INVESTMENT_SYSTEM_PROMPT
        # The internal "my X" tool set must be named in the context
        # of an internal-question routing rule.
        for internal_tool in (
            "fetch_portfolio_summary",
            "fetch_user_securities",
            "fetch_epg_data",
        ):
            self.assertIn(internal_tool, prompt)

    def test_never_websearch_for_internal_data_rule(self):
        """Hard-stop rule: NEVER WebSearch for NAV / holdings / rates.
        Previous arcs tracked this drift explicitly."""
        prompt_lower = INVESTMENT_SYSTEM_PROMPT.lower()
        self.assertRegex(
            prompt_lower,
            r"(?:never websearch|do not websearch|not websearch).*"
            r"(?:internal|nav|holdings|rates)",
            msg="Decision tree must carry a 'do NOT WebSearch for "
                "internal data' rule — recurring prior-arc drift.",
        )

    def test_market_news_question_routes_to_wrapper(self):
        """News / macro / earnings questions route through the
        Bead B wrappers, not raw WebSearch (consistency + source
        hygiene)."""
        prompt = INVESTMENT_SYSTEM_PROMPT
        # The wrappers must be named adjacent to the "news / macro /
        # earnings" question framing.
        self.assertRegex(
            prompt,
            r"[Nn]ews.*fetch_stock_news|fetch_stock_news.*news",
            msg="Decision tree must route news queries to "
                "fetch_stock_news (or vice-versa).",
        )


# ── (4) Prompt housekeeping notes ────────────────────────────────────


class TestPromptHousekeepingNotes(unittest.TestCase):
    """Bead D scope item 4: model-pinning note, confirm-gate flow
    note, history-bounding note."""

    def test_model_pinning_note_present(self):
        """Mention that the model is version-pinned so prompt-edit
        requests don't drift into model-swap requests."""
        prompt_lower = INVESTMENT_SYSTEM_PROMPT.lower()
        self.assertRegex(
            prompt_lower,
            r"(?:model.*pin|pinned.*model|version[- ]pin)",
            msg="Housekeeping: note that the model is version-pinned "
                "(prompt changes, not model swaps).",
        )

    def test_confirm_gate_behaviour_explicit(self):
        """If a web call precedes insert_investment, the confirm-gate
        fires. Prompt must tell the model this is expected."""
        prompt = INVESTMENT_SYSTEM_PROMPT
        self.assertIn("Action cancelled", prompt)
        self.assertIn("confirm", prompt.lower())
        # Flow: wait for user approval, THEN retry. Must appear as
        # explicit guidance.
        self.assertRegex(
            prompt,
            r"(?:wait.*approv|retry.*approv|approv.*retry)",
            msg="Confirm-gate note must instruct 'wait for user "
                "approval, then retry'.",
        )

    def test_history_bounding_note_present(self):
        """Long threads lose earliest context; prompt must instruct
        the model to ASK rather than invent."""
        prompt_lower = INVESTMENT_SYSTEM_PROMPT.lower()
        self.assertRegex(
            prompt_lower,
            r"(?:history|long conversation|long thread|context).*"
            r"(?:ask|re-share|drop)",
            msg="History-bounding note: long threads prune earliest "
                "turns; agent must ASK the user to re-share, not "
                "invent.",
        )


# ── (5) ak-z7lk PII-wording consistency guard ────────────────────────


class TestWebIntelligencePiiWordingConsistency(unittest.TestCase):
    """ak-z7lk follow-up on ak-c2gl reviewer MINOR: the Web
    Intelligence rule 2 (preserved from ak-pb4n) previously listed
    'scheme codes' in BOTH the forbidden list AND the permitted
    list, contradicting itself. Fix mirrors the ak-p8ix MINOR-2
    WealthDigest rewording: forbid only personal identifiers (PAN /
    Aadhaar / account numbers / Firebase user id / email / receipt
    content / ₹ amounts / purchase prices); explicitly permit public
    market identifiers (tickers / AMFI scheme codes / scheme names
    / macro terms / company names).

    These tests pin down that resolution so a future rewrite of the
    Web Intelligence section doesn't silently regress — especially
    important because ak-c2gl's new Analysis Guide + decision tree +
    fetch_fund_factsheet catalogue entry all actively depend on the
    permitted reading.
    """

    def _web_rules_section(self):
        """Extract the Web Intelligence § hard rules block so the
        assertions below only look at the rule text, not the catalog
        entries or the Analysis Guide which naturally mention
        'scheme codes' as a permitted use."""
        prompt = INVESTMENT_SYSTEM_PROMPT
        # Anchor on the ak-pb4n section header; grab up to the next
        # major section.
        start = prompt.find("### Hard rules for web access")
        if start < 0:
            return ""
        end = prompt.find("###", start + 3)
        return prompt[start:end] if end > start else prompt[start:]

    def test_web_rule_2_does_not_forbid_scheme_codes(self):
        """ak-z7lk: 'scheme codes' MUST NOT appear in the forbidden
        list — they're public AMFI identifiers and the mandated
        fetch_fund_factsheet cadence needs them. The ak-c2gl
        reviewer MINOR flagged the contradiction inside this exact
        rule."""
        section = self._web_rules_section()
        self.assertTrue(section, (
            "Web Intelligence hard-rules section not found — "
            "has the heading been renamed?"
        ))
        # The forbidden clause anchors on "Never send" (ak-p8ix
        # MINOR-2 wording) or "Never put" (pre-rework wording).
        # Scan a window AROUND the forbidden clause.
        import re
        forbid_match = re.search(
            r"(?:Never\s+(?:send|put).*?PERSONAL.*?URL\.)",
            section, re.DOTALL | re.IGNORECASE,
        )
        self.assertIsNotNone(forbid_match, (
            "ak-z7lk: Web rule 2 must still carry a 'Never send ...'"
            " forbidden clause with the PERSONAL-identifiers wording"
            " mirroring ak-p8ix MINOR-2."
        ))
        forbid_clause = forbid_match.group(0).lower()
        self.assertNotIn(
            "scheme code", forbid_clause,
            "ak-z7lk REGRESSION: 'scheme codes' is back in the "
            "forbidden list of Web Intelligence rule 2. They're "
            "public AMFI identifiers — per ak-p8ix MINOR-2 "
            "WealthDigest wording, only PERSONAL identifiers belong "
            "in the forbidden list.",
        )

    def test_web_rule_2_explicitly_permits_scheme_codes(self):
        """Mirror of the WealthDigest 'Public market identifiers are
        allowed' affordance. Scheme codes must be named as a
        permitted canonical-query token."""
        section = self._web_rules_section()
        self.assertIn(
            "Public market identifiers are allowed", section,
            "ak-z7lk: Web rule 2 must carry the 'Public market "
            "identifiers are allowed' affordance verbatim (parity "
            "with ak-p8ix MINOR-2 WealthDigest wording).",
        )
        # The permitted-list exemplars must name AMFI scheme codes
        # explicitly — the 120503 ticker is the canonical example
        # fetch_fund_factsheet uses.
        self.assertIn(
            "AMFI scheme codes", section,
            "ak-z7lk: permitted list must name 'AMFI scheme codes' "
            "explicitly to unblock fetch_fund_factsheet cadence.",
        )

    def test_web_rule_2_lists_the_personal_identifier_exemplars(self):
        """The forbidden list must enumerate PAN / Aadhaar / account
        numbers / Firebase user id / email / receipt content / ₹
        amounts / purchase prices — the full ak-p8ix MINOR-2
        vocabulary so the model has concrete anchors for what IS
        forbidden."""
        # Normalize whitespace (the prose is line-wrapped so a
        # multi-word marker like "receipt content" can straddle a
        # newline + indent — flatten before matching).
        import re
        section = re.sub(r"\s+", " ", self._web_rules_section())
        for marker in (
            "PAN", "Aadhaar", "account numbers", "Firebase user id",
            "email addresses", "receipt content",
            "portfolio amounts in ₹", "individual purchase prices",
        ):
            self.assertIn(
                marker, section,
                f"ak-z7lk: forbidden-list exemplar {marker!r} missing "
                f"from Web Intelligence rule 2. The ak-p8ix MINOR-2 "
                f"WealthDigest vocabulary must be mirrored verbatim.",
            )


if __name__ == "__main__":
    unittest.main()
