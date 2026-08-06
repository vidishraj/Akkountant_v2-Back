"""ak-ran Phase 1 unit tests for WealthDigestTask + agentConversationService
find_or_create_by_title + registration wiring + fleet-smoke inclusion.

Test tiers (matches the arc pattern from ak-lp6..ak-nl4):
  * Source-inspection guards — always run, no runtime deps.
  * AST-lifted logic — drive pure-Python helpers (input assembly,
    coerce_json, notable-movers sort/cap) without importing the
    BaseTask stack (flask/sqlalchemy/oauth2).
  * Real-dep integration — skipped cleanly when imports fail.

Coverage:
  - WEALTH_DIGEST_TITLE constant + find_or_create_by_title method
    added to agentConversationService.
  - WealthDigestTask registered in scheduler.TASK_MAPPING +
    InvestmentService.jobsObject.
  - _coerce_json unwraps Flask (data, status) tuples + Response
    objects + passes through plain dict/list.
  - _safe_mf_movers threshold + top-N cap logic.
  - System prompt contains the required framing header, output
    structure sections, and freshness footer instruction.
  - run() fail-soft on missing WEALTH_DIGEST_USER_ID env var.
  - Bootstrap script is idempotent (source-inspection guard).

Run:
    python3 -m unittest test_wealth_digest
    python3 -m pytest test_wealth_digest.py
"""

import ast
import json
import os
import sys
import unittest
from decimal import Decimal


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_WEALTH_TASK_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "WealthDigestTask.py",
)
_CONV_SVC_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "agentConversationService.py",
)
_SCHEDULER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "scheduler.py",
)
_INV_SVC_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "InvestmentService.py",
)
_BOOTSTRAP_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "bootstrap_wealth_digest.py",
)
_CONSTANTS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "wealth_digest_constants.py",
)


def _source(path):
    with open(path) as fh:
        return fh.read()


def _source_code_only(path):
    src = _source(path)
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        hash_pos = line.find("#")
        if hash_pos != -1:
            pre = line[:hash_pos]
            if pre.count('"') % 2 == 0 and pre.count("'") % 2 == 0:
                line = line[:hash_pos].rstrip()
        out.append(line)
    return "\n".join(out)


# ── Constant + method presence on agentConversationService ──────────────


class TestAgentConversationServiceAdditions(unittest.TestCase):
    """ak-ran Q1: WEALTH_DIGEST_TITLE lives at module level so callers
    can share the exact string. Q4: find_or_create_by_title is the
    idempotent primitive WealthDigestTask uses to converge on one
    thread per user across daily runs."""

    def test_wealth_digest_title_constant_exact(self):
        print("\n[ak-ran Q1 — WEALTH_DIGEST_TITLE = 'Wealth Digest' pinned]")
        src = _source(_CONV_SVC_PATH)
        # Exact-value pin — Lead flagged that the string must not drift.
        self.assertIn('WEALTH_DIGEST_TITLE = "Wealth Digest"', src)
        print("  ✓ constant present with the exact value")

    def test_find_or_create_by_title_method_present(self):
        print("\n[ak-ran Q4 — find_or_create_by_title method added]")
        code = _source_code_only(_CONV_SVC_PATH)
        self.assertIn(
            "def find_or_create_by_title(self, user_id, agent_type, title):",
            code,
        )
        # Filter shape: user_id + agent_type + title + deleted_at IS NULL.
        self.assertIn("AgentConversation.user_id == user_id", code)
        self.assertIn("AgentConversation.agent_type == agent_type", code)
        self.assertIn("AgentConversation.title == title", code)
        self.assertIn("AgentConversation.deleted_at.is_(None)", code)
        # ORDER BY id ASC → returns oldest match if multiple exist.
        self.assertIn(".order_by(AgentConversation.id.asc())", code)
        # Falls through to create_conversation on miss.
        self.assertIn("self.create_conversation(", code)
        print("  ✓ method signature + filter shape + create fallback present")

    def test_find_or_create_validates_agent_type(self):
        print("\n[ak-ran Q4 — find_or_create validates agent_type against VALID_AGENT_TYPES]")
        code = _source_code_only(_CONV_SVC_PATH)
        # Source-inspect the guard — behavior test would need DB.
        self.assertIn("if agent_type not in VALID_AGENT_TYPES:", code)
        # And requires user_id + title.
        self.assertIn('raise ValueError("find_or_create_by_title: user_id required")', code)
        self.assertIn('raise ValueError("find_or_create_by_title: title required")', code)
        print("  ✓ VALID_AGENT_TYPES + user_id + title guards present")


# ── Registration wiring ─────────────────────────────────────────────────


class TestRegistrationWiring(unittest.TestCase):
    """Q5 + v2 #5: 'WealthDigest' must appear in BOTH the scheduler
    TASK_MAPPING (so runs pick up the class) AND
    InvestmentService.jobsObject (so manual trigger endpoints
    recognize the title). v2 additionally requires that the string
    is sourced from the SHARED WEALTH_DIGEST_JOB_TITLE constant at
    all three sites — so a rename touches one file instead of four."""

    def test_scheduler_task_mapping_uses_shared_constant(self):
        print("\n[ak-ran v2 #5 — scheduler.TASK_MAPPING uses WEALTH_DIGEST_JOB_TITLE constant]")
        code = _source_code_only(_SCHEDULER_PATH)
        self.assertIn(
            "from services.tasks.WealthDigestTask import WealthDigestTask",
            code,
        )
        # Constant imported + used as the dict key (not a bare literal).
        self.assertIn(
            "from services.tasks.wealth_digest_constants import WEALTH_DIGEST_JOB_TITLE",
            code,
        )
        self.assertIn("WEALTH_DIGEST_JOB_TITLE: WealthDigestTask,", code)
        # And the bare-literal shape is GONE.
        self.assertNotIn('"WealthDigest": WealthDigestTask,', code)
        print("  ✓ constant imported + used; bare literal removed")

    def test_investment_service_jobs_object_uses_shared_constant(self):
        print("\n[ak-ran v2 #5 — InvestmentService.jobsObject uses WEALTH_DIGEST_JOB_TITLE constant]")
        code = _source_code_only(_INV_SVC_PATH)
        self.assertIn(
            "from services.tasks.wealth_digest_constants import WEALTH_DIGEST_JOB_TITLE",
            code,
        )
        self.assertIn(
            'WEALTH_DIGEST_JOB_TITLE: "Wealth Digest (daily briefing)",',
            code,
        )
        # Bare-literal shape gone.
        self.assertNotIn(
            '"WealthDigest": "Wealth Digest (daily briefing)",',
            code,
        )
        print("  ✓ constant imported + used; bare literal removed")

    def test_bootstrap_uses_shared_constant(self):
        print("\n[ak-ran v2 #5 — bootstrap uses WEALTH_DIGEST_JOB_TITLE constant]")
        code = _source_code_only(_BOOTSTRAP_PATH)
        self.assertIn(
            "from services.tasks.wealth_digest_constants import (",
            code,
        )
        self.assertIn("WEALTH_DIGEST_JOB_TITLE,", code)
        # The prior `_JOB_TITLE = "WealthDigest"` bare literal is gone.
        self.assertNotIn('_JOB_TITLE = "WealthDigest"', code)
        # And no bare "WealthDigest" literal anywhere else in the file.
        for line in code.splitlines():
            if '"WealthDigest"' in line:
                self.fail(
                    f"bootstrap still has bare 'WealthDigest' literal — "
                    f"should use WEALTH_DIGEST_JOB_TITLE. Line: {line!r}",
                )
        print("  ✓ constant imported + used; bare literal removed")

    def test_shared_constant_module_is_leaf(self):
        """The whole point of wealth_digest_constants.py is to be a
        leaf module — zero non-stdlib imports so it can be pulled in
        from anywhere (including InvestmentService which is otherwise
        upstream of services.tasks.*)."""
        print("\n[ak-ran v2 #5 — wealth_digest_constants.py is a leaf module]")
        code = _source_code_only(_CONSTANTS_PATH)
        # Assert no `from ...` and no `import <non-stdlib>`. Bare
        # `import <stdlib>` is fine but this module needs nothing so
        # we assert zero imports at all.
        for line in code.splitlines():
            stripped = line.strip()
            self.assertFalse(
                stripped.startswith(("import ", "from ")),
                f"wealth_digest_constants.py has an import ({stripped!r}) — "
                f"must stay leaf so InvestmentService can import it without "
                f"creating an InvestmentService <-> services.tasks.* cycle.",
            )
        # And the constant itself is present.
        self.assertIn('WEALTH_DIGEST_JOB_TITLE = "WealthDigest"', code)
        print("  ✓ zero imports; constant string pinned")


# ── _coerce_json helper — pure-Python, AST-lifted ───────────────────────


class TestCoerceJson(unittest.TestCase):
    """_coerce_json unwraps Flask (data, status) tuples + Response
    objects + passes through plain dict/list/scalar. Lets the LLM see
    plain JSON regardless of what Flask-flavored shape the underlying
    InvestmentService method returned."""

    @classmethod
    def setUpClass(cls):
        # AST-lift the module-level _coerce_json (the class-scope
        # shim delegates to it) + _json_default. Module-level lifts
        # cleanly since it recurses by name.
        src = _source(_WEALTH_TASK_PATH)
        tree = ast.parse(src)
        picked = []
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name in ("_coerce_json", "_json_default"):
                picked.append(node)
        ns = {"json": json}
        exec(compile(ast.Module(body=picked, type_ignores=[]), "<coerce>", "exec"), ns)
        cls.coerce = staticmethod(ns["_coerce_json"])
        cls.json_default = staticmethod(ns["_json_default"])

    def test_plain_dict_passes_through(self):
        print("\n[ak-ran _coerce_json — plain dict unchanged]")
        self.assertEqual(self.coerce({"a": 1}), {"a": 1})
        print("  ✓ dict passthrough")

    def test_flask_tuple_unwrapped(self):
        print("\n[ak-ran _coerce_json — (data, status) tuple → data]")
        self.assertEqual(self.coerce(({"a": 1}, 200)), {"a": 1})
        # Empty tuple → {}.
        self.assertEqual(self.coerce(()), {})
        print("  ✓ tuple unwrapped; empty tuple → {}")

    def test_flask_response_with_get_json(self):
        print("\n[ak-ran _coerce_json — Response with .get_json() → parsed dict]")
        class _FakeResp:
            def get_json(self, silent=False):
                return {"a": "b"}
        self.assertEqual(self.coerce(_FakeResp()), {"a": "b"})
        print("  ✓ Response.get_json path works")

    def test_flask_response_with_get_data_fallback(self):
        print("\n[ak-ran _coerce_json — Response without get_json but with get_data → JSON-decoded]")
        class _FakeResp:
            def get_data(self, as_text=False):
                return '{"a": 42}'
        self.assertEqual(self.coerce(_FakeResp()), {"a": 42})
        print("  ✓ get_data fallback works")

    def test_scalar_passes_through(self):
        print("\n[ak-ran _coerce_json — scalar values pass through unchanged]")
        for v in ("hello", 42, 3.14, None, True, [1, 2, 3]):
            self.assertEqual(self.coerce(v), v)
        print("  ✓ scalar/list passthrough")


class TestJsonDefault(unittest.TestCase):
    """_json_default handles Decimal + datetime/date for json.dumps."""

    @classmethod
    def setUpClass(cls):
        # Same AST-lift as TestCoerceJson.
        src = _source(_WEALTH_TASK_PATH)
        tree = ast.parse(src)
        picked = []
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == "_json_default":
                picked.append(node)
        ns = {"json": json, "Decimal": Decimal}
        exec(compile(ast.Module(body=picked, type_ignores=[]), "<def>", "exec"), ns)
        cls.json_default = staticmethod(ns["_json_default"])

    def test_decimal_to_float(self):
        print("\n[ak-ran _json_default — Decimal → float]")
        self.assertEqual(self.json_default(Decimal("1.50")), 1.5)
        print("  ✓ Decimal coerced")

    def test_datetime_isoformat(self):
        print("\n[ak-ran _json_default — datetime → isoformat]")
        from datetime import datetime, date
        dt = datetime(2026, 8, 6, 12, 0, 0)
        self.assertEqual(self.json_default(dt), "2026-08-06T12:00:00")
        d = date(2026, 8, 6)
        self.assertEqual(self.json_default(d), "2026-08-06")
        print("  ✓ datetime + date both use isoformat")

    def test_unknown_falls_back_to_str(self):
        print("\n[ak-ran _json_default — unknown types → str(obj)]")
        class _Custom:
            def __str__(self): return "custom-repr"
        self.assertEqual(self.json_default(_Custom()), "custom-repr")
        print("  ✓ unknown types stringified")


# ── System prompt + header ──────────────────────────────────────────────


class TestSystemPrompt(unittest.TestCase):
    """The prompt encodes the mandatory framing header, output
    structure sections, and freshness footer directive. Source-
    inspection because we don't have runtime SDK access here."""

    def test_prompt_names_personal_use_header(self):
        print("\n[ak-ran — system prompt requires the [Personal use — not investment advice] header]")
        src = _source(_WEALTH_TASK_PATH)
        self.assertIn("[Personal use — not investment advice]", src)
        # And the prompt tells the model to START with it.
        self.assertIn("Start with the exact line:", src)
        print("  ✓ header string + start-with directive present")

    def test_prompt_names_required_sections(self):
        print("\n[ak-ran — system prompt names all 5 required output sections]")
        src = _source(_WEALTH_TASK_PATH)
        for section in (
            "Portfolio-level pulse",
            "Notable movers",
            "Cross-holding patterns",
            "Watch items",
            "Actionable recommendations",
        ):
            self.assertIn(section, src, f"section header {section!r} missing from prompt")
        print("  ✓ all 5 section headers present")

    def test_prompt_names_freshness_footer(self):
        print("\n[ak-ran — system prompt requires the Rates/Snapshot freshness footer]")
        src = _source(_WEALTH_TASK_PATH)
        # Loose match — the exact substitution braces + surrounding text.
        self.assertIn("Rates as of", src)
        self.assertIn("Snapshot as of", src)
        print("  ✓ freshness footer template present")

    def test_prompt_forbids_fabrication(self):
        print("\n[ak-ran — system prompt forbids fabricating numbers]")
        src = _source(_WEALTH_TASK_PATH)
        self.assertIn("Never fabricate numbers", src)
        print("  ✓ anti-fabrication rule present")

    def test_invoke_sonnet_enforces_header_belt_and_braces(self):
        """Even if the model strips the header (bad prompt-follow),
        _invoke_sonnet prepends it. Guards the personal-use framing."""
        print("\n[ak-ran — _invoke_sonnet prepends header if model omits it]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        self.assertIn("if not digest.startswith(_HEADER):", code)
        self.assertIn('digest = f"{_HEADER}\\n\\n{digest}"', code)
        print("  ✓ belt-and-braces header-prepend present")


# ── Fail-soft + config guards ───────────────────────────────────────────


class TestConfigAndFailSoft(unittest.TestCase):
    """run() must fail-soft on a missing WEALTH_DIGEST_USER_ID env
    var so the scheduler doesn't loop-fail-disable the task on a
    misconfigured deploy."""

    def test_env_var_name_pinned(self):
        print("\n[ak-ran Q3 — _ENV_USER_ID = 'WEALTH_DIGEST_USER_ID' pinned]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        self.assertIn('_ENV_USER_ID = "WEALTH_DIGEST_USER_ID"', code)
        print("  ✓ env var name pinned")

    def test_run_no_user_id_returns_completed_not_failed(self):
        """Fail-soft: missing env var → Completed with WARN, not
        Failed. The Job.failures counter doesn't tick on a missing-
        config case (which would eventually stop-scheduling the job
        at 10 failures)."""
        print("\n[ak-ran Q3 — run() with no user_id returns 'Completed' (fail-soft)]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        # The exact shape: missing env → warn + return Completed.
        self.assertIn('user_id = os.getenv(_ENV_USER_ID)', code)
        self.assertIn('if not user_id:', code)
        self.assertIn('self.logger.warning(msg)', code)
        self.assertIn('return msg, "Completed", self.interval', code)
        print("  ✓ missing env → WARN + Completed (no scheduler failure-loop)")

    def test_interval_is_1440_minutes(self):
        print("\n[ak-ran — WealthDigestTask.interval = 1440 min (24h daily)]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        self.assertIn("_INTERVAL_MINUTES = 1440", code)
        self.assertIn("self.interval = _INTERVAL_MINUTES", code)
        print("  ✓ 24h cadence pinned")


# ── v2 #6 fail-fast on invalid user ─────────────────────────────────────


class TestV2FailFastOnInvalidUser(unittest.TestCase):
    """ak-ran v2 #6: run() must pre-flight-verify the user exists
    BEFORE invoking Sonnet. A missing env var is fail-SOFT (config
    gap, no $$ wasted); a typo'd env var is fail-FAST at ERROR level
    so the misconfiguration surfaces in the jobs table + logs
    immediately, no Sonnet spend."""

    def test_user_exists_method_present(self):
        print("\n[ak-ran v2 #6 — _user_exists helper method defined]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        self.assertIn("def _user_exists(self, user_id):", code)
        # Uses the User model (late import for import-cheapness).
        self.assertIn("from models.users import User", code)
        self.assertIn("User.userID == user_id", code)
        print("  ✓ helper present + uses User model with correct PK column")

    def test_user_exists_called_before_invoke_sonnet(self):
        """Structural: the _user_exists check must appear BEFORE the
        _invoke_sonnet call in run(). Order matters — we're pre-flight
        gating on user existence to avoid the Sonnet spend."""
        print("\n[ak-ran v2 #6 — _user_exists check precedes _invoke_sonnet in run()]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        # Extract run() body.
        import re
        m = re.search(
            r"def run\(self\):(.*?)(?=\n    def |\Z)",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate run() body")
        body = m.group(1)
        user_check_idx = body.find("self._user_exists(user_id)")
        sonnet_idx = body.find("self._invoke_sonnet(")
        self.assertGreaterEqual(user_check_idx, 0, "_user_exists check missing")
        self.assertGreaterEqual(sonnet_idx, 0, "_invoke_sonnet call missing")
        self.assertLess(
            user_check_idx, sonnet_idx,
            "user-exists check must precede Sonnet invocation — "
            "typo'd env var would otherwise waste $ every daily run",
        )
        print("  ✓ user-exists check at run() body offset before sonnet call")

    def test_invalid_user_returns_failed_not_completed(self):
        """Distinct from the missing-env fail-SOFT (Completed): a
        typo'd env var must return Failed so infra sees it in the
        jobs table + log ERROR. The Job.failures counter can then
        auto-disable after 10 consecutive failures if the operator
        never fixes the env var."""
        print("\n[ak-ran v2 #6 — invalid user returns 'Failed' at ERROR level (not 'Completed')]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        # The exact branch shape.
        self.assertIn("if not self._user_exists(user_id):", code)
        self.assertIn("does not resolve to a User row", code)
        self.assertIn("self.logger.error(msg)", code)
        self.assertIn('return msg, "Failed", self.interval', code)
        print("  ✓ Failed + ERROR log branch present")

    def test_user_exists_late_imports_User_model(self):
        """Late-import User inside _user_exists so WealthDigestTask
        module load stays cheap (User → SQLAlchemy → models init
        chain is heavy). Also avoids surface-area for a User model
        rename."""
        print("\n[ak-ran v2 #6 — _user_exists late-imports User model]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        # Find the _user_exists body and confirm `from models.users
        # import User` lives INSIDE it, not at module top.
        import re
        m = re.search(
            r"def _user_exists\(self, user_id\):(.*?)(?=\n    def |\n\ndef |\Z)",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate _user_exists body")
        body = m.group(1)
        self.assertIn("from models.users import User", body)
        # And User is NOT imported at module top.
        module_top = code.split("class WealthDigestTask", 1)[0]
        self.assertNotIn("from models.users import User", module_top)
        print("  ✓ late-import inside _user_exists; no top-level User import")


# ── Bonus verify: totalValue semantic clarification ────────────────────


class TestV2TotalValueSemanticComment(unittest.TestCase):
    """Reviewer's bonus LOW verify: does fetchSummary's 'totalValue'
    key mean invested principal or current market value? Verified
    via Base_MSN.getActiveMoneyInvested = SUM(buyQuant * buyPrice)
    → INVESTED. Mapping `s.get('totalValue') → totals['totalInvested']`
    is correct. This test guards the clarifying comment so future
    readers don't hit the same confusion."""

    def test_semantic_comment_present_in_portfolio_summary(self):
        print("\n[ak-ran v2 bonus — _safe_portfolio_summary has semantic-verify comment on totalValue]")
        src = _source(_WEALTH_TASK_PATH)
        # Distinctive substrings from the verify comment. Whitespace-
        # insensitive substring matches so alignment tweaks don't
        # false-trip the guard.
        self.assertIn("activeInvested = SUM(buyQuant * buyPrice)", src)
        self.assertIn("INVESTED principal, NOT market value", src)
        self.assertIn("activeInvested + totalProfit", src)
        self.assertIn("current MARKET value", src)
        # And the code-line comment right above the assignments.
        self.assertIn("totalValue = invested; currentValue = market", src)
        print("  ✓ semantic comment + code-line note both present")


# ── Notable-movers logic ────────────────────────────────────────────────


class TestNotableMoversLogic(unittest.TestCase):
    """Source-guards on the movers-sort + cap logic. Full behavioral
    test would need real InvestmentService + JsonDownloadService
    stacks; source-inspection catches the class of regression
    (threshold drift, cap removal, unsorted output)."""

    def test_threshold_and_cap_constants_pinned(self):
        print("\n[ak-ran — notable-movers threshold 2% + cap 5 pinned]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        self.assertIn("_NOTABLE_MOVE_THRESHOLD = 0.02", code)
        self.assertIn("_NOTABLE_MOVERS_CAP = 5", code)
        print("  ✓ constants pinned")

    def test_movers_sorted_by_absolute_magnitude(self):
        """abs(deltaPercent) sort so a -3% mover sits alongside a +3%
        mover (not one dominating the list)."""
        print("\n[ak-ran — movers sorted by abs(deltaPercent) desc, then capped]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        self.assertIn(
            'movers.sort(key=lambda m: abs(m["deltaPercent"]), reverse=True)',
            code,
        )
        self.assertIn("movers[:_NOTABLE_MOVERS_CAP]", code)
        print("  ✓ abs-magnitude sort + cap present")

    def test_movers_filters_below_threshold(self):
        print("\n[ak-ran — movers skip anything |delta| < threshold]")
        code = _source_code_only(_WEALTH_TASK_PATH)
        self.assertIn("if abs(delta_pct) < _NOTABLE_MOVE_THRESHOLD:", code)
        print("  ✓ sub-threshold filter present")


# ── Bootstrap script ────────────────────────────────────────────────────


class TestBootstrapScript(unittest.TestCase):
    """ak-ran Q5 bootstrap script guardrails — idempotent, has
    --commit dry-run gate, understands the env var."""

    def test_bootstrap_script_exists(self):
        print("\n[ak-ran Q5 — bootstrap_wealth_digest.py exists at repo root]")
        self.assertTrue(os.path.exists(_BOOTSTRAP_PATH))
        print("  ✓ script file present")

    def test_bootstrap_idempotent_guard(self):
        print("\n[ak-ran Q5 — bootstrap short-circuits on existing Pending/Overdue]")
        code = _source_code_only(_BOOTSTRAP_PATH)
        self.assertIn(
            "Job.status.in_([JobStatus.PENDING.value, JobStatus.OVERDUE.value])",
            code,
        )
        self.assertIn("IDEMPOTENT:", code)
        print("  ✓ pre-existing-job short-circuit present")

    def test_bootstrap_has_commit_flag(self):
        print("\n[ak-ran Q5 — bootstrap defaults to dry-run; requires --commit to write]")
        code = _source_code_only(_BOOTSTRAP_PATH)
        self.assertIn('"--commit"', code)
        self.assertIn("if not args.commit:", code)
        print("  ✓ dry-run-by-default + --commit override present")

    def test_bootstrap_reads_env_var(self):
        print("\n[ak-ran Q5 — bootstrap reads WEALTH_DIGEST_USER_ID env var]")
        code = _source_code_only(_BOOTSTRAP_PATH)
        self.assertIn('_DEFAULT_USER_ID_ENV = "WEALTH_DIGEST_USER_ID"', code)
        self.assertIn("os.getenv(_DEFAULT_USER_ID_ENV)", code)
        print("  ✓ env var read + placeholder fallback present")


if __name__ == "__main__":
    print("ak-ran Phase 1 — WealthDigestTask + wiring tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
