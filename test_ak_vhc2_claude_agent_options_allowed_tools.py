"""ak-vhc2 SECURITY C-1 (2026-11-23): regression tests for the
"every ClaudeAgentOptions must pass allowed_tools" invariant.

Vulnerability class (pre-ak-vhc2):
    Three ClaudeAgentOptions constructions did not pass
    `allowed_tools=`:
      * services/mailProcessorService.py — CLASSIFICATION agent
      * services/mailProcessorService.py — PDF text-mode agent
      * services/jobEmailService.py — job classifier
    Under `permission_mode='bypassPermissions'` (or 'default'
    with no `can_use_tool` callback), the SDK built-ins (Bash,
    Read, Write, Edit, WebFetch, WebSearch) may be silently
    available. A prompt-injected agent could then shell out /
    read arbitrary files / fetch attacker URLs.

    The in-code comment at agentService.py (pre-ak-vhc2) notes
    "built-in tools are silently unavailable and the agent
    returns zero TextBlocks" when allowed_tools is missing — a
    strong hint that empty-list is the SDK's "no tools"
    semantic. ak-vhc2 defense-in-depth: explicit
    `allowed_tools=[]` so the invariant is unambiguous AND
    forgetting it fails CI (this drift guard).

Fix (ak-vhc2):
    * 3 sites set `allowed_tools=[]` (no tools needed —
      classification + structured-output).
    * Repo-wide AST-walk test:
      `test_all_claude_agent_options_have_allowed_tools`
      walks every `*.py` under services/ utils/ controllers/
      scripts/ and asserts every `ClaudeAgentOptions(...)`
      call has an `allowed_tools=` kwarg.
    * Extended comment at agentService.py explaining the
      invariant + referencing this test.

Related:
    * ak-csng Phase 1 (same mail pipeline, orthogonal fix —
      prompt-injection at the LLM input layer via
      nonce-sentinel sandwich).
    * ak-v3mf m1 (jobEmailService.py, orthogonal fix —
      prompt-injection sentinel on the job-email prompt body).
    * agentService.py:901 chat was AUDITED as already-safe
      (allowed_tools=mcp_tool_names present). No code change
      there; documented in bead comment.

Pin-downs:

Per-site source-grep (3):
    (1) mailProcessorService.py CLASSIFICATION has
        `allowed_tools=[]`.
    (2) mailProcessorService.py PDF text mode has
        `allowed_tools=[]`.
    (3) jobEmailService.py classifier has `allowed_tools=[]`.

Repo-wide invariant (AST):
    (4) Every `ClaudeAgentOptions(...)` call in services/,
        utils/, controllers/, scripts/ passes `allowed_tools`.

Comment-shape pin (5):
    (5) agentService.py comment at the chat-options
        construction references `ak-vhc2` + the drift-guard
        test name — if someone refactors the comment away,
        the invariant explanation is lost.

Drift-guard runtime safety:
    (6) The AST walk is quiet (no false-positives) on the
        current repo state.
"""
from __future__ import annotations

import ast
import os
import pathlib
import re
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_REPO_ROOT = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    with open(_REPO_ROOT / rel_path, "r") as f:
        return f.read()


# ─── Per-site source-grep ─────────────────────────────────────


class TestPerSiteAllowedToolsPresent(unittest.TestCase):
    """Three sites fixed in this bead. Pin each explicitly so a
    future refactor that drops `allowed_tools=[]` on any single
    one surfaces.
    """

    # (1) mailProcessorService.py CLASSIFICATION — explicit empty.
    def test_classification_has_allowed_tools_empty(self):
        src = _read_source("services/mailProcessorService.py")
        # Slice _run_classification_batch body.
        m = re.search(
            r"def _run_classification_batch\(.*?(?=\n    def [a-zA-Z_])",
            src, re.DOTALL,
        )
        self.assertIsNotNone(
            m,
            "_run_classification_batch body not found — shape "
            "changed; review this guard",
        )
        body = m.group(0)
        self.assertIn(
            "allowed_tools=[]", body,
            "ak-vhc2: CLASSIFICATION agent must pass "
            "allowed_tools=[] — no MCP tools wired + "
            "max_turns=1 JSON-only task; any SDK built-in "
            "(Bash/Read/Write/Edit/WebFetch) is a prompt-"
            "injection escalation vector",
        )

    # (2) mailProcessorService.py PDF text mode — explicit empty.
    def test_pdf_text_mode_has_allowed_tools_empty(self):
        src = _read_source("services/mailProcessorService.py")
        # Slice the text_mode_options construction.
        m = re.search(
            r"text_mode_options = ClaudeAgentOptions\("
            r"[\s\S]*?\n            \)",
            src,
        )
        self.assertIsNotNone(
            m,
            "text_mode_options construction not found — shape "
            "changed; review this guard",
        )
        body = m.group(0)
        self.assertIn(
            "allowed_tools=[]", body,
            "ak-vhc2: PDF text-mode agent must pass "
            "allowed_tools=[] — structured-output JSON schema "
            "only, no MCP tools wired; any SDK built-in is an "
            "escalation surface for a prompt-injected PDF body",
        )

    # (3) jobEmailService.py job classifier — explicit empty.
    def test_job_classifier_has_allowed_tools_empty(self):
        src = _read_source("services/jobEmailService.py")
        # Slice the ClaudeAgentOptions construction. There's one
        # ClaudeAgentOptions call in this file (confirmed via
        # the repo-wide walk).
        m = re.search(
            r"options = ClaudeAgentOptions\("
            r"[\s\S]*?\n            \)",
            src,
        )
        self.assertIsNotNone(
            m, "ClaudeAgentOptions call not found in jobEmailService",
        )
        body = m.group(0)
        self.assertIn(
            "allowed_tools=[]", body,
            "ak-vhc2: job-email classifier must pass "
            "allowed_tools=[] — same class as CLASSIFICATION "
            "(max_turns=1 JSON-only); prompt-injected job "
            "email can no longer shell out",
        )


# ─── Repo-wide AST invariant ─────────────────────────────────


class TestAllClaudeAgentOptionsHaveAllowedTools(unittest.TestCase):
    """The core ak-vhc2 invariant: EVERY `ClaudeAgentOptions(...)`
    construction in the backend MUST pass `allowed_tools=` as a
    keyword argument. Catches any future copy-paste of the
    options-construction pattern that forgets the whitelist.

    AST-based (not regex) so multi-line constructions + comment
    noise don't confuse the walker. Scans all `*.py` under
    services/, utils/, controllers/, scripts/ (where ANY
    ClaudeAgentOptions constructions have historically lived).
    """

    SCAN_DIRS = ("services", "utils", "controllers", "scripts")

    def _find_claude_agent_options_calls(self):
        """Yield (rel_path, lineno) for every
        `ClaudeAgentOptions(...)` call in the scanned dirs."""
        for sub in self.SCAN_DIRS:
            base = _REPO_ROOT / sub
            if not base.exists():
                continue
            for py in base.rglob("*.py"):
                try:
                    src = py.read_text()
                except (UnicodeDecodeError, OSError):
                    continue
                try:
                    tree = ast.parse(src, filename=str(py))
                except SyntaxError:
                    # Skip malformed sources; shouldn't happen in
                    # a shipping tree but don't fail the invariant
                    # due to a different file's bug.
                    continue
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    # Match both `ClaudeAgentOptions(...)` and
                    # `foo.ClaudeAgentOptions(...)` shapes.
                    name = None
                    if isinstance(node.func, ast.Name):
                        name = node.func.id
                    elif isinstance(node.func, ast.Attribute):
                        name = node.func.attr
                    if name != "ClaudeAgentOptions":
                        continue
                    rel = py.relative_to(_REPO_ROOT)
                    yield str(rel), node.lineno, node

    # (4) The invariant — every call has allowed_tools.
    def test_every_options_construction_has_allowed_tools(self):
        violations = []
        found_any = False
        for rel, lineno, node in self._find_claude_agent_options_calls():
            found_any = True
            kws = {kw.arg for kw in node.keywords if kw.arg}
            if "allowed_tools" not in kws:
                violations.append(f"{rel}:{lineno}")
        self.assertTrue(
            found_any,
            "ak-vhc2 drift guard found ZERO ClaudeAgentOptions "
            "calls — walker shape broken, review this test",
        )
        self.assertEqual(
            violations, [],
            "ak-vhc2: the following ClaudeAgentOptions "
            "constructions are MISSING the `allowed_tools=` "
            "keyword argument. Every call must pass it (empty "
            f"list `[]` is acceptable). Violations: {violations}",
        )

    # (6) Walker finds the expected number of call sites (sanity
    # against the walker returning zero due to a bug).
    def test_walker_finds_expected_sites(self):
        sites = list(self._find_claude_agent_options_calls())
        # 8+ sites (conservative; ak-vhc2 counted exactly 8 on
        # the current tree — future bedrock additions push this
        # up, not down).
        self.assertGreaterEqual(
            len(sites), 7,
            f"ak-vhc2 drift-guard walker found only {len(sites)} "
            f"sites; expected ≥7 (mailProcessorService 4 + "
            f"jobEmailService 1 + web_extract 1 + cronAgent 1 + "
            f"agentService 1 + WealthDigestTask 1 = 9 at ship "
            f"time). If sites have been legitimately dropped, "
            f"update this floor.",
        )


# ─── Comment-shape pin on agentService.py ─────────────────────


class TestAgentServiceCommentReferencesAkVhc2(unittest.TestCase):
    """The extended comment at agentService.py (around the chat
    ClaudeAgentOptions construction) must reference ak-vhc2 +
    the drift-guard test so the invariant explanation survives
    future refactors."""

    # (5) ak-vhc2 tag + test-name reference present.
    def test_comment_references_bead_and_test(self):
        src = _read_source("services/agentService.py")
        # Slice from "allowed_tools is required" to the NEXT
        # "options = ClaudeAgentOptions(" (distance is comment-
        # size-dependent — stream-of-text match, not fixed-cap).
        start = src.find("allowed_tools is required")
        self.assertGreater(
            start, -1,
            "agentService chat-options comment anchor not found "
            "— shape changed",
        )
        end = src.find("options = ClaudeAgentOptions(", start)
        self.assertGreater(
            end, -1,
            "chat ClaudeAgentOptions construction not found "
            "after the comment anchor",
        )
        chunk = src[start:end]
        self.assertIn(
            "ak-vhc2", chunk,
            "ak-vhc2: agentService comment must reference the "
            "bead ID so a future reader knows why allowed_tools "
            "is required",
        )
        self.assertIn(
            "test_all_claude_agent_options_have_allowed_tools",
            chunk,
            "ak-vhc2: agentService comment must reference the "
            "drift-guard test name so a future refactor that "
            "drops the comment can be traced to the invariant",
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
