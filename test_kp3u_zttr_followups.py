"""ak-kp3u + ak-zttr: BE follow-up tests batched as one landing.

Two sibling follow-ups to prior beads:

  * **ak-kp3u** (follow-up on ak-v55t Instruments search):
    SQL LIKE wildcards (``%``, ``_``) must be escaped in the
    user-supplied ``q`` before concatenation into the LIKE
    pattern. Pre-fix, ``q='50%'`` would match any tradingsymbol
    starting with "50" through anything (accidental wildcard);
    a literal underscore in a scheme name would match any
    single char at that position. Public-data surface + 100-row
    clamp bounded the blast radius, but the semantic was wrong.

  * **ak-zttr** (follow-up on ak-c8l Firebase JWT verify): two
    MINOR fixes:
    - verify-fail WARN log dropped ``str(exc)[:200]`` to close
      the token-fragment-leak surface (SDK exceptions can
      transitively surface token-decode intermediate state).
      Logs only ``type(exc).__name__`` now.
    - Docstring init-failure section corrected: the pre-fix
      claim "prevents a broken deploy from locking every user
      out" was FALSE in `verify_only` + `verify_with_raw_uid_
      fallback` + JWT-input paths (all three result in 401 on
      init-failure, which is the security-correct fail-closed
      behaviour). Docstring now describes the ACTUAL behaviour.

Pure source-grep + AST-lift unit tests. Full module imports pull
Flask + Firebase + Google chain unavailable in CI.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ── AST-lift _escape_like_wildcards for unit tests ───────────────────


def _ast_lift_escape_fn():
    """Lift the module-level helper + the two constants it needs."""
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "KiteService.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)
    kept = []
    want_constants = {"_LIKE_ESCAPE_CHAR", "_LIKE_SPECIAL_CHARS"}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            t = node.targets[0]
            if isinstance(t, ast.Name) and t.id in want_constants:
                kept.append(node)
        if (
            isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef),
            )
            and node.name == "_escape_like_wildcards"
        ):
            node.decorator_list = []
            kept.append(node)
    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)
    ns = {"__name__": "test_kp3u_lift"}
    exec(compile(lift, "<kp3u-lift>", "exec"), ns)
    return ns


_LIFT = _ast_lift_escape_fn()
_escape_like_wildcards = _LIFT["_escape_like_wildcards"]


# ── ak-kp3u (1): _escape_like_wildcards pure logic ───────────────────


class TestEscapeLikeWildcards(unittest.TestCase):

    def test_benign_input_unchanged(self):
        """No wildcards in input → output == input (byte-identical).
        Prevents a scramble-everything regression."""
        self.assertEqual(
            _escape_like_wildcards("RELIANCE"), "RELIANCE",
        )
        self.assertEqual(
            _escape_like_wildcards("HDFC Bank Limited"),
            "HDFC Bank Limited",
        )
        self.assertEqual(
            _escape_like_wildcards("abc123"), "abc123",
        )

    def test_percent_escaped(self):
        """``%`` is SQL-LIKE's any-string wildcard. Must escape."""
        self.assertEqual(_escape_like_wildcards("50%"), "50\\%")
        self.assertEqual(_escape_like_wildcards("%"), "\\%")

    def test_underscore_escaped(self):
        """``_`` is SQL-LIKE's any-single-char wildcard."""
        self.assertEqual(
            _escape_like_wildcards("TAMO_IV"), "TAMO\\_IV",
        )
        self.assertEqual(_escape_like_wildcards("_"), "\\_")

    def test_backslash_escaped_first(self):
        """``\\`` is the escape char itself and must be escaped
        FIRST so later-inserted escapes don't themselves get
        prefixed. ``\\`` → ``\\\\``."""
        self.assertEqual(_escape_like_wildcards("\\"), "\\\\")

    def test_escape_order_backslash_before_wildcards(self):
        """Regression guard against a maintainer swapping
        escape-order (escaping % before \\). If you escape % FIRST
        then \\, the percent's prepended backslash gets itself
        escaped, double-escaping the % → wrong pattern.

        Input ``"a\\b%c"``: expected ``"a\\\\b\\%c"``
          (backslash → \\\\, percent → \\%)
        Wrong-order output: ``"a\\\\\\b\\%c"`` or similar.
        """
        self.assertEqual(
            _escape_like_wildcards("a\\b%c"), "a\\\\b\\%c",
        )

    def test_mixed_wildcards(self):
        self.assertEqual(
            _escape_like_wildcards("10%_off"),
            "10\\%\\_off",
        )

    def test_empty_input_passes_through(self):
        """Empty / None input → passes through unchanged. Caller's
        own empty-check is authoritative (it rejects empty before
        wrapping in LIKE pattern)."""
        self.assertEqual(_escape_like_wildcards(""), "")
        self.assertIsNone(_escape_like_wildcards(None))

    def test_idempotent_via_sql_level_escape(self):
        """Running the escaper on already-escaped output produces
        over-escaped output (escaper is NOT idempotent — it's
        single-pass by design). Pin that behaviour so a future
        maintainer who mistakes it for idempotent doesn't call it
        twice and corrupt the query.

        Single-pass: 50% → 50\\%
        Double-pass: 50\\% → 50\\\\\\% (over-escaped)
        """
        once = _escape_like_wildcards("50%")
        twice = _escape_like_wildcards(once)
        self.assertNotEqual(once, twice)
        # The double-pass over-escapes the backslash.
        self.assertEqual(twice, "50\\\\\\%")


# ── ak-kp3u (2): search_instruments call-site drift guard ────────────


class TestSearchInstrumentsCallSite(unittest.TestCase):
    """Drift guard against regressions in the three ILIKE call
    sites: prefix pass + two substring passes (tradingsymbol + name)."""

    def setUp(self):
        self.src = _read_source("services/KiteService.py")

    def test_q_is_pre_escaped_before_pattern_build(self):
        """``_escape_like_wildcards(q)`` must be called BEFORE the
        prefix/substring patterns are built. A regression that
        drops the escape call re-opens the wildcard-interpretation
        surface."""
        m = re.search(
            r"def search_instruments.*?prefix_pattern\s*=",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertIn(
            "q_escaped = _escape_like_wildcards(q)", body,
            "search_instruments must pre-escape q with "
            "_escape_like_wildcards before building LIKE patterns",
        )

    def test_patterns_use_escaped_q(self):
        """Prefix + substring patterns must concatenate
        ``q_escaped``, not the raw ``q``."""
        m = re.search(
            r"prefix_pattern\s*=\s*f\"[^\"]+\"", self.src,
        )
        self.assertIsNotNone(m)
        self.assertIn("{q_escaped}", m.group(0))

        m = re.search(
            r"substring_pattern\s*=\s*f\"[^\"]+\"", self.src,
        )
        self.assertIsNotNone(m)
        self.assertIn("{q_escaped}", m.group(0))

    def test_all_three_ilike_calls_pass_escape_arg(self):
        """Each ``.ilike(pattern, escape='\\\\')`` call must pass
        the ``escape='\\\\'`` kwarg so SQLAlchemy emits the
        ``LIKE 'pattern' ESCAPE '\\\\'`` clause. Without ESCAPE
        '\\\\', the backslash prefix is interpreted as a literal
        in the pattern and the escape doesn't work."""
        # Prefix pass on tradingsymbol.
        self.assertTrue(
            re.search(
                r"KiteInstrument\.tradingsymbol\.ilike\(\s*\n?"
                r"\s*prefix_pattern,\s*escape=\"\\\\\"",
                self.src,
            ),
            "prefix ilike missing escape='\\\\' kwarg",
        )
        # Substring on tradingsymbol.
        self.assertTrue(
            re.search(
                r"KiteInstrument\.tradingsymbol\.ilike\(\s*\n?"
                r"\s*substring_pattern,\s*escape=\"\\\\\"",
                self.src,
            ),
            "substring tradingsymbol ilike missing escape kwarg",
        )
        # Substring on name.
        self.assertTrue(
            re.search(
                r"KiteInstrument\.name\.ilike\(\s*\n?"
                r"\s*substring_pattern,\s*escape=\"\\\\\"",
                self.src,
            ),
            "substring name ilike missing escape kwarg",
        )


# ── ak-zttr (1): firebase_auth WARN doesn't leak token bytes ─────────


class TestFirebaseAuthWarnLogHygiene(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("utils/firebase_auth.py")

    def test_verify_fail_warn_drops_str_exc(self):
        """ak-zttr MINOR 1: verify-fail WARN log must NOT include
        ``str(exc)`` / ``str(exc)[:N]`` — SDK exception messages
        can transitively surface token-decode intermediate state,
        which would leak token fragments into operator logs.

        Anchors on the verify-block specifically (``try: from
        firebase_admin import auth as fb_auth``) to avoid
        confusing it with the module-level init-failure catch
        (which legitimately logs ``{exc}`` because the exc there
        is about credentials-file errors, NOT token content).
        """
        # Locate the verify-id-token try/except block.
        m = re.search(
            r"try:\s*\n\s*from firebase_admin import auth as fb_auth\s*\n"
            r"\s*decoded = fb_auth\.verify_id_token\(header_value\)\s*\n"
            r"\s*except Exception as exc:(.*?)"
            r"raise FirebaseAuthError\(\s*\n?\s*"
            r'"Firebase ID token verification failed"',
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, "verify-fail except block not found")
        # Strip comments before running the negative-assert — the
        # fix docstring MENTIONS str(exc) in explanatory prose
        # ("NOT ``str(exc)``") which would trip a blanket
        # substring check. Removing ``# ...`` lines focuses the
        # assertion on executable code only.
        raw_block = m.group(1)
        code_only = "\n".join(
            line for line in raw_block.splitlines()
            if not line.lstrip().startswith("#")
        )
        # str(exc) / str(exc)[:N] / exc.args / repr(exc) all risk
        # surfacing token bytes in the actual LOG call. The
        # exception TYPE name is sufficient for operator triage.
        self.assertNotIn(
            "str(exc)", code_only,
            "ak-zttr REGRESSION: verify-fail WARN includes "
            "str(exc) in executable code — token-fragment-leak "
            "risk. Log only type(exc).__name__.",
        )
        self.assertNotIn("repr(exc)", code_only)
        self.assertNotIn("exc.args", code_only)
        # Also no f-string interpolation of {exc} / {exc!r} / {exc!s}.
        self.assertNotIn("{exc}", code_only)
        self.assertNotIn("{exc!r}", code_only)
        self.assertNotIn("{exc!s}", code_only)
        # The WARN must still log the exception class name (so
        # operators can triage without opening a debugger).
        self.assertIn(
            "type(exc).__name__", code_only,
            "verify-fail WARN must still log the exception class "
            "name for operator triage — only str(exc) is "
            "dropped.",
        )


# ── ak-zttr (2): firebase_auth docstring init-failure wording ────────


class TestFirebaseAuthDocstringInitFailure(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("utils/firebase_auth.py")

    def test_pre_fix_claim_absent(self):
        """ak-zttr MINOR 2: pre-fix docstring claimed init-failure
        "prevents a broken deploy from locking every user out".
        That was FALSE: in verify_only + verify_with_raw_uid_
        fallback + JWT-input paths, init-failure → 401 → user IS
        locked out (correctly fail-closed per security).
        Regression guard: the misleading claim must NOT reappear.
        """
        self.assertNotIn(
            "prevents a broken deploy from locking every user out",
            self.src,
            "ak-zttr REGRESSION: pre-fix misleading claim about "
            "init-failure behaviour is back. The actual behaviour "
            "is fail-closed on the JWT path in both prod modes.",
        )

    def test_corrected_wording_present(self):
        """Docstring now explains the ACTUAL behaviour: fail-closed
        for JWT-shaped headers across both prod modes; only the
        rollout-cycle bare-UID path tolerates init failure."""
        self.assertIn("Init-failure behaviour (fail-closed", self.src)
        # Must enumerate each mode's actual behaviour.
        self.assertIn("verify_only", self.src)
        self.assertIn("verify_with_raw_uid_fallback", self.src)
        self.assertIn("raw_uid_only", self.src)
        # The correct-for-security framing — "fail-closed is
        # CORRECT in prod mode" — should appear.
        self.assertRegex(
            self.src,
            r"CORRECT fail-closed behaviour|correct fail-closed|"
            r"fail-closed.*?correct",
        )


if __name__ == "__main__":
    unittest.main()
