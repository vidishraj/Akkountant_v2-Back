"""ak-b7vn (2026-11-24): agent preamble leaked into the Wealth Digest
summary (Home digest card + /wealth-digest).

Cause: the digest `text` must start with the personal-use header. When the
model wrote a lead-in first ("I'll research…"), the task PREPENDED a header
(it only checked `startswith`), so the lead-in survived, sandwiched as
`HEADER <lead-in> HEADER <body>`, and the read side served it verbatim.

Fix (backend, at the source; no FE trimming): anchor on the header.
  * write side (WealthDigestTask) drops anything before the model's own
    header before persisting, logging a CHARACTER COUNT only;
  * read side (parse_digest_content) applies the same cut, so digests
    already stored with a lead-in render clean too;
  * the prompt forbids lead-ins/process narration in `text`.
With no header at all there is no safe anchor: text is left unchanged.
All fixture text is synthetic.
"""
from __future__ import annotations

import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils.digest_text import (  # noqa: E402
    DIGEST_HEADER as H, preamble_len, strip_digest_preamble,
)

LEAD = "I'll research your top holdings first, then summarise."
BODY = "\nPortfolio pulse: synthetic body.\n_Rates as of X • Snapshot as of Y_"


class TestStripHelper(unittest.TestCase):

    def test_lead_in_before_header_dropped(self):
        t = f"{LEAD}\n\n{H}{BODY}"
        self.assertEqual(strip_digest_preamble(t), H + BODY)
        self.assertEqual(preamble_len(t), len(LEAD) + 2)

    def test_prefix_era_sandwich_cleaned(self):
        # Exactly what the old prepend produced and stored.
        t = f"{H}\n\n{LEAD}\n\n{H}{BODY}"
        self.assertEqual(strip_digest_preamble(t), H + BODY)
        self.assertGreater(preamble_len(t), len(LEAD))

    def test_adjacent_duplicate_header_collapsed(self):
        self.assertEqual(strip_digest_preamble(f"{H}\n\n{H}{BODY}"), H + BODY)

    def test_clean_digest_untouched(self):
        t = H + BODY
        self.assertEqual(strip_digest_preamble(t), t)
        self.assertEqual(preamble_len(t), 0)

    def test_no_header_untouched_no_guessing(self):
        t = f"{LEAD}\n\nsynthetic body without header"
        self.assertEqual(strip_digest_preamble(t), t)
        self.assertEqual(preamble_len(t), 0)

    def test_degenerate_inputs(self):
        for v in ("", None, 5):
            self.assertEqual(strip_digest_preamble(v), v)
            self.assertEqual(preamble_len(v), 0)

    def test_header_single_sourced(self):
        # review MINOR-2: the task must not keep its own copy.
        import ast
        with open(os.path.join(HERE, "services", "tasks", "WealthDigestTask.py")) as f:
            tree = ast.parse(f.read())
        node = next(n for n in tree.body if isinstance(n, ast.Assign)
                    and getattr(n.targets[0], "id", None) == "_HEADER")
        self.assertEqual(ast.unparse(node.value), "DIGEST_HEADER")
        imports = [n for n in tree.body if isinstance(n, ast.ImportFrom)
                   and n.module == "utils.digest_text"]
        self.assertTrue(any(a.name == "DIGEST_HEADER"
                            for n in imports for a in n.names))

    # ── review MAJOR probes: must FAIL SAFE (0 chars dropped) ──

    def _assert_untouched(self, t):
        self.assertEqual(strip_digest_preamble(t), t)
        self.assertEqual(preamble_len(t), 0)

    def test_trailing_disclaimer_repeating_header_untouched(self):
        body = "Portfolio pulse paragraph. " * 20
        self._assert_untouched(f"{H}\n{body}\n\n{H}")

    def test_quoted_header_mid_body_untouched(self):
        body = "Narrative paragraph one. " * 25
        self._assert_untouched(
            f"{H}\n{body}\n\nNote: the line \"{H}\" is shown on "
            f"every digest.\n\n{body}")

    def test_long_prefix_untouched(self):
        self._assert_untouched(("long prose " * 50) + H + BODY)

    def test_prefix_over_length_limit_untouched(self):
        # 450-char single paragraph, far less than half the text: only
        # the MAX_LEAD_IN_CHARS bound applies.
        self._assert_untouched(("q" * 450) + H + (BODY * 60))

    def test_multi_paragraph_prefix_untouched(self):
        self._assert_untouched(
            f"First paragraph.\n\nSecond paragraph.\n{H}" + BODY * 3)

    def test_prefix_that_is_most_of_the_text_untouched(self):
        # 300-char single-paragraph prefix (within the 400 limit) that is
        # most of a 339-char text: only the "not the bulk" rule applies.
        self._assert_untouched(("p" * 300) + H + "x")

    def test_long_sandwich_middle_untouched(self):
        mid = "real content " * 40
        self._assert_untouched(f"{H}\n{mid}\n{H}{BODY}")


class TestReadSideCleansStoredDigests(unittest.TestCase):
    """Rows stored before the fix must render clean."""

    def setUp(self):
        from test_wealth_digest_parse_content import parse_digest_content
        self.parse = parse_digest_content

    def test_structured_row_with_sandwich(self):
        raw = json.dumps({"text": f"{H}\n\n{LEAD}\n\n{H}{BODY}",
                          "actions": [], "watch_items": [], "news": []})
        self.assertEqual(self.parse(raw)["text"], H + BODY)

    def test_structured_row_with_lead_in(self):
        raw = json.dumps({"text": f"{LEAD}\n{H}{BODY}", "actions": []})
        self.assertEqual(self.parse(raw)["text"], H + BODY)

    def test_legacy_markdown_row_with_lead_in(self):
        self.assertEqual(self.parse(f"{LEAD}\n\n{H}{BODY}")["text"], H + BODY)

    def test_prod_msg285_shape(self):
        # Measured on prod (infra, flags only): stored as plain text (not
        # JSON), a 152-char single-line prose lead-in with NO newline,
        # then the only header. That is concatenated TextBlocks.
        lead = ("x" * 151) + "."
        body = BODY + ("\nSynthetic paragraph. " * 200)
        raw = f"{lead}{H}{body}"
        raw = raw[:2756]  # prod row length (infra: len 2756)
        self.assertFalse(raw.startswith("{"))
        self.assertEqual(len(lead), 152)
        self.assertEqual(len(raw), 2756)
        self.assertEqual(self.parse(raw)["text"], raw[152:])

    def test_prod_msg281_shape_is_normal_structured_storage(self):
        # Header at char 10 inside a '{'-prefixed row is just the JSON
        # wrapper: `{"text": "` is 10 chars. Not a leak; unwraps clean.
        # The task persists with ensure_ascii=False (literal em-dash),
        # which is why the header was found verbatim at char 10.
        raw = json.dumps({"text": H + BODY, "actions": []},
                         ensure_ascii=False)
        self.assertEqual(raw.find(H), 10)
        self.assertEqual(self.parse(raw)["text"], H + BODY)

    def test_clean_rows_unchanged(self):
        raw = json.dumps({"text": H + BODY, "actions": [{"title": "a"}]})
        out = self.parse(raw)
        self.assertEqual(out["text"], H + BODY)
        self.assertEqual(out["actions"], [{"title": "a"}])


try:
    import test_ak_lwed_digest_turn_budget as _lw
    _HAVE_HARNESS = _lw._HAVE_SDK
except Exception:  # pragma: no cover
    _HAVE_HARNESS = False


@unittest.skipUnless(_HAVE_HARNESS, "claude_agent_sdk/anyio not installed")
class TestWriteSideStripsBeforePersist(unittest.TestCase):
    """Drive WealthDigestTask._invoke_agent through the ak-lwed stubbed
    harness with a model `text` that carries a lead-in."""

    def setUp(self):
        self.h = _lw.TestDigestTurnBudgetAndFallback(methodName="_invoke")
        self.h.setUp()

    def tearDown(self):
        self.h.tearDown()

    def _run(self, text):
        digest = {"text": text, "actions": [], "watch_items": [],
                  "news": [{"title": "n"}]}
        with self.h._script([self.h._result(digest)]):
            with self.assertLogs(self.h.fake_self.logger, level="INFO") as cm:
                out = json.loads(self.h._invoke())
        return out, "\n".join(cm.output)

    def test_lead_in_stripped_and_only_a_count_logged(self):
        out, logs = self._run(f"{LEAD}\n\n{H}{BODY}")
        self.assertEqual(out["text"], H + BODY)
        self.assertIn(f"stripped {len(LEAD) + 2}-char preamble", logs)
        self.assertNotIn("research your top holdings", logs)

    def test_missing_header_still_prepended_once(self):
        out, _ = self._run("synthetic body only")
        self.assertEqual(out["text"], f"{H}\n\nsynthetic body only")
        self.assertEqual(out["text"].count(H), 1)

    def test_prompt_forbids_lead_in(self):
        m = self.h.m
        prompt = m._build_system_prompt(*m._make_user_data_tags())
        self.assertIn("nothing before it", prompt)
        self.assertIn("lead-in", prompt)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
