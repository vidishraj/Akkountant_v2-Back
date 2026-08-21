"""ak-6p4: unit tests for services.wealthDigestService.parse_digest_content.

Pure-function guard for the Wave 3 storage-format shape (structured JSON)
vs pre-Wave-3 legacy shape (pure markdown). Endpoint depends on this
helper to render both cleanly in the new component-based FE layout.

AST-lifts the helper from services/wealthDigestService.py so the test
runs in bare env without pulling the services.Base_Service / google.oauth2
/ firebase chain. Same pattern as test_wealth_digest_migration_gap.py.

Run:
    python3 -m unittest test_wealth_digest_parse_content
    python3 -m pytest test_wealth_digest_parse_content.py
"""

import ast
import json
import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _ast_lift_parse_digest_content():
    """AST-lift the parse_digest_content helper + _STRUCTURED_KEYS
    constant from services/wealthDigestService.py, stripping the
    heavy services.Base_Service / models.* imports."""
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "wealthDigestService.py",
    )
    with open(src_path) as fh:
        src = fh.read()
    tree = ast.parse(src)
    kept = []
    for node in tree.body:
        # Keep `from __future__ import annotations` so PEP 604 union
        # types (`str | None`) in the function signature don't
        # evaluate at import time (parser lifts run on Python 3.9).
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            kept.append(node)
            continue
        # Keep `import json` since parse_digest_content uses it.
        if isinstance(node, ast.Import):
            if any(alias.name == "json" for alias in node.names):
                kept.append(node)
            continue
        # Keep the _STRUCTURED_KEYS constant.
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id == "_STRUCTURED_KEYS":
                kept.append(node)
                continue
        # Keep the parse_digest_content function.
        if isinstance(node, ast.FunctionDef) and node.name == "parse_digest_content":
            kept.append(node)
    tree.body = kept
    ns = {"__name__": "test_wealth_digest_parse_content_ast_lift"}
    exec(compile(tree, "<parse_digest_content-AST-lift>", "exec"), ns)
    return ns["parse_digest_content"]


try:
    try:
        from services.wealthDigestService import parse_digest_content  # noqa: F401
    except Exception:
        parse_digest_content = _ast_lift_parse_digest_content()
    _IMPORT_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _IMPORT_OK = False
    _SKIP_REASON = f"import chain unavailable: {_exc}"


class TestImportChannel(unittest.TestCase):
    """ak-6p4 v2 (reviewer MINOR-1): un-decorated guard that fails
    LOUDLY if both real-import AND AST-lift channels break. Without
    this, `@unittest.skipUnless(_IMPORT_OK, ...)` on the behavioral
    class would SKIP every test and unittest would still report OK
    — a regression could ship green. Recurring AST-lift pattern
    across test files; codify the guard now that it's the third
    instance."""

    def test_import_channel_is_live(self):
        self.assertTrue(_IMPORT_OK, _SKIP_REASON)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestParseDigestContent(unittest.TestCase):
    """Cover the 5 branches of parse_digest_content — every one of which
    is a real path an on-disk AgentMessage.content can land in."""

    def test_none_input_returns_all_empty(self):
        out = parse_digest_content(None)
        self.assertEqual(out, {
            "text": "", "actions": [], "watch_items": [], "news": [],
        })

    def test_empty_string_returns_all_empty(self):
        out = parse_digest_content("")
        self.assertEqual(out, {
            "text": "", "actions": [], "watch_items": [], "news": [],
        })

    def test_valid_structured_json_returns_typed(self):
        raw = json.dumps({
            "text": "[Personal use — not investment advice]\n\nBody text here.",
            "actions": [
                {"title": "Rebalance", "detail": "…",
                 "category": "cash", "priority": "high"},
            ],
            "watch_items": [
                {"title": "Equity concentration", "detail": "…",
                 "severity": "warning"},
            ],
            "news": [
                {"headline": "Reliance Q1 strong", "detail": "…",
                 "related_holdings": ["Reliance"]},
            ],
        })
        out = parse_digest_content(raw)
        self.assertTrue(out["text"].startswith("[Personal use"))
        self.assertEqual(len(out["actions"]), 1)
        self.assertEqual(out["actions"][0]["category"], "cash")
        self.assertEqual(len(out["watch_items"]), 1)
        self.assertEqual(out["watch_items"][0]["severity"], "warning")
        self.assertEqual(len(out["news"]), 1)
        self.assertEqual(out["news"][0]["related_holdings"], ["Reliance"])

    def test_structured_missing_arrays_fill_empty(self):
        """If sonnet emits a schema-invalid response that STILL passes
        the has-text-and-one-array check (edge case: text + only
        actions, no watch_items / news), the parser should fill the
        missing arrays with [] instead of KeyError'ing downstream."""
        raw = json.dumps({
            "text": "Body",
            "actions": [
                {"title": "A", "detail": "d",
                 "category": "cash", "priority": "low"},
            ],
        })
        out = parse_digest_content(raw)
        self.assertEqual(out["text"], "Body")
        self.assertEqual(len(out["actions"]), 1)
        self.assertEqual(out["watch_items"], [])
        self.assertEqual(out["news"], [])

    def test_legacy_pure_markdown_wraps_as_text(self):
        """Pre-Wave-3 digests were pure markdown starting with the
        [Personal use…] header. Parser wraps them as text-only with
        empty structured arrays so the FE renders them cleanly in the
        new component layout."""
        raw = (
            "[Personal use — not investment advice]\n\n"
            "## Portfolio-level pulse\n\n"
            "Day change ₹1,25,000 (+0.5%).\n"
        )
        out = parse_digest_content(raw)
        self.assertEqual(out["text"], raw)
        self.assertEqual(out["actions"], [])
        self.assertEqual(out["watch_items"], [])
        self.assertEqual(out["news"], [])

    def test_malformed_json_falls_back_to_legacy_wrap(self):
        """A raw payload that LOOKS JSON-ish (starts with {) but doesn't
        parse must fall back to legacy wrap — never raise."""
        raw = "{this is not valid JSON, actions: [broken}"
        out = parse_digest_content(raw)
        self.assertEqual(out["text"], raw)  # legacy wrap
        self.assertEqual(out["actions"], [])
        self.assertEqual(out["watch_items"], [])
        self.assertEqual(out["news"], [])

    def test_array_top_level_falls_back_to_legacy(self):
        """ak-6p4 v2 (reviewer MINOR-3): a top-level JSON ARRAY (e.g.
        `[1,2,3]`) is valid JSON but not the digest object shape. The
        `{`-gate rejects it before json.loads is even attempted, so it
        falls through to the legacy wrap. Test pins this behavior."""
        raw = "[1,2,3]"
        out = parse_digest_content(raw)
        self.assertEqual(out["text"], raw)  # legacy wrap
        self.assertEqual(out["actions"], [])
        self.assertEqual(out["watch_items"], [])
        self.assertEqual(out["news"], [])

    def test_json_object_without_text_falls_back_to_legacy(self):
        """A valid JSON object that lacks `text` isn't a Wave 3 digest
        — could be a different serialization someone stored by mistake.
        Falls back to legacy wrap (raw goes into text)."""
        raw = json.dumps({"some_other_field": "x", "actions": []})
        out = parse_digest_content(raw)
        self.assertEqual(out["text"], raw)  # legacy wrap
        self.assertEqual(out["actions"], [])
        self.assertEqual(out["watch_items"], [])
        self.assertEqual(out["news"], [])

    def test_arrays_of_wrong_type_normalize_to_empty(self):
        """Defense against SDK edge cases where a field arrives as a
        non-list (dict, string, null). Parser normalizes to []."""
        raw = json.dumps({
            "text": "Body",
            "actions": "should have been a list",
            "watch_items": None,
            "news": {"nested": "dict"},
        })
        out = parse_digest_content(raw)
        self.assertEqual(out["text"], "Body")
        self.assertEqual(out["actions"], [])
        self.assertEqual(out["watch_items"], [])
        self.assertEqual(out["news"], [])


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
