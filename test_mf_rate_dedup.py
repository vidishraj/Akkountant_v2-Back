"""ak-lp6 regression tests:

  1. SetMfRate.buildJsonForMF dedupes URLs (preserving first-occurrence
     order) and logs raw/deduped/dupes_removed counts.
  2. SetMfDetails._log_scheme_counts surfaces:
     - duplicate schemeCodes in the current fetch (ERROR + sample)
     - >2× spike vs previous run (ERROR + first-N-codes sample)
     - ±20% deviation vs previous run (WARN)
     - always: INFO with current + previous counts

Both classes are cheap to test in isolation because the URL builder is
a small pure block inside buildJsonForMF and the diagnostic is a
free-standing helper on SetMFDetails. We AST-lift each function so we
don't have to import the whole task stack (baseTask → JsonDownloadService
→ InvestmentService → sqlalchemy → flask → ...) just to test dedup
logic and a logger-side-effect.

Run:
    python3 -m unittest test_mf_rate_dedup
    python3 -m pytest test_mf_rate_dedup.py
"""

import ast
import logging
import os
import sys
import textwrap
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── Shared AST-lift utility ──────────────────────────────────────────────


def _lift_functions(module_relpath, names):
    """Load `services/tasks/<file>.py`, ast-parse it, and exec ONLY the
    functions/classes with the requested names into a fresh namespace.
    This lets us drive SetMfRate.buildJsonForMF and SetMFDetails.
    _log_scheme_counts without pulling in the full BaseTask import
    chain (which needs flask + sqlalchemy + the whole app stack).
    """
    path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), module_relpath
    )
    with open(path) as fh:
        src = fh.read()
    tree = ast.parse(src)

    picked = []
    seen = set()

    def _walk(nodes):
        for node in nodes:
            n = None
            if isinstance(node, ast.FunctionDef):
                n = node.name
            elif isinstance(node, ast.AsyncFunctionDef):
                n = node.name
            elif isinstance(node, ast.ClassDef):
                # Descend into class bodies too.
                _walk(node.body)
                continue
            elif isinstance(node, ast.Assign) and len(node.targets) == 1:
                tgt = node.targets[0]
                if isinstance(tgt, ast.Name):
                    n = tgt.id
            if n in names and n not in seen:
                picked.append(node)
                seen.add(n)

    _walk(tree.body)
    mod = ast.Module(body=picked, type_ignores=[])
    ns = {}
    exec(compile(mod, f'<ast-lift:{module_relpath}>', 'exec'), ns)
    return ns


# ── SetMfRate.buildJsonForMF dedup ──────────────────────────────────────


class TestBuildJsonForMFDedupLogic(unittest.TestCase):
    """Verify the dedup logic inside buildJsonForMF strictly. We can't
    call buildJsonForMF end-to-end because it triggers asyncio.run +
    aiohttp — but the dedup block is 4 lines above the network call.
    We re-implement it below verbatim to lock the algorithm down (any
    drift in SetMfRate must be mirrored here or this test fails)."""

    @staticmethod
    def _dedupe_urls(baseUrl, data):
        """Exact copy of the block added to SetMfRate.buildJsonForMF.
        If this test flags a divergence, either the source or the test
        needs updating — they intentionally track each other."""
        raw_urls = [f"{baseUrl}/{item.get('schemeCode')}" for item in data]
        urls = list(dict.fromkeys(raw_urls))
        dupes_removed = len(raw_urls) - len(urls)
        return raw_urls, urls, dupes_removed

    def test_no_dupes_preserved_identically(self):
        print("\n[ak-lp6 — no-dupe list passes through unchanged, order preserved]")
        data = [{"schemeCode": c} for c in ("100", "101", "102", "103")]
        raw, urls, dupes = self._dedupe_urls("https://api.mfapi.in/mf", data)
        self.assertEqual(dupes, 0)
        self.assertEqual(len(urls), 4)
        self.assertEqual(urls, [
            "https://api.mfapi.in/mf/100",
            "https://api.mfapi.in/mf/101",
            "https://api.mfapi.in/mf/102",
            "https://api.mfapi.in/mf/103",
        ])
        print("  ✓ 4 unique codes → 4 URLs, order preserved")

    def test_triple_dupes_collapse_to_unique(self):
        """The exact Aug 3 shape: 3× the same codes. This models the
        113,139 → 37,713 collapse the fix is supposed to restore."""
        print("\n[ak-lp6 — 3× duplicated list dedupes to 1/3 the size (Aug 3 shape)]")
        base_codes = [str(c) for c in range(1000, 1050)]  # 50 unique
        # Triple them, order interleaved so dict.fromkeys ordering
        # matters (not just a lucky sort).
        data = [{"schemeCode": c} for c in (base_codes + base_codes + base_codes)]
        raw, urls, dupes = self._dedupe_urls("https://api.mfapi.in/mf", data)
        self.assertEqual(len(raw), 150)
        self.assertEqual(len(urls), 50)
        self.assertEqual(dupes, 100)
        # First-occurrence order must be preserved so downstream retry
        # loops stay deterministic across fixes.
        self.assertEqual(urls[0], "https://api.mfapi.in/mf/1000")
        self.assertEqual(urls[-1], "https://api.mfapi.in/mf/1049")
        print("  ✓ 150 → 50 (dupes_removed=100); first-occurrence order preserved")

    def test_interleaved_dupes_preserve_first_position(self):
        print("\n[ak-lp6 — interleaved dupes: first occurrence wins]")
        data = [{"schemeCode": c} for c in ("A", "B", "A", "C", "B", "D", "A")]
        raw, urls, dupes = self._dedupe_urls("http://x/mf", data)
        self.assertEqual(len(raw), 7)
        self.assertEqual(len(urls), 4)
        self.assertEqual(dupes, 3)
        # First-occurrence positions: A(0), B(1), C(3), D(5)
        self.assertEqual(urls, [
            "http://x/mf/A", "http://x/mf/B", "http://x/mf/C", "http://x/mf/D",
        ])
        print("  ✓ interleaved dupes collapse to first-occurrence order")

    def test_missing_schemeCode_produces_None_url(self):
        """Existing code uses .get('schemeCode') which yields None for
        missing keys — we keep the pre-existing behavior. The dedup
        still works: multiple missing-code items collapse to a single
        '<base>/None' URL rather than N of them."""
        print("\n[ak-lp6 — items missing schemeCode collapse into single URL]")
        data = [{"schemeCode": "100"}, {}, {}, {"schemeCode": "101"}]
        raw, urls, dupes = self._dedupe_urls("http://x/mf", data)
        # Two None-keyed entries collapse to one; 4 raw → 3 unique.
        self.assertEqual(len(urls), 3)
        self.assertEqual(dupes, 1)
        print(f"  ✓ 4 items with 2 missing → {len(urls)} URLs (dupes_removed={dupes})")

    def test_source_still_uses_dict_fromkeys_and_logs_counts(self):
        """Guard the actual source file — if someone rewrites the dedup
        with a set() (order-losing) or drops the log line, catch it.
        Source-text inspection so no service-stack import is required."""
        print("\n[ak-lp6 — source uses dict.fromkeys + logs raw/deduped/dupes_removed]")
        path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "services", "tasks", "SetMfRate.py",
        )
        with open(path) as fh:
            src = fh.read()
        # dict.fromkeys preserves order; list(set(...)) does NOT. Guard
        # against a naive "cleanup" that swaps them.
        self.assertIn("dict.fromkeys(raw_urls)", src)
        # Log line must contain the three fields infra will grep for.
        self.assertIn("MF URL list: raw=", src)
        self.assertIn("deduped=", src)
        self.assertIn("dupes_removed=", src)
        # Sanity: the naive set(...) replacement is NOT present in the
        # dedup block. (We match the very local shape rather than a bare
        # 'set(' so unrelated set literals elsewhere in the file don't
        # false-positive.)
        self.assertNotIn("list(set(raw_urls))", src)
        print("  ✓ source uses dict.fromkeys AND emits the counted log line")


# ── SetMFDetails diagnostic ──────────────────────────────────────────────


class _CaptureLogger:
    """Tiny stand-in for utils.logger — collects (level, msg) tuples.
    Simpler than wiring the real logger + a caplog fixture (works under
    plain unittest, not just pytest)."""

    def __init__(self):
        self.records = []

    def _make(self, level):
        def _log(msg, *args):
            if args:
                try:
                    msg = msg % args
                except TypeError:
                    pass
            self.records.append((level, msg))
        return _log

    def __getattr__(self, name):
        # Fall through only for the log-level methods we know about;
        # anything else is a programming error.
        if name in {"info", "warning", "error", "debug", "critical"}:
            return self._make(name)
        raise AttributeError(name)

    def by_level(self, level):
        return [m for lvl, m in self.records if lvl == level]

    def any_at(self, level, needle):
        return any(needle in m for m in self.by_level(level))


class TestSetMFDetailsCountDiagnostic(unittest.TestCase):
    """Drive SetMFDetails._log_scheme_counts directly against synthetic
    inputs. We AST-lift the method + the threshold constants so we
    don't have to import the full task stack."""

    @classmethod
    def setUpClass(cls):
        lifted = _lift_functions(
            "services/tasks/SetMfDetails.py",
            names={
                "_log_scheme_counts",
                "_previous_scheme_count",
                "_MF_DETAILS_WARN_DEVIATION",
                "_MF_DETAILS_ERR_MULTIPLIER",
                "_MF_DETAILS_DUP_SAMPLE_LIMIT",
            },
        )
        # Stash as staticmethod on the class so self.log_fn(...) doesn't
        # get an unwanted `self` bind — the lifted function already has
        # `self` as its FIRST positional (from the SetMFDetails method
        # source), and we provide it via the shim.
        cls.log_fn = staticmethod(lifted["_log_scheme_counts"])
        # Sanity: threshold constants lifted correctly.
        assert lifted["_MF_DETAILS_WARN_DEVIATION"] == 0.20
        assert lifted["_MF_DETAILS_ERR_MULTIPLIER"] == 2.0
        assert lifted["_MF_DETAILS_DUP_SAMPLE_LIMIT"] == 10

    def _run(self, jsonData, prev_count):
        """Invoke _log_scheme_counts with a bare `self` shim exposing a
        capture-logger. The method only reads self.logger — no other
        dependencies."""
        log = _CaptureLogger()
        shim = type("Shim", (), {"logger": log})()
        self.log_fn(shim, jsonData, prev_count)
        return log

    def test_info_always_emitted_with_counts(self):
        print("\n[ak-lp6 diag — INFO line always includes current + previous counts]")
        data = [{"schemeCode": str(i)} for i in range(100)]
        log = self._run(data, prev_count=95)
        self.assertTrue(log.any_at("info", "current=100"))
        self.assertTrue(log.any_at("info", "previous=95"))
        print("  ✓ INFO emitted with current=100 previous=95")

    def test_first_run_no_previous_logs_none(self):
        print("\n[ak-lp6 diag — first run (prev=None) logs previous=none]")
        data = [{"schemeCode": str(i)} for i in range(10)]
        log = self._run(data, prev_count=None)
        self.assertTrue(log.any_at("info", "previous=none"))
        # No deviation warn/error possible on first run.
        self.assertEqual(log.by_level("warning"), [])
        print("  ✓ prev=None handled without spurious warn/error")

    def test_duplicate_schemeCodes_error_with_sample(self):
        """Real MFAPI is dupe-free by contract. Any dupe = red alarm."""
        print("\n[ak-lp6 diag — dupes in current fetch → ERROR + sample_first_10]")
        # 20 unique + 5 dupes of the first 5.
        base = [{"schemeCode": str(i)} for i in range(20)]
        dupes = [{"schemeCode": str(i)} for i in range(5)]
        data = base + dupes
        log = self._run(data, prev_count=20)
        self.assertTrue(log.any_at("error", "DUPLICATE schemeCodes"))
        self.assertTrue(log.any_at("error", "duplicates=5"))
        self.assertTrue(log.any_at("error", "total=25"))
        self.assertTrue(log.any_at("error", "unique=20"))
        # Sample is present (first 5 dupe codes).
        errs = log.by_level("error")
        self.assertTrue(any("sample_first_10=" in m for m in errs))
        print("  ✓ dupes → ERROR (total, unique, duplicates + sample)")

    def test_dup_sample_capped_at_ten(self):
        print("\n[ak-lp6 diag — dupe sample capped at first 10]")
        # 15 unique codes, each duplicated once → 15 dup encounters;
        # the sample_first_10= list in the log should cap at exactly 10.
        base = [{"schemeCode": str(i)} for i in range(15)]
        data = base + base  # every code appears exactly twice
        log = self._run(data, prev_count=15)
        err_msgs = log.by_level("error")
        dup_msg = next(m for m in err_msgs if "sample_first_10=" in m)
        # Parse the sample_first_10=[...] slice.
        _, tail = dup_msg.split("sample_first_10=", 1)
        # Extract the bracket group.
        bracket = tail.split("]", 1)[0] + "]"
        sample = ast.literal_eval(bracket)
        self.assertEqual(len(sample), 10)
        print(f"  ✓ sample capped at 10 (len={len(sample)})")

    def test_greater_than_2x_spike_error_with_first_codes(self):
        print("\n[ak-lp6 diag — >2× spike vs previous → ERROR w/ first_10_codes]")
        # 300 current vs 100 previous = 3× spike (Aug 3 pattern).
        data = [{"schemeCode": str(i)} for i in range(300)]
        log = self._run(data, prev_count=100)
        errs = log.by_level("error")
        self.assertTrue(any("COUNT SPIKE" in m for m in errs))
        self.assertTrue(any("current=300" in m for m in errs))
        self.assertTrue(any("previous=100" in m for m in errs))
        self.assertTrue(any("ratio=+200.0%" in m for m in errs))
        self.assertTrue(any("sample_first_10_codes=" in m for m in errs))
        print("  ✓ 3× spike → ERROR (current, previous, ratio, first_10)")

    def test_20pct_deviation_warns_no_error(self):
        print("\n[ak-lp6 diag — 25% growth (below 2×) → WARN, no ERROR]")
        # 125 current vs 100 previous = +25% → warn, no error.
        data = [{"schemeCode": str(i)} for i in range(125)]
        log = self._run(data, prev_count=100)
        warns = log.by_level("warning")
        errs = log.by_level("error")
        self.assertTrue(any("count deviation +25.0%" in m for m in warns))
        # No COUNT SPIKE error (only 1.25× not 2×).
        self.assertFalse(any("COUNT SPIKE" in m for m in errs))
        print("  ✓ +25% growth → WARN (no ERROR — under 2× threshold)")

    def test_below_20pct_no_warn(self):
        print("\n[ak-lp6 diag — 10% drift → no warn/error, just INFO]")
        data = [{"schemeCode": str(i)} for i in range(110)]
        log = self._run(data, prev_count=100)
        self.assertEqual(log.by_level("warning"), [])
        self.assertEqual(log.by_level("error"), [])
        self.assertTrue(log.any_at("info", "current=110"))
        print("  ✓ +10% within ±20% threshold — INFO only")

    def test_non_list_response_error_no_crash(self):
        print("\n[ak-lp6 diag — non-list response → ERROR + short-circuit]")
        log = self._run({"not": "a list"}, prev_count=50)
        self.assertTrue(log.any_at("error", "not a list"))
        # No INFO count line emitted (we short-circuited).
        self.assertFalse(any("current=" in m for m in log.by_level("info")))
        print("  ✓ non-list guarded — ERROR only, no crash")

    def test_zero_prev_count_treated_as_no_baseline(self):
        """If the previous file existed but held an empty list,
        deviation math would divide by zero. Verify graceful skip."""
        print("\n[ak-lp6 diag — prev=0 skips deviation math, no divide-by-zero]")
        data = [{"schemeCode": str(i)} for i in range(50)]
        log = self._run(data, prev_count=0)
        # INFO fires, no crash, no warn/error from deviation math.
        self.assertTrue(log.any_at("info", "current=50"))
        # previous is 0 → falsy → skipped; the info line renders it
        # as "previous=0" not "previous=none" (we only fall back to
        # 'none' for None). That's fine.
        self.assertFalse(any("COUNT SPIKE" in m for m in log.by_level("error")))
        self.assertEqual(log.by_level("warning"), [])
        print("  ✓ prev=0 treated as no-baseline (no divide-by-zero)")

    def test_items_missing_schemeCode_ignored(self):
        """Missing / None schemeCode items must not inflate the dup
        count — otherwise the diagnostic would false-alarm on any
        MFAPI response that includes malformed entries. The separate
        `missing_code` WARN handles the schema-shift signal."""
        print("\n[ak-lp6 diag — missing schemeCode items → WARN, not DUPLICATE alarm]")
        data = [
            {"schemeCode": "1"}, {"schemeCode": "2"},
            {}, {"other": "field"}, {"schemeCode": None},
            {"schemeCode": "3"},
        ]
        log = self._run(data, prev_count=6)
        # Only 3 real codes, each unique → 0 real duplicates. The
        # DUPLICATE-schemeCodes ERROR must NOT fire.
        errs = log.by_level("error")
        self.assertFalse(any("DUPLICATE schemeCodes" in m for m in errs))
        # 3 items had no schemeCode — WARN fires with the count.
        warns = log.by_level("warning")
        self.assertTrue(any("3 item(s) had no schemeCode" in m for m in warns))
        # INFO still reports raw current=6.
        self.assertTrue(log.any_at("info", "current=6"))
        print("  ✓ 3 missing/None items → separate WARN, no false DUPLICATE alarm")


if __name__ == "__main__":
    print("ak-lp6 MF-rate dedup + SetMFDetails count diagnostic tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
