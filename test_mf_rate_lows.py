"""ak-nl4 LOW batch regression tests: interval doc reconciliation,
scheme-drop observability, and schemeCode type-coercion canonical form.

Post-ak-iwj. Scoped to:
  * services/tasks/SetMfRate.py (L1 interval comment + L2 malformed
    parse stats + L3 normalize helper wiring at ingest)
  * services/tasks/SetMfDetails.py (L1 interval comment)
  * services/JsonDownloadService.py (L3 normalize_scheme_code
    helper + wiring at read)

L4 is architectural (no code) — see the ak-nl4 v1 commit body for
the observation. If we agree the unified BaseRateTask abstraction
is worth pursuing, a separate design bead will spawn from that.

Run:
    python3 -m unittest test_mf_rate_lows
    python3 -m pytest test_mf_rate_lows.py
"""

import ast
import os
import sys
import unittest
from collections import defaultdict


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_SETMFRATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "SetMfRate.py",
)
_SETMFDETAILS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "SetMfDetails.py",
)
_JSONDL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "JsonDownloadService.py",
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


# ── L1: interval comment reconciliation ─────────────────────────────────


class TestL1IntervalDocReconciliation(unittest.TestCase):
    """The pre-L1 comments said '4 hours' on both SetMFRate (150 min
    = 2.5h) and SetMFDetails (600 min = 10h). Neither the values nor
    the cronAgent contract (6h market / 24h) matched the comment.
    v1 rewrites the comments to be accurate + names the cronAgent
    contract for context."""

    def test_setmfrate_no_4_hours_stale_comment(self):
        print("\n[ak-nl4 L1 — SetMfRate: pre-fix '# 4 hours' comment gone; new comment names 2.5h]")
        src = _source(_SETMFRATE_PATH)
        # Pre-fix bad comment must be gone (immediately above the
        # interval assignment).
        self.assertNotIn("# 4 hours\n            self.interval = 150", src)
        # New comment must describe the actual value AND reference
        # the cronAgent contract for context.
        self.assertIn("self.interval = 150  # minutes → 2.5h self-reschedule cadence", src)
        self.assertIn("cronAgent.py L43", src)
        print("  ✓ 4h comment gone, 2.5h + cronAgent reference present")

    def test_setmfdetails_no_4_hours_stale_comment(self):
        print("\n[ak-nl4 L1 — SetMfDetails: pre-fix '# 4 hours' gone; new comment names 10h]")
        src = _source(_SETMFDETAILS_PATH)
        self.assertNotIn("# 4 hours\n            self.interval = 600", src)
        self.assertIn("self.interval = 600  # minutes → 10h self-reschedule cadence", src)
        self.assertIn("cronAgent.py L46", src)
        print("  ✓ 4h comment gone, 10h + cronAgent reference present")


# ── L2: scheme-drop observability ───────────────────────────────────────


class TestL2SchemeDropObservability(unittest.TestCase):
    """_process_responses now returns a MalformedStats dict tracking
    parsed count + skipped count + reasons. _fetch_all_passes fires a
    per-pass WARN log iff skipped > 0."""

    @classmethod
    def setUpClass(cls):
        # AST-lift _process_responses so we can drive it without the
        # BaseTask import chain. It only reads self.logger + the
        # module-level _ERROR_KEY constant.
        src = _source(_SETMFRATE_PATH)
        tree = ast.parse(src)
        picked = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_process_responses":
                picked.append(node)
                break
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                t = node.targets[0]
                if isinstance(t, ast.Name) and t.id == "_ERROR_KEY":
                    picked.append(node)
        ns = {"defaultdict": defaultdict}
        exec(compile(ast.Module(body=picked, type_ignores=[]), "<l2>", "exec"), ns)
        cls.process_fn = staticmethod(ns["_process_responses"])
        cls.ERROR_KEY = ns["_ERROR_KEY"]

    class _Shim:
        def __init__(self):
            self.records = []
        class _L:
            def __init__(_self, outer): _self.outer = outer
            def _l(_self, level, msg, *a):
                _self.outer.records.append((level, msg % a if a else msg))
            def info(_self, m, *a): _self._l("info", m, *a)
            def warning(_self, m, *a): _self._l("warning", m, *a)
            def error(_self, m, *a): _self._l("error", m, *a)
            def debug(_self, m, *a): pass
        @property
        def logger(self):
            return self._L(self)

    def _run(self, responses):
        shim = self._Shim()
        result_map = {}
        stats = self.process_fn(shim, responses, result_map)
        return stats, result_map

    def test_clean_pass_returns_zero_skipped(self):
        print("\n[ak-nl4 L2 — clean pass (all valid) returns skipped=0]")
        responses = [
            ("100", {"data": [{"date": "01-01-26", "nav": "10.00"}]}),
            ("101", {"data": [{"date": "01-01-26", "nav": "20.00"}]}),
        ]
        stats, result_map = self._run(responses)
        self.assertEqual(stats["parsed"], 2)
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(dict(stats["reasons"]), {})
        self.assertEqual(len(result_map), 2)
        print("  ✓ 2/0 parsed/skipped, reasons empty")

    def test_missing_data_key_counted(self):
        print("\n[ak-nl4 L2 — missing 'data' key → skipped w/ 'missing_data_key' reason]")
        responses = [
            ("100", {"data": [{"date": "01-01-26", "nav": "10"}]}),
            ("101", {"meta": {"scheme_type": "x"}}),  # no 'data' key
        ]
        stats, _ = self._run(responses)
        self.assertEqual(stats["parsed"], 1)
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["reasons"]["missing_data_key"], 1)
        print("  ✓ 1 skipped w/ missing_data_key reason")

    def test_error_marker_not_counted_as_malformed(self):
        """fetch_scheme's error-marker payloads (dicts with _ERROR_KEY)
        are counted by _process_errors, not here. Must NOT inflate
        the malformed stats."""
        print("\n[ak-nl4 L2 — error-marker payloads NOT counted (already tracked by _process_errors)]")
        responses = [
            ("100", {"data": [{"date": "01-01-26", "nav": "10"}]}),
            ("101", {self.ERROR_KEY: "permanent_skip_404", "status": 404}),
            ("102", {self.ERROR_KEY: "final_5xx", "status": 500}),
        ]
        stats, _ = self._run(responses)
        self.assertEqual(stats["parsed"], 1)
        self.assertEqual(stats["skipped"], 0)  # error markers silently ignored
        print("  ✓ 2 error markers ignored (no false 'malformed' inflation)")

    def test_non_dict_payload_counted(self):
        print("\n[ak-nl4 L2 — non-dict payload → skipped w/ 'non_dict_payload' reason]")
        responses = [
            ("100", "unexpected string payload"),
            ("101", [1, 2, 3]),
            ("102", None),
        ]
        stats, _ = self._run(responses)
        self.assertEqual(stats["parsed"], 0)
        self.assertEqual(stats["skipped"], 3)
        self.assertEqual(stats["reasons"]["non_dict_payload"], 3)
        print("  ✓ 3 non-dict payloads all counted")

    def test_empty_nav_data_counted(self):
        print("\n[ak-nl4 L2 — empty nav_data → skipped w/ 'empty_nav_data' reason]")
        responses = [("100", {"data": []})]
        stats, _ = self._run(responses)
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["reasons"]["empty_nav_data"], 1)
        print("  ✓ empty nav_data captured")

    def test_non_list_nav_data_counted(self):
        print("\n[ak-nl4 L2 — non-list nav_data → skipped w/ 'non_list_nav_data' reason]")
        responses = [("100", {"data": {"date": "01-01-26"}})]  # dict not list
        stats, _ = self._run(responses)
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["reasons"]["non_list_nav_data"], 1)
        print("  ✓ non-list nav_data captured")

    def test_key_error_during_construction_counted(self):
        print("\n[ak-nl4 L2 — KeyError during entry construction → skipped w/ exception reason]")
        responses = [("100", {"data": [{"just_a_key": "no_date_no_nav"}]})]
        stats, _ = self._run(responses)
        self.assertEqual(stats["skipped"], 1)
        self.assertIn("exception_KeyError", stats["reasons"])
        self.assertEqual(stats["reasons"]["exception_KeyError"], 1)
        print("  ✓ KeyError classified under exception_KeyError")

    def test_mixed_reasons_all_tallied(self):
        print("\n[ak-nl4 L2 — mixed clean + 3 distinct reasons → all counted]")
        responses = [
            ("1", {"data": [{"date": "d", "nav": "n"}]}),  # OK
            ("2", "not-a-dict"),                            # non_dict_payload
            ("3", {"data": []}),                            # empty_nav_data
            ("4", {"data": {"k": "v"}}),                    # non_list_nav_data
            ("5", {self.ERROR_KEY: "final_5xx", "status": 500}),  # not counted
        ]
        stats, _ = self._run(responses)
        self.assertEqual(stats["parsed"], 1)
        self.assertEqual(stats["skipped"], 3)
        self.assertEqual(stats["reasons"]["non_dict_payload"], 1)
        self.assertEqual(stats["reasons"]["empty_nav_data"], 1)
        self.assertEqual(stats["reasons"]["non_list_nav_data"], 1)
        print("  ✓ 3 distinct reasons all tallied; error-marker excluded")

    def test_log_helper_silent_on_clean_pass(self):
        """The per-pass log helper should NOT fire when skipped=0 —
        clean-run noise avoidance is explicit in the L2 spec."""
        print("\n[ak-nl4 L2 — _log_malformed_if_any silent when skipped=0]")
        code = _source_code_only(_SETMFRATE_PATH)
        self.assertIn("def _log_malformed_if_any(self, parse_stats, *, pass_num):", code)
        # The helper's early-return guard.
        self.assertIn('if parse_stats["skipped"] == 0:', code)
        # And the WARN log body includes the 3 required keys.
        self.assertIn("parsed=", code)
        self.assertIn("skipped_malformed=", code)
        self.assertIn("skipped_reasons=", code)
        print("  ✓ early-return guard + 3-key log body all present")


# ── L3: schemeCode canonical normalization ──────────────────────────────


class TestL3NormalizeSchemeCode(unittest.TestCase):
    """`JsonDownloadService.normalize_scheme_code` is the single canonical-
    form authority for schemeCode values. Applied at ingest
    (SetMfRate.buildJsonForMF) AND at read (getMFRate)."""

    @classmethod
    def setUpClass(cls):
        # AST-lift the staticmethod (it references nothing else).
        # Strip the @staticmethod decorator on the lifted node so the
        # resulting exec produces a plain function, not a staticmethod
        # descriptor. We're calling it as a bare function, so the
        # decorator would only make the second-level wrap unhappy.
        src = _source(_JSONDL_PATH)
        tree = ast.parse(src)
        picked = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "normalize_scheme_code":
                node.decorator_list = []  # strip @staticmethod
                picked.append(node)
                break
        assert picked, "could not locate normalize_scheme_code"
        ns = {}
        exec(compile(ast.Module(body=picked, type_ignores=[]), "<l3>", "exec"), ns)
        # Store as staticmethod on the class so self.normalize(x)
        # doesn't get an unwanted `self` bind.
        cls.normalize = staticmethod(ns["normalize_scheme_code"])

    def test_int_returns_str(self):
        print("\n[ak-nl4 L3 — int 100027 → '100027']")
        self.assertEqual(self.normalize(100027), "100027")
        print("  ✓ int coerced to bare str")

    def test_str_stripped(self):
        print("\n[ak-nl4 L3 — str with whitespace → stripped]")
        self.assertEqual(self.normalize("100027"), "100027")
        self.assertEqual(self.normalize("  100027 "), "100027")
        self.assertEqual(self.normalize("\t100027\n"), "100027")
        print("  ✓ whitespace stripped; canonical str returned")

    def test_float_integer_valued_coerced(self):
        """float 100027.0 → '100027' (drop the '.0' artifact)."""
        print("\n[ak-nl4 L3 — float 100027.0 → '100027' (no '.0' artifact)]")
        self.assertEqual(self.normalize(100027.0), "100027")
        # Also test 0.0 edge — falsy but valid integer float.
        self.assertEqual(self.normalize(0.0), "0")
        print("  ✓ integer-valued floats coerced cleanly")

    def test_float_with_fraction_raises(self):
        print("\n[ak-nl4 L3 — float 100027.5 → ValueError]")
        with self.assertRaises(ValueError) as ctx:
            self.normalize(100027.5)
        self.assertIn("fractional", str(ctx.exception).lower())
        print("  ✓ fractional float rejected")

    def test_nan_raises(self):
        print("\n[ak-nl4 L3 — float('nan') → ValueError (not silently passed)]")
        with self.assertRaises(ValueError) as ctx:
            self.normalize(float("nan"))
        self.assertIn("nan", str(ctx.exception).lower())
        print("  ✓ NaN rejected")

    def test_none_raises(self):
        print("\n[ak-nl4 L3 — None → ValueError]")
        with self.assertRaises(ValueError):
            self.normalize(None)
        print("  ✓ None rejected")

    def test_empty_and_whitespace_raise(self):
        print("\n[ak-nl4 L3 — empty/whitespace str → ValueError]")
        for val in ("", "   ", "\t\n"):
            with self.assertRaises(ValueError):
                self.normalize(val)
        print("  ✓ empty/whitespace-only str rejected")

    def test_bool_raises(self):
        """Python bool is a subclass of int — must reject explicitly
        so `str(True) == '1'` doesn't silently key against scheme
        code 1."""
        print("\n[ak-nl4 L3 — bool True/False → ValueError (bool is int subclass)]")
        with self.assertRaises(ValueError):
            self.normalize(True)
        with self.assertRaises(ValueError):
            self.normalize(False)
        print("  ✓ bool rejected (isinstance(True, int) trap closed)")

    def test_unsupported_type_raises(self):
        print("\n[ak-nl4 L3 — unsupported type (list, dict, tuple) → ValueError]")
        for val in ([1, 2], {"a": 1}, (1, 2), object()):
            with self.assertRaises(ValueError):
                self.normalize(val)
        print("  ✓ unsupported types rejected")

    def test_leading_zeros_preserved(self):
        """If mfapi.in ever returns leading-zero strings, we preserve
        them — don't int-cast and lose semantic content."""
        print("\n[ak-nl4 L3 — leading-zero str '000123' preserved verbatim]")
        self.assertEqual(self.normalize("000123"), "000123")
        self.assertEqual(self.normalize("  000123 "), "000123")
        print("  ✓ leading zeros preserved (don't int-cast strings)")

    def test_wiring_at_ingest_and_read(self):
        """Both call sites must actually USE normalize_scheme_code.
        Guards against a refactor that adds a str() coercion back."""
        print("\n[ak-nl4 L3 — normalize_scheme_code called at both ingest (SetMfRate) + read (getMFRate)]")
        setmfrate_code = _source_code_only(_SETMFRATE_PATH)
        jsondl_code = _source_code_only(_JSONDL_PATH)
        # Ingest side.
        self.assertIn("self.jsonService.normalize_scheme_code(raw_code)", setmfrate_code)
        # Read side (getMFRate).
        self.assertIn("self.normalize_scheme_code(schemeCode)", jsondl_code)
        # Ingest side no longer uses `str(item.get('schemeCode'))` bare.
        # (We can still check the URL construction now uses the
        # normalized `code` var not the raw item.)
        self.assertIn('f"{baseUrl}/{code}"', setmfrate_code)
        print("  ✓ normalize wired at both boundaries; URL uses normalized code")


if __name__ == "__main__":
    print("ak-nl4 LOW batch — MF-rate hardening regression tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
