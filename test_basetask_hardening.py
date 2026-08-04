"""ak-iwj MEDIUM batch regression tests — fleet-wide BaseTask
hardening plus SetMFRate retry metrics.

Scope reminder: unlike ak-lp6/539/5jq (SetMfRate-local), ak-iwj
touches services/tasks/baseTask.py which is INHERITED by every rate
job in the fleet (SetMFRate, SetNPSRate, SetPPFRate, SetGoldRate,
SetEPFRate, SetMFDetails, SetNPSDetails, SetStocksDetails, plus any
future BaseTask-derived class). These tests intentionally exercise
BaseTask directly (via a tiny concrete subclass) so a break here
would flag before it reached any of the specific rate tasks.

  M1  safe_replace_file logs WARNING on old-file OSError (was silently
      swallowed → unbounded disk growth). Adds periodic sweep keeping
      only the last N historical files per prefix.
  M2  save_json uses atomic write (tmp + fsync + os.replace) with a
      post-write json.load sanity check; safe_replace_file also
      validates JSON before deleting old.
  M3  Singleton create + init protected by a class-level threading
      lock (double-check locking). Prevents double-init under
      concurrent instantiation.
  M4  SetMFRate _fetch_all_passes logs per-pass retry-effectiveness
      metrics (previous_missing, recovered, still_failing) + job-end
      summary of RETRY_PASSES effectiveness. Enables data-driven
      tuning.

Runs offline. AST-lifts where imports are heavy; drives real
threading + tempfile where behavior matters.
"""

import ast
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_BASETASK_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "baseTask.py",
)
_SETMFRATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "SetMfRate.py",
)


def _source(path):
    with open(path) as fh:
        return fh.read()


def _source_code_only(path):
    """Strip comments so 'here's what we removed' comments don't
    false-trip source guards."""
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


# ── M2 + M1: save_json atomic write + safe_replace_file validation ─────


class _CaptureLogger:
    """Minimal logger stand-in that records (level, msg) tuples."""
    def __init__(self):
        self.records = []
    def _l(self, level, msg, *a):
        self.records.append((level, msg % a if a else msg))
    def info(self, m, *a): self._l("info", m, *a)
    def warning(self, m, *a): self._l("warning", m, *a)
    def error(self, m, *a): self._l("error", m, *a)
    def debug(self, m, *a): pass
    def by_level(self, level):
        return [m for lvl, m in self.records if lvl == level]
    def any_at(self, level, needle):
        return any(needle in m for m in self.by_level(level))


class TestSaveJsonAtomicAndValidated(unittest.TestCase):
    """ak-iwj M2: save_json writes atomically (tmp + fsync + os.replace)
    and validates the on-disk file parses as JSON before returning.
    Any failure re-raises (was silently logged pre-M2)."""

    @classmethod
    def setUpClass(cls):
        # AST-lift save_json (it uses only os, json, self.logger) so we
        # can drive it without the full BaseTask stack (JsonDownloadService
        # → InvestmentService → sqlalchemy → flask → …).
        src = _source(_BASETASK_PATH)
        tree = ast.parse(src)
        picked = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "save_json":
                picked.append(node)
                break
        # Also lift _KEEP_HISTORICAL_FILES so it's in the ns.
        for node in tree.body:
            if isinstance(node, ast.Assign) and len(node.targets) == 1:
                t = node.targets[0]
                if isinstance(t, ast.Name) and t.id == "_KEEP_HISTORICAL_FILES":
                    picked.append(node)
        # Rebuild as functions (drop `self`) — save_json is a method so
        # we call it via a shim.
        ns = {"os": os, "json": json}
        exec(compile(ast.Module(body=picked, type_ignores=[]), "<m2>", "exec"), ns)
        cls.save_json = staticmethod(ns["save_json"])

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ak-iwj-m2-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.log = _CaptureLogger()
        self.shim = type("S", (), {"logger": self.log})()

    def test_happy_path_writes_and_validates(self):
        print("\n[ak-iwj M2 — happy path: writes + validates + logs success]")
        path = os.path.join(self.tmp, "data.json")
        self.save_json(self.shim, {"a": 1, "b": [1, 2, 3]}, path)
        self.assertTrue(os.path.exists(path))
        with open(path) as fh:
            self.assertEqual(json.load(fh), {"a": 1, "b": [1, 2, 3]})
        self.assertTrue(self.log.any_at("info", "File saved successfully"))
        # Tmp sibling is cleaned up (renamed away).
        self.assertFalse(os.path.exists(path + ".tmp"))
        print("  ✓ file exists + parses + tmp cleaned")

    def test_write_uses_tmp_then_replace(self):
        """Structural evidence of the atomic pattern: the code path
        creates a `.tmp` sibling before the final path. Check the
        source contains the pattern rather than trying to catch it
        mid-write (racy without instrumentation)."""
        print("\n[ak-iwj M2 — source uses tmp + os.replace atomic pattern]")
        code = _source_code_only(_BASETASK_PATH)
        self.assertIn('tmp_path = f"{file_path}.tmp"', code)
        self.assertIn("os.fsync(json_file.fileno())", code)
        self.assertIn("os.replace(tmp_path, file_path)", code)
        # AND a post-write json.load round-trip.
        self.assertIn("json.load(fh)", code)
        print("  ✓ tmp + fsync + os.replace + json.load round-trip all present")

    def test_write_failure_reraises_and_cleans_tmp(self):
        """A failing write (e.g. unwritable path) must raise, not
        silently swallow. Pre-M2 the caller trusted the return."""
        print("\n[ak-iwj M2 — write failure re-raises, tmp cleaned]")
        # Force failure: pass a directory-shaped path that can't be
        # opened for writing.
        bad_path = os.path.join(self.tmp, "does-not-exist-dir", "x.json")
        with self.assertRaises(Exception):
            self.save_json(self.shim, {"a": 1}, bad_path)
        self.assertTrue(self.log.any_at("error", "Error saving JSON"))
        # Tmp shouldn't linger.
        self.assertFalse(os.path.exists(bad_path + ".tmp"))
        print("  ✓ failure re-raised + error logged + tmp cleaned")

    def test_non_serializable_data_raises(self):
        """json.dump-time failure (e.g. a set) must also re-raise."""
        print("\n[ak-iwj M2 — non-JSON-serializable data raises]")
        path = os.path.join(self.tmp, "bad.json")
        with self.assertRaises(TypeError):
            self.save_json(self.shim, {"s": {1, 2, 3}}, path)
        # File must not exist on failure.
        self.assertFalse(os.path.exists(path))
        # And tmp is cleaned.
        self.assertFalse(os.path.exists(path + ".tmp"))
        print("  ✓ TypeError re-raised + no partial file left behind")


class TestSaveJsonSourceGuards(unittest.TestCase):
    """Additional source-level guards for M2 — pre-fix silent-swallow
    must NOT return."""

    def test_no_bare_except_log_and_return(self):
        """The pre-M2 shape was `except Exception: log.error; <return>`
        — swallowed the failure. Post-M2 the except block MUST end
        with `raise`."""
        print("\n[ak-iwj M2 — save_json except block ends with raise]")
        code = _source_code_only(_BASETASK_PATH)
        import re
        m = re.search(
            r"def save_json\(self, data, file_path\):(.*?)(?=\n    (?:async )?def |\Z)",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate save_json body")
        body = m.group(1)
        # `raise` present in the body (in the except block).
        self.assertIn("raise", body)
        print("  ✓ raise present in save_json body")


# ── M1: safe_replace_file OSError logging + historical sweep ───────────


class TestSafeReplaceFileErrorPaths(unittest.TestCase):
    """Source-inspection guards for M1. Behavioral test would require
    driving the full jsonService stack; source guards catch the class
    of regression (silent OSError swallow, no sweep) cheaply."""

    def test_osremove_error_logged_at_warning(self):
        """Pre-M1 the old-file os.remove exception was `pass`ed
        silently. Post-M1 it MUST log at WARNING with the errno."""
        print("\n[ak-iwj M1 — safe_replace_file logs WARNING on old-file OSError]")
        code = _source_code_only(_BASETASK_PATH)
        # The exception handler now calls logger.warning with the errno.
        self.assertIn("except OSError as exc:", code)
        self.assertIn("could not delete old file", code)
        self.assertIn("errno=", code)
        # And the pre-M1 `pass  # Old file stays — not critical` must
        # be gone from the safe_replace_file body.
        import re
        m = re.search(
            r"def safe_replace_file\(self.*?\):(.*?)(?=\n    def |\Z)",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m)
        body = m.group(1)
        # No bare `pass` at the except-block indent immediately after
        # os.remove.
        self.assertNotIn(
            "except OSError:\n                    pass",
            body,
            "pre-M1 silent-swallow (`except OSError: pass`) still present",
        )
        print("  ✓ WARN log + errno; no silent-swallow shape in body")

    def test_historical_sweep_helper_present(self):
        print("\n[ak-iwj M1 — _sweep_historical helper defined + called]")
        code = _source_code_only(_BASETASK_PATH)
        self.assertIn("def _sweep_historical(self, prefix, file_type):", code)
        self.assertIn("self._sweep_historical(prefix, file_type)", code)
        # Retention constant pinned.
        self.assertIn("_KEEP_HISTORICAL_FILES = 3", code)
        print("  ✓ helper + call site + retention constant all present")

    def test_new_file_validated_before_old_deleted(self):
        """M2 crossover: safe_replace_file's `getsize > 0` was the
        pre-fix check. Must now do a json.load before trusting the
        new file — a truncated JSON has size > 0."""
        print("\n[ak-iwj M2 — safe_replace_file json.load-validates new file]")
        code = _source_code_only(_BASETASK_PATH)
        # The validate block: json.load on the new file inside a try/
        # except that returns Failed on parse error.
        self.assertIn("failed JSON parse", code)
        self.assertIn("refusing to delete old file", code)
        self.assertIn("failed JSON validation", code)
        print("  ✓ safe_replace_file json.load-validates before delete")


# ── M3: singleton double-init race under threading ─────────────────────


class TestSingletonThreadSafety(unittest.TestCase):
    """ak-iwj M3: __new__ + __init__ use a class-level lock with
    double-check locking. Under concurrent instantiation, only ONE
    instance is created and __init__ body runs at most once."""

    def _build_toy_task_class(self):
        """AST-lift BaseTask's __new__ + __init__ and stitch them onto
        a concrete Toy class that doesn't need the JsonDownloadService
        / InvestmentService import chain. We stub those out on the
        Toy class so __init__ can run to completion."""
        src = _source(_BASETASK_PATH)
        tree = ast.parse(src)
        # Grab BaseTask class body items we need: __new__, __init__,
        # _instance = None, _singleton_lock = ...
        new_fn = init_fn = None
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "BaseTask":
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        if item.name == "__new__":
                            new_fn = item
                        elif item.name == "__init__":
                            init_fn = item
        self.assertIsNotNone(new_fn, "could not locate BaseTask.__new__")
        self.assertIsNotNone(init_fn, "could not locate BaseTask.__init__")

        # Build a shim class with those two methods + stubs for the
        # heavy dependencies __init__ touches. The stubs double as our
        # thread-safe observable: JSONDownloadService is constructed
        # inside the M3-protected init body EXACTLY ONCE per singleton
        # if the lock works. Counting its constructor calls is a
        # non-racy signal for "init body ran" — much better than the
        # pre-vs-post-call `initialized` check which was racy under
        # concurrent entry.
        _init_body_count = {"n": 0}
        _init_body_lock = threading.Lock()
        class _StubJDS:
            class JSONDownloadService:
                def __init__(self, path):
                    with _init_body_lock:
                        _init_body_count["n"] += 1
                    self.path = path
        class _StubIS:
            def __init__(self): pass
        class _StubTS:
            def __init__(self): pass
        ns_extras = {"_init_body_count": _init_body_count}
        ns = {
            "os": os,
            "threading": threading,
        }
        ns.update({
            "JsonDownloadService": _StubJDS,
            "InvestmentService": _StubIS,
            "TransactionService": _StubTS,
        })
        # __init__ references `self.__class__._singleton_lock` — needs
        # to exist on the class. We build Toy with the lock + _instance.
        exec(
            "class Toy:\n"
            "    _instance = None\n"
            "    _singleton_lock = threading.Lock()\n"
            "    # BaseTask defines tmp_dir as a class attribute; the\n"
            "    # AST-lifted __init__ passes it to os.makedirs.\n"
            "    tmp_dir = '/tmp/ak-iwj-toy-tmpdir'\n"
            "    # init_count is our observable — each call to __init__'s\n"
            "    # protected body increments it. Under M3 concurrent init,\n"
            "    # init_count must reach 1 exactly.\n"
            "    _init_count = 0\n",
            ns,
        )
        # Attach the AST-lifted methods. First, rewrite
        # `super(BaseTask, cls).__new__(cls)` → `object.__new__(cls)`
        # since Toy inherits directly from object and BaseTask isn't
        # in scope. Preserves the double-check-locking semantics —
        # we're only avoiding an MRO artefact from the AST-lift.
        class _SuperRewriter(ast.NodeTransformer):
            def visit_Call(self, node):
                self.generic_visit(node)
                # Match: super(BaseTask, cls).__new__(cls)
                if (isinstance(node.func, ast.Attribute)
                        and node.func.attr == "__new__"
                        and isinstance(node.func.value, ast.Call)
                        and isinstance(node.func.value.func, ast.Name)
                        and node.func.value.func.id == "super"):
                    # Replace with object.__new__(cls).
                    return ast.Call(
                        func=ast.Attribute(
                            value=ast.Name(id="object", ctx=ast.Load()),
                            attr="__new__", ctx=ast.Load(),
                        ),
                        args=node.args, keywords=[],
                    )
                return node
        rewriter = _SuperRewriter()
        new_fn = rewriter.visit(new_fn)
        init_fn = rewriter.visit(init_fn)
        ast.fix_missing_locations(new_fn)
        ast.fix_missing_locations(init_fn)
        module = ast.Module(body=[new_fn, init_fn], type_ignores=[])
        method_ns = {"os": os, "threading": threading,
                     "JsonDownloadService": _StubJDS,
                     "InvestmentService": _StubIS,
                     "TransactionService": _StubTS,
                     # __init__ references __file__ to compute the
                     # assets_path. Use the real baseTask.py path so
                     # the assets dir shape is what production uses.
                     "__file__": _BASETASK_PATH}
        exec(compile(module, "<m3>", "exec"), method_ns)
        Toy = ns["Toy"]
        Toy.__new__ = method_ns["__new__"]
        Toy.__init__ = method_ns["__init__"]
        # Expose the observable on the class so tests can inspect it.
        Toy._init_body_count = ns_extras["_init_body_count"]
        return Toy

    def test_concurrent_instantiation_single_init(self):
        """N=50 threads racing to create Toy simultaneously → exactly
        one __init__ body run, and all threads see the same instance."""
        print("\n[ak-iwj M3 — 50 concurrent Toy() calls → 1 init, 1 instance]")
        Toy = self._build_toy_task_class()
        N = 50
        results = [None] * N
        errors = []
        barrier = threading.Barrier(N)

        def worker(i):
            try:
                barrier.wait(timeout=5)
                results[i] = Toy("title", "P1")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(N)]
        for t in threads: t.start()
        for t in threads: t.join(timeout=5)

        self.assertEqual(errors, [], f"unexpected errors: {errors}")
        # All threads got the SAME instance.
        instances = set(id(r) for r in results)
        self.assertEqual(len(instances), 1,
                         f"expected 1 unique instance, got {len(instances)}")
        # Init body ran EXACTLY ONCE. _init_body_count is incremented
        # from inside the stub JSONDownloadService constructor which
        # is called from within the M3-protected block.
        actual = Toy._init_body_count["n"]
        self.assertEqual(
            actual, 1,
            f"expected 1 init body run, got {actual} — "
            f"double-init race regressed",
        )
        print(f"  ✓ N={N} threads → 1 instance + 1 init body ({actual})")

    def test_sequential_second_call_is_no_op(self):
        """After the first init, subsequent Toy() calls must NOT
        re-run the init body — the fast unlocked check should
        short-circuit."""
        print("\n[ak-iwj M3 — second Toy() call skips init (fast-path)]")
        Toy = self._build_toy_task_class()
        first = Toy("a", "P1")
        second = Toy("b", "P2")
        self.assertIs(first, second)
        self.assertEqual(
            Toy._init_body_count["n"], 1,
            "second call re-ran init body — fast-path broken",
        )
        print("  ✓ same instance, init_body_count still 1 after 2 calls")

    def test_singleton_lock_class_attribute_present(self):
        """Structural: BaseTask has the class-level lock, and __new__
        + __init__ both reference it."""
        print("\n[ak-iwj M3 — BaseTask defines _singleton_lock; __new__/__init__ use it]")
        code = _source_code_only(_BASETASK_PATH)
        self.assertIn("_singleton_lock = threading.Lock()", code)
        self.assertIn("with cls._singleton_lock:", code)
        self.assertIn("with self.__class__._singleton_lock:", code)
        # And the fast unlocked check IS present (double-check pattern
        # — happy path never takes the lock).
        self.assertIn("if cls._instance is not None:", code)
        self.assertIn("if getattr(self, 'initialized', False):", code)
        print("  ✓ lock + double-check pattern all present in BaseTask")


# ── M4: retry-effectiveness metrics ─────────────────────────────────────


class TestRetryMetricsFormat(unittest.TestCase):
    """ak-iwj M4: per-pass "retry_pass=N previous_missing=X recovered=Y
    still_failing=Z" log line + job-end summary of RETRY_PASSES effect.

    We source-inspect the log format so the shape is stable across
    refactors (infra grep patterns depend on it)."""

    def test_per_pass_log_line_format(self):
        print("\n[ak-iwj M4 — per-pass log line uses retry_pass/previous_missing/recovered/still_failing key=value form]")
        code = _source_code_only(_SETMFRATE_PATH)
        # The four required key=value substrings.
        self.assertIn("retry_pass={retry_pass + 2}", code)
        self.assertIn("previous_missing={previous_missing}", code)
        self.assertIn("recovered={recovered}", code)
        self.assertIn("still_failing={still_failing}", code)
        print("  ✓ all 4 M4 keys present in per-pass log")

    def test_job_end_summary_format(self):
        print("\n[ak-iwj M4 — job-end summary contains RETRY_PASSES + per-pass recovery + unrecovered]")
        code = _source_code_only(_SETMFRATE_PATH)
        # The summary key.
        self.assertIn("RETRY_PASSES={RETRY_PASSES}", code)
        # Per-pass recovery format (rendered from recovery_per_pass list).
        self.assertIn("pass_{i+1}_recovered={n}", code)
        self.assertIn("unrecovered={unrecovered}", code)
        print("  ✓ summary format present")

    def test_recovery_per_pass_list_seeded_with_pass_1(self):
        """Pass 1 recoveries from a zero baseline are counted in the
        summary too — otherwise the pass_1_recovered field would
        confuse anyone charting the series."""
        print("\n[ak-iwj M4 — recovery_per_pass initialized w/ pass-1 count]")
        code = _source_code_only(_SETMFRATE_PATH)
        # recovery_per_pass.append(len(result_map)) after pass 1.
        self.assertIn("recovery_per_pass.append(len(result_map))", code)
        # And appended (with `recovered`) after each retry pass.
        self.assertIn("recovery_per_pass.append(recovered)", code)
        print("  ✓ recovery_per_pass seeded and appended each pass")


if __name__ == "__main__":
    print("ak-iwj MEDIUM batch — BaseTask hardening + retry metrics tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
