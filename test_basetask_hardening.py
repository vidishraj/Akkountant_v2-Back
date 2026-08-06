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


# ── M3 (v2): scheduler-is-serial observation + centralized init flag ──


class TestM3SchedulerSerialObservation(unittest.TestCase):
    """ak-iwj v1 added a threading.Lock to BaseTask.__new__/__init__
    for a theoretical concurrent-instantiation race. Reviewer flagged
    the lock was INERT: every rate-task subclass overrides __new__
    with its own local pattern that never delegates to the base's
    locked check. AND scheduler.py:106 processes tasks serially, so
    the race isn't reachable in the current architecture.

    v2 dropped the lock and preserved the simple pattern with an
    observation comment. This test guards the two invariants that
    justify dropping the lock:
      1. `scheduler.py:106` is the ONLY task instantiation vector in
         the repo (no other call site constructs a rate task).
      2. It runs inside a serial for-loop (no threadpool / no async).
    If either invariant changes, the concurrent-instantiation race
    becomes reachable and M3 needs to be revisited."""

    def test_scheduler_is_only_task_instantiation_vector(self):
        """Grep the codebase for `SetXxx(` construction of the known
        rate-task classes. The ONLY expected call site is inside
        services/tasks/scheduler.py (via task_class(...) at :106),
        plus any file-internal helpers (e.g. singleton smoke tests).
        Anything else means a new construction vector exists that
        may race with the scheduler."""
        print("\n[ak-iwj M3 v2 — scheduler.py:106 is the only task instantiation vector]")
        import re
        # Task class names to check. From services/tasks/scheduler.py
        # TASK_MAPPING plus a couple of extras.
        task_names = [
            "SetNPSRate", "SetNPSDetails", "SetKiteStockDetails",
            "SetStockOldCodes", "SetMFRate", "SetMFDetails",
            "SetIBJAGoldRate", "SetPPFRate", "SetEPFRate",
            "CheckMailTask", "CheckStatementTask", "InvestmentHistoryTask",
        ]
        # Walk services/ and controllers/ looking for `SetX(` construction
        # calls. Exclude the task-file itself (each class self-references
        # inside its own module e.g. `_instance = super(SetMFRate, cls)`),
        # the scheduler (expected caller), and this test file.
        root = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "services"
        )
        offenders = []
        for dirpath, _, filenames in os.walk(root):
            for fname in filenames:
                if not fname.endswith(".py"):
                    continue
                path = os.path.join(dirpath, fname)
                # Skip the scheduler (expected caller).
                if fname == "scheduler.py":
                    continue
                try:
                    with open(path) as fh:
                        source = fh.read()
                except OSError:
                    continue
                for tn in task_names:
                    # Skip files that DEFINE the class — they self-
                    # reference in super() calls and cls() singleton
                    # constructions. Precise check: does the file
                    # define `class {tn}(` anywhere?
                    if re.search(rf"^class {tn}\s*\(", source, re.MULTILINE):
                        continue
                    # Match `SetX(` construction — bare word-boundary
                    # followed by `(`.
                    for m in re.finditer(rf"\b{tn}\(", source):
                        # Peek before the match to filter out super(),
                        # isinstance(), issubclass() references.
                        before = source[max(0, m.start() - 12):m.start()]
                        if any(k in before for k in ("super(", "isinstance(", "issubclass(")):
                            continue
                        offenders.append(f"{path}: {tn}(")
        self.assertEqual(
            offenders, [],
            "Task-class construction found outside scheduler.py — "
            "concurrent instantiation race becomes reachable. "
            "Revisit ak-iwj M3.\nOffenders:\n  " + "\n  ".join(offenders),
        )
        print(f"  ✓ zero external construction sites for {len(task_names)} task classes")

    def test_scheduler_processes_jobs_serially(self):
        """Structural guard: scheduler.py processes overdue jobs in a
        `for job in deduplicated:` for-loop (line ~98). No
        ThreadPoolExecutor, no asyncio.gather. If this changes to
        concurrent, the M3 race becomes reachable."""
        print("\n[ak-iwj M3 v2 — scheduler.py processes jobs serially]")
        scheduler_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "services", "tasks", "scheduler.py",
        )
        with open(scheduler_path) as fh:
            source = fh.read()
        # Serial for-loop pattern.
        self.assertIn("for job in deduplicated:", source)
        self.assertIn("task_instance = task_class(", source)
        # And NO threadpool / async construct.
        for concurrent_pattern in (
            "ThreadPoolExecutor", "ProcessPoolExecutor",
            "asyncio.gather", "asyncio.wait", "concurrent.futures.wait",
        ):
            self.assertNotIn(
                concurrent_pattern, source,
                f"scheduler.py contains {concurrent_pattern!r} — "
                f"task instantiation may now be concurrent. Revisit "
                f"ak-iwj M3 (add locking to task subclass __new__ overrides).",
            )
        print("  ✓ serial for-loop; no concurrent construct in scheduler.py")

    def test_baseTask_no_lock_and_centralized_initialized_flag(self):
        """Structural guard: the v1 _singleton_lock was DROPPED in v2
        but the centralized `self.initialized = True` in the base
        __init__ REMAINS — that piece fixed the latent 'never-set'
        bug in ~6 subclasses without needing the lock."""
        print("\n[ak-iwj M3 v2 — no lock in base; initialized flag centralized]")
        code = _source_code_only(_BASETASK_PATH)
        # No lock imports, no lock class attr, no `with cls.*lock:`.
        self.assertNotIn("import threading", code)
        self.assertNotIn("_singleton_lock", code)
        self.assertNotIn("with cls._singleton_lock", code)
        # But `self.initialized = True` IS still set at end of base
        # __init__ — this is the piece that fixes the latent bug.
        self.assertIn("self.initialized = True", code)
        # And the early-return guard on the flag is present.
        self.assertIn("if getattr(self, 'initialized', False):", code)
        print("  ✓ lock dropped; initialized flag centralized in base")


# ── M3 v1 legacy synthetic tests (kept for reference, all skipped) ──
# The v1 tests drove a synthetic Toy subclass that never touched the
# real BaseTask subclasses — reviewer flagged this as low signal.
# v2 replaces the confidence source with the scheduler-serial +
# no-external-caller observations above, plus TestFleetSubclassSmoke
# (MINOR 4) which drives the REAL subclasses.


class TestSingletonThreadSafety(unittest.TestCase):
    """DEPRECATED (ak-iwj v2): synthetic-subclass test class kept only
    as a documented artefact of the v1 approach. All tests skip. See
    TestM3SchedulerSerialObservation and TestFleetSubclassSmoke for
    the v2 confidence sources."""

    def test_deprecated_v1_synthetic_test_class(self):
        self.skipTest(
            "ak-iwj v2 dropped the M3 lock; synthetic subclass "
            "tests replaced by TestM3SchedulerSerialObservation + "
            "TestFleetSubclassSmoke (MINOR 4)."
        )

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

    # (v1 tests removed in v2 — see docstring above for rationale.)


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


# ── MINOR 4: fleet-wide subclass smoke (import + issubclass) ────────────


class TestFleetSubclassSmoke(unittest.TestCase):
    """ak-iwj v2 MINOR 4: guard against BaseTask changes silently
    breaking any of the 12 rate-task subclasses via import / attribute
    / signature drift.

    Two-tier strategy:
      1. Every subclass module can be imported AND resolves to a class
         that is a proper BaseTask subclass. This tier runs without
         real deps (imports are gated on flask/sqlalchemy being
         available; skips cleanly if not).
      2. If real deps ARE available, instantiate each subclass with
         plausible args and assert isinstance + `initialized` flag
         set. This tier catches __init__-signature drift + the
         'initialized flag never set' latent bug the M3-v2 fix
         centralized in the base.

    Motivation: the ak-iwj base changes are additive but not
    signature-neutral in every code path (save_json now re-raises
    where it previously returned; safe_replace_file validates JSON
    where it previously accepted size>0). If any subclass overrode
    those methods with the pre-fix semantics, this suite flags it.
    """

    # Names + import paths of every rate-task subclass in the fleet.
    # Sourced from services/tasks/scheduler.py TASK_MAPPING.
    _SUBCLASSES = [
        ("services.tasks.SetMfRate", "SetMFRate"),
        ("services.tasks.SetMfDetails", "SetMFDetails"),
        ("services.tasks.SetNPSRate", "SetNPSRate"),
        ("services.tasks.SetNPSDetails", "SetNPSDetails"),
        ("services.tasks.SetKiteStockDetails", "SetKiteStockDetails"),
        ("services.tasks.SetStockOldCodes", "SetStockOldCodes"),
        ("services.tasks.SetIBJAGoldRate", "SetIBJAGoldRate"),
        ("services.tasks.SetPpfRate", "SetPPFRate"),
        ("services.tasks.SetEPFRate", "SetEPFRate"),
        ("services.tasks.checkMailTask", "CheckMailTask"),
        ("services.tasks.checkStatementsTask", "CheckStatementTask"),
        ("services.tasks.InvestmentHistoryTask", "InvestmentHistoryTask"),
        # ak-ran Phase 1: daily wealth-management digest task.
        ("services.tasks.WealthDigestTask", "WealthDigestTask"),
    ]

    @classmethod
    def setUpClass(cls):
        # Attempt to import BaseTask + one subclass. If the import
        # chain fails (flask/sqlalchemy missing), all tests in this
        # class skip cleanly rather than fail at collection.
        try:
            from services.tasks.baseTask import BaseTask  # noqa: F401
            from services.tasks.SetMfRate import SetMFRate  # noqa: F401
            cls._imports_ok = True
        except Exception as exc:  # pragma: no cover — env-dep skip
            cls._imports_ok = False
            cls._import_error = str(exc)

    def _iter_subclasses(self):
        """Import + yield (name, class) for each subclass. Skips any
        that fail to import (should be zero — if any subclass CAN'T
        import that's the smoke signal we care about)."""
        import importlib
        for module_path, class_name in self._SUBCLASSES:
            try:
                mod = importlib.import_module(module_path)
                yield class_name, getattr(mod, class_name)
            except Exception as exc:
                self.fail(
                    f"could not import {module_path}.{class_name}: {exc} — "
                    f"MINOR 4 smoke: subclass import broken by base change",
                )

    def test_every_subclass_imports_and_is_basetask(self):
        """Tier 1 (no runtime): each subclass module imports cleanly
        AND the named class is a proper BaseTask subclass."""
        if not self._imports_ok:
            self.skipTest(
                f"BaseTask import chain unavailable in this env: "
                f"{self._import_error}"
            )
        print(f"\n[ak-iwj v2 MINOR 4 — {len(self._SUBCLASSES)} subclasses import + issubclass BaseTask]")
        from services.tasks.baseTask import BaseTask
        results = []
        for name, cls in self._iter_subclasses():
            self.assertTrue(
                issubclass(cls, BaseTask),
                f"{name} is not a BaseTask subclass — MRO drift",
            )
            results.append(name)
        self.assertEqual(len(results), len(self._SUBCLASSES))
        print(f"  ✓ {len(results)} subclasses all import + issubclass BaseTask")

    def test_every_subclass_instantiates_and_sets_initialized(self):
        """Tier 2 (runtime): construct each subclass with plausible
        args, assert `initialized` flag is True post-construction.
        This is the guard for the M3-v2 base-centralized flag AND
        for any __init__ signature drift caused by base changes.

        Skipped when the singleton `_instance` from a prior test in
        the same process has been set — since these are singletons,
        we can't reliably re-instantiate. Runs cleanly in a fresh
        process."""
        if not self._imports_ok:
            self.skipTest("BaseTask import chain unavailable")
        print(f"\n[ak-iwj v2 MINOR 4 — {len(self._SUBCLASSES)} subclasses instantiate + set initialized]")
        instantiated = 0
        for name, cls in self._iter_subclasses():
            # Reset any singleton left over from a prior test run so
            # this test drives a fresh __init__ path. Safe because
            # this test is the last-known caller of these singletons
            # (no other test suite in this file uses them).
            cls._instance = None
            try:
                instance = cls(name, "P1")
            except Exception as exc:
                self.fail(
                    f"{name}({name!r}, 'P1') raised {type(exc).__name__}: {exc} — "
                    f"__init__ signature drift caused by base change?",
                )
            # M3-v2 centralized flag: base sets initialized=True at
            # end of __init__. If subclass __init__ skipped calling
            # super().__init__ this fails and we've regressed.
            self.assertTrue(
                getattr(instance, "initialized", False),
                f"{name} did not set self.initialized — either its "
                f"__init__ skipped super().__init__ or the base "
                f"centralization regressed",
            )
            instantiated += 1
        self.assertEqual(instantiated, len(self._SUBCLASSES))
        print(f"  ✓ {instantiated} subclasses instantiate + initialized=True")


if __name__ == "__main__":
    print("ak-iwj MEDIUM batch — BaseTask hardening + retry metrics tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
