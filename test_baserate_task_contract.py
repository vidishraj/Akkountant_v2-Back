"""ak-2r8: BaseRateTask contract tests — parameterized across all 5
rate tasks. Asserts the coverage-gate machinery hoisted into
BaseRateTask.run() behaves identically for each subclass:

  * degenerate-answerable guard (answerable == 0 → Failed, no clobber)
  * hard-floor gate (ratio < _COVERAGE_HARD_FLOOR → Failed, no write)
  * 98% partial-success gate (below → Failed but file WRITTEN)
  * clean run (above 98% → Completed)

Two tiers:
  1. Source-inspection guards (no runtime deps): assert every rate
     task inherits BaseRateTask and declares the required class
     attributes (_rate_filename, _rate_prefix_attr) and _fetch_all.
  2. Behavioral tests: drive BaseRateTask.run() with stub
     _fetch_all outputs and assert (a) status/msg per gate tier,
     (b) safe_replace_file was/wasn't called per gate tier,
     (c) save_json was/wasn't called per gate tier.

Behavioral tests use a minimal stub subclass so the full app-stack
imports aren't required — the base's gate logic is pure Python and
doesn't need flask/sqlalchemy/etc. Skips cleanly if BaseRateTask
itself fails to import.

Run:
    python3 -m unittest test_baserate_task_contract
    python3 -m pytest test_baserate_task_contract.py
"""

import ast
import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_TASKS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks",
)


def _source(name):
    with open(os.path.join(_TASKS_DIR, name)) as fh:
        return fh.read()


# All 5 rate-task files + expected class + expected filename +
# expected prefix attr on jsonService.
_RATE_TASKS = [
    ("SetMfRate.py", "SetMFRate", "MFRate.json", "MfRatePrefix"),
    ("SetNPSRate.py", "SetNPSRate", "NPSRATE.json", "NpsRatePrefix"),
    ("SetPpfRate.py", "SetPPFRate", "PPFRate.json", "PPFRatePrefix"),
    ("SetEPFRate.py", "SetEPFRate", "EPFRate.json", "EPFRatePrefix"),
    ("SetIBJAGoldRate.py", "SetIBJAGoldRate", "GOLDRATE.json", "GoldRatePrefix"),
]


# ── Tier 1: source-inspection ────────────────────────────────────────────


class TestAllRateTasksInheritBaseRateTask(unittest.TestCase):
    """AST-inspection: every rate task class in the fleet must inherit
    from BaseRateTask (directly or via AIRateTask) and declare the
    required class attributes."""

    def _find_class(self, src, class_name):
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                return node
        return None

    def _class_attrs(self, class_node):
        """Return {attr_name: literal_value} for simple class-level
        assignments in the class body."""
        attrs = {}
        for stmt in class_node.body:
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
                target = stmt.targets[0]
                if isinstance(target, ast.Name):
                    val = stmt.value
                    if isinstance(val, ast.Constant):
                        attrs[target.id] = val.value
                    elif isinstance(val, ast.Name):
                        attrs[target.id] = ('name', val.id)
        return attrs

    def _has_method(self, class_node, method_name):
        for stmt in class_node.body:
            if isinstance(stmt, ast.FunctionDef) and stmt.name == method_name:
                return True
        return False

    def test_every_rate_task_declares_required_class_attrs(self):
        """Each rate task must declare _rate_filename and
        _rate_prefix_attr — BaseRateTask uses these to route the
        write pipeline. Missing attr means broken swap on Completed."""
        print("\n[ak-2r8 — all 5 rate tasks declare _rate_filename + _rate_prefix_attr]")
        for filename, class_name, expected_fname, expected_prefix in _RATE_TASKS:
            src = _source(filename)
            cls_node = self._find_class(src, class_name)
            self.assertIsNotNone(
                cls_node, f"could not find class {class_name} in {filename}",
            )
            attrs = self._class_attrs(cls_node)
            self.assertIn(
                "_rate_filename", attrs,
                f"{class_name} missing _rate_filename class attr",
            )
            self.assertEqual(
                attrs["_rate_filename"], expected_fname,
                f"{class_name}._rate_filename mismatch — "
                f"pipeline would write wrong tmp file",
            )
            self.assertIn(
                "_rate_prefix_attr", attrs,
                f"{class_name} missing _rate_prefix_attr class attr",
            )
            self.assertEqual(
                attrs["_rate_prefix_attr"], expected_prefix,
                f"{class_name}._rate_prefix_attr mismatch — "
                f"safe_replace_file would swap into wrong prefix",
            )
        print(f"  ✓ {len(_RATE_TASKS)} rate tasks all declare correct attrs")

    def test_every_rate_task_implements_fetch_all(self):
        """Each rate task must implement _fetch_all() — BaseRateTask's
        run() is abstract on this method."""
        print("\n[ak-2r8 — all 5 rate tasks implement _fetch_all()]")
        for filename, class_name, _, _ in _RATE_TASKS:
            src = _source(filename)
            cls_node = self._find_class(src, class_name)
            self.assertIsNotNone(cls_node)
            self.assertTrue(
                self._has_method(cls_node, "_fetch_all"),
                f"{class_name} does not implement _fetch_all() — "
                f"BaseRateTask.run() would raise NotImplementedError",
            )
        print(f"  ✓ {len(_RATE_TASKS)} rate tasks all implement _fetch_all")

    def test_no_rate_task_overrides_run_without_calling_super(self):
        """Post ak-2r8, run() is inherited from BaseRateTask. If a
        subclass overrides run() it MUST call super().run() so the
        gates fire. A run() override that returns without calling
        super would silently bypass coverage protection.

        This test allows a run() override (for legitimate wrapping)
        but requires the override body to reference super().run()
        somewhere. Absence = silent gate-bypass regression."""
        print("\n[ak-2r8 — no rate task bypasses BaseRateTask.run() gates]")
        for filename, class_name, _, _ in _RATE_TASKS:
            src = _source(filename)
            cls_node = self._find_class(src, class_name)
            self.assertIsNotNone(cls_node)
            # Find any run() method defined on this class.
            run_body_src = None
            for stmt in cls_node.body:
                if isinstance(stmt, ast.FunctionDef) and stmt.name == "run":
                    run_body_src = ast.unparse(stmt) if hasattr(ast, 'unparse') else None
                    break
            if run_body_src is None:
                # No run() override — inherits from BaseRateTask directly. ✓
                continue
            # Override present — must call super().run().
            self.assertIn(
                "super().run(", run_body_src,
                f"{class_name} overrides run() but never calls "
                f"super().run() — bypasses BaseRateTask coverage gates!",
            )
        print(f"  ✓ {len(_RATE_TASKS)} rate tasks: no gate-bypass overrides")

    def test_base_rate_task_declares_gate_constants(self):
        """BaseRateTask.py declares the class-level threshold constants
        with the MF defaults (98% / 50%). Subclasses may override
        (NPS does, per Q2 option (a))."""
        print("\n[ak-2r8 — BaseRateTask declares _MIN_SUCCESS_RATIO + _COVERAGE_HARD_FLOOR]")
        src = _source("BaseRateTask.py")
        self.assertIn("_MIN_SUCCESS_RATIO: float = 0.98", src)
        self.assertIn("_COVERAGE_HARD_FLOOR: float = 0.5", src)
        print("  ✓ base constants = 0.98 / 0.5 (MF defaults)")


# ── Tier 2: behavioral — drive BaseRateTask.run() with stub _fetch_all ──


def _ast_lift_base_rate_task():
    """Load BaseRateTask class body into an isolated namespace without
    running its top-level `from services.tasks.baseTask import BaseTask`
    (which pulls in flask/sqlalchemy/google.oauth2 via the InvestmentService
    chain). Injects a minimal BaseTask stub as parent so the class body
    executes and we get a live BaseRateTask class object.

    Same pattern as test_mf_rate_hardening_h.py's _lift_from_source
    (AST-based) — ships bare-env behavioral coverage for the gates."""
    src = _source("BaseRateTask.py")
    tree = ast.parse(src)
    # Drop the `from services.tasks.baseTask import BaseTask` import
    # so the class body binds to our stub BaseTask instead.
    kept = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "services.tasks.baseTask":
            continue
        kept.append(node)
    tree.body = kept
    code = compile(tree, filename="<BaseRateTask-AST-lift>", mode="exec")

    # Minimal BaseTask stub — just what BaseRateTask.run() needs at
    # runtime (nothing directly; subclasses stub save_json /
    # safe_replace_file / logger).
    class _StubBase:
        pass

    ns = {"BaseTask": _StubBase, "__name__": "test_baserate_task_ast_lift"}
    exec(code, ns)
    return ns["BaseRateTask"]


try:
    # Prefer real import so we exercise the actual module. If the
    # transitive chain is unavailable, fall back to AST-lift.
    from services.tasks.BaseRateTask import BaseRateTask as _RealBase  # noqa: E402
    BaseRateTask = _RealBase
    _BASE_OK = True
    _BASE_SKIP_REASON = ""
    _BASE_MODE = "real"
except Exception as _real_exc:
    try:
        BaseRateTask = _ast_lift_base_rate_task()
        _BASE_OK = True
        _BASE_SKIP_REASON = ""
        _BASE_MODE = f"ast-lift (real import failed: {_real_exc})"
    except Exception as _lift_exc:  # pragma: no cover — total env fail
        _BASE_OK = False
        _BASE_SKIP_REASON = (
            f"BaseRateTask real-import + AST-lift both failed: "
            f"{_real_exc} / {_lift_exc}"
        )
        _BASE_MODE = "unavailable"


def _make_stub_class():
    """Factory that constructs the _StubTask class lazily so its
    dependency on BaseRateTask being importable is deferred to
    call time (behavioral tests skip cleanly otherwise)."""

    class _StubTask(BaseRateTask):
        _rate_filename = "STUB.json"
        _rate_prefix_attr = "StubPrefix"
        interval = 60
        title = "stub"

        def __init__(self, fetch_result):
            # DELIBERATELY skip BaseTask.__init__ — the base's __init__
            # pulls in flask/sqlalchemy/jsonService which aren't
            # available in the bare test env. We drive run() with
            # stub disk methods below.
            self._fetch_result = fetch_result
            self.logger = self._make_logger()
            self.save_json_called = False
            self.safe_replace_called = False
            self.tmp_dir = "/tmp/stub"
            class _JS:
                StubPrefix = "STUB"
                ratesType = "rates"
            self.jsonService = _JS()

        def _make_logger(self):
            class _Log:
                def __init__(_self):
                    _self.records = []
                def _log(_self, lvl, msg, *a):
                    _self.records.append((lvl, msg % a if a else msg))
                def info(_self, m, *a): _self._log("info", m, *a)
                def warning(_self, m, *a): _self._log("warning", m, *a)
                def error(_self, m, *a): _self._log("error", m, *a)
                def debug(_self, m, *a): pass
            return _Log()

        def _fetch_all(self):
            return self._fetch_result

        def save_json(self, data, file_path):
            self.save_json_called = True

        def safe_replace_file(self, tmp, prefix, file_type):
            self.safe_replace_called = True
            return True, None

    return _StubTask


@unittest.skipUnless(_BASE_OK, _BASE_SKIP_REASON)
class TestBaseRateTaskGateBehavior(unittest.TestCase):
    """Drive BaseRateTask.run() directly via a minimal stub subclass.
    Asserts each gate tier — degenerate / hard-floor / 98% / clean —
    triggers the right status + side-effect combination."""

    @property
    def _StubTask(self):
        # Lazy factory: only invoked when tests actually run (after the
        # skipUnless guard). Avoids class-body evaluation trying to
        # subclass a possibly-unimportable BaseRateTask.
        if not hasattr(type(self), '_stub_cls'):
            type(self)._stub_cls = _make_stub_class()
        return type(self)._stub_cls

    # ---- Gate-tier tests ----

    def test_degenerate_answerable_returns_failed_no_write(self):
        """Universe of 100 with all 100 permanent skips → answerable=0
        → Failed, no write, no swap. Same shape as MF's "everything
        404'd" outage."""
        print("\n[ak-2r8 — degenerate: answerable=0 → Failed, no write]")
        # Base run() reads the skips dict — sum matches total → answerable=0.
        stub = self._StubTask(
            fetch_result=(
                {"data": []}, 100, 0,
                {"permanent_404": 100},   # → answerable = 100 - 100 = 0
                {},
            )
        )
        msg, status, interval = stub.run()
        self.assertEqual(status, "Failed")
        self.assertFalse(
            stub.save_json_called,
            "save_json fired on degenerate — wasted disk write",
        )
        self.assertFalse(
            stub.safe_replace_called,
            "safe_replace_file fired on degenerate — CLOBBERS last-good!",
        )
        self.assertIn("coverage degenerate", msg)
        self.assertIn("preserving last-good", msg)
        print(f"  ✓ degenerate → Failed, no writes: {msg[:100]}")

    def test_hard_floor_returns_failed_no_write(self):
        """30% success (below 50% floor) → Failed, no write, no swap.
        This is the ak-539 v2 hard-floor guarantee, now applied to
        every rate task."""
        print("\n[ak-2r8 — 30% below hard-floor → Failed, no write]")
        stub = self._StubTask(
            fetch_result=(
                {"data": [{"i": i} for i in range(30)]},
                100, 30,   # 30/100 = 30% — below 50% floor
                {}, {},
            )
        )
        msg, status, interval = stub.run()
        self.assertEqual(status, "Failed")
        self.assertFalse(
            stub.save_json_called,
            "save_json fired below hard-floor — wasted disk write",
        )
        self.assertFalse(
            stub.safe_replace_called,
            "safe_replace_file fired below hard-floor — CLOBBERS last-good!",
        )
        self.assertIn("coverage below hard floor", msg)
        self.assertIn("30.00%", msg)
        self.assertIn("preserving last-good", msg)
        print(f"  ✓ 30% → hard-floor Failed, no writes: {msg[:100]}")

    def test_partial_success_writes_but_returns_failed(self):
        """75% success (above floor, below 98%) → file IS written +
        swapped, but status returns Failed for operator visibility.
        This is the ak-539 C1 gate."""
        print("\n[ak-2r8 — 75% above floor, below 98% → written but Failed status]")
        stub = self._StubTask(
            fetch_result=(
                {"data": [{"i": i} for i in range(75)]},
                100, 75,   # 75/100 = 75% — above 50% floor, below 98%
                {}, {},
            )
        )
        msg, status, interval = stub.run()
        self.assertEqual(status, "Failed")
        self.assertTrue(
            stub.save_json_called,
            "save_json must run above hard-floor",
        )
        self.assertTrue(
            stub.safe_replace_called,
            "safe_replace_file must run above hard-floor (degraded > stale)",
        )
        self.assertIn("partial success", msg)
        self.assertIn("75.00%", msg)
        print(f"  ✓ 75% → written + partial-success Failed: {msg[:100]}")

    def test_clean_run_writes_and_returns_completed(self):
        """99% success (above 98%) → written, Completed status."""
        print("\n[ak-2r8 — 99% above threshold → written + Completed]")
        stub = self._StubTask(
            fetch_result=(
                {"data": [{"i": i} for i in range(99)]},
                100, 99,
                {}, {},
            )
        )
        msg, status, interval = stub.run()
        self.assertEqual(status, "Completed")
        self.assertTrue(stub.save_json_called)
        self.assertTrue(stub.safe_replace_called)
        self.assertEqual(msg, 'Completed successfully')
        print(f"  ✓ 99% → written + Completed: {msg}")

    def test_fetch_failure_returns_failed_no_write(self):
        """`_fetch_all` returns (None, ...) → BaseRateTask's data-is-None
        short-circuit → Failed with extras['error'] as the msg. Applied
        by AIRateTask children on LLM extraction failure."""
        print("\n[ak-2r8 — fetch failure (data=None) → Failed, no write]")
        stub = self._StubTask(
            fetch_result=(
                None, 1, 0, {},
                {'error': 'Failed to get PPF Rates: transient LLM timeout'},
            )
        )
        msg, status, interval = stub.run()
        self.assertEqual(status, "Failed")
        self.assertFalse(stub.save_json_called)
        self.assertFalse(stub.safe_replace_called)
        self.assertIn("Failed to get PPF Rates", msg)
        print(f"  ✓ fetch failure → Failed, no writes: {msg[:100]}")

    def test_universe_of_1_success_completes(self):
        """AI-extract shape: (data, 1, 1, {}, {}) → 100% success →
        Completed. Sanity check for PPF/EPF/Gold happy path."""
        print("\n[ak-2r8 — universe-of-1 success → Completed]")
        stub = self._StubTask(
            fetch_result=({"data": [{"Year": "2026-01", "Interest Rate": 8.25}]}, 1, 1, {}, {})
        )
        msg, status, interval = stub.run()
        self.assertEqual(status, "Completed")
        self.assertTrue(stub.safe_replace_called)
        print("  ✓ universe-of-1 (1/1) → Completed")

    def test_boundary_exactly_50pct_falls_through_floor(self):
        """Exact 50% doesn't trip the floor (`ratio < 0.5` is strict)
        — matches ak-539 v2 boundary test for SetMFRate. This proves
        the hoisted gate preserves the pre-refactor boundary math."""
        print("\n[ak-2r8 — exact-50% falls through the strict floor]")
        stub = self._StubTask(
            fetch_result=(
                {"data": [{"i": i} for i in range(50)]},
                100, 50,
                {}, {},
            )
        )
        msg, status, interval = stub.run()
        self.assertTrue(
            stub.safe_replace_called,
            "exact-50% must fall through (`ratio < 0.5` is strict)",
        )
        # Below 98% → partial-success Failed.
        self.assertEqual(status, "Failed")
        self.assertIn("partial success", msg)
        print("  ✓ exact-50% falls through the floor (strict inequality)")

    def test_gate_thresholds_subclass_overridable(self):
        """NPS override case: _COVERAGE_HARD_FLOOR = 0.05 → 10% coverage
        passes the floor. This proves subclasses can tune thresholds
        without touching the base."""
        print("\n[ak-2r8 — subclass override: NPS-shape floor at 5% passes 10% coverage]")

        class _NPSShape(self._StubTask):
            _MIN_SUCCESS_RATIO = 0.05
            _COVERAGE_HARD_FLOOR = 0.05

        stub = _NPSShape(
            fetch_result=(
                {"data": [{"i": i} for i in range(10)]},
                100, 10,  # 10% — well above the 5% floor
                {}, {},
            )
        )
        msg, status, interval = stub.run()
        # Above both floor and threshold → Completed.
        self.assertEqual(status, "Completed")
        self.assertTrue(stub.safe_replace_called)
        print("  ✓ NPS subclass floor override honored (10% passes 5% floor)")

    def test_permanent_skips_excluded_from_denominator(self):
        """MF 4xx-preserving shape: permanent_404 in skips-dict is
        subtracted from denominator; 4xx-in-extras stays. Simulates
        SetMFRate._fetch_all's emit shape."""
        print("\n[ak-2r8 — permanent_skips in dict → excluded from denominator]")
        stub = self._StubTask(
            fetch_result=(
                {"data": [{"i": i} for i in range(85)]},
                100, 85,
                {"permanent_404": 10},   # answerable = 90; ratio = 85/90 = 94.4%
                {"permanent_4xx": 5},    # extras — NOT subtracted
            )
        )
        msg, status, interval = stub.run()
        # 94.4% — above floor, below 98% → written + partial-success Failed.
        self.assertEqual(status, "Failed")
        self.assertTrue(stub.safe_replace_called)
        self.assertIn("partial success", msg)
        self.assertIn("85/90", msg)  # answerable = 90
        print("  ✓ permanent_404 (skips) excluded from denom; permanent_4xx (extras) stays")


if __name__ == "__main__":
    unittest.main(verbosity=2)
