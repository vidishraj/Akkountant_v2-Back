"""ak-l0u (2026-11-22): tests for the defensive flask_app.db guard
in TaskScheduler init + per-thread boundary.

ak-pib was a P0 fleet-down: scheduler.py's standalone __main__
constructed a bare ``Flask(__name__)`` with no ``.db`` attached;
both scheduler threads crashed at init on
``AttributeError: 'Flask' object has no attribute 'db'`` and
systemctl misleadingly reported active(running) because Flask
:5000 stayed up. ak-l0u is the belt-and-braces insurance: a
loud, greppable, actionable RuntimeError at exactly the
construction/boundary moments where that regression would
re-manifest.

This file verifies without a real Flask or DB:

  1. ``TaskScheduler.__init__`` raises RuntimeError when flask_app
     is provided but lacks ``.db``.
  2. ``TaskScheduler.__init__`` allows flask_app=None through (test
     harness + scheduler-less composition path).
  3. ``TaskScheduler.__init__`` allows a flask_app WITH ``.db`` set
     (happy path).
  4. The raised message is greppable: names ak-pib AND ak-l0u,
     names ``.db``, and points at ``from app import app`` as the
     fix.
  5. Both ``_run_overdue_scheduler`` and ``_run_job_processor``
     carry a symmetric post-init slip guard at the thread
     boundary (source-grep drift guard).

AST-lifted ``__init__`` body for runtime behavior; source-inspection
for the symmetric per-thread guards. Mirrors test_scheduler_init
(the ak-pib test file)'s AST pattern for consistency.
"""

import ast
import os
import re
import sys
import types
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_SCHEDULER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "scheduler.py",
)


def _source(path):
    with open(path) as fh:
        return fh.read()


def _ast_lift_init_guard():
    """Extract just the ak-l0u guard block out of
    ``TaskScheduler.__init__`` so we can exercise it as a
    free-standing callable. Avoids importing the real scheduler
    (which pulls Flask-SQLAlchemy + models + google chain)."""
    src = _source(_SCHEDULER_PATH)
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "TaskScheduler":
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                    # Build a free-standing function that executes ONLY
                    # the guard (the `if flask_app is not None and
                    # not hasattr(flask_app, "db"): raise RuntimeError(...)`
                    # block). We walk the __init__ body and pull just
                    # that `If` node — identified by its condition
                    # matching `flask_app is not None and not hasattr`.
                    for stmt in item.body:
                        if (
                            isinstance(stmt, ast.If)
                            and isinstance(stmt.test, ast.BoolOp)
                            and isinstance(stmt.test.op, ast.And)
                            and any(
                                isinstance(v, ast.Compare)
                                and isinstance(v.left, ast.Name)
                                and v.left.id == "flask_app"
                                for v in stmt.test.values
                            )
                        ):
                            # Wrap in a function that accepts flask_app.
                            fn = ast.FunctionDef(
                                name="_run_guard",
                                args=ast.arguments(
                                    posonlyargs=[],
                                    args=[ast.arg(arg="flask_app")],
                                    kwonlyargs=[],
                                    kw_defaults=[],
                                    defaults=[],
                                ),
                                body=[stmt],
                                decorator_list=[],
                                returns=None,
                            )
                            mod = ast.Module(body=[fn], type_ignores=[])
                            ast.fix_missing_locations(mod)
                            ns = {}
                            exec(compile(mod, "<extract>", "exec"), ns)
                            return ns["_run_guard"]
    raise RuntimeError(
        "ak-l0u guard block not found in TaskScheduler.__init__"
    )


class TestAkL0uInitGuardBehavior(unittest.TestCase):
    """Runtime behavior of the __init__ guard (AST-lifted body)."""

    def setUp(self):
        self.run_guard = _ast_lift_init_guard()

    def test_raises_on_bare_flask_without_db(self):
        """ak-pib's exact regression shape: a Flask-like object
        WITHOUT a .db attribute. Guard must raise RuntimeError."""
        bare_flask = types.SimpleNamespace(name="bare")  # no .db
        with self.assertRaises(RuntimeError) as cm:
            self.run_guard(bare_flask)
        msg = str(cm.exception)
        self.assertIn(".db", msg)
        # Message must name ak-pib (incident) + ak-l0u (guard) so a
        # future reader has both the "why this exists" and "which
        # bead to search" — crucial for a greppable error.
        self.assertIn("ak-pib", msg)
        self.assertIn("ak-l0u", msg)
        # And point at the fix: `from app import app`.
        self.assertIn("from app import app", msg)

    def test_allows_none_through(self):
        """flask_app=None is the current default signature. Test
        harness + scheduler-less composition path must still
        construct without the guard firing (it'd break every
        test suite that doesn't wire a Flask)."""
        # No exception expected.
        self.run_guard(None)

    def test_allows_flask_app_with_db_through(self):
        """Happy path: a Flask with .db attached passes."""
        configured_app = types.SimpleNamespace()
        configured_app.db = MagicMock()
        # No exception expected.
        self.run_guard(configured_app)


class TestAkL0uInitGuardSource(unittest.TestCase):
    """Source-level drift guards — the guard structure must
    survive refactors."""

    def setUp(self):
        self.src = _source(_SCHEDULER_PATH)

    def test_init_guard_present(self):
        """Guard must exist in TaskScheduler.__init__."""
        # Find the __init__ body.
        tree = ast.parse(self.src)
        init_body_src = None
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "TaskScheduler":
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                        init_body_src = ast.get_source_segment(self.src, item)
                        break
        self.assertIsNotNone(init_body_src, "__init__ not found")
        # Key semantic tokens must all appear.
        self.assertIn("flask_app is not None", init_body_src)
        self.assertIn('hasattr(flask_app, "db")', init_body_src)
        self.assertIn("raise RuntimeError", init_body_src)

    def test_init_guard_does_not_fire_on_none_sentinel(self):
        """Structural: the guard condition must gate on
        ``flask_app is not None`` BEFORE the hasattr check, so
        None flask_app passes. A naive `not hasattr(None, 'db')`
        would fire on the test-harness default."""
        self.assertIn(
            "flask_app is not None and not hasattr(flask_app",
            self.src,
            "guard must short-circuit on flask_app=None — the "
            "current __init__ signature allows None and the test "
            "harness relies on that",
        )

    def test_init_guard_message_is_greppable(self):
        """Error message must carry the distinguishing tokens a
        future operator would grep for during an incident. Runtime
        check: build the message via the AST-lifted guard and
        scan the raised string (not source text — source has
        literal-wrap whitespace that would break a contiguous
        substring grep on 'threads dead at init')."""
        run_guard = _ast_lift_init_guard()
        bare_flask = types.SimpleNamespace(name="bare")
        with self.assertRaises(RuntimeError) as cm:
            run_guard(bare_flask)
        msg = str(cm.exception)
        # Distinguishing tokens: incident bead + guard bead +
        # fix pointer + symptom.
        self.assertIn("ak-pib", msg)
        self.assertIn("ak-l0u", msg)
        self.assertIn("from app import app", msg)
        self.assertIn("scheduler threads dead at init", msg)
        # And the message must mention the misleading-systemctl
        # symptom so an operator reading the raised error
        # immediately matches it to the "active(running)" lie
        # they just saw.
        self.assertIn("active(running)", msg)


class TestAkL0uPerThreadBoundaryGuards(unittest.TestCase):
    """Both _run_* methods must carry a symmetric guard at thread
    entry (belt-and-braces for post-__init__ invariant slip)."""

    def setUp(self):
        self.src = _source(_SCHEDULER_PATH)
        self.tree = ast.parse(self.src)

    def _method_source(self, method_name):
        for node in self.tree.body:
            if isinstance(node, ast.ClassDef) and node.name == "TaskScheduler":
                for item in node.body:
                    if (
                        isinstance(item, ast.FunctionDef)
                        and item.name == method_name
                    ):
                        return ast.get_source_segment(self.src, item)
        return None

    def test_overdue_scheduler_has_boundary_guard(self):
        body = self._method_source("_run_overdue_scheduler")
        self.assertIsNotNone(body, "_run_overdue_scheduler not found")
        self.assertIn('hasattr(self.flask_app, "db")', body)
        self.assertIn("raise RuntimeError", body)
        self.assertIn("ak-l0u", body)

    def test_job_processor_has_boundary_guard(self):
        body = self._method_source("_run_job_processor")
        self.assertIsNotNone(body, "_run_job_processor not found")
        self.assertIn('hasattr(self.flask_app, "db")', body)
        self.assertIn("raise RuntimeError", body)
        self.assertIn("ak-l0u", body)

    def test_boundary_guards_precede_g_db_assignment(self):
        """The guard must appear BEFORE the ``g.db = self.flask_app.db``
        line in both _run_* methods — otherwise the AttributeError
        beats the guard to the stack trace and the whole point of
        'loud-at-boundary' is lost."""
        for method in ("_run_overdue_scheduler", "_run_job_processor"):
            body = self._method_source(method)
            self.assertIsNotNone(body)
            guard_pos = body.find('hasattr(self.flask_app, "db")')
            assign_pos = body.find("g.db = self.flask_app.db")
            self.assertGreater(
                guard_pos, -1, f"{method}: guard literal missing"
            )
            self.assertGreater(
                assign_pos, -1, f"{method}: g.db assignment missing"
            )
            self.assertLess(
                guard_pos, assign_pos,
                f"{method}: boundary guard must precede "
                "'g.db = self.flask_app.db' assignment",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
