"""ak-pib P0 hotfix tests: scheduler.py's __main__ standalone-entry
block MUST import the fully-configured `app` from app.py — not
create a bare Flask(__name__) with no db attached.

Background (root cause):
  Post-ak-ojd Path B v2 (commit 036e995), both scheduler loops set
  `g.db = self.flask_app.db` at task-invocation boundary as
  defense-in-depth. Also, per-task/per-tick session cleanup calls
  `self.flask_app.db.session.remove()` in a finally block. If
  scheduler.py is run as `python -m services.tasks.scheduler`, the
  __main__ block was previously constructing a bare `Flask(__name__)`
  with no `.db` attribute — the first tick would crash both threads
  with AttributeError: 'Flask' object has no attribute 'db',
  taking the fleet down at init.

Fix (Option A — mirrors reprocess_*.py / retry_failed_pdfs.py):
  Inside `if __name__ == "__main__":`, import the configured Akkountant
  instance from app.py (`from app import app as flask_app`). Placing
  the import INSIDE the guard avoids the circular-import trap: at
  module-import time (when app.py does
  `from services.tasks.scheduler import TaskScheduler`), the __main__
  block doesn't execute. At entry-point time (`python -m
  services.tasks.scheduler`), app.py has not yet been imported, so
  the fresh import succeeds and yields the app with `.db` attached.

Tests are source-inspection only (AST-lifted style, no runtime deps)
so they run cleanly in a bare test env — the app.py import chain
pulls in google.oauth2, firebase, dolt, etc., which are not always
present in verification environments.

Run:
    python3 -m unittest test_scheduler_init
    python3 -m pytest test_scheduler_init.py
"""

import ast
import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_SCHEDULER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "scheduler.py",
)


def _source(path):
    with open(path) as fh:
        return fh.read()


def _find_main_block(tree):
    """Return the body of the `if __name__ == "__main__":` block, or
    None if not present."""
    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        # Match: __name__ == "__main__"
        if (isinstance(test, ast.Compare)
                and isinstance(test.left, ast.Name)
                and test.left.id == "__name__"
                and len(test.ops) == 1
                and isinstance(test.ops[0], ast.Eq)
                and len(test.comparators) == 1
                and isinstance(test.comparators[0], ast.Constant)
                and test.comparators[0].value == "__main__"):
            return node.body
    return None


class TestAkPibSchedulerMainBlockImportsConfiguredApp(unittest.TestCase):
    """The __main__ standalone-entry block must import the configured
    Akkountant `app` (with .db attached) from app.py, NOT construct a
    bare Flask(__name__)."""

    def setUp(self):
        self.src = _source(_SCHEDULER_PATH)
        self.tree = ast.parse(self.src)
        self.main_body = _find_main_block(self.tree)
        self.assertIsNotNone(
            self.main_body,
            "scheduler.py must have an `if __name__ == \"__main__\":` block",
        )

    def test_main_block_imports_app_from_app_py(self):
        """The __main__ block must contain `from app import app ...`
        (mirrors reprocess_*.py / retry_failed_pdfs.py pattern)."""
        print("\n[ak-pib — __main__ imports configured app from app.py]")
        found = False
        for node in self.main_body:
            if isinstance(node, ast.ImportFrom) and node.module == "app":
                for alias in node.names:
                    if alias.name == "app":
                        found = True
                        break
        self.assertTrue(
            found,
            "scheduler.py __main__ must contain `from app import app ...`; "
            "bare Flask(__name__) has no `.db` attribute → scheduler "
            "loops crash at first tick (ak-pib P0)",
        )
        print("  ✓ __main__ imports `app` from app.py")

    def test_main_block_does_not_construct_bare_flask(self):
        """The __main__ block must NOT construct a bare Flask(__name__)
        — that would have no .db attribute and re-introduce ak-pib."""
        print("\n[ak-pib — __main__ does not construct bare Flask(__name__)]")
        for node in ast.walk(ast.Module(body=self.main_body, type_ignores=[])):
            if isinstance(node, ast.Call):
                func = node.func
                # Direct name: Flask(...)
                if isinstance(func, ast.Name) and func.id == "Flask":
                    self.fail(
                        "scheduler.py __main__ still constructs a bare "
                        "Flask(...) — this has no `.db` attribute and "
                        "reintroduces ak-pib. Use `from app import app` "
                        "instead."
                    )
        print("  ✓ no bare Flask(...) construction in __main__")

    def test_main_block_passes_configured_app_to_scheduler(self):
        """TaskScheduler(...) must receive the imported configured
        app via flask_app= kwarg — the app that has `.db` attached."""
        print("\n[ak-pib — TaskScheduler receives configured app]")
        # Locate the TaskScheduler(...) call in __main__.
        target = None
        for node in ast.walk(ast.Module(body=self.main_body, type_ignores=[])):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id == "TaskScheduler":
                    target = node
                    break
        self.assertIsNotNone(
            target, "scheduler.py __main__ must instantiate TaskScheduler(...)",
        )
        # Must pass flask_app=<Name> where <Name> is bound by the
        # `from app import app [as X]` import above.
        kw_flask_app = None
        for kw in target.keywords:
            if kw.arg == "flask_app":
                kw_flask_app = kw
                break
        self.assertIsNotNone(
            kw_flask_app,
            "TaskScheduler(...) call must pass flask_app= kwarg",
        )
        self.assertIsInstance(
            kw_flask_app.value, ast.Name,
            "flask_app= must reference a Name (the imported app), not a "
            "fresh Flask(...) construction",
        )
        # The referenced name must match what `from app import app [as X]`
        # binds locally.
        bound_name = None
        for node in self.main_body:
            if isinstance(node, ast.ImportFrom) and node.module == "app":
                for alias in node.names:
                    if alias.name == "app":
                        bound_name = alias.asname or alias.name
                        break
        self.assertEqual(
            kw_flask_app.value.id, bound_name,
            f"flask_app= must reference the imported app (`{bound_name}`), "
            f"got `{kw_flask_app.value.id}`",
        )
        print(f"  ✓ TaskScheduler receives `{bound_name}` (imported from app.py)")


class TestAkPibDocstringComment(unittest.TestCase):
    """Regression-doc hygiene: the __main__ block must carry a comment
    that names ak-pib so future readers know why the import lives here
    (not at top-of-file) and don't 'clean it up' back to the broken
    shape."""

    def test_main_block_references_ak_pib(self):
        print("\n[ak-pib — __main__ carries ak-pib provenance comment]")
        src = _source(_SCHEDULER_PATH)
        # Grep for ak-pib somewhere in the file (comment or docstring).
        self.assertIn(
            "ak-pib", src,
            "scheduler.py must reference ak-pib so future readers "
            "understand why `from app import app` is inside the "
            "__main__ guard (import-cycle avoidance).",
        )
        # And the reference must be within reach of the __main__ block.
        # Find the line number of `if __name__ == "__main__":`.
        lines = src.splitlines()
        main_line = None
        for i, ln in enumerate(lines):
            if ln.startswith("if __name__ ==") and "\"__main__\"" in ln:
                main_line = i
                break
        self.assertIsNotNone(main_line, "no __main__ guard found")
        # ak-pib mention within 20 lines below the guard.
        nearby = "\n".join(lines[main_line: main_line + 40])
        self.assertIn(
            "ak-pib", nearby,
            "ak-pib comment must live near the __main__ block for "
            "future-reader context",
        )
        print("  ✓ ak-pib provenance comment present near __main__")


class TestAkPibImportCycleGuardIsInsideMainBlock(unittest.TestCase):
    """The `from app import app` MUST be INSIDE the __main__ guard,
    NOT at top-of-file. app.py imports scheduler at module-load time
    (`from services.tasks.scheduler import TaskScheduler`); a top-of-
    file `from app import app` in scheduler.py would create a hard
    import cycle at module-import time. Placing it inside the guard
    means it only fires when scheduler.py is the entry-point script,
    at which moment app.py is not yet on the import stack."""

    def test_no_top_level_from_app_import(self):
        print("\n[ak-pib — `from app import ...` is NOT at module top level]")
        src = _source(_SCHEDULER_PATH)
        tree = ast.parse(src)
        for node in tree.body:
            if isinstance(node, ast.ImportFrom) and node.module == "app":
                self.fail(
                    "Top-level `from app import ...` in scheduler.py "
                    "creates an import cycle with app.py:32 "
                    "(`from services.tasks.scheduler import TaskScheduler`). "
                    "Move the import INSIDE `if __name__ == \"__main__\":`."
                )
        print("  ✓ no top-level `from app import ...` (cycle-safe)")


if __name__ == "__main__":
    unittest.main(verbosity=2)
