"""ak-5vg v3: unit test for services.wealthDigestService._is_migration_gap.

Pure-function guard for the swallow-vs-bubble boundary at the heart of
the v2 MINOR-1 fix. Import-only test — no DB, no Flask app context.

5 test cases per reviewer's push-back:
  1. MySQL/pymysql 'Unknown column' → True  (pre-migration graceful degrade)
  2. SQLite 'no such column' → True         (pre-migration graceful degrade)
  3. Generic Exception → False              (unrelated errors bubble)
  4. MySQL deadlock (OperationalError, non-migration) → False
                                            (connection/txn errors bubble;
                                             mustn't be masked as migration)
  5. Mixed-case 'UNKNOWN COLUMN' → True     (verifies .lower() normalization)

Run:
    python3 -m unittest test_wealth_digest_migration_gap
    python3 -m pytest test_wealth_digest_migration_gap.py
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _ast_lift_is_migration_gap():
    """Load `_MIGRATION_ERROR_FRAGMENTS` + `_is_migration_gap` from
    services/wealthDigestService.py into an isolated namespace,
    stripping the heavy imports (`services.Base_Service`,
    `models.*`, etc.) that pull in the google.oauth2 / firebase /
    dolt chain unavailable in bare test envs.

    Same pattern as test_baserate_task_contract.py's AST-lift so
    pure-function guards can run without app-stack deps."""
    import ast
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "wealthDigestService.py",
    )
    with open(src_path) as fh:
        src = fh.read()
    tree = ast.parse(src)

    # Keep only the pieces `_is_migration_gap` needs: the sqlalchemy.exc
    # import + the fragments constant + the function itself. Drop
    # everything else so the exec doesn't pull in models / services.
    kept = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "sqlalchemy.exc":
            kept.append(node)
            continue
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id == "_MIGRATION_ERROR_FRAGMENTS":
                kept.append(node)
                continue
        if isinstance(node, ast.FunctionDef) and node.name == "_is_migration_gap":
            kept.append(node)
            continue
    tree.body = kept
    ns = {"__name__": "test_wealth_digest_migration_gap_ast_lift"}
    exec(compile(tree, "<_is_migration_gap-AST-lift>", "exec"), ns)
    return ns["_is_migration_gap"], ns["OperationalError"], ns["ProgrammingError"]


try:
    # Prefer real import so we exercise the actual module. Fall back
    # to AST-lift when the transitive chain is unavailable (bare env).
    from sqlalchemy.exc import OperationalError, ProgrammingError
    try:
        from services.wealthDigestService import _is_migration_gap  # noqa: F401
    except Exception:
        _is_migration_gap, OperationalError, ProgrammingError = _ast_lift_is_migration_gap()
    _IMPORT_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _IMPORT_OK = False
    _SKIP_REASON = f"import chain unavailable: {_exc}"


def _make_op_error(msg: str) -> "OperationalError":
    """Construct an OperationalError with `msg` as the error string.

    SQLAlchemy's DBAPIError family takes (statement, params, orig)
    where `orig` is the underlying driver error object. We pass a
    bare Exception carrying the message — good enough because
    `_is_migration_gap` does `str(exc).lower()` on the WHOLE
    SQLAlchemy exception, which by contract embeds `str(orig)`.
    """
    return OperationalError("SELECT 1", {}, Exception(msg))


def _make_prog_error(msg: str) -> "ProgrammingError":
    return ProgrammingError("SELECT 1", {}, Exception(msg))


class TestImportChannel(unittest.TestCase):
    """ak-6p4 v2 (reviewer MINOR-1 backport): un-decorated guard that
    fails LOUDLY if both real-import AND AST-lift channels break.
    Without this, `@unittest.skipUnless(_IMPORT_OK, ...)` on the
    behavioral class would SKIP every test and unittest would still
    report OK — a regression could ship green. Recurring AST-lift
    pattern; codify the guard now."""

    def test_import_channel_is_live(self):
        self.assertTrue(_IMPORT_OK, _SKIP_REASON)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestIsMigrationGap(unittest.TestCase):
    """5 cases pinning the pre-migration vs genuine-DB-error boundary."""

    def test_mysql_unknown_column_is_migration_gap(self):
        exc = _make_op_error(
            "1054 (42S22): Unknown column 'wealth_digest_last_read_at' "
            "in 'field list'"
        )
        self.assertTrue(_is_migration_gap(exc))

    def test_sqlite_no_such_column_is_migration_gap(self):
        exc = _make_op_error("no such column: wealth_digest_last_read_at")
        self.assertTrue(_is_migration_gap(exc))

    def test_generic_exception_is_not_migration_gap(self):
        exc = Exception("some other error")
        self.assertFalse(_is_migration_gap(exc))

    def test_deadlock_operationalerror_is_not_migration_gap(self):
        """A REAL DB error class (OperationalError) whose message is not
        column-missing must NOT match — deadlock / connection loss
        should bubble to the endpoint handler, not silently degrade
        as pre-migration state."""
        exc = _make_op_error(
            "1213 (40001): Deadlock found when trying to get lock; "
            "try restarting transaction"
        )
        self.assertFalse(_is_migration_gap(exc))

    def test_mixed_case_unknown_column_normalizes(self):
        """Verify the .lower() normalization in _is_migration_gap
        matches upper- and mixed-case variants some drivers emit."""
        exc = _make_op_error("UNKNOWN COLUMN 'x' in 'field list'")
        self.assertTrue(_is_migration_gap(exc))

    def test_programming_error_no_such_column_matches(self):
        """Some drivers surface column-missing as ProgrammingError
        instead of OperationalError — both must match."""
        exc = _make_prog_error("no such column: x")
        self.assertTrue(_is_migration_gap(exc))

    def test_non_db_exception_with_matching_text_is_not_migration_gap(self):
        """ak-6p4 v2 (reviewer isinstance-guard backport from ak-5vg
        v3 landing pass): a plain Exception whose message HAPPENS to
        contain 'unknown column' must NOT return True. The isinstance
        check gates on the SQLAlchemy exception class hierarchy
        (OperationalError / ProgrammingError only) so an application-
        code raise like `raise ValueError('Unknown column x — user
        input mismatch')` doesn't silently degrade to pre-migration
        state. Pins the isinstance guard against future refactor."""
        exc = Exception("Unknown column 'x' in 'field list'")
        self.assertFalse(_is_migration_gap(exc))
        # Also pin the SQLite variant on a plain Exception.
        exc_sqlite = Exception("no such column: x")
        self.assertFalse(_is_migration_gap(exc_sqlite))


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
