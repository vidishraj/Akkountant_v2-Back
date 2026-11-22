"""ak-cipj: BE read + resolve endpoint tests for the trade-
reconciliation surface.

Scope:
  * Service layer (ReconciliationService.list_trade_issues +
    resolve_trade_issue) against a real in-memory SQLite — the same
    discipline test_conv_service_db uses to pin down session-shape
    bugs that mocks hide.
  * Controller/route source-grep — matching
    test_kite_bundle2.py::test_bundle2_tool_dispatched_in_executor
    pattern. Full controller-import pulls the Flask + Firebase chain,
    which isn't available in CI; a source-level grep locks the shape
    + route wire-up without fighting that boot.
  * Model shape check: ``resolution_note`` column is present +
    nullable + length-bounded (schema addition this bead landed).

Baseline discipline (per cross-arc precedent): tests must skip
cleanly when deps are unavailable; they MUST NOT add to the
existing 11-errors/7-failed fleet baseline. All service-layer tests
run under a ``@unittest.skipUnless(_DEPS_OK)`` guard.

What these tests pin down:
  1. ``status='open'`` filters resolved_at IS NULL (default).
  2. ``status='resolved'`` filters resolved_at IS NOT NULL.
  3. ``status='all'`` returns everything (open + resolved).
  4. ``status=<junk>`` silently falls back to 'open' at the service
     boundary (defense in depth — controller rejects at 400 but a
     direct-caller gets a safe default rather than leaking all rows).
  5. Listing is user_id-scoped: user A does not see user B's rows.
  6. Listing is ordered by detected_at DESC (newest first).
  7. discrepancy_detail is JSON-parsed in the response (not a
     string-inside-string on the wire).
  8. ``discrepancy_detail`` non-JSON pass-through — defensive.
  9. ``resolve_trade_issue`` on an open row writes resolved_at +
     resolution_note + returns ``idempotent=False``.
  10. ``resolve_trade_issue`` on an already-resolved row returns
      ``idempotent=True`` with the ORIGINAL resolved_at (not now()).
  11. ``resolve_trade_issue`` on a nonexistent id returns 404.
  12. ``resolve_trade_issue`` on a different user's id returns 404
      (not 403) — must not leak existence across users.
  13. ``resolution_note`` capped at 500 chars (truncated silently).
  14. Controller routes + handler shape are present (source grep).
  15. Model has ``resolution_note`` column + nullability.
"""

import ast
import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _ast_lift_trade_issue_methods():
    """AST-lift the two trade-issue service methods +
    _serialize_trade_issue + the two module-level constants from
    ``services/reconciliationService.py`` so the test file can
    exercise them WITHOUT importing BaseService (which pulls the
    Gmail / Drive / Google-auth chain unavailable in this env).

    Same pattern as ``test_fx_idempotency._ast_lift_currency_write_
    path`` — minimal re-execution of the methods we added, no boot
    chain, no network. Paired with a real in-memory SQLite + the
    real ``ReconciliationIssue`` model, we get full behavioural
    coverage of the read + resolve paths.

    Returns a dict with keys::

      {
        'list_trade_issues': <function>,
        'resolve_trade_issue': <function>,
        '_serialize_trade_issue': <function>,
        '_LIST_STATUS_VALID': <frozenset>,
        '_RESOLUTION_NOTE_MAX_LEN': <int>,
      }

    The methods expect a `self` carrying `.db.session` + `.logger`
    (the stubbed ``_StubReconService`` below supplies both).
    """
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "reconciliationService.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()

    tree = ast.parse(src)

    # Harvest module-level constants + the three methods off
    # ReconciliationService (ignore everything else — reconcile_
    # transfers etc. live in the same class and would drag in
    # transactionsService constants we don't need).
    kept_nodes = []
    want_constants = {"_RESOLUTION_NOTE_MAX_LEN", "_LIST_STATUS_VALID"}
    want_methods = {
        "list_trade_issues",
        "resolve_trade_issue",
        "_serialize_trade_issue",
    }

    for node in tree.body:
        # Module-level constants.
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            tgt = node.targets[0]
            if isinstance(tgt, ast.Name) and tgt.id in want_constants:
                kept_nodes.append(node)
                continue
        # ReconciliationService class — pull out just the methods
        # we want, drop everything else.
        if isinstance(node, ast.ClassDef) and node.name == "ReconciliationService":
            for child in node.body:
                if isinstance(
                    child, (ast.FunctionDef, ast.AsyncFunctionDef)
                ) and child.name in want_methods:
                    # Strip decorators (e.g. @staticmethod) so the
                    # lifted method is a plain callable at module
                    # scope. The methods we lift don't need their
                    # class-time binding — _serialize_trade_issue
                    # takes a single positional arg (`row`) and
                    # the two instance methods take (self, ...).
                    child.decorator_list = []
                    kept_nodes.append(child)

    lift_tree = ast.Module(body=kept_nodes, type_ignores=[])
    ast.fix_missing_locations(lift_tree)

    # Namespace: give the exec'd code access to datetime + json +
    # the real ReconciliationIssue model (so the queries resolve to
    # the same table the SQLite setup created).
    import datetime
    import json as _json
    ns = {
        "__name__": "test_reconciliation_ast_lift",
        "datetime": datetime.datetime,  # method body uses `datetime.utcnow()`
        "json": _json,
        "ReconciliationIssue": ReconciliationIssue,
    }
    exec(compile(lift_tree, "<recon-service-AST-lift>", "exec"), ns)
    return {
        "list_trade_issues": ns["list_trade_issues"],
        "resolve_trade_issue": ns["resolve_trade_issue"],
        "_serialize_trade_issue": ns["_serialize_trade_issue"],
        "_LIST_STATUS_VALID": ns["_LIST_STATUS_VALID"],
        "_RESOLUTION_NOTE_MAX_LEN": ns["_RESOLUTION_NOTE_MAX_LEN"],
    }


# Split import guards so the model-shape + AST-lift service tests
# don't skip when the full ReconciliationService boot chain isn't
# available (bare env — no google.oauth2 / firebase). Only the
# ReconciliationIssue model + SQLAlchemy need to import for us; the
# service methods come in via AST lift (see
# _ast_lift_trade_issue_methods above).
try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    # Pull models/__init__.py first so User + its relationship
    # targets register in the declarative registry. Then explicitly
    # import InvestmentHistory — User.investment_history references
    # it by name string but models/__init__.py doesn't import it
    # (ReconciliationIssue's FK to users.userID triggers the User
    # mapper init on first insert, which fails with
    # "could not locate InvestmentHistory" unless the class is
    # already registered).
    import models  # noqa: F401 — side-effect: register declarative
    from models.investmentHistory import (  # noqa: F401 — same reason
        InvestmentHistory,
    )
    from models.reconciliationIssue import ReconciliationIssue
    _MODEL_DEPS_OK = True
    _MODEL_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover — env-dep skip
    _MODEL_DEPS_OK = False
    _MODEL_SKIP_REASON = (
        f"ReconciliationIssue model import failed: {_exc}"
    )

# AST-lift the two service methods out of reconciliationService.py.
# This is PURE SOURCE PARSING — it does NOT import Base_Service or
# the Google auth chain, so there is no env-dep skip reason here.
# A failure here means the methods or module constants were
# removed / renamed — that's a code bug we WANT to red, not skip.
# We still check that _MODEL_DEPS_OK to guard the exec step (which
# needs the real ReconciliationIssue model to bind to the namespace),
# but we don't swallow AST failures as env-skip.
if _MODEL_DEPS_OK:
    _LIFTED = _ast_lift_trade_issue_methods()
    # Loud guard: if any expected symbol is absent from the lifted
    # namespace, that's a tight regression signal (someone deleted
    # or renamed a method). Red the AST-lift instead of silently
    # skipping — matches the mutation-discipline semantic.
    _missing = [
        k for k in (
            "list_trade_issues", "resolve_trade_issue",
            "_serialize_trade_issue",
            "_LIST_STATUS_VALID", "_RESOLUTION_NOTE_MAX_LEN",
        ) if _LIFTED.get(k) is None
    ]
    if _missing:
        raise AssertionError(
            f"ak-cipj service-method AST-lift missing symbols: "
            f"{_missing}. Did they get renamed or removed from "
            f"services/reconciliationService.py?"
        )
    _SVC_DEPS_OK = True
    _SVC_SKIP_REASON = ""
else:
    _LIFTED = None
    _SVC_DEPS_OK = False
    _SVC_SKIP_REASON = (
        f"AST-lift skipped because model import failed: "
        f"{_MODEL_SKIP_REASON}"
    )

# Alias kept for symmetry with other test files in this repo.
_DEPS_OK = _SVC_DEPS_OK
_SKIP_REASON = _SVC_SKIP_REASON


class _StubReconService:
    """Thin stand-in for ReconciliationService — provides the `self.db
    .session` + `self.logger` surfaces the AST-lifted methods expect,
    with zero Flask / Firebase / Google-auth boot. Used by the
    service-layer tests to exercise list_trade_issues +
    resolve_trade_issue against a real in-memory SQLite session.
    """

    def __init__(self, session_factory):
        self._session_factory = session_factory
        self._cached_session = None
        self.logger = _NoopLogger()

    @property
    def db(self):
        return self  # self.db.session → self.session via __getattr__

    @property
    def session(self):
        if self._cached_session is None:
            self._cached_session = self._session_factory()
        return self._cached_session


class _NoopLogger:
    """Silent logger stub — swallows error()/warning()/info() calls
    the service methods emit on the sad path. Tests assert on the
    returned shape, not on log output."""

    def error(self, *_a, **_k):
        pass

    def warning(self, *_a, **_k):
        pass

    def info(self, *_a, **_k):
        pass


def _make_svc(session_factory):
    """Build a _StubReconService with the AST-lifted methods bound.

    Since the lifted methods are plain `def` with `self` first-arg,
    we attach them as instance methods via types.MethodType. Done
    here (in a helper) rather than at _StubReconService.__init__ so
    the module can import cleanly even when _LIFTED is None (bare
    env where AST-lift failed — tests skip via _DEPS_OK guard).
    """
    import types
    svc = _StubReconService(session_factory)
    svc.list_trade_issues = types.MethodType(
        _LIFTED["list_trade_issues"], svc,
    )
    svc.resolve_trade_issue = types.MethodType(
        _LIFTED["resolve_trade_issue"], svc,
    )
    # _serialize_trade_issue is @staticmethod in the real class and
    # called as `self._serialize_trade_issue(row)` from the instance
    # methods. Attach the raw function directly at the instance level
    # — Python's attribute lookup here bypasses descriptor protocol
    # (which only applies to CLASS-level staticmethods), and the raw
    # function invokes correctly via `self._serialize_trade_issue(r)`
    # because it takes a single positional arg.
    svc._serialize_trade_issue = _LIFTED["_serialize_trade_issue"]
    return svc


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestListTradeIssues(unittest.TestCase):
    """Service layer: list_trade_issues against real in-memory SQLite.

    Covers the 8 pin-downs for the read path (points 1-8 in the
    module-level scope).
    """

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        # Only create the table we need. Full Base.metadata.create_all
        # would drag the Firebase/Google-auth chain through model
        # imports (same pattern as test_conv_service_db).
        ReconciliationIssue.__table__.create(self.engine)
        self._SessionFactory = scoped_session(
            sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine,
            )
        )
        self.svc = _make_svc(self._SessionFactory)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    # Helper: insert a raw ReconciliationIssue row bypassing
    # ReconcileTradesTask's upsert path (we want deterministic
    # control over resolved_at + detected_at for sort / filter
    # assertions).
    def _insert(self, **kwargs):
        from datetime import datetime
        kwargs.setdefault("detected_at", datetime(2026, 11, 22, 12, 0, 0))
        row = ReconciliationIssue(**kwargs)
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (1) status=open → resolved_at IS NULL only
    def test_status_open_returns_only_unresolved(self):
        from datetime import datetime
        self._insert(
            user_id="u1", kite_trade_id="T1",
            discrepancy_type="missing_from_statement",
            resolved_at=None,  # open
        )
        self._insert(
            user_id="u1", kite_trade_id="T2",
            discrepancy_type="missing_from_statement",
            resolved_at=datetime(2026, 11, 20, 10, 0, 0),  # resolved
        )
        result = self.svc.list_trade_issues("u1", status="open")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["kite_trade_id"], "T1")
        self.assertIsNone(result[0]["resolved_at"])

    # (2) status=resolved → resolved_at IS NOT NULL only
    def test_status_resolved_returns_only_resolved(self):
        from datetime import datetime
        self._insert(
            user_id="u1", kite_trade_id="T1",
            discrepancy_type="missing_from_statement",
            resolved_at=None,
        )
        self._insert(
            user_id="u1", kite_trade_id="T2",
            discrepancy_type="missing_from_statement",
            resolved_at=datetime(2026, 11, 20, 10, 0, 0),
        )
        result = self.svc.list_trade_issues("u1", status="resolved")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["kite_trade_id"], "T2")
        self.assertIsNotNone(result[0]["resolved_at"])

    # (3) status=all → everything
    def test_status_all_returns_both(self):
        from datetime import datetime
        self._insert(
            user_id="u1", kite_trade_id="T1",
            discrepancy_type="missing_from_statement",
            resolved_at=None,
        )
        self._insert(
            user_id="u1", kite_trade_id="T2",
            discrepancy_type="missing_from_statement",
            resolved_at=datetime(2026, 11, 20, 10, 0, 0),
        )
        result = self.svc.list_trade_issues("u1", status="all")
        trade_ids = {r["kite_trade_id"] for r in result}
        self.assertEqual(trade_ids, {"T1", "T2"})

    # (4) unknown status silently falls back to 'open' (defense in
    # depth — controller 400s, but a direct caller shouldn't leak
    # all rows)
    def test_unknown_status_falls_back_to_open(self):
        from datetime import datetime
        self._insert(
            user_id="u1", kite_trade_id="T1",
            discrepancy_type="missing_from_statement",
            resolved_at=None,
        )
        self._insert(
            user_id="u1", kite_trade_id="T2",
            discrepancy_type="missing_from_statement",
            resolved_at=datetime(2026, 11, 20, 10, 0, 0),
        )
        result = self.svc.list_trade_issues("u1", status="EXFILTRATE")
        self.assertEqual(len(result), 1, (
            "unknown status must fall back to 'open' (not leak "
            "resolved rows or return all)"
        ))
        self.assertEqual(result[0]["kite_trade_id"], "T1")

    # (5) user_id scoping — user A's query never sees user B's rows
    def test_list_is_user_id_scoped(self):
        self._insert(
            user_id="u1", kite_trade_id="T1",
            discrepancy_type="missing_from_statement",
        )
        self._insert(
            user_id="u2", kite_trade_id="T2",
            discrepancy_type="missing_from_statement",
        )
        result_u1 = self.svc.list_trade_issues("u1", status="open")
        self.assertEqual(len(result_u1), 1)
        self.assertEqual(result_u1[0]["kite_trade_id"], "T1")
        # u1 must NOT see u2's row
        self.assertNotIn(
            "T2", [r["kite_trade_id"] for r in result_u1],
            "list_trade_issues leaked u2's row to u1 query",
        )

    # (6) ordering by detected_at DESC
    def test_results_sorted_by_detected_at_desc(self):
        from datetime import datetime
        self._insert(
            user_id="u1", kite_trade_id="T_OLD",
            discrepancy_type="missing_from_statement",
            detected_at=datetime(2026, 11, 20, 10, 0, 0),
        )
        self._insert(
            user_id="u1", kite_trade_id="T_NEW",
            discrepancy_type="missing_from_statement",
            detected_at=datetime(2026, 11, 22, 10, 0, 0),
        )
        self._insert(
            user_id="u1", kite_trade_id="T_MID",
            discrepancy_type="missing_from_statement",
            detected_at=datetime(2026, 11, 21, 10, 0, 0),
        )
        result = self.svc.list_trade_issues("u1", status="open")
        trade_ids = [r["kite_trade_id"] for r in result]
        self.assertEqual(trade_ids, ["T_NEW", "T_MID", "T_OLD"])

    # (7) discrepancy_detail is JSON-parsed in the response
    def test_discrepancy_detail_is_json_parsed_on_output(self):
        import json
        self._insert(
            user_id="u1", kite_trade_id="T1",
            discrepancy_type="missing_from_statement",
            discrepancy_detail=json.dumps({
                "symbol": "RELIANCE", "quantity": 10, "price": "2900.50",
            }),
        )
        result = self.svc.list_trade_issues("u1", status="open")
        detail = result[0]["discrepancy_detail"]
        # Structured dict on the wire — not a json-string-inside-json.
        self.assertIsInstance(detail, dict)
        self.assertEqual(detail["symbol"], "RELIANCE")
        self.assertEqual(detail["quantity"], 10)

    # (8) non-JSON discrepancy_detail pass-through (defensive)
    def test_non_json_detail_passes_through_as_string(self):
        self._insert(
            user_id="u1", kite_trade_id="T1",
            discrepancy_type="missing_from_statement",
            discrepancy_detail="not valid JSON {{{",
        )
        result = self.svc.list_trade_issues("u1", status="open")
        # Field is populated (not nulled), fallback to raw string.
        self.assertEqual(result[0]["discrepancy_detail"], "not valid JSON {{{")


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestResolveTradeIssue(unittest.TestCase):
    """Service layer: resolve_trade_issue. Covers pin-downs 9-13."""

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        ReconciliationIssue.__table__.create(self.engine)
        self._SessionFactory = scoped_session(
            sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine,
            )
        )
        self.svc = _make_svc(self._SessionFactory)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _insert_open(self, **kwargs):
        from datetime import datetime
        kwargs.setdefault(
            "detected_at", datetime(2026, 11, 22, 12, 0, 0),
        )
        kwargs.setdefault("user_id", "u1")
        kwargs.setdefault("kite_trade_id", "T1")
        kwargs.setdefault("discrepancy_type", "missing_from_statement")
        kwargs.setdefault("resolved_at", None)
        row = ReconciliationIssue(**kwargs)
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (9) fresh resolve writes timestamps + idempotent=False
    def test_resolve_fresh_sets_timestamps_and_note(self):
        row = self._insert_open()
        result, status = self.svc.resolve_trade_issue(
            "u1", row.id, resolution_note="manually reconciled",
        )
        self.assertEqual(status, 200)
        self.assertFalse(result["idempotent"])
        self.assertIsNotNone(result["resolved_at"])
        self.assertEqual(result["resolution_note"], "manually reconciled")
        # Verify persisted
        session = self.svc.db.session
        fresh = session.query(ReconciliationIssue).filter_by(
            id=row.id,
        ).first()
        self.assertIsNotNone(fresh.resolved_at)
        self.assertEqual(fresh.resolution_note, "manually reconciled")

    # (10) double-resolve is idempotent — ORIGINAL resolved_at preserved
    def test_resolve_twice_is_idempotent_preserves_original_timestamp(self):
        row = self._insert_open()
        result1, _ = self.svc.resolve_trade_issue(
            "u1", row.id, resolution_note="first",
        )
        self.assertFalse(result1["idempotent"])
        first_resolved_at = result1["resolved_at"]

        # Second resolve — must NOT overwrite resolved_at or note.
        result2, status2 = self.svc.resolve_trade_issue(
            "u1", row.id, resolution_note="second attempt",
        )
        self.assertEqual(status2, 200)
        self.assertTrue(result2["idempotent"])
        self.assertEqual(
            result2["resolved_at"], first_resolved_at,
            "double-resolve overwrote the original resolved_at — audit"
            " trail broken",
        )
        self.assertEqual(
            result2["resolution_note"], "first",
            "double-resolve overwrote the original resolution_note",
        )

    # (11) nonexistent id → 404
    def test_resolve_nonexistent_returns_404(self):
        result, status = self.svc.resolve_trade_issue(
            "u1", issue_id=99999, resolution_note=None,
        )
        self.assertEqual(status, 404)
        self.assertIn("error", result)

    # (12) wrong-user id → 404 (not 403; MUST NOT leak existence)
    def test_resolve_wrong_user_returns_404_not_403(self):
        row = self._insert_open(user_id="owner_user")
        # Different user tries to resolve
        result, status = self.svc.resolve_trade_issue(
            "attacker_user", row.id, resolution_note="pwned",
        )
        self.assertEqual(status, 404, (
            "wrong-user resolve must return 404 (same as not-found); "
            "a 403 would leak that the issue id exists"
        ))
        # And the original row is UNCHANGED (not resolved).
        session = self.svc.db.session
        fresh = session.query(ReconciliationIssue).filter_by(
            id=row.id,
        ).first()
        self.assertIsNone(
            fresh.resolved_at,
            "wrong-user resolve mutated the row — cross-user write",
        )
        self.assertIsNone(fresh.resolution_note)

    # (13) resolution_note > 500 chars is truncated
    def test_resolution_note_truncated_at_500_chars(self):
        row = self._insert_open()
        long_note = "x" * 1200
        result, status = self.svc.resolve_trade_issue(
            "u1", row.id, resolution_note=long_note,
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(result["resolution_note"]), 500)
        self.assertEqual(result["resolution_note"], "x" * 500)

    # Supplemental: empty / None resolution_note leaves it NULL
    def test_resolve_without_note_leaves_field_null(self):
        row = self._insert_open()
        result, status = self.svc.resolve_trade_issue(
            "u1", row.id, resolution_note=None,
        )
        self.assertEqual(status, 200)
        self.assertIsNone(result["resolution_note"])

    def test_resolve_with_empty_string_note_leaves_field_null(self):
        row = self._insert_open()
        result, status = self.svc.resolve_trade_issue(
            "u1", row.id, resolution_note="",
        )
        self.assertEqual(status, 200)
        # Empty string is falsy → service treats as no-note.
        self.assertIsNone(result["resolution_note"])


# ── (14) Controller + route wire-up: source-grep tests ───────────────
#
# Importing the real ReconciliationController pulls the Flask +
# Firebase chain (via Base_Service → GmailServiceUtils →
# GoogleServiceSingleton), which is not available in CI. We grep the
# source for the two route handlers + the route-table wire-up —
# matching the test_bundle2 precedent.

import re


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_reconciliation_controller_defines_listIssues_handler():
    src = _read_source("controllers/reconciliationEP.py")
    # Must be a method on ReconciliationController with the exact
    # name wire.py routes to.
    assert re.search(
        r"def\s+listIssues\s*\(\s*self\s*\)\s*:", src,
    ), "ReconciliationController.listIssues handler missing"


def test_reconciliation_controller_defines_resolveIssue_handler():
    src = _read_source("controllers/reconciliationEP.py")
    # Must accept issue_id from the route path.
    assert re.search(
        r"def\s+resolveIssue\s*\(\s*self\s*,\s*issue_id\s*\)\s*:", src,
    ), "ReconciliationController.resolveIssue handler missing"


def test_reconciliation_controller_rejects_bad_status():
    """400 on status not in {open, resolved, all} — unlike the
    service layer's defense-in-depth fallback, the HTTP boundary
    rejects bad input loudly so bugs surface at the FE."""
    src = _read_source("controllers/reconciliationEP.py")
    assert "_STATUS_ALLOWED" in src
    # The 400 branch must fire BEFORE the service call. Match across
    # whitespace/newlines with DOTALL so wrapped control flow still
    # counts, but anchor on the exact guard shape.
    guard_match = re.search(
        r"if\s+status\s+not\s+in\s+_STATUS_ALLOWED\s*:", src,
    )
    assert guard_match, (
        "controller must guard with `if status not in _STATUS_ALLOWED:`"
    )
    # Immediately after the guard there must be a 400 return (allow
    # wrapped jsonify args across multiple lines).
    tail = src[guard_match.end():]
    assert re.search(
        r"return\s+jsonify\(.*?\),\s*400", tail, re.DOTALL,
    ), "controller must return 400 inside the bad-status guard"


def test_reconciliation_controller_rejects_non_integer_issue_id():
    src = _read_source("controllers/reconciliationEP.py")
    assert re.search(
        r"int\s*\(\s*issue_id\s*\)", src,
    ), "resolveIssue must coerce issue_id to int (400 on bad input)"


def test_reconciliation_routes_wired_in_app():
    src = _read_source("app.py")
    # The two routes must be in the route table.
    assert re.search(
        r"'/reconciliation/issues'\s*,\s*'GET'\s*,\s*"
        r"self\.reconciliationEP\.listIssues",
        src,
    ), "GET /reconciliation/issues route not wired in app.py"
    assert re.search(
        r"'/reconciliation/issues/<issue_id>/resolve'\s*,\s*'POST'\s*,\s*"
        r"self\.reconciliationEP\.resolveIssue",
        src,
    ), "POST /reconciliation/issues/<id>/resolve route not wired in app.py"


def test_reconciliation_controller_instantiated_in_app():
    """ReconciliationController must be instantiated at __init__ time
    and reuse the existing ReconciliationService instance (not spawn
    a duplicate)."""
    src = _read_source("app.py")
    assert re.search(
        r"self\.reconciliationEP\s*=\s*ReconciliationController\s*\(\s*"
        r"self\.reconciliationService",
        src,
    ), (
        "ReconciliationController must be wired with the existing "
        "self.reconciliationService (duplicate service breaks "
        "singleton guarantees and makes per-request state drift)"
    )


# ── (15) Model shape: resolution_note column ─────────────────────────


@unittest.skipUnless(_MODEL_DEPS_OK, _MODEL_SKIP_REASON)
class TestReconciliationIssueModelShape(unittest.TestCase):

    def test_resolution_note_column_exists(self):
        self.assertIn(
            "resolution_note",
            ReconciliationIssue.__table__.columns,
            "ak-cipj added resolution_note — missing from model",
        )

    def test_resolution_note_is_nullable(self):
        col = ReconciliationIssue.__table__.columns["resolution_note"]
        self.assertTrue(
            col.nullable,
            "resolution_note must be nullable — not every resolve "
            "carries a note",
        )

    def test_resolution_note_has_length_cap(self):
        col = ReconciliationIssue.__table__.columns["resolution_note"]
        # Column type must carry a length — storage-boundary contract
        # for the service-layer truncation.
        self.assertTrue(
            hasattr(col.type, "length") and col.type.length is not None,
            "resolution_note column must be bounded String",
        )
        self.assertLessEqual(
            col.type.length, 1000,
            "resolution_note cap must stay small enough to match "
            "convenience-field intent (not a free-text essay field)",
        )


if __name__ == "__main__":
    unittest.main()
