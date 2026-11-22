"""ak-05af: tests for the Watchlist CRUD + LTP-batch surface.

Scope:
  * **Service layer** (WatchlistService.list_items / add_item /
    remove_item / update_note / fetch_ltp) against a real in-
    memory SQLite — same discipline as test_reconciliation_issues
    _endpoint (AST-lift to avoid the Google / Firebase boot
    chain).
  * **Model shape** — watchlist_items columns + uniqueness +
    indexes.
  * **Controller / route wire-up drift guards** — source-grep
    the route table + the controller handler signatures.
  * **Mutation drift guard** (ak-05af-specific): since this is
    the FIRST Kite-roadmap bead with INTENTIONAL mutations, the
    standard READ-ONLY drift guard pattern is adapted to assert
    the EXPECTED route verbs (GET/POST/DELETE/PUT) rather than
    reject-all. A newly-added mutation route on another Kite
    surface that WASN'T there in this bead's commit will still
    fail its own READ-ONLY drift guard on the OTHER bead's
    tests.

What these tests pin down:
  (1)  list_items returns rows user_id-scoped.
  (2)  list_items ordered by added_at DESC.
  (3)  add_item on fresh symbol returns 201 + idempotent=False.
  (4)  add_item on duplicate (user_id, instrument_token) returns
       200 + idempotent=True + existing row (does NOT overwrite
       note).
  (5)  add_item rejects non-integer instrument_token (400).
  (6)  add_item rejects empty tradingsymbol / exchange (400).
  (7)  add_item truncates note at 500 chars silently.
  (8)  add_item is user_id-scoped on uniqueness — same
       instrument_token under different users each insert fresh.
  (9)  remove_item deletes the row + returns 200.
  (10) remove_item on wrong-user id returns 404 (NOT 403; MUST
       NOT leak existence).
  (11) remove_item on nonexistent id returns 404.
  (12) update_note on wrong-user id returns 404.
  (13) update_note truncates note at 500 chars silently.
  (14) update_note with None / empty string CLEARS the note
       (sets NULL).
  (15) fetch_ltp on empty watchlist returns {} WITHOUT hitting
       Kite.
  (16) fetch_ltp batches EXCHANGE:SYMBOL keys + merges Kite's
       response.
  (17) fetch_ltp chunks at 500 instruments per batch.
  (18) fetch_ltp bubbles KiteAuthError (not caught at service).
  (19) Model has (user_id, instrument_token) uniqueness.
  (20) Controller route wire-up: GET / POST / DELETE / PUT /
       GET /ltp all present with the expected verbs + handler
       names (ak-05af-specific mutation drift guard).
  (21) Controller instantiated at app init with the shared
       KiteService (no duplicate).
  (22) Model + service + controller module imports registered
       in models/__init__.py.
  (23) ``_NOTE_MAX_LEN`` constant present at expected value.
"""

import ast
import os
import re
import sys
import types
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── Model-only import (cheap; no Google chain) ────────────────────
try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    # models/__init__.py registers the declarative targets.
    import models  # noqa: F401
    from models.investmentHistory import (  # noqa: F401
        InvestmentHistory,
    )
    from models.watchlistItem import WatchlistItem
    _MODEL_DEPS_OK = True
    _MODEL_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover — env-dep skip
    _MODEL_DEPS_OK = False
    _MODEL_SKIP_REASON = f"WatchlistItem model import failed: {_exc}"


# ── AST-lift service methods ──────────────────────────────────────
#
# WatchlistService inherits BaseService, which pulls the Gmail /
# Drive / Google-auth chain. AST-lifting the methods lets us test
# behaviour against a real SQLite without any of that boot. Same
# pattern as test_reconciliation_issues_endpoint._ast_lift_trade
# _issue_methods.


def _ast_lift_watchlist_service():
    """Return a dict of the service methods + module constants.

    Returns a dict with keys:
      list_items, add_item, remove_item, update_note, fetch_ltp,
      _serialize_item, _NOTE_MAX_LEN, _LTP_BATCH_LIMIT
    """
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "watchlistService.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)

    want_constants = {"_NOTE_MAX_LEN", "_LTP_BATCH_LIMIT"}
    want_methods = {
        "list_items", "add_item", "remove_item", "update_note",
        "fetch_ltp", "_serialize_item",
    }
    kept = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            tgt = node.targets[0]
            if isinstance(tgt, ast.Name) and tgt.id in want_constants:
                kept.append(node)
                continue
        if isinstance(node, ast.ClassDef) and node.name == "WatchlistService":
            for child in node.body:
                if isinstance(
                    child, (ast.FunctionDef, ast.AsyncFunctionDef),
                ) and child.name in want_methods:
                    child.decorator_list = []
                    kept.append(child)

    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)

    # Import IntegrityError to match the service module's import.
    from sqlalchemy.exc import IntegrityError as _IE

    ns = {
        "__name__": "test_watchlist_ast_lift",
        "WatchlistItem": WatchlistItem,
        "IntegrityError": _IE,
    }
    exec(compile(lift, "<watchlist-service-AST-lift>", "exec"), ns)
    return {
        "list_items": ns["list_items"],
        "add_item": ns["add_item"],
        "remove_item": ns["remove_item"],
        "update_note": ns["update_note"],
        "fetch_ltp": ns["fetch_ltp"],
        "_serialize_item": ns["_serialize_item"],
        "_NOTE_MAX_LEN": ns["_NOTE_MAX_LEN"],
        "_LTP_BATCH_LIMIT": ns["_LTP_BATCH_LIMIT"],
    }


if _MODEL_DEPS_OK:
    _LIFTED = _ast_lift_watchlist_service()
    _missing = [
        k for k in (
            "list_items", "add_item", "remove_item", "update_note",
            "fetch_ltp", "_serialize_item", "_NOTE_MAX_LEN",
            "_LTP_BATCH_LIMIT",
        ) if _LIFTED.get(k) is None
    ]
    if _missing:
        raise AssertionError(
            f"ak-05af service AST-lift missing symbols: {_missing}. "
            f"Did they get renamed or removed from "
            f"services/watchlistService.py?"
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


# ── Stubs ───────────────────────────────────────────────────────


class _NoopLogger:
    def error(self, *_a, **_k):
        pass

    def warning(self, *_a, **_k):
        pass

    def info(self, *_a, **_k):
        pass


class _StubWatchlistService:
    """Minimal surface the AST-lifted methods need: self.db.session
    + self.logger + self._kite_service.
    """

    def __init__(self, session_factory, kite_service=None):
        self._session_factory = session_factory
        self._cached_session = None
        self.logger = _NoopLogger()
        self._kite_service = kite_service

    @property
    def db(self):
        return self

    @property
    def session(self):
        if self._cached_session is None:
            self._cached_session = self._session_factory()
        return self._cached_session


class _StubKite:
    """Records calls to get_ltp + returns a canned response. Used
    to pin fetch_ltp batching / chunking behaviour without any
    network.
    """

    def __init__(self, response_by_batch=None, raise_exc=None):
        self.calls = []
        self._responses = response_by_batch or []
        self._raise_exc = raise_exc

    def get_ltp(self, user_id, instruments):
        self.calls.append((user_id, list(instruments)))
        if self._raise_exc is not None:
            raise self._raise_exc
        # Default canned response: echo {instrument: {last_price: 0}}.
        if not self._responses:
            return {
                k: {"instrument_token": 0, "last_price": 100.0}
                for k in instruments
            }
        return self._responses.pop(0)


def _make_svc(session_factory, kite_service=None):
    """Build a stub service with the AST-lifted methods bound."""
    svc = _StubWatchlistService(session_factory, kite_service=kite_service)
    svc.list_items = types.MethodType(_LIFTED["list_items"], svc)
    svc.add_item = types.MethodType(_LIFTED["add_item"], svc)
    svc.remove_item = types.MethodType(_LIFTED["remove_item"], svc)
    svc.update_note = types.MethodType(_LIFTED["update_note"], svc)
    svc.fetch_ltp = types.MethodType(_LIFTED["fetch_ltp"], svc)
    # Staticmethod lifted; attach to instance for self.-style call.
    svc._serialize_item = _LIFTED["_serialize_item"]
    return svc


# ─── Service-layer tests ────────────────────────────────────────


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestListItems(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        WatchlistItem.__table__.create(self.engine)
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

    def _insert(self, **kwargs):
        from datetime import datetime
        kwargs.setdefault("added_at", datetime(2026, 11, 22, 12, 0, 0))
        row = WatchlistItem(**kwargs)
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (1) user_id scoping
    def test_list_items_is_user_id_scoped(self):
        self._insert(
            user_id="u1", instrument_token=100,
            tradingsymbol="RELIANCE", exchange="NSE",
        )
        self._insert(
            user_id="u2", instrument_token=200,
            tradingsymbol="TCS", exchange="NSE",
        )
        result_u1 = self.svc.list_items("u1")
        self.assertEqual(len(result_u1), 1)
        self.assertEqual(result_u1[0]["tradingsymbol"], "RELIANCE")
        # u1 must NOT see u2's row
        tokens = [r["instrument_token"] for r in result_u1]
        self.assertNotIn(200, tokens)

    # (2) ordered by added_at DESC
    def test_list_items_ordered_by_added_at_desc(self):
        from datetime import datetime
        self._insert(
            user_id="u1", instrument_token=1,
            tradingsymbol="A", exchange="NSE",
            added_at=datetime(2026, 11, 20, 10, 0, 0),
        )
        self._insert(
            user_id="u1", instrument_token=2,
            tradingsymbol="B", exchange="NSE",
            added_at=datetime(2026, 11, 22, 10, 0, 0),
        )
        self._insert(
            user_id="u1", instrument_token=3,
            tradingsymbol="C", exchange="NSE",
            added_at=datetime(2026, 11, 21, 10, 0, 0),
        )
        result = self.svc.list_items("u1")
        syms = [r["tradingsymbol"] for r in result]
        self.assertEqual(syms, ["B", "C", "A"])


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestAddItem(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        WatchlistItem.__table__.create(self.engine)
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

    # (3) fresh add returns 201
    def test_add_fresh_returns_201_and_not_idempotent(self):
        result, status = self.svc.add_item(
            "u1", instrument_token=12345,
            tradingsymbol="RELIANCE", exchange="NSE",
            note="hold long",
        )
        self.assertEqual(status, 201)
        self.assertFalse(result["idempotent"])
        item = result["item"]
        self.assertEqual(item["tradingsymbol"], "RELIANCE")
        self.assertEqual(item["exchange"], "NSE")
        self.assertEqual(item["instrument_token"], 12345)
        self.assertEqual(item["note"], "hold long")
        self.assertIsNotNone(item["id"])

    # (4) duplicate add returns 200 + idempotent=True + existing row
    def test_add_duplicate_returns_200_idempotent(self):
        result1, _ = self.svc.add_item(
            "u1", instrument_token=777,
            tradingsymbol="TCS", exchange="NSE",
            note="original note",
        )
        first_id = result1["item"]["id"]
        # Second add with DIFFERENT note — note should NOT overwrite
        result2, status2 = self.svc.add_item(
            "u1", instrument_token=777,
            tradingsymbol="TCS", exchange="NSE",
            note="attempted overwrite",
        )
        self.assertEqual(status2, 200)
        self.assertTrue(result2["idempotent"])
        self.assertEqual(result2["item"]["id"], first_id)
        self.assertEqual(
            result2["item"]["note"], "original note",
            "duplicate-add overwrote the original note — must be "
            "idempotent, not update",
        )

    # (5) non-integer instrument_token
    def test_add_rejects_non_integer_token(self):
        result, status = self.svc.add_item(
            "u1", instrument_token="not-a-number",
            tradingsymbol="RELIANCE", exchange="NSE",
        )
        self.assertEqual(status, 400)
        self.assertIn("error", result)

    def test_add_rejects_non_positive_token(self):
        result, status = self.svc.add_item(
            "u1", instrument_token=0,
            tradingsymbol="RELIANCE", exchange="NSE",
        )
        self.assertEqual(status, 400)
        self.assertIn("error", result)

    # (5b) numeric-string token is ACCEPTED (JSON stringification)
    def test_add_accepts_numeric_string_token(self):
        result, status = self.svc.add_item(
            "u1", instrument_token="12345",
            tradingsymbol="RELIANCE", exchange="NSE",
        )
        self.assertEqual(status, 201)
        self.assertEqual(result["item"]["instrument_token"], 12345)

    # (6) empty tradingsymbol / exchange
    def test_add_rejects_empty_tradingsymbol(self):
        result, status = self.svc.add_item(
            "u1", instrument_token=100,
            tradingsymbol="", exchange="NSE",
        )
        self.assertEqual(status, 400)

    def test_add_rejects_whitespace_tradingsymbol(self):
        result, status = self.svc.add_item(
            "u1", instrument_token=100,
            tradingsymbol="   ", exchange="NSE",
        )
        self.assertEqual(status, 400)

    def test_add_rejects_empty_exchange(self):
        result, status = self.svc.add_item(
            "u1", instrument_token=100,
            tradingsymbol="RELIANCE", exchange="",
        )
        self.assertEqual(status, 400)

    def test_add_rejects_non_string_tradingsymbol(self):
        result, status = self.svc.add_item(
            "u1", instrument_token=100,
            tradingsymbol=12345, exchange="NSE",
        )
        self.assertEqual(status, 400)

    # (7) note truncated at 500 chars silently
    def test_add_truncates_long_note_silently(self):
        long_note = "x" * 1200
        result, status = self.svc.add_item(
            "u1", instrument_token=100,
            tradingsymbol="RELIANCE", exchange="NSE",
            note=long_note,
        )
        self.assertEqual(status, 201)
        self.assertEqual(len(result["item"]["note"]), 500)

    # (8) user_id scoping on uniqueness — same token, different users
    def test_add_user_scoped_uniqueness(self):
        self.svc.add_item(
            "u1", instrument_token=100,
            tradingsymbol="RELIANCE", exchange="NSE",
        )
        # SAME token, DIFFERENT user — must insert fresh (uniqueness
        # is (user_id, instrument_token) not just instrument_token)
        result, status = self.svc.add_item(
            "u2", instrument_token=100,
            tradingsymbol="RELIANCE", exchange="NSE",
        )
        self.assertEqual(status, 201)
        self.assertFalse(result["idempotent"])

    # Exchange is upper-cased on store
    def test_add_uppercases_exchange(self):
        result, _ = self.svc.add_item(
            "u1", instrument_token=100,
            tradingsymbol="RELIANCE", exchange="nse",
        )
        self.assertEqual(result["item"]["exchange"], "NSE")


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestRemoveItem(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        WatchlistItem.__table__.create(self.engine)
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

    def _insert(self, user_id="u1", instrument_token=100, **kwargs):
        kwargs.setdefault("tradingsymbol", "RELIANCE")
        kwargs.setdefault("exchange", "NSE")
        row = WatchlistItem(
            user_id=user_id, instrument_token=instrument_token,
            **kwargs,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (9) remove_item deletes + 200
    def test_remove_deletes_row(self):
        row = self._insert()
        result, status = self.svc.remove_item("u1", row.id)
        self.assertEqual(status, 200)
        self.assertTrue(result["removed"])
        # Row is gone from DB
        fresh = self.svc.db.session.query(WatchlistItem).filter_by(
            id=row.id,
        ).first()
        self.assertIsNone(fresh)

    # (10) wrong-user id returns 404 (NOT 403; MUST NOT leak existence)
    def test_remove_wrong_user_returns_404_not_403(self):
        row = self._insert(user_id="owner")
        result, status = self.svc.remove_item("attacker", row.id)
        self.assertEqual(status, 404, (
            "wrong-user remove must return 404 (same as not-found); "
            "a 403 would leak that the item id exists"
        ))
        # Original row preserved
        fresh = self.svc.db.session.query(WatchlistItem).filter_by(
            id=row.id,
        ).first()
        self.assertIsNotNone(
            fresh, "wrong-user remove leaked the delete",
        )

    # (11) nonexistent id returns 404
    def test_remove_nonexistent_returns_404(self):
        result, status = self.svc.remove_item("u1", 99999)
        self.assertEqual(status, 404)


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestUpdateNote(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        WatchlistItem.__table__.create(self.engine)
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

    def _insert_with_note(self, note="original"):
        row = WatchlistItem(
            user_id="u1", instrument_token=100,
            tradingsymbol="RELIANCE", exchange="NSE",
            note=note,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (12) wrong-user id returns 404
    def test_update_note_wrong_user_returns_404(self):
        row = WatchlistItem(
            user_id="owner", instrument_token=100,
            tradingsymbol="RELIANCE", exchange="NSE",
            note="owner's note",
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        result, status = self.svc.update_note(
            "attacker", row.id, "pwned",
        )
        self.assertEqual(status, 404)
        # Note on actual row is UNCHANGED
        fresh = self.svc.db.session.query(WatchlistItem).filter_by(
            id=row.id,
        ).first()
        self.assertEqual(fresh.note, "owner's note")

    # (13) long note truncated silently at 500
    def test_update_note_truncates_silently(self):
        row = self._insert_with_note()
        result, status = self.svc.update_note(
            "u1", row.id, "x" * 1200,
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(result["note"]), 500)

    # (14) None / empty string clears the note (NULL)
    def test_update_note_none_clears(self):
        row = self._insert_with_note(note="something")
        result, status = self.svc.update_note("u1", row.id, None)
        self.assertEqual(status, 200)
        self.assertIsNone(result["note"])

    def test_update_note_empty_string_clears(self):
        row = self._insert_with_note(note="something")
        result, status = self.svc.update_note("u1", row.id, "")
        self.assertEqual(status, 200)
        self.assertIsNone(result["note"])

    def test_update_note_whitespace_clears(self):
        row = self._insert_with_note(note="something")
        result, status = self.svc.update_note("u1", row.id, "   ")
        self.assertEqual(status, 200)
        self.assertIsNone(result["note"])

    # Positive: set to a non-empty value
    def test_update_note_sets_new_value(self):
        row = self._insert_with_note(note=None)
        result, status = self.svc.update_note(
            "u1", row.id, "hold through Q1",
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["note"], "hold through Q1")


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestFetchLtp(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        WatchlistItem.__table__.create(self.engine)
        self._SessionFactory = scoped_session(
            sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine,
            )
        )
        self.kite = _StubKite()
        self.svc = _make_svc(
            self._SessionFactory, kite_service=self.kite,
        )

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add(self, exchange, tradingsymbol, instrument_token):
        row = WatchlistItem(
            user_id="u1", instrument_token=instrument_token,
            tradingsymbol=tradingsymbol, exchange=exchange,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()

    # (15) empty watchlist → {} and NO Kite call
    def test_empty_watchlist_returns_empty_and_no_kite_call(self):
        result, status = self.svc.fetch_ltp("u1")
        self.assertEqual(status, 200)
        self.assertEqual(result, {"ltp": {}, "count": 0})
        self.assertEqual(
            self.kite.calls, [],
            "fetch_ltp hit Kite on empty watchlist — must short-circuit",
        )

    # (16) builds EXCHANGE:SYMBOL keys + merges response
    def test_fetch_ltp_builds_exchange_symbol_keys(self):
        self._add("NSE", "RELIANCE", 100)
        self._add("BSE", "TCS", 200)
        result, status = self.svc.fetch_ltp("u1")
        self.assertEqual(status, 200)
        self.assertEqual(result["count"], 2)
        # Check Kite was called with the expected key shape
        self.assertEqual(len(self.kite.calls), 1)
        user_id_sent, keys_sent = self.kite.calls[0]
        self.assertEqual(user_id_sent, "u1")
        self.assertIn("NSE:RELIANCE", keys_sent)
        self.assertIn("BSE:TCS", keys_sent)

    # (17) chunking at _LTP_BATCH_LIMIT (500)
    def test_fetch_ltp_chunks_at_batch_limit(self):
        # Insert 1200 items — should trigger 3 chunks (500/500/200)
        for i in range(1200):
            self._add("NSE", f"SYM{i}", i + 1)
        result, status = self.svc.fetch_ltp("u1")
        self.assertEqual(status, 200)
        self.assertEqual(result["count"], 1200)
        # Three calls expected: 500 + 500 + 200
        self.assertEqual(len(self.kite.calls), 3)
        self.assertEqual(len(self.kite.calls[0][1]), 500)
        self.assertEqual(len(self.kite.calls[1][1]), 500)
        self.assertEqual(len(self.kite.calls[2][1]), 200)

    # (18) KiteAuthError propagates (NOT swallowed by service)
    def test_fetch_ltp_propagates_kite_auth_error(self):
        # Create a stand-in error class matching KiteAuthError's shape
        # since the real one requires the kiteconnect import chain.
        class _FakeKiteAuthError(Exception):
            pass

        self._add("NSE", "RELIANCE", 100)
        bad_kite = _StubKite(raise_exc=_FakeKiteAuthError("token expired"))
        svc = _make_svc(self._SessionFactory, kite_service=bad_kite)
        with self.assertRaises(_FakeKiteAuthError):
            svc.fetch_ltp("u1")

    # fetch_ltp without a wired Kite service → 500, not NoneType crash
    def test_fetch_ltp_unwired_returns_500(self):
        svc = _make_svc(self._SessionFactory, kite_service=None)
        result, status = svc.fetch_ltp("u1")
        self.assertEqual(status, 500)

    # fetch_ltp is user_id-scoped — u1 doesn't see u2's instruments
    def test_fetch_ltp_user_scoped(self):
        self._add("NSE", "RELIANCE", 100)
        other = WatchlistItem(
            user_id="u2", instrument_token=200,
            tradingsymbol="TCS", exchange="NSE",
        )
        self.svc.db.session.add(other)
        self.svc.db.session.commit()
        self.svc.fetch_ltp("u1")
        # The one call should have only u1's instruments
        self.assertEqual(len(self.kite.calls), 1)
        _, keys = self.kite.calls[0]
        self.assertEqual(keys, ["NSE:RELIANCE"])


# ─── Model shape ────────────────────────────────────────────────


@unittest.skipUnless(_MODEL_DEPS_OK, _MODEL_SKIP_REASON)
class TestWatchlistItemModelShape(unittest.TestCase):

    def test_tablename(self):
        self.assertEqual(
            WatchlistItem.__tablename__, "watchlist_items",
            "model tablename drift — downstream migration bead would "
            "produce the wrong DDL",
        )

    def test_required_columns_present(self):
        cols = WatchlistItem.__table__.columns
        for name in (
            "id", "user_id", "instrument_token",
            "tradingsymbol", "exchange", "added_at", "note",
        ):
            self.assertIn(
                name, cols, f"required column {name!r} missing",
            )

    # (19) uniqueness constraint on (user_id, instrument_token)
    def test_unique_constraint_on_user_and_token(self):
        from sqlalchemy import UniqueConstraint
        constraints = [
            c for c in WatchlistItem.__table__.constraints
            if isinstance(c, UniqueConstraint)
        ]
        matched = False
        for uc in constraints:
            col_names = {c.name for c in uc.columns}
            if col_names == {"user_id", "instrument_token"}:
                matched = True
                break
        self.assertTrue(
            matched,
            "model must carry UniqueConstraint on (user_id, "
            "instrument_token) — add-idempotency depends on it",
        )

    def test_user_added_index_present(self):
        from sqlalchemy import Index
        indexes = [i for i in WatchlistItem.__table__.indexes
                   if isinstance(i, Index)]
        names = {i.name for i in indexes}
        self.assertIn(
            "ix_watchlist_user_added", names,
            "model must carry ix_watchlist_user_added — primary "
            "list query (user_id + added_at DESC) depends on it",
        )

    def test_note_nullable_with_length_cap(self):
        col = WatchlistItem.__table__.columns["note"]
        self.assertTrue(col.nullable)
        self.assertEqual(
            col.type.length, 500,
            "note column must be String(500) — matches service "
            "truncation constant",
        )

    def test_required_columns_not_nullable(self):
        cols = WatchlistItem.__table__.columns
        for name in ("user_id", "instrument_token", "tradingsymbol", "exchange"):
            self.assertFalse(
                cols[name].nullable,
                f"{name} must be NOT NULL — missing field would "
                f"break list rendering",
            )


# ─── Source-grep: controller / route wire-up drift guards ──────


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_watchlist_controller_defines_list_handler():
    src = _read_source("controllers/watchlistEP.py")
    assert re.search(
        r"def\s+listItems\s*\(\s*self\s*\)\s*:", src,
    ), "WatchlistController.listItems handler missing"


def test_watchlist_controller_defines_add_handler():
    src = _read_source("controllers/watchlistEP.py")
    assert re.search(
        r"def\s+addItem\s*\(\s*self\s*\)\s*:", src,
    ), "WatchlistController.addItem handler missing"


def test_watchlist_controller_defines_remove_handler():
    src = _read_source("controllers/watchlistEP.py")
    assert re.search(
        r"def\s+removeItem\s*\(\s*self\s*,\s*item_id\s*\)\s*:", src,
    ), "WatchlistController.removeItem handler missing"


def test_watchlist_controller_defines_update_note_handler():
    src = _read_source("controllers/watchlistEP.py")
    assert re.search(
        r"def\s+updateNote\s*\(\s*self\s*,\s*item_id\s*\)\s*:", src,
    ), "WatchlistController.updateNote handler missing"


def test_watchlist_controller_defines_fetch_ltp_handler():
    src = _read_source("controllers/watchlistEP.py")
    assert re.search(
        r"def\s+fetchLtp\s*\(\s*self\s*\)\s*:", src,
    ), "WatchlistController.fetchLtp handler missing"


def test_watchlist_controller_uses_g_firebase_id():
    """ak-c8l hygiene — every handler must source user_id from
    g.firebase_id (NOT raw X-Firebase-ID header). Enforces the
    IDOR-fix sweep pattern across every new controller."""
    src = _read_source("controllers/watchlistEP.py")
    # No direct raw-header reads allowed.
    assert "X-Firebase-ID" not in src, (
        "controller reads X-Firebase-ID header directly — must go "
        "through g.firebase_id per ak-c8l"
    )
    assert "request.headers.get" not in src, (
        "controller reads request.headers directly — must go "
        "through g.firebase_id per ak-c8l"
    )
    # Every handler sources from g
    key = 'g.get("firebase_id")'
    hits = src.count(key)
    assert hits >= 5, (
        f"expected >=5 g.get('firebase_id') reads (one per handler) "
        f"— found only {hits}"
    )


def test_watchlist_controller_handles_kite_auth_error_on_ltp_only():
    """fetchLtp is the ONLY handler that catches KiteAuthError;
    CRUD handlers are pure-DB and don't hit Kite."""
    src = _read_source("controllers/watchlistEP.py")
    # KiteAuthError imported + used
    assert "from services.KiteService import KiteAuthError" in src, (
        "fetchLtp must catch KiteAuthError — missing import"
    )
    assert "except KiteAuthError" in src, (
        "fetchLtp must catch KiteAuthError"
    )
    assert "_kiteReconnectResponse" in src, (
        "fetchLtp must map KiteAuthError to the shared reconnect "
        "payload shape"
    )


# ─── Route wire-up: ak-05af mutation drift guard ──────────────


# This is the ak-05af-specific adaptation of the READ-ONLY drift
# guard pattern. Because this bead INTENTIONALLY adds mutations,
# we assert the EXPECTED route verbs are present with the exact
# handler names — any new route added to the watchlist surface
# that drifts from the bead scope (e.g. a surprise bulk-delete)
# fails this guard.
EXPECTED_WATCHLIST_ROUTES = [
    (r"'/watchlist'", r"'GET'", "listItems"),
    (r"'/watchlist'", r"'POST'", "addItem"),
    (r"'/watchlist/<item_id>'", r"'DELETE'", "removeItem"),
    (r"'/watchlist/<item_id>/note'", r"'PUT'", "updateNote"),
    (r"'/watchlist/ltp'", r"'GET'", "fetchLtp"),
]


def test_watchlist_routes_wired_in_app():
    src = _read_source("app.py")
    for rule_re, method_re, handler_name in EXPECTED_WATCHLIST_ROUTES:
        pattern = (
            rf"{rule_re}\s*,\s*{method_re}\s*,\s*"
            rf"self\.watchlistEP\.{handler_name}"
        )
        assert re.search(pattern, src), (
            f"route {rule_re} {method_re} -> {handler_name} not "
            f"wired in app.py (ak-05af mutation drift guard)"
        )


# ak-05af mutation drift guard: enumerate the EXACT mutations
# expected on the /watchlist surface. If a future bead adds a
# surprise route (bulk-delete / batch-add / etc.), this guard
# fails and forces a §23 #5 destructive-gate review before
# shipping.
EXPECTED_WATCHLIST_MUTATIONS = {
    ("'/watchlist'", "'POST'"),
    ("'/watchlist/<item_id>'", "'DELETE'"),
    ("'/watchlist/<item_id>/note'", "'PUT'"),
}


def test_no_unexpected_watchlist_mutations():
    """Drift guard: no MUTATION verb against /watchlist* beyond
    the three in ``EXPECTED_WATCHLIST_MUTATIONS``. Adapted from
    the READ-ONLY reject-all pattern used on earlier Kite surfaces
    (ak-h1mq / ak-zi54 / ak-ue7l) — those beads could reject any
    mutation; this bead legitimately adds three, so we assert the
    EXACT set instead.
    """
    src = _read_source("app.py")
    # Pull out ALL route-tuple entries touching /watchlist* in app.py
    all_watchlist_entries = re.findall(
        r"\(\s*('/watchlist[^']*')\s*,\s*('[A-Z]+')\s*,", src,
    )
    found_mutations = {
        (rule, verb)
        for rule, verb in all_watchlist_entries
        if verb.strip("'") in {"POST", "DELETE", "PUT", "PATCH"}
    }
    extras = found_mutations - EXPECTED_WATCHLIST_MUTATIONS
    assert not extras, (
        f"UNEXPECTED mutation routes on /watchlist*: {extras}. "
        f"If this is intentional, update "
        f"EXPECTED_WATCHLIST_MUTATIONS in this test AND confirm "
        f"§23 #5 destructive-gate review at the bead level."
    )
    missing = EXPECTED_WATCHLIST_MUTATIONS - found_mutations
    assert not missing, (
        f"EXPECTED mutation routes missing from app.py: {missing}"
    )


def test_watchlist_controller_instantiated_in_app():
    """(21) WatchlistController wired with the SHARED KiteService
    (via InvestmentService.StockService.kite_service) — no
    duplicate KiteService instance."""
    src = _read_source("app.py")
    assert re.search(
        r"self\.watchlistService\s*=\s*WatchlistService\s*\(\s*"
        r"kite_service=self\.investmentService\.StockService\.kite_service",
        src,
    ), (
        "WatchlistService must be wired with the existing "
        "self.investmentService.StockService.kite_service "
        "(duplicate KiteService breaks singleton guarantees)"
    )
    assert re.search(
        r"self\.watchlistEP\s*=\s*WatchlistController\s*\(\s*"
        r"self\.watchlistService",
        src,
    ), "WatchlistController wire-up missing in app.py"


def test_watchlist_model_registered_in_models_init():
    """(22) models/__init__.py imports WatchlistItem so
    Base.metadata.create_all materializes the fresh table on
    boot (SAFE #9 case — no infra ALTER needed on fresh install).
    """
    src = _read_source("models/__init__.py")
    assert "from models.watchlistItem import WatchlistItem" in src, (
        "WatchlistItem not registered in models/__init__.py — "
        "fresh installs will miss the table on create_all"
    )


def test_note_max_len_constant_present():
    """(23) ``_NOTE_MAX_LEN`` constant is 500 — matches the model
    column length cap AND the docstring/service-boundary
    truncation semantic."""
    assert _LIFTED is not None, (
        "_LIFTED is None — AST-lift failed; cannot verify "
        "service constants"
    )
    assert _LIFTED["_NOTE_MAX_LEN"] == 500, (
        f"_NOTE_MAX_LEN drift: expected 500, got "
        f"{_LIFTED['_NOTE_MAX_LEN']}"
    )
    assert _LIFTED["_LTP_BATCH_LIMIT"] == 500, (
        f"_LTP_BATCH_LIMIT drift: expected 500 (Kite docs), got "
        f"{_LIFTED['_LTP_BATCH_LIMIT']}"
    )


if __name__ == "__main__":
    unittest.main()
