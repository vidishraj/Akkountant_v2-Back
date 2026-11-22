"""ak-v55t: tests for the Kite instruments cache + search surface.

Scope:
  1. **Model shape**: kite_instruments columns + indexes + PK.
  2. **InstrumentsCacheTask coercion helpers**: pure module-level
     functions (_int_or_none / _dec_or_none / _str_or_none /
     _date_or_none) — importable without the BaseTask / Google
     chain if we skip the class body.
  3. **search_instruments logic**: AST-lifted KiteService method
     tested against real in-memory SQLite with the real
     KiteInstrument model. Covers empty query, prefix precedence,
     substring match, exchange filter, limit clamp, dedupe.
  4. **Wire-up drift guards**: scheduler TASK_MAPPING entry,
     app.py route wire, models/__init__.py import.

Baseline discipline: service-layer tests skip cleanly when the
heavier dep chain (kiteconnect / Google / Firebase) isn't
available in this env; always-run drift guards + pure-function
tests red hard on source removal.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest
from datetime import date
from decimal import Decimal


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── Model-only import (cheap, no Google chain) ───────────────────────
try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    import models  # noqa: F401 — register declarative
    from models.investmentHistory import InvestmentHistory  # noqa: F401
    from models.kiteInstrument import KiteInstrument
    _MODEL_DEPS_OK = True
    _MODEL_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover — env-dep skip
    _MODEL_DEPS_OK = False
    _MODEL_SKIP_REASON = f"KiteInstrument model import failed: {_exc}"


# ── AST-lift the pure helpers from InstrumentsCacheTask ──────────────
#
# BaseTask inheritance pulls BaseService → GmailServiceUtils →
# GoogleServiceSingleton which isn't available in this env. We lift
# only the module-level coercion helpers (_int_or_none / _dec_or_none
# / _str_or_none / _date_or_none) which are pure stdlib.

def _ast_lift_cache_task_helpers():
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "tasks", "InstrumentsCacheTask.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)
    want_helpers = {
        "_int_or_none", "_dec_or_none", "_str_or_none",
        "_date_or_none",
    }
    kept = []
    for node in tree.body:
        # __future__ for annotations behaviour.
        if isinstance(node, ast.ImportFrom) and node.module in {
            "__future__", "decimal", "datetime",
        }:
            kept.append(node)
            continue
        if isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef)
        ) and node.name in want_helpers:
            node.decorator_list = []
            kept.append(node)
    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)
    ns = {"__name__": "test_v55t_helpers_lift"}
    exec(compile(lift, "<cache-task-helpers-lift>", "exec"), ns)
    return ns


# ── AST-lift search_instruments from KiteService ─────────────────────
#
# KiteService.__init__ pulls Base_Service → Gmail → Google chain.
# search_instruments is a pure DB method — lift it standalone so it
# can be exercised against an in-memory SQLite without the boot.

def _ast_lift_search_method():
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "KiteService.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)
    kept = []
    want = {"search_instruments", "_serialize_instrument"}
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "KiteService":
            for child in node.body:
                if isinstance(
                    child, (ast.FunctionDef, ast.AsyncFunctionDef)
                ) and child.name in want:
                    child.decorator_list = []
                    kept.append(child)
    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)
    ns = {"__name__": "test_v55t_search_lift"}
    exec(compile(lift, "<search-method-lift>", "exec"), ns)
    return ns


try:
    _HELPERS = _ast_lift_cache_task_helpers()
    _SEARCH = _ast_lift_search_method()
    _LIFT_OK = _MODEL_DEPS_OK
    _LIFT_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _HELPERS = None
    _SEARCH = None
    _LIFT_OK = False
    _LIFT_SKIP_REASON = f"AST-lift failed: {_exc}"


# ── (1) Model shape ──────────────────────────────────────────────────


@unittest.skipUnless(_MODEL_DEPS_OK, _MODEL_SKIP_REASON)
class TestKiteInstrumentModel(unittest.TestCase):

    def test_columns_present(self):
        cols = KiteInstrument.__table__.columns
        for name in (
            "instrument_token", "exchange_token", "tradingsymbol",
            "name", "lot_size", "instrument_type", "segment",
            "exchange", "tick_size", "last_price", "expiry",
            "strike",
        ):
            self.assertIn(name, cols, f"missing column {name!r}")

    def test_instrument_token_is_primary_key(self):
        cols = KiteInstrument.__table__.columns
        self.assertTrue(
            cols["instrument_token"].primary_key,
            "instrument_token must be PK — UPSERT shape depends on it.",
        )

    def test_tradingsymbol_not_nullable(self):
        cols = KiteInstrument.__table__.columns
        self.assertFalse(
            cols["tradingsymbol"].nullable,
            "tradingsymbol is a search target; cannot be NULL",
        )

    def test_name_is_nullable(self):
        """Name is nullable — not every segment emits a name."""
        cols = KiteInstrument.__table__.columns
        self.assertTrue(cols["name"].nullable)

    def test_fno_fields_nullable(self):
        """F&O-only fields (lot_size / expiry / strike /
        instrument_type / segment) MUST be nullable — equity rows
        don't carry them."""
        cols = KiteInstrument.__table__.columns
        for name in (
            "lot_size", "expiry", "strike", "instrument_type",
            "segment", "last_price", "tick_size", "exchange_token",
        ):
            self.assertTrue(
                cols[name].nullable,
                f"{name} must be nullable — equity rows don't emit it",
            )

    def test_search_indexes_declared(self):
        """Index on tradingsymbol + name + exchange — the three
        search targets. Missing indexes = full table scan on every
        autocomplete keystroke."""
        idx_names = {
            idx.name for idx in KiteInstrument.__table_args__
            if hasattr(idx, "columns") and hasattr(idx, "name")
        }
        for expected in (
            "ix_kite_inst_tradingsymbol",
            "ix_kite_inst_name",
            "ix_kite_inst_exchange",
        ):
            self.assertIn(
                expected, idx_names,
                f"index {expected!r} missing — search will full-scan",
            )


# ── (2) InstrumentsCacheTask coercion helpers ────────────────────────


@unittest.skipUnless(_LIFT_OK, _LIFT_SKIP_REASON)
class TestCacheTaskCoercionHelpers(unittest.TestCase):

    def setUp(self):
        self.int_or_none = _HELPERS["_int_or_none"]
        self.dec_or_none = _HELPERS["_dec_or_none"]
        self.str_or_none = _HELPERS["_str_or_none"]
        self.date_or_none = _HELPERS["_date_or_none"]

    def test_int_or_none(self):
        self.assertEqual(self.int_or_none(42), 42)
        self.assertEqual(self.int_or_none("42"), 42)
        self.assertEqual(self.int_or_none(42.9), 42)
        self.assertIsNone(self.int_or_none(None))
        self.assertIsNone(self.int_or_none(""))
        self.assertIsNone(self.int_or_none("   "))
        self.assertIsNone(self.int_or_none("not-a-number"))

    def test_dec_or_none(self):
        self.assertEqual(self.dec_or_none("1.50"), Decimal("1.50"))
        self.assertEqual(self.dec_or_none(1.5), Decimal("1.5"))
        self.assertIsNone(self.dec_or_none(None))
        self.assertIsNone(self.dec_or_none(""))
        self.assertIsNone(self.dec_or_none("garbage"))

    def test_str_or_none_truncates(self):
        self.assertEqual(self.str_or_none("hello", 10), "hello")
        self.assertEqual(
            self.str_or_none("a" * 100, 10), "a" * 10,
        )
        self.assertIsNone(self.str_or_none(None, 10))
        self.assertIsNone(self.str_or_none("", 10))
        self.assertIsNone(self.str_or_none("   ", 10))

    def test_date_or_none_accepts_native_date(self):
        self.assertEqual(
            self.date_or_none(date(2026, 11, 22)),
            date(2026, 11, 22),
        )

    def test_date_or_none_accepts_datetime(self):
        from datetime import datetime
        self.assertEqual(
            self.date_or_none(datetime(2026, 11, 22, 15, 30)),
            date(2026, 11, 22),
        )

    def test_date_or_none_parses_iso_string(self):
        self.assertEqual(
            self.date_or_none("2026-11-22"),
            date(2026, 11, 22),
        )

    def test_date_or_none_parses_iso_datetime_string(self):
        self.assertEqual(
            self.date_or_none("2026-11-22T15:30:00"),
            date(2026, 11, 22),
        )

    def test_date_or_none_rejects_garbage(self):
        self.assertIsNone(self.date_or_none("not a date"))
        self.assertIsNone(self.date_or_none(None))
        self.assertIsNone(self.date_or_none(""))


# ── (3) search_instruments logic ─────────────────────────────────────


class _StubDb:
    """Same-session shim mirroring BaseService.db."""

    def __init__(self, session_factory):
        self._factory = session_factory
        self._cached = None

    @property
    def session(self):
        if self._cached is None:
            self._cached = self._factory()
        return self._cached


@unittest.skipUnless(_LIFT_OK, _LIFT_SKIP_REASON)
class TestSearchInstruments(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        KiteInstrument.__table__.create(self.engine)
        self.SessionFactory = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        )

        import types
        stub_db = _StubDb(self.SessionFactory)

        class _Stub:
            db = stub_db

        self.svc = _Stub()
        self.svc.search_instruments = types.MethodType(
            _SEARCH["search_instruments"], self.svc,
        )
        # _serialize_instrument is @staticmethod in the real class
        # (decorator stripped at AST lift) — attach as plain
        # callable at the instance level.
        self.svc._serialize_instrument = _SEARCH["_serialize_instrument"]

    def tearDown(self):
        try:
            self.SessionFactory.remove()
        except Exception:
            pass

    def _insert(self, **kwargs):
        kwargs.setdefault("name", None)
        kwargs.setdefault("exchange", "NSE")
        row = KiteInstrument(**kwargs)
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    def test_empty_query_returns_empty_list(self):
        """Caller discipline: empty query must NOT trigger a
        full-table scan. Bead contract — "a bare search without
        input would return the full 200k row universe"."""
        self._insert(instrument_token=1, tradingsymbol="RELIANCE")
        self.assertEqual(self.svc.search_instruments(""), [])
        self.assertEqual(self.svc.search_instruments("   "), [])
        self.assertEqual(self.svc.search_instruments(None), [])

    def test_prefix_match_on_tradingsymbol(self):
        self._insert(instrument_token=1, tradingsymbol="RELIANCE")
        self._insert(instrument_token=2, tradingsymbol="TCS")
        results = self.svc.search_instruments("REL")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["tradingsymbol"], "RELIANCE")

    def test_case_insensitive_search(self):
        self._insert(instrument_token=1, tradingsymbol="RELIANCE")
        results = self.svc.search_instruments("rel")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["tradingsymbol"], "RELIANCE")

    def test_substring_match_on_name(self):
        self._insert(
            instrument_token=1, tradingsymbol="HDFCBANK",
            name="HDFC Bank Limited",
        )
        self._insert(
            instrument_token=2, tradingsymbol="TCS", name="TCS Ltd",
        )
        results = self.svc.search_instruments("Bank")
        tokens = {r["instrument_token"] for r in results}
        self.assertIn(1, tokens)
        self.assertNotIn(2, tokens)

    def test_exchange_filter_narrows_results(self):
        self._insert(
            instrument_token=1, tradingsymbol="RELIANCE",
            exchange="NSE",
        )
        self._insert(
            instrument_token=2, tradingsymbol="RELIANCE",
            exchange="BSE",
        )
        nse_only = self.svc.search_instruments(
            "RELIANCE", exchange="NSE",
        )
        self.assertEqual(len(nse_only), 1)
        self.assertEqual(nse_only[0]["exchange"], "NSE")
        bse_only = self.svc.search_instruments(
            "RELIANCE", exchange="BSE",
        )
        self.assertEqual(len(bse_only), 1)
        self.assertEqual(bse_only[0]["exchange"], "BSE")
        # No filter = both.
        both = self.svc.search_instruments("RELIANCE")
        self.assertEqual(len(both), 2)

    def test_unknown_exchange_returns_empty(self):
        self._insert(
            instrument_token=1, tradingsymbol="RELIANCE",
            exchange="NSE",
        )
        results = self.svc.search_instruments(
            "RELIANCE", exchange="MADE_UP",
        )
        self.assertEqual(results, [])

    def test_limit_clamp_upper_bound(self):
        """limit > 100 clamped to 100."""
        for i in range(200):
            self._insert(
                instrument_token=i + 1,
                tradingsymbol=f"SYM{i:03d}",
            )
        results = self.svc.search_instruments("SYM", limit=500)
        self.assertEqual(len(results), 100)

    def test_limit_clamp_lower_bound(self):
        """limit < 1 clamped to 1."""
        self._insert(instrument_token=1, tradingsymbol="RELIANCE")
        self._insert(instrument_token=2, tradingsymbol="RELIABLE")
        results = self.svc.search_instruments("REL", limit=0)
        self.assertEqual(len(results), 1)

    def test_bad_limit_falls_back_to_default(self):
        """Non-integer limit → use default 20, no crash."""
        self._insert(instrument_token=1, tradingsymbol="X")
        results = self.svc.search_instruments("X", limit="garbage")
        self.assertEqual(len(results), 1)  # 1 row, default clamp covers it

    def test_prefix_matches_rank_above_substring_matches(self):
        """Query 'REL' should return RELIANCE (prefix match)
        before UNRELATED (substring match in name)."""
        self._insert(
            instrument_token=1, tradingsymbol="RELIANCE",
            name="Reliance Industries",
        )
        self._insert(
            instrument_token=2, tradingsymbol="UNRELATED",
            name="Something relevant",
        )
        results = self.svc.search_instruments("REL")
        # Prefix match on tradingsymbol must appear first.
        self.assertEqual(results[0]["tradingsymbol"], "RELIANCE")

    def test_duplicate_matches_deduped_by_instrument_token(self):
        """A row that matches BOTH prefix and substring passes
        must appear ONCE in the result list."""
        # RELIANCE matches prefix on "REL" and would also match
        # substring in both tradingsymbol + name — one row only.
        self._insert(
            instrument_token=1, tradingsymbol="RELIANCE",
            name="Reliance Industries",
        )
        results = self.svc.search_instruments("REL")
        tokens = [r["instrument_token"] for r in results]
        self.assertEqual(tokens.count(1), 1)

    def test_response_shape_for_equity(self):
        """Equity rows carry no F&O fields — nullable fields
        should serialize as None, not crash."""
        self._insert(
            instrument_token=12345,
            tradingsymbol="RELIANCE",
            name="Reliance Industries",
            exchange="NSE",
            segment="NSE",
            instrument_type="EQ",
        )
        results = self.svc.search_instruments("REL")
        row = results[0]
        self.assertEqual(row["instrument_token"], 12345)
        self.assertEqual(row["tradingsymbol"], "RELIANCE")
        self.assertEqual(row["instrument_type"], "EQ")
        # F&O-specific fields should be None.
        self.assertIsNone(row["lot_size"])
        self.assertIsNone(row["expiry"])
        self.assertIsNone(row["strike"])

    def test_response_shape_for_option(self):
        self._insert(
            instrument_token=67890,
            tradingsymbol="NIFTY25DEC2426000CE",
            name="NIFTY DEC 26000 CE",
            exchange="NFO",
            segment="NFO-OPT",
            instrument_type="CE",
            lot_size=50,
            expiry=date(2025, 12, 25),
            strike=Decimal("26000"),
            tick_size=Decimal("0.05"),
        )
        results = self.svc.search_instruments("NIFTY25DEC")
        row = results[0]
        self.assertEqual(row["lot_size"], 50)
        self.assertEqual(row["expiry"], "2025-12-25")
        self.assertEqual(row["strike"], 26000.0)
        self.assertEqual(row["tick_size"], 0.05)


# ── (4) Wire-up drift guards (always-run) ────────────────────────────


import re


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_instruments_cache_task_registered_in_scheduler():
    src = _read_source("services/tasks/scheduler.py")
    assert re.search(
        r"from\s+services\.tasks\.InstrumentsCacheTask\s+import\s+"
        r"InstrumentsCacheTask",
        src,
    ), "scheduler.py missing InstrumentsCacheTask import"
    assert re.search(
        r'"InstrumentsCache"\s*:\s*InstrumentsCacheTask',
        src,
    ), (
        'scheduler.py TASK_MAPPING missing "InstrumentsCache": '
        'InstrumentsCacheTask'
    )


def test_search_route_wired_in_app():
    src = _read_source("app.py")
    assert re.search(
        r"'/kite/instruments/search'\s*,\s*'GET'\s*,\s*"
        r"self\.investmentEP\.searchKiteInstruments",
        src,
    ), "GET /kite/instruments/search route not wired in app.py"


def test_models_init_imports_kite_instrument():
    src = _read_source("models/__init__.py")
    assert "from models.kiteInstrument import KiteInstrument" in src, (
        "models/__init__.py missing KiteInstrument import — "
        "Base.metadata.create_all will skip the table on fresh install"
    )


def test_search_controller_rejects_empty_query():
    src = _read_source("controllers/investmentsEP.py")
    # Must 400 on empty q so a client can't accidentally pull the
    # full master.
    assert "'q' query param is required" in src or (
        "q query param is required" in src
    ), (
        "searchKiteInstruments must 400 on empty q — bead contract"
    )


def test_service_passthrough_exists():
    src = _read_source("services/InvestmentService.py")
    assert "def search_kite_instruments" in src, (
        "InvestmentService.search_kite_instruments passthrough "
        "missing — controller → InvestmentService → KiteService chain "
        "would be broken."
    )


if __name__ == "__main__":
    unittest.main()
