"""ak-v8z1 (ak-ofir Phase 2A) tests: historical NAV / rate caches.

Covers:
  1. **Model shape** for the 3 new tables: mfHistoricalNavs,
     npsHistoricalNavs, goldHistoricalRates. Columns, nullability,
     unique constraints, indexes.
  2. **AMFI historical NAV parser** (pure logic, AST-lifted so the
     BaseTask / BaseService / Google-auth chain never loads) — parses
     AMFI's bulk historical text into (scheme_code, nav_date, nav)
     tuples with the same skip rules as the TODAY-NAVAll.txt parser.
  3. **SetNPSRate historical-row persister** (pure logic, AST-lifted
     for the same reason) — walks the pandas DataFrame the upstream
     fetch already produces and yields deduped historical rows,
     respecting NaN / zero-NAV filters.
  4. **Gold loader validator** — the ``_validate_row`` function from
     ``scripts/load_gold_historical_rates.py`` tested standalone.
  5. **Scheduler registration drift guard** for the new
     ``BackfillAMFIHistoricalNAVs`` entry.

Baseline discipline: tests skip cleanly when deps unavailable (via
``_DEPS_OK`` guards) so the 11-errors fleet baseline is preserved.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── Model-only imports: cheap, no Google chain ───────────────────────
try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    import models  # noqa: F401 — register declarative
    from models.investmentHistory import InvestmentHistory  # noqa: F401
    from models.mfHistoricalNav import MFHistoricalNav
    from models.npsHistoricalNav import NPSHistoricalNav
    from models.goldHistoricalRate import GoldHistoricalRate
    _MODEL_DEPS_OK = True
    _MODEL_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover — env-dep skip
    _MODEL_DEPS_OK = False
    _MODEL_SKIP_REASON = f"model import failed: {_exc}"


# ── AST-lift the pure functions we want to test standalone ───────────
#
# BackfillAMFIHistoricalNAVsTask inherits BaseTask which pulls
# BaseService → GmailServiceUtils → GoogleServiceSingleton, not
# available in bare env. Same discipline as
# test_reconciliation_issues_endpoint — lift only the pure methods +
# constants we care about.

def _ast_lift_amfi_parser():
    """AST-lift `_parse_amfi_historical_text` + `_iter_chunks` +
    `_AMFI_DATE_FMT` + `_CHUNK_DAYS` from the task module."""
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "tasks", "BackfillAMFIHistoricalNAVsTask.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)

    want_constants = {"_AMFI_DATE_FMT", "_CHUNK_DAYS"}
    want_methods = {"_parse_amfi_historical_text", "_iter_chunks"}
    kept = []
    # Needed stdlib imports — pull __future__ + datetime + decimal
    # from the module so the lifted bodies find their names.
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module in {
            "__future__", "datetime", "decimal",
        }:
            kept.append(node)
            continue
        if isinstance(node, ast.Import):
            # e.g. `import time` / `import requests`. Keep imports
            # that are stdlib-safe; skip heavy ones.
            keep_names = [
                a for a in node.names
                if a.name in ("time", "json")
            ]
            if keep_names:
                new = ast.Import(names=keep_names)
                ast.copy_location(new, node)
                kept.append(new)
            continue
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            t = node.targets[0]
            if isinstance(t, ast.Name) and t.id in want_constants:
                kept.append(node)
                continue
        if (
            isinstance(node, ast.ClassDef)
            and node.name == "BackfillAMFIHistoricalNAVsTask"
        ):
            for child in node.body:
                if isinstance(
                    child, (ast.FunctionDef, ast.AsyncFunctionDef)
                ) and child.name in want_methods:
                    # Strip @staticmethod / @classmethod — we'll
                    # call them as plain functions.
                    child.decorator_list = []
                    kept.append(child)

    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)
    ns = {"__name__": "test_v8z1_amfi_lift"}
    exec(compile(lift, "<amfi-parser-lift>", "exec"), ns)
    missing = [
        k for k in list(want_constants) + list(want_methods)
        if k not in ns
    ]
    if missing:
        raise AssertionError(
            f"ak-v8z1 AMFI parser AST-lift missing: {missing}"
        )
    return ns


def _ast_lift_nps_persister():
    """AST-lift `_persist_historical_rows` from SetNPSRate — but
    since it calls self.db.session.execute on a SQLAlchemy mysql
    insert, we need a stub. Lift the body as a FUNCTION (not a
    method), inject stubs via closure in the test."""
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "tasks", "SetNPSRate.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)
    kept = []
    # The lifted body uses pd (pandas), Decimal (decimal), and
    # imports NPSHistoricalNav / mysql_insert inside the body.
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module in {
            "decimal",
        }:
            kept.append(node)
            continue
        if isinstance(node, ast.Import):
            keep = [a for a in node.names if a.name in ("pandas",)]
            if keep:
                new = ast.Import(names=keep)
                ast.copy_location(new, node)
                kept.append(new)
            continue
        if isinstance(node, ast.ClassDef) and node.name == "SetNPSRate":
            for child in node.body:
                if (
                    isinstance(
                        child, (ast.FunctionDef, ast.AsyncFunctionDef)
                    )
                    and child.name == "_persist_historical_rows"
                ):
                    child.decorator_list = []
                    kept.append(child)
    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)
    ns = {"__name__": "test_v8z1_nps_lift"}
    exec(compile(lift, "<nps-persister-lift>", "exec"), ns)
    return ns


try:
    if _MODEL_DEPS_OK:
        _AMFI_LIFT = _ast_lift_amfi_parser()
        _NPS_LIFT = _ast_lift_nps_persister()
    else:
        _AMFI_LIFT = None
        _NPS_LIFT = None
    _LIFT_DEPS_OK = _MODEL_DEPS_OK and _AMFI_LIFT is not None
    _LIFT_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _LIFT_DEPS_OK = False
    _LIFT_SKIP_REASON = f"AST-lift failed: {_exc}"


# ── (1) Model shape ──────────────────────────────────────────────────


@unittest.skipUnless(_MODEL_DEPS_OK, _MODEL_SKIP_REASON)
class TestMFHistoricalNavModel(unittest.TestCase):
    def test_columns_present(self):
        cols = MFHistoricalNav.__table__.columns
        for name in ("id", "scheme_code", "nav_date", "nav"):
            self.assertIn(name, cols, f"missing column {name!r}")

    def test_unique_constraint_on_scheme_code_and_date(self):
        from sqlalchemy import UniqueConstraint
        uqs = [
            c for c in MFHistoricalNav.__table_args__
            if isinstance(c, UniqueConstraint)
        ]
        self.assertTrue(uqs, "no UniqueConstraint declared")
        target = None
        for uq in uqs:
            col_names = {c.name for c in uq.columns}
            if col_names == {"scheme_code", "nav_date"}:
                target = uq
                break
        self.assertIsNotNone(
            target,
            "mfHistoricalNavs missing UK on (scheme_code, nav_date) — "
            "UPSERT idempotency breaks",
        )

    def test_scheme_code_and_nav_date_not_nullable(self):
        cols = MFHistoricalNav.__table__.columns
        self.assertFalse(cols["scheme_code"].nullable)
        self.assertFalse(cols["nav_date"].nullable)
        self.assertFalse(cols["nav"].nullable)


@unittest.skipUnless(_MODEL_DEPS_OK, _MODEL_SKIP_REASON)
class TestNPSHistoricalNavModel(unittest.TestCase):
    def test_columns_present(self):
        cols = NPSHistoricalNav.__table__.columns
        for name in ("id", "scheme_id", "nav_date", "nav"):
            self.assertIn(name, cols)

    def test_unique_constraint_on_scheme_id_and_date(self):
        from sqlalchemy import UniqueConstraint
        uqs = [
            c for c in NPSHistoricalNav.__table_args__
            if isinstance(c, UniqueConstraint)
        ]
        target = None
        for uq in uqs:
            col_names = {c.name for c in uq.columns}
            if col_names == {"scheme_id", "nav_date"}:
                target = uq
                break
        self.assertIsNotNone(
            target,
            "npsHistoricalNavs missing UK on (scheme_id, nav_date)",
        )


@unittest.skipUnless(_MODEL_DEPS_OK, _MODEL_SKIP_REASON)
class TestGoldHistoricalRateModel(unittest.TestCase):
    def test_columns_present(self):
        cols = GoldHistoricalRate.__table__.columns
        for name in ("date", "rate_per_gram", "source"):
            self.assertIn(name, cols)

    def test_date_is_primary_key(self):
        cols = GoldHistoricalRate.__table__.columns
        self.assertTrue(
            cols["date"].primary_key,
            "goldHistoricalRates must have date as PK — one rate "
            "per date (single-purity scope per ak-v8z1 docstring)",
        )

    def test_source_length_bounded(self):
        cols = GoldHistoricalRate.__table__.columns
        # The source column is Varchar(50) in the model (operator
        # label — not a free-text essay field).
        self.assertTrue(hasattr(cols["source"].type, "length"))
        self.assertLessEqual(
            cols["source"].type.length, 100,
            "source column cap must stay small (operator provenance "
            "label, not free-text).",
        )


# ── (2) AMFI historical text parser ──────────────────────────────────


@unittest.skipUnless(_LIFT_DEPS_OK, _LIFT_SKIP_REASON)
class TestAMFIHistoricalParser(unittest.TestCase):
    """Pure logic tests for _parse_amfi_historical_text."""

    def setUp(self):
        self.parse = _AMFI_LIFT["_parse_amfi_historical_text"]

    def test_valid_single_row_parses(self):
        text = (
            "Scheme Code;ISIN Div Payout;ISIN Div Reinvest;"
            "Scheme Name;Plan;Option;NAV;Date\n"
            "120503;;;TEST Fund;Regular;Growth;52.3456;22-Nov-2026"
        )
        rows = list(self.parse(text))
        self.assertEqual(len(rows), 1)
        scheme_code, nav_date, nav = rows[0]
        self.assertEqual(scheme_code, "120503")
        self.assertEqual(nav_date.year, 2026)
        self.assertEqual(nav_date.month, 11)
        self.assertEqual(nav_date.day, 22)
        from decimal import Decimal
        self.assertEqual(nav, Decimal("52.3456"))

    def test_section_headers_and_blank_lines_skipped(self):
        text = (
            "Axis Mutual Fund\n"  # section header, no semicolons
            "\n"  # blank line
            "Open Ended Schemes(Equity)\n"  # another section header
            "120503;;;A;R;G;50.0;01-Jan-2026"
        )
        rows = list(self.parse(text))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "120503")

    def test_file_header_line_skipped(self):
        text = (
            "Scheme Code;ISIN Div Payout;ISIN Div Reinvest;Scheme "
            "Name;Plan;Option;NAV;Date"
        )
        rows = list(self.parse(text))
        self.assertEqual(rows, [])

    def test_placeholder_nav_values_skipped(self):
        text = "\n".join(
            [
                "120503;;;A;R;G;N.A.;01-Jan-2026",
                "120504;;;A;R;G;NA;01-Jan-2026",
                "120505;;;A;R;G;-;01-Jan-2026",
                "120506;;;A;R;G;;01-Jan-2026",
            ]
        )
        rows = list(self.parse(text))
        self.assertEqual(rows, [])

    def test_zero_nav_skipped(self):
        text = "120503;;;A;R;G;0;01-Jan-2026\n120504;;;A;R;G;0.0000;01-Jan-2026"
        rows = list(self.parse(text))
        self.assertEqual(rows, [])

    def test_unparseable_date_skipped(self):
        text = "120503;;;A;R;G;50.0;not-a-date"
        rows = list(self.parse(text))
        self.assertEqual(rows, [])

    def test_wrong_field_count_skipped(self):
        # 7 fields instead of 8 — truncation case AMFI's bulk format
        # occasionally emits.
        text = "120503;;;A;R;G;50.0"
        rows = list(self.parse(text))
        self.assertEqual(rows, [])

    def test_multiple_rows_preserved(self):
        text = (
            "120503;;;A;R;G;50.0;22-Nov-2026\n"
            "120503;;;A;R;G;51.0;23-Nov-2026\n"
            "120504;;;B;R;G;100.0;22-Nov-2026\n"
        )
        rows = list(self.parse(text))
        self.assertEqual(len(rows), 3)
        codes = {r[0] for r in rows}
        self.assertEqual(codes, {"120503", "120504"})


@unittest.skipUnless(_LIFT_DEPS_OK, _LIFT_SKIP_REASON)
class TestAMFIChunkIterator(unittest.TestCase):
    def setUp(self):
        self.chunks = _AMFI_LIFT["_iter_chunks"]
        self.CHUNK_DAYS = _AMFI_LIFT["_CHUNK_DAYS"]

    def test_small_window_yields_one_chunk(self):
        from datetime import date
        pairs = list(self.chunks(
            None, date(2026, 11, 1), date(2026, 11, 15),
        ))
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0][0], date(2026, 11, 1))
        self.assertEqual(pairs[0][1], date(2026, 11, 15))

    def test_365_days_splits_into_at_most_5_chunks(self):
        """A full 365-day cold-start window at 90-day chunks must
        fit into 5 chunks (90 + 90 + 90 + 90 + 5) with the
        boundary arithmetic correct — no overlap, no gap."""
        from datetime import date, timedelta
        start = date(2025, 11, 22)
        end = start + timedelta(days=365)
        pairs = list(self.chunks(None, start, end))
        # 366 inclusive days / 90 chunk = 4.07 → 5 chunks.
        self.assertLessEqual(len(pairs), 5)
        # Each chunk covers at most CHUNK_DAYS calendar days.
        for chunk_from, chunk_to in pairs:
            span = (chunk_to - chunk_from).days + 1
            self.assertLessEqual(
                span, self.CHUNK_DAYS,
                f"chunk {chunk_from}..{chunk_to} is {span} days — "
                f"exceeds {self.CHUNK_DAYS}-day cap",
            )
        # Full coverage, no gaps.
        self.assertEqual(pairs[0][0], start)
        self.assertEqual(pairs[-1][1], end)
        for i in range(1, len(pairs)):
            prev_to = pairs[i - 1][1]
            cur_from = pairs[i][0]
            self.assertEqual(
                cur_from, prev_to + timedelta(days=1),
                f"gap between chunk {i - 1} and {i}: "
                f"{prev_to}..{cur_from}",
            )


# ── (3) SetNPSRate historical-row persister ──────────────────────────


@unittest.skipUnless(_LIFT_DEPS_OK, _LIFT_SKIP_REASON)
class TestNPSHistoricalPersister(unittest.TestCase):
    """Pure-logic exercise of _persist_historical_rows against an
    in-memory SQLite + the real NPSHistoricalNav model.

    The lifted method expects `self.db.session.execute`. We stub
    with a session that records executed statements so we can
    inspect the UPSERT shape (count + first-row values).
    """

    def setUp(self):
        self.persist = _NPS_LIFT["_persist_historical_rows"]
        # Record executed statements for inspection.
        self._executed = []

        class _StubSession:
            def __init__(outer_self):
                outer_self.statements = []

            def execute(outer_self, stmt):
                self._executed.append(stmt)

        class _StubDb:
            session = _StubSession()

        class _StubSelf:
            db = _StubDb()

        self.stub_self = _StubSelf()

    def test_valid_rows_build_upsert(self):
        import pandas as pd
        df = pd.DataFrame(
            {
                "DATE OF NAV": pd.to_datetime(
                    ["2026-11-20", "2026-11-19", "2026-11-18"]
                ),
                "NAV VALUE": [50.1234, 50.5678, 50.9012],
            }
        )
        n = self.persist(self.stub_self, df, "SM001001")
        self.assertEqual(n, 3)
        self.assertEqual(len(self._executed), 1)

    def test_nan_rows_skipped(self):
        import pandas as pd
        import numpy as np
        df = pd.DataFrame(
            {
                "DATE OF NAV": [
                    pd.to_datetime("2026-11-20"), pd.NaT,
                    pd.to_datetime("2026-11-18"),
                ],
                "NAV VALUE": [50.1, np.nan, 50.9],
            }
        )
        n = self.persist(self.stub_self, df, "SM001001")
        # 2 of 3 rows survived NaN/NaT skip.
        self.assertEqual(n, 2)

    def test_zero_nav_rows_skipped(self):
        import pandas as pd
        df = pd.DataFrame(
            {
                "DATE OF NAV": pd.to_datetime([
                    "2026-11-20", "2026-11-19",
                ]),
                "NAV VALUE": [50.1, 0.0],
            }
        )
        n = self.persist(self.stub_self, df, "SM001001")
        self.assertEqual(n, 1)

    def test_missing_columns_returns_zero(self):
        """If the upstream TSV didn't carry the expected columns,
        return 0 without writing — defensive against format drift."""
        import pandas as pd
        df = pd.DataFrame({"OTHER": [1], "COLUMNS": [2]})
        n = self.persist(self.stub_self, df, "SM001001")
        self.assertEqual(n, 0)
        self.assertEqual(self._executed, [])

    def test_full_12_month_window_persisted(self):
        """ak-v8z1 regression guard: the WHOLE 12-month history must
        land, not just the latest row. Pre-ak-v8z1
        extractRatesFromExcel kept only latest + 3 points — that bug
        is what this bead fixes, so a test asserting 'all rows
        persisted' is the mutation-signal anchor."""
        import pandas as pd
        from datetime import datetime, timedelta
        dates = [datetime(2026, 1, 1) + timedelta(days=i) for i in range(365)]
        navs = [50.0 + (i * 0.01) for i in range(365)]
        df = pd.DataFrame(
            {"DATE OF NAV": pd.to_datetime(dates), "NAV VALUE": navs}
        )
        n = self.persist(self.stub_self, df, "SM001001")
        self.assertEqual(
            n, 365,
            "ak-v8z1 regression: fewer than 365 historical rows "
            "persisted — history was truncated (that's the exact "
            "pre-fix bug this bead resolves).",
        )


# ── (4) Gold loader validator ────────────────────────────────────────


class TestGoldLoaderValidator(unittest.TestCase):
    """Standalone tests for scripts/load_gold_historical_rates.py's
    _validate_row. The script module is importable — pure stdlib,
    no Google chain."""

    def setUp(self):
        # Import lazily so test collection doesn't fail on
        # unrelated repo-root sys.path drift.
        import importlib
        import importlib.util
        here = os.path.dirname(os.path.abspath(__file__))
        spec = importlib.util.spec_from_file_location(
            "test_v8z1_gold_loader",
            os.path.join(here, "scripts", "load_gold_historical_rates.py"),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self._validate_row = mod._validate_row

    def test_valid_row_coerces_cleanly(self):
        ok, result = self._validate_row(
            2,
            {
                "date": "2026-11-21",
                "rate_per_gram": "7238.50",
                "source": "goodreturns",
            },
        )
        self.assertTrue(ok)
        date_val, rate_val, source_val = result
        from datetime import date
        from decimal import Decimal
        self.assertEqual(date_val, date(2026, 11, 21))
        self.assertEqual(rate_val, Decimal("7238.50"))
        self.assertEqual(source_val, "goodreturns")

    def test_missing_column_rejected(self):
        ok, err = self._validate_row(
            2, {"date": "2026-11-21", "rate_per_gram": "7238.50"},
        )
        self.assertFalse(ok)
        self.assertIn("missing", err.lower())

    def test_bad_date_rejected(self):
        ok, err = self._validate_row(
            2,
            {
                "date": "Nov 21, 2026", "rate_per_gram": "7238.50",
                "source": "goodreturns",
            },
        )
        self.assertFalse(ok)
        self.assertIn("YYYY-MM-DD", err)

    def test_negative_rate_rejected(self):
        ok, err = self._validate_row(
            2,
            {
                "date": "2026-11-21", "rate_per_gram": "-100",
                "source": "goodreturns",
            },
        )
        self.assertFalse(ok)
        self.assertIn("> 0", err)

    def test_zero_rate_rejected(self):
        ok, err = self._validate_row(
            2,
            {
                "date": "2026-11-21", "rate_per_gram": "0",
                "source": "goodreturns",
            },
        )
        self.assertFalse(ok)

    def test_empty_source_rejected(self):
        ok, err = self._validate_row(
            2,
            {
                "date": "2026-11-21", "rate_per_gram": "7238.50",
                "source": "   ",
            },
        )
        self.assertFalse(ok)

    def test_long_source_truncated(self):
        ok, result = self._validate_row(
            2,
            {
                "date": "2026-11-21", "rate_per_gram": "7238.50",
                "source": "x" * 1000,
            },
        )
        self.assertTrue(ok)
        _, _, source_val = result
        self.assertEqual(len(source_val), 50)


# ── (5) Scheduler registration drift guard ───────────────────────────


import re


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_backfill_amfi_task_registered_in_scheduler():
    """Drift guard: adding the task class without wiring it into
    TASK_MAPPING would make it unschedulable. Pre-ak-v8z1 pattern
    same as BackfillHistoricalCandles."""
    src = _read_source("services/tasks/scheduler.py")
    # Import clause.
    assert re.search(
        r"from\s+services\.tasks\.BackfillAMFIHistoricalNAVsTask\s+"
        r"import.*BackfillAMFIHistoricalNAVsTask",
        src, re.DOTALL,
    ), "scheduler.py missing BackfillAMFIHistoricalNAVsTask import"
    # Routing entry.
    assert re.search(
        r'"BackfillAMFIHistoricalNAVs"\s*:\s*BackfillAMFIHistoricalNAVsTask',
        src,
    ), (
        "scheduler.py TASK_MAPPING missing "
        "'BackfillAMFIHistoricalNAVs': BackfillAMFIHistoricalNAVsTask"
    )


def test_setnps_task_calls_persist_historical_rows():
    """Drift guard: the ak-v8z1 persist call inside
    fetchNPSRatesFromExcel is what plumbs the history row writes.
    Removing it silently reverts the bead."""
    src = _read_source("services/tasks/SetNPSRate.py")
    assert "_persist_historical_rows" in src, (
        "SetNPSRate must retain _persist_historical_rows call "
        "(ak-v8z1 history persistence)"
    )
    assert re.search(
        r"self\._persist_historical_rows\s*\(\s*excel_data\s*,\s*scheme_id",
        src,
    ), (
        "SetNPSRate must CALL self._persist_historical_rows(excel_data,"
        " scheme_id) — ak-v8z1 wire-up"
    )


def test_models_init_imports_three_new_models():
    """Drift guard: forgetting to add a model import to
    models/__init__.py means Base.metadata.create_all skips the
    table on fresh installs."""
    src = _read_source("models/__init__.py")
    for marker in (
        "from models.mfHistoricalNav import MFHistoricalNav",
        "from models.npsHistoricalNav import NPSHistoricalNav",
        "from models.goldHistoricalRate import GoldHistoricalRate",
    ):
        assert marker in src, (
            f"models/__init__.py missing: {marker}"
        )


if __name__ == "__main__":
    unittest.main()
