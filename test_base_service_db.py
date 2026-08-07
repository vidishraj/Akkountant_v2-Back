"""ak-ojd Path B tests: BaseService.db returns current_app.db when
g.db is not set (fleet-wide fix for ak-6si/ak-m6k class).

Two test tiers:
  * Source-inspection guards (always run, no runtime deps):
    - BaseService.db preference order: g.db > current_app.db > legacy
    - Legacy branch emits a WARN log naming ak-6si/ak-m6k
    - TaskScheduler sets g.db = self.flask_app.db at task-invocation
      boundary (defense-in-depth mirror of reprocess_*.py pattern)
  * Behavioral tests with a minimal Flask + Flask-SQLAlchemy app
    context (skipped cleanly if the deps aren't available):
    - Inside app_context with g.db unset → returns current_app.db
    - Inside app_context with g.db set → returns g.db (preference)
    - Outside any app context → returns legacy DotDict + WARN
    - 100 self.db accesses in a background-task path do NOT
      materialize 100 engines (create_engine called EXACTLY once
      — the app.db's one). Regression guard for the connection-
      pool leak.
    - Cross-method read-after-write consistency (INSTANCE 2
      repro): write via one BaseService instance, read via
      another instance in the same app_context, both see each
      other's just-committed data.

Runs offline. AST-lifts nothing — Base_Service is a leaf that only
needs flask + flask_sqlalchemy + sqlalchemy.

Run:
    python3 -m unittest test_base_service_db
    python3 -m pytest test_base_service_db.py
"""

import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_BASE_SERVICE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "Base_Service.py",
)
_SCHEDULER_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "scheduler.py",
)


def _source(path):
    with open(path) as fh:
        return fh.read()


def _source_code_only(path):
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


# ── Source-inspection guards ────────────────────────────────────────────


class TestBaseServiceDbSourceInvariants(unittest.TestCase):
    """Guard the preference-order logic and the legacy-branch WARN.
    Regressing any of these puts us back at ak-6si/ak-m6k."""

    def test_preference_order_g_db_then_current_app_then_legacy(self):
        print("\n[ak-ojd — BaseService.db preference: g.db → current_app.db → legacy]")
        code = _source_code_only(_BASE_SERVICE_PATH)
        # Preference 1: g.db check + return.
        self.assertIn("if g.get('db') is not None:", code)
        self.assertIn("return g.db", code)
        # Preference 2: current_app.db fallback.
        self.assertIn("app_db = current_app.db", code)
        self.assertIn("if app_db is not None:", code)
        self.assertIn("return app_db", code)
        # RuntimeError caught (no app context).
        self.assertIn("except RuntimeError:", code)
        # AttributeError caught (current_app resolves but .db not set).
        self.assertIn("except AttributeError:", code)
        print("  ✓ 3-tier preference chain present with defensive except-branches")

    def test_legacy_branch_warns_naming_the_bugs(self):
        """The legacy create_engine branch MUST log a loud WARN that
        names the ak-6si + ak-m6k class — so a future misconfigured
        caller has a grep-able signature to trace back to root."""
        print("\n[ak-ojd — legacy fresh-engine branch WARNs naming ak-6si/ak-m6k]")
        src = _source(_BASE_SERVICE_PATH)
        # The WARN message body substrings.
        self.assertIn("ak-6si/ak-m6k", src)
        self.assertIn("leaks connections", src)
        self.assertIn("app.app_context()", src)
        self.assertIn("reprocess_*.py pattern", src)
        # And uses a module-level logger (self.logger not available
        # inside the property since BaseService doesn't init logger).
        code = _source_code_only(_BASE_SERVICE_PATH)
        self.assertIn("_logger = _AppLogger(__name__).get_logger()", code)
        self.assertIn("_logger.warning(", code)
        print("  ✓ WARN body + module-level logger both present")

    def test_current_app_imported(self):
        print("\n[ak-ojd — flask.current_app imported alongside g]")
        code = _source_code_only(_BASE_SERVICE_PATH)
        self.assertIn("from flask import g, current_app", code)
        print("  ✓ current_app import present")


class TestSchedulerSetsGDbAtBoundary(unittest.TestCase):
    """Defense-in-depth: TaskScheduler sets g.db = self.flask_app.db
    at task-invocation boundary (mirrors reprocess_*.py pattern).
    Redundant with the current_app.db fallback in Base_Service, but
    explicit-at-boundary makes the 'background tasks share the app's
    stable engine' contract visible in scheduler.py itself."""

    def test_both_scheduler_loops_set_g_db(self):
        print("\n[ak-ojd — TaskScheduler sets g.db in BOTH loop entries]")
        code = _source_code_only(_SCHEDULER_PATH)
        # Both loops set g.db.
        self.assertGreaterEqual(
            code.count("g.db = self.flask_app.db"), 2,
            "g.db = self.flask_app.db must appear in both scheduler loops",
        )
        # And both do it INSIDE the app_context (order matters — g is
        # request/app-context bound).
        import re
        # Rough shape check: `with self.flask_app.app_context():` is
        # followed within a few lines by `g.db = self.flask_app.db`.
        for m in re.finditer(
            r"with self\.flask_app\.app_context\(\):(.*?)(?=\n    def |\Z)",
            code, re.DOTALL,
        ):
            body = m.group(1)
            self.assertIn(
                "g.db = self.flask_app.db", body,
                "app_context block lacks g.db = self.flask_app.db",
            )
        print("  ✓ both loops set g.db inside their app_context")


# ── Behavioral tests (real Flask + Flask-SQLAlchemy) ────────────────────


try:
    from flask import Flask, g
    from flask_sqlalchemy import SQLAlchemy
    from sqlalchemy.orm import DeclarativeBase
    # Also verify BaseService itself imports — its transitive chain
    # pulls in google.oauth2 / firebase / other heavy deps that may
    # be missing in a bare test env. If any link is missing, the
    # behavioral tests below skip cleanly and only the source-
    # inspection guards above run.
    from services.Base_Service import BaseService  # noqa: F401
    _FLASK_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover — env-dep skip
    _FLASK_OK = False
    _SKIP_REASON = (
        f"flask / flask_sqlalchemy / BaseService import chain "
        f"unavailable: {_exc}"
    )


@unittest.skipUnless(_FLASK_OK, _SKIP_REASON)
class TestBaseServiceDbBehavioral(unittest.TestCase):
    """Behavioral: with a minimal Flask+Flask-SQLAlchemy app, verify
    the property returns what the ak-ojd Path B fix says it should.

    Uses `create_engine` monkey-patching to count invocations across
    100 self.db accesses — the connection-pool-leak regression guard."""

    def setUp(self):
        # Minimal Flask app + Flask-SQLAlchemy bound to in-memory SQLite.
        class _Base(DeclarativeBase):
            pass
        self.app = Flask(__name__)
        self.app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
        self.app.db = SQLAlchemy(self.app, model_class=_Base)
        # Reset the BaseService singleton state — the class itself is
        # shared across tests via the module-level BaseService class,
        # so we need to import fresh for each test.
        from services.Base_Service import BaseService
        self.svc = BaseService()

    def test_g_db_wins_over_current_app_db(self):
        print("\n[ak-ojd — g.db is preferred over current_app.db]")
        sentinel = object()
        with self.app.app_context():
            g.db = sentinel
            got = self.svc.db
            self.assertIs(got, sentinel, "g.db not preferred over current_app.db")
        print("  ✓ g.db returned when set")

    def test_current_app_db_fallback_when_g_db_unset(self):
        print("\n[ak-ojd — current_app.db returned when g.db is unset]")
        with self.app.app_context():
            # g.db not set — should fall through to current_app.db.
            got = self.svc.db
            self.assertIs(got, self.app.db,
                          "expected current_app.db, got something else")
        print("  ✓ current_app.db returned when g.db absent")

    def test_current_app_db_has_session_attribute(self):
        """The returned Flask-SQLAlchemy instance must expose a
        `.session` attribute so existing callers doing
        `self.db.session.<op>` keep working."""
        print("\n[ak-ojd — returned db exposes .session (Flask-SQLAlchemy scoped_session)]")
        with self.app.app_context():
            got = self.svc.db
            self.assertTrue(hasattr(got, "session"))
            self.assertTrue(hasattr(got.session, "query"))
            self.assertTrue(hasattr(got.session, "add"))
            self.assertTrue(hasattr(got.session, "commit"))
        print("  ✓ .session with query/add/commit all present")

    def test_no_engine_leak_across_100_accesses(self):
        """Regression: pre-Path-B, EVERY self.db access called
        create_engine(). Post-Path-B, the current_app.db branch
        should NOT create any new engines — it just returns the
        Flask-SQLAlchemy singleton. Monkey-patch create_engine and
        assert zero invocations across 100 accesses."""
        print("\n[ak-ojd — 100 self.db accesses do NOT call create_engine (pool-leak fix)]")
        import services.Base_Service as bs_mod
        call_count = {"n": 0}
        real_create_engine = bs_mod.create_engine

        def _counting_create_engine(*a, **k):
            call_count["n"] += 1
            return real_create_engine(*a, **k)

        bs_mod.create_engine = _counting_create_engine
        try:
            with self.app.app_context():
                for _ in range(100):
                    _ = self.svc.db  # 100 property accesses
            self.assertEqual(
                call_count["n"], 0,
                f"create_engine called {call_count['n']} times across 100 "
                f"self.db accesses — pool-leak regression",
            )
        finally:
            bs_mod.create_engine = real_create_engine
        print("  ✓ 0 create_engine calls in 100 accesses (was 100 pre-Path-B)")

    def test_legacy_fallback_when_no_app_context(self):
        """No app context at all → legacy branch fires + returns a
        DotDict + logs WARN. This branch stays around for
        backward-compat with any historical caller running outside
        an app; the WARN makes such callers surface."""
        print("\n[ak-ojd — no app context → legacy DotDict + WARN log]")
        # NO app_context wrapper here — direct property access.
        # Verify WARN via capture of the module-level logger.
        import logging
        import services.Base_Service as bs_mod
        records = []

        class _CaptureHandler(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        handler = _CaptureHandler()
        bs_mod._logger.addHandler(handler)
        try:
            got = self.svc.db
            # Legacy shape: DotDict with .session attribute.
            self.assertTrue(hasattr(got, "session"))
            # WARN emitted with the expected substring.
            self.assertTrue(
                any("ak-6si/ak-m6k" in m for m in records),
                f"expected ak-6si/ak-m6k WARN, got: {records}",
            )
        finally:
            bs_mod._logger.removeHandler(handler)
        print("  ✓ legacy fallback fires + WARN emitted with ak-6si/ak-m6k signature")

    def test_cross_method_read_after_write_consistency(self):
        """INSTANCE 2 (ak-m6k) repro: WRITE via one code path, READ
        via another (same app_context, both go through self.db).
        Pre-Path-B, the two would materialize DIFFERENT engines and
        the READ could miss the just-committed WRITE. Post-Path-B,
        both hit current_app.db's single engine → snapshot
        consistency by construction."""
        print("\n[ak-ojd — cross-method write→read sees just-committed data (INSTANCE 2 repro)]")
        # Use the app.db directly to define + create a small table.
        from sqlalchemy import Column, Integer, String
        with self.app.app_context():
            class _Item(self.app.db.Model):
                __tablename__ = "test_items_ak_ojd"
                id = Column(Integer, primary_key=True, autoincrement=True)
                name = Column(String(80))
            self.app.db.create_all()

            # Method A: write via svc.db (goes through the new property).
            svc_writer = type(self.svc)()
            item = _Item(name="ojd-probe")
            svc_writer.db.session.add(item)
            svc_writer.db.session.commit()
            new_id = item.id
            self.assertIsNotNone(new_id)

            # Method B: read via a DIFFERENT svc instance's self.db.
            svc_reader = type(self.svc)()
            found = svc_reader.db.session.query(_Item).filter(_Item.id == new_id).first()
            self.assertIsNotNone(
                found,
                "reader svc couldn't see writer svc's just-committed row — "
                "ak-m6k INSTANCE 2 regressed",
            )
            self.assertEqual(found.name, "ojd-probe")
        print(f"  ✓ writer.commit + reader.query on separate svc instances agree (id={new_id})")


if __name__ == "__main__":
    print("ak-ojd Path B — BaseService.db systemic fix tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
