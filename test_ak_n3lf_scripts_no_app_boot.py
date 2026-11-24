"""ak-n3lf: one-off scripts must not boot the app's prod side effects.

Importing `app` builds the Akkountant app at import time: db.create_all()
(DDL), seed job rows, and scheduler threads that write to prod. Every legacy
script now sets AK_SKIP_SCHEDULERS=1 and AK_SKIP_DB_INIT=1 (only when run as
a script) before ANY project import; app.py honours AK_SKIP_DB_INIT by
skipping create_all + the seed jobs. The db-only gold loader no longer
imports app at all (bare engine). Synthetic data only.
"""
from __future__ import annotations

import ast
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SCRIPTS = [
    "scripts/ak32o_hdfc_mp_fire.py", "scripts/ak_8kp4_phase3a_ingest_weekly_securities.py",
    "scripts/load_gold_historical_rates.py", "scripts/synthesize_historical_snapshots.py",
    "boi_backfill_2026.py", "reprocess_icici.py", "reprocess_pdfs.py", "run_mail_pipeline.py",
    "run_overnight_scan.py", "bootstrap_wealth_digest.py", "retry_failed_pdfs.py",
]
PROJECT_PACKAGES = {"app", "services", "models", "utils", "controllers", "dtos", "enums", "wsgi"}

# Intercepts `import app` in a subprocess: prints the gates as they are AT
# IMPORT TIME and stops the script before anything else runs.
_HOOK = r'''
import importlib.abc, importlib.machinery, os, sys
class _F(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, name, path, target=None):
        return importlib.machinery.ModuleSpec(name, self) if name == "app" else None
    def create_module(self, spec):
        return None
    def exec_module(self, module):
        print("APP_IMPORT", os.environ.get("AK_SKIP_SCHEDULERS"), os.environ.get("AK_SKIP_DB_INIT"))
        sys.stdout.flush()
        os._exit(0)
sys.meta_path.insert(0, _F())
'''


def _gate_lines(tree):
    """Line numbers where the two gates are assigned (anywhere at top level,
    including inside `if __name__ == "__main__":`)."""
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            t = node.targets[0]
            if (isinstance(t, ast.Subscript) and isinstance(t.slice, ast.Constant)
                    and t.slice.value in ("AK_SKIP_SCHEDULERS", "AK_SKIP_DB_INIT")
                    and isinstance(node.value, ast.Constant) and node.value.value == "1"):
                found[t.slice.value] = node.lineno
    return found


def _first_project_import(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [a.name.split(".")[0] for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names = [node.module.split(".")[0]]
        else:
            continue
        if any(n in PROJECT_PACKAGES for n in names):
            yield node.lineno


class TestEveryScriptSetsTheGatesFirst(unittest.TestCase):

    def test_gates_precede_every_project_import(self):
        for rel in SCRIPTS:
            with self.subTest(script=rel):
                tree = ast.parse(open(os.path.join(HERE, rel)).read())
                gates = _gate_lines(tree)
                self.assertEqual(set(gates), {"AK_SKIP_SCHEDULERS", "AK_SKIP_DB_INIT"}, rel)
                first = min(_first_project_import(tree), default=None)
                if first is not None:
                    self.assertLess(max(gates.values()), first, rel)

    def test_gold_loader_never_imports_app(self):
        tree = ast.parse(open(os.path.join(HERE, "scripts", "load_gold_historical_rates.py")).read())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                self.assertNotEqual(node.module, "app")
            if isinstance(node, ast.Import):
                self.assertNotIn("app", [a.name for a in node.names])


class TestRepresentativeScriptsAtRuntime(unittest.TestCase):
    """Run real scripts in a subprocess; the hook reports the gates at the
    moment the script imports app."""

    def _run(self, rel, args=()):
        code = _HOOK + (
            "import runpy\n"
            f"sys.argv = [{rel!r}] + {list(args)!r}\n"
            f"sys.path.insert(0, {HERE!r})\n"
            f"runpy.run_path({os.path.join(HERE, rel)!r}, run_name='__main__')\n")
        env = {k: v for k, v in os.environ.items()
               if k not in ("AK_SKIP_SCHEDULERS", "AK_SKIP_DB_INIT")}
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        return subprocess.run([sys.executable, "-c", code], env=env, capture_output=True,
                              text=True, cwd=HERE, timeout=120).stdout

    def test_root_script_imports_app_only_with_both_gates(self):
        out = self._run("retry_failed_pdfs.py")
        self.assertIn("APP_IMPORT 1 1", out)

    def test_scripts_dir_script_imports_app_only_with_both_gates(self):
        out = self._run("scripts/ak_8kp4_phase3a_ingest_weekly_securities.py", ["--help"])
        # --help may exit before importing app; when it does import, gates are set.
        if "APP_IMPORT" in out:
            self.assertIn("APP_IMPORT 1 1", out)

    def test_gold_loader_commit_path_does_not_import_app(self):
        tmp = tempfile.mkdtemp(dir=os.environ.get("TMPDIR"))
        csv_path = os.path.join(tmp, "gold.csv")
        with open(csv_path, "w") as f:
            f.write("date,rate_per_gram,source\n2026-11-21,7238.50,test\n")
        code = (_HOOK +
                "import runpy\n"
                f"sys.path.insert(0, {HERE!r})\n"
                "import test_invoice_service_integration  # import shims\n"
                f"os.environ['DATABASE_URL'] = 'sqlite:///{tmp}/x.db'\n"
                f"sys.argv = ['gold', {csv_path!r}, '--commit']\n"
                "try:\n"
                f"    runpy.run_path({os.path.join(HERE, 'scripts', 'load_gold_historical_rates.py')!r}, run_name='__main__')\n"
                "except SystemExit as e:\n"
                "    print('EXIT', e.code)\n"
                "print('APP_IN_MODULES', 'app' in sys.modules)\n")
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True,
                             text=True, cwd=HERE, timeout=120).stdout
        self.assertNotIn("APP_IMPORT", out)
        self.assertIn("APP_IN_MODULES False", out)
        self.assertIn("EXIT", out)                 # reached the DB step and returned


class TestAppDbInitGate(unittest.TestCase):

    def test_skip_db_init_runs_no_ddl_and_seeds_nothing(self):
        import test_invoice_service_integration  # noqa: F401  (import shims)
        import importlib
        import sqlalchemy as sa
        from flask import Flask
        tmp = tempfile.mkdtemp(dir=os.environ.get("TMPDIR"))
        url = f"sqlite:///{tmp}/gate.db"
        old = os.environ.get("AK_SKIP_DB_INIT")
        os.environ["AK_SKIP_DB_INIT"] = "1"
        try:
            src = open(os.path.join(HERE, "app.py")).read()
            # Load only the class method without running the module's
            # import-time app construction.
            tree = ast.parse(src)
            cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Akkountant")
            fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_setup_database")
            ns = {}
            g = {"os": os, "SQLAlchemy": importlib.import_module("flask_sqlalchemy").SQLAlchemy,
                 "models": importlib.import_module("models"), "inspect": sa.inspect,
                 "datetime": importlib.import_module("datetime")}
            exec(compile(ast.Module(body=[fn], type_ignores=[]), "app.py", "exec"), g, ns)
            fake = Flask("gate")
            fake.config["SQLALCHEMY_DATABASE_URI"] = url
            fake.logger.disabled = True
            fake._DB_INIT_LOCK_PATH = os.path.join(tmp, "lock")
            ns["_setup_database"](fake)
            self.assertIsNotNone(fake.db)
            self.assertEqual(sa.inspect(sa.create_engine(url)).get_table_names(), [])
        finally:
            if old is None:
                os.environ.pop("AK_SKIP_DB_INIT", None)
            else:
                os.environ["AK_SKIP_DB_INIT"] = old

    def _run_setup_database(self, fake_gunicorn):
        """Run the real Akkountant._setup_database on a fresh Flask app with
        AK_SKIP_DB_INIT=1; returns the tables that exist afterwards."""
        import test_invoice_service_integration  # noqa: F401  (import shims)
        import importlib
        import types
        from unittest import mock
        import sqlalchemy as sa
        from flask import Flask
        tmp = tempfile.mkdtemp(dir=os.environ.get("TMPDIR"))
        url = f"sqlite:///{tmp}/gate.db"
        src = open(os.path.join(HERE, "app.py")).read()
        tree = ast.parse(src)
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Akkountant")
        fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_setup_database")
        ns = {}
        g = {"os": os, "SQLAlchemy": importlib.import_module("flask_sqlalchemy").SQLAlchemy,
             "models": importlib.import_module("models"), "inspect": sa.inspect,
             "datetime": importlib.import_module("datetime")}
        exec(compile(ast.Module(body=[fn], type_ignores=[]), "app.py", "exec"), g, ns)
        fake = Flask("gate")
        fake.config["SQLALCHEMY_DATABASE_URI"] = url
        fake.logger = mock.MagicMock()
        fake._DB_INIT_LOCK_PATH = os.path.join(tmp, "lock")
        fake._insert_initial_jobs = mock.MagicMock()
        mods = {"gunicorn": types.ModuleType("gunicorn")} if fake_gunicorn else {}
        env = {"AK_SKIP_DB_INIT": "1"}
        with mock.patch.dict(os.environ, env), mock.patch.dict(sys.modules, mods):
            if not fake_gunicorn:
                sys.modules.pop("gunicorn", None)
            try:
                ns["_setup_database"](fake)
            except Exception:
                pass          # sqlite may reject some mysql-only DDL; tables seen so far count
        return sa.inspect(sa.create_engine(url)).get_table_names(), fake

    def test_flag_is_ignored_under_gunicorn(self):
        # Review minor 1: a stray AK_SKIP_DB_INIT in the server .env must not
        # disable create_all on prod.
        tables, fake = self._run_setup_database(fake_gunicorn=True)
        self.assertTrue(tables, "create_all must still run under gunicorn")
        errors = " ".join(str(c) for c in fake.logger.error.call_args_list)
        self.assertIn("AK_SKIP_DB_INIT ignored under gunicorn", errors)

    def test_flag_skips_outside_gunicorn(self):
        tables, fake = self._run_setup_database(fake_gunicorn=False)
        self.assertEqual(tables, [])
        fake._insert_initial_jobs.assert_not_called()
        self.assertFalse(fake.logger.error.called)

    def test_scripts_carry_the_new_db_usage_note(self):
        for rel in SCRIPTS:
            with self.subTest(script=rel):
                self.assertIn("on a NEW database, start app.service once first",
                              open(os.path.join(HERE, rel)).read())

    def test_gate_is_opt_in(self):
        src = open(os.path.join(HERE, "app.py")).read()
        self.assertIn("if os.getenv('AK_SKIP_DB_INIT') == '1':", src)
        body = src[src.index("    def _setup_database"):]
        body = body[:body.index("\n    def ", 10)]
        gate = body.index("if os.getenv('AK_SKIP_DB_INIT') == '1':")
        # The real calls (not the docstring mentions) come after the gate.
        self.assertLess(gate, body.index("                self.db.create_all()"))
        self.assertLess(gate, body.index('self._insert_initial_jobs("SetNPSRate"'))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
