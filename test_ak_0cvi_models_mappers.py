"""ak-0cvi (P1): `import models` alone must yield a fully configurable ORM.

On prod the bare-engine scripts (ak-3oh audit, zejx backfill dry run) died
before any SQL: Mapper[User(users)] expression 'InvestmentHistory' failed to
locate a name. users.User has relationship('InvestmentHistory'), but
models/__init__.py never imported models.investmentHistory; the app only got
it transitively through the services. The existing script tests didn't see
it because their subprocesses import test helpers that import the module.

These run in CLEAN subprocesses (no test helpers, no services).
"""
from __future__ import annotations

import ast
import glob
import os
import subprocess
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def _clean(code):
    env = {k: v for k, v in os.environ.items()}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run([sys.executable, "-c", f"import sys; sys.path.insert(0, {HERE!r})\n" + code],
                          cwd=HERE, env=env, capture_output=True, text=True, timeout=120)


class TestModelsAloneConfigure(unittest.TestCase):

    def test_import_models_then_configure_mappers(self):
        r = _clean("import models\nimport sqlalchemy.orm as o\no.configure_mappers()\nprint('OK')\n")
        self.assertIn("OK", r.stdout, r.stderr[-800:])

    def test_script_import_path_configures(self):
        # What the 3oh audit imports before its first query.
        r = _clean("from models.freelance_management import Invoice, InvoicePayment\n"
                   "import sqlalchemy.orm as o\no.configure_mappers()\nprint('OK')\n")
        self.assertIn("OK", r.stdout, r.stderr[-800:])

    def test_every_string_relationship_target_is_registered_by_import_models(self):
        """Guard for the whole class: each relationship('Name') across
        models/ resolves to a class registered after a bare `import models`."""
        targets = set()
        for f in glob.glob(os.path.join(HERE, "models", "*.py")):
            for n in ast.walk(ast.parse(open(f).read())):
                if (isinstance(n, ast.Call)
                        and getattr(n.func, "id", getattr(n.func, "attr", "")) == "relationship"
                        and n.args and isinstance(n.args[0], ast.Constant)
                        and isinstance(n.args[0].value, str)):
                    targets.add(n.args[0].value.split(".")[-1])
        self.assertTrue(targets)
        r = _clean("import models\nfrom models.Base import Base\n"
                   "print('\\n'.join(sorted(m.class_.__name__ for m in Base.registry.mappers)))\n")
        registered = set(r.stdout.split())
        self.assertFalse(targets - registered, f"unregistered: {sorted(targets - registered)}")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
