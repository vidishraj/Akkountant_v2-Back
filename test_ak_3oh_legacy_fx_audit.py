"""ak-3oh: read-only audit of pre-ak-lvu non-INR invoice payments.

Pure classifier tests + build_report on the real invoice tables (sqlite,
ak-lvu integration boot) + the no-amounts / read-only guarantees.
Synthetic data only.
"""
from __future__ import annotations

import io
import os
import sys
import unittest
from contextlib import redirect_stdout
from decimal import Decimal
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import legacy_fx_audit as A  # noqa: E402

import test_invoice_service_integration as I  # noqa: E402  (import shims)


def legacy(amount_received):
    return {"amount_received": amount_received, "original_amount": None,
            "original_currency": None, "inr_amount": None, "fx_rate": None}


def post(orig, cur, inr, fx):
    return {"amount_received": inr, "original_amount": orig, "original_currency": cur,
            "inr_amount": inr, "fx_rate": fx}


class TestClassifier(unittest.TestCase):

    def test_usd_legacy_classes(self):
        c = A.classify_invoice
        self.assertEqual(c("USD", "paid", 100, [legacy(8325)]), A.OK)
        self.assertEqual(c("USD", "paid", 100, [legacy(7900)]), A.OK)          # drift / TDS
        self.assertEqual(c("USD", "paid", 100, [legacy(8325 * 83.25)]), A.DOUBLE_CONVERTED)
        self.assertEqual(c("USD", "paid", 100, [legacy(100)]), A.UNCONVERTED)
        self.assertEqual(c("USD", "paid", 100, [legacy(900)]), A.AMBIGUOUS)
        self.assertEqual(c("USD", "paid", 100, [legacy(4000), legacy(4325)]), A.OK)  # summed

    def test_jpy_levels_never_overlap(self):
        # R ~ 0.56: R**0 = 1, R**1 = 0.56, R**2 = 0.31 are close together.
        c = A.classify_invoice
        self.assertEqual(c("JPY", "paid", 10000, [legacy(5600)]), A.OK)
        self.assertEqual(c("JPY", "paid", 10000, [legacy(10000)]), A.UNCONVERTED)
        self.assertEqual(c("JPY", "paid", 10000, [legacy(3136)]), A.DOUBLE_CONVERTED)
        tol = A.tolerance_for(0.56)
        import math
        self.assertLess(2 * tol, abs(math.log(0.56)))

    def test_partial_and_edge_cases(self):
        c = A.classify_invoice
        self.assertEqual(c("USD", "partially_paid", 100, [legacy(4000)]), A.PARTIAL)
        self.assertEqual(c("USD", "paid", 100, []), A.NO_PAYMENT)
        self.assertIsNone(c("USD", "sent", 100, []))
        self.assertEqual(c("USD", "paid", 0, [legacy(10)]), A.ZERO_TOTAL)
        self.assertEqual(c("CHF", "paid", 100, [legacy(9000)]), A.UNKNOWN_CURRENCY)
        self.assertEqual(c("USD", "paid", 100, [legacy(8325), post(10, "USD", 832.5, 83.25)]),
                         A.MIXED)

    def test_post_lvu_rows(self):
        c = A.classify_invoice
        self.assertEqual(c("USD", "paid", 100, [post(100, "USD", 8325, 83.25)]), A.POST_OK)
        self.assertEqual(c("USD", "paid", 100, [post(100, "USD", 9000, 83.25)]),
                         A.POST_INR_MISMATCH)
        # V4-1: INR value stored as the USD original_amount.
        self.assertEqual(c("USD", "paid", 100, [post(8325, "USD", 693056.25, 83.25)]),
                         A.POST_ORIGINAL_IS_INR)
        # USD invoice paid in INR: not the V4-1 shape.
        self.assertEqual(c("USD", "paid", 100, [post(8325, "INR", 8325, 1)]), A.POST_OK)

    def test_jpy_results_are_labelled_approximate(self):
        r = A.Report()
        r.add("id-1", "JPY", A.OK)
        r.add("id-2", "USD", A.OK)
        text = "\n".join(r.lines())
        self.assertIn("note: JPY classes are APPROXIMATE", text)
        self.assertNotIn("USD classes are APPROXIMATE", text)

    def test_report_has_counts_and_numbers_only(self):
        r = A.Report()
        r.add("INV-1", "USD", A.OK)
        r.add("INV-2", "USD", A.DOUBLE_CONVERTED)
        r.add("INV-3", "EUR", None)
        text = "\n".join(r.lines())
        self.assertIn("non-INR invoices scanned: 3", text)
        self.assertIn("suspect_double_converted invoice ids: INV-2", text)
        self.assertNotIn("INV-1", text)                     # OK rows aren't listed
        self.assertNotRegex(text.replace("INV-", ""), r"\d{3,}")   # no amounts


@unittest.skipUnless(I._IMPORT_OK, I._SKIP_REASON)
class TestScriptOnTables(unittest.TestCase):

    def setUp(self):
        self.app, self.db, self.ctx = I._boot_integration_env()
        from models.freelance_management import (
            Invoice, InvoicePayment, CurrencyEnum, InvoiceStatusEnum)
        import datetime as dt
        s = self.db.session

        def inv(num, cur, status, total, pays):
            i = Invoice(user_id="TEST_USER", invoice_number=num, project_name="p",
                        issue_date=dt.date(2025, 1, 1), due_date=dt.date(2025, 2, 1),
                        from_name="a", from_email="a@x", from_address="x", to_name="b",
                        to_email="b@x", to_address="y", currency=CurrencyEnum[cur],
                        subtotal=total, total=total, status=InvoiceStatusEnum[status])
            s.add(i)
            s.flush()
            for p in pays:
                s.add(InvoicePayment(invoice_id=i.id, **p))
        inv("INV-OK", "USD", "paid", 100, [{"amount_received": 8325}])
        inv("INV-DBL", "USD", "paid", 100, [{"amount_received": Decimal("693056.25")}])
        inv("INV-RAW", "EUR", "paid", 200, [{"amount_received": 200}])
        inv("INV-INR", "INR", "paid", 5000, [{"amount_received": 5000}])
        inv("INV-PART", "USD", "partially_paid", 100, [{"amount_received": 4000}])
        s.commit()
        self.s = s

    def tearDown(self):
        self.ctx.pop()

    def test_build_report_classifies_and_skips_inr(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "ak3oh_script", os.path.join(HERE, "scripts", "ak3oh_legacy_fx_audit.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        rep = mod.build_report(self.s)
        from models.freelance_management import Invoice
        ids = {i.invoice_number: i.id for i in self.s.query(Invoice).all()}
        self.assertEqual(rep.scanned, 4)                     # INR invoice skipped
        # Internal ids, never invoice numbers (may embed client initials).
        self.assertEqual(rep.by_class[A.OK], [ids["INV-OK"]])
        self.assertEqual(rep.by_class[A.DOUBLE_CONVERTED], [ids["INV-DBL"]])
        self.assertEqual(rep.by_class[A.UNCONVERTED], [ids["INV-RAW"]])
        self.assertEqual(rep.by_class[A.PARTIAL], [ids["INV-PART"]])
        self.assertNotIn("INV-DBL", "\n".join(rep.lines()))

    def test_script_never_writes(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "ak3oh_script2", os.path.join(HERE, "scripts", "ak3oh_legacy_fx_audit.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        with mock.patch.object(self.s, "commit", side_effect=AssertionError("write!")), \
             mock.patch.object(self.s, "flush", side_effect=AssertionError("write!")):
            mod.build_report(self.s)
        src = open(os.path.join(HERE, "scripts", "ak3oh_legacy_fx_audit.py")).read()
        self.assertNotIn(".commit(", src)
        self.assertNotIn(".add(", src.replace("report.add(", ""))
        self.assertNotIn(".delete(", src)
        self.assertIn("SET SESSION TRANSACTION READ ONLY", src)
        self.assertIn("session.rollback()", src)
        # Review MAJOR: never boot the app (create_all DDL + schedulers).
        self.assertNotIn("from app import", src)
        self.assertNotIn("import app", src)
        flag = src.index('os.environ["AK_SKIP_SCHEDULERS"] = "1"')
        self.assertLess(flag, src.index("from utils import legacy_fx_audit"))

    def test_output_has_no_amounts(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "ak3oh_script3", os.path.join(HERE, "scripts", "ak3oh_legacy_fx_audit.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        rep = mod.build_report(self.s)
        buf = io.StringIO()
        with redirect_stdout(buf):
            for line in rep.lines():
                print(line)
        out = buf.getvalue()
        for amount in ("8325", "693056", "200", "4000"):
            self.assertNotIn(amount, out)


@unittest.skipUnless(I._IMPORT_OK, I._SKIP_REASON)
class TestMainEndToEnd(unittest.TestCase):
    """main() against a real sqlite FILE via DATABASE_URL, in a subprocess:
    exits 3 with ids listed, never imports app, sets the scheduler flag."""

    def test_main_on_sqlite_file_never_imports_app(self):
        import subprocess
        import tempfile
        tmp = tempfile.mkdtemp(dir=os.environ.get("TMPDIR"))
        db = os.path.join(tmp, "audit.db")
        seed = f"""
import os, sys, datetime as dt
sys.path.insert(0, {HERE!r})
import test_invoice_service_integration  # import shims
import sqlalchemy as sa
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.ext.compiler import compiles
@compiles(MEDIUMTEXT, "sqlite")
def _t(e, c, **k): return "TEXT"
from models.Base import Base
from models.freelance_management import Invoice, InvoicePayment, CurrencyEnum, InvoiceStatusEnum
eng = sa.create_engine("sqlite:///{db}")
Base.metadata.create_all(eng, tables=[Invoice.__table__, InvoicePayment.__table__])
from sqlalchemy.orm import Session
s = Session(eng)
i = Invoice(user_id="U", invoice_number="INV-SECRET-AB", project_name="p",
            issue_date=dt.date(2025,1,1), due_date=dt.date(2025,2,1), from_name="a",
            from_email="a@x", from_address="x", to_name="b", to_email="b@x", to_address="y",
            currency=CurrencyEnum.USD, subtotal=100, total=100, status=InvoiceStatusEnum.paid)
s.add(i); s.flush(); s.add(InvoicePayment(invoice_id=i.id, amount_received=693056.25)); s.commit()
print(i.id)
"""
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        inv_id = subprocess.run([sys.executable, "-c", seed], env=env, capture_output=True,
                                text=True, check=True).stdout.strip().splitlines()[-1]
        runner = f"""
import os, sys, runpy
sys.path.insert(0, {HERE!r})
import test_invoice_service_integration  # import shims only
os.environ["DATABASE_URL"] = "sqlite:///{db}"
sys.argv = ["ak3oh"]
try:
    runpy.run_path(os.path.join({HERE!r}, "scripts", "ak3oh_legacy_fx_audit.py"), run_name="__main__")
except SystemExit as e:
    print("EXIT", e.code)
print("APP_IMPORTED", "app" in sys.modules)
print("FLAG", os.environ.get("AK_SKIP_SCHEDULERS"))
"""
        out = subprocess.run([sys.executable, "-c", runner], env=env, capture_output=True,
                             text=True).stdout
        self.assertIn("EXIT 3", out)
        self.assertIn(f"suspect_double_converted invoice ids: {inv_id}", out)
        self.assertNotIn("INV-SECRET-AB", out)
        self.assertNotIn("693056", out)
        self.assertIn("APP_IMPORTED False", out)
        self.assertIn("FLAG 1", out)


class TestImportLeaksNoEnv(unittest.TestCase):
    """Loading the script as a module (tests do) must not set the flag in
    the importing process; running it as __main__ still does."""

    def test_loading_module_does_not_set_flag(self):
        import importlib.util
        from unittest import mock
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("AK_SKIP_SCHEDULERS", None)
            spec = importlib.util.spec_from_file_location("ak3oh_noleak", os.path.join(HERE, "scripts", "ak3oh_legacy_fx_audit.py"))
            m = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(m)
            self.assertNotIn("AK_SKIP_SCHEDULERS", os.environ)
        src = open(os.path.join(HERE, "scripts", "ak3oh_legacy_fx_audit.py")).read()
        self.assertIn('if __name__ == "__main__":\n    os.environ["AK_SKIP_SCHEDULERS"] = "1"', src)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
