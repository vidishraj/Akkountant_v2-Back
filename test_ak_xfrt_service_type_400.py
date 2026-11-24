"""ak-xfrt (2026-11-24): invalid serviceType / missing user id must be 400.

InvestmentController.getUserIdServiceType used to return its error as a
(response, 400) TUPLE while all 14 callers tested
`not isinstance(params, tuple)`, so the error tuple was unpacked as
(user_id, service_type) and the request crashed with a 500 (measured on
base for deleteAllInvestments?serviceType=Bogus). Contract now: the helper
returns a Response (status 400) on error and callers test
`isinstance(params, Response)`.

The endpoint list is discovered by AST (every method calling the helper),
so a new endpoint is covered automatically.
"""
from __future__ import annotations

import ast
import importlib.util
import logging
import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
CTRL = os.path.join(HERE, "controllers", "investmentsEP.py")
sys.path.insert(0, HERE)

try:
    import flask
    _HAVE_FLASK = True
except ImportError:  # pragma: no cover
    _HAVE_FLASK = False


def _callers():
    tree = ast.parse(open(CTRL).read())
    cls = next(n for n in tree.body
               if isinstance(n, ast.ClassDef) and n.name == "InvestmentController")
    out = []
    for fn in cls.body:
        if not isinstance(fn, ast.FunctionDef) or fn.name == "getUserIdServiceType":
            continue
        if any(isinstance(n, ast.Attribute) and n.attr == "getUserIdServiceType"
               for n in ast.walk(fn)):
            out.append(fn.name)
    return out


class TestCallSiteContract(unittest.TestCase):

    def test_fourteen_call_sites_discovered(self):
        self.assertEqual(len(_callers()), 14, _callers())

    def test_no_call_site_uses_the_tuple_check(self):
        tree = ast.parse(open(CTRL).read())
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call)
                    and getattr(node.func, "id", None) == "isinstance"
                    and len(node.args) == 2
                    and getattr(node.args[0], "id", None) == "params"):
                self.assertEqual(getattr(node.args[1], "id", None), "Response",
                                 ast.unparse(node))


@unittest.skipUnless(_HAVE_FLASK, "flask not installed")
class TestEveryEndpointReturns400(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        stubs = {
            "services.InvestmentService": types.ModuleType("services.InvestmentService"),
            "services.KiteService": types.ModuleType("services.KiteService"),
        }
        stubs["services.InvestmentService"].InvestmentService = object

        class KiteAuthError(Exception):
            pass
        stubs["services.KiteService"].KiteAuthError = KiteAuthError
        cls._p = mock.patch.dict(sys.modules, stubs)
        cls._p.start()
        spec = importlib.util.spec_from_file_location("akxfrt_ep", CTRL)
        cls.m = importlib.util.module_from_spec(spec)
        sys.modules["akxfrt_ep"] = cls.m
        spec.loader.exec_module(cls.m)
        cls.app = flask.Flask("akxfrt")

    @classmethod
    def tearDownClass(cls):
        cls._p.stop()

    def _ctrl(self):
        c = self.m.InvestmentController.__new__(self.m.InvestmentController)
        c.InvestmentService = mock.MagicMock()
        c.logger = logging.getLogger("akxfrt")
        return c

    def _call(self, method, query, user, body=None):
        c = self._ctrl()
        with self.app.test_request_context(f"/x?{query}", method="POST",
                                           json=body if body is not None else {"any": "body"}):
            if user:
                flask.g.firebase_id = user
            resp = getattr(c, method)()
        status = resp[1] if isinstance(resp, tuple) else getattr(resp, "status_code", None)
        body = resp[0] if isinstance(resp, tuple) else resp
        return status, body, c.InvestmentService

    def test_invalid_service_type_is_400_everywhere(self):
        for method in _callers():
            with self.subTest(method=method):
                status, body, svc = self._call(method, "serviceType=Bogus&buyId=1&page=1&schemeCode=S1", "u1")
                self.assertEqual(status, 400, method)
                self.assertIn("serviceType", body.get_json()["error"])
                # Nothing downstream ran.
                self.assertEqual(svc.method_calls, [], method)

    def test_missing_service_type_is_400_everywhere(self):
        for method in _callers():
            with self.subTest(method=method):
                status, _, svc = self._call(method, "buyId=1&page=1&schemeCode=S1", "u1")
                self.assertEqual(status, 400, method)
                self.assertEqual(svc.method_calls, [], method)

    def test_missing_user_is_400_everywhere(self):
        for method in _callers():
            with self.subTest(method=method):
                status, body, svc = self._call(method, "serviceType=Stocks&buyId=1&page=1&schemeCode=S1", None)
                self.assertEqual(status, 400, method)
                self.assertEqual(body.get_json()["error"], "User ID not found")
                self.assertEqual(svc.method_calls, [], method)

    def test_valid_input_still_reaches_the_service(self):
        # ak-b8em: delete-all now REQUIRES the confirm echo the FE sends.
        status, _, svc = self._call("deleteAllInvestments", "serviceType=Stocks", "u1",
                                    body={"confirm": "Stocks"})
        svc.deleteAll.assert_called_once()
        args = svc.deleteAll.call_args[0]
        self.assertEqual((args[0].name, args[1]), ("Stocks", "u1"))

    def test_helper_error_is_a_response_not_a_tuple(self):
        with self.app.test_request_context("/x"):
            flask.g.firebase_id = "u1"
            err = self.m.InvestmentController.getUserIdServiceType("Bogus")
            ok = self.m.InvestmentController.getUserIdServiceType("Gold")
        self.assertIsInstance(err, flask.Response)
        self.assertEqual(err.status_code, 400)
        self.assertEqual((ok[0], ok[1].name), ("u1", "Gold"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
