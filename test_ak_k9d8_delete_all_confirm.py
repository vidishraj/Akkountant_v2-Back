"""ak-k9d8 (2026-11-24): server-side confirm on DELETE /deleteAllInvestments.

Defence in depth against stray or replayed calls; the human confirmation
is the FE type-to-confirm dialog. Wire contract (frontend 2b0c9b7):
DELETE /deleteAllInvestments?serviceType=<ST> with JSON body
{"confirm": "<ST>"}, where confirm is the serviceType enum name
(Stocks | Mutual_Funds | NPS | EPF | PF | Gold), compared exactly.

ak-b8em: the one-deploy-cycle transition is over. A MISSING confirm is
rejected (400 DELETE_ALL_CONFIRM_REQUIRED); a present-but-wrong confirm,
including a body that is present but unparsable / not JSON, is rejected
(400 DELETE_ALL_CONFIRM_MISMATCH). NOTHING is deleted on any 400.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

try:
    import flask  # noqa: F401
    _HAVE_FLASK = True
except ImportError:  # pragma: no cover
    _HAVE_FLASK = False

SERVICE_TYPES = ("Stocks", "Mutual_Funds", "NPS", "EPF", "PF", "Gold")


def _load_controller():
    stubs = {
        "services.InvestmentService": types.ModuleType("services.InvestmentService"),
        "services.KiteService": types.ModuleType("services.KiteService"),
    }
    stubs["services.InvestmentService"].InvestmentService = object

    class KiteAuthError(Exception):
        pass
    stubs["services.KiteService"].KiteAuthError = KiteAuthError
    patcher = mock.patch.dict(sys.modules, stubs)
    patcher.start()
    spec = importlib.util.spec_from_file_location(
        "akk9d8_investmentsEP", os.path.join(HERE, "controllers", "investmentsEP.py"))
    m = importlib.util.module_from_spec(spec)
    sys.modules["akk9d8_investmentsEP"] = m
    spec.loader.exec_module(m)
    return m, patcher


@unittest.skipUnless(_HAVE_FLASK, "flask not installed")
class TestCheckHelper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.m, cls._p = _load_controller()

    @classmethod
    def tearDownClass(cls):
        cls._p.stop()

    def test_exact_match_ok(self):
        for st in SERVICE_TYPES:
            self.assertEqual(self.m.check_delete_all_confirm(st, {"confirm": st}),
                             self.m.CONFIRM_OK)

    def test_missing(self):
        for body in (None, {}, {"other": 1}):
            self.assertEqual(self.m.check_delete_all_confirm("Stocks", body),
                             self.m.CONFIRM_MISSING, body)

    def test_unparsed_but_present_body_is_mismatch(self):
        # ak-b8em
        self.assertEqual(self.m.check_delete_all_confirm("Stocks", None, raw_body_present=True),
                         self.m.CONFIRM_MISMATCH)
        self.assertEqual(self.m.check_delete_all_confirm("Stocks", None, raw_body_present=False),
                         self.m.CONFIRM_MISSING)

    def test_mismatch_variants(self):
        bad = [
            {"confirm": "Mutual_Funds"},   # another asset
            {"confirm": "stocks"},         # case-sensitive
            {"confirm": "Mutual Funds"},   # display name, not the enum name
            {"confirm": " Stocks"},        # no trimming
            {"confirm": None},             # present but null
            {"confirm": ["Stocks"]},       # non-string
            ["Stocks"],                    # non-object body
            "Stocks",
        ]
        for body in bad:
            self.assertEqual(self.m.check_delete_all_confirm("Stocks", body),
                             self.m.CONFIRM_MISMATCH, body)

    def test_no_asset_confirmation_satisfies_another(self):
        for a in SERVICE_TYPES:
            for b in SERVICE_TYPES:
                if a != b:
                    self.assertEqual(
                        self.m.check_delete_all_confirm(a, {"confirm": b}),
                        self.m.CONFIRM_MISMATCH)


@unittest.skipUnless(_HAVE_FLASK, "flask not installed")
class TestEndpoint(unittest.TestCase):
    """Drive the real controller method inside a Flask request context."""

    @classmethod
    def setUpClass(cls):
        cls.m, cls._p = _load_controller()
        cls.app = flask.Flask("akk9d8")

    @classmethod
    def tearDownClass(cls):
        cls._p.stop()

    def setUp(self):
        ctrl = self.m.InvestmentController.__new__(self.m.InvestmentController) \
            if hasattr(self.m, "InvestmentController") else None
        if ctrl is None:  # resolve the controller class by its method
            cls = next(v for v in vars(self.m).values()
                       if isinstance(v, type) and hasattr(v, "deleteAllInvestments"))
            ctrl = cls.__new__(cls)
        ctrl.InvestmentService = mock.MagicMock()
        ctrl.InvestmentService.deleteAll.return_value = ({"deleted": True}, 200)
        ctrl.logger = logging.getLogger("akk9d8.ctrl")
        self.ctrl = ctrl

    def _call(self, st, **req_kw):
        with self.app.test_request_context(
                f"/deleteAllInvestments?serviceType={st}", method="DELETE", **req_kw):
            flask.g.firebase_id = "TestUid_1"
            resp = self.ctrl.deleteAllInvestments()
        if isinstance(resp, tuple):
            body, status = resp[0], resp[1]
            data = body.get_json() if hasattr(body, "get_json") else body
            return status, data
        if isinstance(resp, flask.Response):  # ak-xfrt: 400 as a Response
            return resp.status_code, resp.get_json()
        return 200, resp

    def test_match_deletes_that_asset(self):
        status, _ = self._call("Stocks", json={"confirm": "Stocks"})
        self.assertEqual(status, 200)
        args = self.ctrl.InvestmentService.deleteAll.call_args[0]
        self.assertEqual(args[0].name, "Stocks")
        self.assertEqual(args[1], "TestUid_1")

    def test_mismatch_400_and_nothing_deleted(self):
        with self.assertLogs("akk9d8.ctrl", level="WARNING") as cm:
            status, data = self._call("Stocks", json={"confirm": "Mutual_Funds"})
        self.assertEqual(status, 400)
        self.assertEqual(data["error_code"], "DELETE_ALL_CONFIRM_MISMATCH")
        self.ctrl.InvestmentService.deleteAll.assert_not_called()
        self.assertIn("REJECTED", "\n".join(cm.output))

    # ak-b8em: the transition is over; a missing confirm is REJECTED.
    def test_missing_confirm_rejected_nothing_deleted(self):
        for kw in ({}, {"json": {}}, {"json": {"other": "EPF"}}):
            self.ctrl.InvestmentService.deleteAll.reset_mock()
            with self.assertLogs("akk9d8.ctrl", level="WARNING") as cm:
                status, data = self._call("EPF", **kw)
            self.assertEqual(status, 400, kw)
            self.assertEqual(data["error_code"], "DELETE_ALL_CONFIRM_REQUIRED", kw)
            self.ctrl.InvestmentService.deleteAll.assert_not_called()
            self.assertIn("REJECTED", "\n".join(cm.output))

    # ak-b8em: a present-but-unparsable body is a MISMATCH, not MISSING.
    def test_unparsable_body_is_mismatch(self):
        cases = [
            {"data": "not json", "content_type": "application/json"},
            {"data": '{"confirm": "Gold"', "content_type": "application/json"},  # truncated
            {"data": '{"confirm": "Gold"}', "content_type": "text/plain"},       # valid JSON, wrong type
            {"data": "confirm=Gold", "content_type": "application/x-www-form-urlencoded"},
            {"data": "   ", "content_type": "application/json"},
        ]
        for kw in cases:
            self.ctrl.InvestmentService.deleteAll.reset_mock()
            with self.assertLogs("akk9d8.ctrl", level="WARNING"):
                status, data = self._call("Gold", **kw)
            self.assertEqual(status, 400, kw)
            self.assertEqual(data["error_code"], "DELETE_ALL_CONFIRM_MISMATCH", kw)
            self.ctrl.InvestmentService.deleteAll.assert_not_called()

    def test_frontend_wire_shape_still_deletes(self):
        # Exactly what the FE sends (axios DELETE, data {confirm}).
        status, _ = self._call("Mutual_Funds", data='{"confirm":"Mutual_Funds"}',
                               content_type="application/json;charset=utf-8")
        self.assertEqual(status, 200)
        self.ctrl.InvestmentService.deleteAll.assert_called_once()

    def test_non_object_body_rejected(self):
        status, _ = self._call("PF", json=["PF"])
        self.assertEqual(status, 400)
        self.ctrl.InvestmentService.deleteAll.assert_not_called()

    def test_invalid_service_type_never_deletes(self):
        # A confirm that echoes an invalid serviceType must not delete.
        # NOTE (pre-existing, out of ak-k9d8 scope, flagged): invalid
        # serviceType currently yields 500, not 400, because
        # getUserIdServiceType returns its error as a (response, status)
        # TUPLE and callers test `not isinstance(params, tuple)`. Pin only
        # "rejected + nothing deleted" so the follow-up fix doesn't break
        # this test.
        status, _ = self._call("Bogus", json={"confirm": "Bogus"})
        self.assertGreaterEqual(status, 400)
        self.ctrl.InvestmentService.deleteAll.assert_not_called()


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
