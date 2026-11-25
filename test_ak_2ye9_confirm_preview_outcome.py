"""ak-2ye9: structured confirm preview + final outcome event for
assistant write proposals.

Pinned:
  * preview values are exactly the token-bound args (no rewrite); NAV is
    the only computed field, Decimal amount/units at 4 dp, flagged derived
  * Apply with the same args -> tool runs once -> outcome recorded
  * Apply turn where the bound call never ran -> token revoked +
    not_executed (cannot be applied later)
  * Cancel -> token revoked + outcome cancelled; it can't be applied
  * stale tokens from earlier turns emit nothing
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import confirm_preview as P  # noqa: E402
from utils import confirm_tokens as T  # noqa: E402

SECRET_ENV = {"AK_CONFIRM_SECRET": "ak2ye9-test-secret"}
MF_ARGS = {"service_type": "Mutual_Funds",
           "data": {"schemeCode": "120503", "date": "20-11-2026",
                    "quantity": 12.345, "amount": 1000}}


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestPreview(unittest.TestCase):
    def test_mf_insert_fields_and_order(self):
        p = P.build_preview("insert_investment", MF_ARGS)
        self.assertEqual((p["op"], p["op_label"]), ("insert", "Record purchase"))
        self.assertEqual([f["key"] for f in p["fields"]],
                         ["asset", "scheme", "units", "nav", "amount", "date"])
        by = {f["key"]: f for f in p["fields"]}
        self.assertEqual(by["asset"]["value"], "Mutual_Funds")
        self.assertEqual(by["scheme"]["value"], "120503")
        self.assertEqual(by["units"]["value"], 12.345)
        self.assertEqual(by["amount"]["value"], 1000)
        # Parsed exactly as the insert stores it (review minor).
        self.assertEqual(by["date"]["value"], "2026-11-20")
        # The stored buyPrice, computed like the write, unrounded.
        self.assertEqual(by["nav"], {"key": "nav", "label": "NAV",
                                     "value": float(1000) / float(12.345), "derived": True})
        self.assertNotIn("derived", by["amount"])

    def test_values_are_the_args_verbatim(self):
        p = P.build_preview("insert_investment", MF_ARGS)
        src = {"asset": MF_ARGS["service_type"], "scheme": "120503",
               "units": 12.345, "amount": 1000}
        for f in p["fields"]:
            if not f.get("derived") and f["key"] != "date":
                self.assertIs(f["value"], src[f["key"]] if f["key"] != "units"
                              else MF_ARGS["data"]["quantity"])

    def test_dates_parsed_like_the_write(self):
        self.assertEqual(P.parse_write_date("Mutual_Funds", "20-11-2026"), "2026-11-20")
        self.assertEqual(P.parse_write_date("EPF", "11/2026"), "2026-11-01")
        self.assertEqual(P.parse_write_date("EPF", "05/11/2026"), "2026-11-05")
        self.assertIsNone(P.parse_write_date("NPS", "not a date"))
        bad = P.build_preview("insert_investment", {
            "service_type": "NPS",
            "data": {"schemeCode": "x", "date": "not a date", "quantity": 1, "amount": 1}})
        d = {f["key"]: f for f in bad["fields"]}["date"]
        self.assertEqual((d["value"], d.get("unparsed")), ("not a date", True))

    def test_nav_rules(self):
        self.assertEqual(P.derive_nav("100", "3"), 100.0 / 3.0)
        self.assertEqual(P.derive_nav(2, 3), 2.0 / 3.0)
        for a, u in ((100, 0), (100, -1), (0, 5), (True, 1), (100, None), ("x", 1),
                     (float("nan"), 1), (float("inf"), 1)):
            self.assertIsNone(P.derive_nav(a, u), (a, u))
        gold = P.build_preview("insert_investment", {
            "service_type": "Gold",
            "data": {"date": "01-11-2026", "amount": 6000, "quantity": 1, "goldType": "22",
                     "description": "coin"}})
        keys = [f["key"] for f in gold["fields"]]
        self.assertNotIn("nav", keys)
        self.assertEqual({f["key"]: f["label"] for f in gold["fields"]}["units"], "Grams")

    def test_epf_and_deletes_and_update(self):
        epf = P.build_preview("insert_investment", {
            "service_type": "EPF",
            "data": {"date": "01-11-2026", "description": "Nov",
                     "employee_amount": 1800, "employer_amount": 550}})
        self.assertEqual([f["key"] for f in epf["fields"]],
                         ["asset", "description", "employee_amount", "employer_amount", "date"])
        d = P.build_preview("delete_single_investment", {"service_type": "Stocks", "buy_id": "b-9"})
        self.assertEqual((d["op"], [(f["key"], f["value"]) for f in d["fields"]]),
                         ("delete", [("asset", "Stocks"), ("buy_id", "b-9")]))
        self.assertEqual(P.build_preview("delete_all_investments", {"service_type": "NPS"})["op"],
                         "delete_all")
        u = P.build_preview("update_invoice", {"invoice_number": "INV-7",
                                               "data": {"status": "paid", "amount": 5}})
        self.assertEqual(u["fields"], [
            {"key": "invoice_number", "label": "Invoice", "value": "INV-7"},
            {"key": "changes", "label": "Changes", "value": ["amount", "status"]}])
        self.assertEqual(P.build_preview("sync_kite_holdings", {})["fields"], [])
        self.assertEqual(P.build_preview("whatever", None)["fields"], [])

    def test_outcome_shape(self):
        o = P.build_outcome("tok", "insert_investment", "recorded")
        self.assertEqual(o, {"confirm_id": "tok", "tool": "insert_investment",
                             "status": "recorded", "recorded": True, "message": "Recorded."})
        for st in ("failed", "not_executed", "cancelled"):
            self.assertFalse(P.build_outcome("t", "delete_file", st)["recorded"])
        self.assertEqual(P.build_outcome("t", "x", "cancelled")["message"],
                         "Cancelled — nothing recorded.")
        with self.assertRaises(ValueError):
            P.build_outcome("t", "x", "maybe")
        self.assertEqual(P.build_outcome("t", "insert_investment", "failed", interrupted=True)["message"],
                         "Interrupted — check your holdings before retrying.")


class TestRevoke(unittest.TestCase):
    def setUp(self):
        self._env = mock.patch.dict(os.environ, SECRET_ENV)
        self._env.start()
        T._reset_consumed_for_tests()

    def tearDown(self):
        T._reset_consumed_for_tests()
        self._env.stop()

    def test_revoked_token_cannot_be_applied(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        self.assertEqual(T.revoke_confirm_token(tok, "u1"), "delete_file")
        self.assertFalse(T.verify_confirm_token(tok, "delete_file", {"file_id": "f"}, "u1"))
        self.assertIsNone(T.revoke_confirm_token(tok, "u1"))

    def test_rejects_dead_foreign_forged(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        self.assertIsNone(T.revoke_confirm_token(tok, "u2"))
        self.assertIsNone(T.revoke_confirm_token(tok[:-1] + ("0" if tok[-1] != "0" else "1"), "u1"))
        for junk in ("", "abc", None, 5, "a.b"):
            self.assertIsNone(T.revoke_confirm_token(junk, "u1"))
        # consumed by a real apply -> nothing to revoke
        self.assertTrue(T.verify_confirm_token(tok, "delete_file", {"file_id": "f"}, "u1"))
        self.assertIsNone(T.revoke_confirm_token(tok, "u1"))
        # expired
        old = T.issue_confirm_token("delete_file", {}, "u1", ttl_s=1)
        with mock.patch.object(T.time, "time", return_value=time.time() + 5):
            self.assertIsNone(T.revoke_confirm_token(old, "u1"))

    def test_peek_does_not_consume(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        self.assertEqual(T.peek_confirm_tool(tok, "u1"), "delete_file")
        self.assertEqual(T.peek_confirm_tool(tok, "u1"), "delete_file")
        self.assertIsNone(T.peek_confirm_tool(tok, "u2"))
        self.assertTrue(T.verify_confirm_token(tok, "delete_file", {"file_id": "f"}, "u1"))
        self.assertIsNone(T.peek_confirm_tool(tok, "u1"))           # consumed now

    def test_no_secret(self):
        tok = T.issue_confirm_token("delete_file", {}, "u1")
        with mock.patch.dict(os.environ, {"AK_CONFIRM_SECRET": ""}):
            self.assertIsNone(T.revoke_confirm_token(tok, "u1"))


class _Tool:
    def __init__(self, name, description, input_schema, handler):
        self.name, self.handler = name, handler


class TestHandlerAndCloseOut(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.A = _load("services/agentService.py", "_ak2ye9_agentService")
        except Exception as e:  # pragma: no cover - env without deps
            raise unittest.SkipTest(f"agentService import failed: {e}")

    def setUp(self):
        self._env = mock.patch.dict(os.environ, SECRET_ENV)
        self._env.start()
        T._reset_consumed_for_tests()
        self.svc = self.A.AgentService.__new__(self.A.AgentService)
        self.svc.logger = mock.MagicMock()
        for a in ("investment_service", "transaction_service", "invoice_service",
                  "customer_service", "dashboard_service", "mail_processor",
                  "file_attachment_service"):
            setattr(self.svc, a, mock.MagicMock())
        self.exec = mock.MagicMock(return_value={"result": "ok"})
        self._p = [mock.patch.object(self.A, "SdkMcpTool", _Tool),
                   mock.patch.object(self.A, "execute_tool", self.exec)]
        for p in self._p:
            p.start()

    def tearDown(self):
        for p in self._p:
            p.stop()
        T._reset_consumed_for_tests()
        self._env.stop()

    def _handler(self, tool, confirmed_calls=(), web_used=False):
        # Mirrors stream_chat's confirm_state set-up (pending = live tokens).
        events = []
        state = {"matched": {}, "done": set(), "executed": set(),
                 "pending_tools": {t for t in (T.peek_confirm_tool(c, "u1")
                                               for c in confirmed_calls) if t}}
        tools = self.svc._build_sdk_tools(
            "investment", [{"name": tool, "description": "", "input_schema": {}}],
            "u1", events, [], [], web_fetch_state={"used": web_used},
            confirmed_calls=list(confirmed_calls), confirm_state=state)
        h = tools[0].handler
        return (lambda args: asyncio.run(h(args))), events, state

    def test_propose_then_apply_records_exactly_previewed_args(self):
        call, events, _ = self._handler("insert_investment", web_used=True)
        r = call(MF_ARGS)
        self.assertTrue(r["is_error"])
        self.exec.assert_not_called()
        (etype, confirm), = events
        self.assertEqual(etype, "confirm")
        self.assertEqual(confirm["preview"], P.build_preview("insert_investment", MF_ARGS))
        self.assertIs(confirm["input"], MF_ARGS)
        tok = confirm["confirm_id"]

        call, events, state = self._handler("insert_investment", [tok], web_used=True)
        call(MF_ARGS)
        self.exec.assert_called_once()
        self.assertEqual(self.exec.call_args[0][2], MF_ARGS)       # written == previewed
        outcome = [d for t, d in events if t == "outcome"]
        self.assertEqual(outcome, [P.build_outcome(tok, "insert_investment", "recorded")])
        self.assertEqual(self.A.AgentService._close_out_confirms([tok], state, "u1"), [])

    def test_apply_with_changed_args_does_not_write_and_closes_not_executed(self):
        call, events, _ = self._handler("delete_single_investment")
        call({"service_type": "Stocks", "buy_id": "b1"})
        tok = events[0][1]["confirm_id"]
        self.assertEqual(events[0][1]["preview"]["fields"][1], {"key": "buy_id", "label": "Buy ID", "value": "b1"})

        call, events, state = self._handler("delete_single_investment", [tok])
        call({"service_type": "Stocks", "buy_id": "b2"})          # model changed the args
        self.exec.assert_not_called()
        self.assertEqual(events[0][0], "confirm")                  # fresh proposal instead
        closing = self.A.AgentService._close_out_confirms([tok], state, "u1")
        self.assertEqual(closing, [("outcome", P.build_outcome(
            tok, "delete_single_investment", "not_executed", revoked=True))])
        # now dead: the original args can no longer be applied either
        call, events, _ = self._handler("delete_single_investment", [tok])
        call({"service_type": "Stocks", "buy_id": "b1"})
        self.exec.assert_not_called()

    def test_tool_error_reports_failed(self):
        self.exec.return_value = {self.A.TOOL_ERROR_KEY: True, "message": "bad"}
        tok = T.issue_confirm_token("delete_file", {"file_id": "f1"}, "u1")
        call, events, state = self._handler("delete_file", [tok])
        call({"file_id": "f1"})
        self.assertEqual([d for t, d in events if t == "outcome"],
                         [P.build_outcome(tok, "delete_file", "failed")])
        self.assertEqual(self.A.AgentService._close_out_confirms([tok], state, "u1"), [])

    def test_matched_without_outcome_closes_interrupted(self):
        state = {"matched": {"tokX": "delete_file"}, "done": set(), "executed": set()}
        self.assertEqual(self.A.AgentService._close_out_confirms(["tokX"], state, "u1"),
                         [("outcome", P.build_outcome("tokX", "delete_file", "failed",
                                                      interrupted=True))])

    # ── review CRITICAL: Apply turn without web state ────────────────
    def _propose_insert(self):
        call, events, _ = self._handler("insert_investment", web_used=True)
        call(MF_ARGS)
        return events[0][1]["confirm_id"]

    def test_apply_without_web_and_changed_args_is_rejected(self):
        tok = self._propose_insert()
        changed = {"service_type": "Mutual_Funds",
                   "data": dict(MF_ARGS["data"], amount=999999)}
        call, events, state = self._handler("insert_investment", [tok], web_used=False)
        r = call(changed)
        self.assertTrue(r["is_error"])
        self.exec.assert_not_called()                              # 999999 NOT written
        (etype, confirm), = events
        self.assertEqual((etype, confirm["reason"]), ("confirm", "args_changed_since_confirm"))
        self.assertEqual(confirm["preview"], P.build_preview("insert_investment", changed))
        self.assertEqual(self.A.AgentService._close_out_confirms([tok], state, "u1"),
                         [("outcome", P.build_outcome(tok, "insert_investment", "not_executed",
                                                      revoked=True))])

    def test_apply_without_web_and_same_args_records(self):
        tok = self._propose_insert()
        call, events, _ = self._handler("insert_investment", [tok], web_used=False)
        call(MF_ARGS)
        self.exec.assert_called_once()
        self.assertEqual([d for t, d in events if t == "outcome"],
                         [P.build_outcome(tok, "insert_investment", "recorded")])

    def test_insert_without_pending_token_unchanged(self):
        call, events, _ = self._handler("insert_investment", web_used=False)
        call(MF_ARGS)                                             # ordinary, ungated insert
        self.exec.assert_called_once()
        self.assertEqual(events, [("tool_exec", events[0][1])])

    def test_close_out_never_says_not_executed_if_tool_ran(self):
        tok = T.issue_confirm_token("insert_investment", MF_ARGS, "u1")
        state = {"matched": {}, "done": set(), "executed": {"insert_investment"}}
        self.assertEqual(self.A.AgentService._close_out_confirms([tok], state, "u1"), [])
        self.assertIsNone(T.peek_confirm_tool(tok, "u1"))          # still revoked

    # ── review MAJOR 1: service failure shapes ────────────────────────
    def test_service_error_status_reports_failed(self):
        self.exec.return_value = {"Error": "Error in MF entry", "_http_status": 406}
        tok = self._propose_insert()
        call, events, _ = self._handler("insert_investment", [tok], web_used=True)
        call(MF_ARGS)
        self.assertEqual([d for t, d in events if t == "outcome"],
                         [P.build_outcome(tok, "insert_investment", "failed")])

    def test_cancel_revokes_and_reports(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f1"}, "u1")
        events, revoked = self.A.AgentService._revoke_cancelled([tok, "junk"], "u1")
        self.assertEqual(revoked, {tok})
        self.assertEqual(events, [("outcome", P.build_outcome(tok, "delete_file", "cancelled",
                                                              revoked=True))])
        call, _, _ = self._handler("delete_file", [tok])
        call({"file_id": "f1"})
        self.exec.assert_not_called()
        # second cancel of the same id: nothing left to report
        self.assertEqual(self.A.AgentService._revoke_cancelled([tok], "u1"), ([], set()))

    def test_stale_tokens_from_earlier_turns_emit_nothing(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f1"}, "u1")
        call, _, _ = self._handler("delete_file", [tok])
        call({"file_id": "f1"})                                    # applied last turn
        fresh = {"matched": {}, "done": set()}
        self.assertEqual(self.A.AgentService._close_out_confirms([tok], fresh, "u1"), [])

    def test_legacy_name_list_emits_no_outcome(self):
        events, state = [], {"matched": {}, "done": set()}
        tools = self.svc._build_sdk_tools(
            "investment", [{"name": "delete_file", "description": "", "input_schema": {}}],
            "u1", events, [], ["delete_file"], web_fetch_state={"used": False},
            confirmed_calls=[], confirm_state=state)
        asyncio.run(tools[0].handler({"file_id": "f1"}))
        self.exec.assert_called_once()
        self.assertEqual([t for t, _ in events if t == "outcome"], [])


class TestUnwrapKeepsErrorStatus(unittest.TestCase):
    def test_status_kept_and_detected(self):
        try:
            import flask
            X = _load("services/agent_tool_executor.py", "_ak2ye9_executor")
        except Exception as e:  # pragma: no cover
            self.skipTest(f"executor import failed: {e}")
        app = flask.Flask("ak2ye9-unwrap")
        with app.app_context():
            bad = X._unwrap_response((flask.jsonify({"Error": "Error in MF entry"}), 406))
            ok = X._unwrap_response((flask.jsonify({"Message": "inserted"}), 200))
            bare = X._unwrap_response(flask.make_response(flask.jsonify({"x": 1}), 500))
        self.assertEqual(bad, {"Error": "Error in MF entry", "_http_status": 406})
        self.assertEqual(ok, {"Message": "inserted"})
        self.assertEqual(bare["_http_status"], 500)
        self.assertTrue(X.result_failed(bad))
        self.assertTrue(X.result_failed({"_http_status": 409}))
        self.assertTrue(X.result_failed({"error": "x"}))
        self.assertTrue(X.result_failed({X.TOOL_ERROR_KEY: True}))
        self.assertFalse(X.result_failed(ok))
        self.assertFalse(X.result_failed({"result": "success"}))


class TestWiring(unittest.TestCase):
    def test_stream_chat_and_controller_wired(self):
        with open(os.path.join(HERE, "services", "agentService.py")) as f:
            svc = f.read()
        with open(os.path.join(HERE, "controllers", "agentEP.py")) as f:
            ep = f.read()
        self.assertIn("cancelled_calls=None", svc)
        self.assertIn("self._revoke_cancelled(cancelled_calls, user_id)", svc)
        self.assertIn("self._close_out_confirms(confirmed_calls, confirm_state, user_id)", svc)
        # web gate, destructive gate, pending-Apply gate (review CRITICAL)
        self.assertEqual(svc.count('"preview": build_preview(_tn, args)'), 3)
        self.assertIn("cancelled_calls=cancelled_calls,", ep)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
