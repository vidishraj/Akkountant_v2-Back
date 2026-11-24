"""ak-1xu (audit LLM-1 remainder): update_customer is confirm-gated with a
token bound to (tool + args + user), exactly like update_invoice."""
from __future__ import annotations

import asyncio
import importlib.util
import os
import re
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import confirm_preview as P  # noqa: E402
from utils import confirm_tokens as T  # noqa: E402

SECRET_ENV = {"AK_CONFIRM_SECRET": "ak1xu-test-secret"}
ARGS = {"customer_id": "550e8400-e29b-41d4-a716-446655440000",
        "data": {"name": "Acme Pvt Ltd", "address": "1 Main St"}}


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


class TestSourceAndPreview(unittest.TestCase):
    def test_in_destructive_tools(self):
        with open(os.path.join(HERE, "services", "agentService.py")) as f:
            src = f.read()
        m = re.search(r"DESTRUCTIVE_TOOLS\s*=\s*\{([\s\S]*?)\}", src)
        members = {x.strip().strip('"') for x in m.group(1).split(",") if x.strip()}
        self.assertIn("update_customer", members)
        self.assertIn("update_invoice", members)

    def test_preview(self):
        p = P.build_preview("update_customer", ARGS)
        self.assertEqual((p["op"], p["op_label"]), ("update", "Update customer"))
        self.assertEqual(p["fields"], [
            {"key": "customer_id", "label": "Customer", "value": ARGS["customer_id"]},
            {"key": "changes", "label": "Changes", "value": ["address", "name"]},
        ])
        self.assertEqual(P.build_outcome("t", "update_customer", "recorded")["message"], "Updated.")


class _Tool:
    def __init__(self, name, description, input_schema, handler):
        self.name, self.handler = name, handler


class TestGateBehaviour(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.A = _load("services/agentService.py", "_ak1xu_agentService")
        except Exception as e:  # pragma: no cover
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
        self.exec = mock.MagicMock(return_value={"Message": "updated"})
        self._p = [mock.patch.object(self.A, "SdkMcpTool", _Tool),
                   mock.patch.object(self.A, "execute_tool", self.exec)]
        for p in self._p:
            p.start()

    def tearDown(self):
        for p in self._p:
            p.stop()
        T._reset_consumed_for_tests()
        self._env.stop()

    def _call(self, args, confirmed_calls=()):
        events = []
        state = {"matched": {}, "done": set(), "executed": set(),
                 "pending_tools": {t for t in (T.peek_confirm_tool(c, "u1")
                                               for c in confirmed_calls) if t}}
        tools = self.svc._build_sdk_tools(
            "freelance", [{"name": "update_customer", "description": "", "input_schema": {}}],
            "u1", events, [], [], web_fetch_state={"used": False},
            confirmed_calls=list(confirmed_calls), confirm_state=state)
        return asyncio.run(tools[0].handler(args)), events

    def test_unconfirmed_call_is_held(self):
        res, events = self._call(ARGS)
        self.assertTrue(res["is_error"])
        self.exec.assert_not_called()
        (etype, confirm), = events
        self.assertEqual(etype, "confirm")
        self.assertEqual(confirm["message"], f"Update customer (ID: {ARGS['customer_id']})?")
        self.assertEqual(confirm["preview"], P.build_preview("update_customer", ARGS))
        self.assertIn("confirm_id", confirm)

    def test_confirmed_same_args_runs_once(self):
        _, events = self._call(ARGS)
        tok = events[0][1]["confirm_id"]
        _, events = self._call(ARGS, [tok])
        self.exec.assert_called_once()
        self.assertEqual(self.exec.call_args[0][2], ARGS)
        self.assertEqual([d for t, d in events if t == "outcome"],
                         [P.build_outcome(tok, "update_customer", "recorded")])

    def test_confirmed_but_changed_args_does_not_write(self):
        _, events = self._call(ARGS)
        tok = events[0][1]["confirm_id"]
        changed = {"customer_id": ARGS["customer_id"], "data": {"name": "Evil Corp"}}
        res, events = self._call(changed, [tok])
        self.assertTrue(res["is_error"])
        self.exec.assert_not_called()
        self.assertEqual(events[0][0], "confirm")


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
