"""ak-vfa3 (2026-11-24): the agent tool `update_transaction` must only
ever update the agent's own user's transaction.

Before: the executor called service.updateTransaction(ref, updates)
without user_id, and the service only added the user filter when a
user_id was given, so the tool could update ANY user's row by
referenceID.

After: the executor passes user_id=<agent uid>; the service fails closed
without a uid and always filters on Transactions.user.
Synthetic rows only, in-memory SQLite.
"""
from __future__ import annotations

import ast
import os
import sys
import unittest
from datetime import datetime
from decimal import Decimal

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)


def _lift(rel_path, func_name, class_name=None, ns=None):
    with open(os.path.join(HERE, rel_path)) as fh:
        tree = ast.parse(fh.read())
    scope = tree.body
    if class_name:
        scope = next(n for n in tree.body
                     if isinstance(n, ast.ClassDef) and n.name == class_name).body
    fn = next(n for n in scope
              if isinstance(n, ast.FunctionDef) and n.name == func_name)
    fn.decorator_list = []
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)
    ns = dict(ns or {})
    exec(compile(mod, f"<lift:{func_name}>", "exec"), ns)
    return ns[func_name]


try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    import models  # noqa: F401
    from models.investmentHistory import InvestmentHistory  # noqa: F401 (mapper config)
    from models.transactions import Transactions, ProcessingMethod
    _DEPS_OK, _SKIP = True, ""
except Exception as _exc:  # pragma: no cover
    _DEPS_OK, _SKIP = False, f"model import failed: {_exc}"


class _Svc:
    def __init__(self, factory):
        self._factory = factory
        self._s = None

    @property
    def db(self):
        return self

    @property
    def session(self):
        if self._s is None:
            self._s = self._factory()
        return self._s


@unittest.skipUnless(_DEPS_OK, _SKIP)
class TestServiceScoping(unittest.TestCase):

    def setUp(self):
        engine = create_engine("sqlite:///:memory:")
        Transactions.__table__.create(engine)
        self.factory = scoped_session(sessionmaker(bind=engine))
        self.svc = _Svc(self.factory)
        self.update = _lift("services/transactionsService.py", "updateTransaction",
                            "TransactionService", {"Transactions": Transactions})
        for ref, user in (("ref-mine", "me"), ("ref-theirs", "other")):
            self.svc.session.add(Transactions(
                referenceID=ref, user=user, date=datetime(2026, 11, 1),
                details="SYNTHETIC MEMO", tag="", amount=Decimal("10.00"),
                bank="HDFC", source="Statement",
                processed_via=ProcessingMethod.CLAUDE_CODE,
            ))
        self.svc.session.commit()

    def tearDown(self):
        self.factory.remove()

    def _row(self, ref):
        self.svc.session.expire_all()
        return self.svc.session.query(Transactions).filter_by(referenceID=ref).one()

    def test_own_row_updated(self):
        res = self.update(self.svc, "ref-mine", {"tag": "Food"}, user_id="me")
        self.assertNotIn("error", res)
        self.assertEqual(self._row("ref-mine").tag, "Food")

    def test_other_users_row_untouched(self):
        res = self.update(self.svc, "ref-theirs", {"tag": "Hacked", "amount": 1},
                          user_id="me")
        self.assertIn("error", res)
        row = self._row("ref-theirs")
        self.assertEqual(row.tag, "")
        self.assertEqual(row.amount, Decimal("10.00"))

    def test_missing_uid_fails_closed(self):
        for uid in (None, ""):
            res = self.update(self.svc, "ref-theirs", {"tag": "Hacked"}, user_id=uid)
            self.assertIn("error", res)
            res = self.update(self.svc, "ref-mine", {"tag": "Hacked"}, user_id=uid)
            self.assertIn("error", res)
        self.assertEqual(self._row("ref-theirs").tag, "")
        self.assertEqual(self._row("ref-mine").tag, "")
        # Positional-only legacy call shape is refused too.
        self.assertIn("error", self.update(self.svc, "ref-theirs", {"tag": "x"}))


class TestExecutorPassesUid(unittest.TestCase):

    def setUp(self):
        self.exec_tool = _lift("services/agent_tool_executor.py",
                               "_execute_transaction_tool")

    def test_update_transaction_forwards_agent_uid(self):
        calls = []

        class FakeService:
            def updateTransaction(self, *a, **k):
                calls.append((a, k))
                return {"message": "ok"}

        self.exec_tool("update_transaction",
                       {"reference_id": "ref-x", "updates": {"tag": "T"}},
                       "agent-uid", FakeService(), None)
        self.assertEqual(len(calls), 1)
        args, kwargs = calls[0]
        self.assertEqual(args[:2], ("ref-x", {"tag": "T"}))
        self.assertEqual(kwargs.get("user_id"), "agent-uid")


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
