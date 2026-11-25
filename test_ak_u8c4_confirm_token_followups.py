"""ak-u8c4: ak-erkm follow-ups.

  (1) WARN once when an app context exists but no DB token store resolves
      (silent fallback to per-worker single-use); flask_sqlalchemy>=3 pinned
  (2) a revoke that hits a store error still yields a TERMINAL outcome
      (cancelled / not_executed) flagged revoked=False, so the FE card
      resolves; verify stays fail-closed meanwhile
  (3) no deprecated datetime.utcfromtimestamp
"""
from __future__ import annotations

import datetime
import importlib.util
import logging
import os
import re
import sys
import time
import unittest
import warnings
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SECRET_ENV = {"AK_CONFIRM_SECRET": "aku8c4-test-secret"}

try:
    import flask
    from utils import confirm_token_store as S
    from utils import confirm_tokens as T
    from utils import confirm_preview as P
    _OK, _WHY = True, ""
except Exception as e:  # pragma: no cover
    _OK, _WHY = False, f"deps unavailable: {e}"


def _load(rel, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@unittest.skipUnless(_OK, _WHY)
class TestWarnOnce(unittest.TestCase):
    def setUp(self):
        S._reset_warning_for_tests()

    def tearDown(self):
        S._reset_warning_for_tests()

    def test_no_extension_warns_exactly_once(self):
        app = flask.Flask("aku8c4-noext")
        with app.app_context():
            with self.assertLogs("utils.confirm_token_store", level="WARNING") as cm:
                self.assertIsNone(S.current_store())
                self.assertIsNone(S.current_store())
        self.assertEqual(len(cm.records), 1)
        self.assertIn("per-worker", cm.records[0].getMessage())

    def test_extension_without_engine_warns(self):
        app = flask.Flask("aku8c4-noengine")
        app.extensions["sqlalchemy"] = object()      # e.g. flask_sqlalchemy < 3
        with app.app_context():
            with self.assertLogs("utils.confirm_token_store", level="WARNING") as cm:
                self.assertIsNone(S.current_store())
        self.assertIn("has no .engine", cm.records[0].getMessage())

    def test_no_app_context_is_silent(self):
        logger = logging.getLogger("utils.confirm_token_store")
        with mock.patch.object(logger, "warning") as w:
            self.assertIsNone(S.current_store())
        w.assert_not_called()

    def test_requirements_pin(self):
        with open(os.path.join(HERE, "requirements.txt")) as f:
            reqs = f.read()
        self.assertRegex(reqs, r"(?m)^flask_sqlalchemy>=3")


@unittest.skipUnless(_OK, _WHY)
class TestUtcConversion(unittest.TestCase):
    def test_naive_utc_and_no_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            got = S._utc(0)
            got2 = S._utc(1_764_000_000)
        self.assertEqual(got, datetime.datetime(1970, 1, 1, 0, 0, 0))
        self.assertIsNone(got.tzinfo)
        self.assertEqual(got2, datetime.datetime(2025, 11, 24, 16, 0, 0))
        self.assertFalse([x for x in w if issubclass(x.category, DeprecationWarning)])
        with open(os.path.join(HERE, "utils", "confirm_token_store.py")) as f:
            self.assertNotIn("utcfromtimestamp(", f.read())


@unittest.skipUnless(_OK, _WHY)
class TestRevokeStoreError(unittest.TestCase):
    def setUp(self):
        self._env = mock.patch.dict(os.environ, SECRET_ENV)
        self._env.start()
        T._reset_consumed_for_tests()

    def tearDown(self):
        T._reset_consumed_for_tests()
        self._env.stop()

    def test_status_variants(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        with mock.patch.object(T, "_mark_used", return_value=None):
            self.assertEqual(T.revoke_confirm_token_status(tok, "u1"), ("delete_file", T.STORE_ERROR))
            self.assertIsNone(T.revoke_confirm_token(tok, "u1"))          # not revoked
            self.assertFalse(T.verify_confirm_token(tok, "delete_file", {"file_id": "f"}, "u1"))
        self.assertEqual(T.revoke_confirm_token_status(tok, "u1"), ("delete_file", T.REVOKED))
        self.assertEqual(T.revoke_confirm_token_status(tok, "u1"), (None, T.DEAD))
        self.assertEqual(T.revoke_confirm_token_status("forged.0", "u1"), (None, T.DEAD))

    def test_outcome_revoked_flag(self):
        ok = P.build_outcome("t", "delete_file", "cancelled", revoked=True)
        bad = P.build_outcome("t", "delete_file", "cancelled", revoked=False)
        self.assertEqual((ok["revoked"], ok["message"]), (True, "Cancelled — nothing recorded."))
        self.assertEqual(bad["revoked"], False)
        self.assertFalse(bad["recorded"])
        self.assertIn("Don't re-send", bad["message"])
        self.assertNotIn("revoked", P.build_outcome("t", "delete_file", "recorded"))


class TestAgentHelpersTerminalOnStoreError(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not _OK:
            raise unittest.SkipTest(_WHY)
        try:
            cls.A = _load("services/agentService.py", "_aku8c4_agentService")
        except Exception as e:  # pragma: no cover
            raise unittest.SkipTest(f"agentService import failed: {e}")

    def setUp(self):
        self._env = mock.patch.dict(os.environ, SECRET_ENV)
        self._env.start()
        T._reset_consumed_for_tests()

    def tearDown(self):
        T._reset_consumed_for_tests()
        self._env.stop()

    def _store_error(self):
        return mock.patch.object(self.A, "revoke_confirm_token_status",
                                 side_effect=lambda tok, uid: (
                                     T.peek_confirm_tool(tok, uid), T.STORE_ERROR))

    def test_cancel_resolves_with_revoked_false(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        with self._store_error():
            events, excluded = self.A.AgentService._revoke_cancelled([tok, "junk.0"], "u1")
        self.assertEqual(excluded, {tok})                    # kept out of this turn
        self.assertEqual(events, [("outcome", P.build_outcome(
            tok, "delete_file", "cancelled", revoked=False))])

    def test_close_out_resolves_with_revoked_false(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        state = {"matched": {}, "done": set(), "executed": set()}
        with self._store_error():
            events = self.A.AgentService._close_out_confirms([tok], state, "u1")
        self.assertEqual(events, [("outcome", P.build_outcome(
            tok, "delete_file", "not_executed", revoked=False))])

    def test_normal_path_flags_revoked_true(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        events, _ = self.A.AgentService._revoke_cancelled([tok], "u1")
        self.assertTrue(events[0][1]["revoked"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
