"""ak-erkm: confirm tokens are single-use ACROSS gunicorn workers.

The consumed / revoked state lives in the confirmTokenStates table (one
atomic INSERT on a unique nonce), not in each worker's memory. Pinned:
  * a token used in one "worker" is refused in another (memory reset
    between them, so only the DB can be remembering)
  * two real processes racing on the same token: exactly ONE succeeds
  * revoke is visible cross-worker; peek doesn't consume
  * fail-closed: DB error -> verify/revoke refuse, peek still reports
    the token as pending (so the bound-args gate keeps applying)
  * rows hold no args / secrets; expired rows are pruned
"""
from __future__ import annotations

import multiprocessing
import os
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

SECRET_ENV = {"AK_CONFIRM_SECRET": "akerkm-test-secret"}
ARGS = {"service_type": "Mutual_Funds",
        "data": {"schemeCode": "120503", "date": "20-11-2026", "quantity": 1, "amount": 10}}

try:
    import flask
    import sqlalchemy as sa
    from flask_sqlalchemy import SQLAlchemy
    import models
    from models.confirmTokenState import ConfirmTokenState
    from utils import confirm_tokens as T
    from utils import confirm_token_store as S
    _OK, _WHY = True, ""
except Exception as e:  # pragma: no cover - env without deps
    _OK, _WHY = False, f"deps unavailable: {e}"


def _make_app(db_path):
    app = flask.Flask("akerkm")
    app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{db_path}"
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"connect_args": {"timeout": 30}}
    db = SQLAlchemy(app, model_class=models.Base)
    with app.app_context():
        ConfirmTokenState.__table__.create(db.engine, checkfirst=True)
    return app


def _worker_verify(db_path, token, start_at, q):
    # A separate process == a separate gunicorn worker (own memory).
    os.environ.update(SECRET_ENV)
    app = _make_app(db_path)
    T._reset_consumed_for_tests()
    while time.time() < start_at:
        time.sleep(0.001)
    with app.app_context():
        q.put(T.verify_confirm_token(token, "insert_investment", ARGS, "u1"))


@unittest.skipUnless(_OK, _WHY)
class TestDbStore(unittest.TestCase):
    def setUp(self):
        self.eng = sa.create_engine("sqlite://", poolclass=sa.pool.StaticPool,
                                    connect_args={"check_same_thread": False})
        ConfirmTokenState.__table__.create(self.eng)
        self.store = S.DbTokenStore(self.eng)
        S._reset_prune_clock_for_tests()

    def test_mark_is_atomic_single_use(self):
        exp = int(time.time()) + 300
        self.assertIs(self.store.is_used("n1"), False)
        self.assertTrue(self.store.mark("n1", "u1", "delete_file", exp, "consumed"))
        self.assertFalse(self.store.mark("n1", "u1", "delete_file", exp, "consumed"))
        self.assertFalse(self.store.mark("n1", "u1", "delete_file", exp, "revoked"))
        self.assertIs(self.store.is_used("n1"), True)

    def test_row_holds_no_args_or_secret(self):
        self.assertEqual(set(ConfirmTokenState.__table__.columns.keys()),
                         {"nonce", "uid", "tool", "state", "expires_at", "created_at"})

    def test_prune_removes_only_expired(self):
        now = int(time.time())
        S._last_prune = time.time()            # hold the periodic prune off
        self.store.mark("old", "u1", "t", now - 10, "consumed")
        self.store.mark("new", "u1", "t", now + 300, "consumed")
        self.assertEqual(self.store.prune(now), 1)
        self.assertIs(self.store.is_used("old"), False)
        self.assertIs(self.store.is_used("new"), True)

    def test_db_errors_fail_closed(self):
        broken = S.DbTokenStore(sa.create_engine("sqlite:////nonexistent-dir/x.db"))
        self.assertFalse(broken.mark("n", "u1", "t", int(time.time()) + 60, "consumed"))
        self.assertIsNone(broken.is_used("n"))


@unittest.skipUnless(_OK, _WHY)
class TestCrossWorker(unittest.TestCase):
    def setUp(self):
        self._env = mock.patch.dict(os.environ, SECRET_ENV)
        self._env.start()
        self.dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.dir, "tokens.db")
        self.app = _make_app(self.db_path)
        T._reset_consumed_for_tests()

    def tearDown(self):
        T._reset_consumed_for_tests()
        self._env.stop()

    def _as_other_worker(self, fn):
        T._reset_consumed_for_tests()          # a fresh worker's memory
        with self.app.app_context():
            return fn()

    def test_replay_on_another_worker_is_refused(self):
        tok = T.issue_confirm_token("insert_investment", ARGS, "u1")
        ok = self._as_other_worker(lambda: T.verify_confirm_token(tok, "insert_investment", ARGS, "u1"))
        again = self._as_other_worker(lambda: T.verify_confirm_token(tok, "insert_investment", ARGS, "u1"))
        self.assertEqual((ok, again), (True, False))
        with self.app.app_context():
            row = S.current_store().engine.connect().execute(
                sa.select(ConfirmTokenState.__table__)).mappings().one()
        self.assertEqual((row["uid"], row["tool"], row["state"]), ("u1", "insert_investment", "consumed"))

    def test_revoke_is_visible_everywhere(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        self.assertEqual(self._as_other_worker(lambda: T.peek_confirm_tool(tok, "u1")), "delete_file")
        self.assertEqual(self._as_other_worker(lambda: T.revoke_confirm_token(tok, "u1")), "delete_file")
        self.assertFalse(self._as_other_worker(
            lambda: T.verify_confirm_token(tok, "delete_file", {"file_id": "f"}, "u1")))
        self.assertIsNone(self._as_other_worker(lambda: T.peek_confirm_tool(tok, "u1")))
        self.assertIsNone(self._as_other_worker(lambda: T.revoke_confirm_token(tok, "u1")))

    def test_peek_does_not_consume(self):
        tok = T.issue_confirm_token("delete_file", {"file_id": "f"}, "u1")
        for _ in range(3):
            self.assertEqual(self._as_other_worker(lambda: T.peek_confirm_tool(tok, "u1")), "delete_file")
        self.assertTrue(self._as_other_worker(
            lambda: T.verify_confirm_token(tok, "delete_file", {"file_id": "f"}, "u1")))

    def test_store_unreadable_peek_still_pending_verify_refused(self):
        tok = T.issue_confirm_token("insert_investment", ARGS, "u1")
        with self.app.app_context(), \
                mock.patch.object(S.DbTokenStore, "is_used", return_value=None), \
                mock.patch.object(S.DbTokenStore, "mark", return_value=False):
            self.assertEqual(T.peek_confirm_tool(tok, "u1"), "insert_investment")
            self.assertFalse(T.verify_confirm_token(tok, "insert_investment", ARGS, "u1"))
            self.assertIsNone(T.revoke_confirm_token(tok, "u1"))

    def test_two_processes_race_exactly_one_wins(self):
        try:
            ctx = multiprocessing.get_context("fork")
        except ValueError:  # pragma: no cover
            self.skipTest("fork start method unavailable")
        for _ in range(3):
            tok = T.issue_confirm_token("insert_investment", ARGS, "u1")
            q = ctx.Queue()
            start_at = time.time() + 1.0
            procs = [ctx.Process(target=_worker_verify, args=(self.db_path, tok, start_at, q))
                     for _ in range(2)]
            for p in procs:
                p.start()
            results = [q.get(timeout=30) for _ in procs]
            for p in procs:
                p.join(timeout=30)
            self.assertEqual(sorted(results), [False, True], results)

    def test_no_app_context_uses_in_process_fallback(self):
        tok = T.issue_confirm_token("delete_file", {}, "u1")
        self.assertIsNone(S.current_store())
        self.assertTrue(T.verify_confirm_token(tok, "delete_file", {}, "u1"))
        self.assertFalse(T.verify_confirm_token(tok, "delete_file", {}, "u1"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
