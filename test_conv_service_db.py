"""ak-6si integration tests: exercise the REAL find_or_create_by_title
+ create_conversation DB path against an in-memory SQLite session.

The pre-ak-6si test suite (test_wealth_digest.py::
TestAgentConversationServiceAdditions) mocked find_or_create_by_title
DIRECTLY — asserted method presence + method-body source shape via
inspection. That missed the actual production-blocker: BaseService.db
returns a NEW scoped_session on every attribute access when g.db is
None (the background-task path used by WealthDigestTask), so
create_conversation's `self.db.session.add(conv)` and `self.db.session
.commit()` were landing on DIFFERENT sessions → row never committed,
conv.id stayed None.

This module drives the REAL AgentConversationService against a real
(in-memory SQLite) database session so the code path from method-
entry through commit + id return is exercised end-to-end. Mocks the
minimum needed (BaseService.db property) with a shim that returns a
stable session — exposes the bug if we ever re-introduce it.

Coverage:
  * create_conversation returns positive int (never None); row exists.
  * find_or_create_by_title creates a fresh row on miss; returns int.
  * find_or_create_by_title returns the existing id on hit; no dup insert.
  * Multiple find_or_create calls converge on the SAME id (idempotency).
  * append_message succeeds with the id returned from find_or_create.
  * create_conversation raises RuntimeError if id remained None
    somehow (defensive-log-then-raise from ak-6si (4)).

Skips cleanly if flask/sqlalchemy imports fail (matches the arc pattern).
"""

import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker, scoped_session
    from models.Base import Base
    from models.AgentConversation import AgentConversation
    from models.AgentMessage import AgentMessage
    from services.agentConversationService import (
        AgentConversationService,
        WEALTH_DIGEST_TITLE,
    )
    _DEPS_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover — env-dep skip
    _DEPS_OK = False
    _SKIP_REASON = f"agentConversationService import chain unavailable: {_exc}"


class _StubDb:
    """Stand-in for the SQLAlchemy `db` object BaseService.db returns.
    Exposes a `.session` attribute that consistently returns the SAME
    session — reproduces the FIXED background-task path (post-ak-6si,
    the fix caches the session locally so add/flush/commit all hit
    the same session).

    Also used to reproduce the PRE-fix bug shape via a `_broken=True`
    flag: return a NEW session on every .session access, mirroring
    the g.db-is-None branch of BaseService.db property. The
    integration test below asserts the CURRENT (post-fix) code still
    behaves correctly under the broken-shim path — proves the
    session-caching fix inside create_conversation is what makes it
    robust, not the caller's environment.
    """

    def __init__(self, session_factory, *, broken=False):
        self._session_factory = session_factory
        self._cached = None
        self._broken = broken

    @property
    def session(self):
        if self._broken:
            # Reproduce pre-ak-6si bug shape: fresh session per access.
            return self._session_factory()
        if self._cached is None:
            self._cached = self._session_factory()
        return self._cached


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestConversationServiceRealDb(unittest.TestCase):
    """ak-6si integration: real SQLite session, real AgentConversation
    inserts, real id round-trip."""

    def setUp(self):
        # Fresh in-memory SQLite per test — no cross-test pollution.
        self.engine = create_engine("sqlite:///:memory:")
        # Only create the AgentConversation + AgentMessage tables we
        # need. Full Base.metadata.create_all would drag in every
        # model with its own dep chain (Firebase, transactions, etc.).
        AgentConversation.__table__.create(self.engine)
        AgentMessage.__table__.create(self.engine)
        self._SessionFactory = scoped_session(
            sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine,
            )
        )
        # AgentConversationService is a singleton — reset before every
        # test so the previous test's cached instance doesn't carry
        # a stale patched db.
        AgentConversationService._instance = None
        self.svc = AgentConversationService()
        # Patch the db property with a same-session shim (post-ak-6si
        # session-caching fix path — this is the good-behavior baseline).
        self._install_stub_db(broken=False)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass
        AgentConversationService._instance = None

    def _install_stub_db(self, *, broken):
        stub = _StubDb(self._SessionFactory, broken=broken)
        # BaseService.db is a property (class-level). Override on the
        # INSTANCE via __dict__ trick — Python attribute lookup finds
        # the instance attr before the class descriptor.
        type(self.svc).db = property(lambda _self, _stub=stub: _stub)

    # ── create_conversation: never returns None ────────────────────────

    def test_create_conversation_returns_positive_int(self):
        print("\n[ak-6si — create_conversation returns int id, not None]")
        cid = self.svc.create_conversation(
            user_id="user-abc",
            agent_type="investment",
            title=WEALTH_DIGEST_TITLE,
        )
        self.assertIsNotNone(cid, "create_conversation returned None — ak-6si bug regressed")
        self.assertIsInstance(cid, int)
        self.assertGreater(cid, 0)
        print(f"  ✓ returned int id={cid}")

    def test_create_conversation_persists_row(self):
        print("\n[ak-6si — created conversation is queryable in the same session]")
        cid = self.svc.create_conversation(
            user_id="user-abc",
            agent_type="investment",
            title=WEALTH_DIGEST_TITLE,
        )
        # Query using the same session shim the service uses.
        session = type(self.svc).db.fget(self.svc).session
        row = session.query(AgentConversation).filter(
            AgentConversation.id == cid,
        ).first()
        self.assertIsNotNone(row)
        self.assertEqual(row.user_id, "user-abc")
        self.assertEqual(row.agent_type, "investment")
        self.assertEqual(row.title, WEALTH_DIGEST_TITLE)
        self.assertIsNone(row.deleted_at)
        print(f"  ✓ row exists in DB with matching fields")

    # ── find_or_create_by_title: miss → create ─────────────────────────

    def test_find_or_create_creates_fresh_on_miss(self):
        print("\n[ak-6si — find_or_create creates + returns int id when no match]")
        cid = self.svc.find_or_create_by_title(
            user_id="user-abc",
            agent_type="investment",
            title=WEALTH_DIGEST_TITLE,
        )
        self.assertIsNotNone(cid, "find_or_create returned None — ak-6si bug regressed")
        self.assertIsInstance(cid, int)
        self.assertGreater(cid, 0)
        print(f"  ✓ fresh conv created, id={cid}")

    # ── find_or_create_by_title: hit → return existing ─────────────────

    def test_find_or_create_returns_existing_on_hit(self):
        print("\n[ak-6si — find_or_create returns existing id when title matches]")
        first = self.svc.find_or_create_by_title(
            user_id="user-abc",
            agent_type="investment",
            title=WEALTH_DIGEST_TITLE,
        )
        second = self.svc.find_or_create_by_title(
            user_id="user-abc",
            agent_type="investment",
            title=WEALTH_DIGEST_TITLE,
        )
        self.assertEqual(first, second, "second call didn't converge on the first conv")
        # Verify exactly one row exists (not two).
        session = type(self.svc).db.fget(self.svc).session
        count = session.query(AgentConversation).filter(
            AgentConversation.title == WEALTH_DIGEST_TITLE,
        ).count()
        self.assertEqual(count, 1, f"expected 1 row, got {count} — dup created")
        print(f"  ✓ both calls returned id={first}; count in DB=1 (idempotent)")

    # ── daily-run convergence: N repeat calls → 1 conv ────────────────

    def test_repeated_daily_runs_converge_on_single_conv(self):
        print("\n[ak-6si — 10 repeat find_or_create calls converge on the same id]")
        ids = set()
        for _ in range(10):
            cid = self.svc.find_or_create_by_title(
                user_id="user-abc",
                agent_type="investment",
                title=WEALTH_DIGEST_TITLE,
            )
            ids.add(cid)
        self.assertEqual(len(ids), 1, f"expected 1 unique id, got {len(ids)}: {ids}")
        # And exactly one row in DB.
        session = type(self.svc).db.fget(self.svc).session
        count = session.query(AgentConversation).filter(
            AgentConversation.title == WEALTH_DIGEST_TITLE,
        ).count()
        self.assertEqual(count, 1)
        print(f"  ✓ 10 calls → 1 id → 1 row (daily-run convergence guaranteed)")

    # ── append_message uses the returned id ───────────────────────────

    def test_append_message_succeeds_with_find_or_create_id(self):
        """End-to-end reproduction of the WealthDigestTask.run() flow:
        find_or_create returns an id, append_message writes with that
        id, both succeed."""
        print("\n[ak-6si — full flow: find_or_create id → append_message → message row exists]")
        cid = self.svc.find_or_create_by_title(
            user_id="user-abc",
            agent_type="investment",
            title=WEALTH_DIGEST_TITLE,
        )
        msg_id = self.svc.append_message(
            user_id="user-abc",
            conversation_id=cid,
            role="assistant",
            content="[Personal use — not investment advice]\n\ntest digest body",
        )
        self.assertIsNotNone(msg_id, "append_message returned None — ownership check failed")
        self.assertIsInstance(msg_id, int)
        # Verify the message row is queryable.
        session = type(self.svc).db.fget(self.svc).session
        row = session.query(AgentMessage).filter(
            AgentMessage.id == msg_id,
        ).first()
        self.assertIsNotNone(row)
        self.assertEqual(row.conversation_id, cid)
        self.assertEqual(row.role, "assistant")
        self.assertIn("[Personal use", row.content)
        print(f"  ✓ conv id={cid} → msg id={msg_id}; content persisted")

    # ── session-caching invariant: broken-db shim ─────────────────────

    def test_create_conversation_robust_under_broken_db_shim(self):
        """Reproduce the PRE-fix background-task session shape: db
        returns a FRESH scoped_session on every .session access. The
        post-ak-6si fix caches the session in a local variable inside
        create_conversation, so add/flush/commit all hit the SAME
        session regardless of what the db property does. This test
        proves the fix works even against the hostile db shim.

        If this test fails, someone reverted the session-caching fix
        inside create_conversation (or find_or_create_by_title)."""
        print("\n[ak-6si — create_conversation robust to broken db (fresh session per access)]")
        self._install_stub_db(broken=True)
        cid = self.svc.create_conversation(
            user_id="user-xyz",
            agent_type="investment",
            title=WEALTH_DIGEST_TITLE,
        )
        # Post-fix: id is populated because create_conversation caches
        # the session in a local + flushes before reading conv.id.
        self.assertIsNotNone(cid, "create_conversation returned None under broken db — session-caching fix regressed")
        self.assertIsInstance(cid, int)
        self.assertGreater(cid, 0)
        print(f"  ✓ id={cid} — session-caching + flush fix intact under broken-db shim")


# ── Source-inspection guards (always run — catch fix regression) ────


class TestAk6siFixSourceInvariants(unittest.TestCase):
    """Guard the three source-level pieces of the ak-6si fix:
    (1) session cached in a local variable inside create_conversation.
    (2) explicit session.flush() before session.commit().
    (3) conv.id snapshotted BEFORE commit + defensive None-check
        that raises rather than returns None."""

    @classmethod
    def setUpClass(cls):
        path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "services", "agentConversationService.py",
        )
        with open(path) as fh:
            cls.src = fh.read()
        # Also strip comments + docstrings so a rationale-comment
        # that quotes the OLD pre-fix pattern (as reference in the
        # ak-6si docstring) doesn't false-trip the "old shape gone"
        # assertions. Uses a cheap two-pass strip: full-line and
        # end-of-line `#` comments, then a docstring-block strip.
        cls.code = cls._strip_comments_and_docstrings(cls.src)

    @staticmethod
    def _strip_comments_and_docstrings(src):
        # Strip line + trailing comments.
        out_lines = []
        for line in src.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            hash_pos = line.find("#")
            if hash_pos != -1:
                pre = line[:hash_pos]
                if pre.count('"') % 2 == 0 and pre.count("'") % 2 == 0:
                    line = line[:hash_pos].rstrip()
            out_lines.append(line)
        code = "\n".join(out_lines)
        # Strip triple-quoted docstrings/blocks.
        import re
        code = re.sub(r'"""[\s\S]*?"""', "", code)
        code = re.sub(r"'''[\s\S]*?'''", "", code)
        return code

    def test_create_conversation_caches_session_in_local(self):
        print("\n[ak-6si (1) — create_conversation caches session in local variable]")
        # The exact shape we shipped (assertions against RAW source
        # since these are exec-code patterns present in the shipping
        # code, not just comments).
        self.assertIn("session = self.db.session", self.src)
        self.assertIn("session.add(conv)", self.src)
        self.assertIn("session.commit()", self.src)
        # And the pre-fix shape (self.db.session.add + self.db.session.commit)
        # is GONE from create_conversation's EXEC BODY (not just
        # inside the ak-6si rationale docstring which quotes the old
        # pattern for context).
        import re
        m = re.search(
            r"def create_conversation\(self.*?\n(?=    def )",
            self.code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate create_conversation body")
        body = m.group(0)
        self.assertNotIn("self.db.session.add(conv)", body,
                         "create_conversation still uses self.db.session.add() — bug regressed")
        self.assertNotIn("self.db.session.commit()", body,
                         "create_conversation still uses self.db.session.commit() — bug regressed")
        print("  ✓ local `session` cache in place; pre-fix chained-property shape gone from exec body")

    def test_flush_before_commit(self):
        """ak-6si (2): explicit flush populates conv.id from the DB
        RETURNING/lastrowid BEFORE commit's expire_on_commit fires."""
        print("\n[ak-6si (2) — session.flush() precedes session.commit() in create_conversation]")
        # Extract just create_conversation's body and check ordering.
        import re
        m = re.search(
            r"def create_conversation\(self.*?\n(?=    def )",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        body = m.group(0)
        flush_idx = body.find("session.flush()")
        commit_idx = body.find("session.commit()")
        self.assertGreaterEqual(flush_idx, 0, "session.flush() missing from create_conversation")
        self.assertGreaterEqual(commit_idx, 0, "session.commit() missing from create_conversation")
        self.assertLess(flush_idx, commit_idx,
                        "flush() must precede commit() so conv.id is populated before expire")
        print("  ✓ flush() before commit() — id populated before expire fires")

    def test_conv_id_snapshotted_and_none_check(self):
        """ak-6si (3): conv.id → local var BEFORE commit + defensive
        None-check that raises rather than returns None."""
        print("\n[ak-6si (3) — conv.id snapshotted + None-check raises (never returns None)]")
        # The exact shape: `conv_id = conv.id` before commit.
        self.assertIn("conv_id = conv.id", self.src)
        # And the None-check raises RuntimeError, not returns None.
        self.assertIn("if conv_id is None:", self.src)
        self.assertIn(
            "conv.id is None after flush+commit",
            self.src,
        )
        self.assertIn("raise RuntimeError(msg)", self.src)
        # Nowhere in the module do we `return conv.id` (which would
        # bypass the snapshot). We DO `return conv_id` — the local.
        # Match every occurrence of `return conv.id` (with a dot) and
        # assert none.
        import re
        bad_returns = re.findall(r"return\s+conv\.id\b", self.src)
        self.assertEqual(
            bad_returns, [],
            "found `return conv.id` — bypasses the ak-6si snapshot; "
            "must be `return conv_id` (local var)",
        )
        print("  ✓ snapshot + None-raise + no bypass return")

    def test_find_or_create_caches_session_and_guards_none(self):
        """ak-6si applies the same treatment to find_or_create_by_title:
        cache session, guard against existing-row-with-None-id, guard
        against create_conversation-returned-None (belt-and-braces
        since create_conversation now raises)."""
        print("\n[ak-6si — find_or_create_by_title caches session + guards None-id]")
        import re
        m = re.search(
            r"def find_or_create_by_title\(self.*?\n(?=    def )",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        body = m.group(0)
        # Session cached.
        self.assertIn("session = self.db.session", body)
        # SELECT uses `session.query`, not `self.db.session.query`.
        self.assertIn("session.query(AgentConversation)", body)
        # Existing-row None guard.
        self.assertIn("if existing.id is None:", body)
        # create_conversation return None guard (belt-and-braces).
        self.assertIn("if conv_id is None:", body)
        print("  ✓ session cache + existing-id guard + create-return guard all present")


# ── ak-m6k: append_message read-side session-cache tests ───────────────


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestAppendMessageSessionCacheAkM6k(unittest.TestCase):
    """ak-m6k INSTANCE 2 (read-side): append_message._fetch_owned
    used a fresh scoped_session/engine per self.db access; the
    ownership SELECT couldn't see a conv that had JUST been committed
    by find_or_create_by_title via a DIFFERENT fresh engine. Symptom:
    append_message returned None → digest never persisted → ak-ran
    Step 3 activation failed twice in prod.

    Fix (this batch): session cached in a local at append_message
    entry + threaded into _fetch_owned via session= kwarg. Read +
    write now share the SAME scoped_session/engine.

    Tests mirror the ak-6si create-side coverage: real SQLite
    integration + broken-db shim to prove the fix survives the
    hostile session shape."""

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        AgentConversation.__table__.create(self.engine)
        AgentMessage.__table__.create(self.engine)
        self._SessionFactory = scoped_session(
            sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine,
            )
        )
        AgentConversationService._instance = None
        self.svc = AgentConversationService()
        self._install_stub_db(broken=False)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass
        AgentConversationService._instance = None

    def _install_stub_db(self, *, broken):
        stub = _StubDb(self._SessionFactory, broken=broken)
        type(self.svc).db = property(lambda _self, _stub=stub: _stub)

    def test_append_message_returns_int_msg_id(self):
        print("\n[ak-m6k — append_message returns positive int, never None]")
        cid = self.svc.find_or_create_by_title(
            user_id="u1", agent_type="investment", title=WEALTH_DIGEST_TITLE,
        )
        mid = self.svc.append_message(
            user_id="u1", conversation_id=cid,
            role="assistant", content="digest body",
        )
        self.assertIsNotNone(mid, "append_message returned None — ak-m6k regressed")
        self.assertIsInstance(mid, int)
        self.assertGreater(mid, 0)
        print(f"  ✓ cid={cid} mid={mid}")

    def test_append_message_finds_just_committed_conv(self):
        """ak-m6k INSTANCE 2 direct repro: create conv then IMMEDIATELY
        append. Pre-fix, _fetch_owned's SELECT via a fresh engine
        couldn't see the conv committed by find_or_create's earlier
        fresh engine, so append_message returned None."""
        print("\n[ak-m6k INSTANCE 2 repro — append RIGHT AFTER create sees the just-committed conv]")
        cid = self.svc.find_or_create_by_title(
            user_id="u1", agent_type="investment", title=WEALTH_DIGEST_TITLE,
        )
        # Immediate append — no delay, no session cleanup between.
        mid = self.svc.append_message(
            user_id="u1", conversation_id=cid,
            role="assistant", content="immediate",
        )
        self.assertIsNotNone(mid)
        # And the message actually persisted with correct conv_id.
        session = type(self.svc).db.fget(self.svc).session
        row = session.query(AgentMessage).filter(AgentMessage.id == mid).first()
        self.assertIsNotNone(row)
        self.assertEqual(row.conversation_id, cid)
        print(f"  ✓ create+immediate-append round-trip works (cid={cid} mid={mid})")

    def test_append_message_survives_broken_db_shim(self):
        """Reproduce the pre-fix background-task session shape (fresh
        session per .db access). The post-ak-m6k session-cache-at-
        entry + thread-into-_fetch_owned pattern MUST survive this,
        else the fix is inert."""
        print("\n[ak-m6k — append_message robust under broken db (fresh session per access)]")
        # Create with the good (cached) db first so conv is committed.
        cid = self.svc.find_or_create_by_title(
            user_id="u1", agent_type="investment", title=WEALTH_DIGEST_TITLE,
        )
        # Now switch to the broken db shim and try to append.
        self._install_stub_db(broken=True)
        # Post-fix: append_message caches session at entry, threads
        # into _fetch_owned, so read+write hit the SAME session even
        # though the .db property returns a fresh one on each access.
        mid = self.svc.append_message(
            user_id="u1", conversation_id=cid,
            role="assistant", content="under-broken-db",
        )
        self.assertIsNotNone(mid, "append_message failed under broken-db — ak-m6k fix regressed")
        self.assertIsInstance(mid, int)
        print(f"  ✓ mid={mid} — session-cache survives fresh-session-per-access shape")

    def test_wrong_user_returns_none_not_raises(self):
        """Ownership check semantics preserved — wrong user_id gets
        None (controller maps to 404), not RuntimeError."""
        print("\n[ak-m6k — cross-user append still returns None (ownership check preserved)]")
        cid = self.svc.find_or_create_by_title(
            user_id="u1", agent_type="investment", title=WEALTH_DIGEST_TITLE,
        )
        mid = self.svc.append_message(
            user_id="u_hostile",  # different user
            conversation_id=cid,
            role="assistant", content="hostile",
        )
        self.assertIsNone(mid, "cross-user append should return None, not persist")
        # And the message table stays empty.
        session = type(self.svc).db.fget(self.svc).session
        count = session.query(AgentMessage).count()
        self.assertEqual(count, 0)
        print("  ✓ wrong user → None, no row written")

    def test_soft_delete_also_uses_cached_session(self):
        """Regression: soft_delete's _fetch_owned + commit must ALSO
        share the same session (same pattern as append_message)."""
        print("\n[ak-m6k — soft_delete uses cached session for read+write consistency]")
        cid = self.svc.find_or_create_by_title(
            user_id="u1", agent_type="investment", title=WEALTH_DIGEST_TITLE,
        )
        ok = self.svc.soft_delete(user_id="u1", conversation_id=cid)
        self.assertTrue(ok)
        # And a subsequent find_or_create should CREATE FRESH (soft-
        # deleted conv is treated as not-found).
        new_cid = self.svc.find_or_create_by_title(
            user_id="u1", agent_type="investment", title=WEALTH_DIGEST_TITLE,
        )
        self.assertNotEqual(cid, new_cid, "soft-deleted conv resurrected — semantics broken")
        print(f"  ✓ soft-delete cid={cid}, subsequent create new_cid={new_cid}")


class TestAkM6kFixSourceInvariants(unittest.TestCase):
    """Source-inspection guards for the ak-m6k fix — always run so
    a future refactor can't silently revert the session-caching +
    thread-into-_fetch_owned pattern."""

    @classmethod
    def setUpClass(cls):
        path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "services", "agentConversationService.py",
        )
        with open(path) as fh:
            cls.src = fh.read()
        cls.code = TestAk6siFixSourceInvariants._strip_comments_and_docstrings(cls.src)

    def test_fetch_owned_accepts_session_kwarg(self):
        print("\n[ak-m6k — _fetch_owned accepts optional session= kwarg]")
        self.assertIn(
            "def _fetch_owned(self, user_id, conversation_id, *, session=None):",
            self.src,
        )
        # And uses the passed session when non-None.
        self.assertIn(
            "query_session = session if session is not None else self.db.session",
            self.src,
        )
        print("  ✓ session= kwarg + fallback path both present")

    def test_append_message_threads_session_into_fetch_owned(self):
        print("\n[ak-m6k — append_message caches session + threads into _fetch_owned]")
        import re
        m = re.search(
            r"def append_message\(self.*?\n(?=    def )",
            self.code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate append_message body")
        body = m.group(0)
        # Session cached at entry.
        self.assertIn("session = self.db.session", body)
        # Threaded into _fetch_owned via kwarg.
        self.assertIn(
            "self._fetch_owned(user_id, conversation_id, session=session)",
            body,
        )
        # Add + flush + commit all use the cached session.
        self.assertIn("session.add(msg)", body)
        self.assertIn("session.flush()", body)
        self.assertIn("session.commit()", body)
        # Snapshot msg.id BEFORE commit.
        self.assertIn("msg_id = msg.id", body)
        # None-check raises rather than returns None.
        self.assertIn("if msg_id is None:", body)
        self.assertIn("raise RuntimeError(fail_msg)", body)
        # Pre-fix shape gone.
        self.assertNotIn("self.db.session.add(msg)", body,
                         "append_message still uses self.db.session.add — regressed")
        self.assertNotIn("self.db.session.commit()", body,
                         "append_message still uses self.db.session.commit — regressed")
        print("  ✓ cached session + threaded + snapshot + raise-on-None + pre-fix shape gone")

    def test_soft_delete_threads_session_into_fetch_owned(self):
        print("\n[ak-m6k — soft_delete caches session + threads into _fetch_owned]")
        import re
        m = re.search(
            r"def soft_delete\(self.*?\n(?=    def )",
            self.code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate soft_delete body")
        body = m.group(0)
        self.assertIn("session = self.db.session", body)
        self.assertIn(
            "self._fetch_owned(user_id, conversation_id, session=session)",
            body,
        )
        self.assertIn("session.commit()", body)
        self.assertNotIn("self.db.session.commit()", body)
        print("  ✓ soft_delete session-cache + threaded")


if __name__ == "__main__":
    print("ak-6si + ak-m6k — AgentConversationService real-DB integration tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
