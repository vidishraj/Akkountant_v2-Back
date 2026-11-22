"""ak-kxdr: tests for Files vault soft-delete + 30-day restore + trash.

Scope:
  * **Service layer** (list_files / delete_file / list_trash /
    restore_file / hard_delete_file / cleanup_expired_trash /
    get_file) against a real in-memory SQLite — AST-lifted to
    avoid BaseService Google-auth boot chain (same pattern as
    test_watchlist / test_reconciliation_issues_endpoint).
  * **Model shape** — user_files.deleted_at column + the
    user-deleted-at composite index.
  * **Controller / route drift guards** — source-grep.
  * **ak-kxdr MUTATION DRIFT GUARD** — asserts the EXACT set of
    POST/DELETE/PUT routes on /files/*. Matches the ak-05af-
    banked exact-set pattern for legitimate-mutations beads.
  * **Scheduler task wire-up** — VaultTrashCleanupTask registered
    in TASK_MAPPING.

Pin-downs:
  (1) list_files excludes soft-deleted rows.
  (2) delete_file soft-deletes (sets deleted_at, keeps row + disk).
  (3) delete_file is user_id-scoped.
  (4) get_file excludes soft-deleted (can't download trashed).
  (5) move_file / update_label refuse trashed files.
  (6) list_trash returns ONLY soft-deleted, newest-first.
  (7) list_trash days_remaining counter is correct; clamped
      [0, VAULT_RETENTION_DAYS].
  (8) restore_file clears deleted_at.
  (9) restore_file wrong-user returns 404-shape (ValueError).
  (10) hard_delete_file requires confirm_id == file_id.
  (11) hard_delete_file refuses live files (must be in trash).
  (12) hard_delete_file wrong-user returns 404-shape.
  (13) cleanup_expired_trash DROPs rows past retention;
       preserves rows inside window.
  (14) cleanup_expired_trash tolerates missing disk file.
  (15) Model has deleted_at column (nullable) + composite index.
  (16) Controller handlers defined + wired.
  (17) Routes wired with EXACT verb set (ak-kxdr mutation drift
       guard — exact-match, NOT reject-all).
  (18) Hard-delete controller gates on JSON body + confirm_id.
  (19) VaultTrashCleanupTask registered in TASK_MAPPING.
  (20) ``VAULT_RETENTION_DAYS`` constant is 30.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ─── Model-only import ─────────────────────────────────────────


try:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker
    import models  # noqa: F401 — register declarative targets
    from models.investmentHistory import (  # noqa: F401
        InvestmentHistory,
    )
    from models.user_files import UserFile, UserFolder
    _MODEL_DEPS_OK = True
    _MODEL_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover — env-dep skip
    _MODEL_DEPS_OK = False
    _MODEL_SKIP_REASON = f"UserFile model import failed: {_exc}"


# ─── AST-lift service methods ──────────────────────────────────


def _ast_lift_vault_service():
    """Return lifted service methods + module constants.

    Keys: list_files, delete_file, get_file, move_file,
    update_label, list_trash, restore_file, hard_delete_file,
    cleanup_expired_trash, VAULT_RETENTION_DAYS.
    """
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "fileStorageService.py",
    )
    with open(src_path, "r") as fh:
        src = fh.read()
    tree = ast.parse(src)

    want_constants = {"VAULT_RETENTION_DAYS"}
    want_methods = {
        "list_files", "delete_file", "get_file", "move_file",
        "update_label", "list_trash", "restore_file",
        "hard_delete_file", "cleanup_expired_trash",
    }
    kept = []
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            tgt = node.targets[0]
            if isinstance(tgt, ast.Name) and tgt.id in want_constants:
                kept.append(node)
                continue
        if isinstance(node, ast.ClassDef) and node.name == "FileStorageService":
            for child in node.body:
                if isinstance(
                    child, (ast.FunctionDef, ast.AsyncFunctionDef),
                ) and child.name in want_methods:
                    child.decorator_list = []
                    kept.append(child)

    lift = ast.Module(body=kept, type_ignores=[])
    ast.fix_missing_locations(lift)

    ns = {
        "__name__": "test_vault_ast_lift",
        "UserFile": UserFile,
        "UserFolder": UserFolder,
        "datetime": datetime,
        "timedelta": timedelta,
        "os": os,
    }
    exec(compile(lift, "<vault-service-AST-lift>", "exec"), ns)
    return {
        "list_files": ns["list_files"],
        "delete_file": ns["delete_file"],
        "get_file": ns["get_file"],
        "move_file": ns["move_file"],
        "update_label": ns["update_label"],
        "list_trash": ns["list_trash"],
        "restore_file": ns["restore_file"],
        "hard_delete_file": ns["hard_delete_file"],
        "cleanup_expired_trash": ns["cleanup_expired_trash"],
        "VAULT_RETENTION_DAYS": ns["VAULT_RETENTION_DAYS"],
    }


if _MODEL_DEPS_OK:
    _LIFTED = _ast_lift_vault_service()
    _missing = [
        k for k in (
            "list_files", "delete_file", "get_file", "move_file",
            "update_label", "list_trash", "restore_file",
            "hard_delete_file", "cleanup_expired_trash",
            "VAULT_RETENTION_DAYS",
        ) if _LIFTED.get(k) is None
    ]
    if _missing:
        raise AssertionError(
            f"ak-kxdr service AST-lift missing symbols: {_missing}. "
            f"Did they get renamed or removed from "
            f"services/fileStorageService.py?"
        )
    _SVC_DEPS_OK = True
    _SVC_SKIP_REASON = ""
else:
    _LIFTED = None
    _SVC_DEPS_OK = False
    _SVC_SKIP_REASON = f"service AST-lift skipped: {_MODEL_SKIP_REASON}"


# ─── Stubs ───────────────────────────────────────────────────


class _NoopLogger:
    def error(self, *_a, **_k):
        pass

    def warning(self, *_a, **_k):
        pass

    def info(self, *_a, **_k):
        pass


class _StubVaultService:
    """Minimal surface the AST-lifted methods need."""

    def __init__(self, session_factory, firebase_id="u1"):
        self._session_factory = session_factory
        self._cached_session = None
        self.logger = _NoopLogger()
        self._firebase_id = firebase_id

    @property
    def db(self):
        return self

    @property
    def session(self):
        if self._cached_session is None:
            self._cached_session = self._session_factory()
        return self._cached_session


class _StubG:
    """Stand-in for flask.g — AST-lifted methods read `g.get(
    'firebase_id')`. We shove this into the namespace."""

    def __init__(self, firebase_id):
        self._firebase_id = firebase_id

    def get(self, key, default=None):
        if key == "firebase_id":
            return self._firebase_id
        return default


def _make_svc(session_factory, firebase_id="u1"):
    """Build a stub service with the AST-lifted methods bound.

    AST-lifted methods close over the module-scope `g` symbol
    (from `from flask import g`). We rebind `g` in the lifted
    namespace to our stub before method invocation. Since the
    lifted functions live in the shared exec namespace from
    ``_ast_lift_vault_service``, we need to re-exec per-test
    with a scoped g OR globally rebind.

    Trick: attach the stub as a module global on the lifted
    functions' __globals__ dict.
    """
    svc = _StubVaultService(session_factory, firebase_id=firebase_id)
    # Bind the AST-lifted methods as instance methods
    for name in (
        "list_files", "delete_file", "get_file", "move_file",
        "update_label", "list_trash", "restore_file",
        "hard_delete_file", "cleanup_expired_trash",
    ):
        fn = _LIFTED[name]
        # Patch the lifted-function's globals to include our `g` stub
        fn.__globals__["g"] = _StubG(firebase_id)
        setattr(svc, name, types.MethodType(fn, svc))
    return svc


# ─── Service-layer tests ──────────────────────────────────────


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestListFiles(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        UserFolder.__table__.create(self.engine)
        UserFile.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.svc = _make_svc(self._SessionFactory)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add_file(self, user_id="u1", deleted_at=None, **kwargs):
        kwargs.setdefault("original_filename", "test.pdf")
        kwargs.setdefault("stored_filename", "stored.pdf")
        kwargs.setdefault("file_type", "application/pdf")
        kwargs.setdefault("file_extension", "pdf")
        kwargs.setdefault("file_size", 100)
        kwargs.setdefault("storage_path", "uploads/u1/stored.pdf")
        row = UserFile(
            user_id=user_id, deleted_at=deleted_at, **kwargs,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (1) list_files excludes soft-deleted
    def test_list_files_excludes_soft_deleted(self):
        self._add_file(deleted_at=None)  # live
        self._add_file(deleted_at=datetime.utcnow())  # trashed
        result = self.svc.list_files()
        self.assertEqual(len(result), 1, "list_files must exclude trashed")
        self.assertIsNone(result[0].deleted_at)


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestDeleteFile(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        UserFolder.__table__.create(self.engine)
        UserFile.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.svc = _make_svc(self._SessionFactory)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add_live(self, user_id="u1"):
        row = UserFile(
            user_id=user_id, original_filename="test.pdf",
            stored_filename="stored.pdf", file_type="application/pdf",
            file_extension="pdf", file_size=100,
            storage_path="uploads/u1/stored.pdf",
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (2) delete_file is SOFT — sets deleted_at, keeps row
    def test_delete_file_is_soft(self):
        row = self._add_live()
        result = self.svc.delete_file(row.id)
        self.assertTrue(result)
        fresh = self.svc.db.session.query(UserFile).filter_by(
            id=row.id,
        ).first()
        self.assertIsNotNone(
            fresh,
            "delete_file hard-dropped the row — must soft-delete",
        )
        self.assertIsNotNone(
            fresh.deleted_at,
            "delete_file did not set deleted_at",
        )

    # (3) delete_file is user_id-scoped
    def test_delete_file_user_scoped(self):
        other_row = self._add_live(user_id="other_user")
        with self.assertRaises(ValueError):
            # u1 (default) tries to delete other_user's file
            self.svc.delete_file(other_row.id)
        # Original row preserved + NOT soft-deleted
        fresh = self.svc.db.session.query(UserFile).filter_by(
            id=other_row.id,
        ).first()
        self.assertIsNone(
            fresh.deleted_at,
            "delete_file leaked cross-user mutation",
        )


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestGetFile(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        UserFolder.__table__.create(self.engine)
        UserFile.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.svc = _make_svc(self._SessionFactory)
        # Create a real tmpfile so get_file's disk-exists check passes
        self._tmpdir = tempfile.mkdtemp()
        self._cwd_before = os.getcwd()
        os.chdir(self._tmpdir)
        os.makedirs(os.path.join("uploads", "u1"), exist_ok=True)
        with open(
            os.path.join("uploads", "u1", "stored.pdf"), "w",
        ) as f:
            f.write("dummy")

    def tearDown(self):
        os.chdir(self._cwd_before)
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add(self, deleted_at=None):
        row = UserFile(
            user_id="u1", original_filename="test.pdf",
            stored_filename="stored.pdf", file_type="application/pdf",
            file_extension="pdf", file_size=100,
            storage_path="uploads/u1/stored.pdf",
            deleted_at=deleted_at,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (4) get_file refuses trashed files
    def test_get_file_excludes_trashed(self):
        trashed = self._add(deleted_at=datetime.utcnow())
        with self.assertRaises(ValueError):
            self.svc.get_file(trashed.id)

    def test_get_file_allows_live(self):
        live = self._add(deleted_at=None)
        user_file, full_path = self.svc.get_file(live.id)
        self.assertEqual(user_file.id, live.id)


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestEditingRefusesTrashed(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        UserFolder.__table__.create(self.engine)
        UserFile.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.svc = _make_svc(self._SessionFactory)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add(self, deleted_at=None):
        row = UserFile(
            user_id="u1", original_filename="test.pdf",
            stored_filename="stored.pdf", file_type="application/pdf",
            file_extension="pdf", file_size=100,
            storage_path="uploads/u1/stored.pdf",
            deleted_at=deleted_at,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (5) move_file refuses trashed
    def test_move_file_refuses_trashed(self):
        trashed = self._add(deleted_at=datetime.utcnow())
        with self.assertRaises(ValueError):
            self.svc.move_file(trashed.id, folder_id=None)

    def test_update_label_refuses_trashed(self):
        trashed = self._add(deleted_at=datetime.utcnow())
        with self.assertRaises(ValueError):
            self.svc.update_label(trashed.id, "new label")


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestListTrash(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        UserFolder.__table__.create(self.engine)
        UserFile.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.svc = _make_svc(self._SessionFactory)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add(self, user_id="u1", deleted_at=None, filename="test.pdf"):
        row = UserFile(
            user_id=user_id, original_filename=filename,
            stored_filename=filename, file_type="application/pdf",
            file_extension="pdf", file_size=100,
            storage_path=f"uploads/{user_id}/{filename}",
            deleted_at=deleted_at,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (6) list_trash returns ONLY soft-deleted, newest-first
    def test_list_trash_returns_only_trashed_newest_first(self):
        self._add(deleted_at=None, filename="live.pdf")  # live — must excl
        self._add(
            deleted_at=datetime.utcnow() - timedelta(days=5),
            filename="older_trash.pdf",
        )
        self._add(
            deleted_at=datetime.utcnow() - timedelta(days=1),
            filename="newer_trash.pdf",
        )
        result = self.svc.list_trash()
        names = [f.original_filename for f, _ in result]
        self.assertEqual(names, ["newer_trash.pdf", "older_trash.pdf"])
        self.assertNotIn(
            "live.pdf", names,
            "list_trash leaked a live file",
        )

    # (7) days_remaining counter correct + clamped
    def test_list_trash_days_remaining(self):
        retention = _LIFTED["VAULT_RETENTION_DAYS"]
        self.assertEqual(retention, 30, "VAULT_RETENTION_DAYS drift")

        # Fresh trash: ~30 days remaining
        self._add(
            deleted_at=datetime.utcnow(),
            filename="fresh_trash.pdf",
        )
        # 10-day-old: ~20 remaining
        self._add(
            deleted_at=datetime.utcnow() - timedelta(days=10),
            filename="mid_trash.pdf",
        )
        # 35-day-old: clamped to 0 (shouldn't happen in prod — cleanup
        # task would have DROPed by now — but days_remaining must
        # clamp defensively)
        self._add(
            deleted_at=datetime.utcnow() - timedelta(days=35),
            filename="expired.pdf",
        )

        result = self.svc.list_trash()
        by_name = {f.original_filename: days for f, days in result}
        # Allow +/-1 for clock-second rounding
        self.assertIn(by_name["fresh_trash.pdf"], (29, 30))
        self.assertIn(by_name["mid_trash.pdf"], (19, 20))
        self.assertEqual(
            by_name["expired.pdf"], 0,
            "days_remaining must clamp to 0 for expired trash",
        )

    # user-scoped list_trash
    def test_list_trash_user_scoped(self):
        self._add(
            user_id="u1", deleted_at=datetime.utcnow(),
            filename="mine.pdf",
        )
        self._add(
            user_id="u2", deleted_at=datetime.utcnow(),
            filename="theirs.pdf",
        )
        result = self.svc.list_trash()
        names = [f.original_filename for f, _ in result]
        self.assertEqual(names, ["mine.pdf"])


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestRestoreFile(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        UserFolder.__table__.create(self.engine)
        UserFile.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.svc = _make_svc(self._SessionFactory)

    def tearDown(self):
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add_trashed(self, user_id="u1"):
        row = UserFile(
            user_id=user_id, original_filename="test.pdf",
            stored_filename="stored.pdf", file_type="application/pdf",
            file_extension="pdf", file_size=100,
            storage_path=f"uploads/{user_id}/stored.pdf",
            deleted_at=datetime.utcnow(),
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (8) restore_file clears deleted_at
    def test_restore_clears_deleted_at(self):
        row = self._add_trashed()
        result = self.svc.restore_file(row.id)
        self.assertIsNone(result.deleted_at)
        fresh = self.svc.db.session.query(UserFile).filter_by(
            id=row.id,
        ).first()
        self.assertIsNone(fresh.deleted_at)

    # (9) wrong-user → ValueError (controller maps to 404)
    def test_restore_wrong_user_raises(self):
        row = self._add_trashed(user_id="owner")
        svc_attacker = _make_svc(self._SessionFactory, firebase_id="attacker")
        with self.assertRaises(ValueError):
            svc_attacker.restore_file(row.id)
        # Original row preserved + STILL in trash
        fresh = self.svc.db.session.query(UserFile).filter_by(
            id=row.id,
        ).first()
        self.assertIsNotNone(
            fresh.deleted_at,
            "wrong-user restore leaked the mutation",
        )

    def test_restore_nonexistent_raises(self):
        with self.assertRaises(ValueError):
            self.svc.restore_file("nonexistent-uuid")


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestHardDeleteFile(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        UserFolder.__table__.create(self.engine)
        UserFile.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.svc = _make_svc(self._SessionFactory)
        # Set up tmpdir + file so disk-remove path exercises
        self._tmpdir = tempfile.mkdtemp()
        self._cwd_before = os.getcwd()
        os.chdir(self._tmpdir)
        os.makedirs(os.path.join("uploads", "u1"), exist_ok=True)
        with open(
            os.path.join("uploads", "u1", "stored.pdf"), "w",
        ) as f:
            f.write("dummy")

    def tearDown(self):
        os.chdir(self._cwd_before)
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add_trashed(self, user_id="u1"):
        row = UserFile(
            user_id=user_id, original_filename="test.pdf",
            stored_filename="stored.pdf", file_type="application/pdf",
            file_extension="pdf", file_size=100,
            storage_path=f"uploads/{user_id}/stored.pdf",
            deleted_at=datetime.utcnow(),
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    def _add_live(self, user_id="u1"):
        row = UserFile(
            user_id=user_id, original_filename="test.pdf",
            stored_filename="stored.pdf", file_type="application/pdf",
            file_extension="pdf", file_size=100,
            storage_path=f"uploads/{user_id}/stored.pdf",
            deleted_at=None,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (10) confirm_id must equal file_id
    def test_hard_delete_requires_matching_confirm_id(self):
        row = self._add_trashed()
        with self.assertRaises(ValueError) as cm:
            self.svc.hard_delete_file(row.id, confirm_id="wrong")
        self.assertIn("confirm_id", str(cm.exception))
        # Row preserved
        fresh = self.svc.db.session.query(UserFile).filter_by(
            id=row.id,
        ).first()
        self.assertIsNotNone(fresh)

    def test_hard_delete_rejects_truthy_but_non_matching(self):
        row = self._add_trashed()
        # Test that a truthy non-string or a different string both fail
        for bad in (True, "yes", "DELETE", "123"):
            with self.assertRaises(ValueError) as cm:
                self.svc.hard_delete_file(row.id, confirm_id=bad)
            self.assertIn("confirm_id", str(cm.exception))

    def test_hard_delete_with_matching_id_drops_row(self):
        row = self._add_trashed()
        result = self.svc.hard_delete_file(row.id, confirm_id=row.id)
        self.assertTrue(result)
        fresh = self.svc.db.session.query(UserFile).filter_by(
            id=row.id,
        ).first()
        self.assertIsNone(fresh, "hard_delete did not DROP the row")

    # (11) refuses live files (must be in trash first)
    def test_hard_delete_refuses_live_file(self):
        row = self._add_live()
        with self.assertRaises(ValueError):
            self.svc.hard_delete_file(row.id, confirm_id=row.id)
        # Row preserved + still live
        fresh = self.svc.db.session.query(UserFile).filter_by(
            id=row.id,
        ).first()
        self.assertIsNotNone(fresh)
        self.assertIsNone(fresh.deleted_at)

    # (12) wrong-user returns 404-shape (ValueError, NOT 403)
    def test_hard_delete_wrong_user_raises(self):
        row = self._add_trashed(user_id="owner")
        svc_attacker = _make_svc(self._SessionFactory, firebase_id="attacker")
        with self.assertRaises(ValueError):
            svc_attacker.hard_delete_file(row.id, confirm_id=row.id)
        # Row preserved
        fresh = self.svc.db.session.query(UserFile).filter_by(
            id=row.id,
        ).first()
        self.assertIsNotNone(
            fresh, "wrong-user hard_delete leaked the DROP",
        )


@unittest.skipUnless(_SVC_DEPS_OK, _SVC_SKIP_REASON)
class TestCleanupExpiredTrash(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        UserFolder.__table__.create(self.engine)
        UserFile.__table__.create(self.engine)
        self._SessionFactory = scoped_session(sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine,
        ))
        self.svc = _make_svc(self._SessionFactory)
        self._tmpdir = tempfile.mkdtemp()
        self._cwd_before = os.getcwd()
        os.chdir(self._tmpdir)
        os.makedirs(os.path.join("uploads", "u1"), exist_ok=True)

    def tearDown(self):
        os.chdir(self._cwd_before)
        try:
            self._SessionFactory.remove()
        except Exception:
            pass

    def _add(self, deleted_at, filename="stored.pdf", create_disk=True):
        if create_disk:
            # Each test gets a unique filename to avoid reuse
            with open(
                os.path.join("uploads", "u1", filename), "w",
            ) as f:
                f.write("dummy")
        row = UserFile(
            user_id="u1", original_filename="test.pdf",
            stored_filename=filename, file_type="application/pdf",
            file_extension="pdf", file_size=100,
            storage_path=f"uploads/u1/{filename}",
            deleted_at=deleted_at,
        )
        self.svc.db.session.add(row)
        self.svc.db.session.commit()
        return row

    # (13) cleanup DROPs rows past retention; preserves inside window
    def test_cleanup_drops_only_expired(self):
        retention = _LIFTED["VAULT_RETENTION_DAYS"]
        now = datetime.utcnow()
        # Live file (not in trash) — must NOT be touched
        live_row = self._add(deleted_at=None, filename="live.pdf")
        # Fresh trash (1 day old)
        fresh = self._add(
            deleted_at=now - timedelta(days=1),
            filename="fresh.pdf",
        )
        # Expired trash (retention+1 days old)
        expired = self._add(
            deleted_at=now - timedelta(days=retention + 1),
            filename="expired.pdf",
        )
        dropped = self.svc.cleanup_expired_trash(retention_days=retention)
        self.assertEqual(dropped, 1, "only expired should be dropped")

        # Live + fresh preserved
        self.assertIsNotNone(
            self.svc.db.session.query(UserFile).filter_by(
                id=live_row.id,
            ).first(),
            "cleanup dropped a LIVE file — regression",
        )
        self.assertIsNotNone(
            self.svc.db.session.query(UserFile).filter_by(
                id=fresh.id,
            ).first(),
            "cleanup dropped a fresh trash file prematurely",
        )
        # Expired dropped
        self.assertIsNone(
            self.svc.db.session.query(UserFile).filter_by(
                id=expired.id,
            ).first(),
            "cleanup did not drop the expired file",
        )

    # (14) tolerates missing disk file
    def test_cleanup_tolerates_missing_disk_file(self):
        retention = _LIFTED["VAULT_RETENTION_DAYS"]
        expired = self._add(
            deleted_at=datetime.utcnow() - timedelta(days=retention + 1),
            filename="expired.pdf",
            create_disk=False,  # No disk file → OsError on unlink
        )
        # Should not raise
        dropped = self.svc.cleanup_expired_trash(retention_days=retention)
        self.assertEqual(dropped, 1)
        self.assertIsNone(
            self.svc.db.session.query(UserFile).filter_by(
                id=expired.id,
            ).first(),
        )


# ─── Model shape ──────────────────────────────────────────────


@unittest.skipUnless(_MODEL_DEPS_OK, _MODEL_SKIP_REASON)
class TestUserFileModelShape(unittest.TestCase):

    def test_deleted_at_column_exists(self):
        self.assertIn(
            "deleted_at", UserFile.__table__.columns,
            "ak-kxdr: deleted_at column missing on user_files",
        )

    def test_deleted_at_is_nullable(self):
        col = UserFile.__table__.columns["deleted_at"]
        self.assertTrue(
            col.nullable,
            "deleted_at must be nullable — pre-ak-kxdr rows post-ALTER "
            "have NULL = live",
        )

    def test_user_deleted_index_present(self):
        from sqlalchemy import Index
        indexes = [
            i for i in UserFile.__table__.indexes
            if isinstance(i, Index)
        ]
        names = {i.name for i in indexes}
        self.assertIn(
            "idx_uf_user_deleted_at", names,
            "composite (user_id, deleted_at) index missing — primary "
            "live-file and trash queries need it",
        )


# ─── Source-grep drift guards ────────────────────────────────


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_controller_has_list_trash_handler():
    src = _read_source("controllers/fileStorageEP.py")
    assert re.search(
        r"def\s+list_trash\s*\(\s*self\s*\)\s*:", src,
    ), "FileStorageController.list_trash handler missing"


def test_controller_has_restore_file_handler():
    src = _read_source("controllers/fileStorageEP.py")
    assert re.search(
        r"def\s+restore_file\s*\(\s*self\s*,\s*fileId\s*\)\s*:", src,
    ), "FileStorageController.restore_file handler missing"


def test_controller_has_hard_delete_handler():
    src = _read_source("controllers/fileStorageEP.py")
    assert re.search(
        r"def\s+hard_delete_file\s*\(\s*self\s*,\s*fileId\s*\)\s*:", src,
    ), "FileStorageController.hard_delete_file handler missing"


def test_hard_delete_handler_gates_on_json_body():
    """(18) Hard-delete handler must 400 on missing body / missing
    confirm_id. Grep-confirm the guard shape."""
    src = _read_source("controllers/fileStorageEP.py")
    # Slice hard_delete_file body
    m = re.search(
        r"def hard_delete_file\(self, fileId\):(.*?)(?=\n    @Logger"
        r"|\n    # ─|\n    def [a-zA-Z_]|\Z)",
        src, re.DOTALL,
    )
    assert m, "hard_delete_file body not found"
    body = m.group(1)
    assert "request.is_json" in body, (
        "hard_delete_file must check request.is_json before touching data"
    )
    assert "confirm_id" in body, (
        "hard_delete_file must read + require confirm_id from body"
    )
    # Must 400 on missing body / confirm_id
    assert re.search(r"return\s+jsonify\(.*?\),\s*400", body, re.DOTALL), (
        "hard_delete_file must 400 on destructive-gate failure"
    )


def test_soft_delete_endpoint_still_wired_as_delete():
    """Ensures the behavior-change doesn't accidentally re-route
    the existing DELETE /files/<id> endpoint elsewhere."""
    src = _read_source("app.py")
    assert re.search(
        r"'/files/<fileId>'\s*,\s*'DELETE'\s*,\s*"
        r"self\.fileStorageEP\.delete_file",
        src,
    ), "DELETE /files/<fileId> route unwired or renamed"


# ak-kxdr mutation drift guard — EXACT set on /files/* and /folders/*.
# Adapted from the ak-05af-banked exact-set pattern for legitimate-
# mutations beads. Covers EVERY mutation-verb route on both
# /files/* and /files/trash/*; a stray future mutation added on
# either surface fails this guard and forces §23 #5 review.
EXPECTED_FILES_MUTATIONS = {
    ("'/files/upload'", "'POST'"),
    ("'/files/<fileId>'", "'DELETE'"),  # soft-delete (ak-kxdr behavior)
    ("'/files/<fileId>/label'", "'PUT'"),
    ("'/files/<fileId>/move'", "'PUT'"),
    # ak-kxdr new mutations
    ("'/files/trash/<fileId>/restore'", "'POST'"),
    ("'/files/trash/<fileId>'", "'DELETE'"),  # hard-delete w/ gate
}


def test_no_unexpected_files_mutations():
    """ak-kxdr MUTATION DRIFT GUARD — asserts the EXACT set of
    mutation verbs on /files/*. Any surprise mutation added by a
    future bead fails this guard and forces a §23 #5 destructive-
    gate conversation before shipping. Banked from ak-05af."""
    src = _read_source("app.py")
    all_entries = re.findall(
        r"\(\s*('/files[^']*')\s*,\s*('[A-Z]+')\s*,", src,
    )
    found_mutations = {
        (rule, verb)
        for rule, verb in all_entries
        if verb.strip("'") in {"POST", "DELETE", "PUT", "PATCH"}
    }
    extras = found_mutations - EXPECTED_FILES_MUTATIONS
    assert not extras, (
        f"UNEXPECTED mutation routes on /files/*: {extras}. "
        f"If this is intentional, update EXPECTED_FILES_MUTATIONS "
        f"in this test AND confirm §23 #5 destructive-gate review "
        f"at the bead level."
    )
    missing = EXPECTED_FILES_MUTATIONS - found_mutations
    assert not missing, (
        f"EXPECTED mutation routes missing from app.py: {missing}"
    )


def test_trash_routes_wired():
    src = _read_source("app.py")
    # List trash — GET
    assert re.search(
        r"'/files/trash'\s*,\s*'GET'\s*,\s*"
        r"self\.fileStorageEP\.list_trash",
        src,
    ), "GET /files/trash route not wired"
    # Restore — POST
    assert re.search(
        r"'/files/trash/<fileId>/restore'\s*,\s*'POST'\s*,\s*"
        r"self\.fileStorageEP\.restore_file",
        src,
    ), "POST /files/trash/<fileId>/restore route not wired"
    # Hard-delete — DELETE
    assert re.search(
        r"'/files/trash/<fileId>'\s*,\s*'DELETE'\s*,\s*"
        r"self\.fileStorageEP\.hard_delete_file",
        src,
    ), "DELETE /files/trash/<fileId> route not wired"


# ─── Scheduler task registration ───────────────────────────────


def test_vault_cleanup_registered_in_task_mapping():
    """(19) VaultTrashCleanupTask must appear in TASK_MAPPING so
    the scheduler dispatcher routes jobs by that title."""
    src = _read_source("services/tasks/scheduler.py")
    assert "VaultTrashCleanupTask" in src, (
        "VaultTrashCleanupTask not imported in scheduler.py"
    )
    assert re.search(
        r'"VaultTrashCleanup"\s*:\s*VaultTrashCleanupTask', src,
    ), (
        '"VaultTrashCleanup": VaultTrashCleanupTask not registered '
        'in TASK_MAPPING'
    )


def test_vault_cleanup_task_module_shape():
    """Task module must define VaultTrashCleanupTask class with
    the BaseTask run() contract."""
    src = _read_source("services/tasks/VaultTrashCleanupTask.py")
    assert "class VaultTrashCleanupTask(BaseTask):" in src, (
        "VaultTrashCleanupTask must subclass BaseTask"
    )
    # run() must return a 3-tuple per BaseTask contract
    assert re.search(
        r"def run\(self\):", src,
    ), "VaultTrashCleanupTask.run method missing"
    # Must call cleanup_expired_trash
    assert "cleanup_expired_trash" in src, (
        "Task must delegate to FileStorageService.cleanup_expired_trash"
    )


def test_retention_days_constant():
    """(20) VAULT_RETENTION_DAYS == 30."""
    assert _LIFTED is not None, "AST-lift failed"
    assert _LIFTED["VAULT_RETENTION_DAYS"] == 30, (
        f"VAULT_RETENTION_DAYS drift: expected 30, got "
        f"{_LIFTED['VAULT_RETENTION_DAYS']}"
    )


if __name__ == "__main__":
    unittest.main()
